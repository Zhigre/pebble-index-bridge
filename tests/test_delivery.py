from __future__ import annotations

import asyncio
import json
import logging
import time

import httpx

from app.delivery import DeliverySupervisor, DeliveryWorker
from app.store import Store


def run(coro):
    return asyncio.run(coro)


def test_pre_send_connection_timeout_is_shorter_than_total_request_deadline(tmp_path):
    async def exercise():
        worker = DeliveryWorker(
            Store(tmp_path / "queue.sqlite"), "https://example.invalid/hook", "secret", timeout=15
        )
        client = await worker._get_client()
        assert client.timeout.connect == 5
        assert client.timeout.pool == 5
        assert client.timeout.read is None
        await worker.close()

    run(exercise())


def test_200_marks_webhook_accepted_and_reuses_client(tmp_path):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.headers["authorization"], json.loads(request.content)))
        return httpx.Response(200, content=b"an arbitrarily large body is never inspected")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    store = Store(tmp_path / "queue.sqlite")
    receipt = store.enqueue("hello", "1", "ring")
    worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
    assert run(worker.run_once()) is True
    assert store.get(receipt["id"])["status"] == "delivered"
    assert seen[0][0] == "Bearer secret"
    assert seen[0][1]["bridgeRequestId"] == receipt["id"]
    run(worker.close())
    assert client.is_closed is False
    run(client.aclose())


def test_non_2xx_is_attention_without_reading_response_body(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"private downstream details")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    store = Store(tmp_path / "queue.sqlite")
    receipt = store.enqueue("hello", "1", "ring")
    worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
    assert run(worker.run_once()) is True
    metadata = store.get(receipt["id"])
    assert metadata["status"] == "needs_attention"
    assert metadata["httpStatus"] == 500
    assert metadata["lastErrorCode"] == "http_500"
    run(client.aclose())


def test_upstream_outage_statuses_retry_with_backoff_then_need_attention(tmp_path):
    # 2026-10-13: Grok answered 400 during a webhook incident, the note was
    # parked as needs_attention and the FIFO froze behind it for 27 hours.
    # A response that says "not accepted / unavailable" must retry on backoff.
    for status in (400, 408, 425, 429, 502, 503, 504):
        def handler(request: httpx.Request, status=status) -> httpx.Response:
            return httpx.Response(status)

        store = Store(tmp_path / f"queue-{status}.sqlite")
        receipt = store.enqueue("hello", "1", "ring")
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        worker = DeliveryWorker(
            store, "https://example.invalid/hook", "secret", max_attempts=2, client=client
        )
        assert run(worker.run_once()) is True
        retry = store.get(receipt["id"])
        assert retry["status"] == "retry", status
        assert retry["lastErrorCode"] == f"http_{status}"
        assert retry["nextAttemptAt"] >= int(time.time()) + 1
        run(client.aclose())


def test_upstream_outage_retry_budget_exhausts_to_attention(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    store = Store(tmp_path / "queue.sqlite")
    receipt = store.enqueue("hello", "1", "ring")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    worker = DeliveryWorker(
        store, "https://example.invalid/hook", "secret", max_attempts=1, client=client
    )
    assert run(worker.run_once()) is True
    metadata = store.get(receipt["id"])
    assert metadata["status"] == "needs_attention"
    assert metadata["lastErrorCode"] == "http_503_exhausted"
    assert metadata["httpStatus"] == 503
    run(client.aclose())


def test_auth_rejection_needs_attention_immediately(tmp_path):
    for status in (401, 403):
        def handler(request: httpx.Request, status=status) -> httpx.Response:
            return httpx.Response(status)

        store = Store(tmp_path / f"queue-{status}.sqlite")
        receipt = store.enqueue("hello", "1", "ring")
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
        assert run(worker.run_once()) is True
        assert store.get(receipt["id"])["status"] == "needs_attention"
        run(client.aclose())


def test_connect_failure_retries_but_read_timeout_needs_attention(tmp_path):
    def connect_failure(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    path = tmp_path / "queue.sqlite"
    store = Store(path)
    receipt = store.enqueue("connect", "1", "ring")
    client = httpx.AsyncClient(transport=httpx.MockTransport(connect_failure))
    worker = DeliveryWorker(
        store, "https://example.invalid/hook", "secret", retry_base=2, client=client
    )
    assert run(worker.run_once()) is True
    retry = store.get(receipt["id"])
    assert retry["status"] == "retry"
    assert retry["nextAttemptAt"] >= int(time.time()) + 1
    run(client.aclose())

    def accepted_then_timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("response timed out", request=request)

    store = Store(path)
    # The first event is still the retry head, so use a fresh database for the
    # ambiguity check and prove it is never automatically resent.
    ambiguous_store = Store(tmp_path / "ambiguous.sqlite")
    ambiguous = ambiguous_store.enqueue("accepted", "2", "ring")
    timeout_client = httpx.AsyncClient(transport=httpx.MockTransport(accepted_then_timeout))
    timeout_worker = DeliveryWorker(
        ambiguous_store, "https://example.invalid/hook", "secret", client=timeout_client
    )
    assert run(timeout_worker.run_once()) is True
    assert ambiguous_store.get(ambiguous["id"])["status"] == "needs_attention"
    assert run(timeout_worker.run_once()) is False
    run(timeout_client.aclose())


def test_fifo_delivery_order(tmp_path):
    delivered = []

    def handler(request: httpx.Request) -> httpx.Response:
        delivered.append(json.loads(request.content)["transcription"])
        return httpx.Response(200)

    store = Store(tmp_path / "queue.sqlite")
    store.enqueue("first", "1", "ring")
    store.enqueue("second", "2", "ring")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
    assert run(worker.run_once()) is True
    assert run(worker.run_once()) is True
    assert delivered == ["first", "second"]
    run(client.aclose())


def test_cancellation_settles_inflight_event_as_attention(tmp_path):
    async def exercise():
        class HangingTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        store = Store(tmp_path / "queue.sqlite")
        receipt = store.enqueue("cancel me", "1", "ring")
        client = httpx.AsyncClient(transport=HangingTransport())
        worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
        task = asyncio.create_task(worker.run_once())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert store.get(receipt["id"])["status"] == "needs_attention"
        await client.aclose()

    run(exercise())


def test_grok_other_2xx_is_not_documented_acceptance(tmp_path):
    store = Store(tmp_path / "queue.sqlite")
    receipt = store.enqueue("hello", "1", "ring")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(202)))
    worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
    assert run(worker.run_once()) is True
    assert store.get(receipt["id"])["status"] == "needs_attention"
    assert store.get(receipt["id"])["httpStatus"] == 202
    run(client.aclose())


def test_numeric_timestamp_and_sanitized_unexpected_error(tmp_path, capsys):
    def unexpected(request):
        payload = json.loads(request.content)
        assert isinstance(payload["recordedAt"], int)
        raise RuntimeError("private webhook URL and key must not escape")

    store = Store(tmp_path / "queue.sqlite")
    receipt = store.enqueue("hello", "1790037251116", "ring")
    client = httpx.AsyncClient(transport=httpx.MockTransport(unexpected))
    worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
    assert run(worker.run_once()) is True
    assert store.get(receipt["id"])["lastErrorCode"] == "delivery_error"
    assert "private webhook" not in capsys.readouterr().err
    run(client.aclose())


def test_http_client_loggers_never_emit_url_or_key_at_root_info(tmp_path, caplog):
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.NOTSET)
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    store = Store(tmp_path / "queue.sqlite")
    store.enqueue("hello", "1", "ring")
    with caplog.at_level(logging.DEBUG):
        worker = DeliveryWorker(
            store, "https://example.invalid/private-hook", "private-key", client=client
        )
        assert run(worker.run_once()) is True
    assert "private-hook" not in caplog.text
    assert "private-key" not in caplog.text
    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING
    run(client.aclose())


class FlakyStore(Store):
    """Raises on the first claim and mark_delivered calls like a locked database."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.claim_failures = 1
        self.mark_failures = 0

    def claim_next(self):
        if self.claim_failures:
            self.claim_failures -= 1
            raise OSError("database is locked")
        return super().claim_next()

    def mark_delivered(self, event_id, http_status=200):
        if self.mark_failures:
            self.mark_failures -= 1
            raise OSError("database is locked")
        return super().mark_delivered(event_id, http_status=http_status)


def supervise(store, handler, until, timeout=5):
    async def exercise():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        worker = DeliveryWorker(store, "https://example.invalid/hook", "secret", client=client)
        supervisor = DeliverySupervisor(worker, store, restart_base=0.01, restart_max=0.04)
        stop = asyncio.Event()
        task = asyncio.create_task(supervisor.run(stop))
        try:
            async with asyncio.timeout(timeout):
                while not until():
                    await asyncio.sleep(0.01)
        finally:
            stop.set()
            await task
            await client.aclose()
        return supervisor

    return run(exercise())


def test_supervisor_restarts_worker_after_claim_failure(tmp_path, caplog):
    store = FlakyStore(tmp_path / "queue.sqlite")
    receipt = store.enqueue("hello", "1", "ring")
    store.claim_failures = 2
    sent = []

    def handler(request):
        sent.append(1)
        return httpx.Response(200)

    with caplog.at_level(logging.INFO):
        supervisor = supervise(
            store, handler, lambda: store.get(receipt["id"])["status"] == "delivered"
        )
    assert sent == [1]
    assert supervisor.available is False
    assert "code=worker_failed" in caplog.text
    assert "database is locked" not in caplog.text


def test_supervisor_marks_stale_sending_attention_and_never_resends(tmp_path):
    store = FlakyStore(tmp_path / "queue.sqlite")
    receipt = store.enqueue("first", "1", "ring")
    store.enqueue("second", "2", "ring")
    store.claim_failures = 0
    store.mark_failures = 1
    sent = []

    def handler(request):
        sent.append(json.loads(request.content)["transcription"])
        return httpx.Response(200)

    supervise(
        store,
        handler,
        lambda: store.get(receipt["id"])["status"] == "needs_attention",
    )
    metadata = store.get(receipt["id"])
    assert metadata["lastErrorCode"] == "process_restart"
    assert sent == ["first"]  # no blind resend; the head stays blocked for the operator


def test_supervisor_recovery_failure_keeps_unavailable_and_backs_off(tmp_path):
    class RecoveryStore(Store):
        recoveries = 0

        def recover_inflight(self):
            RecoveryStore.recoveries += 1
            if RecoveryStore.recoveries < 3:
                raise OSError("database is locked")
            return super().recover_inflight()

    store = RecoveryStore(tmp_path / "queue.sqlite")
    runs = []

    class Worker:
        async def run(self, stop_event):
            runs.append(RecoveryStore.recoveries)
            await stop_event.wait()

        async def close(self):
            pass

    async def exercise():
        supervisor = DeliverySupervisor(Worker(), store, restart_base=0.01, restart_max=0.04)
        stop = asyncio.Event()
        task = asyncio.create_task(supervisor.run(stop))
        await asyncio.sleep(0.005)
        assert supervisor.available is False
        async with asyncio.timeout(5):
            while not runs:
                await asyncio.sleep(0.01)
        stop.set()
        await task

    run(exercise())
    assert runs == [3]  # the worker never started until recovery succeeded


def test_supervisor_backoff_is_bounded():
    supervisor = DeliverySupervisor(object(), object(), restart_base=1, restart_max=8)
    delays = []
    for failures in range(1, 8):
        supervisor.failures = failures
        delays.append(supervisor._backoff())
    assert delays == [1, 2, 4, 8, 8, 8, 8]
