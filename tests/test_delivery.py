"""Exercise the delivery contract and durable outbox without a live webhook."""
import asyncio
import importlib
import json

import httpx
import pytest


@pytest.fixture
def bridge(tmp_path, monkeypatch):
    monkeypatch.delenv("DESTINATIONS_JSON", raising=False)
    monkeypatch.setenv("BRIDGE_TOKEN", "a" * 32)
    monkeypatch.setenv("GROKBOT_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setenv("GROKBOT_WEBHOOK_KEY", "secret-key")
    monkeypatch.setenv("OUTBOX_PATH", str(tmp_path / "outbox.sqlite3"))
    import app.outbox as outbox
    import app.main as main
    importlib.reload(outbox)
    importlib.reload(main)
    return main


def test_delivery_classification_and_redaction(bridge, caplog):
    payload = {"transcription": "private note", "bridgeRequestId": "request-123"}
    keys = []
    outcomes = [httpx.ReadTimeout("slow"), 500, 429, 400, 204]

    def handler(request):
        keys.append(request.headers["Idempotency-Key"])
        result = outcomes.pop(0)
        if isinstance(result, Exception):
            raise result
        return httpx.Response(result, text="invalid private note secret-key", headers={"Retry-After": "90"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=bridge.REQUEST_TIMEOUT) as client:
            return [await bridge.deliver(client, "https://example.test", "secret-key", payload, "request-123", n, "capture") for n in range(1, 6)]

    results = asyncio.run(run())
    assert [r[:2] for r in results] == [(False, True), (False, True), (False, True), (False, False), (True, False)]
    assert keys == ["request-123"] * 5
    assert results[2][3] == "90"
    assert bridge.outbox.retry_delay(2, results[2][3]) >= 90
    assert "private note" not in caplog.text and "secret-key" not in caplog.text
    assert "payload_summary" in caplog.text and "invalid" in caplog.text
    assert bridge.REQUEST_TIMEOUT.read == 45


def test_outbox_restart_and_replay(bridge):
    box = bridge.outbox
    box.enqueue("id", {"transcription": "private note"})
    importlib.reload(box)
    assert json.loads(box.next_item()[1])["transcription"] == "private note"
    box.finish("id", 1, "dead", "HTTP 400")
    assert box.next_item() is None
    with box.connect() as db:
        db.execute("UPDATE deliveries SET state='pending', attempts=0, next_attempt=0 WHERE id='id'")
    assert box.next_item()[0] == "id"


def test_capture_acknowledged_only_after_persistence(bridge, monkeypatch):
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url="http://test") as client:
            return await client.post("/index", headers={"Authorization": "Bearer " + "a" * 32}, files={"transcription": (None, "a note")})
    result = asyncio.run(run())
    assert result.status_code == 202
    assert bridge.outbox.next_item()[0] == result.json()["requestId"]
    monkeypatch.setattr(bridge.outbox, "enqueue", lambda *args: (_ for _ in ()).throw(OSError("disk full")))
    assert asyncio.run(run()).status_code == 503


def test_worker_retries_after_outage_and_delivers(bridge, monkeypatch):
    # Advance retry time without waiting for a real outage, while exercising the worker.
    box = bridge.outbox
    box.enqueue("request-123", {"transcription": "saved"})
    calls = []

    async def fake_deliver(client, url, key, payload, request_id, attempt, kind, receipt="http"):
        calls.append((request_id, attempt))
        if attempt == 1:
            return False, True, "ReadTimeout", None
        return True, False, "HTTP 200", None

    monkeypatch.setattr(bridge, "deliver", fake_deliver)
    monkeypatch.setattr(box, "retry_delay", lambda *args: 0)

    async def run():
        worker = asyncio.create_task(bridge.outbox_worker())
        for _ in range(100):
            await asyncio.sleep(0.01)
            with box.connect() as db:
                state = db.execute("SELECT state FROM deliveries WHERE id='request-123'").fetchone()[0]
            if state == "delivered":
                break
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        return state

    assert asyncio.run(run()) == "delivered"
    assert calls == [("request-123", 1), ("request-123", 2)]


def test_multiple_destinations_progress_independently(bridge, monkeypatch):
    from app.destinations import Destination
    destinations = [Destination("notes", "https://notes.example/hook", "notes-token"), Destination("archive", "https://archive.example/hook", "archive-token", "completed")]
    monkeypatch.setattr(bridge, "DESTINATIONS", destinations)
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.host == "notes.example":
            raise httpx.ReadTimeout("outage")
        payload = json.loads(request.content)
        return httpx.Response(200, json={"messageId": payload["messageId"], "status": "completed"})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(bridge.httpx, "AsyncClient", lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs))

    async def run():
        async with real_client(transport=httpx.ASGITransport(app=bridge.app), base_url="http://test") as client:
            response = await client.post("/index", headers={"Authorization": "Bearer " + "a" * 32}, files={"transcription": (None, "same words")})
        assert response.json()["destinations"] == ["notes", "archive"]
        workers = [asyncio.create_task(bridge.outbox_worker(item)) for item in destinations]
        for _ in range(100):
            await asyncio.sleep(0.01)
            with bridge.outbox.connect() as db:
                states = dict(db.execute("SELECT destination,state FROM deliveries"))
            if states["archive"] == "delivered":
                break
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        return states

    assert asyncio.run(run()) == {"notes": "pending", "archive": "delivered"}
    assert {req.headers["authorization"] for req in calls} == {"Bearer notes-token", "Bearer archive-token"}
    assert len({req.headers["idempotency-key"] for req in calls}) == 1
    assert {json.loads(req.content)["transcription"] for req in calls} == {"same words"}


def test_invalid_completion_receipt_is_retained_for_review(bridge):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"status": "completed", "messageId": "wrong"}))) as client:
            return await bridge.deliver(client, "https://example.test", "token", {"messageId": "expected"}, "id", 1, "copy", "completed")
    assert asyncio.run(run())[:3] == (False, False, "Invalid completion receipt")


def test_destination_config_validation(monkeypatch):
    from app.destinations import load_destinations
    config = [{"name": "notes", "url": "https://example.test/hook", "token": "private"}]
    monkeypatch.setenv("DESTINATIONS_JSON", json.dumps(config))
    assert load_destinations()[0].name == "notes"
    for bad in [[], config * 2, [{**config[0], "url": "http://example.test"}], [{**config[0], "receipt": "unknown"}]]:
        monkeypatch.setenv("DESTINATIONS_JSON", json.dumps(bad))
        with pytest.raises(RuntimeError):
            load_destinations()


def test_legacy_outbox_migration_preserves_pending(bridge):
    import sqlite3
    with sqlite3.connect(bridge.outbox.DB_PATH) as db:
        db.execute("CREATE TABLE outbox (id TEXT, payload TEXT, state TEXT, attempts INTEGER, next_attempt REAL, last_result TEXT)")
        db.execute("INSERT INTO outbox VALUES ('old-id', '{}', 'pending', 2, 0, 'ReadTimeout')")
    assert bridge.outbox.next_item("grokbot") == ("old-id", "{}", 2)
    assert bridge.outbox.next_item("archive") is None


def test_fanout_storage_rolls_back_if_any_copy_fails(bridge):
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        bridge.outbox.enqueue("id", {"transcription": "saved"}, ["notes", "notes"])
    with bridge.outbox.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
