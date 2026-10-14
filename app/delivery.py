"""Safe FIFO delivery of durable Pebble events to a downstream webhook."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import time
from collections.abc import Callable
from typing import Any

import httpx

from .store import Store

log = logging.getLogger("index-bridge")
if not log.handlers:
    log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)
log.propagate = False


def quiet_http_loggers() -> None:
    """Keep HTTPX/HTTPCore from logging request URLs, even with a root INFO level."""

    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)


quiet_http_loggers()


async def store_call(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run a blocking store method off the event loop.

    Test doubles may expose coroutine methods; their coroutine is awaited here.
    """

    result = await asyncio.to_thread(function, *args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


# Responses that mean "not accepted, try later". The Grok webhook returned 400
# for every call during the 2026-10-13 incident. 500 stays operator-reviewed
# because the body may have been processed before the error; 401/403 mean the
# key is wrong and retrying cannot help.
RETRYABLE_HTTP_STATUSES = frozenset({400, 408, 425, 429, 502, 503, 504})


class DeliveryWorker:
    """Deliver one queue head at a time with conservative failure handling."""

    def __init__(
        self,
        store: Store,
        url: str,
        key: str,
        timeout: float = 15,
        max_attempts: int = 5,
        retry_base: float = 2,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not isinstance(url, str) or not url:
            raise ValueError("delivery URL is required")
        if not isinstance(key, str) or not key:
            raise ValueError("delivery key is required")
        if timeout <= 0 or max_attempts < 1 or retry_base <= 0:
            raise ValueError("invalid delivery limits")
        self.store = store
        self.url = url
        self.key = key
        self.timeout = float(timeout)
        self.max_attempts = int(max_attempts)
        self.retry_base = float(retry_base)
        self._client = client
        self._owns_client = client is None
        quiet_http_loggers()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                verify=True,
                follow_redirects=False,
                trust_env=False,
                timeout=httpx.Timeout(
                    None, connect=min(5.0, self.timeout / 3), pool=min(5.0, self.timeout / 3)
                ),
            )
        return self._client

    def _retry_at(self, attempts: int) -> int:
        # Keep a bad network path from producing an unbounded timestamp.
        delay = min(self.retry_base * (2 ** max(0, attempts - 1)), 3600.0)
        return int(time.time() + delay)

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _send(self, payload: dict[str, Any]) -> int:
        client = await self._get_client()
        headers = {
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }
        # ``stream`` lets HTTPX close and discard the response stream without
        # buffering an untrusted downstream body in memory.
        async with asyncio.timeout(self.timeout):
            async with client.stream("POST", self.url, headers=headers, json=payload) as response:
                return int(response.status_code)

    async def _claim(self) -> dict[str, Any] | None:
        claim = asyncio.ensure_future(store_call(self.store.claim_next))
        try:
            return await asyncio.shield(claim)
        except asyncio.CancelledError:
            # The thread cannot be interrupted; settle whatever it claimed.
            try:
                event = await claim
            except Exception:
                event = None
            if event is not None:
                await store_call(self.store.mark_attention, event["id"], "cancelled")
            raise

    async def run_once(self) -> bool:
        event = await self._claim()
        if event is None:
            return False
        event_id = event["id"]
        attempts = int(event["attempts"])
        try:
            status = await self._send(event["payload"])
        except asyncio.CancelledError:
            await store_call(self.store.mark_attention, event_id, "cancelled")
            raise
        except (httpx.ConnectError, httpx.ConnectTimeout):
            if attempts < self.max_attempts:
                await store_call(
                    self.store.mark_retry, event_id, "connect_error", self._retry_at(attempts)
                )
            else:
                await store_call(self.store.mark_attention, event_id, "connect_error_exhausted")
            return True
        except (asyncio.TimeoutError, httpx.TimeoutException):
            # A timeout can occur after the server accepted the body. Never
            # resend automatically when acceptance is ambiguous.
            await store_call(self.store.mark_attention, event_id, "ambiguous_timeout")
            return True
        except httpx.RequestError:
            # Read/write/protocol failures do not prove that no request was
            # accepted, so they require operator review.
            await store_call(self.store.mark_attention, event_id, "ambiguous_transport_error")
            return True
        except Exception:
            # A process/runtime error after claim is also ambiguous. Do not
            # let an application exception turn into a blind duplicate send.
            await store_call(self.store.mark_attention, event_id, "delivery_error")
            log.error("delivery event_id=%s error=delivery_error", event_id)
            return True

        log.info("delivery event_id=%s http_status=%d", event_id, status)
        if status == 200:
            await store_call(self.store.mark_delivered, event_id, http_status=status)
        elif status in RETRYABLE_HTTP_STATUSES:
            # The webhook answered and refused the body, so a resend cannot
            # duplicate. Retry on backoff instead of freezing the FIFO.
            if attempts < self.max_attempts:
                await store_call(
                    self.store.mark_retry, event_id, f"http_{status}", self._retry_at(attempts)
                )
            else:
                await store_call(
                    self.store.mark_attention,
                    event_id,
                    f"http_{status}_exhausted",
                    http_status=status,
                )
        else:
            await store_call(
                self.store.mark_attention, event_id, f"http_{status}", http_status=status
            )
        return True

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                processed = await self.run_once()
                if not processed:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=0.25)
                    except asyncio.TimeoutError:
                        pass
        finally:
            await self.close()


class DeliverySupervisor:
    """Restart a failed delivery worker with bounded backoff.

    Stale ``sending`` rows are settled as ``needs_attention`` before every
    (re)start, so a send whose outcome is unknown is never retried blindly.
    Only fixed error codes are logged, never exception text.
    """

    def __init__(
        self,
        worker: Any,
        store: Any,
        restart_base: float = 1.0,
        restart_max: float = 30.0,
        stable_seconds: float = 60.0,
    ) -> None:
        if restart_base <= 0 or restart_max < restart_base:
            raise ValueError("invalid restart limits")
        self.worker = worker
        self.store = store
        self.restart_base = float(restart_base)
        self.restart_max = float(restart_max)
        self.stable_seconds = float(stable_seconds)
        self.available = False
        self.failures = 0

    def _backoff(self) -> float:
        exponent = max(0, self.failures - 1)
        if exponent >= math.log2(self.restart_max) - math.log2(self.restart_base):
            return self.restart_max
        return min(math.ldexp(self.restart_base, exponent), self.restart_max)

    async def _start(self, stop_event: asyncio.Event) -> str:
        """Run one worker generation and return the safe code for why it ended."""

        try:
            recover = getattr(self.store, "recover_inflight", None)
            if recover is not None:
                await store_call(recover)
        except Exception:
            return "recovery_failed"
        started = time.monotonic()
        self.available = True
        try:
            await self.worker.run(stop_event)
        except Exception:
            code = "worker_failed"
        else:
            code = "worker_exited"
        finally:
            self.available = False
        if time.monotonic() - started >= self.stable_seconds:
            self.failures = 0
        return code

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                code = await self._start(stop_event)
                if stop_event.is_set():
                    break
                self.failures += 1
                delay = self._backoff()
                log.error(
                    "delivery_supervisor code=%s failures=%d restart_in=%.1f",
                    code,
                    self.failures,
                    delay,
                )
                try:
                    async with asyncio.timeout(delay):
                        await stop_event.wait()
                except TimeoutError:
                    pass
        finally:
            self.available = False
