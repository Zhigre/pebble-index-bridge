"""HTTP intake for the Pebble Index bridge.

The application accepts one small multipart request, durably queues it, and
returns only after the queue has acknowledged the record. Delivery is owned by
``app.delivery``; this module deliberately does not call the downstream webhook.
"""

from __future__ import annotations

import asyncio
import hmac
import inspect
import logging
import re
import time
import unicodedata
from collections import deque
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, cast

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.formparsers import MultiPartException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .config import Settings
from .delivery import DeliverySupervisor, DeliveryWorker, quiet_http_loggers, store_call
from .store import QueueFull, Store

log = logging.getLogger("index-bridge")
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
CLIENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,31}\Z")
RECORDED_AT_RE = re.compile(r"[0-9]{13}\Z")
BODY_READ_TIMEOUT_SECONDS = 10.0
# Docker probes with a 2 second client timeout.
HEALTH_TIMEOUT_SECONDS = 1.5
WORKER_RESTART_BASE_SECONDS = 1.0
WORKER_RESTART_MAX_SECONDS = 30.0
_MISSING = object()


def _json_error(status: int, detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=status)


def _headers(scope: Mapping[str, Any], name: bytes) -> list[bytes]:
    wanted = name.lower()
    return [value for key, value in scope.get("headers", []) if key.lower() == wanted]


class StreamedBodyLimitMiddleware:
    """Buffer a request only after enforcing its total byte limit."""

    def __init__(
        self,
        app: Callable[..., Awaitable[Any]],
        max_bytes: int,
        bridge_token: str,
        tracker: Any,
    ) -> None:
        self.app = app
        self.max_bytes = max_bytes
        self.body_timeout = BODY_READ_TIMEOUT_SECONDS
        self.bridge_token = bridge_token
        self.tracker = tracker

    def _authenticated(self, scope: Mapping[str, Any], bridge_token: str) -> bool:
        values = _headers(scope, b"authorization")
        if len(values) != 1:
            return False
        try:
            authorization = values[0].decode("ascii")
        except UnicodeDecodeError:
            return False
        prefix = "Bearer "
        if not authorization.startswith(prefix) or len(authorization) == len(prefix):
            return False
        return hmac.compare_digest(authorization, f"{prefix}{bridge_token}")

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[..., Awaitable[Any]],
        send: Callable[..., Awaitable[Any]],
    ) -> None:
        if (
            scope.get("type") != "http"
            or scope.get("path") != "/index"
            or scope.get("method") != "POST"
        ):
            await self.app(scope, receive, send)
            return

        if not self._authenticated(scope, self.bridge_token):
            if self.tracker is not None:
                await self.tracker.auth_failure()
            await _send_error(scope, send, 401, "Unauthorized")
            return
        scope["_index_authenticated"] = True

        content_lengths = _headers(scope, b"content-length")
        transfer_encodings = _headers(scope, b"transfer-encoding")
        if len(content_lengths) > 1 or len(transfer_encodings) > 1:
            await _send_error(scope, send, 400, "Invalid request framing")
            return
        if content_lengths and transfer_encodings:
            await _send_error(scope, send, 400, "Conflicting request framing")
            return

        declared_length: int | None = None
        if content_lengths:
            try:
                raw_length = content_lengths[0].decode("ascii")
            except UnicodeDecodeError:
                await _send_error(scope, send, 400, "Invalid Content-Length")
                return
            if not re.fullmatch(r"[0-9]+", raw_length):
                await _send_error(scope, send, 400, "Invalid Content-Length")
                return
            if len(raw_length) > 20:
                await _send_error(scope, send, 400, "Invalid Content-Length")
                return
            declared_length = int(raw_length, 10)
            if declared_length > self.max_bytes:
                await _send_error(scope, send, 413, "Request too large")
                return
        if transfer_encodings:
            try:
                transfer = transfer_encodings[0].decode("ascii").strip().lower()
            except UnicodeDecodeError:
                await _send_error(scope, send, 400, "Invalid Transfer-Encoding")
                return
            if transfer != "chunked":
                await _send_error(scope, send, 400, "Unsupported Transfer-Encoding")
                return

        content_type = _headers(scope, b"content-type")
        if len(content_type) != 1:
            await _send_error(scope, send, 415, "Unsupported media type")
            return
        try:
            content_type_text = content_type[0].decode("ascii").lower()
        except UnicodeDecodeError:
            await _send_error(scope, send, 415, "Unsupported media type")
            return
        if (
            not content_type_text.startswith("multipart/form-data;")
            or "boundary=" not in content_type_text
        ):
            await _send_error(scope, send, 415, "Unsupported media type")
            return

        body_buffer = bytearray()
        total = 0
        try:
            async with asyncio.timeout(self.body_timeout):
                while True:
                    message = await receive()
                    if message.get("type") == "http.disconnect":
                        await _send_error(scope, send, 400, "Malformed request")
                        return
                    if message.get("type") != "http.request":
                        await _send_error(scope, send, 400, "Malformed request")
                        return
                    chunk = message.get("body", b"")
                    total += len(chunk)
                    if total > self.max_bytes:
                        await _send_error(scope, send, 413, "Request too large")
                        return
                    if chunk:
                        body_buffer.extend(chunk)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            await _send_error(scope, send, 408, "Request body timeout")
            return

        # A mismatched declared length is a framing error.  Checking it after
        # reading still preserves the streamed cap and supports chunked input.
        if declared_length is not None and declared_length != total:
            await _send_error(scope, send, 400, "Invalid Content-Length")
            return

        body = bytes(body_buffer)
        delivered = False

        async def replay_receive() -> dict[str, Any]:
            nonlocal delivered
            if delivered:
                return {"type": "http.disconnect"}
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}

        await self.app(scope, replay_receive, send)


async def _send_error(
    scope: dict[str, Any], send: Callable[..., Awaitable[Any]], status: int, detail: str
) -> None:
    response = _json_error(status, detail)
    await response(scope=scope, receive=cast(Any, None), send=send)


class RejectionTracker:
    """Bounded rejection accounting with aggregate counters."""

    def __init__(self, rate_limit: int, window_seconds: int, max_events: int = 4096) -> None:
        self.rate_limit = rate_limit
        self.window_seconds = window_seconds
        event_cap = max(max_events, rate_limit + 1)
        self.accepted: deque[float] = deque(maxlen=event_cap)
        self.auth_failures: deque[float] = deque(maxlen=event_cap)
        self.auth_total = 0
        self.rate_total = 0
        self.malformed_total = 0
        self._last_log: float | None = None
        self._lock = asyncio.Lock()

    def _aggregate_log(self, now: float) -> None:
        if self._last_log is not None and now - self._last_log < 60:
            return
        self._last_log = now
        log.warning(
            "security_rejections auth=%d rate=%d malformed=%d",
            self.auth_total,
            self.rate_total,
            self.malformed_total,
        )

    async def auth_failure(self) -> int:
        now = time.monotonic()
        async with self._lock:
            self.auth_total += 1
            self.auth_failures.append(now)
            cutoff = now - 300
            while self.auth_failures and self.auth_failures[0] < cutoff:
                self.auth_failures.popleft()
            count = len(self.auth_failures)
            self._aggregate_log(now)
        return count

    async def malformed(self) -> None:
        now = time.monotonic()
        async with self._lock:
            self.malformed_total += 1
            self._aggregate_log(now)

    async def accept_or_reject(self) -> bool:
        now = time.monotonic()
        async with self._lock:
            cutoff = now - self.window_seconds
            while self.accepted and self.accepted[0] < cutoff:
                self.accepted.popleft()
            if len(self.accepted) >= self.rate_limit:
                self.rate_total += 1
                self._aggregate_log(now)
                return False
            self.accepted.append(now)
            return True


def _ascii(value: str | None) -> bool:
    if value is None:
        return False
    try:
        value.encode("ascii")
    except UnicodeEncodeError:
        return False
    return True


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _field(value: Any, name: str, default: Any = _MISSING) -> Any:
    if isinstance(value, dict):
        if name in value:
            return value[name]
    else:
        result = getattr(value, name, _MISSING)
        if result is not _MISSING:
            return result
    if default is not _MISSING:
        return default
    raise AttributeError(name)


def _receipt_json(receipt: Any) -> dict[str, Any]:
    receipt_id = _field(receipt, "id")
    status = _field(receipt, "status")
    duplicate = bool(_field(receipt, "duplicate", False))
    return {"ok": True, "eventId": str(receipt_id), "status": str(status), "duplicate": duplicate}


def _public_metadata(record: Any) -> dict[str, Any]:
    allowed = {
        "id",
        "status",
        "duplicate",
        "recorded_at",
        "recordedAt",
        "client",
        "created_at",
        "createdAt",
        "updated_at",
        "updatedAt",
        "attempts",
        "nextAttemptAt",
        "lastErrorCode",
        "httpStatus",
    }
    source = (
        record if isinstance(record, dict) else vars(record) if hasattr(record, "__dict__") else {}
    )
    result: dict[str, Any] = {}
    for key in allowed:
        if key not in source:
            continue
        value = source[key]
        if key == "client" and (not isinstance(value, str) or not CLIENT_RE.fullmatch(value)):
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[key] = value
    if "id" not in result:
        record_id = _field(record, "id", None)
        if record_id is not None:
            result["id"] = str(record_id)
    return result


def _counts_metadata(counts: Any) -> dict[str, Any]:
    if not isinstance(counts, dict):
        return {}
    result: dict[str, Any] = {}
    for key, value in counts.items():
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}", key):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            result[key] = value
    return result


def _capacity_metadata(capacity: Any) -> dict[str, Any]:
    if not isinstance(capacity, dict):
        return {}
    allowed = {
        "limits": {"maxPending", "maxRecords", "maxBytes"},
        "current": {"pending", "records", "bytes"},
        "remaining": {"pending", "records", "bytes"},
    }
    return {
        group: {
            key: value
            for key, value in _counts_metadata(capacity.get(group)).items()
            if key in keys
        }
        for group, keys in allowed.items()
    }


def _build_worker(settings: Settings, store: Store) -> DeliveryWorker:
    return DeliveryWorker(
        store,
        settings.grokbot_webhook_url,
        settings.grokbot_webhook_key,
        timeout=settings.request_timeout_seconds,
        max_attempts=16,  # backoff caps at 1h: ~5h of retries, outlasts a provider incident
        retry_base=2,
        client=None,
    )


def create_app(
    settings: Settings | None = None,
    store: Store | None = None,
    worker: DeliveryWorker | None = None,
    start_worker: bool = True,
) -> FastAPI:
    """Create an isolated application for production or tests."""

    settings = settings or Settings.from_env()
    tracker = RejectionTracker(settings.rate_limit_requests, settings.rate_limit_window_seconds)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        quiet_http_loggers()
        current_store = application.state.store
        store_owned = current_store is None
        if current_store is None:
            current_store = Store(
                application.state.settings.db_path,
                max_pending=application.state.settings.max_pending,
                max_records=application.state.settings.max_records,
                max_bytes=application.state.settings.max_bytes,
            )
            application.state.store = current_store

        current_worker = application.state.worker
        task: asyncio.Task[Any] | None = None
        stop_event = asyncio.Event()
        try:
            if application.state.start_worker:
                if current_worker is None:
                    current_worker = _build_worker(application.state.settings, current_store)
                    application.state.worker = current_worker
                # The supervisor settles stale in-flight rows before each start.
                supervisor = DeliverySupervisor(
                    current_worker,
                    current_store,
                    restart_base=WORKER_RESTART_BASE_SECONDS,
                    restart_max=WORKER_RESTART_MAX_SECONDS,
                )
                application.state.supervisor = supervisor
                task = asyncio.ensure_future(supervisor.run(stop_event))
            else:
                recover = getattr(current_store, "recover_inflight", None)
                if recover is not None:
                    await store_call(recover)
            application.state.worker_task = task
            yield
        finally:
            stop_event.set()
            try:
                if task is not None:
                    grace = max(30.0, application.state.settings.request_timeout_seconds + 5)
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout=grace)
                    except asyncio.TimeoutError:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    except Exception:
                        log.error("shutdown_error code=delivery_supervisor_failed")
            finally:
                try:
                    close_worker = getattr(current_worker, "close", None)
                    if close_worker is not None:
                        await _maybe_await(close_worker())
                except Exception:
                    log.error("shutdown_error code=worker_close_failed")
                finally:
                    close_store = getattr(current_store, "close", None)
                    if store_owned and close_store is not None:
                        await store_call(close_store)

    application = FastAPI(
        title="Pebble Index Bridge",
        version="0.2.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.store = store
    application.state.worker = worker
    application.state.start_worker = start_worker
    application.state.tracker = tracker
    application.state.worker_task = None
    application.state.supervisor = None
    application.state.health_probe = None
    application.add_middleware(TrustedHostMiddleware, allowed_hosts=list(settings.allowed_hosts))
    application.add_middleware(
        StreamedBodyLimitMiddleware,
        max_bytes=settings.max_request_bytes,
        bridge_token=settings.bridge_token,
        tracker=tracker,
    )

    async def authenticate(request: Request) -> None:
        if request.scope.get("_index_authenticated") is True:
            return
        values = _headers(request.scope, b"authorization")
        if len(values) != 1:
            await tracker.auth_failure()
            raise HTTPException(status_code=401, detail="Unauthorized")
        try:
            authorization = values[0].decode("ascii")
        except UnicodeDecodeError:
            await tracker.auth_failure()
            raise HTTPException(status_code=401, detail="Unauthorized")
        prefix = "Bearer "
        if not authorization.startswith(prefix) or len(authorization) == len(prefix):
            await tracker.auth_failure()
            raise HTTPException(status_code=401, detail="Unauthorized")
        expected = f"{prefix}{settings.bridge_token}"
        if not hmac.compare_digest(authorization, expected):
            await tracker.auth_failure()
            raise HTTPException(status_code=401, detail="Unauthorized")

    def delivery_available() -> bool:
        if application.state.store is None:
            return False
        if not application.state.start_worker:
            return True
        supervisor = application.state.supervisor
        task = application.state.worker_task
        return (
            supervisor is not None and supervisor.available and task is not None and not task.done()
        )

    async def store_op(
        operation: str, function: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Call the store off the event loop; storage faults become a safe 503."""

        try:
            return await store_call(function, *args, **kwargs)
        except QueueFull:
            raise
        except Exception:
            log.error("store_error op=%s", operation)
            raise HTTPException(status_code=503, detail="Queue unavailable") from None

    @application.get("/health")
    async def health() -> JSONResponse:
        current_store = application.state.store
        try:
            if current_store is None or not delivery_available():
                raise RuntimeError("delivery unavailable")
            # One probe at a time, so a held database lock cannot pile up threads.
            probe = application.state.health_probe
            if probe is None or probe.done():
                probe = asyncio.ensure_future(store_call(current_store.counts))
                probe.add_done_callback(lambda done: done.cancelled() or done.exception())
                application.state.health_probe = probe
            async with asyncio.timeout(HEALTH_TIMEOUT_SECONDS):
                await asyncio.shield(probe)
        except Exception:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ok"})

    @application.post("/index")
    async def index_webhook(request: Request) -> JSONResponse:
        await authenticate(request)
        if not delivery_available():
            raise HTTPException(status_code=503, detail="Delivery unavailable")
        if not await tracker.accept_or_reject():
            raise HTTPException(status_code=429, detail="Too many requests")
        try:
            form = await request.form(
                max_files=0, max_fields=3, max_part_size=settings.max_request_bytes
            )
        except HTTPException as exc:
            if exc.status_code != 400:
                raise
            await tracker.malformed()
            raise HTTPException(status_code=400, detail="Malformed request") from None
        except MultiPartException:
            await tracker.malformed()
            raise HTTPException(status_code=400, detail="Malformed request")

        items = list(form.multi_items())
        names = [name for name, _ in items]
        if len(names) != len(set(names)):
            raise HTTPException(status_code=400, detail="Duplicate form field")
        if any(
            not isinstance(name, str) or name not in {"transcription", "recordedAt", "client"}
            for name in names
        ):
            raise HTTPException(status_code=400, detail="Unexpected form field")

        fields = dict(items)
        raw_transcription = fields.get("transcription")
        if not isinstance(raw_transcription, str):
            raise HTTPException(status_code=400, detail="Missing transcription")
        transcription = unicodedata.normalize("NFC", raw_transcription).strip()
        if (
            not transcription
            or len(transcription) > settings.max_transcription_chars
            or CONTROL_RE.search(transcription)
        ):
            raise HTTPException(status_code=400, detail="Invalid transcription")

        raw_recorded_at = fields.get("recordedAt")
        if (
            not isinstance(raw_recorded_at, str)
            or not _ascii(raw_recorded_at)
            or not RECORDED_AT_RE.fullmatch(raw_recorded_at)
        ):
            raise HTTPException(status_code=400, detail="Invalid recordedAt")
        recorded_at = raw_recorded_at

        client_name = fields.get("client", "ring")
        if (
            not isinstance(client_name, str)
            or not _ascii(client_name)
            or not CLIENT_RE.fullmatch(client_name)
        ):
            raise HTTPException(status_code=400, detail="Invalid client")

        log.info(
            "capture client=%s recordedAt=%s chars=%d", client_name, recorded_at, len(transcription)
        )
        try:
            receipt = await store_op(
                "enqueue",
                application.state.store.enqueue,
                transcription,
                recorded_at,
                client_name,
            )
        except QueueFull:
            log.warning("security_event=queue_capacity_reached")
            raise HTTPException(status_code=503, detail="Queue unavailable")
        return JSONResponse(_receipt_json(receipt), status_code=202)

    @application.get("/status")
    async def status(request: Request) -> dict[str, Any]:
        await authenticate(request)
        current_store = application.state.store
        recent = await store_op("list_recent", current_store.list_recent, limit=20)
        counts = await store_op("counts", current_store.counts)
        head = await store_op("head", current_store.head)
        capacity = await store_op("capacity", current_store.capacity)
        return {
            "counts": _counts_metadata(counts),
            "capacity": _capacity_metadata(capacity),
            "head": None if head is None else _public_metadata(head),
            "recent": [_public_metadata(item) for item in recent],
        }

    @application.get("/status/{event_id}")
    async def status_event(event_id: str, request: Request) -> dict[str, Any]:
        await authenticate(request)
        if not re.fullmatch(r"[a-f0-9]{64}", event_id):
            raise HTTPException(status_code=404, detail="Not found")
        record = await store_op("get", application.state.store.get, event_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Not found")
        return _public_metadata(record)

    return application


# Uvicorn imports this module in production. Missing or invalid production
# environment therefore fails at startup, while tests can call create_app with
# an explicit Settings object and injected queue doubles.
app = create_app()
