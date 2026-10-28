import asyncio
import hmac
import logging
import os
import re
import time
import unicodedata
import uuid
from contextlib import asynccontextmanager

from pydantic import BaseModel, ConfigDict
from app import outbox
from collections import deque
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("index-bridge")
# Avoid httpx/httpcore logging full webhook URLs (webhook URLs are sensitive identifiers).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


BRIDGE_TOKEN = os.environ["BRIDGE_TOKEN"]
GROKBOT_WEBHOOK_URL = os.environ["GROKBOT_WEBHOOK_URL"]
GROKBOT_WEBHOOK_KEY = os.environ["GROKBOT_WEBHOOK_KEY"]
SECURITY_ALERT_WEBHOOK_URL = os.getenv("SECURITY_ALERT_WEBHOOK_URL", "").strip()
SECURITY_ALERT_WEBHOOK_KEY = os.getenv("SECURITY_ALERT_WEBHOOK_KEY", "").strip()
REQUEST_TIMEOUT = httpx.Timeout(connect=10, read=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "45")), write=10, pool=10)
MAX_REQUEST_BYTES = int(os.getenv("MAX_REQUEST_BYTES", "65536"))
MAX_TRANSCRIPTION_CHARS = int(os.getenv("MAX_TRANSCRIPTION_CHARS", "8000"))
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
AUTH_FAILURE_ALERT_THRESHOLD = int(os.getenv("AUTH_FAILURE_ALERT_THRESHOLD", "10"))
LOG_TRANSCRIPTIONS = os.getenv("LOG_TRANSCRIPTIONS", "false").lower() == "true"

for name, url in (("GROKBOT_WEBHOOK_URL", GROKBOT_WEBHOOK_URL), ("SECURITY_ALERT_WEBHOOK_URL", SECURITY_ALERT_WEBHOOK_URL)):
    if url and urlparse(url).scheme.lower() != "https":
        raise RuntimeError(f"{name} must use https://")
if len(BRIDGE_TOKEN) < 32:
    raise RuntimeError("BRIDGE_TOKEN must be at least 32 characters")

accepted = deque()
auth_failures = deque()
last_alert = 0.0
state_lock = asyncio.Lock()
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

# --- Delivery: keep accepted captures on disk until the downstream acknowledges them. ---
class CapturePayload(BaseModel):
    model_config = ConfigDict(strict=True)
    source: str
    transcription: str
    recordedAt: str | None
    client: str
    bridgeRequestId: str


def payload_summary(payload):
    return {key: {"type": type(value).__name__, "length": len(value) if isinstance(value, str) else None} for key, value in payload.items()}


def safe_response(response, payload, secret):
    # A downstream may echo inputs or credentials; never write those values to logs.
    body = response.text[:4096]
    for value in list(payload.values()) + [secret]:
        if isinstance(value, str) and value:
            body = body.replace(value, "[redacted]")
    return repr(body[:1024])


async def deliver(client, url, key, payload, request_id, attempt, kind):
    headers = {"Content-Type": "application/json", "Idempotency-Key": request_id}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    started = time.monotonic()
    try:
        response = await client.post(url, headers=headers, json=payload)
    except httpx.RequestError as exc:
        log.warning("delivery kind=%s request_id=%s attempt=%d exception=%s elapsed_ms=%d", kind, request_id, attempt, type(exc).__name__, round((time.monotonic()-started)*1000))
        log.debug("delivery traceback", exc_info=True)
        return False, True, type(exc).__name__, None
    status = response.status_code
    log.info("delivery kind=%s request_id=%s attempt=%d status=%d elapsed_ms=%d", kind, request_id, attempt, status, round((time.monotonic()-started)*1000))
    if not 200 <= status < 300:
        log.warning("delivery_rejected kind=%s request_id=%s status=%d body=%s payload_summary=%s", kind, request_id, status, safe_response(response, payload, key), payload_summary(payload))
    return 200 <= status < 300, status in (408, 429) or status >= 500, f"HTTP {status}", response.headers.get("Retry-After")


async def outbox_worker():
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while True:
            try:
                item = outbox.next_item()
                if item is None:
                    await asyncio.sleep(1)
                    continue
                request_id, encoded, attempts = item
                import json
                payload = json.loads(encoded)
                attempts += 1
                ok, retryable, result, retry_after = await deliver(client, GROKBOT_WEBHOOK_URL, GROKBOT_WEBHOOK_KEY, payload, request_id, attempts, "capture")
                state = "delivered" if ok else "pending" if retryable and attempts < outbox.MAX_ATTEMPTS else "dead"
                outbox.finish(request_id, attempts, state, result, outbox.retry_delay(attempts, retry_after) if state == "pending" else 0)
                if state == "dead":
                    log.error("delivery_dead request_id=%s reason=%s", request_id, result)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.error("outbox_worker_error; retrying", exc_info=log.isEnabledFor(logging.DEBUG))
                await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app):
    # Fail startup if the persistent volume is unavailable, rather than accept data we cannot save.
    with outbox.connect():
        pass
    worker = asyncio.create_task(outbox_worker())
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Pebble Index Bridge", version="0.2.0", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)


async def send_security_alert(event: str, severity: str, count: int, action: str):
    global last_alert
    if not SECURITY_ALERT_WEBHOOK_URL:
        return
    now = time.monotonic()
    if now - last_alert < 300:
        return
    last_alert = now
    request_id = str(uuid.uuid4())
    payload = {"source": "pebble-index-bridge-security", "event": event, "severity": severity, "count": count, "action": action, "message": f"Index Bridge security alert: {event}. Severity {severity}. Count {count}. {action}"}
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        for attempt in range(1, 4):
            ok, retryable, _, retry_after = await deliver(client, SECURITY_ALERT_WEBHOOK_URL, SECURITY_ALERT_WEBHOOK_KEY, payload, request_id, attempt, "security_alert")
            if ok or not retryable or attempt == 3:
                return
            await asyncio.sleep(outbox.retry_delay(attempt, retry_after))

async def record_auth_failure():
    now = time.monotonic()
    async with state_lock:
        auth_failures.append(now)
        while auth_failures and auth_failures[0] < now - 300:
            auth_failures.popleft()
        count = len(auth_failures)
    log.warning("security_event=auth_failure count_5m=%d", count)
    if count >= AUTH_FAILURE_ALERT_THRESHOLD:
        asyncio.create_task(send_security_alert("repeated_auth_failure", "HIGH", count, "Requests are being rejected; investigate logs and disable Funnel if activity persists."))

async def enforce_rate_limit():
    now = time.monotonic()
    async with state_lock:
        while accepted and accepted[0] < now - RATE_LIMIT_WINDOW_SECONDS:
            accepted.popleft()
        if len(accepted) >= RATE_LIMIT_REQUESTS:
            count = len(accepted)
            log.warning("security_event=rate_limit count_window=%d", count)
            asyncio.create_task(send_security_alert("authenticated_rate_limit", "HIGH", count, "Authenticated request volume exceeded the configured limit."))
            raise HTTPException(status_code=429, detail="Too many requests")
        accepted.append(now)

@app.middleware("http")
async def request_gate(request: Request, call_next):
    if request.url.path != "/index":
        return JSONResponse({"detail": "Not found"}, status_code=404)
    if request.method != "POST":
        return JSONResponse({"detail": "Method not allowed"}, status_code=405)
    content_type = request.headers.get("content-type", "").lower()
    if not content_type.startswith("multipart/form-data;"):
        log.warning("security_event=invalid_content_type")
        return JSONResponse({"detail": "Unsupported media type"}, status_code=415)
    content_length = request.headers.get("content-length")
    if not content_length:
        return JSONResponse({"detail": "Content-Length required"}, status_code=411)
    try:
        length = int(content_length)
    except ValueError:
        return JSONResponse({"detail": "Invalid Content-Length"}, status_code=400)
    if length < 1 or length > MAX_REQUEST_BYTES:
        log.warning("security_event=request_size_rejected bytes=%s", content_length)
        return JSONResponse({"detail": "Request too large"}, status_code=413)
    return await call_next(request)

@app.post("/index")
async def index_webhook(request: Request, authorization: str | None = Header(default=None)):
    expected = f"Bearer {BRIDGE_TOKEN}"
    if not authorization or not hmac.compare_digest(authorization, expected):
        await record_auth_failure()
        raise HTTPException(status_code=401, detail="Unauthorized")

    await enforce_rate_limit()
    try:
        form = await request.form(max_files=0, max_fields=3, max_part_size=MAX_REQUEST_BYTES)
    except Exception:
        log.warning("security_event=malformed_multipart")
        raise HTTPException(status_code=400, detail="Malformed request")

    allowed = {"transcription", "recordedAt", "client"}
    if any(k not in allowed for k in form.keys()):
        log.warning("security_event=unexpected_form_field")
        raise HTTPException(status_code=400, detail="Unexpected form field")

    raw = form.get("transcription")
    if not isinstance(raw, str):
        raise HTTPException(status_code=400, detail="Missing transcription")
    text = unicodedata.normalize("NFC", raw).strip()
    if not text or len(text) > MAX_TRANSCRIPTION_CHARS or CONTROL_RE.search(text):
        log.warning("security_event=invalid_transcription chars=%d", len(text))
        raise HTTPException(status_code=400, detail="Invalid transcription")

    recorded_at = form.get("recordedAt")
    client_name = form.get("client") or "ring"
    if recorded_at is not None and (not isinstance(recorded_at, str) or len(recorded_at) > 32 or not recorded_at.isdigit()):
        raise HTTPException(status_code=400, detail="Invalid recordedAt")
    if not isinstance(client_name, str) or len(client_name) > 32 or CONTROL_RE.search(client_name):
        raise HTTPException(status_code=400, detail="Invalid client")

    request_id = str(uuid.uuid4())
    log.info("capture request_id=%s client=%s recordedAt=%s chars=%d", request_id, client_name, recorded_at, len(text))
    if LOG_TRANSCRIPTIONS:
        log.info("transcription request_id=%s text=%r", request_id, text)

    payload = CapturePayload(source="pebble-index-01", transcription=text, recordedAt=recorded_at, client=client_name, bridgeRequestId=request_id).model_dump()
    try:
        outbox.enqueue(request_id, payload)
    except Exception:
        log.error("outbox_enqueue_failed request_id=%s", request_id, exc_info=log.isEnabledFor(logging.DEBUG))
        raise HTTPException(status_code=503, detail="Could not persist capture")
    return JSONResponse({"ok": True, "requestId": request_id, "delivery": "queued"}, status_code=202)
