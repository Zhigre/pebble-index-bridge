"""Independent copy worker: retry SECONDARY only; never repeat GrokBot sends."""
import asyncio
import logging
import httpx

log = logging.getLogger("index-bridge")


async def deliver_copy(outbox, url, token, timeout):
    item = outbox.next_due()
    if not item:
        return False
    message_id, payload, _ = item
    status = None
    completed = False
    try:
        # Do not follow redirects with credentials. The durable ID makes a retry
        # safe even when SECONDARY executed but the HTTP acknowledgement was lost.
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
            response = await client.post(url, headers={"Authorization": f"Bearer {token}"},
                                         json={**payload, "messageId": message_id})
        status = response.status_code
        if status == 200:
            receipt = response.json()
            completed = receipt.get("messageId") == message_id and receipt.get("status") == "completed"
    except (httpx.RequestError, ValueError, TypeError, AttributeError):
        # Never log exception text: URLs and transcript bodies may be embedded.
        pass
    outbox.finish_attempt(message_id, status, completed)
    log.info("secondary_copy message_id=%s http_status=%s completed=%s", message_id, status, completed)
    return True


async def run_worker(outbox, url, token, timeout):
    while True:
        try:
            if not await deliver_copy(outbox, url, token, timeout):
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.error("secondary_copy_worker_error; pending copies retained")
            await asyncio.sleep(5)
