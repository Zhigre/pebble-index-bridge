"""Parallel routing contracts with synthetic transcripts and mocked transports."""
import asyncio
import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient
from app.secondary_outbox import SecondaryOutbox
from app.secondary_worker import deliver_copy

REAL_ASYNC_CLIENT = httpx.AsyncClient


async def idle_worker(*args):
    await asyncio.Event().wait()


class ParallelTests(unittest.TestCase):
    def test_primary_and_copy_failure_are_independent(self):
        for scenario in ("normal", "grok_failure", "copy_failure", "startup_store_failure", "disabled"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                filename = str(Path(directory) / "copies.sqlite")
                env = {"BRIDGE_TOKEN": "b" * 40, "GROKBOT_WEBHOOK_URL": "https://grok.example/capture", "GROKBOT_WEBHOOK_KEY": "fixture-key", "SECONDARY_WEBHOOK_URL": "" if scenario == "disabled" else "https://secondary.example/capture", "SECONDARY_WEBHOOK_TOKEN": "" if scenario == "disabled" else "d" * 40, "SECONDARY_OUTBOX_PATH": filename}
                requests = []

                def transport(request):
                    requests.append(request)
                    return httpx.Response(500 if scenario == "grok_failure" else 200)

                with patch.dict(os.environ, env):
                    import app.main as main
                    importlib.reload(main)
                factory = lambda **kwargs: REAL_ASYNC_CLIENT(transport=httpx.MockTransport(transport), **kwargs)
                with patch.object(main, "run_worker", idle_worker), patch.object(main.httpx, "AsyncClient", side_effect=factory), patch.object(main, "SecondaryOutbox", side_effect=OSError("fixture startup disk error")) if scenario == "startup_store_failure" else patch.object(main, "LOG_TRANSCRIPTIONS", False):
                    with TestClient(main.app) as client:
                        with patch.object(SecondaryOutbox, "enqueue", side_effect=OSError("fixture disk error")) if scenario == "copy_failure" else patch.object(main, "LOG_TRANSCRIPTIONS", False):
                            response = client.post("/index", headers={"Authorization": "Bearer " + "b" * 40}, files={"transcription": (None, "  Exact Pebble words  "), "recordedAt": (None, "1791760000000"), "client": (None, "ring")})
                self.assertEqual(response.status_code, 502 if scenario == "grok_failure" else 200)
                self.assertEqual(len(requests), 1, "SECONDARY worker never repeats the primary send")
                self.assertEqual(requests[0].headers["authorization"], "Bearer fixture-key")
                import json
                payload = json.loads(requests[0].content)
                self.assertEqual(payload["transcription"], "Exact Pebble words", "preserve existing bridge normalization")
                if scenario == "disabled":
                    self.assertNotIn("secondaryCopy", response.json())
                elif scenario in ("copy_failure", "startup_store_failure"):
                    self.assertEqual(response.json()["secondaryCopy"]["status"], "not_queued")
                else:
                    with sqlite3.connect(filename) as db:
                        queued = json.loads(db.execute("SELECT payload FROM copies").fetchone()[0])
                    self.assertEqual(queued, payload, "both agents receive the same captured payload")


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_retry_keeps_identity_and_requires_completed_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            outbox = SecondaryOutbox(str(Path(directory) / "copies.sqlite"))
            payload = {"source": "pebble-index-01", "transcription": "Same words", "bridgeRequestId": "fixture-request-id"}
            identity = outbox.enqueue(payload)
            requests = []

            def timeout(request):
                requests.append(request)
                raise httpx.ReadTimeout("fixture", request=request)

            factory = lambda **kwargs: REAL_ASYNC_CLIENT(transport=httpx.MockTransport(timeout), **kwargs)
            with patch("app.secondary_worker.httpx.AsyncClient", side_effect=factory):
                await deliver_copy(outbox, "https://secondary.example/capture", "fixture", 1)
            self.assertEqual(outbox.db.execute("SELECT status FROM copies").fetchone()[0], "pending")
            outbox.db.execute("UPDATE copies SET next_attempt=0")
            outbox.db.commit()

            def completed(request):
                requests.append(request)
                return httpx.Response(200, json={"messageId": identity, "status": "completed"})

            factory = lambda **kwargs: REAL_ASYNC_CLIENT(transport=httpx.MockTransport(completed), **kwargs)
            with patch("app.secondary_worker.httpx.AsyncClient", side_effect=factory):
                await deliver_copy(outbox, "https://secondary.example/capture", "fixture", 1)
            self.assertEqual(requests[0].content, requests[1].content)
            self.assertEqual(outbox.db.execute("SELECT status FROM copies").fetchone()[0], "completed")
            self.assertFalse(await deliver_copy(outbox, "https://secondary.example/capture", "fixture", 1))
            outbox.close()


if __name__ == "__main__":
    unittest.main()
