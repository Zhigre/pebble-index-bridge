"""Durable optional transcript copies; GrokBot delivery remains independent.

The caller stores a copy before forwarding to GrokBot. A separate worker retries
only SECONDARY, using the same message identity after timeouts and process restarts.
Never log bodies, credentials or destination URLs.
"""
import hashlib
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path


class SecondaryOutbox:
    def __init__(self, filename: str):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        # Transcripts are personal data. Secure the database before SQLite can
        # create journal/WAL files, which inherit the database's permissions.
        descriptor = os.open(filename, os.O_CREAT | os.O_RDWR, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        self.db = sqlite3.connect(filename)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS copies (
            id TEXT PRIMARY KEY, recording_key TEXT UNIQUE NOT NULL,
            payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            last_status INTEGER, created_at REAL NOT NULL)""")
        self.db.commit()

    def enqueue(self, payload: dict, delivery_id: str | None = None) -> str:
        # Prefer the ring's recording identity. Legacy unsigned requests can be
        # deduplicated only when recordedAt is present; text alone is never an ID.
        identity = (
            ["delivery", delivery_id] if delivery_id else
            ["recording", payload.get("client"), payload["recordedAt"], payload["transcription"]]
            if payload.get("recordedAt") else ["request", payload["bridgeRequestId"]]
        )
        key = hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()
        encoded = json.dumps(payload, ensure_ascii=False)
        with self.db:
            row = self.db.execute("SELECT id, payload FROM copies WHERE recording_key=?", (key,)).fetchone()
            if row:
                previous = json.loads(row[1])
                if any(previous.get(k) != payload.get(k) for k in ("transcription", "recordedAt", "client")):
                    raise ValueError("Recording identity reused for different content")
                return row[0]
            if self.db.execute("SELECT COUNT(*) FROM copies").fetchone()[0] >= 10000:
                raise RuntimeError("Copy store capacity reached; archive reviewed records")
            message_id = "pebble_" + uuid.uuid4().hex
            self.db.execute("INSERT INTO copies(id, recording_key, payload, created_at) VALUES(?,?,?,?)",
                            (message_id, key, encoded, time.time()))
        return message_id

    def next_due(self, now: float | None = None):
        row = self.db.execute("SELECT id,payload,attempts FROM copies WHERE status='pending' AND next_attempt<=? ORDER BY created_at LIMIT 1",
                              (time.time() if now is None else now,)).fetchone()
        return (row[0], json.loads(row[1]), row[2]) if row else None

    def finish_attempt(self, message_id: str, status: int | None, completed: bool = False):
        # Authentication, validation and identity conflicts need operator review.
        # Transport failures/5xx/429 remain retryable with capped backoff. A
        # successful HTTP response is not proof that the coaching turn completed.
        row = self.db.execute("SELECT attempts FROM copies WHERE id=?", (message_id,)).fetchone()
        if not row:
            raise ValueError("Unknown copy identity")
        attempts = row[0] + 1
        outcome = "completed" if completed else "attention" if status is not None and 200 <= status < 500 and status != 429 else "pending"
        with self.db:
            self.db.execute("UPDATE copies SET status=?,attempts=?,last_status=?,next_attempt=? WHERE id=?",
                            (outcome, attempts, status, time.time() + min(300, 2 ** min(attempts, 9)), message_id))

    def close(self):
        self.db.close()
