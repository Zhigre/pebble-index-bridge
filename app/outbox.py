"""Durable, single-worker delivery queue and local operator CLI."""
import argparse
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

DB_PATH = os.getenv("OUTBOX_PATH", "/data/outbox.sqlite3")
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "12"))

# Each operation commits independently, so accepted captures survive a process restart.
def connect():
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL, last_result TEXT)")
    return db


def enqueue(request_id, payload):
    with connect() as db:
        db.execute("INSERT INTO outbox (id, payload, state, next_attempt) VALUES (?, ?, 'pending', ?)", (request_id, json.dumps(payload, ensure_ascii=False), time.time()))


def next_item():
    with connect() as db:
        return db.execute("SELECT id, payload, attempts FROM outbox WHERE state='pending' AND next_attempt <= ? ORDER BY next_attempt LIMIT 1", (time.time(),)).fetchone()


def finish(request_id, attempts, state, result, delay=0):
    with connect() as db:
        db.execute("UPDATE outbox SET state=?, attempts=?, last_result=?, next_attempt=? WHERE id=?", (state, attempts, result, time.time() + delay, request_id))


def retry_delay(attempt, retry_after=None):
    delay = min(300, 2 ** min(attempt, 8)) + random.uniform(0, 1)
    if retry_after:
        try:
            wait = float(retry_after)
        except ValueError:
            try:
                wait = (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                wait = 0
        delay = max(delay, wait)
    return delay


def main():
    parser = argparse.ArgumentParser(description="Inspect or replay the local delivery outbox")
    parser.add_argument("command", choices=["list", "replay"])
    parser.add_argument("request_id", nargs="?")
    args = parser.parse_args()
    with connect() as db:
        if args.command == "list":
            for row in db.execute("SELECT id, state, attempts, next_attempt, last_result FROM outbox WHERE state != 'delivered' ORDER BY next_attempt"):
                print(json.dumps(dict(zip(("request_id", "state", "attempts", "next_attempt", "last_result"), row))))
        elif args.request_id:
            cursor = db.execute("UPDATE outbox SET state='pending', attempts=0, next_attempt=?, last_result=NULL WHERE id=? AND state='dead'", (time.time(), args.request_id))
            if not cursor.rowcount:
                parser.error("no dead-letter item with that request ID")
        else:
            parser.error("replay requires a request ID")


if __name__ == "__main__":
    main()
