"""Durable, single-worker delivery queue and local operator CLI."""
import argparse
import json
import os
import random
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

DB_PATH = os.getenv("OUTBOX_PATH", "/data/outbox.sqlite3")
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "12"))

# Each operation commits independently, so accepted captures survive a process restart.
@contextmanager
def connect():
    descriptor = os.open(DB_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(descriptor)
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS deliveries (id TEXT NOT NULL, destination TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL, last_result TEXT, PRIMARY KEY (id, destination))")
    # Upgrade v0.2.0 in one transaction without losing pending primary deliveries.
    with db:
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox'").fetchone():
            db.execute("INSERT OR IGNORE INTO deliveries SELECT id, 'grokbot', payload, state, attempts, next_attempt, last_result FROM outbox")
            db.execute("DROP TABLE outbox")
    try:
        with db:
            yield db
    finally:
        db.close()


def enqueue(request_id, payload, destinations=("grokbot",)):
    # Fan-out is atomic: acknowledge only when every configured copy is durable.
    with connect() as db:
        db.executemany("INSERT INTO deliveries (id, destination, payload, state, next_attempt) VALUES (?, ?, ?, 'pending', ?)", [(request_id, name, json.dumps(payload, ensure_ascii=False), time.time()) for name in destinations])


def next_item(destination="grokbot"):
    with connect() as db:
        return db.execute("SELECT id, payload, attempts FROM deliveries WHERE destination=? AND state='pending' AND next_attempt <= ? ORDER BY next_attempt LIMIT 1", (destination, time.time())).fetchone()


def finish(request_id, attempts, state, result, delay=0, destination="grokbot"):
    with connect() as db:
        db.execute("UPDATE deliveries SET state=?, attempts=?, last_result=?, next_attempt=? WHERE id=? AND destination=?", (state, attempts, result, time.time() + delay, request_id, destination))


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
    parser.add_argument("--destination", help="Restrict replay to one destination")
    args = parser.parse_args()
    with connect() as db:
        if args.command == "list":
            for row in db.execute("SELECT id, destination, state, attempts, next_attempt, last_result FROM deliveries WHERE state != 'delivered' ORDER BY next_attempt"):
                print(json.dumps(dict(zip(("request_id", "destination", "state", "attempts", "next_attempt", "last_result"), row))))
        elif args.request_id:
            cursor = db.execute("UPDATE deliveries SET state='pending', attempts=0, next_attempt=?, last_result=NULL WHERE id=? AND state='dead' AND (? IS NULL OR destination=?)", (time.time(), args.request_id, args.destination, args.destination))
            if not cursor.rowcount:
                parser.error("no dead-letter item with that request ID")
        else:
            parser.error("replay requires a request ID")


if __name__ == "__main__":
    main()
