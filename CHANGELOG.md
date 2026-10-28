# Changelog

## 0.2.0

- Persist captures in a Docker-backed SQLite outbox and deliver asynchronously, with retry, dead-letter inspection and replay.
- Extend downstream timeouts, send stable idempotency keys, and log delivery outcomes and redacted rejection diagnostics.
- Retry transient security-alert delivery failures and document delivery and duplicate-delivery limitations.
