# Changelog

## 0.3.0

- Integrate the secondary-destination branch as configurable multi-destination forwarding, without deployment-specific configuration.
- Queue each destination atomically with independent workers, retries, credentials, and selective replay.
- Support optional completed receipts and preserve legacy Grok Bot configuration and queued captures.
- Normalize container source permissions for non-root execution.

## 0.2.0

- Persist captures in a Docker-backed SQLite outbox and deliver asynchronously, with retry, dead-letter inspection and replay.
- Extend downstream timeouts, send stable idempotency keys, and log delivery outcomes and redacted rejection diagnostics.
- Retry transient security-alert delivery failures and document delivery and duplicate-delivery limitations.
