"""Copy reliability contracts: disk persistence, identity and retry honesty."""
import tempfile
import unittest
from pathlib import Path
from app.secondary_outbox import SecondaryOutbox


class OutboxTests(unittest.TestCase):
    def test_restart_replay_and_distinct_recordings(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = str(Path(directory) / "outbox.sqlite")
            payload = {"transcription": "I brushed my teeth", "recordedAt": "1791760000000", "client": "ring", "bridgeRequestId": "request-one"}
            outbox = SecondaryOutbox(filename)
            identity = outbox.enqueue(payload)
            outbox.close()
            outbox = SecondaryOutbox(filename)
            self.assertEqual(identity, outbox.enqueue({**payload, "bridgeRequestId": "retry"}))
            self.assertEqual(identity, outbox.next_due()[0])
            outbox.finish_attempt(identity, None)
            self.assertIsNone(outbox.next_due())
            self.assertEqual(identity, outbox.next_due(float("inf"))[0])
            outbox.finish_attempt(identity, 200, completed=True)
            self.assertIsNone(outbox.next_due(float("inf")))
            self.assertEqual(identity, outbox.enqueue(payload))
            self.assertNotEqual(identity, outbox.enqueue({**payload, "recordedAt": "1791760000001"}))
            outbox.close()

    def test_conflict_and_attention_are_not_success(self):
        with tempfile.TemporaryDirectory() as directory:
            outbox = SecondaryOutbox(str(Path(directory) / "outbox.sqlite"))
            payload = {"transcription": "First", "recordedAt": "1", "client": "ring", "bridgeRequestId": "one"}
            identity = outbox.enqueue(payload, "ring-recording-one")
            with self.assertRaises(ValueError):
                outbox.enqueue({**payload, "transcription": "Changed"}, "ring-recording-one")
            outbox.finish_attempt(identity, 403)
            self.assertIsNone(outbox.next_due(float("inf")))
            self.assertEqual("attention", outbox.db.execute("SELECT status FROM copies WHERE id=?", (identity,)).fetchone()[0])
            outbox.close()

    def test_missing_timestamp_does_not_collapse_repeated_speech(self):
        with tempfile.TemporaryDirectory() as directory:
            outbox = SecondaryOutbox(str(Path(directory) / "outbox.sqlite"))
            payload = {"transcription": "Again", "bridgeRequestId": "one"}
            first = outbox.enqueue(payload)
            self.assertNotEqual(first, outbox.enqueue({**payload, "bridgeRequestId": "two"}))
            outbox.finish_attempt(first, 200)
            self.assertEqual("attention", outbox.db.execute("SELECT status FROM copies WHERE id=?", (first,)).fetchone()[0])
            outbox.close()


if __name__ == "__main__":
    unittest.main()
