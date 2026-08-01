from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qq_image_console.remote import (
    RemoteAuthenticationError,
    RemoteIdentity,
    RemoteSessionStore,
    SnapshotPublisher,
)


class _Response:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class RemoteSecurityTests(unittest.TestCase):
    def test_remote_session_rejects_wrong_csrf(self) -> None:
        identity = RemoteIdentity(subject="one", email="owner@example.com")
        store = RemoteSessionStore()
        session = store.create(identity)
        with self.assertRaises(RemoteAuthenticationError):
            store.validate(session.token, "wrong", identity)
        validated = store.validate(session.token, session.csrf_token, identity)
        self.assertEqual(validated.identity, identity)

    def test_snapshot_publisher_signs_exact_bounded_body(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            secret_file = Path(temporary) / "snapshot.secret"
            secret_file.write_text("s" * 64, encoding="ascii")
            captured = {}

            def fake_open(request, timeout):
                captured["request"] = request
                captured["timeout"] = timeout
                return _Response()

            publisher = SnapshotPublisher(
                "https://status-ingest.example.com/api/ingest",
                secret_file,
                lambda: {"schema_version": 1, "generated_at": 123, "statistics": {}},
            )
            with patch("urllib.request.urlopen", side_effect=fake_open):
                publisher.publish_once()
            request = captured["request"]
            body = request.data
            self.assertEqual(json.loads(body)["schema_version"], 1)
            timestamp = request.headers["X-qqic-timestamp"]
            nonce = request.headers["X-qqic-nonce"]
            expected = hmac.new(
                ("s" * 64).encode("ascii"),
                timestamp.encode("ascii") + b"." + nonce.encode("ascii") + b"." + body,
                hashlib.sha256,
            ).hexdigest()
            self.assertEqual(request.headers["X-qqic-signature"], expected)


if __name__ == "__main__":
    unittest.main()
