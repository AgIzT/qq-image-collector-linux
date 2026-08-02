from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from websockets.exceptions import ConnectionClosedOK

from collector_control import set_setting
from linux.bootstrap import prepare
from linux.cache_cleanup import cleanup_once
from linux import event_probe
from linux.event_probe import receive_sample
from linux.telemetry_report import report as telemetry_report
from linux import url_lifecycle_probe
from qq_image_collector.database import connect_database


class LinuxEventAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        (self.root / ".env.example").write_text("QQAI_RUNTIME_ROOT=./runtime\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_bootstrap_reconciles_http_ws_and_preserves_manager_access(self) -> None:
        results = prepare(self.root, ["100000001"], runtime_root=self.runtime)
        self.assertTrue(results["onebot_token"])
        token = (self.runtime / "napcat-config" / "collector.onebot.token").read_text(encoding="utf-8").strip()
        config = json.loads((self.runtime / "napcat-config" / "onebot11.json").read_text(encoding="utf-8"))
        http = config["network"]["httpServers"][0]
        ws = config["network"]["websocketServers"][0]
        self.assertEqual((http["host"], http["port"], http["debug"]), ("0.0.0.0", 3000, True))
        self.assertEqual((ws["host"], ws["port"], ws["debug"]), ("0.0.0.0", 3001, True))
        self.assertEqual(http["token"], token)
        self.assertEqual(ws["token"], token)

        collector_path = self.runtime / "repository" / "config" / "collector_config.json"
        collector = json.loads(collector_path.read_text(encoding="utf-8"))
        self.assertNotIn("qce", collector)
        self.assertEqual(collector["groups"], ["100000001"])
        self.assertEqual(collector["runtime"]["daily_download_limit"], 3000)

        # 600 was the old production default.  A second prepare must upgrade
        # that exact value without requiring a fresh runtime directory.
        collector["runtime"]["daily_download_limit"] = 600
        collector_path.write_text(json.dumps(collector), encoding="utf-8")

        manager_path = self.runtime / "manager" / "manager_config.json"
        manager = json.loads(manager_path.read_text(encoding="utf-8"))
        manager["direct_public_enabled"] = True
        manager["direct_public_hosts"] = ["status.example.invalid"]
        manager_path.write_text(json.dumps(manager), encoding="utf-8")
        account_config = self.runtime / "napcat-config" / "onebot11_300000003.json"
        account_config.write_text("{}", encoding="utf-8")
        (self.runtime / "napcat-config" / "plugins.json").write_text(
            json.dumps({"napcat-plugin-qce": True, "other": True}), encoding="utf-8"
        )
        prepare(self.root, [], runtime_root=self.runtime)
        collector_after = json.loads(collector_path.read_text(encoding="utf-8"))
        self.assertEqual(collector_after["runtime"]["daily_download_limit"], 3000)
        manager_after = json.loads(manager_path.read_text(encoding="utf-8"))
        self.assertTrue(manager_after["direct_public_enabled"])
        self.assertEqual(manager_after["direct_public_hosts"], ["status.example.invalid"])
        self.assertEqual(json.loads(account_config.read_text(encoding="utf-8")), config)
        plugins = json.loads((self.runtime / "napcat-config" / "plugins.json").read_text(encoding="utf-8"))
        self.assertNotIn("napcat-plugin-qce", plugins)
        self.assertTrue(plugins["other"])

    def test_compose_is_private_and_pinned(self) -> None:
        compose = (Path(__file__).parent / "linux" / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("sha256:e66a6e52", compose)
        self.assertEqual(compose.count("qq-ai-image-collector-console:1.1.3-event"), 2)
        self.assertNotIn(":3000\"", compose)
        self.assertNotIn(":3001\"", compose)
        self.assertNotIn("40653", compose)
        self.assertIn("ss.xingzhige.com:127.0.0.1", compose)
        self.assertIn("secret-service.bietiaop.com:127.0.0.1", compose)
        self.assertIn("restart: unless-stopped", compose)

    def test_cleanup_never_touches_nt_db_or_final(self) -> None:
        account = self.runtime / "qq-session" / "300000003"
        stale_pic = account / "nt_data" / "Pic" / "stale.png"
        database = account / "nt_db" / "messages.db"
        final = self.runtime / "repository" / "final" / "NovelAI" / "keep.png"
        part = self.runtime / "repository" / "temp" / "old.part"
        for path in (stale_pic, database, final, part):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"data")
        old = time.time() - 10 * 3600
        os.utime(stale_pic, (old, old))
        os.utime(database, (old, old))
        os.utime(final, (old, old))
        os.utime(part, (old, old))
        args = argparse.Namespace(
            session_root=self.runtime / "qq-session",
            account=None,
            napcat_log_root=None,
            collector_temp_root=self.runtime / "repository" / "temp",
            collector_state_root=None,
            legacy_qce_root=None,
            short_keep_hours=2,
            media_keep_hours=24,
            log_keep_hours=48,
            legacy_keep_hours=168,
            loop_hours=0,
            apply=True,
        )
        cleanup_once(args)
        self.assertFalse(stale_pic.exists())
        self.assertFalse(part.exists())
        self.assertTrue(database.exists())
        self.assertTrue(final.exists())

        global_cache = self.runtime / "qq-session" / "nt_qq" / "global" / "nt_data" / "Pic" / "old.png"
        global_cache.parent.mkdir(parents=True, exist_ok=True)
        global_cache.write_bytes(b"old")
        os.utime(global_cache, (old, old))
        cleanup_once(args)
        self.assertFalse(global_cache.exists())

    def test_production_source_has_no_callable_get_image_or_qce_client(self) -> None:
        root = Path(__file__).parent
        production = "\n".join(
            path.read_text(encoding="utf-8")
            for path in [root / "collector.py", *(root / "qq_image_collector").glob("*.py")]
        )
        self.assertNotIn('call("get_image"', production)
        self.assertNotIn("QCEClient", production)

    def test_manage_container_python_commands_have_application_import_path(self) -> None:
        manage = (Path(__file__).parent / "linux" / "manage.sh").read_text(
            encoding="utf-8"
        )
        marker = "docker compose exec -T -e PYTHONPATH=/app"
        expected_commands = (
            "collector-console \\\n      python /app/linux/event_probe.py",
            "collector-console \\\n      python /app/linux/diagnostic_compare.py",
            "collector-console \\\n      python /app/linux/url_lifecycle_probe.py",
            "collector-console \\\n      python /app/linux/telemetry_report.py",
            "cache-cleaner python /app/linux/cache_cleanup.py",
        )
        for command in expected_commands:
            with self.subTest(command=command):
                self.assertIn(f"{marker} {command}", manage)

        # Both diagnostic modes and both lifecycle modes share their scripts;
        # the count locks every manage.sh branch, not just each distinct file.
        self.assertEqual(manage.count(f"{marker} collector-console"), 6)
        self.assertEqual(manage.count(f"{marker} cache-cleaner"), 1)
        self.assertNotIn(
            "docker compose exec -T collector-console python /app/linux/", manage
        )
        self.assertIn("./audit_rkey_network.sh qqai-napcat", manage)
        probe_block = manage.split("  probe-event)", 1)[1].split(
            "  diagnose-original)", 1
        )[0]
        self.assertIn(
            '[[ -n "${2:-}" ]] || { echo "isolated test group id is required"',
            probe_block,
        )
        self.assertIn('args=(--group "$2" --image-segments "$segments"', probe_block)
        self.assertNotIn('[[ -z "${2:-}" ]] || args+=(--group "$2")', probe_block)

    def test_event_probe_cli_rejects_missing_group(self) -> None:
        with patch.object(sys, "argv", ["event_probe.py"]):
            with self.assertRaises(SystemExit) as raised:
                event_probe.main()
        self.assertEqual(raised.exception.code, 2)

    def test_event_probe_counts_unmatched_standard_and_raw_as_union(self) -> None:
        config = self.root / "collector.json"
        output = self.root / "probe.json"
        database = self.root / "probe.sqlite3"
        config.write_text(
            json.dumps(
                {
                    "onebot": {"ws_url": "ws://synthetic.invalid", "token": "test"},
                    "storage": {
                        "root": str(self.root / "repository"),
                        "database": str(self.root / "repository" / "state.sqlite3"),
                    },
                }
            ),
            encoding="utf-8",
        )
        event = {
            "post_type": "message",
            "message_type": "group",
            "group_id": 100000001,
            "message": [
                {
                    "type": "image",
                    "data": {
                        "file": "standard-only.png",
                        "url": "https://gchat.qpic.cn/standard?rkey=x",
                    },
                }
            ],
            "raw": {
                "elements": [
                    {
                        "picElement": {
                            "fileName": "raw-only.png",
                            "originImageUrl": "https://gchat.qpic.cn/raw?rkey=y",
                        }
                    }
                ]
            },
        }

        class FakeWebSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def recv(self):
                return json.dumps(event)

        with patch("linux.event_probe.websockets.connect", return_value=FakeWebSocket()):
            code = asyncio.run(
                receive_sample(config, "100000001", 2, 5, output, database)
            )

        payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(code, 0)
        self.assertEqual(payload["independently_matched_pairs"], 0)
        self.assertEqual(payload["captured_estimated_image_slots"], 2)
        self.assertEqual(payload["standard_without_raw_match"], 1)
        self.assertEqual(payload["raw_without_standard_match"], 1)

    def test_event_probe_checkpoints_resumes_and_deduplicates_privately(self) -> None:
        config = self.root / "collector-resume.json"
        output = self.root / "probe-resume.json"
        database = self.root / "probe-resume.sqlite3"
        group = "987654321012345"
        secret_url = "https://gchat.qpic.cn/private-object?rkey=NEVER_STORE_THIS"
        config.write_text(
            json.dumps(
                {
                    "onebot": {"ws_url": "ws://synthetic.invalid", "token": "test"},
                    "storage": {
                        "root": str(self.root / "repository"),
                        "database": str(self.root / "production.sqlite3"),
                    },
                }
            ),
            encoding="utf-8",
        )

        def event(message_id: str, url: str) -> dict:
            return {
                "post_type": "message",
                "message_type": "group",
                "group_id": int(group),
                "message_id": message_id,
                "message": [
                    {"type": "image", "data": {"file": "same.png", "url": url}}
                ],
                "raw": {
                    "msgId": message_id,
                    "elements": [
                        {
                            "picElement": {
                                "fileName": "same.png",
                                "originImageUrl": url,
                                "original": True,
                            }
                        }
                    ],
                },
            }

        first_event = event("1", secret_url)
        second_event = event("2", "https://gchat.qpic.cn/second?rkey=ALSO_SECRET")

        class FakeWebSocket:
            def __init__(self, values):
                self.values = iter(values)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def recv(self):
                value = next(self.values)
                if isinstance(value, BaseException):
                    raise value
                return json.dumps(value)

        first_socket = FakeWebSocket([first_event, asyncio.TimeoutError()])
        with patch("linux.event_probe.websockets.connect", return_value=first_socket):
            first_code = asyncio.run(
                receive_sample(config, group, 2, 1, output, database)
            )
        self.assertEqual(first_code, 2)
        partial = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(partial["captured_estimated_image_slots"], 1)
        self.assertTrue(partial["timed_out"])

        second_socket = FakeWebSocket([first_event, second_event])
        with patch("linux.event_probe.websockets.connect", return_value=second_socket):
            second_code = asyncio.run(
                receive_sample(config, group, 2, 1, output, database)
            )
        self.assertEqual(second_code, 0)
        completed = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(completed["captured_estimated_image_slots"], 2)
        self.assertEqual(completed["events_seen"], 2)
        self.assertTrue(completed["connection"]["resumed_from_checkpoint"])

        with sqlite3.connect(database) as checkpoint:
            self.assertEqual(
                checkpoint.execute("SELECT count(*) FROM probe_events").fetchone()[0], 2
            )
            self.assertEqual(
                checkpoint.execute("SELECT count(*) FROM probe_state").fetchone()[0], 1
            )
        database_bytes = database.read_bytes()
        output_bytes = output.read_bytes()
        for secret in (group, secret_url, "NEVER_STORE_THIS", "ALSO_SECRET"):
            self.assertNotIn(secret.encode(), database_bytes)
            self.assertNotIn(secret.encode(), output_bytes)
        if os.name != "nt":
            self.assertEqual(database.stat().st_mode & 0o777, 0o600)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)

        reset_socket = FakeWebSocket([first_event])
        with patch("linux.event_probe.websockets.connect", return_value=reset_socket):
            reset_code = asyncio.run(
                receive_sample(config, group, 1, 1, output, database, reset=True)
            )
        self.assertEqual(reset_code, 0)
        reset_payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(reset_payload["events_seen"], 1)
        self.assertFalse(reset_payload["connection"]["resumed_from_checkpoint"])
        with sqlite3.connect(database) as checkpoint:
            self.assertEqual(
                checkpoint.execute("SELECT count(*) FROM probe_events").fetchone()[0], 1
            )

    def test_event_probe_normal_close_reconnects_within_deadline(self) -> None:
        config = self.root / "collector-reconnect.json"
        output = self.root / "probe-reconnect.json"
        database = self.root / "probe-reconnect.sqlite3"
        group = "100000009"
        config.write_text(
            json.dumps(
                {
                    "onebot": {"ws_url": "ws://synthetic.invalid", "token": "test"},
                    "storage": {"root": str(self.root / "repository")},
                }
            ),
            encoding="utf-8",
        )
        event = {
            "post_type": "message",
            "message_type": "group",
            "group_id": int(group),
            "message": [
                {"type": "image", "data": {"file": "image.png", "url": ""}}
            ],
            "raw": {"elements": []},
        }

        class ClosedSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def recv(self):
                raise ConnectionClosedOK(None, None)

        class EventSocket(ClosedSocket):
            async def recv(self):
                return json.dumps(event)

        with patch(
            "linux.event_probe.websockets.connect",
            side_effect=[ClosedSocket(), EventSocket()],
        ), patch("linux.event_probe.asyncio.sleep", new_callable=AsyncMock):
            code = asyncio.run(receive_sample(config, group, 1, 5, output, database))
        self.assertEqual(code, 0)
        payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(payload["connection"]["attempts"], 2)
        self.assertEqual(payload["connection"]["disconnects"], 1)
        self.assertEqual(payload["connection"]["reconnects"], 1)
        self.assertEqual(payload["connection"]["state"], "complete")

    def test_telemetry_cannot_pass_before_full_observation_window(self) -> None:
        database = self.root / "telemetry.sqlite3"
        config = self.root / "telemetry.json"
        config.write_text(
            json.dumps(
                {
                    "storage": {
                        "root": str(self.root / "repository"),
                        "database": str(database),
                    }
                }
            ),
            encoding="utf-8",
        )
        now = 2_000_000_000
        with connect_database(database) as connection:
            set_setting(connection, "rollout_started_at", now - 3600)

        with patch("linux.telemetry_report.time.time", return_value=now):
            payload, gate = telemetry_report(config, 72)

        self.assertFalse(gate)
        self.assertEqual(payload["steady_state_gate"], "fail")
        self.assertFalse(payload["duration_requirement_met"])
        self.assertEqual(payload["observation_seconds"], 3600)
        self.assertEqual(payload["required_observation_seconds"], 72 * 3600)

    def test_get_image_diagnostic_consumes_sentinel_before_request(self) -> None:
        source = (Path(__file__).parent / "linux" / "diagnostic_compare.py").read_text(
            encoding="utf-8"
        )
        guarded = source.index("if args.allow_get_image_diagnostic:")
        exists_check = source.index("if sentinel.exists():", guarded)
        sentinel_write = source.index("atomic_private_json(", exists_check)
        outbound_call = source.index("await raw_onebot_get_image(", sentinel_write)
        self.assertLess(exists_check, sentinel_write)
        self.assertLess(sentinel_write, outbound_call)
        self.assertIn("one-time get_image diagnostic has already been consumed", source)

    def test_lifecycle_probe_pairs_range_and_plain_get_without_reading_bodies(self) -> None:
        requests: list[httpx.Request] = []
        streams = []

        class HeaderOnlyStream(httpx.AsyncByteStream):
            def __init__(self) -> None:
                self.iterated = False
                self.closed = False

            async def __aiter__(self):
                self.iterated = True
                raise AssertionError("lifecycle probe must not consume a response body")
                yield b"unreachable"

            async def aclose(self) -> None:
                self.closed = True

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            stream = HeaderOnlyStream()
            streams.append(stream)
            is_range = request.headers.get("range") == "bytes=0-0"
            return httpx.Response(
                206 if is_range else 200,
                request=request,
                headers={
                    "Content-Length": "1" if is_range else "98765",
                    "Content-Range": "bytes 0-0/98765" if is_range else "",
                    "Accept-Ranges": "bytes",
                    "Content-Type": "image/png; charset=binary",
                    "ETag": '"PRIVATE_ETAG"',
                    "Location": "https://redirect.invalid/object?rkey=DO_NOT_STORE",
                    "Content-Disposition": 'attachment; filename="PRIVATE.png"',
                    "Set-Cookie": "PRIVATE_COOKIE=1",
                },
                stream=stream,
            )

        async def run() -> dict:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler), follow_redirects=False
            ) as client:
                return await url_lifecycle_probe._paired_probe(
                    client,
                    "https://multimedia.nt.qq.com.cn/object?rkey=REQUEST_SECRET",
                )

        result = asyncio.run(run())
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(requests[0].headers.get("range"), "bytes=0-0")
        self.assertEqual(requests[1].method, "GET")
        self.assertNotIn("range", requests[1].headers)
        self.assertEqual(result["range_get"]["http_status"], 206)
        self.assertEqual(result["plain_get"]["http_status"], 200)
        self.assertFalse(result["range_get"]["body_consumed"])
        self.assertFalse(result["plain_get"]["body_consumed"])
        self.assertEqual(
            result["range_get"]["response_headers"]["content_range"],
            "bytes 0-0/98765",
        )
        self.assertEqual(
            result["plain_get"]["response_headers"]["location"],
            {
                "present": True,
                "host": "redirect.invalid",
                "relative": False,
                "has_rkey": True,
            },
        )
        self.assertTrue(all(stream.closed for stream in streams))
        self.assertFalse(any(stream.iterated for stream in streams))
        rendered = json.dumps(result)
        for secret in (
            "REQUEST_SECRET",
            "DO_NOT_STORE",
            "PRIVATE_ETAG",
            "PRIVATE.png",
            "PRIVATE_COOKIE",
        ):
            self.assertNotIn(secret, rendered)

    def test_lifecycle_schema_one_is_preserved_as_legacy_without_plain_result(self) -> None:
        secret = self.root / "lifecycle.secret.json"
        report = self.root / "lifecycle.report.json"
        private_url = "https://multimedia.nt.qq.com.cn/object?rkey=NEVER_PRINT"
        secret.write_text(
            json.dumps(
                [
                    {
                        "id": "public-hash",
                        "kind": "data_url",
                        "host": "multimedia.nt.qq.com.cn",
                        "has_rkey": True,
                        "captured_at": 1,
                        "url": private_url,
                    }
                ]
            ),
            encoding="utf-8",
        )
        report.write_text(
            json.dumps(
                {
                    "schema": 1,
                    "checks": [
                        {
                            "label": "T+0",
                            "checked_at": 2,
                            "results": [{"id": "public-hash", "http_status": 206}],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        paired = {
            "pair_started_at": 3,
            "pair_finished_at": 4,
            "range_get": {"http_status": 400, "body_consumed": False},
            "plain_get": {"http_status": 400, "body_consumed": False},
        }
        stdout = io.StringIO()
        with patch(
            "linux.url_lifecycle_probe._paired_probe",
            new_callable=AsyncMock,
            return_value=paired,
        ), redirect_stdout(stdout):
            code = asyncio.run(
                url_lifecycle_probe.check(secret, report, "T+1h", finalize=True)
            )

        self.assertEqual(code, 0)
        self.assertFalse(secret.exists())
        payload = json.loads(report.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema"], 2)
        self.assertEqual(payload["legacy_schema1_checks"], 1)
        self.assertTrue(payload["checks"][0]["legacy"])
        self.assertEqual(payload["checks"][0]["probe_modes"], ["range_get"])
        self.assertFalse(payload["checks"][0]["plain_get_recorded"])
        self.assertNotIn("plain_get", payload["checks"][0]["results"][0])
        self.assertFalse(payload["checks"][1]["legacy"])
        self.assertEqual(
            payload["checks"][1]["probe_modes"], ["range_get", "plain_get"]
        )
        self.assertEqual(payload["checks"][1]["results"][0]["plain_get"]["http_status"], 400)
        self.assertIn("secret_urls_deleted", stdout.getvalue())
        self.assertNotIn("NEVER_PRINT", report.read_text(encoding="utf-8"))
        self.assertNotIn("NEVER_PRINT", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
