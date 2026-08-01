from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from qq_image_console.app import create_app
from qq_image_console.remote import RemoteIdentity
from qq_image_console.repository import Repository
from test_console_repository import make_console_config


class FakeHealth:
    def snapshot(self, force: bool = False):
        return {
            "services": {
                "manager": {"healthy": True, "detail": "ok"},
                "qq": {"healthy": True, "detail": "running"},
                "napcat": {"healthy": True, "detail": "injected"},
                "webui": {"healthy": True, "detail": "6099"},
                "onebot": {"healthy": True, "detail": "3000"},
                "qce": {"healthy": True, "detail": "online"},
            },
            "account": {"user_id": "10000", "nickname": "Tester"},
        }

    def available_groups(self):
        return [
            {
                "group_id": "654321",
                "group_name": "可选群",
                "member_count": 12,
                "max_member_count": 200,
            }
        ]


class FakeSupervisor:
    def __init__(self) -> None:
        self.started = False

    def worker_status(self):
        return {"healthy": self.started, "pid": 123 if self.started else None, "detail": "test"}

    def action(self):
        return {"name": None, "status": "idle", "stage": None, "message": None, "error": None}

    def action_running(self):
        return False

    def worker_pid(self):
        return 123 if self.started else None

    def request_start(self, confirmed: bool = False):
        if not confirmed:
            return {"confirmation_required": True, "reason": "需要确认"}
        self.started = True
        return {"confirmation_required": False, "action": self.action()}

    def request_stop(self):
        self.started = False
        return {"action": self.action()}

    def request_restart(self, confirmed: bool = False):
        return self.request_start(confirmed)

    def stop_worker(self):
        self.started = False

    def start_worker(self):
        self.started = True


class FakeAccessVerifier:
    def verify(self, assertion: str) -> RemoteIdentity:
        if assertion != "signed-access-token":
            from qq_image_console.remote import RemoteAuthenticationError

            raise RemoteAuthenticationError("Cloudflare Access 会话校验失败")
        return RemoteIdentity(subject="cf-user-1", email="owner@example.com")


class ConsoleApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.config = make_console_config(self.base, ["123456"])
        self.repository = Repository(self.config)
        self.supervisor = FakeSupervisor()
        self.app = create_app(
            self.config,
            "a" * 40,
            testing=True,
            repository=self.repository,
            health=FakeHealth(),
            supervisor=self.supervisor,
        )
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.client.close()
        self.temporary.cleanup()

    def login(self) -> None:
        response = self.client.get("/api/v1/session", params={"session_token": "a" * 40})
        self.assertEqual(response.status_code, 200)

    def test_api_requires_random_local_session(self) -> None:
        self.assertEqual(self.client.get("/health").status_code, 200)
        self.assertEqual(self.client.get("/api/v1/status").status_code, 401)
        self.assertEqual(
            self.client.get("/api/v1/session", params={"session_token": "wrong"}).status_code,
            401,
        )
        self.login()
        response = self.client.get("/api/v1/status")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("a" * 40, response.text)

    def test_external_origin_is_rejected_even_with_token(self) -> None:
        self.login()
        response = self.client.get(
            "/api/v1/status", headers={"Origin": "https://example.com"}
        )
        self.assertEqual(response.status_code, 403)

    def test_invalid_host_port_is_rejected_in_production_mode(self) -> None:
        production_app = create_app(
            self.config,
            "a" * 40,
            testing=False,
            repository=self.repository,
            health=FakeHealth(),
            supervisor=self.supervisor,
        )
        with TestClient(production_app, client=("127.0.0.1", 50000)) as client:
            response = client.get("/health", headers={"Host": "127.0.0.1:9999"})
        self.assertEqual(response.status_code, 400)

    def test_explicit_ssh_forward_port_is_allowed_in_production_mode(self) -> None:
        self.config.local_forward_ports = [17891]
        production_app = create_app(
            self.config,
            "a" * 40,
            testing=False,
            repository=self.repository,
            health=FakeHealth(),
            supervisor=self.supervisor,
        )
        headers = {
            "Host": "127.0.0.1:17891",
            "Origin": "http://127.0.0.1:17891",
        }
        with TestClient(production_app, client=("127.0.0.1", 50000)) as client:
            response = client.get("/health", headers=headers)
        self.assertEqual(response.status_code, 200)

    def test_explicit_direct_public_host_uses_token_auth(self) -> None:
        self.config.direct_public_enabled = True
        self.config.direct_public_hosts = ["203.0.113.10"]
        self.config.direct_public_port = 17891
        production_app = create_app(
            self.config,
            "a" * 40,
            testing=False,
            repository=self.repository,
            health=FakeHealth(),
            supervisor=self.supervisor,
        )
        base_headers = {
            "Host": "203.0.113.10:17891",
            "Origin": "http://203.0.113.10:17891",
        }
        with TestClient(
            production_app,
            client=("198.51.100.20", 50000),
        ) as client:
            self.assertEqual(
                client.get("/health", headers=base_headers).status_code,
                200,
            )
            self.assertEqual(
                client.get("/api/v1/status", headers=base_headers).status_code,
                401,
            )
            session = client.get(
                "/api/v1/session",
                params={"session_token": "a" * 40},
                headers=base_headers,
            )
            self.assertEqual(session.status_code, 200)
            self.assertEqual(session.json()["mode"], "direct")
            status_response = client.get(
                "/api/v1/status",
                headers={
                    **base_headers,
                    "X-Local-Token": "a" * 40,
                },
            )
            self.assertEqual(status_response.status_code, 200)
            self.assertEqual(status_response.json()["access"]["mode"], "direct")
            wrong_origin = client.get(
                "/api/v1/status",
                headers={
                    "Host": "203.0.113.10:17891",
                    "Origin": "http://example.com:17891",
                    "X-Local-Token": "a" * 40,
                },
            )
            self.assertEqual(wrong_origin.status_code, 403)
            wrong_host = client.get(
                "/health",
                headers={"Host": "203.0.113.11:17891"},
            )
            self.assertEqual(wrong_host.status_code, 400)

    def test_linux_docker_gateway_is_allowed_only_when_explicitly_trusted(self) -> None:
        self.config.deployment_mode = "linux-docker"
        self.config.launcher_kind = "external"
        self.config.trusted_proxy_cidrs = ["172.16.0.0/12"]
        production_app = create_app(
            self.config,
            "a" * 40,
            testing=False,
            repository=self.repository,
            health=FakeHealth(),
            supervisor=self.supervisor,
        )
        with TestClient(production_app, client=("172.18.0.1", 50000)) as client:
            allowed = client.get(
                "/health",
                headers={"Host": f"127.0.0.1:{self.config.port}"},
            )
        with TestClient(production_app, client=("10.0.0.20", 50000)) as client:
            rejected = client.get(
                "/health",
                headers={"Host": f"127.0.0.1:{self.config.port}"},
            )
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(rejected.status_code, 403)

    def test_start_requires_confirm_for_ordinary_qq(self) -> None:
        self.login()
        first = self.client.post("/api/v1/system/start", json={})
        self.assertEqual(first.status_code, 409)
        self.assertTrue(first.json()["confirmation_required"])
        second = self.client.post(
            "/api/v1/system/start", json={"confirm_close_qq": True}
        )
        self.assertEqual(second.status_code, 200)
        self.assertTrue(self.supervisor.started)

    def test_groups_jobs_and_disable_flow(self) -> None:
        self.login()
        available = self.client.get("/api/v1/groups/available")
        self.assertEqual(available.status_code, 200)
        added = self.client.post(
            "/api/v1/groups", json={"group_id": "654321", "display_name": "可选群"}
        )
        self.assertEqual(added.status_code, 201)
        job = self.client.post(
            "/api/v1/groups/654321/backfill", json={"mode": "continuous"}
        )
        self.assertEqual(job.status_code, 201)
        job_id = job.json()["job_id"]
        cancelled = self.client.post(f"/api/v1/jobs/{job_id}/cancel", json={})
        self.assertEqual(cancelled.status_code, 200)
        disabled = self.client.delete("/api/v1/groups/654321")
        self.assertEqual(disabled.status_code, 200)
        row = next(group for group in self.client.get("/api/v1/groups").json() if group["group_id"] == "654321")
        self.assertEqual(row["enabled"], 0)

    def test_remote_access_requires_jwt_session_csrf_and_restricts_paths(self) -> None:
        self.config.remote_enabled = True
        self.config.remote_public_origin = "https://console.example.com"
        self.config.remote_access_issuer = "https://example.cloudflareaccess.com"
        self.config.remote_access_audience = "audience-tag"
        self.config.remote_allowed_email = "owner@example.com"
        remote_app = create_app(
            self.config,
            "a" * 40,
            testing=True,
            repository=self.repository,
            health=FakeHealth(),
            supervisor=self.supervisor,
            access_verifier=FakeAccessVerifier(),
        )
        base_headers = {
            "Cf-Access-Jwt-Assertion": "signed-access-token",
            "Origin": "https://console.example.com",
        }
        with TestClient(
            remote_app,
            base_url="https://console.example.com",
            headers=base_headers,
        ) as client:
            self.assertEqual(client.get("/api/v1/status", headers={
                "Cf-Access-Jwt-Assertion": "invalid",
                "Origin": "https://console.example.com",
            }).status_code, 401)
            session = client.get("/api/v1/session")
            self.assertEqual(session.status_code, 200)
            self.assertEqual(session.json()["mode"], "remote")
            csrf = session.json()["csrf_token"]

            without_csrf = client.patch(
                "/api/v1/settings",
                json={"collector_paused": True},
            )
            self.assertEqual(without_csrf.status_code, 403)
            safe = client.patch(
                "/api/v1/settings",
                json={"collector_paused": True},
                headers={"X-CSRF-Token": csrf},
            )
            self.assertEqual(safe.status_code, 200)
            self.assertTrue(safe.json()["remote_restricted"])
            self.assertNotIn("storage_root", safe.json())

            unsafe = client.patch(
                "/api/v1/settings",
                json={"qq_path": "C:\\QQ.exe"},
                headers={"X-CSRF-Token": csrf},
            )
            self.assertEqual(unsafe.status_code, 403)
            local_only = client.post(
                "/api/v1/storage/open",
                json={},
                headers={"X-CSRF-Token": csrf},
            )
            self.assertEqual(local_only.status_code, 403)
            status_response = client.get("/api/v1/status")
            self.assertEqual(status_response.json()["access"]["mode"], "remote")
            self.assertEqual(
                status_response.json()["access"]["identity"]["email"],
                "owner@example.com",
            )
            audit = client.get("/api/v1/audit").json()["entries"]
            self.assertTrue(any(row["action"] == "PATCH /api/v1/settings" for row in audit))


if __name__ == "__main__":
    unittest.main()
