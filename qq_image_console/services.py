from __future__ import annotations

import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psutil

from collector import OneBotClient, OneBotError

from .config import ConsoleConfig


WORKER_START_TIMEOUT_SECONDS = 30


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


class HealthService:
    """Read-only health checks for the pure NapCat event deployment."""

    def __init__(self, config: ConsoleConfig, timeout: float = 1.5) -> None:
        self.config = config
        self.timeout = timeout
        self._cache_lock = threading.Lock()
        self._cache: tuple[float, dict[str, Any]] | None = None

    def _port_ready(self, host: str, port: int) -> dict[str, Any]:
        try:
            with socket.create_connection((host, port), timeout=self.timeout):
                return {"healthy": True, "detail": f"{host}:{port}"}
        except OSError as exc:
            return {"healthy": False, "detail": str(exc)}

    def _onebot(self) -> dict[str, Any]:
        settings = self.config.collector_settings().get("onebot", {})
        try:
            client = OneBotClient.from_settings(settings)
            client.timeout = max(1, int(self.timeout))
            account = client.call("get_login_info", {}) or {}
            return {
                "healthy": True,
                "detail": client.base_url,
                "account": {
                    "user_id": str(account.get("user_id") or ""),
                    "nickname": str(account.get("nickname") or ""),
                },
            }
        except (OSError, ValueError, OneBotError) as exc:
            return {"healthy": False, "detail": _error_text(exc), "account": None}

    def snapshot(self, force: bool = False) -> dict[str, Any]:
        with self._cache_lock:
            if not force and self._cache and time.time() - self._cache[0] < 1.0:
                return dict(self._cache[1])
        onebot_settings = self.config.collector_settings().get("onebot", {})
        webui_url = str(onebot_settings.get("webui_url") or "http://napcat:6099")
        ws_url = str(onebot_settings.get("ws_url") or "ws://napcat:3001")
        webui = urlsplit(webui_url)
        websocket = urlsplit(ws_url)
        onebot = self._onebot()
        webui_state = self._port_ready(webui.hostname or "napcat", webui.port or 6099)
        ws_state = self._port_ready(websocket.hostname or "napcat", websocket.port or 3001)
        result = {
            "checked_at": int(time.time()),
            "services": {
                "manager": {"healthy": True, "detail": "管理服务正常"},
                "qq": {
                    "healthy": bool(onebot["healthy"]),
                    "detail": "Linux 容器内 QQ 已登录" if onebot["healthy"] else "等待扫码登录",
                    "processes": [],
                },
                "napcat": {
                    "healthy": bool(webui_state["healthy"]),
                    "detail": "官方 NapCat Docker" if webui_state["healthy"] else "NapCat 未就绪",
                },
                "webui": webui_state,
                "onebot": {key: value for key, value in onebot.items() if key != "account"},
                "event_socket": ws_state,
            },
            "account": onebot.get("account"),
        }
        with self._cache_lock:
            self._cache = (time.time(), result)
        return dict(result)

    def ready_for_collection(self, snapshot: dict[str, Any] | None = None) -> bool:
        services = (snapshot or self.snapshot())["services"]
        return all(bool(services[name]["healthy"]) for name in ("webui", "onebot", "event_socket"))

    def available_groups(self) -> list[dict[str, Any]]:
        client = OneBotClient.from_settings(self.config.collector_settings().get("onebot", {}))
        client.timeout = max(2, int(self.timeout * 2))
        rows = client.call("get_group_list", {}) or []
        return [
            {
                "group_id": str(row.get("group_id") or ""),
                "group_name": str(row.get("group_name") or ""),
                "member_count": int(row.get("member_count") or 0),
                "max_member_count": int(row.get("max_member_count") or 0),
            }
            for row in rows
            if row.get("group_id")
        ]


class ProcessSupervisor:
    def __init__(self, config: ConsoleConfig, health: HealthService) -> None:
        self.config = config
        self.health = health
        self._lock = threading.RLock()
        self._worker_lock = threading.Lock()
        self._action: dict[str, Any] = {
            "name": None,
            "status": "idle",
            "stage": None,
            "message": None,
            "error": None,
            "started_at": None,
            "finished_at": None,
        }

    def _set_action(self, **values: Any) -> None:
        with self._lock:
            self._action.update(values)

    def action(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._action)

    def action_running(self) -> bool:
        return self.action().get("status") == "running"

    def worker_pid_file(self) -> Path:
        runtime = self.config.collector_settings().get("runtime", {})
        return Path(runtime.get("pid_file", self.config.storage_root() / "state" / "collector.pid"))

    def worker_pid(self) -> int | None:
        try:
            pid = int(self.worker_pid_file().read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            return None
        try:
            process = psutil.Process(pid)
            command = " ".join(process.cmdline()).lower()
            if "qq_image_console.main" in command and "--worker" in command:
                return pid
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            pass
        self.worker_pid_file().unlink(missing_ok=True)
        return None

    def worker_status(self) -> dict[str, Any]:
        pid = self.worker_pid()
        return {
            "healthy": pid is not None,
            "pid": pid,
            "detail": f"事件 Worker 运行中，PID {pid}" if pid else "事件 Worker 未运行",
        }

    def _worker_command(self) -> list[str]:
        return [
            sys.executable,
            "-m",
            "qq_image_console.main",
            "--worker",
            "--config",
            str(self.config.manager_config_file),
        ]

    def start_worker(self) -> dict[str, Any]:
        with self._worker_lock:
            if self.worker_pid():
                return self.worker_status()
            if not self.health.ready_for_collection(self.health.snapshot(force=True)):
                raise RuntimeError("NapCat、OneBot HTTP 或事件 WS 尚未就绪")
            log_path = self.config.storage_root() / "logs" / "collector-console.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("ab", buffering=0) as log_file:
                process = subprocess.Popen(
                    self._worker_command(),
                    cwd=str(Path(__file__).resolve().parents[1]),
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                )
            deadline = time.time() + WORKER_START_TIMEOUT_SECONDS
            while time.time() < deadline:
                if self.worker_pid():
                    return self.worker_status()
                if process.poll() is not None:
                    raise RuntimeError(f"采集 Worker 启动失败，退出码 {process.returncode}")
                time.sleep(0.2)
            process.terminate()
            raise RuntimeError("采集 Worker 未在 30 秒内建立单实例 PID")

    def stop_worker(self, timeout: float = 15) -> dict[str, Any]:
        with self._worker_lock:
            pid = self.worker_pid()
            if not pid:
                return self.worker_status()
            try:
                process = psutil.Process(pid)
                process.terminate()
                try:
                    process.wait(timeout=timeout)
                except psutil.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            except psutil.NoSuchProcess:
                pass
            self.worker_pid_file().unlink(missing_ok=True)
            return self.worker_status()

    def request_start(self, _confirm: bool = False) -> dict[str, Any]:
        with self._lock:
            if self.action_running():
                raise RuntimeError("已有系统操作正在执行")
            self._action.update(
                name="start",
                status="running",
                stage="wait_services",
                message="等待 NapCat、OneBot 与事件流就绪",
                error=None,
                started_at=int(time.time()),
                finished_at=None,
            )
        threading.Thread(target=self._start_sequence, name="system-start", daemon=True).start()
        return {"confirmation_required": False, "action": self.action()}

    def _start_sequence(self) -> None:
        try:
            deadline = time.time() + 180
            while time.time() < deadline:
                if self.health.ready_for_collection(self.health.snapshot(force=True)):
                    break
                time.sleep(2)
            else:
                raise TimeoutError("180 秒内未等到 NapCat 登录和 OneBot 事件接口")
            self._set_action(stage="start_worker", message="接口已就绪，正在启动事件 Worker")
            self.start_worker()
            self._set_action(
                status="completed",
                stage="done",
                message="事件驱动采集已启动",
                error=None,
                finished_at=int(time.time()),
            )
        except Exception as exc:
            self._set_action(
                status="failed",
                message="启动失败",
                error=_error_text(exc),
                finished_at=int(time.time()),
            )

    def request_stop(self) -> dict[str, Any]:
        with self._lock:
            if self.action_running():
                raise RuntimeError("已有系统操作正在执行")
            self._action.update(
                name="stop",
                status="running",
                stage="stop_worker",
                message="正在安全停止事件 Worker",
                error=None,
                started_at=int(time.time()),
                finished_at=None,
            )

        def stop() -> None:
            try:
                self.stop_worker()
                self._set_action(
                    status="completed",
                    stage="done",
                    message="事件 Worker 已停止；NapCat 保持运行",
                    error=None,
                    finished_at=int(time.time()),
                )
            except Exception as exc:
                self._set_action(status="failed", message="停止失败", error=_error_text(exc), finished_at=int(time.time()))

        threading.Thread(target=stop, name="system-stop", daemon=True).start()
        return {"action": self.action()}

    def request_restart(self, _confirm: bool = False) -> dict[str, Any]:
        with self._lock:
            if self.action_running():
                raise RuntimeError("已有系统操作正在执行")
            self._action.update(
                name="restart",
                status="running",
                stage="stop_worker",
                message="正在重启事件 Worker",
                error=None,
                started_at=int(time.time()),
                finished_at=None,
            )

        def restart() -> None:
            try:
                self.stop_worker()
                self._start_sequence()
            except Exception as exc:
                self._set_action(status="failed", message="重启失败", error=_error_text(exc), finished_at=int(time.time()))

        threading.Thread(target=restart, name="system-restart", daemon=True).start()
        return {"confirmation_required": False, "action": self.action()}
