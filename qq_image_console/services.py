from __future__ import annotations

import json
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import psutil

from collector import OneBotClient, OneBotError

from .config import ConsoleConfig


# Repository compatibility checks run before the collector acquires its PID
# file. On low-end VPS disks, validating roughly 1 GiB of existing assets can
# take several minutes even though the worker is healthy and making progress.
WORKER_START_TIMEOUT_SECONDS = 600


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


class HealthService:
    """Read-only health checks for QQ, NapCat, OneBot and QCE."""

    def __init__(self, config: ConsoleConfig, timeout: float = 1.5) -> None:
        self.config = config
        self.timeout = timeout
        self._cache_lock = threading.Lock()
        self._cache: tuple[float, dict[str, Any]] | None = None

    def qq_processes(self) -> list[dict[str, Any]]:
        if self.config.external_services:
            return []
        result: list[dict[str, Any]] = []
        for process in psutil.process_iter(["pid", "name", "exe", "create_time"]):
            try:
                if str(process.info.get("name") or "").lower() != "qq.exe":
                    continue
                result.append(
                    {
                        "pid": int(process.info["pid"]),
                        "exe": str(process.info.get("exe") or ""),
                        "create_time": int(process.info.get("create_time") or 0),
                    }
                )
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
        return result

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
        except (KeyError, OSError, ValueError, OneBotError) as exc:
            return {"healthy": False, "detail": _error_text(exc), "account": None}

    def _qce(self) -> dict[str, Any]:
        settings = self.config.collector_settings().get("qce", {})
        base_url = str(settings.get("base_url", "http://127.0.0.1:40653")).rstrip("/")
        request = urllib.request.Request(f"{base_url}/health", method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = json.load(response)
            data = body.get("data") if isinstance(body, dict) else {}
            if not isinstance(data, dict) or not data:
                data = body if isinstance(body, dict) else {}
            api_ok = bool(body.get("success", True)) if isinstance(body, dict) else False
            healthy = api_ok and bool(data.get("online", True))
            return {
                "healthy": healthy,
                "detail": "QQ online" if healthy else str(data.get("message") or "QCE offline"),
            }
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            return {"healthy": False, "detail": _error_text(exc)}

    def snapshot(self, force: bool = False) -> dict[str, Any]:
        with self._cache_lock:
            if not force and self._cache and time.time() - self._cache[0] < 1.0:
                return dict(self._cache[1])

        qq_processes = self.qq_processes()
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="health") as pool:
            webui_future = pool.submit(self._port_ready, "127.0.0.1", 6099)
            onebot_future = pool.submit(self._onebot)
            qce_future = pool.submit(self._qce)
            webui = webui_future.result()
            onebot = onebot_future.result()
            qce = qce_future.result()

        if self.config.external_services:
            qq_healthy = bool(onebot["healthy"])
            injected = bool(webui["healthy"]) and bool(onebot["healthy"])
            qq_detail = (
                "Linux 容器内 QQ 已登录"
                if qq_healthy
                else "等待容器内 QQ 登录"
            )
            napcat_detail = (
                self.config.external_service_detail
                if injected
                else "Docker 中的 NapCat 尚未就绪"
            )
        else:
            qq_healthy = bool(qq_processes)
            injected = bool(qq_processes) and bool(onebot["healthy"])
            qq_detail = f"{len(qq_processes)} 个 QQ 进程" if qq_processes else "未运行"
            napcat_detail = "已注入" if injected else "未检测到有效注入"
        result = {
            "checked_at": int(time.time()),
            "services": {
                "manager": {"healthy": True, "detail": "本地管理服务正常"},
                "qq": {
                    "healthy": qq_healthy,
                    "detail": qq_detail,
                    "processes": qq_processes,
                },
                "napcat": {
                    "healthy": injected,
                    "detail": napcat_detail,
                },
                "webui": webui,
                "onebot": {key: value for key, value in onebot.items() if key != "account"},
                "qce": qce,
            },
            "account": onebot.get("account"),
        }
        with self._cache_lock:
            self._cache = (time.time(), result)
        return dict(result)

    def ready_for_collection(self, snapshot: dict[str, Any] | None = None) -> bool:
        current = snapshot or self.snapshot()
        services = current["services"]
        return all(bool(services[name]["healthy"]) for name in ("webui", "onebot", "qce"))

    def available_groups(self) -> list[dict[str, Any]]:
        settings = self.config.collector_settings().get("onebot", {})
        client = OneBotClient.from_settings(settings)
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
    """Owns the single collector worker and the NapCat startup state machine."""

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
        settings = self.config.collector_settings()
        runtime = settings.get("runtime", {})
        return Path(runtime.get("pid_file", self.config.storage_root() / "state" / "collector.pid"))

    def worker_pid(self) -> int | None:
        try:
            pid = int(self.worker_pid_file().read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            return None
        if psutil.pid_exists(pid):
            try:
                command = " ".join(psutil.Process(pid).cmdline()).lower()
                is_worker = "collector.py" in command or (
                    "qq_image_console.main" in command and "--worker" in command
                )
                if is_worker:
                    return pid
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
        try:
            self.worker_pid_file().unlink(missing_ok=True)
        except OSError:
            pass
        return None

    def worker_status(self) -> dict[str, Any]:
        pid = self.worker_pid()
        return {
            "healthy": pid is not None,
            "pid": pid,
            "detail": f"运行中，PID {pid}" if pid else "未运行",
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
            return self._start_worker_locked()

    def _start_worker_locked(self) -> dict[str, Any]:
        existing = self.worker_pid()
        if existing:
            return self.worker_status()
        snapshot = self.health.snapshot(force=True)
        if not self.health.ready_for_collection(snapshot):
            raise RuntimeError("OneBot、QCE 或 NapCat WebUI 尚未就绪")
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
        # Large repositories perform a compatibility scan before the worker
        # acquires its PID file. Even a few hundred large assets can take
        # several minutes on a low-end VPS disk.
        deadline = time.time() + WORKER_START_TIMEOUT_SECONDS
        while time.time() < deadline:
            pid = self.worker_pid()
            if pid:
                return self.worker_status()
            if process.poll() is not None:
                raise RuntimeError(f"采集 Worker 启动失败，退出码 {process.returncode}")
            time.sleep(0.2)
        process.terminate()
        raise RuntimeError(
            f"采集 Worker 未在 {WORKER_START_TIMEOUT_SECONDS} 秒内建立单实例 PID"
        )

    def stop_worker(self, timeout: float = 15) -> dict[str, Any]:
        with self._worker_lock:
            return self._stop_worker_locked(timeout)

    def _stop_worker_locked(self, timeout: float = 15) -> dict[str, Any]:
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
        try:
            self.worker_pid_file().unlink(missing_ok=True)
        except OSError:
            pass
        return self.worker_status()

    def request_start(self, confirm_close_qq: bool = False) -> dict[str, Any]:
        # NapCat and QQ are owned by Docker Compose, so starting the stack only
        # means waiting for the containers and then launching the worker.
        with self._lock:
            if self._action.get("status") == "running":
                raise RuntimeError("已有系统操作正在执行")
            self._action.update(
                name="start",
                status="running",
                stage="inspect",
                message="正在检查 QQ 与 NapCat",
                error=None,
                started_at=int(time.time()),
                finished_at=None,
            )
        thread = threading.Thread(
            target=self._start_sequence,
            args=(confirm_close_qq,),
            name="system-start",
            daemon=True,
        )
        thread.start()
        return {"confirmation_required": False, "action": self.action()}

    def _start_sequence(self, confirm_close_qq: bool) -> None:
        try:
            snapshot = self.health.snapshot(force=True)
            if not self.health.ready_for_collection(snapshot):
                self._set_action(
                    stage="wait_services",
                    message="正在等待 Docker 中的 QQ、NapCat、OneBot 与 QCE",
                )
                deadline = time.time() + 180
                while time.time() < deadline:
                    self._set_action(stage="wait_services", message="等待 WebUI、OneBot 与 QCE 就绪")
                    current = self.health.snapshot(force=True)
                    if self.health.ready_for_collection(current):
                        break
                    time.sleep(2)
                else:
                    raise TimeoutError("180 秒内未等到 WebUI、OneBot 与 QCE 全部就绪")

            self._set_action(stage="start_worker", message="接口已就绪，正在启动采集 Worker")
            self.start_worker()
            self._set_action(
                status="completed",
                stage="done",
                message="QQ 原图采集已启动",
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
            if self._action.get("status") == "running":
                raise RuntimeError("已有系统操作正在执行")
            self._action.update(
                name="stop",
                status="running",
                stage="stop_worker",
                message="正在安全停止采集 Worker",
                error=None,
                started_at=int(time.time()),
                finished_at=None,
            )
        thread = threading.Thread(target=self._stop_sequence, name="system-stop", daemon=True)
        thread.start()
        return {"action": self.action()}

    def _stop_sequence(self) -> None:
        try:
            self.stop_worker()
            self._set_action(
                status="completed",
                stage="done",
                message="采集 Worker 已停止；QQ 与 NapCat 保持运行",
                error=None,
                finished_at=int(time.time()),
            )
        except Exception as exc:
            self._set_action(
                status="failed",
                message="停止失败",
                error=_error_text(exc),
                finished_at=int(time.time()),
            )

    def request_restart(self, confirm_close_qq: bool = False) -> dict[str, Any]:
        snapshot = self.health.snapshot(force=True)
        services = snapshot["services"]
        if (
            not self.config.external_services
            and services["qq"]["healthy"]
            and not services["onebot"]["healthy"]
            and not confirm_close_qq
        ):
            return {
                "confirmation_required": True,
                "reason": "普通 QQ 正在运行。重启采集前需要关闭并通过 NapCat 启动，是否继续？",
            }
        with self._lock:
            if self._action.get("status") == "running":
                raise RuntimeError("已有系统操作正在执行")
            self._action.update(
                name="restart",
                status="running",
                stage="stop_worker",
                message="正在重启采集服务",
                error=None,
                started_at=int(time.time()),
                finished_at=None,
            )

        def restart() -> None:
            try:
                self.stop_worker()
                self._start_sequence(confirm_close_qq)
            except Exception as exc:
                self._set_action(
                    status="failed",
                    message="重启失败",
                    error=_error_text(exc),
                    finished_at=int(time.time()),
                )

        threading.Thread(target=restart, name="system-restart", daemon=True).start()
        return {"confirmation_required": False, "action": self.action()}
