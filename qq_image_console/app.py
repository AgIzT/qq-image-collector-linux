from __future__ import annotations

import asyncio
import copy
import ipaddress
import json
import logging
import os
import secrets
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from . import __version__
from .config import ConsoleConfig
from .repository import Repository
from .remote import (
    REMOTE_SESSION_COOKIE,
    CloudflareAccessVerifier,
    RemoteAuthenticationError,
    RemoteIdentity,
    RemoteSessionStore,
    SnapshotPublisher,
)
from .services import HealthService, ProcessSupervisor
from .storage import StorageMigrationManager


SESSION_COOKIE = "qqic_session"
REMOTE_PERMISSIONS = ["status", "system", "groups", "gap_recovery", "safe_settings", "audit"]
STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
# Computing a full status costs seconds of cold reads against a database far
# larger than the page cache this container gets.  A refresh interval shorter
# than that cost makes refreshes near-continuous, and their own I/O evicts the
# cache that would have made them fast - the snapshot then falls further behind
# until it stops updating at all.  The interval must stay well above the
# measured compute time.
STATUS_SNAPSHOT_SECONDS = 30.0
LOGGER = logging.getLogger(__name__)


class SystemActionRequest(BaseModel):
    confirm_close_qq: bool = False


class GroupCreate(BaseModel):
    group_id: str = Field(min_length=5, max_length=20)
    display_name: str | None = Field(default=None, max_length=200)


class SettingsPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    download_interval_seconds: int | None = Field(default=None, ge=0, le=3600)
    download_jitter_seconds: int | None = Field(default=None, ge=0, le=60)
    url_preference: Literal["data", "raw"] | None = None
    collector_paused: bool | None = None


class StorageMigrationRequest(BaseModel):
    destination: str = Field(min_length=2, max_length=1024)


class OpenFolderRequest(BaseModel):
    path: str | None = None


@dataclass
class AppContext:
    config: ConsoleConfig
    token: str
    repository: Repository
    health: HealthService
    supervisor: ProcessSupervisor
    migration: StorageMigrationManager
    publisher: SnapshotPublisher | None = None
    status_cache_enabled: bool = True
    _status_cache_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _status_cache: tuple[float, dict[str, Any]] | None = field(default=None, init=False, repr=False)
    _status_refresher: threading.Thread | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.status_cache_enabled:
            self._status_cache = (0.0, self._placeholder_status())

    def _placeholder_status(self) -> dict[str, Any]:
        unavailable = {"healthy": False, "detail": "状态正在后台刷新"}
        today_keys = (
            "events",
            "images_seen",
            "image_segments",
            "cdn_requests",
            "cdn_downloads",
            "cdn_bytes",
            "cdn_400",
            "cdn_403",
            "cdn_429",
            "history_calls",
            "window_history_calls",
            "get_image_blocked",
            "accepted",
            "rejected",
            "duplicates",
            "failed",
            "expired",
            "filtered_gif",
        )
        return {
            "timestamp": int(time.time()),
            "services": {
                key: dict(unavailable)
                for key in (
                    "napcat",
                    "onebot",
                    "worker",
                    "event_stream",
                    "queue",
                    "downloader",
                    "recovery",
                )
            },
            "account": None,
            "action": {
                "name": None,
                "status": "idle",
                "stage": None,
                "message": None,
                "error": None,
            },
            "migration": {
                "status": "idle",
                "stage": None,
                "current": 0,
                "total": 0,
                "error": None,
            },
            "statistics": {
                "unique_images": 0,
                "accepted_records": 0,
                "novelai": 0,
                "comfyui": 0,
                "novelai_unreadable": 0,
                "other_models": 0,
                "disk_bytes": 0,
                "queue": {
                    "depth": 0,
                    "oldest_at": None,
                    "oldest_age_seconds": 0,
                    "high": 0,
                    "medium": 0,
                    "low": 0,
                    "expiring": 0,
                    "expiry_urgent": 0,
                },
                "today": {key: 0 for key in today_keys},
                "events": {},
                "downloader": {"status": "loading"},
                "worker": {},
                "window_recovery": {},
            },
            "groups": [],
            "jobs": [],
            "setup": {"completed": False, "checks": []},
            "remote": {
                "enabled": self.config.remote_enabled,
                "public_origin": self.config.remote_public_origin,
                "snapshot": {"enabled": False},
            },
        }

    def start_status_refresh(self) -> None:
        """Start the single refresher. Safe to call repeatedly; only one runs.

        Refreshing used to be triggered per request, guarded by a flag.  Any
        compute that blocked forever - most easily by holding the repository's
        stats lock - pinned that flag, and every later attempt piled up behind
        the same lock, so the console froze on one snapshot permanently.  One
        long-lived loop cannot wedge that way: a slow pass is merely slow, and
        the next pass always follows it.
        """

        if not self.status_cache_enabled:
            return
        with self._status_cache_lock:
            if self._status_refresher is not None and self._status_refresher.is_alive():
                return
            thread = threading.Thread(
                target=self._status_refresh_loop,
                name="console-status-refresh",
                daemon=True,
            )
            self._status_refresher = thread
        thread.start()

    def _status_refresh_loop(self) -> None:
        while True:
            self._refresh_status()
            time.sleep(STATUS_SNAPSHOT_SECONDS)

    def _refresh_status(self) -> None:
        try:
            payload = self._compute_status()
        except Exception:
            LOGGER.exception("background status refresh failed")
        else:
            with self._status_cache_lock:
                self._status_cache = (time.monotonic(), payload)

    def status(self, *, force: bool = False) -> dict[str, Any]:
        if not self.status_cache_enabled:
            return self._compute_status()
        if force:
            payload = self._compute_status()
            with self._status_cache_lock:
                self._status_cache = (time.monotonic(), payload)
            return copy.deepcopy(payload)

        with self._status_cache_lock:
            assert self._status_cache is not None
            cached_at, cached = self._status_cache
            age = time.monotonic() - cached_at
            payload = copy.deepcopy(cached)
        # The console cannot tell a quiet pipeline from a frozen snapshot
        # unless the payload says how old it is.
        payload["snapshot_age_seconds"] = int(age)
        # Serving a request never computes; it only makes sure the loop is up.
        self.start_status_refresh()
        return payload

    def _compute_status(self) -> dict[str, Any]:
        now = int(time.time())
        health = self.health.snapshot()
        services = dict(health["services"])
        statistics = self.repository.stats()
        event_state = statistics.get("events") or {}
        downloader = statistics.get("downloader") or {}
        worker_runtime = statistics.get("worker") or {}
        queue = statistics.get("queue") or {}
        downloader_status = str(downloader.get("status") or "idle")
        pid_state = self.supervisor.worker_status()
        worker_heartbeat = int(worker_runtime.get("heartbeat_at") or 0)
        worker_fresh = worker_heartbeat > 0 and now - worker_heartbeat <= 30
        services["worker"] = {
            **pid_state,
            "healthy": bool(pid_state.get("healthy")) and worker_fresh,
            "detail": (
                str(pid_state.get("detail"))
                if worker_fresh
                else f"Worker 心跳已停止，最后 {worker_heartbeat or '从未'}"
            ),
        }
        last_event_at = int(event_state.get("last_event_at") or 0)
        event_heartbeat = int(event_state.get("heartbeat_at") or 0)
        event_fresh = event_heartbeat > 0 and now - event_heartbeat <= 30
        services["event_stream"] = {
            "healthy": bool(event_state.get("connected")) and event_fresh and worker_fresh,
            "detail": (
                f"连接心跳正常，最后消息 {last_event_at or '尚无'}"
                if event_state.get("connected") and event_fresh and worker_fresh
                else f"连接心跳已过期，最后 {event_heartbeat or '从未'}"
                if event_state.get("connected")
                else str(event_state.get("last_error") or "未连接")
            ),
        }
        downloader_heartbeat = int(downloader.get("heartbeat_at") or 0)
        downloader_fresh = downloader_heartbeat > 0 and now - downloader_heartbeat <= 30
        services["downloader"] = {
            "healthy": downloader_fresh,
            "detail": downloader_status if downloader_fresh else "下载循环心跳已停止",
        }
        queue_depth = int(queue.get("depth") or 0)
        queue_oldest_age = int(queue.get("oldest_age_seconds") or 0)
        services["queue"] = {
            # Reaching this point already proves the persistent store was read.
            # Backlog age is workload, not readiness; stalled processing is
            # reported independently by the Worker and downloader heartbeats.
            "healthy": True,
            "detail": (
                f"处理中 {queue_depth} 张，最老 {queue_oldest_age} 秒"
                if queue_depth
                else "队列已清空"
            ),
        }
        services["recovery"] = {
            "healthy": bool(services["worker"]["healthy"]),
            "detail": (
                "断线或重启后自动从各群持久游标向前补齐"
                if services["worker"]["healthy"]
                else "Worker 恢复后将自动补齐断档"
            ),
        }
        groups = self.repository.list_groups()
        return {
            "timestamp": int(time.time()),
            "services": services,
            "account": health.get("account"),
            "action": self.supervisor.action(),
            "migration": self.migration.state(),
            "statistics": statistics,
            "groups": groups,
            "jobs": self.repository.list_jobs(30),
            "setup": self.repository.setup_status(groups),
            "remote": {
                "enabled": self.config.remote_enabled,
                "public_origin": self.config.remote_public_origin,
                "snapshot": self.publisher.state() if self.publisher else {"enabled": False},
            },
        }

    def public_snapshot(self) -> dict[str, Any]:
        complete = self.status()
        health = {"account": complete.get("account")}
        services = complete["services"]
        groups = complete["groups"]
        action = self.supervisor.action()
        return {
            "schema_version": 2,
            "generated_at": int(time.time()),
            "account": health.get("account"),
            "services": {
                key: {"healthy": bool(value.get("healthy"))}
                for key, value in services.items()
            },
            "statistics": complete["statistics"],
            "groups": [
                {
                    "group_id": row.get("group_id"),
                    "display_name": row.get("display_name"),
                    "enabled": bool(row.get("enabled")),
                    "event_status": row.get("event_status"),
                    "last_event_at": row.get("last_event_at"),
                    "last_image_at": row.get("last_image_at"),
                    "gap_status": row.get("gap_status"),
                    "queued": int(row.get("queued") or 0),
                    "accepted": int(row.get("accepted") or 0),
                    "duplicates": int(row.get("duplicates") or 0),
                    "rejected": int(row.get("rejected") or 0),
                    "failed": int(row.get("failed") or 0),
                }
                for row in groups
            ],
            "jobs": [
                {
                    "id": row.get("id"),
                    "kind": row.get("kind"),
                    "group_id": row.get("group_id"),
                    "status": row.get("status"),
                    "progress_pages": row.get("progress_pages"),
                    "updated_at": row.get("updated_at"),
                }
                for row in self.repository.list_jobs(30)
            ],
            "action": {
                "name": action.get("name"),
                "status": action.get("status"),
                "stage": action.get("stage"),
            },
        }


@dataclass(frozen=True)
class AuthContext:
    mode: Literal["local", "remote", "direct"]
    identity: RemoteIdentity | None = None


def _frontend_directory() -> Path | None:
    candidates = [
        Path(__file__).resolve().parent / "static",
        Path(__file__).resolve().parents[1] / "frontend" / "dist",
    ]
    return next((candidate for candidate in candidates if (candidate / "index.html").is_file()), None)


def _is_loopback_host(host: str) -> bool:
    return host.lower().strip("[]") in {"127.0.0.1", "localhost", "::1", "testserver"}


def _is_trusted_peer(host: str, config: ConsoleConfig) -> bool:
    if _is_loopback_host(host):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    for value in config.trusted_proxy_cidrs:
        try:
            if address in ipaddress.ip_network(value, strict=False):
                return True
        except ValueError:
            continue
    return False


def create_app(
    config: ConsoleConfig,
    token: str,
    *,
    testing: bool = False,
    repository: Repository | None = None,
    health: HealthService | None = None,
    supervisor: ProcessSupervisor | None = None,
    access_verifier: Any | None = None,
) -> FastAPI:
    repository = repository or Repository(config)
    health = health or HealthService(config)
    supervisor = supervisor or ProcessSupervisor(config, health)
    context = AppContext(
        config=config,
        token=token,
        repository=repository,
        health=health,
        supervisor=supervisor,
        migration=StorageMigrationManager(),
        status_cache_enabled=not testing,
    )

    remote_origin = urlparse(config.remote_public_origin or "")
    if config.remote_enabled:
        if (
            remote_origin.scheme != "https"
            or not remote_origin.hostname
            or remote_origin.path not in {"", "/"}
            or remote_origin.query
            or remote_origin.fragment
        ):
            raise ValueError("remote_public_origin 必须是无路径的 HTTPS 地址")
        access_verifier = access_verifier or CloudflareAccessVerifier(
            str(config.remote_access_issuer or ""),
            str(config.remote_access_audience or ""),
            str(config.remote_allowed_email or ""),
        )
    remote_sessions = RemoteSessionStore()
    publisher = SnapshotPublisher(
        config.snapshot_ingest_url,
        config.snapshot_secret_path,
        context.public_snapshot,
        config.snapshot_interval_seconds,
    )
    context.publisher = publisher

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> Any:
        context.start_status_refresh()
        publisher.start()
        try:
            yield
        finally:
            publisher.stop()

    app = FastAPI(
        title="QQ AI 原图采集控制台",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.context = context

    @app.middleware("http")
    async def security_boundary(request: Request, call_next: Any) -> Response:
        host_header = request.headers.get("host", "")
        try:
            parsed_host = urlparse(f"//{host_header}")
            hostname = parsed_host.hostname or ""
            host_port = parsed_host.port
        except ValueError:
            return JSONResponse({"detail": "Host 格式无效"}, status_code=400)
        allowed_local_ports = {None, config.port, *config.local_forward_ports}
        local_request = (
            _is_loopback_host(hostname)
            and host_port in allowed_local_ports
        )
        direct_public_hosts = {
            value.casefold().strip("[]")
            for value in config.direct_public_hosts
            if value.strip()
        }
        direct_public_request = bool(
            config.direct_public_enabled
            and hostname.casefold().strip("[]") in direct_public_hosts
            and host_port == config.direct_public_port
        )
        remote_request = bool(
            config.remote_enabled
            and remote_origin.hostname
            and hostname.casefold() == remote_origin.hostname.casefold()
            and host_port in {None, 443}
        )
        if not local_request and not direct_public_request and not remote_request:
            return JSONResponse({"detail": "Host 不允许"}, status_code=400)
        if (
            request.client
            and not testing
            and not direct_public_request
            and not _is_trusted_peer(request.client.host, config)
        ):
            return JSONResponse({"detail": "只允许本机或本机 Tunnel 访问"}, status_code=403)
        request.state.access_mode = (
            "remote" if remote_request
            else "direct" if direct_public_request
            else "local"
        )
        origin = request.headers.get("origin")
        if origin:
            try:
                parsed = urlparse(origin)
                origin_port = parsed.port
            except ValueError:
                return JSONResponse({"detail": "Origin 格式无效"}, status_code=403)
            if remote_request:
                supplied_origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
                expected_origin = f"{remote_origin.scheme}://{remote_origin.netloc}".rstrip("/")
                if supplied_origin.casefold() != expected_origin.casefold():
                    return JSONResponse({"detail": "Origin 不允许"}, status_code=403)
            elif direct_public_request:
                if (
                    parsed.scheme not in {"http", "https"}
                    or (parsed.hostname or "").casefold().strip("[]")
                    != hostname.casefold().strip("[]")
                    or origin_port != config.direct_public_port
                ):
                    return JSONResponse({"detail": "Origin 不允许"}, status_code=403)
            else:
                if not _is_loopback_host(parsed.hostname or ""):
                    return JSONResponse({"detail": "Origin 不允许"}, status_code=403)
                if origin_port not in allowed_local_ports and not testing:
                    return JSONResponse({"detail": "Origin 端口不允许"}, status_code=403)
        if remote_request and not testing:
            forwarded_proto = request.headers.get("x-forwarded-proto", "").casefold()
            if forwarded_proto != "https":
                return JSONResponse({"detail": "公网入口必须使用 HTTPS"}, status_code=403)
        response = await call_next(request)
        if (
            (remote_request or direct_public_request)
            and request.method.upper() in STATE_CHANGING_METHODS
            and request.url.path.startswith("/api/v1/")
        ):
            identity = getattr(request.state, "remote_identity", None)
            identity_label = (
                identity.email if identity
                else "direct-token" if direct_public_request
                else "unauthenticated"
            )
            source_ip = (
                request.headers.get("cf-connecting-ip")
                if remote_request
                else request.client.host if request.client
                else None
            )
            try:
                await asyncio.to_thread(
                    context.repository.record_remote_audit,
                    identity_label,
                    f"{request.method.upper()} {request.url.path}",
                    response.status_code,
                    source_ip,
                )
            except Exception:
                pass
        if request.url.path.startswith("/assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        else:
            response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; "
            "form-action 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self'; connect-src 'self'"
        )
        if remote_request:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response

    def require_auth(request: Request) -> AuthContext:
        if getattr(request.state, "access_mode", "local") == "remote":
            if access_verifier is None:
                raise HTTPException(status_code=503, detail="公网鉴权尚未配置")
            assertion = request.headers.get("cf-access-jwt-assertion", "")
            try:
                identity = access_verifier.verify(assertion)
            except RemoteAuthenticationError as exc:
                raise HTTPException(status_code=401, detail=str(exc)) from exc
            request.state.remote_identity = identity
            return AuthContext(mode="remote", identity=identity)
        supplied = request.cookies.get(SESSION_COOKIE) or request.headers.get("X-Local-Token")
        if not supplied or not secrets.compare_digest(supplied, token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="本地会话无效")
        access_mode = getattr(request.state, "access_mode", "local")
        return AuthContext(mode="direct" if access_mode == "direct" else "local")

    def require_mutation(request: Request) -> AuthContext:
        auth = require_auth(request)
        if auth.mode == "remote" and auth.identity:
            try:
                remote_sessions.validate(
                    request.cookies.get(REMOTE_SESSION_COOKIE),
                    request.headers.get("x-csrf-token"),
                    auth.identity,
                )
            except RemoteAuthenticationError as exc:
                code = 429 if "频繁" in str(exc) else 403
                raise HTTPException(status_code=code, detail=str(exc)) from exc
        return auth

    def require_local(request: Request) -> AuthContext:
        auth = require_auth(request)
        if auth.mode == "remote":
            raise HTTPException(status_code=403, detail="此操作只能在本机控制台完成")
        return auth

    @app.get("/health")
    def manager_health() -> dict[str, Any]:
        return {"ok": True, "service": "qq-image-collector-console", "version": __version__}

    @app.get("/api/v1/session")
    def establish_session(
        request: Request,
        response: Response,
        session_token: str | None = None,
    ) -> dict[str, Any]:
        if getattr(request.state, "access_mode", "local") == "remote":
            auth = require_auth(request)
            assert auth.identity is not None
            remote_session = remote_sessions.create(auth.identity)
            response.set_cookie(
                REMOTE_SESSION_COOKIE,
                remote_session.token,
                httponly=True,
                samesite="strict",
                secure=True,
                max_age=remote_sessions.lifetime_seconds,
                path="/",
            )
            return {
                "ok": True,
                "mode": "remote",
                "identity": {"email": auth.identity.email},
                "csrf_token": remote_session.csrf_token,
                "permissions": REMOTE_PERMISSIONS,
            }
        supplied = session_token or request.cookies.get(SESSION_COOKIE) or request.headers.get("X-Local-Token")
        if not supplied or not secrets.compare_digest(supplied, token):
            raise HTTPException(status_code=401, detail="Token 无效")
        response.set_cookie(
            SESSION_COOKIE,
            token,
            httponly=True,
            samesite="strict",
            secure=False,
            max_age=7 * 24 * 60 * 60,
            path="/",
        )
        access_mode = getattr(request.state, "access_mode", "local")
        return {
            "ok": True,
            "mode": access_mode,
            "identity": None,
            "csrf_token": None,
            "permissions": ["*"],
        }

    @app.get("/api/v1/status")
    def get_status(auth: AuthContext = Depends(require_auth)) -> dict[str, Any]:
        payload = context.status()
        payload["access"] = {
            "mode": auth.mode,
            "identity": {"email": auth.identity.email} if auth.identity else None,
            "permissions": REMOTE_PERMISSIONS if auth.mode == "remote" else ["*"],
        }
        return payload

    def run_system_action(action: str, request: SystemActionRequest) -> JSONResponse | dict[str, Any]:
        try:
            if action == "start":
                result = context.supervisor.request_start(request.confirm_close_qq)
            elif action == "stop":
                result = context.supervisor.request_stop()
            else:
                result = context.supervisor.request_restart(request.confirm_close_qq)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if result.get("confirmation_required"):
            return JSONResponse(result, status_code=409)
        return result

    @app.post("/api/v1/system/start", dependencies=[Depends(require_mutation)])
    def system_start(request: SystemActionRequest = SystemActionRequest()) -> Any:
        return run_system_action("start", request)

    @app.post("/api/v1/system/stop", dependencies=[Depends(require_mutation)])
    def system_stop(request: SystemActionRequest = SystemActionRequest()) -> Any:
        return run_system_action("stop", request)

    @app.post("/api/v1/system/restart", dependencies=[Depends(require_mutation)])
    def system_restart(request: SystemActionRequest = SystemActionRequest()) -> Any:
        return run_system_action("restart", request)

    @app.get("/api/v1/groups", dependencies=[Depends(require_auth)])
    def groups() -> list[dict[str, Any]]:
        return context.repository.list_groups()

    @app.get("/api/v1/groups/available", dependencies=[Depends(require_auth)])
    def available_groups() -> list[dict[str, Any]]:
        try:
            rows = context.health.available_groups()
            context.repository.update_group_names(rows)
            return rows
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"无法读取 QQ 群列表：{exc}") from exc

    @app.post("/api/v1/groups", status_code=201, dependencies=[Depends(require_mutation)])
    def add_group(request: GroupCreate) -> dict[str, str]:
        try:
            context.repository.upsert_group(request.group_id, request.display_name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"group_id": request.group_id}

    @app.delete("/api/v1/groups/{group_id}", dependencies=[Depends(require_mutation)])
    def remove_group(group_id: str) -> dict[str, bool]:
        try:
            context.repository.disable_group(group_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"disabled": True}

    @app.post("/api/v1/groups/{group_id}/backfill", dependencies=[Depends(require_mutation)])
    def retired_backfill(group_id: str) -> JSONResponse:
        del group_id
        return JSONResponse(
            status_code=status.HTTP_410_GONE,
            content={"detail": "任意历史回填已永久停用；只能恢复本次 WebSocket 断档。"},
        )

    @app.post(
        "/api/v1/groups/{group_id}/recover-gap",
        dependencies=[Depends(require_mutation)],
    )
    def recover_gap(group_id: str) -> JSONResponse:
        try:
            job_id = context.repository.create_job(group_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content={"job_id": job_id})

    @app.get("/api/v1/jobs", dependencies=[Depends(require_auth)])
    def jobs() -> list[dict[str, Any]]:
        return context.repository.list_jobs()

    @app.post("/api/v1/jobs/{job_id}/cancel", dependencies=[Depends(require_mutation)])
    def cancel_job(job_id: int) -> dict[str, bool]:
        if not context.repository.cancel_job(job_id):
            raise HTTPException(status_code=404, detail="没有可取消的任务")
        return {"cancel_requested": True}

    @app.get("/api/v1/settings")
    def settings(auth: AuthContext = Depends(require_auth)) -> dict[str, Any]:
        values = context.repository.get_app_settings()
        if auth.mode == "remote":
            allowed = {
                "download_interval_seconds",
                "download_jitter_seconds",
                "url_preference",
                "collector_paused",
            }
            return {key: value for key, value in values.items() if key in allowed} | {
                "remote_restricted": True
            }
        return values

    @app.patch("/api/v1/settings")
    def patch_settings(
        request: SettingsPatch,
        auth: AuthContext = Depends(require_mutation),
    ) -> dict[str, Any]:
        values = request.model_dump(exclude_none=True)
        if auth.mode == "remote":
            allowed = {
                "download_interval_seconds",
                "download_jitter_seconds",
                "url_preference",
                "collector_paused",
            }
            forbidden = sorted(set(values) - allowed)
            if forbidden:
                raise HTTPException(status_code=403, detail="公网端不能修改本机路径或自启设置")
        if (
            not context.config.external_services
            and "qq_path" in values
            and not Path(values["qq_path"]).is_file()
        ):
            raise HTTPException(status_code=400, detail="QQ.exe 路径不存在")
        if (
            not context.config.external_services
            and "napcat_root" in values
            and not Path(values["napcat_root"]).is_dir()
        ):
            raise HTTPException(status_code=400, detail="NapCat 目录不存在")
        if (
            not context.config.external_services
            and "shell_launcher" in values
            and values["shell_launcher"]
            and not Path(values["shell_launcher"]).is_file()
        ):
            raise HTTPException(status_code=400, detail="Shell 启动器路径不存在")
        try:
            updated = context.repository.patch_app_settings(values)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if auth.mode == "remote":
            return {key: updated[key] for key in allowed} | {"remote_restricted": True}
        return updated

    @app.get("/api/v1/setup")
    def setup_status(auth: AuthContext = Depends(require_auth)) -> dict[str, Any]:
        values = context.repository.setup_status()
        if auth.mode == "remote":
            values = dict(values)
            values["checks"] = [
                {"key": row["key"], "label": row["label"], "ok": row["ok"], "detail": ""}
                for row in values["checks"]
            ]
        return values

    @app.post("/api/v1/setup/complete", dependencies=[Depends(require_local)])
    def complete_setup() -> dict[str, Any]:
        try:
            return context.repository.complete_setup()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/storage/migrate", status_code=202, dependencies=[Depends(require_local)])
    def migrate(request: StorageMigrationRequest) -> dict[str, Any]:
        if context.supervisor.action_running():
            raise HTTPException(status_code=409, detail="请等待当前系统操作结束")
        try:
            return context.migration.start(
                Path(request.destination), context.config, context.supervisor
            )
        except (ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/storage/open", dependencies=[Depends(require_local)])
    def open_folder(request: OpenFolderRequest) -> dict[str, str]:
        path = Path(request.path) if request.path else context.config.storage_root()
        if not path.is_dir():
            raise HTTPException(status_code=404, detail="文件夹不存在")
        if os.name != "nt":
            raise HTTPException(status_code=501, detail="打开文件夹目前只支持 Windows")
        os.startfile(str(path))
        return {"opened": str(path)}

    @app.get("/api/v1/logs", dependencies=[Depends(require_local)])
    def logs(lines: int = 200) -> dict[str, Any]:
        line_limit = max(1, min(lines, 2000))
        candidates = [
            context.config.storage_root() / "logs" / "collector-console.log",
            context.config.storage_root() / "logs" / "collector.err.log",
            context.config.storage_root() / "logs" / "collector.out.log",
        ]
        result: list[dict[str, Any]] = []
        for path in candidates:
            if not path.is_file():
                continue
            try:
                content = path.read_text(encoding="utf-8", errors="replace").splitlines()
                result.append({"path": str(path), "lines": content[-line_limit:]})
            except OSError:
                continue
        return {"files": result}

    @app.get("/api/v1/audit", dependencies=[Depends(require_auth)])
    def audit(limit: int = 100) -> dict[str, Any]:
        return {"entries": context.repository.list_remote_audit(limit)}

    @app.get("/api/v1/events")
    async def events(
        request: Request,
        auth: AuthContext = Depends(require_auth),
    ) -> StreamingResponse:
        async def stream() -> Any:
            while True:
                if await request.is_disconnected():
                    break
                payload = await asyncio.to_thread(context.status)
                payload["access"] = {
                    "mode": auth.mode,
                    "identity": {"email": auth.identity.email} if auth.identity else None,
                    "permissions": REMOTE_PERMISSIONS if auth.mode == "remote" else ["*"],
                }
                yield f"event: status\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                await asyncio.sleep(2)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Connection": "keep-alive"},
        )

    frontend = _frontend_directory()
    if frontend:
        app.mount("/", StaticFiles(directory=str(frontend), html=True), name="frontend")
    else:
        @app.get("/", response_class=HTMLResponse)
        def frontend_missing() -> str:
            return (
                "<html><meta charset='utf-8'><body><h1>QQ AI 原图采集控制台</h1>"
                "<p>前端尚未构建，请在 frontend 目录执行 npm run build。</p></body></html>"
            )

    return app
