from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


REMOTE_SESSION_COOKIE = "qqic_remote_session"


class RemoteAuthenticationError(ValueError):
    """Raised when a Cloudflare Access assertion cannot be trusted."""


@dataclass(frozen=True)
class RemoteIdentity:
    subject: str
    email: str


@dataclass(frozen=True)
class RemoteSession:
    token: str
    csrf_token: str
    identity: RemoteIdentity
    expires_at: int


class CloudflareAccessVerifier:
    def __init__(self, issuer: str, audience: str, allowed_email: str) -> None:
        self.issuer = issuer.rstrip("/")
        self.audience = audience.strip()
        self.allowed_email = allowed_email.strip().casefold()
        if not self.issuer.startswith("https://"):
            raise ValueError("Cloudflare Access issuer 必须使用 HTTPS")
        if not self.audience or not self.allowed_email:
            raise ValueError("Cloudflare Access audience 和允许邮箱不能为空")
        try:
            import jwt
        except ImportError as exc:  # pragma: no cover - packaging guard
            raise RuntimeError("缺少 PyJWT，无法启用公网鉴权") from exc
        self._jwt = jwt
        self._jwks = jwt.PyJWKClient(
            f"{self.issuer}/cdn-cgi/access/certs",
            cache_jwk_set=True,
            lifespan=3600,
        )

    def verify(self, assertion: str) -> RemoteIdentity:
        if not assertion or len(assertion) > 16_384:
            raise RemoteAuthenticationError("Cloudflare Access 会话无效")
        try:
            signing_key = self._jwks.get_signing_key_from_jwt(assertion)
            payload = self._jwt.decode(
                assertion,
                signing_key.key,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["exp", "iat", "iss", "aud", "sub"]},
            )
        except Exception as exc:
            raise RemoteAuthenticationError("Cloudflare Access 会话校验失败") from exc
        email = str(payload.get("email") or "").strip().casefold()
        subject = str(payload.get("sub") or "").strip()
        if not email or not subject or not secrets.compare_digest(email, self.allowed_email):
            raise RemoteAuthenticationError("当前 Cloudflare 身份不在允许名单")
        return RemoteIdentity(subject=subject, email=email)


class RemoteSessionStore:
    def __init__(self, lifetime_seconds: int = 8 * 60 * 60) -> None:
        self.lifetime_seconds = lifetime_seconds
        self._sessions: dict[str, RemoteSession] = {}
        self._mutations: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def create(self, identity: RemoteIdentity) -> RemoteSession:
        now = int(time.time())
        session = RemoteSession(
            token=secrets.token_urlsafe(32),
            csrf_token=secrets.token_urlsafe(32),
            identity=identity,
            expires_at=now + self.lifetime_seconds,
        )
        with self._lock:
            self._purge_locked(now)
            self._sessions[session.token] = session
        return session

    def validate(
        self,
        token: str | None,
        csrf_token: str | None,
        identity: RemoteIdentity,
        *,
        mutation_limit: int = 30,
        mutation_window_seconds: int = 60,
    ) -> RemoteSession:
        now = int(time.time())
        with self._lock:
            self._purge_locked(now)
            session = self._sessions.get(token or "")
            if (
                session is None
                or session.identity.subject != identity.subject
                or session.identity.email != identity.email
                or not csrf_token
                or not secrets.compare_digest(csrf_token, session.csrf_token)
            ):
                raise RemoteAuthenticationError("远程会话或 CSRF 校验失败")
            bucket = self._mutations[identity.subject]
            cutoff = time.monotonic() - mutation_window_seconds
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= mutation_limit:
                raise RemoteAuthenticationError("远程操作过于频繁，请稍后重试")
            bucket.append(time.monotonic())
            return session

    def _purge_locked(self, now: int) -> None:
        expired = [token for token, value in self._sessions.items() if value.expires_at <= now]
        for token in expired:
            del self._sessions[token]


def load_or_create_secret(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        value = path.read_text(encoding="ascii").strip()
        if len(value) >= 43:
            return value
        raise ValueError("状态快照密钥文件无效")
    value = secrets.token_urlsafe(48)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="ascii")
    temporary.replace(path)
    try:
        import os

        os.chmod(path, 0o600)
    except OSError:
        pass
    return value


class SnapshotPublisher:
    def __init__(
        self,
        endpoint: str | None,
        secret_file: Path,
        snapshot_factory: Callable[[], dict[str, Any]],
        interval_seconds: int = 60,
    ) -> None:
        self.endpoint = (endpoint or "").strip()
        self.secret_file = secret_file
        self.snapshot_factory = snapshot_factory
        self.interval_seconds = max(30, int(interval_seconds))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._state: dict[str, Any] = {
            "enabled": bool(self.endpoint),
            "last_success": None,
            "last_error": None,
        }

    def start(self) -> None:
        if not self.endpoint or self._thread is not None:
            return
        if not self.endpoint.startswith("https://"):
            self._set_state(last_error="快照上传地址必须使用 HTTPS")
            return
        self._thread = threading.Thread(
            target=self._run,
            name="remote-snapshot-publisher",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def state(self) -> dict[str, Any]:
        with self._state_lock:
            return dict(self._state)

    def publish_once(self) -> None:
        secret = load_or_create_secret(self.secret_file)
        body = json.dumps(
            self.snapshot_factory(),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > 256 * 1024:
            raise ValueError("状态快照超过 256 KiB")
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)
        message = timestamp.encode("ascii") + b"." + nonce.encode("ascii") + b"." + body
        signature = hmac.new(secret.encode("ascii"), message, hashlib.sha256).hexdigest()
        request = urllib.request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "QQImageCollectorConsole/0.3",
                "X-QQIC-Timestamp": timestamp,
                "X-QQIC-Nonce": nonce,
                "X-QQIC-Signature": signature,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                if response.status != 204:
                    raise RuntimeError(f"状态服务返回 HTTP {response.status}")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"状态服务返回 HTTP {exc.code}") from exc
        self._set_state(last_success=int(time.time()), last_error=None)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.publish_once()
            except Exception as exc:
                self._set_state(last_error=str(exc)[:300])
            self._stop.wait(self.interval_seconds)

    def _set_state(self, **values: Any) -> None:
        with self._state_lock:
            self._state.update(values)
