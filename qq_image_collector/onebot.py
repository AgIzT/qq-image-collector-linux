from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

import httpx


ALLOWED_ACTIONS = frozenset(
    {
        "get_login_info",
        "get_group_list",
        "get_version_info",
        "get_group_msg_history",
    }
)


class OneBotError(RuntimeError):
    pass


class OneBotPolicyError(OneBotError):
    pass


def _read_token(settings: dict[str, Any]) -> str:
    token_file = str(settings.get("token_file") or "").strip()
    if token_file:
        return Path(token_file).read_text(encoding="utf-8").strip()
    return str(settings.get("token") or "")


class OneBotClient:
    """OneBot HTTP client with a fail-closed action allowlist."""

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: int = 30,
        on_policy_violation: Callable[[str], None] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.on_policy_violation = on_policy_violation

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> "OneBotClient":
        base_url = str(settings.get("base_url") or "").strip()
        if not base_url:
            raise OneBotError("onebot.base_url is required")
        return cls(base_url, _read_token(settings), int(settings.get("timeout_seconds", 30)))

    def _check_action(self, action: str) -> None:
        if action not in ALLOWED_ACTIONS:
            if self.on_policy_violation is not None:
                self.on_policy_violation(action)
            raise OneBotPolicyError(f"OneBot action is blocked by policy: {action}")

    @property
    def headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @staticmethod
    def _unwrap(action: str, body: Any) -> Any:
        if not isinstance(body, dict):
            raise OneBotError(f"{action} returned a non-object response")
        if body.get("status") != "ok" or int(body.get("retcode", -1)) != 0:
            detail = body.get("message") or body.get("wording") or body
            raise OneBotError(f"{action} failed: {detail}")
        return body.get("data")

    def call(self, action: str, params: dict[str, Any] | None = None) -> Any:
        self._check_action(action)
        request = urllib.request.Request(
            f"{self.base_url}/{action}",
            data=json.dumps(params or {}, ensure_ascii=False).encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return self._unwrap(action, json.load(response))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise OneBotError(f"{action} request failed: HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise OneBotError(f"{action} request failed: {exc}") from exc

    async def call_async(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> Any:
        self._check_action(action)
        owns_client = client is None
        http = client or httpx.AsyncClient(timeout=self.timeout)
        try:
            response = await http.post(
                f"{self.base_url}/{action}",
                headers=self.headers,
                json=params or {},
            )
            response.raise_for_status()
            return self._unwrap(action, response.json())
        except (httpx.HTTPError, ValueError) as exc:
            raise OneBotError(f"{action} request failed: {exc}") from exc
        finally:
            if owns_client:
                await http.aclose()


def websocket_settings(settings: dict[str, Any]) -> tuple[str, str]:
    url = str(settings.get("ws_url") or "").strip()
    if not url:
        raise OneBotError("onebot.ws_url is required")
    return url, _read_token(settings)
