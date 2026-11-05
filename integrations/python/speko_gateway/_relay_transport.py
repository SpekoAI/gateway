"""Shared explicit, session-bound HTTP transport for native relay sockets."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import re
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from ._relay_socket import NATIVE_SOCKET_ORIGIN, socket_headers, validate_session_id
from ._user_agent import USER_AGENT
from .client import GatewayError


def _error(payload: dict[str, Any], *, status: int | None = None) -> GatewayError:
    value = payload.get("error", payload)
    value = value if isinstance(value, dict) else {}
    code = value.get("code", "")
    if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", code):
        code = ""
    request_id = value.get("request_id", "")
    if not isinstance(request_id, str) or not re.fullmatch(r"[!-~]{1,256}", request_id):
        request_id = ""
    return GatewayError(
        "Relay speech request failed",
        code=code,
        source="relay",
        request_id=request_id,
        status=status,
        # A relay retry hint does not prove that this model request did no I/O.
        # Framework retries open a new request and could charge it twice.
        retryable=False,
    )


class _RelayTransport:
    def __init__(
        self,
        *,
        api_key: str,
        session_id: str,
        base_url: str | None = None,
        setup_timeout: float = 15,
        socket_path: str | None = None,
        local_auth_token: str | None = None,
    ) -> None:
        if not api_key.startswith("sk_speko_") or any(
            ord(char) < 33 or ord(char) > 126 for char in api_key
        ):
            raise ValueError("A Speko model credential is required")
        validate_session_id(session_id)
        local_headers: dict[str, str] = {}
        if socket_path is not None:
            if base_url is not None:
                raise ValueError("Specify a relay origin or native socket, not both")
            local_headers = socket_headers(
                socket_path=socket_path,
                local_auth_token=local_auth_token,
                api_key=api_key,
                session_id=session_id,
            )
            base_url = NATIVE_SOCKET_ORIGIN
        elif local_auth_token is not None:
            raise ValueError("local_auth_token requires a native Gateway socket")
        if base_url is None:
            raise ValueError("A configured relay origin or native socket is required")
        parsed = urlsplit(base_url)
        loopback = parsed.hostname == "localhost"
        try:
            loopback = (
                loopback or ipaddress.ip_address(parsed.hostname or "").is_loopback
            )
        except ValueError:
            pass
        if socket_path is None and (
            parsed.scheme not in {"https", "http"}
            or not parsed.hostname
            or parsed.scheme == "http"
            and not loopback
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("Relay origin must be HTTPS (HTTP is allowed on loopback)")
        if setup_timeout <= 0:
            raise ValueError("setup_timeout must be positive")
        self._origin = base_url.rstrip("/")
        self._platform_session_id = session_id
        self._setup_timeout = setup_timeout
        trace = aiohttp.TraceConfig()

        async def reject_redirect(*_: Any) -> None:
            raise GatewayError("Relay redirects are not accepted", retryable=False)

        trace.on_request_redirect.append(reject_redirect)

        async def preserve_rejection(_session, _context, params):
            response = params.response
            if response.status < 400:
                return
            try:
                try:
                    raw = await response.content.readexactly(8193)
                except asyncio.IncompleteReadError as incomplete:
                    raw = incomplete.partial
                value = json.loads(raw) if len(raw) <= 8192 else None
            except (ValueError, aiohttp.ClientError):
                value = None
            finally:
                response.close()
            if (
                isinstance(value, dict)
                and isinstance(value.get("error"), dict)
                and isinstance(value["error"].get("hint"), str)
                and isinstance(value["error"].get("retryable"), bool)
            ):
                raise _error(value, status=response.status)
            raise GatewayError(
                "Relay speech handshake failed",
                source="relay",
                code=f"http_{response.status}",
                status=response.status,
                retryable=False,
            )

        trace.on_request_end.append(preserve_rejection)
        self._session = aiohttp.ClientSession(
            connector=aiohttp.UnixConnector(path=socket_path)
            if socket_path is not None
            else None,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": USER_AGENT,
                "Speko-Client-Session-ID": session_id,
                **local_headers,
            },
            trace_configs=[trace],
            timeout=aiohttp.ClientTimeout(total=None, connect=setup_timeout),
        )

    async def aclose(self) -> None:
        await self._session.close()
