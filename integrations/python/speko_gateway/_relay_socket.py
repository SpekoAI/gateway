"""Explicit Unix-socket transport for the native relay sidecar."""

from __future__ import annotations

import uuid
from pathlib import Path

import aiohttp

NATIVE_SOCKET_ORIGIN = "http://speko-gateway"
NATIVE_LOCAL_AUTHORIZATION = "Speko-Local-Authorization"


def socket_headers(
    *, socket_path: str, local_auth_token: str | None, api_key: str, session_id: str
) -> dict[str, str]:
    if not socket_path or not Path(socket_path).is_absolute():
        raise ValueError("An absolute native Gateway socket_path is required")
    if not local_auth_token or any(
        ord(char) < 33 or ord(char) > 126 for char in local_auth_token
    ):
        raise ValueError("Native Gateway local_auth_token is required")
    if not api_key.startswith("sk_speko_") or any(
        ord(char) < 33 or ord(char) > 126 for char in api_key
    ):
        raise ValueError("A Speko model credential is required")
    try:
        uuid.UUID(session_id)
    except (ValueError, AttributeError):
        raise ValueError("A platform session UUID is required") from None
    return {NATIVE_LOCAL_AUTHORIZATION: f"Bearer {local_auth_token}"}


def reject_redirects() -> aiohttp.TraceConfig:
    trace = aiohttp.TraceConfig()

    async def reject(*_):
        # A redirect cannot move either the model bearer or socket credential.
        from .relay import RelayError

        raise RelayError("Relay redirects are not accepted", retryable=False)

    trace.on_request_redirect.append(reject)
    return trace
