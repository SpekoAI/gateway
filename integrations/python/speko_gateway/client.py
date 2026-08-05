"""Versioned Unix-socket client for the public Speko Gateway protocol."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

_SUBPROTOCOL = "speko.voice.v0.r3"
_BASE_URL = "http://speko-gateway"
_DEFAULT_SOCKET_PATH = "/run/speko/runtime.sock"


class GatewayError(RuntimeError):
    """A local Gateway HTTP or stream failure without sensitive plan details."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "",
        source: str = "",
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.source = source
        self.retryable = retryable


@dataclass(frozen=True)
class SessionConfig:
    """Provider-neutral session fields forwarded to ``POST /v1/sessions``."""

    kind: str
    execution: dict[str, Any]
    request: dict[str, Any] = field(default_factory=dict)
    media: dict[str, Any] | None = None
    integration: dict[str, Any] | None = None

    def as_json(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "kind": self.kind,
            "execution": self.execution,
            "request": self.request,
        }
        if self.media is not None:
            body["media"] = self.media
        if self.integration is not None:
            body["integration"] = self.integration
        return body


@dataclass(frozen=True)
class CanonicalEvent:
    """A text event or binary audio frame from the canonical WebSocket."""

    type: str
    data: dict[str, Any] = field(default_factory=dict)
    session_id: str = ""
    attempt_id: str = ""
    sequence: int = 0
    audio: bytes | None = None
    extensions: dict[str, Any] = field(default_factory=dict)


class GatewayClient:
    """Own one authenticated aiohttp transport to the local Gateway."""

    def __init__(self, *, socket_path: str, local_auth_token: str) -> None:
        if not socket_path or not local_auth_token:
            raise ValueError("socket_path and local_auth_token are required")
        self._headers = {"Authorization": f"Bearer {local_auth_token}"}
        self._session = aiohttp.ClientSession(
            connector=aiohttp.UnixConnector(path=socket_path),
            headers=self._headers,
            raise_for_status=False,
        )

    @classmethod
    def from_env(cls) -> GatewayClient:
        """Create a client using the same local variables as Gateway."""

        socket_path = os.environ.get("SPEKO_SOCKET_PATH", _DEFAULT_SOCKET_PATH).strip()
        token = _env_secret("SPEKO_LOCAL_AUTH_TOKEN")
        if not token:
            raise ValueError("SPEKO_LOCAL_AUTH_TOKEN is required")
        return cls(socket_path=socket_path, local_auth_token=token)

    async def aclose(self) -> None:
        await self._session.close()

    async def ready(self) -> bool:
        async with self._session.get(f"{_BASE_URL}/readyz") as response:
            return response.status == 200

    async def open(
        self, config: SessionConfig, *, idempotency_key: str | None = None
    ) -> GatewaySession:
        headers = {"Idempotency-Key": idempotency_key or str(uuid.uuid4())}
        async with self._session.post(
            f"{_BASE_URL}/v1/sessions", json=config.as_json(), headers=headers
        ) as response:
            body = await _decode_json(response)
        if response.status not in (200, 201):
            raise GatewayError(_error_message(response.status, body))
        stream_url = body.get("stream_url")
        if not isinstance(stream_url, str) or not stream_url.startswith(
            "/v1/sessions/"
        ):
            raise GatewayError(
                "Gateway response did not include a canonical stream URL"
            )
        websocket = await self._session.ws_connect(
            f"{_BASE_URL}{stream_url}",
            protocols=[_SUBPROTOCOL],
            headers=self._headers,
        )
        if websocket.protocol != _SUBPROTOCOL:
            await websocket.close()
            raise GatewayError(
                "Gateway did not negotiate the current protocol revision"
            )
        return GatewaySession(websocket, body)


class GatewaySession:
    """One canonical Gateway stream. Read events from exactly one consumer."""

    def __init__(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        metadata: dict[str, Any],
    ) -> None:
        self._websocket = websocket
        self.metadata = metadata
        self._closed = False
        self._write_lock = asyncio.Lock()

    async def send_audio(self, data: bytes | bytearray | memoryview) -> None:
        await self._websocket.send_bytes(bytes(data))

    async def commit_audio(self) -> None:
        await self._command("audio.commit")

    async def append_text(self, text: str) -> None:
        if not text:
            raise ValueError("text must not be empty")
        await self._command("text.append", {"text": text})

    async def commit_text(self) -> None:
        await self._command("text.commit")

    async def cancel(self) -> None:
        await self._command("response.cancel")

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._command("session.close")
        finally:
            await self._websocket.close()

    async def events(self) -> AsyncIterator[CanonicalEvent]:
        async for message in self._websocket:
            if message.type is aiohttp.WSMsgType.BINARY:
                yield CanonicalEvent(type="audio.frame", audio=bytes(message.data))
                continue
            if message.type is aiohttp.WSMsgType.TEXT:
                payload = json.loads(message.data)
                yield CanonicalEvent(
                    type=str(payload.get("type", "")),
                    data=payload.get("data") or {},
                    session_id=str(payload.get("session_id", "")),
                    attempt_id=str(payload.get("attempt_id", "")),
                    sequence=int(payload.get("sequence", 0)),
                    extensions=payload.get("extensions") or {},
                )
                continue
            if message.type is aiohttp.WSMsgType.ERROR:
                raise GatewayError("Gateway WebSocket failed")

    async def _command(self, type_: str, data: dict[str, Any] | None = None) -> None:
        async with self._write_lock:
            if self._closed and type_ != "session.close":
                raise GatewayError("Gateway session is closed")
            await self._websocket.send_json({"type": type_, "data": data})


async def _decode_json(response: aiohttp.ClientResponse) -> dict[str, Any]:
    try:
        value = await response.json(content_type=None)
    except (aiohttp.ContentTypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _error_message(status: int, body: dict[str, Any]) -> str:
    error = body.get("error")
    if isinstance(error, dict) and isinstance(error.get("code"), str):
        return f"Gateway rejected request ({error['code']}, HTTP {status})"
    return f"Gateway rejected request (HTTP {status})"


def _env_secret(name: str) -> str:
    direct = os.environ.get(name, "").strip()
    file_name = os.environ.get(f"{name}_FILE", "").strip()
    if direct and file_name:
        raise ValueError(f"{name} and {name}_FILE are mutually exclusive")
    if not file_name:
        return direct
    path = Path(file_name)
    if path.stat().st_size > 64 * 1024:
        raise ValueError(f"{name}_FILE exceeds 64 KiB")
    return path.read_text(encoding="utf-8").strip()
