"""One native voice socket, bound to an explicit platform session credential.

Frames stay vendor-native. A successful HTTP upgrade is transport readiness,
not provider admission; protocol adapters must wait for the vendor's ready event.
No connection failure, including a failed upgrade, is automatically retried.
"""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from collections.abc import AsyncIterator
from typing import Any, Literal

import aiohttp

from ._relay_transport import _RelayTransport
from .client import GatewayError

RealtimeProtocol = Literal[
    "openai.realtime.v1",
    "xai.realtime.v1",
    "openai.live.v1",
    "google.live.v1",
    "openai.realtime.translation.v1",
    "alibaba.livetranslate.v1",
]

_ROUTES = {
    "openai.realtime.v1": ("/v1/openai/realtime", True),
    "xai.realtime.v1": ("/v1/xai/realtime", True),
    "openai.live.v1": ("/v1/openai/gpt-live", False),
    "google.live.v1": ("/v1/google/gemini-live", False),
    "openai.realtime.translation.v1": ("/v1/openai/realtime-translations", True),
    "alibaba.livetranslate.v1": ("/v1/alibaba/qwen-livetranslate", True),
}
_MODEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_FRAME_BYTES = 2 << 20


class RelayRealtimeClient(_RelayTransport):
    """Own one socket attempt; there is no environment or provider-key fallback."""

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
        super().__init__(
            api_key=api_key,
            session_id=session_id,
            base_url=base_url,
            setup_timeout=setup_timeout,
            socket_path=socket_path,
            local_auth_token=local_auth_token,
        )
        self._attempted = self._closed = False
        self._voice: RelayRealtimeSession | None = None

    async def open(
        self,
        *,
        protocol: RealtimeProtocol,
        model: str,
        idempotency_key: str | None = None,
    ) -> RelayRealtimeSession:
        if not isinstance(protocol, str) or protocol not in _ROUTES:
            raise ValueError("A supported native realtime protocol is required")
        if not isinstance(model, str) or not _MODEL.fullmatch(model) or model == "auto":
            raise ValueError("An exact bounded realtime model is required")
        key = str(uuid.uuid4()) if idempotency_key is None else idempotency_key
        if not isinstance(key, str) or not re.fullmatch(r"[!-~]{1,512}", key):
            raise ValueError("A bounded idempotency key is required")
        if self._closed or self._attempted:
            raise GatewayError("Realtime socket attempt is exhausted", retryable=False)
        # Set before the first await: concurrent callers and framework retries
        # cannot create another admission after an ambiguous first attempt.
        self._attempted = True
        path, query_model = _ROUTES[protocol]
        target = self._origin + path
        if query_model:
            target += "?model=" + model
        try:
            websocket = await asyncio.wait_for(
                self._session.ws_connect(
                    target,
                    headers={"Idempotency-Key": key},
                    max_msg_size=_FRAME_BYTES,
                    heartbeat=15,
                ),
                self._setup_timeout,
            )
        except aiohttp.WSServerHandshakeError as error:
            raise GatewayError(
                "Relay realtime handshake failed",
                source="relay",
                code=f"http_{error.status}",
                status=error.status,
                retryable=False,
            ) from None
        except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError):
            raise GatewayError(
                "Relay realtime connection failed", retryable=False
            ) from None
        self._voice = RelayRealtimeSession(websocket)
        return self._voice

    async def aclose(self) -> None:
        self._closed = True
        try:
            if self._voice is not None:
                await self._voice.aclose()
        finally:
            await super().aclose()


class RelayRealtimeSession:
    """Raw native frames and their close status, with one reader and bounded writes."""

    def __init__(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        self._websocket = websocket
        self._write_lock = asyncio.Lock()
        self._closed = self._reading = False

    @property
    def close_code(self) -> int | None:
        return self._websocket.close_code

    async def send(self, frame: str | bytes) -> None:
        if not isinstance(frame, (str, bytes)):
            raise TypeError("Native realtime frames must be text or bytes")
        size = len(frame.encode("utf-8")) if isinstance(frame, str) else len(frame)
        if not 0 < size <= _FRAME_BYTES:
            raise ValueError("Native realtime frame exceeds its bound")
        async with self._write_lock:
            if self._closed:
                raise GatewayError("Relay realtime socket is closed", retryable=False)
            try:
                write = (
                    self._websocket.send_str(frame)
                    if isinstance(frame, str)
                    else self._websocket.send_bytes(frame)
                )
                await asyncio.wait_for(write, 15)
            except (aiohttp.ClientError, OSError, TimeoutError, asyncio.TimeoutError):
                raise GatewayError(
                    "Relay realtime write failed", retryable=False
                ) from None

    async def send_json(self, event: dict[str, Any]) -> None:
        await self.send(
            json.dumps(
                event, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            )
        )

    async def events(self) -> AsyncIterator[str | bytes]:
        if self._reading:
            raise GatewayError("Realtime socket already has a reader", retryable=False)
        self._reading = True
        try:
            async for message in self._websocket:
                if message.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    yield message.data
                else:
                    raise GatewayError("Relay realtime read failed", retryable=False)
            if not self._closed and self.close_code not in (1000, 1001):
                raise GatewayError(
                    "Relay realtime socket ended abnormally", retryable=False
                )
        except (aiohttp.ClientError, OSError, TimeoutError, asyncio.TimeoutError):
            raise GatewayError("Relay realtime read failed", retryable=False) from None
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        await self._websocket.close()
