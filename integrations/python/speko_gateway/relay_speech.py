"""Session-bound STT/TTS over the router's native execution-grant streams.

Credentials are explicit and remain on this client. The relay obtains its
execution grant from the cell before provider I/O; this client neither obtains
provider keys nor borrows a process-wide Gateway plan or credential.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import re
import uuid
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from ._relay_socket import NATIVE_SOCKET_ORIGIN, socket_headers
from ._user_agent import USER_AGENT
from .client import CanonicalEvent, GatewayError, SessionConfig

_EVENTS = {
    "stt": {"transcript.delta", "transcript.final", "usage.updated"},
    "tts": {
        "utterance.started",
        "utterance.done",
        "utterance.timings",
        "usage.updated",
    },
}
_EVENT_NAMES = {
    "utterance.started": "audio.started",
    "utterance.done": "audio.done",
    "utterance.timings": "audio.timings",
    "usage.updated": "usage.progress",
}


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


def _json(message: aiohttp.WSMessage) -> dict[str, Any]:
    if message.type is not aiohttp.WSMsgType.TEXT:
        raise GatewayError("Relay speech protocol failed", retryable=False)
    try:
        value = json.loads(message.data)
    except (ValueError, TypeError):
        raise GatewayError("Relay speech protocol failed", retryable=False) from None
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise GatewayError("Relay speech protocol failed", retryable=False)
    return value


class RelaySpeechClient:
    """One model credential and platform session, with no environment fallback.

    Supply ``credential_source='managed'`` to the LiveKit/Pipecat integrations
    using this client. A worker's TTS original voice stays blank: its cell signs
    the compiled voice override. Caller-provided voices are forwarded unchanged
    and remain subject to the cell's policy.
    """

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
        try:
            uuid.UUID(session_id)
        except (ValueError, AttributeError):
            raise ValueError("A platform session UUID is required") from None
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

    async def open(
        self,
        config: SessionConfig,
        *,
        idempotency_key: str | None = None,
    ) -> RelaySpeechSession:
        if config.kind not in _EVENTS:
            raise ValueError("RelaySpeechClient supports only STT and TTS")
        execution = config.execution
        if (
            execution.get("credential_source") != "managed"
            or execution.get("provider_route") not in {"auto", "speko_relay"}
            or execution.get("relay_policy") not in {"allowed", "required"}
        ):
            raise ValueError("Native relay speech requires managed relay execution")
        request = config.request
        if request.get("client_session_id") not in (
            None,
            "",
            self._platform_session_id,
        ):
            raise ValueError("The speech request belongs to another platform session")
        provider, model = request.get("provider"), request.get("model")
        if (
            not isinstance(provider, str)
            or not isinstance(model, str)
            or (not provider or provider == "auto" or not model or model == "auto")
        ):
            raise ValueError(
                "Native relay speech requires a concrete provider and model"
            )
        configure: dict[str, Any] = {
            "type": "session.configure",
            "routing": {"mode": "explicit", "provider": provider, "model": model},
            "audio": config.media,
            "language": request.get("language", ""),
        }
        if config.kind == "tts":
            configure["voice"] = request.get("voice", "")
        if request.get(config.kind):
            configure["options"] = request[config.kind]
        text_limit = request.get("max_input_characters")
        if text_limit is not None and (type(text_limit) is not int or text_limit <= 0):
            raise ValueError("max_input_characters must be a positive integer")
        websocket: aiohttp.ClientWebSocketResponse | None = None

        async def establish() -> RelaySpeechSession:
            nonlocal websocket
            websocket = await self._session.ws_connect(
                f"{self._origin}/v1/{config.kind}/stream",
                headers={"Idempotency-Key": idempotency_key or str(uuid.uuid4())},
                max_msg_size=2 << 20,
                heartbeat=15,
            )
            await websocket.send_json(configure)
            ready = _json(await websocket.receive())
            if ready["type"] == "error":
                raise _error(ready)
            route = ready.get("route")
            if (
                ready["type"] != "session.ready"
                or not isinstance(ready.get("request_id"), str)
                or not ready["request_id"]
                or not isinstance(route, dict)
                or route.get("provider") != provider
                or route.get("model") != model
            ):
                raise GatewayError("Relay speech admission failed", retryable=False)
            return RelaySpeechSession(
                websocket,
                config.kind,
                {
                    "session_id": self._platform_session_id,
                    "request_id": ready["request_id"],
                    "route": dict(route),
                },
                text_limit=text_limit,
            )

        try:
            return await asyncio.wait_for(establish(), self._setup_timeout)
        except BaseException as error:
            if websocket is not None:
                await websocket.close()
            if isinstance(error, aiohttp.WSServerHandshakeError):
                raise GatewayError(
                    "Relay speech handshake failed",
                    source="relay",
                    code=f"http_{error.status}",
                    status=error.status,
                    retryable=False,
                ) from None
            if isinstance(
                error, (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError)
            ):
                raise GatewayError(
                    "Relay speech connection failed", retryable=False
                ) from None
            raise


class RelaySpeechSession:
    """Native controls and events projected onto the shared speech interface."""

    def __init__(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        kind: str,
        metadata: dict[str, Any],
        *,
        text_limit: int | None = None,
    ) -> None:
        self._websocket, self._kind, self.metadata = websocket, kind, metadata
        self._lock = asyncio.Lock()
        self._finishing = self._closed = self._reading = self._terminal = False
        self._text_limit, self._text_characters = text_limit, 0

    async def _send(self, value: dict[str, Any] | bytes) -> None:
        async with self._lock:
            if self._closed or self._finishing:
                if isinstance(value, dict) and value["type"] == "session.close":
                    return
                raise GatewayError("Relay speech session is closed", retryable=False)
            if isinstance(value, bytes):
                await self._websocket.send_bytes(value)
            else:
                characters = (
                    len(value["text"]) if value["type"] == "input.append" else 0
                )
                if (
                    self._text_limit is not None
                    and self._text_characters + characters > self._text_limit
                ):
                    raise GatewayError(
                        "Relay speech input limit exceeded", retryable=False
                    )
                await self._websocket.send_json(value)
                self._text_characters += characters
                if value["type"] == "session.close":
                    self._finishing = True

    async def send_audio(self, data: bytes | bytearray | memoryview) -> None:
        if self._kind != "stt":
            raise ValueError("Audio input requires STT")
        await self._send(bytes(data))

    async def commit_audio(self) -> None:
        if self._kind != "stt":
            raise ValueError("Audio commit requires STT")
        await self._send({"type": "input.commit"})

    async def append_text(self, text: str) -> None:
        if self._kind != "tts" or not text:
            raise ValueError("Nonempty text input requires TTS")
        await self._send({"type": "input.append", "text": text})

    async def commit_text(self) -> None:
        if self._kind != "tts":
            raise ValueError("Text commit requires TTS")
        await self._send({"type": "input.commit"})

    async def cancel(self) -> None:
        if self._kind == "stt":
            # Native STT has no cancel control. Disconnect aborts that stream;
            # its durable receipt and settlement lifecycle remains on the relay.
            await self.aclose()
            return
        await self._send({"type": "input.cancel"})

    async def finish(self) -> None:
        await self._send({"type": "session.close"})

    async def aclose(self) -> None:
        self._closed = True
        await self._websocket.close()

    async def events(self) -> AsyncIterator[CanonicalEvent]:
        if self._reading:
            raise GatewayError(
                "Relay speech events already have a reader", retryable=False
            )
        self._reading = True
        try:
            async for message in self._websocket:
                if message.type is aiohttp.WSMsgType.BINARY:
                    if self._kind != "tts":
                        raise GatewayError(
                            "Relay speech protocol failed", retryable=False
                        )
                    yield CanonicalEvent(type="audio.frame", audio=bytes(message.data))
                    continue
                payload = _json(message)
                event = payload["type"]
                if event == "error":
                    self._terminal = True
                    raise _error(payload)
                if event == "session.closed":
                    if not isinstance(payload.get("usage"), dict):
                        raise GatewayError(
                            "Relay speech ended without final usage", retryable=False
                        )
                    self._terminal = True
                    yield CanonicalEvent(
                        type="usage.reported", data=payload.get("usage") or {}
                    )
                    yield CanonicalEvent(type="session.closed")
                    return
                if event not in _EVENTS[self._kind]:
                    raise GatewayError("Relay speech protocol failed", retryable=False)
                data = {key: value for key, value in payload.items() if key != "type"}
                if event == "usage.updated":
                    if not isinstance(payload.get("usage"), dict):
                        raise GatewayError(
                            "Relay speech protocol failed", retryable=False
                        )
                    data = payload["usage"]
                segments = data.get("segments")
                if (
                    event == "transcript.final"
                    and isinstance(segments, list)
                    and segments
                    and all(isinstance(segment, dict) for segment in segments)
                ):
                    data["audio_start_ms"] = segments[0].get("start_ms")
                    data["audio_end_ms"] = segments[-1].get("end_ms")
                yield CanonicalEvent(type=_EVENT_NAMES.get(event, event), data=data)
            if not self._terminal and not self._closed:
                raise GatewayError(
                    "Relay speech ended without final usage", retryable=False
                )
        finally:
            await self.aclose()
