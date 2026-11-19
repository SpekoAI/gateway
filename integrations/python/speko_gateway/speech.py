"""One-shot Router TTS audio transport; no retries or delivery assertions."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import AsyncIterator, Awaitable
from typing import Any, TypeVar
from urllib.parse import urlsplit

import aiohttp

from ._user_agent import USER_AGENT
from .probe import active_probe
from .relay import RelayError, _registered_idempotency_key

_MAX_REQUEST_BYTES = 1024 * 1024
_MAX_AUDIO_BYTES = 64 * 1024 * 1024
_T = TypeVar("_T")


def _header(value: object, *, optional: bool = False) -> bool:
    return isinstance(value, str) and (
        optional
        and value == ""
        or 0 < len(value) <= 200
        and all(32 < ord(char) < 127 for char in value)
    )


def _request(
    request: dict[str, Any],
    idempotency_key: str,
    work_nonce: str | None,
    input_blinder: str | None,
) -> bytes:
    body = None
    try:
        if (
            type(request) is not dict
            or not _registered_idempotency_key(idempotency_key)
            or not all(
                type(key) is str and not key.startswith("speko_") for key in request
            )
            or not isinstance(request.get("input"), str)
            or not request["input"]
        ):
            raise ValueError()
        frozen = dict(request)
        if work_nonce is not None or input_blinder is not None:
            if not all(
                isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value)
                for value in (work_nonce, input_blinder)
            ):
                raise ValueError()
            frozen.update(
                speko_work_nonce=work_nonce, speko_input_blinder=input_blinder
            )
        candidate = json.dumps(
            frozen, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        if len(candidate) <= _MAX_REQUEST_BYTES:
            body = candidate
    except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
        pass
    if body is None:
        raise ValueError("invalid Router speech request")
    return body


class RelayTTSClient:
    """Own an authenticated Router HTTP transport and stream original audio bytes.

    Each synthesize operation makes one POST using an explicit stable retry key.
    Optional work nonce/blinder values are sent together, unchanged. They come
    from the caller's authority; the SDK neither creates nor interprets them.
    Errors, EOF and audio bytes establish no no-I/O, settlement or heard proof.
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://router.speko.dev",
        session_id: str = "",
        io_timeout: float = 30.0,
        max_audio_bytes: int = _MAX_AUDIO_BYTES,
    ) -> None:
        valid = False
        try:
            url = urlsplit(base_url)
            valid = (
                isinstance(api_key, str)
                and bool(api_key)
                and "\r" not in api_key
                and "\n" not in api_key
                and url.scheme in ("http", "https")
                and bool(url.hostname)
                and not url.username
                and not url.password
                and not url.query
                and not url.fragment
                and _header(session_id, optional=True)
                and type(io_timeout) in (int, float)
                and math.isfinite(io_timeout)
                and io_timeout > 0
                and type(max_audio_bytes) is int
                and 0 < max_audio_bytes <= _MAX_AUDIO_BYTES
            )
            _ = url.port
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise ValueError("invalid Router speech configuration")
        self._base_url = base_url.rstrip("/")
        self._session_id = session_id
        self._io_timeout, self._max_audio_bytes = io_timeout, max_audio_bytes
        self._closed = False
        self._responses: set[aiohttp.ClientResponse] = set()
        self._session = aiohttp.ClientSession(
            headers={"Authorization": "Bearer " + api_key, "User-Agent": USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=None),
            auto_decompress=False,
            cookie_jar=aiohttp.DummyCookieJar(),
        )

    async def aclose(self) -> None:
        self._closed = True
        for response in tuple(self._responses):
            response.close()
        self._session.headers.pop("Authorization", None)
        await self._session.close()

    async def synthesize(
        self,
        request: dict[str, Any],
        *,
        idempotency_key: str,
        work_nonce: str | None = None,
        input_blinder: str | None = None,
    ) -> AsyncIterator[bytes]:
        """Freeze before I/O; bound network waits without a timer across yields.

        io_timeout is the cumulative time spent awaiting HTTP headers/audio,
        excluding time in consumer code. max_audio_bytes bounds delivered bytes.
        Neither interruption nor a failed response permits an automatic retry.
        """
        body = _request(request, idempotency_key, work_nonce, input_blinder)
        probe = active_probe()
        turn_id = probe.current_turn_id if probe is not None else None
        headers = {
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "Idempotency-Key": idempotency_key,
        }
        if self._session_id:
            headers["Speko-Client-Session-ID"] = self._session_id
        remaining = self._io_timeout
        loop = asyncio.get_running_loop()

        async def network(call: Awaitable[_T]) -> _T:
            nonlocal remaining
            started = loop.time()
            try:
                return await asyncio.wait_for(call, timeout=max(0, remaining))
            finally:
                remaining -= loop.time() - started

        response = None
        failure = None
        try:
            if self._closed:
                raise RelayError("Router speech client is closed", code="client_closed")
            response = await network(
                self._session.post(
                    self._base_url + "/v1/tts/speech",
                    data=body,
                    headers=headers,
                    allow_redirects=False,
                )
            )
            self._responses.add(response)
            if self._closed:
                raise RelayError("Router speech client is closed", code="client_closed")
            if response.status != 200:
                code = (
                    "authentication_failed"
                    if response.status == 401
                    else "permission_denied"
                    if response.status == 403
                    else "request_rejected"
                )
                raise RelayError(
                    f"Router speech rejected (HTTP {response.status})",
                    code=code,
                    retryable=response.status >= 500 or response.status in (408, 429),
                )
            types = response.headers.getall("Content-Type", [])
            encodings = response.headers.getall("Content-Encoding", [])
            media = types[0].split(";", 1)[0].strip().lower() if len(types) == 1 else ""
            if (
                not (
                    media == "application/octet-stream"
                    or re.fullmatch(r"audio/[a-z0-9!#$&^_.+-]+", media)
                )
                or len(encodings) > 1
                or encodings
                and encodings[0].strip().lower() != "identity"
                or response.content_length is not None
                and response.content_length > self._max_audio_bytes
            ):
                raise RelayError(
                    "Router returned invalid speech audio", code="invalid_response"
                )
            request_ids = response.headers.getall("Speko-Request-ID", [])
            request_id = request_ids[0] if len(request_ids) == 1 else ""
            if (
                re.fullmatch(r"[A-Za-z0-9._:-]{1,256}", request_id)
                and probe is not None
                and turn_id
            ):
                probe.report_leg("tts", request_id=request_id, expected_turn_id=turn_id)
            total = 0
            while True:
                if self._closed:
                    raise RelayError(
                        "Router speech client is closed", code="client_closed"
                    )
                chunk = await network(response.content.read(65536))
                if self._closed:
                    raise RelayError(
                        "Router speech client is closed", code="client_closed"
                    )
                if not chunk:
                    if not total:
                        raise RelayError(
                            "Router returned no speech audio", code="invalid_response"
                        )
                    return
                total += len(chunk)
                if total > self._max_audio_bytes:
                    raise RelayError(
                        "Router speech audio exceeded its bound",
                        code="response_too_large",
                    )
                yield chunk
        except asyncio.TimeoutError:
            failure = RelayError(
                "Router speech I/O timed out", code="timeout", retryable=True
            )
        except (aiohttp.ClientError, OSError):
            failure = RelayError(
                "Router speech transport failed",
                code="transport_failed",
                retryable=True,
            )
        finally:
            if response is not None:
                self._responses.discard(response)
                response.close()
        if failure is not None:
            raise failure
