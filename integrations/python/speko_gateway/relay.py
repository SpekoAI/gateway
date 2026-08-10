"""Authenticated HTTPS client for the hosted Speko relay LLM surface.

Unlike the local Gateway socket, the relay is a hosted Speko service
(`relay.speko.dev`): requests authenticate with the Speko API key and the
conversation content travels to the relay. The wire contract is the public
`relayapi` package in the Speko Gateway repository.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from .client import _env_secret

_DEFAULT_RELAY_URL = "https://relay.speko.dev"
_TERMINAL_EVENTS = {"response.completed", "error"}


class RelayError(RuntimeError):
    """A relay HTTP or stream failure carrying the normalized error code."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "",
        retryable: bool = False,
        request_id: str = "",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.request_id = request_id


class RelayLLMClient:
    """Own one authenticated HTTPS transport to the hosted Speko relay."""

    def __init__(self, *, api_key: str, base_url: str = _DEFAULT_RELAY_URL) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self._base_url = base_url.rstrip("/")
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {api_key}"},
            raise_for_status=False,
        )

    @classmethod
    def from_env(cls) -> RelayLLMClient:
        """Create a client from SPEKO_API_KEY and optional SPEKO_RELAY_URL."""

        api_key = _env_secret("SPEKO_API_KEY")
        if not api_key:
            raise ValueError("SPEKO_API_KEY is required for relay LLM requests")
        base_url = os.environ.get("SPEKO_RELAY_URL", "").strip() or _DEFAULT_RELAY_URL
        return cls(api_key=api_key, base_url=base_url)

    async def aclose(self) -> None:
        await self._session.close()

    async def stream_response(
        self, request: dict[str, Any]
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """POST /v1/llm/responses with stream=true, yielding (event, payload).

        Every stream carries exactly one terminal event: response.completed on
        success — yielded to the caller — or error, raised as RelayError. A
        socket that closes without either was truncated and raises too.
        """

        body = dict(request)
        body["stream"] = True
        headers = {
            "Idempotency-Key": str(uuid.uuid4()),
            "Accept": "text/event-stream",
        }
        async with self._session.post(
            f"{self._base_url}/v1/llm/responses", json=body, headers=headers
        ) as response:
            if response.status != 200:
                raise _envelope_error(response.status, await _decode_json(response))
            terminal = False
            async for event, payload in _sse_events(response):
                if event == "error":
                    raise _event_error(payload)
                yield event, payload
                if event in _TERMINAL_EVENTS:
                    terminal = True
            if not terminal:
                raise RelayError(
                    "relay stream ended without a terminal event",
                    retryable=True,
                )


async def _sse_events(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    event = ""
    data_lines: list[str] = []
    async for raw_line in response.content:
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if line == "":
            if event and data_lines:
                payload = json.loads("\n".join(data_lines))
                if isinstance(payload, dict):
                    yield event, payload
            event = ""
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "event":
            event = value
        elif field == "data":
            data_lines.append(value)
    if event and data_lines:
        payload = json.loads("\n".join(data_lines))
        if isinstance(payload, dict):
            yield event, payload


async def _decode_json(response: aiohttp.ClientResponse) -> dict[str, Any]:
    try:
        value = await response.json(content_type=None)
    except (aiohttp.ContentTypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _envelope_error(status: int, body: dict[str, Any]) -> RelayError:
    error = body.get("error")
    if not isinstance(error, dict):
        return RelayError(
            f"relay rejected request (HTTP {status})",
            retryable=status >= 500,
        )
    return RelayError(
        f"relay rejected request ({error.get('code', '')}, HTTP {status})",
        code=str(error.get("code", "")),
        retryable=bool(error.get("retryable", status >= 500)),
        request_id=str(error.get("request_id", "")),
    )


def _event_error(payload: dict[str, Any]) -> RelayError:
    error = payload.get("error")
    if not isinstance(error, dict):
        return RelayError("relay stream failed", retryable=True)
    return RelayError(
        f"relay stream failed ({error.get('code', '')})",
        code=str(error.get("code", "")),
        retryable=bool(error.get("retryable", False)),
        request_id=str(error.get("request_id", "")),
    )
