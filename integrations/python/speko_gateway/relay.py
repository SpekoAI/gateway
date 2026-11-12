"""Authenticated HTTPS client for the hosted Speko Router LLM surface.

Unlike the local Gateway socket, Router is a hosted Speko service
(`router.speko.dev`): requests authenticate with the Speko API key and the
conversation content travels to Router. The wire contract is the public
`relayapi` package in the Speko Gateway repository.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from typing import Any, Literal, TypedDict

import aiohttp

from ._user_agent import USER_AGENT
from .client import _env_secret
from .probe import ConversationProbe, active_probe
from .probe import report_leg as _report_probe_leg

_DEFAULT_RELAY_URL = "https://router.speko.dev"
_TERMINAL_EVENTS = {"response.completed", "error"}

ReasoningEffort = Literal["none", "minimal", "low", "medium", "high", "xhigh"]
"""The Router's provider-neutral reasoning control, sent as `reasoning_effort`.

`"none"` turns reasoning off. `"minimal"` asks for the least reasoning the
routed model allows; the Router maps it onto each provider's own scale, so on
some models it means no reasoning and on others the lowest level. `"low"`
through `"xhigh"` ask for progressively more. Leaving it unset sends no field
and keeps the provider's default.

`"none"` is accepted on models that cannot reason, where it changes nothing
(older Routers refuse it there). Four models always reason and refuse `"none"`,
with `"minimal"` as their floor: Inception mercury-voice, gpt-oss-120b on
Cerebras and Together, and Together zai-org/GLM-5.3-Flash. Every other value,
`"minimal"` included, needs a model that can reason.

While reasoning is enabled (any value other than `"none"`), the Router refuses
`temperature` and `top_p` in the same request.
"""

REASONING_EFFORTS: tuple[ReasoningEffort, ...] = (
    "none",
    "minimal",
    "low",
    "medium",
    "high",
    "xhigh",
)


def validate_reasoning_effort(value: object) -> ReasoningEffort | None:
    """Return `value` if it is None or a Router reasoning level, else raise.

    Raises ValueError naming the accepted levels, so a typo fails where the
    integration is configured rather than as a Router 400 on the first reply.
    """

    if value is None:
        return None
    if isinstance(value, str) and value in REASONING_EFFORTS:
        return value  # type: ignore[return-value]
    raise ValueError(
        f"reasoning_effort must be None or one of {', '.join(REASONING_EFFORTS)};"
        f" got {value!r}"
    )


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


JSONValue = str | int | float | bool | None | list["JSONValue"] | dict[str, "JSONValue"]


class _RequiredEvaluationQuestion(TypedDict):
    type: Literal["noul", "choice", "score"]
    instructions: JSONValue


class EvaluationQuestion(_RequiredEvaluationQuestion, total=False):
    criteria: JSONValue


class EvaluationAnswer(TypedDict, total=False):
    type: Literal["noul", "choice", "score"]
    noul: float
    choice: str
    score: float
    probabilities: dict[str, float]
    confidence: float
    legend: dict[str, JSONValue]


class EvaluationUsage(TypedDict, total=False):
    input_tokens: int
    output_tokens: int


class EvaluationResponse(TypedDict):
    model: str
    answers: dict[str, EvaluationAnswer]
    usage: EvaluationUsage


class RelayEvaluationClient:
    """Pooled async client for the Router's non-streaming evaluation API."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _DEFAULT_RELAY_URL,
        session_id: str = "",
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self._base_url = base_url.rstrip("/")
        self._platform_session_id = session_id
        self._session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": USER_AGENT,
            },
            raise_for_status=False,
        )

    @classmethod
    def from_env(cls, *, session_id: str = "") -> RelayEvaluationClient:
        api_key = _env_secret("SPEKO_API_KEY")
        if not api_key:
            raise ValueError("SPEKO_API_KEY is required for Router evaluations")
        base_url = (
            os.environ.get("SPEKO_ROUTER_URL", "").strip()
            or os.environ.get("SPEKO_RELAY_URL", "").strip()
            or _DEFAULT_RELAY_URL
        )
        return cls(api_key=api_key, base_url=base_url, session_id=session_id)

    async def aclose(self) -> None:
        await self._session.close()

    async def evaluate(
        self,
        *,
        state: JSONValue,
        questions: dict[str, EvaluationQuestion],
        idempotency_key: str,
        provider: str = "typesafe",
        model: str = "jev-1.13.0",
    ) -> EvaluationResponse:
        """Evaluate one batch; cancelling this coroutine cancels the HTTP call."""

        if not idempotency_key:
            raise ValueError("idempotency_key is required")
        headers = {"Idempotency-Key": idempotency_key}
        if self._platform_session_id:
            headers["Speko-Client-Session-ID"] = self._platform_session_id
        body = {
            "routing": {
                "mode": "explicit",
                "provider": provider,
                "model": model,
            },
            "state": state,
            "questions": questions,
        }
        probe = active_probe()
        probe_turn_id = probe.current_turn_id if probe is not None else None
        async with self._session.post(
            f"{self._base_url}/v1/evaluations", json=body, headers=headers
        ) as response:
            decoded = await _decode_json(response)
            if response.status != 200:
                raise _envelope_error(response.status, decoded)
            _report_evaluation_leg(response, probe, probe_turn_id)
            if not isinstance(decoded.get("answers"), dict):
                raise RelayError(
                    "Router returned a malformed evaluation response",
                    retryable=True,
                )
            return decoded  # type: ignore[return-value]


class RelayLLMClient:
    """Own one authenticated HTTPS transport to the hosted Speko Router."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _DEFAULT_RELAY_URL,
        session_id: str = "",
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self._base_url = base_url.rstrip("/")
        self._session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": USER_AGENT,
            },
            raise_for_status=False,
        )
        self._platform_session_id = session_id

    @classmethod
    def from_env(cls, *, session_id: str = "") -> RelayLLMClient:
        """Create a client from SPEKO_API_KEY and optional SPEKO_ROUTER_URL.

        SPEKO_RELAY_URL remains a compatibility fallback for existing
        deployments.
        """

        api_key = _env_secret("SPEKO_API_KEY")
        if not api_key:
            raise ValueError("SPEKO_API_KEY is required for Router LLM requests")
        base_url = (
            os.environ.get("SPEKO_ROUTER_URL", "").strip()
            or os.environ.get("SPEKO_RELAY_URL", "").strip()
            or _DEFAULT_RELAY_URL
        )
        return cls(api_key=api_key, base_url=base_url, session_id=session_id)

    async def aclose(self) -> None:
        await self._session.close()

    def _registered_request(
        self,
        *,
        rendering: str,
        request_body: str,
        work_nonce: str,
        input_blinder: str,
        idempotency_key: str,
        stream: bool,
    ) -> tuple[bytes, dict[str, str]]:
        """Transport only: the cell and native router verify execution authority."""
        if not isinstance(self._platform_session_id, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", self._platform_session_id
        ):
            raise ValueError("a valid session_id is required for registered model input")
        if not _registered_idempotency_key(idempotency_key):
            raise ValueError("a bounded explicit idempotency_key is required")
        try:
            if (
                not isinstance(rendering, str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", rendering)
                or not isinstance(work_nonce, str)
                or not re.fullmatch(r"[0-9a-f]{64}", work_nonce)
                or not isinstance(input_blinder, str)
                or not re.fullmatch(r"[0-9a-f]{64}", input_blinder)
                or not isinstance(request_body, str)
                or not 0 < len(request_body.encode("utf-8")) <= 262144
            ):
                raise ValueError()
            # Inspect mode without rewriting a byte of the registered request.
            # Reject ambiguous JSON rather than accepting a last-property wins
            # interpretation of the stream setting.
            value = json.loads(
                request_body,
                object_pairs_hook=_registered_object,
                parse_constant=_invalid_registered_constant,
            )
            if not isinstance(value, dict) or value.get("stream") is not stream:
                raise ValueError()
        except (ValueError, TypeError, RecursionError):
            raise ValueError("registered model input is invalid") from None
        envelope = {
            "speko_work_nonce": work_nonce,
            "speko_input_blinder": input_blinder,
            "speko_model_rendering": rendering,
            "speko_model_request": request_body,
        }
        return json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        ), {
            "Idempotency-Key": idempotency_key,
            "Speko-Client-Session-ID": self._platform_session_id,
            "Content-Type": "application/json",
            "Accept": "text/event-stream" if stream else "application/json",
        }

    async def complete_registered_response(
        self,
        *,
        rendering: str,
        request_body: str,
        work_nonce: str,
        input_blinder: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Send one cell-rendered stream:false request under this work's bearer.

        The original request string, nonce, private blinder and retry key must
        come from the caller's registered work. No retry or fallback is made.
        A response is not a cell result commit, settlement or no-I/O proof.
        """
        body, headers = self._registered_request(
            rendering=rendering,
            request_body=request_body,
            work_nonce=work_nonce,
            input_blinder=input_blinder,
            idempotency_key=idempotency_key,
            stream=False,
        )
        async with self._session.post(
            f"{self._base_url}/v1/llm/responses",
            data=body,
            headers=headers,
            allow_redirects=False,
        ) as response:
            decoded = await _decode_json(response)
            if response.status != 200:
                raise _envelope_error(response.status, decoded)
            if "error" in decoded:
                raise _event_error(decoded)
            if not decoded:
                raise RelayError("Router returned a malformed response", retryable=True)
            _report_llm_leg(response)
            return decoded

    async def stream_registered_response(
        self,
        *,
        rendering: str,
        request_body: str,
        work_nonce: str,
        input_blinder: str,
        idempotency_key: str,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Stream one exact registered stream:true request, without a new key.

        Never reconstruct the provider body from parsed JSON, append fields to
        the protected envelope, follow redirects or retry an uncertain send.
        """
        body, headers = self._registered_request(
            rendering=rendering,
            request_body=request_body,
            work_nonce=work_nonce,
            input_blinder=input_blinder,
            idempotency_key=idempotency_key,
            stream=True,
        )
        async with self._session.post(
            f"{self._base_url}/v1/llm/responses",
            data=body,
            headers=headers,
            allow_redirects=False,
        ) as response:
            if response.status != 200:
                raise _envelope_error(response.status, await _decode_json(response))
            _report_llm_leg(response)
            async for event, payload in _sse_events(response):
                if event == "error":
                    raise _event_error(payload)
                yield event, payload
                if event == "response.completed":
                    return
            raise RelayError(
                "relay stream ended without a terminal event", retryable=True
            )

    async def stream_response(
        self, request: dict[str, Any]
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """POST /v1/llm/responses with stream=true, yielding (event, payload).

        `request` is sent as-is. An optional top-level `reasoning_effort`
        (see `ReasoningEffort`) controls reasoning; the framework LLM classes
        validate it before it gets here.

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
        if self._platform_session_id:
            headers["Speko-Client-Session-ID"] = self._platform_session_id
        async with self._session.post(
            f"{self._base_url}/v1/llm/responses", json=body, headers=headers
        ) as response:
            if response.status != 200:
                raise _envelope_error(response.status, await _decode_json(response))
            _report_llm_leg(response)
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


def _report_llm_leg(response: aiohttp.ClientResponse) -> None:
    """Bind the relay request ID to the active conversation probe, if any.

    The `Speko-Request-ID` header is the relay's identity namespace for this
    LLM leg — the only identifier the profiler can use to join relay-side
    records to a caller turn. Strictly a no-op when no probe is active.
    """

    request_id = str(response.headers.get("Speko-Request-ID", ""))
    if request_id:
        _report_probe_leg("llm", request_id=request_id)


def _report_evaluation_leg(
    response: aiohttp.ClientResponse,
    probe: ConversationProbe | None,
    turn_id: str | None,
) -> None:
    request_id = str(response.headers.get("Speko-Request-ID", ""))
    if request_id and probe is not None and turn_id:
        probe.report_leg(
            "evaluation", request_id=request_id, expected_turn_id=turn_id
        )


async def _sse_events(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    # JSONDecodeError, UnicodeDecodeError, and aiohttp's line-limit overrun
    # are all ValueError: a stream that cannot be parsed is normalized to a
    # retryable RelayError so it stays inside the adapter's connection-error
    # classification instead of escaping as a raw decoding exception.
    event = ""
    data_lines: list[str] = []
    try:
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
        if event or data_lines:
            # EOF does not dispatch an SSE event. Even valid terminal JSON is
            # incomplete until the frame's empty-line delimiter was received.
            raise RelayError("relay stream ended with an unterminated event", retryable=True)
    except ValueError as err:
        raise RelayError("relay stream sent a malformed event", retryable=True) from err


def _registered_idempotency_key(value: str) -> bool:
    # Relay keys are opaque and bounded to 256 UTF-8 bytes. Refuse HTTP field
    # controls or surrounding OWS, which a receiver would trim and thus change
    # the original retry identity. Interior spaces/tabs and Unicode stay exact.
    if not isinstance(value, str) or not value.strip() or value.strip(" \t") != value:
        return False
    try:
        return len(value.encode("utf-8")) <= 256 and all(
            (ord(char) >= 32 and ord(char) != 127) or char == "\t" for char in value
        )
    except UnicodeError:
        return False


def _registered_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


def _invalid_registered_constant(_value: str) -> Any:
    raise ValueError()


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
            f"Router rejected request (HTTP {status})",
            retryable=status >= 500,
        )
    return RelayError(
        f"Router rejected request ({error.get('code', '')}, HTTP {status})",
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
