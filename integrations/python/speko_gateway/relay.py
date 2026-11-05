"""Authenticated HTTPS client for the hosted Speko Router LLM surface.

Unlike the local Gateway socket, Router is a hosted Speko service
(`router.speko.dev`): requests authenticate with the Speko API key and the
conversation content travels to Router. The wire contract is the public
`relayapi` package in the Speko Gateway repository.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypedDict

import aiohttp

from ._user_agent import USER_AGENT
from .client import _env_secret
from .probe import ConversationProbe, active_probe
from .probe import report_leg as _report_probe_leg

_DEFAULT_RELAY_URL = "https://router.speko.dev"
_TERMINAL_EVENTS = {"response.completed", "error"}

_logger = logging.getLogger(__name__)

_HEADER_PREPARED_ADMISSION = "Speko-Prepared-Admission"
_HEADER_PREPARED_ADMISSION_OUTCOME = "Speko-Prepared-Admission-Outcome"

PREPARED_ADMISSION_USED = "used"
"""The `Speko-Prepared-Admission-Outcome` value of a request served on its ticket.

Any other value names why the Router admitted the request the ordinary way
instead: `invalid`, `expired`, `stale`, `route`, `budget` or `redialed`. The
request proceeds either way; a ticket never causes a refusal.
"""

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


class AdmissionsUnsupported(RelayError):
    """The Router has no `/v1/llm/admissions` route (an older deployment)."""


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

    async def prepare_admission(
        self, request: dict[str, Any]
    ) -> PreparedAdmission | None:
        """POST /v1/llm/admissions: admit the next request ahead of time.

        `request` is the body `stream_response` would send, built from the
        conversation so far; the Router reserves headroom for the turn still
        being spoken. Returns the ticket to present with
        `stream_response(..., prepared_admission=...)`, or None when the
        Router answers 204 (this region would hand the request to another
        one, where a ticket from here is useless). A refusal raises
        RelayError; a Router without the route (HTTP 404 or 405) raises
        AdmissionsUnsupported.

        The ticket is bound to this client's key and session ID. Keep at most
        one unused ticket per conversation and release one you will not use
        with `release_admission`: until then it holds its credit reservation
        and one organization concurrency slot.
        """

        body = dict(request)
        body["stream"] = True
        async with self._session.post(
            f"{self._base_url}/v1/llm/admissions",
            json=body,
            headers=self._headers(),
        ) as response:
            if response.status == 204:
                return None
            decoded = await _decode_json(response)
            if response.status in (404, 405):
                raise AdmissionsUnsupported(
                    f"Router does not serve prepared admissions (HTTP {response.status})"
                )
            if response.status != 201:
                raise _envelope_error(response.status, decoded)
        ticket = decoded.get("admission")
        expires_at = _parse_rfc3339(decoded.get("expires_at"))
        if not isinstance(ticket, str) or not ticket or expires_at is None:
            raise RelayError(
                "Router returned a malformed admission", retryable=False
            )
        return PreparedAdmission(
            ticket=ticket,
            expires_at=expires_at,
            _deadline=time.monotonic() + (expires_at - time.time()),
        )

    async def release_admission(self, admission: str) -> bool:
        """POST /v1/llm/admissions/release for a ticket that will not be used.

        Returns True when this call released it, False when it was already
        used, already released, or expired. A refusal raises RelayError.
        """

        async with self._session.post(
            f"{self._base_url}/v1/llm/admissions/release",
            json={"admission": admission},
            headers=self._headers(),
        ) as response:
            decoded = await _decode_json(response)
            if response.status != 200:
                raise _envelope_error(response.status, decoded)
        return decoded.get("released") is True

    def _headers(self) -> dict[str, str]:
        headers = {"Idempotency-Key": str(uuid.uuid4())}
        if self._platform_session_id:
            headers["Speko-Client-Session-ID"] = self._platform_session_id
        return headers

    async def stream_response(
        self,
        request: dict[str, Any],
        *,
        prepared_admission: str = "",
        on_prepared_outcome: Callable[[str], None] | None = None,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """POST /v1/llm/responses with stream=true, yielding (event, payload).

        `request` is sent as-is. An optional top-level `reasoning_effort`
        (see `ReasoningEffort`) controls reasoning; the framework LLM classes
        validate it before it gets here.

        Every stream carries exactly one terminal event: response.completed on
        success — yielded to the caller — or error, raised as RelayError. A
        socket that closes without either was truncated and raises too.

        `prepared_admission` presents a ticket from `prepare_admission`. When
        the Router answers, `on_prepared_outcome` receives its
        `Speko-Prepared-Admission-Outcome` (`PREPARED_ADMISSION_USED`, a
        reason the ticket was not used, or "" from a Router that sent none).
        It is not called when no response arrived.
        """

        body = dict(request)
        body["stream"] = True
        headers = {
            "Idempotency-Key": str(uuid.uuid4()),
            "Accept": "text/event-stream",
        }
        if self._platform_session_id:
            headers["Speko-Client-Session-ID"] = self._platform_session_id
        if prepared_admission:
            headers[_HEADER_PREPARED_ADMISSION] = prepared_admission
        async with self._session.post(
            f"{self._base_url}/v1/llm/responses", json=body, headers=headers
        ) as response:
            if prepared_admission and on_prepared_outcome is not None:
                on_prepared_outcome(
                    str(response.headers.get(_HEADER_PREPARED_ADMISSION_OUTCOME, ""))
                )
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


@dataclass(frozen=True)
class PreparedAdmission:
    """A prepared LLM admission ticket. The ticket is a secret: never log it."""

    ticket: str = field(repr=False)
    expires_at: float
    """Plan expiry, Unix seconds, as the Router reported it."""
    _deadline: float = field(repr=False, compare=False)

    def remaining(self) -> float:
        """Seconds of plan life left, measured on this host's monotonic clock."""

        return self._deadline - time.monotonic()


class AdmissionPreparer:
    """Hold at most one prepared LLM admission for one conversation.

    The framework LLM classes drive it: `prepare` when the caller starts
    speaking, `take` before the next LLM request, `settle` once the Router
    reports the ticket's outcome, `aclose` at the end. Nothing here raises into
    the agent: a failed prepare or release only costs the round trip it was
    meant to save.
    """

    def __init__(
        self,
        client: RelayLLMClient,
        *,
        reuse_min_seconds: float = 60.0,
        present_min_seconds: float = 10.0,
        wait_seconds: float = 1.0,
        release_timeout_seconds: float = 2.0,
    ) -> None:
        self._client = client
        self._reuse_min = reuse_min_seconds
        self._present_min = present_min_seconds
        self._wait = wait_seconds
        self._release_timeout = release_timeout_seconds
        self._ticket: PreparedAdmission | None = None
        self._inflight: asyncio.Task[None] | None = None
        self._background: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._unsupported = False

    def prepare(self, build_request: Callable[[], dict[str, Any] | None]) -> None:
        """Prepare an admission for the next request, unless one is live.

        Keeps a ticket with more than `reuse_min_seconds` of life, so a false
        start reuses it. Starts nothing while a prepare is in flight, nor ever
        again once the Router has answered 404 or 405 for the route. Otherwise
        releases the old ticket in the background and prepares a new one from
        `build_request()`, which returns the request body or None to skip.
        Call it on the event loop; it never blocks and never raises.
        """

        try:
            if self._closed or self._unsupported:
                return
            if self._inflight is not None and not self._inflight.done():
                return
            ticket = self._ticket
            if ticket is not None:
                if ticket.remaining() > self._reuse_min:
                    return
                self._ticket = None
                self._release_soon(ticket)
            request = build_request()
            if request is None or not request.get("input"):
                return
            self._inflight = asyncio.get_running_loop().create_task(
                self._prepare(request)
            )
        except Exception:
            _logger.debug("speko router admission prepare skipped", exc_info=True)

    async def _prepare(self, request: dict[str, Any]) -> None:
        try:
            admission = await self._client.prepare_admission(request)
        except AdmissionsUnsupported:
            # Until the Router serves the route, every prepare would be a
            # wasted call; stop for the rest of this conversation.
            self._unsupported = True
            _logger.debug(
                "speko router serves no prepared admissions; preparing turned off"
            )
            return
        except Exception as error:  # noqa: BLE001 - a prepare never fails the agent
            _logger.debug(
                "speko router admission prepare failed: %s", type(error).__name__
            )
            return
        if admission is None:
            _logger.debug("speko router admission prepare: no ticket (204)")
            return
        if self._closed:
            self._release_soon(admission)
            return
        self._ticket = admission

    async def take(self) -> PreparedAdmission | None:
        """Return the live ticket for the next request, and forget it.

        A prepare still in flight started earlier than this request's own
        admission would, so wait up to `wait_seconds` for it; past that the
        request goes the ordinary way and a late ticket waits for the next one.
        A ticket too close to expiry is released instead of presented.
        """

        inflight = self._inflight
        if inflight is not None and not inflight.done():
            # asyncio.wait neither raises the task's outcome nor cancels it.
            await asyncio.wait({inflight}, timeout=self._wait)
        ticket = self._ticket
        self._ticket = None
        if ticket is None:
            return None
        if ticket.remaining() < self._present_min:
            self._release_soon(ticket)
            return None
        return ticket

    def settle(self, admission: PreparedAdmission, outcome: str) -> None:
        """Release a presented ticket the Router did not use.

        `outcome` is the Router's outcome header, or "" when no response
        arrived. Anything but `used` is released in the background; a release
        of a ticket that was spent after all answers false.
        """

        if outcome != PREPARED_ADMISSION_USED:
            self._release_soon(admission)

    def release_unused(self) -> None:
        """Release the live ticket, if any, in the background."""

        ticket = self._ticket
        self._ticket = None
        if ticket is not None:
            self._release_soon(ticket)

    async def aclose(self) -> None:
        """Release the unused ticket and wait, bounded, for pending releases."""

        if self._closed:
            return
        self._closed = True
        inflight = self._inflight
        if inflight is not None and not inflight.done():
            await asyncio.wait({inflight}, timeout=self._wait)
            if not inflight.done():
                inflight.cancel()
        self.release_unused()
        pending = [task for task in self._background if not task.done()]
        if pending:
            await asyncio.wait(pending, timeout=self._release_timeout)

    def _release_soon(self, admission: PreparedAdmission) -> None:
        if admission.remaining() <= 0:
            return
        try:
            task = asyncio.get_running_loop().create_task(self._release(admission))
        except RuntimeError:
            return
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _release(self, admission: PreparedAdmission) -> None:
        try:
            released = await asyncio.wait_for(
                self._client.release_admission(admission.ticket),
                self._release_timeout,
            )
        except Exception as error:  # noqa: BLE001 - a release is best effort
            _logger.debug(
                "speko router admission release failed: %s", type(error).__name__
            )
            return
        _logger.debug("speko router admission released=%s", released)


_RFC3339 = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[Tt ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?([Zz]|[+-]\d{2}:\d{2})$"
)


def _parse_rfc3339(value: object) -> float | None:
    """Unix seconds of an RFC 3339 timestamp, or None if it is not one.

    Go writes up to nine fractional digits, which `datetime.fromisoformat`
    refuses before Python 3.11.
    """

    if not isinstance(value, str):
        return None
    match = _RFC3339.match(value.strip())
    if match is None:
        return None
    day, clock, fraction, offset = match.groups()
    fraction = (fraction or "")[:6].ljust(6, "0")
    offset = "+00:00" if offset in ("Z", "z") else offset
    try:
        return datetime.fromisoformat(f"{day}T{clock}.{fraction}{offset}").timestamp()
    except ValueError:
        return None


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
        if event and data_lines:
            payload = json.loads("\n".join(data_lines))
            if isinstance(payload, dict):
                yield event, payload
    except ValueError as err:
        raise RelayError("relay stream sent a malformed event", retryable=True) from err


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
