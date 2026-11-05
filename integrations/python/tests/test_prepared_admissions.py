from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import web
from livekit.agents import APIConnectionError
from livekit.agents import llm as agents_llm

from speko_gateway.livekit import LLM
from speko_gateway.relay import RelayLLMClient, _parse_rfc3339

SESSION_ID = "conversation-7"


def _go_time(epoch: float) -> str:
    # Go's RFC3339Nano: nine fractional digits and a Z.
    stamp = datetime.fromtimestamp(epoch, timezone.utc)
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.%f") + "123Z"


class FakeRouter:
    def __init__(self) -> None:
        self.admissions: list[tuple[dict[str, str], dict[str, Any]]] = []
        self.releases: list[tuple[dict[str, str], dict[str, Any]]] = []
        self.responses: list[tuple[dict[str, str], dict[str, Any]]] = []
        self.admission_delay = 0.0
        self.admission_reply = "ticket"
        self.expires_in = 300.0
        self.outcome: str | None = "used"
        self.response_status = 200
        self.issued = 0

    async def admit(self, request: web.Request) -> web.StreamResponse:
        self.admissions.append((dict(request.headers), await request.json()))
        await asyncio.sleep(self.admission_delay)
        if self.admission_reply == "none":
            return web.Response(status=204)
        if self.admission_reply == "refused":
            return web.json_response(
                {"error": {"code": "insufficient_credit", "retryable": False}},
                status=402,
            )
        if self.admission_reply in ("404", "405"):
            return web.Response(status=int(self.admission_reply))
        if self.admission_reply == "malformed":
            return web.json_response({"object": "llm.admission"}, status=201)
        self.issued += 1
        return web.json_response(
            {
                "object": "llm.admission",
                "admission": f"spa1.ticket-{self.issued}",
                "expires_at": _go_time(time.time() + self.expires_in),
            },
            status=201,
        )

    async def release(self, request: web.Request) -> web.Response:
        self.releases.append((dict(request.headers), await request.json()))
        return web.json_response({"object": "llm.admission.release", "released": True})

    async def respond(self, request: web.Request) -> web.StreamResponse:
        self.responses.append((dict(request.headers), await request.json()))
        headers = {"Content-Type": "text/event-stream", "Speko-Request-ID": "req-1"}
        if "Speko-Prepared-Admission" in request.headers and self.outcome is not None:
            headers["Speko-Prepared-Admission-Outcome"] = self.outcome
        if self.response_status != 200:
            return web.json_response(
                {"error": {"code": "insufficient_credit", "retryable": False}},
                status=self.response_status,
                headers={k: v for k, v in headers.items() if k != "Content-Type"},
            )
        response = web.StreamResponse(headers=headers)
        await response.prepare(request)
        for event, payload in (
            ("response.created", {"response_id": "resp-1"}),
            ("response.text.delta", {"delta": "Hi"}),
            ("response.completed", {"usage": {"input_tokens": 3, "output_tokens": 1}}),
        ):
            await response.write(
                f"event: {event}\ndata: {json.dumps(payload)}\n\n".encode()
            )
        await response.write_eof()
        return response


@asynccontextmanager
async def router_client(router: FakeRouter, *, session_id: str = SESSION_ID):
    app = web.Application()
    app.router.add_post("/v1/llm/admissions", router.admit)
    app.router.add_post("/v1/llm/admissions/release", router.release)
    app.router.add_post("/v1/llm/responses", router.respond)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    client = RelayLLMClient(
        api_key="router-fixture-only",
        base_url=f"http://127.0.0.1:{port}",
        session_id=session_id,
    )
    try:
        yield client
    finally:
        await client.aclose()
        await runner.cleanup()


class FakeSession:
    def __init__(self, chat_ctx: agents_llm.ChatContext) -> None:
        self.handlers: dict[str, list[Callable[[Any], None]]] = {}
        self.current_agent = SimpleNamespace(chat_ctx=chat_ctx, tools=[])

    def on(self, event: str, callback: Callable[[Any], None]) -> None:
        self.handlers.setdefault(event, []).append(callback)

    def off(self, event: str, callback: Callable[[Any], None]) -> None:
        if callback in self.handlers.get(event, []):
            self.handlers[event].remove(callback)

    def emit(self, event: str, payload: Any) -> None:
        for callback in list(self.handlers.get(event, [])):
            callback(payload)

    def user_starts_speaking(self) -> None:
        self.emit(
            "user_state_changed",
            SimpleNamespace(old_state="listening", new_state="speaking"),
        )


def _history() -> agents_llm.ChatContext:
    ctx = agents_llm.ChatContext.empty()
    ctx.add_message(role="system", content="Be brief.")
    ctx.add_message(role="assistant", content="Hello, how can I help?")
    return ctx


def _turn() -> agents_llm.ChatContext:
    ctx = _history()
    ctx.add_message(role="user", content="What time is it?")
    return ctx


async def until(condition: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


async def reply(plugin: LLM, ctx: agents_llm.ChatContext | None = None) -> Any:
    stream = plugin.chat(chat_ctx=ctx or _turn())
    text = "".join(
        [c.delta.content async for c in stream if c.delta and c.delta.content]
    )
    assert text == "Hi"
    return stream


async def test_prepared_ticket_is_presented_once_and_used() -> None:
    router = FakeRouter()
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        session = FakeSession(_history())
        plugin.attach(session)

        session.user_starts_speaking()
        await until(lambda: router.issued == 1)
        stream = await reply(plugin)

        prepare_headers, prepared = router.admissions[0]
        assert prepared == {**plugin._request_body(_history(), []), "stream": True}
        assert prepare_headers["Speko-Client-Session-ID"] == SESSION_ID
        assert prepare_headers["Authorization"] == "Bearer router-fixture-only"
        headers, _ = router.responses[0]
        assert headers["Speko-Prepared-Admission"] == "spa1.ticket-1"
        assert headers["Speko-Client-Session-ID"] == SESSION_ID
        assert headers["Idempotency-Key"] != prepare_headers["Idempotency-Key"]
        assert stream.prepared_admission_outcome == "used"
        assert plugin.prepared_admission_outcome == "used"

        # A tool-call follow-up has no ticket left and goes the ordinary way.
        follow_up = await reply(plugin)
        assert "Speko-Prepared-Admission" not in router.responses[1][0]
        assert follow_up.prepared_admission_outcome is None
        assert plugin.prepared_admission_outcome is None

        await plugin.aclose()
        assert router.releases == []


@pytest.mark.parametrize("outcome", ["budget", "redialed", "invalid"])
async def test_ticket_the_router_did_not_use_is_released(outcome: str) -> None:
    router = FakeRouter()
    router.outcome = outcome
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())
        await until(lambda: router.issued == 1)

        stream = await reply(plugin)

        assert stream.prepared_admission_outcome == outcome
        await until(lambda: len(router.releases) == 1)
        headers, body = router.releases[0]
        assert body == {"admission": "spa1.ticket-1"}
        assert headers["Speko-Client-Session-ID"] == SESSION_ID
        await plugin.aclose()


async def test_missing_outcome_header_releases_the_ticket() -> None:
    # A request handed off to another region does not carry the ticket on, and
    # its response has no outcome header at all.
    router = FakeRouter()
    router.outcome = None
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())
        await until(lambda: router.issued == 1)

        stream = await reply(plugin)

        assert router.responses[0][0]["Speko-Prepared-Admission"] == "spa1.ticket-1"
        assert stream.prepared_admission_outcome == ""
        assert plugin.prepared_admission_outcome == ""
        await until(lambda: len(router.releases) == 1)
        assert router.releases[0][1] == {"admission": "spa1.ticket-1"}
        await plugin.aclose()


@pytest.mark.parametrize("outcome", ["budget", None])
async def test_refused_request_reports_its_outcome_and_releases(
    outcome: str | None,
) -> None:
    router = FakeRouter()
    router.outcome = outcome
    router.response_status = 402
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())
        await until(lambda: router.issued == 1)

        stream = plugin.chat(chat_ctx=_turn())
        with pytest.raises(APIConnectionError):
            async for _ in stream:
                pass

        assert stream.prepared_admission_outcome == (outcome or "")
        await until(lambda: len(router.releases) == 1)
        await plugin.aclose()


async def test_request_waits_for_a_prepare_in_flight() -> None:
    router = FakeRouter()
    router.admission_delay = 0.3
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())

        await reply(plugin)

        assert router.responses[0][0]["Speko-Prepared-Admission"] == "spa1.ticket-1"
        assert plugin.prepared_admission_outcome == "used"
        await plugin.aclose()


async def test_request_does_not_wait_past_the_bound_for_a_slow_prepare() -> None:
    router = FakeRouter()
    router.admission_delay = 1.4
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())

        started = time.monotonic()
        await reply(plugin)

        assert time.monotonic() - started < 1.3
        assert "Speko-Prepared-Admission" not in router.responses[0][0]
        assert plugin.prepared_admission_outcome is None
        # The late ticket is kept for the next request, and released at close.
        await until(lambda: router.issued == 1)
        await asyncio.sleep(0.05)
        await plugin.aclose()
        assert [body for _, body in router.releases] == [
            {"admission": "spa1.ticket-1"}
        ]


async def test_close_releases_an_unused_ticket() -> None:
    router = FakeRouter()
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())
        await until(lambda: router.issued == 1)
        await asyncio.sleep(0.05)

        await plugin.aclose()

        assert [body for _, body in router.releases] == [
            {"admission": "spa1.ticket-1"}
        ]


async def test_session_close_releases_an_unused_ticket() -> None:
    router = FakeRouter()
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        session = FakeSession(_history())
        plugin.attach(session)
        session.user_starts_speaking()
        await until(lambda: router.issued == 1)
        await asyncio.sleep(0.05)

        session.emit("close", SimpleNamespace(reason="participant_disconnected"))

        await until(lambda: len(router.releases) == 1)
        assert session.handlers == {"user_state_changed": [], "close": []}
        await plugin.aclose()


async def test_live_ticket_is_reused_and_prepares_never_stack() -> None:
    router = FakeRouter()
    router.admission_delay = 0.2
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        session = FakeSession(_history())
        plugin.attach(session)

        session.user_starts_speaking()
        session.user_starts_speaking()
        await until(lambda: router.issued == 1)
        await asyncio.sleep(0.05)
        session.user_starts_speaking()  # a false start keeps the live ticket
        await asyncio.sleep(0.05)

        assert len(router.admissions) == 1
        assert router.releases == []
        await plugin.aclose()


async def test_ticket_near_expiry_is_replaced_on_the_next_speech_start() -> None:
    router = FakeRouter()
    router.expires_in = 30.0
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        session = FakeSession(_history())
        plugin.attach(session)

        session.user_starts_speaking()
        await until(lambda: router.issued == 1)
        await asyncio.sleep(0.05)
        session.user_starts_speaking()
        await until(lambda: router.issued == 2 and len(router.releases) == 1)

        assert router.releases[0][1] == {"admission": "spa1.ticket-1"}
        await asyncio.sleep(0.05)
        await reply(plugin)
        assert router.responses[0][0]["Speko-Prepared-Admission"] == "spa1.ticket-2"
        await plugin.aclose()


async def test_option_off_makes_no_new_calls() -> None:
    router = FakeRouter()
    async with router_client(router) as client:
        plugin = LLM(client)
        session = FakeSession(_history())
        plugin.attach(session)
        plugin.prepare(_history())
        session.user_starts_speaking()
        await asyncio.sleep(0.05)

        stream = await reply(plugin)
        await plugin.aclose()

        assert session.handlers == {}
        assert router.admissions == []
        assert router.releases == []
        assert "Speko-Prepared-Admission" not in router.responses[0][0]
        assert stream.prepared_admission_outcome is None
        assert plugin.prepared_admission_outcome is None


@pytest.mark.parametrize("admission_reply", ["refused", "malformed", "none"])
async def test_prepare_failures_are_swallowed(admission_reply: str) -> None:
    router = FakeRouter()
    router.admission_reply = admission_reply
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        session = FakeSession(_history())
        plugin.attach(session)
        session.user_starts_speaking()
        await until(lambda: len(router.admissions) == 1)
        await asyncio.sleep(0.05)

        await reply(plugin)
        await plugin.aclose()

        assert "Speko-Prepared-Admission" not in router.responses[0][0]
        assert plugin.prepared_admission_outcome is None
        assert router.releases == []


@pytest.mark.parametrize("status", ["404", "405"])
async def test_router_without_the_route_turns_preparing_off(status: str) -> None:
    router = FakeRouter()
    router.admission_reply = status
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        session = FakeSession(_history())
        plugin.attach(session)
        session.user_starts_speaking()
        await until(lambda: len(router.admissions) == 1)
        await asyncio.sleep(0.05)

        await reply(plugin)
        session.user_starts_speaking()
        await asyncio.sleep(0.05)
        await reply(plugin)

        assert len(router.admissions) == 1
        assert all("Speko-Prepared-Admission" not in h for h, _ in router.responses)
        await plugin.aclose()


async def test_refusal_does_not_turn_preparing_off() -> None:
    router = FakeRouter()
    router.admission_reply = "refused"
    async with router_client(router) as client:
        plugin = LLM(client, prepare_admissions=True)
        plugin.prepare(_history())
        await until(lambda: len(router.admissions) == 1)
        await asyncio.sleep(0.05)
        router.admission_reply = "ticket"
        plugin.prepare(_history())
        await until(lambda: router.issued == 1)
        await plugin.aclose()


async def test_unreachable_router_never_raises_into_the_agent() -> None:
    client = RelayLLMClient(api_key="k", base_url="http://127.0.0.1:9")
    plugin = LLM(client, prepare_admissions=True)
    session = FakeSession(_history())
    plugin.attach(session)
    session.user_starts_speaking()
    await asyncio.sleep(0.2)
    await plugin.aclose()
    await client.aclose()


def test_go_timestamps_parse_on_every_supported_python() -> None:
    assert _parse_rfc3339("2026-11-05T17:00:00.123456789Z") == pytest.approx(
        datetime(2026, 11, 5, 17, 0, 0, 123456, timezone.utc).timestamp()
    )
    assert _parse_rfc3339("2026-11-05T19:00:00+02:00") == pytest.approx(
        datetime(2026, 11, 5, 17, 0, 0, tzinfo=timezone.utc).timestamp()
    )
    assert _parse_rfc3339("tomorrow") is None
    assert _parse_rfc3339(None) is None


def test_prepared_admission_repr_hides_the_ticket() -> None:
    from speko_gateway.relay import PreparedAdmission

    admission = PreparedAdmission(ticket="spa1.secret", expires_at=1.0, _deadline=1.0)
    assert "spa1.secret" not in repr(admission)
