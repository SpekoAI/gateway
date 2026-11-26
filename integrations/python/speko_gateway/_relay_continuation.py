"""Live STT stream continuation over the relay.

A dropped live STT stream continues in a new relay session. The server stays
stateless: it adds the continued session's audio offset to every timestamp and
says on finals how far the audio is finalized (``covered_ms``). This client
keeps an ordered log of the audio and commits it sent since that point and
replays it into the new session, so the app sees one transcript on one
timeline. Where the server cannot prove the boundary, the client replays a few
seconds of overlap and drops the words it already delivered, and says so.

Events this adds to the stream (all other events are unchanged):

- ``session.reconnecting`` ``{cause}``: the socket dropped; the transcript
  pauses while a new session opens. Interim text is void.
- ``session.continued`` ``{request_id, route, replayed_ms, exact}``: the server
  confirmed continuation and the log was replayed. ``exact`` is false when the
  seam rests on overlap and word matching rather than ``covered_ms``.
- ``session.restarted`` ``{request_id, route, replayed_ms, from_ms}``: the server
  did not confirm continuation. The log was replayed into a plain new session
  whose timestamps start at 0 again; ``from_ms`` is where on the old timeline
  that session's audio starts.
- ``session.gap`` ``{from_ms, to_ms}``: the transcript of that span of audio is
  not reliable: its audio fell out of the log, or the overlap could not be
  matched, so words in it may be missing or repeated.

Every ``input.commit`` resolves with exactly one ``transcript.final`` carrying
its ``commit_id``. A commit whose audio fell out of the log resolves with an
empty final that also carries ``commit_failed`` (the reason).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

from .client import CanonicalEvent, GatewayError

LOG_CAP_MS = 15_000
OVERLAP_MS = 5_000
MAX_ATTEMPTS = 3
OUTAGE_SECONDS = 30.0
RETRY_DELAY_SECONDS = 0.5

# Errors that a new session cannot fix: the same request would fail again, or
# the account cannot pay for it.
_NEVER_RETRY = frozenset(
    {
        "invalid_request",
        "authentication_failed",
        "insufficient_credit",
        "budget_exhausted",
    }
)
_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
_TAIL_WORDS = 64
_EVENTS = {"transcript.delta", "transcript.final", "usage.updated"}


class DialUnknown(Exception):
    """The dial ended before the server answered: it may have started a session."""


class DialRefused(Exception):
    """The server answered the dial with an error: no session started."""

    def __init__(self, code: str, retryable: bool) -> None:
        super().__init__(code)
        self.code, self.retryable = code, retryable


Dial = Callable[[str, str], Awaitable[tuple[aiohttp.ClientWebSocketResponse, dict]]]


@dataclass
class _Audio:
    start: int
    data: bytes


@dataclass
class _Commit:
    position: int
    commit_id: str


@dataclass
class Limits:
    log_cap_ms: int = LOG_CAP_MS
    overlap_ms: int = OVERLAP_MS
    max_attempts: int = MAX_ATTEMPTS
    outage_seconds: float = OUTAGE_SECONDS
    retry_delay: float = RETRY_DELAY_SECONDS


def _words(text: str) -> list[tuple[int, str]]:
    """(token index, normalized word) for every token that holds a word."""
    words = []
    for index, token in enumerate(text.split()):
        normalized = re.sub(r"[^\w]", "", token.casefold())
        if normalized:
            words.append((index, normalized))
    return words


def align(tail: list[str], head: list[str]) -> int | None:
    """Where ``head`` starts inside ``tail``, when exactly one place fits best.

    A place fits when ``head`` repeats ``tail`` from there to the end of either.
    The longest fit wins; a tie between places, or a best fit shorter than two
    words, is unclear and returns None rather than a guess.
    """
    best: tuple[int, int] | None = None
    tie = False
    for start in range(len(tail)):
        length = min(len(head), len(tail) - start)
        if length and head[:length] == tail[start : start + length]:
            if best is None or length > best[1]:
                best, tie = (start, length), False
            elif length == best[1]:
                tie = True
    if best is None or tie or best[1] < 2:
        return None
    return best[0]


class ContinuingSpeechSession:
    """A live relay STT stream that continues itself across dropped sockets."""

    def __init__(
        self,
        *,
        dial: Dial,
        configure: dict[str, Any],
        websocket: aiohttp.ClientWebSocketResponse,
        ready: dict[str, Any],
        metadata: dict[str, Any],
        sample_rate: int,
        channels: int,
        limits: Limits | None = None,
    ) -> None:
        self._dial, self._configure = dial, configure
        self._ws, self.metadata = websocket, metadata
        self._limits = limits or Limits()
        self._frame = 2 * channels
        self._bytes_per_second = sample_rate * self._frame
        self._lock = asyncio.Lock()
        self._input = asyncio.Event()
        self._finishing = self._closed = self._reading = False
        self._live = True
        # The server takes continuation only if it confirmed it on the first
        # session; an older server gets plain new sessions.
        self._supported = ready.get("continuation") is True
        self._confirmed = self._supported
        self._commit_ids = ready.get("commit_ids") is True
        # Positions are byte offsets into the stream's audio, each byte
        # counted once however often it is replayed.
        self._sent = 0
        self._trim = 0
        self._exact = True
        self._log: list[_Audio | _Commit] = []
        self._pending: dict[str, _Commit] = {}
        self._commit_seq = 0
        self._tail: deque[str] = deque(maxlen=_TAIL_WORDS)
        self._seam: list[str] | None = None
        self._seam_aligned = False
        self._seam_span = (0, 0)
        self._attempts = 0
        self._replay_end = 0

    # Positions ------------------------------------------------------------

    def _ms(self, position: int) -> int:
        return position * 1000 // self._bytes_per_second

    def _position(self, ms: int) -> int:
        position = ms * self._bytes_per_second // 1000
        return position - position % self._frame

    def _log_start(self) -> int:
        for entry in self._log:
            if isinstance(entry, _Audio):
                return entry.start
        return self._sent

    def _drop_audio_before(self, position: int) -> None:
        kept: list[_Audio | _Commit] = []
        for entry in self._log:
            if isinstance(entry, _Audio):
                end = entry.start + len(entry.data)
                if end <= position:
                    continue
                if entry.start < position:
                    entry = _Audio(position, entry.data[position - entry.start :])
            kept.append(entry)
        self._log = kept

    def _enforce_cap(self) -> None:
        cap = self._position(self._limits.log_cap_ms)
        if self._sent - self._log_start() > cap:
            self._drop_audio_before(self._sent - cap)

    def _advance(self, position: int, *, exact: bool) -> None:
        position = max(0, min(position, self._sent))
        if position > self._trim:
            self._trim = position
            self._drop_audio_before(position)
        self._exact = exact

    def _has_work(self) -> bool:
        return bool(self._pending) or any(
            isinstance(entry, _Audio) for entry in self._log
        )

    # Controls -------------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed or self._finishing:
            raise GatewayError("Relay speech session is closed", retryable=False)

    async def _send_live(self, value: dict[str, Any] | bytes) -> None:
        if not self._live:
            return
        try:
            if isinstance(value, bytes):
                await self._ws.send_bytes(value)
            else:
                await self._ws.send_json(value)
        except (ConnectionError, aiohttp.ClientError, RuntimeError):
            # The reader sees the drop and continues the stream; this input
            # is already in the log.
            self._live = False

    async def send_audio(self, data: bytes | bytearray | memoryview) -> None:
        chunk = bytes(data)
        async with self._lock:
            self._check_open()
            if not chunk:
                return
            self._log.append(_Audio(self._sent, chunk))
            self._sent += len(chunk)
            self._enforce_cap()
            self._input.set()
            await self._send_live(chunk)

    async def commit_audio(self) -> str | None:
        async with self._lock:
            self._check_open()
            if not self._commit_ids:
                # Without echoed ids an answer cannot be matched to its commit,
                # so the commit is sent but cannot be carried across a drop.
                await self._send_live({"type": "input.commit"})
                return None
            self._commit_seq += 1
            commit = _Commit(self._sent, f"speko-c{self._commit_seq}")
            self._log.append(commit)
            self._pending[commit.commit_id] = commit
            self._input.set()
            await self._send_live({"type": "input.commit", "commit_id": commit.commit_id})
            return commit.commit_id

    async def append_text(self, text: str) -> None:
        raise ValueError("Nonempty text input requires TTS")

    async def commit_text(self) -> None:
        raise ValueError("Text commit requires TTS")

    async def cancel(self) -> None:
        await self.aclose()

    async def finish(self) -> None:
        async with self._lock:
            if self._closed or self._finishing:
                return
            self._finishing = True
            self._input.set()
            await self._send_live({"type": "session.close"})

    async def aclose(self) -> None:
        self._closed = True
        self._input.set()
        await self._ws.close()

    # Events ---------------------------------------------------------------

    async def events(self) -> AsyncIterator[CanonicalEvent]:
        if self._reading:
            raise GatewayError(
                "Relay speech events already have a reader", retryable=False
            )
        self._reading = True
        try:
            while True:
                cause = "socket_closed"
                try:
                    async for message in self._ws:
                        if message.type is not aiohttp.WSMsgType.TEXT:
                            if message.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                            ):
                                break
                            raise GatewayError(
                                "Relay speech protocol failed", retryable=False
                            )
                        payload = _payload(message.data)
                        kind = payload["type"]
                        if kind == "error":
                            error = payload.get("error")
                            error = error if isinstance(error, dict) else {}
                            code = error.get("code")
                            if error.get("retryable") is True and code not in _NEVER_RETRY:
                                cause = str(code or "error")
                                break
                            for event in self._fail_pending("session_failed"):
                                yield event
                            raise _relay_error(payload)
                        if kind == "session.closed":
                            if not isinstance(payload.get("usage"), dict):
                                raise GatewayError(
                                    "Relay speech ended without final usage",
                                    retryable=False,
                                )
                            yield CanonicalEvent(
                                type="usage.reported", data=payload["usage"]
                            )
                            yield CanonicalEvent(type="session.closed")
                            return
                        if kind not in _EVENTS:
                            raise GatewayError(
                                "Relay speech protocol failed", retryable=False
                            )
                        for event in self._on_event(kind, payload):
                            yield event
                except (aiohttp.ClientError, ConnectionError, asyncio.TimeoutError):
                    cause = "socket_error"
                if self._closed:
                    return
                async for event in self._recover(cause):
                    yield event
        finally:
            await self.aclose()

    def _on_event(self, kind: str, payload: dict[str, Any]) -> list[CanonicalEvent]:
        data = {key: value for key, value in payload.items() if key != "type"}
        if kind == "usage.updated":
            if not isinstance(payload.get("usage"), dict):
                raise GatewayError("Relay speech protocol failed", retryable=False)
            return [CanonicalEvent(type="usage.progress", data=payload["usage"])]
        if kind == "transcript.delta":
            return [CanonicalEvent(type=kind, data=data)]
        events: list[CanonicalEvent] = []
        commit_id = data.get("commit_id")
        answered = isinstance(commit_id, str) and commit_id in self._pending
        if answered:
            commit = self._pending.pop(commit_id)
            self._log = [entry for entry in self._log if entry is not commit]
        text = data.get("text")
        deliver = True
        if self._seam is not None and isinstance(text, str) and _words(text):
            gap, deliver = self._stitch(data)
            events.extend(gap)
        covered = data.get("covered_ms")
        if (
            self._confirmed
            and type(covered) is int
            and covered >= 0
        ):
            self._advance(self._position(covered), exact=True)
        else:
            self._advance(
                self._sent - self._position(self._limits.overlap_ms), exact=False
            )
        if self._sent > self._replay_end:
            # A final on audio sent after the last replay: the stream works.
            self._attempts = 0
        text = data.get("text")
        if isinstance(text, str):
            self._tail.extend(word for _, word in _words(text))
        if deliver:
            _final_span(data)
            events.append(CanonicalEvent(type=kind, data=data))
        return events

    def _stitch(self, data: dict[str, Any]) -> tuple[list[CanonicalEvent], bool]:
        """Drop the words of a final that repeat what was already delivered.

        Returns a gap event when the overlap cannot be matched, and whether the
        final still has anything to deliver.
        """
        assert self._seam is not None
        text: str = data["text"]
        words = _words(text)
        head = [word for _, word in words]
        if self._seam_aligned:
            length = min(len(head), len(self._seam))
            start = 0 if head[:length] == self._seam[:length] else None
        else:
            start = align(self._seam, head)
        if start is None:
            self._seam = None
            gap = CanonicalEvent(
                type="session.gap",
                data={"from_ms": self._seam_span[0], "to_ms": self._seam_span[1]},
            )
            return [gap], True
        repeated = min(len(head), len(self._seam) - start)
        rest = self._seam[start + repeated :]
        self._seam, self._seam_aligned = (rest, True) if rest and repeated == len(head) else (None, False)
        tokens = text.split()
        data["text"] = " ".join(tokens[words[repeated - 1][0] + 1 :])
        _drop_segment_words(data, repeated)
        return [], bool(data["text"]) or "commit_id" in data

    def _fail_pending(self, reason: str, *, only: list[_Commit] | None = None) -> list[CanonicalEvent]:
        failed = list(self._pending.values()) if only is None else only
        events = []
        for commit in failed:
            self._pending.pop(commit.commit_id, None)
            self._log = [entry for entry in self._log if entry is not commit]
            events.append(
                CanonicalEvent(
                    type="transcript.final",
                    data={"text": "", "commit_id": commit.commit_id, "commit_failed": reason},
                )
            )
        return events

    # Recovery -------------------------------------------------------------

    def _continuation_configure(self) -> tuple[str, bool]:
        """The configure frame for the next session, and whether it continues."""
        configure = dict(self._configure)
        request_id = self.metadata.get("request_id")
        asked = (
            self._supported
            and isinstance(request_id, str)
            and _REQUEST_ID.fullmatch(request_id) is not None
        )
        if asked:
            configure["continuation"] = {
                "of": request_id,
                "audio_offset_ms": self._ms(self._trim),
            }
        return json.dumps(configure, separators=(",", ":")), asked

    def _give_up(self, cause: str) -> GatewayError:
        return GatewayError(
            "Relay speech stream could not be continued",
            code=cause,
            source="relay",
            retryable=False,
        )

    async def _recover(self, cause: str) -> AsyncIterator[CanonicalEvent]:
        self._live = False
        await self._ws.close()
        yield CanonicalEvent(type="session.reconnecting", data={"cause": cause})
        # Not while there is no audio to send: a new session would bill an idle
        # stream. Wait for input instead.
        while not self._has_work():
            if self._closed:
                return
            if self._finishing:
                raise GatewayError(
                    "Relay speech ended without final usage", retryable=False
                )
            self._input.clear()
            await self._input.wait()
        if self._closed:
            return
        deadline = time.monotonic() + self._limits.outage_seconds
        # The seam's audio offset is the trim point. It cannot move while no
        # session runs, so a retried dial sends the same configure bytes.
        key, (configure, asked) = str(uuid.uuid4()), self._continuation_configure()
        while True:
            if self._closed:
                return
            remaining = deadline - time.monotonic()
            if self._attempts >= self._limits.max_attempts or remaining <= 0:
                for event in self._fail_pending("stream_not_continued"):
                    yield event
                raise self._give_up(cause)
            self._attempts += 1
            try:
                websocket, ready = await self._dial(configure, key)
            except DialUnknown:
                # The server may have started this request: same key, same
                # bytes, so it answers this one rather than starting another.
                await asyncio.sleep(min(self._limits.retry_delay, remaining))
                continue
            except DialRefused as refusal:
                cause = refusal.code
                if refusal.code == "request_in_progress":
                    self._attempts -= 1
                elif refusal.code == "request_already_started":
                    key = str(uuid.uuid4())
                    continue
                elif refusal.code == "concurrency_exhausted":
                    # The old session frees its slot shortly; wait inside the
                    # outage budget without spending an attempt.
                    self._attempts -= 1
                    key = str(uuid.uuid4())
                elif refusal.code in _NEVER_RETRY or not refusal.retryable:
                    for event in self._fail_pending(refusal.code):
                        yield event
                    raise self._give_up(refusal.code) from None
                else:
                    key = str(uuid.uuid4())
                await asyncio.sleep(min(self._limits.retry_delay, remaining))
                continue
            replayed = await self._resume(websocket, ready, asked=asked)
            if replayed is None:
                # The new socket failed during replay; that session started,
                # so the next dial needs a new key.
                key, cause = str(uuid.uuid4()), "socket_closed"
                continue
            for event in replayed:
                yield event
            return

    async def _resume(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        ready: dict[str, Any],
        *,
        asked: bool,
    ) -> list[CanonicalEvent] | None:
        events: list[CanonicalEvent] = []
        confirmed = asked and ready.get("continuation") is True
        async with self._lock:
            log_start = self._log_start()
            lost = log_start > self._trim
            if lost:
                events.extend(
                    self._fail_pending(
                        "audio_fell_out_of_log",
                        only=[c for c in self._pending.values() if c.position > self._trim],
                    )
                )
                events.append(
                    CanonicalEvent(
                        type="session.gap",
                        data={"from_ms": self._ms(self._trim), "to_ms": self._ms(log_start)},
                    )
                )
            self._ws, self._live = websocket, True
            self._confirmed = confirmed
            self._commit_ids = ready.get("commit_ids") is True
            self.metadata = {
                **self.metadata,
                "request_id": ready["request_id"],
                "route": dict(ready["route"]),
            }
            for entry in self._log:
                if isinstance(entry, _Audio):
                    await self._send_live(entry.data)
                else:
                    await self._send_live(
                        {"type": "input.commit", "commit_id": entry.commit_id}
                    )
            if self._finishing:
                await self._send_live({"type": "session.close"})
            if not self._live:
                await websocket.close()
                return None
            self._replay_end = self._sent
            replayed_ms = self._ms(self._sent - max(self._trim, log_start))
            if not self._exact and self._tail:
                self._seam, self._seam_aligned = list(self._tail), False
                self._seam_span = (self._ms(self._trim), self._ms(self._sent))
            else:
                self._seam = None
            if confirmed:
                events.append(
                    CanonicalEvent(
                        type="session.continued",
                        data={
                            "request_id": ready["request_id"],
                            "route": dict(ready["route"]),
                            "replayed_ms": replayed_ms,
                            "exact": self._exact and not lost,
                        },
                    )
                )
            else:
                events.append(
                    CanonicalEvent(
                        type="session.restarted",
                        data={
                            "request_id": ready["request_id"],
                            "route": dict(ready["route"]),
                            "replayed_ms": replayed_ms,
                            "from_ms": self._ms(max(self._trim, log_start)),
                        },
                    )
                )
        return events


def _payload(raw: Any) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        raise GatewayError("Relay speech protocol failed", retryable=False) from None
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise GatewayError("Relay speech protocol failed", retryable=False)
    return value


def _relay_error(payload: dict[str, Any]) -> GatewayError:
    from .relay_speech import _error

    return _error(payload)


def _final_span(data: dict[str, Any]) -> None:
    segments = data.get("segments")
    if (
        isinstance(segments, list)
        and segments
        and all(isinstance(segment, dict) for segment in segments)
    ):
        data["audio_start_ms"] = segments[0].get("start_ms")
        data["audio_end_ms"] = segments[-1].get("end_ms")


def _drop_segment_words(data: dict[str, Any], count: int) -> None:
    """Drop the segments that hold the first ``count`` words of a final.

    A segment only partly repeated is dropped whole: segments may cover less
    than the text, and a borrowed start time would be invented.
    """
    segments = data.get("segments")
    if not isinstance(segments, list):
        return
    kept, seen = [], 0
    for segment in segments:
        text = segment.get("text") if isinstance(segment, dict) else None
        words = len(_words(text)) if isinstance(text, str) else 0
        if seen >= count:
            kept.append(segment)
        seen += words
    if kept:
        data["segments"] = kept
    else:
        data.pop("segments", None)
