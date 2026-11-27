"""Live STT stream continuation over the relay.

A dropped live STT stream continues in a new relay session. The server stays
stateless: it adds the continued session's audio offset to every timestamp and
says on finals how far the audio is finalized (``covered_ms``). This client
keeps an ordered log of the audio and commits since that point and replays it
into the new session, so the app sees one transcript on one timeline. Where
the server cannot prove the boundary, the client replays a few seconds of
overlap and drops the words it already delivered, and says so.

Positions are byte offsets into the stream's audio, each byte counted once
however often it is replayed. Two of them matter: what the app handed over
(the end of the log) and what was delivered to the current socket. Only
delivered audio reached a provider, so only it can be finalized, proven
covered, or judged silent.

Events this adds to the stream (all other events are unchanged):

- ``session.reconnecting`` ``{cause}``: the socket dropped; the transcript
  pauses while a new session opens. Interim text is void.
- ``session.continued`` ``{request_id, route, replayed_ms, exact}``: the new
  session confirmed continuation and the replay has begun (its first frame is
  out). ``exact`` is false when the seam rests on overlap and matching rather
  than ``covered_ms``.
- ``session.restarted`` ``{request_id, route, replayed_ms, from_ms}``: the new
  session did not confirm continuation. The log is replayed into a plain new
  session, which counts its times from 0; the client adds ``from_ms`` (where on
  the stream that session's audio starts), so its times stay on the stream's
  one timeline as after ``session.continued``.
- ``session.gap`` ``{from_ms, to_ms}``: the transcript of that span of audio is
  not reliable: its audio fell out of the log, or the seam could not be
  matched, so words in it may be missing or repeated.

The replay, and input that arrives meanwhile, goes out in 100 ms frames at no
more than 2x realtime until it has caught up; ``recovering`` is true from
the drop until then. The outage budget (3 attempts, 30 s without a session)
bounds reaching a new session, not catching up. A write that does not finish
in 10 s is a drop like any other, so a hung socket never holds the stream.
Delivered audio a provider found no speech in (no
non-empty delta since the last final) that overflows the 15 s log is silence:
it is dropped without a gap. While a new session is being opened the log is
not trimmed, so the offset it was opened with stays the replay's start; the
outage budget bounds that growth.

Every ``input.commit`` resolves with exactly one ``transcript.final`` carrying
its ``commit_id``, and no id reaches the app twice. A commit that cannot be
answered (its own audio fell out of the log, the session closed or failed, the
new session does not echo commit ids) resolves with an empty final that also
carries ``commit_failed`` (the reason).
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
# Deepgram advises against sending much faster than realtime ("if you send a
# large buffer of audio, the stream may wind up being significantly delayed",
# 1.25x in its guidance) and Speechmatics warns that sending fast fills TCP
# buffers. At 1.25x a 4.4 s backlog took 18 s to catch up on a live Deepgram
# run, the transcript lagging all along; 2x catches up in about the backlog's
# own length, in 100 ms frames.
REPLAY_SPEED = 2.0
# Segment ends are rounded to whole ms and covered_ms is floored, so a word
# that ends at the covered boundary may end up to 1 ms past it.
ROUNDING_MS = 1
REPLAY_FRAME_MS = 100
MAX_PENDING_COMMITS = 256
# A write that does not finish in this long is treated as a dropped socket: a
# 100 ms frame only blocks this long when the connection is gone.
WRITE_TIMEOUT_SECONDS = 10.0
# A replayed word repeats an old one only if it has the same text and starts
# within this of it. Two sessions decoding the same audio place a word's start
# tens of ms apart (10-20 ms acoustic frames, decoder alignment), while a word
# said again starts at least one word's length later: spoken words last well
# over 120 ms, so a real repeat ("yes yes") falls outside it.
REPEAT_START_TOLERANCE_MS = 120
# Delivered words kept to match a straddling segment's start against.
TAIL_WORDS = 30
# asyncio.TimeoutError is its own class before Python 3.11.
_TIMEOUTS = (TimeoutError, asyncio.TimeoutError)

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
_COMMIT_ID = re.compile(r"speko-c([0-9]+)")
_EVENTS = {"transcript.delta", "transcript.final", "usage.updated"}
_PUNCTUATION = "\"'.,;:!?()[]{}«»“”‘’…-"


class DialUnknown(Exception):
    """The dial ended before the server answered: it may have started a session."""


class DialRefused(Exception):
    """The server answered the dial with an error: no session started."""

    def __init__(self, code: str, retryable: bool) -> None:
        super().__init__(code)
        self.code, self.retryable = code, retryable


class DialCancelled(Exception):
    """The session was closed before the dial sent its configure."""


# dial(configure, idempotency key, cancelled): cancelled() is checked right
# before the configure goes out, so a cancelled stream starts no new session.
Dial = Callable[
    [str, str, Callable[[], bool]],
    Awaitable[tuple[aiohttp.ClientWebSocketResponse, dict]],
]


@dataclass
class _Audio:
    start: int
    data: bytes

    @property
    def end(self) -> int:
        return self.start + len(self.data)


@dataclass
class _Commit:
    position: int
    # None: the session does not echo ids, so the answer cannot be matched.
    commit_id: str | None
    # Where this commit's own audio starts: the previous commit's position.
    audio_from: int


@dataclass
class _Final:
    words: list[str]
    # End of the final's last timed segment on the stream timeline, or None.
    end_ms: int | None
    # Audio delivered when it arrived: it holds no word after this.
    arrived: int
    # Its timed segments: (normalized words, start_ms, end_ms) on the stream
    # timeline; empty when the final had none.
    segments: tuple[tuple[tuple[str, ...], int, int], ...] = ()
    # Its words with their own times (normalized word, start_ms, end_ms),
    # when the final carried words[]; empty otherwise.
    timed_words: tuple[tuple[str, int, int], ...] = ()


@dataclass
class _Seam:
    from_ms: int
    to_ms: int
    # The old finals that may hold words inside the replay: their words, and
    # the end of the last one when every one of them was timed.
    old_words: list[str]
    old_end_ms: int | None
    # The last TAIL_WORDS delivered words, across finals, in order.
    tail: list[str]
    # The old finals' timed segments, when every one of them was timed.
    old_segments: list[tuple[tuple[str, ...], int, int]]
    # The old finals' timed words, when every one of them carried words[].
    old_timed_words: tuple[tuple[str, int, int], ...] = ()
    # Old timed words before this index have repeated a new word already.
    word_cursor: int = 0
    # Some new words were dropped as repeats of old ones.
    removed: bool = False
    aligned: bool = False


@dataclass
class Limits:
    log_cap_ms: int = LOG_CAP_MS
    overlap_ms: int = OVERLAP_MS
    max_attempts: int = MAX_ATTEMPTS
    outage_seconds: float = OUTAGE_SECONDS
    retry_delay: float = RETRY_DELAY_SECONDS
    replay_speed: float = REPLAY_SPEED
    replay_frame_ms: int = REPLAY_FRAME_MS
    max_pending_commits: int = MAX_PENDING_COMMITS
    write_timeout: float = WRITE_TIMEOUT_SECONDS


def _words(text: str) -> list[tuple[int, str]]:
    """(token index, normalized word) for every token that holds a word.

    Only surrounding punctuation goes, so "1.5" and "15" stay different.
    """
    words = []
    for index, token in enumerate(text.split()):
        normalized = token.casefold().strip(_PUNCTUATION)
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


def _timed_segments(data: dict[str, Any]) -> list[dict[str, Any]] | None:
    segments = data.get("segments")
    if not isinstance(segments, list) or not segments:
        return None
    for segment in segments:
        if (
            not isinstance(segment, dict)
            or not isinstance(segment.get("text"), str)
            or type(segment.get("start_ms")) is not int
            or type(segment.get("end_ms")) is not int
        ):
            return None
    return segments


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
        # Set once the stream has answered its pending commits for the last
        # time: a commit after that could never be answered.
        self._ended = False
        self._live = True
        # The server takes continuation only if it confirmed it on the first
        # session; an older server gets plain new sessions.
        self._supported = ready.get("continuation") is True
        self._confirmed = self._supported
        self._commit_ids = ready.get("commit_ids") is True
        self._appended = 0
        self._delivered = 0
        self._trim = 0
        self._exact = True
        self._covered_seen = False
        # The current session has sent covered_ms.
        self._session_covered = False
        self._heard = False
        self._log: list[_Audio | _Commit] = []
        self._pending: dict[str, _Commit] = {}
        self._commit_seq = 0
        self._last_commit = 0
        self._history: deque[_Final] = deque(maxlen=32)
        self._seam: _Seam | None = None
        # End of the last delivered timed segment, on the stream timeline.
        self._last_end_ms: int | None = None
        # End of the latest timed final received, repeats included.
        self._final_end_ms: int | None = None
        # The current session's timestamps count from this stream position:
        # 0 on a confirmed continuation, its replay start on a plain one.
        self._base = 0
        self._attempts = 0
        # Outage time left: spent only while there is no session, so catching
        # up never runs it down. Reset by a final on new audio.
        self._budget_left = self._limits.outage_seconds
        self._deadline: float | None = None
        # covered_ms behind the current replay point, when it set it.
        self._covered_boundary: int | None = None
        self._replay_end = 0
        self._recovering = False
        self._pinned = False
        self._queue: deque[_Audio | _Commit] = deque()
        self._draining = False
        self._drainer: asyncio.Task[None] | None = None

    @property
    def recovering(self) -> bool:
        """True from a drop until the replay has caught up with live input."""
        return self._recovering

    # Positions ------------------------------------------------------------

    def _ms(self, position: int) -> int:
        return position * 1000 // self._bytes_per_second

    def _position(self, ms: int) -> int:
        """The stream position at ``ms``, on a frame and so a sample boundary."""
        position = ms * self._bytes_per_second // 1000
        return position - position % self._frame

    def _aligned(self, position: int) -> int:
        """``position`` rounded down to a whole millisecond, which is what an
        offset on the wire can name."""
        return self._position(self._ms(max(0, position)))

    def _log_start(self) -> int:
        for entry in self._log:
            if isinstance(entry, _Audio):
                return entry.start
        return self._appended

    def _drop_audio_before(self, position: int) -> None:
        kept: list[_Audio | _Commit] = []
        for entry in self._log:
            if isinstance(entry, _Audio):
                if entry.end <= position:
                    continue
                if entry.start < position:
                    entry = _Audio(position, entry.data[position - entry.start :])
            elif entry.commit_id is None and entry.position < position:
                continue
            kept.append(entry)
        self._log = kept

    def _enforce_cap(self) -> None:
        if self._pinned:
            return
        # Audio that never reached a provider is never dropped: the session it
        # goes to would see a hole in its timeline.
        cut = self._aligned(
            min(
                self._appended - self._position(self._limits.log_cap_ms),
                self._delivered,
            )
        )
        start = self._log_start()
        if cut <= start:
            return
        # Delivered audio a provider heard no speech in is silence: dropping it
        # loses no word, so the replay point moves past it without a gap. It
        # stays exact only on a boundary covered_ms proved.
        silent = not self._heard and start <= self._trim
        self._drop_audio_before(cut)
        if silent:
            self._trim = max(self._trim, cut)
            self._exact = self._exact and self._covered_seen

    def _advance(self, position: int, *, exact: bool) -> None:
        position = self._aligned(min(position, self._delivered))
        if position > self._trim:
            self._trim = position
            self._drop_audio_before(position)
        self._exact = exact

    def _boundary_ms(self, start: int) -> int:
        """Where finalized audio ends for a replay from ``start``: covered_ms,
        or else the last final's end, when it set this point, though the
        offset had to round it down to a whole ms of samples; otherwise the
        start itself. Alignment never turns new audio into a repeat."""
        covered = self._covered_boundary
        if covered is not None and self._aligned(self._position(covered)) == start:
            return covered
        end = self._final_end_ms
        if (
            not self._session_covered
            and end is not None
            and self._aligned(self._position(end)) == start
        ):
            return end
        return self._ms(start)

    def _has_work(self) -> bool:
        return bool(self._pending) or any(
            isinstance(entry, _Audio) for entry in self._log
        )

    # Controls -------------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed or self._finishing or self._ended:
            raise GatewayError("Relay speech session is closed", retryable=False)

    async def _deliver(self, entry: _Audio | _Commit) -> None:
        """Write one log entry to the current socket. A failed write leaves it
        undelivered: the reader sees the drop and the replay sends it."""
        if self._closed or not self._live:
            return
        try:
            if isinstance(entry, _Audio):
                await self._bounded(self._ws.send_bytes(entry.data))
                self._delivered = entry.end
            elif entry.commit_id is None:
                await self._bounded(self._ws.send_json({"type": "input.commit"}))
            else:
                await self._bounded(
                    self._ws.send_json(
                        {"type": "input.commit", "commit_id": entry.commit_id}
                    )
                )
        except (ConnectionError, aiohttp.ClientError, RuntimeError):
            self._live = False
        except _TIMEOUTS:
            await self._write_timed_out()

    async def _send_control(self, value: dict[str, Any]) -> None:
        if self._closed or not self._live:
            return
        try:
            await self._bounded(self._ws.send_json(value))
        except (ConnectionError, aiohttp.ClientError, RuntimeError):
            self._live = False
        except _TIMEOUTS:
            await self._write_timed_out()

    async def _bounded(self, operation: Awaitable[Any]) -> Any:
        """Await a write for at most write_timeout, so a hung socket cannot
        hold the lock that send, commit, finish and adopting a new session
        all wait on."""
        return await asyncio.wait_for(operation, self._limits.write_timeout)

    async def _write_timed_out(self) -> None:
        # A drop: close the socket so the reader sees it end and continues.
        self._live = False
        await self._close_socket(self._ws)

    async def _close_socket(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        try:
            await asyncio.wait_for(websocket.close(), 1.0)
        except (*_TIMEOUTS, ConnectionError, aiohttp.ClientError, RuntimeError):
            pass

    def _check_backlog(self, size: int) -> None:
        """Refuse a chunk that would leave more undelivered audio than the log
        cap plus the outage budget, counting the chunk itself."""
        limit = self._position(
            self._limits.log_cap_ms + int(self._limits.outage_seconds * 1000)
        )
        if self._appended + size - self._delivered > limit:
            raise GatewayError(
                "Relay speech input fell too far behind", retryable=False
            )

    async def send_audio(self, data: bytes | bytearray | memoryview) -> None:
        chunk = bytes(data)
        async with self._lock:
            self._check_open()
            if not chunk:
                return
            self._check_backlog(len(chunk))
            entry = _Audio(self._appended, chunk)
            self._log.append(entry)
            self._appended = entry.end
            self._input.set()
            if self._draining:
                self._queue.extend(self._pieces(entry))
            elif self._live and not self._recovering:
                await self._deliver(entry)
            self._enforce_cap()

    async def commit_audio(self) -> str | None:
        async with self._lock:
            self._check_open()
            commit_id = None
            # Every commit kept in the log counts, with or without an id.
            if (
                sum(isinstance(entry, _Commit) for entry in self._log)
                >= self._limits.max_pending_commits
            ):
                raise GatewayError(
                    "Relay speech holds too many commit markers in its log "
                    "(answered or not, with or without an id)",
                    retryable=False,
                )
            if self._commit_ids:
                self._commit_seq += 1
                commit_id = f"speko-c{self._commit_seq}"
            commit = _Commit(self._appended, commit_id, self._last_commit)
            self._last_commit = self._appended
            self._log.append(commit)
            if commit_id is not None:
                self._pending[commit_id] = commit
            self._input.set()
            # A commit never overtakes audio queued before it.
            if self._draining:
                self._queue.append(commit)
            elif self._live and not self._recovering:
                await self._deliver(commit)
            return commit_id

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
            if not self._recovering:
                await self._send_control({"type": "session.close"})
            # Otherwise the replay closes the session once it has caught up.

    async def aclose(self) -> None:
        self._closed = True
        self._input.set()
        if self._drainer is not None:
            self._drainer.cancel()
        await self._close_socket(self._ws)

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
                messages = self._ws.__aiter__()
                try:
                    while True:
                        try:
                            message = await messages.__anext__()
                        except StopAsyncIteration:
                            break
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
                            raise _relay_error(payload)
                        if kind == "session.closed":
                            if not isinstance(payload.get("usage"), dict):
                                raise GatewayError(
                                    "Relay speech ended without final usage",
                                    retryable=False,
                                )
                            self._ended = True
                            for event in self._fail_pending("session_closed"):
                                yield event
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
                except (aiohttp.ClientError, ConnectionError, *_TIMEOUTS):
                    cause = "socket_error"
                if self._closed:
                    return
                async for event in self._recover(cause):
                    yield event
        except Exception as error:
            # Whatever ends the stream, every commit still waiting is answered.
            self._ended = True
            for event in self._fail_pending(
                str(getattr(error, "code", "") or "stream_failed")
            ):
                yield event
            raise
        finally:
            await self.aclose()

    def _on_event(self, kind: str, payload: dict[str, Any]) -> list[CanonicalEvent]:
        data = {key: value for key, value in payload.items() if key != "type"}
        if self._base and kind != "usage.updated":
            # A plain new session counts from 0: put its times on the stream's
            # timeline, as a continued session does itself.
            data = _shifted(data, self._ms(self._base))
        if kind == "usage.updated":
            if not isinstance(payload.get("usage"), dict):
                raise GatewayError("Relay speech protocol failed", retryable=False)
            return [CanonicalEvent(type="usage.progress", data=payload["usage"])]
        if kind == "transcript.delta":
            if data.get("text"):
                self._heard = True
            return [CanonicalEvent(type=kind, data=data)]
        return self._on_final(data)

    def _on_final(self, data: dict[str, Any]) -> list[CanonicalEvent]:
        events: list[CanonicalEvent] = []
        commit_id = data.get("commit_id")
        if isinstance(commit_id, str):
            if commit_id in self._pending:
                commit = self._pending.pop(commit_id)
                self._log = [entry for entry in self._log if entry is not commit]
            elif self._ours(commit_id):
                # Already answered or failed: the app never sees an id twice.
                del data["commit_id"]
        arrived = self._delivered
        incoming = _timed_segments(data)
        if incoming:
            self._final_end_ms = max(self._final_end_ms or 0, incoming[-1]["end_ms"])
        deliver = True
        text = data.get("text")
        if self._seam is not None and isinstance(text, str) and _words(text):
            gap, deliver = self._stitch(data)
            events.extend(gap)
        self._heard = False
        # The replay point is covered_ms when the session sends it.
        covered = data.get("covered_ms")
        # Without a usable covered_ms the replay point is the end of the last
        # timed final's words: the next session's words start there and are
        # new, with nothing to dedupe. Final latency grows while a replay
        # catches up, so the audio delivered by arrival can be far past them:
        # 5 s before arrival is only for finals that carry no times at all.
        if self._final_end_ms is not None:
            overlap_start = self._position(self._final_end_ms)
        else:
            overlap_start = arrived - self._position(self._limits.overlap_ms)
        if self._confirmed and type(covered) is int and covered >= 0:
            # Once a session sends covered_ms it alone moves the replay point:
            # audio after it is not closed yet, whatever finals say. Behind
            # the last final's end (beyond rounding) the seam rules take the
            # overlap, so the replay is not exact.
            self._session_covered = self._covered_seen = True
            position = self._position(covered)
            behind = (
                self._final_end_ms is not None
                and covered < self._final_end_ms - ROUNDING_MS
            )
            self._advance(position, exact=not behind)
            if self._aligned(position) == self._trim:
                # covered_ms names the replay point, even when it moved
                # within the same whole ms of samples.
                self._covered_boundary = covered
        else:
            covered = None
            if not self._session_covered:
                self._advance(overlap_start, exact=False)
        segments = _timed_segments(data)
        end_ms = segments[-1]["end_ms"] if segments else None
        if self._on_new_audio(covered, end_ms):
            # A final on audio sent after the last replay: the stream works,
            # so the next drop starts a new outage.
            self._attempts = 0
            self._budget_left = self._limits.outage_seconds
        text = data.get("text")
        if isinstance(text, str) and deliver:
            words = [word for _, word in _words(text)]
            if words:
                self._history.append(
                    _Final(
                        words,
                        end_ms,
                        arrived,
                        tuple(
                            (
                                tuple(word for _, word in _words(segment["text"])),
                                segment["start_ms"],
                                segment["end_ms"],
                            )
                            for segment in segments or ()
                        ),
                        tuple(_timed_words(data) or ()),
                    )
                )
        if deliver:
            self._keep_order(data)
            _final_span(data)
            events.append(CanonicalEvent(type="transcript.final", data=data))
        return events

    def _keep_order(self, data: dict[str, Any]) -> None:
        """Keep timed segments in order across a seam: the first word after a
        covered_ms boundary (floored) may start up to ROUNDING_MS before the
        last delivered word ended (rounded). It starts where that one ended."""
        segments = _timed_segments(data)
        if not segments:
            return
        last = self._last_end_ms
        first = segments[0]
        start = first["start_ms"]
        if last is not None and start < last <= start + ROUNDING_MS:
            segments[0] = {**first, "start_ms": min(last, first["end_ms"])}
            data["segments"] = segments
        self._last_end_ms = segments[-1]["end_ms"]

    def _ours(self, commit_id: str) -> bool:
        match = _COMMIT_ID.fullmatch(commit_id)
        return match is not None and int(match.group(1)) <= self._commit_seq

    def _on_new_audio(self, covered: int | None, end_ms: int | None) -> bool:
        if covered is not None:
            return self._position(covered) > self._replay_end
        if end_ms is not None:
            # Segment ends are rounded: one that ends within the rounding of
            # the replay's end may hold replayed audio only.
            return end_ms > self._ms(self._replay_end) + ROUNDING_MS
        # An untimed final proves nothing about which audio it covers.
        return False

    def _stitch(self, data: dict[str, Any]) -> tuple[list[CanonicalEvent], bool]:
        """Drop the words of a final that repeat what was already delivered,
        and never drop or repeat words silently where that is unsure.

        Both sessions are on one timeline. With words[] on both sides, a new
        word repeats an old one when it has the same text and starts within
        REPEAT_START_TOLERANCE_MS of it, each old word matching once over the
        whole seam. With segment times only, a segment inside the delivered
        audio is a repeat, one that starts at the old end is new (both within
        ROUNDING_MS), and one across it loses its leading words only when they
        exactly repeat every old word it overlaps; otherwise it is kept whole
        with a gap up to the old end. On the words[] path a word that starts
        at the old end is new too.
        Finals without times are aligned by text. Returns the gap event, if
        any, and whether the final still has anything to deliver.
        """
        seam = self._seam
        assert seam is not None
        words = _words(data["text"])
        head = [word for _, word in words]
        if seam.old_end_ms is not None:
            new_words = _timed_words(data)
            if (
                seam.old_timed_words
                and new_words is not None
                and [word for word, _, _ in new_words] == head
            ):
                return self._stitch_words(data, words, new_words)
            segments = _timed_segments(data)
            counts = [len(_words(segment["text"])) for segment in segments or []]
            if segments is not None and sum(counts) == len(words):
                return self._stitch_segments(data, words, segments, counts)
            if seam.removed:
                # A timed repeat already went: text alone cannot tell this
                # final from it, so it is kept and the seam reported.
                self._seam = None
                return [self._gap_event(seam)], True
        return self._stitch_text(data, words)

    def _stitch_words(
        self,
        data: dict[str, Any],
        words: list[tuple[int, str]],
        new_words: list[tuple[str, int, int]],
    ) -> tuple[list[CanonicalEvent], bool]:
        seam = self._seam
        assert seam is not None and seam.old_end_ms is not None
        old, repeated, gap = seam.old_timed_words, 0, None
        for word, start, _ in new_words:
            match = next(
                (
                    index
                    for index in range(seam.word_cursor, len(old))
                    if old[index][0] == word
                    and abs(old[index][1] - start) <= REPEAT_START_TOLERANCE_MS
                ),
                None,
            )
            if start >= seam.old_end_ms - ROUNDING_MS:
                # Starts where every delivered word had ended: new, whatever
                # it matches.
                break
            if match is None:
                if start < seam.old_end_ms - REPEAT_START_TOLERANCE_MS:
                    # A different word over audio already delivered.
                    gap = (start, seam.old_end_ms)
                break
            # Each old word repeats one new word at most, across finals too.
            seam.word_cursor, repeated = match + 1, repeated + 1
        if repeated < len(new_words):
            self._seam = None
        elif repeated:
            seam.removed = True
        self._cut_words(data, words, repeated)
        data["words"] = data["words"][repeated:]
        _drop_segment_words(data, repeated)
        if repeated < len(new_words):
            first = data["words"][0]["start_ms"]
            segments = data.get("segments")
            if segments and type(segments[0].get("start_ms")) is int:
                segments[0] = {
                    **segments[0],
                    "start_ms": min(
                        max(segments[0]["start_ms"], first), segments[0]["end_ms"]
                    ),
                }
        events = [_span_gap(*gap)] if gap else []
        return events, bool(data["text"]) or "commit_id" in data

    def _stitch_segments(
        self,
        data: dict[str, Any],
        words: list[tuple[int, str]],
        segments: list[dict[str, Any]],
        counts: list[int],
    ) -> tuple[list[CanonicalEvent], bool]:
        seam = self._seam
        assert seam is not None and seam.old_end_ms is not None
        old_end = seam.old_end_ms
        head = [word for _, word in words]
        repeated, kept, done, gap = 0, [], False, None
        # Words are cut from the front of the text, so only a leading run of
        # repeats can go: after the first kept word, all is kept.
        for segment, count in zip(segments, counts):
            if done:
                kept.append(segment)
                continue
            start, end = segment["start_ms"], segment["end_ms"]
            own = head[repeated : repeated + count]
            if start >= old_end - ROUNDING_MS:
                # Starts where every delivered word had ended: new.
                kept.append(segment)
                done = True
                continue
            if end <= old_end + ROUNDING_MS:
                # Lies inside audio whose words were delivered: a repeat.
                repeated += count
                continue
            size = self._tail_repeat(seam, own, start)
            if count and size == count:
                repeated += count
                continue
            if size:
                repeated += size
                kept.append(_trim_segment(segment, size, old_end))
            else:
                # Across the old end and no exact repeat of the delivered
                # tail: the seam is unsure over this span, however short.
                gap = (start, old_end)
                kept.append(segment)
            done = True
        if repeated:
            seam.removed = True
            entries = data.get("words")
            if isinstance(entries, list) and len(entries) == len(words):
                data["words"] = entries[repeated:]
        self._cut_words(data, words, repeated)
        if kept:
            data["segments"] = kept
        else:
            data.pop("segments", None)
        if any(segment["end_ms"] > old_end + ROUNDING_MS for segment in kept):
            self._seam = None
        elif kept:
            # Still at the seam: what this final delivers is old from now on.
            seam.tail = (seam.tail + [w for _, w in _words(data["text"])])[
                -TAIL_WORDS:
            ]
            seam.old_end_ms = max(old_end, max(s["end_ms"] for s in kept))
            seam.old_segments += [
                (
                    tuple(word for _, word in _words(segment["text"])),
                    segment["start_ms"],
                    segment["end_ms"],
                )
                for segment in kept
            ]
        events = [_span_gap(*gap)] if gap else []
        return events, bool(data["text"]) or "commit_id" in data

    @staticmethod
    def _tail_repeat(seam: _Seam, own: list[str], start: int) -> int:
        """How many leading words of a segment starting at ``start`` exactly
        repeat the delivered tail. The old words in play are those of every
        old segment that overlaps it; the repeat must cover all of them, or
        all of the segment when it is shorter. 0 when it does not."""
        in_play = sum(
            len(words) for words, _, finish in seam.old_segments if finish > start
        )
        size = min(in_play, len(own), len(seam.tail))
        if size and seam.tail[-size:] == own[:size]:
            return size
        return 0

    def _stitch_text(
        self, data: dict[str, Any], words: list[tuple[int, str]]
    ) -> tuple[list[CanonicalEvent], bool]:
        seam = self._seam
        assert seam is not None
        head = [word for _, word in words]
        if not seam.old_words:
            self._seam = None
            return [], True
        if seam.aligned:
            length = min(len(head), len(seam.old_words))
            start = 0 if head[:length] == seam.old_words[:length] else None
        else:
            start = align(seam.old_words, head)
        if start is None:
            self._seam = None
            return [self._gap_event(seam)], True
        repeated = min(len(head), len(seam.old_words) - start)
        rest = seam.old_words[start + repeated :]
        if rest and repeated == len(head):
            seam.old_words, seam.aligned = rest, True
        else:
            self._seam = None
        self._cut_words(data, words, repeated)
        _drop_segment_words(data, repeated)
        return [], bool(data["text"]) or "commit_id" in data

    @staticmethod
    def _cut_words(
        data: dict[str, Any], words: list[tuple[int, str]], count: int
    ) -> None:
        if count:
            tokens = data["text"].split()
            data["text"] = " ".join(tokens[words[count - 1][0] + 1 :])

    @staticmethod
    def _gap_event(seam: _Seam) -> CanonicalEvent:
        return CanonicalEvent(
            type="session.gap", data={"from_ms": seam.from_ms, "to_ms": seam.to_ms}
        )

    def _fail_commits(
        self, commits: list[_Commit], reason: str
    ) -> list[CanonicalEvent]:
        events = []
        for commit in commits:
            commit_id = commit.commit_id
            if commit_id is None or self._pending.pop(commit_id, None) is None:
                continue
            # Its flush still goes out, without an id nobody would answer.
            commit.commit_id = None
            events.append(
                CanonicalEvent(
                    type="transcript.final",
                    data={
                        "text": "",
                        "commit_id": commit_id,
                        "commit_failed": reason,
                    },
                )
            )
        return events

    def _fail_pending(self, reason: str) -> list[CanonicalEvent]:
        return self._fail_commits(list(self._pending.values()), reason)

    # Recovery -------------------------------------------------------------

    def _continuation_configure(self, start: int) -> tuple[str, bool]:
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
                "audio_offset_ms": self._ms(start),
            }
        return json.dumps(configure, separators=(",", ":")), asked

    def _give_up(self, code: str, last: str = "") -> GatewayError:
        message = "Relay speech stream could not be continued"
        if last and last != code:
            message += f" (last failure: {last})"
        return GatewayError(message, code=code, source="relay", retryable=False)

    async def _recover(self, cause: str) -> AsyncIterator[CanonicalEvent]:
        self._live, self._recovering = False, True
        if self._drainer is not None:
            self._drainer.cancel()
            self._drainer = None
        self._draining = False
        self._queue.clear()
        await self._close_socket(self._ws)
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
        # One budget per outage: repeated drops share it until a final on new
        # audio proves the stream works again. It runs only while there is no
        # session: time spent catching up on one does not count.
        if self._deadline is None:
            self._deadline = time.monotonic() + self._budget_left
        # The log is not trimmed while a session is being opened, so the
        # offset the dial names stays the replay's start, and a retried dial
        # sends the same bytes.
        self._pinned = True
        try:
            start = max(self._trim, self._log_start())
            key = str(uuid.uuid4())
            configure, asked = self._continuation_configure(start)
            while True:
                if self._closed:
                    return
                remaining = self._deadline - time.monotonic()
                if remaining <= 0:
                    raise self._give_up("outage_timeout", cause)
                if self._attempts >= self._limits.max_attempts:
                    raise self._give_up(cause)
                self._attempts += 1
                try:
                    websocket, ready = await asyncio.wait_for(
                        self._dial(configure, key, lambda: self._closed), remaining
                    )
                except DialCancelled:
                    return
                except (DialUnknown, *_TIMEOUTS) as error:
                    if not isinstance(error, DialUnknown) and (
                        self._deadline <= time.monotonic()
                    ):
                        # The dial hung until the budget ran out.
                        raise self._give_up("outage_timeout", cause) from None
                    # The server may have started this request: same key, same
                    # bytes, so it answers this one rather than starting another.
                    cause = "dial_unknown"
                    await self._pause()
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
                        raise self._give_up(refusal.code) from None
                    else:
                        key = str(uuid.uuid4())
                    await self._pause()
                    continue
                for event in await self._resume(
                    websocket, ready, asked=asked, start=start
                ):
                    yield event
                return
        finally:
            self._pinned = False

    async def _pause(self) -> None:
        assert self._deadline is not None
        remaining = self._deadline - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(min(self._limits.retry_delay, remaining))

    def _pieces(self, entry: _Audio) -> list[_Audio]:
        size = max(self._frame, self._position(self._limits.replay_frame_ms))
        return [
            _Audio(entry.start + offset, entry.data[offset : offset + size])
            for offset in range(0, len(entry.data), size)
        ]

    async def _resume(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        ready: dict[str, Any],
        *,
        asked: bool,
        start: int,
    ) -> list[CanonicalEvent]:
        events: list[CanonicalEvent] = []
        confirmed = asked and ready.get("continuation") is True
        # A session is reached: the outage budget stops running here, before
        # waiting for the lock or the first write. What is left of it carries
        # over to a drop before a final on new audio.
        if self._deadline is not None:
            self._budget_left = max(0.0, self._deadline - time.monotonic())
            self._deadline = None
        async with self._lock:
            # From here the log is no longer held at the dial's offset: the cap
            # applies again, however long the reader takes over these events.
            self._pinned = False
            if self._closed:
                # Cancelled while dialling: the new session is never used.
                await self._close_socket(websocket)
                return []
            # Audio between the replay point and the log's start fell out
            # while speech was heard: report it, and fail the commits whose
            # own audio lay there. Later commits keep their whole audio.
            lost = start > self._trim
            if lost:
                lost_from, lost_to = self._trim, start
                # Words up to the end of the last final were delivered: the
                # span missing is from there to where the log starts.
                gap_from = self._ms(lost_from)
                if self._final_end_ms is not None:
                    gap_from = max(gap_from, min(self._final_end_ms, self._ms(lost_to)))
                # The gap first: a consumer that stops at a failed commit has
                # already been told why. Finals that reach the log start leave
                # nothing missing.
                if gap_from < self._ms(lost_to):
                    events.append(
                        CanonicalEvent(
                            type="session.gap",
                            data={
                                "from_ms": gap_from,
                                "to_ms": self._ms(lost_to),
                            },
                        )
                    )
                events.extend(
                    self._fail_commits(
                        [
                            commit
                            for commit in self._pending.values()
                            if commit.position > lost_from
                            and commit.audio_from < lost_to
                        ],
                        "audio_fell_out_of_log",
                    )
                )
                self._trim = start
            self._commit_ids = ready.get("commit_ids") is True
            if not self._commit_ids:
                # Ids the new session will not echo could never be answered:
                # fail those commits and send their markers without ids.
                events.extend(self._fail_pending("commit_ids_not_echoed"))
            self._ws, self._live = websocket, True
            self._confirmed = confirmed
            self._session_covered = False
            self._base = 0 if confirmed else start
            self._delivered = start
            self.metadata = {
                **self.metadata,
                "request_id": ready["request_id"],
                "route": dict(ready["route"]),
            }
            # Old finals that may hold words inside the replay. Without them
            # the replay repeats nothing already delivered.
            start_ms = self._ms(start)
            boundary_ms = self._boundary_ms(start)
            window = [
                final
                for final in self._history
                if (
                    final.end_ms is not None
                    and final.end_ms > boundary_ms + ROUNDING_MS
                )
                or (final.end_ms is None and final.arrived > start)
            ]
            self._seam = None
            if window:
                timed = all(
                    final.end_ms is not None and final.segments for final in window
                )
                self._seam = _Seam(
                    from_ms=start_ms,
                    to_ms=self._ms(self._appended),
                    old_words=[word for final in window for word in final.words],
                    # The latest end, not the last final's: a final delivered
                    # at an earlier seam can end before older ones.
                    old_end_ms=max(final.end_ms or 0 for final in window)
                    if timed
                    else None,
                    tail=[word for final in self._history for word in final.words][
                        -TAIL_WORDS:
                    ],
                    old_segments=[
                        segment for final in window for segment in final.segments
                    ]
                    if timed
                    else [],
                    old_timed_words=tuple(
                        word for final in window for word in final.timed_words
                    )
                    if timed and all(final.timed_words for final in window)
                    else (),
                )
            self._queue = deque()
            for entry in self._log:
                if isinstance(entry, _Audio):
                    self._queue.extend(self._pieces(entry))
                else:
                    if (
                        entry.commit_id is not None
                        and entry.commit_id not in self._pending
                    ):
                        entry = _Commit(entry.position, None, entry.audio_from)
                    self._queue.append(entry)
            self._replay_end = self._appended
            replayed_ms = self._ms(self._appended - start)
            exact = self._exact and self._seam is None and not lost
            # The replay begins before the app hears about it. A failed first
            # write is a drop like any other: the reader finds this socket
            # closed and continues again, and no continuation is announced.
            loop = asyncio.get_running_loop()
            started, sent = loop.time(), 0
            if self._queue:
                first = self._queue.popleft()
                await self._deliver(first)
                if isinstance(first, _Audio):
                    sent = len(first.data)
            if not self._live:
                await self._close_socket(websocket)
                return events
            self._draining = True
            self._drainer = loop.create_task(
                self._drain(websocket, started, sent), name="speko.relay.stt.replay"
            )
        kind = "session.continued" if confirmed else "session.restarted"
        data: dict[str, Any] = {
            "request_id": ready["request_id"],
            "route": dict(ready["route"]),
            "replayed_ms": replayed_ms,
        }
        if confirmed:
            data["exact"] = exact
        else:
            data["from_ms"] = start_ms
        events.append(CanonicalEvent(type=kind, data=data))
        return events

    async def _drain(
        self, websocket: aiohttp.ClientWebSocketResponse, started: float, sent: int
    ) -> None:
        """Send the backlog in order, in frames, at no more than replay_speed x
        realtime, then hand over to live sends."""
        loop = asyncio.get_running_loop()
        rate = self._bytes_per_second * self._limits.replay_speed
        while True:
            delay = started + sent / rate - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._lock:
                if self._ws is not websocket or not self._live or self._closed:
                    return
                if not self._queue:
                    self._draining = self._recovering = False
                    self._drainer = None
                    if self._finishing:
                        await self._send_control({"type": "session.close"})
                    return
                entry = self._queue.popleft()
                await self._deliver(entry)
                if isinstance(entry, _Audio):
                    sent += len(entry.data)
                    self._enforce_cap()


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


def _timed_words(data: dict[str, Any]) -> list[tuple[str, int, int]] | None:
    """A final's words[] as (normalized word, start_ms, end_ms), or None when
    it has none or any entry is malformed or holds other than one word."""
    entries = data.get("words")
    if not isinstance(entries, list) or not entries:
        return None
    words = []
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("text"), str)
            or type(entry.get("start_ms")) is not int
            or type(entry.get("end_ms")) is not int
        ):
            return None
        normalized = _words(entry["text"])
        if len(normalized) != 1:
            return None
        words.append((normalized[0][1], entry["start_ms"], entry["end_ms"]))
    return words


def _span_gap(from_ms: int, to_ms: int) -> CanonicalEvent:
    return CanonicalEvent(
        type="session.gap", data={"from_ms": from_ms, "to_ms": max(from_ms, to_ms)}
    )


def _shifted(data: dict[str, Any], offset_ms: int) -> dict[str, Any]:
    """Event data with its audio times moved by offset_ms."""
    shifted = dict(data)
    for key in ("audio_start_ms", "audio_end_ms", "start_ms", "end_ms"):
        if type(shifted.get(key)) in (int, float):
            shifted[key] += offset_ms
    for key in ("segments", "words"):
        if key in shifted:
            shifted[key] = _shifted_list(shifted[key], offset_ms)
    return shifted


def _shifted_list(items: Any, offset_ms: int) -> Any:
    if isinstance(items, list):
        return [
            {
                **segment,
                **{
                    key: segment[key] + offset_ms
                    for key in ("start_ms", "end_ms")
                    if type(segment.get(key)) in (int, float)
                },
            }
            if isinstance(segment, dict)
            else segment
            for segment in items
        ]
    return items


def _trim_segment(segment: dict[str, Any], count: int, start_ms: int | None) -> dict[str, Any]:
    """``segment`` without its first ``count`` words. A timed one starts no
    earlier than ``start_ms``, where the words it lost ended."""
    words = _words(segment["text"])
    trimmed = dict(segment)
    trimmed["text"] = " ".join(segment["text"].split()[words[count - 1][0] + 1 :])
    if start_ms is not None and type(segment.get("start_ms")) is int:
        end = segment.get("end_ms")
        bound = end if type(end) is int else start_ms
        trimmed["start_ms"] = min(max(segment["start_ms"], start_ms), bound)
    return trimmed


def _drop_segment_words(data: dict[str, Any], count: int) -> None:
    """Drop the first ``count`` words of a final from its segments too, so the
    segments keep spelling out the text: a segment only partly repeated
    keeps its remaining words (and its times, which cannot be split)."""
    segments = data.get("segments")
    if not isinstance(segments, list):
        return
    kept, left = [], count
    for segment in segments:
        text = segment.get("text") if isinstance(segment, dict) else None
        words = len(_words(text)) if isinstance(text, str) else 0
        if left >= words and words:
            left -= words
            continue
        if left and words:
            segment = _trim_segment(segment, left, None)
            left = 0
        kept.append(segment)
    if kept:
        data["segments"] = kept
    else:
        data.pop("segments", None)
