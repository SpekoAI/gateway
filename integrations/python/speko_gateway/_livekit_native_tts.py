"""One long-lived native TTS stream shared by every reply of a LiveKit ``TTS``.

The relay protocol already takes many utterances per stream: each
``input.commit`` is one utterance and only ``session.close`` ends the stream.
A reply used to open its own admitted stream; this keeps one open, lazily, and
lends it to one reply at a time.

- Open at the first reply's text. Reuse it for later replies while the options
  are unchanged.
- The relay renews the stream's lease itself. A stream the relay or the
  provider ended, or that failed while idle, is dropped and the next reply
  opens a new one. Nothing is ever replayed: a reply that meets a dead stream
  fails like any other dropped request.
- An idle stream closes after ``_IDLE_CLOSE`` seconds, so a quiet call does not
  hold its reservation, and the next reply reopens it.
- Past ``max_input_characters`` the stream rotates between utterances.
- A reply that is interrupted sends ``input.cancel`` for its own utterance and
  leaves the stream open. The relay does not acknowledge a cancel, so events
  of the cancelled utterance are dropped until the next ``utterance.started``.
  A cancel before the first audio cannot be told apart from that next
  utterance's start, so that stream is closed instead.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from ._livekit_bridge import LiveKitTTSBridge, LiveKitTTSEvent, LiveKitTTSStream
from .client import GatewayError

_logger = logging.getLogger(__name__)

# Under the default idle timeouts of the NAT gateways on Azure (4 minutes) and
# AWS (350 s), which drop an idle connection without telling either end.
_IDLE_CLOSE = 180.0
# How long a closing stream waits for the relay's terminal frame.
_CLOSE_GRACE = 5.0

_CLOSE_ERRORS = (GatewayError, OSError, asyncio.TimeoutError, aiohttp.ClientError)
_END = object()


def _retrieve(task: asyncio.Task[Any]) -> None:
    if not task.cancelled() and task.exception() is not None:
        _logger.debug("Speko Gateway TTS stream task failed", exc_info=task.exception())


class _Conn:
    """One admitted stream and the one task that reads its events."""

    def __init__(self, stream: LiveKitTTSStream, options: dict[str, Any]) -> None:
        self.stream, self.options = stream, options
        self.characters = 0
        # idle | committed (no audio yet) | started (audio of the utterance)
        self.phase = "idle"
        self.appended = False
        self.discard = False
        self.dead = False
        self.sink: asyncio.Queue[Any] | None = None
        self.idle_timer: asyncio.TimerHandle | None = None
        self.reader = asyncio.create_task(
            self._read(stream.events()), name="speko.livekit.tts.read"
        )
        self.reader.add_done_callback(_retrieve)

    async def _read(self, events: AsyncIterator[LiveKitTTSEvent]) -> None:
        try:
            async for event in events:
                if event.type == "audio.started":
                    self.discard = False
                    self.phase = "started"
                elif self.discard:
                    continue
                elif event.type == "audio.done":
                    self.phase = "idle"
                if self.sink is not None:
                    self.sink.put_nowait(event)
            outcome: Any = GatewayError(
                "Relay speech ended before text completed", retryable=False
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 -- the reply gets the stream's failure
            outcome = error
        self.dead = True
        if self.sink is not None:
            self.sink.put_nowait(outcome)

    async def close(self, *, graceful: bool) -> None:
        if self.idle_timer is not None:
            self.idle_timer.cancel()
        if graceful and not self.dead:
            try:
                await self.stream.finish()
                await asyncio.wait_for(asyncio.shield(self.reader), _CLOSE_GRACE)
            except _CLOSE_ERRORS:
                _logger.debug("Speko Gateway TTS stream did not close cleanly")
        try:
            await self.stream.aclose()
        except _CLOSE_ERRORS:
            _logger.debug("Speko Gateway TTS stream did not close cleanly")
        self.reader.cancel()
        await asyncio.gather(self.reader, return_exceptions=True)


class NativeTTSReply:
    """What one reply sees of the shared stream; it speaks like a ``LiveKitTTSStream``."""

    def __init__(self, shared: NativeTTS, conn: _Conn) -> None:
        self._shared, self._conn = shared, conn
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        conn.sink = self._queue
        self._released = False

    @property
    def session_id(self) -> str:
        return self._conn.stream.session_id

    @property
    def attempt_id(self) -> str:
        return self._conn.stream.attempt_id

    async def append_text(self, text: str) -> None:
        conn = self._conn
        limit = int(conn.options.get("max_input_characters") or 0)
        if (
            limit
            and conn.characters + len(text) > limit
            and conn.characters
            and conn.phase == "idle"
            and not conn.appended
        ):
            conn = self._conn = await self._shared._rotate(conn, self._queue)
        conn.appended = True
        conn.characters += len(text)
        await conn.stream.append_text(text)

    async def commit(self) -> None:
        conn = self._conn
        conn.phase, conn.appended = "committed", False
        await conn.stream.commit()

    async def finish(self) -> None:
        """End this reply's events. The stream stays open for the next one."""
        self._queue.put_nowait(_END)

    async def events(self) -> AsyncIterator[LiveKitTTSEvent]:
        while True:
            item = await self._queue.get()
            if item is _END:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._shared._release(self._conn)


class NativeTTS:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._lock = asyncio.Lock()
        self._conn: _Conn | None = None
        self._cancel: asyncio.Task[None] | None = None
        self._closing: set[asyncio.Task[None]] = set()
        self._closed = False

    async def acquire(self, options: dict[str, Any]) -> NativeTTSReply:
        """Wait for the previous reply to let go, then take the stream, opening it if needed."""
        await self._lock.acquire()
        try:
            if self._closed:
                raise GatewayError("Relay speech session is closed", retryable=False)
            if self._cancel is not None:
                await asyncio.wait({self._cancel})
            conn = self._conn
            if conn is not None and (conn.dead or conn.options != options):
                self._retire(conn)
                conn = None
            if conn is None:
                conn = self._conn = await self._open(options)
            if conn.idle_timer is not None:
                conn.idle_timer.cancel()
            return NativeTTSReply(self, conn)
        except BaseException:
            self._lock.release()
            raise

    async def _open(self, options: dict[str, Any]) -> _Conn:
        stream = await LiveKitTTSBridge(self._client, **options).start()
        return _Conn(stream, options)

    async def _rotate(self, old: _Conn, sink: asyncio.Queue[Any]) -> _Conn:
        self._retire(old)
        conn = self._conn = await self._open(old.options)
        conn.sink = sink
        return conn

    def _release(self, conn: _Conn) -> None:
        try:
            if conn.dead or self._closed:
                return
            if conn.appended or conn.phase != "idle":
                if conn.phase == "committed":
                    self._retire(conn, graceful=False)
                    return
                conn.discard = conn.phase == "started"
                conn.phase, conn.appended = "idle", False
                self._cancel = asyncio.create_task(self._send_cancel(conn))
                self._cancel.add_done_callback(_retrieve)
            conn.sink = None
            conn.idle_timer = asyncio.get_running_loop().call_later(
                _IDLE_CLOSE, self._retire_idle, conn
            )
        finally:
            conn.sink = None
            self._lock.release()

    async def _send_cancel(self, conn: _Conn) -> None:
        try:
            await conn.stream.cancel()
        except _CLOSE_ERRORS:
            conn.dead = True

    def _retire_idle(self, conn: _Conn) -> None:
        if conn is self._conn and not self._lock.locked():
            self._retire(conn)

    def _retire(self, conn: _Conn, *, graceful: bool = True) -> None:
        if self._conn is conn:
            self._conn = None
        conn.sink = None
        task = asyncio.create_task(conn.close(graceful=graceful))
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)
        task.add_done_callback(_retrieve)

    async def aclose(self) -> None:
        self._closed = True
        if self._cancel is not None:
            await asyncio.wait({self._cancel})
        if self._conn is not None:
            self._retire(self._conn)
        if self._closing:
            await asyncio.wait(self._closing)
