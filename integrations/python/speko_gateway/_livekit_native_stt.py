"""VAD-ordered native STT with a terminal request boundary for each turn."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from livekit import rtc
from livekit.agents import APIConnectionError, APIConnectOptions, stt, utils, vad

from ._livekit_bridge import LiveKitSpeechEvent, LiveKitSTTBridge, LiveKitSTTStream
from .client import GatewayError


@dataclass
class NativeInputTimeline:
    """Exact accepted input duration for one SDK recognition pipeline."""

    position: Fraction = Fraction(0)


class NativeSpeechStream(stt.RecognizeStream):
    """Only processed VAD audio reaches a turn's separately admitted request.

    INFERENCE_DONE must identify contiguous, unchanged input frames before its
    boundary events. The inference sample rate can differ from the input rate.
    A turn ends only after its request has delivered terminal usage; a final
    segment alone cannot authorize LiveKit to commit the turn.
    """

    def __init__(
        self,
        *,
        stt_instance: stt.STT,
        bridge: LiveKitSTTBridge,
        language: str,
        conn_options: APIConnectOptions,
        sample_rate: int,
        turn_vad: vad.VAD | None,
        mark_failed: Callable[[], None],
        report_open: Callable[[LiveKitSTTStream], None],
        check_admission: Callable[[], None],
        wait_for_turn_commit: bool = False,
        input_timeline: NativeInputTimeline | None = None,
    ) -> None:
        super().__init__(
            stt=stt_instance, conn_options=conn_options, sample_rate=sample_rate
        )
        self._bridge = bridge
        self._language = language
        self._sample_rate = sample_rate
        self._turn_vad = turn_vad
        self._input_timeline = input_timeline
        self._input_start = input_timeline.position if input_timeline is not None else None
        self._mark_failed = mark_failed
        self._report_open = report_open
        self._check_admission = check_admission
        self._queued_samples = Fraction(0)
        self._input_failure = False
        self._unprocessed = bytearray()
        self._history = bytearray()
        self._last_vad_index = -1
        self._channels: int | None = None
        self._gateway_stream: LiveKitSTTStream | None = None
        self._receiver: asyncio.Task[None] | None = None
        self._vad_stream: vad.VADStream | None = None
        self._finals: list[LiveKitSpeechEvent] = []
        self._final_chars = 0
        self._request_id = utils.shortuuid()
        self._finishing = False
        self._input_finished = False
        self._admission_started = False
        self._processed_samples = 0
        self._assigned_until = 0
        self._request_start_sample = 0
        self._wait_for_turn_commit = wait_for_turn_commit
        self._turn_commit: asyncio.Future[None] | None = None
        self._turn_text = ""
        self._fatal: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def push_frame(self, frame: rtc.AudioFrame) -> None:
        self._check_input_not_ended()
        self._check_not_closed()
        queued = self._queued_samples + Fraction(
            frame.samples_per_channel * self._sample_rate, frame.sample_rate
        )
        over_bound = (
            queued > 30 * self._sample_rate
            if self._turn_vad is not None
            else queued * frame.num_channels * 2 > 128 << 20
        )
        if over_bound:
            # Bound both the SDK input channel and VAD output waiting behind
            # admission/teardown. The task emits one content-free failure.
            self._input_failure = True
            self._input_ch.close()
            return
        super().push_frame(frame)
        self._queued_samples = queued
        if self._input_timeline is not None:
            self._input_timeline.position += Fraction(
                frame.samples_per_channel, frame.sample_rate
            )

    def _stream_offset(self) -> float:
        if self._input_start is not None:
            return float(self._input_start)
        return getattr(self, "start_time_offset", 0.0)

    def _frame(self, raw: bytes) -> rtc.AudioFrame:
        assert self._channels is not None
        return rtc.AudioFrame(
            data=raw,
            sample_rate=self._sample_rate,
            num_channels=self._channels,
            samples_per_channel=len(raw) // (2 * self._channels),
        )

    def _audio(self, frames: list[rtc.AudioFrame]) -> bytes:
        if any(
            frame.sample_rate != self._sample_rate
            or frame.num_channels != self._channels
            for frame in frames
        ):
            raise GatewayError(
                "Local speech detector changed the audio format", retryable=False
            )
        return b"".join(bytes(frame.data) for frame in frames)

    async def _run(self) -> None:
        tasks: list[asyncio.Task[Any]] = []
        work: asyncio.Future[Any] | None = None
        try:
            if self._turn_vad is not None:
                self._vad_stream = self._turn_vad.stream()
            feed = asyncio.create_task(self._feed(), name="speko.native.stt.input")
            tasks.append(feed)
            if self._vad_stream is not None:
                tasks.append(
                    asyncio.create_task(
                        self._consume_vad(), name="speko.native.stt.vad"
                    )
                )
            work = asyncio.gather(*tasks)
            done, _ = await asyncio.wait(
                [work, self._fatal], return_when=asyncio.FIRST_COMPLETED
            )
            if self._fatal in done:
                self._fatal.result()
            await work
        except asyncio.CancelledError:
            if self._admission_started:
                self._mark_failed()
            raise
        except Exception:  # noqa: BLE001 -- supplied VAD/provider errors may contain content
            self._mark_failed()
            raise APIConnectionError(
                "Native Speko STT requires a control-plane recovery decision",
                retryable=False,
            ) from None
        finally:
            await utils.aio.cancel_and_wait(*tasks)
            if work is not None:
                await asyncio.gather(work, return_exceptions=True)
            if self._receiver is not None:
                await utils.aio.cancel_and_wait(self._receiver)
            if self._gateway_stream is not None:
                await self._gateway_stream.aclose()
            if self._vad_stream is not None:
                await self._vad_stream.aclose()
            if self._fatal.done() and not self._fatal.cancelled():
                self._fatal.exception()
            else:
                self._fatal.cancel()
            self._unprocessed.clear()
            self._history.clear()
            self._finals.clear()
            if self._turn_commit is not None:
                self._turn_commit.cancel()
            self._turn_text = ""

    async def _feed(self) -> None:
        async for frame in self._input_ch:
            if self._input_failure:
                raise GatewayError(
                    "Native speech input exceeded its processing bound", retryable=False
                )
            if isinstance(frame, self._FlushSentinel):
                # Native manual_flush is not advertised. end_input closes the
                # whole input below, without resetting a VAD ahead of its work.
                continue
            if self._channels is None:
                self._channels = frame.num_channels
            if frame.num_channels != self._channels:
                raise GatewayError(
                    "Native speech input changed format", retryable=False
                )
            if self._vad_stream is None:
                await self._send_audio(bytes(frame.data))
                self._processed_samples += frame.samples_per_channel
                self._assigned_until = self._processed_samples
                self._queued_samples -= frame.samples_per_channel
            else:
                self._unprocessed.extend(frame.data.cast("B"))
                self._vad_stream.push_frame(frame)
        if self._input_failure:
            raise GatewayError(
                "Native speech input exceeded its processing bound", retryable=False
            )
        self._input_finished = True
        if self._vad_stream is not None:
            self._vad_stream.end_input()
        elif self._gateway_stream is not None:
            await self._finish_turn()

    async def _consume_vad(self) -> None:
        assert self._vad_stream is not None
        async for event in self._vad_stream:
            if event.samples_index < self._last_vad_index:
                raise GatewayError(
                    "Local speech detector moved backwards", retryable=False
                )
            if event.type == vad.VADEventType.INFERENCE_DONE:
                raw = self._audio(event.frames)
                if (
                    not raw
                    or self._unprocessed[: len(raw)] != raw
                    or event.samples_index <= self._last_vad_index
                ):
                    raise GatewayError(
                        "Local speech detector did not acknowledge ordered input",
                        retryable=False,
                    )
                del self._unprocessed[: len(raw)]
                self._queued_samples -= Fraction(len(raw), 2 * self._channels)
                self._history.extend(raw)
                del self._history[: -30 * self._sample_rate * 2 * self._channels]
                self._last_vad_index = event.samples_index
                self._processed_samples += len(raw) // (2 * self._channels)
                if self._gateway_stream is not None:
                    await self._send_audio(raw)
                    self._assigned_until = self._processed_samples
            elif event.type == vad.VADEventType.START_OF_SPEECH:
                raw = self._audio(event.frames)
                if (
                    self._gateway_stream is not None
                    or not raw
                    or not self._history.endswith(raw)
                    or event.samples_index != self._last_vad_index
                ):
                    raise GatewayError(
                        "Local speech boundary did not match processed input",
                        retryable=False,
                    )
                stride = 2 * self._channels
                prefix_start = self._processed_samples - len(raw) // stride
                request_start = max(prefix_start, self._assigned_until)
                raw = raw[(request_start - prefix_start) * stride :]
                if not raw:
                    raise GatewayError(
                        "Local speech boundary reused completed audio", retryable=False
                    )
                self._request_start_sample = request_start
                self._event_ch.send_nowait(
                    stt.SpeechEvent(type=stt.SpeechEventType.START_OF_SPEECH)
                )
                await self._send_audio(raw)
                self._assigned_until = self._processed_samples
            elif event.type == vad.VADEventType.END_OF_SPEECH:
                if event.samples_index != self._last_vad_index:
                    raise GatewayError(
                        "Local speech boundary overtook processed input",
                        retryable=False,
                    )
                if self._gateway_stream is not None:
                    if (
                        not math.isfinite(event.silence_duration)
                        or event.silence_duration < 0
                    ):
                        raise GatewayError(
                            "Local speech detector changed its timeline",
                            retryable=False,
                        )
                    speech_end = self._processed_samples - round(
                        event.silence_duration * self._sample_rate
                    )
                    if speech_end < self._request_start_sample:
                        raise GatewayError(
                            "Local speech detector changed its timeline",
                            retryable=False,
                        )
                    await self._finish_turn(speech_end)
        if not self._input_finished:
            raise GatewayError(
                "Local speech detector ended before input", retryable=False
            )
        if self._gateway_stream is not None:
            # A short final inference window may remain when input closes.
            # No later utterance can arrive beyond this explicit input fence.
            if self._unprocessed:
                await self._send_audio(bytes(self._unprocessed))
                self._processed_samples += len(self._unprocessed) // (
                    2 * self._channels
                )
                self._assigned_until = self._processed_samples
                self._unprocessed.clear()
            await self._finish_turn()

    async def _send_audio(self, raw: bytes) -> None:
        assert self._channels is not None
        stride = 2 * self._channels
        limit = (2 << 20) // stride * stride
        for offset in range(0, len(raw), limit):
            chunk = raw[offset : offset + limit]
            if self._gateway_stream is None:
                await self._open(chunk)
            else:
                await self._gateway_stream.push_frame(self._frame(chunk))

    async def _open(self, raw: bytes) -> None:
        self._check_admission()
        self._finishing = False
        self._finals = []
        self._final_chars = 0
        # Cancellation after configure may already have spent admission even
        # when ready has not returned a stream object to this caller.
        self._admission_started = True
        self._gateway_stream = await self._bridge.start(self._frame(raw))
        self._report_open(self._gateway_stream)
        self._request_id = self._gateway_stream.request_id or utils.shortuuid()
        self._receiver = asyncio.create_task(
            self._receive(self._gateway_stream), name="speko.native.stt.events"
        )
        await self._gateway_stream.push_frame(self._frame(raw))

    async def _receive(self, stream: LiveKitSTTStream) -> None:
        try:
            async for event in stream.events():
                if event.type == "transcript.final":
                    self._final_chars += len(event.text)
                    if self._final_chars > 1 << 20:
                        raise GatewayError(
                            "Native speech transcript exceeded its bound",
                            retryable=False,
                        )
                    self._finals.append(event)
                elif event.type == "transcript.delta":
                    self._emit(event, stt.SpeechEventType.INTERIM_TRANSCRIPT)
            if not self._finishing:
                raise GatewayError(
                    "Native speech ended before its input boundary", retryable=False
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            if not self._fatal.done():
                self._fatal.set_exception(
                    GatewayError("Native speech execution failed", retryable=False)
                )
            raise

    def _emit(self, event: LiveKitSpeechEvent, kind: stt.SpeechEventType) -> None:
        offset = self._request_start_sample / self._sample_rate + self._stream_offset()
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=kind,
                request_id=self._request_id,
                alternatives=[
                    stt.SpeechData(
                        language=event.language or self._language,
                        text=event.text,
                        start_time=event.start_time + offset
                        if event.start_time is not None
                        else 0.0,
                        end_time=event.end_time + offset
                        if event.end_time is not None
                        else 0.0,
                    )
                ],
            )
        )

    def acknowledge_turn(self, transcript: str) -> None:
        """The owned activity accepted this complete framework user turn."""
        if (
            self._turn_commit is not None
            and not self._turn_commit.done()
            and " ".join(transcript.split()) == self._turn_text
        ):
            self._turn_commit.set_result(None)

    async def _finish_turn(self, speech_end: int | None = None) -> None:
        assert self._gateway_stream is not None and self._receiver is not None
        stream, receiver = self._gateway_stream, self._receiver
        self._finishing = True
        await stream.flush()
        await stream.finish()
        await asyncio.wait_for(asyncio.shield(receiver), self._conn_options.timeout)
        if self._wait_for_turn_commit:
            # Some SDK versions retain this stream across Agent handoffs.
            # Bind the actual current activity again before publishing finals,
            # including a handoff while the physical request was in flight.
            self._check_admission()
        self._turn_text = " ".join(
            " ".join(event.text for event in self._finals).split()
        )
        if self._wait_for_turn_commit and self._turn_text:
            self._turn_commit = asyncio.get_running_loop().create_future()
        for event in self._finals:
            self._emit(event, stt.SpeechEventType.FINAL_TRANSCRIPT)
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=stt.SpeechEventType.END_OF_SPEECH,
                request_id=self._request_id,
                alternatives=[
                    stt.SpeechData(
                        language=self._language,
                        text="",
                        end_time=(
                            self._processed_samples
                            if speech_end is None
                            else speech_end
                        )
                        / self._sample_rate
                        + self._stream_offset(),
                    )
                ],
            )
        )
        self._gateway_stream = None
        self._receiver = None
        await stream.aclose()
        self._admission_started = False
        if self._turn_commit is not None:
            # Await the framework's real endpointing decision rather than
            # replaying an elapsed silence or guessing its configured delay.
            await asyncio.wait_for(
                asyncio.shield(self._turn_commit), self._conn_options.timeout
            )
            self._turn_commit = None
        self._turn_text = ""
