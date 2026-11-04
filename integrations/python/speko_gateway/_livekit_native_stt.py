"""VAD-ordered native STT with terminal evidence for each physical utterance."""

from __future__ import annotations

import asyncio
import math
import uuid
from bisect import bisect_left, bisect_right
from collections.abc import Awaitable, Callable
from fractions import Fraction
from typing import Any

from livekit import rtc
from livekit.agents import APIConnectionError, APIConnectOptions, stt, utils, vad

from ._livekit_bridge import LiveKitSpeechEvent, LiveKitSTTBridge, LiveKitSTTStream
from ._livekit_native_input import NativeInputTimeline
from ._livekit_native_speaker import NativeSpeechData
from ._livekit_native_transcript import NativeFinalBatch, NativeTranscript
from .client import GatewayError


class _InputChannel(utils.aio.Chan[Any]):
    def __init__(self, on_audio: Callable[[rtc.AudioFrame], None]) -> None:
        super().__init__()
        self._on_audio = on_audio

    def send_nowait(self, value: Any) -> None:
        super().send_nowait(value)
        if isinstance(value, rtc.AudioFrame):
            self._on_audio(value)


class NativeSpeechStream(stt.RecognizeStream):
    """Only processed VAD audio reaches a turn's separately admitted request.

    INFERENCE_DONE must identify contiguous, unchanged input frames before its
    boundary events. The inference sample rate can differ from the input rate.
    A logical SDK turn may continue through several physical utterances.
    Every request must deliver terminal usage before its complete text can
    participate in framework turn acceptance.
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
        continuation_delay: Callable[[str, float], Awaitable[float | None]] | None = None,
        reconsider_turn: Callable[[], None] = lambda: None,
    ) -> None:
        super().__init__(
            stt=stt_instance, conn_options=conn_options, sample_rate=sample_rate
        )
        self._bridge = bridge
        self._language = language
        self._sample_rate = sample_rate
        self._close_task: asyncio.Task[None] | None = None
        self._turn_vad = turn_vad
        self._input_timeline = input_timeline
        self._received_duration = Fraction(0)
        self._input_end: Fraction | None = None
        self._input_positions: list[Fraction] = []
        self._input_sources: list[Fraction] = []
        self._input_synthetic: list[bool] = []
        self._mark_failed = mark_failed
        self._report_open = report_open
        self._check_admission = check_admission
        self._queued_samples = Fraction(0)
        self._available_samples = 0
        # The SDK resampler retains a filter tail across input pushes. Only
        # its emitted frames can advance VAD's available-input frontier.
        self._input_ch = _InputChannel(self._audio_available)
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
        self._speaker_scope = ""
        self._finishing = False
        self._input_finished = False
        self._admission_started = False
        self._processed_samples = 0
        self._assigned_until = 0
        self._request_start_sample = 0
        self._wait_for_turn_commit = wait_for_turn_commit
        self._continuation_delay = continuation_delay
        self._reconsider_turn = reconsider_turn
        self._vad_speaking = False
        self._last_speech_end: int | None = None
        self._pending_transcript = False
        self._classifying_boundary = False
        self._awaiting_new_turn = False
        self._turn_commit: asyncio.Future[None] | None = None
        self._turn_text = ""
        self._canonical_turn: NativeTranscript | None = None
        self._final_batches: dict[str, NativeFinalBatch] = {}
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
        if self._input_timeline is not None:
            try:
                occurrence = self._input_timeline.occurrence_of(frame)
                start, synthetic = occurrence.start, occurrence.synthetic
                if not synthetic and self._input_end is not None and start < self._input_end:
                    raise GatewayError("Native STT input moved backwards", retryable=False)
            except GatewayError as error:
                if not self._fatal.done():
                    self._fatal.set_exception(error)
                self._input_ch.close()
                return
            previous = (
                self._input_sources[-1] + (
                    Fraction(0) if self._input_synthetic[-1]
                    else self._received_duration - self._input_positions[-1]
                ) if self._input_sources else None
            )
            if previous != start or self._input_synthetic[-1] != synthetic:
                self._input_positions.append(self._received_duration)
                self._input_sources.append(start)
                self._input_synthetic.append(synthetic)
            if not synthetic:
                self._input_end = start + Fraction(frame.samples_per_channel, frame.sample_rate)
        super().push_frame(frame)
        self._queued_samples = queued
        self._received_duration += Fraction(frame.samples_per_channel, frame.sample_rate)

    def _source_time(self, local: Fraction, *, end: bool = False) -> float:
        if self._input_timeline is None or not self._input_positions:
            return float(local) + getattr(self, "start_time_offset", 0.0)
        # At a filtered gap, an end belongs to the preceding frame and a
        # start to the following one. Do not fabricate PCM for omitted input.
        search = bisect_left if end else bisect_right
        index = max(0, search(self._input_positions, local) - 1)
        return float(self._input_sources[index] + (
            Fraction(0) if self._input_synthetic[index]
            else local - self._input_positions[index]
        ))

    def _frame(self, raw: bytes) -> rtc.AudioFrame:
        assert self._channels is not None
        return rtc.AudioFrame(
            data=raw,
            sample_rate=self._sample_rate,
            num_channels=self._channels,
            samples_per_channel=len(raw) // (2 * self._channels),
        )

    def _audio_available(self, frame: rtc.AudioFrame) -> None:
        self._available_samples += frame.samples_per_channel

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

    async def aclose(self) -> None:
        # A live model swap and STT.aclose can both close the retained stream.
        # Cancel its pump once, then let every closer join the same cleanup.
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                super().aclose(), name="speko.native.stt.close"
            )
        await asyncio.shield(self._close_task)
        for batch in list(self._final_batches.values()):
            batch.release()

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
                self._available_samples -= frame.samples_per_channel
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
                self._available_samples -= len(raw) // (2 * self._channels)
                self._history.extend(raw)
                del self._history[: -30 * self._sample_rate * 2 * self._channels]
                self._last_vad_index = event.samples_index
                self._processed_samples += len(raw) // (2 * self._channels)
                self._vad_speaking = event.speaking
                if self._gateway_stream is not None:
                    await self._send_audio(raw)
                    self._assigned_until = self._processed_samples
                self._reconsider_turn()
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
                if (
                    not math.isfinite(event.speech_duration)
                    or event.speech_duration < 0
                ):
                    raise GatewayError(
                        "Local speech detector changed its timeline", retryable=False
                    )
                if self._turn_commit is not None and not self._turn_commit.done():
                    onset = self._processed_samples - round(
                        event.speech_duration * self._sample_rate
                    )
                    gap = (
                        max(0, (onset - self._last_speech_end) / self._sample_rate)
                        if self._last_speech_end is not None
                        else 0
                    )
                    self._classifying_boundary = True
                    try:
                        delay = (
                            await asyncio.wait_for(
                                self._continuation_delay(self._turn_text, gap),
                                self._conn_options.timeout,
                            )
                            if self._continuation_delay is not None
                            else None
                        )
                    finally:
                        self._classifying_boundary = False
                    if delay is None or gap >= delay:
                        self._awaiting_new_turn = True
                        self._reconsider_turn()
                        await asyncio.wait_for(
                            asyncio.shield(self._turn_commit),
                            self._conn_options.timeout,
                        )
                if self._turn_commit is not None and self._turn_commit.done():
                    self._turn_commit = None
                    self._turn_text = ""
                    self._canonical_turn = None
                self._awaiting_new_turn = False
                self._pending_transcript = True
                self._vad_speaking = True
                self._event_ch.send_nowait(
                    stt.SpeechEvent(type=stt.SpeechEventType.START_OF_SPEECH)
                )
                await self._send_audio(raw)
                self._assigned_until = self._processed_samples
            elif event.type == vad.VADEventType.END_OF_SPEECH:
                self._vad_speaking = False
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
                    self._last_speech_end = speech_end
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
        if self._turn_commit is not None:
            await asyncio.wait_for(
                asyncio.shield(self._turn_commit), self._conn_options.timeout
            )

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
        # Provider labels belong to this physical request. A provider request
        # ID may be reused; generate an independent full-width namespace.
        self._speaker_scope = uuid.uuid4().hex
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

    def _emit(self, event: LiveKitSpeechEvent, kind: stt.SpeechEventType, batch: NativeFinalBatch | None = None, index: int = 0) -> None:
        offset = Fraction(self._request_start_sample, self._sample_rate)
        data = NativeSpeechData(
            language=event.language or self._language,
            text=event.text,
            speaker_id=f"{self._speaker_scope}:{event.speaker_id}" if event.speaker_id is not None else None,
            start_time=self._source_time(offset + Fraction(str(event.start_time)))
            if event.start_time is not None
            else 0.0,
            end_time=self._source_time(offset + Fraction(str(event.end_time)), end=True)
            if event.end_time is not None
            else 0.0,
        )
        data.canonical_text = event.canonical_text
        data.provider_speaker_id = event.speaker_id
        data._native_speaker_scope = self._speaker_scope
        if event.start_time is not None and event.end_time is not None:
            data._native_input_times = (
                float(offset) + event.start_time,
                float(offset) + event.end_time,
            )
        if batch is not None:
            data._native_final = (batch.token, index)
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=kind,
                request_id=self._request_id,
                alternatives=[data],
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

    def permits_framework_commit(self, transcript: str) -> bool:
        return not self._wait_for_turn_commit or self._task.done() or (
            not self._pending_transcript
            and not self._classifying_boundary
            and (self._awaiting_new_turn or (not self._vad_speaking and self._available_samples == 0))
            and (not self._turn_text or " ".join(transcript.split()) == self._turn_text)
        )

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
        previous = self._canonical_turn if self._turn_commit is not None and not self._turn_commit.done() else None
        canonical = [event.text if event.canonical_text is None else event.canonical_text for event in self._finals]
        text = " ".join(text for text in canonical if text)
        self._canonical_turn = previous if previous is not None and not text else NativeTranscript(previous, text)
        canonical_text = self._canonical_turn.render()
        if len(canonical_text) > 1 << 20:
            raise GatewayError("Native speech transcript exceeded its bound", retryable=False)
        self._turn_text = " ".join(canonical_text.split())
        if self._wait_for_turn_commit and self._turn_text and (self._turn_commit is None or self._turn_commit.done()):
            self._turn_commit = asyncio.get_running_loop().create_future()
        self._pending_transcript = False
        batch = NativeFinalBatch(self, self._canonical_turn, tuple(event.text for event in self._finals)) if self._wait_for_turn_commit else None
        for index, event in enumerate(self._finals):
            self._emit(event, stt.SpeechEventType.FINAL_TRANSCRIPT, batch, index)
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=stt.SpeechEventType.END_OF_SPEECH,
                request_id=self._request_id,
                alternatives=[
                    stt.SpeechData(
                        language=self._language,
                        text="",
                        end_time=self._source_time(Fraction(
                            self._processed_samples
                            if speech_end is None
                            else speech_end,
                            self._sample_rate,
                        ), end=True),
                    )
                ],
            )
        )
        self._gateway_stream = None
        self._receiver = None
        keep = max(0, bisect_right(self._input_positions, Fraction(self._assigned_until, self._sample_rate)) - 1)
        if keep:
            del self._input_positions[:keep]
            del self._input_sources[:keep]
            del self._input_synthetic[:keep]
        await stream.aclose()
        self._admission_started = False
        self._reconsider_turn()
