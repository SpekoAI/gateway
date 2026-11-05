"""Local SDK VAD streams with captured-audio durations at paused frontiers."""

from __future__ import annotations

import asyncio
import dataclasses
import math
from collections import deque

from livekit import rtc
from livekit.agents import utils, vad

from ._livekit_vad_audio import ProbabilityVAD, VADAudio
from .client import GatewayError


class DrainingVAD(vad.VAD):
    """Drain partial local-inference/Silero windows without inventing speech time.

    Use for an AgentSession's independent VAD. Its underlying detector stays
    local and keeps its configured thresholds; inference-only padding never
    appears in outward event audio or contributes to a duration threshold.
    Each stream owns its buffers, inference tasks and hard flush boundaries.
    """

    def __init__(self, detector: vad.VAD) -> None:
        if type(detector).__module__ not in {
            "livekit.agents.inference.vad",
            "livekit.plugins.silero.vad",
        }:
            raise ValueError("DrainingVAD requires the local inference or Silero VAD")
        super().__init__(capabilities=detector.capabilities)
        self._detector = detector

    @property
    def model(self) -> str:
        return self._detector.model

    @property
    def provider(self) -> str:
        return self._detector.provider

    @property
    def min_silence_duration(self) -> float | None:
        return self._detector._opts.min_silence_duration

    def update_options(self, **kwargs: object) -> None:
        self._detector.update_options(**kwargs)

    def stream(self) -> vad.VADStream:
        return _DrainingVADStream(self)


class _DrainingVADStream(vad.VADStream):
    def __init__(self, parent: DrainingVAD) -> None:
        self._source = parent._detector
        self._sample_rate = self._queued = 0
        self._inner = None
        self._probability = None
        self._segment = self._flushes = 0
        self._idle_at = 0.0
        self._outward_speaking = False
        self._outward_last = None
        self._published: deque[tuple[vad.VADEvent, int]] = deque()
        super().__init__(parent)

    @property
    def activation_threshold(self) -> float:
        return self._inner._opts.activation_threshold

    def push_frame(self, frame: rtc.AudioFrame) -> None:
        if frame.num_channels != 1 or frame.sample_rate <= 0:
            raise GatewayError("Local speech input must be mono PCM", retryable=False)
        if self._sample_rate and frame.sample_rate != self._sample_rate:
            raise GatewayError("Local speech input changed format", retryable=False)
        if self._queued + frame.samples_per_channel > 30 * frame.sample_rate:
            raise GatewayError(
                "Local speech input exceeded its processing bound", retryable=False
            )
        super().push_frame(frame)
        self._sample_rate = frame.sample_rate
        self._queued += frame.samples_per_channel
        interval = self._source.capabilities.update_interval
        if not math.isfinite(interval) or interval <= 0:
            raise GatewayError(
                "Invalid local speech inference interval", retryable=False
            )
        self._idle_at = asyncio.get_running_loop().time() + 2 * interval

    def end_input(self) -> None:
        # Drain actual queued audio before the inner SDK's reset marker.
        self._check_input_not_ended()
        self._check_not_closed()
        self._input_ch.close()

    def flush(self) -> None:
        super().flush()
        # Fence publication synchronously at the framework's boundary. The
        # queued marker still drains accounting and resets the owned detector.
        self._flushes += 1
        if self._outward_speaking:
            # Explicitly close the delivered segment. This is the requested
            # reset, without new inferred audio or invented silence. Pending
            # real capture is classified again with fresh duration counters.
            previous = self._outward_last
            # Classification can have queued the real end before its consumer
            # reaches it. Retain that segment's canonical audio instead of a
            # newer segment's buffer. Otherwise the active buffer contains
            # only acknowledged capture, with detector padding removed.
            completed = next(
                (event for event, segment in self._published
                 if segment == self._flushes - 1
                 and event.type is vad.VADEventType.END_OF_SPEECH),
                None,
            )
            frames = completed.frames if completed is not None else [self._frame(bytes(self._speech))]
            if not frames or not any(frame.samples_per_channel for frame in frames):
                raise GatewayError("Local speech reset lost captured audio", retryable=False)
            if completed is not None:
                previous = completed
            self._emit(
                vad.VADEvent(
                    type=vad.VADEventType.END_OF_SPEECH,
                    timestamp=previous.timestamp,
                    samples_index=previous.samples_index,
                    speech_duration=previous.speech_duration,
                    silence_duration=previous.silence_duration,
                    frames=frames,
                    speaking=False,
                ),
                segment=self._flushes,
            )

    async def __anext__(self) -> vad.VADEvent:
        while True:
            event = await super().__anext__()
            original, segment = self._published.popleft()
            if event is not original:
                raise GatewayError("Local speech event order changed", retryable=False)
            if segment == self._flushes:
                self._outward_last = event
                if event.type is vad.VADEventType.START_OF_SPEECH:
                    self._outward_speaking = True
                elif event.type is vad.VADEventType.END_OF_SPEECH:
                    self._outward_speaking = False
                return event

    async def aclose(self) -> None:
        try:
            await super().aclose()
        finally:
            self._published.clear()

    def _emit(self, event: vad.VADEvent, *, segment: int | None = None) -> None:
        self._published.append((event, self._segment if segment is None else segment))
        self._event_ch.send_nowait(event)

    def _reset(self) -> None:
        self._inner = self._source.stream()
        self._probability = None
        self._audio = VADAudio()
        self._unprocessed = bytearray()
        self._history = bytearray()
        self._speech = bytearray()
        self._processed = self._last_index = 0
        self._deadline = None

    def _arm_tail(self) -> None:
        audio, detector = self._audio, self._probability
        self._deadline = (
            self._idle_at
            if detector is not None
            and self._segment == self._flushes
            and audio.real_pending
            and audio.last_padded != audio.real_fed
            and audio.padding_pending < detector.window_samples * 2
            else None
        )

    def _push(self, frame: rtc.AudioFrame, *, padding: bool = False) -> None:
        raw = bytes(frame.data)
        self._unprocessed.extend(raw)
        self._audio.push(len(raw), padding=padding)
        if self._segment == self._flushes:
            self._inner.push_frame(frame)

    def _frame(self, raw: bytes) -> rtc.AudioFrame:
        return rtc.AudioFrame(
            data=raw,
            sample_rate=self._sample_rate,
            num_channels=1,
            samples_per_channel=len(raw) // 2,
        )

    def _pad(self, *, final: bool = False) -> None:
        audio, detector = self._audio, self._probability
        if detector is None or not audio.real_pending:
            return
        size = detector.window_samples * 2
        if not final and (
            audio.last_padded == audio.real_fed or audio.padding_pending >= size
        ):
            return
        audio.last_padded = audio.real_fed
        self._push(self._frame(bytes(size)), padding=True)

    def _observe(self, event: vad.VADEvent) -> None:
        if self._segment != self._flushes:
            return  # Preserve unacknowledged real input for reclassification.
        if event.type is not vad.VADEventType.INFERENCE_DONE:
            return  # The SDK's boundary durations include inference-only padding.
        if any(
            frame.sample_rate != self._sample_rate or frame.num_channels != 1
            for frame in event.frames
        ):
            raise GatewayError(
                "Local speech detector changed audio format", retryable=False
            )
        raw = b"".join(bytes(frame.data) for frame in event.frames)
        if (
            not raw
            or self._unprocessed[: len(raw)] != raw
            or event.samples_index <= self._last_index
        ):
            raise GatewayError(
                "Local speech detector did not acknowledge ordered input",
                retryable=False,
            )
        self._last_index = event.samples_index
        del self._unprocessed[: len(raw)]
        real = self._audio.acknowledge(raw)
        count = len(real) // 2
        self._processed += count
        self._queued -= count
        if not real:
            return
        self._history.extend(real)
        del self._history[: -30 * self._sample_rate * 2]
        options = self._inner._opts
        maximum = options.max_buffered_speech + options.prefix_padding_duration
        if not math.isfinite(maximum) or maximum <= 0:
            raise GatewayError("Invalid local speech buffer duration", retryable=False)
        maximum_bytes = math.ceil(maximum * self._sample_rate) * 2
        if self._probability.speaking:
            self._speech.extend(real[: max(0, maximum_bytes - len(self._speech))])
        was_speaking = self._probability.speaking
        started_at = self._probability.started_at
        boundary = self._probability.observe(
            event,
            samples=count,
            end=self._processed,
            history=bytes(self._history),
            channels=1,
        )
        state = self._probability
        if self._segment != self._flushes:
            return
        # samples_index uses the detector rate; outward PCM keeps the source
        # rate. Inference publishes speaking/duration before its transition.
        inference_rate = getattr(options, "sample_rate", 16000)
        index = self._processed * inference_rate // self._sample_rate
        self._emit(
            dataclasses.replace(
                event,
                samples_index=index,
                timestamp=self._processed / self._sample_rate,
                frames=[self._frame(real)],
                speaking=was_speaking,
                speech_duration=max(0, self._processed - started_at) / self._sample_rate
                if was_speaking
                else 0,
                silence_duration=state.silence_samples / self._sample_rate,
                raw_accumulated_speech=state.speech_samples / self._sample_rate,
                raw_accumulated_silence=state.silence_samples / self._sample_rate,
            )
        )
        if boundary is not None:
            if boundary.type is vad.VADEventType.START_OF_SPEECH:
                self._speech = bytearray(
                    b"".join(bytes(frame.data) for frame in boundary.frames)[
                        :maximum_bytes
                    ]
                )
            else:
                boundary.frames = [self._frame(bytes(self._speech))]
                self._speech.clear()
            self._emit(
                dataclasses.replace(
                    boundary,
                    samples_index=index,
                    inference_duration=event.inference_duration,
                )
            )

    async def _main_task(self) -> None:
        incoming = observed = None
        ending = False
        self._reset()
        try:
            incoming = asyncio.create_task(self._input_ch.__anext__())
            observed = asyncio.create_task(self._inner.__anext__())
            while True:
                waiting = [task for task in (incoming, observed) if task is not None]
                timeout = (
                    max(0, self._deadline - asyncio.get_running_loop().time())
                    if self._deadline is not None
                    else None
                )
                done, _ = await asyncio.wait(
                    waiting, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    self._deadline = None
                    self._pad()
                    continue
                if observed in done:
                    try:
                        self._observe(observed.result())
                    except StopAsyncIteration:
                        if not ending or self._audio.real_pending:
                            raise GatewayError(
                                "Local speech detector ended before processing input",
                                retryable=False,
                            ) from None
                        await self._inner.aclose()
                        return
                    if not ending:
                        self._arm_tail()
                    observed = asyncio.create_task(self._inner.__anext__())
                if incoming in done:
                    try:
                        value = incoming.result()
                    except StopAsyncIteration:
                        ending = True
                    else:
                        if isinstance(value, self._FlushSentinel):
                            pending = self._audio.pending_real(bytes(self._unprocessed))
                            await utils.aio.cancel_and_wait(observed)
                            await self._inner.aclose()
                            self._reset()
                            self._segment += 1
                            if pending:
                                self._probability = ProbabilityVAD.for_stream(
                                    self._inner, self._sample_rate
                                )
                                if self._probability is None:
                                    raise GatewayError(
                                        "Unsupported local speech detector stream",
                                        retryable=False,
                                    )
                                self._push(self._frame(pending))
                            self._arm_tail()
                            observed = asyncio.create_task(self._inner.__anext__())
                        else:
                            if self._probability is None:
                                self._probability = ProbabilityVAD.for_stream(
                                    self._inner, self._sample_rate
                                )
                                if self._probability is None:
                                    raise GatewayError(
                                        "Unsupported local speech detector stream",
                                        retryable=False,
                                    )
                            self._push(value)
                            self._arm_tail()
                    if ending:
                        incoming = None
                        self._deadline = None
                        self._pad(final=True)
                        self._inner.end_input()
                    else:
                        incoming = asyncio.create_task(self._input_ch.__anext__())
        finally:
            await utils.aio.cancel_and_wait(
                *(task for task in (incoming, observed) if task is not None)
            )
            await self._inner.aclose()
            self._unprocessed.clear()
            self._history.clear()
            self._speech.clear()
