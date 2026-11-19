"""Keep inference-only padding outside captured audio and speech durations."""

from __future__ import annotations

import math
from collections import deque
from typing import Any

from livekit import rtc
from livekit.agents import vad

from .client import GatewayError


class VADAudio:
    """An ordered byte ledger; only real spans can reach a provider."""

    def __init__(self) -> None:
        self.spans: deque[tuple[int, bool]] = deque()
        self.real_pending = self.padding_pending = self.real_fed = 0
        self.last_padded = -1

    def push(self, size: int, *, padding: bool = False) -> None:
        added = size
        if self.spans and self.spans[-1][1] is padding:
            previous, _ = self.spans.pop()
            size += previous
        self.spans.append((size, padding))
        if padding:
            self.padding_pending += added
        else:
            self.real_pending += added
            self.real_fed += added

    def acknowledge(self, raw: bytes) -> bytes:
        offset, kept = 0, []
        while offset < len(raw):
            if not self.spans:
                raise GatewayError("Local speech acknowledgement exceeded input", retryable=False)
            size, padding = self.spans.popleft()
            count = min(size, len(raw) - offset)
            if padding:
                self.padding_pending -= count
            else:
                self.real_pending -= count
                kept.append(raw[offset:offset + count])
            if size > count:
                self.spans.appendleft((size - count, padding))
            offset += count
        return b"".join(kept)

    def pending_real(self, raw: bytes) -> bytes:
        """Recover unacknowledged capture for a fresh detector after flush."""
        offset, kept = 0, []
        for size, padding in self.spans:
            if not padding:
                kept.append(raw[offset:offset + size])
            offset += size
        if offset != len(raw):
            raise GatewayError("Local speech pending input changed", retryable=False)
        return b"".join(kept)


class ProbabilityVAD:
    """Apply the SDK detector's thresholds using captured sample durations.

    Padding completes an inference window but has zero captured duration.
    The SDK's public boundary counters count that padding, so these owned
    counters retain its configured hysteresis/minimums using real audio only.
    Other VAD implementations keep their supplied ordered boundary events.
    """

    @classmethod
    def for_stream(cls, stream: Any, sample_rate: int) -> ProbabilityVAD | None:
        if type(stream).__module__ not in {
            "livekit.agents.inference.vad", "livekit.plugins.silero.vad",
        }:
            return None
        return cls(stream, sample_rate)

    def __init__(self, stream: Any, sample_rate: int) -> None:
        self.stream, self.sample_rate = stream, sample_rate
        interval = stream._vad.capabilities.update_interval
        if not math.isfinite(interval) or interval <= 0:
            raise GatewayError("Invalid local speech inference interval", retryable=False)
        self.window_samples = max(1, math.ceil(interval * sample_rate))
        self.interval = interval
        self.speaking = False
        self.speech_samples = self.silence_samples = 0
        self.started_at = 0
        self._options()

    @property
    def onset_pending(self) -> bool:
        return not self.speaking and self.speech_samples > 0

    def _options(self) -> tuple[float, float, int, int, int]:
        options = self.stream._opts
        activation = options.activation_threshold
        deactivation = getattr(options, "deactivation_threshold", activation)
        values = (activation, deactivation, options.min_speech_duration, options.min_silence_duration, options.prefix_padding_duration)
        if any(not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0 for value in values) or activation > 1 or deactivation > 1:
            raise GatewayError("Invalid local speech detector options", retryable=False)
        return activation, deactivation, *(math.ceil(value * self.sample_rate) for value in values[2:])

    def observe(self, event: vad.VADEvent, *, samples: int, end: int, history: bytes, channels: int) -> vad.VADEvent | None:
        activation, deactivation, minimum_speech, minimum_silence, padding = self._options()
        probability = event.probability
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise GatewayError("Invalid local speech probability", retryable=False)
        if not samples:
            return None
        kind = None
        if probability >= activation or (self.speaking and probability > deactivation):
            self.speech_samples += samples
            self.silence_samples = 0
            if not self.speaking and self.speech_samples >= minimum_speech:
                self.speaking = True
                self.started_at = end - self.speech_samples
                kind = vad.VADEventType.START_OF_SPEECH
        else:
            self.silence_samples += samples
            self.speech_samples = 0
            if self.speaking and self.silence_samples >= minimum_silence:
                self.speaking = False
                kind = vad.VADEventType.END_OF_SPEECH
        if kind is None:
            return None
        frames = []
        if kind is vad.VADEventType.START_OF_SPEECH:
            count = end - max(0, self.started_at - padding)
            stride = 2 * channels
            if count * stride > len(history):
                raise GatewayError("Local speech prefix exceeded retained input", retryable=False)
            frames = [rtc.AudioFrame(data=history[-count * stride:], sample_rate=self.sample_rate, num_channels=channels, samples_per_channel=count)]
        return vad.VADEvent(
            type=kind, samples_index=event.samples_index,
            timestamp=end / self.sample_rate,
            speech_duration=(end - self.started_at - self.silence_samples) / self.sample_rate,
            silence_duration=self.silence_samples / self.sample_rate,
            frames=frames, speaking=self.speaking,
        )
