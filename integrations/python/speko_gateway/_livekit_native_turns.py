"""Observe the owned SDK's endpointing decisions and resume deferred turns."""

from __future__ import annotations

import asyncio
import inspect
import weakref
from dataclasses import dataclass
from typing import Any

from .client import GatewayError


@dataclass
class _Decision:
    transcript: str
    minimum: float
    probability: float | None = None
    delay: float | None = None


class NativeTurnPolicy:
    def __init__(self, recognition: Any) -> None:
        self.recognition = recognition
        self.changed = asyncio.Event()
        self._decisions: weakref.WeakKeyDictionary[Any, _Decision] = (
            weakref.WeakKeyDictionary()
        )
        self._watched: weakref.WeakSet[Any] = weakref.WeakSet()
        self._closed = False
        self.refresh()

    def limits(self) -> tuple[float, float]:
        endpointing = getattr(self.recognition, "_endpointing", None)
        if endpointing is not None:
            return endpointing.min_delay, endpointing.max_delay
        return (
            self.recognition._min_endpointing_delay,
            self.recognition._max_endpointing_delay,
        )

    def refresh(self) -> None:
        detector = self.recognition._turn_detector
        if detector is not None and not (
            isinstance(detector, _Detector) and detector.policy is self
        ):
            while isinstance(detector, _Detector):
                detector = detector.original
            wrapper = (
                _StreamingDetector
                if callable(getattr(detector, "stream", None))
                else _ClassicDetector
            )
            self.recognition._turn_detector = wrapper(self, detector)
            self._decisions.clear()
        stream = getattr(self.recognition, "_turn_detector_stream", None)
        if stream is not None and not (
            isinstance(stream, _Stream) and stream.policy is self
        ):
            while isinstance(stream, _Stream):
                stream = stream.original
            self.recognition._turn_detector_stream = _Stream(self, stream)

    def begin(self) -> _Decision | None:
        task = asyncio.current_task()
        if (
            self._closed
            or task is None
            or task is not getattr(self.recognition, "_end_of_turn_task", None)
        ):
            return None
        decision = _Decision(
            " ".join(self.recognition._audio_transcript.split()), self.limits()[0]
        )
        self._decisions[task] = decision
        return decision

    def current(self) -> _Decision | None:
        if self._closed or asyncio.current_task() is not getattr(
            self.recognition, "_end_of_turn_task", None
        ):
            return None
        return self._decisions.get(asyncio.current_task())

    def record(
        self, decision: _Decision | None, threshold: float | None = None
    ) -> None:
        if decision is None or self._closed:
            return
        decision.delay = (
            self.limits()[1]
            if decision.probability is not None
            and threshold is not None
            and decision.probability < threshold
            else decision.minimum
        )
        self.changed.set()

    def reconsider(self, *, skip_reply: bool = False) -> None:
        run = self.recognition._run_eou_detection
        options: dict[str, Any] = {"skip_reply": skip_reply}
        if "trigger" in inspect.signature(run).parameters:
            options["trigger"] = (
                "manual" if self.recognition._turn_detection_mode == "manual" else "stt"
            )
        run(self.recognition._hooks.retrieve_chat_ctx().copy(), **options)

    async def delay_for(self, transcript: str, gap: float) -> float | None:
        self.refresh()
        if self.recognition._turn_detection_mode == "manual":
            return None
        minimum, maximum = self.limits()
        task = getattr(self.recognition, "_end_of_turn_task", None)
        decision = self._decisions.get(task) if task is not None else None
        if decision is not None and " ".join(decision.transcript.split()) == transcript:
            if decision.delay is not None:
                return decision.delay
            minimum = decision.minimum
        if self.recognition._turn_detector is None or gap < minimum:
            return minimum
        if gap >= maximum:
            return maximum
        if (
            isinstance(self.recognition._turn_detector, _StreamingDetector)
            and self.recognition._speaking
            and self.recognition._turn_detector_prediction_fut is None
        ):
            # The SDK explicitly withdrew its acoustic prediction on resumed
            # speech. Do not synthesize a minimum-delay verdict in its place.
            return maximum
        restarted = False
        while True:
            if self._closed:
                raise GatewayError("Native turn ownership changed", retryable=False)
            task = getattr(self.recognition, "_end_of_turn_task", None)
            decision = self._decisions.get(task) if task is not None else None
            if (
                decision is not None
                and " ".join(decision.transcript.split()) == transcript
                and decision.delay is not None
            ):
                return decision.delay
            if (
                task is not None
                and task.done()
                and not task.cancelled()
                and task.exception() is not None
            ):
                raise GatewayError("Native turn detection failed", retryable=False)
            if task is not None and task.cancelled() and self.recognition._speaking:
                # The framework itself withdrew its pending EOT on resumed speech.
                return maximum
            if " ".join(self.recognition._audio_transcript.split()) == transcript and (
                task is None or task.done()
            ):
                if restarted:
                    raise GatewayError(
                        "Native turn detection produced no decision", retryable=False
                    )
                # A session VAD may already observe the queued next utterance.
                # Ask the SDK to classify the completed, withheld boundary.
                self.reconsider()
                restarted = True
                task = getattr(self.recognition, "_end_of_turn_task", None)
            self.changed.clear()
            if task is not None and task not in self._watched:
                self._watched.add(task)
                task.add_done_callback(lambda _task: self.changed.set())
            await self.changed.wait()

    def close(self) -> None:
        self._closed = True
        detector = self.recognition._turn_detector
        if isinstance(detector, _Detector) and detector.policy is self:
            self.recognition._turn_detector = detector.original
        stream = getattr(self.recognition, "_turn_detector_stream", None)
        if isinstance(stream, _Stream) and stream.policy is self:
            self.recognition._turn_detector_stream = stream.original
        self.changed.set()
        self._decisions.clear()


class _Detector:
    def __init__(self, policy: NativeTurnPolicy, original: Any) -> None:
        self.policy, self.original = policy, original

    @property
    def model(self) -> str:
        return self.original.model

    @property
    def provider(self) -> str:
        return self.original.provider

    async def supports_language(self, *args: Any, **kwargs: Any) -> bool:
        decision = self.policy.begin()
        supported = await self.original.supports_language(*args, **kwargs)
        if not supported:
            self.policy.record(decision)
        return supported

    async def unlikely_threshold(self, *args: Any, **kwargs: Any) -> float | None:
        decision = self.policy.current()
        if isinstance(self, _Stream) and decision is not None:
            future = self.policy.recognition._turn_detector_prediction_fut
            if future is not None and future.done() and not future.cancelled():
                decision.probability = future.result().end_of_turn_probability
        try:
            threshold = await self.original.unlikely_threshold(*args, **kwargs)
        except Exception:
            self.policy.record(decision)
            raise
        self.policy.record(decision, threshold)
        return threshold


class _ClassicDetector(_Detector):
    async def predict_end_of_turn(self, *args: Any, **kwargs: Any) -> float:
        decision = self.policy.current()
        try:
            probability = await self.original.predict_end_of_turn(*args, **kwargs)
        except Exception:
            self.policy.record(decision)
            raise
        if decision is not None:
            decision.probability = probability
        return probability


class _StreamingDetector(_Detector):
    def stream(self, *args: Any, **kwargs: Any) -> _Stream:
        return _Stream(self.policy, self.original.stream(*args, **kwargs))


class _Stream(_Detector):
    @property
    def is_fallback(self) -> bool:
        return self.original.is_fallback

    @property
    def prediction_timeout(self) -> float:
        return self.original.prediction_timeout

    async def supports_language(self, *args: Any, **kwargs: Any) -> bool:
        supported = await super().supports_language(*args, **kwargs)
        if supported and self.policy.recognition._turn_detector_prediction_fut is None:
            self.policy.record(self.policy.current())
        return supported

    async def backchannel_threshold(self, *args: Any, **kwargs: Any) -> float | None:
        return await self.original.backchannel_threshold(*args, **kwargs)

    def predict(self) -> Any:
        return self.original.predict()

    def cancel_inference(self, *, timed_out: bool = False) -> None:
        if timed_out:
            self.policy.record(self.policy.current())
        self.original.cancel_inference(timed_out=timed_out)

    def flush(self, reason: str | None = None) -> None:
        self.original.flush(reason=reason)

    def push_audio(self, frame: Any) -> None:
        self.original.push_audio(frame)

    def end_input(self) -> None:
        self.original.end_input()

    async def aclose(self) -> None:
        await self.original.aclose()
