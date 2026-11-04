"""Project terminal native text after the SDK consumes attributed finals."""

from __future__ import annotations

import inspect
import uuid
import weakref
from dataclasses import dataclass
from functools import wraps
from typing import Any

from livekit.agents import stt

from .client import GatewayError

_batches: weakref.WeakValueDictionary[str, NativeFinalBatch] = weakref.WeakValueDictionary()


def _final_witness(event: Any) -> tuple[NativeFinalBatch, int] | None:
    data = event.alternatives[0] if event.alternatives else None
    witness = getattr(data, "_native_final", None)
    if witness is None:
        return None
    token, index = witness
    batch = _batches.get(token)
    return (batch, index) if batch is not None else None


@dataclass(frozen=True, eq=False)
class NativeTranscript:
    previous: NativeTranscript | None
    text: str

    def render(self) -> str:
        parts, node = [], self
        while node is not None:
            parts.append(node.text)
            node = node.previous
        return " ".join(reversed(parts))


class NativeFinalBatch:
    def __init__(
        self, stream: Any, transcript: NativeTranscript, texts: tuple[str, ...]
    ) -> None:
        self.stream = weakref.ref(stream)
        self.transcript = transcript
        self.texts = texts
        self.next_index = 0
        self.token = uuid.uuid4().hex
        _batches[self.token] = self
        stream._final_batches[self.token] = self
        self.commit = (
            weakref.ref(stream._turn_commit)
            if stream._turn_commit is not None
            else None
        )

    def __deepcopy__(self, memo: dict[int, Any]) -> NativeFinalBatch:
        return self

    def validate(self, index: int, text: str) -> None:
        if (
            not 0 <= index < len(self.texts)
            or index != self.next_index
            or text != self.texts[index]
        ):
            stream = self.stream()
            if stream is not None:
                stream._mark_failed()
            raise GatewayError(
                "Native STT requires unchanged ordered transcript events",
                retryable=False,
            )
        self.next_index += 1

    def complete(self, binding: Any, recognition: Any) -> None:
        stream = self.stream()
        if binding is None or stream is None or stream._stt not in binding.owners:
            return
        commit = stream._turn_commit
        if (
            commit is None
            or self.commit is None
            or commit is not self.commit()
            or commit.done()
        ):
            return
        activity = getattr(binding._session, "_activity", None)
        if (
            getattr(activity, "_audio_recognition", None) is not recognition
            or recognition._closing.is_set()
        ):
            return
        if not stream._stt._owns_native_recognition(activity.stt):
            return
        # SDKs join every final with a space and some promote interims on a
        # manual-commit timeout. Terminal canonical text supersedes both.
        recognition._audio_transcript = self.transcript.render()
        stream._reconsider_turn()

    def release(self) -> None:
        stream = self.stream()
        if stream is not None:
            stream._final_batches.pop(self.token, None)
        _batches.pop(self.token, None)


def validate_native_final(event: Any, binding: Any) -> None:
    if binding is None or event.type is not stt.SpeechEventType.FINAL_TRANSCRIPT:
        return
    witness = _final_witness(event)
    if witness is None:
        return
    batch, index = witness
    stream = batch.stream()
    if stream is None or stream._stt not in binding.owners:
        return
    batch.validate(index, event.alternatives[0].text)


def _processing_method(recognition: Any) -> str:
    # Adaptive replay bypasses the async enqueue callback on newer SDKs.
    return "_process_stt_event" if hasattr(recognition, "_process_stt_event") else "_on_stt_event"


def restore_native_transcript(owner: Any) -> None:
    previous = getattr(owner, "_native_transcript_binding", None)
    if previous is not None:
        recognition, original, observed = previous
        method = _processing_method(recognition)
        if getattr(recognition, method) is observed:
            setattr(recognition, method, original)
        owner._native_transcript_binding = None


def bind_native_transcript(owner: Any, recognition: Any) -> None:
    previous = getattr(owner, "_native_transcript_binding", None)
    if (
        previous is not None
        and previous[0] is recognition
        and getattr(recognition, _processing_method(recognition)) is previous[2]
    ):
        return
    restore_native_transcript(owner)
    method = _processing_method(recognition)
    original = getattr(recognition, method)

    def reconcile(event: Any, before: str) -> None:
        witness = _final_witness(event)
        if witness is not None:
            batch, index = witness
            stream = batch.stream()
            if (
                stream is not None
                and stream._stt is owner
                and index + 1 == len(batch.texts)
                and batch.next_index == len(batch.texts)
                and recognition._audio_transcript != before
            ):
                batch.complete(owner._native_input_binding, recognition)
            if index + 1 == len(batch.texts):
                batch.release()

    if inspect.iscoroutinefunction(original):
        @wraps(original)
        async def observed(event: Any) -> Any:
            before = recognition._audio_transcript
            result = await original(event)
            reconcile(event, before)
            return result
    else:
        @wraps(original)
        def observed(event: Any) -> Any:
            before = recognition._audio_transcript
            result = original(event)
            reconcile(event, before)
            return result

    setattr(recognition, method, observed)
    owner._native_transcript_binding = recognition, original, observed
