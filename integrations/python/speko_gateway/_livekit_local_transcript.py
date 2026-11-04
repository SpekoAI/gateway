"""Keep local Gateway speaker events and canonical SDK turns together."""

from __future__ import annotations

import dataclasses
import inspect
import weakref
from functools import wraps
from typing import Any

from livekit.agents import Agent, stt
from livekit.agents.voice.agent_activity import AgentActivity

from ._livekit_native_speaker import NativeSpeechData
from ._livekit_native_transcript import _processing_method


def bind_local_transcript(owner: Any) -> None:
    # prewarm is called synchronously by these SDK activity methods, including
    # through nested adapters. Match their code objects; never retain frames.
    codes = {
        method.__code__
        for name in ("start", "_update_models")
        if (method := getattr(AgentActivity, name, None)) is not None
    }
    activities: set[Any] = set()
    models: list[Any] = []
    frame = inspect.currentframe()
    try:
        while frame is not None:
            if frame.f_code in codes:
                activity = frame.f_locals.get("self")
                if isinstance(activity, AgentActivity):
                    activities.add(activity)
            elif frame.f_code is Agent.default.stt_node.__code__:
                activity = frame.f_locals.get("activity")
                if isinstance(activity, AgentActivity):
                    activities.add(activity)
            else:
                stream = frame.f_locals.get("self")
                if isinstance(stream, stt.RecognizeStream) and type(stream).__module__.startswith("livekit.agents.stt."):
                    models.append(stream._stt)
            frame = frame.f_back
    finally:
        del frame
    # Older SDK adapters do not delegate prewarm. Their stream entry still
    # identifies the outer adapter. Follow only its SDK metrics listeners to
    # its activities, without calling callbacks or inspecting customer data.
    seen: set[int] = set()
    while models:
        model = models.pop()
        if id(model) in seen:
            continue
        seen.add(id(model))
        if isinstance(model, (stt.MultiSpeakerAdapter, stt.FallbackAdapter)):
            forwarding = getattr(model, "_speko_local_prewarm_binding", None)
            if forwarding is None:
                forwarding = _Prewarm(model)
                model._speko_local_prewarm_binding = forwarding
            forwarding.owners.add(owner)
            owner._local_transcript_bindings.add(forwarding)
        for callback in tuple(model._events.get("metrics_collected", ())):
            target = getattr(callback, "__self__", None)
            if getattr(callback, "__func__", None) is AgentActivity._on_metrics_collected:
                activities.add(target)
            elif isinstance(target, stt.FallbackAdapter) and getattr(callback, "__func__", None) is stt.FallbackAdapter._on_metrics_collected:
                models.append(target)
    for activity in activities:
        recognition = activity._audio_recognition
        if recognition is not None and recognition._closing.is_set():
            continue
        binding = getattr(activity, "_speko_local_transcript_binding", None)
        if binding is None:
            binding = _Binding(activity)
            activity._speko_local_transcript_binding = binding
        binding.owners.add(owner)
        owner._local_transcript_bindings.add(binding)
        binding.start()


class _Prewarm:
    """Observe future activities even when an older wrapper retains its stream."""

    def __init__(self, model: Any) -> None:
        self.model = model
        self.owners: weakref.WeakSet[Any] = weakref.WeakSet()
        self.original = model.prewarm

        @wraps(self.original)
        def prewarm() -> None:
            for owner in list(self.owners):
                bind_local_transcript(owner)
            self.original()

        self.observed = prewarm
        model.prewarm = prewarm

    def release(self, owner: Any) -> None:
        self.owners.discard(owner)
        if not self.owners:
            if self.model.prewarm is self.observed:
                self.model.prewarm = self.original
            if getattr(self.model, "_speko_local_prewarm_binding", None) is self:
                del self.model._speko_local_prewarm_binding


class _Binding:
    def __init__(self, activity: Any) -> None:
        self.activity = activity
        self.owners: weakref.WeakSet[Any] = weakref.WeakSet()
        self.startup: tuple[Any, Any] | None = None
        self.processing: tuple[Any, str, Any, Any] | None = None
        self.turn: tuple[Any, Any] | None = None
        self.prefix: str | None = None
        self.rewritten = False
        self.pending = False
        self.deferred: Any = None

    def start(self) -> None:
        if self.activity._audio_recognition is not None:
            self._bind()
            return
        if self.startup is not None:
            return
        original = self.activity._start_session

        @wraps(original)
        async def start(*args: Any, **kwargs: Any) -> Any:
            try:
                result = await original(*args, **kwargs)
                if self.owners:
                    self._bind()
                return result
            finally:
                self._restore_startup()

        self.activity._start_session = start
        self.startup = original, start

    def _restore_startup(self) -> None:
        if self.startup is not None:
            original, start = self.startup
            if self.activity._start_session is start:
                self.activity._start_session = original
            self.startup = None

    def _bind(self) -> None:
        recognition = self.activity._audio_recognition
        if self.processing is not None and self.processing[0] is recognition:
            return
        self._restore_processing()
        method = _processing_method(recognition)
        original = getattr(recognition, method)

        def prepare(event: Any) -> tuple[NativeSpeechData, str] | None:
            data = event.alternatives[0] if event.alternatives else None
            if (
                not self.owners
                or event.type is not stt.SpeechEventType.FINAL_TRANSCRIPT
                or not isinstance(data, NativeSpeechData)
                or data._local_part_text is None
                or data.canonical_text is None
            ):
                return None
            before = recognition._audio_transcript
            if self.prefix is None:
                self.prefix = before
                self.rewritten = False
            self.rewritten |= data.text != data._local_part_text
            self.pending = not bool(data.canonical_text)
            return data, before

        def reconcile(part: tuple[NativeSpeechData, str] | None) -> None:
            if part is None:
                return
            data, before = part
            if data.canonical_text:
                if not self.rewritten and recognition._audio_transcript != before:
                    recognition._audio_transcript = " ".join(
                        text for text in (self.prefix, data.canonical_text) if text
                    )
                self.prefix = None
                self.pending = False
                if self.deferred is not None and not recognition._closing.is_set():
                    run = recognition._run_eou_detection
                    options = {"skip_reply": self.deferred.skip_reply}
                    if "trigger" in inspect.signature(run).parameters:
                        options["trigger"] = "manual" if recognition._turn_detection_mode == "manual" else "stt"
                    run(recognition._hooks.retrieve_chat_ctx().copy(), **options)

        if inspect.iscoroutinefunction(original):
            @wraps(original)
            async def observed(event: Any) -> Any:
                part = prepare(event)
                result = await original(event)
                reconcile(part)
                return result
        else:
            @wraps(original)
            def observed(event: Any) -> Any:
                part = prepare(event)
                result = original(event)
                reconcile(part)
                return result

        setattr(recognition, method, observed)
        self.processing = recognition, method, original, observed
        if self.turn is None:
            original_turn = self.activity.on_end_of_turn

            @wraps(original_turn)
            def completed(info: Any) -> bool:
                if self.pending:
                    self.deferred = info
                    return False
                if self.deferred is not None and self.deferred.skip_reply and not info.skip_reply:
                    info = dataclasses.replace(info, skip_reply=True)
                accepted = original_turn(info)
                if accepted:
                    self.deferred = None
                return accepted

            self.activity.on_end_of_turn = completed
            self.turn = original_turn, completed

    def _restore_processing(self) -> None:
        if self.processing is not None:
            recognition, method, original, observed = self.processing
            if getattr(recognition, method) is observed:
                setattr(recognition, method, original)
            self.processing = None
        self.prefix, self.pending, self.deferred = None, False, None

    def release(self, owner: Any) -> None:
        self.owners.discard(owner)
        if self.owners:
            return
        self._restore_startup()
        self._restore_processing()
        if self.turn is not None:
            original, completed = self.turn
            if self.activity.on_end_of_turn is completed:
                self.activity.on_end_of_turn = original
            self.turn = None
        if getattr(self.activity, "_speko_local_transcript_binding", None) is self:
            del self.activity._speko_local_transcript_binding
