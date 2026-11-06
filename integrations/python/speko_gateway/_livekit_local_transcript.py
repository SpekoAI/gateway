"""Keep local Gateway speaker events and canonical SDK turns together."""

from __future__ import annotations

import dataclasses
import inspect
import weakref
from functools import wraps
from typing import Any

from livekit.agents import Agent, stt, vad
from livekit.agents.voice.agent_activity import AgentActivity

from ._livekit_native_speaker import NativeSpeechData
from ._livekit_native_transcript import _processing_method


def restore_local_transcript(activity: Any) -> None:
    binding = getattr(activity, "_speko_local_transcript_binding", None)
    if binding is not None:
        for owner in list(binding.owners):
            binding.release(owner)


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
                    if isinstance(stream._stt, stt.MultiSpeakerAdapter) and stream._stt._suppress_background:
                        filtering = getattr(stream, "_speko_local_filter_binding", None)
                        if filtering is None:
                            filtering = _Filter(stream)
                            stream._speko_local_filter_binding = filtering
                        filtering.owners.add(owner)
                        owner._local_transcript_bindings.add(filtering)
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


class _Filter:
    """Preserve an empty final's group marker after the SDK filters a part."""

    def __init__(self, stream: Any) -> None:
        self.stream = stream
        self.owners: weakref.WeakSet[Any] = weakref.WeakSet()
        self.group: str | None = None
        self.parts: list[tuple[str, bool]] = []
        detector = stream._detector
        self.original, self.original_close = detector.on_stt_event, stream.aclose

        @wraps(self.original)
        def filtered(event: Any) -> Any:
            result = self.original(event)
            data = event.alternatives[0] if event.alternatives else None
            if (
                event.type is stt.SpeechEventType.FINAL_TRANSCRIPT
                and isinstance(data, NativeSpeechData)
                and data._local_group is not None
            ):
                if self.group != data._local_group:
                    self.group = data._local_group
                    self.parts.clear()
                self.parts.append((data._local_part_text or data.text, result is None))
                terminal = data._local_terminal
                canonical = _filtered_text(data.canonical_text, self.parts) if terminal and data.canonical_text is not None else ""
                if result is None:
                    # Only group/terminal scalar evidence and surviving text
                    # leave this owned filter. Suppressed content stays in its
                    # private reconciliation state, never on outward events.
                    clean = NativeSpeechData(
                        language="", text="", canonical_text=canonical,
                        _local_part_text="", _local_group=data._local_group,
                        _local_suppressed=True, _local_filtered=True,
                        _local_terminal=terminal, _local_after_eos=data._local_after_eos,
                    )
                    result = stt.SpeechEvent(type=event.type, alternatives=[clean])
                elif terminal:
                    result = dataclasses.replace(result, alternatives=[dataclasses.replace(
                        result.alternatives[0], canonical_text=canonical, _local_filtered=True,
                    ), *result.alternatives[1:]])
                if terminal:
                    self.group = None
                    self.parts.clear()
            return result

        async def close() -> None:
            try:
                await self.original_close()
            finally:
                self.restore()

        self.observed, self.observed_close = filtered, close
        detector.on_stt_event, stream.aclose = filtered, close

    def restore(self) -> None:
        self.group = None
        self.parts.clear()
        if self.stream._detector.on_stt_event is self.observed:
            self.stream._detector.on_stt_event = self.original
        if self.stream.aclose is self.observed_close:
            self.stream.aclose = self.original_close
        if getattr(self.stream, "_speko_local_filter_binding", None) is self:
            del self.stream._speko_local_filter_binding

    def release(self, owner: Any) -> None:
        self.owners.discard(owner)
        if not self.owners:
            self.restore()


def _filtered_text(canonical: str, parts: list[tuple[str, bool]]) -> str | None:
    if not any(suppressed for _, suppressed in parts):
        return canonical
    cursor, accepted = 0, []
    for text, suppressed in parts:
        start = canonical.find(text, cursor)
        if start < 0:
            # A custom node supplied a different set of parts. Its SDK text
            # remains authoritative; never reconstruct omitted content.
            return None
        if suppressed:
            gap = canonical[cursor:start]
            if gap.strip():
                accepted.append(gap)
        else:
            accepted.append(canonical[cursor:start + len(text)])
        cursor = start + len(text)
    accepted.append(canonical[cursor:])
    return "".join(accepted).strip()


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
        self.vad_processing: tuple[Any, Any, Any] | None = None
        self.clear_processing: tuple[Any, str, Any, Any] | None = None
        self.turn: tuple[Any, Any] | None = None
        self.prefix: str | None = None
        self.rewritten = False
        self.pending = False
        self.deferred: Any = None
        self.group: str | None = None
        self.parts: list[tuple[str, bool]] = []

    def _owns_recognition(self) -> bool:
        visited: set[int] = set()

        def owns(model: Any) -> bool:
            if model in self.owners:
                return True
            if id(model) in visited:
                return False
            visited.add(id(model))
            if isinstance(model, stt.MultiSpeakerAdapter):
                return owns(model._stt)
            if isinstance(model, stt.FallbackAdapter):
                return any(owns(child) for child in model._stt_instances)
            return False

        return owns(self.activity.stt)

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

    def _rearm(self, recognition: Any, *, after_eos: bool = False) -> None:
        if not self.owners or self.pending or recognition._closing.is_set() or recognition._speaking:
            return
        if not self._owns_recognition():
            restore_local_transcript(self.activity)
            return
        manual = recognition._turn_detection_mode == "manual"
        requested = bool(getattr(recognition, "_user_turn_committed", False))
        if (manual and not requested and self.deferred is None) or (self.deferred is None and not after_eos):
            return
        run = recognition._run_eou_detection
        options = {"skip_reply": self.deferred.skip_reply if self.deferred is not None else False}
        if "trigger" in inspect.signature(run).parameters:
            options["trigger"] = "manual" if manual else "stt"
        run(recognition._hooks.retrieve_chat_ctx().copy(), **options)

    def _bind(self) -> None:
        previous = getattr(self.activity, "_speko_native_stt_turn_owner", None)
        if previous is not None:
            previous._restore_turn_binding()
        recognition = self.activity._audio_recognition
        if self.processing is not None and self.processing[0] is recognition:
            return
        self._restore_processing()
        method = _processing_method(recognition)
        original = getattr(recognition, method)

        def prepare(event: Any) -> tuple[NativeSpeechData, str] | None:
            if not self._owns_recognition():
                restore_local_transcript(self.activity)
                return None
            data = event.alternatives[0] if event.alternatives else None
            if (
                not self.owners
                or event.type is not stt.SpeechEventType.FINAL_TRANSCRIPT
                or not isinstance(data, NativeSpeechData)
                or data._local_part_text is None
                or (data.canonical_text is None and not data._local_terminal)
                or data._local_group is None
            ):
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    # A fallback/custom node emitted its own final. Leave
                    # that SDK text in place and retire our incomplete group.
                    self.prefix, self.pending, self.group = None, False, None
                    self.parts.clear()
                return None
            before = recognition._audio_transcript
            if self.group != data._local_group:
                self.prefix = before
                self.rewritten = False
                self.group = data._local_group
                self.parts.clear()
            self.parts.append((data._local_part_text, data._local_suppressed))
            self.rewritten |= data.text != ("" if data._local_suppressed else data._local_part_text)
            self.pending = not data._local_terminal
            return data, before

        def reconcile(part: tuple[NativeSpeechData, str] | None) -> None:
            if part is None:
                return
            data, before = part
            if data._local_terminal:
                filtered = data.canonical_text if data._local_filtered else _filtered_text(data.canonical_text or "", self.parts)
                if not self.rewritten and filtered is not None and (recognition._audio_transcript != before or any(suppressed for _, suppressed in self.parts)):
                    recognition._audio_transcript = " ".join(
                        text for text in (self.prefix, filtered) if text
                    )
                if any(suppressed for _, suppressed in self.parts):
                    recognition._audio_interim_transcript = ""
                self.prefix = None
                self.pending = False
                self.group = None
                self.parts.clear()
                self._rearm(recognition, after_eos=data._local_after_eos)

        if inspect.iscoroutinefunction(original):
            @wraps(original)
            async def observed(event: Any) -> Any:
                part = prepare(event)
                result = await original(event)
                reconcile(part)
                if event.type is stt.SpeechEventType.END_OF_SPEECH:
                    self._rearm(recognition)
                return result
        else:
            @wraps(original)
            def observed(event: Any) -> Any:
                part = prepare(event)
                result = original(event)
                reconcile(part)
                if event.type is stt.SpeechEventType.END_OF_SPEECH:
                    self._rearm(recognition)
                return result

        setattr(recognition, method, observed)
        self.processing = recognition, method, original, observed
        original_vad = recognition._on_vad_event

        @wraps(original_vad)
        async def observed_vad(event: Any) -> Any:
            result = await original_vad(event)
            if event.type is vad.VADEventType.END_OF_SPEECH:
                # A rejected manual EOU clears the SDK's committed flag. The
                # explicit decision remains ours until actual speech ends.
                self._rearm(recognition)
            return result

        recognition._on_vad_event = observed_vad
        self.vad_processing = recognition, original_vad, observed_vad
        clear_method = "_clear_user_turn" if hasattr(recognition, "_clear_user_turn") else "clear_user_turn"
        original_clear = getattr(recognition, clear_method)

        @wraps(original_clear)
        def cleared(*args: Any, **kwargs: Any) -> Any:
            self.prefix, self.pending, self.deferred = None, False, None
            self.group = None
            self.parts.clear()
            return original_clear(*args, **kwargs)

        setattr(recognition, clear_method, cleared)
        self.clear_processing = recognition, clear_method, original_clear, cleared
        if self.turn is None:
            original_turn = self.activity.on_end_of_turn

            @wraps(original_turn)
            def completed(info: Any) -> bool:
                if not self._owns_recognition():
                    restore_local_transcript(self.activity)
                    return original_turn(info)
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
        if self.clear_processing is not None:
            recognition, method, original, observed = self.clear_processing
            if getattr(recognition, method) is observed:
                setattr(recognition, method, original)
            self.clear_processing = None
        if self.vad_processing is not None:
            recognition, original, observed = self.vad_processing
            if recognition._on_vad_event is observed:
                recognition._on_vad_event = original
            self.vad_processing = None
        if self.processing is not None:
            recognition, method, original, observed = self.processing
            if getattr(recognition, method) is observed:
                setattr(recognition, method, original)
            self.processing = None
        self.prefix, self.pending, self.deferred = None, False, None
        self.group = None
        self.parts.clear()

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
