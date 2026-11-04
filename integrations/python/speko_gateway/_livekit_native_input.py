"""Observe SDK audio before custom STT nodes consume or buffer it."""

from __future__ import annotations

import contextvars
import inspect
import time
import weakref
from dataclasses import dataclass, field
from fractions import Fraction
from functools import wraps
from typing import Any

from livekit import rtc
from livekit.agents.voice import audio_recognition, io

from ._livekit_native_transcript import validate_native_final
from .client import GatewayError

_POSITION = "_speko_native_input_position"
_SOURCE_POSITION = "_speko_native_source_position"


def _copy_frame(frame: rtc.AudioFrame) -> rtc.AudioFrame:
    size = frame.samples_per_channel * frame.num_channels * 2
    owned = rtc.AudioFrame(
        data=bytearray(frame.data.cast("B")[:size]),
        sample_rate=frame.sample_rate,
        num_channels=frame.num_channels,
        samples_per_channel=frame.samples_per_channel,
    )
    owned.userdata.update(frame.userdata)
    return owned


@dataclass(frozen=True)
class _SourcePosition:
    scope: object
    start: Fraction
    synthetic: bool = False

    def __deepcopy__(self, memo: dict[int, Any]) -> _SourcePosition:
        return self


@dataclass
class _SourceClock:
    position: Fraction | None = None
    owner_scope: object | None = None
    generation: int = -1
    activity: weakref.ReferenceType[Any] | None = None


@dataclass
class _SourceState:
    scope: object = field(default_factory=object)
    end: Fraction = Fraction(0)
    generation: int = 0
    clocks: dict[int, tuple[weakref.ReferenceType[Any], _SourceClock]] = field(default_factory=dict)

    def clock_for(self, source: Any) -> _SourceClock:
        key, clocks = id(source), self.clocks
        entry = clocks.get(key)
        if entry is None or entry[0]() is not source:
            def released(reference: Any) -> None:
                current = clocks.get(key)
                if current is not None and current[0] is reference:
                    del clocks[key]

            entry = weakref.ref(source, released), _SourceClock()
            clocks[key] = entry
        return entry[1]


class _SourceInput(io.AudioInput):
    def __init__(self, source: io.AudioInput, binding: NativeInputBinding) -> None:
        super().__init__(label=source.label, source=source)
        self.binding = binding
        root, seen = source, set()
        while root.source is not None and id(root) not in seen:
            seen.add(id(root))
            root = root.source
        # Adding a delegate around the same source does not replace its
        # captured media clock. Keep the clock weakly keyed by the source.
        self._clock = binding._source_state.clock_for(root)
        self._iterator: Any = None

    def __aiter__(self) -> Any:
        # The SDK has captured this iterator before its first suspension.
        # Restore the public input immediately, before calling user input or
        # yielding control. Reassigning the same public input stays a no-op.
        session = self.binding._session
        if session.input.audio is self:
            session.input._audio_stream = self.source
        if self._iterator is None:
            self._iterator = aiter(self.source)
        return self

    async def __anext__(self) -> rtc.AudioFrame:
        if self._iterator is None:
            self.__aiter__()
        frame = await anext(self._iterator)
        if not isinstance(frame, rtc.AudioFrame) or frame.sample_rate <= 0:
            return frame
        active = _lookup(_sessions, self.binding._session)
        clock = active if active is not None and active.owners else self.binding
        state, observed = clock._source_state, self._clock
        scope = active.scope if active is not None and active.owners else None
        activity = getattr(self.binding._session, "_activity", None)
        if (
            observed.position is None
            or observed.owner_scope is not scope
            or observed.generation != state.generation
            or (observed.activity() if observed.activity is not None else None) is not activity
        ):
            observed.position = max(observed.position or Fraction(0), clock.source_start())
            observed.owner_scope, observed.generation = scope, state.generation
            observed.activity = weakref.ref(activity) if activity is not None else None
        observed.position = max(observed.position, state.end)
        start = observed.position
        observed.position += Fraction(frame.samples_per_channel, frame.sample_rate)
        state.end = max(state.end, observed.position)
        if active is None or not active.owners:
            return frame
        owned = _copy_frame(frame)
        owned.userdata[_SOURCE_POSITION] = _SourcePosition(active.source_scope, start)
        return owned


@dataclass
class NativeInputTimeline:
    position: Fraction = Fraction(0)
    scope: object = field(default_factory=object)
    legacy: bool = False
    source_scope: object | None = None

    def observe(self, frame: rtc.AudioFrame) -> rtc.AudioFrame:
        duration = Fraction(frame.samples_per_channel, frame.sample_rate)
        # One occurrence gets one immutable witness even if an input reuses
        # the same AudioFrame object. Preserve the SDK's valid PCM and userdata.
        owned = _copy_frame(frame)
        source = frame.userdata.get(_SOURCE_POSITION)
        observed = isinstance(source, _SourcePosition) and source.scope is self.source_scope
        start = source.start if self.legacy and observed else self.position
        owned.userdata[_POSITION] = _InputPosition(
            self, start, duration, not self.legacy or observed,
            self.legacy and observed and source.synthetic,
        )
        self.position += duration
        return owned

    def occurrence_of(self, frame: rtc.AudioFrame) -> _InputPosition:
        witness = frame.userdata.get(_POSITION)
        if (
            not isinstance(witness, _InputPosition)
            or witness.timeline is not self
            or not witness.observed
            or witness.duration
            != Fraction(frame.samples_per_channel, frame.sample_rate)
        ):
            raise GatewayError(
                "Native STT lost its audio input position", retryable=False
            )
        return witness


@dataclass(frozen=True)
class _InputPosition:
    timeline: NativeInputTimeline
    start: Fraction
    duration: Fraction
    observed: bool = True
    synthetic: bool = False

    def __deepcopy__(self, memo: dict[int, Any]) -> _InputPosition:
        # The immutable occurrence belongs to its original timeline even
        # when a duration-preserving DSP replacement deep-copies userdata.
        return self


_active: contextvars.ContextVar[NativeInputTimeline | None] = contextvars.ContextVar(
    "speko_native_input_timeline", default=None
)


def native_input_timeline(binding: NativeInputBinding | None) -> NativeInputTimeline:
    timeline = _active.get()
    if timeline is None or binding is None or timeline.scope is not binding.scope:
        raise ValueError(
            "Native STT requires its observed Agent input; explicit consumers use manual_turns=True"
        )
    return timeline


@dataclass
class _AgentHooks:
    original_node: Any = None
    node: Any = None
    original_update: Any = None
    update: Any = None
    active: bool = False
    pending_updates: int = 0


_sessions: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()
_agents: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()
_source_states: weakref.WeakKeyDictionary[Any, _SourceState] = weakref.WeakKeyDictionary()


def _lookup(registry: Any, key: Any) -> NativeInputBinding | None:
    value = registry.get(key)
    return value() if value is not None else None


class NativeInputBinding:
    def __init__(self) -> None:
        self.scope = object()
        self._source_state = _SourceState()
        self.owners: weakref.WeakSet[Any] = weakref.WeakSet()
        self._session: Any = None
        self._session_update: tuple[Any, Any] | None = None
        self._session_forward: tuple[Any, Any] | None = None
        self._session_audio_enabled: tuple[Any, Any, Any] | None = None
        self._recognition_inputs: dict[Any, tuple[Any, Any]] = {}
        self._agents: dict[Any, _AgentHooks] = {}
        self._timelines: weakref.WeakKeyDictionary[Any, NativeInputTimeline] = (
            weakref.WeakKeyDictionary()
        )

    @property
    def source_scope(self) -> object:
        return self._source_state.scope

    def attach_session(self, session: Any) -> NativeInputBinding:
        # Small recovery-pump fixtures do not implement an AgentSession's
        # activity lifecycle. Their explicitly supplied node can still run.
        if not callable(getattr(session, "_update_activity", None)):
            return self
        if self._session is not None and self._session is not session:
            raise ValueError("Native input binding cannot span AgentSessions")
        previous = _lookup(_sessions, session)
        if previous is not None and previous is not self:
            return self._join(previous)
        if self._session is None:
            self._session = session
            # Source coordinates belong to the session even when its final
            # native owner closes and a later owner resumes a captured pump.
            self._source_state = _source_states.setdefault(session, _SourceState())
            _sessions[session] = weakref.ref(self)
            original = session._update_activity

            @wraps(original)
            async def update(agent: Any, *args: Any, **kwargs: Any) -> Any:
                hooks = None
                if self.owners:
                    self.attach_agent(agent)
                    hooks = self._agents[agent]
                    hooks.pending_updates += 1
                try:
                    return await original(agent, *args, **kwargs)
                finally:
                    if hooks is not None:
                        hooks.pending_updates -= 1
                    for previous_agent, previous_hooks in list(self._agents.items()):
                        # The SDK serializes updates after this wrapper runs.
                        # No activity can also mean this Agent is still queued.
                        if (
                            not previous_hooks.pending_updates
                            and getattr(previous_agent, "_activity", None) is None
                        ):
                            self._detach_agent(previous_agent)
                    self._restore_recognition_inputs(closed_only=True)

            session._update_activity = update
            self._session_update = (original, update)
            self._observe_source_task(session)
        activity = getattr(session, "_activity", None)
        agent = getattr(activity, "agent", None) or getattr(activity, "_agent", None)
        if agent is not None:
            self.attach_agent(agent)
        return self

    def _observe_source_task(self, session: Any) -> None:
        if hasattr(audio_recognition.AudioRecognition, "_input_started_at"):
            # Modern SDKs already own the pipeline clock. Leave their public
            # source and attach/detach behavior entirely under SDK control.
            return
        original = getattr(session, "_forward_audio_task", None)
        if not callable(original):
            return

        @wraps(original)
        async def forward() -> None:
            source = session.input.audio
            if not self.owners or source is None:
                await original()
                return
            current, seen = source, set()
            while current is not None and id(current) not in seen:
                seen.add(id(current))
                if (
                    isinstance(current, _SourceInput)
                    and current.binding._session is session
                ):
                    # A recording/input wrapper can already delegate to the
                    # session observer. Keep a single clock for each frame.
                    await original()
                    return
                current = getattr(current, "source", None)
            # Preserve the SDK's pump and attach/detach notifications. Its
            # public setter would cancel this pump, so replace only its owned
            # source reference until __aiter__ confirms that it was captured.
            observed = _SourceInput(source, self)
            session.input._audio_stream = observed
            try:
                await original()
            finally:
                if session.input.audio is observed:
                    session.input._audio_stream = source

        session._forward_audio_task = forward
        self._session_forward = (original, forward)
        original_enable = session.input.set_audio_enabled

        @wraps(original_enable)
        def enabled(*args: Any, **kwargs: Any) -> Any:
            before = session.input.audio_enabled
            result = original_enable(*args, **kwargs)
            if before != session.input.audio_enabled:
                self._source_state.generation += 1
            return result

        session.input.set_audio_enabled = enabled
        self._session_audio_enabled = session.input, original_enable, enabled

    def _observe_recognition_input(self, recognition: Any) -> None:
        self._restore_recognition_inputs(closed_only=True)
        if recognition in self._recognition_inputs:
            return
        original = recognition.push_audio
        real_end: Fraction | None = None

        @wraps(original)
        def push(frame: rtc.AudioFrame, *args: Any, **kwargs: Any) -> Any:
            nonlocal real_end
            active = _lookup(_sessions, self._session)
            current = getattr(
                getattr(self._session, "_activity", None), "_audio_recognition", None
            )
            if (
                active is not None
                and active.owners
                and current is recognition
                and isinstance(frame, rtc.AudioFrame)
            ):
                witness = frame.userdata.get(_SOURCE_POSITION)
                if (
                    not isinstance(witness, _SourcePosition)
                    or witness.scope is not active.source_scope
                ):
                    # The SDK injects flush audio directly into recognition.
                    # Observe it before custom nodes, just like source input.
                    owned = _copy_frame(frame)
                    # Flush PCM has an ingress duration, but was never
                    # captured in the session/recording timeline. Pin it to
                    # the last real ingress, including when a delegated input
                    # still holds an already-observed later source frame.
                    start = real_end if real_end is not None else Fraction(0)
                    owned.userdata[_SOURCE_POSITION] = _SourcePosition(
                        active.source_scope, start, synthetic=True
                    )
                    active._source_state.generation += 1
                    frame = owned
                elif not witness.synthetic:
                    real_end = witness.start + Fraction(frame.samples_per_channel, frame.sample_rate)
            return original(frame, *args, **kwargs)

        recognition.push_audio = push
        self._recognition_inputs[recognition] = original, push

    def _restore_recognition_inputs(self, *, closed_only: bool = False) -> None:
        for recognition, (original, push) in list(self._recognition_inputs.items()):
            if closed_only and not recognition._closing.is_set():
                continue
            if recognition.push_audio is push:
                recognition.push_audio = original
            del self._recognition_inputs[recognition]

    def source_start(self) -> Fraction:
        now = time.time()
        recorder = getattr(self._session, "_recorder_io", None)
        epoch = (
            getattr(recorder, "recording_started_at", None)
            or self._session._started_at
            or now
        )
        # Anchor each attached input at its first actual delivery, before
        # the SDK buffers it. A replacement includes its idle gap and never
        # overlaps media that the previous input has already delivered.
        return max(self._source_state.end, Fraction(str(max(0.0, now - epoch))))

    def _join(self, other: NativeInputBinding) -> NativeInputBinding:
        if (
            self._session is not None
            and other._session is not None
            and self._session is not other._session
        ):
            raise ValueError("Native input binding cannot span AgentSessions")
        agents = list(self._agents)
        for owner in list(self.owners):
            other.owners.add(owner)
            owner._native_input_binding = other
        self.owners.clear()
        self._restore()
        for agent in agents:
            other.attach_agent(agent)
        return other

    def _timeline(self, audio: Any, session: Any) -> NativeInputTimeline:
        recognition = getattr(
            getattr(session, "_activity", None), "_audio_recognition", None
        )
        legacy = recognition is not None and not hasattr(
            recognition, "_input_started_at"
        )
        if legacy:
            self._observe_recognition_input(recognition)
        context = getattr(audio_recognition, "_STTPipelineContextVar", None)
        pipeline = context.get(None) if context is not None else None
        if pipeline is None:
            recognition = getattr(
                getattr(session, "_activity", None), "_audio_recognition", None
            )
            pipeline = getattr(recognition, "_stt_pipeline", None)
        key = pipeline if pipeline is not None else audio
        try:
            timeline = self._timelines.get(key)
        except TypeError:
            return NativeInputTimeline(scope=self.scope, legacy=legacy, source_scope=self.source_scope)
        if timeline is None:
            timeline = NativeInputTimeline(scope=self.scope, legacy=legacy, source_scope=self.source_scope)
            self._timelines[key] = timeline
        return timeline

    def attach_agent(self, agent: Any) -> NativeInputBinding:
        activity = getattr(agent, "_activity", None)
        if (
            self._session is not None
            and activity is not None
            and activity.session is not self._session
        ):
            raise ValueError("Native input binding cannot span AgentSessions")
        previous = _lookup(_agents, agent)
        if (
            previous is not None
            and previous is not self
            and previous._session is not None
            and self._session is not None
            and previous._session is not self._session
            and activity is None
            and previous._agents[agent].active
            and not previous._agents[agent].pending_updates
        ):
            # Older SDKs run on_enter before their session update returns.
            # Release the already-detached Agent even while that old update
            # is still unwinding; its captured pipeline is unaffected.
            previous._detach_agent(agent)
            previous = None
        if previous is not None and previous is not self:
            previous._join(self)
        _agents[agent] = weakref.ref(self)
        hooks = self._agents.setdefault(agent, _AgentHooks())
        if activity is not None:
            hooks.active = True
        if agent.stt_node is not hooks.node:
            original = agent.stt_node

            @wraps(original)
            async def node(audio: Any, settings: Any) -> Any:
                # The SDK may retain this callable after release, then restart
                # it for a local STT model. A closed owner must stay inert.
                binding = self.attach_session(agent.session) if self.owners else None
                timeline = (
                    binding._timeline(audio, agent.session)
                    if binding is not None
                    else None
                )
                # This is the owned SDK pump's task context, inherited by
                # wrapper child tasks. Each node invocation sets its scope.
                if timeline is not None:
                    _active.set(timeline)

                async def observed() -> Any:
                    async for frame in audio:
                        yield (
                            timeline.observe(frame)
                            if timeline is not None
                            and isinstance(frame, rtc.AudioFrame)
                            and binding.owners
                            else frame
                        )

                input_audio, iterator = observed(), None
                try:
                    # Install the active model's instance wrapper fences
                    # before even an outer SDK wrapper creates its stream.
                    for owner in list(binding.owners) if binding is not None else []:
                        owner._bind_native_activity()
                    result = original(input_audio, settings)
                    if inspect.isawaitable(result):
                        result = await result
                    if result is not None:
                        iterator = aiter(result)
                        async for event in iterator:
                            validate_native_final(event, binding)
                            yield event
                finally:
                    try:
                        close = getattr(iterator, "aclose", None)
                        if close is not None:
                            await close()
                    finally:
                        await input_audio.aclose()
                        # Generators may close from a different task. Avoid
                        # resetting its token or clearing another input scope.
                        if _active.get() is timeline:
                            _active.set(None)

            hooks.original_node, hooks.node = original, node
            agent.stt_node = node
        current_update = getattr(agent, "update_options", None)
        if current_update is not None and current_update is not hooks.update:
            original_update = current_update

            @wraps(original_update)
            def update(*args: Any, **kwargs: Any) -> Any:
                if self.owners:
                    self.attach_agent(agent)
                return original_update(*args, **kwargs)

            hooks.original_update, hooks.update = original_update, update
            agent.update_options = update
        return self

    def release(self, owner: Any) -> None:
        self.owners.discard(owner)
        if not self.owners:
            self._restore()

    def _detach_agent(self, agent: Any) -> None:
        hooks = self._agents.pop(agent)
        if agent.stt_node is hooks.node:
            agent.stt_node = hooks.original_node
        if hooks.update is not None and agent.update_options is hooks.update:
            agent.update_options = hooks.original_update
        if _lookup(_agents, agent) is self:
            del _agents[agent]

    def _restore(self) -> None:
        self._restore_recognition_inputs()
        if self._session_audio_enabled is not None:
            input_io, original, enabled = self._session_audio_enabled
            if input_io.set_audio_enabled is enabled:
                input_io.set_audio_enabled = original
            self._session_audio_enabled = None
        if self._session_forward is not None:
            original, forward = self._session_forward
            if self._session._forward_audio_task is forward:
                self._session._forward_audio_task = original
            source = self._session.input.audio
            if isinstance(source, _SourceInput) and source.binding is self:
                self._session.input._audio_stream = source.source
            self._session_forward = None
        if self._session_update is not None:
            original, update = self._session_update
            if self._session._update_activity is update:
                self._session._update_activity = original
            if _lookup(_sessions, self._session) is self:
                del _sessions[self._session]
            self._session_update = None
        for agent in list(self._agents):
            self._detach_agent(agent)


def bind_native_input(
    owner: Any, *, agent: Any = None, session: Any = None
) -> NativeInputBinding:
    binding = (
        _lookup(_sessions, session) if session is not None else _lookup(_agents, agent)
    )
    if (
        agent is not None
        and binding is not None
        and binding._session is not None
        and getattr(agent, "_activity", None) is None
        and binding._agents[agent].active
        and not binding._agents[agent].pending_updates
    ):
        binding._detach_agent(agent)
        binding = None
    if binding is None:
        binding = NativeInputBinding()
    binding.owners.add(owner)
    if session is not None:
        binding = binding.attach_session(session)
    if agent is not None:
        binding = binding.attach_agent(agent)
    return binding
