"""LiveKit Agents STT, TTS, and LLM implementations backed by Speko.

STT and TTS stream through the colocated local Speko Gateway socket. LLM
talks HTTPS to the hosted Speko Router (`router.speko.dev`) — a different
trust boundary: the conversation content travels to the relay.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import logging
import uuid
import weakref
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

import aiohttp
from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    NOT_GIVEN,
    Agent,
    AgentSession,
    APIConnectionError,
    APIConnectOptions,
    NotGivenOr,
    llm,
    stt,
    tts,
    utils,
    vad,
)
from livekit.agents.llm.tool_context import get_raw_function_info
from livekit.agents.utils import is_given

from ._livekit_bridge import (
    LiveKitSpeechEvent,
    LiveKitSTTBridge,
    LiveKitSTTStream,
    LiveKitTTSBridge,
    LiveKitTTSEvent,
    LiveKitTTSStream,
)
from ._livekit_draining_vad import DrainingVAD
from ._livekit_local_transcript import bind_local_transcript, restore_local_transcript
from ._livekit_native_input import (
    NativeInputBinding,
    bind_native_input,
    native_input_timeline,
)
from ._livekit_native_speaker import NativeSpeechData, bind_native_speaker_timing
from ._livekit_native_stt import NativeSpeechStream
from ._livekit_native_transcript import (
    bind_native_transcript,
    restore_native_transcript,
)
from ._livekit_native_turns import NativeTurnPolicy
from ._livekit_tts_text import NativeTextBuffer
from ._speech_client import SpeechClient
from ._voice import CredentialSource, stt_options_payload, tts_options_payload
from .client import GatewayClient, GatewayError
from .probe import report_leg as _report_probe_leg
from .probe import report_marker as _report_probe_marker
from .relay import (
    ReasoningEffort,
    RelayError,
    RelayLLMClient,
    validate_reasoning_effort,
)
from .relay_speech import RelaySpeechClient

__all__ = ["LLM", "STT", "TTS", "DrainingVAD"]


def _speech_connect_options(
    client: SpeechClient, options: APIConnectOptions
) -> APIConnectOptions:
    # Some supported LiveKit releases retry STT regardless of error.retryable;
    # outer chunked synthesis can also replay input. Fence every framework loop.
    return (
        dataclasses.replace(options, max_retry=0)
        if isinstance(client, RelaySpeechClient)
        else options
    )


_logger = logging.getLogger(__name__)


def _report_stream_leg(kind: str, stream: Any) -> None:
    """Attach a Gateway stream's leg identity to the active probe, if any.

    Strictly a no-op without an active probe; the getattr defaults keep it a
    no-op for test fakes that carry no Gateway session identifiers.
    """

    _report_probe_leg(
        kind,
        session_id=str(getattr(stream, "session_id", "") or ""),
        attempt_id=str(getattr(stream, "attempt_id", "") or ""),
    )


def _speech_data(event: LiveKitSpeechEvent, language: str) -> NativeSpeechData:
    data = NativeSpeechData(
        language=event.language or language,
        text=event.text,
        speaker_id=event.speaker_id,
        start_time=event.start_time or 0.0,
        end_time=event.end_time or 0.0,
    )
    data.speaker_segments = tuple(_speech_data(part, language) for part in event.speaker_segments)
    data.canonical_text = event.canonical_text
    data._local_part_text = event.text
    return data


# STTCapabilities fields added after livekit-agents 1.4.5: keyterms and
# chat_context in 1.6.5, manual_flush in 1.8.9.
_NEWER_STT_CAPABILITIES = frozenset({"keyterms", "chat_context", "manual_flush"})

# Providers for which LiveKit may flush the stream at the end of the user's
# turn (livekit-agents >= 1.8.9 `manual_flush`: VAD end of speech, a turn
# detector's end-of-turn verdict, or commit_user_turn). The flush becomes
# audio.commit, which the Soniox adapter sends as Soniox's `finalize`: the final
# then arrives a few hundred milliseconds after the user stops instead of when
# Soniox's own endpointer fires, which can hold a turn for its whole 2 s
# maximum endpoint delay. Other providers and "auto" (route unknown here) keep
# waiting for the provider's endpointer: a commit is a no-op on Deepgram Flux
# (so commit_user_turn would skip the silence LiveKit otherwise pushes), drops
# a sub-50 ms AssemblyAI tail, ends input on Gladia and Gemini, and
# transcribes a buffered batch on Hamsa. Known cost: when commit_user_turn
# flushes audio that holds no new words, Soniox answers with an empty final,
# which this plugin does not forward, so LiveKit waits out its
# transcript_timeout. Without the flush it already waited that long unless a
# final had arrived in the previous 0.5 s.
_FLUSH_ON_TURN_END_PROVIDERS = frozenset({"soniox"})


def _stt_capabilities(**declared: Any) -> stt.STTCapabilities:
    """Build STTCapabilities, omitting newer fields the installed release lacks.

    Both newer fields default to False and nothing in a release that predates
    them reads them, so omitting them changes no behavior; keywords still reach
    the Gateway through the STT options payload. Any other unknown field still
    raises, so a typo is not silently dropped.
    """

    known = {field.name for field in dataclasses.fields(stt.STTCapabilities)}
    return stt.STTCapabilities(
        **{
            name: value
            for name, value in declared.items()
            if name in known or name not in _NEWER_STT_CAPABILITIES
        }
    )


class STT(stt.STT):
    """Stream speech through a colocated Speko Gateway."""

    def __init__(
        self,
        client: SpeechClient | None = None,
        *,
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        credential_source: CredentialSource = "auto",
        sample_rate: int = 16_000,
        diarization: bool | None = None,
        keywords: Sequence[str] | None = None,
        noise_reduction: bool | None = None,
        provider_options: Mapping[str, Mapping[str, Any]] | None = None,
        turn_vad: vad.VAD | None = None,
    ) -> None:
        if isinstance(client, RelaySpeechClient) and turn_vad is None:
            raise ValueError(
                "Native relay STT requires a local turn_vad for speech boundaries"
            )
        self._turn_vad = turn_vad if isinstance(client, RelaySpeechClient) else None
        self._native_failed = False
        # Options are declared here and enforced at session create: a provider
        # that cannot honor a canonical ask refuses the session with the option
        # named (`stt_option_unsupported`), so the capabilities declared below
        # are honest for every session that actually opens. With provider
        # "auto", routing may refuse an ask nondeterministically — pin the
        # provider for deterministic behavior.
        self._stt_options = stt_options_payload(
            diarization=diarization,
            keywords=keywords,
            noise_reduction=noise_reduction,
            provider_options=provider_options,
        )
        super().__init__(
            capabilities=_stt_capabilities(
                streaming=True,
                interim_results=True,
                diarization=bool(diarization),
                aligned_transcript=False,
                offline_recognize=True,
                keyterms=bool(keywords),
                chat_context=False,
                manual_flush=provider.strip().lower() in _FLUSH_ON_TURN_END_PROVIDERS
                and not isinstance(client, RelaySpeechClient),
            )
        )
        self._client = client or GatewayClient.from_env()
        self._owns_client = client is None
        self._language = language
        self._model = model
        self._provider_name = provider
        self._credential_source = credential_source
        self._sample_rate = sample_rate
        self._streams: weakref.WeakSet[stt.RecognizeStream] = weakref.WeakSet()
        self._bound_agent: Agent | None = None
        self._bound_session: AgentSession | None = None
        self._turn_binding: tuple[Any, Any, Any] | None = None
        self._native_transcript_binding: tuple[Any, Any, Any] | None = None
        self._turn_listener: Any = None
        self._native_wrappers: dict[int, tuple[Any, Any, Any]] = {}
        self._native_input_binding: NativeInputBinding | None = None
        self._native_deferred_turn: tuple[Any, bool] | None = None
        self._native_turn_rearmed = False
        self._native_reconsider_scheduled = False
        self._native_turn_policy: NativeTurnPolicy | None = None
        self._local_transcript_bindings: weakref.WeakSet[Any] = weakref.WeakSet()

    def prewarm(self) -> None:
        if not isinstance(self._client, RelaySpeechClient):
            bind_local_transcript(self)

    def bind_agent(self, agent: Agent) -> None:
        """Locate the owned session through its initial Agent at stream start.

        The binding follows that session across Agent handoffs. Public Agent
        lifecycle hooks remain untouched, including turns that skip a reply.
        """
        if not isinstance(self._client, RelaySpeechClient):
            return
        if self._bound_agent is not None or self._bound_session is not None:
            raise ValueError("Native STT is already bound to an AgentSession")
        self._bound_agent = agent
        self._native_input_binding = bind_native_input(self, agent=agent)

    def bind_session(self, session: AgentSession) -> None:
        """Bind native turn ownership before starting the supplied session."""
        if not isinstance(self._client, RelaySpeechClient):
            return
        if self._bound_agent is not None or self._bound_session is not None:
            raise ValueError("Native STT is already bound to an AgentSession")
        self._bound_session = session
        self._native_input_binding = bind_native_input(self, session=session)

    def _restore_turn_binding(self) -> None:
        restore_native_transcript(self)
        if self._native_turn_policy is not None:
            self._native_turn_policy.close()
            self._native_turn_policy = None
        self._native_deferred_turn = None
        self._native_turn_rearmed = False
        if self._turn_binding is not None:
            activity, original, completed = self._turn_binding
            if activity.on_end_of_turn is completed:
                activity.on_end_of_turn = original
            if getattr(activity, "_speko_native_stt_turn_owner", None) is self:
                del activity._speko_native_stt_turn_owner
            self._turn_binding = None

    def _owns_native_recognition(self, candidate: Any) -> bool:
        """Resolve SDK wrappers without granting automatic model failover."""
        visited: set[int] = set()

        def owns(model: Any) -> bool:
            if model is self:
                return True
            if id(model) in visited:
                return False
            visited.add(id(model))
            if isinstance(model, stt.MultiSpeakerAdapter):
                # Older supported SDKs expose only _stt; newer ones also
                # provide wrapped_stt. The adapter forwards this same stream.
                if not owns(getattr(model, "wrapped_stt", model._stt)):
                    return False
                if (
                    model._suppress_background
                    or model._primary_format != "{text}"
                    or model._background_format != "{text}"
                ):
                    raise ValueError(
                        "Native STT turn ownership requires a transcript-preserving MultiSpeakerAdapter"
                    )
                self._fence_native_wrapper(model)
                return True
            if isinstance(model, stt.FallbackAdapter):
                models = model._stt_instances
                if not any(owns(child) for child in models):
                    return False
                if len(models) != 1:
                    raise ValueError(
                        "Native STT model fallback requires a control-plane recovery decision"
                    )
                self._fence_native_wrapper(model)
                return True
            return False

        return owns(candidate)

    def _fence_native_wrapper(self, wrapper: Any) -> None:
        if id(wrapper) in self._native_wrappers:
            return
        original = wrapper.stream

        def stream(*args: Any, **kwargs: Any) -> Any:
            # Some wrappers construct their child asynchronously. Fence the
            # wrapper's synchronous entry too, so nested fallback recovery
            # cannot hide an ambiguous native outcome in an SDK retry loop.
            self._check_native_admission()
            result = original(*args, **kwargs)
            if isinstance(wrapper, stt.MultiSpeakerAdapter):
                bind_native_speaker_timing(result)
            return result

        wrapper.stream = stream
        self._native_wrappers[id(wrapper)] = (wrapper, original, stream)

    def _bind_native_activity(self, *, require_owner: bool = False) -> None:
        if self._bound_session is None:
            try:
                assert self._bound_agent is not None
                self._bound_session = self._bound_agent.session
                self._bound_agent = None
            except RuntimeError:
                raise ValueError(
                    "Native STT streaming requires a running AgentSession"
                ) from None
        # current_agent can announce a handoff target before its activity is
        # active. The actual session activity owns recognition and its commits.
        activity = getattr(self._bound_session, "_activity", None)
        if activity is None:
            raise ValueError(
                "Native STT streaming requires a running AgentSession"
            )
        if not self._owns_native_recognition(activity.stt):
            # Another native model may be bound to this same session for a
            # later Agent. Only the active recognition owner can wrap its
            # turn callback; passive listeners must never wrap each other.
            self._restore_turn_binding()
            if require_owner:
                raise ValueError(
                    "Native STT requires the active recognition model or a supported wrapper"
                )
            return
        restore_local_transcript(activity)
        if self._turn_listener is None:
            # A retained pipeline can publish while its SDK consumer is
            # detached and the previous activity is still current. Observe
            # the session's synchronous handoff and transcript-consumption
            # events too, before the receiving activity can commit that final.
            def observe_activity(_event: Any) -> None:
                if getattr(self._bound_session, "_activity", None) is not None:
                    self._bind_native_activity()
                    if self._native_turn_policy is not None:
                        asyncio.get_running_loop().call_soon(self._native_turn_policy.changed.set)
                    self._schedule_native_reconsider()

            self._turn_listener = observe_activity
            self._bound_session.on("conversation_item_added", observe_activity)
            self._bound_session.on("user_input_transcribed", observe_activity)
        previous = getattr(activity, "_speko_native_stt_turn_owner", None)
        if previous is not None and previous is not self:
            # update_options swaps recognition on this same activity before
            # the previous model's session listener gets a chance to run.
            # Restore its callback before capturing our own predecessor.
            previous._restore_turn_binding()
        original = getattr(activity, "on_end_of_turn", None)
        if self._turn_binding is not None:
            bound, _, completed = self._turn_binding
            if bound is activity and original is completed:
                if getattr(activity, "_audio_recognition", None) is not None:
                    bind_native_transcript(self, activity._audio_recognition)
                if self._native_turn_policy is not None:
                    self._native_turn_policy.refresh()
                return
        self._restore_turn_binding()
        original = getattr(activity, "on_end_of_turn", None)
        if not callable(original) or inspect.iscoroutinefunction(original):
            raise ValueError("Unsupported native AgentSession turn interface")

        def committed(info: Any) -> bool:
            if self._native_failed:
                return False
            if any(
                isinstance(stream, NativeSpeechStream)
                and not stream.permits_framework_commit(info.new_transcript)
                for stream in list(self._streams)
            ):
                previous = self._native_deferred_turn
                self._native_deferred_turn = (
                    activity,
                    info.skip_reply
                    or (
                        previous[1]
                        if previous is not None and previous[0] is activity
                        else False
                    ),
                )
                self._native_turn_rearmed = False
                return False
            previous = self._native_deferred_turn
            if (
                previous is not None
                and previous[0] is activity
                and previous[1]
                and not info.skip_reply
            ):
                info = dataclasses.replace(info, skip_reply=True)
            accepted = original(info)
            if accepted is True:
                self._native_deferred_turn = None
                self._native_turn_rearmed = False
                # AudioRecognition clears its transcript synchronously after
                # this callback returns. Waking the waiter cannot overtake it.
                for stream in list(self._streams):
                    if isinstance(stream, NativeSpeechStream):
                        stream.acknowledge_turn(info.new_transcript)
            return accepted

        activity.on_end_of_turn = committed
        activity._speko_native_stt_turn_owner = self
        self._turn_binding = (activity, original, committed)
        if getattr(activity, "_audio_recognition", None) is not None:
            bind_native_transcript(self, activity._audio_recognition)
            self._native_turn_policy = NativeTurnPolicy(activity._audio_recognition)

    async def _native_continuation_delay(
        self, transcript: str, gap: float
    ) -> float | None:
        if self._native_turn_policy is None:
            return None
        return await self._native_turn_policy.delay_for(transcript, gap)

    def _schedule_native_reconsider(self) -> None:
        if self._native_failed:
            return
        if (
            self._native_deferred_turn is not None
            and not self._native_turn_rearmed
            and not self._native_reconsider_scheduled
        ):
            self._native_reconsider_scheduled = True
            asyncio.get_running_loop().call_soon(self._reconsider_native_turn)

    def _reconsider_native_turn(self) -> None:
        self._native_reconsider_scheduled = False
        deferred = self._native_deferred_turn
        if (
            deferred is None
            or self._native_turn_rearmed
            or getattr(self._bound_session, "_activity", None) is not deferred[0]
        ):
            return
        recognition = deferred[0]._audio_recognition
        if recognition._closing.is_set():
            return
        transcript = recognition._audio_transcript
        if not transcript or any(
            isinstance(stream, NativeSpeechStream)
            and not stream.permits_framework_commit(transcript)
            for stream in list(self._streams)
        ):
            return
        if self._native_turn_policy is not None:
            self._native_turn_rearmed = True
            self._native_turn_policy.reconsider(skip_reply=deferred[1])

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "speko"

    async def recognize(
        self,
        buffer: Any,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> stt.SpeechEvent:
        return await super().recognize(
            buffer,
            language=language,
            conn_options=_speech_connect_options(self._client, conn_options),
        )

    async def _recognize_impl(
        self,
        buffer: Any,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions,
    ) -> stt.SpeechEvent:
        stream = self._stream(
            language=language, conn_options=conn_options, recognize=True
        )
        try:
            frames = buffer if isinstance(buffer, list) else [buffer]
            for frame in frames:
                stream.push_frame(frame)
            stream.end_input()
            final: stt.SpeechEvent | None = None
            segments: list[stt.SpeechData] = []
            canonical_parts: list[str] = []
            async for event in stream:
                if event.type == stt.SpeechEventType.FINAL_TRANSCRIPT:
                    segments.extend(event.alternatives)
                    data = event.alternatives[0]
                    canonical = getattr(data, "canonical_text", None)
                    if canonical is None:
                        canonical = data.text
                    if canonical:
                        canonical_parts.append(canonical)
                    final = event
            if final is None:
                raise APIConnectionError("Speko STT ended without a final transcript")
            if segments:
                if len(segments) > 1:
                    final = stt.SpeechEvent(
                        type=stt.SpeechEventType.FINAL_TRANSCRIPT,
                        request_id=final.request_id,
                        alternatives=[NativeSpeechData(
                            language=segments[-1].language,
                            text="",
                            start_time=segments[0].start_time,
                            end_time=segments[-1].end_time,
                        )],
                    )
                final.alternatives[0].text = " ".join(canonical_parts)
                final.alternatives[0].canonical_text = final.alternatives[0].text
                final.alternatives[0].speaker_segments = tuple(segments) if len(segments) > 1 else ()
            return final
        finally:
            await stream.aclose()

    def stream(
        self,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
        manual_turns: bool = False,
    ) -> stt.RecognizeStream:
        return self._stream(
            language=language,
            conn_options=conn_options,
            recognize=False,
            manual_turns=manual_turns,
        )

    def _stream(
        self,
        *,
        language: NotGivenOr[str],
        conn_options: APIConnectOptions,
        recognize: bool,
        manual_turns: bool = False,
    ) -> stt.RecognizeStream:
        self._check_native_admission()
        if (
            isinstance(self._client, RelaySpeechClient)
            and not recognize
            and not manual_turns
            and self._bound_agent is None
            and self._bound_session is None
        ):
            raise ValueError(
                "Native STT streaming requires bind_agent() or bind_session() before AgentSession starts; explicit event consumers use manual_turns=True"
            )
        conn_options = _speech_connect_options(self._client, conn_options)
        resolved_language = str(language) if is_given(language) else self._language
        bridge = LiveKitSTTBridge(
            self._client,
            language=resolved_language,
            model=self._model,
            provider=self._provider_name,
            credential_source=self._credential_source,
            stt_options=self._stt_options,
        )
        if isinstance(self._client, RelaySpeechClient):
            def prepare_native_turn() -> None:
                self._check_native_admission()
                if not recognize and not manual_turns:
                    self._bind_native_activity(require_owner=True)

            if not recognize and not manual_turns:
                self._bind_native_activity(require_owner=True)
            stream = NativeSpeechStream(
                stt_instance=self,
                bridge=bridge,
                language=resolved_language,
                conn_options=conn_options,
                sample_rate=self._sample_rate,
                turn_vad=None if recognize else self._turn_vad,
                mark_failed=lambda: setattr(self, "_native_failed", True),
                report_open=lambda stream: _report_stream_leg("stt", stream),
                check_admission=prepare_native_turn,
                wait_for_turn_commit=not recognize and not manual_turns,
                continuation_delay=self._native_continuation_delay,
                reconsider_turn=self._schedule_native_reconsider,
                input_timeline=native_input_timeline(self._native_input_binding) if not recognize and not manual_turns else None,
            )
        else:
            bind_local_transcript(self)
            stream = SpeechStream(
                stt_instance=self,
                bridge=bridge,
                language=resolved_language,
                conn_options=conn_options,
                sample_rate=self._sample_rate,
            )
        self._streams.add(stream)
        return stream

    def _check_native_admission(self) -> None:
        if isinstance(self._client, RelaySpeechClient) and self._native_failed:
            # LiveKit's outer AgentSession pump can ignore retryable=False.
            # A non-API error stops that recovery loop before another request.
            raise GatewayError(
                "Native Speko STT requires a control-plane recovery decision",
                source="relay",
                code="recovery_required",
                retryable=False,
            )

    async def aclose(self) -> None:
        if self._bound_session is not None and self._turn_listener is not None:
            self._bound_session.off("conversation_item_added", self._turn_listener)
            self._bound_session.off("user_input_transcribed", self._turn_listener)
            self._turn_listener = None
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()
        for binding in list(self._local_transcript_bindings):
            binding.release(self)
        self._local_transcript_bindings.clear()
        self._restore_turn_binding()
        for wrapper, original, stream in self._native_wrappers.values():
            if wrapper.stream is stream:
                wrapper.stream = original
        self._native_wrappers.clear()
        if self._native_input_binding is not None:
            self._native_input_binding.release(self)
            self._native_input_binding = None
        self._bound_agent = None
        self._bound_session = None
        if self._owns_client:
            await self._client.aclose()


class SpeechStream(stt.RecognizeStream):
    def __init__(
        self,
        *,
        stt_instance: STT,
        bridge: LiveKitSTTBridge,
        language: str,
        conn_options: APIConnectOptions,
        sample_rate: int,
    ) -> None:
        super().__init__(
            stt=stt_instance,
            conn_options=conn_options,
            sample_rate=sample_rate,
        )
        self._bridge = bridge
        self._language = language
        self._request_id = utils.shortuuid()
        self._local_group: str | None = None
        self._gateway_stream: LiveKitSTTStream | None = None
        self._speaking = False
        self._saw_final_for_turn = False
        self._awaiting_final = False

    async def _run(self) -> None:
        first_frame = None
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                continue
            first_frame = item
            break
        if first_frame is None:
            return

        try:
            self._gateway_stream = await self._bridge.start(first_frame)
            _report_stream_leg("stt", self._gateway_stream)
            await self._gateway_stream.push_frame(first_frame)
            send_task = asyncio.create_task(
                self._send_audio(), name="speko.livekit.stt.send"
            )
            receive_task = asyncio.create_task(
                self._receive_events(), name="speko.livekit.stt.receive"
            )
            tasks = (send_task, receive_task)
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
                if receive_task not in done:
                    await asyncio.gather(*tasks)
            finally:
                await utils.aio.cancel_and_wait(*tasks)
        except asyncio.CancelledError:
            raise
        except GatewayError as err:
            classification = "/".join(
                value for value in (err.source, err.code) if value
            )
            suffix = f" ({classification})" if classification else ""
            raise APIConnectionError(
                f"Speko Gateway STT failed{suffix}", retryable=err.retryable
            ) from err
        except OSError as err:
            raise APIConnectionError("Speko Gateway STT transport failed") from err
        finally:
            if self._gateway_stream is not None:
                try:
                    await asyncio.shield(self._gateway_stream.aclose())
                except (GatewayError, OSError):
                    pass

    async def _send_audio(self) -> None:
        assert self._gateway_stream is not None
        last_was_flush = False
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                if not last_was_flush:
                    await self._gateway_stream.flush()
                last_was_flush = True
            else:
                await self._gateway_stream.push_frame(item)
                last_was_flush = False
        if not last_was_flush:
            await self._gateway_stream.flush()
        await self._gateway_stream.finish()

    async def _receive_events(self) -> None:
        assert self._gateway_stream is not None
        async for event in self._gateway_stream.events():
            self._emit(event)
        if self._speaking:
            self._emit_end()

    def _data(self, event: LiveKitSpeechEvent) -> NativeSpeechData:
        data = _speech_data(event, self._language)
        if event.type == "transcript.final":
            if self._local_group is None:
                self._local_group = uuid.uuid4().hex
            data._local_group = self._local_group
            data._local_after_eos = self._awaiting_final
            data.canonical_text = event.text if event.canonical_text is None else event.canonical_text
            data._local_terminal = bool(data.canonical_text)
            if data.canonical_text:
                self._local_group = None
        return data

    def _emit(self, event: LiveKitSpeechEvent) -> None:
        if event.provider_request_id:
            self._request_id = event.provider_request_id
        if event.type == "speech.started":
            self._awaiting_final = False
            if not self._speaking:
                self._speaking = True
                self._saw_final_for_turn = False
                self._event_ch.send_nowait(
                    stt.SpeechEvent(
                        type=stt.SpeechEventType.START_OF_SPEECH,
                        request_id=self._request_id,
                    )
                )
            return
        if event.type == "speech.ended":
            if self._speaking:
                if not self._saw_final_for_turn:
                    self._awaiting_final = True
                self._emit_end()
            return
        # A final with no recognized text still completes its turn. Leaving it
        # pending steals the next transcript-led turn's START marker.
        if event.type == "transcript.final" and not event.text:
            self._saw_final_for_turn = True
            self._awaiting_final = False
            return
        if event.type not in {"transcript.delta", "transcript.final"} or not event.text:
            return
        is_final = event.type == "transcript.final"
        data = self._data(event)
        if is_final and self._awaiting_final:
            self._awaiting_final = not bool(data.canonical_text)
            self._saw_final_for_turn = True
            self._event_ch.send_nowait(
                stt.SpeechEvent(
                    type=stt.SpeechEventType.FINAL_TRANSCRIPT,
                    request_id=self._request_id,
                    alternatives=[
                        data
                    ],
                )
            )
            return

        self._awaiting_final = False
        if not self._speaking:
            self._speaking = True
            self._saw_final_for_turn = False
            self._event_ch.send_nowait(
                stt.SpeechEvent(
                    type=stt.SpeechEventType.START_OF_SPEECH,
                    request_id=self._request_id,
                )
            )
        if is_final:
            self._saw_final_for_turn = True
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=(
                    stt.SpeechEventType.FINAL_TRANSCRIPT
                    if is_final
                    else stt.SpeechEventType.INTERIM_TRANSCRIPT
                ),
                request_id=self._request_id,
                alternatives=[
                    data
                ],
            )
        )

    def _emit_end(self) -> None:
        if not self._speaking:
            return
        self._speaking = False
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=stt.SpeechEventType.END_OF_SPEECH,
                request_id=self._request_id,
            )
        )


# LiveKit does not close the TTS when the AgentSession ends, so this bound is
# what releases a finished call's Gateway session. 180 s also stays under the
# default idle timeouts of the NAT gateways on Azure (4 minutes) and AWS (350 s),
# which drop an idle provider connection without telling either end.
_STANDBY_MAX_AGE = 180.0

# Cancelling an open that is under way leaves its Gateway session unattached,
# holding capacity until the Gateway's attach timeout (30 s by default). A
# session closed while it opens waits up to this many seconds, far longer than
# an open takes, so it can open and then close.
_OPEN_GRACE = 10.0


@dataclasses.dataclass(frozen=True)
class _OpenSession:
    stream: LiveKitTTSStream
    events: AsyncIterator[LiveKitTTSEvent]
    next_event: asyncio.Task[LiveKitTTSEvent | None] | None


class _TTSSession:
    """One Gateway TTS session. A standby is one opened with ``options``.

    The Gateway reports the end of an idle session only as an event on its
    socket, so a standby reads one event ahead, and no reply takes a standby
    that is no longer quiet.
    """

    def __init__(
        self, bridge: LiveKitTTSBridge, options: dict[str, Any] | None = None
    ) -> None:
        self.options = options
        self._open = asyncio.create_task(
            _open_session(bridge, read_ahead=options is not None),
            name="speko.livekit.tts.open",
        )
        self._open.add_done_callback(_retrieve)

    @property
    def quiet(self) -> bool:
        """Whether nothing, not even an end or an error, has come back yet."""
        if not self._open.done():
            return True
        if self._open.cancelled() or self._open.exception() is not None:
            return False
        next_event = self._open.result().next_event
        return next_event is None or not next_event.done()

    @property
    def opening(self) -> bool:
        return not self._open.done()

    async def wait_opened(self) -> None:
        await asyncio.wait({self._open})

    async def stream(self) -> LiveKitTTSStream:
        return (await self._open).stream

    async def events(self) -> AsyncIterator[LiveKitTTSEvent]:
        opened = await self._open
        if opened.next_event is not None:
            event = await opened.next_event
            if event is None:
                return
            yield event
        async for event in opened.events:
            yield event

    async def aclose(self) -> None:
        await asyncio.wait({self._open}, timeout=_OPEN_GRACE)
        await utils.aio.cancel_and_wait(self._open)
        if self._open.cancelled() or self._open.exception() is not None:
            return
        opened = self._open.result()
        if opened.next_event is not None:
            await utils.aio.cancel_and_wait(opened.next_event)
        try:
            await opened.stream.aclose()
        except (GatewayError, OSError):
            pass


async def _open_session(bridge: LiveKitTTSBridge, *, read_ahead: bool) -> _OpenSession:
    stream = await bridge.start()
    events = stream.events()
    next_event = None
    if read_ahead:
        next_event = asyncio.create_task(
            _next_event(events), name="speko.livekit.tts.read_ahead"
        )
        next_event.add_done_callback(_retrieve)
    return _OpenSession(stream, events, next_event)


async def _next_event(
    events: AsyncIterator[LiveKitTTSEvent],
) -> LiveKitTTSEvent | None:
    async for event in events:
        return event
    return None


def _retrieve(task: asyncio.Task[Any]) -> None:
    """Consume a task's failure so asyncio does not report it as unhandled.

    A caller that awaits the task still gets the error.
    """
    if not task.cancelled() and task.exception() is not None:
        _logger.debug(
            "Speko Gateway TTS session task failed", exc_info=task.exception()
        )


class TTS(tts.TTS):
    """Synthesize speech through a Speko Gateway or native speech relay.

    ``prewarm()`` and each reply open up to ``standby_sessions`` Gateway
    sessions in standby, so the next reply's first text does not wait for a
    session to open. A standby that no reply takes within three minutes closes.
    With ``standby_sessions=0``, each reply opens its session at its first text.
    The default is one standby for GatewayClient and zero for RelaySpeechClient;
    native speech preopens provider admissions only with explicit opt-in.
    """

    def __init__(
        self,
        client: SpeechClient | None = None,
        *,
        voice: str = "",
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        sample_rate: int = 24_000,
        max_input_characters: int = 100_000,
        credential_source: CredentialSource = "auto",
        provider_options: Mapping[str, Mapping[str, Any]] | None = None,
        standby_sessions: int | None = None,
    ) -> None:
        if standby_sessions is not None and standby_sessions < 0:
            raise ValueError("standby_sessions must not be negative")
        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=True),
            sample_rate=sample_rate,
            num_channels=1,
        )
        self._client = client or GatewayClient.from_env()
        if standby_sessions is None:
            standby_sessions = 0 if isinstance(self._client, RelaySpeechClient) else 1
        self._owns_client = client is None
        self._voice = voice
        self._language = language
        self._model = model
        self._provider_name = provider
        self._max_input_characters = max_input_characters
        self._credential_source = credential_source
        self._tts_options = tts_options_payload(provider_options=provider_options)
        self._streams: weakref.WeakSet[SynthesisStream] = weakref.WeakSet()
        self._standby_sessions = standby_sessions
        self._standbys: dict[_TTSSession, asyncio.TimerHandle] = {}
        self._closing: set[asyncio.Task[None]] = set()
        self._closed = False

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "speko"

    def prewarm(self) -> None:
        """Open the standby sessions before the first reply needs one."""
        self._fill_standbys()

    def update_options(
        self,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        voice: NotGivenOr[str] = NOT_GIVEN,
        provider_options: NotGivenOr[
            Mapping[str, Mapping[str, Any]] | None
        ] = NOT_GIVEN,
    ) -> None:
        """Change the language, voice or provider settings of the next stream.

        Every LiveKit reply speaks on its own Gateway session, so a multilingual
        agent can follow the caller turn by turn: pass the language the STT
        final reports, and an empty voice lets Speko pick one for it.
        ``provider_options`` replaces the previous settings; None clears them.
        Streams already speaking keep their settings. A change also replaces
        each standby session opened with the old settings.
        """
        if is_given(language):
            self._language = language
        if is_given(voice):
            self._voice = voice
        if is_given(provider_options):
            self._tts_options = tts_options_payload(provider_options=provider_options)
        options = self._session_options()
        if any(session.options != options for session in self._standbys):
            self._fill_standbys()

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> tts.ChunkedStream:
        return self._synthesize_with_stream(
            text, conn_options=_speech_connect_options(self._client, conn_options)
        )

    def stream(
        self,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> SynthesisStream:
        conn_options = _speech_connect_options(self._client, conn_options)
        options = self._session_options()
        stream = SynthesisStream(
            tts_instance=self,
            bridge=LiveKitTTSBridge(self._client, **options),
            options=options,
            conn_options=conn_options,
        )
        self._streams.add(stream)
        return stream

    async def aclose(self) -> None:
        self._closed = True
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()
        await self._release_standbys()
        if self._owns_client:
            await self._client.aclose()

    def _session_options(self) -> dict[str, Any]:
        return {
            "voice": self._voice,
            "language": self._language,
            "model": self._model,
            "provider": self._provider_name,
            "sample_rate": self.sample_rate,
            "num_channels": self.num_channels,
            "max_input_characters": self._max_input_characters,
            "credential_source": self._credential_source,
            "tts_options": self._tts_options,
        }

    def _take_standby(self, options: dict[str, Any]) -> _TTSSession | None:
        session = next(
            (s for s in self._standbys if s.options == options and s.quiet), None
        )
        if session is not None:
            self._standbys.pop(session).cancel()
        return session

    def _fill_standbys(self) -> None:
        options = self._session_options()
        for session in [
            s for s in self._standbys if s.options != options or not s.quiet
        ]:
            self._retire(session)
        while not self._closed and len(self._standbys) < self._standby_sessions:
            session = _TTSSession(LiveKitTTSBridge(self._client, **options), options)
            self._standbys[session] = asyncio.get_running_loop().call_later(
                _STANDBY_MAX_AGE, self._retire, session
            )

    def _retire(self, session: _TTSSession) -> None:
        timer = self._standbys.pop(session, None)
        if timer is not None:
            timer.cancel()
            self._close_soon(session)

    async def _release_standbys(self) -> bool:
        held = bool(self._standbys or self._closing)
        for session in list(self._standbys):
            self._retire(session)
        if self._closing:
            await asyncio.wait(self._closing)
        return held

    def _close_soon(self, session: _TTSSession) -> asyncio.Task[None]:
        task = asyncio.create_task(session.aclose(), name="speko.livekit.tts.close")
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)
        task.add_done_callback(_retrieve)
        return task


class SynthesisStream(tts.SynthesizeStream):
    def __init__(
        self,
        *,
        tts_instance: TTS,
        bridge: LiveKitTTSBridge,
        options: dict[str, Any],
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(tts=tts_instance, conn_options=conn_options)
        self._plugin = tts_instance
        self._bridge = bridge
        self._options = options
        self._session: _TTSSession | None = None
        self._gateway_stream: LiveKitTTSStream | None = None
        self._native_utterance_done = asyncio.Event()
        self._native_text_finished = False
        self._native_segment_open = False

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        # The emitter must be initialized even when no text ever arrives:
        # the base class calls end_input() on it unconditionally after _run
        # returns, and an uninitialized emitter raises there.
        output_emitter.initialize(
            request_id=utils.shortuuid(),
            sample_rate=self._tts.sample_rate,
            num_channels=self._tts.num_channels,
            mime_type="audio/pcm",
            stream=True,
        )
        first_token: str | None = None
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                continue
            first_token = item
            break
        if first_token is None:
            return

        self._session = None
        try:
            _report_probe_marker("tts.requested")
            await self._open_for_text(first_token)
            send_task = asyncio.create_task(
                self._send_text(first_token, output_emitter), name="speko.livekit.tts.send"
            )
            receive_task = asyncio.create_task(
                self._receive_audio(output_emitter), name="speko.livekit.tts.receive"
            )
            try:
                done, _ = await asyncio.wait(
                    (send_task, receive_task), return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()
                if receive_task not in done:
                    await receive_task
            finally:
                await utils.aio.cancel_and_wait(send_task, receive_task)
        except asyncio.CancelledError:
            raise
        except GatewayError as err:
            classification = "/".join(
                value for value in (err.source, err.code) if value
            )
            suffix = f" ({classification})" if classification else ""
            raise APIConnectionError(
                f"Speko Gateway TTS failed{suffix}", retryable=err.retryable
            ) from err
        except OSError as err:
            raise APIConnectionError("Speko Gateway TTS transport failed") from err
        finally:
            if self._session is not None:
                closing = self._plugin._close_soon(self._session)
                if not self._session.opening:
                    await asyncio.shield(closing)

    async def _open_for_text(self, first_token: str) -> None:
        standby = self._plugin._take_standby(self._options)
        if standby is not None:
            self._session = standby
            await standby.wait_opened()
            if standby.quiet:
                self._plugin._fill_standbys()
                self._attach_stream(await standby.stream())
                if not isinstance(self._plugin._client, RelaySpeechClient):
                    await self._gateway_stream.append_text(first_token)
                return
            self._plugin._close_soon(standby)
        self._session = _TTSSession(self._bridge)
        try:
            stream = await self._session.stream()
        except GatewayError as error:
            # A retryable refusal, such as the Gateway's session limit, can come
            # from a standby that is still opening or closing, so free them and
            # try once more. Other refusals surface at once, credit_exhausted
            # included: a closed standby frees its slot when it closes, but its
            # credit only when the Gateway's batched usage report settles it.
            # Native opens admit bounded provider work; even an error marked
            # retryable cannot prove that the original admission did not commit.
            if isinstance(self._plugin._client, RelaySpeechClient) or not (
                error.retryable and await self._plugin._release_standbys()
            ):
                raise
            self._session = _TTSSession(self._bridge)
            stream = await self._session.stream()
        # The next standby opens only now, so it cannot take the Gateway slot
        # this reply's own open needs.
        self._plugin._fill_standbys()
        self._attach_stream(stream)
        if not isinstance(self._plugin._client, RelaySpeechClient):
            await stream.append_text(first_token)

    def _attach_stream(self, stream: LiveKitTTSStream) -> None:
        self._gateway_stream = stream
        _report_stream_leg("tts", stream)
        self._mark_started()

    async def _send_text(self, first_token: str, output_emitter: tts.AudioEmitter) -> None:
        assert self._gateway_stream is not None
        if not isinstance(self._plugin._client, RelaySpeechClient):
            # The local Gateway accepts streaming append immediately. Preserve
            # its token/flush contract; native speech starts each utterance only
            # on commit and therefore needs sentence commits below.
            pending = True
            async for item in self._input_ch:
                if isinstance(item, self._FlushSentinel):
                    if pending:
                        await self._gateway_stream.commit()
                    pending = False
                else:
                    await self._gateway_stream.append_text(item)
                    pending = True
            if pending:
                await self._gateway_stream.commit()
            await self._gateway_stream.finish()
            return
        text = NativeTextBuffer()
        pieces = utils.aio.Chan()

        def push(token: str) -> None:
            for piece in text.push(token):
                pieces.send_nowait(piece)

        def flush() -> None:
            if remaining := text.flush():
                pieces.send_nowait(remaining)

        async def feed() -> None:
            incoming = None
            try:
                push(first_token)
                incoming = asyncio.create_task(self._input_ch.__anext__())
                while True:
                    # Coalesce an ambiguous final period across LLM tokens,
                    # while still speaking "Hello." if its producer pauses.
                    # Keep the read task alive when the lookahead expires;
                    # cancelling a channel read can discard later text.
                    done, _ = await asyncio.wait(
                        [incoming], timeout=0.1 if text.can_flush_idle else None
                    )
                    if not done:
                        flush()
                        continue
                    try:
                        item = incoming.result()
                    except StopAsyncIteration:
                        break
                    if isinstance(item, self._FlushSentinel):
                        flush()
                        pieces.send_nowait(item)
                    else:
                        push(item)
                    incoming = asyncio.create_task(self._input_ch.__anext__())
            finally:
                if incoming is not None:
                    await utils.aio.cancel_and_wait(incoming)
                flush()
                pieces.close()

        feeding = asyncio.create_task(feed(), name="speko.livekit.tts.text")
        try:
            async for piece in pieces:
                if isinstance(piece, self._FlushSentinel):
                    self._end_native_segment(output_emitter)
                    continue
                await self._gateway_stream.append_text(piece)
                # Preserve separators on the wire without asking a provider
                # to synthesize an otherwise empty whitespace-only context.
                if not piece.strip():
                    continue
                self._native_utterance_done.clear()
                await self._gateway_stream.commit()
                # Native adapters may own a single active provider context.
                # Feed future LLM text concurrently, but do not append the
                # next utterance or close while this one is still generating.
                await self._native_utterance_done.wait()
            await feeding
            self._end_native_segment(output_emitter)
            self._native_text_finished = True
            await self._gateway_stream.finish()
        finally:
            await utils.aio.cancel_and_wait(feeding)

    def _end_native_segment(self, output_emitter: tts.AudioEmitter) -> None:
        if self._native_segment_open:
            output_emitter.end_segment()
            self._native_segment_open = False

    async def _receive_audio(self, output_emitter: tts.AudioEmitter) -> None:
        assert self._session is not None
        # Internal sentence commits share an SDK segment. An explicit input
        # flush ends it only after the current provider utterance has drained.
        segment_open = False
        segment_done = False
        native = isinstance(self._plugin._client, RelaySpeechClient)
        first_audio_reported = False
        async for event in self._session.events():
            if segment_done:
                continue
            if event.type == "audio.started":
                if not (self._native_segment_open if native else segment_open):
                    if native:
                        self._native_segment_open = True
                    else:
                        segment_open = True
                    output_emitter.start_segment(
                        segment_id=event.provider_request_id or utils.shortuuid()
                    )
                continue
            if event.type == "audio.frame" and event.audio is not None:
                if not first_audio_reported:
                    first_audio_reported = True
                    _report_probe_marker("tts.first_audio")
                if not (self._native_segment_open if native else segment_open):
                    if native:
                        self._native_segment_open = True
                    else:
                        segment_open = True
                    output_emitter.start_segment(segment_id=utils.shortuuid())
                output_emitter.push(event.audio)
                continue
            if event.type == "audio.done":
                if native:
                    if self._native_segment_open:
                        output_emitter.flush()
                    self._native_utterance_done.set()
                elif segment_open:
                    segment_open = False
                    segment_done = True
                    output_emitter.end_segment()
        if native and not self._native_text_finished:
            raise GatewayError("Relay speech ended before text completed", retryable=False)
        if native:
            self._end_native_segment(output_emitter)
        elif segment_open:
            output_emitter.end_segment()


class LLM(llm.LLM):
    """Generate responses through the hosted Speko Router.

    `reasoning_effort` controls how much the model reasons before answering.
    Use `"none"` to turn reasoning off. `"minimal"` asks for the least the
    routed model allows (the Router maps it per provider), and `"low"` through
    `"xhigh"` ask for more. The default, None, sends no field and keeps the
    provider's default. While reasoning is enabled (any value but `"none"`)
    the Router refuses `temperature`, so leave temperature unset then.

    `"none"` is accepted on models that cannot reason, where it changes nothing
    (older Routers refuse it there, e.g. on `gpt-4.1`). Four models always
    reason and refuse `"none"`, with `"minimal"` as their floor: Inception
    mercury-voice, gpt-oss-120b on Cerebras and Together, and Together
    zai-org/GLM-5.3-Flash. Every other value, `"minimal"` included, needs a
    model that can reason: the Router refuses it for a pinned model that cannot,
    and auto routing considers only models that can.
    """

    def __init__(
        self,
        client: RelayLLMClient | None = None,
        *,
        provider: str = "auto",
        model: str = "auto",
        objective: str = "balanced",
        max_output_tokens: int = 8_192,
        temperature: float | None = None,
        reasoning_effort: ReasoningEffort | None = None,
    ) -> None:
        super().__init__()
        if (provider == "auto") != (model == "auto"):
            raise ValueError("provider and model must both be auto or both be explicit")
        reasoning_effort = validate_reasoning_effort(reasoning_effort)
        self._client = client or RelayLLMClient.from_env()
        self._owns_client = client is None
        self._provider_name = provider
        self._model = model
        self._objective = objective
        self._max_output_tokens = max_output_tokens
        self._temperature = temperature
        self._reasoning_effort = reasoning_effort

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "speko"

    def chat(
        self,
        *,
        chat_ctx: llm.ChatContext,
        tools: list[llm.Tool] | None = None,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
        parallel_tool_calls: NotGivenOr[bool] = NOT_GIVEN,
        tool_choice: NotGivenOr[Any] = NOT_GIVEN,
        extra_kwargs: NotGivenOr[dict[str, Any]] = NOT_GIVEN,
    ) -> RelayLLMStream:
        if isinstance(self._client, RelayLLMClient) and self._client._native_socket:
            conn_options = dataclasses.replace(conn_options, max_retry=0)
        return RelayLLMStream(
            llm_instance=self,
            client=self._client,
            chat_ctx=chat_ctx,
            tools=tools or [],
            conn_options=conn_options,
        )

    def _request_body(
        self, chat_ctx: llm.ChatContext, tools: list[llm.Tool]
    ) -> dict[str, Any]:
        if self._provider_name == "auto":
            routing: dict[str, Any] = {
                "mode": "auto",
                "objective": self._objective,
            }
        else:
            routing = {
                "mode": "explicit",
                "provider": self._provider_name,
                "model": self._model,
            }
        body: dict[str, Any] = {
            "routing": routing,
            "input": _relay_input(chat_ctx),
            "max_output_tokens": self._max_output_tokens,
        }
        if tools:
            body["tools"] = _relay_tools(tools)
        if self._temperature is not None:
            body["temperature"] = self._temperature
        if self._reasoning_effort is not None:
            body["reasoning_effort"] = self._reasoning_effort
        return body

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
        await super().aclose()


class RelayLLMStream(llm.LLMStream):
    def __init__(
        self,
        *,
        llm_instance: LLM,
        client: RelayLLMClient,
        chat_ctx: llm.ChatContext,
        tools: list[llm.Tool],
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(
            llm_instance,
            chat_ctx=chat_ctx,
            tools=tools,
            conn_options=conn_options,
        )
        self._relay_llm = llm_instance
        self._client = client

    async def _run(self) -> None:
        request = self._relay_llm._request_body(self._chat_ctx, self._tools)
        if not request["input"]:
            raise APIConnectionError(
                "Speko Router LLM requires at least one text message in the"
                " chat context",
                retryable=False,
            )
        response_id = utils.shortuuid()
        _report_probe_marker("llm.requested")
        first_token_reported = False
        events = self._client.stream_response(request)
        try:
            async for event, payload in events:
                if event == "response.created":
                    response_id = str(payload.get("response_id", response_id))
                    continue
                if event == "response.text.delta":
                    delta = str(payload.get("delta", ""))
                    if delta:
                        if not first_token_reported:
                            first_token_reported = True
                            _report_probe_marker("llm.first_token")
                        self._event_ch.send_nowait(
                            llm.ChatChunk(
                                id=response_id,
                                delta=llm.ChoiceDelta(role="assistant", content=delta),
                            )
                        )
                    continue
                if event == "response.item.completed":
                    item = payload.get("item")
                    if isinstance(item, dict) and item.get("type") == "function_call":
                        if not first_token_reported:
                            first_token_reported = True
                            _report_probe_marker("llm.first_token")
                        self._event_ch.send_nowait(
                            llm.ChatChunk(
                                id=response_id,
                                delta=llm.ChoiceDelta(
                                    role="assistant",
                                    tool_calls=[
                                        llm.FunctionToolCall(
                                            name=str(item.get("name", "")),
                                            arguments=str(
                                                item.get("arguments") or "{}"
                                            ),
                                            call_id=str(item.get("call_id", "")),
                                        )
                                    ],
                                ),
                            )
                        )
                    continue
                if event == "response.completed":
                    _report_probe_marker("llm.completed", ok=True)
                    usage = payload.get("usage")
                    if isinstance(usage, dict):
                        self._event_ch.send_nowait(
                            llm.ChatChunk(
                                id=response_id, usage=_completion_usage(usage)
                            )
                        )
        except RelayError as err:
            _report_probe_marker("llm.completed", ok=False)
            suffix = f" ({err.code})" if err.code else ""
            raise APIConnectionError(
                f"Speko Router LLM failed{suffix}", retryable=err.retryable
            ) from err
        except (aiohttp.ClientError, OSError) as err:
            _report_probe_marker("llm.completed", ok=False)
            raise APIConnectionError("Speko Router LLM transport failed") from err
        finally:
            await events.aclose()


def _relay_input(chat_ctx: llm.ChatContext) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in chat_ctx.items:
        if item.type == "message":
            role = "system" if item.role == "developer" else item.role
            if role not in ("system", "user", "assistant"):
                continue
            text = item.text_content
            if not text:
                continue
            items.append(
                {
                    "type": "message",
                    "role": role,
                    "content": [{"type": "text", "text": text}],
                }
            )
        elif item.type == "function_call":
            items.append(
                {
                    "type": "function_call",
                    "call_id": item.call_id,
                    "name": item.name,
                    "arguments": item.arguments or "{}",
                }
            )
        elif item.type == "function_call_output":
            entry: dict[str, Any] = {
                "type": "function_result",
                "call_id": item.call_id,
            }
            if item.output:
                entry["result"] = item.output
            items.append(entry)
    return items


def _relay_tools(tools: list[llm.Tool]) -> list[dict[str, Any]]:
    declarations: list[dict[str, Any]] = []
    for tool in tools:
        if llm.is_function_tool(tool):
            schema = llm.utils.build_legacy_openai_schema(tool, internally_tagged=True)
            raw: dict[str, Any] = {
                "name": schema["name"],
                "description": schema.get("description") or "",
                "parameters": schema.get("parameters"),
            }
        elif llm.is_raw_function_tool(tool):
            info = get_raw_function_info(tool)
            raw = {
                "name": info.raw_schema.get("name", info.name),
                "description": info.raw_schema.get("description") or "",
                "parameters": info.raw_schema.get("parameters"),
            }
        else:
            raise ValueError(
                "Speko Router LLM supports function tools only; got "
                f"{type(tool).__name__}"
            )
        declaration: dict[str, Any] = {"name": raw["name"]}
        if raw["description"]:
            declaration["description"] = raw["description"]
        if raw["parameters"] is not None:
            declaration["parameters"] = raw["parameters"]
        declarations.append(declaration)
    return declarations


def _completion_usage(usage: dict[str, Any]) -> llm.CompletionUsage:
    input_tokens = int(usage.get("input_tokens", 0))
    cached = int(usage.get("cached_input_tokens", 0))
    output_tokens = int(usage.get("output_tokens", 0))
    reasoning = int(usage.get("reasoning_tokens", 0))
    prompt_tokens = (
        input_tokens
        + cached
        + int(usage.get("cache_write_5m_tokens", 0))
        + int(usage.get("cache_write_1h_tokens", 0))
    )
    completion_tokens = output_tokens + reasoning
    return llm.CompletionUsage(
        completion_tokens=completion_tokens,
        prompt_tokens=prompt_tokens,
        prompt_cached_tokens=cached,
        total_tokens=prompt_tokens + completion_tokens,
    )
