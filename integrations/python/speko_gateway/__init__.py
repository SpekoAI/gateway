"""Framework integrations for the local Speko Gateway."""

from .client import CanonicalEvent, GatewayClient, GatewayError, GatewaySession
from .relay import (
    EvaluationAnswer,
    EvaluationQuestion,
    EvaluationResponse,
    ReasoningEffort,
    RelayEvaluationClient,
    RelayLLMClient,
)
from .relay_realtime import RelayRealtimeClient, RelayRealtimeSession
from .relay_speech import RelaySpeechClient, RelaySpeechSession
from .speech import RelayTTSClient

__all__ = [
    "CanonicalEvent",
    "EvaluationAnswer",
    "EvaluationQuestion",
    "EvaluationResponse",
    "GatewayClient",
    "GatewayError",
    "GatewaySession",
    "ReasoningEffort",
    "RelayEvaluationClient",
    "RelayLLMClient",
    "RelayRealtimeClient",
    "RelayRealtimeSession",
    "RelaySpeechClient",
    "RelaySpeechSession",
    "RelayTTSClient",
]
