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
    "RelayTTSClient",
]
