"""Framework integrations for the local Speko Gateway."""

from .client import CanonicalEvent, GatewayClient, GatewayError, GatewaySession
from .relay import (
    AdmissionPreparer,
    EvaluationAnswer,
    EvaluationQuestion,
    EvaluationResponse,
    PreparedAdmission,
    ReasoningEffort,
    RelayEvaluationClient,
    RelayLLMClient,
)

__all__ = [
    "AdmissionPreparer",
    "CanonicalEvent",
    "EvaluationAnswer",
    "EvaluationQuestion",
    "EvaluationResponse",
    "GatewayClient",
    "GatewayError",
    "GatewaySession",
    "PreparedAdmission",
    "ReasoningEffort",
    "RelayEvaluationClient",
    "RelayLLMClient",
]
