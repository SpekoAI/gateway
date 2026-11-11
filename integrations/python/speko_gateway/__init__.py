"""Framework integrations for the local Speko Gateway."""

from .client import CanonicalEvent, GatewayClient, GatewayError, GatewaySession
from .relay import (
    AdmissionPreparer,
    AdmissionsUnsupported,
    EvaluationAnswer,
    EvaluationQuestion,
    EvaluationResponse,
    PreparedAdmission,
    PreparedPresentation,
    ReasoningEffort,
    RelayEvaluationClient,
    RelayLLMClient,
)

__all__ = [
    "AdmissionPreparer",
    "AdmissionsUnsupported",
    "CanonicalEvent",
    "EvaluationAnswer",
    "EvaluationQuestion",
    "EvaluationResponse",
    "GatewayClient",
    "GatewayError",
    "GatewaySession",
    "PreparedAdmission",
    "PreparedPresentation",
    "ReasoningEffort",
    "RelayEvaluationClient",
    "RelayLLMClient",
]
