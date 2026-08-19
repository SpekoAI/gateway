"""Shared configuration mapping for Python voice framework integrations."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any, Literal

CredentialSource = Literal["auto", "byok", "managed"]


def execution_from_env(
    credential_source: CredentialSource = "auto",
) -> dict[str, str]:
    """Resolve an explicit credential source or infer it for compatibility."""

    if credential_source not in {"auto", "byok", "managed"}:
        raise ValueError("credential_source must be 'auto', 'byok', or 'managed'")

    managed = credential_source == "managed"
    if credential_source == "auto":
        managed = bool(
            os.environ.get("SPEKO_API_KEY", "").strip()
            or os.environ.get("SPEKO_API_KEY_FILE", "").strip()
        )
    if managed:
        return {
            "provider_route": "auto",
            "credential_source": "managed",
            "relay_policy": "allowed",
        }
    return {
        "provider_route": "provider_direct",
        "credential_source": "byok",
        "relay_policy": "forbidden",
    }


def stt_options_payload(
    *,
    diarization: bool | None = None,
    keywords: Sequence[str] | None = None,
    noise_reduction: bool | None = None,
    provider_options: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Build the request's ``stt`` block, or None when nothing was asked."""

    options: dict[str, Any] = {}
    if diarization is not None:
        options["diarization"] = diarization
    if keywords:
        options["keywords"] = [str(keyword) for keyword in keywords]
    if noise_reduction is not None:
        options["noise_reduction"] = noise_reduction
    if provider_options:
        options["provider_options"] = {
            provider: dict(settings) for provider, settings in provider_options.items()
        }
    return options or None


__all__ = ["CredentialSource", "execution_from_env", "stt_options_payload"]
