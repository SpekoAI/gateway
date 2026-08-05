# Security policy

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting for `SpekoAI/gateway`. If that is unavailable,
email `abat@speko.ai` with the repository name, affected version, impact,
and reproduction details.

We will acknowledge a complete report as soon as practical, coordinate a fix
and disclosure with the reporter, and credit researchers who want attribution.
Do not include real customer credentials, audio, transcripts, or personal data
in a report.

## Scope

The latest released minor version is supported. Security-sensitive areas
include plan verification, provider endpoint validation, credential handling,
local authentication, session isolation, telemetry redaction, and container
hardening.

The trust model and the exact data intentionally sent from the gateway are
documented in [docs/TRUST.md](docs/TRUST.md).
