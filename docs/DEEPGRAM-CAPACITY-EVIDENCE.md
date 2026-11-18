# Deepgram STT capacity observations

`deepgram.STTCapacityRefusedAt(err)` recognizes a private adapter-owned error
from a failed STT WebSocket handshake. It requires HTTP 429 and the exact native
`err_code: "TOO_MANY_REQUESTS"` documented in
[Deepgram's STT errors](https://developers.deepgram.com/docs/errors).
The existing `runtime.ProviderError` code, status, retryability and safe message
remain available through `errors.As`.

The timestamp is sampled when the HTTP response headers arrive, before body
reads and cleanup. The WebSocket library's 1024-byte diagnostic body is accepted
only if the original body independently reached EOF without read failure. JSON
must be complete, contain one exact `err_code` and have no duplicate fields.
Oversized or truncated responses, redirects, generic 429s, payment/quota errors,
transport failures and the separate TTS error family supply no observation.
Each open owns its observation; the configured HTTP client is copied.

Only the original timestamp is exposed. Provider messages, request IDs, bodies
and credentials are not part of the observation. Error wrapping preserves it;
constructing a generic `ProviderError` cannot manufacture it.

This observation proves actual provider I/O, and never proves zero usage,
no-provider-I/O or permission to retry/fallback. A router must separately bind it
to the actual selected credential/account, capability, grant and dispatch barrier
and persist it through its durable receipt lifecycle. This SDK change alone does
not publish control receipts or activate platform cooldowns.

[Deepgram's rate-limit documentation](https://developers.deepgram.com/reference/api-rate-limits)
defines limits by project, not individual API key. Account mapping must reflect
the actual shared provider limit; this observation does not create additional
capacity by rotating keys or projects.
