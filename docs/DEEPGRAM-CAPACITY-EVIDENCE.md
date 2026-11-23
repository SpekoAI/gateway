# Deepgram STT capacity refusals

`deepgram.STTCapacityRefusedAt(err)` tells a Go caller that Deepgram refused
to open a speech-to-text WebSocket because the project is at its rate limit.
It is for programs that embed the Deepgram adapter and track provider
capacity. The code is in `providers/deepgram/stt_capacity.go`.

## How it works

The function returns a timestamp and `true` only when the failed handshake
had both:

- HTTP status 429; and
- a complete JSON body with exactly one `err_code` field equal to
  `TOO_MANY_REQUESTS`, as in
  [Deepgram's STT errors](https://developers.deepgram.com/docs/errors).

The timestamp is taken when the response headers arrive. The usual
`runtime.ProviderError` (code, status, retryability, safe message) is still
reachable through `errors.As`, and wrapping the error keeps the observation.

Everything else returns `false`: a body over 1024 bytes or cut short, a read
error, duplicate JSON fields, a redirect, any other 429, payment or quota
errors, transport failures, and text-to-speech errors.

## Rules

- The observation proves that the provider was contacted. It does not prove
  zero usage, and it does not permit a retry or a fallback.
- Only the timestamp is exposed. Provider messages, request IDs, bodies and
  credentials are not.
- A caller cannot create the observation by building a `ProviderError`.
- [Deepgram rate limits](https://developers.deepgram.com/reference/api-rate-limits)
  apply per project, not per API key. Rotating keys inside one project adds
  no capacity.
