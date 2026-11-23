# Cartesia whole-input TTS

`cartesia.NewBytesTTS(cartesia.BytesTTSConfig{})` is a Go adapter that sends a
whole transcript to Cartesia in one HTTP request and streams the audio back.
Its adapter ID is `cartesia.tts.bytes.v1`. The `speko-gateway` command does
not load it: a Go program that embeds the runtime registers it itself. The
code is in `providers/cartesia/tts_bytes.go` and `tts_bytes_prepared.go`.

The streaming WebSocket adapter `cartesia.tts.v1` is separate and unchanged.

## How it works

The adapter calls Cartesia's
[bytes API](https://docs.cartesia.ai/api-reference/tts/bytes) at
`https://api.cartesia.ai/tts/bytes` with API version `2026-08-14`.

| Input | Accepted values |
| --- | --- |
| Model | `sonic-3`, `sonic-3.5`, `sonic-3.6` |
| Audio | Mono `pcm_s16le` at 8000, 16000, 22050, 24000, 44100 or 48000 Hz |
| Voice, language | Required, explicit |
| `normalization`, `speed` | Same per-model rules as the WebSocket adapter. `speed` is sent as `generation_config.speed` |
| `max_buffer_delay_ms` | Refused by name. The bytes request has no buffering field |

- `text.append` collects UTF-8 text up to a local byte limit (64 KiB by
  default) and the request's character ceiling. The text is sent as given,
  with no trimming or normalization by the adapter.
- `text.commit` sends one POST. Each further utterance is another POST with
  its own operation ID.
- Audio frames hold whole PCM samples. `audio.done` and a complete usage
  observation are published only when a nonempty, sample-aligned response
  ends cleanly.
- Cancel and abort interrupt a pending request. Cancel keeps the session open
  for another utterance.
- BYOK and relay keys are reused for the whole session. A managed
  provider-direct credential that has expired is renewed through the
  session's `RefreshCredential` hook before the next POST.

## Prepared input

A caller that must fix the final text before it commits to the request uses
two steps:

1. `adapter.PrepareInput(request, text)` validates the settings and builds
   the exact provider body. It uses no credential and does no I/O.
   `prepared.Text()` returns the final transcript.
2. `adapter.OpenPrepared(ctx, request, prepared)` checks the endpoint and
   credential, requires the request's settings to produce the same bytes, and
   sends those bytes once. No append or commit can replace them or start
   another utterance.

Only a value returned by `PrepareInput` is accepted.

## Rules

- Redirects, compressed or ambiguous responses and unexpected content types
  are refused. Header waits, error-body reads and idle reads are bounded.
- Error messages never contain the provider's response body.
- Usage is the Unicode scalar count of the submitted text, reported as
  `characters` in thousandths, in `streaming` output mode.
- Every POST that may have been sent first reports an incomplete operation
  with unknown quantity. An empty, truncated, oversized, failed or
  interrupted response never becomes complete.
- A failure before the request is sent (credential renewal, body
  preparation) reports an error and no billing operation.
- The character count is not Cartesia credits. Cartesia's
  [pricing](https://docs.cartesia.ai/pricing) says preprocessing can change
  the credit total, and the adapter reports no provider cost or request ID.
- A prepared body and its text stay in memory. Do not store or log them.
- Preparing input is not permission to run it, and it does not prove that
  audio was delivered or heard.
