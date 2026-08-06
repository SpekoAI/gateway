// Package hume implements the provider-direct Hume Octave TTS adapter.
//
// It targets Hume's HTTP streaming synthesis resource,
// POST https://api.hume.ai/v0/tts/stream/json, which returns a stream of JSON
// objects each carrying one base64-encoded audio chunk. The non-streaming twins
// (POST /v0/tts/json and POST /v0/tts/file) buffer the whole utterance before
// the first byte leaves the server, which is latency realtime voice cannot
// spend. The sibling streaming resource /v0/tts/stream/file returns an opaque
// audio byte stream with no per-chunk metadata; the JSON variant is preferred
// because it carries generation_id, snippet_id, and is_last_chunk alongside the
// audio.
//
// # Octave, not EVI
//
// Hume ships two speech products and only one of them belongs here. EVI
// (Empathic Voice Interface, /v0/evi/chat) is a full conversational agent: it
// owns turn taking, runs its own LLM, and consumes microphone audio. Octave
// (/v0/tts/*) is plain synthesis — text in, audio out. This adapter is a TTS
// adapter, so it speaks Octave. An EVI integration would be a realtime-kind
// adapter, not this one.
//
// # Two credential channels
//
// Hume documents two authentication strategies and, unlike some vendors, they
// are carried by two DIFFERENT headers:
//
//   - BYOK: the customer's permanent portal API key travels in the
//     `X-Hume-Api-Key` request header. The TTS OpenAPI document declares this
//     as the resource's only security scheme.
//   - Managed: a short-lived access token travels in the standard
//     `Authorization: Bearer <token>` header.
//
// The access token is minted by the control plane, not by this adapter, with
//
//	POST https://api.hume.ai/oauth2-cc/token
//	Authorization: Basic base64("<api key>:<secret key>")
//	Content-Type: application/x-www-form-urlencoded
//	grant_type=client_credentials
//
// and the `access_token` member of the JSON response is what lands in
// PlanRoute.Credential.Value. Tokens expire after 30 minutes. Hume states that
// only EVI and Text-to-Speech accept token authentication, so this is not an
// EVI-only credential — it covers exactly the resource this adapter calls.
// Minting cannot happen here: it needs the account's Secret key as well as its
// API key, and a DelegatedCredential carries a single opaque value.
//
// # Wire details worth stating once
//
//   - `version` is a STRING enum, `"1"` or `"2"`, not a number. The gateway's
//     model ids `octave-1` and `octave-2` map onto it.
//   - Octave 2 rejects a request that names no voice, and so does instant mode.
//     The adapter therefore always sends a voice.
//   - The body has no language parameter. Octave infers language from the text
//     and the voice, so RequestOptions.Language is deliberately not forwarded.
//   - Output is a fixed 48 kHz. `format` selects only the container
//     (`{"type":"pcm"}`), never a rate, so the adapter validates the plan's
//     media against 48 kHz instead of asking for the caller's rate.
//   - The response is newline-delimited JSON. The OpenAPI document labels the
//     200 response `text/event-stream`, but the description says "a stream of
//     JSON objects" and both official SDKs parse it as plain JSON lines
//     (TypeScript: messageTerminator "\n"; Python: iter_lines + json.loads with
//     no `data:` stripping). The SDKs are treated as authoritative here.
//
// # No cancel verb
//
// The HTTP surface has no cancel or flush message — Cancel and Abort work by
// cancelling the request context, which drops the connection. Hume's WebSocket
// surface (wss://api.hume.ai/v0/tts/stream/input) does define `flush` ("force
// the generation of audio regardless of how much text has been supplied") and
// `close` ("force the generation of audio and close the stream"), but both
// COMMIT pending text rather than discard it, so neither is a barge-in verb.
// On either surface, stopping playback early means dropping the connection.
package hume
