// Package hopper is the relay's integration for Hopper, which serves NVIDIA's
// Nemotron ASR and Qwen3-TTS on its own inference: the nemotron-asr
// transcription model over a streaming socket and a prerecorded upload, and
// qwen3-tts over an HTTP synthesis endpoint. One API key (sk_hopper_…), sent
// as an Authorization bearer header, serves all three.
//
// # Wire facts this package is built on
//
// Taken from docs.withhopper.com (stt, tts, errors, models) and confirmed
// against the live API with a staging key on 2026-10-13:
//
//   - Streaming transcription is wss://api.withhopper.com/stt/websocket with
//     model, sample_rate and turn_detection in the query and the key in the
//     handshake header (an api_key query parameter is the fallback for
//     clients that cannot set headers, and is not used). sample_rate is one of
//     8000, 16000, 22050, 24000, 44100 or 48000; audio is binary PCM16 LE
//     mono frames, 100 ms in Hopper's example. turn_detection is balanced
//     (the default), patient or responsive.
//   - The server sends {"type":"Ready","stream_id":N} once after connecting,
//     so Open waits for it; stream_id becomes the provider request id. A bad
//     key is refused at the upgrade with HTTP 401. Transcript events carry
//     no type: {text, is_final, audio_start, audio_end} in seconds, plus
//     timestamp, audio_timestamp, stream_id and frames_processed, which are
//     not read. A partial (is_final false) holds the turn's transcript so far
//     and is replaced by the next event; is_final true is the turn's last
//     event and alone carries words [{text, start, end, conf}]. One utterance
//     can yield several finals, one per turn. Text and word text start with a
//     space. conf is reserved and always 0. Text is lowercase and
//     unpunctuated.
//   - There is no finalize: the server's turn detection ends turns. The
//     client sends the text frame END to flush; the server answers END once
//     every final has gone out, and the client then closes. A final flushed
//     this way carries no words, audio_start or audio_end, so every timing
//     is optional.
//   - Streaming bills the audio bytes received, settling every 60 s and at
//     close. A balance that reaches zero mid-session closes the socket 1008.
//     A refused handshake answers with the REST error envelope. No idle
//     timeout or lead limit is documented, so the adapter sends no keepalive
//     and does not pace; real-time pacing is the caller's.
//   - nemotron-asr transcribes English only and takes no language
//     parameter, so a tag other than en is refused at Open.
//   - Prerecorded transcription is POST /v1/audio/transcriptions, multipart
//     with `file` (PCM16 mono or WAV, at most 25 MB, compressed audio
//     refused), `model` and `sample_rate`. The answer is {text, words,
//     duration_seconds}, and the x-hopper-request-cost-usd header carries
//     the request's exact cost (0.000084 for 6.08 s, $0.05 an hour).
//   - Synthesis is POST /v1/audio/speech with {model, voice, input,
//     response_format, stream}. With response_format pcm and stream true the
//     body is headerless 24 kHz mono s16le under Content-Type audio/pcm,
//     written as it is generated. `input` is at most 1000 characters
//     (400 transcript_too_long). speed and every other generation parameter
//     are refused with 400 unsupported_parameter, so none is sent. `voice` is
//     required; ids come from GET /voices?scope=featured and are lowercase
//     names (elena, marcus, grace, all English). First audio arrives in about
//     0.7 to 0.8 s.
//   - Errors are {"error":{message, type, code, param}}: 401 invalid_api_key
//     or api_key_disabled, 402 insufficient_quota, 429 rate_limit_exceeded
//     (with Retry-After), 413 body_too_large, 502/503/504 for an unreachable,
//     offline or slow model server, and a family of 400 validation codes. No
//     response carries a request id.
//
// Hopper has no token exchange, so there is no short-lived credential for a
// managed provider-direct plan: the key is used on BYOK plans and, labelled
// relay_access, on the relay route.
//
// Billing: Hopper bills transcription on input audio and synthesis on
// generated audio. Neither the socket nor synthesis reports usage on the wire,
// so streaming transcription reports the PCM it sent as its duration observation, completed by
// the server's END reply; the observation Ready opens stays incomplete, so a
// socket that dies first settles as unresolved rather than free. Uploads
// report duration_seconds and, from the cost header, provider_cost_usd.
// Synthesis reports the PCM it received, completed by a clean end of body.
// Both byte counts are exact fractions of a second.
//
// No route is routable until its live canary passes.
package hopper
