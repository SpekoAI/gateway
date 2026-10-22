// Package mistral is the relay's integration for Mistral's Voxtral 3 speech
// models: voxtral-mini-transcribe-realtime-3-0 over a realtime socket,
// voxtral-mini-transcribe-3-0 and voxtral-small-transcribe-3-0 over a
// prerecorded upload, and voxtral-mini-tts-3-0 over an HTTP synthesis
// endpoint. One API key, sent as an Authorization bearer header, serves all
// four.
//
// # Wire facts this package is built on
//
// Confirmed against the live API with a staging key on 2026-10-21, the day
// after the 3.0 family launched in public preview:
//
//   - Realtime transcription is
//     wss://api.mistral.ai/v1/audio/transcriptions/realtime?model=<id> with
//     the key in the handshake header. A bad key is refused at the upgrade
//     with 401 {"detail":"Invalid API Key"}; a model the key cannot open
//     with 403 and an empty body. Every frame is JSON text.
//   - The server opens with session.created, whose session.request_id is
//     the provider request id. The client answers session.update with
//     audio_format {encoding, sample_rate}, target_streaming_delay_ms,
//     language, context_bias and turn_detection; the server acknowledges
//     with session.updated, and settings lock once audio starts, so Open
//     waits for the acknowledgement. pcm_s16le at 8000, 16000, 24000, 44100
//     or 48000 Hz is accepted; the delay takes 80 to 1200 ms or exactly 2400
//     and anything else is an error frame (code 3051), as is a language
//     outside the 58 the model lists.
//   - Audio is base64 PCM in input_audio.append, at most 262,144 decoded
//     bytes a frame. transcription.text.delta frames are append-only
//     fragments that split words and carry their own spacing. With
//     turn_detection set (an empty object takes the defaults) the socket
//     also sends turn.start, turn.eager_end, turn.resume and turn.end, each
//     with time_s on the audio clock. There is no confidence and no word
//     timing.
//   - turn.* runs on the audio clock while text lags it by up to the
//     streaming delay: at 480 ms a turn's last words arrived 10 to 240 ms
//     after its turn.end, at 1200 ms up to 800 ms after, and punctuation
//     marks no turn's end. A turn's final therefore goes out once the delay
//     plus 150 ms has passed since turn.end; deltas until then belong to the
//     ended turn, even after the next turn.start, whose speech.started
//     follows that final.
//   - input_audio.flush finalizes the whole stream, not the turn: audio
//     sent after it is ignored. There is no per-turn commit, so CommitAudio
//     sends nothing, as with Meta. Close sends input_audio.flush then
//     input_audio.end; transcription.done follows with the session's whole
//     text and usage.prompt_audio_seconds, a whole number of seconds,
//     floored. An open turn is published as final there.
//   - Failures arrive as {"type":"error","error":{message, code}}; message
//     is a string or a validation object and code a number.
//   - Prerecorded transcription is POST /v1/audio/transcriptions, multipart
//     with `file`, `model`, optional `language`, `diarize=true`,
//     `timestamp_granularities=segment` (word granularity is refused for
//     3.0) and one `context_bias` field per phrase. The answer is {text,
//     segments [{text, start, end, speaker_id}], usage} with language null;
//     a 3.5 s clip took about a second. The Mistral-Correlation-Id header
//     is the request id.
//   - Synthesis is POST /v1/audio/speech with {model, input, voice_id,
//     response_format, stream}. voice_id takes a preset slug (en_paul_neutral)
//     or a voice UUID; an unknown one is 404 invalid_voice, a missing one
//     400. speed is refused with 422 extra_forbidden, so it is never sent.
//     With response_format pcm and stream true the body is text/event-stream:
//     speech.audio.delta events carry base64 raw float32 little-endian mono
//     at 24 kHz, about 0.4 s each, the first after about 0.3 s, and
//     speech.audio.done ends the utterance with token usage. The adapter
//     converts the floats to s16le. An 8,000-character input was accepted.
//   - REST errors are {"object":"error", message, type, param, code,
//     raw_status_code}, except a refused key: 401 {"detail":"Invalid API
//     Key"}. Moderation refuses synthesis with 403.
//
// Mistral has no token exchange, so there is no short-lived credential for a
// managed provider-direct plan: the key is used on BYOK plans and, labelled
// relay_access, on the relay route.
//
// Billing: Mistral bills transcription on input audio and synthesis on input
// characters. The socket and the upload report whole seconds, floored, so
// both report the PCM they sent as their duration observation instead: the
// socket's is completed by transcription.done, and the observation Open
// queues stays incomplete, so a socket that dies first settles as
// unresolved rather than free. Synthesis reports no observation; the relay
// meters the characters it sent.
//
// No route is routable until its live canary passes.
package mistral
