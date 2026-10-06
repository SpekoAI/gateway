// Package paxa is the relay's integration for Paxa Labs (Bangkok), whose
// paxa-tts-flash-v1 model speaks Thai and English over an HTTP synthesis
// endpoint. One API key, sent as an Authorization bearer header, serves it.
//
// # Wire facts this package is built on
//
// Taken from paxalabs.com/docs (text-to-speech, streaming, voices, models,
// errors, credits, rate-limits; read 2026-10-06) and confirmed against the
// live API the same day:
//
//   - Synthesis is POST https://api.paxalabs.com/v1/tts with
//     {text, voice, model, format, stream, language}. With format "wav" and
//     stream true the body is a RIFF/WAVE container whose RIFF and data sizes
//     are the 0xFFFFFFFF streaming placeholder, followed by mono 16-bit PCM
//     at 24 kHz, under Content-Type audio/wav. There is no rate parameter.
//   - The OpenAI-compatible POST /v1/audio/speech ignores `language`, so the
//     native endpoint is used.
//   - `language` is "auto", "th", "en" or "zh". It sets how digits, codes and
//     symbols are read; words keep the language of their script. "auto"
//     takes the language most of the text is written in and Thai on a tie.
//     Only th and en are routed here, so any other tag is refused at Open
//     and an empty one is sent as "auto".
//   - `voice` is a case-insensitive id from GET /v1/voices. Every voice
//     reads every language, the other two with a foreign accent, so the
//     default follows the session language: nomyen (the Thai board voice)
//     or cookie (the English board voice).
//   - `text` is at most 5,000 characters, counted as UTF-16 code units.
//   - Errors are RFC 9457 problem details, application/problem+json, with
//     the stable code in `title` and the status mirrored in `status`.
//   - A streamed failure after the first byte truncates the body with no
//     trailer, so a read that ends in anything but EOF is a failed synthesis.
//   - x-request-id on every response is the id Paxa's support is keyed on.
//   - Requests per minute are counted for the whole account; concurrent
//     speech requests are capped per plan. Both answer 429 and are retryable,
//     so the relay fails over.
//
// Each synthesis reports a billing observation in credits: 0.01 credits per
// UTF-16 unit, rounded to the vendor's hundredth-credit increment, with a
// 0.1-credit minimum. Credits cost $10 per 10,000. Completed audio marks the
// observation complete; interrupted bodies leave incomplete evidence for
// reconciliation. Timestamps would bill 1.25x and are never requested.
//
// The route is not routable until its live canary passes.
package paxa
