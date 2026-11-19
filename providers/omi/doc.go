// Package omi is the relay's integration for Omi Health, which serves the
// omi-medical-1 transcription model over a realtime socket and a prerecorded
// upload. One API key serves both surfaces.
//
// # Wire facts this package is built on
//
// Taken from docs.omi.health and confirmed against the live US host with an
// evaluation key on 2026-11-06 (build omi-medical-1@9e0248f339dc, region
// us-east-2):
//
//   - Every request goes to api.us.omi.health, Omi's US (Ohio) host. The
//     api.omi.health host picks a region per request and api.eu.omi.health is
//     Frankfurt; neither is accepted, so audio is processed in the US whatever
//     cell the session starts from.
//   - Realtime transcription is wss://api.us.omi.health/v1/stream. The key
//     rides the first text frame, a JSON start frame with session_id, model,
//     streaming_mode realtime, event_schema omi-agent-v1, pcm_s16le at
//     16000 Hz mono, language, diarization and optional endpointing
//     {silence_ms: 200..1100}. Because the key is inside a frame, the dial
//     refuses redirects and the frame never reaches an event, an error or a
//     log. The server answers ready with model.requested and model.served
//     (an unknown model is served silently as omi-medical-1, so a mismatch
//     fails Open), max_session_s (1800) and client_controls. No audio is sent
//     before ready. Any other sample rate is refused at start with
//     UNSUPPORTED_AUDIO_FORMAT, so Open refuses it first.
//   - language "auto" is accepted on the socket and then drops every
//     utterance (notice UTTERANCE_DROPPED, no turn_final), while the audio is
//     still metered. The socket therefore never receives auto: no language
//     means en, and an explicit auto is refused. Uploads take auto.
//   - partial frames come from a draft model and hold the turn's transcript
//     so far, keyed by turn_id; each replaces the last. turn_final is the
//     turn's authoritative text, once per turn. turn_start,
//     turn_end_candidate, turn_continuation, confirmed_complete, finalizing,
//     canonical_revision, finalize_ack, keep_alive_ack,
//     configuration_updated and notice are hints and are not forwarded.
//   - {"type":"finalize","id"} flushes the open turn: turn_final, then
//     finalize_ack, about 50 ms server-side; the session stays open, so
//     CommitAudio maps onto it. keep_alive holds an idle socket open.
//   - {"type":"stop"} ends the session: the server sends canonical_record
//     (a repaired transcript that differs from the finals and is never
//     forwarded, a record.status of complete, incomplete or empty, and
//     session_stats.audio_ms) and closes normally.
//   - A refused start answers {"type":"error","code"} in place of ready and
//     closes 1008: UNAUTHORIZED, UNSUPPORTED_LANGUAGE, UNSUPPORTED_AUDIO_FORMAT.
//     A start beyond the project's stream limit is closed 1013
//     TOO_MANY_STREAMS (or SPEAKER_CAPACITY_FULL) before any audio.
//   - Prerecorded transcription is POST /v1/audio/transcriptions, multipart
//     with file, model, language and response_format, under a bearer key, at
//     most 100 MB. verbose_json always carries duration, the billable seconds
//     (digital silence included); json does not, so verbose_json is always
//     asked for. Words and speakers come back only with diarize=true, and in
//     some languages still without word timings. Inline vocabulary is
//     accepted but not applied, so keywords are refused. The same request can
//     answer 200 inline or 202 with a job to long-poll; a finished job holds
//     its result inline or behind a download_url, which may be a signed
//     storage URL that must not receive the key.
//   - REST errors are {"error":{code, message}}.
//
// Omi has no token exchange, so there is no short-lived credential for a
// managed provider-direct plan: the key is used on BYOK plans and, labelled
// relay_access, on the relay route.
//
// Billing: the socket's observation opens incomplete at ready and completes
// with session_stats.audio_ms from canonical_record, which matched the audio
// sent in every probe run. A socket that ends without canonical_record keeps
// it incomplete, carrying the PCM bytes sent as the estimate, so the attempt
// settles as unresolved rather than free or as if Omi had confirmed it.
// Uploads report verbose_json duration; one without it stays incomplete.
// Neither carries a language: every language bills the same rate.
//
// No route is routable until its live canary passes.
package omi
