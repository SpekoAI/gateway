// Package munsit is the relay's integration for Munsit, an Arabic speech
// platform: the munsit and munsit-en-ar transcription models over a streaming
// socket and a prerecorded upload, and the faseeh-v1-preview voice over an
// HTTP synthesis endpoint. One API key, sent in an x-api-key header (not an
// Authorization bearer), serves all three.
//
// # Wire facts this package is built on
//
// Taken from Munsit's API docs and confirmed against the live API on
// 2026-10-07:
//
//   - Streaming transcription is wss://api.munsit.com/api/v1/listen with
//     encoding=linear16, sample_rate, model, language and interim_results in
//     the query and the key in the handshake header. sample_rate is 8000 or
//     16000 and nothing else; audio is mono. The client sends raw PCM as
//     binary frames of 20 to 200 ms, {"type":"KeepAlive"}, and
//     {"type":"CloseStream"}. There is no Finalize: the socket answers one
//     with a recoverable Error 4002 "unknown control type".
//   - On open the socket sends Metadata {session_id, model, sample_rate, …}
//     before any audio, so Open waits for it and a refused parameter fails
//     there. A turn then reads SpeechStarted, a run of Results with
//     is_final false whose transcript is CUMULATIVE for the turn, one Results
//     with is_final and speech_final true, then UtteranceEnd, Gender and
//     Sentiment. Gender and Sentiment are classifications the relay does not
//     carry and are dropped.
//   - The endpointer only advances on audio it receives: a turn whose audio
//     stops without trailing silence stays open until CloseStream, which
//     flushes its final, sends Metadata {audio_seconds_billed, turn_count}
//     and closes 1000 "engine closed".
//   - Error {code, message, recoverable} can arrive at any time. A
//     non-recoverable one is followed by a close whose status mirrors it:
//     1008 for authentication, the session limit or a low wallet, 1011 for an
//     internal failure or 12 s without input, 4002 for a bad parameter, 4008
//     for audio more than 60 s ahead of real time. A KeepAlive every 5 s of
//     silence holds an idle socket open.
//   - The listen Metadata names the serving build: munsit reports munsit-v2
//     and munsit-en-ar reports munsit-en-ar-v1. munsit takes only language ar;
//     munsit-en-ar takes ar, en, ar-en or auto and code-switches between them.
//   - Prerecorded transcription is POST /api/v1/audio/transcribe, multipart
//     with `file`, `model` and, for munsit-en-ar only, `language`. The answer
//     is {statusCode, data:{transcriptionId, transcription, duration,
//     timestamps:[{word,start,end}], stats:{creditsConsumed}}, message}.
//     munsit-en-ar returns the transcription with a leading space. Files must
//     be under 60 minutes.
//   - Synthesis is POST /api/v1/text-to-speech/faseeh-v1-preview with
//     {voice_id, text, stability, streaming, sample_rate}. The 200 body is
//     headerless chunked PCM16 mono at the requested rate, under
//     Content-Type audio/raw;codec=pcm16;rate=N;channels=1. Rates from 8000 to
//     48000 work; first audio arrives in about 0.7 s. Text as short as "ok"
//     synthesizes, although the docs ask for three words.
//   - Every voice reads Arabic and English. ar-najdi-male-2 is the Arabic
//     default and Ly3XBDAK8rmxpRAzDZ3zcwYY ("Eric (V2)") the English one.
//     `dialect` (auto, emirati, fusha) is left unset, which is auto.
//   - REST errors are JSON {errorCode, errorMessage}; the docs spell the text
//     `message` and add statusCode. errorCode is 40001 validation, 40101
//     authentication, 40201 insufficient balance, 42901 concurrency (HTTP
//     429), and 50001 or 50202 upstream failure. No response carries a
//     request id header.
//
// Munsit has no token exchange, so there is no short-lived credential for a
// managed provider-direct plan: the key is used on BYOK plans and, labelled
// relay_access, on the relay route.
//
// Billing: streaming transcription reports audio_seconds_billed in its closing
// Metadata, which becomes the session's duration observation; the opening
// Metadata leaves an incomplete one, so a socket that dies before the closing
// frame settles as unresolved rather than free. Prerecorded transcription
// reports creditsConsumed. Synthesis bills 2 credits per character of the text
// sent, whitespace included and nothing trimmed (checked against the wallet),
// and reports that as its credit quantity.
//
// No route is routable until its live canary passes.
package munsit
