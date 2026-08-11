// Package rime implements the provider-direct Rime TTS adapter.
//
// It targets Rime's flagship JSON WebSocket, /ws3 on users-ws.rime.ai. Rime
// publishes three sockets and one HTTP endpoint; /ws3 is the only surface that
// carries every current model, word-level timestamps, context IDs, and — per
// Rime's own docs — the endpoint whose time-to-first-byte they actively
// optimize. /ws (raw binary) has no timestamps, no context IDs, and no
// structured errors; /ws2 is frozen at Mist v1/v2; the HTTP endpoint
// (POST https://users.rime.ai/v1/rime-tts) is one request per utterance and
// pays a fresh TLS handshake for every turn a voice agent takes.
//
// # Barge-in
//
// Rime documents a real cancel verb. {"operation":"clear"} discards the
// accumulated text buffer without synthesizing it, which is exactly the
// interruption primitive a voice agent needs, and it is mapped to Cancel. This
// is worth stating because it is not universal: some vendors document no
// cancel at all, leaving "drop the socket" as the only barge-in, which throws
// away a warm connection on every interruption. Rime does not have that
// problem. The one caveat is scope — clear empties the *buffer*; audio already
// being synthesized from an earlier flush still arrives, and its `done` still
// follows. Callers must stop playback locally rather than assume the wire goes
// quiet.
//
// # Segmentation
//
// The socket's `segment` query parameter decides when synthesis fires. This
// adapter defaults to segment=never, which Rime itself recommends for
// production voice agents: nothing is synthesized until the client sends
// {"operation":"flush"}, and each flush produces exactly one `done`. That maps
// one-to-one onto the ProviderStream contract — AppendText accumulates,
// CommitText flushes, one utterance yields one audio.done — and it removes the
// punctuation heuristics that make segment=bySentence mis-fire on "2.5ml" or
// "Dr.". It also makes context IDs deterministic: Rime tags audio with
// whichever context ID was current *at the time audio was requested*, so under
// bySentence an early sentence can come back tagged null, while under never no
// audio is requested until after the ID has been set.
//
// # eos
//
// Rime also documents {"operation":"eos"}: synthesize whatever remains in the
// buffer, emit `done`, then close the connection. This adapter does not use
// it. Close waits only for an in-flight flush's `done` and then performs a
// normal WebSocket closure, because `done` after eos is documented only "for
// any content remaining in the buffer" — on an empty buffer there may be no
// `done` at all, and Close would block until its context deadline. Text that
// was appended but never committed is therefore discarded at Close, which is
// consistent with CommitText being the utterance boundary in this interface.
//
// # Timestamps
//
// Alignment events are language-gated upstream. Rime emits `timestamps` only
// when lang is en/eng or es/spa (or is omitted); every other language gets
// `chunk` and `done` with no timestamps event and no error. Nothing in this
// adapter waits on an alignment event, and callers must not either.
//
// # Credentials
//
// Rime is bearer-only. See accessToken for why BYOK, managed, and the relay
// (RouteSpekoRelay) all share one code path: every source sends its key as
// `Authorization: Bearer` on the handshake. The relay arm additionally
// accepts credential kind relay_access alongside bearer, because the relay
// connector synthesizes plans that bypass protocol.SessionPlan.Validate and
// label the connector's permanent key bearer, while validated relay plans
// must label it relay_access.
package rime
