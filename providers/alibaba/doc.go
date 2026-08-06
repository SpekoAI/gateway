// Package alibaba implements the provider-direct Alibaba Cloud DashScope
// (Model Studio / Qwen) streaming STT and TTS adapters.
//
// Both adapters speak the SAME WebSocket resource, /api-ws/v1/realtime, and
// select their model with a `model` query parameter. DashScope modelled that
// resource on the OpenAI Realtime event schema: JSON frames with a `type`
// discriminator, `session.update` for configuration, `*_buffer.append` for
// streaming input, and `session.finish` to close. Nothing else in DashScope
// works this way — the older Qwen-Audio-TTS/CosyVoice and Fun-ASR/Paraformer
// families use a completely different run-task/continue-task/finish-task
// protocol on /api-ws/v1/inference, and the Qwen-ASR batch family uses HTTP.
// Those are different adapters; this package is only the realtime pair.
//
// # Hosts: international, mainland, and workspace-scoped
//
// DashScope splits its estate by region and the split is load bearing, because
// an API key issued in one region is not valid in the other. Four host forms
// are documented:
//
//   - dashscope-intl.aliyuncs.com          Singapore (international)
//   - dashscope.aliyuncs.com               China (Beijing), mainland
//   - {WorkspaceId}.ap-southeast-1.maas.aliyuncs.com   Singapore, new form
//   - {WorkspaceId}.cn-beijing.maas.aliyuncs.com       Beijing, new form
//
// InternationalAPIHost is the only host allowed by default. Everything else,
// including the mainland host in MainlandAPIHost and any workspace-scoped
// host, has to be named in Config.AllowedEndpointHosts, so a session plan
// cannot silently point a customer credential at an unexpected region. The
// workspace-scoped hosts in particular are per tenant, so they can only ever
// reach the policy as configuration.
//
// Alibaba recommends migrating from the bare hosts to the workspace-scoped
// ones for performance, and states the bare hosts "remain fully functional".
// The bare international host stays the default here because it is the one
// form both the STT and the TTS reference name, and the only one that needs no
// per-tenant configuration.
//
// # A documentation contradiction worth knowing
//
// The English "WebSocket API for Qwen-TTS real-time synthesis" page prints the
// SAME URL, wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime, under both
// its "Singapore" and its "China (Beijing)" heading. The Chinese edition of
// the same page correctly prints wss://dashscope.aliyuncs.com/... for Beijing.
// The English page is wrong. Anyone reading only the English TTS reference
// would send Beijing traffic to Singapore and get an authentication failure
// that looks like a bad key. This package treats the Chinese edition as
// correct, which is also what the STT reference independently says.
//
// # One credential, not two
//
// The two credential channels that Cartesia and ElevenLabs need do not exist
// here, and inventing them would be a wire bug rather than caution.
//
// DashScope does mint a short-lived credential. POST /api/v1/tokens with a
// permanent key returns {"token":"st-...","expires_at":<unix>}, valid for 60
// seconds by default and up to 1,800 with expire_in_seconds. Alibaba calls the
// result a "temporary API key" and that is exactly what it is: it inherits the
// minting key's permissions and is presented the same way, as
// `Authorization: Bearer <value>`. There is no separate header, no query
// parameter, and no signature. Both credential sources therefore travel one
// path, and a managed plan differs from a BYOK plan only in whether the value
// starts with st- or sk-.
//
// Alibaba Cloud's general STS service (AssumeRole, AccessKeyId +
// SecretAccessKey + SecurityToken) is NOT accepted here. It authenticates the
// Model Studio management OpenAPI — CreateApiKey, ResetApiKey — not model
// inference. The documented request-header table for both realtime endpoints
// lists exactly one authentication field, Authorization, and no STS material
// has any place to go on this wire.
//
// # Why the two adapters send different headers
//
// STT sends `OpenAI-Beta: realtime=v1` and TTS does not. That asymmetry is
// copied from the vendor, not invented:
//
//   - Every Qwen-ASR-Realtime sample Alibaba publishes (Go, Python, Java, C#,
//     PHP, JavaScript) sets the header. Its reference header table does not
//     list it. The samples are runnable code, so the header is sent.
//   - No Qwen-TTS-Realtime sample sets it and its reference header table does
//     not list it either. Nothing suggests the endpoint wants it, so it is not
//     sent.
//
// X-DashScope-DataInspection is documented as optional and defaults to off.
// Enabling content inspection on customer audio is not a decision an adapter
// should make silently, so the header is never sent.
//
// # STT: what "commit" means here, and why Close is the only flush
//
// The STT adapter always runs in the vendor's VAD mode, where the server
// detects utterance boundaries and finalizes each turn on its own. In that
// mode input_audio_buffer.commit is documented as disabled, so there is no
// per-turn client flush at all: the only end-of-input signal is session.finish,
// and session.finish ends the whole session. The server answers it by
// completing recognition of whatever it still holds, emitting the final
// transcript, then sending session.finished and closing.
//
// CommitAudio and Close therefore converge on one guarded session.finish, the
// same shape the Gladia adapter needs for the same reason. A caller that
// commits has ended the session; further audio is refused rather than written
// to a socket the vendor considers finished. A live multi-turn call needs no
// commit at all, because VAD finalizes every turn by itself.
//
// Interim results arrive as conversation.item.input_audio_transcription.text,
// which splits one hypothesis across TWO fields: `text` is the confirmed
// prefix the model will not revise, and `stash` is the draft suffix it may
// still correct. The complete preview is text + stash. Reading only `text`
// is the subtle failure here — it compiles, it emits deltas, and it silently
// truncates every partial to the confirmed prefix, which is empty for the
// first several frames of an utterance.
//
// Qwen-ASR-Realtime returns no word timestamps at all, so transcript events
// carry no timings. Fun-ASR and Paraformer do, on the other protocol.
//
// # TTS: server_commit, and what cancel can actually do
//
// The TTS session runs in server_commit mode, the vendor's recommended
// default, where the server decides when to synthesize buffered text. Audio
// therefore starts flowing during AppendText rather than waiting for the
// utterance to close. CommitText still maps to input_text_buffer.commit,
// which in this mode means "synthesize everything buffered right now"; the
// session then returns to server_commit. That gives both the latency of
// streaming and a real flush.
//
// Cancel maps to input_text_buffer.clear, the only interrupt the protocol
// defines. It discards text that has not been synthesized yet and cannot
// recall audio the server already generated, so the adapter also drops
// remaining frames locally and emits no audio.done for a cancelled utterance —
// a barge-in that reported completion would be a lie.
//
// Close sends session.finish and waits for session.finished before tearing the
// socket down, because the vendor flushes the tail of the audio in between.
//
// language_type is NOT a BCP-47 tag. DashScope wants English language names —
// "Chinese", "English", "German" — and its documented default, "Auto", detects
// per segment. A portable tag is mapped onto that vocabulary and anything
// outside it falls back to Auto rather than reaching the wire as a code the
// vendor will reject.
package alibaba
