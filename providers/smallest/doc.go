// Package smallest implements the provider-direct Smallest AI (Waves) STT and
// TTS adapters.
//
// Both adapters target the streaming surface:
//
//   - STT: Pulse realtime WebSocket, wss://api.smallest.ai/waves/v1/stt/live
//   - TTS: Lightning streaming WebSocket, wss://api.smallest.ai/waves/v1/tts/live
//
// Neither modality falls back to a batch endpoint. Smallest does publish
// pre-recorded HTTP transcription and a one-shot POST /waves/v1/tts, and it
// publishes an SSE twin of the TTS stream at the same path over HTTPS, but a
// realtime gateway has no reason to buffer a whole utterance when the vendor
// streams.
//
// # Credentials: one permanent account key
//
// Smallest documents exactly one credential for the Waves model APIs: a
// long-lived account API key sent as `Authorization: Bearer <key>` (see
// docs.smallest.ai/models/api-reference/authentication). There is no mint
// endpoint, no query-parameter token, and no session-scoped secret for
// /waves/v1/stt/live or /waves/v1/tts/live.
//
// The single-use `wct_` access token that appears elsewhere in Smallest's docs
// belongs to a DIFFERENT product. It is minted by
// POST https://api.smallest.ai/atoms/v1/conversation/register-call, it is bound
// to an `agent_id`, it lives 30 seconds, and it authenticates the Atoms
// realtime agent socket at wss://api.smallest.ai/atoms/v1. Nothing in the Waves
// documentation accepts it. This is the same per-product trap xAI set, where an
// ephemeral secret covered Speech-to-Speech and not TTS, so both adapters here
// refuse a managed provider-direct credential source outright rather than
// quietly forwarding what would have to be the customer's root API key inside
// a signed plan.
//
// The one managed construction that survives that rule is the Speko relay. A
// RouteSpekoRelay plan is managed for billing purposes but carries the relay
// connector's OWN permanent account key — not a customer secret — and that key
// rides the same `Authorization: Bearer` header as a BYOK key. On the relay
// route the adapters also accept the relay_access credential kind beside
// bearer, because protocol.SessionPlan validation labels a relay plan's
// credential relay_access while the relay connector, which synthesizes its
// plans and never runs Validate, labels the same key bearer.
//
// # Regions
//
// api.smallest.ai is the India region and is the only host these adapters
// accept by default. Smallest serves its East Asian streaming languages
// (zh, yue, ja, ko, multi-asian) from api.us.smallest.ai instead, and rejects
// them on the India host; the South Indian set (ta, te, kn, ml,
// multi-south-indic) is the mirror image and works only on api.smallest.ai.
// Reaching the US region therefore means adding api.us.smallest.ai to
// Config.AllowedEndpointHosts and putting that host in the session plan's
// endpoint — a deliberate configuration step, so a plan can never silently
// retarget a customer key at an unexpected host.
package smallest
