// Package assemblyai implements the provider-direct AssemblyAI Universal-
// Streaming v3 transcription adapter. Customer-owned keys are injected in
// memory only after plan verification.
//
// Auth channels are keyed by the plan's route, never by the credential kind.
// A permanent API key — a customer's on BYOK plans, the relay connector's on
// RouteSpekoRelay plans — rides the `Authorization` header as the bare key
// with no "Bearer" prefix, and must never reach a URL, because URLs reach
// access logs. A single-use temporary streaming token, minted by the control
// plane for managed provider-direct plans, is accepted by the vendor ONLY as
// the `token` query parameter. The two channels are not interchangeable: the
// wrong one fails authentication at the handshake. On the relay route the
// adapter accepts credential kinds bearer and relay_access, which are two
// spellings of the same permanent key (see acceptableCredentialKind).
package assemblyai
