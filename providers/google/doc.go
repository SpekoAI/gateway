// Package google implements the provider-direct Google Cloud Text-to-Speech
// adapter (Chirp 3: HD and every other Cloud TTS voice family).
//
// It is the first HTTP-transport adapter in this repository. Every other
// provider streams over a WebSocket; Cloud Text-to-Speech exposes no REST
// streaming method at all (`streamingSynthesize` is bidirectional gRPC only),
// so this adapter buffers text, performs one POST to `/v1/text:synthesize`,
// and re-streams the single response body as canonical audio frames. The
// engine only needs `runtime.ProviderStream`, so the transport difference is
// invisible above the adapter boundary.
//
// Credentials are customer-owned and injected in memory only after plan
// verification: Cloud TTS is OAuth-bearer, which our capability registry
// records as insufficiently session-scoped, so this ships BYOK-direct.
package google
