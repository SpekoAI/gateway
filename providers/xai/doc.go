// Package xai implements the provider-direct xAI TTS adapter.
//
// Unlike every other adapter in this repository, xAI's text-to-speech surface
// is a unary HTTPS request rather than a WebSocket session: the gateway POSTs
// one JSON body to https://api.x.ai/v1/tts and reads raw audio bytes back off
// the response body. The runtime does not care — it only consumes a
// runtime.ProviderStream — so the adapter buffers AppendText locally and turns
// CommitText into the request that produces one utterance of audio.
//
// Every wire detail below was verified against xAI's official documentation at
// https://docs.x.ai/developers/model-capabilities/audio/text-to-speech on
// 2026-08-07. Where the documentation is silent the code says so explicitly
// rather than guessing.
package xai
