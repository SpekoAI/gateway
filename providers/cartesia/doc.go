// Package cartesia implements the provider-direct Cartesia streaming TTS
// adapter for customer-owned credentials.
//
// The separate BytesTTSAdapter also supports whole-input HTTP synthesis. Its
// PrepareInput method freezes the complete provider body without credentials or
// I/O. A native execution owner can inspect the exact final Text before admission
// and use OpenPrepared to verify the funded settings and send those same bytes.
// Prepared streams follow the work context and permit one operation; preparation
// is neither execution authority nor evidence that audio was delivered or heard.
package cartesia
