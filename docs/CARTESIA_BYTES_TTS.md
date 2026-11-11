# Cartesia whole-input TTS

`cartesia.NewBytesTTS(cartesia.BytesTTSConfig{})` implements
`runtime.Adapter` under `cartesia.tts.bytes.v1`. It accepts a concrete
`sonic-3`, `sonic-3.5` or `sonic-3.6` HTTP route at
`https://api.cartesia.ai/tts/bytes`, a delegated credential and explicit voice,
language and mono PCM16 media options. Registration and native financial
authority remain the consumer's responsibility.

The [bytes API](https://docs.cartesia.ai/api-reference/tts/bytes) takes the
complete transcript and streams audio in one HTTP response. This adapter pins
API version `2026-08-14`, its string voice field and confidential Authorization
header. It requests raw `pcm_s16le` at one of the six documented sample rates.
The existing WebSocket adapter and its wire version remain separately selected.

Append preserves exact UTF-8 fragments within a local byte limit and the
request's optional character ceiling. Commit schedules one typed JSON request
without blocking the runtime's serialized control dispatcher. Header/status
failures arrive on Events. Another utterance gets another POST and operation ID. Normalization uses the
existing per-model provider-option validation. No normalization or trimming is
applied to the submitted speech text, and no private native binding fields enter
the provider JSON. Redirects, compressed/ambiguous responses and unsupported
success content types are refused.

Native owners that must bind final input before admission can call
`adapter.PrepareInput(request, text)`. This validates the concrete generation
settings and creates an immutable complete provider body without credentials,
endpoint resolution or I/O. `prepared.Text()` supplies its exact final transcript
for the owner's input-binding check. The body and text remain private in memory;
never store or log them as control or financial evidence.
The opaque snapshot holds its private implementation by value, so another
consumer sharing it cannot replace an aliased pointee after the binding check.
External wrappers and other implementations are refused by `OpenPrepared`.

After admission, `adapter.OpenPrepared(workContext, fundedRequest, prepared)`
checks the actual endpoint and credential and requires the funded generation
settings to produce the same provider bytes within the current input bounds.
It then sends the original prepared bytes once. No text append or commit can
replace them or start another utterance, even after completion. This prepared
stream follows the work context through actual HTTP cancellation and retains
the ordinary output and billing lifecycle. Preparation supplies no execution
authority, idempotency guarantee, no-I/O proof or heard-audio evidence; the
native owner must still establish those separately.

Audio frames contain whole PCM samples. Only a nonempty, aligned response that
ends cleanly publishes audio.done and a complete observation. Empty, truncated,
oversized, failed or interrupted output cannot become complete. Header waits,
error-body reads, active response-read inactivity and graceful-close inactivity
are bounded. Cancel/Abort interrupt pending headers as well as audio reads;
cancellation preserves the session for another utterance. A backpressured
terminal publication retains its cancellation handle and event order.
Close failures remain available through TerminalError before Events closes.
Error messages omit provider body content.

Permanent BYOK and relay keys remain reusable throughout the authorized session.
Managed provider-direct credentials renew through the supplied same-attempt
RefreshCredential hook before another POST needs an expired credential. The
runtime still enforces the session lease independently of provider key lifetime.

Observations measure the exact submitted Unicode scalar count in `characters`,
with the protocol's thousandths representation and `streaming` output mode.
Every potentially dispatched POST first emits an incomplete operation with an
unknown quantity. Header failures and pre-audio cancellation therefore cannot
hide behind a previous completed utterance or become a measured zero.
Credential renewal or request preparation failures before that dispatch boundary
report an error without creating a provider billing operation.
They do not infer credits, provider cost, audible delivery or a provider request
ID. Cartesia's [pricing documentation](https://docs.cartesia.ai/pricing) says
transcript preprocessing can vary its final credit total. A consumer must use
an explicitly reviewed character-priced policy or retain unknown provider cost;
it cannot relabel this count as exact credits or use the local operation ID as
invoice attribution. Native grant admission, durable receipts and settlement
remain separate consumer work.

Tests use local HTTP peers only. Live provider acceptance and the paired native
router/control-plane lifecycle must be proven before enabling this route.
