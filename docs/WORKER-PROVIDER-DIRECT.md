# Worker-funded provider-direct factories

Status: **unavailable**. No positive factory or provider eligibility is exported.
The supported production matrix is empty. Ordinary worker STT/TTS must refuse;
neither local-token `GatewayClient` nor the relay clients attest native funded
authority. D5 does not permit using relay media to bypass that refusal.

The realtime conversation fixes are a corrective stack on
[unaccepted Gateway #147](https://github.com/SpekoAI/gateway/pull/147), based on
`af7ad1380df6aaae0b984831382fda56ecfdaafb`. They do not merge that implementation
into main or certify its routing, funding, receipts or providers. They repair
Gemini history completion, typed input, playback interruption, unsafe tool
admission and GPT-Live delegated reply context. Gemini tools remain refused
because the supported SDK executes the Agent's original tools without an owned
per-call cancellation hook. Extended-thinking tool sessions are refused too.
OpenAI/xAI and GPT-Live retain their separately tested tool rounds.

## Required upstream source

The canonical runtime profile is proposed in
[contracts #52](https://github.com/SpekoAI/contracts/pull/52), not accepted or
vendored here. Its source is `proto/speko/agents/v1/runtime.proto` and
`docs/worker-runtime-profile.md`. Both `worker-runtime-v1` and
`provider-direct-authority-v1` must be negotiated at claim and spec, including
rollback. The draft only admits LIVEKIT/LIVEKIT; Pipecat remains refused.
Dispatch metadata cannot select an engine, provider or authority.

The router producer and exact connector directory are separate prerequisites.
Neither currently supplies a reviewed mounted creation protocol or an eligible
bounded production provider. Do not infer one from relay catalog coverage,
balanced addresses, credential TTLs or signed declarations of provider policy.

The conceptual private API, which is not a mounted RPC or a Python schema, is:

1. Create using the unchanged original signed command bytes, original private
   creation/setup request bytes, derived worker-session execution credential,
   independently signed native authority, verified grant and current lease.
2. Receive one opaque scoped handle. Open it at most once to obtain native duplex
   I/O plus owned Abort/Close/Wait. There is no automatic retry, redial or replay.

Only the canonical router producer can attest current assignment/worker attempt,
claim, ownership epoch, source/credential revocation and caller revision. A newer
pending caller observation invalidates the previous one. Its source window is
at most five seconds from the DB-clock read **start**, consumed by network time.
Execution stops at the earliest original caller/source/claim/lease horizon;
reporting grace and a refreshed caller observation cannot extend a handle or
allowance. No ongoing renewal protocol is accepted by this SDK.

Signed exact leg/protocol/provider/model/voice/language/region/retention,
credential source/resource version, endpoint/inventory and frozen request bounds
must match independently configured trust and current native checks. The startup
request digest is not authority for arbitrary future audio, tool controls or
backend requests. Separately billed ASR requires its own funded transcription
leg; GPT-Live backend calls require separately funded LLM requests with exact
positive output-token bounds. No provider, fleet or control key enters the SDK.

The router owns durable one-use pre-I/O admission/tickets, allowance and capacity,
physical transport, immutable cumulative receipts and retained cleanup
responsibility. Effect owns reservation, settlement and money. EOF, socket abort,
SDK callbacks and in-memory metadata cannot prove NoIO, zero usage, complete
attribution, durable finalization or settlement. Ambiguous creation/open/drain
retains UNKNOWN exposure. No duplicate balance or worker monetary authority.

## Acceptance evidence still required

The conversation regressions use the actual Python SDK, Go sidecar and owned TLS
WebSocket/HTTP peers. They cover configuration/history ordering, typed replies,
buffered playback interruption, pre-I/O tool refusal, delegated context/tool
rounds and physical close with a paused consumer. They are not positive factory
or real-provider acceptance tests.

Before lifting refusal, exercise the actual reviewed factory and producer with
original signed bytes; current revocation, takeover, caller withdrawal and pending
observations; original deadline/lease expiry; bounded shutdown and producer joins;
reply/tool rounds; unchanged cumulative metering, durable receipt delivery,
settlement and crash cleanup. Report vendor acceptance separately and keep every
unbounded or unsupported provider refused. No live provider calls were made here.
