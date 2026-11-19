package deepgram

import "github.com/SpekoAI/gateway/protocol"

// spekoPaidKey reports whether a plan runs on a Deepgram key Speko pays for:
// a managed provider-direct credential, or the relay connector's own key.
// Those requests settle against a Speko reservation and opt out of Deepgram's
// Model Improvement Program. A customer's own key keeps whatever its Deepgram
// account decides.
func spekoPaidKey(execution protocol.Execution) bool {
	return execution.ProviderRoute == protocol.RouteSpekoRelay || execution.CredentialSource == protocol.CredentialsManaged
}

// mipOptOutParam is the documented per-request opt-out from Deepgram's Model
// Improvement Program. Deepgram enrolls every request that does not send it.
const mipOptOutParam = "mip_opt_out"
