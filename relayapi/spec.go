package relayapi

import _ "embed"

// openAPISpecJSON is the normative, machine-checked HTTP contract served by
// the hosted Relay. Embedding the checked-in mirror prevents the runtime and
// the public contract from drifting into two independently maintained specs.
//
//go:embed openapi.json
var openAPISpecJSON []byte

// OpenAPISpecJSON returns an isolated copy of the normative OpenAPI 3.1
// document. Callers may safely hand the bytes to an HTTP response writer or
// mutate the returned slice without changing future results.
func OpenAPISpecJSON() []byte {
	return append([]byte(nil), openAPISpecJSON...)
}
