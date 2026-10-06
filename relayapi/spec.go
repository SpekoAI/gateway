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

// asyncAPISpecJSON is the normative, machine-checked WebSocket contract served
// by the hosted Relay. Embedded for the same reason as the OpenAPI mirror
// above: the streaming contract the Relay serves and the one this repository
// checks must be the same bytes.
//
//go:embed asyncapi.json
var asyncAPISpecJSON []byte

// AsyncAPISpecJSON returns an isolated copy of the normative AsyncAPI 2.6
// document. Callers may safely hand the bytes to an HTTP response writer or
// mutate the returned slice without changing future results.
func AsyncAPISpecJSON() []byte {
	return append([]byte(nil), asyncAPISpecJSON...)
}
