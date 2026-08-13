package relayapi_test

import (
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestErrorCodeSetIsClosedAndValid(t *testing.T) {
	t.Parallel()

	codes := relayapi.ErrorCodes()
	if len(codes) != 16 {
		t.Fatalf("closed code set has %d codes, want 16", len(codes))
	}
	seen := make(map[relayapi.ErrorCode]bool, len(codes))
	for _, code := range codes {
		if seen[code] {
			t.Fatalf("duplicate error code %q", code)
		}
		seen[code] = true
		body := relayapi.ErrorBody{Code: code, Message: "detail"}
		if err := body.Validate(); err != nil {
			t.Fatalf("code %q must validate: %v", code, err)
		}
	}
}

func TestErrorEnvelopeValidation(t *testing.T) {
	t.Parallel()

	var envelope relayapi.ErrorEnvelope
	decodeFixture(t, "error-envelope.json", &envelope)

	mutated := envelope
	mutated.Error.Code = "provider_meltdown"
	assertInvalid(t, mutated.Validate(), "code: unsupported value")

	mutated = envelope
	mutated.Error.Message = " "
	assertInvalid(t, mutated.Validate(), "message: required")
}

func TestErrorEventValidation(t *testing.T) {
	t.Parallel()

	event := relayapi.ErrorEvent{
		Type:  relayapi.ErrorEventType,
		Error: relayapi.ErrorBody{Code: relayapi.ErrorCodeRelayError, Message: "detail", Retryable: true},
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid error event must validate: %v", err)
	}

	event.Type = "session.error"
	assertInvalid(t, event.Validate(), `got "session.error"`)

	event.Type = relayapi.ErrorEventType
	event.Error.Message = ""
	assertInvalid(t, event.Validate(), "message: required")
}
