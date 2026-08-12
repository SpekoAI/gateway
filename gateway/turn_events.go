// Turn events are the conversation profiler's content-free markers: a worker
// probe observes one framework conversation, mints opaque conversation/turn
// identifiers, and posts small marker batches to this local route. The gateway
// validates a closed marker vocabulary, enriches conversation.started with the
// identity only the gateway knows, and forwards each marker through the
// existing bounded telemetry exporter.
//
// Envelope reuse, deliberately: a marker travels as a runtime.TelemetryEvent
// whose SessionID carries the conversation_id and whose AttemptID carries the
// turn_id ("" for conversation-scoped markers). EventID is deterministic
// (tev_<conversation_id>_<seq>) so at-least-once delivery dedupes server-side
// exactly like session telemetry. Markers are never Required: disabling
// optional telemetry suppresses the profiler entirely.
package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// maxTurnEventBatchEvents and maxTurnEventBytes bound one local batch.
	// The probe batches at the same size, so a compliant probe never splits.
	maxTurnEventBatchEvents = 64
	maxTurnEventBytes       = 256 << 10

	turnEventConversationStarted = "conversation.started"

	// maxTurnEventIdentifierBytes bounds identifier fields (leg session,
	// attempt, and request IDs) and maxTurnEventLabelBytes bounds descriptive
	// fields (integration, versions, provider, model). Identifiers are
	// machine-minted and short; anything longer suggests content smuggled into
	// a field that must stay content-free. The bounds and the character set
	// below mirror the control plane's validator exactly: a marker the gateway
	// accepts but the control plane rejects would abort a whole exported batch
	// after the probe already got its 202, so strictness must match here,
	// where the probe bug is still locally observable.
	maxTurnEventIdentifierBytes = 256
	maxTurnEventLabelBytes      = 128
)

// safeTurnToken reports whether a marker string field is a bounded token of
// [A-Za-z0-9._:-] — the same closed character set the control plane enforces.
// Free-form strings are rejected everywhere: a field that admitted spaces
// would be a place to smuggle content.
func safeTurnToken(value string, maxLength int) bool {
	if value == "" || len(value) > maxLength {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == ':' || character == '-' {
			continue
		}
		return false
	}
	return true
}

// TurnEventDestinations selects where validated turn markers are exported.
// The route chooses the destination; the exporter's anonymous fallback is
// intentionally not relied upon so the choice stays auditable in one place.
type TurnEventDestinations struct {
	// AuthenticatedEndpoint is the control-plane turn-event ingest URL. It must
	// be derived from the configured control-plane origin only, never from
	// request data. Used together with AuthenticatedToken when both are set.
	AuthenticatedEndpoint string
	// AuthenticatedToken is the Speko API key. It is a credential: never log it.
	AuthenticatedToken string
	// AnonymousEndpoint receives markers when no API key is configured. It
	// carries no token and no account linkage.
	AnonymousEndpoint string
}

var (
	conversationIDPattern = regexp.MustCompile(`^conv_[0-9a-f]{32}$`)
	turnIDPattern         = regexp.MustCompile(`^turn_[0-9]{6}$`)
)

type turnEventBatch struct {
	Events []turnEvent `json:"events"`
}

type turnEvent struct {
	Type           string          `json:"type"`
	ConversationID string          `json:"conversation_id"`
	TurnID         string          `json:"turn_id"`
	Seq            uint64          `json:"seq"`
	CreatedAtMS    int64           `json:"created_at_ms"`
	Data           json.RawMessage `json:"data"`
}

// turnEventField validates one marker data field. Every field of every marker
// type is enumerated: a field outside the set rejects the whole batch, which
// is what keeps the vocabulary closed against content smuggling.
type turnEventField struct {
	required bool
	validate func(json.RawMessage) error
}

type turnEventSchema struct {
	turnScoped bool
	fields     map[string]turnEventField
	// verify applies cross-field rules after individual fields validate.
	verify func(map[string]json.RawMessage) error
}

func nonNegativeIntegerField(required bool) turnEventField {
	return turnEventField{required: required, validate: func(raw json.RawMessage) error {
		var value int64
		if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
			return errors.New("must be a non-negative integer")
		}
		return nil
	}}
}

func positiveIntegerField() turnEventField {
	return turnEventField{required: true, validate: func(raw json.RawMessage) error {
		var value int64
		if err := json.Unmarshal(raw, &value); err != nil || value < 1 {
			return errors.New("must be a positive integer")
		}
		return nil
	}}
}

func booleanField(required bool) turnEventField {
	return turnEventField{required: required, validate: func(raw json.RawMessage) error {
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return errors.New("must be a boolean")
		}
		return nil
	}}
}

func enumerationField(values ...string) turnEventField {
	return turnEventField{required: true, validate: func(raw json.RawMessage) error {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return errors.New("must be a string")
		}
		for _, candidate := range values {
			if value == candidate {
				return nil
			}
		}
		return errors.New("is outside its closed vocabulary")
	}}
}

func tokenField(required bool, maxLength int) turnEventField {
	return turnEventField{required: required, validate: func(raw json.RawMessage) error {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil || !safeTurnToken(value, maxLength) {
			return fmt.Errorf("must be a token of 1..%d bytes from [A-Za-z0-9._:-]", maxLength)
		}
		return nil
	}}
}

// timingOnly is the shape shared by most markers: nothing beyond the
// monotonic stamp. Declared once so the vocabulary table reads as data.
func timingOnly(turnScoped bool) turnEventSchema {
	return turnEventSchema{turnScoped: turnScoped, fields: map[string]turnEventField{
		"mono_ms": nonNegativeIntegerField(true),
	}}
}

func withFields(turnScoped bool, extra map[string]turnEventField) turnEventSchema {
	fields := map[string]turnEventField{"mono_ms": nonNegativeIntegerField(true)}
	for name, field := range extra {
		fields[name] = field
	}
	return turnEventSchema{turnScoped: turnScoped, fields: fields}
}

// turnEventVocabulary is the closed marker world, v1. The same vocabulary is
// duplicated as string literals in the control plane on purpose: the two
// repositories must not share new symbols so either side can merge first.
var turnEventVocabulary = map[string]turnEventSchema{
	// conversation.started deliberately omits the enrichment fields
	// (workload_type, workload_id, instance_id, gateway_version) from the
	// caller-facing schema: the gateway injects them after validation, so a
	// caller supplying them is rejected as an unknown field and cannot spoof
	// another workload's identity.
	"conversation.started": withFields(false, map[string]turnEventField{
		"integration":         tokenField(true, maxTurnEventLabelBytes),
		"integration_version": tokenField(true, maxTurnEventLabelBytes),
	}),
	"conversation.ended": withFields(false, map[string]turnEventField{
		"reason":     enumerationField("hangup", "transfer", "error", "shutdown", "unknown"),
		"turn_count": nonNegativeIntegerField(true),
	}),
	"turn.started": withFields(true, map[string]turnEventField{
		"initiator": enumerationField("user", "agent"),
	}),
	"user.speech.started":   timingOnly(true),
	"user.speech.ended":     timingOnly(true),
	"user.transcript.final": timingOnly(true),
	"llm.requested":         timingOnly(true),
	"llm.first_token":       timingOnly(true),
	"llm.completed": withFields(true, map[string]turnEventField{
		"ok": booleanField(true),
	}),
	"tool.started": withFields(true, map[string]turnEventField{
		"tool_index": positiveIntegerField(),
	}),
	"tool.completed": withFields(true, map[string]turnEventField{
		"tool_index": positiveIntegerField(),
		"ok":         booleanField(true),
	}),
	"tts.requested":    timingOnly(true),
	"tts.first_audio":  timingOnly(true),
	"playback.started": timingOnly(true),
	"playback.stopped": withFields(true, map[string]turnEventField{
		"interrupted":          booleanField(true),
		"playback_position_ms": nonNegativeIntegerField(false),
	}),
	"interrupt.detected":    timingOnly(true),
	"interrupt.cancel_sent": timingOnly(true),
	"turn.completed":        timingOnly(true),
	"leg.attached": {
		turnScoped: true,
		fields: map[string]turnEventField{
			"mono_ms":    nonNegativeIntegerField(true),
			"kind":       enumerationField("stt", "tts", "llm"),
			"session_id": tokenField(false, maxTurnEventIdentifierBytes),
			"attempt_id": tokenField(false, maxTurnEventIdentifierBytes),
			"request_id": tokenField(false, maxTurnEventIdentifierBytes),
			"provider":   tokenField(false, maxTurnEventLabelBytes),
			"model":      tokenField(false, maxTurnEventLabelBytes),
		},
		verify: verifyLegAttachment,
	},
}

// verifyLegAttachment enforces that leg identifiers match the leg's identity
// namespace: STT/TTS legs are gateway sessions, LLM legs are relay requests.
// Mixing them would let one namespace masquerade as another in the assembler.
func verifyLegAttachment(fields map[string]json.RawMessage) error {
	var kind string
	_ = json.Unmarshal(fields["kind"], &kind)
	_, hasSessionID := fields["session_id"]
	_, hasAttemptID := fields["attempt_id"]
	_, hasRequestID := fields["request_id"]
	switch kind {
	case "stt", "tts":
		if !hasSessionID || !hasAttemptID || hasRequestID {
			return errors.New("stt and tts legs require session_id and attempt_id and no request_id")
		}
	case "llm":
		if !hasRequestID || hasSessionID || hasAttemptID {
			return errors.New("llm legs require request_id and no session_id or attempt_id")
		}
	}
	return nil
}

// validateTurnEvent checks one marker against the closed vocabulary and
// returns its decoded data fields for enrichment and forwarding. Error
// messages name fields and positions but never echo caller-supplied values.
func validateTurnEvent(event turnEvent) (map[string]json.RawMessage, error) {
	schema, known := turnEventVocabulary[event.Type]
	if !known {
		return nil, errors.New("unknown event type")
	}
	if !conversationIDPattern.MatchString(event.ConversationID) {
		return nil, errors.New("conversation_id must match conv_<32 lowercase hex>")
	}
	if schema.turnScoped {
		if !turnIDPattern.MatchString(event.TurnID) {
			return nil, errors.New("turn_id must match turn_<6 digits>")
		}
	} else if event.TurnID != "" {
		return nil, errors.New("turn_id must be empty for conversation-scoped events")
	}
	if event.Seq < 1 {
		return nil, errors.New("seq must be positive")
	}
	if event.CreatedAtMS < 1 {
		return nil, errors.New("created_at_ms must be positive")
	}
	if len(event.Data) == 0 {
		return nil, errors.New("data is required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(event.Data, &fields); err != nil || fields == nil {
		return nil, errors.New("data must be a JSON object")
	}
	for name, raw := range fields {
		field, allowed := schema.fields[name]
		if !allowed {
			return nil, errors.New("data carries a field outside the closed schema")
		}
		if err := field.validate(raw); err != nil {
			return nil, fmt.Errorf("data field %s %w", name, err)
		}
	}
	for name, field := range schema.fields {
		if field.required {
			if _, present := fields[name]; !present {
				return nil, fmt.Errorf("data field %s is required", name)
			}
		}
	}
	if schema.verify != nil {
		if err := schema.verify(fields); err != nil {
			return nil, err
		}
	}
	return fields, nil
}

// enrichConversationStarted injects the identity only the gateway knows.
// Callers cannot supply these fields (the closed schema rejects them), so the
// stored workload/instance identity is always the configured one. Each value
// comes from operator configuration (environment or hostname), so it is
// injected only when it passes the same token check the control plane
// applies: forwarding a value the receiver rejects would abort the whole
// exported batch long after this route returned 202. A skipped field is an
// absent field, which the receiver treats as optional.
func (s *Server) enrichConversationStarted(fields map[string]json.RawMessage) {
	enrich := func(name, value string, maxLength int) {
		if safeTurnToken(value, maxLength) {
			fields[name] = mustMarshalJSONString(value)
		}
	}
	enrich("instance_id", s.runtime.InstanceID, maxTurnEventIdentifierBytes)
	enrich("gateway_version", s.runtime.Version, maxTurnEventLabelBytes)
	if s.workload != nil {
		enrich("workload_type", s.workload.Type, maxTurnEventLabelBytes)
		enrich("workload_id", s.workload.ID, maxTurnEventIdentifierBytes)
	}
}

func mustMarshalJSONString(value string) json.RawMessage {
	// json.Marshal cannot fail for a string value.
	raw, _ := json.Marshal(value)
	return raw
}

// turnEventDestination picks the export contract for this process: the
// control-plane ingest when an API key is configured, otherwise the anonymous
// endpoint with no token and no account linkage.
func (s *Server) turnEventDestination() protocol.Telemetry {
	if s.turnEventDestinations.AuthenticatedEndpoint != "" && s.turnEventDestinations.AuthenticatedToken != "" {
		return protocol.Telemetry{
			Endpoint: s.turnEventDestinations.AuthenticatedEndpoint,
			Token:    s.turnEventDestinations.AuthenticatedToken,
		}
	}
	return protocol.Telemetry{Endpoint: s.turnEventDestinations.AnonymousEndpoint}
}

// telemetryTurnEvent maps one validated marker onto the exporter contract:
// the marker type passes through as the event name, the conversation and turn
// identifiers ride the session/attempt envelope fields, and seq folds into the
// data payload so the receiver can rebuild the probe's total order.
func telemetryTurnEvent(event turnEvent, fields map[string]json.RawMessage, destination protocol.Telemetry) (runtimepkg.TelemetryEvent, error) {
	sequence := strconv.FormatUint(event.Seq, 10)
	fields["seq"] = json.RawMessage(sequence)
	data, err := json.Marshal(fields)
	if err != nil {
		return runtimepkg.TelemetryEvent{}, errors.New("gateway: encode turn event data")
	}
	return runtimepkg.TelemetryEvent{
		Name:        event.Type,
		SessionID:   event.ConversationID,
		AttemptID:   event.TurnID,
		At:          time.UnixMilli(event.CreatedAtMS),
		EventID:     "tev_" + event.ConversationID + "_" + sequence,
		Destination: destination,
		Data:        data,
		Required:    false,
	}, nil
}

// recordTurnEvents serves POST /v1/turn-events. Any invalid event rejects the
// whole batch — the same abort semantics the control plane applies — so a
// probe bug cannot half-ingest a conversation. The 202 response reports how
// many markers the bounded exporter actually took; the probe never retries,
// because retry (with deduplication by deterministic event ID) belongs to the
// exporter.
func (s *Server) recordTurnEvents(writer http.ResponseWriter, request *http.Request) {
	if !s.authorize(writer, request) {
		return
	}
	var batch turnEventBatch
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxTurnEventBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid turn event batch")
		return
	}
	if len(batch.Events) == 0 {
		writeError(writer, http.StatusBadRequest, "invalid_request", "turn event batch requires at least one event")
		return
	}
	if len(batch.Events) > maxTurnEventBatchEvents {
		writeError(writer, http.StatusBadRequest, "invalid_request", fmt.Sprintf("turn event batch exceeds %d events", maxTurnEventBatchEvents))
		return
	}
	destination := s.turnEventDestination()
	events := make([]runtimepkg.TelemetryEvent, 0, len(batch.Events))
	for index, event := range batch.Events {
		fields, err := validateTurnEvent(event)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_turn_event", fmt.Sprintf("event %d: %v", index, err))
			return
		}
		if event.Type == turnEventConversationStarted {
			s.enrichConversationStarted(fields)
		}
		telemetryEvent, err := telemetryTurnEvent(event, fields, destination)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "turn_event_encoding_failed", "turn event could not be encoded")
			return
		}
		events = append(events, telemetryEvent)
	}
	accepted, dropped := 0, 0
	for _, event := range events {
		if s.telemetry.TryRecord(event) {
			accepted++
		} else {
			dropped++
		}
	}
	writeJSON(writer, http.StatusAccepted, map[string]int{"accepted": accepted, "dropped": dropped})
}
