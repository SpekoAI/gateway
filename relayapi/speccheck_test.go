package relayapi_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

// The spec contract: openapi.yaml and asyncapi.yaml are the hand-authored,
// human-readable sources, and openapi.json / asyncapi.json are their
// checked-in JSON mirrors — the same documents, converted mechanically —
// because these tests parse JSON with the standard library rather than
// pulling a YAML dependency into the contract repo.
//
// The MIRRORS are the normative machine-checked documents: every fixture,
// drift, and subset-validation test in this file reads the JSON, never the
// YAML, so the JSON is what the contract provably says and the YAML is the
// courtesy view for humans.
//
// Each YAML file opens with two fingerprint comment lines:
//
//	# json-mirror: <mirror>.json sha256:<sha256 of the mirror file bytes>
//	# yaml-body: sha256:<sha256 of every YAML byte after these two lines>
//
// TestSpecMirrorFingerprints verifies both hashes, so a YAML edit without a
// regenerated mirror, a mirror edit without a YAML edit, or a stale header
// all fail loudly. The header is a tripwire, not a semantic proof: it forces
// the YAML, the mirror, and the hashes to move in one reviewable change,
// and the fixture and drift tests below then hold the mirror — the document
// the machine checks actually read — to the Go types. What no test can
// catch is a mirror edit that refreshes only the json-mirror header line
// while leaving the YAML text saying something else, so a review that
// touches either file must confirm the other says the same thing. To change
// a spec:
//
//	edit relayapi/<spec>.yaml, then from the repo root
//	ruby -ryaml -rjson -e 'puts JSON.pretty_generate(YAML.load_file("relayapi/<spec>.yaml"))' > relayapi/<spec>.json
//	shasum -a 256 relayapi/<spec>.json          # → the json-mirror line
//	tail -n +3 relayapi/<spec>.yaml | shasum -a 256   # → the yaml-body line
//
// (Any YAML-to-JSON converter works; the header pins whatever bytes it
// produced.)

var specFingerprintHeader = regexp.MustCompile(`^# json-mirror: ([a-z]+\.json) sha256:([0-9a-f]{64})\n# yaml-body: sha256:([0-9a-f]{64})\n`)

func TestSpecMirrorFingerprints(t *testing.T) {
	t.Parallel()

	for _, spec := range []struct{ yaml, mirror string }{
		{"openapi.yaml", "openapi.json"},
		{"asyncapi.yaml", "asyncapi.json"},
	} {
		raw, err := os.ReadFile(spec.yaml)
		if err != nil {
			t.Fatalf("read spec: %v", err)
		}
		match := specFingerprintHeader.FindSubmatchIndex(raw)
		if match == nil {
			t.Fatalf("%s: missing or malformed fingerprint header lines", spec.yaml)
		}
		if named := string(raw[match[2]:match[3]]); named != spec.mirror {
			t.Fatalf("%s: header names mirror %q, want %q", spec.yaml, named, spec.mirror)
		}
		mirror, err := os.ReadFile(spec.mirror)
		if err != nil {
			t.Fatalf("read mirror: %v", err)
		}
		if got, want := sha256Hex(mirror), string(raw[match[4]:match[5]]); got != want {
			t.Fatalf("%s: mirror %s hashes to sha256:%s, header says sha256:%s — regenerate the mirror and refresh the header", spec.yaml, spec.mirror, got, want)
		}
		if got, want := sha256Hex(raw[match[1]:]), string(raw[match[6]:match[7]]); got != want {
			t.Fatalf("%s: yaml body hashes to sha256:%s, header says sha256:%s — refresh the header (and the mirror if the edit was semantic)", spec.yaml, got, want)
		}
	}
}

func sha256Hex(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Every HTTP golden fixture must satisfy the OpenAPI component schema of the
// same shape, so the spec can never quietly diverge from the bytes the golden
// round-trip test already pins.
func TestHTTPFixturesMatchOpenAPISchemas(t *testing.T) {
	t.Parallel()

	doc := loadSpecDoc(t, "openapi.json")
	for _, tc := range []struct {
		fixture string
		schema  string
	}{
		{"routing-auto.json", "Routing"},
		{"routing-explicit.json", "Routing"},
		{"error-envelope.json", "ErrorEnvelope"},
		{"models-response.json", "ModelsResponse"},
		{"stt-transcription-request.json", "TranscriptionRequest"},
		{"stt-transcription-response.json", "TranscriptionResponse"},
		{"tts-speech-request.json", "SpeechRequest"},
		{"llm-request-tools.json", "LLMRequest"},
		{"llm-request-structured.json", "LLMRequest"},
		{"llm-response.json", "LLMResponse"},
	} {
		doc.validateAgainst(t, tc.schema, readFixture(t, tc.fixture), tc.fixture)
	}
}

// Every SSE event sample must satisfy the schema its event name maps to, and
// the fixture must exercise the complete closed event set — a new event
// constant without a spec schema and a sample fails here.
func TestSSEEventSamplesMatchOpenAPISchemas(t *testing.T) {
	t.Parallel()

	schemaByEvent := map[string]string{
		relayapi.SSEResponseCreated:                    "ResponseCreated",
		relayapi.SSEResponseItemAdded:                  "ResponseItemAdded",
		relayapi.SSEResponseTextDelta:                  "ResponseTextDelta",
		relayapi.SSEResponseFunctionCallArgumentsDelta: "ResponseFunctionCallArgumentsDelta",
		relayapi.SSEResponseItemCompleted:              "ResponseItemCompleted",
		relayapi.SSEResponseCompleted:                  "ResponseCompleted",
		relayapi.SSEError:                              "ErrorEnvelope",
	}

	doc := loadSpecDoc(t, "openapi.json")
	var samples []struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	decodeFixture(t, "llm-sse-events.json", &samples)

	seen := make(map[string]bool, len(samples))
	for i, sample := range samples {
		schema, ok := schemaByEvent[sample.Event]
		if !ok {
			t.Fatalf("llm-sse-events.json[%d]: unknown SSE event %q", i, sample.Event)
		}
		seen[sample.Event] = true
		doc.validateAgainst(t, schema, sample.Data, "llm-sse-events.json["+strconv.Itoa(i)+"]")
	}
	for event := range schemaByEvent {
		if !seen[event] {
			t.Fatalf("llm-sse-events.json: fixture must exercise every SSE event, missing %q", event)
		}
	}
}

// Every WebSocket frame sample must satisfy the AsyncAPI schema its type tag
// maps to, and each fixture must exercise its channel's complete closed frame
// set — control frames and event frames alike.
func TestWSMessageSamplesMatchAsyncAPISchemas(t *testing.T) {
	t.Parallel()

	sttSchemaByType := map[string]string{
		string(relayapi.STTControlSessionConfigure): "STTSessionConfigure",
		string(relayapi.STTControlInputCommit):      "STTInputCommit",
		string(relayapi.STTControlSessionClose):     "STTSessionClose",
		string(relayapi.STTEventSessionReady):       "STTSessionReady",
		string(relayapi.STTEventTranscriptDelta):    "STTTranscriptDelta",
		string(relayapi.STTEventTranscriptFinal):    "STTTranscriptFinal",
		string(relayapi.STTEventUsageUpdated):       "STTUsageUpdated",
		string(relayapi.STTEventSessionClosed):      "STTSessionClosed",
		relayapi.ErrorEventType:                     "ErrorEvent",
	}
	ttsSchemaByType := map[string]string{
		string(relayapi.TTSControlSessionConfigure): "TTSSessionConfigure",
		string(relayapi.TTSControlInputAppend):      "TTSInputAppend",
		string(relayapi.TTSControlInputCommit):      "TTSInputCommit",
		string(relayapi.TTSControlInputCancel):      "TTSInputCancel",
		string(relayapi.TTSControlSessionClose):     "TTSSessionClose",
		string(relayapi.TTSEventSessionReady):       "TTSSessionReady",
		string(relayapi.TTSEventUtteranceStarted):   "TTSUtteranceStarted",
		string(relayapi.TTSEventUtteranceTimings):   "TTSUtteranceTimings",
		string(relayapi.TTSEventUtteranceDone):      "TTSUtteranceDone",
		string(relayapi.TTSEventUsageUpdated):       "TTSUsageUpdated",
		string(relayapi.TTSEventSessionClosed):      "TTSSessionClosed",
		relayapi.ErrorEventType:                     "ErrorEvent",
	}

	doc := loadSpecDoc(t, "asyncapi.json")
	for fixture, schemaByType := range map[string]map[string]string{
		"ws-stt-messages.json": sttSchemaByType,
		"ws-tts-messages.json": ttsSchemaByType,
	} {
		var frames []json.RawMessage
		decodeFixture(t, fixture, &frames)

		seen := make(map[string]bool, len(frames))
		for i, frame := range frames {
			var tagged struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(frame, &tagged); err != nil {
				t.Fatalf("%s[%d]: decode type tag: %v", fixture, i, err)
			}
			schema, ok := schemaByType[tagged.Type]
			if !ok {
				t.Fatalf("%s[%d]: unknown frame type %q", fixture, i, tagged.Type)
			}
			seen[tagged.Type] = true
			doc.validateAgainst(t, schema, frame, fixture+"["+strconv.Itoa(i)+"]")
		}
		for frameType := range schemaByType {
			if !seen[frameType] {
				t.Fatalf("%s: fixture must exercise every frame type, missing %q", fixture, frameType)
			}
		}
	}
}

// specRef names one component schema in one spec mirror.
type specRef struct {
	doc    string
	schema string
}

// wireSchemaTable maps every wire struct in the package to the spec schemas
// that document it. Types shared by both transports appear in both documents.
// TestSpecCoversEveryWireStruct holds this table to the package source, so a
// new wire struct without a table entry — and therefore without a spec
// schema — fails there rather than shipping undocumented.
func wireSchemaTable() []struct {
	value any
	refs  []specRef
} {
	both := func(schema string) []specRef {
		return []specRef{{"openapi.json", schema}, {"asyncapi.json", schema}}
	}
	openapi := func(schema string) []specRef { return []specRef{{"openapi.json", schema}} }
	asyncapi := func(schema string) []specRef { return []specRef{{"asyncapi.json", schema}} }

	return []struct {
		value any
		refs  []specRef
	}{
		{relayapi.Routing{}, both("Routing")},
		{relayapi.Route{}, both("Route")},
		{relayapi.ErrorBody{}, both("ErrorBody")},
		{relayapi.ErrorEnvelope{}, openapi("ErrorEnvelope")},
		{relayapi.ErrorEvent{}, asyncapi("ErrorEvent")},
		{relayapi.Usage{}, both("Usage")},
		{relayapi.ModelCapabilities{}, openapi("ModelCapabilities")},
		{relayapi.SampleRateRange{}, openapi("SampleRateRange")},
		{relayapi.AudioFormat{}, openapi("AudioFormat")},
		{relayapi.BatchAudioLimits{}, openapi("BatchAudioLimits")},
		{relayapi.BenchmarkMetadata{}, openapi("BenchmarkMetadata")},
		{relayapi.ModelBenchmark{}, openapi("ModelBenchmark")},
		{relayapi.Model{}, openapi("Model")},
		{relayapi.ModelsResponse{}, openapi("ModelsResponse")},
		{relayapi.TranscriptionRequest{}, openapi("TranscriptionRequest")},
		{relayapi.STTOptions{}, both("STTOptions")},
		{relayapi.TranscriptSegment{}, both("TranscriptSegment")},
		{relayapi.TranscriptWord{}, openapi("TranscriptWord")},
		{relayapi.TranscriptionResponse{}, openapi("TranscriptionResponse")},
		{relayapi.AudioConfig{}, both("AudioConfig")},
		{relayapi.SpeechRequest{}, openapi("SpeechRequest")},
		{relayapi.ContentPart{}, openapi("ContentPart")},
		{relayapi.Item{}, openapi("Item")},
		{relayapi.FunctionTool{}, openapi("FunctionTool")},
		{relayapi.ResponseFormat{}, openapi("ResponseFormat")},
		{relayapi.LLMRequest{}, openapi("LLMRequest")},
		{relayapi.LLMResponse{}, openapi("LLMResponse")},
		{relayapi.ResponseCreated{}, openapi("ResponseCreated")},
		{relayapi.ResponseItemAdded{}, openapi("ResponseItemAdded")},
		{relayapi.ResponseTextDelta{}, openapi("ResponseTextDelta")},
		{relayapi.ResponseFunctionCallArgumentsDelta{}, openapi("ResponseFunctionCallArgumentsDelta")},
		{relayapi.ResponseItemCompleted{}, openapi("ResponseItemCompleted")},
		{relayapi.ResponseCompleted{}, openapi("ResponseCompleted")},
		{relayapi.STTSessionConfigure{}, asyncapi("STTSessionConfigure")},
		{relayapi.STTInputCommit{}, asyncapi("STTInputCommit")},
		{relayapi.STTSessionClose{}, asyncapi("STTSessionClose")},
		{relayapi.STTSessionReady{}, asyncapi("STTSessionReady")},
		{relayapi.STTTranscriptDelta{}, asyncapi("STTTranscriptDelta")},
		{relayapi.STTTranscriptFinal{}, asyncapi("STTTranscriptFinal")},
		{relayapi.STTUsageUpdated{}, asyncapi("STTUsageUpdated")},
		{relayapi.STTSessionClosed{}, asyncapi("STTSessionClosed")},
		{relayapi.TTSSessionConfigure{}, asyncapi("TTSSessionConfigure")},
		{relayapi.TTSInputAppend{}, asyncapi("TTSInputAppend")},
		{relayapi.TTSInputCommit{}, asyncapi("TTSInputCommit")},
		{relayapi.TTSInputCancel{}, asyncapi("TTSInputCancel")},
		{relayapi.TTSSessionClose{}, asyncapi("TTSSessionClose")},
		{relayapi.TTSSessionReady{}, asyncapi("TTSSessionReady")},
		{relayapi.TTSUtteranceStarted{}, asyncapi("TTSUtteranceStarted")},
		{relayapi.TimingSpan{}, asyncapi("TimingSpan")},
		{relayapi.TTSUtteranceTimings{}, asyncapi("TTSUtteranceTimings")},
		{relayapi.TTSUtteranceDone{}, asyncapi("TTSUtteranceDone")},
		{relayapi.TTSUsageUpdated{}, asyncapi("TTSUsageUpdated")},
		{relayapi.TTSSessionClosed{}, asyncapi("TTSSessionClosed")},
	}
}

// The reflection drift check: the JSON field set of every wire struct must
// equal the property-name set of its spec schema (for the Item union, the
// union of property names across the oneOf variants). Both directions are
// checked, so a Go field missing from the spec and a spec property with no Go
// field behind it both fail.
func TestSpecSchemasMatchGoFieldSets(t *testing.T) {
	t.Parallel()

	docs := map[string]*specDoc{
		"openapi.json":  loadSpecDoc(t, "openapi.json"),
		"asyncapi.json": loadSpecDoc(t, "asyncapi.json"),
	}
	for _, entry := range wireSchemaTable() {
		typ := reflect.TypeOf(entry.value)
		goFields := jsonFieldNames(t, typ)
		for _, ref := range entry.refs {
			doc := docs[ref.doc]
			specFields := doc.propertyNames(t, doc.schema(t, ref.schema))
			for name := range goFields {
				if !specFields[name] {
					t.Errorf("%s: field %q is missing from %s schema %s", typ.Name(), name, ref.doc, ref.schema)
				}
			}
			for name := range specFields {
				if !goFields[name] {
					t.Errorf("%s schema %s: property %q has no field on %s", ref.doc, ref.schema, name, typ.Name())
				}
			}
		}
	}
}

// jsonFieldNames returns the wire field names of a struct type. Wire structs
// name every exported field with a json tag; an untagged field would be a
// wire change nobody wrote down, so it fails rather than being guessed at.
func jsonFieldNames(t *testing.T, typ reflect.Type) map[string]bool {
	t.Helper()
	names := make(map[string]bool, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			t.Fatalf("%s.%s: wire struct fields must carry a json tag with a name", typ.Name(), field.Name)
		}
		names[name] = true
	}
	return names
}

// The completeness guard behind wireSchemaTable: parse the package source and
// require every exported struct with a json-tagged field to have a table
// entry, and every table entry to still name such a struct. Structs without
// json-tagged fields (ContentHasher) are not wire types and are exempt.
func TestSpecCoversEveryWireStruct(t *testing.T) {
	t.Parallel()

	inTable := make(map[string]bool)
	for _, entry := range wireSchemaTable() {
		name := reflect.TypeOf(entry.value).Name()
		if inTable[name] {
			t.Fatalf("wireSchemaTable: duplicate entry for %s", name)
		}
		inTable[name] = true
	}

	inSource := wireStructNames(t)
	for name := range inSource {
		// These structs remain temporarily for Runtime connector wire
		// compatibility. They are not part of the public Relay API now that
		// the Router speech-to-speech media route has been removed.
		if strings.HasPrefix(name, "S2S") {
			continue
		}
		if !inTable[name] {
			t.Errorf("exported wire struct %s has no wireSchemaTable entry — add it and its spec schema", name)
		}
	}
	for name := range inTable {
		if !inSource[name] {
			t.Errorf("wireSchemaTable names %s, which is no longer an exported wire struct", name)
		}
	}
}

// wireStructNames parses the package's non-test source files and returns the
// name of every exported struct type that declares at least one json-tagged
// field.
func wireStructNames(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	names := make(map[string]bool)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typeSpec := spec.(*ast.TypeSpec)
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok || !typeSpec.Name.IsExported() {
					continue
				}
				if hasJSONTaggedField(structType) {
					names[typeSpec.Name.Name] = true
				}
			}
		}
	}
	return names
}

func hasJSONTaggedField(structType *ast.StructType) bool {
	for _, field := range structType.Fields.List {
		if field.Tag == nil {
			continue
		}
		tag, err := strconv.Unquote(field.Tag.Value)
		if err != nil {
			continue
		}
		name, _, _ := strings.Cut(reflect.StructTag(tag).Get("json"), ",")
		if name != "" && name != "-" {
			return true
		}
	}
	return false
}

// Both specs must carry the exact closed error code set, in the stable order
// ErrorCodes returns — the function exists so this test never re-lists the
// codes by hand.
func TestSpecErrorCodeEnumsMatchGo(t *testing.T) {
	t.Parallel()

	want := relayapi.ErrorCodes()
	for _, name := range []string{"openapi.json", "asyncapi.json"} {
		doc := loadSpecDoc(t, name)
		enum, ok := doc.schema(t, "ErrorCode")["enum"].([]any)
		if !ok {
			t.Fatalf("%s: ErrorCode schema has no enum", name)
		}
		if len(enum) != len(want) {
			t.Fatalf("%s: ErrorCode enum has %d codes, want %d", name, len(enum), len(want))
		}
		for i, code := range want {
			if enum[i] != string(code) {
				t.Fatalf("%s: ErrorCode enum[%d] = %v, want %q", name, i, enum[i], code)
			}
		}
	}
}

// Every $ref in both documents must resolve, including the discriminator
// mapping targets — this covers the channel and message plumbing the fixture
// tests never walk through.
func TestSpecRefsAllResolve(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"openapi.json", "asyncapi.json"} {
		doc := loadSpecDoc(t, name)
		var walk func(node any)
		walk = func(node any) {
			switch typed := node.(type) {
			case map[string]any:
				for key, value := range typed {
					if key == "$ref" {
						if ref, ok := value.(string); ok {
							if _, err := doc.resolve(ref); err != nil {
								t.Errorf("%s: %v", name, err)
							}
							continue
						}
					}
					if key == "mapping" {
						if mapping, ok := value.(map[string]any); ok {
							for tag, target := range mapping {
								ref, ok := target.(string)
								if !ok {
									continue
								}
								if _, err := doc.resolve(ref); err != nil {
									t.Errorf("%s: discriminator mapping %q: %v", name, tag, err)
								}
							}
							continue
						}
					}
					walk(value)
				}
			case []any:
				for _, value := range typed {
					walk(value)
				}
			}
		}
		walk(doc.root)
	}
}

// The OpenAPI plumbing around the schemas: the server, the bearer scheme,
// the required Idempotency-Key parameter on every POST, the exact response
// header set per endpoint (Speko-Usage-Characters on TTS only), and the LLM
// endpoint's JSON/SSE content duality.
func TestOpenAPIDeclaresRelayEnvelope(t *testing.T) {
	t.Parallel()

	doc := loadSpecDoc(t, "openapi.json")

	servers := specNode(t, doc, "servers").([]any)
	if url := servers[0].(map[string]any)["url"]; url != "https://router.speko.dev" {
		t.Fatalf("servers[0].url = %v, want https://router.speko.dev", url)
	}
	scheme := specNode(t, doc, "components", "securitySchemes", "bearerAuth").(map[string]any)
	if scheme["type"] != "http" || scheme["scheme"] != "bearer" {
		t.Fatalf("bearerAuth must be an http bearer scheme, got %v", scheme)
	}
	publicSecurity := specNode(t, doc, "paths", "/openapi.json", "get", "security").([]any)
	if len(publicSecurity) != 0 {
		t.Fatalf("GET /openapi.json must override bearer auth, got %v", publicSecurity)
	}

	parameter := specNode(t, doc, "components", "parameters", "IdempotencyKey").(map[string]any)
	if parameter["name"] != relayapi.HeaderIdempotencyKey || parameter["in"] != "header" || parameter["required"] != true {
		t.Fatalf("IdempotencyKey parameter must be the required %s header, got %v", relayapi.HeaderIdempotencyKey, parameter)
	}
	for _, path := range []string{"/v1/stt/transcriptions", "/v1/tts/speech", "/v1/llm/responses"} {
		parameters := specNode(t, doc, "paths", path, "post", "parameters").([]any)
		found := false
		for _, entry := range parameters {
			if entry.(map[string]any)["$ref"] == "#/components/parameters/IdempotencyKey" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: POST must reference the IdempotencyKey parameter", path)
		}
	}

	routeHeaders := []string{
		relayapi.HeaderRequestID,
		relayapi.HeaderAttemptID,
		relayapi.HeaderProvider,
		relayapi.HeaderModel,
		relayapi.HeaderRegion,
		relayapi.HeaderRateLimitPolicy,
	}
	for _, tc := range []struct {
		path    string
		method  string
		headers []string
	}{
		{"/openapi.json", "get", []string{relayapi.HeaderRateLimitPolicy}},
		{"/v1/models", "get", []string{relayapi.HeaderRequestID, relayapi.HeaderRegion, relayapi.HeaderRateLimitPolicy}},
		{"/v1/stt/transcriptions", "post", routeHeaders},
		// TTS is the only endpoint with a usage header: the raw audio
		// body has no place for a usage object. STT deliberately has
		// none — its usage lives in the response body.
		{"/v1/tts/speech", "post", append(append([]string{}, routeHeaders...), relayapi.HeaderUsageCharacters)},
		{"/v1/llm/responses", "post", routeHeaders},
	} {
		declared := specNode(t, doc, "paths", tc.path, tc.method, "responses", "200", "headers").(map[string]any)
		if len(declared) != len(tc.headers) {
			t.Errorf("%s: 200 declares %d headers, want %d", tc.path, len(declared), len(tc.headers))
		}
		for _, header := range tc.headers {
			if _, ok := declared[header]; !ok {
				t.Errorf("%s: 200 must declare the %s header", tc.path, header)
			}
		}
	}

	content := specNode(t, doc, "paths", "/v1/llm/responses", "post", "responses", "200", "content").(map[string]any)
	for _, mediaType := range []string{"application/json", "text/event-stream"} {
		if _, ok := content[mediaType]; !ok {
			t.Errorf("/v1/llm/responses: 200 must offer %s (stream=false JSON, stream=true SSE)", mediaType)
		}
	}
}

// The AsyncAPI plumbing around the schemas: both stream channels, the wss
// server, the required Idempotency-Key header on each upgrade, and the
// configure-frame hashing note that defines what a WebSocket retry must
// reproduce.
func TestAsyncAPIDeclaresStreamChannels(t *testing.T) {
	t.Parallel()

	doc := loadSpecDoc(t, "asyncapi.json")

	server := specNode(t, doc, "servers", "router").(map[string]any)
	if server["protocol"] != "wss" {
		t.Fatalf("servers.router.protocol = %v, want wss", server["protocol"])
	}
	if description, _ := server["description"].(string); !strings.Contains(description, "session.configure") {
		t.Fatalf("servers.router.description must carry the session.configure hashing note")
	}

	for _, channel := range []string{"/v1/stt/stream", "/v1/tts/stream"} {
		headers := specNode(t, doc, "channels", channel, "bindings", "ws", "headers").(map[string]any)
		properties := headers["properties"].(map[string]any)
		if _, ok := properties[relayapi.HeaderIdempotencyKey]; !ok {
			t.Errorf("%s: upgrade must declare the %s header", channel, relayapi.HeaderIdempotencyKey)
		}
		required, _ := headers["required"].([]any)
		found := false
		for _, name := range required {
			if name == relayapi.HeaderIdempotencyKey {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: the %s upgrade header must be required", channel, relayapi.HeaderIdempotencyKey)
		}
		if description, _ := specNode(t, doc, "channels", channel, "description").(string); !strings.Contains(description, "session.configure") {
			t.Errorf("%s: description must state that session.configure is the first frame and the idempotency hash input", channel)
		}
	}
}

// specNode walks nested spec objects by key, failing with the full path on
// the first missing step.
func specNode(t *testing.T, doc *specDoc, path ...string) any {
	t.Helper()
	node := any(doc.root)
	for i, key := range path {
		object, ok := node.(map[string]any)
		if !ok {
			t.Fatalf("%s: %s is not an object", doc.name, strings.Join(path[:i], "."))
		}
		node, ok = object[key]
		if !ok {
			t.Fatalf("%s: missing %s", doc.name, strings.Join(path[:i+1], "."))
		}
	}
	return node
}

// The validator must bite: each case is a spec violation the subset has to
// reject, so a regression in the validator cannot leave every fixture check
// vacuously green.
func TestSubsetValidatorRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	docs := map[string]*specDoc{
		"openapi.json":  loadSpecDoc(t, "openapi.json"),
		"asyncapi.json": loadSpecDoc(t, "asyncapi.json"),
	}
	for _, tc := range []struct {
		name   string
		doc    string
		schema string
		value  string
		want   string
	}{
		{"missing required property", "openapi.json", "ErrorBody", `{"code":"relay_error","retryable":true}`, "missing required property"},
		{"unknown enum value", "openapi.json", "Routing", `{"mode":"manual"}`, "not one of the enum values"},
		{"additional property", "openapi.json", "Route", `{"provider":"openai","model":"gpt-5.2","region":"us-west-2","attempt_id":"att_1","zone":"a"}`, "unexpected property"},
		{"pattern violation", "openapi.json", "ResponseCreated", `{"response_id":"nope"}`, "does not match pattern"},
		{"field bleed across union variants", "openapi.json", "Item", `{"type":"message","role":"user","content":[{"type":"text","text":"hi"}],"call_id":"call_1"}`, "unexpected property"},
		{"unknown union tag", "openapi.json", "Item", `{"type":"tool_call"}`, "names no oneOf branch"},
		// The direction-narrowed unions must reject in the schema what the
		// Go validators reject at runtime, so a generated client cannot
		// construct wire shapes the relay refuses.
		{"structured_json in request input", "openapi.json", "InputItem", `{"type":"structured_json","json":{"total":1}}`, "names no oneOf branch"},
		{"function_result in response output", "openapi.json", "OutputItem", `{"type":"function_result","call_id":"call_1"}`, "names no oneOf branch"},
		{"non-assistant message in response output", "openapi.json", "OutputItem", `{"type":"message","role":"user","content":[{"type":"text","text":"hi"}]}`, "not one of the enum values"},
		{"function_result announced as an item shell", "openapi.json", "ItemShell", `{"type":"function_result","call_id":"call_1"}`, "names no oneOf branch"},
		{"wrong primitive type", "openapi.json", "ResponseTextDelta", `{"output_index":"0","delta":"hi"}`, "not of type integer"},
		{"below minimum", "asyncapi.json", "TTSUtteranceStarted", `{"type":"utterance.started","sequence":0}`, "below minimum"},
		{"fractional integer", "asyncapi.json", "TTSUtteranceStarted", `{"type":"utterance.started","sequence":1.5}`, "not of type integer"},
	} {
		doc := docs[tc.doc]
		var value any
		if err := json.Unmarshal([]byte(tc.value), &value); err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		err := doc.validate(doc.schema(t, tc.schema), value, tc.name)
		assertInvalid(t, err, tc.want)
	}
}
