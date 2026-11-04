package mistral

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// The session frames as the live socket sent them on 2026-10-21.
const (
	createdFrame  = `{"type":"session.created","session":{"request_id":"ws-01a15e2a-fffe-77f4-be86-f37768f953a1","model":"voxtral-mini-transcribe-realtime-3-0-4","audio_format":{"encoding":"pcm_s16le","sample_rate":16000},"target_streaming_delay_ms":null,"diarize":false,"verbatim":false,"context_bias":[],"language":null,"speaker_references":null,"turn_detection":null}}`
	updatedFrame  = `{"type":"session.updated","session":{"request_id":"ws-01a15e2a-fffe-77f4-be86-f37768f953a1","model":"voxtral-mini-transcribe-realtime-3-0-4","audio_format":{"encoding":"pcm_s16le","sample_rate":16000},"target_streaming_delay_ms":480,"diarize":false,"verbatim":false,"context_bias":["Speko"],"language":"en","speaker_references":null,"turn_detection":{"start_threshold":0.5,"eager_end_threshold":0.5}}}`
	doneFrame     = `{"model":"voxtral-mini-transcribe-realtime-3-0-4","text":"Hello, this is Speco testing the new Voxtral Voice. How does it sound today?","language":null,"segments":[],"usage":{"prompt_audio_seconds":10,"prompt_tokens":40,"total_tokens":176,"completion_tokens":136},"type":"transcription.done"}`
	testRequestID = "ws-01a15e2a-fffe-77f4-be86-f37768f953a1"
)

// step is one scripted server frame, written after a pause.
type step struct {
	after time.Duration
	frame string
}

func frames(texts ...string) []step {
	steps := make([]step, 0, len(texts))
	for _, text := range texts {
		steps = append(steps, step{frame: text})
	}
	return steps
}

func delta(text string) string {
	payload, _ := json.Marshal(map[string]string{"type": "transcription.text.delta", "text": text})
	return string(payload)
}

// liveTurn480 is the first turn of the live capture at the default delay: the
// turn's last two fragments arrived 60 and 170 ms after turn.end.
var liveTurn480 = []step{
	{frame: `{"time_s": 0.0, "type": "turn.start"}`},
	{frame: delta("Hello")}, {frame: delta(",")}, {frame: delta(" this")}, {frame: delta(" is")},
	{frame: delta(" Spec")}, {frame: delta("o")}, {frame: delta(" testing")}, {frame: delta(" the")},
	{frame: delta(" new")}, {frame: delta(" V")}, {frame: delta("ox")}, {frame: delta("tral")},
	{frame: delta(" Voice")}, {frame: delta(".")}, {frame: delta(" How")}, {frame: delta(" does")},
	{frame: `{"time_s": 3.04, "type": "turn.eager_end"}`},
	{frame: delta(" it")}, {frame: delta(" sound")},
	{frame: `{"time_s": 3.6, "type": "turn.end", "end_reason": "threshold"}`},
	{after: 60 * time.Millisecond, frame: delta(" today")},
	{after: 110 * time.Millisecond, frame: delta("?")},
}

// fakeRealtime stands in for the realtime socket: it sends session.created,
// answers session.update, replays a script on the first append and another
// on the flush.
type fakeRealtime struct {
	t *testing.T
	// created replaces session.created when set; an empty slice sends none.
	created []string
	// onUpdate answers session.update; nil means session.updated.
	onUpdate []string
	onAudio  []step
	// closeAfterAudio closes the socket with this status after onAudio.
	closeAfterAudio websocket.StatusCode
	// onFlush is written after the flush, followed by transcription.done
	// unless ignoreFlush or closeOnFlush is set.
	onFlush      []step
	ignoreFlush  bool
	closeOnFlush bool
	server       *httptest.Server

	mu       sync.Mutex
	header   http.Header
	query    url.Values
	update   map[string]any
	appends  [][]byte
	controls []string
}

func newFakeRealtime(t *testing.T) *fakeRealtime {
	fake := &fakeRealtime{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc(realtimePath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeRealtime) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + realtimePath
}

func (f *fakeRealtime) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeRealtime) handle(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.header, f.query = request.Header.Clone(), request.URL.Query()
	f.mu.Unlock()
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	// A full append is 349,528 bytes of base64.
	conn.SetReadLimit(1 << 20)
	ctx := request.Context()
	created := f.created
	if created == nil {
		created = []string{createdFrame}
	}
	f.write(ctx, conn, frames(created...))
	heard := false
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var message map[string]any
		if json.Unmarshal(payload, &message) != nil {
			f.t.Errorf("client frame is not JSON: %s", payload)
			return
		}
		switch message["type"] {
		case "session.update":
			f.mu.Lock()
			f.update = message
			f.mu.Unlock()
			answer := f.onUpdate
			if answer == nil {
				answer = []string{updatedFrame}
			}
			f.write(ctx, conn, frames(answer...))
		case "input_audio.append":
			audio, err := base64.StdEncoding.DecodeString(message["audio"].(string))
			if err != nil {
				f.t.Errorf("append is not base64: %v", err)
			}
			f.mu.Lock()
			f.appends = append(f.appends, audio)
			f.mu.Unlock()
			if !heard {
				heard = true
				// The script plays out while the client keeps writing.
				go func() {
					f.write(ctx, conn, f.onAudio)
					if f.closeAfterAudio != 0 {
						_ = conn.Close(f.closeAfterAudio, "")
					}
				}()
			}
		case "input_audio.flush", "input_audio.end":
			f.mu.Lock()
			f.controls = append(f.controls, message["type"].(string))
			f.mu.Unlock()
			if message["type"] != "input_audio.flush" || f.ignoreFlush {
				continue
			}
			f.write(ctx, conn, f.onFlush)
			if f.closeOnFlush {
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			}
			f.write(ctx, conn, frames(doneFrame))
		default:
			f.t.Errorf("unexpected client frame: %s", payload)
		}
	}
}

func (f *fakeRealtime) write(ctx context.Context, conn *websocket.Conn, steps []step) {
	for _, step := range steps {
		time.Sleep(step.after)
		if conn.Write(ctx, websocket.MessageText, []byte(step.frame)) != nil {
			return
		}
	}
}

func (f *fakeRealtime) snapshot() (http.Header, url.Values, map[string]any, [][]byte, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.header, f.query, f.update, append([][]byte(nil), f.appends...), append([]string(nil), f.controls...)
}

func sttRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultSTTModel, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "mistral_test_key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "en-US"},
	}
}

func withDelay(request runtimepkg.AdapterRequest, delay any) runtimepkg.AdapterRequest {
	request.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{ProviderName: {"target_streaming_delay_ms": delay}}}
	return request
}

func newTestSTT(t *testing.T, fake *fakeRealtime, config STTConfig) *STTAdapter {
	t.Helper()
	config.AllowedEndpointHosts = []string{fake.host()}
	config.AllowInsecureEndpoint = true
	if config.SetupTimeout == 0 {
		config.SetupTimeout = 2 * time.Second
	}
	if config.CloseDrainTimeout == 0 {
		config.CloseDrainTimeout = 2 * time.Second
	}
	adapter, err := NewSTT(config)
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	return adapter
}

func openSTT(t *testing.T, fake *fakeRealtime, config STTConfig, request runtimepkg.AdapterRequest) runtimepkg.ProviderStream {
	t.Helper()
	stream, err := newTestSTT(t, fake, config).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream
}

// openAndSpeak opens a stream, consumes session.ready and the opening usage,
// and writes one append so the fake starts its script.
func openAndSpeak(t *testing.T, fake *fakeRealtime, request runtimepkg.AdapterRequest) runtimepkg.ProviderStream {
	t.Helper()
	stream := openSTT(t, fake, STTConfig{}, request)
	_ = nextSTTEvent(t, stream)
	_ = nextSTTEvent(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	return stream
}

func drainSTT(t *testing.T, stream runtimepkg.ProviderStream) []runtimepkg.ProviderEvent {
	t.Helper()
	var events []runtimepkg.ProviderEvent
	timeout := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				return events
			}
			events = append(events, event)
		case <-timeout:
			t.Fatalf("events did not close; got %d", len(events))
		}
	}
}

func nextSTTEvent(t *testing.T, stream runtimepkg.ProviderStream) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event := <-stream.Events():
		return event
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for an event")
		return runtimepkg.ProviderEvent{}
	}
}

// untilType collects events up to and including the first of the given type.
func untilType(t *testing.T, stream runtimepkg.ProviderStream, eventType protocol.EventType) []runtimepkg.ProviderEvent {
	t.Helper()
	var events []runtimepkg.ProviderEvent
	for {
		event := nextSTTEvent(t, stream)
		if event.Err != nil {
			t.Fatalf("unexpected error event: %v", event.Err)
		}
		events = append(events, event)
		if event.Type == eventType {
			return events
		}
	}
}

func dataOf(t *testing.T, event runtimepkg.ProviderEvent) map[string]any {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("event %s data %s: %v", event.Type, event.Data, err)
	}
	return data
}

func eventTypes(events []runtimepkg.ProviderEvent) string {
	var types []string
	for _, event := range events {
		types = append(types, string(event.Type))
	}
	return strings.Join(types, ",")
}

func finalText(t *testing.T, events []runtimepkg.ProviderEvent) string {
	t.Helper()
	for _, event := range events {
		if event.Type == protocol.EventTranscriptFinal {
			return dataOf(t, event)["text"].(string)
		}
	}
	t.Fatalf("no final in %s", eventTypes(events))
	return ""
}

func terminalFailure(t *testing.T, events []runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
	var failures []error
	for _, event := range events {
		if event.Err != nil {
			failures = append(failures, event.Err)
		}
	}
	var providerErr *runtimepkg.ProviderError
	if len(failures) != 1 || !errors.As(failures[0], &providerErr) {
		t.Fatalf("failures = %v, want exactly one provider error", failures)
	}
	return providerErr
}

func TestOpenConfiguresTheSessionAndWaitsForSessionUpdated(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	request := sttRequest(fake.endpoint())
	request.Options.STT = &protocol.SttOptions{Keywords: []string{"Speko", "Voxtral"}}
	stream := openSTT(t, fake, STTConfig{}, request)
	header, query, update, _, _ := fake.snapshot()
	if header.Get("Authorization") != "Bearer mistral_test_key" {
		t.Fatalf("handshake headers = %v", header)
	}
	if query.Encode() != "model=voxtral-mini-transcribe-realtime-3-0" {
		t.Fatalf("query = %s", query.Encode())
	}
	session, _ := json.Marshal(update["session"])
	want := `{"audio_format":{"encoding":"pcm_s16le","sample_rate":16000},"context_bias":["Speko","Voxtral"],"language":"en","target_streaming_delay_ms":480,"turn_detection":{}}`
	if string(session) != want {
		t.Fatalf("session.update = %s, want %s", session, want)
	}
	ready := nextSTTEvent(t, stream)
	if data := dataOf(t, ready); ready.Type != protocol.EventSessionReady || data["provider_model"] != DefaultSTTModel || data["provider_request_id"] != testRequestID {
		t.Fatalf("first event = %s %s, want session.ready", ready.Type, ready.Data)
	}
	usage := nextSTTEvent(t, stream)
	if usage.Type != protocol.EventUsageObserved || usage.Billing == nil || usage.Billing.Complete || usage.Billing.Language != "en" || usage.Billing.ProviderRequestID != testRequestID {
		t.Fatalf("opening usage = %s %+v", usage.Type, usage.Billing)
	}
	if err := usage.Billing.Validate(); err != nil {
		t.Fatalf("opening billing invalid: %v", err)
	}
}

// With no language the field is omitted and the model detects it; the
// provider option replaces the default delay.
func TestOpenOmitsAnUnsetLanguageAndForwardsTheDelay(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	request := withDelay(sttRequest(fake.endpoint()), float64(1200))
	request.Options.Language = ""
	openSTT(t, fake, STTConfig{}, request)
	_, _, update, _, _ := fake.snapshot()
	session := update["session"].(map[string]any)
	if _, present := session["language"]; present || session["target_streaming_delay_ms"] != float64(1200) {
		t.Fatalf("session.update = %v", session)
	}
	if _, present := session["context_bias"]; present {
		t.Fatalf("empty keywords were sent: %v", session)
	}
}

// At 480 ms the turn's last words land after turn.end: the final waits for
// them, then speech.ended carries turn.end's audio time.
func TestFinalWaitsForWordsThatTrailTurnEnd(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = liveTurn480
	stream := openAndSpeak(t, fake, sttRequest(fake.endpoint()))
	events := untilType(t, stream, protocol.EventSpeechEnded)
	if got := finalText(t, events); got != "Hello, this is Speco testing the new Voxtral Voice. How does it sound today?" {
		t.Fatalf("final = %q", got)
	}
	types := eventTypes(events)
	if !strings.HasPrefix(types, "speech.started,transcript.delta,") || !strings.HasSuffix(types, "transcript.delta,transcript.final,speech.ended") || strings.Count(types, "transcript.delta") != 20 {
		t.Fatalf("events = %s", types)
	}
	if data := dataOf(t, events[0]); data["audio_start_ms"] != float64(0) || data["turn_id"] != float64(0) {
		t.Fatalf("speech.started = %v", data)
	}
	if data := dataOf(t, events[2]); data["text"] != "Hello," || data["is_final"] != false {
		t.Fatalf("cumulative delta = %v", data)
	}
	final := dataOf(t, events[len(events)-2])
	if final["is_final"] != true || final["speech_final"] != true || final["language"] != "en" || final["audio_end_ms"] != float64(3600) || final["provider_request_id"] != testRequestID {
		t.Fatalf("final = %v", final)
	}
	if _, reported := final["confidence"]; reported {
		t.Fatalf("final invents a confidence: %v", final)
	}
	if data := dataOf(t, events[len(events)-1]); data["audio_end_ms"] != float64(3600) || data["reason"] != "end_of_turn" {
		t.Fatalf("speech.ended = %v", data)
	}
}

// At 1200 ms the trailing words come about half a second after turn.end,
// further apart than the 480 ms deadline would wait. The deadline follows
// the configured delay.
func TestFinalDeadlineFollowsTheStreamingDelay(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = []step{
		{frame: `{"time_s": 0.2, "type": "turn.start"}`},
		{frame: delta("How")}, {frame: delta(" does")}, {frame: delta(" it")},
		{frame: `{"time_s": 1.4, "type": "turn.end", "end_reason": "threshold"}`},
		{after: 400 * time.Millisecond, frame: delta(" sound")},
		{after: 400 * time.Millisecond, frame: delta(" today")},
		{after: 50 * time.Millisecond, frame: delta("?")},
	}
	stream := openAndSpeak(t, fake, withDelay(sttRequest(fake.endpoint()), "1200"))
	events := untilType(t, stream, protocol.EventSpeechEnded)
	if got := finalText(t, events); got != "How does it sound today?" {
		t.Fatalf("final = %q", got)
	}
}

// liveTurn1200 is the second turn of a live capture at 1200 ms: turn.end
// came 250 ms after a mid-turn "Voice." and with the next sentence's first
// word, whose last fragment trailed turn.end by 810 ms.
var liveTurn1200 = []step{
	{frame: `{"time_s": 3.84, "type": "turn.start"}`},
	{frame: delta(" Hello")}, {frame: delta(",")}, {frame: delta(" this")}, {frame: delta(" is")},
	{frame: delta(" Spe")}, {frame: delta("ko")}, {frame: delta(" testing")}, {frame: delta(" the")},
	{frame: delta(" new")}, {frame: delta(" V")}, {frame: delta("ox")}, {frame: delta("tral")},
	{frame: `{"time_s": 7.44, "type": "turn.eager_end"}`},
	{frame: delta(" Voice")}, {frame: delta(".")},
	{after: 250 * time.Millisecond, frame: `{"time_s": 8.0, "type": "turn.end", "end_reason": "threshold"}`},
	// Live it arrived with turn.end; 30 ms apart, the turn would already
	// have been published if the quiet window ran from the last delta.
	{after: 30 * time.Millisecond, frame: delta(" How")},
	{after: 110 * time.Millisecond, frame: delta(" does")},
	{after: 40 * time.Millisecond, frame: delta(" it")},
	{after: 250 * time.Millisecond, frame: delta(" sound")},
	{after: 310 * time.Millisecond, frame: delta(" today")},
	{after: 100 * time.Millisecond, frame: delta("?")},
}

// A sentence that ended well before turn.end does not end the turn: the
// quiet window starts at turn.end, and the words that follow it join.
func TestTurnEndAfterAMidTurnSentenceWaitsForTheRest(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = liveTurn1200
	stream := openAndSpeak(t, fake, withDelay(sttRequest(fake.endpoint()), 1200))
	events := untilType(t, stream, protocol.EventSpeechEnded)
	if got := finalText(t, events); got != "Hello, this is Speko testing the new Voxtral Voice. How does it sound today?" {
		t.Fatalf("final = %q", got)
	}
}

// Without punctuation the final goes out at the delay plus the grace after
// turn.end, and not before.
func TestFinalWithoutPunctuationWaitsForTheDeadline(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.1, "type": "turn.start"}`, delta("thanks"), delta(" for calling"), `{"time_s": 1.1, "type": "turn.end", "end_reason": "timeout"}`)
	stream := openAndSpeak(t, fake, withDelay(sttRequest(fake.endpoint()), 80))
	untilType(t, stream, protocol.EventTranscriptDelta)
	untilType(t, stream, protocol.EventTranscriptDelta)
	started := time.Now()
	events := untilType(t, stream, protocol.EventSpeechEnded)
	if elapsed := time.Since(started); elapsed < 180*time.Millisecond {
		t.Fatalf("final went out %s after the last delta, before the 230 ms deadline", elapsed)
	}
	if got := finalText(t, events); got != "thanks for calling" {
		t.Fatalf("final = %q", got)
	}
}

// Punctuation does not end a turn: a sentence that ends before turn.end is
// joined by the next one, arriving 300 ms after it at a 1200 ms delay, and
// the final waits for the deadline.
func TestPunctuationDoesNotEndATurn(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = []step{
		{frame: `{"time_s": 0.1, "type": "turn.start"}`}, {frame: delta("First sentence.")},
		{frame: `{"time_s": 1.1, "type": "turn.end", "end_reason": "threshold"}`},
		{after: 300 * time.Millisecond, frame: delta(" Second sentence.")},
	}
	stream := openAndSpeak(t, fake, withDelay(sttRequest(fake.endpoint()), 1200))
	untilType(t, stream, protocol.EventTranscriptDelta)
	started := time.Now()
	events := untilType(t, stream, protocol.EventSpeechEnded)
	if elapsed := time.Since(started); elapsed < 1200*time.Millisecond {
		t.Fatalf("final went out %s after turn.end, before the 1350 ms deadline", elapsed)
	}
	if got := finalText(t, events); got != "First sentence. Second sentence." {
		t.Fatalf("final = %q", got)
	}
	if strings.Count(eventTypes(events), "transcript.final") != 1 {
		t.Fatalf("events = %s, want one final", eventTypes(events))
	}
}

// A turn that ends without a word still ends with an empty final, so a
// caller can tell silence from loss.
func TestEmptyTurnEndsWithAnEmptyFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, `{"time_s": 0.6, "type": "turn.end", "end_reason": "timeout"}`)
	stream := openAndSpeak(t, fake, withDelay(sttRequest(fake.endpoint()), 80))
	events := untilType(t, stream, protocol.EventSpeechEnded)
	if got := eventTypes(events); got != "speech.started,transcript.final,speech.ended" || finalText(t, events) != "" {
		t.Fatalf("events = %s", got)
	}
}

// The next turn.start closes a turn whose final is still owed; the words
// after it belong to the new turn.
func TestTurnStartClosesThePendingTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = []step{
		{frame: `{"time_s": 0.0, "type": "turn.start"}`}, {frame: delta("Hello.")},
		{frame: `{"time_s": 0.5, "type": "turn.end", "end_reason": "threshold"}`},
		{after: 200 * time.Millisecond, frame: `{"time_s": 0.7, "type": "turn.start"}`},
		{after: 100 * time.Millisecond, frame: delta(" Next request.")},
		{frame: `{"time_s": 1.5, "type": "turn.end", "end_reason": "threshold"}`},
	}
	stream := openAndSpeak(t, fake, sttRequest(fake.endpoint()))
	first := untilType(t, stream, protocol.EventSpeechEnded)
	second := untilType(t, stream, protocol.EventSpeechEnded)
	if finalText(t, first) != "Hello." || finalText(t, second) != "Next request." {
		t.Fatalf("finals = %q / %q", finalText(t, first), finalText(t, second))
	}
	if got := eventTypes(second); got != "speech.started,transcript.delta,transcript.final,speech.ended" {
		t.Fatalf("second turn = %s", got)
	}
	if data := dataOf(t, second[0]); data["turn_id"] != float64(1) || data["audio_start_ms"] != float64(700) {
		t.Fatalf("second speech.started = %v", data)
	}
}

// Three turns in quick succession give three finals, in order.
func TestRapidTurnsEachGetTheirFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	for index, word := range []string{"One.", " Two.", " Three."} {
		fake.onAudio = append(fake.onAudio,
			step{frame: fmt.Sprintf(`{"time_s": %d.0, "type": "turn.start"}`, index)},
			step{frame: delta(word)},
			step{frame: fmt.Sprintf(`{"time_s": %d.5, "type": "turn.end", "end_reason": "threshold"}`, index)},
		)
	}
	stream := openAndSpeak(t, fake, withDelay(sttRequest(fake.endpoint()), 80))
	var finals []string
	for turn := range 3 {
		events := untilType(t, stream, protocol.EventSpeechEnded)
		if data := dataOf(t, events[len(events)-2]); data["turn_id"] != float64(turn) {
			t.Fatalf("final %d = %v", turn, data)
		}
		finals = append(finals, finalText(t, events))
	}
	if strings.Join(finals, "|") != "One.|Two.|Three." {
		t.Fatalf("finals = %q", finals)
	}
}

// A turn's membership follows arrival times, not how fast the consumer
// drains: with a one-slot event buffer and a paused consumer, the words that
// arrived inside the deadline still join the turn.
func TestAStalledConsumerDoesNotMoveWordsToTheNextTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, delta("First"), `{"time_s": 0.4, "type": "turn.end", "end_reason": "threshold"}`, delta(" still"), delta(" tail"))
	stream := openSTT(t, fake, STTConfig{EventBuffer: 1}, withDelay(sttRequest(fake.endpoint()), 80))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	// The consumer takes session.ready, the opening usage and
	// speech.started, then pauses: the first delta fills the buffer, and
	// readLoop is still blocked publishing " still" when the 230 ms deadline
	// passes, with " tail" already received.
	events := untilType(t, stream, protocol.EventSpeechStarted)
	time.Sleep(300 * time.Millisecond)
	events = append(events, untilType(t, stream, protocol.EventSpeechEnded)...)
	if got := finalText(t, events); got != "First still tail" {
		t.Fatalf("final = %q", got)
	}
}

// A socket that dies before transcription.done still publishes the text it
// delivered, then the failure.
func TestLostSocketPublishesTheOpenTurnBeforeTheFailure(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*fakeRealtime){
		"drain timeout": func(fake *fakeRealtime) { fake.ignoreFlush = true },
		"closed":        func(fake *fakeRealtime) { fake.closeOnFlush = true },
	}
	for name, configure := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeRealtime(t)
			fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, delta("half a sentence"))
			configure(fake)
			stream := openSTT(t, fake, STTConfig{CloseDrainTimeout: 150 * time.Millisecond}, sttRequest(fake.endpoint()))
			if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
				t.Fatal(err)
			}
			untilType(t, stream, protocol.EventTranscriptDelta)
			if err := stream.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			events := drainSTT(t, stream)
			if got := eventTypes(events); got != "transcript.final,speech.ended," {
				t.Fatalf("events = %s", got)
			}
			if got := finalText(t, events); got != "half a sentence" {
				t.Fatalf("final = %q", got)
			}
			if events[2].Err == nil {
				t.Fatal("no failure after the final")
			}
		})
	}
}

// With the consumer stalled, frames are still assigned by when they
// arrived: a word inside the deadline joins the ended turn, and one that
// arrived after it opens a turn of its own, however late readLoop gets to
// either.
func TestAStalledConsumerStillAssignsByArrival(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, delta("Hel"), delta("lo"), `{"time_s": 0.4, "type": "turn.end", "end_reason": "threshold"}`)
	fake.onAudio = append(fake.onAudio, step{after: 50 * time.Millisecond, frame: delta(" still")}, step{after: 400 * time.Millisecond, frame: delta(" orphan")})
	stream := openSTT(t, fake, STTConfig{EventBuffer: 1}, withDelay(sttRequest(fake.endpoint()), 80))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	// Paused after speech.started, the consumer leaves readLoop blocked on
	// the second delta while every other frame arrives.
	untilType(t, stream, protocol.EventSpeechStarted)
	time.Sleep(700 * time.Millisecond)
	first := untilType(t, stream, protocol.EventSpeechEnded)
	second := untilType(t, stream, protocol.EventTranscriptDelta)
	if finalText(t, first) != "Hello still" || eventTypes(second) != "speech.started,transcript.delta" || dataOf(t, second[1])["text"] != "orphan" {
		t.Fatalf("turns = %s %q / %s", eventTypes(first), finalText(t, first), eventTypes(second))
	}
}

// After an error frame has reported the failure, nothing follows it, not
// even when the drain later times out.
func TestNothingFollowsAReportedFailure(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, delta("words"), `{"type":"error","error":{"message":"internal error","code":1000}}`)
	fake.ignoreFlush = true
	stream := openSTT(t, fake, STTConfig{CloseDrainTimeout: 100 * time.Millisecond}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	for event := nextSTTEvent(t, stream); event.Err == nil; event = nextSTTEvent(t, stream) {
	}
	_ = stream.Close(context.Background())
	if rest := drainSTT(t, stream); len(rest) != 0 {
		t.Fatalf("events after the failure = %s", eventTypes(rest))
	}
}

// A drain timeout under backpressure still delivers the open turn's final,
// speech.ended and the failure, in that order, to a consumer that is slow
// but still draining.
func TestDrainTimeoutDeliversEverythingToASlowConsumer(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, delta("half"))
	fake.ignoreFlush = true
	stream := openSTT(t, fake, STTConfig{EventBuffer: 1, CloseDrainTimeout: 100 * time.Millisecond}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	untilType(t, stream, protocol.EventTranscriptDelta)
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1_500 * time.Millisecond)
	events := drainSTT(t, stream)
	if got := eventTypes(events); got != "transcript.final,speech.ended," || events[2].Err == nil {
		t.Fatalf("events = %s", got)
	}
}

// A consumer that stops draining Events fails the session once the frames
// read off the socket pass their bound, rather than buffering without end.
func TestUnpublishedFramesAreBounded(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = []step{{frame: `{"time_s": 0.0, "type": "turn.start"}`}}
	for range 20 {
		fake.onAudio = append(fake.onAudio, step{frame: delta(" word")})
	}
	stream := openSTT(t, fake, STTConfig{EventBuffer: 1, MaxPendingFrameBytes: 256}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	terminal := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
	var providerErr *runtimepkg.ProviderError
	if !errors.As(terminal, &providerErr) || !strings.Contains(providerErr.Message, "backed up") || providerErr.Retryable {
		t.Fatalf("terminal error = %v, want the backlog failure", terminal)
	}
	if failure := terminalFailure(t, drainSTT(t, stream)); failure != providerErr {
		t.Fatalf("published failure = %v, want the terminal error", failure)
	}
}

// CommitAudio sends nothing. Close sends the flush then the end, publishes
// the turn still open when transcription.done arrives, and completes the
// billing with the bytes sent.
func TestCloseFlushesAndPublishesTheOpenTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAudio = frames(`{"time_s": 0.0, "type": "turn.start"}`, delta("I can help"))
	fake.onFlush = frames(delta(" you"), delta(" reset it."))
	stream := openAndSpeak(t, fake, sttRequest(fake.endpoint()))
	untilType(t, stream, protocol.EventTranscriptDelta)
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	events := drainSTT(t, stream)
	if got := eventTypes(events); got != "transcript.delta,transcript.delta,transcript.final,speech.ended,usage.observed" {
		t.Fatalf("events after Close = %s", got)
	}
	if got := finalText(t, events); got != "I can help you reset it." {
		t.Fatalf("final = %q", got)
	}
	if _, present := dataOf(t, events[3])["audio_end_ms"]; present {
		t.Fatalf("speech.ended invents an end time: %s", events[3].Data)
	}
	billing := events[4].Billing
	// 3,200 bytes of 16 kHz PCM16 is a tenth of a second.
	if billing == nil || !billing.Complete || billing.OperationID != "stream" || billing.ProviderRequestID != testRequestID || billing.Quantities["duration_seconds"] != 3_200 || billing.QuantityDenominator("duration_seconds") != 32_000 {
		t.Fatalf("closing billing = %+v", billing)
	}
	if err := billing.Validate(); err != nil {
		t.Fatalf("closing billing invalid: %v", err)
	}
	if _, _, _, _, controls := fake.snapshot(); strings.Join(controls, ",") != "input_audio.flush,input_audio.end" {
		t.Fatalf("controls = %v", controls)
	}
	if err := stream.WriteAudio(context.Background(), []byte{0, 0}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after close = %v", err)
	}
	if err := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(); err != nil {
		t.Fatalf("clean close left a terminal error: %v", err)
	}
}

func TestWriteAudioSplitsAtTheAppendCapOnWholeSamples(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	request := sttRequest(fake.endpoint())
	request.Media.SampleRateHz = 24_000
	stream := openSTT(t, fake, STTConfig{}, request)
	if err := stream.WriteAudio(context.Background(), make([]byte, 600_001)); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteAudio(context.Background(), make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	_, _, update, appends, _ := fake.snapshot()
	if format := update["session"].(map[string]any)["audio_format"].(map[string]any); format["sample_rate"] != float64(24_000) {
		t.Fatalf("audio_format = %v", format)
	}
	var sizes []int
	for _, audio := range appends {
		sizes = append(sizes, len(audio))
	}
	if len(sizes) != 4 || sizes[0] != maxAppendBytes || sizes[1] != maxAppendBytes || sizes[2] != 600_000-2*maxAppendBytes || sizes[3] != 4 {
		t.Fatalf("append sizes = %v", sizes)
	}
	billing := events[len(events)-1].Billing
	if billing == nil || billing.Quantities["duration_seconds"] != 600_004 || billing.QuantityDenominator("duration_seconds") != 48_000 {
		t.Fatalf("billing = %+v, want the bytes sent at 24 kHz", billing)
	}
}

func TestErrorFramesEndTheSession(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		frame     string
		code      string
		retryable bool
	}{
		{name: "validation", frame: `{"type":"error","error":{"message":"1 validation error for tagged-union","code":3051}}`, code: "invalid_request"},
		{name: "object message", frame: `{"type":"error","error":{"message":{"detail":"Invalid API key"},"code":1001}}`, code: "authentication_failed"},
		{name: "unknown", frame: `{"type":"error","error":{"message":"internal error","code":1000}}`, code: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeRealtime(t)
			fake.onAudio = frames(tc.frame)
			fake.closeAfterAudio = websocket.StatusInternalError
			stream := openAndSpeak(t, fake, sttRequest(fake.endpoint()))
			events := drainSTT(t, stream)
			for _, event := range events {
				if event.Billing != nil && event.Billing.Complete {
					t.Fatal("billing completed without transcription.done")
				}
			}
			failure := terminalFailure(t, events)
			if failure.Code != tc.code || failure.Retryable != tc.retryable || strings.Contains(failure.Message, "tagged-union") {
				t.Fatalf("failure = %#v, want %s retryable=%v without the vendor text", failure, tc.code, tc.retryable)
			}
			if stream.(runtimepkg.TerminalErrorProviderStream).TerminalError() == nil {
				t.Fatal("terminal error not preserved")
			}
		})
	}
}

func TestMidSessionCloseIsAFailure(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.closeAfterAudio = websocket.StatusInternalError
	stream := openAndSpeak(t, fake, sttRequest(fake.endpoint()))
	failure := terminalFailure(t, drainSTT(t, stream))
	if failure.Code != "provider_unavailable" || !failure.Retryable {
		t.Fatalf("failure = %#v", failure)
	}
}

func TestRefusedSessionUpdateFailsOpen(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onUpdate = []string{`{"type":"error","error":{"message":"target_streaming_delay_ms must be one of the trained values: ['80-1200', '2400'] (got 5000).","code":3051}}`}
	_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" || providerErr.Retryable {
		t.Fatalf("Open error = %#v, want invalid_request", err)
	}
}

func TestOpenTimesOutWithoutSessionUpdated(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onUpdate = []string{}
	_, err := newTestSTT(t, fake, STTConfig{SetupTimeout: 150 * time.Millisecond}).Open(context.Background(), sttRequest(fake.endpoint()))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("Open error = %#v, want a retryable missing acknowledgement", err)
	}
}

func TestHandshakeRejectionIsClassified(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status int
		body   string
		code   string
	}{
		// As the live upgrade answered on 2026-10-21: a bad key, then a model
		// the key cannot open.
		{http.StatusUnauthorized, `{"detail":"Invalid API Key"}`, "authentication_failed"},
		{http.StatusForbidden, ``, "authentication_failed"},
		{http.StatusTooManyRequests, `{"object":"error","message":"Requests rate limit exceeded","type":"rate_limited"}`, "provider_rate_limited"},
	}
	for _, tc := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		t.Cleanup(server.Close)
		parsed, _ := url.Parse(server.URL)
		adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.Open(context.Background(), sttRequest("ws"+strings.TrimPrefix(server.URL, "http")+realtimePath))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.ProviderStatus != tc.status {
			t.Fatalf("%d: Open error = %#v, want %s", tc.status, err, tc.code)
		}
		if strings.Contains(providerErr.Message, "Invalid") {
			t.Fatalf("message quotes the vendor text: %q", providerErr.Message)
		}
	}
}

// The upgrade never follows a redirect, so the key is never replayed to a
// URL the endpoint policy did not check, a plain-HTTP one included.
func TestHandshakeDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	followed := make(chan string, 4)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(target.Close)
	for _, location := range []string{target.URL + realtimePath, "ws" + strings.TrimPrefix(target.URL, "http") + realtimePath} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, location, http.StatusFound)
		}))
		t.Cleanup(server.Close)
		parsed, _ := url.Parse(server.URL)
		adapter, err := NewSTT(STTConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.Open(context.Background(), sttRequest("wss"+strings.TrimPrefix(server.URL, "https")+realtimePath))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.ProviderStatus != http.StatusFound {
			t.Fatalf("redirect to %s: Open error = %#v, want the 302 refused", location, err)
		}
	}
	select {
	case authorization := <-followed:
		t.Fatalf("the redirect was followed, carrying %q", authorization)
	default:
	}
}

func TestCloseDrainTimeoutReportsTheMissingDone(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.ignoreFlush = true
	stream := openSTT(t, fake, STTConfig{CloseDrainTimeout: 100 * time.Millisecond}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	for _, event := range events {
		if event.Billing != nil && event.Billing.Complete {
			t.Fatal("billing completed without transcription.done")
		}
	}
	if failure := terminalFailure(t, events); failure.Code != "request_timeout" {
		t.Fatalf("failure = %#v, want request_timeout", failure)
	}
}

// A normal close after the flush but before transcription.done is a
// truncated session: it fails retryably and billing stays incomplete.
func TestNormalCloseBeforeDoneIsAFailure(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.closeOnFlush = true
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	for _, event := range events {
		if event.Billing != nil && event.Billing.Complete {
			t.Fatal("billing completed without transcription.done")
		}
	}
	if failure := terminalFailure(t, events); failure.Code != "provider_unavailable" || !failure.Retryable {
		t.Fatalf("failure = %#v, want retryable provider_unavailable", failure)
	}
}

func TestOpenRefusesWhatTheSocketCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	base := sttRequest("wss://api.mistral.ai/v1/audio/transcriptions/realtime")
	cases := map[string]func(*runtimepkg.AdapterRequest){
		"11025 Hz": func(r *runtimepkg.AdapterRequest) {
			r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 11_025, Channels: 1}
		},
		"stereo": func(r *runtimepkg.AdapterRequest) {
			r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 2}
		},
		"swahili":     func(r *runtimepkg.AdapterRequest) { r.Options.Language = "sw-KE" },
		"batch model": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = BatchMiniModel },
		"wrong path":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://api.mistral.ai/v1/realtime" },
		"foreign host": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "wss://example.com/v1/audio/transcriptions/realtime"
		},
		"delay 5000":    func(r *runtimepkg.AdapterRequest) { *r = withDelay(*r, 5000) },
		"delay 40":      func(r *runtimepkg.AdapterRequest) { *r = withDelay(*r, "40") },
		"delay unknown": func(r *runtimepkg.AdapterRequest) { *r = withDelay(*r, "fast") },
		"relay key off relay": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
		},
	}
	for name, mutate := range cases {
		request := base
		media := *base.Media
		request.Media = &media
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Fatalf("%s: Open accepted", name)
		}
	}
	coded := map[string]string{"11025 Hz": "unsupported_media", "swahili": "unsupported_language", "delay 5000": "invalid_request"}
	for name, code := range coded {
		request := base
		media := *base.Media
		request.Media = &media
		cases[name](&request)
		_, err := adapter.Open(context.Background(), request)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != code || providerErr.Hint == "" {
			t.Fatalf("%s: Open error = %#v, want %s with a hint", name, err, code)
		}
	}
}
