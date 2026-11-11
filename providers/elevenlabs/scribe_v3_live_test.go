package elevenlabs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

func scribeLiveRequest(serverURL, model string, stt *protocol.SttOptions) runtimepkg.AdapterRequest {
	request := sttRequest(serverURL, protocol.CredentialsBYOK)
	request.Plan.Route.Model = model
	request.Options.STT = stt
	return request
}

func TestScribeV3LiveQueryCarriesEachOption(t *testing.T) {
	t.Parallel()
	diarize := true
	const base = "audio_format=pcm_16000&commit_strategy=vad&include_timestamps=true&language_code=es&model_id=scribe_v3_live"
	cases := []struct {
		name    string
		options *protocol.SttOptions
		want    string
	}{
		{name: "no options keeps the vad commit strategy", want: base},
		{name: "turn prediction replaces the commit strategy", options: elevenlabsOptions(map[string]any{"turn_prediction": true}),
			want: "audio_format=pcm_16000&commit_strategy=turn_prediction&include_timestamps=true&language_code=es&model_id=scribe_v3_live"},
		{name: "turn prediction off is the default", options: elevenlabsOptions(map[string]any{"turn_prediction": false}), want: base},
		{name: "turn prediction keeps the vad threshold", options: elevenlabsOptions(map[string]any{"turn_prediction": true, "vad_silence_threshold_secs": 0.5}),
			want: "audio_format=pcm_16000&commit_strategy=turn_prediction&include_timestamps=true&language_code=es&model_id=scribe_v3_live&vad_silence_threshold_secs=0.5"},
		{name: "multilingual mode", options: elevenlabsOptions(map[string]any{"multilang_mode": true}),
			want: "audio_format=pcm_16000&commit_strategy=vad&include_timestamps=true&language_code=es&model_id=scribe_v3_live&multilang_mode=true"},
		{name: "multilingual mode off is omitted", options: elevenlabsOptions(map[string]any{"multilang_mode": false}), want: base},
		{name: "diarization", options: &protocol.SttOptions{Diarization: &diarize},
			want: "audio_format=pcm_16000&commit_strategy=vad&diarize=true&include_timestamps=true&language_code=es&model_id=scribe_v3_live"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			queries := make(chan string, 1)
			server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				queries <- request.URL.RawQuery
				_, _, _ = conn.Read(ctx)
			})
			defer server.Close()
			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, ModelScribeV3Live, test.options))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
			if got := <-queries; got != test.want {
				t.Fatalf("query\n got %s\nwant %s", got, test.want)
			}
		})
	}
}

// The vendor answers a v3 Live parameter on another model with invalid_request
// and a closed socket. Each one is refused before the adapter dials.
func TestScribeRealtimeRefusesUnsupportedOptionsBeforeDialing(t *testing.T) {
	t.Parallel()
	diarize := true
	cases := []struct {
		name    string
		model   string
		options *protocol.SttOptions
	}{
		{name: "turn prediction on v2", model: ModelScribeV2Realtime, options: elevenlabsOptions(map[string]any{"turn_prediction": true})},
		{name: "turn prediction off on v2", model: ModelScribeV2Realtime, options: elevenlabsOptions(map[string]any{"turn_prediction": false})},
		{name: "multilingual mode on v2", model: ModelScribeV2Realtime, options: elevenlabsOptions(map[string]any{"multilang_mode": true})},
		{name: "diarization on v2", model: ModelScribeV2Realtime, options: &protocol.SttOptions{Diarization: &diarize}},
		{name: "diarization on an unlisted model", model: "scribe_v9_realtime", options: &protocol.SttOptions{Diarization: &diarize}},
		{name: "a batch option on v3 live", model: ModelScribeV3Live, options: elevenlabsOptions(map[string]any{"numbers_format": "digits"})},
		{name: "a flag that is not a boolean", model: ModelScribeV3Live, options: elevenlabsOptions(map[string]any{"multilang_mode": "yes"})},
		{name: "a keyword past the library's term length", model: ModelScribeV3Live, options: &protocol.SttOptions{Keywords: []string{strings.Repeat("k", 51)}}},
		// The settings below are the adapter's own. The gateway refuses them
		// before routing; a caller driving the adapter directly is refused here.
		{name: "native diarize on v2", model: ModelScribeV2Realtime, options: elevenlabsOptions(map[string]any{"diarize": true})},
		{name: "native diarize on v3 live", model: ModelScribeV3Live, options: elevenlabsOptions(map[string]any{"diarize": true})},
		{name: "native commit_strategy on v2", model: ModelScribeV2Realtime, options: elevenlabsOptions(map[string]any{"commit_strategy": "turn_prediction"})},
		{name: "native commit_strategy in another case", model: ModelScribeV3Live, options: elevenlabsOptions(map[string]any{"Commit_Strategy": "manual"})},
		{name: "native keyterm_library", model: ModelScribeV3Live, options: elevenlabsOptions(map[string]any{"keyterm_library": "Speko"})},
		{name: "a second model id", model: ModelScribeV2Realtime, options: elevenlabsOptions(map[string]any{"model_id": ModelScribeV3Live})},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var dials atomic.Int32
			server := newSTTServer(t, func(context.Context, *http.Request, *websocket.Conn) { dials.Add(1) })
			defer server.Close()
			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), scribeLiveRequest(server.URL, test.model, test.options))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" || providerErr.Retryable {
				t.Fatalf("open = %v, want a non-retryable invalid_request", err)
			}
			if dials.Load() != 0 {
				t.Fatal("the adapter dialed the vendor with an option the model does not support")
			}
		})
	}
}

// A model id classified for the other surface never reaches the vendor, and
// one the table does not list is the vendor's to answer.
func TestScribeModelsAreClassifiedByTheTable(t *testing.T) {
	t.Parallel()
	var dials atomic.Int32
	socket := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		dials.Add(1)
		_, _, _ = conn.Read(ctx)
	})
	defer socket.Close()
	stt, err := NewSTT(sttTestConfig(socket.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	for _, batchModel := range []string{BatchModel, ModelScribeV3} {
		if _, err := stt.Open(context.Background(), scribeLiveRequest(socket.URL, batchModel, nil)); err == nil {
			t.Fatalf("the realtime adapter accepted batch model %q", batchModel)
		}
	}
	if dials.Load() != 0 {
		t.Fatal("the realtime adapter dialed with a batch model")
	}
	stream, err := stt.Open(context.Background(), scribeLiveRequest(socket.URL, "scribe_v9_realtime", nil))
	if err != nil {
		t.Fatalf("an unlisted realtime model must reach the vendor: %v", err)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())

	var uploads atomic.Int32
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		_, _ = w.Write([]byte(`{"language_code":"en","text":"Hi.","audio_duration_secs":1,"words":[]}`))
	}))
	defer upload.Close()
	batch, err := NewBatch(BatchConfig{HTTPClient: upload.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("batch adapter: %v", err)
	}
	request := func(model string) runtimepkg.BatchTranscribeRequest {
		plan := batchPlan(upload.URL + "/v1/speech-to-text")
		plan.Route.Model = model
		return runtimepkg.BatchTranscribeRequest{Plan: plan, Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12}
	}
	// scribe_v3_live has no "realtime" in its name, which is what the batch
	// adapter used to look for.
	for _, realtimeModel := range []string{ModelScribeV2Realtime, ModelScribeV3Live} {
		if _, err := batch.Transcribe(context.Background(), request(realtimeModel)); err == nil {
			t.Fatalf("the batch adapter accepted realtime model %q", realtimeModel)
		}
	}
	if uploads.Load() != 0 {
		t.Fatal("the batch adapter uploaded with a realtime model")
	}
	if _, err := batch.Transcribe(context.Background(), request("scribe_v2_medical")); err != nil || uploads.Load() != 1 {
		t.Fatalf("an unlisted batch model must reach the vendor: %v", err)
	}
}

// Keywords past the query's 50 terms of 20 characters ride keyterm_library on
// the first audio chunk, and on no other chunk.
func TestScribeV3LiveSendsOverflowKeywordsAsALibraryOnTheFirstChunk(t *testing.T) {
	t.Parallel()
	keywords := []string{"Speko", "a term longer than twenty characters"}
	for range 50 {
		keywords = append(keywords, "term")
	}
	for _, test := range []struct {
		model        string
		wantKeyterms int
		wantLibrary  []any
	}{
		{model: ModelScribeV3Live, wantKeyterms: 50, wantLibrary: []any{"a term longer than twenty characters", "term"}},
		// v2 has no library: every keyword stays on the query, as before.
		{model: ModelScribeV2Realtime, wantKeyterms: 52},
	} {
		t.Run(test.model, func(t *testing.T) {
			t.Parallel()
			keyterms := make(chan []string, 1)
			frames := make(chan map[string]any, 3)
			server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				keyterms <- request.URL.Query()["keyterms"]
				for range 3 {
					frame, err := readSTTJSON(ctx, conn)
					if err != nil {
						t.Errorf("read frame: %v", err)
						return
					}
					frames <- frame
				}
				_, _, _ = conn.Read(ctx)
			})
			defer server.Close()
			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, test.model, &protocol.SttOptions{Keywords: keywords}))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
			if got := <-keyterms; len(got) != test.wantKeyterms || got[0] != "Speko" {
				t.Fatalf("keyterms query carried %d terms starting %q, want %d", len(got), got[0], test.wantKeyterms)
			}
			// 20000 bytes cross the frame cap, so one write is two chunks.
			if err := stream.WriteAudio(context.Background(), make([]byte, 20_000)); err != nil {
				t.Fatalf("write audio: %v", err)
			}
			if err := stream.WriteAudio(context.Background(), make([]byte, 320)); err != nil {
				t.Fatalf("write audio: %v", err)
			}
			first := <-frames
			if got, _ := first["keyterm_library"].([]any); !reflect.DeepEqual(got, test.wantLibrary) {
				t.Fatalf("first chunk keyterm_library = %v, want %v", first["keyterm_library"], test.wantLibrary)
			}
			for range 2 {
				if later := <-frames; later["keyterm_library"] != nil {
					t.Fatalf("a later chunk carried keyterm_library: %v", later["keyterm_library"])
				}
			}
		})
	}
}

// On a diarized session the final for a segment is its diarized_transcript:
// the commit alone produces nothing, and a merge relabels what follows.
func TestScribeV3LiveDiarizedSession(t *testing.T) {
	t.Parallel()
	diarize := true
	word := func(text string, start, end float64, speaker any) map[string]any {
		return map[string]any{"text": text, "start": start, "end": end, "type": "word", "speaker_id": speaker}
	}
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, message := range []map[string]any{
			{"message_type": "session_started", "session_id": "el-live-1"},
			{"message_type": "partial_transcript", "text": "hello", "turn_probability": 0.42},
			{"message_type": "committed_transcript", "segment_index": 0, "text": "hello there hi"},
			{"message_type": "committed_transcript_with_timestamps", "text": "hello there hi", "words": []map[string]any{word("hello", 0.1, 0.4, nil)}},
			{"message_type": "committed_transcript", "segment_index": 1, "text": "fine thanks"},
			{"message_type": "diarized_transcript", "segment_index": 0, "text": "hello there hi", "words": []map[string]any{
				word("hello", 0.1, 0.4, "speaker_0"),
				{"text": " ", "start": 0.4, "end": 0.4, "type": "spacing"},
				word("there", 0.45, 0.8, "speaker_0"),
				word("hi", 1.0, 1.2, "speaker_1"),
			}},
			{"message_type": "diarized_transcript", "segment_index": 1, "text": "fine thanks", "words": []map[string]any{
				word("fine", 2.0, 2.3, "speaker_2"), word("thanks", 2.4, 2.9, nil),
			}},
			{"message_type": "speakers_merged", "from_speaker_id": "speaker_2", "to_speaker_id": "speaker_1"},
			{"message_type": "speakers_merged", "from_speaker_id": "speaker_1", "to_speaker_id": "speaker_0"},
			{"message_type": "committed_transcript", "segment_index": 2, "text": "bye"},
			{"message_type": "diarized_transcript", "segment_index": 2, "text": "bye", "words": []map[string]any{word("bye", 3.0, 3.2, "speaker_2")}},
		} {
			if err := writeSTTJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["message_type"], err)
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, ModelScribeV3Live, &protocol.SttOptions{Diarization: &diarize}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())

	events := collectSTTEvents(t, stream.Events(), 7)
	wantTypes := []protocol.EventType{
		protocol.EventUsageObserved, protocol.EventTranscriptDelta,
		protocol.EventTranscriptFinal, protocol.EventTranscriptFinal,
		protocol.EventWarning, protocol.EventWarning, protocol.EventTranscriptFinal,
	}
	for index, want := range wantTypes {
		if events[index].Type != want {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, want)
		}
	}
	if events[0].Billing == nil || events[0].Billing.Model != ModelScribeV3Live {
		t.Fatalf("billing = %+v, want the pinned scribe_v3_live", events[0].Billing)
	}
	var partial struct {
		Text            string  `json:"text"`
		TurnProbability float64 `json:"turn_probability"`
	}
	if err := json.Unmarshal(events[1].Data, &partial); err != nil || partial.Text != "hello" || partial.TurnProbability != 0.42 {
		t.Fatalf("partial = %+v (%v), want the text and its turn probability", partial, err)
	}

	type final struct {
		Text    string `json:"text"`
		IsFinal bool   `json:"is_final"`
		Speaker string `json:"speaker"`
		Words   []struct {
			Text    string `json:"text"`
			StartMs int64  `json:"start_ms"`
			EndMs   int64  `json:"end_ms"`
			Speaker string `json:"speaker"`
		} `json:"words"`
	}
	decode := func(index int) final {
		t.Helper()
		var decoded final
		if err := json.Unmarshal(events[index].Data, &decoded); err != nil {
			t.Fatalf("decode event %d: %v", index, err)
		}
		if events[index].Extensions[extensionID] == nil {
			t.Fatalf("event %d dropped the vendor frame", index)
		}
		return decoded
	}
	first := decode(2)
	if first.Text != "hello there hi" || !first.IsFinal || first.Speaker != "" || len(first.Words) != 3 {
		t.Fatalf("first final = %+v, want three words and no single speaker", first)
	}
	if first.Words[0].Speaker != "speaker_0" || first.Words[2].Speaker != "speaker_1" || first.Words[2].StartMs != 1000 || first.Words[2].EndMs != 1200 {
		t.Fatalf("first final words = %+v, want the vendor's speaker and timing per word", first.Words)
	}
	// An unresolved (null) speaker leaves the word unlabelled, and the final
	// names no speaker: the unlabelled word may be someone else's.
	second := decode(3)
	if second.Text != "fine thanks" || second.Speaker != "" || second.Words[0].Speaker != "speaker_2" || second.Words[1].Speaker != "" {
		t.Fatalf("second final = %+v, want speaker_2 on the first word only and no single speaker", second)
	}
	var merge struct {
		ProviderType string `json:"provider_type"`
		From         string `json:"from_speaker"`
		To           string `json:"to_speaker"`
	}
	if err := json.Unmarshal(events[4].Data, &merge); err != nil || merge.ProviderType != "speakers_merged" || merge.From != "speaker_2" || merge.To != "speaker_1" {
		t.Fatalf("merge = %+v (%v), want speaker_2 into speaker_1", merge, err)
	}
	// Merges chain: speaker_2 became speaker_1, which became speaker_0.
	if third := decode(6); third.Speaker != "speaker_0" || third.Words[0].Speaker != "speaker_0" {
		t.Fatalf("final after the merges = %+v, want the surviving speaker_0", third)
	}
}

// A stream that ends before a segment's diarized_transcript still delivers
// the committed words, without speakers, ahead of the billing observation.
func TestScribeV3LiveDeliversACommitWhoseSpeakersNeverArrived(t *testing.T) {
	t.Parallel()
	diarize := true
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, message := range []map[string]any{
			{"message_type": "session_started", "session_id": "el-live-2"},
			{"message_type": "committed_transcript", "segment_index": 0, "text": "last words"},
		} {
			if err := writeSTTJSON(ctx, conn, message); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, ModelScribeV3Live, &protocol.SttOptions{Diarization: &diarize}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	events := collectSTTEvents(t, stream.Events(), 3)
	if events[1].Type != protocol.EventTranscriptFinal || string(events[1].Data) != `{"is_final":true,"speech_final":true,"text":"last words"}` {
		t.Fatalf("event after the socket closed = %q %s, want the committed text as a final", events[1].Type, events[1].Data)
	}
	if events[2].Type != protocol.EventUsageObserved || events[2].Billing == nil || !events[2].Billing.Complete {
		t.Fatalf("last event = %+v, want the complete billing observation", events[2])
	}
}

// openDiarizedScript opens a diarized scribe_v3_live session against a server
// that sends script and then closes the socket normally.
func openDiarizedScript(t *testing.T, script []map[string]any) runtimepkg.ProviderStream {
	t.Helper()
	diarize := true
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, message := range script {
			if err := writeSTTJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["message_type"], err)
				return
			}
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	t.Cleanup(server.Close)
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, ModelScribeV3Live, &protocol.SttOptions{Diarization: &diarize}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	return stream
}

// drainSTTEvents reads events until the stream closes its channel.
func drainSTTEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent) []runtimepkg.ProviderEvent {
	t.Helper()
	var result []runtimepkg.ProviderEvent
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return result
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("events still open after %d events", len(result))
		}
	}
}

// finalTexts renders each transcript final as text, with "@speaker" when it
// names one, in delivery order.
func finalTexts(t *testing.T, events []runtimepkg.ProviderEvent) []string {
	t.Helper()
	var texts []string
	for _, event := range events {
		if event.Type != protocol.EventTranscriptFinal {
			continue
		}
		var final struct {
			Text    string `json:"text"`
			Speaker string `json:"speaker"`
		}
		if err := json.Unmarshal(event.Data, &final); err != nil {
			t.Fatalf("decode final %s: %v", event.Data, err)
		}
		if final.Speaker != "" {
			final.Text += "@" + final.Speaker
		}
		texts = append(texts, final.Text)
	}
	return texts
}

// eventOrder renders the transcript events and the complete billing
// observation in delivery order: "final text@speaker", "delta text", "billing".
func eventOrder(t *testing.T, events []runtimepkg.ProviderEvent) []string {
	t.Helper()
	var order []string
	for _, event := range events {
		switch {
		case event.Type == protocol.EventTranscriptFinal:
			order = append(order, "final "+finalTexts(t, []runtimepkg.ProviderEvent{event})[0])
		case event.Type == protocol.EventTranscriptDelta:
			var delta struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(event.Data, &delta); err != nil {
				t.Fatalf("decode delta %s: %v", event.Data, err)
			}
			order = append(order, "delta "+delta.Text)
		case event.Billing != nil && event.Billing.Complete:
			order = append(order, "billing")
		}
	}
	return order
}

// A segment's final waits for its speakers, and everything the vendor sent
// after the commit waits behind that final: a client that clears its interim
// text on a final must not lose the next segment's partial.
func TestScribeV3LiveLaterEventsWaitBehindAHeldFinal(t *testing.T) {
	t.Parallel()
	started := map[string]any{"message_type": "session_started", "session_id": "el-live-order"}
	partial := func(text string) map[string]any {
		return map[string]any{"message_type": "partial_transcript", "text": text}
	}
	commit := func(segment int, text string) map[string]any {
		return map[string]any{"message_type": "committed_transcript", "segment_index": segment, "text": text}
	}
	cases := []struct {
		name   string
		script []map[string]any
		want   []string
	}{
		{
			name:   "the speakers arrive",
			script: []map[string]any{started, commit(0, "A"), partial("B so"), diarizedFrame(0, "A", "speaker_0"), commit(1, "B"), diarizedFrame(1, "B", "speaker_1")},
			want:   []string{"final A@speaker_0", "delta B so", "final B@speaker_1", "billing"},
		},
		{
			name:   "a later segment's speakers release an earlier commit without them",
			script: []map[string]any{started, commit(0, "A"), partial("B so"), commit(1, "B"), partial("C so"), diarizedFrame(1, "B", "speaker_1")},
			want:   []string{"final A", "delta B so", "final B@speaker_1", "delta C so", "billing"},
		},
		{
			name:   "the speakers never arrive",
			script: []map[string]any{started, commit(0, "A"), partial("B so")},
			want:   []string{"final A", "delta B so", "billing"},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stream := openDiarizedScript(t, test.script)
			if got := eventOrder(t, drainSTTEvents(t, stream.Events())); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("events = %q, want %q", got, test.want)
			}
		})
	}
}

func diarizedFrame(segment int, text string, speaker string) map[string]any {
	var words []map[string]any
	if text != "" {
		words = []map[string]any{{"text": text, "start": 0.1, "end": 0.4, "type": "word", "speaker_id": speaker}}
	}
	return map[string]any{"message_type": "diarized_transcript", "segment_index": segment, "text": text, "words": words}
}

// A diarized_transcript yields a final only for a commit still waiting for
// it, and finals keep commit order: a frame for a later commit releases the
// earlier ones without speakers, since theirs are not coming in order.
func TestScribeV3LiveDiarizedFinalsFollowTheirCommits(t *testing.T) {
	t.Parallel()
	stream := openDiarizedScript(t, []map[string]any{
		{"message_type": "committed_transcript", "segment_index": 0, "text": "A"},
		{"message_type": "committed_transcript", "segment_index": 1, "text": "B"},
		diarizedFrame(1, "B", "speaker_1"),
		diarizedFrame(1, "B", "speaker_1"),
		diarizedFrame(0, "A", "speaker_0"),
		diarizedFrame(7, "Z", "speaker_0"),
	})
	got := finalTexts(t, drainSTTEvents(t, stream.Events()))
	if want := []string{"A", "B@speaker_1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finals = %q, want %q", got, want)
	}
}

// A diarized_transcript without text does not erase the committed words: the
// commit's text is the final, without speakers. Both empty is no final.
func TestScribeV3LiveEmptyDiarizedFrameKeepsTheCommittedText(t *testing.T) {
	t.Parallel()
	stream := openDiarizedScript(t, []map[string]any{
		{"message_type": "committed_transcript", "segment_index": 0, "text": "hello"},
		diarizedFrame(0, "", ""),
		{"message_type": "committed_transcript", "segment_index": 1, "text": ""},
		diarizedFrame(1, "", ""),
	})
	got := finalTexts(t, drainSTTEvents(t, stream.Events()))
	if want := []string{"hello"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finals = %q, want %q", got, want)
	}
}

// A commit still waiting for its speakers when the caller closes was received
// before the stream's audio was final, so it and the partial held behind it
// reach the consumer ahead of the complete billing observation even if its
// speakers never arrive.
func TestScribeV3LiveCloseDeliversAHeldCommitBeforeBilling(t *testing.T) {
	t.Parallel()
	diarize := true
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, message := range []map[string]any{
			{"message_type": "session_started", "session_id": "el-live-3"},
			{"message_type": "committed_transcript", "segment_index": 0, "text": "hello"},
			{"message_type": "partial_transcript", "text": "next"},
		} {
			if err := writeSTTJSON(ctx, conn, message); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
		for {
			frame, err := readSTTJSON(ctx, conn)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			if frame["commit"] == true {
				break
			}
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, ModelScribeV3Live, &protocol.SttOptions{Diarization: &diarize}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	collectSTTEvents(t, stream.Events(), 1)
	awaitHeld(t, stream, 2)
	if err := stream.WriteAudio(context.Background(), make([]byte, 320)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	events := drainSTTEvents(t, stream.Events())
	if got, want := eventOrder(t, events), []string{"final hello", "delta next", "billing"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after close = %q, want %q", got, want)
	}
	if string(events[0].Data) != `{"is_final":true,"speech_final":true,"text":"hello"}` {
		t.Fatalf("held final = %s, want the committed text without speakers", events[0].Data)
	}
}

// awaitHeld waits until a diarized stream holds count events, which the
// consumer cannot see while a commit ahead of them waits for its speakers.
func awaitHeld(t *testing.T, stream runtimepkg.ProviderStream, count int) {
	t.Helper()
	s := stream.(*sttStream)
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.billingMu.Lock()
		held := len(s.held)
		s.billingMu.Unlock()
		if held >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream holds %d events, want %d", held, count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The diarization frames mean nothing on a session that did not ask for
// speakers, so they stay visible as ignored frames and a commit is a final.
func TestScribeV3LiveWithoutDiarizationFinalizesOnTheCommit(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, message := range []map[string]any{
			{"message_type": "committed_transcript", "text": "hello"},
			{"message_type": "speakers_merged", "from_speaker_id": "speaker_1", "to_speaker_id": "speaker_0"},
		} {
			if err := writeSTTJSON(ctx, conn, message); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), scribeLiveRequest(server.URL, ModelScribeV3Live, nil))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
	events := collectSTTEvents(t, stream.Events(), 2)
	if events[0].Type != protocol.EventTranscriptFinal || events[1].Type != protocol.EventWarning ||
		!strings.Contains(string(events[1].Data), "ignored ElevenLabs message type") {
		t.Fatalf("events = %q %s, %q %s", events[0].Type, events[0].Data, events[1].Type, events[1].Data)
	}
}
