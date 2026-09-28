package soniox

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// translatedData is the normalized payload a translating session emits:
// the original words in text and the translated words beside them.
type translatedData struct {
	Text         string  `json:"text"`
	Translation  *string `json:"translation"`
	IsFinal      bool    `json:"is_final"`
	AudioStartMS *int64  `json:"audio_start_ms"`
	AudioEndMS   *int64  `json:"audio_end_ms"`
}

func decodeTranslated(t *testing.T, data json.RawMessage) translatedData {
	t.Helper()
	var decoded translatedData
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode transcript: %v", err)
	}
	return decoded
}

func translationValue(data translatedData) string {
	if data.Translation == nil {
		return "<absent>"
	}
	return *data.Translation
}

// The start frame carries Soniox's one_way block, with the target reduced to
// the bare code Soniox's language table lists.
func TestSTTTranslationRidesTheStartFrame(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		target string
		want   string
	}{
		{target: "es", want: "es"},
		{target: "pt-BR", want: "pt"},
		{target: "nb", want: "no"},
	} {
		t.Run(testCase.target, func(t *testing.T) {
			t.Parallel()
			starts := make(chan map[string]any, 1)
			server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
				start, err := readJSONObject(ctx, conn)
				if err != nil {
					t.Errorf("read start request: %v", err)
					return
				}
				starts <- start
				waitForPeer(ctx, conn)
			})
			defer server.Close()

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttAdapterRequest(server.URL)
			request.Options.STT = &protocol.SttOptions{Translation: &protocol.SttTranslation{TargetLanguage: testCase.target}}
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer abortStream(stream)

			start := mustReceiveObject(t, starts)
			translation, ok := start["translation"].(map[string]any)
			if !ok {
				t.Fatalf("start frame has no translation block: %v", start)
			}
			if translation["type"] != "one_way" || translation["target_language"] != testCase.want || len(translation) != 2 {
				t.Fatalf("translation block = %v, want one_way to %s", translation, testCase.want)
			}
		})
	}
}

// An untranslated session sends no translation key at all: Soniox would
// otherwise be asked to translate into an empty language.
func TestSTTStartFrameWithoutTranslationHasNoTranslationKey(t *testing.T) {
	t.Parallel()
	if sttTranslationFor("") != nil {
		t.Fatal("no target must produce no translation block")
	}
	payload, err := json.Marshal(sttStartRequest{Model: "stt-rt-v5"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "translation") {
		t.Fatalf("untranslated start frame mentions translation: %s", payload)
	}
}

// The recorded shape of a translating session: originals first, translations
// following in chunks that do not map one to one, translated tokens with no
// timestamps, and one translated chunk landing AFTER the <end> that closed
// its segment. Translated words must never reach text, originals must never
// reach translation, and the late chunk rides the next final instead of
// being lost.
func TestSTTTranslationSplitsInterleavedTokens(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		if err := expectBinary(ctx, conn, []byte{1, 2}); err != nil {
			t.Errorf("audio: %v", err)
			return
		}
		frames := []map[string]any{
			// Frame 1: provisional originals only.
			{"tokens": []any{
				map[string]any{"text": "Hel", "is_final": false, "start_ms": 100, "end_ms": 200, "language": "en", "translation_status": "original"},
				map[string]any{"text": "lo", "is_final": false, "start_ms": 200, "end_ms": 300, "language": "en", "translation_status": "original"},
			}},
			// Frame 2: originals finalize, a provisional translation chunk
			// follows with no timestamps.
			{"tokens": []any{
				map[string]any{"text": "Hel", "is_final": true, "confidence": 0.9, "start_ms": 100, "end_ms": 200, "language": "en", "translation_status": "original"},
				map[string]any{"text": "lo", "is_final": true, "confidence": 0.9, "start_ms": 200, "end_ms": 300, "language": "en", "translation_status": "original"},
				map[string]any{"text": " every", "is_final": true, "confidence": 0.9, "start_ms": 350, "end_ms": 600, "language": "en", "translation_status": "original"},
				map[string]any{"text": "one", "is_final": true, "confidence": 0.9, "start_ms": 600, "end_ms": 800, "language": "en", "translation_status": "original"},
				map[string]any{"text": "Ho", "is_final": false, "language": "es", "source_language": "en", "translation_status": "translation"},
			}},
			// Frame 3: part of the translation finalizes, then the endpointer
			// closes the segment before the rest of the translation exists.
			{"tokens": []any{
				map[string]any{"text": "Hola", "is_final": true, "language": "es", "source_language": "en", "translation_status": "translation"},
				map[string]any{"text": "<end>", "is_final": true, "end_ms": 850},
			}},
			// Frame 4: the late translation chunk, then the next segment's
			// original words and its own translation.
			{"tokens": []any{
				map[string]any{"text": " a", "is_final": true, "language": "es", "source_language": "en", "translation_status": "translation"},
				map[string]any{"text": " todos", "is_final": true, "language": "es", "source_language": "en", "translation_status": "translation"},
				map[string]any{"text": " Thanks", "is_final": true, "confidence": 0.8, "start_ms": 1000, "end_ms": 1300, "language": "en", "translation_status": "original"},
				map[string]any{"text": " Gracias", "is_final": true, "language": "es", "source_language": "en", "translation_status": "translation"},
				map[string]any{"text": "<end>", "is_final": true, "end_ms": 1350},
			}},
		}
		for _, frame := range frames {
			if err := writeJSONFrame(ctx, conn, frame); err != nil {
				t.Errorf("write frame: %v", err)
				return
			}
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := sttAdapterRequest(server.URL)
	request.Options.STT = &protocol.SttOptions{Translation: &protocol.SttTranslation{TargetLanguage: "es"}}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)
	if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
		t.Fatalf("write audio: %v", err)
	}

	// frame1 delta, frame2 delta, frame3 final+ended, frame4 final+ended.
	events := collectEvents(t, stream.Events(), 6)
	wantTypes := "transcript.delta,transcript.delta,transcript.final,speech.ended,transcript.final,speech.ended"
	if got := strings.Join(eventTypeNames(events), ","); got != wantTypes {
		t.Fatalf("event types = %s, want %s", got, wantTypes)
	}

	first := decodeTranslated(t, events[0].Data)
	if first.Text != "Hello" || first.Translation != nil {
		t.Fatalf("first delta = text %q translation %s", first.Text, translationValue(first))
	}
	second := decodeTranslated(t, events[1].Data)
	if second.Text != "Hello everyone" || translationValue(second) != "Ho" {
		t.Fatalf("second delta = text %q translation %s", second.Text, translationValue(second))
	}

	firstFinal := decodeTranslated(t, events[2].Data)
	if firstFinal.Text != "Hello everyone" || translationValue(firstFinal) != "Hola" || !firstFinal.IsFinal {
		t.Fatalf("first final = text %q translation %s", firstFinal.Text, translationValue(firstFinal))
	}
	// Timings come from the original words only; a timestamp-less translated
	// token cannot move the span.
	if firstFinal.AudioStartMS == nil || *firstFinal.AudioStartMS != 100 || firstFinal.AudioEndMS == nil || *firstFinal.AudioEndMS != 800 {
		t.Fatalf("first final span = %v..%v", firstFinal.AudioStartMS, firstFinal.AudioEndMS)
	}

	secondFinal := decodeTranslated(t, events[4].Data)
	if secondFinal.Text != "Thanks" || translationValue(secondFinal) != "a todos Gracias" {
		t.Fatalf("second final = text %q translation %s", secondFinal.Text, translationValue(secondFinal))
	}
	for _, event := range events {
		data := decodeTranslated(t, event.Data)
		for _, spanish := range []string{"Hola", "todos", "Gracias"} {
			if strings.Contains(data.Text, spanish) {
				t.Fatalf("translated word %q leaked into text %q", spanish, data.Text)
			}
		}
	}
}

// A translation that finishes after the last original word still reaches the
// caller: stream completion flushes it as a final with empty text.
func TestSTTTranslationTailIsFlushedOnFinish(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		if err := writeJSONFrame(ctx, conn, map[string]any{"tokens": []any{
			map[string]any{"text": "Yes", "is_final": true, "start_ms": 0, "end_ms": 200, "translation_status": "original"},
			map[string]any{"text": "<end>", "is_final": true},
		}}); err != nil {
			t.Errorf("write: %v", err)
			return
		}
		if err := writeJSONFrame(ctx, conn, map[string]any{"tokens": []any{
			map[string]any{"text": "Sí", "is_final": true, "translation_status": "translation"},
		}, "finished": true, "total_audio_proc_ms": 400}); err != nil {
			t.Errorf("write: %v", err)
			return
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := sttAdapterRequest(server.URL)
	request.Options.STT = &protocol.SttOptions{Translation: &protocol.SttTranslation{TargetLanguage: "es"}}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	// final(Yes), speech.ended, delta(translation only), final(translation), usage.
	events := collectEvents(t, stream.Events(), 5)
	wantTypes := "transcript.final,speech.ended,transcript.delta,transcript.final,usage.observed"
	if got := strings.Join(eventTypeNames(events), ","); got != wantTypes {
		t.Fatalf("event types = %s, want %s", got, wantTypes)
	}
	tail := decodeTranslated(t, events[3].Data)
	if tail.Text != "" || translationValue(tail) != "Sí" || !tail.IsFinal {
		t.Fatalf("tail final = text %q translation %s", tail.Text, translationValue(tail))
	}
	// Billing is the provider's processed-audio measure, exactly as on an
	// untranslated session: translation adds no metered quantity.
	var usage struct {
		AudioProcessedMS int64 `json:"audio_processed_ms"`
	}
	if err := json.Unmarshal(events[4].Data, &usage); err != nil || usage.AudioProcessedMS != 400 {
		t.Fatalf("usage = %s (%v)", events[4].Data, err)
	}
}

// An untranslated session that receives no translation_status keeps its exact
// event bytes: no translation key appears.
func TestSTTUntranslatedEventsCarryNoTranslationKey(t *testing.T) {
	t.Parallel()
	payload := sttTranscriptData("hello", "", true, nil, nil, nil, "req")
	if strings.Contains(string(payload), "translation") {
		t.Fatalf("untranslated event mentions translation: %s", payload)
	}
}

// The async API takes the same translation block, and its transcript
// interleaves translated tokens the same way. Text and segments are built
// from the original tokens only.
func TestBatchTranscribeSplitsTranslation(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var creation map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/files":
			_, _ = w.Write([]byte(`{"id":"file_1"}`))
		case "POST /v1/transcriptions":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &creation)
			_, _ = w.Write([]byte(`{"id":"tx_1","status":"queued"}`))
		case "GET /v1/transcriptions/tx_1":
			_, _ = w.Write([]byte(`{"id":"tx_1","status":"completed","audio_duration_ms":2000}`))
		case "GET /v1/transcriptions/tx_1/transcript":
			// text mixes both languages here on purpose: the adapter must not
			// trust it on a translated job.
			_, _ = w.Write([]byte(`{"id":"tx_1","text":"Hello there. Hola.","tokens":[` +
				`{"text":"Hello","start_ms":100,"end_ms":400,"language":"en","translation_status":"original"},` +
				`{"text":" there.","start_ms":450,"end_ms":800,"language":"en","translation_status":"original"},` +
				`{"text":"Hola","language":"es","translation_status":"translation"},` +
				`{"text":" allí.","language":"es","translation_status":"translation"}]}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	audio := "RIFF....WAVE"
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL + "/v1/transcriptions"),
		Options:    protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{Translation: &protocol.SttTranslation{TargetLanguage: "es-MX"}}},
		Audio:      strings.NewReader(audio),
		AudioBytes: int64(len(audio)),
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	translation, _ := json.Marshal(creation["translation"])
	if string(translation) != `{"target_language":"es","type":"one_way"}` {
		t.Fatalf("creation translation = %s", translation)
	}
	if result.Text != "Hello there." || result.Translation != "Hola allí." || result.Language != "en" {
		t.Fatalf("result = text %q translation %q language %q", result.Text, result.Translation, result.Language)
	}
	if len(result.Segments) != 1 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}
