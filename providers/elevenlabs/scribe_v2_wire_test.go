package elevenlabs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// The v2 models share their adapters with newer Scribe generations. These two
// tests pin what a v2 request puts on the wire, byte for byte, so a change
// made for another model cannot move it.
func TestScribeV2RealtimeQueryIsPinned(t *testing.T) {
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
	request := sttRequest(server.URL, protocol.CredentialsBYOK)
	request.Options.STT = &protocol.SttOptions{
		Keywords:        []string{"Speko", "São Paulo"},
		ProviderOptions: map[string]map[string]any{"elevenlabs": {"vad_silence_threshold_secs": 0.7}},
	}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())

	const want = "audio_format=pcm_16000&commit_strategy=vad&include_timestamps=true&keyterms=Speko&keyterms=S%C3%A3o+Paulo&language_code=es&model_id=scribe_v2_realtime&vad_silence_threshold_secs=0.7"
	if got := <-queries; got != want {
		t.Fatalf("scribe_v2_realtime query\n got %s\nwant %s", got, want)
	}
}

func TestScribeV2BatchFormIsPinned(t *testing.T) {
	t.Parallel()
	diarize := true
	forms := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forms <- orderedForm(t, r)
		_, _ = w.Write([]byte(`{"language_code":"en","text":"Hi.","audio_duration_secs":1,"words":[]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/v1/speech-to-text"),
		Options: protocol.RequestOptions{Language: "en-GB", STT: &protocol.SttOptions{
			Diarization:     &diarize,
			Keywords:        []string{"Speko", "Router"},
			ProviderOptions: map[string]map[string]any{"elevenlabs": {"num_speakers": 2, "no_verbatim": true}},
		}},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	const want = "model_id=scribe_v2|diarize=true|timestamps_granularity=word|tag_audio_events=false|language_code=en|keyterms=Speko|keyterms=Router|no_verbatim=true|num_speakers=2|file=RIFF....WAVE"
	if got := <-forms; got != want {
		t.Fatalf("scribe_v2 form\n got %s\nwant %s", got, want)
	}
}

// orderedForm renders a multipart body as name=value parts in wire order.
func orderedForm(t *testing.T, r *http.Request) string {
	t.Helper()
	reader, err := r.MultipartReader()
	if err != nil {
		t.Errorf("multipart: %v", err)
		return ""
	}
	var parts []string
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("multipart part: %v", err)
			return ""
		}
		value, _ := io.ReadAll(part)
		parts = append(parts, part.FormName()+"="+string(value))
	}
	return strings.Join(parts, "|")
}
