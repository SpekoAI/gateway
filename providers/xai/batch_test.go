package xai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "xai", Model: "grok-stt", Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "xai-key"},
		},
	}
}

func TestBatchTranscribeUsesRESTContract(t *testing.T) {
	t.Parallel()
	diarize := true
	forms := make(chan map[string][]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer xai-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		_ = r.ParseMultipartForm(1 << 20)
		if _, header, err := r.FormFile("file"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("file: %v %v", header, err)
		}
		forms <- r.MultipartForm.Value
		w.Header().Set("x-request-id", "xreq")
		_, _ = w.Write([]byte(`{"text":"Hello there. Hi.","language":"en","duration":1823.17,"words":[{"text":"Hello","start":0.1,"end":0.4,"speaker":0},{"text":"there.","start":0.45,"end":0.8,"speaker":0},{"text":"Hi.","start":1.0,"end":1.2,"speaker":1}]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/v1/stt"), Options: protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko"}}},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	form := <-forms
	for key, want := range map[string]string{"language": "en", "diarize": "true", "keyterm": "Speko"} {
		if values := form[key]; len(values) != 1 || values[0] != want {
			t.Fatalf("form %s = %v", key, values)
		}
	}
	if values := form["model"]; len(values) != 1 || values[0] != "grok-voice-transcribe-2.0" {
		t.Fatalf("form model = %v, want exactly [grok-voice-transcribe-2.0]", values)
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "xreq" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0].Speaker != "0" || result.Segments[1].Speaker != "1" {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

// TestBatchModelPrecedesTheFilePart pins an ORDERING, which ParseMultipartForm
// cannot see: it hands back a map, and a map has no order. xAI streams the
// upload into whichever recogniser it already selected, so a `model` part that
// arrives after `file` is rejected with "The 'model' field must be sent before
// 'file'" -- and a wrong-order request naming the current DEFAULT succeeds
// anyway, so the defect stays invisible until someone pins the other model.
// The raw body is therefore inspected byte-wise.
//
// What this actually guards is batchhttp.Multipart, not the field slice:
// Multipart writes every field before the file part, so moving "model" within
// the slice changes nothing. Reordering the SHARED helper -- which eight
// providers use and only xAI is sensitive to -- is the change that breaks xAI
// silently, and flipping those two blocks is what makes this test fail.
func TestBatchModelPrecedesTheFilePart(t *testing.T) {
	t.Parallel()
	bodies := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		bodies <- string(raw)
		_, _ = w.Write([]byte(`{"text":"hi","language":"en","duration":1.5,"words":[]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	if _, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/v1/stt"), Options: protocol.RequestOptions{Language: "en"},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	}); err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	body := <-bodies
	model := strings.Index(body, `name="model"`)
	file := strings.Index(body, `name="file"`)
	if model < 0 || file < 0 {
		t.Fatalf("model part at %d, file part at %d; both must be present", model, file)
	}
	if model > file {
		t.Fatalf("model part at byte %d comes AFTER the file part at %d; xAI rejects that ordering", model, file)
	}
}

// The charge is keyed on the id the VENDOR ran. "grok-stt" is a Speko catalog
// label that xAI has never heard of, so an observation carrying it would key a
// frozen billing variant on a model that was never executed.
func TestBatchBillingNamesTheVendorModelNotTheCatalogLabel(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("x-request-id", "xreq-batch")
		_, _ = w.Write([]byte(`{"text":"hi","language":"en","duration":12.5,"words":[]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	// The plan names "grok-stt"; the observation must not.
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/v1/stt"), Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Billing == nil || !result.Billing.Complete || len(result.Billing.Operations) != 1 {
		t.Fatalf("billing report = %+v", result.Billing)
	}
	op := result.Billing.Operations[0]
	if err := op.Validate(); err != nil {
		t.Fatalf("observation does not validate: %v", err)
	}
	if op.Model != "grok-voice-transcribe-2.0" {
		t.Fatalf("observation model = %q, want the executed xAI id", op.Model)
	}
	if op.Mode != "batch" {
		t.Fatalf("observation mode = %q, want batch -- the REST half bills $0.10/hr against the socket's $0.20", op.Mode)
	}
	if op.Quantities["duration_seconds"] != 12_500 {
		t.Fatalf("duration_seconds = %d thousandths, want 12500", op.Quantities["duration_seconds"])
	}
	if op.ProviderRequestID != "xreq-batch" {
		t.Fatalf("provider request id = %q", op.ProviderRequestID)
	}
}

func TestBatchTranscribeSizeCap(t *testing.T) {
	t.Parallel()
	adapter, _ := NewBatch(BatchConfig{})
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpoint), Audio: strings.NewReader("x"), AudioBytes: BatchMaxAudioBytes + 1})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}
