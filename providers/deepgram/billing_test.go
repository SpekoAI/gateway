package deepgram

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// TestStreamingMetadataCarriesBillableDuration pins the one quantity a Listen
// stream reports. The negative control is the same session without a Metadata
// frame: the observation must never appear, because a stream that ended
// without the vendor's own total is unresolved evidence, not free audio.
func TestStreamingMetadataCarriesBillableDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		sendMetadata bool
		language     string
		providerKeys map[string]any
		wantDuration int64
		wantComplete bool
		wantLanguage string
		wantFeatures []string
	}{
		{name: "mono", sendMetadata: true, language: "en-US", wantDuration: 12_500, wantComplete: true},
		{name: "multilingual", sendMetadata: true, language: multilingualLanguage, wantDuration: 12_500, wantComplete: true, wantLanguage: multilingualLanguage},
		{name: "add-on fails closed", sendMetadata: true, language: "en-US", providerKeys: map[string]any{"summarize": "v2", "redact": "pci"}, wantDuration: 12_500, wantComplete: true, wantFeatures: []string{"redact", "summarize"}},
		{name: "no metadata frame", sendMetadata: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := newListenServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				if tc.sendMetadata {
					if err := writeJSON(ctx, conn, map[string]any{
						"type": "Metadata", "request_id": "dg_meta_1", "duration": 12.5, "channels": 1,
					}); err != nil {
						t.Errorf("write metadata: %v", err)
						return
					}
				}
				if err := writeJSON(ctx, conn, map[string]any{
					"type": "Results", "start": 0, "duration": 1, "is_final": true, "speech_final": true,
					"metadata": map[string]any{"request_id": "dg_meta_1"},
					"channel":  map[string]any{"alternatives": []map[string]any{{"transcript": "hello"}}},
				}); err != nil {
					t.Errorf("write result: %v", err)
					return
				}
				_ = conn.Close(websocket.StatusNormalClosure, "")
			})
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			options := protocol.RequestOptions{Language: tc.language}
			if tc.providerKeys != nil {
				options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"deepgram": tc.providerKeys}}
			}
			stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, options))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			var observed *protocol.BillingObservation
			for event := range stream.Events() {
				if event.Billing != nil {
					observed = event.Billing
				}
			}
			if !tc.sendMetadata {
				if observed != nil {
					t.Fatalf("billing observed without a Metadata frame: %+v", observed)
				}
				return
			}
			if observed == nil {
				t.Fatal("no billing observation")
			}
			if err := observed.Validate(); err != nil {
				t.Fatalf("observation invalid: %v", err)
			}
			if observed.Complete != tc.wantComplete || observed.Mode != "streaming" || observed.Model != "nova-3" {
				t.Fatalf("identity = %+v", observed)
			}
			if got := observed.Quantities["duration_seconds"]; got != tc.wantDuration {
				t.Fatalf("duration_seconds = %d, want %d", got, tc.wantDuration)
			}
			if observed.ProviderRequestID != "dg_meta_1" {
				t.Fatalf("provider request id = %q", observed.ProviderRequestID)
			}
			if observed.Language != tc.wantLanguage {
				t.Fatalf("language = %q, want %q", observed.Language, tc.wantLanguage)
			}
			if !slices.Equal(observed.Features, tc.wantFeatures) {
				t.Fatalf("features = %v, want %v", observed.Features, tc.wantFeatures)
			}
		})
	}
}

// TestStreamingMetadataWithoutDurationStaysIncomplete is the negative control
// for the quantity itself: a Metadata frame the vendor sent without a duration
// must produce evidence that cannot be priced rather than a zero-second bill.
func TestStreamingMetadataWithoutDurationStaysIncomplete(t *testing.T) {
	t.Parallel()
	server := newListenServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if err := writeJSON(ctx, conn, map[string]any{"type": "Metadata", "request_id": "dg_meta_2", "channels": 1}); err != nil {
			t.Errorf("write metadata: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.RequestOptions{Language: "en-US"}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var observed *protocol.BillingObservation
	for event := range stream.Events() {
		if event.Billing != nil {
			observed = event.Billing
		}
	}
	if observed == nil {
		t.Fatal("no observation")
	}
	if observed.Complete || len(observed.Quantities) != 0 {
		t.Fatalf("a durationless Metadata frame priced as %+v", observed)
	}
}

// TestFluxStreamReportsNoBillableDuration pins the reason flux-general-en is
// not a priced route: the /v2 turn protocol publishes audio WINDOWS, never a
// processed total, so the adapter must not manufacture one.
func TestFluxStreamReportsNoBillableDuration(t *testing.T) {
	t.Parallel()
	server := newListenServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "TurnInfo", "request_id": "flux_1", "event": "EndOfTurn",
			"audio_window_start": 0.2, "audio_window_end": 9.4, "transcript": "hello",
		}); err != nil {
			t.Errorf("write turn: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{})
	request.Plan.Route.Model = fluxEnglishModel
	endpoint, _ := url.Parse(request.Plan.Route.Endpoint)
	endpoint.Path = "/v2/listen"
	request.Plan.Route.Endpoint = endpoint.String()
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for event := range stream.Events() {
		if event.Billing != nil {
			t.Fatalf("Flux produced a billing observation: %+v", event.Billing)
		}
	}
}

// TestBatchResponseCarriesBillableDuration pins the prerecorded half, and its
// negative control is the default language path: `detect_language` leaves the
// billed tier to the vendor, so it must arrive as a feature no frozen variant
// carries instead of silently billing the mono rate.
func TestBatchResponseCarriesBillableDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		language     string
		wantLanguage string
		wantFeatures []string
	}{
		{name: "explicit mono", language: "en", wantLanguage: ""},
		{name: "explicit multilingual", language: multilingualLanguage, wantLanguage: multilingualLanguage},
		{name: "detected language is unpriced", language: "", wantFeatures: []string{"detect_language"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = w.Write([]byte(`{"metadata":{"request_id":"req-9","duration":61.25},"results":{"channels":[{"detected_language":"en","alternatives":[{"transcript":"hi"}]}]}}`))
			}))
			defer server.Close()

			audio := "RIFF....WAVEfmt data...."
			result, err := newBatchTestAdapter(t, server).Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
				Plan:       batchPlan(server.URL+"/v1/listen", "nova-3"),
				Options:    protocol.RequestOptions{Language: tc.language},
				Media:      protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1},
				Audio:      strings.NewReader(audio),
				AudioBytes: int64(len(audio)),
			})
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			if result.Billing == nil || len(result.Billing.Operations) != 1 {
				t.Fatalf("billing = %+v", result.Billing)
			}
			if err := result.Billing.Validate(); err != nil {
				t.Fatalf("report invalid: %v", err)
			}
			operation := result.Billing.Operations[0]
			if !operation.Complete || operation.Mode != "batch" || operation.Model != "nova-3" {
				t.Fatalf("identity = %+v", operation)
			}
			if got := operation.Quantities["duration_seconds"]; got != 61_250 {
				t.Fatalf("duration_seconds = %d, want 61250", got)
			}
			if operation.ProviderRequestID != "req-9" {
				t.Fatalf("provider request id = %q", operation.ProviderRequestID)
			}
			if operation.Language != tc.wantLanguage {
				t.Fatalf("language = %q, want %q", operation.Language, tc.wantLanguage)
			}
			if !slices.Equal(operation.Features, tc.wantFeatures) {
				t.Fatalf("features = %v, want %v", operation.Features, tc.wantFeatures)
			}
		})
	}
}

func TestBillingDimensionsReadTheExecutedQuery(t *testing.T) {
	t.Parallel()
	// A tier set through the provider-key passthrough still counts, because
	// the query is read after the passthrough has been merged in.
	language, features := billingDimensions(url.Values{"model": {"nova-3"}, "language": {multilingualLanguage}, "encoding": {"linear16"}}, streamingBaseQueryKeys)
	if language != multilingualLanguage || len(features) != 0 {
		t.Fatalf("language=%q features=%v", language, features)
	}
	language, features = billingDimensions(url.Values{"model": {"nova-3"}, "language": {"en"}, "diarize": {"true"}, "keyterm": {"Speko"}}, streamingBaseQueryKeys)
	if language != "" || !slices.Equal(features, []string{"diarize", "keyterm"}) {
		t.Fatalf("language=%q features=%v", language, features)
	}
}
