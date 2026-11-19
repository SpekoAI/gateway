package cartesia_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/cartesia"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func preparedRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: "cartesia", Model: "sonic-3.6", Adapter: cartesia.BytesTTSAdapterID, Transport: protocol.TransportHTTP,
				Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "synthetic-funded-credential"}}},
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24000, Channels: 1}, Options: protocol.RequestOptions{Voice: "voice-id", Language: "en"}}
}

func TestPreparedInputSharedConsumerCannotReplaceTheBoundSnapshot(t *testing.T) {
	t.Parallel()
	actual := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text string `json:"transcript"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid provider body")
		}
		actual <- body.Text
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2, 3, 4})
	}))
	defer server.Close()
	adapter, err := cartesia.NewBytesTTS(cartesia.BytesTTSConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	request := preparedRequest(server.URL + "/tts/bytes")
	prepared, err := adapter.PrepareInput(request, "original 漢🙂\x00e\u0301")
	if err != nil {
		t.Fatal(err)
	}
	sharedConsumer := prepared
	boundText := prepared.Text()
	replacement, err := adapter.PrepareInput(request, "different transcript")
	if err != nil {
		t.Fatal(err)
	}
	// Exercise whole-pointee assignment from a different package if the API
	// exposes one: the original exported pointer allowed *shared = *replacement
	// despite private fields. Reflection keeps this regression valid for the
	// opaque value API as well, without unsafe access to private fields.
	target, source := reflect.ValueOf(sharedConsumer), reflect.ValueOf(replacement)
	if target.Kind() == reflect.Pointer && source.Kind() == reflect.Pointer && target.Type() == source.Type() && target.Elem().CanSet() {
		target.Elem().Set(source.Elem())
	}
	stream, err := adapter.OpenPrepared(context.Background(), request, prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-stream.Events():
			if event.Err != nil {
				t.Fatal("prepared synthesis failed")
			}
			if event.Type == protocol.EventAudioDone {
				if <-actual != boundText || prepared.Text() != boundText || sharedConsumer.Text() != boundText {
					t.Fatal("a shared consumer replaced the bound provider transcript")
				}
				if err := stream.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				return
			}
		case <-deadline.C:
			t.Fatal("prepared synthesis did not finish")
		}
	}
}

type wrappedPreparedInput struct{ cartesia.PreparedBytesTTSInput }

func (wrappedPreparedInput) Text() string { return "forged binding input" }

func TestPreparedInputCannotBeReplacedByAnExternalInterfaceWrapper(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	adapter, err := cartesia.NewBytesTTS(cartesia.BytesTTSConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	request := preparedRequest(server.URL + "/tts/bytes")
	prepared, err := adapter.PrepareInput(request, "original")
	if err != nil {
		t.Fatal(err)
	}
	var missing *wrappedPreparedInput
	for _, replacement := range []cartesia.PreparedBytesTTSInput{wrappedPreparedInput{prepared}, missing} {
		stream, err := adapter.OpenPrepared(context.Background(), request, replacement)
		if err == nil || stream != nil || calls.Load() != 0 {
			t.Fatal("a substituted interface implementation reached provider I/O")
		}
	}
}
