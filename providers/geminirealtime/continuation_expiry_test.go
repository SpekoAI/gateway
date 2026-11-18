package geminirealtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

func TestLatePhysicalCallbackCannotDetachNewerOwnedSocket(t *testing.T) {
	peer := &fakeGemini{t: t}
	peer.server = httptest.NewTLSServer(http.HandlerFunc(peer.handle))
	t.Cleanup(peer.server.Close)
	adapter, err := New(Config{HTTPClient: peer.server.Client(), AllowedEndpointHosts: []string{peer.host()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	request := geminiRequest("wss" + strings.TrimPrefix(peer.server.URL, "https") + socketPath)
	request.Plan.Execution.CredentialSource = protocol.CredentialsBYOK
	request.Continuation = true
	request.PhysicalDeadline = time.Now().Add(time.Minute)
	opened, err := adapter.Open(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	stable := opened.(*resumableStream)
	t.Cleanup(func() { _ = stable.Abort(context.Background()) })
	old, done := stable.current, stable.bridgeDone
	stable.Detach()
	_ = old.abort()
	<-done
	request.Plan.Route.Credential.Value = "owned-replacement"
	replacement, err := adapter.openSocket(t.Context(), request, "owned-handle", old.responding, stable.expirePhysical)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.abort() })
	stable.mu.Lock()
	stable.current, stable.detached, stable.bridgeDone = replacement, nil, make(chan struct{})
	go stable.bridge(replacement, stable.bridgeDone)
	stable.mu.Unlock()
	// Exercise an old callback already queued before the new socket's adoption.
	// Both sockets and the new socket's subsequent response are actual TLS WS.
	stable.expirePhysical(old)
	if err := stable.WriteAudio(t.Context(), []byte{1, 0}); err != nil {
		t.Fatalf("old callback detached newer socket: %v", err)
	}
	if err := stable.CommitAudio(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case event, ok := <-stable.Events():
		if !ok || event.Err != nil || event.Type != protocol.EventTranscriptFinal {
			t.Fatalf("newer owned socket lost its stable response: %+v/%v", event, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("newer owned socket failed to return actual response")
	}
}
