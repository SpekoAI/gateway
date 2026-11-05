package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/relayapi"
	"github.com/coder/websocket"
)

const nativeTestSession = "89c0d80a-9edf-4414-9b25-326fd0aa768f"
const nativeTestLocal = "owned-local-authorization"

func nativeHeaders(key string) http.Header {
	headers := http.Header{}
	headers.Set(gateway.NativeLocalAuthorization, "Bearer "+nativeTestLocal)
	headers.Set("Authorization", "Bearer "+key)
	headers.Set("Speko-Client-Session-ID", nativeTestSession)
	headers.Set("Idempotency-Key", "owned-native-request")
	return headers
}

func nativeUpgradeHeaders(key string) http.Header {
	headers := nativeHeaders(key)
	headers.Set("Connection", "Upgrade")
	headers.Set("Upgrade", "websocket")
	headers.Set("Sec-WebSocket-Version", "13")
	headers.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==")
	return headers
}

func TestNativeRelayMalformedWebSocketCannotStartRealtime(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer peer.Close()
	_, local := nativeServer(t, peer, nil)
	for _, row := range []struct {
		name, method string
		change       func(http.Header)
	}{
		{"head", http.MethodHead, func(h http.Header) {}},
		{"connection", http.MethodGet, func(h http.Header) { h.Del("Connection") }},
		{"upgrade", http.MethodGet, func(h http.Header) { h.Set("Upgrade", "http") }},
		{"version", http.MethodGet, func(h http.Header) { h.Set("Sec-WebSocket-Version", "12") }},
		{"missing key", http.MethodGet, func(h http.Header) { h.Del("Sec-WebSocket-Key") }},
		{"invalid key", http.MethodGet, func(h http.Header) { h.Set("Sec-WebSocket-Key", "not-base64") }},
		{"duplicate key", http.MethodGet, func(h http.Header) { h.Add("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==") }},
		{"foreign origin", http.MethodGet, func(h http.Header) { h.Set("Origin", "https://other.invalid") }},
	} {
		t.Run(row.name, func(t *testing.T) {
			request, _ := http.NewRequest(row.method, local.URL+"/v1/xai/realtime?model=grok-voice-3.0", nil)
			request.Header = nativeUpgradeHeaders("sk_speko_agent_owned")
			row.change(request.Header)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode < 400 || response.StatusCode >= 500 {
				t.Fatalf("invalid local WebSocket request reached upstream: %d", response.StatusCode)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid local handshakes started %d upstream requests", calls.Load())
	}
}
func nativeServer(t *testing.T, peer *httptest.Server, change func(*gateway.NativeRelayConfig)) (*gateway.NativeRelayServer, *httptest.Server) {
	t.Helper()
	cfg := gateway.NativeRelayConfig{RelayOrigin: peer.URL, LocalAuthToken: nativeTestLocal, HTTPClient: peer.Client(), WriteTimeout: time.Second}
	if change != nil {
		change(&cfg)
	}
	server, err := gateway.NewNativeRelay(cfg)
	if err != nil {
		t.Fatal(err)
	}
	local := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Drain(ctx)
		local.Close()
	})
	return server, local
}
func nativeAwait(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-deadline.C:
			t.Fatal("native operation did not release resources")
		case <-ticker.C:
		}
	}
}

func TestNativeRelayRefusesInvalidLocalOrModelAuthorityBeforeDial(t *testing.T) {
	var called atomic.Int32
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1); w.WriteHeader(500) }))
	defer peer.Close()
	_, local := nativeServer(t, peer, nil)
	cases := []struct {
		name   string
		change func(http.Header)
		status int
	}{
		{"missing local", func(h http.Header) { h.Del(gateway.NativeLocalAuthorization) }, 401},
		{"local scheme", func(h http.Header) { h.Set(gateway.NativeLocalAuthorization, nativeTestLocal) }, 401},
		{"duplicate local", func(h http.Header) { h.Add(gateway.NativeLocalAuthorization, "Bearer "+nativeTestLocal) }, 401},
		{"missing model", func(h http.Header) { h.Del("Authorization") }, 401},
		{"retired key", func(h http.Header) { h.Set("Authorization", "Bearer sk_live_retired") }, 401},
		{"control token", func(h http.Header) { h.Set("Authorization", "Bearer swt1.not-a-model-key") }, 401},
		{"duplicate model", func(h http.Header) { h.Add("Authorization", "Bearer sk_speko_second") }, 401},
		{"missing session", func(h http.Header) { h.Del("Speko-Client-Session-ID") }, 400},
		{"invalid session", func(h http.Header) { h.Set("Speko-Client-Session-ID", "another/session") }, 400},
		{"session whitespace", func(h http.Header) { h.Set("Speko-Client-Session-ID", "two sessions") }, 400},
		{"session leading punctuation", func(h http.Header) { h.Set("Speko-Client-Session-ID", "_session") }, 400},
		{"session oversized", func(h http.Header) { h.Set("Speko-Client-Session-ID", strings.Repeat("a", 201)) }, 400},
		{"session unicode", func(h http.Header) { h.Set("Speko-Client-Session-ID", "session-é") }, 400},
		{"duplicate session", func(h http.Header) { h.Add("Speko-Client-Session-ID", nativeTestSession) }, 400},
		{"missing idempotency", func(h http.Header) { h.Del("Idempotency-Key") }, 400},
	}
	for _, row := range cases {
		t.Run(row.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", local.URL+"/v1/stt/stream", nil)
			req.Header = nativeHeaders("sk_speko_agent_owned")
			row.change(req.Header)
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != row.status {
				t.Fatalf("status %d, want %d", response.StatusCode, row.status)
			}
		})
	}
	req, _ := http.NewRequest("GET", local.URL+"/v1/stt/stream?relay_origin=https://untrusted.invalid", nil)
	req.Header = nativeHeaders("sk_speko_agent_owned")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatalf("URL params: %d", response.StatusCode)
	}
	if called.Load() != 0 {
		t.Fatalf("unauthorized upstream calls: %d", called.Load())
	}
}

func TestNativeRelayPreservesContractSessionIdentity(t *testing.T) {
	for _, sessionID := range []string{"s", nativeTestSession, "ses_fixture_web", "s" + strings.Repeat("._:-A9", 33) + "z"} {
		t.Run(sessionID, func(t *testing.T) {
			var calls atomic.Int32
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Speko-Client-Session-ID") != sessionID {
					t.Error("canonical session identity changed")
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer peer.Close()
			_, local := nativeServer(t, peer, nil)
			req, _ := http.NewRequest(http.MethodPost, local.URL+"/v1/llm/responses", strings.NewReader(`{"input":[]}`))
			req.Header = nativeHeaders("sk_speko_agent_owned")
			req.Header.Set("Speko-Client-Session-ID", sessionID)
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusNoContent || calls.Load() != 1 {
				t.Fatalf("session refused: status %d, relay calls %d", response.StatusCode, calls.Load())
			}
		})
	}
}

func TestNativeRelayRealtimeRoutesPreserveNativeWireAndModelBinding(t *testing.T) {
	for _, target := range []string{
		"/v1/openai/realtime?model=gpt-realtime-2.1",
		"/v1/xai/realtime?model=grok-voice-3.0",
		"/v1/openai/gpt-live",
		"/v1/google/gemini-live",
		"/v1/openai/realtime-translations?model=gpt-realtime-translate",
		"/v1/alibaba/qwen-livetranslate?model=qwen3.8-livetranslate-flash-realtime",
	} {
		t.Run(target, func(t *testing.T) {
			// The transport must not decode/re-encode vendor frames, including
			// integers beyond JavaScript's exact range and opaque audio payloads.
			input := []byte(`{"type":"owned.native","value":9007199254740993,"audio":"/w8ADw=="}`)
			output := []byte(`{"serverContent":{"owned":9007199254740993,"audio":"AAH/"}}`)
			peerResult := make(chan error, 1)
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.RequestURI() != target || r.Header.Get("Authorization") != "Bearer sk_speko_agent_owned" || r.Header.Get("Speko-Client-Session-ID") != "ses_fixture_web" || r.Header.Get("Idempotency-Key") != "owned-native-request" || r.Header.Get(gateway.NativeLocalAuthorization) != "" {
					peerResult <- errors.New("realtime target or scoped headers changed")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					peerResult <- err
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				defer cancel()
				kind, raw, err := conn.Read(ctx)
				if err == nil && (kind != websocket.MessageText || !bytes.Equal(raw, input)) {
					err = errors.New("native client bytes changed")
				}
				if err == nil {
					err = conn.Write(ctx, websocket.MessageText, output)
				}
				peerResult <- err
				// Keep the provider side alive until the caller closes: dropping
				// it here would race the proxy's final write to the local socket.
				_, _, _ = conn.Read(ctx)
			}))
			defer peer.Close()
			server, local := nativeServer(t, peer, nil)
			headers := nativeHeaders("sk_speko_agent_owned")
			headers.Set("Speko-Client-Session-ID", "ses_fixture_web")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(local.URL, "http")+target, &websocket.DialOptions{HTTPHeader: headers})
			if err != nil {
				status := 0
				if response != nil {
					status = response.StatusCode
				}
				t.Fatalf("native realtime upgrade: status=%d error=%v", status, err)
			}
			defer conn.CloseNow()
			if err := conn.Write(ctx, websocket.MessageText, input); err != nil {
				t.Fatal(err)
			}
			kind, raw, err := conn.Read(ctx)
			if err != nil || kind != websocket.MessageText || !bytes.Equal(raw, output) {
				t.Fatalf("native provider bytes changed: kind=%v error=%v", kind, err)
			}
			if err := <-peerResult; err != nil {
				t.Fatal(err)
			}
			conn.CloseNow()
			nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
		})
	}
}

func TestNativeRelayRealtimeRefusesUnboundQueryBeforeUpstream(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer peer.Close()
	_, local := nativeServer(t, peer, nil)
	for _, target := range []string{
		"/v1/openai/realtime", "/v1/openai/realtime?", "/v1/openai/realtime?model=",
		"/v1/openai/realtime?model=one&model=two",
		"/v1/openai/realtime?model=one&api_key=untrusted",
		"/v1/openai/realtime?model=one%20two", "/v1/openai/realtime?model=%ff",
		"/v1/openai/realtime?model=" + strings.Repeat("a", 129),
		"/v1/openai/gpt-live?model=unbound", "/v1/google/gemini-live?model=unbound",
	} {
		request, _ := http.NewRequest(http.MethodGet, local.URL+target, nil)
		request.Header = nativeHeaders("sk_speko_agent_owned")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("unbound query %q status=%d", target, response.StatusCode)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unbound realtime query reached upstream")
	}
}

func TestNativeRelaySpeechPreservesFramesCredentialsAndTerminalUsage(t *testing.T) {
	for _, kind := range []string{"stt", "tts"} {
		t.Run(kind, func(t *testing.T) {
			configured := []byte(`{"type":"session.configure","routing":{"mode":"explicit","provider":"fixture","model":"fixture-model"},"audio":{"encoding":"pcm_s16le","sample_rate_hz":16000,"channels":1},"options":{"number":9007199254740993,"decimal":0.70000000000000001}}`)
			ready := []byte(`{"type":"session.ready","request_id":"owned","route":{"provider":"fixture","model":"fixture-model"}}`)
			terminal := []byte(`{"type":"session.closed","usage":{"duration_ms":1000}}`)
			pcm := bytes.Repeat([]byte{0xff, 0x0f}, 16000)
			observed := make(chan http.Header, 1)
			peerErrors := make(chan error, 1)
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				observed <- r.Header.Clone()
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					peerErrors <- err
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				_, raw, err := conn.Read(ctx)
				if err != nil {
					peerErrors <- err
					return
				}
				if !bytes.Equal(raw, configured) {
					peerErrors <- errors.New("configure bytes changed")
					return
				}
				if err = conn.Write(ctx, websocket.MessageText, ready); err != nil {
					peerErrors <- err
					return
				}
				if kind == "stt" {
					typ, body, err := conn.Read(ctx)
					if err != nil {
						peerErrors <- err
						return
					}
					if typ != websocket.MessageBinary || !bytes.Equal(body, pcm) {
						peerErrors <- errors.New("input PCM changed")
						return
					}
				} else {
					if err = conn.Write(ctx, websocket.MessageBinary, pcm); err != nil {
						peerErrors <- err
						return
					}
				}
				if err = conn.Write(ctx, websocket.MessageText, terminal); err != nil {
					peerErrors <- err
					return
				}
				peerErrors <- nil
				_ = conn.Close(websocket.StatusNormalClosure, "")
			}))
			defer peer.Close()
			server, local := nativeServer(t, peer, func(cfg *gateway.NativeRelayConfig) {
				jar, _ := cookiejar.New(nil)
				origin, _ := url.Parse(peer.URL)
				jar.SetCookies(origin, []*http.Cookie{{Name: "cached-local", Value: "private"}})
				cfg.HTTPClient.Jar = jar
				cfg.HTTPClient.Timeout = time.Nanosecond
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			headers := nativeHeaders("sk_speko_agent_" + kind)
			headers.Set("Cookie", "must-stay-local=private")
			headers.Set("X-Untrusted-Forwarding", "private")
			conn, _, err := websocket.Dial(ctx, local.URL+"/v1/"+kind+"/stream", &websocket.DialOptions{HTTPHeader: headers})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			if err = conn.Write(ctx, websocket.MessageText, configured); err != nil {
				t.Fatal(err)
			}
			typ, body, err := conn.Read(ctx)
			if err != nil || typ != websocket.MessageText || !bytes.Equal(body, ready) {
				t.Fatalf("ready frame: %v", err)
			}
			if kind == "stt" {
				if err = conn.Write(ctx, websocket.MessageBinary, pcm); err != nil {
					t.Fatal(err)
				}
			} else {
				typ, body, err = conn.Read(ctx)
				if err != nil || typ != websocket.MessageBinary || !bytes.Equal(body, pcm) {
					t.Fatalf("output PCM: %v", err)
				}
			}
			typ, body, err = conn.Read(ctx)
			if err != nil || typ != websocket.MessageText || !bytes.Equal(body, terminal) {
				t.Fatalf("terminal frame: %v", err)
			}
			_, _, err = conn.Read(ctx)
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				t.Fatalf("normal terminal close lost: %v", err)
			}
			if err = <-peerErrors; err != nil {
				t.Fatal(err)
			}
			upstream := <-observed
			if upstream.Get("Authorization") != "Bearer sk_speko_agent_"+kind || upstream.Get("Speko-Client-Session-ID") != nativeTestSession || upstream.Get("Idempotency-Key") != "owned-native-request" {
				t.Fatal("session credential binding changed")
			}
			if upstream.Get(gateway.NativeLocalAuthorization) != "" || upstream.Get("Cookie") != "" || upstream.Get("X-Untrusted-Forwarding") != "" {
				t.Fatal("local credentials or untrusted headers crossed the relay boundary")
			}
			nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
		})
	}
}

func TestNativeRelayCapacityAndDrainKeepLiveSessionsUntilClose(t *testing.T) {
	observed := make(chan string, 4)
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Header.Get("Authorization")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			kind, body, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if conn.Write(r.Context(), kind, body) != nil {
				return
			}
		}
	}))
	defer peer.Close()
	server, local := nativeServer(t, peer, func(c *gateway.NativeRelayConfig) { c.MaxSessions = 2 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connections := make([]*websocket.Conn, 0, 2)
	for _, key := range []string{"sk_speko_agent_first", "sk_speko_agent_second"} {
		conn, _, err := websocket.Dial(ctx, local.URL+"/v1/stt/stream", &websocket.DialOptions{HTTPHeader: nativeHeaders(key)})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.CloseNow()
		connections = append(connections, conn)
	}
	for range 2 {
		select {
		case <-observed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	_, response, err := websocket.Dial(ctx, local.URL+"/v1/stt/stream", &websocket.DialOptions{HTTPHeader: nativeHeaders("sk_speko_agent_third")})
	if err == nil || response == nil || response.StatusCode != 429 {
		t.Fatal("capacity did not refuse before dial")
	}
	server.BeginDrain()
	_, response, err = websocket.Dial(ctx, local.URL+"/v1/stt/stream", &websocket.DialOptions{HTTPHeader: nativeHeaders("sk_speko_agent_fourth")})
	if err == nil || response == nil || response.StatusCode != 503 {
		t.Fatal("drain did not refuse new work")
	}
	drained := make(chan error, 1)
	go func() { drained <- server.Drain(ctx) }()
	for _, conn := range connections {
		if err = conn.Write(ctx, websocket.MessageBinary, []byte{1, 2}); err != nil {
			t.Fatal(err)
		}
		_, raw, err := conn.Read(ctx)
		if err != nil || !bytes.Equal(raw, []byte{1, 2}) {
			t.Fatalf("drain interrupted active media: %v", err)
		}
	}
	select {
	case <-drained:
		t.Fatal("drain completed with active sockets")
	default:
	}
	for _, conn := range connections {
		_ = conn.CloseNow()
	}
	if err = <-drained; err != nil {
		t.Fatal(err)
	}
	if state := server.Stats(); state.ActiveSessions != 0 || !state.Draining || state.SessionsTotal != 2 {
		t.Fatalf("unexpected native state: %+v", state)
	}
	select {
	case <-observed:
		t.Fatal("refused work dialed the relay")
	default:
	}
}

func TestNativeRelayHTTPStreamsExactBodyAndSSEBeforeCompletion(t *testing.T) {
	release := make(chan struct{})
	input := []byte(`{"routing":{"mode":"explicit","provider":"fixture","model":"fixture"},"tools":[{"parameters":{"maximum":9007199254740993}}],"max_output_tokens":64}`)
	first := []byte("event: response.output_text.delta\ndata: {\"delta\":\"synthetic\"}\n\n")
	final := []byte("event: response.completed\ndata: {\"status\":\"completed\"}\n\n")
	observed := make(chan []byte, 1)
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		observed <- raw
		if r.Header.Get(gateway.NativeLocalAuthorization) != "" || r.Header.Get("Cookie") != "" {
			t.Error("private local headers forwarded")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(first)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(final)
	}))
	defer peer.Close()
	server, local := nativeServer(t, peer, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", local.URL+"/v1/llm/responses", bytes.NewReader(input))
	req.Header = nativeHeaders("sk_speko_agent_llm")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", "private=value")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("HTTP status: %d", response.StatusCode)
	}
	raw := make([]byte, len(first))
	if _, err = io.ReadFull(response.Body, raw); err != nil || !bytes.Equal(raw, first) {
		t.Fatalf("initial SSE frame was buffered: %v", err)
	}
	if !bytes.Equal(<-observed, input) {
		t.Fatal("native JSON request was normalized")
	}
	close(release)
	rest, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(rest, final) {
		t.Fatalf("terminal SSE changed: %v", err)
	}
	nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
}

func TestNativeRelayRejectsRedirectsAndOversizeBodies(t *testing.T) {
	var foreignCalls, relayCalls atomic.Int32
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignCalls.Add(1) }))
	defer foreign.Close()
	relay := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		relayCalls.Add(1)
		http.Redirect(w, r, foreign.URL+"/v1/llm/responses", 307)
	}))
	defer relay.Close()
	server, local := nativeServer(t, relay, func(c *gateway.NativeRelayConfig) { c.MaxRequestBytes = 8 })
	for _, size := range []int{8, 9} {
		req, _ := http.NewRequest("POST", local.URL+"/v1/llm/responses", strings.NewReader(strings.Repeat("x", size)))
		req.Header = nativeHeaders("sk_speko_agent_owned")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		wanted := 502
		if size == 9 {
			wanted = 413
		}
		if response.StatusCode != wanted {
			t.Fatalf("size %d: HTTP %d", size, response.StatusCode)
		}
		if response.Header.Get("Location") != "" {
			t.Fatal("relay redirect exposed")
		}
	}
	if foreignCalls.Load() != 0 || relayCalls.Load() != 1 {
		t.Fatalf("unexpected redirect calls: relay=%d foreign=%d", relayCalls.Load(), foreignCalls.Load())
	}
	nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
}

func TestNativeRelayChunkedUploadLimitRemains413ThroughRealTransport(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength != -1 || len(r.TransferEncoding) != 1 || r.TransferEncoding[0] != "chunked" {
			t.Error("expected an unknown-length chunked upstream upload")
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		_, _ = w.Write([]byte("owned"))
	}))
	defer peer.Close()
	server, local := nativeServer(t, peer, func(c *gateway.NativeRelayConfig) {
		c.MaxRequestBytes = 8192
		c.MaxSessions = 1
	})
	for index := 0; index < 20; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		// NopCloser hides the reader's known length, forcing real chunked HTTP.
		body := io.NopCloser(strings.NewReader(strings.Repeat("x", 8193)))
		req, _ := http.NewRequestWithContext(ctx, "POST", local.URL+"/v1/stt/transcriptions", body)
		req.Header = nativeHeaders("sk_speko_agent_owned")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		raw, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		cancel()
		if err != nil || response.StatusCode != 413 || !bytes.Contains(raw, []byte(`"code":"payload_too_large"`)) {
			t.Fatalf("chunked overflow %d: status=%d body=%q error=%v", index, response.StatusCode, raw, err)
		}
		nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
	}
}

func TestNativeRelayDrainDeadlineCancelsLiveNativeSocket(t *testing.T) {
	closed := make(chan struct{})
	relay := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, _, _ = conn.Read(r.Context())
		close(closed)
	}))
	defer relay.Close()
	server, local := nativeServer(t, relay, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, local.URL+"/v1/stt/stream", &websocket.DialOptions{HTTPHeader: nativeHeaders("sk_speko_agent_owned")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err = server.Drain(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain result: %v", err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("expired drain left upstream alive")
	}
	nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
}

func TestNativeRelayFrameBoundDropsOversizeMessagesInEitherDirection(t *testing.T) {
	for _, fromRelay := range []bool{false, true} {
		name := "caller"
		if fromRelay {
			name = "relay"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			oversize := bytes.Repeat([]byte{1}, (2<<20)+1)
			peerReceived := make(chan bool, 1)
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				conn.SetReadLimit(8 << 20)
				if fromRelay {
					_ = conn.Write(ctx, websocket.MessageBinary, oversize)
				}
				_, _, err = conn.Read(ctx)
				peerReceived <- err == nil
			}))
			defer peer.Close()
			server, local := nativeServer(t, peer, nil)
			conn, _, err := websocket.Dial(ctx, local.URL+"/v1/stt/stream", &websocket.DialOptions{HTTPHeader: nativeHeaders("sk_speko_agent_owned")})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			conn.SetReadLimit(8 << 20)
			if !fromRelay {
				_ = conn.Write(ctx, websocket.MessageBinary, oversize)
			}
			if _, _, err = conn.Read(ctx); err == nil {
				t.Fatal("oversize message reached the caller")
			}
			select {
			case received := <-peerReceived:
				if received {
					t.Fatal("oversize message reached the relay")
				}
			case <-ctx.Done():
				t.Fatal("oversize message did not close the relay")
			}
			nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
		})
	}
}

func TestNativeRelayDrainDeadlineCancelsAnOpenHTTPResponse(t *testing.T) {
	closed := make(chan struct{})
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("open\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer peer.Close()
	server, local := nativeServer(t, peer, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", local.URL+"/v1/llm/responses", strings.NewReader("{}"))
	req.Header = nativeHeaders("sk_speko_agent_owned")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	first := make([]byte, 5)
	if _, err = io.ReadFull(response.Body, first); err != nil || string(first) != "open\n" {
		t.Fatalf("initial SSE: %v", err)
	}
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err = server.Drain(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain: %v", err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("HTTP relay remained live after drain expiry")
	}
	nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
}

func TestNativeRelaySetupTimeoutDoesNotLimitHTTPModelExecution(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("owned-model-result"))
	}))
	defer peer.Close()
	_, local := nativeServer(t, peer, func(c *gateway.NativeRelayConfig) { c.SetupTimeout = 50 * time.Millisecond })
	for _, path := range []string{"/v1/llm/responses", "/v1/stt/transcriptions", "/v1/tts/speech"} {
		req, _ := http.NewRequest("POST", local.URL+path, strings.NewReader("{}"))
		req.Header = nativeHeaders("sk_speko_agent_owned")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != 200 || string(raw) != "owned-model-result" {
			t.Fatalf("%s: status=%d result=%q error=%v", path, response.StatusCode, raw, err)
		}
	}
}

func TestNativeRelayStalledUploadReleasesSlotAndCancelsUpstream(t *testing.T) {
	closed := make(chan struct{})
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			close(closed)
			return
		}
		_, _ = w.Write([]byte("owned"))
	}))
	defer peer.Close()
	server, local := nativeServer(t, peer, func(c *gateway.NativeRelayConfig) { c.SetupTimeout = 100 * time.Millisecond; c.MaxSessions = 1 })
	endpoint, _ := url.Parse(local.URL)
	conn, err := net.Dial("tcp", endpoint.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "POST /v1/llm/responses HTTP/1.1\r\nHost: owned\r\nAuthorization: Bearer sk_speko_agent_owned\r\nSpeko-Local-Authorization: Bearer %s\r\nSpeko-Client-Session-ID: %s\r\nIdempotency-Key: owned-stalled\r\nContent-Length: 1000\r\n\r\n{", nativeTestLocal, nativeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	nativeAwait(t, func() bool { return server.Stats().SessionsTotal == 1 })
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("stalled upload did not cancel the upstream body")
	}
	nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
	req, _ := http.NewRequest("POST", local.URL+"/v1/llm/responses", nil)
	req.Header = nativeHeaders("sk_speko_agent_owned")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("stalled upload retained the only slot")
	}
}

func TestNativeRelayWebSocketRefusalPreservesStatusEnvelopeAndRequestIdentity(t *testing.T) {
	for _, message := range []string{strings.Repeat("<", 2000), "x" + strings.Repeat("\u2028", 2000)} {
		for _, status := range []int{402, 409, 429, 503} {
			t.Run(fmt.Sprintf("%d/%x", status, []byte(message)[:3]), func(t *testing.T) {
				envelope := relayapi.ErrorEnvelope{Error: relayapi.ErrorBody{Code: relayapi.ErrorCodeInsufficientCredit, Message: message, Hint: strings.Repeat("h", 512), RequestID: "req-123", Retryable: false}}
				peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					var encoded bytes.Buffer
					encoder := json.NewEncoder(&encoded)
					encoder.SetEscapeHTML(false)
					_ = encoder.Encode(envelope)
					raw := bytes.ReplaceAll(encoded.Bytes(), []byte("\\u2028"), []byte("\u2028"))
					if len(raw) > 8192 {
						t.Fatal("fixture is outside the canonical wire bound")
					}
					_, _ = w.Write(raw)
				}))
				defer peer.Close()
				_, local := nativeServer(t, peer, nil)
				req, _ := http.NewRequest("GET", local.URL+"/v1/stt/stream", nil)
				req.Header = nativeUpgradeHeaders("sk_speko_agent_owned")
				response, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				var actual relayapi.ErrorEnvelope
				if json.NewDecoder(response.Body).Decode(&actual) != nil || actual != envelope || response.StatusCode != status || response.Header.Get(relayapi.HeaderRequestID) != envelope.Error.RequestID {
					t.Fatal("WebSocket rejection lost its normalized identity")
				}
			})
		}
	}
}
