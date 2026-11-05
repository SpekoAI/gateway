package gateway_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/coder/websocket"
)

func TestNativeRelayBodyBearingUpgradeCannotStartRealtime(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, _, _ = conn.Read(r.Context())
	}))
	defer peer.Close()
	server, local := nativeServer(t, peer, func(cfg *gateway.NativeRelayConfig) {
		cfg.SetupTimeout = 50 * time.Millisecond
		cfg.MaxSessions = 1
	})
	origin, err := url.Parse(local.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, framing, body string
	}{
		{"fixed-incomplete", "Content-Length: 1\r\n", ""},
		{"fixed-complete", "Content-Length: 1\r\n", "x"},
		{"chunked-incomplete", "Transfer-Encoding: chunked\r\n", ""},
		{"chunked-complete", "Transfer-Encoding: chunked\r\n", "1\r\nx\r\n0\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", origin.Host, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			writer := bufio.NewWriter(conn)
			_, _ = fmt.Fprintf(writer, "GET /v1/xai/realtime?model=grok-voice-3.0 HTTP/1.1\r\nHost: %s\r\n", origin.Host)
			_ = nativeUpgradeHeaders("sk_speko_agent_owned").Write(writer)
			_, _ = io.WriteString(writer, tc.framing+"\r\n"+tc.body)
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("body-bearing upgrade did not promptly refuse: %v", err)
			}
			defer response.Body.Close()
			raw, err := io.ReadAll(io.LimitReader(response.Body, 8192))
			if err != nil || response.StatusCode != http.StatusBadRequest || !strings.Contains(string(raw), `"code":"invalid_request"`) {
				t.Fatalf("body-bearing upgrade was not refused before admission: status=%d err=%v", response.StatusCode, err)
			}
			nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
			if calls.Load() != 0 {
				t.Fatal("body-bearing local handshake started upstream work")
			}
		})
	}
}

func TestNativeRelayRealtimePreservesCloseInBothDirections(t *testing.T) {
	writeFrames := func(ctx context.Context, conn *websocket.Conn) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			frame := make([]byte, 32<<10)
			for range 256 {
				if conn.Write(ctx, websocket.MessageBinary, frame) != nil {
					return
				}
			}
		}()
		return done
	}
	for _, upstreamCloses := range []bool{true, false} {
		for _, code := range []websocket.StatusCode{websocket.StatusNormalClosure, websocket.StatusPolicyViolation, 4001} {
			t.Run(fmt.Sprintf("upstream-%t/status-%d", upstreamCloses, code), func(t *testing.T) {
				const reason = "owned realtime complete"
				observed := make(chan error, 1)
				peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := websocket.Accept(w, r, nil)
					if err != nil {
						observed <- err
						return
					}
					defer conn.CloseNow()
					ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
					defer cancel()
					if err := conn.Write(ctx, websocket.MessageText, []byte(`{"setupComplete":{}}`)); err != nil {
						observed <- err
						return
					}
					if upstreamCloses {
						// Close while the caller still sends audio. A writer EOF
						// must not overwrite the reader's native close status.
						if _, _, err := conn.Read(ctx); err != nil {
							observed <- err
							return
						}
						observed <- conn.Close(code, reason)
					} else {
						written := writeFrames(ctx, conn)
						_, _, err := conn.Read(ctx)
						<-written
						observed <- err
					}
				}))
				defer peer.Close()
				server, local := nativeServer(t, peer, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(local.URL, "http")+"/v1/google/gemini-live", &websocket.DialOptions{HTTPHeader: nativeHeaders("sk_speko_agent_owned")})
				if err != nil {
					t.Fatal(err)
				}
				defer conn.CloseNow()
				_, ready, err := conn.Read(ctx)
				if err != nil || string(ready) != `{"setupComplete":{}}` {
					t.Fatal("native readiness frame lost")
				}
				var closeErr error
				if upstreamCloses {
					written := writeFrames(ctx, conn)
					_, _, closeErr = conn.Read(ctx)
					<-written
				} else if err := conn.Close(code, reason); err != nil {
					t.Fatalf("local close handshake failed: %v", err)
				}
				select {
				case err := <-observed:
					if !upstreamCloses {
						closeErr = err
					} else if err != nil {
						t.Fatalf("upstream close handshake failed: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("native close did not release upstream")
				}
				var actual websocket.CloseError
				if !errors.As(closeErr, &actual) || actual.Code != code || actual.Reason != reason {
					t.Fatalf("native close status or reason lost: %v", closeErr)
				}
				nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
			})
		}
	}
}
