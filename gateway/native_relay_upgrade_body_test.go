package gateway_test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/coder/websocket"
)

func TestNativeRelayBodyBearingUpgradeRefusesBeforeAnyUpstream(t *testing.T) {
	for _, bodyHeader := range []string{"Content-Length: 10\r\n", "Transfer-Encoding: chunked\r\n"} {
		t.Run(bodyHeader[:4], func(t *testing.T) {
			var upstream atomic.Int32
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstream.Add(1)
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				_, _, _ = conn.Read(r.Context())
			}))
			defer peer.Close()
			const setup = 50 * time.Millisecond
			server, local := nativeServer(t, peer, func(c *gateway.NativeRelayConfig) { c.SetupTimeout = setup; c.MaxSessions = 1 })
			endpoint, _ := url.Parse(local.URL)
			conn, err := net.Dial("tcp", endpoint.Host)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_, err = fmt.Fprintf(conn, "GET /v1/stt/stream HTTP/1.1\r\nHost: owned\r\nAuthorization: Bearer sk_speko_agent_owned\r\nSpeko-Local-Authorization: Bearer %s\r\nSpeko-Client-Session-ID: %s\r\nIdempotency-Key: owned-body-upgrade\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n%s\r\n", nativeTestLocal, nativeTestSession, bodyHeader)
			if err != nil {
				t.Fatal(err)
			}
			nativeAwait(t, func() bool { return server.Stats().SessionsTotal == 1 })
			time.Sleep(3 * setup)
			if upstream.Load() != 0 || server.Stats().ActiveSessions != 0 {
				t.Fatalf("body-bearing upgrade escaped/refused late: upstream=%d active=%d", upstream.Load(), server.Stats().ActiveSessions)
			}
		})
	}
}
