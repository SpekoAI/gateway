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
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/coder/websocket"
)

type nativeCapturedSocket struct {
	http.ResponseWriter
	conn   net.Conn
	reader *bufio.Reader
}

func (w *nativeCapturedSocket) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffer, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.conn, w.reader = conn, buffer.Reader
	}
	return conn, buffer, err
}

func nativeReadRawClose(reader *bufio.Reader) error {
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	if header[0] != 0x88 || header[1]&0x7f > 125 {
		return fmt.Errorf("expected native close frame, got %x", header)
	}
	var mask [4]byte
	if header[1]&0x80 != 0 {
		if _, err := io.ReadFull(reader, mask[:]); err != nil {
			return err
		}
	}
	body := make([]byte, int(header[1]&0x7f))
	_, err := io.ReadFull(reader, body)
	return err
}

func nativeLeavePartialFrame(conn net.Conn, masked bool) error {
	// Complete in-flight data, then another declared two-byte payload with its
	// final byte withheld. The library's Close discards this payload through a
	// bufio.Reader without applying its context deadline to the socket read.
	frames := []byte{0x82, 0x01, 'x', 0x82, 0x02, 'y'}
	if masked {
		frames = []byte{0x82, 0x81, 1, 2, 3, 4, 'x' ^ 1, 0x82, 0x82, 1, 2, 3, 4, 'y' ^ 1}
	}
	_, err := conn.Write(frames)
	return err
}

func TestNativeRelayForwardedCloseCannotRetainPartialFrame(t *testing.T) {
	for _, upstreamStalls := range []bool{false, true} {
		for _, drain := range []bool{false, true} {
			t.Run(fmt.Sprintf("upstream-%t/drain-%t", upstreamStalls, drain), func(t *testing.T) {
				attacked := make(chan struct{}, 1)
				peerDone := make(chan error, 1)
				peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					capture := &nativeCapturedSocket{ResponseWriter: w}
					conn, err := websocket.Accept(capture, r, nil)
					if err != nil {
						peerDone <- err
						return
					}
					defer capture.conn.Close()
					if !upstreamStalls {
						peerDone <- conn.Close(websocket.StatusNormalClosure, "owned close")
						return
					}
					_ = capture.conn.SetDeadline(time.Now().Add(3 * time.Second))
					if err := nativeReadRawClose(capture.reader); err != nil {
						peerDone <- err
						return
					}
					if err := nativeLeavePartialFrame(capture.conn, false); err != nil {
						peerDone <- err
						return
					}
					attacked <- struct{}{}
					var probe [1]byte
					_, err = capture.reader.Read(probe[:])
					if err == nil {
						err = errors.New("upstream stayed open after partial close")
					}
					if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
						peerDone <- err
						return
					}
					peerDone <- nil
				}))
				defer peer.Close()
				server, local := nativeServer(t, peer, func(cfg *gateway.NativeRelayConfig) {
					cfg.WriteTimeout = 100 * time.Millisecond
					if drain {
						cfg.WriteTimeout = 10 * time.Second
					}
				})
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				var stalledLocal *bufio.Reader
				if upstreamStalls {
					conn, _, err := websocket.Dial(ctx, local.URL+"/v1/stt/stream", &websocket.DialOptions{HTTPHeader: nativeHeaders("sk_speko_agent_owned")})
					if err != nil {
						t.Fatal(err)
					}
					defer conn.CloseNow()
					if err := conn.Close(websocket.StatusNormalClosure, "owned close"); err != nil {
						t.Fatal(err)
					}
					select {
					case <-attacked:
					case err := <-peerDone:
						t.Fatalf("upstream attack failed: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				} else {
					conn, err := net.DialTimeout("tcp", strings.TrimPrefix(local.URL, "http://"), time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					request, _ := http.NewRequest(http.MethodGet, local.URL+"/v1/xai/realtime?model=grok-voice-3.0", nil)
					request.Header = nativeUpgradeHeaders("sk_speko_agent_owned")
					if err := request.Write(conn); err != nil {
						t.Fatal(err)
					}
					reader := bufio.NewReader(conn)
					stalledLocal = reader
					response, err := http.ReadResponse(reader, request)
					if err != nil {
						t.Fatal(err)
					}
					if response.StatusCode != http.StatusSwitchingProtocols {
						t.Fatalf("upgrade: %d", response.StatusCode)
					}
					if err := nativeReadRawClose(reader); err != nil {
						t.Fatal(err)
					}
					if err := nativeLeavePartialFrame(conn, true); err != nil {
						t.Fatal(err)
					}
				}
				if drain {
					drainCtx, stop := context.WithTimeout(ctx, 10*time.Millisecond)
					err := server.Drain(drainCtx)
					stop()
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("drain should cancel the stalled session: %v", err)
					}
				}
				nativeAwait(t, func() bool { return server.Stats().ActiveSessions == 0 })
				if stalledLocal != nil {
					_, err := stalledLocal.ReadByte()
					if err == nil {
						t.Fatal("local socket stayed open after partial close")
					}
					if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
						t.Fatal("local physical socket was not closed")
					}
				}
				select {
				case err := <-peerDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("close left upstream alive")
				}
			})
		}
	}
}
