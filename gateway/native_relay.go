package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/SpekoAI/gateway/relayapi"
	"github.com/coder/websocket"
)

// NativeLocalAuthorization authenticates the socket caller independently of
// the session's model bearer. It is consumed locally and never sent upstream.
const NativeLocalAuthorization = "Speko-Local-Authorization"
const nativeMaxFrameBytes = 2 << 20

// NativeRelayConfig configures a transport, not a planner or financial owner.
// The fixed relay obtains real grants from its cell before provider I/O.
type NativeRelayConfig struct {
	RelayOrigin    string
	LocalAuthToken string
	// HTTPClient supplies transport/TLS; native mode owns deadlines and bearer headers.
	HTTPClient      *http.Client
	MaxSessions     int
	MaxRequestBytes int64
	SetupTimeout    time.Duration
	WriteTimeout    time.Duration
	// AllowInsecureLoopback permits owned local peers; the command uses HTTPS.
	AllowInsecureLoopback bool
}

// NativeRelayServer serves native model traffic on an authenticated local
// socket. Each operation carries its own model bearer and platform session.
// It holds no fleet model key, provider credential, signed plan or money state.
type NativeRelayServer struct {
	origin                    *url.URL
	client                    *http.Client
	localHash                 [sha256.Size]byte
	maximum                   int
	maxRequestBytes           int64
	setupTimeout, timeToWrite time.Duration
	mu                        sync.Mutex
	active                    map[uint64]context.CancelFunc
	sequence, total           uint64
	draining                  bool
	drained                   chan struct{}
	drainedClosed             bool
	proxy                     *httputil.ReverseProxy
}

// speko.agents.v1 session IDs are opaque, bounded identifiers, not just UUIDs.
// Keep their exact bytes: the cell verifies the canonical credential binding.
var nativeSessionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$`)
var nativeHTTPRoutes = map[string]bool{
	"/v1/llm/responses":      true,
	"/v1/stt/transcriptions": true,
	"/v1/tts/speech":         true,
}

// The vendor-native voice paths are deliberately separate. Query-selected
// protocols carry one exact model; GPT-Live and Gemini bind it in their first
// frame. No arbitrary URL settings, credentials or alternate relay are accepted.
var nativeVoiceRoutes = map[string]struct{}{
	relayapi.RealtimeRoutePath:        {},
	relayapi.XAIRealtimeRoutePath:     {},
	relayapi.LiveRoutePath:            {},
	relayapi.BidiRoutePath:            {},
	relayapi.TranslationRoutePath:     {},
	relayapi.QwenTranslationRoutePath: {},
}

func nativeQueryAllowed(r *http.Request) bool {
	if _, voice := nativeVoiceRoutes[r.URL.Path]; voice {
		return relayapi.ValidateNativeVoiceURL(r.URL) == nil
	}
	return r.URL.RawQuery == "" && !r.URL.ForceQuery && r.URL.RawPath == ""
}

func NewNativeRelay(config NativeRelayConfig) (*NativeRelayServer, error) {
	origin, err := url.Parse(config.RelayOrigin)
	if err != nil || origin == nil || origin.Host == "" || origin.User != nil || origin.Opaque != "" || origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") || origin.RawPath != "" {
		return nil, errors.New("gateway: native relay requires a configured HTTPS origin")
	}
	loopback := origin.Hostname() == "localhost"
	if ip := net.ParseIP(origin.Hostname()); ip != nil {
		loopback = ip.IsLoopback()
	}
	if origin.Scheme != "https" && !(config.AllowInsecureLoopback && origin.Scheme == "http" && loopback) {
		return nil, errors.New("gateway: native relay requires HTTPS")
	}
	if !nativeVisible(config.LocalAuthToken, 64<<10) {
		return nil, errors.New("gateway: native local authorization is required")
	}
	if config.MaxSessions == 0 {
		config.MaxSessions = 100
	}
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = 128 << 20
	}
	if config.SetupTimeout == 0 {
		config.SetupTimeout = 30 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 15 * time.Second
	}
	if config.MaxSessions < 1 || config.MaxRequestBytes < 1 || config.SetupTimeout < 0 || config.WriteTimeout < 0 {
		return nil, errors.New("gateway: invalid native relay bounds")
	}
	client := http.Client{}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
	}
	client.Jar = nil
	client.Timeout = 0
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	if original, ok := transport.(*http.Transport); ok {
		bounded := original.Clone()
		// HTTP headers may follow a complete batch inference. Bound network
		// establishment separately from the provider's execution lifetime.
		bounded.ResponseHeaderTimeout = 0
		bounded.TLSHandshakeTimeout = config.SetupTimeout
		dial := bounded.DialContext
		if dial == nil {
			dial = (&net.Dialer{}).DialContext
		}
		bounded.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			setup, cancel := context.WithTimeout(ctx, config.SetupTimeout)
			defer cancel()
			return dial(setup, network, address)
		}
		bounded.MaxResponseHeaderBytes = 64 << 10
		transport = bounded
	}
	client.Transport = transport
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("gateway: native relay redirect refused")
	}
	origin.Path = ""
	server := &NativeRelayServer{origin: origin, client: &client, localHash: sha256.Sum256([]byte(config.LocalAuthToken)), maximum: config.MaxSessions, maxRequestBytes: config.MaxRequestBytes, setupTimeout: config.SetupTimeout, timeToWrite: config.WriteTimeout, active: make(map[uint64]context.CancelFunc), drained: make(chan struct{})}
	server.proxy = &httputil.ReverseProxy{
		ErrorLog: log.New(io.Discard, "", 0),
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(server.origin)
			r.Out.URL.Path = r.In.URL.Path
			r.Out.URL.RawPath = ""
			r.Out.URL.RawQuery = ""
			r.Out.Host = server.origin.Host
			r.Out.Header = nativeUpstreamHeaders(r.In.Header)
		},
		Transport:     transport,
		FlushInterval: -1,
		ModifyResponse: func(r *http.Response) error {
			if r.StatusCode >= 300 && r.StatusCode < 400 {
				return errors.New("gateway: native relay redirect refused")
			}
			filtered := make(http.Header)
			for _, name := range []string{"Content-Type", "Content-Encoding", "Cache-Control", relayapi.HeaderRequestID, relayapi.HeaderProvider, relayapi.HeaderModel, relayapi.HeaderRegion, relayapi.HeaderUsageCharacters} {
				if value := r.Header.Values(name); len(value) > 0 {
					filtered[http.CanonicalHeaderKey(name)] = append([]string(nil), value...)
				}
			}
			r.Header = filtered
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "native request exceeds its bound")
				return
			}
			writeError(w, http.StatusBadGateway, "relay_unavailable", "native relay transport failed")
		},
	}
	return server, nil
}

func nativeVisible(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, c := range []byte(value) {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}
func nativeOne(h http.Header, name string) string {
	values := h.Values(name)
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

func nativeWebSocketRequest(w http.ResponseWriter, r *http.Request) bool {
	// net/http may drain an unread body while flushing the 101 response.
	// Do not admit upstream work behind an incomplete local HTTP message,
	// and close HTTP so writing the refusal cannot wait for that body either.
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		w.Header().Set("Connection", "close")
		return false
	}
	contains := func(name, expected string) bool {
		for _, line := range r.Header.Values(name) {
			for _, token := range strings.Split(line, ",") {
				if strings.EqualFold(strings.TrimSpace(token), expected) {
					return true
				}
			}
		}
		return false
	}
	if r.Method != http.MethodGet || !r.ProtoAtLeast(1, 1) || r.ProtoMajor != 1 ||
		!contains("Connection", "Upgrade") || !contains("Upgrade", "websocket") ||
		nativeOne(r.Header, "Sec-WebSocket-Version") != "13" {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(nativeOne(r.Header, "Sec-WebSocket-Key")))
	if err != nil || len(key) != 16 {
		return false
	}
	if origins := r.Header.Values("Origin"); len(origins) > 1 {
		return false
	} else if len(origins) == 1 && origins[0] != "" {
		origin, err := url.Parse(origins[0])
		if err != nil || origin.User != nil || !strings.EqualFold(origin.Host, r.Host) ||
			(!strings.EqualFold(origin.Scheme, "http") && !strings.EqualFold(origin.Scheme, "https")) {
			return false
		}
	}
	for depth := 0; depth < 8; depth++ {
		if _, ok := w.(http.Hijacker); ok {
			return true
		}
		if wrapped, ok := w.(interface{ Unwrap() http.ResponseWriter }); ok {
			w = wrapped.Unwrap()
		} else {
			break
		}
	}
	return false
}
func nativeUpstreamHeaders(source http.Header) http.Header {
	target := make(http.Header)
	for _, name := range []string{"Authorization", "Idempotency-Key", "Speko-Client-Session-ID", "User-Agent", "Content-Type", "Accept"} {
		if values := source.Values(name); len(values) > 0 {
			target[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
	return target
}
func (s *NativeRelayServer) authorizeLocal(w http.ResponseWriter, r *http.Request) bool {
	raw, validScheme := strings.CutPrefix(nativeOne(r.Header, NativeLocalAuthorization), "Bearer ")
	digest := sha256.Sum256([]byte(raw))
	if subtle.ConstantTimeCompare(s.localHash[:], digest[:]) != 1 || !validScheme {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid local credential")
		return false
	}
	return true
}
func (s *NativeRelayServer) authorizeModel(w http.ResponseWriter, r *http.Request) bool {
	value := nativeOne(r.Header, "Authorization")
	token, ok := strings.CutPrefix(value, "Bearer ")
	if !ok || !strings.HasPrefix(token, "sk_speko_") || len(token) <= len("sk_speko_") || !nativeVisible(token, 4096) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid model credential")
		return false
	}
	if !nativeSessionID.MatchString(nativeOne(r.Header, "Speko-Client-Session-ID")) || !nativeVisible(nativeOne(r.Header, "Idempotency-Key"), 512) {
		writeError(w, http.StatusBadRequest, "invalid_request", "session identity and idempotency key are required")
		return false
	}
	return true
}

func (s *NativeRelayServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		draining := s.draining
		s.mu.Unlock()
		if draining {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorizeLocal(w, r) {
			return
		}
		state := s.Stats()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# TYPE speko_gateway_native_requests_active gauge\nspeko_gateway_native_requests_active %d\n# TYPE speko_gateway_native_requests_total counter\nspeko_gateway_native_requests_total %d\n# TYPE speko_gateway_draining gauge\nspeko_gateway_draining %d\n", state.ActiveSessions, state.SessionsTotal, boolMetric(state.Draining))
	})
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorizeLocal(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"runtime": map[string]any{"name": "go-gateway", "placement": "sidecar", "provider_routes": []string{"speko_relay"}, "adapters": []string{"speko.relay.stt.v1", "speko.relay.tts.v1", "speko.relay.llm.v1", "speko.relay.realtime.v1"}}})
	})
	mux.HandleFunc("GET /v1/stt/stream", s.speech)
	mux.HandleFunc("GET /v1/tts/stream", s.speech)
	for path := range nativeVoiceRoutes {
		mux.HandleFunc("GET "+path, s.speech)
	}
	for path := range nativeHTTPRoutes {
		mux.HandleFunc("POST "+path, s.modelHTTP)
	}
	return mux
}
func (s *NativeRelayServer) begin(w http.ResponseWriter, r *http.Request) (context.Context, func(), bool) {
	if !s.authorizeLocal(w, r) || !s.authorizeModel(w, r) {
		return nil, nil, false
	}
	if !nativeQueryAllowed(r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "native model route requires its exact bounded model selector")
		return nil, nil, false
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "gateway_draining", "gateway is draining")
		return nil, nil, false
	}
	if len(s.active) >= s.maximum {
		s.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, "capacity_exhausted", "native gateway capacity is full")
		return nil, nil, false
	}
	s.sequence++
	id := s.sequence
	ctx, cancel := context.WithCancel(r.Context())
	s.active[id] = cancel
	s.total++
	s.mu.Unlock()
	return ctx, func() {
		cancel()
		s.mu.Lock()
		delete(s.active, id)
		s.closeDrained()
		s.mu.Unlock()
	}, true
}
func (s *NativeRelayServer) modelHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, done, ok := s.begin(w, r)
	if !ok {
		return
	}
	defer done()
	if r.ContentLength > s.maxRequestBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "native request exceeds its bound")
		return
	}
	r = r.Clone(ctx)
	control := http.NewResponseController(w)
	bodyContext, cancelBody := context.WithCancel(ctx)
	defer cancelBody()
	r = r.WithContext(bodyContext)
	r.Body = &nativeRelayBody{ReadCloser: http.MaxBytesReader(w, r.Body, s.maxRequestBytes), control: control, timeout: s.setupTimeout, cancel: cancelBody}
	stop := context.AfterFunc(ctx, func() { _ = control.SetReadDeadline(time.Now()); _ = control.SetWriteDeadline(time.Now()) })
	defer stop()
	s.proxy.ServeHTTP(nativeRelayWriter{ResponseWriter: w, timeout: s.timeToWrite, ctx: ctx}, r)
}

// The setup bound applies to stalled upload reads, and is cleared once the
// upload finishes. It must never become a model-execution deadline.
type nativeRelayBody struct {
	io.ReadCloser
	control *http.ResponseController
	timeout time.Duration
	cancel  context.CancelFunc
}

func (b *nativeRelayBody) Read(p []byte) (int, error) {
	_ = b.control.SetReadDeadline(time.Now().Add(b.timeout))
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.Is(err, io.EOF) || errors.As(err, &tooLarge) {
			// The transport must receive the size error itself. Cancelling here
			// can replace it with context.Canceled before the proxy maps it to 413.
			_ = b.control.SetReadDeadline(time.Time{})
		} else {
			_ = b.control.SetReadDeadline(time.Now())
			b.cancel()
		}
	}
	return n, err
}
func (b *nativeRelayBody) Close() error {
	_ = b.control.SetReadDeadline(time.Now())
	err := b.ReadCloser.Close()
	_ = b.control.SetReadDeadline(time.Time{})
	return err
}

type nativeRoundTrip func(*http.Request) (*http.Response, error)

func (f nativeRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Bound each physical write rather than the lifetime of a long SSE response.
type nativeRelayWriter struct {
	http.ResponseWriter
	timeout time.Duration
	ctx     context.Context
}

func (w nativeRelayWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w nativeRelayWriter) Write(body []byte) (int, error) {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.ResponseWriter.Write(body)
}
func (w nativeRelayWriter) Flush() {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
	if w.ctx.Err() != nil {
		return
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (s *NativeRelayServer) speech(w http.ResponseWriter, r *http.Request) {
	ctx, done, ok := s.begin(w, r)
	if !ok {
		return
	}
	defer done()
	// Query-selected voice can start work as soon as the upstream upgrades.
	// Validate the local handshake first, while retaining the ability to pass
	// an upstream's canonical HTTP refusal back before upgrading this socket.
	if !nativeWebSocketRequest(w, r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "a valid local WebSocket upgrade is required")
		return
	}
	endpoint := *s.origin
	endpoint.Path = r.URL.Path
	endpoint.RawQuery = r.URL.RawQuery
	dialCtx, cancelDial := context.WithTimeout(ctx, s.setupTimeout)
	// websocket.Dial retains only 1 KiB of an unsuccessful upgrade body.
	// Capture the bounded canonical envelope before that diagnostic truncation.
	client := *s.client
	var rejection *relayapi.ErrorEnvelope
	var rejectionBody []byte
	client.Transport = nativeRoundTrip(func(req *http.Request) (*http.Response, error) {
		response, err := s.client.Transport.RoundTrip(req)
		if err == nil && response.StatusCode >= 400 && response.StatusCode <= 599 && response.Body != nil {
			raw, readErr := io.ReadAll(io.LimitReader(response.Body, 8193))
			_ = response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(string(raw)))
			var envelope relayapi.ErrorEnvelope
			if readErr == nil && len(raw) <= 8192 && json.Unmarshal(raw, &envelope) == nil && envelope.Validate() == nil {
				rejection = &envelope
				rejectionBody = raw
			}
		}
		return response, err
	})
	upstream, response, err := websocket.Dial(dialCtx, endpoint.String(), &websocket.DialOptions{HTTPClient: &client, HTTPHeader: nativeUpstreamHeaders(r.Header)})
	cancelDial()
	if err != nil {
		status := http.StatusBadGateway
		if response != nil {
			if response.Body != nil {
				_ = response.Body.Close()
			}
			if response.StatusCode >= 400 && response.StatusCode <= 599 {
				status = response.StatusCode
				if requestID := response.Header.Get(relayapi.HeaderRequestID); nativeVisible(requestID, 256) {
					w.Header().Set(relayapi.HeaderRequestID, requestID)
				}
			}
		}
		if rejection != nil {
			if nativeVisible(rejection.Error.RequestID, 256) {
				w.Header().Set(relayapi.HeaderRequestID, rejection.Error.RequestID)
			}
			// Re-encoding may expand HTML/JS characters beyond the same 8 KiB
			// bound enforced by native clients. Preserve the validated JSON.
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.WriteHeader(status)
			_, _ = w.Write(rejectionBody)
			return
		}
		writeError(w, status, "relay_unavailable", "native relay handshake failed")
		return
	}
	defer upstream.CloseNow()
	local, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer local.CloseNow()
	upstream.SetReadLimit(nativeMaxFrameBytes)
	local.SetReadLimit(nativeMaxFrameBytes)
	stop := context.AfterFunc(ctx, func() { _ = upstream.CloseNow(); _ = local.CloseNow() })
	defer stop()
	type copyResult struct {
		destination *websocket.Conn
		err         error
		fromRead    bool
	}
	copyFrame := func(dst, src *websocket.Conn) copyResult {
		for {
			kind, body, err := src.Read(ctx)
			if err != nil {
				return copyResult{dst, err, true}
			}
			writeCtx, cancel := context.WithTimeout(ctx, s.timeToWrite)
			err = dst.Write(writeCtx, kind, body)
			cancel()
			if err != nil {
				return copyResult{dst, err, false}
			}
		}
	}
	complete := make(chan copyResult, 2)
	go func() { complete <- copyFrame(upstream, local) }()
	go func() { complete <- copyFrame(local, upstream) }()
	first := <-complete
	secondReceived := false
	if !first.fromRead {
		// A concurrent writer can observe a closed socket before its reader
		// returns the actual close frame. Let that reader finish within the
		// existing network bound so its status is not replaced by a write EOF.
		deadline := time.NewTimer(s.timeToWrite)
		select {
		case first = <-complete:
			secondReceived = true
		case <-deadline.C:
		case <-ctx.Done():
		}
		deadline.Stop()
	}
	var closed websocket.CloseError
	if first.fromRead && errors.As(first.err, &closed) {
		// A close frame is part of the native protocol. Forward its exact
		// status/reason; an abnormal EOF stays abnormal. Close has its own
		// bounded handshake and the opposite copy continues reading its ack.
		_ = first.destination.Close(closed.Code, closed.Reason)
	}
	_ = upstream.CloseNow()
	_ = local.CloseNow()
	if !secondReceived {
		<-complete
	}
}
func (s *NativeRelayServer) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{ActiveSessions: int64(len(s.active)), SessionCapacity: s.maximum, SessionsTotal: s.total, Draining: s.draining}
}
func (s *NativeRelayServer) closeDrained() {
	if s.draining && len(s.active) == 0 && !s.drainedClosed {
		s.drainedClosed = true
		close(s.drained)
	}
}
func (s *NativeRelayServer) BeginDrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
	s.closeDrained()
}
func (s *NativeRelayServer) Drain(ctx context.Context) error {
	s.BeginDrain()
	select {
	case <-s.drained:
		s.client.CloseIdleConnections()
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		cancel := make([]context.CancelFunc, 0, len(s.active))
		for _, stop := range s.active {
			cancel = append(cancel, stop)
		}
		s.mu.Unlock()
		for _, stop := range cancel {
			stop()
		}
		s.client.CloseIdleConnections()
		return ctx.Err()
	}
}
func (s *NativeRelayServer) Serve(listener net.Listener) error {
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 64 << 10}
	return server.Serve(listener)
}
