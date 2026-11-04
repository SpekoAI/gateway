package main

import (
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
)

func TestNativeConfigNeedsNoFleetModelOrProviderSecret(t *testing.T) {
	t.Setenv("SPEKO_LOCAL_AUTH_TOKEN", "owned-local-token")
	t.Setenv("SPEKO_LOCAL_AUTH_TOKEN_FILE", "")
	t.Setenv("SPEKO_RELAY_URL", "https://relay.owned.test")
	t.Setenv("SPEKO_API_KEY_FILE", "/does-not-exist-must-not-be-read")
	t.Setenv("SPEKO_CARTESIA_BYOK_API_KEY_FILE", "/does-not-exist-must-not-be-read")
	t.Setenv("SPEKO_TELEMETRY_DISABLED", "not-a-boolean-must-not-be-read")
	cfg, socket, err := nativeConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RelayOrigin != "https://relay.owned.test" || cfg.LocalAuthToken != "owned-local-token" || socket == "" {
		t.Fatal("native bootstrap lost configuration")
	}
	if _, err = gateway.NewNativeRelay(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCLIProcess(t *testing.T) {
	if os.Getenv("SPEKO_TEST_NATIVE_CLI_PROCESS") != "1" {
		return
	}
	trustOwnedCLIPeer(t)
	os.Args = []string{os.Args[0], "native"}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCLIUnixSocketStreamsAndDrainsThroughSIGTERM(t *testing.T) {
	const modelKey = "sk_speko_agent_owned_cli"
	const localToken = "owned-local-cli-secret"
	const session = "51c2e1d0-5974-4380-959f-968ddab948d0"
	const body = `{"input":[{"content":"owned-never-log-this-body"}],"max_output_tokens":64}`
	first := []byte("event: response.output_text.delta\ndata: {\"delta\":\"owned\"}\n\n")
	final := []byte("event: response.completed\ndata: {\"status\":\"completed\"}\n\n")
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	observed := make(chan http.Header, 1)
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		if string(raw) != body {
			t.Error("native CLI normalized body")
		}
		observed <- r.Header.Clone()
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
	directory, err := os.MkdirTemp("", "native-cli-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	certificate := filepath.Join(directory, "peer.pem")
	if err = os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "runtime.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeCLIProcess$")
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "SPEKO_") && name != "SSL_CERT_FILE" && name != "SSL_CERT_DIR" {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "SPEKO_TEST_NATIVE_CLI_PROCESS=1", "SPEKO_LOCAL_AUTH_TOKEN="+localToken, "SPEKO_SOCKET_PATH="+socket, "SPEKO_RELAY_URL="+peer.URL, "SSL_CERT_FILE="+certificate,
		"SPEKO_API_KEY_FILE=/native-mode-must-not-read-global-key", "SPEKO_CARTESIA_BYOK_API_KEY_FILE=/native-mode-must-not-read-provider-key")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			<-done
		}
	}()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	status := func() int {
		probe, err := http.NewRequestWithContext(ctx, "GET", "http://speko-gateway/readyz", nil)
		if err != nil {
			return 0
		}
		response, err := client.Do(probe)
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	await := func(wanted int) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			if status() == wanted {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
		t.Fatalf("native CLI readiness did not reach %d", wanted)
	}
	await(200)
	request, _ := http.NewRequestWithContext(ctx, "POST", "http://speko-gateway/v1/llm/responses", strings.NewReader(body))
	request.Header.Set(gateway.NativeLocalAuthorization, "Bearer "+localToken)
	request.Header.Set("Authorization", "Bearer "+modelKey)
	request.Header.Set("Speko-Client-Session-ID", session)
	request.Header.Set("Idempotency-Key", "owned-cli-key")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw := make([]byte, len(first))
	if _, err = io.ReadFull(response.Body, raw); err != nil || !bytes.Equal(raw, first) {
		t.Fatalf("native CLI buffered stream: %v", err)
	}
	headers := <-observed
	if headers.Get("Authorization") != "Bearer "+modelKey || headers.Get("Speko-Client-Session-ID") != session || headers.Get(gateway.NativeLocalAuthorization) != "" {
		t.Fatal("native CLI changed credential scope")
	}
	if err = command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	await(503)
	select {
	case err = <-done:
		waited = true
		t.Fatalf("native CLI exited before active stream drained: %v", err)
	default:
	}
	finish()
	rest, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(rest, final) {
		t.Fatalf("SIGTERM lost terminal stream: %v", err)
	}
	err = <-done
	waited = true
	if err != nil {
		t.Fatalf("native CLI shutdown: %v", err)
	}
	for _, private := range []string{modelKey, localToken, session, "owned-never-log-this-body"} {
		if strings.Contains(output.String(), private) {
			t.Fatal("native CLI logged private session data")
		}
	}
}
