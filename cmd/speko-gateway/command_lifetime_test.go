package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
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
)

// This runs the real command setup, signal subscription, serving and shutdown
// in a child process. No provider plan or media request is made.
func TestGatewayCLIProcess(t *testing.T) {
	if os.Getenv("SPEKO_TEST_CLI_LIFETIME_PROCESS") != "1" {
		return
	}
	trustOwnedCLIPeer(t)
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayCLIUsesOneFencedLifetimeThroughSIGTERM(t *testing.T) {
	incarnations := make(map[string]bool)
	for _, mode := range []string{"uncertain-put", "missing-ack", "refused"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var records []instanceWire
			first := make(chan struct{})
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record := decodeInstanceWire(t, r)
				mu.Lock()
				records = append(records, record)
				initial := len(records) == 1
				mu.Unlock()
				if initial {
					close(first)
				}
				if mode == "uncertain-put" && initial {
					// The report was accepted but its response is lost when the
					// real command cancels the periodic request during SIGTERM.
					<-r.Context().Done()
					return
				}
				if mode == "missing-ack" {
					w.WriteHeader(204)
					return
				}
				if mode == "refused" {
					w.Header().Set("X-Request-ID", "private-fixture-request-id")
					w.WriteHeader(403)
					_, _ = w.Write([]byte("private-fixture-response"))
					return
				}
				acknowledgeInstance(w)
			}))
			defer peer.Close()
			directory, err := os.MkdirTemp("", "gcli-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(directory) })
			certificate := filepath.Join(directory, "peer.pem")
			if err := os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.Certificate().Raw}), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGatewayCLIProcess$")
			for _, entry := range os.Environ() {
				name, _, _ := strings.Cut(entry, "=")
				if !strings.HasPrefix(name, "SPEKO_") && name != "SSL_CERT_FILE" && name != "SSL_CERT_DIR" {
					command.Env = append(command.Env, entry)
				}
			}
			command.Env = append(command.Env,
				"SPEKO_TEST_CLI_LIFETIME_PROCESS=1", "SPEKO_API_KEY=sk_speko_fixture", "SPEKO_LOCAL_AUTH_TOKEN=fixture-local-token",
				"SPEKO_TELEMETRY_DISABLED=true", "SPEKO_WARM_PLAN_TARGET=0", "SPEKO_CONTROL_PLANE_URL="+peer.URL,
				"SPEKO_RUNTIME_INSTANCE_ID=fixture/%2F worker🙂", "SPEKO_INSTANCE_HEARTBEAT_INTERVAL=1s",
				"SPEKO_SOCKET_PATH="+filepath.Join(directory, "runtime.sock"), "SSL_CERT_FILE="+certificate,
			)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			joined := false
			defer func() {
				if !joined {
					_ = command.Process.Kill()
					<-done
				}
			}()
			select {
			case <-first:
			case err := <-done:
				joined = true
				t.Fatalf("command exited before reporting: %v %s", err, output.String())
			case <-ctx.Done():
				t.Fatal("command never reported")
			}
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				joined = true
				if err != nil {
					t.Fatalf("command shutdown: %v %s", err, output.String())
				}
			case <-ctx.Done():
				t.Fatal("command did not drain and exit")
			}
			for _, private := range []string{"fixture/%2F", "sk_speko_fixture", "private-fixture"} {
				if strings.Contains(output.String(), private) {
					t.Fatal("instance diagnostics exposed private request data")
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(records) < 3 || records[len(records)-1].method != http.MethodDelete {
				t.Fatal("actual command did not close its versioned lifetime")
			}
			incarnation := records[0].value.Incarnation
			if incarnations[incarnation] {
				t.Fatal("processes reused an incarnation")
			}
			incarnations[incarnation] = true
			for i, r := range records {
				if r.value.Incarnation != incarnation || !r.value.StartedAt.Equal(records[0].value.StartedAt) {
					t.Fatal("command changed its process identity")
				}
				if i > 0 && (r.value.Sequence < records[i-1].value.Sequence || (r.value.Sequence == records[i-1].value.Sequence && r.body != records[i-1].body)) {
					t.Fatal("command changed retry identity or reordered mutations")
				}
			}
			var terminal map[string]json.RawMessage
			_ = json.Unmarshal([]byte(records[len(records)-1].body), &terminal)
			if len(terminal) != 3 {
				t.Fatal("terminal DELETE includes non-lifetime fields")
			}
			if mode == "uncertain-put" {
				if records[0].body != records[1].body || !records[len(records)-2].value.Draining {
					t.Fatal("command did not replay the uncertain report before draining")
				}
			}
			if _, err := os.Stat(filepath.Join(directory, "runtime.sock")); !os.IsNotExist(err) {
				t.Fatalf("command socket was not removed: %v", err)
			}
		})
	}
}
