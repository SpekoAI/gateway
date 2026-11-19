package main

import (
	"crypto/x509"
	"os"
	"strings"
	"testing"
)

// Child-process fixtures use the real command with a CA owned by their test.
// Darwin's system verifier ignores SSL_CERT_FILE. Force Go's fallback pool in
// the test binary only; production trust configuration is untouched.
func trustOwnedCLIPeer(t *testing.T) {
	t.Helper()
	pem, err := os.ReadFile(os.Getenv("SSL_CERT_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("owned CLI peer certificate is invalid")
	}
	settings := []string{"x509usefallbackroots=1"}
	for _, setting := range strings.Split(os.Getenv("GODEBUG"), ",") {
		if setting != "" && !strings.HasPrefix(setting, "x509usefallbackroots=") {
			settings = append(settings, setting)
		}
	}
	t.Setenv("GODEBUG", strings.Join(settings, ","))
	x509.SetFallbackRoots(roots)
}
