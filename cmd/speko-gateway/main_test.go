package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSecretReadsEnvironmentOrFileWithoutAmbiguity(t *testing.T) {
	t.Run("environment", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_SECRET", "  value-from-env  ")
		got, err := secret("TEST_GATEWAY_SECRET")
		if err != nil || got != "value-from-env" {
			t.Fatalf("secret = %q, err=%v", got, err)
		}
	})

	t.Run("file", func(t *testing.T) {
		secretPath := filepath.Join(t.TempDir(), "provider-key")
		if err := os.WriteFile(secretPath, []byte("value-from-file\n"), 0o600); err != nil {
			t.Fatalf("write secret fixture: %v", err)
		}
		t.Setenv("TEST_GATEWAY_SECRET_FILE", secretPath)
		got, err := secret("TEST_GATEWAY_SECRET")
		if err != nil || got != "value-from-file" {
			t.Fatalf("secret = %q, err=%v", got, err)
		}
	})

	t.Run("mutually exclusive", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_SECRET", "value")
		t.Setenv("TEST_GATEWAY_SECRET_FILE", "/not/read")
		if _, err := secret("TEST_GATEWAY_SECRET"); err == nil {
			t.Fatal("environment and file values were both accepted")
		}
	})

	t.Run("bounded", func(t *testing.T) {
		secretPath := filepath.Join(t.TempDir(), "oversized")
		if err := os.WriteFile(secretPath, []byte(strings.Repeat("x", maxSecretBytes+1)), 0o600); err != nil {
			t.Fatalf("write secret fixture: %v", err)
		}
		t.Setenv("TEST_GATEWAY_SECRET_FILE", secretPath)
		if _, err := secret("TEST_GATEWAY_SECRET"); err == nil {
			t.Fatal("oversized secret was accepted")
		}
	})
}

func TestBoolEnv(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		if got, err := boolEnv("TEST_GATEWAY_BOOL", true); err != nil || !got {
			t.Fatalf("bool env = %t, err=%v", got, err)
		}
	})
	t.Run("false", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_BOOL", "false")
		if got, err := boolEnv("TEST_GATEWAY_BOOL", true); err != nil || got {
			t.Fatalf("bool env = %t, err=%v", got, err)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_BOOL", "sometimes")
		if _, err := boolEnv("TEST_GATEWAY_BOOL", true); err == nil {
			t.Fatal("invalid boolean was accepted")
		}
	})
}

func TestDurationEnv(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		if got, err := durationEnv("TEST_GATEWAY_DURATION", time.Minute); err != nil || got != time.Minute {
			t.Fatalf("duration env = %s, err=%v", got, err)
		}
	})
	t.Run("value", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_DURATION", "90s")
		if got, err := durationEnv("TEST_GATEWAY_DURATION", time.Minute); err != nil || got != 90*time.Second {
			t.Fatalf("duration env = %s, err=%v", got, err)
		}
	})
	t.Run("too small", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_DURATION", "500ms")
		if _, err := durationEnv("TEST_GATEWAY_DURATION", time.Minute); err == nil {
			t.Fatal("sub-second duration was accepted")
		}
	})
}
