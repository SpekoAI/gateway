package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SpekoAI/gateway/gateway"
)

// Native mode uses no process-wide model key, hosted planner or provider
// credentials. Each authenticated socket caller supplies its session bearer.
func nativeConfigFromEnv() (gateway.NativeRelayConfig, string, error) {
	token, err := secret("SPEKO_LOCAL_AUTH_TOKEN")
	if err != nil {
		return gateway.NativeRelayConfig{}, "", err
	}
	if token == "" {
		return gateway.NativeRelayConfig{}, "", errors.New("SPEKO_LOCAL_AUTH_TOKEN (or _FILE) is required")
	}
	origin := env("SPEKO_RELAY_URL")
	if origin == "" {
		return gateway.NativeRelayConfig{}, "", errors.New("SPEKO_RELAY_URL is required in native mode")
	}
	maximum, err := positiveIntEnv("SPEKO_MAX_SESSIONS", 100)
	if err != nil {
		return gateway.NativeRelayConfig{}, "", err
	}
	bytes, err := positiveIntEnv("SPEKO_NATIVE_MAX_REQUEST_BYTES", 128<<20)
	if err != nil {
		return gateway.NativeRelayConfig{}, "", err
	}
	setup, err := durationEnv("SPEKO_NATIVE_SETUP_TIMEOUT", 30*time.Second)
	if err != nil {
		return gateway.NativeRelayConfig{}, "", err
	}
	write, err := durationEnv("SPEKO_NATIVE_WRITE_TIMEOUT", 15*time.Second)
	if err != nil {
		return gateway.NativeRelayConfig{}, "", err
	}
	socket := env("SPEKO_SOCKET_PATH")
	if socket == "" {
		socket = "/run/speko/runtime.sock"
	}
	return gateway.NativeRelayConfig{RelayOrigin: origin, LocalAuthToken: token, MaxSessions: maximum, MaxRequestBytes: int64(bytes), SetupTimeout: setup, WriteTimeout: write}, socket, nil
}

func runNative() error {
	cfg, socket, err := nativeConfigFromEnv()
	if err != nil {
		return err
	}
	server, err := gateway.NewNativeRelay(cfg)
	if err != nil {
		return err
	}
	listener, err := gateway.ListenUnix(socket)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close(); _ = os.Remove(socket) }()
	process, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	log.Printf("routing=native-relay")
	return serveGateway(process, server, listener, nil, nil, 0, nil)
}
