package runtime_test

import (
	"context"
	"errors"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"testing"
	"time"
)

func TestInvalidSignatureNeverPublishesCapacityOrOpensProvider(t *testing.T) {
	adapter := mock.NewSTTAdapter("mock.stt.v1")
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return errors.New("rejected") }), Now: func() time.Time { return fixedNow }})
	if err != nil {
		t.Fatal(err)
	}
	plan := validPlan(protocol.SessionKindSTT, adapter.ID(), 60)
	called := false
	_, err = engine.Open(context.Background(), runtimepkg.OpenRequest{Kind: protocol.SessionKindSTT, Plan: plan, Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1}, OnVerifiedCapacity: func(*protocol.CapacityAttribution, protocol.CredentialSource) { called = true }})
	if err == nil || called || adapter.LastStream() != nil {
		t.Fatal("unverified plan crossed accounting/provider boundary")
	}
}
