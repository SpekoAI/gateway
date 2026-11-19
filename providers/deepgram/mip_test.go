package deepgram

import (
	"testing"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestSpekoPaidKeyCoversManagedAndRelayOnly(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		execution protocol.Execution
		want      bool
	}{
		{"managed provider-direct", protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged}, true},
		{"relay connector key", protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged}, true},
		{"customer BYOK", protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK}, false},
	} {
		if got := spekoPaidKey(test.execution); got != test.want {
			t.Errorf("%s: spekoPaidKey = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestEndpointsCarryMIPOptOutOnlyWhenAsked(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	for _, optOut := range []bool{true, false} {
		want := ""
		if optOut {
			want = "true"
		}
		for _, route := range []struct{ endpoint, model string }{
			{"wss://api.deepgram.com/v1/listen", "nova-3"},
			{"wss://api.deepgram.com/v2/listen", "flux-general-en"},
		} {
			listen, err := listenEndpoint(policy, route.endpoint, route.model, protocol.RequestOptions{}, *media(), runtimepkg.AudioDeliveryLive, "", optOut)
			if err != nil {
				t.Fatalf("listen %s: %v", route.model, err)
			}
			if got := queryOf(t, listen).Get("mip_opt_out"); got != want {
				t.Errorf("listen %s optOut=%v: mip_opt_out=%q", route.model, optOut, got)
			}
		}
		for _, route := range []struct{ endpoint, model string }{
			{"wss://api.deepgram.com/v1/speak", "aura-2-thalia-en"},
			{"wss://api.deepgram.com/v2/speak", "flux-haley-en"},
		} {
			speak, _, err := speakEndpoint(policy, route.endpoint, route.model, protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}, optOut)
			if err != nil {
				t.Fatalf("speak %s: %v", route.model, err)
			}
			if got := queryOf(t, speak).Get("mip_opt_out"); got != want {
				t.Errorf("speak %s optOut=%v: mip_opt_out=%q", route.model, optOut, got)
			}
		}
	}
}

func TestMIPOptOutIsNotABilledFeature(t *testing.T) {
	t.Parallel()
	for name, base := range map[string]map[string]bool{"streaming": streamingBaseQueryKeys, "batch": batchBaseQueryKeys} {
		if _, features := billingDimensions(map[string][]string{"model": {"nova-3"}, "mip_opt_out": {"true"}}, base); len(features) != 0 {
			t.Errorf("%s: mip_opt_out reported as add-on features %v", name, features)
		}
	}
}
