package relayapi_test

import (
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestModelsResponseRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(r *relayapi.ModelsResponse)
		want   string
	}{
		{"truncated digest", func(r *relayapi.ModelsResponse) { r.CatalogDigest = "sha256:abc" }, "catalog_digest"},
		{"unprefixed digest", func(r *relayapi.ModelsResponse) { r.CatalogDigest = r.CatalogDigest[len("sha256:"):] }, "catalog_digest"},
		{"model without id", func(r *relayapi.ModelsResponse) { r.Models[0].ID = "" }, "models[0]: id and provider are required"},
		{"model without provider", func(r *relayapi.ModelsResponse) { r.Models[0].Provider = " " }, "models[0]: id and provider are required"},
		{"unknown kind", func(r *relayapi.ModelsResponse) { r.Models[0].Kind = "realtime" }, "kind: unsupported value"},
		{"no regions", func(r *relayapi.ModelsResponse) { r.Models[1].Regions = nil }, "regions: at least one routable region is required"},
		{"blank region", func(r *relayapi.ModelsResponse) { r.Models[1].Regions[0] = "" }, "regions[0]"},
		{"speech without formats", func(r *relayapi.ModelsResponse) { r.Models[1].AudioFormats = nil }, "audio_formats"},
		{"llm with formats", func(r *relayapi.ModelsResponse) { r.Models[0].AudioFormats = r.Models[1].AudioFormats }, "must be omitted"},
		{"format without rate constraint", func(r *relayapi.ModelsResponse) {
			r.Models[1].AudioFormats[0].SampleRateRangeHz = nil
		}, "exactly one"},
		{"format with both rate constraints", func(r *relayapi.ModelsResponse) {
			r.Models[1].AudioFormats[0].SampleRatesHz = []int{16_000}
		}, "exactly one"},
		{"format without channels", func(r *relayapi.ModelsResponse) {
			r.Models[1].AudioFormats[0].Channels = nil
		}, "channels"},
		{"s2s without output formats", func(r *relayapi.ModelsResponse) { r.Models[2].OutputAudioFormats = nil }, "output_audio_formats"},
		{"s2s without endpoint", func(r *relayapi.ModelsResponse) { r.Models[2].Endpoint = "" }, "endpoint"},
		{"s2s on a foreign route", func(r *relayapi.ModelsResponse) { r.Models[2].Endpoint = "/v1/stt/stream" }, "endpoint"},
		{"s2s without protocol", func(r *relayapi.ModelsResponse) { r.Models[3].Protocol = "" }, "protocol"},
		{"s2s with batch limits", func(r *relayapi.ModelsResponse) {
			r.Models[3].BatchAudioLimits = &relayapi.BatchAudioLimits{MaxPCMBytes: 1}
		}, "batch_audio_limits"},
		{"stt with output formats", func(r *relayapi.ModelsResponse) { r.Models[1].OutputAudioFormats = r.Models[2].OutputAudioFormats }, "output_audio_formats: valid only"},
		{"llm with endpoint", func(r *relayapi.ModelsResponse) { r.Models[0].Endpoint = "/v1/live" }, "endpoint and protocol"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var response relayapi.ModelsResponse
			decodeFixture(t, "models-response.json", &response)
			if err := response.Validate(); err != nil {
				t.Fatalf("fixture must validate before mutation: %v", err)
			}
			tc.mutate(&response)
			assertInvalid(t, response.Validate(), tc.want)
		})
	}
}
