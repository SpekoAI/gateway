package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// sttTestRecognizePath is transcribed by hand from the v2 discovery document's
// flatPath for speech.projects.locations.recognizers.recognize:
// "v2/projects/{projectsId}/locations/{locationsId}/recognizers/{recognizersId}:recognize".
//
// It is a literal on purpose. Building it from the adapter's own regexp or
// constants would make the test agree with whatever the adapter does, which is
// exactly how a misspelled wire string stays green.
const sttTestRecognizePath = "/v2/projects/speko-stt/locations/eu/recognizers/_:recognize"

// TestSTTRecognizeUsesDocumentedRESTContract pins every wire detail a unit test
// can pin and a production incident cannot: the method, the path, the auth
// channel, and the exact shape of RecognizeRequest. The body is compared as a
// WHOLE document rather than field by field, because the failure this guards
// against is an extra or renamed member, which per-field assertions never see.
func TestSTTRecognizeUsesDocumentedRESTContract(t *testing.T) {
	t.Parallel()
	pcm := sttSamplePCM(3_200)
	observed := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- r.Clone(r.Context())
		bodies <- body
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		writeRecognizeResponse(t, w, `{"results":[{"alternatives":[{"transcript":"नमस्ते","confidence":0.94}],"resultEndOffset":"1.500s"}],"metadata":{"requestId":"req-abc"}}`)
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, pcm)
	collectSTTEvents(t, stream.Events(), 3)

	request := <-observed
	if request.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", request.Method)
	}
	if request.URL.Path != sttTestRecognizePath {
		t.Fatalf("path = %q, want %q", request.URL.Path, sttTestRecognizePath)
	}
	if got := request.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("content-type = %q", got)
	}

	var got map[string]any
	if err := json.Unmarshal(<-bodies, &got); err != nil {
		t.Fatalf("request body is not JSON: %v", err)
	}
	want := map[string]any{
		"config": map[string]any{
			// explicitDecodingConfig, NOT autoDecodingConfig: the discovery
			// document marks the explicit form "Required if using headerless PCM
			// audio (linear16, mulaw, alaw)", which is all this gateway carries.
			"explicitDecodingConfig": map[string]any{
				"encoding":          "LINEAR16",
				"sampleRateHertz":   float64(16_000),
				"audioChannelCount": float64(1),
			},
			// languageCodes is REPEATED even for a single language, and it is
			// plural; a scalar "languageCode" is the Cloud TTS spelling.
			"languageCodes": []any{"hi-IN"},
			"model":         "chirp_3",
			"features":      map[string]any{"enableAutomaticPunctuation": true},
		},
		"content": base64.StdEncoding.EncodeToString(pcm),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request body =\n%#v\nwant\n%#v", got, want)
	}
	// Stated separately because it is the single easiest mistake to make here:
	// the gRPC message and every Google SDK sample put `recognizer` INSIDE the
	// request object, but on the REST surface it is a path parameter and
	// RecognizeRequest has only config/configMask/content/uri.
	if _, present := got["recognizer"]; present {
		t.Fatalf("recognizer must be a path parameter, not a body member: %#v", got)
	}
	closeSTTStream(t, stream)
}

// TestSTTContentIsTheBase64OfTheExactAudio guards the base64 hop. Sending raw
// bytes, or double-encoding them, still produces a well-formed request that
// Google answers -- with a transcript of noise.
func TestSTTContentIsTheBase64OfTheExactAudio(t *testing.T) {
	t.Parallel()
	pcm := sttSamplePCM(9_001) // Deliberately not a multiple of 3, so base64 pads.
	bodies := make(chan []byte, 1)
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- body
		writeRecognizeResponse(t, w, `{"results":[]}`)
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, pcm)
	collectSTTEvents(t, stream.Events(), 1)

	var body struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(<-bodies, &body); err != nil {
		t.Fatalf("body: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(body.Content)
	if err != nil {
		t.Fatalf("content is not standard base64: %v", err)
	}
	if !reflect.DeepEqual(decoded, pcm) {
		t.Fatalf("decoded content = %d bytes, want the %d PCM bytes written", len(decoded), len(pcm))
	}
	closeSTTStream(t, stream)
}

// TestSTTLanguageCodeIsRegionQualified guards the field that makes or breaks
// this adapter. Speech V2 rejects a bare primary subtag outright -- the
// platform adapter records the live failure `The language "en" is not supported
// by the model "chirp_3" in the location named "us"` -- and Chirp 3 keys its
// language table on the full tag. hi/ta/te are the board's rank-1 languages.
func TestSTTLanguageCodeIsRegionQualified(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		language string
		want     string
	}{
		{name: "bare hindi expands to the only published variant", language: "hi", want: "hi-IN"},
		{name: "bare tamil expands", language: "ta", want: "ta-IN"},
		{name: "bare telugu expands", language: "te", want: "te-IN"},
		// Ported from the platform adapter's region-default table: Google keys
		// generic Spanish as es-US, not es-ES.
		{name: "bare spanish uses the ported default", language: "es", want: "es-US"},
		{name: "bare english uses the ported default", language: "en", want: "en-US"},
		// Tagalog is normalized across its whole family: bare `fil` is off-spec,
		// so is the ISO 639-1 twin `tl`, and even a region-bearing tl-PH becomes
		// fil-PH.
		{name: "fil normalizes", language: "fil", want: "fil-PH"},
		{name: "tl normalizes to fil", language: "tl", want: "fil-PH"},
		{name: "tl-PH normalizes to fil-PH", language: "tl-PH", want: "fil-PH"},
		// The service normalizes case itself ("en-us becomes en-US") but the
		// canonical tag is what gets logged and compared, so it is normalized
		// locally too.
		{name: "lowercase region is canonicalized", language: "en-us", want: "en-US"},
		{name: "underscore separator is accepted", language: "pt_BR", want: "pt-BR"},
		// A four-letter script subtag has to survive: the Chirp table publishes
		// cmn-Hans-CN, yue-Hant-HK and pa-Guru-IN.
		{name: "script subtag is preserved and title-cased", language: "cmn-hans-cn", want: "cmn-Hans-CN"},
		{name: "cantonese script subtag", language: "yue-Hant-HK", want: "yue-Hant-HK"},
		{name: "punjabi script subtag", language: "pa-guru-in", want: "pa-Guru-IN"},
		// Three-letter primary subtags appear in the table (ast-ES, nso-ZA).
		{name: "three letter primary subtag", language: "nso-za", want: "nso-ZA"},
		// A numeric UN M.49 region must survive uppercasing untouched.
		{name: "numeric region is preserved", language: "es-419", want: "es-419"},
		// Swahili is the one language the Chirp table publishes bare, so a
		// blanket region requirement would refuse a documented tag.
		{name: "swahili is published bare", language: "sw", want: "sw"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			bodies := make(chan []byte, 1)
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				bodies <- body
				writeRecognizeResponse(t, w, `{"results":[]}`)
			})
			defer server.Close()

			stream := openSTTStreamWith(t, server, protocol.CredentialsBYOK, nil, func(request *runtimepkg.AdapterRequest) {
				request.Options.Language = testCase.language
			})
			sttListen(t, stream, sttSamplePCM(64))
			collectSTTEvents(t, stream.Events(), 1)

			var body struct {
				Config struct {
					LanguageCodes []string `json:"languageCodes"`
				} `json:"config"`
			}
			if err := json.Unmarshal(<-bodies, &body); err != nil {
				t.Fatalf("body: %v", err)
			}
			if len(body.Config.LanguageCodes) != 1 || body.Config.LanguageCodes[0] != testCase.want {
				t.Fatalf("languageCodes = %v, want [%q]", body.Config.LanguageCodes, testCase.want)
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTTranscriptsAreAlwaysFinal is the honesty test for this adapter.
//
// Recognize returns SpeechRecognitionResult, which has no isFinal and no
// stability -- those live on StreamingRecognitionResult, reachable only over
// gRPC. So there is nothing to derive an interim result from, and every
// transcript must be final. If someone later "adds streaming" by splitting a
// completed result into synthetic deltas, this fails.
func TestSTTTranscriptsAreAlwaysFinal(t *testing.T) {
	t.Parallel()
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeRecognizeResponse(t, w, `{"results":[
			{"alternatives":[{"transcript":"first segment","confidence":0.91}],"resultEndOffset":"1.250s","languageCode":"hi-IN"},
			{"alternatives":[{"transcript":"second segment","confidence":0.88}],"resultEndOffset":"2.500s"}
		],"metadata":{"requestId":"req-final","totalBilledDuration":"2.500s"}}`)
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, sttSamplePCM(640))
	events := collectSTTEvents(t, stream.Events(), 4)

	if got := strings.Join(sttEventTypes(events), ","); got != "transcript.final,transcript.final,usage.observed,speech.ended" {
		t.Fatalf("event order = %s", got)
	}
	for index, event := range events[:2] {
		if event.Type == protocol.EventTranscriptDelta {
			t.Fatalf("event %d is a fabricated interim result", index)
		}
	}

	var first struct {
		Text                 string  `json:"text"`
		IsFinal              bool    `json:"is_final"`
		Confidence           float64 `json:"confidence"`
		AudioEndMS           int64   `json:"audio_end_ms"`
		Model                string  `json:"model"`
		LanguageCode         string  `json:"language_code"`
		DetectedLanguageCode string  `json:"detected_language_code"`
		Location             string  `json:"location"`
	}
	if err := json.Unmarshal(events[0].Data, &first); err != nil {
		t.Fatalf("transcript data: %v", err)
	}
	if first.Text != "first segment" || !first.IsFinal || first.Confidence != 0.91 {
		t.Fatalf("transcript data = %s", events[0].Data)
	}
	// google-duration is decimal seconds with a mandatory "s" suffix; 1.250s is
	// 1250 ms, not 1 ms and not 1250000.
	if first.AudioEndMS != 1_250 {
		t.Fatalf("audio_end_ms = %d, want 1250", first.AudioEndMS)
	}
	// SpeechRecognitionResult.languageCode is the language DETECTED in the
	// audio, which is a different fact from the one that was requested.
	if first.DetectedLanguageCode != "hi-IN" || first.LanguageCode != "hi-IN" {
		t.Fatalf("language fields = %s", events[0].Data)
	}
	if first.Model != "chirp_3" || first.Location != "eu" {
		t.Fatalf("routing fields = %s", events[0].Data)
	}
	// The second result never reported a detected language, so the field must be
	// absent rather than present-and-empty.
	if strings.Contains(string(events[1].Data), "detected_language_code") {
		t.Fatalf("absent detected language must not be reported: %s", events[1].Data)
	}
	// The raw vendor result is preserved verbatim for debugging, under a
	// namespace naming the API version that produced it. The key is written out
	// rather than referenced: an extension namespace is a wire-visible contract
	// for anything consuming these events, so a silent rename must fail here.
	if raw := events[0].Extensions["speech.googleapis.com/v2"]; raw == nil || !strings.Contains(string(raw), `"confidence":0.91`) {
		t.Fatalf("transcript must carry the raw vendor result under speech.googleapis.com/v2, got %s (keys %v)", raw, sttExtensionKeys(events[0].Extensions))
	}

	var ended struct {
		ResultCount int    `json:"result_count"`
		AudioEndMS  int64  `json:"audio_end_ms"`
		Reason      string `json:"reason"`
	}
	if err := json.Unmarshal(events[3].Data, &ended); err != nil {
		t.Fatalf("speech.ended data: %v", err)
	}
	if ended.ResultCount != 2 || ended.AudioEndMS != 2_500 || ended.Reason != "recognition_complete" {
		t.Fatalf("speech.ended data = %s", events[3].Data)
	}
	closeSTTStream(t, stream)
}

// TestSTTEmitsResultsWhileTheResponseIsStillArriving proves the adapter decodes
// incrementally rather than buffer-then-publish. The handler withholds the
// second result until the first transcript has already been observed; if the
// adapter waited for the whole body, this deadlocks and times out.
//
// This is the only sense in which a unary method can be incremental, and it is
// worth pinning precisely because it is easy to lose in a refactor to
// json.Unmarshal.
func TestSTTEmitsResultsWhileTheResponseIsStillArriving(t *testing.T) {
	t.Parallel()
	firstSeen := make(chan struct{})
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server response writer cannot flush")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[{"alternatives":[{"transcript":"early","confidence":0.5}],"resultEndOffset":"0.500s"}`)
		flusher.Flush()
		select {
		case <-firstSeen:
		case <-time.After(3 * time.Second):
			t.Error("adapter buffered the whole body instead of decoding it incrementally")
		}
		_, _ = io.WriteString(w, `,{"alternatives":[{"transcript":"late","confidence":0.6}],"resultEndOffset":"1.000s"}],"metadata":{"requestId":"req-stream"}}`)
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, sttSamplePCM(320))

	var transcripts []string
	deadline := time.After(5 * time.Second)
	for done := false; !done; {
		select {
		case event, open := <-stream.Events():
			if !open {
				t.Fatal("events closed before speech.ended")
			}
			if event.Err != nil {
				t.Fatalf("provider error: %v", event.Err)
			}
			switch event.Type {
			case protocol.EventTranscriptFinal:
				var payload struct {
					Text string `json:"text"`
				}
				if err := json.Unmarshal(event.Data, &payload); err != nil {
					t.Fatalf("transcript data: %v", err)
				}
				transcripts = append(transcripts, payload.Text)
				if len(transcripts) == 1 {
					close(firstSeen)
				}
			case protocol.EventSpeechEnded:
				done = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for speech.ended")
		}
	}
	if strings.Join(transcripts, "|") != "early|late" {
		t.Fatalf("transcripts = %v", transcripts)
	}
	closeSTTStream(t, stream)
}

// TestSTTUsageObservedCarriesTheGoogleRequestID covers both metering sources:
// RecognitionResponseMetadata.requestId ("Global request identifier
// auto-generated by the API") and, when a deployment does not populate it, the
// frontend correlation header.
func TestSTTUsageObservedCarriesTheGoogleRequestID(t *testing.T) {
	t.Parallel()
	t.Run("from response metadata", func(t *testing.T) {
		t.Parallel()
		server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			// A header is set too, to prove the body wins when both exist.
			w.Header().Set("X-Goog-Request-Id", "header-should-lose")
			writeRecognizeResponse(t, w, `{"results":[],"metadata":{"requestId":"body-wins","totalBilledDuration":"7.250s"}}`)
		})
		defer server.Close()

		stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
		sttListen(t, stream, sttSamplePCM(64))
		events := collectSTTEvents(t, stream.Events(), 2)
		if events[0].Type != protocol.EventUsageObserved {
			t.Fatalf("first event = %s, want usage.observed", events[0].Type)
		}
		var usage struct {
			ProviderRequestID string `json:"provider_request_id"`
			BilledDurationMS  int64  `json:"billed_duration_ms"`
		}
		if err := json.Unmarshal(events[0].Data, &usage); err != nil {
			t.Fatalf("usage data: %v", err)
		}
		if usage.ProviderRequestID != "body-wins" || usage.BilledDurationMS != 7_250 {
			t.Fatalf("usage data = %s", events[0].Data)
		}
		closeSTTStream(t, stream)
	})

	t.Run("falls back to the correlation header", func(t *testing.T) {
		t.Parallel()
		server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("X-Goog-Request-Id", "header-fallback")
			writeRecognizeResponse(t, w, `{"results":[]}`)
		})
		defer server.Close()

		stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
		sttListen(t, stream, sttSamplePCM(64))
		events := collectSTTEvents(t, stream.Events(), 2)
		if events[0].Type != protocol.EventUsageObserved {
			t.Fatalf("first event = %s, want usage.observed", events[0].Type)
		}
		if !strings.Contains(string(events[0].Data), "header-fallback") {
			t.Fatalf("usage data = %s", events[0].Data)
		}
		closeSTTStream(t, stream)
	})
}

// TestSTTHandlesMembersInAnyOrder: proto3 JSON promises no member ordering, so
// a decoder that assumes results-then-metadata would drop metering on a
// service that serializes the other way. An unknown member must be skipped
// rather than treated as corruption.
func TestSTTHandlesMembersInAnyOrder(t *testing.T) {
	t.Parallel()
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeRecognizeResponse(t, w, `{"metadata":{"requestId":"metadata-first"},"someFutureMember":{"nested":[1,2,3]},"results":[{"alternatives":[{"transcript":"ok","confidence":0.7}]}]}`)
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, sttSamplePCM(64))
	events := collectSTTEvents(t, stream.Events(), 3)
	if got := strings.Join(sttEventTypes(events), ","); got != "usage.observed,transcript.final,speech.ended" {
		t.Fatalf("event order = %s", got)
	}
	closeSTTStream(t, stream)
}

// TestSTTSilenceIsReportedAsAnEndedTurnNotAnError: an empty results array is a
// legitimate outcome for silent audio. Reporting it as a provider error would
// tear down a session over a quiet caller. An alternative with an empty
// transcript is likewise skipped rather than published as an empty final turn.
func TestSTTSilenceIsReportedAsAnEndedTurnNotAnError(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "no results", body: `{"results":[]}`},
		{name: "results member absent entirely", body: `{}`},
		{name: "result with no alternatives", body: `{"results":[{"resultEndOffset":"0.100s"}]}`},
		{name: "result with a blank transcript", body: `{"results":[{"alternatives":[{"transcript":"   "}]}]}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				writeRecognizeResponse(t, w, testCase.body)
			})
			defer server.Close()

			stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
			sttListen(t, stream, sttSamplePCM(64))
			events := collectSTTEvents(t, stream.Events(), 1)
			if events[0].Type != protocol.EventSpeechEnded {
				t.Fatalf("event = %s, want speech.ended", events[0].Type)
			}
			var ended struct {
				ResultCount int `json:"result_count"`
			}
			if err := json.Unmarshal(events[0].Data, &ended); err != nil {
				t.Fatalf("speech.ended data: %v", err)
			}
			if ended.ResultCount != 0 {
				t.Fatalf("result_count = %d, want 0", ended.ResultCount)
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTStatusMappingMatchesTheProtocolContract keeps each upstream failure in
// its own bucket. A caller retries on provider_unavailable and
// provider_rate_limited, re-authenticates on authentication_failed, and must
// never retry invalid_request -- collapsing them wastes credit or hides a bug.
func TestSTTStatusMappingMatchesTheProtocolContract(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		status    int
		body      string
		code      string
		retryable bool
		message   string
	}{
		{
			name: "bad language", status: http.StatusBadRequest,
			body:      `{"error":{"code":400,"message":"The language \"en\" is not supported by the model \"chirp_3\" in the location named \"us\".","status":"INVALID_ARGUMENT"}}`,
			code:      "invalid_request",
			message:   `is not supported by the model "chirp_3"`,
			retryable: false,
		},
		{
			name: "expired token", status: http.StatusUnauthorized,
			body: `{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}`,
			code: "authentication_failed", retryable: false,
		},
		{
			name: "no permission on the project", status: http.StatusForbidden,
			body: `{"error":{"code":403,"message":"Permission denied on resource project speko-stt.","status":"PERMISSION_DENIED"}}`,
			code: "authentication_failed", retryable: false,
		},
		{
			name: "oversized request", status: http.StatusRequestEntityTooLarge,
			body: `{"error":{"code":413,"message":"Request payload size exceeds the limit.","status":"FAILED_PRECONDITION"}}`,
			code: "input_too_large", retryable: false,
		},
		{
			name: "quota", status: http.StatusTooManyRequests,
			body: `{"error":{"code":429,"message":"Quota exceeded.","status":"RESOURCE_EXHAUSTED"}}`,
			code: "provider_rate_limited", retryable: true,
		},
		{
			name: "internal", status: http.StatusInternalServerError,
			body: `{"error":{"code":500,"message":"Internal error.","status":"INTERNAL"}}`,
			code: "provider_unavailable", retryable: true,
		},
		{
			name: "unavailable", status: http.StatusServiceUnavailable,
			body: `{"error":{"code":503,"message":"The service is currently unavailable.","status":"UNAVAILABLE"}}`,
			code: "provider_unavailable", retryable: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(testCase.status)
				_, _ = io.WriteString(w, testCase.body)
			})
			defer server.Close()

			stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
			sttListen(t, stream, sttSamplePCM(64))
			providerErr := awaitSTTProviderError(t, stream.Events())
			if providerErr.Code != testCase.code {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.code)
			}
			if providerErr.Retryable != testCase.retryable {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.retryable)
			}
			if providerErr.ProviderStatus != testCase.status {
				t.Fatalf("provider status = %d, want %d", providerErr.ProviderStatus, testCase.status)
			}
			// Google's own message is the payload that makes a 400 debuggable, so
			// it has to survive into the canonical error rather than be replaced by
			// a generic string.
			if testCase.message != "" && !strings.Contains(providerErr.Message, testCase.message) {
				t.Fatalf("message = %q, want it to quote %q", providerErr.Message, testCase.message)
			}
			raw := providerErr.Extensions["speech.googleapis.com/v2"]
			if raw == nil || !strings.Contains(string(raw), `"status"`) {
				t.Fatalf("google.rpc.Status must be preserved verbatim under speech.googleapis.com/v2, got %s (keys %v)", raw, sttExtensionKeys(providerErr.Extensions))
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTOpenRejectsUnsupportedRequests keeps a misrouted plan from ever
// reaching Google with a customer credential attached.
func TestSTTOpenRejectsUnsupportedRequests(t *testing.T) {
	t.Parallel()
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeRecognizeResponse(t, w, `{"results":[]}`)
	})
	defer server.Close()

	for _, testCase := range []struct {
		name   string
		mutate func(*runtimepkg.AdapterRequest)
		reason string
	}{
		{name: "wrong kind", reason: "stt sessions", mutate: func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS }},
		{name: "wrong provider", reason: "cannot open provider", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" }},
		{name: "websocket transport", reason: "http transport", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Transport = protocol.TransportWebSocket
		}},
		// A grpc plan is the interesting rejection: StreamingRecognize really is
		// gRPC, so a control plane could plausibly route one here. This adapter
		// does not speak gRPC and must say so rather than half-work.
		{name: "grpc transport", reason: "http transport", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Transport = protocol.TransportGRPC
		}},
		{name: "auto model", reason: "concrete model", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }},
		{name: "empty model", reason: "concrete model", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "  " }},
		{name: "missing credential", reason: "bearer credential", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }},
		{name: "wrong credential kind", reason: "bearer credential", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
		}},
		{name: "blank credential", reason: "non-empty bearer", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = "   "
		}},
		// An API key sent as a bearer token 401s upstream with a message that
		// reads exactly like an expired OAuth token. Catch it locally instead.
		{name: "api key mistaken for a token", reason: "looks like an API key", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = "AIzaSyD-not-an-access-token"
		}},
		{name: "unknown credential source", reason: "credential source", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.CredentialSource = protocol.CredentialSource("borrowed")
		}},
		// Refusing to guess a language beats transcribing Hindi as confident
		// English nonsense.
		{name: "no language", reason: "requires a language code", mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Language = "" }},
		{name: "unknown bare language", reason: "region subtag", mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Language = "zz" }},
		{name: "not a language tag", reason: "not a BCP-47", mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Language = "english please" }},
		{name: "missing media", reason: "media configuration", mutate: func(r *runtimepkg.AdapterRequest) { r.Media = nil }},
		// OGG_OPUS and WEBM_OPUS are containers; a realtime transport carries bare
		// opus packets that no encoding member describes.
		{name: "opus media", reason: "pcm_s16le", mutate: func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }},
		// protocol.MediaFormat allows up to 192000 Hz; ExplicitDecodingConfig
		// documents "Valid values are: 8000-48000".
		{name: "sample rate above the documented ceiling", reason: "sample rate must be between", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Media.SampleRateHz = 96_000
		}},
		{name: "named recognizer", reason: "implicit recognizer", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/recognizers/_:recognize", "/recognizers/my-tuned-recognizer:recognize", 1)
		}},
		{name: "disallowed host", reason: "host is not allowed", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "https://evil.example.com" + sttTestRecognizePath
		}},
		{name: "wrong path", reason: "endpoint path must be", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, ":recognize", ":batchRecognize", 1)
		}},
		// v1 has a recognize method too (speech.speech.recognize at
		// /v1/speech:recognize) but Chirp 3 is "exclusively available within the
		// Speech-to-Text API V2", so a v1 path must not be accepted.
		{name: "v1 path", reason: "endpoint path must be", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/v2/", "/v1/", 1)
		}},
		{name: "credential in the query string", reason: "clean absolute https URL", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint += "?key=AIzaSyD"
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := NewSTT(sttTestConfig(server))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttGoogleRequest(server, protocol.CredentialsBYOK)
			testCase.mutate(&request)
			stream, err := adapter.Open(context.Background(), request)
			if err == nil {
				_ = stream.Close(context.Background())
				t.Fatalf("open succeeded, want rejection mentioning %q", testCase.reason)
			}
			if !strings.Contains(err.Error(), testCase.reason) {
				t.Fatalf("error = %v, want it to mention %q", err, testCase.reason)
			}
		})
	}
}

// TestSTTEndpointHostMustAgreeWithTheRecognizerLocation guards the routing bug
// this provider is uniquely prone to: the location appears TWICE, once in the
// host and once in the recognizer path, and Google reconciles neither.
//
// This is not hypothetical bookkeeping. Our STT board found Chirp 3 takes rank
// one on hi/ta/te only when served from `eu`, and a plan assembled from a
// sibling Vertex product's region (us-central1, global) reaches a host that
// exists but serves no Chirp 3 -- a failure that reads like a model problem.
func TestSTTEndpointHostMustAgreeWithTheRecognizerLocation(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		endpoint string
		wantErr  string
	}{
		{name: "eu host with eu recognizer", endpoint: "https://eu-speech.googleapis.com/v2/projects/p/locations/eu/recognizers/_:recognize"},
		{name: "us host with us recognizer", endpoint: "https://us-speech.googleapis.com/v2/projects/p/locations/us/recognizers/_:recognize"},
		{name: "specific asia region", endpoint: "https://asia-southeast1-speech.googleapis.com/v2/projects/p/locations/asia-southeast1/recognizers/_:recognize"},
		// The global host has no location in it, so there is nothing to
		// contradict; the discovery rootUrl is https://speech.googleapis.com/.
		{name: "global host is not cross-checked", endpoint: "https://speech.googleapis.com/v2/projects/p/locations/us/recognizers/_:recognize"},
		{
			name:     "us host pointed at an eu recognizer",
			endpoint: "https://us-speech.googleapis.com/v2/projects/p/locations/eu/recognizers/_:recognize",
			wantErr:  `serves location "us" but the recognizer path names "eu"`,
		},
		{
			// The exact near-miss the platform adapter's pickSttLocation exists to
			// prevent: a Gemini Live envelope pinned to us-central1 reused for STT.
			name:     "eu host pointed at a us-central1 recognizer",
			endpoint: "https://eu-speech.googleapis.com/v2/projects/p/locations/us-central1/recognizers/_:recognize",
			wantErr:  `serves location "eu" but the recognizer path names "us-central1"`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			policy, err := newSTTEndpointPolicy([]string{sttOfficialAPIHost}, sttRegionalAPIHosts, false)
			if err != nil {
				t.Fatalf("policy: %v", err)
			}
			target, err := policy.parse(testCase.endpoint)
			if testCase.wantErr != "" {
				if err == nil {
					t.Fatalf("parse succeeded, want rejection mentioning %q", testCase.wantErr)
				}
				if !strings.Contains(err.Error(), testCase.wantErr) {
					t.Fatalf("error = %v, want it to mention %q", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if target.Project != "p" {
				t.Fatalf("project = %q, want p", target.Project)
			}
			if want := "projects/p/locations/" + target.Location + "/recognizers/_"; target.Recognizer() != want {
				t.Fatalf("recognizer = %q, want %q", target.Recognizer(), want)
			}
		})
	}
}

// TestSTTTextOperationsAreUnsupported: an STT session produces no output text,
// and the runtime distinguishes "wrong operation" from "failed operation".
func TestSTTTextOperationsAreUnsupported(t *testing.T) {
	t.Parallel()
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {})
	defer server.Close()
	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	if err := stream.AppendText(context.Background(), "hello"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("append text = %v", err)
	}
	if err := stream.CommitText(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("commit text = %v", err)
	}
	closeSTTStream(t, stream)
}

// TestSTTEnforcesTheSynchronousAudioCeiling checks the published content
// limits, both of them, and checks that the SMALLER one wins.
//
// The limits are in different units -- "~1 Minute" of audio length and 10 MB of
// request size -- so which one binds depends on the media format. At 16 kHz
// mono a minute is 1.92 MB, so a byte-only check would silently accept half an
// hour of audio; at 48 kHz across 8 channels a minute is 46 MB, so a
// duration-only check would build a request Google refuses.
func TestSTTEnforcesTheSynchronousAudioCeiling(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		sampleRate int
		channels   int
		wantLimit  int
	}{
		// 16000 Hz * 1 channel * 2 bytes * 60 s.
		{name: "duration binds at the documented-optimal rate", sampleRate: 16_000, channels: 1, wantLimit: 1_920_000},
		// 8000 Hz * 1 channel * 2 bytes * 60 s: telephony, still duration-bound.
		{name: "duration binds at telephony rate", sampleRate: 8_000, channels: 1, wantLimit: 960_000},
		// 48000 Hz * 8 channels * 2 bytes * 60 s is 46 MB, so the 10 MB request
		// ceiling binds first. 10 MB of base64 holds 10485760/4*3 raw bytes, less
		// slack for the JSON envelope.
		{name: "request size binds on wideband multichannel", sampleRate: 48_000, channels: 8, wantLimit: 10<<20/4*3 - (16 << 10)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				writeRecognizeResponse(t, w, `{"results":[]}`)
			})
			defer server.Close()

			stream := openSTTStreamWith(t, server, protocol.CredentialsBYOK, nil, func(request *runtimepkg.AdapterRequest) {
				request.Media.SampleRateHz = testCase.sampleRate
				request.Media.Channels = testCase.channels
			})
			// Exactly at the ceiling is accepted; one byte past it is not.
			if err := stream.WriteAudio(context.Background(), make([]byte, testCase.wantLimit)); err != nil {
				t.Fatalf("write of exactly %d bytes = %v, want acceptance", testCase.wantLimit, err)
			}
			var providerErr *runtimepkg.ProviderError
			if err := stream.WriteAudio(context.Background(), []byte{0}); !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
				t.Fatalf("write past the ceiling = %v, want input_too_large", err)
			}
			if err := stream.WriteAudio(context.Background(), nil); err == nil {
				t.Fatal("empty write must be rejected")
			}
			_ = stream.Cancel(context.Background())
			if err := stream.CommitAudio(context.Background()); err == nil {
				t.Fatal("commit with nothing buffered must be rejected")
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTCancelAbandonsTheRequestAndKeepsTheSession: Cancel exists so a
// barge-in can be followed by another utterance, so it reports a warning rather
// than a terminal error and the session stays usable.
func TestSTTCancelAbandonsTheRequestAndKeepsTheSession(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	firstArrived := make(chan struct{})
	// Same escape hatch as the Abort test: a failing assertion must not leave
	// the first handler parked, or server.Close() hangs the whole package.
	unparked := make(chan struct{})
	var unparkOnce sync.Once
	defer func() { unparkOnce.Do(func() { close(unparked) }) }()

	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if requests.Add(1) == 1 {
			close(firstArrived)
			select {
			case <-r.Context().Done(): // The client walked away, as Cancel should make it.
			case <-unparked:
			}
			return
		}
		writeRecognizeResponse(t, w, `{"results":[{"alternatives":[{"transcript":"second turn","confidence":0.8}]}]}`)
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, sttSamplePCM(320))
	awaitSTTSignal(t, firstArrived, "first recognition never reached the server")

	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	warning := collectSTTEvents(t, stream.Events(), 1)[0]
	if warning.Type != protocol.EventWarning || !strings.Contains(string(warning.Data), "provider_request_cancelled") {
		t.Fatalf("cancel event = %s %s", warning.Type, warning.Data)
	}

	// The session must still work: this is a barge-in, not a teardown.
	sttListen(t, stream, sttSamplePCM(320))
	events := collectSTTEvents(t, stream.Events(), 2)
	if got := strings.Join(sttEventTypes(events), ","); got != "transcript.final,speech.ended" {
		t.Fatalf("second turn events = %s", got)
	}
	closeSTTStream(t, stream)
}

// TestSTTAbortTearsDownAnInFlightRequest: Abort follows a terminal runtime
// failure, so the in-flight request is cancelled rather than drained and the
// session refuses further input.
func TestSTTAbortTearsDownAnInFlightRequest(t *testing.T) {
	t.Parallel()
	arrived := make(chan struct{})
	// unparked is the escape hatch. The handler normally waits for the request
	// context to be cancelled by Abort, but if an assertion below fails first
	// nothing ever cancels it, the deferred server.Close() blocks on the parked
	// request, and the package times out instead of reporting which assertion
	// failed. That is not hypothetical: it cost a ten-minute run to find.
	unparked := make(chan struct{})
	var unparkOnce sync.Once
	unpark := func() { unparkOnce.Do(func() { close(unparked) }) }
	defer unpark()

	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(arrived)
		select {
		case <-r.Context().Done():
		case <-unparked:
		}
	})
	defer server.Close()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, sttSamplePCM(320))
	awaitSTTSignal(t, arrived, "recognition never reached the server")

	aborter, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("google stt stream must implement AbortingProviderStream")
	}
	if err := aborter.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	for range stream.Events() { //nolint:revive // drain until the channel closes
	}
	if err := stream.WriteAudio(context.Background(), []byte{0, 0}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after abort = %v, want ErrSessionClosed", err)
	}
}

// TestSTTRejectsAConcurrentSecondRecognition: the unary method has one
// in-flight slot, and silently interleaving two would attribute the second
// utterance's audio to the first request.
func TestSTTRejectsAConcurrentSecondRecognition(t *testing.T) {
	t.Parallel()
	arrived := make(chan struct{})
	release := make(chan struct{})
	server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(arrived)
		<-release
		writeRecognizeResponse(t, w, `{"results":[]}`)
	})
	defer server.Close()

	// Released via sync.Once so a FAILING assertion below still unparks the
	// handler. Without this the deferred server.Close() blocks on the parked
	// request forever and the whole package times out instead of reporting which
	// assertion failed — which is exactly how a real defect here stayed invisible.
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	defer unpark()

	stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
	sttListen(t, stream, sttSamplePCM(320))
	awaitSTTSignal(t, arrived, "recognition never reached the server")

	// The REASON matters, not just the rejection. Both calls would also fail
	// with "no buffered audio" once the buffer was drained by the first commit,
	// so asserting err != nil alone passes even with the in-flight guard removed.
	if err := stream.WriteAudio(context.Background(), sttSamplePCM(64)); err == nil || !strings.Contains(err.Error(), "previous recognition has not completed") {
		t.Fatalf("write during an in-flight recognition = %v, want the in-flight guard", err)
	}
	if err := stream.CommitAudio(context.Background()); err == nil || !strings.Contains(err.Error(), "previous recognition has not completed") {
		t.Fatalf("commit during an in-flight recognition = %v, want the in-flight guard", err)
	}
	unpark()
	collectSTTEvents(t, stream.Events(), 1)
	closeSTTStream(t, stream)
}

// TestSTTQuotaProjectHeaderIsSentOnlyWhenConfigured: x-goog-user-project makes
// the request require serviceusage.services.use on that project, so inferring
// it from the recognizer path would turn working plans into 403s.
func TestSTTQuotaProjectHeaderIsSentOnlyWhenConfigured(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		quota  string
		header string
		want   bool
	}{
		{name: "absent by default", quota: "", want: false},
		{name: "sent when configured", quota: "speko-billing", header: "speko-billing", want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			headers := make(chan http.Header, 1)
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				headers <- r.Header.Clone()
				writeRecognizeResponse(t, w, `{"results":[]}`)
			})
			defer server.Close()

			stream := openSTTStream(t, server, protocol.CredentialsBYOK, func(config *STTConfig) {
				config.QuotaProject = testCase.quota
			})
			sttListen(t, stream, sttSamplePCM(64))
			collectSTTEvents(t, stream.Events(), 1)
			// PRESENCE, not value. Header.Get cannot tell an unset header from one
			// set to "", so a value comparison would pass even if the adapter
			// unconditionally sent an empty x-goog-user-project -- which is not the
			// same request, and is exactly what triggers the
			// serviceusage.services.use requirement this default avoids.
			values, present := (<-headers)[http.CanonicalHeaderKey("x-goog-user-project")]
			if present != testCase.want {
				t.Fatalf("x-goog-user-project present = %v (%q), want %v", present, values, testCase.want)
			}
			if testCase.want && (len(values) != 1 || values[0] != testCase.header) {
				t.Fatalf("x-goog-user-project = %q, want [%q]", values, testCase.header)
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTBothCredentialSourcesUseTheBearerHeader: Google exposes exactly one
// documented header mechanism for this API, and no session-scoped credential
// exists to split managed from BYOK. A split would be fiction.
func TestSTTBothCredentialSourcesUseTheBearerHeader(t *testing.T) {
	t.Parallel()
	for _, source := range []protocol.CredentialSource{protocol.CredentialsBYOK, protocol.CredentialsManaged} {
		t.Run(string(source), func(t *testing.T) {
			t.Parallel()
			headers := make(chan http.Header, 1)
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				headers <- r.Header.Clone()
				writeRecognizeResponse(t, w, `{"results":[]}`)
			})
			defer server.Close()

			stream := openSTTStream(t, server, source, nil)
			sttListen(t, stream, sttSamplePCM(64))
			collectSTTEvents(t, stream.Events(), 1)
			header := <-headers
			if got, want := header.Get("Authorization"), "Bearer "+sttCredentialFor(source); got != want {
				t.Fatalf("authorization = %q, want %q", got, want)
			}
			// Speech V2 can render errors as HTML through some frontends; asking
			// for JSON explicitly is what keeps statusError's google.rpc.Status
			// parse meaningful.
			if got := header.Get("Accept"); got != "application/json" {
				t.Fatalf("accept = %q, want application/json", got)
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTMalformedResponsesAreClassifiedNotIgnored: a 200 whose body is not a
// RecognizeResponse is an upstream fault. Swallowing it would look like silence.
func TestSTTMalformedResponsesAreClassifiedNotIgnored(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		body string
		want string
	}{
		{name: "not an object", body: `["nope"]`, want: "not a JSON object"},
		{name: "results is not an array", body: `{"results":{"alternatives":[]}}`, want: "results member is not an array"},
		{name: "truncated", body: `{"results":[{"alternatives":[{"transcript":"cut`, want: "could not be read"},
		{name: "result is not an object", body: `{"results":["plain string"]}`, want: "malformed recognition result"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newRecognizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				writeRecognizeResponse(t, w, testCase.body)
			})
			defer server.Close()

			stream := openSTTStream(t, server, protocol.CredentialsBYOK, nil)
			sttListen(t, stream, sttSamplePCM(64))
			providerErr := awaitSTTProviderError(t, stream.Events())
			if !strings.Contains(providerErr.Message, testCase.want) {
				t.Fatalf("message = %q, want it to mention %q", providerErr.Message, testCase.want)
			}
			closeSTTStream(t, stream)
		})
	}
}

// TestSTTValidateMediaBounds exercises the decoding bounds directly, because
// protocol.MediaFormat.Validate runs first in Open and is stricter on some
// axes: it already caps channels at 8, so an Open-level test can never reach
// the adapter's own channel check. That check is still worth having -- it is
// the one derived from ExplicitDecodingConfig rather than from this gateway's
// protocol -- and this is the only place that can prove it works.
func TestSTTValidateMediaBounds(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		media  protocol.MediaFormat
		reason string
	}{
		{name: "documented optimum", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}},
		{name: "lowest documented rate", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 8_000, Channels: 1}},
		{name: "highest documented rate", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 48_000, Channels: 1}},
		{name: "maximum documented channels", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 8}},
		// "Valid values are: 8000-48000" -- one hertz outside on either side.
		{name: "just below the rate floor", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 7_999, Channels: 1}, reason: "sample rate must be between"},
		{name: "just above the rate ceiling", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 48_001, Channels: 1}, reason: "sample rate must be between"},
		// "The maximum allowed value is 8."
		{name: "nine channels", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 9}, reason: "at most 8 audio channels"},
		{name: "zero channels", media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 0}, reason: "at most 8 audio channels"},
		{name: "opus is a container elsewhere", media: protocol.MediaFormat{Encoding: "opus", SampleRateHz: 48_000, Channels: 1}, reason: "pcm_s16le"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := sttValidateMedia(testCase.media)
			if testCase.reason == "" {
				if err != nil {
					t.Fatalf("validate = %v, want acceptance", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate succeeded, want rejection mentioning %q", testCase.reason)
			}
			if !strings.Contains(err.Error(), testCase.reason) {
				t.Fatalf("error = %v, want it to mention %q", err, testCase.reason)
			}
		})
	}
}

// TestSTTDurationMilliseconds pins the google-duration parser. proto3 renders
// Duration as decimal seconds with a mandatory "s" suffix, so a value that is
// not one must be reported as absent rather than as a zero offset -- a
// transcript claiming audio_end_ms 0 is a lie a caller cannot detect.
func TestSTTDurationMilliseconds(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		in     string
		want   int64
		wantOK bool
	}{
		{in: "1.500s", want: 1_500, wantOK: true},
		{in: "12s", want: 12_000, wantOK: true},
		{in: "0s", want: 0, wantOK: true},
		{in: "0.001s", want: 1, wantOK: true},
		{in: "  2.250s  ", want: 2_250, wantOK: true},
		{in: "3.7500000s", want: 3_750, wantOK: true},
		// Rounding, not truncation: 0.0005 s is half a millisecond.
		{in: "0.0005s", want: 1, wantOK: true},
		{in: "", wantOK: false},
		{in: "1500", wantOK: false},   // No suffix: this is the shape a bad port produces.
		{in: "1500ms", wantOK: false}, // Not the proto3 rendering.
		{in: "abcs", wantOK: false},
		{in: "NaNs", wantOK: false},
		{in: "Infs", wantOK: false},
	} {
		t.Run(fmt.Sprintf("%q", testCase.in), func(t *testing.T) {
			t.Parallel()
			got, ok := sttDurationMilliseconds(testCase.in)
			if ok != testCase.wantOK {
				t.Fatalf("ok = %v, want %v", ok, testCase.wantOK)
			}
			if ok && got != testCase.want {
				t.Fatalf("ms = %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestNewSTTValidatesItsBounds: a zero value must produce a usable adapter, and
// an explicitly nonsensical bound must fail loudly at construction rather than
// mid-session.
func TestNewSTTValidatesItsBounds(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatalf("zero config: %v", err)
	}
	if adapter.ID() != "google.stt.v1" {
		t.Fatalf("default adapter id = %q, want google.stt.v1", adapter.ID())
	}
	for _, testCase := range []struct {
		name   string
		config STTConfig
	}{
		{name: "negative event buffer", config: STTConfig{EventBuffer: -1}},
		{name: "negative response bound", config: STTConfig{MaxResponseBytes: -1}},
		{name: "negative timeout", config: STTConfig{RequestTimeout: -time.Second}},
		{name: "malformed allowed host", config: STTConfig{AllowedEndpointHosts: []string{"https://host/path"}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewSTT(testCase.config); err == nil {
				t.Fatal("construction succeeded, want rejection")
			}
		})
	}
}

// --- helpers -------------------------------------------------------------

func sttCredentialFor(source protocol.CredentialSource) string {
	if source == protocol.CredentialsBYOK {
		return "ya29.customer-owned-access-token"
	}
	return "ya29.control-plane-minted-token"
}

func sttTestConfig(server *httptest.Server) STTConfig {
	host, _, _ := strings.Cut(strings.TrimPrefix(server.URL, "http://"), ":")
	return STTConfig{
		HTTPClient:            server.Client(),
		AllowedEndpointHosts:  []string{host},
		AllowInsecureEndpoint: true,
		RequestTimeout:        5 * time.Second,
	}
}

func sttGoogleRequest(server *httptest.Server, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(server.URL)
	endpoint.Path = sttTestRecognizePath
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			PlanID: "plan_google_stt", SessionID: "sess_google_stt", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "google", Model: DefaultSTTModel, Adapter: STTAdapterID,
				Transport: protocol.TransportHTTP, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: sttCredentialFor(source), ExpiresAt: now.Add(time.Hour)},
			},
			Reservation:  protocol.Reservation{ID: "res_google_stt", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), RenewalURL: "https://control.speko.test/v1/sessions/sess_google_stt/lease-renewals", Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_google_stt", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 600}},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"},
			Signature:    "test",
		},
		Options: protocol.RequestOptions{Language: "hi-IN"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func openSTTStream(t *testing.T, server *httptest.Server, source protocol.CredentialSource, configure func(*STTConfig)) runtimepkg.ProviderStream {
	t.Helper()
	return openSTTStreamWith(t, server, source, configure, nil)
}

func openSTTStreamWith(t *testing.T, server *httptest.Server, source protocol.CredentialSource, configure func(*STTConfig), mutate func(*runtimepkg.AdapterRequest)) runtimepkg.ProviderStream {
	t.Helper()
	config := sttTestConfig(server)
	if configure != nil {
		configure(&config)
	}
	adapter, err := NewSTT(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	if adapter.ID() != STTAdapterID {
		t.Fatalf("adapter id = %q, want %q", adapter.ID(), STTAdapterID)
	}
	request := sttGoogleRequest(server, source)
	if mutate != nil {
		mutate(&request)
	}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return stream
}

func sttListen(t *testing.T, stream runtimepkg.ProviderStream, audio []byte) {
	t.Helper()
	if err := stream.WriteAudio(context.Background(), audio); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
}

func closeSTTStream(t *testing.T, stream runtimepkg.ProviderStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := stream.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, open := <-stream.Events(); open {
		t.Fatal("events must be closed after Close")
	}
}

// newRecognizeServer refuses any path but the documented recognize path, so a
// test that quietly starts hitting a different method fails instead of passing.
func newRecognizeServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sttTestRecognizePath {
			http.NotFound(w, r)
			return
		}
		handler(w, r)
	}))
}

func writeRecognizeResponse(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func sttSamplePCM(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index*11 + 5)
	}
	return payload
}

func collectSTTEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for len(collected) < want {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("provider events closed after %d events", len(collected))
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d events", len(collected), want)
		}
	}
	return collected
}

func awaitSTTProviderError(t *testing.T, events <-chan runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("events closed before a provider error arrived")
			}
			if event.Err == nil {
				continue
			}
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) {
				t.Fatalf("error %v is not a *runtime.ProviderError", event.Err)
			}
			return providerErr
		case <-timer.C:
			t.Fatal("timed out waiting for a provider error")
		}
	}
}

func awaitSTTSignal(t *testing.T, signal <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(reason)
	}
}

// sttExtensionKeys reports which namespaces an event actually carried, so a
// failed extension lookup says what was there instead of just "nil".
func sttExtensionKeys(extensions map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(extensions))
	for key := range extensions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sttEventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return types
}
