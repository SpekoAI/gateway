package google

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// STTAdapterID is the identifier a Google Cloud Speech-to-Text session plan
	// must name in PlanRoute.Adapter.
	STTAdapterID = "google.stt.v1"

	// sttExtensionID namespaces the raw vendor payload attached to canonical
	// events. On success it carries the verbatim SpeechRecognitionResult (or the
	// response metadata); on failure it carries Google's google.rpc.Status error
	// object, which is the payload that actually matters when debugging a 400.
	sttExtensionID = "speech.googleapis.com/v2"

	// DefaultSTTModel is the Chirp 3 model identifier.
	//
	// CONFIRMED FROM RAW SOURCE: the Chirp 3 model page's "Model identifiers"
	// table reads "Model: Chirp 3 | Model identifier: chirp_3", and every code
	// sample on that page passes model="chirp_3". Unlike Cloud TTS -- where the
	// voice name selects the model and the model field is a trap -- Speech V2
	// really does select the model through RecognitionConfig.model.
	DefaultSTTModel = "chirp_3"

	// sttOfficialAPIHost is the global Speech service endpoint, taken from the
	// v2 discovery document's rootUrl (https://speech.googleapis.com/).
	sttOfficialAPIHost = "speech.googleapis.com"

	// sttAPIVersion prefixes every method path in the v2 discovery document.
	sttAPIVersion = "v2"

	// sttImplicitRecognizer is the only recognizer id this adapter accepts. The
	// discovery document describes it as "The {recognizer} segment may be set to
	// `_` to use an empty implicit Recognizer". See sttEndpointPolicy.parse for
	// why a named recognizer is refused rather than supported.
	sttImplicitRecognizer = "_"

	// sttAudioEncodingLinear16 is the exact enum string from the v2 discovery
	// document, whose ExplicitDecodingConfig.encoding enum is
	// [AUDIO_ENCODING_UNSPECIFIED LINEAR16 MULAW ALAW AMR AMR_WB FLAC MP3
	// OGG_OPUS WEBM_OPUS MP4_AAC M4A_AAC MOV_AAC].
	//
	// LINEAR16 is the only member this gateway can reach. protocol.MediaFormat
	// admits exactly two encodings, pcm_s16le and opus; the opus members above
	// are CONTAINERS (Ogg, WebM), not the raw packets a realtime transport
	// carries, so an opus session is refused rather than mislabelled.
	sttAudioEncodingLinear16 = "LINEAR16"

	// sttMinSampleRateHertz / sttMaxSampleRateHertz are the documented bounds on
	// ExplicitDecodingConfig.sampleRateHertz: "Valid values are: 8000-48000, and
	// 16000 is optimal". protocol.MediaFormat allows up to 192000, so the upper
	// bound is a real constraint this adapter has to add.
	sttMinSampleRateHertz = 8_000
	sttMaxSampleRateHertz = 48_000

	// sttMaxAudioChannelCount is the documented ceiling on
	// ExplicitDecodingConfig.audioChannelCount ("The maximum allowed value is 8").
	sttMaxAudioChannelCount = 8

	// sttMaxRequestBytes is the published content limit: "There is a limit of 10
	// MB on all single requests sent to the API using local files. In the case of
	// the Recognize [...] methods, this limit applies to the size of the request
	// sent."
	sttMaxRequestBytes = 10 << 20

	// sttMaxAudioBytes is the largest raw PCM buffer whose request still fits
	// under sttMaxRequestBytes. The limit is on the REQUEST, and RecognizeRequest
	// .content is base64 ("As with all bytes fields, proto buffers use a pure
	// binary representation, whereas JSON representations use base64"), so raw
	// audio inflates by 4/3 on the wire. Counting raw bytes against 10 MB would
	// let a 9 MB buffer build a 12 MB request and fail upstream. The subtracted
	// slack covers the JSON envelope around the audio.
	sttMaxAudioBytes = sttMaxRequestBytes/4*3 - (16 << 10)

	// sttMaxSyncAudioSeconds is the published audio-length limit for the
	// synchronous method: content limits table, "Audio Length | Synchronous
	// Requests | ~1 Minute", with the footnote "Audio longer than ~1 minute must
	// use the uri field to reference an audio file in Cloud Storage". This
	// adapter never sends `uri` (that would mean handing Google a bucket), so one
	// minute is a hard local ceiling.
	sttMaxSyncAudioSeconds = 60

	// sttBytesPerSample is s16le: two bytes per sample per channel.
	sttBytesPerSample = 2

	// sttMaxErrorBodyBytes bounds the google.rpc.Status body read from a failed
	// response.
	sttMaxErrorBodyBytes = 8 << 10

	defaultSTTEventBuffer      = 32
	defaultSTTMaxResponseBytes = 8 << 20
	// defaultSTTRequestTimeout has to cover recognition of a full minute of
	// audio, not just a round trip, because Recognize returns only "after all
	// audio has been sent and processed".
	defaultSTTRequestTimeout = 120 * time.Second

	// sttGoogleAPIKeyPrefix is the observed prefix of a Google Cloud API key. An
	// API key sent as "Authorization: Bearer" is rejected with a bare 401 that
	// reads identically to an expired OAuth token, so it is caught locally.
	sttGoogleAPIKeyPrefix = "AIza"
)

// sttRegionalAPIHosts are the data-residency endpoints Chirp 3 is reachable
// from. The Chirp 3 page's "Regional availability" table lists exactly two GA
// zones -- "us (multi-region)" and "eu (multi-region)" -- and its streaming
// sample adds the comment "Other valid regions include 'eu' or specific
// regions like 'asia-southeast1'". The host template comes from the same
// sample: api_endpoint = f"{region}-speech.googleapis.com".
//
// This matters beyond data residency: our STT board found Chirp 3 takes rank 1
// on hi/ta/te only when the request is served from `eu`, so which host a plan
// names is a quality decision, not just a latency one.
var sttRegionalAPIHosts = []string{
	"us-speech.googleapis.com",
	"eu-speech.googleapis.com",
	"asia-southeast1-speech.googleapis.com",
}

// sttRecognizePathPattern is transcribed from the v2 discovery document: the
// recognize method's flatPath is
// "v2/projects/{projectsId}/locations/{locationsId}/recognizers/{recognizersId}:recognize"
// and its `recognizer` path parameter carries the pattern
// "^projects/[^/]+/locations/[^/]+/recognizers/[^/]+$".
var sttRecognizePathPattern = regexp.MustCompile(`^/` + sttAPIVersion + `/projects/([^/]+)/locations/([^/]+)/recognizers/([^/:]+):recognize$`)

// sttRegionalHostPattern extracts the location a regional host serves, so the
// recognizer path's location can be checked against it. The global host
// "speech.googleapis.com" deliberately does not match: it has no location to
// contradict.
var sttRegionalHostPattern = regexp.MustCompile(`^([a-z0-9]+(?:-[a-z0-9]+)*)-speech\.googleapis\.com$`)

// sttLanguageTagPattern accepts the three tag shapes the Chirp 3
// supported-language table actually publishes: language only ("sw"), language
// plus region ("hi-IN", "es-419", "ast-ES", "nso-ZA"), and language plus a
// four-letter script subtag plus region ("cmn-Hans-CN", "yue-Hant-HK",
// "pa-Guru-IN"). A pattern without the script group -- the one Cloud TTS gets
// away with in this package -- would reject every Chinese and Punjabi tag.
var sttLanguageTagPattern = regexp.MustCompile(`^([A-Za-z]{2,3})(?:-([A-Za-z]{4}))?(?:-([A-Za-z]{2}|[0-9]{3}))?$`)

// sttLanguageRegionDefaults expands a bare primary subtag to the
// region-qualified tag Chirp 3 actually keys on.
//
// This exists because Speech V2 rejects bare codes outright. The platform
// adapter records the verified failure: `en` yields `3 INVALID_ARGUMENT: The
// language "en" is not supported by the model "chirp_3" in the location named
// "us"`. Every caller-facing surface upstream of this gateway carries bare
// slugs, so without expansion the common case is a 400.
//
// The first block is ported verbatim from the platform adapter's
// GOOGLE_STT_REGION_DEFAULTS. The second block is added here: the Chirp 3
// supported-language table publishes exactly one variant of each, so expanding
// them is a table lookup rather than a guess -- and they are the reason this
// adapter exists, since our STT board ranks Google first on hi/ta/te.
var sttLanguageRegionDefaults = map[string]string{
	"en": "en-US",
	"es": "es-US",
	"de": "de-DE",
	"ru": "ru-RU",
	"uz": "uz-UZ",
	"kk": "kk-KZ",
	"th": "th-TH",
	"id": "id-ID",
	"ms": "ms-MY",
	"vi": "vi-VN",

	"hi": "hi-IN",
	"ta": "ta-IN",
	"te": "te-IN",
}

// sttBareLanguages are the primary subtags Chirp 3 publishes WITHOUT a region.
// The supported-language table lists "Swahili (Kenya) sw-KE" and, separately,
// a bare "Swahili sw" -- so a blanket "reject anything region-less" rule would
// refuse a tag the vendor documents.
var sttBareLanguages = map[string]struct{}{
	"sw": {},
}

// STTConfig controls local transport bounds. Provider identity, model,
// language, endpoint, and the access token all come from a verified session
// plan.
type STTConfig struct {
	AdapterID  string
	HTTPClient *http.Client
	// EventBuffer bounds the canonical event channel.
	EventBuffer int
	// MaxResponseBytes bounds a single recognition response body.
	MaxResponseBytes int64
	// RequestTimeout bounds one recognition POST.
	RequestTimeout time.Duration
	// QuotaProject populates the x-goog-user-project header.
	//
	// It is deliberately NOT inferred from the recognizer path even though the
	// project is right there and Google's own Chirp sample sets
	// quota_project_id = project_id. Sending the header makes the request
	// require serviceusage.services.use on that project, which a token that can
	// otherwise transcribe perfectly well may not hold -- so inferring it would
	// turn working plans into 403s. Deployment opts in.
	QuotaProject          string
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements Google Cloud Speech-to-Text V2 over its REST surface.
//
// # Why this adapter is not streaming
//
// Speech V2 does expose Speech.StreamingRecognize, and the Chirp 3 page lists
// it as supported ("V2 | Speech.StreamingRecognize (good for streaming and
// real-time audio) | Supported"). It is bidirectional-streaming gRPC with no
// HTTP/JSON binding, exactly like TextToSpeech.StreamingSynthesize before it.
//
// This was verified rather than assumed, twice:
//
//   - The live v2 discovery document (revision 20260708) publishes 26 methods
//     across projects.locations.{config,customClasses,operations,phraseSets,
//     recognizers}. Exactly one performs recognition from inline audio,
//     recognizers.recognize, and exactly one does it from Cloud Storage,
//     recognizers.batchRecognize. There is no streaming method. The
//     StreamingRecognitionResult *schema* is present -- the message type leaks
//     into the document through proto references -- but no method returns it,
//     which is precisely the shape of an RPC that exists only over gRPC. The v1
//     document has neither the method nor the schema.
//   - Probing the live service agrees: POST :recognize answers 403
//     PERMISSION_DENIED (the path routed, the credential did not), while POST
//     :streamingRecognize answers 404, the same status a nonexistent host
//     returns.
//
// Bidirectional streams cannot be transcoded to REST at all, so there is no
// `alt=sse` escape hatch either. Reaching StreamingRecognize would require gRPC
// framing plus generated protobuf message types -- new Go module dependencies
// this build does not take.
//
// # What is incremental anyway
//
// The adapter buffers input audio and performs one POST per CommitAudio. On the
// way back it does NOT buffer: the response body is walked with a streaming
// json.Decoder and each SpeechRecognitionResult is published as it is decoded,
// so transcripts begin flowing before the body is complete. Framing is local,
// not wire-level.
//
// # What that costs, stated plainly
//
// Every transcript this adapter emits is protocol.EventTranscriptFinal. It
// never emits EventTranscriptDelta, because on this transport there is nothing
// to derive one from: interim results live on StreamingRecognitionResult, which
// carries isFinal and stability, and the unary method returns
// SpeechRecognitionResult, which has neither. Synthesising deltas by splitting
// a completed result would be a fabricated partial -- a caller would see the
// timing of a streaming recogniser without the latency benefit that justifies
// it. A realtime turn also cannot exceed the documented one-minute synchronous
// ceiling. Both are enforced, not papered over.
type STTAdapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	maxResponseBytes int64
	requestTimeout   time.Duration
	quotaProject     string
	endpointPolicy   sttEndpointPolicy
}

// NewSTT creates a bounded Google Cloud Speech-to-Text adapter.
func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultSTTEventBuffer
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultSTTMaxResponseBytes
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultSTTRequestTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("google stt event buffer must be positive")
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("google stt maximum response bytes must be positive")
	}
	if config.RequestTimeout < 0 {
		return nil, errors.New("google stt request timeout must not be negative")
	}
	policy, err := newSTTEndpointPolicy(append([]string{sttOfficialAPIHost}, sttRegionalAPIHosts...), config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id:               config.AdapterID,
		httpClient:       config.HTTPClient,
		eventBuffer:      config.EventBuffer,
		maxResponseBytes: config.MaxResponseBytes,
		requestTimeout:   config.RequestTimeout,
		quotaProject:     strings.TrimSpace(config.QuotaProject),
		endpointPolicy:   policy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open validates the plan and prepares a recognition stream. No network call is
// made here: the unary REST method has no session to establish, so the first
// request happens on CommitAudio.
func (a *STTAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("google stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "google" {
		return nil, fmt.Errorf("google stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("google stt requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("google stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("google stt media: %w", err)
	}
	if err := sttValidateMedia(*request.Media); err != nil {
		return nil, err
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("google stt requires a concrete model in the session plan")
	}
	languageCode, err := sttResolveLanguageCode(request.Options.Language)
	if err != nil {
		return nil, err
	}
	target, err := a.endpointPolicy.parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	headers, err := sttAuthorizationHeaders(request, a.quotaProject)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	return &sttStream{
		ctx:               streamCtx,
		cancel:            cancel,
		events:            make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		httpClient:        sttRequestHTTPClient(a.httpClient),
		endpoint:          target.URL,
		recognizer:        target.Recognizer(),
		location:          target.Location,
		headers:           headers,
		model:             model,
		languageCode:      languageCode,
		sampleRateHertz:   request.Media.SampleRateHz,
		audioChannelCount: request.Media.Channels,
		maxAudioBytes:     sttMaxBufferedAudioBytes(*request.Media),
		maxResponseBytes:  a.maxResponseBytes,
		requestTimeout:    a.requestTimeout,
	}, nil
}

func sttRequestHTTPClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// sttValidateMedia enforces the ExplicitDecodingConfig bounds that
// protocol.MediaFormat is looser than.
func sttValidateMedia(media protocol.MediaFormat) error {
	if media.Encoding != "pcm_s16le" {
		// OGG_OPUS and WEBM_OPUS are containers; a realtime transport carries
		// bare opus packets, which no ExplicitDecodingConfig.encoding member
		// describes. Refusing beats shipping an encoding label that lies.
		return fmt.Errorf("google stt consumes pcm_s16le audio, got %s", media.Encoding)
	}
	if media.SampleRateHz < sttMinSampleRateHertz || media.SampleRateHz > sttMaxSampleRateHertz {
		return fmt.Errorf("google stt sample rate must be between %d and %d Hz, got %d", sttMinSampleRateHertz, sttMaxSampleRateHertz, media.SampleRateHz)
	}
	if media.Channels < 1 || media.Channels > sttMaxAudioChannelCount {
		return fmt.Errorf("google stt supports at most %d audio channels, got %d", sttMaxAudioChannelCount, media.Channels)
	}
	return nil
}

// sttMaxBufferedAudioBytes is the smaller of the two published ceilings: the
// 10 MB request size and the ~1 minute synchronous audio length. At the
// documented-optimal 16 kHz mono the duration limit binds first (1.92 MB), so
// checking only the byte limit would let a caller queue half an hour of 8 kHz
// audio and be told nothing until Google refused it.
func sttMaxBufferedAudioBytes(media protocol.MediaFormat) int {
	perSecond := media.SampleRateHz * media.Channels * sttBytesPerSample
	byDuration := perSecond * sttMaxSyncAudioSeconds
	return min(byDuration, sttMaxAudioBytes)
}

// sttAuthorizationHeaders builds the request headers for both credential
// sources.
//
// Managed and BYOK deliberately share one code path because Google offers one
// documented header mechanism here: "Authorization: Bearer <access-token>", and
// the recognize method's only OAuth scope is
// https://www.googleapis.com/auth/cloud-platform. The two sources differ only
// in who minted the token, never in how it travels. There is no session-scoped
// Google credential to split them with: access tokens last one hour by default
// (twelve only under constraints/iam.allowServiceAccountCredentialLifetimeExtension,
// and Domain-Wide Delegation is capped at one), and Credential Access
// Boundaries are Cloud Storage only.
//
// The one thing this adapter will not do is put the secret in the URL.
func sttAuthorizationHeaders(request runtimepkg.AdapterRequest, quotaProject string) (http.Header, error) {
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer {
		return nil, errors.New("google stt requires a bearer credential")
	}
	token := strings.TrimSpace(credential.Value)
	if token == "" {
		return nil, errors.New("google stt requires a non-empty bearer credential")
	}
	if strings.HasPrefix(token, sttGoogleAPIKeyPrefix) {
		return nil, errors.New("google stt credential looks like an API key; this adapter sends OAuth access tokens in the Authorization header")
	}
	switch request.Plan.Execution.CredentialSource {
	case protocol.CredentialsManaged, protocol.CredentialsBYOK:
	default:
		return nil, fmt.Errorf("google stt cannot use credential source %q", request.Plan.Execution.CredentialSource)
	}
	headers := make(http.Header, 4)
	headers.Set("Authorization", "Bearer "+token)
	headers.Set("Content-Type", "application/json; charset=utf-8")
	headers.Set("Accept", "application/json")
	if quotaProject != "" {
		headers.Set("X-Goog-User-Project", quotaProject)
	}
	return headers, nil
}

// sttResolveLanguageCode produces the BCP-47 tag sent in
// RecognitionConfig.languageCodes.
//
// The tag is region-qualified on purpose. Speech V2 does normalize case for you
// ("Language tags are normalized to BCP-47 before they are used eg 'en-us'
// becomes 'en-US'") but it does not invent a region, and Chirp 3 keys its
// language table on the full tag: hi-IN, ta-IN, te-IN. A bare code is a 400.
//
// Unlike the platform adapter this refuses to default to en-US when no language
// is supplied. Defaulting means Hindi audio comes back as confident English
// nonsense -- a wrong answer that looks like a right one -- and nothing
// downstream can tell the difference.
func sttResolveLanguageCode(requested string) (string, error) {
	tag := strings.ReplaceAll(strings.TrimSpace(requested), "_", "-")
	if tag == "" {
		return "", errors.New("google stt requires a language code, for example hi-IN")
	}
	match := sttLanguageTagPattern.FindStringSubmatch(tag)
	if match == nil {
		return "", fmt.Errorf("google stt language %q is not a BCP-47 language tag", requested)
	}
	language, script, region := strings.ToLower(match[1]), match[2], match[3]

	// Tagalog is special-cased even when it already carries a region: Chirp
	// publishes "Filipino (Philippines) fil-PH", bare `fil` is off-spec, and so
	// is the ISO 639-1 twin `tl`. Normalise the whole family to one tag.
	if language == "fil" || language == "tl" {
		return "fil-PH", nil
	}
	if region == "" {
		if expanded, ok := sttLanguageRegionDefaults[language]; ok && script == "" {
			return expanded, nil
		}
		if _, ok := sttBareLanguages[language]; ok && script == "" {
			return language, nil
		}
		return "", fmt.Errorf("google stt language %q needs a region subtag, for example hi-IN rather than hi", requested)
	}
	canonical := language
	if script != "" {
		canonical += "-" + sttCanonicalScript(script)
	}
	return canonical + "-" + sttCanonicalRegion(region), nil
}

// sttCanonicalScript title-cases an ISO 15924 subtag ("hans" -> "Hans").
func sttCanonicalScript(script string) string {
	return strings.ToUpper(script[:1]) + strings.ToLower(script[1:])
}

// sttCanonicalRegion uppercases an alphabetic region and leaves a numeric UN
// M.49 region ("419") alone.
func sttCanonicalRegion(region string) string {
	if region == "" {
		return ""
	}
	if region[0] >= '0' && region[0] <= '9' {
		return region
	}
	return strings.ToUpper(region)
}

// sttEndpointPolicy is the HTTP counterpart of upstream.WebSocketPolicy. The
// shared helper in internal/upstream only validates wss endpoints and this
// package must not edit shared code, so the same rules are restated for https.
// Consolidating this, the identical policy in tts.go, and internal/upstream
// belongs in internal/upstream.
type sttEndpointPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

// sttRecognizeTarget is a validated recognize URL decomposed into the resource
// coordinates the path encodes.
type sttRecognizeTarget struct {
	URL        string
	Project    string
	Location   string
	recognizer string
}

// Recognizer returns the fully qualified recognizer resource name the path
// addresses. It is reported on events for debugging; the wire request never
// carries it in the body (see sttRecognizeRequest).
func (t sttRecognizeTarget) Recognizer() string {
	return fmt.Sprintf("projects/%s/locations/%s/recognizers/%s", t.Project, t.Location, t.recognizer)
}

func newSTTEndpointPolicy(officialHosts, additionalHosts []string, allowInsecure bool) (sttEndpointPolicy, error) {
	hosts := make(map[string]struct{}, len(officialHosts)+len(additionalHosts))
	for _, host := range append(append([]string{}, officialHosts...), additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return sttEndpointPolicy{}, errors.New("google stt allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return sttEndpointPolicy{hosts: hosts, allowInsecure: allowInsecure}, nil
}

// parse validates scheme, host, port, userinfo, query, and path before a
// customer-owned access token is attached, then decomposes the recognizer path.
func (p sttEndpointPolicy) parse(raw string) (sttRecognizeTarget, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return sttRecognizeTarget{}, errors.New("google stt endpoint must be a clean absolute https URL")
	}
	if endpoint.Scheme != "https" && !(p.allowInsecure && endpoint.Scheme == "http") {
		return sttRecognizeTarget{}, errors.New("google stt endpoint must use https")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return sttRecognizeTarget{}, errors.New("google stt endpoint uses a non-standard port")
	}
	host := strings.ToLower(endpoint.Hostname())
	if _, ok := p.hosts[host]; !ok {
		return sttRecognizeTarget{}, errors.New("google stt endpoint host is not allowed")
	}
	match := sttRecognizePathPattern.FindStringSubmatch(endpoint.Path)
	if match == nil {
		return sttRecognizeTarget{}, fmt.Errorf("google stt endpoint path must be /%s/projects/{project}/locations/{location}/recognizers/_:recognize, got %q", sttAPIVersion, endpoint.Path)
	}
	project, location, recognizer := match[1], match[2], match[3]
	if recognizer != sttImplicitRecognizer {
		// A named recognizer carries its own default_recognition_config, and the
		// discovery document is explicit about what happens then: "If no mask is
		// provided, all non-default valued fields in config override the values in
		// the recognizer". Non-default is the trap -- a stored model or language
		// would survive wherever this request happens to leave a field at its zero
		// value, so the plan's model would apply sometimes. Supporting this means
		// deciding on configMask, which cannot be tested against the real service
		// from here, so it is refused instead of guessed at.
		return sttRecognizeTarget{}, fmt.Errorf("google stt requires the implicit recognizer %q so the session plan alone selects the model, got %q", sttImplicitRecognizer, recognizer)
	}
	// The location is in the path AND in the host, and Google does not reconcile
	// them for you: pointing us-speech.googleapis.com at a locations/eu path is
	// an INVALID_ARGUMENT from the service. Worse is the near-miss -- a plan
	// built from a sibling Vertex product's region (us-central1, global) reaches
	// a host that exists but serves no Chirp 3, and the failure reads like a
	// model problem rather than a routing one.
	if hostMatch := sttRegionalHostPattern.FindStringSubmatch(host); hostMatch != nil && hostMatch[1] != location {
		return sttRecognizeTarget{}, fmt.Errorf("google stt endpoint host serves location %q but the recognizer path names %q", hostMatch[1], location)
	}
	return sttRecognizeTarget{URL: endpoint.String(), Project: project, Location: location, recognizer: recognizer}, nil
}

// sttRecognizeRequest is the documented RecognizeRequest body.
//
// It has exactly two members here, and the absent one is the point: `recognizer`
// is NOT a body field on the REST surface. The discovery document gives
// RecognizeRequest the properties config, configMask, content, and uri, and
// binds the recognizer as a PATH parameter. The gRPC message and every SDK
// sample do carry `recognizer` inside the request object, so porting one of
// those literally produces a body with a field the JSON surface does not know.
//
// `uri` is likewise absent by choice: "Either `content` or `uri` must be
// supplied. Supplying both or neither returns INVALID_ARGUMENT", and this
// gateway sends audio it holds rather than handing Google a bucket.
type sttRecognizeRequest struct {
	Config sttRecognitionConfig `json:"config"`
	// Content is base64 in JSON. Go's encoding/json emits []byte as base64
	// automatically, but the field is a string so the encoding is visible at the
	// call site rather than an implicit property of the type.
	Content string `json:"content"`
}

type sttRecognitionConfig struct {
	// ExplicitDecodingConfig, not autoDecodingConfig: "Explicitly specified
	// decoding parameters. Required if using headerless PCM audio (linear16,
	// mulaw, alaw)." Everything this gateway carries is headerless PCM, so
	// auto-detection has no container to detect and the request would fail.
	ExplicitDecodingConfig sttExplicitDecodingConfig `json:"explicitDecodingConfig"`
	// LanguageCodes is a repeated field even for one language.
	LanguageCodes []string               `json:"languageCodes"`
	Model         string                 `json:"model"`
	Features      sttRecognitionFeatures `json:"features"`
}

type sttExplicitDecodingConfig struct {
	Encoding          string `json:"encoding"`
	SampleRateHertz   int    `json:"sampleRateHertz"`
	AudioChannelCount int    `json:"audioChannelCount"`
}

type sttRecognitionFeatures struct {
	// Ported from the platform adapter's file-transcribe path, which sets
	// enableAutomaticPunctuation on the same unary method.
	EnableAutomaticPunctuation bool `json:"enableAutomaticPunctuation"`
}

// sttAPIError is google.rpc.Status as rendered by the JSON surface.
type sttAPIError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

// sttRecognitionResult is SpeechRecognitionResult. Note what is missing
// relative to StreamingRecognitionResult: no isFinal, no stability. That
// absence is why this adapter emits only transcript.final.
type sttRecognitionResult struct {
	Alternatives []struct {
		Transcript string  `json:"transcript"`
		Confidence float64 `json:"confidence"`
	} `json:"alternatives"`
	LanguageCode string `json:"languageCode"`
	ChannelTag   int    `json:"channelTag"`
	// ResultEndOffset is a google-duration string ("1.500s").
	ResultEndOffset string `json:"resultEndOffset"`
}

// sttResponseMetadata is RecognitionResponseMetadata.
type sttResponseMetadata struct {
	RequestID           string `json:"requestId"`
	TotalBilledDuration string `json:"totalBilledDuration"`
}

type sttStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	httpClient        *http.Client
	endpoint          string
	recognizer        string
	location          string
	headers           http.Header
	model             string
	languageCode      string
	sampleRateHertz   int
	audioChannelCount int
	maxAudioBytes     int
	maxResponseBytes  int64
	requestTimeout    time.Duration

	inflight     sync.WaitGroup
	shutdownOnce sync.Once
	closeErr     error

	stateMu          sync.Mutex
	buffer           bytes.Buffer
	closed           bool
	requestCancel    context.CancelFunc
	requestCancelled bool
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// AppendText and CommitText are output-text operations; an STT session has none.
func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// WriteAudio buffers a frame. The unary method takes one whole utterance, so
// nothing reaches Google until CommitAudio.
func (s *sttStream) WriteAudio(_ context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("google stt audio is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.requestCancel != nil {
		return errors.New("google stt previous recognition has not completed")
	}
	if s.buffer.Len()+len(audio) > s.maxAudioBytes {
		return &runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        fmt.Sprintf("Google Cloud Speech-to-Text accepts at most %d seconds of audio per synchronous request", sttMaxSyncAudioSeconds),
			Retryable:      false,
			ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.buffer.Write(audio)
	return nil
}

// CommitAudio posts the buffered utterance and streams the response back as
// canonical events. It returns as soon as the request is accepted locally;
// transcript.final, usage.observed, and speech.ended arrive on Events, and a
// recognition failure arrives as a terminal ProviderEvent.Err, matching how the
// WebSocket adapters surface upstream faults.
func (s *sttStream) CommitAudio(context.Context) error {
	audio, recognitionID, requestCtx, err := s.startRequest()
	if err != nil {
		return err
	}
	go func() {
		defer s.inflight.Done()
		terminal := s.recognize(requestCtx, audio, recognitionID)
		// Release the in-flight slot BEFORE the terminal event becomes visible. A
		// consumer that reacts to speech.ended by starting the next utterance must
		// not lose the race against this goroutine's own cleanup.
		s.finishRequest()
		if terminal != nil {
			_ = s.emit(*terminal)
		}
	}()
	return nil
}

// startRequest claims the single in-flight slot. The WaitGroup counter is
// incremented under stateMu, and shutdown sets closed under the same lock
// before waiting, so a request can never be registered after the event channel
// has been closed.
func (s *sttStream) startRequest() ([]byte, string, context.Context, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return nil, "", nil, runtimepkg.ErrSessionClosed
	}
	if s.requestCancel != nil {
		return nil, "", nil, errors.New("google stt previous recognition has not completed")
	}
	if s.buffer.Len() == 0 {
		return nil, "", nil, errors.New("google stt has no buffered audio to recognize")
	}
	recognitionID, err := newSTTRecognitionID()
	if err != nil {
		return nil, "", nil, err
	}
	audio := append([]byte(nil), s.buffer.Bytes()...)
	s.buffer.Reset()

	requestCtx, cancel := context.WithCancel(s.ctx)
	if s.requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(s.ctx, s.requestTimeout)
	}
	s.requestCancel = cancel
	s.requestCancelled = false
	s.inflight.Add(1)
	return audio, recognitionID, requestCtx, nil
}

func (s *sttStream) finishRequest() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestCancel != nil {
		s.requestCancel()
		s.requestCancel = nil
	}
	s.requestCancelled = false
}

func (s *sttStream) wasCancelled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.requestCancelled
}

// Cancel abandons the in-flight recognition and drops anything still buffered.
// It is not terminal: the session stays usable for another utterance, which is
// what a barge-in needs.
func (s *sttStream) Cancel(context.Context) error {
	s.stateMu.Lock()
	cancel := s.requestCancel
	buffered := s.buffer.Len() > 0
	s.buffer.Reset()
	if cancel != nil {
		s.requestCancelled = true
	}
	s.stateMu.Unlock()

	if cancel == nil && !buffered {
		return runtimepkg.ErrSessionClosed
	}
	if cancel != nil {
		cancel()
	}
	return nil
}

// Close waits for an in-flight recognition so a caller that closes immediately
// after CommitAudio still receives its transcript, then closes the event
// channel. A ctx deadline bounds that wait.
func (s *sttStream) Close(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.markClosed()
		drained := make(chan struct{})
		go func() { s.inflight.Wait(); close(drained) }()
		select {
		case <-drained:
		case <-ctx.Done():
			s.closeErr = ctx.Err()
		}
		s.cancel()
		s.inflight.Wait()
		close(s.events)
	})
	return s.closeErr
}

// Abort tears the session down immediately after a terminal runtime failure:
// the in-flight HTTP request is cancelled through its context rather than
// drained.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

func (s *sttStream) abort() error {
	s.shutdownOnce.Do(func() {
		s.markClosed()
		s.cancel()
		s.inflight.Wait()
		close(s.events)
	})
	return s.closeErr
}

func (s *sttStream) markClosed() {
	s.stateMu.Lock()
	s.closed = true
	s.stateMu.Unlock()
}

// recognize performs the single POST and returns the recognition's TERMINAL
// event (speech.ended, a cancellation warning, or a ProviderEvent carrying
// Err). Intermediate events are emitted as they are produced. A nil return
// means the stream context ended and nothing more should be published.
func (s *sttStream) recognize(ctx context.Context, audio []byte, recognitionID string) *runtimepkg.ProviderEvent {
	body, err := json.Marshal(sttRecognizeRequest{
		Config: sttRecognitionConfig{
			ExplicitDecodingConfig: sttExplicitDecodingConfig{
				Encoding:          sttAudioEncodingLinear16,
				SampleRateHertz:   s.sampleRateHertz,
				AudioChannelCount: s.audioChannelCount,
			},
			LanguageCodes: []string{s.languageCode},
			Model:         s.model,
			Features:      sttRecognitionFeatures{EnableAutomaticPunctuation: true},
		},
		Content: base64.StdEncoding.EncodeToString(audio),
	})
	if err != nil {
		return sttErrorEvent(&runtimepkg.ProviderError{Code: "invalid_request", Message: "Google STT request could not be encoded", Cause: err})
	}
	if len(body) > sttMaxRequestBytes {
		// The per-frame check in WriteAudio bounds raw audio; this bounds what the
		// wire actually carries, which is the thing the 10 MB limit measures.
		return sttErrorEvent(&runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        "Google Cloud Speech-to-Text accepts at most 10 MB per synchronous request",
			Retryable:      false,
			ProviderStatus: http.StatusRequestEntityTooLarge,
		})
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return sttErrorEvent(&runtimepkg.ProviderError{Code: "invalid_request", Message: "Google STT request could not be built", Cause: err})
	}
	httpRequest.Header = s.headers.Clone()

	response, err := s.httpClient.Do(httpRequest)
	if err != nil {
		if s.wasCancelled() {
			return sttCancelledEvent(recognitionID)
		}
		return sttErrorEvent(&runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Google STT request could not be delivered",
			Retryable: true,
			Cause:     err,
		})
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		return sttErrorEvent(s.statusError(response))
	}
	return s.streamTranscripts(ctx, response, recognitionID)
}

// streamTranscripts walks the response body with a streaming decoder, emitting
// each SpeechRecognitionResult as it is decoded rather than buffering the whole
// RecognizeResponse first. Member order is not assumed: proto3 JSON makes no
// ordering promise, so `metadata` is handled wherever it lands and any
// undocumented member is skipped rather than treated as a parse failure.
func (s *sttStream) streamTranscripts(ctx context.Context, response *http.Response, recognitionID string) *runtimepkg.ProviderEvent {
	decoder := json.NewDecoder(io.LimitReader(response.Body, s.maxResponseBytes+1))
	openBrace, err := decoder.Token()
	if err != nil {
		return sttErrorEvent(s.readError(ctx, recognitionID, err))
	}
	if delim, ok := openBrace.(json.Delim); !ok || delim != '{' {
		return sttErrorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google STT response is not a JSON object", Retryable: true})
	}

	var (
		results     int
		lastEndMS   int64
		haveEndMS   bool
		requestID   string
		usageEmited bool
	)
	for decoder.More() {
		keyToken, keyErr := decoder.Token()
		if keyErr != nil {
			return sttErrorEvent(s.readError(ctx, recognitionID, keyErr))
		}
		key, ok := keyToken.(string)
		if !ok {
			return sttErrorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google STT response has a non-string member name", Retryable: true})
		}
		switch key {
		case "results":
			arrayStart, arrayErr := decoder.Token()
			if arrayErr != nil {
				return sttErrorEvent(s.readError(ctx, recognitionID, arrayErr))
			}
			if delim, isDelim := arrayStart.(json.Delim); !isDelim || delim != '[' {
				return sttErrorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google STT results member is not an array", Retryable: true})
			}
			for decoder.More() {
				var raw json.RawMessage
				if decodeErr := decoder.Decode(&raw); decodeErr != nil {
					return sttErrorEvent(s.readError(ctx, recognitionID, decodeErr))
				}
				var result sttRecognitionResult
				if unmarshalErr := json.Unmarshal(raw, &result); unmarshalErr != nil {
					return sttErrorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google STT returned a malformed recognition result", Retryable: true, Cause: unmarshalErr})
				}
				if endMS, okOffset := sttDurationMilliseconds(result.ResultEndOffset); okOffset {
					lastEndMS, haveEndMS = endMS, true
				}
				if len(result.Alternatives) == 0 || strings.TrimSpace(result.Alternatives[0].Transcript) == "" {
					// An empty alternative is not a transcript. Publishing it would
					// give a caller an empty final turn indistinguishable from silence.
					continue
				}
				results++
				// EventTranscriptFinal, always. See the STTAdapter doc comment: the
				// unary result type has no isFinal to map from, so there is no
				// honest delta to emit on this transport.
				if emitErr := s.emit(runtimepkg.ProviderEvent{
					Type:       protocol.EventTranscriptFinal,
					Data:       s.transcriptData(recognitionID, result),
					Extensions: sttExtension(raw),
				}); emitErr != nil {
					return nil
				}
			}
			if _, arrayEndErr := decoder.Token(); arrayEndErr != nil {
				return sttErrorEvent(s.readError(ctx, recognitionID, arrayEndErr))
			}
		case "metadata":
			var raw json.RawMessage
			if decodeErr := decoder.Decode(&raw); decodeErr != nil {
				return sttErrorEvent(s.readError(ctx, recognitionID, decodeErr))
			}
			var metadata sttResponseMetadata
			if unmarshalErr := json.Unmarshal(raw, &metadata); unmarshalErr != nil {
				return sttErrorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google STT returned malformed response metadata", Retryable: true, Cause: unmarshalErr})
			}
			requestID = strings.TrimSpace(metadata.RequestID)
			if requestID != "" {
				usageEmited = true
				if emitErr := s.emit(runtimepkg.ProviderEvent{
					Type:       protocol.EventUsageObserved,
					Data:       sttUsageData(requestID, metadata.TotalBilledDuration),
					Extensions: sttExtension(raw),
				}); emitErr != nil {
					return nil
				}
			}
		default:
			var skipped json.RawMessage
			if decodeErr := decoder.Decode(&skipped); decodeErr != nil {
				return sttErrorEvent(s.readError(ctx, recognitionID, decodeErr))
			}
		}
	}
	if _, closeErr := decoder.Token(); closeErr != nil {
		return sttErrorEvent(s.readError(ctx, recognitionID, closeErr))
	}

	// RecognitionResponseMetadata.requestId is "auto-generated by the API", but
	// it is not contractual on every deployment, so fall back to whichever
	// correlation header the Google frontend attached rather than losing the
	// metering event entirely.
	if !usageEmited {
		if headerID := sttProviderRequestID(response.Header); headerID != "" {
			if emitErr := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: sttUsageData(headerID, "")}); emitErr != nil {
				return nil
			}
		}
	}

	// speech.ended is the terminal marker for one :recognize call. There is no
	// matching speech.started: nothing in a RecognizeResponse reports speech
	// onset, and emitting one anyway would be a timestamp this adapter invented.
	//
	// Zero results is a legitimate outcome (silence), not a failure, so it is
	// reported as an ended turn carrying result_count 0 rather than an error.
	return &runtimepkg.ProviderEvent{
		Type: protocol.EventSpeechEnded,
		Data: sttSpeechEndedData(recognitionID, results, lastEndMS, haveEndMS),
	}
}

// readError distinguishes a caller-initiated cancel from a genuine transport
// failure while the response body is being consumed.
func (s *sttStream) readError(ctx context.Context, _ string, cause error) *runtimepkg.ProviderError {
	if ctx.Err() != nil && s.wasCancelled() {
		return nil
	}
	return &runtimepkg.ProviderError{
		Code:      "provider_unavailable",
		Message:   "Google STT response could not be read",
		Retryable: true,
		Cause:     cause,
	}
}

func (s *sttStream) statusError(response *http.Response) *runtimepkg.ProviderError {
	raw, _ := io.ReadAll(io.LimitReader(response.Body, sttMaxErrorBodyBytes))
	message := "Google STT rejected the recognition request"
	var payload sttAPIError
	if err := json.Unmarshal(raw, &payload); err == nil && strings.TrimSpace(payload.Error.Message) != "" {
		message = message + ": " + payload.Error.Message
	}
	providerErr := &runtimepkg.ProviderError{
		Code:           sttStatusErrorCode(response.StatusCode),
		Message:        message,
		Retryable:      sttRetryableStatus(response.StatusCode),
		ProviderStatus: response.StatusCode,
	}
	if json.Valid(raw) {
		providerErr.Extensions = map[string]json.RawMessage{sttExtensionID: json.RawMessage(append([]byte(nil), raw...))}
	}
	return providerErr
}

func sttStatusErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status == http.StatusRequestEntityTooLarge:
		return "input_too_large"
	case status >= 500:
		return "provider_unavailable"
	case status >= 400:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func sttRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

// sttProviderRequestID reads whichever correlation header the Google frontend
// attached. Neither is contractual, so a missing header simply means no
// usage.observed event rather than an error.
func sttProviderRequestID(header http.Header) string {
	for _, name := range []string{"X-Goog-Request-Id", "X-Request-Id"} {
		if value := strings.TrimSpace(header.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// sttErrorEvent wraps a provider error as a terminal event. A nil error means
// the failure was a caller-initiated cancel that readError already classified,
// so nothing is published.
func sttErrorEvent(err *runtimepkg.ProviderError) *runtimepkg.ProviderEvent {
	if err == nil {
		return nil
	}
	return &runtimepkg.ProviderEvent{Err: err}
}

// sttCancelledEvent reports a caller-initiated abandon as a warning rather than
// a terminal Err: Cancel exists so a barge-in can be followed by another
// utterance, and an Err would tear the session down instead.
func sttCancelledEvent(recognitionID string) *runtimepkg.ProviderEvent {
	return &runtimepkg.ProviderEvent{
		Type: protocol.EventWarning,
		Data: sttMarshalData(map[string]any{"code": "provider_request_cancelled", "recognition_id": recognitionID}),
	}
}

func (s *sttStream) transcriptData(recognitionID string, result sttRecognitionResult) json.RawMessage {
	alternative := result.Alternatives[0]
	data := map[string]any{
		"text":           alternative.Transcript,
		"is_final":       true,
		"confidence":     alternative.Confidence,
		"recognition_id": recognitionID,
		"model":          s.model,
		"language_code":  s.languageCode,
		"location":       s.location,
	}
	// SpeechRecognitionResult.languageCode is "the language tag of the language
	// detected in the audio", which can differ from what was requested. Report
	// it only when the service actually filled it in.
	if detected := strings.TrimSpace(result.LanguageCode); detected != "" {
		data["detected_language_code"] = detected
	}
	if endMS, ok := sttDurationMilliseconds(result.ResultEndOffset); ok {
		data["audio_end_ms"] = endMS
	}
	if result.ChannelTag != 0 {
		data["channel_tag"] = result.ChannelTag
	}
	return sttMarshalData(data)
}

func sttSpeechEndedData(recognitionID string, results int, lastEndMS int64, haveEndMS bool) json.RawMessage {
	data := map[string]any{
		"recognition_id": recognitionID,
		"result_count":   results,
		"reason":         "recognition_complete",
	}
	if haveEndMS {
		data["audio_end_ms"] = lastEndMS
	}
	return sttMarshalData(data)
}

func sttUsageData(requestID, totalBilledDuration string) json.RawMessage {
	data := map[string]any{"provider_request_id": requestID}
	if billedMS, ok := sttDurationMilliseconds(totalBilledDuration); ok {
		data["billed_duration_ms"] = billedMS
	}
	return sttMarshalData(data)
}

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{sttExtensionID: raw}
}

func sttMarshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

// sttDurationMilliseconds parses the google-duration JSON form, which proto3
// renders as decimal seconds with a mandatory "s" suffix ("1.500s", "12s").
// It is deliberately strict: a value that is not a duration yields ok=false so
// the caller omits the field rather than reporting a zero offset as real.
func sttDurationMilliseconds(value string) (int64, bool) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasSuffix(trimmed, "s") {
		return 0, false
	}
	seconds, err := strconv.ParseFloat(strings.TrimSuffix(trimmed, "s"), 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, false
	}
	return int64(math.Round(seconds * 1_000)), true
}

func newSTTRecognitionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate Google STT recognition id: %w", err)
	}
	return hex.EncodeToString(value), nil
}
