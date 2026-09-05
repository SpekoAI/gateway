package runtime

import (
	"context"
	"encoding/json"
	"io"

	"github.com/SpekoAI/gateway/protocol"
)

// BatchTranscriber is the provider-specific implementation point for
// transcribing one complete PRERECORDED utterance through a provider's
// non-realtime API — the endpoints vendors document as "pre-recorded",
// "async", "file" or "batch" transcription.
//
// It exists because Adapter cannot express that shape. ProviderStream is a
// realtime contract (write audio, commit, drain events) and every realtime
// socket has an ingest rate it can actually sustain; pushing a 30-minute
// upload down one at wire speed silently yields the first 37 seconds and a
// clean close. A batch endpoint takes the whole file, returns a whole
// transcript, and reports the duration it processed — which is also the
// only honest billing basis for prerecorded audio.
//
// One call is one provider request (or one submit-poll-fetch sequence).
// Chunking oversized audio is the caller's job, informed by the limits the
// caller's catalog publishes; adapters refuse rather than split.
type BatchTranscriber interface {
	ID() string
	Transcribe(context.Context, BatchTranscribeRequest) (*BatchTranscription, error)
}

// BatchTranscribeRequest is the already-validated execution choice plus the
// audio. Plan.Route names the BATCH endpoint and model — not the realtime
// pair for the same product — and Route.Credential authenticates exactly as
// it does for Adapter.
type BatchTranscribeRequest struct {
	Plan    protocol.SessionPlan
	Options protocol.RequestOptions
	// Media is the PCM format inside the WAV container.
	Media protocol.MediaFormat
	// Audio is the complete WAV container (RIFF header + PCM data). Adapters
	// receive a container rather than raw PCM because almost no batch endpoint
	// accepts headerless audio, and the ones that do also accept WAV. Adapters
	// that need a second pass over the bytes (an upload step before a submit
	// step, a retry) Seek to the start rather than buffering the file.
	Audio io.ReadSeeker
	// AudioBytes is the container's total length, for Content-Length and for
	// refusing an upload the provider documents as too large before sending
	// any of it.
	AudioBytes int64
	// SourceURL is an HTTPS URL the provider may fetch the same audio from.
	// Set only by callers that can host the audio for providers whose batch
	// API takes URL input exclusively; adapters that upload bytes ignore it.
	SourceURL string
}

// BatchTranscription is the normalized result. Text is the whole transcript
// in reading order; Segments are the provider's time-aligned spans (words
// grouped into utterances where the vendor gives no coarser unit) and may be
// empty when a provider returns untimed text.
type BatchTranscription struct {
	Text     string
	Segments []BatchSegment
	// Words are the provider's per-word timings, populated only when the
	// request asked for them (SttOptions.WordTimestamps) and the provider
	// returned them. Segments stay the coarser unit; Words never replace it.
	Words []BatchWord
	// Language is the BCP-47 tag the provider reported using or detecting;
	// empty when it reported none.
	Language string
	// DurationMS is the audio duration the PROVIDER reported processing —
	// the quantity it bills — or zero when the response carries none. Callers
	// meter from this rather than from bytes sent when it is present.
	DurationMS int64
	// ProviderRequestID is the provider's request or job identifier: evidence
	// for settlement disputes, never customer-visible output.
	ProviderRequestID string
	// Extensions may retain the raw vendor payload for diagnostics. Callers
	// never forward it to customers or telemetry.
	Extensions map[string]json.RawMessage
}

// BatchSegment is one time-aligned span of the transcript. Speaker is the
// vendor's own label carried verbatim (a number, "A", "speaker_1"), empty when
// the provider did not diarize.
type BatchSegment struct {
	Text    string
	StartMS int64
	EndMS   int64
	Speaker string
}

// BatchWord is one word of the transcript with its own timing. Speaker is
// the vendor's own label carried verbatim, empty when the provider did not
// diarize.
type BatchWord struct {
	Text    string
	StartMS int64
	EndMS   int64
	Speaker string
}

// LastTimedMS returns the end of the latest timed span, or zero when the
// result carries no timing. Callers compare it against the audio they sent to
// detect a provider that returned a prefix of the transcript as if it were
// the whole.
func (t *BatchTranscription) LastTimedMS() int64 {
	if t == nil {
		return 0
	}
	var last int64
	for _, segment := range t.Segments {
		if segment.EndMS > last {
			last = segment.EndMS
		}
	}
	return last
}
