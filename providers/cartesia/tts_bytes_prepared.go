package cartesia

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// PreparedBytesTTSInput holds the complete final provider body in private memory.
// It supplies no execution permission or credential and must never be logged.
// Only PrepareInput can produce a valid instance. The private implementation is
// held by value: even a shared consumer cannot overwrite an aliased pointee.
type PreparedBytesTTSInput interface {
	// Text is the exact final transcript encoded in the provider body.
	Text() string
	preparedBytesTTSInput()
}

type preparedBytesTTSInput struct {
	payload    string
	text       string
	characters int64
}

func (preparedBytesTTSInput) preparedBytesTTSInput() {}

// Text returns the exact final transcript encoded in the provider body. A native
// execution owner can bind it before funding, then forward the prepared body.
func (p preparedBytesTTSInput) Text() string {
	return p.text
}

// PrepareInput applies every input rewrite and validates the concrete model,
// media, generation options and bounds without credentials or any provider I/O.
// OpenPrepared verifies the later funded settings and sends these same bytes.
func (a *BytesTTSAdapter) PrepareInput(request runtimepkg.AdapterRequest, text string) (PreparedBytesTTSInput, error) {
	if a == nil {
		return nil, errors.New("cartesia bytes tts: missing adapter")
	}
	input, err := bytesTTSInputTemplate(request)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareBytesTTSInput(input, text, a.config.MaxInputBytes, request.Options.MaxInputCharacters)
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

// OpenPrepared starts exactly one prepared operation after ordinary credential
// and endpoint checks. Changed funded settings refuse before provider I/O. The
// returned stream rejects AppendText and CommitText, including after completion;
// its cancellation, output and billing lifecycle are the ordinary bytes stream.
func (a *BytesTTSAdapter) OpenPrepared(ctx context.Context, request runtimepkg.AdapterRequest, prepared PreparedBytesTTSInput) (runtimepkg.ProviderStream, error) {
	// A consumer may embed the interface and override Text. Only the exact
	// private value returned by PrepareInput may reach validation or dispatch.
	input, ok := prepared.(preparedBytesTTSInput)
	if a == nil || !ok || input.payload == "" {
		return nil, errors.New("cartesia bytes tts: missing prepared input")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stream, err := a.open(request)
	if err != nil {
		return nil, err
	}
	actual, err := prepareBytesTTSInput(bytesSpeechRequest{Model: stream.model, Voice: stream.voice, Language: stream.language, Normalization: stream.normalization,
		OutputFormat: outputFormat{Container: "raw", Encoding: "pcm_s16le", SampleRate: stream.sampleRateHz}}, input.text, stream.maximumInputBytes, stream.maximumCharacters)
	if err != nil || actual.payload != input.payload || actual.characters != input.characters {
		stream.cancel()
		return nil, errors.New("cartesia bytes tts: prepared input does not match funded settings")
	}
	// This stream owns a single operation, so its entire I/O lifetime follows
	// the caller's admitted work rather than an ordinary multi-utterance session.
	stream.cancel()
	stream.ctx, stream.cancel = context.WithCancel(ctx)
	stream.stateMu.Lock()
	stream.preparedOnly = true
	err = stream.startLocked(ctx, []byte(input.payload), input.characters)
	stream.stateMu.Unlock()
	if err != nil {
		stream.cancel()
		return nil, err
	}
	return stream, nil
}

func bytesTTSInputTemplate(request runtimepkg.AdapterRequest) (bytesSpeechRequest, error) {
	if request.Kind != protocol.SessionKindTTS || request.Plan.Route.Provider != "cartesia" || request.Plan.Route.Transport != protocol.TransportHTTP {
		return bytesSpeechRequest{}, errors.New("cartesia bytes tts: incompatible route")
	}
	model := request.Plan.Route.Model
	if model != "sonic-3" && model != "sonic-3.5" && model != "sonic-3.6" {
		return bytesSpeechRequest{}, errors.New("cartesia bytes tts: unsupported concrete model")
	}
	if request.Media == nil || request.Media.Validate() != nil || request.Media.Channels != 1 || request.Media.Encoding != "pcm_s16le" {
		return bytesSpeechRequest{}, &runtimepkg.ProviderError{Code: "unsupported_media", Message: "Cartesia bytes TTS requires mono pcm_s16le"}
	}
	rate := request.Media.SampleRateHz
	if rate != 8000 && rate != 16000 && rate != 22050 && rate != 24000 && rate != 44100 && rate != 48000 {
		return bytesSpeechRequest{}, &runtimepkg.ProviderError{Code: "unsupported_media", Message: "Cartesia bytes TTS sample rate is unsupported"}
	}
	if err := validateGenerationOptions(model, request.Options, *request.Media); err != nil {
		return bytesSpeechRequest{}, err
	}
	normalization, err := ttsNormalization(model, request.Options.TTS.Provider("cartesia"))
	if err != nil {
		return bytesSpeechRequest{}, err
	}
	if request.Options.MaxInputCharacters < 0 || !utf8.ValidString(request.Options.Voice) || !utf8.ValidString(request.Options.Language) || len(request.Options.Voice) > 512 || len(request.Options.Language) > 1024 {
		return bytesSpeechRequest{}, errors.New("cartesia bytes tts: invalid request options")
	}
	return bytesSpeechRequest{Model: model, Voice: request.Options.Voice, Language: request.Options.Language, Normalization: normalization,
		OutputFormat: outputFormat{Container: "raw", Encoding: "pcm_s16le", SampleRate: rate}}, nil
}

func prepareBytesTTSInput(input bytesSpeechRequest, text string, maximumBytes int, maximumCharacters int64) (preparedBytesTTSInput, error) {
	if len(text) > maximumBytes {
		return preparedBytesTTSInput{}, bytesInputTooLarge()
	}
	if !utf8.ValidString(text) || strings.TrimSpace(text) == "" {
		return preparedBytesTTSInput{}, errors.New("cartesia bytes tts text must be nonempty UTF-8")
	}
	characters := int64(utf8.RuneCountInString(text))
	if maximumCharacters > 0 && characters > maximumCharacters {
		return preparedBytesTTSInput{}, bytesInputTooLarge()
	}
	input.Input = text
	payload, err := json.Marshal(input)
	if err != nil {
		return preparedBytesTTSInput{}, errors.New("cartesia bytes tts could not prepare input")
	}
	return preparedBytesTTSInput{payload: string(payload), text: input.Input, characters: characters}, nil
}
