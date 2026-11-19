package relayapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Audio decisions share POST /v1/evaluations with the text evaluations above.
// A JSON body is a text evaluation (state + questions). A multipart body is an
// audio evaluation: a "request" part holding an EvaluationRequest, then an
// "audio" part holding one WAV (PCM s16le) clip. The question and answer
// types are the same noul/choice/score primitives either way, so one caller
// can switch decision models by changing routing alone.
const (
	EvaluationRequestPart = "request"
	EvaluationAudioPart   = "audio"
)

// Evaluation inputs a model accepts, advertised in
// ModelCapabilities.EvaluationInputs. A model that advertises none takes text
// only, which is what every evaluation model took before audio existed.
const (
	EvaluationInputText  = "text"
	EvaluationInputAudio = "audio"
)

// MaxEvaluationQuestionIDChars bounds audio question ids. Audio decision
// vendors key questions with restricted ids (Gemini: [A-Za-z0-9_-]{1,64}), so
// the audio contract fixes the common subset rather than letting one vendor
// reject an id another accepted.
const MaxEvaluationQuestionIDChars = 64

var evaluationQuestionIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidateAudio checks the request part of an audio evaluation. It is Validate
// with the two rules every audio decision vendor shares:
//
//   - state is optional and, when present, is plain-text context about the
//     clip (who is speaking, what was said before). It is never the decided
//     material; the audio is.
//   - instructions, option descriptions, noul criteria and score levels are
//     plain text. Structured JSON content is a text-evaluation feature.
//
// Question ids must match [A-Za-z0-9_-]{1,64}. Per-model bounds (question
// count, option count, text lengths, audio duration) are advertised in
// ModelCapabilities.EvaluationLimits and checked against the routed model.
func (r EvaluationRequest) ValidateAudio() error {
	if err := r.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if r.Routing.Mode != RoutingModeExplicit {
		return fmt.Errorf("routing: evaluation requests require explicit routing")
	}
	if r.HasState() {
		if _, ok := evaluationText(r.State); !ok {
			return fmt.Errorf("state: must be a non-blank string when audio is the input")
		}
	}
	if len(r.Questions) == 0 {
		return fmt.Errorf("questions: at least one question is required")
	}
	for id, question := range r.Questions {
		if !evaluationQuestionIDPattern.MatchString(id) {
			return fmt.Errorf("questions: id %q must match [A-Za-z0-9_-]{1,%d}", id, MaxEvaluationQuestionIDChars)
		}
		if err := question.Validate(); err != nil {
			return fmt.Errorf("questions[%q]: %w", id, err)
		}
		if err := question.validateText(); err != nil {
			return fmt.Errorf("questions[%q]: %w", id, err)
		}
	}
	return nil
}

// HasState reports whether the request carries state. Absent and JSON null
// are the same: EvaluationRequest always marshals the field, so a request
// built without state round-trips as "state":null.
func (r EvaluationRequest) HasState() bool {
	trimmed := bytes.TrimSpace(r.State)
	return len(trimmed) != 0 && !bytes.Equal(trimmed, []byte("null"))
}

// validateText enforces plain-text content on an already Validate()d
// question.
func (q EvaluationQuestion) validateText() error {
	if _, ok := evaluationText(q.Instructions); !ok {
		return fmt.Errorf("instructions: must be a non-blank string")
	}
	switch q.Type {
	case EvaluationQuestionNoul:
		if len(q.Criteria) == 0 {
			return nil
		}
		var criteria map[string]json.RawMessage
		_ = json.Unmarshal(q.Criteria, &criteria)
		for key, value := range criteria {
			if !evaluationTextOrNull(value) {
				return fmt.Errorf("criteria.%s: must be null or a non-blank string", key)
			}
		}
	case EvaluationQuestionChoice:
		var criteria map[string]json.RawMessage
		_ = json.Unmarshal(q.Criteria, &criteria)
		for option, description := range criteria {
			if !evaluationTextOrNull(description) {
				return fmt.Errorf("criteria[%q]: must be null or a non-blank string", option)
			}
		}
	case EvaluationQuestionScore:
		var criteria []json.RawMessage
		_ = json.Unmarshal(q.Criteria, &criteria)
		for index, level := range criteria {
			if _, ok := evaluationText(level); !ok {
				return fmt.Errorf("criteria[%d]: must be a non-blank string", index)
			}
		}
	}
	return nil
}

// evaluationText decodes raw as a JSON string that is not blank.
func evaluationText(raw json.RawMessage) (string, bool) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil || strings.TrimSpace(text) == "" {
		return "", false
	}
	return text, true
}

func evaluationTextOrNull(raw json.RawMessage) bool {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return true
	}
	_, ok := evaluationText(raw)
	return ok
}

// evaluationChars counts what a vendor's length limit counts: the code points
// of a string, or the encoded size of structured content.
func evaluationChars(raw json.RawMessage) int {
	if text, ok := evaluationText(raw); ok {
		return utf8.RuneCountInString(text)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0
	}
	return len(bytes.TrimSpace(raw))
}

// EvaluationLimits are one model's published bounds on an evaluation request.
// A zero field means the model publishes no such bound. Callers that want to
// switch models freely validate against the intersection of the models they
// use; the relay refuses a request the routed model's limits do not accept
// with capability_unsupported before admission.
type EvaluationLimits struct {
	MaxQuestions              int   `json:"max_questions,omitempty"`
	MaxChoiceOptions          int   `json:"max_choice_options,omitempty"`
	MaxScoreLevels            int   `json:"max_score_levels,omitempty"`
	MaxInstructionChars       int   `json:"max_instruction_chars,omitempty"`
	MaxContextChars           int   `json:"max_context_chars,omitempty"`
	MaxOptionNameChars        int   `json:"max_option_name_chars,omitempty"`
	MaxOptionDescriptionChars int   `json:"max_option_description_chars,omitempty"`
	MaxScoreLevelChars        int   `json:"max_score_level_chars,omitempty"`
	MaxNoulCriterionChars     int   `json:"max_noul_criterion_chars,omitempty"`
	MinAudioMS                int64 `json:"min_audio_ms,omitempty"`
	MaxAudioMS                int64 `json:"max_audio_ms,omitempty"`
	MaxAudioBytes             int64 `json:"max_audio_bytes,omitempty"`
}

// Validate checks that the limits are internally consistent.
func (l EvaluationLimits) Validate() error {
	for name, value := range map[string]int64{
		"max_questions": int64(l.MaxQuestions), "max_choice_options": int64(l.MaxChoiceOptions),
		"max_score_levels": int64(l.MaxScoreLevels), "max_instruction_chars": int64(l.MaxInstructionChars),
		"max_context_chars": int64(l.MaxContextChars), "max_option_name_chars": int64(l.MaxOptionNameChars),
		"max_option_description_chars": int64(l.MaxOptionDescriptionChars), "max_score_level_chars": int64(l.MaxScoreLevelChars),
		"max_noul_criterion_chars": int64(l.MaxNoulCriterionChars), "min_audio_ms": l.MinAudioMS,
		"max_audio_ms": l.MaxAudioMS, "max_audio_bytes": l.MaxAudioBytes,
	} {
		if value < 0 {
			return fmt.Errorf("%s: must not be negative", name)
		}
	}
	if l.MaxChoiceOptions == 1 || l.MaxChoiceOptions > MaxChoiceOptions {
		return fmt.Errorf("max_choice_options: must be between 2 and %d", MaxChoiceOptions)
	}
	if l.MaxScoreLevels == 1 || l.MaxScoreLevels > MaxScoreLevels {
		return fmt.Errorf("max_score_levels: must be between 2 and %d", MaxScoreLevels)
	}
	if l.MinAudioMS != 0 && l.MaxAudioMS != 0 && l.MinAudioMS > l.MaxAudioMS {
		return fmt.Errorf("min_audio_ms: must not exceed max_audio_ms")
	}
	return nil
}

// EvaluationShape measures a request in the units EvaluationLimits bound.
// Character counts are Unicode code points of the text values; the audio
// fields are filled in by whoever parsed the clip.
type EvaluationShape struct {
	Questions                 int
	MaxChoiceOptions          int
	MaxScoreLevels            int
	MaxInstructionChars       int
	ContextChars              int
	MaxOptionNameChars        int
	MaxOptionDescriptionChars int
	MaxScoreLevelChars        int
	MaxNoulCriterionChars     int
	AudioMS                   int64
	AudioBytes                int64
}

// Shape measures the request's questions and context. Call it on a request
// that already passed Validate or ValidateAudio.
func (r EvaluationRequest) Shape() EvaluationShape {
	shape := EvaluationShape{Questions: len(r.Questions)}
	if r.HasState() {
		shape.ContextChars = evaluationChars(r.State)
	}
	for _, question := range r.Questions {
		shape.MaxInstructionChars = max(shape.MaxInstructionChars, evaluationChars(question.Instructions))
		switch question.Type {
		case EvaluationQuestionNoul:
			var criteria map[string]json.RawMessage
			_ = json.Unmarshal(question.Criteria, &criteria)
			for _, value := range criteria {
				shape.MaxNoulCriterionChars = max(shape.MaxNoulCriterionChars, evaluationChars(value))
			}
		case EvaluationQuestionChoice:
			var criteria map[string]json.RawMessage
			_ = json.Unmarshal(question.Criteria, &criteria)
			shape.MaxChoiceOptions = max(shape.MaxChoiceOptions, len(criteria))
			for option, description := range criteria {
				shape.MaxOptionNameChars = max(shape.MaxOptionNameChars, utf8.RuneCountInString(option))
				shape.MaxOptionDescriptionChars = max(shape.MaxOptionDescriptionChars, evaluationChars(description))
			}
		case EvaluationQuestionScore:
			var criteria []json.RawMessage
			_ = json.Unmarshal(question.Criteria, &criteria)
			shape.MaxScoreLevels = max(shape.MaxScoreLevels, len(criteria))
			for _, level := range criteria {
				shape.MaxScoreLevelChars = max(shape.MaxScoreLevelChars, evaluationChars(level))
			}
		}
	}
	return shape
}

// Accepts reports whether the limits admit the shape, naming the first limit
// it exceeds (or "min_audio_ms" for a clip that is too short).
func (l EvaluationLimits) Accepts(shape EvaluationShape) (string, bool) {
	checks := []struct {
		name         string
		limit, value int64
	}{
		{"max_questions", int64(l.MaxQuestions), int64(shape.Questions)},
		{"max_choice_options", int64(l.MaxChoiceOptions), int64(shape.MaxChoiceOptions)},
		{"max_score_levels", int64(l.MaxScoreLevels), int64(shape.MaxScoreLevels)},
		{"max_instruction_chars", int64(l.MaxInstructionChars), int64(shape.MaxInstructionChars)},
		{"max_context_chars", int64(l.MaxContextChars), int64(shape.ContextChars)},
		{"max_option_name_chars", int64(l.MaxOptionNameChars), int64(shape.MaxOptionNameChars)},
		{"max_option_description_chars", int64(l.MaxOptionDescriptionChars), int64(shape.MaxOptionDescriptionChars)},
		{"max_score_level_chars", int64(l.MaxScoreLevelChars), int64(shape.MaxScoreLevelChars)},
		{"max_noul_criterion_chars", int64(l.MaxNoulCriterionChars), int64(shape.MaxNoulCriterionChars)},
		{"max_audio_ms", l.MaxAudioMS, shape.AudioMS},
		{"max_audio_bytes", l.MaxAudioBytes, shape.AudioBytes},
	}
	for _, check := range checks {
		if check.limit > 0 && check.value > check.limit {
			return check.name, false
		}
	}
	if l.MinAudioMS > 0 && shape.AudioMS < l.MinAudioMS {
		return "min_audio_ms", false
	}
	return "", true
}

// AcceptsEvaluationInput reports whether an evaluation model takes the input
// ("text" or "audio"). No advertised inputs means text only.
func (c ModelCapabilities) AcceptsEvaluationInput(input string) bool {
	if len(c.EvaluationInputs) == 0 {
		return input == EvaluationInputText
	}
	for _, accepted := range c.EvaluationInputs {
		if accepted == input {
			return true
		}
	}
	return false
}
