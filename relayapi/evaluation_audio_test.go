package relayapi

import (
	"encoding/json"
	"strings"
	"testing"
)

func audioEvaluationRequest() EvaluationRequest {
	return EvaluationRequest{
		Routing: Routing{Mode: RoutingModeExplicit, Provider: "gemini", Model: "gemini-decision-1"},
		Questions: map[string]EvaluationQuestion{
			"sentiment": {
				Type: EvaluationQuestionChoice, Instructions: raw(`"How does the caller sound?"`),
				Criteria: raw(`{"positive":"pleased","neutral":null,"frustrated":"annoyed, impatient","other":"none of these"}`),
			},
			"wants_human": {Type: EvaluationQuestionNoul, Instructions: raw(`"Is the caller asking for a person?"`), Criteria: raw(`{"true":"explicit ask","false":null}`)},
			"agitation":   {Type: EvaluationQuestionScore, Instructions: raw(`"How agitated?"`), Criteria: raw(`["calm","tense","agitated"]`)},
		},
	}
}

func TestValidateAudioAcceptsTextQuestionsWithoutState(t *testing.T) {
	t.Parallel()
	request := audioEvaluationRequest()
	if err := request.ValidateAudio(); err != nil {
		t.Fatalf("audio request without state must validate: %v", err)
	}
	// A request built without state marshals it as null; that is still no
	// state, not a malformed one.
	request.State = raw(`null`)
	if err := request.ValidateAudio(); err != nil || request.HasState() {
		t.Fatalf("null state must read as absent: %v", err)
	}
	request.State = raw(`"The agent just asked for the order number."`)
	if err := request.ValidateAudio(); err != nil {
		t.Fatalf("audio request with text state must validate: %v", err)
	}
	// Inner ASCII spaces in an option key are allowed.
	sentiment := request.Questions["sentiment"]
	sentiment.Criteria = raw(`{"very angry":null,"other":null}`)
	request.Questions["sentiment"] = sentiment
	if err := request.ValidateAudio(); err != nil {
		t.Fatalf("an option key with an inner space must validate: %v", err)
	}
	// The text path still requires state: an audio-shaped body sent as JSON
	// is not a valid text evaluation.
	request.State = nil
	if err := request.Validate(); err == nil {
		t.Fatal("Validate (text evaluation) must still require state")
	}
}

func TestValidateAudioRejectsStructuredContentAndBadIDs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*EvaluationRequest)
		want   string
	}{
		{"structured state", func(r *EvaluationRequest) { r.State = raw(`{"turn":4}`) }, "state"},
		{"blank state", func(r *EvaluationRequest) { r.State = raw(`"  "`) }, "state"},
		{"structured instructions", func(r *EvaluationRequest) {
			q := r.Questions["agitation"]
			q.Instructions = raw(`{"task":"rate"}`)
			r.Questions["agitation"] = q
		}, "instructions"},
		{"structured option description", func(r *EvaluationRequest) {
			q := r.Questions["sentiment"]
			q.Criteria = raw(`{"positive":{"label":"pleased"},"other":null}`)
			r.Questions["sentiment"] = q
		}, "criteria[\"positive\"]"},
		{"structured score level", func(r *EvaluationRequest) {
			q := r.Questions["agitation"]
			q.Criteria = raw(`["calm",{"label":"angry"}]`)
			r.Questions["agitation"] = q
		}, "criteria[1]"},
		{"structured noul criterion", func(r *EvaluationRequest) {
			q := r.Questions["wants_human"]
			q.Criteria = raw(`{"true":["explicit"]}`)
			r.Questions["wants_human"] = q
		}, "criteria.true"},
		{"option with a leading space", func(r *EvaluationRequest) {
			q := r.Questions["sentiment"]
			q.Criteria = raw(`{" leading":null,"other":null}`)
			r.Questions["sentiment"] = q
		}, "start or end with whitespace"},
		{"option with a tab", func(r *EvaluationRequest) {
			q := r.Questions["sentiment"]
			q.Criteria = raw(`{"very\tangry":null,"other":null}`)
			r.Questions["sentiment"] = q
		}, "control characters"},
		{"option with a non-ASCII space", func(r *EvaluationRequest) {
			q := r.Questions["sentiment"]
			q.Criteria = raw(`{"very angry":null,"other":null}`)
			r.Questions["sentiment"] = q
		}, "control characters"},
		{"option with a byte-order mark", func(r *EvaluationRequest) {
			q := r.Questions["sentiment"]
			q.Criteria = raw(`{"calm\ufeff":null,"other":null}`)
			r.Questions["sentiment"] = q
		}, "whitespace"},
		{"option with a C1 control", func(r *EvaluationRequest) {
			q := r.Questions["sentiment"]
			q.Criteria = raw(`{"ca\u0085lm":null,"other":null}`)
			r.Questions["sentiment"] = q
		}, "control characters"},
		{"instructions of only a byte-order mark", func(r *EvaluationRequest) {
			q := r.Questions["agitation"]
			q.Instructions = raw(`"\ufeff"`)
			r.Questions["agitation"] = q
		}, "instructions"},
		{"instructions of only U+0085", func(r *EvaluationRequest) {
			q := r.Questions["agitation"]
			q.Instructions = raw(`"\u0085"`)
			r.Questions["agitation"] = q
		}, "instructions"},
		{"id with a space", func(r *EvaluationRequest) { r.Questions["wants human"] = r.Questions["wants_human"] }, "must match"},
		{"id too long", func(r *EvaluationRequest) { r.Questions[strings.Repeat("a", 65)] = r.Questions["wants_human"] }, "must match"},
		{"auto routing", func(r *EvaluationRequest) { r.Routing = Routing{Mode: RoutingModeAuto, Objective: ObjectiveBalanced} }, "explicit"},
		{"no questions", func(r *EvaluationRequest) { r.Questions = nil }, "at least one question"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := audioEvaluationRequest()
			tc.mutate(&request)
			err := request.ValidateAudio()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateAudio() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestEvaluationShapeMeasuresQuestionsInCodePoints(t *testing.T) {
	t.Parallel()
	request := audioEvaluationRequest()
	request.State = raw(`"日本語のコンテキスト"`)
	shape := request.Shape()
	want := EvaluationShape{
		Questions: 3, MaxChoiceOptions: 4, MaxScoreLevels: 3,
		MaxInstructionChars: len("Is the caller asking for a person?"), ContextChars: 10,
		MaxOptionNameChars: len("frustrated"), MaxOptionDescriptionChars: len("annoyed, impatient"),
		MaxScoreLevelChars: len("agitated"), MaxNoulCriterionChars: len("explicit ask"),
	}
	if shape != want {
		t.Fatalf("Shape() = %+v, want %+v", shape, want)
	}
}

func TestEvaluationLimitsAcceptNameTheFirstExceededBound(t *testing.T) {
	t.Parallel()
	// Hume's published bounds, the tightest audio vendor today.
	limits := EvaluationLimits{
		MaxQuestions: 20, MaxChoiceOptions: 20, MaxScoreLevels: 10, MaxInstructionChars: 500,
		MaxContextChars: 1000, MaxOptionNameChars: 50, MaxOptionDescriptionChars: 100,
		MaxScoreLevelChars: 150, MaxNoulCriterionChars: 300, MinAudioMS: 1000, MaxAudioMS: 300_000,
	}
	if err := limits.Validate(); err != nil {
		t.Fatalf("limits must validate: %v", err)
	}
	shape := audioEvaluationRequest().Shape()
	shape.AudioMS = 4_000
	if name, ok := limits.Accepts(shape); !ok {
		t.Fatalf("Accepts() refused %q", name)
	}
	cases := []struct {
		mutate func(*EvaluationShape)
		want   string
	}{
		{func(s *EvaluationShape) { s.Questions = 21 }, "max_questions"},
		{func(s *EvaluationShape) { s.MaxChoiceOptions = 21 }, "max_choice_options"},
		{func(s *EvaluationShape) { s.MaxOptionNameChars = 51 }, "max_option_name_chars"},
		{func(s *EvaluationShape) { s.ContextChars = 1001 }, "max_context_chars"},
		{func(s *EvaluationShape) { s.AudioMS = 999 }, "min_audio_ms"},
		{func(s *EvaluationShape) { s.AudioMS = 300_001 }, "max_audio_ms"},
	}
	for _, tc := range cases {
		mutated := shape
		tc.mutate(&mutated)
		if name, ok := limits.Accepts(mutated); ok || name != tc.want {
			t.Fatalf("Accepts() = (%q, %v), want (%q, false)", name, ok, tc.want)
		}
	}
	if name, ok := (EvaluationLimits{}).Accepts(EvaluationShape{Questions: 10_000, AudioMS: 1}); !ok {
		t.Fatalf("zero limits must accept anything, refused %q", name)
	}
}

func TestEvaluationLimitsValidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		limits EvaluationLimits
		want   string
	}{
		{EvaluationLimits{MaxQuestions: -1}, "max_questions"},
		{EvaluationLimits{MaxChoiceOptions: 1}, "max_choice_options"},
		{EvaluationLimits{MaxChoiceOptions: MaxChoiceOptions + 1}, "max_choice_options"},
		{EvaluationLimits{MaxScoreLevels: MaxScoreLevels + 1}, "max_score_levels"},
		{EvaluationLimits{MinAudioMS: 2_000, MaxAudioMS: 1_000}, "min_audio_ms"},
	} {
		err := tc.limits.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("Validate(%+v) = %v, want an error containing %q", tc.limits, err, tc.want)
		}
	}
}

func TestEvaluationInputsDefaultToText(t *testing.T) {
	t.Parallel()
	jev := ModelCapabilities{}
	if !jev.AcceptsEvaluationInput(EvaluationInputText) || jev.AcceptsEvaluationInput(EvaluationInputAudio) {
		t.Fatal("a model advertising no inputs takes text only")
	}
	decision := ModelCapabilities{EvaluationInputs: []string{EvaluationInputAudio}}
	if decision.AcceptsEvaluationInput(EvaluationInputText) || !decision.AcceptsEvaluationInput(EvaluationInputAudio) {
		t.Fatal("an audio-only model must refuse text and accept audio")
	}
}

func TestAudioEvaluationModelValidation(t *testing.T) {
	t.Parallel()
	audioFormat := AudioFormat{Encoding: "pcm_s16le", SampleRateRangeHz: &SampleRateRange{Min: 8_000, Max: 48_000}, Channels: []int{1, 2}}
	model := Model{
		ID: "gemini-decision-1", Provider: "gemini", Kind: KindEvaluation, Regions: []string{"us-east-1"},
		Capabilities: ModelCapabilities{
			EvaluationQuestionTypes: []string{EvaluationQuestionChoice, EvaluationQuestionScore, EvaluationQuestionNoul},
			EvaluationInputs:        []string{EvaluationInputAudio},
			EvaluationLimits:        &EvaluationLimits{MaxQuestions: 1000, MaxAudioMS: 60_000},
		},
		AudioFormats: []AudioFormat{audioFormat},
	}
	if err := model.Validate(); err != nil {
		t.Fatalf("valid audio evaluation model: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*Model)
		want   string
	}{
		{"audio without formats", func(m *Model) { m.AudioFormats = nil }, "audio_formats"},
		{"text-only with formats", func(m *Model) { m.Capabilities.EvaluationInputs = nil }, "audio_formats"},
		{"unknown input", func(m *Model) { m.Capabilities.EvaluationInputs = []string{"video"} }, "evaluation_inputs: unsupported"},
		{"duplicate input", func(m *Model) {
			m.Capabilities.EvaluationInputs = []string{EvaluationInputAudio, EvaluationInputAudio}
		}, "evaluation_inputs: duplicate"},
		{"bad limits", func(m *Model) { m.Capabilities.EvaluationLimits = &EvaluationLimits{MaxChoiceOptions: 1} }, "evaluation_limits"},
		{"inputs on an llm", func(m *Model) {
			m.Kind = KindLLM
			m.AudioFormats = nil
			m.Capabilities.EvaluationQuestionTypes = nil
		}, "valid only for evaluation models"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mutated := model
			mutated.Capabilities.EvaluationInputs = append([]string(nil), model.Capabilities.EvaluationInputs...)
			tc.mutate(&mutated)
			err := mutated.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestEvaluationLimitsWireNamesAreStable(t *testing.T) {
	t.Parallel()
	payload, err := json.Marshal(EvaluationLimits{MaxQuestions: 25, MinAudioMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"max_questions":25,"min_audio_ms":1000}` {
		t.Fatalf("EvaluationLimits wire = %s", payload)
	}
}
