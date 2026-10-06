package relayapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
)

const (
	EvaluationQuestionNoul   = "noul"
	EvaluationQuestionChoice = "choice"
	EvaluationQuestionScore  = "score"
	MaxChoiceOptions         = 255
	MaxScoreLevels           = 10
)

// EvaluationRequest preserves TypeSafe's native state/questions shape and
// adds only Speko's explicit route selector.
type EvaluationRequest struct {
	Routing   Routing                       `json:"routing"`
	State     json.RawMessage               `json:"state"`
	Questions map[string]EvaluationQuestion `json:"questions"`
}

type EvaluationQuestion struct {
	Type         string          `json:"type"`
	Instructions json.RawMessage `json:"instructions"`
	Criteria     json.RawMessage `json:"criteria,omitempty"`
}

type EvaluationResponse struct {
	Model   string                      `json:"model"`
	Answers map[string]EvaluationAnswer `json:"answers"`
	Usage   Usage                       `json:"usage"`
}

type EvaluationAnswer struct {
	Type          string                     `json:"type"`
	Noul          *float64                   `json:"noul,omitempty"`
	Choice        string                     `json:"choice,omitempty"`
	Score         *float64                   `json:"score,omitempty"`
	Probabilities map[string]float64         `json:"probabilities,omitempty"`
	Confidence    *float64                   `json:"confidence,omitempty"`
	Legend        map[string]json.RawMessage `json:"legend,omitempty"`
}

// UnmarshalJSON keeps the evaluation surface closed. Generation-only fields
// such as stream, temperature, tools, and max_output_tokens are therefore
// rejected instead of being silently ignored by encoding/json.
func (r *EvaluationRequest) UnmarshalJSON(data []byte) error {
	type wire EvaluationRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded wire
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*r = EvaluationRequest(decoded)
	return nil
}

func (q *EvaluationQuestion) UnmarshalJSON(data []byte) error {
	type wire EvaluationQuestion
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded wire
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*q = EvaluationQuestion(decoded)
	return nil
}

func validEvaluationJSONContent(raw json.RawMessage) bool {
	if len(raw) == 0 || !json.Valid(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	switch value.(type) {
	case string, []any, map[string]any:
		return true
	default:
		return false
	}
}

func (r EvaluationRequest) Validate() error {
	if err := r.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if r.Routing.Mode != RoutingModeExplicit {
		return fmt.Errorf("routing: evaluation requests require explicit routing")
	}
	if !validEvaluationJSONContent(r.State) {
		return fmt.Errorf("state: must be a string, object, or array")
	}
	if len(r.Questions) == 0 {
		return fmt.Errorf("questions: at least one question is required")
	}
	for id, question := range r.Questions {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("questions: question ids must not be blank")
		}
		if err := question.Validate(); err != nil {
			return fmt.Errorf("questions[%q]: %w", id, err)
		}
	}
	return nil
}

func (q EvaluationQuestion) Validate() error {
	if !validEvaluationJSONContent(q.Instructions) {
		return fmt.Errorf("instructions: must be a string, object, or array")
	}
	switch q.Type {
	case EvaluationQuestionNoul:
		if len(q.Criteria) == 0 {
			return nil
		}
		var criteria map[string]json.RawMessage
		if err := json.Unmarshal(q.Criteria, &criteria); err != nil || len(criteria) == 0 {
			return fmt.Errorf("criteria: must contain true and/or false")
		}
		for key, value := range criteria {
			if key != "true" && key != "false" {
				return fmt.Errorf("criteria: unsupported key %q", key)
			}
			if !bytes.Equal(bytes.TrimSpace(value), []byte("null")) && !validEvaluationJSONContent(value) {
				return fmt.Errorf("criteria.%s: must be null, a string, object, or array", key)
			}
		}
	case EvaluationQuestionChoice:
		var criteria map[string]json.RawMessage
		if err := json.Unmarshal(q.Criteria, &criteria); err != nil || len(criteria) < 2 || len(criteria) > MaxChoiceOptions {
			return fmt.Errorf("criteria: choice requires 2 to %d options", MaxChoiceOptions)
		}
		for option, description := range criteria {
			if strings.TrimSpace(option) == "" {
				return fmt.Errorf("criteria: option names must not be blank")
			}
			if bytes.Equal(bytes.TrimSpace(description), []byte("null")) {
				continue
			}
			if !validEvaluationJSONContent(description) {
				return fmt.Errorf("criteria[%q]: must be null, a string, object, or array", option)
			}
		}
	case EvaluationQuestionScore:
		var criteria []json.RawMessage
		if err := json.Unmarshal(q.Criteria, &criteria); err != nil || len(criteria) < 2 || len(criteria) > MaxScoreLevels {
			return fmt.Errorf("criteria: score requires 2 to %d levels", MaxScoreLevels)
		}
		for index, level := range criteria {
			if !validEvaluationJSONContent(level) {
				return fmt.Errorf("criteria[%d]: must be a string, object, or array", index)
			}
		}
	default:
		return fmt.Errorf("type: unsupported value %q", q.Type)
	}
	return nil
}

func evaluationProbability(value float64) bool {
	return !math.IsNaN(value) && value >= 0 && value <= 1
}

func evaluationJSONEqual(left, right json.RawMessage) bool {
	decode := func(raw json.RawMessage) (any, error) {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		return value, nil
	}
	leftValue, leftErr := decode(left)
	rightValue, rightErr := decode(right)
	return leftErr == nil && rightErr == nil && evaluationValueEqual(leftValue, rightValue)
}

func evaluationValueEqual(left, right any) bool {
	switch typedLeft := left.(type) {
	case json.Number:
		typedRight, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftOK := new(big.Rat).SetString(typedLeft.String())
		rightNumber, rightOK := new(big.Rat).SetString(typedRight.String())
		return leftOK && rightOK && leftNumber.Cmp(rightNumber) == 0
	case []any:
		typedRight, ok := right.([]any)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for index := range typedLeft {
			if !evaluationValueEqual(typedLeft[index], typedRight[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		typedRight, ok := right.(map[string]any)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for key, value := range typedLeft {
			rightValue, present := typedRight[key]
			if !present || !evaluationValueEqual(value, rightValue) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

// ValidateFor proves that an upstream response answers exactly the questions
// sent and cannot introduce an option or score level absent from the request.
func (r EvaluationResponse) ValidateFor(request EvaluationRequest) error {
	if strings.TrimSpace(r.Model) == "" || len(r.Answers) != len(request.Questions) {
		return fmt.Errorf("model and exactly one answer per question are required")
	}
	if err := r.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	if r.Usage.Incomplete || r.Usage.InputTokens <= 0 || r.Usage.CachedInputTokens != 0 || r.Usage.CacheWrite5mTokens != 0 || r.Usage.CacheWrite1hTokens != 0 || r.Usage.DurationMS != 0 || r.Usage.Characters != 0 || r.Usage.ReasoningTokens != 0 || r.Usage.ToolCalls != 0 {
		return fmt.Errorf("usage: evaluation usage requires positive input_tokens and optional output_tokens only")
	}
	for id, question := range request.Questions {
		answer, ok := r.Answers[id]
		if !ok || answer.Type != question.Type {
			return fmt.Errorf("answers[%q]: missing or type mismatch", id)
		}
		if err := answer.validate(question); err != nil {
			return fmt.Errorf("answers[%q]: %w", id, err)
		}
	}
	return nil
}

func (a EvaluationAnswer) validate(question EvaluationQuestion) error {
	switch a.Type {
	case EvaluationQuestionNoul:
		if a.Noul == nil || !evaluationProbability(*a.Noul) || a.Choice != "" || a.Score != nil || a.Confidence != nil || len(a.Probabilities) != 0 || len(a.Legend) != 0 {
			return fmt.Errorf("invalid noul answer")
		}
	case EvaluationQuestionChoice:
		var criteria map[string]json.RawMessage
		_ = json.Unmarshal(question.Criteria, &criteria)
		if a.Noul != nil || a.Score != nil || len(a.Legend) != 0 {
			return fmt.Errorf("invalid choice answer")
		}
		if _, ok := criteria[a.Choice]; !ok || a.Confidence == nil || !evaluationProbability(*a.Confidence) || len(a.Probabilities) != len(criteria) {
			return fmt.Errorf("invalid choice answer")
		}
		if err := validateEvaluationDistribution(a.Probabilities, func(key string) bool { _, ok := criteria[key]; return ok }); err != nil {
			return err
		}
	case EvaluationQuestionScore:
		var criteria []json.RawMessage
		_ = json.Unmarshal(question.Criteria, &criteria)
		if a.Noul != nil || a.Choice != "" || a.Score == nil || *a.Score < 0 || *a.Score > float64(len(criteria)-1) || a.Confidence == nil || !evaluationProbability(*a.Confidence) || len(a.Probabilities) != len(criteria) || len(a.Legend) != len(criteria) {
			return fmt.Errorf("invalid score answer")
		}
		for index, level := range criteria {
			key := fmt.Sprint(index)
			legend, ok := a.Legend[key]
			if !ok || !evaluationJSONEqual(legend, level) {
				return fmt.Errorf("invalid score legend")
			}
		}
		if err := validateEvaluationDistribution(a.Probabilities, func(key string) bool {
			for i := range criteria {
				if key == fmt.Sprint(i) {
					return true
				}
			}
			return false
		}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported answer type")
	}
	return nil
}

func validateEvaluationDistribution(values map[string]float64, allowed func(string) bool) error {
	var sum float64
	for key, value := range values {
		if !allowed(key) || !evaluationProbability(value) {
			return fmt.Errorf("invalid probability distribution")
		}
		sum += value
	}
	if math.Abs(sum-1) > 1e-6 {
		return fmt.Errorf("probabilities must sum to 1")
	}
	return nil
}
