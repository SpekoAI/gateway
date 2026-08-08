package relayapi_test

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"
)

// This file is the dependency-free JSON-Schema-subset validator behind
// speccheck_test.go. It exists so the hand-authored OpenAPI/AsyncAPI
// documents are machine-checked against the golden fixtures without pulling a
// schema-validation module into the contract repo.
//
// The keyword set is the deliberate subset the specs actually use: $ref
// (same-document only), type, enum, properties, required,
// additionalProperties: false, items, oneOf with discriminator tag dispatch,
// pattern, minLength, maxLength, minimum, maximum, and exclusiveMinimum.
// Every other keyword — description, format, discriminator's own object, the
// AsyncAPI message envelope fields — is annotation here and is deliberately
// ignored, matching JSON Schema's treatment of unknown keywords. A spec
// change that starts relying on a keyword outside this subset silently
// weakens the check, so keep the specs inside it.

// specDoc is one parsed spec document — always the checked-in JSON mirror of
// a hand-authored YAML spec, because encoding/json is stdlib and a YAML
// parser is not. TestSpecMirrorFingerprints pins the mirror to its YAML.
type specDoc struct {
	name string
	root map[string]any
}

func loadSpecDoc(t *testing.T, name string) *specDoc {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read spec mirror: %v", err)
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatalf("%s: parse: %v", name, err)
	}
	return &specDoc{name: name, root: root}
}

// schema returns the named component schema. Both OpenAPI 3.1 and AsyncAPI
// 2.6 keep reusable schemas under components/schemas, so one lookup serves
// both documents.
func (d *specDoc) schema(t *testing.T, name string) map[string]any {
	t.Helper()
	schema, err := d.resolve("#/components/schemas/" + name)
	if err != nil {
		t.Fatalf("%s: %v", d.name, err)
	}
	return schema
}

// resolve walks a same-document JSON pointer such as
// "#/components/schemas/Routing". The specs use no escaped reference tokens
// (~0/~1), so plain path segments are sufficient — resolve rejects anything
// it cannot walk rather than guessing.
func (d *specDoc) resolve(ref string) (map[string]any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("$ref %q: only same-document refs are supported", ref)
	}
	node := any(d.root)
	for _, segment := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		object, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("$ref %q: segment %q does not address an object", ref, segment)
		}
		node, ok = object[segment]
		if !ok {
			return nil, fmt.Errorf("$ref %q: no such segment %q", ref, segment)
		}
	}
	schema, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("$ref %q: target is not a schema object", ref)
	}
	return schema, nil
}

// validate checks a decoded JSON value (map[string]any / []any / string /
// float64 / bool / nil, as encoding/json produces) against a schema, keyword
// by keyword. path names the value's location for error messages.
func (d *specDoc) validate(schema map[string]any, value any, path string) error {
	if ref, ok := schema["$ref"].(string); ok {
		target, err := d.resolve(ref)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := d.validate(target, value, path); err != nil {
			return err
		}
		// JSON Schema 2020-12 applies keywords next to $ref in
		// conjunction with the target. The specs only put annotations
		// there, but honoring the rule costs one recursion.
		siblings := make(map[string]any, len(schema)-1)
		for keyword, constraint := range schema {
			if keyword != "$ref" {
				siblings[keyword] = constraint
			}
		}
		if len(siblings) == 0 {
			return nil
		}
		return d.validate(siblings, value, path)
	}
	if branches, ok := schema["oneOf"].([]any); ok {
		return d.validateOneOf(schema, branches, value, path)
	}
	if typeName, ok := schema["type"].(string); ok {
		if err := checkInstanceType(typeName, value, path); err != nil {
			return err
		}
	}
	if allowed, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range allowed {
			if candidate == value {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: %v is not one of the enum values", path, value)
		}
	}
	if text, ok := value.(string); ok {
		if pattern, ok := schema["pattern"].(string); ok {
			matcher, err := regexp.Compile(pattern)
			if err != nil {
				return fmt.Errorf("%s: pattern %q does not compile: %v", path, pattern, err)
			}
			if !matcher.MatchString(text) {
				return fmt.Errorf("%s: %q does not match pattern %q", path, text, pattern)
			}
		}
		if bound, ok := schema["minLength"].(float64); ok && len(text) < int(bound) {
			return fmt.Errorf("%s: shorter than minLength %d", path, int(bound))
		}
		if bound, ok := schema["maxLength"].(float64); ok && len(text) > int(bound) {
			return fmt.Errorf("%s: longer than maxLength %d", path, int(bound))
		}
	}
	if number, ok := value.(float64); ok {
		if bound, ok := schema["minimum"].(float64); ok && number < bound {
			return fmt.Errorf("%s: %v is below minimum %v", path, number, bound)
		}
		if bound, ok := schema["maximum"].(float64); ok && number > bound {
			return fmt.Errorf("%s: %v is above maximum %v", path, number, bound)
		}
		if bound, ok := schema["exclusiveMinimum"].(float64); ok && number <= bound {
			return fmt.Errorf("%s: %v is not above exclusiveMinimum %v", path, number, bound)
		}
	}
	if object, ok := value.(map[string]any); ok {
		if required, ok := schema["required"].([]any); ok {
			for _, name := range required {
				if _, present := object[name.(string)]; !present {
					return fmt.Errorf("%s: missing required property %q", path, name)
				}
			}
		}
		properties, _ := schema["properties"].(map[string]any)
		for name, raw := range object {
			property, known := properties[name]
			if !known {
				if additional, ok := schema["additionalProperties"].(bool); ok && !additional {
					return fmt.Errorf("%s: unexpected property %q", path, name)
				}
				continue
			}
			if err := d.validate(property.(map[string]any), raw, path+"."+name); err != nil {
				return err
			}
		}
	}
	if elements, ok := value.([]any); ok {
		if items, ok := schema["items"].(map[string]any); ok {
			for i, element := range elements {
				if err := d.validate(items, element, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateOneOf dispatches a tagged union. With a discriminator — the only
// form the specs use — the tag picks exactly one branch, so a failure names
// the intended variant instead of reporting that every branch failed.
// Without one it falls back to the standard exactly-one-branch rule.
func (d *specDoc) validateOneOf(schema map[string]any, branches []any, value any, path string) error {
	if discriminator, ok := schema["discriminator"].(map[string]any); ok {
		propertyName, _ := discriminator["propertyName"].(string)
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: discriminated union requires an object", path)
		}
		tag, ok := object[propertyName].(string)
		if !ok {
			return fmt.Errorf("%s: missing discriminator property %q", path, propertyName)
		}
		mapping, _ := discriminator["mapping"].(map[string]any)
		ref, ok := mapping[tag].(string)
		if !ok {
			return fmt.Errorf("%s: %q %q names no oneOf branch", path, propertyName, tag)
		}
		target, err := d.resolve(ref)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return d.validate(target, value, path)
	}
	matches := 0
	for _, branch := range branches {
		if d.validate(branch.(map[string]any), value, path) == nil {
			matches++
		}
	}
	if matches != 1 {
		return fmt.Errorf("%s: value matches %d oneOf branches, want exactly 1", path, matches)
	}
	return nil
}

// checkInstanceType maps JSON Schema type names onto encoding/json's decoded
// representations. integer is a number with no fractional part, matching how
// the Go int fields marshal.
func checkInstanceType(typeName string, value any, path string) error {
	ok := false
	switch typeName {
	case "object":
		_, ok = value.(map[string]any)
	case "array":
		_, ok = value.([]any)
	case "string":
		_, ok = value.(string)
	case "boolean":
		_, ok = value.(bool)
	case "number":
		_, ok = value.(float64)
	case "integer":
		number, isNumber := value.(float64)
		ok = isNumber && number == math.Trunc(number)
	case "null":
		ok = value == nil
	default:
		return fmt.Errorf("%s: schema uses type %q, outside the supported subset", path, typeName)
	}
	if !ok {
		return fmt.Errorf("%s: value %v is not of type %s", path, value, typeName)
	}
	return nil
}

// propertyNames returns every property name a schema can carry: the
// properties keys of a plain object schema, or the union across all branches
// of a oneOf after $ref resolution. The drift check compares this set against
// the JSON field set of the corresponding Go struct.
func (d *specDoc) propertyNames(t *testing.T, schema map[string]any) map[string]bool {
	t.Helper()
	names := make(map[string]bool)
	if ref, ok := schema["$ref"].(string); ok {
		target, err := d.resolve(ref)
		if err != nil {
			t.Fatalf("%s: %v", d.name, err)
		}
		for name := range d.propertyNames(t, target) {
			names[name] = true
		}
		return names
	}
	if branches, ok := schema["oneOf"].([]any); ok {
		for _, branch := range branches {
			for name := range d.propertyNames(t, branch.(map[string]any)) {
				names[name] = true
			}
		}
		return names
	}
	properties, _ := schema["properties"].(map[string]any)
	for name := range properties {
		names[name] = true
	}
	return names
}

// validateAgainst is the fixture-facing entry point: decode raw JSON and
// validate it against the named component schema.
func (d *specDoc) validateAgainst(t *testing.T, schemaName string, raw []byte, path string) {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("%s: decode: %v", path, err)
	}
	if err := d.validate(d.schema(t, schemaName), value, path); err != nil {
		t.Fatalf("%s: does not match %s schema %s: %v", path, d.name, schemaName, err)
	}
}
