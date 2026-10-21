package tool

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCompiledParametersValidateSupportedSchema(t *testing.T) {
	schema, err := CompileParameters(json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"value":{"type":"string","minLength":1,"maxLength":4},"count":{"type":"integer","minimum":1,"maximum":3},"options":{"type":"array","items":{"type":"boolean"}}},"required":["value","count"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if kind, ok := schema.ParameterType("count"); !ok || kind != "integer" {
		t.Fatalf("count binding: %q %v", kind, ok)
	}
	for _, input := range []string{`{"value":"ok","count":2}`, `{"value":"好","count":3,"options":[true,false]}`} {
		if err := schema.Validate(json.RawMessage(input)); err != nil {
			t.Fatalf("valid %s: %v", input, err)
		}
	}
	for _, input := range []string{`{}`, `{"value":"","count":2}`, `{"value":"hello","count":2}`, `{"value":"ok","count":0}`, `{"value":"ok","count":1.5}`, `{"value":"ok","count":2,"other":1}`, `{"value":"ok","count":2,"options":[1]}`, `{"value":"ok","count":2} {}`} {
		if err := schema.Validate(json.RawMessage(input)); err == nil {
			t.Fatalf("invalid arguments accepted: %s", input)
		}
	}
}

func TestCompiledParametersRejectUnsupportedOrAmbiguousSchema(t *testing.T) {
	for _, raw := range []string{
		`{"type":"object","properties":{},"additionalProperties":true}`,
		`{"type":"object","properties":{},"additionalProperties":null}`,
		`{"type":"object","properties":{},"additionalProperties":false,"required":["missing"]}`,
		`{"type":"object","properties":{},"additionalProperties":false,"required":null}`,
		`{"type":"object","properties":{"x":{"type":"string","pattern":"a"}},"additionalProperties":false}`,
		`{"type":"object","properties":{"x":{"type":"integer","minimum":2,"maximum":1}},"additionalProperties":false}`,
		`{"type":"array","items":{"type":"string"}}`,
		`{"type":"object","properties":{"x":{"type":"string","minLength":5,"maxLength":2}},"additionalProperties":false}`,
	} {
		if _, err := CompileParameters(json.RawMessage(raw)); err == nil {
			t.Fatalf("unsupported schema accepted: %s", raw)
		}
	}
	deep := `{"type":"string"}`
	for range 10 {
		deep = `{"type":"array","items":` + deep + `}`
	}
	deep = `{"type":"object","properties":{"x":` + deep + `},"additionalProperties":false}`
	if _, err := CompileParameters(json.RawMessage(deep)); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("deep schema: %v", err)
	}
}
