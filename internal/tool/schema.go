package tool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"unicode/utf8"
)

// CompiledParameters is the deliberately small JSON Schema subset supported
// by administrator-configured Process tools. Unsupported keywords fail at
// startup instead of being silently ignored during admission.
type CompiledParameters struct{ root *schemaNode }

type schemaNode struct {
	kind                 string
	properties           map[string]*schemaNode
	required             map[string]bool
	items                *schemaNode
	minimum, maximum     *big.Rat
	minLength, maxLength *int
}

func CompileParameters(raw json.RawMessage) (*CompiledParameters, error) {
	root, err := parseSchema(raw, 0)
	if err != nil {
		return nil, err
	}
	if root.kind != "object" {
		return nil, errors.New("parameters must be an object schema")
	}
	return &CompiledParameters{root: root}, nil
}

func (c *CompiledParameters) ParameterType(name string) (string, bool) {
	if c == nil {
		return "", false
	}
	field, ok := c.root.properties[name]
	if !ok || !c.root.required[name] {
		return "", false
	}
	return field.kind, true
}

func (c *CompiledParameters) Validate(raw json.RawMessage) error {
	if c == nil {
		return errors.New("missing parameter schema")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode arguments: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("arguments must contain one JSON value")
	}
	return c.root.validate(value, "arguments")
}

func parseSchema(raw json.RawMessage, depth int) (*schemaNode, error) {
	if depth > 8 {
		return nil, errors.New("schema nesting exceeds 8 levels")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("schema must be an object")
	}
	for key := range fields {
		switch key {
		case "type", "description", "properties", "required", "additionalProperties", "items", "minimum", "maximum", "minLength", "maxLength":
		default:
			return nil, fmt.Errorf("unsupported schema keyword %q", key)
		}
	}
	var kind string
	if err := json.Unmarshal(fields["type"], &kind); err != nil {
		return nil, errors.New("schema type must be a string")
	}
	node := &schemaNode{kind: kind}
	switch kind {
	case "object":
		if len(fields["items"]) > 0 || len(fields["minimum"]) > 0 || len(fields["maximum"]) > 0 || len(fields["minLength"]) > 0 || len(fields["maxLength"]) > 0 {
			return nil, errors.New("object schema has incompatible keywords")
		}
		if string(fields["additionalProperties"]) != "false" {
			return nil, errors.New("object schema requires additionalProperties: false")
		}
		if len(fields["properties"]) == 0 {
			return nil, errors.New("object schema requires properties")
		}
		var properties map[string]json.RawMessage
		if err := json.Unmarshal(fields["properties"], &properties); err != nil || properties == nil {
			return nil, errors.New("properties must be an object")
		}
		node.properties = make(map[string]*schemaNode, len(properties))
		for name, childRaw := range properties {
			if name == "" {
				return nil, errors.New("property name cannot be empty")
			}
			child, err := parseSchema(childRaw, depth+1)
			if err != nil {
				return nil, fmt.Errorf("property %q: %w", name, err)
			}
			node.properties[name] = child
		}
		node.required = map[string]bool{}
		if len(fields["required"]) > 0 {
			if bytes.Equal(bytes.TrimSpace(fields["required"]), []byte("null")) {
				return nil, errors.New("required must be a string array")
			}
			var required []string
			if err := json.Unmarshal(fields["required"], &required); err != nil {
				return nil, errors.New("required must be a string array")
			}
			for _, name := range required {
				if _, ok := properties[name]; !ok || node.required[name] {
					return nil, fmt.Errorf("invalid required property %q", name)
				}
				node.required[name] = true
			}
		}
	case "array":
		if len(fields["properties"]) > 0 || len(fields["required"]) > 0 || len(fields["additionalProperties"]) > 0 || len(fields["minimum"]) > 0 || len(fields["maximum"]) > 0 || len(fields["minLength"]) > 0 || len(fields["maxLength"]) > 0 {
			return nil, errors.New("array schema has incompatible keywords")
		}
		if len(fields["items"]) == 0 {
			return nil, errors.New("array schema requires items")
		}
		var err error
		node.items, err = parseSchema(fields["items"], depth+1)
		if err != nil {
			return nil, err
		}
	case "string":
		if err := rejectContainerKeywords(fields); err != nil {
			return nil, err
		}
		var err error
		node.minLength, err = parseNonnegativeInt(fields["minLength"])
		if err != nil {
			return nil, fmt.Errorf("minLength: %w", err)
		}
		node.maxLength, err = parseNonnegativeInt(fields["maxLength"])
		if err != nil {
			return nil, fmt.Errorf("maxLength: %w", err)
		}
		if node.minLength != nil && node.maxLength != nil && *node.minLength > *node.maxLength {
			return nil, errors.New("minLength exceeds maxLength")
		}
		if len(fields["minimum"]) > 0 || len(fields["maximum"]) > 0 {
			return nil, errors.New("string schema has numeric bounds")
		}
	case "integer", "number":
		if err := rejectContainerKeywords(fields); err != nil {
			return nil, err
		}
		if len(fields["minLength"]) > 0 || len(fields["maxLength"]) > 0 {
			return nil, errors.New("number schema has string bounds")
		}
		var err error
		node.minimum, err = parseBound(fields["minimum"])
		if err != nil {
			return nil, fmt.Errorf("minimum: %w", err)
		}
		node.maximum, err = parseBound(fields["maximum"])
		if err != nil {
			return nil, fmt.Errorf("maximum: %w", err)
		}
		if node.minimum != nil && node.maximum != nil && node.minimum.Cmp(node.maximum) > 0 {
			return nil, errors.New("minimum exceeds maximum")
		}
	case "boolean", "null":
		if err := rejectContainerKeywords(fields); err != nil {
			return nil, err
		}
		if len(fields["minimum"]) > 0 || len(fields["maximum"]) > 0 || len(fields["minLength"]) > 0 || len(fields["maxLength"]) > 0 {
			return nil, errors.New("schema has incompatible bounds")
		}
	default:
		return nil, fmt.Errorf("unsupported schema type %q", kind)
	}
	if len(fields["description"]) > 0 {
		var description string
		if err := json.Unmarshal(fields["description"], &description); err != nil || bytes.Equal(bytes.TrimSpace(fields["description"]), []byte("null")) {
			return nil, errors.New("description must be a string")
		}
	}
	return node, nil
}

func rejectContainerKeywords(fields map[string]json.RawMessage) error {
	for _, key := range []string{"properties", "required", "additionalProperties", "items"} {
		if len(fields[key]) > 0 {
			return fmt.Errorf("%s is not valid for this type", key)
		}
	}
	return nil
}

func parseNonnegativeInt(raw json.RawMessage) (*int, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("must be a nonnegative integer")
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
		return nil, errors.New("must be a nonnegative integer")
	}
	return &value, nil
}

func parseBound(raw json.RawMessage) (*big.Rat, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value json.Number
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, errors.New("must be a number")
	}
	return rational(value.String())
}

func rational(s string) (*big.Rat, error) {
	if len(s) > 1024 {
		return nil, errors.New("number has too many digits")
	}
	parts := strings.SplitN(strings.ToLower(s), "e", 2)
	value, ok := new(big.Rat).SetString(parts[0])
	if !ok {
		return nil, errors.New("invalid number")
	}
	if len(parts) == 2 {
		var exponent int
		if _, err := fmt.Sscan(parts[1], &exponent); err != nil || exponent < -1000 || exponent > 1000 {
			return nil, errors.New("number exponent is outside supported range")
		}
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exponent))), nil)
		if exponent >= 0 {
			value.Mul(value, new(big.Rat).SetInt(factor))
		} else {
			value.Quo(value, new(big.Rat).SetInt(factor))
		}
	}
	return value, nil
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func (s *schemaNode) validate(value any, path string) error {
	switch s.kind {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		for name := range s.required {
			if _, ok := object[name]; !ok {
				return fmt.Errorf("%s.%s is required", path, name)
			}
		}
		for name, childValue := range object {
			child := s.properties[name]
			if child == nil {
				return fmt.Errorf("%s.%s is not allowed", path, name)
			}
			if err := child.validate(childValue, path+"."+name); err != nil {
				return err
			}
		}
	case "array":
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s must be an array", path)
		}
		for index, item := range array {
			if err := s.items.validate(item, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	case "string":
		stringValue, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s must be a string", path)
		}
		length := utf8.RuneCountInString(stringValue)
		if s.minLength != nil && length < *s.minLength || s.maxLength != nil && length > *s.maxLength {
			return fmt.Errorf("%s length is outside allowed range", path)
		}
	case "integer", "number":
		number, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("%s must be a %s", path, s.kind)
		}
		rat, err := rational(number.String())
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if s.kind == "integer" && !rat.IsInt() {
			return fmt.Errorf("%s must be an integer", path)
		}
		if s.minimum != nil && rat.Cmp(s.minimum) < 0 || s.maximum != nil && rat.Cmp(s.maximum) > 0 {
			return fmt.Errorf("%s is outside allowed range", path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case "null":
		if value != nil {
			return fmt.Errorf("%s must be null", path)
		}
	}
	return nil
}
