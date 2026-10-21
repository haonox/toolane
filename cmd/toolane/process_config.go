package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/haonox/toolane/internal/executor/process"
	"github.com/haonox/toolane/internal/tool"
)

// Process tools are administrator-owned. Only their public description and
// parameter schema are returned to callers; the binding stays private.
type serverProcessToolConfig struct {
	Name           string                  `json:"name"`
	Description    string                  `json:"description"`
	Parameters     json.RawMessage         `json:"parameters"`
	Executable     string                  `json:"executable"`
	Directory      string                  `json:"directory"`
	Environment    map[string]string       `json:"environment,omitempty"`
	Args           []processArgumentConfig `json:"args"`
	QueueTimeoutMS int64                   `json:"queue_timeout_ms,omitempty"`
	RunTimeoutMS   int64                   `json:"run_timeout_ms,omitempty"`
	MaxOutputBytes int                     `json:"max_output_bytes,omitempty"`
	MaxResultBytes int                     `json:"max_result_bytes,omitempty"`
}

type processArgumentConfig struct {
	Literal     *string `json:"literal,omitempty"`
	Parameter   string  `json:"parameter,omitempty"`
	ExecutionID bool    `json:"execution_id,omitempty"`
}

var processToolName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func processToolDefinition(config serverProcessToolConfig) (tool.Definition, error) {
	if !processToolName.MatchString(config.Name) || strings.TrimSpace(config.Description) == "" || len(config.Description) > 2048 {
		return tool.Definition{}, errors.New("process tool needs a valid name and description")
	}
	schema, err := tool.CompileParameters(config.Parameters)
	if err != nil {
		return tool.Definition{}, fmt.Errorf("tool %q parameters: %w", config.Name, err)
	}
	if !filepath.IsAbs(config.Executable) || !filepath.IsAbs(config.Directory) {
		return tool.Definition{}, fmt.Errorf("tool %q requires absolute executable and directory", config.Name)
	}
	info, err := os.Stat(config.Executable)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return tool.Definition{}, fmt.Errorf("tool %q executable must exist and be executable", config.Name)
	}
	info, err = os.Stat(config.Directory)
	if err != nil || !info.IsDir() {
		return tool.Definition{}, fmt.Errorf("tool %q directory must exist", config.Name)
	}
	if len(config.Args) == 0 || len(config.Args) > 64 {
		return tool.Definition{}, fmt.Errorf("tool %q needs 1..64 argument bindings", config.Name)
	}
	for index, arg := range config.Args {
		count := 0
		if arg.Literal != nil {
			count++
			if strings.ContainsRune(*arg.Literal, 0) {
				return tool.Definition{}, fmt.Errorf("tool %q arg %d contains NUL", config.Name, index)
			}
		}
		if arg.Parameter != "" {
			count++
			kind, ok := schema.ParameterType(arg.Parameter)
			if !ok || kind != "string" && kind != "integer" && kind != "number" && kind != "boolean" {
				return tool.Definition{}, fmt.Errorf("tool %q arg %d must reference a required scalar parameter", config.Name, index)
			}
		}
		if arg.ExecutionID {
			count++
		}
		if count != 1 {
			return tool.Definition{}, fmt.Errorf("tool %q arg %d needs exactly one source", config.Name, index)
		}
	}
	if config.QueueTimeoutMS < 0 || config.QueueTimeoutMS > 300000 || config.RunTimeoutMS < 0 || config.RunTimeoutMS > 60000 || config.MaxOutputBytes < 0 || config.MaxOutputBytes > 1024*1024 || config.MaxResultBytes < 0 || config.MaxResultBytes > 64*1024 {
		return tool.Definition{}, fmt.Errorf("tool %q policy exceeds server limits", config.Name)
	}
	environment := make([]string, 0, len(config.Environment))
	for name, value := range config.Environment {
		if !envName.MatchString(name) || strings.ContainsRune(value, 0) {
			return tool.Definition{}, fmt.Errorf("tool %q has an invalid environment entry", config.Name)
		}
		environment = append(environment, name+"="+value)
	}
	sort.Strings(environment)
	bindings := append([]processArgumentConfig(nil), config.Args...)
	buildArgs := func(raw json.RawMessage, options tool.RunOptions) ([]string, error) {
		if err := schema.Validate(raw); err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var values map[string]any
		if err := decoder.Decode(&values); err != nil {
			return nil, err
		}
		argv := make([]string, 0, len(bindings))
		for _, binding := range bindings {
			var value string
			switch {
			case binding.Literal != nil:
				value = *binding.Literal
			case binding.ExecutionID:
				value = options.ExecutionID
			default:
				switch typed := values[binding.Parameter].(type) {
				case string:
					value = typed
				case json.Number:
					value = typed.String()
				case bool:
					value = strconv.FormatBool(typed)
				default:
					return nil, errors.New("unsupported argument value")
				}
			}
			if len(value) > 8192 || strings.ContainsRune(value, 0) {
				return nil, errors.New("process argument exceeds 8192 bytes or contains NUL")
			}
			argv = append(argv, value)
		}
		return argv, nil
	}
	def, err := process.TrustedDefinition(config.Name, process.Spec{Trusted: true, Executable: config.Executable, Directory: config.Directory,
		Environment: environment, ArgsWithOptions: buildArgs, JSONStdout: true})
	if err != nil {
		return tool.Definition{}, err
	}
	def.Description = config.Description
	def.Parameters = append(json.RawMessage(nil), config.Parameters...)
	if config.QueueTimeoutMS > 0 {
		def.Policy.DefaultQueueTimeout = time.Duration(config.QueueTimeoutMS) * time.Millisecond
		def.Policy.MaxQueueTimeout = def.Policy.DefaultQueueTimeout
	}
	if config.RunTimeoutMS > 0 {
		def.Policy.DefaultRunTimeout = time.Duration(config.RunTimeoutMS) * time.Millisecond
		def.Policy.MaxRunTimeout = def.Policy.DefaultRunTimeout
	}
	if config.MaxOutputBytes > 0 {
		def.Policy.DefaultOutputBytes = config.MaxOutputBytes
		def.Policy.MaxOutputBytes = config.MaxOutputBytes
	}
	if config.MaxResultBytes > 0 {
		def.Policy.MaxResultBytes = config.MaxResultBytes
	}
	return def, nil
}
