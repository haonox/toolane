// Package mock provides a deterministic tool for demos and execution tests.
package mock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

const Name = "mock.delay"

type arguments struct {
	DelayMS int64  `json:"delay_ms"`
	Fail    bool   `json:"fail"`
	Value   string `json:"value"`
}

// Definition returns a deterministic, cooperative tool for local demos and tests.
func Definition() tool.Definition {
	return tool.Definition{Name: Name, Validate: validate, Execute: execute}
}

func decode(raw json.RawMessage) (arguments, error) {
	var args arguments
	if len(raw) == 0 {
		return args, errors.New("arguments are required")
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return args, fmt.Errorf("decode mock arguments: %w", err)
	}
	if args.DelayMS < 0 || args.DelayMS > 60_000 {
		return args, errors.New("delay_ms must be between 0 and 60000")
	}
	return args, nil
}

func validate(raw json.RawMessage) error {
	_, err := decode(raw)
	return err
}

func execute(ctx context.Context, raw json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
	args, err := decode(raw)
	if err != nil {
		return tool.Outcome{}, err
	}
	timer := time.NewTimer(time.Duration(args.DelayMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return tool.Outcome{}, ctx.Err()
	case <-timer.C:
	}
	if err := ctx.Err(); err != nil {
		return tool.Outcome{}, err
	}
	if args.Fail {
		return tool.Outcome{}, errors.New("mock requested failure")
	}
	result, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: args.Value})
	return tool.Outcome{Result: result}, err
}
