//go:build !linux

package process

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/haonox/toolane/internal/tool"
)

func run(context.Context, json.RawMessage, tool.RunOptions, Spec) (tool.Outcome, error) {
	return tool.Outcome{}, &tool.RunError{Code: "process_unsupported", Err: errors.New("process executor requires Linux")}
}
