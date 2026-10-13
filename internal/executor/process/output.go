package process

import (
	"strings"
	"sync"

	"github.com/haonox/toolane/internal/tool"
)

// outputBudget counts original bytes from both pipes before text conversion.
// exec.Cmd writes to the two stream writers concurrently.
type outputBudget struct {
	mu        sync.Mutex
	remaining int
	stdout    []byte
	stderr    []byte
	truncated bool
	once      sync.Once
	onLimit   func()
}

func newOutputBudget(options tool.RunOptions) *outputBudget {
	return &outputBudget{remaining: options.MaxOutputBytes, onLimit: options.OnOutputLimit}
}

type streamWriter struct {
	budget *outputBudget
	stderr bool
}

func (w streamWriter) Write(p []byte) (int, error) {
	b := w.budget
	b.mu.Lock()
	n := len(p)
	if n > b.remaining {
		n = b.remaining
	}
	if w.stderr {
		b.stderr = append(b.stderr, p[:n]...)
	} else {
		b.stdout = append(b.stdout, p[:n]...)
	}
	b.remaining -= n
	reached := b.remaining == 0
	if reached {
		b.truncated = true
	}
	b.mu.Unlock()
	if reached {
		b.once.Do(func() {
			if b.onLimit != nil {
				b.onLimit()
			}
		})
	}
	// Continue draining while the process stops; bytes above budget are dropped.
	return len(p), nil
}

func (b *outputBudget) outcome() tool.Outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	return tool.Outcome{
		Stdout:          strings.ToValidUTF8(string(b.stdout), "?"),
		Stderr:          strings.ToValidUTF8(string(b.stderr), "?"),
		OutputTruncated: b.truncated,
	}
}
