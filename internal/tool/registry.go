package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RunOptions are effective limits owned by the manager. OnOutputLimit must be
// called synchronously when the shared output budget is reached.
type RunOptions struct {
	MaxOutputBytes   int
	OnOutputLimit    func()
	OnProcessStarted func(time.Time)
}

// Outcome contains executor facts. The manager alone decides the final state.
type Outcome struct {
	Result           json.RawMessage
	Stdout           string
	Stderr           string
	ExitCode         *int
	OutputTruncated  bool
	ProcessStartedAt *time.Time
	StopUnconfirmed  bool
}

// RunError gives a stable code for an executor failure without making the
// manager depend on a particular backend package.
type RunError struct {
	Code string
	Err  error
}

func (e *RunError) Error() string {
	if e == nil {
		return "execution error"
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code
}
func (e *RunError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Policy narrows the manager's global limits for one registered tool. Zero
// values inherit the global limit or default. A tool maximum cannot exceed
// the corresponding manager maximum.
type Policy struct {
	DefaultQueueTimeout time.Duration
	MaxQueueTimeout     time.Duration
	DefaultRunTimeout   time.Duration
	MaxRunTimeout       time.Duration
	DefaultOutputBytes  int
	MaxOutputBytes      int
	MaxArgumentBytes    int
	MaxResultBytes      int
}

func (p Policy) validate() error {
	if p.DefaultQueueTimeout < 0 || p.MaxQueueTimeout < 0 || p.DefaultRunTimeout < 0 || p.MaxRunTimeout < 0 ||
		p.DefaultOutputBytes < 0 || p.MaxOutputBytes < 0 || p.MaxArgumentBytes < 0 || p.MaxResultBytes < 0 {
		return errors.New("tool policy limits cannot be negative")
	}
	if p.MaxQueueTimeout > 0 && p.DefaultQueueTimeout > p.MaxQueueTimeout ||
		p.MaxRunTimeout > 0 && p.DefaultRunTimeout > p.MaxRunTimeout ||
		p.MaxOutputBytes > 0 && p.DefaultOutputBytes > p.MaxOutputBytes {
		return errors.New("tool policy default exceeds its maximum")
	}
	return nil
}

// Definition is a server-owned tool. Validate runs before admission; Execute
// must return only after its work has stopped when its context is cancelled.
type Definition struct {
	Name     string
	Policy   Policy
	Validate func(json.RawMessage) error
	Execute  func(context.Context, json.RawMessage, RunOptions) (Outcome, error)
}

// Registry stores explicit tool registrations. A Manager takes a snapshot of
// the registry at construction, so later registrations do not affect it.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]Definition
}

func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]Definition)}
}

// Register rejects incomplete or duplicate definitions.
func (r *Registry) Register(def Definition) error {
	if r == nil {
		return errors.New("nil registry")
	}
	if def.Name == "" || def.Validate == nil || def.Execute == nil {
		return errors.New("tool requires name, validator and executor")
	}
	if err := def.Policy.validate(); err != nil {
		return fmt.Errorf("tool %q: %w", def.Name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[def.Name]; exists {
		return fmt.Errorf("tool %q already registered", def.Name)
	}
	r.tools[def.Name] = def
	return nil
}

// Snapshot returns an independent map of registered definitions.
func (r *Registry) Snapshot() map[string]Definition {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyOf := make(map[string]Definition, len(r.tools))
	for name, def := range r.tools {
		copyOf[name] = def
	}
	return copyOf
}
