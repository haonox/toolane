package execution

import (
	"errors"
	"fmt"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

// effectivePolicy is frozen when the manager takes its registry snapshot.
// Global limits remain hard ceilings; a tool can only narrow them.
type effectivePolicy struct {
	defaultQueueTimeout time.Duration
	maxQueueTimeout     time.Duration
	defaultRunTimeout   time.Duration
	maxRunTimeout       time.Duration
	defaultOutputBytes  int
	maxOutputBytes      int
	maxArgumentBytes    int
	maxResultBytes      int
}

func resolveToolPolicy(p tool.Policy, c Config) (effectivePolicy, error) {
	var result effectivePolicy
	var err error
	if result.maxQueueTimeout, err = durationMaximum(p.MaxQueueTimeout, c.MaxQueueTimeout); err != nil {
		return result, fmt.Errorf("queue timeout: %w", err)
	}
	if result.maxRunTimeout, err = durationMaximum(p.MaxRunTimeout, c.MaxRunTimeout); err != nil {
		return result, fmt.Errorf("execution timeout: %w", err)
	}
	if result.maxOutputBytes, err = byteMaximum(p.MaxOutputBytes, c.MaxOutputBytes); err != nil {
		return result, fmt.Errorf("output: %w", err)
	}
	if result.maxArgumentBytes, err = byteMaximum(p.MaxArgumentBytes, c.MaxArgumentBytes); err != nil {
		return result, fmt.Errorf("arguments: %w", err)
	}
	if result.maxResultBytes, err = byteMaximum(p.MaxResultBytes, c.MaxResultBytes); err != nil {
		return result, fmt.Errorf("result: %w", err)
	}
	if result.defaultQueueTimeout, err = durationDefault(p.DefaultQueueTimeout, c.DefaultQueueTimeout, result.maxQueueTimeout); err != nil {
		return result, fmt.Errorf("queue timeout: %w", err)
	}
	if result.defaultRunTimeout, err = durationDefault(p.DefaultRunTimeout, c.DefaultRunTimeout, result.maxRunTimeout); err != nil {
		return result, fmt.Errorf("execution timeout: %w", err)
	}
	if result.defaultOutputBytes, err = byteDefault(p.DefaultOutputBytes, c.MaxOutputBytes, result.maxOutputBytes); err != nil {
		return result, fmt.Errorf("output: %w", err)
	}
	return result, nil
}

func durationMaximum(given, global time.Duration) (time.Duration, error) {
	if given < 0 || given > global {
		return 0, errors.New("maximum outside global limit")
	}
	if given == 0 {
		return global, nil
	}
	return given, nil
}

func byteMaximum(given, global int) (int, error) {
	if given < 0 || given > global {
		return 0, errors.New("maximum outside global limit")
	}
	if given == 0 {
		return global, nil
	}
	return given, nil
}

func durationDefault(given, global, maximum time.Duration) (time.Duration, error) {
	if given < 0 || given > maximum {
		return 0, errors.New("default outside tool limit")
	}
	if given > 0 {
		return given, nil
	}
	return min(global, maximum), nil
}

func byteDefault(given, global, maximum int) (int, error) {
	if given < 0 || given > maximum {
		return 0, errors.New("default outside tool limit")
	}
	if given > 0 {
		return given, nil
	}
	return min(global, maximum), nil
}
