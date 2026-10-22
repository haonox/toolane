package tool

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRegistryRejectsDuplicateAndReturnsSnapshot(t *testing.T) {
	r := NewRegistry()
	def := Definition{
		Name:     "sample",
		Validate: func(json.RawMessage) error { return nil },
		Execute:  func(context.Context, json.RawMessage, RunOptions) (Outcome, error) { return Outcome{}, nil },
	}
	if err := r.Register(def); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(def); err == nil {
		t.Fatal("duplicate tool was accepted")
	}
	snapshot := r.Snapshot()
	delete(snapshot, def.Name)
	if _, ok := r.Snapshot()[def.Name]; !ok {
		t.Fatal("snapshot deletion changed registry")
	}
}

func TestRegistryRejectsInvalidToolPolicy(t *testing.T) {
	for _, policy := range []Policy{
		{MaxArgumentBytes: -1},
		{DefaultQueueTimeout: 2 * time.Second, MaxQueueTimeout: time.Second},
		{DefaultRunTimeout: 2 * time.Second, MaxRunTimeout: time.Second},
		{DefaultOutputBytes: 8, MaxOutputBytes: 4},
	} {
		r := NewRegistry()
		err := r.Register(Definition{
			Name:     "sample",
			Policy:   policy,
			Validate: func(json.RawMessage) error { return nil },
			Execute:  func(context.Context, json.RawMessage, RunOptions) (Outcome, error) { return Outcome{}, nil },
		})
		if err == nil {
			t.Fatalf("invalid policy accepted: %+v", policy)
		}
	}
}
