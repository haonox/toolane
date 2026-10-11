package tool

import (
	"context"
	"encoding/json"
	"testing"
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
