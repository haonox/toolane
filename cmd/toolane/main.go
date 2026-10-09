package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/tool"
)

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func demo() error {
	registry := tool.NewRegistry()
	if err := registry.Register(mock.Definition()); err != nil {
		return err
	}
	manager, err := execution.NewManager(execution.DefaultConfig(), registry)
	if err != nil {
		return err
	}
	defer manager.Close(context.Background())
	initial, err := manager.Submit(context.Background(), execution.Request{
		Tool:      mock.Name,
		AgentID:   "demo-agent",
		Arguments: json.RawMessage(`{"delay_ms":100,"value":"hello from Toolane"}`),
	})
	if err != nil {
		return err
	}
	fmt.Printf("accepted %s (%s)\n", initial.ID, initial.State)
	deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := manager.Get(initial.ID)
		if err != nil {
			return err
		}
		if snapshot.FinishedAt != nil {
			encoded, err := json.MarshalIndent(snapshot, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(encoded))
			return nil
		}
		select {
		case <-ticker.C:
		case <-deadline.Done():
			return deadline.Err()
		}
	}
}
