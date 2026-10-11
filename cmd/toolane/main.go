package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/executor/process"
	"github.com/haonox/toolane/internal/tool"
)

func main() {
	kind := "mock"
	if len(os.Args) == 2 {
		kind = os.Args[1]
	} else if len(os.Args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: toolane [mock|process]")
		os.Exit(2)
	}
	if err := demo(kind); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func demo(kind string) (retErr error) {
	registry := tool.NewRegistry()
	var def tool.Definition
	var args json.RawMessage
	switch kind {
	case "mock":
		def = mock.Definition()
		args = json.RawMessage(`{"delay_ms":100,"value":"hello from Toolane"}`)
	case "process":
		path, err := exec.LookPath("printf")
		if err != nil {
			return err
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return err
		}
		def, err = process.TrustedDefinition("process.printf", process.Spec{
			Trusted:    true,
			Executable: path,
			Args: func(raw json.RawMessage) ([]string, error) {
				var input struct {
					Text string `json:"text"`
				}
				if err := json.Unmarshal(raw, &input); err != nil {
					return nil, err
				}
				return []string{"%s\n", input.Text}, nil
			},
		})
		if err != nil {
			return err
		}
		args = json.RawMessage(`{"text":"hello from Process Executor"}`)
	default:
		return fmt.Errorf("unknown demo %q (choose mock or process)", kind)
	}
	if err := registry.Register(def); err != nil {
		return err
	}
	manager, err := execution.NewManager(execution.DefaultConfig(), registry)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, manager.Close(context.Background())) }()
	initial, err := manager.Submit(context.Background(), execution.Request{
		Tool:      def.Name,
		AgentID:   "demo-agent",
		Arguments: args,
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
			if snapshot.State != execution.Succeeded {
				return fmt.Errorf("demo ended in %s: %s", snapshot.State, snapshot.ErrorCode)
			}
			return nil
		}
		select {
		case <-ticker.C:
		case <-deadline.Done():
			return deadline.Err()
		}
	}
}
