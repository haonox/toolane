package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/executor/process"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		if len(os.Args) != 4 || os.Args[2] != "--config" {
			fmt.Fprintln(os.Stderr, "usage: toolane serve --config /absolute/path/config.json")
			os.Exit(2)
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runServer(ctx, os.Args[3]); err != nil {
			slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("service_failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 5 || os.Args[1] != "demo" || os.Args[3] != "--state-dir" || !filepath.IsAbs(os.Args[4]) {
		fmt.Fprintln(os.Stderr, "usage: toolane demo [mock|process] --state-dir /absolute/path/state or toolane serve --config /absolute/path/config.json")
		os.Exit(2)
	}
	if err := demo(os.Args[2], os.Args[4]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func demo(kind, stateDir string) (retErr error) {
	registry := tool.NewRegistry()
	var def tool.Definition
	var args json.RawMessage
	switch kind {
	case "mock":
		def = mock.Definition()
		args = json.RawMessage(`{"delay_ms":100,"value":"hello from Toolane"}`)
	case "process":
		// Look for the "printf" executable in the system's PATH
		path, err := exec.LookPath("printf")
		fmt.Printf("using %s for process demo\n", path)
		if err != nil {
			return err
		}
		path, err = filepath.Abs(path)
		fmt.Printf("absolute path: %s\n", path)
		if err != nil {
			return err
		}
		// Define a trusted process tool that runs the "printf" command with a JSON argument
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
	// Register the tool definition in the registry
	if err := registry.Register(def); err != nil {
		return err
	}
	// The caller chooses the state directory so demo history remains available.
	store, err := sqlite.Open(context.Background(), stateDir)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, store.Close()) }()
	manager, err := execution.NewManager(context.Background(), execution.DefaultConfig(), registry, store)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, manager.Close(context.Background())) }()
	// Submit an execution request for the tool with the specified arguments
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
