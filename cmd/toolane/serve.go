package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/httpapi"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

type serverConfig struct {
	Listen   string          `json:"listen"`
	StateDir string          `json:"state_dir"`
	Tokens   []httpapi.Token `json:"tokens"`
}

func loadServerConfig(path string) (serverConfig, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return serverConfig{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64*1024 {
		return serverConfig{}, errors.New("server config must be a private regular file of at most 64 KiB")
	}
	file, err := os.Open(path)
	if err != nil {
		return serverConfig{}, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return serverConfig{}, err
	}
	if !os.SameFile(info, openedInfo) || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm()&0077 != 0 {
		return serverConfig{}, errors.New("server config changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err != nil {
		return serverConfig{}, err
	}
	if len(raw) > 64*1024 {
		return serverConfig{}, errors.New("server config exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config serverConfig
	if err := decoder.Decode(&config); err != nil {
		return serverConfig{}, fmt.Errorf("decode server config: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return serverConfig{}, errors.New("server config must contain one JSON object")
	}
	if config.Listen == "" {
		config.Listen = "127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(config.Listen)
	if err != nil || host != "localhost" && (net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback()) {
		return serverConfig{}, errors.New("listen address must use localhost or a loopback IP and a port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return serverConfig{}, errors.New("invalid listen port")
	}
	if !filepath.IsAbs(config.StateDir) || len(config.Tokens) == 0 {
		return serverConfig{}, errors.New("absolute state_dir and tokens are required")
	}
	for _, token := range config.Tokens {
		for _, name := range token.Tools {
			if name != mock.Name {
				return serverConfig{}, errors.New("local server currently supports only mock.delay")
			}
		}
	}
	return config, nil
}

// runServer offers a local, Mock-only durable service. Other backends need an
// explicit recovery callback before they can be enabled for HTTP admission.
func runServer(ctx context.Context, configPath string) (retErr error) {
	config, err := loadServerConfig(configPath)
	if err != nil {
		return err
	}
	registry := tool.NewRegistry()
	if err := registry.Register(mock.Definition()); err != nil {
		return err
	}
	store, err := sqlite.Open(ctx, config.StateDir)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, store.Close()) }()
	manager, err := execution.NewDurableManager(ctx, execution.DefaultConfig(), registry, store, func(_ context.Context, resources []execution.RecoveryResource) error {
		if len(resources) > 0 {
			return errors.New("Docker resources require a configured recovery backend")
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, manager.Close(closeCtx))
	}()
	api, err := httpapi.New(manager, config.Tokens)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: api, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 * 1024}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	fmt.Printf("Toolane HTTP ready at %s\n", listener.Addr())
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, server.Shutdown(shutdownCtx))
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			retErr = errors.Join(retErr, err)
		}
	}
	return retErr
}
