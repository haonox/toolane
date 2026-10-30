package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareConfigReuseAndDiagnostics(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"config", "tools"} {
		if err := os.Mkdir(filepath.Join(root, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, script := range []string{"text.py", "io.py"} {
		if err := os.WriteFile(filepath.Join(root, "tools", script), []byte("print('{}')\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	profile := `{"business_listen":"127.0.0.1:18080","tenant_count":2,"execution":{"workers":1},"process_tools":[{"name":"bench.text","script":"tools/text.py","args":[{"parameter":"text"}]}]}`
	writeTestFile(t, filepath.Join(root, "config", "profile.json"), profile)
	writeTestFile(t, filepath.Join(root, "config", "diagnostic-tools.json"), `{"process_tools":[{"name":"bench.io","script":"tools/io.py","args":[]}]}`)
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}

	first, err := prepare(root, false, python)
	if err != nil || first.reused || first.backup != "" {
		t.Fatalf("first prepare: %+v, %v", first, err)
	}
	var config struct {
		Tokens []token `json:"tokens"`
		Tools  []struct {
			Name        string              `json:"name"`
			Executable  string              `json:"executable"`
			Directory   string              `json:"directory"`
			Environment map[string]string   `json:"environment"`
			Args        []map[string]string `json:"args"`
		} `json:"process_tools"`
	}
	loadTestJSON(t, first.path, &config)
	if len(config.Tokens) != 2 || config.Tokens[0].TenantID != "bench-0001" ||
		config.Tokens[0].Token == config.Tokens[1].Token || len(config.Tokens[0].Token) != 43 ||
		len(config.Tokens[0].Tools) != 1 || config.Tokens[0].Tools[0] != "bench.text" {
		t.Fatalf("invalid tokens: %+v", config.Tokens)
	}
	if len(config.Tools) != 1 || config.Tools[0].Executable != python || config.Tools[0].Directory != root ||
		config.Tools[0].Environment["PYTHONDONTWRITEBYTECODE"] != "1" ||
		config.Tools[0].Args[0]["literal"] != filepath.Join(root, "tools", "text.py") ||
		config.Tools[0].Args[1]["parameter"] != "text" {
		t.Fatalf("invalid tool: %+v", config.Tools)
	}
	info, err := os.Stat(first.path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private config permissions: %v, %v", info, err)
	}

	second, err := prepare(root, false, python)
	if err != nil || !second.reused || second.backup != "" {
		t.Fatalf("reuse: %+v, %v", second, err)
	}
	var reused struct {
		Tokens []token `json:"tokens"`
	}
	loadTestJSON(t, second.path, &reused)
	if reused.Tokens[0].Token != config.Tokens[0].Token {
		t.Fatal("reuse changed tenant token")
	}

	third, err := prepare(root, true, python)
	if err != nil || third.reused || third.backup == "" {
		t.Fatalf("diagnostics: %+v, %v", third, err)
	}
	loadTestJSON(t, third.path, &config)
	if len(config.Tools) != 2 || len(config.Tokens[0].Tools) != 2 ||
		config.Tokens[0].Token == reused.Tokens[0].Token {
		t.Fatalf("diagnostic config was not regenerated: %+v", config)
	}
	fixture := filepath.Join(root, "generated", "io-fixture.sqlite")
	if err := verifyIOFixture(fixture); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(fixture)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("fixture permissions: %v, %v", info, err)
	}
	backup := struct {
		Tokens []token `json:"tokens"`
	}{}
	loadTestJSON(t, third.backup, &backup)
	if backup.Tokens[0].Token != reused.Tokens[0].Token {
		t.Fatal("backup does not contain previous tokens")
	}

	database, err := sql.Open("sqlite3", fixture)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var payload string
	if err := database.QueryRow("SELECT payload FROM records WHERE id = 1").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != strings.Repeat("00000001", 128) {
		t.Fatal("fixture payload differs from the Python preparer")
	}
}

func TestPrepareRejectsScriptOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.py")
	writeTestFile(t, outside, "")
	if err := os.Mkdir(filepath.Join(root, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	profile := `{"tenant_count":1,"process_tools":[{"name":"outside","script":` + string(mustJSON(outside)) + `,"args":[]}]}`
	writeTestFile(t, filepath.Join(root, "config", "profile.json"), profile)
	python, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepare(root, false, python); err == nil || !strings.Contains(err.Error(), "inside benchmarks") {
		t.Fatalf("expected script boundary error, got %v", err)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func loadTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, value); err != nil {
		t.Fatal(err)
	}
}
