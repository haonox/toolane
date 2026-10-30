// Command prepare creates a private Toolane benchmark configuration.
package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type object map[string]json.RawMessage

type toolTemplate struct {
	Name   string            `json:"name"`
	Script string            `json:"script"`
	Args   []json.RawMessage `json:"args"`
}

type token struct {
	TenantID string   `json:"tenant_id"`
	Token    string   `json:"token"`
	Tools    []string `json:"tools"`
}

type preparation struct {
	path   string
	backup string
	reused bool
	count  int
}

func main() {
	root := flag.String("root", "benchmarks", "benchmark directory containing config/ and tools/")
	diagnostics := flag.Bool("diagnostics", false, "include optional diagnostic tools and SQLite fixture")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "prepare accepts no positional arguments")
		os.Exit(2)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		fmt.Fprintln(os.Stderr, "find python3:", err)
		os.Exit(1)
	}
	result, err := prepare(*root, *diagnostics, python)
	if err != nil {
		fmt.Fprintln(os.Stderr, "prepare benchmark config:", err)
		os.Exit(1)
	}
	if result.backup != "" {
		fmt.Println("Saved previous private config as", result.backup)
	}
	if result.reused {
		fmt.Printf("Using existing %s with %d private tenant tokens\n", result.path, result.count)
	} else {
		fmt.Printf("Created %s with %d private tenant tokens\n", result.path, result.count)
	}
}

func prepare(root string, diagnostics bool, python string) (preparation, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return preparation{}, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return preparation{}, fmt.Errorf("resolve benchmark directory: %w", err)
	}
	python, err = filepath.Abs(python)
	if err != nil {
		return preparation{}, err
	}
	profile, err := readObject(filepath.Join(root, "config", "profile.json"))
	if err != nil {
		return preparation{}, err
	}
	var count int
	if err := json.Unmarshal(profile["tenant_count"], &count); err != nil || count < 1 || count > 10000 {
		return preparation{}, errors.New("tenant_count must be between 1 and 10000")
	}
	var tools []object
	if err := json.Unmarshal(profile["process_tools"], &tools); err != nil || len(tools) == 0 {
		return preparation{}, errors.New("process_tools must be a nonempty list")
	}
	if diagnostics {
		extra, err := readObject(filepath.Join(root, "config", "diagnostic-tools.json"))
		if err != nil {
			return preparation{}, err
		}
		var more []object
		if err := json.Unmarshal(extra["process_tools"], &more); err != nil || len(more) == 0 {
			return preparation{}, errors.New("diagnostic process_tools must be a nonempty list")
		}
		tools = append(tools, more...)
	}
	delete(profile, "tenant_count")
	delete(profile, "process_tools")

	names := make([]string, 0, len(tools))
	seen := make(map[string]bool, len(tools))
	configured := make([]object, 0, len(tools))
	for _, item := range tools {
		var template toolTemplate
		raw, err := json.Marshal(item)
		if err != nil || json.Unmarshal(raw, &template) != nil || template.Name == "" || template.Script == "" || seen[template.Name] {
			return preparation{}, errors.New("process tool names must be unique and each tool needs a script")
		}
		seen[template.Name] = true
		names = append(names, template.Name)
		scriptPath := template.Script
		if !filepath.IsAbs(scriptPath) {
			scriptPath = filepath.Join(root, scriptPath)
		}
		script, err := filepath.EvalSymlinks(scriptPath)
		if err != nil {
			return preparation{}, fmt.Errorf("resolve tool script %q: %w", template.Script, err)
		}
		relative, err := filepath.Rel(root, script)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return preparation{}, fmt.Errorf("tool script must be inside benchmarks: %s", script)
		}
		info, err := os.Stat(script)
		if err != nil || !info.Mode().IsRegular() {
			return preparation{}, fmt.Errorf("tool script must be a file: %s", script)
		}
		delete(item, "script")
		set(item, "executable", python)
		set(item, "directory", root)
		set(item, "environment", map[string]string{"PYTHONDONTWRITEBYTECODE": "1"})
		args := append([]json.RawMessage{mustJSON(map[string]string{"literal": script})}, template.Args...)
		set(item, "args", args)
		configured = append(configured, item)
	}

	tokens := make([]token, count)
	for index := range tokens {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return preparation{}, fmt.Errorf("generate token: %w", err)
		}
		tokens[index] = token{TenantID: fmt.Sprintf("bench-%04d", index+1),
			Token: base64.RawURLEncoding.EncodeToString(secret), Tools: names}
	}
	set(profile, "tokens", tokens)
	set(profile, "process_tools", configured)

	generated := filepath.Join(root, "generated")
	if err := os.MkdirAll(generated, 0700); err != nil {
		return preparation{}, err
	}
	if diagnostics {
		if err := createIOFixture(filepath.Join(generated, "io-fixture.sqlite")); err != nil {
			return preparation{}, err
		}
	}
	path := filepath.Join(generated, "server.json")
	result := preparation{path: path, count: count}
	if current, err := os.ReadFile(path); err == nil {
		var existing map[string]any
		var proposed map[string]any
		encoded, err := json.Marshal(profile)
		if err != nil {
			return preparation{}, err
		}
		if json.Unmarshal(current, &existing) != nil || json.Unmarshal(encoded, &proposed) != nil {
			return preparation{}, errors.New("existing server config is not valid JSON")
		}
		oldTokens, _ := existing["tokens"].([]any)
		delete(existing, "tokens")
		delete(proposed, "tokens")
		if len(oldTokens) == count && reflect.DeepEqual(existing, proposed) {
			result.reused = true
			return result, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return preparation{}, err
	}

	encoded, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return preparation{}, err
	}
	encoded = append(encoded, '\n')
	file, err := os.CreateTemp(generated, "server-*.json")
	if err != nil {
		return preparation{}, err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		return preparation{}, err
	}
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return preparation{}, err
	}
	if err := file.Close(); err != nil {
		return preparation{}, err
	}
	if _, err := os.Stat(path); err == nil {
		result.backup = filepath.Join(generated,
			fmt.Sprintf("server.backup-%s-%d.json", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid()))
		if err := os.Rename(path, result.backup); err != nil {
			return preparation{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return preparation{}, err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		if result.backup != "" {
			if restoreErr := os.Rename(result.backup, path); restoreErr != nil {
				return preparation{}, fmt.Errorf("install new config: %v; restore old config: %w", err, restoreErr)
			}
		}
		return preparation{}, err
	}
	return result, nil
}

func readObject(path string) (object, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var result object
	if err := json.Unmarshal(raw, &result); err != nil || result == nil {
		return nil, fmt.Errorf("decode %s: expected JSON object", path)
	}
	return result, nil
}

func set(item object, key string, value any) {
	item[key] = mustJSON(value)
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err) // All values supplied here are JSON-compatible Go values.
	}
	return encoded
}

func createIOFixture(path string) error {
	if _, err := os.Stat(path); err == nil {
		return verifyIOFixture(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), "io-fixture-*.sqlite")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Close(); err != nil {
		return err
	}
	database, err := sql.Open("sqlite3", temporary)
	if err != nil {
		return err
	}
	transaction, err := database.Begin()
	if err != nil {
		database.Close()
		return err
	}
	_, err = transaction.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
	if err == nil {
		var statement *sql.Stmt
		statement, err = transaction.Prepare("INSERT INTO records(id, payload) VALUES (?, ?)")
		if err == nil {
			for index := 1; index <= 1024; index++ {
				if _, err = statement.Exec(index, strings.Repeat(fmt.Sprintf("%08d", index), 128)); err != nil {
					break
				}
			}
			statement.Close()
		}
	}
	if err != nil {
		transaction.Rollback()
		database.Close()
		return err
	}
	if err := transaction.Commit(); err != nil {
		database.Close()
		return err
	}
	if err := database.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporary, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func verifyIOFixture(path string) error {
	fixtureURL := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
	database, err := sql.Open("sqlite3", fixtureURL)
	if err != nil {
		return err
	}
	defer database.Close()
	var count, minimum, maximum int
	if err := database.QueryRow("SELECT COUNT(*), MIN(LENGTH(payload)), MAX(LENGTH(payload)) FROM records").
		Scan(&count, &minimum, &maximum); err != nil {
		return err
	}
	if count != 1024 || minimum != 1024 || maximum != 1024 {
		return fmt.Errorf("invalid diagnostic I/O fixture: %s", path)
	}
	return nil
}
