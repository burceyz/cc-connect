package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Run the real adapter against a child process, without a Codex installation
// or model request. This covers transport, command options, and resume wiring.
func TestAppServerStartup_StdIOAndConfiguredCommand(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, transport := range []string{"", "stdio", "stdio://"} {
		t.Run("transport="+transport, func(t *testing.T) {
			dir := t.TempDir()
			argsFile := filepath.Join(dir, "args.json")
			// An accidental hard-coded `codex` must fail instead of invoking
			// the developer's real installation during this regression test.
			t.Setenv("PATH", dir)
			agent, err := New(map[string]any{
				"backend": "app_server", "app_server_url": transport,
				"work_dir": dir,
				"cmd":      []string{executable, "-test.run=^TestAppServerStartupHelper$", "--", "argument with spaces"},
				"env": map[string]string{
					"CC_CODEX_STARTUP_HELPER": "1", "CC_CODEX_STARTUP_ARGS": argsFile,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			session, err := agent.StartSession(ctx, "original-thread")
			if err != nil {
				t.Fatalf("start original thread: %v", err)
			}
			defer session.Close()
			if got := session.CurrentSessionID(); got != "original-thread" {
				t.Fatalf("resumed thread = %q", got)
			}
			data, err := os.ReadFile(argsFile)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			want := []string{"argument with spaces", "app-server", "--listen", "stdio://"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("child arguments = %q, want %q", got, want)
			}
		})
	}
}

func TestAppServerStartup_RejectsUnsupportedTransport(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, transport := range []string{"ws://127.0.0.1:3845", "unix://", "off"} {
		_, err := New(map[string]any{
			"backend": "app_server", "app_server_url": transport, "cmd": executable,
		})
		if err == nil || !strings.Contains(err.Error(), "stdio") {
			t.Errorf("transport %q: expected an explicit stdio configuration error, got %v", transport, err)
		}
	}
}

func TestAppServerStartupHelper(t *testing.T) {
	if os.Getenv("CC_CODEX_STARTUP_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			data, _ := json.Marshal(os.Args[i+1:])
			if err := os.WriteFile(os.Getenv("CC_CODEX_STARTUP_ARGS"), data, 0o600); err != nil {
				os.Exit(2)
			}
			break
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request struct {
			ID     *int64         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			os.Exit(3)
		}
		if request.ID == nil {
			continue
		}
		result := map[string]any{}
		switch request.Method {
		case "initialize", "account/rateLimits/read":
		case "thread/resume":
			result["thread"] = map[string]any{"id": request.Params["threadId"]}
		default:
			os.Exit(4)
		}
		if encoder.Encode(map[string]any{"id": request.ID, "result": result}) != nil {
			os.Exit(5)
		}
	}
	os.Exit(0)
}
