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

	"github.com/chenhg5/cc-connect/core"
)

// Run the real adapter against a child process, without a Codex installation
// or model request. This covers transport, command options, and resume wiring.
func TestAppServerStartup_StdIOAndConfiguredCommand(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, transport := range []string{"", "stdio", "stdio://", " STDIO ", "STDIO://"} {
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
			want := []string{"argument with spaces", "app-server"}
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

func TestAppServerStartup_ResumesLargeHistoryWithoutReplacingThread(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	agent, err := New(map[string]any{
		"backend": "app_server", "work_dir": dir,
		"cmd": []string{executable, "-test.run=^TestAppServerStartupHelper$", "--"},
		"env": map[string]string{
			"CC_CODEX_STARTUP_HELPER":       "1",
			"CC_CODEX_STARTUP_ARGS":         filepath.Join(dir, "args.json"),
			"CC_CODEX_STARTUP_LARGE_RESUME": "1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	session, err := agent.StartSession(ctx, "original-thread")
	if err != nil {
		t.Fatalf("resume large history: %v", err)
	}
	defer session.Close()
	if got := session.CurrentSessionID(); got != "original-thread" {
		t.Fatalf("resumed thread = %q, want original-thread", got)
	}
	if err := session.Send("continue the original task", "large-resume", nil, nil); err != nil {
		t.Fatalf("send after large resume: %v", err)
	}
	var text string
	for {
		select {
		case event, ok := <-session.Events():
			if !ok {
				t.Fatal("session closed before completing the resumed turn")
			}
			if event.Type == core.EventError {
				t.Fatalf("resumed turn failed: %v", event.Error)
			}
			text += event.Content
			if event.Type == core.EventResult && event.Done {
				if event.SessionID != "original-thread" || !strings.Contains(text, "resumed reply") {
					t.Fatalf("unexpected resumed result: session=%q text=%q", event.SessionID, text)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
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
			thread := map[string]any{"id": request.Params["threadId"]}
			if os.Getenv("CC_CODEX_STARTUP_LARGE_RESUME") == "1" {
				// Resuming includes history in one JSON line, which can exceed
				// Scanner's former 10 MiB ceiling.
				thread["turns"] = []any{map[string]any{
					"id": "history-turn", "items": []any{map[string]any{
						"id": "history-item", "type": "agentMessage",
						"text": strings.Repeat("x", 11*1024*1024),
					}},
				}}
			}
			result["thread"] = thread
		case "turn/start":
			if request.Params["threadId"] != "original-thread" {
				os.Exit(6)
			}
			result["turn"] = map[string]any{"id": "resumed-turn"}
		default:
			os.Exit(4)
		}
		if encoder.Encode(map[string]any{"id": request.ID, "result": result}) != nil {
			os.Exit(5)
		}
		if request.Method == "turn/start" {
			for _, notification := range []map[string]any{
				{"method": "turn/started", "params": map[string]any{
					"threadId": "original-thread", "turn": map[string]any{"id": "resumed-turn"},
				}},
				{"method": "item/completed", "params": map[string]any{
					"threadId": "original-thread", "turnId": "resumed-turn",
					"item": map[string]any{"type": "agentMessage", "text": "resumed reply"},
				}},
				{"method": "turn/completed", "params": map[string]any{
					"threadId": "original-thread", "turn": map[string]any{"id": "resumed-turn", "status": "completed"},
				}},
			} {
				if encoder.Encode(notification) != nil {
					os.Exit(7)
				}
			}
		}
	}
	os.Exit(0)
}
