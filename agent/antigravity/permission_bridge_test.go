package antigravity

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

func TestAgyPermissionOverlayRoutesNativeChecksWithoutChangingUserSettings(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	cliDir := filepath.Join(homeDir, ".gemini", "antigravity-cli")
	if err := os.MkdirAll(filepath.Join(cliDir, "conversations"), 0o700); err != nil {
		t.Fatal(err)
	}
	original := []byte(`{"trustedWorkspaces":["/workspace"],"extra":{"keep":true},"permissions":{"allow":["command(git)"],"deny":["command(sudo)"],"ask":["command(rm)"]}}`)
	path := filepath.Join(cliDir, "settings.json")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, permission := range []bool{false, true} {
		overlay, err := createAgyConfigOverlayForHooks(t.TempDir(), permission, true)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(overlay, "antigravity-cli", "settings.json"))
		if err != nil {
			t.Fatal(err)
		}
		if !permission {
			if !bytes.Equal(data, original) {
				t.Fatal("non-bridged mode changed native permissions")
			}
			continue
		}
		info, err := os.Lstat(filepath.Join(overlay, "antigravity-cli", "settings.json"))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("overlay settings must be a private regular file: %v, %v", info, err)
		}
		var got, before map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(original, &before); err != nil {
			t.Fatal(err)
		}
		permissions := got["permissions"].(map[string]any)
		for _, key := range []string{"deny", "ask"} {
			if !reflect.DeepEqual(permissions[key], before["permissions"].(map[string]any)[key]) {
				t.Fatalf("native %s rules changed", key)
			}
		}
		for _, rule := range []string{"command(git)", "command(*)", "unsandboxed(*)"} {
			found := false
			for _, allowed := range permissions["allow"].([]any) {
				found = found || allowed == rule
			}
			if !found {
				t.Fatalf("native check %q still bypasses the external permission bridge", rule)
			}
		}
		if len(permissions["allow"].([]any)) != 3 {
			t.Fatal("bridge added native grants outside command execution")
		}
		delete(got, "permissions")
		delete(before, "permissions")
		if !reflect.DeepEqual(got, before) {
			t.Fatal("unrelated CLI settings changed")
		}
		target, err := os.Readlink(filepath.Join(overlay, "antigravity-cli", "conversations"))
		if err != nil || target != filepath.Join(cliDir, "conversations") {
			t.Fatalf("native conversation storage changed: %q, %v", target, err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, original) {
		t.Fatal("global settings were modified")
	}
}

func TestAgyPermissionOverlayRejectsMalformedNativeSettings(t *testing.T) {
	for _, settings := range []string{`{`, `[]`, `{"permissions":[]}`, `{"permissions":{"allow":"command(*)"}}`,
		`{"permissions":{"deny":"command(*)"}}`, `{"permissions":{"ask":{}}}`} {
		t.Run(settings, func(t *testing.T) {
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			dir := filepath.Join(homeDir, ".gemini", "antigravity-cli")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(settings), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := createAgyConfigOverlayForHooks(t.TempDir(), true, true); err == nil {
				t.Fatal("malformed native permissions must not start a bridged session")
			}
		})
	}
}

func TestAgyPermissionBridgePreservesHooksAndRelaysDecisions(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	realConfigDir := filepath.Join(homeDir, ".gemini", "config")
	if err := os.MkdirAll(realConfigDir, 0o700); err != nil {
		t.Fatalf("MkdirAll config: %v", err)
	}
	originalHooks := []byte(`{
  "existing-hook": {
    "PreToolUse": [{"matcher": "read_file", "hooks": [{"type": "command", "command": "existing"}]}]
  }
}`)
	realHooksPath := filepath.Join(realConfigDir, "hooks.json")
	if err := os.WriteFile(realHooksPath, originalHooks, 0o600); err != nil {
		t.Fatalf("WriteFile hooks: %v", err)
	}
	if err := os.WriteFile(filepath.Join(realConfigDir, "keep.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile keep config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan core.Event, 4)
	bridge, err := newAgyPermissionBridge(ctx, events)
	if err != nil {
		t.Fatalf("newAgyPermissionBridge: %v", err)
	}
	defer bridge.Close()

	gotOriginal, err := os.ReadFile(realHooksPath)
	if err != nil {
		t.Fatalf("ReadFile original hooks: %v", err)
	}
	if !bytes.Equal(gotOriginal, originalHooks) {
		t.Fatalf("real hooks changed:\n%s", gotOriginal)
	}
	if target, err := os.Readlink(filepath.Join(bridge.AgyConfigDir(), "config", "keep.json")); err != nil {
		t.Fatalf("Readlink preserved config: %v", err)
	} else if target != filepath.Join(realConfigDir, "keep.json") {
		t.Fatalf("preserved config target = %q", target)
	}

	var overlayHooks map[string]json.RawMessage
	overlayData, err := os.ReadFile(filepath.Join(bridge.AgyConfigDir(), "config", "hooks.json"))
	if err != nil {
		t.Fatalf("ReadFile overlay hooks: %v", err)
	}
	if err := json.Unmarshal(overlayData, &overlayHooks); err != nil {
		t.Fatalf("Unmarshal overlay hooks: %v", err)
	}
	if _, ok := overlayHooks["existing-hook"]; !ok {
		t.Fatal("overlay does not preserve existing hook")
	}
	if _, ok := overlayHooks[agyPermissionHookName]; !ok {
		t.Fatal("overlay does not contain cc-connect permission hook")
	}

	testBridgeDecision(t, bridge, events, "allow", "", "allow", "")
	testBridgeDecision(t, bridge, events, "deny", "not now", "deny", "not now")
}

func testBridgeDecision(t *testing.T, bridge *agyPermissionBridge, events <-chan core.Event, behavior, message, wantDecision, wantReason string) {
	t.Helper()

	hookInput := `{
  "conversationId": "conversation-1",
  "stepIdx": 3,
  "toolCall": {
    "name": "run_command",
    "args": {"CommandLine": "touch /tmp/permission-test", "Cwd": "/tmp"}
  }
}`
	var output bytes.Buffer
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- antigravityhook.Relay(strings.NewReader(hookInput), &output, bridge.address, bridge.token)
	}()

	var event core.Event
	select {
	case event = <-events:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for permission event")
	}
	if event.Type != core.EventPermissionRequest {
		t.Fatalf("event type = %q, want permission_request", event.Type)
	}
	if event.RequestID == "" || event.ToolName != "run_command" {
		t.Fatalf("event = %#v, want run_command permission request", event)
	}
	if event.ToolInput != "touch /tmp/permission-test" {
		t.Fatalf("ToolInput = %q", event.ToolInput)
	}

	if err := bridge.RespondPermission(event.RequestID, core.PermissionResult{Behavior: behavior, Message: message}); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}
	select {
	case err := <-relayDone:
		if err != nil {
			t.Fatalf("Relay: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for hook response")
	}

	var response antigravityhook.BridgeResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatalf("Unmarshal hook response %q: %v", output.String(), err)
	}
	if response.Decision != wantDecision || response.Reason != wantReason {
		t.Fatalf("response = %#v, want decision=%q reason=%q", response, wantDecision, wantReason)
	}
}

func TestAgyPermissionBridgeRejectsInvalidBehavior(t *testing.T) {
	bridge := &agyPermissionBridge{pending: make(map[string]chan core.PermissionResult)}
	if err := bridge.RespondPermission("request-1", core.PermissionResult{Behavior: "maybe"}); err == nil {
		t.Fatal("RespondPermission() error = nil, want invalid behavior error")
	}
}

func TestBridgeTokenEqualRequiresMatchingLength(t *testing.T) {
	if !bridgeTokenEqual("same-length-token", "same-length-token") {
		t.Fatal("bridgeTokenEqual() = false, want true")
	}
	if bridgeTokenEqual("short", "same-length-token") {
		t.Fatal("bridgeTokenEqual() = true for length mismatch, want false")
	}
	if bridgeTokenEqual("same-length-tokem", "same-length-token") {
		t.Fatal("bridgeTokenEqual() = true for same-length mismatch, want false")
	}
}
