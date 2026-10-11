package antigravity

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

func TestCommunicationPermissionCannotBeExpandedByApproveAll(t *testing.T) {
	events := make(chan core.Event, 8)
	bridge, err := newAgyPermissionBridgeMode(context.Background(), events, true, "")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	for name, want := range map[string]string{
		"run_command": "deny", "write_to_file": "deny", "view_file": "deny",
		"mcp_other_send_message": "deny", "mcp_cc_communication_shell": "deny",
		"mcp_cc_communication_send_message": "allow", "mcp__cc_communication__read_message": "allow",
		"search_web": "allow", "read_url_content": "deny", "view_content_chunk": "allow",
	} {
		conn, err := net.Dial("tcp", bridge.address)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		input, _ := json.Marshal(map[string]any{"conversationId": "conversation-1", "toolCall": map[string]any{"name": name, "args": map[string]any{}}})
		_ = json.NewEncoder(conn).Encode(antigravityhook.BridgeRequest{Token: bridge.token, HookInput: input})
		var response antigravityhook.BridgeResponse
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if response.Decision != want {
			t.Fatalf("%s: got %s, want %s", name, response.Decision, want)
		}
	}
	if len(events) != 0 {
		t.Fatal("fixed policy emitted an overridable permission request")
	}
	if err := bridge.RespondPermission("any", core.PermissionResult{Behavior: "allow"}); err == nil {
		t.Fatal("approve-all could override communication policy")
	}
}

func TestCommunicationWebReadRejectsLocalFilesAndControlEndpoints(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want bool
	}{
		{"https://antigravity.google/docs/cli/headless", true},
		{"http://example.com/article", true},
		{"file:///private/credential", false},
		{"/private/credential", false},
		{"https://127.0.0.1:1234/", false},
		{"http://[::1]:1234/", false},
		{"https://localhost/", false},
		{"https://control.localhost./", false},
		{"https://192.168.1.1/", false},
		{"https://169.254.169.254/", false},
		{"https://user:password@example.com/", false},
		{"", false},
	} {
		if got := isAgyCommunicationTool("read_url_content", map[string]any{"Url": tc.url}); got != tc.want {
			t.Errorf("web read %q: got %v, want %v", tc.url, got, tc.want)
		}
	}
}

func TestCommunicationConfigDoesNotInheritGlobalHooksOrTools(t *testing.T) {
	privateHome := t.TempDir()
	t.Setenv("HOME", privateHome)
	if err := os.MkdirAll(filepath.Join(privateHome, ".gemini", "config"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(privateHome, ".gemini", "config", "hooks.json"), []byte("invalid global hook config"), 0o600); err != nil {
		t.Fatal(err)
	}
	work, runDir := t.TempDir(), t.TempDir()
	auth := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(auth, []byte("fixture-credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := &agyCommunicationOptions{command: []string{os.Args[0]}, runtimeDir: runDir, authFile: auth}
	s, err := newAntigravitySessionMode(context.Background(), os.Args[0], nil, work, "fixture-model", "communication", "", nil, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, err := os.ReadFile(filepath.Join(s.invocationBridge.configDir, "config", "mcp_config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Servers map[string]any `json:"mcpServers"`
	}
	if err := json.Unmarshal(data, &config); err != nil || len(config.Servers) != 1 || config.Servers["cc_communication"] == nil {
		t.Fatal("unexpected MCP servers")
	}
	if s.reviewCommand != nil {
		t.Fatal("communication mode enabled mandatory review")
	}
	// CLI 1.3.3 builds its token store from appDataDir, which is the
	// antigravity-cli directory below --gemini_dir.
	credentialPath := filepath.Join(s.invocationBridge.configDir, "antigravity-cli", "antigravity-oauth-token")
	credential, err := os.ReadFile(credentialPath)
	if err != nil || string(credential) != "fixture-credential" {
		t.Fatal("native CLI cannot read its dedicated authentication file")
	}
	if err := os.WriteFile(credentialPath, []byte("refreshed-fixture-credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	if original, err := os.ReadFile(auth); err != nil || string(original) != "fixture-credential" {
		t.Fatal("refresh changed the original host credential")
	}
	otherSession, err := newAntigravitySessionMode(context.Background(), os.Args[0], nil, work, "fixture-model", "communication", "", nil, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	defer otherSession.Close()
	if preserved, err := os.ReadFile(filepath.Join(otherSession.invocationBridge.configDir, "antigravity-cli", "antigravity-oauth-token")); err != nil || string(preserved) != "refreshed-fixture-credential" {
		t.Fatal("a new session overwrote its refreshed credential")
	}
	settings, err := os.ReadFile(filepath.Join(runDir, "antigravity-cli", "settings.json"))
	if err != nil || strings.Contains(string(settings), `"unsandboxed(*)", "command(*)"`) {
		t.Fatal("unexpected permission overlay")
	}
	args := s.buildAntigravityArgs("", false, "communication", s.invocationBridge.configDir, "question")
	if !contains(args, "--sandbox") || contains(args, "--dangerously-skip-permissions") {
		t.Fatalf("unsafe args: %v", args)
	}
	t.Setenv("CC_IPC_ORIGIN", "must-not-leak")
	t.Setenv("UNRELATED_PROVIDER_SECRET", "must-not-leak")
	for _, entry := range s.communicationEnv() {
		if strings.Contains(entry, "must-not-leak") {
			t.Fatal("host environment leaked into model process")
		}
	}
}

func TestCommunicationNativeMCPDispatcherChecksServerAndOperation(t *testing.T) {
	bridge, err := newAgyPermissionBridgeMode(context.Background(), make(chan core.Event, 8), true, "")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	for _, tc := range []struct {
		server, operation, want string
	}{
		{"cc_communication", "list_contacts", "allow"},
		{"cc_communication", "send_message", "allow"},
		{"cc_communication", "consult_hermes", "allow"},
		{"other_server", "list_contacts", "deny"},
		{"cc_communication", "run_command", "deny"},
		{"", "list_contacts", "deny"},
	} {
		conn, err := net.Dial("tcp", bridge.address)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		input, _ := json.Marshal(map[string]any{"conversationId": "conversation-1", "toolCall": map[string]any{
			"name": "call_mcp_tool", "args": map[string]any{"ServerName": tc.server, "ToolName": tc.operation, "Arguments": map[string]any{}},
		}})
		_ = json.NewEncoder(conn).Encode(antigravityhook.BridgeRequest{Token: bridge.token, HookInput: input})
		var response antigravityhook.BridgeResponse
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if response.Decision != tc.want {
			t.Fatalf("%s/%s: got %s, want %s", tc.server, tc.operation, response.Decision, tc.want)
		}
	}
}

func TestCommunicationSchemaReadRejectsOtherFilesAndSymlinkEscapes(t *testing.T) {
	cache := t.TempDir()
	allowed := filepath.Join(cache, "list_contacts.json")
	other := filepath.Join(t.TempDir(), "send_message.json")
	unknown := filepath.Join(cache, "run_command.json")
	for _, path := range []string{allowed, other, unknown} {
		if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	escape := filepath.Join(cache, "send_message.json")
	if err := os.Symlink(other, escape); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		want bool
	}{{allowed, true}, {other, false}, {unknown, false}, {escape, false}, {"list_contacts.json", false}} {
		if got := isAgyCommunicationSchemaRead(map[string]any{"AbsolutePath": tc.path}, cache); got != tc.want {
			t.Fatalf("schema %s: got %t, want %t", tc.path, got, tc.want)
		}
	}
	if isAgyCommunicationSchemaRead(map[string]any{"AbsolutePath": allowed}, "") {
		t.Fatal("schema read allowed without a host-pinned cache")
	}
}

func TestCommunicationModeAndWorkspaceAreFixedAcrossModeChanges(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS communication configuration")
	}
	work := t.TempDir()
	opts := map[string]any{"cmd": os.Args[0], "work_dir": work, "mode": "yolo", "communication_mode": true,
		"communication_command": []string{os.Args[0]}, "communication_runtime_dir": t.TempDir()}
	agent, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	a := agent.(*Agent)
	a.SetMode("yolo")
	a.SetWorkDir(t.TempDir())
	if a.GetMode() != "communication" || a.GetWorkDir() != work {
		t.Fatal("runtime controls expanded communication scope")
	}
	if len(a.PermissionModes()) != 1 || a.GlobalMemoryFile() != "" || len(a.CommandDirs()) != 0 || len(a.SkillDirs()) != 0 {
		t.Fatal("communication mode exposed unrestricted host configuration")
	}
	opts["review_command"] = []string{os.Args[0]}
	if _, err := New(opts); err == nil {
		t.Fatal("accepted mandatory review together with communication mode")
	}
}

func TestCommunicationRuleAliasCannotGrantParentWorkspaceRead(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "independent")
	if err := os.Mkdir(work, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{filepath.Join(root, "AGENTS.md"), filepath.Join(work, "AGENTS.md")} {
		if err := os.WriteFile(file, []byte("fixture rule"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("AGENTS.md", filepath.Join(work, "GEMINI.md")); err != nil {
		t.Fatal(err)
	}
	if paths, err := agyCommunicationRulePaths(work); err != nil || len(paths) != 2 {
		t.Fatalf("local rule alias rejected: %v", err)
	}
	if err := os.Symlink("../AGENTS.md", filepath.Join(work, "CLAUDE.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := agyCommunicationRulePaths(work); err == nil {
		t.Fatal("parent workspace rule granted through local alias")
	}
}

func TestCommunicationBackendUsesHostScopeAndRejectsIdentityInjection(t *testing.T) {
	if os.Getenv("GO_WANT_COMMUNICATION_BACKEND") == "1" {
		var request map[string]any
		if json.NewDecoder(os.Stdin).Decode(&request) != nil {
			os.Exit(2)
		}
		contextValue, _ := request["context"].(map[string]any)
		text, _ := json.Marshal(map[string]any{"scope": contextValue["operation_scope"], "cwd": os.Getenv("CC_IPC_CWD"), "project": os.Getenv("CC_PROJECT")})
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"content": []any{map[string]string{"type": "text", "text": string(text)}}, "isError": false})
		os.Exit(0)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work := t.TempDir()
	bridge, err := newAgyCommunicationBridge(ctx, []string{os.Args[0], "-test.run=^TestCommunicationBackendUsesHostScopeAndRejectsIdentityInjection$"}, work,
		[]string{"GO_WANT_COMMUNICATION_BACKEND=1", "CC_PROJECT=fixture-project"})
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	run, err := newAgyRun(ctx, cancel, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.setRun(run, "original-message"); err != nil {
		t.Fatal(err)
	}
	call := func(request any) antigravityhook.CommunicationResponse {
		conn, err := net.Dial("tcp", bridge.listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_ = json.NewEncoder(conn).Encode(request)
		var response antigravityhook.CommunicationResponse
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	request := map[string]any{"token": run.token, "generation": run.generation, "method": "tools/call", "params": map[string]any{"name": "send_message", "arguments": map[string]string{"contact": "atlas", "text": "hello"}}}
	response := call(request)
	if response.Error != "" || !strings.Contains(string(response.Result), textSHA256("communication-message:original-message")) {
		t.Fatalf("host scope missing: %s", response.Error)
	}
	request["context"] = map[string]string{"operation_scope": "forged"}
	if response := call(request); response.Error != "invalid_request" {
		t.Fatal("model injected host context")
	}
	delete(request, "context")
	request["token"] = "wrong"
	if response := call(request); response.Error != "identity_mismatch" {
		t.Fatal("accepted wrong session token")
	}
	request["token"] = run.token
	run.end(false)
	if response := call(request); response.Error != "run_ended" {
		t.Fatal("accepted a tool call from an ended run")
	}
}
