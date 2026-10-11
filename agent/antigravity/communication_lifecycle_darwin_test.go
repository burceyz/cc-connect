//go:build darwin

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
)

func TestCommunicationQueuedCallAfterRunEnded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work := t.TempDir()
	marker := filepath.Join(work, "backend-ran")
	b, err := newAgyCommunicationBridge(ctx, []string{"/bin/sh", "-c", "printf invoked > " + shellQuote(marker) + "; printf '{\"content\":[]}'"}, work, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	run, err := newAgyRun(ctx, cancel, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.setRun(run, "original"); err != nil {
		t.Fatal(err)
	}
	b.callMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			b.callMu.Unlock()
		}
	}()
	conn, err := net.Dial("tcp", b.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(conn).Encode(antigravityhook.CommunicationRequest{
		Token: run.token, Generation: run.generation, Method: "tools/call", Params: json.RawMessage(`{"name":"send_message","arguments":{}}`),
	}); err != nil {
		t.Fatal(err)
	}
	blocked := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stack := make([]byte, 64<<10)
		n := runtime.Stack(stack, true)
		for _, block := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(block, "(*agyCommunicationBridge).handleConnection") && strings.Contains(block, "Mutex).lockSlow") {
				blocked = true
			}
		}
		if blocked {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !blocked {
		t.Fatal("request did not reach the locked backend gate")
	}
	run.end(false)
	if run.ctx.Err() != nil {
		t.Fatal("fixture must represent terminal result before process exit")
	}
	b.callMu.Unlock()
	unlocked = true
	var response antigravityhook.CommunicationResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatalf("ended run still started backend after waiting for callMu; response error=%q", response.Error)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if response.Error != "run_ended" {
		t.Fatalf("got error %q, want run_ended", response.Error)
	}
}

func TestCommunicationCloseCancelsAcceptedIdleConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newAgyCommunicationBridge(ctx, []string{"/usr/bin/true"}, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", b.listener.Addr().String())
	if err != nil {
		b.Close()
		t.Fatal(err)
	}
	defer conn.Close()
	// An incomplete JSON value makes successful Write prove that the handler
	// owns this accepted connection before Close is invoked.
	if _, err := conn.Write([]byte(`{"method":`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	reading := false
	for time.Now().Before(deadline) {
		stack := make([]byte, 64<<10)
		n := runtime.Stack(stack, true)
		for _, block := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(block, "(*agyCommunicationBridge).handleConnection") && strings.Contains(block, "encoding/json.(*Decoder).Decode") {
				reading = true
			}
		}
		if reading {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !reading {
		conn.Close()
		b.Close()
		t.Fatal("handler did not begin decoding")
	}
	done := make(chan struct{})
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		conn.Close()
		<-done
		t.Fatal("Close left accepted idle connection alive; it otherwise waits for the 50-second request deadline")
	}
}

func TestCommunicationSessionOSHostConfigBoundary(t *testing.T) {
	opts := communicationTestOptions(t)
	parentRule := filepath.Join(filepath.Dir(opts.WorkDir), "AGENTS.md")
	localRule := filepath.Join(opts.WorkDir, "AGENTS.md")
	for path, content := range map[string]string{parentRule: "parent-rule\n", localRule: "local-rule\n"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("AGENTS.md", filepath.Join(opts.WorkDir, "GEMINI.md")); err != nil {
		t.Fatal(err)
	}
	otherRule := filepath.Join(t.TempDir(), "AGENTS.md")
	if err := os.WriteFile(otherRule, []byte("other rule"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := newAntigravitySessionMode(ctx, "/bin/sh", nil, opts.WorkDir, "", "communication", "", nil, 0,
		&agyCommunicationOptions{command: []string{"/usr/bin/true"}, runtimeDir: opts.RuntimeDir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	settings := filepath.Join(opts.RuntimeDir, "antigravity-cli", "settings.json")
	script := `read -r value < "$1" || exit 11
[ "$value" = local-rule ] || exit 12
if read -r value < "$2"; then exit 13; fi
if read -r value < "$3"; then exit 14; fi
printf '{"permissions":{"allow":["command(*)","read_file(*)"]}}' > "$4" || exit 15
if read -r value < "$2"; then exit 19; fi
if read -r value < "$3"; then exit 20; fi
printf cache > "$5" || exit 16
printf native-project > "$6" || exit 17
if printf wrong > "$7"; then exit 18; fi
printf boundary-ok`
	cmd, err := s.communicationCommand(ctx, []string{"-c", script, "rule-boundary", localRule, parentRule,
		otherRule, settings, filepath.Join(opts.RuntimeDir, "cache-fixture"),
		filepath.Join(s.invocationBridge.configDir, "config", "projects", "default-cli-project.json"),
		filepath.Join(s.invocationBridge.configDir, "config", "hooks.json")})
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = s.communicationEnv()
	output, err := cmd.Output()
	if err != nil || string(output) != "boundary-ok" {
		t.Fatalf("integrated process boundary: output=%q err=%v", output, err)
	}
	if data, err := os.ReadFile(settings); err != nil || !json.Valid(data) {
		t.Fatal("native settings were not writable")
	}
	for _, name := range []string{"run_command", "view_file", "write_to_file", "mcp_other_send_message"} {
		conn, err := net.Dial("tcp", s.permissionBridge.address)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		input, _ := json.Marshal(map[string]any{"conversationId": "native-allow-all", "toolCall": map[string]any{
			"name": name, "args": map[string]any{"AbsolutePath": parentRule},
		}})
		_ = json.NewEncoder(conn).Encode(antigravityhook.BridgeRequest{Token: s.permissionBridge.token, HookInput: input})
		var response antigravityhook.BridgeResponse
		err = json.NewDecoder(conn).Decode(&response)
		_ = conn.Close()
		if err != nil || response.Decision != "deny" {
			t.Fatalf("native allow-all expanded host permission for %s: %v %s", name, err, response.Decision)
		}
	}
}
