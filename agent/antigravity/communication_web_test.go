package antigravity

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
)

const communicationWebTestSession = "82c3dfc9-f4f1-4d0d-9b08-5da2fbce624a"

func communicationWebFixture(t *testing.T) (*antigravitySession, *agyStream, string) {
	t.Helper()
	runtimeDir := t.TempDir()
	options := &agyCommunicationOptions{command: []string{os.Args[0]}, runtimeDir: runtimeDir}
	session, err := newAntigravitySessionMode(context.Background(), os.Args[0], nil, t.TempDir(), "fixture-model", "communication", "", nil, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	run, err := newAgyRun(ctx, cancel, communicationWebTestSession)
	if err != nil {
		t.Fatal(err)
	}
	stream := &agyStream{session: session, run: run}
	if err := stream.handleLine([]byte(`{"event":"init","init":{"conversation_id":"` + communicationWebTestSession + `"}}`)); err != nil {
		t.Fatal(err)
	}
	return session, stream, filepath.Join(runtimeDir, "antigravity-cli", "brain", communicationWebTestSession, ".system_generated", "steps")
}

func writeCommunicationWebFixture(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func communicationWebStep(t *testing.T, stream *agyStream, index int, state, name string) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"event": "step_update", "step_update": map[string]any{
		"conversation_id": communicationWebTestSession, "step_index": index, "state": state, "step_type": "tool",
		"tool_name": name, "tool_info": map[string]any{"name": name, "parameters": map[string]any{"Url": "https://example.com/article"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.handleLine(data); err != nil {
		t.Fatal(err)
	}
}

func communicationWebPermission(t *testing.T, bridge *agyPermissionBridge, conversation, path, want string) {
	t.Helper()
	conn, err := net.Dial("tcp", bridge.address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	input, _ := json.Marshal(map[string]any{"conversationId": conversation, "toolCall": map[string]any{
		"name": "view_file", "args": map[string]any{"AbsolutePath": path, "StartLine": 1, "EndLine": 20},
	}})
	if err := json.NewEncoder(conn).Encode(antigravityhook.BridgeRequest{Token: bridge.token, HookInput: input}); err != nil {
		t.Fatal(err)
	}
	var response antigravityhook.BridgeResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Decision != want {
		t.Fatalf("read %s in %s: got %s, want %s", path, conversation, response.Decision, want)
	}
}

func TestCommunicationWebCacheReadsOnlyCompletedCurrentSessionContent(t *testing.T) {
	session, stream, steps := communicationWebFixture(t)
	content := filepath.Join(steps, "5", "content.md")
	writeCommunicationWebFixture(t, content, "Fetched article body.\n")
	check := func(path, want string) {
		communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, path, want)
	}
	check(content, "deny")
	communicationWebStep(t, stream, 5, "ACTIVE", "read_url_content")
	check(content, "deny")
	// Native completion events may name the tool without repeating tool_info.
	if err := stream.handleLine([]byte(`{"event":"step_update","step_update":{"conversation_id":"` + communicationWebTestSession + `","step_index":5,"state":"DONE","step_type":"tool","tool_name":"read_url_content"}}`)); err != nil {
		t.Fatal(err)
	}
	check(content, "allow")
	communicationWebPermission(t, session.permissionBridge, "373ca5c5-b3a6-470f-ac07-49f92ef4ecdd", content, "deny")
	for _, path := range []string{
		filepath.Join(steps, "6", "content.md"),
		filepath.Join(steps, "5", "other.md"),
		filepath.Join(session.communication.runtimeDir, "antigravity-cli", "antigravity-oauth-token"),
		filepath.Join(session.communication.runtimeDir, "antigravity-cli", "brain", "373ca5c5-b3a6-470f-ac07-49f92ef4ecdd", ".system_generated", "steps", "5", "content.md"),
	} {
		writeCommunicationWebFixture(t, path, "Unregistered file.\n")
		check(path, "deny")
	}
	check(steps, "deny")
	check("content.md", "deny")
	communicationWebStep(t, stream, 6, "ERROR", "read_url_content")
	check(filepath.Join(steps, "6", "content.md"), "deny")
	communicationWebStep(t, stream, 6, "DONE", "search_web")
	check(filepath.Join(steps, "6", "content.md"), "deny")
}

func TestCommunicationWebCacheRejectsReplacedCacheRoot(t *testing.T) {
	session, stream, steps := communicationWebFixture(t)
	brain := filepath.Join(session.communication.runtimeDir, "antigravity-cli", "brain")
	outside := t.TempDir()
	if err := os.Symlink(outside, brain); err != nil {
		t.Fatal(err)
	}
	content := filepath.Join(steps, "5", "content.md")
	writeCommunicationWebFixture(t, content, "Unrelated directory contents.\n")
	communicationWebStep(t, stream, 5, "DONE", "read_url_content")
	communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, content, "deny")
}

func TestCommunicationWebCacheRejectsChangedFilesAndSymlinkEscapes(t *testing.T) {
	session, stream, steps := communicationWebFixture(t)
	content := filepath.Join(steps, "5", "content.md")
	outside := filepath.Join(t.TempDir(), "private.md")
	writeCommunicationWebFixture(t, content, "Fetched article body.\n")
	writeCommunicationWebFixture(t, outside, "Unrelated private file.\n")
	communicationWebStep(t, stream, 5, "DONE", "read_url_content")
	communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, content, "allow")
	writeCommunicationWebFixture(t, content, "Replaced contents.\n")
	communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, content, "deny")
	if err := os.Remove(content); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, content); err != nil {
		t.Fatal(err)
	}
	communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, content, "deny")
	communicationWebStep(t, stream, 5, "DONE", "read_url_content")
	communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, content, "deny")
	if err := os.Symlink(filepath.Dir(outside), filepath.Join(steps, "7")); err != nil {
		t.Fatal(err)
	}
	writeCommunicationWebFixture(t, filepath.Join(filepath.Dir(outside), "content.md"), "Other directory.\n")
	communicationWebStep(t, stream, 7, "DONE", "read_url_content")
	communicationWebPermission(t, session.permissionBridge, communicationWebTestSession, filepath.Join(steps, "7", "content.md"), "deny")
}
