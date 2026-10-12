package core

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Exercise the public relay entry after a successful global engine start.
// Every agent/platform is an in-process fake; storage is below t.TempDir.
func TestSessionStorage_CorruptWorkspaceRejectsRelay(t *testing.T) {
	baseDir := t.TempDir()
	workspaceDir := filepath.Join(baseDir, "relay-workspace")
	if err := os.Mkdir(workspaceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	p := &mockChannelResolver{name: "mock", names: map[string]string{"C42": "relay-workspace"}}
	e := NewEngine("storage-review", &stubAgent{}, []Platform{p}, filepath.Join(baseDir, "global.json"), LangEnglish)
	t.Cleanup(e.cancel)
	e.SetMultiWorkspace(baseDir, filepath.Join(baseDir, "bindings.json"))
	if err := e.Start(); err != nil {
		t.Fatalf("healthy global engine did not start: %v", err)
	}

	brokenPath := filepath.Join(baseDir, "workspace-sessions.json")
	broken := []byte("{\n")
	if err := os.WriteFile(brokenPath, broken, 0o600); err != nil {
		t.Fatal(err)
	}
	ws := e.workspacePool.GetOrCreate(normalizeWorkspacePath(workspaceDir))
	stubSession := newRecoveryTestSession("fake-native-relay")
	agent := &recoveryTestAgent{session: stubSession, dir: workspaceDir}
	ws.agent = agent
	ws.sessions = NewSessionManager(brokenPath)
	if ws.sessions.LoadError() == nil {
		t.Fatal("probe did not construct an unreadable workspace snapshot")
	}

	response, err := e.HandleRelay(context.Background(), "source", "mock:C42:U1", "resume the authorized task")
	t.Logf("relay error=%v; fake starts=%d; fake sends=%d; response=%q", err, agent.starts.Load(), len(stubSession.sent()), response)
	if agent.starts.Load() != 0 || len(stubSession.sent()) != 0 {
		t.Errorf("corrupt workspace storage admitted an executor before rejection")
	}
	if err == nil {
		t.Errorf("corrupt workspace relay returned success")
	}
	after, readErr := os.ReadFile(brokenPath)
	if readErr != nil || !bytes.Equal(broken, after) {
		t.Errorf("corrupt snapshot changed: data=%q error=%v", after, readErr)
	}
}

func TestSessionStorage_ConcurrentSavesRetainRecoveryAndHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	sm := NewSessionManager(path)
	session := sm.GetOrCreateActive("mock:C1:U1")
	session.SetAgentSessionID("native-old", "stub")
	session.ContextRecovery = &ContextRecoveryRecord{ID: "one-attempt", Phase: "sending", FromSessionID: "native-old", InputStatus: "unknown", TurnStatus: "running"}
	const writers, turns = 4, 5
	errors := make(chan error, writers*turns)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < turns; n++ {
				session.AddHistory("user", "accepted visible input")
				errors <- sm.SaveWithError()
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded := NewSessionManager(path)
	if err := loaded.LoadError(); err != nil {
		t.Fatal(err)
	}
	got := loaded.GetOrCreateActive("mock:C1:U1")
	if got.HistoryLen() != writers*turns || got.GetAgentSessionID() != "native-old" {
		t.Fatalf("lost snapshot data: history=%d native=%q", got.HistoryLen(), got.GetAgentSessionID())
	}
	if r := got.GetContextRecovery(); r == nil || r.ID != "one-attempt" || r.InputStatus != "unknown" {
		t.Fatalf("lost or changed attempt: %+v", r)
	}
	b, err := os.ReadFile(path)
	if err != nil || !json.Valid(b) {
		t.Fatalf("invalid on-disk snapshot: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatalf("unexpected leftover files: %v; error=%v", entries, err)
	}
}
