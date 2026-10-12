package core

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSessionManager_NormalSavePreservesSnapshotWhenAtomicReplaceFails(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires ordinary Unix directory permissions")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "sessions.json")
	sm := NewSessionManager(path)
	session := sm.GetOrCreateActive("user")
	session.ContextRecovery = &ContextRecoveryRecord{ID: "reserved", Phase: "sending", InputStatus: "unknown", TurnStatus: "running"}
	if err := sm.SaveWithError(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	session.AddHistory("user", "accepted after the reservation")
	if err := sm.SaveWithError(); err == nil {
		t.Fatal("snapshot replacement unexpectedly succeeded")
	}
	sm.Save()
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("ordinary Save overwrote the protected snapshot after atomic replacement failed")
	}
	loaded := NewSessionManager(path).GetOrCreateActive("user")
	if loaded.HistoryLen() != 0 || loaded.GetContextRecovery() == nil {
		t.Fatal("the last complete snapshot was not preserved")
	}
}

func TestSessionManager_CorruptSnapshotCannotBeOverwritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	broken := []byte("{\n")
	if err := os.WriteFile(path, broken, 0o600); err != nil {
		t.Fatal(err)
	}
	sm := NewSessionManager(path)
	// Even callers using the legacy no-error methods cannot overwrite the
	// unreadable evidence with a newly created in-memory session.
	sm.GetOrCreateActive("user")
	sm.NewSession("user", "replacement")
	sm.Save()
	if err := sm.SaveWithError(); err == nil {
		t.Error("unreadable existing storage was treated as an empty first start")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, broken) {
		t.Fatal("unreadable existing snapshot was overwritten")
	}
}

func TestEngine_CorruptSnapshotBlocksStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	if err := os.WriteFile(path, []byte("{\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := NewEngine("storage-test", &stubAgent{}, nil, path, LangEnglish)
	t.Cleanup(e.cancel)
	if err := e.Start(); err == nil {
		t.Fatal("engine started with unreadable existing session storage")
	}
}

func TestSessionManager_InvalidExistingSnapshotReportsLoadError(t *testing.T) {
	for _, data := range []string{"{", "null", "{}", `{"sessions":{"s1":null}}`} {
		t.Run(data, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.json")
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			sm := NewSessionManager(path)
			if err := sm.LoadError(); err == nil || !strings.Contains(err.Error(), path) {
				t.Fatalf("missing actionable storage error: %v", err)
			}
		})
	}
	if sm := NewSessionManager(filepath.Join(t.TempDir(), "new", "sessions.json")); sm.LoadError() != nil {
		t.Fatal("a missing snapshot must still support a first start")
	}
	if sm := NewSessionManager(t.TempDir()); sm.LoadError() == nil {
		t.Fatal("an existing directory was accepted as session storage")
	}
}

func TestEngine_CorruptSnapshotRejectsDirectInput(t *testing.T) {
	f := newRecoveryFixture(t)
	broken := []byte("{\n")
	if err := os.WriteFile(f.e.sessions.StorePath(), broken, 0o600); err != nil {
		t.Fatal(err)
	}
	f.e.sessions = NewSessionManager(f.e.sessions.StorePath())
	if err := f.run("direct", "continue"); err == nil {
		t.Fatal("direct execution bypassed the storage load failure")
	}
	f.e.ReceiveMessage(f.p, f.message("command", "/new"))
	if f.a.starts.Load() != 0 || len(f.old.sent()) != 0 {
		t.Fatal("unreadable storage admitted an executor")
	}
	if visible := strings.Join(f.p.getSent(), "\n"); !strings.Contains(visible, f.e.sessions.StorePath()) {
		t.Fatalf("storage failure did not identify the file: %s", visible)
	}
	got, err := os.ReadFile(f.e.sessions.StorePath())
	if err != nil || !bytes.Equal(got, broken) {
		t.Fatalf("direct input changed unreadable storage: %q, %v", got, err)
	}
}
