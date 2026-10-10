package antigravity

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type agySessionFixture struct {
	id         string
	workspaces string
	title      string
	preview    string
	modified   string
	parent     string
	depth      int
}

func agyWorkspaceURIs(t *testing.T, paths ...string) string {
	t.Helper()
	var uris []string
	for _, path := range paths {
		u := antigravityFileURL(path)
		uris = append(uris, u.String())
	}
	data, err := json.Marshal(uris)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeAgySessionIndex(t *testing.T, fixtures ...agySessionFixture) string {
	t.Helper()
	storeDir, err := antigravitySessionDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(storeDir, "conversation_summaries.db")
	u := antigravityFileURL(path)
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE conversation_summaries (
		conversation_id TEXT PRIMARY KEY, title TEXT NOT NULL, preview TEXT NOT NULL,
		step_count INTEGER NOT NULL, last_modified_time datetime NOT NULL,
		workspace_uris TEXT NOT NULL, parent_conversation_id TEXT NOT NULL,
		nesting_depth INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		if fixture.modified == "" {
			fixture.modified = "2026-10-10 12:46:54.456545+00:00"
		}
		if _, err := db.Exec(`INSERT INTO conversation_summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			fixture.id, fixture.title, fixture.preview, 23, fixture.modified,
			fixture.workspaces, fixture.parent, fixture.depth); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func writeLegacyAgySession(t *testing.T, workDir, id, kind, text string) string {
	t.Helper()
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	chatsDir := filepath.Join(homeDir, ".gemini", "tmp", antigravityProjectSlug(workDir), "chats")
	if err := os.MkdirAll(chatsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(sessionFile{SessionID: id, Kind: kind, LastUpdated: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	message, err := json.Marshal(sessionLine{Type: "user", Content: []sessionMessagePart{{Text: text}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(chatsDir, id+".jsonl")
	if err := os.WriteFile(path, []byte(string(header)+"\n"+string(message)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Regression: the real CLI no longer creates ~/.gemini/tmp/<slug>/chats.
func TestAgentListSessions_ReadsSQLiteWorkspaceMetadata(t *testing.T) {
	t.Setenv("HOME", filepath.Join(t.TempDir(), "home ?# 中文"))
	workDir := filepath.Join(t.TempDir(), "project space")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatal(err)
	}
	workspaces := agyWorkspaceURIs(t, workDir)
	indexPath := writeAgySessionIndex(t,
		agySessionFixture{id: "older", workspaces: workspaces, preview: "\n first prompt\nprivate later lines", modified: "2026-10-10 11:00:00+00:00"},
		agySessionFixture{id: "newer", workspaces: agyWorkspaceURIs(t, t.TempDir(), workDir), title: strings.Repeat("会", 65)},
		agySessionFixture{id: "other", workspaces: agyWorkspaceURIs(t, workDir+"-other"), title: "other project"},
		agySessionFixture{id: "child", workspaces: workspaces, title: "subagent", parent: "newer"},
		agySessionFixture{id: "nested", workspaces: workspaces, title: "nested", depth: 1},
		agySessionFixture{id: "unknown-workspace", workspaces: "[]", title: "unbound"},
	)
	before, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	a := &Agent{workDir: workDir}
	sessions, err := a.ListSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0].ID != "newer" || sessions[1].ID != "older" {
		t.Fatalf("wrong project/order: %+v", sessions)
	}
	if sessions[0].Summary != strings.Repeat("会", 60)+"..." || sessions[1].Summary != "first prompt" {
		t.Fatalf("wrong summary selection: %+v", sessions)
	}
	if sessions[0].MessageCount != 0 {
		t.Fatalf("native step count was presented as a message count: %d", sessions[0].MessageCount)
	}
	wantTime := time.Date(2026, 10, 10, 12, 46, 54, 456545000, time.UTC)
	if !sessions[0].ModifiedAt.Equal(wantTime) {
		t.Fatalf("timestamp = %v, want %v", sessions[0].ModifiedAt, wantTime)
	}
	after, err := os.ReadFile(indexPath)
	if err != nil || string(after) != string(before) {
		t.Fatalf("read-only listing changed the index: %v", err)
	}
}

func TestAgentListSessions_LegacyCompatibilityUsesAuthoritativeSQLiteIDs(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	workDir := t.TempDir()
	writeAgySessionIndex(t,
		agySessionFixture{id: "duplicate", workspaces: agyWorkspaceURIs(t, workDir), title: "current title"},
		agySessionFixture{id: "moved", workspaces: agyWorkspaceURIs(t, t.TempDir()), title: "another workspace"},
	)
	writeLegacyAgySession(t, workDir, "duplicate", "", "old title")
	writeLegacyAgySession(t, workDir, "moved", "", "stale workspace copy")
	writeLegacyAgySession(t, workDir, "legacy", "", "compatible history")
	writeLegacyAgySession(t, workDir, "child", "subagent", "hidden subagent")
	sessions, err := (&Agent{workDir: workDir}).ListSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0].ID != "duplicate" || sessions[0].Summary != "current title" ||
		sessions[1].ID != "legacy" || sessions[1].MessageCount != 1 {
		t.Fatalf("legacy fallback ignored current identity or lost legacy session: %+v", sessions)
	}
}

func TestAgentListSessions_MissingStoreDoesNotCreateDatabase(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	sessions, err := (&Agent{workDir: t.TempDir()}).ListSessions(context.Background())
	if err != nil || len(sessions) != 0 {
		t.Fatalf("missing store: %+v, %v", sessions, err)
	}
	if _, err := os.Stat(filepath.Join(homeDir, ".gemini")); !os.IsNotExist(err) {
		t.Fatalf("reading created a store: %v", err)
	}
}

func TestAgentValidateSessionID_RejectsKnownWrongWorkspace(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	workDir := t.TempDir()
	writeAgySessionIndex(t,
		agySessionFixture{id: "local", workspaces: agyWorkspaceURIs(t, workDir)},
		agySessionFixture{id: "foreign", workspaces: agyWorkspaceURIs(t, workDir+"-other")},
		agySessionFixture{id: "child", workspaces: agyWorkspaceURIs(t, workDir), parent: "local"},
	)
	writeLegacyAgySession(t, workDir, "legacy", "", "existing history")
	a := &Agent{workDir: workDir}
	for _, id := range []string{"local", "legacy"} {
		if !a.ValidateSessionID(context.Background(), id) {
			t.Errorf("known local session %q rejected", id)
		}
	}
	for _, id := range []string{"", "foreign", "child", "missing", "../local", "..", "path\\local", "local\n"} {
		if a.ValidateSessionID(context.Background(), id) {
			t.Errorf("invalid or foreign session %q accepted", id)
		}
	}
}

func TestAgentValidateSessionID_UnknownWorkspacePreservesIDAndBlocksResume(t *testing.T) {
	for _, scenario := range []string{"corrupt-index", "index-is-directory", "empty-workspaces", "malformed-workspaces", "missing-summary", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			workDir := t.TempDir()
			ctx := context.Background()
			indexPath := writeAgySessionIndex(t, agySessionFixture{id: "saved", workspaces: agyWorkspaceURIs(t, workDir)})
			switch scenario {
			case "corrupt-index":
				if err := os.WriteFile(indexPath, []byte("not sqlite"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "index-is-directory":
				if err := os.Remove(indexPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(indexPath, 0o700); err != nil {
					t.Fatal(err)
				}
			case "empty-workspaces", "malformed-workspaces":
				db, err := sql.Open("sqlite", indexPath)
				if err != nil {
					t.Fatal(err)
				}
				value := "[]"
				if scenario == "malformed-workspaces" {
					value = `{ "uri": "file:///not-a-list" }`
				}
				_, err = db.Exec("UPDATE conversation_summaries SET workspace_uris = ?", value)
				_ = db.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "missing-summary":
				if err := os.Remove(indexPath); err != nil {
					t.Fatal(err)
				}
				conversationsDir := filepath.Join(filepath.Dir(indexPath), "conversations")
				if err := os.MkdirAll(conversationsDir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(conversationsDir, "saved.db"), []byte("conversation"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			a := &Agent{workDir: workDir, cmd: "a-cli-that-must-not-start"}
			if !a.ValidateSessionID(ctx, "saved") {
				t.Fatal("unavailable metadata would cause core to erase the saved ID")
			}
			if valid, err := validateAntigravitySessionID(ctx, workDir, "saved"); err == nil || valid {
				t.Fatalf("unknown workspace must remain unverified: valid=%v, err=%v", valid, err)
			}
			session, err := a.StartSession(ctx, "saved")
			var unavailable *core.ResumeUnavailableError
			if session != nil || !errors.As(err, &unavailable) {
				t.Fatalf("unverified workspace launched a session: %v, %v", session, err)
			}
		})
	}
}

func TestAgentListSessions_IndexErrorsAreNotReportedAsEmptyHistory(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	indexPath := writeAgySessionIndex(t)
	if err := os.WriteFile(indexPath, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (&Agent{workDir: t.TempDir()}).ListSessions(context.Background()); err == nil {
		t.Fatal("corrupt index was reported as an empty history")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&Agent{workDir: t.TempDir()}).ListSessions(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation lost: %v", err)
	}
}

func TestAntigravityWorkspaceMatches_ExactFileURIAndSymlink(t *testing.T) {
	workDir := filepath.Join(t.TempDir(), "workspace # 中文")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(workDir, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	for _, path := range []string{workDir, alias, workDir + string(os.PathSeparator)} {
		if matches, err := antigravityWorkspaceMatches(workDir, agyWorkspaceURIs(t, path)); err != nil || !matches {
			t.Errorf("same workspace %q failed: %v, %v", path, matches, err)
		}
	}
	if matches, err := antigravityWorkspaceMatches(workDir, agyWorkspaceURIs(t, workDir+"-sibling")); err != nil || matches {
		t.Fatalf("workspace prefix incorrectly matched: %v, %v", matches, err)
	}
	for _, raw := range []string{`[]`, `null`, `{}`, `["ssh://host/project"]`, `["file://remote/project"]`, `["file:///tmp/project?x=1"]`, `["file:relative"]`} {
		if matches, err := antigravityWorkspaceMatches(workDir, raw); err == nil || matches {
			t.Errorf("unverified URI accepted: %s -> %v, %v", raw, matches, err)
		}
	}
}

func TestAgentDeleteSession_SQLiteRefusalKeepsBothStores(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	workDir := t.TempDir()
	indexPath := writeAgySessionIndex(t, agySessionFixture{id: "saved", workspaces: agyWorkspaceURIs(t, workDir)})
	conversationPath := filepath.Join(filepath.Dir(indexPath), "conversations", "saved.db")
	if err := os.MkdirAll(filepath.Dir(conversationPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(conversationPath, []byte("untouched conversation"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacyCopy := writeLegacyAgySession(t, workDir, "saved", "", "legacy copy must also survive")
	before, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	err = (&Agent{workDir: workDir}).DeleteSession(context.Background(), "saved")
	if err == nil || !strings.Contains(err.Error(), "non-interactive interface") {
		t.Fatalf("expected explicit unsupported deletion, got %v", err)
	}
	after, err := os.ReadFile(indexPath)
	if err != nil || string(after) != string(before) {
		t.Fatalf("deletion refusal changed the index: %v", err)
	}
	for _, path := range []string{conversationPath, legacyCopy} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("deletion refusal removed %s: %v", path, err)
		}
	}
}

func TestAgentDeleteSession_LegacyCompatibilityAndWorkspaceBoundary(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	workDir := filepath.Join(t.TempDir(), "local-project")
	otherDir := filepath.Join(t.TempDir(), "other-project")
	localPath := writeLegacyAgySession(t, workDir, "local", "", "delete this session")
	foreignPath := writeLegacyAgySession(t, otherDir, "foreign", "", "preserve other project")
	a := &Agent{workDir: workDir}
	for _, id := range []string{"foreign", "../foreign", "missing"} {
		if err := a.DeleteSession(context.Background(), id); err == nil {
			t.Fatalf("accepted deletion of %q", id)
		}
	}
	if err := a.DeleteSession(context.Background(), "local"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("legacy session not removed: %v", err)
	}
	if _, err := os.Stat(foreignPath); err != nil {
		t.Fatalf("foreign session changed: %v", err)
	}
}

func TestAgentPermissionModes_IncludesConfiguredAcceptEdits(t *testing.T) {
	a := &Agent{}
	a.SetMode("accept-edits")
	for _, mode := range a.PermissionModes() {
		if mode.Key == a.GetMode() {
			if mode.Key != "acceptEdits" || mode.NameZh == "" || mode.DescZh == "" {
				t.Fatalf("incomplete accept-edits mode: %+v", mode)
			}
			return
		}
	}
	t.Fatal("configured acceptEdits mode is absent from mode selection")
}

func TestAntigravityAgent_WorkDirRaceFreeReaders(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	a := &Agent{workDir: dir}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			a.SetWorkDir(filepath.Join(dir, string(rune('a'+i%2))))
		}(i)
		go func() {
			defer wg.Done()
			_, _ = a.ListSessions(context.Background())
			_ = a.ValidateSessionID(context.Background(), "missing")
			_ = a.DeleteSession(context.Background(), "missing")
			_ = a.CommandDirs()
			_ = a.SkillDirs()
			_ = a.ProjectMemoryFile()
		}()
	}
	wg.Wait()
}
