package antigravity

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
	_ "modernc.org/sqlite"
)

var _ core.SessionIDValidator = (*Agent)(nil)

// CLI 1.3.x stores summaries separately from its conversation databases. Read
// the index with SQLite's read-only mode so listing never creates a new store.
func antigravitySessionDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("antigravity: cannot determine home dir: %w", err)
	}
	return filepath.Join(homeDir, ".gemini", "antigravity-cli"), nil
}

func openAntigravitySessionIndex(storeDir string) (*sql.DB, error) {
	path := filepath.Join(storeDir, "conversation_summaries.db")
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("antigravity: conversation summary index is not a regular file")
	}
	u := antigravityFileURL(path)
	query := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(1000)"}}
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("antigravity: open conversation summary index: %w", err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func antigravityFileURL(path string) url.URL {
	path = filepath.ToSlash(path)
	if len(path) >= 2 && path[1] == ':' {
		path = "/" + path
	}
	return url.URL{Scheme: "file", Path: path}
}

func listAntigravitySessions(ctx context.Context, workDir string) ([]core.AgentSessionInfo, error) {
	storeDir, err := antigravitySessionDir()
	if err != nil {
		return nil, err
	}
	sessions, knownIDs, err := listSQLiteAntigravitySessions(ctx, storeDir, workDir)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	legacy, err := listLegacyAntigravitySessions(workDir)
	if err != nil {
		return nil, err
	}
	for _, session := range legacy {
		// The current index is authoritative, including an ID now belonging to
		// another workspace. An older JSONL copy must not reintroduce it.
		if _, known := knownIDs[session.ID]; !known {
			sessions = append(sessions, session)
		}
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].ModifiedAt.Equal(sessions[j].ModifiedAt) {
			return sessions[i].ID < sessions[j].ID
		}
		return sessions[i].ModifiedAt.After(sessions[j].ModifiedAt)
	})
	return sessions, ctx.Err()
}

func listSQLiteAntigravitySessions(ctx context.Context, storeDir, workDir string) ([]core.AgentSessionInfo, map[string]struct{}, error) {
	db, err := openAntigravitySessionIndex(storeDir)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("antigravity: read conversation summary index: %w", err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT conversation_id, title, preview,
		CAST(last_modified_time AS TEXT), workspace_uris, parent_conversation_id, nesting_depth
		FROM conversation_summaries`)
	if err != nil {
		return nil, nil, fmt.Errorf("antigravity: query conversation summaries: %w", err)
	}
	defer rows.Close()
	knownIDs := make(map[string]struct{})
	var sessions []core.AgentSessionInfo
	for rows.Next() {
		var id, title, preview, modified, workspaces, parent string
		var depth int
		if err := rows.Scan(&id, &title, &preview, &modified, &workspaces, &parent, &depth); err != nil {
			return nil, nil, fmt.Errorf("antigravity: read conversation summary: %w", err)
		}
		knownIDs[id] = struct{}{}
		if !validAntigravitySessionID(id) || parent != "" || depth != 0 {
			continue
		}
		matches, err := antigravityWorkspaceMatches(workDir, workspaces)
		if err != nil {
			slog.Warn("antigravity: skipping session with unverified workspace", "session_id", id, "error", err)
			continue
		}
		if !matches {
			continue
		}
		modifiedAt, err := parseAntigravitySessionTime(modified)
		if err != nil {
			return nil, nil, fmt.Errorf("antigravity: read conversation timestamp: %w", err)
		}
		sessions = append(sessions, core.AgentSessionInfo{
			ID: id, Summary: antigravitySessionSummary(title, preview, id), ModifiedAt: modifiedAt,
			// step_count includes tools and internal steps. The index does not
			// provide a verified message count; leave it unknown rather than
			// presenting execution steps as user/assistant messages.
		})
	}
	return sessions, knownIDs, rows.Err()
}

func antigravitySessionSummary(title, preview, id string) string {
	for _, value := range []string{title, preview, id} {
		for _, line := range strings.Split(value, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				if runes := []rune(line); len(runes) > 60 {
					return string(runes[:60]) + "..."
				}
				return line
			}
		}
	}
	return id
}

func parseAntigravitySessionTime(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format")
}

func validAntigravitySessionID(id string) bool {
	if id == "" || id == "." || id == ".." || len(id) > 256 {
		return false
	}
	for _, r := range id {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func canonicalAntigravityWorkspace(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("antigravity: workspace path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("antigravity: resolve workspace: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		return filepath.Clean(resolved), nil
	}
	if os.IsNotExist(err) {
		return filepath.Clean(abs), nil
	}
	return "", fmt.Errorf("antigravity: resolve workspace links: %w", err)
}

func antigravityWorkspaceMatches(workDir, raw string) (bool, error) {
	var workspaces []string
	if err := json.Unmarshal([]byte(raw), &workspaces); err != nil || len(workspaces) == 0 {
		return false, fmt.Errorf("antigravity: workspace URI metadata is unavailable")
	}
	want, err := canonicalAntigravityWorkspace(workDir)
	if err != nil {
		return false, err
	}
	for _, workspace := range workspaces {
		u, err := url.Parse(workspace)
		if err != nil || u.Scheme != "file" || (u.Host != "" && u.Host != "localhost") ||
			u.RawQuery != "" || u.Fragment != "" || u.User != nil || u.Opaque != "" {
			return false, fmt.Errorf("antigravity: unsupported workspace URI metadata")
		}
		path := u.Path
		if runtime.GOOS == "windows" && len(path) >= 3 && path[0] == '/' && path[2] == ':' {
			path = path[1:]
		}
		path = filepath.FromSlash(path)
		if !filepath.IsAbs(path) {
			return false, fmt.Errorf("antigravity: workspace URI path is not absolute")
		}
		got, err := canonicalAntigravityWorkspace(path)
		if err != nil {
			return false, err
		}
		if got == want {
			return true, nil
		}
	}
	return false, nil
}

// found distinguishes an authoritative index entry from a missing row. A row
// without usable workspace metadata is unknown, not a cross-workspace match.
func lookupSQLiteAntigravitySession(ctx context.Context, storeDir, workDir, sessionID string) (found, valid bool, err error) {
	db, err := openAntigravitySessionIndex(storeDir)
	if os.IsNotExist(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("antigravity: read conversation summary index: %w", err)
	}
	defer db.Close()
	var workspaces, parent string
	var depth int
	err = db.QueryRowContext(ctx, `SELECT workspace_uris, parent_conversation_id, nesting_depth
		FROM conversation_summaries WHERE conversation_id = ?`, sessionID).Scan(&workspaces, &parent, &depth)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("antigravity: verify conversation workspace: %w", err)
	}
	if parent != "" || depth != 0 {
		return true, false, nil
	}
	valid, err = antigravityWorkspaceMatches(workDir, workspaces)
	return true, valid, err
}

func validateAntigravitySessionID(ctx context.Context, workDir, sessionID string) (bool, error) {
	if !validAntigravitySessionID(sessionID) {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	storeDir, err := antigravitySessionDir()
	if err != nil {
		return false, err
	}
	found, valid, err := lookupSQLiteAntigravitySession(ctx, storeDir, workDir, sessionID)
	if err != nil || found {
		return valid, err
	}
	// An existing conversation whose summary is not yet available cannot be
	// assigned to this workspace safely, but it also must not be erased by core.
	_, err = os.Stat(filepath.Join(storeDir, "conversations", sessionID+".db"))
	if err == nil {
		return false, fmt.Errorf("antigravity: conversation exists but its workspace summary is unavailable")
	}
	if !os.IsNotExist(err) {
		return false, fmt.Errorf("antigravity: inspect conversation: %w", err)
	}
	path, metadata, err := findLegacyAntigravitySession(ctx, workDir, sessionID)
	return path != "" && metadata.Kind != "subagent", err
}

func deleteAntigravitySession(ctx context.Context, workDir, sessionID string) error {
	valid, err := validateAntigravitySessionID(ctx, workDir, sessionID)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("antigravity: session does not exist in this workspace")
	}
	storeDir, err := antigravitySessionDir()
	if err != nil {
		return err
	}
	found, _, err := lookupSQLiteAntigravitySession(ctx, storeDir, workDir, sessionID)
	if err != nil {
		return err
	}
	if found {
		// The CLI has no supported non-interactive delete API. Removing only
		// one side of its shared index/conversation store would corrupt it.
		return fmt.Errorf("antigravity: SQLite conversation deletion is not supported by the CLI's non-interactive interface; manage this conversation in Antigravity CLI")
	}
	path, _, err := findLegacyAntigravitySession(ctx, workDir, sessionID)
	if err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("antigravity: session does not exist in this workspace")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("antigravity: remove legacy session: %w", err)
	}
	return nil
}

func findLegacyAntigravitySession(ctx context.Context, workDir, sessionID string) (string, sessionFile, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", sessionFile{}, fmt.Errorf("antigravity: cannot determine home dir: %w", err)
	}
	chatsDir := filepath.Join(homeDir, ".gemini", "tmp", antigravityProjectSlug(workDir), "chats")
	entries, err := os.ReadDir(chatsDir)
	if os.IsNotExist(err) {
		return "", sessionFile{}, nil
	}
	if err != nil {
		return "", sessionFile{}, fmt.Errorf("antigravity: read legacy chats directory: %w", err)
	}
	var readErr error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", sessionFile{}, err
		}
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(chatsDir, entry.Name())
		metadata, err := readLegacyAntigravitySessionHeader(path)
		if err != nil {
			readErr = err
			continue
		}
		if metadata.SessionID == sessionID {
			return path, metadata, nil
		}
	}
	return "", sessionFile{}, readErr
}

func readLegacyAntigravitySessionHeader(path string) (sessionFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return sessionFile{}, fmt.Errorf("antigravity: read legacy session header: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var metadata sessionFile
		if err := json.Unmarshal(scanner.Bytes(), &metadata); err != nil || metadata.SessionID == "" {
			return sessionFile{}, fmt.Errorf("antigravity: invalid legacy session metadata")
		}
		return metadata, nil
	}
	if err := scanner.Err(); err != nil {
		return sessionFile{}, fmt.Errorf("antigravity: scan legacy session header: %w", err)
	}
	return sessionFile{}, fmt.Errorf("antigravity: legacy session metadata is unavailable")
}
