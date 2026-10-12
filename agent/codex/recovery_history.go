package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
)

var _ core.RecoveryHistoryProvider = (*Agent)(nil)

type recoveryHistoryLimits struct {
	lineBytes     int
	entryBytes    int
	totalBytes    int
	totalEntries  int
	prefixEntries int
}

var defaultRecoveryHistoryLimits = recoveryHistoryLimits{
	lineBytes:     4 << 20,
	entryBytes:    256 << 10,
	totalBytes:    16 << 20,
	totalEntries:  8192,
	prefixEntries: 16,
}

// GetRecoveryHistory reads only the failed thread's visible evidence. It does
// not resume Codex, rewrite its rollout, or ask the model to maintain a journal.
func (a *Agent) GetRecoveryHistory(ctx context.Context, sessionID string) (core.RecoveryHistory, error) {
	if err := ctx.Err(); err != nil {
		return core.RecoveryHistory{}, err
	}
	if !validRecoverySessionID(sessionID) {
		return core.RecoveryHistory{}, fmt.Errorf("codex recovery history: invalid session ID")
	}
	a.mu.RLock()
	codexHome, workDir := a.codexHome, a.workDir
	a.mu.RUnlock()

	cwd, err := recoveryCanonicalDir(workDir)
	if err != nil {
		return core.RecoveryHistory{}, fmt.Errorf("codex recovery history: working directory: %w", err)
	}
	root, err := recoveryCanonicalDir(filepath.Join(resolveCodexHomeDir(codexHome), "sessions"))
	if err != nil {
		return core.RecoveryHistory{}, fmt.Errorf("codex recovery history: sessions directory: %w", err)
	}
	path, err := findRecoverySessionFile(ctx, root, sessionID)
	if err != nil {
		return core.RecoveryHistory{}, err
	}
	f, err := openRecoverySessionFile(path)
	if err != nil {
		return core.RecoveryHistory{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return core.RecoveryHistory{}, fmt.Errorf("codex recovery history: stat rollout: %w", err)
	}
	// Freeze the byte extent, so a still-appending log cannot keep this read
	// alive indefinitely. Each read block also observes context cancellation.
	history, err := readRecoveryHistory(ctx, io.NewSectionReader(f, 0, info.Size()), sessionID, cwd, defaultRecoveryHistoryLimits)
	history.Source = path // A locator only; the caller exports Entries, never this raw file.
	return history, err
}

func validRecoverySessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func recoveryCanonicalDir(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func findRecoverySessionFile(ctx context.Context, root, id string) (string, error) {
	var found string
	visited := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		visited++
		if visited > 100000 {
			return fmt.Errorf("session search exceeded 100000 directory entries")
		}
		// WalkDir does not follow directory symlinks. Reject file symlinks too.
		if !entry.Type().IsRegular() {
			return nil
		}
		name := entry.Name()
		if name != id+".jsonl" && !strings.HasSuffix(name, "-"+id+".jsonl") {
			return nil
		}
		if found != "" {
			return fmt.Errorf("multiple rollouts match the exact session ID")
		}
		found = path
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("codex recovery history: locate rollout: %w", err)
	}
	if found == "" {
		return "", fmt.Errorf("codex recovery history: exact session rollout not found")
	}
	return found, nil
}

func openRecoverySessionFile(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("codex recovery history: rollout is not a readable regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("codex recovery history: open rollout: %w", err)
	}
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		f.Close()
		return nil, fmt.Errorf("codex recovery history: rollout changed while opening")
	}
	return f, nil
}

type recoveryEnvelope struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type recoveryReadStats struct {
	oversizedLines  int
	malformedLines  int
	omittedBlocks   int
	unsupported     int
	truncatedItems  int
	invalidTimes    int
	eventMessages   int
	toolItems       int
	unterminatedEnd bool
}

func readRecoveryHistory(ctx context.Context, source io.Reader, id, cwd string, limits recoveryHistoryLimits) (history core.RecoveryHistory, err error) {
	collector := recoveryHistoryCollector{limits: limits}
	stats := recoveryReadStats{}
	defer func() {
		history.Entries = collector.entries()
		history.Warnings = recoveryHistoryWarnings(stats, collector)
		if err == nil && len(history.Entries) == 0 {
			history.Warnings = append(history.Warnings, "No supported visible history entries were found; recover the task from other authorized records.")
		}
		if err != nil && len(history.Entries) != 0 {
			history.Warnings = append(history.Warnings, "Reading stopped before the snapshot ended; later task changes may be missing.")
		}
	}()
	r := bufio.NewReaderSize(source, 64<<10)
	verified := false
	for line := 1; ; line++ {
		data, ended, oversized, readErr := readRecoveryLine(ctx, r, limits.lineBytes)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return history, fmt.Errorf("codex recovery history: read source line %d: %w", line, readErr)
		}
		if errors.Is(readErr, io.EOF) && len(data) == 0 && !oversized {
			break
		}
		if !ended {
			stats.unterminatedEnd = true
		}
		if oversized {
			if !verified {
				return history, fmt.Errorf("codex recovery history: initial identity metadata exceeds line limit")
			}
			stats.oversizedLines++
			continue
		}
		if len(bytes.TrimSpace(data)) == 0 {
			continue
		}
		var record recoveryEnvelope
		if json.Unmarshal(data, &record) != nil {
			if !verified {
				return history, fmt.Errorf("codex recovery history: initial identity metadata is malformed")
			}
			stats.malformedLines++
			continue
		}
		if !verified {
			if err := verifyRecoveryMetadata(record, id, cwd); err != nil {
				return history, err
			}
			verified = true
			continue
		}
		entries := recoveryVisibleEntries(record, line, &stats)
		for _, entry := range entries {
			var truncated bool
			entry.Content, truncated = truncateRecoveryText(entry.Content, limits.entryBytes)
			if truncated {
				stats.truncatedItems++
			}
			entry.CallID, truncated = truncateRecoveryText(entry.CallID, 1024)
			if truncated {
				stats.truncatedItems++
			}
			collector.add(entry)
		}
	}
	if !verified {
		return history, fmt.Errorf("codex recovery history: initial identity metadata is missing")
	}
	if ctx.Err() != nil {
		return history, ctx.Err()
	}
	return history, nil
}

// readRecoveryLine bounds allocation even for a multi-megabyte encrypted item.
// An oversized line is consumed through its newline, never mistaken for EOF.
func readRecoveryLine(ctx context.Context, r *bufio.Reader, limit int) ([]byte, bool, bool, error) {
	var data []byte
	oversized := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, oversized, err
		}
		part, err := r.ReadSlice('\n')
		if !oversized {
			if len(data)+len(part) > limit {
				data, oversized = nil, true
			} else {
				data = append(data, part...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return data, err == nil, oversized, err
	}
}

func verifyRecoveryMetadata(record recoveryEnvelope, id, cwd string) error {
	var meta struct {
		ID     string          `json:"id"`
		CWD    string          `json:"cwd"`
		Source json.RawMessage `json:"source"`
	}
	if record.Type != "session_meta" || json.Unmarshal(record.Payload, &meta) != nil || meta.ID != id {
		return fmt.Errorf("codex recovery history: initial session identity mismatch")
	}
	if isSubagentSessionSource(meta.Source) {
		return fmt.Errorf("codex recovery history: subagent rollout is not the original conversation")
	}
	if !filepath.IsAbs(meta.CWD) {
		return fmt.Errorf("codex recovery history: original working directory is missing or relative")
	}
	originalCWD, err := recoveryCanonicalDir(meta.CWD)
	if err != nil || originalCWD != cwd {
		return fmt.Errorf("codex recovery history: original working directory mismatch")
	}
	return nil
}

type recoveryTextBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type recoveryResponseItem struct {
	Type      string              `json:"type"`
	Role      string              `json:"role"`
	Channel   string              `json:"channel"`
	Phase     string              `json:"phase"`
	Content   []recoveryTextBlock `json:"content"`
	Name      string              `json:"name"`
	CallID    string              `json:"call_id"`
	Arguments json.RawMessage     `json:"arguments"`
	Input     json.RawMessage     `json:"input"`
	Output    json.RawMessage     `json:"output"`
}

func recoveryVisibleEntries(record recoveryEnvelope, line int, stats *recoveryReadStats) []core.RecoveryHistoryEntry {
	var entries []core.RecoveryHistoryEntry
	switch record.Type {
	case "response_item":
		var item recoveryResponseItem
		if json.Unmarshal(record.Payload, &item) != nil {
			stats.malformedLines++
			return nil
		}
		switch item.Type {
		case "message", "": // Older rollouts omit type/channel on visible messages.
			entries = recoveryMessageEntries(item, stats)
		case "function_call", "custom_tool_call", "function_call_output", "custom_tool_call_output":
			entries = recoveryToolEntries(item, stats)
		case "reasoning":
			// Native reasoning, summaries and encrypted fields are never evidence.
		default:
			stats.unsupported++
		}
	case "event_msg":
		entries = recoveryEventMessage(record.Payload, stats)
	case "session_meta", "turn_context", "compacted":
		// Identity was checked at the start; these are not visible messages.
	default:
		stats.unsupported++
	}
	if len(entries) == 0 {
		return nil
	}
	ts, err := time.Parse(time.RFC3339Nano, record.Timestamp)
	if err != nil {
		stats.invalidTimes++
	}
	for i := range entries {
		entries[i].Line, entries[i].Timestamp = line, ts
	}
	return entries
}

func recoveryMessageEntries(item recoveryResponseItem, stats *recoveryReadStats) []core.RecoveryHistoryEntry {
	if item.Role != "user" && item.Role != "assistant" {
		return nil
	}
	if (item.Channel != "" && item.Channel != "final" && item.Channel != "commentary") ||
		(item.Phase != "" && item.Phase != "final" && item.Phase != "commentary") {
		return nil
	}
	want := "input_text"
	if item.Role == "assistant" {
		want = "output_text"
	}
	var parts []string
	for _, block := range item.Content {
		if block.Type != want {
			stats.omittedBlocks++
			continue
		}
		if block.Text != "" && !(item.Role == "user" && recoveryInjectedContext(block.Text)) {
			parts = append(parts, block.Text)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return []core.RecoveryHistoryEntry{{Role: item.Role, Kind: "message", Content: strings.Join(parts, "\n")}}
}

func recoveryInjectedContext(text string) bool {
	t := strings.TrimSpace(text)
	// Do not use isUserPrompt: arbitrary XML can be the user's real request.
	for _, prefix := range []string{"# AGENTS.md instructions for ", "#AGENTS.md instructions for ", "<environment_context>", "<permissions instructions>", "<permissions_instructions>"} {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}

func recoveryToolEntries(item recoveryResponseItem, stats *recoveryReadStats) []core.RecoveryHistoryEntry {
	stats.toolItems++
	entry := core.RecoveryHistoryEntry{Role: "assistant", Kind: "tool_call", CallID: item.CallID}
	if strings.HasSuffix(item.Type, "_output") {
		entry.Role, entry.Kind = "tool", "tool_result"
		output, ok := recoveryToolText(item.Output, stats)
		if !ok {
			stats.omittedBlocks++
			output = "[Unsupported tool output omitted; check the original result before continuing.]"
		}
		entry.Content = "Tool output recorded; business completion is not established by this record.\n" + output
	} else {
		raw := item.Arguments
		if item.Type == "custom_tool_call" {
			raw = item.Input
		}
		input, ok := recoveryToolText(raw, stats)
		if !ok {
			stats.omittedBlocks++
			input = "[Unsupported tool input omitted.]"
		}
		entry.Content = "Tool call recorded; execution and completion are not established by this record.\nTool: " + item.Name + "\n" + input
	}
	return []core.RecoveryHistoryEntry{entry}
}

// Decode explicitly visible text only. In particular, arbitrary output objects
// are not re-serialized: they may contain hidden reasoning or encrypted fields.
func recoveryToolText(raw json.RawMessage, stats *recoveryReadStats) (string, bool) {
	var text string
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false
	}
	if json.Unmarshal(raw, &text) == nil {
		return text, true
	}
	var blocks []recoveryTextBlock
	if json.Unmarshal(raw, &blocks) != nil {
		return "", false
	}
	var parts []string
	for _, block := range blocks {
		if block.Type == "text" || block.Type == "input_text" || block.Type == "output_text" {
			parts = append(parts, block.Text)
		} else {
			stats.omittedBlocks++
		}
	}
	return strings.Join(parts, "\n"), true
}

func recoveryEventMessage(raw json.RawMessage, stats *recoveryReadStats) []core.RecoveryHistoryEntry {
	var event struct {
		Type    string `json:"type"`
		Message string `json:"message"`
		Phase   string `json:"phase"`
		Channel string `json:"channel"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return nil
	}
	role := ""
	switch event.Type {
	case "user_message":
		role = "user"
	case "agent_message":
		role = "assistant"
	default:
		return nil
	}
	if (event.Phase != "" && event.Phase != "final" && event.Phase != "commentary") ||
		(event.Channel != "" && event.Channel != "final" && event.Channel != "commentary") {
		return nil
	}
	if event.Message == "" || (role == "user" && recoveryInjectedContext(event.Message)) {
		return nil
	}
	// Preserve each source occurrence. Text equality is not a safe dedup key:
	// two distinct user changes may have the same wording.
	stats.eventMessages++
	return []core.RecoveryHistoryEntry{{Role: role, Kind: "event_message", Content: event.Message}}
}

const recoveryTruncationMarker = "\n[... recovery export omitted middle text ...]\n"

func truncateRecoveryText(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return text, false
	}
	remaining := limit - len(recoveryTruncationMarker)
	if remaining < 0 {
		return "", true
	}
	head := remaining / 2
	for head > 0 && !utf8.RuneStart(text[head]) {
		head--
	}
	tail := len(text) - (remaining - remaining/2)
	for tail < len(text) && !utf8.RuneStart(text[tail]) {
		tail++
	}
	return text[:head] + recoveryTruncationMarker + text[tail:], true
}

// Keep the opening goal and a rolling recent suffix, rather than stopping at
// the first size limit and silently losing subsequent user cancellations.
type recoveryHistoryCollector struct {
	limits         recoveryHistoryLimits
	prefix         []core.RecoveryHistoryEntry
	tail           []core.RecoveryHistoryEntry
	prefixBytes    int
	tailBytes      int
	prefixClosed   bool
	droppedEntries int
	droppedBytes   int
}

func (c *recoveryHistoryCollector) add(entry core.RecoveryHistoryEntry) {
	size := len(entry.Content) + len(entry.CallID)
	if !c.prefixClosed && len(c.prefix) < c.limits.prefixEntries && c.prefixBytes+size <= c.limits.totalBytes/4 {
		c.prefix = append(c.prefix, entry)
		c.prefixBytes += size
		return
	}
	c.prefixClosed = true
	c.tail = append(c.tail, entry)
	c.tailBytes += size
	for len(c.tail) > 0 && (c.prefixBytes+c.tailBytes > c.limits.totalBytes || len(c.prefix)+len(c.tail) > c.limits.totalEntries) {
		dropped := len(c.tail[0].Content) + len(c.tail[0].CallID)
		c.tail[0] = core.RecoveryHistoryEntry{}
		c.tail = c.tail[1:]
		c.tailBytes -= dropped
		c.droppedBytes += dropped
		c.droppedEntries++
	}
}

func (c *recoveryHistoryCollector) entries() []core.RecoveryHistoryEntry {
	entries := make([]core.RecoveryHistoryEntry, 0, len(c.prefix)+len(c.tail))
	entries = append(entries, c.prefix...)
	return append(entries, c.tail...)
}

func recoveryHistoryWarnings(stats recoveryReadStats, collector recoveryHistoryCollector) []string {
	var warnings []string
	if stats.oversizedLines > 0 {
		warnings = append(warnings, fmt.Sprintf("Skipped %d oversized source lines above %d bytes; later lines were still read.", stats.oversizedLines, collector.limits.lineBytes))
	}
	if stats.malformedLines > 0 {
		warnings = append(warnings, fmt.Sprintf("Skipped %d malformed source records; their visible content may be missing.", stats.malformedLines))
	}
	if stats.unterminatedEnd {
		warnings = append(warnings, "The snapshot ends with an unterminated source line; a trailing record may be incomplete.")
	}
	if stats.omittedBlocks > 0 {
		warnings = append(warnings, fmt.Sprintf("Omitted %d non-text or unsupported content blocks, including possible attachments; recover needed original materials separately.", stats.omittedBlocks))
	}
	if stats.unsupported > 0 {
		warnings = append(warnings, fmt.Sprintf("Omitted %d unsupported source record types; additional visible evidence may be missing.", stats.unsupported))
	}
	if stats.truncatedItems > 0 {
		warnings = append(warnings, fmt.Sprintf("Truncated %d visible items or identifiers: text is limited to %d bytes per item and call IDs to 1024 bytes; retained the beginning and end.", stats.truncatedItems, collector.limits.entryBytes))
	}
	if collector.droppedEntries > 0 {
		warnings = append(warnings, fmt.Sprintf("Omitted %d middle history entries (%d visible bytes) to bound recovery memory; kept the opening history and latest entries. Omitted task changes may still matter.", collector.droppedEntries, collector.droppedBytes))
	}
	if stats.invalidTimes > 0 {
		warnings = append(warnings, fmt.Sprintf("%d visible records have missing or invalid timestamps; source line order is preserved.", stats.invalidTimes))
	}
	if stats.eventMessages > 0 {
		warnings = append(warnings, "Event-message records may mirror response-item messages; matching text alone does not establish separate instructions or duplicate actions.")
	}
	if stats.toolItems > 0 {
		warnings = append(warnings, "Tool calls and outputs are evidence, not a task-completion ledger. Missing output does not prove non-execution; verify side effects and ongoing jobs before repeating work.")
	}
	return warnings
}
