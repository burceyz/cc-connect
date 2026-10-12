package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
)

const recoveryTestSessionID = "019f-test-recovery-thread"

func newRecoveryHistoryTestAgent(t *testing.T) *Agent {
	t.Helper()
	cwd, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Agent{workDir: cwd, codexHome: t.TempDir()}
}

func recoveryHistoryTestRecord(t *testing.T, kind string, payload any) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"timestamp": "2026-10-12T07:45:03+08:00", "type": kind, "payload": payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(data) + "\n"
}

func recoveryHistoryTestMessage(t *testing.T, role, channel, text string) string {
	t.Helper()
	blockType := "input_text"
	if role != "user" {
		blockType = "output_text"
	}
	return recoveryHistoryTestRecord(t, "response_item", map[string]any{
		"type": "message", "role": role, "channel": channel,
		"content": []any{map[string]any{"type": blockType, "text": text}},
	})
}

func recoveryHistoryTestMeta(t *testing.T, a *Agent, id string) string {
	t.Helper()
	return recoveryHistoryTestRecord(t, "session_meta", map[string]any{
		"id": id, "cwd": a.workDir, "source": "cli", "instructions": "PRIVATE_METADATA_INSTRUCTIONS",
	})
}

func writeRecoveryHistoryTestRollout(t *testing.T, a *Agent, id, contents string) string {
	t.Helper()
	dir := filepath.Join(a.codexHome, "sessions", "2026", "10", "12")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "rollout-2026-10-12T07-45-00-"+id+".jsonl")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func recoveryHistoryTestText(history core.RecoveryHistory) string {
	var parts []string
	for _, entry := range history.Entries {
		parts = append(parts, entry.Content)
	}
	return strings.Join(parts, "\n")
}

func requireRecoveryHistoryWarning(t *testing.T, history core.RecoveryHistory, fragment string) {
	t.Helper()
	if !strings.Contains(strings.Join(history.Warnings, "\n"), fragment) {
		t.Fatalf("missing warning %q in %#v", fragment, history.Warnings)
	}
}

func TestRecoveryHistory_ExportsVisibleTaskAndLatestChangeWithoutPrivateItems(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestMessage(t, "user", "", "完成 A，但先不要部署。严禁排查上游。") +
		recoveryHistoryTestMessage(t, "assistant", "commentary", "已完成前两步，准备核对文件。") +
		recoveryHistoryTestMessage(t, "assistant", "analysis", "PRIVATE_ANALYSIS_TEXT") +
		recoveryHistoryTestMessage(t, "system", "", "PRIVATE_SYSTEM_TEXT") +
		recoveryHistoryTestMessage(t, "developer", "", "PRIVATE_DEVELOPER_TEXT") +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "reasoning", "role": "assistant", "text": "PRIVATE_REASONING_TEXT",
			"encrypted_content": "PRIVATE_CIPHERTEXT", "summary": []any{map[string]any{"text": "PRIVATE_SUMMARY"}},
			"content": []any{map[string]any{"type": "output_text", "text": "PRIVATE_MISLEADING_ROLE"}},
		}) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "message", "role": "assistant", "phase": "analysis",
			"content": []any{map[string]any{"type": "output_text", "text": "PRIVATE_ANALYSIS_PHASE"}},
		}) +
		recoveryHistoryTestRecord(t, "event_msg", map[string]any{
			"type": "agent_reasoning", "message": "PRIVATE_EVENT_REASONING",
		}) +
		recoveryHistoryTestRecord(t, "compacted", map[string]any{
			"message": "PRIVATE_COMPACTION_SUMMARY",
		}) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "message", "role": "assistant", "channel": "final", "encrypted_content": "PRIVATE_NESTED_CIPHERTEXT",
			"content": []any{map[string]any{"type": "output_text", "text": "invalid_encrypted_content 是此次错误代码。"}},
		}) +
		recoveryHistoryTestMessage(t, "user", "", "<request>暂停 A，先完成 B；不要重复发送之前的通知。</request>")
	path := writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	canonicalPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil {
		t.Fatal(err)
	}
	text := recoveryHistoryTestText(history)
	if strings.Contains(text, "PRIVATE_") {
		t.Fatalf("private native fields reached recovery material: %s", text)
	}
	for _, want := range []string{"完成 A，但先不要部署", "已完成前两步", "invalid_encrypted_content", "<request>暂停 A"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing visible task evidence %q", want)
		}
	}
	if len(history.Entries) != 4 || history.Entries[3].Line != 12 {
		t.Fatalf("entries or source positions differ: %#v", history.Entries)
	}
	if history.Source != canonicalPath || history.Entries[0].Timestamp.IsZero() {
		t.Fatalf("source identity or visible timestamp missing: %#v", history)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != contents {
		t.Fatal("recovery modified the original native rollout")
	}
}

func TestRecoveryHistory_ToolEvidenceDoesNotInventCompletion(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "function_call", "name": "exec_command", "call_id": "finished-call", "arguments": `{"cmd":"git status --short"}`,
			"encrypted_content": "PRIVATE_TOOL_CIPHERTEXT",
		}) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "function_call_output", "call_id": "finished-call", "output": "Process exited with code 0\n M app.go",
		}) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "custom_tool_call", "name": "apply_patch", "call_id": "unknown-call", "input": "*** Begin Patch\n*** End Patch",
		}) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "custom_tool_call_output", "call_id": "unreadable-output",
			"output": map[string]any{"encrypted_content": "PRIVATE_OBJECT_CIPHERTEXT", "summary": "PRIVATE_OBJECT_SUMMARY"},
		}) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "function_call_output", "call_id": "visible-blocks", "output": []any{
				map[string]any{"type": "text", "text": "job accepted: job-17"},
				map[string]any{"type": "image", "data": "PRIVATE_IMAGE_DATA"},
			},
		})
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Entries) != 5 {
		t.Fatalf("expected five visible tool observations, got %#v", history.Entries)
	}
	text := recoveryHistoryTestText(history)
	if strings.Contains(text, "PRIVATE_") || !strings.Contains(text, "job-17") || !strings.Contains(text, "M app.go") {
		t.Fatalf("tool text filtering failed: %s", text)
	}
	for _, entry := range history.Entries {
		if entry.CallID == "unknown-call" && (entry.Kind != "tool_call" || !strings.Contains(entry.Content, "not established")) {
			t.Fatalf("call with no result claimed completion: %#v", entry)
		}
	}
	if history.Entries[1].Role != "tool" || history.Entries[1].CallID != "finished-call" {
		t.Fatalf("lost tool result identity: %#v", history.Entries[1])
	}
	requireRecoveryHistoryWarning(t, history, "side effects")
	requireRecoveryHistoryWarning(t, history, "unsupported content")
}

func TestRecoveryHistory_RejectsWrongIdentityOrWorkingDirectory(t *testing.T) {
	for _, tc := range []string{"different-id", "different-cwd", "missing-cwd", "relative-cwd", "subagent", "missing-meta", "malformed-meta"} {
		t.Run(tc, func(t *testing.T) {
			a := newRecoveryHistoryTestAgent(t)
			meta := map[string]any{"id": recoveryTestSessionID, "cwd": a.workDir, "source": "cli"}
			switch tc {
			case "different-id":
				meta["id"] = "other-thread"
			case "different-cwd":
				meta["cwd"] = t.TempDir()
			case "missing-cwd":
				delete(meta, "cwd")
			case "relative-cwd":
				meta["cwd"] = "."
			case "subagent":
				meta["source"] = map[string]any{"subagent": map[string]any{"thread_spawn": map[string]any{"parent_thread_id": recoveryTestSessionID}}}
			}
			contents := recoveryHistoryTestRecord(t, "session_meta", meta)
			if tc == "missing-meta" {
				contents = ""
			} else if tc == "malformed-meta" {
				contents = "{broken initial metadata\n"
			}
			contents += recoveryHistoryTestMessage(t, "user", "", "FOREIGN_TASK_MUST_NOT_EXPORT")
			// A later copied metadata record cannot repair a wrong first identity.
			contents += recoveryHistoryTestMeta(t, a, recoveryTestSessionID)
			writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
			history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
			if err == nil || len(history.Entries) != 0 {
				t.Fatalf("unverified rollout exported: history=%#v err=%v", history, err)
			}
		})
	}
}

func TestRecoveryHistory_RequiresUnambiguousExactFileAndRejectsSymlink(t *testing.T) {
	for _, tc := range []string{"substring-only", "duplicate", "symlink"} {
		t.Run(tc, func(t *testing.T) {
			a := newRecoveryHistoryTestAgent(t)
			contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID)
			id := recoveryTestSessionID
			if tc == "substring-only" {
				id += "-extra"
			}
			path := writeRecoveryHistoryTestRollout(t, a, id, contents)
			if tc == "duplicate" {
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), "second-"+id+".jsonl"), []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if tc == "symlink" {
				outside := filepath.Join(t.TempDir(), "outside.jsonl")
				if err := os.Rename(path, outside); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, path); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			}
			history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
			if err == nil || len(history.Entries) != 0 {
				t.Fatalf("unexpected export for %s: %#v %v", tc, history, err)
			}
		})
	}
}

func TestRecoveryHistory_CanonicalWorkingDirectoryAndLegacyVisibleMessage(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	realCWD := a.workDir
	alias := filepath.Join(t.TempDir(), "workspace-alias")
	if err := os.Symlink(realCWD, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "legacy visible answer"}},
		})
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	a.workDir = alias
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil || recoveryHistoryTestText(history) != "legacy visible answer" {
		t.Fatalf("canonical cwd or legacy message failed: %#v %v", history, err)
	}
}

func TestRecoveryHistory_OversizedLineDoesNotHideLaterCancellation(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestMessage(t, "user", "", "原目标：完成仓库整理。") +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "reasoning", "encrypted_content": strings.Repeat("X", defaultRecoveryHistoryLimits.lineBytes+1024),
		}) +
		recoveryHistoryTestMessage(t, "user", "", "最新指令：取消发布，只保留本地修改。")
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Entries) != 2 || !strings.Contains(history.Entries[1].Content, "取消发布") || history.Entries[1].Line != 4 {
		t.Fatalf("later cancellation was lost after oversized line: %#v", history.Entries)
	}
	requireRecoveryHistoryWarning(t, history, "oversized source lines")
}

func TestRecoveryHistory_TruncatesLargeVisibleItemKeepingBothEndsAndUTF8(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	text := "最初目标：保留 Hermes 能力。" + strings.Repeat("中间材料", 30000) + "最后更新：先不要部署。"
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) + recoveryHistoryTestMessage(t, "user", "", text)
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil || len(history.Entries) != 1 {
		t.Fatalf("large visible entry unreadable: %#v %v", history, err)
	}
	got := history.Entries[0].Content
	if !utf8.ValidString(got) || len(got) > defaultRecoveryHistoryLimits.entryBytes || !strings.HasPrefix(got, "最初目标") || !strings.HasSuffix(got, "先不要部署。") {
		t.Fatalf("invalid first/last retention or byte limit: length=%d", len(got))
	}
	requireRecoveryHistoryWarning(t, history, "Truncated")
}

func TestRecoveryHistory_MalformedAndUnterminatedRecordsAreExplicit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		suffix      string
		wantEntries int
	}{
		{"incomplete-tail", `{"type":"response_item","payload":{"type":"message"`, 2},
		{"valid-no-newline", strings.TrimSuffix(recoveryHistoryTestMessage(t, "user", "", "最后取消 A"), "\n"), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newRecoveryHistoryTestAgent(t)
			contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
				recoveryHistoryTestMessage(t, "user", "", "开始 A") + "{malformed middle\n" +
				recoveryHistoryTestMessage(t, "user", "", "先检查进度") + tc.suffix
			writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
			history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
			if err != nil || len(history.Entries) != tc.wantEntries {
				t.Fatalf("usable visible records were lost: %#v %v", history, err)
			}
			requireRecoveryHistoryWarning(t, history, "malformed")
			requireRecoveryHistoryWarning(t, history, "unterminated")
		})
	}
}

func TestRecoveryHistory_TotalBoundsPreserveOpeningGoalAndLatestChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries int
		bytes   int
		middle  string
	}{
		{"byte-bound", 100, 2048, strings.Repeat("visible evidence ", 25)},
		{"entry-bound", 8, 16384, "small item"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newRecoveryHistoryTestAgent(t)
			var contents strings.Builder
			contents.WriteString(recoveryHistoryTestMeta(t, a, recoveryTestSessionID))
			contents.WriteString(recoveryHistoryTestMessage(t, "user", "", "原目标：完成任务 A"))
			for range 80 {
				contents.WriteString(recoveryHistoryTestMessage(t, "assistant", "commentary", tc.middle))
			}
			contents.WriteString(recoveryHistoryTestMessage(t, "user", "", "最新修改：取消 A，继续 B；不部署"))
			limits := recoveryHistoryLimits{lineBytes: 4096, entryBytes: 512, totalBytes: tc.bytes, totalEntries: tc.entries, prefixEntries: 2}
			history, err := readRecoveryHistory(context.Background(), strings.NewReader(contents.String()), recoveryTestSessionID, a.workDir, limits)
			if err != nil || len(history.Entries) < 2 {
				t.Fatalf("bounded read failed: %#v %v", history, err)
			}
			if history.Entries[0].Content != "原目标：完成任务 A" || history.Entries[len(history.Entries)-1].Content != "最新修改：取消 A，继续 B；不部署" {
				t.Fatalf("opening or latest user instruction missing: %#v", history.Entries)
			}
			bytes := 0
			for _, entry := range history.Entries {
				bytes += len(entry.Content) + len(entry.CallID)
			}
			if bytes > tc.bytes || len(history.Entries) > tc.entries {
				t.Fatalf("memory bounds exceeded: %d bytes, %d entries", bytes, len(history.Entries))
			}
			requireRecoveryHistoryWarning(t, history, "middle history entries")
		})
	}
}

func TestRecoveryHistory_DoesNotDeduplicateDistinctEqualUserInputs(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestMessage(t, "user", "", "继续") +
		recoveryHistoryTestMessage(t, "user", "", "继续") +
		recoveryHistoryTestRecord(t, "event_msg", map[string]any{"type": "user_message", "message": "最新输入：任务取消"}) +
		recoveryHistoryTestRecord(t, "event_msg", map[string]any{"type": "agent_message", "message": "PRIVATE_EVENT_ANALYSIS", "phase": "analysis"})
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil || len(history.Entries) != 3 {
		t.Fatalf("distinct source inputs were incorrectly deduplicated: %#v %v", history, err)
	}
	if history.Entries[0].Line == history.Entries[1].Line || history.Entries[2].Content != "最新输入：任务取消" {
		t.Fatalf("lost repeated input identity or latest event message: %#v", history.Entries)
	}
	requireRecoveryHistoryWarning(t, history, "may mirror")
}

type recoveryHistoryCancelReader struct {
	io.Reader
	cancel context.CancelFunc
	reads  int
}

func (r *recoveryHistoryCancelReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.reads++
	if r.reads == 2 {
		r.cancel()
	}
	return n, err
}

func TestRecoveryHistory_ObservesCancellationBeforeAndDuringLongLine(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.GetRecoveryHistory(ctx, recoveryTestSessionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup error = %v", err)
	}
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestMessage(t, "user", "", "已知目标") + strings.Repeat("X", 1<<20)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	source := &recoveryHistoryCancelReader{Reader: strings.NewReader(contents), cancel: cancel}
	history, err := readRecoveryHistory(ctx, source, recoveryTestSessionID, a.workDir, defaultRecoveryHistoryLimits)
	if !errors.Is(err, context.Canceled) || source.reads > 2 {
		t.Fatalf("long-line reading did not observe cancellation: reads=%d err=%v", source.reads, err)
	}
	if len(history.Entries) != 1 || history.Entries[0].Content != "已知目标" {
		t.Fatalf("lost already verified partial evidence: %#v", history.Entries)
	}
	requireRecoveryHistoryWarning(t, history, "later task changes may be missing")
}

func TestRecoveryHistory_ReportsEmptyVisibleHistoryAndMissingAttachments(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_image", "image_url": "PRIVATE_IMAGE"}},
		}) +
		recoveryHistoryTestMessage(t, "user", "", "# AGENTS.md instructions for /workspace\n<INSTRUCTIONS>PRIVATE_INJECTED</INSTRUCTIONS>") +
		recoveryHistoryTestMessage(t, "user", "", "<environment_context>PRIVATE_ENVIRONMENT</environment_context>")
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil || len(history.Entries) != 0 {
		t.Fatalf("metadata or attachments leaked: %#v %v", history, err)
	}
	requireRecoveryHistoryWarning(t, history, "attachments")
	requireRecoveryHistoryWarning(t, history, "No supported visible history")
}

func TestRecoveryHistory_InvalidSessionIDCannotReadArbitraryFiles(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	for _, id := range []string{"", ".", "..", "../other", "/absolute", "part/id", "thread id"} {
		if history, err := a.GetRecoveryHistory(context.Background(), id); err == nil || len(history.Entries) != 0 {
			t.Fatalf("invalid id %q was accepted: %#v %v", id, history, err)
		}
	}
}

func TestRecoveryHistory_UnknownNativeSchemaIsOmittedWithWarning(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
		recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "future_private_or_visible_item", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": "UNKNOWN_CONTENT_MUST_NOT_EXPORT"}},
		}) + recoveryHistoryTestMessage(t, "user", "", "最新输入仍需保留")
	writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
	history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
	if err != nil || recoveryHistoryTestText(history) != "最新输入仍需保留" {
		t.Fatalf("unknown schema was trusted or blocked later input: %#v %v", history, err)
	}
	requireRecoveryHistoryWarning(t, history, "unsupported source record types")
}
