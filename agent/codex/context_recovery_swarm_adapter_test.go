package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// Swarm-review regressions. All native histories are synthetic and every
// executor below is the existing local TestContextRecoveryProcessHelper.
func TestContextRecoverySwarmAdapter_FinalAnswerPhaseRemainsVisible(t *testing.T) {
	for _, recordType := range []string{"response_item", "event_msg"} {
		t.Run(recordType, func(t *testing.T) {
			a := newRecoveryHistoryTestAgent(t)
			const finalText = "文件已写入；最新检查结果仍待核对。"
			var payload any
			if recordType == "response_item" {
				payload = map[string]any{
					"type": "message", "role": "assistant", "phase": "final_answer",
					"content": []any{map[string]any{"type": "output_text", "text": finalText}},
				}
			} else {
				payload = map[string]any{"type": "agent_message", "phase": "final_answer", "message": finalText}
			}
			contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
				recoveryHistoryTestMessage(t, "user", "", "先检查现有文件。") +
				recoveryHistoryTestRecord(t, recordType, payload)
			writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
			history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(recoveryHistoryTestText(history), finalText) {
				t.Fatalf("visible final_answer was discarded: entries=%#v warnings=%#v", history.Entries, history.Warnings)
			}
		})
	}
}

func TestContextRecoverySwarmAdapter_FinalAnswerDoesNotExposeHiddenPhases(t *testing.T) {
	for _, recordType := range []string{"response_item", "event_msg"} {
		for _, tc := range []struct{ phase, channel string }{
			{phase: "analysis"}, {phase: "reasoning"}, {phase: "future_phase"},
			{phase: "final_answer", channel: "analysis"},
		} {
			t.Run(recordType+"/"+tc.phase+"/"+tc.channel, func(t *testing.T) {
				a := newRecoveryHistoryTestAgent(t)
				payload := map[string]any{"phase": tc.phase, "channel": tc.channel}
				if recordType == "response_item" {
					payload["type"], payload["role"] = "message", "assistant"
					payload["content"] = []any{map[string]any{"type": "output_text", "text": "PRIVATE_CONTENT"}}
				} else {
					payload["type"], payload["message"] = "agent_message", "PRIVATE_CONTENT"
				}
				contents := recoveryHistoryTestMeta(t, a, recoveryTestSessionID) +
					recoveryHistoryTestRecord(t, recordType, payload)
				writeRecoveryHistoryTestRollout(t, a, recoveryTestSessionID, contents)
				history, err := a.GetRecoveryHistory(context.Background(), recoveryTestSessionID)
				if err != nil || len(history.Entries) != 0 {
					t.Fatalf("hidden or unknown phase exported: entries=%#v err=%v", history.Entries, err)
				}
			})
		}
	}
}

func TestContextRecoverySwarmAdapter_HistoryUsesExecutorHome(t *testing.T) {
	a, _, _ := recoveryTestAgent(t, true, nil)
	executorHome, hostHome := t.TempDir(), t.TempDir()
	t.Setenv("CODEX_HOME", hostHome)
	if err := os.MkdirAll(filepath.Join(hostHome, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	a.codexHome = ""
	a.configEnv = append(a.configEnv, "CODEX_HOME="+executorHome)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := a.StartSession(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	s := session.(*appServerSession)
	defer swarmAdapterCloseAndCheck(t, s)
	if envSliceToMap(s.processEnv)["CODEX_HOME"] != executorHome {
		t.Fatal("fixture did not use the project-configured executor home")
	}
	fixture := &Agent{codexHome: executorHome, workDir: s.GetWorkDir()}
	const latest = "最新修改：取消发布，只检查已有结果。"
	contents := recoveryHistoryTestMeta(t, fixture, s.CurrentSessionID()) +
		recoveryHistoryTestMessage(t, "user", "", latest)
	writeRecoveryHistoryTestRollout(t, fixture, s.CurrentSessionID(), contents)
	// The original counterexample called a.GetRecoveryHistory here and looked
	// in hostHome. The executor provider must work without changing that Agent.
	provider := session.(core.RecoveryHistoryProvider)
	history, err := provider.GetRecoveryHistory(ctx, s.CurrentSessionID())
	if err != nil || !strings.Contains(recoveryHistoryTestText(history), latest) {
		t.Fatalf("history did not use the original executor's CODEX_HOME: err=%v entries=%#v", err, history.Entries)
	}
	if a.codexHome != "" {
		t.Fatal("session history export mutated the shared Agent home")
	}
}

func TestContextRecoverySwarmAdapter_HistoryUsesCapturedHomeFallback(t *testing.T) {
	a, _, _ := recoveryTestAgent(t, true, nil)
	executorHome, changedHome := t.TempDir(), t.TempDir()
	a.codexHome = ""
	a.configEnv = append(a.configEnv, "CODEX_HOME=", "HOME="+executorHome, "USERPROFILE="+executorHome)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := a.StartSession(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	s := session.(*appServerSession)
	defer swarmAdapterCloseAndCheck(t, s)
	t.Setenv("CODEX_HOME", changedHome)
	t.Setenv("HOME", changedHome)
	t.Setenv("USERPROFILE", changedHome)
	fixture := &Agent{codexHome: filepath.Join(executorHome, ".codex"), workDir: s.GetWorkDir()}
	const latest = "最新输入保留在执行器自己的 HOME 中。"
	contents := recoveryHistoryTestMeta(t, fixture, s.CurrentSessionID()) + recoveryHistoryTestMessage(t, "user", "", latest)
	writeRecoveryHistoryTestRollout(t, fixture, s.CurrentSessionID(), contents)
	history, err := session.(core.RecoveryHistoryProvider).GetRecoveryHistory(ctx, s.CurrentSessionID())
	if err != nil || !strings.Contains(recoveryHistoryTestText(history), latest) {
		t.Fatalf("history used a changed host environment: err=%v entries=%#v", err, history.Entries)
	}
}

func TestContextRecoverySwarmAdapter_RelativeHomeUsesProcessDirectory(t *testing.T) {
	a, _, _ := recoveryTestAgent(t, true, nil)
	a.codexHome = ""
	a.configEnv = append(a.configEnv, "CODEX_HOME=native-home")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := a.StartSession(ctx, "original-thread")
	if err != nil {
		t.Fatal(err)
	}
	s := session.(*appServerSession)
	defer swarmAdapterCloseAndCheck(t, s)
	fixture := &Agent{codexHome: filepath.Join(a.workDir, "native-home"), workDir: s.GetWorkDir()}
	const latest = "原进程目录与恢复会话目录不同。"
	contents := recoveryHistoryTestMeta(t, fixture, s.CurrentSessionID()) + recoveryHistoryTestMessage(t, "user", "", latest)
	writeRecoveryHistoryTestRollout(t, fixture, s.CurrentSessionID(), contents)
	history, err := session.(core.RecoveryHistoryProvider).GetRecoveryHistory(ctx, s.CurrentSessionID())
	if err != nil || !strings.Contains(recoveryHistoryTestText(history), latest) {
		t.Fatalf("relative CODEX_HOME was resolved against the wrong directory: err=%v entries=%#v", err, history.Entries)
	}
}

func TestContextRecoverySwarmAdapter_HistoryUsesResumedWorkingDirectory(t *testing.T) {
	a, _, _ := recoveryTestAgent(t, true, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := a.StartSession(ctx, "original-thread")
	if err != nil {
		t.Fatal(err)
	}
	s := session.(*appServerSession)
	defer swarmAdapterCloseAndCheck(t, s)
	if sameRecoveryDirectory(a.workDir, s.GetWorkDir()) {
		t.Fatal("fixture needs the server-reported resumed cwd to differ from the agent default")
	}
	fixture := &Agent{codexHome: a.codexHome, workDir: s.GetWorkDir()}
	const latest = "原会话最新修改：暂停 A，核对 B 的既有回执。"
	contents := recoveryHistoryTestMeta(t, fixture, s.CurrentSessionID()) +
		recoveryHistoryTestMessage(t, "user", "", latest)
	writeRecoveryHistoryTestRollout(t, fixture, s.CurrentSessionID(), contents)
	// Calling the old Agent provider rejected this exact native thread because
	// its default cwd differs. Select the session provider, leaving a unchanged.
	history, err := session.(core.RecoveryHistoryProvider).GetRecoveryHistory(ctx, s.CurrentSessionID())
	if err != nil || !strings.Contains(recoveryHistoryTestText(history), latest) {
		t.Fatalf("verified resumed thread's history was rejected using shared default cwd: err=%v entries=%#v", err, history.Entries)
	}
	if sameRecoveryDirectory(a.workDir, s.GetWorkDir()) {
		t.Fatal("session history export mutated the shared Agent cwd")
	}
}

func TestContextRecoverySwarmAdapter_StartupErrorExportsOnlyOriginalThread(t *testing.T) {
	a, _, _ := recoveryTestAgent(t, true, map[string]string{"CC_RECOVERY_FAIL_RESUME": "1"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, startupErr := a.StartSession(ctx, "original-thread")
	var provider core.RecoveryHistoryProvider
	if !errors.As(startupErr, &provider) {
		t.Fatalf("startup error did not expose its old executor history: %v", startupErr)
	}
	var starter *contextRecoveryStartupError
	if !errors.As(startupErr, &starter) {
		t.Fatalf("startup error did not retain the stopped executor: %v", startupErr)
	}
	defer swarmAdapterCloseAndCheck(t, starter.session)
	fixture := &Agent{codexHome: a.codexHome, workDir: a.workDir}
	for _, id := range []string{"original-thread", "unrelated-thread"} {
		contents := recoveryHistoryTestMeta(t, fixture, id) + recoveryHistoryTestMessage(t, "user", "", "最新输入："+id)
		writeRecoveryHistoryTestRollout(t, fixture, id, contents)
	}
	a.SetWorkDir(t.TempDir())
	history, err := provider.GetRecoveryHistory(ctx, "original-thread")
	if err != nil || !strings.Contains(recoveryHistoryTestText(history), "最新输入：original-thread") {
		t.Fatalf("startup history lost its original executor scope: err=%v entries=%#v", err, history.Entries)
	}
	if history, err := provider.GetRecoveryHistory(ctx, "unrelated-thread"); err == nil || len(history.Entries) != 0 {
		t.Fatalf("startup provider exported a different valid native thread: err=%v entries=%#v", err, history.Entries)
	}
}

func TestContextRecoverySwarmAdapter_MissingExecutorHomeCannotReadHostHome(t *testing.T) {
	t.Setenv("CODEX_HOME", t.TempDir())
	if _, err := recoveryExecutorHome([]string{"UNRELATED=value"}); err == nil {
		t.Fatal("missing captured executor home fell back to the current host environment")
	}
}

func TestContextRecoverySwarmAdapter_ExecutorHomePreservesEnvironmentValue(t *testing.T) {
	want := filepath.Join(t.TempDir(), " native home ")
	got, err := recoveryExecutorHome([]string{"CODEX_HOME=old-value", "CODEX_HOME=" + want})
	if err != nil || got != want {
		t.Fatalf("captured environment path was trimmed or an older assignment won: got=%q want=%q err=%v", got, want, err)
	}
	got, err = recoveryExecutorHome([]string{"CODEX_HOME=", "HOME=" + want, "USERPROFILE=" + want})
	if err != nil || got != filepath.Join(want, ".codex") {
		t.Fatalf("captured user-home path was changed: got=%q err=%v", got, err)
	}
}

func TestContextRecoverySwarmAdapter_IdleBeforeFailureProbe(t *testing.T) {
	s := &appServerSession{
		appServerSessionOptions: appServerSessionOptions{contextRecovery: true},
		events:                  make(chan core.Event, 8), currentTurn: "turn-1",
	}
	s.threadID.Store("thread-1")
	s.handleNotification("thread/status/changed", json.RawMessage(`{"threadId":"thread-1","status":{"type":"idle"}}`))
	s.handleNotification("error", json.RawMessage(`{"threadId":"thread-1","turnId":"turn-1","willRetry":false,"error":{"message":"rejected context","code":"invalid_encrypted_content"}}`))
	var events []core.Event
	for len(s.events) > 0 {
		events = append(events, <-s.events)
	}
	// This records the local state-machine result without assuming the real
	// server can send this ordering. Protocol ordering is outside this probe.
	t.Logf("idle then terminal invalid_encrypted_content produced: %#v", events)
}

func TestContextRecoverySwarmAdapter_BoundsKeepLatestUserChangeBeforeToolTail(t *testing.T) {
	a := newRecoveryHistoryTestAgent(t)
	const latest = "最新修改：取消发布，现有任务只做检查。"
	var contents strings.Builder
	contents.WriteString(recoveryHistoryTestMeta(t, a, recoveryTestSessionID))
	contents.WriteString(recoveryHistoryTestMessage(t, "user", "", "最初目标：准备发布 A。"))
	contents.WriteString(recoveryHistoryTestMessage(t, "assistant", "commentary", "开始检查现有材料。"))
	contents.WriteString(recoveryHistoryTestMessage(t, "user", "", latest))
	for range 20 {
		contents.WriteString(recoveryHistoryTestRecord(t, "response_item", map[string]any{
			"type": "function_call_output", "call_id": "inspection", "output": "Read-only inspection result.",
		}))
	}
	// Small limits exercise the same ring eviction as the default 8192-entry
	// and 16 MiB caps without constructing an unnecessarily large fixture.
	limits := recoveryHistoryLimits{lineBytes: 4096, entryBytes: 512, totalBytes: 4096, totalEntries: 8, prefixEntries: 2}
	history, err := readRecoveryHistory(context.Background(), strings.NewReader(contents.String()), recoveryTestSessionID, a.workDir, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(recoveryHistoryTestText(history), latest) {
		t.Fatalf("latest user change was evicted by later tool records: entries=%d opening=%q warnings=%#v", len(history.Entries), history.Entries[0].Content, history.Warnings)
	}
	swarmAdapterCheckHistoryBounds(t, history, limits)
}

func TestContextRecoverySwarmAdapter_UserWindowIsBoundedAndOrdered(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits recoveryHistoryLimits
	}{
		{"entry-bound", recoveryHistoryLimits{lineBytes: 4096, entryBytes: 512, totalBytes: 4096, totalEntries: 8, prefixEntries: 16}},
		{"byte-bound", recoveryHistoryLimits{lineBytes: 4096, entryBytes: 512, totalBytes: 1024, totalEntries: 100, prefixEntries: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newRecoveryHistoryTestAgent(t)
			var contents strings.Builder
			contents.WriteString(recoveryHistoryTestMeta(t, a, recoveryTestSessionID))
			contents.WriteString(recoveryHistoryTestMessage(t, "user", "", "原目标"))
			contents.WriteString(recoveryHistoryTestMessage(t, "assistant", "commentary", "最初检查"))
			for range 30 {
				contents.WriteString(recoveryHistoryTestMessage(t, "user", "", strings.Repeat("早期补充输入", 8)))
			}
			const cancel = "最近修改：取消发布。"
			const change = "随后修改：只核对 B，不发通知。"
			contents.WriteString(recoveryHistoryTestMessage(t, "user", "", cancel))
			contents.WriteString(recoveryHistoryTestMessage(t, "user", "", change))
			for range 40 {
				contents.WriteString(recoveryHistoryTestRecord(t, "response_item", map[string]any{
					"type": "function_call_output", "call_id": "inspection", "output": "Latest read-only inspection evidence.",
				}))
			}
			history, err := readRecoveryHistory(context.Background(), strings.NewReader(contents.String()), recoveryTestSessionID, a.workDir, tc.limits)
			if err != nil {
				t.Fatal(err)
			}
			text := recoveryHistoryTestText(history)
			for _, want := range []string{"原目标", cancel, change, "Latest read-only inspection evidence."} {
				if !strings.Contains(text, want) {
					t.Fatalf("lost required recent evidence %q: %#v", want, history.Entries)
				}
			}
			requireRecoveryHistoryWarning(t, history, "older user inputs")
			requireRecoveryHistoryWarning(t, history, "Source line gaps")
			swarmAdapterCheckHistoryBounds(t, history, tc.limits)
		})
	}
}

func swarmAdapterCheckHistoryBounds(t *testing.T, history core.RecoveryHistory, limits recoveryHistoryLimits) {
	t.Helper()
	totalBytes, previousLine := 0, 0
	for _, entry := range history.Entries {
		totalBytes += len(entry.Content) + len(entry.CallID)
		if entry.Line <= previousLine {
			t.Fatalf("history is duplicated or out of source order: %d follows %d", entry.Line, previousLine)
		}
		previousLine = entry.Line
	}
	if totalBytes > limits.totalBytes || len(history.Entries) > limits.totalEntries {
		t.Fatalf("history exceeds total bounds: bytes=%d entries=%d", totalBytes, len(history.Entries))
	}
}

func swarmAdapterCloseAndCheck(t *testing.T, s *appServerSession) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Errorf("close fake executor: %v", err)
	}
	s.procMu.Lock()
	confirmed := s.processExitConfirmed
	pid := s.cmd.Process.Pid
	s.procMu.Unlock()
	if !confirmed {
		t.Error("fake executor has no confirmed process wait status")
	}
	waitStopped := false
	select {
	case <-s.processDone:
		waitStopped = true
	default:
		t.Error("fake executor wait goroutine is still running")
	}
	workersStopped := false
	select {
	case <-s.workersDone():
		workersStopped = true
	default:
		t.Error("fake executor transport workers are still running")
	}
	if confirmed && waitStopped && workersStopped {
		t.Logf("fake executor pid=%d reaped; transport workers stopped", pid)
	}
}
