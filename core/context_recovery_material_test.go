package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type recoveryMaterialExporterAgent struct {
	stubAgent
	history RecoveryHistory
	err     error
	calls   []string
	ctxErr  error
	starts  int
}

func (a *recoveryMaterialExporterAgent) GetRecoveryHistory(ctx context.Context, id string) (RecoveryHistory, error) {
	a.calls = append(a.calls, id)
	a.ctxErr = ctx.Err()
	return a.history, a.err
}

func (a *recoveryMaterialExporterAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.starts++
	return nil, errors.New("material collection must not start an executor")
}

func recoveryMaterialFixture(t *testing.T) (*Session, *recoveryInvocation, *ContextRecoveryRecord, *recoveryMaterialExporterAgent) {
	t.Helper()
	stamp := time.Date(2026, 10, 12, 7, 45, 3, 0, time.FixedZone("CST", 8*60*60))
	invocation := &recoveryInvocation{MessageID: "original-input", Content: "继续完成原任务", AcceptedAt: stamp,
		Origin: InvocationOrigin{Kind: "user", ID: "original-input"}}
	record := &ContextRecoveryRecord{ID: strings.Repeat("a", 32), FromSessionID: "original-native-thread", MessageID: invocation.MessageID,
		Reason: RecoveryInvalidEncryptedContent, Phase: "reserved", Origin: invocation.Origin,
		InputStatus: "not_sent", TurnStatus: "not_started", StartedAt: stamp, UpdatedAt: stamp,
		ArtifactDir: filepath.Join(t.TempDir(), "context-recovery", strings.Repeat("a", 32))}
	session := &Session{ID: "s-original", AgentSessionID: record.FromSessionID}
	session.AddHistory("user", "原目标：修复功能并检查所有剩余工作；先不要部署；严禁排查上游。")
	session.AddHistory("assistant", "已完成文件写入；job-17 仍在运行，先查询该任务。")
	agent := &recoveryMaterialExporterAgent{history: RecoveryHistory{
		Source:   filepath.Join(filepath.Dir(record.ArtifactDir), "raw-private-rollout.jsonl"),
		Entries:  []RecoveryHistoryEntry{{Role: "user", Kind: "message", Content: "补充：保留 Hermes 正常能力", Line: 2}},
		Warnings: []string{"Native export skipped an oversized line; check needed missing evidence."},
	}}
	return session, invocation, record, agent
}

func requireRecoveryMaterialText(t *testing.T, text string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			t.Errorf("recovery evidence or instruction missing %q", fragment)
		}
	}
}

func TestRecoveryMaterial_CollectsOnDemandWithoutPeriodicModelStateOrExecution(t *testing.T) {
	session, invocation, record, agent := recoveryMaterialFixture(t)
	session.AddHistory("user", "最后要求：暂时只检查，不发布")
	if len(agent.calls) != 0 {
		t.Fatal("normal history recording unexpectedly requested a recovery export")
	}
	m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
	if len(agent.calls) != 1 || agent.calls[0] != "original-native-thread" || agent.starts != 0 {
		t.Fatalf("material collection read the wrong thread or started a model: %#v", agent)
	}
	if _, err := os.Stat(record.ArtifactDir); !os.IsNotExist(err) {
		t.Fatalf("collection should not implicitly write task checkpoints: %v", err)
	}
	// The snapshot cannot change when host history or the recovery ledger moves on.
	session.mu.Lock()
	session.History[0].Content = "a later independent mutation"
	session.mu.Unlock()
	session.AddHistory("user", "arrived after the snapshot")
	record.Phase, record.FromSessionID = "changed", "different-thread"
	if len(m.HostHistory) != 3 || !strings.Contains(m.HostHistory[0].Content, "原目标") || m.Record.Phase != "reserved" || m.Record.FromSessionID != "original-native-thread" {
		t.Fatalf("collected evidence was not an independent snapshot: %#v", m)
	}
	requireRecoveryMaterialText(t, recoveryPrompt(m), "No periodic model checkpoint", "original model, reasoning effort, working directory and permissions", "No additional access or upstream investigation")
}

func TestRecoveryMaterial_LatestCancellationOverridesOldGoalAndSourceRemainsEvidence(t *testing.T) {
	session, invocation, record, agent := recoveryMaterialFixture(t)
	invocation.Content = "旧要求：现在提交全部改动"
	m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
	m.LaterInputs = []*recoveryInvocation{
		{MessageID: "later-1", Content: "最新要求：取消提交，只审查已有结果", Origin: InvocationOrigin{Kind: "user", ID: "later-1"}},
		{MessageID: "later-2", Content: "继续审查，仍然不要部署", Origin: InvocationOrigin{Kind: "user", ID: "later-2"}},
	}
	m.SteeredInputs = []*recoverySteeredInput{
		{Input: IPCInput{EventID: strings.Repeat("b", 64), Prompt: "job-17 尚未完成，请取原回执", Target: IPCTarget{AgentSessionID: record.FromSessionID}}, Status: "unknown"},
	}
	prompt := recoveryPrompt(m)
	old := strings.Index(prompt, invocation.Content)
	firstUpdate, lastUpdate := strings.Index(prompt, m.LaterInputs[0].Content), strings.Index(prompt, m.LaterInputs[1].Content)
	if old < 0 || firstUpdate <= old || lastUpdate <= firstUpdate {
		t.Fatalf("latest user changes were missing or reordered: old=%d first=%d last=%d", old, firstUpdate, lastUpdate)
	}
	requireRecoveryMaterialText(t, prompt,
		"SAME authorized task", "materials can be incomplete", "Proactively inspect further task instructions",
		"Later user changes, cancellation and pause instructions take precedence", "pause only the dependent action",
		"Do not repeat completed writes", "outcome is unknown", "query/wait for that job rather than dispatching another one",
		"Continue remaining work", "delivery is not business completion", "\"status\":\"unknown\"",
		"do not acknowledge an old event as if you were its old native session", "Native export skipped an oversized line")
	if strings.Contains(prompt, agent.history.Source) {
		t.Fatal("raw native transcript locator was advertised as an inline material input")
	}
	requireRecoveryMaterialText(t, prompt, "native source locator is provenance only", "filtered export rather than opening raw session logs", "Do not read or copy encrypted reasoning, analysis channels or hidden summaries")
}

func TestRecoveryMaterial_PreservesTimerCronAndIPCInvocationIdentity(t *testing.T) {
	for _, kind := range []string{"timer", "cron", "ipc", "user"} {
		t.Run(kind, func(t *testing.T) {
			session, invocation, record, agent := recoveryMaterialFixture(t)
			origin := InvocationOrigin{Kind: kind, ID: "original-occurrence", JobID: "scheduled-job", ScheduledAt: record.StartedAt, TriggeredAt: record.UpdatedAt}
			invocation.Origin, record.Origin = origin, origin
			invocation.Content = "本次输入：完成 07:45 安排的整理，继承此前未完成部分。"
			m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
			prompt := recoveryPrompt(m)
			requireRecoveryMaterialText(t, prompt, `"kind":"`+kind+`"`, "original-occurrence", "scheduled-job", "2026-10-12T07:45:03+08:00", invocation.Content,
				"do not reschedule or refire the trigger or rerun its shell command", "model-turn completion and business-task completion are separate facts")
			if m.Record.Origin != origin || m.Invocation.Origin != origin {
				t.Fatal("material replaced the original trigger identity")
			}
		})
	}
}

func TestRecoveryMaterial_ExporterFailureRetainsPartialEvidenceWithoutRawError(t *testing.T) {
	session, invocation, record, agent := recoveryMaterialFixture(t)
	agent.err = errors.New("PRIVATE_RAW_LOG encrypted_content=PRIVATE_CIPHER analysis=PRIVATE_REASONING")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := buildRecoveryMaterial(ctx, session, agent, invocation, record)
	if !errors.Is(agent.ctxErr, context.Canceled) || len(m.HostHistory) != 2 || len(m.NativeHistory.Entries) != 1 {
		t.Fatalf("partial export or cancellation context lost: %#v", m)
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "PRIVATE_") || strings.Contains(recoveryPrompt(m), "PRIVATE_") {
		t.Fatal("raw adapter error leaked into recovery evidence")
	}
	requireRecoveryMaterialText(t, recoveryPrompt(m), "unavailable or incomplete", "Materials may be incomplete", "Historical attachments and tool outputs may be absent")
}

func TestRecoveryMaterial_AbsentExporterExplainsEvidenceBoundary(t *testing.T) {
	session, invocation, record, _ := recoveryMaterialFixture(t)
	m := buildRecoveryMaterial(context.Background(), session, &stubAgent{}, invocation, record)
	if len(m.HostHistory) != 2 || len(m.NativeHistory.Entries) != 0 {
		t.Fatalf("fallback history incorrect: %#v", m)
	}
	requireRecoveryMaterialText(t, recoveryPrompt(m), "no visible-history exporter", "only host evidence is available", "Do not invent missing state or permissions")
}

func TestRecoveryMaterial_SavedBundleKeepsFullMiddleAndLatestInput(t *testing.T) {
	session, invocation, record, agent := recoveryMaterialFixture(t)
	for i := range 45 {
		session.AddHistory("user", fmt.Sprintf("中间任务资料-%02d", i))
	}
	m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
	m.LaterInputs = []*recoveryInvocation{{MessageID: "new-update", Content: "开头目标" + strings.Repeat("待查材料", 5000) + "最新：暂停部署"}}
	if err := saveRecoveryMaterial(m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(record.ArtifactDir, "visible-material.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved contextRecoveryMaterial
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.HostHistory) != len(m.HostHistory) || saved.HostHistory[20].Content != m.HostHistory[20].Content || saved.LaterInputs[0].Content != m.LaterInputs[0].Content {
		t.Fatal("inline excerpt rules discarded full evidence from the saved bundle")
	}
	if saved.NativeHistory.Source != agent.history.Source || saved.NativeHistory.Entries[0].Content != agent.history.Entries[0].Content {
		t.Fatal("filtered evidence or its provenance was lost")
	}
	prompt := recoveryPrompt(m)
	if strings.Contains(prompt, "中间任务资料-20") {
		t.Fatal("fixture did not exercise inline middle omission")
	}
	requireRecoveryMaterialText(t, prompt, "read the bundle for the middle", "read visible-material.json", "最新：暂停部署")
}

func TestRecoveryMaterial_AttachmentMetadataPreservesIdentityWithoutBodies(t *testing.T) {
	session, invocation, record, agent := recoveryMaterialFixture(t)
	imageBody, fileBody := []byte("PRIVATE_IMAGE_BODY"), []byte("PRIVATE_FILE_BODY")
	invocation.Images = []ImageAttachment{{MimeType: "image/png", FileName: "原图.png", Data: imageBody}}
	invocation.Files = []FileAttachment{{MimeType: "application/pdf", FileName: "任务说明.pdf", Data: fileBody}}
	m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
	m.LaterInputs = []*recoveryInvocation{{MessageID: "attachment-update", Content: "以后续图为准", Images: []ImageAttachment{{MimeType: "image/jpeg", FileName: "更新图.jpg", Data: imageBody}}}}
	if err := saveRecoveryMaterial(m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(record.ArtifactDir, "visible-material.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Invocation struct {
			ImageCount int `json:"image_count"`
			FileCount  int `json:"file_count"`
		} `json:"failed_invocation"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Invocation.ImageCount != 1 || saved.Invocation.FileCount != 1 {
		t.Errorf("attachment counts missing: %#v", saved.Invocation)
	}
	for _, text := range []string{string(data), recoveryPrompt(m)} {
		requireRecoveryMaterialText(t, text, "原图.png", "任务说明.pdf", "更新图.jpg", "image/png", "application/pdf")
		for _, forbidden := range []string{"PRIVATE_IMAGE_BODY", "PRIVATE_FILE_BODY", base64.StdEncoding.EncodeToString(imageBody), base64.StdEncoding.EncodeToString(fileBody)} {
			if strings.Contains(text, forbidden) {
				t.Error("attachment body was copied into textual recovery evidence")
			}
		}
	}
}

func TestRecoveryMaterial_PrivatePermissionsAndNoTemporaryFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permissions are not available on Windows")
	}
	session, invocation, record, agent := recoveryMaterialFixture(t)
	m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
	if err := os.MkdirAll(record.ArtifactDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(record.ArtifactDir, "visible-material.json")
	if err := os.WriteFile(path, []byte("old content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := saveRecoveryMaterial(m); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		path string
		mode os.FileMode
	}{{record.ArtifactDir, 0o700}, {path, 0o600}} {
		info, err := os.Stat(item.path)
		if err != nil || info.Mode().Perm() != item.mode {
			t.Fatalf("private permissions missing: %s, info=%v err=%v", item.path, info, err)
		}
	}
	entries, err := os.ReadDir(record.ArtifactDir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "visible-material.json" {
		t.Fatalf("material save left temporary files: %#v %v", entries, err)
	}
}

func TestRecoveryMaterial_RejectsSymlinkedArtifactOrDedicatedRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink permissions differ on Windows")
	}
	for _, component := range []string{"artifact", "dedicated-root"} {
		t.Run(component, func(t *testing.T) {
			session, invocation, record, agent := recoveryMaterialFixture(t)
			outside := t.TempDir()
			if err := os.Chmod(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			link := record.ArtifactDir
			if component == "dedicated-root" {
				link = filepath.Dir(record.ArtifactDir)
			}
			if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, link); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
			m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
			if err := saveRecoveryMaterial(m); err == nil {
				t.Error("recovery followed a symlink instead of rejecting the target")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Errorf("recovery wrote outside its dedicated directory: %#v %v", entries, err)
			}
			info, err := os.Stat(outside)
			if err != nil || info.Mode().Perm() != 0o755 {
				t.Errorf("recovery changed permissions of the symlink target: %v %v", info, err)
			}
		})
	}
}

func TestRecoveryMaterial_AtomicWriteFailureDoesNotLeavePartialFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "visible-material.json")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := durableWriteFile(target, []byte("sensitive replacement"), 0o600); err == nil {
		t.Fatal("write should fail when the existing target is a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || !entries[0].IsDir() || entries[0].Name() != "visible-material.json" {
		t.Fatalf("failed atomic replacement left a partial/temp artifact: %#v %v", entries, err)
	}
}

func TestRecoveryMaterial_ExcerptIsUTF8AndLimitIncludesGapMarker(t *testing.T) {
	input := "原目标：修复完成" + strings.Repeat("中文字😀", 500) + "最新要求：取消部署"
	got := recoveryExcerpt(input, 512)
	if !utf8.ValidString(got) || len(got) > 512 {
		t.Errorf("excerpt exceeds its complete byte budget or splits UTF-8: length=%d valid=%v", len(got), utf8.ValidString(got))
	}
	if !strings.HasPrefix(got, "原目标：修复完成") || !strings.HasSuffix(got, "最新要求：取消部署") {
		t.Error("excerpt lost original goal or final user change")
	}
	requireRecoveryMaterialText(t, got, "gap", "read visible-material.json")
	if unchanged := recoveryExcerpt("完整短输入", 512); unchanged != "完整短输入" {
		t.Fatal("short input should remain exact")
	}
	for _, limit := range []int{0, 1, 5, 32} {
		excerpt := recoveryExcerpt(input, limit)
		if !utf8.ValidString(excerpt) || len(excerpt) > limit {
			t.Errorf("small excerpt limit %d not respected: bytes=%d valid=%v", limit, len(excerpt), utf8.ValidString(excerpt))
		}
	}
}

func TestRecoveryMaterial_InlineBudgetKeepsFailedInputAndNewestCancellation(t *testing.T) {
	session, invocation, record, agent := recoveryMaterialFixture(t)
	invocation.Content = "ORIGINAL-REQUEST " + strings.Repeat("旧任务资料😀", 12000) + " ORIGINAL-END"
	for range 80 {
		session.AddHistory("assistant", strings.Repeat("历史结果材料", 2000))
	}
	m := buildRecoveryMaterial(context.Background(), session, agent, invocation, record)
	for i := range 128 {
		m.SteeredInputs = append(m.SteeredInputs, &recoverySteeredInput{
			Input: IPCInput{EventID: fmt.Sprintf("event-%d", i), Prompt: strings.Repeat("IPC 作业材料", 1200)}, Status: "unknown",
		})
	}
	for i := range 5 {
		m.LaterInputs = append(m.LaterInputs, &recoveryInvocation{MessageID: fmt.Sprintf("later-%d", i),
			Content: fmt.Sprintf("LATEST-%d ", i) + strings.Repeat("补充资料😀", 7000) + fmt.Sprintf(" UPDATE-END-%d", i)})
	}
	m.LaterInputs[4].Content += " LAST-CANCEL-DO-NOT-DEPLOY"
	for range 80 {
		m.Warnings = append(m.Warnings, strings.Repeat("Missing evidence. ", 100))
	}
	prompt := recoveryPrompt(m)
	if len(prompt) > 96<<10 || !utf8.ValidString(prompt) {
		t.Fatalf("inline prompt exceeded complete byte budget or broke UTF-8: %d bytes", len(prompt))
	}
	requireRecoveryMaterialText(t, prompt, "ORIGINAL-REQUEST", "ORIGINAL-END", "LATEST-4", "LAST-CANCEL-DO-NOT-DEPLOY", "read visible-material.json")
	if strings.Index(prompt, "LAST-CANCEL-DO-NOT-DEPLOY") < strings.Index(prompt, "IPC attempts during") {
		t.Fatal("budget handling cut or moved the latest cancellation behind older work")
	}
	if err := saveRecoveryMaterial(m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(record.ArtifactDir, "visible-material.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved contextRecoveryMaterial
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.SteeredInputs) != 128 || len(saved.HostHistory) != len(m.HostHistory) || saved.Invocation.Content != invocation.Content || saved.LaterInputs[4].Content != m.LaterInputs[4].Content {
		t.Fatal("bounded inline rendering discarded available full evidence from the bundle")
	}
}
