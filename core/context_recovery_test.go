package core

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recoveryTestFailure struct{}

func (recoveryTestFailure) Error() string          { return "rejected encrypted context" }
func (recoveryTestFailure) RecoveryReason() string { return RecoveryInvalidEncryptedContent }

type recoveryTestSession struct {
	stubAgentSession
	mu         sync.Mutex
	id         string
	events     chan Event
	prompts    []string
	messageIDs []string
	closed     bool
	mode       string
	confirmed  bool
	closeErr   error
	onSend     func(int, string) error
	onFresh    func(context.Context) (AgentSession, error)
	freshCalls atomic.Int32
	steerErr   error
}

func newRecoveryTestSession(id string) *recoveryTestSession {
	s := &recoveryTestSession{id: id, mode: "suggest", events: make(chan Event, 32)}
	s.onSend = func(_ int, _ string) error {
		s.events <- Event{Type: EventResult, Content: "task continued", Done: true}
		return nil
	}
	return s
}
func (s *recoveryTestSession) CurrentSessionID() string { return s.id }
func (s *recoveryTestSession) GetMode() string          { s.mu.Lock(); defer s.mu.Unlock(); return s.mode }
func (s *recoveryTestSession) SetLiveMode(mode string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
	return true
}
func (s *recoveryTestSession) Events() <-chan Event { return s.events }
func (s *recoveryTestSession) Alive() bool          { s.mu.Lock(); defer s.mu.Unlock(); return !s.closed }
func (s *recoveryTestSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
func (s *recoveryTestSession) CloseForRecovery(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeErr != nil {
		return s.closeErr
	}
	s.closed, s.confirmed = true, true
	return nil
}
func (s *recoveryTestSession) StartFreshForRecovery(ctx context.Context) (AgentSession, error) {
	s.freshCalls.Add(1)
	s.mu.Lock()
	confirmed := s.confirmed
	s.mu.Unlock()
	if !confirmed {
		return nil, errors.New("old executor still live")
	}
	return s.onFresh(ctx)
}
func (s *recoveryTestSession) Send(prompt, id string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("closed executor")
	}
	s.prompts = append(s.prompts, prompt)
	s.messageIDs = append(s.messageIDs, id)
	n := len(s.prompts)
	fn := s.onSend
	s.mu.Unlock()
	return fn(n, prompt)
}
func (s *recoveryTestSession) SteerInput(expected, prompt string) (string, error) {
	if expected != s.id {
		return "", errors.New("wrong identity")
	}
	return "original-turn", s.steerErr
}
func (s *recoveryTestSession) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.prompts...)
}

type recoveryStartupFailure struct {
	recoveryTestFailure
	starter *recoveryTestSession
}

func (s *recoveryStartupFailure) StartFreshForRecovery(ctx context.Context) (AgentSession, error) {
	return s.starter.StartFreshForRecovery(ctx)
}

type recoveryTestAgent struct {
	stubAgent
	dir          string
	session      *recoveryTestSession
	startErr     error
	starts       atomic.Int32
	historyReads atomic.Int32
	listed       []AgentSessionInfo
}

func (a *recoveryTestAgent) GetWorkDir() string      { return a.dir }
func (a *recoveryTestAgent) SupportsLiveInput() bool { return true }
func (a *recoveryTestAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return a.listed, nil
}
func (a *recoveryTestAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.starts.Add(1)
	return a.session, a.startErr
}
func (a *recoveryTestAgent) GetRecoveryHistory(context.Context, string) (RecoveryHistory, error) {
	a.historyReads.Add(1)
	return RecoveryHistory{Entries: []RecoveryHistoryEntry{{Role: "assistant", Kind: "tool_result", CallID: "write-1", Content: "artifact already written; verify before repeating"}}}, nil
}

type recoveryFixture struct {
	e          *Engine
	p          *ipcTestPlatform
	a          *recoveryTestAgent
	old, fresh *recoveryTestSession
	session    *Session
	key        string
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old, fresh := newRecoveryTestSession("native-old"), newRecoveryTestSession("native-new")
	old.onFresh = func(context.Context) (AgentSession, error) { return fresh, nil }
	a := &recoveryTestAgent{dir: dir, session: old}
	p := &ipcTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("recovery", a, []Platform{p}, filepath.Join(dir, "sessions.json"), LangEnglish)
	e.SetStreamPreviewCfg(StreamPreviewCfg{Enabled: false})
	f := &recoveryFixture{e: e, p: p, a: a, old: old, fresh: fresh, key: "test:recovery"}
	f.session = e.sessions.GetOrCreateActive(f.key)
	f.session.SetAgentSessionID(old.id, a.Name())
	t.Cleanup(func() {
		e.cancel()
		e.interactiveMu.Lock()
		var keys []string
		for key := range e.interactiveStates {
			keys = append(keys, key)
		}
		e.interactiveMu.Unlock()
		for _, key := range keys {
			e.cleanupInteractiveState(key)
		}
	})
	return f
}
func (f *recoveryFixture) message(id, content string) *Message {
	return &Message{SessionKey: f.key, Platform: "test", UserID: "owner", UserName: "owner", MessageID: id, Content: content, ReplyCtx: f.key}
}
func (f *recoveryFixture) run(id, content string) error {
	gen, ok := f.session.TryLock()
	if !ok {
		return errors.New("test session busy")
	}
	return f.e.processInteractiveMessage(f.p, f.message(id, content), f.session, gen)
}
func recoveryAwait(t *testing.T, reason string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", reason)
}
func readRecoveryBundle(t *testing.T, s *Session) contextRecoveryMaterial {
	t.Helper()
	r := s.GetContextRecovery()
	if r == nil {
		t.Fatal("missing recovery ledger")
	}
	b, err := os.ReadFile(filepath.Join(r.ArtifactDir, "visible-material.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m contextRecoveryMaterial
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestContextRecovery_StartupSendAndTerminalFailures(t *testing.T) {
	for _, source := range []string{"startup", "send", "terminal"} {
		t.Run(source, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.session.AddHistory("user", "Finish the report; do not deploy; use existing job receipt job-7.")
			switch source {
			case "startup":
				f.old.confirmed = true
				f.a.startErr = &recoveryStartupFailure{starter: f.old}
				f.a.session = nil
			case "send":
				f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
			case "terminal":
				f.old.onSend = func(int, string) error {
					f.old.events <- Event{Type: EventError, Error: recoveryTestFailure{}}
					return nil
				}
			}
			if err := f.run("continue-2", "continue the same task"); err != nil {
				t.Fatal(err)
			}
			if f.a.starts.Load() != 1 || f.old.freshCalls.Load() != 1 {
				t.Fatalf("starts=%d fresh=%d", f.a.starts.Load(), f.old.freshCalls.Load())
			}
			r := f.session.GetContextRecovery()
			if r.FromSessionID != f.old.id || r.ToSessionID != f.fresh.id || r.Phase != "model_turn_completed" || r.InputStatus != "accepted" || r.TurnStatus != "completed" {
				t.Fatalf("ledger=%+v", r)
			}
			if !strings.Contains(f.fresh.sent()[0], "do not deploy") || !strings.Contains(f.fresh.sent()[0], "continue the same task") {
				t.Fatal("lost original scope or failed input")
			}
			if f.a.historyReads.Load() != 1 {
				t.Fatal("history must be read once on fault")
			}
			m := readRecoveryBundle(t, f.session)
			if m.Invocation.MessageID != "continue-2" || m.HostSessionID != f.session.ID {
				t.Fatalf("wrong material source: %+v", m)
			}
			loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key)
			if loaded.GetContextRecovery().ID != r.ID || loaded.GetAgentSessionID() != f.fresh.id {
				t.Fatal("recovery binding not durable")
			}
		})
	}
}

func TestContextRecovery_OrdinaryErrorsDoNotOptIn(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return errors.New("the document mentions invalid_encrypted_content") }
	if err := f.run("plain-error", "a task"); err == nil {
		t.Fatal("expected send failure")
	}
	if f.old.freshCalls.Load() != 0 || f.a.historyReads.Load() != 0 || f.session.GetContextRecovery() != nil {
		t.Fatal("ordinary text opted into recovery")
	}
}

func TestContextRecovery_FailedQueuedInputIsCurrentInvocation(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(n int, _ string) error {
		if n == 2 {
			return recoveryTestFailure{}
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- f.run("first", "original task") }()
	recoveryAwait(t, "first input", func() bool { return len(f.old.sent()) == 1 })
	if !f.e.queueMessageForBusySession(f.p, f.message("queued", "changed task scope"), f.key) {
		t.Fatal("queue rejected")
	}
	f.old.events <- Event{Type: EventResult, Content: "first step finished", Done: true}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m := readRecoveryBundle(t, f.session)
	if m.Invocation.MessageID != "queued" || m.Invocation.Content != "changed task scope" {
		t.Fatalf("recovered wrong input: %+v", m.Invocation)
	}
	if strings.Count(strings.Join(f.fresh.sent(), "\n"), "LATEST user updates") != 0 {
		t.Fatal("failed queued input incorrectly duplicated as later input")
	}
}

func TestContextRecovery_LatestUpdateDuringStartupIsIncludedOnce(t *testing.T) {
	f := newRecoveryFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.old.onFresh = func(ctx context.Context) (AgentSession, error) {
		close(started)
		select {
		case <-release:
			return f.fresh, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "prepare and publish report") }()
	<-started
	if !f.e.queueMessageForBusySession(f.p, f.message("cancel-publish", "cancel publication; only prepare local draft"), f.key) {
		t.Fatal("update rejected during recovery")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m := readRecoveryBundle(t, f.session)
	if len(m.LaterInputs) != 1 || m.LaterInputs[0].MessageID != "cancel-publish" {
		t.Fatalf("latest update lost: %+v", m.LaterInputs)
	}
	if len(f.fresh.sent()) != 1 || !strings.Contains(f.fresh.sent()[0], "cancel publication") {
		t.Fatal("latest update missing or sent twice")
	}
}

func TestContextRecovery_StopNewAndSwitchCannotResurrect(t *testing.T) {
	for _, action := range []string{"stop", "new", "switch"} {
		t.Run(action, func(t *testing.T) {
			f := newRecoveryFixture(t)
			other := f.e.sessions.NewSideSession(f.key, "other")
			other.SetAgentSessionID("native-other", f.a.Name())
			f.a.listed = []AgentSessionInfo{{ID: "native-other", Summary: "other"}}
			started, release := make(chan struct{}), make(chan struct{})
			f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
			// Simulate a late startup result even after cancellation. The host
			// must discard and close it rather than binding it to a newer turn.
			f.old.onFresh = func(context.Context) (AgentSession, error) { close(started); <-release; return f.fresh, nil }
			done := make(chan error, 1)
			go func() { done <- f.run("original", "continue task") }()
			<-started
			switch action {
			case "stop":
				f.e.stopInteractiveSessionWithOptions(f.key, false)
			case "new":
				f.e.cmdNew(f.p, f.message("new", "/new"), nil)
			case "switch":
				f.e.cmdSwitch(f.p, f.message("switch", "/switch"), []string{"native-other"})
			}
			close(release)
			if err := <-done; err == nil {
				t.Fatal("cancelled recovery returned success")
			}
			if len(f.fresh.sent()) != 0 || f.fresh.Alive() {
				t.Fatal("late fresh executor was left running or received input")
			}
			if f.e.sessions.GetOrCreateActive(f.key).GetAgentSessionID() == f.fresh.id {
				t.Fatal("overwrote user selection")
			}
		})
	}
}

func TestContextRecovery_DurableReservationAndConfirmedCloseRequired(t *testing.T) {
	for _, failure := range []string{"close", "persistence"} {
		t.Run(failure, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
			if failure == "close" {
				f.old.closeErr = errors.New("kill unconfirmed")
			} else {
				f.e.sessions.storePath = t.TempDir()
			}
			if err := f.run("failing", "continue"); err == nil {
				t.Fatal("unsafe recovery succeeded")
			}
			if f.old.freshCalls.Load() != 0 {
				t.Fatal("fresh context created before durable reservation/exit confirmation")
			}
		})
	}
}

func TestContextRecovery_BudgetSurvivesRestartAndNativeRebinding(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	if err := f.run("first", "first task"); err != nil {
		t.Fatal(err)
	}
	original := f.session.GetContextRecovery()
	f.e.cleanupInteractiveState(f.key)
	newSession := newRecoveryTestSession(f.fresh.id)
	newSession.onSend = func(int, string) error { return recoveryTestFailure{} }
	a := &recoveryTestAgent{dir: f.a.dir, session: newSession}
	e := NewEngine("recovery", a, []Platform{f.p}, f.e.sessions.StorePath(), LangEnglish)
	t.Cleanup(e.cancel)
	s := e.sessions.GetOrCreateActive(f.key)
	gen, _ := s.TryLock()
	if err := e.processInteractiveMessage(f.p, f.message("later", "continue"), s, gen); err == nil || !strings.Contains(err.Error(), "budget_exhausted") {
		t.Fatalf("expected spent budget: %v", err)
	}
	if newSession.freshCalls.Load() != 0 || s.GetContextRecovery().ID != original.ID {
		t.Fatal("restart/native ID reset budget")
	}
	e.cleanupInteractiveState(f.key)
}

func TestContextRecovery_InterruptedPreparationDoesNotReplayOnRestart(t *testing.T) {
	for _, phase := range []string{"reserved", "starting", "ready", "sending", "input_accepted"} {
		t.Run(phase, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.session.ContextRecovery = &ContextRecoveryRecord{ID: "interrupted", Phase: phase, FromSessionID: f.old.id, InputStatus: "unknown"}
			if err := f.e.sessions.SaveWithError(); err != nil {
				t.Fatal(err)
			}
			f.e.ensureInteractiveStateForQueueing(f.key, f.p, f.key)
			if err := f.run("restart", "continue"); err == nil {
				t.Fatal("interrupted preparation replayed")
			}
			if f.a.starts.Load() != 0 || len(f.old.sent()) != 0 {
				t.Fatal("started executor for uncertain prior attempt")
			}
		})
	}
}

func TestContextRecovery_RepeatedFailureStopsAfterOneAttempt(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.fresh.onSend = func(int, string) error { return recoveryTestFailure{} }
	if err := f.run("input", "continue"); err == nil || !strings.Contains(err.Error(), "budget_exhausted") {
		t.Fatalf("expected spent budget, got %v", err)
	}
	if f.old.freshCalls.Load() != 1 || f.fresh.freshCalls.Load() != 0 {
		t.Fatal("more than one automatic recovery")
	}
	if r := f.session.GetContextRecovery(); r.Phase != "failed" || r.TurnStatus != "failed" {
		t.Fatalf("failed recovery still appears active: %+v", r)
	}
}

func TestContextRecovery_TimerAndCronKeepOriginalOccurrence(t *testing.T) {
	for _, kind := range []string{"timer", "cron"} {
		for _, mode := range []string{"reuse", "new_per_run"} {
			for _, failFresh := range []bool{false, true} {
				t.Run(kind+"/"+mode+"/"+map[bool]string{false: "success", true: "failure"}[failFresh], func(t *testing.T) {
					f := newRecoveryFixture(t)
					f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
					if failFresh {
						f.fresh.onSend = func(int, string) error {
							f.fresh.events <- Event{Type: EventError, Error: errors.New("local fake task failure")}
							return nil
						}
					}
					scheduled := time.Now().Add(-time.Minute).UTC()
					var err error
					if kind == "timer" {
						err = f.e.ExecuteTimerJob(&TimerJob{ID: "job-745", Project: "recovery", SessionKey: f.key, ScheduledAt: scheduled, Prompt: "finish the original scheduled report", SessionMode: mode})
					} else {
						err = f.e.ExecuteCronJob(&CronJob{ID: "job-745", Project: "recovery", SessionKey: f.key, Prompt: "finish the original scheduled report", SessionMode: mode})
					}
					if (err != nil) != failFresh {
						t.Fatalf("trigger outcome=%v, failFresh=%v", err, failFresh)
					}
					if f.old.freshCalls.Load() != 1 || len(f.old.sent()) != 1 || len(f.fresh.sent()) != 1 {
						t.Fatal("trigger was restarted or prompt repeated")
					}
					var record *ContextRecoveryRecord
					f.e.sessions.mu.RLock()
					for _, s := range f.e.sessions.sessions {
						if r := s.GetContextRecovery(); r != nil {
							record = r
						}
					}
					f.e.sessions.mu.RUnlock()
					if record == nil || record.Origin.Kind != kind || record.Origin.JobID != "job-745" || record.Origin.ID == "" {
						t.Fatalf("missing original occurrence: %+v", record)
					}
					if kind == "timer" && !record.Origin.ScheduledAt.Equal(scheduled) {
						t.Fatal("timer schedule changed during recovery")
					}
				})
			}
		}
	}
}

func TestContextRecovery_IPCOriginalReceiptAndIdentityRemainPinned(t *testing.T) {
	for _, source := range []string{"idle", "busy-unknown"} {
		t.Run(source, func(t *testing.T) {
			f := newRecoveryFixture(t)
			api := &APIServer{engines: map[string]*Engine{"recovery": f.e}}
			if err := api.initSessionIPC(filepath.Join(f.a.dir, "ipc")); err != nil {
				t.Fatal(err)
			}
			input := IPCInput{EventID: strings.Repeat("c", 64), Prompt: "original job callback; inspect its receipt before any new work", Target: IPCTarget{Project: "recovery", SessionKey: f.key, SessionID: f.session.ID, AgentSessionID: f.old.id, CWD: f.a.dir}}
			var done chan error
			if source == "busy-unknown" {
				f.old.onSend = func(int, string) error { return nil }
				f.old.steerErr = errors.New("lost local RPC reply")
				done = make(chan error, 1)
				go func() { done <- f.run("working", "finish current job") }()
				recoveryAwait(t, "busy input", func() bool { return len(f.old.sent()) == 1 })
			} else {
				f.old.onSend = func(int, string) error {
					f.old.events <- Event{Type: EventError, Error: recoveryTestFailure{}}
					return nil
				}
			}
			if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			if done != nil {
				f.old.events <- Event{Type: EventError, Error: recoveryTestFailure{}}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			recoveryAwait(t, "IPC recovery receipt", func() bool {
				r := ipcReceipt(t, api, input.EventID)
				return r.Recovery != nil && r.Recovery.Phase == "model_turn_completed" && !f.session.Busy()
			})
			r := ipcReceipt(t, api, input.EventID)
			wantStatus := "delivered"
			if source == "busy-unknown" {
				wantStatus = "unknown"
			}
			if r.Input != input || r.Status != wantStatus || r.AcknowledgedAt != "" {
				t.Fatalf("original receipt rewritten: %+v", r)
			}
			m := readRecoveryBundle(t, f.session)
			if source == "busy-unknown" && (len(m.SteeredInputs) != 1 || m.SteeredInputs[0].Status != "unknown") {
				t.Fatal("uncertain steer evidence lost")
			}
			ack := map[string]any{"event_id": input.EventID, "target": input.Target}
			if w := ipcPost(t, api.handleIPCAck, ack); w.Code != 409 {
				t.Fatal("old identity ACK accepted")
			}
			input.EventID = strings.Repeat("d", 64)
			if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			if next := ipcReceipt(t, api, input.EventID); next.Status != "blocked" || next.Code != "ipc_target_changed" {
				t.Fatalf("future old target followed new context: %+v", next)
			}
			if len(f.fresh.sent()) != 1 {
				t.Fatal("old-target future event was retargeted")
			}
		})
	}
}

func TestContextRecovery_TimerFailureIsFiredWithErrorAndNotRescheduled(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.fresh.onSend = func(int, string) error { return errors.New("model turn failed locally") }
	dir := filepath.Join(f.a.dir, "timers")
	store, err := NewTimerStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	job := &TimerJob{ID: "once-only", Project: "recovery", SessionKey: f.key, ScheduledAt: time.Now().Add(-time.Second), Prompt: "finish scheduled work"}
	if err := store.Add(job); err != nil {
		t.Fatal(err)
	}
	scheduler := NewTimerScheduler(store)
	scheduler.RegisterEngine("recovery", f.e)
	scheduler.executeJob(job.ID)
	saved := store.Get(job.ID)
	if saved == nil || !saved.Fired || saved.LastError == "" {
		t.Fatalf("failed turn reported as success: %+v", saved)
	}
	reloaded, err := NewTimerStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart := NewTimerScheduler(reloaded)
	afterRestart.RegisterEngine("recovery", f.e)
	afterRestart.executeJob(job.ID)
	if len(f.old.sent()) != 1 || len(f.fresh.sent()) != 1 {
		t.Fatal("fired timer was automatically replayed")
	}
}

func TestContextRecovery_PreparationUsesRemainingTurnDeadline(t *testing.T) {
	f := newRecoveryFixture(t)
	f.e.maxTurnTime = 150 * time.Millisecond
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.old.onFresh = func(ctx context.Context) (AgentSession, error) { <-ctx.Done(); return nil, ctx.Err() }
	started := time.Now()
	if err := f.run("bounded", "continue"); err == nil {
		t.Fatal("unbounded recovery returned success")
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("recovery reset original turn time budget")
	}
	if len(f.fresh.sent()) != 0 {
		t.Fatal("timed-out preparation sent input")
	}
}

func TestContextRecovery_PSSupplementIsRetainedBeforeFailure(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return nil }
	done := make(chan error, 1)
	go func() { done <- f.run("task", "prepare and publish report") }()
	recoveryAwait(t, "original input", func() bool { return len(f.old.sent()) == 1 })
	f.e.cmdPs(f.p, f.message("ps", "/ps"), []string{"do not deploy; only prepare locally"})
	f.old.events <- Event{Type: EventError, Error: recoveryTestFailure{}}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.fresh.sent()[0], "do not deploy; only prepare locally") {
		t.Fatal("lost PS scope restriction")
	}
	m := readRecoveryBundle(t, f.session)
	found := false
	for _, h := range m.HostHistory {
		if h.Role == "user" && h.Content == "do not deploy; only prepare locally" {
			found = true
		}
	}
	if !found {
		t.Fatal("PS accepted input not retained in host evidence")
	}
}

func TestContextRecovery_PSDuringPreparationQueuesForFreshContext(t *testing.T) {
	f := newRecoveryFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.old.onFresh = func(context.Context) (AgentSession, error) { close(started); <-release; return f.fresh, nil }
	done := make(chan error, 1)
	go func() { done <- f.run("original", "publish report") }()
	<-started
	f.e.cmdPs(f.p, f.message("ps", "/ps"), []string{"cancel publishing; keep local"})
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(f.old.sent()) != 1 || len(f.fresh.sent()) != 1 || !strings.Contains(f.fresh.sent()[0], "cancel publishing; keep local") {
		t.Fatal("PS was lost or sent separately before recovery input")
	}
}
