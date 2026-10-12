package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestContextRecovery_StartupRecoveryDrainsLateQueuedInput(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.confirmed = true
	f.a.startErr = &recoveryStartupFailure{starter: f.old}
	f.a.session = nil
	release := make(chan struct{})
	f.fresh.onSend = func(n int, _ string) error {
		f.fresh.events <- Event{Type: EventResult, Content: "task continued", Done: true}
		if n == 1 {
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue prior task") }()
	recoveryAwait(t, "recovery completed event", func() bool {
		r := f.session.GetContextRecovery()
		return r != nil && r.Phase == "model_turn_completed"
	})
	// Keep Send blocked after EventResult, then deliver a correction once
	// the event loop has had time to observe an empty queue.
	time.Sleep(40 * time.Millisecond)
	queued := f.e.queueMessageForBusySession(f.p, f.message("late", "also include this correction"), f.key)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !queued {
		t.Fatal("late input was rejected")
	}
	if got := len(f.fresh.sent()); got != 2 {
		t.Fatalf("late accepted input was orphaned: fresh sends = %d, want 2", got)
	}
}

func TestContextRecovery_UnconfirmedCloseBlocksNextInput(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(n int, _ string) error {
		if n == 1 {
			return recoveryTestFailure{}
		}
		f.old.events <- Event{Type: EventResult, Content: "continued unsafely", Done: true}
		return nil
	}
	f.old.closeErr = errors.New("old executor shutdown not confirmed")
	if err := f.run("first", "continue"); err == nil || err.Error() != "old_executor_unconfirmed" {
		t.Fatalf("setup did not reach failed close: %v", err)
	}
	if err := f.run("next", "continue same task"); err == nil || !strings.Contains(err.Error(), "review_required") {
		t.Fatalf("unconfirmed executor was reused for next input: err = %v, sends = %d", err, len(f.old.sent()))
	}
	if got := len(f.old.sent()); got != 1 {
		t.Fatalf("unconfirmed executor received %d inputs, want 1", got)
	}
}

func TestContextRecovery_UnconfirmedStartupBlocksNextInputAndRestart(t *testing.T) {
	for _, followup := range []string{"next_input", "restart"} {
		t.Run(followup, func(t *testing.T) {
			f := newRecoveryFixture(t)
			// Match an adapter that identified the context rejection but
			// could not confirm startup cleanup: typed error, no starter.
			f.a.startErr = recoveryTestFailure{}
			f.a.session = nil
			if err := f.run("first", "continue original task"); err == nil {
				t.Fatal("unconfirmed startup unexpectedly succeeded")
			}
			if f.a.starts.Load() != 1 || f.old.freshCalls.Load() != 0 {
				t.Fatalf("unexpected initial startup attempts: starts=%d fresh=%d", f.a.starts.Load(), f.old.freshCalls.Load())
			}

			if followup == "next_input" {
				err := f.run("next", "continue the same task")
				if got := f.a.starts.Load(); got != 1 {
					t.Errorf("next input retried an unconfirmed executor: StartSession calls=%d, want 1", got)
				}
				if err == nil {
					t.Error("next input did not require recovery review")
				}
			} else {
				f.e.cancel()
				f.e.cleanupInteractiveState(f.key)
				afterRestart := &recoveryTestAgent{dir: f.a.dir, startErr: recoveryTestFailure{}}
				restarted := NewEngine("recovery", afterRestart, []Platform{f.p}, f.e.sessions.StorePath(), LangEnglish)
				t.Cleanup(func() {
					restarted.cancel()
					restarted.cleanupInteractiveState(f.key)
				})
				session := restarted.sessions.GetOrCreateActive(f.key)
				gen, locked := session.TryLock()
				if !locked {
					t.Fatal("restarted session unexpectedly busy")
				}
				err := restarted.processInteractiveMessage(f.p, f.message("next", "continue the same task"), session, gen)
				if got := afterRestart.starts.Load(); got != 0 {
					t.Errorf("restart forgot unconfirmed cleanup: StartSession calls=%d, want 0", got)
				}
				if err == nil {
					t.Error("restarted input did not require recovery review")
				}
			}

			loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key)
			if record := loaded.GetContextRecovery(); record == nil || !recoveryNeedsReview(record) {
				t.Errorf("unconfirmed startup has no durable review fence: %+v", record)
			}
			if loaded.GetAgentSessionID() != f.old.id {
				t.Fatalf("unconfirmed startup changed the original native target: %q", loaded.GetAgentSessionID())
			}
		})
	}
}

func TestContextRecovery_DeadExecutorDoesNotBypassReview(t *testing.T) {
	f := newRecoveryFixture(t)
	f.fresh.closed = true
	f.a.session = newRecoveryTestSession(f.fresh.id)
	f.session.SetAgentSessionID(f.fresh.id, f.a.Name())
	f.session.ContextRecovery = &ContextRecoveryRecord{ID: "reserved", FromSessionID: f.old.id, ToSessionID: f.fresh.id,
		Phase: "failed", ErrorCode: "recovery_persistence_failed", InputStatus: "unknown", TurnStatus: "failed"}
	f.e.interactiveStates[f.key] = &interactiveState{agentSession: f.fresh, busySession: f.session, agent: f.a,
		platform: f.p, replyCtx: f.key, eventsNeedResync: true, logicalSessionKey: f.key}
	if err := f.run("next", "continue same task"); err == nil || !strings.Contains(err.Error(), "review_required") {
		t.Fatalf("dead state bypassed review: err = %v, StartSession calls = %d", err, f.a.starts.Load())
	}
	if f.a.starts.Load() != 0 {
		t.Fatal("review-required record must stop normal startup")
	}
}

func TestContextRecovery_AncestorSelectionKeepsBudgetAndExactNative(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	if err := f.run("first", "continue original task"); err != nil {
		t.Fatal(err)
	}
	original := f.session.GetContextRecovery()
	f.e.cleanupInteractiveState(f.key)
	selected := f.e.sessions.SwitchToAgentSession(f.key, f.old.id, f.a.Name(), "original native context")
	if selected.GetAgentSessionID() != f.old.id {
		t.Fatalf("ancestor selection silently followed new native: %q", selected.GetAgentSessionID())
	}
	if r := selected.GetContextRecovery(); r == nil || r.ID != original.ID {
		t.Fatalf("selecting an ancestor reset the recovery budget: %+v", r)
	}
	loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key)
	if r := loaded.GetContextRecovery(); r == nil || r.ID != original.ID || loaded.GetAgentSessionID() != f.old.id {
		t.Fatalf("ancestor identity/budget not durable: native=%q recovery=%+v", loaded.GetAgentSessionID(), r)
	}
	newTask := f.e.sessions.NewSession(f.key, "new task")
	if newTask.GetContextRecovery() != nil || newTask.GetAgentSessionID() != "" {
		t.Fatal("explicit new task inherited the old task's recovery budget or native binding")
	}
	otherUser := f.e.sessions.SwitchToAgentSession("test:another-owner", f.old.id, f.a.Name(), "separate owner")
	if otherUser.GetContextRecovery() != nil {
		t.Fatal("recovery ledger crossed the owning session key")
	}
}

func TestContextRecovery_CompletedResultWinsOverLateSendError(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	release := make(chan struct{})
	f.fresh.onSend = func(int, string) error {
		f.fresh.events <- Event{Type: EventResult, Content: "task continued", Done: true}
		<-release
		return errors.New("local reply acknowledgement lost after completed result")
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue original task") }()
	recoveryAwait(t, "completed result before send receipt", func() bool {
		r := f.session.GetContextRecovery()
		return r != nil && r.Phase == "model_turn_completed"
	})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("a confirmed completed turn became a failure: %v", err)
	}
	f.e.interactiveMu.Lock()
	state := f.e.interactiveStates[f.key]
	f.e.interactiveMu.Unlock()
	if state != nil {
		state.mu.Lock()
		pending := state.recoveryInputPending
		state.mu.Unlock()
		if pending {
			t.Fatal("completed recovery left future live input behind a permanent submission fence")
		}
	}
	r := f.session.GetContextRecovery()
	if r.Phase != "model_turn_completed" || r.TurnStatus != "completed" || r.InputStatus != "accepted" || r.ErrorCode != "" {
		t.Fatalf("late Send error rewrote completed evidence: %+v", r)
	}
	loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key)
	if saved := loaded.GetContextRecovery(); saved == nil || saved.ID != r.ID || saved.Phase != r.Phase || saved.InputStatus != r.InputStatus {
		t.Fatalf("completed receipt was not preserved on disk: %+v", saved)
	}
}

type recoveryReviewFailingPlatform struct {
	*ipcTestPlatform
	rejected atomic.Int32
}

func (p *recoveryReviewFailingPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	if strings.Contains(content, "task continued") {
		p.rejected.Add(1)
		return errors.New("local platform final reply rejected")
	}
	return p.ipcTestPlatform.Send(ctx, replyCtx, content)
}

func TestContextRecovery_PlatformFailurePreservesModelCompletion(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	p := &recoveryReviewFailingPlatform{ipcTestPlatform: f.p}
	gen, _ := f.session.TryLock()
	err := f.e.processInteractiveMessage(p, f.message("original", "continue original task"), f.session, gen)
	if err == nil || p.rejected.Load() == 0 {
		t.Fatalf("platform delivery failure was not reported: err=%v rejected=%d", err, p.rejected.Load())
	}
	// The model input receipt can complete just after the platform reports
	// its failure; wait for that local write before checking durable state.
	recoveryAwait(t, "recovery input receipt", func() bool {
		f.e.interactiveMu.Lock()
		state := f.e.interactiveStates[f.key]
		f.e.interactiveMu.Unlock()
		if state == nil {
			return true
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		return !state.recoveryInputPending
	})
	r := f.session.GetContextRecovery()
	if r == nil || r.Phase != "model_turn_completed" || r.TurnStatus != "completed" || r.InputStatus != "accepted" {
		t.Fatalf("platform delivery failure rewrote model completion: %+v", r)
	}
	loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key)
	if saved := loaded.GetContextRecovery(); saved == nil || saved.ID != r.ID || saved.TurnStatus != "completed" {
		t.Fatalf("model completion was not durable: %+v", saved)
	}
}

type recoveryReviewModeSession struct {
	*recoveryTestSession
	modeMu sync.Mutex
	mode   string
}

func (s *recoveryReviewModeSession) SetLiveMode(mode string) bool {
	s.modeMu.Lock()
	defer s.modeMu.Unlock()
	s.mode = mode
	return true
}

func (s *recoveryReviewModeSession) GetMode() string {
	s.modeMu.Lock()
	defer s.modeMu.Unlock()
	return s.mode
}

type recoveryReviewModeAgent struct {
	*recoveryTestAgent
	live         AgentSession
	initialError error
	baseMode     string
}

func (a *recoveryReviewModeAgent) GetMode() string { return a.baseMode }

func (a *recoveryReviewModeAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.starts.Add(1)
	return a.live, a.initialError
}

func TestContextRecovery_TemporaryModeRestoredOnReplacement(t *testing.T) {
	for _, source := range []string{"startup", "send"} {
		for _, outcome := range []string{"success", "failure"} {
			t.Run(source+"/"+outcome, func(t *testing.T) {
				f := newRecoveryFixture(t)
				old := &recoveryReviewModeSession{recoveryTestSession: f.old, mode: "plan"}
				fresh := &recoveryReviewModeSession{recoveryTestSession: f.fresh, mode: "plan"}
				a := &recoveryReviewModeAgent{recoveryTestAgent: f.a, live: old, baseMode: "default"}
				if source == "startup" {
					f.old.confirmed = true
					a.live, a.initialError, a.baseMode = nil, &recoveryStartupFailure{starter: f.old}, "plan"
				}
				f.e.agent = a
				f.old.onFresh = func(context.Context) (AgentSession, error) { return fresh, nil }
				f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
				usedMode := make(chan string, 1)
				f.fresh.onSend = func(int, string) error {
					usedMode <- fresh.GetMode()
					if outcome == "failure" {
						return errors.New("local model turn failure")
					}
					f.fresh.events <- Event{Type: EventResult, Content: "task continued", Done: true}
					return nil
				}
				msg := f.message("original", "continue original task")
				msg.ModeOverride = "bypassPermissions"
				gen, _ := f.session.TryLock()
				err := f.e.processInteractiveMessage(f.p, msg, f.session, gen)
				if (err != nil) != (outcome == "failure") {
					t.Fatalf("unexpected turn result: %v", err)
				}
				if got := <-usedMode; got != msg.ModeOverride {
					t.Fatalf("recovery input used mode %q, want %q", got, msg.ModeOverride)
				}
				if fresh.GetMode() != "plan" || old.GetMode() != "plan" {
					t.Fatalf("temporary mode leaked after turn: old=%q fresh=%q", old.GetMode(), fresh.GetMode())
				}
				if source == "send" && a.GetMode() != "default" {
					t.Fatal("runtime mode restoration modified shared agent defaults")
				}
			})
		}
	}
}
