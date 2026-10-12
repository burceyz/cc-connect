package core

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func swarmLifecyclePersistenceBarrier(t *testing.T, path string) (func() error, func()) {
	t.Helper()
	backup := path + ".swarm-backup"
	restore := func() {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			if err := os.Remove(path); err != nil {
				t.Errorf("remove test write barrier: %v", err)
			}
		}
		if _, err := os.Stat(backup); err == nil {
			if err := os.Rename(backup, path); err != nil {
				t.Errorf("restore original test snapshot: %v", err)
			}
		}
	}
	t.Cleanup(restore)
	block := func() error {
		if err := os.Rename(path, backup); err != nil {
			return err
		}
		return os.Mkdir(path, 0o700)
	}
	return block, restore
}

func TestSwarmLifecycle_UnknownRecoverySendCannotResumeAfterCleanup(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.fresh.onSend = func(int, string) error {
		_ = f.fresh.Close()
		return errors.New("send delivery unknown")
	}
	if err := f.run("original", "continue the existing task"); err == nil {
		t.Fatal("expected an unacknowledged recovery send to fail")
	}
	r := f.session.GetContextRecovery()
	t.Logf("after failed recovery send: phase=%s input=%s turn=%s code=%s", r.Phase, r.InputStatus, r.TurnStatus, r.ErrorCode)
	if r.Phase != "failed" || r.InputStatus != "unknown" || r.TurnStatus != "failed" {
		t.Errorf("internal cleanup must preserve the failed delivery evidence: %+v", r)
	}
	if !recoveryNeedsReview(r) {
		t.Errorf("internal executor cleanup must not authorize a new input while delivery is unknown: %+v", r)
	}

	resumed := newRecoveryTestSession(f.fresh.id)
	f.a.session = resumed
	starts := f.a.starts.Load()
	err := f.run("followup", "continue")
	t.Logf("next input: error=%v additional starts=%d prompts=%q", err, f.a.starts.Load()-starts, resumed.sent())
	if err == nil || !strings.Contains(err.Error(), "review_required") {
		t.Errorf("expected review gate after uncertain recovery send, got %v", err)
	}
	if f.a.starts.Load() != starts || len(resumed.sent()) != 0 {
		t.Error("the next input reached a resumed executor without reconciling the uncertain recovery")
	}
}

func TestSwarmLifecycle_AcknowledgementPersistenceFailureKeepsReviewGate(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	blockWrites, restore := swarmLifecyclePersistenceBarrier(t, f.e.sessions.StorePath())
	f.fresh.onSend = func(int, string) error {
		// Preserve the durable sending receipt, while making the following
		// acknowledgement and final-receipt writes deterministically fail.
		return blockWrites()
	}
	err := f.run("original", "continue the existing task")
	if err == nil || err.Error() != "recovery_persistence_failed" {
		t.Fatalf("expected acknowledgement persistence failure, got %v", err)
	}
	r := f.session.GetContextRecovery()
	t.Logf("after acknowledgement write failure: phase=%s input=%s turn=%s code=%s", r.Phase, r.InputStatus, r.TurnStatus, r.ErrorCode)
	if r.ErrorCode != "recovery_persistence_failed" {
		t.Errorf("lost the durable receipt failure reason: %+v", r)
	}
	if !recoveryNeedsReview(r) {
		t.Errorf("persistence failure must retain the review gate: %+v", r)
	}

	restore()
	f.fresh.mu.Lock()
	f.fresh.onSend = func(int, string) error {
		f.fresh.events <- Event{Type: EventResult, Content: "followup ran", Done: true}
		return nil
	}
	f.fresh.mu.Unlock()
	err = f.run("followup", "continue")
	t.Logf("next input: error=%v prompt count=%d", err, len(f.fresh.sent()))
	if err == nil || !strings.Contains(err.Error(), "review_required") {
		t.Errorf("expected review gate after a failed durable receipt, got %v", err)
	}
	if len(f.fresh.sent()) != 1 {
		t.Error("the next input reached the executor after acknowledgement persistence failed")
	}
}

func TestSwarmLifecycle_CompletedModelEvidenceSurvivesReceiptPersistenceFailure(t *testing.T) {
	for _, failedReceipt := range []string{"completion", "late_input_acknowledgement"} {
		t.Run(failedReceipt, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
			blockWrites, restore := swarmLifecyclePersistenceBarrier(t, f.e.sessions.StorePath())
			ready, release := make(chan struct{}), make(chan struct{})
			var readyOnce, releaseOnce sync.Once
			releaseSend := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseSend)
			f.fresh.onSend = func(int, string) error {
				if failedReceipt == "late_input_acknowledgement" {
					f.fresh.events <- Event{Type: EventResult, Content: "task continued", Done: true}
					<-release
				}
				return nil
			}
			msg := f.message("original", "continue the existing task")
			msg.OnContextRecovery = func(record *ContextRecoveryRecord) {
				if failedReceipt == "completion" && record.Phase == "input_accepted" ||
					failedReceipt == "late_input_acknowledgement" && record.Phase == "model_turn_completed" {
					readyOnce.Do(func() { close(ready) })
				}
			}
			gen, locked := f.session.TryLock()
			if !locked {
				t.Fatal("test session is busy")
			}
			done := make(chan error, 1)
			go func() { done <- f.e.processInteractiveMessage(f.p, msg, f.session, gen) }()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("the preceding receipt was not persisted")
			}
			if err := blockWrites(); err != nil {
				t.Fatal(err)
			}
			if failedReceipt == "completion" {
				f.fresh.events <- Event{Type: EventResult, Content: "task continued", Done: true}
			} else {
				releaseSend()
			}
			select {
			case err := <-done:
				if err == nil || err.Error() != "recovery_persistence_failed" {
					t.Fatalf("expected durable receipt failure, got %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("recovery did not return after the receipt failure")
			}
			r := f.session.GetContextRecovery()
			if r.Phase != "model_turn_completed" || r.TurnStatus != "completed" || r.InputStatus != "accepted" || r.ErrorCode != "recovery_persistence_failed" {
				t.Fatalf("completion evidence or failed persistence reason was lost: %+v", r)
			}
			restore()
			if err := f.run("followup", "continue"); err == nil || !strings.Contains(err.Error(), "review_required") {
				t.Fatalf("next input bypassed the failed completion receipt: %v", err)
			}
			if len(f.fresh.sent()) != 1 {
				t.Fatal("the next input reached the executor before receipt reconciliation")
			}
			loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key).GetContextRecovery()
			if loaded == nil || loaded.TurnStatus != "completed" || loaded.ErrorCode != "recovery_persistence_failed" {
				t.Fatalf("a later successful save dropped the completed evidence or review gate: %+v", loaded)
			}
		})
	}
}

func TestSwarmLifecycle_CancelledUncertainReceiptsBlockLaterInput(t *testing.T) {
	for _, scenario := range []struct {
		name, inputStatus, code string
	}{
		{name: "unknown", inputStatus: "unknown"},
		{name: "sending", inputStatus: "sending"},
		{name: "unconfirmed_executor", inputStatus: "not_sent", code: "old_executor_unconfirmed"},
		{name: "failed_persistence", inputStatus: "accepted", code: "recovery_persistence_failed"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.session.ContextRecovery = &ContextRecoveryRecord{ID: "cancelled-recovery", FromSessionID: f.old.id,
				Phase: "cancelled", TurnStatus: "cancelled", InputStatus: scenario.inputStatus, ErrorCode: scenario.code}
			if err := f.run("followup", "continue"); err == nil || !strings.Contains(err.Error(), "review_required") {
				t.Fatalf("cancelled but uncertain recovery did not block later input: %v", err)
			}
			if f.a.starts.Load() != 0 || len(f.old.sent()) != 0 {
				t.Fatal("an executor started before reconciling the cancelled recovery")
			}
		})
	}
}

func TestSwarmLifecycle_StopBeforeRecoveryInputAllowsLaterMessage(t *testing.T) {
	f := newRecoveryFixture(t)
	started := make(chan struct{})
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.old.onFresh = func(ctx context.Context) (AgentSession, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue the existing task") }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not start")
	}
	f.e.stopInteractiveSessionWithOptions(f.key, false)
	if err := <-done; err == nil {
		t.Fatal("stopped recovery unexpectedly succeeded")
	}
	r := f.session.GetContextRecovery()
	if r.Phase != "cancelled" || r.InputStatus != "not_sent" {
		t.Fatalf("stop before input produced an uncertain delivery: %+v", r)
	}
	resumed := newRecoveryTestSession(f.old.id)
	f.a.session = resumed
	if err := f.run("followup", "inspect the local draft"); err != nil {
		t.Fatalf("confirmed stop before sending blocked later user input: %v", err)
	}
	if len(resumed.sent()) != 1 || f.session.GetContextRecovery().ID != r.ID {
		t.Fatal("later input must run while preserving the original spent recovery budget")
	}
}

func TestSwarmLifecycle_BusySelfHealKeepsRecoveryGeneration(t *testing.T) {
	f := newRecoveryFixture(t)
	// Keep any stale-lock diagnostic output inside this test's temporary area.
	t.Setenv("TMPDIR", t.TempDir())
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseFresh := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFresh)
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
	terminal := make(chan *ContextRecoveryRecord, 4)
	first := f.message("original", "prepare and publish the existing report")
	first.OnContextRecovery = func(record *ContextRecoveryRecord) {
		switch record.Phase {
		case "failed", "cancelled", "model_turn_completed":
			select {
			case terminal <- record:
			default:
			}
		}
	}
	f.e.ReceiveMessage(f.p, first)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("fresh context preparation did not start")
	}
	// The old turn can already have run for over two minutes before its
	// invalid-context error. No long sleep or lower production threshold is
	// needed to represent that state at the recovery-start boundary.
	f.session.mu.Lock()
	f.session.busySince = time.Now().Add(-f.e.staleLockBreakAfter - time.Minute)
	generation := f.session.lockGen
	f.session.mu.Unlock()
	accepted := make(chan struct{})
	update := f.message("scope-change", "cancel publication; only prepare the local draft")
	update.OnAccepted = func() { close(accepted) }
	f.e.ReceiveMessage(f.p, update)
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("scope change was not accepted")
	}
	f.session.mu.Lock()
	gotGeneration := f.session.lockGen
	f.session.mu.Unlock()
	t.Logf("recovery busy generation before=%d after=%d", generation, gotGeneration)
	if gotGeneration != generation {
		t.Error("busy self-heal invalidated the live recovery while the old executor was intentionally stopped")
	}
	releaseFresh()
	select {
	case r := <-terminal:
		t.Logf("recovery receipt after latest scope update: phase=%s code=%s", r.Phase, r.ErrorCode)
		if r.Phase != "model_turn_completed" {
			t.Errorf("the accepted scope change must remain queued for recovery, got phase=%s code=%s", r.Phase, r.ErrorCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not finish")
	}
	recoveryAwait(t, "recovery executor settled", func() bool {
		return !f.session.Busy() && (!f.fresh.Alive() || len(f.fresh.sent()) > 0)
	})
	if prompts := f.fresh.sent(); len(prompts) != 1 || !strings.Contains(prompts[0], "cancel publication") {
		t.Errorf("latest scope change did not reach the replacement context: %q", prompts)
	}
}

func TestSwarmLifecycle_NewSupplementFailureReplacesStaleSignal(t *testing.T) {
	f := newRecoveryFixture(t)
	gen, locked := f.session.TryLock()
	if !locked {
		t.Fatal("test session is busy")
	}
	defer f.session.Unlock(gen)
	oldInvocation := f.e.recoveryInvocationForMessage(f.message("old-turn", "continue the report"))
	newInvocation := f.e.recoveryInvocationForMessage(f.message("queued-turn", "inspect the local draft"))
	state := &interactiveState{agentSession: f.old, agent: f.a, platform: f.p, replyCtx: f.key,
		busySession: f.session, logicalSessionKey: f.key, currentInvocation: oldInvocation, currentTurnStartedAt: time.Now()}
	f.e.interactiveMu.Lock()
	f.e.interactiveStates[f.key] = state
	f.e.interactiveMu.Unlock()
	if !f.e.reportRecoveryInputFailure(state, f.session, f.e.sessions, f.key, f.old, oldInvocation, recoveryTestFailure{}) {
		t.Fatal("the old invocation's supplemental failure was not accepted")
	}
	// EventResult can select the next queued turn before the ready signal
	// is consumed. Its typing/card setup runs before the next select, so a
	// new supplemental Send can fail during that interval as well.
	state.mu.Lock()
	state.currentInvocation = newInvocation
	state.mu.Unlock()
	if !f.e.reportRecoveryInputFailure(state, f.session, f.e.sessions, f.key, f.old, newInvocation, recoveryTestFailure{}) {
		t.Fatal("the current invocation's supplemental failure was not accepted")
	}
	state.mu.Lock()
	queued := state.recoveryInputFailure
	err := state.takeRecoveryInputFailureLocked()
	state.mu.Unlock()
	if queued == nil || queued.invocation != newInvocation {
		t.Error("the stale pending failure masked the current invocation's accepted failure")
	}
	if !isContextRecoveryError(err) {
		t.Errorf("the current supplemental failure disappeared when the stale signal was consumed: %v", err)
	}
}

func TestSwarmLifecycle_PendingSupplementFailureKeepsBusyGenerationAndLatestInput(t *testing.T) {
	swarmLifecycleSupplementFailureKeepsBusyGeneration(t, false)
}

func TestSwarmLifecycle_ClaimedSupplementFailureKeepsBusyGenerationAndLatestInput(t *testing.T) {
	swarmLifecycleSupplementFailureKeepsBusyGeneration(t, true)
}

func swarmLifecycleSupplementFailureKeepsBusyGeneration(t *testing.T, claimSignal bool) {
	t.Helper()
	f := newRecoveryFixture(t)
	t.Setenv("TMPDIR", t.TempDir())
	gen, locked := f.session.TryLock()
	if !locked {
		t.Fatal("test session is busy")
	}
	defer f.session.Unlock(gen)
	invocation := f.e.recoveryInvocationForMessage(f.message("original", "prepare and publish the report"))
	state := &interactiveState{agentSession: f.old, agent: f.a, platform: f.p, replyCtx: f.key,
		busySession: f.session, logicalSessionKey: f.key, currentInvocation: invocation, currentTurnStartedAt: time.Now()}
	f.e.interactiveMu.Lock()
	f.e.interactiveStates[f.key] = state
	f.e.interactiveMu.Unlock()
	// The supplemental RPC reports the typed rejection after its executor
	// exits, while the foreground is still handling a preceding event.
	_ = f.old.Close()
	if !f.e.reportRecoveryInputFailure(state, f.session, f.e.sessions, f.key, f.old, invocation, recoveryTestFailure{}) {
		t.Fatal("the supplemental failure was not queued")
	}
	if claimSignal {
		// The consumer has taken the signal, but may still be finalizing a
		// progress card or stopping its idle reader before preparation starts.
		state.mu.Lock()
		cause := state.takeRecoveryInputFailureLocked()
		state.mu.Unlock()
		if !isContextRecoveryError(cause) {
			t.Fatal("the pending supplemental failure was not claimed")
		}
	}
	f.session.mu.Lock()
	f.session.busySince = time.Now().Add(-f.e.staleLockBreakAfter - time.Minute)
	f.session.mu.Unlock()
	f.e.ReceiveMessage(f.p, f.message("scope-change", "cancel publication; only prepare the local draft"))
	f.session.mu.Lock()
	gotGeneration := f.session.lockGen
	f.session.mu.Unlock()
	state.mu.Lock()
	queued := append([]queuedMessage(nil), state.pendingMessages...)
	state.mu.Unlock()
	t.Logf("pending recovery signal: generation before=%d after=%d queued updates=%d", gen, gotGeneration, len(queued))
	if gotGeneration != gen {
		t.Error("busy self-heal broke the turn before its pending recovery failure was consumed")
		// The buggy branch started an asynchronous ordinary turn; allow that
		// fake processor to finish before test fixture cleanup.
		recoveryAwait(t, "unexpected ordinary turn settled", func() bool { return !f.session.Busy() })
	}
	if len(queued) != 1 || queued[0].messageID != "scope-change" {
		t.Errorf("the pending recovery signal did not protect the latest accepted scope update: %+v", queued)
	}
}
