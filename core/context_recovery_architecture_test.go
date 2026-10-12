package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestContextRecovery_PastRecoveryDoesNotDropOrdinaryQueuedInput(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		name := "without_recovery"
		if recovered {
			name = "after_completed_recovery"
		}
		t.Run(name, func(t *testing.T) {
			f := newRecoveryFixture(t)
			current := f.old
			if recovered {
				f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
				current = f.fresh
			}
			if err := f.run("initial", "complete initial task"); err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseSend := func() { once.Do(func() { close(release) }) }
			t.Cleanup(releaseSend)
			failureSend := len(current.sent()) + 1
			current.mu.Lock()
			current.onSend = func(n int, _ string) error {
				if n == failureSend {
					close(started)
					<-release
					current.events <- Event{Type: EventError, Error: errors.New("ordinary per-turn failure")}
				} else {
					current.events <- Event{Type: EventResult, Content: "queued task handled", Done: true}
				}
				return nil
			}
			current.mu.Unlock()
			done := make(chan error, 1)
			go func() { done <- f.run("ordinary", "later ordinary task") }()
			<-started
			if !f.e.queueMessageForBusySession(f.p, f.message("queued", "accepted next task"), f.key) {
				t.Fatal("queued input rejected")
			}
			releaseSend()
			if err := <-done; err == nil {
				t.Fatal("ordinary error was lost")
			}
			if got := len(current.sent()); got != failureSend+1 {
				t.Fatalf("ordinary queued input lost after historical recovery: sends=%d, want=%d", got, failureSend+1)
			}
			if !strings.Contains(strings.Join(f.p.getSent(), "\n"), "queued task handled") {
				t.Fatal("queued user did not receive its result")
			}
		})
	}
}

func TestContextRecovery_CommitFailureNotifiesClaimedQueuedRecipient(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires ordinary Unix directory permissions")
	}
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseStart := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseStart)
	t.Cleanup(func() { _ = os.Chmod(f.a.dir, 0o700) })
	f.old.onFresh = func(context.Context) (AgentSession, error) {
		close(started)
		<-release
		if err := os.Chmod(f.a.dir, 0o500); err != nil {
			return nil, err
		}
		return f.fresh, nil
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue original task") }()
	<-started
	recipient := &ipcTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	if !f.e.queueMessageForBusySession(recipient, f.message("queued", "supplement from another recipient"), f.key) {
		t.Fatal("queued input rejected")
	}
	before := len(recipient.getSent())
	releaseStart()
	if err := <-done; !errors.Is(err, errContextRecoveryPersistence) {
		t.Fatalf("expected commit persistence failure, got %v", err)
	}
	if len(f.fresh.sent()) != 0 {
		t.Fatal("recovery input sent before durable commit")
	}
	if got := recipient.getSent(); len(got) != before+1 || !strings.Contains(got[len(got)-1], "recovery_persistence_failed") {
		t.Fatalf("claimed queued recipient lost its failure receipt: %v", got)
	}
	if material := readRecoveryBundle(t, f.session); len(material.LaterInputs) != 1 {
		t.Fatal("claimed queued input disappeared from saved evidence")
	}
}

// Hold the existing snapshot writer after fresh startup. No injected network
// or filesystem implementation is needed to reproduce a delayed commit.
func recoveryHoldBindingStore(t *testing.T, f *recoveryFixture) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	releaseStore := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseStore)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.old.onFresh = func(context.Context) (AgentSession, error) {
		f.e.sessions.saveMu.Lock()
		go func() { <-release; f.e.sessions.saveMu.Unlock() }()
		return f.fresh, nil
	}
	return releaseStore
}

func TestContextRecovery_StoreWaitDoesNotBlockUnrelatedSession(t *testing.T) {
	f := newRecoveryFixture(t)
	releaseStore := recoveryHoldBindingStore(t, f)
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue original task") }()
	recoveryAwait(t, "binding commit", func() bool {
		r := f.session.GetContextRecovery()
		return r != nil && r.ToSessionID == f.fresh.id
	})
	commandDone := make(chan struct{})
	go func() {
		f.e.cmdPs(f.p, &Message{SessionKey: "test:unrelated", ReplyCtx: "test:unrelated"}, []string{"status"})
		close(commandDone)
	}()
	select {
	case <-commandDone:
	case <-time.After(500 * time.Millisecond):
		t.Error("unrelated session command waited for the recovery snapshot writer")
	}
	releaseStore()
	<-commandDone
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestContextRecovery_ReviewGateExplainsReasonAndRecord(t *testing.T) {
	f := newRecoveryFixture(t)
	artifact := filepath.Join(f.a.dir, "context-recovery", "needs-review")
	f.session.ContextRecovery = &ContextRecoveryRecord{ID: "needs-review", Phase: "failed", FromSessionID: f.old.id,
		ErrorCode: "old_executor_unconfirmed", InputStatus: "not_sent", TurnStatus: "failed", ArtifactDir: artifact}
	if err := f.run("followup", "continue"); err == nil {
		t.Fatal("unconfirmed recovery admitted another input")
	}
	visible := strings.Join(f.p.getSent(), "\n")
	for _, want := range []string{"needs-review", artifact, "/new", "stopped"} {
		if !strings.Contains(visible, want) {
			t.Errorf("review message does not explain %q: %s", want, visible)
		}
	}
	if f.a.starts.Load() != 0 {
		t.Fatal("review gate started an executor")
	}
}

func TestContextRecovery_InputDuringStoreWaitIsIncludedOnce(t *testing.T) {
	f := newRecoveryFixture(t)
	releaseStore := recoveryHoldBindingStore(t, f)
	done := make(chan error, 1)
	go func() { done <- f.run("original", "publish the draft") }()
	recoveryAwait(t, "binding commit", func() bool {
		r := f.session.GetContextRecovery()
		return r != nil && r.ToSessionID == f.fresh.id
	})
	queued := make(chan bool, 1)
	go func() {
		queued <- f.e.queueMessageForBusySession(f.p, f.message("correction", "do not publish; keep the draft local"), f.key)
	}()
	select {
	case accepted := <-queued:
		if !accepted {
			t.Fatal("input during storage wait was rejected")
		}
	case <-time.After(500 * time.Millisecond):
		releaseStore()
		<-queued
		<-done
		t.Fatal("input admission blocked on the snapshot writer")
	}
	if len(f.fresh.sent()) != 0 {
		t.Fatal("fresh input was sent before durable commit")
	}
	releaseStore()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if sends := f.fresh.sent(); len(sends) != 1 || !strings.Contains(sends[0], "do not publish; keep the draft local") {
		t.Fatalf("late correction missing or replayed: %v", sends)
	}
	m := readRecoveryBundle(t, f.session)
	if len(m.LaterInputs) != 1 || m.LaterInputs[0].MessageID != "correction" {
		t.Fatalf("late input evidence is wrong: %+v", m.LaterInputs)
	}
	count := 0
	for _, entry := range f.session.GetHistory(0) {
		if entry.Role == "user" && entry.Content == "do not publish; keep the draft local" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("late input recorded %d times, want 1", count)
	}
}

func TestContextRecovery_StoreWaitAllowsStopAndNotifiesClaimedInputsOnce(t *testing.T) {
	f := newRecoveryFixture(t)
	releaseStore := recoveryHoldBindingStore(t, f)
	blockedFresh := f.old.onFresh
	recipient := &ipcTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	f.old.onFresh = func(ctx context.Context) (AgentSession, error) {
		if !f.e.queueMessageForBusySession(recipient, f.message("claimed", "queued before commit"), f.key) {
			return nil, errors.New("fixture input rejected")
		}
		return blockedFresh(ctx)
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue task") }()
	recoveryAwait(t, "binding commit", func() bool {
		r := f.session.GetContextRecovery()
		return r != nil && r.ToSessionID == f.fresh.id
	})
	stopDone := make(chan bool, 1)
	go func() { stopDone <- f.e.stopInteractiveSessionWithOptions(f.key, true) }()
	select {
	case stopped := <-stopDone:
		if !stopped {
			t.Fatal("stop did not find the recovering session")
		}
	case <-time.After(500 * time.Millisecond):
		releaseStore()
		<-stopDone
		<-done
		t.Fatal("stop waited for the recovery snapshot writer")
	}
	releaseStore()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped recovery returned %v", err)
	}
	if len(f.fresh.sent()) != 0 || f.fresh.Alive() {
		t.Fatal("cancelled recovery sent work or kept the fresh executor alive")
	}
	if got := recipient.getSent(); len(got) != 2 || !strings.Contains(got[1], "session reset") {
		t.Fatalf("claimed input needs exactly one cancellation notice after queue acceptance: %v", got)
	}
}

func TestContextRecovery_ClaimedInputsKeepFailureReceiptAfterSend(t *testing.T) {
	for _, failure := range []string{"unknown_send", "failed_turn"} {
		t.Run(failure, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
			recipient := &ipcTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
			f.old.onFresh = func(context.Context) (AgentSession, error) {
				if !f.e.queueMessageForBusySession(recipient, f.message("claimed", "update the task"), f.key) {
					return nil, errors.New("fixture input rejected")
				}
				return f.fresh, nil
			}
			f.fresh.onSend = func(int, string) error {
				err := errors.New("local fake recovery failure")
				if failure == "unknown_send" {
					return err
				}
				f.fresh.events <- Event{Type: EventError, Error: err}
				return nil
			}
			if err := f.run("original", "continue"); err == nil {
				t.Fatal("fixture did not fail")
			}
			if got := recipient.getSent(); len(got) != 2 || !strings.Contains(got[1], "local fake recovery failure") {
				t.Fatalf("claimed input lost or repeated its failure notice: %v", got)
			}
			if len(f.fresh.sent()) != 1 {
				t.Fatal("claimed input was automatically replayed after failure")
			}
		})
	}
}

func TestContextRecovery_ReceiptStoreWaitDoesNotBlockStop(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	release := make(chan struct{})
	var once sync.Once
	releaseStore := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseStore)
	f.fresh.onSend = func(int, string) error {
		f.e.sessions.saveMu.Lock()
		go func() { <-release; f.e.sessions.saveMu.Unlock() }()
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- f.run("original", "continue") }()
	recoveryAwait(t, "input receipt awaiting storage", func() bool {
		r := f.session.GetContextRecovery()
		return r != nil && r.InputStatus == "accepted"
	})
	stopDone := make(chan bool, 1)
	go func() { stopDone <- f.e.stopInteractiveSessionWithOptions(f.key, true) }()
	select {
	case stopped := <-stopDone:
		if !stopped {
			t.Fatal("recovering session not stopped")
		}
	case <-time.After(500 * time.Millisecond):
		releaseStore()
		<-stopDone
		<-done
		t.Fatal("stop waited for input receipt storage")
	}
	releaseStore()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped recovery returned %v", err)
	}
}

func TestContextRecovery_CompletedRecoveryThenQueuedOrdinaryFailure(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	f.fresh.onSend = func(n int, _ string) error {
		switch n {
		case 1:
			f.e.queueMessageForBusySession(f.p, f.message("second", "ordinary task"), f.key)
			f.e.queueMessageForBusySession(f.p, f.message("third", "task after ordinary error"), f.key)
			f.fresh.events <- Event{Type: EventResult, Content: "recovery complete", Done: true}
		case 2:
			f.fresh.events <- Event{Type: EventError, Error: errors.New("ordinary task failure")}
		default:
			f.fresh.events <- Event{Type: EventResult, Content: "third task completed", Done: true}
		}
		return nil
	}
	if err := f.run("original", "continue"); err == nil || err.Error() != "ordinary task failure" {
		t.Fatalf("ordinary error lost: %v", err)
	}
	if got := len(f.fresh.sent()); got != 3 {
		t.Fatalf("later queued task was affected by the completed recovery: sends=%d", got)
	}
	if !strings.Contains(strings.Join(f.p.getSent(), "\n"), "third task completed") {
		t.Fatal("third task result missing")
	}
}

func TestContextRecovery_LateReceiptFailureFencesAlreadyQueuedTurn(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	blockWrites, restore := swarmLifecyclePersistenceBarrier(t, f.e.sessions.StorePath())
	defer restore()
	completed, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseSend := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseSend)
	f.fresh.onSend = func(int, string) error {
		f.e.queueMessageForBusySession(f.p, f.message("next", "must wait for the recovery receipt"), f.key)
		f.fresh.events <- Event{Type: EventResult, Content: "model completed recovery", Done: true}
		<-release
		return nil
	}
	msg := f.message("original", "continue")
	msg.OnContextRecovery = func(r *ContextRecoveryRecord) {
		if r.Phase == "model_turn_completed" {
			close(completed)
		}
	}
	gen, _ := f.session.TryLock()
	done := make(chan error, 1)
	go func() { done <- f.e.processInteractiveMessage(f.p, msg, f.session, gen) }()
	<-completed
	if err := blockWrites(); err != nil {
		t.Fatal(err)
	}
	releaseSend()
	if err := <-done; !errors.Is(err, errContextRecoveryPersistence) {
		t.Fatalf("late receipt did not stop the turn: %v", err)
	}
	if got := len(f.fresh.sent()); got != 1 {
		t.Fatalf("queued work bypassed an unpersisted recovery receipt: sends=%d", got)
	}
}
