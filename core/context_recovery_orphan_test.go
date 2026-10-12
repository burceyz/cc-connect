package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type recoveryOrphanFailurePlatform struct {
	stubPlatformEngine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *recoveryOrphanFailurePlatform) Send(ctx context.Context, replyCtx any, content string) error {
	_ = p.stubPlatformEngine.Send(ctx, replyCtx, content)
	if strings.Contains(content, "recovery_persistence_failed") {
		p.once.Do(func() { close(p.entered) })
		<-p.release
	}
	return nil
}

func recoveryOrphanWait(t *testing.T, label string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestContextRecovery_OrphanQueueCannotBypassPersistenceReviewGate(t *testing.T) {
	f := newRecoveryFixture(t)
	blockWrites, restore := swarmLifecyclePersistenceBarrier(t, f.e.sessions.StorePath())
	defer restore()
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	claimed := &recoveryOrphanFailurePlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		entered:            make(chan struct{}), release: make(chan struct{}),
	}
	var releaseClaimedOnce sync.Once
	releaseClaimed := func() { releaseClaimedOnce.Do(func() { close(claimed.release) }) }
	t.Cleanup(releaseClaimed)
	f.old.onFresh = func(context.Context) (AgentSession, error) {
		if !f.e.queueMessageForBusySession(claimed, f.message("claimed", "part of the recovery input"), f.key) {
			return nil, errors.New("fixture could not queue claimed input")
		}
		return f.fresh, nil
	}
	f.fresh.onSend = func(n int, _ string) error {
		if n > 1 {
			f.fresh.events <- Event{Type: EventResult, Content: "unexpected orphan execution", Done: true}
		}
		return nil
	}
	callbackErr := make(chan error, 1)
	msg := f.message("original", "continue the existing task")
	msg.OnContextRecovery = func(r *ContextRecoveryRecord) {
		if r.Phase == "input_accepted" {
			callbackErr <- blockWrites()
			f.fresh.events <- Event{Type: EventResult, Content: "recovery completed", Done: true}
		}
	}
	gen, _ := f.session.TryLock()
	done := make(chan error, 1)
	go func() { done <- f.e.processInteractiveMessage(f.p, msg, f.session, gen) }()
	recoveryOrphanWait(t, "claimed input failure notification", claimed.entered)
	if err := <-callbackErr; err != nil {
		t.Fatal(err)
	}
	record := f.session.GetContextRecovery()
	if record == nil || !recoveryNeedsReview(record) {
		t.Fatalf("fixture did not establish a review gate: %+v", record)
	}

	// Hold queue acceptance until the failed foreground releases its busy
	// generation. This is the supported orphan-queue race in handleMessage.
	accepted, releaseAccepted, handlerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseAcceptedOnce sync.Once
	releaseLate := func() { releaseAcceptedOnce.Do(func() { close(releaseAccepted) }) }
	t.Cleanup(releaseLate)
	late := f.message("late", "must not execute while the recovery receipt needs review")
	late.OnAccepted = func() { close(accepted); <-releaseAccepted }
	go func() { f.e.handleMessage(f.p, late); close(handlerDone) }()
	recoveryOrphanWait(t, "late queue acceptance", accepted)
	releaseClaimed()
	select {
	case err := <-done:
		if !errors.Is(err, errContextRecoveryPersistence) {
			t.Fatalf("original result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original foreground did not release")
	}
	restore()
	releaseLate()
	recoveryOrphanWait(t, "late handler", handlerDone)
	recoveryAwait(t, "orphan queue settled", func() bool { return !f.session.Busy() })
	if sends := f.fresh.sent(); len(sends) != 1 {
		t.Fatalf("orphan queue bypassed recovery review gate: %d sends; last input=%q; receipt=%+v", len(sends), sends[len(sends)-1], f.session.GetContextRecovery())
	}
}
