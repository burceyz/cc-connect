package core

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ipcQueuedCall struct {
	expected string
	eventID  string
	prompt   string
	report   func(LiveInputProgress) error
}

type ipcQueueTestSession struct {
	stubAgentSession
	live        atomic.Bool
	calls       atomic.Int32
	received    chan ipcQueuedCall
	returnGate  chan struct{}
	beforeQueue chan struct{}
	gateOnce    sync.Once
	holdReturn  bool
	mu          sync.Mutex
	queueError  error
	reports     []func(LiveInputProgress) error
	idle        *ipcTestSession
}

func (s *ipcQueueTestSession) CurrentSessionID() string { return "thread-ipc" }
func (s *ipcQueueTestSession) Alive() bool              { return s.live.Load() }
func (s *ipcQueueTestSession) Events() <-chan Event     { return s.idle.Events() }
func (s *ipcQueueTestSession) Send(prompt, messageID string, images []ImageAttachment, files []FileAttachment) error {
	return s.idle.Send(prompt, messageID, images, files)
}
func (s *ipcQueueTestSession) Close() error {
	s.live.Store(false)
	s.mu.Lock()
	reports := append([]func(LiveInputProgress) error(nil), s.reports...)
	s.mu.Unlock()
	for _, report := range reports {
		_ = report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: LiveInputCodeStopped})
	}
	return nil
}
func (s *ipcQueueTestSession) QueueInput(expected, eventID, prompt string, report func(LiveInputProgress) error) error {
	s.calls.Add(1)
	if s.beforeQueue != nil {
		<-s.beforeQueue
	}
	call := ipcQueuedCall{expected: expected, eventID: eventID, prompt: prompt, report: report}
	s.mu.Lock()
	err := s.queueError
	s.mu.Unlock()
	if err != nil {
		s.received <- call
		return err
	}
	if err := report(LiveInputProgress{Phase: LiveInputPhaseQueued}); err != nil {
		s.received <- call
		return err
	}
	s.mu.Lock()
	s.reports = append(s.reports, report)
	s.mu.Unlock()
	s.received <- call
	if s.holdReturn {
		<-s.returnGate
	}
	return nil
}

var _ LiveInputQueuer = (*ipcQueueTestSession)(nil)

func ipcQueueFixture(t *testing.T) (*APIServer, *Engine, *ipcQueueTestSession, IPCInput) {
	t.Helper()
	api, e, as, input := ipcFixture(t, true)
	q := &ipcQueueTestSession{received: make(chan ipcQueuedCall, 16), returnGate: make(chan struct{}), idle: as}
	q.live.Store(true)
	e.interactiveStates[input.Target.SessionKey].agentSession = q
	t.Cleanup(func() {
		q.gateOnce.Do(func() { close(q.returnGate) })
		_ = q.Close()
	})
	return api, e, q, input
}

func ipcQueued(t *testing.T, q *ipcQueueTestSession) ipcQueuedCall {
	t.Helper()
	select {
	case call := <-q.received:
		return call
	case <-time.After(2 * time.Second):
		t.Fatal("backend did not receive the pending event")
		return ipcQueuedCall{}
	}
}

func ipcWaitStatus(t *testing.T, api *APIServer, eventID, status string) *IPCReceipt {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		receipt := ipcReceipt(t, api, eventID)
		if receipt.Status == status {
			return receipt
		}
		if time.Now().After(deadline) {
			t.Fatalf("want %s; receipt=%+v", status, receipt)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIPCQueue_HTTPReturnsDurableReceiptBeforeBackendAndDeduplicates(t *testing.T) {
	api, _, q, input := ipcQueueFixture(t)
	q.holdReturn = true // Deliberately violate the prompt-return recommendation.
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- ipcPost(t, api.handleIPCEvent, input) }()
	select {
	case response := <-done:
		if response.Code != 200 {
			t.Fatal(response.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP response waited for the backend invocation boundary")
	}
	call := ipcQueued(t, q)
	if call.expected != input.Target.AgentSessionID || call.eventID != input.EventID || call.prompt != input.Prompt {
		t.Fatal("backend input did not preserve the exact event and target")
	}
	queued := ipcReceipt(t, api, input.EventID)
	if queued.Status != "dispatching" || queued.Phase != "queued" || queued.AttemptID == "" || queued.QueuedAt == "" {
		t.Fatalf("queue acceptance is not a durable pending receipt: %+v", queued)
	}
	var duplicates sync.WaitGroup
	for range 12 {
		duplicates.Add(1)
		go func() { defer duplicates.Done(); ipcPost(t, api.handleIPCEvent, input) }()
	}
	duplicates.Wait()
	if q.calls.Load() != 1 {
		t.Fatal("duplicates entered the backend queue", q.calls.Load())
	}
	if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed, TurnID: "fixture-invocation-1"}); err != nil {
		t.Fatal(err)
	}
	claimed := ipcReceipt(t, api, input.EventID) // Reads the file, not an in-memory queue.
	if claimed.Status != "dispatching" || claimed.ClaimedAt == "" || claimed.InjectedAt != "" {
		t.Fatalf("a hook claim was mistaken for delivery: %+v", claimed)
	}
	if err := call.report(LiveInputProgress{Phase: LiveInputPhaseInjected, TurnID: "fixture-observed-input-1"}); err != nil {
		t.Fatal(err)
	}
	delivered := ipcReceipt(t, api, input.EventID)
	if delivered.Status != "delivered" || delivered.InjectedAt == "" || delivered.TurnID != "fixture-observed-input-1" {
		t.Fatalf("confirmed fixture consumption did not finish delivery: %+v", delivered)
	}
	_ = call.report(LiveInputProgress{Phase: LiveInputPhaseUnknown, Code: "late_teardown"})
	if receipt := ipcReceipt(t, api, input.EventID); receipt.Status != "delivered" || receipt.Phase != "injected" {
		t.Fatal("late callback downgraded confirmed delivery")
	}
}

func TestIPCQueue_AckBeforeBackendEndCannotBeDowngraded(t *testing.T) {
	api, _, q, input := ipcQueueFixture(t)
	ipcPost(t, api.handleIPCEvent, input)
	call := ipcQueued(t, q)
	if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed, TurnID: "fixture-claim"}); err != nil {
		t.Fatal(err)
	}
	if response := ipcPost(t, api.handleIPCAck, map[string]any{"event_id": input.EventID, "target": input.Target}); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	for _, phase := range []string{LiveInputPhaseUnknown, LiveInputPhaseReleased} {
		if err := call.report(LiveInputProgress{Phase: phase, Code: LiveInputCodeStopped}); err != nil {
			t.Fatal(err)
		}
	}
	receipt := ipcReceipt(t, api, input.EventID)
	if receipt.Status != "delivered" || receipt.AcknowledgedAt == "" || receipt.Phase != "claimed" || receipt.InjectedAt != "" {
		t.Fatalf("ack evidence was replaced by a backend assumption: %+v", receipt)
	}
	if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed}); err == nil {
		t.Fatal("already acknowledged event authorized another claim")
	}
}

func TestIPCQueue_ReleasedRetryRejectsAllPreviousAttemptCallbacks(t *testing.T) {
	api, _, q, input := ipcQueueFixture(t)
	ipcPost(t, api.handleIPCEvent, input)
	first := ipcQueued(t, q)
	firstAttempt := ipcReceipt(t, api, input.EventID).AttemptID
	if err := first.report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: "turn_finished"}); err != nil {
		t.Fatal(err)
	}
	ipcWaitStatus(t, api, input.EventID, "waiting")
	ipcPost(t, api.handleIPCEvent, input)
	second := ipcQueued(t, q)
	if ipcReceipt(t, api, input.EventID).AttemptID == firstAttempt {
		t.Fatal("safe retry reused its attempt identity")
	}
	for _, phase := range []string{LiveInputPhaseQueued, LiveInputPhaseClaimed, LiveInputPhaseUnknown, LiveInputPhaseReleased} {
		if err := first.report(LiveInputProgress{Phase: phase}); err == nil {
			t.Fatalf("old %s callback was accepted", phase)
		}
	}
	if receipt := ipcReceipt(t, api, input.EventID); receipt.Status != "dispatching" || receipt.Phase != "queued" {
		t.Fatal("old callback modified the replacement attempt")
	}
	if q.calls.Load() != 2 {
		t.Fatal(q.calls.Load())
	}
	_ = second.report(LiveInputProgress{Phase: LiveInputPhaseUnknown, Code: "fixture_finished"})
}

func TestIPCQueue_RestartAndAmbiguousErrorsFreezeWithoutRequeue(t *testing.T) {
	for _, scenario := range []string{"restart", "backend_error"} {
		t.Run(scenario, func(t *testing.T) {
			api, _, q, input := ipcQueueFixture(t)
			if scenario == "backend_error" {
				q.queueError = errors.New("unconfirmed fixture transport")
			}
			ipcPost(t, api.handleIPCEvent, input)
			call := ipcQueued(t, q)
			if scenario == "restart" {
				api.ipcMu.Lock()
				api.ipcBoot = "replacement-boot"
				api.ipcMu.Unlock()
				if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed}); err == nil {
					t.Fatal("old receiver boot authorized a claim")
				}
			}
			ipcWaitStatus(t, api, input.EventID, "unknown")
			for range 3 {
				ipcPost(t, api.handleIPCEvent, input)
			}
			if q.calls.Load() != 1 {
				t.Fatal("uncertain event was requeued")
			}
		})
	}
}

func TestIPCQueue_NoActiveInputCanRetryWithoutClaim(t *testing.T) {
	api, _, q, input := ipcQueueFixture(t)
	q.queueError = ErrNoActiveInput
	ipcPost(t, api.handleIPCEvent, input)
	ipcQueued(t, q)
	receipt := ipcWaitStatus(t, api, input.EventID, "waiting")
	if receipt.ClaimedAt != "" || receipt.Code != "turn_not_ready" {
		t.Fatalf("no-active input was not a safe wait: %+v", receipt)
	}
	q.mu.Lock()
	q.queueError = nil
	q.mu.Unlock()
	ipcPost(t, api.handleIPCEvent, input)
	call := ipcQueued(t, q)
	if q.calls.Load() != 2 {
		t.Fatal("safe wait did not retry")
	}
	_ = call.report(LiveInputProgress{Phase: LiveInputPhaseUnknown})
}

func TestIPCQueue_StopBeforeNoActiveReplyCannotBecomeSafeRetry(t *testing.T) {
	api, e, q, input := ipcQueueFixture(t)
	q.queueError = ErrNoActiveInput
	gate := make(chan struct{})
	q.beforeQueue = gate
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	ipcPost(t, api.handleIPCEvent, input)
	deadline := time.Now().Add(2 * time.Second)
	for q.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if q.calls.Load() == 0 {
		t.Fatal("queue did not start")
	}
	// The receiver saw a busy live session; /stop wins before QueueInput can
	// report that the invocation has ended. A safe retry would undo that stop.
	e.interactiveStates[input.Target.SessionKey].markStopped()
	release.Do(func() { close(gate) })
	ipcQueued(t, q)
	receipt := ipcWaitStatus(t, api, input.EventID, "blocked")
	if receipt.Code != LiveInputCodeStopped || receipt.ClaimedAt != "" {
		t.Fatalf("stop/no-active race was not fenced: %+v", receipt)
	}
}

func TestIPCQueue_NaturalFinishCanResumeOnlyThePinnedIdleSession(t *testing.T) {
	api, e, q, input := ipcQueueFixture(t)
	e.SetInjectSender(true)
	ipcPost(t, api.handleIPCEvent, input)
	call := ipcQueued(t, q)
	if call.prompt != input.Prompt {
		t.Fatal("busy IPC changed the fixed prompt")
	}
	if err := call.report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: "turn_finished"}); err != nil {
		t.Fatal(err)
	}
	session := e.sessions.GetOrCreateActive(input.Target.SessionKey)
	session.ForceUnlock()
	ipcPost(t, api.handleIPCEvent, input)
	if receipt := ipcWaitStatus(t, api, input.EventID, "delivered"); receipt.Method != "start" {
		t.Fatalf("safe release did not resume the pinned idle session: %+v", receipt)
	}
	select {
	case prompt := <-q.idle.input:
		if prompt != call.prompt {
			t.Fatalf("idle IPC changed the previously queued prompt: %q", prompt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle IPC did not reach the pinned session")
	}
	deadline := time.Now().Add(2 * time.Second)
	for session.Busy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if session.Busy() {
		t.Fatal("resumed fixture turn did not finish")
	}
}

func TestIPCQueue_ReleasedRetryNeverRestoresStoppedOrReplacedInstance(t *testing.T) {
	for _, boundary := range []string{"stopped", "closed", "dead", "missing", "replaced", "restarted"} {
		t.Run(boundary, func(t *testing.T) {
			api, e, q, input := ipcQueueFixture(t)
			ipcPost(t, api.handleIPCEvent, input)
			call := ipcQueued(t, q)
			if err := call.report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: "turn_finished"}); err != nil {
				t.Fatal(err)
			}
			ipcWaitStatus(t, api, input.EventID, "waiting")
			session := e.sessions.GetOrCreateActive(input.Target.SessionKey)
			session.ForceUnlock()
			switch boundary {
			case "stopped":
				e.interactiveStates[input.Target.SessionKey].markStopped()
			case "closed":
				e.cleanupInteractiveState(input.Target.SessionKey)
			case "dead":
				q.live.Store(false)
			case "missing":
				delete(e.interactiveStates, input.Target.SessionKey)
			case "replaced":
				e.interactiveStates[input.Target.SessionKey] = &interactiveState{agentSession: q,
					platform: e.platforms[0], replyCtx: input.Target.SessionKey}
			case "restarted":
				api.ipcMu.Lock()
				api.ipcBoot = "new-receiver-boot"
				api.ipcMu.Unlock()
			}
			// All five target fields still match: the fence must identify the
			// retired instance, not merely re-resolve the persisted conversation.
			ipcPost(t, api.handleIPCEvent, input)
			receipt := ipcWaitStatus(t, api, input.EventID, "blocked")
			if receipt.Code != "ipc_queue_instance_changed" || len(q.idle.input) != 0 || q.calls.Load() != 1 {
				t.Fatalf("released event restarted a retired instance: %+v", receipt)
			}
		})
	}
}

type ipcQueueStartTracker struct {
	*ipcTestAgent
	starts    atomic.Int32
	onWorkDir func()
}

func (a *ipcQueueStartTracker) StartSession(ctx context.Context, id string) (AgentSession, error) {
	a.starts.Add(1)
	return a.ipcTestAgent.StartSession(ctx, id)
}

func (a *ipcQueueStartTracker) GetWorkDir() string {
	if a.onWorkDir != nil {
		a.onWorkDir()
	}
	return a.ipcTestAgent.GetWorkDir()
}

func TestIPCQueue_BusyRetryCannotLoseInstanceFence(t *testing.T) {
	for _, boundary := range []string{"missing", "steerer_replacement"} {
		t.Run(boundary, func(t *testing.T) {
			api, e, q, input := ipcQueueFixture(t)
			ipcPost(t, api.handleIPCEvent, input)
			call := ipcQueued(t, q)
			if err := call.report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: "turn_finished"}); err != nil {
				t.Fatal(err)
			}
			tracker := &ipcQueueStartTracker{ipcTestAgent: e.agent.(*ipcTestAgent)}
			var retire sync.Once
			tracker.onWorkDir = func() {
				retire.Do(func() {
					// Target revalidation follows the first retry fence. The
					// session remains busy while its old instance is removed.
					e.interactiveMu.Lock()
					e.interactiveStates[input.Target.SessionKey].markStopped()
					delete(e.interactiveStates, input.Target.SessionKey)
					if boundary == "steerer_replacement" {
						e.interactiveStates[input.Target.SessionKey] = &interactiveState{agentSession: q.idle}
					}
					e.interactiveMu.Unlock()
				})
			}
			e.agent = tracker
			ipcPost(t, api.handleIPCEvent, input)
			receipt := ipcReceipt(t, api, input.EventID)
			if receipt.Status != "blocked" || receipt.Code != "ipc_queue_instance_changed" {
				t.Fatalf("busy retry lost the queue instance fence: %+v", receipt)
			}
			e.sessions.GetOrCreateActive(input.Target.SessionKey).ForceUnlock()
			ipcPost(t, api.handleIPCEvent, input)
			if tracker.starts.Load() != 0 || q.idle.steers.Load() != 0 || len(q.idle.input) != 0 {
				t.Fatal("retired queue instance was restored or replaced by a steer attempt")
			}
		})
	}
}

type ipcQueueBoundaryPlatform struct {
	*ipcTestPlatform
	onReconstruct func()
	onTyping      func()
}

func (p *ipcQueueBoundaryPlatform) ReconstructReplyCtx(key string) (any, error) {
	if p.onReconstruct != nil {
		p.onReconstruct()
	}
	return key, nil
}

func (p *ipcQueueBoundaryPlatform) StartTyping(context.Context, any) func() {
	if p.onTyping != nil {
		p.onTyping()
	}
	return func() {}
}

func TestIPCQueue_StopAfterIdleLeaseCannotRestartBeforeSend(t *testing.T) {
	for _, boundary := range []string{"before_engine_start", "before_send", "replacement_before_send"} {
		t.Run(boundary, func(t *testing.T) {
			api, e, q, input := ipcQueueFixture(t)
			ipcPost(t, api.handleIPCEvent, input)
			call := ipcQueued(t, q)
			if err := call.report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: "turn_finished"}); err != nil {
				t.Fatal(err)
			}
			session := e.sessions.GetOrCreateActive(input.Target.SessionKey)
			session.ForceUnlock()
			tracker := &ipcQueueStartTracker{ipcTestAgent: e.agent.(*ipcTestAgent)}
			e.agent = tracker
			p := &ipcQueueBoundaryPlatform{ipcTestPlatform: e.platforms[0].(*ipcTestPlatform)}
			if boundary == "before_engine_start" {
				p.onReconstruct = func() {
					// This is after the idle lease and instance check but
					// before the asynchronous engine input path begins.
					e.cleanupInteractiveState(input.Target.SessionKey)
				}
			} else {
				p.onTyping = func() {
					// Typing begins after the engine selects its backend,
					// so the final Send check must still reject retirement.
					e.interactiveMu.Lock()
					state := e.interactiveStates[input.Target.SessionKey]
					if boundary == "before_send" {
						state.markStopped()
					} else {
						e.interactiveStates[input.Target.SessionKey] = &interactiveState{agentSession: q}
					}
					e.interactiveMu.Unlock()
				}
			}
			e.platforms = []Platform{p}
			ipcPost(t, api.handleIPCEvent, input)
			receipt := ipcWaitStatus(t, api, input.EventID, "blocked")
			if receipt.Code != "ipc_queue_instance_changed" || tracker.starts.Load() != 0 || len(q.idle.input) != 0 {
				t.Fatalf("stop after dispatch lease restarted the instance: receipt=%+v starts=%d", receipt, tracker.starts.Load())
			}
		})
	}
}

func TestIPCQueue_InvalidTransitionsCannotAuthorizeDeliveryOrReplay(t *testing.T) {
	for _, scenario := range []string{"injected_without_claim", "release_after_claim", "duplicate_claim", "invalid_phase"} {
		t.Run(scenario, func(t *testing.T) {
			api, _, q, input := ipcQueueFixture(t)
			ipcPost(t, api.handleIPCEvent, input)
			call := ipcQueued(t, q)
			if scenario == "release_after_claim" || scenario == "duplicate_claim" {
				if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed}); err != nil {
					t.Fatal(err)
				}
			}
			phase := map[string]string{"injected_without_claim": LiveInputPhaseInjected,
				"release_after_claim": LiveInputPhaseReleased, "duplicate_claim": LiveInputPhaseClaimed,
				"invalid_phase": "delivered"}[scenario]
			if err := call.report(LiveInputProgress{Phase: phase}); err == nil {
				t.Fatal("invalid progress was accepted")
			}
			receipt := ipcReceipt(t, api, input.EventID)
			if receipt.Status == "delivered" || receipt.Status == "waiting" || receipt.InjectedAt != "" {
				t.Fatalf("invalid progress allowed delivery/replay: %+v", receipt)
			}
			if scenario == "injected_without_claim" || scenario == "release_after_claim" {
				if receipt.Status != "unknown" {
					t.Fatal("ambiguous backend protocol violation was not frozen")
				}
			}
			_ = call.report(LiveInputProgress{Phase: LiveInputPhaseUnknown})
		})
	}
}

func TestIPCQueue_StopFencesPendingClaimAndPreservesClaimedUncertainty(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "queued", true: "claimed"}[claimed], func(t *testing.T) {
			api, e, q, input := ipcQueueFixture(t)
			ipcPost(t, api.handleIPCEvent, input)
			call := ipcQueued(t, q)
			if claimed {
				if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed}); err != nil {
					t.Fatal(err)
				}
			}
			e.interactiveStates[input.Target.SessionKey].markStopped()
			if claimed {
				_ = call.report(LiveInputProgress{Phase: LiveInputPhaseUnknown, Code: LiveInputCodeStopped})
				ipcWaitStatus(t, api, input.EventID, "unknown")
			} else {
				if err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed}); err == nil {
					t.Fatal("stopped instance authorized a new claim")
				}
				receipt := ipcWaitStatus(t, api, input.EventID, "blocked")
				if receipt.ClaimedAt != "" {
					t.Fatal("stopped event acquired claim evidence")
				}
			}
			ipcPost(t, api.handleIPCEvent, input)
			if q.calls.Load() != 1 {
				t.Fatal("stop was undone by automatically retrying the event")
			}
		})
	}
}

func TestIPCQueue_CloseCallbackDoesNotAcquireInteractiveMutex(t *testing.T) {
	api, e, q, input := ipcQueueFixture(t)
	ipcPost(t, api.handleIPCEvent, input)
	ipcQueued(t, q)
	e.interactiveMu.Lock()
	done := make(chan struct{})
	go func() { _ = q.Close(); close(done) }()
	select {
	case <-done:
		e.interactiveMu.Unlock()
	case <-time.After(time.Second):
		e.interactiveMu.Unlock()
		t.Fatal("backend teardown callback deadlocked on engine interactive mutex")
	}
	ipcWaitStatus(t, api, input.EventID, "blocked")
}

func TestIPCQueue_ReceiptFailuresPreventQueueAndClaim(t *testing.T) {
	t.Run("initial_save", func(t *testing.T) {
		api, _, q, input := ipcQueueFixture(t)
		api.ipcDir = filepath.Join(t.TempDir(), "not-created")
		response := ipcPost(t, api.handleIPCEvent, input)
		if response.Code != 500 || q.calls.Load() != 0 {
			t.Fatal("backend ran without a durable initial attempt", response.Code)
		}
	})
	t.Run("claim_read", func(t *testing.T) {
		api, _, q, input := ipcQueueFixture(t)
		ipcPost(t, api.handleIPCEvent, input)
		call := ipcQueued(t, q)
		api.ipcMu.Lock()
		original := api.ipcDir
		api.ipcDir = filepath.Join(t.TempDir(), "not-created")
		api.ipcMu.Unlock()
		err := call.report(LiveInputProgress{Phase: LiveInputPhaseClaimed})
		api.ipcMu.Lock()
		api.ipcDir = original
		api.ipcMu.Unlock()
		if err == nil || ipcReceipt(t, api, input.EventID).ClaimedAt != "" {
			t.Fatal("claim succeeded without its durable receipt")
		}
		_ = call.report(LiveInputProgress{Phase: LiveInputPhaseUnknown})
	})
}
