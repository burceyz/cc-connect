package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recoverySourcesRunTurn(t *testing.T, f *recoveryFixture, run func() error) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- run()
	}()
	t.Cleanup(func() {
		f.e.cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("foreground test turn did not exit after cancellation")
		}
	})
	return done
}

func recoverySourcesRunPS(t *testing.T, f *recoveryFixture, id, text string) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.e.cmdPs(f.p, f.message(id, "/ps "+text), []string{text})
	}()
	t.Cleanup(func() {
		f.e.cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("supplemental test input did not exit after cancellation")
		}
	})
	return done
}

func recoverySourcesWaitSignal(t *testing.T, reason string, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out: %s", reason)
	}
}

func recoverySourcesWaitTurn(t *testing.T, reason string, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out: %s", reason)
		return nil
	}
}

func TestContextRecoverySwarmSources_PSTypedSendFailureReachesRecovery(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(n int, _ string) error {
		if n == 2 {
			return recoveryTestFailure{}
		}
		return nil
	}
	done := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "finish the report") })
	recoveryAwait(t, "original turn input", func() bool { return len(f.old.sent()) == 1 })

	// Codex turn/start RPC failures can be returned by Send without an
	// additional terminal notification. /ps must not hide that typed fault.
	f.e.cmdPs(f.p, f.message("ps-update", "/ps only prepare locally"), []string{"only prepare locally"})
	if err := recoverySourcesWaitTurn(t, "supplemental fault recovery", done); err != nil {
		t.Fatal(err)
	}
	if f.old.freshCalls.Load() != 1 || len(f.fresh.sent()) != 1 {
		t.Fatalf("typed /ps Send failure was not recovered: fresh starts=%d fresh inputs=%d", f.old.freshCalls.Load(), len(f.fresh.sent()))
	}
	if !strings.Contains(f.fresh.sent()[0], "only prepare locally") {
		t.Error("recovered turn lost the /ps scope restriction")
	}
}

func TestContextRecoverySwarmSources_PSTypedFailureAfterResultRecoversOnce(t *testing.T) {
	for _, executorExited := range []bool{false, true} {
		name := "executor-alive"
		if executorExited {
			name = "executor-exited"
		}
		t.Run(name, func(t *testing.T) {
			f := newRecoveryFixture(t)
			psStarted, psRelease := make(chan struct{}), make(chan struct{})
			f.old.onSend = func(n int, _ string) error {
				if n == 2 {
					close(psStarted)
					select {
					case <-psRelease:
						return recoveryTestFailure{}
					case <-f.e.ctx.Done():
						return f.e.ctx.Err()
					}
				}
				return nil
			}
			originalDone := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "finish the report") })
			recoveryAwait(t, "original turn input", func() bool { return len(f.old.sent()) == 1 })
			psDone := recoverySourcesRunPS(t, f, "ps-update", "only prepare locally")
			recoverySourcesWaitSignal(t, "supplemental input entered", psStarted)

			f.old.events <- Event{Type: EventResult, Content: "original step already completed", Done: true}
			if err := recoverySourcesWaitTurn(t, "original result", originalDone); err != nil {
				t.Fatal(err)
			}
			if f.session.Busy() || f.session.GetContextRecovery() != nil {
				t.Fatal("original turn did not finish before supplemental RPC reply")
			}
			if executorExited {
				if err := f.old.Close(); err != nil {
					t.Fatal(err)
				}
			}
			close(psRelease)
			recoverySourcesWaitSignal(t, "late supplemental error returned", psDone)
			recoveryAwait(t, "orphan supplemental fault recovery completed", func() bool {
				r := f.session.GetContextRecovery()
				return r != nil && r.TurnStatus == "completed" && !f.session.Busy()
			})
			if f.old.freshCalls.Load() != 1 || len(f.fresh.sent()) != 1 {
				t.Fatalf("late supplemental failure recovery count: starts=%d inputs=%d", f.old.freshCalls.Load(), len(f.fresh.sent()))
			}
			for _, evidence := range []string{"only prepare locally", "original step already completed"} {
				if !strings.Contains(f.fresh.sent()[0], evidence) {
					t.Errorf("late recovery lost evidence %q", evidence)
				}
			}
		})
	}
}

func TestContextRecoverySwarmSources_PSLateFailureCannotInterruptNewOrSwitchedTask(t *testing.T) {
	for _, command := range []string{"new", "switch"} {
		t.Run(command, func(t *testing.T) {
			f := newRecoveryFixture(t)
			psStarted, psRelease := make(chan struct{}), make(chan struct{})
			f.old.onSend = func(n int, _ string) error {
				if n == 2 {
					close(psStarted)
					select {
					case <-psRelease:
						return recoveryTestFailure{}
					case <-f.e.ctx.Done():
						return f.e.ctx.Err()
					}
				}
				return nil
			}
			replacement := newRecoveryTestSession("native-next-task")
			replacement.onSend = func(int, string) error { return nil }
			replacement.onFresh = func(context.Context) (AgentSession, error) {
				return nil, errors.New("unexpected recovery of replacement task")
			}
			f.a.listed = []AgentSessionInfo{{ID: replacement.id, Summary: "next task"}}
			originalDone := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "old task") })
			recoveryAwait(t, "original input", func() bool { return len(f.old.sent()) == 1 })
			psDone := recoverySourcesRunPS(t, f, "ps-old", "old task supplement")
			recoverySourcesWaitSignal(t, "old supplemental input entered", psStarted)

			if command == "new" {
				f.e.cmdNew(f.p, f.message("new-task", "/new"), nil)
			} else {
				f.e.cmdSwitch(f.p, f.message("switch-task", "/switch"), []string{replacement.id})
			}
			if err := recoverySourcesWaitTurn(t, "old task stopped", originalDone); err == nil {
				t.Fatal("old task unexpectedly completed instead of being stopped")
			}
			f.a.session = replacement
			nextSession := f.e.sessions.GetOrCreateActive(f.key)
			nextDone := recoverySourcesRunTurn(t, f, func() error {
				gen, ok := nextSession.TryLock()
				if !ok {
					return errors.New("new task session is busy")
				}
				return f.e.processInteractiveMessage(f.p, f.message("next", "work on the new task"), nextSession, gen)
			})
			recoveryAwait(t, "replacement task input", func() bool { return len(replacement.sent()) == 1 })
			close(psRelease)
			recoverySourcesWaitSignal(t, "old supplemental RPC error returned", psDone)
			if f.old.freshCalls.Load() != 0 || replacement.freshCalls.Load() != 0 || !replacement.Alive() || !nextSession.Busy() {
				t.Fatal("late old supplemental failure interrupted the replacement task")
			}
			if f.session.GetContextRecovery() != nil || nextSession.GetContextRecovery() != nil || f.e.sessions.GetOrCreateActive(f.key) != nextSession {
				t.Fatal("late old failure changed task selection or reserved a recovery")
			}
			replacement.events <- Event{Type: EventResult, Content: "new task completed", Done: true}
			if err := recoverySourcesWaitTurn(t, "replacement task completed", nextDone); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContextRecoverySwarmSources_PSLateFailureCannotInterruptNextInvocation(t *testing.T) {
	f := newRecoveryFixture(t)
	psStarted, psRelease := make(chan struct{}), make(chan struct{})
	f.old.onSend = func(n int, _ string) error {
		if n == 2 {
			close(psStarted)
			select {
			case <-psRelease:
				return recoveryTestFailure{}
			case <-f.e.ctx.Done():
				return f.e.ctx.Err()
			}
		}
		return nil
	}
	originalDone := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "first task") })
	recoveryAwait(t, "original input", func() bool { return len(f.old.sent()) == 1 })
	psDone := recoverySourcesRunPS(t, f, "old-ps", "supplement the first task")
	recoverySourcesWaitSignal(t, "old supplemental input entered", psStarted)
	f.old.events <- Event{Type: EventResult, Content: "first task completed", Done: true}
	if err := recoverySourcesWaitTurn(t, "first task completed", originalDone); err != nil {
		t.Fatal(err)
	}
	nextDone := recoverySourcesRunTurn(t, f, func() error { return f.run("next", "work on a separate task") })
	recoveryAwait(t, "next invocation input", func() bool { return len(f.old.sent()) == 3 })
	close(psRelease)
	recoverySourcesWaitSignal(t, "old supplemental error returned during next invocation", psDone)
	if f.old.freshCalls.Load() != 0 || !f.old.Alive() || !f.session.Busy() || f.session.GetContextRecovery() != nil {
		t.Fatal("late supplemental failure interrupted the next invocation on the same executor")
	}
	f.old.events <- Event{Type: EventResult, Content: "separate task completed", Done: true}
	if err := recoverySourcesWaitTurn(t, "next invocation completed", nextDone); err != nil {
		t.Fatal(err)
	}
}

func TestContextRecoverySwarmSources_PSLateFailureCannotInterruptRecoveredExecutor(t *testing.T) {
	f := newRecoveryFixture(t)
	psStarted, psRelease := make(chan struct{}), make(chan struct{})
	f.old.onSend = func(n int, _ string) error {
		if n == 2 {
			close(psStarted)
			select {
			case <-psRelease:
				return recoveryTestFailure{}
			case <-f.e.ctx.Done():
				return f.e.ctx.Err()
			}
		}
		return nil
	}
	f.fresh.onSend = func(int, string) error { return nil }
	originalDone := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "finish the task") })
	recoveryAwait(t, "original input", func() bool { return len(f.old.sent()) == 1 })
	psDone := recoverySourcesRunPS(t, f, "old-ps", "keep publication cancelled")
	recoverySourcesWaitSignal(t, "old supplemental input entered", psStarted)
	f.old.events <- Event{Type: EventError, Error: recoveryTestFailure{}}
	recoveryAwait(t, "replacement executor accepted input", func() bool {
		f.e.interactiveMu.Lock()
		state := f.e.interactiveStates[f.key]
		f.e.interactiveMu.Unlock()
		if state == nil {
			return false
		}
		state.mu.Lock()
		ready := state.agentSession == f.fresh && !state.recovering && !state.recoveryInputPending
		state.mu.Unlock()
		return ready && len(f.fresh.sent()) == 1
	})
	close(psRelease)
	recoverySourcesWaitSignal(t, "old supplemental error returned after executor replacement", psDone)
	if f.old.freshCalls.Load() != 1 || f.fresh.freshCalls.Load() != 0 || !f.fresh.Alive() || !f.session.Busy() {
		t.Fatal("old supplemental failure interrupted the recovered executor")
	}
	f.fresh.events <- Event{Type: EventResult, Content: "recovered task completed", Done: true}
	if err := recoverySourcesWaitTurn(t, "recovered task completed", originalDone); err != nil {
		t.Fatal(err)
	}
}

func TestContextRecoverySwarmSources_PSDuplicateSignalsRecoverOnlyOnce(t *testing.T) {
	f := newRecoveryFixture(t)
	mainSendRelease := make(chan struct{})
	f.old.onSend = func(n int, _ string) error {
		if n == 1 {
			select {
			case <-mainSendRelease:
				return nil
			case <-f.e.ctx.Done():
				return f.e.ctx.Err()
			}
		}
		return recoveryTestFailure{}
	}
	originalDone := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "finish the task") })
	recoveryAwait(t, "original input", func() bool { return len(f.old.sent()) == 1 })
	f.old.events <- Event{Type: EventResult, Content: "first part completed", Done: true}
	recoveryAwait(t, "original EventResult consumed before Send returned", func() bool {
		for _, entry := range f.session.GetHistory(0) {
			if entry.Role == "assistant" && entry.Content == "first part completed" {
				return true
			}
		}
		return false
	})
	f.e.interactiveMu.Lock()
	state := f.e.interactiveStates[f.key]
	f.e.interactiveMu.Unlock()
	state.mu.Lock()
	invocation := state.currentInvocation
	state.mu.Unlock()
	// The event consumer is waiting for the original Send return. Both typed
	// errors arrive while the same invocation still owns its busy lock.
	f.e.cmdPs(f.p, f.message("ps-one", "/ps first correction"), []string{"first correction"})
	f.e.cmdPs(f.p, f.message("ps-two", "/ps second correction"), []string{"second correction"})
	if f.old.freshCalls.Load() != 0 {
		t.Fatal("supplemental failure bypassed the foreground owner")
	}
	close(mainSendRelease)
	if err := recoverySourcesWaitTurn(t, "deduplicated supplemental recovery", originalDone); err != nil {
		t.Fatal(err)
	}
	if f.old.freshCalls.Load() != 1 || len(f.fresh.sent()) != 1 {
		t.Fatalf("duplicate signals created repeated recovery: starts=%d inputs=%d", f.old.freshCalls.Load(), len(f.fresh.sent()))
	}
	if f.e.reportRecoveryInputFailure(state, f.session, f.e.sessions, f.key, f.old, invocation, recoveryTestFailure{}) {
		t.Fatal("duplicate error from the replaced executor was accepted")
	}
	for _, correction := range []string{"first correction", "second correction"} {
		if !strings.Contains(f.fresh.sent()[0], correction) {
			t.Errorf("duplicate-signal handling lost %q", correction)
		}
	}
}

func TestContextRecoverySwarmSources_QueuedSenderIdentityIsRecoverable(t *testing.T) {
	f := newRecoveryFixture(t)
	f.e.injectSender = true
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	started, release := make(chan struct{}), make(chan struct{})
	f.old.onFresh = func(ctx context.Context) (AgentSession, error) {
		close(started)
		select {
		case <-release:
			return f.fresh, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	done := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "finish the shared-channel task") })
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		f.e.cancel()
		<-done
		t.Fatal("recovery preparation did not start")
	}

	update := f.message("later-scope", "only prepare a local report")
	update.UserID, update.UserName, update.ChannelKey = "scope-owner-73", "Scope owner", "shared-review-room"
	accepted := f.e.queueMessageForBusySession(f.p, update, f.key)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !accepted {
		t.Fatal("latest input was not accepted")
	}
	if !strings.Contains(f.e.buildSenderPrompt(update.Content, update.UserID, update.UserName, update.Platform, update.SessionKey, update.ChannelKey), update.UserID) {
		t.Fatal("fixture does not exercise configured sender identity injection")
	}
	r := f.session.GetContextRecovery()
	bundle, err := os.ReadFile(filepath.Join(r.ArtifactDir, "visible-material.json"))
	if err != nil {
		t.Fatal(err)
	}
	// This update never reached the old executor, so neither its native
	// transcript nor any earlier host history can recover the missing sender.
	for _, identity := range []string{update.UserID, update.UserName, update.ChannelKey} {
		if !strings.Contains(string(bundle), identity) {
			t.Errorf("accepted queued input lost sender identity %q from its complete recovery evidence", identity)
		}
	}
}

func TestContextRecoverySwarmSources_InvocationSenderContextPreservesOriginalInput(t *testing.T) {
	for _, source := range []string{"message", "queued"} {
		for _, tc := range []struct {
			name         string
			injectSender bool
			userID       string
			wantContext  string
		}{
			{name: "enabled", injectSender: true, userID: "scope-owner-73", wantContext: "[cc-connect sender_id=scope-owner-73 sender_name=\"Scope 'owner' review\" platform=test chat_id=shared-review-room]\n"},
			{name: "disabled", userID: "scope-owner-73"},
			{name: "missing-user-id", injectSender: true},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				e := &Engine{injectSender: tc.injectSender}
				msg := &Message{MessageID: "original-message", Content: "  原始输入\n保留结尾空白  ", UserID: tc.userID, UserName: "Scope \"owner\"\nreview", Platform: "test", SessionKey: "test:shared", ChannelKey: "shared-review-room"}
				originalHash := sha256.Sum256([]byte(msg.Content))
				var invocation *recoveryInvocation
				if source == "message" {
					invocation = e.recoveryInvocationForMessage(msg)
				} else {
					invocation = e.recoveryInvocationForQueued(queuedMessage{
						messageID: msg.MessageID, content: msg.Content, userID: msg.UserID, userName: msg.UserName,
						msgPlatform: msg.Platform, msgSessionKey: msg.SessionKey, channelKey: msg.ChannelKey,
					})
				}
				if invocation.SenderContext != tc.wantContext {
					t.Errorf("sender context=%q, want %q", invocation.SenderContext, tc.wantContext)
				}
				if invocation.Content != msg.Content || sha256.Sum256([]byte(invocation.Content)) != originalHash {
					t.Fatal("sender context changed the original content or its input digest")
				}
				encoded, err := json.Marshal(invocation)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(encoded, &fields); err != nil {
					t.Fatal(err)
				}
				if _, present := fields["sender_context"]; present != (tc.wantContext != "") {
					t.Errorf("serialized sender context present=%v, want %v", present, tc.wantContext != "")
				}
				var saved recoveryInvocation
				if err := json.Unmarshal(encoded, &saved); err != nil {
					t.Fatal(err)
				}
				if saved.Content != msg.Content || saved.SenderContext != tc.wantContext {
					t.Fatal("material serialization changed original content or sender context")
				}
			})
		}
	}
}

func TestContextRecoverySwarmSources_IPCAckHashAndDuplicateRemainOriginal(t *testing.T) {
	for _, steerErr := range []error{nil, errors.New("lost steer response")} {
		name := "accepted"
		if steerErr != nil {
			name = "unknown_then_acknowledged"
		}
		t.Run(name, func(t *testing.T) {
			f := newRecoveryFixture(t)
			api := &APIServer{engines: map[string]*Engine{"recovery": f.e}}
			if err := api.initSessionIPC(filepath.Join(f.a.dir, "ipc")); err != nil {
				t.Fatal(err)
			}
			input := IPCInput{EventID: strings.Repeat("9", 64), Prompt: "inspect the existing worker receipt; do not launch it again", Target: IPCTarget{Project: "recovery", SessionKey: f.key, SessionID: f.session.ID, AgentSessionID: f.old.id, CWD: f.a.dir}}
			encoded, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(encoded)
			wantHash := hex.EncodeToString(digest[:])
			f.old.onSend = func(int, string) error { return nil }
			f.old.steerErr = steerErr
			done := recoverySourcesRunTurn(t, f, func() error { return f.run("original", "finish current task") })
			recoveryAwait(t, "original input", func() bool { return len(f.old.sent()) == 1 })
			if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			if w := ipcPost(t, api.handleIPCAck, map[string]any{"event_id": input.EventID, "target": input.Target}); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			before := ipcReceipt(t, api, input.EventID)
			f.old.events <- Event{Type: EventError, Error: recoveryTestFailure{}}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			after := ipcReceipt(t, api, input.EventID)
			if after.Input != input || after.SHA256 != wantHash || after.AcknowledgedAt != before.AcknowledgedAt || after.Status != "delivered" {
				t.Fatalf("recovery rewrote original IPC evidence: before=%+v after=%+v", before, after)
			}
			if after.Recovery == nil || after.Recovery.FromSessionID != input.Target.AgentSessionID || after.Recovery.ToSessionID != f.fresh.id || after.Recovery.TurnStatus != "completed" {
				t.Fatalf("missing independent recovery receipt: %+v", after.Recovery)
			}
			if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			if len(f.fresh.sent()) != 1 {
				t.Fatal("duplicate original IPC event created another recovered input")
			}
			newTarget := input.Target
			newTarget.AgentSessionID = f.fresh.id
			if w := ipcPost(t, api.handleIPCAck, map[string]any{"event_id": input.EventID, "target": newTarget}); w.Code != 409 {
				t.Fatal("replacement native session was allowed to acknowledge the original target")
			}
		})
	}
}

type recoverySourcesAttachmentSession struct {
	*recoveryTestSession
	images []ImageAttachment
	files  []FileAttachment
}

func (s *recoverySourcesAttachmentSession) Send(prompt, id string, images []ImageAttachment, files []FileAttachment) error {
	s.images = append([]ImageAttachment(nil), images...)
	s.files = append([]FileAttachment(nil), files...)
	return s.recoveryTestSession.Send(prompt, id, images, files)
}

func TestContextRecoverySwarmSources_OriginalAndLatestAttachmentsArriveOnce(t *testing.T) {
	f := newRecoveryFixture(t)
	f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
	fresh := &recoverySourcesAttachmentSession{recoveryTestSession: f.fresh}
	started, release := make(chan struct{}), make(chan struct{})
	f.old.onFresh = func(ctx context.Context) (AgentSession, error) {
		close(started)
		select {
		case <-release:
			return fresh, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	original := f.message("original", "prepare the report from the attachment")
	original.Images = []ImageAttachment{{FileName: "original.png", MimeType: "image/png", Data: []byte("image-one")}}
	original.Files = []FileAttachment{{FileName: "original.txt", MimeType: "text/plain", Data: []byte("file-one")}}
	gen, ok := f.session.TryLock()
	if !ok {
		t.Fatal("session busy before original input")
	}
	done := recoverySourcesRunTurn(t, f, func() error { return f.e.processInteractiveMessage(f.p, original, f.session, gen) })
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		f.e.cancel()
		<-done
		t.Fatal("recovery did not start")
	}
	latest := f.message("latest", "cancel publishing; use this corrected attachment")
	latest.Images = []ImageAttachment{{FileName: "corrected.png", MimeType: "image/png", Data: []byte("image-two")}}
	latest.Files = []FileAttachment{{FileName: "corrected.txt", MimeType: "text/plain", Data: []byte("file-two")}}
	accepted := f.e.queueMessageForBusySession(f.p, latest, f.key)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !accepted || len(f.fresh.sent()) != 1 || !strings.Contains(f.fresh.sent()[0], latest.Content) {
		t.Fatal("latest correction was lost or delivered in an additional turn")
	}
	if len(fresh.images) != 2 || string(fresh.images[0].Data) != "image-one" || string(fresh.images[1].Data) != "image-two" {
		t.Fatalf("incorrect recovered image order or payload: %+v", fresh.images)
	}
	if len(fresh.files) != 2 || string(fresh.files[0].Data) != "file-one" || string(fresh.files[1].Data) != "file-two" {
		t.Fatalf("incorrect recovered file order or payload: %+v", fresh.files)
	}
	material := readRecoveryBundle(t, f.session)
	if len(material.LaterInputs) != 1 || material.LaterInputs[0].MessageID != latest.MessageID {
		t.Fatal("latest input missing from saved evidence")
	}
}
