package antigravity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

type agyProgressRecorder struct {
	mu       sync.Mutex
	progress []core.LiveInputProgress
}

func (r *agyProgressRecorder) report(progress core.LiveInputProgress) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress = append(r.progress, progress)
	return nil
}

func (r *agyProgressRecorder) snapshot() []core.LiveInputProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.LiveInputProgress(nil), r.progress...)
}

func testAgyRun(t *testing.T, sessionID string) *agyRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	run, err := newAgyRun(ctx, cancel, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { run.end(true); cancel() })
	if err := run.bind(sessionID); err != nil {
		t.Fatal(err)
	}
	if err := run.acceptInput(sessionID); err != nil {
		t.Fatal(err)
	}
	return run
}

func invocationInput(id string, invocation int) antigravityhook.InvocationInput {
	steps := invocation + 1
	return antigravityhook.InvocationInput{ConversationID: id, InvocationNum: &invocation, InitialSteps: &steps}
}

func TestAgyQueueClaimPreservesSenderDataAndDoesNotInventConsumption(t *testing.T) {
	for _, sender := range []string{"Hermes", "Atlas"} {
		t.Run(sender, func(t *testing.T) {
			run := testAgyRun(t, "main-conversation")
			recorder := &agyProgressRecorder{}
			id := strings.Repeat("a", 64)
			prompt := `{"sender":"` + sender + `","reply_to":"fixed-origin","text":"review result"}`
			if err := run.queue("main-conversation", id, prompt, recorder.report); err != nil {
				t.Fatal(err)
			}
			if err := run.queue("main-conversation", id, prompt, recorder.report); err != nil {
				t.Fatal(err)
			}
			if err := run.queue("main-conversation", id, "changed", recorder.report); err == nil {
				t.Fatal("conflicting event was accepted")
			}
			output, err := run.claim(invocationInput("main-conversation", 7), "post")
			if err != nil || len(output.InjectSteps) != 1 || output.InjectSteps[0].UserMessage != prompt || output.TerminationBehavior != "force_continue" {
				t.Fatalf("claim = %#v, %v", output, err)
			}
			duplicate, err := run.claim(invocationInput("main-conversation", 8), "pre")
			if err != nil || len(duplicate.InjectSteps) != 0 {
				t.Fatalf("input was claimed twice: %#v, %v", duplicate, err)
			}
			if err := run.acceptInput("main-conversation"); err != nil {
				t.Fatal(err)
			}
			run.end(false)
			progress := recorder.snapshot()
			if len(progress) != 3 || progress[0].Phase != core.LiveInputPhaseQueued || progress[1].Phase != core.LiveInputPhaseClaimed || progress[2].Phase != core.LiveInputPhaseUnknown {
				t.Fatalf("body-less input or hook handoff invented delivery: %+v", progress)
			}
			if !strings.Contains(progress[1].TurnID, ":post:7") || !strings.Contains(progress[1].TurnID, run.generation) {
				t.Fatalf("claim has no verifiable invocation boundary: %+v", progress[1])
			}
		})
	}
}

func TestAgyQueueRejectsUnboundWrongAndInactiveTargets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run, err := newAgyRun(ctx, cancel, "expected")
	if err != nil {
		t.Fatal(err)
	}
	report := func(core.LiveInputProgress) error { return nil }
	if err := run.queue("expected", strings.Repeat("a", 64), "text", report); !errors.Is(err, core.ErrNoActiveInput) {
		t.Fatalf("unbound queue error = %v", err)
	}
	if err := run.bind("wrong"); err == nil {
		t.Fatal("resume accepted a different conversation")
	}
	if err := run.bind("expected"); err != nil {
		t.Fatal(err)
	}
	if err := run.acceptInput("expected"); err != nil {
		t.Fatal(err)
	}
	if err := run.queue("wrong", strings.Repeat("a", 64), "text", report); err == nil {
		t.Fatal("wrong target was accepted")
	}
	for _, input := range []struct{ id, prompt string }{{"../event", "text"}, {strings.Repeat("a", 64), " "}, {strings.Repeat("a", 64), strings.Repeat("x", 8193)}} {
		if err := run.queue("expected", input.id, input.prompt, report); err == nil {
			t.Fatal("invalid input was accepted")
		}
	}
	run.end(false)
	if err := run.queue("expected", strings.Repeat("a", 64), "text", report); !errors.Is(err, core.ErrNoActiveInput) {
		t.Fatalf("ended queue error = %v", err)
	}
}

func TestAgyQueuedInputIsReleasedOnlyBeforeClaim(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		run := testAgyRun(t, "main")
		recorder := &agyProgressRecorder{}
		if err := run.queue("main", strings.Repeat("b", 64), "text", recorder.report); err != nil {
			t.Fatal(err)
		}
		run.end(stopped)
		output, err := run.claim(invocationInput("main", 0), "post")
		if err != nil || len(output.InjectSteps) != 0 {
			t.Fatalf("released input escaped: %+v, %v", output, err)
		}
		progress := recorder.snapshot()
		if len(progress) != 2 || progress[1].Phase != core.LiveInputPhaseReleased || (progress[1].Code == core.LiveInputCodeStopped) != stopped {
			t.Fatalf("release reports = %+v", progress)
		}
	}
}

func TestAgyFailedReceiptNeverExposesInput(t *testing.T) {
	for _, failedPhase := range []string{core.LiveInputPhaseQueued, core.LiveInputPhaseClaimed} {
		t.Run(failedPhase, func(t *testing.T) {
			run := testAgyRun(t, "main")
			report := func(progress core.LiveInputProgress) error {
				if progress.Phase == failedPhase {
					return errors.New("disk failure")
				}
				return nil
			}
			err := run.queue("main", strings.Repeat("c", 64), "never expose", report)
			if (err != nil) != (failedPhase == core.LiveInputPhaseQueued) {
				t.Fatalf("queue error = %v", err)
			}
			for i := 0; i < 2; i++ {
				output, _ := run.claim(invocationInput("main", i), "pre")
				if len(output.InjectSteps) != 0 {
					t.Fatalf("input exposed after failed %s receipt: %+v", failedPhase, output)
				}
			}
		})
	}
}

func TestAgyStopDuringReceiptDoesNotDeadlockOrInject(t *testing.T) {
	for _, stopPhase := range []string{core.LiveInputPhaseQueued, core.LiveInputPhaseClaimed} {
		t.Run(stopPhase, func(t *testing.T) {
			run := testAgyRun(t, "main")
			recorder := &agyProgressRecorder{}
			report := func(progress core.LiveInputProgress) error {
				if progress.Phase == stopPhase {
					run.end(true)
				}
				return recorder.report(progress)
			}
			if err := run.queue("main", strings.Repeat("d", 64), "text", report); err != nil {
				t.Fatal(err)
			}
			output, err := run.claim(invocationInput("main", 0), "post")
			if err != nil || len(output.InjectSteps) != 0 {
				t.Fatalf("input escaped stop: %+v, %v", output, err)
			}
			progress := recorder.snapshot()
			last := progress[len(progress)-1]
			if stopPhase == core.LiveInputPhaseQueued && (last.Phase != core.LiveInputPhaseReleased || last.Code != core.LiveInputCodeStopped) {
				t.Fatalf("queued stop = %+v", progress)
			}
			if stopPhase == core.LiveInputPhaseClaimed && last.Phase != core.LiveInputPhaseUnknown {
				t.Fatalf("claimed stop = %+v", progress)
			}
		})
	}
}

func TestAgyConcurrentHooksHaveOnlyOneClaimant(t *testing.T) {
	run := testAgyRun(t, "main")
	recorder := &agyProgressRecorder{}
	if err := run.queue("main", strings.Repeat("e", 64), "text", recorder.report); err != nil {
		t.Fatal(err)
	}
	outputs := make(chan antigravityhook.InvocationOutput, 20)
	var wg sync.WaitGroup
	for i := 0; i < cap(outputs); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			output, _ := run.claim(invocationInput("main", i), "post")
			outputs <- output
		}(i)
	}
	wg.Wait()
	close(outputs)
	count := 0
	for output := range outputs {
		count += len(output.InjectSteps)
	}
	if count != 1 || len(recorder.snapshot()) != 2 {
		t.Fatalf("claim count = %d, reports = %+v", count, recorder.snapshot())
	}
}

func TestAgyInvocationBridgeRejectsOldRunAndSubagent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	bridge, err := newAgyInvocationBridge(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	oldRun := testAgyRun(t, "main")
	if err := bridge.setRun(oldRun); err != nil {
		t.Fatal(err)
	}
	oldRun.end(false)
	run := testAgyRun(t, "main")
	if err := bridge.setRun(run); err != nil {
		t.Fatal(err)
	}
	recorder := &agyProgressRecorder{}
	if err := run.queue("main", strings.Repeat("f", 64), "intended for main", recorder.report); err != nil {
		t.Fatal(err)
	}
	relay := func(identity *agyRun, conversation string) (antigravityhook.InvocationOutput, error) {
		input, _ := json.Marshal(invocationInput(conversation, 3))
		var output bytes.Buffer
		err := antigravityhook.RelayInvocation(bytes.NewReader(input), &output, bridge.listener.Addr().String(), identity.token, identity.generation, "post")
		var decoded antigravityhook.InvocationOutput
		if err == nil {
			err = json.Unmarshal(output.Bytes(), &decoded)
		}
		return decoded, err
	}
	if _, err := relay(oldRun, "main"); err == nil {
		t.Fatal("old process identity was accepted")
	}
	if output, err := relay(run, "subagent"); err != nil || len(output.InjectSteps) != 0 {
		t.Fatalf("subagent stole input: %+v, %v", output, err)
	}
	if output, err := relay(run, "main"); err != nil || len(output.InjectSteps) != 1 {
		t.Fatalf("main failed to claim: %+v, %v", output, err)
	}
}

func TestInvocationHooksExistInEveryModeAndPreserveUserHooks(t *testing.T) {
	for _, mode := range []string{"default", "plan", "yolo"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			configDir := filepath.Join(home, ".gemini", "config")
			if err := os.MkdirAll(configDir, 0o700); err != nil {
				t.Fatal(err)
			}
			original := []byte(`{"user-hook":{"PreInvocation":[{"command":"user-pre"}],"PostInvocation":[{"command":"user-post"}]}}`)
			path := filepath.Join(configDir, "hooks.json")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			session, err := newAntigravitySession(context.Background(), "unused", nil, t.TempDir(), "", mode, "", nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			data, err := os.ReadFile(filepath.Join(session.invocationBridge.configDir, "config", "hooks.json"))
			if err != nil {
				t.Fatal(err)
			}
			var hooks map[string]json.RawMessage
			if json.Unmarshal(data, &hooks) != nil || hooks["user-hook"] == nil || hooks[agyInvocationHookName] == nil || (hooks[agyPermissionHookName] != nil) != (mode == "default") {
				t.Fatalf("unexpected overlay hooks: %s", data)
			}
			unchanged, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(unchanged, original) {
				t.Fatal("user hooks were changed")
			}
		})
	}
}

func TestInvocationOverlayRejectsReservedNameInsteadOfReplacingUserHook(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configDir := filepath.Join(home, ".gemini", "config")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(configDir, "hooks.json")
	original := []byte(`{"cc-connect-invocation-bridge":{"PreInvocation":[{"command":"user-owned"}]}}`)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newAgyInvocationBridge(context.Background(), false); err == nil {
		t.Fatal("reserved name silently replaced a user hook")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, original) {
		t.Fatal("original hook was modified")
	}
}
