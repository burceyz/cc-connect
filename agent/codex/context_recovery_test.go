package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const encryptedFailureBody = `{"error":{"message":"rejected context","type":"invalid_request_error","param":"","code":"invalid_encrypted_content"}}`

func recoveryError(t *testing.T, err error, want bool) {
	t.Helper()
	var typed core.ContextRecoveryError
	if got := errors.As(err, &typed); got != want {
		t.Fatalf("recovery classification = %v, want %v: %v", got, want, err)
	}
	if want && typed.RecoveryReason() != core.RecoveryInvalidEncryptedContent {
		t.Fatalf("recovery reason = %q", typed.RecoveryReason())
	}
}

func TestContextRecovery_ExactStructuredClassification(t *testing.T) {
	for _, tc := range []struct {
		name, message, data string
		want                bool
	}{
		{"message envelope", encryptedFailureBody, "", true},
		{"RPC data code", "request failed", `{"code":"invalid_encrypted_content"}`, true},
		{"RPC data envelope", "request failed", encryptedFailureBody, true},
		{"different 400", `{"error":{"code":"invalid_request_error","message":"bad request"}}`, "", false},
		{"plain mention", "The text mentions invalid_encrypted_content", "", false},
		{"embedded quoted body", "unrelated failure: " + encryptedFailureBody, "", false},
		{"assistant content field", "failed", `{"content":"invalid_encrypted_content"}`, false},
		{"wrong nested field", "failed", `{"result":{"code":"invalid_encrypted_content"}}`, false},
		{"code prefix", `{"error":{"code":"invalid_encrypted_content_extra"}}`, "", false},
		{"numeric code", "400", `{"code":400}`, false},
		{"malformed JSON", `{"error":{"code":"invalid_encrypted_content"}`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyContextRecoveryError(tc.message, json.RawMessage(tc.data))
			recoveryError(t, fmt.Errorf("wrapped: %w", err), tc.want)
		})
	}
}

func TestContextRecovery_TerminalNotificationsOnly(t *testing.T) {
	for _, tc := range []struct {
		name, method, status, thread, turn string
		failure                            any
		retry                              *bool
		disabled, typed, noEvent           bool
	}{
		{name: "terminal turn", method: "turn/completed", status: "failed", typed: true},
		{name: "direct error code", method: "turn/completed", status: "failed", failure: map[string]any{"message": "failure", "code": "invalid_encrypted_content"}, typed: true},
		{name: "additional details", method: "turn/completed", status: "failed", failure: map[string]any{"message": "failure", "additionalDetails": encryptedFailureBody}, typed: true},
		{name: "terminal error event", method: "error", retry: boolPtr(false), typed: true},
		{name: "backend retrying", method: "error", retry: boolPtr(true), noEvent: true},
		{name: "unscoped error event", method: "error", noEvent: true},
		{name: "different failed turn", method: "turn/completed", status: "failed", turn: "previous-turn", noEvent: true},
		{name: "child error", method: "turn/completed", status: "failed", thread: "child", noEvent: true},
		{name: "not failed status", method: "turn/completed", status: "completed"},
		{name: "other failure", method: "turn/completed", status: "failed", failure: map[string]any{"message": "service unavailable"}},
		{name: "disabled by default", method: "turn/completed", status: "failed", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &appServerSession{
				appServerSessionOptions: appServerSessionOptions{contextRecovery: !tc.disabled},
				events:                  make(chan core.Event, 8), currentTurn: "turn-1",
			}
			s.threadID.Store("thread-1")
			thread, turn := tc.thread, tc.turn
			if thread == "" {
				thread = "thread-1"
			}
			if turn == "" {
				turn = "turn-1"
			}
			failure := tc.failure
			if failure == nil {
				failure = map[string]any{"message": encryptedFailureBody}
			}
			params := map[string]any{"threadId": thread}
			if tc.method == "turn/completed" {
				params["turn"] = map[string]any{"id": turn, "status": tc.status, "error": failure}
			} else {
				params["turnId"], params["error"] = turn, failure
				if tc.retry != nil {
					params["willRetry"] = *tc.retry
				}
			}
			raw, _ := json.Marshal(params)
			s.handleNotification(tc.method, raw)
			if tc.noEvent {
				if len(s.events) != 0 || s.currentTurn != "turn-1" {
					t.Fatal("non-terminal or foreign error ended the active turn")
				}
				return
			}
			select {
			case event := <-s.events:
				if event.Type != core.EventError {
					t.Fatalf("event = %#v", event)
				}
				recoveryError(t, event.Error, tc.typed)
			case <-time.After(time.Second):
				t.Fatal("terminal failure produced no event")
			}
			s.handleNotification("thread/status/changed", json.RawMessage(`{"threadId":"thread-1","status":{"type":"idle"}}`))
			if len(s.events) != 0 {
				t.Fatal("failed turn later emitted a successful completion")
			}
		})
	}
}

func boolPtr(value bool) *bool { return &value }

func TestContextRecovery_AssistantQuotedErrorIsOrdinaryOutput(t *testing.T) {
	s := &appServerSession{
		appServerSessionOptions: appServerSessionOptions{contextRecovery: true},
		events:                  make(chan core.Event, 4), currentTurn: "turn-1",
	}
	s.threadID.Store("thread-1")
	raw, _ := json.Marshal(map[string]any{"threadId": "thread-1", "turnId": "turn-1",
		"item": map[string]any{"type": "agentMessage", "text": encryptedFailureBody}})
	s.handleNotification("item/completed", raw)
	s.handleNotification("turn/completed", json.RawMessage(`{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`))
	for len(s.events) > 0 {
		if event := <-s.events; event.Type == core.EventError {
			t.Fatalf("assistant text treated as protocol error: %#v", event)
		}
	}
}

type recoveryRPCWriter struct {
	session *appServerSession
	failure *rpcError
	before  func()
}

func (w *recoveryRPCWriter) Close() error { return nil }
func (w *recoveryRPCWriter) Write(b []byte) (int, error) {
	var req struct{ ID any }
	if err := json.Unmarshal(b, &req); err != nil {
		return 0, err
	}
	if w.before != nil {
		w.before()
	}
	w.session.handleResponse(rpcResponseEnvelope{ID: req.ID, Error: w.failure, Result: json.RawMessage(`{"turn":{"id":"turn-1"}}`)})
	return len(b), nil
}

func TestContextRecovery_RPCErrorClassificationIsMethodAndOptionScoped(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, method := range []string{"turn/start", "thread/resume", "thread/start", "turn/steer", "initialize"} {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, method), func(t *testing.T) {
				s := &appServerSession{appServerSessionOptions: appServerSessionOptions{contextRecovery: enabled}, ctx: context.Background()}
				s.stdin = &recoveryRPCWriter{session: s, failure: &rpcError{Code: -32000, Message: encryptedFailureBody}}
				err := s.request(method, nil, nil)
				recoveryError(t, err, enabled && (method == "turn/start" || method == "thread/resume"))
			})
		}
	}
}

func TestContextRecovery_SendRetainsTypedErrorAndFastTerminalState(t *testing.T) {
	s := &appServerSession{
		appServerSessionOptions: appServerSessionOptions{contextRecovery: true},
		ctx:                     context.Background(), events: make(chan core.Event, 2),
	}
	s.threadID.Store("thread-1")
	s.alive.Store(true)
	w := &recoveryRPCWriter{session: s, failure: &rpcError{Code: -32000, Message: encryptedFailureBody}}
	s.stdin = w
	err := s.Send("original input", "message-1", nil, nil)
	recoveryError(t, err, true)

	w.failure = nil
	w.before = func() {
		s.handleNotification("turn/started", json.RawMessage(`{"threadId":"thread-1","turn":{"id":"turn-1"}}`))
		raw, _ := json.Marshal(map[string]any{"threadId": "thread-1", "turn": map[string]any{
			"id": "turn-1", "status": "failed", "error": map[string]any{"message": encryptedFailureBody},
		}})
		s.handleNotification("turn/completed", raw)
	}
	if err := s.Send("a separate test input", "message-2", nil, nil); err != nil {
		t.Fatal(err)
	}
	s.stateMu.Lock()
	current := s.currentTurn
	s.stateMu.Unlock()
	if current != "" {
		t.Fatal("late turn/start RPC response revived failed turn")
	}
	recoveryError(t, (<-s.events).Error, true)
}

type recoveryFailingCloser struct{ err error }

type recoveryBlockedCloser struct{ release chan struct{} }

func (w recoveryBlockedCloser) Write(b []byte) (int, error) { return len(b), nil }
func (w recoveryBlockedCloser) Close() error {
	<-w.release
	return nil
}

func (w recoveryFailingCloser) Write(b []byte) (int, error) { return len(b), nil }
func (w recoveryFailingCloser) Close() error                { return w.err }

func TestContextRecovery_ShutdownMustBeConfirmed(t *testing.T) {
	for _, name := range []string{"kill failure", "close failure", "blocked close", "wait failure", "still running", "reader still running", "already cancelled", "confirmed"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			s := &appServerSession{
				appServerSessionOptions: appServerSessionOptions{contextRecovery: true},
				events:                  make(chan core.Event, 1), processDone: make(chan struct{}),
			}
			close(s.processDone)
			s.processExitConfirmed = true
			s.killProcess = func(*exec.Cmd) error { return nil }
			switch name {
			case "kill failure":
				s.killProcess = func(*exec.Cmd) error { return errors.New("signal denied") }
			case "close failure":
				s.stdin = recoveryFailingCloser{errors.New("close denied")}
			case "blocked close":
				release := make(chan struct{})
				s.stdin = recoveryBlockedCloser{release}
				defer close(release)
			case "wait failure":
				s.processExitConfirmed = false
				s.processWaitErr = errors.New("wait failed")
			case "still running":
				s.processDone = make(chan struct{})
				s.alive.Store(false) // not evidence of actual termination
			case "reader still running":
				s.wg.Add(1)
				defer s.wg.Done()
			case "already cancelled":
				cancel()
			}
			err := s.CloseForRecovery(ctx)
			if name == "confirmed" {
				if err != nil || !s.recoveryClosed.Load() {
					t.Fatalf("confirmed shutdown failed: %v", err)
				}
				return
			}
			if err == nil || s.recoveryClosed.Load() {
				t.Fatal("unconfirmed shutdown permitted recovery")
			}
			if _, err := s.StartFreshForRecovery(context.Background()); err == nil {
				t.Fatal("unconfirmed executor provided a fresh context")
			}
		})
	}
}

func TestContextRecovery_CloseReportsKillFailure(t *testing.T) {
	want := errors.New("kill failed")
	s := &appServerSession{events: make(chan core.Event), killProcess: func(*exec.Cmd) error { return want }}
	if err := s.Close(); !errors.Is(err, want) {
		t.Fatalf("Close error = %v, want kill failure", err)
	}
}

func TestContextRecovery_CloseSignalsGroupBeforeCancellingProcessContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cancelledBeforeSignal bool
	s := &appServerSession{
		ctx: ctx, cancel: cancel, events: make(chan core.Event),
		killProcess: func(*exec.Cmd) error {
			cancelledBeforeSignal = ctx.Err() != nil
			return nil
		},
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if cancelledBeforeSignal || ctx.Err() == nil {
		t.Fatal("process group must be signalled before CommandContext cancellation")
	}
}

type recoveryProcessRecord struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params,omitempty"`
	Args   []string       `json:"args,omitempty"`
	CWD    string         `json:"cwd,omitempty"`
	Marker string         `json:"marker,omitempty"`
	Env    string         `json:"env,omitempty"`
	PID    int            `json:"pid,omitempty"`
}

func recoveryTestAgent(t *testing.T, enabled bool, flags map[string]string) (*Agent, string, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir, runtimeDir := t.TempDir(), t.TempDir()
	logPath := filepath.Join(dir, "requests.jsonl")
	t.Setenv("PATH", dir) // accidental invocation of a real codex must fail
	t.Setenv("CC_RECOVERY_INHERITED", "original inherited value")
	env := map[string]string{
		"CC_RECOVERY_HELPER": "1", "CC_RECOVERY_LOG": logPath,
		"CC_RECOVERY_RUNTIME_DIR": runtimeDir, "CC_RECOVERY_MARKER": "original marker",
	}
	for key, value := range flags {
		env[key] = value
	}
	a, err := New(map[string]any{
		"backend": "app_server", "work_dir": dir, "model": "configured-default",
		"reasoning_effort": "medium", "mode": "suggest", "context_recovery": enabled,
		"system_prompt": "Keep original instructions", "append_system_prompt": "Additional instruction",
		"cmd": []string{executable, "-test.run=^TestContextRecoveryProcessHelper$", "--"},
		"env": env, "codex_home": filepath.Join(dir, "mock-codex-home"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return a.(*Agent), logPath, runtimeDir
}

func readRecoveryProcessRecords(t *testing.T, path string) []recoveryProcessRecord {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatal(err)
	}
	var records []recoveryProcessRecord
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var record recoveryProcessRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func TestContextRecovery_FreshContextPreservesActualRuntimeAndDoesNotSend(t *testing.T) {
	a, logPath, runtimeDir := recoveryTestAgent(t, true, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	original, err := a.StartSession(ctx, "original-thread")
	if err != nil {
		t.Fatal(err)
	}
	old := original.(*appServerSession)
	defer old.Close()
	if _, err := old.StartFreshForRecovery(ctx); err == nil {
		t.Fatal("fresh start permitted before shutdown")
	}
	a.SetModel("changed-shared-default")
	a.SetReasoningEffort("low")
	a.SetMode("yolo")
	a.SetSessionEnv([]string{"CC_RECOVERY_MARKER=changed marker"})
	// A recovery accidentally routed through Agent.StartSession would rewrite
	// these shared files with the changed provider below. Only fixture data is
	// used; the fake app-server never reads credentials or makes a model call.
	if err := os.MkdirAll(old.codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinels := map[string]string{
		"config.toml": "model = \"original-test-model\"\n",
		"auth.json":   `{"auth_mode":"apikey","OPENAI_API_KEY":"fixture-only"}`,
	}
	for name, content := range sentinels {
		if err := os.WriteFile(filepath.Join(old.codexHome, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	a.mu.Lock()
	a.providers = []core.ProviderConfig{{Name: "changed-test-provider", APIKey: "fixture-changed", BaseURL: "https://unused.invalid", Model: "changed-provider-model"}}
	a.activeIdx = 0
	a.mu.Unlock()
	t.Setenv("CC_RECOVERY_INHERITED", "changed inherited value")
	if err := old.CloseForRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	freshSession, err := old.StartFreshForRecovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fresh := freshSession.(*appServerSession)
	defer fresh.Close()
	if fresh.GetModel() != "actual-runtime-model" || fresh.GetReasoningEffort() != "high" || fresh.GetWorkDir() != runtimeDir {
		t.Fatalf("changed actual runtime: model=%q effort=%q cwd=%q", fresh.GetModel(), fresh.GetReasoningEffort(), fresh.GetWorkDir())
	}
	if fresh.CurrentSessionID() != "fresh-thread" || fresh.mode != "suggest" || fresh.promptPreamble != old.promptPreamble || fresh.codexHome != old.codexHome {
		t.Fatal("fresh context lost identity, permissions, preamble, or home settings")
	}
	var starts, resumes, turns int
	for _, record := range readRecoveryProcessRecords(t, logPath) {
		switch record.Method {
		case "thread/start":
			starts++
		case "thread/resume":
			resumes++
		case "turn/start":
			turns++
		case "process":
			if record.Marker != "original marker" || record.Env != "original inherited value" {
				t.Fatal("recovery used changed shared or inherited environment")
			}
		}
	}
	if starts != 1 || resumes != 1 || turns != 0 {
		t.Fatalf("adapter replayed input or resumed history: starts=%d resumes=%d turns=%d", starts, resumes, turns)
	}
	for name, want := range sentinels {
		got, err := os.ReadFile(filepath.Join(old.codexHome, name))
		if err != nil || string(got) != want {
			t.Fatalf("recovery modified shared fixture %s", name)
		}
	}
	if _, err := old.StartFreshForRecovery(ctx); err == nil {
		t.Fatal("same starter allowed another fresh attempt")
	}
}

func TestContextRecovery_StartupErrorExposesStarterOnlyWhenEnabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			a, logPath, _ := recoveryTestAgent(t, enabled, map[string]string{"CC_RECOVERY_FAIL_RESUME": "1"})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := a.StartSession(ctx, "original-thread")
			recoveryError(t, err, enabled)
			var starter core.ContextRecoveryStarter
			if errors.As(fmt.Errorf("outer: %w", err), &starter) != enabled {
				t.Fatalf("startup starter enabled=%t: %v", enabled, err)
			}
			for _, record := range readRecoveryProcessRecords(t, logPath) {
				if record.Method == "thread/start" || record.Method == "turn/start" {
					t.Fatal("startup error performed automatic recovery")
				}
			}
			if enabled {
				old := starter.(*contextRecoveryStartupError).session
				if !old.recoveryClosed.Load() || old.Alive() || !old.processExitConfirmed {
					t.Fatal("starter exposed before original executor was reaped")
				}
				fresh, err := starter.StartFreshForRecovery(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := fresh.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestContextRecovery_StartupCloseFailureHasNoStarter(t *testing.T) {
	s := &appServerSession{
		appServerSessionOptions: appServerSessionOptions{contextRecovery: true},
		events:                  make(chan core.Event), killProcess: func(*exec.Cmd) error { return errors.New("kill denied") },
	}
	err := s.closeFailedStartup(classifyContextRecoveryError(encryptedFailureBody), "original-thread")
	recoveryError(t, err, true)
	var starter core.ContextRecoveryStarter
	if errors.As(err, &starter) {
		t.Fatal("failed shutdown exposed recovery starter")
	}
}

func TestContextRecovery_FreshStartCanBeCancelled(t *testing.T) {
	a, logPath, _ := recoveryTestAgent(t, true, map[string]string{"CC_RECOVERY_BLOCK_START": "1"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	original, err := a.StartSession(ctx, "original-thread")
	if err != nil {
		t.Fatal(err)
	}
	old := original.(*appServerSession)
	defer old.Close()
	if err := old.CloseForRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	startCtx, stopStart := context.WithCancel(ctx)
	defer stopStart()
	result := make(chan error, 1)
	go func() {
		_, err := old.StartFreshForRecovery(startCtx)
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	started := false
	for !started && time.Now().Before(deadline) {
		for _, record := range readRecoveryProcessRecords(t, logPath) {
			started = started || record.Method == "thread/start"
		}
		if !started {
			time.Sleep(time.Millisecond)
		}
	}
	if !started {
		t.Fatal("fresh startup never reached blocked thread/start")
	}
	stopStart()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled startup error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh startup ignored cancellation")
	}
}

func TestContextRecovery_FreshRuntimeMismatchStopsBeforeInput(t *testing.T) {
	a, logPath, _ := recoveryTestAgent(t, true, map[string]string{"CC_RECOVERY_RUNTIME_MISMATCH": "1"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	original, err := a.StartSession(ctx, "original-thread")
	if err != nil {
		t.Fatal(err)
	}
	old := original.(*appServerSession)
	defer old.Close()
	if err := old.CloseForRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := old.StartFreshForRecovery(ctx); err == nil || !strings.Contains(err.Error(), "changed execution settings") {
		t.Fatalf("runtime mismatch error = %v", err)
	}
	for _, record := range readRecoveryProcessRecords(t, logPath) {
		if record.Method == "turn/start" {
			t.Fatal("recovery input sent under changed settings")
		}
	}
}

func TestContextRecovery_OptionDefaultAndWorkspaceClone(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		a, _, _ := recoveryTestAgent(t, enabled, nil)
		if a.contextRecovery != enabled || a.WorkspaceAgentOptions()["context_recovery"] != enabled {
			t.Fatal("workspace clone lost opt-in setting")
		}
	}
	executable, _ := os.Executable()
	a, err := New(map[string]any{"cmd": executable})
	if err != nil || a.(*Agent).contextRecovery {
		t.Fatal("context recovery must default to disabled")
	}
	if _, err := New(map[string]any{"cmd": executable, "context_recovery": true, "backend": "exec"}); err == nil {
		t.Fatal("unsupported backend silently accepted context recovery")
	}
}

// This subprocess is the only endpoint used by the integration fixtures.
// It speaks just enough local JSONL to exercise process, RPC, and runtime state.
func TestContextRecoveryProcessHelper(t *testing.T) {
	if os.Getenv("CC_RECOVERY_HELPER") != "1" {
		return
	}
	log, err := os.OpenFile(os.Getenv("CC_RECOVERY_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(2)
	}
	defer log.Close()
	writeRecord := func(record recoveryProcessRecord) {
		b, _ := json.Marshal(record)
		if _, err := log.Write(append(b, '\n')); err != nil {
			os.Exit(3)
		}
	}
	cwd, _ := os.Getwd()
	writeRecord(recoveryProcessRecord{Method: "process", Args: os.Args, CWD: cwd, PID: os.Getpid(),
		Marker: os.Getenv("CC_RECOVERY_MARKER"), Env: os.Getenv("CC_RECOVERY_INHERITED")})
	settings := map[string]string{}
	for i, arg := range os.Args {
		if arg == "-c" && i+1 < len(os.Args) {
			key, value, found := strings.Cut(os.Args[i+1], "=")
			if found {
				settings[key], _ = strconv.Unquote(value)
			}
		}
	}
	scanner, encoder := bufio.NewScanner(os.Stdin), json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request struct {
			ID     *int64         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			os.Exit(4)
		}
		if request.ID == nil {
			continue
		}
		writeRecord(recoveryProcessRecord{Method: request.Method, Params: request.Params})
		response := map[string]any{"id": request.ID}
		result := map[string]any{}
		switch request.Method {
		case "initialize", "account/rateLimits/read":
		case "thread/resume":
			if os.Getenv("CC_RECOVERY_FAIL_RESUME") == "1" {
				response["error"] = map[string]any{"code": -32000, "message": encryptedFailureBody}
			} else {
				result = map[string]any{"thread": map[string]any{"id": request.Params["threadId"]},
					"cwd": os.Getenv("CC_RECOVERY_RUNTIME_DIR"), "model": "actual-runtime-model", "reasoningEffort": "high"}
			}
		case "thread/start":
			if os.Getenv("CC_RECOVERY_BLOCK_START") == "1" {
				_, _ = io.Copy(io.Discard, os.Stdin)
				os.Exit(0)
			}
			result = map[string]any{"thread": map[string]any{"id": "fresh-thread"}, "cwd": cwd,
				"model": settings["model"], "reasoningEffort": settings["model_reasoning_effort"]}
			if os.Getenv("CC_RECOVERY_RUNTIME_MISMATCH") == "1" {
				result["model"] = "changed-model"
			}
		case "turn/start":
			result["turn"] = map[string]any{"id": "fresh-turn"}
		default:
			os.Exit(5)
		}
		if response["error"] == nil {
			response["result"] = result
		}
		if encoder.Encode(response) != nil {
			os.Exit(6)
		}
	}
	os.Exit(0)
}
