package antigravity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// This fake review executable validates transport and failure handling only. It
// does not stand in for Hermes, source review, or the real gate's evidence tests.
func TestAgyReviewHelper(t *testing.T) {
	if os.Getenv("GO_WANT_AGY_REVIEW_HELPER") != "1" {
		return
	}
	dir := os.Getenv("CC_TEST_AGY_DIR")
	_ = os.WriteFile(filepath.Join(dir, "review-origin-env.json"), mustJSON(map[string]string{
		"origin": os.Getenv("CC_IPC_ORIGIN"), "project": os.Getenv("CC_PROJECT"),
		"session_key": os.Getenv("CC_SESSION_KEY"), "data_dir": os.Getenv("CC_DATA_DIR"),
	}), 0o600)
	modeData, _ := os.ReadFile(filepath.Join(dir, "review-mode"))
	mode := string(modeData)
	if mode == "timeout" {
		helperWaitFile(filepath.Join(dir, "never-release-gate"))
	}
	var request struct {
		Protocol        string          `json:"protocol"`
		Op              string          `json:"op"`
		TaskID          string          `json:"task_id"`
		Origin          agyReviewOrigin `json:"origin"`
		RequestText     string          `json:"request_text"`
		RequestSHA256   string          `json:"request_sha256"`
		Continuation    bool            `json:"continuation"`
		EventID         string          `json:"event_id"`
		ConversationID  string          `json:"conversation_id"`
		CandidateText   string          `json:"candidate_text"`
		CandidateSHA256 string          `json:"candidate_sha256"`
		ManifestPath    string          `json:"manifest_path"`
	}
	if json.NewDecoder(os.Stdin).Decode(&request) != nil || request.Protocol != agyReviewProtocol {
		os.Exit(2)
	}
	if request.Op != "begin" && request.Op != "check" && request.Op != "bind" && request.Op != "refresh" {
		os.Exit(2)
	}
	_ = os.WriteFile(filepath.Join(dir, "review-"+request.Op+".json"), mustJSON(request), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "review-args.json"), mustJSON(os.Args), 0o600)
	response := map[string]any{"protocol": agyReviewProtocol, "task_id": request.TaskID}
	if request.Op == "bind" || request.Op == "refresh" {
		taskDir := filepath.Join(dir, "tasks", request.TaskID)
		response["task_dir"], response["manifest_path"] = taskDir, filepath.Join(taskDir, "manifest.json")
		response["instructions"] = "FIXED REVIEW INSTRUCTIONS"
		response["status"], response["allow_publish"] = "pending_review", false
	} else if request.Op == "begin" {
		if textSHA256(request.RequestText) != request.RequestSHA256 || (request.EventID != "") != request.Continuation {
			os.Exit(2)
		}
		if request.Continuation && mode == "same-task" {
			data, _ := os.ReadFile(filepath.Join(dir, "active-review-task"))
			response["task_id"] = string(data)
		} else if !request.Continuation {
			_ = os.WriteFile(filepath.Join(dir, "active-review-task"), []byte(request.TaskID), 0o600)
		}
		if mode == "wrong-begin-task" {
			response["task_id"] = "different-task"
		}
		taskDir := filepath.Join(dir, "tasks", response["task_id"].(string))
		_ = os.MkdirAll(taskDir, 0o700)
		_ = os.WriteFile(filepath.Join(taskDir, "manifest.json"), []byte("{\"fixed\":true}\n"), 0o600)
		response["task_dir"], response["manifest_path"] = taskDir, filepath.Join(taskDir, "manifest.json")
		response["instructions"] = "FIXED REVIEW INSTRUCTIONS"
		response["status"], response["allow_publish"] = "pending_review", false
		if mode == "unknown-begin-status" {
			response["status"] = "unexpected"
		}
		if mode == "blocked-begin" {
			response["status"] = "blocked"
		}
		if mode == "missing-begin-allow" {
			delete(response, "allow_publish")
		}
		if mode == "busy-begin" || mode == "bad-pending-begin" {
			delete(response, "task_dir")
			delete(response, "manifest_path")
			response["reasons"] = []string{"review_gate_busy"}
			if mode == "bad-pending-begin" {
				response["reasons"] = []string{"other_pending_reason"}
			}
		}
	} else {
		if textSHA256(request.CandidateText) != request.CandidateSHA256 {
			os.Exit(2)
		}
		manifest, _ := os.ReadFile(request.ManifestPath)
		response["candidate_sha256"], response["manifest_sha256"] = request.CandidateSHA256, textSHA256(string(manifest))
		response["status"], response["allow_publish"] = "verified", true
		switch mode {
		case "pending":
			response["status"], response["allow_publish"] = "pending_review", false
		case "manifest-missing":
			response["status"], response["allow_publish"] = "pending_review", false
			response["reasons"] = []string{"manifest_missing"}
		case "blocked":
			response["status"], response["allow_publish"] = "blocked", false
		case "wrong-hash":
			response["candidate_sha256"] = strings.Repeat("0", 64)
		case "wrong-manifest":
			response["manifest_sha256"] = strings.Repeat("0", 64)
		case "changed-manifest":
			_ = os.WriteFile(request.ManifestPath, []byte("changed during gate\n"), 0o600)
		case "wrong-task":
			response["task_id"] = "another-task"
		case "wrong-protocol":
			response["protocol"] = "other-protocol"
		case "denied-verified":
			response["allow_publish"] = false
		case "unknown-status":
			response["status"] = "unknown"
		case "invalid-json":
			_, _ = io.WriteString(os.Stdout, "not json")
			os.Exit(0)
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(response)
	if mode == "extra-json" {
		_ = json.NewEncoder(os.Stdout).Encode(response)
	}
	if mode == "exit-error" {
		_, _ = io.WriteString(os.Stderr, "PRIVATE CANDIDATE SHOULD NEVER APPEAR")
		os.Exit(9)
	}
	os.Exit(0)
}

func enableTestReview(t *testing.T, session *antigravitySession, dir, mode string) {
	t.Helper()
	session.reviewCommand = []string{os.Args[0], "-test.run=^TestAgyReviewHelper$", "--"}
	session.extraEnv = append(session.extraEnv, "GO_WANT_AGY_REVIEW_HELPER=1")
	setReviewMode(t, dir, mode)
}

func setReviewMode(t *testing.T, dir, mode string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "review-mode"), []byte(mode), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestAgyReviewPendingKeepsCandidatePrivateAndAllowsCallback(t *testing.T) {
	session, dir := testCLI(t, "multi-response")
	enableTestReview(t, session, dir, "pending")
	if err := session.Send("original task", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed := terminalEvent(t, session)
	if event.Type != core.EventPending || streamed != "" || event.Content != "" || event.Pending == nil ||
		event.Pending.Stage != "review_waiting" || !session.Alive() {
		t.Fatalf("pending leaked candidate or closed session: %+v, %q", event, streamed)
	}
	setReviewMode(t, dir, "same-task")
	if err := session.Send("review callback", "ipc-"+strings.Repeat("a", 64), nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed = terminalEvent(t, session)
	if event.Type != core.EventResult || streamed != "" || event.Content != testAgyCandidate {
		t.Fatalf("callback did not publish the exact approved candidate: %+v, %q", event, streamed)
	}
	var checked struct {
		CandidateText   string `json:"candidate_text"`
		CandidateSHA256 string `json:"candidate_sha256"`
		ConversationID  string `json:"conversation_id"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "review-check.json"))
	if err != nil || json.Unmarshal(data, &checked) != nil {
		t.Fatal("missing exact check payload")
	}
	hash := sha256.Sum256([]byte(testAgyCandidate))
	if checked.CandidateText != testAgyCandidate || checked.CandidateSHA256 != hex.EncodeToString(hash[:]) || checked.ConversationID != "stream-session" {
		t.Fatalf("candidate bytes or identity changed: %+v", checked)
	}
}

func TestAgyReviewMissingManifestDoesNotClaimHermesWasDispatched(t *testing.T) {
	session, dir := testCLI(t, "multi-response")
	enableTestReview(t, session, dir, "manifest-missing")
	if err := session.Send("original task", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Missing materials now allow two bounded repair runs. Account for all
	// three helper processes, including the race runtime's exit delay.
	event, streamed := recoveryTerminalEvent(t, session)
	if event.Type != core.EventPending || event.Pending == nil ||
		event.Pending.Stage != "preparation" || event.Pending.Code != "manifest_missing" ||
		event.Content != "" || streamed != "" {
		t.Fatalf("missing manifest was reported as dispatched review or leaked candidate: %+v, %q", event, streamed)
	}
}

func TestAgyReviewAcceptsOfficialAuxiliarySteps(t *testing.T) {
	for _, scenario := range []string{
		"checkpoint", "checkpoint-active-done", "checkpoint-multi-response",
		"system-message-before-response", "system-message-after-response", "finish", "subagent-before-response",
	} {
		t.Run(scenario, func(t *testing.T) {
			session, dir := testCLI(t, scenario)
			enableTestReview(t, session, dir, "verified")
			if err := session.Send("request", "", nil, nil); err != nil {
				t.Fatal(err)
			}
			event, streamed := terminalEvent(t, session)
			if event.Type != core.EventResult || event.Content != testAgyCandidate || streamed != "" {
				t.Fatalf("official response/auxiliary sequence did not publish the reviewed candidate: %+v, %q", event, streamed)
			}
		})
	}
}

func TestAgyReviewRejectsMissingOrIncompleteFinalResponse(t *testing.T) {
	for _, scenario := range []string{
		"incomplete-response", "no-final-response", "result-only", "empty-response", "no-step-index",
		"checkpoint-incomplete", "checkpoint-incomplete-response", "checkpoint-around-tool", "checkpoint-around-user-input",
		"checkpoint-before-unknown", "checkpoint-text", "checkpoint-tool-name", "checkpoint-tool-info",
		"checkpoint-empty-tool-info", "checkpoint-subagent-info", "checkpoint-unknown-state",
		"system-message-without-response",
	} {
		t.Run(scenario, func(t *testing.T) {
			session, dir := testCLI(t, scenario)
			enableTestReview(t, session, dir, "verified")
			if err := session.Send("request", "", nil, nil); err != nil {
				t.Fatal(err)
			}
			event, streamed := terminalEvent(t, session)
			if (event.Type != core.EventError && event.Type != core.EventPending) || event.Content != "" || streamed != "" || !session.Alive() {
				t.Fatalf("incomplete response escaped the gate: %+v, %q", event, streamed)
			}
			if _, err := os.Stat(filepath.Join(dir, "review-check.json")); !os.IsNotExist(err) {
				t.Fatal("an unidentified candidate was submitted to review")
			}
		})
	}
}

func TestAgyReviewRejectsMismatchedAndFailedReceipts(t *testing.T) {
	for _, mode := range []string{"blocked", "wrong-hash", "wrong-manifest", "changed-manifest", "wrong-task", "wrong-protocol", "denied-verified", "unknown-status", "invalid-json", "extra-json", "exit-error"} {
		t.Run(mode, func(t *testing.T) {
			session, dir := testCLI(t, "normal")
			enableTestReview(t, session, dir, "verified")
			task, err := session.beginReview(context.Background(), "request", "", "stream-session")
			if err != nil {
				t.Fatal(err)
			}
			setReviewMode(t, dir, mode)
			err = session.checkReview(context.Background(), task, "stream-session", testAgyCandidate)
			if err == nil || strings.Contains(err.Error(), testAgyCandidate) || strings.Contains(err.Error(), "PRIVATE CANDIDATE") {
				t.Fatalf("gate result was accepted or leaked content: %v", err)
			}
		})
	}
}

func TestAgyReviewBeginPinsNewTaskIdentityAndRejectsUnknownStatus(t *testing.T) {
	for _, mode := range []string{"wrong-begin-task", "unknown-begin-status", "blocked-begin", "missing-begin-allow"} {
		t.Run(mode, func(t *testing.T) {
			session, dir := testCLI(t, "normal")
			enableTestReview(t, session, dir, mode)
			if _, err := session.beginReview(context.Background(), "request", "", ""); err == nil {
				t.Fatal("invalid begin response was accepted")
			}
		})
	}
}

func TestAgyBusyReviewTaskRoutingKeepsDifferentTasksWaiting(t *testing.T) {
	session, dir := testCLI(t, "delayed")
	enableTestReview(t, session, dir, "verified")
	done := make(chan error, 1)
	go func() { done <- session.Send("original task", "", nil, nil) }()
	select {
	case <-session.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("no init")
	}
	if err := os.WriteFile(filepath.Join(dir, "accept"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	recorder := &agyProgressRecorder{}
	id := strings.Repeat("b", 64)
	setReviewMode(t, dir, "busy-begin")
	if err := session.QueueInput("stream-session", id, "new Atlas task", recorder.report); !errors.Is(err, core.ErrNoActiveInput) {
		t.Fatalf("known gate busy should wait before queueing: %v", err)
	}
	if len(recorder.snapshot()) != 0 {
		t.Fatal("gate busy took ownership of the input")
	}
	setReviewMode(t, dir, "bad-pending-begin")
	if err := session.QueueInput("stream-session", id, "new Atlas task", recorder.report); err == nil || errors.Is(err, core.ErrNoActiveInput) {
		t.Fatalf("arbitrary pending response was treated as retryable: %v", err)
	}
	setReviewMode(t, dir, "verified")
	var firstProposed string
	for i := 0; i < 2; i++ {
		if err := session.QueueInput("stream-session", id, "new Atlas task", recorder.report); !errors.Is(err, core.ErrNoActiveInput) {
			t.Fatalf("different task should wait without side effects: %v", err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "review-begin.json"))
		var begun struct {
			TaskID       string `json:"task_id"`
			Continuation bool   `json:"continuation"`
			EventID      string `json:"event_id"`
		}
		if json.Unmarshal(data, &begun) != nil || !begun.Continuation || begun.EventID != id {
			t.Fatalf("invalid queued continuation request: %s", data)
		}
		if i > 0 && begun.TaskID != firstProposed {
			t.Fatal("waiting retry invented a new task UUID")
		}
		firstProposed = begun.TaskID
	}
	if len(recorder.snapshot()) != 0 {
		t.Fatal("different task was queued in the current candidate")
	}
	setReviewMode(t, dir, "same-task")
	if err := session.QueueInput("stream-session", strings.Repeat("c", 64), "registered callback", recorder.report); err != nil {
		t.Fatal(err)
	}
	session.startMu.Lock()
	run := session.activeRun
	session.startMu.Unlock()
	output, err := run.claim(invocationInput("stream-session", 2), "post")
	if err != nil || len(output.InjectSteps) != 1 || !strings.Contains(output.InjectSteps[0].UserMessage, "FIXED REVIEW INSTRUCTIONS") {
		t.Fatalf("same-task callback not injected: %+v, %v", output, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "finish"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	terminalEvent(t, session)
}

func TestAgyReviewUsesArgvAndBoundsExecution(t *testing.T) {
	session, dir := testCLI(t, "normal")
	enableTestReview(t, session, dir, "verified")
	sentinel := filepath.Join(dir, "must-not-exist")
	literal := "$(touch " + sentinel + "); literal argument"
	session.reviewCommand = append(session.reviewCommand, literal)
	if _, err := session.beginReview(context.Background(), "request", "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatal("review argument was evaluated by a shell")
	}
	data, _ := os.ReadFile(filepath.Join(dir, "review-args.json"))
	var args []string
	if json.Unmarshal(data, &args) != nil || !contains(args, literal) {
		t.Fatalf("argument was not passed literally: %s", data)
	}
	setReviewMode(t, dir, "timeout")
	var output any
	if err := session.callReview(context.Background(), map[string]any{}, &output, 20*time.Millisecond); err == nil {
		t.Fatal("timed out gate was accepted")
	}
	buffer := &reviewOutputBuffer{limit: 8}
	if _, err := io.Copy(buffer, bytes.NewBufferString(strings.Repeat("x", 32))); err == nil || len(buffer.Bytes()) > 8 {
		t.Fatal("review stdout limit was bypassed")
	}
}

func TestAgyOptionsValidateEffortAndReviewArgv(t *testing.T) {
	if _, err := New(map[string]any{"cmd": os.Args[0], "review_command": []string{filepath.Join(t.TempDir(), "missing-gate")}}); err == nil {
		t.Fatal("missing configured gate was accepted at startup")
	}
	for _, input := range []any{nil, "", "low", "medium", "high", " High "} {
		if _, err := parseAgyEffort(input); err != nil {
			t.Fatalf("valid effort %v: %v", input, err)
		}
	}
	for _, input := range []any{"max", true, 3} {
		if _, err := parseAgyEffort(input); err == nil {
			t.Fatalf("invalid effort accepted: %v", input)
		}
	}
	for _, input := range []any{"command --arg", []any{"command", 1}, []string{""}, []string{"command", "bad\x00arg"}} {
		if _, err := parseReviewCommand(input); err == nil {
			t.Fatalf("invalid review command accepted: %v", input)
		}
	}
	for _, input := range []any{nil, []any{}, []string{"python3", "path with spaces"}, []any{"python3", "script"}} {
		if _, err := parseReviewCommand(input); err != nil {
			t.Fatalf("valid review argv rejected: %v", err)
		}
	}
}
