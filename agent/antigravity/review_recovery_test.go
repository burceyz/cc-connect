package antigravity

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type agyRecoveryRequest struct {
	Protocol        string `json:"protocol"`
	Op              string `json:"op"`
	TaskID          string `json:"task_id"`
	RequestText     string `json:"request_text"`
	RequestSHA256   string `json:"request_sha256"`
	Continuation    bool   `json:"continuation"`
	Resume          bool   `json:"resume"`
	EventID         string `json:"event_id"`
	ConversationID  string `json:"conversation_id"`
	CandidateText   string `json:"candidate_text"`
	CandidateSHA256 string `json:"candidate_sha256"`
	ManifestPath    string `json:"manifest_path"`
	Revision        string `json:"revision"`
	// Trace-only observations from the fake CLI, not review protocol fields.
	ObservedCLIArgs []string `json:"observed_cli_arguments,omitempty"`
	ObservedCLITask string   `json:"observed_cli_task,omitempty"`
}

func TestMalformedReviewRecoveryIsBoundedAndDoesNotRetryAmbiguousSubmissions(t *testing.T) {
	for _, test := range []struct {
		code   string
		repair bool
	}{
		{"hermes_report_invalid_json", true},
		{"review_format_attempts_exhausted", false},
		{"submission_unconfirmed", false},
		{"native_two_subreviews_missing", false},
	} {
		pending := pendingReview(agyReviewResponse{Status: "pending_review", Reasons: []string{test.code}})
		if pending.repair != test.repair || pending.code != test.code {
			t.Fatalf("%s: got repair=%v, code=%s", test.code, pending.repair, pending.code)
		}
	}
}

// This executable tests the Go gate transport and publication boundary only.
// It never calls Hermes or proves an actual review; the Python gate tests and
// the real service acceptance run cover that separate boundary.
func TestAgyReviewRecoveryGateHelper(t *testing.T) {
	if os.Getenv("GO_WANT_AGY_REVIEW_RECOVERY_HELPER") != "1" {
		return
	}
	dir := os.Getenv("CC_TEST_AGY_DIR")
	write := func(path string, data []byte) {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			os.Exit(2)
		}
	}
	var request agyRecoveryRequest
	if json.NewDecoder(os.Stdin).Decode(&request) != nil || request.Protocol != agyReviewProtocol {
		os.Exit(2)
	}
	if request.Op == "bind" {
		args, err := os.ReadFile(filepath.Join(dir, "args.json"))
		if err != nil || json.Unmarshal(args, &request.ObservedCLIArgs) != nil {
			os.Exit(2)
		}
		data, err := os.ReadFile(filepath.Join(dir, "cli-origin-env.json"))
		var environment map[string]string
		if err != nil || json.Unmarshal(data, &environment) != nil {
			os.Exit(2)
		}
		request.ObservedCLITask = environment["review_task"]
	}
	log, err := os.OpenFile(filepath.Join(dir, "recovery-requests.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil || json.NewEncoder(log).Encode(request) != nil || log.Close() != nil {
		os.Exit(2)
	}
	modeBytes, err := os.ReadFile(filepath.Join(dir, "recovery-mode"))
	if err != nil {
		os.Exit(2)
	}
	mode := string(modeBytes)
	taskID := request.TaskID
	if request.Op == "begin" {
		if request.RequestSHA256 != textSHA256(request.RequestText) || request.Continuation != (request.EventID != "") {
			os.Exit(2)
		}
		if request.Resume || request.Continuation {
			active, err := os.ReadFile(filepath.Join(dir, "recovery-active-task"))
			if err != nil {
				os.Exit(2)
			}
			taskID = string(active)
		} else {
			write(filepath.Join(dir, "recovery-active-task"), []byte(taskID))
		}
	}
	taskDir := filepath.Join(dir, "recovery-tasks", taskID)
	manifestPath := filepath.Join(taskDir, "manifest.json")
	fixturePath := filepath.Join(taskDir, "fixture.json")
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		os.Exit(2)
	}
	if request.Op == "begin" && !request.Resume && !request.Continuation && mode != "manifest_missing" {
		write(manifestPath, []byte("{\"transport_fixture\":true}\n"))
	}
	allow := false
	response := agyReviewResponse{
		Protocol: agyReviewProtocol, TaskID: taskID, TaskDir: taskDir, ManifestPath: manifestPath,
		Instructions: "TEST: preserve the frozen candidate after review.", Status: "pending_review", AllowPublish: &allow,
	}
	var fixture agyReviewResponse
	if data, err := os.ReadFile(fixturePath); err == nil {
		if json.Unmarshal(data, &fixture) != nil {
			os.Exit(2)
		}
		response.Revision, response.CandidateSHA256 = fixture.Revision, fixture.CandidateSHA256
		response.ManifestSHA256, response.CandidatePath = fixture.ManifestSHA256, fixture.CandidatePath
		response.Instructions = "TEST: read task " + taskID + " revision " + fixture.Revision +
			"; preserve the frozen candidate after review."
	}
	switch request.Op {
	case "begin":
	case "bind", "refresh":
		if request.ConversationID != "stream-session" {
			os.Exit(2)
		}
	case "check":
		if request.ConversationID != "stream-session" || request.ManifestPath != manifestPath ||
			request.CandidateSHA256 != textSHA256(request.CandidateText) {
			os.Exit(2)
		}
		response.CandidateSHA256 = request.CandidateSHA256
		manifest, err := os.ReadFile(manifestPath)
		if os.IsNotExist(err) {
			response.Reasons = []string{"manifest_missing"}
			break
		}
		if err != nil {
			os.Exit(2)
		}
		response.ManifestSHA256 = textSHA256(string(manifest))
		response.Revision = textSHA256(taskID + ":" + response.CandidateSHA256)
		response.CandidatePath = filepath.Join(taskDir, "revisions", response.Revision, "materials", "candidate.txt")
		if err := os.MkdirAll(filepath.Dir(response.CandidatePath), 0o700); err != nil {
			os.Exit(2)
		}
		write(response.CandidatePath, []byte(request.CandidateText))
		write(fixturePath, mustJSON(response))
		response.Reasons = []string{"review_pending"}
		if mode == "submission_unconfirmed" {
			response.Reasons = []string{mode}
		}
	case "finalize":
		if request.ConversationID != "stream-session" || request.Revision != fixture.Revision || fixture.Revision == "" {
			os.Exit(2)
		}
		if mode == "pending" || mode == "submission_unconfirmed" {
			response.Reasons = []string{"review_pending"}
			if mode == "submission_unconfirmed" {
				response.Reasons = []string{mode}
			}
			break
		}
		allow, response.Status = true, "verified"
		switch mode {
		case "wrong-task":
			response.TaskID = "wrong-task"
		case "wrong-revision":
			response.Revision = strings.Repeat("a", 64)
		case "wrong-candidate-hash":
			response.CandidateSHA256 = strings.Repeat("b", 64)
		case "wrong-manifest-hash":
			response.ManifestSHA256 = strings.Repeat("c", 64)
		case "outside-path", "symlink-candidate":
			outside := filepath.Join(dir, "outside-candidate.txt")
			write(outside, []byte(testAgyCandidate))
			if mode == "outside-path" {
				response.CandidatePath = outside
			} else {
				if os.Remove(fixture.CandidatePath) != nil || os.Symlink(outside, fixture.CandidatePath) != nil {
					os.Exit(2)
				}
			}
		case "changed-candidate":
			write(fixture.CandidatePath, []byte("UNREVIEWED REPLACEMENT"))
		case "changed-manifest":
			write(manifestPath, []byte("{\"changed_during_finalize\":true}\n"))
		}
		write(filepath.Join(dir, "recovery-finalize-mode-applied"), []byte(mode))
	default:
		os.Exit(2)
	}
	if json.NewEncoder(os.Stdout).Encode(response) != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func enableRecoveryReview(t *testing.T, session *antigravitySession, dir, mode string) {
	t.Helper()
	session.reviewCommand = []string{os.Args[0], "-test.run=^TestAgyReviewRecoveryGateHelper$", "--"}
	session.extraEnv = append(session.extraEnv, "GO_WANT_AGY_REVIEW_RECOVERY_HELPER=1")
	session.timeout = 30 * time.Second
	setRecoveryReviewMode(t, dir, mode)
}

func setRecoveryReviewMode(t *testing.T, dir, mode string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "recovery-mode"), []byte(mode), 0o600); err != nil {
		t.Fatal(err)
	}
}

func recoveryRequests(t *testing.T, dir string) []agyRecoveryRequest {
	t.Helper()
	file, err := os.Open(filepath.Join(dir, "recovery-requests.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var requests []agyRecoveryRequest
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var request agyRecoveryRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return requests
}

func recoveryTerminalEvent(t *testing.T, session *antigravitySession) (core.Event, string) {
	t.Helper()
	timer := time.NewTimer(25 * time.Second)
	defer timer.Stop()
	var text strings.Builder
	for {
		select {
		case event := <-session.Events():
			switch event.Type {
			case core.EventText:
				text.WriteString(event.Content)
			case core.EventResult, core.EventError, core.EventPending:
				return event, text.String()
			}
		case <-timer.C:
			t.Fatal("review recovery did not reach a bounded terminal state")
		}
	}
}

func seedRecoveryReview(t *testing.T, session *antigravitySession, dir string) string {
	t.Helper()
	enableRecoveryReview(t, session, dir, "pending")
	if err := session.Send("original independent task", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed := recoveryTerminalEvent(t, session)
	if event.Type != core.EventPending || event.Pending == nil || event.Pending.TaskID == "" ||
		event.Content != "" || streamed != "" || event.Pending.Stage != "review_waiting" {
		t.Fatalf("first review did not wait privately: %+v, streamed=%q", event, streamed)
	}
	return event.Pending.TaskID
}

func TestAgyReviewRecovery_CallbackWithoutFinalResponsePublishesFrozenCandidate(t *testing.T) {
	session, dir := testCLI(t, "multi-response")
	taskID := seedRecoveryReview(t, session, dir)
	setRecoveryReviewMode(t, dir, "verified")
	session.extraEnv = append(session.extraEnv, "CC_TEST_AGY_SCENARIO=no-final-response")
	if err := session.Send("fixed review callback", "ipc-"+strings.Repeat("d", 64), nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed := recoveryTerminalEvent(t, session)
	if event.Type != core.EventResult || event.Content != testAgyCandidate || streamed != "" ||
		strings.Contains(event.Content, testAgyProgress) || event.SessionID != "stream-session" {
		t.Fatalf("callback lost frozen bytes or leaked aggregate progress: %+v, streamed=%q", event, streamed)
	}
	counts := map[string]int{}
	for _, request := range recoveryRequests(t, dir) {
		counts[request.Op]++
		if request.Op != "begin" && request.TaskID != taskID {
			t.Fatalf("callback changed task: %+v", request)
		}
		if request.Op == "finalize" && (request.Revision == "" || request.ConversationID != "stream-session") {
			t.Fatalf("finalize lost the original revision/conversation: %+v", request)
		}
	}
	if counts["check"] != 1 || counts["finalize"] != 1 || counts["refresh"] != 1 || counts["bind"] != 2 {
		t.Fatalf("callback resubmitted candidate or skipped binding: %v", counts)
	}
}

func TestAgyReviewRecovery_ExplicitContinueRestoresTaskButIndependentRequestDoesNot(t *testing.T) {
	session, dir := testCLI(t, "normal")
	taskID := seedRecoveryReview(t, session, dir)
	continued, err := session.beginReview(context.Background(), "继续", "", "stream-session")
	if err != nil || continued.taskID != taskID || continued.revision == "" {
		t.Fatalf("explicit continue failed to recover fixed task: %+v, %v", continued, err)
	}
	independent, err := session.beginReview(context.Background(), "请处理另一个独立问题", "", "stream-session")
	if err != nil || independent.taskID == taskID || independent.revision != "" {
		t.Fatalf("independent request inherited previous task: %+v, %v", independent, err)
	}
	var begins []agyRecoveryRequest
	for _, request := range recoveryRequests(t, dir) {
		if request.Op == "begin" {
			begins = append(begins, request)
		}
	}
	if len(begins) != 3 || begins[0].Resume || !begins[1].Resume || begins[1].Continuation || begins[2].Resume {
		t.Fatalf("resume intent was not explicit: %+v", begins)
	}
}

func TestAgyReviewRecovery_FinalizeRejectsMismatchedOrChangedMaterials(t *testing.T) {
	for _, mode := range []string{"wrong-task", "wrong-revision", "wrong-candidate-hash", "wrong-manifest-hash", "outside-path", "symlink-candidate", "changed-candidate", "changed-manifest"} {
		t.Run(mode, func(t *testing.T) {
			session, dir := testCLI(t, "normal")
			seedRecoveryReview(t, session, dir)
			setRecoveryReviewMode(t, dir, mode)
			session.extraEnv = append(session.extraEnv, "CC_TEST_AGY_SCENARIO=no-final-response")
			if err := session.Send("fixed review callback", "ipc-"+strings.Repeat("e", 64), nil, nil); err != nil {
				t.Fatal(err)
			}
			event, streamed := recoveryTerminalEvent(t, session)
			if event.Type != core.EventError || event.Error == nil || event.Content != "" || streamed != "" ||
				strings.Contains(event.Error.Error(), testAgyCandidate) || strings.Contains(event.Error.Error(), "UNREVIEWED REPLACEMENT") {
				t.Fatalf("invalid finalization escaped the gate: %+v, streamed=%q", event, streamed)
			}
			applied, err := os.ReadFile(filepath.Join(dir, "recovery-finalize-mode-applied"))
			if err != nil || string(applied) != mode {
				t.Fatalf("test did not reach its intended finalization fault: %q, %v", applied, err)
			}
		})
	}
}

func TestAgyReviewRecovery_MissingMaterialsHaveTwoRepairsOnSameTask(t *testing.T) {
	for _, mode := range []string{"manifest_missing", "candidate_missing"} {
		t.Run(mode, func(t *testing.T) {
			scenario := "normal"
			if mode == "candidate_missing" {
				scenario = "no-final-response"
			}
			session, dir := testCLI(t, scenario)
			enableRecoveryReview(t, session, dir, mode)
			if err := session.Send("original task with incomplete materials", "", nil, nil); err != nil {
				t.Fatal(err)
			}
			event, streamed := recoveryTerminalEvent(t, session)
			if event.Type != core.EventPending || event.Pending == nil || event.Pending.Code != mode ||
				event.Pending.Stage != "preparation" || event.Content != "" || streamed != "" || !session.Alive() {
				t.Fatalf("incomplete materials were published or not bounded: %+v, streamed=%q", event, streamed)
			}
			counts := map[string]int{}
			for _, request := range recoveryRequests(t, dir) {
				counts[request.Op]++
				if request.TaskID != event.Pending.TaskID || (request.Op != "begin" && request.ConversationID != "stream-session") {
					t.Fatalf("internal repair changed task or conversation: %+v", request)
				}
				if request.Op == "bind" && request.ObservedCLITask != event.Pending.TaskID {
					t.Fatalf("repair CLI lost the original task environment: %+v", request)
				}
				if request.Op == "bind" && counts["bind"] > 1 &&
					(!contains(request.ObservedCLIArgs, "--conversation") || !contains(request.ObservedCLIArgs, "stream-session")) {
					t.Fatalf("repair CLI did not resume the original conversation: %+v", request)
				}
			}
			wantChecks := 3
			if mode == "candidate_missing" {
				wantChecks = 0
			}
			if counts["begin"] != 1 || counts["bind"] != 3 || counts["check"] != wantChecks || counts["finalize"] != 0 {
				t.Fatalf("repairs were not exactly initial attempt + two continuations: %v", counts)
			}
		})
	}
}

func TestAgyReviewRecovery_UnknownSubmissionNeverResubmitsOrRepairs(t *testing.T) {
	session, dir := testCLI(t, "normal")
	enableRecoveryReview(t, session, dir, "submission_unconfirmed")
	if err := session.Send("original task", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	first, streamed := recoveryTerminalEvent(t, session)
	if first.Type != core.EventPending || first.Pending == nil || first.Pending.Code != "submission_unconfirmed" || streamed != "" {
		t.Fatalf("unknown submission did not remain pending: %+v, %q", first, streamed)
	}
	session.extraEnv = append(session.extraEnv, "CC_TEST_AGY_SCENARIO=no-final-response")
	if err := session.Send("fixed review callback", "ipc-"+strings.Repeat("f", 64), nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed := recoveryTerminalEvent(t, session)
	if event.Type != core.EventPending || event.Pending == nil || event.Pending.TaskID != first.Pending.TaskID ||
		event.Pending.Code != "submission_unconfirmed" || event.Content != "" || streamed != "" {
		t.Fatalf("unknown callback was replaced or published: %+v, %q", event, streamed)
	}
	counts := map[string]int{}
	for _, request := range recoveryRequests(t, dir) {
		counts[request.Op]++
	}
	if counts["begin"] != 2 || counts["bind"] != 2 || counts["check"] != 1 || counts["finalize"] != 1 {
		t.Fatalf("unknown submission caused automatic retry/resubmission: %v", counts)
	}
}

func TestAgyReviewRecovery_QueueInputRequiresCollectedReport(t *testing.T) {
	for _, scenario := range []string{"missing_report", "regular_report"} {
		t.Run(scenario, func(t *testing.T) {
			session, dir := testCLI(t, "normal")
			enableRecoveryReview(t, session, dir, "pending")
			task, err := session.beginReview(context.Background(), "original task", "", "stream-session")
			if err != nil {
				t.Fatal(err)
			}
			// The live invocation still holds the earlier task state while the
			// gate has frozen the candidate and acquired a review revision.
			original := *task
			original.instructions = "STALE LIVE INSTRUCTIONS"
			err = session.checkReview(context.Background(), task, "stream-session", testAgyCandidate)
			var pending *agyReviewPending
			if !errors.As(err, &pending) || task.revision == "" {
				t.Fatalf("fixture did not create a pending fixed revision: %+v, %v", task, err)
			}
			run := testAgyRun(t, "stream-session")
			run.reviewTask = &original
			session.chatID.Store("stream-session")
			session.startMu.Lock()
			session.activeRun = run
			session.startMu.Unlock()
			reportPath := filepath.Join(task.taskDir, "revisions", task.revision, "hermes-response.txt")
			if scenario == "regular_report" {
				if err := os.WriteFile(reportPath, []byte("Transport fixture only; no actual model review.\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if _, err := os.Lstat(reportPath); !os.IsNotExist(err) {
				t.Fatalf("missing-report fixture unexpectedly has a report: %v", err)
			}
			before := len(recoveryRequests(t, dir))
			recorder := &agyProgressRecorder{}
			eventID, prompt := strings.Repeat("9", 64), "Fixed review callback: inspect the saved report."
			err = session.QueueInput("stream-session", eventID, prompt, recorder.report)
			if scenario == "missing_report" {
				if !errors.Is(err, core.ErrNoActiveInput) || len(recorder.snapshot()) != 0 {
					t.Fatalf("uncollected callback took ownership or did not defer: %v, %+v", err, recorder.snapshot())
				}
				run.mu.Lock()
				unchanged := run.reviewTask == &original && len(run.inputs) == 0 && len(run.order) == 0
				run.mu.Unlock()
				if !unchanged {
					t.Fatal("uncollected callback changed the active task or retained the event")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				run.mu.Lock()
				updated := run.reviewTask
				run.mu.Unlock()
				if updated == &original || updated.taskID != task.taskID || updated.revision != task.revision ||
					updated.candidateSHA != task.candidateSHA || updated.manifestSHA != task.manifestSHA {
					t.Fatalf("collected callback did not install its fixed revision: %+v", updated)
				}
				if progress := recorder.snapshot(); len(progress) != 1 || progress[0].Phase != core.LiveInputPhaseQueued {
					t.Fatalf("callback was not queued once: %+v", progress)
				}
			}
			output, err := run.claim(invocationInput("stream-session", 7), "post")
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "missing_report" {
				if len(output.InjectSteps) != 0 || len(recorder.snapshot()) != 0 {
					t.Fatalf("uncollected report was claimable: %+v", output)
				}
			} else {
				if len(output.InjectSteps) != 1 || output.TerminationBehavior != "force_continue" {
					t.Fatalf("collected callback was not injected: %+v", output)
				}
				message := output.InjectSteps[0].UserMessage
				if !strings.HasPrefix(message, prompt) || !strings.Contains(message, task.taskID) ||
					!strings.Contains(message, task.revision) || strings.Contains(message, original.instructions) ||
					strings.Contains(message, testAgyCandidate) {
					t.Fatalf("injection lost revision instructions or exposed the candidate: %q", message)
				}
				if progress := recorder.snapshot(); len(progress) != 2 || progress[1].Phase != core.LiveInputPhaseClaimed {
					t.Fatalf("claim invented delivery or was recorded twice: %+v", progress)
				}
			}
			requests := recoveryRequests(t, dir)[before:]
			if len(requests) != 1 || requests[0].Op != "begin" || !requests[0].Continuation ||
				requests[0].EventID != eventID || requests[0].ConversationID != "stream-session" {
				t.Fatalf("hot callback path performed refresh/finalize or lost its fixed route: %+v", requests)
			}
			select {
			case event := <-session.Events():
				t.Fatalf("queue/claim published a candidate or completed a review: %+v", event)
			default:
			}
		})
	}
}
