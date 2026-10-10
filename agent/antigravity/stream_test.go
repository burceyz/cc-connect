package antigravity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const testAgyCandidate = "候选结果  \nexact trailing newline\n"
const testAgyProgress = "Checking the completed review.\n"

// This is an isolated test executable, not the installed agy or a model call.
func TestAgyCLIHelper(t *testing.T) {
	if os.Getenv("GO_WANT_AGY_CLI_HELPER") != "1" {
		return
	}
	dir, scenario := os.Getenv("CC_TEST_AGY_DIR"), os.Getenv("CC_TEST_AGY_SCENARIO")
	if err := os.WriteFile(filepath.Join(dir, "args.json"), mustJSON(os.Args), 0o600); err != nil {
		os.Exit(2)
	}
	_ = os.WriteFile(filepath.Join(dir, "cli-origin-env.json"), mustJSON(map[string]string{
		"origin": os.Getenv("CC_IPC_ORIGIN"), "project": os.Getenv("CC_PROJECT"),
		"session_key": os.Getenv("CC_SESSION_KEY"), "data_dir": os.Getenv("CC_DATA_DIR"),
		"review_task": os.Getenv("CC_REVIEW_TASK_ID"), "review_dir": os.Getenv("CC_REVIEW_TASK_DIR"),
		"review_manifest": os.Getenv("CC_REVIEW_MANIFEST_PATH"),
	}), 0o600)
	if scenario == "exit-failure" {
		os.Exit(7)
	}
	emit := func(value any) { _ = json.NewEncoder(os.Stdout).Encode(value) }
	emit(map[string]any{"event": "init", "conversation_id": "stream-session", "init": map[string]any{"model": "configured-model"}})
	if scenario == "malformed" {
		fmt.Fprintln(os.Stdout, "not-json")
		os.Exit(0)
	}
	if scenario == "delayed" {
		helperWaitFile(filepath.Join(dir, "accept"))
	}
	if scenario != "no-input" {
		emit(map[string]any{"event": "step_update", "step_update": map[string]any{
			"conversation_id": "stream-session", "step_index": 0, "state": "DONE", "step_type": "user_input",
		}})
	}
	if scenario == "delayed" {
		helperWaitFile(filepath.Join(dir, "finish"))
	}
	if scenario == "no-result" {
		os.Exit(0)
	}
	emitText := func(index int, state, text string) {
		step := map[string]any{
			"conversation_id": "stream-session", "step_index": index, "state": state, "step_type": "agent_response", "text_delta": text,
		}
		if scenario == "no-step-index" {
			delete(step, "step_index")
		}
		emit(map[string]any{"event": "step_update", "step_update": step})
	}
	response, responseIndex := testAgyCandidate, 1
	checkpoint := strings.HasPrefix(scenario, "checkpoint")
	if checkpoint || strings.HasPrefix(scenario, "system-message") || scenario == "finish" || scenario == "subagent-before-response" {
		responseIndex = 3
	}
	if scenario == "multi-response" || scenario == "incomplete-response" || scenario == "no-final-response" || scenario == "checkpoint-multi-response" {
		emitText(1, "ACTIVE", "Checking ")
		emitText(1, "DONE", "the completed review.\n")
		emit(map[string]any{"event": "step_update", "step_update": map[string]any{
			"conversation_id": "stream-session", "step_index": 2, "state": "DONE", "step_type": "tool",
			"tool_info": map[string]any{"name": "view_file", "output": "fixed candidate read"},
		}})
		response, responseIndex = testAgyProgress+testAgyCandidate, 3
	}
	if strings.HasPrefix(scenario, "system-message") || scenario == "subagent-before-response" {
		kind := "system_message"
		if scenario == "subagent-before-response" {
			kind = "subagent"
		}
		emit(map[string]any{"event": "step_update", "step_update": map[string]any{
			"conversation_id": "stream-session", "step_index": 2, "state": "DONE", "step_type": kind,
		}})
	}
	if checkpoint {
		// The official headless example ends with response 3, checkpoint 4.
		state := "DONE"
		if scenario == "checkpoint-incomplete-response" {
			state = "ACTIVE"
		}
		emitText(responseIndex, state, testAgyCandidate)
	} else if scenario == "empty-response" {
		emitText(responseIndex, "DONE", "")
	} else if scenario != "result-only" && scenario != "no-final-response" && scenario != "system-message-without-response" {
		for i, text := range []string{"候选结果  \n", "exact trailing newline\n"} {
			state := "ACTIVE"
			if i == 1 && scenario != "incomplete-response" {
				state = "DONE"
			}
			emitText(responseIndex, state, text)
		}
	}
	if checkpoint || scenario == "system-message-after-response" || scenario == "finish" {
		kind := "checkpoint"
		if scenario == "system-message-after-response" {
			kind = "system_message"
		} else if scenario == "finish" {
			kind = "finish"
		}
		step := map[string]any{
			"conversation_id": "stream-session", "step_index": 4, "state": "DONE", "step_type": kind,
		}
		if scenario == "checkpoint-active-done" || scenario == "checkpoint-incomplete" ||
			scenario == "checkpoint-around-tool" || scenario == "checkpoint-around-user-input" {
			step["state"] = "ACTIVE"
			emit(map[string]any{"event": "step_update", "step_update": step})
			step["state"] = "DONE"
		}
		switch scenario {
		case "checkpoint-around-tool", "checkpoint-around-user-input":
			kind := "tool"
			if scenario == "checkpoint-around-user-input" {
				kind = "user_input"
			}
			emit(map[string]any{"event": "step_update", "step_update": map[string]any{
				"conversation_id": "stream-session", "step_index": 5, "state": "DONE", "step_type": kind,
			}})
		case "checkpoint-text":
			step["text_delta"] = "unreviewed checkpoint payload"
		case "checkpoint-tool-name":
			step["tool_name"] = "view_file"
		case "checkpoint-tool-info":
			step["tool_info"] = map[string]any{"name": "view_file"}
		case "checkpoint-empty-tool-info":
			step["tool_info"] = map[string]any{}
		case "checkpoint-subagent-info":
			step["subagent_info"] = map[string]any{"subagents": []any{}}
		case "checkpoint-unknown-state":
			step["state"] = "UNKNOWN"
		}
		if scenario != "checkpoint-incomplete" {
			emit(map[string]any{"event": "step_update", "step_update": step})
		}
		if scenario == "checkpoint-before-unknown" {
			emit(map[string]any{"event": "step_update", "step_update": map[string]any{
				"conversation_id": "stream-session", "step_index": 5, "state": "DONE", "step_type": "unknown",
			}})
		}
	}
	status, errorText := "SUCCESS", ""
	if scenario == "terminal-error" {
		status, errorText = "ERROR", "simulated backend failure"
	}
	emit(map[string]any{"event": "result", "result": map[string]any{
		"conversation_id": "stream-session", "status": status, "response": response, "error": errorText,
		"usage": map[string]int{"input_tokens": 12, "output_tokens": 7, "cache_read_tokens": 3},
	}})
	os.Exit(0)
}

func helperWaitFile(path string) {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	os.Exit(3)
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func testCLI(t *testing.T, scenario string) (*antigravitySession, string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	session, err := newAntigravitySession(context.Background(), os.Args[0],
		[]string{"-test.run=^TestAgyCLIHelper$", "--"}, dir, "gemini-3.8-flash-high", "plan", "", []string{
			"GO_WANT_AGY_CLI_HELPER=1", "CC_TEST_AGY_DIR=" + dir, "CC_TEST_AGY_SCENARIO=" + scenario,
			"CC_PROJECT=test-project", "CC_SESSION_KEY=test:fixed-origin",
		}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, dir
}

func terminalEvent(t *testing.T, session *antigravitySession) (core.Event, string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var text strings.Builder
	for {
		select {
		case event := <-session.Events():
			switch event.Type {
			case core.EventText:
				text.WriteString(event.Content)
			case core.EventResult, core.EventError:
				return event, text.String()
			}
		case <-timer.C:
			t.Fatal("timeout waiting for terminal event")
		}
	}
}

func TestAgySendWaitsForInputAcceptanceAndExposesFirstRunID(t *testing.T) {
	session, dir := testCLI(t, "delayed")
	done := make(chan error, 1)
	go func() { done <- session.Send("original request", "", nil, nil) }()
	select {
	case event := <-session.Events():
		if event.SessionID != "stream-session" || session.CurrentSessionID() != "stream-session" {
			t.Fatalf("first init identity = %+v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("init was not available before the first turn ended")
	}
	select {
	case err := <-done:
		t.Fatalf("Send returned before input acceptance: %v", err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "accept"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Send did not return after input acceptance")
	}
	if err := os.WriteFile(filepath.Join(dir, "finish"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	event, text := terminalEvent(t, session)
	if event.Type != core.EventResult || event.Content != testAgyCandidate || text != testAgyCandidate {
		t.Fatalf("stream/result duplicated or changed text: %+v, %q", event, text)
	}
}

func TestAgyModelAndEffortArePassedToEachResume(t *testing.T) {
	session, dir := testCLI(t, "normal")
	session.effort = "high"
	var generation string
	for i := 0; i < 2; i++ {
		if err := session.Send("request", "", nil, nil); err != nil {
			t.Fatal(err)
		}
		session.invocationBridge.mu.Lock()
		currentGeneration := session.invocationBridge.run.generation
		session.invocationBridge.mu.Unlock()
		if i > 0 && currentGeneration == generation {
			t.Fatal("resume reused the old hook generation")
		}
		generation = currentGeneration
		event, _ := terminalEvent(t, session)
		if event.Type != core.EventResult || event.InputTokens != 12 || event.OutputTokens != 7 || event.CacheReadInputTokens != 3 {
			t.Fatalf("result = %+v", event)
		}
		data, err := os.ReadFile(filepath.Join(dir, "args.json"))
		if err != nil {
			t.Fatal(err)
		}
		var args []string
		if json.Unmarshal(data, &args) != nil || !contains(args, "--model") || !contains(args, "gemini-3.8-flash-high") || !contains(args, "--effort") || !contains(args, "high") {
			t.Fatalf("configured model/effort was ignored: %v", args)
		}
		if i > 0 && (!contains(args, "--conversation") || !contains(args, "stream-session")) {
			t.Fatalf("resume lost the fixed conversation: %v", args)
		}
	}
}

func TestAgyUngatedStreamPreservesAggregateResponse(t *testing.T) {
	session, _ := testCLI(t, "multi-response")
	if err := session.Send("request", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed := terminalEvent(t, session)
	want := testAgyProgress + testAgyCandidate
	if event.Type != core.EventResult || event.Content != want || streamed != want {
		t.Fatalf("ungated response changed: %+v, %q", event, streamed)
	}
}

func TestAgyFailuresDoNotMasqueradeAsSuccessfulInputOrResults(t *testing.T) {
	for _, scenario := range []string{"no-input", "exit-failure", "malformed", "no-result", "terminal-error"} {
		t.Run(scenario, func(t *testing.T) {
			session, _ := testCLI(t, scenario)
			err := session.Send("request", "", nil, nil)
			accepted := scenario == "no-result" || scenario == "terminal-error"
			if (err == nil) != accepted {
				t.Fatalf("Send error = %v, want accepted=%v", err, accepted)
			}
			event, _ := terminalEvent(t, session)
			if event.Type != core.EventError || event.Error == nil || event.Content != "" || !session.Alive() {
				t.Fatalf("failure event = %+v, alive=%v", event, session.Alive())
			}
		})
	}
}

func TestAgyStreamRejectsIdentityConflictsAndDoesNotPrintJSON(t *testing.T) {
	for _, lines := range [][]string{
		{`{"event":"init","conversation_id":"wrong","init":{}}`},
		{`{"event":"init","conversation_id":"main","init":{"conversation_id":"different"}}`},
		{`{"event":"step_update","step_update":{"conversation_id":"main","step_type":"user_input","state":"DONE"}}`},
		{`{"event":"init","conversation_id":"main","init":{}}`, `{"event":"init","conversation_id":"main","init":{}}`},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		run, err := newAgyRun(ctx, cancel, "main")
		if err != nil {
			t.Fatal(err)
		}
		session := &antigravitySession{ctx: ctx, events: make(chan core.Event, 8)}
		session.chatID.Store("main")
		stream := &agyStream{session: session, run: run}
		for _, line := range lines {
			err = stream.handleLine([]byte(line))
		}
		if err == nil {
			t.Fatalf("identity violation was accepted: %v", lines)
		}
		for len(session.events) > 0 {
			if event := <-session.events; event.Content != "" {
				t.Fatalf("raw JSON escaped: %+v", event)
			}
		}
		cancel()
	}
}

func TestAgyCloseUnblocksSendBeforeInputAcceptance(t *testing.T) {
	session, _ := testCLI(t, "delayed")
	done := make(chan error, 1)
	go func() { done <- session.Send("request", "", nil, nil) }()
	select {
	case <-session.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("no init before close")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed, unaccepted input reported success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close left Send blocked")
	}
	if session.Alive() {
		t.Fatal("closed session still alive")
	}
}

func TestAgyStreamReportsToolProgressWithoutCandidateLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run, err := newAgyRun(ctx, cancel, "")
	if err != nil {
		t.Fatal(err)
	}
	session := &antigravitySession{ctx: ctx, events: make(chan core.Event, 16), reviewCommand: []string{"configured-gate"}}
	stream := &agyStream{session: session, run: run}
	for _, line := range []string{
		`{"event":"init","init":{"conversation_id":"main"}}`,
		`{"event":"step_update","step_update":{"conversation_id":"main","step_index":0,"step_type":"user_input","state":"DONE"}}`,
		`{"event":"step_update","step_update":{"conversation_id":"main","step_index":1,"step_type":"agent_response","state":"ACTIVE","text_delta":"unreviewed candidate"}}`,
		`{"event":"step_update","step_update":{"conversation_id":"main","step_index":2,"step_type":"tool","state":"ACTIVE","tool_info":{"name":"run_command","parameters":{"CommandLine":"echo test"}}}}`,
		`{"event":"step_update","step_update":{"conversation_id":"main","step_index":2,"step_type":"tool","state":"DONE","tool_name":"run_command","tool_info":{"output":"test"}}}`,
		`{"event":"step_update","step_update":{"conversation_id":"main","step_index":3,"step_type":"tool","state":"DONE","tool_name":"view_file","tool_info":{"error":{"message":"missing file"}}}}`,
		`{"event":"future_protocol_event"}`,
	} {
		if err := stream.handleLine([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	uses, results := 0, 0
	for len(session.events) > 0 {
		event := <-session.events
		if strings.Contains(event.Content, "unreviewed candidate") {
			t.Fatal("candidate leaked while reporting tool progress")
		}
		if event.Type == core.EventToolUse {
			uses++
		}
		if event.Type == core.EventToolResult {
			results++
		}
	}
	if uses != 2 || results != 2 {
		t.Fatalf("tool transitions = %d uses, %d results", uses, results)
	}
}
