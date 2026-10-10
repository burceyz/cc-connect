package antigravity

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

type agyUsage struct {
	InputTokens     int `json:"input_tokens"`
	OutputTokens    int `json:"output_tokens"`
	CacheReadTokens int `json:"cache_read_tokens"`
}

type agyResult struct {
	ConversationID string   `json:"conversation_id"`
	Status         string   `json:"status"`
	Response       string   `json:"response"`
	Error          string   `json:"error"`
	Usage          agyUsage `json:"usage"`
}

type agyStreamStep struct {
	ConversationID string           `json:"conversation_id"`
	StepIndex      *int             `json:"step_index"`
	State          string           `json:"state"`
	StepType       string           `json:"step_type"`
	TextDelta      string           `json:"text_delta"`
	ToolName       string           `json:"tool_name"`
	ToolInfo       *agyToolInfo     `json:"tool_info"`
	SubagentInfo   *json.RawMessage `json:"subagent_info"`
}

type agyToolInfo struct {
	Name       string         `json:"name"`
	Parameters map[string]any `json:"parameters"`
	Output     string         `json:"output"`
	Error      *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type agyStreamEvent struct {
	Event          string `json:"event"`
	ConversationID string `json:"conversation_id"`
	Init           *struct {
		ConversationID string `json:"conversation_id"`
		Model          string `json:"model"`
	} `json:"init"`
	Step   *agyStreamStep `json:"step_update"`
	Result *agyResult     `json:"result"`
}

type agyStream struct {
	session       *antigravitySession
	run           *agyRun
	result        *agyResult
	response      strings.Builder
	responseSize  int
	lastStepIndex int
	lastStepType  string
	candidate     *agyResponseStep
	checkpoints   map[int]bool
	tools         map[int]bool
}

type agyResponseStep struct {
	index int
	done  bool
	text  strings.Builder
}

func (s *agyStream) handleLine(line []byte) error {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	var event agyStreamEvent
	if err := json.Unmarshal(line, &event); err != nil || event.Event == "" {
		return fmt.Errorf("antigravity: invalid stream-json event")
	}
	if s.result != nil {
		return fmt.Errorf("antigravity: stream event after terminal result")
	}
	switch event.Event {
	case "init":
		if event.Init == nil {
			return fmt.Errorf("antigravity: init payload is missing")
		}
		id := event.ConversationID
		if id == "" {
			id = event.Init.ConversationID
		} else if event.Init.ConversationID != "" && event.Init.ConversationID != id {
			return fmt.Errorf("antigravity: conflicting init identities")
		}
		if err := s.run.bind(id); err != nil {
			return err
		}
		s.session.chatID.Store(id)
		s.session.emit(core.Event{Type: core.EventText, SessionID: id, ToolName: event.Init.Model})
	case "step_update":
		if event.Step == nil {
			return fmt.Errorf("antigravity: step payload is missing")
		}
		return s.handleStep(*event.Step)
	case "result":
		if event.Result == nil {
			return fmt.Errorf("antigravity: terminal payload is missing")
		}
		if (event.Result.ConversationID == "" && event.Result.Status == "SUCCESS") ||
			(event.Result.ConversationID != "" && event.Result.ConversationID != s.session.CurrentSessionID()) {
			return fmt.Errorf("antigravity: terminal conversation mismatch")
		}
		s.result = event.Result
		s.run.end(false)
	default:
		// Forward-compatible event types are ignored, never rendered as text.
	}
	return nil
}

func (s *agyStream) handleStep(step agyStreamStep) error {
	if step.ConversationID == "" || step.ConversationID != s.session.CurrentSessionID() {
		return fmt.Errorf("antigravity: step conversation mismatch")
	}
	s.run.mu.Lock()
	bound := s.run.sessionID != ""
	s.run.mu.Unlock()
	if !bound {
		return fmt.Errorf("antigravity: step received before init")
	}
	gated := len(s.session.reviewCommand) > 0
	if gated {
		if step.StepIndex == nil || *step.StepIndex < 0 {
			return fmt.Errorf("antigravity: review stream step index is missing or invalid")
		}
		switch step.StepType {
		case "checkpoint":
			// The official CLI may append bookkeeping after its final reply.
			// Keep its completion state without replacing the last content step.
			return s.handleCheckpointStep(step)
		case "user_input", "agent_response", "tool":
			if _, checkpoint := s.checkpoints[*step.StepIndex]; checkpoint {
				return fmt.Errorf("antigravity: review step index changed type")
			}
		default:
			return fmt.Errorf("antigravity: unsupported review stream step type; candidate withheld")
		}
		if *step.StepIndex >= s.lastStepIndex {
			s.lastStepIndex, s.lastStepType = *step.StepIndex, step.StepType
		}
	}
	switch step.StepType {
	case "user_input":
		if step.State == "DONE" {
			return s.run.acceptInput(step.ConversationID)
		}
	case "agent_response":
		if s.responseSize+len(step.TextDelta) > 16<<20 {
			return fmt.Errorf("antigravity: response exceeds supported size")
		}
		s.responseSize += len(step.TextDelta)
		if gated {
			return s.handleCandidateStep(step)
		}
		// Existing installations without a gate retain live streaming and
		// the CLI's aggregate result response.
		s.response.WriteString(step.TextDelta)
		if step.TextDelta != "" {
			s.session.emit(core.Event{Type: core.EventText, Content: step.TextDelta})
		}
	case "tool":
		s.handleTool(step)
	}
	return nil
}

func (s *agyStream) handleCandidateStep(step agyStreamStep) error {
	if step.State != "ACTIVE" && step.State != "DONE" {
		return fmt.Errorf("antigravity: review response step has an unsupported state")
	}
	index := *step.StepIndex
	if s.candidate == nil || index > s.candidate.index {
		s.candidate = &agyResponseStep{index: index}
	} else if index < s.candidate.index || s.candidate.done {
		return fmt.Errorf("antigravity: review response step is out of order or already complete")
	}
	s.candidate.text.WriteString(step.TextDelta)
	s.candidate.done = step.State == "DONE"
	return nil
}

func (s *agyStream) handleCheckpointStep(step agyStreamStep) error {
	if step.TextDelta != "" || step.ToolName != "" || step.ToolInfo != nil || step.SubagentInfo != nil {
		return fmt.Errorf("antigravity: review checkpoint contains response or tool payload; candidate withheld")
	}
	if step.State != "ACTIVE" && step.State != "DONE" {
		return fmt.Errorf("antigravity: review checkpoint has an unsupported state")
	}
	index := *step.StepIndex
	if index == s.lastStepIndex && s.lastStepType != "" {
		return fmt.Errorf("antigravity: review step index changed type")
	}
	if s.checkpoints == nil {
		s.checkpoints = make(map[int]bool)
	}
	if s.checkpoints[index] {
		return fmt.Errorf("antigravity: review checkpoint is already complete")
	}
	s.checkpoints[index] = step.State == "DONE"
	return nil
}

func (s *agyStream) reviewCandidate() (string, error) {
	// result.response can concatenate progress and several assistant messages.
	// Only the final complete response step identifies the bytes to review.
	if s.candidate == nil || !s.candidate.done || s.candidate.text.Len() == 0 ||
		s.candidate.index != s.lastStepIndex || s.lastStepType != "agent_response" {
		return "", fmt.Errorf("antigravity: final review response step is missing or incomplete; candidate withheld")
	}
	for _, done := range s.checkpoints {
		if !done {
			return "", fmt.Errorf("antigravity: review checkpoint is incomplete; candidate withheld")
		}
	}
	return s.candidate.text.String(), nil
}

func (s *agyStream) handleTool(step agyStreamStep) {
	if s.tools == nil {
		s.tools = make(map[int]bool)
	}
	info := step.ToolInfo
	if info == nil {
		info = &agyToolInfo{}
	}
	name := step.ToolName
	if name == "" {
		name = info.Name
	}
	index := 0
	if step.StepIndex != nil {
		index = *step.StepIndex
	}
	if !s.tools[index] {
		s.tools[index] = true
		s.session.emit(core.Event{Type: core.EventToolUse, ToolName: name,
			ToolInput: formatAgyToolInput(info.Parameters), ToolInputRaw: info.Parameters})
	}
	if step.State == "DONE" {
		output := info.Output
		status := "completed"
		if info.Error != nil {
			status = "failed"
			output = info.Error.Message
		}
		s.session.emit(core.Event{Type: core.EventToolResult, ToolName: name, ToolResult: output, Content: output, ToolStatus: status})
	}
}

func (as *antigravitySession) readLoop(run *agyRun, cmd *exec.Cmd, stdout io.ReadCloser, stderr *bytes.Buffer, tempFiles []string) {
	defer as.wg.Done()
	defer run.cancel()
	defer func() {
		for _, path := range tempFiles {
			_ = os.Remove(path)
		}
	}()
	readDone := make(chan struct{})
	defer close(readDone)
	go func() {
		select {
		case <-run.ctx.Done():
			_ = stdout.Close()
		case <-readDone:
		}
	}()
	stream := &agyStream{session: as, run: run}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	var streamErr error
	for scanner.Scan() {
		if streamErr = stream.handleLine(scanner.Bytes()); streamErr != nil {
			run.cancel()
			break
		}
	}
	if err := scanner.Err(); err != nil && streamErr == nil {
		streamErr = fmt.Errorf("antigravity: read stream: %w", err)
	}
	processErr := cmd.Wait()
	if streamErr == nil && processErr != nil {
		streamErr = fmt.Errorf("antigravity: process failed: %w", processErr)
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			streamErr = fmt.Errorf("%w: %s", streamErr, detail)
		}
	}
	result := agyResult{ConversationID: as.CurrentSessionID()}
	if stream.result != nil {
		result = *stream.result
		if len(as.reviewCommand) == 0 && result.Response == "" {
			result.Response = stream.response.String()
		}
	} else if streamErr == nil {
		streamErr = fmt.Errorf("antigravity: process ended without terminal result")
	}
	if streamErr == nil && result.Status != "SUCCESS" {
		streamErr = fmt.Errorf("antigravity: terminal status %s", result.Status)
		if result.Error != "" {
			streamErr = fmt.Errorf("%w: %s", streamErr, result.Error)
		}
	}
	if streamErr == nil && len(as.reviewCommand) > 0 {
		result.Response, streamErr = stream.reviewCandidate()
	}
	as.completeRun(run, result, streamErr)
}

// completeRun is the only path that can publish a candidate answer. A configured
// completion-review gate runs here before the terminal event is constructed.
func (as *antigravitySession) completeRun(run *agyRun, result agyResult, err error) {
	run.end(as.ctx.Err() != nil)
	run.mu.Lock()
	accepted := run.inputAccepted
	task := run.reviewTask
	run.mu.Unlock()
	if !accepted && err == nil {
		err = fmt.Errorf("antigravity: process ended before input acceptance")
	}
	if err == nil && len(as.reviewCommand) > 0 {
		if task == nil {
			err = fmt.Errorf("antigravity: review task is unavailable; candidate withheld")
		} else {
			err = as.checkReview(run.ctx, task, result.ConversationID, result.Response)
		}
	}
	run.acceptOnce.Do(func() { run.accepted <- err })
	as.startMu.Lock()
	if as.activeRun == run {
		as.activeRun = nil
	}
	as.startMu.Unlock()
	if err != nil {
		// Core's EventError path ends a turn while keeping this session alive.
		// EventResult.Error alone is not a completion gate in the engine.
		as.emit(core.Event{Type: core.EventError, SessionID: result.ConversationID, Error: err})
		return
	}
	event := core.Event{Type: core.EventResult, SessionID: result.ConversationID, Done: true,
		InputTokens: result.Usage.InputTokens, OutputTokens: result.Usage.OutputTokens, CacheReadInputTokens: result.Usage.CacheReadTokens}
	event.Content = result.Response
	as.emit(event)
}
