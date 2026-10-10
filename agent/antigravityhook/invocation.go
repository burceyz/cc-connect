package antigravityhook

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const (
	EnvInvocationAddress    = "CC_CONNECT_AGY_INVOCATION_ADDR"
	EnvInvocationToken      = "CC_CONNECT_AGY_INVOCATION_TOKEN"
	EnvInvocationGeneration = "CC_CONNECT_AGY_INVOCATION_GENERATION"
)

// InvocationInput deliberately excludes transcript paths and other fields that
// the bridge does not need. Hooks never read the conversation transcript.
type InvocationInput struct {
	ConversationID string `json:"conversationId"`
	InvocationNum  *int   `json:"invocationNum"`
	InitialSteps   *int   `json:"initialNumSteps"`
}

func (i InvocationInput) Valid() bool {
	return i.ConversationID != "" && len(i.ConversationID) <= 256 &&
		!strings.ContainsAny(i.ConversationID, "\r\n\x00") &&
		i.InvocationNum != nil && *i.InvocationNum >= 0 &&
		i.InitialSteps != nil && *i.InitialSteps >= 0
}

type InvocationRequest struct {
	Token      string          `json:"token"`
	Generation string          `json:"generation"`
	Phase      string          `json:"phase"`
	Input      InvocationInput `json:"input"`
}

// External inputs enter as user messages, never system/ephemeral messages or
// executable tool calls. The sender/reply metadata remains in the input text.
type InjectedStep struct {
	UserMessage string `json:"userMessage"`
}

type InvocationOutput struct {
	InjectSteps         []InjectedStep `json:"injectSteps,omitempty"`
	TerminationBehavior string         `json:"terminationBehavior,omitempty"`
}

type InvocationResponse struct {
	Output InvocationOutput `json:"output"`
	Error  string           `json:"error,omitempty"`
}

func RelayInvocation(in io.Reader, out io.Writer, address, token, generation, phase string) error {
	if token == "" || generation == "" {
		return fmt.Errorf("invocation bridge environment is missing")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("invocation bridge address must be loopback")
	}
	if phase != "pre" && phase != "post" {
		return fmt.Errorf("invalid invocation hook phase")
	}
	data, err := io.ReadAll(io.LimitReader(in, maxHookInput+1))
	if err != nil || len(data) > maxHookInput {
		return fmt.Errorf("read invocation hook input")
	}
	var input InvocationInput
	if json.Unmarshal(data, &input) != nil || !input.Valid() {
		return fmt.Errorf("invalid invocation hook input")
	}
	conn, err := net.DialTimeout("tcp", address, bridgeDialTimeout)
	if err != nil {
		return fmt.Errorf("connect invocation bridge: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	request := InvocationRequest{Token: token, Generation: generation, Phase: phase, Input: input}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return fmt.Errorf("send invocation request: %w", err)
	}
	var response InvocationResponse
	if err := json.NewDecoder(io.LimitReader(conn, 1<<20)).Decode(&response); err != nil {
		return fmt.Errorf("read invocation response: %w", err)
	}
	if response.Error != "" {
		return fmt.Errorf("invocation bridge rejected request")
	}
	if err := validateInvocationOutput(response.Output, phase); err != nil {
		return err
	}
	// Writing this JSON is a handoff, not proof that the CLI consumed a message.
	if err := json.NewEncoder(out).Encode(response.Output); err != nil {
		return fmt.Errorf("write invocation hook response: %w", err)
	}
	return nil
}

func validateInvocationOutput(output InvocationOutput, phase string) error {
	if len(output.InjectSteps) > 1 {
		return fmt.Errorf("invalid invocation injection count")
	}
	for _, step := range output.InjectSteps {
		if strings.TrimSpace(step.UserMessage) == "" || len(step.UserMessage) > 64<<10 {
			return fmt.Errorf("invalid invocation message")
		}
	}
	if output.TerminationBehavior != "" &&
		(output.TerminationBehavior != "force_continue" || phase != "post" || len(output.InjectSteps) == 0) {
		return fmt.Errorf("invalid invocation termination behavior")
	}
	return nil
}
