package antigravityhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
)

const validInvocationInput = `{"conversationId":"main","invocationNum":0,"initialNumSteps":1,"transcriptPath":"/private/must-not-be-forwarded"}`

func TestInvocationRelayRejectsInvalidInputWithoutConnecting(t *testing.T) {
	for _, test := range []struct{ input, address, token, generation, phase string }{
		{validInvocationInput, "203.0.113.10:1", "token", "run", "pre"},
		{validInvocationInput, "127.0.0.1:1", "", "run", "pre"},
		{validInvocationInput, "127.0.0.1:1", "token", "", "pre"},
		{validInvocationInput, "127.0.0.1:1", "token", "run", "stop"},
		{`{}`, "127.0.0.1:1", "token", "run", "pre"},
		{`{"conversationId":"main","invocationNum":-1,"initialNumSteps":1}`, "127.0.0.1:1", "token", "run", "pre"},
		{strings.Repeat(" ", maxHookInput+1), "127.0.0.1:1", "token", "run", "pre"},
	} {
		var output bytes.Buffer
		err := RelayInvocation(strings.NewReader(test.input), &output, test.address, test.token, test.generation, test.phase)
		if err == nil || strings.Contains(err.Error(), "connect invocation") || output.Len() > 0 {
			t.Fatalf("invalid input reached the network or stdout: %v, %q", err, output.String())
		}
	}
}

func invocationTestServer(t *testing.T, response string) (string, <-chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	requests := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var raw json.RawMessage
		if json.NewDecoder(conn).Decode(&raw) == nil {
			requests <- raw
		}
		_, _ = io.WriteString(conn, response)
	}()
	return listener.Addr().String(), requests
}

func TestInvocationRelayOnlyForwardsRequiredMetadataAndUserMessage(t *testing.T) {
	address, requests := invocationTestServer(t, `{"output":{"injectSteps":[{"userMessage":"sender=Atlas; reply=fixed","ephemeralMessage":"must not escape","toolCall":{"name":"bad"}}],"terminationBehavior":"force_continue"}}`)
	var output bytes.Buffer
	if err := RelayInvocation(strings.NewReader(validInvocationInput), &output, address, "token", "run", "post"); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if bytes.Contains(request, []byte("transcriptPath")) || bytes.Contains(request, []byte("/private/")) {
		t.Fatalf("unused private metadata was forwarded: %s", request)
	}
	var decoded map[string]any
	if json.Unmarshal(output.Bytes(), &decoded) != nil || strings.Contains(output.String(), "ephemeralMessage") || strings.Contains(output.String(), "toolCall") || !strings.Contains(output.String(), "sender=Atlas") {
		t.Fatalf("invalid output trust boundary: %s", output.String())
	}
}

func TestInvocationRelayRejectsUnsafeOrInvalidResponses(t *testing.T) {
	for _, response := range []string{
		`not-json`,
		`{"error":"rejected","output":{}}`,
		`{"output":{"injectSteps":[{"toolCall":{"name":"bad"}}]}}`,
		`{"output":{"injectSteps":[{"userMessage":"one"},{"userMessage":"two"}]}}`,
		`{"output":{"terminationBehavior":"force_continue"}}`,
		`{"output":{"injectSteps":[{"userMessage":"text"}],"terminationBehavior":"terminate"}}`,
	} {
		address, _ := invocationTestServer(t, response)
		var output bytes.Buffer
		err := RelayInvocation(strings.NewReader(validInvocationInput), &output, address, "token", "run", "post")
		if err == nil || output.Len() > 0 {
			t.Fatalf("invalid response reached stdout: %s (%v)", output.String(), err)
		}
	}
}

type failingHookOutput struct{}

func (failingHookOutput) Write([]byte) (int, error) { return 0, errors.New("stdout closed") }

func TestInvocationRelayReportsHandoffFailureWithoutAcknowledgingConsumption(t *testing.T) {
	address, requests := invocationTestServer(t, `{"output":{"injectSteps":[{"userMessage":"text"}]}}`)
	err := RelayInvocation(strings.NewReader(validInvocationInput), failingHookOutput{}, address, "token", "run", "pre")
	if err == nil || !strings.Contains(err.Error(), "write invocation hook response") {
		t.Fatalf("handoff failure = %v", err)
	}
	var request InvocationRequest
	if json.Unmarshal(<-requests, &request) != nil || request.Phase != "pre" || request.Generation != "run" {
		t.Fatalf("request = %+v", request)
	}
}
