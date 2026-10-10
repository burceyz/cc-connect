package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

func TestAntigravityInvocationCommandDispatchesBeforeNormalStartup(t *testing.T) {
	if os.Getenv("GO_WANT_AGY_COMMAND_HELPER") == "1" {
		os.Args = []string{"cc-connect", "_agy-invocation-hook", "pre"}
		main()
		os.Exit(0)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestAntigravityInvocationCommandDispatchesBeforeNormalStartup$")
	command.Env = core.MergeEnv(os.Environ(), []string{
		"GO_WANT_AGY_COMMAND_HELPER=1",
		antigravityhook.EnvInvocationAddress + "=",
		antigravityhook.EnvInvocationToken + "=",
		antigravityhook.EnvInvocationGeneration + "=",
	})
	command.Stdin = strings.NewReader(`{"conversationId":"test","invocationNum":0,"initialNumSteps":1}`)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if json.Unmarshal(stdout.Bytes(), &output) != nil || len(output) != 0 || strings.TrimSpace(stdout.String()) != "{}" {
		t.Fatalf("internal hook stdout was polluted by normal CLI startup: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "invocation hook") {
		t.Fatalf("missing safe hook diagnostic: %q", stderr.String())
	}
}
