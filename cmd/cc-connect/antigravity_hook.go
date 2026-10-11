package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
)

func runAntigravityPermissionHook() {
	err := antigravityhook.Relay(
		os.Stdin,
		os.Stdout,
		os.Getenv(antigravityhook.EnvAddress),
		os.Getenv(antigravityhook.EnvToken),
	)
	if err == nil {
		return
	}

	fmt.Fprintf(os.Stderr, "cc-connect Agy permission hook: %v\n", err)
	_ = json.NewEncoder(os.Stdout).Encode(antigravityhook.BridgeResponse{
		Decision: "deny",
		Reason:   "cc-connect permission bridge is unavailable",
	})
}

func runAntigravityInvocationHook(phase string) {
	err := antigravityhook.RelayInvocation(
		os.Stdin, os.Stdout,
		os.Getenv(antigravityhook.EnvInvocationAddress),
		os.Getenv(antigravityhook.EnvInvocationToken),
		os.Getenv(antigravityhook.EnvInvocationGeneration), phase,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc-connect Agy invocation hook: %v\n", err)
		_ = json.NewEncoder(os.Stdout).Encode(antigravityhook.InvocationOutput{})
	}
}

func runAntigravityCommunicationTool() {
	err := antigravityhook.ServeCommunication(os.Stdin, os.Stdout,
		os.Getenv(antigravityhook.EnvCommunicationAddress),
		os.Getenv(antigravityhook.EnvCommunicationToken),
		os.Getenv(antigravityhook.EnvCommunicationGeneration))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cc-connect communication tool stopped")
		os.Exit(1)
	}
}
