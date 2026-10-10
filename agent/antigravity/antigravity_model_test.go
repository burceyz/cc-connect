package antigravity

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestConfiguredModels_BoundaryConditions(t *testing.T) {
	a := &Agent{
		providers: []core.ProviderConfig{
			{Models: []core.ModelOption{{Name: "first"}}},
			{Models: []core.ModelOption{{Name: "second"}}},
		},
	}

	tests := []struct {
		name      string
		activeIdx int
		wantNil   bool
		wantName  string
	}{
		{name: "negative index", activeIdx: -1, wantNil: true},
		{name: "out of range", activeIdx: 2, wantNil: true},
		{name: "valid index", activeIdx: 1, wantName: "second"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a.activeIdx = tt.activeIdx
			got := a.configuredModels()
			if tt.wantNil {
				if got != nil {
					t.Fatalf("configuredModels() = %v, want nil", got)
				}
				return
			}
			if len(got) != 1 || got[0].Name != tt.wantName {
				t.Fatalf("configuredModels() = %v, want %q", got, tt.wantName)
			}
		})
	}
}

func TestAgentFactoryPassesConfiguredModelEffortAndReviewToSession(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	setReviewMode(t, dir, "verified")
	agent, err := New(map[string]any{
		"cmd":      []any{os.Args[0], "-test.run=^TestAgyCLIHelper$", "--"},
		"work_dir": dir, "mode": "plan", "model": "fallback-model", "effort": "high", "timeout_mins": 1,
		"review_command": []any{os.Args[0], "-test.run=^TestAgyReviewHelper$", "--"},
		"env": map[string]any{"GO_WANT_AGY_CLI_HELPER": "1", "GO_WANT_AGY_REVIEW_HELPER": "1",
			"CC_TEST_AGY_DIR": dir, "CC_TEST_AGY_SCENARIO": "normal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := agent.(*Agent)
	a.SetProviders([]core.ProviderConfig{{Name: "selected", Model: "gemini-3.8-flash-high"}})
	if !a.SetActiveProvider("selected") || !a.SupportsLiveInput() {
		t.Fatal("configured agent capability/provider missing")
	}
	a.SetSessionEnv([]string{"CC_PROJECT=project", "CC_SESSION_KEY=channel:origin", "CC_IPC_ORIGIN=test-origin-handle", "CC_DATA_DIR=" + dir})
	started, err := a.StartSession(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer started.Close()
	if err := started.Send("factory request", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	event, streamed := terminalEvent(t, started.(*antigravitySession))
	if event.Type != core.EventResult || event.Content != testAgyCandidate || streamed != "" {
		t.Fatalf("factory omitted gate configuration: %+v, %q", event, streamed)
	}
	data, err := os.ReadFile(filepath.Join(dir, "args.json"))
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	if json.Unmarshal(data, &args) != nil || !contains(args, "gemini-3.8-flash-high") || !contains(args, "--effort") || !contains(args, "high") {
		t.Fatalf("factory model/effort not propagated: %v", args)
	}
	for _, filename := range []string{"cli-origin-env.json", "review-origin-env.json"} {
		data, err := os.ReadFile(filepath.Join(dir, filename))
		var values map[string]string
		if err != nil || json.Unmarshal(data, &values) != nil || values["origin"] != "test-origin-handle" ||
			values["project"] != "project" || values["session_key"] != "channel:origin" || values["data_dir"] != dir {
			t.Fatalf("host origin was not passed to %s: %s (%v)", filename, data, err)
		}
		if filename == "cli-origin-env.json" && (values["review_task"] == "" || values["review_dir"] == "" || values["review_manifest"] == "") {
			t.Fatal("CLI did not receive the fixed review task environment")
		}
	}
}

func TestGetModel_PrefersActiveProviderModel(t *testing.T) {
	a := &Agent{
		model: "gemini-2.5-flash",
		providers: []core.ProviderConfig{
			{Name: "google", Model: "gemini-2.5-pro"},
		},
		activeIdx: 0,
	}

	if got := a.GetModel(); got != "gemini-2.5-pro" {
		t.Fatalf("GetModel() = %q, want gemini-2.5-pro", got)
	}
}
