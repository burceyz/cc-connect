package core

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type recoverySwarmHistorySource struct {
	expectedID string
	marker     string
	err        error
	reads      atomic.Int32
}

func (s *recoverySwarmHistorySource) GetRecoveryHistory(_ context.Context, id string) (RecoveryHistory, error) {
	s.reads.Add(1)
	if id != s.expectedID {
		return RecoveryHistory{}, errors.New("history requested for a different execution context")
	}
	return RecoveryHistory{Entries: []RecoveryHistoryEntry{{Role: "assistant", Kind: "message", Content: s.marker}}}, s.err
}

type recoverySwarmHistorySession struct {
	*recoveryTestSession
	*recoverySwarmHistorySource
}

type recoverySwarmHistoryStartupError struct {
	*recoveryStartupFailure
	*recoverySwarmHistorySource
}

type recoverySwarmHistoryAgent struct {
	*recoveryTestAgent
	executor AgentSession
}

func (a *recoverySwarmHistoryAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.starts.Add(1)
	return a.executor, a.startErr
}

func TestContextRecoverySwarm_HistoryUsesFailedExecutorBeforeAgentDefaults(t *testing.T) {
	for _, startup := range []bool{false, true} {
		for _, exportFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("startup=%t/export-fails=%t", startup, exportFails), func(t *testing.T) {
				f := newRecoveryFixture(t)
				source := &recoverySwarmHistorySource{expectedID: f.old.id, marker: "visible evidence from original execution environment"}
				if exportFails {
					source.err = errors.New("partial export")
				}
				if startup {
					f.old.confirmed = true
					f.a.session = nil
					f.a.startErr = fmt.Errorf("resume failed: %w", &recoverySwarmHistoryStartupError{
						recoveryStartupFailure:     &recoveryStartupFailure{starter: f.old},
						recoverySwarmHistorySource: source,
					})
				} else {
					f.old.onSend = func(int, string) error { return recoveryTestFailure{} }
					f.e.agent = &recoverySwarmHistoryAgent{recoveryTestAgent: f.a, executor: &recoverySwarmHistorySession{f.old, source}}
				}
				if err := f.run("original", "finish the original task using its existing evidence"); err != nil {
					t.Fatal(err)
				}
				if source.reads.Load() != 1 || f.a.historyReads.Load() != 0 {
					t.Fatalf("history source reads: executor=%d shared defaults=%d", source.reads.Load(), f.a.historyReads.Load())
				}
				bundle := readRecoveryBundle(t, f.session)
				if len(bundle.NativeHistory.Entries) != 1 || bundle.NativeHistory.Entries[0].Content != source.marker {
					t.Fatalf("original execution evidence missing: %+v", bundle.NativeHistory)
				}
				// Native evidence stays in the filtered bundle; the prompt directs
				// the replacement executor there instead of duplicating raw logs.
				bundlePath := filepath.Join(bundle.Record.ArtifactDir, "visible-material.json")
				if prompts := f.fresh.sent(); len(prompts) != 1 || !strings.Contains(prompts[0], bundlePath) {
					t.Fatal("fresh invocation did not receive the original execution evidence locator")
				}
				if exportFails && !strings.Contains(strings.Join(bundle.Warnings, "\n"), "export is unavailable or incomplete") {
					t.Fatal("partial export did not preserve its evidence boundary")
				}
			})
		}
	}
}

func TestContextRecoverySwarm_IdleResetCannotBypassUnresolvedRecovery(t *testing.T) {
	f := newRecoveryFixture(t)
	f.e.SetResetOnIdle(time.Minute)
	f.session.mu.Lock()
	f.session.LastUserActivity = time.Now().Add(-time.Hour)
	f.session.ContextRecovery = &ContextRecoveryRecord{
		ID: "unconfirmed-executor", FromSessionID: f.old.id,
		Phase: "failed", ErrorCode: "old_executor_unconfirmed",
		InputStatus: "not_sent", TurnStatus: "failed",
	}
	f.session.mu.Unlock()
	f.e.sessions.Save()
	f.e.ReceiveMessage(f.p, f.message("later", "continue the same task"))
	recoveryAwait(t, "next input handled", func() bool {
		return len(f.p.getSent()) > 0 && !f.e.sessions.GetOrCreateActive(f.key).Busy()
	})
	if f.a.starts.Load() != 0 {
		t.Errorf("idle reset bypassed unresolved recovery and started %d executors", f.a.starts.Load())
	}
	current := f.e.sessions.GetOrCreateActive(f.key)
	if current != f.session || current.GetContextRecovery() == nil {
		t.Errorf("idle reset replaced the unresolved task: old=%s current=%s record=%+v", f.session.ID, current.ID, current.GetContextRecovery())
	}
	if strings.Contains(strings.Join(f.p.getSent(), "\n"), "task continued") {
		t.Fatal("unconfirmed task executed again after idle reset")
	}
}

func TestContextRecoverySwarm_ProviderAndRecoverySurviveBothSavePaths(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "ordinary_save"
		if durable {
			name = "recovery_save"
		}
		t.Run(name, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.session.SetActiveProvider("synthetic-selected-provider")
			f.session.mu.Lock()
			f.session.ContextRecovery = &ContextRecoveryRecord{
				ID: "spent-attempt", FromSessionID: f.old.id, ToSessionID: f.fresh.id,
				Phase: "model_turn_completed", InputStatus: "accepted", TurnStatus: "completed",
			}
			f.session.mu.Unlock()
			if durable {
				if err := f.e.sessions.SaveWithError(); err != nil {
					t.Fatal(err)
				}
			} else {
				f.e.sessions.Save()
			}
			loaded := NewSessionManager(f.e.sessions.StorePath()).GetOrCreateActive(f.key)
			if got := loaded.GetActiveProvider(); got != "synthetic-selected-provider" {
				t.Fatalf("provider changed across restart: %q", got)
			}
			if r := loaded.GetContextRecovery(); r == nil || r.ID != "spent-attempt" {
				t.Fatalf("recovery budget missing across restart: %+v", r)
			}
		})
	}
}
