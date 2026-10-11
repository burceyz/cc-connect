package codex

import (
	"encoding/json"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestAppServerSession_SubagentNotificationsDoNotCompleteParentTurn(t *testing.T) {
	s := &appServerSession{
		events:      make(chan core.Event, 16),
		currentTurn: "parent-turn",
		pendingMsgs: []string{"Parent result"},
	}
	s.threadID.Store("parent-thread")

	for _, notification := range []struct {
		method string
		params string
	}{
		{"turn/started", `{"threadId":"child-thread","turn":{"id":"child-turn"}}`},
		{"item/completed", `{"threadId":"child-thread","turnId":"child-turn","item":{"type":"agentMessage","text":"Child result"}}`},
		{"turn/completed", `{"threadId":"child-thread","turn":{"id":"child-turn","status":"completed"}}`},
		{"thread/status/changed", `{"threadId":"child-thread","status":{"type":"idle"}}`},
	} {
		s.handleNotification(notification.method, json.RawMessage(notification.params))
	}
	select {
	case event := <-s.events:
		t.Fatalf("subagent notification reached parent event stream: %#v", event)
	default:
	}
	if s.currentTurn != "parent-turn" {
		t.Fatalf("current turn = %q, want parent-turn", s.currentTurn)
	}

	s.handleNotification("turn/completed", json.RawMessage(`{"threadId":"parent-thread","turn":{"id":"parent-turn","status":"completed"}}`))
	textEvent := <-s.events
	if textEvent.Type != core.EventText || textEvent.Content != "Parent result" {
		t.Fatalf("parent text event = %#v", textEvent)
	}
	resultEvent := <-s.events
	if resultEvent.Type != core.EventResult || !resultEvent.Done || resultEvent.SessionID != "parent-thread" {
		t.Fatalf("parent completion event = %#v", resultEvent)
	}
	s.handleNotification("thread/status/changed", json.RawMessage(`{"threadId":"parent-thread","status":{"type":"idle"}}`))
	select {
	case event := <-s.events:
		t.Fatalf("duplicate parent completion: %#v", event)
	default:
	}
}

func TestAppServerSession_ForeignThreadCannotChangeParentState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		params string
	}{
		{"started", "turn/started", `{"threadId":"other","turn":{"id":"other-turn"}}`},
		{"tool started", "item/started", `{"threadId":"other","item":{"type":"commandExecution","command":"echo child"}}`},
		{"tool completed", "item/completed", `{"threadId":"other","item":{"type":"commandExecution","command":"echo child","status":"completed","exitCode":0}}`},
		{"message", "item/completed", `{"threadId":"other","item":{"type":"agentMessage","text":"Other answer"}}`},
		{"completed", "turn/completed", `{"threadId":"other","turn":{"id":"other-turn","status":"completed"}}`},
		{"failed", "turn/completed", `{"threadId":"other","turn":{"id":"other-turn","status":"failed","error":{"message":"child failed"}}}`},
		{"idle", "thread/status/changed", `{"threadId":"other","status":{"type":"idle"}}`},
		{"usage", "thread/tokenUsage/updated", `{"threadId":"other","tokenUsage":{"last":{"totalTokens":500},"modelContextWindow":1000}}`},
		{"error", "error", `{"threadId":"other","message":"child error"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &appServerSession{
				events:      make(chan core.Event, 16),
				currentTurn: "parent-turn",
				pendingMsgs: []string{"Parent answer"},
			}
			s.threadID.Store("parent")
			s.storeContextUsage(&core.ContextUsage{UsedTokens: 1234})
			s.handleNotification(tc.method, json.RawMessage(tc.params))
			if s.currentTurn != "parent-turn" || len(s.pendingMsgs) != 1 || s.pendingMsgs[0] != "Parent answer" {
				t.Fatalf("foreign notification changed parent state: turn=%q messages=%q", s.currentTurn, s.pendingMsgs)
			}
			if usage := s.GetContextUsage(); usage == nil || usage.UsedTokens != 1234 {
				t.Fatalf("foreign notification changed parent usage: %#v", usage)
			}
			select {
			case event := <-s.events:
				t.Fatalf("foreign notification emitted parent event: %#v", event)
			default:
			}
		})
	}
}
