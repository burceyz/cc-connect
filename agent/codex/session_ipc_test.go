package codex

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

type ipcRPCWriter struct {
	session   *appServerSession
	requests  chan map[string]any
	replyTurn string
}

func (w *ipcRPCWriter) Close() error { return nil }
func (w *ipcRPCWriter) Write(b []byte) (int, error) {
	var req map[string]any
	if err := json.Unmarshal(b, &req); err != nil {
		return 0, err
	}
	w.requests <- req
	result, _ := json.Marshal(map[string]string{"turnId": w.replyTurn})
	w.session.handleResponse(rpcResponseEnvelope{ID: req["id"], Result: result})
	return len(b), nil
}

func TestIPCAppServer_SteerPinsThreadAndTurnWithoutStarting(t *testing.T) {
	s := &appServerSession{ctx: context.Background(), currentTurn: "turn-1", pending: make(map[int64]chan rpcResponseEnvelope)}
	s.threadID.Store("thread-1")
	s.alive.Store(true)
	w := &ipcRPCWriter{session: s, requests: make(chan map[string]any, 4), replyTurn: "turn-1"}
	s.stdin = w
	turn, err := s.SteerInput("thread-1", "event metadata")
	if err != nil || turn != "turn-1" {
		t.Fatal(turn, err)
	}
	req := <-w.requests
	p := req["params"].(map[string]any)
	if req["method"] != "turn/steer" || p["threadId"] != "thread-1" || p["expectedTurnId"] != "turn-1" {
		t.Fatalf("%+v", req)
	}
	for _, forbidden := range []string{"model", "cwd", "sandboxPolicy", "approvalPolicy", "effort"} {
		if _, exists := p[forbidden]; exists {
			t.Fatal("steer changed execution settings")
		}
	}
	if s.currentTurn != "turn-1" {
		t.Fatal("steer replaced the active turn")
	}
	if _, err := s.SteerInput("wrong-thread", "event"); err == nil {
		t.Fatal("wrong thread accepted")
	}
	s.currentTurn = ""
	if _, err := s.SteerInput("thread-1", "event"); !errors.Is(err, core.ErrNoActiveInput) {
		t.Fatal(err)
	}
	if len(w.requests) != 0 {
		t.Fatal("invalid steer wrote to backend")
	}
	s.currentTurn = "turn-1"
	w.replyTurn = "different-turn"
	if _, err := s.SteerInput("thread-1", "event"); err == nil {
		t.Fatal("mismatched acknowledgement accepted")
	}
}
