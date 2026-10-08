package codex

import (
	"fmt"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// SteerInput appends to the exact active turn; it never starts or resumes one.
func (s *appServerSession) SteerInput(expectedSessionID, prompt string) (string, error) {
	if s.CurrentSessionID() != expectedSessionID || !s.Alive() {
		return "", fmt.Errorf("ipc session identity mismatch or closed")
	}
	s.stateMu.Lock()
	turnID := s.currentTurn
	s.stateMu.Unlock()
	if turnID == "" {
		return "", core.ErrNoActiveInput // nothing has been written to the transport
	}
	params := map[string]any{
		"threadId": expectedSessionID, "expectedTurnId": turnID,
		"input": []map[string]any{{"type": "text", "text": prompt, "text_elements": []any{}}},
	}
	var reply struct {
		TurnID string `json:"turnId"`
	}
	if err := s.requestWithTimeout("turn/steer", params, &reply, 10*time.Second); err != nil {
		return "", err // ambiguous; the IPC receiver freezes this event
	}
	if reply.TurnID != turnID {
		return "", fmt.Errorf("ipc steer turn acknowledgement mismatch")
	}
	return turnID, nil
}
