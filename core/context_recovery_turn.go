package core

import (
	"sync"
	"time"
)

// contextRecoveryTurn belongs to one invocation, unlike the durable recovery
// record that also fences later turns. Keep it until the next invocation so
// finishing a receipt cannot erase the current turn's recovery outcome.
type contextRecoveryTurn struct {
	id        string
	finished  bool
	queued    []queuedMessage // claimed inputs remain ours until delivery or failure
	callbacks []func(*ContextRecoveryRecord)
	receiptMu sync.Mutex // serializes durable receipts without holding Engine/state locks
}

// Caller holds s.mu. Every foreground/queued invocation starts here. The
// executor's cancellation function outlives a turn and is deliberately kept.
func (s *interactiveState) beginTurnLocked(p Platform, replyCtx any, invocation *recoveryInvocation, started time.Time, fromVoice bool, messageTimeMs int64) {
	if s.currentMessageID != invocation.MessageID {
		s.lastRecallProbeMessageID = ""
		s.lastRecallProbeAt = time.Time{}
		s.recallProbeInFlight = false
	}
	s.platform, s.replyCtx = p, replyCtx
	s.currentMessageID = invocation.MessageID
	s.currentInvocation, s.currentTurnStartedAt = invocation, started
	s.fromVoice, s.currentTurnUserMessageTimeMs = fromVoice, messageTimeMs
	s.sideText = ""
	s.recoveryTurn = nil
	s.recovering, s.recoveryInputPending = false, false
	s.recoveryInputFailure = nil
}

func (s *interactiveState) attemptedRecoveryThisTurn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoveryTurn != nil
}

// Caller holds s.mu. Claiming an input for a recovery bundle transfers its
// failure-notification responsibility; it does not make that input disappear.
func (s *interactiveState) takeQueuedMessagesLocked() []queuedMessage {
	var messages []queuedMessage
	if s.recoveryTurn != nil {
		messages = s.recoveryTurn.queued
		s.recoveryTurn.queued = nil
	}
	messages = append(messages, s.pendingMessages...)
	s.pendingMessages = nil
	return messages
}

func (s *interactiveState) completeRecoveryDelivery() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recoveryTurn != nil {
		s.recoveryTurn.queued = nil
	}
}
