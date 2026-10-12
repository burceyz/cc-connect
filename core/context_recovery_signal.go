package core

// A supplemental Send can fail without an EventError. Keep its failure tied
// to the executor and invocation that received it, so an old response cannot
// interrupt a replacement context or a later task turn.
type recoveryInputFailure struct {
	executor   AgentSession
	invocation *recoveryInvocation
	err        error
}

func (s *interactiveState) recoveryFailureSignal() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recoveryFailureReady == nil {
		s.recoveryFailureReady = make(chan struct{}, 1)
	}
	return s.recoveryFailureReady
}

// Caller holds s.mu. A stale notification is drained without changing the
// current invocation or interrupting its executor.
func (s *interactiveState) takeRecoveryInputFailureLocked() error {
	failure := s.recoveryInputFailure
	s.recoveryInputFailure = nil
	if failure == nil || s.stopped || s.agentSession != failure.executor || s.currentInvocation != failure.invocation {
		return nil
	}
	// Transfer the busy/queue fence while still holding the same mutex. The
	// caller may finalize a progress card or stop an idle reader before it can
	// enter preparation; that handoff must never look like an abandoned turn.
	s.recovering = true
	return failure.err
}

func (e *Engine) reportRecoveryInputFailure(state *interactiveState, session *Session, sessions *SessionManager, key string, executor AgentSession, invocation *recoveryInvocation, cause error) bool {
	if !isContextRecoveryError(cause) || invocation == nil {
		return false
	}
	e.interactiveMu.Lock()
	if e.interactiveStates[key] != state {
		e.interactiveMu.Unlock()
		return false
	}
	state.mu.Lock()
	if state.stopped || state.recovering || state.recoveryInputPending || state.agentSession != executor || state.currentInvocation != invocation {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return false
	}
	if state.recoveryFailureReady == nil {
		state.recoveryFailureReady = make(chan struct{}, 1)
	}
	// The event loop may have advanced to a queued invocation before consuming
	// the old notification. Keep the newly validated failure, not that stale slot.
	state.recoveryInputFailure = &recoveryInputFailure{executor: executor, invocation: invocation, err: cause}
	select {
	case state.recoveryFailureReady <- struct{}{}:
	default:
	}
	// Mirror normal queue insertion: if foreground already released the
	// busy lock, take ownership and drain this failure rather than losing it
	// in the gap between EventResult and the next user message.
	gen, acquired := session.TryLock()
	agent, workspaceDir := state.agent, state.workspaceDir
	state.mu.Unlock()
	e.interactiveMu.Unlock()
	if acquired {
		go e.drainOrphanedQueue(session, sessions, key, agent, workspaceDir, gen)
	}
	return true
}

func (e *Engine) drainRecoveryInputFailure(state *interactiveState, session *Session, sessions *SessionManager, key string, gen uint64, cause error) error {
	e.stopUnsolicitedReader(state)
	e.cancelAgentSessionIdleClose(state)
	sendDone, err := e.prepareContextRecovery(state, session, sessions, key, gen, cause)
	if err != nil {
		return err
	}
	state.mu.Lock()
	msgID, started, replyCtx := state.currentMessageID, state.currentTurnStartedAt, state.replyCtx
	state.mu.Unlock()
	if err := e.processInteractiveEvents(state, session, sessions, key, msgID, started, nil, sendDone, replyCtx, gen); err != nil {
		return err
	}
	state.mu.Lock()
	alive := state.agentSession != nil && state.agentSession.Alive() && !state.stopped && !state.eventsNeedResync
	workspaceDir := state.workspaceDir
	state.mu.Unlock()
	if alive {
		e.startUnsolicitedReader(state, session, sessions, key, workspaceDir)
		e.scheduleAgentSessionIdleClose(key, state)
	}
	return nil
}
