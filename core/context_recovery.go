package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"
)

const contextRecoveryPrepareTimeout = 90 * time.Second

var (
	errContextRecoveryPersistence = errors.New("recovery_persistence_failed")
	errContextRecoveryReview      = errors.New("context_recovery_interrupted_review_required")
)

func (e *Engine) contextRecoveryReviewMessage(record *ContextRecoveryRecord, storePath string) string {
	reason := MsgRecoveryReviewIncomplete
	switch {
	case record.ErrorCode == "old_executor_unconfirmed":
		reason = MsgRecoveryReviewExecutor
	case record.ErrorCode == errContextRecoveryPersistence.Error():
		reason = MsgRecoveryReviewPersistence
	case record.InputStatus == "unknown" || record.InputStatus == "sending":
		reason = MsgRecoveryReviewInput
	}
	return e.i18n.Tf(MsgRecoveryReviewRequired, e.i18n.T(reason), record.ID, storePath, record.ArtifactDir)
}

// The executor can change during this invocation. Restore a temporary mode on
// the current executor as well as the original one, never on shared defaults.
func restoreInvocationMode(state *interactiveState, agent Agent) func() {
	state.mu.Lock()
	original := state.agentSession
	state.mu.Unlock()
	mode := "default"
	if getter, ok := agent.(interface{ GetMode() string }); ok && getter.GetMode() != "" {
		mode = getter.GetMode()
	}
	if getter, ok := original.(interface{ GetMode() string }); ok && getter.GetMode() != "" {
		mode = getter.GetMode()
	}
	return func() {
		if switcher, ok := original.(LiveModeSwitcher); ok {
			switcher.SetLiveMode(mode)
		}
		state.mu.Lock()
		current := state.agentSession
		state.mu.Unlock()
		if current != original {
			if switcher, ok := current.(LiveModeSwitcher); ok {
				switcher.SetLiveMode(mode)
			}
		}
	}
}

func isContextRecoveryError(err error) bool {
	var recoveryErr ContextRecoveryError
	return errors.As(err, &recoveryErr) && recoveryErr.RecoveryReason() == RecoveryInvalidEncryptedContent
}

func recoveryNeedsReview(r *ContextRecoveryRecord) bool {
	if r.ErrorCode == "old_executor_unconfirmed" || r.ErrorCode == errContextRecoveryPersistence.Error() {
		return true
	}
	switch r.Phase {
	case "model_turn_completed":
		return false
	case "cancelled":
		return r.InputStatus == "unknown" || r.InputStatus == "sending"
	case "failed":
		return r.InputStatus != "accepted"
	default:
		// A restart during preparation or send is not authorization to replay
		// the failed input. The saved material/receipt needs reconciliation.
		return true
	}
}

// All paths that begin another invocation share this check, including queues
// picked up after the original foreground goroutine has already returned.
func sessionInputError(session *Session, sessions *SessionManager) error {
	if err := sessions.LoadError(); err != nil {
		return err
	}
	if record := session.GetContextRecovery(); record != nil && recoveryNeedsReview(record) {
		return errContextRecoveryReview
	}
	return nil
}

func (e *Engine) notifyRejectedQueuedInput(state *interactiveState, session *Session, sessions *SessionManager, err error) {
	text := e.i18n.Tf(MsgSessionStoreUnavailable, err)
	if record := session.GetContextRecovery(); errors.Is(err, errContextRecoveryReview) && record != nil {
		text = e.contextRecoveryReviewMessage(record, sessions.StorePath())
	}
	e.notifyQueuedMessages(state, text)
}

// recoveryTargetValid is checked around every side effect. interactiveMu is
// acquired before state.mu everywhere, matching /stop, /new and queueing.
func (e *Engine) recoveryTargetValid(state *interactiveState, session *Session, sessions *SessionManager, key, nativeID string, gen uint64) bool {
	state.mu.Lock()
	logicalKey, side := state.logicalSessionKey, state.sideSession
	state.mu.Unlock()
	// Session-manager writers may be waiting for disk. Do not wait for them
	// while holding the engine-wide lock. Host selection commands invalidate
	// the interactive state before changing the selected logical session.
	if !sessions.ownsRecoverySession(logicalKey, session, side) {
		return false
	}
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	if e.interactiveStates[key] != state {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return !state.stopped && session.ownsRecoveryTurn(nativeID, gen)
}

func (e *Engine) publishContextRecovery(state *interactiveState, session *Session, sessions *SessionManager, turn *contextRecoveryTurn, phase, code string) error {
	// Serialize receipt updates and persistence so a late input receipt cannot
	// clear a failed durable write recorded by the completion path, or vice versa.
	turn.receiptMu.Lock()
	defer turn.receiptMu.Unlock()
	id := turn.id
	if previous := session.GetContextRecovery(); previous != nil && previous.ID == id && previous.ErrorCode == errContextRecoveryPersistence.Error() {
		code = errContextRecoveryPersistence.Error()
	}
	record := session.updateContextRecovery(id, phase, code)
	if record == nil {
		return errors.New("context_recovery_record_changed")
	}
	if err := sessions.SaveWithError(); err != nil {
		// Keep model completion evidence intact while fencing future inputs in
		// this process. The prior on-disk in-flight receipt also fences restart.
		session.mu.Lock()
		if current := session.ContextRecovery; current != nil && current.ID == id {
			current.ErrorCode = errContextRecoveryPersistence.Error()
			current.UpdatedAt = time.Now().UTC()
		}
		session.mu.Unlock()
		return fmt.Errorf("%w: %v", errContextRecoveryPersistence, err)
	}
	state.mu.Lock()
	callbacks := append([]func(*ContextRecoveryRecord){}, turn.callbacks...)
	state.mu.Unlock()
	for _, callback := range callbacks {
		callback(cloneContextRecovery(record))
	}
	return nil
}

func (e *Engine) finishContextRecoveryTurn(state *interactiveState, session *Session, sessions *SessionManager, phase, code string) error {
	state.mu.Lock()
	turn := state.recoveryTurn
	if turn == nil || turn.id == "" || turn.finished {
		state.mu.Unlock()
		return nil
	}
	turn.finished = true
	state.mu.Unlock()
	if err := e.publishContextRecovery(state, session, sessions, turn, phase, code); err != nil {
		slog.Error("context recovery final receipt failed", "recovery_id", turn.id, "error", err)
		return errContextRecoveryPersistence
	}
	return nil
}

// processInteractiveEvents keeps normal event rendering unchanged. Only an
// adapter-classified failure can opt into rebuilding this same logical turn.
func (e *Engine) processInteractiveEvents(state *interactiveState, session *Session, sessions *SessionManager, key, msgID string, turnStart time.Time, stopTyping func(), sendDone <-chan error, replyCtx any, lockGen uint64) error {
	for {
		err := e.consumeInteractiveEvents(state, session, sessions, key, msgID, turnStart, stopTyping, sendDone, replyCtx, lockGen)
		stopTyping = nil // the consumer always releases its indicator
		// EventResult can arrive before Send returns. Preserve a later durable
		// receipt failure even when the consumer already saw model completion.
		if record := session.GetContextRecovery(); record != nil && record.ErrorCode == errContextRecoveryPersistence.Error() {
			err = errContextRecoveryPersistence
		}
		if err == nil {
			return nil
		}
		if !isContextRecoveryError(err) {
			phase := "failed"
			if errors.Is(err, context.Canceled) {
				phase = "cancelled"
			}
			code := "recovery_turn_failed"
			if errors.Is(err, errContextRecoveryPersistence) {
				code = errContextRecoveryPersistence.Error()
			}
			if finishErr := e.finishContextRecoveryTurn(state, session, sessions, phase, code); finishErr != nil {
				return finishErr
			}
			return err
		}
		sendDone, err = e.prepareContextRecovery(state, session, sessions, key, lockGen, err)
		if err != nil {
			return err
		}
		state.mu.Lock()
		msgID, replyCtx, turnStart = state.currentMessageID, state.replyCtx, state.currentTurnStartedAt
		state.mu.Unlock()
	}
}

func (e *Engine) prepareContextRecovery(state *interactiveState, session *Session, sessions *SessionManager, key string, gen uint64, cause error) (sendDone <-chan error, recoveryErr error) {
	if !isContextRecoveryError(cause) {
		return nil, cause
	}
	state.mu.Lock()
	old, invocation, agent := state.agentSession, state.currentInvocation, state.agent
	platform, replyCtx, turnStart := state.platform, state.replyCtx, state.currentTurnStartedAt
	state.eventsNeedResync = true
	if state.recoveryTurn == nil {
		state.recoveryTurn = &contextRecoveryTurn{}
	}
	turn := state.recoveryTurn
	state.mu.Unlock()
	if agent == nil {
		agent = e.agent
	}

	// No implicit fresh fallback and no best-effort attempt without a durable
	// once-only ledger. Any refusal is returned to the original trigger.
	var record *ContextRecoveryRecord
	var fresh AgentSession
	defer func() {
		if recoveryErr == nil {
			return
		}
		state.mu.Lock()
		cancel := state.recoveryCancel
		state.recovering = false
		state.recoveryInputPending = false
		state.mu.Unlock()
		if record != nil {
			if cancel != nil {
				cancel()
			}
			phase := "failed"
			if errors.Is(recoveryErr, context.Canceled) || state.isStopped() {
				phase = "cancelled"
			}
			if err := e.publishContextRecovery(state, session, sessions, turn, phase, recoveryErr.Error()); err != nil {
				slog.Error("context recovery failure receipt failed", "recovery_id", record.ID, "error", err)
				if errors.Is(err, errContextRecoveryPersistence) {
					recoveryErr = errContextRecoveryPersistence
				}
			}
			state.mu.Lock()
			turn.finished = true
			state.mu.Unlock()
		} else {
			if err := e.finishContextRecoveryTurn(state, session, sessions, "failed", recoveryErr.Error()); err != nil {
				recoveryErr = err
			}
		}
		if fresh != nil {
			_ = fresh.Close()
		}
		if !state.isStopped() {
			e.send(platform, replyCtx, e.i18n.Tf(MsgContextRecoveryFailed, recoveryErr))
		}
	}()
	if invocation == nil || session.GetAgentSessionID() == "" {
		return nil, errors.New("context_recovery_source_missing")
	}
	if sessions.StorePath() == "" {
		return nil, errors.New("context_recovery_persistence_required")
	}
	if session.GetContextRecovery() != nil {
		return nil, errors.New("context_recovery_budget_exhausted")
	}
	nativeID := session.GetAgentSessionID()
	if !e.recoveryTargetValid(state, session, sessions, key, nativeID, gen) {
		return nil, context.Canceled
	}
	ctx, cancel := context.WithCancel(e.ctx)
	prepareLimit := contextRecoveryPrepareTimeout
	if e.maxTurnTime > 0 {
		remaining := time.Until(turnStart.Add(e.maxTurnTime))
		if remaining <= 0 {
			cancel()
			return nil, context.DeadlineExceeded
		}
		if remaining < prepareLimit {
			prepareLimit = remaining
		}
	}
	timer := time.AfterFunc(prepareLimit, cancel)
	defer timer.Stop()
	state.mu.Lock()
	if state.stopped {
		state.mu.Unlock()
		cancel()
		return nil, context.Canceled
	}
	state.recovering, state.recoveryCancel = true, cancel
	state.recoveryInputPending = true
	if invocation.OnRecovery != nil {
		turn.callbacks = append(turn.callbacks, invocation.OnRecovery)
	}
	inputs := append([]*recoverySteeredInput(nil), state.recoveryInputs...)
	droppedInputs := state.recoveryInputsDropped
	state.mu.Unlock()
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		cancel()
		return nil, errors.New("context_recovery_identity_failed")
	}
	id := hex.EncodeToString(buf)
	digest := sha256.Sum256([]byte(invocation.Content))
	materialRoot, err := filepath.Abs(filepath.Join(filepath.Dir(sessions.StorePath()), "context-recovery"))
	if err != nil {
		return nil, errors.New("recovery_material_path_invalid")
	}
	now := time.Now().UTC()
	record = &ContextRecoveryRecord{ID: id, Reason: RecoveryInvalidEncryptedContent, Phase: "reserved",
		FromSessionID: nativeID, MessageID: invocation.MessageID, InputSHA256: hex.EncodeToString(digest[:]), Origin: invocation.Origin,
		ArtifactDir: filepath.Join(materialRoot, id), StartedAt: now, UpdatedAt: now,
		InputStatus: "not_sent", TurnStatus: "not_started"}
	if err := session.reserveContextRecovery(record, gen); err != nil {
		return nil, err
	}
	state.mu.Lock()
	turn.id = id
	state.mu.Unlock()
	if err := sessions.SaveWithError(); err != nil {
		return nil, errContextRecoveryPersistence
	}
	// A failed startup without a starter may still own an unconfirmed
	// executor. Persist the attempt before refusing recovery so a later
	// message or restart cannot bypass that refusal through normal startup.
	var starter ContextRecoveryStarter
	var closer ContextRecoverableSession
	if old != nil {
		var ok bool
		closer, ok = old.(ContextRecoverableSession)
		if !ok {
			return nil, errors.New("context_recovery_unsupported")
		}
		starter = closer
	} else if !errors.As(cause, &starter) {
		return nil, errors.New("context_recovery_startup_unsupported")
	}
	e.send(platform, replyCtx, e.i18n.T(MsgContextRecovering))
	if closer != nil {
		if err := closer.CloseForRecovery(ctx); err != nil {
			return nil, errors.New("old_executor_unconfirmed")
		}
	}
	// Inputs already racing a failure are evidence even when their RPC result
	// is unknown. Closing the old executor settles their delivery attempt.
	for _, input := range inputs {
		select {
		case <-input.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		state.mu.Lock()
		if input.onRecovery != nil {
			turn.callbacks = append(turn.callbacks, input.onRecovery)
		}
		state.mu.Unlock()
	}
	if !e.recoveryTargetValid(state, session, sessions, key, nativeID, gen) {
		return nil, context.Canceled
	}
	// Use the failed executor's actual history location and working directory.
	// Shared agent defaults may have changed since that executor was started.
	var historySource any = agent
	if provider, ok := old.(RecoveryHistoryProvider); ok {
		historySource = provider
	} else if old == nil {
		var provider RecoveryHistoryProvider
		if errors.As(cause, &provider) {
			historySource = provider
		}
	}
	material := buildRecoveryMaterial(ctx, session, historySource, invocation, record)
	material.SteeredInputs = inputs
	if droppedInputs > 0 {
		material.Warnings = append(material.Warnings, fmt.Sprintf("%d earlier IPC attempts are not in the inline evidence; check their original durable receipts if relevant.", droppedInputs))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := saveRecoveryMaterial(material); err != nil {
		return nil, errors.New("recovery_material_write_failed")
	}
	if err := e.publishContextRecovery(state, session, sessions, turn, "starting", ""); err != nil {
		return nil, errContextRecoveryPersistence
	}
	fresh, err = starter.StartFreshForRecovery(ctx)
	if err != nil {
		return nil, errors.New("recovery_start_failed")
	}
	if fresh == nil || fresh.CurrentSessionID() == "" || fresh.CurrentSessionID() == nativeID {
		return nil, errors.New("recovery_fresh_identity_invalid")
	}
	if invocation.Mode != "" {
		if switcher, ok := fresh.(LiveModeSwitcher); ok && !switcher.SetLiveMode(invocation.Mode) {
			return nil, errors.New("recovery_mode_restore_failed")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !e.recoveryTargetValid(state, session, sessions, key, nativeID, gen) {
		return nil, context.Canceled
	}

	input, err := e.commitContextRecovery(ctx, state, session, sessions, key, gen, fresh, turn, material)
	if err != nil {
		return nil, err
	}
	timer.Stop() // ctx now owns the fresh session lifetime; /stop still cancels it
	done := make(chan error, 1)
	recoveredSession := fresh
	go func() {
		if ctx.Err() != nil || !e.recoveryTargetValid(state, session, sessions, key, recoveredSession.CurrentSessionID(), gen) {
			done <- context.Canceled
			return
		}
		err := recoveredSession.Send(input.prompt, "recovery-"+id, input.images, input.files)
		phase := "input_accepted"
		if err != nil {
			phase = "input_unknown"
		}
		saveErr := e.publishContextRecovery(state, session, sessions, turn, phase, "")
		if saveErr != nil {
			slog.Error("context recovery input receipt failed", "recovery_id", id, "error", saveErr)
			err = errContextRecoveryPersistence
		}
		completed := session.GetContextRecovery()
		if saveErr == nil && (err == nil || (completed != nil && completed.ID == id && completed.TurnStatus == "completed")) {
			state.mu.Lock()
			if state.recoveryTurn == turn {
				state.recoveryInputPending = false
			}
			state.mu.Unlock()
		}
		done <- err
	}()
	return done, nil
}

type preparedRecoveryInput struct {
	prompt string
	images []ImageAttachment
	files  []FileAttachment
}

// Build and persist outside Engine/state locks. If new input arrives while
// storage is busy, include it before the first Send instead of replaying it as
// another turn. The preparation deadline bounds a continuously growing queue.
func (e *Engine) commitContextRecovery(ctx context.Context, state *interactiveState, session *Session, sessions *SessionManager, key string, gen uint64, fresh AgentSession, turn *contextRecoveryTurn, material *contextRecoveryMaterial) (*preparedRecoveryInput, error) {
	nativeID := material.Record.FromSessionID
	freshID := fresh.CurrentSessionID()
	historyAdded := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !e.recoveryTargetValid(state, session, sessions, key, nativeID, gen) {
			return nil, context.Canceled
		}
		state.mu.Lock()
		turn.queued = append(turn.queued, state.pendingMessages...)
		state.pendingMessages = nil
		queued := append([]queuedMessage(nil), turn.queued...)
		state.mu.Unlock()

		input := &preparedRecoveryInput{
			images: append([]ImageAttachment(nil), material.Invocation.Images...),
			files:  append([]FileAttachment(nil), material.Invocation.Files...),
		}
		material.LaterInputs = nil
		for _, q := range queued {
			material.LaterInputs = append(material.LaterInputs, e.recoveryInvocationForQueued(q))
			input.images = append(input.images, q.images...)
			input.files = append(input.files, q.files...)
		}
		material.Record.ToSessionID, material.Record.Phase = freshID, "ready"
		if err := saveRecoveryMaterial(material); err != nil {
			return nil, errors.New("recovery_material_write_failed")
		}
		input.prompt = recoveryPrompt(material)
		if err := durableWriteFile(filepath.Join(material.Record.ArtifactDir, "continuation.txt"), []byte(input.prompt), 0o600); err != nil {
			return nil, errors.New("recovery_material_write_failed")
		}

		// Selection commands invalidate state before changing the session.
		// Only the short binding/history mutation uses the engine-wide lock.
		e.interactiveMu.Lock()
		state.mu.Lock()
		if ctx.Err() != nil || e.interactiveStates[key] != state || state.stopped || !session.ownsRecoveryTurn(nativeID, gen) {
			state.mu.Unlock()
			e.interactiveMu.Unlock()
			return nil, context.Canceled
		}
		if nativeID != freshID {
			if err := session.bindContextRecovery(turn.id, freshID, gen); err != nil {
				state.mu.Unlock()
				e.interactiveMu.Unlock()
				return nil, err
			}
			nativeID = freshID
			state.agentSession, state.startupError = fresh, nil
			state.eventsNeedResync = false
			session.updateContextRecovery(turn.id, "sending", "")
		}
		for _, q := range queued[historyAdded:] {
			session.AddHistory("user", q.content)
		}
		historyAdded = len(queued)
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		if err := sessions.SaveWithError(); err != nil {
			return nil, errContextRecoveryPersistence
		}
		if !e.recoveryTargetValid(state, session, sessions, key, freshID, gen) {
			return nil, context.Canceled
		}
		state.mu.Lock()
		if ctx.Err() != nil || state.stopped {
			state.mu.Unlock()
			return nil, context.Canceled
		}
		if len(state.pendingMessages) == 0 {
			state.recovering = false
			state.mu.Unlock()
			return input, nil
		}
		state.mu.Unlock()
	}
}
