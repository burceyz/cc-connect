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
	switch r.Phase {
	case "model_turn_completed", "cancelled":
		return false
	case "failed":
		return r.ErrorCode == "old_executor_unconfirmed" || r.ErrorCode == "recovery_persistence_failed" || r.InputStatus != "accepted"
	default:
		// A restart during preparation or send is not authorization to replay
		// the failed input. The saved material/receipt needs reconciliation.
		return true
	}
}

// recoveryTargetValid is checked around every side effect. interactiveMu is
// acquired before state.mu everywhere, matching /stop, /new and queueing.
func (e *Engine) recoveryTargetValid(state *interactiveState, session *Session, sessions *SessionManager, key, nativeID string, gen uint64) bool {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	if e.interactiveStates[key] != state {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return !state.stopped && sessions.ownsRecoverySession(state.logicalSessionKey, session, state.sideSession) && session.ownsRecoveryTurn(nativeID, gen)
}

func (e *Engine) publishContextRecovery(state *interactiveState, session *Session, sessions *SessionManager, id, phase, code string) error {
	record := session.updateContextRecovery(id, phase, code)
	if record == nil {
		return errors.New("context_recovery_record_changed")
	}
	if err := sessions.SaveWithError(); err != nil {
		return fmt.Errorf("persist context recovery: %w", err)
	}
	state.mu.Lock()
	callbacks := append([]func(*ContextRecoveryRecord){}, state.recoveryCallbacks...)
	state.mu.Unlock()
	for _, callback := range callbacks {
		callback(cloneContextRecovery(record))
	}
	return nil
}

func (e *Engine) finishContextRecoveryTurn(state *interactiveState, session *Session, sessions *SessionManager, phase, code string) error {
	state.mu.Lock()
	id := state.recoveryTurnID
	state.recoveryTurnID = ""
	state.mu.Unlock()
	if id == "" {
		return nil
	}
	if err := e.publishContextRecovery(state, session, sessions, id, phase, code); err != nil {
		slog.Error("context recovery final receipt failed", "recovery_id", id, "error", err)
		return errors.New("recovery_persistence_failed")
	}
	return nil
}

// processInteractiveEvents keeps normal event rendering unchanged. Only an
// adapter-classified failure can opt into rebuilding this same logical turn.
func (e *Engine) processInteractiveEvents(state *interactiveState, session *Session, sessions *SessionManager, key, msgID string, turnStart time.Time, stopTyping func(), sendDone <-chan error, replyCtx any, lockGen uint64) error {
	for {
		err := e.consumeInteractiveEvents(state, session, sessions, key, msgID, turnStart, stopTyping, sendDone, replyCtx, lockGen)
		stopTyping = nil // the consumer always releases its indicator
		if err == nil {
			return nil
		}
		if !isContextRecoveryError(err) {
			phase := "failed"
			if state.isStopped() || errors.Is(err, context.Canceled) {
				phase = "cancelled"
			}
			e.finishContextRecoveryTurn(state, session, sessions, phase, "recovery_turn_failed")
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
			if err := e.publishContextRecovery(state, session, sessions, record.ID, phase, recoveryErr.Error()); err != nil {
				slog.Error("context recovery failure receipt failed", "recovery_id", record.ID, "error", err)
			}
			state.mu.Lock()
			state.recoveryTurnID = ""
			state.mu.Unlock()
		} else {
			_ = e.finishContextRecoveryTurn(state, session, sessions, "failed", recoveryErr.Error())
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
	state.recoveryCallbacks = nil
	if invocation.OnRecovery != nil {
		state.recoveryCallbacks = append(state.recoveryCallbacks, invocation.OnRecovery)
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
	state.recoveryTurnID = id
	state.mu.Unlock()
	if err := sessions.SaveWithError(); err != nil {
		return nil, errors.New("recovery_persistence_failed")
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
			state.recoveryCallbacks = append(state.recoveryCallbacks, input.onRecovery)
		}
		state.mu.Unlock()
	}
	if !e.recoveryTargetValid(state, session, sessions, key, nativeID, gen) {
		return nil, context.Canceled
	}
	material := buildRecoveryMaterial(ctx, session, agent, invocation, record)
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
	if err := e.publishContextRecovery(state, session, sessions, id, "starting", ""); err != nil {
		return nil, errors.New("recovery_persistence_failed")
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

	// Commit the binding and the exact set of queued updates while commands
	// and queue insertion are fenced. No process call is made under these
	// locks; disk writes finish before the send goroutine is started.
	e.interactiveMu.Lock()
	state.mu.Lock()
	if e.interactiveStates[key] != state || state.stopped || !sessions.ownsRecoverySession(state.logicalSessionKey, session, state.sideSession) {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return nil, context.Canceled
	}
	queued := append([]queuedMessage(nil), state.pendingMessages...)
	for _, q := range queued {
		material.LaterInputs = append(material.LaterInputs, invocationForQueued(q))
	}
	// Attachments for later updates are supplied once to the fresh turn too.
	images, files := append([]ImageAttachment(nil), invocation.Images...), append([]FileAttachment(nil), invocation.Files...)
	for _, q := range queued {
		images = append(images, q.images...)
		files = append(files, q.files...)
	}
	material.Record.ToSessionID, material.Record.Phase = fresh.CurrentSessionID(), "ready"
	if err := saveRecoveryMaterial(material); err != nil {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return nil, errors.New("recovery_material_write_failed")
	}
	prompt := recoveryPrompt(material)
	if err := recoveryWriteFile(filepath.Join(record.ArtifactDir, "continuation.txt"), []byte(prompt), 0o600); err != nil {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return nil, errors.New("recovery_material_write_failed")
	}
	if err := session.bindContextRecovery(id, fresh.CurrentSessionID(), gen); err != nil {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return nil, err
	}
	for _, q := range queued {
		session.AddHistory("user", q.content)
	}
	state.pendingMessages = nil
	state.agentSession, state.startupError = fresh, nil
	state.eventsNeedResync = false
	session.updateContextRecovery(id, "sending", "")
	if err := sessions.SaveWithError(); err != nil {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return nil, errors.New("recovery_persistence_failed")
	}
	if err := ctx.Err(); err != nil {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return nil, err
	}
	timer.Stop() // ctx now owns the fresh session lifetime; /stop still cancels it
	state.recovering = false
	state.mu.Unlock()
	e.interactiveMu.Unlock()
	done := make(chan error, 1)
	recoveredSession := fresh
	go func() {
		if !e.recoveryTargetValid(state, session, sessions, key, recoveredSession.CurrentSessionID(), gen) {
			done <- context.Canceled
			return
		}
		err := recoveredSession.Send(prompt, "recovery-"+id, images, files)
		phase := "input_accepted"
		if err != nil {
			phase = "input_unknown"
		}
		saveErr := e.publishContextRecovery(state, session, sessions, id, phase, "")
		if saveErr != nil && err == nil {
			err = errors.New("recovery_persistence_failed")
		}
		completed := session.GetContextRecovery()
		if saveErr == nil && (err == nil || (completed != nil && completed.ID == id && completed.TurnStatus == "completed")) {
			state.mu.Lock()
			state.recoveryInputPending = false
			state.mu.Unlock()
		}
		done <- err
	}()
	return done, nil
}
