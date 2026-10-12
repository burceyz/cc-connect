package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// Recovery is an adapter capability. The engine owns the attempt budget,
// handoff evidence, binding changes, and the decision to send another input.
var _ core.ContextRecoverableSession = (*appServerSession)(nil)

type contextRecoveryError struct {
	cause error
}

func (e *contextRecoveryError) Error() string { return e.cause.Error() }
func (e *contextRecoveryError) Unwrap() error { return e.cause }
func (e *contextRecoveryError) RecoveryReason() string {
	return core.RecoveryInvalidEncryptedContent
}

// Only a startup failure whose executor has been confirmed stopped exposes a
// starter. A failed close keeps the typed error but never exposes this method.
type contextRecoveryStartupError struct {
	err     error
	session *appServerSession
}

func (e *contextRecoveryStartupError) Error() string { return e.err.Error() }
func (e *contextRecoveryStartupError) Unwrap() error { return e.err }
func (e *contextRecoveryStartupError) RecoveryReason() string {
	return core.RecoveryInvalidEncryptedContent
}
func (e *contextRecoveryStartupError) StartFreshForRecovery(ctx context.Context) (core.AgentSession, error) {
	return e.session.StartFreshForRecovery(ctx)
}

// These are error fields, not arbitrary model output. The code can arrive in
// an RPC error's data or in a complete JSON error body inside message/details.
type appServerTurnError struct {
	Message           string          `json:"message"`
	Code              json.RawMessage `json:"code"`
	AdditionalDetails *string         `json:"additionalDetails"`
}

func invalidEncryptedErrorBody(raw json.RawMessage) bool {
	var body struct {
		Code  string          `json:"code"`
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return false
	}
	if body.Code == core.RecoveryInvalidEncryptedContent {
		return true
	}
	// Recognize the error envelope, never search arbitrary fields
	// or substrings (a quoted error in an unrelated failure is not evidence).
	var nested struct {
		Code string `json:"code"`
	}
	return json.Unmarshal(body.Error, &nested) == nil && nested.Code == core.RecoveryInvalidEncryptedContent
}

func classifyContextRecoveryError(message string, details ...json.RawMessage) error {
	err := errors.New(strings.TrimSpace(message))
	if invalidEncryptedErrorBody(json.RawMessage(message)) {
		return &contextRecoveryError{cause: err}
	}
	for _, detail := range details {
		if invalidEncryptedErrorBody(detail) {
			return &contextRecoveryError{cause: err}
		}
	}
	return err
}

func (s *appServerSession) rpcFailure(method string, failure *rpcError) error {
	if s.contextRecovery && (method == "turn/start" || method == "thread/resume") {
		return classifyContextRecoveryError(failure.Message, failure.Data)
	}
	return errors.New(strings.TrimSpace(failure.Message))
}

func (s *appServerSession) terminalFailure(failure *appServerTurnError, terminal bool) error {
	message := "turn failed (no details)"
	if failure != nil && strings.TrimSpace(failure.Message) != "" {
		message = strings.TrimSpace(failure.Message)
	}
	if !s.contextRecovery || !terminal || failure == nil {
		return errors.New(message)
	}
	codeBody, _ := json.Marshal(struct {
		Code json.RawMessage `json:"code"`
	}{failure.Code})
	var additional json.RawMessage
	if failure.AdditionalDetails != nil {
		additional = json.RawMessage(*failure.AdditionalDetails)
	}
	return classifyContextRecoveryError(message, codeBody, additional)
}

const appServerCloseTimeout = 2 * time.Second

// CloseForRecovery requires both process reaping and reader shutdown. Alive()
// turning false (or a successful signal) alone does not prove either property.
func (s *appServerSession) CloseForRecovery(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.contextRecovery {
		return errors.New("codex context recovery is disabled")
	}
	if err := s.waitForStop(ctx); err != nil {
		return err
	}
	s.procMu.Lock()
	processDone := s.processDone
	s.procMu.Unlock()
	if processDone == nil {
		return errors.New("codex app-server: process exit cannot be confirmed")
	}
	select {
	case <-processDone:
	case <-ctx.Done():
		return fmt.Errorf("codex app-server: waiting for process exit: %w", ctx.Err())
	}
	s.procMu.Lock()
	confirmed, waitErr := s.processExitConfirmed, s.processWaitErr
	s.procMu.Unlock()
	if !confirmed {
		if waitErr == nil {
			waitErr = errors.New("no process wait status")
		}
		return fmt.Errorf("codex app-server: process exit unconfirmed: %w", waitErr)
	}
	select {
	case <-s.workersDone():
	case <-ctx.Done():
		return fmt.Errorf("codex app-server: waiting for transport shutdown: %w", ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.recoveryClosed.Store(true)
	return nil
}

func (s *appServerSession) StartFreshForRecovery(ctx context.Context) (core.AgentSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.contextRecovery || !s.recoveryClosed.Load() {
		return nil, errors.New("codex app-server: recovery requires confirmed executor shutdown")
	}
	if !s.recoveryStarted.CompareAndSwap(false, true) {
		return nil, errors.New("codex app-server: recovery starter already used")
	}
	opts := s.recoveryOptions()
	fresh, err := newAppServerSession(ctx, opts, "")
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, fresh.Close())
	}
	if (opts.model != "" && fresh.GetModel() != opts.model) ||
		(opts.effort != "" && fresh.GetReasoningEffort() != opts.effort) ||
		!sameRecoveryDirectory(fresh.GetWorkDir(), opts.workDir) ||
		fresh.CurrentSessionID() == s.CurrentSessionID() {
		closeErr := fresh.Close()
		return nil, errors.Join(errors.New("codex app-server: fresh context changed execution settings"), closeErr)
	}
	return fresh, nil
}

func sameRecoveryDirectory(left, right string) bool {
	left, err := filepath.EvalSymlinks(left)
	if err != nil {
		return false
	}
	right, err = filepath.EvalSymlinks(right)
	if err != nil {
		return false
	}
	left, err = filepath.Abs(left)
	if err != nil {
		return false
	}
	right, err = filepath.Abs(right)
	return err == nil && left == right
}

func (s *appServerSession) recoveryOptions() appServerSessionOptions {
	s.runtimeMu.RLock()
	opts := appServerSessionOptions{
		cliBin: s.cliBin, cliExtraArgs: append([]string(nil), s.cliExtraArgs...),
		url: s.url, workDir: s.workDir, model: s.model, effort: s.effort, mode: s.mode,
		baseURL: s.baseURL, modelProvider: s.modelProvider,
		extraEnv: append([]string(nil), s.extraEnv...), codexHome: s.codexHome,
		promptPreamble: s.promptPreamble, contextRecovery: s.contextRecovery,
	}
	s.runtimeMu.RUnlock()
	s.procMu.Lock()
	opts.processEnv = append([]string(nil), s.processEnv...)
	s.procMu.Unlock()
	return opts
}

func (s *appServerSession) closeFailedStartup(err error, resumeID string) error {
	var recoverable core.ContextRecoveryError
	if !s.contextRecovery || resumeID == "" || resumeID == core.ContinueSession || !errors.As(err, &recoverable) {
		return errors.Join(err, s.Close())
	}
	ctx, cancel := context.WithTimeout(context.Background(), appServerCloseTimeout)
	defer cancel()
	if closeErr := s.CloseForRecovery(ctx); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return &contextRecoveryStartupError{err: err, session: s}
}
