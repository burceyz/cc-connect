package core

import (
	"context"
	"time"
)

// RecoveryInvalidEncryptedContent identifies a rejected conversation context.
// Adapters must classify actual terminal errors, never ordinary assistant text.
const RecoveryInvalidEncryptedContent = "invalid_encrypted_content"

// ContextRecoveryError opts a specific backend failure into local recovery.
// Other errors keep the existing retry and reporting behavior.
type ContextRecoveryError interface {
	error
	RecoveryReason() string
}

// ContextRecoveryStarter creates a clean execution context with the original
// execution settings. Startup errors may implement this interface when the
// failed executor has already been confirmed stopped.
type ContextRecoveryStarter interface {
	StartFreshForRecovery(context.Context) (AgentSession, error)
}

// ContextRecoverableSession confirms that its old executor can no longer run
// the failed turn before starting a fresh context. Alive alone is insufficient.
type ContextRecoverableSession interface {
	ContextRecoveryStarter
	CloseForRecovery(context.Context) error
}

// RecoveryHistoryProvider exports visible evidence only, on demand after a
// failure. It is separate from the abbreviated history used for display.
type RecoveryHistoryProvider interface {
	GetRecoveryHistory(context.Context, string) (RecoveryHistory, error)
}

type RecoveryHistory struct {
	Source   string                 `json:"source,omitempty"`
	Entries  []RecoveryHistoryEntry `json:"entries"`
	Warnings []string               `json:"warnings,omitempty"`
}

// RecoveryHistoryEntry is a visible source item, not a reconstructed task
// status. A tool result can be missing, and a model statement is not a receipt.
type RecoveryHistoryEntry struct {
	Role      string    `json:"role"`
	Kind      string    `json:"kind,omitempty"`
	Content   string    `json:"content"`
	CallID    string    `json:"call_id,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Line      int       `json:"source_line,omitempty"`
}
