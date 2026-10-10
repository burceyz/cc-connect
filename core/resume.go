package core

// ResumeUnavailableError reports a failure that must not fall back to a new
// conversation. The saved session ID is still potentially valid, but cannot
// currently be resumed safely (for example, its workspace index is unreadable).
// Callers must preserve the ID so a later retry can recover the same history.
type ResumeUnavailableError struct {
	Err error
}

func (e *ResumeUnavailableError) Error() string {
	if e.Err == nil {
		return "saved conversation is temporarily unavailable"
	}
	return e.Err.Error()
}

func (e *ResumeUnavailableError) Unwrap() error { return e.Err }
