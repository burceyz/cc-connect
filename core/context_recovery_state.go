package core

import (
	"errors"
	"time"
)

// InvocationOrigin is supplied by the host that accepted an input. An ID is
// one trigger occurrence, not merely the ID of a recurring timer definition.
type InvocationOrigin struct {
	Kind        string    `json:"kind"`
	ID          string    `json:"id,omitempty"`
	JobID       string    `json:"job_id,omitempty"`
	ScheduledAt time.Time `json:"scheduled_at,omitempty"`
	TriggeredAt time.Time `json:"triggered_at,omitempty"`
}

// ContextRecoveryRecord follows the same logical host session across native
// contexts. Without an explicit new task boundary, its budget never resets on
// a new message, native ID, process restart, or successful recovery response.
type ContextRecoveryRecord struct {
	ID            string           `json:"id"`
	Reason        string           `json:"reason"`
	Phase         string           `json:"phase"`
	FromSessionID string           `json:"from_session_id"`
	ToSessionID   string           `json:"to_session_id,omitempty"`
	MessageID     string           `json:"message_id,omitempty"`
	InputSHA256   string           `json:"input_sha256"`
	Origin        InvocationOrigin `json:"origin"`
	ArtifactDir   string           `json:"artifact_dir"`
	StartedAt     time.Time        `json:"started_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
	ErrorCode     string           `json:"error_code,omitempty"`
	InputStatus   string           `json:"input_status"`
	TurnStatus    string           `json:"turn_status"`
}

func cloneContextRecovery(record *ContextRecoveryRecord) *ContextRecoveryRecord {
	if record == nil {
		return nil
	}
	copy := *record
	return &copy
}

func (s *Session) GetContextRecovery() *ContextRecoveryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneContextRecovery(s.ContextRecovery)
}

func (s *Session) ownsRecoveryTurn(nativeID string, generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.busy && s.lockGen == generation && s.AgentSessionID == nativeID
}

func (s *Session) reserveContextRecovery(record *ContextRecoveryRecord, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.busy || s.lockGen != generation || s.AgentSessionID != record.FromSessionID {
		return errors.New("context_recovery_target_changed")
	}
	if s.ContextRecovery != nil {
		return errors.New("context_recovery_budget_exhausted")
	}
	s.ContextRecovery = cloneContextRecovery(record)
	return nil
}

func (s *Session) updateContextRecovery(id, phase, code string) *ContextRecoveryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ContextRecovery == nil || s.ContextRecovery.ID != id {
		return nil
	}
	r := s.ContextRecovery
	terminal := r.Phase == "model_turn_completed" || r.Phase == "failed" || r.Phase == "cancelled"
	if !terminal || (phase != "input_accepted" && phase != "input_unknown") {
		r.Phase = phase
	}
	switch phase {
	case "sending":
		r.InputStatus, r.TurnStatus = "sending", "running"
	case "input_accepted":
		r.InputStatus = "accepted"
	case "input_unknown":
		if r.TurnStatus != "completed" {
			r.InputStatus = "unknown"
		}
	case "model_turn_completed":
		r.InputStatus, r.TurnStatus = "accepted", "completed"
	case "failed", "cancelled":
		r.TurnStatus = phase
		if r.InputStatus == "sending" {
			r.InputStatus = "unknown"
		}
	}
	if !terminal || (phase != "input_accepted" && phase != "input_unknown") {
		s.ContextRecovery.ErrorCode = code
	}
	s.ContextRecovery.UpdatedAt = time.Now().UTC()
	return cloneContextRecovery(s.ContextRecovery)
}

func (s *Session) bindContextRecovery(id, nativeID string, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ContextRecovery
	if r == nil || r.ID != id || !s.busy || s.lockGen != generation || s.AgentSessionID != r.FromSessionID {
		return errors.New("context_recovery_target_changed")
	}
	s.recordPastAgentSessionID()
	s.AgentSessionID = nativeID
	r.ToSessionID = nativeID
	r.Phase = "ready"
	r.UpdatedAt = time.Now().UTC()
	return nil
}

func (sm *SessionManager) isActiveSession(key string, session *Session) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[sm.activeSession[key]] == session
}

func (sm *SessionManager) ownsRecoverySession(key string, session *Session, side bool) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[session.ID] == session && (side || sm.sessions[sm.activeSession[key]] == session)
}

// invocation contains only the current accepted input. It is ordinary runtime
// bookkeeping; no task summary or model checkpoint is required on happy paths.
type recoveryInvocation struct {
	MessageID     string                       `json:"message_id,omitempty"`
	Content       string                       `json:"content"`
	SenderContext string                       `json:"sender_context,omitempty"`
	Origin        InvocationOrigin             `json:"origin"`
	AcceptedAt    time.Time                    `json:"accepted_at"`
	Mode          string                       `json:"mode_override,omitempty"`
	Images        []ImageAttachment            `json:"-"`
	Files         []FileAttachment             `json:"-"`
	OnRecovery    func(*ContextRecoveryRecord) `json:"-"`
}

// recoverySteeredInput records an input attempt, not a completed task. The
// original IPC receipt remains authoritative for delivery/acknowledgement.
type recoverySteeredInput struct {
	Input      IPCInput  `json:"input"`
	Status     string    `json:"status"`
	TurnID     string    `json:"turn_id,omitempty"`
	AcceptedAt time.Time `json:"accepted_at"`
	done       chan struct{}
	onRecovery func(*ContextRecoveryRecord)
}

func invocationForMessage(msg *Message) *recoveryInvocation {
	origin := msg.Origin
	if origin.Kind == "" {
		origin = InvocationOrigin{Kind: "user", ID: msg.MessageID}
	}
	return &recoveryInvocation{
		MessageID: msg.MessageID, Content: msg.Content, Origin: origin,
		AcceptedAt: time.Now().UTC(), Mode: msg.ModeOverride,
		Images: append([]ImageAttachment(nil), msg.Images...), Files: append([]FileAttachment(nil), msg.Files...),
		OnRecovery: msg.OnContextRecovery,
	}
}

func invocationForQueued(q queuedMessage) *recoveryInvocation {
	return &recoveryInvocation{
		MessageID: q.messageID, Content: q.content,
		Origin: InvocationOrigin{Kind: "user", ID: q.messageID}, AcceptedAt: time.Now().UTC(),
		Images: append([]ImageAttachment(nil), q.images...), Files: append([]FileAttachment(nil), q.files...),
	}
}

// Keep the configured sender header separate from the original input so the
// recovery receipt continues to hash the exact content accepted by the host.
func (e *Engine) recoveryInvocationForMessage(msg *Message) *recoveryInvocation {
	invocation := invocationForMessage(msg)
	invocation.SenderContext = e.buildSenderPrompt("", msg.UserID, msg.UserName, msg.Platform, msg.SessionKey, msg.ChannelKey)
	return invocation
}

func (e *Engine) recoveryInvocationForQueued(q queuedMessage) *recoveryInvocation {
	invocation := invocationForQueued(q)
	invocation.SenderContext = e.buildSenderPrompt("", q.userID, q.userName, q.msgPlatform, q.msgSessionKey, q.channelKey)
	return invocation
}
