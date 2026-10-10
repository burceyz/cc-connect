package core

import (
	"strings"
	"unicode"
)

const (
	PendingStageReviewWaiting   = "review_waiting"
	PendingStagePreparation     = "preparation"
	PendingStageChangesRequired = "changes_required"
)

// PendingInfo describes a cleanly ended turn whose task still needs work.
// An adapter must withhold unpublished candidate text before emitting
// EventPending and stop producing events for that turn. Code is a fixed,
// credential-free diagnostic identifier, never a subprocess error message.
type PendingInfo struct {
	TaskID   string `json:"task_id,omitempty"`
	Reviewer string `json:"reviewer,omitempty"`
	Stage    string `json:"stage,omitempty"`
	Code     string `json:"code,omitempty"`
}

// pendingTurnError carries a non-success outcome through the legacy synchronous
// relay signature. RelayManager converts it to a structured pending response.
type pendingTurnError struct {
	info    PendingInfo
	message string
}

func (p *pendingTurnError) Error() string { return p.message }

func safeStatusID(value string, maxLen int) string {
	if value == "" || len(value) > maxLen {
		return ""
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return ""
	}
	return value
}

func safeReviewerName(value string) string {
	value = strings.TrimSpace(value)
	if len([]rune(value)) > 80 {
		return ""
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == ' ' || strings.ContainsRune("-_.()（）", r) {
			continue
		}
		return ""
	}
	return value
}

func normalizedPendingInfo(info *PendingInfo) PendingInfo {
	if info == nil {
		return PendingInfo{}
	}
	pending := PendingInfo{
		TaskID:   safeStatusID(info.TaskID, 128),
		Reviewer: safeReviewerName(info.Reviewer),
		Code:     safeStatusID(info.Code, 64),
	}
	switch info.Stage {
	case PendingStageReviewWaiting, PendingStagePreparation, PendingStageChangesRequired:
		pending.Stage = info.Stage
	}
	return pending
}

func pendingStatusText(i18n *I18n, info *PendingInfo) string {
	pending := normalizedPendingInfo(info)
	text := i18n.T(MsgPendingGeneric)
	switch pending.Stage {
	case PendingStageReviewWaiting:
		text = i18n.T(MsgPendingReviewWaiting)
		if pending.Reviewer != "" {
			text = i18n.Tf(MsgPendingReviewWaitingNamed, pending.Reviewer)
		}
	case PendingStagePreparation:
		text = i18n.T(MsgPendingPreparation)
	case PendingStageChangesRequired:
		text = i18n.T(MsgPendingChangesRequired)
	}
	if pending.TaskID != "" {
		text += "\n" + i18n.Tf(MsgPendingTask, pending.TaskID)
	}
	if pending.Code != "" {
		text += "\n" + i18n.Tf(MsgPendingCode, pending.Code)
	}
	return text
}

// persistPendingSessionID preserves the backend's actual conversation even if
// this turn has no final result. Event.SessionID is a fallback for backends that
// have already torn down their per-turn process when the event is consumed.
func persistPendingSessionID(session *Session, sessions *SessionManager, backend AgentSession, agentName, eventID string) {
	id := eventID
	if backend != nil {
		if currentID := backend.CurrentSessionID(); currentID != "" {
			id = currentID
		}
	}
	if id == "" || id == session.GetAgentSessionID() {
		return
	}
	wasEmpty := session.GetAgentSessionID() == ""
	session.SetAgentSessionID(id, agentName)
	if wasEmpty {
		if name := session.GetName(); name != "" && name != "session" && name != "default" {
			sessions.SetSessionName(id, name)
		}
	}
	sessions.Save()
}

// verifiedReviewReceipt reads only host-verified identifiers. It is sent as a
// separate status message so the reviewed candidate remains byte-for-byte
// independent of this receipt. Adapters must not copy model claims into it.
func verifiedReviewReceipt(i18n *I18n, event Event) string {
	if event.Type != EventResult || !event.Done {
		return ""
	}
	review, ok := event.Metadata["verified_review"].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := review["reviewer"].(string)
	taskID, _ := review["task_id"].(string)
	revision, _ := review["revision"].(string)
	name = safeReviewerName(name)
	taskID = safeStatusID(taskID, 128)
	revision = safeStatusID(revision, 128)
	if name == "" || taskID == "" || revision == "" {
		return ""
	}
	return i18n.Tf(MsgVerifiedReviewReceipt, name, taskID, revision)
}
