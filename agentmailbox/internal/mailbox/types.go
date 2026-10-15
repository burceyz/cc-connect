// Package mailbox implements the agent-mailbox v1 service: a small local
// message store with a private Unix-socket HTTP/JSON interface.
//
// It keeps communication state only. It does not load model credentials,
// start model or user commands, or expose any file-reading endpoint.
package mailbox

import "errors"

const (
	// ProtocolName and ProtocolVersion identify the wire contract.
	ProtocolName    = "agent-mailbox-v1"
	ProtocolVersion = "1"

	// MaxRequestBody caps a whole HTTP request body (128 KiB).
	MaxRequestBody = 128 << 10
	// MaxBodyBytes caps one message body (60 KiB of UTF-8).
	MaxBodyBytes = 60 << 10

	DefaultInboxLimit = 50
	MaxInboxLimit     = 200
)

// Message transport states, ordered from weakest to strongest.
// Explicit read confirmation (ack) is not a transport state: it is the
// independent received_at timestamp.
const (
	StatePending     = "pending"
	StateDispatching = "dispatching"
	StateUnknown     = "unknown"
	StateOffered     = "offered"
	StateAccepted    = "accepted"
)

var stateRank = map[string]int{
	StatePending:     0,
	StateDispatching: 1,
	StateUnknown:     2,
	StateOffered:     3,
	StateAccepted:    4,
}

// Member is a registered mailbox. Token material is never serialized.
type Member struct {
	MailboxID       string         `json:"mailbox_id"`
	Label           string         `json:"label"`
	Kind            string         `json:"kind"`
	Coordinator     bool           `json:"coordinator"`
	Scope           string         `json:"scope"`
	Team            string         `json:"team"`
	ParentID        string         `json:"parent_id"`
	Metadata        map[string]any `json:"metadata"`
	Generation      string         `json:"generation"`
	NativeSessionID string         `json:"native_session_id"`
	Capabilities    []string       `json:"capabilities"`
	Active          bool           `json:"active"`
	State           string         `json:"state"`
	LastSeen        int64          `json:"last_seen"`
	CreatedAt       int64          `json:"created_at"`

	registrationKey string
	tokenDigest     string
	metadataJSON    string
}

// Peer is the reduced member view returned by GET /v1/peers.
type Peer struct {
	MailboxID    string   `json:"mailbox_id"`
	Label        string   `json:"label"`
	Kind         string   `json:"kind"`
	Team         string   `json:"team"`
	State        string   `json:"state"`
	LastSeen     int64    `json:"last_seen"`
	Capabilities []string `json:"capabilities"`
}

// Receipt is one distinct delivery observation for a claim. Identical
// (state, evidence) retries are stored once.
type Receipt struct {
	State     string `json:"state"`
	Evidence  string `json:"evidence"`
	CreatedAt int64  `json:"created_at"`
}

// Message is one stored envelope with its delivery state and public
// reconciliation data.
type Message struct {
	MessageID           string    `json:"message_id"`
	ClientMessageID     string    `json:"client_message_id"`
	SenderID            string    `json:"sender_id"`
	ToID                string    `json:"to_id"`
	Body                string    `json:"body"`
	TaskID              string    `json:"task_id"`
	ReplyTo             string    `json:"reply_to"`
	State               string    `json:"state"`
	AttemptID           string    `json:"attempt_id"`
	ClaimedByGeneration string    `json:"claimed_by_generation"`
	ClaimedAt           int64     `json:"claimed_at"`
	ReceivedAt          int64     `json:"received_at"`
	Receipts            []Receipt `json:"receipts"`
	CreatedAt           int64     `json:"created_at"`
	UpdatedAt           int64     `json:"updated_at"`

	seq int64
}

// RegisterRequest is the body of POST /v1/register.
type RegisterRequest struct {
	RegistrationKey string         `json:"registration_key"`
	Token           string         `json:"token"`
	Label           string         `json:"label"`
	Kind            string         `json:"kind"`
	Coordinator     bool           `json:"coordinator"`
	Scope           string         `json:"scope"`
	Team            string         `json:"team"`
	ParentID        string         `json:"parent_id"`
	Metadata        map[string]any `json:"metadata"`
}

// SendRequest is the body of POST /v1/send.
type SendRequest struct {
	ClientMessageID string `json:"client_message_id"`
	To              string `json:"to"`
	Body            string `json:"body"`
	TaskID          string `json:"task_id"`
	ReplyTo         string `json:"reply_to"`
}

// Sentinel errors are mapped to stable API codes by the HTTP layer.
var (
	ErrNotFound              = errors.New("not found")
	ErrPeerNotAllowed        = errors.New("peer not allowed")
	ErrReplyChainInvalid     = errors.New("reply chain invalid")
	ErrRegistrationConflict  = errors.New("registration conflict")
	ErrIdempotencyConflict   = errors.New("idempotency conflict")
	ErrRequestIDConflict     = errors.New("request id conflict")
	ErrGenerationMismatch    = errors.New("generation mismatch")
	ErrGenerationEnded       = errors.New("generation ended")
	ErrCapabilityConflict    = errors.New("capability conflict")
	ErrNativeSessionMismatch = errors.New("native session mismatch")
	ErrAttemptMismatch       = errors.New("attempt mismatch")
	ErrInvalidParent         = errors.New("invalid parent")
	ErrInvalidRequest        = errors.New("invalid request")
	ErrInvalidCursor         = errors.New("invalid cursor")
)

func (m *Member) derivedState() string {
	if m.Active {
		return "active"
	}
	return "idle"
}

func validReceiptState(state string) bool {
	switch state {
	case StateOffered, StateAccepted, StateUnknown:
		return true
	}
	return false
}

// canCommunicate is the single authorization rule for peer messaging.
// Parent/child communication needs the parent side to be an actual
// coordinator; kind labels never grant permissions.
func canCommunicate(a, b *Member) bool {
	if a == nil || b == nil || a.Scope == "" || a.Scope != b.Scope {
		return false
	}
	if a.Team != "" && a.Team == b.Team {
		return true
	}
	if a.Coordinator && b.ParentID == a.MailboxID {
		return true
	}
	if b.Coordinator && a.ParentID == b.MailboxID {
		return true
	}
	return false
}

func sameParticipants(original *Message, senderID, toID string) bool {
	return (original.SenderID == senderID && original.ToID == toID) ||
		(original.SenderID == toID && original.ToID == senderID)
}
