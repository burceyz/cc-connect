package mailbox

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// schemaVersion guards against silently reusing an older, incompatible local
// database. There is no in-place migration in this round: a database created
// by an earlier schema is refused with a clear error instead of being faked
// into the new shape.
const schemaVersion = 2

const schemaSQL = `
CREATE TABLE IF NOT EXISTS members (
	mailbox_id        TEXT PRIMARY KEY,
	registration_key  TEXT NOT NULL UNIQUE,
	token_digest      TEXT NOT NULL,
	label             TEXT NOT NULL DEFAULT '',
	kind              TEXT NOT NULL DEFAULT '',
	coordinator       INTEGER NOT NULL DEFAULT 0,
	scope             TEXT NOT NULL,
	team              TEXT NOT NULL DEFAULT '',
	parent_id         TEXT NOT NULL DEFAULT '',
	metadata          TEXT NOT NULL DEFAULT '{}',
	generation        TEXT NOT NULL DEFAULT '',
	native_session_id TEXT NOT NULL DEFAULT '',
	capabilities      TEXT NOT NULL DEFAULT '[]',
	active            INTEGER NOT NULL DEFAULT 0,
	last_seen         INTEGER NOT NULL DEFAULT 0,
	created_at        INTEGER NOT NULL,
	updated_at        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS generations (
	mailbox_id        TEXT NOT NULL,
	generation        TEXT NOT NULL,
	native_session_id TEXT NOT NULL DEFAULT '',
	capabilities      TEXT NOT NULL DEFAULT '[]',
	ended             INTEGER NOT NULL DEFAULT 0,
	created_at        INTEGER NOT NULL,
	updated_at        INTEGER NOT NULL,
	PRIMARY KEY (mailbox_id, generation)
);

CREATE TABLE IF NOT EXISTS messages (
	seq                  INTEGER PRIMARY KEY AUTOINCREMENT,
	message_id           TEXT NOT NULL UNIQUE,
	client_message_id    TEXT NOT NULL,
	sender_id            TEXT NOT NULL,
	to_id                TEXT NOT NULL,
	body                 TEXT NOT NULL,
	task_id              TEXT NOT NULL DEFAULT '',
	reply_to             TEXT NOT NULL DEFAULT '',
	state                TEXT NOT NULL DEFAULT 'pending',
	attempt_id           TEXT NOT NULL DEFAULT '',
	claimed_by_generation TEXT NOT NULL DEFAULT '',
	claimed_at           INTEGER NOT NULL DEFAULT 0,
	received_at          INTEGER NOT NULL DEFAULT 0,
	created_at           INTEGER NOT NULL,
	updated_at           INTEGER NOT NULL,
	UNIQUE (sender_id, client_message_id)
);

CREATE TABLE IF NOT EXISTS attempts (
	attempt_id  TEXT PRIMARY KEY,
	message_id  TEXT NOT NULL,
	mailbox_id  TEXT NOT NULL,
	generation  TEXT NOT NULL,
	created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS claims (
	mailbox_id  TEXT NOT NULL,
	request_id  TEXT NOT NULL,
	generation  TEXT NOT NULL DEFAULT '',
	message_id  TEXT NOT NULL DEFAULT '',
	attempt_id  TEXT NOT NULL DEFAULT '',
	created_at  INTEGER NOT NULL,
	PRIMARY KEY (mailbox_id, request_id)
);

CREATE TABLE IF NOT EXISTS receipts (
	receipt_id  INTEGER PRIMARY KEY AUTOINCREMENT,
	message_id  TEXT NOT NULL,
	attempt_id  TEXT NOT NULL,
	state       TEXT NOT NULL,
	evidence    TEXT NOT NULL DEFAULT '',
	created_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_inbox ON messages(to_id, seq);
CREATE INDEX IF NOT EXISTS idx_receipts_message ON receipts(message_id);
`

// Store is the SQLite-backed mailbox state. A single connection plus one
// write mutex keeps read-modify-write transactions atomic and simple.
type Store struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// OpenStore opens (creating if needed) the SQLite database at path.
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	var existingMembers int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='members'`).Scan(&existingMembers); err != nil {
		db.Close()
		return nil, fmt.Errorf("inspect schema: %w", err)
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("read schema version: %w", err)
	}
	if existingMembers > 0 && version != schemaVersion {
		db.Close()
		return nil, fmt.Errorf("database schema version %d is not supported (want %d); use a new runtime directory instead of reusing this database", version, schemaVersion)
	}
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// initSchema applies the schema and records the version in one transaction, so
// an interrupted first initialization leaves no half-built tables behind.
func initSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin schema transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(schemaSQL); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema: %w", err)
	}
	return nil
}

func sqliteDSN(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{}
	q.Add("_txlock", "immediate")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	return u.String()
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

// writeTx serializes one write transaction.
func (s *Store) writeTx(fn func(tx *sql.Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

const memberColumns = `mailbox_id, registration_key, token_digest, label, kind, coordinator, scope, team, parent_id, metadata, generation, native_session_id, capabilities, active, last_seen, created_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMember(sc rowScanner) (*Member, error) {
	var (
		m        Member
		coord    int
		active   int
		metaJSON string
		capsJSON string
	)
	err := sc.Scan(&m.MailboxID, &m.registrationKey, &m.tokenDigest, &m.Label, &m.Kind, &coord,
		&m.Scope, &m.Team, &m.ParentID, &metaJSON, &m.Generation, &m.NativeSessionID,
		&capsJSON, &active, &m.LastSeen, &m.CreatedAt)
	if err != nil {
		return nil, err
	}
	m.Coordinator = coord != 0
	m.Active = active != 0
	m.State = m.derivedState()
	if metaJSON == "" {
		metaJSON = "{}"
	}
	m.metadataJSON = metaJSON
	m.Metadata = decodeMetadata(metaJSON)
	m.Capabilities = decodeStringList(capsJSON)
	return &m, nil
}

func memberByID(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, mailboxID string) (*Member, error) {
	return scanMember(q.QueryRow(`SELECT `+memberColumns+` FROM members WHERE mailbox_id = ?`, mailboxID))
}

func memberByRegistrationKey(tx *sql.Tx, key string) (*Member, error) {
	return scanMember(tx.QueryRow(`SELECT `+memberColumns+` FROM members WHERE registration_key = ?`, key))
}

// MemberByID loads one member by mailbox id.
func (s *Store) MemberByID(mailboxID string) (*Member, error) {
	return memberByID(s.db, mailboxID)
}

// MemberByToken resolves a bearer token to its mailbox.
func (s *Store) MemberByToken(token string) (*Member, error) {
	if token == "" {
		return nil, ErrNotFound
	}
	return scanMember(s.db.QueryRow(`SELECT `+memberColumns+` FROM members WHERE token_digest = ?`, tokenDigest(token)))
}

func tokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// generationRow is one persisted execution generation.
type generationRow struct {
	nativeSessionID string
	capabilities    string
	ended           bool
}

func generationByKey(tx *sql.Tx, mailboxID, generation string) (*generationRow, error) {
	var row generationRow
	var ended int
	err := tx.QueryRow(`SELECT native_session_id, capabilities, ended FROM generations WHERE mailbox_id=? AND generation=?`,
		mailboxID, generation).Scan(&row.nativeSessionID, &row.capabilities, &ended)
	if err != nil {
		return nil, err
	}
	row.ended = ended != 0
	return &row, nil
}

// Register creates a mailbox, or returns the original one for an identical
// re-submission of the same registration key. Any field change conflicts.
func (s *Store) Register(req RegisterRequest) (*Member, bool, error) {
	req.RegistrationKey = strings.TrimSpace(req.RegistrationKey)
	if req.RegistrationKey == "" || req.Token == "" || strings.TrimSpace(req.Scope) == "" {
		return nil, false, ErrInvalidRequest
	}
	if len(req.RegistrationKey) > 512 || len(req.Token) > 512 {
		return nil, false, ErrInvalidRequest
	}
	metaJSON, err := canonicalJSON(req.Metadata)
	if err != nil {
		return nil, false, ErrInvalidRequest
	}
	digest := tokenDigest(req.Token)

	var created *Member
	var isNew bool
	err = s.writeTx(func(tx *sql.Tx) error {
		existing, err := memberByRegistrationKey(tx, req.RegistrationKey)
		switch {
		case err == nil:
			if existing.tokenDigest != digest || existing.Label != req.Label || existing.Kind != req.Kind ||
				existing.Coordinator != req.Coordinator || existing.Scope != req.Scope ||
				existing.Team != req.Team || existing.ParentID != req.ParentID ||
				existing.metadataJSON != metaJSON {
				return ErrRegistrationConflict
			}
			created = existing
			return nil
		case errors.Is(err, sql.ErrNoRows):
			// fall through to create
		default:
			return err
		}

		if req.ParentID != "" {
			parent, err := memberByID(tx, req.ParentID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrInvalidParent
				}
				return err
			}
			if parent.Scope != req.Scope || !parent.Coordinator {
				return ErrInvalidParent
			}
		}
		now := time.Now().Unix()
		mailboxID := newMailboxID()
		_, err = tx.Exec(`INSERT INTO members
			(mailbox_id, registration_key, token_digest, label, kind, coordinator, scope, team, parent_id, metadata, generation, native_session_id, capabilities, active, last_seen, created_at, updated_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			mailboxID, req.RegistrationKey, digest, req.Label, req.Kind, boolInt(req.Coordinator),
			req.Scope, req.Team, req.ParentID, metaJSON, "", "", "[]", 0, 0, now, now)
		if err != nil {
			return err
		}
		created, err = memberByID(tx, mailboxID)
		if err != nil {
			return err
		}
		isNew = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return created, isNew, nil
}

// Activate binds one execution generation to the mailbox. A generation that
// already ended can never be activated again, and the capabilities of an
// active generation are immutable.
func (s *Store) Activate(mailboxID, generation, nativeSessionID string, capabilities []string) (*Member, error) {
	if generation == "" {
		return nil, ErrInvalidRequest
	}
	nativeSessionID = normalizeNativeSessionID(nativeSessionID)
	var result *Member
	err := s.writeTx(func(tx *sql.Tx) error {
		m, err := memberByID(tx, mailboxID)
		if err != nil {
			return notFoundOr(err)
		}
		caps := encodeStringList(capabilities)

		history, err := generationByKey(tx, mailboxID, generation)
		switch {
		case err == nil:
			if history.ended || !m.Active || m.Generation != generation {
				return ErrGenerationEnded
			}
			if caps != history.capabilities {
				return ErrCapabilityConflict
			}
			native, err := mergeNativeSession(history.nativeSessionID, nativeSessionID)
			if err != nil {
				return err
			}
			now := time.Now().Unix()
			if _, err := tx.Exec(`UPDATE generations SET native_session_id=?, updated_at=? WHERE mailbox_id=? AND generation=?`,
				native, now, mailboxID, generation); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE members SET native_session_id=?, last_seen=?, updated_at=? WHERE mailbox_id=?`,
				native, now, now, mailboxID); err != nil {
				return err
			}
			result, err = memberByID(tx, mailboxID)
			return err
		case errors.Is(err, sql.ErrNoRows):
			// new generation
		default:
			return err
		}

		if m.Active && m.Generation != "" {
			return ErrGenerationMismatch
		}
		now := time.Now().Unix()
		if _, err := tx.Exec(`INSERT INTO generations
			(mailbox_id, generation, native_session_id, capabilities, ended, created_at, updated_at)
			VALUES (?,?,?,?,0,?,?)`,
			mailboxID, generation, nativeSessionID, caps, now, now); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE members SET generation=?, native_session_id=?, capabilities=?, active=1, last_seen=?, updated_at=? WHERE mailbox_id=?`,
			generation, nativeSessionID, caps, now, now, mailboxID); err != nil {
			return err
		}
		result, err = memberByID(tx, mailboxID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Heartbeat refreshes last_seen for the current active generation and may
// fill a still-empty native session id.
func (s *Store) Heartbeat(mailboxID, generation, nativeSessionID string) (*Member, error) {
	if generation == "" {
		return nil, ErrInvalidRequest
	}
	nativeSessionID = normalizeNativeSessionID(nativeSessionID)
	var result *Member
	err := s.writeTx(func(tx *sql.Tx) error {
		m, err := memberByID(tx, mailboxID)
		if err != nil {
			return notFoundOr(err)
		}
		if !m.Active || m.Generation == "" || m.Generation != generation {
			return ErrGenerationMismatch
		}
		history, err := generationByKey(tx, mailboxID, generation)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrGenerationMismatch
			}
			return err
		}
		native, err := mergeNativeSession(history.nativeSessionID, nativeSessionID)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		if _, err := tx.Exec(`UPDATE generations SET native_session_id=?, updated_at=? WHERE mailbox_id=? AND generation=?`,
			native, now, mailboxID, generation); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE members SET native_session_id=?, last_seen=?, updated_at=? WHERE mailbox_id=?`,
			native, now, now, mailboxID); err != nil {
			return err
		}
		result, err = memberByID(tx, mailboxID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Deactivate closes the caller's own generation for good. Messages are
// untouched and its attempts can still be reconciled by receipt.
func (s *Store) Deactivate(mailboxID, generation string) (*Member, error) {
	if generation == "" {
		return nil, ErrInvalidRequest
	}
	var result *Member
	err := s.writeTx(func(tx *sql.Tx) error {
		m, err := memberByID(tx, mailboxID)
		if err != nil {
			return notFoundOr(err)
		}
		if !m.Active || m.Generation == "" || m.Generation != generation {
			return ErrGenerationMismatch
		}
		now := time.Now().Unix()
		if _, err := tx.Exec(`UPDATE generations SET ended=1, updated_at=? WHERE mailbox_id=? AND generation=?`,
			now, mailboxID, generation); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE members SET active=0, last_seen=?, updated_at=? WHERE mailbox_id=?`, now, now, mailboxID); err != nil {
			return err
		}
		result, err = memberByID(tx, mailboxID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Peers returns every member this mailbox may talk to.
func (s *Store) Peers(me *Member) ([]*Peer, error) {
	rows, err := s.db.Query(`SELECT ` + memberColumns + ` FROM members ORDER BY created_at, mailbox_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	peers := []*Peer{}
	for rows.Next() {
		m, err := scanMember(rows)
		if err != nil {
			return nil, err
		}
		if m.MailboxID == me.MailboxID || !canCommunicate(me, m) {
			continue
		}
		peers = append(peers, &Peer{
			MailboxID:    m.MailboxID,
			Label:        m.Label,
			Kind:         m.Kind,
			Team:         m.Team,
			State:        m.State,
			LastSeen:     m.LastSeen,
			Capabilities: m.Capabilities,
		})
	}
	return peers, rows.Err()
}

// Send stores a message. The second result is false when an identical
// (sender, client_message_id) message already existed.
func (s *Store) Send(sender *Member, req SendRequest) (*Message, bool, error) {
	if req.ClientMessageID == "" || req.To == "" || req.Body == "" {
		return nil, false, ErrInvalidRequest
	}
	var result *Message
	created := false
	err := s.writeTx(func(tx *sql.Tx) error {
		target, err := memberByID(tx, req.To)
		if err != nil {
			return notFoundOr(err)
		}
		if !canCommunicate(sender, target) {
			return ErrPeerNotAllowed
		}
		if req.ReplyTo != "" {
			original, err := messageByID(tx, req.ReplyTo)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrReplyChainInvalid
				}
				return err
			}
			if !sameParticipants(original, sender.MailboxID, req.To) {
				return ErrReplyChainInvalid
			}
		}
		existing, err := messageBySenderClientID(tx, sender.MailboxID, req.ClientMessageID)
		if err == nil {
			if existing.ToID == req.To && existing.Body == req.Body &&
				existing.TaskID == req.TaskID && existing.ReplyTo == req.ReplyTo {
				result = existing
				return nil
			}
			return ErrIdempotencyConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		messageID := newMessageID()
		now := time.Now().Unix()
		_, err = tx.Exec(`INSERT INTO messages
			(message_id, client_message_id, sender_id, to_id, body, task_id, reply_to, state, attempt_id, claimed_by_generation, claimed_at, received_at, created_at, updated_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			messageID, req.ClientMessageID, sender.MailboxID, req.To, req.Body, req.TaskID, req.ReplyTo,
			StatePending, "", "", 0, 0, now, now)
		if err != nil {
			return err
		}
		result, err = messageByID(tx, messageID)
		if err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if err := s.attachReceipts(result); err != nil {
		return nil, false, err
	}
	return result, created, nil
}

// MessageFor returns a message when viewer is allowed to read it. The result
// carries the original claim and the deduplicated receipt evidence.
func (s *Store) MessageFor(viewer *Member, messageID string) (*Message, error) {
	msg, err := messageByID(s.db, messageID)
	if err != nil {
		return nil, notFoundOr(err)
	}
	if !s.canRead(viewer, msg) {
		return nil, ErrNotFound
	}
	if err := s.attachReceipts(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// canRead implements the message visibility rule.
func (s *Store) canRead(viewer *Member, msg *Message) bool {
	if viewer.MailboxID == msg.SenderID || viewer.MailboxID == msg.ToID {
		return true
	}
	if !viewer.Coordinator {
		return false
	}
	sender, senderErr := s.MemberByID(msg.SenderID)
	recipient, recipientErr := s.MemberByID(msg.ToID)
	if senderErr != nil || recipientErr != nil {
		return false
	}
	return viewer.Scope == sender.Scope &&
		(sender.ParentID == viewer.MailboxID || recipient.ParentID == viewer.MailboxID)
}

// Inbox returns messages addressed to mailboxID with seq greater than after.
func (s *Store) Inbox(mailboxID string, after int64, limit int) ([]*Message, int64, error) {
	if limit <= 0 {
		limit = DefaultInboxLimit
	}
	if limit > MaxInboxLimit {
		limit = MaxInboxLimit
	}
	rows, err := s.db.Query(`SELECT `+messageColumns+` FROM messages WHERE to_id = ? AND seq > ? ORDER BY seq LIMIT ?`,
		mailboxID, after, limit)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	messages := []*Message{}
	cursor := after
	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, after, err
		}
		messages = append(messages, msg)
		cursor = msg.seq
	}
	if err := rows.Err(); err != nil {
		return nil, after, err
	}
	if err := s.attachReceipts(messages...); err != nil {
		return nil, after, err
	}
	return messages, cursor, nil
}

// Claim atomically takes at most one pending, not-yet-acked message for the
// active generation. A repeated (mailbox_id, request_id) is answered from its
// stored result, but only for the same generation.
func (s *Store) Claim(mailboxID, generation, requestID string) (*Message, string, error) {
	if generation == "" || requestID == "" {
		return nil, "", ErrInvalidRequest
	}
	var result *Message
	var attemptID string
	err := s.writeTx(func(tx *sql.Tx) error {
		var storedMessage, storedAttempt, storedGeneration string
		err := tx.QueryRow(`SELECT message_id, attempt_id, generation FROM claims WHERE mailbox_id=? AND request_id=?`,
			mailboxID, requestID).Scan(&storedMessage, &storedAttempt, &storedGeneration)
		switch {
		case err == nil:
			if storedGeneration != generation {
				return ErrRequestIDConflict
			}
			if storedMessage == "" {
				return nil
			}
			result, err = messageByID(tx, storedMessage)
			if err != nil {
				return ErrInternal
			}
			attemptID = storedAttempt
			return nil
		case errors.Is(err, sql.ErrNoRows):
			// first use of this request_id
		default:
			return err
		}

		m, err := memberByID(tx, mailboxID)
		if err != nil {
			return notFoundOr(err)
		}
		if !m.Active || m.Generation == "" || m.Generation != generation {
			return ErrGenerationMismatch
		}

		var messageID string
		err = tx.QueryRow(`SELECT message_id FROM messages WHERE to_id=? AND state=? AND received_at=0 ORDER BY seq LIMIT 1`,
			mailboxID, StatePending).Scan(&messageID)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.Exec(`INSERT INTO claims (mailbox_id, request_id, generation, message_id, attempt_id, created_at) VALUES (?,?,?,?,?,?)`,
				mailboxID, requestID, generation, "", "", time.Now().Unix())
			return err
		}
		if err != nil {
			return err
		}

		attemptID = newAttemptID()
		now := time.Now().Unix()
		res, err := tx.Exec(`UPDATE messages SET state=?, attempt_id=?, claimed_by_generation=?, claimed_at=?, updated_at=? WHERE message_id=? AND state=? AND received_at=0`,
			StateDispatching, attemptID, generation, now, now, messageID, StatePending)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			return ErrInternal
		}
		if _, err := tx.Exec(`INSERT INTO attempts (attempt_id, message_id, mailbox_id, generation, created_at) VALUES (?,?,?,?,?)`,
			attemptID, messageID, mailboxID, generation, now); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO claims (mailbox_id, request_id, generation, message_id, attempt_id, created_at) VALUES (?,?,?,?,?,?)`,
			mailboxID, requestID, generation, messageID, attemptID, now); err != nil {
			return err
		}
		result, err = messageByID(tx, messageID)
		return err
	})
	if err != nil {
		return nil, "", err
	}
	if err := s.attachReceipts(result); err != nil {
		return nil, "", err
	}
	return result, attemptID, nil
}

// Receipt records a delivery observation for the caller's own claim. An
// identical (state, evidence) retry is stored once; a weaker state never
// overwrites a stronger existing state.
func (s *Store) Receipt(mailboxID, generation, messageID, attemptID, state, evidence string) (*Message, error) {
	if !validReceiptState(state) || messageID == "" || attemptID == "" || generation == "" {
		return nil, ErrInvalidRequest
	}
	var result *Message
	err := s.writeTx(func(tx *sql.Tx) error {
		msg, err := messageByID(tx, messageID)
		if err != nil {
			return notFoundOr(err)
		}
		if msg.ToID != mailboxID {
			return ErrNotFound
		}
		if msg.AttemptID == "" || msg.AttemptID != attemptID {
			return ErrAttemptMismatch
		}
		if msg.ClaimedByGeneration != generation {
			return ErrGenerationMismatch
		}
		now := time.Now().Unix()
		var duplicates int
		if err := tx.QueryRow(`SELECT count(*) FROM receipts WHERE message_id=? AND attempt_id=? AND state=? AND evidence=?`,
			messageID, attemptID, state, evidence).Scan(&duplicates); err != nil {
			return err
		}
		if duplicates == 0 {
			if _, err := tx.Exec(`INSERT INTO receipts (message_id, attempt_id, state, evidence, created_at) VALUES (?,?,?,?,?)`,
				messageID, attemptID, state, evidence, now); err != nil {
				return err
			}
		}
		if stateRank[state] > stateRank[msg.State] {
			if _, err := tx.Exec(`UPDATE messages SET state=?, updated_at=? WHERE message_id=?`, state, now, messageID); err != nil {
				return err
			}
			msg.State = state
			msg.UpdatedAt = now
		}
		result = msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := s.attachReceipts(result); err != nil {
		return nil, err
	}
	return result, nil
}

// Ack records explicit receipt by the current recipient. It only writes the
// independent received_at timestamp; the transport state is left untouched.
func (s *Store) Ack(mailboxID, messageID string) (*Message, error) {
	if messageID == "" {
		return nil, ErrInvalidRequest
	}
	var result *Message
	err := s.writeTx(func(tx *sql.Tx) error {
		msg, err := messageByID(tx, messageID)
		if err != nil {
			return notFoundOr(err)
		}
		if msg.ToID != mailboxID {
			return ErrNotFound
		}
		if msg.ReceivedAt == 0 {
			now := time.Now().Unix()
			if _, err := tx.Exec(`UPDATE messages SET received_at=?, updated_at=? WHERE message_id=?`, now, now, messageID); err != nil {
				return err
			}
			msg.ReceivedAt = now
			msg.UpdatedAt = now
		}
		result = msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := s.attachReceipts(result); err != nil {
		return nil, err
	}
	return result, nil
}

const messageColumns = `seq, message_id, client_message_id, sender_id, to_id, body, task_id, reply_to, state, attempt_id, claimed_by_generation, claimed_at, received_at, created_at, updated_at`

func scanMessage(sc rowScanner) (*Message, error) {
	m := Message{Receipts: []Receipt{}}
	err := sc.Scan(&m.seq, &m.MessageID, &m.ClientMessageID, &m.SenderID, &m.ToID, &m.Body, &m.TaskID,
		&m.ReplyTo, &m.State, &m.AttemptID, &m.ClaimedByGeneration, &m.ClaimedAt,
		&m.ReceivedAt, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func messageByID(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, messageID string) (*Message, error) {
	return scanMessage(q.QueryRow(`SELECT `+messageColumns+` FROM messages WHERE message_id = ?`, messageID))
}

func messageBySenderClientID(tx *sql.Tx, senderID, clientMessageID string) (*Message, error) {
	return scanMessage(tx.QueryRow(`SELECT `+messageColumns+` FROM messages WHERE sender_id = ? AND client_message_id = ?`,
		senderID, clientMessageID))
}

// attachReceipts fills the public receipt list on already-loaded messages
// with one query, in insertion order.
func (s *Store) attachReceipts(messages ...*Message) error {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		message.Receipts = []Receipt{}
		ids = append(ids, message.MessageID)
	}
	if len(ids) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.Query(`SELECT message_id, state, evidence, created_at FROM receipts WHERE message_id IN (`+placeholders+`) ORDER BY receipt_id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	byMessage := map[string][]Receipt{}
	for rows.Next() {
		var messageID string
		var receipt Receipt
		if err := rows.Scan(&messageID, &receipt.State, &receipt.Evidence, &receipt.CreatedAt); err != nil {
			return err
		}
		byMessage[messageID] = append(byMessage[messageID], receipt)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, message := range messages {
		if message == nil {
			continue
		}
		if receipts, ok := byMessage[message.MessageID]; ok {
			message.Receipts = receipts
		}
	}
	return nil
}

// ErrInternal means a uniqueness or invariant check failed unexpectedly.
var ErrInternal = errors.New("internal error")

// notFoundOr maps a missing row to ErrNotFound and passes real errors up.
func notFoundOr(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// decodeMetadata keeps JSON number literals intact (no float64 rounding).
func decodeMetadata(raw string) map[string]any {
	out := map[string]any{}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func canonicalJSON(v any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func encodeStringList(list []string) string {
	if list == nil {
		list = []string{}
	}
	b, err := json.Marshal(list)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func decodeStringList(raw string) []string {
	out := []string{}
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &out)
	}
	if out == nil {
		out = []string{}
	}
	return out
}

// normalizeNativeSessionID trims a native session id at its entry point, so
// first activation, repeat activation, and heartbeat agree on one value.
func normalizeNativeSessionID(value string) string {
	return strings.TrimSpace(value)
}

func mergeNativeSession(current, incoming string) (string, error) {
	incoming = normalizeNativeSessionID(incoming)
	if incoming == "" {
		return current, nil
	}
	if current == "" || current == incoming {
		return incoming, nil
	}
	return "", ErrNativeSessionMismatch
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
