package mailbox

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type apiError struct {
	status int
	code   string
}

func (e *apiError) Error() string { return e.code }

var (
	errInvalidRequest  = &apiError{http.StatusBadRequest, "invalid_request"}
	errInvalidCursor   = &apiError{http.StatusBadRequest, "invalid_cursor"}
	errUnauthorized    = &apiError{http.StatusUnauthorized, "unauthorized"}
	errNotFound        = &apiError{http.StatusNotFound, "not_found"}
	errPayloadTooLarge = &apiError{http.StatusRequestEntityTooLarge, "payload_too_large"}
	errInternal        = &apiError{http.StatusInternalServerError, "internal"}
)

func apiErrorFor(err error) *apiError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound):
		return errNotFound
	case errors.Is(err, ErrPeerNotAllowed):
		return &apiError{http.StatusForbidden, "peer_not_allowed"}
	case errors.Is(err, ErrReplyChainInvalid):
		return &apiError{http.StatusForbidden, "reply_chain_invalid"}
	case errors.Is(err, ErrRegistrationConflict):
		return &apiError{http.StatusConflict, "registration_conflict"}
	case errors.Is(err, ErrIdempotencyConflict):
		return &apiError{http.StatusConflict, "idempotency_conflict"}
	case errors.Is(err, ErrRequestIDConflict):
		return &apiError{http.StatusConflict, "request_id_conflict"}
	case errors.Is(err, ErrGenerationMismatch):
		return &apiError{http.StatusConflict, "generation_mismatch"}
	case errors.Is(err, ErrGenerationEnded):
		return &apiError{http.StatusConflict, "generation_ended"}
	case errors.Is(err, ErrCapabilityConflict):
		return &apiError{http.StatusConflict, "capability_conflict"}
	case errors.Is(err, ErrNativeSessionMismatch):
		return &apiError{http.StatusConflict, "native_session_mismatch"}
	case errors.Is(err, ErrAttemptMismatch):
		return &apiError{http.StatusConflict, "attempt_mismatch"}
	case errors.Is(err, ErrInvalidParent):
		return &apiError{http.StatusBadRequest, "invalid_parent"}
	case errors.Is(err, ErrInvalidCursor):
		return errInvalidCursor
	case errors.Is(err, ErrInvalidRequest):
		return errInvalidRequest
	default:
		return errInternal
	}
}

// Server adapts the Store to the v1 HTTP interface.
type Server struct {
	store      *Store
	adminToken string
	instanceID string
	logger     *slog.Logger
	startedAt  time.Time
}

// NewServer builds the request handler for one service process.
func NewServer(store *Store, adminToken string, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		store:      store,
		adminToken: adminToken,
		instanceID: newInstanceID(),
		logger:     logger,
		startedAt:  time.Now(),
	}
}

// InstanceID is this process's startup identity.
func (s *Server) InstanceID() string { return s.instanceID }

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("POST /v1/register", s.requireAdmin(s.handleRegister))
	mux.HandleFunc("GET /v1/me", s.requireMember(s.handleMe))
	mux.HandleFunc("GET /v1/peers", s.requireMember(s.handlePeers))
	mux.HandleFunc("POST /v1/activate", s.requireMember(s.handleActivate))
	mux.HandleFunc("POST /v1/heartbeat", s.requireMember(s.handleHeartbeat))
	mux.HandleFunc("POST /v1/deactivate", s.requireMember(s.handleDeactivate))
	mux.HandleFunc("POST /v1/send", s.requireMember(s.handleSend))
	mux.HandleFunc("GET /v1/messages/{id}", s.requireMember(s.handleGetMessage))
	mux.HandleFunc("GET /v1/inbox", s.requireMember(s.handleInbox))
	mux.HandleFunc("POST /v1/claim", s.requireMember(s.handleClaim))
	mux.HandleFunc("POST /v1/receipt", s.requireMember(s.handleReceipt))
	mux.HandleFunc("POST /v1/ack", s.requireMember(s.handleAck))
	mux.HandleFunc("/v1", s.handleUnknown)
	mux.HandleFunc("/", s.handleUnknown)
	return s.withLogging(mux)
}

type authInfo struct {
	admin  bool
	member *Member
}

func (s *Server) authenticate(r *http.Request) (*authInfo, *apiError) {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return nil, errUnauthorized
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if token == "" {
		return nil, errUnauthorized
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.adminToken)) == 1 {
		return &authInfo{admin: true}, nil
	}
	member, err := s.store.MemberByToken(token)
	if err != nil {
		return nil, errUnauthorized
	}
	return &authInfo{member: member}, nil
}

func (s *Server) requireAdmin(h func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		info, apiErr := s.authenticate(r)
		if apiErr != nil || !info.admin {
			writeError(w, errUnauthorized)
			return
		}
		h(w, r)
	}
}

func (s *Server) requireMember(h func(http.ResponseWriter, *http.Request, *Member)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		info, apiErr := s.authenticate(r)
		if apiErr != nil || info.member == nil {
			writeError(w, errUnauthorized)
			return
		}
		h(w, r, info.member)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"protocol":    ProtocolName,
		"version":     ProtocolVersion,
		"instance_id": s.instanceID,
	})
}

func (s *Server) handleUnknown(w http.ResponseWriter, _ *http.Request) {
	writeError(w, errNotFound)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	member, created, err := s.store.Register(req)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"mailbox": member})
}

func (s *Server) handleMe(w http.ResponseWriter, _ *http.Request, member *Member) {
	writeJSON(w, http.StatusOK, map[string]any{"mailbox": member})
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request, member *Member) {
	peers, err := s.store.Peers(member)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"peers": peers})
}

func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request, member *Member) {
	var req struct {
		Generation      string   `json:"generation"`
		NativeSessionID string   `json:"native_session_id"`
		Capabilities    []string `json:"capabilities"`
	}
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	updated, err := s.store.Activate(member.MailboxID, req.Generation, req.NativeSessionID, req.Capabilities)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"mailbox": updated})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request, member *Member) {
	var req struct {
		Generation      string `json:"generation"`
		NativeSessionID string `json:"native_session_id"`
	}
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	updated, err := s.store.Heartbeat(member.MailboxID, req.Generation, req.NativeSessionID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"mailbox": updated})
}

func (s *Server) handleDeactivate(w http.ResponseWriter, r *http.Request, member *Member) {
	var req struct {
		Generation string `json:"generation"`
	}
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	updated, err := s.store.Deactivate(member.MailboxID, req.Generation)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"mailbox": updated})
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request, member *Member) {
	var req SendRequest
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	if req.Body == "" || req.To == "" || req.ClientMessageID == "" {
		writeError(w, errInvalidRequest)
		return
	}
	if len(req.Body) > MaxBodyBytes {
		writeError(w, errPayloadTooLarge)
		return
	}
	if !utf8.ValidString(req.Body) {
		writeError(w, errInvalidRequest)
		return
	}
	message, created, err := s.store.Send(member, req)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"message": message})
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request, member *Member) {
	message, err := s.store.MessageFor(member, r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": message})
}

func (s *Server) handleInbox(w http.ResponseWriter, r *http.Request, member *Member) {
	after, apiErr := parseAfter(r.URL.Query().Get("after"))
	if apiErr != nil {
		writeError(w, apiErr)
		return
	}
	limit := DefaultInboxLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, errInvalidRequest)
			return
		}
		limit = parsed
	}
	messages, cursor, err := s.store.Inbox(member.MailboxID, after, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"messages": messages,
		"cursor":   strconv.FormatInt(cursor, 10),
	})
}

func parseAfter(raw string) (int64, *apiError) {
	if raw == "" {
		return 0, nil
	}
	after, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || after < 0 {
		return 0, errInvalidCursor
	}
	return after, nil
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request, member *Member) {
	var req struct {
		Generation string `json:"generation"`
		RequestID  string `json:"request_id"`
	}
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	message, attemptID, err := s.store.Claim(member.MailboxID, req.Generation, req.RequestID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": message, "attempt_id": nullable(attemptID)})
}

func (s *Server) handleReceipt(w http.ResponseWriter, r *http.Request, member *Member) {
	var req struct {
		Generation string `json:"generation"`
		MessageID  string `json:"message_id"`
		AttemptID  string `json:"attempt_id"`
		State      string `json:"state"`
		Evidence   string `json:"evidence"`
	}
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	message, err := s.store.Receipt(member.MailboxID, req.Generation, req.MessageID, req.AttemptID, req.State, req.Evidence)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": message})
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request, member *Member) {
	var req struct {
		MessageID string `json:"message_id"`
	}
	if apiErr := decodeBody(w, r, &req); apiErr != nil {
		writeError(w, apiErr)
		return
	}
	message, err := s.store.Ack(member.MailboxID, req.MessageID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": message})
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) *apiError {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRequestBody))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return errPayloadTooLarge
		}
		return errInvalidRequest
	}
	// JSON is UTF-8 by definition; reject raw invalid bytes instead of letting
	// the decoder silently replace them with U+FFFD.
	if !utf8.Valid(body) {
		return errInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return errInvalidRequest
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errInvalidRequest
	}
	return nil
}

// fail maps a store error to its stable code, logging only the fact of an
// internal failure (never the raw exception, request body, or token).
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	apiErr := apiErrorFor(err)
	if apiErr.status >= 500 {
		s.logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "code", apiErr.code)
	}
	writeError(w, apiErr)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent; nothing safe remains to report.
		return
	}
}

func writeError(w http.ResponseWriter, apiErr *apiError) {
	writeJSON(w, apiErr.status, map[string]string{"error": apiErr.code})
}

// nullable renders an empty string as JSON null.
func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		// Method/path/status only: never tokens, bodies, or raw errors.
		s.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}
