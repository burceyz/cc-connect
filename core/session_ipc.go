package core

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// LiveInputSteerer is optional. ErrNoActiveInput guarantees no input was sent.
// Every other error is ambiguous and must NOT be automatically retried.
type LiveInputSteerer interface {
	SteerInput(expectedSessionID, prompt string) (turnID string, err error)
}

var ErrNoActiveInput = errors.New("no active input turn")
var ipcEventIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var ipcOriginPattern = regexp.MustCompile(`^cc-origin-v1-[a-f0-9]{64}$`)

const sessionIPCProtocol = "cc-connect-session-ipc-v1"

// Origins are deliberately process-local. A persisted conversation is not
// evidence that a tool inherited its environment from this live instance.
func newIPCOrigin() (string, error) {
	id, err := newIPCIdentity()
	if err != nil {
		return "", err
	}
	return "cc-origin-v1-" + id, nil
}

func newIPCIdentity() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("ipc identity: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (state *interactiveState) ipcQueueIdentity() (string, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.ipcQueueInstanceID == "" {
		id, err := newIPCIdentity()
		if err != nil {
			return "", err
		}
		state.ipcQueueInstanceID = id
	}
	return state.ipcQueueInstanceID, nil
}

// IPCTarget pins the existing conversation, never a newest-session alias.
type IPCTarget struct {
	Project        string `json:"project"`
	SessionKey     string `json:"session_key"`
	SessionID      string `json:"session_id"`
	AgentSessionID string `json:"agent_session_id"`
	CWD            string `json:"cwd"`
}

type IPCInput struct {
	EventID string    `json:"event_id"`
	Target  IPCTarget `json:"target"`
	Prompt  string    `json:"prompt"`
}

type IPCReceipt struct {
	Input          IPCInput `json:"input"`
	SHA256         string   `json:"sha256"`
	Status         string   `json:"status"`
	Code           string   `json:"code,omitempty"`
	Method         string   `json:"method,omitempty"`
	TurnID         string   `json:"turn_id,omitempty"`
	Boot           string   `json:"boot"`
	AttemptID      string   `json:"attempt_id,omitempty"`
	InstanceID     string   `json:"instance_id,omitempty"`
	Phase          string   `json:"phase,omitempty"`
	QueuedAt       string   `json:"queued_at,omitempty"`
	ClaimedAt      string   `json:"claimed_at,omitempty"`
	InjectedAt     string   `json:"injected_at,omitempty"`
	UpdatedAt      string   `json:"updated_at"`
	AcknowledgedAt string   `json:"acknowledged_at,omitempty"`
}

func (s *APIServer) initSessionIPC(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("ipc receipt directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("ipc receipt permissions: %w", err)
	}
	boot := make([]byte, 16)
	if _, err := rand.Read(boot); err != nil {
		return fmt.Errorf("ipc boot identity: %w", err)
	}
	s.ipcDir, s.ipcBoot = dir, hex.EncodeToString(boot)
	return nil
}

func (s *APIServer) handleIPCHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	apiJSON(w, http.StatusOK, map[string]string{"protocol": sessionIPCProtocol, "transport": "unix"})
}

func ipcDecode(w http.ResponseWriter, r *http.Request, out any) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		http.Error(w, "invalid ipc request", http.StatusBadRequest)
		return false
	}
	if dec.Decode(new(any)) != io.EOF {
		http.Error(w, "one JSON object required", http.StatusBadRequest)
		return false
	}
	return true
}

// This endpoint is registered ONLY on the private Unix API, not HTTP webhooks.
func (s *APIServer) handleIPCTarget(w http.ResponseWriter, r *http.Request) {
	var target IPCTarget
	if !ipcDecode(w, r, &target) {
		return
	}
	e, session, err := s.resolveIPCTarget(&target, true)
	if err != nil {
		apiJSON(w, http.StatusConflict, map[string]string{"code": err.Error()})
		return
	}
	s.writeIPCTarget(w, e, session, target)
}

func (s *APIServer) writeIPCTarget(w http.ResponseWriter, e *Engine, session *Session, target IPCTarget) {
	canSteer := false
	if capable, ok := e.agent.(interface{ SupportsLiveInput() bool }); ok {
		canSteer = capable.SupportsLiveInput()
	}
	apiJSON(w, http.StatusOK, map[string]any{
		"protocol": sessionIPCProtocol, "target": target,
		"busy": session.Busy(), "supports_live_input": canSteer,
	})
}

func (s *APIServer) resolveIPCProject(project, directory string) (*Engine, error) {
	s.mu.RLock()
	e := s.engines[project]
	s.mu.RUnlock()
	if e == nil || e.multiWorkspace {
		return nil, errors.New("ipc_project_unsupported")
	}
	wd, ok := e.agent.(interface{ GetWorkDir() string })
	if !ok {
		return nil, errors.New("ipc_cwd_unavailable")
	}
	cwd, err := filepath.EvalSymlinks(wd.GetWorkDir())
	if err != nil || cwd != directory {
		return nil, errors.New("ipc_cwd_mismatch")
	}
	return e, nil
}

func (s *APIServer) resolveIPCTarget(target *IPCTarget, capture bool) (*Engine, *Session, error) {
	if target.Project == "" || target.SessionKey == "" || target.AgentSessionID == "" || target.CWD == "" {
		return nil, nil, errors.New("ipc_target_incomplete")
	}
	e, err := s.resolveIPCProject(target.Project, target.CWD)
	if err != nil {
		return nil, nil, err
	}
	e.sessions.mu.RLock()
	defer e.sessions.mu.RUnlock()
	session := e.sessions.sessions[e.sessions.activeSession[target.SessionKey]]
	if session == nil || session.GetAgentSessionID() != target.AgentSessionID {
		return nil, nil, errors.New("ipc_target_changed")
	}
	if capture && target.SessionID == "" {
		target.SessionID = session.ID
	}
	if target.SessionID != session.ID {
		return nil, nil, errors.New("ipc_target_changed")
	}
	return e, session, nil
}

type ipcOriginRequest struct {
	OriginID   string `json:"origin_id"`
	Project    string `json:"project"`
	SessionKey string `json:"session_key"`
	CWD        string `json:"cwd"`
}

// The caller supplies an instance handle, never an agent-side ID to adopt.
// This endpoint is registered only on the private Unix API.
func (s *APIServer) handleIPCOrigin(w http.ResponseWriter, r *http.Request) {
	var origin ipcOriginRequest
	if !ipcDecode(w, r, &origin) {
		return
	}
	if origin.OriginID == "" || origin.Project == "" || origin.SessionKey == "" || origin.CWD == "" {
		apiJSON(w, http.StatusBadRequest, map[string]string{"code": "ipc_origin_incomplete"})
		return
	}
	if !ipcOriginPattern.MatchString(origin.OriginID) {
		apiJSON(w, http.StatusBadRequest, map[string]string{"code": "ipc_origin_invalid"})
		return
	}
	e, err := s.resolveIPCProject(origin.Project, origin.CWD)
	if err != nil {
		apiJSON(w, http.StatusConflict, map[string]string{"code": err.Error()})
		return
	}
	session, target, bound, err := resolveIPCOrigin(e, origin)
	if err != nil {
		apiJSON(w, http.StatusConflict, map[string]string{"code": err.Error()})
		return
	}
	if bound {
		// Save only after releasing the interactive and session-map locks.
		// Preserve first-assignment naming when this wins the init-event race.
		if name := session.GetName(); name != "" && name != "session" && name != "default" {
			e.sessions.SetSessionName(target.AgentSessionID, name)
		}
		e.sessions.Save()
	}
	s.writeIPCTarget(w, e, session, target)
}

func resolveIPCOrigin(e *Engine, origin ipcOriginRequest) (*Session, IPCTarget, bool, error) {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	state := e.interactiveStates[origin.SessionKey]
	if state == nil {
		return nil, IPCTarget{}, false, errors.New("ipc_origin_changed")
	}
	state.mu.Lock()
	as, stopped := state.agentSession, state.stopped
	matching := state.ipcOrigin == origin.OriginID
	owner := state.ipcOriginSessionID
	state.mu.Unlock()
	if !matching || stopped || as == nil || !as.Alive() || e.ctx.Err() != nil {
		return nil, IPCTarget{}, false, errors.New("ipc_origin_changed")
	}
	e.sessions.mu.RLock()
	defer e.sessions.mu.RUnlock()
	session := e.sessions.sessions[e.sessions.activeSession[origin.SessionKey]]
	if session == nil || session.ID != owner {
		return nil, IPCTarget{}, false, errors.New("ipc_origin_changed")
	}
	id := as.CurrentSessionID()
	if id == "" || id == ContinueSession {
		return nil, IPCTarget{}, false, errors.New("ipc_origin_not_ready")
	}
	bound := false
	if session.GetAgentSessionID() == "" {
		bound = session.CompareAndSetAgentSessionID(id, e.agent.Name())
	}
	if session.GetAgentSessionID() != id {
		return nil, IPCTarget{}, false, errors.New("ipc_origin_changed")
	}
	target := IPCTarget{Project: origin.Project, SessionKey: origin.SessionKey,
		SessionID: session.ID, AgentSessionID: id, CWD: origin.CWD}
	return session, target, bound, nil
}

func (s *APIServer) ipcRead(id string) (*IPCReceipt, error) {
	if !ipcEventIDPattern.MatchString(id) {
		return nil, errors.New("invalid_ipc_event_id")
	}
	b, err := os.ReadFile(filepath.Join(s.ipcDir, id+".json"))
	if err != nil {
		return nil, err
	}
	var receipt IPCReceipt
	if err := json.Unmarshal(b, &receipt); err != nil {
		return nil, err
	}
	if receipt.Input.EventID != id {
		return nil, errors.New("ipc_receipt_mismatch")
	}
	if receipt.Status == "dispatching" && receipt.Boot != s.ipcBoot {
		receipt.Status, receipt.Code = "unknown", "receiver_restarted_during_send"
	}
	return &receipt, nil
}

// Caller holds ipcMu. Flush the attempt before any backend side effect.
func (s *APIServer) ipcSave(receipt *IPCReceipt) error {
	receipt.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	b, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.ipcDir, ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), filepath.Join(s.ipcDir, receipt.Input.EventID+".json")); err != nil {
		return err
	}
	dir, err := os.Open(s.ipcDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *APIServer) handleIPCEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.ipcMu.Lock()
		receipt, err := s.ipcRead(r.URL.Query().Get("event_id"))
		s.ipcMu.Unlock()
		if err != nil {
			apiJSON(w, http.StatusNotFound, map[string]string{"code": "ipc_receipt_unavailable"})
			return
		}
		apiJSON(w, http.StatusOK, receipt)
		return
	}
	var input IPCInput
	if !ipcDecode(w, r, &input) {
		return
	}
	if !ipcEventIDPattern.MatchString(input.EventID) || strings.TrimSpace(input.Prompt) == "" || len(input.Prompt) > 8192 {
		apiJSON(w, http.StatusBadRequest, map[string]string{"code": "ipc_input_invalid"})
		return
	}
	b, _ := json.Marshal(input)
	hash := sha256.Sum256(b)
	digest := hex.EncodeToString(hash[:])
	s.ipcMu.Lock()
	prior, err := s.ipcRead(input.EventID)
	if err == nil {
		if prior.SHA256 != digest {
			s.ipcMu.Unlock()
			apiJSON(w, http.StatusConflict, map[string]string{"code": "ipc_event_conflict"})
			return
		}
		if prior.Status != "waiting" {
			s.ipcMu.Unlock()
			apiJSON(w, http.StatusOK, prior)
			return
		}
	} else if !os.IsNotExist(err) {
		s.ipcMu.Unlock()
		apiJSON(w, http.StatusConflict, map[string]string{"code": "ipc_receipt_unreadable"})
		return
	}
	if prior != nil && (prior.Method == "queue" || prior.InstanceID != "") {
		previousAttempt, previousBoot := prior.AttemptID, prior.Boot
		// Never take interactiveMu while holding ipcMu: a backend Close may
		// report progress while its engine holds interactiveMu. Re-read after
		// taking the snapshot so another sender cannot race this safe retry.
		s.ipcMu.Unlock()
		retryEngine, retryState := s.ipcQueueRetryState(input.Target)
		s.ipcMu.Lock()
		prior, err = s.ipcRead(input.EventID)
		if err != nil || prior.SHA256 != digest {
			s.ipcMu.Unlock()
			apiJSON(w, http.StatusConflict, map[string]string{"code": "ipc_receipt_changed"})
			return
		}
		if prior.Status != "waiting" || prior.AttemptID != previousAttempt || prior.Boot != previousBoot {
			s.ipcMu.Unlock()
			apiJSON(w, http.StatusOK, prior)
			return
		}
		if !ipcQueueRetryValid(prior, s.ipcBoot, retryEngine, retryState) {
			prior.Status, prior.Code = "blocked", "ipc_queue_instance_changed"
			if err := s.ipcSave(prior); err != nil {
				s.ipcMu.Unlock()
				apiJSON(w, http.StatusInternalServerError, map[string]string{"code": "ipc_receipt_write_failed"})
				return
			}
			s.ipcMu.Unlock()
			apiJSON(w, http.StatusOK, prior)
			return
		}
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		s.ipcMu.Unlock()
		apiJSON(w, http.StatusInternalServerError, map[string]string{"code": "ipc_attempt_identity_failed"})
		return
	}
	receipt := &IPCReceipt{Input: input, SHA256: digest, Status: "dispatching", Boot: s.ipcBoot,
		AttemptID: hex.EncodeToString(nonce)}
	if prior != nil && (prior.Method == "queue" || prior.InstanceID != "") {
		receipt.InstanceID = prior.InstanceID
	}
	attempt := ipcAttempt{eventID: input.EventID, id: receipt.AttemptID, boot: receipt.Boot, instanceID: receipt.InstanceID}
	e, session, err := s.resolveIPCTarget(&input.Target, false)
	if err != nil {
		receipt.Status, receipt.Code = "blocked", err.Error()
	}
	if err := s.ipcSave(receipt); err != nil {
		s.ipcMu.Unlock()
		apiJSON(w, http.StatusInternalServerError, map[string]string{"code": "ipc_receipt_write_failed"})
		return
	}
	s.ipcMu.Unlock()
	if receipt.Status == "dispatching" {
		s.dispatchIPC(e, session, input, attempt)
	}
	s.ipcMu.Lock()
	result, err := s.ipcRead(input.EventID)
	s.ipcMu.Unlock()
	if err != nil {
		apiJSON(w, http.StatusInternalServerError, map[string]string{"code": "ipc_receipt_unavailable"})
		return
	}
	apiJSON(w, http.StatusOK, result)
}

// A retry from waiting is a distinct attempt, even within the same receiver
// boot. Late backend callbacks must never modify its replacement's receipt.
type ipcAttempt struct {
	eventID    string
	id         string
	boot       string
	instanceID string
}

var errIPCQueueInstanceChanged = errors.New("ipc_queue_instance_changed")

func (s *APIServer) ipcQueueRetryState(target IPCTarget) (*Engine, *interactiveState) {
	s.mu.RLock()
	e := s.engines[target.Project]
	s.mu.RUnlock()
	if e == nil {
		return nil, nil
	}
	e.interactiveMu.Lock()
	state := e.interactiveStates[target.SessionKey]
	e.interactiveMu.Unlock()
	return e, state
}

func ipcQueueRetryValid(receipt *IPCReceipt, boot string, e *Engine, state *interactiveState) bool {
	if receipt.Boot != boot || receipt.InstanceID == "" || e == nil || state == nil || e.ctx.Err() != nil {
		return false
	}
	state.mu.Lock()
	as := state.agentSession
	matching := !state.stopped && state.ipcQueueInstanceID == receipt.InstanceID
	state.mu.Unlock()
	return matching && as != nil && as.Alive()
}

// Queue retries that have become idle must never enter the normal restore
// path. Recheck the exact instance when the asynchronous engine turn starts
// and immediately before Send, including a stop after the HTTP idle lease.
func (e *Engine) currentIPCQueueBackend(key string, expected *interactiveState) AgentSession {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	if expected == nil || e.interactiveStates[key] != expected || e.ctx.Err() != nil {
		return nil
	}
	expected.mu.Lock()
	as, stopped := expected.agentSession, expected.stopped
	expected.mu.Unlock()
	if stopped || as == nil || !as.Alive() {
		return nil
	}
	return as
}

// Caller holds ipcMu.
func (s *APIServer) readIPCAttempt(attempt ipcAttempt) (*IPCReceipt, error) {
	receipt, err := s.ipcRead(attempt.eventID)
	if err != nil {
		return nil, err
	}
	if attempt.boot != s.ipcBoot || receipt.Boot != attempt.boot || receipt.AttemptID != attempt.id {
		return nil, errors.New("ipc_attempt_changed")
	}
	return receipt, nil
}

func (s *APIServer) finishIPC(attempt ipcAttempt, status, method, turn, code string) {
	s.ipcMu.Lock()
	defer s.ipcMu.Unlock()
	receipt, err := s.readIPCAttempt(attempt)
	if err != nil {
		slog.Debug("ipc attempt update skipped", "event_id", attempt.eventID)
		return
	}
	// Positive delivery evidence is immutable, including an acknowledgement
	// racing the backend's final failure or teardown report.
	if receipt.AcknowledgedAt != "" || receipt.Status != "dispatching" {
		return
	}
	receipt.Status, receipt.Method, receipt.TurnID, receipt.Code = status, method, turn, code
	if err := s.ipcSave(receipt); err != nil {
		slog.Error("ipc receipt update failed", "event_id", attempt.eventID)
	}
}

func (s *APIServer) dispatchIPC(e *Engine, session *Session, input IPCInput, attempt ipcAttempt) {
	lockGen, locked := session.TryLock()
	if !locked {
		s.steerIPC(e, input, attempt)
		return
	}
	var pinnedQueueState *interactiveState
	if attempt.instanceID != "" {
		retryEngine, retryState := s.ipcQueueRetryState(input.Target)
		s.ipcMu.Lock()
		boot := s.ipcBoot
		s.ipcMu.Unlock()
		if retryEngine != e || !ipcQueueRetryValid(&IPCReceipt{Boot: attempt.boot, InstanceID: attempt.instanceID}, boot, retryEngine, retryState) {
			session.UnlockWithoutUpdate(lockGen)
			s.finishIPC(attempt, "blocked", "queue", "", "ipc_queue_instance_changed")
			return
		}
		pinnedQueueState = retryState
	}
	// Revalidate after obtaining the idle-session lease; no implicit /switch.
	if _, current, err := s.resolveIPCTarget(&input.Target, false); err != nil || current != session {
		session.UnlockWithoutUpdate(lockGen)
		s.finishIPC(attempt, "blocked", "", "", "ipc_target_changed")
		return
	}
	var platform Platform
	for _, p := range e.platforms {
		if strings.HasPrefix(input.Target.SessionKey, p.Name()+":") {
			platform = p
			break
		}
	}
	rc, ok := platform.(ReplyContextReconstructor)
	if !ok {
		session.UnlockWithoutUpdate(lockGen)
		s.finishIPC(attempt, "blocked", "", "", "ipc_reply_target_unavailable")
		return
	}
	replyCtx, err := rc.ReconstructReplyCtx(input.Target.SessionKey)
	if err != nil {
		session.UnlockWithoutUpdate(lockGen)
		s.finishIPC(attempt, "blocked", "", "", "ipc_reply_target_unavailable")
		return
	}
	msg := &Message{
		SessionKey: input.Target.SessionKey, Platform: platform.Name(),
		MessageID: "ipc-" + input.EventID, UserID: "ipc", UserName: "ipc",
		Content: input.Prompt, ReplyCtx: replyCtx,
		ipcEventID:             input.EventID,
		ExpectedAgentSessionID: input.Target.AgentSessionID,
		ipcQueueState:          pinnedQueueState,
		OnAgentInput: func(err error) {
			status, method, code := "delivered", "start", ""
			if errors.Is(err, errIPCQueueInstanceChanged) {
				status, method, code = "blocked", "queue", errIPCQueueInstanceChanged.Error()
			} else if err != nil {
				status, code = "unknown", "agent_input_failed_or_unconfirmed"
			}
			s.finishIPC(attempt, status, method, "", code)
		},
	}
	if pinnedQueueState == nil {
		e.ensureInteractiveStateForQueueing(input.Target.SessionKey, platform, replyCtx)
	}
	go e.processInteractiveMessage(platform, msg, session, lockGen)
}

func (s *APIServer) steerIPC(e *Engine, input IPCInput, attempt ipcAttempt) {
	e.interactiveMu.Lock()
	state := e.interactiveStates[input.Target.SessionKey]
	var as AgentSession
	if state != nil {
		state.mu.Lock()
		as = state.agentSession
		state.mu.Unlock()
	}
	e.interactiveMu.Unlock()
	if attempt.instanceID != "" {
		s.ipcMu.Lock()
		boot := s.ipcBoot
		s.ipcMu.Unlock()
		if !ipcQueueRetryValid(&IPCReceipt{Boot: attempt.boot, InstanceID: attempt.instanceID}, boot, e, state) ||
			e.currentIPCQueueBackend(input.Target.SessionKey, state) != as {
			s.finishIPC(attempt, "blocked", "queue", "", errIPCQueueInstanceChanged.Error())
			return
		}
	}
	if as == nil || !as.Alive() {
		s.finishIPC(attempt, "waiting", "", "", "session_busy_without_live_input")
		return
	}
	steerer, ok := as.(LiveInputSteerer)
	if !ok {
		if queuer, ok := as.(LiveInputQueuer); ok {
			// The durable dispatching receipt already exists. Neither a long
			// invocation nor an unexpectedly slow backend may hold this HTTP
			// response open until the sender times out and freezes delivery.
			go s.queueIPC(e, state, as, queuer, input, attempt)
			return
		}
		s.finishIPC(attempt, "waiting", "", "", "session_busy_without_live_input")
		return
	}
	turn, err := steerer.SteerInput(input.Target.AgentSessionID, input.Prompt)
	if errors.Is(err, ErrNoActiveInput) {
		s.finishIPC(attempt, "waiting", "", "", "turn_not_ready")
	} else if err != nil {
		s.finishIPC(attempt, "unknown", "steer", "", "steer_unconfirmed")
	} else {
		s.finishIPC(attempt, "delivered", "steer", turn, "")
	}
}

func (s *APIServer) queueIPC(e *Engine, state *interactiveState, as AgentSession, queuer LiveInputQueuer, input IPCInput, attempt ipcAttempt) {
	instanceID, err := state.ipcQueueIdentity()
	if err != nil {
		s.finishIPC(attempt, "blocked", "queue", "", "ipc_queue_identity_unavailable")
		return
	}
	report := func(progress LiveInputProgress) error {
		return s.reportQueuedIPC(e, state, as, input, attempt, instanceID, progress)
	}
	// No API, interactive-state, or session-map lock is held across backend
	// code. Some backends report synchronously from QueueInput or Close.
	err = queuer.QueueInput(input.Target.AgentSessionID, input.EventID, input.Prompt, report)
	if errors.Is(err, ErrNoActiveInput) {
		if report(LiveInputProgress{Phase: LiveInputPhaseReleased, Code: "turn_not_ready"}) != nil {
			_ = report(LiveInputProgress{Phase: LiveInputPhaseUnknown, Code: "queue_release_unconfirmed"})
		}
	} else if err != nil {
		_ = report(LiveInputProgress{Phase: LiveInputPhaseUnknown, Code: "queue_unconfirmed"})
	}
}

// This check never acquires interactiveMu: a backend may call report during
// Close while the engine holds that lock to replace an instance.
func (s *APIServer) checkIPCQueueTarget(e *Engine, state *interactiveState, as AgentSession, input IPCInput) error {
	state.mu.Lock()
	current := !state.stopped && state.agentSession == as
	state.mu.Unlock()
	if !current || e.ctx.Err() != nil || !as.Alive() {
		return errors.New(LiveInputCodeStopped)
	}
	if _, _, err := s.resolveIPCTarget(&input.Target, false); err != nil {
		return err
	}
	if as.CurrentSessionID() != input.Target.AgentSessionID {
		return errors.New("ipc_target_changed")
	}
	return nil
}

func (s *APIServer) reportQueuedIPC(e *Engine, state *interactiveState, as AgentSession, input IPCInput, attempt ipcAttempt, instanceID string, progress LiveInputProgress) error {
	if err := progress.validate(); err != nil {
		return err
	}
	s.ipcMu.Lock()
	defer s.ipcMu.Unlock()
	receipt, err := s.readIPCAttempt(attempt)
	if err != nil {
		return err
	}
	if receipt.AcknowledgedAt != "" || receipt.Status != "dispatching" {
		// Ignore terminal evidence arriving late, but refuse a new queue or
		// claim so a late callback cannot authorize another input side effect.
		if progress.Phase == LiveInputPhaseUnknown || progress.Phase == LiveInputPhaseReleased ||
			(progress.Phase == LiveInputPhaseInjected && receipt.Status == "delivered") {
			return nil
		}
		return errors.New("ipc_attempt_finished")
	}
	if receipt.InstanceID != "" && receipt.InstanceID != instanceID {
		return s.rejectIPCQueue(receipt, "ipc_queue_instance_changed")
	}
	receipt.InstanceID = instanceID
	if progress.Phase == LiveInputPhaseReleased {
		state.mu.Lock()
		stopped := state.stopped || state.agentSession != as
		state.mu.Unlock()
		if stopped || e.ctx.Err() != nil {
			// ErrNoActiveInput can race an explicit stop before the backend
			// reports queued. Turning it into waiting would restart that task.
			progress.Code = LiveInputCodeStopped
		}
	}
	if progress.Phase == LiveInputPhaseQueued || progress.Phase == LiveInputPhaseClaimed {
		if err := s.checkIPCQueueTarget(e, state, as, input); err != nil {
			return s.rejectIPCQueue(receipt, err.Error())
		}
	}
	changed, transitionErr := applyIPCLiveProgress(receipt, progress)
	if changed {
		if err := s.ipcSave(receipt); err != nil {
			return err
		}
	}
	return transitionErr
}

// Caller holds ipcMu. Never describe a possibly exposed claim as unsent.
func (s *APIServer) rejectIPCQueue(receipt *IPCReceipt, code string) error {
	receipt.Status, receipt.Phase, receipt.Code, receipt.Method = "blocked", LiveInputPhaseReleased, code, "queue"
	if receipt.ClaimedAt != "" {
		receipt.Status, receipt.Phase = "unknown", LiveInputPhaseUnknown
	}
	if err := s.ipcSave(receipt); err != nil {
		return err
	}
	return errors.New(code)
}

// Returns changed even when freezing a backend protocol violation. In that
// case the caller must durably save the uncertainty before returning the error.
func applyIPCLiveProgress(receipt *IPCReceipt, progress LiveInputProgress) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	receipt.Method = "queue"
	if progress.TurnID != "" {
		receipt.TurnID = progress.TurnID
	}
	switch progress.Phase {
	case LiveInputPhaseQueued:
		if receipt.Phase != "" && receipt.Phase != LiveInputPhaseQueued {
			return false, errors.New("ipc_queue_progress_out_of_order")
		}
		if receipt.QueuedAt == "" {
			receipt.QueuedAt = now
		}
	case LiveInputPhaseClaimed:
		if receipt.Phase != LiveInputPhaseQueued {
			return false, errors.New("ipc_claim_not_available")
		}
		receipt.ClaimedAt = now
	case LiveInputPhaseInjected:
		if receipt.Phase != LiveInputPhaseClaimed {
			receipt.Status, receipt.Phase, receipt.Code = "unknown", LiveInputPhaseUnknown, "ipc_injection_without_claim"
			return true, errors.New("ipc_injection_without_claim")
		}
		receipt.Status, receipt.InjectedAt = "delivered", now
	case LiveInputPhaseReleased:
		if receipt.ClaimedAt != "" {
			receipt.Status, receipt.Phase, receipt.Code = "unknown", LiveInputPhaseUnknown, "ipc_release_after_claim"
			return true, errors.New("ipc_release_after_claim")
		}
		receipt.Status = "waiting"
		if progress.Code == LiveInputCodeStopped {
			receipt.Status = "blocked"
		}
	case LiveInputPhaseUnknown:
		receipt.Status = "unknown"
	}
	receipt.Phase, receipt.Code = progress.Phase, progress.Code
	if receipt.Status == "delivered" {
		receipt.Code = ""
	}
	return true, nil
}

func (s *APIServer) handleIPCAck(w http.ResponseWriter, r *http.Request) {
	var ack struct {
		EventID string    `json:"event_id"`
		Target  IPCTarget `json:"target"`
	}
	if !ipcDecode(w, r, &ack) {
		return
	}
	s.ipcMu.Lock()
	defer s.ipcMu.Unlock()
	receipt, err := s.ipcRead(ack.EventID)
	if err != nil || receipt.Input.Target != ack.Target {
		apiJSON(w, http.StatusConflict, map[string]string{"code": "ipc_ack_mismatch"})
		return
	}
	if _, _, err := s.resolveIPCTarget(&ack.Target, false); err != nil {
		apiJSON(w, http.StatusConflict, map[string]string{"code": "ipc_ack_target_changed"})
		return
	}
	if receipt.AcknowledgedAt == "" {
		receipt.AcknowledgedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	receipt.Status, receipt.Code = "delivered", ""
	if err := s.ipcSave(receipt); err != nil {
		apiJSON(w, http.StatusInternalServerError, map[string]string{"code": "ipc_receipt_write_failed"})
		return
	}
	apiJSON(w, http.StatusOK, receipt)
}
