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

const sessionIPCProtocol = "cc-connect-session-ipc-v1"

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
	canSteer := false
	if capable, ok := e.agent.(interface{ SupportsLiveInput() bool }); ok {
		canSteer = capable.SupportsLiveInput()
	}
	apiJSON(w, http.StatusOK, map[string]any{
		"protocol": sessionIPCProtocol, "target": target,
		"busy": session.Busy(), "supports_live_input": canSteer,
	})
}

func (s *APIServer) resolveIPCTarget(target *IPCTarget, capture bool) (*Engine, *Session, error) {
	if target.Project == "" || target.SessionKey == "" || target.AgentSessionID == "" || target.CWD == "" {
		return nil, nil, errors.New("ipc_target_incomplete")
	}
	s.mu.RLock()
	e := s.engines[target.Project]
	s.mu.RUnlock()
	if e == nil || e.multiWorkspace {
		return nil, nil, errors.New("ipc_project_unsupported")
	}
	wd, ok := e.agent.(interface{ GetWorkDir() string })
	if !ok {
		return nil, nil, errors.New("ipc_cwd_unavailable")
	}
	cwd, err := filepath.EvalSymlinks(wd.GetWorkDir())
	if err != nil || cwd != target.CWD {
		return nil, nil, errors.New("ipc_cwd_mismatch")
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
	receipt := &IPCReceipt{Input: input, SHA256: digest, Status: "dispatching", Boot: s.ipcBoot}
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
		s.dispatchIPC(e, session, input)
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

func (s *APIServer) finishIPC(id, status, method, turn, code string) {
	s.ipcMu.Lock()
	defer s.ipcMu.Unlock()
	receipt, err := s.ipcRead(id)
	if err != nil {
		slog.Error("ipc receipt read failed", "event_id", id)
		return
	}
	// An explicit agent acknowledgement is stronger evidence than a lost RPC reply.
	if receipt.AcknowledgedAt != "" {
		status, code = "delivered", ""
	}
	receipt.Status, receipt.Method, receipt.TurnID, receipt.Code = status, method, turn, code
	if err := s.ipcSave(receipt); err != nil {
		slog.Error("ipc receipt update failed", "event_id", id)
	}
}

func (s *APIServer) dispatchIPC(e *Engine, session *Session, input IPCInput) {
	lockGen, locked := session.TryLock()
	if !locked {
		s.steerIPC(e, input)
		return
	}
	// Revalidate after obtaining the idle-session lease; no implicit /switch.
	if _, current, err := s.resolveIPCTarget(&input.Target, false); err != nil || current != session {
		session.UnlockWithoutUpdate(lockGen)
		s.finishIPC(input.EventID, "blocked", "", "", "ipc_target_changed")
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
		s.finishIPC(input.EventID, "blocked", "", "", "ipc_reply_target_unavailable")
		return
	}
	replyCtx, err := rc.ReconstructReplyCtx(input.Target.SessionKey)
	if err != nil {
		session.UnlockWithoutUpdate(lockGen)
		s.finishIPC(input.EventID, "blocked", "", "", "ipc_reply_target_unavailable")
		return
	}
	msg := &Message{
		SessionKey: input.Target.SessionKey, Platform: platform.Name(),
		MessageID: "ipc-" + input.EventID, UserID: "ipc", UserName: "ipc",
		Content: input.Prompt, ReplyCtx: replyCtx,
		ExpectedAgentSessionID: input.Target.AgentSessionID,
		OnAgentInput: func(err error) {
			status, code := "delivered", ""
			if err != nil {
				status, code = "unknown", "agent_input_failed_or_unconfirmed"
			}
			s.finishIPC(input.EventID, status, "start", "", code)
		},
	}
	e.ensureInteractiveStateForQueueing(input.Target.SessionKey, platform, replyCtx)
	go e.processInteractiveMessage(platform, msg, session, lockGen)
}

func (s *APIServer) steerIPC(e *Engine, input IPCInput) {
	e.interactiveMu.Lock()
	state := e.interactiveStates[input.Target.SessionKey]
	var as AgentSession
	if state != nil {
		state.mu.Lock()
		as = state.agentSession
		state.mu.Unlock()
	}
	e.interactiveMu.Unlock()
	steerer, ok := as.(LiveInputSteerer)
	if !ok || !as.Alive() {
		s.finishIPC(input.EventID, "waiting", "", "", "session_busy_without_live_input")
		return
	}
	turn, err := steerer.SteerInput(input.Target.AgentSessionID, input.Prompt)
	if errors.Is(err, ErrNoActiveInput) {
		s.finishIPC(input.EventID, "waiting", "", "", "turn_not_ready")
	} else if err != nil {
		s.finishIPC(input.EventID, "unknown", "steer", "", "steer_unconfirmed")
	} else {
		s.finishIPC(input.EventID, "delivered", "steer", turn, "")
	}
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
