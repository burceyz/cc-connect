package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ipcTestSession struct {
	stubAgentSession
	input      chan string
	events     chan Event
	steers     atomic.Int32
	steerError error
}

func (s *ipcTestSession) CurrentSessionID() string { return "thread-ipc" }
func (s *ipcTestSession) Events() <-chan Event     { return s.events }
func (s *ipcTestSession) Send(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.input <- prompt
	s.events <- Event{Type: EventText, Content: "callback received"}
	s.events <- Event{Type: EventResult, Done: true, SessionID: s.CurrentSessionID()}
	return nil
}
func (s *ipcTestSession) SteerInput(expected, prompt string) (string, error) {
	if expected != s.CurrentSessionID() {
		return "", errors.New("wrong thread")
	}
	s.steers.Add(1)
	if s.steerError != nil {
		return "", s.steerError
	}
	s.input <- prompt
	return "turn-ipc", nil
}

type ipcTestAgent struct {
	stubAgent
	dir     string
	session *ipcTestSession
}

func (a *ipcTestAgent) GetWorkDir() string      { return a.dir }
func (a *ipcTestAgent) SupportsLiveInput() bool { return true }
func (a *ipcTestAgent) StartSession(_ context.Context, id string) (AgentSession, error) {
	if id != a.session.CurrentSessionID() {
		return nil, errors.New("must resume pinned thread")
	}
	return a.session, nil
}

type ipcTestPlatform struct{ stubPlatformEngine }

func (p *ipcTestPlatform) ReconstructReplyCtx(key string) (any, error) { return key, nil }

func ipcFixture(t *testing.T, busy bool) (*APIServer, *Engine, *ipcTestSession, IPCInput) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	as := &ipcTestSession{input: make(chan string, 16), events: make(chan Event, 16)}
	p := &ipcTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("project", &ipcTestAgent{dir: dir, session: as}, []Platform{p}, "", LangEnglish)
	t.Cleanup(e.cancel)
	session := e.sessions.GetOrCreateActive("test:owner")
	session.SetAgentInfo(as.CurrentSessionID(), "stub", "owner")
	if busy {
		session.TryLock()
	}
	e.interactiveStates["test:owner"] = &interactiveState{agentSession: as, platform: p, replyCtx: "test:owner"}
	api := &APIServer{engines: map[string]*Engine{"project": e}}
	if err := api.initSessionIPC(filepath.Join(dir, "ipc")); err != nil {
		t.Fatal(err)
	}
	input := IPCInput{EventID: strings.Repeat("a", 64), Prompt: "callback metadata, no new authorization",
		Target: IPCTarget{Project: "project", SessionKey: "test:owner", SessionID: session.ID,
			AgentSessionID: as.CurrentSessionID(), CWD: dir}}
	return api, e, as, input
}

func ipcPost(t *testing.T, handler http.HandlerFunc, value any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodPost, "/ipc/event", bytes.NewReader(b)))
	return w
}

func ipcReceipt(t *testing.T, api *APIServer, id string) *IPCReceipt {
	t.Helper()
	api.ipcMu.Lock()
	defer api.ipcMu.Unlock()
	r, err := api.ipcRead(id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestIPC_ActiveSteerAndDuplicateAreOneInput(t *testing.T) {
	api, _, as, input := ipcFixture(t, true)
	for range 2 {
		if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 200 {
			t.Fatal(w.Body.String())
		}
	}
	r := ipcReceipt(t, api, input.EventID)
	if r.Status != "delivered" || r.Method != "steer" || r.TurnID != "turn-ipc" || as.steers.Load() != 1 {
		t.Fatalf("receipt=%+v steers=%d", r, as.steers.Load())
	}
	input.Prompt = "conflicting body"
	if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 409 {
		t.Fatal(w.Code)
	}
	if as.steers.Load() != 1 {
		t.Fatal("conflict reached agent")
	}
}

func TestIPC_IdleResumesPinnedSessionAndAcknowledges(t *testing.T) {
	api, e, as, input := ipcFixture(t, false)
	delete(e.interactiveStates, input.Target.SessionKey) // also covers process restart
	if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	select {
	case got := <-as.input:
		if !strings.Contains(got, input.Prompt) {
			t.Fatal(got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle callback not resumed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for ipcReceipt(t, api, input.EventID).Status == "dispatching" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r := ipcReceipt(t, api, input.EventID); r.Status != "delivered" || r.Method != "start" {
		t.Fatalf("%+v", r)
	}
	ack := map[string]any{"event_id": input.EventID, "target": input.Target}
	if w := ipcPost(t, api.handleIPCAck, ack); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if ipcReceipt(t, api, input.EventID).AcknowledgedAt == "" {
		t.Fatal("no explicit ack")
	}
	session := e.sessions.GetOrCreateActive(input.Target.SessionKey)
	deadline = time.Now().Add(2 * time.Second)
	for session.Busy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if session.Busy() {
		t.Fatal("completed IPC turn did not release its session lock")
	}
}

func TestIPC_IdleReplyTargetFailureReleasesSessionLock(t *testing.T) {
	api, e, as, input := ipcFixture(t, false)
	// A platform without reply-context reconstruction must reject delivery
	// without leaving the session locked against subsequent user input.
	e.platforms = []Platform{&stubPlatformEngine{n: "test"}}
	session := e.sessions.GetOrCreateActive(input.Target.SessionKey)
	updatedAt := session.UpdatedAt
	ipcPost(t, api.handleIPCEvent, input)
	if receipt := ipcReceipt(t, api, input.EventID); receipt.Status != "blocked" || receipt.Code != "ipc_reply_target_unavailable" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if session.Busy() || !session.UpdatedAt.Equal(updatedAt) {
		t.Fatal("rejected IPC delivery left a lock or updated conversation activity")
	}
	if len(as.input) != 0 {
		t.Fatal("rejected IPC delivery reached the agent")
	}
	gen, locked := session.TryLock()
	if !locked {
		t.Fatal("next turn cannot acquire the session")
	}
	session.UnlockWithoutUpdate(gen)
}

func TestIPC_PinRejectsProjectThreadSessionAndDirectoryChanges(t *testing.T) {
	for _, change := range []string{"project", "thread", "session", "cwd", "key"} {
		t.Run(change, func(t *testing.T) {
			api, e, as, input := ipcFixture(t, true)
			switch change {
			case "project":
				input.Target.Project = "typo"
			case "thread":
				input.Target.AgentSessionID = "other"
			case "session":
				input.Target.SessionID = "other"
			case "cwd":
				input.Target.CWD += "/other"
			case "key":
				input.Target.SessionKey = "test:other"
			}
			before := len(e.sessions.AllSessions())
			ipcPost(t, api.handleIPCEvent, input)
			if ipcReceipt(t, api, input.EventID).Status != "blocked" || as.steers.Load() != 0 {
				t.Fatal("target fence bypassed")
			}
			if len(e.sessions.AllSessions()) != before {
				t.Fatal("IPC created a replacement session")
			}
		})
	}
}

func TestIPC_BusyUnsupportedWaitsWithoutSendingThenIdleStarts(t *testing.T) {
	api, e, as, input := ipcFixture(t, true)
	e.interactiveStates[input.Target.SessionKey].agentSession = &stubAgentSession{}
	ipcPost(t, api.handleIPCEvent, input)
	if ipcReceipt(t, api, input.EventID).Status != "waiting" || as.steers.Load() != 0 {
		t.Fatal("busy input was not retained")
	}
	e.interactiveStates[input.Target.SessionKey].agentSession = as
	e.sessions.GetOrCreateActive(input.Target.SessionKey).ForceUnlock()
	ipcPost(t, api.handleIPCEvent, input)
	select {
	case <-as.input:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting callback was lost")
	}
	// Send and its durable receipt finish asynchronously. Wait for that boundary
	// before TempDir cleanup, not merely for the fake agent to see the input.
	deadline := time.Now().Add(2 * time.Second)
	for ipcReceipt(t, api, input.EventID).Status == "dispatching" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ipcReceipt(t, api, input.EventID).Status != "delivered" {
		t.Fatal("callback receipt did not finish")
	}
}

func TestIPC_UncertainSteerAndReceiverRestartNeverRetry(t *testing.T) {
	api, _, as, input := ipcFixture(t, true)
	as.steerError = errors.New("response lost")
	ipcPost(t, api.handleIPCEvent, input)
	ipcPost(t, api.handleIPCEvent, input)
	if r := ipcReceipt(t, api, input.EventID); r.Status != "unknown" || as.steers.Load() != 1 {
		t.Fatalf("%+v", r)
	}
	api.ipcMu.Lock()
	r, _ := api.ipcRead(input.EventID)
	r.Status = "dispatching"
	if err := api.ipcSave(r); err != nil {
		t.Fatal(err)
	}
	api.ipcBoot = "another-process"
	api.ipcMu.Unlock()
	ipcPost(t, api.handleIPCEvent, input)
	if r := ipcReceipt(t, api, input.EventID); r.Status != "unknown" || as.steers.Load() != 1 {
		t.Fatal("ambiguous delivery replayed")
	}
}

func TestIPC_ConcurrentDuplicatesAndOriginOnlyAck(t *testing.T) {
	api, _, as, input := ipcFixture(t, true)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() { defer wg.Done(); ipcPost(t, api.handleIPCEvent, input) }()
	}
	wg.Wait()
	if as.steers.Load() != 1 {
		t.Fatal(as.steers.Load())
	}
	wrong := input.Target
	wrong.AgentSessionID = "another-thread"
	if w := ipcPost(t, api.handleIPCAck, map[string]any{"event_id": input.EventID, "target": wrong}); w.Code != 409 {
		t.Fatal(w.Code)
	}
	if ipcReceipt(t, api, input.EventID).AcknowledgedAt != "" {
		t.Fatal("foreign ack accepted")
	}
}

func TestIPC_RequestValidationAndPrivateReceipts(t *testing.T) {
	api, _, as, input := ipcFixture(t, true)
	for _, body := range []string{`{"event_id":"../escape"}`, `{}`, `{"unknown":true}`, `{}` + `{}`, strings.Repeat("x", 20000)} {
		w := httptest.NewRecorder()
		api.handleIPCEvent(w, httptest.NewRequest(http.MethodPost, "/ipc/event", strings.NewReader(body)))
		if w.Code != 400 {
			t.Fatalf("code=%d", w.Code)
		}
	}
	input.Prompt = strings.Repeat("x", 8193)
	if w := ipcPost(t, api.handleIPCEvent, input); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if as.steers.Load() != 0 {
		t.Fatal("invalid request sent")
	}
	input.Prompt = "metadata"
	ipcPost(t, api.handleIPCEvent, input)
	info, err := os.Stat(filepath.Join(api.ipcDir, input.EventID+".json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("receipt is not private", err)
	}
}

func TestIPC_NoActiveTurnIsSafeToWaitAndCaptureDoesNotCreate(t *testing.T) {
	api, _, as, input := ipcFixture(t, true)
	as.steerError = ErrNoActiveInput
	ipcPost(t, api.handleIPCEvent, input)
	if ipcReceipt(t, api, input.EventID).Status != "waiting" {
		t.Fatal("no-active incorrectly marked sent")
	}
	as.steerError = nil
	ipcPost(t, api.handleIPCEvent, input)
	if ipcReceipt(t, api, input.EventID).Status != "delivered" {
		t.Fatal("safe wait did not resume")
	}
	target := input.Target
	target.SessionID = ""
	w := ipcPost(t, api.handleIPCTarget, target)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"session_id":"s1"`) {
		t.Fatal(w.Body.String())
	}
}

func TestIPC_PrivateUnixSocketRoundTrip(t *testing.T) {
	_, engine, as, input := ipcFixture(t, true)
	// Short path also works on macOS, where sockaddr_un has a small path limit.
	dir, err := os.MkdirTemp("/tmp", "cc-ipc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	api, err := NewAPIServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	api.RegisterEngine("project", engine)
	api.Start()
	defer api.Stop()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", api.SocketPath())
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	request := func(method, route string, value any) map[string]any {
		t.Helper()
		var body io.Reader
		if value != nil {
			data, _ := json.Marshal(value)
			body = bytes.NewReader(data)
		}
		req, _ := http.NewRequest(method, "http://localhost"+route, body)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatal(response.StatusCode)
		}
		var result map[string]any
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if request("GET", "/ipc/health", nil)["protocol"] != sessionIPCProtocol {
		t.Fatal("protocol route missing")
	}
	if request("POST", "/ipc/target", input.Target)["supports_live_input"] != true {
		t.Fatal("capability route missing")
	}
	if request("POST", "/ipc/event", input)["status"] != "delivered" {
		t.Fatal("event not delivered")
	}
	if request("GET", "/ipc/event?event_id="+input.EventID, nil)["method"] != "steer" {
		t.Fatal("receipt route missing")
	}
	if as.steers.Load() != 1 {
		t.Fatal("not exactly one backend input")
	}
	info, err := os.Stat(api.SocketPath())
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("socket not private", err)
	}
}
