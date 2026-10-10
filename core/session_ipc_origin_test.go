package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type ipcOriginAgentSession struct {
	stubAgentSession
	id   atomic.Value
	live atomic.Bool
}

func newIPCOriginAgentSession(id string) *ipcOriginAgentSession {
	s := &ipcOriginAgentSession{}
	s.id.Store(id)
	s.live.Store(true)
	return s
}

func (s *ipcOriginAgentSession) CurrentSessionID() string { return s.id.Load().(string) }
func (s *ipcOriginAgentSession) Alive() bool              { return s.live.Load() }
func (s *ipcOriginAgentSession) Close() error             { s.live.Store(false); return nil }

type ipcOriginEnvAgent struct {
	sessionEnvRecordingAgent
	dir          string
	failResume   bool
	startOrigins []string
}

func (a *ipcOriginEnvAgent) GetWorkDir() string      { return a.dir }
func (a *ipcOriginEnvAgent) SupportsLiveInput() bool { return true }
func (a *ipcOriginEnvAgent) StartSession(_ context.Context, id string) (AgentSession, error) {
	a.startOrigins = append(a.startOrigins, a.EnvValue("CC_IPC_ORIGIN"))
	if a.failResume && id != "" {
		return nil, errors.New("test resume rejected")
	}
	return a.session, nil
}

func ipcOriginFixture(t *testing.T, id string) (*APIServer, *Engine, *ipcOriginEnvAgent, *ipcOriginAgentSession, ipcOriginRequest) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	as := newIPCOriginAgentSession(id)
	a := &ipcOriginEnvAgent{sessionEnvRecordingAgent: sessionEnvRecordingAgent{session: as}, dir: dir}
	p := &ipcTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("project", a, []Platform{p}, "", LangEnglish)
	t.Cleanup(e.cancel)
	session := e.sessions.GetOrCreateActive("test:owner")
	e.getOrCreateInteractiveStateWith("test:owner", p, "test:owner", session, e.sessions, nil, "")
	api := &APIServer{engines: map[string]*Engine{"project": e}}
	if err := api.initSessionIPC(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	origin := ipcOriginRequest{OriginID: a.EnvValue("CC_IPC_ORIGIN"), Project: "project", SessionKey: "test:owner", CWD: dir}
	return api, e, a, as, origin
}

func TestIPCOrigin_FirstInitBindsEmptySessionAndPersistsWithoutHandle(t *testing.T) {
	api, e, agent, as, origin := ipcOriginFixture(t, "")
	session := e.sessions.GetOrCreateActive(origin.SessionKey)
	session.SetName("first task")
	e.sessions.storePath = filepath.Join(t.TempDir(), "sessions.json")
	if !ipcOriginPattern.MatchString(origin.OriginID) || len(agent.startOrigins) != 1 || agent.startOrigins[0] != origin.OriginID {
		t.Fatal("origin was not fixed before StartSession")
	}
	// The adapter has parsed init, but its EventText has not reached core yet.
	as.id.Store("conversation-from-init")
	response := ipcPost(t, api.handleIPCOrigin, origin)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"agent_session_id":"conversation-from-init"`) {
		t.Fatal(response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), origin.OriginID) {
		t.Fatal("origin capability leaked into response")
	}
	reloaded := NewSessionManager(e.sessions.storePath)
	if reloaded.GetOrCreateActive(origin.SessionKey).GetAgentSessionID() != "conversation-from-init" ||
		reloaded.GetSessionName("conversation-from-init") != "first task" {
		t.Fatal("first init binding or its pending name was not saved")
	}
	data, err := os.ReadFile(e.sessions.storePath)
	if err != nil || strings.Contains(string(data), origin.OriginID) {
		t.Fatal("origin capability persisted", err)
	}
	if agent.EnvValue("CC_PROJECT") != origin.Project || agent.EnvValue("CC_SESSION_KEY") != origin.SessionKey {
		t.Fatal("existing session environment changed")
	}
}

func TestIPCOrigin_NotReadyDoesNotGuessAndExistingIDIsNeverOverwritten(t *testing.T) {
	api, e, _, as, origin := ipcOriginFixture(t, "")
	session := e.sessions.GetOrCreateActive(origin.SessionKey)
	response := ipcPost(t, api.handleIPCOrigin, origin)
	if response.Code != 409 || !strings.Contains(response.Body.String(), "ipc_origin_not_ready") {
		t.Fatal(response.Code, response.Body.String())
	}
	if session.GetAgentSessionID() != "" || len(e.sessions.AllSessions()) != 1 {
		t.Fatal("not-ready lookup created or guessed a binding")
	}
	session.SetAgentSessionID("existing-conversation", "stub")
	as.id.Store("different-live-conversation")
	response = ipcPost(t, api.handleIPCOrigin, origin)
	if response.Code != 409 || session.GetAgentSessionID() != "existing-conversation" {
		t.Fatal("origin lookup overwrote an existing conversation", response.Body.String())
	}
}

func TestIPCOrigin_RejectsChangedInstanceRouteAndDirectory(t *testing.T) {
	for _, change := range []string{"prefix", "handle", "project", "key", "cwd", "stopped", "closed", "switched", "restarted", "shutdown"} {
		t.Run(change, func(t *testing.T) {
			api, e, _, as, origin := ipcOriginFixture(t, "conversation-one")
			wantStatus := 409
			switch change {
			case "prefix":
				origin.OriginID = strings.Replace(origin.OriginID, "v1", "v2", 1)
				wantStatus = 400
			case "handle":
				origin.OriginID = "cc-origin-v1-" + strings.Repeat("0", 64)
			case "project":
				origin.Project = "other"
			case "key":
				origin.SessionKey = "test:other"
			case "cwd":
				origin.CWD += "/other"
			case "stopped":
				e.interactiveStates[origin.SessionKey].markStopped()
			case "closed":
				_ = as.Close()
			case "switched":
				e.sessions.NewSession(origin.SessionKey, "another")
			case "restarted":
				e.interactiveStates = make(map[string]*interactiveState)
			case "shutdown":
				e.cancel()
			}
			before := len(e.sessions.AllSessions())
			response := ipcPost(t, api.handleIPCOrigin, origin)
			if response.Code != wantStatus || len(e.sessions.AllSessions()) != before {
				t.Fatal(response.Code, response.Body.String())
			}
		})
	}
}

func TestIPCOrigin_ReusedInstanceKeepsHandleAndReplacementRotatesIt(t *testing.T) {
	api, e, agent, _, origin := ipcOriginFixture(t, "conversation-one")
	session := e.sessions.GetOrCreateActive(origin.SessionKey)
	p := e.platforms[0]
	first := e.interactiveStates[origin.SessionKey]
	if e.getOrCreateInteractiveStateWith(origin.SessionKey, p, origin.SessionKey, session, e.sessions, nil, "") != first ||
		agent.EnvValue("CC_IPC_ORIGIN") != origin.OriginID {
		t.Fatal("same live instance unexpectedly changed origin")
	}
	// Even another CC Session pointing at the same backend ID is a new owner.
	another := e.sessions.NewSession(origin.SessionKey, "another")
	another.SetAgentSessionID("conversation-one", "stub")
	agent.session = newIPCOriginAgentSession("conversation-one")
	replacement := e.getOrCreateInteractiveStateWith(origin.SessionKey, p, origin.SessionKey, another, e.sessions, nil, "")
	if replacement == first || replacement.ipcOrigin == origin.OriginID {
		t.Fatal("inherited origin was retargeted to another CC session")
	}
	if response := ipcPost(t, api.handleIPCOrigin, origin); response.Code != 409 {
		t.Fatal("old instance origin remained valid")
	}
	origin.OriginID = agent.EnvValue("CC_IPC_ORIGIN")
	if response := ipcPost(t, api.handleIPCOrigin, origin); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
}

func TestIPCOrigin_ResumeFallbackNeverReusesFailedInstanceHandle(t *testing.T) {
	api, e, agent, _, origin := ipcOriginFixture(t, "old-conversation")
	e.cleanupInteractiveState(origin.SessionKey)
	agent.session = newIPCOriginAgentSession("fresh-conversation")
	agent.failResume = true
	agent.startOrigins = nil
	session := e.sessions.GetOrCreateActive(origin.SessionKey)
	e.getOrCreateInteractiveStateWith(origin.SessionKey, e.platforms[0], origin.SessionKey, session, e.sessions, nil, "")
	if len(agent.startOrigins) != 2 || agent.startOrigins[0] == agent.startOrigins[1] {
		t.Fatal("failed resume and fresh instance shared an origin")
	}
	origin.OriginID = agent.startOrigins[0]
	if response := ipcPost(t, api.handleIPCOrigin, origin); response.Code != 409 {
		t.Fatal("failed instance can resolve replacement origin")
	}
	origin.OriginID = agent.startOrigins[1]
	if response := ipcPost(t, api.handleIPCOrigin, origin); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
}
