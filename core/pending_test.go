package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func reviewPendingFixture() Event {
	return Event{Type: EventPending, Content: "unpublished-candidate", Pending: &PendingInfo{
		TaskID: "task-42", Reviewer: "Reviewer One", Stage: PendingStageReviewWaiting, Code: "review_running",
	}}
}

func TestPendingStatusText_LocalizedAndSafe(t *testing.T) {
	for _, lang := range []Language{LangEnglish, LangChinese, LangTraditionalChinese, LangJapanese, LangSpanish} {
		for _, stage := range []string{PendingStageReviewWaiting, PendingStagePreparation, PendingStageChangesRequired, ""} {
			i18n := NewI18n(lang)
			text := pendingStatusText(i18n, &PendingInfo{Stage: stage, Reviewer: "Reviewer One", TaskID: "task-42", Code: "review_running"})
			if !strings.Contains(text, "task-42") || !strings.Contains(text, "review_running") || strings.Contains(text, "%!") {
				t.Fatalf("missing or invalid localized pending status for %s/%s: %q", lang, stage, text)
			}
			if lang != LangEnglish && strings.Contains(text, "The conversation") {
				t.Fatalf("pending status fell back to English for %s/%s: %q", lang, stage, text)
			}
		}
	}
	unsafe := &PendingInfo{Stage: PendingStageReviewWaiting, TaskID: "/private/task", Reviewer: "name\nsecret", Code: "token=private-value"}
	text := pendingStatusText(NewI18n(LangEnglish), unsafe)
	for _, hidden := range []string{"/private", "secret", "private-value"} {
		if strings.Contains(text, hidden) {
			t.Fatalf("unsafe diagnostic escaped: %q", text)
		}
	}
}

func runPendingEventFixture(t *testing.T, p Platform, rich bool) (*Engine, *Session, *interactiveState) {
	t.Helper()
	e := NewEngine("test", &stubAgent{}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.SetStreamPreviewCfg(StreamPreviewCfg{Enabled: false})
	cfg := DisplayCfg{ThinkingMessages: true, ToolMessages: true}
	if rich {
		cfg.CardMode = "rich"
	}
	e.SetDisplayConfig(cfg)
	t.Cleanup(func() { _ = e.Stop() })
	key := "test:pending"
	session := e.sessions.GetOrCreateActive(key)
	backend := newControllableSession("pending-conversation")
	state := &interactiveState{agentSession: backend, platform: p, replyCtx: "original-context", eventsNeedResync: true}
	e.interactiveStates[key] = state
	backend.events <- Event{Type: EventThinking, Content: "Preparing review"}
	backend.events <- Event{Type: EventText, Content: "buffered-unpublished-draft"}
	backend.events <- reviewPendingFixture()
	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "pending-message", time.Now(), nil, nil, state.replyCtx, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pending event did not end the turn")
	}
	return e, session, state
}

func TestPendingTurn_DoesNotPublishCandidateOrMarkSuccess(t *testing.T) {
	p := &stubDoneReactionPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e, session, state := runPendingEventFixture(t, p, false)
	sent := strings.Join(p.getSent(), "\n")
	if !strings.Contains(sent, "Awaiting review by Reviewer One") || !strings.Contains(sent, "review_running") {
		t.Fatalf("pending status not visible: %q", sent)
	}
	for _, hidden := range []string{"unpublished", "❌", "(empty response)"} {
		if strings.Contains(sent, hidden) {
			t.Fatalf("pending turn leaked a candidate or false outcome: %q", sent)
		}
	}
	if count, _ := p.doneSnapshot(); count != 0 {
		t.Fatal("pending task received a done reaction")
	}
	if session.HistoryLen() != 0 {
		t.Fatal("pending candidate was recorded as assistant history")
	}
	if got := session.GetAgentSessionID(); got != "pending-conversation" {
		t.Fatalf("conversation lost: %q", got)
	}
	if got := NewSessionManager(e.sessions.storePath).GetOrCreateActive("test:pending").GetAgentSessionID(); got != "pending-conversation" {
		t.Fatalf("pending conversation was not persisted: %q", got)
	}
	state.mu.Lock()
	resync := state.eventsNeedResync
	state.mu.Unlock()
	if resync {
		t.Fatal("clean pending boundary was marked for error resync")
	}
}

func TestPendingTurn_ProgressCardsEndWithNeutralStatus(t *testing.T) {
	for _, rich := range []bool{false, true} {
		t.Run(fmt.Sprintf("rich=%t", rich), func(t *testing.T) {
			p := &stubCompactProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, style: "card", supportPayload: true}
			runPendingEventFixture(t, p, rich)
			edits := p.getPreviewEdits()
			if len(edits) == 0 {
				t.Fatal("active progress card was not finalized")
			}
			last := edits[len(edits)-1]
			if rich {
				if !strings.Contains(last, "status=pending") || !strings.Contains(last, "streaming=false") || !strings.Contains(last, "Awaiting review") {
					t.Fatalf("rich card did not become a neutral pending card: %s", last)
				}
				if strings.Contains(last, "unpublished") {
					t.Fatalf("rich card retained unpublished candidate: %s", last)
				}
			} else {
				payload, ok := ParseProgressCardPayload(last)
				if !ok || payload.State != ProgressCardStatePending {
					t.Fatalf("compact progress card not pending: %s", last)
				}
			}
		})
	}
}

func TestPendingTurn_StreamingCardEndsWithoutSuccessContent(t *testing.T) {
	p := &recordingStreamCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, card: &recordingStreamCard{}}
	runPendingEventFixture(t, p, false)
	if !p.card.finalized() || !strings.Contains(p.card.finalContent(), "Awaiting review") {
		t.Fatalf("streaming card did not end at pending: %q", p.card.finalContent())
	}
	if strings.Contains(p.card.finalContent(), "unpublished") || strings.Contains(p.card.finalContent(), "Done") {
		t.Fatalf("streaming card falsely delivered candidate: %q", p.card.finalContent())
	}
}

func TestUnsolicitedPending_ClearsCandidateBeforeLaterResult(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	key := "test:pending-background"
	session := e.sessions.GetOrCreateActive(key)
	backend := newControllableSession("background-conversation")
	state := &interactiveState{agentSession: backend, platform: p, replyCtx: "ctx"}
	e.interactiveStates[key] = state
	e.startUnsolicitedReader(state, session, e.sessions, key, "")
	defer e.stopUnsolicitedReader(state)
	backend.events <- Event{Type: EventText, Content: "unpublished-background-draft"}
	backend.events <- reviewPendingFixture()
	if got := waitForPlatformSend(p, 1, 2*time.Second); len(got) != 1 || !strings.Contains(got[0], "Awaiting review") {
		t.Fatalf("background pending missing: %v", got)
	}
	// An empty later result must not resurrect the draft from the pending turn.
	backend.events <- Event{Type: EventResult, Done: true}
	backend.events <- Event{Type: EventResult, Content: "approved-original-body", Done: true}
	got := waitForPlatformSend(p, 2, 2*time.Second)
	e.stopUnsolicitedReader(state)
	if len(got) != 2 || got[1] != "approved-original-body" {
		t.Fatalf("later result was mixed with pending draft: %v", got)
	}
	if session.GetAgentSessionID() != "background-conversation" {
		t.Fatal("background pending lost the conversation")
	}
	for _, entry := range session.GetHistory(0) {
		if strings.Contains(entry.Content, "unpublished") {
			t.Fatal("unpublished background draft entered history")
		}
	}
}

func TestRelayPending_ReturnsAcceptedWithoutCandidate(t *testing.T) {
	backend := newControllableSession("relay-pending-session")
	agent := &controllableAgent{nextSession: backend}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("target", agent, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	rm := NewRelayManager("")
	rm.RegisterEngine("target", e)
	rm.Bind("test", "chat-1", map[string]string{"source": "source", "target": "target"})
	rm.SetVisibility(RelayVisibilityNone)
	api := &APIServer{relay: rm}
	body, _ := json.Marshal(RelayRequest{From: "source", To: "target", SessionKey: "test:chat-1:user", Message: "prepare work"})
	backend.events <- Event{Type: EventText, Content: "unpublished-relay-draft"}
	backend.events <- reviewPendingFixture()
	rec := httptest.NewRecorder()
	api.handleRelaySend(rec, httptest.NewRequest(http.MethodPost, "/relay/send", bytes.NewReader(body)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("pending relay status = %d: %s", rec.Code, rec.Body.String())
	}
	var result RelayResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "pending" || result.Pending == nil || result.Pending.TaskID != "task-42" || strings.Contains(result.Response, "unpublished") {
		t.Fatalf("relay did not expose a structured pending outcome: %+v", result)
	}
	if session := e.sessions.GetOrCreateActive("relay:source:test:chat-1"); session.GetAgentSessionID() != "relay-pending-session" {
		t.Fatal("pending relay lost its resumable conversation")
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("one-shot relay session remained open after pending")
	}
}

func TestDrainRelaySession_PendingEndsBackgroundDrain(t *testing.T) {
	e := NewEngine("test", &stubAgent{}, nil, "", LangEnglish)
	defer e.Stop()
	backend := newControllableSession("drain-pending-session")
	session := e.sessions.GetOrCreateActive("relay:drain")
	backend.events <- reviewPendingFixture()
	done := make(chan struct{})
	go func() {
		e.drainRelaySession(backend, session, e.sessions, e.agent.Name(), "relay:drain")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pending turn left background relay drain running")
	}
	if session.GetAgentSessionID() != "drain-pending-session" {
		t.Fatal("pending background relay ID was not saved")
	}
}

func TestCompressPending_EndsTurnWithoutSuccessOrCandidate(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	key := "test:compress-pending"
	session := e.sessions.GetOrCreateActive(key)
	lockGen, _ := session.TryLock()
	backend := newControllableSession("compress-pending-conversation")
	state := &interactiveState{agentSession: backend, platform: p, replyCtx: "ctx", eventsNeedResync: true}
	e.interactiveStates[key] = state
	backend.events <- Event{Type: EventText, Content: "unpublished-compress-draft"}
	backend.events <- reviewPendingFixture()
	unlocked := false
	done := make(chan struct{})
	go func() {
		e.processCompressEvents(state, session, e.sessions, key, p, "ctx", &unlocked, false, lockGen)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("compress collector ignored terminal pending event")
	}
	text := strings.Join(p.getSent(), "\n")
	if !strings.Contains(text, "Awaiting review") || strings.Contains(text, "unpublished") || strings.Contains(text, e.i18n.T(MsgCompressDone)) {
		t.Fatalf("compress pending was delivered as success or candidate: %q", text)
	}
	if !unlocked || session.Busy() || session.HistoryLen() != 0 || state.eventsNeedResync {
		t.Fatal("compress pending failed to release a clean turn")
	}
}

func TestResumeUnavailable_NeverStartsFreshConversation(t *testing.T) {
	for _, relay := range []bool{false, true} {
		t.Run(fmt.Sprintf("relay=%t", relay), func(t *testing.T) {
			starts := []string{}
			agent := &controllableAgent{startSessionFn: func(_ context.Context, id string) (AgentSession, error) {
				starts = append(starts, id)
				return nil, fmt.Errorf("resume: %w", &ResumeUnavailableError{Err: errors.New("history index temporarily unavailable")})
			}}
			p := &stubPlatformEngine{n: "test"}
			store := filepath.Join(t.TempDir(), "sessions.json")
			e := NewEngine("test", agent, []Platform{p}, store, LangEnglish)
			defer e.Stop()
			key := "test:resume"
			if relay {
				key = "relay:source:test:chat-1"
			}
			session := e.sessions.GetOrCreateActive(key)
			session.SetAgentSessionID("original-conversation", agent.Name())
			e.sessions.Save()
			if relay {
				_, err := e.HandleRelay(context.Background(), "source", "test:chat-1:user", "continue")
				var unavailable *ResumeUnavailableError
				if !errors.As(err, &unavailable) {
					t.Fatalf("resume diagnostic was lost: %v", err)
				}
			} else if state := e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, ""); state.agentSession != nil {
				t.Fatal("unavailable resume unexpectedly started a new backend")
			}
			if len(starts) != 1 || starts[0] != "original-conversation" {
				t.Fatalf("resume unavailable attempted a fresh conversation: %v", starts)
			}
			if got := NewSessionManager(store).GetOrCreateActive(key).GetAgentSessionID(); got != "original-conversation" {
				t.Fatalf("unavailable history destroyed saved conversation: %q", got)
			}
		})
	}
}

func TestVerifiedReviewReceipt_RequiresCompleteHostMetadata(t *testing.T) {
	event := Event{Type: EventResult, Content: "untouched candidate", Done: true,
		Metadata: map[string]any{"verified_review": map[string]any{"reviewer": "Reviewer One", "task_id": "task-42", "revision": "rev-3"}}}
	if text := verifiedReviewReceipt(NewI18n(LangEnglish), event); !strings.Contains(text, "Reviewer One") || !strings.Contains(text, "rev-3") {
		t.Fatalf("verified review receipt missing: %q", text)
	}
	event.Metadata["verified_review"].(map[string]any)["revision"] = "secret/path"
	if text := verifiedReviewReceipt(NewI18n(LangEnglish), event); text != "" {
		t.Fatalf("unsafe host metadata was exposed: %q", text)
	}
	event = Event{Type: EventPending, Metadata: event.Metadata}
	if text := verifiedReviewReceipt(NewI18n(LangEnglish), event); text != "" {
		t.Fatalf("pending event claimed completed review: %q", text)
	}
}
