package mailbox

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// doCall performs one JSON request and returns status plus decoded body.
func doCall(t *testing.T, client *http.Client, baseURL, token, method, path string, payload any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, baseURL+path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("decode response %q: %v", raw, err)
		}
	}
	return resp.StatusCode, body
}

type testEnv struct {
	t      *testing.T
	store  *Store
	server *Server
	ts     *httptest.Server
	admin  string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	store := openTestStore(t)
	server := NewServer(store, "admin-token-test", quietLogger())
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return &testEnv{t: t, store: store, server: server, ts: ts, admin: "admin-token-test"}
}

func (e *testEnv) call(token, method, path string, payload any) (int, map[string]any) {
	e.t.Helper()
	return doCall(e.t, e.ts.Client(), e.ts.URL, token, method, path, payload)
}

// registerMemberHTTP registers through the API and returns mailbox id and token.
func (e *testEnv) registerMemberHTTP(key string, mutate func(map[string]any)) (string, string) {
	e.t.Helper()
	payload := map[string]any{
		"registration_key": key,
		"token":            "token-" + key,
		"label":            key,
		"kind":             "test",
		"scope":            "scope-1",
		"team":             "team-1",
	}
	if mutate != nil {
		mutate(payload)
	}
	status, body := e.call(e.admin, "POST", "/v1/register", payload)
	if status != http.StatusCreated {
		e.t.Fatalf("register %s: status %d body %v", key, status, body)
	}
	mailbox, ok := body["mailbox"].(map[string]any)
	if !ok {
		e.t.Fatalf("register %s: missing mailbox in %v", key, body)
	}
	return mailbox["mailbox_id"].(string), "token-" + key
}

func (e *testEnv) activate(token, generation string) map[string]any {
	e.t.Helper()
	status, body := e.call(token, "POST", "/v1/activate", map[string]any{"generation": generation})
	if status != http.StatusOK {
		e.t.Fatalf("activate: status %d body %v", status, body)
	}
	return body["mailbox"].(map[string]any)
}

func TestHealthIsUnauthenticatedAndStable(t *testing.T) {
	env := newTestEnv(t)
	status, body := env.call("", "GET", "/v1/health", nil)
	if status != http.StatusOK {
		t.Fatalf("health status = %d body %v", status, body)
	}
	if body["protocol"] != ProtocolName || body["version"] != ProtocolVersion {
		t.Fatalf("health body = %v", body)
	}
	first := body["instance_id"]
	_, again := env.call("", "GET", "/v1/health", nil)
	if again["instance_id"] != first {
		t.Fatalf("instance_id changed within one process: %v != %v", again["instance_id"], first)
	}
	if env.server.InstanceID() == "" {
		t.Fatalf("server instance id is empty")
	}
}

func TestAuthErrors(t *testing.T) {
	env := newTestEnv(t)
	memberID, memberToken := env.registerMemberHTTP("member", nil)

	cases := []struct {
		name   string
		token  string
		method string
		path   string
		body   any
	}{
		{"no token", "", "GET", "/v1/me", nil},
		{"bad token", "nope", "GET", "/v1/me", nil},
		{"admin on member route", env.admin, "GET", "/v1/me", nil},
		{"member on admin route", memberToken, "POST", "/v1/register", map[string]any{"registration_key": "x", "token": "y", "scope": "s"}},
	}
	for _, tc := range cases {
		status, body := env.call(tc.token, tc.method, tc.path, tc.body)
		if status != http.StatusUnauthorized {
			t.Fatalf("%s: status %d body %v", tc.name, status, body)
		}
		if body["error"] != "unauthorized" {
			t.Fatalf("%s: error = %v", tc.name, body["error"])
		}
	}
	if memberID == "" {
		t.Fatalf("member id empty")
	}
}

func TestRegisterHTTPIdempotencyAndConflicts(t *testing.T) {
	env := newTestEnv(t)
	memberID, memberToken := env.registerMemberHTTP("reg-1", nil)

	status, body := env.call(env.admin, "POST", "/v1/register", map[string]any{
		"registration_key": "reg-1", "token": "token-reg-1", "label": "reg-1", "kind": "test",
		"scope": "scope-1", "team": "team-1",
	})
	if status != http.StatusOK {
		t.Fatalf("identical re-register status = %d body %v", status, body)
	}
	if got := body["mailbox"].(map[string]any)["mailbox_id"]; got != memberID {
		t.Fatalf("re-register mailbox = %v, want %v", got, memberID)
	}

	status, body = env.call(env.admin, "POST", "/v1/register", map[string]any{
		"registration_key": "reg-1", "token": "token-reg-1", "label": "changed", "kind": "test",
		"scope": "scope-1", "team": "team-1",
	})
	if status != http.StatusConflict || body["error"] != "registration_conflict" {
		t.Fatalf("changed re-register: status %d body %v", status, body)
	}

	status, body = env.call(env.admin, "POST", "/v1/register", map[string]any{
		"registration_key": "reg-2", "token": "token-reg-2", "scope": "",
	})
	if status != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("missing scope: status %d body %v", status, body)
	}

	status, body = env.call(memberToken, "GET", "/v1/me", nil)
	if status != http.StatusOK {
		t.Fatalf("me status = %d body %v", status, body)
	}
	if got := body["mailbox"].(map[string]any)["mailbox_id"]; got != memberID {
		t.Fatalf("me mailbox = %v, want %v", got, memberID)
	}
}

func TestSendClaimReceiptAckRoundTrip(t *testing.T) {
	env := newTestEnv(t)
	aliceID, aliceToken := env.registerMemberHTTP("alice", nil)
	bobID, bobToken := env.registerMemberHTTP("bob", nil)

	// Third member in another scope must not read the conversation.
	carolID, carolToken := env.registerMemberHTTP("carol", func(m map[string]any) { m["scope"] = "scope-2" })
	_ = carolID

	env.activate(bobToken, "bob-gen-1")

	status, body := env.call(aliceToken, "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobID, "body": "你好，世界", "task_id": "task-1",
	})
	if status != http.StatusCreated {
		t.Fatalf("send status %d body %v", status, body)
	}
	message := body["message"].(map[string]any)
	messageID := message["message_id"].(string)
	if message["sender_id"] != aliceID || message["state"] != StatePending {
		t.Fatalf("message = %v", message)
	}

	status, body = env.call(aliceToken, "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobID, "body": "你好，世界", "task_id": "task-1",
	})
	if status != http.StatusOK || body["message"].(map[string]any)["message_id"] != messageID {
		t.Fatalf("repeat send: status %d body %v", status, body)
	}

	status, body = env.call(aliceToken, "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobID, "body": "changed", "task_id": "task-1",
	})
	if status != http.StatusConflict || body["error"] != "idempotency_conflict" {
		t.Fatalf("conflicting send: status %d body %v", status, body)
	}

	if status, body := env.call(carolToken, "GET", "/v1/messages/"+messageID, nil); status != http.StatusNotFound {
		t.Fatalf("outsider read: status %d body %v", status, body)
	}

	status, body = env.call(bobToken, "GET", "/v1/inbox", nil)
	if status != http.StatusOK {
		t.Fatalf("inbox status %d body %v", status, body)
	}
	messages := body["messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["message_id"] != messageID {
		t.Fatalf("inbox = %v", body)
	}
	cursor := body["cursor"].(string)

	status, body = env.call(bobToken, "POST", "/v1/claim", map[string]any{"generation": "bob-gen-1", "request_id": "req-1"})
	if status != http.StatusOK {
		t.Fatalf("claim status %d body %v", status, body)
	}
	attempt := body["attempt_id"].(string)
	if body["message"].(map[string]any)["message_id"] != messageID || attempt == "" {
		t.Fatalf("claim = %v", body)
	}

	status, body = env.call(bobToken, "POST", "/v1/receipt", map[string]any{
		"generation": "bob-gen-1", "message_id": messageID, "attempt_id": attempt, "state": "accepted", "evidence": "engine ok",
	})
	if status != http.StatusOK || body["message"].(map[string]any)["state"] != StateAccepted {
		t.Fatalf("receipt: status %d body %v", status, body)
	}

	status, body = env.call(bobToken, "POST", "/v1/receipt", map[string]any{
		"generation": "bob-gen-1", "message_id": messageID, "attempt_id": attempt, "state": "offered",
	})
	if status != http.StatusOK || body["message"].(map[string]any)["state"] != StateAccepted {
		t.Fatalf("weaker receipt: status %d body %v", status, body)
	}
	message = body["message"].(map[string]any)
	if len(message["receipts"].([]any)) != 2 {
		t.Fatalf("receipts = %v, want two distinct entries", message["receipts"])
	}
	if message["claimed_by_generation"] != "bob-gen-1" || message["claimed_at"].(float64) == 0 {
		t.Fatalf("message missing public claim info: %v", message)
	}

	// The exact same receipt again must not grow the audit list.
	status, body = env.call(bobToken, "POST", "/v1/receipt", map[string]any{
		"generation": "bob-gen-1", "message_id": messageID, "attempt_id": attempt, "state": "accepted", "evidence": "engine ok",
	})
	if status != http.StatusOK {
		t.Fatalf("duplicate receipt: status %d body %v", status, body)
	}
	if got := len(body["message"].(map[string]any)["receipts"].([]any)); got != 2 {
		t.Fatalf("duplicate receipt grew audit list to %d", got)
	}

	status, body = env.call(bobToken, "POST", "/v1/ack", map[string]any{"message_id": messageID})
	if status != http.StatusOK || body["message"].(map[string]any)["state"] != StateAccepted {
		t.Fatalf("ack: status %d body %v", status, body)
	}
	if got := body["message"].(map[string]any)["received_at"].(float64); got == 0 {
		t.Fatalf("ack did not set received_at: %v", body)
	}

	status, body = env.call(bobToken, "GET", "/v1/inbox?after="+cursor, nil)
	if status != http.StatusOK || len(body["messages"].([]any)) != 0 {
		t.Fatalf("inbox after cursor: status %d body %v", status, body)
	}
}

func TestSendSizeAndValidationErrors(t *testing.T) {
	env := newTestEnv(t)
	aliceToken := "token-alice"
	env.registerMemberHTTP("alice", nil)
	bobID, _ := env.registerMemberHTTP("bob", nil)

	oversize := strings.Repeat("x", MaxBodyBytes+1)
	status, body := env.call(aliceToken, "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobID, "body": oversize,
	})
	if status != http.StatusRequestEntityTooLarge || body["error"] != "payload_too_large" {
		t.Fatalf("oversize body: status %d body %v", status, body)
	}

	status, body = env.call(aliceToken, "POST", "/v1/send", map[string]any{"to": bobID, "body": "hi"})
	if status != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("missing client id: status %d body %v", status, body)
	}

	status, body = env.call(aliceToken, "GET", "/v1/inbox?after=not-a-number", nil)
	if status != http.StatusBadRequest || body["error"] != "invalid_cursor" {
		t.Fatalf("bad cursor: status %d body %v", status, body)
	}
}

func TestOversizeRequestIsRejected(t *testing.T) {
	env := newTestEnv(t)
	env.registerMemberHTTP("alice", nil)
	huge := strings.Repeat("a", MaxRequestBody+1024)
	status, body := env.call("token-alice", "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": "mbx_x", "body": huge,
	})
	if status != http.StatusRequestEntityTooLarge || body["error"] != "payload_too_large" {
		t.Fatalf("oversize request: status %d body %v", status, body)
	}
}

func TestSenderCannotBeImpersonated(t *testing.T) {
	env := newTestEnv(t)
	aliceID, aliceToken := env.registerMemberHTTP("alice", nil)
	bobID, _ := env.registerMemberHTTP("bob", nil)

	status, body := env.call(aliceToken, "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobID, "body": "hi",
		"sender_id": bobID,
	})
	if status != http.StatusCreated {
		t.Fatalf("send status %d body %v", status, body)
	}
	if got := body["message"].(map[string]any)["sender_id"]; got != aliceID {
		t.Fatalf("sender_id = %v, want authenticated %v", got, aliceID)
	}
}

func TestUnknownPathAndWrongMethod(t *testing.T) {
	env := newTestEnv(t)
	status, body := env.call("", "GET", "/v1/does-not-exist", nil)
	if status != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("unknown path: status %d body %v", status, body)
	}

	status, body = env.call("", "DELETE", "/v1/send", nil)
	if status != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("wrong method: status %d body %v", status, body)
	}
}

func TestSendRejectsInvalidUTF8(t *testing.T) {
	env := newTestEnv(t)
	bobID, _ := env.registerMemberHTTP("bob", nil)
	env.registerMemberHTTP("alice", nil)

	raw := append([]byte(`{"client_message_id":"c-1","to":"`+bobID+`","body":"`), 0xff, 0xfe)
	raw = append(raw, []byte(`"}`)...)
	req, err := http.NewRequest("POST", env.ts.URL+"/v1/send", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer token-alice")
	req.Header.Set("Content-Type", "application/json")
	resp, err := env.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("invalid_request")) {
		t.Fatalf("invalid UTF-8: status %d body %s", resp.StatusCode, body)
	}
}

func TestSendAcceptsExactlyMaxBodyBytes(t *testing.T) {
	env := newTestEnv(t)
	bobID, _ := env.registerMemberHTTP("bob", nil)
	env.registerMemberHTTP("alice", nil)
	status, body := env.call("token-alice", "POST", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobID, "body": strings.Repeat("x", MaxBodyBytes),
	})
	if status != http.StatusCreated {
		t.Fatalf("exact max body: status %d body %v", status, body)
	}
}

func TestPeersResponseExposesOnlyPublicFields(t *testing.T) {
	env := newTestEnv(t)
	env.registerMemberHTTP("alice", nil)
	env.registerMemberHTTP("bob", nil)
	status, body := env.call("token-alice", "GET", "/v1/peers", nil)
	if status != http.StatusOK {
		t.Fatalf("peers status %d body %v", status, body)
	}
	peers := body["peers"].([]any)
	if len(peers) != 1 {
		t.Fatalf("peers = %v, want one", peers)
	}
	peer := peers[0].(map[string]any)
	want := map[string]bool{
		"mailbox_id": true, "label": true, "kind": true, "team": true,
		"state": true, "last_seen": true, "capabilities": true,
	}
	for key := range peer {
		if !want[key] {
			t.Fatalf("peer exposes unexpected field %q: %v", key, peer)
		}
	}
	if len(peer) != len(want) {
		t.Fatalf("peer fields = %v, want %d fields", peer, len(want))
	}
}

func TestRegisterMetadataKeepsNumberLiteral(t *testing.T) {
	env := newTestEnv(t)
	raw := []byte(`{"registration_key":"k1","token":"t1","scope":"s1","metadata":{"n":9007199254740993}}`)
	req, err := http.NewRequest("POST", env.ts.URL+"/v1/register", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+env.admin)
	req.Header.Set("Content-Type", "application/json")
	resp, err := env.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register status %d body %s", resp.StatusCode, body)
	}
	if !bytes.Contains(body, []byte("9007199254740993")) {
		t.Fatalf("metadata number was rounded: %s", body)
	}
}

func TestGenerationClaimAndCapabilityConflictCodes(t *testing.T) {
	env := newTestEnv(t)
	_, bobToken := env.registerMemberHTTP("bob", nil)

	status, body := env.call(bobToken, "POST", "/v1/activate", map[string]any{"generation": "g1", "capabilities": []string{"a"}})
	if status != http.StatusOK {
		t.Fatalf("activate g1: status %d body %v", status, body)
	}
	if status, body = env.call(bobToken, "POST", "/v1/activate", map[string]any{"generation": "g1", "capabilities": []string{"a"}}); status != http.StatusOK {
		t.Fatalf("idempotent activate: status %d body %v", status, body)
	}
	status, body = env.call(bobToken, "POST", "/v1/activate", map[string]any{"generation": "g1", "capabilities": []string{"b"}})
	if status != http.StatusConflict || body["error"] != "capability_conflict" {
		t.Fatalf("capability change: status %d body %v", status, body)
	}

	if status, body = env.call(bobToken, "POST", "/v1/claim", map[string]any{"generation": "g1", "request_id": "r-1"}); status != http.StatusOK {
		t.Fatalf("empty claim: status %d body %v", status, body)
	}
	if status, body = env.call(bobToken, "POST", "/v1/deactivate", map[string]any{"generation": "g1"}); status != http.StatusOK {
		t.Fatalf("deactivate g1: status %d body %v", status, body)
	}
	env.activate(bobToken, "g2")
	status, body = env.call(bobToken, "POST", "/v1/claim", map[string]any{"generation": "g2", "request_id": "r-1"})
	if status != http.StatusConflict || body["error"] != "request_id_conflict" {
		t.Fatalf("request id reuse: status %d body %v", status, body)
	}

	if status, body = env.call(bobToken, "POST", "/v1/deactivate", map[string]any{"generation": "g2"}); status != http.StatusOK {
		t.Fatalf("deactivate g2: status %d body %v", status, body)
	}
	status, body = env.call(bobToken, "POST", "/v1/activate", map[string]any{"generation": "g1", "capabilities": []string{"a"}})
	if status != http.StatusConflict || body["error"] != "generation_ended" {
		t.Fatalf("ended generation: status %d body %v", status, body)
	}
}

func TestUnregisteredPathsReturnJSONNotFound(t *testing.T) {
	env := newTestEnv(t)

	for _, path := range []string{"/", "/v2/health", "/v1/deeply/unknown"} {
		status, body := env.call("", "GET", path, nil)
		if status != http.StatusNotFound || body["error"] != "not_found" {
			t.Fatalf("GET %s: status %d body %v", path, status, body)
		}
	}

	// /v1 is unregistered and must answer JSON, not a redirect.
	noRedirect := &http.Client{
		Transport: env.ts.Client().Transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	status, body := doCall(t, noRedirect, env.ts.URL, "", "GET", "/v1", nil)
	if status != http.StatusNotFound || body["error"] != "not_found" {
		t.Fatalf("GET /v1: status %d body %v", status, body)
	}

	// Registered endpoints keep their behavior.
	if status, _ := env.call("", "GET", "/v1/health", nil); status != http.StatusOK {
		t.Fatalf("health regressed: status %d", status)
	}
	if status, _ := env.call("", "GET", "/v1/me", nil); status != http.StatusUnauthorized {
		t.Fatalf("me auth regressed: status %d", status)
	}
}
