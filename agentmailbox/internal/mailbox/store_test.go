package mailbox

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "mailbox.sqlite3"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func registerMember(t *testing.T, store *Store, key string, mutate func(*RegisterRequest)) *Member {
	t.Helper()
	req := RegisterRequest{
		RegistrationKey: key,
		Token:           "token-" + key,
		Label:           key,
		Kind:            "test",
		Scope:           "scope-1",
		Team:            "team-1",
	}
	if mutate != nil {
		mutate(&req)
	}
	member, _, err := store.Register(req)
	if err != nil {
		t.Fatalf("Register(%s): %v", key, err)
	}
	return member
}

func activateMember(t *testing.T, store *Store, member *Member, generation string) *Member {
	t.Helper()
	updated, err := store.Activate(member.MailboxID, generation, "", nil)
	if err != nil {
		t.Fatalf("Activate(%s): %v", member.MailboxID, err)
	}
	return updated
}

func sendMessage(t *testing.T, store *Store, sender *Member, to, clientID, body string) *Message {
	t.Helper()
	message, _, err := store.Send(sender, SendRequest{ClientMessageID: clientID, To: to, Body: body, TaskID: "task-1"})
	if err != nil {
		t.Fatalf("Send(%s->%s): %v", sender.MailboxID, to, err)
	}
	return message
}

func TestRegisterIdempotentAndConflict(t *testing.T) {
	store := openTestStore(t)

	first, created, err := store.Register(RegisterRequest{
		RegistrationKey: "reg-1", Token: "tok-1", Label: "hermes", Kind: "hermes",
		Scope: "scope-1", Team: "team-1", Metadata: map[string]any{"cwd": "/tmp/x"},
	})
	if err != nil || !created {
		t.Fatalf("first register: created=%v err=%v", created, err)
	}

	same, created, err := store.Register(RegisterRequest{
		RegistrationKey: "reg-1", Token: "tok-1", Label: "hermes", Kind: "hermes",
		Scope: "scope-1", Team: "team-1", Metadata: map[string]any{"cwd": "/tmp/x"},
	})
	if err != nil || created {
		t.Fatalf("repeat register: created=%v err=%v", created, err)
	}
	if same.MailboxID != first.MailboxID {
		t.Fatalf("repeat register changed mailbox: %s != %s", same.MailboxID, first.MailboxID)
	}

	conflicts := []RegisterRequest{
		{RegistrationKey: "reg-1", Token: "tok-1", Label: "changed", Scope: "scope-1", Team: "team-1"},
		{RegistrationKey: "reg-1", Token: "other-token", Label: "hermes", Scope: "scope-1", Team: "team-1"},
		{RegistrationKey: "reg-1", Token: "tok-1", Label: "hermes", Scope: "scope-1", Team: "team-1", Metadata: map[string]any{"cwd": "/tmp/y"}},
	}
	for i, req := range conflicts {
		if _, _, err := store.Register(req); !errors.Is(err, ErrRegistrationConflict) {
			t.Fatalf("conflict %d: got %v, want ErrRegistrationConflict", i, err)
		}
	}

	if _, _, err := store.Register(RegisterRequest{
		RegistrationKey: "reg-2", Token: "tok-2", Scope: "other-scope", ParentID: first.MailboxID,
	}); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("cross-scope parent: got %v, want ErrInvalidParent", err)
	}
	if _, _, err := store.Register(RegisterRequest{
		RegistrationKey: "reg-3", Token: "tok-3", Scope: "scope-1", ParentID: "mbx_missing",
	}); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("missing parent: got %v, want ErrInvalidParent", err)
	}
}

func TestSendDeduplicatesBySenderAndClientID(t *testing.T) {
	store := openTestStore(t)
	alice := registerMember(t, store, "alice", nil)
	bob := registerMember(t, store, "bob", nil)
	carol := registerMember(t, store, "carol", nil)

	first := sendMessage(t, store, alice, bob.MailboxID, "c-1", "hello")
	again, created, err := store.Send(alice, SendRequest{ClientMessageID: "c-1", To: bob.MailboxID, Body: "hello", TaskID: "task-1"})
	if err != nil || created {
		t.Fatalf("repeat send: created=%v err=%v", created, err)
	}
	if again.MessageID != first.MessageID {
		t.Fatalf("repeat send changed message: %s != %s", again.MessageID, first.MessageID)
	}

	changedField := []SendRequest{
		{ClientMessageID: "c-1", To: bob.MailboxID, Body: "different", TaskID: "task-1"},
		{ClientMessageID: "c-1", To: carol.MailboxID, Body: "hello", TaskID: "task-1"},
		{ClientMessageID: "c-1", To: bob.MailboxID, Body: "hello", TaskID: "task-2"},
		{ClientMessageID: "c-1", To: bob.MailboxID, Body: "hello", TaskID: "task-1", ReplyTo: "msg_nope"},
	}
	for i, req := range changedField {
		if _, _, err := store.Send(alice, req); !errors.Is(err, ErrIdempotencyConflict) && !errors.Is(err, ErrReplyChainInvalid) {
			t.Fatalf("changed field %d: got %v, want idempotency/reply-chain error", i, err)
		}
	}

	// The same client id from a different sender is a different message.
	if _, created, err := store.Send(bob, SendRequest{ClientMessageID: "c-1", To: alice.MailboxID, Body: "hello"}); err != nil || !created {
		t.Fatalf("other sender same client id: created=%v err=%v", created, err)
	}
}

func TestSendPermissions(t *testing.T) {
	store := openTestStore(t)
	base := func() *Member { return registerMember(t, store, "base", nil) }
	sender := base()

	otherTeam := registerMember(t, store, "other-team", func(r *RegisterRequest) { r.Team = "team-2" })
	otherScope := registerMember(t, store, "other-scope", func(r *RegisterRequest) { r.Scope = "scope-2" })
	noTeam := registerMember(t, store, "no-team", func(r *RegisterRequest) { r.Team = "" })

	for _, target := range []*Member{otherTeam, otherScope, noTeam} {
		if _, _, err := store.Send(sender, SendRequest{ClientMessageID: "x", To: target.MailboxID, Body: "hi"}); !errors.Is(err, ErrPeerNotAllowed) {
			t.Fatalf("send to %s: got %v, want ErrPeerNotAllowed", target.Label, err)
		}
	}

	coordinator := registerMember(t, store, "coordinator", func(r *RegisterRequest) {
		r.Coordinator = true
		r.Team = "atlas"
	})
	child := registerMember(t, store, "child", func(r *RegisterRequest) {
		r.Team = "team-9"
		r.ParentID = coordinator.MailboxID
	})
	if _, _, err := store.Send(coordinator, SendRequest{ClientMessageID: "p-c", To: child.MailboxID, Body: "do it"}); err != nil {
		t.Fatalf("coordinator to child: %v", err)
	}
	if _, _, err := store.Send(child, SendRequest{ClientMessageID: "c-p", To: coordinator.MailboxID, Body: "done"}); err != nil {
		t.Fatalf("child to coordinator: %v", err)
	}
	stranger := registerMember(t, store, "stranger", func(r *RegisterRequest) { r.Team = "team-3" })
	if _, _, err := store.Send(stranger, SendRequest{ClientMessageID: "s-c", To: child.MailboxID, Body: "hi"}); !errors.Is(err, ErrPeerNotAllowed) {
		t.Fatalf("stranger to child: got %v, want ErrPeerNotAllowed", err)
	}
}

func TestReplyChainValidation(t *testing.T) {
	store := openTestStore(t)
	alice := registerMember(t, store, "alice", nil)
	bob := registerMember(t, store, "bob", nil)
	carol := registerMember(t, store, "carol", nil)

	original := sendMessage(t, store, alice, bob.MailboxID, "c-1", "question")

	if _, _, err := store.Send(bob, SendRequest{ClientMessageID: "c-2", To: alice.MailboxID, Body: "answer", ReplyTo: original.MessageID}); err != nil {
		t.Fatalf("valid reply: %v", err)
	}
	if _, _, err := store.Send(carol, SendRequest{ClientMessageID: "c-3", To: alice.MailboxID, Body: "me too", ReplyTo: original.MessageID}); !errors.Is(err, ErrReplyChainInvalid) {
		t.Fatalf("reply from third party: got %v, want ErrReplyChainInvalid", err)
	}
	if _, _, err := store.Send(bob, SendRequest{ClientMessageID: "c-4", To: alice.MailboxID, Body: "x", ReplyTo: "msg_missing"}); !errors.Is(err, ErrReplyChainInvalid) {
		t.Fatalf("reply to missing: got %v, want ErrReplyChainInvalid", err)
	}
	// A chain between bob and carol cannot be reused by alice.
	bc := sendMessage(t, store, bob, carol.MailboxID, "c-5", "bc")
	if _, _, err := store.Send(alice, SendRequest{ClientMessageID: "c-6", To: bob.MailboxID, Body: "x", ReplyTo: bc.MessageID}); !errors.Is(err, ErrReplyChainInvalid) {
		t.Fatalf("reply across chain: got %v, want ErrReplyChainInvalid", err)
	}
}

func TestClaimAtomicUnderConcurrency(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "gen-1")

	const total = 5
	for i := 0; i < total; i++ {
		sendMessage(t, store, sender, recipient.MailboxID, fmt.Sprintf("c-%d", i), fmt.Sprintf("body %d", i))
	}

	var wg sync.WaitGroup
	claimed := make([]*Message, total)
	attempts := make([]string, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, attempt, err := store.Claim(recipient.MailboxID, "gen-1", fmt.Sprintf("req-%d", i))
			if err != nil {
				t.Errorf("claim %d: %v", i, err)
				return
			}
			claimed[i] = msg
			attempts[i] = attempt
		}(i)
	}
	wg.Wait()

	seenIDs := map[string]bool{}
	seenAttempts := map[string]bool{}
	for i, msg := range claimed {
		if msg == nil {
			t.Fatalf("claim %d returned no message; %d pending available", i, total)
		}
		if seenIDs[msg.MessageID] {
			t.Fatalf("message %s claimed twice", msg.MessageID)
		}
		seenIDs[msg.MessageID] = true
		if msg.State != StateDispatching {
			t.Fatalf("claim %d state = %s, want dispatching", i, msg.State)
		}
		if attempts[i] == "" || seenAttempts[attempts[i]] {
			t.Fatalf("claim %d attempt_id = %q (duplicate or empty)", i, attempts[i])
		}
		seenAttempts[attempts[i]] = true
	}

	if msg, _, err := store.Claim(recipient.MailboxID, "gen-1", "req-empty"); err != nil || msg != nil {
		t.Fatalf("extra claim: msg=%v err=%v, want empty", msg, err)
	}
}

func TestClaimIdempotentIncludingEmpty(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)

	if _, _, err := store.Claim(recipient.MailboxID, "gen-1", "req-1"); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("claim before activate: err=%v", err)
	}

	activateMember(t, store, recipient, "gen-1")

	msg, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-empty")
	if err != nil || msg != nil || attempt != "" {
		t.Fatalf("empty claim: msg=%v attempt=%q err=%v", msg, attempt, err)
	}
	msg, attempt, err = store.Claim(recipient.MailboxID, "gen-1", "req-empty")
	if err != nil || msg != nil || attempt != "" {
		t.Fatalf("repeated empty claim: msg=%v attempt=%q err=%v", msg, attempt, err)
	}

	original := sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	msg, attempt, err = store.Claim(recipient.MailboxID, "gen-1", "req-2")
	if err != nil || msg == nil || attempt == "" {
		t.Fatalf("claim: msg=%v attempt=%q err=%v", msg, attempt, err)
	}
	again, againAttempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-2")
	if err != nil || again.MessageID != original.MessageID || againAttempt != attempt {
		t.Fatalf("repeated claim: %v/%q err=%v, want %s/%q", again, againAttempt, err, original.MessageID, attempt)
	}
	if msg, _, err := store.Claim(recipient.MailboxID, "gen-1", "req-3"); err != nil || msg != nil {
		t.Fatalf("claim after consume: msg=%v err=%v", msg, err)
	}
	if _, _, err := store.Claim(recipient.MailboxID, "gen-old", "req-4"); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("old generation claim: got %v, want ErrGenerationMismatch", err)
	}
}

func TestGenerationRules(t *testing.T) {
	store := openTestStore(t)
	member := registerMember(t, store, "member", nil)

	activated := activateMember(t, store, member, "gen-1")
	if !activated.Active || activated.Generation != "gen-1" {
		t.Fatalf("activate result: %+v", activated)
	}
	if _, err := store.Activate(member.MailboxID, "gen-1", "", nil); err != nil {
		t.Fatalf("idempotent activate: %v", err)
	}
	if _, err := store.Activate(member.MailboxID, "gen-2", "", nil); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("second active generation: got %v, want ErrGenerationMismatch", err)
	}
	if _, err := store.Heartbeat(member.MailboxID, "gen-2", ""); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("heartbeat old generation: got %v", err)
	}

	updated, err := store.Heartbeat(member.MailboxID, "gen-1", "native-1")
	if err != nil || updated.NativeSessionID != "native-1" {
		t.Fatalf("heartbeat fill native: %+v err=%v", updated, err)
	}
	if _, err := store.Heartbeat(member.MailboxID, "gen-1", "native-2"); !errors.Is(err, ErrNativeSessionMismatch) {
		t.Fatalf("conflicting native: got %v, want ErrNativeSessionMismatch", err)
	}
	if _, err := store.Heartbeat(member.MailboxID, "gen-1", "native-1"); err != nil {
		t.Fatalf("same native: %v", err)
	}

	if _, err := store.Deactivate(member.MailboxID, "gen-2"); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("deactivate wrong generation: got %v", err)
	}
	closed, err := store.Deactivate(member.MailboxID, "gen-1")
	if err != nil || closed.Active {
		t.Fatalf("deactivate: %+v err=%v", closed, err)
	}
	if _, err := store.Activate(member.MailboxID, "gen-1", "", nil); !errors.Is(err, ErrGenerationEnded) {
		t.Fatalf("ended generation reactivate: got %v, want ErrGenerationEnded", err)
	}
	reopened, err := store.Activate(member.MailboxID, "gen-2", "", nil)
	if err != nil || !reopened.Active || reopened.Generation != "gen-2" || reopened.NativeSessionID != "" {
		t.Fatalf("reactivate: %+v err=%v", reopened, err)
	}
}

func TestReceiptMonotonicAndIdempotent(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	other := registerMember(t, store, "other", nil)
	activateMember(t, store, recipient, "gen-1")

	sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	msg, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-1")
	if err != nil || msg == nil {
		t.Fatalf("claim: msg=%v err=%v", msg, err)
	}

	if _, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, "received", ""); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("receipt with state received: got %v, want ErrInvalidRequest", err)
	}
	if _, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, "att_wrong", StateAccepted, ""); !errors.Is(err, ErrAttemptMismatch) {
		t.Fatalf("receipt wrong attempt: got %v", err)
	}
	if _, err := store.Receipt(other.MailboxID, "gen-1", msg.MessageID, attempt, StateAccepted, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("receipt from other mailbox: got %v, want ErrNotFound", err)
	}
	if _, err := store.Receipt(recipient.MailboxID, "gen-2", msg.MessageID, attempt, StateAccepted, ""); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("receipt wrong generation: got %v", err)
	}

	accepted, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, StateAccepted, "engine accepted")
	if err != nil || accepted.State != StateAccepted {
		t.Fatalf("receipt accepted: %+v err=%v", accepted, err)
	}
	afterOffer, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, StateOffered, "")
	if err != nil || afterOffer.State != StateAccepted {
		t.Fatalf("weaker receipt: %+v err=%v, want accepted", afterOffer, err)
	}
	afterUnknown, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, StateUnknown, "")
	if err != nil || afterUnknown.State != StateAccepted {
		t.Fatalf("unknown receipt: %+v err=%v, want accepted", afterUnknown, err)
	}
	if _, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, StateAccepted, ""); err != nil {
		t.Fatalf("idempotent receipt: %v", err)
	}
}

func TestReceiptUpgradesUnknown(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "gen-1")

	sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	msg, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	unknown, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, StateUnknown, "timeout")
	if err != nil || unknown.State != StateUnknown {
		t.Fatalf("receipt unknown: %+v err=%v", unknown, err)
	}
	accepted, err := store.Receipt(recipient.MailboxID, "gen-1", msg.MessageID, attempt, StateAccepted, "late evidence")
	if err != nil || accepted.State != StateAccepted {
		t.Fatalf("upgrade unknown: %+v err=%v", accepted, err)
	}
}

func TestAckKeepsTransportStateAndBlocksClaim(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "gen-1")

	original := sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	if _, err := store.Ack(sender.MailboxID, original.MessageID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("sender ack: got %v, want ErrNotFound", err)
	}
	acked, err := store.Ack(recipient.MailboxID, original.MessageID)
	if err != nil || acked.State != StatePending || acked.ReceivedAt == 0 {
		t.Fatalf("ack: %+v err=%v, want pending with received_at", acked, err)
	}
	again, err := store.Ack(recipient.MailboxID, original.MessageID)
	if err != nil || again.ReceivedAt != acked.ReceivedAt || again.State != StatePending {
		t.Fatalf("repeat ack: %+v err=%v, want same received_at %d", again, err, acked.ReceivedAt)
	}

	// An explicitly acked pending message must not be auto-claimed.
	message, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-1")
	if err != nil || message != nil || attempt != "" {
		t.Fatalf("claim after ack: message=%v attempt=%q err=%v, want empty", message, attempt, err)
	}
	// It stays explicitly queryable with its original transport state.
	stored, err := store.MessageFor(recipient, original.MessageID)
	if err != nil || stored.State != StatePending || stored.ReceivedAt != acked.ReceivedAt {
		t.Fatalf("explicit query after ack: %+v err=%v", stored, err)
	}
}

func TestAckAfterAcceptedKeepsAcceptedState(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "gen-1")

	sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	claimed, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := store.Receipt(recipient.MailboxID, "gen-1", claimed.MessageID, attempt, StateAccepted, ""); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	acked, err := store.Ack(recipient.MailboxID, claimed.MessageID)
	if err != nil || acked.State != StateAccepted || acked.ReceivedAt == 0 {
		t.Fatalf("ack after accepted: %+v err=%v", acked, err)
	}
}

func TestMessageVisibility(t *testing.T) {
	store := openTestStore(t)
	coordinator := registerMember(t, store, "coordinator", func(r *RegisterRequest) {
		r.Coordinator = true
		r.Team = "atlas-team"
	})
	childA := registerMember(t, store, "child-a", func(r *RegisterRequest) {
		r.Team = "workers"
		r.ParentID = coordinator.MailboxID
	})
	childB := registerMember(t, store, "child-b", func(r *RegisterRequest) {
		r.Team = "workers"
		r.ParentID = coordinator.MailboxID
	})
	outsider := registerMember(t, store, "outsider", func(r *RegisterRequest) { r.Scope = "scope-2" })
	neighbor := registerMember(t, store, "neighbor", func(r *RegisterRequest) { r.Team = "team-3" })

	message := sendMessage(t, store, childA, childB.MailboxID, "c-1", "private note")

	for _, viewer := range []*Member{childA, childB, coordinator} {
		if _, err := store.MessageFor(viewer, message.MessageID); err != nil {
			t.Fatalf("%s should read message: %v", viewer.Label, err)
		}
	}
	for _, viewer := range []*Member{outsider, neighbor} {
		if _, err := store.MessageFor(viewer, message.MessageID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s should not read message: %v", viewer.Label, err)
		}
	}
}

func TestPeersFiltering(t *testing.T) {
	store := openTestStore(t)
	coordinator := registerMember(t, store, "coordinator", func(r *RegisterRequest) {
		r.Coordinator = true
		r.Team = "atlas-team"
	})
	childA := registerMember(t, store, "child-a", func(r *RegisterRequest) {
		r.Team = "workers"
		r.ParentID = coordinator.MailboxID
	})
	childB := registerMember(t, store, "child-b", func(r *RegisterRequest) {
		r.Team = "workers"
		r.ParentID = coordinator.MailboxID
	})
	outsider := registerMember(t, store, "outsider", func(r *RegisterRequest) { r.Scope = "scope-2" })
	neighbor := registerMember(t, store, "neighbor", func(r *RegisterRequest) { r.Team = "team-3" })

	assertPeers := func(member *Member, want ...*Member) {
		t.Helper()
		peers, err := store.Peers(member)
		if err != nil {
			t.Fatalf("Peers(%s): %v", member.Label, err)
		}
		got := map[string]bool{}
		for _, peer := range peers {
			got[peer.MailboxID] = true
		}
		for _, wantMember := range want {
			if !got[wantMember.MailboxID] {
				t.Fatalf("%s missing peer %s", member.Label, wantMember.Label)
			}
		}
		if len(peers) != len(want) {
			t.Fatalf("%s peers = %d, want %d", member.Label, len(peers), len(want))
		}
	}

	assertPeers(coordinator, childA, childB)
	assertPeers(childA, coordinator, childB)
	assertPeers(childB, coordinator, childA)
	assertPeers(outsider)
	assertPeers(neighbor)
}

func TestRestartPreservesDeliveryFacts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mailbox.sqlite3")

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "gen-1")

	first := sendMessage(t, store, sender, recipient.MailboxID, "c-1", "first")
	second := sendMessage(t, store, sender, recipient.MailboxID, "c-2", "second")
	claimed, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "req-1")
	if err != nil || claimed.MessageID != first.MessageID {
		t.Fatalf("claim: %+v/%q err=%v", claimed, attempt, err)
	}
	if _, err := store.Receipt(recipient.MailboxID, "gen-1", first.MessageID, attempt, StateAccepted, "ok"); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	third := sendMessage(t, store, sender, recipient.MailboxID, "c-3", "third")
	if _, _, err := store.Claim(recipient.MailboxID, "gen-1", "req-2"); err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if _, err := store.Receipt(recipient.MailboxID, "gen-1", second.MessageID, "att_missing", StateUnknown, ""); !errors.Is(err, ErrAttemptMismatch) {
		t.Fatalf("wrong attempt: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	stored, err := reopened.MessageFor(recipient, first.MessageID)
	if err != nil || stored.State != StateAccepted {
		t.Fatalf("accepted message after restart: %+v err=%v", stored, err)
	}
	// Repeating the original claim request returns the same attempt after restart.
	replayed, replayedAttempt, err := reopened.Claim(recipient.MailboxID, "gen-1", "req-1")
	if err != nil || replayed.MessageID != first.MessageID || replayedAttempt != attempt {
		t.Fatalf("claim replay after restart: %+v/%q err=%v", replayed, replayedAttempt, err)
	}

	// A new generation must not pick up the old dispatching message.
	if _, err := reopened.Deactivate(recipient.MailboxID, "gen-1"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := reopened.Activate(recipient.MailboxID, "gen-2", "", nil); err != nil {
		t.Fatalf("activate gen-2: %v", err)
	}
	next, nextAttempt, err := reopened.Claim(recipient.MailboxID, "gen-2", "req-new")
	if err != nil || next == nil || next.MessageID != third.MessageID || nextAttempt == "" {
		t.Fatalf("new generation claim: %+v/%q err=%v, want %s", next, nextAttempt, err, third.MessageID)
	}
	if next.MessageID == second.MessageID {
		t.Fatalf("new generation reclaimed old dispatching message")
	}
}

func TestInboxCursorPagination(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	for i := 0; i < 7; i++ {
		sendMessage(t, store, sender, recipient.MailboxID, fmt.Sprintf("c-%d", i), fmt.Sprintf("body %d", i))
	}

	seen := []string{}
	cursor := int64(0)
	for {
		messages, next, err := store.Inbox(recipient.MailboxID, cursor, 3)
		if err != nil {
			t.Fatalf("Inbox: %v", err)
		}
		if len(messages) == 0 {
			break
		}
		for _, message := range messages {
			seen = append(seen, message.MessageID)
		}
		cursor = next
	}
	if len(seen) != 7 {
		t.Fatalf("paged %d messages, want 7", len(seen))
	}
	unique := map[string]bool{}
	for _, id := range seen {
		if unique[id] {
			t.Fatalf("duplicate message %s in pagination", id)
		}
		unique[id] = true
	}
	// Inbox must not change delivery state.
	first, err := store.MessageFor(recipient, seen[0])
	if err != nil || first.State != StatePending {
		t.Fatalf("inbox changed state: %+v err=%v", first, err)
	}
}

func TestRegisterConflictOnOwnershipFields(t *testing.T) {
	store := openTestStore(t)
	base := RegisterRequest{RegistrationKey: "k", Token: "t", Label: "l", Kind: "k", Scope: "scope-1", Team: "team-1"}
	if _, _, err := store.Register(base); err != nil {
		t.Fatalf("base register: %v", err)
	}
	parent := registerMember(t, store, "parent", nil)
	variants := []RegisterRequest{
		{RegistrationKey: "k", Token: "t", Label: "l", Kind: "k", Scope: "scope-2", Team: "team-1"},
		{RegistrationKey: "k", Token: "t", Label: "l", Kind: "k", Scope: "scope-1", Team: "team-2"},
		{RegistrationKey: "k", Token: "t", Label: "l", Kind: "k", Coordinator: true, Scope: "scope-1", Team: "team-1"},
		{RegistrationKey: "k", Token: "t", Label: "l", Kind: "k", Scope: "scope-1", Team: "team-1", ParentID: parent.MailboxID},
	}
	for i, variant := range variants {
		if _, _, err := store.Register(variant); !errors.Is(err, ErrRegistrationConflict) {
			t.Fatalf("ownership variant %d: got %v, want ErrRegistrationConflict", i, err)
		}
	}
}

func TestClaimSameRequestIDConcurrent(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "gen-1")
	original := sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")

	const workers = 8
	messages := make([]*Message, workers)
	attempts := make([]string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			message, attempt, err := store.Claim(recipient.MailboxID, "gen-1", "same-request")
			if err != nil {
				t.Errorf("claim %d: %v", i, err)
				return
			}
			messages[i] = message
			attempts[i] = attempt
		}(i)
	}
	wg.Wait()

	for i := range messages {
		if messages[i] == nil || messages[i].MessageID != original.MessageID {
			t.Fatalf("claim %d = %+v, want message %s", i, messages[i], original.MessageID)
		}
		if attempts[i] == "" || attempts[i] != attempts[0] {
			t.Fatalf("claim %d attempt = %q, want %q", i, attempts[i], attempts[0])
		}
	}
}

func TestEndedGenerationCannotReactivate(t *testing.T) {
	store := openTestStore(t)
	member := registerMember(t, store, "member", nil)

	activateMember(t, store, member, "g1")
	if _, err := store.Deactivate(member.MailboxID, "g1"); err != nil {
		t.Fatalf("deactivate g1: %v", err)
	}
	activateMember(t, store, member, "g2")
	if _, err := store.Deactivate(member.MailboxID, "g2"); err != nil {
		t.Fatalf("deactivate g2: %v", err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "", nil); !errors.Is(err, ErrGenerationEnded) {
		t.Fatalf("reactivate ended g1: got %v, want ErrGenerationEnded", err)
	}
	if _, err := store.Activate(member.MailboxID, "g3", "", nil); err != nil {
		t.Fatalf("new generation g3: %v", err)
	}
}

func TestActiveGenerationCapabilitiesImmutable(t *testing.T) {
	store := openTestStore(t)
	member := registerMember(t, store, "member", nil)

	if _, err := store.Activate(member.MailboxID, "g1", "", []string{"read", "write"}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "", []string{"read", "write"}); err != nil {
		t.Fatalf("identical reactivate: %v", err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "native-1", []string{"read", "write"}); err != nil {
		t.Fatalf("native fill via activate: %v", err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "native-2", []string{"read", "write"}); !errors.Is(err, ErrNativeSessionMismatch) {
		t.Fatalf("native change: got %v, want ErrNativeSessionMismatch", err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "", []string{"read"}); !errors.Is(err, ErrCapabilityConflict) {
		t.Fatalf("capability change: got %v, want ErrCapabilityConflict", err)
	}
}

func TestClaimRequestIDBoundToGeneration(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "g1")

	first := sendMessage(t, store, sender, recipient.MailboxID, "c-1", "first")
	second := sendMessage(t, store, sender, recipient.MailboxID, "c-2", "second")

	claimed, attempt, err := store.Claim(recipient.MailboxID, "g1", "r-1")
	if err != nil || claimed.MessageID != first.MessageID {
		t.Fatalf("claim: %+v/%q err=%v", claimed, attempt, err)
	}
	if _, err := store.Deactivate(recipient.MailboxID, "g1"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := store.Activate(recipient.MailboxID, "g2", "", nil); err != nil {
		t.Fatalf("activate g2: %v", err)
	}
	if _, _, err := store.Claim(recipient.MailboxID, "g2", "r-1"); !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("reuse request id across generations: got %v, want ErrRequestIDConflict", err)
	}
	next, _, err := store.Claim(recipient.MailboxID, "g2", "r-2")
	if err != nil || next.MessageID != second.MessageID {
		t.Fatalf("new generation claim: %+v err=%v, want %s", next, err, second.MessageID)
	}
	replayed, replayedAttempt, err := store.Claim(recipient.MailboxID, "g1", "r-1")
	if err != nil || replayed.MessageID != first.MessageID || replayedAttempt != attempt {
		t.Fatalf("replay ended generation request: %+v/%q err=%v", replayed, replayedAttempt, err)
	}
}

func TestReceiptStillReconcilesAfterGenerationEnds(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "g1")

	sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	claimed, attempt, err := store.Claim(recipient.MailboxID, "g1", "r-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := store.Deactivate(recipient.MailboxID, "g1"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := store.Activate(recipient.MailboxID, "g2", "", nil); err != nil {
		t.Fatalf("activate g2: %v", err)
	}
	reconciled, err := store.Receipt(recipient.MailboxID, "g1", claimed.MessageID, attempt, StateAccepted, "late evidence")
	if err != nil || reconciled.State != StateAccepted {
		t.Fatalf("late reconciliation: %+v err=%v", reconciled, err)
	}
}

func TestRegisterRejectsNonCoordinatorParent(t *testing.T) {
	store := openTestStore(t)
	plain := registerMember(t, store, "plain", nil)
	if _, _, err := store.Register(RegisterRequest{
		RegistrationKey: "child", Token: "t", Scope: "scope-1", Team: "team-9", ParentID: plain.MailboxID,
	}); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("non-coordinator parent: got %v, want ErrInvalidParent", err)
	}
}

func TestCoordinatorFlagRequiredForChildMessaging(t *testing.T) {
	store := openTestStore(t)
	coordinator := registerMember(t, store, "coordinator", func(r *RegisterRequest) {
		r.Coordinator = true
		r.Team = "atlas-team"
	})
	child := registerMember(t, store, "child", func(r *RegisterRequest) {
		r.Team = "workers"
		r.ParentID = coordinator.MailboxID
	})
	if _, _, err := store.Send(coordinator, SendRequest{ClientMessageID: "c-1", To: child.MailboxID, Body: "hi"}); err != nil {
		t.Fatalf("coordinator to child: %v", err)
	}

	// Dropping the coordinator flag must remove the cross-team permission even
	// though the parent link remains.
	if _, err := store.db.Exec(`UPDATE members SET coordinator=0 WHERE mailbox_id=?`, coordinator.MailboxID); err != nil {
		t.Fatalf("clear coordinator flag: %v", err)
	}
	reloaded, err := store.MemberByID(coordinator.MailboxID)
	if err != nil {
		t.Fatalf("reload coordinator: %v", err)
	}
	if _, _, err := store.Send(reloaded, SendRequest{ClientMessageID: "c-2", To: child.MailboxID, Body: "hi"}); !errors.Is(err, ErrPeerNotAllowed) {
		t.Fatalf("parent without coordinator: got %v, want ErrPeerNotAllowed", err)
	}
	if _, _, err := store.Send(child, SendRequest{ClientMessageID: "c-3", To: coordinator.MailboxID, Body: "hi"}); !errors.Is(err, ErrPeerNotAllowed) {
		t.Fatalf("child to non-coordinator: got %v, want ErrPeerNotAllowed", err)
	}
}

func TestMessageExposesClaimAndDeduplicatedReceipts(t *testing.T) {
	store := openTestStore(t)
	sender := registerMember(t, store, "sender", nil)
	recipient := registerMember(t, store, "recipient", nil)
	activateMember(t, store, recipient, "g1")

	original := sendMessage(t, store, sender, recipient.MailboxID, "c-1", "hello")
	claimed, attempt, err := store.Claim(recipient.MailboxID, "g1", "r-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := store.Receipt(recipient.MailboxID, "g1", claimed.MessageID, attempt, StateAccepted, "engine accepted"); err != nil {
			t.Fatalf("receipt %d: %v", i, err)
		}
	}
	if _, err := store.Receipt(recipient.MailboxID, "g1", claimed.MessageID, attempt, StateOffered, "handed to hook"); err != nil {
		t.Fatalf("second evidence: %v", err)
	}

	viewed, err := store.MessageFor(sender, original.MessageID)
	if err != nil {
		t.Fatalf("sender read: %v", err)
	}
	if viewed.State != StateAccepted {
		t.Fatalf("state = %s, want accepted", viewed.State)
	}
	if viewed.ClaimedByGeneration != "g1" || viewed.ClaimedAt == 0 || viewed.AttemptID != attempt {
		t.Fatalf("claim info = %+v", viewed)
	}
	if len(viewed.Receipts) != 2 {
		t.Fatalf("receipts = %+v, want two distinct entries", viewed.Receipts)
	}

	inbox, _, err := store.Inbox(recipient.MailboxID, 0, 10)
	if err != nil || len(inbox) != 1 {
		t.Fatalf("inbox: %v err=%v", inbox, err)
	}
	if len(inbox[0].Receipts) != 2 {
		t.Fatalf("inbox receipts = %+v", inbox[0].Receipts)
	}
}

func TestRestartPreservesGenerationAndClaimDecisions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mailbox.sqlite3")

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	member := registerMember(t, store, "member", nil)
	activateMember(t, store, member, "g1")
	if _, _, err := store.Claim(member.MailboxID, "g1", "r-1"); err != nil {
		t.Fatalf("empty claim: %v", err)
	}
	if _, err := store.Deactivate(member.MailboxID, "g1"); err != nil {
		t.Fatalf("deactivate g1: %v", err)
	}
	activateMember(t, store, member, "g2")
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if _, err := reopened.Activate(member.MailboxID, "g1", "", nil); !errors.Is(err, ErrGenerationEnded) {
		t.Fatalf("ended generation after restart: got %v, want ErrGenerationEnded", err)
	}
	if _, _, err := reopened.Claim(member.MailboxID, "g2", "r-1"); !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("request id conflict after restart: got %v, want ErrRequestIDConflict", err)
	}
}

func TestActivateNormalizesNativeSessionID(t *testing.T) {
	store := openTestStore(t)
	member := registerMember(t, store, "member", nil)

	activated, err := store.Activate(member.MailboxID, "g1", " n1 ", nil)
	if err != nil || activated.NativeSessionID != "n1" {
		t.Fatalf("activate with padded native id: %+v err=%v", activated, err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "n1", nil); err != nil {
		t.Fatalf("repeat activate with trimmed id: %v", err)
	}
	if _, err := store.Heartbeat(member.MailboxID, "g1", " n1 "); err != nil {
		t.Fatalf("heartbeat with padded id: %v", err)
	}
	if _, err := store.Activate(member.MailboxID, "g1", "", nil); err != nil {
		t.Fatalf("activate with empty id keeps bound id: %v", err)
	}

	// A new generation starts empty and can be filled once.
	if _, err := store.Deactivate(member.MailboxID, "g1"); err != nil {
		t.Fatalf("deactivate g1: %v", err)
	}
	reopened, err := store.Activate(member.MailboxID, "g2", "  ", nil)
	if err != nil || reopened.NativeSessionID != "" {
		t.Fatalf("new generation with blank id: %+v err=%v", reopened, err)
	}
	filled, err := store.Heartbeat(member.MailboxID, "g2", " n2 ")
	if err != nil || filled.NativeSessionID != "n2" {
		t.Fatalf("heartbeat fill: %+v err=%v", filled, err)
	}
	if _, err := store.Activate(member.MailboxID, "g2", "n2", nil); err != nil {
		t.Fatalf("activate matching filled id: %v", err)
	}
}

func TestSendValidationPrecedesIdempotency(t *testing.T) {
	store := openTestStore(t)
	alice := registerMember(t, store, "alice", nil)
	bob := registerMember(t, store, "bob", nil)
	otherTeam := registerMember(t, store, "other-team", func(r *RegisterRequest) { r.Team = "team-2" })

	sendMessage(t, store, alice, bob.MailboxID, "c-1", "hello")

	// Same id and same valid content: deduplicated.
	if _, created, err := store.Send(alice, SendRequest{ClientMessageID: "c-1", To: bob.MailboxID, Body: "hello", TaskID: "task-1"}); err != nil || created {
		t.Fatalf("repeat send: created=%v err=%v", created, err)
	}
	// Same id but an unauthorized target is rejected by authorization first.
	if _, _, err := store.Send(alice, SendRequest{ClientMessageID: "c-1", To: otherTeam.MailboxID, Body: "hello", TaskID: "task-1"}); !errors.Is(err, ErrPeerNotAllowed) {
		t.Fatalf("invalid target with same id: got %v, want ErrPeerNotAllowed", err)
	}
	// Same id but an invalid reply chain is rejected by chain validation first.
	if _, _, err := store.Send(alice, SendRequest{ClientMessageID: "c-1", To: bob.MailboxID, Body: "hello", TaskID: "task-1", ReplyTo: "msg_missing"}); !errors.Is(err, ErrReplyChainInvalid) {
		t.Fatalf("invalid reply with same id: got %v, want ErrReplyChainInvalid", err)
	}
	// Same id, valid content but changed body: idempotency conflict.
	if _, _, err := store.Send(alice, SendRequest{ClientMessageID: "c-1", To: bob.MailboxID, Body: "changed", TaskID: "task-1"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed valid content: got %v, want ErrIdempotencyConflict", err)
	}
}

func TestOpenStoreRollsBackPartialSchemaOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mailbox.sqlite3")

	// Pre-create a conflicting messages table so the schema transaction fails
	// at the messages index, after several tables were created inside it.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE messages (bogus INTEGER)`); err != nil {
		raw.Close()
		t.Fatalf("pre-create messages: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	if _, err := OpenStore(path); err == nil {
		t.Fatalf("OpenStore succeeded despite a mid-schema failure")
	}

	check, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer check.Close()
	var partial int
	if err := check.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('members','generations','attempts','claims','receipts')`).Scan(&partial); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if partial != 0 {
		t.Fatalf("partial schema persisted: %d new tables", partial)
	}
	var version int
	if err := check.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != 0 {
		t.Fatalf("user_version = %d, want 0 after rollback", version)
	}
	var kept int
	if err := check.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='messages'`).Scan(&kept); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if kept != 1 {
		t.Fatalf("pre-existing messages table lost")
	}
}
