package mailbox

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
)

// randomID builds an unguessable identifier with a readable prefix.
// crypto/rand failure is unrecoverable for this service, so it panics.
func randomID(prefix string, n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("agent-mailbox: crypto/rand failed: " + err.Error())
	}
	return prefix + hex.EncodeToString(b)
}

func newMailboxID() string  { return randomID("mbx_", 16) }
func newMessageID() string  { return randomID("msg_", 16) }
func newAttemptID() string  { return randomID("att_", 16) }
func newInstanceID() string { return randomID("inst_", 8) }

// newToken returns a fresh 256-bit bearer token.
func newToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("agent-mailbox: crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
