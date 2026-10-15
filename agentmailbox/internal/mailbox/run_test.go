package mailbox

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// shortRuntimeDir keeps the Unix socket path under the macOS 104-byte limit.
func shortRuntimeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "amb-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func startService(t *testing.T, dir string) (socketPath, adminToken string, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, dir, quietLogger()) }()

	socketPath = filepath.Join(dir, "api.sock")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("Run exited early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("service socket %s did not appear", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}

	token, err := os.ReadFile(filepath.Join(dir, "admin-token"))
	if err != nil {
		t.Fatalf("read admin token: %v", err)
	}
	adminToken = strings.TrimSpace(string(token))

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not stop after cancel")
		}
	}
	t.Cleanup(stop)
	return socketPath, adminToken, stop
}

func unixClient(socketPath string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

func sendJSON(t *testing.T, client *http.Client, baseURL, token, path string, payload map[string]any) map[string]any {
	t.Helper()
	status, body := doCall(t, client, baseURL, token, "POST", path, payload)
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("POST %s: status %d body %v", path, status, body)
	}
	return body
}

func TestRunServesWithPrivatePermissions(t *testing.T) {
	dir := shortRuntimeDir(t)
	socketPath, adminToken, stop := startService(t, dir)
	client := unixClient(socketPath)

	dirInfo, err := os.Stat(dir)
	if err != nil || dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("runtime dir mode = %v err=%v, want 0700", dirInfo.Mode().Perm(), err)
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v err=%v, want 0600 socket", socketInfo.Mode(), err)
	}
	tokenInfo, err := os.Stat(filepath.Join(dir, "admin-token"))
	if err != nil || tokenInfo.Mode().Perm() != 0o600 {
		t.Fatalf("admin token mode = %v err=%v, want 0600", tokenInfo.Mode().Perm(), err)
	}
	if adminToken == "" {
		t.Fatalf("admin token is empty")
	}

	status, body := doCall(t, client, "http://unix", "", "GET", "/v1/health", nil)
	if status != http.StatusOK || body["protocol"] != ProtocolName {
		t.Fatalf("health: status %d body %v", status, body)
	}
	status, body = doCall(t, client, "http://unix", adminToken, "POST", "/v1/register", map[string]any{
		"registration_key": "k1", "token": "t1", "scope": "s1", "team": "team-1",
	})
	if status != http.StatusCreated {
		t.Fatalf("register: status %d body %v", status, body)
	}

	stop()
	if _, err := os.Lstat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("socket still exists after graceful stop: %v", err)
	}
}

func TestSecondInstanceRefusedAndLiveSocketKept(t *testing.T) {
	dir := shortRuntimeDir(t)
	socketPath, _, stop := startService(t, dir)
	before, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}

	errCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { errCh <- Run(ctx, dir, quietLogger()) }()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrAlreadyRunning) {
			t.Fatalf("second instance error = %v, want ErrAlreadyRunning", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("second instance did not fail")
	}

	client := unixClient(socketPath)
	if status, body := doCall(t, client, "http://unix", "", "GET", "/v1/health", nil); status != http.StatusOK {
		t.Fatalf("first instance stopped serving: status %d body %v", status, body)
	}
	after, err := os.Lstat(socketPath)
	if err != nil || after.Mode()&os.ModeSocket == 0 {
		t.Fatalf("live socket missing or replaced: err=%v mode=%v", err, after.Mode())
	}
	if !os.SameFile(before, after) {
		t.Fatalf("live socket was replaced by the second attempt")
	}
	stop()
}

func runOnce(t *testing.T, dir string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return Run(ctx, dir, quietLogger())
}

func TestRunRefusesSymlinksAndUnexpectedFiles(t *testing.T) {
	t.Run("runtime symlink", func(t *testing.T) {
		parent := t.TempDir()
		real := t.TempDir()
		link := filepath.Join(parent, "runtime")
		if err := os.Symlink(real, link); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		err := runOnce(t, link)
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("runtime symlink error = %v, want symlink refusal", err)
		}
	})

	t.Run("socket symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
			t.Fatalf("write target: %v", err)
		}
		if err := os.Symlink(target, filepath.Join(dir, "api.sock")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		err := runOnce(t, dir)
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("socket symlink error = %v, want symlink refusal", err)
		}
	})

	t.Run("socket regular file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "api.sock"), []byte("x"), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		err := runOnce(t, dir)
		if err == nil || !strings.Contains(err.Error(), "not a socket") {
			t.Fatalf("unexpected socket file error = %v, want refusal", err)
		}
	})
}

func TestRestartKeepsAdminTokenAndMessages(t *testing.T) {
	dir := shortRuntimeDir(t)
	socket1, admin1, stop1 := startService(t, dir)
	client1 := unixClient(socket1)

	_, health1 := doCall(t, client1, "http://unix", "", "GET", "/v1/health", nil)
	instance1 := health1["instance_id"]

	bobMailbox := ""
	for _, name := range []string{"alice", "bob"} {
		body := sendJSON(t, client1, "http://unix", admin1, "/v1/register", map[string]any{
			"registration_key": name, "token": "token-" + name, "label": name, "kind": "test",
			"scope": "scope-1", "team": "team-1",
		})
		mailbox := body["mailbox"].(map[string]any)
		if name == "bob" {
			bobMailbox = mailbox["mailbox_id"].(string)
		}
	}
	sendJSON(t, client1, "http://unix", "token-alice", "/v1/send", map[string]any{
		"client_message_id": "c-1", "to": bobMailbox, "body": "survives restart", "task_id": "task-1",
	})
	stop1()

	socket2, admin2, stop2 := startService(t, dir)
	defer stop2()
	if admin2 != admin1 {
		t.Fatalf("admin token changed across restart")
	}
	client2 := unixClient(socket2)
	_, health2 := doCall(t, client2, "http://unix", "", "GET", "/v1/health", nil)
	if health2["instance_id"] == instance1 {
		t.Fatalf("instance_id did not change across restart")
	}

	status, body := doCall(t, client2, "http://unix", "token-bob", "GET", "/v1/inbox", nil)
	if status != http.StatusOK {
		t.Fatalf("inbox after restart: status %d body %v", status, body)
	}
	messages := body["messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["body"] != "survives restart" {
		t.Fatalf("messages lost across restart: %v", body)
	}
}

func TestRunRefusesSymlinkedControlFiles(t *testing.T) {
	cases := []struct {
		name     string
		target   string
		fragment string
	}{
		{"admin token", "admin-token", "admin token"},
		{"service lock", "service.lock", "lock file"},
		{"database", "mailbox.sqlite3", "symlink"},
		{"database wal", "mailbox.sqlite3-wal", "symlink"},
		{"database shm", "mailbox.sqlite3-shm", "symlink"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
				t.Fatalf("write target: %v", err)
			}
			if err := os.Symlink(target, filepath.Join(dir, tc.target)); err != nil {
				t.Fatalf("symlink: %v", err)
			}
			err := runOnce(t, dir)
			if err == nil || !strings.Contains(err.Error(), tc.fragment) {
				t.Fatalf("%s symlink error = %v, want fragment %q", tc.name, err, tc.fragment)
			}
		})
	}
}

func TestRemoveStaleSocketKeepsLiveSocket(t *testing.T) {
	dir := shortRuntimeDir(t)
	path := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	if err := removeStaleSocket(path, dialUnixSocket); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("live socket: err = %v, want ErrAlreadyRunning", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("live socket was removed: %v", err)
	}

	listener.Close()
	if err := removeStaleSocket(path, dialUnixSocket); err != nil {
		t.Fatalf("closed listener socket: %v", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket file still present: %v", err)
	}
}

func TestRemoveStaleSocketKeepsFileOnInconclusiveDial(t *testing.T) {
	dir := shortRuntimeDir(t)
	path := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	inconclusive := func(string) (net.Conn, error) { return nil, errors.New("permission denied") }
	if err := removeStaleSocket(path, inconclusive); err == nil || errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("inconclusive dial: err = %v, want a clear non-ErrAlreadyRunning error", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("inconclusive dial removed the socket: %v", err)
	}
}

func TestRunTightensPreexistingDatabaseFileMode(t *testing.T) {
	dir := shortRuntimeDir(t)
	dbPath := filepath.Join(dir, "mailbox.sqlite3")
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Chmod(dbPath, 0o666); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	startService(t, dir)
	info, err := os.Stat(dbPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("db mode = %v err=%v, want 0600", info.Mode().Perm(), err)
	}
}
