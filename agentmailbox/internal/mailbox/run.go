package mailbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// ErrAlreadyRunning reports that another service holds this runtime's lock.
var ErrAlreadyRunning = errors.New("another agent-mailbox instance is using this runtime")

type runtime struct {
	dir        string
	socketPath string
	tokenPath  string
	dbPath     string
	lockFile   *os.File
}

func (rt *runtime) release() {
	if rt.lockFile != nil {
		rt.lockFile.Close()
		rt.lockFile = nil
	}
}

// openRuntime prepares the private directory and takes the exclusive
// single-instance lock. It refuses to follow symlinks on the control path.
func openRuntime(dir string) (*runtime, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("runtime directory is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime directory: %w", err)
	}
	info, err := os.Lstat(abs)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("runtime path %s is a symlink; refusing to follow it", abs)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("runtime path %s is not a directory", abs)
		}
		if err := os.Chmod(abs, 0o700); err != nil {
			return nil, fmt.Errorf("chmod runtime directory: %w", err)
		}
	case os.IsNotExist(err):
		if err := os.MkdirAll(abs, 0o700); err != nil {
			return nil, fmt.Errorf("create runtime directory: %w", err)
		}
	default:
		return nil, fmt.Errorf("inspect runtime directory: %w", err)
	}

	rt := &runtime{
		dir:        abs,
		socketPath: filepath.Join(abs, "api.sock"),
		tokenPath:  filepath.Join(abs, "admin-token"),
		dbPath:     filepath.Join(abs, "mailbox.sqlite3"),
	}
	lockFile, err := os.OpenFile(filepath.Join(abs, "service.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, ErrAlreadyRunning
	}
	rt.lockFile = lockFile
	return rt, nil
}

// dialUnixSocket connects to a local Unix socket with a short bound.
func dialUnixSocket(path string) (net.Conn, error) {
	return net.DialTimeout("unix", path, 300*time.Millisecond)
}

// isDeadSocket reports whether dialing proved that nobody is listening. Only
// explicit "connection refused" and "no such file" count as proof; timeouts,
// permission errors, and anything else are inconclusive.
func isDeadSocket(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT)
}

// removeStaleSocket removes only a real, dead socket file. It first proves the
// socket has no live listener, so a live service socket is never deleted even
// if the instance lock file was removed out from under it. An inconclusive
// dial keeps the file so an operator can inspect it.
func removeStaleSocket(path string, dial func(string) (net.Conn, error)) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect socket path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("socket path %s is a symlink; refusing to follow it", path)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("socket path %s exists and is not a socket; refusing to remove it", path)
	}
	conn, err := dial(path)
	if err == nil {
		conn.Close()
		return ErrAlreadyRunning
	}
	if !isDeadSocket(err) {
		return fmt.Errorf("cannot determine whether socket %s is stale: %w", path, err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	return nil
}

// loadOrCreateAdminToken reuses the persisted admin token across restarts.
// O_NOFOLLOW refuses to read or write through a symlink on the control path.
func loadOrCreateAdminToken(path string) (string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err == nil {
		data, readErr := io.ReadAll(file)
		file.Close()
		if readErr != nil {
			return "", fmt.Errorf("read admin token: %w", readErr)
		}
		token := strings.TrimSpace(string(data))
		if token != "" {
			if err := os.Chmod(path, 0o600); err != nil {
				return "", fmt.Errorf("chmod admin token: %w", err)
			}
			return token, nil
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read admin token: %w", err)
	}
	token := newToken()
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return "", fmt.Errorf("write admin token: %w", err)
	}
	if _, err := out.WriteString(token + "\n"); err != nil {
		out.Close()
		return "", fmt.Errorf("write admin token: %w", err)
	}
	if err := out.Close(); err != nil {
		return "", fmt.Errorf("write admin token: %w", err)
	}
	return token, nil
}

// chmodPrivate tightens an existing file to 0600; a missing file is fine.
func chmodPrivate(path string) error {
	if err := os.Chmod(path, 0o600); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}

// refuseSymlink rejects a pre-existing symlink at path.
func refuseSymlink(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path %s is a symlink; refusing to follow it", path)
	}
	return nil
}

// Run serves the mailbox on runtimeDir until ctx is cancelled. It returns nil
// after a graceful shutdown. It never cancels model work or removes messages.
func Run(ctx context.Context, runtimeDir string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	rt, err := openRuntime(runtimeDir)
	if err != nil {
		return err
	}
	defer rt.release()

	adminToken, err := loadOrCreateAdminToken(rt.tokenPath)
	if err != nil {
		return err
	}
	if err := refuseSymlink(rt.dbPath); err != nil {
		return err
	}
	if err := refuseSymlink(rt.dbPath + "-wal"); err != nil {
		return err
	}
	if err := refuseSymlink(rt.dbPath + "-shm"); err != nil {
		return err
	}
	store, err := OpenStore(rt.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	// A database created before this run may carry a looser mode; tighten it
	// (and its sidecars) even though the 0700 runtime dir already gates access.
	for _, path := range []string{rt.dbPath, rt.dbPath + "-wal", rt.dbPath + "-shm"} {
		if err := chmodPrivate(path); err != nil {
			return err
		}
	}

	if err := removeStaleSocket(rt.socketPath, dialUnixSocket); err != nil {
		return err
	}
	listener, err := net.Listen("unix", rt.socketPath)
	if err != nil {
		return fmt.Errorf("listen unix socket: %w", err)
	}
	if err := os.Chmod(rt.socketPath, 0o600); err != nil {
		listener.Close()
		return fmt.Errorf("chmod socket: %w", err)
	}

	server := NewServer(store, adminToken, logger)
	httpServer := &http.Server{
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.Serve(listener) }()
	logger.Info("agent-mailbox listening", "socket", rt.socketPath, "instance_id", server.InstanceID())

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("graceful shutdown incomplete")
	}
	_ = os.Remove(rt.socketPath)
	logger.Info("agent-mailbox stopped", "socket", rt.socketPath)
	return nil
}

// AdminTokenPath returns the admin token file location for a runtime dir.
func AdminTokenPath(runtimeDir string) string {
	abs, err := filepath.Abs(runtimeDir)
	if err != nil {
		return filepath.Join(runtimeDir, "admin-token")
	}
	return filepath.Join(abs, "admin-token")
}

// SocketPath returns the Unix socket location for a runtime dir.
func SocketPath(runtimeDir string) string {
	abs, err := filepath.Abs(runtimeDir)
	if err != nil {
		return filepath.Join(runtimeDir, "api.sock")
	}
	return filepath.Join(abs, "api.sock")
}
