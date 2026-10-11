package antigravity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func communicationTestOptions(t *testing.T) CommunicationSandboxOptions {
	t.Helper()
	// Darwin Unix-domain socket paths are limited to 104 bytes; Go's
	// test-name-based temporary paths can exceed that before any socket name.
	root, err := os.MkdirTemp("", "cc-asb-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	opts := CommunicationSandboxOptions{WorkDir: filepath.Join(root, "work"), RuntimeDir: filepath.Join(root, "runtime")}
	for _, dir := range []string{opts.WorkDir, opts.RuntimeDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return opts
}

func TestCommunicationSandboxRejectsBroadOrMalformedGrants(t *testing.T) {
	opts := communicationTestOptions(t)
	for _, path := range []string{"", ".", "/", os.TempDir(), "/tmp", "/private/tmp", "/var/tmp", "/private/var/tmp", opts.WorkDir + "\n(allow default)"} {
		t.Run("root_"+strings.ReplaceAll(path, "/", "_"), func(t *testing.T) {
			candidate := opts
			candidate.WorkDir = path
			if _, err := BuildCommunicationSandboxProfile(candidate); err == nil {
				t.Fatal("unsafe directory was accepted")
			}
		})
	}
	for _, address := range []string{"localhost:1234", "127.0.0.1:*", "0.0.0.0:1234", "127.0.0.1:0", "127.0.0.1:65536", "example.com:443"} {
		candidate := opts
		candidate.LoopbackAddresses = []string{address}
		if _, err := BuildCommunicationSandboxProfile(candidate); err == nil {
			t.Errorf("unsafe endpoint %q was accepted", address)
		}
	}
	candidate := opts
	candidate.RuntimeDir = opts.WorkDir
	if _, err := BuildCommunicationSandboxProfile(candidate); err == nil {
		t.Fatal("overlapping work and runtime directories were accepted")
	}
	candidate = opts
	candidate.ReadOnlyPaths = []string{"/"}
	if _, err := BuildCommunicationSandboxProfile(candidate); err == nil {
		t.Fatal("whole-filesystem read grant was accepted")
	}
}

func TestCommunicationSandboxCanonicalizesPathsAndKeepsRulesDeterministic(t *testing.T) {
	opts := communicationTestOptions(t)
	alias := filepath.Join(filepath.Dir(opts.WorkDir), "alias")
	if err := os.Symlink(opts.WorkDir, alias); err != nil {
		t.Fatal(err)
	}
	opts.WorkDir = alias
	opts.LoopbackAddresses = []string{"127.0.0.1:9876", "127.0.0.1:9876", "[::1]:9877"}
	first, err := BuildCommunicationSandboxProfile(opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildCommunicationSandboxProfile(opts)
	if err != nil || first != second {
		t.Fatalf("profile is not deterministic: %v", err)
	}
	if strings.Contains(first, alias) || strings.Count(first, `remote tcp "localhost:9876"`) != 1 {
		t.Fatal("path canonicalization or endpoint deduplication failed")
	}
	if strings.Contains(first, "(allow default)") || strings.Contains(first, `(import "system.sb")`) {
		t.Fatal("profile inherited unrelated system grants")
	}
}
