package antigravity

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// CommunicationSandboxOptions is supplied by the trusted host, never by an
// agent tool. WorkDir and RuntimeDir must already exist. ReadOnlyPaths can
// protect host-created configuration inside RuntimeDir as well as executables
// outside it. No part of the user's home or temporary directory is implicitly
// readable. AllowHTTPS permits HTTPS outside localhost and the DNS resolver, not
// arbitrary loopback connections or the host's Unix-domain IPC sockets.
type CommunicationSandboxOptions struct {
	WorkDir           string
	RuntimeDir        string
	ReadOnlyPaths     []string
	LoopbackAddresses []string
	AllowHTTPS        bool
}

type communicationSandboxPath struct {
	path string
	dir  bool
}

// BuildCommunicationSandboxProfile constructs the macOS process sandbox. The
// profile is an allowlist, not a list of filenames the model should avoid.
// Descendants inherit the same restrictions, including executables started by
// MCP and lifecycle hooks. It does not grant Keychain or other generic Mach IPC.
func BuildCommunicationSandboxProfile(opts CommunicationSandboxOptions) (string, error) {
	work, err := communicationSandboxRoot(opts.WorkDir)
	if err != nil {
		return "", fmt.Errorf("antigravity: communication work directory: %w", err)
	}
	state, err := communicationSandboxRoot(opts.RuntimeDir)
	if err != nil {
		return "", fmt.Errorf("antigravity: communication runtime directory: %w", err)
	}
	if work == state || communicationPathWithin(work, state) || communicationPathWithin(state, work) {
		return "", fmt.Errorf("antigravity: communication work and runtime directories must be separate")
	}

	readOnly, err := communicationReadOnlyPaths(opts.ReadOnlyPaths)
	if err != nil {
		return "", err
	}
	addresses, err := communicationLoopbackAddresses(opts.LoopbackAddresses)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	out.WriteString("(version 1)\n(deny default)\n")
	out.WriteString("(allow process-fork)\n(allow signal (target self))\n(allow sysctl-read)\n")
	// macOS dyld opens the root directory while locating its shared cache.
	// This exact-directory grant does not permit reading any root descendant.
	out.WriteString("(allow file-read-data (literal \"/\"))\n")
	// Resolve the standard macOS aliases before evaluating their canonical
	// descendants. Metadata permission is not permission to list these dirs.
	out.WriteString("(allow file-read-metadata (literal \"/var\") (literal \"/tmp\") (literal \"/etc\"))\n")
	// Do not import system.sb: its implicit filesystem and IPC grants would
	// make the boundary depend on an unrelated system profile.
	for _, path := range []string{
		"/System/Library", "/usr/lib", "/usr/share/zoneinfo",
		"/System/Volumes/Preboot/Cryptexes/OS/System/Library",
		"/System/Volumes/Preboot/Cryptexes/OS/usr/lib",
	} {
		fmt.Fprintf(&out, "(allow file-read* (subpath %s))\n", strconv.Quote(path))
	}
	for _, path := range []string{"/dev/null", "/dev/random", "/dev/urandom", "/private/etc/localtime", "/private/var/select/sh"} {
		fmt.Fprintf(&out, "(allow file-read* (literal %s))\n", strconv.Quote(path))
	}
	out.WriteString("(allow file-write-data (literal \"/dev/null\"))\n")
	// Antigravity runs its trusted hook commands through a system shell. The
	// shell inherits the sandbox and cannot read user startup files.
	for _, path := range []string{"/bin/sh", "/bin/bash", "/bin/zsh"} {
		fmt.Fprintf(&out, "(allow file-read* process-exec (literal %s))\n", strconv.Quote(path))
	}

	ancestors := map[string]bool{}
	for _, root := range []string{work, state} {
		communicationAddAncestors(ancestors, root)
		fmt.Fprintf(&out, "(allow file-read* (subpath %s))\n", strconv.Quote(root))
		writeCommunicationSandboxWriteRule(&out, root, readOnly)
	}
	for _, item := range readOnly {
		communicationAddAncestors(ancestors, item.path)
		kind := "literal"
		if item.dir {
			kind = "subpath"
		}
		fmt.Fprintf(&out, "(allow file-read* (%s %s))\n", kind, strconv.Quote(item.path))
		if !item.dir {
			fmt.Fprintf(&out, "(allow process-exec (literal %s))\n", strconv.Quote(item.path))
		}
	}
	for _, path := range communicationSortedKeys(ancestors) {
		fmt.Fprintf(&out, "(allow file-read-metadata (literal %s))\n", strconv.Quote(path))
	}
	for _, addr := range addresses {
		// Seatbelt's endpoint grammar only accepts '*' or 'localhost', not
		// literal IP addresses. The input validator still rejects non-loopback
		// addresses; the kernel grant is limited to this localhost port.
		_, port, _ := net.SplitHostPort(addr)
		fmt.Fprintf(&out, "(allow network-outbound (remote tcp %s))\n", strconv.Quote("localhost:"+port))
	}
	if opts.AllowHTTPS {
		out.WriteString(`(allow network-outbound
    (require-all (remote tcp "*:443")
        (require-not (remote ip "localhost:*"))))
(allow network-outbound (literal "/private/var/run/mDNSResponder"))
(allow file-read* (literal "/private/etc/hosts") (literal "/private/etc/resolv.conf"))
(allow mach-lookup (global-name "com.apple.trustd") (global-name "com.apple.trustd.agent"))
`)
	}
	return out.String(), nil
}

// CommunicationSandboxCommand returns an unstarted process. Its profile is
// passed directly to sandbox-exec, so no writable profile file can be swapped
// between validation and launch. The caller adds its explicit environment;
// HOME is not changed. Set TMPDIR to a directory under RuntimeDir when needed.
// Unsupported platforms fail closed instead of launching an unsandboxed agent.
func CommunicationSandboxCommand(ctx context.Context, opts CommunicationSandboxOptions, executable string, args ...string) (*exec.Cmd, error) {
	if runtime.GOOS != "darwin" {
		return nil, fmt.Errorf("antigravity: communication sandbox requires macOS Seatbelt")
	}
	program, err := communicationSandboxPathInfo(executable)
	if err != nil || program.dir {
		return nil, fmt.Errorf("antigravity: communication executable must be an existing regular file")
	}
	opts.ReadOnlyPaths = append(append([]string{}, opts.ReadOnlyPaths...), program.path)
	profile, err := BuildCommunicationSandboxProfile(opts)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat("/usr/bin/sandbox-exec"); err != nil {
		return nil, fmt.Errorf("antigravity: macOS sandbox-exec is unavailable: %w", err)
	}
	argv := []string{"-p", profile, "--", program.path}
	cmd := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", append(argv, args...)...)
	cmd.Dir, _ = filepath.EvalSymlinks(opts.WorkDir)
	return cmd, nil
}

func communicationSandboxPathInfo(path string) (communicationSandboxPath, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\r\n") {
		return communicationSandboxPath{}, fmt.Errorf("expected an absolute path without control characters")
	}
	real, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return communicationSandboxPath{}, fmt.Errorf("resolve sandbox path: %w", err)
	}
	info, err := os.Stat(real)
	if err != nil || (!info.Mode().IsRegular() && !info.IsDir()) {
		return communicationSandboxPath{}, fmt.Errorf("sandbox path must be a regular file or directory")
	}
	return communicationSandboxPath{path: real, dir: info.IsDir()}, nil
}

func communicationSandboxRoot(path string) (string, error) {
	item, err := communicationSandboxPathInfo(path)
	if err != nil {
		return "", err
	}
	if !item.dir {
		return "", fmt.Errorf("expected a directory")
	}
	homeDir, _ := os.UserHomeDir()
	for _, broad := range []string{"/", homeDir, os.TempDir(), "/tmp", "/private/tmp", "/var/tmp", "/private/var/tmp"} {
		if real, err := filepath.EvalSymlinks(broad); err == nil && communicationPathWithin(item.path, real) {
			return "", fmt.Errorf("refusing a filesystem, home, or shared temporary directory root")
		}
	}
	return item.path, nil
}

func communicationReadOnlyPaths(paths []string) ([]communicationSandboxPath, error) {
	seen := make(map[string]communicationSandboxPath)
	for _, path := range paths {
		item, err := communicationSandboxPathInfo(path)
		if err != nil {
			return nil, fmt.Errorf("antigravity: communication read-only path: %w", err)
		}
		if item.dir {
			if _, err := communicationSandboxRoot(item.path); err != nil {
				return nil, fmt.Errorf("antigravity: communication read-only directory: %w", err)
			}
		}
		seen[item.path] = item
		// Protect a configured symlink's directory entry too. Protecting only
		// its resolved target would allow a writable runtime to replace the
		// symlink with an agent-owned settings or hooks file.
		parent, err := filepath.EvalSymlinks(filepath.Dir(path))
		if err != nil {
			return nil, fmt.Errorf("antigravity: resolve read-only path parent: %w", err)
		}
		entry := filepath.Join(parent, filepath.Base(path))
		seen[entry] = communicationSandboxPath{path: entry, dir: item.dir}
	}
	var result []communicationSandboxPath
	for _, path := range communicationSortedKeys(seen) {
		result = append(result, seen[path])
	}
	return result, nil
}

func communicationLoopbackAddresses(addresses []string) ([]string, error) {
	seen := map[string]bool{}
	for _, address := range addresses {
		host, port, err := net.SplitHostPort(address)
		n, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || n < 1 || n > 65535 || (host != "127.0.0.1" && host != "::1") {
			return nil, fmt.Errorf("antigravity: communication bridge must use an explicit loopback IP and port")
		}
		seen[net.JoinHostPort(host, strconv.Itoa(n))] = true
	}
	return communicationSortedKeys(seen), nil
}

func writeCommunicationSandboxWriteRule(out *strings.Builder, root string, readOnly []communicationSandboxPath) {
	excluded := map[string]bool{root: true}
	var excludedTrees []string
	for _, item := range readOnly {
		if !communicationPathWithin(root, item.path) {
			continue
		}
		if item.dir {
			excludedTrees = append(excludedTrees, item.path)
		}
		for path := item.path; communicationPathWithin(root, path); path = filepath.Dir(path) {
			excluded[path] = true
			if path == root {
				break
			}
		}
	}
	fmt.Fprintf(out, "(allow file-write* (require-all (subpath %s)", strconv.Quote(root))
	for _, path := range communicationSortedKeys(excluded) {
		fmt.Fprintf(out, " (require-not (literal %s))", strconv.Quote(path))
	}
	for _, path := range excludedTrees {
		fmt.Fprintf(out, " (require-not (subpath %s))", strconv.Quote(path))
	}
	out.WriteString("))\n")
}

func communicationAddAncestors(paths map[string]bool, path string) {
	for path = filepath.Dir(path); ; path = filepath.Dir(path) {
		paths[path] = true
		if path == "/" {
			return
		}
	}
}

func communicationPathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func communicationSortedKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
