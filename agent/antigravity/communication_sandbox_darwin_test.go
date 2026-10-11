//go:build darwin

package antigravity

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommunicationSandboxSystemExecutable(t *testing.T) {
	opts := communicationTestOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd, err := CommunicationSandboxCommand(ctx, opts, "/usr/bin/true")
	if err != nil {
		t.Fatal(err)
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("system executable could not start: %v\n%s", err, output)
	}
}

func TestCommunicationSandboxProcessBoundary(t *testing.T) {
	if os.Getenv("CC_AGY_SANDBOX_HELPER") == "1" {
		communicationSandboxHelper()
		return
	}
	opts := communicationTestOptions(t)
	root := filepath.Dir(opts.WorkDir)
	outside := filepath.Join(root, "other-project")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(outside, "marker.txt")
	if err := os.WriteFile(marker, []byte("outside-marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(opts.WorkDir, "outside-link")); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(opts.RuntimeDir, "host-config")
	if err := os.Mkdir(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(configDir, "hooks.json")
	if err := os.WriteFile(config, []byte("host-controlled"), 0o600); err != nil {
		t.Fatal(err)
	}
	settings := filepath.Join(opts.RuntimeDir, "settings.json")
	externalSettings := filepath.Join(root, "host-settings.json")
	if err := os.WriteFile(externalSettings, []byte("host-settings"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(externalSettings, settings); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnlyPaths = []string{config, settings}
	unixPath := filepath.Join(root, "api.sock")
	raw, err := net.Listen("unix", unixPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	localUnixPath := filepath.Join(opts.WorkDir, "i.sock")
	localUnix, err := net.Listen("unix", localUnixPath)
	if err != nil {
		t.Fatal(err)
	}
	defer localUnix.Close()
	bridge, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	unlisted, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer unlisted.Close()
	opts.LoopbackAddresses = []string{bridge.Addr().String()}
	bridgeDone := make(chan error, 4)
	go func() {
		for range 4 {
			conn, err := bridge.Accept()
			if err == nil {
				_, err = conn.Write([]byte("bridge-ok"))
				conn.Close()
			}
			bridgeDone <- err
		}
	}()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []struct{ https, nativeRPC bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("https_%v_native_rpc_%v", mode.https, mode.nativeRPC), func(t *testing.T) {
			candidate := opts
			candidate.AllowHTTPS = mode.https
			candidate.AllowNativeRPC = mode.nativeRPC
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd, err := CommunicationSandboxCommand(ctx, candidate, executable, "-test.run=^TestCommunicationSandboxProcessBoundary$")
			if err != nil {
				t.Fatal(err)
			}
			cmd.Env = append(os.Environ(), "CC_AGY_SANDBOX_HELPER=1", "CC_AGY_SANDBOX_WORK="+opts.WorkDir,
				"CC_AGY_SANDBOX_RUNTIME="+opts.RuntimeDir, "CC_AGY_SANDBOX_OUTSIDE="+outside,
				"CC_AGY_SANDBOX_UNIX="+unixPath, "CC_AGY_SANDBOX_BRIDGE="+bridge.Addr().String(),
				"CC_AGY_SANDBOX_LOCAL_UNIX="+localUnixPath,
				fmt.Sprintf("CC_AGY_SANDBOX_NATIVE_RPC=%v", mode.nativeRPC),
				"CC_AGY_SANDBOX_UNLISTED="+unlisted.Addr().String(), "TMPDIR="+opts.RuntimeDir)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("sandbox helper: %v\n%s", err, output)
			}
			var results struct {
				Checks      map[string]bool
				ChildOutput string
				ChildError  string
			}
			if err := json.Unmarshal(output, &results); err != nil {
				t.Fatalf("decode helper result: %v\n%s", err, output)
			}
			for name, passed := range results.Checks {
				if !passed {
					t.Errorf("process boundary failed: %s (child error=%s output=%q)", name, results.ChildError, results.ChildOutput)
				}
			}
		})
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "outside-marker" {
		t.Fatal("outside marker changed")
	}
	if data, err := os.ReadFile(config); err != nil || string(data) != "host-controlled" {
		t.Fatal("host control file changed")
	}
	if data, err := os.ReadFile(externalSettings); err != nil || string(data) != "host-settings" {
		t.Fatal("external host settings changed through their runtime symlink")
	}
	select {
	case err := <-bridgeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("allowed bridge was not reached")
	}
}

func communicationSandboxHelper() {
	work := os.Getenv("CC_AGY_SANDBOX_WORK")
	state := os.Getenv("CC_AGY_SANDBOX_RUNTIME")
	outside := os.Getenv("CC_AGY_SANDBOX_OUTSIDE")
	results := map[string]bool{}
	_, err := os.ReadFile(filepath.Join(outside, "marker.txt"))
	results["outside_read_denied"] = err != nil
	results["outside_write_denied"] = os.WriteFile(filepath.Join(outside, "marker.txt"), []byte("wrong"), 0o600) != nil
	_, err = os.ReadFile(filepath.Join(work, "outside-link", "marker.txt"))
	results["symlink_read_denied"] = err != nil
	results["symlink_write_denied"] = os.WriteFile(filepath.Join(work, "outside-link", "marker.txt"), []byte("wrong"), 0o600) != nil
	results["outside_hardlink_denied"] = os.Link(filepath.Join(outside, "marker.txt"), filepath.Join(work, "hardlink.txt")) != nil
	_, err = os.ReadDir(filepath.Dir(work))
	results["parent_listing_denied"] = err != nil
	results["parent_replace_denied"] = os.Rename(outside, filepath.Join(work, "stolen-project")) != nil
	results["work_root_replace_denied"] = os.Rename(work, filepath.Join(state, "moved-work")) != nil
	configDir := filepath.Join(state, "host-config")
	config := filepath.Join(configDir, "hooks.json")
	data, err := os.ReadFile(config)
	results["config_read_allowed"] = err == nil && string(data) == "host-controlled"
	results["config_write_denied"] = os.WriteFile(config, []byte("wrong"), 0o600) != nil
	results["config_chmod_denied"] = os.Chmod(config, 0o777) != nil
	results["config_parent_replace_denied"] = os.Rename(configDir, filepath.Join(state, "moved-config")) != nil
	results["settings_symlink_replace_denied"] = os.Remove(filepath.Join(state, "settings.json")) != nil
	data, err = os.ReadFile(filepath.Join(state, "settings.json"))
	results["settings_symlink_read_allowed"] = err == nil && string(data) == "host-settings"
	results["settings_symlink_write_denied"] = os.WriteFile(filepath.Join(state, "settings.json"), []byte("wrong"), 0o600) != nil
	results["work_write_allowed"] = os.WriteFile(filepath.Join(work, "own.txt"), []byte("own\n"), 0o600) == nil
	results["runtime_write_allowed"] = os.WriteFile(filepath.Join(state, "cache.txt"), []byte("cache"), 0o600) == nil
	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	nativeRPC := os.Getenv("CC_AGY_SANDBOX_NATIVE_RPC") == "true"
	results["native_listener_requires_explicit_grant"] = (listenErr == nil) == nativeRPC
	if listener != nil {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		results["listener_does_not_grant_outbound"] = err != nil
		if conn != nil {
			conn.Close()
		}
		listener.Close()
	}
	listener, listenErr = net.Listen("tcp", "0.0.0.0:0")
	if !nativeRPC {
		results["public_listener_denied_by_default"] = listenErr != nil
	}
	if listener != nil {
		listener.Close()
	}
	for _, entry := range []struct{ name, network, address string }{
		{"raw_unix_denied", "unix", os.Getenv("CC_AGY_SANDBOX_UNIX")},
		{"unix_inside_work_denied", "unix", os.Getenv("CC_AGY_SANDBOX_LOCAL_UNIX")},
		{"unlisted_tcp_denied", "tcp", os.Getenv("CC_AGY_SANDBOX_UNLISTED")},
		{"bridge_tcp_allowed", "tcp", os.Getenv("CC_AGY_SANDBOX_BRIDGE")},
	} {
		conn, err := net.DialTimeout(entry.network, entry.address, time.Second)
		results[entry.name] = err != nil
		if entry.name == "bridge_tcp_allowed" {
			results[entry.name] = false
			if err == nil {
				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 32)
				n, readErr := conn.Read(buf)
				results[entry.name] = readErr == nil && string(buf[:n]) == "bridge-ok"
			}
		}
		if conn != nil {
			conn.Close()
		}
	}
	// A trusted shell remains sandboxed; read denial is not merely a Go
	// helper's convention or an agent permission prompt.
	child := exec.Command("/bin/sh", "-c", "read -r value < \"$CC_AGY_SANDBOX_WORK/own.txt\"; printf '%s' \"$value\"; read -r value < \"$CC_AGY_SANDBOX_OUTSIDE/marker.txt\"")
	child.Env = append(os.Environ(), "PWD="+work)
	output, err := child.Output()
	results["child_inherits_boundary"] = err != nil && strings.HasPrefix(string(output), "own")
	_ = json.NewEncoder(os.Stdout).Encode(struct {
		Checks      map[string]bool
		ChildOutput string
		ChildError  string
	}{results, string(output), fmt.Sprint(err)})
	os.Exit(0)
}

// This optional installed-CLI check only invokes metadata/configuration
// subcommands. It never starts a conversation or sends a model request.
func TestCommunicationSandboxInstalledCLIMetadata(t *testing.T) {
	program := os.Getenv("CC_AGY_METADATA_BINARY")
	if program == "" {
		t.Skip("set CC_AGY_METADATA_BINARY to check an installed CLI without model calls")
	}
	opts := communicationTestOptions(t)
	configRoot := filepath.Join(opts.RuntimeDir, "gemini")
	if err := os.Mkdir(configRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--version"}, {"--help"},
		{"--gemini_dir=" + configRoot, "mcp", "add", "cc_sandbox_fixture", "/usr/bin/true"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd, err := CommunicationSandboxCommand(ctx, opts, program, args...)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		cmd.Env = append(cmd.Environ(), "TMPDIR="+opts.RuntimeDir)
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("CLI metadata command %q: %v\n%s", args[0], err, output)
		}
	}
	path := filepath.Join(configRoot, "config", "mcp_config.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("isolated MCP config was not created at %s: %v", path, err)
	}
	var config struct {
		Servers map[string]struct {
			Command string `json:"command"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(data, &config); err != nil || config.Servers["cc_sandbox_fixture"].Command != "/usr/bin/true" {
		t.Fatalf("isolated MCP configuration has unexpected structure: %v", err)
	}
}
