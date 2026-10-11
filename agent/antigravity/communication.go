package antigravity

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

type agyCommunicationOptions struct {
	command       []string
	runtimeDir    string
	authFile      string
	readOnlyPaths []string
}

func parseAgyCommunicationOptions(opts map[string]any, workDir string, reviewCommand, extraArgs []string) (*agyCommunicationOptions, error) {
	value, exists := opts["communication_mode"]
	if !exists {
		return nil, nil
	}
	enabled, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("antigravity: communication_mode must be a boolean")
	}
	if !enabled {
		return nil, nil
	}
	if runtime.GOOS != "darwin" {
		return nil, fmt.Errorf("antigravity: communication mode requires macOS process sandbox support")
	}
	if len(reviewCommand) > 0 || len(extraArgs) > 0 {
		return nil, fmt.Errorf("antigravity: communication mode cannot use review_command or extra CLI arguments")
	}
	if !filepath.IsAbs(workDir) {
		return nil, fmt.Errorf("antigravity: communication work_dir must be absolute")
	}
	command, err := parseReviewCommand(opts["communication_command"])
	if err != nil || len(command) == 0 {
		return nil, fmt.Errorf("antigravity: communication_command must be a nonempty argument array")
	}
	resolved, err := exec.LookPath(command[0])
	if err != nil {
		return nil, fmt.Errorf("antigravity: communication executable is unavailable")
	}
	command[0] = resolved
	runtimeDir, _ := opts["communication_runtime_dir"].(string)
	if !filepath.IsAbs(runtimeDir) || filepath.Clean(runtimeDir) == string(filepath.Separator) {
		return nil, fmt.Errorf("antigravity: communication_runtime_dir must be a dedicated absolute directory")
	}
	authFile, _ := opts["communication_auth_file"].(string)
	if authFile != "" && !filepath.IsAbs(authFile) {
		return nil, fmt.Errorf("antigravity: communication_auth_file must be absolute")
	}
	paths, err := parseReviewCommand(opts["communication_read_only_paths"])
	if err != nil {
		return nil, fmt.Errorf("antigravity: communication_read_only_paths must be an argument array")
	}
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("antigravity: communication read-only paths must be absolute")
		}
	}
	return &agyCommunicationOptions{command: command, runtimeDir: runtimeDir, authFile: authFile, readOnlyPaths: paths}, nil
}

func isAgyCommunicationTool(name string, args map[string]any) bool {
	switch name {
	case "search_web", "view_content_chunk":
		return true
	case "read_url_content":
		value, _ := args["Url"].(string)
		parsed, err := url.Parse(value)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.User != nil {
			return false
		}
		host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
		if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
			return false
		}
		ip := net.ParseIP(host)
		return ip == nil || (ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback())
	case "call_mcp_tool":
		server, _ := args["ServerName"].(string)
		operation, _ := args["ToolName"].(string)
		return server == "cc_communication" && isAgyCommunicationOperation(operation)
	}
	for _, prefix := range []string{"mcp_cc_communication_", "mcp__cc_communication__"} {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		return isAgyCommunicationOperation(strings.TrimPrefix(name, prefix))
	}
	return false
}

func isAgyCommunicationOperation(name string) bool {
	switch name {
	case "list_contacts", "send_message", "reply_message", "read_message", "message_status", "ack_message", "consult_hermes", "consultation_result":
		return true
	}
	return false
}

// The native CLI lazily reads MCP schemas through view_file. This grant is
// limited to the eight communication schemas in the dedicated native cache.
func isAgyCommunicationSchemaRead(args map[string]any, schemaDir string) bool {
	path, _ := args["AbsolutePath"].(string)
	if schemaDir == "" || !filepath.IsAbs(path) || filepath.Ext(path) != ".json" ||
		!isAgyCommunicationOperation(strings.TrimSuffix(filepath.Base(path), ".json")) {
		return false
	}
	root, err := filepath.EvalSymlinks(schemaDir)
	if err != nil {
		return false
	}
	real, err := filepath.EvalSymlinks(path)
	return err == nil && filepath.Dir(real) == root && filepath.Base(real) == filepath.Base(path)
}

func (as *antigravitySession) initializeCommunication() error {
	options := as.communication
	for _, path := range []string{options.runtimeDir, filepath.Join(options.runtimeDir, "antigravity-cli"), filepath.Join(options.runtimeDir, "projects"), filepath.Join(options.runtimeDir, "home"), filepath.Join(options.runtimeDir, "tmp")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("antigravity: create communication runtime: %w", err)
		}
	}
	bridge, err := newAgyCommunicationBridge(as.ctx, options.command, as.workDir, as.extraEnv)
	if err != nil {
		return err
	}
	as.communicationBridge = bridge
	nativeDir, err := filepath.EvalSymlinks(filepath.Join(options.runtimeDir, "antigravity-cli"))
	if err != nil {
		return fmt.Errorf("antigravity: resolve native cache directory: %w", err)
	}
	as.permissionBridge.webCache = &agyCommunicationWebCache{rootDir: filepath.Join(nativeDir, "brain")}
	configRoot := filepath.Join(as.invocationBridge.rootDir, "agy-config")
	configDir := filepath.Join(configRoot, "config")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return err
	}
	if err := os.Symlink(filepath.Join(options.runtimeDir, "antigravity-cli"), filepath.Join(configRoot, "antigravity-cli")); err != nil {
		return err
	}
	// Native project records are mutable session state. Keep only that subtree
	// in the runtime; hooks and MCP configuration stay in the host-owned overlay.
	if err := os.Symlink(filepath.Join(options.runtimeDir, "projects"), filepath.Join(configDir, "projects")); err != nil {
		return err
	}
	if options.authFile != "" {
		if err := installAgyCommunicationCredential(options.authFile, options.runtimeDir); err != nil {
			return err
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	hooks := map[string]any{
		agyPermissionHookName: map[string]any{"PreToolUse": []any{map[string]any{
			"matcher": "*", "hooks": []any{map[string]any{"type": "command", "command": shellQuote(executable) + " _agy-permission-hook", "timeout": 15}},
		}}},
		agyInvocationHookName: map[string]any{
			"PreInvocation":  []any{map[string]any{"type": "command", "command": shellQuote(executable) + " _agy-invocation-hook pre", "timeout": 15}},
			"PostInvocation": []any{map[string]any{"type": "command", "command": shellQuote(executable) + " _agy-invocation-hook post", "timeout": 15}},
		},
	}
	if err := writeAgyCommunicationJSON(filepath.Join(configDir, "hooks.json"), hooks); err != nil {
		return err
	}
	mcp := map[string]any{"mcpServers": map[string]any{"cc_communication": map[string]any{
		"command": executable, "args": []string{"_agy-communication-tool"}, "cwd": as.workDir,
	}}}
	if err := writeAgyCommunicationJSON(filepath.Join(configDir, "mcp_config.json"), mcp); err != nil {
		return err
	}
	// The pre-tool hook additionally requires a completed web-tool event and
	// the unchanged body from this conversation before allowing any cache read.
	// Native read_file directory rules are recursive; embedded globs are literal.
	allowed := []string{"read_url(*)", "read_file(" + filepath.Join(nativeDir, "brain") + ")"}
	for _, operation := range []string{"list_contacts", "send_message", "reply_message", "read_message", "message_status", "ack_message", "consult_hermes", "consultation_result"} {
		allowed = append(allowed, "mcp(cc_communication/"+operation+")",
			"read_file("+filepath.Join(options.runtimeDir, "antigravity-cli", "mcp", "cc_communication", operation+".json")+")")
	}
	settings := map[string]any{"permissions": map[string]any{
		"deny":  []string{"command(*)", "unsandboxed(*)"},
		"allow": allowed,
	}, "allowNonWorkspaceAccess": false}
	if err := writeAgyCommunicationJSON(filepath.Join(options.runtimeDir, "antigravity-cli", "settings.json"), settings); err != nil {
		return err
	}
	as.invocationBridge.configDir = configRoot
	return nil
}

func writeAgyCommunicationJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("antigravity: communication configuration is not a regular file")
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func installAgyCommunicationCredential(source, runtimeDir string) error {
	// CLI NewCLITokenStorage uses appDataDir (the antigravity-cli directory)
	// and its product prefix, antigravity. Keep the refreshable file in the
	// dedicated native store, never in the host credential location.
	const filename = "antigravity-oauth-token"
	target := filepath.Join(runtimeDir, "antigravity-cli", filename)
	if info, err := os.Lstat(target); os.IsNotExist(err) {
		data, err := readReviewFile(source, 1<<20)
		if err != nil {
			return fmt.Errorf("antigravity: configured authentication file is unavailable")
		}
		if err := os.WriteFile(target, data, 0o600); err != nil {
			return fmt.Errorf("antigravity: create dedicated authentication file")
		}
	} else if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("antigravity: dedicated authentication file is invalid")
	}
	return nil
}

func (as *antigravitySession) communicationCommand(ctx context.Context, args []string) (*exec.Cmd, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cli, err := exec.LookPath(as.cmd)
	if err != nil {
		return nil, err
	}
	paths := append([]string(nil), as.communication.readOnlyPaths...)
	// The CLI normalizes its private settings on load and discards them if
	// that write fails. These preferences are not the security boundary:
	// immutable hooks, the fixed host dispatcher, and Seatbelt enforce it.
	paths = append(paths, executable, cli, as.invocationBridge.configDir)
	rules, err := agyCommunicationRulePaths(as.workDir)
	if err != nil {
		return nil, err
	}
	paths = append(paths, rules...)
	return CommunicationSandboxCommand(ctx, CommunicationSandboxOptions{
		WorkDir: as.workDir, RuntimeDir: as.communication.runtimeDir, ReadOnlyPaths: paths,
		LoopbackAddresses: []string{as.permissionBridge.address, as.invocationBridge.listener.Addr().String(), as.communicationBridge.listener.Addr().String()},
		AllowHTTPS:        true, AllowNativeRPC: true,
	}, cli, args...)
}

// Workspace rule aliases must not turn a model-writable filename into a host
// grant for a parent workspace or an unrelated private file on the next run.
func agyCommunicationRulePaths(workDir string) ([]string, error) {
	root, err := filepath.EvalSymlinks(workDir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, name := range []string{"AGENTS.md", "CLAUDE.md", "GEMINI.md"} {
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := filepath.EvalSymlinks(path)
			if err != nil || name == "AGENTS.md" || target != filepath.Join(root, "AGENTS.md") {
				return nil, fmt.Errorf("antigravity: communication rule alias must refer to the local AGENTS.md")
			}
			info, err = os.Stat(target)
			if err != nil {
				return nil, err
			}
		} else if name != "AGENTS.md" {
			return nil, fmt.Errorf("antigravity: communication rule aliases must link to the single AGENTS.md source")
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("antigravity: communication rule must be a local regular file")
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func (as *antigravitySession) communicationEnv() []string {
	allowed := map[string]bool{"PATH": true, "USER": true, "LOGNAME": true, "LANG": true, "LC_ALL": true, "LC_CTYPE": true, "TZ": true,
		"GEMINI_API_KEY": true, "GOOGLE_API_KEY": true, "SSL_CERT_FILE": true, "SSL_CERT_DIR": true}
	var env []string
	for _, value := range core.MergeEnv(os.Environ(), as.extraEnv) {
		key, _, _ := strings.Cut(value, "=")
		if allowed[key] {
			env = append(env, value)
		}
	}
	return core.MergeEnv(env, []string{
		"HOME=" + filepath.Join(as.communication.runtimeDir, "home"),
		"TMPDIR=" + filepath.Join(as.communication.runtimeDir, "tmp"),
		"XDG_CONFIG_HOME=" + filepath.Join(as.communication.runtimeDir, "home", ".config"),
		"XDG_CACHE_HOME=" + filepath.Join(as.communication.runtimeDir, "home", ".cache"),
	})
}

func appendAgyCommunicationInstructions(prompt string) string {
	return prompt + "\n\n[Host communication policy]\n" +
		"This is a communication session. Answer ordinary questions directly; do not run a mandatory review or reopen old review tasks. " +
		"Use native search_web and read_url_content for web research, and cc_communication MCP tools for messages and consultation. " +
		"When read_url_content saves the article to a content.md cache, use view_file on that exact returned file to read the body. The host permits only unchanged web content registered from successful tools in this conversation; after a host restart, fetch the URL again before reading its cache. " +
		"Do not read other files, run shell commands, edit project files, change permissions/configuration, or spawn operation agents. " +
		"For an IPC notice use read_message with its event ID to read the verified body, and reply_message to answer its original sender. " +
		"Check message_status after uncertain delivery; do not create replacement sends. Use consult_hermes only when this question needs independent judgment or the user requested it. " +
		"Consultation is optional for ordinary replies. A correct report of a defect can be delivered while the reviewed artifact remains unapproved."
}

func (a *Agent) validateConfiguredSessionID(ctx context.Context, workDir, sessionID string) (bool, error) {
	if a.communication == nil {
		return validateAntigravitySessionID(ctx, workDir, sessionID)
	}
	if !validAntigravitySessionID(sessionID) {
		return false, nil
	}
	storeDir := filepath.Join(a.communication.runtimeDir, "antigravity-cli")
	found, valid, err := lookupSQLiteAntigravitySession(ctx, storeDir, workDir, sessionID)
	if err != nil || found {
		return valid, err
	}
	if _, err := os.Stat(filepath.Join(storeDir, "conversations", sessionID+".db")); err == nil {
		return false, fmt.Errorf("antigravity: dedicated conversation summary is unavailable")
	} else if !os.IsNotExist(err) {
		return false, err
	}
	return false, nil
}
