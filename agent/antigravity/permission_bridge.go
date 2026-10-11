package antigravity

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

const agyPermissionHookName = "cc-connect-permission-bridge"

type agyHookInput struct {
	ConversationID string `json:"conversationId"`
	StepIndex      int    `json:"stepIdx"`
	ToolCall       struct {
		Name string         `json:"name"`
		Args map[string]any `json:"args"`
	} `json:"toolCall"`
}

type agyPermissionBridge struct {
	ctx           context.Context
	cancel        context.CancelFunc
	listener      net.Listener
	address       string
	token         string
	rootDir       string
	configDir     string
	events        chan<- core.Event
	communication bool
	schemaDir     string

	nextID    atomic.Uint64
	pendingMu sync.Mutex
	pending   map[string]chan core.PermissionResult
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newAgyPermissionBridge(ctx context.Context, events chan<- core.Event) (*agyPermissionBridge, error) {
	return newAgyPermissionBridgeMode(ctx, events, false, "")
}

func newAgyPermissionBridgeMode(ctx context.Context, events chan<- core.Event, communication bool, schemaDir string) (*agyPermissionBridge, error) {
	bridgeCtx, cancel := context.WithCancel(ctx)
	rootDir, err := os.MkdirTemp("", "cc-connect-agy-permission-")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create permission bridge directory: %w", err)
	}

	configDir := ""
	if !communication {
		configDir, err = createAgyConfigOverlay(rootDir)
		if err != nil {
			cancel()
			_ = os.RemoveAll(rootDir)
			return nil, err
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		_ = os.RemoveAll(rootDir)
		return nil, fmt.Errorf("listen for Agy permission hooks: %w", err)
	}

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		cancel()
		_ = listener.Close()
		_ = os.RemoveAll(rootDir)
		return nil, fmt.Errorf("generate permission bridge token: %w", err)
	}

	bridge := &agyPermissionBridge{
		ctx:           bridgeCtx,
		cancel:        cancel,
		listener:      listener,
		address:       listener.Addr().String(),
		token:         base64.RawURLEncoding.EncodeToString(tokenBytes),
		rootDir:       rootDir,
		configDir:     configDir,
		events:        events,
		communication: communication,
		schemaDir:     schemaDir,
		pending:       make(map[string]chan core.PermissionResult),
	}
	bridge.wg.Add(1)
	go bridge.acceptLoop()
	return bridge, nil
}

func createAgyConfigOverlay(rootDir string) (string, error) {
	return createAgyConfigOverlayForHooks(rootDir, true, false)
}

func createAgyConfigOverlayForHooks(rootDir string, permission, invocation bool) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory for Agy permission bridge: %w", err)
	}
	// Antigravity currently stores its CLI state under .gemini and exposes it
	// through the --gemini_dir compatibility flag.
	realConfigRoot := filepath.Join(homeDir, ".gemini")
	overlayConfigRoot := filepath.Join(rootDir, "agy-config")
	overlayConfigDir := filepath.Join(overlayConfigRoot, "config")
	if err := os.MkdirAll(overlayConfigDir, 0o700); err != nil {
		return "", fmt.Errorf("create Agy permission overlay: %w", err)
	}

	skipped := map[string]bool{"config": true, "antigravity-cli": permission}
	if err := mirrorDirectoryEntries(realConfigRoot, overlayConfigRoot, skipped); err != nil {
		return "", err
	}
	realConfigDir := filepath.Join(realConfigRoot, "config")
	if err := mirrorDirectoryEntries(realConfigDir, overlayConfigDir, map[string]bool{"hooks.json": true}); err != nil {
		return "", err
	}

	hooks := make(map[string]json.RawMessage)
	hooksPath := filepath.Join(realConfigDir, "hooks.json")
	if data, err := os.ReadFile(hooksPath); err == nil {
		if err := json.Unmarshal(data, &hooks); err != nil {
			return "", fmt.Errorf("parse existing Agy hooks %s: %w", hooksPath, err)
		}
		if hooks == nil {
			hooks = make(map[string]json.RawMessage)
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read existing Agy hooks %s: %w", hooksPath, err)
	}

	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve cc-connect executable for Agy hook: %w", err)
	}
	bridgeHook, err := json.Marshal(map[string]any{
		"PreToolUse": []any{
			map[string]any{
				"matcher": "*",
				"hooks": []any{
					map[string]any{
						"type":    "command",
						"command": shellQuote(executable) + " _agy-permission-hook",
						"timeout": 86400,
					},
				},
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal Agy permission hook: %w", err)
	}
	if permission {
		if _, exists := hooks[agyPermissionHookName]; exists {
			return "", fmt.Errorf("existing Agy hook uses reserved name %s", agyPermissionHookName)
		}
		hooks[agyPermissionHookName] = bridgeHook
	}
	if invocation {
		if _, exists := hooks[agyInvocationHookName]; exists {
			return "", fmt.Errorf("existing Agy hook uses reserved name %s", agyInvocationHookName)
		}
		invocationHook, err := json.Marshal(map[string]any{
			"PreInvocation": []any{map[string]any{
				"type": "command", "command": shellQuote(executable) + " _agy-invocation-hook pre", "timeout": 15,
			}},
			"PostInvocation": []any{map[string]any{
				"type": "command", "command": shellQuote(executable) + " _agy-invocation-hook post", "timeout": 15,
			}},
		})
		if err != nil {
			return "", fmt.Errorf("marshal Agy invocation hook: %w", err)
		}
		hooks[agyInvocationHookName] = invocationHook
	}

	data, err := json.MarshalIndent(hooks, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal Agy hooks overlay: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(overlayConfigDir, "hooks.json"), data, 0o600); err != nil {
		return "", fmt.Errorf("write Agy hooks overlay: %w", err)
	}
	if permission {
		if err := createAgyNativePermissionOverlay(realConfigRoot, overlayConfigRoot); err != nil {
			return "", err
		}
	}
	return overlayConfigRoot, nil
}

// CLI 1.3.3 checks command permissions again inside run_command, after PreToolUse.
// An external Allow would otherwise still be soft-denied in print mode. Route
// those default resource checks through the installed, authenticated hook. The
// hook still requires the owning session's decision and hook failures block
// execution. Explicit native deny/ask rules retain their higher precedence.
// Only this private overlay changes; user settings and conversation stores stay
// intact. Never install these rules for a mode without the permission bridge.
func createAgyNativePermissionOverlay(realRoot, overlayRoot string) error {
	realCLI := filepath.Join(realRoot, "antigravity-cli")
	overlayCLI := filepath.Join(overlayRoot, "antigravity-cli")
	if err := os.MkdirAll(overlayCLI, 0o700); err != nil {
		return fmt.Errorf("create Agy CLI overlay: %w", err)
	}
	if err := mirrorDirectoryEntries(realCLI, overlayCLI, map[string]bool{"settings.json": true}); err != nil {
		return err
	}
	settings := make(map[string]json.RawMessage)
	if data, err := os.ReadFile(filepath.Join(realCLI, "settings.json")); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			return fmt.Errorf("parse Agy CLI settings: %w", err)
		}
		if settings == nil {
			settings = make(map[string]json.RawMessage)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read Agy CLI settings: %w", err)
	}
	permissions := make(map[string]json.RawMessage)
	if raw, ok := settings["permissions"]; ok {
		if err := json.Unmarshal(raw, &permissions); err != nil {
			return fmt.Errorf("parse Agy native permissions: %w", err)
		}
		if permissions == nil {
			permissions = make(map[string]json.RawMessage)
		}
	}
	var allowed []string
	for _, name := range []string{"allow", "deny", "ask"} {
		if raw, ok := permissions[name]; ok {
			var rules []string
			if err := json.Unmarshal(raw, &rules); err != nil {
				return fmt.Errorf("parse Agy native %s rules: %w", name, err)
			}
			if name == "allow" {
				allowed = rules
			}
		}
	}
	seen := make(map[string]bool, len(allowed))
	for _, rule := range allowed {
		seen[rule] = true
	}
	for _, rule := range []string{"command(*)", "unsandboxed(*)"} {
		if !seen[rule] {
			allowed = append(allowed, rule)
		}
	}
	permissions["allow"], _ = json.Marshal(allowed)
	settings["permissions"], _ = json.Marshal(permissions)
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal Agy CLI settings overlay: %w", err)
	}
	if err := os.WriteFile(filepath.Join(overlayCLI, "settings.json"), append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write Agy CLI settings overlay: %w", err)
	}
	return nil
}

func mirrorDirectoryEntries(sourceDir, targetDir string, skip map[string]bool) error {
	entries, err := os.ReadDir(sourceDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Agy config directory %s: %w", sourceDir, err)
	}
	for _, entry := range entries {
		if skip[entry.Name()] {
			continue
		}
		source := filepath.Join(sourceDir, entry.Name())
		target := filepath.Join(targetDir, entry.Name())
		if err := os.Symlink(source, target); err != nil {
			return fmt.Errorf("link Agy config %s: %w", source, err)
		}
	}
	return nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func bridgeTokenEqual(got, want string) bool {
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (b *agyPermissionBridge) Env() []string {
	return []string{
		antigravityhook.EnvAddress + "=" + b.address,
		antigravityhook.EnvToken + "=" + b.token,
	}
}

func (b *agyPermissionBridge) AgyConfigDir() string { return b.configDir }

func (b *agyPermissionBridge) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if b.ctx.Err() == nil {
				select {
				case b.events <- core.Event{Type: core.EventError, Error: fmt.Errorf("antigravity permission bridge: %w", err)}:
				case <-b.ctx.Done():
				}
			}
			return
		}
		b.wg.Add(1)
		go b.handleConnection(conn)
	}
}

func (b *agyPermissionBridge) handleConnection(conn net.Conn) {
	defer b.wg.Done()
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	var request antigravityhook.BridgeRequest
	if err := json.NewDecoder(io.LimitReader(conn, 4<<20)).Decode(&request); err != nil {
		b.writeResponse(conn, antigravityhook.BridgeResponse{Decision: "deny", Reason: "invalid cc-connect permission bridge request"})
		return
	}
	if !bridgeTokenEqual(request.Token, b.token) {
		b.writeResponse(conn, antigravityhook.BridgeResponse{Decision: "deny", Reason: "cc-connect permission bridge authentication failed"})
		return
	}
	_ = conn.SetDeadline(time.Time{})

	var input agyHookInput
	if err := json.Unmarshal(request.HookInput, &input); err != nil || strings.TrimSpace(input.ToolCall.Name) == "" {
		b.writeResponse(conn, antigravityhook.BridgeResponse{Decision: "deny", Reason: "invalid Agy tool permission request"})
		return
	}
	if input.ToolCall.Args == nil {
		input.ToolCall.Args = make(map[string]any)
	}
	if b.communication {
		decision := "deny"
		reason := "This session supports web research and communication tools. Project operations require a separately configured task."
		if isAgyCommunicationTool(input.ToolCall.Name, input.ToolCall.Args) ||
			(input.ToolCall.Name == "view_file" && isAgyCommunicationSchemaRead(input.ToolCall.Args, b.schemaDir)) {
			decision, reason = "allow", ""
		}
		b.writeResponse(conn, antigravityhook.BridgeResponse{Decision: decision, Reason: reason})
		return
	}

	requestID := fmt.Sprintf("agy-perm-%d", b.nextID.Add(1))
	resultCh := make(chan core.PermissionResult, 1)
	b.pendingMu.Lock()
	b.pending[requestID] = resultCh
	b.pendingMu.Unlock()
	defer func() {
		b.pendingMu.Lock()
		delete(b.pending, requestID)
		b.pendingMu.Unlock()
	}()

	preview := formatAgyToolInput(input.ToolCall.Args)
	event := core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     input.ToolCall.Name,
		ToolInput:    preview,
		ToolInputRaw: input.ToolCall.Args,
	}
	select {
	case b.events <- event:
	case <-b.ctx.Done():
		b.writeResponse(conn, antigravityhook.BridgeResponse{Decision: "deny", Reason: "cc-connect session closed"})
		return
	}

	select {
	case result := <-resultCh:
		response := antigravityhook.BridgeResponse{Decision: "allow"}
		if strings.EqualFold(strings.TrimSpace(result.Behavior), "deny") {
			response.Decision = "deny"
			response.Reason = strings.TrimSpace(result.Message)
			if response.Reason == "" {
				response.Reason = "User denied this tool use."
			}
		}
		b.writeResponse(conn, response)
	case <-b.ctx.Done():
		b.writeResponse(conn, antigravityhook.BridgeResponse{Decision: "deny", Reason: "cc-connect session closed"})
	}
}

func formatAgyToolInput(input map[string]any) string {
	if command, _ := input["CommandLine"].(string); strings.TrimSpace(command) != "" {
		return command
	}
	data, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", input)
	}
	return string(data)
}

func (b *agyPermissionBridge) writeResponse(conn net.Conn, response antigravityhook.BridgeResponse) {
	_ = json.NewEncoder(conn).Encode(response)
}

func (b *agyPermissionBridge) RespondPermission(requestID string, result core.PermissionResult) error {
	if b.communication {
		return fmt.Errorf("antigravity: communication permissions are fixed by the host policy")
	}
	behavior := strings.ToLower(strings.TrimSpace(result.Behavior))
	if behavior != "allow" && behavior != "deny" {
		return fmt.Errorf("antigravity: invalid permission behavior %q", result.Behavior)
	}
	result.Behavior = behavior

	b.pendingMu.Lock()
	ch := b.pending[requestID]
	b.pendingMu.Unlock()
	if ch == nil {
		return fmt.Errorf("antigravity: unknown permission request %q", requestID)
	}
	select {
	case ch <- result:
		return nil
	default:
		return fmt.Errorf("antigravity: permission request %q is already resolved", requestID)
	}
}

func (b *agyPermissionBridge) Close() {
	b.closeOnce.Do(func() {
		b.cancel()
		_ = b.listener.Close()
		b.wg.Wait()
		_ = os.RemoveAll(b.rootDir)
	})
}
