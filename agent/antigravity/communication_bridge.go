package antigravity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

const agyCommunicationProtocol = "cc-dorothy-communication-v1"

type agyCommunicationBridge struct {
	ctx       context.Context
	cancel    context.CancelFunc
	listener  net.Listener
	command   []string
	workDir   string
	env       []string
	mu        sync.Mutex
	run       *agyRun
	scope     string
	closed    bool
	callMu    sync.Mutex
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newAgyCommunicationBridge(ctx context.Context, command []string, workDir string, env []string) (*agyCommunicationBridge, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("antigravity: communication command is unavailable")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("antigravity: listen for communication tools: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &agyCommunicationBridge{ctx: ctx, cancel: cancel, listener: listener,
		command: append([]string(nil), command...), workDir: workDir, env: append([]string(nil), env...)}
	b.wg.Add(1)
	go b.acceptLoop()
	return b, nil
}

func (b *agyCommunicationBridge) setRun(run *agyRun, messageID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("antigravity: communication bridge is closed")
	}
	b.run = run
	b.scope = run.generation
	if messageID != "" {
		b.scope = textSHA256("communication-message:" + messageID)
	}
	return nil
}

func (b *agyCommunicationBridge) Env(run *agyRun) []string {
	return []string{
		antigravityhook.EnvCommunicationAddress + "=" + b.listener.Addr().String(),
		antigravityhook.EnvCommunicationToken + "=" + run.token,
		antigravityhook.EnvCommunicationGeneration + "=" + run.generation,
	}
}

func (b *agyCommunicationBridge) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.wg.Add(1)
		go b.handleConnection(conn)
	}
}

func (b *agyCommunicationBridge) handleConnection(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()
	stopClose := context.AfterFunc(b.ctx, func() { _ = conn.Close() })
	defer stopClose()
	_ = conn.SetDeadline(time.Now().Add(50 * time.Second))
	respond := func(result json.RawMessage, code string) {
		_ = json.NewEncoder(conn).Encode(antigravityhook.CommunicationResponse{Result: result, Error: code})
	}
	var request antigravityhook.CommunicationRequest
	decoder := json.NewDecoder(io.LimitReader(conn, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || (request.Method != "tools/list" && request.Method != "tools/call") || !json.Valid(request.Params) {
		respond(nil, "invalid_request")
		return
	}
	b.mu.Lock()
	run, scope, closed := b.run, b.scope, b.closed
	b.mu.Unlock()
	if closed || run == nil || request.Generation != run.generation || !bridgeTokenEqual(request.Token, run.token) {
		respond(nil, "identity_mismatch")
		return
	}
	run.mu.Lock()
	ended := run.ended
	run.mu.Unlock()
	if ended || run.ctx.Err() != nil {
		respond(nil, "run_ended")
		return
	}
	// Only one backend invocation runs at a time. Its durable operation key
	// handles repeated calls without creating a second outgoing event/job.
	b.callMu.Lock()
	defer b.callMu.Unlock()
	b.mu.Lock()
	current, closed := b.run, b.closed
	b.mu.Unlock()
	run.mu.Lock()
	ended = run.ended
	run.mu.Unlock()
	if current != run || closed || ended || run.ctx.Err() != nil || b.ctx.Err() != nil {
		respond(nil, "run_ended")
		return
	}
	result, err := b.callBackend(run.toolCtx, request, scope)
	if err != nil {
		respond(nil, "backend_unavailable")
		return
	}
	respond(result, "")
}

func (b *agyCommunicationBridge) callBackend(ctx context.Context, request antigravityhook.CommunicationRequest, scope string) (json.RawMessage, error) {
	data, err := json.Marshal(map[string]any{
		"protocol": agyCommunicationProtocol, "method": request.Method, "params": request.Params,
		"context": map[string]string{"operation_scope": scope},
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	stopCancel := context.AfterFunc(b.ctx, cancel)
	defer stopCancel()
	cmd := exec.CommandContext(ctx, b.command[0], b.command[1:]...)
	cmd.Dir = b.workDir
	cmd.Env = core.MergeEnv(core.MergeEnv(os.Environ(), b.env), []string{"CC_IPC_CWD=" + b.workDir})
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(data)
	stdout := &reviewOutputBuffer{limit: 1 << 20}
	stderr := &reviewOutputBuffer{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("communication command failed")
	}
	var output map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if decoder.Decode(&output) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("invalid communication response")
	}
	if (request.Method == "tools/list" && output["tools"] == nil) || (request.Method == "tools/call" && output["content"] == nil) {
		return nil, fmt.Errorf("incomplete communication response")
	}
	return json.RawMessage(stdout.Bytes()), nil
}

func (b *agyCommunicationBridge) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		b.cancel()
		_ = b.listener.Close()
		b.wg.Wait()
	})
}
