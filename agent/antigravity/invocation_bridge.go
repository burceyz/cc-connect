package antigravity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

const agyInvocationHookName = "cc-connect-invocation-bridge"

var agyEventIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type agyQueuedInput struct {
	prompt  string
	content string
	phase   string
	turnID  string
	report  func(core.LiveInputProgress) error
}

// A run is one -p process, not an Antigravity user turn. The CLI can add user
// turns after hook injection while keeping this process and conversation alive.
type agyRun struct {
	ctx        context.Context
	cancel     context.CancelFunc
	generation string
	token      string
	expectedID string
	bound      chan struct{}
	accepted   chan error
	acceptOnce sync.Once

	mu            sync.Mutex
	sessionID     string
	inputAccepted bool
	ended         bool
	stopped       bool
	inputs        map[string]*agyQueuedInput
	order         []string
	reviewTask    *agyReviewTask
	reviewRepairs int
}

func newAgyRun(ctx context.Context, cancel context.CancelFunc, expectedID string) (*agyRun, error) {
	random := make([]byte, 48)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("generate Agy invocation identity: %w", err)
	}
	return &agyRun{
		ctx: ctx, cancel: cancel, expectedID: expectedID,
		generation: hex.EncodeToString(random[:16]), token: hex.EncodeToString(random[16:]),
		bound: make(chan struct{}), accepted: make(chan error, 1),
		inputs: make(map[string]*agyQueuedInput),
	}, nil
}

func (r *agyRun) bind(sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended || sessionID == "" || len(sessionID) > 256 || strings.ContainsAny(sessionID, "\r\n\x00") {
		return fmt.Errorf("invalid Agy conversation identity")
	}
	if r.sessionID != "" || (r.expectedID != "" && r.expectedID != sessionID) {
		return fmt.Errorf("Agy conversation identity changed")
	}
	r.sessionID = sessionID
	close(r.bound)
	return nil
}

func (r *agyRun) acceptInput(sessionID string) error {
	r.mu.Lock()
	if r.sessionID == "" || sessionID != r.sessionID || r.ended {
		r.mu.Unlock()
		return fmt.Errorf("Agy input identity is unavailable")
	}
	first := !r.inputAccepted
	r.inputAccepted = true
	r.mu.Unlock()
	if first {
		r.acceptOnce.Do(func() { r.accepted <- nil })
	}
	return nil
}

func (r *agyRun) queue(expectedID, eventID, prompt string, report func(core.LiveInputProgress) error) error {
	return r.queuePrepared(expectedID, eventID, prompt, prompt, report)
}

func (r *agyRun) queuePrepared(expectedID, eventID, prompt, content string, report func(core.LiveInputProgress) error) error {
	if !agyEventIDPattern.MatchString(eventID) || strings.TrimSpace(prompt) == "" || len(prompt) > 8192 ||
		len(content) > 64<<10 || report == nil {
		return fmt.Errorf("invalid Agy queued input")
	}
	r.mu.Lock()
	if r.ended || !r.inputAccepted || r.ctx.Err() != nil {
		r.mu.Unlock()
		return core.ErrNoActiveInput
	}
	if expectedID == "" || r.sessionID != expectedID {
		r.mu.Unlock()
		return fmt.Errorf("Agy queued input conversation mismatch")
	}
	if prior := r.inputs[eventID]; prior != nil {
		r.mu.Unlock()
		if prior.prompt != prompt {
			return fmt.Errorf("Agy queued input conflicts with existing event")
		}
		return nil
	}
	item := &agyQueuedInput{prompt: prompt, content: content, phase: "registering", report: report}
	r.inputs[eventID] = item
	r.order = append(r.order, eventID)
	r.mu.Unlock()

	// No lock required by teardown or CurrentSessionID is held while core
	// persists a receipt. Registering inputs cannot be claimed by a hook.
	if err := report(core.LiveInputProgress{Phase: core.LiveInputPhaseQueued}); err != nil {
		r.mu.Lock()
		item.phase = core.LiveInputPhaseUnknown
		r.mu.Unlock()
		return fmt.Errorf("persist Agy queued input: %w", err)
	}
	r.mu.Lock()
	ended, stopped := r.ended, r.stopped
	if ended {
		item.phase = core.LiveInputPhaseReleased
	} else {
		item.phase = core.LiveInputPhaseQueued
	}
	r.mu.Unlock()
	if ended {
		return report(releasedProgress(stopped))
	}
	return nil
}

func releasedProgress(stopped bool) core.LiveInputProgress {
	progress := core.LiveInputProgress{Phase: core.LiveInputPhaseReleased}
	if stopped {
		progress.Code = core.LiveInputCodeStopped
	}
	return progress
}

func (r *agyRun) claim(input antigravityhook.InvocationInput, phase string) (antigravityhook.InvocationOutput, error) {
	var output antigravityhook.InvocationOutput
	if !input.Valid() || (phase != "pre" && phase != "post") {
		return output, fmt.Errorf("invalid Agy invocation boundary")
	}
	r.mu.Lock()
	if r.ended || r.ctx.Err() != nil || !r.inputAccepted || input.ConversationID != r.sessionID {
		r.mu.Unlock()
		return output, nil
	}
	var item *agyQueuedInput
	for _, id := range r.order {
		if r.inputs[id].phase == core.LiveInputPhaseQueued {
			item = r.inputs[id]
			break
		}
	}
	if item == nil {
		r.mu.Unlock()
		return output, nil
	}
	// This identifies a real hook invocation, not a native turn/steer ID.
	item.turnID = fmt.Sprintf("agy:%s:run:%s:%s:%d", r.sessionID, r.generation, phase, *input.InvocationNum)
	item.phase = "claiming"
	turnID := item.turnID
	r.mu.Unlock()

	if err := item.report(core.LiveInputProgress{Phase: core.LiveInputPhaseClaimed, TurnID: turnID}); err != nil {
		r.mu.Lock()
		item.phase = core.LiveInputPhaseUnknown
		r.mu.Unlock()
		return output, fmt.Errorf("persist Agy input claim: %w", err)
	}
	r.mu.Lock()
	if r.ended || r.ctx.Err() != nil {
		item.phase = core.LiveInputPhaseUnknown
		r.mu.Unlock()
		_ = item.report(core.LiveInputProgress{Phase: core.LiveInputPhaseUnknown, TurnID: turnID, Code: "invocation_ended_during_claim"})
		return output, nil
	}
	item.phase = core.LiveInputPhaseClaimed
	output.InjectSteps = []antigravityhook.InjectedStep{{UserMessage: item.content}}
	if phase == "post" {
		output.TerminationBehavior = "force_continue"
	}
	r.mu.Unlock()
	return output, nil
}

func (r *agyRun) end(stopped bool) {
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return
	}
	r.ended, r.stopped = true, stopped
	type pendingReport struct {
		item     *agyQueuedInput
		progress core.LiveInputProgress
	}
	var reports []pendingReport
	for _, id := range r.order {
		item := r.inputs[id]
		switch item.phase {
		case core.LiveInputPhaseQueued:
			item.phase = core.LiveInputPhaseReleased
			reports = append(reports, pendingReport{item, releasedProgress(stopped)})
		case core.LiveInputPhaseClaimed:
			item.phase = core.LiveInputPhaseUnknown
			reports = append(reports, pendingReport{item, core.LiveInputProgress{
				Phase: core.LiveInputPhaseUnknown, TurnID: item.turnID, Code: "invocation_consumption_unconfirmed",
			}})
			// Queue/claim owns the notification while its durable report is in flight.
		}
	}
	r.mu.Unlock()
	for _, report := range reports {
		if err := report.item.report(report.progress); err != nil {
			slog.Warn("antigravity: live input terminal receipt failed", "phase", report.progress.Phase)
		}
	}
}

type agyInvocationBridge struct {
	ctx       context.Context
	cancel    context.CancelFunc
	listener  net.Listener
	rootDir   string
	configDir string
	mu        sync.Mutex
	run       *agyRun
	closed    bool
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newAgyInvocationBridge(ctx context.Context, permission bool) (*agyInvocationBridge, error) {
	bridgeCtx, cancel := context.WithCancel(ctx)
	root, err := os.MkdirTemp("", "cc-connect-agy-invocation-")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create Agy invocation directory: %w", err)
	}
	configDir, err := createAgyConfigOverlayForHooks(root, permission, true)
	if err != nil {
		cancel()
		_ = os.RemoveAll(root)
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		_ = os.RemoveAll(root)
		return nil, fmt.Errorf("listen for Agy invocation hooks: %w", err)
	}
	b := &agyInvocationBridge{ctx: bridgeCtx, cancel: cancel, listener: listener, rootDir: root, configDir: configDir}
	b.wg.Add(1)
	go b.acceptLoop()
	return b, nil
}

func (b *agyInvocationBridge) setRun(run *agyRun) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("Agy invocation bridge is closed")
	}
	b.run = run
	return nil
}

func (b *agyInvocationBridge) Env(run *agyRun) []string {
	return []string{
		antigravityhook.EnvInvocationAddress + "=" + b.listener.Addr().String(),
		antigravityhook.EnvInvocationToken + "=" + run.token,
		antigravityhook.EnvInvocationGeneration + "=" + run.generation,
	}
}

func (b *agyInvocationBridge) acceptLoop() {
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

func (b *agyInvocationBridge) handleConnection(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	var request antigravityhook.InvocationRequest
	if json.NewDecoder(io.LimitReader(conn, 4<<20)).Decode(&request) != nil || !request.Input.Valid() ||
		(request.Phase != "pre" && request.Phase != "post") {
		_ = json.NewEncoder(conn).Encode(antigravityhook.InvocationResponse{Error: "invalid_invocation_request"})
		return
	}
	b.mu.Lock()
	run, closed := b.run, b.closed
	b.mu.Unlock()
	if closed || run == nil || request.Generation != run.generation || !bridgeTokenEqual(request.Token, run.token) {
		_ = json.NewEncoder(conn).Encode(antigravityhook.InvocationResponse{Error: "invocation_identity_mismatch"})
		return
	}
	// stdout and hook execution are separate pipes. Let the stream parser bind
	// the primary ID; never bind to a hook (which might belong to a subagent).
	select {
	case <-run.bound:
	case <-run.ctx.Done():
		return
	case <-b.ctx.Done():
		return
	case <-time.After(5 * time.Second):
		_ = json.NewEncoder(conn).Encode(antigravityhook.InvocationResponse{Error: "invocation_identity_unbound"})
		return
	}
	output, err := run.claim(request.Input, request.Phase)
	response := antigravityhook.InvocationResponse{Output: output}
	if err != nil {
		response.Error = "invocation_claim_failed"
	}
	// Serialize the final handoff with end/stop. A failed/partial write remains
	// claimed; it is never released or automatically requeued.
	run.mu.Lock()
	if run.ended || run.ctx.Err() != nil {
		response.Output = antigravityhook.InvocationOutput{}
	}
	_ = json.NewEncoder(conn).Encode(response)
	run.mu.Unlock()
}

func (b *agyInvocationBridge) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		run := b.run
		b.mu.Unlock()
		b.cancel()
		_ = b.listener.Close()
		if run != nil {
			run.end(true)
		}
		b.wg.Wait()
		_ = os.RemoveAll(b.rootDir)
	})
}
