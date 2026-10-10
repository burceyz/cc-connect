package antigravity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// antigravitySession resumes a fixed conversation with one -p process per Send.
type antigravitySession struct {
	cmd              string
	extraArgs        []string
	workDir          string
	model            string
	effort           string
	reviewCommand    []string
	mode             string
	timeout          time.Duration
	extraEnv         []string
	events           chan core.Event
	closeOnce        sync.Once
	chatID           atomic.Value // stores string
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	alive            atomic.Bool
	startMu          sync.Mutex // prevents Send/Close WaitGroup races
	activeRun        *agyRun
	permissionBridge *agyPermissionBridge
	invocationBridge *agyInvocationBridge
}

func newAntigravitySession(ctx context.Context, cmd string, extraArgs []string, workDir, model, mode, resumeID string, extraEnv []string, timeout time.Duration) (*antigravitySession, error) {
	sessionCtx, cancel := context.WithCancel(ctx)
	as := &antigravitySession{
		cmd: cmd, extraArgs: extraArgs, workDir: workDir, model: model, mode: mode,
		timeout: timeout, extraEnv: extraEnv, events: make(chan core.Event, 64), ctx: sessionCtx, cancel: cancel,
	}
	if mode == "default" {
		bridge, err := newAgyPermissionBridge(sessionCtx, as.events)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("antigravity: initialize permission bridge: %w", err)
		}
		as.permissionBridge = bridge
	}
	bridge, err := newAgyInvocationBridge(sessionCtx, as.permissionBridge != nil)
	if err != nil {
		cancel()
		if as.permissionBridge != nil {
			as.permissionBridge.Close()
		}
		return nil, fmt.Errorf("antigravity: initialize invocation bridge: %w", err)
	}
	as.invocationBridge = bridge
	if resumeID != "" && resumeID != core.ContinueSession {
		as.chatID.Store(resumeID)
	}
	as.alive.Store(true)
	return as, nil
}

func (as *antigravitySession) Send(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	run, err := as.startRun(prompt, messageID, images, files)
	if err != nil {
		return err
	}
	// Process creation is not an input acknowledgement. The first user_input
	// event confirms acceptance of this process's sole -p argument. Subsequent
	// body-less user_input events do not identify an external IPC message.
	timer := time.NewTimer(120 * time.Second)
	defer timer.Stop()
	select {
	case err := <-run.accepted:
		return err
	case <-run.ctx.Done():
		select {
		case err := <-run.accepted:
			return err
		default:
			return fmt.Errorf("antigravity: input acceptance interrupted: %w", run.ctx.Err())
		}
	case <-timer.C:
		run.cancel()
		return fmt.Errorf("antigravity: input acceptance timed out")
	}
}

func (as *antigravitySession) startRun(prompt, messageID string, images []core.ImageAttachment, files []core.FileAttachment) (*agyRun, error) {
	as.startMu.Lock()
	if !as.alive.Load() || as.ctx.Err() != nil {
		as.startMu.Unlock()
		return nil, fmt.Errorf("antigravity: session is closed")
	}
	if as.activeRun != nil {
		as.startMu.Unlock()
		return nil, fmt.Errorf("antigravity: a run is already active")
	}
	ctx, cancel := context.WithCancel(as.ctx)
	if as.timeout > 0 {
		cancel()
		ctx, cancel = context.WithTimeout(as.ctx, as.timeout)
	}
	run, err := newAgyRun(ctx, cancel, as.CurrentSessionID())
	if err != nil {
		cancel()
		as.startMu.Unlock()
		return nil, err
	}
	as.activeRun = run
	as.wg.Add(1)
	as.startMu.Unlock()
	fullPrompt, tempFiles := as.preparePrompt(prompt, messageID, images, files)
	started := false
	defer func() {
		if !started {
			cancel()
			run.end(false)
			as.startMu.Lock()
			if as.activeRun == run {
				as.activeRun = nil
			}
			as.startMu.Unlock()
			as.wg.Done()
			for _, path := range tempFiles {
				_ = os.Remove(path)
			}
		}
	}()
	if len(as.reviewCommand) > 0 {
		task, err := as.beginReview(run.ctx, fullPrompt, ipcEventID(messageID), as.CurrentSessionID())
		if err != nil {
			return nil, err
		}
		run.mu.Lock()
		run.reviewTask = task
		run.mu.Unlock()
		fullPrompt = appendReviewInstructions(fullPrompt, task.instructions)
	}
	chatID := as.CurrentSessionID()
	args := as.buildAntigravityArgs(chatID, chatID != "", as.mode, as.invocationBridge.configDir, fullPrompt)
	cmd := exec.CommandContext(ctx, as.cmd, args...)
	cmd.WaitDelay = time.Second
	cmd.Dir = as.workDir
	cmd.Env = core.MergeEnv(os.Environ(), as.extraEnv)
	if as.permissionBridge != nil {
		cmd.Env = core.MergeEnv(cmd.Env, as.permissionBridge.Env())
	}
	cmd.Env = core.MergeEnv(cmd.Env, as.invocationBridge.Env(run))
	if run.reviewTask != nil {
		cmd.Env = core.MergeEnv(cmd.Env, run.reviewTask.Env())
	}
	// -p consumes stdin to EOF. Never leave an open input pipe on this path.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("antigravity: stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := as.invocationBridge.setRun(run); err != nil {
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		run.end(false)
		return nil, fmt.Errorf("antigravity: start: %w", err)
	}
	started = true
	go as.readLoop(run, cmd, stdout, &stderr, tempFiles)
	return run, nil
}

func (as *antigravitySession) preparePrompt(prompt, messageID string, images []core.ImageAttachment, files []core.FileAttachment) (string, []string) {
	attachDir := filepath.Join(as.workDir, ".cc-connect", "attachments")
	if len(images) > 0 && os.MkdirAll(attachDir, 0o700) != nil {
		attachDir = os.TempDir()
	}
	var imageRefs []string
	for _, img := range images {
		ext := ".png"
		switch img.MimeType {
		case "image/jpeg":
			ext = ".jpg"
		case "image/gif":
			ext = ".gif"
		case "image/webp":
			ext = ".webp"
		}
		file, err := os.CreateTemp(attachDir, "img-*"+ext)
		if err != nil {
			slog.Warn("antigravity: cannot save image attachment")
			continue
		}
		_, err = file.Write(img.Data)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			_ = os.Remove(file.Name())
			continue
		}
		imageRefs = append(imageRefs, file.Name())
	}
	fileRefs := core.SaveFilesToDisk(as.workDir, messageID, files)
	if len(imageRefs) > 0 {
		if prompt == "" {
			prompt = "Please analyze the attached image(s)."
		}
		prompt += "\n\n[Attached images saved at: " + strings.Join(imageRefs, ", ") + "]"
	}
	if len(fileRefs) > 0 {
		if prompt == "" {
			prompt = "Please analyze the attached file(s)."
		}
		prompt += "\n\n[Attached files saved at: " + strings.Join(fileRefs, ", ") + "]"
	}
	return prompt, append(imageRefs, fileRefs...)
}

func (as *antigravitySession) buildAntigravityArgs(chatID string, isResume bool, mode, agyConfigDir, fullPrompt string) []string {
	args := append([]string{}, as.extraArgs...)
	if agyConfigDir != "" {
		args = append(args, "--gemini_dir="+agyConfigDir)
	}
	args = append(args, "--output-format", "stream-json", "--print-timeout=24h")
	if isResume {
		args = append(args, "--conversation", chatID)
	}
	if model := strings.TrimSpace(as.model); model != "" {
		args = append(args, "--model", model)
	}
	if as.effort != "" {
		args = append(args, "--effort", as.effort)
	}
	switch mode {
	case "yolo":
		args = append(args, "--dangerously-skip-permissions")
	case "plan":
		args = append(args, "--sandbox")
	}
	return append(args, "-p", fullPrompt)
}

func (as *antigravitySession) QueueInput(expectedSessionID, eventID, prompt string, report func(core.LiveInputProgress) error) error {
	if !agyEventIDPattern.MatchString(eventID) || strings.TrimSpace(prompt) == "" || len(prompt) > 8192 || report == nil {
		return fmt.Errorf("invalid Agy queued input")
	}
	as.startMu.Lock()
	run := as.activeRun
	alive := as.alive.Load()
	as.startMu.Unlock()
	if !alive || run == nil {
		return core.ErrNoActiveInput
	}
	content := prompt
	if len(as.reviewCommand) > 0 {
		run.mu.Lock()
		task, ready := run.reviewTask, run.inputAccepted && !run.ended
		matches := run.sessionID == expectedSessionID && expectedSessionID != ""
		run.mu.Unlock()
		if !ready || task == nil {
			return core.ErrNoActiveInput
		}
		if !matches {
			return fmt.Errorf("Agy queued input conversation mismatch")
		}
		callback, err := as.beginReview(run.ctx, prompt, eventID, expectedSessionID)
		if err != nil {
			if errors.Is(err, errAgyReviewGateBusy) {
				return core.ErrNoActiveInput
			}
			return err
		}
		if callback.taskID != task.taskID {
			// A distinct task needs its own candidate and review manifest. The
			// deterministic event ID lets core retry it at idle without new tasks.
			return core.ErrNoActiveInput
		}
		content = appendReviewInstructions(prompt, callback.instructions)
	}
	return run.queuePrepared(expectedSessionID, eventID, prompt, content, report)
}

func (as *antigravitySession) RespondPermission(requestID string, result core.PermissionResult) error {
	if !as.alive.Load() {
		return fmt.Errorf("session is closed")
	}
	if as.permissionBridge == nil {
		return fmt.Errorf("antigravity: permission responses are only available in default mode")
	}
	return as.permissionBridge.RespondPermission(requestID, result)
}

func (as *antigravitySession) emit(event core.Event) {
	select {
	case as.events <- event:
	case <-as.ctx.Done():
	}
}

func (as *antigravitySession) Events() <-chan core.Event { return as.events }

func (as *antigravitySession) CurrentSessionID() string {
	v, _ := as.chatID.Load().(string)
	return v
}

func (as *antigravitySession) Alive() bool { return as.alive.Load() }

func (as *antigravitySession) Close() error {
	as.startMu.Lock()
	as.alive.Store(false)
	run := as.activeRun
	as.startMu.Unlock()
	if run != nil {
		run.end(true)
	}
	as.cancel()
	as.invocationBridge.Close()
	if as.permissionBridge != nil {
		as.permissionBridge.Close()
	}
	done := make(chan struct{})
	go func() {
		as.wg.Wait()
		as.closeOnce.Do(func() { close(as.events) })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		slog.Warn("antigravity: close timed out")
	}
	return nil
}
