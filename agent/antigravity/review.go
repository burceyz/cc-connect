package antigravity

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const agyReviewProtocol = "cc-connect-review-gate-v1"

var errAgyReviewGateBusy = errors.New("antigravity: review gate is busy before task acquisition")

var reviewCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,99}$`)

type agyReviewOrigin struct {
	Project    string `json:"project"`
	SessionKey string `json:"session_key"`
	CWD        string `json:"cwd"`
}

type agyReviewTask struct {
	taskID       string
	taskDir      string
	manifestPath string
	instructions string
	origin       agyReviewOrigin
	revision     string
	candidateSHA string
	manifestSHA  string
}

type agyReviewResponse struct {
	Protocol        string   `json:"protocol"`
	TaskID          string   `json:"task_id"`
	TaskDir         string   `json:"task_dir"`
	ManifestPath    string   `json:"manifest_path"`
	Instructions    string   `json:"instructions"`
	Revision        string   `json:"revision"`
	CandidateSHA256 string   `json:"candidate_sha256"`
	ManifestSHA256  string   `json:"manifest_sha256"`
	CandidatePath   string   `json:"candidate_path"`
	Status          string   `json:"status"`
	AllowPublish    *bool    `json:"allow_publish"`
	Reasons         []string `json:"reasons"`
}

// A pending review ends the current execution turn without publishing a draft.
// Only explicitly recoverable author/material errors start an internal repair.
type agyReviewPending struct {
	code         string
	stage        string
	instructions string
	repair       bool
}

func (p *agyReviewPending) Error() string { return "antigravity: review pending: " + p.code }

func pendingReview(response agyReviewResponse) *agyReviewPending {
	code := "review_pending"
	if len(response.Reasons) > 0 && reviewCodePattern.MatchString(response.Reasons[0]) {
		code = response.Reasons[0]
	}
	p := &agyReviewPending{code: code, stage: "review_waiting", instructions: response.Instructions}
	switch code {
	case "manifest_missing", "candidate_missing", "candidate_decision_missing", "candidate_revision_required",
		"candidate_materials_changed", "dorothy_response_missing", "dorothy_response_invalid_json", "dorothy_response_binding_mismatch",
		"dorothy_review_not_read", "dorothy_findings_not_answered", "dorothy_response_schema_mismatch",
		"manifest_schema_mismatch", "manifest_task_mismatch", "explicit_manifest_mode_required",
		"independent_judgment_missing", "invalid_manifest_files", "invalid_manifest_file",
		"artifact_hash_mismatch", "source_changed_after_review", "hermes_launch_failed":
		p.stage, p.repair = "preparation", true
	case "hermes_changes_or_unresolved", "prior_findings_not_verified", "dorothy_findings_unresolved",
		"dorothy_resolution_requires_review":
		p.stage, p.repair = "changes_required", true
	default:
		if response.Status == "blocked" {
			p.stage = "changes_required"
		}
	}
	return p
}

func isReviewResumeRequest(prompt string) bool {
	prompt = strings.ToLower(strings.Trim(strings.TrimSpace(prompt), "。！!？?，,；; ."))
	switch prompt {
	case "继续", "继续执行", "继续执行任务", "继续完成", "继续完成任务", "接着做", "重试", "重试一下",
		"继续刚才的任务", "继续上一个任务", "continue", "resume", "retry":
		return true
	}
	return false
}

func (t *agyReviewTask) Env() []string {
	return []string{
		"CC_REVIEW_TASK_ID=" + t.taskID,
		"CC_REVIEW_TASK_DIR=" + t.taskDir,
		"CC_REVIEW_MANIFEST_PATH=" + t.manifestPath,
	}
}

func parseReviewCommand(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	var command []string
	switch v := value.(type) {
	case []string:
		command = append(command, v...)
	case []any:
		for _, arg := range v {
			text, ok := arg.(string)
			if !ok {
				return nil, fmt.Errorf("antigravity: review_command must be an argument array")
			}
			command = append(command, text)
		}
	default:
		return nil, fmt.Errorf("antigravity: review_command must be an argument array")
	}
	if len(command) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(command[0]) == "" {
		return nil, fmt.Errorf("antigravity: review_command executable is empty")
	}
	for _, arg := range command {
		if strings.ContainsRune(arg, '\x00') {
			return nil, fmt.Errorf("antigravity: review_command contains a NUL byte")
		}
	}
	return command, nil
}

func ipcEventID(messageID string) string {
	id := strings.TrimPrefix(messageID, "ipc-")
	if id != messageID && agyEventIDPattern.MatchString(id) {
		return id
	}
	return ""
}

func textSHA256(text string) string {
	digest := sha256.Sum256([]byte(text))
	return hex.EncodeToString(digest[:])
}

func reviewTaskID(eventID string) (string, error) {
	var data [16]byte
	if eventID == "" {
		if _, err := rand.Read(data[:]); err != nil {
			return "", fmt.Errorf("generate review task identity: %w", err)
		}
	} else {
		// A waiting IPC may call begin repeatedly. Its proposed UUID is stable;
		// the gate's durable event registry remains the source of truth.
		digest := sha256.Sum256([]byte("agy-review-event:" + eventID))
		copy(data[:], digest[:16])
	}
	data[6] = data[6]&0x0f | 0x40
	data[8] = data[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", data[0:4], data[4:6], data[6:8], data[8:10], data[10:16]), nil
}

func (as *antigravitySession) reviewOrigin() (agyReviewOrigin, error) {
	value := func(key string) string {
		for i := len(as.extraEnv) - 1; i >= 0; i-- {
			if strings.HasPrefix(as.extraEnv[i], key+"=") {
				return strings.TrimPrefix(as.extraEnv[i], key+"=")
			}
		}
		return os.Getenv(key)
	}
	cwd, err := filepath.Abs(as.workDir)
	if err == nil {
		cwd, err = filepath.EvalSymlinks(cwd)
	}
	origin := agyReviewOrigin{Project: value("CC_PROJECT"), SessionKey: value("CC_SESSION_KEY"), CWD: cwd}
	if err != nil || origin.Project == "" || origin.SessionKey == "" {
		return agyReviewOrigin{}, fmt.Errorf("antigravity: review origin is unavailable")
	}
	return origin, nil
}

func (as *antigravitySession) beginReview(ctx context.Context, prompt, eventID, conversationID string) (*agyReviewTask, error) {
	resume := eventID == "" && conversationID != "" && isReviewResumeRequest(prompt)
	return as.beginReviewRequest(ctx, prompt, eventID, conversationID, resume)
}

func (as *antigravitySession) beginReviewRequest(ctx context.Context, prompt, eventID, conversationID string, resume bool) (*agyReviewTask, error) {
	origin, err := as.reviewOrigin()
	if err != nil {
		return nil, err
	}
	id, err := reviewTaskID(eventID)
	if err != nil {
		return nil, err
	}
	request := struct {
		Protocol       string          `json:"protocol"`
		Op             string          `json:"op"`
		TaskID         string          `json:"task_id"`
		Origin         agyReviewOrigin `json:"origin"`
		RequestText    string          `json:"request_text"`
		RequestSHA256  string          `json:"request_sha256"`
		Continuation   bool            `json:"continuation"`
		EventID        string          `json:"event_id"`
		ConversationID string          `json:"conversation_id"`
		Resume         bool            `json:"resume"`
	}{agyReviewProtocol, "begin", id, origin, prompt, textSHA256(prompt), eventID != "", eventID, conversationID, resume}
	var response agyReviewResponse
	if err := as.callReview(ctx, request, &response, 5*time.Second); err != nil {
		return nil, fmt.Errorf("antigravity: review begin failed: %w", err)
	}
	if resume && response.Protocol == agyReviewProtocol && response.TaskID == id &&
		response.AllowPublish != nil && !*response.AllowPublish && response.Status == "blocked" &&
		len(response.Reasons) == 1 && response.Reasons[0] == "no_pending_review" {
		return as.beginReviewRequest(ctx, prompt, eventID, conversationID, false)
	}
	if response.Protocol == agyReviewProtocol && response.TaskID == id && response.Status == "pending_review" &&
		response.AllowPublish != nil && !*response.AllowPublish && response.TaskDir == "" && response.ManifestPath == "" &&
		len(response.Reasons) == 1 && response.Reasons[0] == "review_gate_busy" {
		return nil, errAgyReviewGateBusy
	}
	if response.Protocol != agyReviewProtocol || response.TaskID == "" || len(response.TaskID) > 256 ||
		strings.ContainsAny(response.TaskID, "\r\n\x00") || !filepath.IsAbs(response.TaskDir) ||
		response.ManifestPath != filepath.Join(response.TaskDir, "manifest.json") ||
		(eventID == "" && !resume && response.TaskID != id) || response.AllowPublish == nil || *response.AllowPublish {
		return nil, fmt.Errorf("antigravity: invalid review begin response")
	}
	if response.Status == "blocked" {
		return nil, fmt.Errorf("antigravity: review begin blocked (%s)", pendingReview(response).code)
	}
	if response.Status != "pending_review" {
		return nil, fmt.Errorf("antigravity: invalid review begin status")
	}
	if len(response.Instructions)+len(prompt) > 63<<10 {
		return nil, fmt.Errorf("antigravity: review instructions exceed supported size")
	}
	task := &agyReviewTask{taskID: response.TaskID, taskDir: response.TaskDir, manifestPath: response.ManifestPath,
		instructions: response.Instructions, origin: origin}
	if err := task.update(response); err != nil {
		return nil, err
	}
	return task, nil
}

func (t *agyReviewTask) update(response agyReviewResponse) error {
	if response.Revision != "" {
		if !agyEventIDPattern.MatchString(response.Revision) || !agyEventIDPattern.MatchString(response.CandidateSHA256) ||
			!agyEventIDPattern.MatchString(response.ManifestSHA256) {
			return fmt.Errorf("antigravity: invalid review revision identity")
		}
		t.revision, t.candidateSHA, t.manifestSHA = response.Revision, response.CandidateSHA256, response.ManifestSHA256
	}
	if response.Instructions != "" {
		if len(response.Instructions) > 60<<10 {
			return fmt.Errorf("antigravity: review instructions exceed supported size")
		}
		t.instructions = response.Instructions
	}
	return nil
}

func (as *antigravitySession) syncReview(ctx context.Context, task *agyReviewTask, conversationID, op, observedModel string) error {
	request := map[string]any{"protocol": agyReviewProtocol, "op": op, "task_id": task.taskID,
		"origin": task.origin, "conversation_id": conversationID}
	if observedModel != "" && as.model != "" && as.effort != "" {
		request["runtime_evidence"] = map[string]string{"configured_model": as.model,
			"observed_model": observedModel, "effort": as.effort, "mode": as.mode}
	}
	var response agyReviewResponse
	if err := as.callReview(ctx, request, &response, 60*time.Second); err != nil {
		return fmt.Errorf("antigravity: review %s failed: %w", op, err)
	}
	if response.Protocol != agyReviewProtocol || response.TaskID != task.taskID || response.AllowPublish == nil ||
		*response.AllowPublish || response.TaskDir != task.taskDir || response.ManifestPath != task.manifestPath {
		return fmt.Errorf("antigravity: invalid review %s identity", op)
	}
	if response.Status != "pending_review" {
		return fmt.Errorf("antigravity: review %s blocked (%s)", op, pendingReview(response).code)
	}
	return task.update(response)
}

func appendReviewInstructions(prompt, instructions string) string {
	if instructions == "" {
		return prompt
	}
	return prompt + "\n\n[Configured completion review instructions]\n" + instructions
}

func (as *antigravitySession) checkReview(ctx context.Context, task *agyReviewTask, conversationID, candidate string) error {
	manifest, manifestErr := readReviewManifest(task.manifestPath)
	if manifestErr != nil && !os.IsNotExist(manifestErr) {
		return fmt.Errorf("antigravity: review manifest cannot be read safely")
	}
	expectedManifestSHA := textSHA256(string(manifest))
	request := struct {
		Protocol        string          `json:"protocol"`
		Op              string          `json:"op"`
		TaskID          string          `json:"task_id"`
		Origin          agyReviewOrigin `json:"origin"`
		ConversationID  string          `json:"conversation_id"`
		CandidateText   string          `json:"candidate_text"`
		CandidateSHA256 string          `json:"candidate_sha256"`
		ManifestPath    string          `json:"manifest_path"`
	}{agyReviewProtocol, "check", task.taskID, task.origin, conversationID, candidate, textSHA256(candidate), task.manifestPath}
	var response agyReviewResponse
	if err := as.callReview(ctx, request, &response, 60*time.Second); err != nil {
		return fmt.Errorf("antigravity: review check failed: %w", err)
	}
	if response.Protocol != agyReviewProtocol || response.TaskID != task.taskID || response.CandidateSHA256 != request.CandidateSHA256 ||
		response.AllowPublish == nil || (*response.AllowPublish && response.Status != "verified") {
		return fmt.Errorf("antigravity: review receipt identity mismatch")
	}
	if err := task.update(response); err != nil {
		return err
	}
	switch response.Status {
	case "verified":
		current, err := readReviewManifest(task.manifestPath)
		if *response.AllowPublish && manifestErr == nil && err == nil &&
			response.ManifestSHA256 == expectedManifestSHA && bytes.Equal(current, manifest) {
			return nil
		}
	case "pending_review", "blocked":
		return pendingReview(response)
	}
	return fmt.Errorf("antigravity: invalid review receipt")
}

// The model may finish a review callback by writing response.json, without
// reproducing a final response. Only the gate can authorize these frozen bytes.
func (as *antigravitySession) finalizeReview(ctx context.Context, task *agyReviewTask, conversationID string) (string, error) {
	if !agyEventIDPattern.MatchString(task.revision) {
		return "", fmt.Errorf("antigravity: finalize revision is unavailable")
	}
	manifest, manifestErr := readReviewManifest(task.manifestPath)
	if manifestErr != nil && !os.IsNotExist(manifestErr) {
		return "", fmt.Errorf("antigravity: review manifest cannot be read safely")
	}
	request := map[string]any{"protocol": agyReviewProtocol, "op": "finalize", "task_id": task.taskID,
		"origin": task.origin, "conversation_id": conversationID, "revision": task.revision}
	var response agyReviewResponse
	if err := as.callReview(ctx, request, &response, 60*time.Second); err != nil {
		return "", fmt.Errorf("antigravity: review finalize failed: %w", err)
	}
	if response.Protocol != agyReviewProtocol || response.TaskID != task.taskID || response.Revision != task.revision ||
		response.CandidateSHA256 != task.candidateSHA || response.ManifestSHA256 != task.manifestSHA ||
		response.AllowPublish == nil || (*response.AllowPublish && response.Status != "verified") {
		return "", fmt.Errorf("antigravity: finalize receipt identity mismatch")
	}
	if response.Status == "pending_review" || response.Status == "blocked" {
		if err := task.update(response); err != nil {
			return "", err
		}
		return "", pendingReview(response)
	}
	expectedPath := filepath.Join(task.taskDir, "revisions", task.revision, "materials", "candidate.txt")
	if response.Status != "verified" || !*response.AllowPublish || manifestErr != nil || response.CandidatePath != expectedPath ||
		textSHA256(string(manifest)) != task.manifestSHA {
		return "", fmt.Errorf("antigravity: invalid finalized candidate")
	}
	root, rootErr := filepath.EvalSymlinks(task.taskDir)
	resolved, pathErr := filepath.EvalSymlinks(expectedPath)
	if rootErr != nil || pathErr != nil || resolved != filepath.Join(root, "revisions", task.revision, "materials", "candidate.txt") {
		return "", fmt.Errorf("antigravity: finalized candidate path escaped its revision")
	}
	candidate, err := readReviewFile(expectedPath, 8<<20)
	if err != nil || len(candidate) == 0 || textSHA256(string(candidate)) != task.candidateSHA {
		return "", fmt.Errorf("antigravity: finalized candidate hash mismatch")
	}
	current, err := readReviewManifest(task.manifestPath)
	if err != nil || !bytes.Equal(current, manifest) {
		return "", fmt.Errorf("antigravity: manifest changed during finalization")
	}
	return string(candidate), nil
}

func readReviewManifest(path string) ([]byte, error) {
	return readReviewFile(path, 4<<20)
}

func readReviewFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("review manifest must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("review manifest changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, fmt.Errorf("review manifest exceeds supported size")
	}
	return data, nil
}

type reviewOutputBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *reviewOutputBuffer) Bytes() []byte { return b.buffer.Bytes() }

func (b *reviewOutputBuffer) Write(data []byte) (int, error) {
	if b.buffer.Len()+len(data) > b.limit {
		return 0, fmt.Errorf("review command output exceeds supported size")
	}
	return b.buffer.Write(data)
}

func (as *antigravitySession) callReview(ctx context.Context, input, output any, timeout time.Duration) error {
	if len(as.reviewCommand) == 0 {
		return fmt.Errorf("review command is unavailable")
	}
	data, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode review request")
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// The configured executable and arguments are passed directly. No shell,
	// interpolation, or candidate text is ever placed on the command line.
	cmd := exec.CommandContext(callCtx, as.reviewCommand[0], as.reviewCommand[1:]...)
	cmd.Dir = as.workDir
	cmd.Env = core.MergeEnv(os.Environ(), as.extraEnv)
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(data)
	stdout := &reviewOutputBuffer{limit: 1 << 20}
	stderr := &reviewOutputBuffer{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		// Gate output can include private paths or candidate contents; do not
		// copy raw stdout/stderr into platform errors or logs.
		if callCtx.Err() != nil {
			return fmt.Errorf("review_command_timeout_or_cancelled")
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("review_command_exit_%d", exitErr.ExitCode())
		}
		return fmt.Errorf("review_command_start_failed")
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if err := decoder.Decode(output); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid review command JSON response")
	}
	return nil
}
