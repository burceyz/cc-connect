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
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const agyReviewProtocol = "cc-connect-review-gate-v1"

var errAgyReviewGateBusy = errors.New("antigravity: review gate is busy before task acquisition")

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
	}{agyReviewProtocol, "begin", id, origin, prompt, textSHA256(prompt), eventID != "", eventID, conversationID}
	var response struct {
		Protocol     string   `json:"protocol"`
		TaskID       string   `json:"task_id"`
		TaskDir      string   `json:"task_dir"`
		ManifestPath string   `json:"manifest_path"`
		Instructions string   `json:"instructions"`
		Status       string   `json:"status"`
		AllowPublish *bool    `json:"allow_publish"`
		Reasons      []string `json:"reasons"`
	}
	if err := as.callReview(ctx, request, &response, 5*time.Second); err != nil {
		return nil, fmt.Errorf("antigravity: review begin failed: %w", err)
	}
	if response.Protocol == agyReviewProtocol && response.TaskID == id && response.Status == "pending_review" &&
		response.AllowPublish != nil && !*response.AllowPublish && response.TaskDir == "" && response.ManifestPath == "" &&
		len(response.Reasons) == 1 && response.Reasons[0] == "review_gate_busy" {
		return nil, errAgyReviewGateBusy
	}
	if response.Protocol != agyReviewProtocol || response.TaskID == "" || len(response.TaskID) > 256 ||
		strings.ContainsAny(response.TaskID, "\r\n\x00") || !filepath.IsAbs(response.TaskDir) ||
		response.ManifestPath != filepath.Join(response.TaskDir, "manifest.json") ||
		(eventID == "" && response.TaskID != id) || response.AllowPublish == nil || *response.AllowPublish {
		return nil, fmt.Errorf("antigravity: invalid review begin response")
	}
	if response.Status == "blocked" {
		return nil, fmt.Errorf("antigravity: 复核任务已阻塞，未接纳新输入（任务 %s）", response.TaskID)
	}
	if response.Status != "pending_review" {
		return nil, fmt.Errorf("antigravity: invalid review begin status")
	}
	if len(response.Instructions)+len(prompt) > 63<<10 {
		return nil, fmt.Errorf("antigravity: review instructions exceed supported size")
	}
	return &agyReviewTask{taskID: response.TaskID, taskDir: response.TaskDir, manifestPath: response.ManifestPath,
		instructions: response.Instructions, origin: origin}, nil
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
		return fmt.Errorf("antigravity: 无法固定复核材料，候选结果未发布（任务 %s）", task.taskID)
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
	var response struct {
		Protocol        string `json:"protocol"`
		TaskID          string `json:"task_id"`
		CandidateSHA256 string `json:"candidate_sha256"`
		ManifestSHA256  string `json:"manifest_sha256"`
		Status          string `json:"status"`
		AllowPublish    bool   `json:"allow_publish"`
	}
	if err := as.callReview(ctx, request, &response, 60*time.Second); err != nil {
		return fmt.Errorf("antigravity: 复核关口执行失败，候选结果未发布（任务 %s）", task.taskID)
	}
	if response.Protocol != agyReviewProtocol || response.TaskID != task.taskID || response.CandidateSHA256 != request.CandidateSHA256 {
		return fmt.Errorf("antigravity: 复核回执身份不匹配，候选结果未发布（任务 %s）", task.taskID)
	}
	switch response.Status {
	case "verified":
		current, err := readReviewManifest(task.manifestPath)
		if response.AllowPublish && manifestErr == nil && err == nil &&
			response.ManifestSHA256 == expectedManifestSHA && bytes.Equal(current, manifest) {
			return nil
		}
	case "pending_review":
		return fmt.Errorf("antigravity: 结果已保存，等待 Hermes 复核（任务 %s）", task.taskID)
	case "blocked":
		return fmt.Errorf("antigravity: 结果已保存，复核尚未通过（任务 %s）", task.taskID)
	}
	return fmt.Errorf("antigravity: 复核回执无效，候选结果未发布（任务 %s）", task.taskID)
}

func readReviewManifest(path string) ([]byte, error) {
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
	data, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil || len(data) > 4<<20 {
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
		return fmt.Errorf("review command failed")
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if err := decoder.Decode(output); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid review command JSON response")
	}
	return nil
}
