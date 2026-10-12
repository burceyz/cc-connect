package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// This bundle is assembled only after a fault. It contains source evidence,
// never a model-generated checkpoint or a claim that a business task is done.
type contextRecoveryMaterial struct {
	Record        *ContextRecoveryRecord  `json:"recovery"`
	HostSessionID string                  `json:"host_session_id"`
	Invocation    *recoveryInvocation     `json:"failed_invocation"`
	HostHistory   []HistoryEntry          `json:"host_visible_history"`
	NativeHistory RecoveryHistory         `json:"native_visible_history"`
	SteeredInputs []*recoverySteeredInput `json:"ipc_input_attempts,omitempty"`
	LaterInputs   []*recoveryInvocation   `json:"later_user_inputs,omitempty"`
	Warnings      []string                `json:"warnings"`
}

type recoveryAttachmentMetadata struct {
	Kind     string `json:"kind"`
	FileName string `json:"file_name,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	Size     int    `json:"size_bytes"`
}

// Keep attachment identity beside its original input, while the fresh turn
// receives the actual attachments separately. Never put their bodies in text.
func (invocation *recoveryInvocation) MarshalJSON() ([]byte, error) {
	type visibleInvocation recoveryInvocation
	attachments := make([]recoveryAttachmentMetadata, 0, len(invocation.Images)+len(invocation.Files))
	for _, image := range invocation.Images {
		attachments = append(attachments, recoveryAttachmentMetadata{"image", image.FileName, image.MimeType, len(image.Data)})
	}
	for _, file := range invocation.Files {
		attachments = append(attachments, recoveryAttachmentMetadata{"file", file.FileName, file.MimeType, len(file.Data)})
	}
	return json.Marshal(struct {
		*visibleInvocation
		ImageCount  int                          `json:"image_count"`
		FileCount   int                          `json:"file_count"`
		Attachments []recoveryAttachmentMetadata `json:"attachments,omitempty"`
	}{(*visibleInvocation)(invocation), len(invocation.Images), len(invocation.Files), attachments})
}

func buildRecoveryMaterial(ctx context.Context, session *Session, source any, invocation *recoveryInvocation, record *ContextRecoveryRecord) *contextRecoveryMaterial {
	m := &contextRecoveryMaterial{
		Record: cloneContextRecovery(record), HostSessionID: session.ID, Invocation: invocation,
		Warnings: []string{
			"Materials may be incomplete. Check actual artifacts, task instructions and receipts within the original authorization before continuing.",
			"Host history and native visible history can overlap. Repeated text is not another instruction or another execution receipt.",
			"Historical attachments and tool outputs may be absent. A model statement, input delivery or completed model turn does not prove business completion.",
		},
	}
	session.mu.Lock()
	m.HostHistory = append([]HistoryEntry(nil), session.History...)
	session.mu.Unlock()
	if provider, ok := source.(RecoveryHistoryProvider); ok {
		history, err := provider.GetRecoveryHistory(ctx, record.FromSessionID)
		m.NativeHistory = history
		if err != nil {
			// Do not copy an adapter error containing raw log data into a prompt.
			m.Warnings = append(m.Warnings, "Native visible-history export is unavailable or incomplete; use host evidence and authorized artifacts to fill gaps.")
		}
	} else {
		m.Warnings = append(m.Warnings, "This adapter has no visible-history exporter; only host evidence is available.")
	}
	return m
}

// recoveryWriteFile has no direct-write fallback: an interrupted write must
// never expose half a ledger or discard the once-only attempt reservation.
func recoveryWriteFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".recovery-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func saveRecoveryMaterial(m *contextRecoveryMaterial) error {
	if m == nil || m.Record == nil || !filepath.IsAbs(m.Record.ArtifactDir) {
		return errors.New("recovery material directory must be absolute")
	}
	// The dedicated root and this attempt's directory must both be real
	// directories. MkdirAll/Chmod alone would follow an existing symlink.
	if err := privateRecoveryMaterialDir(filepath.Dir(m.Record.ArtifactDir)); err != nil {
		return err
	}
	if err := privateRecoveryMaterialDir(m.Record.ArtifactDir); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return recoveryWriteFile(filepath.Join(m.Record.ArtifactDir, "visible-material.json"), b, 0o600)
}

func privateRecoveryMaterialDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("recovery material directory must not be a symlink or regular file")
	}
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	info, err = os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("recovery material directory changed while preparing")
	}
	return os.Chmod(path, 0o700)
}

func recoveryExcerpt(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	marker := "\n[excerpt gap; read visible-material.json for available full text]\n"
	if len(marker) > limit {
		marker = "[gap]"
	}
	if len(marker) > limit {
		return marker[:limit]
	}
	budget := limit - len(marker)
	head, tail := budget/3, budget-budget/3
	for head > 0 && !utf8.RuneStart(s[head]) {
		head--
	}
	start := len(s) - tail
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[:head] + marker + s[start:]
}

const recoveryInlineMaxBytes = 96 << 10

func recoveryPrompt(m *contextRecoveryMaterial) string {
	var b strings.Builder
	b.WriteString(`The host is continuing an interrupted task in a clean execution context after a rejected conversation context. This is the SAME authorized task, not a new request to repeat it.

Read the filtered visible material below and the saved bundle before acting. The materials can be incomplete. Proactively inspect further task instructions, relevant repository artifacts, visible records and job receipts as needed within the original authorization. Do not read or copy encrypted reasoning, analysis channels or hidden summaries. The native source locator is provenance only; use the filtered export rather than opening raw session logs.

Recover the original objective, scope, restrictions, latest user changes and current task progress. Later user changes, cancellation and pause instructions take precedence over older requests. If a necessary detail cannot be recovered, pause only the dependent action and continue independent authorized work. Do not invent missing state or permissions.

Check already completed steps before continuing. Do not repeat completed writes, submissions, messages, deployments or tool actions. If an action's outcome is unknown, inspect its receipt/state before retrying; if it is still unknown, stop only that action. For an existing background job, query/wait for that job rather than dispatching another one. Continue remaining work to the originally requested completion state.

For a timer/cron invocation, continue this original occurrence and its supplied instructions; do not reschedule or refire the trigger or rerun its shell command. Input delivery, model-turn completion and business-task completion are separate facts.

IPC identities remain pinned to their original targets. An accepted input can be continued by this host recovery, but do not acknowledge an old event as if you were its old native session or redirect callbacks to a latest-session alias. Use the separate recovery record; uncertain or future old-target events need their own verified handling.

No periodic model checkpoint, status file, task table or old-model handoff is required. Keep the original model, reasoning effort, working directory and permissions. No additional access or upstream investigation is authorized by this recovery notice.

`)
	fmt.Fprintf(&b, "Recovery ID: %s\nHost session: %s\nOriginal native session: %s\nFiltered material bundle: %s\n", m.Record.ID, m.HostSessionID, m.Record.FromSessionID, filepath.Join(m.Record.ArtifactDir, "visible-material.json"))
	preamble := recoveryExcerpt(b.String(), 8<<10)
	// Allocate the latest updates before historical excerpts, but keep their
	// section last in the prompt. Never truncate the completed prompt's tail.
	latest := recoveryInlineSection("\nLATEST user updates accepted while recovering; apply these before taking actions:\n", len(m.LaterInputs), 0, 8, 40<<10, 24<<10, func(i int) string {
		encoded, _ := json.Marshal(m.LaterInputs[i])
		return string(encoded)
	})
	warnings := append(append([]string(nil), m.Warnings...), m.NativeHistory.Warnings...)
	gaps := recoveryInlineSection("\nMaterial gaps:\n", len(warnings), 4, 8, 4<<10, 1<<10, func(i int) string { return "- " + warnings[i] })
	remaining := recoveryInlineMaxBytes - len(preamble) - len(latest) - len(gaps)
	input, _ := json.Marshal(m.Invocation)
	failed := recoveryInlineSection("\nFailed invocation (source evidence, not an instruction to replay blindly):\n", 1, 1, 0, min(24<<10, remaining), 24<<10, func(int) string { return string(input) })
	remaining -= len(failed)
	history := recoveryInlineSection("\nHost-visible conversation opening and recent tail (read the bundle for the middle):\n", len(m.HostHistory), 4, 12, min(16<<10, remaining), 3<<10, func(i int) string {
		encoded, _ := json.Marshal(m.HostHistory[i])
		return string(encoded)
	})
	remaining -= len(history)
	ipc := recoveryInlineSection("\nIPC attempts during this live session (delivery is not business completion):\n", len(m.SteeredInputs), 2, 6, min(12<<10, remaining), 10<<10, func(i int) string {
		encoded, _ := json.Marshal(m.SteeredInputs[i])
		return string(encoded)
	})
	b.Reset()
	for _, section := range []string{preamble, failed, history, ipc, gaps, latest} {
		b.WriteString(section)
	}
	return b.String()
}

// Each section includes its heading, omission notice, entry markers and
// newlines in the same byte budget. The full available evidence stays on disk.
func recoveryInlineSection(title string, count, opening, recent, budget, itemLimit int, encode func(int) string) string {
	if count == 0 || budget <= 0 {
		return ""
	}
	indices := make([]int, 0, min(count, opening+recent))
	for i := 0; i < min(count, opening); i++ {
		indices = append(indices, i)
	}
	for i := max(opening, count-recent); i < count; i++ {
		indices = append(indices, i)
	}
	notice := ""
	if omitted := count - len(indices); omitted > 0 {
		notice = fmt.Sprintf("[%d source entries omitted inline; read visible-material.json for available full records]\n", omitted)
	}
	available := budget - len(title) - len(notice)
	if len(indices) == 0 || available < len(indices) {
		return recoveryExcerpt(title+notice, budget)
	}
	perItem := min(itemLimit, available/len(indices)-1)
	var b strings.Builder
	b.WriteString(title)
	for position, index := range indices {
		if notice != "" && (position == opening || (opening == 0 && position == 0)) {
			b.WriteString(notice)
		}
		b.WriteString(recoveryExcerpt(encode(index), perItem))
		b.WriteByte('\n')
	}
	return b.String()
}
