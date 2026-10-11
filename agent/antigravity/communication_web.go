package antigravity

import (
	"path/filepath"
	"strconv"
	"sync"
)

const agyCommunicationWebContentLimit = 16 << 20

// The native read_url_content tool saves its body under the tool's step index
// and asks the model to use view_file. Only successful events from the owning
// process can register these files; a model-supplied path never creates a grant.
type agyCommunicationWebCache struct {
	rootDir string
	mu      sync.RWMutex
	session string
	files   map[string]string
}

func (c *agyCommunicationWebCache) record(step agyStreamStep) {
	if c == nil || step.State != "DONE" || step.StepIndex == nil || *step.StepIndex < 0 ||
		!validAntigravitySessionID(step.ConversationID) {
		return
	}
	name := step.ToolName
	if step.ToolInfo != nil {
		if step.ToolInfo.Error != nil || (name != "" && step.ToolInfo.Name != "" && name != step.ToolInfo.Name) {
			return
		}
		if name == "" {
			name = step.ToolInfo.Name
		}
	}
	if name != "read_url_content" {
		return
	}
	root, err := filepath.EvalSymlinks(c.rootDir)
	if err != nil || root != c.rootDir {
		return
	}
	path := filepath.Join(root, step.ConversationID, ".system_generated", "steps", strconv.Itoa(*step.StepIndex), "content.md")
	real, err := filepath.EvalSymlinks(path)
	if err != nil || real != path {
		return
	}
	data, err := readReviewFile(path, agyCommunicationWebContentLimit)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != "" && c.session != step.ConversationID {
		return
	}
	c.session = step.ConversationID
	if c.files == nil {
		c.files = make(map[string]string)
	}
	c.files[path] = textSHA256(string(data))
}

func (c *agyCommunicationWebCache) permits(input agyHookInput) bool {
	if c == nil || input.ToolCall.Name != "view_file" || input.ConversationID == "" {
		return false
	}
	path, _ := input.ToolCall.Args["AbsolutePath"].(string)
	if !filepath.IsAbs(path) {
		return false
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	c.mu.RLock()
	expected, registered := c.files[real]
	current := input.ConversationID == c.session
	c.mu.RUnlock()
	if !current || !registered {
		return false
	}
	data, err := readReviewFile(real, agyCommunicationWebContentLimit)
	return err == nil && textSHA256(string(data)) == expected
}
