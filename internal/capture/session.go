package capture

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	maxFiles    = 60
	maxCommands = 8
	maxPending  = 12
	maxField    = 240
)

type Tool struct {
	Name   string `json:"name"`
	Detail string `json:"detail,omitempty"`
}

type Session struct {
	ID         string          `json:"id"`
	Cwd        string          `json:"cwd,omitempty"`
	Branch     string          `json:"branch,omitempty"`
	Title      string          `json:"title,omitempty"`
	Started    int64           `json:"started,omitempty"`
	Last       int64           `json:"last,omitempty"`
	Prompt     string          `json:"prompt,omitempty"`
	Files      []string        `json:"files,omitempty"`
	Commands   []string        `json:"commands,omitempty"`
	Background []string        `json:"background,omitempty"`
	Pending    map[string]Tool `json:"pending,omitempty"`
	LastError  string          `json:"last_error,omitempty"`
	Errors     int             `json:"errors,omitempty"`
	Turns      int             `json:"turns,omitempty"`
	LastKind   string          `json:"last_kind,omitempty"`
	Ended      bool            `json:"ended,omitempty"`
}

type entry struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	Branch    string `json:"gitBranch"`
	Timestamp string `json:"timestamp"`
	Sidechain bool   `json:"isSidechain"`
	Meta      bool   `json:"isMeta"`
	AITitle   string `json:"aiTitle"`
	Message   struct {
		Content json.RawMessage `json:"content"`
	} `json:"message"`
}

type block struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	ToolUseID string          `json:"tool_use_id"`
	IsError   bool            `json:"is_error"`
	Input     json.RawMessage `json:"input"`
	Content   json.RawMessage `json:"content"`
}

// field reads one tool input. Tools from other servers can use any shape for
// the same name, and that shouldn't cost the rest of the message.
func field[T any](input json.RawMessage, name string) T {
	var fields map[string]json.RawMessage
	var v T
	if json.Unmarshal(input, &fields) == nil {
		_ = json.Unmarshal(fields[name], &v)
	}
	return v
}

func parseContent(raw json.RawMessage) (string, []block) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "", nil
	}
	if raw[0] == '"' {
		var s string
		_ = json.Unmarshal(raw, &s)
		return s, nil
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return "", nil
	}
	blocks := make([]block, 0, len(items))
	for _, item := range items {
		var b block
		if json.Unmarshal(item, &b) == nil {
			blocks = append(blocks, b)
		}
	}
	var text []string
	for _, b := range blocks {
		if b.Type == "text" {
			text = append(text, b.Text)
		}
	}
	return strings.Join(text, "\n"), blocks
}

func harnessText(s string) bool {
	s = strings.TrimSpace(s)
	for _, p := range []string{"<task-notification", "<system-reminder", "<command-", "<local-command", "Caveat:", "[Request interrupted"} {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return s == ""
}

// Feed applies one transcript line. It returns false for lines it can't read;
// unknown entry types are fine and just ignored.
func (s *Session) Feed(line []byte) bool {
	var e entry
	if json.Unmarshal(line, &e) != nil {
		return false
	}
	if e.Sidechain {
		return true
	}
	if e.Type == "ai-title" && e.AITitle != "" {
		s.Title = Clip(Redact(e.AITitle), maxField)
		return true
	}
	if e.Type != "user" && e.Type != "assistant" {
		return true
	}
	s.Ended = false
	if s.ID == "" {
		s.ID = e.SessionID
	}
	if s.Cwd == "" {
		s.Cwd = e.Cwd
	}
	if e.Branch != "" {
		s.Branch = e.Branch
	}
	if t, err := time.Parse(time.RFC3339Nano, e.Timestamp); err == nil {
		ms := t.UnixMilli()
		if s.Started == 0 {
			s.Started = ms
		}
		s.Last = max(s.Last, ms)
	}
	text, blocks := parseContent(e.Message.Content)
	if e.Type == "user" {
		s.feedUser(text, blocks, e.Meta)
	} else {
		s.feedAssistant(blocks)
	}
	return true
}

func (s *Session) feedUser(text string, blocks []block, meta bool) {
	results := false
	for _, b := range blocks {
		if b.Type != "tool_result" {
			continue
		}
		results = true
		delete(s.Pending, b.ToolUseID)
		if b.IsError {
			s.Errors++
			out, _ := parseContent(b.Content)
			s.LastError = Clip(Redact(s.scrub(out)), maxField)
		}
	}
	if results {
		s.LastKind = "tool_result"
	}
	if meta || harnessText(text) {
		return
	}
	s.LastKind = "user"
	s.Turns++
	s.Prompt = Clip(Redact(s.scrub(text)), maxField)
}

func (s *Session) feedAssistant(blocks []block) {
	for _, b := range blocks {
		switch b.Type {
		case "text":
			s.LastKind = "assistant"
		case "tool_use":
			s.LastKind = "tool_use"
			s.recordTool(b)
		}
	}
}

func (s *Session) recordTool(b block) {
	detail := ""
	switch b.Name {
	case "Edit", "Write", "NotebookEdit", "MultiEdit":
		detail = field[string](b.Input, "file_path")
		if detail == "" {
			detail = field[string](b.Input, "notebook_path")
		}
		if detail != "" {
			s.Files = capLast(appendUnique(s.Files, Clip(detail, maxField)), maxFiles)
		}
	case "Bash":
		detail = Clip(Redact(s.scrub(field[string](b.Input, "command"))), maxField)
		s.Commands = capLast(append(s.Commands, detail), maxCommands)
		if field[bool](b.Input, "run_in_background") {
			s.Background = capLast(appendUnique(s.Background, detail), maxCommands)
		}
	}
	if b.ID == "" {
		return
	}
	if s.Pending == nil {
		s.Pending = map[string]Tool{}
	}
	if len(s.Pending) < maxPending {
		s.Pending[b.ID] = Tool{Name: b.Name, Detail: detail}
	}
}

func appendUnique(list []string, v string) []string {
	if i := slices.Index(list, v); i >= 0 {
		list = slices.Delete(list, i, i+1)
	}
	return append(list, v)
}

func capLast(list []string, n int) []string {
	if len(list) > n {
		return slices.Clone(list[len(list)-n:])
	}
	return list
}

func (s *Session) scrub(text string) string {
	if s.Cwd != "" {
		text = strings.ReplaceAll(text, s.Cwd+"/", "")
	}
	return text
}

// Portable rewrites paths so they mean something on a Mac with a different
// home directory or checkout location.
func (s Session) Portable(root, home string) Session {
	out := s
	out.Cwd = Portable(root, home, s.Cwd)
	out.Files = make([]string, len(s.Files))
	for i, f := range s.Files {
		out.Files[i] = Portable(root, home, f)
	}
	if s.Files == nil {
		out.Files = nil
	}
	if len(s.Pending) > 0 {
		out.Pending = make(map[string]Tool, len(s.Pending))
		for id, t := range s.Pending {
			if strings.HasPrefix(t.Detail, "/") {
				t.Detail = Portable(root, home, t.Detail)
			}
			out.Pending[id] = t
		}
	}
	return out
}

func Portable(root, home, p string) string {
	if p == "" || !filepath.IsAbs(p) {
		return p
	}
	if root != "" && (p == root || strings.HasPrefix(p, root+"/")) {
		rel, _ := filepath.Rel(root, p)
		return rel
	}
	if home != "" && (p == home || strings.HasPrefix(p, home+"/")) {
		return "~" + strings.TrimPrefix(p, home)
	}
	return p
}
