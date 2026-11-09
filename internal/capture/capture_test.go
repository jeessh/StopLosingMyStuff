package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

func line(t *testing.T, kind string, content any, extra map[string]any) string {
	t.Helper()
	m := map[string]any{
		"type": kind, "sessionId": "s1", "cwd": "/home/me/repo", "gitBranch": "main",
		"timestamp": "2026-11-09T10:00:00.000Z", "message": map[string]any{"content": content},
	}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b) + "\n"
}

func toolUse(id, name string, input map[string]any) []any {
	return []any{map[string]any{"type": "tool_use", "id": id, "name": name, "input": input}}
}

func toolResult(id string) []any {
	return []any{map[string]any{"type": "tool_result", "tool_use_id": id, "content": "ok"}}
}

func setup(t *testing.T) (*Watcher, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "proj")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return NewWatcher(root), filepath.Join(dir, "s1.jsonl")
}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

func scan(t *testing.T, w *Watcher) {
	t.Helper()
	if _, err := w.Scan(); err != nil {
		t.Fatal(err)
	}
}

func TestToolWithNoResultStaysPending(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "user", "fix the bug", nil))
	appendTo(t, path, line(t, "assistant", toolUse("a", "Edit", map[string]any{"file_path": "/home/me/repo/x.go"}), nil))
	appendTo(t, path, line(t, "user", toolResult("a"), nil))
	appendTo(t, path, line(t, "assistant", toolUse("b", "Bash", map[string]any{"command": "make migrate"}), nil))
	scan(t, w)
	s := w.Cursors[path].Session
	if len(s.Pending) != 1 || s.Pending["b"].Detail != "make migrate" {
		t.Fatalf("pending = %+v", s.Pending)
	}
	if s.Prompt != "fix the bug" || s.Turns != 1 || len(s.Files) != 1 {
		t.Fatalf("session = %+v", s)
	}
}

func TestHalfWrittenLineWaitsForTheRest(t *testing.T) {
	w, path := setup(t)
	full := line(t, "user", "hello", nil)
	appendTo(t, path, full[:len(full)/2])
	scan(t, w)
	if w.Cursors[path].Offset != 0 || w.Health.BadLines != 0 {
		t.Fatalf("consumed a partial line: %+v", w.Cursors[path])
	}
	appendTo(t, path, full[len(full)/2:])
	scan(t, w)
	if s := w.Cursors[path].Session; s == nil || s.Prompt != "hello" {
		t.Fatalf("session = %+v", s)
	}
}

func TestGarbageLineIsCountedNotFatal(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, "{not json\n"+line(t, "user", "still here", nil))
	scan(t, w)
	if w.Health.BadLines != 1 || w.Cursors[path].Session.Prompt != "still here" {
		t.Fatalf("health %+v session %+v", w.Health, w.Cursors[path].Session)
	}
}

func TestSubagentsAndHarnessTextAreIgnored(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "user", "real prompt", nil))
	appendTo(t, path, line(t, "user", "side quest", map[string]any{"isSidechain": true}))
	appendTo(t, path, line(t, "user", "<task-notification>x</task-notification>", nil))
	appendTo(t, path, line(t, "user", "meta", map[string]any{"isMeta": true}))
	scan(t, w)
	if s := w.Cursors[path].Session; s.Prompt != "real prompt" || s.Turns != 1 {
		t.Fatalf("session = %+v", s)
	}
}

func TestReplacedFileIsReadFromTheStart(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "user", "first version of the file, quite long", nil))
	scan(t, w)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	appendTo(t, path, line(t, "user", "second", nil))
	scan(t, w)
	if s := w.Cursors[path].Session; s.Prompt != "second" || s.Turns != 1 {
		t.Fatalf("session = %+v", s)
	}
}

func TestCursorSurvivesRestartWithoutRereading(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "user", "one", nil))
	scan(t, w)
	state := filepath.Join(t.TempDir(), "cursors.json")
	if err := w.Save(vfs.OS{}, state); err != nil {
		t.Fatal(err)
	}
	w2 := NewWatcher(w.Root)
	if err := w2.Load(vfs.OS{}, state); err != nil {
		t.Fatal(err)
	}
	appendTo(t, path, line(t, "user", "two", nil))
	scan(t, w2)
	if s := w2.Cursors[path].Session; s.Turns != 2 || s.Prompt != "two" {
		t.Fatalf("session = %+v", s)
	}
}

func TestOldTranscriptsAreSkippedUntilTheyChange(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "user", "old work", nil))
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	scan(t, w)
	if w.Cursors[path].Session != nil {
		t.Fatal("backfilled an old transcript")
	}
	appendTo(t, path, line(t, "user", "resumed", nil))
	scan(t, w)
	if s := w.Cursors[path].Session; s == nil || s.Turns != 2 {
		t.Fatalf("session = %+v", s)
	}
}

func TestOversizedLineIsSkipped(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, strings.Repeat("x", chunkSize+1024)+"\n"+line(t, "user", "after", nil))
	for range 3 {
		scan(t, w)
	}
	if s := w.Cursors[path].Session; s == nil || s.Prompt != "after" {
		t.Fatalf("session = %+v", s)
	}
}

func TestPortablePaths(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/Users/a/code/x/main.go", "main.go"},
		{"/Users/a/code/x", "."},
		{"/Users/a/code/xy/main.go", "~/code/xy/main.go"},
		{"/etc/hosts", "/etc/hosts"},
		{"relative.go", "relative.go"},
	}
	for _, c := range cases {
		if got := Portable("/Users/a/code/x", "/Users/a", c.in); got != c.want {
			t.Errorf("Portable(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestRedact(t *testing.T) {
	secrets := []string{
		"ghp_" + strings.Repeat("a", 36),
		"sk-ant-" + strings.Repeat("b", 40),
		"xoxb-" + strings.Repeat("1", 20),
		"AKIA" + strings.Repeat("Z", 16),
		"eyJhbGciOiJIUzI1.eyJzdWIiOiIxMjM0.SflKxwRJSMeKKF2QT4",
	}
	for _, s := range secrets {
		if out := Redact("value " + s + " end"); strings.Contains(out, s) {
			t.Errorf("leaked %q in %q", s, out)
		}
	}
	for _, in := range []string{
		"export API_KEY=hunter2hunter2", `password: "correct horse"`, "DB_TOKEN='abc def'",
		"curl -H 'Authorization: Bearer abcdefghijk'", "postgres://me:hunter2@db/x",
		"-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----",
	} {
		if out := Redact(in); strings.Contains(out, "hunter2") || strings.Contains(out, "horse") ||
			strings.Contains(out, "abc") {
			t.Errorf("leaked in %q", out)
		}
	}
	if got := Redact("go test ./... && git status"); got != "go test ./... && git status" {
		t.Errorf("changed innocent text: %q", got)
	}
}

func TestDigestKeepsNewestAndRedacts(t *testing.T) {
	w, path := setup(t)
	_ = w
	for i := range 50 {
		appendTo(t, path, line(t, "user", fmt.Sprintf("message %d %s", i, strings.Repeat("y", 300)), nil))
	}
	appendTo(t, path, line(t, "assistant", []any{map[string]any{"type": "text", "text": "key is sk-ant-" + strings.Repeat("c", 40)}}, nil))
	got, err := Digest(path, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "message 0 ") || !strings.Contains(got, "message 49") {
		t.Fatalf("wrong window: %.200s", got)
	}
	if strings.Contains(got, "ccccccccc") {
		t.Fatal("digest leaked a secret")
	}
}

func TestRedactCommandLineForms(t *testing.T) {
	for _, in := range []string{
		"mysql -u root --password hunter2xyz db",
		"mysql -uroot -phunter2xyz db",
		"sshpass -p hunter2xyz ssh box",
		"curl -u admin:hunter2xyz https://x",
		`curl -d "{\"password\":\"hunter2xyz\"}" https://x`,
		"STRIPE_KEY=hunter2xyz make",
		"pay sk_live_hunter2xyzabc",
		"auth npm_hunter2xyzhunter2xyzabc",
		"push glpat-hunter2xyzabc",
	} {
		if out := Redact(in); strings.Contains(out, "hunter2") {
			t.Errorf("leaked: %q", out)
		}
	}
	for _, in := range []string{"mkdir -p src/app", "git log -p -3", "ssh -p 2222 box", "curl -u admin https://x"} {
		if out := Redact(in); out != in {
			t.Errorf("changed innocent text: %q to %q", in, out)
		}
	}
}

func TestOddToolInputKeepsTheRestOfTheMessage(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "assistant", []any{
		map[string]any{"type": "tool_use", "id": "a", "name": "Edit", "input": map[string]any{"file_path": "/x/a.go"}},
		map[string]any{"type": "tool_use", "id": "b", "name": "mcp__thing", "input": map[string]any{"command": []string{"not", "a", "string"}}},
		map[string]any{"type": "text", "text": 42},
	}, nil))
	scan(t, w)
	s := w.Cursors[path].Session
	if len(s.Files) != 1 || len(s.Pending) != 2 {
		t.Fatalf("session = %+v", s)
	}
}

func TestInterruptMarkerIsNotAPrompt(t *testing.T) {
	w, path := setup(t)
	appendTo(t, path, line(t, "user", "real work", nil))
	appendTo(t, path, line(t, "user", []any{map[string]any{"type": "text", "text": "[Request interrupted by user]"}}, nil))
	scan(t, w)
	if s := w.Cursors[path].Session; s.Prompt != "real work" || s.Turns != 1 {
		t.Fatalf("session = %+v", s)
	}
}
