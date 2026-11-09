package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const (
	chunkSize   = 8 << 20
	backfillAge = 14 * 24 * time.Hour
	coldAge     = 7 * 24 * time.Hour
)

// Cursor is how far one transcript has been read and what it said so far.
// Only whole lines are consumed, so a line still being written is read later.
type Cursor struct {
	Offset      int64    `json:"offset"`
	Inode       uint64   `json:"inode"`
	Skipping    bool     `json:"skipping,omitempty"`
	Cold        bool     `json:"cold,omitempty"`
	Session     *Session `json:"session,omitempty"`
	Dirty       bool     `json:"dirty,omitempty"`
	LastEmit    int64    `json:"last_emit,omitempty"`
	SummaryOf   int64    `json:"summary_of,omitempty"`
	SummaryNext int64    `json:"summary_next,omitempty"`
	SummaryTry  int      `json:"summary_try,omitempty"`
	SummaryFail int64    `json:"summary_fail,omitempty"`
	Compacting  bool     `json:"compacting,omitempty"`
	Path        string   `json:"path"`
}

type Health struct {
	Lines    int `json:"lines"`
	BadLines int `json:"bad_lines"`
	Sessions int `json:"sessions"`
}

type Watcher struct {
	Root    string
	Now     func() time.Time
	Cursors map[string]*Cursor
	Health  Health
	changed bool
}

func NewWatcher(root string) *Watcher {
	return &Watcher{Root: root, Now: time.Now, Cursors: map[string]*Cursor{}}
}

func (w *Watcher) Load(fsys vfs.FS, path string) error {
	data, err := fsys.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if json.Unmarshal(data, &w.Cursors) != nil || w.Cursors == nil {
		w.Cursors = map[string]*Cursor{}
	}
	return nil
}

func (w *Watcher) Save(fsys vfs.FS, path string) error {
	if !w.changed {
		return nil
	}
	data, err := json.Marshal(w.Cursors)
	if err != nil {
		return err
	}
	if err := vfs.WriteAtomic(fsys, path, data); err != nil {
		return err
	}
	w.changed = false
	return nil
}

func (w *Watcher) MarkChanged() { w.changed = true }

// Scan reads new lines from every transcript and returns the cursors that moved.
func (w *Watcher) Scan() ([]*Cursor, error) {
	files, err := filepath.Glob(filepath.Join(w.Root, "*", "*.jsonl"))
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(files))
	var moved []*Cursor
	now := w.Now()
	for _, path := range files {
		seen[path] = true
		fi, err := os.Stat(path)
		if err != nil {
			continue
		}
		c := w.Cursors[path]
		if c == nil {
			c = &Cursor{Path: path}
			w.Cursors[path] = c
			w.changed = true
			if now.Sub(fi.ModTime()) > backfillAge {
				c.Offset, c.Inode, c.Cold = fi.Size(), inode(fi), true
				continue
			}
		}
		if w.read(c, path, fi) {
			moved = append(moved, c)
		}
		if c.Session != nil && !c.Dirty && now.Sub(fi.ModTime()) > coldAge {
			c.Session, c.Cold = nil, true
			w.changed = true
		}
	}
	for path := range w.Cursors {
		if !seen[path] {
			delete(w.Cursors, path)
			w.changed = true
		}
	}
	w.Health.Sessions = len(w.Cursors)
	return moved, nil
}

func inode(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}

func (w *Watcher) read(c *Cursor, path string, fi os.FileInfo) bool {
	ino := inode(fi)
	if fi.Size() < c.Offset || (c.Inode != 0 && ino != c.Inode) {
		c.Offset, c.Skipping, c.Session, c.Cold = 0, false, nil, false
		w.changed = true
	}
	c.Inode = ino
	if fi.Size() == c.Offset {
		return false
	}
	if c.Cold {
		c.Offset, c.Session, c.Cold = 0, nil, false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	if _, err := f.Seek(c.Offset, io.SeekStart); err != nil {
		return false
	}
	buf := make([]byte, chunkSize)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return false
	}
	buf = buf[:n]
	if c.Skipping {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			c.Offset += int64(n)
			w.changed = true
			return false
		}
		c.Skipping = false
		c.Offset += int64(i + 1)
		buf = buf[i+1:]
	}
	end := bytes.LastIndexByte(buf, '\n')
	if end < 0 {
		if n == chunkSize {
			c.Offset += int64(n)
			c.Skipping = true
			w.changed = true
		}
		return false
	}
	if c.Session == nil {
		c.Session = &Session{}
	}
	for _, line := range bytes.Split(buf[:end], []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		w.Health.Lines++
		if !c.Session.Feed(line) {
			w.Health.BadLines++
		}
	}
	if c.Session.ID == "" {
		c.Session.ID = strings.TrimSuffix(filepath.Base(path), ".jsonl")
	}
	c.Offset += int64(end + 1)
	c.Dirty = true
	w.changed = true
	return true
}

// Digest is the recent conversation, for summarizing. Tool output stays out.
func Digest(path string, limit int) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}
	const window = 16 << 20
	if fi.Size() > window {
		if _, err := f.Seek(fi.Size()-window, io.SeekStart); err != nil {
			return "", err
		}
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	var out []string
	size := 0
	lines := bytes.Split(data, []byte{'\n'})
	for i := len(lines) - 1; i >= 0 && size < limit; i-- {
		var e entry
		if json.Unmarshal(lines[i], &e) != nil || e.Sidechain || e.Meta || (e.Type != "user" && e.Type != "assistant") {
			continue
		}
		text, _ := parseContent(e.Message.Content)
		if harnessText(text) {
			continue
		}
		part := e.Type + ": " + Clip(Redact(text), 2000)
		out = append(out, part)
		size += len(part)
	}
	var b strings.Builder
	for i := len(out) - 1; i >= 0; i-- {
		b.WriteString(out[i])
		b.WriteString("\n\n")
	}
	return b.String(), nil
}
