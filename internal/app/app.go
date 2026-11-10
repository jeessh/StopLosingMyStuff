package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/brief"
	"github.com/jeessh/StopLosingMyStuff/internal/capture"
	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
	"github.com/jeessh/StopLosingMyStuff/internal/project"
	"github.com/jeessh/StopLosingMyStuff/internal/replica"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const (
	ServicesProject = "machine:services"
	maxStatusRows   = 25
	briefGitLimit   = 1500 * time.Millisecond
)

type Request struct {
	Cmd     string `json:"cmd"`
	Dir     string `json:"dir,omitempty"`
	Text    string `json:"text,omitempty"`
	Key     string `json:"key,omitempty"`
	Session string `json:"session,omitempty"`
}

type Response struct {
	Out string `json:"out,omitempty"`
	Err string `json:"err,omitempty"`
}

type Service struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}

type ServiceRecord struct {
	Service
	Up      bool   `json:"up"`
	Node    string `json:"node"`
	Checked int64  `json:"checked"`
}

type App struct {
	Store  *store.Store
	FS     vfs.FS
	Dir    string
	Home   string
	Now    func() time.Time
	Extra  func() string
	Health func() capture.Health
	mu     sync.Mutex
	acks   map[string]peerAck
}

func (a *App) Do(ctx context.Context, r Request) Response {
	var out string
	var err error
	switch r.Cmd {
	case "brief":
		out = a.Brief(ctx, r.Dir)
	case "decisions":
		out = a.Decisions(ctx, r.Dir)
	case "status":
		out = a.Status()
	case "doctor":
		out = a.Doctor()
	case "note":
		err = a.Note(ctx, r.Dir, r.Text)
	case "goal", "next":
		err = a.SetText(ctx, r.Dir, r.Cmd, r.Text)
	case "track":
		err = a.Track(ctx, r.Dir)
	case "compact":
		out, err = a.Compact()
	default:
		err = fmt.Errorf("unknown command %q", r.Cmd)
	}
	if err != nil {
		return Response{Err: err.Error()}
	}
	return Response{Out: out}
}

func (a *App) Brief(ctx context.Context, dir string) string {
	info := project.Resolve(ctx, dir, a.Home)
	quick, cancel := context.WithTimeout(ctx, briefGitLimit)
	defer cancel()
	local := project.Check(quick, info.Root)
	var out string
	a.Store.View(func(s *state.State) {
		out = brief.Render(s.Projects[info.ID], a.Store.NodeID(), local, a.Now())
	})
	return out
}

func (a *App) Decisions(ctx context.Context, dir string) string {
	info := project.Resolve(ctx, dir, a.Home)
	var b strings.Builder
	a.Store.View(func(s *state.State) {
		for _, w := range s.Projects[info.ID].Items("decision") {
			var t brief.Text
			if json.Unmarshal(w.Value, &t) == nil && t.Text != "" {
				fmt.Fprintf(&b, "%s  %s\n", time.UnixMilli(w.Time.Wall).Format("2006-01-02 15:04"), t.Text)
			}
		}
	})
	return b.String()
}

func (a *App) text(s string) brief.Text {
	return brief.Text{Text: s, Node: a.Store.Name(), AsOf: a.Now().UnixMilli()}
}

func (a *App) Note(ctx context.Context, dir, text string) error {
	text = strings.TrimSpace(capture.Redact(text))
	if text == "" {
		return errors.New("empty note")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	info := project.Resolve(ctx, dir, a.Home)
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d\x00%s", a.Store.NodeID(), a.Now().UnixNano(), text))
	body, err := store.Add(info.ID, "decision", hex.EncodeToString(sum[:8]), a.text(capture.Clip(text, 2000)))
	if err != nil {
		return err
	}
	_, err = a.Store.Commit(body)
	return err
}

// SetText replaces every visible value, which also settles any conflict.
func (a *App) SetText(ctx context.Context, dir, key, text string) error {
	text = strings.TrimSpace(capture.Redact(text))
	if text == "" {
		return errors.New("empty text")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	info := project.Resolve(ctx, dir, a.Home)
	body, err := store.Set(info.ID, key, a.text(capture.Clip(text, 2000)), a.visible(info.ID, key))
	if err != nil {
		return err
	}
	_, err = a.Store.Commit(body)
	return err
}

func (a *App) visible(id, key string) []state.Write {
	var out []state.Write
	a.Store.View(func(s *state.State) { out = s.Projects[id].Get(key) })
	return out
}

func (a *App) Track(ctx context.Context, dir string) error {
	if _, err := project.Track(dir); err != nil {
		return err
	}
	return a.RememberPath(project.Resolve(ctx, dir, a.Home))
}

func (a *App) RememberPath(info project.Info) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := "path/" + a.Store.NodeID()
	cur := a.visible(info.ID, key)
	if len(cur) == 1 {
		var t brief.Text
		if json.Unmarshal(cur[0].Value, &t) == nil && t.Text == info.Root {
			return nil
		}
	}
	body, err := store.Set(info.ID, key, brief.Text{Text: info.Root, Node: a.Store.Name()}, cur)
	if err != nil {
		return err
	}
	_, err = a.Store.Commit(body)
	return err
}

// Put writes a value unless it is already the only visible one.
func (a *App) Put(projectID, key string, value any, same func(json.RawMessage) bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := a.visible(projectID, key)
	if len(cur) == 1 && same != nil && same(cur[0].Value) {
		return nil
	}
	body, err := store.Set(projectID, key, value, cur)
	if err != nil {
		return err
	}
	_, err = a.Store.Commit(body)
	return err
}

// PutNewer writes a summary field unless a newer session already set it.
func (a *App) PutNewer(projectID, key string, t brief.Text) error {
	if t.Text == "" {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := a.visible(projectID, key)
	limit := a.Now().UnixMilli() + hlc.MaxDrift
	for _, w := range cur {
		var old brief.Text
		if json.Unmarshal(w.Value, &old) == nil && old.AsOf > t.AsOf && old.AsOf <= limit {
			return nil
		}
	}
	body, err := store.Set(projectID, key, t, ownWrites(cur, t.Node))
	if err != nil {
		return err
	}
	_, err = a.Store.Commit(body)
	return err
}

// ownWrites picks what a summary may replace. An open conflict belongs to the
// person, so a summary only updates this machine's side of it.
func ownWrites(cur []state.Write, node string) []state.Write {
	if len(cur) < 2 {
		return cur
	}
	var own []state.Write
	for _, w := range cur {
		var t brief.Text
		if json.Unmarshal(w.Value, &t) == nil && t.Node == node {
			own = append(own, w)
		}
	}
	return own
}

func (a *App) AddDecisions(projectID, session string, list []string, t brief.Text) error {
	if len(list) == 0 {
		return nil
	}
	bodies := make([]event.Body, 0, len(list))
	for _, d := range list {
		sum := sha256.Sum256([]byte(session + "\x00" + d))
		v := t
		v.Text = d
		b, err := store.Add(projectID, "decision", hex.EncodeToString(sum[:8]), v)
		if err != nil {
			return err
		}
		bodies = append(bodies, b)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.Store.Commit(bodies...)
	return err
}

type row struct {
	id   string
	last int64
	out  string
}

func (a *App) Status() string {
	now := a.Now()
	var rows []row
	var services []string
	a.Store.View(func(s *state.State) {
		for id, p := range s.Projects {
			if id == ServicesProject {
				services = renderServices(p, now)
				continue
			}
			rows = append(rows, renderProject(id, p, now))
		}
	})
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].last != rows[j].last {
			return rows[i].last > rows[j].last
		}
		return rows[i].id < rows[j].id
	})
	var b strings.Builder
	if len(rows) == 0 {
		b.WriteString("nothing captured yet\n")
	}
	for i, r := range rows {
		if i == maxStatusRows {
			fmt.Fprintf(&b, "...and %d older projects\n", len(rows)-i)
			break
		}
		b.WriteString(r.out)
	}
	if len(services) > 0 {
		b.WriteString("services\n")
		for _, l := range services {
			b.WriteString("  " + l + "\n")
		}
	}
	if a.Extra != nil {
		b.WriteString(a.Extra())
	} else {
		b.WriteString("daemon not running, showing local data only\n")
	}
	return b.String()
}

func renderProject(id string, p *state.Project, now time.Time) row {
	var b strings.Builder
	b.WriteString(id + "\n")
	sessions := brief.Sessions(p)
	var last int64
	if len(sessions) > 0 {
		s := sessions[0]
		last = s.Last
		fmt.Fprintf(&b, "  last session: %s on %s (%s)\n", brief.Age(s.Last, now), s.NodeName, brief.Condition(s.Session, now))
	}
	writeRegister(&b, "next", p.Get("next"))
	for _, r := range brief.Repos(p) {
		fmt.Fprintf(&b, "  %s: %s\n", r.Name, brief.RepoLine(r.Status))
		last = max(last, r.Newest)
	}
	return row{id: id, last: last, out: b.String()}
}

func writeRegister(b *strings.Builder, label string, ws []state.Write) {
	for _, w := range ws {
		var t brief.Text
		if json.Unmarshal(w.Value, &t) != nil || t.Text == "" {
			continue
		}
		if len(ws) > 1 {
			fmt.Fprintf(b, "  %s [conflict, %s]: %s\n", label, brief.OneLine(t.Node), brief.OneLine(t.Text))
		} else {
			fmt.Fprintf(b, "  %s: %s\n", label, brief.OneLine(t.Text))
		}
	}
}

func renderServices(p *state.Project, now time.Time) []string {
	var out []string
	for _, key := range p.Keys("service/") {
		for _, w := range p.Get(key) {
			var r ServiceRecord
			if json.Unmarshal(w.Value, &r) != nil {
				continue
			}
			state := "down"
			if r.Up {
				state = "up"
			}
			out = append(out, fmt.Sprintf("%s on %s port %d: %s, checked %s", r.Name, r.Node, r.Port, state, brief.Age(r.Checked, now)))
		}
	}
	return out
}

func (a *App) Doctor() string {
	var b strings.Builder
	fmt.Fprintf(&b, "node: %s (%s)\n", a.Store.Name(), a.Store.NodeID())
	v := a.Store.Vector()
	var total uint64
	for _, n := range v {
		total += n
	}
	fmt.Fprintf(&b, "events: %d from %d writers\n", total, len(v))
	size, files := a.Store.LogSize()
	fmt.Fprintf(&b, "log: %s in %d files\n", mb(size), files)
	if r := a.Store.Recovery(); r.TornBytes > 0 || r.Quarantined > 0 {
		fmt.Fprintf(&b, "last start: cut %d torn bytes and quarantined %d bytes, all saved as .bad files in %s\n",
			r.TornBytes, r.Quarantined, filepath.Join(a.Dir, "log"))
	}
	if r := a.Store.Recovery(); r.BadSnapshots > 0 {
		fmt.Fprintf(&b, "last start: found %d damaged snapshot copies\n", r.BadSnapshots)
	}
	for origin, why := range a.Store.Forks() {
		fmt.Fprintf(&b, "PROBLEM: stopped accepting writer %s: %s\n", origin, why)
	}
	if a.Health != nil {
		h := a.Health()
		fmt.Fprintf(&b, "transcripts: %d sessions, %d lines read, %d unreadable\n", h.Sessions, h.Lines, h.BadLines)
		if h.Lines > 100 && h.BadLines*5 > h.Lines {
			b.WriteString("PROBLEM: many transcript lines are unreadable, Claude Code's format may have changed\n")
		}
	}
	if a.Extra != nil {
		b.WriteString(a.Extra())
	} else {
		b.WriteString("PROBLEM: daemon is not running\n")
	}
	settings, _ := a.FS.ReadFile(filepath.Join(a.Home, ".claude", "settings.json"))
	if !strings.Contains(string(settings), " hook SessionStart") {
		b.WriteString("PROBLEM: Claude Code hooks are not installed, run slms install\n")
	}
	return b.String()
}

func readJSON[T any](fsys vfs.FS, path string) (T, error) {
	var v T
	data, err := fsys.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	return v, json.Unmarshal(data, &v)
}

func writeJSON(fsys vfs.FS, path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return vfs.WriteAtomic(fsys, path, data)
}

func LoadPeers(fsys vfs.FS, dir string) ([]replica.PeerConfig, error) {
	return readJSON[[]replica.PeerConfig](fsys, filepath.Join(dir, "peers.json"))
}

func SavePeers(fsys vfs.FS, dir string, peers []replica.PeerConfig) error {
	return writeJSON(fsys, filepath.Join(dir, "peers.json"), peers)
}

func LoadServices(fsys vfs.FS, dir string) ([]Service, error) {
	return readJSON[[]Service](fsys, filepath.Join(dir, "services.json"))
}

func SaveServices(fsys vfs.FS, dir string, list []Service) error {
	return writeJSON(fsys, filepath.Join(dir, "services.json"), list)
}
