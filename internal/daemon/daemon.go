package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/app"
	"github.com/jeessh/StopLosingMyStuff/internal/brief"
	"github.com/jeessh/StopLosingMyStuff/internal/capture"
	"github.com/jeessh/StopLosingMyStuff/internal/project"
	"github.com/jeessh/StopLosingMyStuff/internal/replica"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
	"github.com/jeessh/StopLosingMyStuff/internal/summary"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const (
	captureEvery   = 2 * time.Second
	emitEvery      = time.Minute
	repoEvery      = time.Minute
	serviceEvery   = 30 * time.Second
	syncEvery      = 20 * time.Second
	compactEvery   = time.Hour
	syncDebounce   = time.Second
	resolveFor     = 5 * time.Minute
	resolveRetry   = time.Minute
	freshness      = time.Hour
	summaryIdle    = 10 * time.Minute
	summaryMaxAge  = 24 * time.Hour
	summaryRetry   = 30 * time.Minute
	summaryGap     = 90 * time.Second
	summaryTries   = 5
	summaryDigest  = 24 << 10
	maxRecordBytes = 48 << 10
)

var (
	ErrLocked = errors.New("another daemon holds the data directory")
	errRetry  = errors.New("project not known yet")
)

type Config struct {
	Dir        string
	Home       string
	ClaudeRoot string
	Listen     string
	Name       string
	Summarizer summary.Summarizer
	Log        *log.Logger
}

// SocketPath is where the CLI and hooks reach the daemon.
func SocketPath(dir string) string { return filepath.Join(dir, "sock") }

// Lock takes the data directory for one writer. The kernel drops it if the
// process dies, so a crash can't leave a stale lock behind.
func Lock(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, ErrLocked
	}
	return f, nil
}

type peerState struct {
	lastOK time.Time
	err    string
	behind uint64
}

type signal struct {
	session string
	event   string
}

type summaryDone struct {
	cursor  *capture.Cursor
	asOf    int64
	info    project.Info
	result  summary.Result
	err     error
	session string
}

type resolved struct {
	info project.Info
	at   time.Time
}

type daemon struct {
	cfg      Config
	fs       vfs.FS
	store    *store.Store
	app      *app.App
	id       replica.Identity
	watcher  *capture.Watcher
	log      *log.Logger
	wake     chan struct{}
	sync     chan struct{}
	signals  chan signal
	done     chan summaryDone
	fatal    chan error
	mu       sync.Mutex
	peers    map[string]peerState
	health   capture.Health
	resolves map[string]resolved
	busy     bool
	stuck    int
	lastCall time.Time
}

func Run(ctx context.Context, cfg Config) error {
	lock, err := Lock(cfg.Dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	if cfg.Log == nil {
		cfg.Log = log.New(os.Stderr, "", log.LstdFlags)
	}
	d := &daemon{
		cfg:      cfg,
		fs:       vfs.OS{},
		log:      cfg.Log,
		wake:     make(chan struct{}, 1),
		sync:     make(chan struct{}, 1),
		signals:  make(chan signal, 64),
		done:     make(chan summaryDone, 1),
		fatal:    make(chan error, 8),
		peers:    map[string]peerState{},
		resolves: map[string]resolved{},
		watcher:  capture.NewWatcher(cfg.ClaudeRoot),
	}
	st, err := store.Open(d.fs, cfg.Dir, store.Options{Name: cfg.Name, OnCommit: d.triggerSync})
	if err != nil {
		return err
	}
	defer st.Close()
	d.store = st
	if r := st.Recovery(); r.TornBytes > 0 || r.Quarantined > 0 || r.BadSnapshots > 0 {
		d.log.Printf("recovered log: %d torn bytes cut, %d bytes quarantined, %d snapshot copies damaged", r.TornBytes, r.Quarantined, r.BadSnapshots)
	}
	if d.id, err = replica.LoadIdentity(cfg.Dir); err != nil {
		return err
	}
	d.app = &app.App{Store: st, FS: d.fs, Dir: cfg.Dir, Home: cfg.Home, Now: time.Now, Extra: d.extra, Health: d.healthNow}
	if err := d.watcher.Load(d.fs, d.cursorPath()); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	sock, err := d.listenSocket()
	if err != nil {
		ln.Close()
		return err
	}
	defer os.Remove(SocketPath(cfg.Dir))
	d.log.Printf("listening on %s as %s", ln.Addr(), st.Name())

	var wg sync.WaitGroup
	run := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
		}()
	}
	run(func(ctx context.Context) {
		if err := replica.Serve(ctx, ln, d.local(), d.id, d.allowed); err != nil {
			d.fail(err)
		}
	})
	run(func(ctx context.Context) { d.serveSocket(ctx, sock) })
	run(d.captureLoop)
	run(d.repoLoop)
	run(d.serviceLoop)
	run(d.syncLoop)
	run(d.compactLoop)

	var result error
	select {
	case <-ctx.Done():
	case result = <-d.fatal:
		d.log.Printf("stopping: %v", result)
	}
	cancel()
	wg.Wait()
	if err := d.watcher.Save(d.fs, d.cursorPath()); err != nil && result == nil {
		result = err
	}
	return result
}

func (d *daemon) fail(err error) {
	select {
	case d.fatal <- err:
	default:
	}
}

func (d *daemon) cursorPath() string { return filepath.Join(d.cfg.Dir, "cursors.json") }

func (d *daemon) healthNow() capture.Health {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.health
}

func nudge(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func (d *daemon) triggerSync() { nudge(d.sync) }

func (d *daemon) local() replica.Local {
	return replica.Local{Store: d.store, OnHello: func(h replica.Hello) {
		ours := d.store.Vector()
		for origin, seq := range h.Vector {
			if seq > ours[origin] {
				d.triggerSync()
				return
			}
		}
	}}
}

func (d *daemon) allowed(fp string) bool {
	peers, err := app.LoadPeers(d.fs, d.cfg.Dir)
	if err != nil {
		return false
	}
	for _, p := range peers {
		if p.Fingerprint == fp {
			return true
		}
	}
	return false
}

func (d *daemon) listenSocket() (net.Listener, error) {
	path := SocketPath(d.cfg.Dir)
	if len(path) > 100 {
		return nil, fmt.Errorf("socket path too long for macOS: %s", path)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	return ln, os.Chmod(path, 0o600)
}

func (d *daemon) serveSocket(ctx context.Context, ln net.Listener) {
	stop := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stop()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go d.handle(ctx, c)
	}
}

func (d *daemon) handle(ctx context.Context, c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	line, err := bufio.NewReader(c).ReadBytes('\n')
	if err != nil {
		return
	}
	var req app.Request
	if json.Unmarshal(line, &req) != nil {
		return
	}
	var resp app.Response
	if req.Cmd == "signal" {
		select {
		case d.signals <- signal{session: req.Session, event: req.Key}:
		default:
		}
		nudge(d.wake)
	} else {
		resp = d.app.Do(ctx, req)
	}
	_ = json.NewEncoder(c).Encode(resp)
}

func (d *daemon) captureLoop(ctx context.Context) {
	tick := time.NewTicker(captureEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			d.pass(ctx, true)
			return
		case <-tick.C:
		case <-d.wake:
		case r := <-d.done:
			d.finishSummary(r)
			continue
		}
		d.pass(ctx, false)
	}
}

func (d *daemon) pass(ctx context.Context, final bool) {
	if _, err := d.watcher.Scan(); err != nil {
		d.log.Printf("scan: %v", err)
		return
	}
	d.mu.Lock()
	d.health = d.watcher.Health
	d.mu.Unlock()
	force := d.applySignals()
	now := time.Now()
	stuck := 0
	for path, c := range d.watcher.Cursors {
		if c.Session == nil || !c.Dirty {
			continue
		}
		if !final && !force[c.Session.ID] && now.UnixMilli()-c.LastEmit < emitEvery.Milliseconds() {
			continue
		}
		err := d.emit(context.WithoutCancel(ctx), c)
		if errors.Is(err, errRetry) {
			stuck++
			continue
		}
		if err != nil {
			d.log.Printf("emit %s: %v", path, err)
			d.fail(err)
			return
		}
		c.Dirty, c.LastEmit = false, now.UnixMilli()
		d.watcher.MarkChanged()
	}
	d.mu.Lock()
	d.stuck = stuck
	d.mu.Unlock()
	if err := d.watcher.Save(d.fs, d.cursorPath()); err != nil {
		d.log.Printf("save cursors: %v", err)
	}
	if !final {
		d.maybeSummarize(ctx, now)
	}
}

func (d *daemon) applySignals() map[string]bool {
	force := map[string]bool{}
	for {
		select {
		case s := <-d.signals:
			force[s.session] = true
			for _, c := range d.watcher.Cursors {
				if c.Session == nil || c.Session.ID != s.session {
					continue
				}
				switch s.event {
				case "SessionEnd":
					c.Session.Ended = true
				case "PreCompact":
					c.Compacting = true
				}
				c.Dirty = true
				d.watcher.MarkChanged()
			}
		default:
			return force
		}
	}
}

func (d *daemon) resolve(ctx context.Context, cwd string) project.Info {
	if r, ok := d.resolves[cwd]; ok {
		limit := resolveFor
		if r.info.Unsure {
			limit = resolveRetry
		}
		if time.Since(r.at) < limit {
			return r.info
		}
	}
	info := project.Resolve(ctx, cwd, d.cfg.Home)
	if info.Unsure {
		d.log.Printf("could not read git info for %s, will retry", cwd)
	}
	d.resolves[cwd] = resolved{info, time.Now()}
	return info
}

func (d *daemon) internal(cwd string) bool {
	return cwd == d.cfg.Dir || strings.HasPrefix(cwd, d.cfg.Dir+"/")
}

func (d *daemon) emit(ctx context.Context, c *capture.Cursor) error {
	s := c.Session
	if s.Cwd == "" || d.internal(s.Cwd) {
		return nil
	}
	info := d.resolve(ctx, s.Cwd)
	if info.Unsure {
		return errRetry
	}
	rec := brief.SessionRecord{
		Session:  bound(s.Portable(info.Root, d.cfg.Home)),
		Node:     d.store.NodeID(),
		NodeName: d.store.Name(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := d.app.RememberPath(info); err != nil {
		return err
	}
	return d.app.Put(info.ID, "session/"+s.ID, rec, func(old json.RawMessage) bool {
		return string(old) == string(raw)
	})
}

// bound keeps a session record under the event size limit whatever is in it.
func bound(s capture.Session) capture.Session {
	for {
		raw, err := json.Marshal(s)
		if err != nil || len(raw) < maxRecordBytes {
			return s
		}
		switch {
		case len(s.Files) > 0:
			s.Files = s.Files[len(s.Files)/2+1:]
		case len(s.Commands) > 0:
			s.Commands = s.Commands[len(s.Commands)/2+1:]
		case len(s.Background) > 0:
			s.Background = s.Background[len(s.Background)/2+1:]
		case len(s.Pending) > 0:
			s.Pending = nil
		default:
			s.Prompt, s.Title, s.LastError = "", "", ""
			return s
		}
	}
}

func eligible(c *capture.Cursor, now time.Time) bool {
	s := c.Session
	if s == nil || s.Turns < 2 || s.Last <= c.SummaryOf || now.UnixMilli() < c.SummaryNext {
		return false
	}
	if c.SummaryTry >= summaryTries && c.SummaryFail == s.Last {
		return false
	}
	age := now.Sub(time.UnixMilli(s.Last))
	return age < summaryMaxAge && (s.Ended || c.Compacting || age >= summaryIdle)
}

func (d *daemon) maybeSummarize(ctx context.Context, now time.Time) {
	if d.cfg.Summarizer == nil || d.busy || now.Sub(d.lastCall) < summaryGap {
		return
	}
	var pick *capture.Cursor
	paths := make([]string, 0, len(d.watcher.Cursors))
	for p := range d.watcher.Cursors {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		c := d.watcher.Cursors[p]
		if eligible(c, now) && !d.internal(c.Session.Cwd) && c.Session.Cwd != "" {
			if pick == nil || c.Session.Last > pick.Session.Last {
				pick = c
			}
		}
	}
	if pick == nil {
		return
	}
	d.busy, d.lastCall = true, now
	asOf, session, path := pick.Session.Last, pick.Session.ID, pick.Path
	info := d.resolve(ctx, pick.Session.Cwd)
	go func() {
		out := summaryDone{cursor: pick, asOf: asOf, info: info, session: session}
		digest, err := capture.Digest(path, summaryDigest)
		if err == nil {
			out.result, err = d.cfg.Summarizer.Summarize(ctx, digest)
		}
		out.err = err
		select {
		case d.done <- out:
		case <-ctx.Done():
		}
	}()
}

func (d *daemon) finishSummary(r summaryDone) {
	d.busy = false
	c := r.cursor
	d.watcher.MarkChanged()
	if r.err != nil {
		c.SummaryTry++
		c.SummaryFail = r.asOf
		c.SummaryNext = time.Now().Add(summaryRetry).UnixMilli()
		d.log.Printf("summary failed, session %s: %v", r.session, r.err)
		return
	}
	t := brief.Text{Node: d.store.Name(), AsOf: r.asOf, Session: r.session}
	for _, f := range []struct{ key, text string }{{"goal", r.result.Goal}, {"next", r.result.Next}} {
		v := t
		v.Text = f.text
		if err := d.app.PutNewer(r.info.ID, f.key, v); err != nil {
			d.fail(err)
			return
		}
	}
	if err := d.app.AddDecisions(r.info.ID, r.session, r.result.Decisions, t); err != nil {
		d.fail(err)
		return
	}
	c.SummaryOf, c.SummaryTry, c.SummaryNext, c.Compacting = r.asOf, 0, 0, false
}

func (d *daemon) repoLoop(ctx context.Context) {
	tick := time.NewTicker(repoEvery)
	defer tick.Stop()
	for {
		d.checkRepos(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (d *daemon) checkRepos(ctx context.Context) {
	type target struct{ id, root string }
	var targets []target
	key := "path/" + d.store.NodeID()
	d.store.View(func(s *state.State) {
		for id, p := range s.Projects {
			for _, w := range p.Get(key) {
				var t brief.Text
				if json.Unmarshal(w.Value, &t) == nil && t.Text != "" {
					targets = append(targets, target{id, t.Text})
				}
			}
		}
	})
	for _, t := range targets {
		if ctx.Err() != nil {
			return
		}
		st := project.Check(ctx, t.root)
		if st.Unknown {
			continue
		}
		rec := brief.RepoRecord{Status: st, Name: d.store.Name(), Checked: time.Now().UnixMilli()}
		err := d.app.Put(t.id, "repo/"+d.store.NodeID(), rec, func(old json.RawMessage) bool {
			var prev brief.RepoRecord
			return json.Unmarshal(old, &prev) == nil && prev.Status == st &&
				rec.Checked-prev.Checked < freshness.Milliseconds()
		})
		if err != nil {
			d.fail(err)
			return
		}
	}
}

func (d *daemon) serviceLoop(ctx context.Context) {
	tick := time.NewTicker(serviceEvery)
	defer tick.Stop()
	for {
		d.checkServices(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (d *daemon) checkServices(ctx context.Context) {
	list, err := app.LoadServices(d.fs, d.cfg.Dir)
	if err != nil {
		d.log.Printf("services: %v", err)
		return
	}
	for _, s := range list {
		dialer := net.Dialer{Timeout: time.Second}
		c, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", s.Port))
		if err == nil {
			c.Close()
		}
		rec := app.ServiceRecord{Service: s, Up: err == nil, Node: d.store.Name(), Checked: time.Now().UnixMilli()}
		key := "service/" + s.Name + "/" + d.store.NodeID()
		perr := d.app.Put(app.ServicesProject, key, rec, func(old json.RawMessage) bool {
			var prev app.ServiceRecord
			return json.Unmarshal(old, &prev) == nil && prev.Up == rec.Up && prev.Service == rec.Service &&
				rec.Checked-prev.Checked < freshness.Milliseconds()
		})
		if perr != nil {
			d.fail(perr)
			return
		}
	}
}

func (d *daemon) compactLoop(ctx context.Context) {
	tick := time.NewTicker(compactEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		ran, err := d.app.CompactIfDue()
		if err != nil {
			d.log.Printf("compact: %v", err)
		} else if ran {
			d.log.Printf("compacted the log")
		}
		if serr := d.store.Err(); serr != nil {
			d.fail(serr)
			return
		}
	}
}

func (d *daemon) syncLoop(ctx context.Context) {
	tick := time.NewTicker(syncEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-d.sync:
			select {
			case <-ctx.Done():
				return
			case <-time.After(syncDebounce):
			}
		}
		d.syncPeers(ctx)
	}
}

func (d *daemon) syncPeers(ctx context.Context) {
	peers, err := app.LoadPeers(d.fs, d.cfg.Dir)
	if err != nil {
		d.log.Printf("peers: %v", err)
		return
	}
	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.syncPeer(ctx, p)
		}()
	}
	wg.Wait()
}

func (d *daemon) syncPeer(ctx context.Context, p replica.PeerConfig) {
	them, err := d.pullFrom(ctx, p)
	if serr := d.store.Err(); serr != nil {
		d.fail(serr)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.peers[p.Name]
	if err != nil {
		if st.err != err.Error() {
			d.log.Printf("sync %s: %v", p.Name, err)
		}
		st.err = err.Error()
	} else {
		if st.err != "" {
			d.log.Printf("sync %s: connected again", p.Name)
		}
		st.err, st.lastOK = "", time.Now()
		st.behind = d.store.Missing(them.Vector)
		d.app.SawPeer(p.Name, them.Vector)
	}
	d.peers[p.Name] = st
}

func (d *daemon) pullFrom(ctx context.Context, p replica.PeerConfig) (replica.Hello, error) {
	c, err := replica.Dial(ctx, p, d.id)
	if err != nil {
		return replica.Hello{}, err
	}
	defer c.Close()
	them, _, err := replica.Pull(ctx, d.store, c)
	return them, err
}

func (d *daemon) extra() string {
	peers, _ := app.LoadPeers(d.fs, d.cfg.Dir)
	d.mu.Lock()
	defer d.mu.Unlock()
	var b strings.Builder
	if d.stuck > 0 {
		fmt.Fprintf(&b, "PROBLEM: %d sessions waiting because git could not read their folder, macOS may need to allow access for sls\n", d.stuck)
	}
	if len(peers) == 0 {
		b.WriteString("no peers paired yet\n")
	}
	for _, p := range peers {
		st := d.peers[p.Name]
		switch {
		case st.lastOK.IsZero() && st.err == "":
			fmt.Fprintf(&b, "peer %s: not tried yet\n", p.Name)
		case st.err != "":
			last := "never"
			if !st.lastOK.IsZero() {
				last = brief.Age(st.lastOK.UnixMilli(), time.Now())
			}
			fmt.Fprintf(&b, "peer %s: unreachable, last synced %s\n", p.Name, last)
		default:
			fmt.Fprintf(&b, "peer %s: synced %s, %d events waiting\n", p.Name, brief.Age(st.lastOK.UnixMilli(), time.Now()), st.behind)
		}
	}
	return b.String()
}
