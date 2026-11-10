package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/replica"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const home = "/data"

var errLink = errors.New("link down")

type rng struct{ r *rand.Rand }

func (g rng) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(g.r.UintN(256))
	}
	return len(p), nil
}

type machine struct {
	name   string
	fs     *vfs.MemFS
	s      *store.Store
	skew   int64
	backup *vfs.MemFS
}

type world struct {
	t   *testing.T
	r   *rand.Rand
	now int64
	m   []*machine
	all *state.State
}

func (w *world) open(m *machine) {
	w.t.Helper()
	if m.s != nil {
		_ = m.s.Close()
	}
	s, err := store.Open(m.fs, home, store.Options{
		Name:        m.name,
		Now:         func() int64 { return w.now + m.skew },
		Rand:        rng{w.r},
		SegmentSize: 2048,
	})
	if err != nil {
		w.t.Fatalf("%s: reopen: %v", m.name, err)
	}
	m.s = s
}

// link fails or short-delivers at random, like a laptop leaving the network.
type link struct {
	replica.Local
	r    *rand.Rand
	loss int
}

func (l link) Hello(ctx context.Context, h replica.Hello) (replica.Hello, error) {
	if l.r.IntN(100) < l.loss {
		return replica.Hello{}, errLink
	}
	return l.Local.Hello(ctx, h)
}

func (l link) Snapshot(ctx context.Context, id string, offset int64) (replica.Chunk, error) {
	if l.r.IntN(100) < l.loss/4 {
		return replica.Chunk{}, errLink
	}
	return l.Local.Snapshot(ctx, id, offset)
}

func (l link) Pull(ctx context.Context, have store.Vector) (replica.Batch, error) {
	if l.r.IntN(100) < l.loss {
		return replica.Batch{}, errLink
	}
	b, err := l.Local.Pull(ctx, have)
	if err != nil {
		return b, err
	}
	if len(b.Events) > 1 && l.r.IntN(4) == 0 {
		b.Events = b.Events[:1+l.r.IntN(len(b.Events)-1)]
		b.More = true
	}
	if len(b.Events) > 0 && l.r.IntN(4) == 0 {
		b.Events = append(b.Events, b.Events[l.r.IntN(len(b.Events))])
	}
	return b, nil
}

type config struct {
	machines int
	steps    int
	destroy  bool
	compact  bool
}

func newWorld(t *testing.T, seed uint64, cfg config) *world {
	w := &world{t: t, r: rand.New(rand.NewPCG(seed, 77)), now: 1_000_000, all: state.New()}
	for i := range cfg.machines {
		m := &machine{name: fmt.Sprintf("mac%d", i), fs: vfs.NewMem()}
		w.open(m)
		w.m = append(w.m, m)
	}
	return w
}

func (w *world) commit(m *machine) []event.ID {
	project := fmt.Sprintf("p%d", w.r.IntN(3))
	var body event.Body
	var err error
	switch w.r.IntN(4) {
	case 0, 1:
		key := fmt.Sprintf("k%d", w.r.IntN(3))
		var over []state.Write
		m.s.View(func(st *state.State) { over = st.Projects[project].Get(key) })
		body, err = store.Set(project, key, w.r.Uint32(), over)
	case 2:
		body, err = store.Add(project, "notes", fmt.Sprintf("e%d", w.r.IntN(40)), w.r.Uint32())
	default:
		body = event.Body{Project: project, Op: event.OpDel, Key: "notes", Elem: fmt.Sprintf("e%d", w.r.IntN(40))}
	}
	if err != nil {
		w.t.Fatal(err)
	}
	ids, err := m.s.Commit(body)
	if err != nil {
		w.t.Fatalf("commit: %v", err)
	}
	have := m.s.Vector()
	have[ids[0].Origin]--
	events, _, err := m.s.Since(have, 1, 1<<20)
	if err != nil || len(events) != 1 {
		w.t.Fatalf("read back commit: %d events, %v", len(events), err)
	}
	w.all.Apply(events[0])
	return ids
}

// guarded runs fn, which may hit a planned crash, and reports if it did.
func guarded(fn func()) (crashed bool) {
	defer func() {
		if v := recover(); v != nil {
			if v != vfs.ErrCrash {
				panic(v)
			}
			crashed = true
		}
	}()
	fn()
	return false
}

func (w *world) settle(m *machine, crashed bool) {
	m.fs.CrashAfter(0)
	m.fs.FailNextSyncs(0)
	m.fs.FailNextWrites(0)
	if crashed || m.s.Err() != nil {
		m.fs.Crash(w.r)
		w.open(m)
	}
}

// compact may wait for every peer, wait for nobody, or be cut off partway.
func (w *world) compact(m *machine) {
	var wait []store.Vector
	if w.r.IntN(2) == 0 {
		for _, other := range w.m {
			if other != m {
				wait = append(wait, other.s.Vector())
			}
		}
	}
	fault := w.r.IntN(4)
	crashed := guarded(func() {
		switch fault {
		case 0:
			m.fs.CrashAfter(1 + w.r.IntN(16))
		case 1:
			m.fs.FailNextSyncs(1 + w.r.IntN(2))
		case 2:
			m.fs.FailNextWrites(1)
		}
		if _, err := m.s.Compact(wait); err != nil && fault == 3 {
			w.t.Fatalf("compact: %v", err)
		}
	})
	w.settle(m, crashed)
}

// crashingPull cuts the power at some point during a pull or snapshot install.
func (w *world) crashingPull(a, b *machine) {
	crashed := guarded(func() {
		a.fs.CrashAfter(1 + w.r.IntN(12))
		_, _, _ = replica.Pull(context.Background(), a.s, replica.Local{Store: b.s})
	})
	w.settle(a, crashed)
}

func (w *world) pair() (*machine, *machine) {
	a := w.r.IntN(len(w.m))
	b := (a + 1 + w.r.IntN(len(w.m)-1)) % len(w.m)
	return w.m[a], w.m[b]
}

func (w *world) heal() {
	w.t.Helper()
	ctx := context.Background()
	for round := 0; ; round++ {
		if round > 20 {
			w.t.Fatal("replication did not settle")
		}
		moved := 0
		for _, a := range w.m {
			for _, b := range w.m {
				if a == b {
					continue
				}
				_, n, err := replica.Pull(ctx, a.s, replica.Local{Store: b.s})
				if err != nil {
					w.t.Fatalf("%s pull %s: %v", a.name, b.name, err)
				}
				moved += n
			}
		}
		if moved == 0 {
			return
		}
	}
}

func (w *world) assertConverged(seed uint64) {
	w.t.Helper()
	first := w.m[0]
	for _, m := range w.m {
		if len(m.s.Forks()) != 0 {
			w.t.Fatalf("seed %d: %s reports forks: %v", seed, m.name, m.s.Forks())
		}
		if m.s.Missing(first.s.Vector()) != 0 || first.s.Missing(m.s.Vector()) != 0 {
			w.t.Fatalf("seed %d: vectors differ", seed)
		}
		if m.s.Hash() != first.s.Hash() {
			w.t.Fatalf("seed %d: %s state differs from %s", seed, m.name, first.name)
		}
	}
}

func run(t *testing.T, seed uint64, cfg config) {
	w := newWorld(t, seed, cfg)
	ctx := context.Background()
	acked := map[event.ID]bool{}
	loss := w.r.IntN(60)
	for range cfg.steps {
		w.now += int64(w.r.IntN(2000))
		m := w.m[w.r.IntN(len(w.m))]
		switch roll := w.r.IntN(100); {
		case cfg.compact && roll < 8:
			w.compact(m)
		case cfg.compact && roll < 12:
			a, b := w.pair()
			w.crashingPull(a, b)
		case roll < 50:
			for _, id := range w.commit(m) {
				acked[id] = true
			}
		case roll < 75:
			a, b := w.pair()
			_, _, err := replica.Pull(ctx, a.s, link{replica.Local{Store: b.s}, w.r, loss})
			if err != nil && !errors.Is(err, errLink) {
				t.Fatalf("seed %d: pull: %v", seed, err)
			}
		case roll < 85:
			m.fs.Crash(w.r)
			w.open(m)
		case roll < 90:
			m.skew = int64(w.r.IntN(1<<40)) - 1<<39
		case cfg.destroy && roll < 94:
			m.backup = m.fs.Clone()
		case cfg.destroy && roll < 97 && m.backup != nil:
			m.fs = m.backup.Clone()
			w.open(m)
		case cfg.destroy:
			suffix := ".seg"
			if cfg.compact && w.r.IntN(2) == 0 {
				suffix = ".snap"
			}
			m.fs.Rot(w.r, suffix)
			m.fs.Crash(w.r)
			w.open(m)
		}
	}
	for _, m := range w.m {
		m.fs.Crash(w.r)
		w.open(m)
	}
	w.heal()
	w.assertConverged(seed)
	if cfg.destroy {
		return
	}
	v := w.m[0].s.Vector()
	for id := range acked {
		if v[id.Origin] < id.Seq {
			t.Fatalf("seed %d: acknowledged event %s was lost", seed, id)
		}
	}
	if w.m[0].s.Hash() != w.all.Hash() {
		t.Fatalf("seed %d: final state differs from replaying every event", seed)
	}
}

func seeds(t *testing.T, fallback uint64) uint64 {
	if s := os.Getenv("SLS_SIM_SEEDS"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if testing.Short() {
		return 50
	}
	return fallback
}

func TestNoAcknowledgedEventIsLost(t *testing.T) {
	for seed := range seeds(t, 400) {
		run(t, seed, config{machines: 2 + int(seed%2), steps: 300})
	}
}

func TestConvergesAfterBackupRestoreAndBitRot(t *testing.T) {
	for seed := range seeds(t, 400) {
		run(t, seed, config{machines: 2 + int(seed%2), steps: 300, destroy: true})
	}
}

func TestCompactionNeverChangesTheResult(t *testing.T) {
	for seed := range seeds(t, 400) {
		run(t, seed, config{machines: 2 + int(seed%2), steps: 300, compact: true})
	}
}

func TestCompactionConvergesAfterBackupRestoreAndBitRot(t *testing.T) {
	for seed := range seeds(t, 400) {
		run(t, seed, config{machines: 2 + int(seed%2), steps: 300, destroy: true, compact: true})
	}
}

func pull(t *testing.T, to, from *machine) {
	t.Helper()
	if _, _, err := replica.Pull(context.Background(), to.s, replica.Local{Store: from.s}); err != nil {
		t.Fatal(err)
	}
}

func set(t *testing.T, m *machine, value int) {
	t.Helper()
	var over []state.Write
	m.s.View(func(st *state.State) { over = st.Projects["p"].Get("k") })
	body, err := store.Set("p", "k", value, over)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.s.Commit(body); err != nil {
		t.Fatal(err)
	}
}

func values(m *machine) []string {
	var out []string
	m.s.View(func(st *state.State) {
		for _, w := range st.Projects["p"].Get("k") {
			out = append(out, string(w.Value))
		}
	})
	return out
}

func compactNow(t *testing.T, m *machine) store.CompactResult {
	t.Helper()
	r, err := m.s.Compact(nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAwayMacKeepsItsOwnWorkAndConflictsAfterSnapshot(t *testing.T) {
	w := newWorld(t, 5, config{machines: 2})
	a, b := w.m[0], w.m[1]
	set(t, a, 1)
	pull(t, b, a)
	set(t, a, 2)
	set(t, b, 3)
	compactNow(t, a)
	pull(t, b, a)
	pull(t, a, b)
	w.assertConverged(5)
	if got := values(b); len(got) != 2 {
		t.Fatalf("conflict was not kept: %v", got)
	}
}

func TestSupersededWriteStaysDeadAfterPrunedSnapshot(t *testing.T) {
	w := newWorld(t, 6, config{machines: 2})
	a, b := w.m[0], w.m[1]
	set(t, a, 1)
	pull(t, b, a)
	set(t, a, 2)
	compactNow(t, a)
	pull(t, b, a)
	if got := values(b); len(got) != 1 || got[0] != "2" {
		t.Fatalf("old value came back: %v", got)
	}
}

func TestCompactedWriterKeepsItsChain(t *testing.T) {
	w := newWorld(t, 7, config{machines: 2})
	a, b := w.m[0], w.m[1]
	set(t, a, 1)
	compactNow(t, a)
	set(t, a, 2)
	pull(t, b, a)
	w.open(a)
	w.open(b)
	set(t, a, 3)
	pull(t, b, a)
	w.assertConverged(7)
}

func TestMovingDamagedSnapshotsAsideRebuildsFromPeer(t *testing.T) {
	w := newWorld(t, 8, config{machines: 2})
	a, b := w.m[0], w.m[1]
	set(t, a, 1)
	set(t, a, 2)
	pull(t, b, a)
	compactNow(t, a)
	if !a.fs.RotAll(".snap") {
		t.Fatal("no snapshot to damage")
	}
	if _, err := store.Open(a.fs, home, store.Options{Name: a.name}); !errors.Is(err, store.ErrSnapshotsDamaged) {
		t.Fatalf("got %v", err)
	}
	names, _ := a.fs.List(home + "/snap")
	for _, name := range names {
		if err := a.fs.Remove(home + "/snap/" + name); err != nil {
			t.Fatal(err)
		}
	}
	w.open(a)
	w.heal()
	w.assertConverged(8)
	if got := values(a); len(got) != 1 || got[0] != "2" {
		t.Fatalf("did not recover: %v", got)
	}
}

func TestRejectedWriterIsNotDownloadedAgain(t *testing.T) {
	w := newWorld(t, 9, config{machines: 2})
	a, b := w.m[0], w.m[1]
	set(t, a, 1)
	events, _, err := a.s.Since(store.Vector{}, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	bad := events[0]
	bad.Body = []byte(`{"r":"p","op":"set","k":"x","v":1}`)
	if _, err := b.s.Apply([]event.Event{bad}); err == nil {
		t.Fatal("tampered event accepted")
	}
	compactNow(t, a)
	counter := &countingPeer{Local: replica.Local{Store: a.s}}
	if _, _, err := replica.Pull(context.Background(), b.s, counter); err == nil {
		t.Fatal("expected a refusal")
	}
	if counter.chunks != 0 {
		t.Fatalf("downloaded %d chunks before refusing", counter.chunks)
	}
}

type countingPeer struct {
	replica.Local
	chunks int
}

func (c *countingPeer) Snapshot(ctx context.Context, id string, offset int64) (replica.Chunk, error) {
	c.chunks++
	return c.Local.Snapshot(ctx, id, offset)
}

func TestRestoredBackupGetsNewerWorkBack(t *testing.T) {
	w := newWorld(t, 1, config{machines: 2})
	a, b := w.m[0], w.m[1]
	w.commit(a)
	_ = a.fs.SyncDir(home)
	old := a.fs.Clone()
	last := w.commit(a)[0]
	if _, _, err := replica.Pull(context.Background(), b.s, replica.Local{Store: a.s}); err != nil {
		t.Fatal(err)
	}
	a.fs = old
	w.open(a)
	if a.s.Vector()[last.Origin] >= last.Seq {
		t.Fatal("backup should predate the last event")
	}
	w.commit(a)
	w.heal()
	w.assertConverged(1)
	if a.s.Vector()[last.Origin] != last.Seq {
		t.Fatal("restored machine did not get its own newer event back")
	}
}

func TestTamperedEventIsQuarantined(t *testing.T) {
	w := newWorld(t, 2, config{machines: 2})
	a, b := w.m[0], w.m[1]
	w.commit(a)
	events, _, err := a.s.Since(store.Vector{}, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	events[0].Body = []byte(`{"r":"p","op":"set","k":"x","v":1}`)
	if _, err := b.s.Apply(events); err == nil {
		t.Fatal("tampered event accepted")
	}
	if len(b.s.Forks()) != 1 || len(b.s.Vector()) != 0 {
		t.Fatalf("forks %v vector %v", b.s.Forks(), b.s.Vector())
	}
}

func TestFailedDiskNeverExposesTheEvent(t *testing.T) {
	w := newWorld(t, 3, config{machines: 2})
	a := w.m[0]
	w.commit(a)
	before := a.s.Vector()
	a.fs.FailNextSyncs(1)
	body, _ := store.Set("p", "k", 1, nil)
	if _, err := a.s.Commit(body); err == nil {
		t.Fatal("commit succeeded on a failed fsync")
	}
	if a.s.Missing(before) != 0 {
		t.Fatal("unsynced event became visible")
	}
	if _, err := a.s.Commit(body); err == nil {
		t.Fatal("store kept writing after a failed fsync")
	}
	w.open(a)
	if _, err := a.s.Commit(body); err != nil {
		t.Fatalf("store did not recover after reopen: %v", err)
	}
}

func TestHostileVectorCannotCrashTheServer(t *testing.T) {
	w := newWorld(t, 4, config{machines: 1})
	id := w.commit(w.m[0])[0]
	events, more, err := w.m[0].s.Since(store.Vector{id.Origin: ^uint64(0)}, 10, 1<<20)
	if err != nil || more || len(events) != 0 {
		t.Fatalf("%d events, more %v, err %v", len(events), more, err)
	}
}
