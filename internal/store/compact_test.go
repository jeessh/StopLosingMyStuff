package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

func open(t *testing.T, m *vfs.MemFS) *Store {
	t.Helper()
	s, err := Open(m, "/d", Options{Name: "n", SegmentSize: 2048})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fill(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := range n {
		body, err := Set("p", fmt.Sprintf("k%d", i%5), strings.Repeat("v", 100), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Commit(body); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestartAfterCompactionKeepsEverything(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 100)
	want := s.Hash()
	res, err := s.Compact(nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 || res.Freed == 0 {
		t.Fatalf("nothing removed: %+v", res)
	}
	m.Crash(seeded(1))
	s = open(t, m)
	if s.Hash() != want {
		t.Fatal("state changed across compaction and restart")
	}
	fill(t, s, 3)
}

func TestCompactionWaitsForPeers(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 100)
	res, err := s.Compact([]Vector{{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 0 || res.Held == 0 {
		t.Fatalf("removed files a peer still needs: %+v", res)
	}
	events, _, err := s.Since(Vector{}, 200, 1<<30)
	if err != nil || len(events) != 100 {
		t.Fatalf("%d events, %v", len(events), err)
	}
	if !s.NeedsCompaction([]Vector{s.Vector()}, 1<<40) {
		t.Fatal("caught-up peer should make old files removable")
	}
	res, err = s.Compact([]Vector{s.Vector()})
	if err != nil || res.Removed == 0 {
		t.Fatalf("%+v %v", res, err)
	}
	if _, _, err := s.Since(Vector{}, 200, 1<<30); !errors.Is(err, ErrSnapshotNeeded) {
		t.Fatalf("expected snapshot request, got %v", err)
	}
}

func TestRetainedEventsStayServableAfterRestart(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 50)
	if _, err := s.Compact([]Vector{{}}); err != nil {
		t.Fatal(err)
	}
	fill(t, s, 5)
	s = open(t, m)
	events, _, err := s.Since(Vector{}, 200, 1<<30)
	if err != nil || len(events) != 55 {
		t.Fatalf("%d events, %v", len(events), err)
	}
}

func TestCrashAtEveryCompactionStep(t *testing.T) {
	base := vfs.NewMem()
	s := open(t, base)
	fill(t, s, 100)
	want := s.Hash()
	for step := 1; ; step++ {
		finished := false
		for seed := range uint64(4) {
			m := base.Clone()
			s := open(t, m)
			finished = func() (done bool) {
				defer func() {
					if v := recover(); v != nil && v != vfs.ErrCrash {
						panic(v)
					}
				}()
				m.CrashAfter(step)
				if _, err := s.Compact(nil); err != nil {
					t.Fatal(err)
				}
				return true
			}()
			m.CrashAfter(0)
			m.Crash(seeded(seed))
			r := open(t, m)
			if r.Hash() != want {
				t.Fatalf("step %d: state changed after crash", step)
			}
			fill(t, r, 1)
		}
		if finished {
			if step < 8 {
				t.Fatalf("only %d steps were tried", step)
			}
			return
		}
	}
}

func TestInstalledSnapshotSurvivesRestart(t *testing.T) {
	a := open(t, vfs.NewMem())
	fill(t, a, 20)
	if _, err := a.Compact(nil); err != nil {
		t.Fatal(err)
	}
	_, _, data, err := a.SnapshotChunk("", 0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	m := vfs.NewMem()
	b := open(t, m)
	if _, err := b.Install(data); err != nil {
		t.Fatal(err)
	}
	m.Crash(seeded(2))
	b = open(t, m)
	if b.Hash() != a.Hash() || b.Missing(a.Vector()) != 0 || a.Missing(b.Vector()) != 0 {
		t.Fatal("installed snapshot was lost")
	}
}

func TestDiskFullDuringCompactionChangesNothing(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 100)
	want := s.Hash()
	m.FailNextWrites(1)
	if _, err := s.Compact(nil); err == nil {
		t.Fatal("expected failure")
	}
	if s.Err() != nil || s.Hash() != want {
		t.Fatalf("store damaged: %v", s.Err())
	}
	if events, _, err := s.Since(Vector{}, 200, 1<<30); err != nil || len(events) != 100 {
		t.Fatalf("%d events, %v", len(events), err)
	}
	fill(t, s, 1)
}

func TestDamagedSnapshotCopyIsRepaired(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 100)
	want := s.Hash()
	if _, err := s.Compact(nil); err != nil {
		t.Fatal(err)
	}
	if !m.Rot(seeded(1), ".a.snap") {
		t.Fatal("no snapshot found")
	}
	s = open(t, m)
	if s.Hash() != want || s.Recovery().BadSnapshots != 1 {
		t.Fatalf("recovery %+v", s.Recovery())
	}
	s = open(t, m)
	if s.Recovery().BadSnapshots != 0 {
		t.Fatal("damaged copy was not rewritten")
	}
}

func TestEventsFromNewerBuildsSurviveCompaction(t *testing.T) {
	a := open(t, vfs.NewMem())
	raw := []byte(`{"r":"p","op":"future","k":"x"}`)
	e := event.Event{Origin: "newer", Seq: 1, Time: hlc.T{Wall: 5}, Body: raw}
	e.Seal()
	if _, err := a.Apply([]event.Event{e}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Compact(nil); err != nil {
		t.Fatal(err)
	}
	_, total, data, err := a.SnapshotChunk("", 0, 1<<30)
	if err != nil || int64(len(data)) != total {
		t.Fatal(err)
	}
	b := open(t, vfs.NewMem())
	if _, err := b.Install(data); err != nil {
		t.Fatal(err)
	}
	if len(b.unknown) != 1 || string(b.unknown[0].Body) != string(raw) {
		t.Fatalf("unknown event lost: %+v", b.unknown)
	}
}

func TestSnapshotFromAnotherHistoryIsRefused(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 3)
	want, o := s.Hash(), ""
	for origin := range s.Vector() {
		o = origin
	}
	bad, err := encodeSnapshot(snapshot{
		Vector: Vector{o: 3, "other": 1},
		Heads:  map[string]string{o: "not-the-real-hash", "other": "x"},
		State:  s.state.Clone(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Install(bad); err == nil {
		t.Fatal("accepted a snapshot with a different history")
	}
	if s.Hash() != want || len(s.Forks()) != 1 {
		t.Fatalf("forks %v", s.Forks())
	}
}

func TestDamagedTransferIsRefused(t *testing.T) {
	a := open(t, vfs.NewMem())
	fill(t, a, 3)
	if _, err := a.Compact(nil); err != nil {
		t.Fatal(err)
	}
	_, _, data, err := a.SnapshotChunk("", 0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	data = append([]byte(nil), data...)
	data[len(data)-1] ^= 1
	b := open(t, vfs.NewMem())
	if _, err := b.Install(data); err == nil || len(b.Vector()) != 0 {
		t.Fatalf("accepted damaged snapshot: %v", err)
	}
}

func TestReadOnlySeesCompactedStore(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 100)
	if _, err := s.Compact(nil); err != nil {
		t.Fatal(err)
	}
	fill(t, s, 2)
	r, err := Open(m, "/d", Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.Hash() != s.Hash() {
		t.Fatal("read-only view differs")
	}
}

func seeded(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, 3)) }

func sealed(t *testing.T, origin string, seq uint64, prev string, body event.Body) event.Event {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	e := event.Event{Origin: origin, Seq: seq, Prev: prev, Time: hlc.T{Wall: int64(seq)}, Body: raw}
	e.Seal()
	return e
}

func values(s *Store) string {
	out := ""
	s.View(func(st *state.State) {
		for _, w := range st.Projects["p"].Get("k") {
			out += string(w.Value)
		}
	})
	return out
}

func snapshotOf(t *testing.T, sn snapshot) []byte {
	t.Helper()
	if sn.State == nil {
		sn.State = state.New()
	}
	if sn.Heads == nil {
		sn.Heads = map[string]string{}
	}
	data, err := encodeSnapshot(sn)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestStaleRelayedEventCannotReviveAReplacedValue(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	y := sealed(t, "up", 1, "", event.Body{Project: "p", Op: event.OpSet, Key: "k", Value: []byte(`"y"`)})
	z := sealed(t, "up", 2, y.Hash, event.Body{Project: "p", Op: event.OpSet, Key: "k", Value: []byte(`"z"`), Over: []event.ID{y.ID()}})
	if _, err := s.Apply([]event.Event{y, z}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(nil); err != nil {
		t.Fatal(err)
	}
	other := sealed(t, "other", 1, "", event.Body{Project: "q", Op: event.OpSet, Key: "x", Value: []byte(`1`)})
	data := snapshotOf(t, snapshot{
		Vector:  Vector{"up": 1, "other": 1},
		Heads:   map[string]string{"up": y.Hash, "other": other.Hash},
		Unknown: []event.Event{y},
	})
	if _, err := s.Install(data); err != nil {
		t.Fatalf("install: %v", err)
	}
	s = open(t, m)
	if got := values(s); got != `"z"` {
		t.Fatalf("got %s", got)
	}
}

func TestRelayedEventThisBuildKnowsIsAppliedRightAway(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	y := sealed(t, "up", 1, "", event.Body{Project: "p", Op: event.OpSet, Key: "k", Value: []byte(`"y"`)})
	if _, err := s.Install(snapshotOf(t, snapshot{Vector: Vector{"up": 1}, Heads: map[string]string{"up": y.Hash}, Unknown: []event.Event{y}})); err != nil {
		t.Fatal(err)
	}
	before := s.Hash()
	if values(s) != `"y"` {
		t.Fatalf("not applied: %s", values(s))
	}
	if s = open(t, m); s.Hash() != before {
		t.Fatal("state changed across restart")
	}
}

func TestEventTheSenderUnderstoodLeavesTheRelayList(t *testing.T) {
	s := open(t, vfs.NewMem())
	raw := sealed(t, "up", 1, "", event.Body{Project: "p", Op: "future", Key: "k"})
	if _, err := s.Apply([]event.Event{raw}); err != nil {
		t.Fatal(err)
	}
	other := sealed(t, "other", 1, "", event.Body{Project: "q", Op: event.OpSet, Key: "x", Value: []byte(`1`)})
	if _, err := s.Install(snapshotOf(t, snapshot{Vector: Vector{"up": 1, "other": 1}, Heads: map[string]string{"up": raw.Hash, "other": other.Hash}})); err != nil {
		t.Fatal(err)
	}
	if len(s.unknown) != 0 {
		t.Fatalf("still relaying %+v", s.unknown)
	}
}

func TestBothCopiesDamagedRefusesToStartAndKeepsTheLog(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 20)
	if _, err := s.Compact(nil); err != nil {
		t.Fatal(err)
	}
	fill(t, s, 5)
	m.RotAll(".snap")
	names, _ := m.List("/d/log")
	if _, err := Open(m, "/d", Options{Name: "n", SegmentSize: 2048}); !errors.Is(err, ErrSnapshotsDamaged) {
		t.Fatalf("got %v", err)
	}
	if _, err := Open(m, "/d", Options{ReadOnly: true}); !errors.Is(err, ErrSnapshotsDamaged) {
		t.Fatalf("read only got %v", err)
	}
	after, _ := m.List("/d/log")
	if !slices.Equal(names, after) {
		t.Fatalf("log changed: %v -> %v", names, after)
	}
}

func TestDamageInSnapshottedFileKeepsNewerEvents(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 40)
	if _, err := s.Compact([]Vector{{}}); err != nil {
		t.Fatal(err)
	}
	fill(t, s, 5)
	want := s.Vector()
	if !m.Rot(seeded(9), "000000000001.seg") {
		t.Fatal("no first segment")
	}
	s = open(t, m)
	got := s.Vector()
	for o, seq := range want {
		if got[o] != seq {
			t.Fatalf("lost newer events: want %v got %v", want, got)
		}
	}
}

func TestReadErrorIsNotCalledAFork(t *testing.T) {
	m := vfs.NewMem()
	s := open(t, m)
	fill(t, s, 40)
	o := ""
	for origin := range s.Vector() {
		o = origin
	}
	if err := m.Remove("/d/log/000000000001.seg"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Install(snapshotOf(t, snapshot{Vector: Vector{o: 1, "other": 1}, Heads: map[string]string{o: "x", "other": "y"}}))
	if err == nil || len(s.Forks()) != 0 {
		t.Fatalf("err %v forks %v", err, s.Forks())
	}
}

func TestInstalledSnapshotKeepsTheSendersClock(t *testing.T) {
	m := vfs.NewMem()
	var now int64 = 1000
	opt := Options{Name: "n", SegmentSize: 2048, Now: func() int64 { return now }}
	s, err := Open(m, "/d", opt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Install(snapshotOf(t, snapshot{Vector: Vector{"o": 1}, Heads: map[string]string{"o": "h"}, Clock: hlc.T{Wall: 1500}})); err != nil {
		t.Fatal(err)
	}
	s, err = Open(m, "/d", opt)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.clock.Last(); got.Wall != 1500 {
		t.Fatalf("clock %v", got)
	}
}

func TestAbsurdVectorIsRefused(t *testing.T) {
	s := open(t, vfs.NewMem())
	if _, err := s.Install(snapshotOf(t, snapshot{Vector: Vector{"o": ^uint64(0)}, Heads: map[string]string{"o": "h"}})); err == nil || len(s.Vector()) != 0 {
		t.Fatalf("accepted: %v", err)
	}
}
