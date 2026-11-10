package state

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
)

func ev(origin string, seq uint64, wall int64, b event.Body) event.Event {
	raw, _ := json.Marshal(b)
	return event.Event{Origin: origin, Seq: seq, Time: hlc.T{Wall: wall}, Body: raw}
}

func set(key, v string, over ...event.ID) event.Body {
	return event.Body{Project: "p", Op: event.OpSet, Key: key, Value: json.RawMessage(`"` + v + `"`), Over: over}
}

func TestApplyIgnoresDeliveryOrderAndDuplicates(t *testing.T) {
	for seed := range uint64(300) {
		r := rand.New(rand.NewPCG(seed, 9))
		var events []event.Event
		var ids []event.ID
		var elems []string
		for i := range 60 {
			origin := fmt.Sprintf("n%d", r.IntN(3))
			seq := uint64(i + 1)
			key := fmt.Sprintf("k%d", r.IntN(3))
			wall := int64(r.IntN(5))
			var b event.Body
			switch r.IntN(4) {
			case 0, 1:
				b = set(key, fmt.Sprint(i))
				if len(ids) > 0 && r.IntN(2) == 0 {
					b.Over = []event.ID{ids[r.IntN(len(ids))]}
				}
				ids = append(ids, event.ID{Origin: origin, Seq: seq})
			case 2:
				e := fmt.Sprintf("e%d", r.IntN(8))
				elems = append(elems, e)
				b = event.Body{Project: "p", Op: event.OpAdd, Key: "s", Elem: e, Value: json.RawMessage(fmt.Sprint(i))}
			default:
				if len(elems) == 0 {
					continue
				}
				b = event.Body{Project: "p", Op: event.OpDel, Key: "s", Elem: elems[r.IntN(len(elems))]}
			}
			events = append(events, ev(origin, seq, wall, b))
		}
		want := New()
		for _, e := range events {
			want.Apply(e)
		}
		for range 5 {
			got := New()
			r.Shuffle(len(events), func(i, j int) { events[i], events[j] = events[j], events[i] })
			for _, e := range events {
				got.Apply(e)
				if r.IntN(4) == 0 {
					got.Apply(e)
				}
			}
			if got.Hash() != want.Hash() {
				t.Fatalf("seed %d: state depends on delivery order", seed)
			}
		}
	}
}

func TestConcurrentWritesAreBothKept(t *testing.T) {
	s := New()
	s.Apply(ev("a", 1, 1, set("next", "base")))
	base := event.ID{Origin: "a", Seq: 1}
	s.Apply(ev("a", 2, 2, set("next", "from a", base)))
	s.Apply(ev("b", 1, 3, set("next", "from b", base)))
	if got := s.Projects["p"].Get("next"); len(got) != 2 {
		t.Fatalf("got %d live writes, want 2", len(got))
	}
	s.Apply(ev("a", 3, 4, set("next", "merged", event.ID{Origin: "a", Seq: 2}, event.ID{Origin: "b", Seq: 1})))
	if got := s.Projects["p"].Get("next"); len(got) != 1 || string(got[0].Value) != `"merged"` {
		t.Fatalf("conflict not resolved: %v", got)
	}
}

func TestWrongClockCannotHideAConflict(t *testing.T) {
	s := New()
	s.Apply(ev("a", 1, 9_000_000_000_000, set("next", "future")))
	s.Apply(ev("b", 1, 1, set("next", "past")))
	if got := s.Projects["p"].Get("next"); len(got) != 2 {
		t.Fatalf("got %d live writes, want 2", len(got))
	}
}

func TestDeletedElementStaysDeleted(t *testing.T) {
	s := New()
	add := event.Body{Project: "p", Op: event.OpAdd, Key: "s", Elem: "x", Value: json.RawMessage("1")}
	s.Apply(ev("a", 1, 1, add))
	s.Apply(ev("a", 2, 2, event.Body{Project: "p", Op: event.OpDel, Key: "s", Elem: "x"}))
	s.Apply(ev("b", 1, 3, add))
	if got := s.Projects["p"].Items("s"); len(got) != 0 {
		t.Fatalf("deleted element came back: %v", got)
	}
}

func TestUnknownOpsAreIgnored(t *testing.T) {
	s := New()
	s.Apply(event.Event{Origin: "a", Seq: 1, Body: []byte(`{"r":"p","op":"from-the-future","k":"x"}`)})
	s.Apply(event.Event{Origin: "a", Seq: 2, Body: []byte(`not json`)})
	if got := s.Projects["p"].Get("x"); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestMergeMatchesReplayEvenWhenPruned(t *testing.T) {
	for seed := range uint64(400) {
		r := rand.New(rand.NewPCG(seed, 9))
		var events [2][]event.Event
		full, sides := New(), [2]*State{New(), New()}
		seq := [2]uint64{}
		origins := [2]string{"a", "b"}
		var live [2][]event.ID
		for i := range 60 {
			n := r.IntN(2)
			seq[n]++
			id := event.ID{Origin: origins[n], Seq: seq[n]}
			b := event.Body{Project: "p", Op: event.OpSet, Key: "k", Over: live[n]}
			if other := events[1-n]; len(other) > 0 && r.IntN(3) == 0 {
				b.Over = append(slices.Clone(b.Over), other[r.IntN(len(other))].ID())
			}
			if r.IntN(4) == 0 {
				b = event.Body{Project: "p", Op: event.OpAdd, Key: "s", Elem: fmt.Sprint(r.IntN(5))}
			} else {
				live[n] = []event.ID{id}
			}
			e := ev(id.Origin, id.Seq, int64(i), b)
			events[n] = append(events[n], e)
			full.Apply(e)
			sides[n].Apply(e)
		}
		seen := func(n int) func(event.ID) bool {
			return func(id event.ID) bool { return id.Origin == origins[n] && id.Seq <= seq[n] }
		}
		sides[0].Prune(seen(0))
		sides[1].Prune(seen(1))
		ab, ba := sides[0].Clone(), sides[1].Clone()
		ab.Merge(sides[1], seen(0), seen(1))
		ba.Merge(sides[0], seen(1), seen(0))
		if ab.Hash() != full.Hash() || ba.Hash() != full.Hash() {
			t.Fatalf("seed %d: merge differs from replay", seed)
		}
		ab.Merge(ba, func(event.ID) bool { return true }, func(event.ID) bool { return true })
		if ab.Hash() != full.Hash() {
			t.Fatalf("seed %d: merge is not idempotent", seed)
		}
	}
}
