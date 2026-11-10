package state

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
)

type Write struct {
	ID    event.ID        `json:"i"`
	Time  hlc.T           `json:"t"`
	Value json.RawMessage `json:"v"`
}

// Register keeps every write no other write has superseded. Two surviving
// writes mean two Macs changed the same thing without seeing each other.
type Register struct {
	Live map[string]Write `json:"l"`
	Dead map[string]bool  `json:"d"`
}

// Set elements have stable IDs, so re-importing a transcript can't duplicate
// one and a deleted element can't come back.
type Set struct {
	Items map[string]Write `json:"i"`
	Gone  map[string]bool  `json:"g"`
}

type Project struct {
	Regs map[string]*Register `json:"r"`
	Sets map[string]*Set      `json:"s"`
}

type State struct {
	Projects map[string]*Project `json:"p"`
}

func New() *State { return &State{Projects: map[string]*Project{}} }

// Knows reports whether this build can apply e. Compaction keeps the rest as
// raw events, so a newer Mac's writes survive passing through an older one.
func Knows(e event.Event) bool {
	var b event.Body
	if json.Unmarshal(e.Body, &b) != nil || b.Project == "" || b.Key == "" {
		return false
	}
	switch b.Op {
	case event.OpSet:
		return true
	case event.OpAdd, event.OpDel:
		return b.Elem != ""
	}
	return false
}

// Normalize fills maps a decoded snapshot may have left nil.
func (s *State) Normalize() {
	if s.Projects == nil {
		s.Projects = map[string]*Project{}
	}
	for id, p := range s.Projects {
		if p == nil {
			delete(s.Projects, id)
			continue
		}
		if p.Regs == nil {
			p.Regs = map[string]*Register{}
		}
		if p.Sets == nil {
			p.Sets = map[string]*Set{}
		}
		for k, r := range p.Regs {
			if r == nil {
				delete(p.Regs, k)
				continue
			}
			if r.Live == nil {
				r.Live = map[string]Write{}
			}
			if r.Dead == nil {
				r.Dead = map[string]bool{}
			}
		}
		for k, set := range p.Sets {
			if set == nil {
				delete(p.Sets, k)
				continue
			}
			if set.Items == nil {
				set.Items = map[string]Write{}
			}
			if set.Gone == nil {
				set.Gone = map[string]bool{}
			}
		}
	}
}

func (s *State) Clone() *State {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	out := New()
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
	out.Normalize()
	return out
}

// Prune forgets superseded IDs for writes already seen. Replay never applies
// them again, and Merge can tell they're dead from what each side has seen.
func (s *State) Prune(seen func(event.ID) bool) {
	for _, p := range s.Projects {
		for _, r := range p.Regs {
			for key := range r.Dead {
				if id, ok := event.ParseID(key); ok && seen(id) {
					delete(r.Dead, key)
				}
			}
		}
	}
}

// Merge folds in another replica's state. mine and theirs report which writes
// each side has already seen: a write one side has seen but no longer holds
// was superseded there, even with its Dead entry pruned.
func (s *State) Merge(o *State, mine, theirs func(event.ID) bool) {
	for id, p := range o.Projects {
		local := s.project(id)
		for k, r := range p.Regs {
			local.register(k).merge(r, mine, theirs)
		}
		for k, set := range p.Sets {
			local.set(k).merge(set)
		}
	}
	empty := &Register{}
	for id, p := range s.Projects {
		remote := o.Projects[id]
		for k, r := range p.Regs {
			if remote == nil || remote.Regs[k] == nil {
				r.merge(empty, mine, theirs)
			}
		}
	}
}

func (r *Register) merge(o *Register, mine, theirs func(event.ID) bool) {
	live := map[string]Write{}
	for id, w := range r.Live {
		_, both := o.Live[id]
		if !o.Dead[id] && (both || !theirs(w.ID)) {
			live[id] = w
		}
	}
	for id, w := range o.Live {
		_, both := r.Live[id]
		if !r.Dead[id] && (both || !mine(w.ID)) {
			live[id] = w
		}
	}
	r.Live = live
	for id := range o.Dead {
		r.Dead[id] = true
	}
}

func (s *Set) merge(o *Set) {
	for elem := range o.Gone {
		s.Gone[elem] = true
		delete(s.Items, elem)
	}
	for elem, w := range o.Items {
		if s.Gone[elem] {
			continue
		}
		if cur, ok := s.Items[elem]; !ok || later(w, cur) {
			s.Items[elem] = w
		}
	}
}

func (s *State) project(id string) *Project {
	p := s.Projects[id]
	if p == nil {
		p = &Project{Regs: map[string]*Register{}, Sets: map[string]*Set{}}
		s.Projects[id] = p
	}
	return p
}

// Apply is commutative and idempotent. Replication only guarantees order per
// origin, so a superseding write can arrive before the write it replaces.
func (s *State) Apply(e event.Event) {
	var b event.Body
	if json.Unmarshal(e.Body, &b) != nil || b.Project == "" || b.Key == "" {
		return
	}
	w := Write{ID: e.ID(), Time: e.Time, Value: b.Value}
	switch b.Op {
	case event.OpSet:
		r := s.project(b.Project).register(b.Key)
		if !r.Dead[w.ID.String()] {
			r.Live[w.ID.String()] = w
		}
		for _, id := range b.Over {
			r.Dead[id.String()] = true
			delete(r.Live, id.String())
		}
	case event.OpAdd:
		if b.Elem == "" {
			return
		}
		set := s.project(b.Project).set(b.Key)
		if set.Gone[b.Elem] {
			return
		}
		if cur, ok := set.Items[b.Elem]; !ok || later(w, cur) {
			set.Items[b.Elem] = w
		}
	case event.OpDel:
		if b.Elem == "" {
			return
		}
		set := s.project(b.Project).set(b.Key)
		set.Gone[b.Elem] = true
		delete(set.Items, b.Elem)
	}
}

func later(a, b Write) bool {
	if c := a.Time.Compare(b.Time); c != 0 {
		return c > 0
	}
	return a.ID.String() > b.ID.String()
}

func (p *Project) register(key string) *Register {
	r := p.Regs[key]
	if r == nil {
		r = &Register{Live: map[string]Write{}, Dead: map[string]bool{}}
		p.Regs[key] = r
	}
	return r
}

func (p *Project) set(key string) *Set {
	s := p.Sets[key]
	if s == nil {
		s = &Set{Items: map[string]Write{}, Gone: map[string]bool{}}
		p.Sets[key] = s
	}
	return s
}

func sorted(m map[string]Write) []Write {
	out := make([]Write, 0, len(m))
	for _, w := range m {
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b Write) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.ID.String(), b.ID.String())
	})
	return out
}

// Get returns surviving writes oldest first.
func (p *Project) Get(key string) []Write {
	if p == nil || p.Regs[key] == nil {
		return nil
	}
	return sorted(p.Regs[key].Live)
}

func (p *Project) Items(key string) []Write {
	if p == nil || p.Sets[key] == nil {
		return nil
	}
	return sorted(p.Sets[key].Items)
}

func (p *Project) Keys(prefix string) []string {
	if p == nil {
		return nil
	}
	var out []string
	for k, r := range p.Regs {
		if strings.HasPrefix(k, prefix) && len(r.Live) > 0 {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// Hash covers what a reader can see. Dead is left out because how much of it
// has been pruned differs between Macs that agree on everything else.
func (s *State) Hash() string {
	view := New()
	for id, p := range s.Projects {
		out := &Project{Regs: map[string]*Register{}, Sets: map[string]*Set{}}
		for k, r := range p.Regs {
			if len(r.Live) > 0 {
				out.Regs[k] = &Register{Live: r.Live}
			}
		}
		for k, set := range p.Sets {
			if len(set.Items) > 0 || len(set.Gone) > 0 {
				out.Sets[k] = set
			}
		}
		if len(out.Regs) > 0 || len(out.Sets) > 0 {
			view.Projects[id] = out
		}
	}
	b, err := json.Marshal(view)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
