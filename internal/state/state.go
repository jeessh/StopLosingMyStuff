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

func (s *State) Hash() string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
