package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"sync"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
	"github.com/jeessh/StopLosingMyStuff/internal/wal"
)

const defaultSegment = 16 << 20

var ErrReadOnly = errors.New("store is read only")

type Vector map[string]uint64

type Options struct {
	Name        string
	Now         func() int64
	Rand        io.Reader
	SegmentSize int64
	ReadOnly    bool
	OnCommit    func()
}

type node struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Store struct {
	mu       sync.RWMutex
	opt      Options
	log      *wal.WAL
	node     node
	clock    *hlc.Clock
	state    *state.State
	vector   Vector
	heads    map[string]string
	index    map[string][]wal.Loc
	top      map[string]uint64
	segs     map[uint32]*segInfo
	forks    map[string]string
	origin   string
	recovery wal.Recovery
	fs       vfs.FS
	dir      string
	base     Vector
	unknown  []event.Event
	gen      uint64
	since    int64

	sealedBefore uint32
}

func Open(fsys vfs.FS, dir string, opt Options) (*Store, error) {
	if opt.Rand == nil {
		opt.Rand = rand.Reader
	}
	if opt.SegmentSize == 0 {
		opt.SegmentSize = defaultSegment
	}
	s := &Store{opt: opt, fs: fsys, dir: dir, clock: hlc.New(opt.Now), forks: map[string]string{}}
	s.reset()
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	if err := s.loadNode(fsys, dir); err != nil {
		return nil, err
	}
	if opt.ReadOnly {
		return s, s.readAll()
	}
	bad, err := s.loadSnapshot()
	if err != nil {
		return nil, err
	}
	s.log, s.recovery, err = wal.Open(fsys, s.logDir(), opt.SegmentSize, s.sealedBefore, s.visit)
	if err != nil {
		return nil, err
	}
	s.recovery.BadSnapshots = bad
	s.finishIndex()
	return s, nil
}

func (s *Store) logDir() string { return filepath.Join(s.dir, "log") }

func (s *Store) reset() {
	s.state = state.New()
	s.vector, s.base = Vector{}, Vector{}
	s.heads = map[string]string{}
	s.index = map[string][]wal.Loc{}
	s.top = map[string]uint64{}
	s.segs = map[uint32]*segInfo{}
	s.unknown, s.gen, s.since, s.sealedBefore = nil, 0, 0, 0
}

// readAll retries if a daemon compacts underneath it. Every compaction writes
// a new snapshot before removing anything, so an unchanged generation means
// the scan saw a consistent log.
func (s *Store) readAll() error {
	for try := 0; ; try++ {
		s.reset()
		if _, err := s.loadSnapshot(); err != nil {
			return err
		}
		err := wal.Scan(s.fs, s.logDir(), s.sealedBefore, s.visit)
		s.finishIndex()
		gens, lerr := snapGens(s.fs, s.snapDir())
		same := lerr == nil && (len(gens) == 0 && s.gen == 0 || len(gens) > 0 && gens[0] == s.gen)
		if same && !errors.Is(err, wal.ErrVanished) {
			return err
		}
		if try == 5 {
			return errors.New("log kept changing while reading it")
		}
	}
}

func (s *Store) visit(l wal.Loc, rec []byte) error {
	var e event.Event
	if err := json.Unmarshal(rec, &e); err != nil {
		return err
	}
	if e.Seq <= s.base[e.Origin] {
		if err := e.Verify(); err != nil {
			return err
		}
		s.place(e, l, len(rec))
		return nil
	}
	if err := s.check(e); err != nil {
		return err
	}
	s.absorb(e, l, len(rec))
	return nil
}

func (s *Store) loadNode(fsys vfs.FS, dir string) error {
	path := filepath.Join(dir, "node.json")
	data, err := fsys.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, &s.node); err != nil {
			return fmt.Errorf("node.json: %w", err)
		}
		if s.opt.Name != "" {
			s.node.Name = s.opt.Name
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	id, err := s.randomID(8)
	if err != nil {
		return err
	}
	s.node = node{ID: id, Name: s.opt.Name}
	if s.opt.ReadOnly {
		return nil
	}
	data, err = json.Marshal(s.node)
	if err != nil {
		return err
	}
	return vfs.WriteAtomic(fsys, path, data)
}

func (s *Store) randomID(n int) (string, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(s.opt.Rand, b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (s *Store) check(e event.Event) error {
	if err := e.Verify(); err != nil {
		return err
	}
	if e.Seq != s.vector[e.Origin]+1 {
		return fmt.Errorf("gap: %s after %d", e.ID(), s.vector[e.Origin])
	}
	if e.Prev != s.heads[e.Origin] {
		return fmt.Errorf("fork at %s", e.ID())
	}
	return nil
}

func (s *Store) absorb(e event.Event, l wal.Loc, size int) {
	s.vector[e.Origin] = e.Seq
	s.heads[e.Origin] = e.Hash
	s.place(e, l, size)
	s.since += int64(size + wal.Overhead)
	if !state.Knows(e) {
		s.unknown = append(s.unknown, e)
	}
	s.state.Apply(e)
	s.clock.Observe(e.Time)
}

// place records where an event lives. The index for an origin always ends at
// its newest event, so a jump from a snapshot starts it over.
func (s *Store) place(e event.Event, l wal.Loc, size int) {
	if len(s.index[e.Origin]) > 0 && s.top[e.Origin]+1 != e.Seq {
		s.index[e.Origin] = nil
	}
	s.index[e.Origin] = append(s.index[e.Origin], l)
	s.top[e.Origin] = e.Seq
	info := s.segs[l.Seg]
	if info == nil {
		info = &segInfo{last: map[string]uint64{}}
		s.segs[l.Seg] = info
	}
	info.last[e.Origin] = e.Seq
	info.bytes += int64(size + wal.Overhead)
}

func (s *Store) finishIndex() {
	for o := range s.index {
		if s.top[o] != s.vector[o] {
			delete(s.index, o)
			delete(s.top, o)
		}
	}
}

// floor is the newest event of an origin this node can no longer serve.
func (s *Store) floor(o string) uint64 { return s.vector[o] - uint64(len(s.index[o])) }

// Commit makes local changes durable. Each process writes under a fresh origin
// chosen at its first commit, so sequence numbers are never reused even if the
// data directory is restored from a backup or copied to another machine.
func (s *Store) Commit(bodies ...event.Body) ([]event.ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return nil, ErrReadOnly
	}
	if err := s.log.Err(); err != nil {
		return nil, err
	}
	if s.origin == "" {
		inc, err := s.randomID(6)
		if err != nil {
			return nil, err
		}
		s.origin = s.node.ID[:8] + "-" + inc
	}
	seq, prev := s.vector[s.origin], s.heads[s.origin]
	events := make([]event.Event, len(bodies))
	for i, b := range bodies {
		raw, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		seq++
		e := event.Event{Origin: s.origin, Seq: seq, Prev: prev, Time: s.clock.Now(), Body: raw}
		e.Seal()
		if err := e.Verify(); err != nil {
			return nil, err
		}
		prev = e.Hash
		events[i] = e
	}
	if err := s.write(events); err != nil {
		return nil, err
	}
	ids := make([]event.ID, len(events))
	for i, e := range events {
		ids[i] = e.ID()
	}
	if s.opt.OnCommit != nil {
		s.opt.OnCommit()
	}
	return ids, nil
}

func (s *Store) write(events []event.Event) error {
	records := make([][]byte, len(events))
	for i, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			return err
		}
		records[i] = raw
	}
	locs, err := s.log.Append(records)
	if err != nil {
		return err
	}
	for i, e := range events {
		s.absorb(e, locs[i], len(records[i]))
	}
	return nil
}

// Apply stores events pulled from a peer and returns how many were new.
// Bad events from an origin stop that origin only.
func (s *Store) Apply(events []event.Event) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return 0, ErrReadOnly
	}
	seq := map[string]uint64{}
	head := map[string]string{}
	var fresh []event.Event
	var firstErr error
	for _, e := range events {
		if s.forks[e.Origin] != "" {
			continue
		}
		if err := e.Verify(); err != nil {
			s.forks[e.Origin] = err.Error()
			firstErr = cmp(firstErr, fmt.Errorf("%s: %w", e.ID(), err))
			continue
		}
		cur, ok := seq[e.Origin]
		if !ok {
			cur, head[e.Origin] = s.vector[e.Origin], s.heads[e.Origin]
		}
		if e.Seq <= cur {
			seq[e.Origin] = cur
			continue
		}
		if e.Seq != cur+1 {
			firstErr = cmp(firstErr, fmt.Errorf("gap at %s", e.ID()))
			seq[e.Origin] = cur
			continue
		}
		if e.Prev != head[e.Origin] {
			s.forks[e.Origin] = fmt.Sprintf("history diverged at seq %d", e.Seq)
			firstErr = cmp(firstErr, fmt.Errorf("fork at %s", e.ID()))
			continue
		}
		seq[e.Origin], head[e.Origin] = e.Seq, e.Hash
		fresh = append(fresh, e)
	}
	if len(fresh) == 0 {
		return 0, firstErr
	}
	if err := s.write(fresh); err != nil {
		return 0, err
	}
	return len(fresh), firstErr
}

func cmp(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

// Since returns only events already fsynced here, in order per origin, so a
// peer can never hold an event this node might lose.
func (s *Store) Since(have Vector, maxCount, maxBytes int) ([]event.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return nil, false, ErrReadOnly
	}
	origins := make([]string, 0, len(s.vector))
	for o := range s.vector {
		origins = append(origins, o)
	}
	sort.Strings(origins)
	for _, o := range origins {
		if have[o] < s.floor(o) {
			return nil, false, ErrSnapshotNeeded
		}
	}
	var out []event.Event
	size := 0
	for _, o := range origins {
		if have[o] >= s.vector[o] {
			continue
		}
		for seq := have[o] + 1; seq <= s.vector[o]; seq++ {
			if len(out) >= maxCount || size >= maxBytes {
				return out, true, nil
			}
			rec, err := s.log.Read(s.index[o][seq-s.floor(o)-1])
			if err != nil {
				return out, false, fmt.Errorf("read %s:%d: %w", o, seq, err)
			}
			var e event.Event
			if err := json.Unmarshal(rec, &e); err != nil {
				return out, false, err
			}
			out = append(out, e)
			size += len(rec)
		}
	}
	return out, false, nil
}

func (s *Store) Vector() Vector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := make(Vector, len(s.vector))
	for k, n := range s.vector {
		v[k] = n
	}
	return v
}

// Missing counts events this node has that the given vector lacks.
func (s *Store) Missing(have Vector) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n uint64
	for o, seq := range s.vector {
		if seq > have[o] {
			n += seq - have[o]
		}
	}
	return n
}

func (s *Store) View(fn func(*state.State)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.state)
}

func (s *Store) Hash() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Hash()
}

func (s *Store) NodeID() string { return s.node.ID }
func (s *Store) Name() string   { return s.node.Name }

func (s *Store) Forks() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.forks))
	for k, v := range s.forks {
		out[k] = v
	}
	return out
}

func (s *Store) Recovery() wal.Recovery { return s.recovery }

// Err reports a failed log. After one, the process has to restart.
func (s *Store) Err() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.log == nil {
		return nil
	}
	return s.log.Err()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return nil
	}
	return s.log.Close()
}

func Set(project, key string, value any, over []state.Write) (event.Body, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return event.Body{}, err
	}
	b := event.Body{Project: project, Op: event.OpSet, Key: key, Value: raw}
	for _, w := range over {
		b.Over = append(b.Over, w.ID)
	}
	return b, nil
}

func Add(project, key, elem string, value any) (event.Body, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return event.Body{}, err
	}
	return event.Body{Project: project, Op: event.OpAdd, Key: key, Elem: elem, Value: raw}, nil
}
