package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const (
	// Change the magic whenever the format changes meaning, so an older build
	// refuses a snapshot instead of half understanding it.
	snapMagic  = "SLMSSNAP1\n"
	snapSuffix = ".snap"
	sumSize    = 64
	maxSeq     = 1 << 62
)

// A snapshot is written twice because once the log files it replaces are gone
// it's the only local copy of that history.
var snapCopies = []string{"a", "b"}

var (
	ErrSnapshotNeeded   = errors.New("events were compacted, fetch the snapshot")
	ErrSnapshotChanged  = errors.New("snapshot changed during transfer")
	ErrSnapshotsDamaged = errors.New("every copy of the snapshot is damaged")
	errBadSnapshot      = errors.New("bad snapshot")
	errFork             = errors.New("history differs")
)

type snapshot struct {
	Vector  Vector            `json:"vector"`
	Heads   map[string]string `json:"heads"`
	Clock   hlc.T             `json:"clock"`
	State   *state.State      `json:"state"`
	Unknown []event.Event     `json:"unknown,omitempty"`
	// Every log segment numbered below this holds only covered events.
	SealedBefore uint32 `json:"sealed_before"`
}

type segInfo struct {
	last  map[string]uint64
	bytes int64
}

type CompactResult struct {
	SnapshotBytes int64
	Removed       int
	Freed         int64
	Held          int
}

func encodeSnapshot(sn snapshot) ([]byte, error) {
	body, err := json.Marshal(sn)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	out := make([]byte, 0, len(snapMagic)+sumSize+1+len(body))
	out = append(out, snapMagic...)
	out = append(out, hex.EncodeToString(sum[:])...)
	out = append(out, '\n')
	return append(out, body...), nil
}

func snapshotBody(data []byte) ([]byte, error) {
	rest, ok := bytes.CutPrefix(data, []byte(snapMagic))
	if !ok || len(rest) <= sumSize || rest[sumSize] != '\n' {
		return nil, errBadSnapshot
	}
	body := rest[sumSize+1:]
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != string(rest[:sumSize]) {
		return nil, errBadSnapshot
	}
	return body, nil
}

func decodeSnapshot(data []byte) (*snapshot, error) {
	body, err := snapshotBody(data)
	if err != nil {
		return nil, err
	}
	var sn snapshot
	if err := json.Unmarshal(body, &sn); err != nil {
		return nil, fmt.Errorf("%w: %v", errBadSnapshot, err)
	}
	if sn.State == nil || sn.Vector == nil || sn.Heads == nil {
		return nil, errBadSnapshot
	}
	for _, seq := range sn.Vector {
		if seq == 0 || seq > maxSeq {
			return nil, errBadSnapshot
		}
	}
	for _, e := range sn.Unknown {
		if err := e.Verify(); err != nil {
			return nil, fmt.Errorf("%w: %v", errBadSnapshot, err)
		}
	}
	sn.State.Normalize()
	return &sn, nil
}

func snapshotID(data []byte) string {
	return string(data[len(snapMagic) : len(snapMagic)+sumSize])
}

func snapName(gen uint64, copyName string) string {
	return fmt.Sprintf("%012d.%s%s", gen, copyName, snapSuffix)
}

func (s *Store) snapDir() string { return filepath.Join(s.dir, "snap") }

// snapGens lists snapshot generations, newest first.
func snapGens(fsys vfs.FS, dir string) ([]uint64, error) {
	names, err := fsys.List(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var gens []uint64
	for _, name := range names {
		base, ok := strings.CutSuffix(name, snapSuffix)
		if !ok {
			continue
		}
		num, _, ok := strings.Cut(base, ".")
		if !ok {
			continue
		}
		if n, err := strconv.ParseUint(num, 10, 64); err == nil && !slices.Contains(gens, n) {
			gens = append(gens, n)
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] > gens[j] })
	return gens, nil
}

// loadSnapshot starts from the newest snapshot and rewrites a missing or
// damaged copy. Older generations are only leftovers waiting to be removed. If
// no copy is readable it refuses to start: the log after the snapshot may hold
// events nothing else has, and replaying it without the snapshot would set them
// aside.
func (s *Store) loadSnapshot() (int, error) {
	gens, err := snapGens(s.fs, s.snapDir())
	if err != nil || len(gens) == 0 {
		return 0, err
	}
	gen, bad := gens[0], 0
	var good []byte
	var repair []string
	for _, c := range snapCopies {
		data, err := s.fs.ReadFile(filepath.Join(s.snapDir(), snapName(gen, c)))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			repair = append(repair, c)
		case err != nil:
			return bad, err
		default:
			if _, err := snapshotBody(data); err != nil {
				bad++
				repair = append(repair, c)
			} else if good == nil {
				good = data
			}
		}
	}
	if good == nil {
		return bad, fmt.Errorf("%w: nothing was changed. Move %s aside to rebuild from the other Mac", ErrSnapshotsDamaged, s.snapDir())
	}
	sn, err := decodeSnapshot(good)
	if err != nil {
		return bad, fmt.Errorf("%w: %v", ErrSnapshotsDamaged, err)
	}
	s.adopt(sn, gen)
	if !s.opt.ReadOnly {
		for _, c := range repair {
			if err := vfs.WriteAtomic(s.fs, filepath.Join(s.snapDir(), snapName(gen, c)), good); err != nil {
				return bad, err
			}
		}
	}
	return bad, nil
}

func (s *Store) adopt(sn *snapshot, gen uint64) {
	s.state = sn.State
	s.vector = maps(sn.Vector)
	s.base = maps(sn.Vector)
	s.heads = sn.Heads
	s.gen = gen
	s.sealedBefore = sn.SealedBefore
	s.clock.Observe(sn.Clock)
	s.unknown = nil
	for _, e := range sn.Unknown {
		if state.Knows(e) {
			s.state.Apply(e)
		} else {
			s.unknown = append(s.unknown, e)
		}
	}
}

func maps(v Vector) Vector {
	out := make(Vector, len(v))
	for k, n := range v {
		out[k] = n
	}
	return out
}

func seenBy(v Vector, unknown []event.Event) func(event.ID) bool {
	skip := make(map[event.ID]bool, len(unknown))
	for _, e := range unknown {
		skip[e.ID()] = true
	}
	return func(id event.ID) bool { return id.Seq <= v[id.Origin] && !skip[id] }
}

// writeSnapshot makes both copies durable before anything old is touched.
func (s *Store) writeSnapshot(sn snapshot) (int64, error) {
	sn.Clock = s.clock.Last()
	data, err := encodeSnapshot(sn)
	if err != nil {
		return 0, err
	}
	if err := s.fs.MkdirAll(s.snapDir()); err != nil {
		return 0, err
	}
	if err := s.fs.SyncDir(s.dir); err != nil {
		return 0, err
	}
	gen := s.gen + 1
	for _, c := range snapCopies {
		if err := vfs.WriteAtomic(s.fs, filepath.Join(s.snapDir(), snapName(gen, c)), data); err != nil {
			return 0, err
		}
	}
	s.gen = gen
	s.removeOldSnapshots()
	return int64(len(data)), nil
}

// removeOldSnapshots is best effort. A leftover costs disk until the next run.
func (s *Store) removeOldSnapshots() {
	names, err := s.fs.List(s.snapDir())
	if err != nil {
		return
	}
	current := []string{snapName(s.gen, "a"), snapName(s.gen, "b")}
	for _, name := range names {
		if !slices.Contains(current, name) {
			_ = s.fs.Remove(filepath.Join(s.snapDir(), name))
		}
	}
	_ = s.fs.SyncDir(s.snapDir())
}

// Compact saves the current state and removes old log files. A file goes only
// once the snapshot covers it and every vector in wait has all of it, so a
// peer that's keeping up never needs a full snapshot. Waiting is an
// optimization: a peer that fell behind merges the snapshot instead.
func (s *Store) Compact(wait []Vector) (CompactResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return CompactResult{}, ErrReadOnly
	}
	if err := s.log.Err(); err != nil {
		return CompactResult{}, err
	}
	if err := s.log.Roll(); err != nil {
		return CompactResult{}, err
	}
	s.state.Prune(seenBy(s.vector, s.unknown))
	size, err := s.writeSnapshot(snapshot{Vector: s.vector, Heads: s.heads, State: s.state, Unknown: s.unknown, SealedBefore: s.log.Active()})
	if err != nil {
		return CompactResult{}, err
	}
	s.base = maps(s.vector)
	s.since = 0
	s.sealedBefore = s.log.Active()
	res, err := s.dropSegments(wait)
	res.SnapshotBytes = size
	return res, err
}

func (s *Store) sealed() []uint32 {
	active := s.log.Active()
	out := make([]uint32, 0, len(s.segs))
	for seg := range s.segs {
		if seg < active {
			out = append(out, seg)
		}
	}
	slices.Sort(out)
	return out
}

func (s *Store) removable(seg uint32, wait []Vector) bool {
	for o, seq := range s.segs[seg].last {
		if seq > s.base[o] {
			return false
		}
		for _, v := range wait {
			if seq > v[o] {
				return false
			}
		}
	}
	return true
}

// dropSegments removes oldest first, so what's left of any origin is always a
// run of events ending at its newest.
func (s *Store) dropSegments(wait []Vector) (CompactResult, error) {
	var res CompactResult
	sealed := s.sealed()
	var last uint32
	var err error
	for _, seg := range sealed {
		if !s.removable(seg, wait) {
			break
		}
		size := s.segs[seg].bytes
		if err = s.log.Remove(seg); err != nil {
			break
		}
		delete(s.segs, seg)
		res.Removed++
		res.Freed += size
		last = seg
	}
	res.Held = len(sealed) - res.Removed
	if res.Removed > 0 {
		for o, idx := range s.index {
			n := 0
			for n < len(idx) && idx[n].Seg <= last {
				n++
			}
			if n > 0 {
				s.index[o] = slices.Clone(idx[n:])
			}
		}
	}
	return res, err
}

// NeedsCompaction reports whether enough new log has built up since the last
// snapshot, or an old file has become removable.
func (s *Store) NeedsCompaction(wait []Vector, after int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.log == nil {
		return false
	}
	if s.since >= after {
		return true
	}
	sealed := s.sealed()
	return len(sealed) > 0 && s.removable(sealed[0], wait)
}

// Refuses reports why a snapshot covering v can't be installed, so a peer
// doesn't download one only to reject it.
func (s *Store) Refuses(v Vector) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for o, seq := range v {
		if seq > s.vector[o] && s.forks[o] != "" {
			return fmt.Errorf("peer compacted past rejected writer %s, restart after fixing it", o)
		}
	}
	return nil
}

// SnapshotVector is what the newest snapshot covers.
func (s *Store) SnapshotVector() Vector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps(s.base)
}

// LogSize returns the bytes and number of log files holding events.
func (s *Store) LogSize() (int64, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, info := range s.segs {
		total += info.bytes
	}
	return total, len(s.segs)
}

// SnapshotChunk serves part of the newest snapshot. A non-empty id must still
// be current, or the caller has to start over.
func (s *Store) SnapshotChunk(id string, offset int64, n int) (string, int64, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.currentSnapshot()
	if err != nil {
		return "", 0, nil, err
	}
	cur := snapshotID(data)
	if id != "" && id != cur {
		return "", 0, nil, ErrSnapshotChanged
	}
	total := int64(len(data))
	if offset < 0 || offset >= total {
		return "", 0, nil, fmt.Errorf("snapshot offset %d out of range", offset)
	}
	return cur, total, data[offset:min(offset+int64(n), total)], nil
}

func (s *Store) currentSnapshot() ([]byte, error) {
	if s.gen == 0 {
		return nil, errors.New("no snapshot to send")
	}
	for _, c := range snapCopies {
		data, err := s.fs.ReadFile(filepath.Join(s.snapDir(), snapName(s.gen, c)))
		if err != nil {
			continue
		}
		if _, err := snapshotBody(data); err == nil {
			return data, nil
		}
	}
	return nil, errBadSnapshot
}

// sameHistory checks a snapshot against an origin this node is at least as far
// along on, where the event the snapshot ends at can still be looked up.
func (s *Store) sameHistory(o string, seq uint64, head string) error {
	switch floor := s.floor(o); {
	case seq == s.vector[o]:
		if head != s.heads[o] {
			return fmt.Errorf("%w at %s:%d", errFork, o, seq)
		}
	case seq > floor:
		rec, err := s.log.Read(s.index[o][seq-floor-1])
		if err != nil {
			return err
		}
		var e event.Event
		if err := json.Unmarshal(rec, &e); err != nil {
			return err
		}
		if e.Hash != head {
			return fmt.Errorf("%w at %s:%d", errFork, o, seq)
		}
	}
	return nil
}

// Install merges a peer's snapshot and returns how many events it covered that
// this node lacked. Local events it doesn't cover stay in the log and can still
// be served. The merge is durable before any of it becomes visible.
func (s *Store) Install(data []byte) (int, error) {
	sn, err := decodeSnapshot(data)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return 0, ErrReadOnly
	}
	if err := s.log.Err(); err != nil {
		return 0, err
	}
	var gained uint64
	for o, seq := range sn.Vector {
		if seq > s.vector[o] {
			if s.forks[o] != "" {
				return 0, fmt.Errorf("snapshot covers rejected writer %s", o)
			}
			gained += min(seq-s.vector[o], 1<<32)
			continue
		}
		if err := s.sameHistory(o, seq, sn.Heads[o]); err != nil {
			if errors.Is(err, errFork) {
				s.forks[o] = "snapshot disagrees with local history"
			}
			return 0, err
		}
	}
	if gained == 0 {
		return 0, nil
	}
	merged := s.state.Clone()
	merged.Merge(sn.State, seenBy(s.vector, s.unknown), seenBy(sn.Vector, sn.Unknown))
	vector, heads := maps(s.vector), make(map[string]string, len(s.heads))
	for o, h := range s.heads {
		heads[o] = h
	}
	var jumped []string
	for o, seq := range sn.Vector {
		if seq > vector[o] {
			vector[o], heads[o] = seq, sn.Heads[o]
			jumped = append(jumped, o)
		}
	}
	listed := make(map[event.ID]bool, len(sn.Unknown))
	for _, e := range sn.Unknown {
		listed[e.ID()] = true
	}
	var unknown []event.Event
	for _, e := range s.unknown {
		if e.Seq > sn.Vector[e.Origin] || listed[e.ID()] {
			unknown = append(unknown, e)
		}
	}
	for _, e := range sn.Unknown {
		switch {
		case e.Seq <= s.vector[e.Origin]:
		case state.Knows(e):
			merged.Apply(e)
		default:
			unknown = append(unknown, e)
		}
	}
	merged.Prune(seenBy(vector, unknown))
	s.clock.Observe(sn.Clock)
	active := s.log.Active()
	if _, err := s.writeSnapshot(snapshot{Vector: vector, Heads: heads, State: merged, Unknown: unknown, SealedBefore: active}); err != nil {
		return 0, err
	}
	s.state, s.vector, s.heads, s.unknown = merged, vector, heads, unknown
	s.base = maps(vector)
	s.since = 0
	s.sealedBefore = active
	for _, o := range jumped {
		delete(s.index, o)
		delete(s.top, o)
	}
	return int(gained), nil
}
