package vfs

import (
	"bytes"
	"errors"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var ErrIO = errors.New("simulated io error")

type inode struct {
	data   []byte
	synced []byte
}

// MemFS keeps unsynced data and directory entries separate from what would
// survive a power cut, so Crash can lose exactly what a real disk may lose.
type MemFS struct {
	mu        sync.Mutex
	live      map[string]*inode
	durable   map[string]*inode
	failSync  int
	failWrite int
	stepsLeft int
}

// ErrCrash is the panic value when a planned crash point is reached.
var ErrCrash = errors.New("simulated crash")

func (m *MemFS) step() {
	if m.stepsLeft > 0 {
		if m.stepsLeft--; m.stepsLeft == 0 {
			panic(ErrCrash)
		}
	}
}

// CrashAfter makes the nth mutating call panic, to crash inside an operation.
func (m *MemFS) CrashAfter(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stepsLeft = n
}

// FailNextWrites makes writes save half their data and return ErrIO, like a
// disk that just filled up.
func (m *MemFS) FailNextWrites(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failWrite = n
}

func NewMem() *MemFS {
	return &MemFS{live: map[string]*inode{}, durable: map[string]*inode{}}
}

func (m *MemFS) MkdirAll(string) error { return nil }

type memFile struct {
	m      *MemFS
	n      *inode
	closed bool
}

func (m *MemFS) Append(path string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.live[path]
	if n == nil {
		n = &inode{}
		m.live[path] = n
	}
	return &memFile{m: m, n: n}, nil
}

func (m *MemFS) Create(path string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := &inode{}
	m.live[path] = n
	return &memFile{m: m, n: n}, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	f.m.step()
	if f.m.failWrite > 0 {
		f.m.failWrite--
		f.n.data = append(f.n.data, p[:len(p)/2]...)
		return len(p) / 2, ErrIO
	}
	f.n.data = append(f.n.data, p...)
	return len(p), nil
}

func (f *memFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	f.m.step()
	if f.m.failSync > 0 {
		f.m.failSync--
		return ErrIO
	}
	f.n.synced = bytes.Clone(f.n.data)
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	f.m.step()
	f.n.data = f.n.data[:size:size]
	return nil
}

func (f *memFile) Close() error { return nil }

func (m *MemFS) ReadFile(path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.live[path]
	if n == nil {
		return nil, fs.ErrNotExist
	}
	return bytes.Clone(n.data), nil
}

func (m *MemFS) Rename(from, to string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.step()
	n := m.live[from]
	if n == nil {
		return fs.ErrNotExist
	}
	delete(m.live, from)
	m.live[to] = n
	return nil
}

func (m *MemFS) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.step()
	if m.live[path] == nil {
		return fs.ErrNotExist
	}
	delete(m.live, path)
	return nil
}

func (m *MemFS) List(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for p := range m.live {
		if filepath.Dir(p) == dir {
			names = append(names, filepath.Base(p))
		}
	}
	sort.Strings(names)
	return names, nil
}

func (m *MemFS) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.step()
	for p := range m.durable {
		if filepath.Dir(p) == dir && m.live[p] == nil {
			delete(m.durable, p)
		}
	}
	for p, n := range m.live {
		if filepath.Dir(p) == dir {
			m.durable[p] = n
		}
	}
	return nil
}

func (m *MemFS) FailNextSyncs(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failSync = n
}

// Crash replaces the filesystem with one possible state after a power cut:
// unsynced names vanish and unsynced appends survive partly, sometimes garbled.
func (m *MemFS) Crash(r *rand.Rand) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failSync, m.failWrite, m.stepsLeft = 0, 0, 0
	live := map[string]*inode{}
	durable := map[string]*inode{}
	seen := map[*inode]*inode{}
	paths := make([]string, 0, len(m.durable))
	for p := range m.durable {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		n := m.durable[p]
		c := seen[n]
		if c == nil {
			b := afterCrash(n, r)
			c = &inode{data: b, synced: bytes.Clone(b)}
			seen[n] = c
		}
		live[p], durable[p] = c, c
	}
	m.live, m.durable = live, durable
}

func afterCrash(n *inode, r *rand.Rand) []byte {
	if !bytes.HasPrefix(n.data, n.synced) {
		if r.IntN(2) == 0 {
			return bytes.Clone(n.data)
		}
		return bytes.Clone(n.synced)
	}
	extra := n.data[len(n.synced):]
	keep := 0
	if len(extra) > 0 {
		keep = r.IntN(len(extra) + 1)
	}
	out := append(bytes.Clone(n.synced), extra[:keep]...)
	switch {
	case keep > 0 && r.IntN(3) == 0:
		out[len(out)-1-r.IntN(keep)] ^= byte(1 + r.IntN(255))
	case len(extra) > 0 && r.IntN(4) == 0:
		out = append(out, make([]byte, 1+r.IntN(64))...)
	}
	return out
}

// Clone returns what a backup taken right now would restore.
func (m *MemFS) Clone() *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := NewMem()
	for p, n := range m.durable {
		b := &inode{data: bytes.Clone(n.synced), synced: bytes.Clone(n.synced)}
		c.live[p], c.durable[p] = b, b
	}
	return c
}

// RotAll damages every durable file whose name ends in suffix.
func (m *MemFS) RotAll(suffix string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for p, n := range m.durable {
		if strings.HasSuffix(p, suffix) && len(n.synced) > 0 {
			n.synced[len(n.synced)-1] ^= 1
			n.data = bytes.Clone(n.synced)
			found = true
		}
	}
	return found
}

// Rot flips one durable byte in a random file whose name ends in suffix.
func (m *MemFS) Rot(r *rand.Rand, suffix string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	var paths []string
	for p, n := range m.durable {
		if strings.HasSuffix(p, suffix) && len(n.synced) > 0 {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		return false
	}
	sort.Strings(paths)
	n := m.durable[paths[r.IntN(len(paths))]]
	i := r.IntN(len(n.synced))
	n.synced[i] ^= byte(1 + r.IntN(255))
	n.data = bytes.Clone(n.synced)
	return true
}
