package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const (
	headerSize = 8
	Overhead   = headerSize
	maxRecord  = 1 << 20
	suffix     = ".seg"
)

var (
	castagnoli  = crc32.MakeTable(crc32.Castagnoli)
	ErrBad      = errors.New("bad record")
	ErrVanished = errors.New("segment removed during scan")
)

type Loc struct {
	Seg uint32
	Off uint32
}

type Recovery struct {
	TornBytes    int
	Quarantined  int
	BadSnapshots int
}

type Visit func(Loc, []byte) error

type WAL struct {
	fs      vfs.FS
	dir     string
	limit   int64
	f       vfs.File
	seg     uint32
	size    int64
	failed  error
	cacheN  uint32
	cacheB  []byte
	scratch []byte
}

func segName(n uint32) string { return fmt.Sprintf("%012d%s", n, suffix) }

func segments(fsys vfs.FS, dir string) ([]uint32, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return nil, err
	}
	var out []uint32
	for _, name := range names {
		if num, ok := strings.CutSuffix(name, suffix); ok {
			n, err := strconv.ParseUint(num, 10, 32)
			if err != nil {
				continue
			}
			out = append(out, uint32(n))
		}
	}
	return out, nil
}

func scan(seg uint32, data []byte, visit Visit) (good int, clean, torn bool) {
	for off := 0; off < len(data); {
		rest := data[off:]
		if len(rest) < headerSize {
			return off, false, true
		}
		n := int(binary.BigEndian.Uint32(rest))
		sum := binary.BigEndian.Uint32(rest[4:])
		if n == 0 || n > maxRecord {
			return off, false, allZero(rest)
		}
		if len(rest) < headerSize+n {
			return off, false, true
		}
		body := rest[headerSize : headerSize+n]
		if crc32.Checksum(body, castagnoli) != sum {
			return off, false, len(rest) == headerSize+n
		}
		if visit != nil {
			if err := visit(Loc{seg, uint32(off)}, body); err != nil {
				return off, false, false
			}
		}
		off += headerSize + n
	}
	return len(data), true, false
}

// badPath never reuses a name, because segment numbers repeat after a repair.
func badPath(fsys vfs.FS, dir string, seg uint32) (string, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return "", err
	}
	for i := 1; ; i++ {
		name := fmt.Sprintf("%s.bad.%d", segName(seg), i)
		if !slices.Contains(names, name) {
			return filepath.Join(dir, name), nil
		}
	}
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Open replays every valid record and repairs the log. A torn tail is cut.
// Anything else stops replay there and moves the remainder aside, since records
// after a hole can't be trusted to be contiguous. Segments below covered are
// already in a snapshot, so damage there only costs that segment's remainder.
func Open(fsys vfs.FS, dir string, limit int64, covered uint32, visit Visit) (*WAL, Recovery, error) {
	var rec Recovery
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, rec, err
	}
	if err := fsys.SyncDir(filepath.Dir(dir)); err != nil {
		return nil, rec, err
	}
	segs, err := segments(fsys, dir)
	if err != nil {
		return nil, rec, err
	}
	w := &WAL{fs: fsys, dir: dir, limit: limit}
	for i, n := range segs {
		path := filepath.Join(dir, segName(n))
		data, err := fsys.ReadFile(path)
		if err != nil {
			return nil, rec, err
		}
		good, clean, torn := scan(n, data, visit)
		if clean {
			continue
		}
		last := i == len(segs)-1
		if last && torn {
			rec.TornBytes = len(data) - good
		} else {
			rec.Quarantined += len(data) - good
		}
		bad, err := badPath(fsys, dir, n)
		if err != nil {
			return nil, rec, err
		}
		if err := vfs.WriteAtomic(fsys, bad, data[good:]); err != nil {
			return nil, rec, err
		}
		if err := truncate(fsys, path, int64(good)); err != nil {
			return nil, rec, err
		}
		if n < covered && !last {
			continue
		}
		for _, later := range segs[i+1:] {
			p := filepath.Join(dir, segName(later))
			if b, err := fsys.ReadFile(p); err == nil {
				rec.Quarantined += len(b)
			}
			bad, err := badPath(fsys, dir, later)
			if err != nil {
				return nil, rec, err
			}
			if err := fsys.Rename(p, bad); err != nil {
				return nil, rec, err
			}
		}
		if err := fsys.SyncDir(dir); err != nil {
			return nil, rec, err
		}
		segs = segs[:i+1]
		break
	}
	if len(segs) == 0 {
		if err := w.roll(1); err != nil {
			return nil, rec, err
		}
		return w, rec, nil
	}
	w.seg = segs[len(segs)-1]
	path := filepath.Join(dir, segName(w.seg))
	data, err := fsys.ReadFile(path)
	if err != nil {
		return nil, rec, err
	}
	if w.f, err = fsys.Append(path); err != nil {
		return nil, rec, err
	}
	w.size = int64(len(data))
	return w, rec, nil
}

// Scan reads without repairing, so it is safe alongside a live writer.
func Scan(fsys vfs.FS, dir string, covered uint32, visit Visit) error {
	segs, err := segments(fsys, dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, n := range segs {
		data, err := fsys.ReadFile(filepath.Join(dir, segName(n)))
		if errors.Is(err, fs.ErrNotExist) {
			return ErrVanished
		}
		if err != nil {
			return err
		}
		if _, clean, _ := scan(n, data, visit); !clean && n >= covered {
			return nil
		}
	}
	return nil
}

func truncate(fsys vfs.FS, path string, size int64) error {
	f, err := fsys.Append(path)
	if err != nil {
		return err
	}
	if err = f.Truncate(size); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (w *WAL) roll(n uint32) error {
	if w.f != nil {
		if err := w.f.Close(); err != nil {
			return err
		}
		w.f = nil
	}
	f, err := w.fs.Append(filepath.Join(w.dir, segName(n)))
	if err != nil {
		return err
	}
	if err := w.fs.SyncDir(w.dir); err != nil {
		_ = f.Close()
		return err
	}
	w.f, w.seg, w.size = f, n, 0
	return nil
}

// Append writes and fsyncs records as one batch. After any failure the WAL
// refuses further writes: a failed fsync leaves page cache state unknowable,
// so the only safe move is to reopen and replay.
func (w *WAL) Append(records [][]byte) ([]Loc, error) {
	if w.failed != nil {
		return nil, w.failed
	}
	total := int64(0)
	for _, r := range records {
		if len(r) == 0 || len(r) > maxRecord {
			return nil, fmt.Errorf("record size %d out of range", len(r))
		}
		total += int64(headerSize + len(r))
	}
	if w.size > 0 && w.size+total > w.limit {
		if err := w.roll(w.seg + 1); err != nil {
			w.failed = err
			return nil, err
		}
	}
	buf := w.scratch[:0]
	locs := make([]Loc, len(records))
	off := w.size
	for i, r := range records {
		locs[i] = Loc{w.seg, uint32(off)}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(r)))
		buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(r, castagnoli))
		buf = append(buf, r...)
		off += int64(headerSize + len(r))
	}
	w.scratch = buf
	if _, err := w.f.Write(buf); err != nil {
		w.failed = err
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		w.failed = err
		return nil, err
	}
	w.size = off
	return locs, nil
}

// Roll seals the current segment so compaction can remove it later.
func (w *WAL) Roll() error {
	if w.failed != nil {
		return w.failed
	}
	if w.size == 0 {
		return nil
	}
	if err := w.roll(w.seg + 1); err != nil {
		w.failed = err
		return err
	}
	return nil
}

// Active is the segment still being written, which can't be removed.
func (w *WAL) Active() uint32 { return w.seg }

// Remove deletes a sealed segment and syncs the directory, so a crash can't
// bring back a later segment without the earlier one. Failure is sticky for
// the same reason a failed fsync is.
func (w *WAL) Remove(seg uint32) error {
	if w.failed != nil {
		return w.failed
	}
	if seg >= w.seg {
		return fmt.Errorf("segment %d is still active", seg)
	}
	err := w.fs.Remove(filepath.Join(w.dir, segName(seg)))
	if err == nil {
		err = w.fs.SyncDir(w.dir)
	}
	if err != nil {
		w.failed = err
		return err
	}
	if w.cacheN == seg {
		w.cacheN, w.cacheB = 0, nil
	}
	return nil
}

func (w *WAL) Read(l Loc) ([]byte, error) {
	if w.cacheN != l.Seg || int(l.Off)+headerSize > len(w.cacheB) || w.short(l) {
		data, err := w.fs.ReadFile(filepath.Join(w.dir, segName(l.Seg)))
		if err != nil {
			return nil, err
		}
		w.cacheN, w.cacheB = l.Seg, data
	}
	if int(l.Off) > len(w.cacheB) {
		return nil, ErrBad
	}
	rest := w.cacheB[l.Off:]
	if len(rest) < headerSize {
		return nil, ErrBad
	}
	n := int(binary.BigEndian.Uint32(rest))
	if n == 0 || n > maxRecord || len(rest) < headerSize+n {
		return nil, ErrBad
	}
	body := rest[headerSize : headerSize+n]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(rest[4:]) {
		return nil, ErrBad
	}
	return body, nil
}

func (w *WAL) short(l Loc) bool {
	rest := w.cacheB[l.Off:]
	return len(rest) < headerSize+int(binary.BigEndian.Uint32(rest))
}

func (w *WAL) Err() error { return w.failed }

func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}
