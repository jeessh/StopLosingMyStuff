package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const dir = "/log"

func collect(t *testing.T, fsys vfs.FS, limit int64) (*WAL, [][]byte, Recovery) {
	t.Helper()
	var got [][]byte
	w, rec, err := Open(fsys, dir, limit, func(_ Loc, b []byte) error {
		got = append(got, bytes.Clone(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return w, got, rec
}

func rec(i int) []byte { return []byte(fmt.Sprintf("record-%06d", i)) }

func TestSyncedRecordsSurviveAnyCrash(t *testing.T) {
	for seed := range uint64(500) {
		r := rand.New(rand.NewPCG(seed, 1))
		m := vfs.NewMem()
		next := 0
		for range 8 {
			w, got, _ := collect(t, m, 128)
			if len(got) > next || !prefixOK(got, next) {
				t.Fatalf("seed %d: got %d records, acked %d", seed, len(got), next)
			}
			for j := 0; j < r.IntN(12); j++ {
				if _, err := w.Append([][]byte{rec(next)}); err != nil {
					t.Fatal(err)
				}
				next++
			}
			f, _ := m.Append(dir + "/" + segName(w.seg))
			_, _ = f.Write(rec(99)[:r.IntN(10)+1])
			m.Crash(r)
		}
		_, got, _ := collect(t, m, 128)
		if len(got) != next {
			t.Fatalf("seed %d: lost records, got %d want %d", seed, len(got), next)
		}
	}
}

func prefixOK(got [][]byte, n int) bool {
	for i, b := range got {
		if i >= n || !bytes.Equal(b, rec(i)) {
			return false
		}
	}
	return true
}

func TestReopenAfterTornTailAcceptsNewWrites(t *testing.T) {
	m := vfs.NewMem()
	w, _, _ := collect(t, m, 1<<20)
	if _, err := w.Append([][]byte{rec(0)}); err != nil {
		t.Fatal(err)
	}
	f, _ := m.Append(dir + "/" + segName(1))
	_, _ = f.Write([]byte{0, 0, 0, 200, 1, 2, 3})
	_ = f.Sync()
	w, got, r := collect(t, m, 1<<20)
	if len(got) != 1 || r.TornBytes != 7 {
		t.Fatalf("got %d records, torn %d", len(got), r.TornBytes)
	}
	if _, err := w.Append([][]byte{rec(1)}); err != nil {
		t.Fatal(err)
	}
	if _, got, _ = collect(t, m, 1<<20); len(got) != 2 {
		t.Fatalf("got %d records after repair", len(got))
	}
}

func TestMidLogCorruptionStopsAndQuarantines(t *testing.T) {
	m := vfs.NewMem()
	w, _, _ := collect(t, m, 64)
	for i := range 20 {
		if _, err := w.Append([][]byte{rec(i)}); err != nil {
			t.Fatal(err)
		}
	}
	path := dir + "/" + segName(2)
	data, _ := m.ReadFile(path)
	data[headerSize+1] ^= 0xff
	f, _ := m.Create(path)
	_, _ = f.Write(data)
	_ = f.Sync()
	_ = m.SyncDir(dir)

	_, got, r := collect(t, m, 64)
	if !prefixOK(got, 20) || len(got) >= 20 || r.Quarantined == 0 {
		t.Fatalf("got %d records, quarantined %d", len(got), r.Quarantined)
	}
	names, _ := m.List(dir)
	bad := 0
	for _, n := range names {
		if bytes.Contains([]byte(n), []byte(".bad.")) {
			bad++
		}
	}
	if bad == 0 {
		t.Fatal("corrupt data was not kept for inspection")
	}
}

func TestFailedSyncPoisonsLog(t *testing.T) {
	m := vfs.NewMem()
	w, _, _ := collect(t, m, 1<<20)
	m.FailNextSyncs(1)
	if _, err := w.Append([][]byte{rec(0)}); !errors.Is(err, vfs.ErrIO) {
		t.Fatalf("err = %v", err)
	}
	if _, err := w.Append([][]byte{rec(1)}); !errors.Is(err, vfs.ErrIO) {
		t.Fatalf("log accepted a write after failed fsync: %v", err)
	}
}

func TestNewSegmentNameIsDurable(t *testing.T) {
	m := vfs.NewMem()
	w, _, _ := collect(t, m, 32)
	for i := range 6 {
		if _, err := w.Append([][]byte{rec(i)}); err != nil {
			t.Fatal(err)
		}
	}
	m.Crash(rand.New(rand.NewPCG(1, 2)))
	if _, got, _ := collect(t, m, 32); len(got) != 6 {
		t.Fatalf("got %d records", len(got))
	}
}

func TestCrashAtEveryStepKeepsAcknowledgedRecords(t *testing.T) {
	for seed := range uint64(400) {
		r := rand.New(rand.NewPCG(seed, 3))
		m := vfs.NewMem()
		acked := 0
		for range 6 {
			func() {
				defer func() {
					if v := recover(); v != nil && v != vfs.ErrCrash {
						panic(v)
					}
				}()
				m.CrashAfter(1 + r.IntN(30))
				w, got, _ := collect(t, m, 96)
				if len(got) < acked || !prefixOK(got, len(got)) {
					t.Fatalf("seed %d: got %d records, acked %d", seed, len(got), acked)
				}
				acked = len(got)
				for acked < 500 {
					if _, err := w.Append([][]byte{rec(acked)}); err != nil {
						t.Fatal(err)
					}
					acked++
				}
			}()
			m.Crash(r)
		}
		m.CrashAfter(0)
		_, got, _ := collect(t, m, 96)
		if len(got) < acked || !prefixOK(got, len(got)) {
			t.Fatalf("seed %d: got %d records, acked %d", seed, len(got), acked)
		}
	}
}

func TestDiskFullLeavesNothingVisibleAndRecovers(t *testing.T) {
	m := vfs.NewMem()
	w, _, _ := collect(t, m, 1<<20)
	if _, err := w.Append([][]byte{rec(0)}); err != nil {
		t.Fatal(err)
	}
	m.FailNextWrites(1)
	if _, err := w.Append([][]byte{rec(1)}); err == nil {
		t.Fatal("write on a full disk succeeded")
	}
	w, got, _ := collect(t, m, 1<<20)
	if len(got) != 1 {
		t.Fatalf("got %d records", len(got))
	}
	if _, err := w.Append([][]byte{rec(1)}); err != nil {
		t.Fatal(err)
	}
}

func TestCutBytesAreAlwaysSavedAndNeverOverwritten(t *testing.T) {
	m := vfs.NewMem()
	for round := range 2 {
		w, _, _ := collect(t, m, 1<<20)
		if _, err := w.Append([][]byte{rec(round)}); err != nil {
			t.Fatal(err)
		}
		f, _ := m.Append(dir + "/" + segName(1))
		_, _ = f.Write([]byte{0, 0, 3, 0, 1, 2})
		_ = f.Sync()
		collect(t, m, 1<<20)
	}
	names, _ := m.List(dir)
	bad := 0
	for _, n := range names {
		if bytes.Contains([]byte(n), []byte(".bad.")) {
			bad++
		}
	}
	if bad != 2 {
		t.Fatalf("got %d saved files in %v", bad, names)
	}
}
