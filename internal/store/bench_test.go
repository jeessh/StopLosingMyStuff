package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

func body(i int) event.Body {
	b, _ := Set("github.com/me/project", fmt.Sprintf("session/%d", i%400), strings.Repeat("x", 2048), nil)
	return b
}

func BenchmarkCommitRealDisk(b *testing.B) {
	s, err := Open(vfs.OS{}, b.TempDir(), Options{Name: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ResetTimer()
	for i := range b.N {
		if _, err := s.Commit(body(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecoverHundredThousandEvents(b *testing.B) {
	dir := b.TempDir()
	s, err := Open(vfs.OS{}, dir, Options{Name: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100_000; i += 500 {
		batch := make([]event.Body, 500)
		for j := range batch {
			batch[j] = body(i + j)
		}
		if _, err := s.Commit(batch...); err != nil {
			b.Fatal(err)
		}
	}
	s.Close()
	b.ResetTimer()
	for range b.N {
		s, err := Open(vfs.OS{}, dir, Options{Name: "bench", ReadOnly: true})
		if err != nil {
			b.Fatal(err)
		}
		if len(s.Vector()) != 1 {
			b.Fatal("missing events")
		}
	}
}

func BenchmarkReplicateTenThousandEvents(b *testing.B) {
	src, err := Open(vfs.OS{}, b.TempDir(), Options{Name: "src"})
	if err != nil {
		b.Fatal(err)
	}
	defer src.Close()
	for i := 0; i < 10_000; i += 500 {
		batch := make([]event.Body, 500)
		for j := range batch {
			batch[j] = body(i + j)
		}
		if _, err := src.Commit(batch...); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		dst, err := Open(vfs.OS{}, b.TempDir(), Options{Name: "dst"})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		have := Vector{}
		for {
			events, more, err := src.Since(have, 512, 4<<20)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := dst.Apply(events); err != nil {
				b.Fatal(err)
			}
			have = dst.Vector()
			if !more {
				break
			}
		}
		b.StopTimer()
		dst.Close()
		b.StartTimer()
	}
}
