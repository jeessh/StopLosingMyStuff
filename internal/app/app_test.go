package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/brief"
	"github.com/jeessh/StopLosingMyStuff/internal/replica"
	"github.com/jeessh/StopLosingMyStuff/internal/state"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

func newApp(t *testing.T, name string) *App {
	t.Helper()
	m := vfs.NewMem()
	s, err := store.Open(m, "/d", store.Options{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	return &App{Store: s, FS: m, Dir: "/d", Home: "/nope", Now: time.Now}
}

func values(a *App, key string) []string {
	var out []string
	a.Store.View(func(s *state.State) {
		for _, w := range s.Projects["p"].Get(key) {
			var t brief.Text
			_ = json.Unmarshal(w.Value, &t)
			out = append(out, t.Text)
		}
	})
	return out
}

func exchange(t *testing.T, a, b *App) {
	t.Helper()
	ctx := context.Background()
	for _, p := range [][2]*App{{a, b}, {b, a}} {
		if _, _, err := replica.Pull(ctx, p[0].Store, replica.Local{Store: p[1].Store}); err != nil {
			t.Fatal(err)
		}
	}
}

func conflict(t *testing.T) (*App, *App) {
	t.Helper()
	a, b := newApp(t, "a"), newApp(t, "b")
	for _, x := range []*App{a, b} {
		if err := x.PutNewer("p", "next", brief.Text{Text: "from " + x.Store.Name(), Node: x.Store.Name(), AsOf: 10}); err != nil {
			t.Fatal(err)
		}
	}
	exchange(t, a, b)
	if got := values(a, "next"); len(got) != 2 {
		t.Fatalf("no conflict: %v", got)
	}
	return a, b
}

func TestSummaryCannotEraseAnOpenConflict(t *testing.T) {
	a, b := conflict(t)
	if err := a.PutNewer("p", "next", brief.Text{Text: "a again", Node: "a", AsOf: 20}); err != nil {
		t.Fatal(err)
	}
	exchange(t, a, b)
	got := strings.Join(values(b, "next"), "|")
	if !strings.Contains(got, "from b") || !strings.Contains(got, "a again") || strings.Contains(got, "from a") {
		t.Fatalf("got %s", got)
	}
}

func TestOlderSummaryCannotReplaceNewerOne(t *testing.T) {
	a := newApp(t, "a")
	_ = a.PutNewer("p", "next", brief.Text{Text: "new", Node: "a", AsOf: 100})
	_ = a.PutNewer("p", "next", brief.Text{Text: "old", Node: "a", AsOf: 50})
	if got := values(a, "next"); len(got) != 1 || got[0] != "new" {
		t.Fatalf("got %v", got)
	}
}

func TestFutureDatedSummaryDoesNotBlockUpdates(t *testing.T) {
	a := newApp(t, "a")
	future := time.Now().Add(24 * time.Hour).UnixMilli()
	_ = a.PutNewer("p", "next", brief.Text{Text: "from bad clock", Node: "b", AsOf: future})
	_ = a.PutNewer("p", "next", brief.Text{Text: "real", Node: "a", AsOf: time.Now().UnixMilli()})
	if got := values(a, "next"); len(got) != 1 || got[0] != "real" {
		t.Fatalf("got %v", got)
	}
}

func TestRepeatedDecisionsCollapse(t *testing.T) {
	a := newApp(t, "a")
	for range 3 {
		if err := a.AddDecisions("p", "s1", []string{"use go"}, brief.Text{Node: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	a.Store.View(func(s *state.State) { n = len(s.Projects["p"].Items("decision")) })
	if n != 1 {
		t.Fatalf("got %d decisions", n)
	}
}

func TestPutSkipsUnchangedValues(t *testing.T) {
	a := newApp(t, "a")
	same := func(json.RawMessage) bool { return true }
	_ = a.Put("p", "k", 1, same)
	before := a.Store.Vector()
	_ = a.Put("p", "k", 1, same)
	if a.Store.Missing(before) != 0 {
		t.Fatal("wrote an unchanged value")
	}
}

func TestNotesAreRedacted(t *testing.T) {
	a := newApp(t, "a")
	if err := a.Note(context.Background(), "/nope/x", "db password="+"hunter2"+"hunter"); err != nil {
		t.Fatal(err)
	}
	if out := a.Brief(context.Background(), "/nope/x"); out != "" && strings.Contains(out, "hunter2") {
		t.Fatalf("leaked: %s", out)
	}
	var found bool
	a.Store.View(func(s *state.State) {
		for _, p := range s.Projects {
			for _, w := range p.Items("decision") {
				found = true
				if strings.Contains(string(w.Value), "hunter2") {
					t.Errorf("stored secret: %s", w.Value)
				}
			}
		}
	})
	if !found {
		t.Fatal("note was not stored")
	}
}
