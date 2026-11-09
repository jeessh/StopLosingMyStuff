package daemon

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/capture"
)

func TestBoundKeepsRecordsSmall(t *testing.T) {
	long := strings.Repeat("x", 3000)
	s := capture.Session{ID: "s", Prompt: long, Title: long, LastError: long}
	for range 200 {
		s.Files = append(s.Files, long)
		s.Commands = append(s.Commands, long)
	}
	raw, err := json.Marshal(bound(s))
	if err != nil || len(raw) >= maxRecordBytes {
		t.Fatalf("len %d, err %v", len(raw), err)
	}
}

func TestEligible(t *testing.T) {
	now := time.Now()
	idle := now.Add(-time.Hour).UnixMilli()
	ok := func(c capture.Cursor) bool { return eligible(&c, now) }
	s := &capture.Session{Turns: 3, Last: idle}
	cases := []struct {
		name string
		c    capture.Cursor
		want bool
	}{
		{"quiet session", capture.Cursor{Session: s}, true},
		{"nothing new", capture.Cursor{Session: s, SummaryOf: idle}, false},
		{"waiting to retry", capture.Cursor{Session: s, SummaryNext: now.Add(time.Hour).UnixMilli()}, false},
		{"gave up", capture.Cursor{Session: s, SummaryTry: summaryTries, SummaryFail: idle}, false},
		{"new work after giving up", capture.Cursor{Session: s, SummaryTry: summaryTries, SummaryFail: idle - 1}, true},
		{"too short", capture.Cursor{Session: &capture.Session{Turns: 1, Last: idle}}, false},
		{"still going", capture.Cursor{Session: &capture.Session{Turns: 3, Last: now.UnixMilli()}}, false},
		{"ended just now", capture.Cursor{Session: &capture.Session{Turns: 3, Last: now.UnixMilli(), Ended: true}}, true},
		{"ancient", capture.Cursor{Session: &capture.Session{Turns: 3, Last: now.Add(-48 * time.Hour).UnixMilli()}}, false},
	}
	for _, c := range cases {
		if got := ok(c.c); got != c.want {
			t.Errorf("%s: got %v", c.name, got)
		}
	}
}
