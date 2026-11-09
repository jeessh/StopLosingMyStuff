package event

import (
	"strings"
	"testing"

	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
)

func sealed() Event {
	e := Event{Origin: "a", Seq: 1, Prev: "p", Time: hlc.T{Wall: 5}, Body: []byte(`{}`)}
	e.Seal()
	return e
}

func TestVerifyCatchesAnyChange(t *testing.T) {
	if err := sealed().Verify(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Event){
		"body":   func(e *Event) { e.Body = []byte(`{"x":1}`) },
		"seq":    func(e *Event) { e.Seq = 2 },
		"prev":   func(e *Event) { e.Prev = "q" },
		"origin": func(e *Event) { e.Origin = "b" },
		"time":   func(e *Event) { e.Time.Logical = 1 },
	} {
		e := sealed()
		change(&e)
		if e.Verify() == nil {
			t.Errorf("changed %s went unnoticed", name)
		}
	}
}

func TestVerifyEnforcesLimits(t *testing.T) {
	e := sealed()
	e.Body = []byte(strings.Repeat("x", MaxBody+1))
	e.Seal()
	if e.Verify() == nil {
		t.Fatal("oversized body accepted")
	}
	e = sealed()
	e.Seq = 0
	e.Seal()
	if e.Verify() == nil {
		t.Fatal("seq 0 accepted")
	}
}
