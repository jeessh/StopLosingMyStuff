package hlc

import "testing"

func TestNeverGoesBackward(t *testing.T) {
	wall := int64(1000)
	c := New(func() int64 { return wall })
	prev := c.Now()
	for _, w := range []int64{1000, 900, 1500, 1} {
		wall = w
		next := c.Now()
		if next.Compare(prev) <= 0 {
			t.Fatalf("%v not after %v", next, prev)
		}
		prev = next
	}
}

func TestObserveMovesForward(t *testing.T) {
	c := New(func() int64 { return 1000 })
	c.Observe(T{Wall: 2000, Logical: 5})
	if got := c.Now(); got.Compare(T{Wall: 2000, Logical: 5}) <= 0 {
		t.Fatalf("got %v", got)
	}
}

func TestObserveIgnoresFarFuture(t *testing.T) {
	c := New(func() int64 { return 1000 })
	c.Observe(T{Wall: 1000 + MaxDrift + 1})
	if got := c.Now(); got.Wall != 1000 {
		t.Fatalf("clock was poisoned: %v", got)
	}
}
