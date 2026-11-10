package hlc

import (
	"sync"
	"time"
)

// MaxDrift caps how far a remote timestamp can pull the local clock forward,
// so one Mac with a bad date can't poison the other.
const MaxDrift = int64(10 * time.Minute / time.Millisecond)

type T struct {
	Wall    int64  `json:"w"`
	Logical uint32 `json:"l,omitempty"`
}

func (t T) Compare(o T) int {
	switch {
	case t.Wall < o.Wall:
		return -1
	case t.Wall > o.Wall:
		return 1
	case t.Logical < o.Logical:
		return -1
	case t.Logical > o.Logical:
		return 1
	}
	return 0
}

type Clock struct {
	mu   sync.Mutex
	now  func() int64
	last T
}

func New(now func() int64) *Clock {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &Clock{now: now}
}

func (c *Clock) Now() T {
	c.mu.Lock()
	defer c.mu.Unlock()
	if wall := c.now(); wall > c.last.Wall {
		c.last = T{Wall: wall}
	} else {
		c.last.Logical++
	}
	return c.last
}

func (c *Clock) Last() T {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

func (c *Clock) Observe(t T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.Wall > c.now()+MaxDrift {
		return
	}
	if t.Compare(c.last) > 0 {
		c.last = t
	}
}
