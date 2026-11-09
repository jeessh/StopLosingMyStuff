package replica

import (
	"context"
	"errors"
	"fmt"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
)

const (
	Proto     = 1
	batchSize = 512
	batchByte = 4 << 20
)

var ErrProto = errors.New("protocol version mismatch")

type Hello struct {
	Node   string       `json:"node"`
	Name   string       `json:"name"`
	Proto  int          `json:"proto"`
	Vector store.Vector `json:"vector"`
}

type Batch struct {
	Events []event.Event `json:"events"`
	More   bool          `json:"more"`
}

// Peer is pull only: each side asks for what it lacks, so a retry after any
// failure is just the same request again.
type Peer interface {
	Hello(ctx context.Context, me Hello) (Hello, error)
	Pull(ctx context.Context, have store.Vector) (Batch, error)
}

func HelloFrom(s *store.Store) Hello {
	return Hello{Node: s.NodeID(), Name: s.Name(), Proto: Proto, Vector: s.Vector()}
}

// Pull copies everything the peer has that s lacks.
func Pull(ctx context.Context, s *store.Store, p Peer) (Hello, int, error) {
	them, err := p.Hello(ctx, HelloFrom(s))
	if err != nil {
		return Hello{}, 0, err
	}
	if them.Proto != Proto {
		return them, 0, fmt.Errorf("%w: peer %d, local %d", ErrProto, them.Proto, Proto)
	}
	total := 0
	var applyErr error
	for {
		batch, err := p.Pull(ctx, s.Vector())
		if err != nil {
			return them, total, err
		}
		n, err := s.Apply(batch.Events)
		total += n
		if err != nil && applyErr == nil {
			applyErr = err
		}
		if !batch.More || n == 0 {
			return them, total, applyErr
		}
	}
}

// Local answers peer requests from a store.
type Local struct {
	Store   *store.Store
	OnHello func(Hello)
}

func (l Local) Hello(_ context.Context, them Hello) (Hello, error) {
	if l.OnHello != nil {
		l.OnHello(them)
	}
	return HelloFrom(l.Store), nil
}

func (l Local) Pull(_ context.Context, have store.Vector) (Batch, error) {
	events, more, err := l.Store.Since(have, batchSize, batchByte)
	if err != nil {
		return Batch{}, err
	}
	return Batch{Events: events, More: more}, nil
}
