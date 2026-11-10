package replica

import (
	"context"
	"errors"
	"fmt"

	"github.com/jeessh/StopLosingMyStuff/internal/event"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
)

const (
	Proto       = 2
	batchSize   = 512
	batchByte   = 4 << 20
	chunkSize   = 4 << 20
	maxInstalls = 3
	maxSnapshot = 1 << 30
)

var ErrProto = errors.New("protocol version mismatch")

type Hello struct {
	Node   string       `json:"node"`
	Name   string       `json:"name"`
	Proto  int          `json:"proto"`
	Vector store.Vector `json:"vector"`
}

// Batch.Snapshot means the events asked for were compacted away, so the
// requester has to fetch the snapshot before pulling again.
type Batch struct {
	Events   []event.Event `json:"events"`
	More     bool          `json:"more"`
	Snapshot bool          `json:"snapshot,omitempty"`
	Covers   store.Vector  `json:"covers,omitempty"`
}

type Chunk struct {
	ID    string `json:"id"`
	Total int64  `json:"total"`
	Data  []byte `json:"data"`
}

// Peer is pull only: each side asks for what it lacks, so a retry after any
// failure is just the same request again.
type Peer interface {
	Hello(ctx context.Context, me Hello) (Hello, error)
	Pull(ctx context.Context, have store.Vector) (Batch, error)
	Snapshot(ctx context.Context, id string, offset int64) (Chunk, error)
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
	installs := 0
	for {
		batch, err := p.Pull(ctx, s.Vector())
		if err != nil {
			return them, total, err
		}
		if batch.Snapshot {
			if installs++; installs > maxInstalls {
				return them, total, errors.New("peer keeps asking for a snapshot")
			}
			if err := s.Refuses(batch.Covers); err != nil {
				return them, total, err
			}
			data, err := fetchSnapshot(ctx, p)
			if err != nil {
				return them, total, err
			}
			n, err := s.Install(data)
			if err != nil {
				return them, total, err
			}
			total += n
			continue
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

func fetchSnapshot(ctx context.Context, p Peer) ([]byte, error) {
	var buf []byte
	id := ""
	for {
		c, err := p.Snapshot(ctx, id, int64(len(buf)))
		if err != nil {
			return nil, err
		}
		if c.Total <= 0 || c.Total > maxSnapshot {
			return nil, fmt.Errorf("snapshot of %d bytes refused", c.Total)
		}
		if id != "" && c.ID != id {
			return nil, errors.New("snapshot changed during transfer")
		}
		if len(c.Data) == 0 || int64(len(buf)+len(c.Data)) > c.Total {
			return nil, errors.New("bad snapshot chunk")
		}
		id = c.ID
		buf = append(buf, c.Data...)
		if int64(len(buf)) == c.Total {
			return buf, nil
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
	if errors.Is(err, store.ErrSnapshotNeeded) {
		return Batch{Snapshot: true, Covers: l.Store.SnapshotVector()}, nil
	}
	if err != nil {
		return Batch{}, err
	}
	return Batch{Events: events, More: more}, nil
}

func (l Local) Snapshot(_ context.Context, id string, offset int64) (Chunk, error) {
	got, total, data, err := l.Store.SnapshotChunk(id, offset, chunkSize)
	if err != nil {
		return Chunk{}, err
	}
	return Chunk{ID: got, Total: total, Data: data}, nil
}
