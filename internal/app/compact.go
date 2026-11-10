package app

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/replica"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
)

const (
	// A peer gone longer than this stops holding back cleanup. It catches up
	// from a snapshot when it returns.
	peerPatience = 30 * 24 * time.Hour
	compactAfter = 32 << 20
)

type peerAck struct {
	Vector store.Vector `json:"vector"`
	Seen   int64        `json:"seen"`
}

func (a *App) ackPath() string { return filepath.Join(a.Dir, "acks.json") }

func (a *App) loadAcks() error {
	if a.acks != nil {
		return nil
	}
	acks, err := readJSON[map[string]peerAck](a.FS, a.ackPath())
	if err != nil {
		return err
	}
	if acks == nil {
		acks = map[string]peerAck{}
	}
	a.acks = acks
	return nil
}

// SawPeer records what a peer has confirmed holding.
func (a *App) SawPeer(name string, have store.Vector) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.loadAcks() == nil {
		a.acks[name] = peerAck{Vector: have, Seen: a.Now().UnixMilli()}
	}
}

func waitFor(peers []replica.PeerConfig, acks map[string]peerAck, now time.Time) []store.Vector {
	var wait []store.Vector
	for _, p := range peers {
		if ack, ok := acks[p.Name]; ok && now.Sub(time.UnixMilli(ack.Seen)) < peerPatience {
			wait = append(wait, ack.Vector)
		}
	}
	return wait
}

func (a *App) waiting() ([]store.Vector, error) {
	peers, err := LoadPeers(a.FS, a.Dir)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.loadAcks(); err != nil {
		return nil, err
	}
	if err := writeJSON(a.FS, a.ackPath(), a.acks); err != nil {
		return nil, err
	}
	return waitFor(peers, a.acks, a.Now()), nil
}

// CompactIfDue runs compaction when the log has grown enough to be worth it.
func (a *App) CompactIfDue() (bool, error) {
	wait, err := a.waiting()
	if err != nil {
		return false, err
	}
	if !a.Store.NeedsCompaction(wait, compactAfter) {
		return false, nil
	}
	_, err = a.Store.Compact(wait)
	return true, err
}

func (a *App) Compact() (string, error) {
	wait, err := a.waiting()
	if err != nil {
		return "", err
	}
	r, err := a.Store.Compact(wait)
	if err != nil {
		return "", err
	}
	out := fmt.Sprintf("saved a %s snapshot, removed %d log files, freed %s\n", mb(r.SnapshotBytes), r.Removed, mb(r.Freed))
	if r.Held > 0 {
		out += fmt.Sprintf("kept %d log files another Mac hasn't confirmed yet\n", r.Held)
	}
	return out, nil
}

func mb(n int64) string { return fmt.Sprintf("%.1f MB", float64(n)/(1<<20)) }
