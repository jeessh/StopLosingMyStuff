package event

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jeessh/StopLosingMyStuff/internal/hlc"
)

const (
	MaxBody   = 64 << 10
	maxOrigin = 64

	OpSet = "set"
	OpAdd = "add"
	OpDel = "del"
)

type ID struct {
	Origin string `json:"o"`
	Seq    uint64 `json:"s"`
}

func (id ID) String() string { return fmt.Sprintf("%s:%d", id.Origin, id.Seq) }

// Body travels as opaque bytes so older builds can relay events they can't read.
type Body struct {
	Project string          `json:"r"`
	Op      string          `json:"op"`
	Key     string          `json:"k"`
	Value   json.RawMessage `json:"v,omitempty"`
	Elem    string          `json:"e,omitempty"`
	Over    []ID            `json:"x,omitempty"`
}

type Event struct {
	Origin string `json:"o"`
	Seq    uint64 `json:"s"`
	Prev   string `json:"p,omitempty"`
	Hash   string `json:"h"`
	Time   hlc.T  `json:"t"`
	Body   []byte `json:"b"`
}

func (e Event) ID() ID { return ID{e.Origin, e.Seq} }

func (e Event) digest() string {
	h := sha256.New()
	var buf [8]byte
	field := func(b []byte) {
		binary.BigEndian.PutUint64(buf[:], uint64(len(b)))
		h.Write(buf[:])
		h.Write(b)
	}
	num := func(v uint64) {
		binary.BigEndian.PutUint64(buf[:], v)
		h.Write(buf[:])
	}
	field([]byte(e.Origin))
	num(e.Seq)
	field([]byte(e.Prev))
	num(uint64(e.Time.Wall))
	num(uint64(e.Time.Logical))
	field(e.Body)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func (e *Event) Seal() { e.Hash = e.digest() }

func (e Event) Verify() error {
	switch {
	case e.Origin == "" || len(e.Origin) > maxOrigin:
		return errors.New("bad origin")
	case e.Seq == 0:
		return errors.New("bad seq")
	case len(e.Body) > MaxBody:
		return errors.New("body too large")
	case e.Hash != e.digest():
		return errors.New("hash mismatch")
	}
	return nil
}
