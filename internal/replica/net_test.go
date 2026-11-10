package replica

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/store"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

func serve(t *testing.T, s *store.Store, id Identity, allowed func(string) bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = Serve(ctx, ln, Local{Store: s}, id, allowed) }()
	return ln.Addr().String()
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(vfs.NewMem(), "/d", store.Options{Name: "n"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func identity(t *testing.T) Identity {
	t.Helper()
	id, err := LoadIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func commit(t *testing.T, s *store.Store, n int) {
	t.Helper()
	for i := range n {
		b, _ := store.Add("p", "k", string(rune('a'+i%26))+string(rune('a'+i/26)), i)
		if _, err := s.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPullOverTLSUsesFirstWorkingAddress(t *testing.T) {
	a, b := openStore(t), openStore(t)
	commit(t, a, 1200)
	ida, idb := identity(t), identity(t)
	addr := serve(t, a, ida, func(fp string) bool { return fp == idb.Fingerprint })
	cfg := PeerConfig{Fingerprint: ida.Fingerprint, Addrs: []string{"127.0.0.1:1", addr, "192.0.2.1:9"}}
	c, err := Dial(context.Background(), cfg, idb)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := Pull(context.Background(), b, c); err != nil {
		t.Fatal(err)
	}
	if b.Hash() != a.Hash() {
		t.Fatal("stores differ after pull")
	}
}

func TestLargeSnapshotCrossesTheNetworkInChunks(t *testing.T) {
	a, b := openStore(t), openStore(t)
	value := strings.Repeat("x", 60_000)
	for i := range 200 {
		body, _ := store.Set("p", fmt.Sprintf("k%d", i), value, nil)
		if _, err := a.Commit(body); err != nil {
			t.Fatal(err)
		}
	}
	res, err := a.Compact(nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 || res.SnapshotBytes <= chunkSize {
		t.Fatalf("test needs a multi-chunk snapshot: %+v", res)
	}
	ida, idb := identity(t), identity(t)
	addr := serve(t, a, ida, func(fp string) bool { return fp == idb.Fingerprint })
	c, err := Dial(context.Background(), PeerConfig{Fingerprint: ida.Fingerprint, Addrs: []string{addr}}, idb)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := Pull(context.Background(), b, c); err != nil {
		t.Fatal(err)
	}
	if b.Hash() != a.Hash() {
		t.Fatal("stores differ after snapshot transfer")
	}
}

func TestStrangerIsRejected(t *testing.T) {
	a, b := openStore(t), openStore(t)
	commit(t, a, 3)
	ida, idb, stranger := identity(t), identity(t), identity(t)
	addr := serve(t, a, ida, func(fp string) bool { return fp == idb.Fingerprint })
	cfg := PeerConfig{Fingerprint: ida.Fingerprint, Addrs: []string{addr}}
	c, err := Dial(context.Background(), cfg, stranger)
	if err == nil {
		defer c.Close()
		if _, _, err = Pull(context.Background(), b, c); err == nil {
			t.Fatal("server talked to an unknown client")
		}
	}
	if len(b.Vector()) != 0 {
		t.Fatal("stranger received events")
	}
}

func TestWrongServerIsRejected(t *testing.T) {
	a := openStore(t)
	ida, idb, imposter := identity(t), identity(t), identity(t)
	addr := serve(t, a, imposter, func(string) bool { return true })
	cfg := PeerConfig{Fingerprint: ida.Fingerprint, Addrs: []string{addr}}
	if c, err := Dial(context.Background(), cfg, idb); err == nil {
		c.Close()
		t.Fatal("connected to a server with the wrong fingerprint")
	}
}

func TestNoAddressAnswersFails(t *testing.T) {
	start := time.Now()
	cfg := PeerConfig{Fingerprint: "x", Addrs: []string{"127.0.0.1:1"}}
	if _, err := Dial(context.Background(), cfg, identity(t)); err == nil {
		t.Fatal("expected failure")
	}
	if time.Since(start) > dialWait+time.Second {
		t.Fatal("dial ignored its timeout")
	}
}

func TestProtocolMismatchStopsSync(t *testing.T) {
	b := openStore(t)
	if _, _, err := Pull(context.Background(), b, futurePeer{}); err == nil {
		t.Fatal("synced with a newer protocol")
	}
}

type futurePeer struct{}

func (futurePeer) Hello(context.Context, Hello) (Hello, error) { return Hello{Proto: Proto + 1}, nil }
func (futurePeer) Pull(context.Context, store.Vector) (Batch, error) {
	return Batch{}, nil
}
func (futurePeer) Snapshot(context.Context, string, int64) (Chunk, error) {
	return Chunk{}, nil
}

func TestIdleStrangersDoNotBlockPeers(t *testing.T) {
	a, b := openStore(t), openStore(t)
	commit(t, a, 3)
	ida, idb := identity(t), identity(t)
	addr := serve(t, a, ida, func(fp string) bool { return fp == idb.Fingerprint })
	for range maxPerHost + 4 {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	time.Sleep(handshake + time.Second)
	c, err := Dial(context.Background(), PeerConfig{Fingerprint: ida.Fingerprint, Addrs: []string{addr}}, idb)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := Pull(context.Background(), b, c); err != nil {
		t.Fatal(err)
	}
}
