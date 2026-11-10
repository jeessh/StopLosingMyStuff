package replica

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/store"
)

const (
	maxFrame   = 16 << 20
	ioDeadline = 20 * time.Second
	dialWait   = 4 * time.Second
	maxConns   = 16
	handshake  = 5 * time.Second
	maxPending = 64
	maxPerHost = 8
	chunk      = 64 << 10
)

type Identity struct {
	Cert        tls.Certificate
	Fingerprint string
}

type PeerConfig struct {
	Name        string   `json:"name"`
	Fingerprint string   `json:"fingerprint"`
	Addrs       []string `json:"addrs"`
}

// LoadIdentity reads this machine's key pair, making one on first use.
func LoadIdentity(dir string) (Identity, error) {
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if errors.Is(err, fs.ErrNotExist) {
		if err := generate(certPath, keyPath); err != nil {
			return Identity{}, err
		}
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
	}
	if err != nil {
		return Identity{}, err
	}
	return Identity{Cert: cert, Fingerprint: fingerprint(cert.Certificate[0])}, nil
}

func fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

func generate(certPath, keyPath string) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "slms"},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().AddDate(50, 0, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return err
	}
	key, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		return err
	}
	return os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
}

// tlsConfig pins peers by certificate fingerprint. There is no CA, so chain
// verification is off on purpose and VerifyConnection does the real check.
func tlsConfig(id Identity, allowed func(string) bool) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{id.Cert},
		MinVersion:         tls.VersionTLS13,
		ClientAuth:         tls.RequireAnyClientCert,
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("peer sent no certificate")
			}
			if !allowed(fingerprint(cs.PeerCertificates[0].Raw)) {
				return errors.New("unknown peer")
			}
			return nil
		},
	}
}

type request struct {
	Method string       `json:"m"`
	Hello  *Hello       `json:"hello,omitempty"`
	Have   store.Vector `json:"have,omitempty"`
	ID     string       `json:"id,omitempty"`
	Offset int64        `json:"offset,omitempty"`
}

type response struct {
	Err   string `json:"err,omitempty"`
	Hello *Hello `json:"hello,omitempty"`
	Batch *Batch `json:"batch,omitempty"`
	Chunk *Chunk `json:"chunk,omitempty"`
}

func writeFrame(c net.Conn, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body) > maxFrame {
		return fmt.Errorf("frame of %d bytes too large", len(body))
	}
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(body)), uint32(len(body)))
	frame = append(frame, body...)
	for len(frame) > 0 {
		n := min(len(frame), chunk)
		_ = c.SetWriteDeadline(time.Now().Add(ioDeadline))
		if _, err := c.Write(frame[:n]); err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}

func readFrame(c net.Conn, v any) error {
	_ = c.SetReadDeadline(time.Now().Add(ioDeadline))
	var size [4]byte
	if _, err := io.ReadFull(c, size[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n > maxFrame {
		return fmt.Errorf("frame of %d bytes too large", n)
	}
	body := make([]byte, n)
	for read := 0; read < len(body); {
		end := min(read+chunk, len(body))
		_ = c.SetReadDeadline(time.Now().Add(ioDeadline))
		if _, err := io.ReadFull(c, body[read:end]); err != nil {
			return err
		}
		read = end
	}
	return json.Unmarshal(body, v)
}

// Serve answers peers until ctx ends. Authentication happens in the TLS
// handshake, so a stranger never reaches the protocol. Handshakes have their
// own limits, so idle strangers can't use up the slots peers need.
func Serve(ctx context.Context, ln net.Listener, l Local, id Identity, allowed func(string) bool) error {
	conf := tlsConfig(id, allowed)
	stop := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stop()
	slots := make(chan struct{}, maxConns)
	gate := newGate()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		host := remoteHost(c)
		if !gate.enter(host) {
			_ = c.Close()
			continue
		}
		go func() {
			tc := tls.Server(c, conf)
			hctx, cancel := context.WithTimeout(ctx, handshake)
			err := tc.HandshakeContext(hctx)
			cancel()
			gate.leave(host)
			if err != nil {
				_ = c.Close()
				return
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
				defer tc.Close()
				serveConn(ctx, tc, l)
			default:
				_ = tc.Close()
			}
		}()
	}
}

func remoteHost(c net.Conn) string {
	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		return c.RemoteAddr().String()
	}
	return host
}

type gate struct {
	mu    sync.Mutex
	total int
	hosts map[string]int
}

func newGate() *gate { return &gate{hosts: map[string]int{}} }

func (g *gate) enter(host string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.total >= maxPending || g.hosts[host] >= maxPerHost {
		return false
	}
	g.total++
	g.hosts[host]++
	return true
}

func (g *gate) leave(host string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.total--
	if g.hosts[host]--; g.hosts[host] == 0 {
		delete(g.hosts, host)
	}
}

func serveConn(ctx context.Context, c net.Conn, l Local) {
	for ctx.Err() == nil {
		var req request
		if err := readFrame(c, &req); err != nil {
			return
		}
		var resp response
		switch req.Method {
		case "hello":
			if req.Hello == nil {
				resp.Err = "missing hello"
				break
			}
			h, err := l.Hello(ctx, *req.Hello)
			if err != nil {
				resp.Err = err.Error()
				break
			}
			resp.Hello = &h
		case "pull":
			b, err := l.Pull(ctx, req.Have)
			if err != nil {
				resp.Err = err.Error()
				break
			}
			resp.Batch = &b
		case "snapshot":
			chunk, err := l.Snapshot(ctx, req.ID, req.Offset)
			if err != nil {
				resp.Err = err.Error()
				break
			}
			resp.Chunk = &chunk
		default:
			resp.Err = "unknown method"
		}
		if writeFrame(c, resp) != nil {
			return
		}
	}
}

// Client is a Peer reached over the network.
type Client struct{ c net.Conn }

// Dial tries every address at once and keeps the first to finish the
// handshake, so Ethernet, Wi-Fi and Tailscale can all be listed and the
// current network simply wins.
func Dial(ctx context.Context, p PeerConfig, id Identity) (*Client, error) {
	if len(p.Addrs) == 0 {
		return nil, errors.New("peer has no addresses")
	}
	ctx, cancel := context.WithTimeout(ctx, dialWait)
	defer cancel()
	conf := tlsConfig(id, func(fp string) bool { return fp == p.Fingerprint })
	type result struct {
		c   net.Conn
		err error
	}
	results := make(chan result, len(p.Addrs))
	for _, addr := range p.Addrs {
		go func() {
			d := tls.Dialer{Config: conf}
			c, err := d.DialContext(ctx, "tcp", addr)
			results <- result{c, err}
		}()
	}
	var firstErr error
	for i := range p.Addrs {
		r := <-results
		if r.err == nil {
			go func(rest int) {
				for range rest {
					if o := <-results; o.c != nil {
						_ = o.c.Close()
					}
				}
			}(len(p.Addrs) - i - 1)
			return &Client{c: r.c}, nil
		}
		if firstErr == nil {
			firstErr = r.err
		}
	}
	return nil, firstErr
}

func (c *Client) call(ctx context.Context, req request) (response, error) {
	stop := context.AfterFunc(ctx, func() { _ = c.c.Close() })
	defer stop()
	var resp response
	if err := writeFrame(c.c, req); err != nil {
		return resp, err
	}
	if err := readFrame(c.c, &resp); err != nil {
		return resp, err
	}
	if resp.Err != "" {
		return resp, errors.New(resp.Err)
	}
	return resp, nil
}

func (c *Client) Hello(ctx context.Context, me Hello) (Hello, error) {
	resp, err := c.call(ctx, request{Method: "hello", Hello: &me})
	if err != nil {
		return Hello{}, err
	}
	if resp.Hello == nil {
		return Hello{}, errors.New("empty hello")
	}
	return *resp.Hello, nil
}

func (c *Client) Pull(ctx context.Context, have store.Vector) (Batch, error) {
	resp, err := c.call(ctx, request{Method: "pull", Have: have})
	if err != nil {
		return Batch{}, err
	}
	if resp.Batch == nil {
		return Batch{}, errors.New("empty batch")
	}
	return *resp.Batch, nil
}

func (c *Client) Snapshot(ctx context.Context, id string, offset int64) (Chunk, error) {
	resp, err := c.call(ctx, request{Method: "snapshot", ID: id, Offset: offset})
	if err != nil {
		return Chunk{}, err
	}
	if resp.Chunk == nil {
		return Chunk{}, errors.New("empty chunk")
	}
	return *resp.Chunk, nil
}

func (c *Client) Close() error { return c.c.Close() }
