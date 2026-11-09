package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jeessh/StopLosingMyStuff/internal/app"
	"github.com/jeessh/StopLosingMyStuff/internal/daemon"
	"github.com/jeessh/StopLosingMyStuff/internal/replica"
	"github.com/jeessh/StopLosingMyStuff/internal/store"
	"github.com/jeessh/StopLosingMyStuff/internal/summary"
	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const usage = `sls keeps project context in sync between your Macs.

  sls init                       set up this machine
  sls install [--no-launchd]     add Claude Code hooks and start at login
  sls id                         print the line to pair with this machine
  sls peer add NAME FINGERPRINT ADDR[,ADDR...]
  sls peer ls | rm NAME
  sls status                     every project across every machine
  sls brief [DIR]                what a new session would be told
  sls note TEXT                  record a decision
  sls decisions [DIR]            every decision recorded for a project
  sls goal TEXT | next TEXT      set the goal or next step, settles conflicts
  sls track [DIR]                follow a folder that isn't a git repo
  sls service add NAME PORT      watch a local service
  sls doctor                     check that everything is healthy
  sls daemon [--listen ADDR] [--no-summaries]
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sls:", err)
		os.Exit(1)
	}
}

func home() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return h
}

func dataDir() string {
	if d := os.Getenv("SLS_HOME"); d != "" {
		return d
	}
	return filepath.Join(home(), ".stoplosing")
}

func claudeRoot() string {
	if d := os.Getenv("SLS_CLAUDE_ROOT"); d != "" {
		return d
	}
	return filepath.Join(home(), ".claude", "projects")
}

func hostname() string {
	if n := os.Getenv("SLS_NAME"); n != "" {
		return n
	}
	h, err := os.Hostname()
	if err != nil {
		return "mac"
	}
	return strings.TrimSuffix(h, ".local")
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "init":
		return initNode()
	case "id":
		return printID()
	case "peer":
		return peer(rest)
	case "daemon":
		return runDaemon(rest)
	case "hook":
		return hook(rest)
	case "install":
		return install(rest)
	case "service":
		return service(rest)
	case "status", "doctor":
		return show(app.Request{Cmd: cmd})
	case "brief", "decisions":
		return show(app.Request{Cmd: cmd, Dir: dirArg(rest)})
	case "track":
		return show(app.Request{Cmd: cmd, Dir: dirArg(rest)})
	case "note", "goal", "next":
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		return show(app.Request{Cmd: cmd, Dir: wd, Text: strings.Join(rest, " ")})
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	}
	return fmt.Errorf("unknown command %q, run sls help", cmd)
}

func dirArg(args []string) string {
	dir := "."
	if len(args) > 0 {
		dir = args[0]
	}
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return dir
}

func initNode() error {
	if err := os.MkdirAll(dataDir(), 0o700); err != nil {
		return err
	}
	lock, err := daemon.Lock(dataDir())
	if err == nil {
		defer lock.Close()
		st, err := store.Open(vfs.OS{}, dataDir(), store.Options{Name: hostname()})
		if err != nil {
			return err
		}
		defer st.Close()
	} else if !errors.Is(err, daemon.ErrLocked) {
		return err
	}
	return printID()
}

func printID() error {
	id, err := replica.LoadIdentity(dataDir())
	if err != nil {
		return err
	}
	fmt.Printf("%s %s\n", hostname(), id.Fingerprint)
	return nil
}

func peer(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sls peer add|ls|rm")
	}
	fsys, dir := vfs.OS{}, dataDir()
	peers, err := app.LoadPeers(fsys, dir)
	if err != nil {
		return err
	}
	switch args[0] {
	case "ls":
		for _, p := range peers {
			fmt.Printf("%s %s %s\n", p.Name, p.Fingerprint, strings.Join(p.Addrs, ","))
		}
		return nil
	case "add":
		if len(args) != 4 {
			return errors.New("usage: sls peer add NAME FINGERPRINT ADDR[,ADDR...]")
		}
		addrs := strings.Split(args[3], ",")
		for i, a := range addrs {
			if _, _, err := net.SplitHostPort(a); err != nil {
				addrs[i] = net.JoinHostPort(a, defaultPort)
			}
		}
		kept := peers[:0]
		for _, p := range peers {
			if p.Name != args[1] {
				kept = append(kept, p)
			}
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		return app.SavePeers(fsys, dir, append(kept, replica.PeerConfig{Name: args[1], Fingerprint: args[2], Addrs: addrs}))
	case "rm":
		if len(args) != 2 {
			return errors.New("usage: sls peer rm NAME")
		}
		kept := peers[:0]
		for _, p := range peers {
			if p.Name != args[1] {
				kept = append(kept, p)
			}
		}
		return app.SavePeers(fsys, dir, kept)
	}
	return fmt.Errorf("unknown peer command %q", args[0])
}

func service(args []string) error {
	if len(args) != 3 || args[0] != "add" {
		return errors.New("usage: sls service add NAME PORT")
	}
	port, err := strconv.Atoi(args[2])
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("bad port %q", args[2])
	}
	fsys, dir := vfs.OS{}, dataDir()
	list, err := app.LoadServices(fsys, dir)
	if err != nil {
		return err
	}
	kept := list[:0]
	for _, s := range list {
		if s.Name != args[1] {
			kept = append(kept, s)
		}
	}
	return app.SaveServices(fsys, dir, append(kept, app.Service{Name: args[1], Port: port}))
}

const defaultPort = "7742"

func runDaemon(args []string) error {
	fl := flag.NewFlagSet("daemon", flag.ContinueOnError)
	listen := fl.String("listen", ":"+defaultPort, "address for peers")
	noSummaries := fl.Bool("no-summaries", false, "skip AI summaries")
	if err := fl.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg := daemon.Config{Dir: dataDir(), Home: home(), ClaudeRoot: claudeRoot(), Listen: *listen, Name: hostname()}
	if !*noSummaries {
		work := filepath.Join(dataDir(), "summaries")
		if err := os.MkdirAll(work, 0o700); err != nil {
			return err
		}
		cfg.Summarizer = summary.CLI{Bin: os.Getenv("SLS_CLAUDE_BIN"), Dir: work}
	}
	return daemon.Run(ctx, cfg)
}

func call(ctx context.Context, req app.Request) (app.Response, error) {
	d := net.Dialer{}
	c, err := d.DialContext(ctx, "unix", daemon.SocketPath(dataDir()))
	if err != nil {
		return app.Response{}, err
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if err := json.NewEncoder(c).Encode(req); err != nil {
		return app.Response{}, err
	}
	var resp app.Response
	return resp, json.NewDecoder(c).Decode(&resp)
}

func show(req app.Request) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := request(ctx, req)
	if err != nil {
		return err
	}
	if resp.Err != "" {
		return errors.New(resp.Err)
	}
	fmt.Print(resp.Out)
	return nil
}

// request prefers the daemon and falls back to the files. If the lock is held
// but the socket isn't answering, a daemon is mid-start, so it waits for it.
func request(ctx context.Context, req app.Request) (app.Response, error) {
	for {
		resp, err := call(ctx, req)
		if err == nil {
			return resp, nil
		}
		resp, err = direct(ctx, req)
		if !errors.Is(err, daemon.ErrLocked) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return app.Response{}, err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// direct handles a command with no daemon running. Reads work without the
// lock. Writes take it, so they can't race a daemon that is starting.
func direct(ctx context.Context, req app.Request) (app.Response, error) {
	readOnly := req.Cmd == "status" || req.Cmd == "brief" || req.Cmd == "doctor" || req.Cmd == "decisions"
	if !readOnly {
		lock, err := daemon.Lock(dataDir())
		if err != nil {
			return app.Response{}, err
		}
		defer lock.Close()
	}
	st, err := store.Open(vfs.OS{}, dataDir(), store.Options{ReadOnly: readOnly, Name: hostname()})
	if err != nil {
		return app.Response{}, err
	}
	defer st.Close()
	a := &app.App{Store: st, FS: vfs.OS{}, Dir: dataDir(), Home: home(), Now: time.Now}
	return a.Do(ctx, req), nil
}

type hookInput struct {
	SessionID string `json:"session_id"`
	Cwd       string `json:"cwd"`
}

// hook never fails or stalls, since it runs inside someone's coding session.
func hook(args []string) error {
	if len(args) != 1 || os.Getenv(summary.InternalEnv) != "" {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHook(args[0])
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
	}
	return nil
}

func runHook(event string) {
	data, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil {
		return
	}
	var in hookInput
	if json.Unmarshal(data, &in) != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if event != "SessionStart" {
		_, _ = call(ctx, app.Request{Cmd: "signal", Key: event, Session: in.SessionID})
		return
	}
	if in.Cwd == "" {
		return
	}
	resp, err := request(ctx, app.Request{Cmd: "brief", Dir: in.Cwd})
	if err == nil && resp.Err == "" {
		fmt.Print(resp.Out)
	}
}
