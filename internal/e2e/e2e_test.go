package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "slse2e")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "sls")
	out, err := exec.Command("go", "build", "-race", "-o", bin, "github.com/jeessh/StopLosingMyStuff/cmd/sls").CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("%v: %s", err, out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type mac struct {
	t      *testing.T
	name   string
	home   string
	data   string
	root   string
	port   int
	daemon *exec.Cmd
	logf   *os.File
}

func newMac(t *testing.T, name string) *mac {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "sls")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(home) })
	home, _ = filepath.EvalSymlinks(home)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	m := &mac{t: t, name: name, home: home, data: filepath.Join(home, ".sl"), root: filepath.Join(home, "claude"), port: port}
	t.Cleanup(func() {
		m.kill()
		if t.Failed() {
			b, _ := os.ReadFile(filepath.Join(home, "daemon.log"))
			t.Logf("%s log:\n%s", name, b)
		}
	})
	m.run("init")
	return m
}

func (m *mac) env() []string {
	fake := filepath.Join(m.home, "fake-claude")
	_ = os.WriteFile(fake, []byte("#!/bin/sh\ncat >/dev/null\necho '{\"goal\":\"ship the demo\",\"next\":\"write docs\",\"decisions\":[\"use go\"]}'\n"), 0o700)
	return append(os.Environ(), "HOME="+m.home, "SLS_HOME="+m.data, "SLS_CLAUDE_ROOT="+m.root, "SLS_NAME="+m.name, "SLS_CLAUDE_BIN="+fake)
}

func (m *mac) cmd(dir string, args ...string) (string, error) {
	c := exec.Command(bin, args...)
	c.Env = m.env()
	c.Dir = dir
	var out bytes.Buffer
	c.Stdout, c.Stderr = &out, &out
	err := c.Run()
	return out.String(), err
}

func (m *mac) run(args ...string) string {
	m.t.Helper()
	out, err := m.cmd(m.home, args...)
	if err != nil {
		m.t.Fatalf("%s: sls %v: %v: %s", m.name, args, err, out)
	}
	return out
}

func (m *mac) start() {
	m.t.Helper()
	f, err := os.OpenFile(filepath.Join(m.home, "daemon.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		m.t.Fatal(err)
	}
	c := exec.Command(bin, "daemon", "--listen", fmt.Sprintf("127.0.0.1:%d", m.port))
	c.Env = m.env()
	c.Stdout, c.Stderr = f, f
	if err := c.Start(); err != nil {
		m.t.Fatal(err)
	}
	m.daemon, m.logf = c, f
	waitFor(m.t, m.name+" daemon up", func() bool {
		c, err := net.DialTimeout("unix", filepath.Join(m.data, "sock"), time.Second)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
}

func (m *mac) kill() {
	if m.daemon == nil {
		return
	}
	_ = m.daemon.Process.Signal(syscall.SIGKILL)
	_ = m.daemon.Wait()
	m.daemon = nil
	m.logf.Close()
}

func (m *mac) repo(remote string) string {
	m.t.Helper()
	dir := filepath.Join(m.home, "code", "demo-"+m.name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		m.t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"remote", "add", "origin", remote}} {
		c := exec.Command("git", args...)
		c.Dir = dir
		if out, err := c.CombinedOutput(); err != nil {
			m.t.Fatalf("git: %v %s", err, out)
		}
	}
	return dir
}

func pair(t *testing.T, a, b *mac) {
	t.Helper()
	fp := func(m *mac) string { return strings.Fields(m.run("id"))[1] }
	fa, fb := fp(a), fp(b)
	a.run("peer", "add", b.name, fb, fmt.Sprintf("127.0.0.1:1,127.0.0.1:%d", b.port))
	b.run("peer", "add", a.name, fa, fmt.Sprintf("127.0.0.1:%d", a.port))
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (m *mac) writeLine(session, cwd, kind string, content any) {
	m.t.Helper()
	dir := filepath.Join(m.root, "proj")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		m.t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{
		"type": kind, "sessionId": session, "cwd": cwd, "gitBranch": "main",
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "message": map[string]any{"content": content},
	})
	f, err := os.OpenFile(filepath.Join(dir, session+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		m.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		m.t.Fatal(err)
	}
}

func (m *mac) hook(event, session, cwd string) string {
	m.t.Helper()
	c := exec.Command(bin, "hook", event)
	c.Env = m.env()
	c.Stdin = strings.NewReader(fmt.Sprintf(`{"session_id":%q,"cwd":%q}`, session, cwd))
	out, err := c.Output()
	if err != nil {
		m.t.Fatalf("hook %s: %v", event, err)
	}
	return string(out)
}

func contains(s string, subs ...string) bool {
	for _, x := range subs {
		if !strings.Contains(s, x) {
			return false
		}
	}
	return true
}

func tool(name string, input map[string]any) []any {
	return []any{map[string]any{"type": "tool_use", "id": "t1", "name": name, "input": input}}
}

func TestHandoffAcrossMachines(t *testing.T) {
	a, b := newMac(t, "maca"), newMac(t, "macb")
	pair(t, a, b)
	repoA := a.repo("git@github.com:me/demo.git")
	repoB := b.repo("https://github.com/me/demo.git")
	a.start()
	b.start()

	a.writeLine("s1", repoA, "user", "add the login page")
	a.writeLine("s1", repoA, "assistant", tool("Edit", map[string]any{"file_path": repoA + "/login.go"}))
	waitFor(t, "session to reach macb", func() bool {
		return contains(b.run("brief", repoB), "maca", "add the login page", "login.go")
	})
	if out := b.run("brief", repoB); strings.Contains(out, a.home) {
		t.Fatalf("briefing leaked the other machine's paths:\n%s", out)
	}

	got := b.hook("SessionStart", "new", repoB)
	if !contains(got, "not instructions", "add the login page") {
		t.Fatalf("SessionStart output:\n%s", got)
	}

	a.writeLine("s1", repoA, "user", "and tests please")
	a.writeLine("s1", repoA, "assistant", []any{map[string]any{"type": "text", "text": "done"}})
	a.hook("SessionEnd", "s1", repoA)
	waitFor(t, "summary to reach macb", func() bool {
		return contains(b.run("brief", repoB), "Goal: ship the demo", "Next: write docs", "use go")
	})
}

func TestSurvivesKillAndOfflineConflict(t *testing.T) {
	a, b := newMac(t, "maca"), newMac(t, "macb")
	pair(t, a, b)
	repoA := a.repo("git@github.com:me/demo.git")
	repoB := b.repo("git@github.com:me/demo.git")
	a.start()
	b.start()
	a.writeLine("s2", repoA, "user", "run the migration")
	a.writeLine("s2", repoA, "assistant", tool("Bash", map[string]any{"command": "make migrate"}))
	waitFor(t, "first sync", func() bool { return contains(b.run("brief", repoB), "run the migration") })

	a.kill()
	b.kill()

	if out, err := a.cmd(repoA, "next", "finish the migration"); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if out, err := b.cmd(repoB, "next", "roll it back"); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if out := a.run("brief", repoA); !contains(out, "Next: finish the migration") {
		t.Fatalf("offline read:\n%s", out)
	}

	a.start()
	b.start()
	waitFor(t, "conflict on both", func() bool {
		return contains(a.run("brief", repoA), "conflict", "finish the migration", "roll it back") &&
			contains(b.run("brief", repoB), "conflict", "finish the migration", "roll it back")
	})
	if out, err := a.cmd(repoA, "next", "finish it, rollback is too risky"); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	waitFor(t, "resolution to reach macb", func() bool {
		out := b.run("brief", repoB)
		return contains(out, "Next: finish it, rollback is too risky") && !strings.Contains(out, "conflict")
	})
}

func TestKillDuringWritesLosesNothing(t *testing.T) {
	a, b := newMac(t, "maca"), newMac(t, "macb")
	pair(t, a, b)
	repoA := a.repo("git@github.com:me/demo.git")
	repoB := b.repo("git@github.com:me/demo.git")
	b.start()
	var acked []string
	for round := range 6 {
		a.start()
		results := make(chan string, 16)
		go func() {
			defer close(results)
			for i := range 16 {
				note := fmt.Sprintf("decision-%d-%d", round, i)
				if _, err := a.cmd(repoA, "note", note); err == nil {
					results <- note
				}
			}
		}()
		time.Sleep(time.Duration(50+rand.IntN(350)) * time.Millisecond)
		a.kill()
		for note := range results {
			acked = append(acked, note)
		}
	}
	if len(acked) == 0 {
		t.Fatal("no writes were acknowledged")
	}
	a.start()
	waitFor(t, "every acknowledged note on macb", func() bool {
		got := b.run("decisions", repoB)
		for _, note := range acked {
			if !strings.Contains(got, note+"\n") {
				return false
			}
		}
		return true
	})
	if got := a.run("decisions", repoA); strings.Count(got, "decision-") != len(acked) {
		t.Fatalf("got %d notes, acknowledged %d", strings.Count(got, "decision-"), len(acked))
	}
	if out := a.run("doctor"); strings.Contains(out, "stopped accepting") {
		t.Fatalf("doctor:\n%s", out)
	}
}

func TestDataSurvivesAndSecondDaemonIsRefused(t *testing.T) {
	a := newMac(t, "maca")
	a.start()
	a.run("note", "hello")
	c := exec.Command(bin, "daemon", "--listen", "127.0.0.1:0")
	c.Env = a.env()
	if out, err := c.CombinedOutput(); err == nil || !strings.Contains(string(out), "another daemon") {
		t.Fatalf("second daemon: %v %s", err, out)
	}
}

func TestHookIsSilentWithNoDataAndWhenInternal(t *testing.T) {
	a := newMac(t, "maca")
	if out := a.hook("SessionStart", "x", a.home); out != "" {
		t.Fatalf("expected nothing, got %q", out)
	}
	c := exec.Command(bin, "hook", "SessionStart")
	c.Env = append(a.env(), "STOPLOSING_INTERNAL=1")
	c.Stdin = strings.NewReader("this is not json")
	if out, err := c.Output(); err != nil || len(out) != 0 {
		t.Fatalf("%v %q", err, out)
	}
}
