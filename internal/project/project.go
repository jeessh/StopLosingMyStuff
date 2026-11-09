package project

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	Marker      = ".stoplosing-id"
	gitTimeout  = 5 * time.Second
	maxWalkFile = 20000
)

type Info struct {
	ID     string
	Root   string
	Unsure bool
}

type Status struct {
	Exists  bool   `json:"exists"`
	Unknown bool   `json:"unknown,omitempty"`
	Git     bool   `json:"git,omitempty"`
	Branch  string `json:"branch,omitempty"`
	Head    string `json:"head,omitempty"`
	Ahead   int    `json:"ahead,omitempty"`
	Behind  int    `json:"behind,omitempty"`
	Dirty   int    `json:"dirty,omitempty"`
	Files   int    `json:"files,omitempty"`
	Newest  int64  `json:"newest,omitempty"`
}

type gitResult struct {
	out    string
	code   int
	stderr string
	err    error
}

func git(ctx context.Context, dir string, args ...string) gitResult {
	ctx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-C", dir}, args...)...)
	cmd.WaitDelay = time.Second
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	r := gitResult{out: out.String(), stderr: errOut.String(), err: err}
	if ctx.Err() != nil {
		r.code = -1
		return r
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		r.code = exit.ExitCode()
	} else if err != nil {
		r.code = -1
	}
	return r
}

// notRepo is the only failure that proves a folder has no git repository.
// A timeout after waking or a macOS permission denial proves nothing.
func (r gitResult) notRepo() bool {
	return errors.Is(r.err, exec.ErrNotFound) ||
		(r.code == 128 && strings.Contains(strings.ToLower(r.stderr), "not a git repository"))
}

func folderAbsent(dir string) bool {
	_, err := os.Stat(dir)
	return errors.Is(err, fs.ErrNotExist)
}

// Resolve names a project the same way on every machine: by git remote, then
// by a marker file, then by its path under the home directory. Unsure means
// git failed in a way that says nothing about the folder, so the caller
// shouldn't file anything permanent under this ID.
func Resolve(ctx context.Context, dir, home string) Info {
	dir = filepath.Clean(dir)
	top := git(ctx, dir, "rev-parse", "--show-toplevel")
	if top.err == nil {
		root := resolveLinks(strings.TrimSpace(top.out))
		if common := git(ctx, dir, "rev-parse", "--path-format=absolute", "--git-common-dir"); common.err == nil {
			if c := strings.TrimSpace(common.out); filepath.Base(c) == ".git" {
				root = resolveLinks(filepath.Dir(c))
			}
		}
		remote := git(ctx, dir, "config", "--get", "remote.origin.url")
		if remote.err == nil && strings.TrimSpace(remote.out) != "" {
			return Info{ID: NormalizeRemote(strings.TrimSpace(remote.out)), Root: root}
		}
		if remote.err != nil && remote.code != 1 {
			return Info{ID: pathID(root, home), Root: root, Unsure: true}
		}
		if id := readMarker(root); id != "" {
			return Info{ID: id, Root: root}
		}
		return Info{ID: pathID(root, home), Root: root}
	}
	for cur := dir; ; cur = filepath.Dir(cur) {
		if id := readMarker(cur); id != "" {
			return Info{ID: id, Root: cur}
		}
		if cur == filepath.Dir(cur) || cur == home {
			break
		}
	}
	return Info{ID: pathID(dir, home), Root: dir, Unsure: !top.notRepo() && !folderAbsent(dir)}
}

func resolveLinks(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return p
}

func pathID(p, home string) string {
	if home != "" && (p == home || strings.HasPrefix(p, home+"/")) {
		return "path:~" + strings.TrimPrefix(p, home)
	}
	return "path:" + p
}

func readMarker(dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, Marker))
	if err != nil {
		return ""
	}
	if id := strings.TrimSpace(string(data)); id != "" {
		return id
	}
	return ""
}

// NormalizeRemote makes the ssh and https forms of a remote compare equal and
// drops any credentials in the URL.
func NormalizeRemote(raw string) string {
	s := strings.TrimSpace(raw)
	if strings.Contains(s, "://") {
		if u, err := url.Parse(s); err == nil {
			s = u.Host + "/" + strings.TrimPrefix(u.Path, "/")
		}
	} else if at, colon := strings.Index(s, "@"), strings.Index(s, ":"); colon > 0 && (at < 0 || at < colon) {
		s = s[at+1:colon] + "/" + s[colon+1:]
	}
	s = strings.TrimSuffix(strings.TrimRight(s, "/"), ".git")
	if host, rest, ok := strings.Cut(s, "/"); ok {
		s = strings.ToLower(host) + "/" + rest
	}
	return s
}

// Track gives a non-git folder a stable identity that survives being copied.
func Track(dir string) (string, error) {
	if id := readMarker(dir); id != "" {
		return id, nil
	}
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := "tracked:" + filepath.Base(dir) + "-" + hex.EncodeToString(b)
	return id, os.WriteFile(filepath.Join(dir, Marker), []byte(id+"\n"), 0o644)
}

func Check(ctx context.Context, root string) Status {
	if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
		return Status{}
	}
	r := git(ctx, root, "status", "--porcelain=v2", "--branch")
	if r.err != nil {
		if r.notRepo() {
			return walk(root)
		}
		return Status{Exists: true, Unknown: true}
	}
	st := Status{Exists: true, Git: true}
	for _, line := range strings.Split(r.out, "\n") {
		fields := strings.Fields(line)
		switch {
		case len(fields) >= 3 && fields[0] == "#" && fields[1] == "branch.oid":
			st.Head = fields[2]
			if len(st.Head) > 12 {
				st.Head = st.Head[:12]
			}
		case len(fields) >= 3 && fields[0] == "#" && fields[1] == "branch.head":
			st.Branch = fields[2]
		case len(fields) >= 4 && fields[0] == "#" && fields[1] == "branch.ab":
			st.Ahead, _ = strconv.Atoi(strings.TrimPrefix(fields[2], "+"))
			st.Behind, _ = strconv.Atoi(strings.TrimPrefix(fields[3], "-"))
		case len(fields) > 0 && fields[0] != "#":
			st.Dirty++
		}
	}
	return st
}

var errEnough = errors.New("enough")

func walk(root string) Status {
	st := Status{Exists: true}
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if n := d.Name(); n == ".git" || n == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		st.Files++
		if fi, err := d.Info(); err == nil && fi.ModTime().UnixMilli() > st.Newest {
			st.Newest = fi.ModTime().UnixMilli()
		}
		if st.Files >= maxWalkFile {
			return errEnough
		}
		return nil
	})
	return st
}
