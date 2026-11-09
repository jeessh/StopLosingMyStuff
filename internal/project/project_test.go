package project

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestNormalizeRemote(t *testing.T) {
	want := "github.com/me/repo"
	for _, in := range []string{
		"git@github.com:me/repo.git", "https://github.com/me/repo.git", "https://github.com/me/repo",
		"ssh://git@github.com/me/repo.git", "https://user:" + "tok3n" + "@GitHub.com/me/repo.git", "git@GITHUB.com:me/repo/",
	} {
		if got := NormalizeRemote(in); got != want {
			t.Errorf("NormalizeRemote(%q) = %q", in, got)
		}
	}
}

func run(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func repo(t *testing.T, remote string) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run(t, dir, "init", "-q", "-b", "main")
	if remote != "" {
		run(t, dir, "remote", "add", "origin", remote)
	}
	return dir
}

func TestSameRepoGetsSameIDAtDifferentPaths(t *testing.T) {
	ctx := context.Background()
	a := Resolve(ctx, repo(t, "git@github.com:me/x.git"), "/nope")
	b := Resolve(ctx, repo(t, "https://github.com/me/x"), "/nope")
	if a.ID != b.ID || a.ID != "github.com/me/x" {
		t.Fatalf("%q vs %q", a.ID, b.ID)
	}
}

func TestSubdirAndWorktreeResolveToTheProject(t *testing.T) {
	ctx := context.Background()
	dir := repo(t, "git@github.com:me/x.git")
	sub := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := Resolve(ctx, sub, "/nope"); got.Root != dir {
		t.Fatalf("root = %q, want %q", got.Root, dir)
	}
	run(t, dir, "commit", "-q", "--allow-empty", "-m", "x")
	wt := filepath.Join(t.TempDir(), "wt")
	run(t, dir, "worktree", "add", "-q", wt)
	if got := Resolve(ctx, wt, "/nope"); got.ID != "github.com/me/x" || got.Root != dir {
		t.Fatalf("worktree = %+v, want root %q", got, dir)
	}
}

func TestTrackedFolderIdentitySurvivesACopy(t *testing.T) {
	ctx := context.Background()
	a := t.TempDir()
	id, err := Track(a)
	if err != nil {
		t.Fatal(err)
	}
	b := t.TempDir()
	data, _ := os.ReadFile(filepath.Join(a, Marker))
	if err := os.WriteFile(filepath.Join(b, Marker), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := Resolve(ctx, b, "/nope"); got.ID != id {
		t.Fatalf("%q vs %q", got.ID, id)
	}
	again, _ := Track(a)
	if again != id {
		t.Fatal("Track changed an existing identity")
	}
}

func TestDeletedFolderStillResolves(t *testing.T) {
	got := Resolve(context.Background(), "/Users/me/gone/thing", "/Users/me")
	if got.ID != "path:~/gone/thing" {
		t.Fatalf("id = %q", got.ID)
	}
	if st := Check(context.Background(), "/Users/me/gone/thing"); st.Exists {
		t.Fatal("missing folder reported as existing")
	}
}

func TestCheckCountsUnpushedAndDirty(t *testing.T) {
	ctx := context.Background()
	up := repo(t, "")
	run(t, up, "commit", "-q", "--allow-empty", "-m", "base")
	dir := filepath.Join(t.TempDir(), "c")
	run(t, up, "clone", "-q", up, dir)
	run(t, dir, "commit", "-q", "--allow-empty", "-m", "local")
	if err := os.WriteFile(filepath.Join(dir, "new"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	st := Check(ctx, dir)
	if !st.Git || st.Branch != "main" || st.Ahead != 1 || st.Dirty != 1 {
		t.Fatalf("status = %+v", st)
	}
}

func TestCheckFolderWithoutGit(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mod.pak"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := Check(context.Background(), dir)
	if !st.Exists || st.Git || st.Files != 1 || st.Newest == 0 {
		t.Fatalf("status = %+v", st)
	}
}

func fakeGit(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
}

func TestGitFailureIsNotTreatedAsNoRepo(t *testing.T) {
	dir := t.TempDir()
	fakeGit(t, "echo 'fatal: cannot change to folder: Operation not permitted' >&2; exit 128")
	if got := Resolve(context.Background(), dir, "/nope"); !got.Unsure {
		t.Fatalf("got %+v", got)
	}
	if st := Check(context.Background(), dir); !st.Unknown || st.Git {
		t.Fatalf("status = %+v", st)
	}
}

func TestPlainFolderIsSure(t *testing.T) {
	dir := t.TempDir()
	fakeGit(t, "echo 'fatal: not a git repository (or any of the parent directories)' >&2; exit 128")
	if got := Resolve(context.Background(), dir, "/nope"); got.Unsure {
		t.Fatalf("got %+v", got)
	}
}

func TestHungGitChildDoesNotHangTheCaller(t *testing.T) {
	fakeGit(t, "sleep 60 &\nsleep 60")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	Check(ctx, t.TempDir())
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %v", d)
	}
}
