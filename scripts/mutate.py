#!/usr/bin/env python3
"""Plant known bugs in a copy of the repo and check the tests notice."""
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAL, STORE = "internal/wal/wal.go", "internal/store/store.go"
STATE, NET = "internal/state/state.go", "internal/replica/net.go"
SESSION, WATCH = "internal/capture/session.go", "internal/capture/watcher.go"
SNAP = "internal/store/snapshot.go"

MUTANTS = [
    ("no fsync on append", WAL,
     "\tif err := w.f.Sync(); err != nil {\n\t\tw.failed = err\n\t\treturn nil, err\n\t}\n\tw.size = off",
     "\tw.size = off", "./internal/wal ./internal/sim"),
    ("no dir fsync on new segment", WAL,
     "\tif err := w.fs.SyncDir(w.dir); err != nil {\n\t\t_ = f.Close()\n\t\treturn err\n\t}\n\tw.f, w.seg",
     "\tw.f, w.seg", "./internal/wal ./internal/sim"),
    ("torn tail not cut", WAL,
     "\t\tif err := truncate(fsys, path, int64(good)); err != nil {\n\t\t\treturn nil, rec, err\n\t\t}\n",
     "", "./internal/wal ./internal/sim"),
    ("failed fsync not sticky", WAL,
     "\tif err := w.f.Sync(); err != nil {\n\t\tw.failed = err",
     "\tif err := w.f.Sync(); err != nil {", "./internal/wal ./internal/sim"),
    ("origin reused after restart", STORE,
     's.origin = s.node.ID[:8] + "-" + inc',
     "_ = inc\n\t\ts.origin = s.node.ID[:8]", "./internal/sim"),
    ("superseded write can return", STATE,
     "\t\tif !r.Dead[w.ID.String()] {\n\t\t\tr.Live[w.ID.String()] = w\n\t\t}",
     "\t\tr.Live[w.ID.String()] = w", "./internal/state ./internal/sim"),
    ("deleted element can return", STATE,
     "\t\tif set.Gone[b.Elem] {\n\t\t\treturn\n\t\t}\n",
     "", "./internal/state ./internal/sim"),
    ("any tls peer accepted", NET,
     '\t\t\tif !allowed(fingerprint(cs.PeerCertificates[0].Raw)) {\n\t\t\t\treturn errors.New("unknown peer")\n\t\t\t}\n',
     "", "./internal/replica"),
    ("finished tools stay pending", SESSION,
     "\t\tdelete(s.Pending, b.ToolUseID)\n", "", "./internal/capture"),
    ("partial line consumed", WATCH,
     "\tend := bytes.LastIndexByte(buf, '\\n')", "\tend := len(buf)", "./internal/capture"),
    ("summary erases conflict", "internal/app/app.go",
     "\tif len(cur) < 2 {\n\t\treturn cur\n\t}\n",
     "\tif len(cur) >= 0 {\n\t\treturn cur\n\t}\n", "./internal/app"),
    ("cleanup ignores peers", SNAP,
     "\t\t\tif seq > v[o] {\n\t\t\t\treturn false\n\t\t\t}\n", "\t\t\t_ = v\n", "./internal/store"),
    ("merge revives superseded write", STATE,
     "if !r.Dead[id] && (both || !mine(w.ID)) {", "if _ = both; !r.Dead[id] {", "./internal/state ./internal/sim"),
    ("dead ids pruned too early", STATE,
     "if id, ok := event.ParseID(key); ok && seen(id) {", "if _, ok := event.ParseID(key); ok {", "./internal/state ./internal/sim"),
    ("snapshot checksum ignored", SNAP,
     "\tif hex.EncodeToString(sum[:]) != string(rest[:sumSize]) {\n\t\treturn nil, errBadSnapshot\n\t}\n",
     "\t_ = sum\n", "./internal/store"),
    ("snapshot written once", SNAP,
     'var snapCopies = []string{"a", "b"}', 'var snapCopies = []string{"a"}', "./internal/store ./internal/sim"),
    ("snapshot not durable before cleanup", SNAP,
     "if err := vfs.WriteAtomic(s.fs, filepath.Join(s.snapDir(), snapName(gen, c)), data); err != nil {",
     "if err := func() error { f, err := s.fs.Create(filepath.Join(s.snapDir(), snapName(gen, c))); if err != nil { return err }; _, err = f.Write(data); return err }(); err != nil {",
     "./internal/store ./internal/sim"),
    ("replay reapplies snapshot events", STORE,
     "\tif e.Seq <= s.base[e.Origin] {", "\tif false {", "./internal/store ./internal/sim"),
    ("installed snapshot not saved", SNAP,
     "if _, err := s.writeSnapshot(snapshot{Vector: vector, Heads: heads, State: merged, Unknown: unknown, SealedBefore: active}); err != nil {",
     "if err := error(nil); err != nil {", "./internal/store ./internal/sim"),
    ("events from newer builds dropped", STORE,
     "\tif !state.Knows(e) {\n\t\ts.unknown = append(s.unknown, e)\n\t}\n", "", "./internal/store"),
    ("stale relayed event accepted", SNAP,
     "\t\tcase e.Seq <= s.vector[e.Origin]:\n", "", "./internal/store"),
    ("damaged snapshots replaced by empty start", SNAP,
     "\tif good == nil {\n\t\treturn bad, fmt.Errorf(", "\tif good == nil {\n\t\treturn bad, nil\n\t}\n\tif good == nil {\n\t\treturn bad, fmt.Errorf(", "./internal/store ./internal/sim"),
    ("covered damage discards newer events", "internal/wal/wal.go",
     "\t\tif n < covered && !last {\n\t\t\tcontinue\n\t\t}\n", "", "./internal/wal ./internal/store"),
    ("snapshot from other history accepted", SNAP,
     "\t\tif head != s.heads[o] {\n\t\t\treturn fmt.Errorf(\"%w at %s:%d\", errFork, o, seq)\n\t\t}\n", "", "./internal/store"),
]


def main():
    env = dict(os.environ, SLMS_SIM_SEEDS="300")
    missed = 0
    for name, path, old, new, packages in MUTANTS:
        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "repo")
            shutil.copytree(ROOT, work, ignore=shutil.ignore_patterns(".git"))
            target = os.path.join(work, path)
            with open(target) as f:
                src = f.read()
            if old not in src:
                print(f"stale   {name}")
                missed += 1
                continue
            with open(target, "w") as f:
                f.write(src.replace(old, new, 1))
            run = subprocess.run(["go", "test", "-count=1"] + packages.split(),
                                 cwd=work, env=env, capture_output=True, text=True)
            output = run.stdout + run.stderr
            if run.returncode == 0 or "build failed" in output:
                print(f"missed  {name}")
                missed += 1
            else:
                print(f"caught  {name}")
    sys.exit(1 if missed else 0)


if __name__ == "__main__":
    main()
