# StopLosingMyStuff

I work across two Macs and lose context every time I switch. New Claude Code sessions don't know what the last one did, I keep writing handoff notes, and when I leave my desk the connection between the laptops breaks.

`slms` runs in the background on each Mac and fixes that. It watches what happens in your coding sessions, syncs it between machines, and tells each new session where things stand. No handoff files.

A new session gets something like this automatically:

```
Next (conflict, changed on two machines while apart, ask which one is right):
- [macbook-a] finish the migration
- [macbook-b] roll it back

Session on macbook-b, 3h ago, interrupted: add billing
Unfinished Bash, outcome unknown, verify before repeating: make migrate
Files edited: billing/charge.go, billing/charge_test.go

Machines:
- this machine now: main, 2 uncommitted
- macbook-b, last seen 4m ago: feature/billing, 3 unpushed
```

## Setup

Needs Go 1.25+, git and Claude Code.

```sh
go install github.com/jeessh/StopLosingMyStuff/cmd/slms@latest
slms init
slms install
```

Pair the Macs by running `slms id` on each one, then adding the other. List every address it might be reachable at, and whichever works gets used.

```sh
slms peer add other-mac <fingerprint> other-mac.tailnet.ts.net,192.168.1.20
```

## Commands

```
slms status                  every project across every machine
slms brief                   what a new session would be told
slms note "use postgres"     record a decision
slms next "ship the login"   set the next step, settles a conflict
slms track ~/mods/skyrim     follow a folder that isn't a git repo
slms service add render 8188 watch a local service
slms doctor                  check that everything is healthy
slms compact                 shrink the log now, also runs on its own
```

## How it works

Each Mac keeps an append-only log. Macs pull entries they're missing, and state is calculated from the log in a way that doesn't care what order entries arrive in. If both Macs change the same thing while apart, both versions are kept and you get asked.

Old entries get replaced by a snapshot once they stop mattering. A Mac that's been away too long merges the snapshot instead of replaying entries that no longer exist.

Diagrams and the reasoning behind each decision are in [docs/architecture.md](docs/architecture.md). Everything that can go wrong and the test covering it is in [docs/edge-cases.md](docs/edge-cases.md).

## Testing

I only have one laptop with me, so the second Mac is simulated.

- A fake disk that loses unsynced data the way a power cut does, including half-written and garbled records.
- A deterministic simulator with 2 or 3 Macs going through crashes, bad connections, restored backups, bit rot and wrong clocks. Any failure replays from its seed. Run it longer with `SLMS_SIM_SEEDS=20000 go test ./internal/sim`.
- Real daemons talking over real TLS on one machine, with `kill -9` during writes.
- `scripts/mutate.py` plants known bugs, like removing an fsync, and checks the tests catch them.

Numbers on an M5 Pro: about 3.5 ms per durable commit, 100k events recovered in 0.6 s, 10k events copied between stores in 0.16 s. Compacting 100k updates to 200 sessions took a 227 MB log down to a 0.3 MB snapshot and startup from 700 ms to 1 ms.

Real power loss, two physical Macs and an actual Tailscale connection haven't been tested yet.

## Limits

It tracks your code but doesn't copy it, that's still git's job. Reading transcripts depends on Claude Code's file format, which isn't a public API, so `slms doctor` warns if it stops parsing.

Secret redaction is pattern based, so it catches common formats and not everything.

If your projects live in `~/Documents`, macOS may need to allow `slms` to read that folder. `slms doctor` tells you when sessions are waiting on this.

## License

MIT
