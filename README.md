# swarm-candidate-impl

Working `swarm` CLI implementation base.

This repo now keeps the frozen public contract as a regression baseline while
moving the CLI toward product behavior that can be used day to day.

Current implemented surface:

- `swarm message check`
- `swarm message read --channel ...`
- `swarm message send --target ...` using stdin for channels, threads, and DMs
- freshness-hold draft output across message targets with newer local context
- `swarm task create --channel ... --title ...`
- `swarm task list --channel ...`
- `swarm task claim --channel ... --number ...`
- `swarm task update --channel ... --number ... --status ...`
- `swarm reminder schedule --title ... --at ...`
- `swarm reminder list`
- `swarm reminder snooze --id ... --until ...`
- `swarm reminder update --id ...`
- `swarm reminder cancel --id ...`
- `swarm reminder log --id ...`
- `--content` rejection
- local SQLite-backed persistence
- generated message IDs and wall-clock sent timestamps

It does not implement a daemon, server, automatic reminder firing,
integrations, attachments, or production workspace access.

## Verify

From the `swarm-harness` checkout:

```bash
SWARM_CANDIDATE_STATE_DIR="$(mktemp -d)" SWARM_CLI=/path/to/swarm-candidate-impl/swarm python3 scripts/contract_check.py --live
```

From this checkout:

```bash
python3 scripts/anti_stub_probe.py
```

The local implementation stores state in `state.sqlite3`. By default it uses
the user state directory (`$XDG_STATE_HOME/swarm-cli` or
`~/.local/state/swarm-cli`); set `SWARM_CANDIDATE_STATE_DIR` for isolated test
runs or to inspect a specific store.

The anti-stub probe sends fixture-absent message bodies, reads them back, checks
thread/target isolation, drains real inbox state, and exercises the
freshness-hold draft cursor, DM persistence, target-generic freshness, and
wall-clock sent timestamps. It also checks SQLite-backed task lifecycle
create/list/claim/update behavior, reminder schedule/list/snooze/update/cancel/log
behavior, and concurrent write serialization.
