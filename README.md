# swarm-candidate-impl

Working `swarm` CLI implementation base.

This repo now keeps the frozen public contract as a regression baseline while
moving the CLI toward product behavior that can be used day to day.

Current implemented surface:

- `swarm message check`
- `swarm message read --channel ...`
- `swarm message send --target ...` using stdin for channels, threads, and DMs
- freshness-hold draft output across message targets with newer local context
- `--content` rejection
- local JSON-backed persistence
- generated message IDs and wall-clock sent timestamps

It does not implement a daemon, server, task board, reminders, integrations,
attachments, or production workspace access.

## Verify

From the `swarm-harness` checkout:

```bash
SWARM_CLI=/path/to/swarm-candidate-impl/swarm python3 scripts/contract_check.py --live
```

From this checkout:

```bash
python3 scripts/anti_stub_probe.py
```

The local implementation stores live-check state outside the repo by default.
Set `SWARM_CANDIDATE_STATE_DIR` to inspect or override that state.

The anti-stub probe sends fixture-absent message bodies, reads them back, checks
thread/target isolation, drains real inbox state, and exercises the
freshness-hold draft cursor, DM persistence, target-generic freshness, and
wall-clock sent timestamps.
