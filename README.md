# swarm-candidate-impl

Minimal candidate `swarm` CLI built from the public executable contract.

Phase 1 intentionally covers only the v0 CLI surface:

- `swarm message check`
- `swarm message read --channel ...`
- `swarm message send --target ...` using stdin
- freshness-hold draft output
- `--content` rejection

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

The local implementation stores deterministic live-check state outside the repo
by default. Set `SWARM_CANDIDATE_STATE_DIR` to inspect or override that state.

The anti-stub probe sends fixture-absent message bodies, reads them back, checks
thread/target isolation, drains real inbox state, and exercises the
freshness-hold draft cursor.

## Known v0 Limits

- Freshness hold is implemented for the seeded `#general` channel only.
- DM targets are out of Phase 1 scope.
- Sent fixture timestamps are deterministic rather than wall-clock generated.
