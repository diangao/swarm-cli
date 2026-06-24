# swarm-candidate-impl

Working `swarm` CLI implementation base.

This repo now keeps the frozen public contract as a regression baseline while
moving the CLI toward product behavior that can be used day to day.

Current implemented surface:

- `swarm message check`
- `swarm message read --channel ... [--limit ...] [--before/--after/--around ...]`
- `swarm message search --query ... [--channel/--sender/--sort/--before/--after/--limit ...]`
- `swarm message resolve <id>`
- `swarm message react --message-id ... --emoji ... [--remove]`
- `swarm message send --target ... [--attachment-id ...]` using stdin for channels, threads, and DMs
- freshness-hold draft output across message targets with newer local context
- `swarm task create --channel ... --title ... [--title ...]`
- `swarm task list --channel ... [--status ...] [--assignee ...|--mine|--unassigned]`
- `swarm task claim --channel ... (--number ...|--message-id ...) [...]`
- `swarm task unclaim --channel ... --number ... [--number ...]`
- `swarm task update --channel ... --number ... --status ...`
- `swarm reminder schedule --title ... --at ...`
- `swarm reminder list`
- `swarm reminder snooze --id ... --until ...`
- `swarm reminder update --id ...`
- `swarm reminder cancel --id ...`
- `swarm reminder log --id ...`
- `swarm daemon run [--once] [--poll-interval ...]` for local reminder auto-fire
- `swarm server info`
- `swarm channel members ...`
- `swarm channel join ...`
- `swarm channel leave ...`
- `swarm thread unfollow ...`
- `swarm profile show`
- `swarm profile update --display-name ... --description ... [--avatar-url ...|--avatar-file ...]`
- `swarm integration list`
- `swarm integration login --service ... [--account ...]`
- `swarm integration env --service ...`
- `swarm attachment upload --path ... --channel ... [--mime-type ...]`
- `swarm attachment view --id ... --output ...`
- `swarm action prepare --target ...` for local pending `channel:create` / `agent:create` action cards
- `swarm slack configure --workspace ... --bot-token-env ... [--signing-secret-env ...] [--app-token-env ...]`
- `swarm slack env --workspace ...`
- `swarm slack ingest [--event-file ...]` to import a Slack message-event JSON payload into swarm state
- `swarm slack resolve --workspace ... --channel-id ... --ts ...` to resolve a Slack message timestamp to its swarm target/message
- `swarm slack outbound --workspace ... (--target ...|--message-id ...) [--after-seq ...]` to render Slack `chat.postMessage` request plans from canonical swarm messages
- `swarm slack mark-sent --workspace ... --message-id ... --ts ... [--channel-id ...]` to persist the Slack timestamp returned by a later sender
- `--content` rejection
- local SQLite-backed persistence
- generated message IDs and wall-clock sent timestamps

It does not implement a network server, remote integration authentication, or
production workspace access. The daemon, integration, and action-card surfaces
are local-only in this slice: the daemon scans the same SQLite store and fires
due reminders, integration login creates a local placeholder record plus
per-service environment paths without third-party identity exchange, and
prepared actions are pending records/messages for a human commit path, not a
remote execution backend.

## Slack Adapter Boundary

Slack is treated as an adapter input and UI surface, not as the canonical
coordination store. The current seam accepts local Slack-style message-event
JSON, stores workspace configuration by environment-variable name, and renders
outbound request plans. It does not connect to Slack Web API, Events API, OAuth,
Socket Mode, or real workspaces.

`swarm slack configure` persists only names such as `SLACK_BOT_TOKEN`; it never
stores token or signing-secret values. `swarm slack env` shows the configured
names a real adapter process would need in its environment.

`swarm slack ingest` maps a Slack root message to a swarm channel target derived
from the Slack channel id (`C123` -> `#slack-c123`), stores a durable
`slack_messages` mapping row, appends the canonical swarm message, and enqueues
normal local inbox delivery. Slack thread replies require the root Slack message
to have been ingested first; replies map to the canonical swarm thread target
derived from the root swarm message id. Duplicate Slack events are idempotent
and resolve back to the original swarm message instead of appending another row.

This keeps task, reminder, claim, read/search/resolve, and freshness semantics
owned by swarm's SQLite state.

`swarm slack outbound` reads canonical swarm messages and renders newline-
delimited `chat.postMessage` request plans with Slack channel ids, text,
`client_msg_id`, and thread timestamps when a swarm thread maps to a Slack
thread. It sends no network request. `swarm slack mark-sent` is the durable
acknowledgement seam for a future real sender: after Slack returns a `ts`, the
adapter records that timestamp against the swarm message so later outbound
plans skip messages that are already mapped.

Together these commands define the local process boundary for a later
`swarm-slack-adapter` process to perform real Slack authentication, event
subscription, and message sending while swarm remains the state owner.

## Verify

From the `swarm-harness` checkout:

```bash
SWARM_CANDIDATE_STATE_DIR="$(mktemp -d)" SWARM_CANDIDATE_SEED_FIXTURES=1 SWARM_CLI=/path/to/swarm-candidate-impl/swarm python3 scripts/contract_check.py --live
```

From this checkout:

```bash
python3 scripts/anti_stub_probe.py
```

The local implementation stores state in `state.sqlite3`. By default it uses
the user state directory (`$XDG_STATE_HOME/swarm-cli` or
`~/.local/state/swarm-cli`); set `SWARM_CANDIDATE_STATE_DIR` for isolated test
runs or to inspect a specific store.

Fresh product stores start empty: no fixture messages, tasks, channels, or
inbox entries are injected. The frozen public harness still expects its
historical contract fixtures, so test runs that need those rows must set
`SWARM_CANDIDATE_SEED_FIXTURES=1` against an isolated state directory.

The anti-stub probe sends fixture-absent message bodies, reads them back, checks
that unseeded fresh stores are empty, then uses explicit test fixtures for
history pagination and bounded limits, message search/resolve, thread/target isolation, drains
real inbox state, and exercises the freshness-hold draft cursor, DM
persistence, target-generic freshness, and wall-clock sent timestamps. It also
checks SQLite-backed message reaction add/remove rendering, task lifecycle
create/list/claim/unclaim/update behavior,
including repeatable task create/claim/unclaim flags, reminder
schedule/list/snooze/update/cancel/log plus local daemon auto-fire behavior,
local server/channel/profile
catalog reads, profile update and avatar persistence, channel join/leave,
thread unfollow state, local integration manifest/login/env state, local
attachment upload/view byte persistence, message attachment rendering,
persisted action-card preparation, and concurrent write serialization.
It also checks Slack adapter root-message import, duplicate idempotence,
thread-root fail-closed behavior, Slack-to-swarm resolve, inbox delivery,
channel cataloging, workspace env-name configuration, outbound `chat.postMessage`
plan rendering without network sends, mark-sent acknowledgement mapping, and
persisted mapping rows.
