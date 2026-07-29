# swarm-cli

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
- `swarm daemon slack --workspace ...` for Socket Mode event intake into canonical state
- `swarm daemon wakes [--agent ...]` and `swarm daemon finish-turn --agent ...` for per-agent wake single-flight state
- `swarm daemon dispatch enqueue/claim/complete/fail/list` for leased daemon work
- `swarm daemon turn run --agent ... [--dispatch-id ... --owner ...]` to spawn a registered runtime with workspace cwd, runtime-specific session persistence (Claude `--resume`; Codex `exec` / `exec resume` JSON adapter), watchdog timeout, dispatch finalization, and credential-shaped output blocking
- `swarm daemon turn list [--agent ...] [--status ...]`
- `swarm curator install --source-config ... --entity-radar ... --owner-watchlist ... --report-target ... [--grind-server ... --grind-db ... --prompt-version ... --codex-home ...]` to register a scheduled curator agent with a scrubbed watch-list seed; without `--grind-server` it uses the local fixture queue, and with `--grind-server` it runs the real Grind `curator_jobs` queue through `npm run curator:codex`. For real Codex-backed Grind runs under the resident daemon, pass `--codex-home` to an already-authenticated Codex profile directory because the spawned agent HOME is its isolated workspace.
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
- `swarm agent register/list/heartbeat/seed/supervisor-plan`
- `swarm agent turn-context --name ... [--target ...] [--event-id ...] [--session-id ...] [--require-seed] [--require-memory] [--write-manifest] [--json]` to build a versioned per-turn context manifest
- `swarm agent worker --name ... [--once] [--require-seed]` for a persistent heartbeat loop backed by an agent workspace seed
- `swarm agent collab-smoke --channel ... --task-author ... --worker ... --verifier ...` to exercise the A→B→C task/report/verify path in canonical state
- Ordinary human Slack events entering `swarm daemon resident --workspace ...` now follow the live orchestration path automatically: durable ingest → metadata-only worker notices → capability route/claim → owner-only body query → native runtime turn → freshness-aware thread result/receipt → restart recovery. Simple greetings route to one owner; substantive goals can use the dissect/research/execute/verify/receipt lanes. The execute owner publishes one validated workspace-relative artifact path/SHA-256; non-execute lanes reference that exact artifact, and unavailable verification waits without rebuilding.
- `swarm agent orchestrate --channel ... --message-id ... [--max-workers ...]` exposes the same state-backed path as a replay/manual inspection seam; it is not a separate scripted demo path.
- `SWARM_DYNAMIC_TASKS_V1=1` enables the gated resumable, emergent-task path for ordinary human Slack roots. A claimed native bootstrap turn contributes the first validated variable `tasks[]`; any later claimed owner may append genuinely new work during its own fenced turn. Every accepted task is a visible message-backed task, create and claim remain separate, and open or capability-matched seats self-select through atomic claims. A task that needs more than one model turn persists a bounded checkpoint/next action, resumes the same task-owned harness session, and re-wakes the same owner+attempt until a typed `complete`, `held`, or `failed` verdict. The default remains the frozen fixed-lane path while this feature is independently gated.
- `swarm agent dynamic-start --channel ... --message-id ...` creates or idempotently reopens a dynamic planning run for a human root.
- `swarm agent task-claim --run-id ... --graph-version ... --task-key ... --expected-attempt ... --agent ...` exposes the cheap seat-local claim preflight and fencing attempt.
- `swarm agent plan-commit --run-id ... --attempt ...` accepts typed plan JSON on stdin only from the current live planner turn after its explicit owner read.
- `swarm agent task-create --run-id ... --graph-version ... --parent-task-key ... --attempt ...` accepts a typed append batch only from the current claimed owner turn. New rows carry creator/parent provenance, are visible and unowned, and reuse the normal notice/claim/fence path.
- `swarm agent task-progress-commit --run-id ... --graph-version ... --task-key ... --attempt ...` records exactly one typed `continue|complete|held|failed` verdict for the current turn. `continue` requires a privacy-scoped checkpoint and non-empty next action. `complete` requires zero-based `checkpoint.acceptance_evidence` coverage for every declared acceptance criterion; prose alone cannot complete a task. The default eight-turn budget ends in an explicit hold.
- `swarm agent dynamic-list [--run-id ...] [--json]` inspects dynamic runs, tasks, attempts, leases, and receipts.
- `swarm slack configure --workspace ... --bot-token-env ... [--signing-secret-env ...] [--app-token-env ...]`
- `swarm slack env --workspace ...`
- `swarm slack export-history --workspace ... --channel-id ... [--channel-name ...] [--include-replies]` to export Slack channel history as ingest-compatible event JSON rows
- `swarm slack ingest [--event-file ...]` to import a Slack message-event JSON payload into swarm state
- `swarm slack resolve --workspace ... --channel-id ... --ts ...` to resolve a Slack message timestamp to its swarm target/message
- `swarm slack outbound --workspace ... (--target ...|--message-id ...) [--after-seq ...]` to render Slack `chat.postMessage` request plans from canonical swarm messages
- `swarm slack send --workspace ... (--target ...|--message-id ...) [--after-seq ...] [--mock-response-file ...]` to send rendered plans through Slack Web API and record successful returned timestamps
- `swarm slack mark-sent --workspace ... --message-id ... --ts ... [--channel-id ...]` to persist the Slack timestamp returned by a later sender
- `--content` rejection
- local SQLite-backed persistence
- generated message IDs and wall-clock sent timestamps

It does not implement a general network coordination server or remote
integration authentication. Canonical coordination state remains single-host
SQLite. With operator-provided environment credentials, the Slack daemon can
consume a real Socket Mode stream and the sender can call `chat.postMessage`;
Slack OAuth and workspace provisioning remain outside this repo. Other
integration login records are local placeholders, and prepared actions remain
pending records/messages for a human commit path.

## Slack Adapter Boundary

Slack is treated as an adapter input and UI surface, not as the canonical
coordination store. The adapter can export Slack channel history into local
Slack-style message-event JSON, ingest those events, store workspace
configuration by environment-variable name, consume Socket Mode events, render
outbound request plans, and send through a small Slack Web API seam for
`chat.postMessage`. It does not implement Slack OAuth, workspace provisioning,
or an HTTP Events API endpoint.

`swarm slack configure` persists only names such as `SLACK_BOT_TOKEN`; it never
stores token or signing-secret values. `swarm slack env` shows the configured
names a real adapter process would need in its environment.

`swarm slack export-history` is a read-only bridge from Slack Web API history
to the existing ingest seam. It calls `conversations.history` and optionally
`conversations.replies`, then emits newline-delimited event JSON on stdout that
can be replayed row-by-row through `swarm slack ingest`. Status and
token-source notes go to stderr so stdout remains machine-consumable. History
rows that the current ingest seam does not support, such as Slack system
subtypes, are skipped instead of emitted as invalid events. The exporter does
not mutate swarm SQLite state; ingest remains the only path that appends
canonical swarm messages or `slack_messages` mappings.

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

`swarm slack send` is the real sender seam. It reads the bot token at call time
from the configured environment-variable name, never writes the token value to
state or output, calls Slack Web API only after rendering a plan from swarm
state, and records a returned Slack `ts` only after a successful response. The
`--mock-response-file` flag injects Slack-like JSON responses for offline tests,
so plan rendering and ledger acknowledgement stay verifiable without network
access. Failed Slack responses do not create `slack_messages` mappings.

The sender assumes one adapter worker owns a given workspace/target stream. If
multiple send workers are introduced, add an explicit "sending" reservation
state before the Slack POST so two workers cannot both pass preflight and post
the same swarm message before either one records Slack's returned timestamp.

Together these commands define the process boundary for a later
`swarm-slack-adapter` process to perform real Slack event subscription while
swarm remains the state owner.

## Verify

From the `swarm-harness` checkout:

```bash
SWARM_CANDIDATE_STATE_DIR="$(mktemp -d)" SWARM_CANDIDATE_SEED_FIXTURES=1 SWARM_CLI=/path/to/swarm-cli/swarm python3 scripts/contract_check.py --live
```

From this checkout:

```bash
python3 scripts/anti_stub_probe.py
python3 scripts/behavior_eval_loop.py
python3 scripts/behavior_eval_loop.py --manifest docs/evals/scenario-consult-old-evidence.json
python3 scripts/behavior_eval_loop.py --manifest docs/evals/scenario-chat-task-orchestration.json
python3 scripts/consult_old_evidence_probe.py
python3 scripts/dynamic_task_probe.py
python3 scripts/resumable_emergent_probe.py
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
plan rendering without network sends, mocked Slack Web API send + mark-sent
acknowledgement mapping, failed-send ledger protection, and persisted mapping
rows.

`scripts/behavior_eval_loop.py` is an implementation-owned smoke runner, not the
final acceptance judge. It consumes a JSON scenario manifest
(`docs/evals/probes-smoke-v0.json` by default), executes local probes, and
emits structured evidence with `scenario_id`, `fixture`, `timeline`,
`factors`, `evidence`, `metrics`, and advisory `runner_verdict` fields. This
slice records factor values and accepts repeated `--factor key=value` metadata
overrides; factor-specific runtime switches should be added with the matching
behavior implementation. Verifier-owned acceptance remains separate: passing
this smoke loop means the slice is ready for independent gating, not that full
behavioral coverage has been achieved.

The consult-old-evidence scenario seeds a real prior message outside the
automatic turn context. The runner must use the public `message search` and
`message read` CLI surfaces before it can commit an answer, and the answer
must cite the seeded message's stable channel/message reference. The companion
probe randomizes the fact and verifies fail-closed paths for injected context,
missing retrieval, missing provenance, and answer-without-query bypasses.

The chat-task-orchestration scenario is a trace/eval preflight for the live
runtime, not a replacement for a Slack test. It drives five distinct-capability
owners through the same daemon turn boundary and checks the 15 release
conditions in `docs/evals/scenario-chat-task-orchestration.json`, including
notice-first body withholding, owner-only body reads, loser conflict-stop,
freshness, lifecycle, exact-thread receipts, restart idempotency, lane-role
artifact fidelity, and coordination SLOs. Every owner must also use the real same-state CLI to list
visible channels, check source-channel membership, read a different visible
channel on demand, and still commit only to the exact source thread. Native
Codex and Claude workers launch with their
approval/filesystem sandbox bypass modes explicitly enabled, while child
environment credential scrubbing, credential-shaped output blocking, exact
target commits, and freshness/idempotency receipts remain enforced. Each turn
also performs a harmless workspace create/read/delete smoke before the native
runtime starts. Direct user acceptance still means starting five real workers
on the exact gated public SHA and sending an ordinary Slack message.

`scripts/dynamic_task_probe.py` is the focused offline gate for the feature-
flagged dynamic path. It launches real helper runtimes and barrier-synchronized
seat claim clients against one isolated SQLite store, then checks winner/loser
CAS evidence, 0/0/0/0 loser economics, typed-plan provenance and idempotency,
phase opening, timeout takeover and stale-attempt fencing, shape-changing steer
hold behavior, no-eligible escalation, exact-once receipts, and restart replay.
The verifier-owned frozen acceptance contract is
`docs/evals/scenario-emergent-task-graph-v1.json`; its live five-seat and
multi-goal matrix remains a separate release gate.

`scripts/resumable_emergent_probe.py` is the focused continuation and
open-creation gate. It restarts the resident process mid-chain, completes one
task across three resumed turns under one owner+attempt+session, verifies
idempotent typed progress and evidence-gated completion, lets that non-bootstrap
owner append a visible child task, proves create/claim separation by having a
different seat win the child, observes a real phase barrier, and replays the
root without duplicating the append. It also rejects and audits conflicting
same-turn appends and a global over-budget append without mutating existing
tasks, while checking metadata-only notice fan-out and 0/0/0/0 losing-claim
economics. A separate chain emits eight consecutive continue verdicts and must
end in an explicit budget hold with exactly one terminal escalation receipt.
The neutral locked contracts are
`docs/evals/scenario-long-horizon-continuation-v1.json` and
`docs/evals/scenario-open-task-creation-v1.json`.
