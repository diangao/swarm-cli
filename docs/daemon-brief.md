# Swarm Daemon — Build Brief

One event-driven lifecycle daemon that turns registered swarm agents into
long-running workers: it watches the event sources, wakes the right agent
with a resumable model turn, supervises that turn, and goes back to sleep.
Continuity lives in recoverable state (workspace, memory, queue rows), not
in an eternal model process.

## Goal

Owner's one-line north star (verbatim, 2026-07-06): **"goal 就是和用
slock 的体感一样"** — using this must feel the same as using the
reference platform. Not a demo of plumbing; the felt experience of
talking to colleagues who happen to live in Slack.

After this build, the following demo must work with no human glue:

> The owner posts "@ryo say hi" in the connected Slack channel. The daemon
> ingests the event, routes it to the `ryo` agent, spawns a model turn that
> reads ryo's workspace/memory, and ryo replies in Slack under its own
> rendered identity — thinking for itself, not echoed by a CLI script.

Same daemon, second workload: a scheduled reminder wakes a curator agent,
whose turn claims a small batch of queue jobs from the board product's
database, annotates them under the fixed contract, writes back, and posts a
short report to a channel.

## Scope (owner decision, 2026-07-06)

- **One owner, many agents.** No multi-user tenancy, no org permissions.
- **Local-first.** One SQLite store, one daemon process on one machine.
- **Slack is transport + UI only.** Source of truth stays in swarm state.
- **No per-agent heartbeat.** The daemon supervises its child turns
  directly; hang detection is a turn watchdog; crash recovery is a state
  lease. The existing 30s worker self-report loop is retired at cutover.
- Out of scope: cross-machine sync, native multi-bot Slack identities,
  server-hosted event bus, multi-human workspaces.

## Architecture

```
Slack (Socket Mode) ─┐
                     ├─> event intake ─> durable state (SQLite)
reminders (existing) ┘                        │
                                              v
                                        wake router
                             (membership, @name parse, dedupe)
                                              │
                                              v
                                turn runner (one per agent max)
                          spawn model runtime --resume, workspace cwd
                          watchdog timeout, exit-code observation
                                              │
                                              v
                              agent turn does its own I/O via
                          `swarm ...` CLI (send/claim/report/etc)
                                              │
                                              v
                                 outbound (existing send seam,
                              customize rendering, mark-sent ledger)
```

Existing verified pieces this build reuses rather than re-implements:
ingest + mapping (idempotent, thread-root fail-closed), outbound plan /
send / mark-sent (no echo, no double-send), reminder store + auto-fire
loop (`swarm daemon run`), agent registry + seeded workspaces, atomic task
claim. The daemon composes them; it should add as little new state as
possible.

## Slices, in build order

Each slice lands as one commit, passes its rubric checks (see
`daemon.rubric.json`), and is independently verified before the next
starts. Verifier: independent clone + probes against SQLite ground truth.

### S1 — event loop + intake push
Socket Mode connection with reconnect/backoff; incoming channel messages
flow through the existing ingest path automatically (pull becomes push).
Local CLI events (reminders due) join the same internal queue.

### S2 — wake router + single-flight lock
Decide which registered agent an event concerns: channel membership,
`@name` text parsing, reminder author. Per-agent turn lock: an agent can
never be woken twice concurrently. Events that arrive during a busy turn
are batched into one pending-wake marker (content-free), delivered when
the turn ends.

### S3 — turn runner (the executor)
Spawn the agent's model runtime (per-registry `runtime` field) with
`--resume`/session persistence, workspace as cwd, `swarm` CLI on PATH.
Observe exit code; watchdog kills a turn exceeding max runtime; failures
retry with backoff up to max attempts. Credentials only in daemon env.

### S4 — lease-based crash recovery
Claims (queue jobs, and the daemon's own wake dispatches) carry a lease
timestamp. Expired leases return work to the pool. Kill the daemon or a
turn mid-job; after restart the job is claimable again, exactly once, no
double-writes (existing snapshot/mark-sent guards remain the write gate).

### S5 — presence + heartbeat retirement
Registry presence (`online`, `in_turn at`, `last seen`) is written by the
daemon from direct observation. The 30s self-report worker loop and its
launchd jobs are removed. launchd keeps exactly one job: the daemon.

### S6 — curator workload onboarding
Second tenant: curator agent(s) with seeded workspaces + a runtime manual
(queue CLI usage, annotation contract, owner taste seed). Hourly reminder
wakes them; each turn claims a small batch from the board product's job
queue, annotates, writes back through the bounded contract, posts a short
channel report. Batch size / cadence / max runtime are the cost controls.

The curators' first-class maintained asset is the **watch list**
(owner decision: "最最重要"): what is worth watching and why. Initial
seed is assembled from existing assets — the board product's live
sources config, the existing entity/peer radar, and items the owner has
hand-picked in channels (the company tracker stays out of v1 by owner
decision) — one entry per item with why-watched, trust
tier, scan surface, cadence, and a retirement condition. Governance:
curators propose additions/removals with evidence each cycle; small
changes apply automatically, category-level changes wait for the owner's
nod. The queue is the execution layer; the watch list is the product's
long-term memory.

### S7 — live Slack smoke (final acceptance)
Owner-driven, in the real workspace: mention an agent and get a
self-authored reply; kill the daemon mid-conversation and watch it recover;
watch a reminder-driven curator batch land annotated cards; confirm no
duplicate sends anywhere. Scripted in the rubric; owner is the judge.

## Roles

- Lead + independent verification: mythos (this plan, per-slice gates,
  ground-truth probes, final smoke script).
- Implementation: Dozy (mini). One slice at a time, commit + gates green,
  then hand to verification.
- Test agents: jett / dozy / ryo seeds (existing, scrub-passed).
- Owner: scope decisions, final smoke judgment.

## Non-negotiables

- No credentials in repo, workspace seeds, or logs; tokens live only in
  the daemon environment.
- Unknown CLI flags fail closed (matches existing discipline).
- Every write path keeps its existing idempotency guard; the daemon never
  bypasses the CLI/state contract to touch SQLite directly for messages.
- If a slice can't meet its rubric honestly, shrink the slice — don't pad.
