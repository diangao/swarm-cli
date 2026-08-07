import { isChannelTarget, isValidBrandedId } from "./evidence.js";
import type {
  BodyQueryDeliveryFact,
  ClaimAttemptFact,
  DecompositionFact,
  DeliveryObservationFact,
  EvidenceLedger,
  NoticeDeliveryFact,
} from "./evidence.js";
import type { Condition, ConditionVerdict, Scenario } from "./scenario.js";

// Delivery discriminated-union narrowing guards.
function isNotice(o: DeliveryObservationFact): o is NoticeDeliveryFact {
  return o.kind === "notice_metadata";
}
function isBodyQuery(o: DeliveryObservationFact): o is BodyQueryDeliveryFact {
  return o.kind === "body_read";
}

// Lowercase-hex SHA-256 digest shape, re-checked at the oracle (fixtures built
// via the builder bypass the ingestion parser).
const SHA256_HEX = /^[0-9a-f]{64}$/u;

// Defensive target/ref checks reuse the SHARED ingestion validators
// (isChannelTarget / isValidBrandedId) so a predicate called directly — parser
// and runner bypassed — still cannot PASS a malformed target or message ref,
// and the grammar cannot drift from ingestion.

// The condition predicates: the correctness core. Each faithfully encodes the
// `assert` text of the source acceptance contract
// (scenario-chat-task-orchestration.json, pass_conditions) and returns a
// ConditionVerdict whose conditionId is EXACTLY the source id string.

function pass(conditionId: string, reason: string): ConditionVerdict {
  return { conditionId, status: "pass", reason };
}

function fail(conditionId: string, reason: string): ConditionVerdict {
  return { conditionId, status: "fail", reason };
}

function placeholder(conditionId: string, reason: string): ConditionVerdict {
  return { conditionId, status: "placeholder", reason };
}

// The SINGLE canonical decomposition/root for the scenario. Every asserted
// predicate that reasons about the task graph MUST consume this one object, so
// provenance cannot be spliced from an unrelated graph (decomposed_by_runtime
// borrowing one graph while routing/ownership read another). Ambiguous or
// missing graph evidence (0 or >1 decompositions) is rejected: there is exactly
// one goal decomposition per scenario run, so multiple rows are not a valid
// graph and every consuming predicate fails closed.
function canonicalDecomposition(
  ledger: EvidenceLedger,
): DecompositionFact | undefined {
  return ledger.decompositions.length === 1
    ? ledger.decompositions[0]
    : undefined;
}

// The canonical subtask id set. EVERY subtask-referencing predicate joins to
// this, so evidence (routes, claims, executions, statuses, restarts, steers,
// deliveries) can only prove the scenario's canonical subtasks — never an
// unrelated task spliced in to satisfy a leg on phantom work. Undefined when
// there is no single canonical decomposition (0 or multiple graphs).
function canonicalSubtaskIdSet(
  ledger: EvidenceLedger,
): ReadonlySet<string> | undefined {
  const decomp = canonicalDecomposition(ledger);
  if (decomp === undefined) return undefined;
  return new Set(decomp.subtasks.map((s) => s.taskId as string));
}

// The canonical winning lane set: each canonical subtask that has exactly one
// winning claim, mapped to its winner seat. This is the routed/winning lane set
// completeness checks iterate — every winning lane must carry all its legs
// (wake, notice-first, post-claim body read).
function canonicalWinners(
  ledger: EvidenceLedger,
  canonicalIds: ReadonlySet<string>,
): ReadonlyMap<string, string> {
  const winners = new Map<string, string>();
  for (const subtaskId of canonicalIds) {
    const won = ledger.claims.filter(
      (c) => (c.subtaskId as string) === subtaskId && c.outcome === "won",
    );
    if (won.length === 1) winners.set(subtaskId, won[0]!.seat);
  }
  return winners;
}

// ---------------------------------------------------------------------------
// 1. decomposed_by_runtime
// PASS iff a DecompositionFact exists with authoredBy === "runtime", a defined
// modelTurnId, and >= 2 subtasks each with a non-empty capability.
// Catches seed S7 (stub plan / no model turn provenance).
// ---------------------------------------------------------------------------
const decomposedByRuntime: Condition = {
  id: "decomposed_by_runtime",
  evidence: ["decomposition"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "decomposed_by_runtime";
    // Provenance must come from the SINGLE canonical graph, never spliced from
    // an unrelated runtime graph while downstream predicates read another.
    if (ledger.decompositions.length === 0) {
      return fail(id, "no decomposition present");
    }
    if (ledger.decompositions.length > 1) {
      return fail(
        id,
        `${ledger.decompositions.length} decompositions present; exactly one canonical graph is required (ambiguous/spliceable provenance)`,
      );
    }
    const decomp = ledger.decompositions[0]!;
    if (decomp.authoredBy !== "runtime") {
      return fail(
        id,
        `decomposition authoredBy="${decomp.authoredBy}", not "runtime" (stub/human-authored)`,
      );
    }
    if (decomp.modelTurnId === undefined) {
      return fail(
        id,
        "runtime decomposition has no modelTurnId (no model turn provenance)",
      );
    }
    if (decomp.subtasks.length < 2) {
      return fail(
        id,
        `decomposition has ${decomp.subtasks.length} subtask(s), need >= 2 (flat task)`,
      );
    }
    const distinct =
      new Set(decomp.subtasks.map((s) => s.taskId as string)).size ===
      decomp.subtasks.length;
    if (!distinct) {
      return fail(id, "decomposition has duplicate subtask ids (not distinct)");
    }
    if (!decomp.subtasks.every((s) => s.capability.length > 0)) {
      return fail(id, "a subtask has an empty capability");
    }
    return pass(
      id,
      `runtime-authored canonical decomposition with model turn provenance and ${decomp.subtasks.length} capability-typed subtasks`,
    );
  },
};

// ---------------------------------------------------------------------------
// 2. routed_by_capability
// FAIL-CLOSED. Requires the decomposition's subtasks to EXIST, and for EVERY
// decomposed subtask: a RouteFact whose routedCapability EQUALS the subtask's
// capability, a DEFINED matchedWorkerSeat, and that seat is a member of
// availableWorkerSeats. Missing route, capability mismatch, undefined seat, or
// an unavailable seat -> FAIL. The prior "pass an unroutable subtask when no
// matching worker exists" false-pass path is REMOVED: the source contract says
// each subtask must be routed to a lane/capability that matches an available
// worker, and the scenario fixture guarantees a matching worker for each seeded
// capability, so an unroutable subtask is always a routing defect here.
// ---------------------------------------------------------------------------
const routedByCapability: Condition = {
  id: "routed_by_capability",
  evidence: ["routing", "decomposition"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "routed_by_capability";
    const decomp = canonicalDecomposition(ledger);
    if (decomp === undefined) {
      return fail(
        id,
        "no single canonical decomposition to route (missing or ambiguous graph evidence)",
      );
    }
    if (decomp.subtasks.length === 0) {
      return fail(id, "decomposition has no subtasks to route");
    }
    const available = new Set<string>(ledger.availableWorkerSeats);
    const seatBySubtask = new Map<string, string>();
    for (const subtask of decomp.subtasks) {
      // Exactly one route per subtask: two routes (one valid, one not) must not
      // pass by first-match. Route cardinality mirrors execution cardinality.
      const routes = ledger.routes.filter(
        (r) => r.subtaskId === subtask.taskId,
      );
      if (routes.length !== 1) {
        return fail(
          id,
          `subtask ${subtask.taskId} has ${routes.length} routes (expected exactly one)`,
        );
      }
      const route = routes[0]!;
      if (route.routedCapability !== subtask.capability) {
        return fail(
          id,
          `subtask ${subtask.taskId} routed to capability "${route.routedCapability}" but its capability is "${subtask.capability}"`,
        );
      }
      if (route.matchedWorkerSeat === undefined) {
        return fail(
          id,
          `subtask ${subtask.taskId} has no matched worker seat (unroutable)`,
        );
      }
      if (!available.has(route.matchedWorkerSeat)) {
        return fail(
          id,
          `subtask ${subtask.taskId} routed to seat "${route.matchedWorkerSeat}" which is not an available worker`,
        );
      }
      seatBySubtask.set(subtask.taskId as string, route.matchedWorkerSeat);
    }
    // Distinct-owner clause: route owner seats must be INJECTIVE across the
    // parallel lanes; one seat may not own two capability-distinct subtasks.
    const seats = [...seatBySubtask.values()];
    if (new Set(seats).size !== seats.length) {
      return fail(
        id,
        "route owner seats are not distinct across the parallel lanes (a seat owns more than one lane)",
      );
    }
    return pass(
      id,
      `all ${decomp.subtasks.length} subtasks routed to a capability-matching, available, distinct worker seat`,
    );
  },
};

// ---------------------------------------------------------------------------
// 3. single_owner_no_duplicate
// FAIL-CLOSED, joined to the decomposition. Requires claims AND executions to
// EXIST for every decomposed subtask. For EACH subtask:
//   - exactly ONE "won" claim (0 winners or >1 winners -> FAIL);
//   - every non-winning claimant is "lost" or "conflict_stop" (a contested
//     subtask whose non-winners are not stopped -> FAIL);
//   - exactly ONE execution, by the winning seat (no execution, execution by a
//     non-winner, executionCount>1, or two distinct executing seats -> FAIL).
// An empty ledger FAILs because the decomposed subtasks have no claims. Catches
// seed M6 (double winner / double spawn) and S6 (loser executes).
// ---------------------------------------------------------------------------
const singleOwnerNoDuplicate: Condition = {
  id: "single_owner_no_duplicate",
  evidence: ["claims", "execution"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "single_owner_no_duplicate";
    const decomp = canonicalDecomposition(ledger);
    if (decomp === undefined || decomp.subtasks.length === 0) {
      return fail(
        id,
        "no single canonical decomposition to check ownership for (missing or ambiguous graph)",
      );
    }
    for (const subtask of decomp.subtasks) {
      const subtaskId = subtask.taskId as string;
      const claims = ledger.claims.filter(
        (c) => (c.subtaskId as string) === subtaskId,
      );
      if (claims.length === 0) {
        return fail(id, `subtask ${subtaskId} has no claim attempts`);
      }
      const winners = claims.filter((c) => c.outcome === "won");
      if (winners.length === 0) {
        return fail(id, `subtask ${subtaskId} has no winning claim`);
      }
      if (winners.length > 1) {
        return fail(
          id,
          `subtask ${subtaskId} has ${winners.length} winning claims (expected exactly one)`,
        );
      }
      const winnerSeat = winners[0]!.seat;
      // Contested: every non-winner must be lost/conflict_stop.
      const notStopped = claims.filter(
        (c) => c.outcome !== "won" && c.outcome !== "lost" && c.outcome !== "conflict_stop",
      );
      if (notStopped.length > 0) {
        return fail(
          id,
          `subtask ${subtaskId} has a non-winning claimant that did not stop`,
        );
      }
      // Execution CARDINALITY: exactly ONE execution row, count 1, by the
      // winner. Two rows each with count 1 (total 2) is a duplicate execution;
      // checking only per-row count and distinct seats misses it.
      const execs = ledger.executions.filter(
        (e) => (e.subtaskId as string) === subtaskId,
      );
      if (execs.length !== 1) {
        return fail(
          id,
          `subtask ${subtaskId} has ${execs.length} execution rows (expected exactly one)`,
        );
      }
      const exec = execs[0]!;
      if (exec.executionCount !== 1) {
        return fail(
          id,
          `subtask ${subtaskId} executionCount=${exec.executionCount} (expected exactly 1)`,
        );
      }
      if (exec.seat !== winnerSeat) {
        return fail(
          id,
          `subtask ${subtaskId} executed by non-winner seat ${exec.seat} (winner is ${winnerSeat})`,
        );
      }
      // Full-chain join: routed owner seat = winning claimant = execution seat
      // = thread owner, for this subtask (route -> winner -> execution ->
      // thread owner must be one seat).
      const route = ledger.routes.find(
        (r) => (r.subtaskId as string) === subtaskId,
      );
      if (route === undefined || route.matchedWorkerSeat !== winnerSeat) {
        return fail(
          id,
          `subtask ${subtaskId} winning seat ${winnerSeat} is not the routed owner seat`,
        );
      }
      const statuses = ledger.threadStatuses.filter(
        (t) => (t.subtaskId as string) === subtaskId,
      );
      if (statuses.length !== 1) {
        return fail(
          id,
          `subtask ${subtaskId} has ${statuses.length} thread statuses (expected exactly one)`,
        );
      }
      if (statuses[0]!.ownerSeat !== winnerSeat) {
        return fail(
          id,
          `subtask ${subtaskId} thread owner is not the winning seat ${winnerSeat}`,
        );
      }
    }
    return pass(
      id,
      `all ${decomp.subtasks.length} subtasks have exactly one winner, stopped losers, one owner execution, and a route->winner->execution->thread-owner chain`,
    );
  },
};

// ---------------------------------------------------------------------------
// 4. restart_no_reexecution
// FAIL-CLOSED. Requires restart/replay evidence to be PRESENT (the scenario
// injects a restart), and requires at least one completed subtask to observe
// across it. For every subtask carrying a restart marker, that subtask must be
// completed (done/closed) AND its execution must be exactly one (no duplicate
// execution/receipt after the restart). An empty ledger FAILs because no
// restart evidence exists. Catches seed S13 (re-execution after restart).
// ---------------------------------------------------------------------------
const restartNoReexecution: Condition = {
  id: "restart_no_reexecution",
  evidence: ["restart_idempotency", "execution"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "restart_no_reexecution";
    const canonicalIds = canonicalSubtaskIdSet(ledger);
    if (canonicalIds === undefined) {
      return fail(
        id,
        "no single canonical decomposition (missing or ambiguous graph)",
      );
    }
    // Canonical FILTER: consider only restart evidence on canonical subtasks;
    // unrelated-task restart rows are irrelevant noise, not violations. The
    // restart leg must be PROVEN on a canonical subtask (>= 1 canonical restart).
    const restarts = ledger.restarts.filter((r) =>
      canonicalIds.has(r.subtaskId as string),
    );
    if (restarts.length === 0) {
      return fail(id, "no restart on a canonical subtask (restart leg unproven)");
    }
    const completed = new Set<string>(
      ledger.threadStatuses
        .filter(
          (t) =>
            (t.status === "done" || t.status === "closed") &&
            canonicalIds.has(t.subtaskId as string),
        )
        .map((t) => t.subtaskId as string),
    );
    if (completed.size === 0) {
      return fail(id, "no completed canonical subtask to check idempotency across restart");
    }
    for (const restart of restarts) {
      const subtaskId = restart.subtaskId as string;
      // Bind the restart marker to the replayed root: replayOf must be present
      // and equal exactly one external wake delivery for THIS canonical subtask.
      // A marker with no replayOf, or one matching a different task's wake,
      // proves nothing about idempotent replay of the same root.
      if (restart.replayOf === undefined) {
        return fail(
          id,
          `restarted subtask ${subtaskId} has no replayOf (marker not bound to a replayed wake)`,
        );
      }
      const externalWakeDeliveries = ledger.wakes.filter(
        (w) =>
          (w.subtaskId as string) === subtaskId && w.wakeDeliveryId !== undefined,
      );
      const replayMatches = externalWakeDeliveries.filter(
        (w) => w.wakeDeliveryId === restart.replayOf,
      );
      if (replayMatches.length !== 1) {
        return fail(
          id,
          `restarted subtask ${subtaskId} replayOf matches ${replayMatches.length} same-subtask external wake deliveries (expected exactly one)`,
        );
      }
      if (!completed.has(subtaskId)) {
        return fail(
          id,
          `restarted subtask ${subtaskId} is not recorded completed after the restart`,
        );
      }
      const execs = ledger.executions.filter(
        (e) => (e.subtaskId as string) === subtaskId,
      );
      if (execs.length === 0) {
        return fail(
          id,
          `restarted subtask ${subtaskId} has no execution record`,
        );
      }
      const total = execs.reduce((sum, e) => sum + e.executionCount, 0);
      if (total !== 1) {
        return fail(
          id,
          `restarted subtask ${subtaskId} executed ${total} times across restart (expected exactly 1)`,
        );
      }
    }
    return pass(
      id,
      `all ${ledger.restarts.length} restarted subtask(s) completed with exactly one execution (no re-execution)`,
    );
  },
};

// ---------------------------------------------------------------------------
// 5. steer_honored_precommit
// FAIL-CLOSED. Requires >= 1 SteerFact PRESENT (the scenario injects one human
// correction). For every steer: (appliedBeforeCommit OR requiresReplanHold)
// AND committedStale === false. An empty steer set FAILs (the steer leg is
// unproven). Catches seeds S16 (shape-changing steer) / M4 (unsafe-boundary
// steer) via a committed-stale or unhonored steer.
// ---------------------------------------------------------------------------
const steerHonoredPrecommit: Condition = {
  id: "steer_honored_precommit",
  evidence: ["steer_effect"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "steer_honored_precommit";
    const canonicalIds = canonicalSubtaskIdSet(ledger);
    if (canonicalIds === undefined) {
      return fail(
        id,
        "no single canonical decomposition (missing or ambiguous graph)",
      );
    }
    // Canonical FILTER: consider only steers on canonical subtasks; unrelated
    // steer rows are irrelevant noise. The steer leg must be PROVEN on a
    // canonical subtask (>= 1 canonical steer).
    const steers = ledger.steers.filter((s) =>
      canonicalIds.has(s.subtaskId as string),
    );
    if (steers.length === 0) {
      return fail(id, "no steer on a canonical subtask (steer leg unproven)");
    }
    for (const steer of steers) {
      if (steer.committedStale) {
        return fail(
          id,
          `subtask ${steer.subtaskId} committed a stale pre-correction result`,
        );
      }
      if (!steer.appliedBeforeCommit && !steer.requiresReplanHold) {
        return fail(
          id,
          `subtask ${steer.subtaskId} steer neither applied before commit nor held for replan (silent mutation)`,
        );
      }
      // Bind the steer to the turn it corrected: expectedTurnId must equal
      // exactly one external wake turn for THIS canonical subtask. A steer whose
      // expected turn is not that subtask's in-flight wake turn (the frozen M4
      // wrong-turn fence) proves nothing about a precommit correction.
      const subtaskId = steer.subtaskId as string;
      const externalWakes = ledger.wakes.filter(
        (w) =>
          (w.subtaskId as string) === subtaskId && w.wakeDeliveryId !== undefined,
      );
      const turnMatches = externalWakes.filter(
        (w) => w.turnId === steer.expectedTurnId,
      );
      if (turnMatches.length !== 1) {
        return fail(
          id,
          `subtask ${subtaskId} steer expectedTurnId matches ${turnMatches.length} same-subtask external wake turns (expected exactly one)`,
        );
      }
    }
    return pass(
      id,
      "every steer applied before commit or held for replan, none committed stale, each bound to its subtask wake turn",
    );
  },
};

// ---------------------------------------------------------------------------
// 6. wake_starts_turn
// FAIL-CLOSED. Requires >= 1 WakeProvenanceFact PRESENT (each owner turn for
// the goal must be externally woken), and every wake must carry a defined
// wakeDeliveryId. "all 0 turns" FAILs (no external-wake provenance recorded),
// as does any orphan turn with no wakeDeliveryId (resident-poll origin).
// ---------------------------------------------------------------------------
const wakeStartsTurn: Condition = {
  id: "wake_starts_turn",
  evidence: ["claims", "thread_status"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "wake_starts_turn";
    const canonicalIds = canonicalSubtaskIdSet(ledger);
    if (canonicalIds === undefined) {
      return fail(
        id,
        "no single canonical decomposition (missing or ambiguous graph)",
      );
    }
    const winners = canonicalWinners(ledger, canonicalIds);
    if (winners.size === 0) {
      return fail(id, "no canonical winning owner turn to prove externally woken");
    }
    // Completeness + canonical join: EVERY canonical winning owner turn must have
    // an external wake bound to its subtask, each carrying a delivery id. One
    // wake cannot prove two distinct owners were woken.
    for (const [subtaskId] of winners) {
      const wakes = ledger.wakes.filter(
        (w) => (w.subtaskId as string) === subtaskId,
      );
      if (wakes.length === 0) {
        return fail(
          id,
          `canonical subtask ${subtaskId} owner turn has no external wake`,
        );
      }
      const orphan = wakes.find((w) => w.wakeDeliveryId === undefined);
      if (orphan !== undefined) {
        return fail(
          id,
          `canonical subtask ${subtaskId} wake ${orphan.turnId} has no external delivery (resident-poll origin)`,
        );
      }
    }
    return pass(
      id,
      `all ${winners.size} canonical winning owner turns externally woken`,
    );
  },
};

// ---------------------------------------------------------------------------
// 7. notice_first_body_withheld
// FAIL-CLOSED, joined to the canonical decomposition. Proves durable-body-
// before-fanout, not a weaker per-lane insertion-order surrogate:
//   - EXACTLY ONE valid BodyDurabilityFact for the canonical parent (positive
//     bytes, SHA-256 digest);
//   - every canonical claim participant (winner or loser) has a valid content-
//     free NOTICE on its own (seat, subtask) lane;
//   - the durability ordinal precedes every canonical NOTICE, and every NOTICE
//     precedes every canonical claim and every canonical body query.
// Ambient non-canonical rows are filtered. Missing/duplicate/zero-byte
// durability, a missing/malformed NOTICE, or any out-of-order ordinal -> FAIL.
// ---------------------------------------------------------------------------
const noticeFirstBodyWithheld: Condition = {
  id: "notice_first_body_withheld",
  evidence: ["delivery_observation", "claims"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "notice_first_body_withheld";
    const decomp = canonicalDecomposition(ledger);
    if (decomp === undefined) {
      return fail(
        id,
        "no single canonical decomposition (missing or ambiguous graph)",
      );
    }
    const canonicalIds = new Set(decomp.subtasks.map((s) => s.taskId as string));
    const parentId = decomp.parentTaskId as string;

    // (1) Exactly one valid durable-body receipt for the canonical parent.
    const durability = ledger.bodyDurability.filter(
      (d) => (d.parentTaskId as string) === parentId,
    );
    if (durability.length !== 1) {
      return fail(
        id,
        `${durability.length} body-durability facts for the canonical parent (expected exactly one)`,
      );
    }
    const dur = durability[0]!;
    if (!(dur.bodyBytes > 0)) {
      return fail(id, "canonical body durability has non-positive byte size");
    }
    if (!SHA256_HEX.test(dur.bodyDigest)) {
      return fail(id, "canonical body durability digest is not a SHA-256 hex");
    }

    const participants = ledger.claims.filter((c) =>
      canonicalIds.has(c.subtaskId as string),
    );
    if (participants.length === 0) {
      return fail(id, "no canonical claim participant to prove notice-first");
    }

    const canonicalNotices = ledger.deliveries
      .filter(isNotice)
      .filter((o) => canonicalIds.has(o.subtaskId as string));
    const canonicalBodyQueries = ledger.deliveries
      .filter(isBodyQuery)
      .filter((o) => canonicalIds.has(o.subtaskId as string));

    // (2a) EVERY canonical NOTICE observation must be well-formed — validated
    // per row, not first-match-per-lane, so a malformed duplicate on a lane
    // cannot hide behind a valid first row (the direct-predicate seam). The
    // frozen source requires metadata-only NOTICE rows but does not cap a lane
    // at one, so legitimate replay/reconnect duplicates that are ALL valid pass.
    for (const notice of canonicalNotices) {
      if (
        notice.bodyPresent !== false ||
        !isChannelTarget(notice.target) ||
        !isValidBrandedId(notice.firstMessageId, "msg") ||
        !isValidBrandedId(notice.latestMessageId, "msg")
      ) {
        return fail(
          id,
          `canonical NOTICE for ${notice.seat} on subtask ${notice.subtaskId} is malformed (body present, non-channel target, or non-MessageId first/latest)`,
        );
      }
    }
    // (2b) Completeness: every canonical participant lane carries at least one
    // NOTICE (fan-out proven per participant).
    for (const claim of participants) {
      const seat = claim.seat;
      const subtaskId = claim.subtaskId as string;
      const has = canonicalNotices.some(
        (o) => o.seat === seat && (o.subtaskId as string) === subtaskId,
      );
      if (!has) {
        return fail(
          id,
          `canonical participant ${seat} on subtask ${subtaskId} has no NOTICE (fan-out unproven)`,
        );
      }
    }

    // (3) Global ordering: durability < every NOTICE < every canonical claim
    // and every canonical body query.
    for (const notice of canonicalNotices) {
      if (!(dur.ordinal < notice.ordinal)) {
        return fail(
          id,
          `NOTICE ordinal ${notice.ordinal} is not after the body-durability ordinal ${dur.ordinal} (fan-out before durable body)`,
        );
      }
    }
    const maxNoticeOrdinal = Math.max(
      ...canonicalNotices.map((o) => o.ordinal),
    );
    for (const claim of participants) {
      if (!(maxNoticeOrdinal < claim.ordinal)) {
        return fail(
          id,
          `canonical claim ordinal ${claim.ordinal} is not after every NOTICE (${maxNoticeOrdinal})`,
        );
      }
    }
    for (const query of canonicalBodyQueries) {
      if (!(maxNoticeOrdinal < query.ordinal)) {
        return fail(
          id,
          `canonical body query ordinal ${query.ordinal} is not after every NOTICE (${maxNoticeOrdinal})`,
        );
      }
    }
    return pass(
      id,
      "durable body committed before content-free notices to every canonical participant, and every notice precedes claims and body queries",
    );
  },
};

// ---------------------------------------------------------------------------
// 8. owner_only_body_read
// FAIL-CLOSED, joined to the canonical decomposition. Proves winner-only
// EXPLICIT post-claim body querying and a NON-VACUOUS conflict lane:
//   - every canonical winner performs an explicit body query (explicitQuery)
//     whose ordinal FOLLOWS its own claim and whose queryTurnId equals its
//     subtask's external-wake turn, with a valid target/message ref;
//   - every canonical body query belongs to its subtask's winner;
//   - at least one canonical conflict-stop/lost loser lane exists and PROVES
//     its zeros (exactly one zero loser-activity row + a loser NOTICE): a
//     ledger with no loser lane cannot vacuously pass;
//   - every canonical loser proves zero body-read / reply / execution.
// Absence of the conflict lane, a non-post-claim / wrong-turn / non-owner body
// read, or any loser activity -> FAIL. Ambient rows filtered.
// ---------------------------------------------------------------------------
const ownerOnlyBodyRead: Condition = {
  id: "owner_only_body_read",
  evidence: ["delivery_observation", "claims"],
  predicate: (ledger: EvidenceLedger): ConditionVerdict => {
    const id = "owner_only_body_read";
    const canonicalIds = canonicalSubtaskIdSet(ledger);
    if (canonicalIds === undefined) {
      return fail(
        id,
        "no single canonical decomposition (missing or ambiguous graph)",
      );
    }
    const isOwner = (seat: string, subtaskId: string): boolean =>
      ledger.claims.some(
        (c: ClaimAttemptFact) =>
          c.seat === seat &&
          (c.subtaskId as string) === subtaskId &&
          c.outcome === "won",
      );

    // Canonical FILTER: only body queries on canonical subtasks are in scope.
    const bodyQueries = ledger.deliveries
      .filter(isBodyQuery)
      .filter((o) => canonicalIds.has(o.subtaskId as string));
    if (bodyQueries.length === 0) {
      return fail(
        id,
        "no canonical body-read evidence present (owner read unproven)",
      );
    }
    const winners = canonicalWinners(ledger, canonicalIds);
    if (winners.size === 0) {
      return fail(id, "no canonical winning owner to prove body-read");
    }

    // Every canonical winner performs an explicit post-claim body query on its
    // own wake turn.
    for (const [subtaskId, winnerSeat] of winners) {
      const winnerClaim = ledger.claims.find(
        (c) =>
          (c.subtaskId as string) === subtaskId &&
          c.seat === winnerSeat &&
          c.outcome === "won",
      );
      if (winnerClaim === undefined) {
        return fail(id, `canonical subtask ${subtaskId} winner claim missing`);
      }
      const wakeTurns = ledger.wakes
        .filter(
          (w) =>
            (w.subtaskId as string) === subtaskId &&
            w.wakeDeliveryId !== undefined,
        )
        .map((w) => w.turnId as string);
      const winnerQueries = bodyQueries.filter(
        (o) => (o.subtaskId as string) === subtaskId && o.seat === winnerSeat,
      );
      if (winnerQueries.length === 0) {
        return fail(
          id,
          `canonical subtask ${subtaskId} winner ${winnerSeat} has no post-claim body query`,
        );
      }
      for (const q of winnerQueries) {
        if (q.explicitQuery !== true) {
          return fail(
            id,
            `winner ${winnerSeat} body read on subtask ${subtaskId} is not an explicit query`,
          );
        }
        if (!(q.ordinal > winnerClaim.ordinal)) {
          return fail(
            id,
            `winner ${winnerSeat} body query ordinal ${q.ordinal} does not follow its claim ordinal ${winnerClaim.ordinal}`,
          );
        }
        if (
          wakeTurns.filter((t) => t === (q.queryTurnId as string)).length !== 1
        ) {
          return fail(
            id,
            `winner ${winnerSeat} body query turn is not the subtask ${subtaskId} external-wake turn`,
          );
        }
        if (!isChannelTarget(q.queryTarget) || !isValidBrandedId(q.queriedMessageId, "msg")) {
          return fail(
            id,
            `winner ${winnerSeat} body query on subtask ${subtaskId} has a non-channel target or non-MessageId ref`,
          );
        }
      }
    }

    // Every canonical body query must belong to its subtask's winner.
    for (const obs of bodyQueries) {
      if (!isOwner(obs.seat, obs.subtaskId as string)) {
        return fail(
          id,
          `non-owner seat ${obs.seat} issued a body_read for subtask ${obs.subtaskId}`,
        );
      }
    }

    // Every canonical loser proves zero body-read / reply / execution. Checked
    // before the presence rule so a loser with non-zero activity fails with its
    // specific reason rather than the vacuous-conflict message.
    for (const claim of ledger.claims) {
      if (claim.outcome === "won") continue;
      const seat = claim.seat;
      const subtaskId = claim.subtaskId as string;
      if (!canonicalIds.has(subtaskId)) continue;
      const loserBodyRead = bodyQueries.find(
        (o) => o.seat === seat && (o.subtaskId as string) === subtaskId,
      );
      if (loserBodyRead !== undefined) {
        return fail(
          id,
          `loser seat ${seat} issued a body_read for subtask ${subtaskId}`,
        );
      }
      const loserExec = ledger.executions.find(
        (e) => e.seat === seat && (e.subtaskId as string) === subtaskId,
      );
      if (loserExec !== undefined) {
        return fail(id, `loser seat ${seat} executed subtask ${subtaskId}`);
      }
      const activities = ledger.loserActivity.filter(
        (a) => a.seat === seat && (a.subtaskId as string) === subtaskId,
      );
      if (activities.length !== 1) {
        return fail(
          id,
          `loser seat ${seat} on subtask ${subtaskId} has ${activities.length} activity facts (expected exactly one)`,
        );
      }
      if (activities.some((a) => a.replyCount !== 0 || a.executionCount !== 0)) {
        return fail(
          id,
          `loser seat ${seat} has non-zero activity for subtask ${subtaskId}`,
        );
      }
    }

    // Non-vacuous conflict proof: at least one canonical conflict-stop/lost
    // loser lane must exist AND prove its zeros (exactly one zero loser-activity
    // row) with a loser NOTICE on that lane. Dropping the loser lane entirely
    // must not pass.
    const conflictLosers = ledger.claims.filter(
      (c) =>
        canonicalIds.has(c.subtaskId as string) &&
        (c.outcome === "lost" || c.outcome === "conflict_stop"),
    );
    const provenConflictLane = conflictLosers.some((loser) => {
      const seat = loser.seat;
      const subtaskId = loser.subtaskId as string;
      const activities = ledger.loserActivity.filter(
        (a) => a.seat === seat && (a.subtaskId as string) === subtaskId,
      );
      if (activities.length !== 1) return false;
      if (activities[0]!.replyCount !== 0 || activities[0]!.executionCount !== 0) {
        return false;
      }
      const loserNotice = ledger.deliveries.some(
        (o) =>
          isNotice(o) && o.seat === seat && (o.subtaskId as string) === subtaskId,
      );
      return loserNotice;
    });
    if (!provenConflictLane) {
      return fail(
        id,
        "no canonical conflict-stop loser lane proving zero activity with a loser NOTICE (conflict proof is vacuous)",
      );
    }
    return pass(
      id,
      "every winner made an explicit post-claim body query on its wake turn; a conflict loser proved its zeros; all losers zero",
    );
  },
};

// ---------------------------------------------------------------------------
// Placeholders: the remaining 7 source conditions, explicitly unbound with a
// named Wave-1 entry condition (from the seed catalog). They never pass or
// fail; the runner reports them separately.
// ---------------------------------------------------------------------------
function placeholderCondition(id: string, reason: string): Condition {
  return {
    id,
    evidence: [],
    predicate: (): ConditionVerdict => placeholder(id, reason),
  };
}

const placeholders: readonly Condition[] = [
  placeholderCondition(
    "thread_owner_status_receipt",
    "binds when Wave-1 daemon delivery lands the server-receipt thread surface (owner + status + durable server receipt per subtask)",
  ),
  placeholderCondition(
    "lifecycle_and_illegal_transition",
    "binds when the canonical lifecycle + assignee-only transition guard lands; illegal same-state or non-assignee transition rejected",
  ),
  placeholderCondition(
    "task_status_freshness_hold",
    "binds when the task-status freshness/hold path lands; stale status update held with bounded newer context and rerun path",
  ),
  placeholderCondition(
    "delivery_reply_trailer",
    "binds when the canonical delivery reply-trailer lands; full-body frame carries target-selection + completion-before-stopping instruction",
  ),
  placeholderCondition(
    "coordination_slos",
    "binds when trace timestamps expose the coordination SLO windows; p95 accepted/conflict-stop/first-progress thresholds",
  ),
  placeholderCondition(
    "channel_navigation_exact_target",
    "binds when Wave-1 worker channel-navigation surface lands; enumerate channels + authorized cross-channel read + exact-target reply",
  ),
  placeholderCondition(
    "lane_role_fidelity",
    "binds when Wave-1 coordination artifact-reference field (owner artifact path+hash) lands (seed M2); non-execute lanes reference not rebuild",
  ),
];

export const chatTaskOrchestrationScenario: Scenario = {
  scenarioId: "chat-task-orchestration",
  conditions: [
    decomposedByRuntime,
    routedByCapability,
    singleOwnerNoDuplicate,
    restartNoReexecution,
    steerHonoredPrecommit,
    wakeStartsTurn,
    noticeFirstBodyWithheld,
    ownerOnlyBodyRead,
    ...placeholders,
  ],
};
