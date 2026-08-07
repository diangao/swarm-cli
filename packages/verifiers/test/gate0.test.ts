import assert from "node:assert/strict";
import { test } from "node:test";

import type {
  DeliveryId,
  ProtocolVersion,
  TaskId,
  TransitionReceipt,
  TurnId,
} from "@swarm/protocol";

import {
  EvidenceLedgerBuilder,
  LedgerIngestError,
  chatTaskOrchestrationScenario,
  runScenario,
} from "../src/index.js";
import type {
  BodyQueryDeliveryFact,
  ClaimAttemptFact,
  ConditionVerdict,
  EvidenceLedger,
  NoticeDeliveryFact,
} from "../src/index.js";
import {
  mintDeliveryId,
  mintMessageId,
  mintProducerFactId,
  mintReceiptId,
  mintServerId,
  mintTaskId,
  mintTurnId,
  resetIdCounters,
} from "../src/ids.js";

const PROTOCOL_VERSION = 1 as ProtocolVersion;

// Two neutral worker seats. Never real names.
const WORKER_1 = "worker-1";
const WORKER_2 = "worker-2";
const CAP_A = "capability-A";
const CAP_B = "capability-B";

// Global evidence ordinal bands: durable body < every NOTICE < every claim <
// every explicit body query. Distinct values within a band keep the healthy
// order strict while leaving room for negatives to reorder a single row.
const ORD_DURABILITY = 1;
const ORD_NOTICE_A = 10;
const ORD_NOTICE_LOSER = 11;
const ORD_NOTICE_B = 12;
const ORD_CLAIM_A_WIN = 20;
const ORD_CLAIM_A_STOP = 21;
const ORD_CLAIM_B_WIN = 22;
const ORD_QUERY_A = 30;
const ORD_QUERY_B = 31;
// A valid lowercase-hex SHA-256 digest for the durable canonical body.
const BODY_DIGEST = "a1b2c3d4e5f60718293a4b5c6d7e8f90".repeat(2);

type Fixture = {
  readonly parentTaskId: TaskId;
  readonly subtaskA: TaskId;
  readonly subtaskB: TaskId;
  readonly modelTurnId: TurnId;
  readonly turnWakeA: TurnId;
  readonly turnWakeB: TurnId;
  readonly wakeDeliveryA: DeliveryId;
  readonly wakeDeliveryB: DeliveryId;
  readonly bodyDeliveryA: DeliveryId;
  readonly bodyDeliveryB: DeliveryId;
  readonly noticeDeliveryA: DeliveryId;
  readonly noticeDeliveryB: DeliveryId;
  readonly noticeDeliveryLoser: DeliveryId;
  readonly restartMarkerA: DeliveryId;
};

const ASSERTED_IDS = [
  "decomposed_by_runtime",
  "routed_by_capability",
  "single_owner_no_duplicate",
  "restart_no_reexecution",
  "steer_honored_precommit",
  "wake_starts_turn",
  "notice_first_body_withheld",
  "owner_only_body_read",
] as const;

function serverReceipt(): TransitionReceipt {
  return {
    protocolVersion: PROTOCOL_VERSION,
    receiptId: mintReceiptId(),
    kind: "server_accepted",
    producerFactId: mintProducerFactId(),
    actor: { serverId: mintServerId() },
    fence: {},
    occurredAt: "2026-01-01T00:00:00.000Z",
  };
}

// Typed fixture helpers so negatives can construct the discriminated-union
// rows without repeating every field. A typed return keeps `bodyPresent:false`
// / `explicitQuery:true` as literals rather than widening to boolean.
function claimFact(
  subtaskId: TaskId,
  seat: string,
  outcome: ClaimAttemptFact["outcome"],
  ordinal: number,
): ClaimAttemptFact {
  return { subtaskId, seat, outcome, ordinal };
}

function noticeFact(
  seat: string,
  deliveryId: DeliveryId,
  subtaskId: TaskId,
  ordinal: number,
): NoticeDeliveryFact {
  return {
    kind: "notice_metadata",
    seat,
    deliveryId,
    subtaskId,
    bodyPresent: false,
    target: "#lane",
    count: 1,
    firstMessageId: mintMessageId(),
    latestMessageId: mintMessageId(),
    ordinal,
  };
}

function bodyQueryFact(
  seat: string,
  deliveryId: DeliveryId,
  subtaskId: TaskId,
  ordinal: number,
  queryTurnId: TurnId,
): BodyQueryDeliveryFact {
  return {
    kind: "body_read",
    seat,
    deliveryId,
    subtaskId,
    explicitQuery: true,
    queryTarget: "#lane",
    queriedMessageId: mintMessageId(),
    queryTurnId,
    ordinal,
  };
}

/**
 * Build a HEALTHY ledger where all 8 asserted conditions pass. Returns the
 * builder plus the fixture ids so a negative control can rebuild it with
 * exactly one seeded defect. Each call resets id counters for reproducibility.
 *
 * Layout:
 *  - subtaskA: contested. WORKER_1 wins (capability A), WORKER_2 conflict-stops.
 *  - subtaskB: WORKER_2 wins (capability B), uncontested.
 *  - restart replays subtaskA; it stays exactly-once executed.
 *  - WORKER_2 (loser of subtaskA) proves zero replies/executions on subtaskA.
 */
function healthy(): { builder: EvidenceLedgerBuilder; fx: Fixture } {
  resetIdCounters();
  const parentTaskId = mintTaskId();
  const subtaskA = mintTaskId();
  const subtaskB = mintTaskId();
  const modelTurnId = mintTurnId();
  const turnWakeA = mintTurnId();
  const turnWakeB = mintTurnId();
  const wakeDeliveryA = mintDeliveryId();
  const wakeDeliveryB = mintDeliveryId();
  const noticeDeliveryA = mintDeliveryId();
  const noticeDeliveryB = mintDeliveryId();
  const noticeDeliveryLoser = mintDeliveryId();
  const bodyDeliveryA = mintDeliveryId();
  const bodyDeliveryB = mintDeliveryId();
  const restartMarkerA = mintDeliveryId();

  const fx: Fixture = {
    parentTaskId,
    subtaskA,
    subtaskB,
    modelTurnId,
    turnWakeA,
    turnWakeB,
    wakeDeliveryA,
    wakeDeliveryB,
    bodyDeliveryA,
    bodyDeliveryB,
    noticeDeliveryA,
    noticeDeliveryB,
    noticeDeliveryLoser,
    restartMarkerA,
  };

  const builder = new EvidenceLedgerBuilder();

  builder.addAvailableWorkerSeat(WORKER_1).addAvailableWorkerSeat(WORKER_2);

  builder.addDecomposition({
    parentTaskId,
    authoredBy: "runtime",
    modelTurnId,
    subtasks: [
      { taskId: subtaskA, capability: CAP_A },
      { taskId: subtaskB, capability: CAP_B },
    ],
  });

  builder
    .addRoute({
      subtaskId: subtaskA,
      routedCapability: CAP_A,
      matchedWorkerSeat: WORKER_1,
    })
    .addRoute({
      subtaskId: subtaskB,
      routedCapability: CAP_B,
      matchedWorkerSeat: WORKER_2,
    });

  // subtaskA contested: WORKER_1 wins, WORKER_2 conflict-stops.
  builder
    .addClaim({
      subtaskId: subtaskA,
      seat: WORKER_1,
      outcome: "won",
      ordinal: ORD_CLAIM_A_WIN,
    })
    .addClaim({
      subtaskId: subtaskA,
      seat: WORKER_2,
      outcome: "conflict_stop",
      ordinal: ORD_CLAIM_A_STOP,
    })
    // subtaskB: WORKER_2 wins.
    .addClaim({
      subtaskId: subtaskB,
      seat: WORKER_2,
      outcome: "won",
      ordinal: ORD_CLAIM_B_WIN,
    });

  builder
    .addExecution({ subtaskId: subtaskA, seat: WORKER_1, executionCount: 1 })
    .addExecution({ subtaskId: subtaskB, seat: WORKER_2, executionCount: 1 });

  builder
    .addThreadStatus({
      subtaskId: subtaskA,
      ownerSeat: WORKER_1,
      status: "done",
      receiptId: mintReceiptId(),
    })
    .addThreadStatus({
      subtaskId: subtaskB,
      ownerSeat: WORKER_2,
      status: "done",
      receiptId: mintReceiptId(),
    });

  builder.addReceipt(serverReceipt());

  builder
    .addWake({ turnId: turnWakeA, subtaskId: subtaskA, wakeDeliveryId: wakeDeliveryA })
    .addWake({ turnId: turnWakeB, subtaskId: subtaskB, wakeDeliveryId: wakeDeliveryB });

  // Durable body committed BEFORE any notice fanned out (ordinal precedes all).
  builder.addBodyDurability({
    parentTaskId,
    bodyDigest: BODY_DIGEST,
    bodyBytes: 2048,
    receiptId: mintReceiptId(),
    ordinal: ORD_DURABILITY,
  });

  // Delivery contract: a content-free NOTICE to every participant lane BEFORE
  // its claim, then the winner's EXPLICIT post-claim body query on its wake turn.
  builder
    // WORKER_1 (subtaskA winner): notice, then explicit body query of subtaskA.
    .addDelivery({
      kind: "notice_metadata",
      seat: WORKER_1,
      deliveryId: noticeDeliveryA,
      subtaskId: subtaskA,
      bodyPresent: false,
      target: "#subtask-a",
      count: 1,
      firstMessageId: mintMessageId(),
      latestMessageId: mintMessageId(),
      ordinal: ORD_NOTICE_A,
    })
    .addDelivery({
      kind: "body_read",
      seat: WORKER_1,
      deliveryId: bodyDeliveryA,
      subtaskId: subtaskA,
      explicitQuery: true,
      queryTarget: "#subtask-a",
      queriedMessageId: mintMessageId(),
      queryTurnId: turnWakeA,
      ordinal: ORD_QUERY_A,
    })
    // WORKER_2 lost subtaskA (loser NOTICE only) and won subtaskB.
    .addDelivery({
      kind: "notice_metadata",
      seat: WORKER_2,
      deliveryId: noticeDeliveryLoser,
      subtaskId: subtaskA,
      bodyPresent: false,
      target: "#subtask-a",
      count: 1,
      firstMessageId: mintMessageId(),
      latestMessageId: mintMessageId(),
      ordinal: ORD_NOTICE_LOSER,
    })
    .addDelivery({
      kind: "notice_metadata",
      seat: WORKER_2,
      deliveryId: noticeDeliveryB,
      subtaskId: subtaskB,
      bodyPresent: false,
      target: "#subtask-b",
      count: 1,
      firstMessageId: mintMessageId(),
      latestMessageId: mintMessageId(),
      ordinal: ORD_NOTICE_B,
    })
    .addDelivery({
      kind: "body_read",
      seat: WORKER_2,
      deliveryId: bodyDeliveryB,
      subtaskId: subtaskB,
      explicitQuery: true,
      queryTarget: "#subtask-b",
      queriedMessageId: mintMessageId(),
      queryTurnId: turnWakeB,
      ordinal: ORD_QUERY_B,
    });

  builder.addSteer({
    subtaskId: subtaskA,
    expectedTurnId: turnWakeA,
    appliedBeforeCommit: true,
    committedStale: false,
    requiresReplanHold: false,
  });

  // Restart replays subtaskA; it stays exactly-once executed.
  builder.addRestart({
    subtaskId: subtaskA,
    restartMarker: restartMarkerA,
    replayOf: wakeDeliveryA,
  });

  // WORKER_2 lost subtaskA: prove the structural zeros.
  builder.addLoserActivity({
    subtaskId: subtaskA,
    seat: WORKER_2,
    replyCount: 0,
    executionCount: 0,
  });

  return { builder, fx };
}

/**
 * Rebuild the healthy ledger into a fresh builder, so a caller can mutate one
 * slice (drop or replace it) while every other slice stays healthy. This is how
 * we seed MISSING-evidence negatives without disturbing the rest of the ledger.
 */
function rebuild(
  base: EvidenceLedger,
  overrides: Partial<{
    decompositions: EvidenceLedger["decompositions"];
    routes: EvidenceLedger["routes"];
    claims: EvidenceLedger["claims"];
    executions: EvidenceLedger["executions"];
    threadStatuses: EvidenceLedger["threadStatuses"];
    wakes: EvidenceLedger["wakes"];
    bodyDurability: EvidenceLedger["bodyDurability"];
    deliveries: EvidenceLedger["deliveries"];
    steers: EvidenceLedger["steers"];
    restarts: EvidenceLedger["restarts"];
    loserActivity: EvidenceLedger["loserActivity"];
    availableWorkerSeats: EvidenceLedger["availableWorkerSeats"];
  }>,
): EvidenceLedger {
  const b = new EvidenceLedgerBuilder();
  for (const s of overrides.availableWorkerSeats ?? base.availableWorkerSeats)
    b.addAvailableWorkerSeat(s);
  for (const d of overrides.decompositions ?? base.decompositions)
    b.addDecomposition(d);
  for (const r of overrides.routes ?? base.routes) b.addRoute(r);
  for (const c of overrides.claims ?? base.claims) b.addClaim(c);
  for (const e of overrides.executions ?? base.executions) b.addExecution(e);
  for (const t of overrides.threadStatuses ?? base.threadStatuses)
    b.addThreadStatus(t);
  for (const rec of base.receipts) b.addReceipt(rec);
  for (const w of overrides.wakes ?? base.wakes) b.addWake(w);
  for (const d of overrides.bodyDurability ?? base.bodyDurability)
    b.addBodyDurability(d);
  for (const d of overrides.deliveries ?? base.deliveries) b.addDelivery(d);
  for (const s of overrides.steers ?? base.steers) b.addSteer(s);
  for (const r of overrides.restarts ?? base.restarts) b.addRestart(r);
  for (const a of overrides.loserActivity ?? base.loserActivity)
    b.addLoserActivity(a);
  return b.build();
}

function verdictFor(
  ledger: EvidenceLedger,
  conditionId: string,
): ConditionVerdict {
  const run = runScenario(chatTaskOrchestrationScenario, ledger);
  const found = run.verdicts.find((v) => v.conditionId === conditionId);
  assert.ok(found, `condition ${conditionId} must be present in verdicts`);
  return found;
}

/** Assert exactly one asserted condition fails and it is the expected one. */
/**
 * Assert the EXACT asserted-condition failure set: every id in `failing` must
 * fail and every other asserted condition must pass. This catches silent
 * multi-failing (a negative that fails more than its declared target), which
 * would otherwise mask whether the target is really the isolated cause.
 */
function assertFailingSet(
  ledger: EvidenceLedger,
  failing: readonly string[],
): void {
  const run = runScenario(chatTaskOrchestrationScenario, ledger);
  assert.equal(
    run.advisory,
    "fail",
    `advisory must fail for [${failing.join(", ")}]`,
  );
  const failSet = new Set(failing);
  for (const id of failing) {
    assert.ok(
      run.asserted.some((v) => v.conditionId === id),
      `${id} must be present in asserted conditions`,
    );
  }
  for (const v of run.asserted) {
    const expected = failSet.has(v.conditionId) ? "fail" : "pass";
    assert.equal(
      v.status,
      expected,
      `${v.conditionId} should ${expected} but was ${v.status}: ${v.reason}`,
    );
  }
}

function assertOnlyFailing(ledger: EvidenceLedger, conditionId: string): void {
  assertFailingSet(ledger, [conditionId]);
}

// ---------------------------------------------------------------------------
// POSITIVE CONTROL
// ---------------------------------------------------------------------------
test("positive control: healthy ledger passes all 8 asserted conditions", () => {
  const { builder } = healthy();
  const ledger = builder.build();
  const run = runScenario(chatTaskOrchestrationScenario, ledger);

  assert.equal(run.advisory, "pass");
  assert.equal(run.asserted.length, 8);
  for (const v of run.asserted) {
    assert.equal(v.status, "pass", `${v.conditionId} should pass: ${v.reason}`);
  }
});

// ---------------------------------------------------------------------------
// REGRESSION: an external adversarial ledger.
// Two capability-typed subtasks routed to the SAME unavailable seat with the
// WRONG capability, and ZERO claims/executions/replay/steers/wakes/deliveries/
// statuses/receipts. This is the oracle-soundness minimal reproduction; it MUST make
// advisory=fail AND every asserted condition fail closed.
// ---------------------------------------------------------------------------
test("REGRESSION: zero-evidence adversarial ledger fails advisory and EVERY asserted condition", () => {
  resetIdCounters();
  const parent = mintTaskId();
  const s1 = mintTaskId();
  const s2 = mintTaskId();
  const modelTurnId = mintTurnId();
  const unavailableSeat = "worker-ghost"; // never added as available
  const wrongCap = "capability-Z"; // matches neither subtask capability

  const b = new EvidenceLedgerBuilder();
  // NOTE: no available worker seats added at all.
  b.addDecomposition({
    parentTaskId: parent,
    authoredBy: "runtime",
    modelTurnId,
    subtasks: [
      { taskId: s1, capability: CAP_A },
      { taskId: s2, capability: CAP_B },
    ],
  });
  // Both routed to the SAME unavailable seat with the WRONG capability.
  b.addRoute({
    subtaskId: s1,
    routedCapability: wrongCap,
    matchedWorkerSeat: unavailableSeat,
  });
  b.addRoute({
    subtaskId: s2,
    routedCapability: wrongCap,
    matchedWorkerSeat: unavailableSeat,
  });
  // ZERO of everything else.
  const ledger = b.build();

  const run = runScenario(chatTaskOrchestrationScenario, ledger);
  assert.equal(run.advisory, "fail", "adversarial ledger must be advisory=fail");

  // decomposed_by_runtime is the ONLY asserted condition the adversarial ledger
  // does not defeat (it seeds a runtime decomposition to prove the other joins
  // fail closed on the positive-fact absence). Every OTHER asserted condition
  // must fail closed.
  const byId = new Map(run.asserted.map((v) => [v.conditionId, v]));
  for (const id of ASSERTED_IDS) {
    const v = byId.get(id);
    assert.ok(v, `asserted condition ${id} must be present`);
    if (id === "decomposed_by_runtime") {
      assert.equal(v.status, "pass", "decomposition is intentionally healthy");
    } else {
      assert.equal(
        v.status,
        "fail",
        `${id} must fail closed on zero-evidence: ${v.reason}`,
      );
    }
  }
});

// A stricter variant: even with a runtime decomposition ABSENT, all 8 fail.
test("REGRESSION: fully empty ledger fails advisory and all 8 asserted conditions", () => {
  const ledger = new EvidenceLedgerBuilder().build();
  const run = runScenario(chatTaskOrchestrationScenario, ledger);
  assert.equal(run.advisory, "fail");
  assert.equal(run.asserted.length, 8);
  for (const v of run.asserted) {
    assert.equal(v.status, "fail", `${v.conditionId} must fail on empty: ${v.reason}`);
  }
});

// Two capability-distinct lanes routed, claimed, and executed on ONE available
// seat, otherwise healthy. This previously passed all 8; the distinct-owner
// (injective route seat) clause must now make routed_by_capability FAIL.
test("REGRESSION: both lanes owned by one seat fails routed_by_capability (distinct-owner)", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  // Both routes point at WORKER_1 (non-injective seats) while WORKER_1 wins both
  // lanes. The contested lane (WORKER_2 conflict-stop on subtaskA) is retained so
  // the non-vacuous conflict proof still holds and only routing's injectivity
  // clause fails.
  const oneSeat = rebuild(base, {
    routes: [
      { subtaskId: fx.subtaskA, routedCapability: CAP_A, matchedWorkerSeat: WORKER_1 },
      { subtaskId: fx.subtaskB, routedCapability: CAP_B, matchedWorkerSeat: WORKER_1 },
    ],
    claims: [
      claimFact(fx.subtaskA, WORKER_1, "won", ORD_CLAIM_A_WIN),
      claimFact(fx.subtaskA, WORKER_2, "conflict_stop", ORD_CLAIM_A_STOP),
      claimFact(fx.subtaskB, WORKER_1, "won", ORD_CLAIM_B_WIN),
    ],
    executions: [
      { subtaskId: fx.subtaskA, seat: WORKER_1, executionCount: 1 },
      { subtaskId: fx.subtaskB, seat: WORKER_1, executionCount: 1 },
    ],
    threadStatuses: [
      { subtaskId: fx.subtaskA, ownerSeat: WORKER_1, status: "done", receiptId: mintReceiptId() },
      { subtaskId: fx.subtaskB, ownerSeat: WORKER_1, status: "done", receiptId: mintReceiptId() },
    ],
    deliveries: [
      noticeFact(WORKER_1, fx.noticeDeliveryA, fx.subtaskA, ORD_NOTICE_A),
      bodyQueryFact(WORKER_1, fx.bodyDeliveryA, fx.subtaskA, ORD_QUERY_A, fx.turnWakeA),
      noticeFact(WORKER_2, fx.noticeDeliveryLoser, fx.subtaskA, ORD_NOTICE_LOSER),
      noticeFact(WORKER_1, fx.noticeDeliveryB, fx.subtaskB, ORD_NOTICE_B),
      bodyQueryFact(WORKER_1, fx.bodyDeliveryB, fx.subtaskB, ORD_QUERY_B, fx.turnWakeB),
    ],
  });
  assertOnlyFailing(oneSeat, "routed_by_capability");
});

// Provenance splice: a human-authored first graph carries every downstream fact
// while an unrelated runtime-authored second graph supplies provenance. This
// previously passed all 8; a single canonical graph is now required, so the
// ambiguous two-graph evidence must make decomposed_by_runtime FAIL.
test("REGRESSION: cross-graph provenance splice fails decomposed_by_runtime", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const spliced = rebuild(base, {
    decompositions: [
      {
        parentTaskId: fx.parentTaskId,
        authoredBy: "human",
        subtasks: [
          { taskId: fx.subtaskA, capability: CAP_A },
          { taskId: fx.subtaskB, capability: CAP_B },
        ],
      },
      {
        parentTaskId: mintTaskId(),
        authoredBy: "runtime",
        modelTurnId: mintTurnId(),
        subtasks: [
          { taskId: mintTaskId(), capability: CAP_A },
          { taskId: mintTaskId(), capability: CAP_B },
        ],
      },
    ],
  });
  // Two decompositions make the canonical decomposition ambiguous, so every
  // canonical-join predicate fails closed, not just decomposed_by_runtime.
  assertFailingSet(spliced, [
    "decomposed_by_runtime",
    "routed_by_capability",
    "single_owner_no_duplicate",
    "restart_no_reexecution",
    "steer_honored_precommit",
    "wake_starts_turn",
    "notice_first_body_withheld",
    "owner_only_body_read",
  ]);
});

// A canonical subtask's sole winner has TWO execution rows, each count 1
// (total 2). Previously passed because only per-row count and distinct seats
// were checked; execution cardinality must now make single_owner FAIL.
test("REGRESSION: duplicate execution rows for one winner fails single_owner_no_duplicate", () => {
  const { builder, fx } = healthy();
  builder.addExecution({ subtaskId: fx.subtaskA, seat: WORKER_1, executionCount: 1 });
  const ledger = builder.build();
  // The extra execution also makes subtaskA's restart total execution 2, so
  // restart_no_reexecution fails alongside single_owner.
  assertFailingSet(ledger, ["single_owner_no_duplicate", "restart_no_reexecution"]);
});

// The canonical subtasks carry no steer or restart; the only steer/restart
// target an unrelated non-canonical task (with matching status/execution). This
// previously passed all eight; every subtask-referencing predicate now joins the
// canonical subtask set, so restart_no_reexecution and steer_honored_precommit
// must FAIL on the unrelated-task evidence.
test("REGRESSION: steer/restart on an unrelated task fails restart and steer", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const unrelated = mintTaskId();
  const ledger = rebuild(base, {
    steers: [
      {
        subtaskId: unrelated,
        expectedTurnId: fx.turnWakeA,
        appliedBeforeCommit: true,
        committedStale: false,
        requiresReplanHold: false,
      },
    ],
    restarts: [
      { subtaskId: unrelated, restartMarker: mintDeliveryId(), replayOf: mintDeliveryId() },
    ],
    threadStatuses: [
      ...base.threadStatuses,
      { subtaskId: unrelated, ownerSeat: WORKER_1, status: "done", receiptId: mintReceiptId() },
    ],
    executions: [
      ...base.executions,
      { subtaskId: unrelated, seat: WORKER_1, executionCount: 1 },
    ],
  });
  assertFailingSet(ledger, [
    "restart_no_reexecution",
    "steer_honored_precommit",
  ]);
});

// The other side of the canonical join: healthy canonical evidence PLUS a pile
// of unrelated-task noise (even deliberately "bad" noise) must still PASS all
// eight. The canonical join FILTERS unrelated rows out; it does not fail on them.
test("REGRESSION: healthy canonical evidence plus unrelated noise still passes all eight", () => {
  const { builder, fx } = healthy();
  const noise = mintTaskId();
  builder
    .addRestart({ subtaskId: noise, restartMarker: mintDeliveryId(), replayOf: mintDeliveryId() })
    .addSteer({ subtaskId: noise, expectedTurnId: fx.turnWakeA, appliedBeforeCommit: false, committedStale: true, requiresReplanHold: false })
    .addDelivery(bodyQueryFact("worker-9", mintDeliveryId(), noise, 90, mintTurnId()))
    .addClaim(claimFact(noise, "worker-9", "won", 91))
    .addExecution({ subtaskId: noise, seat: "worker-9", executionCount: 5 })
    .addThreadStatus({ subtaskId: noise, ownerSeat: "worker-9", status: "done", receiptId: mintReceiptId() })
    .addLoserActivity({ subtaskId: noise, seat: "worker-8", replyCount: 3, executionCount: 2 });
  const run = runScenario(chatTaskOrchestrationScenario, builder.build());
  assert.equal(run.advisory, "pass");
  for (const v of run.asserted) {
    assert.equal(v.status, "pass", `${v.conditionId} should pass despite unrelated noise: ${v.reason}`);
  }
});

// Proactive cardinality analogs of the execution-cardinality hole: two routes or
// two thread statuses for one subtask must not pass by first-match.
test("REGRESSION: two routes for one subtask fails routed_by_capability", () => {
  const { builder, fx } = healthy();
  builder.addRoute({ subtaskId: fx.subtaskA, routedCapability: CAP_A, matchedWorkerSeat: WORKER_2 });
  const ledger = builder.build();
  assertOnlyFailing(ledger, "routed_by_capability");
});

test("REGRESSION: two thread statuses for one subtask fails single_owner_no_duplicate", () => {
  const { builder, fx } = healthy();
  builder.addThreadStatus({ subtaskId: fx.subtaskA, ownerSeat: WORKER_1, status: "done", receiptId: mintReceiptId() });
  const ledger = builder.build();
  assertOnlyFailing(ledger, "single_owner_no_duplicate");
});

// Wake completeness: two canonical winning owners but only ONE wake row.
// subtaskB's winner loses its wake, so wake_starts_turn fails AND owner_only
// fails (subtaskB's body query can no longer bind to an external wake turn).
test("REGRESSION: one wake for two canonical winners fails wake and owner_only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    wakes: [{ turnId: fx.turnWakeA, subtaskId: fx.subtaskA, wakeDeliveryId: fx.wakeDeliveryA }],
  });
  assertFailingSet(ledger, ["wake_starts_turn", "owner_only_body_read"]);
});

// Per-lane delivery completeness: drop every delivery for canonical subtaskB. Its
// winner has no NOTICE and no body-read, so both delivery conditions must FAIL
// while the other six PASS.
test("REGRESSION: a canonical lane with no notice/body-read fails both delivery conditions", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    deliveries: base.deliveries.filter(
      (o) => (o.subtaskId as string) !== (fx.subtaskB as string),
    ),
  });
  assertFailingSet(ledger, [
    "notice_first_body_withheld",
    "owner_only_body_read",
  ]);
});

// Loser-activity cardinality: a valid zero row followed by a conflicting
// replyCount:1 row must not pass by first-match. owner_only_body_read must FAIL.
test("REGRESSION: conflicting nonzero loser-activity duplicate fails owner_only_body_read", () => {
  const { builder, fx } = healthy();
  builder.addLoserActivity({ subtaskId: fx.subtaskA, seat: WORKER_2, replyCount: 1, executionCount: 0 });
  const ledger = builder.build();
  assertOnlyFailing(ledger, "owner_only_body_read");
});

// Global ordering: a NOTICE whose ordinal falls AFTER a canonical claim (i.e.
// the body was fanned out after the claim, not before) must fail notice_first
// while every other condition, including owner_only, still passes.
test("REGRESSION: a notice ordered after a claim fails notice_first only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    deliveries: [
      noticeFact(WORKER_1, fx.noticeDeliveryA, fx.subtaskA, ORD_NOTICE_A),
      bodyQueryFact(WORKER_1, fx.bodyDeliveryA, fx.subtaskA, ORD_QUERY_A, fx.turnWakeA),
      noticeFact(WORKER_2, fx.noticeDeliveryLoser, fx.subtaskA, ORD_NOTICE_LOSER),
      // WORKER_2's subtaskB NOTICE lands AFTER the claims (ordinal 25 > 22).
      noticeFact(WORKER_2, fx.noticeDeliveryB, fx.subtaskB, 25),
      bodyQueryFact(WORKER_2, fx.bodyDeliveryB, fx.subtaskB, ORD_QUERY_B, fx.turnWakeB),
    ],
  });
  assertOnlyFailing(ledger, "notice_first_body_withheld");
});

// ---------------------------------------------------------------------------
// PER-CONDITION MISSING-EVIDENCE NEGATIVES: drop the required positive facts for
// ONE condition at a time; that condition must fail closed.
// ---------------------------------------------------------------------------

test("missing-evidence: routed_by_capability fails when routes are dropped", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { routes: [] });
  // Routes feed both routing and single_owner's route->winner chain.
  assertFailingSet(ledger, ["routed_by_capability", "single_owner_no_duplicate"]);
});

test("missing-evidence: single_owner_no_duplicate fails when claims are dropped", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { claims: [], loserActivity: [] });
  // No claims => no canonical winners, so every winner-derived predicate fails.
  assertFailingSet(ledger, [
    "single_owner_no_duplicate",
    "wake_starts_turn",
    "notice_first_body_withheld",
    "owner_only_body_read",
  ]);
});

test("missing-evidence: single_owner_no_duplicate fails when executions are dropped", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { executions: [] });
  // Executions feed single_owner and the restart idempotency check.
  assertFailingSet(ledger, ["single_owner_no_duplicate", "restart_no_reexecution"]);
});

test("missing-evidence: restart_no_reexecution fails when restart evidence is dropped", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { restarts: [] });
  assertOnlyFailing(ledger, "restart_no_reexecution");
});

test("missing-evidence: steer_honored_precommit fails when steers are dropped", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { steers: [] });
  assertOnlyFailing(ledger, "steer_honored_precommit");
});

test("missing-evidence: wake_starts_turn fails when wakes are dropped (all 0 turns)", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { wakes: [] });
  // Wakes are foundational: restart replayOf, steer expected-turn, and owner
  // body-query turn all bind to an external wake, so all four fail closed.
  assertFailingSet(ledger, [
    "wake_starts_turn",
    "restart_no_reexecution",
    "steer_honored_precommit",
    "owner_only_body_read",
  ]);
});

test("missing-evidence: notice_first_body_withheld fails when deliveries are dropped", () => {
  const { builder } = healthy();
  // Dropping deliveries removes the owner body-read too, so owner_only_body_read
  // fails alongside notice_first_body_withheld. Assert the full explicit set.
  const ledger = rebuild(builder.build(), { deliveries: [] });
  assertFailingSet(ledger, ["notice_first_body_withheld", "owner_only_body_read"]);
});

test("missing-evidence: owner_only_body_read fails when loser-activity proof is dropped", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { loserActivity: [] });
  assertOnlyFailing(ledger, "owner_only_body_read");
});

// ---------------------------------------------------------------------------
// WRONG-RELATIONSHIP NEGATIVES: correct cardinality, wrong join.
// ---------------------------------------------------------------------------

// S7: stub/checklist decomposition (no model turn provenance).
test("wrong: decomposed_by_runtime fails on stub decomposition (S7)", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    decompositions: [
      {
        parentTaskId: fx.parentTaskId,
        authoredBy: "human",
        subtasks: [
          { taskId: fx.subtaskA, capability: CAP_A },
          { taskId: fx.subtaskB, capability: CAP_B },
        ],
      },
    ],
  });
  assertOnlyFailing(ledger, "decomposed_by_runtime");
});

// Wrong capability on a route (capability mismatch).
test("wrong: routed_by_capability fails on capability mismatch", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    routes: [
      { subtaskId: fx.subtaskA, routedCapability: CAP_B, matchedWorkerSeat: WORKER_1 },
      { subtaskId: fx.subtaskB, routedCapability: CAP_B, matchedWorkerSeat: WORKER_2 },
    ],
  });
  const v = verdictFor(ledger, "routed_by_capability");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /capability/u);
});

// Unavailable seat.
test("wrong: routed_by_capability fails on unavailable matched seat", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    routes: [
      { subtaskId: fx.subtaskA, routedCapability: CAP_A, matchedWorkerSeat: "worker-ghost" },
      { subtaskId: fx.subtaskB, routedCapability: CAP_B, matchedWorkerSeat: WORKER_2 },
    ],
  });
  const v = verdictFor(ledger, "routed_by_capability");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /not an available worker/u);
});

// Undefined seat (unroutable) now FAILS (false-pass path removed).
test("wrong: routed_by_capability fails on unroutable subtask (undefined seat)", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    routes: [
      { subtaskId: fx.subtaskA, routedCapability: CAP_A },
      { subtaskId: fx.subtaskB, routedCapability: CAP_B, matchedWorkerSeat: WORKER_2 },
    ],
  });
  const v = verdictFor(ledger, "routed_by_capability");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /unroutable/u);
});

// M6: double-winner / double-spawn.
test("wrong: single_owner_no_duplicate fails on two winners (M6)", () => {
  const { builder, fx } = healthy();
  builder.addClaim(claimFact(fx.subtaskA, WORKER_2, "won", 23));
  // WORKER_2 is now a winner of subtaskA, so drop its loser-activity for A to
  // keep this a single-defect (two-winners) case; owner_only would otherwise
  // read a winner as a loser. But two winners also breaks single_owner; assert.
  const ledger = builder.build();
  const v = verdictFor(ledger, "single_owner_no_duplicate");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /winning claims/u);
  assert.equal(runScenario(chatTaskOrchestrationScenario, ledger).advisory, "fail");
});

// Loser executes the subtask (non-winner execution).
test("wrong: single_owner_no_duplicate fails when a loser executes the subtask", () => {
  const { builder, fx } = healthy();
  // WORKER_2 (loser of subtaskA) also executes subtaskA.
  builder.addExecution({ subtaskId: fx.subtaskA, seat: WORKER_2, executionCount: 1 });
  const ledger = builder.build();
  const v = verdictFor(ledger, "single_owner_no_duplicate");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /distinct seats|non-winner|execution rows/u);
});

// S13: completed subtask re-executes after restart.
test("wrong: restart_no_reexecution fails on re-execution after restart (S13)", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    executions: [
      { subtaskId: fx.subtaskA, seat: WORKER_1, executionCount: 2 },
      { subtaskId: fx.subtaskB, seat: WORKER_2, executionCount: 1 },
    ],
  });
  const v = verdictFor(ledger, "restart_no_reexecution");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /across restart|executed/u);
  // executionCount:2 is genuinely both a duplicate execution and a restart
  // re-execution under this evidence model, so both conditions fail.
  assertFailingSet(ledger, ["single_owner_no_duplicate", "restart_no_reexecution"]);
});

// S16/M4: stale steer committed.
test("wrong: steer_honored_precommit fails on committed-stale steer (S16/M4)", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    steers: [
      {
        subtaskId: fx.subtaskA,
        expectedTurnId: fx.turnWakeA,
        appliedBeforeCommit: false,
        committedStale: true,
        requiresReplanHold: false,
      },
    ],
  });
  assertOnlyFailing(ledger, "steer_honored_precommit");
});

// Missing wake provenance on one turn (resident-poll origin).
test("wrong: wake_starts_turn fails on a wake with no wakeDeliveryId", () => {
  const { builder, fx } = healthy();
  builder.addWake({ turnId: fx.modelTurnId, subtaskId: fx.subtaskA });
  const ledger = builder.build();
  const v = verdictFor(ledger, "wake_starts_turn");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /resident-poll|no external/u);
});

// M7: a winner lane has its explicit body query but NO content-free NOTICE, so
// the body reached the owner without a notice-first fan-out. notice_first must
// fail while owner_only (the query is a valid post-claim owner read) passes.
test("wrong: notice_first_body_withheld fails on a winner lane with no NOTICE (M7)", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    deliveries: [
      // WORKER_1 (subtaskA winner) has only its body query, no NOTICE.
      bodyQueryFact(WORKER_1, fx.bodyDeliveryA, fx.subtaskA, ORD_QUERY_A, fx.turnWakeA),
      noticeFact(WORKER_2, fx.noticeDeliveryLoser, fx.subtaskA, ORD_NOTICE_LOSER),
      noticeFact(WORKER_2, fx.noticeDeliveryB, fx.subtaskB, ORD_NOTICE_B),
      bodyQueryFact(WORKER_2, fx.bodyDeliveryB, fx.subtaskB, ORD_QUERY_B, fx.turnWakeB),
    ],
  });
  assertOnlyFailing(ledger, "notice_first_body_withheld");
  assert.equal(verdictFor(ledger, "owner_only_body_read").status, "pass");
});

// S6: non-owner (loser) issues a body read.
test("wrong: owner_only_body_read fails on loser body read (S6)", () => {
  const { builder, fx } = healthy();
  // WORKER_2 (loser of subtaskA) issues an explicit body query of subtaskA.
  builder.addDelivery(
    bodyQueryFact(WORKER_2, mintDeliveryId(), fx.subtaskA, 33, fx.turnWakeA),
  );
  const ledger = builder.build();
  const v = verdictFor(ledger, "owner_only_body_read");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /body_read/u);
  assert.equal(verdictFor(ledger, "notice_first_body_withheld").status, "pass");
});

// Loser has non-zero reply activity.
test("wrong: owner_only_body_read fails when a loser has non-zero replies", () => {
  const { builder, fx } = healthy();
  const ledger = rebuild(builder.build(), {
    loserActivity: [
      { subtaskId: fx.subtaskA, seat: WORKER_2, replyCount: 1, executionCount: 0 },
    ],
  });
  const v = verdictFor(ledger, "owner_only_body_read");
  assert.equal(v.status, "fail");
  assert.match(v.reason, /non-zero activity/u);
});

// ---------------------------------------------------------------------------
// DELIVERY-CONTRACT CONTROLS: the three routed join regressions plus the
// durability / ordering / explicit-query negatives. Each isolates exactly the
// condition the frozen contract binds.
// ---------------------------------------------------------------------------

// Join 1: dropping the entire canonical conflict loser lane must not vacuously
// pass. owner_only_body_read fails; every other condition still passes.
test("JOIN: no canonical conflict loser lane fails owner_only only (non-vacuous)", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    claims: base.claims.filter(
      (c) =>
        !(
          (c.subtaskId as string) === (fx.subtaskA as string) &&
          c.seat === WORKER_2
        ),
    ),
    loserActivity: [],
    deliveries: base.deliveries.filter(
      (o) => o.deliveryId !== fx.noticeDeliveryLoser,
    ),
  });
  assertOnlyFailing(ledger, "owner_only_body_read");
});

// Join 2: a canonical steer whose expected turn is not the subtask's own
// external-wake turn fails steer_honored_precommit only.
test("JOIN: steer expected-turn not the subtask wake turn fails steer only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    steers: [
      {
        subtaskId: fx.subtaskA,
        expectedTurnId: fx.turnWakeB,
        appliedBeforeCommit: true,
        committedStale: false,
        requiresReplanHold: false,
      },
    ],
  });
  assertOnlyFailing(ledger, "steer_honored_precommit");
});

// Join 3a: a restart marker with no replayOf fails restart_no_reexecution only.
test("JOIN: restart with no replayOf fails restart only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    restarts: [{ subtaskId: fx.subtaskA, restartMarker: mintDeliveryId() }],
  });
  assertOnlyFailing(ledger, "restart_no_reexecution");
});

// Join 3b: a restart replayOf that matches a DIFFERENT subtask's wake delivery
// (cross-task) fails restart_no_reexecution only.
test("JOIN: restart replayOf matching another subtask wake fails restart only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    restarts: [
      {
        subtaskId: fx.subtaskA,
        restartMarker: mintDeliveryId(),
        replayOf: fx.wakeDeliveryB,
      },
    ],
  });
  assertOnlyFailing(ledger, "restart_no_reexecution");
});

// NOTICE ordinal negatives: each must fail notice_first_body_withheld only.
test("NOTICE-neg: missing body durability fails notice_first only", () => {
  const { builder } = healthy();
  const ledger = rebuild(builder.build(), { bodyDurability: [] });
  assertOnlyFailing(ledger, "notice_first_body_withheld");
});

test("NOTICE-neg: a zero-byte body durability row is rejected at ingestion", () => {
  const { builder } = healthy();
  const base = builder.build();
  // Zero bytes is a malformed durability row, so the fail-closed seam rejects
  // it at builder ingestion rather than letting the oracle catch a surrogate.
  assert.throws(
    () =>
      rebuild(base, {
        bodyDurability: [{ ...base.bodyDurability[0]!, bodyBytes: 0 }],
      }),
    LedgerIngestError,
  );
});

test("NOTICE-neg: durability ordered AFTER a notice fails notice_first only", () => {
  const { builder } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    bodyDurability: [{ ...base.bodyDurability[0]!, ordinal: 15 }],
  });
  assertOnlyFailing(ledger, "notice_first_body_withheld");
});

test("NOTICE-neg: a notice ordered after a body query fails notice_first only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    deliveries: [
      noticeFact(WORKER_1, fx.noticeDeliveryA, fx.subtaskA, ORD_NOTICE_A),
      bodyQueryFact(WORKER_1, fx.bodyDeliveryA, fx.subtaskA, ORD_QUERY_A, fx.turnWakeA),
      noticeFact(WORKER_2, fx.noticeDeliveryLoser, fx.subtaskA, ORD_NOTICE_LOSER),
      // subtaskB NOTICE ordered AFTER the body queries (ordinal 35 > 31).
      noticeFact(WORKER_2, fx.noticeDeliveryB, fx.subtaskB, 35),
      bodyQueryFact(WORKER_2, fx.bodyDeliveryB, fx.subtaskB, ORD_QUERY_B, fx.turnWakeB),
    ],
  });
  assertOnlyFailing(ledger, "notice_first_body_withheld");
});

// Body-query negatives: each must fail owner_only_body_read only.
test("QUERY-neg: winner body query at/before its claim fails owner_only only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    deliveries: [
      noticeFact(WORKER_1, fx.noticeDeliveryA, fx.subtaskA, ORD_NOTICE_A),
      // WORKER_1's query ordinal 15 is after the notices but NOT after its
      // claim (ordinal 20): a body read at/before the claim is not post-claim.
      bodyQueryFact(WORKER_1, fx.bodyDeliveryA, fx.subtaskA, 15, fx.turnWakeA),
      noticeFact(WORKER_2, fx.noticeDeliveryLoser, fx.subtaskA, ORD_NOTICE_LOSER),
      noticeFact(WORKER_2, fx.noticeDeliveryB, fx.subtaskB, ORD_NOTICE_B),
      bodyQueryFact(WORKER_2, fx.bodyDeliveryB, fx.subtaskB, ORD_QUERY_B, fx.turnWakeB),
    ],
  });
  assertOnlyFailing(ledger, "owner_only_body_read");
});

test("QUERY-neg: winner body query on the wrong wake turn fails owner_only only", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const ledger = rebuild(base, {
    deliveries: [
      noticeFact(WORKER_1, fx.noticeDeliveryA, fx.subtaskA, ORD_NOTICE_A),
      // WORKER_1's query turn is subtaskB's wake turn, not subtaskA's.
      bodyQueryFact(WORKER_1, fx.bodyDeliveryA, fx.subtaskA, ORD_QUERY_A, fx.turnWakeB),
      noticeFact(WORKER_2, fx.noticeDeliveryLoser, fx.subtaskA, ORD_NOTICE_LOSER),
      noticeFact(WORKER_2, fx.noticeDeliveryB, fx.subtaskB, ORD_NOTICE_B),
      bodyQueryFact(WORKER_2, fx.bodyDeliveryB, fx.subtaskB, ORD_QUERY_B, fx.turnWakeB),
    ],
  });
  assertOnlyFailing(ledger, "owner_only_body_read");
});

// Ambient positive: complete canonical evidence PLUS a valid durable body for an
// unrelated parent and unrelated deliveries still passes all eight (filtered).
test("AMBIENT: canonical evidence plus unrelated durability/deliveries still passes all eight", () => {
  const { builder, fx } = healthy();
  const otherParent = mintTaskId();
  const otherSubtask = mintTaskId();
  builder
    .addBodyDurability({
      parentTaskId: otherParent,
      bodyDigest: BODY_DIGEST,
      bodyBytes: 999,
      receiptId: mintReceiptId(),
      ordinal: 200,
    })
    .addDelivery(noticeFact("worker-9", mintDeliveryId(), otherSubtask, 201))
    .addDelivery(
      bodyQueryFact("worker-9", mintDeliveryId(), otherSubtask, 202, fx.turnWakeA),
    );
  const run = runScenario(chatTaskOrchestrationScenario, builder.build());
  assert.equal(run.advisory, "pass");
  for (const v of run.asserted) {
    assert.equal(v.status, "pass", `${v.conditionId} should pass: ${v.reason}`);
  }
});

// ---------------------------------------------------------------------------
// INGESTION SEAM: the runtime fail-closed parser is on the ACTUAL public paths,
// not beside them. A body-bearing/grammar-invalid row cannot reach the ledger
// through the builder, and a plain-object ledger is revalidated by the runner.
// ---------------------------------------------------------------------------

test("SEAM: builder addDelivery rejects a body-bearing row before storage", () => {
  const { builder, fx } = healthy();
  const bodyBearing = {
    ...noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13),
    body: "the secret body",
  } as unknown as NoticeDeliveryFact;
  assert.throws(() => builder.addDelivery(bodyBearing), LedgerIngestError);
});

test("SEAM: builder addDelivery rejects an unknown-field row before storage", () => {
  const { builder, fx } = healthy();
  const extra = {
    ...noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13),
    surprise: 1,
  } as unknown as NoticeDeliveryFact;
  assert.throws(() => builder.addDelivery(extra), LedgerIngestError);
});

test("SEAM: builder addDelivery rejects a delivery hiding a non-enumerable body", () => {
  const { builder, fx } = healthy();
  const hidden = { ...noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13) };
  Object.defineProperty(hidden, "body", { value: "the secret body", enumerable: false });
  assert.throws(
    () => builder.addDelivery(hidden as unknown as NoticeDeliveryFact),
    LedgerIngestError,
  );
});

test("SEAM: builder addBodyDurability rejects a body-bearing row before storage", () => {
  const { builder, fx } = healthy();
  const bad = {
    parentTaskId: fx.parentTaskId,
    bodyDigest: BODY_DIGEST,
    bodyBytes: 16,
    receiptId: mintReceiptId(),
    ordinal: 5,
    rawBody: "the secret body",
  } as unknown as Parameters<EvidenceLedgerBuilder["addBodyDurability"]>[0];
  assert.throws(() => builder.addBodyDurability(bad), LedgerIngestError);
});

test("SEAM: runScenario revalidates a plain-object ledger and rejects a body-bearing delivery", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const tainted = {
    ...base,
    deliveries: [
      ...base.deliveries,
      {
        ...noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13),
        body: "the secret body",
      },
    ],
  } as unknown as EvidenceLedger;
  assert.throws(
    () => runScenario(chatTaskOrchestrationScenario, tainted),
    LedgerIngestError,
  );
});

test("SEAM: runScenario rejects a plain-object ledger with a non-channel target", () => {
  const { builder } = healthy();
  const base = builder.build();
  const tainted = {
    ...base,
    deliveries: base.deliveries.map((o, i) =>
      i === 0 ? { ...o, target: "not-a-channel-target" } : o,
    ),
  } as unknown as EvidenceLedger;
  assert.throws(
    () => runScenario(chatTaskOrchestrationScenario, tainted),
    LedgerIngestError,
  );
});

test("SEAM: runScenario rejects a delivery hiding a NON-ENUMERABLE body", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const hidden = { ...noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13) };
  Object.defineProperty(hidden, "body", { value: "the secret body", enumerable: false });
  const tainted = {
    ...base,
    deliveries: [...base.deliveries, hidden],
  } as unknown as EvidenceLedger;
  assert.throws(
    () => runScenario(chatTaskOrchestrationScenario, tainted),
    LedgerIngestError,
  );
});

// Direct oracle predicate invocation (bypassing the parser AND the runner
// revalidation): the defensive oracle target/ref checks must still reject a
// malformed-but-present message ref, so a bypassed call cannot PASS.
function predicateVerdict(
  ledger: EvidenceLedger,
  conditionId: string,
): ConditionVerdict {
  const cond = chatTaskOrchestrationScenario.conditions.find(
    (c) => c.id === conditionId,
  );
  assert.ok(cond, `condition ${conditionId} must exist`);
  return cond.predicate(ledger);
}

// Direct-call analog of assertOnlyFailing for ledgers that cannot go through
// runScenario (its ingestion revalidation would THROW on the malformed row):
// call every asserted predicate directly and assert exactly `conditionId` fails.
function assertOnlyDirectFailing(
  ledger: EvidenceLedger,
  conditionId: string,
): void {
  for (const cond of chatTaskOrchestrationScenario.conditions) {
    const v = cond.predicate(ledger);
    if (v.status === "placeholder") continue;
    if (cond.id === conditionId) {
      assert.equal(v.status, "fail", `${cond.id} must fail`);
    } else {
      assert.equal(
        v.status,
        "pass",
        `${cond.id} must pass but failed: ${v.reason}`,
      );
    }
  }
}

test("DIRECT-DEFENSE: notice_first rejects malformed / wrong-prefix NOTICE message refs", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const badRefs: ReadonlyArray<readonly [string, string]> = [
    ["firstMessageId", "not-a-message-id"],
    ["latestMessageId", mintDeliveryId() as string], // wrong dlv_ prefix
  ];
  for (const [key, bad] of badRefs) {
    const tainted = {
      ...base,
      deliveries: base.deliveries.map((o) =>
        o.kind === "notice_metadata" &&
        (o.subtaskId as string) === (fx.subtaskA as string)
          ? { ...o, [key]: bad }
          : o,
      ),
    } as unknown as EvidenceLedger;
    const v = predicateVerdict(tainted, "notice_first_body_withheld");
    assert.equal(v.status, "fail", `notice_first must fail on ${key}=${bad}`);
    assert.match(v.reason, /non-MessageId|malformed/u);
  }
});

test("DIRECT-DEFENSE: owner_only rejects malformed / short / wrong-prefix queriedMessageId", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  for (const bad of ["not-a-message-id", "msg_short", mintDeliveryId() as string]) {
    const tainted = {
      ...base,
      deliveries: base.deliveries.map((o) =>
        o.kind === "body_read" &&
        (o.subtaskId as string) === (fx.subtaskA as string)
          ? { ...o, queriedMessageId: bad }
          : o,
      ),
    } as unknown as EvidenceLedger;
    const v = predicateVerdict(tainted, "owner_only_body_read");
    assert.equal(v.status, "fail", `owner_only must fail on queriedMessageId=${bad}`);
    assert.match(v.reason, /non-MessageId|non-channel/u);
  }
});

test("DIRECT-DEFENSE positive: valid refs still PASS the direct predicates", () => {
  const { builder } = healthy();
  const base = builder.build();
  assert.equal(
    predicateVerdict(base, "notice_first_body_withheld").status,
    "pass",
  );
  assert.equal(predicateVerdict(base, "owner_only_body_read").status, "pass");
});

// A malformed DUPLICATE NOTICE on a lane whose FIRST row is healthy must not be
// masked: notice_first validates EVERY canonical NOTICE, not first-per-lane.
test("DIRECT-DEFENSE: notice_first rejects a malformed duplicate NOTICE behind a valid first row", () => {
  const { builder, fx } = healthy();
  const base = builder.build();
  const badRefs: ReadonlyArray<readonly [string, string]> = [
    ["firstMessageId", "not-a-message-id"],
    ["latestMessageId", mintDeliveryId() as string], // wrong dlv_ prefix
  ];
  for (const [key, bad] of badRefs) {
    const dup = {
      ...noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13),
      [key]: bad,
    };
    const tainted = {
      ...base,
      deliveries: [...base.deliveries, dup],
    } as unknown as EvidenceLedger;
    // Exact sibling set via direct calls: ONLY notice_first fails.
    assertOnlyDirectFailing(tainted, "notice_first_body_withheld");
  }
});

// A LEGITIMATE second valid NOTICE on one lane (replay/reconnect) must still
// PASS — the frozen source does not cap a lane at one NOTICE.
test("notice_first PASSES with a legitimate multiple-valid NOTICE on one lane", () => {
  const { builder, fx } = healthy();
  builder.addDelivery(noticeFact(WORKER_1, mintDeliveryId(), fx.subtaskA, 13));
  const run = runScenario(chatTaskOrchestrationScenario, builder.build());
  assert.equal(run.advisory, "pass");
  for (const v of run.asserted) {
    assert.equal(v.status, "pass", `${v.conditionId} should pass: ${v.reason}`);
  }
});

// ---------------------------------------------------------------------------
// PLACEHOLDERS
// ---------------------------------------------------------------------------
test("placeholders report placeholder status and never affect advisory", () => {
  const { builder } = healthy();
  const run = runScenario(chatTaskOrchestrationScenario, builder.build());

  assert.equal(run.placeholders.length, 7);
  for (const v of run.placeholders) {
    assert.equal(v.status, "placeholder");
    assert.ok(v.reason.length > 0, `${v.conditionId} needs an entry-condition reason`);
  }
  assert.equal(run.advisory, "pass");
  // Total conditions = 8 asserted + 7 placeholder = 15.
  assert.equal(run.verdicts.length, 15);

  const placeholderIds = new Set(run.placeholders.map((v) => v.conditionId));
  for (const id of [
    "thread_owner_status_receipt",
    "lifecycle_and_illegal_transition",
    "task_status_freshness_hold",
    "delivery_reply_trailer",
    "coordination_slos",
    "channel_navigation_exact_target",
    "lane_role_fidelity",
  ]) {
    assert.ok(placeholderIds.has(id), `missing placeholder ${id}`);
  }
});
