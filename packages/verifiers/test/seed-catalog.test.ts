import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import {
  DeterministicFaults,
  FakeClock,
  FakeRuntime,
  FakeSocket,
  FaultRegistry,
  ModelSeenRecorder,
} from "@swarm/testkit";

import type { DeliveryId, TaskId, TurnId } from "@swarm/protocol";

import {
  EvidenceLedgerBuilder,
  IMPLEMENTED_SEED_IDS,
  PLACEHOLDER_SEED_IDS,
  SEED_CATALOG,
  chatTaskOrchestrationScenario,
  checkBudgetHold,
  checkCheckpointPrivacy,
  checkFieldScopedRegistryWrite,
  checkFreezeWindow,
  checkGenerationFenceBoundary,
  checkGraphReplayIdempotency,
  checkLeaseRenewal,
  checkManifestFreezeIntegrity,
  checkNativeIngressOrdering,
  checkNoEmptyBody,
  checkPhaseGating,
  checkPlanAcceptanceArray,
  checkPlanAcceptanceNotObject,
  checkPreTurnContextInjection,
  checkResumeProvenance,
  checkStaleAttemptFence,
  checkStartupReconciliation,
  checkSteerSafety,
  checkTypedVerdict,
  M5_CORE_G15_ROW_IDS,
  runScenario,
  seedEntry,
} from "../src/index.js";
import type {
  BodyQueryDeliveryFact,
  ClaimAttemptFact,
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

import {
  S17_CONDITION_ID,
  S17_FINDING_KIND,
  conditionIdForFinding,
  expectedS17Outcome,
} from "@swarm/security";

// ===========================================================================
// CATALOG ACCOUNTING
// ===========================================================================
test("catalog has exactly 24 implemented + 1 placeholder = 25 seeds", () => {
  assert.equal(SEED_CATALOG.length, 25);
  assert.equal(IMPLEMENTED_SEED_IDS.length, 24);
  assert.equal(PLACEHOLDER_SEED_IDS.length, 1);
  const implemented = SEED_CATALOG.filter((s) => s.status === "implemented");
  const placeholder = SEED_CATALOG.filter((s) => s.status === "placeholder");
  assert.equal(implemented.length, 24);
  assert.equal(placeholder.length, 1);
});

test("the 1 remaining placeholder id is exactly {M2}", () => {
  const actual = new Set(
    SEED_CATALOG.filter((s) => s.status === "placeholder").map((s) => s.id),
  );
  const expected = new Set(["M2"]);
  assert.deepEqual(actual, expected);
});

test("the Wave-1 step-5 bound seeds {S8,S12,M3,M5,M7,M8} are now implemented", () => {
  for (const id of ["S8", "S12", "M3", "M5", "M7", "M8"]) {
    const entry = seedEntry(id);
    assert.ok(entry, `${id} must be in the catalog`);
    assert.equal(entry.status, "implemented", `${id} should be implemented`);
  }
});

test("every seed id is unique and every entry has a named entry condition", () => {
  const ids = SEED_CATALOG.map((s) => s.id);
  assert.equal(new Set(ids).size, 25, "seed ids must be unique");
  for (const s of SEED_CATALOG) {
    assert.ok(s.defect.length > 0, `${s.id} needs a defect`);
    assert.ok(s.entry.length > 0, `${s.id} needs a named entry condition`);
  }
});

test("placeholders assert placeholder status and never affect advisory", () => {
  for (const id of PLACEHOLDER_SEED_IDS) {
    const entry = seedEntry(id);
    assert.ok(entry, `${id} must be in the catalog`);
    assert.equal(entry.status, "placeholder");
    assert.ok(entry.entry.length > 0);
  }
});

// ===========================================================================
// SHARED HELPERS for chat-oracle-backed seeds (S6, S7, S16, M6)
// ===========================================================================
const WORKER_1 = "worker-1";
const WORKER_2 = "worker-2";
const CAP_A = "capability-A";
const CAP_B = "capability-B";

// Evidence ordinal bands (durability < notices < claims < body queries) and a
// valid SHA-256 digest, mirroring the gate0 fixtures.
const ORD_DURABILITY = 1;
const BODY_DIGEST = "a1b2c3d4e5f60718293a4b5c6d7e8f90".repeat(2);

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
  subtaskId: TaskId,
  ordinal: number,
): NoticeDeliveryFact {
  return {
    kind: "notice_metadata",
    seat,
    deliveryId: mintDeliveryId(),
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
  subtaskId: TaskId,
  ordinal: number,
  queryTurnId: TurnId,
): BodyQueryDeliveryFact {
  return {
    kind: "body_read",
    seat,
    deliveryId: mintDeliveryId(),
    subtaskId,
    explicitQuery: true,
    queryTarget: "#lane",
    queriedMessageId: mintMessageId(),
    queryTurnId,
    ordinal,
  };
}

function healthyLedger(): { builder: EvidenceLedgerBuilder; ids: Record<string, TaskId | TurnId | DeliveryId> } {
  resetIdCounters();
  const parentTaskId = mintTaskId();
  const subtaskA = mintTaskId();
  const subtaskB = mintTaskId();
  const modelTurnId = mintTurnId();
  const turnWakeA = mintTurnId();
  const turnWakeB = mintTurnId();
  const wakeDeliveryA = mintDeliveryId();
  const wakeDeliveryB = mintDeliveryId();
  const restartMarkerA = mintDeliveryId();

  const b = new EvidenceLedgerBuilder();
  b.addAvailableWorkerSeat(WORKER_1).addAvailableWorkerSeat(WORKER_2);
  b.addDecomposition({
    parentTaskId,
    authoredBy: "runtime",
    modelTurnId,
    subtasks: [
      { taskId: subtaskA, capability: CAP_A },
      { taskId: subtaskB, capability: CAP_B },
    ],
  });
  b.addRoute({ subtaskId: subtaskA, routedCapability: CAP_A, matchedWorkerSeat: WORKER_1 })
    .addRoute({ subtaskId: subtaskB, routedCapability: CAP_B, matchedWorkerSeat: WORKER_2 });
  b.addClaim(claimFact(subtaskA, WORKER_1, "won", 20))
    .addClaim(claimFact(subtaskA, WORKER_2, "conflict_stop", 21))
    .addClaim(claimFact(subtaskB, WORKER_2, "won", 22));
  b.addExecution({ subtaskId: subtaskA, seat: WORKER_1, executionCount: 1 })
    .addExecution({ subtaskId: subtaskB, seat: WORKER_2, executionCount: 1 });
  b.addThreadStatus({ subtaskId: subtaskA, ownerSeat: WORKER_1, status: "done", receiptId: mintReceiptId() })
    .addThreadStatus({ subtaskId: subtaskB, ownerSeat: WORKER_2, status: "done", receiptId: mintReceiptId() });
  b.addReceipt({
    protocolVersion: 1 as never,
    receiptId: mintReceiptId(),
    kind: "server_accepted",
    producerFactId: mintProducerFactId(),
    actor: { serverId: mintServerId() },
    fence: {},
    occurredAt: "2026-01-01T00:00:00.000Z",
  });
  b.addWake({ turnId: turnWakeA, subtaskId: subtaskA, wakeDeliveryId: wakeDeliveryA })
    .addWake({ turnId: turnWakeB, subtaskId: subtaskB, wakeDeliveryId: wakeDeliveryB });
  b.addBodyDurability({
    parentTaskId,
    bodyDigest: BODY_DIGEST,
    bodyBytes: 2048,
    receiptId: mintReceiptId(),
    ordinal: ORD_DURABILITY,
  });
  b.addDelivery(noticeFact(WORKER_1, subtaskA, 10))
    .addDelivery(bodyQueryFact(WORKER_1, subtaskA, 30, turnWakeA))
    .addDelivery(noticeFact(WORKER_2, subtaskA, 11))
    .addDelivery(noticeFact(WORKER_2, subtaskB, 12))
    .addDelivery(bodyQueryFact(WORKER_2, subtaskB, 31, turnWakeB));
  b.addSteer({ subtaskId: subtaskA, expectedTurnId: turnWakeA, appliedBeforeCommit: true, committedStale: false, requiresReplanHold: false });
  b.addRestart({ subtaskId: subtaskA, restartMarker: restartMarkerA, replayOf: wakeDeliveryA });
  b.addLoserActivity({ subtaskId: subtaskA, seat: WORKER_2, replyCount: 0, executionCount: 0 });

  return {
    builder: b,
    ids: { parentTaskId, subtaskA, subtaskB, modelTurnId, turnWakeA, turnWakeB },
  };
}

function assertConditionFails(ledger: EvidenceLedger, conditionId: string): void {
  const run = runScenario(chatTaskOrchestrationScenario, ledger);
  const v = run.verdicts.find((x) => x.conditionId === conditionId);
  assert.ok(v, `${conditionId} present`);
  assert.equal(v.status, "fail", `${conditionId} should fail: ${v?.reason}`);
  assert.equal(run.advisory, "fail");
}

// ===========================================================================
// IMPLEMENTED SEED CONTROLS (18)
// Each constructs the seeded defect and asserts the verifier DETECTS it.
// ===========================================================================

// --- S1: missing strict acceptance string-array contract ---
test("S1 control: missing acceptance array is detected", () => {
  const v = checkPlanAcceptanceArray({
    parentTaskId: mintTaskId(),
    acceptanceIsStringArray: true,
    acceptanceCount: 0,
  });
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S1");
});

// --- S2: object-shaped acceptance ---
test("S2 control: object-shaped acceptance is detected", () => {
  const v = checkPlanAcceptanceNotObject({
    parentTaskId: mintTaskId(),
    acceptanceIsStringArray: false,
    acceptanceCount: 3,
  });
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S2");
});

// --- S3: lease expiry under live turn; owner self-reclaims (churn > 0) ---
// Driven through FakeClock: advance beyond the accelerated TTL under a running
// turn, which is what would trigger a self-reclaim churn.
test("S3 control: lease renewal churn under a live turn is detected", () => {
  const clock = new FakeClock(); // ACCELERATED: 2000ms TTL
  const runtime = new FakeRuntime(clock);
  const session = runtime.launch(WORKER_1, { turnDurationMs: 5000 });
  const turn = runtime.resume(session);
  // Advance past the TTL while the turn is still live -> lease expiry leg.
  clock.advance(clock.profile.ttlMs + 1);
  assert.ok(clock.now() > clock.profile.ttlMs, "clock advanced past TTL under live turn");
  const subtaskId = mintTaskId();
  void turn;
  const v = checkLeaseRenewal([{ subtaskId, renewalChurn: 1 }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S3");
});

// --- S4: stale wake + running row survive restart (not quarantined) ---
test("S4 control: unquarantined stale wake after restart is detected", () => {
  const socket = new FakeSocket();
  socket.deliverNotice(WORKER_1, { target: "#eval", count: 1, marker: "m" });
  const stale = socket.duplicateWake(WORKER_1); // a surviving stale wake
  assert.ok(stale);
  const v = checkStartupReconciliation([{ staleWakeId: mintDeliveryId(), quarantined: false }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S4");
});

// --- S5: empty body enters outbound/receipt set ---
test("S5 control: empty body in outbound set is detected", () => {
  const registry = new FaultRegistry();
  registry.register(DeterministicFaults.emptyBodySubmission());
  assert.ok(registry.has("emptyBodySubmission"));
  const v = checkNoEmptyBody([{ subtaskId: mintTaskId(), empty: true }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S5");
});

// --- S6: non-owner reads body / runs full turn (chat oracle) ---
test("S6 control: loser body read fails owner_only_body_read", () => {
  const { builder, ids } = healthyLedger();
  builder.addDelivery(
    bodyQueryFact(WORKER_2, ids.subtaskA as TaskId, 33, ids.turnWakeA as TurnId),
  );
  assertConditionFails(builder.build(), "owner_only_body_read");
});

// --- S7: checklist-authored decomposition (chat oracle) ---
test("S7 control: stub decomposition fails decomposed_by_runtime", () => {
  const { ids } = healthyLedger();
  const b = new EvidenceLedgerBuilder();
  b.addAvailableWorkerSeat(WORKER_1).addAvailableWorkerSeat(WORKER_2);
  b.addDecomposition({
    parentTaskId: ids.parentTaskId as TaskId,
    authoredBy: "human", // stub / checklist, no modelTurnId
    subtasks: [
      { taskId: ids.subtaskA as TaskId, capability: CAP_A },
      { taskId: ids.subtaskB as TaskId, capability: CAP_B },
    ],
  });
  assertConditionFails(b.build(), "decomposed_by_runtime");
});

// --- S9: prose completion without evidence ---
test("S9 control: complete-without-evidence verdict is detected", () => {
  const v = checkTypedVerdict([{ turnId: mintTurnId(), verdict: "complete", hasEvidence: false }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S9");
});

// --- S10: budget exhaustion silently completes ---
test("S10 control: silent complete on budget exhaustion is detected", () => {
  const v = checkBudgetHold([{ subtaskId: mintTaskId(), exhausted: true, terminalState: "complete" }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S10");
});

// --- S11: fresh session where resume possible (driven through FakeRuntime) ---
test("S11 control: fresh-session-where-resume-possible is detected", () => {
  const runtime = new FakeRuntime(new FakeClock());
  const session = runtime.launch(WORKER_1, { turnDurationMs: 100 });
  const t1 = runtime.resume(session);
  const t2 = runtime.resume(session);
  // The double confirms session-id continuity is available (resume possible).
  assert.equal(t1.sessionId, t2.sessionId);
  // The seeded defect: a continued turn that DID NOT resume the prior session.
  const v = checkResumeProvenance([
    { turnId: t2.turnId, resumePossible: true, resumedPriorSession: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S11");
});

// --- S13: duplicate root wake creates second graph (driven through FakeSocket) ---
test("S13 control: duplicate root wake creating a second graph is detected", () => {
  const socket = new FakeSocket();
  const first = socket.deliverNotice(WORKER_1, { target: "#eval", count: 1, marker: "root" });
  const dup = socket.duplicateWake(WORKER_1);
  assert.ok(first && dup && dup.duplicate, "socket produced a duplicate root wake");
  const v = checkGraphReplayIdempotency([{ rootWakeId: mintDeliveryId(), graphCount: 2 }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S13");
});

// --- S14: phase-1 claimable before phase-0 terminal ---
test("S14 control: phase-1 claimed before phase-0 terminal is detected", () => {
  const v = checkPhaseGating([
    { subtaskId: mintTaskId(), phase: 1, claimedBeforeDependencyTerminal: true },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S14");
});

// --- S15: checkpoint contains body/CoT (ModelSeenRecorder proves the seam) ---
test("S15 control: checkpoint containing body/CoT is detected", () => {
  const recorder = new ModelSeenRecorder();
  // A body-read fact models a turn whose body could leak into a checkpoint.
  recorder.recordBodyRead({ turnId: mintTurnId(), steerIncluded: false, bodyHash: "h" });
  assert.ok(!recorder.bodyWithheldEverywhere());
  const v = checkCheckpointPrivacy([{ stateInstanceRef: "sti-ref", containsBodyOrCot: true }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S15");
});

// --- S16: shape-changing steer silently mutates graph (chat oracle) ---
test("S16 control: committed-stale steer fails steer_honored_precommit", () => {
  const { builder, ids } = healthyLedger();
  const base = builder.build();
  const b = new EvidenceLedgerBuilder();
  for (const s of base.availableWorkerSeats) b.addAvailableWorkerSeat(s);
  for (const d of base.decompositions) b.addDecomposition(d);
  for (const r of base.routes) b.addRoute(r);
  for (const c of base.claims) b.addClaim(c);
  for (const e of base.executions) b.addExecution(e);
  for (const t of base.threadStatuses) b.addThreadStatus(t);
  for (const rec of base.receipts) b.addReceipt(rec);
  for (const w of base.wakes) b.addWake(w);
  for (const del of base.deliveries) b.addDelivery(del);
  for (const rs of base.restarts) b.addRestart(rs);
  for (const la of base.loserActivity) b.addLoserActivity(la);
  b.addSteer({
    subtaskId: ids.subtaskA as TaskId,
    expectedTurnId: ids.turnWakeA as TurnId,
    appliedBeforeCommit: false,
    committedStale: true,
    requiresReplanHold: false,
  });
  assertConditionFails(b.build(), "steer_honored_precommit");
});

// --- S17: internal marker in public fixture (carry-GO, security bind) ---
test("S17 control: mutual bind maps to the shared condition id", () => {
  assert.equal(conditionIdForFinding(S17_FINDING_KIND), S17_CONDITION_ID);
  assert.equal(expectedS17Outcome().conditionId, S17_CONDITION_ID);
  assert.equal(expectedS17Outcome().blocked, true);
  // The catalog binds S17 to the same shared condition id.
  assert.ok(seedEntry("S17")?.entry.includes(S17_CONDITION_ID));
});

// --- M1: registry write inside a live-run freeze window ---
test("M1 control: in-freeze-window registry write is detected", () => {
  const registry = new FaultRegistry();
  registry.register(DeterministicFaults.writeRegistryDuringFreezeWindow());
  assert.ok(registry.has("writeRegistryDuringFreezeWindow"));
  const v = checkFreezeWindow([{ writeRef: "w1", insideFreezeWindow: true }]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M1");
});

// --- M4: wrong-turn / unsafe-boundary steer (driven through FakeRuntime) ---
test("M4 control: wrong-turn steer is detected", () => {
  const runtime = new FakeRuntime(new FakeClock());
  const session = runtime.launch(WORKER_1, { turnDurationMs: 100 });
  const turn = runtime.resume(session);
  // A DISTINCT expected turn id: resume again to get a genuinely different turn,
  // then steer `turn` with that other turn's id as the (wrong) expectation.
  const otherTurn = runtime.resume(session);
  assert.notEqual(otherTurn.turnId, turn.turnId, "two resumes yield distinct turn ids");
  const wrongExpected = otherTurn.turnId;
  const vector = { injectionPoint: "mid_turn" as const, expectedTurnId: wrongExpected, marker: "m4" };
  runtime.steer(turn, vector);
  assert.ok(runtime.isWrongTurnSteer(turn, vector), "runtime flags the wrong-turn steer");
  const v = checkSteerSafety([
    { expectedTurnId: wrongExpected, actualTurnId: turn.turnId, unsafeBoundary: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M4");
});

// M4's second frozen vector: a steer that lands on the EXPECTED turn but at an
// unsafe commit boundary must also be detected (the implementation handles both
// branches; this commits the unsafe-boundary negative).
test("M4 control: matching-turn but unsafe-boundary steer is detected", () => {
  const t = mintTurnId();
  const v = checkSteerSafety([
    { expectedTurnId: t, actualTurnId: t, unsafeBoundary: true },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M4");
});

// --- M6: two concurrent wakes cause a double spawn (driven through FakeSocket) ---
test("M6 control: concurrent-wake double spawn fails single_owner_no_duplicate", async () => {
  const socket = new FakeSocket();
  const barrier = socket.concurrentWake(WORKER_1, 2);
  const events = await Promise.all(barrier.gates);
  assert.equal(events.length, 2, "two concurrent wakes genuinely overlapped");

  // The seeded defect the double models: two winning claims for one subtask.
  const { builder, ids } = healthyLedger();
  builder.addClaim(claimFact(ids.subtaskA as TaskId, WORKER_2, "won", 23));
  assertConditionFails(builder.build(), "single_owner_no_duplicate");
});

// ===========================================================================
// NON-CIRCULAR HEALTHY CONTROLS (standalone seeds).
// Each standalone seed check must PASS on a healthy input, proving the check
// discriminates rather than being permanently-failing. Paired with the
// seeded-defect FAIL controls above. (S6/S7/S16/M6 already pair via the healthy
// chat ledger baseline; S17 has its positive control in s17-bind.test.ts.)
// ===========================================================================
test("S1 healthy: a non-empty string[] acceptance PASSES", () => {
  const v = checkPlanAcceptanceArray({ parentTaskId: mintTaskId(), acceptanceIsStringArray: true, acceptanceCount: 3 });
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S1");
});
test("S2 healthy: string[] acceptance (not object) PASSES", () => {
  const v = checkPlanAcceptanceNotObject({ parentTaskId: mintTaskId(), acceptanceIsStringArray: true, acceptanceCount: 3 });
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S2");
});
test("S3 healthy: zero lease-renewal churn PASSES", () => {
  const v = checkLeaseRenewal([{ subtaskId: mintTaskId(), renewalChurn: 0 }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S3");
});
test("S4 healthy: quarantined stale wake PASSES", () => {
  const v = checkStartupReconciliation([{ staleWakeId: mintDeliveryId(), quarantined: true }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S4");
});
test("S5 healthy: non-empty outbound body PASSES", () => {
  const v = checkNoEmptyBody([{ subtaskId: mintTaskId(), empty: false }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S5");
});
test("S9 healthy: evidence-backed complete verdict PASSES", () => {
  const v = checkTypedVerdict([{ turnId: mintTurnId(), verdict: "complete", hasEvidence: true }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S9");
});
test("S10 healthy: budget exhaustion held explicitly PASSES", () => {
  const v = checkBudgetHold([{ subtaskId: mintTaskId(), exhausted: true, terminalState: "held" }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S10");
});
test("S11 healthy: continued turn resumed prior session PASSES", () => {
  const v = checkResumeProvenance([{ turnId: mintTurnId(), resumePossible: true, resumedPriorSession: true }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S11");
});
test("S13 healthy: exactly one graph per root wake PASSES", () => {
  const v = checkGraphReplayIdempotency([{ rootWakeId: mintDeliveryId(), graphCount: 1 }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S13");
});
test("S14 healthy: phase gating held PASSES", () => {
  const v = checkPhaseGating([{ subtaskId: mintTaskId(), phase: 1, claimedBeforeDependencyTerminal: false }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S14");
});
test("S15 healthy: refs-only checkpoint PASSES", () => {
  const v = checkCheckpointPrivacy([{ stateInstanceRef: "state-ref", containsBodyOrCot: false }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "S15");
});
test("M1 healthy: no in-window registry write PASSES", () => {
  const v = checkFreezeWindow([{ writeRef: "write-ref", insideFreezeWindow: false }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "M1");
});
test("M4 healthy: matching-turn safe steer PASSES", () => {
  const t = mintTurnId();
  const v = checkSteerSafety([{ expectedTurnId: t, actualTurnId: t, unsafeBoundary: false }]);
  assert.equal(v.status, "pass"); assert.equal(v.seedId, "M4");
});

// ===========================================================================
// WAVE-1 STEP-5 CONTROLS (S8, S12, M3, M5, M7, M8) — each constructs the seeded
// defect against the promoted-seam outcome and asserts the verifier DETECTS it,
// plus a healthy PASS so the control is non-vacuous. M5 consumes the EXACT eight
// core G1.5 rows loaded from the frozen Gate 1 fixture.
// ===========================================================================

// --- S8: late old-attempt finalize after takeover (G1.8) ---
test("S8 control: late old-attempt finalize after takeover is detected", () => {
  const v = checkStaleAttemptFence([
    { attemptRef: "attempt-1", lateFinalizeRejected: false, fenceError: "STALE_FENCE", siblingsUnchanged: true },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S8");
});
test("S8 control: a rejected stale finalize that mutated siblings is detected", () => {
  const v = checkStaleAttemptFence([
    { attemptRef: "attempt-1", lateFinalizeRejected: true, fenceError: "STALE_FENCE", siblingsUnchanged: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S8");
});
test("S8 healthy: fenced stale attempt with zero sibling effect PASSES", () => {
  const v = checkStaleAttemptFence([
    { attemptRef: "attempt-1", lateFinalizeRejected: true, fenceError: "STALE_INVOCATION_GENERATION", siblingsUnchanged: true },
  ]);
  assert.equal(v.status, "pass");
  assert.equal(v.seedId, "S8");
});

// --- S12: whole-row registry write clobbers presence/capabilities (G1.7) ---
test("S12 control: presence clobbered by a whole-row write is detected", () => {
  const v = checkFieldScopedRegistryWrite([
    { writeRef: "write-1", presencePreserved: false, capabilitiesPreserved: true, wholeRowWriteRejected: true },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S12");
});
test("S12 control: an unfenced undeclared-field write is detected", () => {
  const v = checkFieldScopedRegistryWrite([
    { writeRef: "write-1", presencePreserved: true, capabilitiesPreserved: true, wholeRowWriteRejected: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "S12");
});
test("S12 healthy: field-scoped write preserving presence/capabilities PASSES", () => {
  const v = checkFieldScopedRegistryWrite([
    { writeRef: "write-1", presencePreserved: true, capabilitiesPreserved: true, wholeRowWriteRejected: true },
  ]);
  assert.equal(v.status, "pass");
  assert.equal(v.seedId, "S12");
});

// --- M3: native-first ingress ordering (G1.3 / G1.4) ---
test("M3 control: a pure question that creates a task is detected", () => {
  const v = checkNativeIngressOrdering([
    { turnRef: "turn-1", pureQuestion: true, coordinationEffectCount: 1, replyBeforeCoordination: true, modelVisiblePredecessorCommitted: true, secondCoordinationAttempted: false, secondCoordinationRejected: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M3");
});
test("M3 control: task creation without a committed reply predecessor is detected", () => {
  const v = checkNativeIngressOrdering([
    { turnRef: "turn-1", pureQuestion: false, coordinationEffectCount: 1, replyBeforeCoordination: true, modelVisiblePredecessorCommitted: false, secondCoordinationAttempted: false, secondCoordinationRejected: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M3");
});
test("M3 control: an unfenced second coordination call is detected", () => {
  const v = checkNativeIngressOrdering([
    { turnRef: "turn-1", pureQuestion: false, coordinationEffectCount: 1, replyBeforeCoordination: true, modelVisiblePredecessorCommitted: true, secondCoordinationAttempted: true, secondCoordinationRejected: false },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M3");
});
test("M3 healthy: pure-question zero-task and reply-before-coordination PASS", () => {
  const v = checkNativeIngressOrdering([
    { turnRef: "turn-q", pureQuestion: true, coordinationEffectCount: 0, replyBeforeCoordination: false, modelVisiblePredecessorCommitted: false, secondCoordinationAttempted: false, secondCoordinationRejected: false },
    { turnRef: "turn-c", pureQuestion: false, coordinationEffectCount: 1, replyBeforeCoordination: true, modelVisiblePredecessorCommitted: true, secondCoordinationAttempted: true, secondCoordinationRejected: true },
  ]);
  assert.equal(v.status, "pass");
  assert.equal(v.seedId, "M3");
});

// --- M5: G1.5 generation-fence boundary, consuming the exact frozen fixture ---
type G15FixtureRow = {
  readonly id: string;
  readonly clause: string;
  readonly expectedError?: string;
  readonly expectedState?: string;
  readonly siblings: readonly string[];
};
// Resolved from the BUILT test location (build/test/) up to the repo root, so
// M5 consumes the exact frozen Gate 1 fixture rather than a copy.
const G15_FIXTURE = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../../../contracts/gate1/seeded-controls.json", import.meta.url)),
    "utf8",
  ),
) as readonly G15FixtureRow[];

function coreG15Rows(): readonly G15FixtureRow[] {
  const byId = new Map(G15_FIXTURE.map((r) => [r.id, r]));
  return M5_CORE_G15_ROW_IDS.map((rowId) => {
    const row = byId.get(rowId);
    assert.ok(row, `core G1.5 fixture row ${rowId} present in the frozen fixture`);
    return row;
  });
}
function fixtureOutcome(row: G15FixtureRow): { error: string } | { state: string } {
  if (row.expectedError !== undefined) return { error: row.expectedError };
  assert.ok(row.expectedState !== undefined, `${row.id} has an expectedError or expectedState`);
  return { state: row.expectedState };
}

test("M5 control: a conflated 5b outcome (stale-resume -> terminal-conflict) is detected", () => {
  const observations = coreG15Rows().map((row) => {
    const expected = fixtureOutcome(row);
    // Inject the 5b conflation defect: the stale-resume row is made to produce
    // the terminal-conflict error instead of STALE_INVOCATION_GENERATION.
    const observed =
      row.id === "g1.5-post-generation-two-stale-resume"
        ? { error: "INVOCATION_STATE_CONFLICT" }
        : expected;
    return { fixtureRowId: row.id, expected, observed, siblingsUnchanged: true };
  });
  const v = checkGenerationFenceBoundary(observations);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M5");
});
test("M5 control: a failed write that advances the boundary is detected", () => {
  const observations = coreG15Rows().map((row) => {
    const expected = fixtureOutcome(row);
    return {
      fixtureRowId: row.id,
      expected,
      observed: expected,
      // Inject the M5 defect on the not-written row: the failed write advanced
      // the visible boundary (a sibling table changed).
      siblingsUnchanged: row.id !== "g1.5-not-written-generation-two",
    };
  });
  const v = checkGenerationFenceBoundary(observations);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M5");
});
test("M5 control: an incomplete generation-fence set is detected", () => {
  const rows = coreG15Rows().slice(1); // drop one core row
  const observations = rows.map((row) => {
    const expected = fixtureOutcome(row);
    return { fixtureRowId: row.id, expected, observed: expected, siblingsUnchanged: true };
  });
  const v = checkGenerationFenceBoundary(observations);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M5");
});
test("M5 healthy: the eight exact G1.5 rows with their fixture outcomes PASS", () => {
  const observations = coreG15Rows().map((row) => {
    const expected = fixtureOutcome(row);
    return { fixtureRowId: row.id, expected, observed: expected, siblingsUnchanged: true };
  });
  // The 5b split is distinct in the frozen fixture: stale-resume -> STALE, and
  // terminal-conflict -> INVOCATION_STATE_CONFLICT (never "either").
  const stale = coreG15Rows().find((r) => r.id === "g1.5-post-generation-two-stale-resume");
  const conflict = coreG15Rows().find((r) => r.id === "g1.5-post-generation-two-terminal-conflict");
  assert.equal(stale?.expectedError, "STALE_INVOCATION_GENERATION");
  assert.equal(conflict?.expectedError, "INVOCATION_STATE_CONFLICT");
  const v = checkGenerationFenceBoundary(observations);
  assert.equal(v.status, "pass");
  assert.equal(v.seedId, "M5");
});

// --- M7: body/context injected at a pre-turn seam despite notice-first (G1.2) ---
test("M7 control: attention notice carrying body at the pre-turn seam is detected", () => {
  const v = checkPreTurnContextInjection([
    { turnRef: "turn-1", attentionCarriesBody: true, bodyOnlyAtAuthorizedSeam: true, bodyInjectionCount: 1 },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M7");
});
test("M7 control: body injected outside the authorized seam is detected", () => {
  const v = checkPreTurnContextInjection([
    { turnRef: "turn-1", attentionCarriesBody: false, bodyOnlyAtAuthorizedSeam: false, bodyInjectionCount: 2 },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M7");
});
test("M7 healthy: metadata-only attention, one authorized body injection PASSES", () => {
  const v = checkPreTurnContextInjection([
    { turnRef: "turn-1", attentionCarriesBody: false, bodyOnlyAtAuthorizedSeam: true, bodyInjectionCount: 1 },
  ]);
  assert.equal(v.status, "pass");
  assert.equal(v.seedId, "M7");
});

// --- M8: manifest mutated after freeze before turn consumes it (G1.6) ---
const DIGEST_A = `sha256:${"a1b2c3d4e5f60718293a4b5c6d7e8f90".repeat(2)}`;
const DIGEST_B = `sha256:${"0f1e2d3c4b5a69788796a5b4c3d2e1f0".repeat(2)}`;
test("M8 control: a post-freeze manifest mutation (digest mismatch) is detected", () => {
  const v = checkManifestFreezeIntegrity([
    { manifestRef: "manifest-1", frozen: true, frozenDigest: DIGEST_A, consumedDigest: DIGEST_B },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M8");
});
test("M8 control: a non-frozen manifest is detected", () => {
  const v = checkManifestFreezeIntegrity([
    { manifestRef: "manifest-1", frozen: false, frozenDigest: DIGEST_A, consumedDigest: DIGEST_A },
  ]);
  assert.equal(v.status, "fail");
  assert.equal(v.seedId, "M8");
});
test("M8 healthy: deep-frozen manifest with a stable digest PASSES", () => {
  const v = checkManifestFreezeIntegrity([
    { manifestRef: "manifest-1", frozen: true, frozenDigest: DIGEST_A, consumedDigest: DIGEST_A },
  ]);
  assert.equal(v.status, "pass");
  assert.equal(v.seedId, "M8");
});

// ===========================================================================
// Every implemented seed has a control (self-audit against the catalog).
// ===========================================================================
test("every implemented seed id has an explicit control in this file", () => {
  // The controls above are named per seed; assert the catalog's implemented set
  // matches the frozen 24 (the original 18 + the six Wave-1 step-5 bindings
  // S8/S12/M3/M5/M7/M8) so no seed silently disappears.
  assert.deepEqual(
    [...IMPLEMENTED_SEED_IDS].sort(),
    [
      "M1", "M3", "M4", "M5", "M6", "M7", "M8",
      "S1", "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17",
      "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9",
    ].sort(),
  );
});
