import type { DeliveryId, TaskId, TurnId } from "@swarm/protocol";

// Minimal, SOUND oracle surface for the seeded controls that assert beyond the
// eight chat-contract conditions. Each check is FAIL-CLOSED: it requires the
// positive evidence the seed's defect would remove, so a zero-evidence ledger
// fails. All evidence fields are neutral: ids, counts, refs, flags, hashes only,
// never bodies / prompts / chain-of-thought / paths / names.
//
// These are deliberately small: each proves that the verifier CAN detect the
// seeded defect, which is the Gate-0 obligation ("a verifier that cannot fail is
// not a verifier"). They are not the full Wave-1 production oracle.

export type SeedCheckVerdict = {
  readonly seedId: string;
  readonly status: "pass" | "fail";
  readonly reason: string;
};

function ok(seedId: string, reason: string): SeedCheckVerdict {
  return { seedId, status: "pass", reason };
}
function bad(seedId: string, reason: string): SeedCheckVerdict {
  return { seedId, status: "fail", reason };
}

// ---------------------------------------------------------------------------
// S1 / S2 — plan-shape: acceptance must be a NON-EMPTY string array; an
// object-shaped or empty acceptance FAILs.
// ---------------------------------------------------------------------------
export type PlanShapeFact = {
  readonly parentTaskId: TaskId;
  /** True iff acceptance was committed as a string[] (not an object). */
  readonly acceptanceIsStringArray: boolean;
  /** Count of acceptance entries (0 -> missing contract). */
  readonly acceptanceCount: number;
};

export function checkPlanAcceptanceArray(
  fact: PlanShapeFact | undefined,
): SeedCheckVerdict {
  const seedId = "S1";
  if (fact === undefined) return bad(seedId, "no plan-shape evidence present");
  if (!fact.acceptanceIsStringArray) {
    return bad(seedId, "acceptance is not a string[] (object-shaped)");
  }
  if (fact.acceptanceCount < 1) {
    return bad(seedId, "acceptance array is empty (missing strict contract)");
  }
  return ok(seedId, `acceptance is a string[] with ${fact.acceptanceCount} entries`);
}

export function checkPlanAcceptanceNotObject(
  fact: PlanShapeFact | undefined,
): SeedCheckVerdict {
  const seedId = "S2";
  if (fact === undefined) return bad(seedId, "no plan-shape evidence present");
  if (!fact.acceptanceIsStringArray) {
    return bad(seedId, "object-shaped acceptance committed");
  }
  return ok(seedId, "acceptance committed as a string[]");
}

// ---------------------------------------------------------------------------
// S3 — lease renewal during a live turn: renewal churn must be 0.
// ---------------------------------------------------------------------------
export type LeaseRenewalFact = {
  readonly subtaskId: TaskId;
  /** Number of self-reclaim/renewal churns under a live turn (must be 0). */
  readonly renewalChurn: number;
};

export function checkLeaseRenewal(
  facts: readonly LeaseRenewalFact[],
): SeedCheckVerdict {
  const seedId = "S3";
  if (facts.length === 0) return bad(seedId, "no lease-renewal evidence present");
  const churned = facts.find((f) => f.renewalChurn > 0);
  if (churned !== undefined) {
    return bad(
      seedId,
      `subtask ${churned.subtaskId} lease churned ${churned.renewalChurn} times under a live turn`,
    );
  }
  return ok(seedId, `no lease churn across ${facts.length} live turn(s)`);
}

// ---------------------------------------------------------------------------
// S4 — startup reconciliation: a stale wake + running-turn row surviving a
// restart must be quarantined (reconciled), not left live.
// ---------------------------------------------------------------------------
export type ReconciliationFact = {
  readonly staleWakeId: DeliveryId;
  /** True iff the stale wake/running-row was quarantined on restart. */
  readonly quarantined: boolean;
};

export function checkStartupReconciliation(
  facts: readonly ReconciliationFact[],
): SeedCheckVerdict {
  const seedId = "S4";
  if (facts.length === 0) return bad(seedId, "no reconciliation evidence present");
  const leaked = facts.find((f) => !f.quarantined);
  if (leaked !== undefined) {
    return bad(seedId, `stale wake ${leaked.staleWakeId} not quarantined on restart`);
  }
  return ok(seedId, `all ${facts.length} stale wake(s) quarantined`);
}

// ---------------------------------------------------------------------------
// S5 — empty body must not enter the outbound/receipt/finalize set.
// ---------------------------------------------------------------------------
export type OutboundContentFact = {
  readonly subtaskId: TaskId;
  /** True iff the outbound/receipt content was empty/whitespace. */
  readonly empty: boolean;
};

export function checkNoEmptyBody(
  facts: readonly OutboundContentFact[],
): SeedCheckVerdict {
  const seedId = "S5";
  if (facts.length === 0) return bad(seedId, "no outbound-content evidence present");
  const empty = facts.find((f) => f.empty);
  if (empty !== undefined) {
    return bad(seedId, `empty body entered outbound/receipt for subtask ${empty.subtaskId}`);
  }
  return ok(seedId, `no empty body across ${facts.length} outbound item(s)`);
}

// ---------------------------------------------------------------------------
// S9 — typed turn verdict: a "complete" verdict must carry evidence.
// ---------------------------------------------------------------------------
export type TypedVerdictFact = {
  readonly turnId: TurnId;
  readonly verdict: "continue" | "complete" | "held" | "failed";
  /** True iff a durable artifact/receipt backs the verdict. */
  readonly hasEvidence: boolean;
};

export function checkTypedVerdict(
  facts: readonly TypedVerdictFact[],
): SeedCheckVerdict {
  const seedId = "S9";
  if (facts.length === 0) return bad(seedId, "no typed-verdict evidence present");
  const prose = facts.find((f) => f.verdict === "complete" && !f.hasEvidence);
  if (prose !== undefined) {
    return bad(seedId, `turn ${prose.turnId} claims complete with no evidence (prose completion)`);
  }
  return ok(seedId, `all ${facts.length} verdict(s) evidence-backed where complete`);
}

// ---------------------------------------------------------------------------
// S10 — budget exhaustion must produce an explicit hold, not a silent complete.
// ---------------------------------------------------------------------------
export type BudgetFact = {
  readonly subtaskId: TaskId;
  readonly exhausted: boolean;
  /** Terminal state the runtime recorded on budget exhaustion. */
  readonly terminalState: "held" | "complete" | "failed";
};

export function checkBudgetHold(facts: readonly BudgetFact[]): SeedCheckVerdict {
  const seedId = "S10";
  if (facts.length === 0) return bad(seedId, "no budget evidence present");
  const silent = facts.find((f) => f.exhausted && f.terminalState === "complete");
  if (silent !== undefined) {
    return bad(seedId, `subtask ${silent.subtaskId} silently completed on budget exhaustion`);
  }
  return ok(seedId, `budget exhaustion held explicitly across ${facts.length} subtask(s)`);
}

// ---------------------------------------------------------------------------
// S11 — resume-session provenance: a continued turn must reuse the prior
// session id, not open a fresh session where resume was possible.
// ---------------------------------------------------------------------------
export type ResumeProvenanceFact = {
  readonly turnId: TurnId;
  /** True iff a prior session existed and could be resumed. */
  readonly resumePossible: boolean;
  /** True iff the turn actually resumed the prior session id. */
  readonly resumedPriorSession: boolean;
};

export function checkResumeProvenance(
  facts: readonly ResumeProvenanceFact[],
): SeedCheckVerdict {
  const seedId = "S11";
  if (facts.length === 0) return bad(seedId, "no resume-provenance evidence present");
  const fresh = facts.find((f) => f.resumePossible && !f.resumedPriorSession);
  if (fresh !== undefined) {
    return bad(seedId, `turn ${fresh.turnId} opened a fresh session where resume was possible`);
  }
  return ok(seedId, `all ${facts.length} continued turn(s) resumed the prior session`);
}

// ---------------------------------------------------------------------------
// S13 — graph replay idempotency: a duplicate root wake must NOT create a
// second task graph (root+version+key stays unique).
// ---------------------------------------------------------------------------
export type GraphReplayFact = {
  readonly rootWakeId: DeliveryId;
  /** Number of distinct task graphs created for this root (must be 1). */
  readonly graphCount: number;
};

export function checkGraphReplayIdempotency(
  facts: readonly GraphReplayFact[],
): SeedCheckVerdict {
  const seedId = "S13";
  if (facts.length === 0) return bad(seedId, "no graph-replay evidence present");
  const dup = facts.find((f) => f.graphCount !== 1);
  if (dup !== undefined) {
    return bad(
      seedId,
      `root wake ${dup.rootWakeId} created ${dup.graphCount} graphs (duplicate root wake)`,
    );
  }
  return ok(seedId, `all ${facts.length} root wake(s) created exactly one graph`);
}

// ---------------------------------------------------------------------------
// S14 — phase gating: a phase-1 subtask must not be claimable before its
// phase-0 dependency is terminal.
// ---------------------------------------------------------------------------
export type PhaseGateFact = {
  readonly subtaskId: TaskId;
  readonly phase: number;
  /** True iff a lower-phase dependency was still non-terminal at claim time. */
  readonly claimedBeforeDependencyTerminal: boolean;
};

export function checkPhaseGating(
  facts: readonly PhaseGateFact[],
): SeedCheckVerdict {
  const seedId = "S14";
  if (facts.length === 0) return bad(seedId, "no phase-gating evidence present");
  const early = facts.find((f) => f.claimedBeforeDependencyTerminal);
  if (early !== undefined) {
    return bad(
      seedId,
      `subtask ${early.subtaskId} (phase ${early.phase}) claimable before its dependency terminal`,
    );
  }
  return ok(seedId, `phase gating held across ${facts.length} subtask(s)`);
}

// ---------------------------------------------------------------------------
// S15 — checkpoint privacy: a checkpoint must carry refs/hashes only, never a
// body/CoT payload.
// ---------------------------------------------------------------------------
export type CheckpointFact = {
  readonly stateInstanceRef: string;
  /** True iff the checkpoint embedded raw body/CoT text (privacy violation). */
  readonly containsBodyOrCot: boolean;
};

export function checkCheckpointPrivacy(
  facts: readonly CheckpointFact[],
): SeedCheckVerdict {
  const seedId = "S15";
  if (facts.length === 0) return bad(seedId, "no checkpoint evidence present");
  const leak = facts.find((f) => f.containsBodyOrCot);
  if (leak !== undefined) {
    return bad(seedId, `checkpoint ${leak.stateInstanceRef} contains body/CoT (privacy bar)`);
  }
  return ok(seedId, `all ${facts.length} checkpoint(s) carry refs/hashes only`);
}

// ---------------------------------------------------------------------------
// M1 — freeze-window audit: no registry write may be timestamped inside a
// live-run freeze window.
// ---------------------------------------------------------------------------
export type FreezeWindowWriteFact = {
  readonly writeRef: string;
  /** True iff the write was timestamped inside a live-run freeze window. */
  readonly insideFreezeWindow: boolean;
};

export function checkFreezeWindow(
  facts: readonly FreezeWindowWriteFact[],
): SeedCheckVerdict {
  const seedId = "M1";
  if (facts.length === 0) return bad(seedId, "no freeze-window evidence present");
  const inWindow = facts.find((f) => f.insideFreezeWindow);
  if (inWindow !== undefined) {
    return bad(seedId, `registry write ${inWindow.writeRef} landed inside a freeze window`);
  }
  return ok(seedId, `no in-window write across ${facts.length} registry write(s)`);
}

// ---------------------------------------------------------------------------
// M4 — steer safety: a steer's expectedTurnId must match the turn it lands in,
// and it must not land on an unsafe boundary.
// ---------------------------------------------------------------------------
export type SteerSafetyFact = {
  readonly expectedTurnId: TurnId;
  readonly actualTurnId: TurnId;
  /** True iff the steer landed at an unsafe commit boundary. */
  readonly unsafeBoundary: boolean;
};

export function checkSteerSafety(
  facts: readonly SteerSafetyFact[],
): SeedCheckVerdict {
  const seedId = "M4";
  if (facts.length === 0) return bad(seedId, "no steer-safety evidence present");
  const wrong = facts.find(
    (f) => f.expectedTurnId !== f.actualTurnId || f.unsafeBoundary,
  );
  if (wrong !== undefined) {
    return bad(
      seedId,
      wrong.expectedTurnId !== wrong.actualTurnId
        ? `steer expected turn ${wrong.expectedTurnId} but landed in ${wrong.actualTurnId} (wrong-turn)`
        : `steer landed on an unsafe boundary`,
    );
  }
  return ok(seedId, `all ${facts.length} steer(s) landed on the expected safe turn`);
}
