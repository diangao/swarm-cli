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

// ===========================================================================
// WAVE-1 CONTROLS (S8, S12, M3, M5, M7, M8) — bound to the promoted Lane A
// (server-facts/storage) and Lane B (native-first runtime) seams at the final
// integration base. Each remains FAIL-CLOSED and neutral: it consumes only the
// fence OUTCOME (error codes, generation numbers, digests, counts, flags) that
// the real seam produced, never the body / prompt / manifest content / path /
// name. M2 (lane-role fidelity) stays a placeholder; it is NOT bound here.
// ===========================================================================

// ---------------------------------------------------------------------------
// S8 — late old-attempt finalize after takeover (G1.8). The promoted
// stale-attempt repository fence must reject a finalize from a superseded
// delivery attempt / stale invocation generation, and the reject must leave the
// terminal-state siblings (artifacts, review_receipts, task_terminal_state)
// unchanged. A finalize that lands after takeover, or a reject with a non-zero
// sibling effect, FAILs.
// ---------------------------------------------------------------------------

/** Fence error codes the promoted stale-attempt repository seam raises. */
const S8_FENCE_ERRORS: ReadonlySet<string> = new Set([
  "STALE_FENCE",
  "OUTBOX_STALE_ATTEMPT",
  "STALE_DELIVERY_FENCE",
  "STALE_INVOCATION_GENERATION",
]);

export type StaleAttemptFenceFact = {
  /** Opaque ref for the superseded delivery attempt / job. */
  readonly attemptRef: string;
  /** True iff the late old-attempt finalize was rejected by the fence. */
  readonly lateFinalizeRejected: boolean;
  /** The fence error the seam raised (must be a stale-attempt fence code). */
  readonly fenceError: string;
  /**
   * True iff the terminal-state siblings (artifacts / review_receipts /
   * task_terminal_state) were left byte-unchanged by the rejected attempt.
   */
  readonly siblingsUnchanged: boolean;
};

export function checkStaleAttemptFence(
  facts: readonly StaleAttemptFenceFact[],
): SeedCheckVerdict {
  const seedId = "S8";
  if (facts.length === 0) {
    return bad(seedId, "no stale-attempt fence evidence present");
  }
  for (const f of facts) {
    if (!f.lateFinalizeRejected) {
      return bad(
        seedId,
        `attempt ${f.attemptRef} late-finalized after takeover (stale attempt not fenced)`,
      );
    }
    if (!S8_FENCE_ERRORS.has(f.fenceError)) {
      return bad(
        seedId,
        `attempt ${f.attemptRef} rejected with "${f.fenceError}", not a stale-attempt fence code`,
      );
    }
    if (!f.siblingsUnchanged) {
      return bad(
        seedId,
        `attempt ${f.attemptRef} stale finalize mutated terminal-state siblings (non-zero effect)`,
      );
    }
  }
  return ok(
    seedId,
    `all ${facts.length} late old-attempt finalize(s) fenced with zero terminal-state effect`,
  );
}

// ---------------------------------------------------------------------------
// S12 — whole-row registry write clobbers presence/capabilities (G1.7). A
// configuration write must be FIELD-SCOPED: it may not clobber the presence or
// capabilities fields, and an undeclared-field (whole-row) write must be
// rejected (UNDECLARED_FIELD_WRITE), with a stale row-version/routing-generation
// rejected as STALE_FENCE. Clobbered presence/capabilities, or an unfenced
// whole-row write, FAILs.
// ---------------------------------------------------------------------------

export type RegistryFieldWriteFact = {
  /** Opaque ref for the registry write. */
  readonly writeRef: string;
  /** True iff the presence field was preserved (not clobbered) by the write. */
  readonly presencePreserved: boolean;
  /** True iff the capabilities field was preserved (not clobbered). */
  readonly capabilitiesPreserved: boolean;
  /** True iff an attempted undeclared-field (whole-row) write was rejected. */
  readonly wholeRowWriteRejected: boolean;
};

export function checkFieldScopedRegistryWrite(
  facts: readonly RegistryFieldWriteFact[],
): SeedCheckVerdict {
  const seedId = "S12";
  if (facts.length === 0) {
    return bad(seedId, "no registry field-write evidence present");
  }
  for (const f of facts) {
    if (!f.presencePreserved) {
      return bad(
        seedId,
        `write ${f.writeRef} clobbered the presence field (whole-row write)`,
      );
    }
    if (!f.capabilitiesPreserved) {
      return bad(
        seedId,
        `write ${f.writeRef} clobbered the capabilities field (whole-row write)`,
      );
    }
    if (!f.wholeRowWriteRejected) {
      return bad(
        seedId,
        `write ${f.writeRef} undeclared-field write not fenced (UNDECLARED_FIELD_WRITE missing)`,
      );
    }
  }
  return ok(
    seedId,
    `all ${facts.length} registry write(s) field-scoped; presence/capabilities preserved`,
  );
}

// ---------------------------------------------------------------------------
// M3 — native-first ingress ordering (G1.3 / G1.4). A pure question must create
// ZERO coordination effect (no task/command). A coordination effect (task
// creation) requires a committed model-visible reply predecessor and must
// follow the reply, and a second coordination call in the same turn must be
// rejected. A pure question that creates a task, a task without a committed
// reply predecessor, coordination before reply, or an unfenced second
// coordination call FAILs.
// ---------------------------------------------------------------------------

export type NativeIngressOrderFact = {
  /** Opaque ref for the native ingress turn. */
  readonly turnRef: string;
  /** True iff the ingress was a pure question (no coordination expected). */
  readonly pureQuestion: boolean;
  /** Number of coordination effects (tasks/commands) the turn produced. */
  readonly coordinationEffectCount: number;
  /** True iff a coordination effect followed a committed reply on the turn. */
  readonly replyBeforeCoordination: boolean;
  /** True iff task creation had a committed model-visible reply predecessor. */
  readonly modelVisiblePredecessorCommitted: boolean;
  /** True iff a second coordination call was attempted in the turn. */
  readonly secondCoordinationAttempted: boolean;
  /** True iff that second coordination call was rejected. */
  readonly secondCoordinationRejected: boolean;
};

export function checkNativeIngressOrdering(
  facts: readonly NativeIngressOrderFact[],
): SeedCheckVerdict {
  const seedId = "M3";
  if (facts.length === 0) {
    return bad(seedId, "no native-ingress ordering evidence present");
  }
  for (const f of facts) {
    if (f.pureQuestion && f.coordinationEffectCount !== 0) {
      return bad(
        seedId,
        `turn ${f.turnRef} pure question produced ${f.coordinationEffectCount} coordination effect(s) (UNEXPECTED_COORDINATION_EFFECT)`,
      );
    }
    if (f.coordinationEffectCount > 0) {
      if (!f.modelVisiblePredecessorCommitted) {
        return bad(
          seedId,
          `turn ${f.turnRef} created a task with no committed model-visible reply predecessor (MODEL_VISIBLE_PREDECESSOR_REQUIRED)`,
        );
      }
      if (!f.replyBeforeCoordination) {
        return bad(
          seedId,
          `turn ${f.turnRef} coordination preceded its reply (native-first ordering violated)`,
        );
      }
    }
    if (f.secondCoordinationAttempted && !f.secondCoordinationRejected) {
      return bad(
        seedId,
        `turn ${f.turnRef} second coordination call not rejected (SECOND_COORDINATION_CALL)`,
      );
    }
  }
  return ok(
    seedId,
    `all ${facts.length} native ingress turn(s): pure-question zero-task, reply-before-coordination, single coordination`,
  );
}

// ---------------------------------------------------------------------------
// M5 — failed native write advances boundary / replay re-renders (G1.5). This
// control consumes the EXACT eight core G1.5 generation-fence rows and asserts
// each produced its precise expected outcome with zero sibling effect: the
// conjunctive membership/route/generation fences before consume, not_written ->
// generation-two authorization, historical generation-one proof replay,
// DISTINCT stale-generation vs terminal-conflict errors (never "either"), and
// the second-crash {gen1: not_written, gen2: ambiguous} / no gen3 state. A
// boundary advanced on a failed write, a replay that re-renders instead of
// replaying historically, a conflated 5b error, or any non-zero sibling effect
// FAILs. The oracle is authoritative: it does not bend to a builder outcome.
// ---------------------------------------------------------------------------

/**
 * The exact eight core G1.5 generation-semantics fixture ids (the "eight rows
 * with the two 5b codes split") this control binds. Storage-integrity G1.5 rows
 * (PG constraints, journal, reconcile, ack-predecessor) are Lane A repository
 * concerns, not M5's boundary/generation oracle.
 */
export const M5_CORE_G15_ROW_IDS: readonly string[] = [
  "g1.5-fresh-generation-revoked-membership",
  "g1.5-fresh-generation-superseded-route",
  "g1.5-stale-generation-current-authority",
  "g1.5-not-written-generation-two",
  "g1.5-generation-one-proof-replay",
  "g1.5-post-generation-two-stale-resume",
  "g1.5-post-generation-two-terminal-conflict",
  "g1.5-second-crash-generation-two",
];

/** A row's contractually-expected fence outcome: an error XOR a state. */
export type FenceOutcome =
  | { readonly error: string }
  | { readonly state: string };

export type GenerationFenceObservation = {
  /** The G1.5 fixture row id this observation binds. */
  readonly fixtureRowId: string;
  /** The row's contractually expected outcome (from the G1.5 fixture). */
  readonly expected: FenceOutcome;
  /** The outcome the promoted generation fence actually produced. */
  readonly observed: FenceOutcome;
  /**
   * True iff the row's sibling tables were left unchanged (a failed write did
   * not advance the visible boundary and a proof replay did not re-render).
   */
  readonly siblingsUnchanged: boolean;
};

function sameOutcome(a: FenceOutcome, b: FenceOutcome): boolean {
  if ("error" in a) return "error" in b && a.error === b.error;
  return "state" in b && a.state === b.state;
}

function outcomeText(o: FenceOutcome): string {
  return "error" in o ? `error:${o.error}` : `state:${o.state}`;
}

export function checkGenerationFenceBoundary(
  facts: readonly GenerationFenceObservation[],
): SeedCheckVerdict {
  const seedId = "M5";
  if (facts.length === 0) {
    return bad(seedId, "no generation-fence evidence present");
  }
  // Completeness: every one of the eight core rows must be observed, so a
  // missing row cannot vacuously pass.
  const seen = new Set(facts.map((f) => f.fixtureRowId));
  for (const rowId of M5_CORE_G15_ROW_IDS) {
    if (!seen.has(rowId)) {
      return bad(seedId, `core G1.5 row ${rowId} has no fence observation (incomplete)`);
    }
  }
  for (const f of facts) {
    if (!f.siblingsUnchanged) {
      return bad(
        seedId,
        `${f.fixtureRowId}: fence had a non-zero sibling effect (failed write advanced the boundary or replay re-rendered)`,
      );
    }
    if (!sameOutcome(f.expected, f.observed)) {
      return bad(
        seedId,
        `${f.fixtureRowId}: expected ${outcomeText(f.expected)} but fence produced ${outcomeText(f.observed)} (conflated/bent outcome)`,
      );
    }
  }
  return ok(
    seedId,
    `all ${facts.length} G1.5 generation-fence rows produced their exact expected outcome with zero sibling effect`,
  );
}

// ---------------------------------------------------------------------------
// M7 — body/full context injected at a pre-turn seam despite notice-first. This
// control reaches BOTH G1.2 (attention audience suppression: attention notices
// at the seam must be metadata-only, never carrying body) AND G1.6 (pre-turn
// privacy: the current body is injected EXACTLY ONCE at the authorized compile
// seam, never smuggled through attention or injected elsewhere). An attention
// notice carrying a body (G1.2 PRIVATE_BODY_EXPOSURE), a body injected outside
// the authorized seam, or a body injection count != 1 (G1.6 exactly-once)
// FAILs.
// ---------------------------------------------------------------------------

export type PreTurnContextFact = {
  /** Opaque ref for the pre-turn compile. */
  readonly turnRef: string;
  /** True iff any attention notice carried body/full context at the seam. */
  readonly attentionCarriesBody: boolean;
  /** True iff the body was injected only at the authorized compile seam. */
  readonly bodyOnlyAtAuthorizedSeam: boolean;
  /** Count of body injections at the pre-turn seam (authorized: exactly one). */
  readonly bodyInjectionCount: number;
};

export function checkPreTurnContextInjection(
  facts: readonly PreTurnContextFact[],
): SeedCheckVerdict {
  const seedId = "M7";
  if (facts.length === 0) {
    return bad(seedId, "no pre-turn context evidence present");
  }
  for (const f of facts) {
    if (f.attentionCarriesBody) {
      return bad(
        seedId,
        `turn ${f.turnRef} attention notice carried body at the pre-turn seam (PRIVATE_BODY_EXPOSURE)`,
      );
    }
    if (!f.bodyOnlyAtAuthorizedSeam) {
      return bad(
        seedId,
        `turn ${f.turnRef} body injected outside the authorized notice-first compile seam`,
      );
    }
    if (f.bodyInjectionCount !== 1) {
      return bad(
        seedId,
        `turn ${f.turnRef} body injected ${f.bodyInjectionCount} times pre-turn (expected exactly one authorized injection)`,
      );
    }
  }
  return ok(
    seedId,
    `all ${facts.length} pre-turn compile(s): attention metadata-only, body injected once at the authorized seam`,
  );
}

// ---------------------------------------------------------------------------
// M8 — manifest mutated after freeze before the turn consumes it (G1.6). The
// standing manifest must be deep-frozen at composition and the digest the turn
// consumes must equal the frozen digest. A non-frozen manifest, or a
// consumed-digest that differs from the frozen digest, FAILs
// (MANIFEST_DIGEST_MISMATCH).
// ---------------------------------------------------------------------------

/** Lowercase-hex or `sha256:`-prefixed digest shape (neutral hash evidence). */
const MANIFEST_DIGEST_SHAPE = /^(?:sha256:)?[0-9a-f]{64}$/u;

export type ManifestFreezeFact = {
  /** Opaque ref for the standing manifest. */
  readonly manifestRef: string;
  /** True iff the composed manifest was deep-frozen. */
  readonly frozen: boolean;
  /** The digest computed at freeze time. */
  readonly frozenDigest: string;
  /** The digest observed when the turn consumed the manifest. */
  readonly consumedDigest: string;
};

export function checkManifestFreezeIntegrity(
  facts: readonly ManifestFreezeFact[],
): SeedCheckVerdict {
  const seedId = "M8";
  if (facts.length === 0) {
    return bad(seedId, "no manifest-freeze evidence present");
  }
  for (const f of facts) {
    if (!f.frozen) {
      return bad(seedId, `manifest ${f.manifestRef} was not deep-frozen at composition`);
    }
    if (!MANIFEST_DIGEST_SHAPE.test(f.frozenDigest)) {
      return bad(seedId, `manifest ${f.manifestRef} frozen digest is not a SHA-256 digest`);
    }
    if (f.frozenDigest !== f.consumedDigest) {
      return bad(
        seedId,
        `manifest ${f.manifestRef} mutated after freeze: consumed digest differs from frozen digest (MANIFEST_DIGEST_MISMATCH)`,
      );
    }
  }
  return ok(
    seedId,
    `all ${facts.length} manifest(s) deep-frozen with a stable digest through turn consume`,
  );
}
