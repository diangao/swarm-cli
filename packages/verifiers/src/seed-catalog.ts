import { S17_CONDITION_ID } from "@swarm/security";

// The FULL frozen Wave-0 seeded-control catalog: 25 seeds (S1-S17 + M1-M8),
// accounted for as a SEPARATE dimension from the 8+7 chat-contract conditions.
// This is the catalog-accounting surface: readiness accounting fixes the frozen
// accounting to 18 WAVE-0-IMPLEMENTED + 7 PLACEHOLDER = 25.
//
// A seed is `implemented` (a real control test drives the seeded defect and the
// verifier detects it) or `placeholder` (a named Wave-1 entry condition; never
// affects advisory). After the Wave-1 §9 step-5 binding, S8/S12/M3/M5/M7/M8 are
// bound to the promoted Lane A/B seams and are now implemented; the ONE
// remaining placeholder is EXACTLY {M2} (lane-role fidelity, held to Wave 3);
// the other 24 are implemented.

export type SeedStatus = "implemented" | "placeholder";

export type SeedCatalogEntry = {
  readonly id: string;
  readonly status: SeedStatus;
  /** One-line defect the seed injects. */
  readonly defect: string;
  /**
   * For an implemented seed: the oracle condition/seed id its control asserts
   * FAILs. For a placeholder: the named Wave-1 entry condition that must land
   * before it can bind.
   */
  readonly entry: string;
};

/**
 * The remaining placeholder ids, frozen by accounting. After the Wave-1 step-5
 * binding this is EXACTLY {M2}: lane-role fidelity stays held to Wave 3.
 */
export const PLACEHOLDER_SEED_IDS: readonly string[] = ["M2"];

export const SEED_CATALOG: readonly SeedCatalogEntry[] = [
  // --- WAVE-0-IMPLEMENTED (18) ---
  { id: "S1", status: "implemented", defect: "planner prompt missing strict acceptance string-array contract", entry: "plan-shape gate: checkPlanAcceptanceArray FAILs" },
  { id: "S2", status: "implemented", defect: "object-shaped acceptance committed", entry: "plan-shape gate: checkPlanAcceptanceNotObject FAILs" },
  { id: "S3", status: "implemented", defect: "lease expires under live turn; owner self-reclaims", entry: "lease_renewal_during_turn: checkLeaseRenewal FAILs" },
  { id: "S4", status: "implemented", defect: "stale wake + running-turn row survive restart", entry: "startup_state_reconciliation: checkStartupReconciliation FAILs" },
  { id: "S5", status: "implemented", defect: "empty body enters outbound/receipt set", entry: "exact_once_receipts: checkNoEmptyBody FAILs" },
  { id: "S6", status: "implemented", defect: "non-owner reads body / runs full turn", entry: "owner_only_body_read (chat) FAILs" },
  { id: "S7", status: "implemented", defect: "checklist-authored decomposition (stub plan)", entry: "decomposed_by_runtime (chat) FAILs" },
  { id: "S9", status: "implemented", defect: "completion claimed via prose without evidence", entry: "typed_turn_verdict: checkTypedVerdict FAILs" },
  { id: "S10", status: "implemented", defect: "budget exhaustion silently converts to complete", entry: "budget_exhaustion_explicit_hold: checkBudgetHold FAILs" },
  { id: "S11", status: "implemented", defect: "fresh session started where resume possible", entry: "resume_same_session_provenance: checkResumeProvenance FAILs" },
  { id: "S13", status: "implemented", defect: "duplicate root wake creates second graph", entry: "graph_replay_idempotency: checkGraphReplayIdempotency FAILs" },
  { id: "S14", status: "implemented", defect: "phase-1 claimable before phase-0 terminal", entry: "phase_gating: checkPhaseGating FAILs" },
  { id: "S15", status: "implemented", defect: "checkpoint contains body/CoT text", entry: "checkpoint_durability_and_scope: checkCheckpointPrivacy FAILs" },
  { id: "S16", status: "implemented", defect: "shape-changing steer silently mutates graph", entry: "steer_honored_precommit (chat) FAILs" },
  { id: "S17", status: "implemented", defect: "internal actor/lineage string in public fixture", entry: `publication scanner mutual-bind (${S17_CONDITION_ID})` },
  { id: "M1", status: "implemented", defect: "registry write inside a live-run freeze window", entry: "freeze-window audit: checkFreezeWindow FAILs" },
  { id: "M4", status: "implemented", defect: "wrong-turn / unsafe-boundary steer", entry: "steer_safety: checkSteerSafety FAILs" },
  { id: "M6", status: "implemented", defect: "two concurrent wakes cause a double spawn", entry: "single_owner_no_duplicate (chat) FAILs" },

  // --- WAVE-1-IMPLEMENTED (6, bound at §9 step 5 to the promoted seams) ---
  { id: "S8", status: "implemented", defect: "late old-attempt finalize after takeover", entry: "stale_attempt_fence: checkStaleAttemptFence FAILs (G1.8 STALE_FENCE)" },
  { id: "S12", status: "implemented", defect: "whole-row registry write clobbers presence/capabilities", entry: "field_scoped_registry_write: checkFieldScopedRegistryWrite FAILs (G1.7 UNDECLARED_FIELD_WRITE)" },
  { id: "M3", status: "implemented", defect: "native-first ingress ordering violated", entry: "native_ingress_ordering: checkNativeIngressOrdering FAILs (G1.3/G1.4 UNEXPECTED_COORDINATION_EFFECT / MODEL_VISIBLE_PREDECESSOR_REQUIRED)" },
  { id: "M5", status: "implemented", defect: "failed native write still advances the visible boundary / replay re-renders", entry: "generation_fence_boundary: checkGenerationFenceBoundary FAILs (G1.5 eight-row generation semantics)" },
  { id: "M7", status: "implemented", defect: "body/full context injected at a pre-turn seam despite notice-first", entry: "pre_turn_context_injection: checkPreTurnContextInjection FAILs (G1.2 audience suppression + G1.6 pre-turn privacy / current-body exactly-once)" },
  { id: "M8", status: "implemented", defect: "manifest mutated after freeze before the turn consumes it", entry: "manifest_freeze_integrity: checkManifestFreezeIntegrity FAILs (G1.6 MANIFEST_DIGEST_MISMATCH)" },

  // --- PLACEHOLDER (1, held to Wave 3) ---
  { id: "M2", status: "placeholder", defect: "verify/receipt lane re-creates the owner artifact instead of referencing path+hash", entry: "binds in Wave 3 when the execute-owner artifact path+hash field lands in the task/receipt row (lane-role fidelity)" },
];

/** All implemented seed ids. */
export const IMPLEMENTED_SEED_IDS: readonly string[] = SEED_CATALOG.filter(
  (s) => s.status === "implemented",
).map((s) => s.id);

/** Look up a seed catalog entry by id. */
export function seedEntry(id: string): SeedCatalogEntry | undefined {
  return SEED_CATALOG.find((s) => s.id === id);
}
