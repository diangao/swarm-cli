import { S17_CONDITION_ID } from "@swarm/security";

// The FULL frozen Wave-0 seeded-control catalog: 25 seeds (S1-S17 + M1-M8),
// accounted for as a SEPARATE dimension from the 8+7 chat-contract conditions.
// This is the catalog-accounting surface: readiness accounting fixes the frozen
// accounting to 18 WAVE-0-IMPLEMENTED + 7 PLACEHOLDER = 25.
//
// A seed is `implemented` (a real control test drives the seeded defect and the
// verifier detects it) or `placeholder` (a named Wave-1 entry condition; never
// affects advisory). The 7 placeholders are EXACTLY {S8, S12, M2, M3, M5, M7,
// M8}; the other 18 are implemented.

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

/** The exactly-seven placeholder ids, frozen by accounting. */
export const PLACEHOLDER_SEED_IDS: readonly string[] = [
  "S8",
  "S12",
  "M2",
  "M3",
  "M5",
  "M7",
  "M8",
];

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

  // --- PLACEHOLDER (7) ---
  { id: "S8", status: "placeholder", defect: "late old-attempt finalize after takeover", entry: "binds when fence/attempt fields land in the storage skeleton" },
  { id: "S12", status: "placeholder", defect: "whole-row registry write clobbers presence/capabilities", entry: "binds when the registry table lands with a field-scoped write path (Wave 1)" },
  { id: "M2", status: "placeholder", defect: "verify/receipt lane re-creates the owner artifact instead of referencing path+hash", entry: "binds when the execute-owner artifact path+hash field lands in the task/receipt row (Wave 1)" },
  { id: "M3", status: "placeholder", defect: "native-first ingress ordering violated", entry: "binds when task-creation vs native-reply ordering is observable in the ledger (Wave 1 daemon delivery)" },
  { id: "M5", status: "placeholder", defect: "failed native write still advances the visible boundary / replay re-renders", entry: "binds when write-result + boundary-advance events land in the storage skeleton" },
  { id: "M7", status: "placeholder", defect: "body/full context injected at a pre-turn seam despite notice-first", entry: "binds when the context-compiler seam name lands (Wave 1)" },
  { id: "M8", status: "placeholder", defect: "manifest mutated after freeze before the turn consumes it", entry: "binds when the manifest freeze/hash from the prompt composer lands (Wave 1)" },
];

/** All implemented seed ids. */
export const IMPLEMENTED_SEED_IDS: readonly string[] = SEED_CATALOG.filter(
  (s) => s.status === "implemented",
).map((s) => s.id);

/** Look up a seed catalog entry by id. */
export function seedEntry(id: string): SeedCatalogEntry | undefined {
  return SEED_CATALOG.find((s) => s.id === id);
}
