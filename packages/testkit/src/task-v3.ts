import {
  parseTaskLeaseV3,
  type AgentId,
  type ArtifactConsumptionId,
  type ArtifactContractId,
  type ArtifactContractTemplateId,
  type ArtifactId,
  type ArtifactMaterializationGrantId,
  type ArtifactObjectId,
  type ClaimId,
  type FenceToken,
  type LeaseId,
  type ProtocolVersion,
  type RepositoryId,
  type ReviewAssignmentId,
  type ServerId,
  type TaskId,
  type TaskLeaseV3,
  type Timestamp,
  type WorkspaceContractId,
  type WorkspaceReservationId,
} from "@swarm/protocol";

export const GATE3_SCENARIO_IDS = [
  "g3_1_atomic_claim_contention",
  "g3_2_lease_renewal_and_expiry",
  "g3_3_guarded_mutations_and_owner_loss",
  "g3_4_startup_reconciliation",
  "g3_5_atomic_graph_proposal",
  "g3_6_cycles_dependencies_and_ready_leaves",
  "g3_7_capability_and_bounds",
  "g3_8_workspace_and_path_ownership",
  "g3_9_immutable_artifact_publication",
  "g3_10_exact_artifact_review_barrier",
  "g3_11_reviewed_integration",
  "g3_12_wave_carry_and_deferred_truth",
] as const;
export type Gate3ScenarioId = (typeof GATE3_SCENARIO_IDS)[number];

export const GATE3_FACT_KINDS = [
  "artifact_published", "artifact_validated", "blocked_closure_derived_from_exact_stored_topology",
  "carry_gate", "claim_eligibility_computed", "claim_won", "integration_candidate_frozen",
  "lease_lost", "lease_renewed", "native_reply_then_coordination", "native_turn_running",
  "path_claim_reserved", "real_pg_interleavings_reached_every_latch", "reconciliation_decision",
  "reconciliation_started", "reply_committed", "review_barrier_satisfied", "review_requested",
  "review_verdict", "runtime_interrupt_requested", "scenario_disposition", "task_graph_created",
  "task_graph_validated", "task_status_changed", "typed_transitive_block_closure_committed",
  "workspace_contract_frozen",
] as const;
export type Gate3FactKind = (typeof GATE3_FACT_KINDS)[number];

export type Gate3Fact = Readonly<{
  scenarioId: Gate3ScenarioId;
  kind: Gate3FactKind;
  ordinal: number;
  at: Timestamp;
  identifiers: Readonly<Record<string, string | number | boolean | null>>;
}>;

export class Gate3FactRecorder {
  readonly #facts: Gate3Fact[] = [];
  #ordinal = 0;

  record(input: Omit<Gate3Fact, "ordinal">): Gate3Fact {
    if (!GATE3_SCENARIO_IDS.includes(input.scenarioId) || !GATE3_FACT_KINDS.includes(input.kind)) {
      throw new Error("UNKNOWN_GATE3_FACT");
    }
    const fact = Object.freeze({ ...input, ordinal: ++this.#ordinal });
    this.#facts.push(fact);
    return fact;
  }

  snapshot(): readonly Gate3Fact[] {
    return Object.freeze(this.#facts.map((fact) => Object.freeze({ ...fact })));
  }

  count(kind: Gate3FactKind): number {
    return this.#facts.filter((fact) => fact.kind === kind).length;
  }
}

const CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz";
let ordinal = 0;
function body(seed: number): string {
  let value = seed >>> 0;
  let result = "";
  for (let index = 0; index < 26; index += 1) {
    value = (value * 1664525 + 1013904223 + index) >>> 0;
    result += CROCKFORD[value & 31];
  }
  return result;
}
function mint<Id extends string>(prefix: string): Id { return `${prefix}_${body(++ordinal)}` as Id; }
export function resetTaskV3Ids(): void { ordinal = 0; }

export const mintRepositoryId = (): RepositoryId => mint("rpo");
export const mintWorkspaceContractId = (): WorkspaceContractId => mint("wsc");
export const mintWorkspaceReservationId = (): WorkspaceReservationId => mint("rsv");
export const mintArtifactContractTemplateId = (): ArtifactContractTemplateId => mint("act");
export const mintArtifactContractId = (): ArtifactContractId => mint("acc");
export const mintArtifactId = (): ArtifactId => mint("art");
export const mintArtifactObjectId = (): ArtifactObjectId => mint("aob");
export const mintArtifactConsumptionId = (): ArtifactConsumptionId => mint("acm");
export const mintArtifactMaterializationGrantId = (): ArtifactMaterializationGrantId => mint("amg");
export const mintReviewAssignmentId = (): ReviewAssignmentId => mint("ras");

export function createTaskLeaseV3Fixture(overrides: Partial<TaskLeaseV3> = {}): TaskLeaseV3 {
  const acquiredAt = "2026-08-11T16:00:00.000Z" as Timestamp;
  return parseTaskLeaseV3({
    protocolVersion: 1 as ProtocolVersion,
    serverId: mint<ServerId>("srv"),
    rootTaskId: mint<TaskId>("tsk"),
    taskId: mint<TaskId>("tsk"),
    claimId: mint<ClaimId>("clm"),
    leaseId: mint<LeaseId>("lse"),
    ownerAgentId: mint<AgentId>("agt"),
    attempt: 1,
    leaseEpoch: 1,
    leaseRevision: 1,
    fenceToken: mint<FenceToken>("fnc"),
    acquiredAt,
    expiresAt: "2026-08-11T16:00:30.000Z" as Timestamp,
    taskRowVersion: 2,
    graphRevision: 0,
    ...overrides,
  });
}
