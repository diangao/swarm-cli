import { canonicalProtocolJson, parseProtocolJson } from "./json.js";
import { fail } from "./errors.js";
import type {
  AgentId,
  ArtifactDigest,
  ClaimId,
  CommandId,
  FenceToken,
  LeaseId,
  MessageId,
  ProducerFactId,
  ProtocolVersion,
  ServerId,
  TaskId,
  TurnId,
} from "./types.js";

declare const taskV3Brand: unique symbol;
export type TaskV3Brand<Value, Name extends string> = Value & {
  readonly [taskV3Brand]: Name;
};

export type RepositoryId = TaskV3Brand<string, "RepositoryId">;
export type WorkspaceContractId = TaskV3Brand<string, "WorkspaceContractId">;
export type WorkspaceReservationId = TaskV3Brand<
  string,
  "WorkspaceReservationId"
>;
export type ArtifactContractTemplateId = TaskV3Brand<
  string,
  "ArtifactContractTemplateId"
>;
export type ArtifactContractId = TaskV3Brand<string, "ArtifactContractId">;
export type ArtifactId = TaskV3Brand<string, "ArtifactId">;
export type ArtifactObjectId = TaskV3Brand<string, "ArtifactObjectId">;
export type ArtifactConsumptionId = TaskV3Brand<
  string,
  "ArtifactConsumptionId"
>;
export type ArtifactMaterializationGrantId = TaskV3Brand<
  string,
  "ArtifactMaterializationGrantId"
>;
export type ReviewAssignmentId = TaskV3Brand<string, "ReviewAssignmentId">;
export type CommitSha = TaskV3Brand<string, "CommitSha">;
export type TreeSha = TaskV3Brand<string, "TreeSha">;
export type CapabilityKey = TaskV3Brand<string, "CapabilityKey">;
export type ReviewSeatKey = TaskV3Brand<string, "ReviewSeatKey">;
export type LaunchCapabilityRef = TaskV3Brand<string, "LaunchCapabilityRef">;
export type Timestamp = TaskV3Brand<string, "Timestamp">;

export type TaskStatus =
  | "todo"
  | "in_progress"
  | "waiting"
  | "in_review"
  | "done"
  | "blocked";
export type TaskEdgeKind = "contains" | "depends_on";
export type TaskLaneRole =
  | "server"
  | "daemon"
  | "driver"
  | "storage"
  | "protocol"
  | "verifier"
  | "security"
  | "review"
  | "integration";

export const TASK_V3_LIMITS = Object.freeze({
  leaseDurationMs: 30_000,
  renewalLeadMs: 10_000,
  maxProposalChildren: 8,
  maxGraphNodes: 32,
  maxGraphEdges: 64,
  maxGraphDepth: 4,
  maxOpenLeaves: 8,
  maxDependenciesPerTask: 8,
  maxReviewSeats: 4,
  maxCapabilityKeys: 16,
  maxPathClaims: 32,
  maxTitleUtf8Bytes: 240,
  titleSourceReuseMinUtf8Bytes: 32,
} as const);

export type TaskLeaseV3 = {
  protocolVersion: ProtocolVersion;
  serverId: ServerId;
  rootTaskId: TaskId;
  taskId: TaskId;
  claimId: ClaimId;
  leaseId: LeaseId;
  ownerAgentId: AgentId;
  attempt: number;
  leaseEpoch: number;
  leaseRevision: number;
  fenceToken: FenceToken;
  acquiredAt: Timestamp;
  expiresAt: Timestamp;
  taskRowVersion: number;
  graphRevision: number;
};
export type ClaimTaskInputV3 = {
  commandId: CommandId;
  taskId: TaskId;
  agentId: AgentId;
  expectedTaskRowVersion: number;
  expectedGraphRevision: number;
  expectedWorkspaceGeneration: number;
};
export type ClaimTaskResultV3 =
  | { kind: "claimed"; lease: TaskLeaseV3 }
  | {
      kind: "conflict";
      code:
        | "TASK_CLAIM_CONFLICT"
        | "TASK_NOT_READY"
        | "TASK_CAPABILITY_MISMATCH";
      observedTaskRowVersion: number;
      observedLeaseEpoch: number | null;
    };
export type RenewTaskLeaseInputV3 = {
  commandId: CommandId;
  expectedLease: TaskLeaseV3;
};
export type RenewTaskLeaseResultV3 =
  | {
      kind: "renewed";
      previousLease: TaskLeaseV3;
      currentLease: TaskLeaseV3;
      serverObservedAt: Timestamp;
    }
  | {
      kind: "rejected";
      code:
        | "TASK_LEASE_RENEWAL_TOO_EARLY"
        | "TASK_LEASE_RENEWAL_CONFLICT"
        | "TASK_LEASE_EXPIRED";
      serverObservedAt: Timestamp;
      retryNotBefore: Timestamp | null;
      observedLeaseRevision: number | null;
    };
export type TaskLeaseCloseReasonV3 =
  | "work_yielded"
  | "graph_expanded"
  | "graph_blocked"
  | "artifact_published"
  | "owner_stopping"
  | "reconciliation_relinquish"
  | "review_barrier_blocked"
  | "server_expired";
export type VoluntaryTaskLeaseCloseReasonV3 =
  | "work_yielded"
  | "owner_stopping"
  | "reconciliation_relinquish";
export type ReleaseTaskLeaseInputV3 = {
  commandId: CommandId;
  expectedLease: TaskLeaseV3;
  reason: VoluntaryTaskLeaseCloseReasonV3;
};
export type ReleaseTaskLeaseResultV3 = {
  kind: "released";
  closedLease: TaskLeaseV3;
  reason: VoluntaryTaskLeaseCloseReasonV3;
  previousTaskStatus: "in_progress";
  currentTaskStatus: "todo";
  currentTaskRowVersion: number;
};
export type TaskMutationFenceV3 = {
  commandId: CommandId;
  expectedLease: TaskLeaseV3;
  expectedTaskStatus: "in_progress";
  expectedTaskRowVersion: number;
};
export type BlockTaskInputV3 = TaskMutationFenceV3 & {
  reason: "execution_failed";
};
export type BlockedTaskTransitionV3 = {
  taskId: TaskId;
  previousStatus: "todo" | "in_progress" | "waiting";
  currentStatus: "blocked";
  previousTaskRowVersion: number;
  currentTaskRowVersion: number;
  closedClaimId: ClaimId | null;
  closedLeaseId: LeaseId | null;
  leaseCloseReason: "graph_blocked" | null;
  closedReservationIds: WorkspaceReservationId[];
};
export type BlockTaskResultV3 = {
  kind: "blocked";
  rootCauseTaskId: TaskId;
  previousGraphRevision: number;
  currentGraphRevision: number;
  affected: BlockedTaskTransitionV3[];
};

export type RepositoryPathClaimV3 = { kind: "file" | "subtree"; path: string };
export type ProposedWorkspaceV3 = {
  repositoryId: RepositoryId;
  baseCommit: CommitSha;
  baseTree: TreeSha;
  workspaceGeneration: number;
  executionMode: "isolated_worktree" | "read_only_artifact";
  pathClaims: RepositoryPathClaimV3[];
  readArtifactDigest: ArtifactDigest | null;
  integrationOwnerTaskId: TaskId;
  artifactContractTemplateDigest: ArtifactDigest;
  policyDigest: ArtifactDigest;
};
export type WorkspaceContractV3 = ProposedWorkspaceV3 & {
  protocolVersion: ProtocolVersion;
  workspaceContractId: WorkspaceContractId;
  taskId: TaskId;
  rootTaskId: TaskId;
};
export type ProposedChildV3 = {
  clientKey: string;
  title: string;
  titleDigest: ArtifactDigest;
  titleClassifierPolicyDigest: ArtifactDigest;
  laneRole: TaskLaneRole;
  requiredCapabilities: CapabilityKey[];
  workspace: ProposedWorkspaceV3;
};
export type ValidateTaskTitleInputV3 = {
  sourceMessageId: MessageId;
  sourceProducerFactId: ProducerFactId;
  title: string;
  titleDigest: ArtifactDigest;
};
export type ValidateTaskTitleResultV3 = {
  titleDigest: ArtifactDigest;
  classifierPolicyDigest: ArtifactDigest;
};
export interface TaskTitlePrivacyPortV3 {
  validateTaskTitle(
    input: ValidateTaskTitleInputV3,
  ): Promise<ValidateTaskTitleResultV3>;
}
export type ProposedDependencyEndpointV3 =
  | { kind: "child"; clientKey: string }
  | { kind: "task"; taskId: TaskId };
export type ProposedDependencyV3 = {
  prerequisite: ProposedDependencyEndpointV3;
  dependent: ProposedDependencyEndpointV3;
};
export type ProposeTaskGraphInputV3 = TaskMutationFenceV3 & {
  rootTaskId: TaskId;
  sourceMessageId: MessageId;
  sourceTurnId: TurnId;
  sourceProducerFactId: ProducerFactId;
  committedReplyMessageId: MessageId;
  expectedGraphRevision: number;
  expectedPolicyDigest: ArtifactDigest;
  expectedTitleClassifierPolicyDigest: ArtifactDigest;
  children: ProposedChildV3[];
  dependencies: ProposedDependencyV3[];
};
export type ProposeTaskGraphResultV3 = {
  rootTaskId: TaskId;
  previousGraphRevision: number;
  currentGraphRevision: number;
  policyDigest: ArtifactDigest;
  children: Array<{
    clientKey: string;
    taskId: TaskId;
    taskNumber: number;
    status: "todo";
    titleDigest: ArtifactDigest;
    titleClassifierPolicyDigest: ArtifactDigest;
  }>;
  proposerStatus: "waiting";
};

export type ArtifactContractTemplateV3 = {
  protocolVersion: ProtocolVersion;
  templateId: ArtifactContractTemplateId;
  templateRevision: number;
  allowedKinds: Array<"git_commit" | "digest_blob">;
  maxMaterialBytes: number;
  scopePolicy: "exact_workspace_claims";
  receiptPolicyDigest: ArtifactDigest;
  requiredReviewSeats: ReviewSeatKey[];
  policyDigest: ArtifactDigest;
};
export type MaterializedArtifactContractV3 = {
  protocolVersion: ProtocolVersion;
  artifactContractId: ArtifactContractId;
  templateDigest: ArtifactDigest;
  serverId: ServerId;
  rootTaskId: TaskId;
  taskId: TaskId;
  workspaceContractId: WorkspaceContractId;
  workspaceContractDigest: ArtifactDigest;
  baseCommit: CommitSha;
  baseTree: TreeSha;
  pathClaimsDigest: ArtifactDigest;
  integrationOwnerTaskId: TaskId;
  allowedKinds: Array<"git_commit" | "digest_blob">;
  maxMaterialBytes: number;
  scopePolicy: "exact_workspace_claims";
  receiptPolicyDigest: ArtifactDigest;
  requiredReviewSeats: ReviewSeatKey[];
  gate3PlanDigest: ArtifactDigest;
  policyDigest: ArtifactDigest;
};
export type ArtifactMaterialRoleV3 =
  | "artifact"
  | "scope_manifest"
  | "acceptance_receipt";
export type StagedArtifactMaterialV3 = {
  stagedObjectId: ArtifactObjectId;
  role: ArtifactMaterialRoleV3;
  kind: "prerequisite_bound_git_bundle" | "digest_blob" | "canonical_json";
  mediaType: string;
  byteLength: number;
  sha256: ArtifactDigest;
  prerequisiteCommit: CommitSha | null;
  expiresAt: Timestamp;
};
export type StageArtifactMaterialInputV3 = {
  commandId: CommandId;
  expectedLease: TaskLeaseV3;
  artifactContractDigest: ArtifactDigest;
  role: ArtifactMaterialRoleV3;
  sourceCapability: LaunchCapabilityRef;
};
export type ArtifactIdentityV3 =
  | {
      kind: "git_commit";
      commitSha: CommitSha;
      treeSha: TreeSha;
      orderedParents: CommitSha[];
    }
  | {
      kind: "digest_blob";
      mediaType: string;
      byteLength: number;
      sha256: ArtifactDigest;
    };
export type ArtifactClaimsV3 = {
  protocolVersion: ProtocolVersion;
  workspaceContractDigest: ArtifactDigest;
  artifactContractDigest: ArtifactDigest;
  gate3PlanDigest: ArtifactDigest;
  identity: ArtifactIdentityV3;
  artifactDigest: ArtifactDigest;
  scopeManifestDigest: ArtifactDigest;
  acceptanceReceiptDigest: ArtifactDigest;
};
export type ProposedArtifactV3 = ArtifactClaimsV3 & {
  stagedMaterial: StagedArtifactMaterialV3;
  stagedScopeManifest: StagedArtifactMaterialV3;
  stagedAcceptanceReceipt: StagedArtifactMaterialV3;
};
export type ArtifactDescriptorV3 = ArtifactClaimsV3 & {
  artifactId: ArtifactId;
  materialObjectId: ArtifactObjectId;
  materialKind: "prerequisite_bound_git_bundle" | "digest_blob";
  materialMediaType: string;
  materialByteLength: number;
  materialSha256: ArtifactDigest;
  materialPrerequisiteCommit: CommitSha | null;
  scopeManifestObjectId: ArtifactObjectId;
  scopeManifestSha256: ArtifactDigest;
  scopeManifestByteLength: number;
  acceptanceReceiptObjectId: ArtifactObjectId;
  acceptanceReceiptSha256: ArtifactDigest;
  acceptanceReceiptByteLength: number;
  consumedSourceArtifactIds: ArtifactId[];
  rootTaskId: TaskId;
  taskId: TaskId;
  attempt: number;
  builderAgentId: AgentId;
  contributorAgentIds: AgentId[];
};
export type TaskAttemptContributorV3 = {
  taskId: TaskId;
  attempt: number;
  agentId: AgentId;
  source: "claim_owner" | "accepted_upstream_artifact";
  sourceArtifactId: ArtifactId | null;
  recordedAt: Timestamp;
};
export type TaskAttemptContributionLedgerV3 = {
  taskId: TaskId;
  attempt: number;
  ledgerRevision: number;
  acceptedSourceArtifactIds: ArtifactId[];
  contributorAgentIds: AgentId[];
};
export type ConsumeAcceptedArtifactInputV3 = TaskMutationFenceV3 & {
  sourceArtifactId: ArtifactId;
  sourceArtifactDigest: ArtifactDigest;
  expectedSourceTaskRowVersion: number;
  expectedSourceBarrierRevision: number;
  expectedTargetGraphRevision: number;
  expectedContributorLedgerRevision: number;
};
export type ConsumeAcceptedArtifactResultV3 = {
  kind: "consumed";
  consumptionId: ArtifactConsumptionId;
  materializationGrantId: ArtifactMaterializationGrantId;
  sourceArtifactId: ArtifactId;
  sourceArtifactDigest: ArtifactDigest;
  previousContributorLedgerRevision: number;
  currentContributorLedgerRevision: number;
  acceptedSourceArtifactIds: ArtifactId[];
  contributorAgentIds: AgentId[];
};
export type OpenConsumedArtifactMaterialInputV3 = {
  expectedLease: TaskLeaseV3;
  consumptionId: ArtifactConsumptionId;
  materializationGrantId: ArtifactMaterializationGrantId;
  sourceArtifactId: ArtifactId;
  sourceArtifactDigest: ArtifactDigest;
};
export interface AcceptedArtifactMaterialPortV3 {
  openConsumedArtifactMaterial(
    input: OpenConsumedArtifactMaterialInputV3,
  ): Promise<ReadableStream<Uint8Array>>;
}
export type PublishArtifactInputV3 = TaskMutationFenceV3 & {
  artifact: ProposedArtifactV3;
};

export type ReviewRequirementV3 = {
  artifactId: ArtifactId;
  artifactDigest: ArtifactDigest;
  artifactContractDigest: ArtifactDigest;
  gate3PlanDigest: ArtifactDigest;
  scenarioVersion: number;
  seatKey: ReviewSeatKey;
};
export type ReviewAssignmentV3 = ReviewRequirementV3 & {
  assignmentId: ReviewAssignmentId;
  reviewTaskId: TaskId;
  reviewerAgentId: AgentId;
  reviewerRegistryRevision: number;
  assignmentRevision: number;
  assignmentStatus: "current" | "closed";
  closedReason: "reassigned" | "terminal_verdict" | "barrier_blocked" | null;
};
export type AssignReviewSeatInputV3 = {
  commandId: CommandId;
  requirement: ReviewRequirementV3;
  reviewTaskId: TaskId;
  expectedBarrierRevision: number;
  reviewerAgentId: AgentId;
  expectedRegistryRevision: number;
  expectedAssignmentRevision: number | null;
  expectedBarrierState: "open";
  expectedReviewTaskStatus: "todo" | "in_progress";
  expectedTerminalVerdictId: null;
};
export type AssignReviewSeatResultV3 = {
  assignment: ReviewAssignmentV3;
  previousAssignmentId: ReviewAssignmentId | null;
  currentBarrierRevision: number;
};
export type ReviewVerdictV3 = ReviewAssignmentV3 & {
  reviewAttempt: number;
  verdict: "GO" | "BLOCK";
  findingsDigest: ArtifactDigest;
  evidenceReceiptDigest: ArtifactDigest;
};
export type SubmitReviewVerdictInputV3 = TaskMutationFenceV3 & {
  verdict: ReviewVerdictV3;
};

const ID = /^[0-9a-hjkmnp-tv-z]{26}$/u;
const SHA256 = /^sha256:[0-9a-f]{64}$/u;
const SHA1 = /^[0-9a-f]{40}$/u;
const UTC =
  /^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}Z$/u;
const KEY = /^[a-z][a-z0-9._:-]{0,127}$/u;
const MAX_SAFE = Number.MAX_SAFE_INTEGER;

function input(value: unknown): unknown {
  if (value instanceof Uint8Array) return parseProtocolJson(value);
  return value;
}
function record(
  value: unknown,
  keys: readonly string[],
): Record<string, unknown> {
  value = input(value);
  if (value === null || typeof value !== "object" || Array.isArray(value))
    fail("INVALID_SCALAR");
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null)
    fail("INVALID_SCALAR");
  if (Object.getOwnPropertySymbols(value).length !== 0) fail("UNKNOWN_FIELD");
  const names = Object.getOwnPropertyNames(value);
  if (
    names.length !== keys.length ||
    names.some((name) => !keys.includes(name))
  )
    fail("UNKNOWN_FIELD");
  for (const key of keys) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (
      descriptor === undefined ||
      !descriptor.enumerable ||
      !("value" in descriptor)
    )
      fail("INVALID_SCALAR");
  }
  return value as Record<string, unknown>;
}
function oneOf<T extends string>(value: unknown, choices: readonly T[]): T {
  if (typeof value !== "string" || !choices.includes(value as T))
    fail("UNSUPPORTED_VARIANT");
  return value as T;
}
function text(value: unknown, maxBytes = 1024, allowEmpty = false): string {
  if (
    typeof value !== "string" ||
    (!allowEmpty && value.length === 0) ||
    value.includes("\0") ||
    new TextEncoder().encode(value).byteLength > maxBytes
  )
    fail("INVALID_SCALAR");
  return value;
}
function integer(value: unknown, min = 0, max = MAX_SAFE): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < min ||
    value > max
  )
    fail("INVALID_SCALAR");
  return value;
}
function nullable<T>(value: unknown, parse: (item: unknown) => T): T | null {
  return value === null ? null : parse(value);
}
function branded<T extends string>(value: unknown, prefix: string): T {
  if (
    typeof value !== "string" ||
    !value.startsWith(`${prefix}_`) ||
    !ID.test(value.slice(prefix.length + 1))
  )
    fail("INVALID_SCALAR");
  return value as T;
}
function digest(value: unknown): ArtifactDigest {
  if (typeof value !== "string" || !SHA256.test(value)) fail("INVALID_SCALAR");
  return value as ArtifactDigest;
}
function timestamp(value: unknown): Timestamp {
  if (
    typeof value !== "string" ||
    !UTC.test(value) ||
    new Date(value).toISOString() !== value
  )
    fail("INVALID_SCALAR");
  return value as Timestamp;
}
function sha<T extends string>(value: unknown): T {
  if (typeof value !== "string" || !SHA1.test(value)) fail("INVALID_SCALAR");
  return value as T;
}
function protocolVersion(value: unknown): ProtocolVersion {
  return integer(value, 1, 999_999) as ProtocolVersion;
}
function key<T extends string>(value: unknown): T {
  if (typeof value !== "string" || !KEY.test(value)) fail("INVALID_SCALAR");
  return value as T;
}
function array<T>(
  value: unknown,
  parse: (item: unknown) => T,
  max: number,
  options: {
    empty?: boolean;
    canonical?: (item: T) => string;
    ordered?: boolean;
  } = {},
): T[] {
  if (!Array.isArray(value)) fail("INVALID_SCALAR");
  if (
    Object.getPrototypeOf(value) !== Array.prototype ||
    Object.getOwnPropertySymbols(value).length !== 0 ||
    (!options.empty && value.length === 0) ||
    value.length > max ||
    Object.getOwnPropertyNames(value).some(
      (name) => name !== "length" && !/^(?:0|[1-9]\d*)$/u.test(name),
    )
  )
    fail("INVALID_SCALAR");
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (
      descriptor === undefined ||
      !descriptor.enumerable ||
      !("value" in descriptor)
    )
      fail("INVALID_SCALAR");
  }
  const parsed = value.map(parse);
  const tokens = parsed.map(
    (item) =>
      options.canonical?.(item) ??
      new TextDecoder().decode(canonicalProtocolJson(item)),
  );
  if (new Set(tokens).size !== tokens.length) fail("INVARIANT_VIOLATION");
  if (
    !options.ordered &&
    tokens.some(
      (token, index) => index > 0 && (tokens[index - 1] as string) >= token,
    )
  )
    fail("INVARIANT_VIOLATION");
  return parsed;
}
function expectDigestIdentity(
  value: unknown,
  identity: ArtifactIdentityV3,
): ArtifactDigest {
  const result = digest(value);
  const bytes = canonicalProtocolJson(identity);
  // Parsing verifies grammar. Storage's object validator is authoritative for the cryptographic comparison.
  if (bytes.byteLength === 0) fail("INVARIANT_VIOLATION");
  return result;
}

const id = {
  server: (v: unknown) => branded<ServerId>(v, "srv"),
  task: (v: unknown) => branded<TaskId>(v, "tsk"),
  claim: (v: unknown) => branded<ClaimId>(v, "clm"),
  lease: (v: unknown) => branded<LeaseId>(v, "lse"),
  agent: (v: unknown) => branded<AgentId>(v, "agt"),
  command: (v: unknown) => branded<CommandId>(v, "cmd"),
  message: (v: unknown) => branded<MessageId>(v, "msg"),
  producer: (v: unknown) => branded<ProducerFactId>(v, "fac"),
  turn: (v: unknown) => branded<TurnId>(v, "trn"),
  repository: (v: unknown) => branded<RepositoryId>(v, "rpo"),
  workspace: (v: unknown) => branded<WorkspaceContractId>(v, "wsc"),
  reservation: (v: unknown) => branded<WorkspaceReservationId>(v, "rsv"),
  template: (v: unknown) => branded<ArtifactContractTemplateId>(v, "act"),
  contract: (v: unknown) => branded<ArtifactContractId>(v, "acc"),
  artifact: (v: unknown) => branded<ArtifactId>(v, "art"),
  object: (v: unknown) => branded<ArtifactObjectId>(v, "aob"),
  consumption: (v: unknown) => branded<ArtifactConsumptionId>(v, "acm"),
  grant: (v: unknown) => branded<ArtifactMaterializationGrantId>(v, "amg"),
  assignment: (v: unknown) => branded<ReviewAssignmentId>(v, "ras"),
};

export function normalizeRepositoryPathV3(value: unknown): string {
  const path = text(value, 4096);
  if (path.startsWith("/") || /^[A-Za-z]:/u.test(path) || path.includes("\\"))
    fail("INVALID_SCALAR");
  const members = path.split("/");
  if (
    members.some((member) => member === "" || member === "." || member === "..")
  )
    fail("INVALID_SCALAR");
  if (members.join("/") !== path) fail("INVALID_SCALAR");
  return path;
}
export function repositoryPathClaimsOverlapV3(
  left: RepositoryPathClaimV3,
  right: RepositoryPathClaimV3,
): boolean {
  const contains = (
    owner: RepositoryPathClaimV3,
    candidate: RepositoryPathClaimV3,
  ) =>
    owner.path === candidate.path ||
    (owner.kind === "subtree" && candidate.path.startsWith(`${owner.path}/`));
  return contains(left, right) || contains(right, left);
}

export function parseTaskLeaseV3(value: unknown): TaskLeaseV3 {
  const v = record(value, [
    "protocolVersion",
    "serverId",
    "rootTaskId",
    "taskId",
    "claimId",
    "leaseId",
    "ownerAgentId",
    "attempt",
    "leaseEpoch",
    "leaseRevision",
    "fenceToken",
    "acquiredAt",
    "expiresAt",
    "taskRowVersion",
    "graphRevision",
  ]);
  const acquiredAt = timestamp(v.acquiredAt);
  const expiresAt = timestamp(v.expiresAt);
  if (expiresAt <= acquiredAt) fail("INVARIANT_VIOLATION");
  return {
    protocolVersion: protocolVersion(v.protocolVersion),
    serverId: id.server(v.serverId),
    rootTaskId: id.task(v.rootTaskId),
    taskId: id.task(v.taskId),
    claimId: id.claim(v.claimId),
    leaseId: id.lease(v.leaseId),
    ownerAgentId: id.agent(v.ownerAgentId),
    attempt: integer(v.attempt, 1, 2_147_483_647),
    leaseEpoch: integer(v.leaseEpoch, 1),
    leaseRevision: integer(v.leaseRevision, 1),
    fenceToken: branded<FenceToken>(v.fenceToken, "fnc"),
    acquiredAt,
    expiresAt,
    taskRowVersion: integer(v.taskRowVersion, 1),
    graphRevision: integer(v.graphRevision, 0),
  };
}
export function parseClaimTaskInputV3(value: unknown): ClaimTaskInputV3 {
  const v = record(value, [
    "commandId",
    "taskId",
    "agentId",
    "expectedTaskRowVersion",
    "expectedGraphRevision",
    "expectedWorkspaceGeneration",
  ]);
  return {
    commandId: id.command(v.commandId),
    taskId: id.task(v.taskId),
    agentId: id.agent(v.agentId),
    expectedTaskRowVersion: integer(v.expectedTaskRowVersion, 0),
    expectedGraphRevision: integer(v.expectedGraphRevision, 0),
    expectedWorkspaceGeneration: integer(v.expectedWorkspaceGeneration, 1),
  };
}
export function parseClaimTaskResultV3(value: unknown): ClaimTaskResultV3 {
  const b = record(
    value,
    Object.hasOwn(input(value) as object, "lease")
      ? ["kind", "lease"]
      : ["kind", "code", "observedTaskRowVersion", "observedLeaseEpoch"],
  );
  if (b.kind === "claimed")
    return { kind: "claimed", lease: parseTaskLeaseV3(b.lease) };
  return {
    kind: oneOf(b.kind, ["conflict"]),
    code: oneOf(b.code, [
      "TASK_CLAIM_CONFLICT",
      "TASK_NOT_READY",
      "TASK_CAPABILITY_MISMATCH",
    ]),
    observedTaskRowVersion: integer(b.observedTaskRowVersion, 0),
    observedLeaseEpoch: nullable(b.observedLeaseEpoch, (v) => integer(v, 1)),
  };
}
export function parseRenewTaskLeaseInputV3(
  value: unknown,
): RenewTaskLeaseInputV3 {
  const v = record(value, ["commandId", "expectedLease"]);
  return {
    commandId: id.command(v.commandId),
    expectedLease: parseTaskLeaseV3(v.expectedLease),
  };
}
export function parseRenewTaskLeaseResultV3(
  value: unknown,
): RenewTaskLeaseResultV3 {
  const raw = input(value) as Record<string, unknown>;
  if (raw.kind === "renewed") {
    const v = record(raw, [
      "kind",
      "previousLease",
      "currentLease",
      "serverObservedAt",
    ]);
    const previousLease = parseTaskLeaseV3(v.previousLease),
      currentLease = parseTaskLeaseV3(v.currentLease);
    if (
      currentLease.leaseRevision !== previousLease.leaseRevision + 1 ||
      currentLease.leaseEpoch !== previousLease.leaseEpoch ||
      currentLease.fenceToken !== previousLease.fenceToken ||
      currentLease.expiresAt <= previousLease.expiresAt
    )
      fail("INVARIANT_VIOLATION");
    return {
      kind: "renewed",
      previousLease,
      currentLease,
      serverObservedAt: timestamp(v.serverObservedAt),
    };
  }
  const v = record(raw, [
    "kind",
    "code",
    "serverObservedAt",
    "retryNotBefore",
    "observedLeaseRevision",
  ]);
  const code = oneOf(v.code, [
    "TASK_LEASE_RENEWAL_TOO_EARLY",
    "TASK_LEASE_RENEWAL_CONFLICT",
    "TASK_LEASE_EXPIRED",
  ]);
  const retryNotBefore = nullable(v.retryNotBefore, timestamp);
  if ((code === "TASK_LEASE_RENEWAL_TOO_EARLY") !== (retryNotBefore !== null))
    fail("INVARIANT_VIOLATION");
  return {
    kind: oneOf(v.kind, ["rejected"]),
    code,
    serverObservedAt: timestamp(v.serverObservedAt),
    retryNotBefore,
    observedLeaseRevision: nullable(v.observedLeaseRevision, (x) =>
      integer(x, 1),
    ),
  };
}
export function parseReleaseTaskLeaseInputV3(
  value: unknown,
): ReleaseTaskLeaseInputV3 {
  const v = record(value, ["commandId", "expectedLease", "reason"]);
  return {
    commandId: id.command(v.commandId),
    expectedLease: parseTaskLeaseV3(v.expectedLease),
    reason: oneOf(v.reason, [
      "work_yielded",
      "owner_stopping",
      "reconciliation_relinquish",
    ]),
  };
}
export function parseReleaseTaskLeaseResultV3(
  value: unknown,
): ReleaseTaskLeaseResultV3 {
  const v = record(value, [
    "kind",
    "closedLease",
    "reason",
    "previousTaskStatus",
    "currentTaskStatus",
    "currentTaskRowVersion",
  ]);
  return {
    kind: oneOf(v.kind, ["released"]),
    closedLease: parseTaskLeaseV3(v.closedLease),
    reason: oneOf(v.reason, [
      "work_yielded",
      "owner_stopping",
      "reconciliation_relinquish",
    ]),
    previousTaskStatus: oneOf(v.previousTaskStatus, ["in_progress"]),
    currentTaskStatus: oneOf(v.currentTaskStatus, ["todo"]),
    currentTaskRowVersion: integer(v.currentTaskRowVersion, 1),
  };
}
export function parseTaskMutationFenceV3(value: unknown): TaskMutationFenceV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "expectedTaskStatus",
    "expectedTaskRowVersion",
  ]);
  return {
    commandId: id.command(v.commandId),
    expectedLease: parseTaskLeaseV3(v.expectedLease),
    expectedTaskStatus: oneOf(v.expectedTaskStatus, ["in_progress"]),
    expectedTaskRowVersion: integer(v.expectedTaskRowVersion, 1),
  };
}

function parsePathClaim(value: unknown): RepositoryPathClaimV3 {
  const v = record(value, ["kind", "path"]);
  return {
    kind: oneOf(v.kind, ["file", "subtree"]),
    path: normalizeRepositoryPathV3(v.path),
  };
}
export function parseProposedWorkspaceV3(value: unknown): ProposedWorkspaceV3 {
  const v = record(value, [
    "repositoryId",
    "baseCommit",
    "baseTree",
    "workspaceGeneration",
    "executionMode",
    "pathClaims",
    "readArtifactDigest",
    "integrationOwnerTaskId",
    "artifactContractTemplateDigest",
    "policyDigest",
  ]);
  const executionMode = oneOf(v.executionMode, [
    "isolated_worktree",
    "read_only_artifact",
  ]);
  const pathClaims = array(
    v.pathClaims,
    parsePathClaim,
    TASK_V3_LIMITS.maxPathClaims,
    {
      empty: executionMode === "read_only_artifact",
      canonical: (x) => `${x.path}\0${x.kind}`,
    },
  );
  const readArtifactDigest = nullable(v.readArtifactDigest, digest);
  if (
    executionMode === "isolated_worktree"
      ? pathClaims.length === 0 || readArtifactDigest !== null
      : pathClaims.length !== 0 || readArtifactDigest === null
  )
    fail("INVARIANT_VIOLATION");
  for (let a = 0; a < pathClaims.length; a += 1)
    for (let b = a + 1; b < pathClaims.length; b += 1)
      if (repositoryPathClaimsOverlapV3(pathClaims[a]!, pathClaims[b]!))
        fail("INVARIANT_VIOLATION");
  return {
    repositoryId: id.repository(v.repositoryId),
    baseCommit: sha<CommitSha>(v.baseCommit),
    baseTree: sha<TreeSha>(v.baseTree),
    workspaceGeneration: integer(v.workspaceGeneration, 1),
    executionMode,
    pathClaims,
    readArtifactDigest,
    integrationOwnerTaskId: id.task(v.integrationOwnerTaskId),
    artifactContractTemplateDigest: digest(v.artifactContractTemplateDigest),
    policyDigest: digest(v.policyDigest),
  };
}
export function parseWorkspaceContractV3(value: unknown): WorkspaceContractV3 {
  const v = record(value, [
    "protocolVersion",
    "workspaceContractId",
    "taskId",
    "rootTaskId",
    "repositoryId",
    "baseCommit",
    "baseTree",
    "workspaceGeneration",
    "executionMode",
    "pathClaims",
    "readArtifactDigest",
    "integrationOwnerTaskId",
    "artifactContractTemplateDigest",
    "policyDigest",
  ]);
  const workspace = parseProposedWorkspaceV3(
    Object.fromEntries(
      Object.entries(v).filter(
        ([k]) =>
          ![
            "protocolVersion",
            "workspaceContractId",
            "taskId",
            "rootTaskId",
          ].includes(k),
      ),
    ),
  );
  return {
    ...workspace,
    protocolVersion: protocolVersion(v.protocolVersion),
    workspaceContractId: id.workspace(v.workspaceContractId),
    taskId: id.task(v.taskId),
    rootTaskId: id.task(v.rootTaskId),
  };
}
function parseChild(value: unknown): ProposedChildV3 {
  const v = record(value, [
    "clientKey",
    "title",
    "titleDigest",
    "titleClassifierPolicyDigest",
    "laneRole",
    "requiredCapabilities",
    "workspace",
  ]);
  const title = text(v.title, TASK_V3_LIMITS.maxTitleUtf8Bytes);
  return {
    clientKey: key<string>(v.clientKey),
    title,
    titleDigest: digest(v.titleDigest),
    titleClassifierPolicyDigest: digest(v.titleClassifierPolicyDigest),
    laneRole: oneOf(v.laneRole, [
      "server",
      "daemon",
      "driver",
      "storage",
      "protocol",
      "verifier",
      "security",
      "review",
      "integration",
    ]),
    requiredCapabilities: array(
      v.requiredCapabilities,
      (x) => key<CapabilityKey>(x),
      TASK_V3_LIMITS.maxCapabilityKeys,
      { empty: true },
    ),
    workspace: parseProposedWorkspaceV3(v.workspace),
  };
}
function parseDependencyEndpoint(value: unknown): ProposedDependencyEndpointV3 {
  const raw = input(value) as Record<string, unknown>;
  if (raw.kind === "child") {
    const v = record(raw, ["kind", "clientKey"]);
    return { kind: "child", clientKey: key<string>(v.clientKey) };
  }
  const v = record(raw, ["kind", "taskId"]);
  return { kind: oneOf(v.kind, ["task"]), taskId: id.task(v.taskId) };
}
function parseDependency(value: unknown): ProposedDependencyV3 {
  const v = record(value, ["prerequisite", "dependent"]);
  const result = {
    prerequisite: parseDependencyEndpoint(v.prerequisite),
    dependent: parseDependencyEndpoint(v.dependent),
  };
  if (
    new TextDecoder().decode(canonicalProtocolJson(result.prerequisite)) ===
    new TextDecoder().decode(canonicalProtocolJson(result.dependent))
  )
    fail("INVARIANT_VIOLATION");
  return result;
}
export function parseProposeTaskGraphInputV3(
  value: unknown,
): ProposeTaskGraphInputV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "expectedTaskStatus",
    "expectedTaskRowVersion",
    "rootTaskId",
    "sourceMessageId",
    "sourceTurnId",
    "sourceProducerFactId",
    "committedReplyMessageId",
    "expectedGraphRevision",
    "expectedPolicyDigest",
    "expectedTitleClassifierPolicyDigest",
    "children",
    "dependencies",
  ]);
  const fence = parseTaskMutationFenceV3(
    Object.fromEntries(
      Object.entries(v).filter(([k]) =>
        [
          "commandId",
          "expectedLease",
          "expectedTaskStatus",
          "expectedTaskRowVersion",
        ].includes(k),
      ),
    ),
  );
  const children = array(
    v.children,
    parseChild,
    TASK_V3_LIMITS.maxProposalChildren,
    { canonical: (x) => x.clientKey },
  );
  const dependencies = array(
    v.dependencies,
    parseDependency,
    TASK_V3_LIMITS.maxGraphEdges,
    {
      empty: true,
      canonical: (dependency) =>
        new TextDecoder().decode(canonicalProtocolJson(dependency)),
    },
  );
  return {
    ...fence,
    rootTaskId: id.task(v.rootTaskId),
    sourceMessageId: id.message(v.sourceMessageId),
    sourceTurnId: id.turn(v.sourceTurnId),
    sourceProducerFactId: id.producer(v.sourceProducerFactId),
    committedReplyMessageId: id.message(v.committedReplyMessageId),
    expectedGraphRevision: integer(v.expectedGraphRevision, 0),
    expectedPolicyDigest: digest(v.expectedPolicyDigest),
    expectedTitleClassifierPolicyDigest: digest(
      v.expectedTitleClassifierPolicyDigest,
    ),
    children,
    dependencies,
  };
}

function parseKinds(value: unknown): Array<"git_commit" | "digest_blob"> {
  return array(value, (x) => oneOf(x, ["digest_blob", "git_commit"]), 2);
}
function parseSeats(value: unknown): ReviewSeatKey[] {
  return array(
    value,
    (x) => key<ReviewSeatKey>(x),
    TASK_V3_LIMITS.maxReviewSeats,
  );
}
export function parseArtifactContractTemplateV3(
  value: unknown,
): ArtifactContractTemplateV3 {
  const v = record(value, [
    "protocolVersion",
    "templateId",
    "templateRevision",
    "allowedKinds",
    "maxMaterialBytes",
    "scopePolicy",
    "receiptPolicyDigest",
    "requiredReviewSeats",
    "policyDigest",
  ]);
  return {
    protocolVersion: protocolVersion(v.protocolVersion),
    templateId: id.template(v.templateId),
    templateRevision: integer(v.templateRevision, 1),
    allowedKinds: parseKinds(v.allowedKinds),
    maxMaterialBytes: integer(v.maxMaterialBytes, 1),
    scopePolicy: oneOf(v.scopePolicy, ["exact_workspace_claims"]),
    receiptPolicyDigest: digest(v.receiptPolicyDigest),
    requiredReviewSeats: parseSeats(v.requiredReviewSeats),
    policyDigest: digest(v.policyDigest),
  };
}
export function parseMaterializedArtifactContractV3(
  value: unknown,
): MaterializedArtifactContractV3 {
  const v = record(value, [
    "protocolVersion",
    "artifactContractId",
    "templateDigest",
    "serverId",
    "rootTaskId",
    "taskId",
    "workspaceContractId",
    "workspaceContractDigest",
    "baseCommit",
    "baseTree",
    "pathClaimsDigest",
    "integrationOwnerTaskId",
    "allowedKinds",
    "maxMaterialBytes",
    "scopePolicy",
    "receiptPolicyDigest",
    "requiredReviewSeats",
    "gate3PlanDigest",
    "policyDigest",
  ]);
  return {
    protocolVersion: protocolVersion(v.protocolVersion),
    artifactContractId: id.contract(v.artifactContractId),
    templateDigest: digest(v.templateDigest),
    serverId: id.server(v.serverId),
    rootTaskId: id.task(v.rootTaskId),
    taskId: id.task(v.taskId),
    workspaceContractId: id.workspace(v.workspaceContractId),
    workspaceContractDigest: digest(v.workspaceContractDigest),
    baseCommit: sha<CommitSha>(v.baseCommit),
    baseTree: sha<TreeSha>(v.baseTree),
    pathClaimsDigest: digest(v.pathClaimsDigest),
    integrationOwnerTaskId: id.task(v.integrationOwnerTaskId),
    allowedKinds: parseKinds(v.allowedKinds),
    maxMaterialBytes: integer(v.maxMaterialBytes, 1),
    scopePolicy: oneOf(v.scopePolicy, ["exact_workspace_claims"]),
    receiptPolicyDigest: digest(v.receiptPolicyDigest),
    requiredReviewSeats: parseSeats(v.requiredReviewSeats),
    gate3PlanDigest: digest(v.gate3PlanDigest),
    policyDigest: digest(v.policyDigest),
  };
}
export function parseStagedArtifactMaterialV3(
  value: unknown,
): StagedArtifactMaterialV3 {
  const v = record(value, [
    "stagedObjectId",
    "role",
    "kind",
    "mediaType",
    "byteLength",
    "sha256",
    "prerequisiteCommit",
    "expiresAt",
  ]);
  const role = oneOf(v.role, [
    "artifact",
    "scope_manifest",
    "acceptance_receipt",
  ]);
  const kind = oneOf(v.kind, [
    "prerequisite_bound_git_bundle",
    "digest_blob",
    "canonical_json",
  ]);
  if (
    role === "artifact" ? kind === "canonical_json" : kind !== "canonical_json"
  )
    fail("INVARIANT_VIOLATION");
  const prerequisiteCommit = nullable(v.prerequisiteCommit, (x) =>
    sha<CommitSha>(x),
  );
  if (
    (kind === "prerequisite_bound_git_bundle") !==
    (prerequisiteCommit !== null)
  )
    fail("INVARIANT_VIOLATION");
  return {
    stagedObjectId: id.object(v.stagedObjectId),
    role,
    kind,
    mediaType: text(v.mediaType, 255),
    byteLength: integer(v.byteLength, 1),
    sha256: digest(v.sha256),
    prerequisiteCommit,
    expiresAt: timestamp(v.expiresAt),
  };
}
function parseArtifactIdentity(value: unknown): ArtifactIdentityV3 {
  const raw = input(value) as Record<string, unknown>;
  if (raw.kind === "git_commit") {
    const v = record(raw, ["kind", "commitSha", "treeSha", "orderedParents"]);
    return {
      kind: "git_commit",
      commitSha: sha<CommitSha>(v.commitSha),
      treeSha: sha<TreeSha>(v.treeSha),
      orderedParents: array(v.orderedParents, (x) => sha<CommitSha>(x), 16, {
        empty: true,
        ordered: true,
      }),
    };
  }
  const v = record(raw, ["kind", "mediaType", "byteLength", "sha256"]);
  return {
    kind: oneOf(v.kind, ["digest_blob"]),
    mediaType: text(v.mediaType, 255),
    byteLength: integer(v.byteLength, 1),
    sha256: digest(v.sha256),
  };
}
export function parseArtifactClaimsV3(value: unknown): ArtifactClaimsV3 {
  const v = record(value, [
    "protocolVersion",
    "workspaceContractDigest",
    "artifactContractDigest",
    "gate3PlanDigest",
    "identity",
    "artifactDigest",
    "scopeManifestDigest",
    "acceptanceReceiptDigest",
  ]);
  const identity = parseArtifactIdentity(v.identity);
  return {
    protocolVersion: protocolVersion(v.protocolVersion),
    workspaceContractDigest: digest(v.workspaceContractDigest),
    artifactContractDigest: digest(v.artifactContractDigest),
    gate3PlanDigest: digest(v.gate3PlanDigest),
    identity,
    artifactDigest: expectDigestIdentity(v.artifactDigest, identity),
    scopeManifestDigest: digest(v.scopeManifestDigest),
    acceptanceReceiptDigest: digest(v.acceptanceReceiptDigest),
  };
}
export function parseProposedArtifactV3(value: unknown): ProposedArtifactV3 {
  const v = record(value, [
    "protocolVersion",
    "workspaceContractDigest",
    "artifactContractDigest",
    "gate3PlanDigest",
    "identity",
    "artifactDigest",
    "scopeManifestDigest",
    "acceptanceReceiptDigest",
    "stagedMaterial",
    "stagedScopeManifest",
    "stagedAcceptanceReceipt",
  ]);
  const claims = parseArtifactClaimsV3(
    Object.fromEntries(
      Object.entries(v).filter(([k]) => !k.startsWith("staged")),
    ),
  );
  const stagedMaterial = parseStagedArtifactMaterialV3(v.stagedMaterial),
    stagedScopeManifest = parseStagedArtifactMaterialV3(v.stagedScopeManifest),
    stagedAcceptanceReceipt = parseStagedArtifactMaterialV3(
      v.stagedAcceptanceReceipt,
    );
  if (
    stagedMaterial.role !== "artifact" ||
    stagedScopeManifest.role !== "scope_manifest" ||
    stagedAcceptanceReceipt.role !== "acceptance_receipt" ||
    stagedScopeManifest.sha256 !== claims.scopeManifestDigest ||
    stagedAcceptanceReceipt.sha256 !== claims.acceptanceReceiptDigest
  )
    fail("INVARIANT_VIOLATION");
  return {
    ...claims,
    stagedMaterial,
    stagedScopeManifest,
    stagedAcceptanceReceipt,
  };
}
export function parseTaskAttemptContributionLedgerV3(
  value: unknown,
): TaskAttemptContributionLedgerV3 {
  const v = record(value, [
    "taskId",
    "attempt",
    "ledgerRevision",
    "acceptedSourceArtifactIds",
    "contributorAgentIds",
  ]);
  return {
    taskId: id.task(v.taskId),
    attempt: integer(v.attempt, 1),
    ledgerRevision: integer(v.ledgerRevision, 1),
    acceptedSourceArtifactIds: array(
      v.acceptedSourceArtifactIds,
      id.artifact,
      32,
      { empty: true },
    ),
    contributorAgentIds: array(v.contributorAgentIds, id.agent, 64, {
      empty: true,
    }),
  };
}

function parseRequirement(value: unknown): ReviewRequirementV3 {
  const v = record(value, [
    "artifactId",
    "artifactDigest",
    "artifactContractDigest",
    "gate3PlanDigest",
    "scenarioVersion",
    "seatKey",
  ]);
  return {
    artifactId: id.artifact(v.artifactId),
    artifactDigest: digest(v.artifactDigest),
    artifactContractDigest: digest(v.artifactContractDigest),
    gate3PlanDigest: digest(v.gate3PlanDigest),
    scenarioVersion: integer(v.scenarioVersion, 1),
    seatKey: key<ReviewSeatKey>(v.seatKey),
  };
}
export const parseReviewRequirementV3 = parseRequirement;
export function parseReviewAssignmentV3(value: unknown): ReviewAssignmentV3 {
  const v = record(value, [
    "artifactId",
    "artifactDigest",
    "artifactContractDigest",
    "gate3PlanDigest",
    "scenarioVersion",
    "seatKey",
    "assignmentId",
    "reviewTaskId",
    "reviewerAgentId",
    "reviewerRegistryRevision",
    "assignmentRevision",
    "assignmentStatus",
    "closedReason",
  ]);
  const requirement = parseRequirement(
    Object.fromEntries(
      Object.entries(v).filter(([k]) =>
        [
          "artifactId",
          "artifactDigest",
          "artifactContractDigest",
          "gate3PlanDigest",
          "scenarioVersion",
          "seatKey",
        ].includes(k),
      ),
    ),
  );
  const assignmentStatus = oneOf(v.assignmentStatus, ["current", "closed"]);
  const closedReason: ReviewAssignmentV3["closedReason"] = nullable(
    v.closedReason,
    (x) =>
      oneOf(x, ["reassigned", "terminal_verdict", "barrier_blocked"] as const),
  );
  if ((assignmentStatus === "current") !== (closedReason === null))
    fail("INVARIANT_VIOLATION");
  return {
    ...requirement,
    assignmentId: id.assignment(v.assignmentId),
    reviewTaskId: id.task(v.reviewTaskId),
    reviewerAgentId: id.agent(v.reviewerAgentId),
    reviewerRegistryRevision: integer(v.reviewerRegistryRevision, 1),
    assignmentRevision: integer(v.assignmentRevision, 1),
    assignmentStatus,
    closedReason,
  };
}
export function parseReviewVerdictV3(value: unknown): ReviewVerdictV3 {
  const v = record(value, [
    "artifactId",
    "artifactDigest",
    "artifactContractDigest",
    "gate3PlanDigest",
    "scenarioVersion",
    "seatKey",
    "assignmentId",
    "reviewTaskId",
    "reviewerAgentId",
    "reviewerRegistryRevision",
    "assignmentRevision",
    "assignmentStatus",
    "closedReason",
    "reviewAttempt",
    "verdict",
    "findingsDigest",
    "evidenceReceiptDigest",
  ]);
  const assignment = parseReviewAssignmentV3(
    Object.fromEntries(
      Object.entries(v).filter(
        ([k]) =>
          ![
            "reviewAttempt",
            "verdict",
            "findingsDigest",
            "evidenceReceiptDigest",
          ].includes(k),
      ),
    ),
  );
  if (
    assignment.assignmentStatus !== "current" ||
    assignment.closedReason !== null
  )
    fail("INVARIANT_VIOLATION");
  return {
    ...assignment,
    reviewAttempt: integer(v.reviewAttempt, 1),
    verdict: oneOf(v.verdict, ["GO", "BLOCK"]),
    findingsDigest: digest(v.findingsDigest),
    evidenceReceiptDigest: digest(v.evidenceReceiptDigest),
  };
}

export function parseBlockTaskInputV3(value: unknown): BlockTaskInputV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "expectedTaskStatus",
    "expectedTaskRowVersion",
    "reason",
  ]);
  const fence = parseTaskMutationFenceV3({
    commandId: v.commandId,
    expectedLease: v.expectedLease,
    expectedTaskStatus: v.expectedTaskStatus,
    expectedTaskRowVersion: v.expectedTaskRowVersion,
  });
  return { ...fence, reason: oneOf(v.reason, ["execution_failed"]) };
}
function parseBlockedTransition(value: unknown): BlockedTaskTransitionV3 {
  const v = record(value, [
    "taskId",
    "previousStatus",
    "currentStatus",
    "previousTaskRowVersion",
    "currentTaskRowVersion",
    "closedClaimId",
    "closedLeaseId",
    "leaseCloseReason",
    "closedReservationIds",
  ]);
  const closedClaimId = nullable(v.closedClaimId, id.claim);
  const closedLeaseId = nullable(v.closedLeaseId, id.lease);
  const leaseCloseReason: BlockedTaskTransitionV3["leaseCloseReason"] =
    nullable(v.leaseCloseReason, (item) =>
      oneOf(item, ["graph_blocked"] as const),
    );
  const closedReservationIds = array(
    v.closedReservationIds,
    id.reservation,
    TASK_V3_LIMITS.maxPathClaims,
    { empty: true },
  );
  if (
    (closedClaimId === null) !== (closedLeaseId === null) ||
    (closedClaimId === null) !== (leaseCloseReason === null) ||
    (closedClaimId === null && closedReservationIds.length !== 0)
  )
    fail("INVARIANT_VIOLATION");
  const previousTaskRowVersion = integer(v.previousTaskRowVersion, 1);
  const currentTaskRowVersion = integer(v.currentTaskRowVersion, 2);
  if (currentTaskRowVersion !== previousTaskRowVersion + 1)
    fail("INVARIANT_VIOLATION");
  return {
    taskId: id.task(v.taskId),
    previousStatus: oneOf(v.previousStatus, ["todo", "in_progress", "waiting"]),
    currentStatus: oneOf(v.currentStatus, ["blocked"]),
    previousTaskRowVersion,
    currentTaskRowVersion,
    closedClaimId,
    closedLeaseId,
    leaseCloseReason,
    closedReservationIds,
  };
}
export function parseBlockTaskResultV3(value: unknown): BlockTaskResultV3 {
  const v = record(value, [
    "kind",
    "rootCauseTaskId",
    "previousGraphRevision",
    "currentGraphRevision",
    "affected",
  ]);
  const previousGraphRevision = integer(v.previousGraphRevision, 0),
    currentGraphRevision = integer(v.currentGraphRevision, 1);
  if (currentGraphRevision !== previousGraphRevision + 1)
    fail("INVARIANT_VIOLATION");
  const affected = array(
    v.affected,
    parseBlockedTransition,
    TASK_V3_LIMITS.maxGraphNodes,
    { canonical: (item) => item.taskId },
  );
  const rootCauseTaskId = id.task(v.rootCauseTaskId);
  if (!affected.some((item) => item.taskId === rootCauseTaskId))
    fail("INVARIANT_VIOLATION");
  return {
    kind: oneOf(v.kind, ["blocked"]),
    rootCauseTaskId,
    previousGraphRevision,
    currentGraphRevision,
    affected,
  };
}
export function parseProposeTaskGraphResultV3(
  value: unknown,
): ProposeTaskGraphResultV3 {
  const v = record(value, [
    "rootTaskId",
    "previousGraphRevision",
    "currentGraphRevision",
    "policyDigest",
    "children",
    "proposerStatus",
  ]);
  const previousGraphRevision = integer(v.previousGraphRevision, 0),
    currentGraphRevision = integer(v.currentGraphRevision, 1);
  if (currentGraphRevision !== previousGraphRevision + 1)
    fail("INVARIANT_VIOLATION");
  const children = array(
    v.children,
    (item) => {
      const child = record(item, [
        "clientKey",
        "taskId",
        "taskNumber",
        "status",
        "titleDigest",
        "titleClassifierPolicyDigest",
      ]);
      return {
        clientKey: key<string>(child.clientKey),
        taskId: id.task(child.taskId),
        taskNumber: integer(child.taskNumber, 1),
        status: oneOf(child.status, ["todo"]),
        titleDigest: digest(child.titleDigest),
        titleClassifierPolicyDigest: digest(child.titleClassifierPolicyDigest),
      };
    },
    TASK_V3_LIMITS.maxProposalChildren,
    { canonical: (item) => item.clientKey },
  );
  return {
    rootTaskId: id.task(v.rootTaskId),
    previousGraphRevision,
    currentGraphRevision,
    policyDigest: digest(v.policyDigest),
    children,
    proposerStatus: oneOf(v.proposerStatus, ["waiting"]),
  };
}
export function parseStageArtifactMaterialInputV3(
  value: unknown,
): StageArtifactMaterialInputV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "artifactContractDigest",
    "role",
    "sourceCapability",
  ]);
  return {
    commandId: id.command(v.commandId),
    expectedLease: parseTaskLeaseV3(v.expectedLease),
    artifactContractDigest: digest(v.artifactContractDigest),
    role: oneOf(v.role, ["artifact", "scope_manifest", "acceptance_receipt"]),
    sourceCapability: key<LaunchCapabilityRef>(v.sourceCapability),
  };
}
export function parseArtifactDescriptorV3(
  value: unknown,
): ArtifactDescriptorV3 {
  const keys = [
    "protocolVersion",
    "workspaceContractDigest",
    "artifactContractDigest",
    "gate3PlanDigest",
    "identity",
    "artifactDigest",
    "scopeManifestDigest",
    "acceptanceReceiptDigest",
    "artifactId",
    "materialObjectId",
    "materialKind",
    "materialMediaType",
    "materialByteLength",
    "materialSha256",
    "materialPrerequisiteCommit",
    "scopeManifestObjectId",
    "scopeManifestSha256",
    "scopeManifestByteLength",
    "acceptanceReceiptObjectId",
    "acceptanceReceiptSha256",
    "acceptanceReceiptByteLength",
    "consumedSourceArtifactIds",
    "rootTaskId",
    "taskId",
    "attempt",
    "builderAgentId",
    "contributorAgentIds",
  ] as const;
  const v = record(value, keys);
  const claims = parseArtifactClaimsV3(
    Object.fromEntries(
      Object.entries(v).filter(([name]) =>
        [
          "protocolVersion",
          "workspaceContractDigest",
          "artifactContractDigest",
          "gate3PlanDigest",
          "identity",
          "artifactDigest",
          "scopeManifestDigest",
          "acceptanceReceiptDigest",
        ].includes(name),
      ),
    ),
  );
  const materialKind = oneOf(v.materialKind, [
    "prerequisite_bound_git_bundle",
    "digest_blob",
  ]);
  const materialPrerequisiteCommit = nullable(
    v.materialPrerequisiteCommit,
    (item) => sha<CommitSha>(item),
  );
  if (
    (materialKind === "prerequisite_bound_git_bundle") !==
    (materialPrerequisiteCommit !== null)
  )
    fail("INVARIANT_VIOLATION");
  const scopeManifestSha256 = digest(v.scopeManifestSha256),
    acceptanceReceiptSha256 = digest(v.acceptanceReceiptSha256);
  if (
    scopeManifestSha256 !== claims.scopeManifestDigest ||
    acceptanceReceiptSha256 !== claims.acceptanceReceiptDigest
  )
    fail("INVARIANT_VIOLATION");
  return {
    ...claims,
    artifactId: id.artifact(v.artifactId),
    materialObjectId: id.object(v.materialObjectId),
    materialKind,
    materialMediaType: text(v.materialMediaType, 255),
    materialByteLength: integer(v.materialByteLength, 1),
    materialSha256: digest(v.materialSha256),
    materialPrerequisiteCommit,
    scopeManifestObjectId: id.object(v.scopeManifestObjectId),
    scopeManifestSha256,
    scopeManifestByteLength: integer(v.scopeManifestByteLength, 1),
    acceptanceReceiptObjectId: id.object(v.acceptanceReceiptObjectId),
    acceptanceReceiptSha256,
    acceptanceReceiptByteLength: integer(v.acceptanceReceiptByteLength, 1),
    consumedSourceArtifactIds: array(
      v.consumedSourceArtifactIds,
      id.artifact,
      TASK_V3_LIMITS.maxGraphNodes,
      { empty: true },
    ),
    rootTaskId: id.task(v.rootTaskId),
    taskId: id.task(v.taskId),
    attempt: integer(v.attempt, 1),
    builderAgentId: id.agent(v.builderAgentId),
    contributorAgentIds: array(v.contributorAgentIds, id.agent, 64, {
      empty: true,
    }),
  };
}
export function parseTaskAttemptContributorV3(
  value: unknown,
): TaskAttemptContributorV3 {
  const v = record(value, [
    "taskId",
    "attempt",
    "agentId",
    "source",
    "sourceArtifactId",
    "recordedAt",
  ]);
  const source = oneOf(v.source, ["claim_owner", "accepted_upstream_artifact"]),
    sourceArtifactId = nullable(v.sourceArtifactId, id.artifact);
  if ((source === "claim_owner") !== (sourceArtifactId === null))
    fail("INVARIANT_VIOLATION");
  return {
    taskId: id.task(v.taskId),
    attempt: integer(v.attempt, 1),
    agentId: id.agent(v.agentId),
    source,
    sourceArtifactId,
    recordedAt: timestamp(v.recordedAt),
  };
}
export function parseConsumeAcceptedArtifactInputV3(
  value: unknown,
): ConsumeAcceptedArtifactInputV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "expectedTaskStatus",
    "expectedTaskRowVersion",
    "sourceArtifactId",
    "sourceArtifactDigest",
    "expectedSourceTaskRowVersion",
    "expectedSourceBarrierRevision",
    "expectedTargetGraphRevision",
    "expectedContributorLedgerRevision",
  ]);
  const fence = parseTaskMutationFenceV3({
    commandId: v.commandId,
    expectedLease: v.expectedLease,
    expectedTaskStatus: v.expectedTaskStatus,
    expectedTaskRowVersion: v.expectedTaskRowVersion,
  });
  return {
    ...fence,
    sourceArtifactId: id.artifact(v.sourceArtifactId),
    sourceArtifactDigest: digest(v.sourceArtifactDigest),
    expectedSourceTaskRowVersion: integer(v.expectedSourceTaskRowVersion, 1),
    expectedSourceBarrierRevision: integer(v.expectedSourceBarrierRevision, 0),
    expectedTargetGraphRevision: integer(v.expectedTargetGraphRevision, 0),
    expectedContributorLedgerRevision: integer(
      v.expectedContributorLedgerRevision,
      1,
    ),
  };
}
export function parseConsumeAcceptedArtifactResultV3(
  value: unknown,
): ConsumeAcceptedArtifactResultV3 {
  const v = record(value, [
    "kind",
    "consumptionId",
    "materializationGrantId",
    "sourceArtifactId",
    "sourceArtifactDigest",
    "previousContributorLedgerRevision",
    "currentContributorLedgerRevision",
    "acceptedSourceArtifactIds",
    "contributorAgentIds",
  ]);
  const previousContributorLedgerRevision = integer(
      v.previousContributorLedgerRevision,
      1,
    ),
    currentContributorLedgerRevision = integer(
      v.currentContributorLedgerRevision,
      2,
    );
  if (
    currentContributorLedgerRevision !==
    previousContributorLedgerRevision + 1
  )
    fail("INVARIANT_VIOLATION");
  const sourceArtifactId = id.artifact(v.sourceArtifactId);
  const acceptedSourceArtifactIds = array(
    v.acceptedSourceArtifactIds,
    id.artifact,
    TASK_V3_LIMITS.maxGraphNodes,
  );
  if (!acceptedSourceArtifactIds.includes(sourceArtifactId))
    fail("INVARIANT_VIOLATION");
  return {
    kind: oneOf(v.kind, ["consumed"]),
    consumptionId: id.consumption(v.consumptionId),
    materializationGrantId: id.grant(v.materializationGrantId),
    sourceArtifactId,
    sourceArtifactDigest: digest(v.sourceArtifactDigest),
    previousContributorLedgerRevision,
    currentContributorLedgerRevision,
    acceptedSourceArtifactIds,
    contributorAgentIds: array(v.contributorAgentIds, id.agent, 64),
  };
}
export function parseOpenConsumedArtifactMaterialInputV3(
  value: unknown,
): OpenConsumedArtifactMaterialInputV3 {
  const v = record(value, [
    "expectedLease",
    "consumptionId",
    "materializationGrantId",
    "sourceArtifactId",
    "sourceArtifactDigest",
  ]);
  return {
    expectedLease: parseTaskLeaseV3(v.expectedLease),
    consumptionId: id.consumption(v.consumptionId),
    materializationGrantId: id.grant(v.materializationGrantId),
    sourceArtifactId: id.artifact(v.sourceArtifactId),
    sourceArtifactDigest: digest(v.sourceArtifactDigest),
  };
}
export function parsePublishArtifactInputV3(
  value: unknown,
): PublishArtifactInputV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "expectedTaskStatus",
    "expectedTaskRowVersion",
    "artifact",
  ]);
  const fence = parseTaskMutationFenceV3({
    commandId: v.commandId,
    expectedLease: v.expectedLease,
    expectedTaskStatus: v.expectedTaskStatus,
    expectedTaskRowVersion: v.expectedTaskRowVersion,
  });
  return { ...fence, artifact: parseProposedArtifactV3(v.artifact) };
}
export function parseAssignReviewSeatInputV3(
  value: unknown,
): AssignReviewSeatInputV3 {
  const v = record(value, [
    "commandId",
    "requirement",
    "reviewTaskId",
    "expectedBarrierRevision",
    "reviewerAgentId",
    "expectedRegistryRevision",
    "expectedAssignmentRevision",
    "expectedBarrierState",
    "expectedReviewTaskStatus",
    "expectedTerminalVerdictId",
  ]);
  if (v.expectedTerminalVerdictId !== null) fail("INVARIANT_VIOLATION");
  return {
    commandId: id.command(v.commandId),
    requirement: parseRequirement(v.requirement),
    reviewTaskId: id.task(v.reviewTaskId),
    expectedBarrierRevision: integer(v.expectedBarrierRevision, 0),
    reviewerAgentId: id.agent(v.reviewerAgentId),
    expectedRegistryRevision: integer(v.expectedRegistryRevision, 1),
    expectedAssignmentRevision: nullable(v.expectedAssignmentRevision, (item) =>
      integer(item, 1),
    ),
    expectedBarrierState: oneOf(v.expectedBarrierState, ["open"]),
    expectedReviewTaskStatus: oneOf(v.expectedReviewTaskStatus, [
      "todo",
      "in_progress",
    ]),
    expectedTerminalVerdictId: null,
  };
}
export function parseAssignReviewSeatResultV3(
  value: unknown,
): AssignReviewSeatResultV3 {
  const v = record(value, [
    "assignment",
    "previousAssignmentId",
    "currentBarrierRevision",
  ]);
  const assignment = parseReviewAssignmentV3(v.assignment),
    previousAssignmentId = nullable(v.previousAssignmentId, id.assignment);
  if ((assignment.assignmentRevision === 1) !== (previousAssignmentId === null))
    fail("INVARIANT_VIOLATION");
  return {
    assignment,
    previousAssignmentId,
    currentBarrierRevision: integer(v.currentBarrierRevision, 1),
  };
}
export function parseSubmitReviewVerdictInputV3(
  value: unknown,
): SubmitReviewVerdictInputV3 {
  const v = record(value, [
    "commandId",
    "expectedLease",
    "expectedTaskStatus",
    "expectedTaskRowVersion",
    "verdict",
  ]);
  const fence = parseTaskMutationFenceV3({
    commandId: v.commandId,
    expectedLease: v.expectedLease,
    expectedTaskStatus: v.expectedTaskStatus,
    expectedTaskRowVersion: v.expectedTaskRowVersion,
  });
  const verdict = parseReviewVerdictV3(v.verdict);
  if (
    verdict.reviewAttempt !== fence.expectedLease.attempt ||
    verdict.reviewTaskId !== fence.expectedLease.taskId
  )
    fail("INVARIANT_VIOLATION");
  return { ...fence, verdict };
}

// Canonical digest helper shared by storage and tests; it rejects values the protocol JSON encoder cannot represent.
export function taskV3CanonicalDigestInput(value: unknown): Uint8Array {
  return canonicalProtocolJson(value);
}
