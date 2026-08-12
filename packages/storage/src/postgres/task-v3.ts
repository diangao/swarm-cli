import { createHash, randomBytes } from "node:crypto";
import {
  TASK_V3_LIMITS,
  canonicalProtocolJson,
  parseArtifactClaimsV3,
  parseArtifactContractTemplateV3,
  parseArtifactDescriptorV3,
  parseAssignReviewSeatInputV3,
  parseBlockTaskInputV3,
  parseClaimTaskInputV3,
  parseConsumeAcceptedArtifactInputV3,
  parseMaterializedArtifactContractV3,
  parsePublishArtifactInputV3,
  parseProposeTaskGraphInputV3,
  parseReleaseTaskLeaseInputV3,
  parseRenewTaskLeaseInputV3,
  parseReviewVerdictV3,
  parseStagedArtifactMaterialV3,
  parseSubmitReviewVerdictInputV3,
  parseTaskLeaseV3,
  parseWorkspaceContractV3,
  repositoryPathClaimsOverlapV3,
  type AgentId,
  type ArtifactContractTemplateV3,
  type ArtifactDescriptorV3,
  type ArtifactDigest,
  type ArtifactMaterializationGrantId,
  type AssignReviewSeatInputV3,
  type AssignReviewSeatResultV3,
  type BlockTaskInputV3,
  type BlockTaskResultV3,
  type ClaimTaskInputV3,
  type ClaimTaskResultV3,
  type ConsumeAcceptedArtifactInputV3,
  type ConsumeAcceptedArtifactResultV3,
  type MaterializedArtifactContractV3,
  type ProposeTaskGraphInputV3,
  type ProposeTaskGraphResultV3,
  type ProposedArtifactV3,
  type ReleaseTaskLeaseInputV3,
  type ReleaseTaskLeaseResultV3,
  type RenewTaskLeaseInputV3,
  type RenewTaskLeaseResultV3,
  type RepositoryId,
  type ReviewVerdictV3,
  type ServerId,
  type StagedArtifactMaterialV3,
  type TaskId,
  type TaskLaneRole,
  type TaskLeaseV3,
  type TaskStatus,
  type TaskTitlePrivacyPortV3,
  type Timestamp,
  type WorkspaceContractV3,
  type WorkspaceReservationId,
} from "@swarm/protocol";
import { assertPostgresWave3MigrationContract } from "../contracts.js";
import { storageFail } from "../errors.js";
import {
  WAVE3_POSTGRES_MIGRATION,
  WAVE3_POSTGRES_MIGRATION_CHECKSUM,
} from "../migrations.js";
import { PsqlSession, sqlLiteral } from "./session.js";

const SCHEMA = /^[a-z][a-z0-9_]{0,62}$/u;
const CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz";

function mint<Id extends string>(prefix: string): Id {
  const bytes = randomBytes(17);
  let bits = 0;
  let count = 0;
  let body = "";
  for (const byte of bytes) {
    bits = (bits << 8) | byte;
    count += 8;
    while (count >= 5 && body.length < 26) {
      count -= 5;
      body += CROCKFORD[(bits >>> count) & 31];
      bits &= (1 << count) - 1;
    }
  }
  while (body.length < 26) body += CROCKFORD[(bits << (5 - count)) & 31];
  return `${prefix}_${body}` as Id;
}

function digest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}
function utf8Digest(value: string): ArtifactDigest {
  return `sha256:${createHash("sha256").update(value, "utf8").digest("hex")}` as ArtifactDigest;
}
function json(value: unknown): string {
  return sqlLiteral(new TextDecoder().decode(canonicalProtocolJson(value)));
}
function timestampSql(column: string): string {
  return `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
}
function leaseJson(alias = "c"): string {
  return `json_build_object('protocolVersion',1,'serverId',g.server_id,'rootTaskId',${alias}.root_task_id,
    'taskId',${alias}.task_id,'claimId',${alias}.claim_id,'leaseId',${alias}.lease_id,
    'ownerAgentId',${alias}.owner_agent_id,'attempt',${alias}.attempt,'leaseEpoch',${alias}.lease_epoch,
    'leaseRevision',${alias}.lease_revision,'fenceToken',${alias}.fence_token,
    'acquiredAt',${timestampSql(`${alias}.acquired_at`)},'expiresAt',${timestampSql(`${alias}.expires_at`)},
    'taskRowVersion',${alias}.task_row_version,'graphRevision',${alias}.graph_revision)`;
}
function currentLeasePredicate(lease: TaskLeaseV3, alias = "c"): string {
  return `${alias}.claim_id=${sqlLiteral(lease.claimId)} AND ${alias}.task_id=${sqlLiteral(lease.taskId)}
    AND ${alias}.root_task_id=${sqlLiteral(lease.rootTaskId)} AND ${alias}.lease_id=${sqlLiteral(lease.leaseId)}
    AND ${alias}.owner_agent_id=${sqlLiteral(lease.ownerAgentId)} AND ${alias}.attempt=${sqlLiteral(lease.attempt)}
    AND ${alias}.lease_epoch=${sqlLiteral(lease.leaseEpoch)} AND ${alias}.lease_revision=${sqlLiteral(lease.leaseRevision)}
    AND ${alias}.fence_token=${sqlLiteral(lease.fenceToken)} AND ${alias}.task_row_version=${sqlLiteral(lease.taskRowVersion)}
    AND ${alias}.graph_revision=${sqlLiteral(lease.graphRevision)} AND ${alias}.closed_at IS NULL`;
}

type ReceiptRow = { requestDigest: string; result: unknown };

export type Wave3PgTestLatchName =
  | "graph_after_locked_snapshot"
  | "claim_after_repository_lock"
  | "renew_after_repository_lock"
  | "renew_before_claim_lock"
  | "renew_after_claim_lock_and_server_clock"
  | "block_after_repository_lock"
  | "review_block_after_repository_lock";
export interface Wave3PgTestLatchPort {
  reach(name: Wave3PgTestLatchName): Promise<void>;
}

class Wave3SessionOwner {
  readonly databaseUrl: string;
  readonly schema: string;
  readonly testLatches: Wave3PgTestLatchPort | undefined;
  constructor(
    databaseUrl: string,
    schema = "swarm_storage",
    testLatches?: Wave3PgTestLatchPort,
  ) {
    if (!SCHEMA.test(schema)) storageFail("INVALID_IDENTIFIER", schema);
    this.databaseUrl = databaseUrl;
    this.schema = schema;
    this.testLatches = testLatches;
  }
  protected async reachLatch(name: Wave3PgTestLatchName): Promise<void> {
    await this.testLatches?.reach(name);
  }
  async transaction<T>(
    operation: (session: PsqlSession) => Promise<T>,
  ): Promise<T> {
    const session = await PsqlSession.open(this.databaseUrl);
    try {
      await session.execute(
        `BEGIN; SET LOCAL search_path TO ${this.schema}, pg_catalog;`,
      );
      const result = await operation(session);
      await session.execute("COMMIT;");
      await session.close();
      return result;
    } catch (error) {
      await session.rollbackAndClose();
      throw error;
    }
  }
  async commandReplay<T>(
    session: PsqlSession,
    commandId: string,
    _commandKind: string,
    request: unknown,
    parse: (value: unknown) => T,
  ): Promise<T | undefined> {
    const requestDigest = digest(request);
    const existing =
      await session.queryJson<ReceiptRow | null>(`SELECT coalesce((SELECT json_build_object(
      'requestDigest',request_digest,'result',result_json) FROM task_v3_command_receipts
      WHERE command_id=${sqlLiteral(commandId)} FOR UPDATE),'null'::json);`);
    if (existing === null) return undefined;
    if (existing.requestDigest !== requestDigest)
      storageFail("IDEMPOTENCY_CONFLICT", commandId);
    return parse(existing.result);
  }
  async storeReceipt(
    session: PsqlSession,
    commandId: string,
    commandKind: string,
    request: unknown,
    result: unknown,
  ): Promise<void> {
    await session.execute(`INSERT INTO task_v3_command_receipts(command_id,request_digest,command_kind,result_json)
      VALUES(${sqlLiteral(commandId)},${sqlLiteral(digest(request))},${sqlLiteral(commandKind)},${json(result)}::jsonb);`);
  }
}

export class Wave3SchemaRepository extends Wave3SessionOwner {
  async migrate(): Promise<{ checksum: string; applied: boolean }> {
    assertPostgresWave3MigrationContract(WAVE3_POSTGRES_MIGRATION);
    return this.transaction(async (session) => {
      await session.execute(WAVE3_POSTGRES_MIGRATION, 60_000);
      return { checksum: WAVE3_POSTGRES_MIGRATION_CHECKSUM, applied: true };
    });
  }
}

export type RegisterRootTaskV3 = {
  serverId: ServerId;
  repositoryId: RepositoryId;
  repositoryDigest: ArtifactDigest;
  rootTaskId: TaskId;
  taskNumber: number;
  laneRole: TaskLaneRole;
  requiredCapabilities: string[];
  workspace: WorkspaceContractV3;
  artifactContract: MaterializedArtifactContractV3;
  titleDigest: ArtifactDigest;
  titleClassifierPolicyDigest: ArtifactDigest;
  graphPolicyDigest: ArtifactDigest;
  scenarioVersion: number;
};
export type RegisterTaskV3 = Omit<
  RegisterRootTaskV3,
  "repositoryDigest" | "graphPolicyDigest" | "scenarioVersion"
> & { parentTaskId: TaskId };

export class Wave3RegistryRepository extends Wave3SessionOwner {
  async registerRepository(input: {
    repositoryId: RepositoryId;
    serverId: ServerId;
    repositoryDigest: ArtifactDigest;
  }): Promise<void> {
    await this.transaction(async (session) =>
      session.execute(`INSERT INTO workspace_repositories(repository_id,server_id,repository_digest)
      VALUES(${sqlLiteral(input.repositoryId)},${sqlLiteral(input.serverId)},${sqlLiteral(input.repositoryDigest)})
      ON CONFLICT(repository_id) DO UPDATE SET repository_digest=excluded.repository_digest
      WHERE workspace_repositories.server_id=excluded.server_id;`),
    );
  }
  async registerAgentCapabilities(input: {
    agentId: AgentId;
    registryRevision: number;
    capabilityKeys: string[];
    current?: boolean;
  }): Promise<void> {
    const keys = [...input.capabilityKeys].sort();
    if (
      new Set(keys).size !== keys.length ||
      keys.length > TASK_V3_LIMITS.maxCapabilityKeys
    )
      storageFail("INVALID_IDENTIFIER", keys);
    await this.transaction(async (session) =>
      session.execute(`INSERT INTO agent_capabilities_v3(agent_id,registry_revision,capability_keys,current)
      VALUES(${sqlLiteral(input.agentId)},${sqlLiteral(input.registryRevision)},${json(keys)}::jsonb,${sqlLiteral(input.current ?? true)})
      ON CONFLICT(agent_id) DO UPDATE SET registry_revision=excluded.registry_revision,capability_keys=excluded.capability_keys,current=excluded.current;`),
    );
  }
  async registerArtifactTemplate(
    input: ArtifactContractTemplateV3,
  ): Promise<ArtifactDigest> {
    const template = parseArtifactContractTemplateV3(input),
      templateDigest = digest(template);
    await this.transaction(async (session) =>
      session.execute(`INSERT INTO artifact_contract_templates_v3(template_digest,template_id,template_json)
      VALUES(${sqlLiteral(templateDigest)},${sqlLiteral(template.templateId)},${json(template)}::jsonb)
      ON CONFLICT(template_digest) DO NOTHING;`),
    );
    return templateDigest;
  }
  async registerRootTask(input: RegisterRootTaskV3): Promise<void> {
    const workspace = parseWorkspaceContractV3(input.workspace),
      contract = parseMaterializedArtifactContractV3(input.artifactContract);
    if (
      workspace.taskId !== input.rootTaskId ||
      workspace.rootTaskId !== input.rootTaskId ||
      contract.taskId !== input.rootTaskId ||
      contract.rootTaskId !== input.rootTaskId ||
      !Number.isSafeInteger(input.scenarioVersion) ||
      input.scenarioVersion < 1
    )
      storageFail("WORKSPACE_CONTRACT_MISMATCH");
    await this.transaction(async (session) => {
      await session.execute(`INSERT INTO tasks(task_id,server_id,task_number,status,row_version) VALUES(${sqlLiteral(input.rootTaskId)},${sqlLiteral(input.serverId)},${sqlLiteral(input.taskNumber)},'todo',1) ON CONFLICT(task_id) DO NOTHING;
        INSERT INTO task_v3_graphs(root_task_id,server_id,repository_id,graph_revision,scenario_version,policy_digest,title_classifier_policy_digest)
          VALUES(${sqlLiteral(input.rootTaskId)},${sqlLiteral(input.serverId)},${sqlLiteral(input.repositoryId)},0,${sqlLiteral(input.scenarioVersion)},${sqlLiteral(input.graphPolicyDigest)},${sqlLiteral(input.titleClassifierPolicyDigest)});
        INSERT INTO workspace_contracts_v3(workspace_contract_id,task_id,root_task_id,repository_id,contract_digest,contract_json,workspace_generation,execution_mode)
          VALUES(${sqlLiteral(workspace.workspaceContractId)},${sqlLiteral(workspace.taskId)},${sqlLiteral(workspace.rootTaskId)},${sqlLiteral(workspace.repositoryId)},${sqlLiteral(digest(workspace))},${json(workspace)}::jsonb,${sqlLiteral(workspace.workspaceGeneration)},${sqlLiteral(workspace.executionMode)});
        INSERT INTO artifact_contracts_v3(artifact_contract_digest,artifact_contract_id,task_id,contract_json)
          VALUES(${sqlLiteral(digest(contract))},${sqlLiteral(contract.artifactContractId)},${sqlLiteral(contract.taskId)},${json(contract)}::jsonb);
        INSERT INTO task_v3_state(task_id,server_id,root_task_id,parent_task_id,repository_id,lane_role,status,row_version,required_capabilities,workspace_contract_digest,artifact_contract_digest,title_digest,title_classifier_policy_digest)
          VALUES(${sqlLiteral(input.rootTaskId)},${sqlLiteral(input.serverId)},${sqlLiteral(input.rootTaskId)},NULL,${sqlLiteral(input.repositoryId)},${sqlLiteral(input.laneRole)},'todo',1,${json([...input.requiredCapabilities].sort())}::jsonb,${sqlLiteral(digest(workspace))},${sqlLiteral(digest(contract))},${sqlLiteral(input.titleDigest)},${sqlLiteral(input.titleClassifierPolicyDigest)});`);
    });
  }
}

type ClaimState = {
  found: boolean;
  status?: TaskStatus;
  rowVersion?: number;
  rootTaskId?: string;
  serverId?: string;
  repositoryId?: string;
  graphRevision?: number;
  requiredCapabilities?: string[];
  workspace?: WorkspaceContractV3;
  laneRole?: TaskLaneRole;
  reviewAuthorized?: boolean;
};

export class TaskLeaseV3Repository extends Wave3SessionOwner {
  async claim(raw: ClaimTaskInputV3): Promise<ClaimTaskResultV3> {
    const input = parseClaimTaskInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "claim",
        input,
        parseClaimResult,
      );
      if (replay !== undefined) return replay;
      const locator = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id,'rootTaskId',root_task_id) FROM task_v3_state WHERE task_id=${sqlLiteral(input.taskId)}),'null'::json);`,
      );
      if (locator === null) return storageFail("TASK_NOT_READY", input.taskId);
      await session.execute(`SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(locator.repositoryId)} FOR UPDATE;`);
      await this.reachLatch("claim_after_repository_lock");
      await session.execute(`SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(locator.rootTaskId)} FOR UPDATE;
        SELECT 1 FROM task_v3_state WHERE root_task_id=${sqlLiteral(locator.rootTaskId)} ORDER BY task_id FOR UPDATE;`);
      const readState = () =>
        session.queryJson<ClaimState>(
          `SELECT json_build_object('found',true,'status',s.status,'rowVersion',s.row_version,'rootTaskId',s.root_task_id,'serverId',s.server_id,'repositoryId',s.repository_id,'graphRevision',g.graph_revision,'requiredCapabilities',s.required_capabilities,'workspace',w.contract_json,'laneRole',s.lane_role,'reviewAuthorized',s.lane_role<>'review' OR EXISTS(SELECT 1 FROM review_requirements_v3 r JOIN review_assignments_v3 a ON a.artifact_id=r.artifact_id AND a.seat_key=r.seat_key AND a.assignment_status='current' JOIN review_barriers_v3 b ON b.artifact_id=r.artifact_id AND b.state='open' WHERE r.review_task_id=s.task_id AND a.reviewer_agent_id=${sqlLiteral(input.agentId)} AND NOT EXISTS(SELECT 1 FROM review_verdicts_v3 v WHERE v.assignment_id=a.assignment_id))) FROM task_v3_state s JOIN task_v3_graphs g USING(root_task_id) JOIN workspace_contracts_v3 w USING(task_id) WHERE s.task_id=${sqlLiteral(input.taskId)};`,
        );
      let state = await readState();
      const open = await session.queryJson<{ lease: TaskLeaseV3 } | null>(
        `SELECT coalesce((SELECT json_build_object('lease',${leaseJson("c")}) FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE c.task_id=${sqlLiteral(input.taskId)} AND c.closed_at IS NULL FOR UPDATE),'null'::json);`,
      );
      const now = await session.queryJson<{ now: string }>(
        `SELECT json_build_object('now',${timestampSql("clock_timestamp()")});`,
      );
      let expiredFromExpectedImage = false;
      if (open !== null) {
        const lease = parseTaskLeaseV3(open.lease);
        if (lease.expiresAt <= now.now) {
          const expiredRowVersion = state.rowVersion;
          await this.expireLocked(session, lease);
          state = await readState();
          expiredFromExpectedImage =
            expiredRowVersion === input.expectedTaskRowVersion &&
            state.rowVersion === input.expectedTaskRowVersion + 1;
        } else
          return this.storeConflict(
            session,
            input,
            "TASK_CLAIM_CONFLICT",
            state.rowVersion ?? 0,
            lease.leaseEpoch,
          );
      }
      if (
        state.status !== "todo" ||
        (state.rowVersion !== input.expectedTaskRowVersion &&
          !expiredFromExpectedImage)
      )
        return this.storeConflict(
          session,
          input,
          "TASK_NOT_READY",
          state.rowVersion ?? 0,
          null,
        );
      if (state.laneRole === "review" && state.reviewAuthorized !== true)
        return this.storeConflict(
          session,
          input,
          "TASK_NOT_READY",
          state.rowVersion ?? 0,
          null,
        );
      if (state.graphRevision !== input.expectedGraphRevision)
        return this.storeConflict(
          session,
          input,
          "TASK_NOT_READY",
          state.rowVersion ?? 0,
          null,
        );
      if (
        state.workspace?.workspaceGeneration !==
        input.expectedWorkspaceGeneration
      )
        return this.storeConflict(
          session,
          input,
          "TASK_NOT_READY",
          state.rowVersion ?? 0,
          null,
        );
      const dependencies = await session.queryJson<{ ready: boolean }>(
        `SELECT json_build_object('ready',NOT EXISTS(SELECT 1 FROM task_v3_edges e JOIN task_v3_state p ON p.task_id=e.prerequisite_task_id WHERE e.root_task_id=${sqlLiteral(state.rootTaskId!)} AND e.edge_kind='depends_on' AND e.dependent_task_id=${sqlLiteral(input.taskId)} AND p.status<>'done'));`,
      );
      const children = await session.queryJson<{ ready: boolean }>(
        `SELECT json_build_object('ready',NOT EXISTS(SELECT 1 FROM task_v3_edges e JOIN task_v3_state c ON c.task_id=e.prerequisite_task_id WHERE e.root_task_id=${sqlLiteral(state.rootTaskId!)} AND e.edge_kind='contains' AND e.dependent_task_id=${sqlLiteral(input.taskId)} AND c.status<>'done'));`,
      );
      if (!dependencies.ready || !children.ready)
        return this.storeConflict(
          session,
          input,
          "TASK_NOT_READY",
          state.rowVersion ?? 0,
          null,
        );
      const capabilities = await session.queryJson<{
        current: boolean;
        keys: string[];
      } | null>(
        `SELECT coalesce((SELECT json_build_object('current',current,'keys',capability_keys) FROM agent_capabilities_v3 WHERE agent_id=${sqlLiteral(input.agentId)} FOR SHARE),'null'::json);`,
      );
      const required = state.requiredCapabilities ?? [];
      if (
        capabilities === null ||
        !capabilities.current ||
        required.some((k) => !capabilities.keys.includes(k))
      )
        return this.storeConflict(
          session,
          input,
          "TASK_CAPABILITY_MISMATCH",
          state.rowVersion ?? 0,
          null,
        );
      const claims = state.workspace?.pathClaims ?? [];
      const active = await session.queryJson<
        Array<{ kind: "file" | "subtree"; path: string }>
      >(
        `SELECT coalesce(json_agg(json_build_object('kind',path_kind,'path',path) ORDER BY path),'[]'::json) FROM workspace_reservations_v3 WHERE repository_id=${sqlLiteral(state.repositoryId!)} AND closed_at IS NULL;`,
      );
      if (
        claims.some((a) =>
          active.some((b) => repositoryPathClaimsOverlapV3(a, b)),
        )
      )
        return this.storeConflict(
          session,
          input,
          "TASK_NOT_READY",
          state.rowVersion ?? 0,
          null,
        );
      const maxima = await session.queryJson<{
        attempt: number;
        epoch: number;
      }>(
        `SELECT json_build_object('attempt',coalesce(max(attempt),0)+1,'epoch',coalesce(max(lease_epoch),0)+1) FROM task_claims_v3 WHERE task_id=${sqlLiteral(input.taskId)};`,
      );
      const claimId = mint<string>("clm"),
        leaseId = mint<string>("lse"),
        fenceToken = mint<string>("fnc");
      await session.execute(`UPDATE task_v3_state SET status='in_progress',row_version=row_version+1 WHERE task_id=${sqlLiteral(input.taskId)};
        INSERT INTO task_claims_v3(claim_id,root_task_id,task_id,lease_id,owner_agent_id,attempt,lease_epoch,lease_revision,fence_token,acquired_at,expires_at,task_row_version,graph_revision)
        VALUES(${sqlLiteral(claimId)},${sqlLiteral(state.rootTaskId!)},${sqlLiteral(input.taskId)},${sqlLiteral(leaseId)},${sqlLiteral(input.agentId)},${sqlLiteral(maxima.attempt)},${sqlLiteral(maxima.epoch)},1,${sqlLiteral(fenceToken)},clock_timestamp(),clock_timestamp()+interval '30 seconds',${sqlLiteral((state.rowVersion ?? 0) + 1)},${sqlLiteral(state.graphRevision!)});
        INSERT INTO task_attempt_ledgers_v3(task_id,attempt,ledger_revision) VALUES(${sqlLiteral(input.taskId)},${sqlLiteral(maxima.attempt)},1);
        INSERT INTO task_attempt_contributors_v3(task_id,attempt,agent_id,source,source_artifact_id) VALUES(${sqlLiteral(input.taskId)},${sqlLiteral(maxima.attempt)},${sqlLiteral(input.agentId)},'claim_owner',NULL);`);
      for (const path of claims) {
        await session.execute(
          `INSERT INTO workspace_reservations_v3(reservation_id,repository_id,task_id,claim_id,lease_id,attempt,lease_epoch,path_kind,path) VALUES(${sqlLiteral(mint<string>("rsv"))},${sqlLiteral(state.repositoryId!)},${sqlLiteral(input.taskId)},${sqlLiteral(claimId)},${sqlLiteral(leaseId)},${sqlLiteral(maxima.attempt)},${sqlLiteral(maxima.epoch)},${sqlLiteral(path.kind)},${sqlLiteral(path.path)});`,
        );
      }
      const lease = await session.queryJson<TaskLeaseV3>(
        `SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE c.claim_id=${sqlLiteral(claimId)};`,
      );
      const result = parseClaimResult({ kind: "claimed", lease });
      await this.storeReceipt(session, input.commandId, "claim", input, result);
      return result;
    });
  }
  private async storeConflict(
    session: PsqlSession,
    input: ClaimTaskInputV3,
    code: "TASK_CLAIM_CONFLICT" | "TASK_NOT_READY" | "TASK_CAPABILITY_MISMATCH",
    row: number,
    epoch: number | null,
  ): Promise<ClaimTaskResultV3> {
    const result: ClaimTaskResultV3 = {
      kind: "conflict",
      code,
      observedTaskRowVersion: row,
      observedLeaseEpoch: epoch,
    };
    await this.storeReceipt(session, input.commandId, "claim", input, result);
    return result;
  }
  private async expireLocked(
    session: PsqlSession,
    lease: TaskLeaseV3,
  ): Promise<void> {
    const result = await session.queryJson<{ changed: number }>(
      `WITH closed AS(UPDATE task_claims_v3 c SET closed_at=clock_timestamp(),close_reason='server_expired' WHERE ${currentLeasePredicate(lease)} AND expires_at<=clock_timestamp() RETURNING 1), reservations AS(UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='server_expired' WHERE claim_id=${sqlLiteral(lease.claimId)} AND lease_id=${sqlLiteral(lease.leaseId)} AND closed_at IS NULL AND EXISTS(SELECT 1 FROM closed) RETURNING 1), task AS(UPDATE task_v3_state SET status='todo',row_version=row_version+1 WHERE task_id=${sqlLiteral(lease.taskId)} AND status='in_progress' AND EXISTS(SELECT 1 FROM closed) RETURNING 1) SELECT json_build_object('changed',(SELECT count(*) FROM closed)+(SELECT count(*) FROM task));`,
    );
    if (result.changed !== 2)
      storageFail("TASK_LEASE_EXPIRY_ATOMICITY_VIOLATION", lease.taskId);
  }
  async renew(raw: RenewTaskLeaseInputV3): Promise<RenewTaskLeaseResultV3> {
    const input = parseRenewTaskLeaseInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "renew",
        input,
        parseRenewResult,
      );
      if (replay !== undefined) return replay;
      const expected = input.expectedLease;
      const locator = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id,'rootTaskId',root_task_id) FROM task_v3_state WHERE task_id=${sqlLiteral(expected.taskId)}),'null'::json);`,
      );
      if (locator !== null) {
        await session.execute(
          `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(locator.repositoryId)} FOR UPDATE;`,
        );
        await this.reachLatch("renew_after_repository_lock");
        await session.execute(
          `SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(locator.rootTaskId)} FOR UPDATE;`,
        );
      }
      await this.reachLatch("renew_before_claim_lock");
      const current = await session.queryJson<{ lease: TaskLeaseV3 } | null>(
        `SELECT coalesce((SELECT json_build_object('lease',${leaseJson("c")}) FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE c.task_id=${sqlLiteral(expected.taskId)} AND c.closed_at IS NULL FOR UPDATE),'null'::json);`,
      );
      await session.execute(
        `SELECT 1 FROM task_v3_state WHERE task_id=${sqlLiteral(expected.taskId)} FOR UPDATE;`,
      );
      const observedAt = await session.queryJson<{ now: string }>(
        `SELECT json_build_object('now',${timestampSql("clock_timestamp()")});`,
      );
      await this.reachLatch("renew_after_claim_lock_and_server_clock");
      const at = observedAt.now as Timestamp;
      let result: RenewTaskLeaseResultV3;
      if (current === null) {
        result = {
          kind: "rejected",
          code: "TASK_LEASE_RENEWAL_CONFLICT",
          serverObservedAt: at,
          retryNotBefore: null,
          observedLeaseRevision: null,
        };
      } else {
        const observed = parseTaskLeaseV3(current.lease);
        if (observed.expiresAt <= observedAt.now)
          result = {
            kind: "rejected",
            code: "TASK_LEASE_EXPIRED",
            serverObservedAt: at,
            retryNotBefore: null,
            observedLeaseRevision: observed.leaseRevision,
          };
        else if (digest(observed) !== digest(input.expectedLease))
          result = {
            kind: "rejected",
            code: "TASK_LEASE_RENEWAL_CONFLICT",
            serverObservedAt: at,
            retryNotBefore: null,
            observedLeaseRevision: observed.leaseRevision,
          };
        else {
          const retry = new Date(
            Date.parse(observed.expiresAt) - TASK_V3_LIMITS.renewalLeadMs,
          ).toISOString() as Timestamp;
          if (observedAt.now < retry)
            result = {
              kind: "rejected",
              code: "TASK_LEASE_RENEWAL_TOO_EARLY",
              serverObservedAt: at,
              retryNotBefore: retry,
              observedLeaseRevision: observed.leaseRevision,
            };
          else {
            const changed = await session.queryJson<{
              claims: number;
              tasks: number;
            }>(
              `WITH renewed AS(UPDATE task_claims_v3 c SET lease_revision=lease_revision+1,expires_at=clock_timestamp()+interval '30 seconds',task_row_version=task_row_version+1 WHERE ${currentLeasePredicate(observed)} AND expires_at>clock_timestamp() RETURNING 1), task AS(UPDATE task_v3_state SET row_version=row_version+1 WHERE task_id=${sqlLiteral(observed.taskId)} AND status='in_progress' AND row_version=${sqlLiteral(observed.taskRowVersion)} AND EXISTS(SELECT 1 FROM renewed) RETURNING 1) SELECT json_build_object('claims',(SELECT count(*) FROM renewed),'tasks',(SELECT count(*) FROM task));`,
            );
            if (changed.claims === 0) {
              const expiredAt = await session.queryJson<{ now: string }>(
                `SELECT json_build_object('now',${timestampSql("clock_timestamp()")});`,
              );
              result = {
                kind: "rejected",
                code: "TASK_LEASE_EXPIRED",
                serverObservedAt: expiredAt.now as Timestamp,
                retryNotBefore: null,
                observedLeaseRevision: observed.leaseRevision,
              };
            } else {
              if (changed.tasks !== 1)
                storageFail("TASK_LEASE_CLOCK_AUTHORITY_VIOLATION");
              const renewed = await session.queryJson<TaskLeaseV3>(
                `SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE c.claim_id=${sqlLiteral(observed.claimId)};`,
              );
              result = {
                kind: "renewed",
                previousLease: observed,
                currentLease: parseTaskLeaseV3(renewed),
                serverObservedAt: at,
              };
            }
          }
        }
      }
      await this.storeReceipt(session, input.commandId, "renew", input, result);
      return result;
    });
  }
  async release(
    raw: ReleaseTaskLeaseInputV3,
  ): Promise<ReleaseTaskLeaseResultV3> {
    const input = parseReleaseTaskLeaseInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "release",
        input,
        parseReleaseResult,
      );
      if (replay !== undefined) return replay;
      const lease = input.expectedLease;
      const locator = await session.queryJson<{ repositoryId: string } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)}),'null'::json);`,
      );
      if (locator === null) storageFail("TASK_LEASE_STALE");
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(locator.repositoryId)} FOR UPDATE; SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} FOR UPDATE;`,
      );
      const current = await session.queryJson<{
        lease: TaskLeaseV3;
        now: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('lease',${leaseJson("c")},'now',${timestampSql("clock_timestamp()")}) FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE c.task_id=${sqlLiteral(lease.taskId)} AND c.closed_at IS NULL FOR UPDATE),'null'::json);`,
      );
      if (
        current === null ||
        digest(parseTaskLeaseV3(current.lease)) !== digest(lease)
      )
        storageFail("TASK_LEASE_STALE");
      if (current.now >= lease.expiresAt) storageFail("TASK_LEASE_EXPIRED");
      const changed = await session.queryJson<{
        claims: number;
        tasks: number;
      }>(
        `WITH c AS(UPDATE task_claims_v3 c SET closed_at=clock_timestamp(),close_reason=${sqlLiteral(input.reason)} WHERE ${currentLeasePredicate(lease)} RETURNING 1),r AS(UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason=${sqlLiteral(input.reason)} WHERE claim_id=${sqlLiteral(lease.claimId)} AND lease_id=${sqlLiteral(lease.leaseId)} AND closed_at IS NULL AND EXISTS(SELECT 1 FROM c) RETURNING 1),t AS(UPDATE task_v3_state SET status='todo',row_version=row_version+1 WHERE task_id=${sqlLiteral(lease.taskId)} AND status='in_progress' AND row_version=${sqlLiteral(lease.taskRowVersion)} AND EXISTS(SELECT 1 FROM c) RETURNING row_version) SELECT json_build_object('claims',(SELECT count(*) FROM c),'tasks',(SELECT count(*) FROM t));`,
      );
      if (changed.claims !== 1 || changed.tasks !== 1)
        storageFail("TASK_LEASE_RELEASE_ATOMICITY_VIOLATION");
      const result: ReleaseTaskLeaseResultV3 = {
        kind: "released",
        closedLease: lease,
        reason: input.reason,
        previousTaskStatus: "in_progress",
        currentTaskStatus: "todo",
        currentTaskRowVersion: lease.taskRowVersion + 1,
      };
      await this.storeReceipt(
        session,
        input.commandId,
        "release",
        input,
        result,
      );
      return result;
    });
  }
}

function parseClaimResult(value: unknown): ClaimTaskResultV3 {
  const v = value as ClaimTaskResultV3;
  return v.kind === "claimed"
    ? { kind: "claimed", lease: parseTaskLeaseV3(v.lease) }
    : v;
}
function parseRenewResult(value: unknown): RenewTaskLeaseResultV3 {
  const v = value as RenewTaskLeaseResultV3;
  if (v.kind === "renewed")
    return {
      ...v,
      previousLease: parseTaskLeaseV3(v.previousLease),
      currentLease: parseTaskLeaseV3(v.currentLease),
    };
  return v;
}
function parseReleaseResult(value: unknown): ReleaseTaskLeaseResultV3 {
  const v = value as ReleaseTaskLeaseResultV3;
  return { ...v, closedLease: parseTaskLeaseV3(v.closedLease) };
}

export class WorkspaceReservationV3Repository extends Wave3SessionOwner {
  async listOpen(repositoryId: RepositoryId): Promise<
    Array<{
      reservationId: WorkspaceReservationId;
      taskId: TaskId;
      kind: "file" | "subtree";
      path: string;
    }>
  > {
    return this.transaction(async (session) =>
      session.queryJson(
        `SELECT coalesce(json_agg(json_build_object('reservationId',reservation_id,'taskId',task_id,'kind',path_kind,'path',path) ORDER BY path,reservation_id),'[]'::json) FROM workspace_reservations_v3 WHERE repository_id=${sqlLiteral(repositoryId)} AND closed_at IS NULL;`,
      ),
    );
  }
}

export class TaskGraphV3Repository extends Wave3SessionOwner {
  readonly titlePrivacyPort: TaskTitlePrivacyPortV3;
  constructor(
    databaseUrl: string,
    schema: string,
    titlePrivacyPort: TaskTitlePrivacyPortV3,
    testLatches?: Wave3PgTestLatchPort,
  ) {
    super(databaseUrl, schema, testLatches);
    this.titlePrivacyPort = titlePrivacyPort;
  }
  async propose(
    raw: ProposeTaskGraphInputV3,
  ): Promise<ProposeTaskGraphResultV3> {
    const input = parseProposeTaskGraphInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "graph",
        input,
        (x) => x as ProposeTaskGraphResultV3,
      );
      if (replay !== undefined) return replay;
      const lease = input.expectedLease;
      const location = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
        serverId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id,'rootTaskId',root_task_id,'serverId',server_id) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)}),'null'::json);`,
      );
      if (
        location === null ||
        location.rootTaskId !== input.rootTaskId ||
        location.rootTaskId !== lease.rootTaskId ||
        location.serverId !== lease.serverId
      )
        storageFail("TASK_GRAPH_SCOPE_VIOLATION");
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(location.repositoryId)} FOR UPDATE;SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(input.rootTaskId)} FOR UPDATE;SELECT 1 FROM task_v3_state WHERE root_task_id=${sqlLiteral(input.rootTaskId)} ORDER BY task_id FOR UPDATE;`,
      );
      await this.reachLatch("graph_after_locked_snapshot");
      const graph = await session.queryJson<{
        revision: number;
        policyDigest: string;
        titleDigest: string;
        serverId: string;
        repositoryId: string;
        nodes: number;
        edges: number;
      }>(
        `SELECT json_build_object('revision',g.graph_revision,'policyDigest',g.policy_digest,'titleDigest',g.title_classifier_policy_digest,'serverId',g.server_id,'repositoryId',g.repository_id,'nodes',(SELECT count(*) FROM task_v3_state WHERE root_task_id=g.root_task_id),'edges',(SELECT count(*) FROM task_v3_edges WHERE root_task_id=g.root_task_id)) FROM task_v3_graphs g WHERE root_task_id=${sqlLiteral(input.rootTaskId)};`,
      );
      if (
        graph.serverId !== lease.serverId ||
        graph.repositoryId !== location.repositoryId
      )
        storageFail("TASK_GRAPH_SCOPE_VIOLATION");
      const priorCoordination = await session.queryJson<{
        commandId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('commandId',command_id) FROM task_v3_coordinations WHERE committed_reply_message_id=${sqlLiteral(input.committedReplyMessageId)} FOR UPDATE),'null'::json);`,
      );
      if (priorCoordination !== null)
        storageFail("TASK_COORDINATION_ALREADY_COMMITTED");
      if (graph.revision !== input.expectedGraphRevision)
        storageFail("TASK_GRAPH_REVISION_STALE");
      if (
        graph.policyDigest !== input.expectedPolicyDigest ||
        graph.titleDigest !== input.expectedTitleClassifierPolicyDigest
      )
        storageFail("TASK_TITLE_POLICY_STALE");
      if (
        graph.nodes + input.children.length > TASK_V3_LIMITS.maxGraphNodes ||
        graph.edges + input.children.length + input.dependencies.length >
          TASK_V3_LIMITS.maxGraphEdges
      )
        storageFail("TASK_GRAPH_LIMIT_EXCEEDED");
      const current = await session.queryJson<TaskLeaseV3 | null>(
        `SELECT coalesce((SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE ${currentLeasePredicate(lease)} FOR UPDATE),'null'::json);`,
      );
      if (
        current === null ||
        digest(parseTaskLeaseV3(current)) !== digest(lease)
      )
        storageFail("TASK_LEASE_STALE");
      const predecessors = await session.queryJson<{
        source: boolean;
        reply: boolean;
      }>(
        `SELECT json_build_object('source',EXISTS(SELECT 1 FROM messages WHERE message_id=${sqlLiteral(input.sourceMessageId)} AND producer_fact_id=${sqlLiteral(input.sourceProducerFactId)}),'reply',EXISTS(SELECT 1 FROM messages m JOIN receipts r ON r.effect_message_id=m.message_id WHERE m.message_id=${sqlLiteral(input.committedReplyMessageId)} AND m.author_kind='agent' AND m.author_id=${sqlLiteral(lease.ownerAgentId)} AND m.parent_message_id=${sqlLiteral(input.sourceMessageId)} AND m.caused_by_producer_fact_id=${sqlLiteral(input.sourceProducerFactId)} AND r.producer_fact_id=${sqlLiteral(input.sourceProducerFactId)} AND r.turn_id=${sqlLiteral(input.sourceTurnId)} AND r.kind='side_effect_applied' AND r.effect_kind='reply_committed' AND r.actor_agent_id=${sqlLiteral(lease.ownerAgentId)}));`,
      );
      if (!predecessors.source || !predecessors.reply)
        storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED");
      for (const child of input.children) {
        const actualTitleDigest = utf8Digest(child.title);
        const validation = await this.titlePrivacyPort.validateTaskTitle({
          sourceMessageId: input.sourceMessageId,
          sourceProducerFactId: input.sourceProducerFactId,
          title: child.title,
          titleDigest: actualTitleDigest,
        });
        if (
          actualTitleDigest !== child.titleDigest ||
          validation.titleDigest !== actualTitleDigest ||
          validation.classifierPolicyDigest !== graph.titleDigest ||
          validation.classifierPolicyDigest !==
            input.expectedTitleClassifierPolicyDigest ||
          validation.classifierPolicyDigest !==
            child.titleClassifierPolicyDigest
        )
          storageFail("TASK_TITLE_POLICY_STALE");
      }
      const existingEdges = await session.queryJson<
        Array<{ from: string; to: string; kind: string }>
      >(
        `SELECT coalesce(json_agg(json_build_object('from',prerequisite_task_id,'to',dependent_task_id,'kind',edge_kind)),'[]'::json) FROM task_v3_edges WHERE root_task_id=${sqlLiteral(input.rootTaskId)};`,
      );
      const taskIds = new Map(
        input.children.map((c) => [c.clientKey, mint<TaskId>("tsk")]),
      );
      const resolve = (
        e:
          | { kind: "child"; clientKey: string }
          | { kind: "task"; taskId: TaskId },
      ) =>
        e.kind === "child"
          ? (taskIds.get(e.clientKey) ??
            storageFail("TASK_GRAPH_INPUT_INVALID", e.clientKey))
          : e.taskId;
      const taskRows = await session.queryJson<
        Array<{
          taskId: string;
          parentTaskId: string | null;
          status: TaskStatus;
          serverId: string;
          repositoryId: string;
        }>
      >(
        `SELECT coalesce(json_agg(json_build_object('taskId',task_id,'parentTaskId',parent_task_id,'status',status,'serverId',server_id,'repositoryId',repository_id) ORDER BY task_id),'[]'::json) FROM task_v3_state WHERE root_task_id=${sqlLiteral(input.rootTaskId)};`,
      );
      const rowByTask = new Map(taskRows.map((row) => [row.taskId, row]));
      const referencedExisting = new Set<string>();
      for (const dependency of input.dependencies)
        for (const endpoint of [
          dependency.prerequisite,
          dependency.dependent,
        ])
          if (endpoint.kind === "task") referencedExisting.add(endpoint.taskId);
      for (const child of input.children) {
        referencedExisting.add(child.workspace.integrationOwnerTaskId);
        if (
          child.workspace.executionMode !== "isolated_worktree" ||
          child.workspace.readArtifactDigest !== null ||
          child.workspace.repositoryId !== graph.repositoryId
        )
          storageFail("WORKSPACE_CONTRACT_MISMATCH");
      }
      for (const taskId of referencedExisting) {
        const row = rowByTask.get(taskId);
        if (
          row === undefined ||
          row.serverId !== graph.serverId ||
          row.repositoryId !== graph.repositoryId
        )
          storageFail("TASK_GRAPH_SCOPE_VIOLATION", taskId);
      }
      const knownCapabilities = await session.queryJson<string[]>(
        `SELECT coalesce(json_agg(DISTINCT caps.capability ORDER BY caps.capability),'[]'::json) FROM agent_capabilities_v3, LATERAL jsonb_array_elements_text(capability_keys) AS caps(capability) WHERE current;`,
      );
      if (
        input.children.some((child) =>
          child.requiredCapabilities.some(
            (capability) => !knownCapabilities.includes(capability),
          ),
        )
      )
        storageFail("TASK_GRAPH_INPUT_INVALID");
      const waits = existingEdges.map((edge) => [edge.from, edge.to]);
      for (const child of input.children)
        waits.push([taskIds.get(child.clientKey)!, lease.taskId]);
      const proposedDependencyKeys = new Set<string>();
      for (const dep of input.dependencies) {
        const from = resolve(dep.prerequisite);
        const to = resolve(dep.dependent);
        const key = `${from}\0${to}`;
        if (
          proposedDependencyKeys.has(key) ||
          existingEdges.some(
            (edge) =>
              edge.kind === "depends_on" && edge.from === from && edge.to === to,
          )
        )
          storageFail("TASK_GRAPH_INPUT_INVALID", key);
        proposedDependencyKeys.add(key);
        waits.push([from, to]);
      }
      if (hasCycle(waits)) storageFail("TASK_GRAPH_CYCLE");
      const depthOf = (taskId: string): number => {
        let currentTaskId: string | null = taskId;
        let depth = 0;
        const seen = new Set<string>();
        while (currentTaskId !== null) {
          if (seen.has(currentTaskId)) storageFail("TASK_GRAPH_CYCLE");
          seen.add(currentTaskId);
          const row = rowByTask.get(currentTaskId);
          if (row === undefined) storageFail("TASK_GRAPH_SCOPE_VIOLATION");
          currentTaskId = row.parentTaskId;
          if (currentTaskId !== null) depth += 1;
        }
        return depth;
      };
      if (
        depthOf(lease.taskId) + 1 > TASK_V3_LIMITS.maxGraphDepth
      )
        storageFail("TASK_GRAPH_LIMIT_EXCEEDED");
      const dependencyCounts = new Map<string, number>();
      for (const edge of existingEdges)
        if (edge.kind === "depends_on")
          dependencyCounts.set(
            edge.to,
            (dependencyCounts.get(edge.to) ?? 0) + 1,
          );
      for (const dep of input.dependencies) {
        const dependent = resolve(dep.dependent);
        dependencyCounts.set(
          dependent,
          (dependencyCounts.get(dependent) ?? 0) + 1,
        );
      }
      if (
        [...dependencyCounts.values()].some(
          (count) => count > TASK_V3_LIMITS.maxDependenciesPerTask,
        )
      )
        storageFail("TASK_GRAPH_LIMIT_EXCEEDED");
      const prospectiveStatus = new Map<string, TaskStatus>(
        taskRows.map((row) => [row.taskId, row.status]),
      );
      prospectiveStatus.set(lease.taskId, "waiting");
      for (const taskId of taskIds.values()) prospectiveStatus.set(taskId, "todo");
      const prospectiveEdges = [
        ...existingEdges,
        ...[...taskIds.values()].map((taskId) => ({
          from: taskId,
          to: lease.taskId,
          kind: "contains",
        })),
        ...input.dependencies.map((dep) => ({
          from: resolve(dep.prerequisite),
          to: resolve(dep.dependent),
          kind: "depends_on",
        })),
      ];
      const readyLeaves = [...prospectiveStatus].filter(
        ([taskId, status]) =>
          status === "todo" &&
          !prospectiveEdges.some(
            (edge) =>
              edge.to === taskId &&
              prospectiveStatus.get(edge.from) !== "done",
          ),
      ).length;
      if (readyLeaves > TASK_V3_LIMITS.maxOpenLeaves)
        storageFail("TASK_GRAPH_LIMIT_EXCEEDED");
      for (let a = 0; a < input.children.length; a += 1)
        for (let b = a + 1; b < input.children.length; b += 1)
          for (const x of input.children[a]!.workspace.pathClaims)
            for (const y of input.children[b]!.workspace.pathClaims)
              if (repositoryPathClaimsOverlapV3(x, y))
                storageFail("WORKSPACE_PATH_CONFLICT");
      const maxNumber = await session.queryJson<{ next: number }>(
        `SELECT json_build_object('next',coalesce(max(task_number),0)+1) FROM tasks WHERE server_id=${sqlLiteral(lease.serverId)};`,
      );
      const preparedChildren: Array<{
        child: (typeof input.children)[number];
        taskId: TaskId;
        taskNumber: number;
        workspace: WorkspaceContractV3;
        workspaceDigest: ArtifactDigest;
        artifactContract: MaterializedArtifactContractV3;
        artifactContractDigest: ArtifactDigest;
      }> = [];
      let offset = 0;
      for (const child of input.children) {
        const taskId = taskIds.get(child.clientKey)!;
        const taskNumber = maxNumber.next + offset++;
        const workspaceId = mint<string>("wsc");
        const workspace: WorkspaceContractV3 = {
          ...child.workspace,
          protocolVersion: lease.protocolVersion,
          workspaceContractId: workspaceId as never,
          taskId,
          rootTaskId: input.rootTaskId,
        };
        const workspaceDigest = digest(workspace);
        const template =
          await session.queryJson<ArtifactContractTemplateV3 | null>(
            `SELECT coalesce((SELECT template_json FROM artifact_contract_templates_v3 WHERE template_digest=${sqlLiteral(child.workspace.artifactContractTemplateDigest)}),'null'::jsonb);`,
          );
        if (template === null) storageFail("ARTIFACT_CONTRACT_MISMATCH");
        const artifactContract: MaterializedArtifactContractV3 = {
          protocolVersion: lease.protocolVersion,
          artifactContractId: mint<string>("acc") as never,
          templateDigest: child.workspace.artifactContractTemplateDigest,
          serverId: lease.serverId,
          rootTaskId: input.rootTaskId,
          taskId,
          workspaceContractId: workspace.workspaceContractId,
          workspaceContractDigest: workspaceDigest,
          baseCommit: workspace.baseCommit,
          baseTree: workspace.baseTree,
          pathClaimsDigest: digest(workspace.pathClaims),
          integrationOwnerTaskId: workspace.integrationOwnerTaskId,
          allowedKinds: template.allowedKinds,
          maxMaterialBytes: template.maxMaterialBytes,
          scopePolicy: "exact_workspace_claims",
          receiptPolicyDigest: template.receiptPolicyDigest,
          requiredReviewSeats: template.requiredReviewSeats,
          gate3PlanDigest: input.expectedPolicyDigest,
          policyDigest: template.policyDigest,
        };
        const artifactContractDigest = digest(artifactContract);
        preparedChildren.push({
          child,
          taskId,
          taskNumber,
          workspace,
          workspaceDigest,
          artifactContract,
          artifactContractDigest,
        });
      }
      await session.execute(
        `INSERT INTO task_v3_coordinations(committed_reply_message_id,command_id,root_task_id,task_id,source_message_id,source_producer_fact_id,source_turn_id,current_owner_agent_id) VALUES(${sqlLiteral(input.committedReplyMessageId)},${sqlLiteral(input.commandId)},${sqlLiteral(input.rootTaskId)},${sqlLiteral(lease.taskId)},${sqlLiteral(input.sourceMessageId)},${sqlLiteral(input.sourceProducerFactId)},${sqlLiteral(input.sourceTurnId)},${sqlLiteral(lease.ownerAgentId)});`,
      );
      const resultChildren: ProposeTaskGraphResultV3["children"] = [];
      for (const prepared of preparedChildren) {
        const {
          child,
          taskId,
          taskNumber,
          workspace,
          workspaceDigest,
          artifactContract,
          artifactContractDigest,
        } = prepared;
        await session.execute(
          `INSERT INTO tasks(task_id,server_id,task_number,status,row_version) VALUES(${sqlLiteral(taskId)},${sqlLiteral(lease.serverId)},${sqlLiteral(taskNumber)},'todo',1);INSERT INTO workspace_contracts_v3(workspace_contract_id,task_id,root_task_id,repository_id,contract_digest,contract_json,workspace_generation,execution_mode) VALUES(${sqlLiteral(workspace.workspaceContractId)},${sqlLiteral(taskId)},${sqlLiteral(input.rootTaskId)},${sqlLiteral(workspace.repositoryId)},${sqlLiteral(workspaceDigest)},${json(workspace)}::jsonb,${sqlLiteral(workspace.workspaceGeneration)},${sqlLiteral(workspace.executionMode)});INSERT INTO artifact_contracts_v3(artifact_contract_digest,artifact_contract_id,task_id,contract_json) VALUES(${sqlLiteral(artifactContractDigest)},${sqlLiteral(artifactContract.artifactContractId)},${sqlLiteral(taskId)},${json(artifactContract)}::jsonb);INSERT INTO task_v3_state(task_id,server_id,root_task_id,parent_task_id,repository_id,lane_role,status,row_version,required_capabilities,workspace_contract_digest,artifact_contract_digest,title_digest,title_classifier_policy_digest) VALUES(${sqlLiteral(taskId)},${sqlLiteral(lease.serverId)},${sqlLiteral(input.rootTaskId)},${sqlLiteral(lease.taskId)},${sqlLiteral(workspace.repositoryId)},${sqlLiteral(child.laneRole)},'todo',1,${json(child.requiredCapabilities)}::jsonb,${sqlLiteral(workspaceDigest)},${sqlLiteral(artifactContractDigest)},${sqlLiteral(child.titleDigest)},${sqlLiteral(child.titleClassifierPolicyDigest)});INSERT INTO task_v3_edges(root_task_id,prerequisite_task_id,dependent_task_id,edge_kind) VALUES(${sqlLiteral(input.rootTaskId)},${sqlLiteral(taskId)},${sqlLiteral(lease.taskId)},'contains');`,
        );
        resultChildren.push({
          clientKey: child.clientKey,
          taskId,
          taskNumber,
          status: "todo",
          titleDigest: child.titleDigest,
          titleClassifierPolicyDigest: child.titleClassifierPolicyDigest,
        });
      }
      for (const dep of input.dependencies)
        await session.execute(
          `INSERT INTO task_v3_edges(root_task_id,prerequisite_task_id,dependent_task_id,edge_kind) VALUES(${sqlLiteral(input.rootTaskId)},${sqlLiteral(resolve(dep.prerequisite))},${sqlLiteral(resolve(dep.dependent))},'depends_on');`,
        );
      await session.execute(
        `UPDATE task_v3_graphs SET graph_revision=graph_revision+1,policy_digest=${sqlLiteral(input.expectedPolicyDigest)},title_classifier_policy_digest=${sqlLiteral(input.expectedTitleClassifierPolicyDigest)} WHERE root_task_id=${sqlLiteral(input.rootTaskId)};UPDATE task_v3_state SET status='waiting',row_version=row_version+1 WHERE task_id=${sqlLiteral(lease.taskId)};UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='graph_expanded' WHERE claim_id=${sqlLiteral(lease.claimId)};UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='graph_expanded' WHERE claim_id=${sqlLiteral(lease.claimId)} AND closed_at IS NULL;`,
      );
      const result: ProposeTaskGraphResultV3 = {
        rootTaskId: input.rootTaskId,
        previousGraphRevision: graph.revision,
        currentGraphRevision: graph.revision + 1,
        policyDigest: input.expectedPolicyDigest,
        children: resultChildren,
        proposerStatus: "waiting",
      };
      await this.storeReceipt(session, input.commandId, "graph", input, result);
      return result;
    });
  }
  async block(raw: BlockTaskInputV3): Promise<BlockTaskResultV3> {
    const input = parseBlockTaskInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "block",
        input,
        (x) => x as BlockTaskResultV3,
      );
      if (replay !== undefined) return replay;
      const lease = input.expectedLease;
      const repo = await session.queryJson<{ id: string }>(
        `SELECT json_build_object('id',repository_id) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)};`,
      );
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(repo.id)} FOR UPDATE;`,
      );
      await this.reachLatch("block_after_repository_lock");
      await session.execute(
        `SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} FOR UPDATE;SELECT 1 FROM task_v3_state WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} ORDER BY task_id FOR UPDATE;`,
      );
      const current = await session.queryJson<TaskLeaseV3 | null>(
        `SELECT coalesce((SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE ${currentLeasePredicate(lease)} FOR UPDATE),'null'::json);`,
      );
      if (current === null) storageFail("TASK_LEASE_STALE");
      const edges = await session.queryJson<
        Array<{ from: string; to: string }>
      >(
        `SELECT coalesce(json_agg(json_build_object('from',prerequisite_task_id,'to',dependent_task_id)),'[]'::json) FROM task_v3_edges WHERE root_task_id=${sqlLiteral(lease.rootTaskId)};`,
      );
      const closure = reachable(lease.taskId, edges);
      const rows = await session.queryJson<
        Array<{
          taskId: TaskId;
          status: "todo" | "in_progress" | "waiting";
          rowVersion: number;
        }>
      >(
        `SELECT coalesce(json_agg(json_build_object('taskId',task_id,'status',status,'rowVersion',row_version) ORDER BY task_id),'[]'::json) FROM task_v3_state WHERE task_id IN (${closure.map(sqlLiteral).join(",")}) AND status IN ('todo','in_progress','waiting');`,
      );
      const affected: BlockTaskResultV3["affected"] = [];
      for (const row of rows) {
        const claim = await session.queryJson<{
          claimId: string;
          leaseId: string;
        } | null>(
          `SELECT coalesce((SELECT json_build_object('claimId',claim_id,'leaseId',lease_id) FROM task_claims_v3 WHERE task_id=${sqlLiteral(row.taskId)} AND closed_at IS NULL FOR UPDATE),'null'::json);`,
        );
        const reservations =
          claim === null
            ? []
            : await session.queryJson<WorkspaceReservationId[]>(
                `SELECT coalesce(json_agg(reservation_id ORDER BY reservation_id),'[]'::json) FROM workspace_reservations_v3 WHERE claim_id=${sqlLiteral(claim.claimId)} AND closed_at IS NULL;`,
              );
        if (claim !== null) {
          await session.execute(
            `UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='graph_blocked' WHERE claim_id=${sqlLiteral(claim.claimId)};UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='graph_blocked' WHERE claim_id=${sqlLiteral(claim.claimId)} AND closed_at IS NULL;`,
          );
        }
        await session.execute(
          `UPDATE task_v3_state SET status='blocked',row_version=row_version+1,terminal_reason='execution_failed' WHERE task_id=${sqlLiteral(row.taskId)};`,
        );
        affected.push({
          taskId: row.taskId,
          previousStatus: row.status,
          currentStatus: "blocked",
          previousTaskRowVersion: row.rowVersion,
          currentTaskRowVersion: row.rowVersion + 1,
          closedClaimId: (claim?.claimId as never) ?? null,
          closedLeaseId: (claim?.leaseId as never) ?? null,
          leaseCloseReason: claim === null ? null : "graph_blocked",
          closedReservationIds: reservations,
        });
      }
      const rev = await session.queryJson<{
        previous: number;
        current: number;
      }>(
        `WITH x AS(UPDATE task_v3_graphs SET graph_revision=graph_revision+1 WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} RETURNING graph_revision) SELECT json_build_object('previous',graph_revision-1,'current',graph_revision) FROM x;`,
      );
      const result: BlockTaskResultV3 = {
        kind: "blocked",
        rootCauseTaskId: lease.taskId,
        previousGraphRevision: rev.previous,
        currentGraphRevision: rev.current,
        affected,
      };
      await this.storeReceipt(session, input.commandId, "block", input, result);
      return result;
    });
  }
}

function hasCycle(edges: string[][]): boolean {
  const graph = new Map<string, string[]>();
  for (const [from, to] of edges) {
    const out = graph.get(from!) ?? [];
    out.push(to!);
    graph.set(from!, out);
  }
  const visiting = new Set<string>(),
    done = new Set<string>();
  const visit = (n: string): boolean => {
    if (visiting.has(n)) return true;
    if (done.has(n)) return false;
    visiting.add(n);
    for (const m of graph.get(n) ?? []) if (visit(m)) return true;
    visiting.delete(n);
    done.add(n);
    return false;
  };
  return [...graph.keys()].some(visit);
}
function reachable(
  root: string,
  edges: Array<{ from: string; to: string }>,
): string[] {
  const out = new Map<string, string[]>();
  for (const e of edges) {
    const a = out.get(e.from) ?? [];
    a.push(e.to);
    out.set(e.from, a);
  }
  const seen = new Set([root]),
    queue = [root];
  while (queue.length) {
    for (const next of out.get(queue.shift()!) ?? [])
      if (!seen.has(next)) {
        seen.add(next);
        queue.push(next);
      }
  }
  return [...seen].sort();
}

export class ArtifactV3Repository extends Wave3SessionOwner {
  async sealStagedMaterial(
    raw: StagedArtifactMaterialV3,
  ): Promise<StagedArtifactMaterialV3> {
    const material = parseStagedArtifactMaterialV3(raw);
    await this.transaction(async (session) =>
      session.execute(
        `INSERT INTO staged_artifact_materials_v3(staged_object_id,role,material_kind,media_type,byte_length,sha256,prerequisite_commit,expires_at) VALUES(${sqlLiteral(material.stagedObjectId)},${sqlLiteral(material.role)},${sqlLiteral(material.kind)},${sqlLiteral(material.mediaType)},${sqlLiteral(material.byteLength)},${sqlLiteral(material.sha256)},${sqlLiteral(material.prerequisiteCommit)},${sqlLiteral(material.expiresAt)}::timestamptz) ON CONFLICT(staged_object_id) DO NOTHING;`,
      ),
    );
    return material;
  }
  async publish(raw: {
    commandId: string;
    expectedLease: TaskLeaseV3;
    expectedTaskStatus: "in_progress";
    expectedTaskRowVersion: number;
    artifact: ProposedArtifactV3;
  }): Promise<ArtifactDescriptorV3> {
    const input = parsePublishArtifactInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "publish",
        input,
        (x) => x as ArtifactDescriptorV3,
      );
      if (replay !== undefined) return replay;
      const lease = parseTaskLeaseV3(input.expectedLease);
      const location = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id,'rootTaskId',root_task_id) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)}),'null'::json);`,
      );
      if (location === null || location.rootTaskId !== lease.rootTaskId)
        storageFail("TASK_GRAPH_SCOPE_VIOLATION");
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(location.repositoryId)} FOR UPDATE;SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} FOR UPDATE;SELECT 1 FROM task_v3_state WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} ORDER BY task_id FOR UPDATE;`,
      );
      const current = await session.queryJson<TaskLeaseV3 | null>(
        `SELECT coalesce((SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE ${currentLeasePredicate(lease)} FOR UPDATE),'null'::json);`,
      );
      if (current === null) storageFail("TASK_LEASE_STALE");
      const task = await session.queryJson<{
        status: string;
        rowVersion: number;
        workspaceDigest: string;
        artifactContractDigest: string;
        scenarioVersion: number;
        workspace: WorkspaceContractV3;
      }>(
        `SELECT json_build_object('status',s.status,'rowVersion',s.row_version,'workspaceDigest',s.workspace_contract_digest,'artifactContractDigest',s.artifact_contract_digest,'scenarioVersion',g.scenario_version,'workspace',w.contract_json) FROM task_v3_state s JOIN task_v3_graphs g USING(root_task_id) JOIN workspace_contracts_v3 w USING(task_id) WHERE s.task_id=${sqlLiteral(lease.taskId)} FOR UPDATE;`,
      );
      if (
        task.status !== input.expectedTaskStatus ||
        task.rowVersion !== input.expectedTaskRowVersion ||
        task.rowVersion !== lease.taskRowVersion
      )
        storageFail("TASK_MUTATION_FENCE_REQUIRED");
      const contract =
        await session.queryJson<MaterializedArtifactContractV3 | null>(
          `SELECT coalesce((SELECT contract_json FROM artifact_contracts_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND artifact_contract_digest=${sqlLiteral(input.artifact.artifactContractDigest)}),'null'::jsonb);`,
        );
      if (contract === null) storageFail("ARTIFACT_CONTRACT_MISMATCH");
      if (
        task.workspaceDigest !== input.artifact.workspaceContractDigest ||
        task.artifactContractDigest !== input.artifact.artifactContractDigest ||
        contract.workspaceContractDigest !==
          input.artifact.workspaceContractDigest ||
        contract.gate3PlanDigest !== input.artifact.gate3PlanDigest ||
        !contract.allowedKinds.includes(input.artifact.identity.kind) ||
        input.artifact.stagedMaterial.byteLength > contract.maxMaterialBytes
      )
        storageFail("ARTIFACT_CONTRACT_MISMATCH");
      if (digest(input.artifact.identity) !== input.artifact.artifactDigest)
        storageFail("ARTIFACT_DIGEST_MISMATCH");
      const objects = await session.queryJson<
        Array<{
          id: string;
          role: string;
          sha: string;
          bytes: number;
          kind: string;
          media: string;
          prerequisite: string | null;
        }>
      >(
        `SELECT coalesce(json_agg(json_build_object('id',staged_object_id,'role',role,'sha',sha256,'bytes',byte_length,'kind',material_kind,'media',media_type,'prerequisite',prerequisite_commit)),'[]'::json) FROM staged_artifact_materials_v3 WHERE staged_object_id IN (${[input.artifact.stagedMaterial.stagedObjectId, input.artifact.stagedScopeManifest.stagedObjectId, input.artifact.stagedAcceptanceReceipt.stagedObjectId].map(sqlLiteral).join(",")}) AND sealed AND expires_at>clock_timestamp();`,
      );
      if (objects.length !== 3) storageFail("ARTIFACT_MATERIAL_UNAVAILABLE");
      const stagedById = new Map(objects.map((object) => [object.id, object]));
      for (const proposed of [
        input.artifact.stagedMaterial,
        input.artifact.stagedScopeManifest,
        input.artifact.stagedAcceptanceReceipt,
      ]) {
        const sealed = stagedById.get(proposed.stagedObjectId);
        if (
          sealed === undefined ||
          sealed.role !== proposed.role ||
          sealed.sha !== proposed.sha256 ||
          sealed.bytes !== proposed.byteLength ||
          sealed.kind !== proposed.kind ||
          sealed.media !== proposed.mediaType ||
          sealed.prerequisite !== proposed.prerequisiteCommit
        )
          storageFail("ARTIFACT_MATERIAL_UNAVAILABLE");
      }
      const existing = await session.queryJson<ArtifactDescriptorV3 | null>(
        `SELECT coalesce((SELECT descriptor_json FROM artifacts_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)} FOR UPDATE),'null'::jsonb);`,
      );
      if (existing !== null) storageFail("ARTIFACT_IMMUTABLE_CONFLICT");
      const ledgerAgents = await session.queryJson<AgentId[]>(
        `SELECT coalesce(json_agg(agent_id ORDER BY agent_id),'[]'::json) FROM task_attempt_contributors_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)};`,
      );
      const sourceIds = await session.queryJson<any[]>(
        `SELECT coalesce(json_agg(source_artifact_id ORDER BY source_artifact_id) FILTER(WHERE source_artifact_id IS NOT NULL),'[]'::json) FROM task_attempt_contributors_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)};`,
      );
      const artifactId = mint<string>("art");
      const claims = parseArtifactClaimsV3({
        protocolVersion: input.artifact.protocolVersion,
        workspaceContractDigest: input.artifact.workspaceContractDigest,
        artifactContractDigest: input.artifact.artifactContractDigest,
        gate3PlanDigest: input.artifact.gate3PlanDigest,
        identity: input.artifact.identity,
        artifactDigest: input.artifact.artifactDigest,
        scopeManifestDigest: input.artifact.scopeManifestDigest,
        acceptanceReceiptDigest: input.artifact.acceptanceReceiptDigest,
      });
      const descriptor = parseArtifactDescriptorV3({
        ...claims,
        artifactId: artifactId as never,
        materialObjectId: input.artifact.stagedMaterial.stagedObjectId,
        materialKind: input.artifact.stagedMaterial.kind as
          | "prerequisite_bound_git_bundle"
          | "digest_blob",
        materialMediaType: input.artifact.stagedMaterial.mediaType,
        materialByteLength: input.artifact.stagedMaterial.byteLength,
        materialSha256: input.artifact.stagedMaterial.sha256,
        materialPrerequisiteCommit:
          input.artifact.stagedMaterial.prerequisiteCommit,
        scopeManifestObjectId:
          input.artifact.stagedScopeManifest.stagedObjectId,
        scopeManifestSha256: input.artifact.stagedScopeManifest.sha256,
        scopeManifestByteLength: input.artifact.stagedScopeManifest.byteLength,
        acceptanceReceiptObjectId:
          input.artifact.stagedAcceptanceReceipt.stagedObjectId,
        acceptanceReceiptSha256: input.artifact.stagedAcceptanceReceipt.sha256,
        acceptanceReceiptByteLength:
          input.artifact.stagedAcceptanceReceipt.byteLength,
        consumedSourceArtifactIds: sourceIds,
        rootTaskId: lease.rootTaskId,
        taskId: lease.taskId,
        attempt: lease.attempt,
        builderAgentId: lease.ownerAgentId,
        contributorAgentIds: ledgerAgents.filter(
          (a) => a !== lease.ownerAgentId,
        ),
      });
      await session.execute(
        `INSERT INTO artifacts_v3(artifact_id,artifact_digest,root_task_id,task_id,attempt,builder_agent_id,artifact_contract_digest,descriptor_json,material_object_id,scope_manifest_object_id,acceptance_receipt_object_id) VALUES(${sqlLiteral(artifactId)},${sqlLiteral(descriptor.artifactDigest)},${sqlLiteral(lease.rootTaskId)},${sqlLiteral(lease.taskId)},${sqlLiteral(lease.attempt)},${sqlLiteral(lease.ownerAgentId)},${sqlLiteral(descriptor.artifactContractDigest)},${json(descriptor)}::jsonb,${sqlLiteral(descriptor.materialObjectId)},${sqlLiteral(descriptor.scopeManifestObjectId)},${sqlLiteral(descriptor.acceptanceReceiptObjectId)});INSERT INTO review_barriers_v3(artifact_id,state) VALUES(${sqlLiteral(artifactId)},'open');UPDATE task_v3_state SET status='in_review',row_version=row_version+1 WHERE task_id=${sqlLiteral(lease.taskId)};UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='artifact_published' WHERE claim_id=${sqlLiteral(lease.claimId)};UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='artifact_published' WHERE claim_id=${sqlLiteral(lease.claimId)} AND closed_at IS NULL;`,
      );
      let n = 0;
      for (const seat of contract.requiredReviewSeats) {
        const reviewTaskId = mint<string>("tsk"),
          taskNumber = await session.queryJson<{ n: number }>(
            `SELECT json_build_object('n',coalesce(max(task_number),0)+1) FROM tasks WHERE server_id=${sqlLiteral(lease.serverId)};`,
          );
        const reviewWorkspace = parseWorkspaceContractV3({
          ...task.workspace,
          workspaceContractId: mint<string>("wsc"),
          taskId: reviewTaskId,
          rootTaskId: lease.rootTaskId,
          workspaceGeneration: 1,
          executionMode: "read_only_artifact",
          pathClaims: [],
          readArtifactDigest: descriptor.artifactDigest,
        });
        const reviewWorkspaceDigest = digest(reviewWorkspace);
        await session.execute(
          `INSERT INTO tasks(task_id,server_id,task_number,status,row_version) VALUES(${sqlLiteral(reviewTaskId)},${sqlLiteral(lease.serverId)},${sqlLiteral(taskNumber.n + n++)},'todo',1);INSERT INTO workspace_contracts_v3(workspace_contract_id,task_id,root_task_id,repository_id,contract_digest,contract_json,workspace_generation,execution_mode) VALUES(${sqlLiteral(reviewWorkspace.workspaceContractId)},${sqlLiteral(reviewTaskId)},${sqlLiteral(lease.rootTaskId)},${sqlLiteral(reviewWorkspace.repositoryId)},${sqlLiteral(reviewWorkspaceDigest)},${json(reviewWorkspace)}::jsonb,${sqlLiteral(reviewWorkspace.workspaceGeneration)},'read_only_artifact');INSERT INTO task_v3_state(task_id,server_id,root_task_id,parent_task_id,repository_id,lane_role,status,row_version,required_capabilities,workspace_contract_digest,artifact_contract_digest,title_digest,title_classifier_policy_digest) SELECT ${sqlLiteral(reviewTaskId)},server_id,root_task_id,task_id,repository_id,'review','todo',1,'[]'::jsonb,${sqlLiteral(reviewWorkspaceDigest)},artifact_contract_digest,title_digest,title_classifier_policy_digest FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)};INSERT INTO review_requirements_v3(artifact_id,seat_key,artifact_digest,artifact_contract_digest,gate3_plan_digest,scenario_version,review_task_id) VALUES(${sqlLiteral(artifactId)},${sqlLiteral(seat)},${sqlLiteral(descriptor.artifactDigest)},${sqlLiteral(descriptor.artifactContractDigest)},${sqlLiteral(descriptor.gate3PlanDigest)},${sqlLiteral(task.scenarioVersion)},${sqlLiteral(reviewTaskId)});`,
        );
      }
      await this.storeReceipt(
        session,
        input.commandId,
        "publish",
        input,
        descriptor,
      );
      return descriptor;
    });
  }
  async consume(
    raw: ConsumeAcceptedArtifactInputV3,
  ): Promise<ConsumeAcceptedArtifactResultV3> {
    const input = parseConsumeAcceptedArtifactInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "consume",
        input,
        (x) => x as ConsumeAcceptedArtifactResultV3,
      );
      if (replay !== undefined) return replay;
      const lease = input.expectedLease;
      const targetLocation = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id,'rootTaskId',root_task_id) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)}),'null'::json);`,
      );
      if (
        targetLocation === null ||
        targetLocation.rootTaskId !== lease.rootTaskId
      )
        storageFail("TASK_LEASE_STALE");
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(targetLocation.repositoryId)} FOR UPDATE;SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} FOR UPDATE;`,
      );
      const target = await session.queryJson<{
        status: string;
        rowVersion: number;
        graphRevision: number;
      }>(
        `SELECT json_build_object('status',s.status,'rowVersion',s.row_version,'graphRevision',g.graph_revision) FROM task_v3_state s JOIN task_v3_graphs g USING(root_task_id) WHERE s.task_id=${sqlLiteral(lease.taskId)} FOR UPDATE;`,
      );
      const current = await session.queryJson<TaskLeaseV3 | null>(
        `SELECT coalesce((SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE ${currentLeasePredicate(lease)} FOR UPDATE),'null'::json);`,
      );
      if (current === null) storageFail("TASK_LEASE_STALE");
      if (
        target.status !== input.expectedTaskStatus ||
        target.rowVersion !== input.expectedTaskRowVersion ||
        target.rowVersion !== lease.taskRowVersion
      )
        storageFail("TASK_MUTATION_FENCE_REQUIRED");
      if (target.graphRevision !== input.expectedTargetGraphRevision)
        storageFail("TASK_GRAPH_REVISION_STALE");
      const source = await session.queryJson<{
        taskId: string;
        digest: string;
        descriptor: ArtifactDescriptorV3;
        status: string;
        rowVersion: number;
        barrier: string;
        revision: number;
        terminalSeatsCurrentGo: boolean;
        materialAvailable: boolean;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('taskId',a.task_id,'digest',a.artifact_digest,'descriptor',a.descriptor_json,'status',s.status,'rowVersion',s.row_version,'barrier',b.state,'revision',b.barrier_revision,'terminalSeatsCurrentGo',NOT EXISTS(SELECT 1 FROM review_requirements_v3 r WHERE r.artifact_id=a.artifact_id AND NOT EXISTS(SELECT 1 FROM review_verdicts_v3 v JOIN review_assignments_v3 ra ON ra.assignment_id=v.assignment_id WHERE v.artifact_id=r.artifact_id AND v.seat_key=r.seat_key AND v.verdict='GO' AND ra.assignment_revision=(SELECT max(ra2.assignment_revision) FROM review_assignments_v3 ra2 WHERE ra2.artifact_id=r.artifact_id AND ra2.seat_key=r.seat_key))),'materialAvailable',(SELECT count(*)=3 FROM staged_artifact_materials_v3 m WHERE m.staged_object_id IN(a.material_object_id,a.scope_manifest_object_id,a.acceptance_receipt_object_id) AND m.sealed)) FROM artifacts_v3 a JOIN task_v3_state s USING(task_id) JOIN review_barriers_v3 b USING(artifact_id) WHERE a.artifact_id=${sqlLiteral(input.sourceArtifactId)} FOR SHARE),'null'::json);`,
      );
      if (
        source === null ||
        source.digest !== input.sourceArtifactDigest ||
        source.status !== "done" ||
        source.rowVersion !== input.expectedSourceTaskRowVersion ||
        source.barrier !== "satisfied" ||
        source.revision !== input.expectedSourceBarrierRevision ||
        !source.terminalSeatsCurrentGo ||
        !source.materialAvailable
      )
        storageFail("ARTIFACT_SOURCE_NOT_ACCEPTED");
      source.descriptor = parseArtifactDescriptorV3(source.descriptor);
      if (
        source.descriptor.artifactId !== input.sourceArtifactId ||
        source.descriptor.artifactDigest !== input.sourceArtifactDigest
      )
        storageFail("ARTIFACT_SOURCE_NOT_ACCEPTED");
      const declared = await session.queryJson<{ ok: boolean }>(
        `SELECT json_build_object('ok',EXISTS(SELECT 1 FROM task_v3_edges WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} AND edge_kind='depends_on' AND prerequisite_task_id=${sqlLiteral(source.taskId)} AND dependent_task_id=${sqlLiteral(lease.taskId)}));`,
      );
      if (!declared.ok) storageFail("ARTIFACT_SOURCE_NOT_ACCEPTED");
      const consumed = await session.queryJson<{ yes: boolean }>(
        `SELECT json_build_object('yes',EXISTS(SELECT 1 FROM artifact_consumptions_v3 WHERE target_task_id=${sqlLiteral(lease.taskId)} AND target_attempt=${sqlLiteral(lease.attempt)} AND source_artifact_id=${sqlLiteral(input.sourceArtifactId)}));`,
      );
      if (consumed.yes) storageFail("ARTIFACT_CONTRIBUTOR_LEDGER_STALE");
      const ledger = await session.queryJson<{ revision: number }>(
        `SELECT json_build_object('revision',ledger_revision) FROM task_attempt_ledgers_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)} FOR UPDATE;`,
      );
      if (ledger.revision !== input.expectedContributorLedgerRevision)
        storageFail("ARTIFACT_CONTRIBUTOR_LEDGER_STALE");
      const agents = [
        source.descriptor.builderAgentId,
        ...source.descriptor.contributorAgentIds,
      ].sort();
      for (const agent of agents)
        await session.execute(
          `INSERT INTO task_attempt_contributors_v3(task_id,attempt,agent_id,source,source_artifact_id) VALUES(${sqlLiteral(lease.taskId)},${sqlLiteral(lease.attempt)},${sqlLiteral(agent)},'accepted_upstream_artifact',${sqlLiteral(input.sourceArtifactId)}) ON CONFLICT DO NOTHING;`,
        );
      await session.execute(
        `UPDATE task_attempt_ledgers_v3 SET ledger_revision=ledger_revision+1 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)};`,
      );
      const consumptionId = mint<string>("acm"),
        grantId = mint<string>("amg");
      await session.execute(
        `INSERT INTO artifact_consumptions_v3(consumption_id,grant_id,target_task_id,target_attempt,target_lease_epoch,target_fence_token,source_artifact_id,source_artifact_digest) VALUES(${sqlLiteral(consumptionId)},${sqlLiteral(grantId)},${sqlLiteral(lease.taskId)},${sqlLiteral(lease.attempt)},${sqlLiteral(lease.leaseEpoch)},${sqlLiteral(lease.fenceToken)},${sqlLiteral(input.sourceArtifactId)},${sqlLiteral(input.sourceArtifactDigest)});`,
      );
      const all = await session.queryJson<{
        sources: any[];
        agents: AgentId[];
      }>(
        `SELECT json_build_object('sources',coalesce((SELECT json_agg(DISTINCT source_artifact_id ORDER BY source_artifact_id) FROM task_attempt_contributors_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)} AND source_artifact_id IS NOT NULL),'[]'::json),'agents',coalesce((SELECT json_agg(DISTINCT agent_id ORDER BY agent_id) FROM task_attempt_contributors_v3 WHERE task_id=${sqlLiteral(lease.taskId)} AND attempt=${sqlLiteral(lease.attempt)}),'[]'::json));`,
      );
      const result: ConsumeAcceptedArtifactResultV3 = {
        kind: "consumed",
        consumptionId: consumptionId as never,
        materializationGrantId: grantId as ArtifactMaterializationGrantId,
        sourceArtifactId: input.sourceArtifactId,
        sourceArtifactDigest: input.sourceArtifactDigest,
        previousContributorLedgerRevision: ledger.revision,
        currentContributorLedgerRevision: ledger.revision + 1,
        acceptedSourceArtifactIds: all.sources,
        contributorAgentIds: all.agents,
      };
      await this.storeReceipt(
        session,
        input.commandId,
        "consume",
        input,
        result,
      );
      return result;
    });
  }
}

export class ReviewV3Repository extends Wave3SessionOwner {
  async assign(
    raw: AssignReviewSeatInputV3,
  ): Promise<AssignReviewSeatResultV3> {
    const input = parseAssignReviewSeatInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "assign_review",
        input,
        (x) => x as AssignReviewSeatResultV3,
      );
      if (replay !== undefined) return replay;
      const location = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',s.repository_id,'rootTaskId',s.root_task_id) FROM artifacts_v3 a JOIN task_v3_state s ON s.task_id=a.task_id WHERE a.artifact_id=${sqlLiteral(input.requirement.artifactId)}),'null'::json);`,
      );
      if (location === null) storageFail("REVIEW_ASSIGNMENT_STALE");
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(location.repositoryId)} FOR UPDATE;SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(location.rootTaskId)} FOR UPDATE;SELECT 1 FROM task_v3_state WHERE root_task_id=${sqlLiteral(location.rootTaskId)} ORDER BY task_id FOR UPDATE;`,
      );
      const barrier = await session.queryJson<{
        revision: number;
        state: string;
        builder: string;
        artifactTaskStatus: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('revision',b.barrier_revision,'state',b.state,'builder',a.builder_agent_id,'artifactTaskStatus',s.status) FROM review_barriers_v3 b JOIN artifacts_v3 a USING(artifact_id) JOIN task_v3_state s ON s.task_id=a.task_id WHERE b.artifact_id=${sqlLiteral(input.requirement.artifactId)} FOR UPDATE),'null'::json);`,
      );
      if (
        barrier === null ||
        barrier.state !== input.expectedBarrierState ||
        barrier.artifactTaskStatus !== "in_review" ||
        barrier.revision !== input.expectedBarrierRevision
      )
        storageFail("REVIEW_ASSIGNMENT_STALE");
      const requirement = await session.queryJson<{
        value: AssignReviewSeatInputV3["requirement"];
        reviewTaskId: string;
        reviewTaskStatus: string;
        requiredCapabilities: string[];
        terminal: boolean;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('value',json_build_object('artifactId',r.artifact_id,'artifactDigest',r.artifact_digest,'artifactContractDigest',r.artifact_contract_digest,'gate3PlanDigest',r.gate3_plan_digest,'scenarioVersion',r.scenario_version,'seatKey',r.seat_key),'reviewTaskId',r.review_task_id,'reviewTaskStatus',s.status,'requiredCapabilities',s.required_capabilities,'terminal',EXISTS(SELECT 1 FROM review_verdicts_v3 v WHERE v.artifact_id=r.artifact_id AND v.seat_key=r.seat_key)) FROM review_requirements_v3 r JOIN task_v3_state s ON s.task_id=r.review_task_id WHERE r.artifact_id=${sqlLiteral(input.requirement.artifactId)} AND r.seat_key=${sqlLiteral(input.requirement.seatKey)} FOR UPDATE),'null'::json);`,
      );
      if (
        requirement === null ||
        digest(requirement.value) !== digest(input.requirement) ||
        requirement.reviewTaskId !== input.reviewTaskId ||
        requirement.reviewTaskStatus !== input.expectedReviewTaskStatus ||
        requirement.terminal !== (input.expectedTerminalVerdictId !== null)
      )
        storageFail(
          requirement?.terminal
            ? "REVIEW_ASSIGNMENT_TERMINAL"
            : "REVIEW_ASSIGNMENT_STALE",
        );
      const excluded = await session.queryJson<{ yes: boolean }>(
        `SELECT json_build_object('yes',${sqlLiteral(input.reviewerAgentId)}=${sqlLiteral(barrier.builder)} OR EXISTS(SELECT 1 FROM task_attempt_contributors_v3 c JOIN artifacts_v3 a ON a.task_id=c.task_id AND a.attempt=c.attempt WHERE a.artifact_id=${sqlLiteral(input.requirement.artifactId)} AND c.agent_id=${sqlLiteral(input.reviewerAgentId)}));`,
      );
      if (excluded.yes) storageFail("REVIEW_SELF_ASSIGNMENT_FORBIDDEN");
      const agent = await session.queryJson<{
        revision: number;
        current: boolean;
        capabilities: string[];
      } | null>(
        `SELECT coalesce((SELECT json_build_object('revision',registry_revision,'current',current,'capabilities',capability_keys) FROM agent_capabilities_v3 WHERE agent_id=${sqlLiteral(input.reviewerAgentId)} FOR SHARE),'null'::json);`,
      );
      if (
        agent === null ||
        !agent.current ||
        agent.revision !== input.expectedRegistryRevision ||
        requirement.requiredCapabilities.some(
          (capability) => !agent.capabilities.includes(capability),
        )
      )
        storageFail("REVIEW_ASSIGNMENT_STALE");
      const existing = await session.queryJson<{
        id: string;
        revision: number;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('id',assignment_id,'revision',assignment_revision) FROM review_assignments_v3 WHERE artifact_id=${sqlLiteral(input.requirement.artifactId)} AND seat_key=${sqlLiteral(input.requirement.seatKey)} AND assignment_status='current' FOR UPDATE),'null'::json);`,
      );
      if ((existing?.revision ?? null) !== input.expectedAssignmentRevision)
        storageFail("REVIEW_ASSIGNMENT_STALE");
      if (existing !== null)
        await session.execute(
          `UPDATE review_assignments_v3 SET assignment_status='closed',closed_reason='reassigned' WHERE assignment_id=${sqlLiteral(existing.id)};UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE task_id=${sqlLiteral(input.reviewTaskId)} AND closed_at IS NULL;UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE task_id=${sqlLiteral(input.reviewTaskId)} AND closed_at IS NULL;UPDATE task_v3_state SET status='todo',row_version=row_version+1 WHERE task_id=${sqlLiteral(input.reviewTaskId)} AND status='in_progress';`,
        );
      const duplicate = await session.queryJson<{ yes: boolean }>(
        `SELECT json_build_object('yes',EXISTS(SELECT 1 FROM review_assignments_v3 WHERE artifact_id=${sqlLiteral(input.requirement.artifactId)} AND reviewer_agent_id=${sqlLiteral(input.reviewerAgentId)} AND assignment_status='current'));`,
      );
      if (duplicate.yes) storageFail("REVIEW_INDEPENDENCE_VIOLATION");
      const assignmentId = mint<string>("ras"),
        revision = (existing?.revision ?? 0) + 1;
      await session.execute(
        `INSERT INTO review_assignments_v3(assignment_id,artifact_id,seat_key,review_task_id,reviewer_agent_id,reviewer_registry_revision,assignment_revision,assignment_status) VALUES(${sqlLiteral(assignmentId)},${sqlLiteral(input.requirement.artifactId)},${sqlLiteral(input.requirement.seatKey)},${sqlLiteral(input.reviewTaskId)},${sqlLiteral(input.reviewerAgentId)},${sqlLiteral(input.expectedRegistryRevision)},${sqlLiteral(revision)},'current');UPDATE review_barriers_v3 SET barrier_revision=barrier_revision+1,updated_at=clock_timestamp() WHERE artifact_id=${sqlLiteral(input.requirement.artifactId)};`,
      );
      const assignment = {
        ...input.requirement,
        assignmentId: assignmentId as never,
        reviewTaskId: input.reviewTaskId,
        reviewerAgentId: input.reviewerAgentId,
        reviewerRegistryRevision: input.expectedRegistryRevision,
        assignmentRevision: revision,
        assignmentStatus: "current" as const,
        closedReason: null,
      };
      const result: AssignReviewSeatResultV3 = {
        assignment,
        previousAssignmentId: (existing?.id as never) ?? null,
        currentBarrierRevision: barrier.revision + 1,
      };
      await this.storeReceipt(
        session,
        input.commandId,
        "assign_review",
        input,
        result,
      );
      return result;
    });
  }
  async submit(raw: {
    commandId: any;
    expectedLease: TaskLeaseV3;
    expectedTaskStatus: "in_progress";
    expectedTaskRowVersion: number;
    verdict: ReviewVerdictV3;
  }): Promise<{
    verdict: ReviewVerdictV3;
    barrierState: "open" | "satisfied" | "blocked";
    barrierRevision: number;
  }> {
    const input = parseSubmitReviewVerdictInputV3(raw);
    return this.transaction(async (session) => {
      const replay = await this.commandReplay(
        session,
        input.commandId,
        "review_verdict",
        input,
        (x) =>
          x as {
            verdict: ReviewVerdictV3;
            barrierState: "open" | "satisfied" | "blocked";
            barrierRevision: number;
          },
      );
      if (replay !== undefined) return replay;
      const verdict = parseReviewVerdictV3(input.verdict),
        lease = input.expectedLease;
      const location = await session.queryJson<{
        repositoryId: string;
        rootTaskId: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('repositoryId',repository_id,'rootTaskId',root_task_id) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)}),'null'::json);`,
      );
      if (location === null || location.rootTaskId !== lease.rootTaskId)
        storageFail("TASK_GRAPH_SCOPE_VIOLATION");
      await session.execute(
        `SELECT 1 FROM workspace_repositories WHERE repository_id=${sqlLiteral(location.repositoryId)} FOR UPDATE;`,
      );
      await this.reachLatch("review_block_after_repository_lock");
      await session.execute(
        `SELECT 1 FROM task_v3_graphs WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} FOR UPDATE;`,
      );
      const assignment = await session.queryJson<{
        status: string;
        revision: number;
        reviewer: string;
        registryRevision: number;
        artifactId: string;
        artifactDigest: string;
        artifactContractDigest: string;
        gate3PlanDigest: string;
        scenarioVersion: number;
        seatKey: string;
        reviewTaskId: string;
        barrierState: string;
      } | null>(
        `SELECT coalesce((SELECT json_build_object('status',a.assignment_status,'revision',a.assignment_revision,'reviewer',a.reviewer_agent_id,'registryRevision',a.reviewer_registry_revision,'artifactId',r.artifact_id,'artifactDigest',r.artifact_digest,'artifactContractDigest',r.artifact_contract_digest,'gate3PlanDigest',r.gate3_plan_digest,'scenarioVersion',r.scenario_version,'seatKey',r.seat_key,'reviewTaskId',r.review_task_id,'barrierState',b.state) FROM review_assignments_v3 a JOIN review_requirements_v3 r USING(artifact_id,seat_key) JOIN review_barriers_v3 b USING(artifact_id) WHERE a.assignment_id=${sqlLiteral(verdict.assignmentId)} FOR UPDATE),'null'::json);`,
      );
      if (
        assignment === null ||
        assignment.status !== "current" ||
        assignment.revision !== verdict.assignmentRevision ||
        assignment.reviewer !== lease.ownerAgentId ||
        assignment.registryRevision !== verdict.reviewerRegistryRevision ||
        assignment.artifactId !== verdict.artifactId ||
        assignment.artifactDigest !== verdict.artifactDigest ||
        assignment.artifactContractDigest !== verdict.artifactContractDigest ||
        assignment.gate3PlanDigest !== verdict.gate3PlanDigest ||
        assignment.scenarioVersion !== verdict.scenarioVersion ||
        assignment.seatKey !== verdict.seatKey ||
        assignment.reviewTaskId !== verdict.reviewTaskId ||
        assignment.barrierState !== "open" ||
        verdict.assignmentStatus !== "current" ||
        verdict.closedReason !== null
      )
        storageFail("REVIEW_ASSIGNMENT_STALE");
      const current = await session.queryJson<TaskLeaseV3 | null>(
        `SELECT coalesce((SELECT ${leaseJson("c")} FROM task_claims_v3 c JOIN task_v3_graphs g USING(root_task_id) WHERE ${currentLeasePredicate(lease)} FOR UPDATE),'null'::json);`,
      );
      if (current === null) storageFail("TASK_LEASE_STALE");
      await session.execute(
        `SELECT 1 FROM task_v3_state WHERE root_task_id=${sqlLiteral(lease.rootTaskId)} ORDER BY task_id FOR UPDATE;`,
      );
      const task = await session.queryJson<{
        status: string;
        rowVersion: number;
      }>(
        `SELECT json_build_object('status',status,'rowVersion',row_version) FROM task_v3_state WHERE task_id=${sqlLiteral(lease.taskId)} FOR UPDATE;`,
      );
      if (
        task.status !== input.expectedTaskStatus ||
        task.rowVersion !== input.expectedTaskRowVersion ||
        task.rowVersion !== lease.taskRowVersion
      )
        storageFail("TASK_MUTATION_FENCE_REQUIRED");
      await session.execute(
        `INSERT INTO review_verdicts_v3(assignment_id,artifact_id,seat_key,review_attempt,verdict,findings_digest,evidence_receipt_digest) VALUES(${sqlLiteral(verdict.assignmentId)},${sqlLiteral(verdict.artifactId)},${sqlLiteral(verdict.seatKey)},${sqlLiteral(verdict.reviewAttempt)},${sqlLiteral(verdict.verdict)},${sqlLiteral(verdict.findingsDigest)},${sqlLiteral(verdict.evidenceReceiptDigest)});UPDATE review_assignments_v3 SET assignment_status='closed',closed_reason='terminal_verdict' WHERE assignment_id=${sqlLiteral(verdict.assignmentId)};UPDATE task_v3_state SET status='done',row_version=row_version+1 WHERE task_id=${sqlLiteral(verdict.reviewTaskId)};UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE claim_id=${sqlLiteral(lease.claimId)};UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE claim_id=${sqlLiteral(lease.claimId)} AND closed_at IS NULL;`,
      );
      const counts = await session.queryJson<{
        required: number;
        go: number;
        block: number;
        artifactTask: string;
      }>(
        `SELECT json_build_object('required',(SELECT count(*) FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}),'go',(SELECT count(DISTINCT seat_key) FROM review_verdicts_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)} AND verdict='GO'),'block',(SELECT count(*) FROM review_verdicts_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)} AND verdict='BLOCK'),'artifactTask',(SELECT task_id FROM artifacts_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}));`,
      );
      let state: "open" | "satisfied" | "blocked" = "open";
      if (counts.block > 0) {
        state = "blocked";
        await session.execute(
          `UPDATE review_barriers_v3 SET state='blocked',barrier_revision=barrier_revision+1 WHERE artifact_id=${sqlLiteral(verdict.artifactId)} AND state='open';UPDATE review_assignments_v3 SET assignment_status='closed',closed_reason='barrier_blocked' WHERE artifact_id=${sqlLiteral(verdict.artifactId)} AND assignment_status='current';UPDATE task_v3_state SET status='blocked',row_version=row_version+1 WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}) AND status IN('todo','in_progress');UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='review_barrier_blocked' WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}) AND closed_at IS NULL;UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='review_barrier_blocked' WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}) AND closed_at IS NULL;UPDATE task_v3_state SET status='todo',row_version=row_version+1 WHERE task_id=${sqlLiteral(counts.artifactTask)} AND status='in_review';`,
        );
        const post = await session.queryJson<{
          barrier: boolean;
          artifact: boolean;
          assignments: boolean;
          tasks: boolean;
          claims: boolean;
          reservations: boolean;
        }>(
          `SELECT json_build_object('barrier',(SELECT state='blocked' FROM review_barriers_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}),'artifact',(SELECT status='todo' FROM task_v3_state WHERE task_id=${sqlLiteral(counts.artifactTask)}),'assignments',NOT EXISTS(SELECT 1 FROM review_assignments_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)} AND assignment_status='current'),'tasks',NOT EXISTS(SELECT 1 FROM task_v3_state WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}) AND status NOT IN('done','blocked')),'claims',NOT EXISTS(SELECT 1 FROM task_claims_v3 WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}) AND closed_at IS NULL),'reservations',NOT EXISTS(SELECT 1 FROM workspace_reservations_v3 WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)}) AND closed_at IS NULL));`,
        );
        if (
          !post.barrier ||
          !post.artifact ||
          !post.assignments ||
          !post.tasks ||
          !post.claims ||
          !post.reservations
        )
          storageFail("REVIEW_BARRIER_ATOMICITY_VIOLATION", post);
      } else if (counts.go === counts.required) {
        state = "satisfied";
        await session.execute(
          `UPDATE review_barriers_v3 SET state='satisfied',barrier_revision=barrier_revision+1 WHERE artifact_id=${sqlLiteral(verdict.artifactId)};UPDATE task_v3_state SET status='done',row_version=row_version+1 WHERE task_id=${sqlLiteral(counts.artifactTask)};`,
        );
      }
      const barrier = await session.queryJson<{ revision: number }>(
        `SELECT json_build_object('revision',barrier_revision) FROM review_barriers_v3 WHERE artifact_id=${sqlLiteral(verdict.artifactId)};`,
      );
      const result = {
        verdict,
        barrierState: state,
        barrierRevision: barrier.revision,
      };
      await this.storeReceipt(
        session,
        input.commandId,
        "review_verdict",
        input,
        result,
      );
      return result;
    });
  }
}
