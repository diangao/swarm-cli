import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import {
  canonicalProtocolJson,
  parseArtifactContractTemplateV3,
  parseMaterializedArtifactContractV3,
  parseWorkspaceContractV3,
  type ArtifactDigest,
  type MessageId,
  type ProducerFactId,
  type TaskLeaseV3,
  type TaskTitlePrivacyPortV3,
  type TurnId,
} from "@swarm/protocol";
import { createHash } from "node:crypto";
import {
  PostgresMigrator,
  PsqlSession,
  ArtifactV3Repository,
  ReviewV3Repository,
  StorageError,
  TaskGraphV3Repository,
  TaskLeaseV3Repository,
  Wave3RegistryRepository,
  Wave3SchemaRepository,
  WorkspaceReservationV3Repository,
  type Wave3PgTestLatchName,
  type Wave3PgTestLatchPort,
} from "../src/index.js";

function requiredDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value)
    throw new Error("DATABASE_URL is required for Wave 3 PostgreSQL tests");
  return value;
}
const databaseUrl = requiredDatabaseUrl();
const schema = `swarm_wave3_${process.pid}`;
const id = (prefix: string, ch: string) => `${prefix}_${ch.repeat(26)}`;
const variant = (prefix: string, ch: string, tail: string) =>
  `${prefix}_${ch.repeat(25)}${tail}`;
const sha = (ch: string): ArtifactDigest =>
  `sha256:${ch.repeat(64)}` as ArtifactDigest;
const canonicalDigest = (value: unknown): ArtifactDigest =>
  `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
const utf8Digest = (value: string): ArtifactDigest =>
  `sha256:${createHash("sha256").update(value, "utf8").digest("hex")}` as ArtifactDigest;
let templateDigest: ArtifactDigest;
let sourceSequence = 0;

async function raw(sql: string): Promise<void> {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`SET search_path TO ${schema}, pg_catalog; ${sql}`);
  } finally {
    await session.close();
  }
}

async function rawJson<T>(sql: string): Promise<T> {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`SET search_path TO ${schema}, pg_catalog;`);
    return await session.queryJson<T>(sql);
  } finally {
    await session.close();
  }
}

class ControlledLatches implements Wave3PgTestLatchPort {
  readonly counts = new Map<Wave3PgTestLatchName, number>();
  readonly #holds = new Map<
    Wave3PgTestLatchName,
    { reached: Promise<void>; noteReached: () => void; release: () => void; wait: Promise<void> }
  >();

  hold(name: Wave3PgTestLatchName): {
    reached: Promise<void>;
    release: () => void;
  } {
    let noteReached!: () => void;
    let release!: () => void;
    const reached = new Promise<void>((resolve) => {
      noteReached = resolve;
    });
    const wait = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.#holds.set(name, { reached, noteReached, release, wait });
    return { reached, release };
  }

  async reach(name: Wave3PgTestLatchName): Promise<void> {
    this.counts.set(name, (this.counts.get(name) ?? 0) + 1);
    const hold = this.#holds.get(name);
    hold?.noteReached();
    await hold?.wait;
  }
}

class AsyncBarrier {
  readonly participants: number;
  #arrived = 0;
  #release!: () => void;
  readonly #wait = new Promise<void>((resolve) => {
    this.#release = resolve;
  });

  constructor(participants: number) {
    this.participants = participants;
  }

  async arrive(): Promise<void> {
    this.#arrived += 1;
    if (this.#arrived === this.participants) this.#release();
    await this.#wait;
  }
}

async function settleWithin<T>(promise: Promise<T>, milliseconds: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`operation did not settle within ${milliseconds}ms`)),
          milliseconds,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

class ExactTitlePrivacyPort implements TaskTitlePrivacyPortV3 {
  readonly calls: Array<{ sourceMessageId: string; title: string }> = [];
  constructor(
    readonly classifierPolicyDigest: ArtifactDigest,
    readonly forcedPolicyDigest: ArtifactDigest | null = null,
  ) {}

  async validateTaskTitle(input: Parameters<TaskTitlePrivacyPortV3["validateTaskTitle"]>[0]) {
    this.calls.push({ sourceMessageId: input.sourceMessageId, title: input.title });
    assert.equal(input.titleDigest, utf8Digest(input.title));
    return {
      titleDigest: input.titleDigest,
      classifierPolicyDigest:
        this.forcedPolicyDigest ?? this.classifierPolicyDigest,
    };
  }
}

async function registerRoot(
  character: string,
  taskNumber: number,
  requiredCapabilities: string[] = ["storage.pg16"],
  pathClaims: Array<{ kind: "file" | "subtree"; path: string }> = [
    { kind: "subtree", path: `fixtures/${character}` },
  ],
) {
  const registry = new Wave3RegistryRepository(databaseUrl, schema);
  const workspace = parseWorkspaceContractV3({
    protocolVersion: 1,
    workspaceContractId: id("wsc", character),
    taskId: id("tsk", character),
    rootTaskId: id("tsk", character),
    repositoryId: id("rpo", "a"),
    baseCommit: "4".repeat(40),
    baseTree: "5".repeat(40),
    workspaceGeneration: 1,
    executionMode: "isolated_worktree",
    pathClaims,
    readArtifactDigest: null,
    integrationOwnerTaskId: id("tsk", character),
    artifactContractTemplateDigest: templateDigest,
    policyDigest: sha("6"),
  });
  const contract = parseMaterializedArtifactContractV3({
    protocolVersion: 1,
    artifactContractId: id("acc", character),
    templateDigest,
    serverId: id("srv", "a"),
    rootTaskId: id("tsk", character),
    taskId: id("tsk", character),
    workspaceContractId: workspace.workspaceContractId,
    workspaceContractDigest: canonicalDigest(workspace),
    baseCommit: workspace.baseCommit,
    baseTree: workspace.baseTree,
    pathClaimsDigest: canonicalDigest(workspace.pathClaims),
    integrationOwnerTaskId: id("tsk", character),
    allowedKinds: ["git_commit"],
    maxMaterialBytes: 1_000_000,
    scopePolicy: "exact_workspace_claims",
    receiptPolicyDigest: sha("2"),
    requiredReviewSeats: ["scope"],
    gate3PlanDigest: sha("7"),
    policyDigest: sha("3"),
  });
  await registry.registerRootTask({
    serverId: id("srv", "a") as never,
    repositoryId: id("rpo", "a") as never,
    repositoryDigest: sha("1"),
    rootTaskId: id("tsk", character) as never,
    taskNumber,
    laneRole: "storage",
    requiredCapabilities,
    workspace,
    artifactContract: contract,
    titleDigest: sha("8"),
    titleClassifierPolicyDigest: sha("9"),
    graphPolicyDigest: sha("a"),
    scenarioVersion: 8,
  });
  return { workspace, contract };
}

async function claimRoot(
  character: string,
  commandCharacter: string,
  latches?: Wave3PgTestLatchPort,
): Promise<TaskLeaseV3> {
  const result = await new TaskLeaseV3Repository(
    databaseUrl,
    schema,
    latches,
  ).claim({
    commandId: id("cmd", commandCharacter) as never,
    taskId: id("tsk", character) as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: 1,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(result.kind, "claimed");
  if (result.kind !== "claimed") throw new Error("expected claim");
  return result.lease;
}

async function createPredecessor(character: string): Promise<{
  sourceMessageId: MessageId;
  sourceProducerFactId: ProducerFactId;
  sourceTurnId: TurnId;
  committedReplyMessageId: MessageId;
}> {
  const sourceMessageId = id("msg", character);
  const replyCharacters: Record<string, string> = {
    b: "n",
    c: "p",
    d: "q",
    e: "r",
    f: "s",
    g: "t",
    h: "v",
  };
  const replyCharacter = replyCharacters[character] ?? "w";
  const suffix = String(sourceSequence + 1).slice(-1);
  const committedReplyMessageId = variant("msg", replyCharacter, suffix);
  const sourceProducerFactId = id("fac", character);
  const sourceTurnId = id("trn", character);
  const sourceSeq = ++sourceSequence;
  const replySeq = ++sourceSequence;
  await raw(`
    INSERT INTO messages(message_id,target_kind,target_id,author_kind,author_id,target_seq,body,producer_fact_id,payload_digest)
      VALUES('${sourceMessageId}','channel','${id("chn", "a")}','human','${id("hum", "a")}',${sourceSeq},'source-${character}','${sourceProducerFactId}','${sha("1")}');
    INSERT INTO messages(message_id,target_kind,target_id,author_kind,author_id,target_seq,body,parent_message_id,producer_fact_id,payload_digest,caused_by_producer_fact_id)
      VALUES('${committedReplyMessageId}','channel','${id("chn", "a")}','agent','${id("agt", "a")}',${replySeq},'reply-${character}','${sourceMessageId}','${variant("fac", replyCharacter, suffix)}','${sha("2")}','${sourceProducerFactId}');
    INSERT INTO receipts(receipt_id,producer_fact_id,kind,actor_machine_id,actor_agent_id,launch_id,state_instance_id,turn_id,session_id,occurred_at,detail_json,receipt_digest,effect_kind,effect_message_id)
      VALUES('${id("rcp", character)}','${sourceProducerFactId}','side_effect_applied','${id("mch", "a")}','${id("agt", "a")}','${id("lnc", "a")}','${id("sti", character)}','${sourceTurnId}','${id("ses", character)}',clock_timestamp(),'{"protocolVersion":1}','${sha("3")}','reply_committed','${committedReplyMessageId}');
  `);
  return {
    sourceMessageId: sourceMessageId as MessageId,
    sourceProducerFactId: sourceProducerFactId as ProducerFactId,
    sourceTurnId: sourceTurnId as TurnId,
    committedReplyMessageId: committedReplyMessageId as MessageId,
  };
}

function graphCommand(
  rootCharacter: string,
  commandCharacter: string,
  lease: TaskLeaseV3,
  predecessor: Awaited<ReturnType<typeof createPredecessor>>,
) {
  const title = `child-${rootCharacter}`;
  return {
    commandId: id("cmd", commandCharacter) as never,
    expectedLease: lease,
    expectedTaskStatus: "in_progress" as const,
    expectedTaskRowVersion: lease.taskRowVersion,
    rootTaskId: id("tsk", rootCharacter) as never,
    ...predecessor,
    expectedGraphRevision: lease.graphRevision,
    expectedPolicyDigest: sha("a"),
    expectedTitleClassifierPolicyDigest: sha("9"),
    children: [
      {
        clientKey: `child-${rootCharacter}`,
        title,
        titleDigest: utf8Digest(title),
        titleClassifierPolicyDigest: sha("9"),
        laneRole: "storage" as const,
        requiredCapabilities: [] as never[],
        workspace: {
          repositoryId: id("rpo", "a") as never,
          baseCommit: "4".repeat(40) as never,
          baseTree: "5".repeat(40) as never,
          workspaceGeneration: 1,
          executionMode: "isolated_worktree" as const,
          pathClaims: [{ kind: "file" as const, path: `fixtures/${rootCharacter}/child.ts` }],
          readArtifactDigest: null,
          integrationOwnerTaskId: id("tsk", rootCharacter) as never,
          artifactContractTemplateDigest: templateDigest,
          policyDigest: sha("6"),
        },
      },
    ],
    dependencies: [] as never[],
  };
}

before(async () => {
  await new PostgresMigrator(databaseUrl, schema).migrate();
  await new Wave3SchemaRepository(databaseUrl, schema).migrate();
  await raw(`
    INSERT INTO servers(server_id,display_name) VALUES('${id("srv", "a")}','wave3');
    INSERT INTO machines(machine_id,server_id) VALUES('${id("mch", "a")}','${id("srv", "a")}');
    INSERT INTO agents(agent_id,server_id) VALUES('${id("agt", "a")}','${id("srv", "a")}');
    INSERT INTO agents(agent_id,server_id) VALUES('${id("agt", "b")}','${id("srv", "a")}');
    INSERT INTO agents(agent_id,server_id) VALUES('${id("agt", "c")}','${id("srv", "a")}');
    INSERT INTO humans(human_id,server_id,display_name) VALUES('${id("hum", "a")}','${id("srv", "a")}','source owner');
    INSERT INTO channels(channel_id,server_id,visibility,name) VALUES('${id("chn", "a")}','${id("srv", "a")}','private','wave3-source');
    INSERT INTO agent_launches(launch_id,machine_id,agent_id,runtime_kind,workspace_generation,routing_generation,state,activated_at)
      VALUES('${id("lnc", "a")}','${id("mch", "a")}','${id("agt", "a")}','codex',1,1,'activated',clock_timestamp());
  `);

  const registry = new Wave3RegistryRepository(databaseUrl, schema);
  await registry.registerRepository({
    repositoryId: id("rpo", "a") as never,
    serverId: id("srv", "a") as never,
    repositoryDigest: sha("1"),
  });
  await registry.registerAgentCapabilities({
    agentId: id("agt", "a") as never,
    registryRevision: 1,
    capabilityKeys: ["storage.pg16"],
  });
  await registry.registerAgentCapabilities({
    agentId: id("agt", "b") as never,
    registryRevision: 1,
    capabilityKeys: ["storage.pg16"],
  });
  await registry.registerAgentCapabilities({
    agentId: id("agt", "c") as never,
    registryRevision: 1,
    capabilityKeys: ["storage.pg16"],
  });
  const template = parseArtifactContractTemplateV3({
    protocolVersion: 1,
    templateId: id("act", "a"),
    templateRevision: 1,
    allowedKinds: ["git_commit"],
    maxMaterialBytes: 1_000_000,
    scopePolicy: "exact_workspace_claims",
    receiptPolicyDigest: sha("2"),
    requiredReviewSeats: ["scope"],
    policyDigest: sha("3"),
  });
  templateDigest = await registry.registerArtifactTemplate(template);
  const workspace = parseWorkspaceContractV3({
    protocolVersion: 1,
    workspaceContractId: id("wsc", "a"),
    taskId: id("tsk", "a"),
    rootTaskId: id("tsk", "a"),
    repositoryId: id("rpo", "a"),
    baseCommit: "4".repeat(40),
    baseTree: "5".repeat(40),
    workspaceGeneration: 1,
    executionMode: "isolated_worktree",
    pathClaims: [{ kind: "subtree", path: "packages/storage" }],
    readArtifactDigest: null,
    integrationOwnerTaskId: id("tsk", "a"),
    artifactContractTemplateDigest: templateDigest,
    policyDigest: sha("6"),
  });
  const workspaceDigest = canonicalDigest(workspace);
  const contract = parseMaterializedArtifactContractV3({
    protocolVersion: 1,
    artifactContractId: id("acc", "a"),
    templateDigest,
    serverId: id("srv", "a"),
    rootTaskId: id("tsk", "a"),
    taskId: id("tsk", "a"),
    workspaceContractId: id("wsc", "a"),
    workspaceContractDigest: workspaceDigest,
    baseCommit: "4".repeat(40),
    baseTree: "5".repeat(40),
    pathClaimsDigest: canonicalDigest(workspace.pathClaims),
    integrationOwnerTaskId: id("tsk", "a"),
    allowedKinds: ["git_commit"],
    maxMaterialBytes: 1_000_000,
    scopePolicy: "exact_workspace_claims",
    receiptPolicyDigest: sha("2"),
    requiredReviewSeats: ["scope"],
    gate3PlanDigest: sha("7"),
    policyDigest: sha("3"),
  });
  await registry.registerRootTask({
    serverId: id("srv", "a") as never,
    repositoryId: id("rpo", "a") as never,
    repositoryDigest: sha("1"),
    rootTaskId: id("tsk", "a") as never,
    taskNumber: 1,
    laneRole: "storage",
    requiredCapabilities: ["storage.pg16"],
    workspace,
    artifactContract: contract,
    titleDigest: sha("8"),
    titleClassifierPolicyDigest: sha("9"),
    graphPolicyDigest: sha("a"),
    scenarioVersion: 8,
  });
});

after(async () => {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
  } finally {
    await session.close();
  }
});

test("real PostgreSQL 2/3/5-way contention yields one lease and closes exact reservation", async () => {
  const repo = new TaskLeaseV3Repository(databaseUrl, schema);
  const reservations = new WorkspaceReservationV3Repository(
    databaseUrl,
    schema,
  );
  const claimCharacters = [
    ["b", "c"],
    ["d", "e", "f"],
    ["g", "h", "j", "k", "m"],
  ];
  const releaseCharacters = ["n", "p", "q"];

  for (let round = 0; round < claimCharacters.length; round += 1) {
    const expectedTaskRowVersion = 1 + round * 2;
    const characters = claimCharacters[round]!;
    const results = await Promise.all(
      characters.map((character, index) =>
        repo.claim({
          commandId: id("cmd", character) as never,
          taskId: id("tsk", "a") as never,
          agentId: (index === 0 ? id("agt", "a") : id("agt", "b")) as never,
          expectedTaskRowVersion,
          expectedGraphRevision: 0,
          expectedWorkspaceGeneration: 1,
        }),
      ),
    );
    const winners = results.filter((result) => result.kind === "claimed");
    assert.equal(winners.length, 1);
    assert.equal(
      results.filter((result) => result.kind === "conflict").length,
      characters.length - 1,
    );
    const winner = winners[0]!;
    assert.equal(winner.kind, "claimed");
    if (winner.kind !== "claimed") return;
    assert.equal(
      (await reservations.listOpen(id("rpo", "a") as never)).length,
      1,
    );
    const winnerIndex = results.findIndex(
      (result) => result.kind === "claimed",
    );
    const replay = await repo.claim({
      commandId: id("cmd", characters[winnerIndex]!) as never,
      taskId: id("tsk", "a") as never,
      agentId: winner.lease.ownerAgentId,
      expectedTaskRowVersion,
      expectedGraphRevision: 0,
      expectedWorkspaceGeneration: 1,
    });
    assert.deepEqual(replay, winner);
    const release = await repo.release({
      commandId: id("cmd", releaseCharacters[round]!) as never,
      expectedLease: winner.lease,
      reason: "work_yielded",
    });
    assert.equal(release.currentTaskStatus, "todo");
    assert.equal(
      (await reservations.listOpen(id("rpo", "a") as never)).length,
      0,
    );
  }
});

test("real PostgreSQL 2/3-way cross-root absent-row overlap has one winner, prompt losers, and exact release", async (t) => {
  const rounds = [
    { characters: ["k", "m"], taskNumber: 100, path: "shared/cross-root-2.ts" },
    { characters: ["n", "p", "q"], taskNumber: 110, path: "shared/cross-root-3.ts" },
  ];

  for (let round = 0; round < rounds.length; round += 1) {
    const fixture = rounds[round]!;
    for (let index = 0; index < fixture.characters.length; index += 1) {
      await registerRoot(
        fixture.characters[index]!,
        fixture.taskNumber + index,
        ["storage.pg16"],
        [{ kind: "file", path: fixture.path }],
      );
    }
    type ContentionRow = {
      taskId: string;
      status: string;
      rowVersion: number;
      claimRows: number;
      openClaims: number;
      reservationRows: number;
      openReservations: number;
      attemptLedgers: number;
    };
    const taskIdsSql = fixture.characters
      .map((character) => `'${id("tsk", character)}'`)
      .join(",");
    const readContentionImage = () =>
      rawJson<ContentionRow[]>(
        `SELECT coalesce(json_agg(json_build_object('taskId',s.task_id,'status',s.status,'rowVersion',s.row_version,'claimRows',(SELECT count(*) FROM task_claims_v3 c WHERE c.task_id=s.task_id),'openClaims',(SELECT count(*) FROM task_claims_v3 c WHERE c.task_id=s.task_id AND c.closed_at IS NULL),'reservationRows',(SELECT count(*) FROM workspace_reservations_v3 r WHERE r.task_id=s.task_id),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 r WHERE r.task_id=s.task_id AND r.closed_at IS NULL),'attemptLedgers',(SELECT count(*) FROM task_attempt_ledgers_v3 l WHERE l.task_id=s.task_id)) ORDER BY s.task_id),'[]'::json) FROM task_v3_state s WHERE s.task_id IN(${taskIdsSql});`,
      );
    const beforeImage = await readContentionImage();
    assert.equal(beforeImage.length, fixture.characters.length);
    for (const row of beforeImage) {
      assert.deepEqual(row, {
        taskId: row.taskId,
        status: "todo",
        rowVersion: 1,
        claimRows: 0,
        openClaims: 0,
        reservationRows: 0,
        openReservations: 0,
        attemptLedgers: 0,
      });
    }
    const latches = new ControlledLatches();
    const namespaceHold = latches.hold("claim_after_repository_lock");
    const repository = new TaskLeaseV3Repository(databaseUrl, schema, latches);
    const claim = (character: string, index: number) =>
      repository.claim({
        commandId: variant("cmd", character, String(round)) as never,
        taskId: id("tsk", character) as never,
        agentId: (index % 2 === 0 ? id("agt", "a") : id("agt", "b")) as never,
        expectedTaskRowVersion: 1,
        expectedGraphRevision: 0,
        expectedWorkspaceGeneration: 1,
      });

    const first = claim(fixture.characters[0]!, 0);
    await namespaceHold.reached;
    let loserSettlements = 0;
    const losers = fixture.characters.slice(1).map((character, index) =>
      claim(character, index + 1).finally(() => {
        loserSettlements += 1;
      }),
    );
    await new Promise((resolve) => setTimeout(resolve, 75));
    assert.equal(loserSettlements, 0);
    const releasedAt = Date.now();
    namespaceHold.release();
    const results = await settleWithin(Promise.all([first, ...losers]), 1_500);
    const releaseToSettleMs = Date.now() - releasedAt;
    assert.ok(releaseToSettleMs < 1_500);
    assert.equal(loserSettlements, fixture.characters.length - 1);
    assert.equal(
      latches.counts.get("claim_after_repository_lock"),
      fixture.characters.length,
    );

    const winners = results.filter((result) => result.kind === "claimed");
    const conflicts = results.filter((result) => result.kind === "conflict");
    assert.equal(winners.length, 1);
    assert.equal(conflicts.length, fixture.characters.length - 1);
    for (const conflict of conflicts) {
      if (conflict.kind === "conflict") {
        assert.equal(conflict.code, "TASK_NOT_READY");
        assert.equal(conflict.observedTaskRowVersion, 1);
        assert.equal(conflict.observedLeaseEpoch, null);
      }
    }
    const winner = winners[0]!;
    assert.equal(winner.kind, "claimed");
    if (winner.kind !== "claimed") return;

    const image = await readContentionImage();
    assert.equal(image.length, fixture.characters.length);
    for (const row of image) {
      if (row.taskId === winner.lease.taskId) {
        assert.deepEqual(row, {
          taskId: winner.lease.taskId,
          status: "in_progress",
          rowVersion: 2,
          claimRows: 1,
          openClaims: 1,
          reservationRows: 1,
          openReservations: 1,
          attemptLedgers: 1,
        });
      } else {
        assert.deepEqual(row, {
          taskId: row.taskId,
          status: "todo",
          rowVersion: 1,
          claimRows: 0,
          openClaims: 0,
          reservationRows: 0,
          openReservations: 0,
          attemptLedgers: 0,
        });
      }
    }
    const loserBefore = beforeImage.filter(
      (row) => row.taskId !== winner.lease.taskId,
    );
    const loserAfter = image.filter(
      (row) => row.taskId !== winner.lease.taskId,
    );
    assert.deepEqual(loserAfter, loserBefore);
    const winningReservation = await rawJson<{
      taskId: string;
      claimId: string;
      leaseId: string;
      attempt: number;
      leaseEpoch: number;
      kind: string;
      path: string;
    }>(
      `SELECT json_build_object('taskId',task_id,'claimId',claim_id,'leaseId',lease_id,'attempt',attempt,'leaseEpoch',lease_epoch,'kind',path_kind,'path',path) FROM workspace_reservations_v3 WHERE task_id='${winner.lease.taskId}' AND closed_at IS NULL;`,
    );
    assert.deepEqual(winningReservation, {
      taskId: winner.lease.taskId,
      claimId: winner.lease.claimId,
      leaseId: winner.lease.leaseId,
      attempt: winner.lease.attempt,
      leaseEpoch: winner.lease.leaseEpoch,
      kind: "file",
      path: fixture.path,
    });

    const released = await repository.release({
      commandId: variant("cmd", fixture.characters[0]!, String(round + 5)) as never,
      expectedLease: winner.lease,
      reason: "work_yielded",
    });
    assert.equal(released.currentTaskStatus, "todo");
    const releasedImage = await rawJson<{
      status: string;
      rowVersion: number;
      openClaims: number;
      closedClaims: number;
      openReservations: number;
      closedReservations: number;
      reservationCloseReason: string;
    }>(
      `SELECT json_build_object('status',(SELECT status FROM task_v3_state WHERE task_id='${winner.lease.taskId}'),'rowVersion',(SELECT row_version FROM task_v3_state WHERE task_id='${winner.lease.taskId}'),'openClaims',(SELECT count(*) FROM task_claims_v3 WHERE task_id='${winner.lease.taskId}' AND closed_at IS NULL),'closedClaims',(SELECT count(*) FROM task_claims_v3 WHERE task_id='${winner.lease.taskId}' AND closed_at IS NOT NULL),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE repository_id='${id("rpo", "a")}' AND closed_at IS NULL),'closedReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE task_id='${winner.lease.taskId}' AND closed_at IS NOT NULL),'reservationCloseReason',(SELECT close_reason FROM workspace_reservations_v3 WHERE task_id='${winner.lease.taskId}'));`,
    );
    assert.deepEqual(releasedImage, {
      status: "todo",
      rowVersion: 3,
      openClaims: 0,
      closedClaims: 1,
      openReservations: 0,
      closedReservations: 1,
      reservationCloseReason: "work_yielded",
    });
    t.diagnostic(
      JSON.stringify({
        proof: "cross_root_absent_row_overlap",
        contenders: fixture.characters.length,
        exactStartSynchronization: {
          firstHeldAtRepositoryNamespaceLock: true,
          loserSettlementsBeforeRelease: 0,
          repositoryNamespaceLockArrivals:
            latches.counts.get("claim_after_repository_lock"),
        },
        boundedLoserLatency: { releaseToSettleMs, boundMs: 1_500 },
        loserZeroEffect: { before: loserBefore, after: loserAfter },
        winnerReservation: winningReservation,
        winnerRelease: releasedImage,
      }),
    );
  }
});

test("cross-root absent-reservation schedule kills a repository-lock bypass mutant", async (t) => {
  const characters = ["r", "s"];
  const path = "shared/cross-root-mutant.ts";
  for (let index = 0; index < characters.length; index += 1) {
    await registerRoot(
      characters[index]!,
      120 + index,
      ["storage.pg16"],
      [{ kind: "file", path }],
    );
  }
  const absentRowBarrier = new AsyncBarrier(characters.length);
  const claimWithoutRepositoryNamespaceLock = async (character: string) => {
    const session = await PsqlSession.open(databaseUrl);
    try {
      await session.execute(
        `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SELECT 1 FROM task_v3_graphs WHERE root_task_id='${id("tsk", character)}' FOR UPDATE; SELECT 1 FROM task_v3_state WHERE root_task_id='${id("tsk", character)}' ORDER BY task_id FOR UPDATE;`,
      );
      const before = await session.queryJson<{ openReservations: number }>(
        `SELECT json_build_object('openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE repository_id='${id("rpo", "a")}' AND closed_at IS NULL));`,
      );
      assert.equal(before.openReservations, 0);
      await absentRowBarrier.arrive();
      await session.execute(
        `UPDATE task_v3_state SET status='in_progress',row_version=2 WHERE task_id='${id("tsk", character)}';
         INSERT INTO task_claims_v3(claim_id,root_task_id,task_id,lease_id,owner_agent_id,attempt,lease_epoch,lease_revision,fence_token,acquired_at,expires_at,task_row_version,graph_revision)
           VALUES('${id("clm", character)}','${id("tsk", character)}','${id("tsk", character)}','${id("lse", character)}','${id("agt", "a")}',1,1,1,'${id("fnc", character)}',clock_timestamp(),clock_timestamp()+interval '30 seconds',2,0);
         INSERT INTO task_attempt_ledgers_v3(task_id,attempt,ledger_revision) VALUES('${id("tsk", character)}',1,1);
         INSERT INTO task_attempt_contributors_v3(task_id,attempt,agent_id,source,source_artifact_id) VALUES('${id("tsk", character)}',1,'${id("agt", "a")}','claim_owner',NULL);
         INSERT INTO workspace_reservations_v3(reservation_id,repository_id,task_id,claim_id,lease_id,attempt,lease_epoch,path_kind,path)
           VALUES('${id("rsv", character)}','${id("rpo", "a")}','${id("tsk", character)}','${id("clm", character)}','${id("lse", character)}',1,1,'file','${path}');
         COMMIT;`,
      );
      await session.close();
      return true;
    } catch (error) {
      await session.rollbackAndClose();
      throw error;
    }
  };

  const mutantWinners = await settleWithin(
    Promise.all(characters.map(claimWithoutRepositoryNamespaceLock)),
    1_500,
  );
  const mutantImage = await rawJson<{
    winners: number;
    openClaims: number;
    openReservations: number;
    distinctRoots: number;
  }>(
    `SELECT json_build_object('winners',(SELECT count(*) FROM task_v3_state WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND status='in_progress'),'openClaims',(SELECT count(*) FROM task_claims_v3 WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND closed_at IS NULL),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND closed_at IS NULL),'distinctRoots',(SELECT count(DISTINCT root_task_id) FROM task_claims_v3 WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND closed_at IS NULL));`,
  );
  assert.deepEqual(mutantWinners, [true, true]);
  assert.deepEqual(mutantImage, {
    winners: 2,
    openClaims: 2,
    openReservations: 2,
    distinctRoots: 2,
  });
  assert.equal(
    mutantImage.winners === 1 && mutantImage.openReservations === 1,
    false,
  );
  t.diagnostic(
    JSON.stringify({
      mutant: "repository_namespace_lock_deleted",
      synchronizedAbsentReservationReaders: characters.length,
      observed: mutantImage,
      oneWinnerInvariant: false,
      killed: true,
    }),
  );

  await raw(`
    UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND closed_at IS NULL;
    UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND closed_at IS NULL;
    UPDATE task_v3_state SET status='todo',row_version=row_version+1 WHERE task_id IN('${id("tsk", "r")}','${id("tsk", "s")}') AND status='in_progress';
  `);
});

test("renewal rejects before the server-owned boundary without mutation", async () => {
  const repo = new TaskLeaseV3Repository(databaseUrl, schema);
  const claimed = await repo.claim({
    commandId: id("cmd", "r") as never,
    taskId: id("tsk", "a") as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: 7,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(claimed.kind, "claimed");
  if (claimed.kind !== "claimed") return;
  const result = await repo.renew({
    commandId: id("cmd", "s") as never,
    expectedLease: claimed.lease,
  });
  assert.equal(result.kind, "rejected");
  if (result.kind === "rejected") {
    assert.equal(result.code, "TASK_LEASE_RENEWAL_TOO_EARLY");
    assert.equal(result.observedLeaseRevision, claimed.lease.leaseRevision);
    assert.notEqual(result.retryNotBefore, null);
  }
});

test("renew waits on exact claim row past expiry and rejects with zero mutation", async (t) => {
  await registerRoot("w", 130);
  const claimed = await new TaskLeaseV3Repository(databaseUrl, schema).claim({
    commandId: variant("cmd", "w", "0") as never,
    taskId: id("tsk", "w") as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: 1,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(claimed.kind, "claimed");
  if (claimed.kind !== "claimed") return;
  await raw(
    `UPDATE task_claims_v3 SET expires_at=clock_timestamp()+interval '700 milliseconds' WHERE claim_id='${claimed.lease.claimId}';`,
  );
  const expiration = await rawJson<{ expiresAt: string }>(
    `SELECT json_build_object('expiresAt',to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) FROM task_claims_v3 WHERE claim_id='${claimed.lease.claimId}';`,
  );
  const expectedLease = {
    ...claimed.lease,
    expiresAt: expiration.expiresAt,
  } as TaskLeaseV3;
  const leaseImage = () =>
    rawJson<{
      taskStatus: string;
      taskRowVersion: number;
      leaseRevision: number;
      leaseTaskRowVersion: number;
      expiresAt: string;
      closedAt: string | null;
      openReservations: number;
      artifacts: number;
    }>(
      `SELECT json_build_object('taskStatus',(SELECT status FROM task_v3_state WHERE task_id='${expectedLease.taskId}'),'taskRowVersion',(SELECT row_version FROM task_v3_state WHERE task_id='${expectedLease.taskId}'),'leaseRevision',(SELECT lease_revision FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'leaseTaskRowVersion',(SELECT task_row_version FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'expiresAt',(SELECT to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'closedAt',(SELECT to_char(closed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NULL),'artifacts',(SELECT count(*) FROM artifacts_v3 WHERE task_id='${expectedLease.taskId}'));`,
    );
  const before = await leaseImage();
  const locker = await PsqlSession.open(databaseUrl);
  const latches = new ControlledLatches();
  const beforeLock = latches.hold("renew_before_claim_lock");
  try {
    await locker.execute(
      `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SELECT 1 FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}' FOR UPDATE;`,
    );
    const renewal = new TaskLeaseV3Repository(
      databaseUrl,
      schema,
      latches,
    ).renew({
      commandId: variant("cmd", "w", "1") as never,
      expectedLease,
    });
    await beforeLock.reached;
    beforeLock.release();
    await new Promise((resolve) => setTimeout(resolve, 900));
    await locker.execute("COMMIT;");
    const result = await settleWithin(renewal, 1_500);
    assert.equal(result.kind, "rejected");
    if (result.kind === "rejected") {
      assert.equal(result.code, "TASK_LEASE_EXPIRED");
      assert.equal(result.observedLeaseRevision, 1);
      assert.ok(result.serverObservedAt >= expectedLease.expiresAt);
    }
    assert.deepEqual(await leaseImage(), before);
    assert.equal(latches.counts.get("renew_before_claim_lock"), 1);
    assert.equal(
      latches.counts.get("renew_after_claim_lock_and_server_clock"),
      1,
    );
    t.diagnostic(
      JSON.stringify({
        proof: "renew_exact_row_wait_crosses_expiry",
        killedMutant: "server_clock_sampled_before_claim_row_lock",
        zeroEffect: before,
      }),
    );
  } finally {
    await locker.rollbackAndClose();
  }
});

test("renewal update-time expiry CAS rejects a lease that expires after the locked server-clock sample", async (t) => {
  await registerRoot("v", 131);
  const claimed = await new TaskLeaseV3Repository(databaseUrl, schema).claim({
    commandId: variant("cmd", "v", "0") as never,
    taskId: id("tsk", "v") as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: 1,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(claimed.kind, "claimed");
  if (claimed.kind !== "claimed") return;
  await raw(
    `UPDATE task_claims_v3 SET expires_at=clock_timestamp()+interval '600 milliseconds' WHERE claim_id='${claimed.lease.claimId}';`,
  );
  const expiration = await rawJson<{ expiresAt: string }>(
    `SELECT json_build_object('expiresAt',to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) FROM task_claims_v3 WHERE claim_id='${claimed.lease.claimId}';`,
  );
  const expectedLease = {
    ...claimed.lease,
    expiresAt: expiration.expiresAt,
  } as TaskLeaseV3;
  const image = () =>
    rawJson<{
      taskStatus: string;
      taskRowVersion: number;
      leaseRevision: number;
      leaseTaskRowVersion: number;
      expiresAt: string;
      closedClaims: number;
      openReservations: number;
    }>(
      `SELECT json_build_object('taskStatus',(SELECT status FROM task_v3_state WHERE task_id='${expectedLease.taskId}'),'taskRowVersion',(SELECT row_version FROM task_v3_state WHERE task_id='${expectedLease.taskId}'),'leaseRevision',(SELECT lease_revision FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'leaseTaskRowVersion',(SELECT task_row_version FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'expiresAt',(SELECT to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'closedClaims',(SELECT count(*) FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NOT NULL),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NULL));`,
    );
  const before = await image();
  const latches = new ControlledLatches();
  const afterClock = latches.hold("renew_after_claim_lock_and_server_clock");
  const renewal = new TaskLeaseV3Repository(
    databaseUrl,
    schema,
    latches,
  ).renew({
    commandId: variant("cmd", "v", "1") as never,
    expectedLease,
  });
  await afterClock.reached;
  await new Promise((resolve) => setTimeout(resolve, 750));
  afterClock.release();
  const result = await settleWithin(renewal, 1_500);
  assert.equal(result.kind, "rejected");
  if (result.kind === "rejected") {
    assert.equal(result.code, "TASK_LEASE_EXPIRED");
    assert.equal(result.observedLeaseRevision, 1);
    assert.ok(result.serverObservedAt >= expectedLease.expiresAt);
  }
  assert.deepEqual(await image(), before);
  t.diagnostic(
    JSON.stringify({
      proof: "renew_update_time_expiry_cas",
      killedMutant: "renewal_update_missing_expires_at_server_clock_cas",
      zeroEffect: before,
    }),
  );
});

test("renew versus release follows repository-root-claim-task order without a public-operation lock cycle", async (t) => {
  await registerRoot("y", 133);
  const claimed = await new TaskLeaseV3Repository(databaseUrl, schema).claim({
    commandId: variant("cmd", "y", "0") as never,
    taskId: id("tsk", "y") as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: 1,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(claimed.kind, "claimed");
  if (claimed.kind !== "claimed") return;
  await raw(
    `UPDATE task_claims_v3 SET expires_at=clock_timestamp()+interval '4 seconds' WHERE claim_id='${claimed.lease.claimId}';`,
  );
  const expiration = await rawJson<{ expiresAt: string }>(
    `SELECT json_build_object('expiresAt',to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) FROM task_claims_v3 WHERE claim_id='${claimed.lease.claimId}';`,
  );
  const expectedLease = {
    ...claimed.lease,
    expiresAt: expiration.expiresAt,
  } as TaskLeaseV3;
  const latches = new ControlledLatches();
  const repositoryHold = latches.hold("renew_after_repository_lock");
  const renewal = new TaskLeaseV3Repository(
    databaseUrl,
    schema,
    latches,
  ).renew({
    commandId: variant("cmd", "y", "1") as never,
    expectedLease,
  });
  await repositoryHold.reached;
  const release = new TaskLeaseV3Repository(databaseUrl, schema).release({
    commandId: variant("cmd", "y", "2") as never,
    expectedLease,
    reason: "work_yielded",
  });
  repositoryHold.release();
  const settled = await settleWithin(
    Promise.allSettled([renewal, release]),
    1_500,
  );
  assert.equal(settled[0].status, "fulfilled");
  if (settled[0].status !== "fulfilled") return;
  assert.equal(settled[0].value.kind, "renewed");
  if (settled[0].value.kind !== "renewed") return;
  assert.equal(settled[1].status, "rejected");
  if (settled[1].status !== "rejected") return;
  assert.ok(settled[1].reason instanceof StorageError);
  assert.equal(settled[1].reason.code, "TASK_LEASE_STALE");
  assert.notEqual(settled[1].reason.code, "DATABASE_UNAVAILABLE");
  const finalImage = await rawJson<{
    taskStatus: string;
    taskRowVersion: number;
    claimId: string;
    leaseId: string;
    ownerAgentId: string;
    attempt: number;
    leaseEpoch: number;
    leaseRevision: number;
    fenceToken: string;
    expiresAt: string;
    leaseTaskRowVersion: number;
    graphRevision: number;
    openClaims: number;
    closedClaims: number;
    reservationTaskId: string;
    reservationClaimId: string;
    reservationLeaseId: string;
    reservationAttempt: number;
    reservationLeaseEpoch: number;
    reservationKind: string;
    reservationPath: string;
    openReservations: number;
    closedReservations: number;
    claimCloseReason: string | null;
    reservationCloseReason: string | null;
  }>(
    `SELECT json_build_object('taskStatus',(SELECT status FROM task_v3_state WHERE task_id='${expectedLease.taskId}'),'taskRowVersion',(SELECT row_version FROM task_v3_state WHERE task_id='${expectedLease.taskId}'),'claimId',(SELECT claim_id FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'leaseId',(SELECT lease_id FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'ownerAgentId',(SELECT owner_agent_id FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'attempt',(SELECT attempt FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'leaseEpoch',(SELECT lease_epoch FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'leaseRevision',(SELECT lease_revision FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'fenceToken',(SELECT fence_token FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'expiresAt',(SELECT to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'leaseTaskRowVersion',(SELECT task_row_version FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'graphRevision',(SELECT graph_revision FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'openClaims',(SELECT count(*) FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NULL),'closedClaims',(SELECT count(*) FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NOT NULL),'reservationTaskId',(SELECT task_id FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationClaimId',(SELECT claim_id FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationLeaseId',(SELECT lease_id FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationAttempt',(SELECT attempt FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationLeaseEpoch',(SELECT lease_epoch FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationKind',(SELECT path_kind FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationPath',(SELECT path FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NULL),'closedReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}' AND closed_at IS NOT NULL),'claimCloseReason',(SELECT close_reason FROM task_claims_v3 WHERE claim_id='${expectedLease.claimId}'),'reservationCloseReason',(SELECT close_reason FROM workspace_reservations_v3 WHERE claim_id='${expectedLease.claimId}'));`,
  );
  assert.deepEqual(finalImage, {
    taskStatus: "in_progress",
    taskRowVersion: settled[0].value.currentLease.taskRowVersion,
    claimId: settled[0].value.currentLease.claimId,
    leaseId: settled[0].value.currentLease.leaseId,
    ownerAgentId: settled[0].value.currentLease.ownerAgentId,
    attempt: settled[0].value.currentLease.attempt,
    leaseEpoch: settled[0].value.currentLease.leaseEpoch,
    leaseRevision: 2,
    fenceToken: settled[0].value.currentLease.fenceToken,
    expiresAt: settled[0].value.currentLease.expiresAt,
    leaseTaskRowVersion: settled[0].value.currentLease.taskRowVersion,
    graphRevision: settled[0].value.currentLease.graphRevision,
    openClaims: 1,
    closedClaims: 0,
    reservationTaskId: settled[0].value.currentLease.taskId,
    reservationClaimId: settled[0].value.currentLease.claimId,
    reservationLeaseId: settled[0].value.currentLease.leaseId,
    reservationAttempt: settled[0].value.currentLease.attempt,
    reservationLeaseEpoch: settled[0].value.currentLease.leaseEpoch,
    reservationKind: "subtree",
    reservationPath: "fixtures/y",
    openReservations: 1,
    closedReservations: 0,
    claimCloseReason: null,
    reservationCloseReason: null,
  });
  assert.equal(latches.counts.get("renew_after_repository_lock"), 1);
  t.diagnostic(
    JSON.stringify({
      proof: "renew_release_public_operation_lock_cycle_killed",
      lockOrder: ["repository", "root", "claim", "task"],
      semanticWinner: "renewed",
      typedLoser: "TASK_LEASE_STALE",
      databaseUnavailable: 0,
      finalImage,
    }),
  );
});

test("claim atomically expires then reclaims with next history-inclusive epoch and stale owner release has zero effect", async (t) => {
  await registerRoot("x", 132);
  const repository = new TaskLeaseV3Repository(databaseUrl, schema);
  const first = await repository.claim({
    commandId: variant("cmd", "x", "0") as never,
    taskId: id("tsk", "x") as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: 1,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(first.kind, "claimed");
  if (first.kind !== "claimed") return;
  await raw(
    `UPDATE task_claims_v3 SET acquired_at=clock_timestamp()-interval '2 seconds',expires_at=clock_timestamp()-interval '1 second' WHERE claim_id='${first.lease.claimId}';`,
  );
  const second = await repository.claim({
    commandId: variant("cmd", "x", "1") as never,
    taskId: id("tsk", "x") as never,
    agentId: id("agt", "b") as never,
    expectedTaskRowVersion: first.lease.taskRowVersion,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(second.kind, "claimed");
  if (second.kind !== "claimed") return;
  assert.equal(second.lease.attempt, 2);
  assert.equal(second.lease.leaseEpoch, 2);
  assert.equal(second.lease.taskRowVersion, first.lease.taskRowVersion + 2);
  const image = () =>
    rawJson<{
      taskStatus: string;
      taskRowVersion: number;
      oldClaimClosedReason: string;
      oldReservationsClosedReason: string;
      newOpenClaims: number;
      newOpenReservations: number;
      newAttempt: number;
      newEpoch: number;
      attemptLedgers: number;
    }>(
      `SELECT json_build_object('taskStatus',(SELECT status FROM task_v3_state WHERE task_id='${second.lease.taskId}'),'taskRowVersion',(SELECT row_version FROM task_v3_state WHERE task_id='${second.lease.taskId}'),'oldClaimClosedReason',(SELECT close_reason FROM task_claims_v3 WHERE claim_id='${first.lease.claimId}'),'oldReservationsClosedReason',(SELECT close_reason FROM workspace_reservations_v3 WHERE claim_id='${first.lease.claimId}'),'newOpenClaims',(SELECT count(*) FROM task_claims_v3 WHERE claim_id='${second.lease.claimId}' AND closed_at IS NULL),'newOpenReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE claim_id='${second.lease.claimId}' AND closed_at IS NULL),'newAttempt',(SELECT attempt FROM task_claims_v3 WHERE claim_id='${second.lease.claimId}'),'newEpoch',(SELECT lease_epoch FROM task_claims_v3 WHERE claim_id='${second.lease.claimId}'),'attemptLedgers',(SELECT count(*) FROM task_attempt_ledgers_v3 WHERE task_id='${second.lease.taskId}'));`,
    );
  const afterReplacement = await image();
  assert.deepEqual(afterReplacement, {
    taskStatus: "in_progress",
    taskRowVersion: first.lease.taskRowVersion + 2,
    oldClaimClosedReason: "server_expired",
    oldReservationsClosedReason: "server_expired",
    newOpenClaims: 1,
    newOpenReservations: 1,
    newAttempt: 2,
    newEpoch: 2,
    attemptLedgers: 2,
  });
  await assert.rejects(
    repository.release({
      commandId: variant("cmd", "x", "2") as never,
      expectedLease: first.lease,
      reason: "work_yielded",
    }),
    (error: unknown) =>
      error instanceof StorageError && error.code === "TASK_LEASE_STALE",
  );
  assert.deepEqual(await image(), afterReplacement);
  t.diagnostic(
    JSON.stringify({
      proof: "expired_claim_same_transaction_epoch_plus_one_reclaim",
      oldLease: {
        attempt: first.lease.attempt,
        epoch: first.lease.leaseEpoch,
      },
      replacementLease: {
        attempt: second.lease.attempt,
        epoch: second.lease.leaseEpoch,
      },
      staleOwnerReleaseZeroEffect: true,
      image: afterReplacement,
    }),
  );
});

test("graph proposal reaches the named locked-snapshot latch before validation and forces a waiter", async () => {
  await registerRoot("b", 20);
  const lease = await claimRoot("b", "t");
  const predecessor = await createPredecessor("b");
  const latches = new ControlledLatches();
  const hold = latches.hold("graph_after_locked_snapshot");
  const titlePort = new ExactTitlePrivacyPort(sha("9"));
  const graph = new TaskGraphV3Repository(
    databaseUrl,
    schema,
    titlePort,
    latches,
  );
  const proposal = graph.propose(graphCommand("b", "v", lease, predecessor));
  await hold.reached;
  let transitionSettled = false;
  const transition = (async () => {
    const session = await PsqlSession.open(databaseUrl);
    try {
      await session.execute(
        `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SELECT 1 FROM workspace_repositories WHERE repository_id='${id("rpo", "a")}' FOR UPDATE; SELECT 1 FROM task_v3_graphs WHERE root_task_id='${id("tsk", "b")}' FOR UPDATE; UPDATE task_v3_state SET status='waiting',row_version=row_version+1 WHERE task_id='${id("tsk", "b")}'; COMMIT;`,
      );
    } finally {
      transitionSettled = true;
      await session.close();
    }
  })();
  await new Promise((resolve) => setTimeout(resolve, 75));
  assert.equal(transitionSettled, false);
  assert.equal(latches.counts.get("graph_after_locked_snapshot"), 1);
  hold.release();
  const result = await proposal;
  await transition;
  assert.equal(result.children.length, 1);
  assert.equal(titlePort.calls.length, 1);
  const state = await rawJson<{ graphRevision: number; status: string }>(
    `SELECT json_build_object('graphRevision',g.graph_revision,'status',s.status) FROM task_v3_graphs g JOIN task_v3_state s ON s.task_id=g.root_task_id WHERE g.root_task_id='${id("tsk", "b")}';`,
  );
  assert.equal(state.graphRevision, 1);
  assert.equal(state.status, "waiting");
});

test("reverse graph schedule forces proposal to wait then reject the new revision", async () => {
  await registerRoot("c", 30);
  const lease = await claimRoot("c", "w");
  const predecessor = await createPredecessor("c");
  const transition = await PsqlSession.open(databaseUrl);
  await transition.execute(
    `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SELECT 1 FROM workspace_repositories WHERE repository_id='${id("rpo", "a")}' FOR UPDATE; SELECT 1 FROM task_v3_graphs WHERE root_task_id='${id("tsk", "c")}' FOR UPDATE; UPDATE task_v3_graphs SET graph_revision=graph_revision+1 WHERE root_task_id='${id("tsk", "c")}';`,
  );
  const latches = new ControlledLatches();
  const graph = new TaskGraphV3Repository(
    databaseUrl,
    schema,
    new ExactTitlePrivacyPort(sha("9")),
    latches,
  );
  let settled = false;
  const proposal = graph
    .propose(graphCommand("c", "x", lease, predecessor))
    .finally(() => {
      settled = true;
    });
  await new Promise((resolve) => setTimeout(resolve, 75));
  assert.equal(settled, false);
  await transition.execute("COMMIT;");
  await transition.close();
  await assert.rejects(
    proposal,
    (error: unknown) =>
      error instanceof StorageError && error.code === "TASK_GRAPH_REVISION_STALE",
  );
  assert.equal(latches.counts.get("graph_after_locked_snapshot"), 1);
  const effects = await rawJson<{ children: number; coordinations: number }>(
    `SELECT json_build_object('children',(SELECT count(*) FROM task_v3_state WHERE root_task_id='${id("tsk", "c")}' AND task_id<>root_task_id),'coordinations',(SELECT count(*) FROM task_v3_coordinations WHERE root_task_id='${id("tsk", "c")}'));`,
  );
  assert.deepEqual(effects, { children: 0, coordinations: 0 });
});

test("one committed reply rejects a second distinct graph command before stale graph checks", async () => {
  await registerRoot("d", 40);
  const lease = await claimRoot("d", "y");
  const predecessor = await createPredecessor("d");
  const graph = new TaskGraphV3Repository(
    databaseUrl,
    schema,
    new ExactTitlePrivacyPort(sha("9")),
  );
  const first = graphCommand("d", "z", lease, predecessor);
  const result = await graph.propose(first);
  assert.equal(result.children.length, 1);
  await assert.rejects(
    graph.propose({ ...first, commandId: variant("cmd", "z", "1") as never }),
    (error: unknown) =>
      error instanceof StorageError &&
      error.code === "TASK_COORDINATION_ALREADY_COMMITTED",
  );
  const counts = await rawJson<{ graphRevision: number; coordinationCount: number }>(
    `SELECT json_build_object('graphRevision',(SELECT graph_revision FROM task_v3_graphs WHERE root_task_id='${id("tsk", "d")}'),'coordinationCount',(SELECT count(*) FROM task_v3_coordinations WHERE committed_reply_message_id='${predecessor.committedReplyMessageId}'));`,
  );
  assert.deepEqual(counts, { graphRevision: 1, coordinationCount: 1 });
});

test("title privacy policy mismatch fails before every graph effect", async () => {
  await registerRoot("e", 50);
  const lease = await claimRoot("e", "0");
  const predecessor = await createPredecessor("e");
  const port = new ExactTitlePrivacyPort(sha("9"), sha("8"));
  await assert.rejects(
    new TaskGraphV3Repository(databaseUrl, schema, port).propose(
      graphCommand("e", "1", lease, predecessor),
    ),
    (error: unknown) =>
      error instanceof StorageError && error.code === "TASK_TITLE_POLICY_STALE",
  );
  assert.equal(port.calls.length, 1);
  const effects = await rawJson<{
    graphRevision: number;
    children: number;
    coordinations: number;
    openClaim: number;
  }>(
    `SELECT json_build_object('graphRevision',(SELECT graph_revision FROM task_v3_graphs WHERE root_task_id='${id("tsk", "e")}'),'children',(SELECT count(*) FROM task_v3_state WHERE root_task_id='${id("tsk", "e")}' AND task_id<>root_task_id),'coordinations',(SELECT count(*) FROM task_v3_coordinations WHERE root_task_id='${id("tsk", "e")}'),'openClaim',(SELECT count(*) FROM task_claims_v3 WHERE task_id='${id("tsk", "e")}' AND closed_at IS NULL));`,
  );
  assert.deepEqual(effects, {
    graphRevision: 0,
    children: 0,
    coordinations: 0,
    openClaim: 1,
  });
});

test("claim versus block reaches both repository-first latches and serializes without deadlock", async () => {
  await registerRoot("f", 60);
  const lease = await claimRoot("f", "2");
  const latches = new ControlledLatches();
  const blockHold = latches.hold("block_after_repository_lock");
  const graph = new TaskGraphV3Repository(
    databaseUrl,
    schema,
    new ExactTitlePrivacyPort(sha("9")),
    latches,
  );
  const block = graph.block({
    commandId: id("cmd", "3") as never,
    expectedLease: lease,
    expectedTaskStatus: "in_progress",
    expectedTaskRowVersion: lease.taskRowVersion,
    reason: "execution_failed",
  });
  await blockHold.reached;
  let claimSettled = false;
  const claim = new TaskLeaseV3Repository(databaseUrl, schema, latches)
    .claim({
      commandId: id("cmd", "4") as never,
      taskId: id("tsk", "f") as never,
      agentId: id("agt", "b") as never,
      expectedTaskRowVersion: lease.taskRowVersion,
      expectedGraphRevision: 0,
      expectedWorkspaceGeneration: 1,
    })
    .finally(() => {
      claimSettled = true;
    });
  await new Promise((resolve) => setTimeout(resolve, 75));
  assert.equal(claimSettled, false);
  blockHold.release();
  const blocked = await block;
  const claimResult = await claim;
  assert.equal(blocked.affected.length, 1);
  assert.equal(claimResult.kind, "conflict");
  assert.equal(latches.counts.get("block_after_repository_lock"), 1);
  assert.equal(latches.counts.get("claim_after_repository_lock"), 1);
});

test("inverse root-before-repository lock-order mutant is killed by bounded PostgreSQL lock timeout", async (t) => {
  await registerRoot("g", 70);
  const inverse = await PsqlSession.open(databaseUrl);
  const conforming = await PsqlSession.open(databaseUrl);
  try {
    await inverse.execute(
      `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SET LOCAL lock_timeout='250ms'; SELECT 1 FROM task_v3_graphs WHERE root_task_id='${id("tsk", "g")}' FOR UPDATE;`,
    );
    await conforming.execute(
      `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SET LOCAL lock_timeout='250ms'; SELECT 1 FROM workspace_repositories WHERE repository_id='${id("rpo", "a")}' FOR UPDATE;`,
    );
    const inverseWait = inverse.execute(
      `SELECT 1 FROM workspace_repositories WHERE repository_id='${id("rpo", "a")}' FOR UPDATE;`,
      2_000,
    );
    await new Promise((resolve) => setTimeout(resolve, 30));
    const conformingWait = conforming.execute(
      `SELECT 1 FROM task_v3_graphs WHERE root_task_id='${id("tsk", "g")}' FOR UPDATE;`,
      2_000,
    );
    const outcomes = await Promise.allSettled([inverseWait, conformingWait]);
    const killed = outcomes.filter(
      (outcome) =>
        outcome.status === "rejected" &&
        outcome.reason instanceof StorageError &&
        outcome.reason.code === "DATABASE_UNAVAILABLE" &&
        /lock timeout|deadlock detected/u.test(
          JSON.stringify(outcome.reason.causeDetail),
        ),
    );
    assert.equal(killed.length, 1);
    t.diagnostic(
      JSON.stringify({
        mutant: "inverse_root_before_repository_lock_order",
        boundMs: 2_000,
        outcomes: outcomes.map((outcome) =>
          outcome.status === "fulfilled"
            ? { status: "fulfilled" }
            : {
                status: "rejected",
                code:
                  outcome.reason instanceof StorageError
                    ? outcome.reason.code
                    : "UNKNOWN",
              },
        ),
        killedCount: killed.length,
        killed: killed.length === 1,
      }),
    );
  } finally {
    await inverse.rollbackAndClose();
    await conforming.rollbackAndClose();
  }
});

test("transitive block traverses the exact stored length-2 lifecycle path", async () => {
  await registerRoot("h", 80);
  const lease = await claimRoot("h", "5");
  const task22 = variant("tsk", "h", "2");
  const task23 = variant("tsk", "h", "3");
  await raw(`
    INSERT INTO tasks(task_id,server_id,task_number,status,row_version) VALUES
      ('${task22}','${id("srv", "a")}',81,'todo',1),
      ('${task23}','${id("srv", "a")}',82,'todo',1);
    INSERT INTO task_v3_state(task_id,server_id,root_task_id,parent_task_id,repository_id,lane_role,status,row_version,required_capabilities,workspace_contract_digest,artifact_contract_digest,title_digest,title_classifier_policy_digest) VALUES
      ('${task22}','${id("srv", "a")}','${id("tsk", "h")}','${task23}','${id("rpo", "a")}','storage','waiting',1,'[]','${sha("b")}','${sha("c")}','${sha("d")}','${sha("9")}'),
      ('${task23}','${id("srv", "a")}','${id("tsk", "h")}','${id("tsk", "h")}','${id("rpo", "a")}','storage','waiting',1,'[]','${sha("e")}','${sha("f")}','${sha("0")}','${sha("9")}');
    INSERT INTO task_v3_edges(root_task_id,prerequisite_task_id,dependent_task_id,edge_kind) VALUES
      ('${id("tsk", "h")}','${id("tsk", "h")}','${task22}','depends_on'),
      ('${id("tsk", "h")}','${task22}','${task23}','contains');
  `);
  const result = await new TaskGraphV3Repository(
    databaseUrl,
    schema,
    new ExactTitlePrivacyPort(sha("9")),
  ).block({
    commandId: id("cmd", "6") as never,
    expectedLease: lease,
    expectedTaskStatus: "in_progress",
    expectedTaskRowVersion: lease.taskRowVersion,
    reason: "execution_failed",
  });
  assert.deepEqual(
    result.affected.map((row) => row.taskId),
    [id("tsk", "h"), task22, task23].sort(),
  );
  const oneHop = [id("tsk", "h"), task22].sort();
  assert.notDeepEqual(result.affected.map((row) => row.taskId), oneHop);
});

test("publication propagates scenario 8 into read-only review authority and BLOCK closes the whole image", async (t) => {
  const registered = await registerRoot("j", 90);
  const lease = await claimRoot("j", "7");
  const artifacts = new ArtifactV3Repository(databaseUrl, schema);
  const material = {
    stagedObjectId: id("aob", "j") as never,
    role: "artifact" as const,
    kind: "prerequisite_bound_git_bundle" as const,
    mediaType: "application/x-git-bundle",
    byteLength: 128,
    sha256: sha("4"),
    prerequisiteCommit: registered.workspace.baseCommit,
    expiresAt: "2099-01-01T00:00:00.000Z" as never,
  };
  const scopeManifest = {
    stagedObjectId: id("aob", "k") as never,
    role: "scope_manifest" as const,
    kind: "canonical_json" as const,
    mediaType: "application/json",
    byteLength: 64,
    sha256: sha("5"),
    prerequisiteCommit: null,
    expiresAt: "2099-01-01T00:00:00.000Z" as never,
  };
  const acceptanceReceipt = {
    stagedObjectId: id("aob", "m") as never,
    role: "acceptance_receipt" as const,
    kind: "canonical_json" as const,
    mediaType: "application/json",
    byteLength: 64,
    sha256: sha("6"),
    prerequisiteCommit: null,
    expiresAt: "2099-01-01T00:00:00.000Z" as never,
  };
  await artifacts.sealStagedMaterial(material);
  await artifacts.sealStagedMaterial(scopeManifest);
  await artifacts.sealStagedMaterial(acceptanceReceipt);
  const identity = {
    kind: "git_commit" as const,
    commitSha: "6".repeat(40) as never,
    treeSha: "7".repeat(40) as never,
    orderedParents: [registered.workspace.baseCommit],
  };
  const descriptor = await artifacts.publish({
    commandId: variant("cmd", "j", "p"),
    expectedLease: lease,
    expectedTaskStatus: "in_progress",
    expectedTaskRowVersion: lease.taskRowVersion,
    artifact: {
      protocolVersion: lease.protocolVersion,
      workspaceContractDigest: canonicalDigest(registered.workspace),
      artifactContractDigest: canonicalDigest(registered.contract),
      gate3PlanDigest: registered.contract.gate3PlanDigest,
      identity,
      artifactDigest: canonicalDigest(identity),
      scopeManifestDigest: scopeManifest.sha256,
      acceptanceReceiptDigest: acceptanceReceipt.sha256,
      stagedMaterial: material,
      stagedScopeManifest: scopeManifest,
      stagedAcceptanceReceipt: acceptanceReceipt,
    },
  });
  const review = await rawJson<{
    taskId: string;
    taskRowVersion: number;
    scenarioVersion: number;
    workspace: {
      executionMode: string;
      pathClaims: unknown[];
      readArtifactDigest: string;
    };
  }>(
    `SELECT json_build_object('taskId',r.review_task_id,'taskRowVersion',s.row_version,'scenarioVersion',r.scenario_version,'workspace',w.contract_json) FROM review_requirements_v3 r JOIN task_v3_state s ON s.task_id=r.review_task_id JOIN workspace_contracts_v3 w ON w.task_id=r.review_task_id WHERE r.artifact_id='${descriptor.artifactId}' AND r.seat_key='scope';`,
  );
  assert.equal(review.scenarioVersion, 8);
  assert.equal(review.workspace.executionMode, "read_only_artifact");
  assert.deepEqual(review.workspace.pathClaims, []);
  assert.equal(review.workspace.readArtifactDigest, descriptor.artifactDigest);

  const unassigned = await new TaskLeaseV3Repository(databaseUrl, schema).claim({
    commandId: variant("cmd", "j", "n") as never,
    taskId: review.taskId as never,
    agentId: id("agt", "b") as never,
    expectedTaskRowVersion: review.taskRowVersion,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(unassigned.kind, "conflict");
  if (unassigned.kind === "conflict")
    assert.equal(unassigned.code, "TASK_NOT_READY");

  const reviewer = new ReviewV3Repository(databaseUrl, schema);
  const requirement = {
    artifactId: descriptor.artifactId,
    artifactDigest: descriptor.artifactDigest,
    artifactContractDigest: descriptor.artifactContractDigest,
    gate3PlanDigest: descriptor.gate3PlanDigest,
    scenarioVersion: 8,
    seatKey: "scope" as never,
  };
  const assigned = await reviewer.assign({
    commandId: variant("cmd", "j", "a") as never,
    requirement,
    reviewTaskId: review.taskId as never,
    expectedBarrierRevision: 1,
    reviewerAgentId: id("agt", "b") as never,
    expectedRegistryRevision: 1,
    expectedAssignmentRevision: null,
    expectedBarrierState: "open",
    expectedReviewTaskStatus: "todo",
    expectedTerminalVerdictId: null,
  });
  const wrongReviewer = await new TaskLeaseV3Repository(databaseUrl, schema).claim({
    commandId: variant("cmd", "j", "w") as never,
    taskId: review.taskId as never,
    agentId: id("agt", "a") as never,
    expectedTaskRowVersion: review.taskRowVersion,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(wrongReviewer.kind, "conflict");
  if (wrongReviewer.kind === "conflict")
    assert.equal(wrongReviewer.code, "TASK_NOT_READY");
  const rightReviewer = await new TaskLeaseV3Repository(databaseUrl, schema).claim({
    commandId: variant("cmd", "j", "r") as never,
    taskId: review.taskId as never,
    agentId: id("agt", "b") as never,
    expectedTaskRowVersion: review.taskRowVersion,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(rightReviewer.kind, "claimed");
  if (rightReviewer.kind !== "claimed") return;

  const displacedReviewTaskId = variant("tsk", "j", "d");
  const displacedWorkspace = parseWorkspaceContractV3({
    protocolVersion: 1,
    workspaceContractId: variant("wsc", "j", "d"),
    taskId: displacedReviewTaskId,
    rootTaskId: id("tsk", "j"),
    repositoryId: id("rpo", "a"),
    baseCommit: registered.workspace.baseCommit,
    baseTree: registered.workspace.baseTree,
    workspaceGeneration: 1,
    executionMode: "read_only_artifact",
    pathClaims: [],
    readArtifactDigest: descriptor.artifactDigest,
    integrationOwnerTaskId: id("tsk", "j"),
    artifactContractTemplateDigest: templateDigest,
    policyDigest: registered.workspace.policyDigest,
  });
  const displacedWorkspaceDigest = canonicalDigest(displacedWorkspace);
  await raw(`
    INSERT INTO tasks(task_id,server_id,task_number,status,row_version)
      VALUES('${displacedReviewTaskId}','${id("srv", "a")}',92,'todo',1);
    INSERT INTO workspace_contracts_v3(workspace_contract_id,task_id,root_task_id,repository_id,contract_digest,contract_json,workspace_generation,execution_mode)
      VALUES('${displacedWorkspace.workspaceContractId}','${displacedReviewTaskId}','${id("tsk", "j")}','${id("rpo", "a")}','${displacedWorkspaceDigest}','${Buffer.from(canonicalProtocolJson(displacedWorkspace)).toString("utf8")}'::jsonb,1,'read_only_artifact');
    INSERT INTO task_v3_state(task_id,server_id,root_task_id,parent_task_id,repository_id,lane_role,status,row_version,required_capabilities,workspace_contract_digest,artifact_contract_digest,title_digest,title_classifier_policy_digest)
      VALUES('${displacedReviewTaskId}','${id("srv", "a")}','${id("tsk", "j")}','${id("tsk", "j")}','${id("rpo", "a")}','review','todo',1,'[]'::jsonb,'${displacedWorkspaceDigest}','${descriptor.artifactContractDigest}','${sha("8")}','${sha("9")}');
    INSERT INTO review_requirements_v3(artifact_id,seat_key,artifact_digest,artifact_contract_digest,gate3_plan_digest,scenario_version,review_task_id)
      VALUES('${descriptor.artifactId}','safety','${descriptor.artifactDigest}','${descriptor.artifactContractDigest}','${descriptor.gate3PlanDigest}',8,'${displacedReviewTaskId}');
  `);
  const displacedRequirement = {
    ...requirement,
    seatKey: "safety" as never,
  };
  const displacedAssignment = await reviewer.assign({
    commandId: variant("cmd", "j", "s") as never,
    requirement: displacedRequirement,
    reviewTaskId: displacedReviewTaskId as never,
    expectedBarrierRevision: assigned.currentBarrierRevision,
    reviewerAgentId: id("agt", "c") as never,
    expectedRegistryRevision: 1,
    expectedAssignmentRevision: null,
    expectedBarrierState: "open",
    expectedReviewTaskStatus: "todo",
    expectedTerminalVerdictId: null,
  });
  const displacedReviewer = await new TaskLeaseV3Repository(
    databaseUrl,
    schema,
  ).claim({
    commandId: variant("cmd", "j", "c") as never,
    taskId: displacedReviewTaskId as never,
    agentId: id("agt", "c") as never,
    expectedTaskRowVersion: 1,
    expectedGraphRevision: 0,
    expectedWorkspaceGeneration: 1,
  });
  assert.equal(displacedReviewer.kind, "claimed");
  if (displacedReviewer.kind !== "claimed") return;
  await raw(`
    INSERT INTO workspace_reservations_v3(reservation_id,repository_id,task_id,claim_id,lease_id,attempt,lease_epoch,path_kind,path)
      VALUES('${variant("rsv", "j", "b")}','${id("rpo", "a")}','${displacedReviewTaskId}','${displacedReviewer.lease.claimId}','${displacedReviewer.lease.leaseId}',${displacedReviewer.lease.attempt},${displacedReviewer.lease.leaseEpoch},'file','review-transient/evidence.json');
  `);

  const wholeImage = async () =>
    rawJson<{
      barrier: string;
      artifactStatus: string;
      submittingReviewStatus: string;
      displacedReviewStatus: string;
      currentAssignments: number;
      openClaims: number;
      openReservations: number;
    }>(
      `SELECT json_build_object('barrier',(SELECT state FROM review_barriers_v3 WHERE artifact_id='${descriptor.artifactId}'),'artifactStatus',(SELECT status FROM task_v3_state WHERE task_id='${id("tsk", "j")}'),'submittingReviewStatus',(SELECT status FROM task_v3_state WHERE task_id='${review.taskId}'),'displacedReviewStatus',(SELECT status FROM task_v3_state WHERE task_id='${displacedReviewTaskId}'),'currentAssignments',(SELECT count(*) FROM review_assignments_v3 WHERE artifact_id='${descriptor.artifactId}' AND assignment_status='current'),'openClaims',(SELECT count(*) FROM task_claims_v3 WHERE task_id IN('${review.taskId}','${displacedReviewTaskId}') AND closed_at IS NULL),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE task_id IN('${review.taskId}','${displacedReviewTaskId}') AND closed_at IS NULL));`,
    );
  const beforeBlock = await wholeImage();
  assert.deepEqual(beforeBlock, {
    barrier: "open",
    artifactStatus: "in_review",
    submittingReviewStatus: "in_progress",
    displacedReviewStatus: "in_progress",
    currentAssignments: 2,
    openClaims: 2,
    openReservations: 1,
  });

  const reservationCloseStatement = `UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='review_barrier_blocked' WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id='${descriptor.artifactId}') AND closed_at IS NULL;`;
  const conformingBlockStatements = `UPDATE review_barriers_v3 SET state='blocked',barrier_revision=barrier_revision+1 WHERE artifact_id='${descriptor.artifactId}' AND state='open';UPDATE review_assignments_v3 SET assignment_status='closed',closed_reason='barrier_blocked' WHERE artifact_id='${descriptor.artifactId}' AND assignment_status='current';UPDATE task_v3_state SET status='blocked',row_version=row_version+1 WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id='${descriptor.artifactId}') AND status IN('todo','in_progress');UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='review_barrier_blocked' WHERE task_id IN(SELECT review_task_id FROM review_requirements_v3 WHERE artifact_id='${descriptor.artifactId}') AND closed_at IS NULL;${reservationCloseStatement}UPDATE task_v3_state SET status='todo',row_version=row_version+1 WHERE task_id='${id("tsk", "j")}' AND status='in_review';`;
  const reservationCloseDeletionMutant = conformingBlockStatements.replace(
    reservationCloseStatement,
    "",
  );
  assert.equal(
    conformingBlockStatements.length - reservationCloseDeletionMutant.length,
    reservationCloseStatement.length,
  );
  let reservationCloseDeletionMutantImage: Awaited<
    ReturnType<typeof wholeImage>
  > | null = null;
  const mutant = await PsqlSession.open(databaseUrl);
  try {
    await mutant.execute(
      `BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog; SELECT 1 FROM workspace_repositories WHERE repository_id='${id("rpo", "a")}' FOR UPDATE; SELECT 1 FROM task_v3_graphs WHERE root_task_id='${id("tsk", "j")}' FOR UPDATE; SELECT 1 FROM task_v3_state WHERE root_task_id='${id("tsk", "j")}' ORDER BY task_id FOR UPDATE;
       INSERT INTO review_verdicts_v3(assignment_id,artifact_id,seat_key,review_attempt,verdict,findings_digest,evidence_receipt_digest) VALUES('${assigned.assignment.assignmentId}','${descriptor.artifactId}','scope',${rightReviewer.lease.attempt},'BLOCK','${sha("7")}','${sha("8")}');
       UPDATE review_assignments_v3 SET assignment_status='closed',closed_reason='terminal_verdict' WHERE assignment_id='${assigned.assignment.assignmentId}';
       UPDATE task_v3_state SET status='done',row_version=row_version+1 WHERE task_id='${review.taskId}';
       UPDATE task_claims_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE claim_id='${rightReviewer.lease.claimId}';
       UPDATE workspace_reservations_v3 SET closed_at=clock_timestamp(),close_reason='work_yielded' WHERE claim_id='${rightReviewer.lease.claimId}' AND closed_at IS NULL;
       ${reservationCloseDeletionMutant}`,
    );
    const mutantImage = await mutant.queryJson<{
      barrier: string;
      artifactStatus: string;
      submittingReviewStatus: string;
      displacedReviewStatus: string;
      currentAssignments: number;
      openClaims: number;
      openReservations: number;
    }>(
      `SELECT json_build_object('barrier',(SELECT state FROM review_barriers_v3 WHERE artifact_id='${descriptor.artifactId}'),'artifactStatus',(SELECT status FROM task_v3_state WHERE task_id='${id("tsk", "j")}'),'submittingReviewStatus',(SELECT status FROM task_v3_state WHERE task_id='${review.taskId}'),'displacedReviewStatus',(SELECT status FROM task_v3_state WHERE task_id='${displacedReviewTaskId}'),'currentAssignments',(SELECT count(*) FROM review_assignments_v3 WHERE artifact_id='${descriptor.artifactId}' AND assignment_status='current'),'openClaims',(SELECT count(*) FROM task_claims_v3 WHERE task_id IN('${review.taskId}','${displacedReviewTaskId}') AND closed_at IS NULL),'openReservations',(SELECT count(*) FROM workspace_reservations_v3 WHERE task_id IN('${review.taskId}','${displacedReviewTaskId}') AND closed_at IS NULL));`,
    );
    reservationCloseDeletionMutantImage = mutantImage;
    assert.deepEqual(mutantImage, {
      barrier: "blocked",
      artifactStatus: "todo",
      submittingReviewStatus: "done",
      displacedReviewStatus: "blocked",
      currentAssignments: 0,
      openClaims: 0,
      openReservations: 1,
    });
    assert.equal(mutantImage.openReservations === 0, false);
  } finally {
    await mutant.rollbackAndClose();
  }
  assert.deepEqual(await wholeImage(), beforeBlock);
  const submittingAssignment = await rawJson<{
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
  }>(
    `SELECT json_build_object('status',a.assignment_status,'revision',a.assignment_revision,'reviewer',a.reviewer_agent_id,'registryRevision',a.reviewer_registry_revision,'artifactId',r.artifact_id,'artifactDigest',r.artifact_digest,'artifactContractDigest',r.artifact_contract_digest,'gate3PlanDigest',r.gate3_plan_digest,'scenarioVersion',r.scenario_version,'seatKey',r.seat_key,'reviewTaskId',r.review_task_id,'barrierState',b.state) FROM review_assignments_v3 a JOIN review_requirements_v3 r USING(artifact_id,seat_key) JOIN review_barriers_v3 b USING(artifact_id) WHERE a.assignment_id='${assigned.assignment.assignmentId}';`,
  );
  assert.deepEqual(submittingAssignment, {
    status: "current",
    revision: assigned.assignment.assignmentRevision,
    reviewer: rightReviewer.lease.ownerAgentId,
    registryRevision: assigned.assignment.reviewerRegistryRevision,
    artifactId: assigned.assignment.artifactId,
    artifactDigest: assigned.assignment.artifactDigest,
    artifactContractDigest: assigned.assignment.artifactContractDigest,
    gate3PlanDigest: assigned.assignment.gate3PlanDigest,
    scenarioVersion: assigned.assignment.scenarioVersion,
    seatKey: assigned.assignment.seatKey,
    reviewTaskId: assigned.assignment.reviewTaskId,
    barrierState: "open",
  });

  const reviewLatches = new ControlledLatches();
  const blockHold = reviewLatches.hold("review_block_after_repository_lock");
  const blockRepository = new ReviewV3Repository(
    databaseUrl,
    schema,
    reviewLatches,
  );
  const block = blockRepository.submit({
    commandId: variant("cmd", "j", "v") as never,
    expectedLease: rightReviewer.lease,
    expectedTaskStatus: "in_progress",
    expectedTaskRowVersion: rightReviewer.lease.taskRowVersion,
    verdict: {
      ...assigned.assignment,
      reviewAttempt: rightReviewer.lease.attempt,
      verdict: "BLOCK",
      findingsDigest: sha("7"),
      evidenceReceiptDigest: sha("8"),
    },
  });
  await blockHold.reached;
  const siblingRenewal = new TaskLeaseV3Repository(
    databaseUrl,
    schema,
  ).renew({
    commandId: variant("cmd", "j", "z") as never,
    expectedLease: rightReviewer.lease,
  });
  blockHold.release();
  const siblingSettled = await settleWithin(
    Promise.allSettled([block, siblingRenewal]),
    1_500,
  );
  assert.equal(siblingSettled[0].status, "fulfilled");
  if (siblingSettled[0].status !== "fulfilled") return;
  const result = siblingSettled[0].value;
  assert.equal(result.barrierState, "blocked");
  assert.equal(siblingSettled[1].status, "fulfilled");
  if (siblingSettled[1].status !== "fulfilled") return;
  assert.equal(siblingSettled[1].value.kind, "rejected");
  if (siblingSettled[1].value.kind === "rejected")
    assert.equal(
      siblingSettled[1].value.code,
      "TASK_LEASE_RENEWAL_CONFLICT",
    );
  assert.equal(reviewLatches.counts.get("review_block_after_repository_lock"), 1);
  const image = await wholeImage();
  assert.deepEqual(image, {
    barrier: "blocked",
    artifactStatus: "todo",
    submittingReviewStatus: "done",
    displacedReviewStatus: "blocked",
    currentAssignments: 0,
    openClaims: 0,
    openReservations: 0,
  });
  const displacedClosure = await rawJson<{
    claimReason: string;
    reservationReason: string;
  }>(
    `SELECT json_build_object('claimReason',(SELECT close_reason FROM task_claims_v3 WHERE claim_id='${displacedReviewer.lease.claimId}'),'reservationReason',(SELECT close_reason FROM workspace_reservations_v3 WHERE claim_id='${displacedReviewer.lease.claimId}'));`,
  );
  assert.deepEqual(displacedClosure, {
    claimReason: "review_barrier_blocked",
    reservationReason: "review_barrier_blocked",
  });
  assert.equal(displacedAssignment.assignment.assignmentStatus, "current");
  t.diagnostic(
    JSON.stringify({
      proof: "block_closes_nonzero_review_reservation",
      before: beforeBlock,
      reservationCloseDeletionMutant: {
        observed: reservationCloseDeletionMutantImage,
        zeroReservationInvariant: false,
        killed: true,
      },
      after: image,
      displacedClosure,
      siblingRenewalRace: {
        semanticWinner: "review_barrier_blocked",
        typedLoser: siblingSettled[1].value.code,
        databaseUnavailable: 0,
      },
    }),
  );
});
