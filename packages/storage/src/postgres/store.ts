import { createHash } from "node:crypto";
import {
  canonicalProtocolJson,
  type ArtifactDigest,
  type DeliveryEnvelope,
  type ReceiptActor,
  type Target,
  type TaskLease,
  type TransitionReceipt,
} from "@swarm/protocol";
import { storageFail } from "../errors.js";
import type { MigrationReceipt } from "../migrations.js";
import {
  assertArtifactDigest,
  assertFenceToken,
  assertProtocolId,
  parseFrozenDelivery,
  parseFrozenTaskLease,
  parseFrozenTransitionReceipt,
  targetColumns,
} from "../protocol.js";
import { PostgresMigrator } from "./migrate.js";
import { PsqlSession, sqlLiteral } from "./session.js";

const SCHEMA = /^[a-z][a-z0-9_]{0,62}$/u;

export const IDEMPOTENCY_SCOPES = [
  "message.append.v1",
  "claim.mutate.v1",
  "task_graph.mutate.v1",
  "reminder.mutate.v1",
  "launch.mutate.v1",
  "delivery.mutate.v1",
  "receipt.record.v1",
  "artifact.mutate.v1",
  "outbox.mutate.v1",
] as const;

export type IdempotencyScope = (typeof IDEMPOTENCY_SCOPES)[number];
const IDEMPOTENCY_SCOPE_SET: ReadonlySet<string> = new Set(IDEMPOTENCY_SCOPES);

export type IdempotentRequest = {
  actor: ReceiptActor;
  scope: IdempotencyScope;
  requestKind: "command" | "producer_fact";
  requestId: string;
  requestDigest: ArtifactDigest;
};

export type VersionedResult = { protocolVersion: number } & Record<string, unknown>;

export type IdempotentResult<T extends VersionedResult> = {
  result: T;
  resultDigest: ArtifactDigest;
  replayed: boolean;
};

type MessageAppendInput = {
  messageId: string;
  target: Target;
  author: ReceiptActor;
  body: string;
  parentMessageId?: string;
  producerFactId: string;
  payloadDigest: ArtifactDigest;
};

type OutboxEvent = {
  kind: "message_delivery" | "receipt_delivery" | "reminder_fire" | "artifact_publication";
  producerFactId: string;
  version: number;
  payload: VersionedResult;
};

type OutboxJob = {
  jobId: number;
  namespace: string;
  idempotencyKey: string;
  producerFactId: string;
  eventKind: string;
  eventVersion: number;
  attempt: number;
};

type ActorColumns = { kind: "server" | "agent"; id: string };

function actorColumns(actor: ReceiptActor): ActorColumns {
  if ("agentId" in actor) {
    return { kind: "agent", id: assertProtocolId(actor.agentId, "agt") };
  }
  if ("serverId" in actor) {
    return { kind: "server", id: assertProtocolId(actor.serverId, "srv") };
  }
  return storageFail("INVALID_IDENTIFIER", actor);
}

function digestCanonical(value: unknown): ArtifactDigest {
  const bytes = canonicalProtocolJson(value);
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}` as ArtifactDigest;
}

function digestReceiptPayload(receipt: TransitionReceipt): ArtifactDigest {
  const { receiptId: _wireIdentity, ...payload } = receipt;
  return digestCanonical(payload);
}

function canonicalJson(value: unknown): string {
  return new TextDecoder().decode(canonicalProtocolJson(value));
}

function validateVersionedResult<T extends VersionedResult>(value: T): T {
  if (
    !Number.isSafeInteger(value.protocolVersion) ||
    value.protocolVersion < 1 ||
    value.protocolVersion > 999_999
  ) {
    storageFail("IDEMPOTENCY_CONFLICT", "invalid result protocolVersion");
  }
  canonicalProtocolJson(value);
  return value;
}

function validateRequest(request: IdempotentRequest): ActorColumns {
  const actor = actorColumns(request.actor);
  if (!IDEMPOTENCY_SCOPE_SET.has(request.scope)) {
    storageFail("INVALID_IDENTIFIER", request.scope);
  }
  assertProtocolId(request.requestId, request.requestKind === "command" ? "cmd" : "fac");
  assertArtifactDigest(request.requestDigest);
  return actor;
}

function asTaskLease(value: TaskLease): TaskLease {
  return parseFrozenTaskLease(canonicalProtocolJson(value));
}

export class MessageRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async append(input: MessageAppendInput): Promise<{ messageId: string; targetSeq: number; replayed: boolean }> {
    assertProtocolId(input.messageId, "msg");
    assertProtocolId(input.producerFactId, "fac");
    if (input.parentMessageId !== undefined) assertProtocolId(input.parentMessageId, "msg");
    assertArtifactDigest(input.payloadDigest);
    if (input.body.trim().length === 0) storageFail("INVALID_IDENTIFIER", "empty message body");
    const target = targetColumns(input.target);
    const author = actorColumns(input.author);
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'producer_fact:' || ${sqlLiteral(input.producerFactId)}, 0
      ));`,
    );
    const existing = await this.#session.queryJson<{
      found: boolean;
      messageId?: string;
      targetSeq?: number;
      payloadDigest?: string;
      targetKind?: string;
      targetId?: string;
      threadRootMessageId?: string | null;
      authorKind?: string;
      authorId?: string;
      body?: string;
      parentMessageId?: string | null;
    }>(
      `SELECT coalesce(
        (SELECT json_build_object(
          'found', true, 'messageId', message_id, 'targetSeq', target_seq,
          'payloadDigest', payload_digest, 'targetKind', target_kind,
          'targetId', target_id, 'threadRootMessageId', thread_root_message_id,
          'authorKind', author_kind, 'authorId', author_id, 'body', body,
          'parentMessageId', parent_message_id
        ) FROM messages WHERE producer_fact_id = ${sqlLiteral(input.producerFactId)}),
        json_build_object('found', false)
      );`,
    );
    if (existing.found) {
      if (
        existing.payloadDigest !== input.payloadDigest ||
        existing.targetKind !== target.kind ||
        existing.targetId !== target.ownerId ||
        (existing.threadRootMessageId ?? null) !== target.threadRootMessageId ||
        existing.authorKind !== author.kind ||
        existing.authorId !== author.id ||
        existing.body !== input.body ||
        (existing.parentMessageId ?? null) !== (input.parentMessageId ?? null)
      ) {
        storageFail("IDEMPOTENCY_CONFLICT", input.producerFactId);
      }
      return {
        messageId: existing.messageId ?? storageFail("IDEMPOTENCY_CONFLICT"),
        targetSeq: existing.targetSeq ?? storageFail("IDEMPOTENCY_CONFLICT"),
        replayed: true,
      };
    }
    const allocated = await this.#session.queryJson<{ targetSeq: number }>(
      `WITH allocated AS (
        INSERT INTO target_sequences (
          target_kind, target_id, thread_root_message_id, next_seq
        ) VALUES (
          ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)},
          ${sqlLiteral(target.threadRootMessageId)}, 2
        )
        ON CONFLICT (target_kind, target_id, thread_root_message_id)
        DO UPDATE SET next_seq = target_sequences.next_seq + 1
        RETURNING next_seq - 1 AS target_seq
      )
      SELECT json_build_object('targetSeq', target_seq) FROM allocated;`,
    );
    await this.#session.execute(
      `INSERT INTO messages (
        message_id, target_kind, target_id, thread_root_message_id,
        author_kind, author_id, target_seq, body, parent_message_id,
        producer_fact_id, payload_digest
      ) VALUES (
        ${sqlLiteral(input.messageId)}, ${sqlLiteral(target.kind)},
        ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
        ${sqlLiteral(author.kind)}, ${sqlLiteral(author.id)},
        ${sqlLiteral(allocated.targetSeq)}, ${sqlLiteral(input.body)},
        ${sqlLiteral(input.parentMessageId ?? null)},
        ${sqlLiteral(input.producerFactId)}, ${sqlLiteral(input.payloadDigest)}
      );`,
    );
    return { messageId: input.messageId, targetSeq: allocated.targetSeq, replayed: false };
  }
}

export class ClaimFenceRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async acquire(input: {
    taskId: string;
    claimId: string;
    leaseId: string;
    fenceToken: string;
    attempt: number;
    ownerAgentId: string;
    expiresAt: string;
  }): Promise<TaskLease> {
    assertProtocolId(input.taskId, "tsk");
    assertProtocolId(input.claimId, "clm");
    assertProtocolId(input.leaseId, "lse");
    assertFenceToken(input.fenceToken);
    assertProtocolId(input.ownerAgentId, "agt");
    if (!Number.isSafeInteger(input.attempt) || input.attempt < 1) {
      storageFail("INVALID_IDENTIFIER", input.attempt);
    }
    const task = await this.#session.queryJson<{ found: boolean; ownerCurrent?: boolean }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true,
        'ownerCurrent', EXISTS (
          SELECT 1 FROM agents
          WHERE agent_id = ${sqlLiteral(input.ownerAgentId)}
            AND server_id = tasks.server_id
        )
      )
        FROM tasks WHERE task_id = ${sqlLiteral(input.taskId)} FOR UPDATE),
        json_build_object('found', false));`,
    );
    if (!task.found || !task.ownerCurrent) storageFail("INVALID_IDENTIFIER", input.taskId);
    await this.#session.execute(
      `UPDATE task_claims
       SET released_at = clock_timestamp(), terminal_reason = 'expired'
       WHERE task_id = ${sqlLiteral(input.taskId)} AND released_at IS NULL
         AND expires_at <= clock_timestamp();`,
    );
    const next = await this.#session.queryJson<{ leaseEpoch: number; open: boolean }>(
      `SELECT json_build_object(
        'leaseEpoch', coalesce(max(lease_epoch), 0) + 1,
        'open', count(*) FILTER (WHERE released_at IS NULL) > 0
      )
       FROM task_claims WHERE task_id = ${sqlLiteral(input.taskId)};`,
    );
    if (next.open) storageFail("STALE_FENCE", input.taskId);
    const result = await this.#session.queryJson<{
      taskId: string;
      claimId: string;
      leaseId: string;
      leaseEpoch: number;
      fenceToken: string;
      attempt: number;
      acquiredAt: string;
      expiresAt: string;
    }>(
      `WITH inserted AS (
        INSERT INTO task_claims (
          claim_id, task_id, lease_id, lease_epoch, fence_token, attempt,
          owner_agent_id, expires_at
        ) VALUES (
          ${sqlLiteral(input.claimId)}, ${sqlLiteral(input.taskId)},
          ${sqlLiteral(input.leaseId)}, ${sqlLiteral(next.leaseEpoch)},
          ${sqlLiteral(input.fenceToken)}, ${sqlLiteral(input.attempt)},
          ${sqlLiteral(input.ownerAgentId)}, ${sqlLiteral(input.expiresAt)}::timestamptz
        )
        RETURNING *
      ) SELECT json_build_object(
        'taskId', task_id, 'claimId', claim_id, 'leaseId', lease_id,
        'leaseEpoch', lease_epoch, 'fenceToken', fence_token, 'attempt', attempt,
        'acquiredAt', to_char(acquired_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'expiresAt', to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
      ) FROM inserted;`,
    );
    return asTaskLease(result as TaskLease);
  }

  async assertCurrent(fenceInput: TaskLease): Promise<TaskLease> {
    const fence = asTaskLease(fenceInput);
    const result = await this.#session.queryJson<{ current: boolean }>(
      `SELECT json_build_object('current', EXISTS (
        SELECT 1 FROM task_claims
        WHERE task_id = ${sqlLiteral(fence.taskId)}
          AND claim_id = ${sqlLiteral(fence.claimId)}
          AND lease_id = ${sqlLiteral(fence.leaseId)}
          AND lease_epoch = ${sqlLiteral(fence.leaseEpoch)}
          AND fence_token = ${sqlLiteral(fence.fenceToken)}
          AND attempt = ${sqlLiteral(fence.attempt)}
          AND released_at IS NULL AND expires_at > clock_timestamp()
      ));`,
    );
    if (!result.current) storageFail("STALE_FENCE", fence.taskId);
    return fence;
  }

  async guardedTaskStatus(fenceInput: TaskLease, from: string, to: string): Promise<void> {
    const fence = asTaskLease(fenceInput);
    const allowed = new Set(["todo", "in_progress", "in_review", "done"]);
    if (!allowed.has(from) || !allowed.has(to)) storageFail("INVALID_STATE_TRANSITION");
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE tasks SET status = ${sqlLiteral(to)}, row_version = row_version + 1,
                         updated_at = clock_timestamp()
        WHERE task_id = ${sqlLiteral(fence.taskId)}
          AND status = ${sqlLiteral(from)}
          AND EXISTS (
            SELECT 1 FROM task_claims
            WHERE task_id = ${sqlLiteral(fence.taskId)}
              AND claim_id = ${sqlLiteral(fence.claimId)}
              AND lease_id = ${sqlLiteral(fence.leaseId)}
              AND lease_epoch = ${sqlLiteral(fence.leaseEpoch)}
              AND fence_token = ${sqlLiteral(fence.fenceToken)}
              AND attempt = ${sqlLiteral(fence.attempt)}
              AND released_at IS NULL AND expires_at > clock_timestamp()
          ) RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("STALE_FENCE", fence.taskId);
  }

  async release(fenceInput: TaskLease, reason: string): Promise<void> {
    const fence = asTaskLease(fenceInput);
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE task_claims SET released_at = clock_timestamp(), terminal_reason = ${sqlLiteral(reason)}
        WHERE task_id = ${sqlLiteral(fence.taskId)}
          AND claim_id = ${sqlLiteral(fence.claimId)}
          AND lease_id = ${sqlLiteral(fence.leaseId)}
          AND lease_epoch = ${sqlLiteral(fence.leaseEpoch)}
          AND fence_token = ${sqlLiteral(fence.fenceToken)}
          AND attempt = ${sqlLiteral(fence.attempt)} AND released_at IS NULL
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("STALE_FENCE", fence.taskId);
  }
}

export class TargetSequenceRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async advanceAgentState(input: {
    agentId: string;
    target: Target;
    expectedVersion: number;
    pendingServerSeq?: number;
    consumedServerSeq?: number;
  }): Promise<number> {
    assertProtocolId(input.agentId, "agt");
    if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0) {
      storageFail("INVALID_IDENTIFIER", input.expectedVersion);
    }
    const target = targetColumns(input.target);
    const result = await this.#session.queryJson<{ version: number } | null>(
      `WITH changed AS (
        INSERT INTO agent_message_state (
          agent_id, target_kind, target_id, thread_root_message_id,
          pending_server_seq, consumed_server_seq, row_version
        ) SELECT
          ${sqlLiteral(input.agentId)}, ${sqlLiteral(target.kind)},
          ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
          ${sqlLiteral(input.pendingServerSeq ?? null)},
          ${sqlLiteral(input.consumedServerSeq ?? null)}, 1
        WHERE ${sqlLiteral(input.expectedVersion)} = 0
        ON CONFLICT (agent_id, target_kind, target_id, thread_root_message_id)
        DO UPDATE SET
          pending_server_seq = excluded.pending_server_seq,
          consumed_server_seq = excluded.consumed_server_seq,
          row_version = agent_message_state.row_version + 1
        WHERE agent_message_state.row_version = ${sqlLiteral(input.expectedVersion)}
        RETURNING row_version
      ) SELECT coalesce((SELECT json_build_object('version', row_version) FROM changed), 'null'::json);`,
    );
    if (result === null) storageFail("STALE_FENCE", "agent message state version");
    return Number(result.version);
  }
}

export class TaskGraphRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async addEdge(input: {
    serverId: string;
    parentTaskId: string;
    childTaskId: string;
    expectedRevision?: number;
  }): Promise<number> {
    assertProtocolId(input.serverId, "srv");
    assertProtocolId(input.parentTaskId, "tsk");
    assertProtocolId(input.childTaskId, "tsk");
    if (input.parentTaskId === input.childTaskId) storageFail("TASK_GRAPH_CYCLE");
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(
        hashtextextended('task_graph:' || ${sqlLiteral(input.serverId)}, 0)
      );
      INSERT INTO task_graphs(server_id, graph_revision)
      VALUES (${sqlLiteral(input.serverId)}, 0)
      ON CONFLICT (server_id) DO NOTHING;`,
    );
    const state = await this.#session.queryJson<{
      endpoints: number;
      revision: number;
      cycle: boolean;
    }>(
      `SELECT json_build_object(
        'endpoints', (SELECT count(*) FROM tasks WHERE server_id = ${sqlLiteral(input.serverId)}
          AND task_id IN (${sqlLiteral(input.parentTaskId)}, ${sqlLiteral(input.childTaskId)})),
        'revision', (SELECT graph_revision FROM task_graphs WHERE server_id = ${sqlLiteral(input.serverId)}),
        'cycle', EXISTS (
          WITH RECURSIVE reachable(task_id) AS (
            VALUES (${sqlLiteral(input.childTaskId)}::task_id_text)
            UNION
            SELECT edge.child_task_id FROM task_edges AS edge
            JOIN reachable ON edge.parent_task_id = reachable.task_id
            WHERE edge.server_id = ${sqlLiteral(input.serverId)}
          ) SELECT 1 FROM reachable WHERE task_id = ${sqlLiteral(input.parentTaskId)}
        )
      );`,
    );
    if (Number(state.endpoints) !== 2) storageFail("INVALID_IDENTIFIER", "cross-server edge");
    if (input.expectedRevision !== undefined && Number(state.revision) !== input.expectedRevision) {
      storageFail("STALE_FENCE", "graph revision");
    }
    if (state.cycle) storageFail("TASK_GRAPH_CYCLE");
    const updated = await this.#session.queryJson<{ revision: number }>(
      `WITH inserted AS (
        INSERT INTO task_edges(server_id, parent_task_id, child_task_id)
        VALUES (${sqlLiteral(input.serverId)}, ${sqlLiteral(input.parentTaskId)}, ${sqlLiteral(input.childTaskId)})
        ON CONFLICT DO NOTHING RETURNING 1
      ), advanced AS (
        UPDATE task_graphs SET graph_revision = graph_revision + 1
        WHERE server_id = ${sqlLiteral(input.serverId)} AND EXISTS (SELECT 1 FROM inserted)
        RETURNING graph_revision
      )
      SELECT json_build_object(
        'revision', coalesce((SELECT graph_revision FROM advanced),
          (SELECT graph_revision FROM task_graphs WHERE server_id = ${sqlLiteral(input.serverId)}))
      );`,
    );
    return Number(updated.revision);
  }
}

export class DeliveryRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async create(envelopeBytes: Uint8Array): Promise<DeliveryEnvelope> {
    const envelope = parseFrozenDelivery(envelopeBytes);
    const target = targetColumns(envelope.target);
    const lineage = await this.#session.queryJson<{
      found: boolean;
      targetKind?: string;
      targetId?: string;
      threadRootMessageId?: string | null;
      targetSeq?: number;
      producerFactId?: string;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true,
        'targetKind', target_kind,
        'targetId', target_id,
        'threadRootMessageId', thread_root_message_id,
        'targetSeq', target_seq,
        'producerFactId', producer_fact_id
      ) FROM messages WHERE message_id = ${sqlLiteral(envelope.messageId)} FOR SHARE),
      json_build_object('found', false));`,
    );
    const binding = await this.#session.queryJson<{
      actorPairCurrent: boolean;
      launchCurrent: boolean;
    }>(
      `SELECT json_build_object(
        'actorPairCurrent', EXISTS (
          SELECT 1 FROM agents AS agent
          JOIN machines AS machine ON machine.server_id = agent.server_id
          WHERE agent.agent_id = ${sqlLiteral(envelope.agentId)}
            AND machine.machine_id = ${sqlLiteral(envelope.machineId)}
        ),
        'launchCurrent', CASE
          WHEN ${sqlLiteral(envelope.expectedLaunchId ?? null)} IS NULL THEN true
          ELSE EXISTS (
            SELECT 1 FROM agent_launches AS launch
            WHERE launch.launch_id = ${sqlLiteral(envelope.expectedLaunchId ?? null)}
              AND launch.agent_id = ${sqlLiteral(envelope.agentId)}
              AND launch.machine_id = ${sqlLiteral(envelope.machineId)}
              AND launch.state IN ('requested', 'ready', 'activated')
          )
        END
      );`,
    );
    if (
      !lineage.found ||
      lineage.targetKind !== target.kind ||
      lineage.targetId !== target.ownerId ||
      (lineage.threadRootMessageId ?? null) !== target.threadRootMessageId ||
      Number(lineage.targetSeq) !== envelope.serverSeq ||
      lineage.producerFactId !== envelope.producerFactId ||
      !binding.actorPairCurrent ||
      !binding.launchCurrent
    ) {
      storageFail("INVALID_STATE_TRANSITION", envelope.deliveryId);
    }
    if (envelope.replayOf !== undefined) {
      const prior = await this.#session.queryJson<{
        found: boolean;
        attempt?: number;
        messageId?: string;
        targetKind?: string;
        targetId?: string;
        threadRootMessageId?: string | null;
        targetSeq?: number;
        agentId?: string;
      }>(
        `SELECT coalesce((SELECT json_build_object(
          'found', true, 'attempt', attempt, 'messageId', message_id,
          'targetKind', target_kind, 'targetId', target_id,
          'threadRootMessageId', thread_root_message_id, 'targetSeq', target_seq,
          'agentId', agent_id
        ) FROM deliveries WHERE delivery_id = ${sqlLiteral(envelope.replayOf)} FOR UPDATE),
        json_build_object('found', false));`,
      );
      if (
        !prior.found ||
        prior.attempt !== envelope.attempt - 1 ||
        prior.messageId !== envelope.messageId ||
        prior.targetKind !== target.kind ||
        prior.targetId !== target.ownerId ||
        (prior.threadRootMessageId ?? null) !== target.threadRootMessageId ||
        prior.targetSeq !== envelope.serverSeq ||
        prior.agentId !== envelope.agentId
      ) {
        storageFail("INVALID_STATE_TRANSITION", envelope.deliveryId);
      }
    }
    await this.#session.execute(
      `INSERT INTO deliveries (
        delivery_id, attempt, message_id, target_kind, target_id,
        thread_root_message_id, target_seq, producer_fact_id, agent_id,
        machine_id, expected_launch_id, replay_of, status
      ) VALUES (
        ${sqlLiteral(envelope.deliveryId)}, ${sqlLiteral(envelope.attempt)},
        ${sqlLiteral(envelope.messageId)}, ${sqlLiteral(target.kind)},
        ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
        ${sqlLiteral(envelope.serverSeq)}, ${sqlLiteral(envelope.producerFactId)},
        ${sqlLiteral(envelope.agentId)}, ${sqlLiteral(envelope.machineId)},
        ${sqlLiteral(envelope.expectedLaunchId ?? null)},
        ${sqlLiteral(envelope.replayOf ?? null)}, 'pending'
      );`,
    );
    return envelope;
  }

  async advanceBoundary(input:
    | {
        deliveryId: string;
        to: "daemon_accepted" | "input_written" | "model_visible";
        receiptBytes: Uint8Array;
        receiptDigest: ArtifactDigest;
      }
    | {
        deliveryId: string;
        to: "acked";
        occurredAt: string;
      }
  ): Promise<void> {
    assertProtocolId(input.deliveryId, "dlv");
    const delivery = await this.#session.queryJson<{
      found: boolean;
      status?: string;
      producerFactId?: string;
      agentId?: string;
      machineId?: string;
      expectedLaunchId?: string | null;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'status', status, 'producerFactId', producer_fact_id,
        'agentId', agent_id, 'machineId', machine_id,
        'expectedLaunchId', expected_launch_id
      ) FROM deliveries WHERE delivery_id = ${sqlLiteral(input.deliveryId)} FOR UPDATE),
      json_build_object('found', false));`,
    );
    if (!delivery.found) storageFail("INVALID_STATE_TRANSITION", input.deliveryId);
    const expectedFrom: Record<typeof input.to, readonly string[]> = {
      daemon_accepted: ["pending", "leased"],
      input_written: ["daemon_accepted"],
      model_visible: ["input_written"],
      acked: ["model_visible"],
    };
    if (!expectedFrom[input.to].includes(delivery.status ?? "")) {
      storageFail("INVALID_STATE_TRANSITION", input.deliveryId);
    }
    const column: Record<typeof input.to, string> = {
      daemon_accepted: "daemon_accepted_at",
      input_written: "input_written_at",
      model_visible: "model_visible_at",
      acked: "acked_at",
    };
    let occurredAt: string;
    if (input.to === "acked") {
      occurredAt = input.occurredAt;
    } else {
      const receipt = parseFrozenTransitionReceipt(input.receiptBytes);
      if (
        receipt.kind !== input.to ||
        receipt.producerFactId !== delivery.producerFactId ||
        !("machineId" in receipt.actor) ||
        receipt.actor.machineId !== delivery.machineId ||
        receipt.actor.agentId !== delivery.agentId
      ) {
        storageFail("INVALID_STATE_TRANSITION", input.deliveryId);
      }
      if (input.to === "input_written" || input.to === "model_visible") {
        if (
          delivery.expectedLaunchId === null ||
          receipt.fence.launchId !== delivery.expectedLaunchId
        ) {
          storageFail("STALE_FENCE", input.deliveryId);
        }
      }
      await new ReceiptRepository(this.#session).record(
        input.receiptBytes,
        input.receiptDigest,
      );
      occurredAt = receipt.occurredAt;
    }
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE deliveries SET status = ${sqlLiteral(input.to)},
          ${column[input.to]} = ${sqlLiteral(occurredAt)}::timestamptz
        WHERE delivery_id = ${sqlLiteral(input.deliveryId)}
          AND status = ${sqlLiteral(delivery.status ?? "")} RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("INVALID_STATE_TRANSITION", input.deliveryId);
  }
}

type ReceiptColumns = {
  actorServerId: string | null;
  actorMachineId: string | null;
  actorAgentId: string | null;
  leaseEpoch: number | null;
  fenceToken: string | null;
  launchId: string | null;
  stateInstanceId: string | null;
  turnId: string | null;
  sessionId: string | null;
  artifactDigest: string | null;
};

function receiptColumns(receipt: TransitionReceipt): ReceiptColumns {
  const actorServerId = "serverId" in receipt.actor ? receipt.actor.serverId : null;
  const actorMachineId = "machineId" in receipt.actor ? receipt.actor.machineId : null;
  const actorAgentId = "agentId" in receipt.actor ? receipt.actor.agentId : null;
  const fence = receipt.fence as Record<string, unknown>;
  return {
    actorServerId,
    actorMachineId,
    actorAgentId,
    leaseEpoch: typeof fence.leaseEpoch === "number" ? fence.leaseEpoch : null,
    fenceToken: typeof fence.fenceToken === "string" ? fence.fenceToken : null,
    launchId: typeof fence.launchId === "string" ? fence.launchId : null,
    stateInstanceId: typeof fence.stateInstanceId === "string" ? fence.stateInstanceId : null,
    turnId: typeof fence.turnId === "string" ? fence.turnId : null,
    sessionId: typeof fence.sessionId === "string" ? fence.sessionId : null,
    artifactDigest: "artifactDigest" in receipt ? receipt.artifactDigest : null,
  };
}

export class ReceiptRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async record(bytes: Uint8Array, receiptDigest: ArtifactDigest): Promise<{ receiptId: string; replayed: boolean }> {
    const receipt = parseFrozenTransitionReceipt(bytes);
    assertArtifactDigest(receiptDigest);
    if (digestReceiptPayload(receipt) !== receiptDigest) {
      storageFail("IDEMPOTENCY_CONFLICT", receipt.receiptId);
    }
    const columns = receiptColumns(receipt);
    const identity = await this.#session.queryJson<{ actorCurrent: boolean; launchCurrent: boolean }>(
      `SELECT json_build_object(
        'actorCurrent', CASE
          WHEN ${sqlLiteral(columns.actorServerId)} IS NOT NULL
            AND ${sqlLiteral(columns.actorAgentId)} IS NOT NULL THEN EXISTS (
              SELECT 1 FROM agents WHERE agent_id = ${sqlLiteral(columns.actorAgentId)}
                AND server_id = ${sqlLiteral(columns.actorServerId)}
            )
          WHEN ${sqlLiteral(columns.actorMachineId)} IS NOT NULL
            AND ${sqlLiteral(columns.actorAgentId)} IS NOT NULL THEN EXISTS (
              SELECT 1 FROM machines AS machine
              JOIN agents AS agent ON agent.server_id = machine.server_id
              WHERE machine.machine_id = ${sqlLiteral(columns.actorMachineId)}
                AND agent.agent_id = ${sqlLiteral(columns.actorAgentId)}
            )
          ELSE EXISTS (
            SELECT 1 FROM servers WHERE server_id = ${sqlLiteral(columns.actorServerId)}
          )
        END,
        'launchCurrent', CASE
          WHEN ${sqlLiteral(columns.launchId)} IS NULL THEN true
          ELSE EXISTS (
            SELECT 1 FROM agent_launches
            WHERE launch_id = ${sqlLiteral(columns.launchId)}
              AND machine_id = ${sqlLiteral(columns.actorMachineId)}
              AND agent_id = ${sqlLiteral(columns.actorAgentId)}
          )
        END
      );`,
    );
    if (!identity.actorCurrent || !identity.launchCurrent) {
      storageFail("INVALID_STATE_TRANSITION", receipt.receiptId);
    }
    const logicalKey = digestCanonical({
      producerFactId: receipt.producerFactId,
      kind: receipt.kind,
      ...columns,
    });
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'receipt_transition:' || ${sqlLiteral(logicalKey)}, 0
      ));`,
    );
    const existing = await this.#session.queryJson<{ receiptId: string; receiptDigest: string } | null>(
      `SELECT coalesce((SELECT row_to_json(found) FROM (
        SELECT receipt_id AS "receiptId", receipt_digest AS "receiptDigest"
        FROM receipts WHERE
          producer_fact_id = ${sqlLiteral(receipt.producerFactId)} AND kind = ${sqlLiteral(receipt.kind)}
          AND actor_server_id IS NOT DISTINCT FROM ${sqlLiteral(columns.actorServerId)}
          AND actor_machine_id IS NOT DISTINCT FROM ${sqlLiteral(columns.actorMachineId)}
          AND actor_agent_id IS NOT DISTINCT FROM ${sqlLiteral(columns.actorAgentId)}
          AND lease_epoch IS NOT DISTINCT FROM ${sqlLiteral(columns.leaseEpoch)}
          AND fence_token IS NOT DISTINCT FROM ${sqlLiteral(columns.fenceToken)}
          AND launch_id IS NOT DISTINCT FROM ${sqlLiteral(columns.launchId)}
          AND state_instance_id IS NOT DISTINCT FROM ${sqlLiteral(columns.stateInstanceId)}
          AND turn_id IS NOT DISTINCT FROM ${sqlLiteral(columns.turnId)}
          AND session_id IS NOT DISTINCT FROM ${sqlLiteral(columns.sessionId)}
          AND artifact_digest IS NOT DISTINCT FROM ${sqlLiteral(columns.artifactDigest)}
      ) AS found), 'null'::json);`,
    );
    if (existing !== null) {
      if (existing.receiptDigest !== receiptDigest) storageFail("IDEMPOTENCY_CONFLICT", receipt.receiptId);
      return { receiptId: existing.receiptId, replayed: true };
    }
    await this.#session.execute(
      `INSERT INTO receipts (
        receipt_id, producer_fact_id, kind, actor_server_id, actor_machine_id,
        actor_agent_id, lease_epoch, fence_token, launch_id, state_instance_id,
        turn_id, session_id, artifact_digest, occurred_at, detail_json, receipt_digest
      ) VALUES (
        ${sqlLiteral(receipt.receiptId)}, ${sqlLiteral(receipt.producerFactId)},
        ${sqlLiteral(receipt.kind)}, ${sqlLiteral(columns.actorServerId)},
        ${sqlLiteral(columns.actorMachineId)}, ${sqlLiteral(columns.actorAgentId)},
        ${sqlLiteral(columns.leaseEpoch)}, ${sqlLiteral(columns.fenceToken)},
        ${sqlLiteral(columns.launchId)}, ${sqlLiteral(columns.stateInstanceId)},
        ${sqlLiteral(columns.turnId)}, ${sqlLiteral(columns.sessionId)},
        ${sqlLiteral(columns.artifactDigest)}, ${sqlLiteral(receipt.occurredAt)}::timestamptz,
        ${sqlLiteral(canonicalJson(receipt))}::jsonb, ${sqlLiteral(receiptDigest)}
      );`,
    );
    return { receiptId: receipt.receiptId, replayed: false };
  }
}

export class ReminderRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async advanceGeneration(input: {
    reminderId: string;
    owner: ReceiptActor;
    anchor: Record<string, unknown>;
    schedule: VersionedResult;
    nextFireAt?: string;
    expectedGeneration?: number;
  }): Promise<number> {
    assertProtocolId(input.reminderId, "cmd");
    const owner = actorColumns(input.owner);
    validateVersionedResult(input.schedule);
    canonicalProtocolJson(input.anchor);
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'reminder:' || ${sqlLiteral(input.reminderId)}, 0
      ));`,
    );
    const current = await this.#session.queryJson<{ generation: number }>(
      `SELECT json_build_object('generation', coalesce((
        SELECT generation FROM reminders
        WHERE reminder_id = ${sqlLiteral(input.reminderId)}
        ORDER BY generation DESC LIMIT 1 FOR UPDATE
      ), 0));`,
    );
    if (
      input.expectedGeneration !== undefined &&
      Number(current.generation) !== input.expectedGeneration
    ) {
      storageFail("STALE_FENCE", input.reminderId);
    }
    const generation = Number(current.generation) + 1;
    await this.#session.execute(
      `INSERT INTO reminders (
        reminder_id, owner_kind, owner_id, anchor_json, schedule_json,
        generation, next_fire_at
      ) VALUES (
        ${sqlLiteral(input.reminderId)}, ${sqlLiteral(owner.kind)}, ${sqlLiteral(owner.id)},
        ${sqlLiteral(canonicalJson(input.anchor))}::jsonb,
        ${sqlLiteral(canonicalJson(input.schedule))}::jsonb,
        ${sqlLiteral(generation)}, ${sqlLiteral(input.nextFireAt ?? null)}::timestamptz
      );`,
    );
    return generation;
  }

  async cancel(input: { reminderId: string; generation: number }): Promise<void> {
    assertProtocolId(input.reminderId, "cmd");
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'reminder:' || ${sqlLiteral(input.reminderId)}, 0
      ));`,
    );
    const current = await this.#session.queryJson<{ generation: number | null }>(
      `SELECT json_build_object('generation', (
        SELECT generation FROM reminders
        WHERE reminder_id = ${sqlLiteral(input.reminderId)}
        ORDER BY generation DESC LIMIT 1 FOR UPDATE
      ));`,
    );
    if (Number(current.generation) !== input.generation) {
      storageFail("STALE_FENCE", input.reminderId);
    }
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE reminders SET canceled_at = clock_timestamp()
        WHERE reminder_id = ${sqlLiteral(input.reminderId)}
          AND generation = ${sqlLiteral(input.generation)} AND canceled_at IS NULL
          AND generation = (SELECT max(generation) FROM reminders WHERE reminder_id = ${sqlLiteral(input.reminderId)})
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("STALE_FENCE", input.reminderId);
  }
}

export class LaunchRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async create(input: {
    launchId: string;
    machineId: string;
    agentId: string;
    runtimeKind: "codex" | "claude";
    workspaceGeneration: number;
    routingGeneration: number;
  }): Promise<void> {
    assertProtocolId(input.launchId, "lnc");
    assertProtocolId(input.machineId, "mch");
    assertProtocolId(input.agentId, "agt");
    const pairing = await this.#session.queryJson<{ valid: boolean }>(
      `SELECT json_build_object('valid', EXISTS (
        SELECT 1 FROM machines AS machine
        JOIN agents AS agent ON agent.server_id = machine.server_id
        WHERE machine.machine_id = ${sqlLiteral(input.machineId)}
          AND agent.agent_id = ${sqlLiteral(input.agentId)}
      ));`,
    );
    if (!pairing.valid) storageFail("INVALID_STATE_TRANSITION", input.launchId);
    await this.#session.execute(
      `INSERT INTO agent_launches (
        launch_id, machine_id, agent_id, runtime_kind, workspace_generation,
        routing_generation, state
      ) VALUES (
        ${sqlLiteral(input.launchId)}, ${sqlLiteral(input.machineId)},
        ${sqlLiteral(input.agentId)}, ${sqlLiteral(input.runtimeKind)},
        ${sqlLiteral(input.workspaceGeneration)}, ${sqlLiteral(input.routingGeneration)},
        'requested'
      );`,
    );
  }

  async transition(input: {
    launchId: string;
    from: "requested" | "ready" | "activated";
    to: "ready" | "activated" | "terminal";
    terminalReason?: string;
  }): Promise<void> {
    assertProtocolId(input.launchId, "lnc");
    if (input.to !== "terminal" && input.terminalReason !== undefined) {
      storageFail("INVALID_STATE_TRANSITION", input.launchId);
    }
    const valid =
      (input.from === "requested" && input.to === "ready") ||
      (input.from === "ready" && input.to === "activated") ||
      (input.to === "terminal" && input.from !== "activated") ||
      (input.from === "activated" && input.to === "terminal");
    if (!valid) storageFail("INVALID_STATE_TRANSITION", input.launchId);
    const timestampColumn = input.to === "ready"
      ? "ready_at"
      : input.to === "activated"
        ? "activated_at"
        : "terminal_at";
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE agent_launches SET state = ${sqlLiteral(input.to)},
          ${timestampColumn} = clock_timestamp(),
          terminal_reason = ${sqlLiteral(input.terminalReason ?? null)}
        WHERE launch_id = ${sqlLiteral(input.launchId)} AND state = ${sqlLiteral(input.from)}
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("STALE_FENCE", input.launchId);
  }
}

export class ArtifactRepository {
  readonly #session: PsqlSession;
  readonly #claims: ClaimFenceRepository;

  constructor(session: PsqlSession, claims: ClaimFenceRepository) {
    this.#session = session;
    this.#claims = claims;
  }

  async publish(input: {
    fence: TaskLease;
    artifactDigest: ArtifactDigest;
    commitSha?: string;
  }): Promise<void> {
    const fence = await this.#claims.assertCurrent(input.fence);
    assertArtifactDigest(input.artifactDigest);
    if (input.commitSha !== undefined && !/^[0-9a-f]{40}$/u.test(input.commitSha)) {
      storageFail("INVALID_IDENTIFIER", input.commitSha);
    }
    await this.#session.execute(
      `INSERT INTO artifacts (
        artifact_digest, commit_sha, task_id, claim_id, lease_id,
        lease_epoch, fence_token
      ) VALUES (
        ${sqlLiteral(input.artifactDigest)}, ${sqlLiteral(input.commitSha ?? null)},
        ${sqlLiteral(fence.taskId)}, ${sqlLiteral(fence.claimId)},
        ${sqlLiteral(fence.leaseId)}, ${sqlLiteral(fence.leaseEpoch)},
        ${sqlLiteral(fence.fenceToken)}
      );`,
    );
  }

  async recordReview(input: {
    receiptBytes: Uint8Array;
    receiptDigest: ArtifactDigest;
    reviewerSeat: string;
    verdict: "go" | "block";
    scenarioVersion: number;
  }): Promise<void> {
    const receipt = parseFrozenTransitionReceipt(input.receiptBytes);
    if (receipt.kind !== "review_verdict") storageFail("INVALID_STATE_TRANSITION");
    assertArtifactDigest(input.receiptDigest);
    if (digestReceiptPayload(receipt) !== input.receiptDigest) {
      storageFail("IDEMPOTENCY_CONFLICT", receipt.receiptId);
    }
    if (input.reviewerSeat.trim().length === 0) {
      storageFail("INVALID_IDENTIFIER", input.reviewerSeat);
    }
    if (!Number.isSafeInteger(input.scenarioVersion) || input.scenarioVersion < 1) {
      storageFail("INVALID_IDENTIFIER", input.scenarioVersion);
    }
    if (!("agentId" in receipt.actor)) storageFail("INVALID_STATE_TRANSITION");
    const currentReviewerFence = await this.#session.queryJson<{ current: boolean }>(
      `SELECT json_build_object('current', EXISTS (
        SELECT 1 FROM task_claims
        WHERE owner_agent_id = ${sqlLiteral(receipt.actor.agentId)}
          AND lease_epoch = ${sqlLiteral(receipt.fence.leaseEpoch)}
          AND fence_token = ${sqlLiteral(receipt.fence.fenceToken)}
          AND released_at IS NULL AND expires_at > clock_timestamp()
      ));`,
    );
    if (!currentReviewerFence.current) storageFail("STALE_FENCE", receipt.receiptId);
    const reviewKey = digestCanonical({
      artifactDigest: receipt.artifactDigest,
      reviewerAgentId: receipt.actor.agentId,
      reviewerSeat: input.reviewerSeat,
      scenarioVersion: input.scenarioVersion,
    });
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'review:' || ${sqlLiteral(reviewKey)}, 0
      ));`,
    );
    const existing = await this.#session.queryJson<{
      reviewId: string;
      verdict: string;
    } | null>(
      `SELECT coalesce((SELECT json_build_object(
        'reviewId', review_id, 'verdict', verdict
      ) FROM reviews
      WHERE artifact_digest = ${sqlLiteral(receipt.artifactDigest)}
        AND reviewer_agent_id = ${sqlLiteral(receipt.actor.agentId)}
        AND reviewer_seat = ${sqlLiteral(input.reviewerSeat)}
        AND scenario_version = ${sqlLiteral(input.scenarioVersion)}), 'null'::json);`,
    );
    if (existing !== null) {
      if (existing.verdict !== input.verdict) {
        storageFail("IDEMPOTENCY_CONFLICT", existing.reviewId);
      }
      return;
    }
    const recorded = await new ReceiptRepository(this.#session).record(
      input.receiptBytes,
      input.receiptDigest,
    );
    await this.#session.execute(
      `INSERT INTO reviews (
        review_id, artifact_digest, reviewer_agent_id, reviewer_seat,
        verdict, scenario_version
      ) VALUES (
        ${sqlLiteral(recorded.receiptId)}, ${sqlLiteral(receipt.artifactDigest)},
        ${sqlLiteral(receipt.actor.agentId)}, ${sqlLiteral(input.reviewerSeat)},
        ${sqlLiteral(input.verdict)}, ${sqlLiteral(input.scenarioVersion)}
      );`,
    );
  }
}

export class OutboxRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async enqueue(event: OutboxEvent): Promise<number> {
    assertProtocolId(event.producerFactId, "fac");
    if (event.version !== 1) storageFail("INVALID_IDENTIFIER", event.version);
    validateVersionedResult(event.payload);
    const namespace = `${event.kind}.v${event.version}`;
    const canonicalPayload = canonicalJson(event.payload);
    const idempotencyKey = digestCanonical({
      producerFactId: event.producerFactId,
      payload: event.payload,
    });
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'outbox_fact:' || ${sqlLiteral(event.producerFactId)} || ':' ||
          ${sqlLiteral(event.kind)} || ':' || ${sqlLiteral(event.version)}, 0
      ));
      SELECT pg_advisory_xact_lock(hashtextextended(
        'outbox:' || ${sqlLiteral(namespace)} || ':' || ${sqlLiteral(idempotencyKey)}, 0
      ));`,
    );
    const factEvent = await this.#session.queryJson<{
      jobId: number;
      namespace: string;
      idempotencyKey: string;
      payload: VersionedResult;
    } | null>(
      `SELECT coalesce((SELECT json_build_object(
        'jobId', job_id, 'namespace', idempotency_namespace,
        'idempotencyKey', idempotency_key, 'payload', payload_json
      ) FROM outbox_jobs
      WHERE producer_fact_id = ${sqlLiteral(event.producerFactId)}
        AND event_kind = ${sqlLiteral(event.kind)}
        AND event_version = ${sqlLiteral(event.version)}), 'null'::json);`,
    );
    if (factEvent !== null) {
      if (
        factEvent.namespace !== namespace ||
        factEvent.idempotencyKey !== idempotencyKey ||
        canonicalJson(factEvent.payload) !== canonicalPayload
      ) {
        storageFail("IDEMPOTENCY_CONFLICT", event.producerFactId);
      }
      return Number(factEvent.jobId);
    }
    const existing = await this.#session.queryJson<{
      jobId: number;
      producerFactId: string;
      eventKind: string;
      eventVersion: number;
      payload: VersionedResult;
    } | null>(
      `SELECT coalesce((SELECT json_build_object(
        'jobId', job_id, 'producerFactId', producer_fact_id,
        'eventKind', event_kind, 'eventVersion', event_version,
        'payload', payload_json
      ) FROM outbox_jobs
      WHERE idempotency_namespace = ${sqlLiteral(namespace)}
        AND idempotency_key = ${sqlLiteral(idempotencyKey)}), 'null'::json);`,
    );
    if (existing !== null) {
      if (
        existing.producerFactId !== event.producerFactId ||
        existing.eventKind !== event.kind ||
        Number(existing.eventVersion) !== event.version ||
        canonicalJson(existing.payload) !== canonicalPayload
      ) {
        storageFail("IDEMPOTENCY_CONFLICT", idempotencyKey);
      }
      return Number(existing.jobId);
    }
    const result = await this.#session.queryJson<{ jobId: number }>(
      `WITH inserted AS (
        INSERT INTO outbox_jobs (
          idempotency_namespace, idempotency_key, producer_fact_id,
          event_kind, event_version, payload_json
        ) VALUES (
          ${sqlLiteral(namespace)}, ${sqlLiteral(idempotencyKey)},
          ${sqlLiteral(event.producerFactId)}, ${sqlLiteral(event.kind)},
          ${sqlLiteral(event.version)}, ${sqlLiteral(canonicalPayload)}::jsonb
        )
        RETURNING job_id
      ) SELECT json_build_object('jobId', job_id) FROM inserted;`,
    );
    return Number(result.jobId);
  }

  async leaseBatch(input: {
    workerLeaseId: string;
    leaseUntil: string;
    limit: number;
  }): Promise<OutboxJob[]> {
    assertProtocolId(input.workerLeaseId, "lse");
    if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 1000) {
      storageFail("INVALID_IDENTIFIER", input.limit);
    }
    await this.#session.execute(
      `UPDATE outbox_jobs SET status = 'pending', worker_lease_id = NULL,
        worker_lease_until = NULL, updated_at = clock_timestamp()
       WHERE status = 'leased' AND worker_lease_until <= clock_timestamp();`,
    );
    return this.#session.queryJson<OutboxJob[]>(
      `WITH candidates AS (
        SELECT job_id FROM outbox_jobs
        WHERE status = 'pending' AND due_at <= clock_timestamp()
        ORDER BY due_at, job_id
        FOR UPDATE SKIP LOCKED LIMIT ${sqlLiteral(input.limit)}
      ), leased AS (
        UPDATE outbox_jobs AS jobs SET
          status = 'leased', attempt = jobs.attempt + 1,
          worker_lease_id = ${sqlLiteral(input.workerLeaseId)},
          worker_lease_until = ${sqlLiteral(input.leaseUntil)}::timestamptz,
          updated_at = clock_timestamp()
        FROM candidates WHERE jobs.job_id = candidates.job_id
        RETURNING jobs.job_id AS "jobId", jobs.idempotency_namespace AS namespace,
          jobs.idempotency_key AS "idempotencyKey", jobs.producer_fact_id AS "producerFactId",
          jobs.event_kind AS "eventKind", jobs.event_version AS "eventVersion", jobs.attempt
      ) SELECT coalesce(json_agg(leased ORDER BY "jobId"), '[]'::json) FROM leased;`,
    );
  }

  async ack(input: { jobId: number; attempt: number; workerLeaseId: string }): Promise<void> {
    assertProtocolId(input.workerLeaseId, "lse");
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE outbox_jobs SET status = 'acked', worker_lease_id = NULL,
          worker_lease_until = NULL, updated_at = clock_timestamp()
        WHERE job_id = ${sqlLiteral(input.jobId)} AND attempt = ${sqlLiteral(input.attempt)}
          AND worker_lease_id = ${sqlLiteral(input.workerLeaseId)}
          AND worker_lease_until > clock_timestamp() AND status = 'leased'
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("OUTBOX_STALE_ATTEMPT", input.jobId);
  }

  async release(input: {
    jobId: number;
    attempt: number;
    workerLeaseId: string;
    dueAt: string;
    error: string;
  }): Promise<void> {
    assertProtocolId(input.workerLeaseId, "lse");
    const result = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE outbox_jobs SET status = 'pending', worker_lease_id = NULL,
          worker_lease_until = NULL, due_at = ${sqlLiteral(input.dueAt)}::timestamptz,
          last_error = ${sqlLiteral(input.error)}, updated_at = clock_timestamp()
        WHERE job_id = ${sqlLiteral(input.jobId)} AND attempt = ${sqlLiteral(input.attempt)}
          AND worker_lease_id = ${sqlLiteral(input.workerLeaseId)}
          AND status = 'leased' RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(result.changed) !== 1) storageFail("OUTBOX_STALE_ATTEMPT", input.jobId);
  }
}

export class SharedTransaction {
  readonly sequences: TargetSequenceRepository;
  readonly messages: MessageRepository;
  readonly claims: ClaimFenceRepository;
  readonly taskGraph: TaskGraphRepository;
  readonly reminders: ReminderRepository;
  readonly launches: LaunchRepository;
  readonly deliveries: DeliveryRepository;
  readonly receipts: ReceiptRepository;
  readonly artifacts: ArtifactRepository;
  readonly outbox: OutboxRepository;

  constructor(session: PsqlSession) {
    this.sequences = new TargetSequenceRepository(session);
    this.messages = new MessageRepository(session);
    this.claims = new ClaimFenceRepository(session);
    this.taskGraph = new TaskGraphRepository(session);
    this.reminders = new ReminderRepository(session);
    this.launches = new LaunchRepository(session);
    this.deliveries = new DeliveryRepository(session);
    this.receipts = new ReceiptRepository(session);
    this.artifacts = new ArtifactRepository(session, this.claims);
    this.outbox = new OutboxRepository(session);
  }
}

type StoredRequest<T> = {
  found: boolean;
  requestDigest?: string;
  result?: T;
  resultDigest?: string;
};

export class SharedStore {
  readonly #databaseUrl: string;
  readonly #schema: string;

  constructor(databaseUrl: string, schema = "swarm_storage") {
    if (!SCHEMA.test(schema)) storageFail("INVALID_IDENTIFIER", schema);
    this.#databaseUrl = databaseUrl;
    this.#schema = schema;
  }

  migrate(): Promise<MigrationReceipt[]> {
    return new PostgresMigrator(this.#databaseUrl, this.#schema).migrate();
  }

  resetForTests(): Promise<void> {
    return new PostgresMigrator(this.#databaseUrl, this.#schema).resetForTests();
  }

  async transaction<T extends VersionedResult>(
    request: IdempotentRequest,
    body: (transaction: SharedTransaction) => Promise<T>,
  ): Promise<IdempotentResult<T>> {
    const actor = validateRequest(request);
    const session = await PsqlSession.open(this.#databaseUrl);
    try {
      await session.execute(
        `BEGIN;
         SET LOCAL search_path TO ${this.#schema}, pg_catalog;
         SET LOCAL idle_in_transaction_session_timeout = '30s';`,
      );
      const existing = await this.#readRequest<T>(session, request, actor);
      if (existing.found) {
        if (existing.requestDigest !== request.requestDigest) {
          storageFail("IDEMPOTENCY_CONFLICT", request.requestId);
        }
        await session.execute("COMMIT;");
        await session.close();
        return {
          result: existing.result ?? storageFail("IDEMPOTENCY_CONFLICT"),
          resultDigest: (existing.resultDigest ?? storageFail("IDEMPOTENCY_CONFLICT")) as ArtifactDigest,
          replayed: true,
        };
      }
      const result = validateVersionedResult(await body(new SharedTransaction(session)));
      const resultDigest = digestCanonical(result);
      await session.execute(
        `INSERT INTO command_requests (
          actor_kind, actor_id, scope, request_kind, request_id,
          request_digest, result_json, result_digest
        ) VALUES (
          ${sqlLiteral(actor.kind)}, ${sqlLiteral(actor.id)}, ${sqlLiteral(request.scope)},
          ${sqlLiteral(request.requestKind)}, ${sqlLiteral(request.requestId)},
          ${sqlLiteral(request.requestDigest)}, ${sqlLiteral(canonicalJson(result))}::jsonb,
          ${sqlLiteral(resultDigest)}
        );
        COMMIT;`,
      );
      await session.close();
      return { result, resultDigest, replayed: false };
    } catch (error) {
      await session.rollbackAndClose();
      const winner = await this.#readWinner<T>(request, actor);
      if (winner !== undefined) return winner;
      throw error;
    }
  }

  async #readRequest<T extends VersionedResult>(
    session: PsqlSession,
    request: IdempotentRequest,
    actor: ActorColumns,
  ): Promise<StoredRequest<T>> {
    return session.queryJson<StoredRequest<T>>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', request_digest,
        'result', result_json, 'resultDigest', result_digest
      ) FROM command_requests WHERE
        actor_kind = ${sqlLiteral(actor.kind)} AND actor_id = ${sqlLiteral(actor.id)}
        AND scope = ${sqlLiteral(request.scope)} AND request_kind = ${sqlLiteral(request.requestKind)}
        AND request_id = ${sqlLiteral(request.requestId)}),
        json_build_object('found', false));`,
    );
  }

  async #readWinner<T extends VersionedResult>(
    request: IdempotentRequest,
    actor: ActorColumns,
  ): Promise<IdempotentResult<T> | undefined> {
    let reader: PsqlSession;
    try {
      reader = await PsqlSession.open(this.#databaseUrl);
    } catch {
      return undefined;
    }
    try {
      await reader.execute(`SET search_path TO ${this.#schema}, pg_catalog;`);
      const stored = await this.#readRequest<T>(reader, request, actor);
      if (!stored.found) return undefined;
      if (stored.requestDigest !== request.requestDigest) {
        storageFail("IDEMPOTENCY_CONFLICT", request.requestId);
      }
      return {
        result: stored.result ?? storageFail("IDEMPOTENCY_CONFLICT"),
        resultDigest: (stored.resultDigest ?? storageFail("IDEMPOTENCY_CONFLICT")) as ArtifactDigest,
        replayed: true,
      };
    } finally {
      await reader.close();
    }
  }
}
