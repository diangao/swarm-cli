import { createHash } from "node:crypto";

import {
  canonicalProtocolJson,
  messageBodyHasContent,
  parseAttentionNotice,
  type ArtifactDigest,
  type AttentionNotice,
  type ProtocolVersion,
  type Target,
} from "@swarm/protocol";
import { storageFail } from "../errors.js";
import { assertArtifactDigest, assertFenceToken, assertProtocolId, targetColumns } from "../protocol.js";
import { PsqlSession, sqlLiteral } from "./session.js";
import { TaskCommandRepository } from "./wave1.js";

function canonicalJson(value: unknown): string {
  return new TextDecoder().decode(canonicalProtocolJson(value));
}

function canonicalDigest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

export type AppendHumanMessageInput = {
  protocolVersion: 1;
  messageId: string;
  target: Target;
  humanId: string;
  body: string;
  producerFactId: string;
  requestDigest: ArtifactDigest;
  serverId: string;
  receiptId: string;
  deliveryId: string;
  occurredAt: string;
};

export type CreateTaskInput = {
  commandId: string;
  requestDigest: ArtifactDigest;
  incomingProducerFactId: string;
  sourceMessageId: string;
  turnId: string;
  taskId: string;
  title: string;
  serverId: string;
  taskNumber: number;
  result: Record<string, unknown>;
  receiptId: string;
  machineId: string;
  agentId: string;
  launchId: string;
  stateInstanceId: string;
  sessionId: string;
  leaseEpoch: number;
  fenceToken: string;
  occurredAt: string;
};

export function appendHumanMessageDigest(
  input: Omit<AppendHumanMessageInput, "requestDigest" | "receiptId" | "deliveryId" | "occurredAt">,
): ArtifactDigest {
  return canonicalDigest({
    protocolVersion: input.protocolVersion,
    messageId: input.messageId,
    target: input.target,
    humanId: input.humanId,
    body: input.body,
    producerFactId: input.producerFactId,
    serverId: input.serverId,
  });
}

type LockedRoute = {
  found: boolean;
  agentId?: string;
  machineId?: string;
  launchId?: string;
  membershipEpoch?: number;
  routingGeneration?: number;
  routeVersion?: number;
  launchState?: string;
  launchRoutingGeneration?: number;
};

export class ServerMessageRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async append(input: AppendHumanMessageInput): Promise<{
    messageId: string;
    targetSeq: number;
    producerFactId: string;
    deliveryId: string;
    outboxJobId: number;
    replayed: boolean;
  }> {
    assertProtocolId(input.messageId, "msg");
    assertProtocolId(input.humanId, "hum");
    assertProtocolId(input.producerFactId, "fac");
    assertProtocolId(input.serverId, "srv");
    assertProtocolId(input.receiptId, "rcp");
    assertProtocolId(input.deliveryId, "dlv");
    assertArtifactDigest(input.requestDigest);
    if (!messageBodyHasContent(input.body)) storageFail("EMPTY_MESSAGE", input.messageId);
    if (appendHumanMessageDigest(input) !== input.requestDigest) {
      storageFail("IDEMPOTENCY_CONFLICT", input.producerFactId);
    }
    const target = targetColumns(input.target);
    const membershipTable = target.kind === "channel" ? "memberships" : "conversation_memberships";
    const membershipTargetColumn = target.kind === "channel" ? "channel_id" : "conversation_id";

    const actor = await this.#session.queryJson<{ valid: boolean; epoch?: number }>(
      `WITH human AS (
        SELECT 1 FROM humans WHERE human_id = ${sqlLiteral(input.humanId)}
          AND server_id = ${sqlLiteral(input.serverId)} FOR UPDATE
      ), member AS (
        SELECT membership_epoch FROM ${membershipTable}
        WHERE ${membershipTargetColumn} = ${sqlLiteral(target.ownerId)}
          AND actor_kind = 'human' AND actor_id = ${sqlLiteral(input.humanId)}
          AND state = 'active' FOR UPDATE
      ) SELECT json_build_object(
        'valid', EXISTS (SELECT 1 FROM human) AND EXISTS (SELECT 1 FROM member),
        'epoch', (SELECT membership_epoch FROM member)
      );`,
    );
    if (!actor.valid) storageFail("MEMBERSHIP_REVOKED_BEFORE_CONSUME", input.humanId);

    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'producer_fact:' || ${sqlLiteral(input.producerFactId)}, 0
      ));`,
    );
    const existing = await this.#session.queryJson<{
      found: boolean;
      messageId?: string;
      targetSeq?: number;
      requestDigest?: string;
      body?: string;
      deliveryId?: string;
      outboxJobId?: number;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'messageId', m.message_id, 'targetSeq', m.target_seq,
        'requestDigest', m.payload_digest, 'body', m.body,
        'deliveryId', d.delivery_id, 'outboxJobId', d.outbox_job_id
      ) FROM messages m
      JOIN deliveries d ON d.message_id = m.message_id AND d.attempt = 1
      JOIN receipts r ON r.producer_fact_id = m.producer_fact_id AND r.kind = 'server_accepted'
      WHERE m.producer_fact_id = ${sqlLiteral(input.producerFactId)}), json_build_object('found', false));`,
    );
    if (existing.found) {
      if (
        existing.messageId !== input.messageId
        || existing.requestDigest !== input.requestDigest
        || existing.body !== input.body
      ) storageFail("IDEMPOTENCY_CONFLICT", input.producerFactId);
      return {
        messageId: existing.messageId,
        targetSeq: Number(existing.targetSeq),
        producerFactId: input.producerFactId,
        deliveryId: existing.deliveryId ?? storageFail("IDEMPOTENCY_CONFLICT", input.producerFactId),
        outboxJobId: Number(existing.outboxJobId),
        replayed: true,
      };
    }
    await this.#session.execute(
      `SELECT actor_kind, actor_id FROM ${membershipTable}
       WHERE ${membershipTargetColumn} = ${sqlLiteral(target.ownerId)} AND state = 'active'
       ORDER BY actor_kind, actor_id FOR UPDATE;`,
    );

    const route = await this.#session.queryJson<LockedRoute>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'agentId', r.agent_id, 'machineId', r.machine_id,
        'launchId', r.expected_launch_id, 'membershipEpoch', r.membership_epoch,
        'routingGeneration', r.routing_generation, 'routeVersion', r.route_version,
        'launchState', l.state, 'launchRoutingGeneration', l.routing_generation
      ) FROM target_owner_routes r
      JOIN ${membershipTable} m ON m.${membershipTargetColumn} = r.target_id
        AND m.actor_kind = 'agent' AND m.actor_id = r.agent_id
        AND m.state = 'active' AND m.membership_epoch = r.membership_epoch
      JOIN agent_launches l ON l.launch_id = r.expected_launch_id
        AND l.machine_id = r.machine_id AND l.agent_id = r.agent_id
      WHERE r.target_kind = ${sqlLiteral(target.kind)} AND r.target_id = ${sqlLiteral(target.ownerId)}
        AND r.thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(target.threadRootMessageId)}
      FOR UPDATE OF r, m, l), json_build_object('found', false));`,
    );
    if (
      !route.found || route.launchState !== "activated"
      || Number(route.launchRoutingGeneration) !== Number(route.routingGeneration)
    ) storageFail("ROUTE_SUPERSEDED_BEFORE_CONSUME", target.ownerId);
    const allocated = await this.#session.queryJson<{ targetSeq: number }>(
      `WITH allocated AS (
        INSERT INTO target_sequences(target_kind, target_id, thread_root_message_id, next_seq)
        VALUES (${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)},
          ${sqlLiteral(target.threadRootMessageId)}, 2)
        ON CONFLICT (target_kind, target_id, thread_root_message_id)
        DO UPDATE SET next_seq = target_sequences.next_seq + 1
        RETURNING next_seq - 1 AS target_seq
      ) SELECT json_build_object('targetSeq', target_seq) FROM allocated;`,
    );

    await this.#session.execute(
      `INSERT INTO messages(
        message_id, target_kind, target_id, thread_root_message_id, author_kind,
        author_id, target_seq, body, producer_fact_id, payload_digest
      ) VALUES (
        ${sqlLiteral(input.messageId)}, ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)},
        ${sqlLiteral(target.threadRootMessageId)}, 'human', ${sqlLiteral(input.humanId)},
        ${sqlLiteral(allocated.targetSeq)}, ${sqlLiteral(input.body)},
        ${sqlLiteral(input.producerFactId)}, ${sqlLiteral(input.requestDigest)}
      );`,
    );

    const receiptDetail = {
      protocolVersion: 1,
      kind: "server_accepted",
      messageId: input.messageId,
      producerFactId: input.producerFactId,
    };
    await this.#session.execute(
      `INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_server_id, occurred_at,
        detail_json, receipt_digest
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(input.producerFactId)}, 'server_accepted',
        ${sqlLiteral(input.serverId)}, ${sqlLiteral(input.occurredAt)}::timestamptz,
        ${sqlLiteral(canonicalJson(receiptDetail))}::jsonb,
        ${sqlLiteral(canonicalDigest(receiptDetail))}
      );
      INSERT INTO message_audience(message_id, actor_kind, actor_id, membership_epoch, audience_mode)
      SELECT ${sqlLiteral(input.messageId)}, actor_kind, actor_id, membership_epoch,
        CASE
          WHEN actor_kind = 'human' THEN 'member_body'
          WHEN actor_id = ${sqlLiteral(route.agentId ?? null)} THEN 'owner_body'
          ELSE 'attention_metadata'
        END
      FROM ${membershipTable}
      WHERE ${membershipTargetColumn} = ${sqlLiteral(target.ownerId)} AND state = 'active';
      INSERT INTO message_owner_routes(
        producer_fact_id, message_id, target_kind, target_id, thread_root_message_id,
        agent_id, machine_id, expected_launch_id, membership_epoch,
        routing_generation, route_version
      ) VALUES (
        ${sqlLiteral(input.producerFactId)}, ${sqlLiteral(input.messageId)},
        ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
        ${sqlLiteral(route.agentId ?? null)}, ${sqlLiteral(route.machineId ?? null)},
        ${sqlLiteral(route.launchId ?? null)}, ${sqlLiteral(route.membershipEpoch ?? null)},
        ${sqlLiteral(route.routingGeneration ?? null)}, ${sqlLiteral(route.routeVersion ?? null)}
      );`,
    );

    const outboxPayload = {
      protocolVersion: 1,
      messageId: input.messageId,
      producerFactId: input.producerFactId,
      target: input.target,
      serverSeq: allocated.targetSeq,
      agentId: route.agentId,
      machineId: route.machineId,
      expectedLaunchId: route.launchId,
      membershipEpoch: route.membershipEpoch,
      routingGeneration: route.routingGeneration,
      routeVersion: route.routeVersion,
    };
    const outbox = await this.#session.queryJson<{ jobId: number }>(
      `WITH inserted AS (
        INSERT INTO outbox_jobs(
          idempotency_namespace, idempotency_key, producer_fact_id,
          event_kind, event_version, payload_json, status
        ) VALUES (
          'message_delivery.v1', ${sqlLiteral(`${input.producerFactId}:${route.agentId}`)},
          ${sqlLiteral(input.producerFactId)}, 'message_delivery', 1,
          ${sqlLiteral(canonicalJson(outboxPayload))}::jsonb, 'pending'
        ) RETURNING job_id
      ) SELECT json_build_object('jobId', job_id) FROM inserted;`,
    );
    await this.#session.execute(
      `INSERT INTO deliveries(
        delivery_id, attempt, message_id, target_kind, target_id, thread_root_message_id,
        target_seq, producer_fact_id, agent_id, machine_id, expected_launch_id,
        status, outbox_job_id, membership_epoch, routing_generation, route_version
      ) VALUES (
        ${sqlLiteral(input.deliveryId)}, 1, ${sqlLiteral(input.messageId)},
        ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
        ${sqlLiteral(allocated.targetSeq)}, ${sqlLiteral(input.producerFactId)},
        ${sqlLiteral(route.agentId ?? null)}, ${sqlLiteral(route.machineId ?? null)},
        ${sqlLiteral(route.launchId ?? null)}, 'pending', ${sqlLiteral(outbox.jobId)},
        ${sqlLiteral(route.membershipEpoch ?? null)}, ${sqlLiteral(route.routingGeneration ?? null)},
        ${sqlLiteral(route.routeVersion ?? null)}
      );`,
    );
    return {
      messageId: input.messageId,
      targetSeq: allocated.targetSeq,
      producerFactId: input.producerFactId,
      deliveryId: input.deliveryId,
      outboxJobId: outbox.jobId,
      replayed: false,
    };
  }

  async readHumanBody(input: { target: Target; messageId: string; humanId: string }): Promise<{
    protocolVersion: 1;
    messageId: string;
    producerFactId: string;
    serverSeq: number;
    body: string;
    membershipEpoch: number;
  } | null> {
    assertProtocolId(input.messageId, "msg");
    assertProtocolId(input.humanId, "hum");
    const target = targetColumns(input.target);
    const table = target.kind === "channel" ? "memberships" : "conversation_memberships";
    const column = target.kind === "channel" ? "channel_id" : "conversation_id";
    return await this.#session.queryJson<{
      protocolVersion: 1;
      messageId: string;
      producerFactId: string;
      serverSeq: number;
      body: string;
      membershipEpoch: number;
    } | null>(
      `WITH member AS (
        SELECT membership_epoch FROM ${table}
        WHERE ${column} = ${sqlLiteral(target.ownerId)} AND actor_kind = 'human'
          AND actor_id = ${sqlLiteral(input.humanId)} AND state = 'active' FOR UPDATE
      ) SELECT coalesce((SELECT json_build_object(
        'protocolVersion', 1, 'messageId', m.message_id, 'producerFactId', m.producer_fact_id,
        'serverSeq', m.target_seq, 'body', m.body, 'membershipEpoch', member.membership_epoch
      ) FROM member JOIN message_audience a ON a.actor_kind = 'human'
        AND a.actor_id = ${sqlLiteral(input.humanId)}
        AND a.membership_epoch = member.membership_epoch AND a.audience_mode = 'member_body'
      JOIN messages m ON m.message_id = a.message_id
      WHERE m.message_id = ${sqlLiteral(input.messageId)}
        AND m.target_kind = ${sqlLiteral(target.kind)} AND m.target_id = ${sqlLiteral(target.ownerId)}
        AND m.thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(target.threadRootMessageId)}), 'null'::json);`,
    );
  }

  async listAttention(input: { target: Target; agentId: string }): Promise<AttentionNotice[]> {
    assertProtocolId(input.agentId, "agt");
    const target = targetColumns(input.target);
    const table = target.kind === "channel" ? "memberships" : "conversation_memberships";
    const column = target.kind === "channel" ? "channel_id" : "conversation_id";
    const aggregate = await this.#session.queryJson<{
      pendingCount: number;
      firstMessageId: string | null;
      latestMessageId: string | null;
      firstServerSeq: number | null;
      latestServerSeq: number | null;
    }>(
      `WITH member AS (
        SELECT membership_epoch FROM ${table}
        WHERE ${column} = ${sqlLiteral(target.ownerId)} AND actor_kind = 'agent'
          AND actor_id = ${sqlLiteral(input.agentId)} AND state = 'active' FOR UPDATE
      ), cursor AS (
        SELECT coalesce(max(server_seq), 0) AS server_seq FROM observation_cursors, member
        WHERE actor_kind = 'agent' AND actor_id = ${sqlLiteral(input.agentId)}
          AND stream = 'agent_attention' AND target_kind = ${sqlLiteral(target.kind)}
          AND target_id = ${sqlLiteral(target.ownerId)}
          AND thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(target.threadRootMessageId)}
          AND observation_cursors.membership_epoch = member.membership_epoch
      ), authorized AS (
        SELECT m.message_id, m.target_seq FROM member
        JOIN message_audience a ON a.actor_kind = 'agent'
          AND a.actor_id = ${sqlLiteral(input.agentId)}
          AND a.membership_epoch = member.membership_epoch
          AND a.audience_mode = 'attention_metadata'
        JOIN messages m ON m.message_id = a.message_id
        CROSS JOIN cursor
        WHERE m.target_kind = ${sqlLiteral(target.kind)} AND m.target_id = ${sqlLiteral(target.ownerId)}
          AND m.thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(target.threadRootMessageId)}
          AND m.target_seq > cursor.server_seq
      ) SELECT json_build_object(
        'pendingCount', count(*),
        'firstMessageId', (array_agg(message_id ORDER BY target_seq))[1],
        'latestMessageId', (array_agg(message_id ORDER BY target_seq DESC))[1],
        'firstServerSeq', min(target_seq), 'latestServerSeq', max(target_seq)
      ) FROM authorized;`,
    );
    if (Number(aggregate.pendingCount) === 0) return [];
    const notice = {
      protocolVersion: 1,
      target: input.target,
      pendingCount: Number(aggregate.pendingCount),
      firstMessageId: aggregate.firstMessageId,
      latestMessageId: aggregate.latestMessageId,
      firstServerSeq: Number(aggregate.firstServerSeq),
      latestServerSeq: Number(aggregate.latestServerSeq),
    };
    return [parseAttentionNotice(canonicalProtocolJson(notice), 1 as ProtocolVersion)];
  }

  async appendReply(input: {
    protocolVersion: 1;
    messageId: string;
    producerFactId: string;
    requestDigest: ArtifactDigest;
    incomingMessageId: string;
    incomingProducerFactId: string;
    target: Target;
    body: string;
    receiptId: string;
    machineId: string;
    agentId: string;
    launchId: string;
    stateInstanceId: string;
    turnId: string;
    sessionId: string;
    occurredAt: string;
  }): Promise<{ messageId: string; targetSeq: number; outboxJobId: number; replayed: boolean }> {
    assertProtocolId(input.messageId, "msg");
    assertProtocolId(input.producerFactId, "fac");
    assertProtocolId(input.incomingMessageId, "msg");
    assertProtocolId(input.incomingProducerFactId, "fac");
    assertProtocolId(input.receiptId, "rcp");
    assertProtocolId(input.machineId, "mch");
    assertProtocolId(input.agentId, "agt");
    assertProtocolId(input.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.turnId, "trn");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.requestDigest);
    if (!messageBodyHasContent(input.body)) storageFail("EMPTY_MESSAGE", input.messageId);
    const target = targetColumns(input.target);
    const expectedDigest = canonicalDigest({
      protocolVersion: 1, messageId: input.messageId, producerFactId: input.producerFactId,
      incomingMessageId: input.incomingMessageId, incomingProducerFactId: input.incomingProducerFactId,
      target: input.target, body: input.body, agentId: input.agentId, turnId: input.turnId,
    });
    if (expectedDigest !== input.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", input.producerFactId);

    const source = await this.#session.queryJson<{ found: boolean }>(
      `SELECT json_build_object('found', EXISTS (
        SELECT 1 FROM messages WHERE message_id = ${sqlLiteral(input.incomingMessageId)}
          AND producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND target_kind = ${sqlLiteral(target.kind)} AND target_id = ${sqlLiteral(target.ownerId)}
          AND thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(target.threadRootMessageId)}
        FOR UPDATE
      ));`,
    );
    if (!source.found) storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.turnId);

    const predecessor = await this.#session.queryJson<{ found: boolean }>(
      `WITH locked AS (
        SELECT j.status AS job_status, j.hold_reason, d.delivery_id, d.attempt,
          d.active_invocation_generation, i.invocation_id
        FROM outbox_jobs j JOIN deliveries d ON d.outbox_job_id = j.job_id
        JOIN delivery_invocations i ON i.delivery_id = d.delivery_id AND i.attempt = d.attempt
          AND i.invocation_generation = d.active_invocation_generation
        WHERE j.producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND j.idempotency_namespace = 'message_delivery.v1'
        FOR UPDATE OF j, d, i
      ) SELECT json_build_object('found', EXISTS (
        SELECT 1 FROM locked JOIN receipts r
          ON r.producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND r.turn_id = ${sqlLiteral(input.turnId)} AND r.boundary = 'model_visible'
          AND r.delivery_id = locked.delivery_id AND r.attempt = locked.attempt
          AND r.invocation_generation = locked.active_invocation_generation
          AND r.invocation_id = locked.invocation_id
        WHERE locked.job_status = 'acked' AND locked.hold_reason IS NULL
          AND r.actor_machine_id = ${sqlLiteral(input.machineId)}
          AND r.actor_agent_id = ${sqlLiteral(input.agentId)}
          AND r.launch_id = ${sqlLiteral(input.launchId)}
          AND r.state_instance_id = ${sqlLiteral(input.stateInstanceId)}
          AND r.session_id = ${sqlLiteral(input.sessionId)}
        FOR UPDATE OF r
      ));`,
    );
    if (!predecessor.found) storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.turnId);

    const replay = await this.#session.queryJson<{
      found: boolean; messageId?: string; targetSeq?: number; requestDigest?: string; jobId?: number;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'messageId', m.message_id, 'targetSeq', m.target_seq,
        'requestDigest', m.payload_digest, 'jobId', j.job_id
      ) FROM messages m JOIN outbox_jobs j ON j.producer_fact_id = m.producer_fact_id
        AND j.idempotency_namespace = 'client_message.v1'
      JOIN receipts r ON r.effect_message_id = m.message_id AND r.effect_kind = 'reply_committed'
      WHERE m.producer_fact_id = ${sqlLiteral(input.producerFactId)}), json_build_object('found', false));`,
    );
    if (replay.found) {
      if (replay.messageId !== input.messageId || replay.requestDigest !== input.requestDigest) {
        storageFail("IDEMPOTENCY_CONFLICT", input.producerFactId);
      }
      return { messageId: input.messageId, targetSeq: Number(replay.targetSeq), outboxJobId: Number(replay.jobId), replayed: true };
    }

    const table = target.kind === "channel" ? "memberships" : "conversation_memberships";
    const column = target.kind === "channel" ? "channel_id" : "conversation_id";
    await this.#session.execute(
      `SELECT actor_id FROM ${table} WHERE ${column} = ${sqlLiteral(target.ownerId)}
        AND actor_kind = 'human' AND state = 'active' ORDER BY actor_id FOR UPDATE;`,
    );
    const allocated = await this.#session.queryJson<{ targetSeq: number }>(
      `WITH allocated AS (
        INSERT INTO target_sequences(target_kind, target_id, thread_root_message_id, next_seq)
        VALUES (${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)}, 2)
        ON CONFLICT (target_kind, target_id, thread_root_message_id)
        DO UPDATE SET next_seq = target_sequences.next_seq + 1
        RETURNING next_seq - 1 AS target_seq
      ) SELECT json_build_object('targetSeq', target_seq) FROM allocated;`,
    );
    await this.#session.execute(
      `INSERT INTO messages(
        message_id, target_kind, target_id, thread_root_message_id, author_kind, author_id,
        target_seq, body, parent_message_id, producer_fact_id, payload_digest,
        caused_by_producer_fact_id
      ) VALUES (
        ${sqlLiteral(input.messageId)}, ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)},
        ${sqlLiteral(target.threadRootMessageId)}, 'agent', ${sqlLiteral(input.agentId)},
        ${sqlLiteral(allocated.targetSeq)}, ${sqlLiteral(input.body)}, ${sqlLiteral(input.incomingMessageId)},
        ${sqlLiteral(input.producerFactId)}, ${sqlLiteral(input.requestDigest)},
        ${sqlLiteral(input.incomingProducerFactId)}
      );
      INSERT INTO message_audience(message_id, actor_kind, actor_id, membership_epoch, audience_mode)
      SELECT ${sqlLiteral(input.messageId)}, 'human', actor_id, membership_epoch, 'member_body'
      FROM ${table} WHERE ${column} = ${sqlLiteral(target.ownerId)}
        AND actor_kind = 'human' AND state = 'active';`,
    );
    const outboxPayload = {
      protocolVersion: 1, messageId: input.messageId, producerFactId: input.producerFactId,
      target: input.target, serverSeq: allocated.targetSeq, audienceAuthority: "message_audience",
    };
    const outbox = await this.#session.queryJson<{ jobId: number }>(
      `WITH inserted AS (
        INSERT INTO outbox_jobs(
          idempotency_namespace, idempotency_key, producer_fact_id,
          event_kind, event_version, payload_json, status
        ) VALUES (
          'client_message.v1', ${sqlLiteral(input.producerFactId)}, ${sqlLiteral(input.producerFactId)},
          'client_message', 1, ${sqlLiteral(canonicalJson(outboxPayload))}::jsonb, 'pending'
        ) RETURNING job_id
      ) SELECT json_build_object('jobId', job_id) FROM inserted;`,
    );
    const receiptDetail = {
      protocolVersion: 1, kind: "reply_committed", incomingProducerFactId: input.incomingProducerFactId,
      replyMessageId: input.messageId, replyProducerFactId: input.producerFactId, turnId: input.turnId,
    };
    await this.#session.execute(
      `INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id,
        launch_id, state_instance_id, turn_id, session_id, occurred_at,
        detail_json, receipt_digest, effect_kind, effect_message_id
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(input.incomingProducerFactId)},
        'side_effect_applied', ${sqlLiteral(input.machineId)}, ${sqlLiteral(input.agentId)},
        ${sqlLiteral(input.launchId)}, ${sqlLiteral(input.stateInstanceId)}, ${sqlLiteral(input.turnId)},
        ${sqlLiteral(input.sessionId)}, ${sqlLiteral(input.occurredAt)}::timestamptz,
        ${sqlLiteral(canonicalJson(receiptDetail))}::jsonb, ${sqlLiteral(canonicalDigest(receiptDetail))},
        'reply_committed', ${sqlLiteral(input.messageId)}
      );`,
    );
    return { messageId: input.messageId, targetSeq: allocated.targetSeq, outboxJobId: outbox.jobId, replayed: false };
  }

  async createTask(input: CreateTaskInput): Promise<{ replayed: boolean; taskId: string; result: Record<string, unknown> }> {
    assertProtocolId(input.receiptId, "rcp");
    assertProtocolId(input.machineId, "mch");
    assertProtocolId(input.agentId, "agt");
    assertProtocolId(input.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    if (!Number.isSafeInteger(input.leaseEpoch) || input.leaseEpoch < 1) {
      storageFail("INVALID_IDENTIFIER", input.leaseEpoch);
    }
    assertFenceToken(input.fenceToken);
    const recorded = await new TaskCommandRepository(this.#session).record(input);
    const predecessorFence = await this.#session.queryJson<{ valid: boolean }>(
      `SELECT json_build_object('valid', EXISTS (
        SELECT 1 FROM receipts WHERE producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND turn_id = ${sqlLiteral(input.turnId)} AND effect_kind = 'reply_committed'
          AND actor_machine_id = ${sqlLiteral(input.machineId)}
          AND actor_agent_id = ${sqlLiteral(input.agentId)}
          AND launch_id = ${sqlLiteral(input.launchId)}
          AND state_instance_id = ${sqlLiteral(input.stateInstanceId)}
          AND session_id = ${sqlLiteral(input.sessionId)}
        FOR UPDATE
      ));`,
    );
    if (!predecessorFence.valid) storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.turnId);
    const existing = await this.#session.queryJson<{ found: boolean; taskId?: string }>(
      `SELECT json_build_object('found', EXISTS (
        SELECT 1 FROM receipts WHERE producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND turn_id = ${sqlLiteral(input.turnId)} AND effect_kind = 'task_created'
          AND effect_task_id = ${sqlLiteral(recorded.taskId)}
      ), 'taskId', (SELECT effect_task_id FROM receipts
        WHERE producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND turn_id = ${sqlLiteral(input.turnId)} AND effect_kind = 'task_created'));`,
    );
    if (existing.found) {
      if (existing.taskId !== recorded.taskId) storageFail("SECOND_COORDINATION_CALL", input.turnId);
      return { ...recorded, replayed: true };
    }
    const detail = {
      protocolVersion: 1, kind: "task_created", taskId: recorded.taskId,
      incomingProducerFactId: input.incomingProducerFactId, turnId: input.turnId,
    };
    await this.#session.execute(
      `INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id,
        lease_epoch, fence_token, launch_id, state_instance_id, turn_id, session_id, occurred_at,
        detail_json, receipt_digest, effect_kind, effect_task_id
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(input.incomingProducerFactId)},
        'side_effect_applied', ${sqlLiteral(input.machineId)}, ${sqlLiteral(input.agentId)},
        ${sqlLiteral(input.leaseEpoch)}, ${sqlLiteral(input.fenceToken)},
        ${sqlLiteral(input.launchId)}, ${sqlLiteral(input.stateInstanceId)}, ${sqlLiteral(input.turnId)},
        ${sqlLiteral(input.sessionId)}, ${sqlLiteral(input.occurredAt)}::timestamptz,
        ${sqlLiteral(canonicalJson(detail))}::jsonb, ${sqlLiteral(canonicalDigest(detail))},
        'task_created', ${sqlLiteral(recorded.taskId)}
      );`,
    );
    return recorded;
  }
}
