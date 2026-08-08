import {
  canonicalProtocolJson,
  parseAcquireConsumePermit,
  parseReconcileDeliveryAttempt,
  parseReconcileDeliveryResult,
  parseResumeConsumePermit,
  type AcquireConsumePermit,
  type ArtifactDigest,
  type CommandId,
  type ReconcileDeliveryAttempt,
  type ReconcileDeliveryResult,
  type ResumeConsumePermit,
  type ProtocolVersion,
} from "@swarm/protocol";
import { storageFail, type StorageErrorCode } from "../errors.js";
import { assertArtifactDigest, assertProtocolId } from "../protocol.js";
import { PsqlSession, sqlLiteral } from "./session.js";
import { NativeIngressRepository } from "./wave1.js";

type PermitCommand = AcquireConsumePermit | ResumeConsumePermit;

export type PermitMutationInput = {
  command: PermitCommand;
  commandKind: "acquire" | "resume_same" | "resume_next";
  permitId: CommandId;
  resultInvocationGeneration: number;
  resultInvocationId: CommandId;
  resultWithoutBody: Record<string, unknown>;
  createdFromProofDigest?: ArtifactDigest;
};

type DeliveryAuthority = {
  found: boolean;
  messageId?: string;
  producerFactId?: string;
  targetKind?: "channel" | "direct";
  targetId?: string;
  threadRootMessageId?: string | null;
  targetSeq?: number;
  agentId?: string;
  machineId?: string;
  launchId?: string;
  membershipEpoch?: number;
  routingGeneration?: number;
  routeVersion?: number;
  outboxJobId?: number;
  deliveryStatus?: string;
  jobStatus?: string;
};

export type PermitBodyResult =
  | {
      kind: "authorized";
      replayed: boolean;
      invocationGeneration: number;
      invocationId: string;
      body: string;
    }
  | {
      kind: "suppressed";
      code: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME";
    };

type PermitSuppressionCode =
  | "MEMBERSHIP_REVOKED_BEFORE_CONSUME"
  | "ROUTE_SUPERSEDED_BEFORE_CONSUME";

export class ServerDeliveryRepository {
  readonly #session: PsqlSession;
  readonly #native: NativeIngressRepository;

  constructor(session: PsqlSession) {
    this.#session = session;
    this.#native = new NativeIngressRepository(session);
  }

  async lease(input: {
    jobId: number;
    workerLeaseId: string;
    leaseUntil: string;
    nextDeliveryId?: string;
  }): Promise<{
    jobId: number;
    jobAttempt: number;
    deliveryId: string;
    attempt: number;
    replayOf?: string;
    messageId: string;
    producerFactId: string;
    targetKind: "channel" | "direct";
    targetId: string;
    threadRootMessageId: string | null;
    serverSeq: number;
    agentId: string;
    machineId: string;
    expectedLaunchId: string;
    membershipEpoch: number;
    routingGeneration: number;
    routeVersion: number;
  }> {
    if (!Number.isSafeInteger(input.jobId) || input.jobId < 1) storageFail("INVALID_IDENTIFIER", input.jobId);
    assertProtocolId(input.workerLeaseId, "lse");
    if (input.nextDeliveryId !== undefined) assertProtocolId(input.nextDeliveryId, "dlv");
    const job = await this.#session.queryJson<{
      found: boolean; status?: string; eventKind?: string; jobAttempt?: number;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'status', status, 'eventKind', event_kind, 'jobAttempt', attempt
      ) FROM outbox_jobs WHERE job_id = ${sqlLiteral(input.jobId)}
        AND due_at <= clock_timestamp() FOR UPDATE),
      json_build_object('found', false));`,
    );
    if (!job.found || job.status !== "pending" || job.eventKind !== "message_delivery") {
      storageFail("OUTBOX_STALE_ATTEMPT", input.jobId);
    }
    let latest = await this.#session.queryJson<DeliveryAuthority & {
      status?: string; deliveryId?: string; attempt?: number; replayOf?: string | null;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'status', status, 'deliveryId', delivery_id, 'attempt', attempt,
        'replayOf', replay_of, 'messageId', message_id, 'producerFactId', producer_fact_id,
        'targetKind', target_kind, 'targetId', target_id, 'threadRootMessageId', thread_root_message_id,
        'targetSeq', target_seq, 'agentId', agent_id, 'machineId', machine_id,
        'launchId', expected_launch_id, 'membershipEpoch', membership_epoch,
        'routingGeneration', routing_generation, 'routeVersion', route_version, 'outboxJobId', outbox_job_id
      ) FROM deliveries WHERE outbox_job_id = ${sqlLiteral(input.jobId)}
        ORDER BY attempt DESC LIMIT 1 FOR UPDATE), json_build_object('found', false));`,
    );
    if (!latest.found) storageFail("STALE_DELIVERY_FENCE", input.jobId);
    if (latest.status === "canceled") {
      const nextDeliveryId = input.nextDeliveryId ?? storageFail("INVALID_IDENTIFIER", "next delivery id");
      const nextAttempt = Number(latest.attempt) + 1;
      await this.#session.execute(
        `INSERT INTO deliveries(
          delivery_id, attempt, message_id, target_kind, target_id, thread_root_message_id,
          target_seq, producer_fact_id, agent_id, machine_id, expected_launch_id,
          replay_of, status, outbox_job_id, membership_epoch, routing_generation, route_version
        ) VALUES (
          ${sqlLiteral(nextDeliveryId)}, ${sqlLiteral(nextAttempt)}, ${sqlLiteral(latest.messageId ?? null)},
          ${sqlLiteral(latest.targetKind ?? null)}, ${sqlLiteral(latest.targetId ?? null)},
          ${sqlLiteral(latest.threadRootMessageId ?? null)}, ${sqlLiteral(latest.targetSeq ?? null)},
          ${sqlLiteral(latest.producerFactId ?? null)}, ${sqlLiteral(latest.agentId ?? null)},
          ${sqlLiteral(latest.machineId ?? null)}, ${sqlLiteral(latest.launchId ?? null)},
          ${sqlLiteral(latest.deliveryId ?? null)}, 'pending', ${sqlLiteral(input.jobId)},
          ${sqlLiteral(latest.membershipEpoch ?? null)}, ${sqlLiteral(latest.routingGeneration ?? null)},
          ${sqlLiteral(latest.routeVersion ?? null)}
        );`,
      );
      latest = {
        ...latest,
        deliveryId: nextDeliveryId,
        attempt: nextAttempt,
        ...(latest.deliveryId === undefined ? {} : { replayOf: latest.deliveryId }),
        status: "pending",
      };
    }
    if (latest.status !== "pending") storageFail("OUTBOX_STALE_ATTEMPT", input.jobId);
    const changed = await this.#session.queryJson<{ jobAttempt: number; deliveryChanged: number }>(
      `WITH job AS (
        UPDATE outbox_jobs SET status = 'leased', attempt = attempt + 1,
          worker_lease_id = ${sqlLiteral(input.workerLeaseId)},
          worker_lease_until = ${sqlLiteral(input.leaseUntil)}::timestamptz,
          updated_at = clock_timestamp()
        WHERE job_id = ${sqlLiteral(input.jobId)} AND status = 'pending'
          AND ${sqlLiteral(input.leaseUntil)}::timestamptz > clock_timestamp()
        RETURNING attempt
      ), delivery AS (
        UPDATE deliveries SET status = 'leased', worker_lease_id = ${sqlLiteral(input.workerLeaseId)},
          worker_lease_until = ${sqlLiteral(input.leaseUntil)}::timestamptz
        WHERE delivery_id = ${sqlLiteral(latest.deliveryId ?? null)}
          AND attempt = ${sqlLiteral(latest.attempt ?? null)} AND status = 'pending'
          AND ${sqlLiteral(input.leaseUntil)}::timestamptz > clock_timestamp()
        RETURNING 1
      ) SELECT json_build_object(
        'jobAttempt', coalesce((SELECT attempt FROM job), 0),
        'deliveryChanged', (SELECT count(*) FROM delivery)
      );`,
    );
    if (Number(changed.jobAttempt) < 1 || Number(changed.deliveryChanged) !== 1) {
      storageFail("OUTBOX_STALE_ATTEMPT", input.jobId);
    }
    return {
      jobId: input.jobId,
      jobAttempt: Number(changed.jobAttempt),
      deliveryId: latest.deliveryId ?? storageFail("STALE_DELIVERY_FENCE"),
      attempt: Number(latest.attempt),
      ...(latest.replayOf ? { replayOf: latest.replayOf } : {}),
      messageId: latest.messageId ?? storageFail("STALE_DELIVERY_FENCE"),
      producerFactId: latest.producerFactId ?? storageFail("STALE_DELIVERY_FENCE"),
      targetKind: latest.targetKind ?? storageFail("STALE_DELIVERY_FENCE"),
      targetId: latest.targetId ?? storageFail("STALE_DELIVERY_FENCE"),
      threadRootMessageId: latest.threadRootMessageId ?? null,
      serverSeq: Number(latest.targetSeq),
      agentId: latest.agentId ?? storageFail("STALE_DELIVERY_FENCE"),
      machineId: latest.machineId ?? storageFail("STALE_DELIVERY_FENCE"),
      expectedLaunchId: latest.launchId ?? storageFail("STALE_DELIVERY_FENCE"),
      membershipEpoch: Number(latest.membershipEpoch),
      routingGeneration: Number(latest.routingGeneration),
      routeVersion: Number(latest.routeVersion),
    };
  }

  async reconcilePrePermit(
    command: ReconcileDeliveryAttempt,
    result: ReconcileDeliveryResult,
  ): Promise<{ replayed: boolean; result: ReconcileDeliveryResult }> {
    command = parseReconcileDeliveryAttempt(canonicalProtocolJson(command), 1 as ProtocolVersion);
    result = parseReconcileDeliveryResult(canonicalProtocolJson(result));
    assertProtocolId(command.commandId, "cmd");
    assertArtifactDigest(command.requestDigest);
    assertArtifactDigest(command.evidenceDigest);
    const existing = await this.#session.queryJson<{
      found: boolean; requestDigest?: string; result?: ReconcileDeliveryResult;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', request_digest, 'result', result_json
      ) FROM delivery_reconciliation_commands WHERE command_id = ${sqlLiteral(command.commandId)}),
      json_build_object('found', false));`,
    );
    if (existing.found) {
      if (existing.requestDigest !== command.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", command.commandId);
      return { replayed: true, result: existing.result ?? storageFail("IDEMPOTENCY_CONFLICT") };
    }
    if (
      command.evidence.kind !== "pre_permit_disconnect" || command.permitId !== null
      || command.invocation !== null || result.kind !== "pre_permit_requeued"
      || result.replayOfAttempt !== command.attempt || result.nextAttempt !== command.attempt + 1
      || result.jobState !== "pending"
    ) storageFail("RECONCILIATION_STATE_CONFLICT", command.commandId);
    const changed = await this.#session.queryJson<{ deliveryChanged: number; jobChanged: number }>(
      `WITH delivery AS (
        UPDATE deliveries SET status = 'canceled', worker_lease_id = NULL, worker_lease_until = NULL
        WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
          AND producer_fact_id = ${sqlLiteral(command.producerFactId)}
          AND agent_id = ${sqlLiteral(command.agentId)} AND machine_id = ${sqlLiteral(command.machineId)}
          AND expected_launch_id = ${sqlLiteral(command.launchId)}
          AND membership_epoch = ${sqlLiteral(command.membershipEpoch)}
          AND routing_generation = ${sqlLiteral(command.routingGeneration)}
          AND route_version = ${sqlLiteral(command.routeVersion)}
          AND status = 'leased' AND consume_permit_id IS NULL
        RETURNING outbox_job_id
      ), job AS (
        UPDATE outbox_jobs SET status = 'pending', worker_lease_id = NULL, worker_lease_until = NULL,
          due_at = clock_timestamp(), last_error = 'PRE_PERMIT_DISCONNECT', updated_at = clock_timestamp()
        WHERE job_id = (SELECT outbox_job_id FROM delivery) AND status = 'leased'
        RETURNING 1
      ) SELECT json_build_object(
        'deliveryChanged', (SELECT count(*) FROM delivery), 'jobChanged', (SELECT count(*) FROM job)
      );`,
    );
    if (Number(changed.deliveryChanged) !== 1 || Number(changed.jobChanged) !== 1) {
      storageFail("RECONCILIATION_STATE_CONFLICT", command.commandId);
    }
    await this.#session.execute(
      `INSERT INTO delivery_reconciliation_commands(
        command_id, request_digest, delivery_id, attempt, permit_id,
        invocation_generation, invocation_id, evidence_kind, evidence_digest, result_json
      ) VALUES (
        ${sqlLiteral(command.commandId)}, ${sqlLiteral(command.requestDigest)},
        ${sqlLiteral(command.deliveryId)}, ${sqlLiteral(command.attempt)}, NULL, NULL, NULL,
        'pre_permit_disconnect', ${sqlLiteral(command.evidenceDigest)},
        ${sqlLiteral(JSON.stringify(result))}::jsonb
      );`,
    );
    return { replayed: false, result };
  }

  async acquireOrResume(input: PermitMutationInput): Promise<PermitBodyResult> {
    const command = "resumeMode" in input.command
      ? parseResumeConsumePermit(canonicalProtocolJson(input.command), 1 as ProtocolVersion)
      : parseAcquireConsumePermit(canonicalProtocolJson(input.command), 1 as ProtocolVersion);
    const normalized = { ...input, command };
    const replayCandidate = await this.#session.queryJson<{ found: boolean }>(
      `SELECT json_build_object('found', EXISTS (
        SELECT 1 FROM delivery_permit_commands WHERE command_id = ${sqlLiteral(command.commandId)}
      ));`,
    );
    const suppression = await this.#authorize(command, replayCandidate.found);
    if (suppression !== null) {
      await this.#suppress(command, suppression);
      return { kind: "suppressed", code: suppression };
    }
    const recorded = await this.#native.recordPermit(normalized);
    const body = await this.#session.queryJson<{ body: string } | null>(
      `SELECT coalesce((SELECT json_build_object('body', m.body)
        FROM deliveries d
        JOIN delivery_invocations i ON i.delivery_id = d.delivery_id AND i.attempt = d.attempt
          AND i.invocation_generation = d.active_invocation_generation
        JOIN messages m ON m.message_id = d.message_id
        JOIN message_audience a ON a.message_id = m.message_id AND a.actor_kind = 'agent'
          AND a.actor_id = d.agent_id AND a.membership_epoch = d.membership_epoch
          AND a.audience_mode = 'owner_body'
        WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)}
          AND d.attempt = ${sqlLiteral(command.attempt)}
          AND d.active_invocation_generation = ${sqlLiteral(recorded.invocationGeneration)}
          AND i.invocation_id = ${sqlLiteral(recorded.invocationId)} AND i.status = 'authorized'
        FOR UPDATE OF d, i, a, m), 'null'::json);`,
    );
    if (body === null) storageFail("STALE_INVOCATION_GENERATION", command.deliveryId);
    return {
      kind: "authorized",
      replayed: recorded.replayed,
      invocationGeneration: recorded.invocationGeneration,
      invocationId: recorded.invocationId,
      body: body.body,
    };
  }

  async #authorize(command: PermitCommand, replayCandidate: boolean): Promise<PermitSuppressionCode | null> {
    const delivery = await this.#session.queryJson<DeliveryAuthority>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'messageId', d.message_id, 'producerFactId', d.producer_fact_id,
        'targetKind', d.target_kind, 'targetId', d.target_id, 'threadRootMessageId', d.thread_root_message_id,
        'targetSeq', d.target_seq, 'agentId', d.agent_id, 'machineId', d.machine_id,
        'launchId', d.expected_launch_id, 'membershipEpoch', d.membership_epoch,
        'routingGeneration', d.routing_generation, 'routeVersion', d.route_version,
        'outboxJobId', d.outbox_job_id, 'deliveryStatus', d.status, 'jobStatus', j.status
      ) FROM deliveries d JOIN outbox_jobs j ON j.job_id = d.outbox_job_id
      WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)}
        AND d.attempt = ${sqlLiteral(command.attempt)}), json_build_object('found', false));`,
    );
    if (
      !delivery.found || delivery.producerFactId !== command.producerFactId
      || delivery.agentId !== command.agentId || delivery.machineId !== command.machineId
      || delivery.launchId !== command.launchId
      || Number(delivery.membershipEpoch) !== command.membershipEpoch
      || Number(delivery.routingGeneration) !== command.routingGeneration
      || Number(delivery.routeVersion) !== command.routeVersion
    ) storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
    const table = delivery.targetKind === "channel" ? "memberships" : "conversation_memberships";
    const column = delivery.targetKind === "channel" ? "channel_id" : "conversation_id";
    const member = await this.#session.queryJson<{ valid: boolean }>(
      `SELECT json_build_object('valid', EXISTS (
        SELECT 1 FROM ${table} WHERE ${column} = ${sqlLiteral(delivery.targetId ?? null)}
          AND actor_kind = 'agent' AND actor_id = ${sqlLiteral(command.agentId)}
          AND state = 'active' AND membership_epoch = ${sqlLiteral(command.membershipEpoch)}
        FOR UPDATE
      ));`,
    );
    if (!member.valid) return "MEMBERSHIP_REVOKED_BEFORE_CONSUME";
    const route = await this.#session.queryJson<{ valid: boolean }>(
      `SELECT json_build_object('valid', EXISTS (
        SELECT 1 FROM target_owner_routes r JOIN agent_launches l
          ON l.launch_id = r.expected_launch_id AND l.machine_id = r.machine_id AND l.agent_id = r.agent_id
        WHERE r.target_kind = ${sqlLiteral(delivery.targetKind ?? null)}
          AND r.target_id = ${sqlLiteral(delivery.targetId ?? null)}
          AND r.thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(delivery.threadRootMessageId ?? null)}
          AND r.agent_id = ${sqlLiteral(command.agentId)} AND r.machine_id = ${sqlLiteral(command.machineId)}
          AND r.expected_launch_id = ${sqlLiteral(command.launchId)}
          AND r.membership_epoch = ${sqlLiteral(command.membershipEpoch)}
          AND r.routing_generation = ${sqlLiteral(command.routingGeneration)}
          AND r.route_version = ${sqlLiteral(command.routeVersion)}
          AND l.state = 'activated' AND l.routing_generation = ${sqlLiteral(command.routingGeneration)}
        FOR UPDATE OF r, l
      ));`,
    );
    if (!route.valid) return "ROUTE_SUPERSEDED_BEFORE_CONSUME";
    const audience = await this.#session.queryJson<{ valid: boolean }>(
      `SELECT json_build_object('valid', EXISTS (
        SELECT 1 FROM message_audience WHERE message_id = ${sqlLiteral(delivery.messageId ?? null)}
          AND actor_kind = 'agent' AND actor_id = ${sqlLiteral(command.agentId)}
          AND membership_epoch = ${sqlLiteral(command.membershipEpoch)} AND audience_mode = 'owner_body'
        FOR UPDATE
      ));`,
    );
    if (!audience.valid) return "MEMBERSHIP_REVOKED_BEFORE_CONSUME";
    const locked = await this.#session.queryJson<{ valid: boolean; deliveryStatus?: string; jobStatus?: string }>(
      `SELECT coalesce((SELECT json_build_object(
        'valid', true, 'deliveryStatus', d.status, 'jobStatus', j.status
      ) FROM deliveries d JOIN outbox_jobs j ON j.job_id = d.outbox_job_id
      WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)} AND d.attempt = ${sqlLiteral(command.attempt)}
        AND d.producer_fact_id = ${sqlLiteral(command.producerFactId)}
        AND d.agent_id = ${sqlLiteral(command.agentId)} AND d.machine_id = ${sqlLiteral(command.machineId)}
        AND d.expected_launch_id = ${sqlLiteral(command.launchId)}
        AND d.membership_epoch = ${sqlLiteral(command.membershipEpoch)}
        AND d.routing_generation = ${sqlLiteral(command.routingGeneration)}
        AND d.route_version = ${sqlLiteral(command.routeVersion)}
      FOR UPDATE OF d, j), json_build_object('valid', false));`,
    );
    if (!locked.valid) storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
    if (
      !("resumeMode" in command) && !replayCandidate
      && (locked.deliveryStatus !== "leased" || locked.jobStatus !== "leased")
    ) storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
    return null;
  }

  async #suppress(command: PermitCommand, code: StorageErrorCode): Promise<void> {
    await this.#session.execute(
      `WITH delivery AS (
        UPDATE deliveries SET status = 'dead', worker_lease_id = NULL, worker_lease_until = NULL
        WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
          AND status IN ('pending', 'leased', 'daemon_accepted')
        RETURNING outbox_job_id
      ) UPDATE outbox_jobs SET status = 'dead', hold_reason = NULL,
        worker_lease_id = NULL, worker_lease_until = NULL, last_error = ${sqlLiteral(code)},
        updated_at = clock_timestamp()
      WHERE job_id = (SELECT outbox_job_id FROM delivery);`,
    );
  }
}
