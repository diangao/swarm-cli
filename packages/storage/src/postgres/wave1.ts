import { createHash } from "node:crypto";

import {
  canonicalProtocolJson,
  messageBodyHasContent,
  parseDeliveryAckResult,
  type AcquireConsumePermit,
  type ArtifactDigest,
  type BeginNativeWrite,
  type CommandId,
  type DeliveryAck,
  type DeliveryAckResult,
  type ObservationCursorAck,
  type ReconcileDeliveryAttempt,
  type ReconcileDeliveryResult,
  type ResumeConsumePermit,
  type StateInstanceId,
  type Target,
} from "@swarm/protocol";
import { storageFail } from "../errors.js";
import {
  assertArtifactDigest,
  assertProtocolId,
  targetColumns,
} from "../protocol.js";
import { PsqlSession, sqlLiteral } from "./session.js";

function canonicalJson(value: unknown): string {
  return new TextDecoder().decode(canonicalProtocolJson(value));
}

function canonicalDigest(value: unknown): ArtifactDigest {
  const hash = createHash("sha256").update(canonicalProtocolJson(value)).digest("hex");
  return `sha256:${hash}` as ArtifactDigest;
}

function bytesDigest(value: Uint8Array): ArtifactDigest {
  return `sha256:${createHash("sha256").update(value).digest("hex")}` as ArtifactDigest;
}

function assertNonNegative(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) storageFail("INVALID_IDENTIFIER", label);
}

function actorId(value: string, kind: "human" | "agent"): string {
  return assertProtocolId(value, kind === "human" ? "hum" : "agt");
}

export class AgentRegistryRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async updateConfiguration(input: {
    agentId: string;
    configuration: Record<string, unknown>;
    expectedRowVersion: number;
    expectedRoutingGeneration: number;
    nextRoutingGeneration: number;
  }): Promise<number> {
    assertProtocolId(input.agentId, "agt");
    assertNonNegative(input.expectedRowVersion, "row version");
    assertNonNegative(input.expectedRoutingGeneration, "routing generation");
    if (input.nextRoutingGeneration < input.expectedRoutingGeneration) {
      storageFail("STALE_FENCE", input.agentId);
    }
    const configuration = canonicalJson(input.configuration);
    const result = await this.#session.queryJson<{ version: number } | null>(
      `WITH changed AS (
        UPDATE agents SET
          configuration = ${sqlLiteral(configuration)}::jsonb,
          routing_generation = ${sqlLiteral(input.nextRoutingGeneration)},
          row_version = row_version + 1
        WHERE agent_id = ${sqlLiteral(input.agentId)}
          AND row_version = ${sqlLiteral(input.expectedRowVersion)}
          AND routing_generation = ${sqlLiteral(input.expectedRoutingGeneration)}
        RETURNING row_version
      ) SELECT coalesce((SELECT json_build_object('version', row_version) FROM changed), 'null'::json);`,
    );
    if (result === null) storageFail("STALE_FENCE", input.agentId);
    return Number(result.version);
  }

  async heartbeat(input: {
    agentId: string;
    expectedRowVersion: number;
    expectedRoutingGeneration: number;
    presence: "offline" | "online" | "busy";
    occurredAt: string;
    lastTurnAt?: string;
  }): Promise<number> {
    assertProtocolId(input.agentId, "agt");
    assertNonNegative(input.expectedRowVersion, "row version");
    assertNonNegative(input.expectedRoutingGeneration, "routing generation");
    const result = await this.#session.queryJson<{ version: number } | null>(
      `WITH changed AS (
        UPDATE agents SET
          presence = ${sqlLiteral(input.presence)},
          presence_updated_at = ${sqlLiteral(input.occurredAt)}::timestamptz,
          last_turn_at = coalesce(${sqlLiteral(input.lastTurnAt ?? null)}::timestamptz, last_turn_at),
          last_activity_at = ${sqlLiteral(input.occurredAt)}::timestamptz,
          row_version = row_version + 1
        WHERE agent_id = ${sqlLiteral(input.agentId)}
          AND row_version = ${sqlLiteral(input.expectedRowVersion)}
          AND routing_generation = ${sqlLiteral(input.expectedRoutingGeneration)}
        RETURNING row_version
      ) SELECT coalesce((SELECT json_build_object('version', row_version) FROM changed), 'null'::json);`,
    );
    if (result === null) storageFail("STALE_FENCE", input.agentId);
    return Number(result.version);
  }
}

export class MembershipRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async mutate(input: {
    target: Target;
    actorKind: "human" | "agent";
    actorId: string;
    state: "active" | "removed";
    expectedEpoch: number;
    expectedRowVersion: number;
  }): Promise<{ membershipEpoch: number; rowVersion: number }> {
    const target = targetColumns(input.target);
    if (target.threadRootMessageId !== null) {
      storageFail("INVALID_IDENTIFIER", "thread membership is inherited from its base target");
    }
    const identifier = actorId(input.actorId, input.actorKind);
    assertNonNegative(input.expectedEpoch, "membership epoch");
    assertNonNegative(input.expectedRowVersion, "membership row version");
    const table = target.kind === "channel" ? "memberships" : "conversation_memberships";
    const targetColumn = target.kind === "channel" ? "channel_id" : "conversation_id";
    const result = await this.#session.queryJson<{ epoch: number; version: number } | null>(
      `WITH changed AS (
        INSERT INTO ${table}(
          ${targetColumn}, actor_kind, actor_id, state, membership_epoch, row_version
        ) SELECT
          ${sqlLiteral(target.ownerId)}, ${sqlLiteral(input.actorKind)}, ${sqlLiteral(identifier)},
          ${sqlLiteral(input.state)}, 1, 1
        WHERE ${sqlLiteral(input.expectedEpoch)} = 0 AND ${sqlLiteral(input.expectedRowVersion)} = 0
        ON CONFLICT (${targetColumn}, actor_kind, actor_id) DO UPDATE SET
          state = excluded.state,
          membership_epoch = ${table}.membership_epoch + 1,
          row_version = ${table}.row_version + 1,
          updated_at = clock_timestamp()
        WHERE ${table}.membership_epoch = ${sqlLiteral(input.expectedEpoch)}
          AND ${table}.row_version = ${sqlLiteral(input.expectedRowVersion)}
          AND ${table}.state <> excluded.state
        RETURNING membership_epoch, row_version
      ) SELECT coalesce((SELECT json_build_object(
        'epoch', membership_epoch, 'version', row_version
      ) FROM changed), 'null'::json);`,
    );
    if (result === null) storageFail("STALE_FENCE", identifier);
    return { membershipEpoch: Number(result.epoch), rowVersion: Number(result.version) };
  }
}

export class RouteRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async compareAndSet(input: {
    target: Target;
    agentId: string;
    machineId: string;
    expectedLaunchId: string;
    membershipEpoch: number;
    routingGeneration: number;
    expectedRouteVersion: number;
  }): Promise<number> {
    const target = targetColumns(input.target);
    assertProtocolId(input.agentId, "agt");
    assertProtocolId(input.machineId, "mch");
    assertProtocolId(input.expectedLaunchId, "lnc");
    if (!Number.isSafeInteger(input.membershipEpoch) || input.membershipEpoch < 1) {
      storageFail("INVALID_IDENTIFIER", "membership epoch");
    }
    assertNonNegative(input.routingGeneration, "routing generation");
    assertNonNegative(input.expectedRouteVersion, "route version");
    const nextVersion = input.expectedRouteVersion + 1;
    const membershipTable = target.kind === "channel" ? "memberships" : "conversation_memberships";
    const membershipTargetColumn = target.kind === "channel" ? "channel_id" : "conversation_id";
    const authority = `EXISTS (
      SELECT 1 FROM ${membershipTable} m
      WHERE m.${membershipTargetColumn} = ${sqlLiteral(target.ownerId)}
        AND m.actor_kind = 'agent' AND m.actor_id = ${sqlLiteral(input.agentId)}
        AND m.state = 'active' AND m.membership_epoch = ${sqlLiteral(input.membershipEpoch)}
    ) AND EXISTS (
      SELECT 1 FROM agent_launches l
      WHERE l.launch_id = ${sqlLiteral(input.expectedLaunchId)}
        AND l.machine_id = ${sqlLiteral(input.machineId)}
        AND l.agent_id = ${sqlLiteral(input.agentId)}
        AND l.state = 'activated'
        AND l.routing_generation = ${sqlLiteral(input.routingGeneration)}
    )`;
    const lockedAuthority = await this.#session.queryJson<{ valid: boolean }>(
      `WITH member AS (
        SELECT 1 FROM ${membershipTable} m
        WHERE m.${membershipTargetColumn} = ${sqlLiteral(target.ownerId)}
          AND m.actor_kind = 'agent' AND m.actor_id = ${sqlLiteral(input.agentId)}
          AND m.state = 'active' AND m.membership_epoch = ${sqlLiteral(input.membershipEpoch)}
        FOR UPDATE
      ), launch AS (
        SELECT 1 FROM agent_launches l
        WHERE l.launch_id = ${sqlLiteral(input.expectedLaunchId)}
          AND l.machine_id = ${sqlLiteral(input.machineId)}
          AND l.agent_id = ${sqlLiteral(input.agentId)}
          AND l.state = 'activated'
          AND l.routing_generation = ${sqlLiteral(input.routingGeneration)}
        FOR UPDATE
      ) SELECT json_build_object(
        'valid', EXISTS (SELECT 1 FROM member) AND EXISTS (SELECT 1 FROM launch)
      );`,
    );
    if (!lockedAuthority.valid) storageFail("ROUTE_SUPERSEDED_BEFORE_CONSUME", input.agentId);
    const result = await this.#session.queryJson<{ routeVersion: number } | null>(
      `WITH changed AS (
        INSERT INTO target_owner_routes(
          target_kind, target_id, thread_root_message_id, agent_id, machine_id,
          expected_launch_id, membership_epoch, routing_generation, route_version, row_version
        ) SELECT
          ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
          ${sqlLiteral(input.agentId)}, ${sqlLiteral(input.machineId)}, ${sqlLiteral(input.expectedLaunchId)},
          ${sqlLiteral(input.membershipEpoch)}, ${sqlLiteral(input.routingGeneration)}, 1, 1
        WHERE ${sqlLiteral(input.expectedRouteVersion)} = 0 AND ${authority}
        ON CONFLICT (target_kind, target_id, thread_root_message_id) DO UPDATE SET
          agent_id = excluded.agent_id, machine_id = excluded.machine_id,
          expected_launch_id = excluded.expected_launch_id,
          membership_epoch = excluded.membership_epoch,
          routing_generation = excluded.routing_generation,
          route_version = target_owner_routes.route_version + 1,
          row_version = target_owner_routes.row_version + 1,
          updated_at = clock_timestamp()
        WHERE target_owner_routes.route_version = ${sqlLiteral(input.expectedRouteVersion)}
          AND ${authority}
        RETURNING route_version
      ) SELECT coalesce((SELECT json_build_object('routeVersion', route_version) FROM changed), 'null'::json);`,
    );
    if (result === null || Number(result.routeVersion) !== nextVersion) {
      storageFail("STALE_FENCE", "route version");
    }
    return Number(result.routeVersion);
  }
}

export class ObservationCursorRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async acknowledge(ack: ObservationCursorAck): Promise<number> {
    const target = targetColumns(ack.target);
    const rawActor = String(ack.actorId);
    const kind = rawActor.startsWith("hum_") ? "human" : "agent";
    actorId(rawActor, kind);
    const result = await this.#session.queryJson<{ sequence: number } | null>(
      `WITH changed AS (
        INSERT INTO observation_cursors(
          actor_kind, actor_id, stream, target_kind, target_id,
          thread_root_message_id, membership_epoch, server_seq
        ) VALUES (
          ${sqlLiteral(kind)}, ${sqlLiteral(rawActor)}, ${sqlLiteral(ack.stream)},
          ${sqlLiteral(target.kind)}, ${sqlLiteral(target.ownerId)}, ${sqlLiteral(target.threadRootMessageId)},
          ${sqlLiteral(ack.membershipEpoch)}, ${sqlLiteral(ack.serverSeq)}
        )
        ON CONFLICT (actor_kind, actor_id, stream, target_kind, target_id, thread_root_message_id, membership_epoch)
        DO UPDATE SET server_seq = excluded.server_seq, updated_at = clock_timestamp()
        WHERE observation_cursors.server_seq <= excluded.server_seq
        RETURNING server_seq
      ) SELECT coalesce((SELECT json_build_object('sequence', server_seq) FROM changed), 'null'::json);`,
    );
    if (result === null) storageFail("BOUNDARY_REGRESSION", rawActor);
    return Number(result.sequence);
  }
}

type PermitCommand = AcquireConsumePermit | ResumeConsumePermit;

export class NativeIngressRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async #lockCurrentPermitAuthority(command: PermitCommand): Promise<void> {
    const located = await this.#session.queryJson<{
      found: boolean;
      targetKind?: "channel" | "direct";
      targetId?: string;
      threadRootMessageId?: string | null;
      messageId?: string;
      producerFactId?: string;
      agentId?: string;
      machineId?: string;
      launchId?: string;
      membershipEpoch?: number;
      routingGeneration?: number;
      routeVersion?: number;
      outboxJobId?: number | null;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'targetKind', target_kind, 'targetId', target_id,
        'threadRootMessageId', thread_root_message_id, 'messageId', message_id,
        'producerFactId', producer_fact_id, 'agentId', agent_id,
        'machineId', machine_id, 'launchId', expected_launch_id,
        'membershipEpoch', membership_epoch, 'routingGeneration', routing_generation,
        'routeVersion', route_version, 'outboxJobId', outbox_job_id
      ) FROM deliveries WHERE delivery_id = ${sqlLiteral(command.deliveryId)}
        AND attempt = ${sqlLiteral(command.attempt)}), json_build_object('found', false));`,
    );
    if (
      !located.found
      || located.producerFactId !== command.producerFactId
      || located.agentId !== command.agentId
      || located.machineId !== command.machineId
      || located.launchId !== command.launchId
      || Number(located.membershipEpoch) !== command.membershipEpoch
      || Number(located.routingGeneration) !== command.routingGeneration
      || Number(located.routeVersion) !== command.routeVersion
    ) {
      storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
    }
    const targetKind = located.targetKind ?? storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
    const targetId = located.targetId ?? storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
    const messageId = located.messageId ?? storageFail("STALE_DELIVERY_FENCE", command.deliveryId);

    const membershipTable = targetKind === "channel" ? "memberships" : "conversation_memberships";
    const membershipTargetColumn = targetKind === "channel" ? "channel_id" : "conversation_id";
    const membership = await this.#session.queryJson<{ found: boolean; state?: string; epoch?: number }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'state', state, 'epoch', membership_epoch
      ) FROM ${membershipTable}
      WHERE ${membershipTargetColumn} = ${sqlLiteral(targetId)}
        AND actor_kind = 'agent' AND actor_id = ${sqlLiteral(command.agentId)}
      FOR UPDATE), json_build_object('found', false));`,
    );
    if (
      !membership.found || membership.state !== "active"
      || Number(membership.epoch) !== command.membershipEpoch
    ) {
      storageFail("MEMBERSHIP_REVOKED_BEFORE_CONSUME", command.agentId);
    }

    const route = await this.#session.queryJson<{
      found: boolean;
      agentId?: string;
      machineId?: string;
      launchId?: string;
      membershipEpoch?: number;
      routingGeneration?: number;
      routeVersion?: number;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'agentId', agent_id, 'machineId', machine_id,
        'launchId', expected_launch_id, 'membershipEpoch', membership_epoch,
        'routingGeneration', routing_generation, 'routeVersion', route_version
      ) FROM target_owner_routes
      WHERE target_kind = ${sqlLiteral(targetKind)}
        AND target_id = ${sqlLiteral(targetId)}
        AND thread_root_message_id IS NOT DISTINCT FROM ${sqlLiteral(located.threadRootMessageId ?? null)}
      FOR UPDATE), json_build_object('found', false));`,
    );
    const launch = route.found
      ? await this.#session.queryJson<{ found: boolean; state?: string; routingGeneration?: number }>(
        `SELECT coalesce((SELECT json_build_object(
          'found', true, 'state', state, 'routingGeneration', routing_generation
        ) FROM agent_launches WHERE launch_id = ${sqlLiteral(route.launchId ?? null)}
          AND machine_id = ${sqlLiteral(route.machineId ?? null)} AND agent_id = ${sqlLiteral(route.agentId ?? null)}
        FOR UPDATE), json_build_object('found', false));`,
      )
      : { found: false };
    if (
      !route.found
      || route.agentId !== command.agentId
      || route.machineId !== command.machineId
      || route.launchId !== command.launchId
      || Number(route.membershipEpoch) !== command.membershipEpoch
      || Number(route.routingGeneration) !== command.routingGeneration
      || Number(route.routeVersion) !== command.routeVersion
      || !launch.found || launch.state !== "activated"
      || Number(launch.routingGeneration) !== command.routingGeneration
    ) {
      storageFail("ROUTE_SUPERSEDED_BEFORE_CONSUME", command.agentId);
    }

    const audience = await this.#session.queryJson<{
      found: boolean;
      membershipEpoch?: number;
      audienceMode?: string;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'membershipEpoch', membership_epoch, 'audienceMode', audience_mode
      ) FROM message_audience WHERE message_id = ${sqlLiteral(messageId)}
        AND actor_kind = 'agent' AND actor_id = ${sqlLiteral(command.agentId)}
      FOR UPDATE), json_build_object('found', false));`,
    );
    if (
      !audience.found || audience.audienceMode !== "owner_body"
      || Number(audience.membershipEpoch) !== command.membershipEpoch
    ) {
      storageFail("MEMBERSHIP_REVOKED_BEFORE_CONSUME", command.agentId);
    }

    if (located.outboxJobId !== null && located.outboxJobId !== undefined) {
      await this.#session.execute(
        `SELECT job_id FROM outbox_jobs WHERE job_id = ${sqlLiteral(located.outboxJobId)} FOR UPDATE;`,
      );
    }
    const lockedDelivery = await this.#session.queryJson<{ valid: boolean }>(
      `SELECT json_build_object('valid', EXISTS (
        SELECT 1 FROM deliveries WHERE delivery_id = ${sqlLiteral(command.deliveryId)}
          AND attempt = ${sqlLiteral(command.attempt)}
          AND producer_fact_id = ${sqlLiteral(command.producerFactId)}
          AND agent_id = ${sqlLiteral(command.agentId)}
          AND machine_id = ${sqlLiteral(command.machineId)}
          AND expected_launch_id = ${sqlLiteral(command.launchId)}
          AND membership_epoch = ${sqlLiteral(command.membershipEpoch)}
          AND routing_generation = ${sqlLiteral(command.routingGeneration)}
          AND route_version = ${sqlLiteral(command.routeVersion)}
        FOR UPDATE
      ));`,
    );
    if (!lockedDelivery.valid) storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
  }

  async recordPermit(input: {
    command: PermitCommand;
    commandKind: "acquire" | "resume_same" | "resume_next";
    permitId: CommandId;
    resultInvocationGeneration: number;
    resultInvocationId: CommandId;
    resultWithoutBody: Record<string, unknown>;
    createdFromProofDigest?: ArtifactDigest;
  }): Promise<{ replayed: boolean; invocationGeneration: number; invocationId: string }> {
    const command = input.command;
    const existing = await this.#session.queryJson<{
      found: boolean;
      requestDigest?: string;
      generation?: number;
      invocationId?: string;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', request_digest,
        'generation', result_invocation_generation, 'invocationId', result_invocation_id
      ) FROM delivery_permit_commands WHERE command_id = ${sqlLiteral(command.commandId)}),
      json_build_object('found', false));`,
    );
    if (existing.found) {
      if (existing.requestDigest !== command.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", command.commandId);
      return {
        replayed: true,
        invocationGeneration: Number(existing.generation),
        invocationId: existing.invocationId ?? storageFail("IDEMPOTENCY_CONFLICT", command.commandId),
      };
    }

    if (
      (input.commandKind === "acquire" && "resumeMode" in command)
      || (input.commandKind === "resume_same"
        && (!("resumeMode" in command) || command.resumeMode !== "same_invocation_before_begin"))
      || (input.commandKind === "resume_next"
        && (!("resumeMode" in command) || command.resumeMode !== "next_after_not_written"))
      || ("permitId" in command && command.permitId !== input.permitId)
    ) {
      storageFail("PERMIT_MISMATCH", command.commandId);
    }
    await this.#lockCurrentPermitAuthority(command);

    const expected = input.commandKind === "acquire"
      ? 0
      : "expectedActiveInvocationGeneration" in command
        ? command.expectedActiveInvocationGeneration
        : storageFail("PERMIT_MISMATCH", command.commandId);

    if (input.commandKind === "acquire") {
      const changed = await this.#session.queryJson<{ changed: number }>(
        `WITH changed AS (
          UPDATE deliveries SET consume_permit_id = ${sqlLiteral(input.permitId)},
            consume_permitted_at = clock_timestamp(), active_invocation_generation = 1,
            status = 'daemon_accepted', daemon_accepted_at = clock_timestamp()
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
            AND consume_permit_id IS NULL AND active_invocation_generation IS NULL
          RETURNING 1
        ) SELECT json_build_object('changed', count(*)) FROM changed;`,
      );
      if (Number(changed.changed) !== 1 || input.resultInvocationGeneration !== 1) {
        storageFail("STALE_DELIVERY_FENCE", command.deliveryId);
      }
      await this.#session.execute(
        `INSERT INTO delivery_invocations(
          delivery_id, attempt, invocation_generation, invocation_id, permit_id, status
        ) VALUES (
          ${sqlLiteral(command.deliveryId)}, ${sqlLiteral(command.attempt)}, 1,
          ${sqlLiteral(input.resultInvocationId)}, ${sqlLiteral(input.permitId)}, 'authorized'
        );
        UPDATE outbox_jobs SET status = 'held', hold_reason = 'CONSUME_PERMITTED',
          worker_lease_id = NULL, worker_lease_until = NULL, updated_at = clock_timestamp()
        WHERE job_id = (SELECT outbox_job_id FROM deliveries
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)});`,
      );
    } else if (input.commandKind === "resume_next") {
      const proofDigest = assertArtifactDigest(
        input.createdFromProofDigest ?? storageFail("FAKE_NOT_WRITTEN_PROOF_REQUIRED"),
      );
      if (input.resultInvocationGeneration !== expected + 1) storageFail("STALE_INVOCATION_GENERATION");
      await this.#session.execute(
        `INSERT INTO delivery_invocations(
          delivery_id, attempt, invocation_generation, invocation_id, permit_id,
          previous_invocation_generation, created_from_proof_digest, status
        ) SELECT
          ${sqlLiteral(command.deliveryId)}, ${sqlLiteral(command.attempt)},
          ${sqlLiteral(input.resultInvocationGeneration)}, ${sqlLiteral(input.resultInvocationId)},
          ${sqlLiteral(input.permitId)}, ${sqlLiteral(expected)}, ${sqlLiteral(proofDigest)},
          'authorized'
        FROM delivery_invocations AS prior
        WHERE prior.delivery_id = ${sqlLiteral(command.deliveryId)} AND prior.attempt = ${sqlLiteral(command.attempt)}
          AND prior.invocation_generation = ${sqlLiteral(expected)} AND prior.status = 'not_written'
          AND prior.not_written_proof_digest = ${sqlLiteral(proofDigest)};`,
      );
      const changed = await this.#session.queryJson<{ changed: number }>(
        `WITH changed AS (
          UPDATE deliveries SET active_invocation_generation = ${sqlLiteral(input.resultInvocationGeneration)}
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
            AND consume_permit_id = ${sqlLiteral(input.permitId)}
            AND active_invocation_generation = ${sqlLiteral(expected)}
            AND EXISTS (
              SELECT 1 FROM delivery_invocations WHERE delivery_id = ${sqlLiteral(command.deliveryId)}
                AND attempt = ${sqlLiteral(command.attempt)}
                AND invocation_generation = ${sqlLiteral(input.resultInvocationGeneration)}
            ) RETURNING 1
        ) SELECT json_build_object('changed', count(*)) FROM changed;`,
      );
      if (Number(changed.changed) !== 1) storageFail("STALE_INVOCATION_GENERATION", command.deliveryId);
    } else {
      const current = await this.#session.queryJson<{
        found: boolean;
        activeGeneration?: number;
        invocationId?: string;
        status?: string;
      }>(
        `WITH current AS (
          SELECT d.active_invocation_generation, i.invocation_id, i.status
          FROM deliveries d JOIN delivery_invocations i
            ON i.delivery_id = d.delivery_id AND i.attempt = d.attempt
            AND i.invocation_generation = d.active_invocation_generation
          WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)} AND d.attempt = ${sqlLiteral(command.attempt)}
            AND d.consume_permit_id = ${sqlLiteral(input.permitId)}
          FOR UPDATE OF d, i
        ) SELECT coalesce((SELECT json_build_object(
          'found', true, 'activeGeneration', active_invocation_generation,
          'invocationId', invocation_id, 'status', status
        ) FROM current), json_build_object('found', false));`,
      );
      if (
        !current.found
        || Number(current.activeGeneration) !== expected
        || input.resultInvocationGeneration !== expected
      ) {
        storageFail("STALE_INVOCATION_GENERATION", command.deliveryId);
      }
      if (current.invocationId !== input.resultInvocationId || current.status !== "authorized") {
        storageFail("INVOCATION_STATE_CONFLICT", command.deliveryId);
      }
    }

    await this.#session.execute(
      `INSERT INTO delivery_permit_commands(
        command_id, request_digest, command_kind, delivery_id, attempt, permit_id,
        expected_invocation_generation, result_invocation_generation, result_invocation_id,
        result_json_without_body
      ) VALUES (
        ${sqlLiteral(command.commandId)}, ${sqlLiteral(command.requestDigest)}, ${sqlLiteral(input.commandKind)},
        ${sqlLiteral(command.deliveryId)}, ${sqlLiteral(command.attempt)}, ${sqlLiteral(input.permitId)},
        ${sqlLiteral(expected)}, ${sqlLiteral(input.resultInvocationGeneration)},
        ${sqlLiteral(input.resultInvocationId)}, ${sqlLiteral(canonicalJson(input.resultWithoutBody))}::jsonb
      );`,
    );
    return {
      replayed: false,
      invocationGeneration: input.resultInvocationGeneration,
      invocationId: input.resultInvocationId,
    };
  }

  async beginWrite(command: BeginNativeWrite, result: Record<string, unknown>): Promise<{ replayed: boolean }> {
    const existing = await this.#session.queryJson<{ found: boolean; requestDigest?: string }>(
      `SELECT coalesce((SELECT json_build_object('found', true, 'requestDigest', begin_request_digest)
        FROM delivery_invocations WHERE begin_command_id = ${sqlLiteral(command.commandId)}),
        json_build_object('found', false));`,
    );
    if (existing.found) {
      if (existing.requestDigest !== command.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", command.commandId);
      return { replayed: true };
    }
    const changed = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE delivery_invocations SET
          status = 'write_started', begin_command_id = ${sqlLiteral(command.commandId)},
          begin_request_digest = ${sqlLiteral(command.requestDigest)}, input_digest = ${sqlLiteral(command.inputDigest)},
          write_started_entry_id = ${sqlLiteral(command.writeStartedEntryId)},
          write_started_entry_digest = ${sqlLiteral(command.writeStartedEntryDigest)},
          begin_result_json = ${sqlLiteral(canonicalJson(result))}::jsonb,
          updated_at = clock_timestamp()
        WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
          AND permit_id = ${sqlLiteral(command.permitId)}
          AND invocation_generation = ${sqlLiteral(command.invocationGeneration)}
          AND invocation_id = ${sqlLiteral(command.invocationId)} AND status = 'authorized'
          AND begin_command_id IS NULL
          AND EXISTS (SELECT 1 FROM deliveries d WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)}
            AND d.attempt = ${sqlLiteral(command.attempt)}
            AND d.active_invocation_generation = ${sqlLiteral(command.invocationGeneration)})
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(changed.changed) !== 1) storageFail("WRITE_STARTED_BINDING_MISMATCH", command.commandId);
    return { replayed: false };
  }

  async recordAck(input: {
    command: DeliveryAck;
    receiptId: string;
    stateInstanceId: StateInstanceId;
    occurredAt: string;
    resultBytes: Uint8Array;
  }): Promise<{ replayed: boolean; aliased: boolean; receiptId: string; resultBytes: Uint8Array }> {
    const { command } = input;
    assertProtocolId(input.receiptId, "rcp");
    assertProtocolId(input.stateInstanceId, "sti");
    const decoded = parseDeliveryAckResult(input.resultBytes);
    if (
      decoded.receiptId !== input.receiptId || decoded.boundary !== command.boundary
      || decoded.invocation.invocationGeneration !== command.invocationGeneration
      || decoded.invocation.invocationId !== command.invocationId
    ) storageFail("RECONCILIATION_STATE_CONFLICT", input.receiptId);

    const existingCommand = await this.#session.queryJson<{
      found: boolean;
      requestDigest?: string;
      receiptId?: string;
      resultHex?: string;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', c.request_digest, 'receiptId', c.canonical_receipt_id,
        'resultHex', encode(r.result_json_bytes, 'hex'))
      FROM delivery_ack_commands c JOIN delivery_boundary_ack_results r
        ON r.receipt_id = c.canonical_receipt_id
      WHERE c.command_id = ${sqlLiteral(command.commandId)}), json_build_object('found', false));`,
    );
    if (existingCommand.found) {
      if (existingCommand.requestDigest !== command.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", command.commandId);
      return {
        replayed: true,
        aliased: false,
        receiptId: existingCommand.receiptId ?? storageFail("IDEMPOTENCY_CONFLICT"),
        resultBytes: Buffer.from(existingCommand.resultHex ?? storageFail("IDEMPOTENCY_CONFLICT"), "hex"),
      };
    }

    const canonical = await this.#session.queryJson<{ receiptId: string; resultHex: string } | null>(
      `SELECT coalesce((SELECT json_build_object('receiptId', receipt_id, 'resultHex', encode(result_json_bytes, 'hex'))
        FROM delivery_boundary_ack_results WHERE delivery_id = ${sqlLiteral(command.deliveryId)}
          AND attempt = ${sqlLiteral(command.attempt)} AND permit_id = ${sqlLiteral(command.permitId)}
          AND invocation_generation = ${sqlLiteral(command.invocationGeneration)}
          AND invocation_id = ${sqlLiteral(command.invocationId)} AND boundary = ${sqlLiteral(command.boundary)}),
        'null'::json);`,
    );
    if (canonical !== null) {
      await this.#session.execute(
        `INSERT INTO delivery_ack_commands(
          command_id, request_digest, delivery_id, attempt, permit_id, invocation_generation,
          invocation_id, boundary, canonical_receipt_id
        ) VALUES (
          ${sqlLiteral(command.commandId)}, ${sqlLiteral(command.requestDigest)}, ${sqlLiteral(command.deliveryId)},
          ${sqlLiteral(command.attempt)}, ${sqlLiteral(command.permitId)}, ${sqlLiteral(command.invocationGeneration)},
          ${sqlLiteral(command.invocationId)}, ${sqlLiteral(command.boundary)}, ${sqlLiteral(canonical.receiptId)}
        );`,
      );
      return {
        replayed: false,
        aliased: true,
        receiptId: canonical.receiptId,
        resultBytes: Buffer.from(canonical.resultHex, "hex"),
      };
    }

    const expected = command.boundary === "input_written" ? "write_started" : "input_written";
    const changed = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE delivery_invocations SET status = ${sqlLiteral(command.boundary)}, updated_at = clock_timestamp()
        WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
          AND permit_id = ${sqlLiteral(command.permitId)}
          AND invocation_generation = ${sqlLiteral(command.invocationGeneration)}
          AND invocation_id = ${sqlLiteral(command.invocationId)} AND status = ${sqlLiteral(expected)}
          AND EXISTS (SELECT 1 FROM deliveries d WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)}
            AND d.attempt = ${sqlLiteral(command.attempt)}
            AND d.active_invocation_generation = ${sqlLiteral(command.invocationGeneration)})
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(changed.changed) !== 1) {
      storageFail(command.boundary === "model_visible" ? "ACK_PREDECESSOR_REQUIRED" : "INVOCATION_STATE_CONFLICT", command.commandId);
    }

    const resultDigest = bytesDigest(input.resultBytes);
    const detail = canonicalJson(command);
    await this.#session.execute(
      `INSERT INTO delivery_ack_commands(
        command_id, request_digest, delivery_id, attempt, permit_id, invocation_generation,
        invocation_id, boundary, canonical_receipt_id
      ) VALUES (
        ${sqlLiteral(command.commandId)}, ${sqlLiteral(command.requestDigest)}, ${sqlLiteral(command.deliveryId)},
        ${sqlLiteral(command.attempt)}, ${sqlLiteral(command.permitId)}, ${sqlLiteral(command.invocationGeneration)},
        ${sqlLiteral(command.invocationId)}, ${sqlLiteral(command.boundary)}, ${sqlLiteral(input.receiptId)}
      );
      INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id,
        launch_id, state_instance_id, turn_id, session_id, occurred_at,
        detail_json, receipt_digest, delivery_id, attempt, permit_id,
        invocation_generation, invocation_id, boundary, boundary_ack_command_id
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(command.producerFactId)}, ${sqlLiteral(command.boundary)},
        ${sqlLiteral(command.machineId)}, ${sqlLiteral(command.agentId)}, ${sqlLiteral(command.launchId)},
        ${sqlLiteral(input.stateInstanceId)}, ${sqlLiteral(command.turnId)}, ${sqlLiteral(command.sessionId)},
        ${sqlLiteral(input.occurredAt)}::timestamptz, ${sqlLiteral(detail)}::jsonb, ${sqlLiteral(canonicalDigest(command))},
        ${sqlLiteral(command.deliveryId)}, ${sqlLiteral(command.attempt)}, ${sqlLiteral(command.permitId)},
        ${sqlLiteral(command.invocationGeneration)}, ${sqlLiteral(command.invocationId)}, ${sqlLiteral(command.boundary)},
        ${sqlLiteral(command.commandId)}
      );
      INSERT INTO delivery_boundary_ack_results(
        receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id,
        boundary, result_json_bytes, result_digest
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(command.deliveryId)}, ${sqlLiteral(command.attempt)},
        ${sqlLiteral(command.permitId)}, ${sqlLiteral(command.invocationGeneration)}, ${sqlLiteral(command.invocationId)},
        ${sqlLiteral(command.boundary)}, decode('${Buffer.from(input.resultBytes).toString("hex")}', 'hex'),
        ${sqlLiteral(resultDigest)}
      );
      UPDATE deliveries SET status = ${sqlLiteral(command.boundary)},
        ${command.boundary === "input_written" ? "input_written_at" : "model_visible_at"} = ${sqlLiteral(input.occurredAt)}::timestamptz
      WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)};
      UPDATE outbox_jobs SET
        status = ${sqlLiteral(command.boundary === "input_written" ? "held" : "acked")},
        hold_reason = ${sqlLiteral(command.boundary === "input_written" ? "INPUT_WRITTEN" : null)},
        worker_lease_id = NULL, worker_lease_until = NULL, updated_at = clock_timestamp()
      WHERE job_id = (SELECT outbox_job_id FROM deliveries
        WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)});`,
    );
    return { replayed: false, aliased: false, receiptId: input.receiptId, resultBytes: input.resultBytes };
  }

  async recordReconciliationCommand(
    command: ReconcileDeliveryAttempt,
    result: ReconcileDeliveryResult,
  ): Promise<{ replayed: boolean; result: ReconcileDeliveryResult }> {
    const existing = await this.#session.queryJson<{ found: boolean; requestDigest?: string; result?: ReconcileDeliveryResult }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', request_digest, 'result', result_json
      ) FROM delivery_reconciliation_commands WHERE command_id = ${sqlLiteral(command.commandId)}),
      json_build_object('found', false));`,
    );
    if (existing.found) {
      if (existing.requestDigest !== command.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", command.commandId);
      return { replayed: true, result: existing.result ?? storageFail("IDEMPOTENCY_CONFLICT") };
    }
    if (command.evidence.kind === "scripted_not_written") {
      if (
        command.permitId === null || command.invocation === null
        || result.kind !== "same_attempt_resumable"
        || result.attempt !== command.attempt
        || result.permitId !== command.permitId
        || result.resumeMode !== "next_after_not_written"
        || result.expectedActiveInvocationGeneration !== command.invocation.invocationGeneration
        || result.nextInvocationGeneration !== command.invocation.invocationGeneration + 1
        || command.evidence.proof.invocationGeneration !== command.invocation.invocationGeneration
        || command.evidence.proof.invocationId !== command.invocation.invocationId
        || command.evidence.proof.writeStartedEntryId !== command.evidence.writeStarted.entryId
        || command.evidence.proof.writeStartedEntryDigest !== command.evidence.writeStarted.entryDigest
      ) {
        storageFail("RECONCILIATION_STATE_CONFLICT", command.commandId);
      }
      const changed = await this.#session.queryJson<{ changed: number }>(
        `WITH changed AS (
          UPDATE delivery_invocations SET status = 'not_written',
            not_written_proof_digest = ${sqlLiteral(command.evidence.proof.proofDigest)},
            updated_at = clock_timestamp()
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
            AND permit_id = ${sqlLiteral(command.permitId)}
            AND invocation_generation = ${sqlLiteral(command.invocation.invocationGeneration)}
            AND invocation_id = ${sqlLiteral(command.invocation.invocationId)}
            AND status = 'write_started'
            AND write_started_entry_id = ${sqlLiteral(command.evidence.writeStarted.entryId)}
            AND write_started_entry_digest = ${sqlLiteral(command.evidence.writeStarted.entryDigest)}
            AND input_digest = ${sqlLiteral(command.evidence.writeStarted.inputDigest)}
            AND EXISTS (SELECT 1 FROM deliveries d
              WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)} AND d.attempt = ${sqlLiteral(command.attempt)}
                AND d.consume_permit_id = ${sqlLiteral(command.permitId)}
                AND d.active_invocation_generation = ${sqlLiteral(command.invocation.invocationGeneration)})
          RETURNING 1
        ) SELECT json_build_object('changed', count(*)) FROM changed;`,
      );
      if (Number(changed.changed) !== 1) storageFail("RECONCILIATION_STATE_CONFLICT", command.commandId);
      await this.#session.execute(
        `UPDATE outbox_jobs SET status = 'held', hold_reason = 'CONSUME_PERMITTED',
          worker_lease_id = NULL, worker_lease_until = NULL, updated_at = clock_timestamp()
        WHERE job_id = (SELECT outbox_job_id FROM deliveries
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)});`,
      );
    } else if (command.evidence.kind === "write_started_ambiguous") {
      if (
        command.permitId === null || command.invocation === null
        || result.kind !== "held_ambiguous"
        || result.attempt !== command.attempt
        || result.permitId !== command.permitId
        || result.invocation.invocationGeneration !== command.invocation.invocationGeneration
        || result.invocation.invocationId !== command.invocation.invocationId
      ) {
        storageFail("RECONCILIATION_STATE_CONFLICT", command.commandId);
      }
      const changed = await this.#session.queryJson<{ changed: number }>(
        `WITH changed AS (
          UPDATE delivery_invocations SET status = 'ambiguous', updated_at = clock_timestamp()
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)}
            AND permit_id = ${sqlLiteral(command.permitId)}
            AND invocation_generation = ${sqlLiteral(command.invocation.invocationGeneration)}
            AND invocation_id = ${sqlLiteral(command.invocation.invocationId)}
            AND status = 'write_started'
            AND write_started_entry_id = ${sqlLiteral(command.evidence.writeStarted.entryId)}
            AND write_started_entry_digest = ${sqlLiteral(command.evidence.writeStarted.entryDigest)}
            AND input_digest = ${sqlLiteral(command.evidence.writeStarted.inputDigest)}
            AND EXISTS (SELECT 1 FROM deliveries d
              WHERE d.delivery_id = ${sqlLiteral(command.deliveryId)} AND d.attempt = ${sqlLiteral(command.attempt)}
                AND d.consume_permit_id = ${sqlLiteral(command.permitId)}
                AND d.active_invocation_generation = ${sqlLiteral(command.invocation.invocationGeneration)})
          RETURNING 1
        ) SELECT json_build_object('changed', count(*)) FROM changed;`,
      );
      if (Number(changed.changed) !== 1) storageFail("RECONCILIATION_STATE_CONFLICT", command.commandId);
      await this.#session.execute(
        `UPDATE outbox_jobs SET status = 'held', hold_reason = 'AMBIGUOUS_NATIVE_WRITE',
          worker_lease_id = NULL, worker_lease_until = NULL, updated_at = clock_timestamp()
        WHERE job_id = (SELECT outbox_job_id FROM deliveries
          WHERE delivery_id = ${sqlLiteral(command.deliveryId)} AND attempt = ${sqlLiteral(command.attempt)});`,
      );
    }
    await this.#session.execute(
      `INSERT INTO delivery_reconciliation_commands(
        command_id, request_digest, delivery_id, attempt, permit_id, invocation_generation,
        invocation_id, evidence_kind, evidence_digest, result_json
      ) VALUES (
        ${sqlLiteral(command.commandId)}, ${sqlLiteral(command.requestDigest)}, ${sqlLiteral(command.deliveryId)},
        ${sqlLiteral(command.attempt)}, ${sqlLiteral(command.permitId)},
        ${sqlLiteral(command.invocation?.invocationGeneration ?? null)},
        ${sqlLiteral(command.invocation?.invocationId ?? null)}, ${sqlLiteral(command.evidence.kind)},
        ${sqlLiteral(command.evidenceDigest)}, ${sqlLiteral(canonicalJson(result))}::jsonb
      );`,
    );
    return { replayed: false, result };
  }

  async projectReconciledBoundary(input: {
    command: ReconcileDeliveryAttempt;
    receiptId: string;
    stateInstanceId: StateInstanceId;
    boundary: DeliveryAckResult["boundary"];
    occurredAt: string;
    resultBytes: Uint8Array;
  }): Promise<void> {
    if (input.command.invocation === null || input.command.permitId === null) storageFail("PERMIT_REQUIRED");
    const decoded = parseDeliveryAckResult(input.resultBytes);
    if (decoded.receiptId !== input.receiptId || decoded.boundary !== input.boundary) {
      storageFail("RECONCILIATION_STATE_CONFLICT");
    }
    if (
      decoded.invocation.invocationGeneration !== input.command.invocation.invocationGeneration
      || decoded.invocation.invocationId !== input.command.invocation.invocationId
    ) {
      storageFail("RECONCILIATION_STATE_CONFLICT");
    }
    const expected = input.boundary === "input_written" ? "write_started" : "input_written";
    const changed = await this.#session.queryJson<{ changed: number }>(
      `WITH changed AS (
        UPDATE delivery_invocations SET status = ${sqlLiteral(input.boundary)}, updated_at = clock_timestamp()
        WHERE delivery_id = ${sqlLiteral(input.command.deliveryId)}
          AND attempt = ${sqlLiteral(input.command.attempt)}
          AND permit_id = ${sqlLiteral(input.command.permitId)}
          AND invocation_generation = ${sqlLiteral(input.command.invocation.invocationGeneration)}
          AND invocation_id = ${sqlLiteral(input.command.invocation.invocationId)}
          AND status = ${sqlLiteral(expected)}
          AND EXISTS (SELECT 1 FROM deliveries d
            WHERE d.delivery_id = ${sqlLiteral(input.command.deliveryId)}
              AND d.attempt = ${sqlLiteral(input.command.attempt)}
              AND d.active_invocation_generation = ${sqlLiteral(input.command.invocation.invocationGeneration)})
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(changed.changed) !== 1) {
      storageFail(input.boundary === "model_visible" ? "ACK_PREDECESSOR_REQUIRED" : "INVOCATION_STATE_CONFLICT");
    }
    const detail = canonicalJson(input.command);
    await this.#session.execute(
      `INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id, launch_id,
        state_instance_id, turn_id, session_id, occurred_at, detail_json, receipt_digest,
        delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary,
        boundary_reconciliation_command_id
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(input.command.producerFactId)}, ${sqlLiteral(input.boundary)},
        ${sqlLiteral(input.command.machineId)}, ${sqlLiteral(input.command.agentId)}, ${sqlLiteral(input.command.launchId)},
        ${sqlLiteral(input.stateInstanceId)}, ${sqlLiteral(input.command.turnId)}, ${sqlLiteral(input.command.sessionId)},
        ${sqlLiteral(input.occurredAt)}::timestamptz, ${sqlLiteral(detail)}::jsonb,
        ${sqlLiteral(canonicalDigest(input.command))}, ${sqlLiteral(input.command.deliveryId)},
        ${sqlLiteral(input.command.attempt)}, ${sqlLiteral(input.command.permitId)},
        ${sqlLiteral(input.command.invocation.invocationGeneration)}, ${sqlLiteral(input.command.invocation.invocationId)},
        ${sqlLiteral(input.boundary)}, ${sqlLiteral(input.command.commandId)}
      );
      INSERT INTO delivery_boundary_ack_results(
        receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id,
        boundary, result_json_bytes, result_digest
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(input.command.deliveryId)}, ${sqlLiteral(input.command.attempt)},
        ${sqlLiteral(input.command.permitId)}, ${sqlLiteral(input.command.invocation.invocationGeneration)},
        ${sqlLiteral(input.command.invocation.invocationId)}, ${sqlLiteral(input.boundary)},
        decode('${Buffer.from(input.resultBytes).toString("hex")}', 'hex'), ${sqlLiteral(bytesDigest(input.resultBytes))}
      );
      UPDATE deliveries SET status = ${sqlLiteral(input.boundary)},
        ${input.boundary === "input_written" ? "input_written_at" : "model_visible_at"} = ${sqlLiteral(input.occurredAt)}::timestamptz
      WHERE delivery_id = ${sqlLiteral(input.command.deliveryId)} AND attempt = ${sqlLiteral(input.command.attempt)};
      UPDATE outbox_jobs SET
        status = ${sqlLiteral(input.boundary === "input_written" ? "held" : "acked")},
        hold_reason = ${sqlLiteral(input.boundary === "input_written" ? "INPUT_WRITTEN" : null)},
        worker_lease_id = NULL, worker_lease_until = NULL, updated_at = clock_timestamp()
      WHERE job_id = (SELECT outbox_job_id FROM deliveries
        WHERE delivery_id = ${sqlLiteral(input.command.deliveryId)} AND attempt = ${sqlLiteral(input.command.attempt)});`,
    );
  }
}

export class TaskCommandRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async record(input: {
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
  }): Promise<{ replayed: boolean; taskId: string; result: Record<string, unknown> }> {
    assertProtocolId(input.commandId, "cmd");
    assertArtifactDigest(input.requestDigest);
    assertProtocolId(input.incomingProducerFactId, "fac");
    assertProtocolId(input.sourceMessageId, "msg");
    assertProtocolId(input.turnId, "trn");
    assertProtocolId(input.taskId, "tsk");
    assertProtocolId(input.serverId, "srv");
    if (!messageBodyHasContent(input.title)) storageFail("EMPTY_MESSAGE");
    canonicalProtocolJson(input.result);
    const source = await this.#session.queryJson<{ found: boolean }>(
      `SELECT json_build_object('found', EXISTS (
        SELECT 1 FROM messages
        WHERE message_id = ${sqlLiteral(input.sourceMessageId)}
          AND producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
        FOR UPDATE
      ));`,
    );
    if (!source.found) storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.turnId);
    const existing = await this.#session.queryJson<{
      found: boolean;
      requestDigest?: string;
      taskId?: string;
      result?: Record<string, unknown>;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', request_digest, 'taskId', task_id, 'result', result_json
      ) FROM task_commands WHERE command_id = ${sqlLiteral(input.commandId)}), json_build_object('found', false));`,
    );
    if (existing.found) {
      if (existing.requestDigest !== input.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", input.commandId);
      return {
        replayed: true,
        taskId: existing.taskId ?? storageFail("IDEMPOTENCY_CONFLICT"),
        result: existing.result ?? storageFail("IDEMPOTENCY_CONFLICT"),
      };
    }
    const occupied = await this.#session.queryJson<{ exists: boolean }>(
      `SELECT json_build_object('exists', EXISTS (
        SELECT 1 FROM task_commands WHERE incoming_producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND turn_id = ${sqlLiteral(input.turnId)}
      ));`,
    );
    if (occupied.exists) storageFail("SECOND_COORDINATION_CALL", input.turnId);
    const predecessor = await this.#session.queryJson<{ exists: boolean }>(
      `SELECT json_build_object('exists', EXISTS (
        SELECT 1 FROM receipts WHERE producer_fact_id = ${sqlLiteral(input.incomingProducerFactId)}
          AND turn_id = ${sqlLiteral(input.turnId)} AND effect_kind = 'reply_committed'
      ));`,
    );
    if (!predecessor.exists) storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.turnId);
    await this.#session.execute(
      `INSERT INTO tasks(
        task_id, server_id, task_number, source_message_id, source_producer_fact_id,
        title, status
      ) VALUES (
        ${sqlLiteral(input.taskId)}, ${sqlLiteral(input.serverId)}, ${sqlLiteral(input.taskNumber)},
        ${sqlLiteral(input.sourceMessageId)}, ${sqlLiteral(input.incomingProducerFactId)},
        ${sqlLiteral(input.title)}, 'todo'
      );
      INSERT INTO task_commands(
        command_id, request_digest, incoming_producer_fact_id, source_message_id,
        turn_id, task_id, result_json
      ) VALUES (
        ${sqlLiteral(input.commandId)}, ${sqlLiteral(input.requestDigest)},
        ${sqlLiteral(input.incomingProducerFactId)}, ${sqlLiteral(input.sourceMessageId)},
        ${sqlLiteral(input.turnId)}, ${sqlLiteral(input.taskId)}, ${sqlLiteral(canonicalJson(input.result))}::jsonb
      );`,
    );
    return { replayed: false, taskId: input.taskId, result: input.result };
  }
}

export class ReminderHeadRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async advance(input: { reminderId: string; expectedGeneration: number; expectedRowVersion: number }): Promise<number> {
    assertProtocolId(input.reminderId, "cmd");
    const changed = await this.#session.queryJson<{ generation: number } | null>(
      `WITH changed AS (
        UPDATE reminder_heads SET current_generation = current_generation + 1,
          row_version = row_version + 1
        WHERE reminder_id = ${sqlLiteral(input.reminderId)}
          AND current_generation = ${sqlLiteral(input.expectedGeneration)}
          AND row_version = ${sqlLiteral(input.expectedRowVersion)}
        RETURNING current_generation
      ) SELECT coalesce((SELECT json_build_object('generation', current_generation) FROM changed), 'null'::json);`,
    );
    if (changed === null) storageFail("STALE_REMINDER_GENERATION", input.reminderId);
    return Number(changed.generation);
  }
}
