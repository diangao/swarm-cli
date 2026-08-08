import { createHash } from "node:crypto";

import { canonicalProtocolJson, type ArtifactDigest, type ReceiptActor } from "@swarm/protocol";
import { storageFail } from "../errors.js";
import { assertArtifactDigest, assertProtocolId } from "../protocol.js";
import { PsqlSession, sqlLiteral } from "./session.js";

function canonicalJson(value: unknown): string {
  return new TextDecoder().decode(canonicalProtocolJson(value));
}

function canonicalDigest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

function ownerColumns(owner: ReceiptActor | { humanId: string }): { kind: "server" | "agent" | "human"; id: string } {
  if ("humanId" in owner) return { kind: "human", id: assertProtocolId(owner.humanId, "hum") };
  if ("machineId" in owner) return { kind: "agent", id: assertProtocolId(owner.agentId, "agt") };
  if ("agentId" in owner) return { kind: "agent", id: assertProtocolId(owner.agentId, "agt") };
  return { kind: "server", id: assertProtocolId(owner.serverId, "srv") };
}

export type ReminderMutationInput = {
  reminderId: string;
  operation: "schedule" | "snooze" | "cancel";
  owner: ReceiptActor | { humanId: string };
  anchor: Record<string, unknown>;
  schedule: { protocolVersion: number } & Record<string, unknown>;
  nextFireAt?: string;
  expectedGeneration: number;
  expectedHeadRowVersion: number;
  fireProducerFactId?: string;
  requestDigest?: ArtifactDigest;
};

export function reminderFireDigest(reminderId: string, generation: number): ArtifactDigest {
  return canonicalDigest({ protocolVersion: 1, reminderId, generation });
}

export class ServerReminderRepository {
  readonly #session: PsqlSession;

  constructor(session: PsqlSession) {
    this.#session = session;
  }

  async mutate(input: ReminderMutationInput): Promise<{ generation: number; headRowVersion: number }> {
    assertProtocolId(input.reminderId, "cmd");
    if (!Number.isSafeInteger(input.expectedGeneration) || input.expectedGeneration < 0) {
      storageFail("INVALID_IDENTIFIER", input.expectedGeneration);
    }
    if (!Number.isSafeInteger(input.expectedHeadRowVersion) || input.expectedHeadRowVersion < 0) {
      storageFail("INVALID_IDENTIFIER", input.expectedHeadRowVersion);
    }
    const owner = ownerColumns(input.owner);
    canonicalProtocolJson(input.anchor);
    canonicalProtocolJson(input.schedule);
    if (
      !Number.isSafeInteger(input.schedule.protocolVersion)
      || input.schedule.protocolVersion < 1
    ) storageFail("INVALID_IDENTIFIER", "schedule protocolVersion");
    if (input.operation !== "cancel" && input.nextFireAt === undefined) {
      storageFail("INVALID_STATE_TRANSITION", "scheduled generation requires nextFireAt");
    }
    await this.#session.execute(
      `SELECT pg_advisory_xact_lock(hashtextextended(
        'reminder:' || ${sqlLiteral(input.reminderId)}, 0
      ));`,
    );
    const head = await this.#session.queryJson<{ found: boolean; generation?: number; rowVersion?: number }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'generation', current_generation, 'rowVersion', row_version
      ) FROM reminder_heads WHERE reminder_id = ${sqlLiteral(input.reminderId)} FOR UPDATE),
      json_build_object('found', false));`,
    );
    if (
      (head.found ? Number(head.generation) : 0) !== input.expectedGeneration
      || (head.found ? Number(head.rowVersion) : 0) !== input.expectedHeadRowVersion
    ) storageFail("STALE_REMINDER_GENERATION", input.reminderId);

    const generation = input.expectedGeneration + 1;
    const fireProducerFactId = input.fireProducerFactId ?? `fac_${createHash("sha256")
      .update(`${input.reminderId}:${generation}`)
      .digest("hex")
      .slice(0, 26)}`;
    assertProtocolId(fireProducerFactId, "fac");
    const expectedRequestDigest = canonicalDigest({
      protocolVersion: 1,
      reminderId: input.reminderId,
      operation: input.operation,
      generation,
      owner,
      anchor: input.anchor,
      schedule: input.schedule,
      nextFireAt: input.nextFireAt ?? null,
      fireProducerFactId,
    });
    const requestDigest = assertArtifactDigest(input.requestDigest ?? expectedRequestDigest);
    if (requestDigest !== expectedRequestDigest) storageFail("IDEMPOTENCY_CONFLICT", input.reminderId);
    const status = input.operation === "cancel" ? "canceled" : "scheduled";
    await this.#session.execute(
      `UPDATE reminders SET status = 'canceled', canceled_at = clock_timestamp(),
        row_version = row_version + 1
      WHERE reminder_id = ${sqlLiteral(input.reminderId)}
        AND generation = ${sqlLiteral(input.expectedGeneration)} AND status = 'scheduled';
      INSERT INTO reminders(
        reminder_id, owner_kind, owner_id, anchor_json, schedule_json,
        generation, next_fire_at, canceled_at, fire_producer_fact_id,
        request_digest, status, row_version
      ) VALUES (
        ${sqlLiteral(input.reminderId)}, ${sqlLiteral(owner.kind)}, ${sqlLiteral(owner.id)},
        ${sqlLiteral(canonicalJson(input.anchor))}::jsonb,
        ${sqlLiteral(canonicalJson(input.schedule))}::jsonb,
        ${sqlLiteral(generation)}, ${sqlLiteral(input.operation === "cancel" ? null : input.nextFireAt ?? null)}::timestamptz,
        ${input.operation === "cancel" ? "clock_timestamp()" : "NULL"},
        ${sqlLiteral(fireProducerFactId)}, ${sqlLiteral(requestDigest)},
        ${sqlLiteral(status)}, 0
      );
      INSERT INTO reminder_heads(reminder_id, current_generation, row_version)
      VALUES (${sqlLiteral(input.reminderId)}, ${sqlLiteral(generation)}, 0)
      ON CONFLICT (reminder_id) DO UPDATE SET
        current_generation = excluded.current_generation,
        row_version = reminder_heads.row_version + 1
      WHERE reminder_heads.current_generation = ${sqlLiteral(input.expectedGeneration)}
        AND reminder_heads.row_version = ${sqlLiteral(input.expectedHeadRowVersion)};`,
    );
    return {
      generation,
      headRowVersion: head.found ? input.expectedHeadRowVersion + 1 : 0,
    };
  }

  async advanceGeneration(input: Omit<ReminderMutationInput, "operation">): Promise<number> {
    return (await this.mutate({ ...input, operation: "schedule" })).generation;
  }

  async schedule(input: Omit<ReminderMutationInput, "operation">): Promise<{ generation: number; headRowVersion: number }> {
    return await this.mutate({ ...input, operation: "schedule" });
  }

  async snooze(input: Omit<ReminderMutationInput, "operation">): Promise<{ generation: number; headRowVersion: number }> {
    return await this.mutate({ ...input, operation: "snooze" });
  }

  async cancel(input: Omit<ReminderMutationInput, "operation" | "nextFireAt">): Promise<{
    generation: number; headRowVersion: number;
  }> {
    return await this.mutate({ ...input, operation: "cancel" });
  }

  async leaseDue(input: {
    reminderId: string;
    generation: number;
    expectedHeadRowVersion: number;
    workerLeaseId: string;
    leaseUntil: string;
    now: string;
  }): Promise<void> {
    assertProtocolId(input.reminderId, "cmd");
    assertProtocolId(input.workerLeaseId, "lse");
    const changed = await this.#session.queryJson<{ changed: number }>(
      `WITH head AS (
        SELECT 1 FROM reminder_heads WHERE reminder_id = ${sqlLiteral(input.reminderId)}
          AND current_generation = ${sqlLiteral(input.generation)}
          AND row_version = ${sqlLiteral(input.expectedHeadRowVersion)} FOR UPDATE
      ), changed AS (
        UPDATE reminders SET worker_lease_id = ${sqlLiteral(input.workerLeaseId)},
          worker_lease_until = ${sqlLiteral(input.leaseUntil)}::timestamptz,
          row_version = row_version + 1
        WHERE reminder_id = ${sqlLiteral(input.reminderId)} AND generation = ${sqlLiteral(input.generation)}
          AND status = 'scheduled' AND next_fire_at <= ${sqlLiteral(input.now)}::timestamptz
          AND ${sqlLiteral(input.leaseUntil)}::timestamptz > ${sqlLiteral(input.now)}::timestamptz
          AND (worker_lease_id IS NULL OR worker_lease_until <= ${sqlLiteral(input.now)}::timestamptz)
          AND EXISTS (SELECT 1 FROM head)
        RETURNING 1
      ) SELECT json_build_object('changed', count(*)) FROM changed;`,
    );
    if (Number(changed.changed) !== 1) storageFail("STALE_REMINDER_GENERATION", input.reminderId);
  }

  async fire(input: {
    reminderId: string;
    generation: number;
    expectedHeadRowVersion: number;
    workerLeaseId: string;
    now: string;
    serverId: string;
    receiptId: string;
    requestDigest: ArtifactDigest;
    occurredAt: string;
  }): Promise<{
    replayed: boolean;
    reminderId: string;
    generation: number;
    producerFactId: string;
    outboxJobId: number;
    receiptId: string;
  }> {
    assertProtocolId(input.reminderId, "cmd");
    assertProtocolId(input.workerLeaseId, "lse");
    assertProtocolId(input.serverId, "srv");
    assertProtocolId(input.receiptId, "rcp");
    assertArtifactDigest(input.requestDigest);
    if (input.requestDigest !== reminderFireDigest(input.reminderId, input.generation)) {
      storageFail("IDEMPOTENCY_CONFLICT", input.reminderId);
    }
    const existing = await this.#session.queryJson<{
      found: boolean; requestDigest?: string; producerFactId?: string; outboxJobId?: number; receiptId?: string;
    }>(
      `SELECT coalesce((SELECT json_build_object(
        'found', true, 'requestDigest', request_digest, 'producerFactId', producer_fact_id,
        'outboxJobId', outbox_job_id, 'receiptId', receipt_id
      ) FROM reminder_fires WHERE reminder_id = ${sqlLiteral(input.reminderId)}
        AND generation = ${sqlLiteral(input.generation)}), json_build_object('found', false));`,
    );
    if (existing.found) {
      if (existing.requestDigest !== input.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", input.reminderId);
      return {
        replayed: true, reminderId: input.reminderId, generation: input.generation,
        producerFactId: existing.producerFactId ?? storageFail("IDEMPOTENCY_CONFLICT"),
        outboxJobId: Number(existing.outboxJobId),
        receiptId: existing.receiptId ?? storageFail("IDEMPOTENCY_CONFLICT"),
      };
    }
    const due = await this.#session.queryJson<{
      found: boolean; producerFactId?: string; nextFireAt?: string; status?: string;
    }>(
      `WITH head AS (
        SELECT 1 FROM reminder_heads WHERE reminder_id = ${sqlLiteral(input.reminderId)}
          AND current_generation = ${sqlLiteral(input.generation)}
          AND row_version = ${sqlLiteral(input.expectedHeadRowVersion)} FOR UPDATE
      ) SELECT coalesce((SELECT json_build_object(
        'found', true, 'producerFactId', fire_producer_fact_id,
        'nextFireAt', next_fire_at, 'status', status
      ) FROM reminders WHERE reminder_id = ${sqlLiteral(input.reminderId)}
        AND generation = ${sqlLiteral(input.generation)} AND EXISTS (SELECT 1 FROM head)
        AND status = 'scheduled' AND next_fire_at <= ${sqlLiteral(input.now)}::timestamptz
        AND worker_lease_id = ${sqlLiteral(input.workerLeaseId)}
        AND worker_lease_until > ${sqlLiteral(input.now)}::timestamptz
      FOR UPDATE), json_build_object('found', false));`,
    );
    if (!due.found) {
      const raced = await this.#session.queryJson<{
        found: boolean; requestDigest?: string; producerFactId?: string; outboxJobId?: number; receiptId?: string;
      }>(
        `SELECT coalesce((SELECT json_build_object(
          'found', true, 'requestDigest', request_digest, 'producerFactId', producer_fact_id,
          'outboxJobId', outbox_job_id, 'receiptId', receipt_id
        ) FROM reminder_fires WHERE reminder_id = ${sqlLiteral(input.reminderId)}
          AND generation = ${sqlLiteral(input.generation)}), json_build_object('found', false));`,
      );
      if (!raced.found) storageFail("STALE_REMINDER_GENERATION", input.reminderId);
      if (raced.requestDigest !== input.requestDigest) storageFail("IDEMPOTENCY_CONFLICT", input.reminderId);
      return {
        replayed: true, reminderId: input.reminderId, generation: input.generation,
        producerFactId: raced.producerFactId ?? storageFail("IDEMPOTENCY_CONFLICT"),
        outboxJobId: Number(raced.outboxJobId),
        receiptId: raced.receiptId ?? storageFail("IDEMPOTENCY_CONFLICT"),
      };
    }
    const producerFactId = due.producerFactId ?? storageFail("STALE_REMINDER_GENERATION");
    const payload = {
      protocolVersion: 1,
      reminderId: input.reminderId,
      generation: input.generation,
      producerFactId,
    };
    const outbox = await this.#session.queryJson<{ jobId: number }>(
      `WITH inserted AS (
        INSERT INTO outbox_jobs(
          idempotency_namespace, idempotency_key, producer_fact_id,
          event_kind, event_version, payload_json, status
        ) VALUES (
          'reminder_fire.v1', ${sqlLiteral(`${input.reminderId}:${input.generation}`)},
          ${sqlLiteral(producerFactId)}, 'reminder_fire', 1,
          ${sqlLiteral(canonicalJson(payload))}::jsonb, 'pending'
        ) RETURNING job_id
      ) SELECT json_build_object('jobId', job_id) FROM inserted;`,
    );
    const detail = { ...payload, kind: "reminder_fired" };
    await this.#session.execute(
      `INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_server_id, occurred_at,
        detail_json, receipt_digest, effect_kind, effect_reminder_id, effect_reminder_generation
      ) VALUES (
        ${sqlLiteral(input.receiptId)}, ${sqlLiteral(producerFactId)}, 'side_effect_applied',
        ${sqlLiteral(input.serverId)}, ${sqlLiteral(input.occurredAt)}::timestamptz,
        ${sqlLiteral(canonicalJson(detail))}::jsonb, ${sqlLiteral(canonicalDigest(detail))},
        'reminder_fired', ${sqlLiteral(input.reminderId)}, ${sqlLiteral(input.generation)}
      );
      INSERT INTO reminder_fires(
        reminder_id, generation, producer_fact_id, request_digest, outbox_job_id, receipt_id
      ) VALUES (
        ${sqlLiteral(input.reminderId)}, ${sqlLiteral(input.generation)}, ${sqlLiteral(producerFactId)},
        ${sqlLiteral(input.requestDigest)}, ${sqlLiteral(outbox.jobId)}, ${sqlLiteral(input.receiptId)}
      );
      UPDATE reminders SET status = 'fired', worker_lease_id = NULL, worker_lease_until = NULL,
        row_version = row_version + 1
      WHERE reminder_id = ${sqlLiteral(input.reminderId)} AND generation = ${sqlLiteral(input.generation)}
        AND status = 'scheduled';`,
    );
    return {
      replayed: false,
      reminderId: input.reminderId,
      generation: input.generation,
      producerFactId,
      outboxJobId: outbox.jobId,
      receiptId: input.receiptId,
    };
  }
}
