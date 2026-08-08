import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { after, before, test } from "node:test";
import {
  MESSAGE_BODY_CONTENT_FIXTURES,
  canonicalProtocolJson,
  messageBodyHasContent,
  type AcquireConsumePermit,
  type AgentId,
  type ArtifactDigest,
  type BeginNativeWrite,
  type ChannelId,
  type CommandId,
  type DeliveryAck,
  type DeliveryAckResult,
  type DeliveryEnvelope,
  type DeliveryId,
  type InputWrittenJournalEntry,
  type InvocationJournalEntry,
  type LaunchId,
  type MachineId,
  type MessageId,
  type ModelVisibleJournalEntry,
  type NativeInvocationFence,
  type ProducerFactId,
  type ProtocolVersion,
  type ReconcileDeliveryAttempt,
  type ReconcileDeliveryResult,
  type ReceiptActor,
  type ReceiptId,
  type ResumeConsumePermit,
  type ServerId,
  type ScriptedNotWrittenProof,
  type StateInstanceId,
  type Target,
  type TransitionReceipt,
  type WriteStartedJournalEntry,
} from "@swarm/protocol";
import {
  PsqlSession,
  SharedStore,
  StorageError,
  appendHumanMessageDigest,
  sqlLiteral,
  type IdempotentRequest,
  type IdempotencyScope,
} from "../src/index.js";

function requiredDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (value === undefined || value.length === 0) {
    throw new Error("DATABASE_URL is required; PostgreSQL Gate 0 never falls back");
  }
  return value;
}

const databaseUrl = requiredDatabaseUrl();

const schema = `swarm_storage_gate0_${process.pid}`;
const store = new SharedStore(databaseUrl, schema);

function id(prefix: string, character: string): string {
  return `${prefix}_${character.repeat(26)}`;
}

function variantId(prefix: string, character: string, tail: string): string {
  assert.ok(tail.length > 0 && tail.length < 26);
  return `${prefix}_${character.repeat(26 - tail.length)}${tail}`;
}

function digest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

function receiptDigest(receipt: TransitionReceipt): ArtifactDigest {
  const { receiptId: _wireIdentity, ...payload } = receipt;
  return digest(payload);
}

function request(
  character: string,
  scope: IdempotencyScope = "message.append.v1",
): IdempotentRequest {
  return {
    actor: { serverId: id("srv", "a") as ServerId },
    scope,
    requestKind: "command",
    requestId: id("cmd", character),
    requestDigest: digest({ character, scope }),
  };
}

function channelTarget(character = "a", thread?: string): Target {
  const value: { kind: "channel"; channelId: ChannelId; threadRootMessageId?: MessageId } = {
    kind: "channel",
    channelId: id("chn", character) as ChannelId,
  };
  if (thread !== undefined) value.threadRootMessageId = thread as MessageId;
  return value;
}

function humanAppend(
  messageCharacter: string,
  target: Target,
  body: string,
  producerCharacter: string,
) {
  const base = {
    protocolVersion: 1 as const,
    messageId: id("msg", messageCharacter),
    target,
    humanId: id("hum", "c"),
    body,
    producerFactId: id("fac", producerCharacter),
    serverId: id("srv", "a"),
  };
  return {
    ...base,
    requestDigest: appendHumanMessageDigest(base),
    receiptId: variantId("rcp", messageCharacter, "s"),
    deliveryId: variantId("dlv", messageCharacter, "s"),
    occurredAt: "2026-08-08T00:00:00.000Z",
  };
}

async function raw(sql: string): Promise<string> {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`SET search_path TO ${schema}, pg_catalog;`);
    const statement = sql.trim();
    return await session.execute(statement.endsWith(";") ? statement : `${statement};`);
  } finally {
    await session.close();
  }
}

type NativePermitSeed = {
  character: string;
  fence: Omit<AcquireConsumePermit, "commandId" | "requestDigest" | "boundary">;
  permitId: CommandId;
  invocation1: CommandId;
  invocation2: CommandId;
};

async function seedNativePermitCandidate(character: string): Promise<NativePermitSeed> {
  await raw(`
    BEGIN;
    INSERT INTO machines(machine_id, server_id)
      VALUES ('${id("mch", character)}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id)
      VALUES ('${id("agt", character)}', '${id("srv", "a")}');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", character)}', '${id("srv", "a")}', 'private', 'native-${character}');
    INSERT INTO memberships(channel_id, actor_kind, actor_id, state, membership_epoch, row_version)
      VALUES ('${id("chn", character)}', 'agent', '${id("agt", character)}', 'active', 1, 1);
    INSERT INTO agent_launches(
      launch_id, machine_id, agent_id, runtime_kind, workspace_generation,
      routing_generation, state, activated_at
    ) VALUES (
      '${id("lnc", character)}', '${id("mch", character)}', '${id("agt", character)}',
      'codex', 1, 0, 'activated', clock_timestamp()
    );
    INSERT INTO target_owner_routes(
      target_kind, target_id, agent_id, machine_id, expected_launch_id,
      membership_epoch, routing_generation, route_version, row_version
    ) VALUES (
      'channel', '${id("chn", character)}', '${id("agt", character)}',
      '${id("mch", character)}', '${id("lnc", character)}', 1, 0, 1, 1
    );
    INSERT INTO messages(
      message_id, target_kind, target_id, author_kind, author_id, target_seq,
      body, producer_fact_id, payload_digest
    ) VALUES (
      '${id("msg", character)}', 'channel', '${id("chn", character)}',
      'server', '${id("srv", "a")}', 1, 'repository permit seed',
      '${id("fac", character)}', 'sha256:${"5".repeat(64)}'
    );
    INSERT INTO message_audience(message_id, actor_kind, actor_id, membership_epoch, audience_mode)
      VALUES ('${id("msg", character)}', 'agent', '${id("agt", character)}', 1, 'owner_body');
    INSERT INTO message_owner_routes(
      producer_fact_id, message_id, target_kind, target_id, agent_id, machine_id,
      expected_launch_id, membership_epoch, routing_generation, route_version
    ) VALUES (
      '${id("fac", character)}', '${id("msg", character)}', 'channel', '${id("chn", character)}',
      '${id("agt", character)}', '${id("mch", character)}', '${id("lnc", character)}', 1, 0, 1
    );
    INSERT INTO outbox_jobs(
      idempotency_namespace, idempotency_key, producer_fact_id,
      event_kind, event_version, payload_json, status
    ) VALUES (
      'message_delivery.v1', 'permit-${character}', '${id("fac", character)}',
      'message_delivery', 1, '{"protocolVersion":1}', 'pending'
    );
    INSERT INTO deliveries(
      delivery_id, attempt, message_id, target_kind, target_id, target_seq,
      producer_fact_id, agent_id, machine_id, expected_launch_id, status,
      outbox_job_id, membership_epoch, routing_generation, route_version
    ) SELECT
      '${id("dlv", character)}', 1, '${id("msg", character)}', 'channel', '${id("chn", character)}', 1,
      '${id("fac", character)}', '${id("agt", character)}', '${id("mch", character)}',
      '${id("lnc", character)}', 'pending', job_id, 1, 0, 1
    FROM outbox_jobs WHERE producer_fact_id = '${id("fac", character)}';
    COMMIT;
  `);
  return {
    character,
    fence: {
      protocolVersion: 1 as ProtocolVersion,
      deliveryId: id("dlv", character) as DeliveryId,
      attempt: 1,
      producerFactId: id("fac", character) as ProducerFactId,
      agentId: id("agt", character) as AgentId,
      machineId: id("mch", character) as MachineId,
      launchId: id("lnc", character) as LaunchId,
      membershipEpoch: 1,
      routingGeneration: 0,
      routeVersion: 1,
      sessionId: id("ses", character) as AcquireConsumePermit["sessionId"],
      turnId: id("trn", character) as AcquireConsumePermit["turnId"],
    },
    permitId: variantId("cmd", character, "p") as CommandId,
    invocation1: variantId("cmd", character, "1") as CommandId,
    invocation2: variantId("cmd", character, "2") as CommandId,
  };
}

function commandWithDigest<T extends Record<string, unknown>>(value: T): T & { requestDigest: ArtifactDigest } {
  return { ...value, requestDigest: digest(value) };
}

function deliveryRequest(character: string, tail: string): IdempotentRequest {
  return {
    actor: { serverId: id("srv", "a") as ServerId },
    scope: "delivery.mutate.v1",
    requestKind: "command",
    requestId: variantId("cmd", character, tail),
    requestDigest: digest({ character, tail, scope: "delivery.mutate.v1" }),
  };
}

async function nativePermitSnapshot(character: string): Promise<Record<string, unknown>> {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`SET search_path TO ${schema}, pg_catalog;`);
    return await session.queryJson<Record<string, unknown>>(`SELECT json_build_object(
      'membership', (SELECT json_build_object('state', state, 'epoch', membership_epoch, 'version', row_version)
        FROM memberships WHERE channel_id = '${id("chn", character)}' AND actor_kind = 'agent'
          AND actor_id = '${id("agt", character)}'),
      'route', (SELECT json_build_object('launchId', expected_launch_id, 'version', route_version,
          'routingGeneration', routing_generation)
        FROM target_owner_routes WHERE target_kind = 'channel' AND target_id = '${id("chn", character)}'),
      'delivery', (SELECT to_jsonb(d) FROM deliveries d WHERE delivery_id = '${id("dlv", character)}'),
      'job', (SELECT to_jsonb(o) FROM outbox_jobs o WHERE producer_fact_id = '${id("fac", character)}'),
      'invocations', coalesce((SELECT jsonb_agg(to_jsonb(i) ORDER BY invocation_generation)
        FROM delivery_invocations i WHERE delivery_id = '${id("dlv", character)}'), '[]'::jsonb),
      'permitCommands', (SELECT count(*) FROM delivery_permit_commands
        WHERE delivery_id = '${id("dlv", character)}'),
      'reconciliationCommands', (SELECT count(*) FROM delivery_reconciliation_commands
        WHERE delivery_id = '${id("dlv", character)}'),
      'receipts', (SELECT count(*) FROM receipts WHERE delivery_id = '${id("dlv", character)}'),
      'projections', (SELECT count(*) FROM delivery_boundary_ack_results
        WHERE delivery_id = '${id("dlv", character)}')
    );`);
  } finally {
    await session.close();
  }
}

function acquirePermit(seed: NativePermitSeed, tail: string): AcquireConsumePermit {
  return commandWithDigest({
    ...seed.fence,
    commandId: variantId("cmd", seed.character, tail) as CommandId,
    boundary: "daemon_accepted" as const,
  });
}

function resumePermit(
  seed: NativePermitSeed,
  tail: string,
  expectedActiveInvocationGeneration: number,
  resumeMode: ResumeConsumePermit["resumeMode"],
): ResumeConsumePermit {
  return commandWithDigest({
    ...seed.fence,
    commandId: variantId("cmd", seed.character, tail) as CommandId,
    permitId: seed.permitId,
    expectedActiveInvocationGeneration,
    resumeMode,
    boundary: "daemon_accepted" as const,
  });
}

function journalEntry<K extends "permit_recorded" | "write_started" | "input_written" | "model_visible">(
  seed: NativePermitSeed,
  invocation: NativeInvocationFence,
  kind: K,
  entryTail: string,
  sequence: number,
  previousEntryDigest: ArtifactDigest | null,
  extra: K extends "write_started"
    ? { inputDigest: ArtifactDigest }
    : K extends "input_written"
      ? { runtimeWriteId: CommandId }
      : K extends "model_visible"
        ? { runtimeWriteId: CommandId; visibilityEventId: CommandId }
        : Record<string, never>,
): InvocationJournalEntry<K> & Record<string, unknown> {
  const unsigned = {
    ...seed.fence,
    ...invocation,
    permitId: seed.permitId,
    journalId: invocation.invocationId,
    entryId: variantId("cmd", seed.character, entryTail) as CommandId,
    sequence,
    kind,
    previousEntryDigest,
    ...extra,
  };
  return { ...unsigned, entryDigest: digest(unsigned) };
}

type NativeJournal = {
  permitRecorded: InvocationJournalEntry<"permit_recorded">;
  writeStarted: WriteStartedJournalEntry;
  inputWritten: InputWrittenJournalEntry;
  modelVisible: ModelVisibleJournalEntry;
};

function nativeJournal(
  seed: NativePermitSeed,
  invocation: NativeInvocationFence,
  tail: string,
): NativeJournal {
  const inputDigest = digest({ character: seed.character, generation: invocation.invocationGeneration, input: tail });
  const runtimeWriteId = variantId("cmd", seed.character, `${tail}w`) as CommandId;
  const permitRecorded = journalEntry(
    seed,
    invocation,
    "permit_recorded",
    `${tail}p`,
    1,
    null,
    {},
  ) as InvocationJournalEntry<"permit_recorded">;
  const writeStarted = journalEntry(
    seed,
    invocation,
    "write_started",
    `${tail}s`,
    2,
    permitRecorded.entryDigest,
    { inputDigest },
  ) as WriteStartedJournalEntry;
  const inputWritten = journalEntry(
    seed,
    invocation,
    "input_written",
    `${tail}n`,
    3,
    writeStarted.entryDigest,
    { runtimeWriteId },
  ) as InputWrittenJournalEntry;
  const modelVisible = journalEntry(
    seed,
    invocation,
    "model_visible",
    `${tail}v`,
    4,
    inputWritten.entryDigest,
    {
      runtimeWriteId,
      visibilityEventId: variantId("cmd", seed.character, `${tail}e`) as CommandId,
    },
  ) as ModelVisibleJournalEntry;
  return { permitRecorded, writeStarted, inputWritten, modelVisible };
}

function beginNativeWrite(
  seed: NativePermitSeed,
  invocation: NativeInvocationFence,
  journal: NativeJournal,
  tail: string,
): BeginNativeWrite {
  return commandWithDigest({
    ...seed.fence,
    ...invocation,
    commandId: variantId("cmd", seed.character, tail) as CommandId,
    permitId: seed.permitId,
    boundary: "write_started" as const,
    inputDigest: journal.writeStarted.inputDigest,
    writeStartedEntryId: journal.writeStarted.entryId,
    writeStartedEntryDigest: journal.writeStarted.entryDigest,
  });
}

function scriptedNotWrittenProof(
  seed: NativePermitSeed,
  invocation: NativeInvocationFence,
  journal: NativeJournal,
  tail: string,
): ScriptedNotWrittenProof {
  const unsigned = {
    driverKind: "scripted_fake" as const,
    fixtureId: variantId("cmd", seed.character, `${tail}f`) as CommandId,
    scriptDigest: digest({ character: seed.character, script: tail }),
    invocationId: invocation.invocationId,
    invocationGeneration: invocation.invocationGeneration,
    writeStartedEntryId: journal.writeStarted.entryId,
    writeStartedEntryDigest: journal.writeStarted.entryDigest,
    outcomeOrdinal: 1,
    outcome: "not_written" as const,
  };
  return { ...unsigned, proofDigest: digest(unsigned) };
}

function reconciliationCommand(
  seed: NativePermitSeed,
  invocation: NativeInvocationFence,
  evidence: ReconcileDeliveryAttempt["evidence"],
  tail: string,
): ReconcileDeliveryAttempt {
  return commandWithDigest({
    ...seed.fence,
    commandId: variantId("cmd", seed.character, tail) as CommandId,
    permitId: seed.permitId,
    invocation,
    evidenceDigest: digest(evidence),
    evidence,
  });
}

function deliveryAck(
  seed: NativePermitSeed,
  invocation: NativeInvocationFence,
  boundary: DeliveryAck["boundary"],
  tail: string,
): DeliveryAck {
  return commandWithDigest({
    ...seed.fence,
    ...invocation,
    commandId: variantId("cmd", seed.character, tail) as CommandId,
    permitId: seed.permitId,
    boundary,
  });
}

function deliveryAckBytes(
  invocation: NativeInvocationFence,
  receiptId: ReceiptId,
  boundary: DeliveryAckResult["boundary"],
): Uint8Array {
  return canonicalProtocolJson({
    boundary,
    receiptId,
    invocation,
    jobState: boundary === "input_written" ? "held/INPUT_WRITTEN" : "acked/MODEL_VISIBLE",
  });
}

function boundaryResultSql(
  receiptCharacter: string,
  invocationCharacter: string,
  boundary: "input_written" | "model_visible",
): { bytes: string; digest: ArtifactDigest } {
  const value = {
    boundary,
    receiptId: id("rcp", receiptCharacter),
    invocation: {
      invocationGeneration: 1,
      invocationId: id("cmd", invocationCharacter),
    },
    jobState: boundary === "input_written" ? "held/INPUT_WRITTEN" : "acked/MODEL_VISIBLE",
  };
  const encoded = canonicalProtocolJson(value);
  return {
    bytes: Buffer.from(encoded).toString("hex"),
    digest: `sha256:${createHash("sha256").update(encoded).digest("hex")}` as ArtifactDigest,
  };
}

async function seedNativeAttempt(character: string, sequence: number): Promise<void> {
  await raw(`
    BEGIN;
    INSERT INTO agent_launches(
      launch_id, machine_id, agent_id, runtime_kind, workspace_generation,
      routing_generation, state
    ) VALUES (
      '${id("lnc", "a")}', '${id("mch", "a")}', '${id("agt", "a")}',
      'codex', 1, 0, 'requested'
    ) ON CONFLICT (launch_id) DO NOTHING;
    INSERT INTO messages(
      message_id, target_kind, target_id, author_kind, author_id, target_seq,
      body, producer_fact_id, payload_digest
    ) VALUES (
      '${id("msg", character)}', 'channel', '${id("chn", "a")}', 'server', '${id("srv", "a")}',
      ${sequence}, 'native boundary seed', '${id("fac", character)}', 'sha256:${"6".repeat(64)}'
    );
    INSERT INTO outbox_jobs(
      idempotency_namespace, idempotency_key, producer_fact_id,
      event_kind, event_version, payload_json, status, hold_reason
    ) VALUES (
      'message_delivery.v1', 'native-${character}', '${id("fac", character)}',
      'message_delivery', 1, '{"protocolVersion":1}', 'held', 'CONSUME_PERMITTED'
    );
    INSERT INTO deliveries(
      delivery_id, attempt, message_id, target_kind, target_id, target_seq,
      producer_fact_id, agent_id, machine_id, expected_launch_id, status,
      daemon_accepted_at, outbox_job_id, membership_epoch, routing_generation,
      route_version, consume_permit_id, consume_permitted_at, active_invocation_generation
    ) SELECT
      '${id("dlv", character)}', 1, '${id("msg", character)}', 'channel', '${id("chn", "a")}',
      ${sequence}, '${id("fac", character)}', '${id("agt", "a")}', '${id("mch", "a")}',
      '${id("lnc", "a")}', 'daemon_accepted', clock_timestamp(), job_id,
      1, 0, 1, '${id("cmd", character)}', clock_timestamp(), 1
    FROM outbox_jobs WHERE producer_fact_id = '${id("fac", character)}';
    INSERT INTO delivery_invocations(
      delivery_id, attempt, invocation_generation, invocation_id, permit_id,
      status, begin_command_id, begin_request_digest, input_digest,
      write_started_entry_id, write_started_entry_digest, begin_result_json
    ) VALUES (
      '${id("dlv", character)}', 1, 1, '${id("cmd", character)}',
      '${id("cmd", character)}', 'write_started', '${id("cmd", character)}',
      'sha256:${"a".repeat(64)}', 'sha256:${"b".repeat(64)}', '${id("cmd", character)}',
      'sha256:${"c".repeat(64)}', '{"protocolVersion":1}'
    );
    COMMIT;
  `);
}

function ackCommandInsert(character: string, boundary: "input_written" | "model_visible"): string {
  return `INSERT INTO delivery_ack_commands(
    command_id, request_digest, delivery_id, attempt, permit_id,
    invocation_generation, invocation_id, boundary, canonical_receipt_id
  ) VALUES (
    '${id("cmd", character)}', 'sha256:${"d".repeat(64)}', '${id("dlv", character)}', 1,
    '${id("cmd", character)}', 1, '${id("cmd", character)}', '${boundary}', '${id("rcp", character)}'
  )`;
}

function reconciliationCommandInsert(
  character: string,
  repaired: readonly ["input_written"] | readonly ["input_written", "model_visible"] = [
    "input_written",
    "model_visible",
  ],
): string {
  const result = JSON.stringify({ kind: "boundary_repaired", repaired });
  return `INSERT INTO delivery_reconciliation_commands(
    command_id, request_digest, delivery_id, attempt, permit_id,
    invocation_generation, invocation_id, evidence_kind, evidence_digest, result_json
  ) VALUES (
    '${id("cmd", character)}', 'sha256:${"e".repeat(64)}', '${id("dlv", character)}', 1,
    '${id("cmd", character)}', 1, '${id("cmd", character)}', 'input_written',
    'sha256:${"f".repeat(64)}', '${result}'
  )`;
}

function boundaryReceiptInsert(
  character: string,
  boundary: "input_written" | "model_visible",
  creator: "ack" | "reconciliation",
): string {
  return `INSERT INTO receipts(
    receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id,
    launch_id, state_instance_id, turn_id, session_id, occurred_at,
    detail_json, receipt_digest, delivery_id, attempt, permit_id,
    invocation_generation, invocation_id, boundary,
    boundary_ack_command_id, boundary_reconciliation_command_id
  ) VALUES (
    '${id("rcp", character)}', '${id("fac", character)}', '${boundary}',
    '${id("mch", "a")}', '${id("agt", "a")}', '${id("lnc", "a")}',
    '${id("sti", character)}', '${id("trn", character)}', '${id("ses", character)}',
    clock_timestamp(), '{"protocolVersion":1}', 'sha256:${"1".repeat(64)}',
    '${id("dlv", character)}', 1, '${id("cmd", character)}', 1,
    '${id("cmd", character)}', '${boundary}',
    ${creator === "ack" ? `'${id("cmd", character)}'` : "NULL"},
    ${creator === "reconciliation" ? `'${id("cmd", character)}'` : "NULL"}
  )`;
}

function boundaryProjectionInsert(character: string, boundary: "input_written" | "model_visible"): string {
  const result = boundaryResultSql(character, character, boundary);
  return `INSERT INTO delivery_boundary_ack_results(
    receipt_id, delivery_id, attempt, permit_id, invocation_generation,
    invocation_id, boundary, result_json_bytes, result_digest
  ) VALUES (
    '${id("rcp", character)}', '${id("dlv", character)}', 1, '${id("cmd", character)}',
    1, '${id("cmd", character)}', '${boundary}', decode('${result.bytes}', 'hex'), '${result.digest}'
  )`;
}

before(async () => {
  const session = await PsqlSession.open(databaseUrl);
  try {
    const identity = await session.queryJson<{ databaseName: string; version: number }>(
      "SELECT json_build_object('databaseName', current_database(), 'version', current_setting('server_version_num')::integer);",
    );
    assert.match(identity.databaseName, /^swarm_storage_test_[a-z0-9_]+$/u);
    assert.ok(identity.version >= 160000 && identity.version < 170000);
    await session.execute(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
  } finally {
    await session.close();
  }
  const receipts = await store.migrate();
  assert.ok(receipts.length > 0 && receipts.every((receipt) => receipt.applied));
  await raw(`
    INSERT INTO servers(server_id, display_name) VALUES ('${id("srv", "a")}', 'Gate 0');
    INSERT INTO machines(machine_id, server_id) VALUES ('${id("mch", "a")}', '${id("srv", "a")}');
    INSERT INTO machines(machine_id, server_id) VALUES ('${id("mch", "b")}', '${id("srv", "a")}');
    INSERT INTO machines(machine_id, server_id) VALUES ('${id("mch", "c")}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id) VALUES ('${id("agt", "a")}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id) VALUES ('${id("agt", "b")}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id) VALUES ('${id("agt", "c")}', '${id("srv", "a")}');
    INSERT INTO humans(human_id, server_id, display_name)
      VALUES ('${id("hum", "c")}', '${id("srv", "a")}', 'Gate human');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", "a")}', '${id("srv", "a")}', 'private', 'gate0-a');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", "b")}', '${id("srv", "a")}', 'private', 'gate0-b');
    INSERT INTO memberships(channel_id, actor_kind, actor_id, state, membership_epoch, row_version)
      VALUES
        ('${id("chn", "a")}', 'human', '${id("hum", "c")}', 'active', 1, 1),
        ('${id("chn", "a")}', 'agent', '${id("agt", "c")}', 'active', 1, 1),
        ('${id("chn", "b")}', 'human', '${id("hum", "c")}', 'active', 1, 1),
        ('${id("chn", "b")}', 'agent', '${id("agt", "c")}', 'active', 1, 1);
    INSERT INTO agent_launches(
      launch_id, machine_id, agent_id, runtime_kind, workspace_generation,
      routing_generation, state, activated_at
    ) VALUES (
      '${id("lnc", "c")}', '${id("mch", "c")}', '${id("agt", "c")}',
      'codex', 1, 1, 'activated', clock_timestamp()
    );
    INSERT INTO target_owner_routes(
      target_kind, target_id, agent_id, machine_id, expected_launch_id,
      membership_epoch, routing_generation, route_version, row_version
    ) VALUES
      ('channel', '${id("chn", "a")}', '${id("agt", "c")}', '${id("mch", "c")}',
        '${id("lnc", "c")}', 1, 1, 1, 1),
      ('channel', '${id("chn", "b")}', '${id("agt", "c")}', '${id("mch", "c")}',
        '${id("lnc", "c")}', 1, 1, 1, 1);
  `);
});

after(async () => {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
  } finally {
    await session.close();
  }
});

test("migrate replays checksums without reapplying", async () => {
  const receipts = await store.migrate();
  assert.ok(receipts.length > 0 && receipts.every((receipt) => !receipt.applied));
});

test("guarded PostgreSQL reset rebuilds an isolated schema from forward migrations", async () => {
  const resetSchema = `swarm_storage_reset_${process.pid}`;
  const resetStore = new SharedStore(databaseUrl, resetSchema);
  await resetStore.migrate();
  const prior = process.env.NODE_ENV;
  process.env.NODE_ENV = "test";
  try {
    await resetStore.resetForTests();
    const replay = await resetStore.migrate();
    assert.ok(replay.length > 0 && replay.every((receipt) => !receipt.applied));
  } finally {
    if (prior === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = prior;
    const session = await PsqlSession.open(databaseUrl);
    try {
      await session.execute(`DROP SCHEMA IF EXISTS ${resetSchema} CASCADE;`);
    } finally {
      await session.close();
    }
  }
});

test("corrupted PostgreSQL migration checksum fails closed", async () => {
  const corruptSchema = `swarm_storage_corrupt_${process.pid}`;
  const corruptStore = new SharedStore(databaseUrl, corruptSchema);
  await corruptStore.migrate();
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(
      `UPDATE ${corruptSchema}.schema_migrations SET checksum = '${"0".repeat(64)}';`,
    );
  } finally {
    await session.close();
  }
  try {
    await assert.rejects(
      corruptStore.migrate(),
      (error: unknown) => error instanceof StorageError && error.code === "MIGRATION_CHECKSUM_MISMATCH",
    );
  } finally {
    const cleanup = await PsqlSession.open(databaseUrl);
    try {
      await cleanup.execute(`DROP SCHEMA IF EXISTS ${corruptSchema} CASCADE;`);
    } finally {
      await cleanup.close();
    }
  }
});

test("concurrent first appenders allocate sequence 1 and 2 exactly once", async () => {
  const values = await Promise.all([
    store.transaction(request("b"), async (transaction) => {
      const result = await transaction.messages.append(humanAppend("a", channelTarget("b"), "first", "a"));
      return { protocolVersion: 1, sequence: result.targetSeq };
    }),
    store.transaction(request("c"), async (transaction) => {
      const result = await transaction.messages.append(humanAppend("b", channelTarget("b"), "second", "b"));
      return { protocolVersion: 1, sequence: result.targetSeq };
    }),
  ]);
  assert.deepEqual(values.map((value) => Number(value.result.sequence)).sort(), [1, 2]);
});

test("same request and digest returns one committed canonical result", async () => {
  const same = request("d");
  const execute = () => store.transaction(same, async (transaction) => {
    const result = await transaction.messages.append(humanAppend("c", channelTarget("a"), "once", "c"));
    return { protocolVersion: 1, messageId: result.messageId, sequence: result.targetSeq };
  });
  const [left, right] = await Promise.all([execute(), execute()]);
  assert.deepEqual(left.result, right.result);
  assert.equal([left.replayed, right.replayed].filter(Boolean).length, 1);
  assert.equal(await raw(`SELECT count(*) FROM messages WHERE producer_fact_id = '${id("fac", "c")}'`), "1");
});

test("same request key with a different digest conflicts", async () => {
  const prior = request("e");
  await store.transaction(prior, async () => ({ protocolVersion: 1, value: "winner" }));
  await assert.rejects(
    store.transaction(
      { ...prior, requestDigest: `sha256:${"f".repeat(64)}` as ArtifactDigest },
      async () => ({ protocolVersion: 1, value: "loser" }),
    ),
    (error: unknown) => error instanceof StorageError && error.code === "IDEMPOTENCY_CONFLICT",
  );
});

test("receipt followed by injected failure rolls back the whole callback transaction", async () => {
  const receipt: TransitionReceipt = {
    protocolVersion: 1 as ProtocolVersion,
    receiptId: id("rcp", "a") as ReceiptId,
    kind: "server_accepted",
    producerFactId: id("fac", "d") as ProducerFactId,
    actor: { serverId: id("srv", "a") as ServerId },
    fence: {},
    occurredAt: "2026-08-06T16:00:00.000Z",
  };
  await assert.rejects(
    store.transaction(request("f", "receipt.record.v1"), async (transaction) => {
      await transaction.receipts.record(canonicalProtocolJson(receipt), receiptDigest(receipt));
      throw new Error("seeded failure after receipt before outbox");
    }),
    /seeded failure/u,
  );
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "a")}'`), "0");
});

test("logical receipt replay returns the winner for a fresh wire ID and rejects changed content", async () => {
  const original: TransitionReceipt = {
    protocolVersion: 1 as ProtocolVersion,
    receiptId: id("rcp", "b") as ReceiptId,
    kind: "server_accepted",
    producerFactId: id("fac", "m") as ProducerFactId,
    actor: { serverId: id("srv", "a") as ServerId },
    fence: {},
    occurredAt: "2026-08-06T16:01:00.000Z",
  };
  const first = await store.transaction(request("m", "receipt.record.v1"), async (transaction) => ({
    protocolVersion: 1,
    recorded: await transaction.receipts.record(canonicalProtocolJson(original), receiptDigest(original)),
  }));
  assert.equal(first.result.recorded.replayed, false);
  const replay = await store.transaction(request("r", "receipt.record.v1"), async (transaction) => ({
    protocolVersion: 1,
    recorded: await transaction.receipts.record(canonicalProtocolJson(original), receiptDigest(original)),
  }));
  assert.equal(replay.result.recorded.replayed, true);
  const freshIdentity = { ...original, receiptId: id("rcp", "c") as ReceiptId };
  const fresh = await store.transaction(request("s", "receipt.record.v1"), async (transaction) => ({
    protocolVersion: 1,
    recorded: await transaction.receipts.record(
      canonicalProtocolJson(freshIdentity),
      receiptDigest(freshIdentity),
    ),
  }));
  assert.deepEqual(fresh.result.recorded, { receiptId: original.receiptId, replayed: true });
  const conflicting = {
    ...freshIdentity,
    receiptId: id("rcp", "h") as ReceiptId,
    occurredAt: "2026-08-06T16:01:01.000Z",
  };
  await assert.rejects(
    store.transaction(request("9", "receipt.record.v1"), async (transaction) => ({
      protocolVersion: 1,
      recorded: await transaction.receipts.record(
        canonicalProtocolJson(conflicting),
        receiptDigest(conflicting),
      ),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "IDEMPOTENCY_CONFLICT",
  );
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE producer_fact_id = '${id("fac", "m")}'`), "1");
});

test("a superseded claim fence cannot mutate or validate after reacquisition", async () => {
  await raw(`INSERT INTO tasks(task_id, server_id, task_number, status)
    VALUES ('${id("tsk", "c")}', '${id("srv", "a")}', 3, 'todo');`);
  const first = await store.transaction(request("t", "claim.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.claims.acquire({
      taskId: id("tsk", "c"),
      claimId: id("clm", "a"),
      leaseId: id("lse", "a"),
      fenceToken: id("fnc", "a"),
      attempt: 1,
      ownerAgentId: id("agt", "a"),
      expiresAt: "2099-01-01T00:00:00.000Z",
    }),
  }));
  await store.transaction(request("v", "claim.mutate.v1"), async (transaction) => {
    await transaction.claims.release(first.result.lease, "seeded_reacquire");
    return { protocolVersion: 1 };
  });
  const second = await store.transaction(request("w", "claim.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.claims.acquire({
      taskId: id("tsk", "c"),
      claimId: id("clm", "b"),
      leaseId: id("lse", "b"),
      fenceToken: id("fnc", "b"),
      attempt: 2,
      ownerAgentId: id("agt", "b"),
      expiresAt: "2099-01-01T00:00:00.000Z",
    }),
  }));
  assert.equal(second.result.lease.leaseEpoch, first.result.lease.leaseEpoch + 1);
  await assert.rejects(
    store.transaction(request("x", "claim.mutate.v1"), async (transaction) => {
      await transaction.claims.assertCurrent(first.result.lease);
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_FENCE",
  );
});

test("concurrent claim acquisitions serialize to one open fence", async () => {
  await raw(`INSERT INTO tasks(task_id, server_id, task_number, status)
    VALUES ('${id("tsk", "d")}', '${id("srv", "a")}', 4, 'todo');`);
  const acquire = (character: string, owner: string) => store.transaction(
    request(character, "claim.mutate.v1"),
    async (transaction) => ({
      protocolVersion: 1,
      lease: await transaction.claims.acquire({
        taskId: id("tsk", "d"),
        claimId: id("clm", character),
        leaseId: id("lse", character),
        fenceToken: id("fnc", character),
        attempt: 1,
        ownerAgentId: id("agt", owner),
        expiresAt: "2099-01-01T00:00:00.000Z",
      }),
    }),
  );
  const settled = await Promise.allSettled([
    acquire("c", "a"),
    acquire("d", "b"),
  ]);
  assert.equal(settled.filter((item) => item.status === "fulfilled").length, 1);
  const rejected = settled.find((item) => item.status === "rejected");
  assert.ok(rejected?.status === "rejected" && rejected.reason instanceof StorageError);
  assert.equal(rejected.reason.code, "STALE_FENCE");
  assert.equal(await raw(`SELECT count(*) FROM task_claims WHERE task_id = '${id("tsk", "d")}' AND released_at IS NULL`), "1");
});

test("legacy delivery replay and daemon acceptance preserve immutable lineage", async () => {
  const prepared = await store.transaction(request("y", "delivery.mutate.v1"), async (transaction) => {
    const message = await transaction.messages.append(humanAppend("f", channelTarget("a"), "delivery fact", "g"));
    await transaction.launches.create({
      launchId: id("lnc", "a"),
      machineId: id("mch", "a"),
      agentId: id("agt", "a"),
      runtimeKind: "codex",
      workspaceGeneration: 1,
      routingGeneration: 1,
    });
    const envelope: DeliveryEnvelope = {
      protocolVersion: 1 as ProtocolVersion,
      deliveryId: id("dlv", "a") as DeliveryId,
      attempt: 1,
      messageId: id("msg", "f") as MessageId,
      target: channelTarget("a"),
      serverSeq: message.targetSeq,
      producerFactId: id("fac", "g") as ProducerFactId,
      agentId: id("agt", "a") as AgentId,
      machineId: id("mch", "a") as MachineId,
      expectedLaunchId: id("lnc", "a") as LaunchId,
    };
    await transaction.deliveries.create(canonicalProtocolJson(envelope));
    return { protocolVersion: 1, sequence: message.targetSeq };
  });

  const wrongActor: TransitionReceipt = {
    protocolVersion: 1 as ProtocolVersion,
    receiptId: id("rcp", "d") as ReceiptId,
    kind: "daemon_accepted",
    producerFactId: id("fac", "g") as ProducerFactId,
    actor: {
      machineId: id("mch", "b") as MachineId,
      agentId: id("agt", "a") as AgentId,
    },
    fence: {},
    occurredAt: "2026-08-06T16:02:00.000Z",
  };
  await assert.rejects(
    store.transaction(request("z", "delivery.mutate.v1"), async (transaction) => {
      await transaction.deliveries.advanceBoundary({
        deliveryId: id("dlv", "a"),
        to: "daemon_accepted",
        receiptBytes: canonicalProtocolJson(wrongActor),
        receiptDigest: receiptDigest(wrongActor),
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "INVALID_STATE_TRANSITION",
  );

  const accepted: TransitionReceipt = {
    ...wrongActor,
    receiptId: id("rcp", "e") as ReceiptId,
    actor: {
      machineId: id("mch", "a") as MachineId,
      agentId: id("agt", "a") as AgentId,
    },
  };
  await store.transaction(request("2", "delivery.mutate.v1"), async (transaction) => {
    await transaction.deliveries.advanceBoundary({
      deliveryId: id("dlv", "a"),
      to: "daemon_accepted",
      receiptBytes: canonicalProtocolJson(accepted),
      receiptDigest: receiptDigest(accepted),
    });
    return { protocolVersion: 1 };
  });

  const wrongLaunch: TransitionReceipt = {
    protocolVersion: 1 as ProtocolVersion,
    receiptId: id("rcp", "f") as ReceiptId,
    kind: "input_written",
    producerFactId: id("fac", "g") as ProducerFactId,
    actor: {
      machineId: id("mch", "a") as MachineId,
      agentId: id("agt", "a") as AgentId,
    },
    fence: {
      launchId: id("lnc", "b") as LaunchId,
      stateInstanceId: id("sti", "a"),
      turnId: id("trn", "a"),
      sessionId: id("ses", "a"),
    },
    occurredAt: "2026-08-06T16:03:00.000Z",
  } as TransitionReceipt;
  await assert.rejects(
    store.transaction(request("3", "delivery.mutate.v1"), async (transaction) => {
      await transaction.deliveries.advanceBoundary({
        deliveryId: id("dlv", "a"),
        to: "input_written",
        receiptBytes: canonicalProtocolJson(wrongLaunch),
        receiptDigest: receiptDigest(wrongLaunch),
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_FENCE",
  );

  const wrongReplay: DeliveryEnvelope = {
    protocolVersion: 1 as ProtocolVersion,
    deliveryId: id("dlv", "b") as DeliveryId,
    attempt: 2,
    messageId: id("msg", "f") as MessageId,
    target: channelTarget("a"),
    serverSeq: prepared.result.sequence,
    producerFactId: id("fac", "g") as ProducerFactId,
    agentId: id("agt", "b") as AgentId,
    machineId: id("mch", "a") as MachineId,
    expectedLaunchId: id("lnc", "a") as LaunchId,
    replayOf: id("dlv", "a") as DeliveryId,
  };
  await assert.rejects(
    store.transaction(request("5", "delivery.mutate.v1"), async (transaction) => ({
      protocolVersion: 1,
      delivery: await transaction.deliveries.create(canonicalProtocolJson(wrongReplay)),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "INVALID_STATE_TRANSITION",
  );
  const replay = {
    ...wrongReplay,
    agentId: id("agt", "a") as AgentId,
  };
  await store.transaction(request("6", "delivery.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    delivery: await transaction.deliveries.create(canonicalProtocolJson(replay)),
  }));
  assert.equal(await raw(`SELECT count(*) FROM deliveries WHERE producer_fact_id = '${id("fac", "g")}'`), "3");
});

test("route repository binds an active membership epoch to an activated launch", async () => {
  await raw(`DELETE FROM target_owner_routes WHERE target_kind = 'channel'
    AND target_id = '${id("chn", "a")}' AND agent_id = '${id("agt", "c")}'`);
  await store.transaction(request("0", "launch.mutate.v1"), async (transaction) => {
    await transaction.launches.transition({ launchId: id("lnc", "a"), from: "requested", to: "ready" });
    await transaction.launches.transition({ launchId: id("lnc", "a"), from: "ready", to: "activated" });
    return { protocolVersion: 1 };
  });
  await raw(`INSERT INTO memberships(channel_id, actor_kind, actor_id, state)
    VALUES ('${id("chn", "a")}', 'agent', '${id("agt", "a")}', 'active');`);
  const routed = await store.transaction(request("1", "route.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    routeVersion: await transaction.routes.compareAndSet({
      target: channelTarget("a"),
      agentId: id("agt", "a"),
      machineId: id("mch", "a"),
      expectedLaunchId: id("lnc", "a"),
      membershipEpoch: 1,
      routingGeneration: 1,
      expectedRouteVersion: 0,
    }),
  }));
  assert.equal(routed.result.routeVersion, 1);
  await raw(`UPDATE memberships SET state = 'removed', membership_epoch = 2, row_version = 1
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'agent' AND actor_id = '${id("agt", "a")}';`);
  await assert.rejects(
    store.transaction(request("4", "route.mutate.v1"), async (transaction) => ({
      protocolVersion: 1,
      routeVersion: await transaction.routes.compareAndSet({
        target: channelTarget("a"),
        agentId: id("agt", "a"),
        machineId: id("mch", "a"),
        expectedLaunchId: id("lnc", "a"),
        membershipEpoch: 2,
        routingGeneration: 1,
        expectedRouteVersion: 1,
      }),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "ROUTE_SUPERSEDED_BEFORE_CONSUME",
  );
});

test("task command locks its exact source, replays stored result, and rejects a second command", async () => {
  await raw(`
    INSERT INTO receipts(
      receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id,
      launch_id, state_instance_id, turn_id, session_id, occurred_at,
      detail_json, receipt_digest, effect_kind, effect_message_id
    ) VALUES (
      '${id("rcp", "p")}', '${id("fac", "g")}', 'side_effect_applied', '${id("mch", "a")}',
      '${id("agt", "a")}', '${id("lnc", "a")}', '${id("sti", "p")}', '${id("trn", "p")}',
      '${id("ses", "p")}', clock_timestamp(), '{"protocolVersion":1}', 'sha256:${"8".repeat(64)}',
      'reply_committed', '${id("msg", "f")}'
    );
  `);
  const commandInput = {
    commandId: id("cmd", "z"),
    requestDigest: `sha256:${"9".repeat(64)}` as ArtifactDigest,
    incomingProducerFactId: id("fac", "g"),
    sourceMessageId: id("msg", "f"),
    turnId: id("trn", "p"),
    taskId: id("tsk", "z"),
    title: "one flat task",
    serverId: id("srv", "a"),
    taskNumber: 99,
    result: { protocolVersion: 1, taskId: id("tsk", "z") },
  };
  const created = await store.transaction(request("z", "task.create.v1"), async (transaction) => ({
    protocolVersion: 1,
    recorded: await transaction.messages.createTask({
      ...commandInput,
      receiptId: variantId("rcp", "z", "t"),
      machineId: id("mch", "a"), agentId: id("agt", "a"), launchId: id("lnc", "a"),
      stateInstanceId: id("sti", "p"), sessionId: id("ses", "p"),
      leaseEpoch: 1, fenceToken: id("fnc", "z"),
      occurredAt: "2026-08-08T00:00:00.000Z",
    }),
  }));
  assert.equal(created.result.recorded.replayed, false);
  const replay = await store.transaction(request("y", "task.create.v1"), async (transaction) => ({
    protocolVersion: 1,
    recorded: await transaction.messages.createTask({
      ...commandInput,
      receiptId: variantId("rcp", "z", "t"),
      machineId: id("mch", "a"), agentId: id("agt", "a"), launchId: id("lnc", "a"),
      stateInstanceId: id("sti", "p"), sessionId: id("ses", "p"),
      leaseEpoch: 1, fenceToken: id("fnc", "z"),
      occurredAt: "2026-08-08T00:00:00.000Z",
    }),
  }));
  assert.equal(replay.result.recorded.replayed, true);
  assert.deepEqual(replay.result.recorded.result, commandInput.result);
  await assert.rejects(
    store.transaction(request("x", "task.create.v1"), async (transaction) => ({
      protocolVersion: 1,
      recorded: await transaction.messages.createTask({
        ...commandInput,
        commandId: id("cmd", "x"),
        taskId: id("tsk", "x"),
        taskNumber: 100,
        result: { protocolVersion: 1, taskId: id("tsk", "x") },
        receiptId: variantId("rcp", "x", "t"),
        machineId: id("mch", "a"), agentId: id("agt", "a"), launchId: id("lnc", "a"),
        stateInstanceId: id("sti", "p"), sessionId: id("ses", "p"),
        leaseEpoch: 1, fenceToken: id("fnc", "x"),
        occurredAt: "2026-08-08T00:00:00.000Z",
      }),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "SECOND_COORDINATION_CALL",
  );
});

test("parent and sibling thread aggregate state remain distinct", async () => {
  const actor = id("agt", "a");
  const targets = [
    channelTarget("a"),
    channelTarget("a", id("msg", "d")),
    channelTarget("a", id("msg", "e")),
  ];
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`BEGIN; SET LOCAL search_path TO ${schema}, pg_catalog;`);
    for (const [index, item] of targets.entries()) {
      const owner = item.kind === "channel" ? item.channelId : "";
      await session.execute(`INSERT INTO agent_message_state (
        agent_id, target_kind, target_id, thread_root_message_id, pending_server_seq
      ) VALUES (
        '${actor}', 'channel', '${owner}',
        ${item.threadRootMessageId === undefined ? "NULL" : `'${item.threadRootMessageId}'`},
        ${index + 1}
      );`);
    }
    await session.execute("COMMIT;");
  } finally {
    await session.close();
  }
  assert.equal(await raw(`SELECT count(*) FROM agent_message_state WHERE agent_id = '${actor}'`), "3");
});

test("reciprocal edge insertions serialize and exactly one cycle is rejected", async () => {
  await raw(`
    INSERT INTO tasks(task_id, server_id, task_number, status)
      VALUES ('${id("tsk", "a")}', '${id("srv", "a")}', 1, 'todo');
    INSERT INTO tasks(task_id, server_id, task_number, status)
      VALUES ('${id("tsk", "b")}', '${id("srv", "a")}', 2, 'todo');
  `);
  const edge = (character: string, parent: string, child: string) =>
    store.transaction(request(character, "task_graph.mutate.v1"), async (transaction) => {
      const revision = await transaction.taskGraph.addEdge({
        serverId: id("srv", "a"),
        parentTaskId: parent,
        childTaskId: child,
      });
      return { protocolVersion: 1, revision };
    });
  const settled = await Promise.allSettled([
    edge("g", id("tsk", "a"), id("tsk", "b")),
    edge("h", id("tsk", "b"), id("tsk", "a")),
  ]);
  assert.equal(settled.filter((item) => item.status === "fulfilled").length, 1);
  const rejected = settled.find((item) => item.status === "rejected");
  assert.ok(rejected?.status === "rejected" && rejected.reason instanceof StorageError);
  assert.equal(rejected.reason.code, "TASK_GRAPH_CYCLE");
  assert.equal(await raw("SELECT count(*) FROM task_edges"), "1");
  assert.equal(await raw(`SELECT graph_revision FROM task_graphs WHERE server_id = '${id("srv", "a")}'`), "1");
});

test("outbox workers lease disjoint jobs and a stale ACK is rejected", async () => {
  const actor: ReceiptActor = { serverId: id("srv", "a") as ServerId };
  for (const [index, character] of ["p", "q"].entries()) {
    await store.transaction(
      { ...request(character, "outbox.mutate.v1"), actor },
      async (transaction) => {
        const jobId = await transaction.outbox.enqueue({
          kind: "message_delivery",
          producerFactId: id("fac", ["e", "f"][index] ?? "g"),
          version: 1,
          payload: { protocolVersion: 1, index },
        });
        return { protocolVersion: 1, jobId };
      },
    );
  }
  const lease = (character: string) => store.transaction(
    request(character, "outbox.mutate.v1"),
    async (transaction) => ({
      protocolVersion: 1,
      jobs: await transaction.outbox.leaseBatch({
        workerLeaseId: id("lse", character),
        leaseUntil: "2099-01-01T00:00:00.000Z",
        limit: 1,
      }),
    }),
  );
  const [left, right] = await Promise.all([lease("k"), lease("m")]);
  const jobs = [...left.result.jobs, ...right.result.jobs];
  assert.equal(new Set(jobs.map((job) => job.jobId)).size, 2);
  const first = left.result.jobs[0] ?? right.result.jobs[0];
  const firstLeaseId = left.result.jobs.length > 0 ? id("lse", "k") : id("lse", "m");
  assert.ok(first !== undefined);
  await assert.rejects(
    store.transaction(request("n", "outbox.mutate.v1"), async (transaction) => {
      await transaction.outbox.ack({
        jobId: first.jobId,
        attempt: first.attempt + 1,
        workerLeaseId: firstLeaseId,
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "OUTBOX_STALE_ATTEMPT",
  );
});

test("derived outbox keys are equal across namespaces but remain isolated", async () => {
  const enqueue = (character: string, kind: "message_delivery" | "receipt_delivery") =>
    store.transaction(request(character, "outbox.mutate.v1"), async (transaction) => ({
      protocolVersion: 1,
      jobId: await transaction.outbox.enqueue({
        kind,
        producerFactId: id("fac", "k"),
        version: 1,
        payload: { protocolVersion: 1, logicalIdentity: "cross-namespace" },
      }),
    }));
  const left = await enqueue("a", "message_delivery");
  const right = await enqueue("b", "receipt_delivery");
  assert.notEqual(left.result.jobId, right.result.jobId);
  assert.equal(
    await raw(`SELECT count(DISTINCT idempotency_key) FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "k")}'`),
    "1",
  );
});

test("expired outbox lease is replayed with the same job and incremented attempt", async () => {
  await raw("UPDATE outbox_jobs SET status = 'dead' WHERE status = 'pending';");
  const created = await store.transaction(request("c", "outbox.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    jobId: await transaction.outbox.enqueue({
      kind: "reminder_fire",
      producerFactId: id("fac", "n"),
      version: 1,
      payload: { protocolVersion: 1, logicalIdentity: "expired-lease" },
    }),
  }));
  const first = await store.transaction(request("d", "outbox.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    jobs: await transaction.outbox.leaseBatch({
      workerLeaseId: id("lse", "e"),
      leaseUntil: "2000-01-01T00:00:00.000Z",
      limit: 1,
    }),
  }));
  const second = await store.transaction(request("e", "outbox.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    jobs: await transaction.outbox.leaseBatch({
      workerLeaseId: id("lse", "f"),
      leaseUntil: "2099-01-01T00:00:00.000Z",
      limit: 1,
    }),
  }));
  assert.equal(first.result.jobs[0]?.jobId, created.result.jobId);
  assert.equal(second.result.jobs[0]?.jobId, created.result.jobId);
  assert.equal(first.result.jobs[0]?.attempt, 1);
  assert.equal(second.result.jobs[0]?.attempt, 2);
  assert.equal(first.result.jobs[0]?.idempotencyKey, second.result.jobs[0]?.idempotencyKey);
});

test("outbox logical event rejects a changed payload under the same producer", async () => {
  const enqueue = (character: string, producer: string, value: string) => store.transaction(
    request(character, "outbox.mutate.v1"),
    async (transaction) => ({
      protocolVersion: 1,
      jobId: await transaction.outbox.enqueue({
        kind: "receipt_delivery",
        producerFactId: id("fac", producer),
        version: 1,
        payload: { protocolVersion: 1, value },
      }),
    }),
  );
  await enqueue("7", "h", "original");
  await assert.rejects(
    enqueue("8", "h", "changed"),
    (error: unknown) => error instanceof StorageError && error.code === "IDEMPOTENCY_CONFLICT",
  );
  assert.equal(await raw(`SELECT count(*) FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "h")}'`), "1");
});

test("PostgreSQL and protocol share the exact generated message-content truth table", async () => {
  for (const fixture of MESSAGE_BODY_CONTENT_FIXTURES) {
    assert.equal(messageBodyHasContent(fixture.body), fixture.hasContent, fixture.name);
    assert.equal(
      await raw(`SELECT message_body_has_content(${sqlLiteral(fixture.body)})`),
      fixture.hasContent ? "t" : "f",
      fixture.name,
    );
  }
});

test("reminder generations freeze distinct fire facts and a server-worker effect shape", async () => {
  const schedule = { protocolVersion: 1, kind: "once", at: "2099-01-01T00:00:00.000Z" };
  const first = await store.transaction(request("0", "reminder.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    generation: await transaction.reminders.advanceGeneration({
      reminderId: id("cmd", "m"),
      owner: { serverId: id("srv", "a") as ServerId },
      anchor: { target: "gate" },
      schedule,
      nextFireAt: schedule.at,
      expectedGeneration: 0,
      expectedHeadRowVersion: 0,
    }),
  }));
  const second = await store.transaction(request("1", "reminder.mutate.v1"), async (transaction) => ({
    protocolVersion: 1,
    generation: await transaction.reminders.advanceGeneration({
      reminderId: id("cmd", "m"),
      owner: { serverId: id("srv", "a") as ServerId },
      anchor: { target: "gate" },
      schedule,
      nextFireAt: schedule.at,
      expectedGeneration: 1,
      expectedHeadRowVersion: 0,
    }),
  }));
  assert.equal(first.result.generation, 1);
  assert.equal(second.result.generation, 2);
  assert.equal(
    await raw(`SELECT count(DISTINCT fire_producer_fact_id) FROM reminders WHERE reminder_id = '${id("cmd", "m")}'`),
    "2",
  );
  assert.equal(
    await raw(`SELECT current_generation || ':' || row_version FROM reminder_heads WHERE reminder_id = '${id("cmd", "m")}'`),
    "2:1",
  );
  await raw(`
    INSERT INTO receipts(
      receipt_id, producer_fact_id, kind, actor_server_id, occurred_at, detail_json,
      receipt_digest, effect_kind, effect_reminder_id, effect_reminder_generation
    ) SELECT
      '${id("rcp", "n")}', fire_producer_fact_id, 'side_effect_applied', '${id("srv", "a")}',
      clock_timestamp(), '{"protocolVersion":1}', 'sha256:${"7".repeat(64)}',
      'reminder_fired', reminder_id, generation
    FROM reminders WHERE reminder_id = '${id("cmd", "m")}' AND generation = 2;
  `);
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "n")}'`), "1");
});

test("native ingress repository advances not-written generation, rejects stale resume, and holds a second crash", async () => {
  const seed = await seedNativePermitCandidate("2");
  const invocation1: NativeInvocationFence = { invocationGeneration: 1, invocationId: seed.invocation1 };
  const invocation2: NativeInvocationFence = { invocationGeneration: 2, invocationId: seed.invocation2 };
  const journal1 = nativeJournal(seed, invocation1, "a");
  const journal2 = nativeJournal(seed, invocation2, "b");
  const acquire = acquirePermit(seed, "c");

  await store.transaction(deliveryRequest("2", "d"), async (transaction) => {
    const recorded = await transaction.nativeIngress.recordPermit({
      command: acquire,
      commandKind: "acquire",
      permitId: seed.permitId,
      resultInvocationGeneration: 1,
      resultInvocationId: seed.invocation1,
      resultWithoutBody: { invocation: invocation1, permitId: seed.permitId },
    });
    assert.deepEqual(recorded, { replayed: false, invocationGeneration: 1, invocationId: seed.invocation1 });
    return { protocolVersion: 1 };
  });
  assert.equal(
    await raw(`SELECT status || '/' || hold_reason FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "2")}'`),
    "held/CONSUME_PERMITTED",
  );

  await store.transaction(deliveryRequest("2", "e"), async (transaction) => {
    await transaction.nativeIngress.beginWrite(
      beginNativeWrite(seed, invocation1, journal1, "f"),
      { invocation: invocation1, status: "write_started" },
    );
    return { protocolVersion: 1 };
  });

  const proof = scriptedNotWrittenProof(seed, invocation1, journal1, "h");
  const notWritten = reconciliationCommand(seed, invocation1, {
    kind: "scripted_not_written",
    permitRecorded: journal1.permitRecorded,
    writeStarted: journal1.writeStarted,
    proof,
  }, "j");
  const notWrittenResult: ReconcileDeliveryResult = {
    kind: "same_attempt_resumable",
    jobState: "held/CONSUME_PERMITTED",
    attempt: 1,
    permitId: seed.permitId,
    resumeMode: "next_after_not_written",
    expectedActiveInvocationGeneration: 1,
    nextInvocationGeneration: 2,
  };
  await store.transaction(deliveryRequest("2", "k"), async (transaction) => {
    const recorded = await transaction.nativeIngress.recordReconciliationCommand(notWritten, notWrittenResult);
    assert.equal(recorded.replayed, false);
    return { protocolVersion: 1 };
  });

  const resumeNext = resumePermit(seed, "m", 1, "next_after_not_written");
  await store.transaction(deliveryRequest("2", "n"), async (transaction) => {
    const recorded = await transaction.nativeIngress.recordPermit({
      command: resumeNext,
      commandKind: "resume_next",
      permitId: seed.permitId,
      resultInvocationGeneration: 2,
      resultInvocationId: seed.invocation2,
      resultWithoutBody: { invocation: invocation2, permitId: seed.permitId },
      createdFromProofDigest: proof.proofDigest,
    });
    assert.deepEqual(recorded, { replayed: false, invocationGeneration: 2, invocationId: seed.invocation2 });
    return { protocolVersion: 1 };
  });
  assert.equal(
    await raw(`SELECT previous_invocation_generation || ':' || created_from_proof_digest
      FROM delivery_invocations WHERE delivery_id = '${id("dlv", "2")}' AND invocation_generation = 2`),
    `1:${proof.proofDigest}`,
  );
  assert.equal(
    await raw(`SELECT count(*) FROM delivery_permit_commands WHERE delivery_id = '${id("dlv", "2")}'`),
    "2",
  );

  await store.transaction(deliveryRequest("2", "p"), async (transaction) => {
    await transaction.nativeIngress.beginWrite(
      beginNativeWrite(seed, invocation2, journal2, "q"),
      { invocation: invocation2, status: "write_started" },
    );
    return { protocolVersion: 1 };
  });

  const beforeStaleResume = await nativePermitSnapshot("2");
  const staleResume = resumePermit(seed, "r", 1, "same_invocation_before_begin");
  await assert.rejects(
    store.transaction(deliveryRequest("2", "s"), async (transaction) => {
      await transaction.nativeIngress.recordPermit({
        command: staleResume,
        commandKind: "resume_same",
        permitId: seed.permitId,
        resultInvocationGeneration: 1,
        resultInvocationId: seed.invocation1,
        resultWithoutBody: { invocation: invocation1, permitId: seed.permitId },
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_INVOCATION_GENERATION",
  );
  assert.deepEqual(await nativePermitSnapshot("2"), beforeStaleResume);

  const beforeHistoricalReplay = await nativePermitSnapshot("2");
  await store.transaction(deliveryRequest("2", "t"), async (transaction) => {
    const replay = await transaction.nativeIngress.recordReconciliationCommand(notWritten, notWrittenResult);
    assert.equal(replay.replayed, true);
    assert.deepEqual(replay.result, notWrittenResult);
    return { protocolVersion: 1 };
  });
  assert.deepEqual(await nativePermitSnapshot("2"), beforeHistoricalReplay);

  const ambiguous = reconciliationCommand(seed, invocation2, {
    kind: "write_started_ambiguous",
    permitRecorded: journal2.permitRecorded,
    writeStarted: journal2.writeStarted,
    driverKind: "native_process",
  }, "v");
  const ambiguousResult: ReconcileDeliveryResult = {
    kind: "held_ambiguous",
    jobState: "held/AMBIGUOUS_NATIVE_WRITE",
    attempt: 1,
    permitId: seed.permitId,
    invocation: invocation2,
  };
  await store.transaction(deliveryRequest("2", "w"), async (transaction) => {
    await transaction.nativeIngress.recordReconciliationCommand(ambiguous, ambiguousResult);
    return { protocolVersion: 1 };
  });
  assert.equal(
    await raw(`SELECT string_agg(invocation_generation || ':' || status, ',' ORDER BY invocation_generation)
      FROM delivery_invocations WHERE delivery_id = '${id("dlv", "2")}'`),
    "1:not_written,2:ambiguous",
  );
  assert.equal(
    await raw(`SELECT status || '/' || hold_reason FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "2")}'`),
    "held/AMBIGUOUS_NATIVE_WRITE",
  );
  assert.equal(
    await raw(`SELECT active_invocation_generation FROM deliveries WHERE delivery_id = '${id("dlv", "2")}'`),
    "2",
  );
  assert.equal(
    await raw(`SELECT count(*) FROM delivery_invocations WHERE delivery_id = '${id("dlv", "2")}'
      AND invocation_generation = 3`),
    "0",
  );

  const beforeTerminalConflict = await nativePermitSnapshot("2");
  await assert.rejects(
    store.transaction(deliveryRequest("2", "x"), async (transaction) => {
      await transaction.nativeIngress.recordPermit({
        command: resumePermit(seed, "y", 2, "same_invocation_before_begin"),
        commandKind: "resume_same",
        permitId: seed.permitId,
        resultInvocationGeneration: 2,
        resultInvocationId: seed.invocation2,
        resultWithoutBody: { invocation: invocation2, permitId: seed.permitId },
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "INVOCATION_STATE_CONFLICT",
  );
  assert.deepEqual(await nativePermitSnapshot("2"), beforeTerminalConflict);
});

test("native ingress repository rejects a fresh generation after membership revocation with zero sibling mutation", async () => {
  const seed = await seedNativePermitCandidate("3");
  const invocation: NativeInvocationFence = { invocationGeneration: 1, invocationId: seed.invocation1 };
  await store.transaction(deliveryRequest("3", "a"), async (transaction) => {
    await transaction.nativeIngress.recordPermit({
      command: acquirePermit(seed, "b"),
      commandKind: "acquire",
      permitId: seed.permitId,
      resultInvocationGeneration: 1,
      resultInvocationId: seed.invocation1,
      resultWithoutBody: { invocation, permitId: seed.permitId },
    });
    return { protocolVersion: 1 };
  });
  await raw(`UPDATE memberships SET state = 'removed', membership_epoch = 2, row_version = 2
    WHERE channel_id = '${id("chn", "3")}' AND actor_kind = 'agent' AND actor_id = '${id("agt", "3")}'`);
  const before = await nativePermitSnapshot("3");
  await assert.rejects(
    store.transaction(deliveryRequest("3", "c"), async (transaction) => {
      await transaction.nativeIngress.recordPermit({
        command: resumePermit(seed, "d", 1, "same_invocation_before_begin"),
        commandKind: "resume_same",
        permitId: seed.permitId,
        resultInvocationGeneration: 1,
        resultInvocationId: seed.invocation1,
        resultWithoutBody: { invocation, permitId: seed.permitId },
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "MEMBERSHIP_REVOKED_BEFORE_CONSUME",
  );
  assert.deepEqual(await nativePermitSnapshot("3"), before);
});

test("native ingress repository rejects a fresh generation after route supersession with zero sibling mutation", async () => {
  const seed = await seedNativePermitCandidate("4");
  const invocation: NativeInvocationFence = { invocationGeneration: 1, invocationId: seed.invocation1 };
  await store.transaction(deliveryRequest("4", "a"), async (transaction) => {
    await transaction.nativeIngress.recordPermit({
      command: acquirePermit(seed, "b"),
      commandKind: "acquire",
      permitId: seed.permitId,
      resultInvocationGeneration: 1,
      resultInvocationId: seed.invocation1,
      resultWithoutBody: { invocation, permitId: seed.permitId },
    });
    return { protocolVersion: 1 };
  });
  await raw(`UPDATE target_owner_routes SET route_version = 2, row_version = 2
    WHERE target_kind = 'channel' AND target_id = '${id("chn", "4")}'`);
  const before = await nativePermitSnapshot("4");
  await assert.rejects(
    store.transaction(deliveryRequest("4", "c"), async (transaction) => {
      await transaction.nativeIngress.recordPermit({
        command: resumePermit(seed, "d", 1, "same_invocation_before_begin"),
        commandKind: "resume_same",
        permitId: seed.permitId,
        resultInvocationGeneration: 1,
        resultInvocationId: seed.invocation1,
        resultWithoutBody: { invocation, permitId: seed.permitId },
      });
      return { protocolVersion: 1 };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "ROUTE_SUPERSEDED_BEFORE_CONSUME",
  );
  assert.deepEqual(await nativePermitSnapshot("4"), before);
});

test("native ingress repository direct ACKs project input-written then model-visible", async () => {
  const seed = await seedNativePermitCandidate("6");
  const invocation: NativeInvocationFence = { invocationGeneration: 1, invocationId: seed.invocation1 };
  const journal = nativeJournal(seed, invocation, "a");
  await store.transaction(deliveryRequest("6", "b"), async (transaction) => {
    await transaction.nativeIngress.recordPermit({
      command: acquirePermit(seed, "c"),
      commandKind: "acquire",
      permitId: seed.permitId,
      resultInvocationGeneration: 1,
      resultInvocationId: seed.invocation1,
      resultWithoutBody: { invocation, permitId: seed.permitId },
    });
    return { protocolVersion: 1 };
  });
  await store.transaction(deliveryRequest("6", "d"), async (transaction) => {
    await transaction.nativeIngress.beginWrite(
      beginNativeWrite(seed, invocation, journal, "e"),
      { invocation, status: "write_started" },
    );
    return { protocolVersion: 1 };
  });

  for (const [index, boundary] of (["input_written", "model_visible"] as const).entries()) {
    const receiptId = variantId("rcp", "6", index === 0 ? "f" : "g") as ReceiptId;
    await store.transaction(deliveryRequest("6", index === 0 ? "h" : "j"), async (transaction) => {
      const recorded = await transaction.nativeIngress.recordAck({
        command: deliveryAck(seed, invocation, boundary, index === 0 ? "m" : "n"),
        receiptId,
        stateInstanceId: variantId("sti", "6", index === 0 ? "p" : "q") as StateInstanceId,
        occurredAt: `2026-08-07T19:2${index}:00.000Z`,
        resultBytes: deliveryAckBytes(invocation, receiptId, boundary),
      });
      assert.equal(recorded.replayed, false);
      assert.equal(recorded.aliased, false);
      return { protocolVersion: 1 };
    });
  }
  assert.equal(
    await raw(`SELECT status FROM delivery_invocations WHERE delivery_id = '${id("dlv", "6")}' AND invocation_generation = 1`),
    "model_visible",
  );
  assert.equal(await raw(`SELECT status FROM deliveries WHERE delivery_id = '${id("dlv", "6")}'`), "model_visible");
  assert.equal(await raw(`SELECT status FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "6")}'`), "acked");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE delivery_id = '${id("dlv", "6")}'`), "2");
  assert.equal(await raw(`SELECT count(*) FROM delivery_boundary_ack_results WHERE delivery_id = '${id("dlv", "6")}'`), "2");
});

test("native ingress repository reconciliation projects both repaired boundaries", async () => {
  const seed = await seedNativePermitCandidate("9");
  const invocation: NativeInvocationFence = { invocationGeneration: 1, invocationId: seed.invocation1 };
  const journal = nativeJournal(seed, invocation, "a");
  await store.transaction(deliveryRequest("9", "b"), async (transaction) => {
    await transaction.nativeIngress.recordPermit({
      command: acquirePermit(seed, "c"),
      commandKind: "acquire",
      permitId: seed.permitId,
      resultInvocationGeneration: 1,
      resultInvocationId: seed.invocation1,
      resultWithoutBody: { invocation, permitId: seed.permitId },
    });
    return { protocolVersion: 1 };
  });
  await store.transaction(deliveryRequest("9", "d"), async (transaction) => {
    await transaction.nativeIngress.beginWrite(
      beginNativeWrite(seed, invocation, journal, "e"),
      { invocation, status: "write_started" },
    );
    return { protocolVersion: 1 };
  });

  const reconcile = reconciliationCommand(seed, invocation, {
    kind: "model_visible",
    permitRecorded: journal.permitRecorded,
    writeStarted: journal.writeStarted,
    inputWritten: journal.inputWritten,
    modelVisible: journal.modelVisible,
  }, "f");
  const reconcileResult: ReconcileDeliveryResult = {
    kind: "boundary_repaired",
    repaired: ["input_written", "model_visible"],
    jobState: "acked/MODEL_VISIBLE",
    attempt: 1,
    permitId: seed.permitId,
    invocation,
  };
  await store.transaction(deliveryRequest("9", "g"), async (transaction) => {
    await transaction.nativeIngress.recordReconciliationCommand(reconcile, reconcileResult);
    for (const [index, boundary] of (["input_written", "model_visible"] as const).entries()) {
      const receiptId = variantId("rcp", "9", index === 0 ? "h" : "j") as ReceiptId;
      await transaction.nativeIngress.projectReconciledBoundary({
        command: reconcile,
        receiptId,
        stateInstanceId: variantId("sti", "9", index === 0 ? "k" : "m") as StateInstanceId,
        boundary,
        occurredAt: `2026-08-07T19:3${index}:00.000Z`,
        resultBytes: deliveryAckBytes(invocation, receiptId, boundary),
      });
    }
    return { protocolVersion: 1 };
  });
  assert.equal(
    await raw(`SELECT status FROM delivery_invocations WHERE delivery_id = '${id("dlv", "9")}' AND invocation_generation = 1`),
    "model_visible",
  );
  assert.equal(await raw(`SELECT status FROM deliveries WHERE delivery_id = '${id("dlv", "9")}'`), "model_visible");
  assert.equal(await raw(`SELECT status FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "9")}'`), "acked");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE delivery_id = '${id("dlv", "9")}'`), "2");
  assert.equal(await raw(`SELECT count(*) FROM delivery_boundary_ack_results WHERE delivery_id = '${id("dlv", "9")}'`), "2");
});

test("direct ACK closes the deferred creator-projection-receipt cycle", async () => {
  await seedNativeAttempt("q", 901);
  await raw(`
    BEGIN;
    ${ackCommandInsert("q", "input_written")};
    ${boundaryReceiptInsert("q", "input_written", "ack")};
    ${boundaryProjectionInsert("q", "input_written")};
    COMMIT;
  `);
  assert.equal(await raw(`SELECT count(*) FROM delivery_ack_commands WHERE command_id = '${id("cmd", "q")}'`), "1");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "q")}'`), "1");
  assert.equal(await raw(`SELECT count(*) FROM delivery_boundary_ack_results WHERE receipt_id = '${id("rcp", "q")}'`), "1");
});

test("reconciliation closes receipt-projection plus typed creator at commit", async () => {
  await seedNativeAttempt("r", 902);
  await raw(`
    BEGIN;
    ${reconciliationCommandInsert("r")};
    ${boundaryReceiptInsert("r", "model_visible", "reconciliation")};
    ${boundaryProjectionInsert("r", "model_visible")};
    COMMIT;
  `);
  assert.equal(await raw(`SELECT count(*) FROM delivery_reconciliation_commands WHERE command_id = '${id("cmd", "r")}'`), "1");
  assert.equal(await raw(`SELECT count(*) FROM delivery_boundary_ack_results WHERE receipt_id = '${id("rcp", "r")}'`), "1");
});

test("reconciliation creator must name the receipt's exact repaired boundary", async () => {
  await seedNativeAttempt("z", 906);
  await assert.rejects(
    raw(`
      BEGIN;
      ${reconciliationCommandInsert("z", ["input_written"])};
      ${boundaryReceiptInsert("z", "model_visible", "reconciliation")};
      ${boundaryProjectionInsert("z", "model_visible")};
      COMMIT;
    `),
    (error: unknown) => error instanceof StorageError && error.code === "DATABASE_UNAVAILABLE",
  );
  assert.equal(
    await raw(`SELECT count(*) FROM delivery_reconciliation_commands WHERE command_id = '${id("cmd", "z")}'`),
    "0",
  );
});

test("deferred cycle rejects receipt, projection, and creator omissions at commit", async () => {
  await seedNativeAttempt("s", 903);
  await assert.rejects(
    raw(`BEGIN; ${ackCommandInsert("s", "input_written")}; ${boundaryProjectionInsert("s", "input_written")}; COMMIT;`),
    (error: unknown) => error instanceof StorageError && error.code === "DATABASE_UNAVAILABLE",
  );
  assert.equal(await raw(`SELECT count(*) FROM delivery_ack_commands WHERE command_id = '${id("cmd", "s")}'`), "0");

  await seedNativeAttempt("t", 904);
  await assert.rejects(
    raw(`BEGIN; ${reconciliationCommandInsert("t")}; ${boundaryReceiptInsert("t", "input_written", "reconciliation")}; COMMIT;`),
    (error: unknown) => error instanceof StorageError && error.code === "DATABASE_UNAVAILABLE",
  );
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "t")}'`), "0");

  await seedNativeAttempt("v", 905);
  await assert.rejects(
    raw(`BEGIN; ${boundaryReceiptInsert("v", "input_written", "ack")}; ${boundaryProjectionInsert("v", "input_written")}; COMMIT;`),
    (error: unknown) => error instanceof StorageError && error.code === "DATABASE_UNAVAILABLE",
  );
  assert.equal(await raw(`SELECT count(*) FROM delivery_boundary_ack_results WHERE receipt_id = '${id("rcp", "v")}'`), "0");
});

test("MATCH SIMPLE admits only fully-null non-boundary tuples", async () => {
  await raw(`
    INSERT INTO receipts(
      receipt_id, producer_fact_id, kind, actor_server_id, occurred_at,
      detail_json, receipt_digest
    ) VALUES (
      '${id("rcp", "w")}', '${id("fac", "w")}', 'server_accepted', '${id("srv", "a")}',
      clock_timestamp(), '{"protocolVersion":1}', 'sha256:${"2".repeat(64)}'
    );
  `);
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "w")}'`), "1");
  await assert.rejects(
    raw(`
      INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_server_id, occurred_at,
        detail_json, receipt_digest, delivery_id
      ) VALUES (
        '${id("rcp", "x")}', '${id("fac", "x")}', 'server_accepted', '${id("srv", "a")}',
        clock_timestamp(), '{"protocolVersion":1}', 'sha256:${"3".repeat(64)}', '${id("dlv", "q")}'
      );
    `),
    (error: unknown) => error instanceof StorageError && error.code === "DATABASE_UNAVAILABLE",
  );
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "x")}'`), "0");
});

test("native boundary partial cardinality rejects a second canonical row", async () => {
  const duplicateResult = boundaryResultSql("y", "q", "input_written");
  await assert.rejects(
    raw(`
      BEGIN;
      INSERT INTO delivery_ack_commands(
        command_id, request_digest, delivery_id, attempt, permit_id,
        invocation_generation, invocation_id, boundary, canonical_receipt_id
      ) VALUES (
        '${id("cmd", "y")}', 'sha256:${"4".repeat(64)}', '${id("dlv", "q")}', 1,
        '${id("cmd", "q")}', 1, '${id("cmd", "q")}', 'input_written', '${id("rcp", "y")}'
      );
      INSERT INTO receipts(
        receipt_id, producer_fact_id, kind, actor_machine_id, actor_agent_id,
        launch_id, state_instance_id, turn_id, session_id, occurred_at,
        detail_json, receipt_digest, delivery_id, attempt, permit_id,
        invocation_generation, invocation_id, boundary, boundary_ack_command_id
      ) VALUES (
        '${id("rcp", "y")}', '${id("fac", "q")}', 'input_written', '${id("mch", "a")}',
        '${id("agt", "a")}', '${id("lnc", "a")}', '${id("sti", "y")}', '${id("trn", "q")}',
        '${id("ses", "y")}', clock_timestamp(), '{"protocolVersion":1}', 'sha256:${"5".repeat(64)}',
        '${id("dlv", "q")}', 1, '${id("cmd", "q")}', 1, '${id("cmd", "q")}',
        'input_written', '${id("cmd", "y")}'
      );
      INSERT INTO delivery_boundary_ack_results(
        receipt_id, delivery_id, attempt, permit_id, invocation_generation,
        invocation_id, boundary, result_json_bytes, result_digest
      ) VALUES (
        '${id("rcp", "y")}', '${id("dlv", "q")}', 1, '${id("cmd", "q")}', 1,
        '${id("cmd", "q")}', 'input_written', decode('${duplicateResult.bytes}', 'hex'), '${duplicateResult.digest}'
      );
      COMMIT;
    `),
    (error: unknown) => error instanceof StorageError && error.code === "DATABASE_UNAVAILABLE",
  );
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE receipt_id = '${id("rcp", "y")}'`), "0");
});
