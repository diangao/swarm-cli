import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { after, before, test } from "node:test";

import {
  canonicalProtocolJson,
  type AcquireConsumePermit,
  type ArtifactDigest,
  type BeginNativeWrite,
  type CommandId,
  type ReconcileDeliveryAttempt,
  type ReconcileDeliveryResult,
  type ServerId,
  type Target,
} from "@swarm/protocol";
import { PsqlSession, SharedStore, StorageError, appendHumanMessageDigest } from "../src/index.js";

const databaseUrl = process.env.DATABASE_URL ?? (() => { throw new Error("DATABASE_URL is required"); })();
const schema = `swarm_server_delivery_${process.pid}`;
const store = new SharedStore(databaseUrl, schema);

function id(prefix: string, character: string): string {
  return `${prefix}_${character.repeat(26)}`;
}

function variant(prefix: string, character: string, tail: string): string {
  return `${prefix}_${character.repeat(26 - tail.length)}${tail}`;
}

function digest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

const laneTarget = { kind: "channel", channelId: id("chn", "a") } as Target;

function request(character: string) {
  return {
    actor: { serverId: id("srv", "a") as ServerId },
    scope: "delivery.mutate.v1" as const,
    requestKind: "command" as const,
    requestId: id("cmd", character),
    requestDigest: digest({ request: character }),
  };
}

function appendInput(character: string, body: string) {
  const base = {
    protocolVersion: 1 as const,
    messageId: id("msg", character), target: laneTarget, humanId: id("hum", "a"), body,
    producerFactId: id("fac", character), serverId: id("srv", "a"),
  };
  return {
    ...base, requestDigest: appendHumanMessageDigest(base), receiptId: id("rcp", character),
    deliveryId: id("dlv", character), occurredAt: "2026-08-08T02:00:00.000Z",
  };
}

async function raw(sql: string): Promise<string> {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`SET search_path TO ${schema}, pg_catalog;`);
    return await session.execute(sql.trim().endsWith(";") ? sql : `${sql};`);
  } finally {
    await session.close();
  }
}

before(async () => {
  await store.migrate();
  await raw(`
    INSERT INTO servers(server_id, display_name) VALUES ('${id("srv", "a")}', 'Lane A');
    INSERT INTO machines(machine_id, server_id) VALUES ('${id("mch", "a")}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id) VALUES ('${id("agt", "a")}', '${id("srv", "a")}');
    INSERT INTO humans(human_id, server_id, display_name)
      VALUES ('${id("hum", "a")}', '${id("srv", "a")}', 'Author');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", "a")}', '${id("srv", "a")}', 'private', 'delivery');
    INSERT INTO memberships(channel_id, actor_kind, actor_id, state, membership_epoch, row_version) VALUES
      ('${id("chn", "a")}', 'human', '${id("hum", "a")}', 'active', 1, 1),
      ('${id("chn", "a")}', 'agent', '${id("agt", "a")}', 'active', 1, 1);
    INSERT INTO agent_launches(
      launch_id, machine_id, agent_id, runtime_kind, workspace_generation,
      routing_generation, state, activated_at
    ) VALUES ('${id("lnc", "a")}', '${id("mch", "a")}', '${id("agt", "a")}',
      'codex', 1, 1, 'activated', clock_timestamp());
    INSERT INTO target_owner_routes(
      target_kind, target_id, agent_id, machine_id, expected_launch_id,
      membership_epoch, routing_generation, route_version, row_version
    ) VALUES ('channel', '${id("chn", "a")}', '${id("agt", "a")}', '${id("mch", "a")}',
      '${id("lnc", "a")}', 1, 1, 1, 1);
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

test("only pre-permit reconciliation requeues the logical job and next lease creates attempt N+1", async () => {
  const appended = await store.transaction({ ...request("b"), scope: "message.append.v1" }, async (transaction) => ({
    protocolVersion: 1, value: await transaction.messages.append(appendInput("b", "lease lineage")),
  }));
  const payloadBefore = await raw(`SELECT payload_json::text FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`);
  const lease1 = await store.transaction(request("c"), async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.serverDelivery.lease({
      jobId: appended.result.value.outboxJobId,
      workerLeaseId: id("lse", "b"),
      leaseUntil: "2099-01-01T00:00:00.000Z",
    }),
  }));
  assert.equal(lease1.result.lease.attempt, 1);
  const commandBase = {
    protocolVersion: 1,
    deliveryId: id("dlv", "b"), attempt: 1, producerFactId: id("fac", "b"),
    agentId: id("agt", "a"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
    membershipEpoch: 1, routingGeneration: 1, routeVersion: 1,
    sessionId: id("ses", "b"), turnId: id("trn", "b"),
    commandId: variant("cmd", "b", "r"), permitId: null, invocation: null,
    evidenceDigest: digest({ kind: "pre_permit_disconnect", disconnectId: variant("cmd", "b", "d") }),
    evidence: { kind: "pre_permit_disconnect", disconnectId: variant("cmd", "b", "d") },
  };
  const command = { ...commandBase, requestDigest: digest(commandBase) } as ReconcileDeliveryAttempt;
  const result: ReconcileDeliveryResult = {
    kind: "pre_permit_requeued", jobState: "pending", replayOfAttempt: 1, nextAttempt: 2,
  };
  const reconciled = await store.transaction(request("d"), async (transaction) => ({
    protocolVersion: 1, value: await transaction.serverDelivery.reconcilePrePermit(command, result),
  }));
  assert.equal(reconciled.result.value.replayed, false);
  assert.equal(await raw(`SELECT status FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`), "pending");
  assert.equal(await raw(`SELECT status FROM deliveries WHERE delivery_id = '${id("dlv", "b")}'`), "canceled");
  assert.equal(await raw(`SELECT count(*) FROM deliveries WHERE outbox_job_id = ${appended.result.value.outboxJobId}`), "1");
  const lease2 = await store.transaction(request("e"), async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.serverDelivery.lease({
      jobId: appended.result.value.outboxJobId,
      workerLeaseId: id("lse", "c"), leaseUntil: "2099-01-01T00:00:00.000Z",
      nextDeliveryId: variant("dlv", "b", "2"),
    }),
  }));
  assert.equal(lease2.result.lease.attempt, 2);
  assert.equal(lease2.result.lease.replayOf, id("dlv", "b"));
  assert.equal(await raw(`SELECT payload_json::text FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`), payloadBefore);
  assert.equal(await raw(`SELECT count(*) FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "b")}'`), "1");
});

test("permit is the body linearization point and replay cannot return cached body after begin", async () => {
  const commandBase = {
    protocolVersion: 1,
    deliveryId: variant("dlv", "b", "2"), attempt: 2, producerFactId: id("fac", "b"),
    agentId: id("agt", "a"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
    membershipEpoch: 1, routingGeneration: 1, routeVersion: 1,
    sessionId: id("ses", "c"), turnId: id("trn", "c"),
    commandId: variant("cmd", "c", "p"), boundary: "daemon_accepted" as const,
  };
  const command = { ...commandBase, requestDigest: digest(commandBase) } as AcquireConsumePermit;
  const permitInput = {
    command, commandKind: "acquire" as const, permitId: variant("cmd", "c", "q") as CommandId,
    resultInvocationGeneration: 1, resultInvocationId: variant("cmd", "c", "1") as CommandId,
    resultWithoutBody: { protocolVersion: 1, permitId: variant("cmd", "c", "q") },
  };
  const authorized = await store.transaction(request("f"), async (transaction) => ({
    protocolVersion: 1, value: await transaction.serverDelivery.acquireOrResume(permitInput),
  }));
  assert.equal(authorized.result.value.kind, "authorized");
  assert.equal(authorized.result.value.kind === "authorized" ? authorized.result.value.body : null, "lease lineage");
  assert.equal(await raw("SELECT status || ':' || hold_reason FROM outbox_jobs WHERE producer_fact_id = '" + id("fac", "b") + "'"), "held:CONSUME_PERMITTED");

  const beginBase = {
    ...commandBase,
    invocationGeneration: 1,
    invocationId: variant("cmd", "c", "1"),
    commandId: variant("cmd", "c", "w"),
    permitId: variant("cmd", "c", "q"),
    boundary: "write_started" as const,
    inputDigest: digest({ input: "lease lineage" }),
    writeStartedEntryId: variant("cmd", "c", "e"),
    writeStartedEntryDigest: digest({ entry: "write-started" }),
  };
  const begin = { ...beginBase, requestDigest: digest(beginBase) } as BeginNativeWrite;
  await store.transaction(request("g"), async (transaction) => ({
    protocolVersion: 1, value: await transaction.nativeIngress.beginWrite(begin, { protocolVersion: 1 }),
  }));
  await assert.rejects(
    store.transaction(request("h"), async (transaction) => ({
      protocolVersion: 1, value: await transaction.serverDelivery.acquireOrResume(permitInput),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_INVOCATION_GENERATION",
  );
});

test("membership revocation before permit terminally suppresses without revealing body", async () => {
  const appended = await store.transaction({ ...request("j"), scope: "message.append.v1" }, async (transaction) => ({
    protocolVersion: 1, value: await transaction.messages.append(appendInput("c", "private body")),
  }));
  await store.transaction(request("k"), async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.serverDelivery.lease({
      jobId: appended.result.value.outboxJobId, workerLeaseId: id("lse", "d"),
      leaseUntil: "2099-01-01T00:00:00.000Z",
    }),
  }));
  await raw(`UPDATE memberships SET state = 'removed', membership_epoch = 2, row_version = 2
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'agent' AND actor_id = '${id("agt", "a")}'`);
  const commandBase = {
    protocolVersion: 1,
    deliveryId: id("dlv", "c"), attempt: 1, producerFactId: id("fac", "c"),
    agentId: id("agt", "a"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
    membershipEpoch: 1, routingGeneration: 1, routeVersion: 1,
    sessionId: id("ses", "d"), turnId: id("trn", "d"),
    commandId: variant("cmd", "d", "p"), boundary: "daemon_accepted" as const,
  };
  const command = { ...commandBase, requestDigest: digest(commandBase) } as AcquireConsumePermit;
  const suppressed = await store.transaction(request("m"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.serverDelivery.acquireOrResume({
      command, commandKind: "acquire", permitId: variant("cmd", "d", "q") as CommandId,
      resultInvocationGeneration: 1, resultInvocationId: variant("cmd", "d", "1") as CommandId,
      resultWithoutBody: { protocolVersion: 1 },
    }),
  }));
  assert.deepEqual(suppressed.result.value, { kind: "suppressed", code: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" });
  assert.equal(await raw(`SELECT status || ':' || last_error FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`),
    "dead:MEMBERSHIP_REVOKED_BEFORE_CONSUME");
  assert.equal(await raw(`SELECT count(*) FROM delivery_permit_commands WHERE delivery_id = '${id("dlv", "c")}'`), "0");
});

test("route supersession before permit terminally suppresses the frozen route", async () => {
  await raw(`UPDATE memberships SET state = 'active', membership_epoch = 3, row_version = 3
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'agent' AND actor_id = '${id("agt", "a")}';
    UPDATE target_owner_routes SET membership_epoch = 3, route_version = 2, row_version = 2
    WHERE target_kind = 'channel' AND target_id = '${id("chn", "a")}'`);
  const appended = await store.transaction({ ...request("n"), scope: "message.append.v1" }, async (transaction) => ({
    protocolVersion: 1, value: await transaction.messages.append(appendInput("d", "route-private body")),
  }));
  await store.transaction(request("p"), async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.serverDelivery.lease({
      jobId: appended.result.value.outboxJobId, workerLeaseId: id("lse", "e"),
      leaseUntil: "2099-01-01T00:00:00.000Z",
    }),
  }));
  await raw(`UPDATE target_owner_routes SET route_version = 3, row_version = 3
    WHERE target_kind = 'channel' AND target_id = '${id("chn", "a")}'`);
  const commandBase = {
    protocolVersion: 1,
    deliveryId: id("dlv", "d"), attempt: 1, producerFactId: id("fac", "d"),
    agentId: id("agt", "a"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
    membershipEpoch: 3, routingGeneration: 1, routeVersion: 2,
    sessionId: id("ses", "e"), turnId: id("trn", "e"),
    commandId: variant("cmd", "e", "p"), boundary: "daemon_accepted" as const,
  };
  const command = { ...commandBase, requestDigest: digest(commandBase) } as AcquireConsumePermit;
  const suppressed = await store.transaction(request("r"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.serverDelivery.acquireOrResume({
      command, commandKind: "acquire", permitId: variant("cmd", "e", "q") as CommandId,
      resultInvocationGeneration: 1, resultInvocationId: variant("cmd", "e", "1") as CommandId,
      resultWithoutBody: { protocolVersion: 1 },
    }),
  }));
  assert.deepEqual(suppressed.result.value, { kind: "suppressed", code: "ROUTE_SUPERSEDED_BEFORE_CONSUME" });
  assert.equal(await raw(`SELECT status || ':' || last_error FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`),
    "dead:ROUTE_SUPERSEDED_BEFORE_CONSUME");
  assert.equal(await raw(`SELECT count(*) FROM delivery_permit_commands WHERE delivery_id = '${id("dlv", "d")}'`), "0");
});

test("an unleased logical job cannot acquire a consume permit through the public server path", async () => {
  const appended = await store.transaction({ ...request("s"), scope: "message.append.v1" }, async (transaction) => ({
    protocolVersion: 1, value: await transaction.messages.append(appendInput("e", "not leased")),
  }));
  const commandBase = {
    protocolVersion: 1,
    deliveryId: id("dlv", "e"), attempt: 1, producerFactId: id("fac", "e"),
    agentId: id("agt", "a"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
    membershipEpoch: 3, routingGeneration: 1, routeVersion: 3,
    sessionId: id("ses", "f"), turnId: id("trn", "f"),
    commandId: variant("cmd", "f", "p"), boundary: "daemon_accepted" as const,
  };
  const command = { ...commandBase, requestDigest: digest(commandBase) } as AcquireConsumePermit;
  await assert.rejects(
    store.transaction(request("t"), async (transaction) => ({
      protocolVersion: 1,
      value: await transaction.serverDelivery.acquireOrResume({
        command, commandKind: "acquire", permitId: variant("cmd", "f", "q") as CommandId,
        resultInvocationGeneration: 1, resultInvocationId: variant("cmd", "f", "1") as CommandId,
        resultWithoutBody: { protocolVersion: 1 },
      }),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_DELIVERY_FENCE",
  );
  assert.equal(await raw(`SELECT status FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`), "pending");
  assert.equal(await raw(`SELECT count(*) FROM delivery_permit_commands WHERE delivery_id = '${id("dlv", "e")}'`), "0");
});

test("a logical delivery job is not leasable before its immutable due time", async () => {
  const appended = await store.transaction({ ...request("v"), scope: "message.append.v1" }, async (transaction) => ({
    protocolVersion: 1, value: await transaction.messages.append(appendInput("f", "not due")),
  }));
  await raw(`UPDATE outbox_jobs SET due_at = '2098-01-01T00:00:00.000Z'
    WHERE job_id = ${appended.result.value.outboxJobId}`);
  await assert.rejects(
    store.transaction(request("w"), async (transaction) => ({
      protocolVersion: 1,
      lease: await transaction.serverDelivery.lease({
        jobId: appended.result.value.outboxJobId,
        workerLeaseId: id("lse", "f"),
        leaseUntil: "2099-01-01T00:00:00.000Z",
      }),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "OUTBOX_STALE_ATTEMPT",
  );
  assert.equal(await raw(`SELECT status FROM outbox_jobs WHERE job_id = ${appended.result.value.outboxJobId}`), "pending");
  assert.equal(await raw(`SELECT status FROM deliveries WHERE delivery_id = '${id("dlv", "f")}'`), "pending");
});
