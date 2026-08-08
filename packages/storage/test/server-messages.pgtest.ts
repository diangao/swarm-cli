import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { after, before, test } from "node:test";

import {
  canonicalProtocolJson,
  type AcquireConsumePermit,
  type ArtifactDigest,
  type BeginNativeWrite,
  type CommandId,
  type DeliveryAck,
  type ProtocolVersion,
  type ServerId,
  type StateInstanceId,
  type Target,
} from "@swarm/protocol";
import {
  PsqlSession,
  SharedStore,
  StorageError,
  appendHumanMessageDigest,
} from "../src/index.js";

const databaseUrl = process.env.DATABASE_URL ?? (() => { throw new Error("DATABASE_URL is required"); })();
const schema = `swarm_server_messages_${process.pid}`;
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

function target(): Target {
  return { kind: "channel", channelId: id("chn", "a") } as Target;
}

function request(character: string) {
  return {
    actor: { serverId: id("srv", "a") as ServerId },
    scope: "message.append.v1" as const,
    requestKind: "command" as const,
    requestId: id("cmd", character),
    requestDigest: digest({ request: character }),
  };
}

function appendInput(
  messageCharacter: string,
  body: string,
  producerCharacter = messageCharacter,
  humanCharacter = "a",
) {
  const base = {
    protocolVersion: 1 as const,
    messageId: id("msg", messageCharacter),
    target: target(),
    humanId: id("hum", humanCharacter),
    body,
    producerFactId: id("fac", producerCharacter),
    serverId: id("srv", "a"),
  };
  return {
    ...base,
    requestDigest: appendHumanMessageDigest(base),
    receiptId: id("rcp", messageCharacter),
    deliveryId: id("dlv", messageCharacter),
    occurredAt: "2026-08-08T01:00:00.000Z",
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
    INSERT INTO machines(machine_id, server_id) VALUES
      ('${id("mch", "a")}', '${id("srv", "a")}'),
      ('${id("mch", "b")}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id) VALUES
      ('${id("agt", "a")}', '${id("srv", "a")}'),
      ('${id("agt", "b")}', '${id("srv", "a")}');
    INSERT INTO humans(human_id, server_id, display_name) VALUES
      ('${id("hum", "a")}', '${id("srv", "a")}', 'Author'),
      ('${id("hum", "b")}', '${id("srv", "a")}', 'Reader'),
      ('${id("hum", "c")}', '${id("srv", "a")}', 'Non-member');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", "a")}', '${id("srv", "a")}', 'private', 'lane-a');
    INSERT INTO memberships(channel_id, actor_kind, actor_id, state, membership_epoch, row_version) VALUES
      ('${id("chn", "a")}', 'human', '${id("hum", "a")}', 'active', 1, 1),
      ('${id("chn", "a")}', 'human', '${id("hum", "b")}', 'active', 1, 1),
      ('${id("chn", "a")}', 'agent', '${id("agt", "a")}', 'active', 1, 1),
      ('${id("chn", "a")}', 'agent', '${id("agt", "b")}', 'active', 1, 1);
    INSERT INTO agent_launches(
      launch_id, machine_id, agent_id, runtime_kind, workspace_generation,
      routing_generation, state, activated_at
    ) VALUES (
      '${id("lnc", "a")}', '${id("mch", "a")}', '${id("agt", "a")}',
      'codex', 1, 1, 'activated', clock_timestamp()
    );
    INSERT INTO target_owner_routes(
      target_kind, target_id, agent_id, machine_id, expected_launch_id,
      membership_epoch, routing_generation, route_version, row_version
    ) VALUES (
      'channel', '${id("chn", "a")}', '${id("agt", "a")}', '${id("mch", "a")}',
      '${id("lnc", "a")}', 1, 1, 1, 1
    );
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

test("empty append fails before sequence allocation with zero siblings", async () => {
  await assert.rejects(
    store.transaction(request("b"), async (transaction) => ({
      protocolVersion: 1,
      appended: await transaction.messages.append(appendInput("b", "\u00a0\u2009\ufeff")),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "EMPTY_MESSAGE",
  );
  assert.equal(await raw("SELECT count(*) FROM target_sequences"), "0");
  assert.equal(await raw("SELECT count(*) FROM messages"), "0");
  assert.equal(await raw("SELECT count(*) FROM receipts"), "0");
  assert.equal(await raw("SELECT count(*) FROM deliveries"), "0");
  assert.equal(await raw("SELECT count(*) FROM outbox_jobs"), "0");
});

test("a registered non-member cannot append or allocate any sibling", async () => {
  await assert.rejects(
    store.transaction(request("x"), async (transaction) => ({
      protocolVersion: 1,
      appended: await transaction.messages.append(appendInput("x", "private", "x", "c")),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "MEMBERSHIP_REVOKED_BEFORE_CONSUME",
  );
  assert.equal(await raw("SELECT count(*) FROM target_sequences"), "0");
  assert.equal(await raw("SELECT count(*) FROM messages"), "0");
  assert.equal(await raw("SELECT count(*) FROM receipts"), "0");
  assert.equal(await raw("SELECT count(*) FROM deliveries"), "0");
  assert.equal(await raw("SELECT count(*) FROM outbox_jobs"), "0");
});

test("append freezes route and audience while logical outbox stays identifier-only", async () => {
  const exactBody = "  exact UTF-8 body\n";
  const appended = await store.transaction(request("c"), async (transaction) => ({
    protocolVersion: 1,
    appended: await transaction.messages.append(appendInput("c", exactBody)),
  }));
  assert.equal(appended.result.appended.targetSeq, 1);
  assert.equal(
    await raw(`SELECT encode(convert_to(body, 'UTF8'), 'hex') FROM messages WHERE message_id = '${id("msg", "c")}'`),
    Buffer.from(exactBody, "utf8").toString("hex"),
  );
  assert.equal(
    await raw(`SELECT NOT (payload_json ?| array['body','deliveryId','attempt'])
      FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "c")}'`),
    "t",
  );
  assert.equal(
    await raw(`SELECT string_agg(actor_kind || ':' || actor_id || ':' || audience_mode, ',' ORDER BY actor_kind, actor_id)
      FROM message_audience WHERE message_id = '${id("msg", "c")}'`),
    [
      `agent:${id("agt", "a")}:owner_body`,
      `agent:${id("agt", "b")}:attention_metadata`,
      `human:${id("hum", "a")}:member_body`,
      `human:${id("hum", "b")}:member_body`,
    ].join(","),
  );
  const read = await store.transaction(request("d"), async (transaction) => ({
    protocolVersion: 1,
    frame: await transaction.messages.readHumanBody({ target: target(), messageId: id("msg", "c"), humanId: id("hum", "b") }),
  }));
  assert.equal(read.result.frame?.body, exactBody);
  const attention = await store.transaction(request("e"), async (transaction) => ({
    protocolVersion: 1,
    attention: await transaction.messages.listAttention({ target: target(), agentId: id("agt", "b") }),
  }));
  assert.equal(attention.result.attention.length, 1);
  assert.equal(attention.result.attention[0]?.pendingCount, 1);
  assert.equal(Object.hasOwn(attention.result.attention[0] ?? {}, "body"), false);

  await raw(`UPDATE memberships SET state = 'removed', membership_epoch = 2, row_version = 2
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'agent' AND actor_id = '${id("agt", "b")}';
    UPDATE memberships SET state = 'active', membership_epoch = 3, row_version = 3
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'agent' AND actor_id = '${id("agt", "b")}'`);
  const resetAttention = await store.transaction(request("w"), async (transaction) => ({
    protocolVersion: 1,
    attention: await transaction.messages.listAttention({ target: target(), agentId: id("agt", "b") }),
  }));
  assert.deepEqual(resetAttention.result.attention, []);

  await raw(`UPDATE memberships SET state = 'removed', membership_epoch = 2, row_version = 2
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'human' AND actor_id = '${id("hum", "b")}';
    UPDATE memberships SET state = 'active', membership_epoch = 3, row_version = 3
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'human' AND actor_id = '${id("hum", "b")}';`);
  const blocked = await store.transaction(request("f"), async (transaction) => ({
    protocolVersion: 1,
    frame: await transaction.messages.readHumanBody({ target: target(), messageId: id("msg", "c"), humanId: id("hum", "b") }),
  }));
  assert.equal(blocked.result.frame, null);
});

test("outbox conflict rolls message, sequence, receipt, audience, route, and delivery back together", async () => {
  await raw(`INSERT INTO outbox_jobs(
    idempotency_namespace, idempotency_key, producer_fact_id,
    event_kind, event_version, payload_json, status
  ) VALUES (
    'message_delivery.v1', 'seeded-conflict', '${id("fac", "d")}',
    'message_delivery', 1, '{"protocolVersion":1}', 'pending'
  )`);
  await assert.rejects(store.transaction(request("g"), async (transaction) => ({
    protocolVersion: 1,
    appended: await transaction.messages.append(appendInput("d", "must roll back")),
  })));
  assert.equal(await raw(`SELECT count(*) FROM messages WHERE producer_fact_id = '${id("fac", "d")}'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE producer_fact_id = '${id("fac", "d")}'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM message_owner_routes WHERE producer_fact_id = '${id("fac", "d")}'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM deliveries WHERE producer_fact_id = '${id("fac", "d")}'`), "0");
  assert.equal(await raw("SELECT next_seq FROM target_sequences WHERE target_kind = 'channel'"), "2");
});

test("reply without exact model-visible predecessor leaves zero reply siblings", async () => {
  const replyBase = {
    protocolVersion: 1 as const,
    messageId: id("msg", "e"),
    producerFactId: id("fac", "e"),
    incomingMessageId: id("msg", "c"),
    incomingProducerFactId: id("fac", "c"),
    target: target(),
    body: "not yet",
    agentId: id("agt", "a"),
    turnId: id("trn", "e"),
  };
  await assert.rejects(
    store.transaction({ ...request("h"), scope: "message.reply.v1" }, async (transaction) => ({
      protocolVersion: 1,
      reply: await transaction.messages.appendReply({
        ...replyBase,
        requestDigest: digest(replyBase),
        receiptId: id("rcp", "e"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
        stateInstanceId: id("sti", "e"), sessionId: id("ses", "e"),
        occurredAt: "2026-08-08T01:10:00.000Z",
      }),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "MODEL_VISIBLE_PREDECESSOR_REQUIRED",
  );
  assert.equal(await raw(`SELECT count(*) FROM messages WHERE message_id = '${id("msg", "e")}'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "e")}'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE effect_message_id = '${id("msg", "e")}'`), "0");
  assert.equal(await raw("SELECT next_seq FROM target_sequences WHERE target_kind = 'channel'"), "2");
});

test("terminal model-visible predecessor serializes reply before the sole task effect", async () => {
  const incoming = await store.transaction({ ...request("j"), scope: "message.append.v1" }, async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.messages.append(appendInput("f", "please execute", "f")),
  }));
  await store.transaction({ ...request("k"), scope: "delivery.mutate.v1" }, async (transaction) => ({
    protocolVersion: 1,
    lease: await transaction.serverDelivery.lease({
      jobId: incoming.result.value.outboxJobId,
      workerLeaseId: id("lse", "f"), leaseUntil: "2099-01-01T00:00:00.000Z",
    }),
  }));
  const fence = {
    protocolVersion: 1 as ProtocolVersion,
    deliveryId: id("dlv", "f"), attempt: 1, producerFactId: id("fac", "f"),
    agentId: id("agt", "a"), machineId: id("mch", "a"), launchId: id("lnc", "a"),
    membershipEpoch: 1, routingGeneration: 1, routeVersion: 1,
    sessionId: id("ses", "f"), turnId: id("trn", "f"),
  };
  const acquireBase = { ...fence, commandId: variant("cmd", "f", "p"), boundary: "daemon_accepted" as const };
  const acquire = { ...acquireBase, requestDigest: digest(acquireBase) } as AcquireConsumePermit;
  const invocation = { invocationGeneration: 1, invocationId: variant("cmd", "f", "1") as CommandId };
  const permitId = variant("cmd", "f", "q") as CommandId;
  const permit = await store.transaction({ ...request("m"), scope: "delivery.mutate.v1" }, async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.serverDelivery.acquireOrResume({
      command: acquire, commandKind: "acquire", workerLeaseId: id("lse", "f"), permitId,
      resultInvocationGeneration: 1, resultInvocationId: invocation.invocationId,
      resultWithoutBody: { protocolVersion: 1, permitId },
    }),
  }));
  assert.equal(permit.result.value.kind === "authorized" ? permit.result.value.body : null, "please execute");
  const beginBase = {
    ...fence, ...invocation, commandId: variant("cmd", "f", "w"), permitId,
    boundary: "write_started" as const, inputDigest: digest({ body: "please execute" }),
    writeStartedEntryId: variant("cmd", "f", "e") as CommandId,
    writeStartedEntryDigest: digest({ entry: "write_started" }),
  };
  const begin = { ...beginBase, requestDigest: digest(beginBase) } as BeginNativeWrite;
  await store.transaction({ ...request("n"), scope: "delivery.mutate.v1" }, async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.nativeIngress.beginWrite(begin, { protocolVersion: 1 }),
  }));
  const ack = async (
    requestCharacter: string,
    commandCharacter: string,
    boundary: "input_written" | "model_visible",
    receiptCharacter: string,
  ) => {
    const commandBase = { ...fence, ...invocation, commandId: id("cmd", commandCharacter), permitId, boundary };
    const command = { ...commandBase, requestDigest: digest(commandBase) } as DeliveryAck;
    const resultBytes = canonicalProtocolJson({
      boundary, receiptId: id("rcp", receiptCharacter), invocation,
      jobState: boundary === "input_written" ? "held/INPUT_WRITTEN" : "acked/MODEL_VISIBLE",
    });
    return await store.transaction({ ...request(requestCharacter), scope: "delivery.mutate.v1" }, async (transaction) => ({
      protocolVersion: 1,
      value: await transaction.nativeIngress.recordAck({
        command, receiptId: id("rcp", receiptCharacter), stateInstanceId: id("sti", "f") as StateInstanceId,
        occurredAt: "2026-08-08T01:20:00.000Z", resultBytes,
      }),
    }));
  };
  await ack("p", "q", "input_written", "q");
  await ack("r", "s", "model_visible", "r");

  const replyBase = {
    protocolVersion: 1 as const, messageId: id("msg", "g"), producerFactId: id("fac", "g"),
    incomingMessageId: id("msg", "f"), incomingProducerFactId: id("fac", "f"),
    target: target(), body: "I did it", agentId: id("agt", "a"), turnId: id("trn", "f"),
  };
  const reply = await store.transaction({ ...request("t"), scope: "message.reply.v1" }, async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.messages.appendReply({
      ...replyBase, requestDigest: digest(replyBase), receiptId: id("rcp", "s"),
      machineId: id("mch", "a"), launchId: id("lnc", "a"), stateInstanceId: id("sti", "f"),
      sessionId: id("ses", "f"), occurredAt: "2026-08-08T01:21:00.000Z",
    }),
  }));
  assert.equal(reply.result.value.replayed, false);
  assert.equal(await raw(`SELECT count(*) FROM tasks WHERE source_producer_fact_id = '${id("fac", "f")}'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE producer_fact_id = '${id("fac", "f")}' AND effect_kind = 'task_created'`), "0");
  assert.equal(await raw(`SELECT count(*) FROM message_audience WHERE message_id = '${id("msg", "g")}' AND actor_kind = 'agent'`), "0");
  assert.equal(await raw(`SELECT NOT (payload_json ? 'body') FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "g")}'`), "t");

  const taskResult = { protocolVersion: 1, taskId: id("tsk", "a") };
  const created = await store.transaction({ ...request("v"), scope: "task.create.v1" }, async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.messages.createTask({
      commandId: id("cmd", "v"), requestDigest: digest({ command: "task-a" }),
      incomingProducerFactId: id("fac", "f"), sourceMessageId: id("msg", "f"),
      turnId: id("trn", "f"), taskId: id("tsk", "a"), title: "one flat task",
      serverId: id("srv", "a"), taskNumber: 1, result: taskResult,
      receiptId: id("rcp", "t"), machineId: id("mch", "a"), agentId: id("agt", "a"),
      launchId: id("lnc", "a"), stateInstanceId: id("sti", "f"), sessionId: id("ses", "f"),
      leaseEpoch: 1, fenceToken: id("fnc", "a"),
      occurredAt: "2026-08-08T01:22:00.000Z",
    }),
  }));
  assert.equal(created.result.value.replayed, false);
  assert.equal(
    await raw(`SELECT bool_and(reply_order < task_order) FROM (
      SELECT min(causal_order) FILTER (WHERE effect_kind = 'reply_committed') AS reply_order,
        min(causal_order) FILTER (WHERE effect_kind = 'task_created') AS task_order
      FROM receipts WHERE producer_fact_id = '${id("fac", "f")}'
    ) ordered`),
    "t",
  );
});

test("revoked human membership blocks both append replay layers without changing the canonical append", async () => {
  const input = appendInput("0", "immutable replay body");
  const original = await store.transaction(request("y"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.messages.append(input),
  }));
  assert.equal(original.result.value.replayed, false);
  const canonicalSnapshot = () => raw(`SELECT json_build_object(
    'message', (SELECT row_to_json(m) FROM messages m WHERE producer_fact_id = '${id("fac", "0")}'),
    'receipt', (SELECT row_to_json(r) FROM receipts r WHERE producer_fact_id = '${id("fac", "0")}'),
    'audience', (SELECT json_agg(row_to_json(a) ORDER BY actor_kind, actor_id)
      FROM message_audience a WHERE message_id = '${id("msg", "0")}'),
    'route', (SELECT row_to_json(mor) FROM message_owner_routes mor WHERE producer_fact_id = '${id("fac", "0")}'),
    'delivery', (SELECT row_to_json(d) FROM deliveries d WHERE producer_fact_id = '${id("fac", "0")}'),
    'outbox', (SELECT row_to_json(j) FROM outbox_jobs j WHERE producer_fact_id = '${id("fac", "0")}'),
    'nextSeq', (SELECT next_seq FROM target_sequences
      WHERE target_kind = 'channel' AND target_id = '${id("chn", "a")}' AND thread_root_message_id IS NULL),
    'commandRequests', (SELECT count(*) FROM command_requests)
  )::text`);
  const before = await canonicalSnapshot();
  await raw(`UPDATE memberships SET state = 'removed', membership_epoch = 2, row_version = 2
    WHERE channel_id = '${id("chn", "a")}' AND actor_kind = 'human' AND actor_id = '${id("hum", "a")}'`);

  await assert.rejects(
    store.transaction(request("z"), async (transaction) => ({
      protocolVersion: 1,
      value: await transaction.messages.append(input),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "MEMBERSHIP_REVOKED_BEFORE_CONSUME",
  );
  let outerReplayBodyReached = false;
  await assert.rejects(
    store.transaction(request("y"), async () => {
      outerReplayBodyReached = true;
      throw new Error("outer append replay must authorize from stored canonical facts");
    }),
    (error: unknown) => error instanceof StorageError && error.code === "MEMBERSHIP_REVOKED_BEFORE_CONSUME",
  );
  assert.equal(outerReplayBodyReached, false);
  assert.equal(await canonicalSnapshot(), before);
  assert.equal(await raw(`SELECT count(*) FROM command_requests WHERE request_id = '${id("cmd", "z")}'`), "0");
});
