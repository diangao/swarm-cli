import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { after, before, test } from "node:test";
import {
  canonicalProtocolJson,
  type AgentId,
  type ArtifactDigest,
  type ChannelId,
  type DeliveryEnvelope,
  type DeliveryId,
  type LaunchId,
  type MachineId,
  type MessageId,
  type ProducerFactId,
  type ProtocolVersion,
  type ReceiptActor,
  type ReceiptId,
  type ServerId,
  type Target,
  type TransitionReceipt,
} from "@swarm/protocol";
import {
  PsqlSession,
  SharedStore,
  StorageError,
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
    requestDigest: `sha256:${character.repeat(64)}` as ArtifactDigest,
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

async function raw(sql: string): Promise<string> {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`SET search_path TO ${schema}, pg_catalog;`);
    return await session.execute(sql);
  } finally {
    await session.close();
  }
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
    INSERT INTO agents(agent_id, server_id) VALUES ('${id("agt", "a")}', '${id("srv", "a")}');
    INSERT INTO agents(agent_id, server_id) VALUES ('${id("agt", "b")}', '${id("srv", "a")}');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", "a")}', '${id("srv", "a")}', 'private', 'gate0-a');
    INSERT INTO channels(channel_id, server_id, visibility, name)
      VALUES ('${id("chn", "b")}', '${id("srv", "a")}', 'private', 'gate0-b');
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
      const result = await transaction.messages.append({
        messageId: id("msg", "a"),
        target: channelTarget("b"),
        author: { serverId: id("srv", "a") as ServerId },
        body: "first",
        producerFactId: id("fac", "a"),
        payloadDigest: `sha256:${"a".repeat(64)}` as ArtifactDigest,
      });
      return { protocolVersion: 1, sequence: result.targetSeq };
    }),
    store.transaction(request("c"), async (transaction) => {
      const result = await transaction.messages.append({
        messageId: id("msg", "b"),
        target: channelTarget("b"),
        author: { serverId: id("srv", "a") as ServerId },
        body: "second",
        producerFactId: id("fac", "b"),
        payloadDigest: `sha256:${"b".repeat(64)}` as ArtifactDigest,
      });
      return { protocolVersion: 1, sequence: result.targetSeq };
    }),
  ]);
  assert.deepEqual(values.map((value) => Number(value.result.sequence)).sort(), [1, 2]);
});

test("same request and digest returns one committed canonical result", async () => {
  const same = request("d");
  const execute = () => store.transaction(same, async (transaction) => {
    const result = await transaction.messages.append({
      messageId: id("msg", "c"),
      target: channelTarget("a"),
      author: { serverId: id("srv", "a") as ServerId },
      body: "once",
      producerFactId: id("fac", "c"),
      payloadDigest: `sha256:${"c".repeat(64)}` as ArtifactDigest,
    });
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

test("delivery replay and runtime boundaries require immutable lineage and exact receipts", async () => {
  const prepared = await store.transaction(request("y", "delivery.mutate.v1"), async (transaction) => {
    const message = await transaction.messages.append({
      messageId: id("msg", "f"),
      target: channelTarget("a"),
      author: { serverId: id("srv", "a") as ServerId },
      body: "delivery fact",
      producerFactId: id("fac", "g"),
      payloadDigest: `sha256:${"7".repeat(64)}` as ArtifactDigest,
    });
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

  const written = {
    ...wrongLaunch,
    receiptId: id("rcp", "g") as ReceiptId,
    fence: { ...wrongLaunch.fence, launchId: id("lnc", "a") as LaunchId },
  } as TransitionReceipt;
  await store.transaction(request("4", "delivery.mutate.v1"), async (transaction) => {
    await transaction.deliveries.advanceBoundary({
      deliveryId: id("dlv", "a"),
      to: "input_written",
      receiptBytes: canonicalProtocolJson(written),
      receiptDigest: receiptDigest(written),
    });
    return { protocolVersion: 1 };
  });

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
  assert.equal(await raw(`SELECT count(*) FROM deliveries WHERE producer_fact_id = '${id("fac", "g")}'`), "2");
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
