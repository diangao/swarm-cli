import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { after, before, test } from "node:test";

import { canonicalProtocolJson, type ArtifactDigest, type ServerId } from "@swarm/protocol";
import { PsqlSession, SharedStore, StorageError, reminderFireDigest } from "../src/index.js";

const databaseUrl = process.env.DATABASE_URL ?? (() => { throw new Error("DATABASE_URL is required"); })();
const schema = `swarm_server_reminders_${process.pid}`;
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

function request(character: string) {
  return {
    actor: { serverId: id("srv", "a") as ServerId },
    scope: "reminder.mutate.v1" as const,
    requestKind: "command" as const,
    requestId: id("cmd", character),
    requestDigest: digest({ request: character }),
  };
}

const schedule = { protocolVersion: 1, kind: "once", at: "2026-08-08T03:00:00.000Z" };

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
  await raw(`INSERT INTO servers(server_id, display_name) VALUES ('${id("srv", "a")}', 'Lane A')`);
});

after(async () => {
  const session = await PsqlSession.open(databaseUrl);
  try {
    await session.execute(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
  } finally {
    await session.close();
  }
});

test("two competing workers produce one same-generation immutable fire", async () => {
  const scheduled = await store.transaction(request("b"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.reminders.schedule({
      reminderId: id("cmd", "a"), owner: { serverId: id("srv", "a") as ServerId },
      anchor: { channelId: "lane-a" }, schedule, nextFireAt: schedule.at,
      expectedGeneration: 0, expectedHeadRowVersion: 0,
    }),
  }));
  assert.deepEqual(scheduled.result.value, { generation: 1, headRowVersion: 0 });
  const lease = (character: string) => store.transaction(request(character), async (transaction) => {
    await transaction.reminders.leaseDue({
      reminderId: id("cmd", "a"), generation: 1, expectedHeadRowVersion: 0,
      workerLeaseId: id("lse", character), leaseUntil: "2026-08-08T04:00:00.000Z",
      now: "2026-08-08T03:00:00.000Z",
    });
    return { protocolVersion: 1, leased: true };
  });
  const leases = await Promise.allSettled([lease("c"), lease("d")]);
  assert.equal(leases.filter((item) => item.status === "fulfilled").length, 1);
  const winner = leases[0]?.status === "fulfilled" ? "c" : "d";
  const fireDigest = reminderFireDigest(id("cmd", "a"), 1);
  const fire = (character: string) => store.transaction(request(character), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.reminders.fire({
      reminderId: id("cmd", "a"), generation: 1, expectedHeadRowVersion: 0,
      workerLeaseId: id("lse", winner), now: "2026-08-08T03:01:00.000Z",
      serverId: id("srv", "a"), receiptId: id("rcp", "a"), requestDigest: fireDigest,
      occurredAt: "2026-08-08T03:01:00.000Z",
    }),
  }));
  const [first, replay] = await Promise.all([fire("e"), fire("f")]);
  assert.equal([first.result.value.replayed, replay.result.value.replayed].filter(Boolean).length, 1);
  assert.equal(first.result.value.producerFactId, replay.result.value.producerFactId);
  assert.equal(first.result.value.outboxJobId, replay.result.value.outboxJobId);
  assert.equal(await raw(`SELECT count(*) FROM reminder_fires WHERE reminder_id = '${id("cmd", "a")}'`), "1");
  assert.equal(await raw(`SELECT count(*) FROM outbox_jobs WHERE producer_fact_id = '${first.result.value.producerFactId}'`), "1");
  assert.equal(await raw(`SELECT count(*) FROM receipts WHERE effect_reminder_id = '${id("cmd", "a")}'`), "1");
  assert.equal(
    await raw(`SELECT NOT (payload_json ?| array['body','workerLeaseId','receiptId'])
      FROM outbox_jobs WHERE job_id = ${first.result.value.outboxJobId}`),
    "t",
  );
});

test("cancel and snooze advance head CAS while a stale worker has zero effects", async () => {
  await store.transaction(request("g"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.reminders.schedule({
      reminderId: id("cmd", "b"), owner: { serverId: id("srv", "a") as ServerId },
      anchor: { channelId: "lane-a" }, schedule, nextFireAt: schedule.at,
      expectedGeneration: 0, expectedHeadRowVersion: 0,
    }),
  }));
  await store.transaction(request("h"), async (transaction) => {
    await transaction.reminders.leaseDue({
      reminderId: id("cmd", "b"), generation: 1, expectedHeadRowVersion: 0,
      workerLeaseId: id("lse", "b"), leaseUntil: "2026-08-08T04:00:00.000Z",
      now: "2026-08-08T03:00:00.000Z",
    });
    return { protocolVersion: 1, leased: true };
  });
  const canceled = await store.transaction(request("j"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.reminders.cancel({
      reminderId: id("cmd", "b"), owner: { serverId: id("srv", "a") as ServerId },
      anchor: { channelId: "lane-a" }, schedule,
      expectedGeneration: 1, expectedHeadRowVersion: 0,
    }),
  }));
  assert.deepEqual(canceled.result.value, { generation: 2, headRowVersion: 1 });
  const before = await raw(`SELECT (SELECT count(*) FROM reminder_fires WHERE reminder_id = '${id("cmd", "b")}') || ':' ||
    (SELECT count(*) FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "b")}') || ':' ||
    (SELECT count(*) FROM receipts WHERE effect_reminder_id = '${id("cmd", "b")}')`);
  await assert.rejects(
    store.transaction(request("k"), async (transaction) => ({
      protocolVersion: 1,
      value: await transaction.reminders.fire({
        reminderId: id("cmd", "b"), generation: 1, expectedHeadRowVersion: 0,
        workerLeaseId: id("lse", "b"), now: "2026-08-08T03:01:00.000Z",
        serverId: id("srv", "a"), receiptId: id("rcp", "b"),
        requestDigest: reminderFireDigest(id("cmd", "b"), 1),
        occurredAt: "2026-08-08T03:01:00.000Z",
      }),
    })),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_REMINDER_GENERATION",
  );
  assert.equal(await raw(`SELECT (SELECT count(*) FROM reminder_fires WHERE reminder_id = '${id("cmd", "b")}') || ':' ||
    (SELECT count(*) FROM outbox_jobs WHERE producer_fact_id = '${id("fac", "b")}') || ':' ||
    (SELECT count(*) FROM receipts WHERE effect_reminder_id = '${id("cmd", "b")}')`), before);
  const snoozed = await store.transaction(request("m"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.reminders.snooze({
      reminderId: id("cmd", "b"), owner: { serverId: id("srv", "a") as ServerId },
      anchor: { channelId: "lane-a" },
      schedule: { ...schedule, at: "2026-08-09T03:00:00.000Z" },
      nextFireAt: "2026-08-09T03:00:00.000Z",
      expectedGeneration: 2, expectedHeadRowVersion: 1,
    }),
  }));
  assert.deepEqual(snoozed.result.value, { generation: 3, headRowVersion: 2 });
  assert.equal(await raw(`SELECT current_generation || ':' || row_version FROM reminder_heads
    WHERE reminder_id = '${id("cmd", "b")}'`), "3:2");
  assert.equal(await raw(`SELECT count(*) FROM reminders WHERE reminder_id = '${id("cmd", "b")}' AND status = 'scheduled'`), "1");
});

test("fake clock refuses a lease before due time", async () => {
  await store.transaction(request("n"), async (transaction) => ({
    protocolVersion: 1,
    value: await transaction.reminders.schedule({
      reminderId: id("cmd", "c"), owner: { serverId: id("srv", "a") as ServerId },
      anchor: { channelId: "lane-a" }, schedule, nextFireAt: schedule.at,
      expectedGeneration: 0, expectedHeadRowVersion: 0,
    }),
  }));
  await assert.rejects(
    store.transaction(request("p"), async (transaction) => {
      await transaction.reminders.leaseDue({
        reminderId: id("cmd", "c"), generation: 1, expectedHeadRowVersion: 0,
        workerLeaseId: variant("lse", "c", "1"), leaseUntil: "2026-08-08T04:00:00.000Z",
        now: "2026-08-08T02:59:59.999Z",
      });
      return { protocolVersion: 1, leased: true };
    }),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_REMINDER_GENERATION",
  );
  assert.equal(await raw(`SELECT worker_lease_id IS NULL FROM reminders WHERE reminder_id = '${id("cmd", "c")}'`), "t");
});
