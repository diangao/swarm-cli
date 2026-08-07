import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, test } from "node:test";
import {
  canonicalProtocolJson,
  type AgentId,
  type ArtifactDigest,
  type ChannelId,
  type ConversationId,
  type DeliveryId,
  type LaunchId,
  type MachineId,
  type MessageId,
  type ProducerFactId,
  type ProtocolVersion,
  type SessionId,
  type StateInstanceId,
  type Target,
  type TurnId,
} from "@swarm/protocol";
import { DaemonJournal, StorageError, canonicalTargetKey } from "../src/index.js";

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function id(prefix: string, character: string): string {
  return `${prefix}_${character.repeat(26)}`;
}

function digest(bytes: Uint8Array): ArtifactDigest {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}` as ArtifactDigest;
}

function target(thread?: string): Target {
  const value: { kind: "channel"; channelId: ChannelId; threadRootMessageId?: MessageId } = {
    kind: "channel",
    channelId: id("chn", "a") as ChannelId,
  };
  if (thread !== undefined) value.threadRootMessageId = thread as MessageId;
  return value;
}

function envelope(input: {
  delivery: string;
  message: string;
  fact: string;
  target: Target;
  sequence: number;
  attempt?: number;
  replayOf?: string;
  agent?: string;
  expectedLaunch?: string;
}): Uint8Array {
  const value: Record<string, unknown> = {
    protocolVersion: 1 as ProtocolVersion,
    deliveryId: input.delivery as DeliveryId,
    attempt: input.attempt ?? 1,
    messageId: input.message as MessageId,
    target: input.target,
    serverSeq: input.sequence,
    producerFactId: input.fact as ProducerFactId,
    agentId: (input.agent ?? id("agt", "a")) as AgentId,
    machineId: id("mch", "a") as MachineId,
  };
  if (input.replayOf !== undefined) value.replayOf = input.replayOf as DeliveryId;
  if (input.expectedLaunch !== undefined) value.expectedLaunchId = input.expectedLaunch as LaunchId;
  return canonicalProtocolJson(value);
}

function openJournal(): { journal: DaemonJournal; path: string } {
  const root = mkdtempSync(join(tmpdir(), "swarm-storage-test-"));
  roots.push(root);
  const path = join(root, "swarm-storage-test-journal.sqlite");
  const journal = DaemonJournal.open(path);
  journal.migrate();
  return { journal, path };
}

const binding = {
  launchId: id("lnc", "a") as LaunchId,
  stateInstanceId: id("sti", "a") as StateInstanceId,
  sessionId: id("ses", "a") as SessionId,
  turnId: id("trn", "a") as TurnId,
};

const detailDigest = `sha256:${"a".repeat(64)}` as ArtifactDigest;

test("migrations are checksum-stable and a second daemon cannot own the journal", () => {
  const { journal, path } = openJournal();
  assert.deepEqual(journal.migrate().map((receipt) => receipt.applied), [false]);
  assert.throws(() => DaemonJournal.open(path), (error: unknown) => {
    return error instanceof StorageError && error.code === "JOURNAL_LOCKED";
  });
  journal.close();
});

test("a dead process lock is recovered and the journal file is owner-only", () => {
  const root = mkdtempSync(join(tmpdir(), "swarm-storage-test-"));
  roots.push(root);
  const path = join(root, "swarm-storage-test-stale-lock.sqlite");
  writeFileSync(`${path}.lock`, "2147483647\n", { encoding: "utf8", mode: 0o600 });
  const journal = DaemonJournal.open(path);
  journal.migrate();
  assert.equal(statSync(path).mode & 0o777, 0o600);
  journal.close();
});

test("delivery evidence preserves input-written and model-visible as distinct reopen boundaries", () => {
  const { journal, path } = openJournal();
  const ambiguous = envelope({
    delivery: id("dlv", "a"),
    message: id("msg", "a"),
    fact: id("fac", "a"),
    target: target(),
    sequence: 1,
  });
  const visible = envelope({
    delivery: id("dlv", "b"),
    message: id("msg", "b"),
    fact: id("fac", "b"),
    target: target(id("msg", "c")),
    sequence: 2,
  });
  journal.transaction((transaction) => {
    transaction.recordDelivery(ambiguous, {
      ...binding,
      envelopeDigest: digest(ambiguous),
      receivedAt: "2026-08-06T15:00:00.000Z",
    });
    transaction.markInputWritten(id("dlv", "a"), "2026-08-06T15:00:01.000Z", detailDigest);
    transaction.recordDelivery(visible, {
      ...binding,
      envelopeDigest: digest(visible),
      receivedAt: "2026-08-06T15:00:02.000Z",
    });
    transaction.markInputWritten(id("dlv", "b"), "2026-08-06T15:00:03.000Z", detailDigest);
    transaction.markModelVisible(id("dlv", "b"), "2026-08-06T15:00:04.000Z", detailDigest);
  });
  journal.close();

  const reopened = DaemonJournal.open(path);
  reopened.migrate();
  const evidence = reopened.listRecoveryEvidence().filter((item) => item.kind === "delivery");
  assert.deepEqual(
    evidence.map((item) => [item.identity, item.ambiguousBoundary, item.suppressInputReplay]),
    [
      [id("dlv", "a"), true, false],
      [id("dlv", "b"), false, true],
    ],
  );
  reopened.close();
});

test("canonical target keys isolate parent and sibling thread checkpoints across reopen", () => {
  const { journal, path } = openJournal();
  const targets = [target(), target(id("msg", "b")), target(id("msg", "c"))];
  assert.equal(new Set(targets.map(canonicalTargetKey)).size, 3);
  targets.forEach((item, index) => {
    const character = ["a", "b", "c"][index] ?? "d";
    const bytes = envelope({
      delivery: id("dlv", character),
      message: id("msg", character),
      fact: id("fac", character),
      target: item,
      sequence: index + 1,
    });
    journal.transaction((transaction) => {
      transaction.recordDelivery(bytes, {
        ...binding,
        envelopeDigest: digest(bytes),
        receivedAt: `2026-08-06T15:00:0${index}.000Z`,
      });
      transaction.markInputWritten(id("dlv", character), `2026-08-06T15:01:0${index}.000Z`, detailDigest);
      transaction.markModelVisible(id("dlv", character), `2026-08-06T15:02:0${index}.000Z`, detailDigest);
    });
  });
  journal.close();
  const reopened = DaemonJournal.open(path);
  reopened.migrate();
  targets.forEach((item, index) => {
    assert.equal(reopened.checkpoint(item, binding.sessionId)?.sequence, index + 1);
  });
  reopened.close();
});

test("replay requires fresh identity and immutable lineage", () => {
  const { journal } = openJournal();
  const first = envelope({
    delivery: id("dlv", "a"),
    message: id("msg", "a"),
    fact: id("fac", "a"),
    target: target(),
    sequence: 1,
  });
  const replay = envelope({
    delivery: id("dlv", "b"),
    message: id("msg", "a"),
    fact: id("fac", "a"),
    target: target(),
    sequence: 1,
    attempt: 2,
    replayOf: id("dlv", "a"),
  });
  journal.transaction((transaction) => {
    transaction.recordDelivery(first, {
      ...binding,
      envelopeDigest: digest(first),
      receivedAt: "2026-08-06T15:00:00.000Z",
    });
    transaction.recordDelivery(replay, {
      ...binding,
      envelopeDigest: digest(replay),
      receivedAt: "2026-08-06T15:00:01.000Z",
    });
  });
  const wrongAgent = envelope({
    delivery: id("dlv", "c"),
    message: id("msg", "a"),
    fact: id("fac", "a"),
    target: target(),
    sequence: 1,
    attempt: 3,
    replayOf: id("dlv", "b"),
    agent: id("agt", "b"),
  });
  assert.throws(
    () => journal.transaction((transaction) => transaction.recordDelivery(wrongAgent, {
      ...binding,
      envelopeDigest: digest(wrongAgent),
      receivedAt: "2026-08-06T15:00:02.000Z",
    })),
    (error: unknown) => error instanceof StorageError && error.code === "INVALID_STATE_TRANSITION",
  );
  journal.close();
});

test("local outbound intent remains recovery evidence, never shared confirmation", () => {
  const { journal } = openJournal();
  journal.transaction((transaction) => {
    transaction.prepareIntent({
      intentId: id("cmd", "a"),
      commandKind: "task_status",
      requestId: id("cmd", "b"),
      payloadDigest: detailDigest,
      preparedAt: "2026-08-06T15:00:00.000Z",
    });
  });
  assert.deepEqual(journal.listRecoveryEvidence(), [
    {
      kind: "intent",
      identity: id("cmd", "a"),
      state: "prepared",
      ambiguousBoundary: false,
      suppressInputReplay: true,
    },
  ]);
  journal.close();
});

test("prepared local operation survives reopen only as safe recovery evidence", () => {
  const { journal, path } = openJournal();
  journal.transaction((transaction) => {
    transaction.prepareOperation({
      operationId: id("cmd", "c"),
      operationKind: "runtime_effect",
      ...binding,
      idempotencyKey: "local-operation-c",
      payloadDigest: detailDigest,
      preparedAt: "2026-08-06T15:05:00.000Z",
      detailJson: new TextDecoder().decode(canonicalProtocolJson({
        protocolVersion: 1,
        operation: "runtime_effect",
      })),
    });
  });
  journal.close();
  const reopened = DaemonJournal.open(path);
  reopened.migrate();
  assert.deepEqual(reopened.listRecoveryEvidence(), [{
    kind: "operation",
    identity: id("cmd", "c"),
    state: "prepared",
    ambiguousBoundary: false,
    suppressInputReplay: false,
  }]);
  reopened.close();
});

test("local delivery binding rejects a different expected launch fence", () => {
  const { journal } = openJournal();
  const bytes = envelope({
    delivery: id("dlv", "d"),
    message: id("msg", "d"),
    fact: id("fac", "d"),
    target: target(),
    sequence: 4,
    expectedLaunch: id("lnc", "b"),
  });
  assert.throws(
    () => journal.transaction((transaction) => transaction.recordDelivery(bytes, {
      ...binding,
      envelopeDigest: digest(bytes),
      receivedAt: "2026-08-06T15:06:00.000Z",
    })),
    (error: unknown) => error instanceof StorageError && error.code === "STALE_FENCE",
  );
  assert.deepEqual(journal.listRecoveryEvidence(), []);
  journal.close();
});

test("corrupted migration checksum fails closed and preserves the journal", () => {
  const { journal, path } = openJournal();
  journal.close();
  const raw = new DatabaseSync(path);
  raw.exec(`UPDATE journal_migrations SET checksum = '${"0".repeat(64)}' WHERE version = '0001'`);
  raw.close();
  const reopened = DaemonJournal.open(path);
  assert.throws(() => reopened.migrate(), (error: unknown) => {
    return error instanceof StorageError && error.code === "MIGRATION_CHECKSUM_MISMATCH";
  });
  reopened.close();
});

test("test reset is guarded and rebuilds from forward migrations", () => {
  const { journal } = openJournal();
  const prior = process.env.NODE_ENV;
  process.env.NODE_ENV = "test";
  try {
    assert.deepEqual(journal.resetForTests().map((receipt) => receipt.applied), [true]);
  } finally {
    if (prior === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = prior;
    journal.close();
  }
});

test("direct and channel targets never share a canonical key", () => {
  const direct: Target = {
    kind: "direct",
    conversationId: id("cvs", "a") as ConversationId,
  };
  assert.notEqual(canonicalTargetKey(target()), canonicalTargetKey(direct));
});
