import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, test } from "node:test";

import {
  canonicalProtocolJson,
  type AgentId,
  type ArtifactDigest,
  type ChannelId,
  type CommandId,
  type DeliveryFence,
  type DeliveryId,
  type InvocationJournalEntry,
  type LaunchId,
  type MachineId,
  type MessageId,
  type ProducerFactId,
  type ProtocolVersion,
  type ReceiptId,
  type SessionId,
  type StateInstanceId,
  type Target,
  type TurnId,
} from "@swarm/protocol";
import { DaemonJournal, StorageError } from "../src/index.js";

const roots: string[] = [];
const journalPaths = new WeakMap<DaemonJournal, string>();

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function id(prefix: string, character: string): string {
  return `${prefix}_${character.repeat(26)}`;
}

function digest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

function openJournal(): DaemonJournal {
  const root = mkdtempSync(join(tmpdir(), "swarm-storage-test-runtime-"));
  roots.push(root);
  const path = join(root, "swarm-storage-test-runtime.sqlite");
  const journal = DaemonJournal.open(path);
  journalPaths.set(journal, path);
  assert.deepEqual(journal.migrate().map((item) => item.version), ["0001", "0002"]);
  return journal;
}

function driverEventRows(journal: DaemonJournal): string {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    return JSON.stringify({
      cursors: database.prepare(
        `SELECT state_instance_id, session_id, next_ordinal, last_event_digest,
                reader_owner_token, reader_epoch, updated_at
         FROM driver_event_cursor ORDER BY state_instance_id, session_id`,
      ).all(),
      events: database.prepare(
        `SELECT state_instance_id, session_id, ordinal, event_digest, turn_id,
                binding_digest, recorded_at
         FROM driver_event_records ORDER BY state_instance_id, session_id, ordinal`,
      ).all(),
    });
  } finally {
    database.close();
  }
}

const target: Target = {
  kind: "channel",
  channelId: id("chn", "a") as ChannelId,
};
const agentId = id("agt", "a") as AgentId;
const machineId = id("mch", "a") as MachineId;
const launchId = id("lnc", "a") as LaunchId;
const stateInstanceId = id("sti", "a") as StateInstanceId;
const sessionId = id("ses", "a") as SessionId;
const turnId = id("trn", "a") as TurnId;
const deliveryId = id("dlv", "a") as DeliveryId;
const messageId = id("msg", "a") as MessageId;
const producerFactId = id("fac", "a") as ProducerFactId;
const protocolVersion = 1 as ProtocolVersion;

const fence: DeliveryFence = {
  protocolVersion,
  deliveryId,
  attempt: 1,
  producerFactId,
  agentId,
  machineId,
  launchId,
  membershipEpoch: 1,
  routingGeneration: 0,
  routeVersion: 1,
  sessionId,
  turnId,
};

function reserve(journal: DaemonJournal): void {
  journal.transaction((transaction) => {
    transaction.reserveLaunch({
      agentId,
      machineId,
      launchId,
      runtime: "codex",
      routingGeneration: 0,
      workspaceGeneration: 1,
      stopEpoch: 0,
      queueOrdinal: 1,
      driverIdentityDigest: digest({ driver: "codex" }),
      queuedAt: "2026-08-09T07:00:00.000Z",
    });
  });
}

function bindDelivery(journal: DaemonJournal): void {
  const envelope = canonicalProtocolJson({
    protocolVersion,
    deliveryId,
    attempt: 1,
    messageId,
    target,
    serverSeq: 11,
    producerFactId,
    agentId,
    machineId,
    expectedLaunchId: launchId,
  });
  journal.transaction((transaction) => {
    transaction.recordDelivery(envelope, {
      launchId,
      stateInstanceId,
      sessionId,
      turnId,
      envelopeDigest: digest(JSON.parse(new TextDecoder().decode(envelope))),
      receivedAt: "2026-08-09T07:00:01.000Z",
    });
    transaction.bindNativeAttempt({ fence, stateInstanceId });
  });
}

function journalEntry<K extends "permit_recorded" | "write_started" | "input_written">(
  kind: K,
  sequence: number,
  previousEntryDigest: ArtifactDigest | null,
  invocationGeneration = 1,
): InvocationJournalEntry<K> {
  const invocationId = id(
    "cmd",
    String.fromCharCode(96 + invocationGeneration),
  ) as CommandId;
  const unsigned: Omit<InvocationJournalEntry<K>, "entryDigest"> = {
    journalId: invocationId,
    entryId: id("cmd", String.fromCharCode(97 + sequence)) as CommandId,
    sequence,
    kind,
    previousEntryDigest,
    ...fence,
    invocationGeneration,
    invocationId,
    permitId: id("cmd", "b") as CommandId,
  };
  return { ...unsigned, entryDigest: digest(unsigned) };
}

function storageCode(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    assert(error instanceof StorageError);
    return error.code;
  }
  return "NO_ERROR";
}

test("launch reservation aliases exact input and rejects a second active launch", () => {
  const journal = openJournal();
  reserve(journal);
  assert.deepEqual(journal.transaction((transaction) => transaction.reserveLaunch({
    agentId,
    machineId,
    launchId,
    runtime: "codex",
    routingGeneration: 0,
    workspaceGeneration: 1,
    stopEpoch: 0,
    queueOrdinal: 1,
    driverIdentityDigest: digest({ driver: "codex" }),
    queuedAt: "2026-08-09T07:00:00.000Z",
  })), { applied: false });
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.reserveLaunch({
    agentId,
    machineId,
    launchId: id("lnc", "b") as LaunchId,
    runtime: "codex",
    routingGeneration: 0,
    workspaceGeneration: 1,
    stopEpoch: 0,
    queueOrdinal: 2,
    driverIdentityDigest: digest({ driver: "codex" }),
    queuedAt: "2026-08-09T07:00:02.000Z",
  }))), "START_SLOT_CONFLICT");
  journal.close();
});

test("visibility ledger requires local model-visible plus observed terminal ACK", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const receiptId = id("rcp", "a") as ReceiptId;
  const commit = {
    sessionId,
    target,
    messageId,
    deliveryId,
    attempt: 1,
    serverSeq: 11,
    modelVisibleAck: { observed: true as const, receiptId },
    visibleAt: "2026-08-09T07:00:05.000Z",
  };
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.commitVisibleMessage(commit),
  )), "MODEL_VISIBLE_PREDECESSOR_REQUIRED");
  journal.transaction((transaction) => {
    transaction.markInputWritten(deliveryId, "2026-08-09T07:00:03.000Z", digest({ input: 1 }));
    transaction.markModelVisible(deliveryId, "2026-08-09T07:00:04.000Z", digest({ visible: 1 }));
  });
  assert.deepEqual(journal.transaction((transaction) => transaction.commitVisibleMessage(commit)), { applied: true });
  assert.deepEqual(journal.transaction((transaction) => transaction.commitVisibleMessage(commit)), { applied: false });
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.commitVisibleMessage({
    ...commit,
    modelVisibleAck: { observed: true, receiptId: id("rcp", "b") as ReceiptId },
  }))), "VISIBILITY_LEDGER_CONFLICT");
  assert.equal(journal.checkpoint(target, sessionId)?.sequence, 11);
  journal.close();
});

test("notice epoch key aliases exact range and rejects changed range without mutation", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const input = {
    sessionId,
    target,
    membershipEpoch: 3,
    firstMessageId: id("msg", "b") as MessageId,
    latestMessageId: id("msg", "c") as MessageId,
    firstServerSeq: 8,
    latestServerSeq: 10,
    inputDeliveryId: deliveryId,
    inputAttempt: 1,
    committedAt: "2026-08-09T07:00:05.000Z",
  };
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.commitNoticeVisibility(input),
  )), "NOTICE_DEDUPE_PREDECESSOR_REQUIRED");

  const permit = journalEntry("permit_recorded", 1, null);
  const started = journalEntry("write_started", 2, permit.entryDigest);
  const written = journalEntry("input_written", 3, started.entryDigest);
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
    transaction.appendNativeInvocationEntry({ entry: started });
    transaction.appendNativeInvocationEntry({ entry: written });
  });
  assert.deepEqual(journal.transaction((transaction) => transaction.commitNoticeVisibility(input)), { applied: true });
  assert.deepEqual(journal.transaction((transaction) => transaction.commitNoticeVisibility({
    ...input,
    committedAt: "2026-08-09T07:00:06.000Z",
  })), { applied: false });
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.commitNoticeVisibility({
    ...input,
    latestServerSeq: 11,
  }))), "VISIBILITY_LEDGER_CONFLICT");
  assert.deepEqual(journal.transaction((transaction) => transaction.commitNoticeVisibility({
    ...input,
    membershipEpoch: 4,
    latestServerSeq: 11,
  })), { applied: true });
  journal.close();
});

test("native invocation entries reject a changed digest before mutation", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const permit = journalEntry("permit_recorded", 1, null);
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.appendNativeInvocationEntry({
      entry: { ...permit, entryDigest: digest({ changed: true }) },
    }),
  )), "INVALID_JOURNAL_CHAIN");
  assert.deepEqual(journal.transaction(
    (transaction) => transaction.appendNativeInvocationEntry({ entry: permit }),
  ), { applied: true });
  const started = journalEntry("write_started", 2, permit.entryDigest);
  const nextPermit = journalEntry("permit_recorded", 3, started.entryDigest, 2);
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: started });
    assert.deepEqual(
      transaction.appendNativeInvocationEntry({ entry: nextPermit }),
      { applied: true },
    );
  });
  journal.close();
});

test("turn preparation and transition CAS preserve one active turn and one in-flight input", () => {
  const journal = openJournal();
  reserve(journal);
  const driverTurnRefDigest = digest({ driverTurn: "a" });
  const ordinary = {
    protocolTurnId: turnId,
    launchId,
    stateInstanceId,
    sessionId,
    rootProducerFactId: producerFactId,
    inputOrdinal: 0,
    driverTurnRefDigest,
    mode: "ordinary" as const,
    queuedAt: "2026-08-09T07:01:00.000Z",
  };
  assert.deepEqual(journal.transaction(
    (transaction) => transaction.prepareTurn(ordinary),
  ), { applied: true });
  assert.deepEqual(journal.transaction(
    (transaction) => transaction.prepareTurn(ordinary),
  ), { applied: false });
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.prepareTurn({
      ...ordinary,
      protocolTurnId: id("trn", "b") as TurnId,
      rootProducerFactId: id("fac", "b") as ProducerFactId,
    }),
  )), "ACTIVE_TURN_CONFLICT");

  for (const [expectedState, nextState, updatedAt] of [
    ["queued", "write_started", "2026-08-09T07:01:01.000Z"],
    ["write_started", "input_written", "2026-08-09T07:01:02.000Z"],
    ["input_written", "model_visible", "2026-08-09T07:01:03.000Z"],
  ] as const) {
    const transition = {
      protocolTurnId: turnId,
      inputOrdinal: 0,
      expectedState,
      nextState,
      updatedAt,
    };
    assert.deepEqual(journal.transaction(
      (transaction) => transaction.advanceTurn(transition),
    ), { applied: true });
    assert.deepEqual(journal.transaction(
      (transaction) => transaction.advanceTurn(transition),
    ), { applied: false });
  }

  const steer = {
    ...ordinary,
    inputOrdinal: 1,
    mode: "steer" as const,
    expectedTurnId: turnId,
    queuedAt: "2026-08-09T07:01:04.000Z",
  };
  assert.deepEqual(journal.transaction(
    (transaction) => transaction.prepareTurn(steer),
  ), { applied: true });
  assert.deepEqual(journal.transaction(
    (transaction) => transaction.prepareTurn(steer),
  ), { applied: false });
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.prepareTurn({
      ...steer,
      inputOrdinal: 2,
      queuedAt: "2026-08-09T07:01:05.000Z",
    }),
  )), "TURN_INPUT_ALREADY_IN_FLIGHT");

  for (const [expectedState, nextState, updatedAt] of [
    ["queued", "write_started", "2026-08-09T07:01:05.000Z"],
    ["write_started", "input_written", "2026-08-09T07:01:06.000Z"],
    ["input_written", "model_visible", "2026-08-09T07:01:07.000Z"],
  ] as const) {
    assert.deepEqual(journal.transaction((transaction) => transaction.advanceTurn({
      protocolTurnId: turnId,
      inputOrdinal: 1,
      expectedState,
      nextState,
      updatedAt,
    })), { applied: true });
  }
  assert.deepEqual(journal.transaction((transaction) => transaction.prepareTurn({
    ...steer,
    inputOrdinal: 2,
    queuedAt: "2026-08-09T07:01:08.000Z",
  })), { applied: true });
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.advanceTurn({
      protocolTurnId: turnId,
      inputOrdinal: 1,
      expectedState: "queued",
      nextState: "write_started",
      updatedAt: "2026-08-09T07:01:09.000Z",
    }),
  )), "ACTIVE_TURN_CONFLICT");
  journal.close();
});

test("one event reader owns a cursor; identity and temporal errors stay separate", () => {
  const journal = openJournal();
  const owner = digest({ pump: 1 });
  const replacement = digest({ pump: 2 });
  assert.deepEqual(journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: owner,
    mode: "start",
    claimedAt: "2026-08-09T07:00:00.000Z",
  })), { readerEpoch: 1 });
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: replacement,
    mode: "subscribe",
    claimedAt: "2026-08-09T07:00:01.000Z",
  }))), "DRIVER_EVENT_READER_CONFLICT");
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: replacement,
    mode: "resume",
    claimedAt: "2026-08-09T07:00:01.000Z",
  }))), "DRIVER_RESUME_OVERLAP");

  const first = {
    stateInstanceId,
    sessionId,
    ownerToken: owner,
    readerEpoch: 1,
    ordinal: 0,
    eventDigest: digest({ event: 0 }),
    turnId,
    bindingDigest: digest({ binding: 0 }),
    recordedAt: "2026-08-09T07:00:02.000Z",
  };
  assert.deepEqual(journal.transaction((transaction) => transaction.commitDriverEvent(first)), {
    applied: true,
    nextOrdinal: 1,
  });
  assert.deepEqual(journal.transaction((transaction) => transaction.commitDriverEvent(first)), {
    applied: false,
    nextOrdinal: 1,
  });
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.commitDriverEvent({
    ...first,
    eventDigest: digest({ changed: true }),
  }))), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.deepEqual(journal.transaction((transaction) => transaction.commitDriverEvent({
    ...first,
    ordinal: 1,
    eventDigest: digest({ event: 1 }),
    recordedAt: "2026-08-09T07:00:02.500Z",
  })), { applied: true, nextOrdinal: 2 });
  assert.deepEqual(journal.transaction((transaction) => transaction.commitDriverEvent(first)), {
    applied: false,
    nextOrdinal: 2,
  });
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.commitDriverEvent({
    ...first,
    ordinal: 3,
    eventDigest: digest({ event: 3 }),
  }))), "DRIVER_EVENT_ORDER_INVALID");

  journal.transaction((transaction) => transaction.releaseDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: owner,
    readerEpoch: 1,
    releasedAt: "2026-08-09T07:00:03.000Z",
  }));
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: replacement,
    mode: "subscribe",
    claimedAt: "2026-08-09T07:00:03.500Z",
  }))), "DRIVER_EVENT_READER_CONFLICT");
  assert.deepEqual(journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: replacement,
    mode: "resume",
    claimedAt: "2026-08-09T07:00:04.000Z",
  })), { readerEpoch: 2 });
  journal.close();
});

test("state-instance reader ownership binds one immutable session with zero conflict mutation", () => {
  const journal = openJournal();
  const sessionB = id("ses", "b") as SessionId;
  const ownerA = digest({ pump: "state-owner-a" });
  const ownerB = digest({ pump: "state-owner-b" });
  assert.deepEqual(journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: ownerA,
    mode: "start",
    claimedAt: "2026-08-09T07:10:00.000Z",
  })), { readerEpoch: 1 });
  const ownedRows = driverEventRows(journal);

  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId: sessionB,
    ownerToken: ownerB,
    mode: "start",
    claimedAt: "2026-08-09T07:10:01.000Z",
  }))), "DRIVER_EVENT_READER_CONFLICT");
  assert.equal(driverEventRows(journal), ownedRows);
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.commitDriverEvent({
    stateInstanceId,
    sessionId: sessionB,
    ownerToken: ownerB,
    readerEpoch: 1,
    ordinal: 0,
    eventDigest: digest({ event: "wrong-session" }),
    recordedAt: "2026-08-09T07:10:02.000Z",
  }))), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(driverEventRows(journal), ownedRows);

  assert.deepEqual(journal.transaction((transaction) => transaction.commitDriverEvent({
    stateInstanceId,
    sessionId,
    ownerToken: ownerA,
    readerEpoch: 1,
    ordinal: 0,
    eventDigest: digest({ event: "accepted-session" }),
    recordedAt: "2026-08-09T07:10:03.000Z",
  })), { applied: true, nextOrdinal: 1 });
  journal.close();
});

test("stale reader epochs cannot release or commit when an owner token is reused", () => {
  const journal = openJournal();
  const reusedOwner = digest({ pump: "reused-owner" });
  assert.deepEqual(journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    mode: "start",
    claimedAt: "2026-08-09T07:20:00.000Z",
  })), { readerEpoch: 1 });
  journal.transaction((transaction) => transaction.releaseDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    readerEpoch: 1,
    releasedAt: "2026-08-09T07:20:01.000Z",
  }));
  assert.deepEqual(journal.transaction((transaction) => transaction.claimDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    mode: "resume",
    claimedAt: "2026-08-09T07:20:02.000Z",
  })), { readerEpoch: 2 });
  const epochTwoRows = driverEventRows(journal);

  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.releaseDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    readerEpoch: 1,
    releasedAt: "2026-08-09T07:20:03.000Z",
  }))), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(driverEventRows(journal), epochTwoRows);
  assert.equal(storageCode(() => journal.transaction((transaction) => transaction.commitDriverEvent({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    readerEpoch: 1,
    ordinal: 0,
    eventDigest: digest({ event: "stale-epoch" }),
    recordedAt: "2026-08-09T07:20:04.000Z",
  }))), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(driverEventRows(journal), epochTwoRows);

  assert.deepEqual(journal.transaction((transaction) => transaction.commitDriverEvent({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    readerEpoch: 2,
    ordinal: 0,
    eventDigest: digest({ event: "current-epoch" }),
    recordedAt: "2026-08-09T07:20:05.000Z",
  })), { applied: true, nextOrdinal: 1 });
  journal.transaction((transaction) => transaction.releaseDriverEventReader({
    stateInstanceId,
    sessionId,
    ownerToken: reusedOwner,
    readerEpoch: 2,
    releasedAt: "2026-08-09T07:20:06.000Z",
  }));
  journal.close();
});
