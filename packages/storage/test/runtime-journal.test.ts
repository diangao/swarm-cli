import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, test } from "node:test";

import {
  buildContributionBinding,
  canonicalProtocolJson,
  type ContributionBindingInput,
  type TurnCompletionEvidence,
  type TurnCoordinationDisposition,
  type TurnReplyResult,
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

function journalEntry<
  K extends "permit_recorded" | "write_started" | "input_written" | "model_visible",
>(
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

test("notice compare reads new/replay/conflict before input without mutation", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const range = {
    sessionId,
    target,
    membershipEpoch: 3,
    firstMessageId: id("msg", "b") as MessageId,
    latestMessageId: id("msg", "c") as MessageId,
    firstServerSeq: 8,
    latestServerSeq: 10,
    inputDeliveryId: deliveryId,
    inputAttempt: 1,
  };
  assert.equal(journal.transaction(
    (transaction) => transaction.compareNoticeVisibility(range),
  ), "new");

  const permit = journalEntry("permit_recorded", 1, null);
  const started = journalEntry("write_started", 2, permit.entryDigest);
  const written = journalEntry("input_written", 3, started.entryDigest);
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
    transaction.appendNativeInvocationEntry({ entry: started });
    transaction.appendNativeInvocationEntry({ entry: written });
    transaction.commitNoticeVisibility({
      ...range,
      committedAt: "2026-08-09T07:00:05.000Z",
    });
  });
  const before = noticeRows(journal);
  assert.equal(journal.transaction(
    (transaction) => transaction.compareNoticeVisibility(range),
  ), "replay");
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.compareNoticeVisibility({
      ...range,
      latestServerSeq: 11,
    }),
  )), "VISIBILITY_LEDGER_CONFLICT");
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.compareNoticeVisibility({
      ...range,
      inputDeliveryId: id("dlv", "z") as DeliveryId,
    }),
  )), "VISIBILITY_LEDGER_CONFLICT");
  assert.equal(storageCode(() => journal.transaction(
    (transaction) => transaction.compareNoticeVisibility({
      ...range,
      inputAttempt: 2,
    }),
  )), "VISIBILITY_LEDGER_CONFLICT");
  assert.equal(journal.transaction(
    (transaction) => transaction.compareNoticeVisibility({
      ...range,
      membershipEpoch: 4,
    }),
  ), "new");
  assert.equal(noticeRows(journal), before);
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

function noticeRows(journal: DaemonJournal): string {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    return JSON.stringify(database.prepare(
      `SELECT session_id, target_key, membership_epoch, first_message_id,
              latest_message_id, first_server_seq, latest_server_seq,
              input_delivery_id, input_attempt, committed_at
       FROM notice_visibility
       ORDER BY session_id, target_key, membership_epoch`,
    ).all());
  } finally {
    database.close();
  }
}

function attemptRows(journal: DaemonJournal): string {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    return JSON.stringify({
      attempts: database.prepare(
        `SELECT delivery_id, attempt, state, permit_id, invocation_generation,
                invocation_id, body_digest, previous_invocation_generation,
                previous_proof_digest, proof_json, disconnect_id,
                suppression_reason
         FROM native_attempts ORDER BY delivery_id, attempt`,
      ).all(),
      entries: database.prepare(
        `SELECT delivery_id, attempt, invocation_generation, sequence, kind
         FROM native_invocation_entries
         ORDER BY delivery_id, attempt, sequence`,
      ).all(),
      visible: database.prepare(
        `SELECT session_id, target_key, message_id, delivery_id, attempt
         FROM visible_message_ids ORDER BY session_id, target_key, message_id`,
      ).all(),
      completions: database.prepare(
        `SELECT delivery_id, attempt, reply_receipt_id, reply_result_digest,
                coordination_kind, coordination_terminal_turn_id,
                coordination_command_id, coordination_receipt_id,
                coordination_result_digest, contribution_binding_digest
         FROM native_attempt_completions ORDER BY delivery_id, attempt`,
      ).all(),
    });
  } finally {
    database.close();
  }
}

function boundEntry<
  K extends "permit_recorded" | "write_started" | "input_written" | "model_visible",
>(
  kind: K,
  sequence: number,
  previousEntryDigest: ArtifactDigest | null,
  extra: Record<string, unknown>,
  invocationGeneration = 1,
): InvocationJournalEntry<K> & Record<string, unknown> {
  const invocationId = id(
    "cmd",
    String.fromCharCode(96 + invocationGeneration),
  ) as CommandId;
  const unsigned = {
    journalId: invocationId,
    entryId: id("cmd", String.fromCharCode(97 + sequence)) as CommandId,
    sequence,
    kind,
    previousEntryDigest,
    ...fence,
    invocationGeneration,
    invocationId,
    permitId: id("cmd", "b") as CommandId,
    ...extra,
  };
  return { ...unsigned, entryDigest: digest(unsigned) } as
    InvocationJournalEntry<K> & Record<string, unknown>;
}

function transitionInput(
  expectedState: string,
  nextState: string,
  extra: Record<string, unknown> = {},
): never {
  return {
    fence,
    stateInstanceId,
    expectedState,
    nextState,
    ...extra,
  } as never;
}

const bodyDigestGen1 = digest({ body: 1 });
const runtimeWriteId = id("cmd", "w") as CommandId;
const visibilityEventId = id("cmd", "v") as CommandId;

function advanceToModelVisible(journal: DaemonJournal): {
  permit: ReturnType<typeof boundEntry<"permit_recorded">>;
  started: ReturnType<typeof boundEntry<"write_started">>;
} {
  const permit = boundEntry("permit_recorded", 1, null, {});
  const started = boundEntry("write_started", 2, permit.entryDigest, {
    inputDigest: bodyDigestGen1,
  });
  const written = boundEntry("input_written", 3, started.entryDigest, {
    runtimeWriteId,
  });
  const visible = boundEntry("model_visible", 4, written.entryDigest, {
    runtimeWriteId,
    visibilityEventId,
  });
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", { permitId: permit.permitId }),
    );
    transaction.appendNativeInvocationEntry({ entry: started });
    transaction.transitionNativeAttempt(
      transitionInput("permit_recorded", "write_started", {
        invocationGeneration: 1,
        invocationId: started.invocationId,
        bodyDigest: bodyDigestGen1,
      }),
    );
    transaction.appendNativeInvocationEntry({ entry: written });
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "input_written"),
    );
    transaction.appendNativeInvocationEntry({ entry: visible });
    transaction.transitionNativeAttempt(
      transitionInput("input_written", "model_visible"),
    );
  });
  return { permit, started };
}

test("attempt transitions require byte-exact journal evidence bindings", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const permit = boundEntry("permit_recorded", 1, null, {});
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", { permitId: permit.permitId }),
    ),
  )), "INVALID_JOURNAL_CHAIN");
  assert.equal(attemptRows(journal), before);
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
  });
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", {
        permitId: id("cmd", "x") as CommandId,
      }),
    ),
  )), "WRITE_STARTED_BINDING_MISMATCH");
  journal.transaction((transaction) => {
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", { permitId: permit.permitId }),
    );
  });
  const started = boundEntry("write_started", 2, permit.entryDigest, {
    inputDigest: bodyDigestGen1,
  });
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: started });
  });
  const beforeWrite = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("permit_recorded", "write_started", {
        invocationGeneration: 1,
        invocationId: started.invocationId,
        bodyDigest: digest({ body: "different" }),
      }),
    ),
  )), "WRITE_STARTED_BINDING_MISMATCH");
  assert.equal(attemptRows(journal), beforeWrite);
  journal.close();
});

test("entry fence fields beyond the delivery quad cannot be forged", () => {
  const forgedFenceMutations: Record<string, unknown>[] = [
    { producerFactId: id("fac", "z") as ProducerFactId },
    { routeVersion: 9 },
    { turnId: id("trn", "z") as TurnId },
    { membershipEpoch: 7 },
    { routingGeneration: 5 },
  ];
  for (const mutation of forgedFenceMutations) {
    const journal = openJournal();
    reserve(journal);
    bindDelivery(journal);
    const forged = boundEntry("permit_recorded", 1, null, mutation);
    journal.transaction((transaction) => {
      transaction.appendNativeInvocationEntry({ entry: forged });
    });
    const before = attemptRows(journal);
    assert.equal(storageCode(() => journal.transaction((transaction) =>
      transaction.transitionNativeAttempt(
        transitionInput("accepted", "permit_recorded", {
          permitId: forged.permitId,
        }),
      ),
    )), "WRITE_STARTED_BINDING_MISMATCH");
    assert.equal(attemptRows(journal), before);
    journal.close();
  }
});

test("an entry missing a fence field cannot authorize a transition", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const {
    turnId: _omitted,
    entryDigest: _stale,
    ...strippedUnsigned
  } = boundEntry("permit_recorded", 1, null, {});
  const stripped = {
    ...strippedUnsigned,
    entryDigest: digest(strippedUnsigned),
  };
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: stripped as never });
  });
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", {
        permitId: stripped.permitId,
      }),
    ),
  )), "WRITE_STARTED_BINDING_MISMATCH");
  assert.equal(attemptRows(journal), before);
  journal.close();
});

test("a caller fence differing beyond the delivery quad is stale", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const permit = boundEntry("permit_recorded", 1, null, {});
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
  });
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt({
      ...transitionInput("accepted", "permit_recorded", {
        permitId: permit.permitId,
      }) as Record<string, unknown>,
      fence: { ...fence, turnId: id("trn", "z") as TurnId },
    } as never),
  )), "STALE_DELIVERY_FENCE");
  assert.equal(attemptRows(journal), before);
  journal.close();
});

function completeTurn(journal: DaemonJournal): void {
  journal.transaction((transaction) => {
    transaction.prepareTurn({
      protocolTurnId: turnId,
      launchId,
      stateInstanceId,
      sessionId,
      rootProducerFactId: producerFactId,
      inputOrdinal: 0,
      driverTurnRefDigest: digest({ turnRef: 1 }),
      mode: "ordinary",
      queuedAt: "2026-08-09T07:00:00.500Z",
    });
    const chain = [
      ["queued", "write_started"],
      ["write_started", "input_written"],
      ["input_written", "model_visible"],
      ["model_visible", "completed"],
    ] as const;
    for (const [expectedState, nextState] of chain) {
      transaction.advanceTurn({
        protocolTurnId: turnId,
        inputOrdinal: 0,
        expectedState,
        nextState,
        updatedAt: "2026-08-09T07:00:04.500Z",
      });
    }
  });
}

function completionEvidence(
  overrides: {
    contribution?: Partial<ContributionBindingInput>;
    reply?: TurnReplyResult;
    coordination?: TurnCoordinationDisposition;
  } = {},
): TurnCompletionEvidence {
  const contribution = buildContributionBinding({
    fence,
    stateInstanceId,
    inputOrdinal: 0,
    invocationId: id("cmd", "a") as CommandId,
    invocationGeneration: 1,
    permitId: id("cmd", "b") as CommandId,
    runtimeWriteId,
    visibilityEventId,
    ...overrides.contribution,
  });
  return {
    contribution,
    reply: overrides.reply ?? {
      receiptId: id("rcp", "b") as ReceiptId,
      resultDigest: digest({ reply: 1 }),
    },
    coordination: overrides.coordination ?? {
      kind: "not_requested",
      terminalTurnId: turnId,
    },
  };
}

test("attempt state machine reaches consumed only after ledger and completion", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  advanceToModelVisible(journal);

  const beforeConsume = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("model_visible", "consumed"),
    ),
  )), "MODEL_VISIBLE_PREDECESSOR_REQUIRED");
  assert.equal(attemptRows(journal), beforeConsume);

  const completion = {
    fence,
    stateInstanceId,
    evidence: completionEvidence(),
    recordedAt: "2026-08-09T07:00:06.000Z",
  };
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.recordAttemptCompletion(completion),
  )), "MODEL_VISIBLE_PREDECESSOR_REQUIRED");

  journal.transaction((transaction) => {
    transaction.markInputWritten(deliveryId, "2026-08-09T07:00:03.000Z", digest({ input: 1 }));
    transaction.markModelVisible(deliveryId, "2026-08-09T07:00:04.000Z", digest({ visible: 1 }));
    transaction.commitVisibleMessage({
      sessionId,
      target,
      messageId,
      deliveryId,
      attempt: 1,
      serverSeq: 11,
      modelVisibleAck: { observed: true, receiptId: id("rcp", "a") as ReceiptId },
      visibleAt: "2026-08-09T07:00:05.000Z",
    });
  });

  const beforeCompletion = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("model_visible", "consumed"),
    ),
  )), "ACK_PREDECESSOR_REQUIRED");
  assert.equal(attemptRows(journal), beforeCompletion);

  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.recordAttemptCompletion(completion),
  )), "INVALID_JOURNAL_CHAIN");
  assert.equal(attemptRows(journal), beforeCompletion);
  completeTurn(journal);

  assert.deepEqual(journal.transaction((transaction) =>
    transaction.recordAttemptCompletion(completion),
  ), { applied: true });
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.recordAttemptCompletion(completion),
  ), { applied: false });
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.recordAttemptCompletion({
      ...completion,
      evidence: completionEvidence({
        reply: { receiptId: id("rcp", "b") as ReceiptId, resultDigest: digest({ reply: 2 }) },
      }),
    }),
  )), "INVOCATION_STATE_CONFLICT");

  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
  ), { applied: true });
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
  ), { applied: false });
  assert.equal(journal.transaction((transaction) =>
    transaction.readNativeAttempt(deliveryId, 1),
  )?.state, "consumed");
  journal.close();
});

function readyForCompletion(journal: DaemonJournal): void {
  reserve(journal);
  bindDelivery(journal);
  advanceToModelVisible(journal);
  journal.transaction((transaction) => {
    transaction.markInputWritten(deliveryId, "2026-08-09T07:00:03.000Z", digest({ input: 1 }));
    transaction.markModelVisible(deliveryId, "2026-08-09T07:00:04.000Z", digest({ visible: 1 }));
    transaction.commitVisibleMessage({
      sessionId,
      target,
      messageId,
      deliveryId,
      attempt: 1,
      serverSeq: 11,
      modelVisibleAck: { observed: true, receiptId: id("rcp", "a") as ReceiptId },
      visibleAt: "2026-08-09T07:00:05.000Z",
    });
  });
  completeTurn(journal);
}

test("completion evidence must join this attempt's contribution exactly", () => {
  const rejected: Array<{
    evidence: TurnCompletionEvidence;
    code: string;
  }> = [
    {
      evidence: completionEvidence({
        coordination: {
          kind: "not_requested",
          terminalTurnId: id("trn", "z") as TurnId,
        },
      }),
      code: "INVALID_JOURNAL_CHAIN",
    },
    {
      evidence: completionEvidence({
        contribution: { fence: { ...fence, turnId: id("trn", "z") as TurnId } },
        coordination: {
          kind: "not_requested",
          terminalTurnId: id("trn", "z") as TurnId,
        },
      }),
      code: "INVALID_JOURNAL_CHAIN",
    },
    {
      evidence: completionEvidence({
        contribution: { stateInstanceId: id("sti", "z") as StateInstanceId },
      }),
      code: "WRITE_STARTED_BINDING_MISMATCH",
    },
    {
      evidence: completionEvidence({
        contribution: { runtimeWriteId: id("cmd", "x") as CommandId },
      }),
      code: "WRITE_STARTED_BINDING_MISMATCH",
    },
    {
      evidence: completionEvidence({
        contribution: { visibilityEventId: id("cmd", "x") as CommandId },
      }),
      code: "WRITE_STARTED_BINDING_MISMATCH",
    },
    {
      evidence: completionEvidence({ contribution: { inputOrdinal: 1 } }),
      code: "WRITE_STARTED_BINDING_MISMATCH",
    },
    {
      evidence: completionEvidence({
        contribution: { permitId: id("cmd", "x") as CommandId },
      }),
      code: "WRITE_STARTED_BINDING_MISMATCH",
    },
  ];
  for (const { evidence, code } of rejected) {
    const journal = openJournal();
    readyForCompletion(journal);
    const before = attemptRows(journal);
    assert.equal(storageCode(() => journal.transaction((transaction) =>
      transaction.recordAttemptCompletion({
        fence,
        stateInstanceId,
        evidence,
        recordedAt: "2026-08-09T07:00:06.000Z",
      }),
    )), code);
    assert.equal(attemptRows(journal), before);
    journal.close();
  }
});

test("committed coordination records the exact server result and consumes", () => {
  const journal = openJournal();
  readyForCompletion(journal);
  const committed = completionEvidence({
    coordination: {
      kind: "committed",
      commandId: id("cmd", "k") as CommandId,
      receiptId: id("rcp", "k") as ReceiptId,
      resultDigest: digest({ coordination: 1 }),
    },
  });
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.recordAttemptCompletion({
      fence,
      stateInstanceId,
      evidence: committed,
      recordedAt: "2026-08-09T07:00:06.000Z",
    }),
  ), { applied: true });
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
  ), { applied: true });
  assert.equal(journal.transaction((transaction) =>
    transaction.readNativeAttempt(deliveryId, 1),
  )?.state, "consumed");
  journal.close();
});

test("consumed refuses a completion row that no longer matches journal truth", () => {
  const journal = openJournal();
  readyForCompletion(journal);
  journal.transaction((transaction) =>
    transaction.recordAttemptCompletion({
      fence,
      stateInstanceId,
      evidence: completionEvidence(),
      recordedAt: "2026-08-09T07:00:06.000Z",
    }),
  );
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path);
  try {
    database.prepare(
      "UPDATE native_attempt_completions SET contribution_binding_digest = ?",
    ).run(digest({ forged: 1 }));
  } finally {
    database.close();
  }
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("model_visible", "consumed"),
    ),
  )), "WRITE_STARTED_BINDING_MISMATCH");
  assert.equal(attemptRows(journal), before);
  journal.close();
});

const turnOwner = digest({ turnPump: 1 });
const turnBinding = digest({ turnBinding: 1 });

function turnFence(): { stateInstanceId: StateInstanceId; sessionId: SessionId; ownerToken: ArtifactDigest; readerEpoch: number } {
  return { stateInstanceId, sessionId, ownerToken: turnOwner, readerEpoch: 1 };
}

function claimTurnReader(journal: DaemonJournal): void {
  journal.transaction((transaction) => {
    transaction.claimDriverEventReader({
      stateInstanceId,
      sessionId,
      ownerToken: turnOwner,
      mode: "start",
      claimedAt: "2026-08-09T07:00:00.100Z",
    });
  });
}

function durable(
  phase: string,
  overrides: Partial<{
    inputOrdinal: number;
    bindingDigest: ArtifactDigest | null;
    steerable: boolean;
    replyCommitted: boolean;
  }> = {},
): never {
  return {
    protocolTurnId: turnId,
    phase,
    inputOrdinal: overrides.inputOrdinal ?? 0,
    bindingDigest: overrides.bindingDigest === undefined ? turnBinding : overrides.bindingDigest,
    steerable: overrides.steerable ?? false,
    replyCommitted: overrides.replyCommitted ?? false,
  } as never;
}

function beginInput(overrides: Record<string, unknown> = {}): never {
  return {
    ...turnFence(),
    protocolTurnId: turnId,
    launchId,
    rootProducerFactId: producerFactId,
    inputOrdinal: 0,
    driverTurnRefDigest: digest({ turnRef: 1 }),
    mode: { kind: "ordinary" },
    bindingDigest: turnBinding,
    expected: null,
    next: durable("write_started"),
    recordedAt: "2026-08-09T07:00:00.200Z",
    ...overrides,
  } as never;
}

function stepInput(ordinal: number, kind: string, expectedPhase: string, nextPhase: string, overrides: Record<string, unknown> = {}): never {
  return {
    ...turnFence(),
    event: {
      ordinal,
      eventDigest: digest({ turnEvent: ordinal }),
      turnId,
      bindingDigest: turnBinding,
    },
    kind,
    expected: durable(expectedPhase),
    next: durable(nextPhase),
    recordedAt: `2026-08-09T07:00:0${Math.min(ordinal + 1, 9)}.000Z`,
    ...overrides,
  } as never;
}

function terminalInput(overrides: Record<string, unknown> = {}): never {
  return {
    ...turnFence(),
    basis: {
      reply: {
        ordinal: 2,
        eventDigest: digest({ turnEvent: "reply" }),
        turnId,
        bindingDigest: turnBinding,
        replyCommandId: id("cmd", "r") as CommandId,
      },
      completed: {
        ordinal: 3,
        eventDigest: digest({ turnEvent: "completed" }),
        turnId,
        bindingDigest: turnBinding,
      },
    },
    evidence: completionEvidence(),
    expected: durable("model_visible"),
    next: durable("completed", { replyCommitted: true }),
    recordedAt: "2026-08-09T07:00:06.000Z",
    ...overrides,
  } as never;
}

function readyForTerminal(journal: DaemonJournal): void {
  reserve(journal);
  bindDelivery(journal);
  claimTurnReader(journal);
  advanceToModelVisible(journal);
  journal.transaction((transaction) => {
    transaction.markInputWritten(deliveryId, "2026-08-09T07:00:03.000Z", digest({ input: 1 }));
    transaction.markModelVisible(deliveryId, "2026-08-09T07:00:04.000Z", digest({ visible: 1 }));
    transaction.commitVisibleMessage({
      sessionId,
      target,
      messageId,
      deliveryId,
      attempt: 1,
      serverSeq: 11,
      modelVisibleAck: { observed: true, receiptId: id("rcp", "a") as ReceiptId },
      visibleAt: "2026-08-09T07:00:05.000Z",
    });
  });
  journal.transaction((transaction) => {
    transaction.beginTurnContribution(beginInput());
    transaction.commitTurnStep(stepInput(0, "input_written", "write_started", "input_written"));
    transaction.commitTurnStep(stepInput(1, "model_visible", "input_written", "model_visible"));
  });
}

function turnStateRows(journal: DaemonJournal): string {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    return JSON.stringify({
      turns: database.prepare(
        `SELECT protocol_turn_id, input_ordinal, state, binding_digest,
                steerable, operation_digest, mode, expected_turn_id
         FROM local_turns ORDER BY protocol_turn_id`,
      ).all(),
      cursor: database.prepare(
        `SELECT state_instance_id, next_ordinal FROM driver_event_cursor
         ORDER BY state_instance_id`,
      ).all(),
      records: database.prepare(
        `SELECT ordinal, event_digest, turn_id, binding_digest, operation_digest
         FROM driver_event_records ORDER BY ordinal`,
      ).all(),
      completions: database.prepare(
        `SELECT delivery_id, attempt, reply_command_id, operation_digest
         FROM native_attempt_completions ORDER BY delivery_id, attempt`,
      ).all(),
    });
  } finally {
    database.close();
  }
}

test("atomic turn port drives begin, steps, and terminal to consumed", () => {
  const journal = openJournal();
  readyForTerminal(journal);
  const preTerminal = journal.transaction((transaction) =>
    transaction.readDurableTurnState({ ...turnFence(), protocolTurnId: turnId }),
  );
  assert.deepEqual(preTerminal, {
    protocolTurnId: turnId,
    phase: "model_visible",
    inputOrdinal: 0,
    bindingDigest: turnBinding,
    steerable: false,
    replyCommitted: false,
  });
  const result = journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput()),
  );
  assert.equal(result.applied, true);
  assert.equal(result.nextOrdinal, 4);
  assert.equal(result.durable.phase, "completed");
  assert.equal(result.durable.replyCommitted, true);
  assert.equal(result.durable.steerable, false);
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
  ), { applied: true });
  journal.close();
});

test("turn port replay aliases under new timestamps and refuses forged transitions", () => {
  const journal = openJournal();
  readyForTerminal(journal);
  assert.equal(journal.transaction((transaction) =>
    transaction.beginTurnContribution(beginInput({ recordedAt: "2026-08-09T09:59:00.000Z" })),
  ).applied, false);
  const before = turnStateRows(journal);
  const replay = journal.transaction((transaction) =>
    transaction.commitTurnStep(stepInput(0, "input_written", "write_started", "input_written", {
      recordedAt: "2026-08-09T09:59:01.000Z",
    })),
  );
  assert.equal(replay.applied, false);
  assert.equal(turnStateRows(journal), before);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.commitTurnStep(stepInput(0, "input_written", "write_started", "model_visible")),
  )), "ACTIVE_TURN_CONFLICT");
  assert.equal(turnStateRows(journal), before);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.beginTurnContribution(beginInput({
      bindingDigest: digest({ turnBinding: 2 }),
      next: durable("write_started", { bindingDigest: digest({ turnBinding: 2 }) }),
    })),
  )), "ACTIVE_TURN_CONFLICT");
  assert.equal(turnStateRows(journal), before);
  journal.close();
});

test("terminal batch is atomic, replay-stable, and refuses divergence", () => {
  const journal = openJournal();
  readyForTerminal(journal);
  assert.equal(journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput()),
  ).applied, true);
  const before = turnStateRows(journal);
  const replay = journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput({ recordedAt: "2026-08-09T09:59:06.000Z" })),
  );
  assert.equal(replay.applied, false);
  assert.equal(turnStateRows(journal), before);
  // The attempt legally advances to consumed; the same logical terminal
  // operation must STILL alias exactly, and divergence must still conflict,
  // both with zero mutation.
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
  ), { applied: true });
  const afterConsumed = turnStateRows(journal);
  const afterConsumedAttempts = attemptRows(journal);
  const postConsumedReplay = journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput({ recordedAt: "2026-08-09T10:59:06.000Z" })),
  );
  assert.equal(postConsumedReplay.applied, false);
  assert.equal(turnStateRows(journal), afterConsumed);
  assert.equal(attemptRows(journal), afterConsumedAttempts);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput({
      evidence: completionEvidence({
        reply: { receiptId: id("rcp", "q") as ReceiptId, resultDigest: digest({ reply: 9 }) },
      }),
    })),
  )), "INVOCATION_STATE_CONFLICT");
  assert.equal(turnStateRows(journal), afterConsumed);
  assert.equal(attemptRows(journal), afterConsumedAttempts);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput({
      basis: {
        reply: {
          ordinal: 2,
          eventDigest: digest({ turnEvent: "reply" }),
          turnId,
          bindingDigest: turnBinding,
          replyCommandId: id("cmd", "z") as CommandId,
        },
        completed: {
          ordinal: 3,
          eventDigest: digest({ turnEvent: "completed" }),
          turnId,
          bindingDigest: turnBinding,
        },
      },
    })),
  )), "INVOCATION_STATE_CONFLICT");
  assert.equal(turnStateRows(journal), before);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput({
      expected: durable("completed", { replyCommitted: true }),
    })),
  )), "INVALID_STATE_TRANSITION");
  journal.close();
});

test("terminal basis enforces roles, order, uniformity, and command distinctness", () => {
  const cases: Array<{ overrides: Record<string, unknown>; code: string }> = [
    {
      overrides: {
        basis: {
          reply: { ordinal: 2, eventDigest: digest({ turnEvent: "reply" }), turnId, bindingDigest: turnBinding, replyCommandId: id("cmd", "r") as CommandId },
          completed: { ordinal: 4, eventDigest: digest({ turnEvent: "completed" }), turnId, bindingDigest: turnBinding },
        },
      },
      code: "DRIVER_EVENT_ORDER_INVALID",
    },
    {
      overrides: {
        basis: {
          reply: { ordinal: 2, eventDigest: digest({ turnEvent: "reply" }), turnId, bindingDigest: digest({ turnBinding: 9 }), replyCommandId: id("cmd", "r") as CommandId },
          completed: { ordinal: 3, eventDigest: digest({ turnEvent: "completed" }), turnId, bindingDigest: turnBinding },
        },
      },
      code: "INVALID_STATE_TRANSITION",
    },
    {
      overrides: {
        basis: {
          reply: { ordinal: 2, eventDigest: digest({ turnEvent: "reply" }), turnId, bindingDigest: turnBinding, replyCommandId: id("cmd", "r") as CommandId },
          coordination: { ordinal: 3, eventDigest: digest({ turnEvent: "coordination" }), turnId, bindingDigest: turnBinding, commandId: id("cmd", "r") as CommandId, commandDigest: digest({ command: 1 }) },
          completed: { ordinal: 4, eventDigest: digest({ turnEvent: "completed" }), turnId, bindingDigest: turnBinding },
        },
      },
      code: "SECOND_COORDINATION_CALL",
    },
    {
      overrides: {
        basis: {
          reply: { ordinal: 2, eventDigest: digest({ turnEvent: "reply" }), turnId, bindingDigest: turnBinding, replyCommandId: id("cmd", "r") as CommandId },
          coordination: { ordinal: 3, eventDigest: digest({ turnEvent: "coordination" }), turnId, bindingDigest: turnBinding, commandId: id("cmd", "k") as CommandId, commandDigest: digest({ command: 1 }) },
          completed: { ordinal: 4, eventDigest: digest({ turnEvent: "completed" }), turnId, bindingDigest: turnBinding },
        },
      },
      code: "SECOND_COORDINATION_CALL",
    },
  ];
  for (const { overrides, code } of cases) {
    const journal = openJournal();
    readyForTerminal(journal);
    const before = turnStateRows(journal);
    assert.equal(storageCode(() => journal.transaction((transaction) =>
      transaction.commitTurnTerminal(terminalInput(overrides)),
    )), code);
    assert.equal(turnStateRows(journal), before);
    journal.close();
  }
});

test("terminal commit joins the contribution to stored entry truth", () => {
  const forged: Array<{ contribution: Record<string, unknown> }> = [
    { contribution: { runtimeWriteId: id("cmd", "x") as CommandId } },
    { contribution: { visibilityEventId: id("cmd", "x") as CommandId } },
    {
      contribution: {
        runtimeWriteId: id("cmd", "x") as CommandId,
        visibilityEventId: id("cmd", "y") as CommandId,
      },
    },
  ];
  for (const { contribution } of forged) {
    const journal = openJournal();
    readyForTerminal(journal);
    const beforeTurn = turnStateRows(journal);
    const beforeAttempt = attemptRows(journal);
    assert.equal(storageCode(() => journal.transaction((transaction) =>
      transaction.commitTurnTerminal(terminalInput({
        evidence: completionEvidence({ contribution }),
      })),
    )), "WRITE_STARTED_BINDING_MISMATCH");
    assert.equal(turnStateRows(journal), beforeTurn);
    assert.equal(attemptRows(journal), beforeAttempt);
    journal.close();
  }
});

function tamperVisibleEntry(
  journal: DaemonJournal,
  mutate: (entry: Record<string, unknown>) => void,
  resign: boolean,
): void {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path);
  try {
    const stored = database.prepare(
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = 1 AND kind = 'model_visible'`,
    ).get(deliveryId) as { entry_json: string };
    const entry = JSON.parse(stored.entry_json) as Record<string, unknown>;
    mutate(entry);
    if (resign) {
      const unsigned = Object.fromEntries(
        Object.entries(entry).filter(([key]) => key !== "entryDigest"),
      );
      entry.entryDigest = digest(unsigned);
    }
    database.prepare(
      `UPDATE native_invocation_entries SET entry_json = ?
       WHERE delivery_id = ? AND attempt = 1 AND kind = 'model_visible'`,
    ).run(JSON.stringify(entry), deliveryId);
  } finally {
    database.close();
  }
}

function tamperEntryPairSequences(journal: DaemonJournal): void {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path);
  try {
    const load = (kind: string): Record<string, unknown> => JSON.parse((database.prepare(
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = 1 AND kind = ?`,
    ).get(deliveryId, kind) as { entry_json: string }).entry_json) as Record<string, unknown>;
    const resign = (entry: Record<string, unknown>): Record<string, unknown> => {
      const unsigned = Object.fromEntries(
        Object.entries(entry).filter(([key]) => key !== "entryDigest"),
      );
      entry.entryDigest = digest(unsigned);
      return entry;
    };
    const written = load("input_written");
    written.sequence = 99;
    resign(written);
    const visible = load("model_visible");
    visible.sequence = 100;
    visible.previousEntryDigest = written.entryDigest;
    resign(visible);
    // JSON-consistent 99/100 pair with an intact digest chain, but the
    // relational sequence columns still hold the original ordinals.
    database.prepare(
      `UPDATE native_invocation_entries SET entry_json = ?
       WHERE delivery_id = ? AND attempt = 1 AND kind = 'input_written'`,
    ).run(JSON.stringify(written), deliveryId);
    database.prepare(
      `UPDATE native_invocation_entries SET entry_json = ?
       WHERE delivery_id = ? AND attempt = 1 AND kind = 'model_visible'`,
    ).run(JSON.stringify(visible), deliveryId);
  } finally {
    database.close();
  }
}

test("terminal join binds input_written to the stored write_started predecessor", () => {
  for (const postConsumed of [false, true]) {
    const journal = openJournal();
    readyForTerminal(journal);
    if (postConsumed) {
      journal.transaction((transaction) => transaction.commitTurnTerminal(terminalInput()));
      journal.transaction((transaction) =>
        transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
      );
    }
    // Disconnect input_written from write_started: forge its predecessor
    // digest, re-sign it, then relink and re-sign model_visible so the
    // written→visible half of the chain stays intact. Fence/runtime
    // identities and relational ordinals unchanged.
    {
      const path = journalPaths.get(journal);
      if (path === undefined) assert.fail("journal path is not registered");
      const database = new DatabaseSync(path);
      try {
        const load = (kind: string): Record<string, unknown> => JSON.parse((database.prepare(
          `SELECT entry_json FROM native_invocation_entries
           WHERE delivery_id = ? AND attempt = 1 AND kind = ?`,
        ).get(deliveryId, kind) as { entry_json: string }).entry_json) as Record<string, unknown>;
        const resign = (entry: Record<string, unknown>): Record<string, unknown> => {
          const unsigned = Object.fromEntries(
            Object.entries(entry).filter(([key]) => key !== "entryDigest"),
          );
          entry.entryDigest = digest(unsigned);
          return entry;
        };
        const written = load("input_written");
        written.previousEntryDigest = digest({ forged: "predecessor" });
        resign(written);
        const visible = load("model_visible");
        visible.previousEntryDigest = written.entryDigest;
        resign(visible);
        database.prepare(
          `UPDATE native_invocation_entries SET entry_json = ?
           WHERE delivery_id = ? AND attempt = 1 AND kind = 'input_written'`,
        ).run(JSON.stringify(written), deliveryId);
        database.prepare(
          `UPDATE native_invocation_entries SET entry_json = ?
           WHERE delivery_id = ? AND attempt = 1 AND kind = 'model_visible'`,
        ).run(JSON.stringify(visible), deliveryId);
      } finally {
        database.close();
      }
    }
    const beforeTurn = turnStateRows(journal);
    const beforeAttempt = attemptRows(journal);
    assert.equal(storageCode(() => journal.transaction((transaction) =>
      transaction.commitTurnTerminal(terminalInput(
        postConsumed ? { recordedAt: "2026-08-09T13:59:06.000Z" } : {},
      )),
    )), "INVALID_JOURNAL_CHAIN");
    assert.equal(turnStateRows(journal), beforeTurn);
    assert.equal(attemptRows(journal), beforeAttempt);
    journal.close();
  }
});

function tamperStartedAndRelink(
  journal: DaemonJournal,
  mutate: (entry: Record<string, unknown>) => void,
): void {
  const path = journalPaths.get(journal);
  if (path === undefined) assert.fail("journal path is not registered");
  const database = new DatabaseSync(path);
  try {
    const load = (kind: string): Record<string, unknown> => JSON.parse((database.prepare(
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = 1 AND kind = ?`,
    ).get(deliveryId, kind) as { entry_json: string }).entry_json) as Record<string, unknown>;
    const resign = (entry: Record<string, unknown>): Record<string, unknown> => {
      const unsigned = Object.fromEntries(
        Object.entries(entry).filter(([key]) => key !== "entryDigest"),
      );
      entry.entryDigest = digest(unsigned);
      return entry;
    };
    const save = (kind: string, entry: Record<string, unknown>): void => {
      database.prepare(
        `UPDATE native_invocation_entries SET entry_json = ?
         WHERE delivery_id = ? AND attempt = 1 AND kind = ?`,
      ).run(JSON.stringify(entry), deliveryId, kind);
    };
    const started = load("write_started");
    mutate(started);
    resign(started);
    const written = load("input_written");
    written.previousEntryDigest = started.entryDigest;
    resign(written);
    const visible = load("model_visible");
    visible.previousEntryDigest = written.entryDigest;
    resign(visible);
    save("write_started", started);
    save("input_written", written);
    save("model_visible", visible);
  } finally {
    database.close();
  }
}

test("terminal join anchors the chain at permit_recorded and binds inputDigest", () => {
  const corruptions: Array<{
    mutate: (entry: Record<string, unknown>) => void;
    code: string;
  }> = [
    // disconnect write_started from its permit_recorded root
    { mutate: (entry) => { entry.previousEntryDigest = digest({ forged: "root" }); }, code: "INVALID_JOURNAL_CHAIN" },
    // forge a valid-looking inputDigest that no longer matches hardened body_digest
    { mutate: (entry) => { entry.inputDigest = digest({ forged: "input" }); }, code: "WRITE_STARTED_BINDING_MISMATCH" },
  ];
  for (const { mutate, code } of corruptions) {
    for (const postConsumed of [false, true]) {
      const journal = openJournal();
      readyForTerminal(journal);
      if (postConsumed) {
        journal.transaction((transaction) => transaction.commitTurnTerminal(terminalInput()));
        journal.transaction((transaction) =>
          transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
        );
      }
      tamperStartedAndRelink(journal, mutate);
      const beforeTurn = turnStateRows(journal);
      const beforeAttempt = attemptRows(journal);
      assert.equal(storageCode(() => journal.transaction((transaction) =>
        transaction.commitTurnTerminal(terminalInput(
          postConsumed ? { recordedAt: "2026-08-09T14:59:06.000Z" } : {},
        )),
      )), code);
      assert.equal(turnStateRows(journal), beforeTurn);
      assert.equal(attemptRows(journal), beforeAttempt);
      journal.close();
    }
  }
});

test("terminal join binds relational sequence and refuses a resigned ordinal pair", () => {
  for (const postConsumed of [false, true]) {
    const journal = openJournal();
    readyForTerminal(journal);
    if (postConsumed) {
      journal.transaction((transaction) => transaction.commitTurnTerminal(terminalInput()));
      journal.transaction((transaction) =>
        transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
      );
    }
    tamperEntryPairSequences(journal);
    const beforeTurn = turnStateRows(journal);
    const beforeAttempt = attemptRows(journal);
    assert.equal(storageCode(() => journal.transaction((transaction) =>
      transaction.commitTurnTerminal(terminalInput(
        postConsumed ? { recordedAt: "2026-08-09T12:59:06.000Z" } : {},
      )),
    )), "INVALID_JOURNAL_CHAIN");
    assert.equal(turnStateRows(journal), beforeTurn);
    assert.equal(attemptRows(journal), beforeAttempt);
    journal.close();
  }
});

test("terminal join validates the full stored entry chain, not just runtime IDs", () => {
  const corruptions: Array<{
    mutate: (entry: Record<string, unknown>) => void;
    resign: boolean;
    code: string;
  }> = [
    { mutate: (entry) => { entry.membershipEpoch = 2; }, resign: true, code: "WRITE_STARTED_BINDING_MISMATCH" },
    { mutate: (entry) => { entry.entryId = id("cmd", "z"); }, resign: false, code: "INVALID_JOURNAL_CHAIN" },
    { mutate: (entry) => { entry.previousEntryDigest = digest({ forged: "link" }); }, resign: true, code: "INVALID_JOURNAL_CHAIN" },
    // journalId !== invocationId (Protocol SSOT journal() invariant), resigned
    { mutate: (entry) => { entry.journalId = id("cmd", "z"); }, resign: true, code: "INVALID_JOURNAL_CHAIN" },
  ];
  for (const { mutate, resign, code } of corruptions) {
    {
      const journal = openJournal();
      readyForTerminal(journal);
      tamperVisibleEntry(journal, mutate, resign);
      const beforeTurn = turnStateRows(journal);
      const beforeAttempt = attemptRows(journal);
      assert.equal(storageCode(() => journal.transaction((transaction) =>
        transaction.commitTurnTerminal(terminalInput()),
      )), code);
      assert.equal(turnStateRows(journal), beforeTurn);
      assert.equal(attemptRows(journal), beforeAttempt);
      journal.close();
    }
    {
      const journal = openJournal();
      readyForTerminal(journal);
      journal.transaction((transaction) =>
        transaction.commitTurnTerminal(terminalInput()),
      );
      journal.transaction((transaction) =>
        transaction.transitionNativeAttempt(transitionInput("model_visible", "consumed")),
      );
      tamperVisibleEntry(journal, mutate, resign);
      const beforeTurn = turnStateRows(journal);
      const beforeAttempt = attemptRows(journal);
      assert.equal(storageCode(() => journal.transaction((transaction) =>
        transaction.commitTurnTerminal(terminalInput({ recordedAt: "2026-08-09T11:59:06.000Z" })),
      )), code);
      assert.equal(turnStateRows(journal), beforeTurn);
      assert.equal(attemptRows(journal), beforeAttempt);
      journal.close();
    }
  }
});

test("steer admission is whole-state guarded and settle repairs are closed", () => {
  const journal = openJournal();
  readyForTerminal(journal);
  const before = turnStateRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.beginTurnContribution(beginInput({
      mode: { kind: "steer", expectedTurnId: turnId },
      inputOrdinal: 1,
      expected: durable("model_visible", { steerable: true }),
      next: durable("write_started", { inputOrdinal: 1 }),
    })),
  )), "ACTIVE_TURN_CONFLICT");
  assert.equal(turnStateRows(journal), before);
  const settle = {
    ...turnFence(),
    protocolTurnId: turnId,
    inputOrdinal: 0,
    kind: "ambiguous",
    expected: durable("model_visible"),
    next: durable("ambiguous"),
    recordedAt: "2026-08-09T07:00:07.000Z",
  } as never;
  assert.equal(journal.transaction((transaction) =>
    transaction.settleTurnContribution(settle),
  ).applied, true);
  const settled = turnStateRows(journal);
  assert.equal(journal.transaction((transaction) =>
    transaction.settleTurnContribution(settle),
  ).applied, false);
  assert.equal(turnStateRows(journal), settled);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.settleTurnContribution({
      ...(settle as Record<string, unknown>),
      kind: "interrupted",
      next: durable("interrupted"),
    } as never),
  )), "ACTIVE_TURN_CONFLICT");
  assert.equal(turnStateRows(journal), settled);
  journal.close();
});

test("turn port refuses stale readers on every surface", () => {
  const journal = openJournal();
  readyForTerminal(journal);
  const stale = { ...turnFence(), readerEpoch: 9 };
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.readDurableTurnState({ ...stale, protocolTurnId: turnId }),
  )), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.commitTurnStep(stepInput(2, "turn_boundary", "model_visible", "model_visible", stale)),
  )), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.commitTurnTerminal(terminalInput(stale)),
  )), "DRIVER_EVENT_FENCE_MISMATCH");
  journal.close();
});

function reserveFake(journal: DaemonJournal): void {
  journal.transaction((transaction) => {
    transaction.reserveLaunch({
      agentId,
      machineId,
      launchId,
      runtime: "scripted_fake",
      routingGeneration: 0,
      workspaceGeneration: 1,
      stopEpoch: 0,
      queueOrdinal: 1,
      driverIdentityDigest: digest({ driver: "fake" }),
      queuedAt: "2026-08-09T07:00:00.000Z",
    });
  });
}

function scriptedProof(
  started: { invocationId: CommandId; entryId: CommandId; entryDigest: ArtifactDigest },
  generation = 1,
): Record<string, unknown> {
  const unsigned = {
    driverKind: "scripted_fake",
    fixtureId: id("cmd", "f") as CommandId,
    scriptDigest: digest({ script: 1 }),
    invocationId: started.invocationId,
    invocationGeneration: generation,
    writeStartedEntryId: started.entryId,
    writeStartedEntryDigest: started.entryDigest,
    outcomeOrdinal: 1,
    outcome: "not_written",
  };
  return { ...unsigned, proofDigest: digest(unsigned) };
}

test("not_written accepts only the exact canonical scripted proof", () => {
  const journal = openJournal();
  reserveFake(journal);
  bindDelivery(journal);
  const permit = boundEntry("permit_recorded", 1, null, {});
  const started = boundEntry("write_started", 2, permit.entryDigest, {
    inputDigest: bodyDigestGen1,
  });
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", { permitId: permit.permitId }),
    );
    transaction.appendNativeInvocationEntry({ entry: started });
    transaction.transitionNativeAttempt(
      transitionInput("permit_recorded", "write_started", {
        invocationGeneration: 1,
        invocationId: started.invocationId,
        bodyDigest: bodyDigestGen1,
      }),
    );
  });
  const before = attemptRows(journal);
  const good = scriptedProof(started);

  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "not_written", {
        notWrittenProofJson: JSON.stringify({ ...good, driverKind: "native_process" }),
      }),
    ),
  )), "REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN");
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "not_written", {
        notWrittenProofJson: JSON.stringify({ ...good, rawPayload: "secret" }),
      }),
    ),
  )), "FAKE_NOT_WRITTEN_PROOF_REQUIRED");
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "not_written", {
        notWrittenProofJson: JSON.stringify({
          ...good,
          writeStartedEntryDigest: digest({ wrong: true }),
        }),
      }),
    ),
  )), "WRITE_STARTED_BINDING_MISMATCH");
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "not_written", {
        notWrittenProofJson: JSON.stringify({ ...good, proofDigest: digest({ x: 1 }) }),
      }),
    ),
  )), "INVALID_JOURNAL_CHAIN");
  assert.equal(attemptRows(journal), before);

  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "not_written", {
        notWrittenProofJson: JSON.stringify(good),
      }),
    ),
  ), { applied: true });

  const secondStarted = boundEntry("write_started", 3, started.entryDigest, {
    inputDigest: digest({ body: 2 }),
  }, 2);
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: secondStarted });
  });
  const held = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("not_written", "write_started", {
        invocationGeneration: 2,
        invocationId: secondStarted.invocationId,
        bodyDigest: digest({ body: 2 }),
        expectedPreviousProofDigest: digest({ wrong: true }),
      }),
    ),
  )), "STALE_INVOCATION_GENERATION");
  assert.equal(attemptRows(journal), held);
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("not_written", "write_started", {
        invocationGeneration: 2,
        invocationId: secondStarted.invocationId,
        bodyDigest: digest({ body: 2 }),
        expectedPreviousProofDigest: good.proofDigest as ArtifactDigest,
      }),
    ),
  ), { applied: true });
  journal.close();
});

test("real-driver launches cannot record a not_written proof at all", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const permit = boundEntry("permit_recorded", 1, null, {});
  const started = boundEntry("write_started", 2, permit.entryDigest, {
    inputDigest: bodyDigestGen1,
  });
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", { permitId: permit.permitId }),
    );
    transaction.appendNativeInvocationEntry({ entry: started });
    transaction.transitionNativeAttempt(
      transitionInput("permit_recorded", "write_started", {
        invocationGeneration: 1,
        invocationId: started.invocationId,
        bodyDigest: bodyDigestGen1,
      }),
    );
  });
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "not_written", {
        notWrittenProofJson: JSON.stringify(scriptedProof(started)),
      }),
    ),
  )), "REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN");
  assert.equal(attemptRows(journal), before);
  journal.close();
});

test("ambiguous holds the attempt and refuses a new write without reconciliation", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const permit = boundEntry("permit_recorded", 1, null, {});
  const started = boundEntry("write_started", 2, permit.entryDigest, {
    inputDigest: bodyDigestGen1,
  });
  journal.transaction((transaction) => {
    transaction.appendNativeInvocationEntry({ entry: permit });
    transaction.transitionNativeAttempt(
      transitionInput("accepted", "permit_recorded", { permitId: permit.permitId }),
    );
    transaction.appendNativeInvocationEntry({ entry: started });
    transaction.transitionNativeAttempt(
      transitionInput("permit_recorded", "write_started", {
        invocationGeneration: 1,
        invocationId: started.invocationId,
        bodyDigest: bodyDigestGen1,
      }),
    );
    transaction.transitionNativeAttempt(
      transitionInput("write_started", "ambiguous", {
        disconnectId: id("cmd", "z") as CommandId,
      }),
    );
  });
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("ambiguous", "write_started", {
        invocationGeneration: 2,
        invocationId: started.invocationId,
        bodyDigest: digest({ body: 2 }),
      }),
    ),
  )), "INVALID_STATE_TRANSITION");
  assert.equal(attemptRows(journal), before);
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.transitionNativeAttempt(
      transitionInput("ambiguous", "suppressed", {
        suppressionReason: "reconciled_already_visible",
      }),
    ),
  ), { applied: true });
  journal.close();
});

test("stale fence transitions mutate nothing", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  const before = attemptRows(journal);
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt({
      fence: { ...fence, launchId: id("lnc", "b") as LaunchId },
      stateInstanceId,
      expectedState: "accepted",
      nextState: "suppressed",
      suppressionReason: "stale",
    } as never),
  )), "STALE_DELIVERY_FENCE");
  assert.equal(storageCode(() => journal.transaction((transaction) =>
    transaction.transitionNativeAttempt({
      fence,
      stateInstanceId: id("sti", "b") as StateInstanceId,
      expectedState: "accepted",
      nextState: "suppressed",
      suppressionReason: "stale",
    } as never),
  )), "STALE_DELIVERY_FENCE");
  assert.equal(attemptRows(journal), before);
  journal.close();
});

test("visible-message readback returns the exact ledger row for suppression rechecks", () => {
  const journal = openJournal();
  reserve(journal);
  bindDelivery(journal);
  assert.equal(journal.transaction((transaction) =>
    transaction.readVisibleMessage({ sessionId, target, messageId }),
  ), undefined);
  journal.transaction((transaction) => {
    transaction.markInputWritten(deliveryId, "2026-08-09T07:00:03.000Z", digest({ input: 1 }));
    transaction.markModelVisible(deliveryId, "2026-08-09T07:00:04.000Z", digest({ visible: 1 }));
    transaction.commitVisibleMessage({
      sessionId,
      target,
      messageId,
      deliveryId,
      attempt: 1,
      serverSeq: 11,
      modelVisibleAck: { observed: true, receiptId: id("rcp", "a") as ReceiptId },
      visibleAt: "2026-08-09T07:00:05.000Z",
    });
  });
  assert.deepEqual(journal.transaction((transaction) =>
    transaction.readVisibleMessage({ sessionId, target, messageId }),
  ), {
    deliveryId,
    attempt: 1,
    serverSeq: 11,
    modelVisibleReceiptId: id("rcp", "a") as ReceiptId,
    visibleAt: "2026-08-09T07:00:05.000Z",
  });
  journal.close();
});
