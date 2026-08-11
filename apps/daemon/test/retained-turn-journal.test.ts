import assert from "node:assert/strict";
import { test } from "node:test";

import type { ArtifactDigest, CommandId, LaunchId, ProtocolVersion, SessionId, StateInstanceId } from "@swarm/protocol";
import type { RuntimeJournalTransaction, TurnStepResult } from "@swarm/storage";
import type {
  DriverPrivateEventRetentionPort,
  RetainedDriverEventLease,
} from "@swarm/drivers";

import { RetainedTurnJournal } from "../src/retained-turn-journal.js";

const stateInstanceId = id("sti", "1") as StateInstanceId;
const sessionId = id("ses", "2") as SessionId;
const ownerToken = digest("3");
const currentDigest = digest("4");
const lease = {
  protocolVersion: 2 as ProtocolVersion,
  launchId: id("lnc", "5") as LaunchId,
  stateInstanceId,
  sessionId,
  ownerToken,
  readerEpoch: 4,
  nextOrdinal: 1,
  lastEventDigest: digest("6"),
  claimAttemptId: id("cmd", "7") as CommandId,
  processMode: "resume" as const,
  replayMode: "live" as const,
  snapshotHeadNextOrdinal: 8,
};

test("old step and terminal replays ACK the current storage cursor, never the old input digest", () => {
  const acks: Array<{ nextOrdinal: number; lastEventDigest: ArtifactDigest | null }> = [];
  const retention = {
    acknowledgeCommitted(_lease: RetainedDriverEventLease, ack: typeof acks[number]) {
      acks.push(ack);
      return { applied: true };
    },
  } as unknown as DriverPrivateEventRetentionPort;
  const result: TurnStepResult = {
    applied: false,
    durable: {} as TurnStepResult["durable"],
    nextOrdinal: 8,
    lastEventDigest: currentDigest,
  };
  const transaction = {
    commitTurnStep: () => result,
    commitTurnTerminal: () => result,
  } as unknown as RuntimeJournalTransaction;
  const wrapper = new RetainedTurnJournal(retention);
  const fence = { stateInstanceId, sessionId, ownerToken, readerEpoch: 4 };
  assert.equal(wrapper.commitStep(transaction, lease, fence as never), result);
  assert.equal(wrapper.commitTerminal(transaction, lease, fence as never), result);
  assert.deepEqual(acks, [
    { stateInstanceId, sessionId, nextOrdinal: 8, lastEventDigest: currentDigest },
    { stateInstanceId, sessionId, nextOrdinal: 8, lastEventDigest: currentDigest },
  ]);
});

test("ACK wrapper fences wrong owner and epoch before either storage or private mutation", () => {
  let storageCalls = 0;
  let ackCalls = 0;
  const transaction = {
    commitTurnStep: () => { storageCalls += 1; throw new Error("unexpected"); },
  } as unknown as RuntimeJournalTransaction;
  const retention = {
    acknowledgeCommitted: () => { ackCalls += 1; return { applied: true }; },
  } as unknown as DriverPrivateEventRetentionPort;
  const wrapper = new RetainedTurnJournal(retention);
  assert.throws(() => wrapper.commitStep(transaction, lease, {
    stateInstanceId,
    sessionId,
    ownerToken: digest("8"),
    readerEpoch: 4,
  } as never));
  assert.equal(storageCalls, 0);
  assert.equal(ackCalls, 0);
});

function id(prefix: string, marker: string): string {
  return `${prefix}_${marker.repeat(26)}`;
}

function digest(marker: string): ArtifactDigest {
  return `sha256:${marker.repeat(64)}` as ArtifactDigest;
}
