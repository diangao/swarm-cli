import assert from "node:assert/strict";
import { test } from "node:test";

import type {
  ArtifactDigest,
  CommandId,
  LaunchId,
  MessageId,
  ProtocolVersion,
  SessionId,
  StateInstanceId,
  TurnId,
} from "@swarm/protocol";

import {
  DriverRetentionError,
  FiniteDriverRetainedReplay,
  RetainedDriverEventPump,
  driverArtifactDigest,
  retainedEventDigest,
  retainedRecordDigest,
  validateRetainedSnapshot,
  type DriverCompositeCursorClaimHandle,
  type DriverEventObservation,
  type DriverPrivateClaimAttempt,
  type DriverPrivateEventRetentionPort,
  type PreparedRetainedDriverEventEnvelope,
  type RetainedDriverEventLease,
  type RetainedDriverEventRecord,
} from "../src/retained-events.js";

const version = 1 as ProtocolVersion;
const launchId = id("lch", "1") as LaunchId;
const stateInstanceId = id("sti", "2") as StateInstanceId;
const sessionId = id("ses", "3") as SessionId;
const turnId = id("trn", "4") as TurnId;
const waiterId = id("cmd", "5") as CommandId;
const lifecycleWaiterId = id("cmd", "6") as CommandId;
const sourceMessageId = id("msg", "7") as MessageId;
const ownerToken = hash("8");
const bindingDigest = hash("9");
const claimAttemptId = id("cmd", "a") as CommandId;

test("per-lease FIFO allocates concurrent turn callbacks consecutively before lifecycle delivery", async () => {
  const retention = new MemoryRetention();
  const pump = new RetainedDriverEventPump(stateInstanceId, retention);
  const lease = await pump.claimCursor(attempt());
  await pump.registerWaiter(lease, {
    kind: "turn",
    waiterId,
    stateInstanceId,
    sessionId,
    turnId,
    bindingDigest,
  });
  await pump.registerWaiter(lease, {
    kind: "initialize",
    waiterId: lifecycleWaiterId,
    stateInstanceId,
    sessionId,
    bindingDigest: hash("b"),
  });
  const observed: string[] = [];
  const subscriber = (async () => {
    for await (const record of pump.subscribe(lease)) {
      observed.push(`${record.stream}:${record.event.kind}:${record.ordinal}`);
      if (observed.length === 3) return;
    }
  })();
  assert.throws(
    () => pump.subscribe(lease),
    (error: unknown) => error instanceof Error && error.message === "DRIVER_EVENT_FENCE_MISMATCH",
  );

  const first = pump.enqueue(lease, [observation({
    kind: "assistant_reply",
    turnId,
    text: "first private reply",
  })]);
  const second = pump.enqueue(lease, [observation({ kind: "turn_completed", turnId })]);
  const terminal = pump.enqueue(lease, [{
    stream: "lifecycle",
    resolvedWaiterId: lifecycleWaiterId,
    bindingDigest: hash("b"),
    event: { kind: "runtime_terminal", reason: "process_exited" },
  }]);
  const [firstRecords, secondRecords, terminalRecords] = await Promise.all([first, second, terminal]);
  await subscriber;

  assert.equal(firstRecords[0]?.ordinal, 0);
  assert.equal(secondRecords[0]?.ordinal, 1);
  assert.equal(terminalRecords[0]?.stream, "lifecycle");
  assert.deepEqual(observed, [
    "turn:assistant_reply:0",
    "turn:turn_completed:1",
    "lifecycle:runtime_terminal:0",
  ]);
  assert.deepEqual(retention.preparedOrdinals, [0, 1]);
});

test("a live claim exposes the retained unacknowledged prefix before new observations", async () => {
  const retention = new MemoryRetention();
  const original = await retention.claim(attempt());
  retention.appendPrepared(original, retention.prepareAppend(original, observation({
    kind: "assistant_reply",
    turnId,
    text: "retained first",
  })));
  retention.appendPrepared(original, retention.prepareAppend(original, observation({
    kind: "turn_completed",
    turnId,
  })));
  await retention.releaseAttempt();

  const pump = new RetainedDriverEventPump(stateInstanceId, retention);
  const resumed = await pump.claimCursor({
    ...attempt(),
    readerEpoch: 2,
    claimAttemptId: id("cmd", "b") as CommandId,
    processMode: "resume",
  });
  const records: RetainedDriverEventRecord[] = [];
  for await (const record of pump.subscribe(resumed)) {
    if (record.stream === "turn") records.push(record);
    if (records.length === 2) break;
  }
  assert.deepEqual(records.map((record) => record.event.kind), [
    "assistant_reply",
    "turn_completed",
  ]);
});

test("waiters use nullable empty snapshots and strict later-record causality", async () => {
  const retention = new MemoryRetention();
  const pump = new RetainedDriverEventPump(stateInstanceId, retention);
  const lease = await pump.claimCursor(attempt());
  const waiterSpec = {
    kind: "turn",
    waiterId,
    stateInstanceId,
    sessionId,
    turnId,
    bindingDigest,
  } as const;
  const waiter = await pump.registerWaiter(lease, waiterSpec);
  assert.equal(waiter.registeredThroughOrdinal, null);
  const waiting = pump.waitForRecord(lease, waiter);
  await pump.enqueue(lease, [observation({
    kind: "model_visible",
    turnId,
    visibilityEventId: id("cmd", "f") as CommandId,
  })]);
  assert.equal((await waiting).ordinal, 0);

  await pump.cancelWaiter(lease, waiterSpec);
  const nextWaiter = await pump.registerWaiter(lease, waiterSpec);
  assert.equal(nextWaiter.registeredThroughOrdinal, 0);
  const waitingForNext = pump.waitForRecord(lease, nextWaiter);
  await pump.enqueue(lease, [observation({
    kind: "model_visible",
    turnId,
    visibilityEventId: id("cmd", "e") as CommandId,
  })]);
  assert.equal((await waitingForNext).ordinal, 1);
});

test("retained replay validates the whole finite suffix before yielding and closes once", async () => {
  const retention = new MemoryRetention();
  const live = await retention.claim(attempt());
  const reply = retention.appendPrepared(live, retention.prepareAppend(live, observation({
    kind: "assistant_reply",
    turnId,
    text: "private reply marker",
  }))).record;
  const completed = retention.appendPrepared(live, retention.prepareAppend(live, observation({
    kind: "turn_completed",
    turnId,
  }))).record;
  const replayLease = { ...live, processMode: "resume", replayMode: "retained_only", snapshotHeadNextOrdinal: 2 } as const;
  let releases = 0;
  const claim: DriverCompositeCursorClaimHandle = {
    authority: replayLease,
    privateLease: replayLease,
    async abort() { releases += 1; return closeResult(releases === 1); },
    async release() { releases += 1; return closeResult(releases === 1); },
  };
  const replay = new FiniteDriverRetainedReplay(claim, [reply, completed], expectation());
  assert.deepEqual((await collect(replay.records)).map((record) => record.event.kind), [
    "assistant_reply",
    "turn_completed",
  ]);
  const firstClose = replay.close();
  assert.equal(firstClose, replay.close());
  await firstClose;
  assert.equal(releases, 1);
});

test("retained replay accepts legal finite prefixes at the frozen snapshot head", async (t) => {
  const cases: Array<[
    string,
    readonly DriverEventObservation["event"][],
    readonly RetainedDriverEventRecord["event"]["kind"][],
  ]> = [
    ["empty", [], []],
    ["boundary", [{
      kind: "turn_boundary",
      turnId,
      boundary: "tool",
      steerable: true,
    }], ["turn_boundary"]],
    ["reply", [{
      kind: "assistant_reply",
      turnId,
      text: "private reply marker",
    }], ["assistant_reply"]],
    ["reply and coordination", [{
      kind: "assistant_reply",
      turnId,
      text: "private reply marker",
    }, {
      kind: "coordination_call",
      turnId,
      commandId: id("cmd", "f") as CommandId,
      command: {
        protocolVersion: version,
        title: "Follow up",
        sourceMessageId,
      },
    }], ["assistant_reply", "coordination_call"]],
    ["completed", [{
      kind: "assistant_reply",
      turnId,
      text: "private reply marker",
    }, {
      kind: "turn_completed",
      turnId,
    }], ["assistant_reply", "turn_completed"]],
  ];

  for (const [name, events, kinds] of cases) {
    await t.test(name, async () => {
      const retention = new MemoryRetention();
      const live = await retention.claim(attempt());
      const records = events.map((event) => retention.appendPrepared(
        live,
        retention.prepareAppend(live, observation(event)),
      ).record);
      const replayLease = {
        ...live,
        processMode: "resume",
        replayMode: "retained_only",
        snapshotHeadNextOrdinal: records.length,
      } as const;
      const claim: DriverCompositeCursorClaimHandle = {
        authority: replayLease,
        privateLease: replayLease,
        async abort() { return closeResult(true); },
        async release() { return closeResult(true); },
      };
      const replay = new FiniteDriverRetainedReplay(claim, records, expectation());
      assert.deepEqual((await collect(replay.records)).map((record) => record.event.kind), kinds);
      await replay.close();
    });
  }
});

test("retained prefix finalization rejects illegal turn order before first yield", async (t) => {
  const coordination = (character: string): DriverEventObservation["event"] => ({
    kind: "coordination_call",
    turnId,
    commandId: id("cmd", character) as CommandId,
    command: {
      protocolVersion: version,
      title: "Follow up",
      sourceMessageId,
    },
  });
  const reply = (text: string): DriverEventObservation["event"] => ({
    kind: "assistant_reply",
    turnId,
    text,
  });
  const cases: Array<[string, readonly DriverEventObservation["event"][]]> = [
    ["coordination before reply", [coordination("1")]],
    ["completion before reply", [{ kind: "turn_completed", turnId }]],
    ["duplicate reply", [reply("first"), reply("second")]],
    ["duplicate coordination", [reply("first"), coordination("2"), coordination("3")]],
    ["event after completion", [
      reply("first"),
      { kind: "turn_completed", turnId },
      reply("after completion"),
    ]],
    ["pre-watermark event", [{
      kind: "model_visible",
      turnId,
      visibilityEventId: id("cmd", "4") as CommandId,
    }]],
  ];

  for (const [name, events] of cases) {
    await t.test(name, async () => {
      const retention = new MemoryRetention();
      const live = await retention.claim(attempt());
      const records = events.map((event) => retention.appendPrepared(
        live,
        retention.prepareAppend(live, observation(event)),
      ).record);
      const replayLease = {
        ...live,
        processMode: "resume",
        replayMode: "retained_only",
        snapshotHeadNextOrdinal: records.length,
      } as const;
      assert.throws(
        () => validateRetainedSnapshot(replayLease, records, expectation()),
        (error: unknown) => error instanceof Error
          && error.message === "DRIVER_EVENT_ORDER_INVALID",
      );
    });
  }
});

test("wrong source, waiter, binding, gap, and illegal suffix fail before first yield", async (t) => {
  const retention = new MemoryRetention();
  const lease = await retention.claim(attempt());
  const reply = retention.appendPrepared(lease, retention.prepareAppend(lease, observation({
    kind: "assistant_reply",
    turnId,
    text: "private",
  }))).record;
  const completed = retention.appendPrepared(lease, retention.prepareAppend(lease, observation({
    kind: "turn_completed",
    turnId,
  }))).record;
  const replayLease = { ...lease, replayMode: "retained_only", snapshotHeadNextOrdinal: 2 } as const;
  const cases: Array<[string, readonly RetainedDriverEventRecord[]]> = [
    ["source", [{ ...reply, sourceMessageId: id("msg", "c") as MessageId }, completed]],
    ["waiter", [{ ...reply, resolvedWaiterId: id("cmd", "d") as CommandId }, completed]],
    ["binding", [{ ...reply, bindingDigest: hash("e") }, completed]],
    ["gap", [reply]],
    ["incomplete", [{ ...reply }, { ...completed, event: { kind: "assistant_reply", turnId, text: "again" }, eventKind: "assistant_reply" }]],
  ];
  for (const [name, records] of cases) {
    await t.test(name, () => {
      assert.throws(() => validateRetainedSnapshot(replayLease, records, expectation()));
    });
  }
});

class MemoryRetention implements DriverPrivateEventRetentionPort {
  readonly preparedOrdinals: number[] = [];
  readonly #records: RetainedDriverEventRecord[] = [];
  #lease: RetainedDriverEventLease | undefined;

  async claim(input: DriverPrivateClaimAttempt): Promise<RetainedDriverEventLease> {
    const lease = { ...input, snapshotHeadNextOrdinal: this.#records.length };
    this.#lease = lease;
    return lease;
  }

  async releaseAttempt(): Promise<{ applied: boolean }> {
    this.#lease = undefined;
    return { applied: true };
  }

  prepareAppend(
    lease: RetainedDriverEventLease,
    value: DriverEventObservation,
  ): PreparedRetainedDriverEventEnvelope {
    this.#assert(lease);
    const ordinal = this.#records.length;
    this.preparedOrdinals.push(ordinal);
    const base = {
      protocolVersion: lease.protocolVersion,
      launchId: lease.launchId,
      stateInstanceId: lease.stateInstanceId,
      sessionId: lease.sessionId,
      keyGeneration: 1,
      ordinal,
      previousRecordDigest: this.#records.at(-1)?.recordDigest ?? null,
      resolvedWaiterId: value.resolvedWaiterId,
      sourceMessageId: value.sourceMessageId,
      turnId: value.event.turnId,
      bindingDigest: value.bindingDigest,
      eventKind: value.event.kind,
      payloadCipherDigest: driverArtifactDigest(`cipher:${ordinal}`),
    } as const;
    const eventDigest = retainedEventDigest(base);
    const recordDigest = retainedRecordDigest({ ...base, eventDigest });
    return {
      ...base,
      eventDigest,
      recordDigest,
      nonce: new Uint8Array(12),
      authTag: new Uint8Array(16),
      ciphertext: new TextEncoder().encode(JSON.stringify(value.event)),
    };
  }

  appendPrepared(
    lease: RetainedDriverEventLease,
    envelope: PreparedRetainedDriverEventEnvelope,
  ) {
    this.#assert(lease);
    const occupied = this.#records[envelope.ordinal];
    if (occupied !== undefined) {
      if (occupied.recordDigest !== envelope.recordDigest) {
        throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
      }
      return { applied: false, record: occupied };
    }
    if (envelope.ordinal !== this.#records.length) {
      throw new DriverRetentionError("DRIVER_RETAINED_EVENT_GAP");
    }
    const event = JSON.parse(new TextDecoder().decode(envelope.ciphertext)) as RetainedDriverEventRecord["event"];
    const record: RetainedDriverEventRecord = {
      stream: "turn",
      ...envelope,
      readerEpoch: lease.readerEpoch,
      event,
    };
    this.#records.push(record);
    return { applied: true, record };
  }

  read(lease: RetainedDriverEventLease, ordinal: number): RetainedDriverEventRecord | null {
    this.#assert(lease);
    return this.#records[ordinal] ?? null;
  }

  acknowledgeCommitted(): { applied: boolean } {
    return { applied: true };
  }

  #assert(lease: RetainedDriverEventLease): void {
    if (this.#lease?.claimAttemptId !== lease.claimAttemptId) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
  }
}

function attempt(): DriverPrivateClaimAttempt {
  return {
    protocolVersion: version,
    launchId,
    stateInstanceId,
    sessionId,
    ownerToken,
    readerEpoch: 1,
    nextOrdinal: 0,
    lastEventDigest: null,
    claimAttemptId,
    processMode: "start",
    replayMode: "live",
  };
}

function observation(event: DriverEventObservation["event"]): DriverEventObservation {
  return {
    stream: "turn",
    resolvedWaiterId: waiterId,
    sourceMessageId,
    bindingDigest,
    event,
  };
}

function expectation() {
  return {
    expectedTurnId: turnId,
    expectedBindingDigest: bindingDigest,
    expectedResolvedWaiterId: waiterId,
    expectedSourceMessageId: sourceMessageId,
  };
}

function closeResult(applied: boolean) {
  return { applied, storageReleased: true, privateReleased: true };
}

function id(prefix: string, character: string): string {
  return `${prefix}_${character.repeat(26)}`;
}

function hash(character: string): ArtifactDigest {
  return `sha256:${character.repeat(64)}` as ArtifactDigest;
}

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const value of values) result.push(value);
  return result;
}
