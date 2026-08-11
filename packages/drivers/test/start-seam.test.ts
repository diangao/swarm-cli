import assert from "node:assert/strict";
import { test } from "node:test";

import type {
  AgentId,
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  LaunchId,
  MachineId,
  NormalizedDriverEvent,
  ProtocolVersion,
  ReadyLaunchFence,
  SessionId,
  StateInstanceId,
} from "@swarm/protocol";

import { DriverNormalizationError } from "../src/normalizer.js";
import {
  DriverStartPreparation,
  type DriverEventPump,
  type DriverEventPumpLease,
  type DriverEventRecord,
  type DriverRegisteredEventWaiter,
  type DriverEventWaiterSpec,
  type DriverLaunchSpec,
  type DriverResumeSpec,
  type DriverStartedProcess,
  type NativeProcessDriver,
  type SpawnHandle,
} from "../src/port.js";
import type {
  DriverCompositeCursorClaimHandle,
  DriverCursorAuthority,
  DriverPrivateClaimAttempt,
} from "../src/retained-events.js";

const token = "01j00000000000000000000000";
const version = 1 as ProtocolVersion;
const agentId = `agt_${token}` as AgentId;
const machineId = `mac_${token}` as MachineId;
const launchId = `lch_${token}` as LaunchId;
const stateInstanceId = `sti_${token}` as StateInstanceId;
const sessionId = `ses_${token}` as SessionId;
const ownerToken = digest("1");
const transportDigest = digest("2");
const bindingDigest = digest("3");
const waiterId = `cmd_${token}` as CommandId;
const siblingWaiterId = `cmd_${"02j00000000000000000000000"}` as CommandId;

const identity: DriverIdentity = {
  protocolVersion: version,
  runtime: "codex",
  executableDigest: digest("4"),
  version: "0.1.0",
  wireProtocolDigest: digest("5"),
  capability: {
    start: true,
    resume: true,
    steer: true,
    interrupt: true,
    reviewBoundary: true,
    compactionBoundary: true,
  },
};

const launchSpec: DriverLaunchSpec = {
  launch: {
    protocolVersion: version,
    agentId,
    machineId,
    launchId,
    routingGeneration: 3,
    workspaceGeneration: 4,
    stopEpoch: 5,
  },
  sessionId,
  driverIdentity: identity,
  transportDigest,
  launchEnvironmentRefPrivate: "private-env",
};

const process: SpawnHandle = {
  launchId,
  stateInstanceId,
  processHandleRefPrivate: "private-process",
  processHandleDigest: digest("6"),
  transportDigest,
};

const readyLaunch: ReadyLaunchFence = {
  ...launchSpec.launch,
  stateInstanceId,
  sessionId,
};

const lease: DriverEventPumpLease = {
  protocolVersion: version,
  launchId,
  stateInstanceId,
  sessionId,
  ownerToken,
  readerEpoch: 1,
  nextOrdinal: 0,
  lastEventDigest: null,
  claimAttemptId: waiterId,
  processMode: "start",
  replayMode: "live",
  snapshotHeadNextOrdinal: 0,
};

const waiter: DriverRegisteredEventWaiter = {
  kind: "initialize",
  waiterId,
  stateInstanceId,
  sessionId,
  bindingDigest,
  registeredBeforeWrite: true,
  stream: "lifecycle",
  readerEpoch: lease.readerEpoch,
  registeredThroughOrdinal: 0,
};

const runtimeReady = {
  kind: "runtime_ready",
  runtimeSessionRef: "private-session",
  runtimeSessionRefDigest: digest("7"),
} satisfies NormalizedDriverEvent;

const readyRecord: DriverEventRecord = {
  stream: "lifecycle",
  stateInstanceId,
  sessionId,
  readerEpoch: lease.readerEpoch,
  resolvedWaiterId: waiterId,
  ordinal: 1,
  eventDigest: digest("8"),
  bindingDigest,
  event: runtimeReady,
};

type PumpOptions = {
  claimedLease?: DriverEventPumpLease;
  waiter?: DriverRegisteredEventWaiter;
  record?: DriverEventRecord;
  recordPromise?: Promise<DriverEventRecord>;
  registerError?: Error;
  registerErrorAfterInstall?: boolean;
  waitError?: Error;
  cancelError?: Error;
  releaseError?: Error;
  installedWaiters?: DriverEventWaiterSpec[];
};

class RecordingPump implements DriverEventPump {
  readonly stateInstanceId = stateInstanceId;
  readonly calls: string[];
  readonly cancelled: DriverEventWaiterSpec[] = [];
  readonly installedWaiters = new Map<CommandId, DriverEventWaiterSpec>();
  readonly #options: PumpOptions;

  constructor(calls: string[] = [], options: PumpOptions = {}) {
    this.calls = calls;
    this.#options = options;
    for (const spec of options.installedWaiters ?? []) {
      this.installedWaiters.set(spec.waiterId, spec);
    }
  }

  async claimCursor(input: DriverPrivateClaimAttempt): Promise<DriverEventPumpLease> {
    this.calls.push("claim");
    assert.deepEqual(input, claimAttempt());
    return this.#options.claimedLease ?? lease;
  }

  async releaseClaimAttempt(input: DriverPrivateClaimAttempt): Promise<void> {
    this.calls.push("release");
    assert.deepEqual(input, claimAttempt());
    if (this.#options.releaseError !== undefined) throw this.#options.releaseError;
  }

  async closeLeaseObservers(_lease: DriverEventPumpLease): Promise<void> {}

  async registerWaiter(
    actualLease: DriverEventPumpLease,
    spec: DriverEventWaiterSpec,
  ): Promise<DriverRegisteredEventWaiter> {
    this.calls.push("waiter");
    assert.deepEqual(actualLease, this.#options.claimedLease ?? lease);
    assert.deepEqual(spec, exactWaiterSpec());
    if (this.#options.registerError !== undefined && !this.#options.registerErrorAfterInstall) {
      throw this.#options.registerError;
    }
    this.installedWaiters.set(spec.waiterId, spec);
    if (this.#options.registerError !== undefined) throw this.#options.registerError;
    return this.#options.waiter ?? waiter;
  }

  async cancelWaiter(
    actualLease: DriverEventPumpLease,
    spec: DriverEventWaiterSpec,
  ): Promise<void> {
    this.calls.push("cancel");
    assert.deepEqual(actualLease, this.#options.claimedLease ?? lease);
    this.cancelled.push(spec);
    this.installedWaiters.delete(spec.waiterId);
    if (this.#options.cancelError !== undefined) throw this.#options.cancelError;
  }

  async waitForRecord(
    actualLease: DriverEventPumpLease,
    actualWaiter: DriverRegisteredEventWaiter,
  ): Promise<DriverEventRecord> {
    this.calls.push("record");
    assert.deepEqual(actualLease, lease);
    assert.deepEqual(actualWaiter, waiter);
    if (this.#options.waitError !== undefined) throw this.#options.waitError;
    if (this.#options.recordPromise !== undefined) return await this.#options.recordPromise;
    return this.#options.record ?? readyRecord;
  }

  async *subscribe(actualLease: DriverEventPumpLease): AsyncIterable<DriverEventRecord> {
    assert.deepEqual(actualLease, lease);
  }

}

class ConformingStartDriver implements Pick<NativeProcessDriver, "start"> {
  readonly #pump: DriverEventPump;
  readonly #calls: string[];

  constructor(pump: DriverEventPump, calls: string[]) {
    this.#pump = pump;
    this.#calls = calls;
  }

  async start(spec: DriverLaunchSpec): Promise<DriverStartedProcess> {
    this.#calls.push("spawn");
    const preparation = await DriverStartPreparation.start({
      spec,
      process,
      pump: this.#pump,
      cursorClaim: await claimForTest(this.#pump),
      initializeWaiterId: waiterId,
      initializeBindingDigest: bindingDigest,
      writeInitialize: async ({ lease: witnessedLease, waiter: witnessedWaiter }) => {
        assert.deepEqual(witnessedLease, lease);
        assert.deepEqual(witnessedWaiter, waiter);
        this.#calls.push("transport-write");
      },
    });
    return { process, preparation };
  }
}

test("conforming start cannot write before its exact cursor and initialize waiter", async () => {
  const calls: string[] = [];
  const pump = new RecordingPump(calls);
  const driver: Pick<NativeProcessDriver, "start"> = new ConformingStartDriver(pump, calls);

  const started = await driver.start(launchSpec);
  assert.equal(started.process, process);
  assert.ok(started.preparation instanceof DriverStartPreparation);
  assert.deepEqual(calls, ["spawn", "claim", "waiter", "transport-write"]);

  const persisted = await started.preparation.bindRuntimeReady(readyLaunch, async (bound) => {
    assert.equal(bound.record, readyRecord);
    assert.deepEqual(bound.lease, lease);
    assert.deepEqual(bound.waiter, waiter);
    return { disposition: "applied", value: "ready" };
  });
  assert.deepEqual(persisted, { disposition: "applied", value: "ready" });
  assert.deepEqual(calls, ["spawn", "claim", "waiter", "transport-write", "record"]);
  assert.equal(await asyncDriverCode(() => started.preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "applied", value: "duplicate" }),
  )), "DRIVER_EVENT_ORDER_INVALID");

  // @ts-expect-error A NativeProcessDriver start result cannot forge the private preparation witness.
  const forged: DriverStartedProcess = { process, preparation: {} };
  void forged;
});

test("throw-after-install cancels only the preallocated waiter and preserves a same-binding sibling", async () => {
  const calls: string[] = [];
  const sibling = exactWaiterSpec(siblingWaiterId);
  const pump = new RecordingPump(calls, {
    registerError: new Error("register failed after install"),
    registerErrorAfterInstall: true,
    installedWaiters: [sibling],
  });
  let writeCalls = 0;

  await assert.rejects(
    prepare(pump, async () => { writeCalls += 1; }),
    /register failed after install/u,
  );
  assert.equal(writeCalls, 0);
  assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
  assert.deepEqual(pump.cancelled, [exactWaiterSpec()]);
  assert.deepEqual([...pump.installedWaiters.keys()], [siblingWaiterId]);
});

test("waiter cancellation failure still releases the lease and preserves both failures", async () => {
  const calls: string[] = [];
  const registerError = new Error("register failed");
  const cancelError = new Error("cancel failed");
  const pump = new RecordingPump(calls, {
    registerError,
    registerErrorAfterInstall: true,
    cancelError,
  });

  await assert.rejects(prepare(pump), (error: unknown) => {
    assert.ok(error instanceof AggregateError);
    assert.deepEqual(error.errors, [registerError, cancelError]);
    return true;
  });
  assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
});

test("mismatched waiter identity, epoch, and transport failure unwind exact ownership", async (t) => {
  await t.test("mismatched waiter", async () => {
    const calls: string[] = [];
    const pump = new RecordingPump(calls, {
      waiter: { ...waiter, waiterId: siblingWaiterId },
    });
    let writeCalls = 0;
    assert.equal(await asyncDriverCode(() => prepare(pump, async () => { writeCalls += 1; })),
      "DRIVER_EVENT_FENCE_MISMATCH");
    assert.equal(writeCalls, 0);
    assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
    assert.deepEqual(pump.cancelled, [exactWaiterSpec()]);
  });

  await t.test("mismatched waiter epoch", async () => {
    const calls: string[] = [];
    const wrongEpochWaiter = { ...waiter, readerEpoch: lease.readerEpoch + 1 };
    const pump = new RecordingPump(calls, { waiter: wrongEpochWaiter });
    let writeCalls = 0;
    assert.equal(await asyncDriverCode(() => prepare(pump, async () => { writeCalls += 1; })),
      "DRIVER_EVENT_FENCE_MISMATCH");
    assert.equal(writeCalls, 0);
    assert.deepEqual(
      { ...wrongEpochWaiter, readerEpoch: waiter.readerEpoch },
      waiter,
      "the killing waiter differs from the healthy waiter only by readerEpoch",
    );
    assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
    assert.deepEqual(pump.cancelled, [exactWaiterSpec()]);
  });

  await t.test("transport failure", async () => {
    const calls: string[] = [];
    const pump = new RecordingPump(calls);
    await assert.rejects(prepare(pump, async () => {
      calls.push("transport-write");
      throw new Error("write failed");
    }), /write failed/u);
    assert.deepEqual(calls, ["claim", "waiter", "transport-write", "cancel", "release"]);
    assert.deepEqual(pump.cancelled, [exactWaiterSpec()]);
  });
});

test("a post-claim lease mismatch releases the claimed lease without registering or writing", async () => {
  const calls: string[] = [];
  const mismatchedLease = { ...lease, sessionId: `ses_${"02j00000000000000000000000"}` as SessionId };
  const pump = new RecordingPump(calls, { claimedLease: mismatchedLease });
  let writeCalls = 0;
  assert.equal(await asyncDriverCode(() => prepare(pump, async () => { writeCalls += 1; })),
    "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(writeCalls, 0);
  assert.deepEqual(calls, ["claim", "release"]);
});

test("runtime readiness rejects every wrong fence, waiter, and non-causal ordinal", async (t) => {
  const wrongRecords: Array<[string, DriverEventRecord]> = [
    ["state", { ...readyRecord, stateInstanceId: `sti_${"02j00000000000000000000000"}` as StateInstanceId }],
    ["session", { ...readyRecord, sessionId: `ses_${"02j00000000000000000000000"}` as SessionId }],
    ["reader epoch", { ...readyRecord, readerEpoch: lease.readerEpoch + 1 }],
    ["binding", { ...readyRecord, bindingDigest: digest("9") }],
    ["waiter", { ...readyRecord, resolvedWaiterId: siblingWaiterId }],
    ["snapshot ordinal", { ...readyRecord, ordinal: 0 }],
    ["predecessor ordinal", { ...readyRecord, ordinal: -1 }],
    ["kind", { ...readyRecord, event: { kind: "runtime_terminal", reason: "process_exited" } }],
  ];

  for (const [name, record] of wrongRecords) {
    await t.test(name, async () => {
      const calls: string[] = [];
      const pump = new RecordingPump(calls, { record });
      const preparation = await prepare(pump);
      let persistenceCalls = 0;
      assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(readyLaunch, async () => {
        persistenceCalls += 1;
        return { disposition: "applied", value: undefined };
      })), "DRIVER_EVENT_FENCE_MISMATCH");
      assert.equal(persistenceCalls, 0);
      if (name === "reader epoch") {
        assert.deepEqual(
          { ...record, readerEpoch: readyRecord.readerEpoch },
          readyRecord,
          "the killing record differs from the healthy record only by readerEpoch",
        );
      }
      assert.deepEqual(calls, ["claim", "waiter", "record", "cancel", "release"]);
      assert.deepEqual(pump.cancelled, [exactWaiterSpec()]);
    });
  }
});

test("runtime readiness accepts the immediate next and a gapped later waiter-bound record", async (t) => {
  for (const ordinal of [1, 4]) {
    await t.test(`ordinal ${ordinal}`, async () => {
      const record = { ...readyRecord, ordinal };
      const calls: string[] = [];
      const preparation = await prepare(new RecordingPump(calls, { record }));
      const persisted = await preparation.bindRuntimeReady(readyLaunch, async (bound) => {
        assert.equal(bound.record, record);
        assert.equal(bound.record.resolvedWaiterId, waiterId);
        return { disposition: "applied", value: ordinal };
      });
      assert.deepEqual(persisted, { disposition: "applied", value: ordinal });
      assert.deepEqual(calls, ["claim", "waiter", "record"]);
    });
  }
});

test("wrong ready launch unwinds before observing a record or invoking persistence", async () => {
  const calls: string[] = [];
  const pump = new RecordingPump(calls);
  const preparation = await prepare(pump);
  let persistenceCalls = 0;
  const wrongLaunch = { ...readyLaunch, sessionId: `ses_${"02j00000000000000000000000"}` as SessionId };
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(wrongLaunch, async () => {
    persistenceCalls += 1;
    return { disposition: "applied", value: undefined };
  })), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(persistenceCalls, 0);
  assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
});

test("async persistence rejection leaves the same exact record retryable and seals only success", async () => {
  const calls: string[] = [];
  const pump = new RecordingPump(calls);
  const preparation = await prepare(pump);
  let persistenceCalls = 0;

  await assert.rejects(preparation.bindRuntimeReady(readyLaunch, async (bound) => {
    persistenceCalls += 1;
    assert.equal(bound.record, readyRecord);
    throw new Error("database unavailable");
  }), /database unavailable/u);
  assert.equal(persistenceCalls, 1);
  assert.deepEqual(calls, ["claim", "waiter", "record"]);

  const retried = await preparation.bindRuntimeReady(readyLaunch, async (bound) => {
    persistenceCalls += 1;
    assert.equal(bound.record, readyRecord);
    return { disposition: "applied", value: 2 };
  });
  assert.deepEqual(retried, { disposition: "applied", value: 2 });
  assert.equal(persistenceCalls, 2);
  assert.deepEqual(calls, ["claim", "waiter", "record"]);
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(
    readyLaunch,
    async () => {
      persistenceCalls += 1;
      return { disposition: "applied", value: 3 };
    },
  )), "DRIVER_EVENT_ORDER_INVALID");
  assert.equal(persistenceCalls, 2);
});

test("a durable replay disposition is accepted once and seals the preparation", async () => {
  const preparation = await prepare(new RecordingPump());
  const result = await preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "replayed", value: "same-fact" }),
  );
  assert.deepEqual(result, { disposition: "replayed", value: "same-fact" });
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "replayed", value: "duplicate" }),
  )), "DRIVER_EVENT_ORDER_INVALID");
});

test("stop aborts an in-flight readiness wait exactly once and every later bind rejects", async () => {
  const pendingRecord = deferred<DriverEventRecord>();
  const calls: string[] = [];
  const preparation = await prepare(new RecordingPump(calls, {
    recordPromise: pendingRecord.promise,
  }));
  let persistenceCalls = 0;
  const binding = preparation.bindRuntimeReady(readyLaunch, async () => {
    persistenceCalls += 1;
    return { disposition: "applied", value: "should-not-run" };
  });
  assert.deepEqual(calls, ["claim", "waiter", "record"]);

  const firstAbort = preparation.abort("stop");
  const secondAbort = preparation.abort("timeout");
  assert.equal(firstAbort, secondAbort);
  assert.deepEqual(await firstAbort, { disposition: "aborted", reason: "stop" });
  assert.equal(await asyncDriverCode(() => binding), "DRIVER_EVENT_ORDER_INVALID");
  assert.equal(persistenceCalls, 0);
  assert.deepEqual(calls, ["claim", "waiter", "record", "cancel", "release"]);
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "applied", value: undefined }),
  )), "DRIVER_EVENT_ORDER_INVALID");
  pendingRecord.resolve(readyRecord);
});

test("timeout before readiness observation cancels exact ownership and is idempotent", async () => {
  const calls: string[] = [];
  const preparation = await prepare(new RecordingPump(calls));
  assert.deepEqual(await preparation.abort("timeout"), {
    disposition: "aborted",
    reason: "timeout",
  });
  assert.deepEqual(await preparation.abort("timeout"), {
    disposition: "aborted",
    reason: "timeout",
  });
  assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "applied", value: undefined }),
  )), "DRIVER_EVENT_ORDER_INVALID");
});

test("abort cleanup aggregates cancellation and release failures without retrying effects", async () => {
  const calls: string[] = [];
  const cancelError = new Error("abort cancel failed");
  const releaseError = new Error("abort release failed");
  const preparation = await prepare(new RecordingPump(calls, { cancelError, releaseError }));
  const firstAbort = preparation.abort("controller_cancelled");
  const secondAbort = preparation.abort("stop");
  assert.equal(firstAbort, secondAbort);
  await assert.rejects(firstAbort, (error: unknown) => {
    assert.ok(error instanceof AggregateError);
    assert.deepEqual(error.errors, [cancelError, releaseError]);
    return true;
  });
  await assert.rejects(secondAbort, AggregateError);
  assert.deepEqual(calls, ["claim", "waiter", "cancel", "release"]);
});

test("a successful in-flight persistence CAS wins an abort race without tearing ownership", async () => {
  const calls: string[] = [];
  const enteredPersistence = deferred<void>();
  const persistence = deferred<{ disposition: "applied"; value: string }>();
  const preparation = await prepare(new RecordingPump(calls));
  const binding = preparation.bindRuntimeReady(readyLaunch, () => {
    enteredPersistence.resolve(undefined);
    return persistence.promise;
  });
  await enteredPersistence.promise;

  const abort = preparation.abort("controller_cancelled");
  assert.deepEqual(calls, ["claim", "waiter", "record"]);
  persistence.resolve({ disposition: "applied", value: "ready" });
  assert.deepEqual(await binding, { disposition: "applied", value: "ready" });
  assert.deepEqual(await abort, { disposition: "ready_won" });
  assert.deepEqual(calls, ["claim", "waiter", "record"]);
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "replayed", value: "duplicate" }),
  )), "DRIVER_EVENT_ORDER_INVALID");
});

test("a rejected in-flight persistence CAS lets the pending abort clean exact ownership", async () => {
  const calls: string[] = [];
  const enteredPersistence = deferred<void>();
  const persistence = deferred<{ disposition: "applied"; value: string }>();
  const preparation = await prepare(new RecordingPump(calls));
  const binding = preparation.bindRuntimeReady(readyLaunch, () => {
    enteredPersistence.resolve(undefined);
    return persistence.promise;
  });
  await enteredPersistence.promise;

  const abort = preparation.abort("stop");
  const persistenceError = new Error("CAS rejected");
  persistence.reject(persistenceError);
  await assert.rejects(binding, /CAS rejected/u);
  assert.deepEqual(await abort, { disposition: "aborted", reason: "stop" });
  assert.deepEqual(calls, ["claim", "waiter", "record", "cancel", "release"]);
  assert.equal(await asyncDriverCode(() => preparation.bindRuntimeReady(
    readyLaunch,
    async () => ({ disposition: "applied", value: "late" }),
  )), "DRIVER_EVENT_ORDER_INVALID");
});

test("start session allocation does not weaken the existing exact resume fence", () => {
  const resume: DriverResumeSpec = {
    launch: { ...launchSpec.launch, stateInstanceId },
    expectedSessionId: sessionId,
    runtimeSessionRefPrivate: "private-session",
    cursorOwnerToken: ownerToken,
    liveResumeAuthorization: Object.freeze({
      protocolVersion: launchSpec.launch.protocolVersion,
      launchId: launchSpec.launch.launchId,
      stateInstanceId,
      sessionId,
      ownerToken,
      provedReaderEpoch: 1,
      nextOrdinal: 0,
      lastEventDigest: null,
    }),
  };
  assert.equal(resume.expectedSessionId, launchSpec.sessionId);
  assert.equal(resume.launch.stateInstanceId, process.stateInstanceId);
});

function exactWaiterSpec(actualWaiterId: CommandId = waiterId): DriverEventWaiterSpec {
  return {
    kind: "initialize",
    waiterId: actualWaiterId,
    stateInstanceId,
    sessionId,
    bindingDigest,
  };
}

async function prepare(
  pump: DriverEventPump,
  writeInitialize: () => Promise<void> = async () => undefined,
): Promise<DriverStartPreparation> {
  return DriverStartPreparation.start({
    spec: launchSpec,
    process,
    pump,
    cursorClaim: await claimForTest(pump),
    initializeWaiterId: waiterId,
    initializeBindingDigest: bindingDigest,
    writeInitialize,
  });
}

function claimAttempt(): DriverPrivateClaimAttempt {
  return {
    protocolVersion: version,
    launchId,
    stateInstanceId,
    sessionId,
    ownerToken,
    readerEpoch: 1,
    nextOrdinal: 0,
    lastEventDigest: null,
    claimAttemptId: waiterId,
    processMode: "start",
    replayMode: "live",
  };
}

async function claimForTest(pump: DriverEventPump): Promise<DriverCompositeCursorClaimHandle> {
  const attempt = claimAttempt();
  const privateLease = await pump.claimCursor(attempt);
  const authority: DriverCursorAuthority = {
    protocolVersion: attempt.protocolVersion,
    launchId: attempt.launchId,
    stateInstanceId: attempt.stateInstanceId,
    sessionId: attempt.sessionId,
    ownerToken: attempt.ownerToken,
    readerEpoch: attempt.readerEpoch,
    nextOrdinal: attempt.nextOrdinal,
    lastEventDigest: attempt.lastEventDigest,
  };
  let closed = false;
  const close = async () => {
    if (closed) return { applied: false, storageReleased: true, privateReleased: true };
    closed = true;
    await pump.closeLeaseObservers(privateLease);
    await pump.releaseClaimAttempt(attempt);
    return { applied: true, storageReleased: true, privateReleased: true };
  };
  return {
    authority,
    privateLease,
    abort: close,
    release: close,
  };
}

async function asyncDriverCode(action: () => Promise<unknown>): Promise<string> {
  try {
    await action();
  } catch (error) {
    assert.ok(error instanceof DriverNormalizationError);
    return error.code;
  }
  return "NO_ERROR";
}

function digest(character: string): ArtifactDigest {
  return `sha256:${character.repeat(64)}` as ArtifactDigest;
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(cause: unknown): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (cause: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}
