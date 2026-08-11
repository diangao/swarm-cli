import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type {
  AgentId,
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  LaunchId,
  MachineId,
  MessageId,
  ProducerFactId,
  ProtocolVersion,
  ReadyLaunchFence,
  SessionId,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import {
  RetainedDriverEventPump,
  type DriverEventPump,
  type DriverPrivateClaimAttempt,
  type NativeProcessDriver,
} from "@swarm/drivers";
import { DaemonJournal, StorageError } from "@swarm/storage";

import { createNativeDriverRuntime } from "../src/composition.js";
import { NativeCursorClaimCoordinator, NativeTurnRuntime } from "../src/native-turn-runtime.js";
import { SqlitePrivateDriverEventRetention } from "../src/private-driver-events.js";

const version = 2 as ProtocolVersion;
const launchId = id("lnc", "1") as LaunchId;
const sessionId = id("ses", "2") as SessionId;
const turnId = id("trn", "3") as TurnId;
const sourceMessageId = id("msg", "4") as MessageId;
const transportDigest = digest("1");

test("real factory composes storage, encrypted retention, FIFO pump, and both concrete child hosts", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-native-runtime-"));
  const codexFixture = new URL("../../test/fixtures/codex-child.mjs", import.meta.url).pathname;
  const claudeFixture = new URL("../../test/fixtures/claude-child.mjs", import.meta.url).pathname;
  const composition = createNativeDriverRuntime({
    waveZeroSqlitePath: join(temporary, "journal.sqlite"),
    privateLaunchRoot: join(temporary, "private"),
    protocolVersion: version,
    launchId,
    codex: { executable: process.execPath, prefixArgv: [codexFixture], environment: {} },
    claude: { executable: process.execPath, prefixArgv: [claudeFixture], environment: {} },
  });
  try {
    const codex = await exercise(
      "codex",
      composition.codex,
      codexIdentity(),
      "Codex retained reply",
    );
    const claude = await exercise(
      "claude",
      composition.claude,
      claudeIdentity(),
      "Claude retained reply",
    );
    assert.deepEqual(codex.map((event) => event.kind), ["assistant_reply", "turn_complete"]);
    assert.deepEqual(claude.map((event) => event.kind), ["assistant_reply", "turn_complete"]);
  } finally {
    await composition.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("host-free fresh recovery closes null state and same-generation live ownership cannot be stolen", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-native-recovery-"));
  const journal = DaemonJournal.open(join(temporary, "journal.sqlite"));
  journal.migrate();
  const retention = new SqlitePrivateDriverEventRetention({
    launchRoot: join(temporary, "private"),
    protocolVersion: version,
    launchId,
  });
  try {
    const coordinator = new NativeCursorClaimCoordinator({ journal });
    const process = {
      launchId,
      stateInstanceId: id("sti", "5") as import("@swarm/protocol").StateInstanceId,
      processHandleRefPrivate: "synthetic-process",
      processHandleDigest: digest("8"),
      transportDigest,
    };
    const spec: import("@swarm/drivers").DriverLaunchSpec = {
      launch: {
        protocolVersion: version,
        agentId: id("agt", "6") as AgentId,
        machineId: id("mch", "7") as MachineId,
        launchId,
        routingGeneration: 1,
        workspaceGeneration: 1,
        stopEpoch: 0,
      },
      sessionId,
      driverIdentity: codexIdentity(),
      transportDigest,
      launchEnvironmentRefPrivate: "private",
    };
    const pump = new RetainedDriverEventPump(process.stateInstanceId, retention);
    const started = await coordinator.claimAfterSpawn({
      spec,
      process,
      pump,
      cursorOwnerToken: digest("9"),
    });
    await assert.rejects(
      coordinator.claimForReplay({
        protocolVersion: version,
        launchId,
        stateInstanceId: process.stateInstanceId,
        sessionId,
        cursorOwnerToken: digest("a"),
        pump,
      }),
      (error: unknown) => error instanceof StorageError && error.code === "DRIVER_RESUME_OVERLAP",
    );
    await started.release();

    let providerCalls = 0;
    const driver = new Proxy({}, {
      get(_target, property) {
        if (property === "recoverEvents") return async () => { providerCalls += 1; throw new Error("unexpected"); };
        return async () => { throw new Error("provider path must remain unused"); };
      },
    }) as NativeProcessDriver;
    const runtime = new NativeTurnRuntime({
      journal,
      coordinator,
      codex: driver,
      claude: driver,
    });
    const noActive = await runtime.recoverRetainedTurn({
      runtime: "codex",
      protocolVersion: version,
      launchId,
      stateInstanceId: process.stateInstanceId,
      sessionId,
      cursorOwnerToken: digest("b"),
      pump,
    });
    assert.equal(noActive.kind, "no_active");
    assert.equal(providerCalls, 0);
    assert.equal(noActive.kind, "no_active");
    if (noActive.kind !== "no_active") throw new Error("expected no-active recovery");
    const liveTicket = coordinator.beginLiveResume({
      protocolVersion: version,
      launchId,
      stateInstanceId: process.stateInstanceId,
      sessionId,
      cursorOwnerToken: digest("b"),
      authorization: noActive.liveResumeAuthorization,
    });
    assert.throws(() => coordinator.beginLiveResume({
      protocolVersion: version,
      launchId,
      stateInstanceId: process.stateInstanceId,
      sessionId,
      cursorOwnerToken: digest("b"),
      authorization: noActive.liveResumeAuthorization,
    }), /DRIVER_START_AUTHORITY_REQUIRED/u);
    const restart = await coordinator.claimForLiveResume({
      ticket: liveTicket,
      pump,
    });
    await assert.rejects(
      coordinator.claimForLiveResume({ ticket: liveTicket, pump }),
      /DRIVER_START_AUTHORITY_REQUIRED/u,
    );
    assert.equal(restart.privateLease.replayMode, "live");
    const futureWaiterId = id("cmd", "f") as CommandId;
    const futureBindingDigest = digest("d");
    const futureWaiter = await pump.registerWaiter(restart.privateLease, {
      kind: "turn",
      waiterId: futureWaiterId,
      stateInstanceId: process.stateInstanceId,
      sessionId,
      turnId,
      bindingDigest: futureBindingDigest,
    });
    const futureRecords = await pump.enqueue(restart.privateLease, [{
      stream: "turn",
      resolvedWaiterId: futureWaiter.waiterId,
      sourceMessageId,
      bindingDigest: futureBindingDigest,
      event: { kind: "assistant_reply", turnId, text: "future resumed turn" },
    }]);
    assert.equal(futureRecords.length, 1);
    assert.equal(futureRecords[0]?.event.kind, "assistant_reply");
    await restart.release();
  } finally {
    retention.close();
    journal.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("claim unknown-result cleanup releases storage first and cancels the exact preallocated private attempt", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-native-unwind-"));
  const journal = DaemonJournal.open(join(temporary, "journal.sqlite"));
  journal.migrate();
  const process = {
    launchId,
    stateInstanceId: id("sti", "h") as import("@swarm/protocol").StateInstanceId,
    processHandleRefPrivate: "synthetic-unwind-process",
    processHandleDigest: digest("8"),
    transportDigest,
  };
  const spec: import("@swarm/drivers").DriverLaunchSpec = {
    launch: {
      protocolVersion: version,
      agentId: id("agt", "j") as AgentId,
      machineId: id("mch", "k") as MachineId,
      launchId,
      routingGeneration: 1,
      workspaceGeneration: 1,
      stopEpoch: 0,
    },
    sessionId,
    driverIdentity: codexIdentity(),
    transportDigest,
    launchEnvironmentRefPrivate: "private",
  };
  const released: DriverPrivateClaimAttempt[] = [];
  let installed: DriverPrivateClaimAttempt | undefined;
  let failAfterInstall = true;
  const pump = {
    stateInstanceId: process.stateInstanceId,
    async claimCursor(attempt: DriverPrivateClaimAttempt) {
      installed = attempt;
      if (failAfterInstall) {
        failAfterInstall = false;
        throw new Error("PRIVATE_CLAIM_RESULT_LOST");
      }
      return { ...attempt, snapshotHeadNextOrdinal: attempt.nextOrdinal };
    },
    async releaseClaimAttempt(attempt: DriverPrivateClaimAttempt) {
      released.push(attempt);
    },
    async closeLeaseObservers() {},
  } as unknown as DriverEventPump;
  const claimIds = [id("cmd", "m"), id("cmd", "n")] as CommandId[];
  const coordinator = new NativeCursorClaimCoordinator({
    journal,
    nextClaimAttemptId: () => claimIds.shift()!,
  });
  try {
    await assert.rejects(
      coordinator.claimAfterSpawn({
        spec,
        process,
        pump,
        cursorOwnerToken: digest("d"),
      }),
      /PRIVATE_CLAIM_RESULT_LOST/u,
    );
    assert.ok(installed !== undefined);
    assert.deepEqual(released, [installed]);

    const retry = await coordinator.claimForReplay({
      protocolVersion: version,
      launchId,
      stateInstanceId: process.stateInstanceId,
      sessionId,
      cursorOwnerToken: digest("e"),
      pump,
    });
    assert.equal(retry.authority.readerEpoch, 2);
    await retry.release();
    assert.equal(released.length, 2);
  } finally {
    journal.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

async function exercise(
  runtime: "codex" | "claude",
  driver: import("@swarm/drivers").NativeProcessDriver,
  identity: DriverIdentity,
  expectedReply: string,
) {
  const spec: import("@swarm/drivers").DriverLaunchSpec = {
    launch: {
      protocolVersion: version,
      agentId: id("agt", runtime === "codex" ? "5" : "6") as AgentId,
      machineId: id("mch", "7") as MachineId,
      launchId,
      routingGeneration: 1,
      workspaceGeneration: 1,
      stopEpoch: 0,
    },
    sessionId,
    driverIdentity: identity,
    transportDigest,
    launchEnvironmentRefPrivate: "synthetic-private-home",
  };
  const started = await driver.start(spec);
  const ready: ReadyLaunchFence = {
    ...spec.launch,
    stateInstanceId: started.process.stateInstanceId,
    sessionId,
  };
  const bound = await started.preparation.bindRuntimeReady(ready, async ({ record }) => ({
    disposition: "applied" as const,
    value: record.event.runtimeSessionRef,
  }));
  const session: DriverSession = {
    launch: ready,
    driverIdentity: identity,
    runtimeSessionRef: bound.value,
  };
  const binding = turnBinding(spec, runtime);
  const compiled: CompiledNativeTurn = {
    input: {
      current: { delivery: { messageId: sourceMessageId } },
    } as CompiledNativeTurn["input"],
    bytes: new TextEncoder().encode("PRIVATE_NATIVE_INPUT"),
    inputDigest: binding.inputDigest,
  };
  const outcome = await driver.startTurn(started.process, session, compiled, binding);
  assert.equal(outcome.kind, "written");
  if (outcome.kind !== "written") throw new Error("native write did not start");
  const events = await collect(outcome.events);
  assert.deepEqual(events[0], { kind: "assistant_reply", text: expectedReply });
  await driver.stop(started.process, "daemon_shutdown");
  return events;
}

function turnBinding(
  spec: import("@swarm/drivers").DriverLaunchSpec,
  runtime: "codex" | "claude",
): DriverTurnBinding {
  return {
    protocolTurnId: turnId,
    rootProducerFactId: id("fac", "8") as ProducerFactId,
    inputOrdinal: 0,
    mode: { kind: "ordinary" },
    driverTurnRefDigest: digest("2"),
    delivery: {
      protocolVersion: version,
      deliveryId: id("dlv", runtime === "codex" ? "9" : "a") as DriverTurnBinding["delivery"]["deliveryId"],
      attempt: 1,
      producerFactId: id("fac", "8") as ProducerFactId,
      agentId: spec.launch.agentId,
      machineId: spec.launch.machineId,
      launchId,
      membershipEpoch: 1,
      routingGeneration: 1,
      routeVersion: 1,
      sessionId,
      turnId,
    },
    invocation: {
      invocationGeneration: 1,
      invocationId: id("cmd", runtime === "codex" ? "b" : "c") as CommandId,
    },
    permitId: id("cmd", runtime === "codex" ? "d" : "e") as CommandId,
    runtimeWriteId: id("cmd", runtime === "codex" ? "f" : "1") as CommandId,
    visibilityEventId: id("cmd", runtime === "codex" ? "2" : "3") as CommandId,
    inputDigest: digest(runtime === "codex" ? "3" : "4"),
  };
}

function codexIdentity(): DriverIdentity {
  return {
    protocolVersion: version,
    runtime: "codex",
    executableDigest: digest("5"),
    version: "0.145.0",
    wireProtocolDigest: "sha256:33e163c58a7e9c276f18e109d7ac361f01f8c2394881fc8e3f3177efeaed7cf3" as ArtifactDigest,
    capability: {
      start: true, resume: true, steer: true, interrupt: true,
      reviewBoundary: true, compactionBoundary: true,
    },
  };
}

function claudeIdentity(): DriverIdentity {
  return {
    protocolVersion: version,
    runtime: "claude",
    executableDigest: digest("6"),
    version: "2.1.226",
    wireProtocolDigest: digest("7"),
    capability: {
      start: true, resume: true, steer: false, interrupt: true,
      reviewBoundary: false, compactionBoundary: false,
    },
  };
}

function id(prefix: string, marker: string): string {
  return `${prefix}_${marker.repeat(26)}`;
}

function digest(marker: string): ArtifactDigest {
  return `sha256:${marker.repeat(64)}` as ArtifactDigest;
}

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const value of values) result.push(value);
  return result;
}
