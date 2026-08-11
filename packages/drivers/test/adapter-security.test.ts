import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type {
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  NormalizedDriverEvent,
  ReadyLaunchFence,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import type {
  DriverEventPump,
  DriverEventPumpLease,
  DriverEventRecord,
  DriverEventWaiterSpec,
  DriverLaunchSpec,
  DriverProbeSpec,
  DriverRegisteredEventWaiter,
  DriverResumeSpec,
  DriverStatus,
  DriverCursorClaimCoordinator,
  SpawnHandle,
} from "../src/port.js";
import type {
  DriverCompositeCursorClaimHandle,
  DriverPrivateClaimAttempt,
  DriverRetainedEventSource,
} from "../src/retained-events.js";
import { DriverNormalizationError } from "../src/normalizer.js";
import { CodexNativeProcessDriver } from "../src/codex/adapter.js";
import {
  CODEX_APP_SERVER_ARGV,
  CODEX_CAPABILITY,
  CODEX_RUNTIME_VERSION,
  CODEX_VERSION_OUTPUT,
  CODEX_WIRE_PROTOCOL_DIGEST,
} from "../src/codex/constants.js";
import type {
  CodexJsonRpcNotification,
  CodexJsonRpcRequest,
  CodexRuntimeHost,
  CodexWireConsumer,
} from "../src/codex/types.js";
import { ClaudeNativeProcessDriver } from "../src/claude/adapter.js";
import {
  CLAUDE_CAPABILITY,
  CLAUDE_RUNTIME_VERSION,
  CLAUDE_STREAM_ARGV,
  CLAUDE_VERSION_OUTPUT,
} from "../src/claude/constants.js";
import type { ClaudeRuntimeHost, ClaudeWireConsumer } from "../src/claude/types.js";
import { claudeRuntimeSessionUuid } from "../src/claude/uuid.js";

const positive = JSON.parse(readFileSync(
  new URL("../../../../contracts/protocol/fixtures/wave2-positive.json", import.meta.url),
  "utf8",
)) as { ordinaryBinding: DriverTurnBinding };
const binding = positive.ordinaryBinding;
const digest = (char: string) => `sha256:${char.repeat(64)}` as ArtifactDigest;
const stateInstanceId = `sti_${"0".repeat(26)}` as SpawnHandle["stateInstanceId"];
const process: SpawnHandle = {
  launchId: binding.delivery.launchId,
  stateInstanceId,
  processHandleRefPrivate: "private-process",
  processHandleDigest: digest("4"),
  transportDigest: digest("5"),
};
const codexIdentity: DriverIdentity = {
  protocolVersion: binding.delivery.protocolVersion,
  runtime: "codex",
  executableDigest: digest("6"),
  version: CODEX_RUNTIME_VERSION,
  wireProtocolDigest: CODEX_WIRE_PROTOCOL_DIGEST,
  capability: { ...CODEX_CAPABILITY },
};
const claudeIdentity: DriverIdentity = {
  protocolVersion: binding.delivery.protocolVersion,
  runtime: "claude",
  executableDigest: digest("7"),
  version: CLAUDE_RUNTIME_VERSION,
  wireProtocolDigest: digest("8"),
  capability: { ...CLAUDE_CAPABILITY },
};
const compiled = {
  input: {
    current: {
      delivery: { messageId: `msg_${"0".repeat(26)}` },
    },
  } as CompiledNativeTurn["input"],
  bytes: new TextEncoder().encode("PRIVATE_BODY_MUST_NOT_ENTER_ARGV"),
  inputDigest: binding.inputDigest,
} satisfies CompiledNativeTurn;

test("Codex start proves claim→waiter→wire order and mismatch probe cannot spawn", async () => {
  const pump = new RecordingPump();
  const host = new CodexHost(pump);
  const driver = codexDriver(host);
  const spec = launchSpec(codexIdentity);
  const started = await driver.start(spec);
  assert.deepEqual(host.argv, CODEX_APP_SERVER_ARGV);
  assert.deepEqual(pump.calls.slice(0, 2), ["claim:start", "register:initialize"]);
  assert.deepEqual(host.writes.slice(0, 3), ["initialize", "initialized", "thread/start"]);
  assert.ok(pump.timeline.indexOf("register:initialize") < pump.timeline.indexOf("write:initialize"));
  await bindReady(started.preparation, spec, process);

  const badHost = new CodexHost(new RecordingPump());
  badHost.probeVersion = "codex-cli 0.145.1";
  const bad = codexDriver(badHost);
  assert.equal(await asyncDriverCode(() => bad.probe(probeSpec(codexIdentity))), "DRIVER_PROTOCOL_UNSUPPORTED");
  assert.equal(badHost.spawnCount, 0);
});

test("Claude start uses the exact secret-free argv after waiter registration", async () => {
  const pump = new RecordingPump();
  const host = new ClaudeHost(pump);
  const driver = claudeDriver(host);
  const spec = launchSpec(claudeIdentity);
  const started = await driver.start(spec);
  const runtimeSessionRef = claudeRuntimeSessionUuid(spec.sessionId);
  assert.deepEqual(host.argv, [...CLAUDE_STREAM_ARGV, "--session-id", runtimeSessionRef]);
  assert.doesNotMatch(JSON.stringify(host.argv), /PRIVATE_BODY_MUST_NOT_ENTER_ARGV|secret|token/u);
  assert.deepEqual(pump.calls.slice(0, 2), ["claim:start", "register:initialize"]);
  assert.ok(pump.timeline.indexOf("register:initialize") < pump.timeline.indexOf("begin"));
  await bindReady(started.preparation, spec, process);
});

test("Codex and Claude resume claim live authority before accepting a future new turn", async () => {
  const codexPump = new RecordingPump();
  const codexHost = new CodexHost(codexPump);
  codexHost.completeTurn = true;
  const codex = codexDriver(codexHost);
  const codexSpec = resumeSpec(codexIdentity, "codex-thread-private");
  const codexProcess = await codex.resume(codexSpec);
  assert.equal(codexPump.lease.processMode, "resume");
  assert.equal(codexPump.lease.replayMode, "live");
  const codexOutcome = await codex.startTurn(
    codexProcess,
    readySession(launchSpec(codexIdentity), codexIdentity, "codex-thread-private"),
    compiled,
    binding,
  );
  assert.equal(codexOutcome.kind, "written");
  if (codexOutcome.kind === "written") {
    assert.deepEqual(await collect(codexOutcome.events), [
      { kind: "assistant_reply", text: "Done." },
      { kind: "turn_complete" },
    ]);
  }
  await codex.stop(codexProcess, "daemon_shutdown");

  const claudePump = new RecordingPump();
  const claudeHost = new ClaudeHost(claudePump);
  claudeHost.completeTurn = true;
  const claude = claudeDriver(claudeHost);
  const claudeSession = claudeRuntimeSessionUuid(binding.delivery.sessionId);
  const claudeSpec = resumeSpec(claudeIdentity, claudeSession);
  const claudeProcess = await claude.resume(claudeSpec);
  assert.equal(claudePump.lease.processMode, "resume");
  assert.equal(claudePump.lease.replayMode, "live");
  const claudeOutcome = await claude.startTurn(
    claudeProcess,
    readySession(launchSpec(claudeIdentity), claudeIdentity, claudeSession),
    compiled,
    binding,
  );
  assert.equal(claudeOutcome.kind, "written");
  if (claudeOutcome.kind === "written") {
    assert.deepEqual(await collect(claudeOutcome.events), [
      { kind: "assistant_reply", text: "Done." },
      { kind: "turn_complete" },
    ]);
  }
  await claude.stop(claudeProcess, "daemon_shutdown");
});

test("live resume authorization is consumed before any provider host effect", async () => {
  const pump = new RecordingPump();
  const host = new CodexHost(pump);
  const rejectedCoordinator: DriverCursorClaimCoordinator = {
    ...testCursorCoordinator,
    beginLiveResume() {
      throw new Error("DRIVER_START_AUTHORITY_REQUIRED");
    },
  };
  const driver = new CodexNativeProcessDriver(host, unusedRetainedSource, rejectedCoordinator);
  await assert.rejects(
    driver.resume(resumeSpec(codexIdentity, "codex-thread-private")),
    /DRIVER_START_AUTHORITY_REQUIRED/u,
  );
  assert.equal(host.spawnCount, 0);
  assert.deepEqual(host.writes, []);
});

test("a failed post-spawn handshake unwinds pump ownership and stops the exact child", async () => {
  const pump = new RecordingPump();
  const host = new CodexHost(pump);
  host.failInitialize = true;
  const driver = codexDriver(host);
  assert.equal(
    await asyncErrorMessage(() => driver.start(launchSpec(codexIdentity))),
    "wire failed",
  );
  assert.deepEqual(pump.calls, [
    "claim:start",
    "register:initialize",
    "cancel:initialize",
    "release",
  ]);
  assert.deepEqual(host.stops, ["private-process:driver_protocol_error"]);
});

test("waiter mismatch is cleaned before write and the same exact turn can retry", async () => {
  const pump = new RecordingPump();
  const host = new CodexHost(pump);
  const driver = codexDriver(host);
  const spec = launchSpec(codexIdentity);
  const started = await driver.start(spec);
  await bindReady(started.preparation, spec, process);
  const session = readySession(spec, codexIdentity, "codex-thread-private");

  pump.wrongNextWaiterEpoch = true;
  assert.equal(
    await asyncDriverCode(() => driver.startTurn(process, session, compiled, binding)),
    "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(host.turnWrites, 0);
  assert.equal(pump.calls.at(-1), "cancel:turn");

  host.failTurnWrite = true;
  const outcome = await driver.startTurn(process, session, compiled, binding);
  assert.deepEqual(outcome, { kind: "ambiguous" });
  assert.equal(host.turnWrites, 1, "real uncertain write is never retried");
  assert.equal(pump.calls.at(-1), "cancel:turn");
});

test("a live Codex pump closes the per-turn stream at exact completion", async () => {
  const pump = new RecordingPump();
  pump.hangAfterRecords = true;
  const host = new CodexHost(pump);
  host.completeTurn = true;
  const driver = codexDriver(host);
  const spec = launchSpec(codexIdentity);
  const started = await driver.start(spec);
  await bindReady(started.preparation, spec, process);
  const outcome = await driver.startTurn(
    process,
    readySession(spec, codexIdentity, "codex-thread-private"),
    compiled,
    binding,
  );
  assert.equal(outcome.kind, "written");
  if (outcome.kind !== "written") return;
  assert.equal(outcome.runtimeWriteId, binding.runtimeWriteId);
  assert.equal(outcome.visibilityEventId, binding.visibilityEventId);
  const events = await Promise.race([
    collect(outcome.events),
    new Promise<never>((_resolve, reject) => {
      setTimeout(() => reject(new Error("turn stream did not close")), 100);
    }),
  ]);
  assert.deepEqual(events, [
    { kind: "assistant_reply", text: "Done." },
    { kind: "turn_complete" },
  ]);
});

test("a live Claude pump closes the per-turn stream at exact result", async () => {
  const pump = new RecordingPump();
  pump.hangAfterRecords = true;
  const host = new ClaudeHost(pump);
  host.completeTurn = true;
  const driver = claudeDriver(host);
  const spec = launchSpec(claudeIdentity);
  const started = await driver.start(spec);
  await bindReady(started.preparation, spec, process);
  const outcome = await driver.startTurn(
    process,
    readySession(spec, claudeIdentity, claudeRuntimeSessionUuid(spec.sessionId)),
    compiled,
    binding,
  );
  assert.equal(outcome.kind, "written");
  if (outcome.kind !== "written") return;
  assert.equal(outcome.runtimeWriteId, binding.runtimeWriteId);
  assert.equal(outcome.visibilityEventId, binding.visibilityEventId);
  const events = await Promise.race([
    collect(outcome.events),
    new Promise<never>((_resolve, reject) => {
      setTimeout(() => reject(new Error("turn stream did not close")), 100);
    }),
  ]);
  assert.deepEqual(events, [
    { kind: "assistant_reply", text: "Done." },
    { kind: "turn_complete" },
  ]);
});

test("swapped or wrong visibility ids produce no native written outcome and preserve siblings", async () => {
  const snapshot = structuredClone(binding);
  const codexPump = new RecordingPump();
  codexPump.wrongNextVisibilityEventId = binding.runtimeWriteId;
  const codexHost = new CodexHost(codexPump);
  codexHost.completeTurn = true;
  const codexDriver = codexDriverForHost(codexHost);
  const codexSpec = launchSpec(codexIdentity);
  const codexStarted = await codexDriver.start(codexSpec);
  await bindReady(codexStarted.preparation, codexSpec, process);
  assert.deepEqual(
    await codexDriver.startTurn(
      process,
      readySession(codexSpec, codexIdentity, "codex-thread-private"),
      compiled,
      binding,
    ),
    { kind: "ambiguous" },
  );
  assert.equal(codexPump.calls.at(-1), "cancel:turn");
  assert.deepEqual(binding, snapshot, "Codex rejection leaves non-target binding siblings unchanged");

  const claudePump = new RecordingPump();
  claudePump.wrongNextVisibilityEventId = `cmd_${"9".repeat(26)}` as CommandId;
  const claudeHost = new ClaudeHost(claudePump);
  claudeHost.completeTurn = true;
  const claudeDriver = claudeDriverForHost(claudeHost);
  const claudeSpec = launchSpec(claudeIdentity);
  const claudeStarted = await claudeDriver.start(claudeSpec);
  await bindReady(claudeStarted.preparation, claudeSpec, process);
  assert.deepEqual(
    await claudeDriver.startTurn(
      process,
      readySession(claudeSpec, claudeIdentity, claudeRuntimeSessionUuid(claudeSpec.sessionId)),
      compiled,
      binding,
    ),
    { kind: "ambiguous" },
  );
  assert.equal(claudePump.calls.at(-1), "cancel:turn");
  assert.deepEqual(binding, snapshot, "Claude rejection leaves non-target binding siblings unchanged");
});

class RecordingPump implements DriverEventPump {
  readonly stateInstanceId = stateInstanceId;
  readonly calls: string[] = [];
  readonly timeline: string[] = [];
  readonly lease: DriverEventPumpLease = {
    protocolVersion: binding.delivery.protocolVersion,
    launchId: binding.delivery.launchId,
    stateInstanceId,
    sessionId: binding.delivery.sessionId,
    ownerToken: digest("9"),
    readerEpoch: 1,
    nextOrdinal: 0,
    lastEventDigest: null,
    claimAttemptId: binding.invocation.invocationId,
    processMode: "start",
    replayMode: "live",
    snapshotHeadNextOrdinal: 0,
  };
  wrongNextWaiterEpoch = false;
  wrongNextVisibilityEventId: CommandId | undefined;
  hangAfterRecords = false;
  #waiter: DriverRegisteredEventWaiter | undefined;
  #records: DriverEventRecord[] = [];
  #ordinal = 0;

  async claimCursor(input: DriverPrivateClaimAttempt): Promise<DriverEventPumpLease> {
    this.calls.push(`claim:${input.processMode}`);
    this.timeline.push(`claim:${input.processMode}`);
    this.lease.ownerToken = input.ownerToken;
    Object.assign(this.lease, input, { snapshotHeadNextOrdinal: input.nextOrdinal });
    return this.lease;
  }

  async releaseClaimAttempt(_input: DriverPrivateClaimAttempt): Promise<void> {
    this.calls.push("release");
    this.timeline.push("release");
  }

  async closeLeaseObservers(_lease: DriverEventPumpLease): Promise<void> {}

  async registerWaiter(
    _lease: DriverEventPumpLease,
    spec: DriverEventWaiterSpec,
  ): Promise<DriverRegisteredEventWaiter> {
    this.calls.push(`register:${spec.kind}`);
    this.timeline.push(`register:${spec.kind}`);
    const waiter = {
      ...spec,
      registeredBeforeWrite: true as const,
      stream: spec.kind === "turn" ? "turn" as const : "lifecycle" as const,
      readerEpoch: this.wrongNextWaiterEpoch ? this.lease.readerEpoch + 1 : this.lease.readerEpoch,
      registeredThroughOrdinal: this.#ordinal,
    };
    this.wrongNextWaiterEpoch = false;
    this.#waiter = waiter;
    return waiter;
  }

  async cancelWaiter(_lease: DriverEventPumpLease, spec: DriverEventWaiterSpec): Promise<void> {
    this.calls.push(`cancel:${spec.kind}`);
    this.timeline.push(`cancel:${spec.kind}`);
    if (this.#waiter?.waiterId === spec.waiterId) this.#waiter = undefined;
  }

  async waitForRecord(
    _lease: DriverEventPumpLease,
    waiter: DriverRegisteredEventWaiter,
  ): Promise<DriverEventRecord> {
    this.calls.push(`wait:${waiter.kind}`);
    this.timeline.push(`wait:${waiter.kind}`);
    const expectedKind = waiter.kind === "turn" ? "model_visible" : "runtime_ready";
    const index = this.#records.findIndex((record) => (
      record.resolvedWaiterId === waiter.waiterId && record.event.kind === expectedKind
    ));
    if (index < 0) throw new Error("record missing");
    return this.#records.splice(index, 1)[0]!;
  }

  async *subscribe(_lease: DriverEventPumpLease): AsyncIterable<DriverEventRecord> {
    while (this.#records.length > 0) yield this.#records.shift()!;
    if (this.hangAfterRecords) await new Promise<void>(() => undefined);
  }

  push(events: readonly NormalizedDriverEvent[]): void {
    const waiter = this.#waiter;
    assert.ok(waiter !== undefined, "wire event requires a registered waiter");
    for (const event of events) {
      const storedEvent = event.kind === "model_visible" && this.wrongNextVisibilityEventId !== undefined
        ? { ...event, visibilityEventId: this.wrongNextVisibilityEventId }
        : event;
      if (event.kind === "model_visible") this.wrongNextVisibilityEventId = undefined;
      this.#ordinal += 1;
      this.#records.push({
        stream: storedEvent.kind === "runtime_ready" || storedEvent.kind === "runtime_terminal"
          ? "lifecycle"
          : "turn",
        stateInstanceId,
        sessionId: binding.delivery.sessionId,
        readerEpoch: this.lease.readerEpoch,
        resolvedWaiterId: waiter.waiterId,
        ordinal: this.#ordinal,
        eventDigest: digest("a"),
        bindingDigest: waiter.bindingDigest,
        event: storedEvent,
      } as DriverEventRecord);
    }
  }
}

class CodexHost implements CodexRuntimeHost {
  readonly calls: string[] = [];
  readonly writes: string[] = [];
  readonly stops: string[] = [];
  readonly #pump: RecordingPump;
  argv: readonly string[] = [];
  probeVersion = CODEX_VERSION_OUTPUT;
  spawnCount = 0;
  turnWrites = 0;
  failInitialize = false;
  failTurnWrite = false;
  completeTurn = false;

  constructor(pump: RecordingPump) {
    this.#pump = pump;
  }

  async probe(spec: DriverProbeSpec) {
    return {
      versionOutput: this.probeVersion,
      executableDigest: spec.executableDigest,
      wireProtocolDigest: spec.wireProtocolDigest,
    };
  }

  async spawn(_spec: DriverLaunchSpec, input: {
    readonly argv: readonly string[];
    readonly acceptWireMessage: CodexWireConsumer;
  }) {
    this.spawnCount += 1;
    this.argv = input.argv;
    return {
      process,
      pump: this.#pump,
      cursorOwnerToken: this.#pump.lease.ownerToken,
      initializeWaiterId: `cmd_${"1".repeat(26)}` as CommandId,
      initializeBindingDigest: digest("b"),
      transport: {
        request: async (
          request: CodexJsonRpcRequest,
          _predecessor: unknown,
          onWritten?: () => void,
        ) => {
          this.calls.push(`write:${request.method}`);
          this.#pump.timeline.push(`write:${request.method}`);
          this.writes.push(request.method);
          onWritten?.();
          if (request.method === "initialize" && this.failInitialize) throw new Error("wire failed");
          if (request.method === "turn/start") {
            this.turnWrites += 1;
            if (this.failTurnWrite) throw new Error("uncertain write");
          }
          const events = input.acceptWireMessage(responseFor(request));
          this.#pump.push(events);
          if (request.method === "turn/start" && this.completeTurn) {
            this.#pump.push(input.acceptWireMessage({
              jsonrpc: "2.0",
              method: "turn/started",
              params: {
                threadId: "codex-thread-private",
                turn: {
                  id: "codex-turn-private",
                  items: [{ type: "userMessage", clientId: binding.invocation.invocationId }],
                },
              },
            }));
            this.#pump.push(input.acceptWireMessage({
              jsonrpc: "2.0",
              method: "item/completed",
              params: {
                threadId: "codex-thread-private",
                turnId: "codex-turn-private",
                item: { type: "agentMessage", id: "reply", text: "Done." },
              },
            }));
            this.#pump.push(input.acceptWireMessage({
              jsonrpc: "2.0",
              method: "turn/completed",
              params: { threadId: "codex-thread-private", turn: { id: "codex-turn-private" } },
            }));
          }
        },
        notify: async (notification: CodexJsonRpcNotification) => {
          this.calls.push(`write:${notification.method}`);
          this.#pump.timeline.push(`write:${notification.method}`);
          this.writes.push(notification.method);
        },
      },
    };
  }

  async resume(spec: DriverResumeSpec, input: Parameters<CodexRuntimeHost["resume"]>[1]) {
    const spawned = await this.spawn(launchSpec(codexIdentity), input);
    return {
      ...spawned,
      cursorOwnerToken: spec.cursorOwnerToken,
      resumeWaiterId: `cmd_${"2".repeat(26)}` as CommandId,
      resumeBindingDigest: digest("d"),
    };
  }

  async status(): Promise<DriverStatus> {
    return { kind: "terminal", reason: "process_exited" };
  }

  async stop(actual: SpawnHandle, reason: Parameters<CodexRuntimeHost["stop"]>[1]): Promise<void> {
    this.stops.push(`${actual.processHandleRefPrivate}:${reason}`);
  }
}

class ClaudeHost implements ClaudeRuntimeHost {
  readonly calls: string[] = [];
  readonly #pump: RecordingPump;
  argv: readonly string[] = [];
  completeTurn = false;

  constructor(pump: RecordingPump) {
    this.#pump = pump;
  }

  async probe(spec: DriverProbeSpec) {
    return {
      versionOutput: CLAUDE_VERSION_OUTPUT,
      executableDigest: spec.executableDigest,
      wireProtocolDigest: spec.wireProtocolDigest,
    };
  }

  async spawn(spec: DriverLaunchSpec, input: {
    readonly argv: readonly string[];
    readonly acceptWireMessage: ClaudeWireConsumer;
  }) {
    this.argv = input.argv;
    return {
      process,
      pump: this.#pump,
      cursorOwnerToken: this.#pump.lease.ownerToken,
      initializeWaiterId: `cmd_${"1".repeat(26)}` as CommandId,
      initializeBindingDigest: digest("c"),
      transport: {
        begin: async () => {
          this.calls.push("begin");
          this.#pump.timeline.push("begin");
          this.#pump.push(input.acceptWireMessage({
            type: "system",
            subtype: "init",
            session_id: claudeRuntimeSessionUuid(spec.sessionId),
            capabilities: { control_requests: { interrupt: true, queue_receipt: true } },
          }));
        },
        writeLine: async (
          line: Uint8Array,
          _waiter: DriverRegisteredEventWaiter,
          onWritten: () => readonly NormalizedDriverEvent[],
        ) => {
          this.#pump.push(onWritten());
          if (this.completeTurn) {
            const replay = JSON.parse(new TextDecoder().decode(line)) as Record<string, unknown>;
            this.#pump.push(input.acceptWireMessage({ ...replay, isReplay: true }));
            this.#pump.push(input.acceptWireMessage({
              type: "assistant",
              session_id: claudeRuntimeSessionUuid(spec.sessionId),
              message: { content: [{ type: "text", text: "Done." }] },
            }));
            this.#pump.push(input.acceptWireMessage({
              type: "result",
              session_id: claudeRuntimeSessionUuid(spec.sessionId),
              subtype: "success",
            }));
          }
        },
        writeControl: async () => ({
          type: "control_response",
          request_id: binding.permitId,
          response: { subtype: "success", still_queued: [], cancelled: [] },
        }),
      },
    };
  }

  async resume(spec: DriverResumeSpec, input: Parameters<ClaudeRuntimeHost["resume"]>[1]) {
    const spawned = await this.spawn(launchSpec(claudeIdentity), input);
    return {
      ...spawned,
      cursorOwnerToken: spec.cursorOwnerToken,
      resumeWaiterId: `cmd_${"3".repeat(26)}` as CommandId,
      resumeBindingDigest: digest("e"),
    };
  }

  async status(): Promise<DriverStatus> {
    return { kind: "terminal", reason: "process_exited" };
  }

  async stop(): Promise<void> {}
}

function responseFor(request: CodexJsonRpcRequest): unknown {
  if (request.method === "initialize" || request.method === "turn/interrupt") {
    return { jsonrpc: "2.0", id: request.id, result: {} };
  }
  if (request.method === "thread/start" || request.method === "thread/resume") {
    return { jsonrpc: "2.0", id: request.id, result: { thread: { id: "codex-thread-private" } } };
  }
  if (request.method === "turn/start") {
    return { jsonrpc: "2.0", id: request.id, result: { turn: { id: "codex-turn-private" } } };
  }
  return { jsonrpc: "2.0", id: request.id, result: { turnId: "codex-turn-private" } };
}

function launchSpec(identity: DriverIdentity): DriverLaunchSpec {
  return {
    launch: {
      protocolVersion: identity.protocolVersion,
      agentId: binding.delivery.agentId,
      machineId: binding.delivery.machineId,
      launchId: binding.delivery.launchId,
      routingGeneration: binding.delivery.routingGeneration,
      workspaceGeneration: 1,
      stopEpoch: 0,
    },
    sessionId: binding.delivery.sessionId,
    driverIdentity: identity,
    transportDigest: process.transportDigest,
    launchEnvironmentRefPrivate: "private-home",
  };
}

function resumeSpec(identity: DriverIdentity, runtimeSessionRefPrivate: string): DriverResumeSpec {
  const start = launchSpec(identity);
  const cursorOwnerToken = digest("9");
  return {
    launch: { ...start.launch, stateInstanceId },
    expectedSessionId: start.sessionId,
    runtimeSessionRefPrivate,
    cursorOwnerToken,
    liveResumeAuthorization: Object.freeze({
      protocolVersion: start.launch.protocolVersion,
      launchId: start.launch.launchId,
      stateInstanceId,
      sessionId: start.sessionId,
      ownerToken: cursorOwnerToken,
      provedReaderEpoch: 1,
      nextOrdinal: 0,
      lastEventDigest: null,
    }),
  };
}

function probeSpec(identity: DriverIdentity): DriverProbeSpec {
  return {
    protocolVersion: identity.protocolVersion,
    runtime: identity.runtime as "codex" | "claude",
    executableRefPrivate: "private-executable",
    executableDigest: identity.executableDigest,
    wireProtocolDigest: identity.wireProtocolDigest,
  };
}

async function bindReady(
  preparation: import("../src/port.js").DriverStartPreparation,
  spec: DriverLaunchSpec,
  actualProcess: SpawnHandle,
): Promise<void> {
  const launch: ReadyLaunchFence = {
    ...spec.launch,
    stateInstanceId: actualProcess.stateInstanceId,
    sessionId: spec.sessionId,
  };
  const persisted = await preparation.bindRuntimeReady(launch, async () => ({
    disposition: "applied",
    value: "ready",
  }));
  assert.equal(persisted.value, "ready");
}

const unusedRetainedSource: DriverRetainedEventSource = {
  async openReplay() {
    throw new Error("retained replay is not used by the live adapter controls");
  },
};

const testCursorCoordinator: DriverCursorClaimCoordinator = {
  async claimAfterSpawn(input) {
    return claimPump(input.pump, {
      protocolVersion: input.spec.launch.protocolVersion,
      launchId: input.spec.launch.launchId,
      stateInstanceId: input.process.stateInstanceId,
      sessionId: input.spec.sessionId,
      ownerToken: input.cursorOwnerToken,
      readerEpoch: 1,
      nextOrdinal: 0,
      lastEventDigest: null,
      claimAttemptId: binding.invocation.invocationId,
      processMode: "start",
      replayMode: "live",
    });
  },
  async claimForReplay(input) {
    return claimPump(input.pump, {
      protocolVersion: input.protocolVersion,
      launchId: input.launchId,
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      ownerToken: input.cursorOwnerToken,
      readerEpoch: 1,
      nextOrdinal: 0,
      lastEventDigest: null,
      claimAttemptId: binding.invocation.invocationId,
      processMode: "resume",
      replayMode: "retained_only",
    });
  },
  beginLiveResume(input) {
    assert.equal(input.authorization.protocolVersion, input.protocolVersion);
    assert.equal(input.authorization.launchId, input.launchId);
    assert.equal(input.authorization.stateInstanceId, input.stateInstanceId);
    assert.equal(input.authorization.sessionId, input.sessionId);
    assert.equal(input.authorization.ownerToken, input.cursorOwnerToken);
    return Object.freeze({
      protocolVersion: input.protocolVersion,
      launchId: input.launchId,
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      ownerToken: input.cursorOwnerToken,
      nextOrdinal: input.authorization.nextOrdinal,
      lastEventDigest: input.authorization.lastEventDigest,
    });
  },
  async claimForLiveResume(input) {
    return claimPump(input.pump, {
      protocolVersion: input.ticket.protocolVersion,
      launchId: input.ticket.launchId,
      stateInstanceId: input.ticket.stateInstanceId,
      sessionId: input.ticket.sessionId,
      ownerToken: input.ticket.ownerToken,
      readerEpoch: 1,
      nextOrdinal: input.ticket.nextOrdinal,
      lastEventDigest: input.ticket.lastEventDigest,
      claimAttemptId: binding.invocation.invocationId,
      processMode: "resume",
      replayMode: "live",
    });
  },
  cancelLiveResume() {},
  async releaseReplayAsNoActive(claim) {
    await claim.abort();
    return Object.freeze({
      protocolVersion: claim.authority.protocolVersion,
      launchId: claim.authority.launchId,
      stateInstanceId: claim.authority.stateInstanceId,
      sessionId: claim.authority.sessionId,
      ownerToken: claim.authority.ownerToken,
      provedReaderEpoch: claim.authority.readerEpoch,
      nextOrdinal: claim.authority.nextOrdinal,
      lastEventDigest: claim.authority.lastEventDigest,
    });
  },
};

function codexDriver(host: CodexRuntimeHost): CodexNativeProcessDriver {
  return new CodexNativeProcessDriver(host, unusedRetainedSource, testCursorCoordinator);
}

function codexDriverForHost(host: CodexRuntimeHost): CodexNativeProcessDriver {
  return codexDriver(host);
}

function claudeDriver(host: ClaudeRuntimeHost): ClaudeNativeProcessDriver {
  return new ClaudeNativeProcessDriver(host, unusedRetainedSource, testCursorCoordinator);
}

function claudeDriverForHost(host: ClaudeRuntimeHost): ClaudeNativeProcessDriver {
  return claudeDriver(host);
}

async function claimPump(
  pump: DriverEventPump,
  attempt: DriverPrivateClaimAttempt,
): Promise<DriverCompositeCursorClaimHandle> {
  const privateLease = await pump.claimCursor(attempt);
  let closed = false;
  const close = async () => {
    if (closed) return { applied: false, storageReleased: true, privateReleased: true };
    closed = true;
    await pump.closeLeaseObservers(privateLease);
    await pump.releaseClaimAttempt(attempt);
    return { applied: true, storageReleased: true, privateReleased: true };
  };
  return {
    authority: {
      protocolVersion: attempt.protocolVersion,
      launchId: attempt.launchId,
      stateInstanceId: attempt.stateInstanceId,
      sessionId: attempt.sessionId,
      ownerToken: attempt.ownerToken,
      readerEpoch: attempt.readerEpoch,
      nextOrdinal: attempt.nextOrdinal,
      lastEventDigest: attempt.lastEventDigest,
    },
    privateLease,
    abort: close,
    release: close,
  };
}

function readySession(
  spec: DriverLaunchSpec,
  identity: DriverIdentity,
  runtimeSessionRef: string,
): DriverSession {
  return {
    launch: {
      ...spec.launch,
      stateInstanceId,
      sessionId: spec.sessionId,
    },
    driverIdentity: identity,
    runtimeSessionRef,
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

async function asyncErrorMessage(action: () => Promise<unknown>): Promise<string> {
  try {
    await action();
  } catch (error) {
    assert.ok(error instanceof Error);
    return error.message;
  }
  return "NO_ERROR";
}

async function collect<T>(events: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const event of events) values.push(event);
  return values;
}
