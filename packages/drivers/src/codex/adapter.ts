import type {
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  NativeRuntimeEvent,
  NormalizedDriverEvent,
  SessionId,
  StopReason,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import { protocolDigest } from "@swarm/runtime-contract";

import {
  DriverStartPreparation,
  type DriverEventPumpLease,
  type DriverEventRecord,
  type DriverEventWaiterSpec,
  type DriverLaunchSpec,
  type DriverProbeSpec,
  type DriverRegisteredEventWaiter,
  type DriverResumeSpec,
  type DriverStartedProcess,
  type DriverStatus,
  type NativeProcessDriver,
  type NativeProcessWriteOutcome,
  type SpawnHandle,
} from "../port.js";
import {
  DriverNormalizationError,
  assertDriverCapability,
} from "../normalizer.js";
import {
  CODEX_APP_SERVER_ARGV,
  CODEX_CAPABILITY,
  CODEX_RUNTIME_VERSION,
  CODEX_VERSION_OUTPUT,
  CODEX_WIRE_PROTOCOL_DIGEST,
} from "./constants.js";
import type {
  CodexRuntimeHost,
  CodexSpawnedRuntime,
  CodexTransport,
} from "./types.js";
import { CodexWireState } from "./wire.js";

type CodexProcessContext = {
  readonly process: SpawnHandle;
  readonly pump: CodexSpawnedRuntime["pump"];
  readonly transport: CodexTransport;
  readonly wire: CodexWireState;
  readonly sessionId: SessionId;
  readonly identity?: DriverIdentity;
  readonly activeTurnCommands: Map<TurnId, CommandId>;
  lease: DriverEventPumpLease;
  preparation?: DriverStartPreparation;
  released: boolean;
};

export class CodexNativeProcessDriver implements NativeProcessDriver {
  readonly #host: CodexRuntimeHost;
  readonly #contexts = new Map<string, CodexProcessContext>();

  constructor(host: CodexRuntimeHost) {
    this.#host = host;
  }

  async probe(spec: DriverProbeSpec): Promise<DriverIdentity> {
    if (spec.runtime !== "codex" || spec.wireProtocolDigest !== CODEX_WIRE_PROTOCOL_DIGEST) {
      throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
    }
    const observed = await this.#host.probe(spec);
    if (
      observed.versionOutput !== CODEX_VERSION_OUTPUT
      || observed.executableDigest !== spec.executableDigest
      || observed.wireProtocolDigest !== CODEX_WIRE_PROTOCOL_DIGEST
    ) throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
    return {
      protocolVersion: spec.protocolVersion,
      runtime: "codex",
      executableDigest: observed.executableDigest,
      version: CODEX_RUNTIME_VERSION,
      wireProtocolDigest: CODEX_WIRE_PROTOCOL_DIGEST,
      capability: { ...CODEX_CAPABILITY },
    };
  }

  async start(spec: DriverLaunchSpec): Promise<DriverStartedProcess> {
    assertIdentity(spec.driverIdentity, spec);
    const wire = new CodexWireState();
    const spawned = await this.#host.spawn(spec, {
      argv: CODEX_APP_SERVER_ARGV,
      acceptWireMessage: (message) => wire.accept(message),
    });
    let capturedLease: DriverEventPumpLease | undefined;
    let preparation: DriverStartPreparation;
    try {
      preparation = await DriverStartPreparation.start({
        spec,
        process: spawned.process,
        pump: spawned.pump,
        cursorOwnerToken: spawned.cursorOwnerToken,
        initializeWaiterId: spawned.initializeWaiterId,
        initializeBindingDigest: spawned.initializeBindingDigest,
        writeInitialize: async (witness) => {
          capturedLease = witness.lease;
          const initialize = wire.initializeRequest(spawned.initializeWaiterId);
          await spawned.transport.request(initialize, witness);
          await spawned.transport.notify(wire.initializedNotification(), witness);
          const start = wire.threadStartRequest(
            `${spawned.initializeWaiterId}:thread`,
            spec.sessionId,
          );
          await spawned.transport.request(start, witness);
        },
      });
    } catch (cause) {
      return await stopAfterFailedStart(
        this.#host,
        spawned.process,
        cause,
        "CODEX_DRIVER_START_FAILED",
      );
    }
    if (capturedLease === undefined) {
      throw new DriverNormalizationError("DRIVER_WAITER_PREDECESSOR_REQUIRED");
    }
    const context: CodexProcessContext = {
      process: spawned.process,
      pump: spawned.pump,
      transport: spawned.transport,
      wire,
      sessionId: spec.sessionId,
      identity: spec.driverIdentity,
      activeTurnCommands: new Map(),
      lease: capturedLease,
      preparation,
      released: false,
    };
    this.#contexts.set(spawned.process.processHandleRefPrivate, context);
    return { process: spawned.process, preparation };
  }

  async resume(spec: DriverResumeSpec): Promise<SpawnHandle> {
    const wire = new CodexWireState();
    const resumed = await this.#host.resume(spec, {
      argv: CODEX_APP_SERVER_ARGV,
      acceptWireMessage: (message) => wire.accept(message),
    });
    assertResumeProcess(resumed.process, spec);
    const lease = await resumed.pump.claimCursor({
      sessionId: spec.expectedSessionId,
      ownerToken: spec.cursorOwnerToken,
      mode: "resume",
    });
    const waiterSpec: DriverEventWaiterSpec = {
      kind: "resume",
      waiterId: resumed.resumeWaiterId,
      stateInstanceId: resumed.process.stateInstanceId,
      sessionId: spec.expectedSessionId,
      bindingDigest: resumed.resumeBindingDigest,
    };
    let waiter: DriverRegisteredEventWaiter | undefined;
    try {
      assertLease(lease, resumed.process, spec.expectedSessionId, spec.cursorOwnerToken);
      waiter = await resumed.pump.registerWaiter(lease, waiterSpec);
      assertWaiter(waiter, waiterSpec, lease.readerEpoch);
      await resumed.transport.request(wire.initializeRequest(resumed.initializeWaiterId), waiter);
      await resumed.transport.notify(wire.initializedNotification(), waiter);
      const request = wire.threadResumeRequest(
        resumed.resumeWaiterId,
        spec.expectedSessionId,
        spec.runtimeSessionRefPrivate,
      );
      await resumed.transport.request(request, waiter);
      const ready = await resumed.pump.waitForRecord(lease, waiter);
      assertReadyRecord(ready, resumed.process, spec.expectedSessionId, lease, waiter);
    } catch (cause) {
      await cleanupWaiter(resumed.pump, lease, waiterSpec, cause, true);
    }
    this.#contexts.set(resumed.process.processHandleRefPrivate, {
      process: resumed.process,
      pump: resumed.pump,
      transport: resumed.transport,
      wire,
      sessionId: spec.expectedSessionId,
      activeTurnCommands: new Map(),
      lease,
      released: false,
    });
    return resumed.process;
  }

  startTurn(
    process: SpawnHandle,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): Promise<NativeProcessWriteOutcome> {
    const context = this.#context(process, session);
    const requestId = binding.invocation.invocationId;
    const request = context.wire.turnStartRequest(requestId, session, input, binding);
    return this.#writeTurn(context, requestId, request, binding);
  }

  steerTurn(
    process: SpawnHandle,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): Promise<NativeProcessWriteOutcome> {
    assertDriverCapability(session.driverIdentity, "steer");
    const context = this.#context(process, session);
    const requestId = binding.invocation.invocationId;
    const request = context.wire.turnSteerRequest(requestId, session, input, binding);
    return this.#writeTurn(context, requestId, request, binding);
  }

  async interrupt(
    process: SpawnHandle,
    session: DriverSession,
    expectedTurnId: TurnId,
  ): Promise<void> {
    assertDriverCapability(session.driverIdentity, "interrupt");
    const context = this.#context(process, session);
    const id = context.activeTurnCommands.get(expectedTurnId);
    if (id === undefined) this.#fence();
    const request = context.wire.interruptRequest(id, session, expectedTurnId);
    let waiter: DriverRegisteredEventWaiter;
    try {
      waiter = await this.#registerTurnWaiter(
        context,
        expectedTurnId,
        id,
        protocolDigest({ kind: "interrupt", expectedTurnId, sessionId: context.sessionId }),
      );
    } catch (cause) {
      context.wire.cancelPendingRequest(id);
      throw cause;
    }
    try {
      await context.transport.request(request, waiter, () => context.wire.markRequestWritten(id));
    } catch (cause) {
      await cleanupWaiter(context.pump, context.lease, waiter, cause, false);
    }
    await cancelAfterAcknowledged(context.pump, context.lease, waiter, "CODEX_INTERRUPT_CLEANUP_FAILED");
  }

  status(process: SpawnHandle, session?: DriverSession): Promise<DriverStatus> {
    const context = this.#contexts.get(process.processHandleRefPrivate);
    if (context === undefined || context.process !== process) this.#fence();
    const identity = session?.driverIdentity ?? context.identity;
    if (identity === undefined) this.#protocol();
    if (session !== undefined) this.#assertSession(context, session);
    return this.#host.status(process, identity);
  }

  events(process: SpawnHandle): AsyncIterable<NormalizedDriverEvent> {
    const context = this.#contexts.get(process.processHandleRefPrivate);
    if (context === undefined || context.process !== process || context.released) this.#fence();
    return records(context);
  }

  async stop(process: SpawnHandle, reason: StopReason): Promise<void> {
    const context = this.#contexts.get(process.processHandleRefPrivate);
    if (context === undefined || context.process !== process) this.#fence();
    const failures: unknown[] = [];
    let preparationReleased = false;
    if (context.preparation !== undefined) {
      try {
        const result = await context.preparation.abort("stop");
        preparationReleased = result.disposition === "aborted";
      } catch (error) {
        failures.push(error);
        preparationReleased = true;
      }
    }
    try {
      await this.#host.stop(process, reason);
    } catch (error) {
      failures.push(error);
    }
    if (!preparationReleased && !context.released) {
      try {
        await context.pump.release(context.lease);
      } catch (error) {
        failures.push(error);
      }
    }
    context.released = true;
    this.#contexts.delete(process.processHandleRefPrivate);
    if (failures.length > 0) throw new AggregateError(failures, "CODEX_DRIVER_STOP_FAILED");
  }

  async #writeTurn(
    context: CodexProcessContext,
    requestId: string,
    request: Parameters<CodexTransport["request"]>[0],
    binding: DriverTurnBinding,
  ): Promise<NativeProcessWriteOutcome> {
    const bindingDigest = protocolDigest(binding);
    let waiter: DriverRegisteredEventWaiter;
    try {
      waiter = await this.#registerTurnWaiter(
        context,
        binding.protocolTurnId,
        binding.invocation.invocationId,
        bindingDigest,
      );
    } catch (cause) {
      context.wire.cancelPendingRequest(requestId);
      throw cause;
    }
    try {
      await context.transport.request(request, waiter, () => context.wire.markRequestWritten(requestId));
    } catch {
      await cancelIgnoringFailure(context, waiter);
      return { kind: "ambiguous" };
    }
    let record: DriverEventRecord;
    try {
      record = await context.pump.waitForRecord(context.lease, waiter);
      assertTurnRecord(record, context, waiter, binding);
    } catch {
      await cancelIgnoringFailure(context, waiter);
      return { kind: "ambiguous" };
    }
    context.activeTurnCommands.set(binding.protocolTurnId, binding.permitId);
    return {
      kind: "written",
      runtimeWriteId: binding.runtimeWriteId,
      visibilityEventId: binding.visibilityEventId,
      events: nativeEvents(context, binding.protocolTurnId),
    };
  }

  async #registerTurnWaiter(
    context: CodexProcessContext,
    turnId: TurnId,
    waiterId: CommandId,
    bindingDigest: ArtifactDigest,
  ): Promise<DriverRegisteredEventWaiter> {
    const spec: DriverEventWaiterSpec = {
      kind: "turn",
      waiterId,
      stateInstanceId: context.process.stateInstanceId,
      sessionId: context.sessionId,
      turnId,
      bindingDigest,
    };
    try {
      const waiter = await context.pump.registerWaiter(context.lease, spec);
      assertWaiter(waiter, spec, context.lease.readerEpoch);
      return waiter;
    } catch (cause) {
      return await cleanupWaiter(context.pump, context.lease, spec, cause, false);
    }
  }

  #context(process: SpawnHandle, session: DriverSession): CodexProcessContext {
    const context = this.#contexts.get(process.processHandleRefPrivate);
    if (context === undefined || context.process !== process || context.released) this.#fence();
    this.#assertSession(context, session);
    return context;
  }

  #assertSession(context: CodexProcessContext, session: DriverSession): void {
    if (
      session.driverIdentity.runtime !== "codex"
      || session.launch.launchId !== context.process.launchId
      || session.launch.stateInstanceId !== context.process.stateInstanceId
      || session.launch.sessionId !== context.sessionId
    ) this.#fence();
    context.wire.assertSession(session);
  }

  #fence(): never {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }

  #protocol(): never {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
}

function assertIdentity(identity: DriverIdentity, spec: DriverLaunchSpec): void {
  if (
    identity.runtime !== "codex"
    || identity.version !== CODEX_RUNTIME_VERSION
    || identity.wireProtocolDigest !== CODEX_WIRE_PROTOCOL_DIGEST
    || identity.protocolVersion !== spec.launch.protocolVersion
  ) throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  for (const [key, value] of Object.entries(CODEX_CAPABILITY)) {
    if (identity.capability[key as keyof typeof CODEX_CAPABILITY] !== value) {
      throw new DriverNormalizationError("DRIVER_CAPABILITY_MISMATCH");
    }
  }
}

function assertResumeProcess(process: SpawnHandle, spec: DriverResumeSpec): void {
  if (
    process.launchId !== spec.launch.launchId
    || process.stateInstanceId !== spec.launch.stateInstanceId
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

function assertLease(
  lease: DriverEventPumpLease,
  process: SpawnHandle,
  sessionId: SessionId,
  ownerToken: ArtifactDigest,
): void {
  if (
    lease.stateInstanceId !== process.stateInstanceId
    || lease.sessionId !== sessionId
    || lease.ownerToken !== ownerToken
    || !Number.isSafeInteger(lease.readerEpoch)
    || lease.readerEpoch < 1
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

function assertWaiter(
  waiter: DriverRegisteredEventWaiter,
  expected: DriverEventWaiterSpec,
  readerEpoch: number,
): void {
  if (
    waiter.kind !== expected.kind
    || waiter.waiterId !== expected.waiterId
    || waiter.stateInstanceId !== expected.stateInstanceId
    || waiter.sessionId !== expected.sessionId
    || waiter.turnId !== expected.turnId
    || waiter.bindingDigest !== expected.bindingDigest
    || waiter.readerEpoch !== readerEpoch
    || waiter.registeredBeforeWrite !== true
    || !Number.isSafeInteger(waiter.registeredThroughOrdinal)
    || waiter.registeredThroughOrdinal < 0
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

function assertReadyRecord(
  record: DriverEventRecord,
  process: SpawnHandle,
  sessionId: SessionId,
  lease: DriverEventPumpLease,
  waiter: DriverRegisteredEventWaiter,
): void {
  if (
    record.stateInstanceId !== process.stateInstanceId
    || record.sessionId !== sessionId
    || record.readerEpoch !== lease.readerEpoch
    || record.resolvedWaiterId !== waiter.waiterId
    || record.bindingDigest !== waiter.bindingDigest
    || record.ordinal <= waiter.registeredThroughOrdinal
    || record.event.kind !== "runtime_ready"
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

function assertTurnRecord(
  record: DriverEventRecord,
  context: CodexProcessContext,
  waiter: DriverRegisteredEventWaiter,
  binding: DriverTurnBinding,
): void {
  if (
    record.stateInstanceId !== context.process.stateInstanceId
    || record.sessionId !== context.sessionId
    || record.readerEpoch !== context.lease.readerEpoch
    || record.resolvedWaiterId !== waiter.waiterId
    || record.bindingDigest !== waiter.bindingDigest
    || record.ordinal <= waiter.registeredThroughOrdinal
    || record.event.kind !== "model_visible"
    || record.event.turnId !== binding.protocolTurnId
    || record.event.visibilityEventId !== binding.visibilityEventId
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

async function* records(context: CodexProcessContext): AsyncIterable<NormalizedDriverEvent> {
  for await (const record of context.pump.subscribe(context.lease)) {
    if (
      record.stateInstanceId !== context.process.stateInstanceId
      || record.sessionId !== context.sessionId
      || record.readerEpoch !== context.lease.readerEpoch
    ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
    yield record.event;
  }
}

async function* nativeEvents(
  context: CodexProcessContext,
  turnId: TurnId,
): AsyncIterable<NativeRuntimeEvent> {
  for await (const record of context.pump.subscribe(context.lease)) {
    if (
      record.stateInstanceId !== context.process.stateInstanceId
      || record.sessionId !== context.sessionId
      || record.readerEpoch !== context.lease.readerEpoch
    ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
    const event = record.event;
    if ("turnId" in event && event.turnId !== turnId) {
      throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
    }
    if (event.kind === "assistant_reply") yield { kind: "assistant_reply", text: event.text };
    if (event.kind === "coordination_call") {
      yield { kind: "coordination_call", commandId: event.commandId, command: event.command };
    }
    if (event.kind === "turn_completed") {
      yield { kind: "turn_complete" };
      return;
    }
    if (event.kind === "runtime_terminal") return;
  }
}

async function cancelAfterAcknowledged(
  pump: CodexSpawnedRuntime["pump"],
  lease: DriverEventPumpLease,
  waiter: DriverEventWaiterSpec,
  message: string,
): Promise<void> {
  try {
    await pump.cancelWaiter(lease, waiter);
  } catch (cause) {
    throw new AggregateError([cause], message);
  }
}

async function stopAfterFailedStart(
  host: CodexRuntimeHost,
  process: SpawnHandle,
  cause: unknown,
  message: string,
): Promise<never> {
  try {
    await host.stop(process, "driver_protocol_error");
  } catch (stopCause) {
    throw new AggregateError([cause, stopCause], message);
  }
  throw cause;
}

async function cancelIgnoringFailure(
  context: CodexProcessContext,
  waiter: DriverRegisteredEventWaiter,
): Promise<void> {
  try {
    await context.pump.cancelWaiter(context.lease, waiter);
  } catch {
    // The real write is already ambiguous; cleanup failure cannot make it retryable.
  }
}

async function cleanupWaiter(
  pump: CodexSpawnedRuntime["pump"],
  lease: DriverEventPumpLease,
  waiter: DriverEventWaiterSpec,
  cause: unknown,
  release: boolean,
): Promise<never> {
  const failures: unknown[] = [cause];
  try {
    await pump.cancelWaiter(lease, waiter);
  } catch (error) {
    failures.push(error);
  }
  if (release) {
    try {
      await pump.release(lease);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length > 1) throw new AggregateError(failures, "CODEX_DRIVER_CLEANUP_FAILED");
  throw cause;
}
