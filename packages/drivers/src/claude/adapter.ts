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
  type DriverCursorClaimCoordinator,
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
import type {
  DriverCompositeCursorClaimHandle,
  DriverRetainedEventSource,
  DriverRetainedReplay,
} from "../retained-events.js";
import { DriverNormalizationError, assertDriverCapability } from "../normalizer.js";
import {
  CLAUDE_CAPABILITY,
  CLAUDE_RUNTIME_VERSION,
  CLAUDE_STREAM_ARGV,
  CLAUDE_VERSION_OUTPUT,
} from "./constants.js";
import type {
  ClaudeRuntimeHost,
  ClaudeSpawnedRuntime,
  ClaudeTransport,
} from "./types.js";
import { claudeRuntimeSessionUuid } from "./uuid.js";
import { ClaudeWireState } from "./wire.js";

type ClaudeProcessContext = {
  readonly process: SpawnHandle;
  readonly pump: ClaudeSpawnedRuntime["pump"];
  readonly transport: ClaudeTransport;
  readonly wire: ClaudeWireState;
  readonly sessionId: SessionId;
  readonly runtimeSessionRef: string;
  readonly identity?: DriverIdentity;
  readonly cursorClaim: DriverCompositeCursorClaimHandle;
  lease: DriverEventPumpLease;
  preparation?: DriverStartPreparation;
  released: boolean;
};

export class ClaudeNativeProcessDriver implements NativeProcessDriver {
  readonly #host: ClaudeRuntimeHost;
  readonly #retained: DriverRetainedEventSource;
  readonly #cursorClaimCoordinator: DriverCursorClaimCoordinator;
  readonly #contexts = new Map<string, ClaudeProcessContext>();

  constructor(
    host: ClaudeRuntimeHost,
    retained: DriverRetainedEventSource,
    cursorClaimCoordinator: DriverCursorClaimCoordinator,
  ) {
    this.#host = host;
    this.#retained = retained;
    this.#cursorClaimCoordinator = cursorClaimCoordinator;
  }

  async probe(spec: DriverProbeSpec): Promise<DriverIdentity> {
    if (spec.runtime !== "claude") this.#protocol();
    const observed = await this.#host.probe(spec);
    if (
      observed.versionOutput !== CLAUDE_VERSION_OUTPUT
      || observed.executableDigest !== spec.executableDigest
      || observed.wireProtocolDigest !== spec.wireProtocolDigest
    ) this.#protocol();
    return {
      protocolVersion: spec.protocolVersion,
      runtime: "claude",
      executableDigest: observed.executableDigest,
      version: CLAUDE_RUNTIME_VERSION,
      wireProtocolDigest: observed.wireProtocolDigest,
      capability: { ...CLAUDE_CAPABILITY },
    };
  }

  async start(spec: DriverLaunchSpec): Promise<DriverStartedProcess> {
    assertIdentity(spec.driverIdentity, spec);
    const runtimeSessionRef = claudeRuntimeSessionUuid(spec.sessionId);
    const wire = new ClaudeWireState(spec.sessionId, runtimeSessionRef);
    const spawned = await this.#host.spawn(spec, {
      argv: [...CLAUDE_STREAM_ARGV, "--session-id", runtimeSessionRef],
      acceptWireMessage: (message) => wire.accept(message),
    });
    let cursorClaim: DriverCompositeCursorClaimHandle;
    try {
      cursorClaim = await this.#cursorClaimCoordinator.claimAfterSpawn({
        spec,
        process: spawned.process,
        pump: spawned.pump,
        cursorOwnerToken: spawned.cursorOwnerToken,
      });
    } catch (cause) {
      return await stopAfterFailedStart(
        this.#host,
        spawned.process,
        cause,
        "CLAUDE_DRIVER_START_AUTHORITY_FAILED",
      );
    }
    let capturedLease: DriverEventPumpLease | undefined;
    let preparation: DriverStartPreparation;
    try {
      preparation = await DriverStartPreparation.start({
        spec,
        process: spawned.process,
        pump: spawned.pump,
        cursorClaim,
        initializeWaiterId: spawned.initializeWaiterId,
        initializeBindingDigest: spawned.initializeBindingDigest,
        writeInitialize: async (witness) => {
          capturedLease = witness.lease;
          await spawned.transport.begin(witness);
        },
      });
    } catch (cause) {
      return await stopAfterFailedStart(
        this.#host,
        spawned.process,
        cause,
        "CLAUDE_DRIVER_START_FAILED",
      );
    }
    if (capturedLease === undefined) {
      throw new DriverNormalizationError("DRIVER_WAITER_PREDECESSOR_REQUIRED");
    }
    const context: ClaudeProcessContext = {
      process: spawned.process,
      pump: spawned.pump,
      transport: spawned.transport,
      wire,
      sessionId: spec.sessionId,
      runtimeSessionRef,
      identity: spec.driverIdentity,
      cursorClaim,
      lease: capturedLease,
      preparation,
      released: false,
    };
    this.#contexts.set(spawned.process.processHandleRefPrivate, context);
    return { process: spawned.process, preparation };
  }

  async resume(spec: DriverResumeSpec): Promise<SpawnHandle> {
    if (!isUuid(spec.runtimeSessionRefPrivate)) this.#protocol();
    const wire = new ClaudeWireState(spec.expectedSessionId, spec.runtimeSessionRefPrivate);
    const ticket = this.#cursorClaimCoordinator.beginLiveResume({
      protocolVersion: spec.launch.protocolVersion,
      launchId: spec.launch.launchId,
      stateInstanceId: spec.launch.stateInstanceId,
      sessionId: spec.expectedSessionId,
      cursorOwnerToken: spec.cursorOwnerToken,
      authorization: spec.liveResumeAuthorization,
    });
    let resumed: Awaited<ReturnType<ClaudeRuntimeHost["resume"]>>;
    try {
      resumed = await this.#host.resume(spec, {
        argv: [...CLAUDE_STREAM_ARGV, "--resume", spec.runtimeSessionRefPrivate],
        acceptWireMessage: (message) => wire.accept(message),
      });
    } catch (cause) {
      this.#cursorClaimCoordinator.cancelLiveResume(ticket);
      throw cause;
    }
    let cursorClaim: DriverCompositeCursorClaimHandle;
    try {
      assertResumeProcess(resumed.process, spec);
      cursorClaim = await this.#cursorClaimCoordinator.claimForLiveResume({
        ticket,
        pump: resumed.pump,
      });
    } catch (cause) {
      this.#cursorClaimCoordinator.cancelLiveResume(ticket);
      return await stopAfterFailedStart(
        this.#host,
        resumed.process,
        cause,
        "CLAUDE_DRIVER_RESUME_AUTHORITY_FAILED",
      );
    }
    const lease = cursorClaim.privateLease;
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
      await resumed.transport.begin(waiter);
      const ready = await resumed.pump.waitForRecord(lease, waiter);
      assertReadyRecord(ready, resumed.process, spec.expectedSessionId, lease, waiter);
    } catch (cause) {
      let cleanedCause: unknown = cause;
      try {
        await cleanupWaiter(resumed.pump, lease, waiterSpec, cause, cursorClaim);
      } catch (error) {
        cleanedCause = error;
      }
      return await stopAfterFailedStart(
        this.#host,
        resumed.process,
        cleanedCause,
        "CLAUDE_DRIVER_RESUME_FAILED",
      );
    }
    this.#contexts.set(resumed.process.processHandleRefPrivate, {
      process: resumed.process,
      pump: resumed.pump,
      transport: resumed.transport,
      wire,
      sessionId: spec.expectedSessionId,
      runtimeSessionRef: spec.runtimeSessionRefPrivate,
      cursorClaim,
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
    return this.#writeTurn(context, session, input, binding);
  }

  async steerTurn(
    process: SpawnHandle,
    session: DriverSession,
    _input: CompiledNativeTurn,
    _binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): Promise<NativeProcessWriteOutcome> {
    this.#context(process, session);
    assertDriverCapability(session.driverIdentity, "steer");
    throw new DriverNormalizationError("DRIVER_CAPABILITY_MISMATCH");
  }

  async interrupt(
    process: SpawnHandle,
    session: DriverSession,
    expectedTurnId: TurnId,
  ): Promise<void> {
    assertDriverCapability(session.driverIdentity, "interrupt");
    const context = this.#context(process, session);
    const prepared = context.wire.interruptRequest(expectedTurnId);
    const waiter = await this.#registerTurnWaiter(
      context,
      expectedTurnId,
      prepared.request.request_id,
      protocolDigest({
        kind: "interrupt",
        requestId: prepared.request.request_id,
        sessionId: context.sessionId,
        expectedTurnId,
      }),
    );
    let receipt: unknown;
    try {
      receipt = await context.transport.writeControl(prepared.line, waiter);
    } catch (cause) {
      await cleanupWaiter(context.pump, context.lease, waiter, cause);
    }
    try {
      context.wire.acceptInterruptReceipt(receipt, prepared.request.request_id);
    } catch (cause) {
      await cleanupWaiter(context.pump, context.lease, waiter, cause);
    }
    await cancelAfterAcknowledged(context.pump, context.lease, waiter, "CLAUDE_INTERRUPT_CLEANUP_FAILED");
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

  recoverEvents(input: {
    claim: DriverCompositeCursorClaimHandle;
    expectedTurnId: TurnId;
    expectedBindingDigest: ArtifactDigest;
    expectedResolvedWaiterId: CommandId;
    expectedSourceMessageId: import("@swarm/protocol").MessageId;
  }): Promise<DriverRetainedReplay> {
    return this.#retained.openReplay(input);
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
        await context.cursorClaim.release();
      } catch (error) {
        failures.push(error);
      }
    }
    context.released = true;
    this.#contexts.delete(process.processHandleRefPrivate);
    if (failures.length > 0) throw new AggregateError(failures, "CLAUDE_DRIVER_STOP_FAILED");
  }

  async #writeTurn(
    context: ClaudeProcessContext,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): Promise<NativeProcessWriteOutcome> {
    const waiter = await this.#registerTurnWaiter(
      context,
      binding.protocolTurnId,
      binding.invocation.invocationId,
      protocolDigest(binding),
    );
    let prepared: ReturnType<ClaudeWireState["beginTurn"]>;
    try {
      prepared = context.wire.beginTurn(session, input, binding);
    } catch (cause) {
      return await cleanupWaiter(context.pump, context.lease, waiter, cause);
    }
    try {
      await context.transport.writeLine(
        prepared.line,
        waiter,
        () => context.wire.markInputWritten(prepared.input.uuid),
        { predecessor: waiter, sourceMessageId: input.input.current.delivery.messageId },
      );
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
    return {
      kind: "written",
      runtimeWriteId: binding.runtimeWriteId,
      visibilityEventId: binding.visibilityEventId,
      events: nativeEvents(context, binding.protocolTurnId, waiter.waiterId),
    };
  }

  async #registerTurnWaiter(
    context: ClaudeProcessContext,
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
      return await cleanupWaiter(context.pump, context.lease, spec, cause);
    }
  }

  #context(process: SpawnHandle, session: DriverSession): ClaudeProcessContext {
    const context = this.#contexts.get(process.processHandleRefPrivate);
    if (context === undefined || context.process !== process || context.released) this.#fence();
    this.#assertSession(context, session);
    return context;
  }

  #assertSession(context: ClaudeProcessContext, session: DriverSession): void {
    if (
      session.driverIdentity.runtime !== "claude"
      || session.launch.launchId !== context.process.launchId
      || session.launch.stateInstanceId !== context.process.stateInstanceId
      || session.launch.sessionId !== context.sessionId
      || session.runtimeSessionRef !== context.runtimeSessionRef
    ) this.#fence();
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
    identity.runtime !== "claude"
    || identity.version !== CLAUDE_RUNTIME_VERSION
    || identity.protocolVersion !== spec.launch.protocolVersion
  ) throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  for (const [key, value] of Object.entries(CLAUDE_CAPABILITY)) {
    if (identity.capability[key as keyof typeof CLAUDE_CAPABILITY] !== value) {
      throw new DriverNormalizationError("DRIVER_CAPABILITY_MISMATCH");
    }
  }
}

function assertResumeProcess(process: SpawnHandle, spec: DriverResumeSpec): void {
  if (process.launchId !== spec.launch.launchId || process.stateInstanceId !== spec.launch.stateInstanceId) {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }
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
    || waiter.stream !== (expected.kind === "turn" ? "turn" : "lifecycle")
    || (waiter.registeredThroughOrdinal !== null
      && (!Number.isSafeInteger(waiter.registeredThroughOrdinal)
        || waiter.registeredThroughOrdinal < 0))
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
    || record.stream !== "lifecycle"
    || record.resolvedWaiterId !== waiter.waiterId
    || record.bindingDigest !== waiter.bindingDigest
    || (waiter.registeredThroughOrdinal !== null
      && record.ordinal <= waiter.registeredThroughOrdinal)
    || record.event.kind !== "runtime_ready"
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

function assertTurnRecord(
  record: DriverEventRecord,
  context: ClaudeProcessContext,
  waiter: DriverRegisteredEventWaiter,
  binding: DriverTurnBinding,
): void {
  if (
    record.stateInstanceId !== context.process.stateInstanceId
    || record.sessionId !== context.sessionId
    || record.readerEpoch !== context.lease.readerEpoch
    || record.stream !== "turn"
    || record.resolvedWaiterId !== waiter.waiterId
    || record.bindingDigest !== waiter.bindingDigest
    || (waiter.registeredThroughOrdinal !== null
      && record.ordinal <= waiter.registeredThroughOrdinal)
    || record.event.kind !== "model_visible"
    || record.event.turnId !== binding.protocolTurnId
    || record.event.visibilityEventId !== binding.visibilityEventId
  ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
}

async function* records(context: ClaudeProcessContext): AsyncIterable<NormalizedDriverEvent> {
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
  context: ClaudeProcessContext,
  turnId: TurnId,
  waiterId: CommandId,
): AsyncIterable<NativeRuntimeEvent> {
  for await (const record of context.pump.subscribe(context.lease)) {
    if (
      record.stateInstanceId !== context.process.stateInstanceId
      || record.sessionId !== context.sessionId
      || record.readerEpoch !== context.lease.readerEpoch
    ) throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
    if (record.resolvedWaiterId !== waiterId) continue;
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
  pump: ClaudeSpawnedRuntime["pump"],
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
  host: ClaudeRuntimeHost,
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
  context: ClaudeProcessContext,
  waiter: DriverRegisteredEventWaiter,
): Promise<void> {
  try {
    await context.pump.cancelWaiter(context.lease, waiter);
  } catch {
    // Ambiguous real writes remain held; cleanup failure cannot make them retryable.
  }
}

async function cleanupWaiter(
  pump: ClaudeSpawnedRuntime["pump"],
  lease: DriverEventPumpLease,
  waiter: DriverEventWaiterSpec,
  cause: unknown,
  claim?: DriverCompositeCursorClaimHandle,
): Promise<never> {
  const failures: unknown[] = [cause];
  try {
    await pump.cancelWaiter(lease, waiter);
  } catch (error) {
    failures.push(error);
  }
  if (claim !== undefined) {
    try {
      await claim.abort();
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length > 1) throw new AggregateError(failures, "CLAUDE_DRIVER_CLEANUP_FAILED");
  throw cause;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value);
}
