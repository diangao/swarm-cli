import type {
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  LaunchId,
  LocalLaunchFence,
  NativeRuntimeEvent,
  NormalizedDriverEvent,
  ProtocolVersion,
  ReadyLaunchFence,
  ScriptedNotWrittenProof,
  SessionId,
  SpawnedLaunchFence,
  StateInstanceId,
  StopReason,
  TerminalReason,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import { DriverNormalizationError } from "./normalizer.js";
import type {
  DriverCompositeCursorClaimHandle,
  DriverEventRecord as RetainedDriverRecord,
  DriverLiveResumeAuthorization,
  DriverLiveResumeTicket,
  DriverPrivateClaimAttempt,
  DriverRetainedReplay,
  RetainedDriverEventLease,
} from "./retained-events.js";
import { DriverRetentionError } from "./retained-events.js";

export type NativeWriteBinding = {
  invocationId: CommandId;
  invocationGeneration: number;
  writeStartedEntryId: CommandId;
  writeStartedEntryDigest: ArtifactDigest;
};

export type NativeWrittenTurn = {
  kind: "written";
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
  events: AsyncIterable<NativeRuntimeEvent>;
};

export type NativeWriteOutcome =
  | NativeWrittenTurn
  | { kind: "not_written"; proof: ScriptedNotWrittenProof }
  | { kind: "ambiguous" };

export type NativeProcessWriteOutcome =
  | NativeWriteOutcome
  | { kind: "rejected_before_write"; proof: DriverPreflightProof };

export type DriverPreflightProof = {
  kind: "daemon_preflight_rejection";
  proofId: CommandId;
  requestDigest: ArtifactDigest;
  reason: "capability_absent" | "invalid_fence" | "waiter_not_registered";
  proofDigest: ArtifactDigest;
};

export type DriverProbeSpec = {
  protocolVersion: ProtocolVersion;
  runtime: "codex" | "claude" | "scripted_fake";
  executableRefPrivate: string;
  executableDigest: ArtifactDigest;
  wireProtocolDigest: ArtifactDigest;
};

export type DriverLaunchSpec = {
  launch: LocalLaunchFence;
  sessionId: SessionId;
  driverIdentity: DriverIdentity;
  transportDigest: ArtifactDigest;
  launchEnvironmentRefPrivate: string;
};

export type DriverResumeSpec = {
  launch: SpawnedLaunchFence;
  expectedSessionId: SessionId;
  runtimeSessionRefPrivate: string;
  cursorOwnerToken: ArtifactDigest;
  liveResumeAuthorization: DriverLiveResumeAuthorization;
};

export type SpawnHandle = {
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  processHandleRefPrivate: string;
  processHandleDigest: ArtifactDigest;
  transportDigest: ArtifactDigest;
};

export interface DriverCursorClaimCoordinator {
  claimAfterSpawn(input: {
    spec: DriverLaunchSpec;
    process: SpawnHandle;
    pump: DriverEventPump;
    cursorOwnerToken: ArtifactDigest;
  }): Promise<DriverCompositeCursorClaimHandle>;

  claimForReplay(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    pump: DriverEventPump;
  }): Promise<DriverCompositeCursorClaimHandle>;

  beginLiveResume(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    authorization: DriverLiveResumeAuthorization;
  }): DriverLiveResumeTicket;

  claimForLiveResume(input: {
    ticket: DriverLiveResumeTicket;
    pump: DriverEventPump;
  }): Promise<DriverCompositeCursorClaimHandle>;

  cancelLiveResume(ticket: DriverLiveResumeTicket): void;

  releaseReplayAsNoActive(
    claim: DriverCompositeCursorClaimHandle,
  ): Promise<DriverLiveResumeAuthorization>;
}

export type DriverStatus =
  | { kind: "spawning"; launch: SpawnedLaunchFence }
  | { kind: "ready"; launch: ReadyLaunchFence }
  | { kind: "running"; launch: ReadyLaunchFence; activeTurnId: TurnId }
  | { kind: "terminal"; reason: TerminalReason };

export type DriverEventPumpLease = RetainedDriverEventLease;

export type DriverEventWaiterSpec = {
  kind: "initialize" | "resume" | "turn";
  waiterId: CommandId;
  stateInstanceId: StateInstanceId;
  sessionId?: SessionId;
  turnId?: TurnId;
  bindingDigest: ArtifactDigest;
};

/** Minimal predecessor proof accepted by the generic write guard. */
export type DriverEventWaiter = DriverEventWaiterSpec & {
  registeredBeforeWrite: true;
  [proofField: string]: unknown;
};

/** Exact pump registration returned after snapshotting the durable cursor. */
export type DriverRegisteredEventWaiter = DriverEventWaiter & {
  stream: "lifecycle" | "turn";
  readerEpoch: number;
  registeredThroughOrdinal: number | null;
};

export type DriverEventRecord = RetainedDriverRecord;

export interface DriverEventPump {
  readonly stateInstanceId: StateInstanceId;
  claimCursor(input: DriverPrivateClaimAttempt): Promise<RetainedDriverEventLease>;
  releaseClaimAttempt(input: DriverPrivateClaimAttempt): Promise<void>;
  registerWaiter(
    lease: DriverEventPumpLease,
    spec: DriverEventWaiterSpec,
  ): Promise<DriverRegisteredEventWaiter>;
  cancelWaiter(
    lease: DriverEventPumpLease,
    spec: DriverEventWaiterSpec,
  ): Promise<void>;
  waitForRecord(
    lease: RetainedDriverEventLease,
    waiter: DriverRegisteredEventWaiter,
  ): Promise<DriverEventRecord>;
  subscribe(lease: DriverEventPumpLease): AsyncIterable<DriverEventRecord>;
  closeLeaseObservers(lease: RetainedDriverEventLease): Promise<void>;
}

export type DriverStartWriteWitness = {
  lease: DriverEventPumpLease;
  waiter: DriverRegisteredEventWaiter;
};

export type DriverStartRuntimeReadyRecord = DriverEventRecord & {
  bindingDigest: ArtifactDigest;
  event: Extract<NormalizedDriverEvent, { kind: "runtime_ready" }>;
};

export type DriverStartReadyBinding = DriverStartWriteWitness & {
  launch: ReadyLaunchFence;
  record: DriverStartRuntimeReadyRecord;
};

export type DriverStartPersistence<T> = {
  disposition: "applied" | "replayed";
  value: T;
};

export type DriverStartPreparationInput = {
  spec: DriverLaunchSpec;
  process: SpawnHandle;
  pump: DriverEventPump;
  cursorClaim: DriverCompositeCursorClaimHandle;
  initializeWaiterId: CommandId;
  initializeBindingDigest: ArtifactDigest;
  writeInitialize(witness: DriverStartWriteWitness): Promise<void>;
};

export type DriverStartAbortReason = "stop" | "timeout" | "controller_cancelled";

export type DriverStartAbortResult =
  | { disposition: "aborted"; reason: DriverStartAbortReason }
  | { disposition: "ready_won" };

export class DriverStartPreparation {
  readonly #spec: DriverLaunchSpec;
  readonly #process: SpawnHandle;
  readonly #pump: DriverEventPump;
  readonly #cursorClaim: DriverCompositeCursorClaimHandle;
  readonly #lease: DriverEventPumpLease;
  readonly #waiter: DriverRegisteredEventWaiter;
  readonly #waiterSpec: DriverEventWaiterSpec;
  readonly #abortSignal: Promise<void>;
  readonly #resolveAbortSignal: () => void;
  #record: DriverStartRuntimeReadyRecord | undefined;
  #bindingInFlight = false;
  #bindingPhase: "idle" | "waiting" | "persisting" = "idle";
  #bindingSettled: Promise<void> | undefined;
  #resolveBindingSettled: (() => void) | undefined;
  #sealed = false;
  #unwound = false;
  #abortRequested = false;
  #abortPromise: Promise<DriverStartAbortResult> | undefined;
  #cleanupPromise: Promise<unknown[]> | undefined;

  private constructor(
    input: DriverStartPreparationInput,
    lease: DriverEventPumpLease,
    waiter: DriverRegisteredEventWaiter,
    waiterSpec: DriverEventWaiterSpec,
  ) {
    this.#spec = input.spec;
    this.#process = input.process;
    this.#pump = input.pump;
    this.#cursorClaim = input.cursorClaim;
    this.#lease = lease;
    this.#waiter = waiter;
    this.#waiterSpec = waiterSpec;
    let resolveAbortSignal: (() => void) | undefined;
    this.#abortSignal = new Promise<void>((resolve) => {
      resolveAbortSignal = resolve;
    });
    this.#resolveAbortSignal = resolveAbortSignal!;
  }

  static async start(input: DriverStartPreparationInput): Promise<DriverStartPreparation> {
    assertStartInput(input);

    const waiterSpec: DriverEventWaiterSpec = {
      kind: "initialize",
      waiterId: input.initializeWaiterId,
      stateInstanceId: input.process.stateInstanceId,
      sessionId: input.spec.sessionId,
      bindingDigest: input.initializeBindingDigest,
    };
    const lease = input.cursorClaim.privateLease;
    let waiterRegistrationAttempted = false;

    try {
      assertLease(
        lease,
        input.process.stateInstanceId,
        input.spec.sessionId,
        input.cursorClaim.authority.ownerToken,
      );

      waiterRegistrationAttempted = true;
      const waiter = await input.pump.registerWaiter(lease, waiterSpec);
      assertWaiter(waiter, waiterSpec, lease.readerEpoch);
      await input.writeInitialize({ lease, waiter });

      return new DriverStartPreparation(input, lease, waiter, waiterSpec);
    } catch (cause) {
      await unwindAfterFailure(
        input.pump,
        input.cursorClaim,
        waiterRegistrationAttempted ? waiterSpec : undefined,
        cause,
      );
      throw cause;
    }
  }

  async bindRuntimeReady<T>(
    launch: ReadyLaunchFence,
    onBound: (binding: DriverStartReadyBinding) => Promise<DriverStartPersistence<T>>,
  ): Promise<DriverStartPersistence<T>> {
    if (this.#sealed || this.#bindingInFlight || this.#unwound || this.#abortRequested) {
      throw new DriverNormalizationError("DRIVER_EVENT_ORDER_INVALID");
    }
    this.#bindingInFlight = true;
    this.#bindingPhase = "waiting";
    this.#bindingSettled = new Promise<void>((resolve) => {
      this.#resolveBindingSettled = resolve;
    });
    try {
      if (!sameReadyLaunch(launch, this.#spec, this.#process)) {
        return await this.#failAndUnwind(
          new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH"),
        );
      }

      let record = this.#record;
      if (record === undefined) {
        const observed = await Promise.race([
          this.#pump.waitForRecord(this.#lease, this.#waiter).then(
            (value) => ({ kind: "record" as const, value }),
            (cause: unknown) => ({ kind: "error" as const, cause }),
          ),
          this.#abortSignal.then(() => ({ kind: "aborted" as const })),
        ]);
        if (observed.kind === "aborted") {
          return await this.#rejectAfterAbort();
        }
        if (observed.kind === "error") {
          if (this.#abortRequested) return await this.#rejectAfterAbort();
          return await this.#failAndUnwind(observed.cause);
        }
        try {
          record = assertRuntimeReadyRecord(
            observed.value,
            this.#process.stateInstanceId,
            this.#spec.sessionId,
            this.#lease.readerEpoch,
            this.#waiter,
          );
        } catch (cause) {
          return await this.#failAndUnwind(cause);
        }
        if (this.#abortRequested || this.#unwound) return await this.#rejectAfterAbort();
        this.#record = record;
      }

      this.#bindingPhase = "persisting";
      const persisted = await onBound({
        launch,
        record,
        lease: this.#lease,
        waiter: this.#waiter,
      });
      if (persisted.disposition !== "applied" && persisted.disposition !== "replayed") {
        throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
      }
      this.#sealed = true;
      return persisted;
    } finally {
      this.#bindingInFlight = false;
      this.#bindingPhase = "idle";
      this.#resolveBindingSettled?.();
      this.#bindingSettled = undefined;
      this.#resolveBindingSettled = undefined;
    }
  }

  abort(reason: DriverStartAbortReason): Promise<DriverStartAbortResult> {
    if (this.#abortPromise !== undefined) return this.#abortPromise;
    this.#abortRequested = true;
    this.#resolveAbortSignal();
    this.#abortPromise = this.#abortInternal(reason);
    return this.#abortPromise;
  }

  async #abortInternal(reason: DriverStartAbortReason): Promise<DriverStartAbortResult> {
    if (this.#bindingInFlight && this.#bindingPhase === "persisting") {
      await this.#bindingSettled;
    }
    if (this.#sealed) return { disposition: "ready_won" };
    this.#unwound = true;
    const cleanupErrors = await this.#cleanup();
    if (cleanupErrors.length > 0) {
      throw new AggregateError(cleanupErrors, "DRIVER_START_ABORT_FAILED");
    }
    return { disposition: "aborted", reason };
  }

  async #rejectAfterAbort(): Promise<never> {
    try {
      await this.#abortPromise;
    } catch {
      // The abort caller owns cleanup failure reporting; binding still fails closed.
    }
    throw new DriverNormalizationError("DRIVER_EVENT_ORDER_INVALID");
  }

  async #failAndUnwind(cause: unknown): Promise<never> {
    this.#abortRequested = true;
    this.#resolveAbortSignal();
    this.#unwound = true;
    const cleanupErrors = await this.#cleanup();
    return throwWithCleanup(cause, cleanupErrors, "DRIVER_START_PREPARATION_FAILED");
  }

  #cleanup(): Promise<unknown[]> {
    this.#cleanupPromise ??= cleanupWaiterAndClaim(
      this.#pump,
      this.#cursorClaim,
      this.#waiterSpec,
    );
    return this.#cleanupPromise;
  }
}

export type DriverStartedProcess = {
  process: SpawnHandle;
  preparation: DriverStartPreparation;
};

export interface NativeProcessDriver {
  probe(spec: DriverProbeSpec): Promise<DriverIdentity>;
  start(spec: DriverLaunchSpec): Promise<DriverStartedProcess>;
  resume(spec: DriverResumeSpec): Promise<SpawnHandle>;
  startTurn(
    process: SpawnHandle,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): Promise<NativeProcessWriteOutcome>;
  steerTurn(
    process: SpawnHandle,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): Promise<NativeProcessWriteOutcome>;
  interrupt(
    process: SpawnHandle,
    session: DriverSession,
    expectedTurnId: TurnId,
  ): Promise<void>;
  status(process: SpawnHandle, session?: DriverSession): Promise<DriverStatus>;
  events(process: SpawnHandle): AsyncIterable<NormalizedDriverEvent>;
  recoverEvents(input: {
    claim: DriverCompositeCursorClaimHandle;
    expectedTurnId: TurnId;
    expectedBindingDigest: ArtifactDigest;
    expectedResolvedWaiterId: CommandId;
    expectedSourceMessageId: import("@swarm/protocol").MessageId;
  }): Promise<DriverRetainedReplay>;
  stop(process: SpawnHandle, reason: StopReason): Promise<void>;
}

function assertStartInput(input: DriverStartPreparationInput): void {
  const authority = input.cursorClaim.authority;
  const lease = input.cursorClaim.privateLease;
  if (
    input.process.launchId !== input.spec.launch.launchId
    || input.process.transportDigest !== input.spec.transportDigest
    || input.pump.stateInstanceId !== input.process.stateInstanceId
    || authority.protocolVersion !== input.spec.launch.protocolVersion
    || authority.launchId !== input.spec.launch.launchId
    || authority.stateInstanceId !== input.process.stateInstanceId
    || authority.sessionId !== input.spec.sessionId
    || authority.nextOrdinal !== lease.nextOrdinal
    || authority.lastEventDigest !== lease.lastEventDigest
    || authority.ownerToken !== lease.ownerToken
    || authority.readerEpoch !== lease.readerEpoch
    || lease.replayMode !== "live"
    || lease.processMode !== "start"
  ) {
    throw new DriverRetentionError("DRIVER_START_AUTHORITY_REQUIRED");
  }
}

function assertLease(
  lease: DriverEventPumpLease,
  stateInstanceId: StateInstanceId,
  sessionId: SessionId,
  ownerToken: ArtifactDigest,
): void {
  if (
    lease.stateInstanceId !== stateInstanceId
    || lease.sessionId !== sessionId
    || lease.ownerToken !== ownerToken
    || !Number.isSafeInteger(lease.readerEpoch)
    || lease.readerEpoch < 1
  ) {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }
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
    || waiter.registeredBeforeWrite !== true
    || waiter.readerEpoch !== readerEpoch
    || waiter.stream !== (expected.kind === "turn" ? "turn" : "lifecycle")
    || (waiter.registeredThroughOrdinal !== null
      && (!Number.isSafeInteger(waiter.registeredThroughOrdinal)
        || waiter.registeredThroughOrdinal < 0))
  ) {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }
}

function assertRuntimeReadyRecord(
  record: DriverEventRecord,
  stateInstanceId: StateInstanceId,
  sessionId: SessionId,
  readerEpoch: number,
  waiter: DriverRegisteredEventWaiter,
): DriverStartRuntimeReadyRecord {
  if (
    record.stateInstanceId !== stateInstanceId
    || record.sessionId !== sessionId
    || record.readerEpoch !== readerEpoch
    || record.stream !== "lifecycle"
    || record.resolvedWaiterId !== waiter.waiterId
    || record.bindingDigest !== waiter.bindingDigest
    || !Number.isSafeInteger(record.ordinal)
    || (waiter.registeredThroughOrdinal !== null
      && record.ordinal <= waiter.registeredThroughOrdinal)
    || record.event.kind !== "runtime_ready"
  ) {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }
  return record as DriverStartRuntimeReadyRecord;
}

function sameReadyLaunch(
  launch: ReadyLaunchFence,
  spec: DriverLaunchSpec,
  process: SpawnHandle,
): boolean {
  const expected = spec.launch;
  return launch.protocolVersion === expected.protocolVersion
    && launch.agentId === expected.agentId
    && launch.machineId === expected.machineId
    && launch.launchId === expected.launchId
    && launch.routingGeneration === expected.routingGeneration
    && launch.workspaceGeneration === expected.workspaceGeneration
    && launch.stopEpoch === expected.stopEpoch
    && launch.stateInstanceId === process.stateInstanceId
    && launch.sessionId === spec.sessionId;
}

async function unwindAfterFailure(
  pump: DriverEventPump,
  claim: DriverCompositeCursorClaimHandle,
  waiterSpec: DriverEventWaiterSpec | undefined,
  cause: unknown,
): Promise<never> {
  const cleanupErrors = await cleanupWaiterAndClaim(pump, claim, waiterSpec);
  return throwWithCleanup(cause, cleanupErrors, "DRIVER_START_PREPARATION_FAILED");
}

async function cleanupWaiterAndClaim(
  pump: DriverEventPump,
  claim: DriverCompositeCursorClaimHandle,
  waiterSpec: DriverEventWaiterSpec | undefined,
): Promise<unknown[]> {
  const failures: unknown[] = [];
  if (waiterSpec !== undefined) {
    try {
      await pump.cancelWaiter(claim.privateLease, waiterSpec);
    } catch (cleanupError) {
      failures.push(cleanupError);
    }
  }
  try {
    await claim.abort();
  } catch (cleanupError) {
    failures.push(cleanupError);
  }
  return failures;
}

function throwWithCleanup(
  cause: unknown,
  cleanupErrors: readonly unknown[],
  message: string,
): never {
  if (cleanupErrors.length > 0) {
    throw new AggregateError([cause, ...cleanupErrors], message);
  }
  throw cause;
}

export interface NativeRuntimePort {
  readonly driverKind: "native_process" | "scripted_fake";
  writeTurn(turn: CompiledNativeTurn, binding: NativeWriteBinding): Promise<NativeWriteOutcome>;
}
