import { createHash } from "node:crypto";

import {
  canonicalProtocolJson,
  parseNormalizedDriverEvent,
  type ArtifactDigest,
  type CommandId,
  type LaunchId,
  type MessageId,
  type NormalizedDriverEvent,
  type ProtocolVersion,
  type SessionId,
  type StateInstanceId,
  type TurnId,
} from "@swarm/protocol";

import { DriverEventStreamNormalizer, DriverNormalizationError } from "./normalizer.js";
import type {
  DriverEventPump,
  DriverEventWaiterSpec,
  DriverRegisteredEventWaiter,
} from "./port.js";

export type DriverReplayWatermark = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  nextOrdinal: number;
  lastEventDigest: ArtifactDigest | null;
};

export type DriverCursorAuthority = DriverReplayWatermark & {
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
};

/** One-shot app authority minted only after a closed no-active recovery read. */
export type DriverLiveResumeAuthorization = Readonly<DriverReplayWatermark & {
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  ownerToken: ArtifactDigest;
  provedReaderEpoch: number;
}>;

/** One-shot ticket proving the no-active authorization was consumed before host resume. */
export type DriverLiveResumeTicket = Readonly<DriverReplayWatermark & {
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  ownerToken: ArtifactDigest;
}>;

export type DriverPrivateClaimAttempt = DriverCursorAuthority & {
  claimAttemptId: CommandId;
  processMode: "start" | "resume";
  replayMode: "live" | "retained_only";
};

export type RetainedTurnEvent = Extract<NormalizedDriverEvent, { turnId: TurnId }>;

export type DriverEventObservation = {
  stream: "turn";
  resolvedWaiterId: CommandId;
  sourceMessageId: MessageId;
  bindingDigest: ArtifactDigest;
  event: RetainedTurnEvent;
};

export type DriverLifecycleObservation = {
  stream: "lifecycle";
  resolvedWaiterId: CommandId;
  bindingDigest: ArtifactDigest;
  event: Extract<NormalizedDriverEvent, { kind: "runtime_ready" | "runtime_terminal" }>;
};

export type DriverPumpObservation = DriverEventObservation | DriverLifecycleObservation;

export type RetainedDriverEventIdentity = {
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  keyGeneration: number;
  ordinal: number;
  previousRecordDigest: ArtifactDigest | null;
  resolvedWaiterId: CommandId;
  sourceMessageId: MessageId;
  turnId: TurnId;
  bindingDigest: ArtifactDigest;
  eventKind: RetainedTurnEvent["kind"];
  payloadCipherDigest: ArtifactDigest;
  eventDigest: ArtifactDigest;
  recordDigest: ArtifactDigest;
};

export type RetainedDriverEventRecord = RetainedDriverEventIdentity & {
  stream: "turn";
  readerEpoch: number;
  event: RetainedTurnEvent;
};

export type DriverLifecycleEventRecord = {
  stream: "lifecycle";
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  readerEpoch: number;
  resolvedWaiterId: CommandId;
  ordinal: number;
  eventDigest: ArtifactDigest;
  bindingDigest: ArtifactDigest;
  event: DriverLifecycleObservation["event"];
};

export type DriverEventRecord = RetainedDriverEventRecord | DriverLifecycleEventRecord;

export type RetainedDriverEventLease = DriverCursorAuthority & {
  claimAttemptId: CommandId;
  processMode: "start" | "resume";
  replayMode: "live" | "retained_only";
  snapshotHeadNextOrdinal: number;
};

export type PreparedRetainedDriverEventEnvelope = RetainedDriverEventIdentity & {
  nonce: Uint8Array;
  authTag: Uint8Array;
  ciphertext: Uint8Array;
};

export type RetainedAppendResult = {
  applied: boolean;
  record: RetainedDriverEventRecord;
};

export type DriverDurableAck = DriverReplayWatermark;

export type DriverCompositeClaimCloseResult = {
  applied: boolean;
  storageReleased: boolean;
  privateReleased: boolean;
};

export interface DriverCompositeCursorClaimHandle {
  readonly authority: DriverCursorAuthority;
  readonly privateLease: RetainedDriverEventLease;
  abort(): Promise<DriverCompositeClaimCloseResult>;
  release(): Promise<DriverCompositeClaimCloseResult>;
}

export interface DriverPrivateEventRetentionPort {
  claim(input: DriverPrivateClaimAttempt): Promise<RetainedDriverEventLease>;
  releaseAttempt(input: DriverPrivateClaimAttempt): Promise<{ applied: boolean }>;
  prepareAppend(
    lease: RetainedDriverEventLease,
    observation: DriverEventObservation,
  ): PreparedRetainedDriverEventEnvelope;
  appendPrepared(
    lease: RetainedDriverEventLease,
    envelope: PreparedRetainedDriverEventEnvelope,
  ): RetainedAppendResult;
  read(lease: RetainedDriverEventLease, ordinal: number): RetainedDriverEventRecord | null;
  acknowledgeCommitted(
    lease: RetainedDriverEventLease,
    ack: DriverDurableAck,
  ): { applied: boolean };
}

export type DriverRetentionErrorCode =
  | "DRIVER_EVENT_FENCE_MISMATCH"
  | "DRIVER_RESUME_OVERLAP"
  | "DRIVER_RETAINED_EVENT_CONFLICT"
  | "DRIVER_RETAINED_EVENT_GAP"
  | "DRIVER_RETAINED_WATERMARK_MISMATCH"
  | "DRIVER_RETAINED_LOG_CORRUPT"
  | "DRIVER_RETAINED_VERSION_UNSUPPORTED"
  | "DRIVER_PRIVATE_BOUNDARY_VIOLATION"
  | "DRIVER_START_AUTHORITY_REQUIRED"
  | "DRIVER_CURSOR_CLEANUP_FAILED"
  | "DRIVER_OBSERVATION_SEQUENCE_CLOSED";

export class DriverRetentionError extends Error {
  readonly code: DriverRetentionErrorCode;

  constructor(code: DriverRetentionErrorCode) {
    super(code);
    this.name = "DriverRetentionError";
    this.code = code;
  }
}

export type DriverRetainedReplayExpectation = {
  expectedTurnId: TurnId;
  expectedBindingDigest: ArtifactDigest;
  expectedResolvedWaiterId: CommandId;
  expectedSourceMessageId: MessageId;
};

export interface DriverRetainedEventSource {
  openReplay(input: DriverRetainedReplayExpectation & {
    claim: DriverCompositeCursorClaimHandle;
  }): Promise<DriverRetainedReplay>;
}

export interface DriverRetainedReplay {
  readonly lease: RetainedDriverEventLease;
  readonly records: AsyncIterable<RetainedDriverEventRecord>;
  close(): Promise<void>;
}

export interface DriverObservationSequencer {
  enqueue(
    lease: RetainedDriverEventLease,
    batch: readonly DriverPumpObservation[],
  ): Promise<readonly DriverEventRecord[]>;
  close(lease: RetainedDriverEventLease): Promise<void>;
}

export type RetainedEventAad = {
  schema: "swarm.private-driver-event";
  version: 1;
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  keyGeneration: number;
  ordinal: number;
  previousRecordDigest: ArtifactDigest | null;
  resolvedWaiterId: CommandId;
  sourceMessageId: MessageId;
  turnId: TurnId;
  bindingDigest: ArtifactDigest;
  eventKind: RetainedTurnEvent["kind"];
};

export function retainedEventAad(
  identity: Omit<RetainedDriverEventIdentity, "payloadCipherDigest" | "eventDigest" | "recordDigest">,
): RetainedEventAad {
  return {
    schema: "swarm.private-driver-event",
    version: 1,
    protocolVersion: identity.protocolVersion,
    launchId: identity.launchId,
    stateInstanceId: identity.stateInstanceId,
    sessionId: identity.sessionId,
    keyGeneration: identity.keyGeneration,
    ordinal: identity.ordinal,
    previousRecordDigest: identity.previousRecordDigest,
    resolvedWaiterId: identity.resolvedWaiterId,
    sourceMessageId: identity.sourceMessageId,
    turnId: identity.turnId,
    bindingDigest: identity.bindingDigest,
    eventKind: identity.eventKind,
  };
}

export function driverArtifactDigest(value: Uint8Array | string): ArtifactDigest {
  return `sha256:${createHash("sha256").update(value).digest("hex")}` as ArtifactDigest;
}

export function retainedEventDigest(
  identity: Omit<RetainedDriverEventIdentity, "eventDigest" | "recordDigest">,
): ArtifactDigest {
  return driverArtifactDigest(canonicalProtocolJson({
    schema: "swarm.private-driver-event-digest",
    version: 1,
    protocolVersion: identity.protocolVersion,
    launchId: identity.launchId,
    stateInstanceId: identity.stateInstanceId,
    sessionId: identity.sessionId,
    keyGeneration: identity.keyGeneration,
    ordinal: identity.ordinal,
    eventKind: identity.eventKind,
    turnId: identity.turnId,
    resolvedWaiterId: identity.resolvedWaiterId,
    sourceMessageId: identity.sourceMessageId,
    bindingDigest: identity.bindingDigest,
    payloadCipherDigest: identity.payloadCipherDigest,
  }));
}

export function retainedRecordDigest(
  identity: Omit<RetainedDriverEventIdentity, "recordDigest">,
): ArtifactDigest {
  const {
    protocolVersion,
    launchId,
    stateInstanceId,
    sessionId,
    keyGeneration,
    ordinal,
    previousRecordDigest,
    resolvedWaiterId,
    sourceMessageId,
    turnId,
    bindingDigest,
    eventKind,
  } = identity;
  return driverArtifactDigest(canonicalProtocolJson({
    ...retainedEventAad({
      protocolVersion,
      launchId,
      stateInstanceId,
      sessionId,
      keyGeneration,
      ordinal,
      previousRecordDigest,
      resolvedWaiterId,
      sourceMessageId,
      turnId,
      bindingDigest,
      eventKind,
    }),
    payloadCipherDigest: identity.payloadCipherDigest,
    eventDigest: identity.eventDigest,
  }));
}

export function assertSafeOrdinal(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
  }
}

/**
 * Validates an entire retained snapshot before exposing its first element.
 * This deliberately materializes the bounded snapshot so corruption can never
 * partially mutate a recovering consumer.
 */
export function validateRetainedSnapshot(
  lease: RetainedDriverEventLease,
  records: readonly RetainedDriverEventRecord[],
  expectation: DriverRetainedReplayExpectation,
): readonly RetainedDriverEventRecord[] {
  let ordinal = lease.nextOrdinal;
  let previousEventDigest = lease.lastEventDigest;
  let previousRecordDigest: ArtifactDigest | null = null;
  const normalizer = DriverEventStreamNormalizer.fromDurableModelVisible(expectation);
  for (const record of records) {
    assertSafeOrdinal(record.ordinal);
    if (
      record.stream !== "turn"
      || record.protocolVersion !== lease.protocolVersion
      || record.launchId !== lease.launchId
      || record.stateInstanceId !== lease.stateInstanceId
      || record.sessionId !== lease.sessionId
      || record.readerEpoch !== lease.readerEpoch
      || record.ordinal !== ordinal
      || record.turnId !== expectation.expectedTurnId
      || record.bindingDigest !== expectation.expectedBindingDigest
      || record.resolvedWaiterId !== expectation.expectedResolvedWaiterId
      || record.sourceMessageId !== expectation.expectedSourceMessageId
      || record.event.turnId !== expectation.expectedTurnId
      || record.event.kind !== record.eventKind
      || (ordinal === lease.nextOrdinal
        ? (lease.nextOrdinal === 0 ? record.previousRecordDigest !== null : false)
        : record.previousRecordDigest !== previousRecordDigest)
    ) {
      throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
    }
    if (ordinal === lease.nextOrdinal && lease.nextOrdinal > 0 && previousEventDigest === null) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
    const parsed = parseNormalizedDriverEvent(
      canonicalProtocolJson(record.event),
      lease.protocolVersion,
    );
    normalizer.accept(parsed);
    previousEventDigest = record.eventDigest;
    previousRecordDigest = record.recordDigest;
    ordinal += 1;
  }
  normalizer.finish();
  if (ordinal !== lease.snapshotHeadNextOrdinal) {
    throw new DriverRetentionError("DRIVER_RETAINED_EVENT_GAP");
  }
  return records;
}

export class FiniteDriverRetainedReplay implements DriverRetainedReplay {
  readonly lease: RetainedDriverEventLease;
  readonly records: AsyncIterable<RetainedDriverEventRecord>;
  readonly #claim: DriverCompositeCursorClaimHandle;
  #closePromise: Promise<void> | undefined;

  constructor(
    claim: DriverCompositeCursorClaimHandle,
    records: readonly RetainedDriverEventRecord[],
    expectation: DriverRetainedReplayExpectation,
  ) {
    this.#claim = claim;
    this.lease = claim.privateLease;
    const validated = validateRetainedSnapshot(this.lease, records, expectation);
    this.records = finite(validated);
  }

  close(): Promise<void> {
    this.#closePromise ??= this.#claim.release().then(() => undefined);
    return this.#closePromise;
  }
}

async function* finite(
  records: readonly RetainedDriverEventRecord[],
): AsyncIterable<RetainedDriverEventRecord> {
  for (const record of records) yield record;
}

type PumpWait = {
  waiter: DriverRegisteredEventWaiter;
  resolve(record: DriverEventRecord): void;
  reject(error: unknown): void;
};

type PumpSubscriber = {
  queue: DriverEventRecord[];
  wake: (() => void) | undefined;
  closed: boolean;
};

type PumpLeaseState = {
  lease: RetainedDriverEventLease;
  waiters: Map<CommandId, DriverRegisteredEventWaiter>;
  pending: Map<CommandId, PumpWait>;
  records: DriverEventRecord[];
  subscribers: Set<PumpSubscriber>;
  lifecycleNextOrdinal: number;
  tail: Promise<void>;
  nextTicket: number;
  closed: boolean;
};

/**
 * Authority-bound pump used by the concrete child hosts. Its one promise tail
 * is the per-lease FIFO: correlation, private append, and notification all
 * finish before the next ingress ticket begins.
 */
export class RetainedDriverEventPump implements DriverEventPump, DriverObservationSequencer {
  readonly stateInstanceId: StateInstanceId;
  readonly #retention: DriverPrivateEventRetentionPort;
  readonly #states = new Map<CommandId, PumpLeaseState>();

  constructor(stateInstanceId: StateInstanceId, retention: DriverPrivateEventRetentionPort) {
    this.stateInstanceId = stateInstanceId;
    this.#retention = retention;
  }

  async claimCursor(input: DriverPrivateClaimAttempt): Promise<RetainedDriverEventLease> {
    if (input.stateInstanceId !== this.stateInstanceId) this.#fence();
    const lease = await this.#retention.claim(input);
    const existing = this.#states.get(lease.claimAttemptId);
    if (existing !== undefined) {
      if (!sameLease(existing.lease, lease)) this.#fence();
      return existing.lease;
    }
    const retained: DriverEventRecord[] = [];
    if (lease.replayMode === "live") {
      for (let ordinal = lease.nextOrdinal; ordinal < lease.snapshotHeadNextOrdinal; ordinal += 1) {
        const record = this.#retention.read(lease, ordinal);
        if (record === null) throw new DriverRetentionError("DRIVER_RETAINED_EVENT_GAP");
        retained.push(record);
      }
    }
    this.#states.set(lease.claimAttemptId, {
      lease,
      waiters: new Map(),
      pending: new Map(),
      records: retained,
      subscribers: new Set(),
      lifecycleNextOrdinal: 0,
      tail: Promise.resolve(),
      nextTicket: 0,
      closed: false,
    });
    return lease;
  }

  async releaseClaimAttempt(input: DriverPrivateClaimAttempt): Promise<void> {
    await this.#retention.releaseAttempt(input);
    this.#states.delete(input.claimAttemptId);
  }

  async registerWaiter(
    lease: RetainedDriverEventLease,
    spec: DriverEventWaiterSpec,
  ): Promise<DriverRegisteredEventWaiter> {
    const state = this.#state(lease);
    if (state.closed || state.waiters.has(spec.waiterId)) this.#fence();
    if (
      spec.stateInstanceId !== lease.stateInstanceId
      || spec.sessionId !== lease.sessionId
      || (spec.kind === "turn") !== (spec.turnId !== undefined)
    ) this.#fence();
    const stream = spec.kind === "turn" ? "turn" : "lifecycle";
    const currentTurnHeadNextOrdinal = state.records.reduce(
      (next, record) => record.stream === "turn" ? Math.max(next, record.ordinal + 1) : next,
      lease.snapshotHeadNextOrdinal,
    );
    const registeredThroughOrdinal = stream === "turn"
      ? (currentTurnHeadNextOrdinal === 0 ? null : currentTurnHeadNextOrdinal - 1)
      : (state.lifecycleNextOrdinal === 0 ? null : state.lifecycleNextOrdinal - 1);
    const waiter: DriverRegisteredEventWaiter = {
      ...spec,
      stream,
      readerEpoch: lease.readerEpoch,
      registeredBeforeWrite: true,
      registeredThroughOrdinal,
    };
    state.waiters.set(spec.waiterId, waiter);
    return waiter;
  }

  async cancelWaiter(
    lease: RetainedDriverEventLease,
    spec: DriverEventWaiterSpec,
  ): Promise<void> {
    const state = this.#state(lease);
    const existing = state.waiters.get(spec.waiterId);
    if (existing !== undefined && !sameWaiterSpec(existing, spec)) this.#fence();
    state.waiters.delete(spec.waiterId);
    const pending = state.pending.get(spec.waiterId);
    if (pending !== undefined) {
      state.pending.delete(spec.waiterId);
      pending.reject(new DriverRetentionError("DRIVER_OBSERVATION_SEQUENCE_CLOSED"));
    }
  }

  waitForRecord(
    lease: RetainedDriverEventLease,
    waiter: DriverRegisteredEventWaiter,
  ): Promise<DriverEventRecord> {
    const state = this.#state(lease);
    const registered = state.waiters.get(waiter.waiterId);
    if (registered === undefined || !sameRegisteredWaiter(registered, waiter)) this.#fence();
    const existing = state.records.find((record) => resolves(waiter, record));
    if (existing !== undefined) return Promise.resolve(existing);
    if (state.closed) {
      return Promise.reject(new DriverRetentionError("DRIVER_OBSERVATION_SEQUENCE_CLOSED"));
    }
    if (state.pending.has(waiter.waiterId)) this.#fence();
    return new Promise<DriverEventRecord>((resolve, reject) => {
      state.pending.set(waiter.waiterId, { waiter, resolve, reject });
    });
  }

  subscribe(lease: RetainedDriverEventLease): AsyncIterable<DriverEventRecord> {
    const state = this.#state(lease);
    if (state.closed) {
      throw new DriverRetentionError("DRIVER_OBSERVATION_SEQUENCE_CLOSED");
    }
    if (state.subscribers.size !== 0) this.#fence();
    const subscriber: PumpSubscriber = { queue: [...state.records], wake: undefined, closed: false };
    state.subscribers.add(subscriber);
    return subscription(state, subscriber);
  }

  enqueue(
    lease: RetainedDriverEventLease,
    batch: readonly DriverPumpObservation[],
  ): Promise<readonly DriverEventRecord[]> {
    const state = this.#state(lease);
    if (state.closed) {
      return Promise.reject(new DriverRetentionError("DRIVER_OBSERVATION_SEQUENCE_CLOSED"));
    }
    const ticket = state.nextTicket;
    state.nextTicket += 1;
    let resolveResult: ((records: readonly DriverEventRecord[]) => void) | undefined;
    let rejectResult: ((error: unknown) => void) | undefined;
    const result = new Promise<readonly DriverEventRecord[]>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    state.tail = state.tail.then(async () => {
      if (state.closed) throw new DriverRetentionError("DRIVER_OBSERVATION_SEQUENCE_CLOSED");
      const committed: DriverEventRecord[] = [];
      for (const observation of batch) {
        const waiter = state.waiters.get(observation.resolvedWaiterId);
        if (
          waiter === undefined
          || waiter.bindingDigest !== observation.bindingDigest
          || waiter.stream !== observation.stream
          || (observation.stream === "turn" && waiter.turnId !== observation.event.turnId)
        ) this.#fence();
        let record: DriverEventRecord;
        if (observation.stream === "turn") {
          const prepared = this.#retention.prepareAppend(lease, observation);
          record = this.#retention.appendPrepared(lease, prepared).record;
        } else {
          record = {
            stream: "lifecycle",
            stateInstanceId: lease.stateInstanceId,
            sessionId: lease.sessionId,
            readerEpoch: lease.readerEpoch,
            resolvedWaiterId: observation.resolvedWaiterId,
            ordinal: state.lifecycleNextOrdinal,
            eventDigest: driverArtifactDigest(canonicalProtocolJson({
              stream: "lifecycle",
              stateInstanceId: lease.stateInstanceId,
              sessionId: lease.sessionId,
              ordinal: state.lifecycleNextOrdinal,
              bindingDigest: observation.bindingDigest,
              event: observation.event,
            })),
            bindingDigest: observation.bindingDigest,
            event: observation.event,
          };
          state.lifecycleNextOrdinal += 1;
        }
        committed.push(record);
        state.records.push(record);
        this.#publish(state, record);
      }
      resolveResult!(committed);
    }).catch((error: unknown) => {
      rejectResult!(error);
    });
    void ticket;
    return result;
  }

  async closeLeaseObservers(lease: RetainedDriverEventLease): Promise<void> {
    const state = this.#state(lease);
    if (state.closed) return;
    const barrier = state.tail.then(() => {
      state.closed = true;
      const error = new DriverRetentionError("DRIVER_OBSERVATION_SEQUENCE_CLOSED");
      for (const pending of state.pending.values()) pending.reject(error);
      state.pending.clear();
      state.waiters.clear();
      for (const subscriber of state.subscribers) {
        subscriber.closed = true;
        subscriber.wake?.();
      }
    });
    state.tail = barrier;
    await barrier;
  }

  leaseForWaiter(waiter: DriverRegisteredEventWaiter): RetainedDriverEventLease {
    for (const state of this.#states.values()) {
      const registered = state.waiters.get(waiter.waiterId);
      if (registered !== undefined && sameRegisteredWaiter(registered, waiter)) {
        return state.lease;
      }
    }
    this.#fence();
  }

  close(lease: RetainedDriverEventLease): Promise<void> {
    return this.closeLeaseObservers(lease);
  }

  #publish(state: PumpLeaseState, record: DriverEventRecord): void {
    const pending = state.pending.get(record.resolvedWaiterId);
    if (pending !== undefined && resolves(pending.waiter, record)) {
      state.pending.delete(record.resolvedWaiterId);
      pending.resolve(record);
    }
    for (const subscriber of state.subscribers) {
      subscriber.queue.push(record);
      subscriber.wake?.();
      subscriber.wake = undefined;
    }
  }

  #state(lease: RetainedDriverEventLease): PumpLeaseState {
    if (lease.stateInstanceId !== this.stateInstanceId) this.#fence();
    const state = this.#states.get(lease.claimAttemptId);
    if (state === undefined || !sameLease(state.lease, lease)) this.#fence();
    return state;
  }

  #fence(): never {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }
}

async function* subscription(
  state: PumpLeaseState,
  subscriber: PumpSubscriber,
): AsyncIterable<DriverEventRecord> {
  try {
    while (true) {
      const record = subscriber.queue.shift();
      if (record !== undefined) {
        yield record;
        continue;
      }
      if (subscriber.closed) return;
      await new Promise<void>((resolve) => {
        subscriber.wake = resolve;
      });
    }
  } finally {
    state.subscribers.delete(subscriber);
  }
}

function resolves(waiter: DriverRegisteredEventWaiter, record: DriverEventRecord): boolean {
  return waiter.waiterId === record.resolvedWaiterId
    && waiter.stream === record.stream
    && (waiter.kind === "turn"
      ? record.event.kind === "model_visible"
      : record.event.kind === "runtime_ready")
    && (waiter.registeredThroughOrdinal === null
      || record.ordinal > waiter.registeredThroughOrdinal);
}

function sameWaiterSpec(waiter: DriverRegisteredEventWaiter, spec: DriverEventWaiterSpec): boolean {
  return waiter.kind === spec.kind
    && waiter.waiterId === spec.waiterId
    && waiter.stateInstanceId === spec.stateInstanceId
    && waiter.sessionId === spec.sessionId
    && waiter.turnId === spec.turnId
    && waiter.bindingDigest === spec.bindingDigest;
}

function sameRegisteredWaiter(
  left: DriverRegisteredEventWaiter,
  right: DriverRegisteredEventWaiter,
): boolean {
  return sameWaiterSpec(left, right)
    && left.readerEpoch === right.readerEpoch
    && left.stream === right.stream
    && left.registeredThroughOrdinal === right.registeredThroughOrdinal
    && right.registeredBeforeWrite === true;
}

function sameLease(left: RetainedDriverEventLease, right: RetainedDriverEventLease): boolean {
  return left.protocolVersion === right.protocolVersion
    && left.launchId === right.launchId
    && left.stateInstanceId === right.stateInstanceId
    && left.sessionId === right.sessionId
    && left.ownerToken === right.ownerToken
    && left.readerEpoch === right.readerEpoch
    && left.nextOrdinal === right.nextOrdinal
    && left.lastEventDigest === right.lastEventDigest
    && left.claimAttemptId === right.claimAttemptId
    && left.processMode === right.processMode
    && left.replayMode === right.replayMode
    && left.snapshotHeadNextOrdinal === right.snapshotHeadNextOrdinal;
}
