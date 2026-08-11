import type {
  ArtifactDigest,
  CommandId,
  DeliveryFence,
  DeliveryId,
  DriverInputMode,
  DriverTurnBinding,
  LaunchId,
  LocalLaunchFence,
  MessageId,
  NativeInvocationFence,
  NormalizedDriverEvent,
  ProducerFactId,
  ProtocolVersion,
  SessionId,
  StateInstanceId,
  TurnCompletionEvidence,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import type { NativeProcessWriteOutcome } from "@swarm/drivers";

// ===========================================================================
// AtomicTurnJournalPort — a RE-DECLARED, wire-identical mirror of the promoted
// storage runtime-journal turn contract. daemon-core MUST NOT import from
// @swarm/storage (package-boundary policy), so the types are re-declared here
// field-by-field / return-type-by-return-type identical to the current promoted
// contract. TurnCompletionEvidence and the branded primitives are imported from
// @swarm/protocol exactly as the storage journal does. `LocalTurnState` is the
// storage-local eight-state string union — re-declared here (the same shape)
// because @swarm/protocol does not export it.
// ===========================================================================

/** The storage-local eight-state turn phase union. */
export type LocalTurnState =
  | "queued"
  | "write_started"
  | "input_written"
  | "model_visible"
  | "completed"
  | "ambiguous"
  | "interrupted"
  | "terminal_error";

/** The four ordered non-terminal driver-event step kinds. */
export type TurnStepKind =
  | "turn_started"
  | "input_written"
  | "model_visible"
  | "turn_boundary";

/** The three durable settle kinds for an aborted/held contribution. */
export type TurnSettleKind = "terminal_error" | "interrupted" | "ambiguous";

/** The durable per-turn state projection the CAS boundary compares. */
export type DurableTurnState = {
  protocolTurnId: TurnId;
  phase: LocalTurnState;
  inputOrdinal: number;
  bindingDigest: ArtifactDigest | null;
  steerable: boolean;
  replyCommitted: boolean;
};

/** One recorded driver-event row: ordinal, event digest, turn, binding fence. */
export type TurnEventRecordInput = {
  ordinal: number;
  eventDigest: ArtifactDigest;
  turnId: TurnId;
  bindingDigest: ArtifactDigest;
};

/** The reply/coordination?/completed leg basis of the single terminal commit. */
export type TurnTerminalCommitBasis = {
  reply: TurnEventRecordInput & { replyCommandId: CommandId };
  coordination?: TurnEventRecordInput & {
    commandId: CommandId;
    commandDigest: ArtifactDigest;
  };
  completed: TurnEventRecordInput;
};

/** Admission mode: a fresh ordinary contribution or an expected-turn steer. */
export type TurnAdmissionMode =
  | { kind: "ordinary" }
  | { kind: "steer"; expectedTurnId: TurnId };

/** The reader-lock fence carried on every journal call (CAS'd on the cursor). */
export type TurnReaderFence = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
};

/**
 * The admission input. Carries the ordered seven-member durable admission
 * preimage (`deliveryId`, `attempt`, `invocationId`, `invocationGeneration`,
 * `permitId`, `runtimeWriteId`, `visibilityEventId`) the promoted admission
 * brand-asserts, pairwise-distinctness-checks, joins against the stored
 * delivery/attempt fence + authenticated source, and re-binds via the sole
 * SSOT contribution-binding recompute — all before any mutation.
 */
export type BeginTurnContributionInput = TurnReaderFence & {
  protocolTurnId: TurnId;
  launchId: LaunchId;
  rootProducerFactId: ProducerFactId;
  inputOrdinal: number;
  driverTurnRefDigest: ArtifactDigest;
  mode: TurnAdmissionMode;
  bindingDigest: ArtifactDigest;
  deliveryId: DeliveryId;
  attempt: number;
  invocationId: CommandId;
  invocationGeneration: number;
  permitId: CommandId;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
  expected: DurableTurnState | null;
  next: DurableTurnState;
  recordedAt: string;
};

/** A non-terminal cursor step commit: durable + cursor advance atomically. */
export type CommitTurnStepInput = TurnReaderFence & {
  event: TurnEventRecordInput;
  kind: TurnStepKind;
  expected: DurableTurnState;
  next: DurableTurnState;
  recordedAt: string;
};

/** The single evidence-verified terminal commit (reply/coordination?/completed). */
export type CommitTurnTerminalInput = TurnReaderFence & {
  basis: TurnTerminalCommitBasis;
  evidence: TurnCompletionEvidence;
  expected: DurableTurnState;
  next: DurableTurnState;
  recordedAt: string;
};

/** A durable settle of an aborted/held contribution (terminal_error/interrupted/ambiguous). */
export type SettleTurnContributionInput = TurnReaderFence & {
  protocolTurnId: TurnId;
  inputOrdinal: number;
  kind: TurnSettleKind;
  expected: DurableTurnState;
  next: DurableTurnState;
  recordedAt: string;
};

/** The mutation result: whether it applied and the resulting durable projection. */
export type TurnMutationResult = { applied: boolean; durable: DurableTurnState };
/**
 * A step/terminal result: the mutation result plus the authoritative post-commit
 * cursor pair (`nextOrdinal` + `lastEventDigest`), so a consumer can ACK the exact
 * current cursor rather than reconstructing it.
 */
export type TurnStepResult = TurnMutationResult & {
  nextOrdinal: number;
  lastEventDigest: ArtifactDigest | null;
};

/** The exact input type of readDurableTurnState. */
export type ReadDurableTurnStateInput = TurnReaderFence & { protocolTurnId: TurnId };

/**
 * The five durable turn methods the machine drives, with signatures identical to
 * the promoted storage journal turn methods: same inputs, same return types, same
 * thrown codes. Full-state CAS lives on both sides of the commit boundary; an
 * exact operationDigest replay aliases `applied:false` with no double effect.
 */
export interface AtomicTurnJournalPort {
  beginTurnContribution(input: BeginTurnContributionInput): TurnMutationResult;
  commitTurnStep(input: CommitTurnStepInput): TurnStepResult;
  commitTurnTerminal(input: CommitTurnTerminalInput): TurnStepResult;
  settleTurnContribution(input: SettleTurnContributionInput): TurnMutationResult;
  readDurableTurnState(input: ReadDurableTurnStateInput): DurableTurnState | null;
}

// ===========================================================================
// Driver-event reader cursor claim. The machine claims the durable driver-event
// reader before any event may be applied; the claim returns the lock-owning
// reader epoch together with the authoritative cursor pair it is anchored to.
// The readerEpoch is carried on the TurnReaderFence of every subsequent journal
// call (matching the cursor fence CAS on the storage side). Orphan/takeover
// claiming is NOT part of this ordinary claim seam — it lives in the promoted
// recovery coordinator.
// ===========================================================================

/** The authoritative durable cursor pair anchored to the reader lock. */
export type DriverEventCursorSnapshot = {
  nextOrdinal: number;
  lastEventDigest: ArtifactDigest | null;
};

/** The ordinary-claim result: the cursor pair plus the granted reader epoch. */
export type DriverEventReaderClaimResult = DriverEventCursorSnapshot & {
  readerEpoch: number;
};

export type DriverEventReaderClaim = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ownerToken: ArtifactDigest;
  mode: "start" | "resume";
  claimedAt: string;
};

export interface DriverEventReaderPort {
  claimDriverEventReader(input: DriverEventReaderClaim): DriverEventReaderClaimResult;
}

// ===========================================================================
// Native driver adapter port — the exact NativeProcessDriver write/steer/
// interrupt shapes. startTurn/steerTurn return the 4-way NativeProcessWriteOutcome
// union. This adapter ALSO owns the durable invocation-entry chain
// (permit_recorded -> write_started -> input_written -> model_visible) that
// #requireContributionJoin re-joins at terminal: the four pairwise-distinct
// command ids (invocationId, permitId, runtimeWriteId, visibilityEventId) carried
// in the DriverTurnBinding are echoed back on a `written` outcome and end up
// stored as the entry-chain runtimeWriteId / visibilityEventId (contribution-join
// invariant).
// ===========================================================================

export interface TurnDriverPort {
  startTurn(
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): Promise<NativeProcessWriteOutcome>;
  steerTurn(
    input: CompiledNativeTurn,
    binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): Promise<NativeProcessWriteOutcome>;
  interrupt(expectedTurnId: TurnId): Promise<void>;
}

// ===========================================================================
// Supporting request / id ports.
// ===========================================================================

/**
 * This module's OWN native-id minting for the four pairwise-distinct ids
 * (invocationId, permitId is caller-supplied, runtimeWriteId, visibilityEventId).
 * This is the ONLY id source the machine consumes; it never mints turn identity
 * (== delivery.turnId), only per-contribution runtime receipts.
 *
 * NOTE: this module deliberately does NOT declare or consume Lane D's reserved
 * `DeliveryCommandIdDerivationPort` reply seam. The reply commandId is derived by
 * Lane D from this module's command-id-free terminal draft basis (see machine.ts
 * TurnTerminalDraftBasis); this module does not expose that Lane-D port here.
 */
export interface TurnCommandIdSource {
  nextCommandId(): CommandId;
  now(): string;
}

/**
 * A caller submission. `mode.kind === "ordinary"` submits a fresh delivery;
 * `mode.kind === "steer"` is the ONLY steer entry point and carries the
 * caller's own expectedTurnId. A plain ordinary submit while a turn is active
 * QUEUES (never auto-steers). The logical turn identity is `delivery.turnId`
 * end-to-end; it is NEVER minted by the machine.
 */
export type TurnSubmission = {
  delivery: DriverTurnBinding["delivery"];
  compiled: CompiledNativeTurn;
  rootProducerFactId: ProducerFactId;
  driverTurnRefDigest: ArtifactDigest;
  inputDigest: ArtifactDigest;
  permitId: CommandId;
  mode: DriverInputMode;
};

/**
 * One applied driver event with its cursor ordinal + event digest. `bindingDigest`
 * is REQUIRED: it is the per-contribution fence the source producer stamps on the
 * event, and the promoted commitTurnStep records it on the
 * driver event row. The machine uses `input.bindingDigest` DIRECTLY (no fallback
 * to the active contribution's binding), so an old-producer event carrying a
 * stale binding is rejected (WRITE_STARTED_BINDING_MISMATCH) rather than being
 * silently reattributed to the current contribution.
 */
export type TurnEventInput = {
  ordinal: number;
  eventDigest: ArtifactDigest;
  bindingDigest: ArtifactDigest;
  event: NormalizedDriverEvent;
};

// Re-export protocol handles the machine consumes so downstream lanes need not
// re-derive them from @swarm/protocol.
export type { NativeInvocationFence, LocalLaunchFence, DeliveryFence };

// ===========================================================================
// FRESH-PROCESS RECOVERY PORTS (composition of the durable turn-recovery read +
// the retained finite replay).
//
// On a fresh process (daemon restart mid-turn) the machine reconstructs its
// active contribution HONESTLY from durable + retained sources, never a
// machine-local invention. daemon-core MUST NOT import @swarm/storage or
// @swarm/drivers/apps for these seams (package-boundary policy re-declares the
// promoted contracts field-by-field, exactly as AtomicTurnJournalPort above
// re-declares the storage journal). The app layer wires the concrete
// readTurnRecovery / retained-replay / live-resume implementations. Every type
// here is a wire-identical mirror of the promoted shape it is annotated with.
// ===========================================================================

// -- durable turn-recovery read (packages/storage/src/sqlite/runtime-journal.ts) --

/**
 * Mirrors runtime-journal.ts DriverEventCursorSnapshot (line 247-250) EXACTLY.
 * The authoritative cursor pair the recovery read returns on the `replayable`
 * arm; the machine sets its cursor from this and cross-checks the composite
 * claim watermark against it (fail-closed on divergence).
 */
export type TurnRecoveryCursorSnapshot = {
  nextOrdinal: number;
  lastEventDigest: ArtifactDigest | null;
};

/**
 * Mirrors runtime-journal.ts ReplayableTurnRecoveryBasis (line 393-415) EXACTLY,
 * field-by-field. This is the ONLY arm from which #active is restored: a fully
 * admitted, model_visible contribution whose entry chain reached depth 4. All
 * seven admission ids are non-null (the storage read fails closed otherwise);
 * `durable`/`attemptState` are the literal "model_visible"; `entryChainDepth` is
 * the literal 4. `sourceMessageId` is the storage-authenticated source message.
 */
export type ReplayableTurnRecoveryBasis = {
  fence: DeliveryFence;
  deliveryId: DeliveryId;
  attempt: number;
  sourceMessageId: MessageId;
  protocolTurnId: TurnId;
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  rootProducerFactId: ProducerFactId;
  inputOrdinal: number;
  driverTurnRefDigest: ArtifactDigest;
  mode: TurnAdmissionMode;
  invocationId: CommandId;
  invocationGeneration: number;
  permitId: CommandId;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
  bindingDigest: ArtifactDigest;
  durable: "model_visible";
  attemptState: "model_visible";
  entryChainDepth: 4;
};

/**
 * Mirrors runtime-journal.ts TurnRecoveryReadResult (line 417-428) EXACTLY. The
 * closed union has a `replayable` arm (basis + cursor pair) and a `held_ambiguous`
 * arm carrying the two promoted reasons. The read method itself returns
 * `TurnRecoveryReadResult | null`; the `null` arm is the no-active/no-admitted-
 * candidate path (runtime-journal.ts line 2691).
 */
export type TurnRecoveryReadResult =
  | {
      kind: "replayable";
      basis: ReplayableTurnRecoveryBasis;
      cursor: TurnRecoveryCursorSnapshot;
    }
  | {
      kind: "held_ambiguous";
      reason: "PRE_MODEL_VISIBLE_EFFECT_UNKNOWN" | "ACTIVE_AMBIGUOUS";
    };

/**
 * The abstract recovery port the app wires to the concrete
 * `RuntimeJournalTransaction.readTurnRecovery` (runtime-journal.ts line 2656).
 * The input is the exact TurnReaderFence carried on every journal call; the
 * result is the closed union OR `null` (no admitted active contribution).
 */
export interface TurnRecoveryReadPort {
  readTurnRecovery(fence: TurnReaderFence): TurnRecoveryReadResult | null;
}

// -- retained finite replay from the exact watermark (drivers) ----------------

/**
 * Mirrors drivers/src/retained-events.ts DriverReplayWatermark (line 24-29)
 * EXACTLY. The exact watermark the retained suffix replays FROM: the durable
 * cursor pair under the state-instance / session fence.
 */
export type RetainedReplayWatermark = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  nextOrdinal: number;
  lastEventDigest: ArtifactDigest | null;
};

/**
 * Mirrors drivers/src/retained-events.ts RetainedDriverEventLease (line 116-121)
 * EXACTLY (DriverCursorAuthority[line 31-36] + DriverReplayWatermark + the claim
 * fields). The lease the retained replay is bound to; the suffix begins at
 * `nextOrdinal` and ends at `snapshotHeadNextOrdinal` (finite, bounded).
 */
export type RetainedEventLease = RetainedReplayWatermark & {
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
  claimAttemptId: CommandId;
  processMode: "start" | "resume";
  replayMode: "live" | "retained_only";
  snapshotHeadNextOrdinal: number;
};

/**
 * Mirrors drivers/src/retained-events.ts RetainedTurnEvent (line 59): a
 * turn-referencing normalized driver event (the only kind retained on the turn
 * stream). Structurally the Extract<NormalizedDriverEvent, { turnId: TurnId }>.
 */
export type RetainedTurnEvent = Extract<NormalizedDriverEvent, { turnId: TurnId }>;

/**
 * Mirrors drivers/src/retained-events.ts RetainedDriverEventRecord
 * (RetainedDriverEventIdentity[line 78-94] + the stream/readerEpoch/event tail,
 * line 96-100) EXACTLY, field-by-field. One retained record of the turn suffix,
 * carrying its full identity fences (waiter/source/binding, ordinal, digests)
 * and the normalized event itself. No provider payload is exposed in the clear
 * beyond the normalized event (the ciphered payload lives behind
 * `payloadCipherDigest`).
 */
export type RetainedTurnEventRecord = {
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
  stream: "turn";
  readerEpoch: number;
  event: RetainedTurnEvent;
};

/**
 * Mirrors drivers/src/retained-events.ts DriverRetainedReplayExpectation
 * (line 190-195) EXACTLY. The four fences every retained record MUST match
 * (validated against the recovered replayable basis): turn, per-contribution
 * binding, the resolved waiter (== invocationId), and the authenticated source
 * message. A record that mismatches any fence is rejected fail-closed.
 */
export type RetainedReplayExpectation = {
  expectedTurnId: TurnId;
  expectedBindingDigest: ArtifactDigest;
  expectedResolvedWaiterId: CommandId;
  expectedSourceMessageId: MessageId;
};

/**
 * Mirrors drivers/src/retained-events.ts DriverRetainedReplay (line 203-207)
 * EXACTLY: the finite, watermark-anchored suffix. `records` is a FINITE async
 * iterable validated whole before its first element is exposed (no partial
 * mutation of a recovering consumer); `close()` releases the composite claim.
 * The lease carries the exact watermark the suffix begins at.
 */
export interface RetainedTurnReplay {
  readonly lease: RetainedEventLease;
  readonly records: AsyncIterable<RetainedTurnEventRecord>;
  close(): Promise<void>;
}

/**
 * The abstract retained-suffix source the app wires to the concrete
 * `NativeProcessDriver.recoverEvents` (drivers/src/port.ts line 449) /
 * `DriverRetainedEventSource.openReplay` (retained-events.ts line 197). It
 * opens a FINITE replay of the retained turn suffix from the exact watermark
 * under the given claim handle, with NO provider re-execution and NO
 * host/concrete-driver dependency. The expectation fences bind the suffix to
 * the recovered contribution.
 */
export interface RetainedTurnEventSource {
  openReplay(
    input: RetainedReplayExpectation & { claim: RecoveryCompositeCursorClaim },
  ): Promise<RetainedTurnReplay>;
}

// -- composite cursor claim + no-active live-resume handoff ------------------

/**
 * Mirrors drivers/src/retained-events.ts DriverCursorAuthority (line 31-36)
 * EXACTLY. The storage-side cursor authority carried on the composite claim: the
 * watermark plus the protocol/launch/owner/epoch fences.
 */
export type RecoveryCursorAuthority = RetainedReplayWatermark & {
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
};

/**
 * Mirrors drivers/src/retained-events.ts DriverCompositeClaimCloseResult
 * (line 136-140) EXACTLY. The close/abort outcome: whether each of the two
 * boundaries (storage cursor + private retained lease) was released.
 */
export type RecoveryCompositeClaimCloseResult = {
  applied: boolean;
  storageReleased: boolean;
  privateReleased: boolean;
};

/**
 * Mirrors drivers/src/retained-events.ts DriverCompositeCursorClaimHandle
 * (line 142-147) EXACTLY. The composite (storage cursor + private retained
 * lease) claim handle. `abort()` is the fail-closed, effect-free teardown of
 * BOTH boundaries (used on every null/held/error/cross-fence path);
 * `release()` is the clean teardown after a successful replay.
 */
export interface RecoveryCompositeCursorClaim {
  readonly authority: RecoveryCursorAuthority;
  readonly privateLease: RetainedEventLease;
  abort(): Promise<RecoveryCompositeClaimCloseResult>;
  release(): Promise<RecoveryCompositeClaimCloseResult>;
}

/**
 * Mirrors drivers/src/retained-events.ts DriverLiveResumeAuthorization
 * (line 39-44) EXACTLY. The one-shot app authority minted ONLY after a closed
 * no-active recovery read (readTurnRecovery === null). `provedReaderEpoch` binds
 * it to the aborted retained_only claim's epoch; it is a Readonly (frozen) value.
 */
export type RecoveryLiveResumeAuthorization = Readonly<
  RetainedReplayWatermark & {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    ownerToken: ArtifactDigest;
    provedReaderEpoch: number;
  }
>;

/**
 * Mirrors drivers/src/retained-events.ts DriverLiveResumeTicket (line 47-51)
 * EXACTLY. The one-shot ticket proving the no-active authorization was consumed
 * before host resume; a Readonly (frozen) value carrying the watermark + fences.
 */
export type RecoveryLiveResumeTicket = Readonly<
  RetainedReplayWatermark & {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    ownerToken: ArtifactDigest;
  }
>;

/**
 * The abstract no-active live-resume port the app wires to the concrete
 * `DriverCursorClaimCoordinator` (drivers/src/port.ts line 97-133) /
 * `NativeCursorClaimCoordinator` (apps/daemon/src/native-turn-runtime.ts).
 *
 * The ONLY no-active composition this module drives: after a closed `null` recovery
 * proof, `releaseReplayAsNoActive` aborts the retained_only claim and mints the
 * one-shot authorization; `beginLiveResume` exchanges it (once) for a ticket;
 * `claimForLiveResume` redeems the ticket (once) for a fresh live composite
 * claim. Every arm is one-shot and fail-closed; a claim that fails is aborted.
 */
export interface TurnLiveResumePort {
  releaseReplayAsNoActive(
    claim: RecoveryCompositeCursorClaim,
  ): Promise<RecoveryLiveResumeAuthorization>;
  beginLiveResume(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    authorization: RecoveryLiveResumeAuthorization;
  }): RecoveryLiveResumeTicket;
  claimForLiveResume(input: {
    ticket: RecoveryLiveResumeTicket;
  }): Promise<RecoveryCompositeCursorClaim>;
}

/**
 * The composite fresh-process recovery port the app wires: a single opener that
 * yields the composite claim to run the recovery read against (mirroring
 * `DriverCursorClaimCoordinator.claimForReplay`, drivers/src/port.ts line 105).
 * This module opens it in `retained_only` resume mode, then reads recovery under
 * the claim's authority fence. Kept minimal: the app owns pump/spec wiring.
 */
export interface TurnRecoveryClaimPort {
  claimForReplay(): Promise<RecoveryCompositeCursorClaim>;
}
