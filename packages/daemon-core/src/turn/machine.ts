import type {
  ArtifactDigest,
  CommandId,
  ContributionBinding,
  DeliveryFence,
  DriverTurnBinding,
  LaunchId,
  NativeInvocationFence,
  NormalizedDriverEvent,
  ReceiptId,
  SessionId,
  SimpleTaskCommand,
  StateInstanceId,
  TurnCoordinationDisposition,
  TurnId,
} from "@swarm/protocol";
import {
  buildContributionBinding,
  verifyContributionBinding,
} from "@swarm/protocol";
import { protocolDigest } from "@swarm/runtime-contract";
import type { NativeProcessWriteOutcome } from "@swarm/drivers";

import { turnFail } from "./errors.js";
import type {
  AtomicTurnJournalPort,
  CommitTurnStepInput,
  DriverEventReaderClaimResult,
  DriverEventReaderPort,
  DurableTurnState,
  RecoveryCompositeCursorClaim,
  RecoveryLiveResumeTicket,
  ReplayableTurnRecoveryBasis,
  RetainedEventLease,
  RetainedTurnEventRecord,
  RetainedTurnEventSource,
  RetainedTurnReplay,
  TurnCommandIdSource,
  TurnDriverPort,
  TurnEventInput,
  TurnEventRecordInput,
  TurnLiveResumePort,
  TurnMutationResult,
  TurnRecoveryClaimPort,
  TurnRecoveryReadPort,
  TurnRecoveryReadResult,
  TurnStepKind,
  TurnStepResult,
  TurnSubmission,
} from "./ports.js";

// ---------------------------------------------------------------------------
// Local value shapes (not part of the wire contract).
// ---------------------------------------------------------------------------

/** Per-contribution runtime receipts captured from a `written` outcome. */
type ContributionReceipts = {
  readonly runtimeWriteId: CommandId;
  readonly visibilityEventId: CommandId;
};

/**
 * The verified server coordination result Lane D binds AFTER committing the
 * coordination. `commandId` is the recorded coordination_call's own commandId;
 * `receiptId` is the non-forgeable server ReceiptId; `resultDigest` is the
 * ArtifactDigest of the verified result.
 */
type BoundCoordinationResult = {
  readonly kind: "committed" | "terminal_replay";
  readonly commandId: CommandId;
  readonly receiptId: ReceiptId;
  readonly resultDigest: ArtifactDigest;
};

/**
 * A coordination_call seen after the reply: its recorded commandId + requested
 * command, plus the bound disposition once Lane D binds it. A requested-but-
 * unbound coordination keeps `bound === null` (no forgeable disposition).
 */
type PendingCoordination = {
  readonly commandId: CommandId;
  readonly command: SimpleTaskCommand;
  bound: BoundCoordinationResult | null;
};

/**
 * A single active contribution: the expected receipts for one input write, its
 * delivery/invocation/permit, its per-contribution binding digest, and the
 * mirror of the durable state the promoted CAS is validated against.
 * `inputOrdinal` is 0 for the root ordinary turn, N>=1 for the Nth steer.
 */
type ActiveContribution = {
  readonly turnId: TurnId;
  readonly inputOrdinal: number;
  readonly invocation: NativeInvocationFence;
  readonly permitId: CommandId;
  readonly delivery: DeliveryFence;
  readonly receipts: ContributionReceipts;
  /** The per-contribution binding-digest fence (binding invariant #4). */
  readonly bindingDigest: ArtifactDigest;
  phase: "write_started" | "input_written" | "model_visible";
  /**
   * Whether an assistant_reply event has been captured for this turn. It stays
   * true (turn-wide) once seen; the durable replyCommitted only flips true at
   * commitTurnTerminal (matching the promoted expected.replyCommitted === false
   * gate). A second reply is MULTIPLE_ASSISTANT_REPLIES.
   */
  replyStaged: boolean;
  /**
   * The command-id-free reply leg captured AT the reply event (bound early, not
   * reconstructed at terminal). Part of the exposed command-id-free draft basis
   * and of the promoted terminal basis.
   */
  pendingReply: TurnEventRecordInput | undefined;
  pendingCoordination: PendingCoordination | null;
  /** The command-id-free coordination leg captured AT the coordination_call event. */
  pendingCoordinationRecord: TurnEventRecordInput | undefined;
  /**
   * true once model_visible is reached (steerable tool boundary); flipped false
   * by a non-steerable review/compaction boundary. A steer while false is
   * ACTIVE_TURN_NOT_STEERABLE.
   */
  steerable: boolean;
  /**
   * The last durable state the journal returned for this contribution's logical
   * turn. Passed as `expected` on the next mutation (full-state CAS).
   */
  durable: DurableTurnState;
};

/**
 * A queued ORDINARY submission awaiting the active turn to drain (unsafe steers
 * are NEVER queued). Its logical turn identity is its own
 * delivery.turnId; it admits fresh at inputOrdinal 0.
 */
type QueuedContribution = {
  readonly submission: TurnSubmission;
};

/**
 * The terminal turn record: the coordination snapshot + the canonical
 * ContributionBinding + the single fresh terminal stage snapshot, captured
 * atomically when `turn_completed` reduces. Held by `#terminal` on the same-
 * process path and by the off-side recovered image during fresh-process recovery.
 */
type TerminalRecord = {
  pendingCoordination: PendingCoordination | null;
  contribution: ContributionBinding;
  // The single fresh terminal stage snapshot, reconstructed together.
  stage: TurnTerminalStage;
};

/**
 * The off-side recovered machine image assembled during fresh-process recovery:
 * the local active contribution (mutated by the reply/coordination/boundary legs)
 * plus a local terminal-stage holder (set when the suffix reaches turn_completed,
 * at which point `active` clears). The whole retained suffix reduces INTO this
 * image via `#reduceRetainedInto` WITHOUT writing any live `this.#...` field; the
 * image is published atomically only after the whole suffix reduces successfully.
 */
type RecoveredImage = {
  active: ActiveContribution | null;
  terminal: TerminalRecord | null;
};

export type SubmitOutcome =
  | { kind: "written"; turnId: TurnId; inputOrdinal: number; receipts: ContributionReceipts }
  | { kind: "queued"; turnId: TurnId; inputOrdinal: number }
  | { kind: "rejected_before_write" }
  | { kind: "next_generation" }
  | { kind: "held_ambiguous" };

export type ApplyEventOutcome =
  | {
      kind: "advanced";
      nextOrdinal: number;
      lastEventDigest: ArtifactDigest | null;
      committed: boolean;
    }
  | { kind: "ignored_no_cursor_advance"; reason: "stale_turn" | "wrong_turn" };

export type InterruptOutcome =
  | { kind: "interrupted"; turnId: TurnId }
  | { kind: "no_active_turn" }
  | { kind: "uncorrelated" };

export type TurnCoordinationRequest =
  | { kind: "not_requested"; terminalTurnId: TurnId }
  | { kind: "requested"; commandId: CommandId; command: SimpleTaskCommand };

/**
 * The final terminal handoff: the canonical ContributionBinding plus the
 * protocol-owned TurnCoordinationDisposition (post-bind). Lane D verifies this
 * contribution against its own expected fence, adds the verified reply, and
 * builds the completion envelope.
 */
export type TurnTerminalResult = {
  contribution: ContributionBinding;
  coordination: TurnCoordinationDisposition;
};

/**
 * The terminal draft basis this module STAGES (never commits) for Lane D. PIN 1:
 * only the REPLY leg is command-id-free; Lane D adds the replyCommandId when it
 * reconstitutes the promoted TurnTerminalCommitBasis. The coordination leg is NOT
 * command-id-free — it carries the machine-side request `commandId` plus a
 * `commandDigest` = protocolDigest(canonical requested command). All legs are
 * captured AT their events (bound early), never reconstructed after terminal.
 */
export type TurnTerminalDraftBasis = {
  reply: TurnEventRecordInput;
  coordination?: TurnEventRecordInput & { commandId: CommandId; commandDigest: ArtifactDigest };
  completed: TurnEventRecordInput;
};

/**
 * The SINGLE fresh, recursively deep-frozen terminal stage snapshot this module exposes
 * (PIN 2). Reconstructed together so a caller cannot mutate later truth and so
 * there is no torn read of #active after the stage clears. this module leaves cursor /
 * local turn / native attempt UNCOMMITTED — Lane D makes the single
 * commitTurnTerminal call using this snapshot.
 */
export type TurnTerminalStage = {
  contribution: ContributionBinding;
  coordinationRequest: TurnCoordinationRequest;
  basis: TurnTerminalDraftBasis;
  readerFence: {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    readerEpoch: number;
  };
  expected: DurableTurnState;
  next: DurableTurnState;
};

/**
 * The narrow terminal port Lane D binds to — NEVER the concrete
 * TurnMachine. `terminalDraft()` is PURE STAGING (no commit, no synthetic
 * evidence). `bindCoordinationResult()`/`terminalResult()` are the POST-SERVER
 * phase Lane D drives AFTER its own server reply + the sole commitTurnTerminal;
 * this module does NOT commit through them or synthesize server evidence.
 */
export interface TurnTerminalBindingPort {
  terminalDraft(): TurnTerminalStage;
  bindCoordinationResult(
    disposition: Extract<TurnCoordinationDisposition, { kind: "committed" | "terminal_replay" }>,
  ): TurnTerminalResult;
  terminalResult(): TurnTerminalResult;
}

export type MachineConfig = {
  readonly launchId: LaunchId;
  readonly stateInstanceId: StateInstanceId;
  readonly sessionId: SessionId;
  readonly readerOwnerToken: ArtifactDigest;
};

/**
 * Recursively freeze a value so a caller of the port can never mutate returned
 * draft/result truth (immutable / replay-stable).
 */
function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const key of Object.keys(value as Record<string, unknown>)) {
      deepFreeze((value as Record<string, unknown>)[key]);
    }
    Object.freeze(value);
  }
  return value;
}

/**
 * Canonical scalar reconstruction of a SimpleTaskCommand: ONLY the known
 * canonical scalar fields survive, so unknown / symbol / non-enumerable keys can
 * never cross the terminal port boundary via a captured or returned command.
 */
function canonicalCommand(command: SimpleTaskCommand): SimpleTaskCommand {
  return {
    protocolVersion: command.protocolVersion,
    title: command.title,
    sourceMessageId: command.sourceMessageId,
  };
}

/**
 * Port-authoritative replay identity: the canonical identity of a committed step
 * keyed on the driver event's OWN immutable facts (stateInstance/session + ordinal + eventDigest +
 * turn + bindingDigest + kind) — the fields that distinguish a byte-identical
 * replay from a divergent event at the same ordinal. Excludes recordedAt /
 * ownerToken / readerEpoch (non-identity). Two byte-identical events map to the
 * same identity; any changed eventDigest/bindingDigest/turn/kind changes it, so
 * the divergent event is not aliased and reaches the port to conflict.
 */
function stepIdentity(commit: CommitTurnStepInput): string {
  return JSON.stringify([
    "commitTurnStep",
    commit.stateInstanceId,
    commit.sessionId,
    commit.event.ordinal,
    commit.event.eventDigest,
    commit.event.turnId,
    commit.event.bindingDigest,
    commit.kind,
  ]);
}

// ---------------------------------------------------------------------------
// Fresh-process recovery (composition of the durable turn-recovery read + the
// retained finite replay + no-active live-resume). See #recoverFreshProcess.
// ---------------------------------------------------------------------------

/**
 * The ONLY event kinds a retained turn SUFFIX may carry after a model_visible
 * replayable arm: the post-model_visible terminal legs. A pre-watermark kind
 * (turn_started / input_written / model_visible) in the suffix is an impossible
 * cross-fenced cell and is rejected fail-closed during whole-suffix validation.
 */
const RETAINED_SUFFIX_KINDS = new Set<NormalizedDriverEvent["kind"]>([
  "assistant_reply",
  "coordination_call",
  "turn_completed",
  "turn_boundary",
]);

/**
 * The recovery ports the APP wires (out of scope for this module's 5 files). Passed to
 * `recoverFreshProcess()` (NOT the constructor — the same-process constructor
 * and its accepted semantics are preserved unchanged). Each is a wire-identical
 * abstraction of a promoted recovery/retained-replay seam declared in ports.ts.
 */
export type FreshProcessRecoveryPorts = {
  /** The composite (storage cursor + private retained lease) claim opener. */
  claim: TurnRecoveryClaimPort;
  /** readTurnRecovery — the authoritative durable recovery read. */
  recovery: TurnRecoveryReadPort;
  /** The retained finite suffix replay from the exact watermark. */
  retained: RetainedTurnEventSource;
  /** The no-active live-resume authorization/ticket handoff. */
  liveResume: TurnLiveResumePort;
};

/**
 * The disposition of a fresh-process recovery. Mirrors the app-level
 * FreshTurnRecoveryDisposition (apps/daemon/src/native-turn-runtime.ts line
 * 371-380) intent, projected to this module's contract:
 *   - `recovered`: readTurnRecovery `replayable` -> #active + cursor rebuilt
 *     from the retained suffix and RE-VERIFIED through the SAME protocol paths.
 *   - `live_resume`: readTurnRecovery `null` (no_active) -> the minimal
 *     live-resume transition composed AFTER a valid no_active proof.
 *   - `held_ambiguous`: readTurnRecovery `held_ambiguous` -> fail-closed,
 *     effect-free (composite claim aborted, NO #active).
 */
export type FreshProcessRecoveryOutcome =
  | {
      kind: "recovered";
      turnId: TurnId;
      inputOrdinal: number;
      phase: ActiveContribution["phase"];
      nextOrdinal: number;
      replyStaged: boolean;
      terminalStaged: boolean;
    }
  | { kind: "live_resume"; ticket: RecoveryLiveResumeTicket; claim: RecoveryCompositeCursorClaim }
  | { kind: "held_ambiguous"; reason: "PRE_MODEL_VISIBLE_EFFECT_UNKNOWN" | "ACTIVE_AMBIGUOUS" };

export class TurnMachine implements TurnTerminalBindingPort {
  readonly #journal: AtomicTurnJournalPort;
  readonly #reader: DriverEventReaderPort;
  readonly #driver: TurnDriverPort;
  readonly #ids: TurnCommandIdSource;
  readonly #config: MachineConfig;

  #readerEpoch: number | null = null;
  #active: ActiveContribution | null = null;
  #queue: QueuedContribution[] = [];
  /**
   * The terminal turn record's §7.9 coordination snapshot + the canonical
   * ContributionBinding, captured atomically when `turn_completed` reduces.
   */
  #terminal: TerminalRecord | null = null;
  /** The current active invocation generation for §8.5b gating. */
  #activeGeneration = 0;
  /** Held after an ambiguous write: no further driver call is permitted. */
  #ambiguousHold = false;
  #seenTurnIds = new Set<string>();

  constructor(input: {
    journal: AtomicTurnJournalPort;
    reader: DriverEventReaderPort;
    driver: TurnDriverPort;
    ids: TurnCommandIdSource;
    config: MachineConfig;
  }) {
    this.#journal = input.journal;
    this.#reader = input.reader;
    this.#driver = input.driver;
    this.#ids = input.ids;
    this.#config = input.config;
  }

  // -- read helpers -----------------------------------------------------------

  get activeTurnId(): TurnId | null {
    return this.#active?.turnId ?? null;
  }

  get activePhase(): ActiveContribution["phase"] | null {
    return this.#active?.phase ?? null;
  }

  /**
   * replyCommitted is TURN-WIDE (per logical turn = delivery.turnId) and reflects
   * the turn-wide reply CLOSURE: the in-memory STAGED reply OR the durable
   * committed reply. It closes at the reply and never
   * reopens; a post-reply steer cannot reopen the reply slot. (this module stages the
   * reply; Lane D durably commits it, so before Lane D's commit only the staged
   * flag is set.)
   */
  get replyCommitted(): boolean {
    if (this.#active === null) return false;
    return this.#active.replyStaged || this.#active.durable.replyCommitted;
  }

  /** 0 or 1 for the active logical turn (exactly-one assistant reply per turn). */
  get replyCount(): number {
    return this.replyCommitted ? 1 : 0;
  }

  get queuedCount(): number {
    return this.#queue.length;
  }

  get isAmbiguousHeld(): boolean {
    return this.#ambiguousHold;
  }

  get activeGeneration(): number {
    return this.#activeGeneration;
  }

  /** The last durable state the journal returned for the active turn (read-only). */
  get activeDurable(): DurableTurnState | null {
    return this.#active === null ? null : { ...this.#active.durable };
  }

  /**
   * The active contribution's per-contribution binding-digest fence (read-only).
   * The REQUIRED `TurnEventInput.bindingDigest` must equal this
   * for an event to be attributed to the active contribution; an event carrying
   * any other binding is rejected (WRITE_STARTED_BINDING_MISMATCH), never
   * reattributed. A stale/old-producer event thus cannot ride the current turn.
   */
  get activeBindingDigest(): ArtifactDigest | null {
    return this.#active?.bindingDigest ?? null;
  }

  #fence(): {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    readerEpoch: number;
  } {
    if (this.#readerEpoch === null) {
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "reader_not_claimed");
    }
    return {
      stateInstanceId: this.#config.stateInstanceId,
      sessionId: this.#config.sessionId,
      ownerToken: this.#config.readerOwnerToken,
      readerEpoch: this.#readerEpoch,
    };
  }

  /**
   * Claim the driver-event reader cursor before any event may be applied. The
   * ordinary claim returns the granted reader epoch together with the
   * authoritative cursor pair (`nextOrdinal` + `lastEventDigest`) it is anchored
   * to; the machine adopts all three so a later ACK carries the exact current
   * cursor rather than reconstructing it.
   */
  claimReader(mode: "start" | "resume"): DriverEventReaderClaimResult {
    const { readerEpoch, nextOrdinal, lastEventDigest } = this.#reader.claimDriverEventReader({
      stateInstanceId: this.#config.stateInstanceId,
      sessionId: this.#config.sessionId,
      ownerToken: this.#config.readerOwnerToken,
      mode,
      claimedAt: this.#ids.now(),
    });
    this.#readerEpoch = readerEpoch;
    this.#lastNextOrdinal = nextOrdinal;
    this.#lastEventDigest = lastEventDigest;
    return { readerEpoch, nextOrdinal, lastEventDigest };
  }

  /**
   * Restart recovery: rebuild the active contribution's durable-state mirror from
   * the journal after a process restart. Reads the durable turn state through the
   * reader fence; a `write_started`/`input_written`/`model_visible` row rehydrates
   * the mirror so the next CAS carries the correct `expected`.
   */
  recoverDurable(protocolTurnId: TurnId): DurableTurnState | null {
    const durable = this.#journal.readDurableTurnState({
      ...this.#fence(),
      protocolTurnId,
    });
    if (durable !== null && this.#active !== null && this.#active.turnId === protocolTurnId) {
      this.#active.durable = durable;
    }
    return durable;
  }

  // -- fresh-process recovery (readTurnRecovery + retained/live) ---------------

  /**
   * FRESH-PROCESS RECOVERY (composition). On a fresh process (daemon
   * restart mid-turn) the machine reconstructs its active contribution HONESTLY
   * from durable + retained sources, never a machine-local invention. The flow,
   * in order:
   *
   *   1) Open the composite (storage cursor + private retained lease) claim
   *      in `retained_only` resume mode; adopt its authority `readerEpoch` as the
   *      reader fence so every subsequent journal read fences correctly.
   *   2) Consume the promoted `readTurnRecovery` under the claim's authority
   *      fence. It returns a closed union OR `null`.
   *        - `null`   (no admitted active contribution): the ONLY no-active
   *          composition — release the retained_only claim as no-active (which
   *          ABORTS it), mint the one-shot live-resume authorization, exchange it
   *          for a ticket, and redeem the ticket for a fresh live claim. ZERO
   *          #active / journal / cursor / queue mutation; NO provider work.
   *        - `held_ambiguous` (either reason): ABORT the composite claim, publish
   *          NO #active. Fail-closed, effect-free.
   *        - `replayable`: restore #active ONLY from this arm (see below).
   *   3) `replayable`: cross-check the recovery cursor pair against the composite
   *      claim's authority watermark (a divergence ABORTS, fail-closed). Set the
   *      authoritative cursor from the recovery cursor pair. Open the retained
   *      finite suffix replay from the EXACT watermark under the four expectation
   *      fences (turn/binding/waiter/source); the source validates the whole
   *      suffix (below-watermark / stale / missing / cross-fenced rejected)
   *      before exposing its first record, with NO provider re-execution.
   *   4) Rebuild #active from the replayable basis, then REPLAY the retained suffix
   *      IN MEMORY, rebuilding pending reply / coordination and (on turn_completed)
   *      the terminal stage through the SAME reducer paths the same-process machine
   *      uses — so the reconstructed contribution is RE-VERIFIED through the SAME
   *      `buildContributionBinding` / `verifyContributionBinding` paths. NO terminal
   *      stage, reply text, raw payload, or SimpleTaskCommand.title enters shared
   *      storage; this module rebuilds them in memory from the replayed suffix only.
   *
   * Every stale / missing / cross-fenced / null / held / error path is FAIL-CLOSED
   * and EFFECT-FREE (composite claim aborted; zero machine/journal/cursor/queue
   * mutation; no provider work).
   */
  async recoverFreshProcess(
    ports: FreshProcessRecoveryPorts,
  ): Promise<FreshProcessRecoveryOutcome> {
    // Fail-closed precondition: fresh-process recovery must run on a fresh
    // machine (no active/staged in-memory contribution invented locally).
    if (this.#active !== null || this.#terminal !== null) {
      turnFail("INVALID_STATE_TRANSITION", "recover_requires_fresh_machine");
    }

    // (1) Open the composite claim (retained_only resume) and adopt its authority
    //     readerEpoch as the reader fence. The claim owns BOTH boundaries; every
    //     fail-closed path below aborts it (effect-free teardown of both).
    const claim = await ports.claim.claimForReplay();

    // (2) Read the promoted durable recovery under the claim's authority fence.
    let recovery: TurnRecoveryReadResult | null;
    try {
      recovery = ports.recovery.readTurnRecovery({
        stateInstanceId: claim.authority.stateInstanceId,
        sessionId: claim.authority.sessionId,
        ownerToken: claim.authority.ownerToken,
        readerEpoch: claim.authority.readerEpoch,
      });
    } catch (cause) {
      // Error path: abort the composite claim, publish NO #active, re-throw.
      await claim.abort();
      throw cause;
    }

    // null -> the ONLY no-active composition: live-resume handoff.
    if (recovery === null) {
      return this.#composeLiveResume(ports, claim);
    }

    // held_ambiguous -> fail-closed, effect-free.
    if (recovery.kind === "held_ambiguous") {
      await claim.abort();
      return { kind: "held_ambiguous", reason: recovery.reason };
    }

    // replayable -> honest reconstruction from durable + retained sources.
    return this.#recoverReplayable(ports, claim, recovery);
  }

  /**
   * The no-active live-resume transition. Composed ONLY after a valid
   * `null` (no_active) recovery proof. releaseReplayAsNoActive ABORTS the
   * retained_only claim and mints the one-shot authorization; beginLiveResume
   * exchanges it (once) for a ticket; claimForLiveResume redeems it (once) for a
   * fresh live composite claim. ZERO #active / journal / cursor / queue mutation.
   */
  async #composeLiveResume(
    ports: FreshProcessRecoveryPorts,
    claim: RecoveryCompositeCursorClaim,
  ): Promise<FreshProcessRecoveryOutcome> {
    // releaseReplayAsNoActive aborts the retained_only claim internally and
    // returns the one-shot authorization (it is a fail-closed operation).
    const authorization = await ports.liveResume.releaseReplayAsNoActive(claim);
    const ticket = ports.liveResume.beginLiveResume({
      protocolVersion: authorization.protocolVersion,
      launchId: authorization.launchId,
      stateInstanceId: this.#config.stateInstanceId,
      sessionId: this.#config.sessionId,
      cursorOwnerToken: authorization.ownerToken,
      authorization,
    });
    const liveClaim = await ports.liveResume.claimForLiveResume({ ticket });
    // No #active: a no-active resume starts a fresh live session; the caller
    // wires the live pump. this module mutates no machine/journal/cursor/queue state.
    return { kind: "live_resume", ticket, claim: liveClaim };
  }

  /**
   * Reconstruct #active + cursor from the promoted `replayable` arm and the
   * retained finite suffix, RE-VERIFYING through the SAME protocol paths.
   */
  async #recoverReplayable(
    ports: FreshProcessRecoveryPorts,
    claim: RecoveryCompositeCursorClaim,
    recovery: Extract<TurnRecoveryReadResult, { kind: "replayable" }>,
  ): Promise<FreshProcessRecoveryOutcome> {
    const basis = recovery.basis;

    // (3a) Cross-check the recovery cursor pair against the composite claim's
    //      authority watermark. A divergence is a cross-fenced/torn read -> abort,
    //      fail-closed, effect-free.
    if (
      recovery.cursor.nextOrdinal !== claim.authority.nextOrdinal ||
      recovery.cursor.lastEventDigest !== claim.authority.lastEventDigest
    ) {
      await claim.abort();
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "recovery_watermark_mismatch");
    }

    // (3b) RE-VERIFY the reconstructed contribution binding through the SAME
    //      protocol builder/verifier the same-process machine uses. A recomputed
    //      digest that does not equal the basis' stored bindingDigest is a
    //      corrupted/forged recovery -> abort, fail-closed. This is the identical
    //      binding computation used at admission and at #stageTerminal.
    let reconstructed: ContributionBinding;
    try {
      reconstructed = verifyContributionBinding(
        buildContributionBinding({
          fence: basis.fence,
          stateInstanceId: basis.stateInstanceId,
          inputOrdinal: basis.inputOrdinal,
          invocationId: basis.invocationId,
          invocationGeneration: basis.invocationGeneration,
          permitId: basis.permitId,
          runtimeWriteId: basis.runtimeWriteId,
          visibilityEventId: basis.visibilityEventId,
        }),
        { expectedFence: basis.fence },
      );
    } catch (cause) {
      await claim.abort();
      throw cause;
    }
    if (
      reconstructed.contributionBindingDigest !== basis.bindingDigest ||
      basis.stateInstanceId !== this.#config.stateInstanceId ||
      basis.sessionId !== this.#config.sessionId ||
      basis.launchId !== this.#config.launchId
    ) {
      await claim.abort();
      turnFail("WRITE_STARTED_BINDING_MISMATCH", "recovery_binding_mismatch");
    }

    // Build the recovered active contribution as a LOCAL object (the replayable
    // arm is, by contract, a model_visible contribution: durable==="model_visible",
    // attemptState==="model_visible", chain depth 4). NO `this.#...` field is
    // written here — the entire recovered image is assembled + validated OFF-SIDE
    // and only published atomically after the whole suffix reduces successfully.
    const durable: DurableTurnState = {
      protocolTurnId: basis.protocolTurnId,
      phase: "model_visible",
      inputOrdinal: basis.inputOrdinal,
      bindingDigest: basis.bindingDigest,
      steerable: true,
      replyCommitted: false,
    };
    const active: ActiveContribution = {
      turnId: basis.protocolTurnId,
      inputOrdinal: basis.inputOrdinal,
      invocation: {
        invocationGeneration: basis.invocationGeneration,
        invocationId: basis.invocationId,
      },
      permitId: basis.permitId,
      delivery: basis.fence,
      receipts: {
        runtimeWriteId: basis.runtimeWriteId,
        visibilityEventId: basis.visibilityEventId,
      },
      bindingDigest: basis.bindingDigest,
      phase: "model_visible",
      replyStaged: false,
      pendingReply: undefined,
      pendingCoordination: null,
      pendingCoordinationRecord: undefined,
      steerable: true,
      durable,
    };
    // The OFF-SIDE recovered image: the local active contribution + a local
    // terminal-stage holder. The whole suffix reduces INTO this sink; no live
    // machine field is touched until the entire suffix reduces successfully.
    const sink: RecoveredImage = { active, terminal: null };
    // The reader fence the off-side reduction stamps on any rebuilt terminal
    // stage: derived from the claim authority (the readerEpoch adopted on publish),
    // NOT from `this.#readerEpoch` (still unclaimed until the atomic publish).
    const recoveryReaderFence = {
      stateInstanceId: this.#config.stateInstanceId,
      sessionId: this.#config.sessionId,
      ownerToken: this.#config.readerOwnerToken,
      readerEpoch: claim.authority.readerEpoch,
    };

    // (3c) Open the retained finite replay from the EXACT watermark, under the
    //      four expectation fences bound to the recovered basis. The source
    //      validates the WHOLE suffix (below-watermark / stale / missing /
    //      cross-fenced rejected fail-closed) before exposing its first record;
    //      NO provider re-execution. Materialize into a bounded snapshot AND
    //      reduce it OFF-SIDE BEFORE any machine mutation (no partial mutation on
    //      a corrupt or reducer-invalid suffix). The whole open -> materialize ->
    //      off-side reduce sequence is wrapped so `replay.close()` (which releases
    //      the composite claim) runs on EVERY exit path — success or any throw.
    let replay: RetainedTurnReplay;
    try {
      replay = await ports.retained.openReplay({
        expectedTurnId: basis.protocolTurnId,
        expectedBindingDigest: basis.bindingDigest,
        expectedResolvedWaiterId: basis.invocationId,
        expectedSourceMessageId: basis.sourceMessageId,
        claim,
      });
    } catch (cause) {
      // openReplay itself failed: the replay was never created, so close the
      // composite claim by aborting it directly. ZERO live machine mutation.
      await claim.abort();
      throw cause;
    }

    // Materialize + structurally validate the whole finite suffix, then reduce it
    // INTO the off-side image. Either step may throw (structural fence OR semantic
    // reduction failure); the whole image is built off-side, so NO live machine
    // field is touched on any failure path. Capture the primary failure so the
    // guaranteed close below cannot mask it.
    let primaryFailure: { readonly cause: unknown } | null = null;
    try {
      const suffix = await this.#materializeSuffix(replay.lease, replay.records, recovery, basis);
      for (const record of suffix) {
        this.#reduceRetainedInto(record, sink, recoveryReaderFence);
      }
    } catch (cause) {
      primaryFailure = { cause };
    }
    // GUARANTEE close on every exit path (success or any throw): close() releases
    // the composite claim and is attempted UNCONDITIONALLY. Its own failure is
    // captured separately so it can NEVER replace a primary materialize/reduce
    // failure.
    let closeFailure: { readonly cause: unknown } | null = null;
    try {
      await replay.close();
    } catch (cause) {
      closeFailure = { cause };
    }
    // Surface failures WITHOUT masking: a primary + a close failure raises BOTH
    // causes (an aggregate); a primary alone raises the primary; a close failure
    // alone raises the close cause. Every one of these paths leaves ZERO live
    // machine mutation (the recovered image was built off-side and is discarded
    // here). Only a fully clean materialize + reduce + close reaches the atomic
    // publish below.
    if (primaryFailure !== null && closeFailure !== null) {
      throw new AggregateError(
        [primaryFailure.cause, closeFailure.cause],
        "recovery teardown failed after a recovery failure",
      );
    }
    if (primaryFailure !== null) throw primaryFailure.cause;
    if (closeFailure !== null) throw closeFailure.cause;

    // (4) The ENTIRE legal suffix reduced successfully into the side image. PUBLISH
    //     atomically: assign every live field in one un-interruptible block (no
    //     await between assignments), then return. If the suffix ended in
    //     turn_completed the recovered turn is staged-terminal (sink.active === null,
    //     sink.terminal set); otherwise the turn stays alive/steerable.
    const recoveredActive = sink.active;
    const recoveredTerminal = sink.terminal;
    this.#readerEpoch = claim.authority.readerEpoch;
    this.#lastNextOrdinal = recovery.cursor.nextOrdinal;
    this.#lastEventDigest = recovery.cursor.lastEventDigest;
    this.#activeGeneration = basis.invocationGeneration;
    this.#active = recoveredActive;
    if (recoveredTerminal !== null) this.#terminal = recoveredTerminal;
    this.#seenTurnIds.add(basis.protocolTurnId as unknown as string);

    return {
      kind: "recovered",
      turnId: basis.protocolTurnId,
      inputOrdinal: basis.inputOrdinal,
      phase: recoveredActive?.phase ?? "model_visible",
      nextOrdinal: this.#lastNextOrdinal,
      replyStaged: active.replyStaged,
      terminalStaged: recoveredTerminal !== null,
    };
  }

  /**
   * Materialize + validate the retained finite suffix into a bounded snapshot,
   * fail-closed, BEFORE any machine mutation. Each record must ride the exact
   * watermark-anchored lease (state-instance/session/launch/owner/epoch, protocol
   * version) and the four expectation fences (turn/binding/waiter/source); its
   * ordinal must be contiguous from the watermark `nextOrdinal` (a below-watermark
   * / gap / cross-fenced record is rejected). The record's declared `eventKind`
   * must match its normalized event kind. This mirrors the promoted
   * `validateRetainedSnapshot` fences (drivers/src/retained-events.ts line 328).
   */
  async #materializeSuffix(
    lease: RetainedEventLease,
    records: AsyncIterable<RetainedTurnEventRecord>,
    recovery: Extract<TurnRecoveryReadResult, { kind: "replayable" }>,
    basis: ReplayableTurnRecoveryBasis,
  ): Promise<RetainedTurnEventRecord[]> {
    // The suffix begins at the EXACT watermark and the lease's watermark must
    // equal the recovered cursor pair (no below-watermark replay).
    if (
      lease.stateInstanceId !== this.#config.stateInstanceId ||
      lease.sessionId !== this.#config.sessionId ||
      lease.launchId !== this.#config.launchId ||
      lease.ownerToken !== this.#config.readerOwnerToken ||
      lease.nextOrdinal !== recovery.cursor.nextOrdinal ||
      lease.lastEventDigest !== recovery.cursor.lastEventDigest
    ) {
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "retained_lease_watermark_mismatch");
    }
    const out: RetainedTurnEventRecord[] = [];
    let ordinal = lease.nextOrdinal;
    let previousRecordDigest: ArtifactDigest | null = null;
    for await (const record of records) {
      if (
        record.stream !== "turn" ||
        record.stateInstanceId !== this.#config.stateInstanceId ||
        record.sessionId !== this.#config.sessionId ||
        record.launchId !== this.#config.launchId ||
        record.readerEpoch !== lease.readerEpoch ||
        record.ordinal !== ordinal ||
        record.turnId !== basis.protocolTurnId ||
        record.bindingDigest !== basis.bindingDigest ||
        record.resolvedWaiterId !== basis.invocationId ||
        record.sourceMessageId !== basis.sourceMessageId ||
        record.event.turnId !== basis.protocolTurnId ||
        record.event.kind !== record.eventKind ||
        (ordinal === lease.nextOrdinal
          ? record.previousRecordDigest !== null && lease.nextOrdinal === 0
          : record.previousRecordDigest !== previousRecordDigest)
      ) {
        turnFail("DRIVER_EVENT_FENCE_MISMATCH", "retained_record_cross_fence");
      }
      // The replayable arm is, by promoted contract, already at model_visible; the
      // retained SUFFIX therefore only carries the post-model_visible terminal
      // legs. A pre-watermark kind (turn_started / input_written / model_visible)
      // in the suffix is an impossible/cross-fenced cell -> fail-closed, validated
      // WHOLE-suffix BEFORE any machine mutation (effect-free).
      if (!RETAINED_SUFFIX_KINDS.has(record.event.kind)) {
        turnFail("INVALID_STATE_TRANSITION", { at: "retained_suffix", kind: record.event.kind });
      }
      out.push(record);
      previousRecordDigest = record.recordDigest;
      ordinal += 1;
    }
    // Finite/bounded: the suffix must end exactly at the private snapshot head.
    if (ordinal !== lease.snapshotHeadNextOrdinal) {
      turnFail("DRIVER_EVENT_ORDER_INVALID", "retained_suffix_incomplete");
    }
    return out;
  }

  /**
   * Reduce one retained suffix record IN MEMORY through the SAME staged-terminal
   * semantics the same-process machine uses, threading the OFF-SIDE recovered image
   * (local `active`/`terminal`) rather than writing any live `this.#active` /
   * `this.#terminal` field. This mirrors #reduceTurn / #record / #stageTerminal
   * (via #buildTerminalRecord) exactly, preserving all reduction validations; a
   * reducer-invalid suffix (e.g. coordination before any reply) fails closed here,
   * BEFORE the recovered image is ever published, so no live state is mutated. NO
   * journal / cursor mutation: the suffix rebuilds pending reply / coordination /
   * terminal stage in memory only — reply text, coordination command (title), and
   * terminal stage never enter shared storage.
   */
  #reduceRetainedInto(
    record: RetainedTurnEventRecord,
    sink: RecoveredImage,
    readerFence: {
      stateInstanceId: StateInstanceId;
      sessionId: SessionId;
      ownerToken: ArtifactDigest;
      readerEpoch: number;
    },
  ): void {
    const active = sink.active;
    if (active === null) return;
    // The per-contribution binding fence rides on every retained record; a record
    // carrying any other binding was already rejected in #materializeSuffix.
    const asEvent: TurnEventInput = {
      ordinal: record.ordinal,
      eventDigest: record.eventDigest,
      bindingDigest: record.bindingDigest,
      event: record.event,
    };
    switch (record.event.kind) {
      case "assistant_reply": {
        if (active.phase !== "model_visible") {
          turnFail("ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE", { phase: active.phase });
        }
        if (active.replyStaged || active.durable.replyCommitted) {
          turnFail("MULTIPLE_ASSISTANT_REPLIES");
        }
        active.pendingReply = this.#record(asEvent, active);
        active.replyStaged = true;
        return;
      }
      case "coordination_call": {
        if (!active.replyStaged) {
          turnFail("ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE", "coordination_before_reply");
        }
        if (active.pendingCoordination !== null) {
          turnFail("SECOND_COORDINATION_CALL");
        }
        const rec = this.#record(asEvent, active);
        const command = deepFreeze(canonicalCommand(record.event.command));
        active.pendingCoordination = {
          commandId: record.event.commandId,
          command,
          bound: null,
        };
        active.pendingCoordinationRecord = rec;
        return;
      }
      case "turn_completed": {
        // Rebuild the terminal stage IN MEMORY through the SAME staged-terminal
        // path (builds + verifies the ContributionBinding, snapshots the
        // command-id-free basis) — into the OFF-SIDE image. On turn_completed the
        // local active clears and the local terminal holder is set. No journal
        // mutation and no live-field write.
        sink.terminal = this.#buildTerminalRecord(asEvent, active, readerFence);
        sink.active = null;
        return;
      }
      case "turn_boundary": {
        // A non-steerable boundary in the retained suffix closes the steer window
        // (re-verified in memory); a steerable boundary leaves it open.
        const nextSteerable = record.event.steerable && active.phase === "model_visible";
        active.steerable = nextSteerable;
        active.durable = { ...active.durable, steerable: nextSteerable };
        return;
      }
      default:
        // input_written / model_visible / turn_started belong to the pre-watermark
        // prefix (the replayable arm is already at model_visible); a suffix record
        // of these kinds is an impossible/cross-fenced cell -> fail-closed.
        turnFail("INVALID_STATE_TRANSITION", { at: "retained_suffix", kind: record.event.kind });
    }
  }

  // -- submit / steer / queue -------------------------------------------------

  async submit(submission: TurnSubmission): Promise<SubmitOutcome> {
    if (this.#ambiguousHold) turnFail("AMBIGUOUS_NATIVE_WRITE");
    const isSteer = submission.mode.kind === "steer";
    if (isSteer) return this.#submitSteer(submission);
    if (this.#active !== null) return this.#enqueueOrdinary(submission);
    return this.#writeOrdinary(submission);
  }

  #enqueueOrdinary(submission: TurnSubmission): SubmitOutcome {
    // A fresh ordinary delivery arriving while a turn is active becomes its own
    // future turn, identified by its own admission DeliveryFence.turnId.
    const turnId = submission.delivery.turnId;
    this.#queue.push({ submission });
    return { kind: "queued", turnId, inputOrdinal: 0 };
  }

  async #submitSteer(submission: TurnSubmission): Promise<SubmitOutcome> {
    if (submission.mode.kind !== "steer") turnFail("INVALID_STATE_TRANSITION");
    const expectedTurnId = submission.mode.expectedTurnId;
    const active = this.#active;

    // A steer requires an active turn whose id matches the caller's own
    // expectedTurnId; a wrong/completed target is ACTIVE_TURN_CONFLICT.
    if (active === null || active.turnId !== expectedTurnId) {
      turnFail("ACTIVE_TURN_CONFLICT", { expectedTurnId });
    }
    // §8 same-logical-turn: the steer's OWN admission delivery fence must carry
    // the exact active turn identity (fix #2/#3). A sibling delivery.turnId is a
    // different logical turn, rejected before ANY write/journal/cursor effect.
    if (submission.delivery.turnId !== active.turnId) {
      turnFail("ACTIVE_TURN_CONFLICT", { expectedTurnId });
    }
    // Steerability CLOSES at the reply — checked on
    // the turn-wide staged OR durable reply closure (a steer contribution
    // INHERITS the logical-turn reply closure and never reopens it). A post-reply
    // steer is rejected with ZERO machine/journal/cursor/queue effect (Lane D
    // re-admits it as a fresh ordinary turn).
    if (active.replyStaged || active.durable.replyCommitted) {
      turnFail("ACTIVE_TURN_NOT_STEERABLE", { turnId: active.turnId, reason: "reply_closed" });
    }

    const nextOrdinal = active.inputOrdinal + 1;

    // An unsafe/non-native steer (not at a steerable model_visible
    // boundary) is REJECTED with ZERO queue mutation. This module does NOT enqueue it and
    // does NOT reuse the active turnId. Lane D retains the contribution and later
    // re-admits it as an ORDINARY submission carrying a NEWLY issued exact
    // DeliveryFence/turnId (the normal ordinary admission path).
    if (active.phase !== "model_visible" || !active.steerable) {
      turnFail("ACTIVE_TURN_NOT_STEERABLE", { turnId: active.turnId });
    }

    // A steerable steer admits within the SAME logical turn (same turnId, next
    // input ordinal) via the ordinary write discipline.
    return this.#writeContribution(
      submission,
      active.turnId,
      nextOrdinal,
      expectedTurnId,
      true,
      active.durable,
    );
  }

  async #writeOrdinary(submission: TurnSubmission): Promise<SubmitOutcome> {
    // §8: the logical turn identity IS the admission DeliveryFence.turnId, never a
    // separately minted id.
    const turnId = submission.delivery.turnId;
    return this.#writeContribution(submission, turnId, 0, undefined, false, null);
  }

  /**
   * Ordered write discipline (fix #1 admission-first). Steps, in order:
   *   1) fail-closed pairwise-distinct guard on the four command ids.
   *   2) beginTurnContribution (admission): validate-pure CAS BEFORE any effect,
   *      sets durable phase to write_started (the promoted contract does not use a
   *      separate write_started driver step). Aliases applied:false on exact replay.
   *   3) native driver adapter write -> 4-way NativeProcessWriteOutcome. The
   *      adapter owns the permit_recorded/write_started/input_written/model_visible
   *      invocation-entry chain and ECHOES the preallocated runtime-write +
   *      visibility ids (binding invariant #1).
   *   4) branch on the union; settle to terminal_error / ambiguous on a bad path.
   */
  async #writeContribution(
    submission: TurnSubmission,
    turnId: TurnId,
    inputOrdinal: number,
    expectedTurnId: TurnId | undefined,
    isSteer: boolean,
    expectedDurable: DurableTurnState | null,
  ): Promise<SubmitOutcome> {
    const nextGeneration = this.#activeGeneration + 1;
    const invocation: NativeInvocationFence = {
      invocationGeneration: nextGeneration,
      invocationId: this.#ids.nextCommandId(),
    };
    const runtimeWriteId = this.#ids.nextCommandId();
    const visibilityEventId = this.#ids.nextCommandId();
    // Fail-closed pairwise-distinct guard on {invocationId, permitId,
    // runtimeWriteId, visibilityEventId} (binding invariant #1): a collision fails
    // BEFORE admission / any driver write, so #requireContributionJoin can always
    // re-join the stored entries by these ids.
    if (
      new Set([invocation.invocationId, submission.permitId, runtimeWriteId, visibilityEventId])
        .size !== 4
    ) {
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "runtime_ids_not_pairwise_distinct");
    }
    const now = this.#ids.now();

    // The per-contribution binding digest (binding invariant #4 fence). Built from
    // the SAME authoritative facts the terminal contribution binds over, so the
    // fence validated before every step equals the terminal contribution digest.
    const bindingDigest = buildContributionBinding({
      fence: submission.delivery,
      stateInstanceId: this.#config.stateInstanceId,
      inputOrdinal,
      invocationId: invocation.invocationId,
      invocationGeneration: invocation.invocationGeneration,
      permitId: submission.permitId,
      runtimeWriteId,
      visibilityEventId,
    }).contributionBindingDigest;

    const next: DurableTurnState = {
      protocolTurnId: turnId,
      phase: "write_started",
      inputOrdinal,
      bindingDigest,
      steerable: false,
      replyCommitted: false,
    };

    // (2) admission — validate-pure BEFORE the driver effect (fix #1). The
    // promoted CAS throws ACTIVE_TURN_CONFLICT on a conflicting/occupied turn and
    // aliases applied:false on an exact operationDigest replay.
    const admitted: TurnMutationResult = this.#journal.beginTurnContribution({
      ...this.#fence(),
      protocolTurnId: turnId,
      launchId: this.#config.launchId,
      rootProducerFactId: submission.rootProducerFactId,
      inputOrdinal,
      driverTurnRefDigest: submission.driverTurnRefDigest,
      mode: isSteer
        ? { kind: "steer", expectedTurnId: expectedTurnId as TurnId }
        : { kind: "ordinary" },
      bindingDigest,
      // The ordered seven-member durable admission preimage. Every value is the
      // authoritative one already in scope at this call site (the same set the
      // per-contribution binding digest was built over above); admission
      // brand-asserts, pairwise-distinctness-checks, joins them against the
      // stored delivery/attempt fence + authenticated source, and re-binds them
      // via the sole SSOT recompute before any mutation.
      deliveryId: submission.delivery.deliveryId,
      attempt: submission.delivery.attempt,
      invocationId: invocation.invocationId,
      invocationGeneration: invocation.invocationGeneration,
      permitId: submission.permitId,
      runtimeWriteId,
      visibilityEventId,
      expected: expectedDurable,
      next,
      recordedAt: now,
    });
    const admittedDurable = admitted.durable;

    const binding = this.#binding(
      submission,
      turnId,
      inputOrdinal,
      invocation,
      runtimeWriteId,
      visibilityEventId,
      isSteer,
      expectedTurnId,
    );

    // (3) driver write effect. The adapter writes the invocation-entry chain and
    // returns the 4-way outcome. On a throw AFTER admission, settle the durable
    // row to terminal_error so journal and memory do not diverge (crash-repairable).
    let outcome: NativeProcessWriteOutcome;
    try {
      outcome = isSteer
        ? await this.#driver.steerTurn(submission.compiled, {
            ...binding,
            expectedTurnId: expectedTurnId as TurnId,
          })
        : await this.#driver.startTurn(submission.compiled, binding);
    } catch (cause) {
      this.#settle(turnId, inputOrdinal, admittedDurable, "terminal_error");
      throw cause;
    }

    // (4) branch on the 4-way NativeProcessWriteOutcome union.
    if (outcome.kind === "rejected_before_write") {
      // No input/notice/visibility boundary. Safe retry: settle terminal_error,
      // publish NO active turn.
      this.#settle(turnId, inputOrdinal, admittedDurable, "terminal_error");
      return { kind: "rejected_before_write" };
    }
    if (outcome.kind === "not_written") {
      // Proven negative: settle terminal_error and bump the active generation so a
      // late event from this generation is STALE.
      this.#settle(turnId, inputOrdinal, admittedDurable, "terminal_error");
      this.#activeGeneration = nextGeneration;
      return { kind: "next_generation" };
    }
    if (outcome.kind === "ambiguous") {
      // Hold: settle ambiguous; no further driver call.
      this.#settle(turnId, inputOrdinal, admittedDurable, "ambiguous");
      this.#ambiguousHold = true;
      this.#activeGeneration = nextGeneration;
      return { kind: "held_ambiguous" };
    }

    // written: the adapter ECHOES the preallocated runtimeWriteId +
    // visibilityEventId. Verify the exact echo (fail-closed, settling terminal_
    // error) and carry the preallocated ids end-to-end (binding invariant #1).
    if (
      outcome.runtimeWriteId !== runtimeWriteId ||
      outcome.visibilityEventId !== visibilityEventId
    ) {
      this.#settle(turnId, inputOrdinal, admittedDurable, "terminal_error");
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "runtime_id_echo_mismatch");
    }
    this.#activeGeneration = nextGeneration;
    const contribution: ActiveContribution = {
      turnId,
      inputOrdinal,
      invocation,
      permitId: submission.permitId,
      delivery: submission.delivery,
      receipts: { runtimeWriteId, visibilityEventId },
      bindingDigest,
      phase: "write_started",
      replyStaged: false,
      pendingReply: undefined,
      pendingCoordination: null,
      pendingCoordinationRecord: undefined,
      steerable: false,
      durable: admittedDurable,
    };
    this.#active = contribution;
    this.#seenTurnIds.add(turnId as unknown as string);
    return { kind: "written", turnId, inputOrdinal, receipts: contribution.receipts };
  }

  #settle(
    turnId: TurnId,
    inputOrdinal: number,
    expected: DurableTurnState,
    kind: "terminal_error" | "interrupted" | "ambiguous",
  ): DurableTurnState {
    const result = this.#journal.settleTurnContribution({
      ...this.#fence(),
      protocolTurnId: turnId,
      inputOrdinal,
      kind,
      expected,
      next: {
        protocolTurnId: turnId,
        phase: kind,
        inputOrdinal,
        bindingDigest: expected.bindingDigest,
        steerable: false,
        replyCommitted: expected.replyCommitted,
      },
      recordedAt: this.#ids.now(),
    });
    return result.durable;
  }

  #binding(
    submission: TurnSubmission,
    turnId: TurnId,
    inputOrdinal: number,
    invocation: NativeInvocationFence,
    runtimeWriteId: CommandId,
    visibilityEventId: CommandId,
    isSteer: boolean,
    expectedTurnId: TurnId | undefined,
  ): DriverTurnBinding {
    return {
      protocolTurnId: turnId,
      rootProducerFactId: submission.rootProducerFactId,
      inputOrdinal,
      mode:
        isSteer && expectedTurnId !== undefined
          ? { kind: "steer", expectedTurnId }
          : { kind: "ordinary" },
      driverTurnRefDigest: submission.driverTurnRefDigest,
      delivery: submission.delivery,
      invocation,
      permitId: submission.permitId,
      runtimeWriteId,
      visibilityEventId,
      inputDigest: submission.inputDigest,
    };
  }

  // -- driver events: validate-then-commit ------------------------------------

  /**
   * Apply one in-order driver event. Fix #1: the validate-pure step runs BEFORE
   * the durable commit; local state mutates ONLY when the port returns
   * applied:true. Fix #3: a stale/wrong-turn/rejected event advances ZERO cursor
   * ordinal (returns ignored_no_cursor_advance), so a rejected event moves no
   * ordinal and unchanged-sibling proofs hold. An exact replay ALIASES the port's
   * applied:false (no throw, no double effect).
   */
  applyEvent(input: TurnEventInput): ApplyEventOutcome {
    if (this.#readerEpoch === null) turnFail("DRIVER_EVENT_FENCE_MISMATCH", "reader_not_claimed");
    const event = input.event;

    const turnRef = this.#turnRef(event);
    if (turnRef !== undefined) {
      const decision = this.#gateTurnEvent(turnRef);
      if (decision.kind === "ignore") {
        return { kind: "ignored_no_cursor_advance", reason: decision.reason };
      }
    }

    // Non-cursor events (runtime lifecycle) that carry no turn ref settle/observe
    // without touching the driver-event cursor.
    if (turnRef === undefined) {
      this.#reduceLifecycle(event);
      return {
        kind: "advanced",
        nextOrdinal: this.#lastNextOrdinal,
        lastEventDigest: this.#lastEventDigest,
        committed: false,
      };
    }

    // Turn-referencing events: fix #4 fence check happens INSIDE #reduceTurn
    // (validate-pure) BEFORE the durable commit. The commit is the promoted
    // commitTurnStep / commitTurnTerminal which advances BOTH the cursor and the
    // durable turn state atomically (binding invariant chain-anchored). The
    // ACK-facing result surfaces the authoritative cursor PAIR (nextOrdinal +
    // lastEventDigest), tracked from the port's returned cursor.
    const result = this.#reduceTurn(input, turnRef);
    return {
      kind: "advanced",
      nextOrdinal: result.nextOrdinal,
      lastEventDigest: this.#lastEventDigest,
      committed: result.applied,
    };
  }

  #lastNextOrdinal = 0;
  /**
   * The authoritative last-committed driver-event digest, tracked alongside
   * `#lastNextOrdinal`. Adopted from every commit/claim/recovery cursor pair so a
   * downstream ACK can carry the exact current cursor rather than reconstructing it.
   */
  #lastEventDigest: ArtifactDigest | null = null;

  #turnRef(event: NormalizedDriverEvent): TurnId | undefined {
    switch (event.kind) {
      case "input_written":
      case "model_visible":
      case "turn_boundary":
      case "assistant_reply":
      case "coordination_call":
      case "turn_completed":
      case "turn_started":
        return event.turnId;
      case "runtime_ready":
      case "runtime_terminal":
        return undefined;
      default:
        return undefined;
    }
  }

  #gateTurnEvent(
    turnRef: TurnId,
  ): { kind: "apply" } | { kind: "ignore"; reason: "stale_turn" | "wrong_turn" } {
    const active = this.#active;
    if (active === null) return { kind: "ignore", reason: "stale_turn" };
    if (active.turnId !== turnRef) return { kind: "ignore", reason: "wrong_turn" };
    return { kind: "apply" };
  }

  #record(input: TurnEventInput, active: ActiveContribution): TurnEventRecordInput {
    // The per-contribution bindingDigest fence rides on every recorded event
    // (binding-fence invariant). The REQUIRED source binding is used
    // DIRECTLY (no fallback to the active contribution's binding). An event whose
    // bindingDigest does not equal the active contribution's fence is an
    // old-producer/late binding -> rejected, NEVER reattributed.
    if (input.bindingDigest !== active.bindingDigest) {
      turnFail("WRITE_STARTED_BINDING_MISMATCH", "late_or_old_binding");
    }
    return {
      ordinal: input.ordinal,
      eventDigest: input.eventDigest,
      turnId: active.turnId,
      bindingDigest: input.bindingDigest,
    };
  }

  /**
   * Reduce a turn-referencing event. Validate-pure gates run FIRST (fence,
   * boundary predecessors, exact receipt ids); ONLY after they pass is the
   * promoted commit invoked, and local state mutates ONLY when applied:true.
   */
  #reduceTurn(
    input: TurnEventInput,
    turnRef: TurnId,
  ): { applied: boolean; nextOrdinal: number } {
    const event = input.event;
    const active = this.#requireActive(turnRef);

    switch (event.kind) {
      case "turn_started": {
        if (input.ordinal < this.#lastNextOrdinal) {
          return this.#replayConsumedStep(input, active, "turn_started");
        }
        // The impl records turn_started as a non-mutating step commit (phase
        // unchanged, still write_started). It advances the cursor ordinal.
        const step = this.#commitStep(input, active, "turn_started", active.durable);
        return { applied: step.applied, nextOrdinal: step.nextOrdinal };
      }

      case "turn_boundary": {
        if (input.ordinal < this.#lastNextOrdinal) {
          return this.#replayConsumedStep(input, active, "turn_boundary");
        }
        const nextSteerable = event.steerable && active.phase === "model_visible";
        const next: DurableTurnState = { ...active.durable, steerable: nextSteerable };
        const step = this.#commitStep(input, active, "turn_boundary", next);
        if (step.applied) active.steerable = nextSteerable;
        return { applied: step.applied, nextOrdinal: step.nextOrdinal };
      }

      case "input_written": {
        // binding invariant #1 fence: exact per-contribution runtimeWriteId.
        if (event.runtimeWriteId !== active.receipts.runtimeWriteId) {
          turnFail("DRIVER_EVENT_FENCE_MISMATCH", "runtimeWriteId");
        }
        // Replay identity is PORT-AUTHORITATIVE. Any already-
        // consumed ordinal (byte-identical replay OR a divergent event at the same
        // ordinal) is routed to the port, which either aliases applied:false (exact
        // operationDigest match) or fails ACTIVE_TURN_CONFLICT (occupied ordinal,
        // divergent digest). ZERO local/cursor mutation on either outcome.
        if (input.ordinal < this.#lastNextOrdinal) {
          return this.#replayConsumedStep(input, active, "input_written");
        }
        if (active.phase !== "write_started") {
          turnFail("INVOCATION_STATE_CONFLICT", { phase: active.phase, at: "input_written" });
        }
        const next: DurableTurnState = { ...active.durable, phase: "input_written" };
        const step = this.#commitStep(input, active, "input_written", next);
        if (step.applied) active.phase = "input_written";
        return { applied: step.applied, nextOrdinal: step.nextOrdinal };
      }

      case "model_visible": {
        if (event.visibilityEventId !== active.receipts.visibilityEventId) {
          turnFail("DRIVER_EVENT_FENCE_MISMATCH", "visibilityEventId");
        }
        if (input.ordinal < this.#lastNextOrdinal) {
          return this.#replayConsumedStep(input, active, "model_visible");
        }
        if (active.phase !== "input_written") {
          turnFail("INVOCATION_STATE_CONFLICT", { phase: active.phase, at: "model_visible" });
        }
        const next: DurableTurnState = {
          ...active.durable,
          phase: "model_visible",
          steerable: true,
        };
        const step = this.#commitStep(input, active, "model_visible", next);
        if (step.applied) {
          active.phase = "model_visible";
          active.steerable = true;
        }
        return { applied: step.applied, nextOrdinal: step.nextOrdinal };
      }

      case "assistant_reply": {
        // reply-before-visible fails closed.
        if (active.phase !== "model_visible") {
          turnFail("ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE", { phase: active.phase });
        }
        // Exactly-one assistant reply per LOGICAL turn.
        // The one-reply fence checks BOTH the in-memory STAGED reply AND the
        // durable committed reply — a steer contribution INHERITS the logical-
        // turn staged/committed reply closure and never reopens it. A second
        // reply (same contribution OR post-steer) is MULTIPLE_ASSISTANT_REPLIES.
        if (active.replyStaged || active.durable.replyCommitted) {
          turnFail("MULTIPLE_ASSISTANT_REPLIES");
        }
        // The reply is NOT a cursor step at this module's layer: it is a member of the
        // STAGED terminal basis. Record it in memory (bound early). Lane D makes
        // the single commitTurnTerminal call; this module leaves the cursor UNCOMMITTED
        // for the reply/coordination/completed ordinals.
        active.pendingReply = this.#record(input, active);
        active.replyStaged = true;
        return { applied: true, nextOrdinal: this.#lastNextOrdinal };
      }

      case "coordination_call": {
        // Validate-before-mutate: validate EVERY turn/binding/ordinal/state fence BEFORE
        // touching any internal field. A wrong-binding (or pre-reply, or second)
        // coordination_call must leave machine/journal/cursor/queue state byte-
        // equivalent, so #record (the binding/turn fence) and all predecessor
        // gates run FIRST; only after they all pass are the fields set.
        if (!active.replyStaged) {
          turnFail("ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE", "coordination_before_reply");
        }
        if (active.pendingCoordination !== null) {
          turnFail("SECOND_COORDINATION_CALL");
        }
        // Validate-pure: the binding/turn fence (may throw WRITE_STARTED_BINDING_
        // MISMATCH) is computed BEFORE any field mutation.
        const record = this.#record(input, active);
        const command = deepFreeze(canonicalCommand(event.command));
        // All gates passed — NOW mutate (no partial state on any rejected path).
        active.pendingCoordination = { commandId: event.commandId, command, bound: null };
        active.pendingCoordinationRecord = record;
        return { applied: true, nextOrdinal: this.#lastNextOrdinal };
      }

      case "turn_completed": {
        return this.#stageTerminal(input, active);
      }

      default:
        return { applied: false, nextOrdinal: this.#lastNextOrdinal };
    }
  }

  /**
   * The promoted commitTurnStep: validate-pure CAS, cursor + durable advance
   * atomic, exact operationDigest replay aliases applied:false. On applied:true
   * the returned durable becomes the mirror; on applied:false (exact replay) the
   * mirror is aliased with no double effect.
   */
  #commitStep(
    input: TurnEventInput,
    active: ActiveContribution,
    kind: TurnStepKind,
    next: DurableTurnState,
  ): TurnStepResult {
    // The EXACT committed input. On a byte-identical replay this is re-sent so the
    // port recomputes the SAME operationDigest and aliases applied:false.
    const commit: CommitTurnStepInput = {
      ...this.#fence(),
      event: this.#record(input, active),
      kind,
      expected: { ...active.durable },
      next: { ...next },
      recordedAt: this.#ids.now(),
    };
    const result = this.#journal.commitTurnStep(commit);
    // Fix #1: mutate the local mirror only from the port's returned durable.
    active.durable = result.durable;
    this.#lastNextOrdinal = result.nextOrdinal;
    this.#lastEventDigest = result.lastEventDigest;
    if (result.applied) {
      // Cache the ORIGINAL committed input + its FULL identity AFTER
      // the port returns. A later same-ordinal event is a replay ONLY when the
      // entire identity matches; any divergence reaches the port and conflicts.
      this.#appliedSteps.set(input.ordinal, { identity: stepIdentity(commit), commit });
    }
    return result;
  }

  /** Ordinal -> the ORIGINAL committed input + its FULL canonical identity of
   * every APPLIED non-terminal step, for port-authoritative replay recognition. */
  #appliedSteps = new Map<number, { identity: string; commit: CommitTurnStepInput }>();

  /**
   * An already-consumed ordinal is PORT-AUTHORITATIVE. A byte-
   * identical replay re-sends the ORIGINAL committed input so the port recomputes
   * the SAME operationDigest and aliases applied:false. A DIVERGENT event at the
   * same ordinal (any changed eventDigest/bindingDigest/turn/kind/state-projection)
   * is sent freshly-projected so the port's occupied-ordinal check fails
   * ACTIVE_TURN_CONFLICT. ZERO local/cursor mutation on either outcome.
   */
  #replayConsumedStep(
    input: TurnEventInput,
    active: ActiveContribution,
    kind: TurnStepKind,
  ): { applied: boolean; nextOrdinal: number } {
    // A stale/old binding is rejected here too (required source binding), never reattributed.
    if (input.bindingDigest !== active.bindingDigest) {
      turnFail("WRITE_STARTED_BINDING_MISMATCH", "late_or_old_binding");
    }
    const candidateIdentity = stepIdentity({
      ...this.#fence(),
      event: {
        ordinal: input.ordinal,
        eventDigest: input.eventDigest,
        turnId: active.turnId,
        bindingDigest: input.bindingDigest,
      },
      kind,
      expected: { ...active.durable },
      next: { ...active.durable },
      recordedAt: "",
    });
    const prior = this.#appliedSteps.get(input.ordinal);
    if (prior !== undefined && prior.identity === candidateIdentity) {
      // Byte-identical replay: re-send the ORIGINAL input; the port aliases.
      const result = this.#journal.commitTurnStep(prior.commit);
      return { applied: result.applied, nextOrdinal: result.nextOrdinal };
    }
    // Divergent event at a consumed ordinal: reach the port with a freshly-
    // projected commit; the occupied-ordinal check fails ACTIVE_TURN_CONFLICT
    // BEFORE any CAS, so nothing mutates.
    const divergent: CommitTurnStepInput = {
      ...this.#fence(),
      event: {
        ordinal: input.ordinal,
        eventDigest: input.eventDigest,
        turnId: active.turnId,
        bindingDigest: input.bindingDigest,
      },
      kind,
      expected: { ...active.durable },
      next: { ...active.durable, phase: active.durable.phase },
      recordedAt: this.#ids.now(),
    };
    const result = this.#journal.commitTurnStep(divergent);
    return { applied: result.applied, nextOrdinal: result.nextOrdinal };
  }

  /**
   * Build the terminal record (contribution + command-id-free stage) IN MEMORY
   * from the active contribution's terminal suffix. Pure: it VALIDATES + SNAPSHOTS
   * and returns the record WITHOUT writing any live field, so both the same-process
   * path (#stageTerminal) and the fresh-process recovery reduction (#reduceRetained
   * Into) obtain byte-identical terminal semantics from the same code.
   */
  #buildTerminalRecord(
    input: TurnEventInput,
    active: ActiveContribution,
    readerFence: {
      stateInstanceId: StateInstanceId;
      sessionId: SessionId;
      ownerToken: ArtifactDigest;
      readerEpoch: number;
    },
  ): TerminalRecord {
    // completion-without-reply fails closed.
    if (!active.replyStaged || active.pendingReply === undefined) {
      turnFail("TURN_COMPLETION_WITHOUT_REPLY");
    }
    const replyRecord = active.pendingReply;
    // The completed leg carries the SAME per-contribution bindingDigest fence
    // (rejecting a late/old binding) — validate-pure.
    const completedRecord = this.#record(input, active);

    // Build + verify the canonical ContributionBinding from the terminal
    // contribution's authoritative facts (the SSOT builder computes both digests;
    // the verifier re-checks fail-closed before it is exposed to Lane D).
    const contribution = verifyContributionBinding(
      buildContributionBinding({
        fence: active.delivery,
        stateInstanceId: this.#config.stateInstanceId,
        inputOrdinal: active.inputOrdinal,
        invocationId: active.invocation.invocationId,
        invocationGeneration: active.invocation.invocationGeneration,
        permitId: active.permitId,
        runtimeWriteId: active.receipts.runtimeWriteId,
        visibilityEventId: active.receipts.visibilityEventId,
      }),
    );

    // The exact `expected` (pre-terminal) + `next` (terminal) DurableTurnState
    // Lane D passes to the single commitTurnTerminal call.
    const expected: DurableTurnState = { ...active.durable };
    const next: DurableTurnState = {
      protocolTurnId: active.turnId,
      phase: "completed",
      inputOrdinal: active.inputOrdinal,
      bindingDigest: active.durable.bindingDigest,
      steerable: false,
      replyCommitted: true,
    };

    // The command-id-free terminal basis. The reply leg is command-id-free (Lane D
    // adds replyCommandId). The coordination leg carries the machine-side request
    // commandId + commandDigest = protocolDigest(canonical requested command). The
    // completed leg is command-id-free.
    const basis: TurnTerminalDraftBasis = {
      reply: { ...replyRecord },
      ...(active.pendingCoordination !== null && active.pendingCoordinationRecord !== undefined
        ? {
            coordination: {
              ...active.pendingCoordinationRecord,
              commandId: active.pendingCoordination.commandId,
              commandDigest: protocolDigest(active.pendingCoordination.command),
            },
          }
        : {}),
      completed: { ...completedRecord },
    };

    const coordinationRequest: TurnCoordinationRequest =
      active.pendingCoordination === null
        ? { kind: "not_requested", terminalTurnId: contribution.fence.turnId }
        : {
            kind: "requested",
            commandId: active.pendingCoordination.commandId,
            command: canonicalCommand(active.pendingCoordination.command),
          };

    // Snapshot IN MEMORY (no journal mutation). Carries the single fresh stage plus
    // the pendingCoordination for the post-server bind phase.
    return {
      pendingCoordination: active.pendingCoordination,
      contribution,
      stage: {
        contribution,
        coordinationRequest,
        basis,
        readerFence: { ...readerFence },
        expected,
        next,
      },
    };
  }

  /**
   * STAGE-ONLY terminal. this module does NOT call commitTurnTerminal. On turn_completed
   * it VALIDATES + SNAPSHOTS the terminal suffix IN MEMORY — the canonical
   * ContributionBinding, the command-id-free contiguous terminal basis (only the
   * reply leg is command-id-free), the reader fence, and the exact `expected` +
   * terminal `next` DurableTurnState — and LEAVES the cursor / local turn / native
   * attempt UNCOMMITTED. Lane D derives the replyCommandId, does the server reply
   * -> optional coordination -> builds + verifies TurnCompletionEvidence -> makes
   * the SINGLE commitTurnTerminal call. this module synthesizes NO reply/coordination
   * receipt, result digest, disposition, or placeholder evidence.
   */
  #stageTerminal(
    input: TurnEventInput,
    active: ActiveContribution,
  ): { applied: boolean; nextOrdinal: number } {
    this.#terminal = this.#buildTerminalRecord(input, active, this.#fence());
    // Clear the active contribution: the logical turn is staged-terminal. The
    // cursor is UNCOMMITTED for reply/coordination/completed — that is Lane D's
    // single commit. this module's return reports no cursor advance at its layer.
    this.#active = null;
    return { applied: false, nextOrdinal: this.#lastNextOrdinal };
  }

  #reduceLifecycle(event: NormalizedDriverEvent): void {
    switch (event.kind) {
      case "runtime_ready":
        return;
      case "runtime_terminal": {
        // Fix #3: PRESERVE any pending steer / queued ordinary delivery. Drop only
        // the active in-flight contribution, settling its durable row to
        // terminal_error; re-queued steers remain so a continuation is not starved.
        if (this.#active !== null) {
          this.#settle(
            this.#active.turnId,
            this.#active.inputOrdinal,
            this.#active.durable,
            "terminal_error",
          );
          this.#active = null;
        }
        return;
      }
      default:
        return;
    }
  }

  #requireActive(turnRef: TurnId | undefined): ActiveContribution {
    const active = this.#active;
    if (active === null || (turnRef !== undefined && active.turnId !== turnRef)) {
      turnFail("INVOCATION_STATE_CONFLICT", "active_missing");
    }
    return active;
  }

  // -- §7.9 narrow terminal binding port (TurnTerminalBindingPort) ------------

  /**
   * PIN 2: the SINGLE fresh, recursively deep-frozen terminal STAGE snapshot —
   * { contribution, coordinationRequest, basis, readerFence, expected, next } —
   * reconstructed together so a caller cannot mutate later truth and there is no
   * torn read of #active. This is PURE STAGING: this module commits nothing. Lane D
   * makes the single commitTurnTerminal call from this snapshot.
   */
  terminalDraft(): TurnTerminalStage {
    const terminal = this.#terminal;
    if (terminal === null) {
      turnFail("INVOCATION_STATE_CONFLICT", "turn_not_terminal");
    }
    const stage = terminal.stage;
    // Reconstruct fresh: the contribution through the protocol verifier, the
    // command-id-free basis + reader fence + expected/next as fresh copies. The
    // whole wrapper is recursively frozen.
    const contribution = verifyContributionBinding(stage.contribution);
    const coordinationRequest: TurnCoordinationRequest =
      stage.coordinationRequest.kind === "not_requested"
        ? { kind: "not_requested", terminalTurnId: contribution.fence.turnId }
        : {
            kind: "requested",
            commandId: stage.coordinationRequest.commandId,
            command: canonicalCommand(stage.coordinationRequest.command),
          };
    const basis: TurnTerminalDraftBasis = {
      reply: { ...stage.basis.reply },
      ...(stage.basis.coordination !== undefined
        ? { coordination: { ...stage.basis.coordination } }
        : {}),
      completed: { ...stage.basis.completed },
    };
    return deepFreeze({
      contribution,
      coordinationRequest,
      basis,
      readerFence: { ...stage.readerFence },
      expected: { ...stage.expected },
      next: { ...stage.next },
    });
  }

  /**
   * POST-SERVER phase (Lane D drives). this module does NOT commit through this or
   * synthesize server evidence; it only records the bound disposition the caller
   * supplies for later exact-replay/read symmetry.
   */
  bindCoordinationResult(
    disposition: Extract<TurnCoordinationDisposition, { kind: "committed" | "terminal_replay" }>,
  ): TurnTerminalResult {
    const terminal = this.#terminal;
    if (terminal === null) {
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "coordination_command_id");
    }
    const pending = terminal.pendingCoordination;
    if (pending === null) {
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "coordination_command_id");
    }
    if (disposition.commandId !== pending.commandId) {
      turnFail("DRIVER_EVENT_FENCE_MISMATCH", "coordination_command_id");
    }
    const next: BoundCoordinationResult = {
      kind: disposition.kind,
      commandId: pending.commandId,
      receiptId: disposition.receiptId,
      resultDigest: disposition.resultDigest,
    };
    if (pending.bound !== null) {
      const prior = pending.bound;
      if (
        prior.kind !== next.kind ||
        prior.receiptId !== next.receiptId ||
        prior.resultDigest !== next.resultDigest
      ) {
        turnFail("DRIVER_EVENT_FENCE_MISMATCH", "coordination_result_conflict");
      }
    } else {
      pending.bound = next;
    }
    return this.terminalResult();
  }

  coordinationDisposition(): TurnCoordinationDisposition {
    const terminal = this.#terminal;
    if (terminal === null) {
      turnFail("INVOCATION_STATE_CONFLICT", "turn_not_terminal");
    }
    const pending = terminal.pendingCoordination;
    if (pending === null) {
      return { kind: "not_requested", terminalTurnId: terminal.contribution.fence.turnId };
    }
    if (pending.bound === null) {
      turnFail("INVOCATION_STATE_CONFLICT", "coordination_unresolved");
    }
    return {
      kind: pending.bound.kind,
      commandId: pending.bound.commandId,
      receiptId: pending.bound.receiptId,
      resultDigest: pending.bound.resultDigest,
    };
  }

  terminalResult(): TurnTerminalResult {
    const terminal = this.#terminal;
    if (terminal === null) {
      turnFail("INVOCATION_STATE_CONFLICT", "turn_not_terminal");
    }
    const coordination = this.coordinationDisposition();
    const contribution = verifyContributionBinding(terminal.contribution);
    return deepFreeze({ contribution, coordination });
  }

  // -- explicit generation gating exercise (§8.5b) ----------------------------

  /**
   * Assert that a driver event tagged with `eventGeneration` may act on the
   * active generation. Exercises the two DISTINCT §8.5b codes:
   *   - a strictly-older generation -> STALE_INVOCATION_GENERATION
   *   - the current generation while the active state conflicts -> INVOCATION_STATE_CONFLICT
   */
  assertGenerationCurrent(eventGeneration: number, conflictingState: DurableTurnState["phase"] | null): void {
    if (eventGeneration < this.#activeGeneration) {
      turnFail("STALE_INVOCATION_GENERATION", {
        eventGeneration,
        active: this.#activeGeneration,
      });
    }
    if (eventGeneration === this.#activeGeneration && conflictingState !== null) {
      turnFail("INVOCATION_STATE_CONFLICT", { generation: eventGeneration, conflictingState });
    }
  }

  // -- interrupt --------------------------------------------------------------

  async interrupt(expectedTurnId: TurnId): Promise<InterruptOutcome> {
    const active = this.#active;
    if (active === null) {
      if (this.#everSeen(expectedTurnId)) return { kind: "no_active_turn" };
      return { kind: "uncorrelated" };
    }
    if (active.turnId !== expectedTurnId) {
      return { kind: "uncorrelated" };
    }
    await this.#driver.interrupt(expectedTurnId);
    // Settle the interrupted contribution durably (interrupted phase).
    this.#settle(active.turnId, active.inputOrdinal, active.durable, "interrupted");
    this.#active = null;
    return { kind: "interrupted", turnId: expectedTurnId };
  }

  #everSeen(turnId: TurnId): boolean {
    return this.#seenTurnIds.has(turnId as unknown as string);
  }

  // -- draining ---------------------------------------------------------------

  /**
   * Drain the next queued ORDINARY contribution once the active turn has cleared.
   * The queue holds ONLY ordinary deliveries (unsafe steers are
   * NEVER enqueued — Lane D re-admits them as fresh ordinary submissions). Each
   * queued delivery admits via the ordinary path at its OWN delivery.turnId /
   * inputOrdinal 0 — this module never reuses a completed turnId. A FAILED admission
   * re-queues the item at the FRONT so nothing is lost (no-starve, FIFO). Peek-
   * then-admit-then-commit: the item is not removed until admission succeeds.
   */
  async drainNext(): Promise<SubmitOutcome | { kind: "empty" }> {
    if (this.#active !== null) turnFail("ACTIVE_TURN_CONFLICT", "still_active");
    if (this.#ambiguousHold) turnFail("AMBIGUOUS_NATIVE_WRITE");
    const next = this.#queue[0];
    if (next === undefined) return { kind: "empty" };
    // Do NOT shift/drain before a successful fresh admission: a
    // throwing admission must not lose the pending item.
    let outcome: SubmitOutcome;
    try {
      outcome = await this.#writeContribution(
        next.submission,
        next.submission.delivery.turnId,
        0,
        undefined,
        false,
        null,
      );
    } catch (cause) {
      // Admission failed: the item stays queued at the front (not lost, id never
      // reused). Lane D re-admits it later with a fresh ordinary fence.
      throw cause;
    }
    // Admission succeeded: NOW remove the item from the queue.
    this.#queue.shift();
    this.#seenTurnIds.add(next.submission.delivery.turnId as unknown as string);
    return outcome;
  }
}
