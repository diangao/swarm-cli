import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";

import type {
  ArtifactDigest,
  CommandId,
  DeliveryEnvelope,
  DeliveryFence,
  DeliveryId,
  DriverInputMode,
  DriverTurnBinding,
  LaunchId,
  MessageId,
  NormalizedDriverEvent,
  ProducerFactId,
  ProtocolVersion,
  ReceiptId,
  SessionId,
  StateInstanceId,
  Target,
  TurnId,
} from "@swarm/protocol";
import {
  buildContributionBinding,
  canonicalProtocolJson,
  parseDeliveryEnvelope,
  verifyTurnCompletionEvidence,
} from "@swarm/protocol";
import { protocolDigest } from "@swarm/runtime-contract";
import type { NativeProcessWriteOutcome } from "@swarm/drivers";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import {
  TurnError,
  TurnMachine,
  type AtomicTurnJournalPort,
  type BeginTurnContributionInput,
  type CommitTurnStepInput,
  type CommitTurnTerminalInput,
  type DriverEventReaderClaim,
  type DriverEventReaderPort,
  type DurableTurnState,
  type FreshProcessRecoveryPorts,
  type MachineConfig,
  type ReadDurableTurnStateInput,
  type RecoveryCompositeClaimCloseResult,
  type RecoveryCompositeCursorClaim,
  type RecoveryCursorAuthority,
  type RecoveryLiveResumeAuthorization,
  type RecoveryLiveResumeTicket,
  type ReplayableTurnRecoveryBasis,
  type RetainedEventLease,
  type RetainedReplayExpectation,
  type RetainedTurnEventRecord,
  type RetainedTurnEventSource,
  type RetainedTurnReplay,
  type SettleTurnContributionInput,
  type TurnCommandIdSource,
  type TurnDriverPort,
  type TurnEventInput,
  type TurnLiveResumePort,
  type TurnMutationResult,
  type TurnRecoveryClaimPort,
  type TurnRecoveryReadPort,
  type TurnRecoveryReadResult,
  type TurnStepResult,
  type TurnSubmission,
} from "../src/turn/index.js";

const version = 1 as ProtocolVersion;
const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
const digest = (fill: string): ArtifactDigest => `sha256:${fill.repeat(64)}` as ArtifactDigest;

function opaque(prefix: string, value: number): string {
  return `${prefix}_${alphabet[value % alphabet.length]?.repeat(26)}`;
}

// Replicated EXACTLY from the promoted `assertProtocolId` ID_PATTERN
// (packages/storage/src/protocol.ts): a valid prefix + `_` + exactly 26
// Crockford-base32 chars `[0-9a-hjkmnp-tv-z]` (no i/l/o/u). Every `opaque(...)`
// fixture (26 repeated alphabet chars) and every `Ids`-minted id (26 padded
// digits) satisfies this; malformed correct-prefix ids fail closed.
const ADMISSION_ID_PATTERN =
  /^(srv|mch|agt|hum|chn|cvs|msg|dlv|fac|tsk|clm|lse|lnc|cmd|rcp|sti|trn|ses)_[0-9a-hjkmnp-tv-z]{26}$/u;

function canonicalDigest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(JSON.stringify(sortDeep(value))).digest("hex")}` as ArtifactDigest;
}

function sortDeep(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortDeep);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortDeep((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

// A deterministic id/clock source. Ids never repeat across the whole run.
class Ids implements TurnCommandIdSource {
  #next = 1;
  #clock = 0;
  nextCommandId(): CommandId {
    return `cmd_${String(this.#next++).padStart(26, "0")}` as CommandId;
  }
  now(): string {
    this.#clock += 1;
    return `2026-08-04T00:00:${String(this.#clock).padStart(2, "0")}.000Z`;
  }
}

function durableEqual(left: DurableTurnState, right: DurableTurnState): boolean {
  return (
    left.protocolTurnId === right.protocolTurnId &&
    left.phase === right.phase &&
    left.inputOrdinal === right.inputOrdinal &&
    left.bindingDigest === right.bindingDigest &&
    left.steerable === right.steerable &&
    left.replyCommitted === right.replyCommitted
  );
}

// ---------------------------------------------------------------------------
// Shared native-attempt store. The driver (native adapter) populates the
// invocation-entry chain (permit_recorded -> write_started -> input_written ->
// model_visible) with the four distinct ids on a `written` outcome; the journal
// re-joins it at commitTurnTerminal (binding invariant #1). This mirrors the fact
// that in the real system both seams share the same durable store.
// ---------------------------------------------------------------------------

type EntryChain = {
  turnId: TurnId;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
  permitId: CommandId;
  invocationId: CommandId;
  invocationGeneration: number;
  inputDigest: ArtifactDigest;
  // The STORED delivery fence (attempt.fence_json ground truth). Evidence is
  // verified against THIS, never against the evidence's own fence (stored-fence trust root).
  storedFence: DeliveryFence;
  // permit_recorded is sequence 1 with previousEntryDigest === null; every
  // subsequent entry sequence === prev.sequence+1 (binding invariant #3, anchor).
  sequences: readonly number[];
  anchored: boolean;
  // Attempt lifecycle: model_visible -> consumed (advanced legally by Lane D).
  attemptState: "model_visible" | "consumed";
};

// A stored delivery/attempt row — the admission trust root. In the real system
// the permit / acquire-consume flow writes this pending_deliveries row (with its
// canonical stored delivery fence + authenticated source envelope) BEFORE
// admission; admission joins against it and never trusts a caller-supplied
// fence. The four invocation/permit members are left absent so the same stored
// row can serve both an ordinary admission and a later same-delivery steer.
//
// Mirroring the promoted `#requireAuthenticatedSourceMessage`
// (packages/storage/src/sqlite/runtime-journal.ts:1476-1578): the row stores the
// full envelope FACTS (messageId, target, producerFactId, agentId, machineId,
// serverSeq, optional expectedLaunchId/replayOf) PLUS a precomputed
// `envelopeDigest` (sha256 over canonicalProtocolJson of the canonical envelope)
// and a `targetKey` (a canonical projection of the target). The source join
// reconstructs the envelope from these stored columns, recomputes the digest +
// target key, requires BOTH to equal the stored values, and only then returns the
// reconstructed messageId — tampering any stored column that is not reflected in
// the stored digest is rejected with zero mutation.
type StoredDeliveryEnvelope = {
  messageId: MessageId;
  target: Target;
  producerFactId: ProducerFactId;
  agentId: string;
  machineId: string;
  serverSeq: number;
  expectedLaunchId?: LaunchId;
  replayOf?: DeliveryId;
};

type DeliveryLedgerRow = {
  storedFence: DeliveryFence;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  launchId: LaunchId;
  // The full stored envelope facts (never trusted until reconstructed +
  // digest-verified below).
  envelope: StoredDeliveryEnvelope;
  // Precomputed sha256 over canonicalProtocolJson of the canonical envelope
  // (the promoted digestBytes path). The join recomputes and requires equality.
  envelopeDigest: string;
  // Canonical projection of the target (promoted canonicalTargetKey path). The
  // join recomputes and requires equality.
  targetKey: string;
};

// The stored delivery/attempt ledger keyed by (deliveryId::attempt). The
// `submission()` fixture seeds the pre-admission row exactly as the permit flow
// would; the fake journal joins against it before any admission mutation.
const deliveryLedger = new Map<string, DeliveryLedgerRow>();

function deliveryLedgerKey(deliveryId: DeliveryId, attempt: number): string {
  return `${deliveryId}::${attempt}`;
}

// The canonical DeliveryEnvelope the promoted reconstruction rebuilds from the
// stored columns. attempt===1 forbids replayOf; attempt>1 requires it (mirroring
// parseDeliveryEnvelope's INVARIANT_VIOLATION guard).
function canonicalEnvelope(
  deliveryId: DeliveryId,
  attempt: number,
  env: StoredDeliveryEnvelope,
): DeliveryEnvelope {
  const candidate = {
    protocolVersion: 1,
    deliveryId,
    attempt,
    messageId: env.messageId,
    target: env.target,
    serverSeq: env.serverSeq,
    producerFactId: env.producerFactId,
    agentId: env.agentId,
    machineId: env.machineId,
    ...(env.expectedLaunchId === undefined ? {} : { expectedLaunchId: env.expectedLaunchId }),
    ...(env.replayOf === undefined ? {} : { replayOf: env.replayOf }),
  };
  // parseDeliveryEnvelope validates + freezes; canonicalProtocolJson normalizes.
  return parseDeliveryEnvelope(canonicalProtocolJson(candidate), version);
}

// sha256:<hex> over canonicalProtocolJson(envelope) — the promoted digestBytes path.
function envelopeDigestBytes(envelope: DeliveryEnvelope): string {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(envelope)).digest("hex")}`;
}

// A canonical projection of the target (promoted canonicalTargetKey path): the
// canonical JSON of the reconstructed, revalidated target alone.
function targetKeyOf(target: Target): string {
  return new TextDecoder().decode(
    canonicalProtocolJson(
      canonicalEnvelope(opaque("dlv", 1) as DeliveryId, 1, {
        messageId: opaque("msg", 9) as MessageId,
        target,
        producerFactId: opaque("fac", 1) as ProducerFactId,
        agentId: opaque("agt", 1),
        machineId: opaque("mch", 1),
        serverSeq: 1,
      }).target,
    ),
  );
}

function defaultStoredEnvelope(): StoredDeliveryEnvelope {
  return {
    messageId: opaque("msg", 9) as MessageId,
    target: { kind: "direct", conversationId: opaque("cvs", 1) as never },
    producerFactId: opaque("fac", 1) as ProducerFactId,
    agentId: opaque("agt", 1),
    machineId: opaque("mch", 1),
    serverSeq: 1,
  };
}

function seedDelivery(
  fenceValue: DeliveryFence,
  envelope: StoredDeliveryEnvelope = defaultStoredEnvelope(),
): void {
  // Single-source the producer fact: the envelope's producerFactId IS the stored
  // fence's producer fact (one datum), so a legitimate row cannot silently carry a
  // divergent envelope producer fact and the expected-facts join is authoritative.
  const sourced: StoredDeliveryEnvelope = { ...envelope, producerFactId: fenceValue.producerFactId };
  const canonical = canonicalEnvelope(fenceValue.deliveryId, fenceValue.attempt, sourced);
  deliveryLedger.set(deliveryLedgerKey(fenceValue.deliveryId, fenceValue.attempt), {
    storedFence: { ...fenceValue },
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    sessionId: fenceValue.sessionId,
    launchId: fenceValue.launchId,
    envelope: { ...sourced },
    envelopeDigest: envelopeDigestBytes(canonical),
    targetKey: targetKeyOf(canonical.target),
  });
}

class NativeAttemptStore {
  readonly chains = new Map<string, EntryChain>();
  // The completion row keyed by (deliveryId::attempt) — the replay-alias identity
  // is this row's operationDigest alone (terminal replay-alias identity).
  readonly completions = new Map<string, string>();
  // The pre-admission stored delivery/attempt rows shared with the fixture ledger.
  readonly deliveries = deliveryLedger;

  record(binding: DriverTurnBinding): void {
    // Root-anchored chain: permit_recorded(1) -> write_started(2) ->
    // input_written(3) -> model_visible(4). write_started.inputDigest ===
    // binding.inputDigest (binding invariant #2).
    this.chains.set(binding.protocolTurnId as unknown as string, {
      turnId: binding.protocolTurnId,
      runtimeWriteId: binding.runtimeWriteId,
      visibilityEventId: binding.visibilityEventId,
      permitId: binding.permitId,
      invocationId: binding.invocation.invocationId,
      invocationGeneration: binding.invocation.invocationGeneration,
      inputDigest: binding.inputDigest,
      storedFence: { ...binding.delivery },
      sequences: [1, 2, 3, 4],
      anchored: true,
      attemptState: "model_visible",
    });
  }

  join(turnId: TurnId): EntryChain | undefined {
    return this.chains.get(turnId as unknown as string);
  }

  completionKey(fence: DeliveryFence): string {
    return `${fence.deliveryId}::${fence.attempt}`;
  }
}

// ---------------------------------------------------------------------------
// In-memory AtomicTurnJournalPort faithfully enforcing the promoted contract:
//   - #requireTurnReaderFence: session/ownerToken/readerEpoch CAS on the cursor
//   - full-state CAS on `expected` (both sides of the commit boundary)
//   - operationDigest-style exact-replay aliasing (applied:false, no double effect)
//   - commitTurnStep: cursor-ordinal CAS + atomic durable+cursor advance
//   - commitTurnTerminal: evidence-verify + contribution-join + chain-anchor
//     BEFORE the replay-alias check; commits reply/coordination?/completed ordinals
//   - settleTurnContribution / readDurableTurnState
// It records every mutation in `log` and exposes the durable cursor ordinal.
// ---------------------------------------------------------------------------

type StoredTurn = {
  durable: DurableTurnState;
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  operationDigest: string;
};

class MemoryTurnJournal implements AtomicTurnJournalPort {
  readonly log: string[] = [];
  readonly turns = new Map<string, StoredTurn>();
  readonly records = new Map<number, string>();
  readonly attempts: NativeAttemptStore;
  // STAGE-ONLY guard: this module must NEVER drive commitTurnTerminal. Any call the
  // machine makes increments this; the tests assert it stays 0 on the machine
  // path. A Lane-D-simulation test calls it DIRECTLY to prove the staged snapshot
  // commits, tracked separately.
  terminalCommitCalls = 0;
  // Per-stateInstanceId cursor store — the lock-owning claim state machine keyed
  // by state_instance_id (mirrors the promoted driver_event_cursor table). Each
  // row carries an owner (`owner` non-null == live/owned; null == released) plus
  // the authoritative cursor pair. `#cursor` points to the CURRENTLY-owned cursor
  // the commit boundary fences against.
  readonly #cursors = new Map<
    string,
    {
      stateInstanceId: StateInstanceId;
      sessionId: SessionId;
      owner: ArtifactDigest | null;
      readerEpoch: number;
      nextOrdinal: number;
      lastEventDigest: ArtifactDigest | null;
    }
  >();
  #cursor: {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    readerEpoch: number;
    nextOrdinal: number;
    lastEventDigest: ArtifactDigest | null;
  } | null = null;

  constructor(attempts: NativeAttemptStore) {
    this.attempts = attempts;
    // Per-test isolation: the delivery ledger is a shared fixture registry the
    // `submission()` factory seeds AFTER this journal is constructed. Clear it
    // here so each test starts from a clean pre-admission delivery store.
    deliveryLedger.clear();
  }

  get cursorNextOrdinal(): number {
    return this.#cursor?.nextOrdinal ?? 0;
  }

  get cursorLastEventDigest(): ArtifactDigest | null {
    return this.#cursor?.lastEventDigest ?? null;
  }

  turnState(turnId: TurnId): DurableTurnState | undefined {
    return this.turns.get(turnId as unknown as string)?.durable;
  }

  // The lock-owning claim STATE MACHINE, replicating the promoted
  // DaemonJournal.claimDriverEventReader (packages/storage/src/sqlite/journal.ts:
  // 644-719) branch-for-branch. Keyed by state_instance_id. Every illegal branch
  // fails closed with ZERO mutation (no epoch bump, no re-own, no `claim_reader`
  // log); DRIVER_EVENT_FENCE_MISMATCH carries a distinguishing detail string.
  #claim(input: DriverEventReaderClaim): {
    readerEpoch: number;
    nextOrdinal: number;
    lastEventDigest: ArtifactDigest | null;
  } {
    // assertProtocolId(stateInstanceId,"sti"), assertProtocolId(sessionId,"ses").
    if (!ADMISSION_ID_PATTERN.test(input.stateInstanceId) || !input.stateInstanceId.startsWith("sti_")) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "bad_state_instance_id");
    }
    if (!ADMISSION_ID_PATTERN.test(input.sessionId) || !input.sessionId.startsWith("ses_")) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "bad_session_id");
    }
    // mode must be "start" | "resume".
    if (input.mode !== "start" && input.mode !== "resume") {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "bad_mode");
    }
    const key = input.stateInstanceId as unknown as string;
    const existing = this.#cursors.get(key);
    if (existing === undefined) {
      // No existing cursor: mode MUST be "start" -> create a fresh owned cursor.
      if (input.mode !== "start") {
        throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "no_cursor_requires_start");
      }
      const created = {
        stateInstanceId: input.stateInstanceId,
        sessionId: input.sessionId,
        owner: input.ownerToken,
        readerEpoch: 1,
        nextOrdinal: 0,
        lastEventDigest: null as ArtifactDigest | null,
      };
      this.#cursors.set(key, created);
      this.#adoptCursor(created);
      this.log.push("claim_reader:1");
      return { readerEpoch: 1, nextOrdinal: 0, lastEventDigest: null };
    }
    // Existing cursor.
    if (existing.sessionId !== input.sessionId) {
      // Session mismatch -> conflict (fail closed).
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "session_mismatch");
    }
    if (input.mode !== "resume") {
      // A second "start" on an existing cursor -> conflict.
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "existing_start_conflict");
    }
    if (existing.owner !== null) {
      // An owner is still live -> resume overlap (fail closed).
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "resume_overlap");
    }
    // Released (owner null): epoch+1, re-own, RETURN the preserved cursor pair.
    existing.readerEpoch += 1;
    existing.owner = input.ownerToken;
    this.#adoptCursor(existing);
    this.log.push(`claim_reader:${existing.readerEpoch}`);
    return {
      readerEpoch: existing.readerEpoch,
      nextOrdinal: existing.nextOrdinal,
      lastEventDigest: existing.lastEventDigest,
    };
  }

  // Point the commit-boundary `#cursor` at the given store row and keep it
  // synchronized: commit-boundary advances (`commitTurnStep`/`commitTurnTerminal`)
  // write through `this.#cursor`, so mirror those advances back onto the store row
  // via `#syncCursor` after each mutation.
  #adoptCursor(row: {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    owner: ArtifactDigest | null;
    readerEpoch: number;
    nextOrdinal: number;
    lastEventDigest: ArtifactDigest | null;
  }): void {
    this.#cursor = {
      stateInstanceId: row.stateInstanceId,
      sessionId: row.sessionId,
      ownerToken: row.owner as ArtifactDigest,
      readerEpoch: row.readerEpoch,
      nextOrdinal: row.nextOrdinal,
      lastEventDigest: row.lastEventDigest,
    };
  }

  // Mirror the commit-boundary cursor-pair advance back onto the owning store row
  // so a later released-resume returns the up-to-date preserved pair.
  #syncCursor(): void {
    if (this.#cursor === null) return;
    const row = this.#cursors.get(this.#cursor.stateInstanceId as unknown as string);
    if (row === undefined) return;
    row.readerEpoch = this.#cursor.readerEpoch;
    row.nextOrdinal = this.#cursor.nextOrdinal;
    row.lastEventDigest = this.#cursor.lastEventDigest;
  }

  // TEST-ONLY: model a reader RELEASE (owner cleared, cursor pair preserved) so
  // the legal released-resume branch can be exercised. Syncs the live commit
  // cursor onto the store row first, then drops ownership.
  releaseReader(stateInstanceId: StateInstanceId): void {
    this.#syncCursor();
    const row = this.#cursors.get(stateInstanceId as unknown as string);
    if (row === undefined) throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "release_unknown_cursor");
    row.owner = null;
  }

  claimForReaderPort(input: DriverEventReaderClaim): {
    readerEpoch: number;
    nextOrdinal: number;
    lastEventDigest: ArtifactDigest | null;
  } {
    return this.#claim(input);
  }

  #requireFence(fence: {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    readerEpoch: number;
  }): { nextOrdinal: number; lastEventDigest: ArtifactDigest | null } {
    const cursor = this.#cursor;
    if (
      cursor === null ||
      cursor.stateInstanceId !== fence.stateInstanceId ||
      cursor.sessionId !== fence.sessionId ||
      cursor.ownerToken !== fence.ownerToken ||
      cursor.readerEpoch !== fence.readerEpoch
    ) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", fence.stateInstanceId);
    }
    return { nextOrdinal: cursor.nextOrdinal, lastEventDigest: cursor.lastEventDigest };
  }

  #assertDurableShape(state: DurableTurnState): void {
    if (!Number.isSafeInteger(state.inputOrdinal) || state.inputOrdinal < 0) {
      throw new TurnError("INVALID_STATE_TRANSITION", state.inputOrdinal);
    }
    if (typeof state.steerable !== "boolean" || typeof state.replyCommitted !== "boolean") {
      throw new TurnError("INVALID_STATE_TRANSITION", state.protocolTurnId);
    }
  }

  #requireExpected(row: StoredTurn, expected: DurableTurnState): void {
    // Full-state CAS: EVERY durable field must match (partial/divergent -> conflict).
    if (!durableEqual(row.durable, expected)) {
      throw new TurnError("ACTIVE_TURN_CONFLICT", expected.protocolTurnId);
    }
  }

  // ------------------------------------------------------------------------
  // Admission trust boundary. The promoted admission asserts the ordered
  // seven-member durable admission preimage BEFORE any mutation: brand each id,
  // range-check the two integers, require the four command ids pairwise
  // distinct, join against the stored delivery/attempt fence + authenticated
  // source, and re-bind via the sole SSOT contribution-binding recompute. Any
  // violation fails closed with ZERO mutation. Mirrored here field-for-field.
  // ------------------------------------------------------------------------

  // Promoted `assertProtocolId` (packages/storage/src/protocol.ts): the value must
  // match the full ID_PATTERN (a valid prefix + `_` + exactly 26 Crockford-base32
  // chars) AND start with the specific expected prefix. A malformed correct-prefix
  // id (short, or containing non-Crockford i/l/o/u chars) fails closed.
  #assertBrand(value: string, prefix: string, at: unknown): void {
    if (!ADMISSION_ID_PATTERN.test(value) || !value.startsWith(`${prefix}_`)) {
      throw new TurnError("INVALID_STATE_TRANSITION", at);
    }
  }

  #assertAdmissionPreimage(input: BeginTurnContributionInput): void {
    this.#assertBrand(input.deliveryId as unknown as string, "dlv", input.deliveryId);
    this.#assertBrand(input.invocationId as unknown as string, "cmd", input.deliveryId);
    this.#assertBrand(input.permitId as unknown as string, "cmd", input.deliveryId);
    this.#assertBrand(input.runtimeWriteId as unknown as string, "cmd", input.deliveryId);
    this.#assertBrand(input.visibilityEventId as unknown as string, "cmd", input.deliveryId);
    if (!Number.isSafeInteger(input.attempt) || input.attempt < 1 || input.attempt > 2147483647) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.deliveryId);
    }
    if (
      !Number.isSafeInteger(input.invocationGeneration) ||
      input.invocationGeneration < 1 ||
      input.invocationGeneration > 9007199254740991
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.deliveryId);
    }
    if (
      new Set([input.invocationId, input.permitId, input.runtimeWriteId, input.visibilityEventId])
        .size !== 4
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.deliveryId);
    }
  }

  // Faithful mirror of the promoted #requireAuthenticatedSourceMessage
  // (runtime-journal.ts:1496-1578): FIRST cross-check the stored source row's
  // facts {attempt, launchId, stateInstanceId, sessionId, turnId, producerFactId}
  // against the EXPECTED admission facts. The envelope's OWN producerFactId — the
  // datum reconstruction consumes and the digest is computed over — is part of that
  // join and is single-sourced with the stored fence, so a well-formed WRONG
  // producer fact with a consistently RECOMPUTED digest still fails here, not only
  // the stale-digest class. Only then is the full canonical DeliveryEnvelope
  // reconstructed, its canonical target key required to equal the stored target_key
  // AND its canonical digest to equal the stored envelope_digest, and the
  // RECONSTRUCTED messageId returned (never the caller's). Any facts /
  // reconstruction / target-key / digest mismatch fails closed with zero mutation.
  #requireAuthenticatedSource(
    row: DeliveryLedgerRow,
    deliveryId: DeliveryId,
    attempt: number,
    expected: {
      launchId: LaunchId;
      stateInstanceId: StateInstanceId;
      sessionId: SessionId;
      turnId: TurnId;
      producerFactId: ProducerFactId;
    },
  ): MessageId {
    // (a) Expected/source-facts equality BEFORE reconstruction. Both the stored
    //     fence's producer fact AND the envelope copy that feeds reconstruction
    //     must equal the expected root producer fact (they are single-sourced).
    if (
      row.storedFence.attempt !== attempt ||
      row.launchId !== expected.launchId ||
      row.stateInstanceId !== expected.stateInstanceId ||
      row.sessionId !== expected.sessionId ||
      row.storedFence.turnId !== expected.turnId ||
      row.storedFence.producerFactId !== expected.producerFactId ||
      row.envelope.producerFactId !== expected.producerFactId
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", deliveryId);
    }
    // (b) Reconstruct + target-key + digest.
    let reconstructed: DeliveryEnvelope;
    try {
      reconstructed = canonicalEnvelope(deliveryId, attempt, row.envelope);
    } catch {
      throw new TurnError("INVALID_STATE_TRANSITION", deliveryId);
    }
    if (reconstructed.producerFactId !== expected.producerFactId) {
      throw new TurnError("INVALID_STATE_TRANSITION", deliveryId);
    }
    if (targetKeyOf(reconstructed.target) !== row.targetKey) {
      throw new TurnError("INVALID_STATE_TRANSITION", deliveryId);
    }
    if (envelopeDigestBytes(reconstructed) !== row.envelopeDigest) {
      throw new TurnError("INVALID_STATE_TRANSITION", deliveryId);
    }
    return reconstructed.messageId;
  }

  beginTurnContribution(input: BeginTurnContributionInput): TurnMutationResult {
    this.#requireFence(input);
    this.#assertDurableShape(input.next);
    if (input.expected !== null) this.#assertDurableShape(input.expected);
    if (
      input.next.protocolTurnId !== input.protocolTurnId ||
      input.next.inputOrdinal !== input.inputOrdinal ||
      input.next.bindingDigest !== input.bindingDigest ||
      input.next.phase !== "write_started" ||
      input.next.steerable !== false ||
      input.next.replyCommitted !== false
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.protocolTurnId);
    }
    // Brand / range / pairwise-distinct asserts on the seven-member preimage.
    this.#assertAdmissionPreimage(input);
    if (input.mode.kind === "ordinary") {
      if (input.expected !== null || input.inputOrdinal !== 0) {
        throw new TurnError("INVALID_STATE_TRANSITION", input.protocolTurnId);
      }
    } else {
      if (
        input.expected === null ||
        input.mode.expectedTurnId !== input.protocolTurnId ||
        input.expected.protocolTurnId !== input.protocolTurnId ||
        input.expected.steerable !== true ||
        input.inputOrdinal < 1
      ) {
        throw new TurnError("INVALID_STATE_TRANSITION", input.protocolTurnId);
      }
    }
    // Stored delivery/attempt/fence join (the trust root; the caller never
    // supplies the stored fence). The pre-admission row must exist for
    // (deliveryId, attempt) and reconstruct the exact fence facts.
    const delivery = this.attempts.deliveries.get(
      deliveryLedgerKey(input.deliveryId, input.attempt),
    );
    if (delivery === undefined) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.deliveryId);
    }
    const storedFence = delivery.storedFence;
    if (
      storedFence.deliveryId !== input.deliveryId ||
      storedFence.attempt !== input.attempt ||
      storedFence.turnId !== input.protocolTurnId ||
      storedFence.producerFactId !== input.rootProducerFactId ||
      storedFence.launchId !== input.launchId ||
      storedFence.sessionId !== input.sessionId ||
      delivery.stateInstanceId !== input.stateInstanceId ||
      delivery.sessionId !== input.sessionId ||
      delivery.launchId !== input.launchId
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.deliveryId);
    }
    // Authenticated source join (mirrors the promoted
    // #requireAuthenticatedSourceMessage, runtime-journal.ts:1476-1578). The
    // stored source message id is NEVER taken from the caller: it is exposed only
    // after the full canonical DeliveryEnvelope is reconstructed from the stored
    // columns, its canonical target key revalidates against the stored target_key,
    // and its canonical digest equals the stored envelope_digest. Any
    // reconstruction / target-key / digest mismatch fails closed with zero
    // mutation, so tampering any stored envelope column that is not reflected in
    // the stored digest is rejected.
    const sourceMessageId = this.#requireAuthenticatedSource(delivery, input.deliveryId, input.attempt, {
      launchId: input.launchId,
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      turnId: input.protocolTurnId,
      producerFactId: input.rootProducerFactId,
    });
    void sourceMessageId; // reconstructed id (never the caller's) — the join is the gate
    // SSOT contribution-binding recompute over the STORED fence + the same
    // seven-member set; the caller-proposed binding digest must match.
    let recomputedBindingDigest: ArtifactDigest;
    try {
      recomputedBindingDigest = buildContributionBinding({
        fence: storedFence,
        stateInstanceId: input.stateInstanceId,
        inputOrdinal: input.inputOrdinal,
        invocationId: input.invocationId,
        invocationGeneration: input.invocationGeneration,
        permitId: input.permitId,
        runtimeWriteId: input.runtimeWriteId,
        visibilityEventId: input.visibilityEventId,
      }).contributionBindingDigest;
    } catch {
      throw new TurnError("WRITE_STARTED_BINDING_MISMATCH", input.protocolTurnId);
    }
    if (recomputedBindingDigest !== input.bindingDigest) {
      throw new TurnError("WRITE_STARTED_BINDING_MISMATCH", input.protocolTurnId);
    }
    // The operation-digest preimage INCLUDES all seven admission members, so an
    // exact-preimage replay aliases applied:false while any divergent preimage
    // member yields a distinct operation (conflict on an occupied turn).
    const operationDigest = canonicalDigest({
      method: "beginTurnContribution",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      protocolTurnId: input.protocolTurnId,
      launchId: input.launchId,
      rootProducerFactId: input.rootProducerFactId,
      inputOrdinal: input.inputOrdinal,
      driverTurnRefDigest: input.driverTurnRefDigest,
      mode: input.mode,
      bindingDigest: input.bindingDigest,
      deliveryId: input.deliveryId,
      attempt: input.attempt,
      invocationId: input.invocationId,
      invocationGeneration: input.invocationGeneration,
      permitId: input.permitId,
      runtimeWriteId: input.runtimeWriteId,
      visibilityEventId: input.visibilityEventId,
      expected: input.expected,
      next: input.next,
    });
    const existing = this.turns.get(input.protocolTurnId as unknown as string);
    if (existing !== undefined && existing.operationDigest === operationDigest) {
      // Exact operationDigest replay -> alias applied:false, no double effect.
      return { applied: false, durable: existing.durable };
    }
    if (input.mode.kind === "ordinary") {
      if (existing !== undefined) {
        throw new TurnError("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
      // Only one live turn per session.
      for (const row of this.turns.values()) {
        if (
          row.sessionId === input.sessionId &&
          ["write_started", "input_written", "model_visible"].includes(row.durable.phase)
        ) {
          throw new TurnError("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
        }
      }
    } else {
      if (
        existing === undefined ||
        existing.stateInstanceId !== input.stateInstanceId ||
        existing.sessionId !== input.sessionId ||
        existing.launchId !== input.launchId
      ) {
        throw new TurnError("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
      this.#requireExpected(existing, input.expected as DurableTurnState);
    }
    this.turns.set(input.protocolTurnId as unknown as string, {
      durable: { ...input.next },
      launchId: input.launchId,
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      operationDigest,
    });
    this.log.push(`begin:${input.mode.kind}:${input.inputOrdinal}`);
    return { applied: true, durable: { ...input.next } };
  }

  commitTurnStep(input: CommitTurnStepInput): TurnStepResult {
    const cursor = this.#requireFence(input);
    this.#assertDurableShape(input.expected);
    this.#assertDurableShape(input.next);
    const kinds = ["turn_started", "input_written", "model_visible", "turn_boundary"];
    if (!kinds.includes(input.kind)) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.kind);
    }
    if (
      input.event.turnId !== input.expected.protocolTurnId ||
      input.next.protocolTurnId !== input.expected.protocolTurnId ||
      input.next.inputOrdinal !== input.expected.inputOrdinal ||
      input.event.bindingDigest !== input.expected.bindingDigest ||
      input.next.replyCommitted !== input.expected.replyCommitted
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.event.turnId);
    }
    const operationDigest = canonicalDigest({
      method: "commitTurnStep",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      event: input.event,
      kind: input.kind,
      expected: input.expected,
      next: input.next,
    });
    const occupied = this.records.get(input.event.ordinal);
    if (occupied !== undefined) {
      if (occupied === operationDigest) {
        const row = this.turns.get(input.event.turnId as unknown as string);
        if (row === undefined) throw new TurnError("ACTIVE_TURN_CONFLICT", input.event.turnId);
        // Exact replay returns the CURRENT authoritative cursor pair from the
        // same reader-fenced row, not oldOrdinal + 1.
        return {
          applied: false,
          nextOrdinal: cursor.nextOrdinal,
          lastEventDigest: cursor.lastEventDigest,
          durable: row.durable,
        };
      }
      throw new TurnError("ACTIVE_TURN_CONFLICT", input.event.ordinal);
    }
    const row = this.turns.get(input.event.turnId as unknown as string);
    if (
      row === undefined ||
      row.stateInstanceId !== input.stateInstanceId ||
      row.sessionId !== input.sessionId
    ) {
      throw new TurnError("ACTIVE_TURN_CONFLICT", input.event.turnId);
    }
    this.#requireExpected(row, input.expected);
    if (cursor.nextOrdinal !== input.event.ordinal) {
      throw new TurnError("DRIVER_EVENT_ORDER_INVALID", {
        expected: cursor.nextOrdinal,
        actual: input.event.ordinal,
      });
    }
    // Atomic: durable + cursor pair advance together.
    this.records.set(input.event.ordinal, operationDigest);
    row.durable = { ...input.next };
    row.operationDigest = operationDigest;
    this.#cursor!.nextOrdinal = input.event.ordinal + 1;
    this.#cursor!.lastEventDigest = input.event.eventDigest;
    this.#syncCursor();
    this.log.push(`step:${input.kind}:${input.event.ordinal}`);
    return {
      applied: true,
      nextOrdinal: input.event.ordinal + 1,
      lastEventDigest: input.event.eventDigest,
      durable: { ...input.next },
    };
  }

  commitTurnTerminal(input: CommitTurnTerminalInput): TurnStepResult {
    this.terminalCommitCalls += 1;
    const cursor = this.#requireFence(input);
    this.#assertDurableShape(input.expected);
    this.#assertDurableShape(input.next);
    const members = [
      input.basis.reply,
      ...(input.basis.coordination ? [input.basis.coordination] : []),
      input.basis.completed,
    ];
    for (const member of members) {
      if (
        member.turnId !== input.expected.protocolTurnId ||
        member.bindingDigest !== input.expected.bindingDigest
      ) {
        throw new TurnError("INVALID_STATE_TRANSITION", member.turnId);
      }
    }
    if (!/^cmd_/u.test(input.basis.reply.replyCommandId as unknown as string)) {
      throw new TurnError("INVALID_STATE_TRANSITION", "reply_command_id");
    }
    if (input.basis.coordination !== undefined) {
      if (input.basis.coordination.commandId === input.basis.reply.replyCommandId) {
        throw new TurnError("SECOND_COORDINATION_CALL", input.basis.coordination.commandId);
      }
    }
    for (let index = 1; index < members.length; index += 1) {
      const current = members[index];
      const previous = members[index - 1];
      if (current === undefined || previous === undefined || current.ordinal !== previous.ordinal + 1) {
        throw new TurnError("DRIVER_EVENT_ORDER_INVALID", { at: index });
      }
    }
    if (
      input.next.phase !== "completed" ||
      input.next.steerable !== false ||
      input.next.replyCommitted !== true ||
      input.next.bindingDigest !== input.expected.bindingDigest ||
      input.next.inputOrdinal !== input.expected.inputOrdinal ||
      input.expected.replyCommitted !== false
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.expected.protocolTurnId);
    }

    // Mirror the promoted ATOMIC commitTurnTerminal EXACTLY
    // (runtime-journal.ts :1778-2082). Evidence trust root is the STORED fence
    // (attempt.fence_json), NEVER the evidence's own fence (:1849,:1859).
    const chain = this.attempts.join(input.expected.protocolTurnId);
    if (chain === undefined) {
      throw new TurnError("INVALID_JOURNAL_CHAIN", input.expected.protocolTurnId);
    }
    const storedFence = chain.storedFence; // JSON.parse(attempt.fence_json) ground truth
    try {
      verifyTurnCompletionEvidence(input.evidence, { expectedFence: storedFence });
    } catch {
      throw new TurnError("INVALID_JOURNAL_CHAIN", input.expected.protocolTurnId);
    }
    const contribution = input.evidence.contribution;

    // #requireTerminalEntryChain (:1405) runs at :1909 BEFORE BOTH the replay
    // alias AND the first insert — a corrupted stored chain cannot alias even when
    // the digest matches. Chain anchor: permit_recorded is seq 1, then contiguous.
    if (
      chain.sequences[0] !== 1 ||
      !chain.anchored ||
      chain.sequences.some((seq, i) => (i === 0 ? seq !== 1 : seq !== (chain.sequences[i - 1] ?? 0) + 1))
    ) {
      throw new TurnError("INVALID_JOURNAL_CHAIN", input.expected.protocolTurnId);
    }
    // Stored-truth join: evidence's runtime-write/visibility/permit/invocation
    // identities must equal the committed invocation-entry chain (:1918-1927).
    if (
      chain.runtimeWriteId !== contribution.runtimeWriteId ||
      chain.visibilityEventId !== contribution.visibilityEventId ||
      chain.permitId !== contribution.permitId ||
      chain.invocationId !== contribution.invocationId ||
      chain.invocationGeneration !== contribution.invocationGeneration
    ) {
      throw new TurnError("WRITE_STARTED_BINDING_MISMATCH", input.expected.protocolTurnId);
    }
    // Recompute the contribution binding from STORED facts + STORED fence alone
    // and compare to the evidence binding (:1928-1951).
    const rebuilt = buildContributionBinding({
      fence: storedFence,
      stateInstanceId: contribution.stateInstanceId,
      inputOrdinal: input.expected.inputOrdinal,
      invocationId: chain.invocationId,
      invocationGeneration: chain.invocationGeneration,
      permitId: chain.permitId,
      runtimeWriteId: chain.runtimeWriteId,
      visibilityEventId: chain.visibilityEventId,
    }).contributionBindingDigest;
    if (rebuilt !== contribution.contributionBindingDigest) {
      throw new TurnError("WRITE_STARTED_BINDING_MISMATCH", input.expected.protocolTurnId);
    }

    // operationDigest components EXACTLY per :1953-1961 — INCLUDE evidence;
    // EXCLUDE recordedAt, ownerToken, readerEpoch (nothing more).
    const operationDigest = canonicalDigest({
      method: "commitTurnTerminal",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      basis: input.basis,
      evidence: input.evidence,
      expected: input.expected,
      next: input.next,
    });
    const row = this.turns.get(input.expected.protocolTurnId as unknown as string);
    if (row === undefined) throw new TurnError("ACTIVE_TURN_CONFLICT", input.expected.protocolTurnId);

    // Replay alias keyed on the COMPLETION ROW for (deliveryId, attempt): digest-
    // equality alone -> {applied:false} EVEN AFTER the attempt advanced to consumed
    // (the first-insert predecessor gates do NOT run on the replay path). Any
    // divergence -> INVOCATION_STATE_CONFLICT, zero mutation.
    const completionKey = this.attempts.completionKey(storedFence);
    const existing = this.attempts.completions.get(completionKey);
    if (existing !== undefined) {
      if (existing === operationDigest) {
        return {
          applied: false,
          nextOrdinal: cursor.nextOrdinal,
          lastEventDigest: cursor.lastEventDigest,
          durable: { ...row.durable },
        };
      }
      throw new TurnError("INVOCATION_STATE_CONFLICT", input.expected.protocolTurnId);
    }
    // First-insert-only predecessor gates — run ONLY on the first insert (never on
    // the replay path above). Attempt must still be model_visible.
    if (chain.attemptState !== "model_visible") {
      throw new TurnError("INVALID_STATE_TRANSITION", "model_visible_predecessor_required");
    }
    this.#requireExpected(row, input.expected);
    if (cursor.nextOrdinal !== input.basis.reply.ordinal) {
      throw new TurnError("DRIVER_EVENT_ORDER_INVALID", {
        expected: cursor.nextOrdinal,
        actual: input.basis.reply.ordinal,
      });
    }
    for (const member of members) {
      this.records.set(member.ordinal, operationDigest);
    }
    const nextOrdinal = input.basis.completed.ordinal + 1;
    this.#cursor!.nextOrdinal = nextOrdinal;
    this.#cursor!.lastEventDigest = input.basis.completed.eventDigest;
    this.#syncCursor();
    row.durable = { ...input.next };
    row.operationDigest = operationDigest;
    // Persist the completion row + legally advance the attempt to consumed.
    this.attempts.completions.set(completionKey, operationDigest);
    chain.attemptState = "consumed";
    this.log.push(`terminal:${input.basis.completed.ordinal}`);
    return {
      applied: true,
      nextOrdinal,
      lastEventDigest: input.basis.completed.eventDigest,
      durable: { ...input.next },
    };
  }

  settleTurnContribution(input: SettleTurnContributionInput): TurnMutationResult {
    this.#requireFence(input);
    this.#assertDurableShape(input.expected);
    this.#assertDurableShape(input.next);
    const kinds = ["terminal_error", "interrupted", "ambiguous"];
    if (!kinds.includes(input.kind)) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.kind);
    }
    if (
      input.expected.protocolTurnId !== input.protocolTurnId ||
      input.next.protocolTurnId !== input.protocolTurnId ||
      input.inputOrdinal !== input.expected.inputOrdinal ||
      input.next.inputOrdinal !== input.expected.inputOrdinal ||
      input.next.phase !== input.kind ||
      input.next.steerable !== false ||
      input.next.replyCommitted !== input.expected.replyCommitted
    ) {
      throw new TurnError("INVALID_STATE_TRANSITION", input.protocolTurnId);
    }
    const operationDigest = canonicalDigest({
      method: "settleTurnContribution",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      protocolTurnId: input.protocolTurnId,
      inputOrdinal: input.inputOrdinal,
      kind: input.kind,
      expected: input.expected,
      next: input.next,
    });
    const row = this.turns.get(input.protocolTurnId as unknown as string);
    if (row !== undefined && row.operationDigest === operationDigest) {
      return { applied: false, durable: row.durable };
    }
    if (
      row === undefined ||
      row.stateInstanceId !== input.stateInstanceId ||
      row.sessionId !== input.sessionId
    ) {
      throw new TurnError("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    this.#requireExpected(row, input.expected);
    row.durable = { ...input.next };
    row.operationDigest = operationDigest;
    this.log.push(`settle:${input.kind}`);
    return { applied: true, durable: { ...input.next } };
  }

  readDurableTurnState(input: ReadDurableTurnStateInput): DurableTurnState | null {
    this.#requireFence(input);
    const row = this.turns.get(input.protocolTurnId as unknown as string);
    if (row === undefined) return null;
    if (row.stateInstanceId !== input.stateInstanceId || row.sessionId !== input.sessionId) {
      throw new TurnError("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    return { ...row.durable };
  }
}

// The reader port shares the journal's cursor.
class MemoryReaderPort implements DriverEventReaderPort {
  constructor(readonly journal: MemoryTurnJournal) {}
  claimDriverEventReader(input: DriverEventReaderClaim): {
    readerEpoch: number;
    nextOrdinal: number;
    lastEventDigest: ArtifactDigest | null;
  } {
    return this.journal.claimForReaderPort(input);
  }
}

// ---------------------------------------------------------------------------
// In-memory native driver adapter mirroring the REAL NativeProcessDriver write-
// outcome union. On a `written` outcome it records the invocation-entry chain in
// the shared NativeAttemptStore (so the journal can re-join at terminal).
// ---------------------------------------------------------------------------

async function* noEvents(): AsyncIterable<never> {}

function writtenOutcome(runtimeWriteId: string, visibilityEventId: string): NativeProcessWriteOutcome {
  return {
    kind: "written",
    runtimeWriteId: runtimeWriteId as CommandId,
    visibilityEventId: visibilityEventId as CommandId,
    events: noEvents(),
  };
}

type ScriptStep =
  | { kind: "written" }
  | { kind: "written_wrong_echo" }
  | { kind: "rejected_before_write" }
  | { kind: "not_written" }
  | { kind: "ambiguous" }
  | { kind: "throw" };

class MemoryTurnDriver implements TurnDriverPort {
  readonly starts: DriverTurnBinding[] = [];
  readonly steers: Array<DriverTurnBinding & { expectedTurnId: TurnId }> = [];
  readonly interrupts: TurnId[] = [];
  #script: ScriptStep[];

  constructor(readonly attempts: NativeAttemptStore, script: readonly ScriptStep[]) {
    this.#script = [...script];
  }

  #outcome(binding: DriverTurnBinding): NativeProcessWriteOutcome {
    const step = this.#script.shift() ?? { kind: "written" };
    if (step.kind === "throw") throw new Error("DRIVER_WRITE_THREW");
    if (step.kind === "written") {
      // Record the root-anchored invocation-entry chain with the four ids echoed.
      this.attempts.record(binding);
      return writtenOutcome(binding.runtimeWriteId, binding.visibilityEventId);
    }
    if (step.kind === "written_wrong_echo") {
      return writtenOutcome(opaque("cmd", 90), opaque("cmd", 91));
    }
    if (step.kind === "rejected_before_write") {
      return {
        kind: "rejected_before_write",
        proof: {
          kind: "daemon_preflight_rejection",
          proofId: opaque("cmd", 80) as CommandId,
          requestDigest: digest("e"),
          reason: "invalid_fence",
          proofDigest: digest("f"),
        },
      };
    }
    if (step.kind === "not_written") {
      return {
        kind: "not_written",
        proof: {
          driverKind: "scripted_fake",
          fixtureId: opaque("cmd", 81) as CommandId,
          scriptDigest: digest("a"),
          invocationId: opaque("cmd", 82) as CommandId,
          invocationGeneration: 1,
          writeStartedEntryId: opaque("cmd", 83) as CommandId,
          writeStartedEntryDigest: digest("b"),
          outcomeOrdinal: 1,
          outcome: "not_written",
          proofDigest: digest("c"),
        },
      };
    }
    return { kind: "ambiguous" };
  }

  async startTurn(_input: CompiledNativeTurn, binding: DriverTurnBinding): Promise<NativeProcessWriteOutcome> {
    this.starts.push(binding);
    return this.#outcome(binding);
  }

  async steerTurn(
    _input: CompiledNativeTurn,
    binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): Promise<NativeProcessWriteOutcome> {
    this.steers.push(binding);
    return this.#outcome(binding);
  }

  async interrupt(expectedTurnId: TurnId): Promise<void> {
    this.interrupts.push(expectedTurnId);
  }
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

function config(): MachineConfig {
  return {
    launchId: opaque("lnc", 1) as LaunchId,
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    sessionId: opaque("ses", 1) as SessionId,
    readerOwnerToken: digest("9"),
  };
}

function fence(): DeliveryFence {
  return {
    protocolVersion: version,
    deliveryId: opaque("dlv", 1) as DeliveryId,
    attempt: 1,
    producerFactId: opaque("fac", 1) as ProducerFactId,
    agentId: opaque("agt", 1) as never,
    machineId: opaque("mch", 1) as never,
    launchId: opaque("lnc", 1) as LaunchId,
    membershipEpoch: 1,
    routingGeneration: 1,
    routeVersion: 1,
    sessionId: opaque("ses", 1) as SessionId,
    turnId: opaque("trn", 1) as TurnId,
  };
}

const stubCompiled = { input: {}, inputDigest: digest("d") } as unknown as CompiledNativeTurn;

// §8: a turn's identity IS its admission DeliveryFence.turnId. Distinct turns use
// distinct `deliverySuffix` (=> distinct turnId); a steer reuses its target turn's
// suffix so its delivery fence carries the exact same turnId (same logical turn).
function submission(mode: DriverInputMode, deliverySuffix = 1): TurnSubmission {
  const delivery: DeliveryFence = {
    ...fence(),
    deliveryId: opaque("dlv", deliverySuffix) as DeliveryId,
    turnId: opaque("trn", deliverySuffix) as TurnId,
  };
  // Seed the pre-admission stored delivery/attempt row (the permit-flow trust
  // root the fake admission joins against before any mutation). Idempotent per
  // (deliveryId, attempt): a same-delivery steer reuses the identical row.
  seedDelivery(delivery);
  return {
    delivery,
    compiled: stubCompiled,
    rootProducerFactId: opaque("fac", 1) as ProducerFactId,
    driverTurnRefDigest: digest("7"),
    inputDigest: digest("d"),
    permitId: opaque("cmd", 70) as CommandId,
    mode,
  };
}

function machine(script: readonly ScriptStep[]): {
  m: TurnMachine;
  journal: MemoryTurnJournal;
  driver: MemoryTurnDriver;
  attempts: NativeAttemptStore;
} {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const reader = new MemoryReaderPort(journal);
  const driver = new MemoryTurnDriver(attempts, script);
  const m = new TurnMachine({ journal, reader, driver, ids: new Ids(), config: config() });
  return { m, journal, driver, attempts };
}

// bindingDigest is REQUIRED. `ev` stamps the machine's CURRENT
// active-contribution binding (the fence the event legitimately belongs to). Use
// `evBinding` to force a specific (e.g. stale) binding for the fence-mismatch
// controls.
function ev(m: TurnMachine, ordinal: number, event: NormalizedDriverEvent): TurnEventInput {
  const bindingDigest = m.activeBindingDigest ?? digest("0");
  return { ordinal, eventDigest: digest(alphabet[ordinal % 32] ?? "0"), bindingDigest, event };
}

function evBinding(
  ordinal: number,
  bindingDigest: ArtifactDigest,
  event: NormalizedDriverEvent,
): TurnEventInput {
  return { ordinal, eventDigest: digest(alphabet[ordinal % 32] ?? "0"), bindingDigest, event };
}

// A same-ordinal event with a DIFFERENT eventDigest (divergent replay control).
function evAt(
  m: TurnMachine,
  ordinal: number,
  eventDigest: ArtifactDigest,
  event: NormalizedDriverEvent,
): TurnEventInput {
  return { ordinal, eventDigest, bindingDigest: m.activeBindingDigest ?? digest("0"), event };
}

type Written = {
  kind: "written";
  turnId: TurnId;
  inputOrdinal: number;
  receipts: { runtimeWriteId: CommandId; visibilityEventId: CommandId };
};

async function toModelVisible(m: TurnMachine, out: Written, startOrdinal: number): Promise<number> {
  const turnId = out.turnId;
  let o = startOrdinal;
  m.applyEvent(ev(m, o++, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  m.applyEvent(ev(m, o++, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId }));
  return o;
}

async function toRepliedModelVisible(m: TurnMachine, out: Written): Promise<{ turnId: TurnId; o: number }> {
  const turnId = out.turnId;
  let o = await toModelVisible(m, out, 0);
  m.applyEvent(ev(m, o++, { kind: "assistant_reply", turnId, text: "x" }));
  return { turnId, o };
}

function coordinationCall(turnId: TurnId, commandId: CommandId): NormalizedDriverEvent {
  return {
    kind: "coordination_call",
    turnId,
    commandId,
    command: { protocolVersion: version, title: "t", sourceMessageId: opaque("msg", 1) as never },
  };
}

// The commandDigest this module stages for a coordination leg (protocolDigest of the
// canonical requested command). Mirrors machine.#stageTerminal.
function protocolDigestOfCommand(commandId: CommandId): ArtifactDigest {
  void commandId; // shape is fixed by coordinationCall()
  return protocolDigest({
    protocolVersion: version,
    title: "t",
    sourceMessageId: opaque("msg", 1),
  });
}

// Lane D simulation: reconstitute the promoted TurnTerminalCommitBasis
// from this module's command-id-free stage (adding only replyCommandId to the reply
// leg), build the completion evidence with a server reply + bound disposition,
// and make the SINGLE commitTurnTerminal call. this module stages; Lane D commits. The
// replyCommandId is Lane-D-owned; a distinct one yields a distinct operation
// (used to prove exactly-once collision vs byte-identical replay aliasing).
function laneDCommitTerminal(
  m: TurnMachine,
  journal: MemoryTurnJournal,
  replyCommandId: CommandId = opaque("cmd", 40) as CommandId,
  opts: { replyResultDigest?: ArtifactDigest } = {},
): TurnStepResult {
  const stage = m.terminalDraft();
  const reply = {
    receiptId: opaque("rcp", 3) as ReceiptId,
    resultDigest: opts.replyResultDigest ?? digest("2"),
  };
  const coordination =
    stage.coordinationRequest.kind === "not_requested"
      ? { kind: "not_requested" as const, terminalTurnId: stage.contribution.fence.turnId }
      : {
          kind: "committed" as const,
          commandId: stage.coordinationRequest.commandId,
          receiptId: opaque("rcp", 4) as ReceiptId,
          resultDigest: digest("3"),
        };
  return journal.commitTurnTerminal({
    ...stage.readerFence,
    basis: {
      reply: { ...stage.basis.reply, replyCommandId },
      ...(stage.basis.coordination !== undefined ? { coordination: stage.basis.coordination } : {}),
      completed: stage.basis.completed,
    },
    evidence: { contribution: stage.contribution, reply, coordination },
    expected: stage.expected,
    next: stage.next,
    recordedAt: "2026-08-04T00:10:00.000Z",
  });
}

// Byte-identical Lane-D commit (fixed replyCommandId) — for replay-alias proof.
function laneDCommitTerminalFixed(m: TurnMachine, journal: MemoryTurnJournal): TurnStepResult {
  return laneDCommitTerminal(m, journal, opaque("cmd", 41) as CommandId);
}

// Drive turn_completed (this module stages) THEN simulate Lane D's single commit so the
// durable row advances to `completed` and the session frees for the next turn.
function completeTurn(m: TurnMachine, journal: MemoryTurnJournal, turnId: TurnId, o: number): void {
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  laneDCommitTerminal(m, journal, opaque("cmd", 44 + (o % 20)) as CommandId);
}

// ===========================================================================
// KILLING CONTROLS
// ===========================================================================

test("ordinary turn: written, reader claimed, one reply, STAGED terminal; this module never commits terminal", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  assert.equal(m.activeTurnId, turnId);
  assert.equal(m.activePhase, "write_started");
  assert.equal(journal.turnState(turnId)?.phase, "write_started");

  let o = 0;
  const a1 = m.applyEvent(ev(m, o++, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(a1.kind, "advanced");
  assert.equal(m.activePhase, "input_written");
  m.applyEvent(ev(m, o++, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId }));
  assert.equal(m.activePhase, "model_visible");
  m.applyEvent(ev(m, o++, { kind: "assistant_reply", turnId, text: "hi" }));
  assert.equal(m.replyCount, 1);
  const done = m.applyEvent(ev(m, o++, { kind: "turn_completed", turnId }));
  // STAGE-ONLY: input_written(0), model_visible(1) committed cursor steps; the
  // reply/completed ordinals are NOT committed by this module (that is Lane D's single
  // commitTurnTerminal). turn_completed reports committed:false; cursor stays at 2.
  assert.equal(done.kind, "advanced");
  if (done.kind !== "advanced") return;
  assert.equal(done.committed, false);
  assert.equal(journal.cursorNextOrdinal, 2);
  // this module NEVER drives commitTurnTerminal.
  assert.equal(journal.terminalCommitCalls, 0);
  // The active contribution is cleared (staged-terminal); the durable turn row
  // was left UNCOMMITTED at model_visible (Lane D advances it to completed).
  assert.equal(m.activeTurnId, null);
  assert.equal(journal.turnState(turnId)?.phase, "model_visible");
  assert.equal(journal.turnState(turnId)?.replyCommitted, false);
  // The terminal suffix is STAGED in memory and readable as a single snapshot.
  const stage = m.terminalDraft();
  assert.equal(stage.contribution.fence.turnId, turnId);
  assert.equal(stage.next.phase, "completed");
  assert.equal(stage.next.replyCommitted, true);
  assert.equal(stage.expected.replyCommitted, false);
});

test("post-STAGED-reply steer must not reopen the reply slot -> ACTIVE_TURN_NOT_STEERABLE (turn-wide closure)", async () => {
  const { m } = machine([{ kind: "written" }, { kind: "written" }]);
  m.claimReader("start");
  const first = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(first.kind, "written");
  if (first.kind !== "written") return;
  const turnId = first.turnId;
  const o = await toModelVisible(m, first, 0);
  // The contribution is at a steerable model_visible boundary. Stage a reply
  // (does not advance this module's cursor — reply is a staged terminal-basis leg).
  m.applyEvent(ev(m, o, { kind: "assistant_reply", turnId, text: "one" }));
  assert.equal(m.replyCount, 1);

  // The reply is STAGED (durable replyCommitted stays false because
  // this module does not commit terminal), yet the turn-wide staged closure blocks a
  // steer from reopening the reply slot even though the contribution is still at
  // a steerable model_visible boundary.
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_NOT_STEERABLE",
  );
});

test("turn-WIDE single reply across a steer: exactly one assistant reply per logical turn", async () => {
  // A steer BEFORE the reply keeps the same logical turn; the reply then commits
  // once. This proves reply/steerability are per-logical-turn, not per-contribution.
  const { m, journal } = machine([{ kind: "written" }, { kind: "written" }]);
  m.claimReader("start");
  const first = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(first.kind, "written");
  if (first.kind !== "written") return;
  const turnId = first.turnId;
  let o = await toModelVisible(m, first, 0);
  // No reply yet: steerable at model_visible. Steer within the same logical turn.
  const steer = await m.submit(submission({ kind: "steer", expectedTurnId: turnId }));
  assert.equal(steer.kind, "written");
  if (steer.kind !== "written") return;
  assert.equal(steer.turnId, turnId);
  assert.equal(steer.inputOrdinal, 1);
  assert.notEqual(steer.receipts.runtimeWriteId, first.receipts.runtimeWriteId);
  assert.notEqual(steer.receipts.visibilityEventId, first.receipts.visibilityEventId);
  o = await toModelVisible(m, steer, o);
  assert.equal(m.replyCount, 0);
  m.applyEvent(ev(m, o++, { kind: "assistant_reply", turnId, text: "final" }));
  assert.equal(m.replyCount, 1);
  // A SECOND reply on the same logical turn (post-steer, pre-terminal) is refused.
  assert.throws(
    () => m.applyEvent(ev(m, o, { kind: "assistant_reply", turnId, text: "again" })),
    (e: unknown) => e instanceof TurnError && e.code === "MULTIPLE_ASSISTANT_REPLIES",
  );
  const done = m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  assert.equal(done.kind, "advanced");
  // STAGE-ONLY: durable stays model_visible; the terminal is staged.
  assert.equal(journal.turnState(turnId)?.replyCommitted, false);
  assert.equal(journal.terminalCommitCalls, 0);
  assert.equal(m.terminalDraft().next.inputOrdinal, 1);
});

test("second reply -> MULTIPLE_ASSISTANT_REPLIES; sibling reply_count stays 1", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  let o = await toModelVisible(m, out, 0);
  m.applyEvent(ev(m, o++, { kind: "assistant_reply", turnId, text: "one" }));
  assert.equal(m.replyCount, 1);
  assert.throws(
    () => m.applyEvent(ev(m, o, { kind: "assistant_reply", turnId, text: "two" })),
    (e: unknown) => e instanceof TurnError && e.code === "MULTIPLE_ASSISTANT_REPLIES",
  );
  assert.equal(m.replyCount, 1);
});

test("wrong / completed expectedTurn steer -> ACTIVE_TURN_CONFLICT", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const wrongTurn = opaque("trn", 9) as TurnId;
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: wrongTurn })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
  const turnId = out.turnId;
  const { o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
});

test("no-steerable-boundary steer -> ACTIVE_TURN_NOT_STEERABLE with ZERO queue/journal/cursor effect (unsafe steer never queued)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;

  const cursorBefore = journal.cursorNextOrdinal;
  const stepsBefore = journal.log.length;
  // Before model_visible: not steerable -> rejected with ZERO queue mutation
  // (this module never enqueues an unsafe steer).
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_NOT_STEERABLE",
  );
  assert.equal(m.queuedCount, 0);
  assert.equal(journal.cursorNextOrdinal, cursorBefore);
  assert.equal(journal.log.length, stepsBefore);

  let o = await toModelVisible(m, out, 0);
  // A non-steerable review boundary (at the cursor's next ordinal) closes the
  // steer window even though the contribution is at model_visible.
  m.applyEvent(ev(m, o++, { kind: "turn_boundary", turnId, boundary: "review", steerable: false }));
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_NOT_STEERABLE",
  );
  assert.equal(m.queuedCount, 0);
  // A non-steerable compaction boundary likewise keeps the steer window closed.
  m.applyEvent(ev(m, o++, { kind: "turn_boundary", turnId, boundary: "compaction", steerable: false }));
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_NOT_STEERABLE",
  );
  assert.equal(m.queuedCount, 0);
});

test("plain ordinary submit while active QUEUES; drains as next ordinary turn (fresh id), not visible early", async () => {
  const { m, driver, journal } = machine([{ kind: "written" }, { kind: "written" }]);
  m.claimReader("start");
  const first = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(first.kind, "written");
  if (first.kind !== "written") return;
  const turnId = first.turnId;

  const queued = await m.submit(submission({ kind: "ordinary" }, 2));
  assert.equal(queued.kind, "queued");
  assert.equal(driver.starts.length, 1);
  assert.equal(driver.steers.length, 0);

  const { o } = await toRepliedModelVisible(m, first);
  completeTurn(m, journal, turnId, o);
  assert.equal(m.activeTurnId, null);

  const next = await m.drainNext();
  assert.equal(next.kind, "written");
  if (next.kind !== "written") return;
  assert.notEqual(next.turnId, turnId);
  assert.equal(next.inputOrdinal, 0);
  assert.equal(driver.starts.length, 2);
  assert.equal(driver.steers.length, 0);
  assert.equal(journal.turnState(next.turnId)?.phase, "write_started");
});

test("interrupt: exact correlate + drives driver; old/uncorrelated refuse with no driver call + no fabricated terminal", async () => {
  const { m, driver, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const none = await m.interrupt(opaque("trn", 5) as TurnId);
  assert.equal(none.kind, "uncorrelated");
  assert.equal(driver.interrupts.length, 0);

  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;

  const wrong = await m.interrupt(opaque("trn", 7) as TurnId);
  assert.equal(wrong.kind, "uncorrelated");
  assert.equal(driver.interrupts.length, 0);
  assert.equal(m.activeTurnId, turnId);

  const exact = await m.interrupt(turnId);
  assert.equal(exact.kind, "interrupted");
  assert.deepEqual(driver.interrupts, [turnId]);
  assert.equal(m.activeTurnId, null);
  assert.equal(journal.turnState(turnId)?.phase, "interrupted");

  const old = await m.interrupt(turnId);
  assert.equal(old.kind, "no_active_turn");
  assert.equal(driver.interrupts.length, 1);
});

test("continuation never starves external: FIFO drain of queued external delivery", async () => {
  const { m, driver, journal } = machine([{ kind: "written" }, { kind: "written" }]);
  m.claimReader("start");
  const first = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(first.kind, "written");
  if (first.kind !== "written") return;
  const turnId = first.turnId;
  const external = await m.submit(submission({ kind: "ordinary" }, 3));
  assert.equal(external.kind, "queued");
  assert.equal(m.queuedCount, 1);

  const { o } = await toRepliedModelVisible(m, first);
  completeTurn(m, journal, turnId, o);

  const drained = await m.drainNext();
  assert.equal(drained.kind, "written");
  assert.equal(driver.starts.length, 2);
});

test("stale / wrong-turn event advances ZERO cursor ordinal; unchanged siblings hold", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(journal.cursorNextOrdinal, 1);

  const wrong = m.applyEvent(ev(m, 1, { kind: "assistant_reply", turnId: opaque("trn", 9) as TurnId, text: "x" }));
  assert.equal(wrong.kind, "ignored_no_cursor_advance");
  assert.equal(journal.cursorNextOrdinal, 1);

  m.applyEvent(ev(m, 1, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId }));
  m.applyEvent(ev(m, 2, { kind: "assistant_reply", turnId, text: "x" }));
  m.applyEvent(ev(m, 3, { kind: "turn_completed", turnId }));
  // STAGE-ONLY: only input_written(0) + model_visible(1) committed cursor steps;
  // the reply/completed ordinals are staged (Lane D commits). Cursor stays at 2.
  assert.equal(journal.cursorNextOrdinal, 2);
  const stale = m.applyEvent(ev(m, 4, { kind: "assistant_reply", turnId, text: "late" }));
  assert.equal(stale.kind, "ignored_no_cursor_advance");
  assert.equal(journal.cursorNextOrdinal, 2);
});

test("rejected_before_write: no boundary effect, safe retry, no active turn", async () => {
  const { m, journal } = machine([{ kind: "rejected_before_write" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.deepEqual(out, { kind: "rejected_before_write" });
  assert.equal(m.activeTurnId, null);
  assert.ok(journal.log.includes("begin:ordinary:0"));
  assert.ok(journal.log.includes("settle:terminal_error"));
  assert.equal(journal.cursorNextOrdinal, 0);
});

test("not_written: next-generation path bumps active generation, no active turn", async () => {
  const { m } = machine([{ kind: "not_written" }]);
  m.claimReader("start");
  assert.equal(m.activeGeneration, 0);
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.deepEqual(out, { kind: "next_generation" });
  assert.equal(m.activeGeneration, 1);
  assert.equal(m.activeTurnId, null);
});

test("ambiguous: held AMBIGUOUS_NATIVE_WRITE, no further driver call", async () => {
  const { m, driver, journal } = machine([{ kind: "ambiguous" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.deepEqual(out, { kind: "held_ambiguous" });
  assert.equal(m.isAmbiguousHeld, true);
  assert.equal(driver.starts.length, 1);
  assert.ok(journal.log.includes("settle:ambiguous"));
  await assert.rejects(
    m.submit(submission({ kind: "ordinary" }, 4)),
    (e: unknown) => e instanceof TurnError && e.code === "AMBIGUOUS_NATIVE_WRITE",
  );
  assert.equal(driver.starts.length, 1);
});

test("written_wrong_echo -> DRIVER_EVENT_FENCE_MISMATCH; row reconciled terminal_error", async () => {
  const { m, journal } = machine([{ kind: "written_wrong_echo" }]);
  m.claimReader("start");
  await assert.rejects(
    m.submit(submission({ kind: "ordinary" })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(m.activeTurnId, null);
  assert.ok(journal.log.includes("settle:terminal_error"));
});

test("wrong runtimeWriteId -> DRIVER_EVENT_FENCE_MISMATCH; cursor unchanged (zero-mutation sibling)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  assert.throws(
    () => m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: opaque("cmd", 99) as CommandId })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(journal.cursorNextOrdinal, 0);
  assert.equal(journal.turnState(turnId)?.phase, "write_started");
});

test("wrong visibilityEventId -> DRIVER_EVENT_FENCE_MISMATCH; cursor unchanged", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.throws(
    () => m.applyEvent(ev(m, 1, { kind: "model_visible", turnId, visibilityEventId: opaque("cmd", 98) as CommandId })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(journal.cursorNextOrdinal, 1);
});

test("STALE_INVOCATION_GENERATION and INVOCATION_STATE_CONFLICT are distinct §8.5b codes", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  await m.submit(submission({ kind: "ordinary" }));
  assert.equal(m.activeGeneration, 1);
  assert.throws(
    () => m.assertGenerationCurrent(0, null),
    (e: unknown) => e instanceof TurnError && e.code === "STALE_INVOCATION_GENERATION",
  );
  assert.throws(
    () => m.assertGenerationCurrent(1, "model_visible"),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
});

test("INVOCATION_STATE_CONFLICT via out-of-order boundary (distinct from reply count)", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  m.applyEvent(ev(m, 1, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId }));
  assert.throws(
    () => m.applyEvent(ev(m, 2, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId })),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
});

test("reply-before-model-visible fails closed (ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE); zero advance", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.throws(
    () => m.applyEvent(ev(m, 1, { kind: "assistant_reply", turnId, text: "early" })),
    (e: unknown) => e instanceof TurnError && e.code === "ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE",
  );
  assert.equal(journal.cursorNextOrdinal, 1);
  assert.equal(m.replyCount, 0);
});

test("completion-without-reply fails closed (TURN_COMPLETION_WITHOUT_REPLY)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const o = await toModelVisible(m, out, 0);
  assert.throws(
    () => m.applyEvent(ev(m, o, { kind: "turn_completed", turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "TURN_COMPLETION_WITHOUT_REPLY",
  );
  assert.equal(m.replyCommitted, false);
  assert.equal(journal.cursorNextOrdinal, 2);
});

test("coordination_call recorded as after-reply predecessor; SECOND_COORDINATION_CALL", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  let o = await toModelVisible(m, out, 0);
  assert.throws(
    () => m.applyEvent(ev(m, o, coordinationCall(turnId, opaque("cmd", 60) as CommandId))),
    (e: unknown) => e instanceof TurnError && e.code === "ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE",
  );
  m.applyEvent(ev(m, o++, { kind: "assistant_reply", turnId, text: "x" }));
  const coord = m.applyEvent(ev(m, o++, coordinationCall(turnId, opaque("cmd", 61) as CommandId)));
  assert.equal(coord.kind, "advanced");
  assert.throws(
    () => m.applyEvent(ev(m, o, coordinationCall(turnId, opaque("cmd", 62) as CommandId))),
    (e: unknown) => e instanceof TurnError && e.code === "SECOND_COORDINATION_CALL",
  );
});

test("runtime_terminal drops the active in-flight contribution; PRESERVES queued ORDINARY deliveries (no-starve)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  // A queued ORDINARY external delivery (only ordinaries queue; an
  // unsafe steer is never enqueued).
  const external = await m.submit(submission({ kind: "ordinary" }, 3));
  assert.equal(external.kind, "queued");
  assert.equal(m.queuedCount, 1);
  m.applyEvent(ev(m, 0, { kind: "runtime_terminal", reason: "process_exited" }));
  assert.equal(m.activeTurnId, null);
  // The queued ordinary delivery is PRESERVED (a continuation is never starved).
  assert.equal(m.queuedCount, 1);
  assert.equal(journal.turnState(turnId)?.phase, "terminal_error");
});

test("driver write throwing AFTER admission reconciles journal + memory (crash-repairable)", async () => {
  const { m, journal } = machine([{ kind: "throw" }]);
  m.claimReader("start");
  await assert.rejects(m.submit(submission({ kind: "ordinary" })), /DRIVER_WRITE_THREW/);
  assert.equal(m.activeTurnId, null);
  assert.ok(journal.log.includes("begin:ordinary:0"));
  assert.ok(journal.log.includes("settle:terminal_error"));
});

test("queued-not-visible-early: written but NOT visible until the model_visible event lands", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  assert.equal(m.activePhase, "write_started");
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(m.activePhase, "input_written");
  m.applyEvent(ev(m, 1, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId }));
  assert.equal(m.activePhase, "model_visible");
});

test("idempotent exact replay of a step aliases applied:false (no double effect / no double cursor advance)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const first = m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(first.kind, "advanced");
  assert.equal(journal.cursorNextOrdinal, 1);
  // Exact replay of the SAME ordinal + event: the port aliases applied:false; the
  // machine re-sees committed=false and the cursor does NOT double-advance. Local
  // phase is unchanged (already input_written).
  const replay = m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(replay.kind, "advanced");
  if (replay.kind !== "advanced") return;
  assert.equal(replay.committed, false);
  assert.equal(journal.cursorNextOrdinal, 1);
  assert.equal(m.activePhase, "input_written");
});

test("partial/divergent CAS -> ACTIVE_TURN_CONFLICT with zero mutation (out-of-order ordinal)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  // A model_visible at the WRONG ordinal (2 instead of 1): cursor-CAS rejects with
  // DRIVER_EVENT_ORDER_INVALID; zero durable/cursor mutation.
  assert.throws(
    () => m.applyEvent(ev(m, 2, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_ORDER_INVALID",
  );
  assert.equal(journal.cursorNextOrdinal, 1);
  assert.equal(journal.turnState(turnId)?.phase, "input_written");
});

test("cursor validate-then-commit: a failing validation advances ZERO ordinal", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  // A model_visible BEFORE input_written: the pure phase gate throws
  // INVOCATION_STATE_CONFLICT before the durable commit; cursor stays at 0.
  assert.throws(
    () => m.applyEvent(ev(m, 0, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId })),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
  assert.equal(journal.cursorNextOrdinal, 0);
});

// REQUIRED-SOURCE-BINDING KILLING CONTROL.
test("required-source-binding: a stale-binding event is NOT reattributed via fallback (WRITE_STARTED_BINDING_MISMATCH); the correct binding then succeeds", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const activeBinding = m.activeBindingDigest;
  assert.ok(activeBinding);
  // An old-producer event carrying a STALE binding (!= the active contribution's
  // fence) is rejected — NEVER silently reattributed to the current contribution.
  assert.throws(
    () =>
      m.applyEvent(
        evBinding(0, digest("f"), { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }),
      ),
    (e: unknown) => e instanceof TurnError && e.code === "WRITE_STARTED_BINDING_MISMATCH",
  );
  // Zero effect: cursor + durable byte-unchanged.
  assert.equal(journal.cursorNextOrdinal, 0);
  assert.equal(journal.turnState(turnId)?.phase, "write_started");
  // The SAME event with the CORRECT (required) source binding is attributed and
  // advances exactly once (no fallback was masking the mismatch above).
  const ok = m.applyEvent(
    evBinding(0, activeBinding, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }),
  );
  assert.equal(ok.kind, "advanced");
  assert.equal(journal.cursorNextOrdinal, 1);
  assert.equal(journal.turnState(turnId)?.phase, "input_written");
});

test("restart-recovery rebuild: recoverDurable rehydrates the active durable mirror from the journal", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  const recovered = m.recoverDurable(turnId);
  assert.notEqual(recovered, null);
  assert.equal(recovered?.phase, "input_written");
  assert.deepEqual(recovered, journal.turnState(turnId));
  // A missing turn recovers to null (no throw).
  assert.equal(m.recoverDurable(opaque("trn", 15) as TurnId), null);
});

test("crash-repairable write reconcile: rejected_before_write leaves a settle-able terminal_error row", async () => {
  const { m, journal } = machine([{ kind: "rejected_before_write" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.deepEqual(out, { kind: "rejected_before_write" });
  assert.equal(journal.turnState(submission({ kind: "ordinary" }).delivery.turnId)?.phase, "terminal_error");
});

// ===========================================================================
// Unsafe / post-reply steer: ZERO machine/journal/cursor/queue effect + Lane-D-
// readmit-as-fresh. A queued unsafe steer that drains drains as a FRESH identity.
// ===========================================================================

test("post-reply steer -> ZERO machine/journal/cursor/queue effect (Lane D re-admits as fresh)", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  // Drive to terminal so replyCommitted is durably true; then a steer targets a
  // completed turn.
  const { o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const cursorBefore = journal.cursorNextOrdinal;
  const logLen = journal.log.length;
  const queuedBefore = m.queuedCount;
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
  // Zero effect: no cursor advance, no journal mutation, no queue growth.
  assert.equal(journal.cursorNextOrdinal, cursorBefore);
  assert.equal(journal.log.length, logLen);
  assert.equal(m.queuedCount, queuedBefore);
});

test("unsafe steer is NEVER enqueued: the queue holds no old-turnId entry; this module never reuses a completed turnId", async () => {
  const { m, driver, journal } = machine([{ kind: "written" }, { kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  // An unsafe steer (before model_visible) is rejected with ZERO queue mutation —
  // NOT enqueued with the old active turnId.
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_NOT_STEERABLE",
  );
  assert.equal(m.queuedCount, 0);
  // Complete the active turn; the queue is empty, so draining is a no-op — there is
  // NO demoted old-turnId drain path (that path was removed).
  const { o } = await toRepliedModelVisible(m, out);
  completeTurn(m, journal, turnId, o);
  const drained = await m.drainNext();
  assert.deepEqual(drained, { kind: "empty" });
  assert.equal(driver.starts.length, 1);
  assert.equal(driver.steers.length, 0);
});

// ===========================================================================
// Storage-join / chain-anchor: the terminal commit re-joins the stored entry
// chain (binding invariants #1-#4). These prove the machine feeds the SAME
// distinct ids + the same canonical contribution the port re-derives.
// ===========================================================================

test("terminal contribution-join: staged ids re-join the stored chain when Lane D commits (machine feeds SAME ids)", async () => {
  const { m, journal, attempts } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const { o } = await toRepliedModelVisible(m, out);
  // The stored chain carries the exact distinct ids the machine preallocated.
  const chain = attempts.join(turnId);
  assert.equal(chain?.runtimeWriteId, out.receipts.runtimeWriteId);
  assert.equal(chain?.visibilityEventId, out.receipts.visibilityEventId);
  // this module STAGES the terminal (no commit). Lane D then makes the single commit; the
  // port re-joins the staged contribution to the stored chain + verifies -> applied.
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  assert.equal(journal.terminalCommitCalls, 0);
  const committed = laneDCommitTerminal(m, journal);
  assert.equal(committed.applied, true);
  assert.equal(journal.terminalCommitCalls, 1);
  assert.equal(journal.turnState(turnId)?.phase, "completed");
});

test("Lane D terminal commit fails INVALID_JOURNAL_CHAIN when the stored entry chain is absent (mis-drive caught)", async () => {
  const { m, journal, attempts } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const { o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  // Corrupt the shared store: drop the entry chain so the join has nothing. The
  // machine already staged; Lane D's single commit re-joins and fails closed.
  attempts.chains.delete(turnId as unknown as string);
  assert.throws(
    () => laneDCommitTerminal(m, journal),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_JOURNAL_CHAIN",
  );
});

test("no-synthetic-evidence: the staged contribution-join fails if the machine fed a WRONG id (mis-drive caught)", async () => {
  const { m, journal, attempts } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const { o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  // Corrupt the stored chain's visibilityEventId so it diverges from the staged
  // contribution: Lane D's join detects the mismatch and fails closed.
  const chain = attempts.chains.get(turnId as unknown as string);
  assert.ok(chain);
  attempts.chains.set(turnId as unknown as string, {
    ...chain,
    visibilityEventId: opaque("cmd", 55) as CommandId,
  });
  assert.throws(
    () => laneDCommitTerminal(m, journal),
    (e: unknown) => e instanceof TurnError && e.code === "WRITE_STARTED_BINDING_MISMATCH",
  );
});

test("four ids stay pairwise-distinct end-to-end; a colliding permitId fails closed before any write", async () => {
  const { m, driver } = machine([{ kind: "written" }]);
  m.claimReader("start");
  // Force permitId to collide with a preallocated id. The first minted id from Ids
  // is cmd_...0001 (invocationId). Set permitId to the SAME value it will mint for
  // a later contribution is hard to time; instead assert the guard fires when the
  // permitId equals the invocationId the source is about to mint. We approximate by
  // driving one normal turn and asserting distinctness of stored ids.
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const binding = driver.starts[0];
  assert.ok(binding);
  const set = new Set([
    binding.invocation.invocationId,
    binding.permitId,
    binding.runtimeWriteId,
    binding.visibilityEventId,
  ]);
  assert.equal(set.size, 4);
});

test("pairwise-distinct guard fires: a permitId equal to a minted id fails closed with no driver write", async () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const reader = new MemoryReaderPort(journal);
  const driver = new MemoryTurnDriver(attempts, [{ kind: "written" }]);
  // An Ids source whose FIRST mint (invocationId) equals the submission permitId.
  class CollidingIds extends Ids {
    #n = 0;
    override nextCommandId(): CommandId {
      this.#n += 1;
      if (this.#n === 1) return opaque("cmd", 70) as CommandId; // == submission permitId
      return super.nextCommandId();
    }
  }
  const m = new TurnMachine({ journal, reader, driver, ids: new CollidingIds(), config: config() });
  m.claimReader("start");
  await assert.rejects(
    m.submit(submission({ kind: "ordinary" })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(driver.starts.length, 0);
});

// ===========================================================================
// Admission trust boundary: the faithful journal fake enforces the ordered
// seven-member durable admission preimage (brand / range / pairwise-distinct
// asserts + stored delivery/attempt/fence join + authenticated source join +
// the sole SSOT contribution-binding recompute) BEFORE any mutation. These
// controls drive the fake's beginTurnContribution DIRECTLY to prove the exact
// preimage is carried and to prove each member fails closed with ZERO mutation.
// ===========================================================================

// A canonical valid seven-member admission input over a freshly seeded stored
// delivery/attempt row. `bindingDigest` is the SSOT recompute of the stored
// fence + the seven members, so admission's joins + recompute all pass.
function admissionInput(
  journal: MemoryTurnJournal,
  overrides: Partial<BeginTurnContributionInput> = {},
): BeginTurnContributionInput {
  const delivery: DeliveryFence = {
    ...fence(),
    deliveryId: opaque("dlv", 1) as DeliveryId,
    turnId: opaque("trn", 1) as TurnId,
  };
  seedDelivery(delivery);
  const readerEpoch = journal.claimForReaderPort({
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    sessionId: opaque("ses", 1) as SessionId,
    ownerToken: digest("9"),
    mode: "start",
    claimedAt: "2026-08-04T00:00:00.000Z",
  }).readerEpoch;
  const invocationId = opaque("cmd", 1) as CommandId;
  const invocationGeneration = 1;
  const permitId = opaque("cmd", 2) as CommandId;
  const runtimeWriteId = opaque("cmd", 3) as CommandId;
  const visibilityEventId = opaque("cmd", 4) as CommandId;
  const bindingDigest = buildContributionBinding({
    fence: delivery,
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    inputOrdinal: 0,
    invocationId,
    invocationGeneration,
    permitId,
    runtimeWriteId,
    visibilityEventId,
  }).contributionBindingDigest;
  const next: DurableTurnState = {
    protocolTurnId: delivery.turnId,
    phase: "write_started",
    inputOrdinal: 0,
    bindingDigest,
    steerable: false,
    replyCommitted: false,
  };
  return {
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    sessionId: opaque("ses", 1) as SessionId,
    ownerToken: digest("9"),
    readerEpoch,
    protocolTurnId: delivery.turnId,
    launchId: opaque("lnc", 1) as LaunchId,
    rootProducerFactId: delivery.producerFactId,
    inputOrdinal: 0,
    driverTurnRefDigest: digest("7"),
    mode: { kind: "ordinary" },
    bindingDigest,
    deliveryId: delivery.deliveryId,
    attempt: delivery.attempt,
    invocationId,
    invocationGeneration,
    permitId,
    runtimeWriteId,
    visibilityEventId,
    expected: null,
    next,
    recordedAt: "2026-08-04T00:00:01.000Z",
    ...overrides,
  };
}

test("admission POSITIVE: the exact seven-member preimage is carried; the stored-fence join + SSOT recompute pass -> applied", async () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const input = admissionInput(journal, { recordedAt: "2026-08-04T00:00:01.000Z" });
  const result = journal.beginTurnContribution(input);
  assert.equal(result.applied, true);
  assert.equal(result.durable.phase, "write_started");
  assert.equal(result.durable.bindingDigest, input.bindingDigest);
  assert.ok(journal.log.includes("begin:ordinary:0"));
  // An EXACT-preimage replay of the SAME input aliases applied:false with no
  // double effect (the operation-digest preimage includes all seven members).
  const replay = journal.beginTurnContribution({ ...input, recordedAt: "2026-08-04T00:00:02.000Z" });
  assert.equal(replay.applied, false);
  assert.equal(journal.log.filter((l) => l === "begin:ordinary:0").length, 1);
});

test("admission SIBLING conflict: an admission diverging on a preimage member is a DISTINCT operation -> ACTIVE_TURN_CONFLICT (zero mutation)", async () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const input = admissionInput(journal, { recordedAt: "2026-08-04T00:00:01.000Z" });
  assert.equal(journal.beginTurnContribution(input).applied, true);
  // Diverge on a preimage member (a fresh runtimeWriteId) while keeping the same
  // turn occupied. The recomputed binding then diverges too, so the sibling is
  // rejected before any mutation (the occupied turn is not re-admitted).
  const runtimeWriteId = opaque("cmd", 5) as CommandId;
  const bindingDigest = buildContributionBinding({
    fence: { ...fence(), deliveryId: opaque("dlv", 1) as DeliveryId, turnId: opaque("trn", 1) as TurnId },
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    inputOrdinal: 0,
    invocationId: input.invocationId,
    invocationGeneration: input.invocationGeneration,
    permitId: input.permitId,
    runtimeWriteId,
    visibilityEventId: input.visibilityEventId,
  }).contributionBindingDigest;
  const sibling: BeginTurnContributionInput = {
    ...input,
    runtimeWriteId,
    bindingDigest,
    next: { ...input.next, bindingDigest },
    recordedAt: "2026-08-04T00:00:03.000Z",
  };
  assert.throws(
    () => journal.beginTurnContribution(sibling),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
  // Zero mutation: exactly one admission was logged.
  assert.equal(journal.log.filter((l) => l.startsWith("begin:")).length, 1);
});

// KILLING CONTROL for the authenticated-source join (item 2): seed a VALID
// delivery, then tamper ONE stored envelope column (the messageId, or the target
// id) WITHOUT updating the stored envelope digest. The reconstruct→target-key→
// digest chain recomputes the digest from the tampered columns, which no longer
// equals the stored digest, so admission fails closed with ZERO mutation. This
// proves the stored source is trusted only after reconstruction + digest-verify —
// never by shape alone.
test("admission source-tamper: a tampered stored envelope column (digest unchanged) fails the reconstruct→digest join with ZERO mutation", () => {
  for (const tamper of ["messageId", "targetId"] as const) {
    const attempts = new NativeAttemptStore();
    const journal = new MemoryTurnJournal(attempts);
    const input = admissionInput(journal, { recordedAt: "2026-08-04T00:00:01.000Z" });
    // The delivery ledger row is now seeded (admissionInput -> seedDelivery). Tamper
    // ONE stored envelope column in place, leaving envelopeDigest/targetKey stale.
    const row = deliveryLedger.get(deliveryLedgerKey(input.deliveryId, input.attempt));
    assert.ok(row);
    if (tamper === "messageId") {
      row.envelope = { ...row.envelope, messageId: opaque("msg", 7) as MessageId };
    } else {
      row.envelope = {
        ...row.envelope,
        target: { kind: "direct", conversationId: opaque("cvs", 7) as never },
      };
    }
    // Recompute over the tampered columns yields a digest (or target key) that no
    // longer equals the stored one -> fail closed.
    assert.throws(
      () => journal.beginTurnContribution(input),
      (e: unknown) => e instanceof TurnError && e.code === "INVALID_STATE_TRANSITION",
    );
    assert.equal(journal.log.filter((l) => l.startsWith("begin:")).length, 0);
    assert.equal(journal.turnState(input.protocolTurnId), undefined);
  }
});

// The recomputed-digest class (the case the review named): a well-formed WRONG
// producerFactId with a CONSISTENTLY recomputed envelope digest and an untouched
// target. The reconstruct→target-key→digest chain alone would accept it (the digest
// is internally consistent); the expected/source-facts join (the envelope's own
// producerFactId vs the expected root producer fact) rejects it, exactly as
// promoted #requireAuthenticatedSourceMessage does.
test("admission source-tamper (recomputed digest): a well-formed WRONG producerFactId with a consistently recomputed digest fails the expected-facts join with ZERO mutation", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const input = admissionInput(journal, { recordedAt: "2026-08-04T00:00:01.000Z" });
  const row = deliveryLedger.get(deliveryLedgerKey(input.deliveryId, input.attempt));
  assert.ok(row);
  // Tamper the envelope producer fact to a well-formed but WRONG value (target
  // untouched) and RECONSTRUCT the stored digest + target key over the tampered
  // envelope, so the reconstruct→target-key→digest chain is internally consistent.
  const tampered: StoredDeliveryEnvelope = {
    ...row.envelope,
    producerFactId: opaque("fac", 9) as ProducerFactId,
  };
  const tamperedCanonical = canonicalEnvelope(input.deliveryId, input.attempt, tampered);
  row.envelope = { ...tampered };
  row.envelopeDigest = envelopeDigestBytes(tamperedCanonical);
  row.targetKey = targetKeyOf(tamperedCanonical.target);
  // The stored fence is untouched (fence join passes) and the digest/target key are
  // self-consistent, so ONLY the expected/source-facts join catches the wrong
  // producer fact. Fail closed, zero mutation.
  assert.throws(
    () => journal.beginTurnContribution(input),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_STATE_TRANSITION",
  );
  assert.equal(journal.log.filter((l) => l.startsWith("begin:")).length, 0);
  assert.equal(journal.turnState(input.protocolTurnId), undefined);
});

// ---------------------------------------------------------------------------
// Item 3: TWO separated admission fail-closed families over the seven preimage
// members. Both families assert admission fails closed with ZERO mutation (no
// `begin:` log, no durable row).
//
//   (A) OMISSION family — a real runtime-boundary ABSENCE: the member is DELETED
//       / set to `undefined` (cast through `unknown`/`Partial` so it compiles).
//       This is a genuinely missing FIELD, not a malformed value.
//   (B) DIVERGENT/malformed family — every property is supplied but the member
//       carries a malformed / out-of-range value (trips its own assert) OR a
//       well-formed-but-wrong value that breaks the stored-fence join / the SSOT
//       recompute.
// ---------------------------------------------------------------------------

// The seven preimage members that must be present at the runtime boundary.
const admissionPreimageMembers = [
  "deliveryId",
  "attempt",
  "invocationId",
  "invocationGeneration",
  "permitId",
  "runtimeWriteId",
  "visibilityEventId",
] as const;

// (A) OMISSION family: DELETE each member at the runtime boundary.
for (const member of admissionPreimageMembers) {
  test(`admission OMISSION family: a missing (absent) ${member} fails closed with ZERO mutation`, () => {
    const attempts = new NativeAttemptStore();
    const journal = new MemoryTurnJournal(attempts);
    const input = admissionInput(journal, { recordedAt: "2026-08-04T00:00:01.000Z" });
    // Delete the member entirely at the runtime boundary (Partial<unknown> cast so
    // it compiles) — this models a real absence, not a malformed value.
    delete (input as unknown as Record<string, unknown>)[member];
    assert.throws(
      () => journal.beginTurnContribution(input),
      (e: unknown) => e instanceof TurnError,
    );
    // ZERO mutation: no admission logged, no durable turn row written.
    assert.equal(journal.log.filter((l) => l.startsWith("begin:")).length, 0);
    assert.equal(journal.turnState(input.protocolTurnId), undefined);
  });
}

// (B) DIVERGENT/malformed family: every property present, one member malformed /
// out-of-range / duplicate (malformed) OR well-formed-but-wrong (divergent).
const admissionDivergentCases: ReadonlyArray<{
  member: string;
  malformed: Partial<BeginTurnContributionInput>;
  divergent: Partial<BeginTurnContributionInput>;
}> = [
  {
    member: "deliveryId",
    malformed: { deliveryId: opaque("cmd", 9) as unknown as DeliveryId }, // wrong brand ("cmd")
    divergent: { deliveryId: opaque("dlv", 7) as DeliveryId }, // no stored row for this delivery
  },
  {
    member: "attempt",
    malformed: { attempt: 0 }, // out of [1, 2147483647]
    divergent: { attempt: 2 }, // no stored row for (delivery, attempt=2)
  },
  {
    member: "invocationId",
    malformed: { invocationId: opaque("dlv", 9) as unknown as CommandId }, // wrong brand ("dlv")
    divergent: { invocationId: opaque("cmd", 20) as CommandId }, // recompute diverges
  },
  {
    member: "invocationGeneration",
    malformed: { invocationGeneration: 0 }, // out of [1, MAX_SAFE_INTEGER]
    divergent: { invocationGeneration: 9 }, // recompute diverges
  },
  {
    member: "permitId",
    malformed: { permitId: opaque("dlv", 8) as unknown as CommandId }, // wrong brand
    divergent: { permitId: opaque("cmd", 21) as CommandId }, // recompute diverges
  },
  {
    member: "runtimeWriteId",
    malformed: { runtimeWriteId: opaque("dlv", 6) as unknown as CommandId }, // wrong brand
    divergent: { runtimeWriteId: opaque("cmd", 22) as CommandId }, // recompute diverges
  },
  {
    member: "visibilityEventId",
    malformed: { visibilityEventId: opaque("dlv", 5) as unknown as CommandId }, // wrong brand
    divergent: { visibilityEventId: opaque("cmd", 23) as CommandId }, // recompute diverges
  },
];

for (const kase of admissionDivergentCases) {
  for (const variant of ["malformed", "divergent"] as const) {
    test(`admission DIVERGENT family: a ${variant} ${kase.member} fails closed with ZERO mutation`, () => {
      const attempts = new NativeAttemptStore();
      const journal = new MemoryTurnJournal(attempts);
      const input = admissionInput(journal, {
        ...kase[variant],
        recordedAt: "2026-08-04T00:00:01.000Z",
      });
      assert.throws(
        () => journal.beginTurnContribution(input),
        (e: unknown) => e instanceof TurnError,
      );
      // ZERO mutation: no admission logged, no durable turn row written.
      assert.equal(journal.log.filter((l) => l.startsWith("begin:")).length, 0);
      assert.equal(journal.turnState(input.protocolTurnId), undefined);
    });
  }
}

// Item 1: malformed correct-prefix ids are now REJECTED by the promoted brand
// check (full ID_PATTERN, not a bare prefix). A correct-prefix id that is short
// or carries non-Crockford chars (i/l/o/u) fails closed with ZERO mutation.
for (const [member, badId] of [
  ["deliveryId", "dlv_short"],
  ["invocationId", "cmd_short"],
  ["permitId", `cmd_${"1".repeat(25)}`], // 25 chars (one short)
  ["runtimeWriteId", `cmd_${"1".repeat(27)}`], // 27 chars (one long)
  ["visibilityEventId", `cmd_${"i".repeat(26)}`], // 26 non-Crockford (i)
  ["invocationId", `cmd_${"o".repeat(26)}`], // 26 non-Crockford (o)
  ["permitId", `cmd_${"u".repeat(26)}`], // 26 non-Crockford (u)
] as const) {
  test(`admission brand-check: a malformed correct-prefix ${member} (${badId}) fails the full ID_PATTERN with ZERO mutation`, () => {
    const attempts = new NativeAttemptStore();
    const journal = new MemoryTurnJournal(attempts);
    const input = admissionInput(journal, {
      [member]: badId as unknown,
      recordedAt: "2026-08-04T00:00:01.000Z",
    } as Partial<BeginTurnContributionInput>);
    assert.throws(
      () => journal.beginTurnContribution(input),
      (e: unknown) => e instanceof TurnError && e.code === "INVALID_STATE_TRANSITION",
    );
    assert.equal(journal.log.filter((l) => l.startsWith("begin:")).length, 0);
    assert.equal(journal.turnState(input.protocolTurnId), undefined);
  });
}

// ===========================================================================
// Item 4: the reader fake implements the lock-owning claim STATE MACHINE
// (mirrors DaemonJournal.claimDriverEventReader, journal.ts:644-719). These
// controls drive the fake's claim surface DIRECTLY to prove each legal/illegal
// branch, all fail-closed cases mutate ZERO cursor state, and the legal
// released-resume preserves + returns the cursor pair.
// ===========================================================================

function readerClaim(overrides: Partial<DriverEventReaderClaim> = {}): DriverEventReaderClaim {
  return {
    stateInstanceId: opaque("sti", 1) as StateInstanceId,
    sessionId: opaque("ses", 1) as SessionId,
    ownerToken: digest("9"),
    mode: "start",
    claimedAt: "2026-08-04T00:00:00.000Z",
    ...overrides,
  };
}

test("reader claim: fresh cursor via mode=start -> {epoch 1, nextOrdinal 0, lastEventDigest null}; owned", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const r = journal.claimForReaderPort(readerClaim({ mode: "start" }));
  assert.deepEqual(r, { readerEpoch: 1, nextOrdinal: 0, lastEventDigest: null });
  assert.equal(journal.log.filter((l) => l.startsWith("claim_reader")).length, 1);
});

test("reader claim: no cursor + mode=resume -> fail closed (DRIVER_EVENT_FENCE_MISMATCH), ZERO mutation", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  assert.throws(
    () => journal.claimForReaderPort(readerClaim({ mode: "resume" })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(journal.log.length, 0);
});

test("reader claim: existing-cursor SECOND start -> conflict (fail closed), ZERO epoch bump", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  assert.equal(journal.claimForReaderPort(readerClaim({ mode: "start" })).readerEpoch, 1);
  const logAfterFirst = journal.log.length;
  assert.throws(
    () => journal.claimForReaderPort(readerClaim({ mode: "start" })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  // ZERO mutation: no second claim_reader log (epoch stayed 1).
  assert.equal(journal.log.length, logAfterFirst);
});

test("reader claim: overlap-resume (owner still live) -> fail closed (resume overlap), ZERO epoch bump", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  journal.claimForReaderPort(readerClaim({ mode: "start" }));
  const logAfterFirst = journal.log.length;
  // The owner is still live (never released): a resume overlaps and fails closed.
  assert.throws(
    () => journal.claimForReaderPort(readerClaim({ mode: "resume" })),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(journal.log.length, logAfterFirst);
});

test("reader claim: cross-session resume -> fail closed (session mismatch), ZERO epoch bump", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  journal.claimForReaderPort(readerClaim({ mode: "start" }));
  journal.releaseReader(opaque("sti", 1) as StateInstanceId);
  const logAfterRelease = journal.log.length;
  // Released, but a DIFFERENT session resuming the same state-instance is rejected.
  assert.throws(
    () =>
      journal.claimForReaderPort(
        readerClaim({ mode: "resume", sessionId: opaque("ses", 2) as SessionId }),
      ),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(journal.log.length, logAfterRelease);
});

test("reader claim: mismatched state-instance resume (no cursor for it) -> fail closed", () => {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  journal.claimForReaderPort(readerClaim({ mode: "start" }));
  journal.releaseReader(opaque("sti", 1) as StateInstanceId);
  // Resume for a DIFFERENT state-instance that has no cursor -> no-cursor+resume fail.
  assert.throws(
    () =>
      journal.claimForReaderPort(
        readerClaim({ mode: "resume", stateInstanceId: opaque("sti", 2) as StateInstanceId }),
      ),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
});

test("reader claim POSITIVE legal released-resume: epoch+1, re-own, RETURNS the preserved cursor pair", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  // Advance the cursor via two committed steps so the preserved pair is non-trivial.
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  m.applyEvent(ev(m, 1, { kind: "model_visible", turnId, visibilityEventId: out.receipts.visibilityEventId }));
  const preservedNextOrdinal = journal.cursorNextOrdinal;
  const preservedDigest = journal.cursorLastEventDigest;
  assert.equal(preservedNextOrdinal, 2);
  // Release the reader (owner cleared, cursor pair preserved), then resume it: the
  // released-resume branch bumps the epoch, re-owns, and RETURNS the preserved pair.
  journal.releaseReader(opaque("sti", 1) as StateInstanceId);
  const resumed = journal.claimForReaderPort(readerClaim({ mode: "resume" }));
  assert.equal(resumed.readerEpoch, 2); // epoch was 1 at start -> +1 on resume
  assert.equal(resumed.nextOrdinal, preservedNextOrdinal);
  assert.equal(resumed.lastEventDigest, preservedDigest);
});

// ===========================================================================
// §7.9 non-forgeable coordination-result disposition + terminal binding port.
// ===========================================================================

test("§7.9: NO coordination_call -> disposition is not_requested{terminalTurnId}", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  assert.deepEqual(m.coordinationDisposition(), { kind: "not_requested", terminalTurnId: turnId });
});

test("§7.9: coordination_call + bind committed -> disposition committed carries receiptId+resultDigest", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o: o0 } = await toRepliedModelVisible(m, out);
  let o = o0;
  const commandId = opaque("cmd", 61) as CommandId;
  m.applyEvent(ev(m, o++, coordinationCall(turnId, commandId)));
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const receiptId = opaque("rcp", 5) as ReceiptId;
  const resultDigest = digest("c");
  m.bindCoordinationResult({ commandId, kind: "committed", receiptId, resultDigest });
  assert.deepEqual(m.coordinationDisposition(), { kind: "committed", commandId, receiptId, resultDigest });
});

test("§7.9: bind with WRONG commandId -> DRIVER_EVENT_FENCE_MISMATCH; sibling unchanged", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o: o0 } = await toRepliedModelVisible(m, out);
  let o = o0;
  const commandId = opaque("cmd", 61) as CommandId;
  m.applyEvent(ev(m, o++, coordinationCall(turnId, commandId)));
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  assert.throws(
    () =>
      m.bindCoordinationResult({
        commandId: opaque("cmd", 62) as CommandId,
        kind: "committed",
        receiptId: opaque("rcp", 7) as ReceiptId,
        resultDigest: digest("e"),
      }),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
});

test("§7.9: requested-but-unbound coordination -> disposition refuses (INVOCATION_STATE_CONFLICT)", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o: o0 } = await toRepliedModelVisible(m, out);
  let o = o0;
  m.applyEvent(ev(m, o++, coordinationCall(turnId, opaque("cmd", 61) as CommandId)));
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  assert.throws(
    () => m.coordinationDisposition(),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
});

test("stage snapshot exactness: single fresh frozen { contribution, coordinationRequest, basis, readerFence, expected, next }", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o: o0 } = await toRepliedModelVisible(m, out);
  let o = o0;
  const commandId = opaque("cmd", 61) as CommandId;
  m.applyEvent(ev(m, o++, coordinationCall(turnId, commandId)));
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const stage = m.terminalDraft();
  // PIN 2: ONE object carrying all six fields.
  assert.deepEqual(
    Object.keys(stage).sort(),
    ["basis", "contribution", "coordinationRequest", "expected", "next", "readerFence"],
  );
  // PIN 1: the REPLY leg is command-id-free; this module never mints replyCommandId.
  assert.ok(!("replyCommandId" in stage.basis.reply));
  assert.equal(stage.basis.reply.turnId, turnId);
  assert.equal(stage.basis.completed.turnId, turnId);
  // PIN 1: the coordination leg carries commandId + commandDigest (NOT command-id-free).
  assert.equal(stage.basis.coordination?.commandId, commandId);
  assert.equal(
    stage.basis.coordination?.commandDigest,
    protocolDigestOfCommand(commandId),
  );
  assert.ok(!("requestedCommandId" in (stage.basis.coordination ?? {})));
  // expected/next exact DurableTurnState.
  assert.equal(stage.expected.phase, "model_visible");
  assert.equal(stage.expected.replyCommitted, false);
  assert.equal(stage.next.phase, "completed");
  assert.equal(stage.next.replyCommitted, true);
  // reader fence carried.
  assert.equal(stage.readerFence.readerEpoch, 1);
  // Recursively deep-frozen; a caller cannot mutate later truth.
  assert.ok(Object.isFrozen(stage));
  assert.ok(Object.isFrozen(stage.basis));
  assert.ok(Object.isFrozen(stage.basis.reply));
  assert.ok(Object.isFrozen(stage.contribution));
  assert.ok(Object.isFrozen(stage.contribution.fence));
  assert.ok(Object.isFrozen(stage.expected));
  // this module committed nothing.
  assert.equal(journal.terminalCommitCalls, 0);
});

test("stage snapshot reconstructed-fresh each read: a nested caller mutation cannot change later truth (byte-identical replay)", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const draft = m.terminalDraft();
  const draft2 = m.terminalDraft();
  assert.deepEqual(draft2.contribution, draft.contribution);
  assert.deepEqual(draft2.basis, draft.basis);
  assert.notEqual(draft2, draft); // fresh wrapper each call
  assert.notEqual(draft2.basis, draft.basis); // fresh legs each call
});

test("NO synthetic evidence crosses the seam: this module stages, and a placeholder commit later collides with Lane D's real evidence", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  // this module produced NO evidence: the staged snapshot has no reply receipt/result,
  // no disposition, no TurnCompletionEvidence. Lane D builds all of that.
  const stage = m.terminalDraft();
  assert.ok(!("reply" in stage)); // no server reply
  assert.ok(!("coordination" in stage)); // no bound disposition (only a request)
  assert.equal(stage.coordinationRequest.kind, "not_requested");
  // Lane D's single, exactly-once commit succeeds.
  const committed = laneDCommitTerminal(m, journal, opaque("cmd", 42) as CommandId);
  assert.equal(committed.applied, true);
  // A DIFFERENT placeholder terminal (distinct operation) for the SAME turn later
  // collides (exactly-once) -> INVOCATION_STATE_CONFLICT. The turn can never
  // double-complete with divergent evidence.
  assert.throws(
    () => laneDCommitTerminal(m, journal, opaque("cmd", 43) as CommandId),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
});

test("Lane D terminal exact replay aliases applied:false (no double effect) after evidence+join pass", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  // Two BYTE-IDENTICAL Lane-D commits: the second aliases applied:false with no
  // double cursor effect (the port's exact operationDigest replay-aliasing).
  const first = laneDCommitTerminalFixed(m, journal);
  assert.equal(first.applied, true);
  const cursorAfter = journal.cursorNextOrdinal;
  const replay = laneDCommitTerminalFixed(m, journal);
  assert.equal(replay.applied, false);
  assert.equal(journal.cursorNextOrdinal, cursorAfter);
});

test("port INVALID_STATE_TRANSITION: a malformed terminal `next` (not completed) is rejected by the CAS", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const stage = m.terminalDraft();
  // Lane D (or a mis-driver) that commits a terminal whose `next` phase is not
  // `completed` is rejected fail-closed with INVALID_STATE_TRANSITION.
  assert.throws(
    () =>
      journal.commitTurnTerminal({
        ...stage.readerFence,
        basis: {
          reply: { ...stage.basis.reply, replyCommandId: opaque("cmd", 45) as CommandId },
          completed: stage.basis.completed,
        },
        evidence: {
          contribution: stage.contribution,
          reply: { receiptId: opaque("rcp", 3) as ReceiptId, resultDigest: digest("2") },
          coordination: { kind: "not_requested", terminalTurnId: turnId },
        },
        expected: stage.expected,
        next: { ...stage.next, phase: "model_visible" }, // malformed
        recordedAt: "2026-08-04T00:10:00.000Z",
      }),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_STATE_TRANSITION",
  );
  assert.equal(journal.turnState(turnId)?.phase, "model_visible");
});

// ===========================================================================
// NAMED KILLING CONTROLS for the five coordinator invariants.
// ===========================================================================

// PORT-AUTHORITATIVE REPLAY KILLING CONTROL.
test("port-authoritative replay: a DIFFERENT event (changed eventDigest, then changed bindingDigest) at the SAME ordinal reaches the port -> conflict, durable+cursor byte-unchanged", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;
  const activeBinding = m.activeBindingDigest;
  assert.ok(activeBinding);
  // Commit an event at ordinal 0.
  m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(journal.cursorNextOrdinal, 1);
  const durableBefore = journal.turnState(turnId);

  // A DIFFERENT event at the SAME ordinal 0 — changed eventDigest — must REACH the
  // journal (not a local shortcut) and fail there with ZERO mutation.
  assert.throws(
    () =>
      m.applyEvent(
        evAt(m, 0, digest("e"), { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }),
      ),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
  assert.deepEqual(journal.turnState(turnId), durableBefore);
  assert.equal(journal.cursorNextOrdinal, 1);

  // Separately, a changed bindingDigest at the same ordinal is rejected before any
  // mutation (required source binding, never reattributed).
  assert.throws(
    () =>
      m.applyEvent(
        evBinding(0, digest("c"), { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }),
      ),
    (e: unknown) => e instanceof TurnError && e.code === "WRITE_STARTED_BINDING_MISMATCH",
  );
  assert.deepEqual(journal.turnState(turnId), durableBefore);
  assert.equal(journal.cursorNextOrdinal, 1);

  // And a BYTE-IDENTICAL replay at ordinal 0 aliases applied:false (no double
  // effect), reaching the port which recomputes the SAME operationDigest.
  const replay = m.applyEvent(ev(m, 0, { kind: "input_written", turnId, runtimeWriteId: out.receipts.runtimeWriteId }));
  assert.equal(replay.kind, "advanced");
  if (replay.kind !== "advanced") return;
  assert.equal(replay.committed, false);
  assert.equal(journal.cursorNextOrdinal, 1);
  assert.deepEqual(journal.turnState(turnId), durableBefore);
});

// VALIDATE-BEFORE-MUTATE KILLING CONTROL.
test("coordination validate-before-mutate: a wrong-binding coordination_call leaves state byte-equivalent; the correct-binding call then succeeds ONCE", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  const commandId = opaque("cmd", 61) as CommandId;
  // Snapshot the full observable state before the wrong-binding call.
  const cursorBefore = journal.cursorNextOrdinal;
  const logBefore = journal.log.length;
  const queuedBefore = m.queuedCount;
  const durableBefore = journal.turnState(turnId);

  // A wrong-binding coordination_call is rejected BEFORE setting any field.
  assert.throws(
    () => m.applyEvent(evBinding(o, digest("f"), coordinationCall(turnId, commandId))),
    (e: unknown) => e instanceof TurnError && e.code === "WRITE_STARTED_BINDING_MISMATCH",
  );
  // Byte-equivalent: machine/journal/cursor/queue unchanged.
  assert.equal(journal.cursorNextOrdinal, cursorBefore);
  assert.equal(journal.log.length, logBefore);
  assert.equal(m.queuedCount, queuedBefore);
  assert.deepEqual(journal.turnState(turnId), durableBefore);

  // The EXACT correct-binding coordination_call now succeeds ONCE — it must NOT
  // hit SECOND_COORDINATION_CALL (proving the wrong-binding call set nothing).
  const ok = m.applyEvent(ev(m, o, coordinationCall(turnId, commandId)));
  assert.equal(ok.kind, "advanced");
  m.applyEvent(ev(m, o + 1, { kind: "turn_completed", turnId }));
  assert.equal(m.terminalDraft().basis.coordination?.commandId, commandId);
});

// UNSAFE-STEER-NEVER-ENQUEUED KILLING CONTROL.
test("unsafe steer stays Lane-D pending: ZERO queue mutation; a fresh ordinary re-admit succeeds ONCE via the ordinary path; a failed admit is not lost and never reuses the old id", async () => {
  const { m, driver, journal } = machine([
    { kind: "written" }, // first ordinary turn
    { kind: "written" }, // fresh ordinary re-admit
  ]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const turnId = out.turnId;

  // An unsafe steer (before model_visible) is REJECTED with ZERO queue mutation —
  // this module does NOT enqueue it and never reuses the active turnId (no old-ID drain).
  const queuedBefore = m.queuedCount;
  await assert.rejects(
    m.submit(submission({ kind: "steer", expectedTurnId: turnId })),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_NOT_STEERABLE",
  );
  assert.equal(m.queuedCount, queuedBefore); // no old-turnId queue entry
  assert.equal(driver.steers.length, 0);

  // Lane D retains the contribution and later re-admits it as an ORDINARY
  // submission with a NEWLY issued exact DeliveryFence/turnId. It queues while the
  // first turn is active.
  const readmit = await m.submit(submission({ kind: "ordinary" }, 7));
  assert.equal(readmit.kind, "queued");
  assert.equal(m.queuedCount, 1);

  // A drain attempt while the first turn is STILL active fails (ACTIVE_TURN_CONFLICT)
  // and does NOT lose the pending item nor reuse the old id (peek-then-admit).
  await assert.rejects(
    m.drainNext(),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
  assert.equal(m.queuedCount, 1); // still queued at the front, not lost

  // Complete the first turn (Lane D commits it), then drain: the re-admitted
  // contribution admits EXACTLY ONCE via the ordinary path at its OWN fresh turnId
  // (suffix 7), inputOrdinal 0 — never the old completed id, never a steer.
  const { o } = await toRepliedModelVisible(m, out);
  completeTurn(m, journal, turnId, o);
  const drained = await m.drainNext();
  assert.equal(drained.kind, "written");
  if (drained.kind !== "written") return;
  assert.equal(drained.turnId, opaque("trn", 7) as TurnId);
  assert.notEqual(drained.turnId, turnId);
  assert.equal(drained.inputOrdinal, 0);
  assert.equal(m.queuedCount, 0);
  assert.equal(driver.steers.length, 0);
});

// ATOMIC-COMMIT-TERMINAL KILLING CONTROLS (fake mirrors promoted commitTurnTerminal EXACTLY).
test("5(a) evidence-divergence: changing the evidence yields a different operationDigest -> INVOCATION_STATE_CONFLICT, not alias", async () => {
  const { m, journal } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  // First commit succeeds.
  const first = laneDCommitTerminal(m, journal, opaque("cmd", 40) as CommandId, {
    replyResultDigest: digest("2"),
  });
  assert.equal(first.applied, true);
  // Same replyCommandId + same expected/next/basis, but CHANGED evidence
  // (different reply.resultDigest) -> different operationDigest -> divergence.
  assert.throws(
    () =>
      laneDCommitTerminal(m, journal, opaque("cmd", 40) as CommandId, { replyResultDigest: digest("9") }),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
});

test("5(b) wrong-stored-fence: evidence verified against a WRONG stored fence fails (evidence's own fence is never the root)", async () => {
  const { m, journal, attempts } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  // Corrupt the STORED fence (attempt.fence_json ground truth) so it no longer
  // matches the staged evidence's fence. The port verifies against the STORED
  // fence, so verification fails even though the evidence is internally consistent.
  const chain = attempts.chains.get(turnId as unknown as string);
  assert.ok(chain);
  attempts.chains.set(turnId as unknown as string, {
    ...chain,
    storedFence: { ...chain.storedFence, deliveryId: opaque("dlv", 9) as DeliveryId },
  });
  assert.throws(
    () => laneDCommitTerminal(m, journal),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_JOURNAL_CHAIN",
  );
});

test("5(c) corrupted-chain-no-alias: after a successful commit, corrupting the stored chain makes a byte-exact-digest replay NOT alias (chain check runs first)", async () => {
  const { m, journal, attempts } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const committed = laneDCommitTerminalFixed(m, journal);
  assert.equal(committed.applied, true);
  // Corrupt the stored entry chain (break the root anchor). A byte-exact-digest
  // replay must NOT alias — the chain check runs BEFORE the replay-alias.
  const chain = attempts.chains.get(turnId as unknown as string);
  assert.ok(chain);
  attempts.chains.set(turnId as unknown as string, { ...chain, sequences: [2, 3, 4, 5] });
  assert.throws(
    () => laneDCommitTerminalFixed(m, journal),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_JOURNAL_CHAIN",
  );
});

test("5(d) alias-after-consumed: a byte-exact-digest replay AFTER the attempt advanced to consumed still aliases {applied:false}", async () => {
  const { m, journal, attempts } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  if (out.kind !== "written") return;
  const { turnId, o } = await toRepliedModelVisible(m, out);
  m.applyEvent(ev(m, o, { kind: "turn_completed", turnId }));
  const first = laneDCommitTerminalFixed(m, journal);
  assert.equal(first.applied, true);
  // The commit already advanced the attempt to `consumed`. A byte-exact-digest
  // replay STILL aliases {applied:false} — the first-insert predecessor gates do
  // NOT run on the replay path (digest-equality alone).
  assert.equal(attempts.chains.get(turnId as unknown as string)?.attemptState, "consumed");
  const cursorAfter = journal.cursorNextOrdinal;
  const replay = laneDCommitTerminalFixed(m, journal);
  assert.equal(replay.applied, false);
  assert.equal(journal.cursorNextOrdinal, cursorAfter);
});

// ===========================================================================
// FRESH-PROCESS RECOVERY KILLING CONTROLS (composition of the
// promoted durable turn-recovery read + retained finite replay + no-active
// live-resume). The fakes below are wire-identical mirrors of the promoted
// shapes; they PRODUCE the same replayable basis / retained records the concrete
// seams do, so the machine RE-VERIFIES the reconstructed contribution through the
// SAME buildContributionBinding / verifyContributionBinding protocol paths.
// ===========================================================================

const RECOVER_VERSION = version;

// The fresh-process config (same fences the same-process machine uses).
function recoverConfig(): MachineConfig {
  return config();
}

// The recovered contribution's fence (== a same-process ordinary turn fence).
function recoverFence(deliverySuffix = 5): DeliveryFence {
  return {
    ...fence(),
    deliveryId: opaque("dlv", deliverySuffix) as DeliveryId,
    turnId: opaque("trn", deliverySuffix) as TurnId,
  };
}

type RecoveredIds = {
  invocationId: CommandId;
  invocationGeneration: number;
  permitId: CommandId;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
};

function recoveredIds(): RecoveredIds {
  return {
    invocationId: opaque("cmd", 10) as CommandId,
    invocationGeneration: 1,
    permitId: opaque("cmd", 11) as CommandId,
    runtimeWriteId: opaque("cmd", 12) as CommandId,
    visibilityEventId: opaque("cmd", 13) as CommandId,
  };
}

// The REAL per-contribution binding digest for the recovered contribution: the
// machine re-verifies against exactly this via buildContributionBinding.
function recoveredBindingDigest(
  fenceValue: DeliveryFence,
  ids: RecoveredIds,
  inputOrdinal = 0,
): ArtifactDigest {
  return buildContributionBinding({
    fence: fenceValue,
    stateInstanceId: recoverConfig().stateInstanceId,
    inputOrdinal,
    invocationId: ids.invocationId,
    invocationGeneration: ids.invocationGeneration,
    permitId: ids.permitId,
    runtimeWriteId: ids.runtimeWriteId,
    visibilityEventId: ids.visibilityEventId,
  }).contributionBindingDigest;
}

const RECOVER_SOURCE_MESSAGE = opaque("msg", 5) as MessageId;

// Build a promoted-identical ReplayableTurnRecoveryBasis for a recovered
// model_visible contribution.
function replayableBasis(
  fenceValue: DeliveryFence,
  ids: RecoveredIds,
  inputOrdinal = 0,
): ReplayableTurnRecoveryBasis {
  return {
    fence: fenceValue,
    deliveryId: fenceValue.deliveryId,
    attempt: fenceValue.attempt,
    sourceMessageId: RECOVER_SOURCE_MESSAGE,
    protocolTurnId: fenceValue.turnId,
    launchId: recoverConfig().launchId,
    stateInstanceId: recoverConfig().stateInstanceId,
    sessionId: recoverConfig().sessionId,
    rootProducerFactId: fenceValue.producerFactId,
    inputOrdinal,
    driverTurnRefDigest: digest("7"),
    mode: { kind: "ordinary" },
    invocationId: ids.invocationId,
    invocationGeneration: ids.invocationGeneration,
    permitId: ids.permitId,
    runtimeWriteId: ids.runtimeWriteId,
    visibilityEventId: ids.visibilityEventId,
    bindingDigest: recoveredBindingDigest(fenceValue, ids, inputOrdinal),
    durable: "model_visible",
    attemptState: "model_visible",
    entryChainDepth: 4,
  };
}

// A retained turn-suffix record (wire-identical to RetainedDriverEventRecord)
// riding the exact watermark-anchored lease + expectation fences.
function retainedRecord(
  lease: RetainedEventLease,
  basis: ReplayableTurnRecoveryBasis,
  ordinal: number,
  previousRecordDigest: ArtifactDigest | null,
  event: Extract<NormalizedDriverEvent, { turnId: TurnId }>,
): RetainedTurnEventRecord {
  return {
    protocolVersion: lease.protocolVersion,
    launchId: lease.launchId,
    stateInstanceId: lease.stateInstanceId,
    sessionId: lease.sessionId,
    keyGeneration: 1,
    ordinal,
    previousRecordDigest,
    resolvedWaiterId: basis.invocationId,
    sourceMessageId: basis.sourceMessageId,
    turnId: basis.protocolTurnId,
    bindingDigest: basis.bindingDigest,
    eventKind: event.kind,
    payloadCipherDigest: digest("c"),
    eventDigest: digest(alphabet[ordinal % 32] ?? "0"),
    recordDigest: canonicalDigest(["rec", ordinal, event.kind, basis.protocolTurnId]),
    stream: "turn",
    readerEpoch: lease.readerEpoch,
    event,
  };
}

// A fresh composite claim (storage cursor + private retained lease) at the exact
// watermark. `abort`/`release` record their calls so effect-free-teardown proofs
// can assert exactly one of them fired on every fail-closed path.
class FakeCompositeClaim implements RecoveryCompositeCursorClaim {
  aborts = 0;
  releases = 0;
  readonly authority: RecoveryCursorAuthority;
  readonly privateLease: RetainedEventLease;
  constructor(
    watermark: { nextOrdinal: number; lastEventDigest: ArtifactDigest | null },
    epoch = 1,
    replayMode: "live" | "retained_only" = "retained_only",
    overrides: Partial<RetainedEventLease> = {},
  ) {
    const cfg = recoverConfig();
    this.authority = {
      stateInstanceId: cfg.stateInstanceId,
      sessionId: cfg.sessionId,
      nextOrdinal: watermark.nextOrdinal,
      lastEventDigest: watermark.lastEventDigest,
      protocolVersion: RECOVER_VERSION,
      launchId: cfg.launchId,
      ownerToken: cfg.readerOwnerToken,
      readerEpoch: epoch,
    };
    this.privateLease = {
      stateInstanceId: cfg.stateInstanceId,
      sessionId: cfg.sessionId,
      nextOrdinal: watermark.nextOrdinal,
      lastEventDigest: watermark.lastEventDigest,
      protocolVersion: RECOVER_VERSION,
      launchId: cfg.launchId,
      ownerToken: cfg.readerOwnerToken,
      readerEpoch: epoch,
      claimAttemptId: opaque("cmd", 20) as CommandId,
      processMode: "resume",
      replayMode,
      snapshotHeadNextOrdinal: watermark.nextOrdinal,
      ...overrides,
    };
  }
  abort(): Promise<RecoveryCompositeClaimCloseResult> {
    this.aborts += 1;
    return Promise.resolve({ applied: true, storageReleased: true, privateReleased: true });
  }
  release(): Promise<RecoveryCompositeClaimCloseResult> {
    this.releases += 1;
    return Promise.resolve({ applied: true, storageReleased: true, privateReleased: true });
  }
}

class FakeRecoveryClaimPort implements TurnRecoveryClaimPort {
  claims = 0;
  constructor(readonly claim: FakeCompositeClaim) {}
  claimForReplay(): Promise<RecoveryCompositeCursorClaim> {
    this.claims += 1;
    return Promise.resolve(this.claim);
  }
}

class FakeRecoveryReadPort implements TurnRecoveryReadPort {
  reads = 0;
  constructor(readonly result: TurnRecoveryReadResult | null | (() => never)) {}
  readTurnRecovery(): TurnRecoveryReadResult | null {
    this.reads += 1;
    if (typeof this.result === "function") return this.result();
    return this.result;
  }
}

// Retained-prefix finalization mirror. This aligns the in-memory retained source
// with the promoted retained-replay port's finalizer: it accepts EXACTLY the
// legal finite prefixes the real port accepts { empty, boundary-only, reply-only,
// reply + coordination, completed } and REJECTS exactly the same illegal orderings
// { coordination before reply, completion before reply, duplicate reply, duplicate
// coordination, event after completion, pre-watermark kind }, and it distinguishes
// the "active" vs "completed" outcome the real finalizer returns. It is a whole-
// prefix check over a suffix that starts immediately after a durable model_visible
// event; no provider re-execution. Illegal orderings surface the same TurnError
// `.code`s the machine reducer uses so the source never loosens the port boundary.
function retainedPrefixOutcome(
  records: readonly RetainedTurnEventRecord[],
): "active" | "completed" {
  // The suffix begins immediately after a durable model_visible event: the turn is
  // model-visible, no reply/coordination seen, and not yet completed.
  let completed = false;
  let replySeen = false;
  let coordinationSeen = false;
  for (const record of records) {
    if (completed) throw new TurnError("INVALID_STATE_TRANSITION", "event_after_completion");
    switch (record.event.kind) {
      case "turn_boundary":
        // A boundary is legal at model_visible and leaves the turn active.
        break;
      case "assistant_reply":
        if (replySeen) throw new TurnError("MULTIPLE_ASSISTANT_REPLIES");
        replySeen = true;
        break;
      case "coordination_call":
        if (!replySeen) {
          throw new TurnError("ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE", "coordination_before_reply");
        }
        if (coordinationSeen) throw new TurnError("SECOND_COORDINATION_CALL");
        coordinationSeen = true;
        break;
      case "turn_completed":
        if (!replySeen) throw new TurnError("TURN_COMPLETION_WITHOUT_REPLY");
        completed = true;
        break;
      default:
        // turn_started / input_written / model_visible belong to the pre-watermark
        // prefix; a suffix record of these kinds is an impossible/cross-fenced cell.
        throw new TurnError("INVALID_STATE_TRANSITION", { at: "retained_suffix", kind: record.event.kind });
    }
  }
  // The finalizer's cut: a completed suffix reports "completed"; any legal open
  // prefix (empty / boundary / reply / reply + coordination) reports "active".
  return completed ? "completed" : "active";
}

// A finite retained replay over a fixed snapshot; `closes` proves the claim is
// released after a successful suffix consumption. Mirroring the promoted
// FiniteDriverRetainedReplay, the whole finite prefix is finalized at construction
// (before its first record is exposed) so the accept/reject boundary and the
// active-vs-completed outcome match the real port EXACTLY. This faithful mirror
// has NO finalization bypass: every legal prefix is accepted and every illegal one
// is rejected before first yield, precisely as the promoted source does.
class FakeRetainedReplay implements RetainedTurnReplay {
  closes = 0;
  readonly lease: RetainedEventLease;
  readonly records: AsyncIterable<RetainedTurnEventRecord>;
  readonly outcome: "active" | "completed";
  constructor(
    lease: RetainedEventLease,
    records: readonly RetainedTurnEventRecord[],
    readonly claim: FakeCompositeClaim,
  ) {
    this.lease = lease;
    // Finalize the whole prefix BEFORE exposing any record (real-port parity):
    // accept the legal prefixes, reject the illegal ones, and classify the cut.
    this.outcome = retainedPrefixOutcome(records);
    this.records = (async function* () {
      for (const r of records) yield r;
    })();
  }
  async close(): Promise<void> {
    this.closes += 1;
    await this.claim.release();
  }
}

// The retained source: builds a suffix from the recovered basis. `suffix`
// receives (lease, basis) and returns the ordered records (possibly stale/cross-
// fenced for the fail-closed controls). `throwOnOpen` forces an openReplay error.
// This faithful source ALWAYS finalizes the prefix (real-port parity) and has no
// bypass. (A separate, explicitly contract-violating adversarial double lives
// below for the machine-reducer defense-in-depth control.)
class FakeRetainedSource implements RetainedTurnEventSource {
  opens = 0;
  lastReplay: FakeRetainedReplay | undefined;
  constructor(
    readonly basis: ReplayableTurnRecoveryBasis,
    readonly suffix: (
      lease: RetainedEventLease,
      basis: ReplayableTurnRecoveryBasis,
    ) => readonly RetainedTurnEventRecord[],
    readonly opts: {
      throwOnOpen?: boolean;
      leaseOverrides?: Partial<RetainedEventLease>;
      expectationSpy?: (e: RetainedReplayExpectation) => void;
    } = {},
  ) {}
  openReplay(
    input: RetainedReplayExpectation & { claim: RecoveryCompositeCursorClaim },
  ): Promise<RetainedTurnReplay> {
    this.opens += 1;
    this.opts.expectationSpy?.(input);
    if (this.opts.throwOnOpen) {
      return Promise.reject(new TurnError("DRIVER_EVENT_ORDER_INVALID", "retained_gap"));
    }
    const claim = input.claim as FakeCompositeClaim;
    // The private snapshot head equals the watermark + the retained suffix length
    // (the real retention lease exposes the same head), unless a control overrides
    // it to force a gap.
    const baseLease: RetainedEventLease = { ...claim.privateLease };
    const records = this.suffix(baseLease, this.basis);
    const lease: RetainedEventLease = {
      ...baseLease,
      snapshotHeadNextOrdinal: baseLease.nextOrdinal + records.length,
      ...this.opts.leaseOverrides,
    };
    const replay = new FakeRetainedReplay(lease, records, claim);
    this.lastReplay = replay;
    return Promise.resolve(replay);
  }
}

// An ADVERSARIAL retained double that DELIBERATELY VIOLATES the source contract:
// it exposes a structurally-fenced but reducer-invalid suffix WITHOUT the
// retained-prefix finalization the faithful FakeRetainedSource always performs.
// It is NOT the product-faithful mirror and never stands in for it; it exists ONLY
// to prove the machine's own #reduceRetainedInto is an INDEPENDENT fail-closed
// authority (defense-in-depth) should a future source regression ever expose such
// a suffix. `closeThrows` additionally fails teardown, to prove a reduce failure
// and a close failure surface together without masking.
class AdversarialUnfinalizedReplay implements RetainedTurnReplay {
  closes = 0;
  readonly lease: RetainedEventLease;
  readonly records: AsyncIterable<RetainedTurnEventRecord>;
  constructor(
    lease: RetainedEventLease,
    records: readonly RetainedTurnEventRecord[],
    readonly claim: FakeCompositeClaim,
    readonly closeThrows = false,
  ) {
    this.lease = lease;
    // Deliberately NO finalization: the faithful source rejects this suffix before
    // first yield; this double skips that so the record reaches the machine reducer.
    this.records = (async function* () {
      for (const r of records) yield r;
    })();
  }
  async close(): Promise<void> {
    this.closes += 1;
    await this.claim.release();
    if (this.closeThrows) {
      throw new TurnError("INVALID_STATE_TRANSITION", "adversarial_teardown_failure");
    }
  }
}

class AdversarialUnfinalizedSource implements RetainedTurnEventSource {
  opens = 0;
  lastReplay: AdversarialUnfinalizedReplay | undefined;
  constructor(
    readonly basis: ReplayableTurnRecoveryBasis,
    readonly suffix: (
      lease: RetainedEventLease,
      basis: ReplayableTurnRecoveryBasis,
    ) => readonly RetainedTurnEventRecord[],
    readonly opts: { closeThrows?: boolean } = {},
  ) {}
  openReplay(
    input: RetainedReplayExpectation & { claim: RecoveryCompositeCursorClaim },
  ): Promise<RetainedTurnReplay> {
    this.opens += 1;
    const claim = input.claim as FakeCompositeClaim;
    const baseLease: RetainedEventLease = { ...claim.privateLease };
    const records = this.suffix(baseLease, this.basis);
    const lease: RetainedEventLease = {
      ...baseLease,
      snapshotHeadNextOrdinal: baseLease.nextOrdinal + records.length,
    };
    const replay = new AdversarialUnfinalizedReplay(lease, records, claim, this.opts.closeThrows);
    this.lastReplay = replay;
    return Promise.resolve(replay);
  }
}

// The no-active live-resume handoff. Records each one-shot arm.
class FakeLiveResumePort implements TurnLiveResumePort {
  releases = 0;
  begins = 0;
  liveClaims = 0;
  lastAuthorization: RecoveryLiveResumeAuthorization | undefined;
  lastTicket: RecoveryLiveResumeTicket | undefined;
  liveClaim: FakeCompositeClaim | undefined;
  releaseReplayAsNoActive(
    claim: RecoveryCompositeCursorClaim,
  ): Promise<RecoveryLiveResumeAuthorization> {
    this.releases += 1;
    // Mirror the promoted contract: releaseReplayAsNoActive ABORTS the
    // retained_only claim before minting the authorization.
    void claim.abort();
    const a = claim.authority;
    const authorization: RecoveryLiveResumeAuthorization = Object.freeze({
      stateInstanceId: a.stateInstanceId,
      sessionId: a.sessionId,
      nextOrdinal: a.nextOrdinal,
      lastEventDigest: a.lastEventDigest,
      protocolVersion: a.protocolVersion,
      launchId: a.launchId,
      ownerToken: a.ownerToken,
      provedReaderEpoch: a.readerEpoch,
    });
    this.lastAuthorization = authorization;
    return Promise.resolve(authorization);
  }
  beginLiveResume(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    authorization: RecoveryLiveResumeAuthorization;
  }): RecoveryLiveResumeTicket {
    this.begins += 1;
    if (this.lastAuthorization === undefined || input.authorization !== this.lastAuthorization) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "live_resume_authorization");
    }
    const ticket: RecoveryLiveResumeTicket = Object.freeze({
      stateInstanceId: input.authorization.stateInstanceId,
      sessionId: input.authorization.sessionId,
      nextOrdinal: input.authorization.nextOrdinal,
      lastEventDigest: input.authorization.lastEventDigest,
      protocolVersion: input.authorization.protocolVersion,
      launchId: input.authorization.launchId,
      ownerToken: input.authorization.ownerToken,
    });
    this.lastTicket = ticket;
    return ticket;
  }
  claimForLiveResume(input: { ticket: RecoveryLiveResumeTicket }): Promise<RecoveryCompositeCursorClaim> {
    this.liveClaims += 1;
    if (this.lastTicket === undefined || input.ticket !== this.lastTicket) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "live_resume_ticket");
    }
    const claim = new FakeCompositeClaim(
      { nextOrdinal: input.ticket.nextOrdinal, lastEventDigest: input.ticket.lastEventDigest },
      9,
      "live",
    );
    this.liveClaim = claim;
    return Promise.resolve(claim);
  }
}

function recoverPorts(input: {
  claim: FakeCompositeClaim;
  recovery: TurnRecoveryReadResult | null | (() => never);
  basis?: ReplayableTurnRecoveryBasis;
  suffix?: (
    lease: RetainedEventLease,
    basis: ReplayableTurnRecoveryBasis,
  ) => readonly RetainedTurnEventRecord[];
  retainedOpts?: {
    throwOnOpen?: boolean;
    leaseOverrides?: Partial<RetainedEventLease>;
    expectationSpy?: (e: RetainedReplayExpectation) => void;
  };
}): FreshProcessRecoveryPorts & {
  claimPort: FakeRecoveryClaimPort;
  readPort: FakeRecoveryReadPort;
  source: FakeRetainedSource | undefined;
  live: FakeLiveResumePort;
} {
  const claimPort = new FakeRecoveryClaimPort(input.claim);
  const readPort = new FakeRecoveryReadPort(input.recovery);
  const live = new FakeLiveResumePort();
  const source =
    input.basis !== undefined && input.suffix !== undefined
      ? new FakeRetainedSource(input.basis, input.suffix, input.retainedOpts ?? {})
      : undefined;
  const emptySource: RetainedTurnEventSource = {
    openReplay: () => Promise.reject(new Error("no retained source configured")),
  };
  return {
    claim: claimPort,
    recovery: readPort,
    retained: source ?? emptySource,
    liveResume: live,
    claimPort,
    readPort,
    source,
    live,
  };
}

// A fresh machine (no submit) so recovery runs against a truly fresh process.
function freshMachine(): { m: TurnMachine; journal: MemoryTurnJournal } {
  const attempts = new NativeAttemptStore();
  const journal = new MemoryTurnJournal(attempts);
  const reader = new MemoryReaderPort(journal);
  const driver = new MemoryTurnDriver(attempts, []);
  const m = new TurnMachine({ journal, reader, driver, ids: new Ids(), config: recoverConfig() });
  return { m, journal };
}

// A recovered turn's full suffix: assistant_reply -> coordination_call ->
// turn_completed (rebuilt in memory only). Ordinals start at the watermark.
function fullSuffix(
  lease: RetainedEventLease,
  basis: ReplayableTurnRecoveryBasis,
): readonly RetainedTurnEventRecord[] {
  const t = basis.protocolTurnId;
  const r0 = retainedRecord(lease, basis, lease.nextOrdinal, null, {
    kind: "assistant_reply",
    turnId: t,
    text: "recovered-reply",
  });
  const r1 = retainedRecord(lease, basis, lease.nextOrdinal + 1, r0.recordDigest, {
    kind: "coordination_call",
    turnId: t,
    commandId: opaque("cmd", 30) as CommandId,
    command: { protocolVersion: version, title: "recovered", sourceMessageId: opaque("msg", 6) as never },
  });
  const r2 = retainedRecord(lease, basis, lease.nextOrdinal + 2, r1.recordDigest, {
    kind: "turn_completed",
    turnId: t,
  });
  return [r0, r1, r2];
}

test("fresh-process recovery: replayable rebuilds #active + cursor + re-verifies (reply/coordination/terminal in memory only)", async () => {
  const { m, journal } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    suffix: fullSuffix,
  });

  const outcome = await m.recoverFreshProcess(ports);
  assert.equal(outcome.kind, "recovered");
  if (outcome.kind !== "recovered") return;
  // #active restored ONLY from the replayable arm; the recovered turn identity is
  // the replayable basis' turn.
  assert.equal(outcome.turnId, fenceValue.turnId);
  // The full suffix ends in turn_completed, so the recovered contribution is
  // STAGED-TERMINAL (same-process semantics: #active clears, #terminal is set).
  assert.equal(outcome.terminalStaged, true);
  assert.equal(m.activeTurnId, null);
  // Authoritative cursor set from the recovery cursor pair.
  assert.equal(outcome.nextOrdinal, watermark.nextOrdinal);
  // Reply + coordination + terminal stage rebuilt IN MEMORY (turn-wide reply=1 was
  // observed during the in-memory replay).
  assert.equal(outcome.replyStaged, true);
  // The terminal stage re-verifies through buildContributionBinding /
  // verifyContributionBinding: it is exposable and carries the recovered turn.
  const stage = m.terminalDraft();
  assert.equal(stage.contribution.fence.turnId, fenceValue.turnId);
  assert.equal(stage.coordinationRequest.kind, "requested");
  assert.equal(stage.next.replyCommitted, true);
  // NO terminal stage / reply text / title reached shared storage: this module made ZERO
  // journal writes during recovery (the fake journal's log is empty; no terminal
  // commit; the durable turn row is untouched by this module).
  assert.equal(journal.log.length, 0);
  assert.equal(journal.terminalCommitCalls, 0);
  assert.equal(journal.turnState(fenceValue.turnId), undefined);
  // The finite suffix was consumed and the composite claim released once.
  assert.equal(ports.source?.lastReplay?.closes, 1);
  assert.equal(claim.releases, 1);
  assert.equal(claim.aborts, 0);
  // NO provider re-execution: the retained source replays; no driver write/steer.
  assert.equal(ports.source?.opens, 1);
});

test("fresh-process recovery: replayable with a PARTIAL suffix (reply only, no turn_completed) keeps #active alive with rebuilt reply + binding", async () => {
  const { m, journal } = freshMachine();
  const fenceValue = recoverFence(6);
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 3, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 4);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    // Suffix does NOT reach turn_completed: reply staged, still steerable-active.
    suffix: (lease, b) => [
      retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "assistant_reply",
        turnId: b.protocolTurnId,
        text: "partial-reply",
      }),
    ],
  });
  const outcome = await m.recoverFreshProcess(ports);
  assert.equal(outcome.kind, "recovered");
  if (outcome.kind !== "recovered") return;
  // #active is alive at model_visible, re-verified binding, reply STAGED in memory.
  assert.equal(m.activeTurnId, fenceValue.turnId);
  assert.equal(m.activePhase, "model_visible");
  assert.equal(m.activeBindingDigest, basis.bindingDigest);
  assert.equal(m.replyCount, 1);
  assert.equal(outcome.replyStaged, true);
  assert.equal(outcome.terminalStaged, false);
  // Cursor set from the recovery pair; NOTHING entered shared storage.
  assert.equal(outcome.nextOrdinal, watermark.nextOrdinal);
  assert.equal(journal.log.length, 0);
  assert.equal(journal.turnState(fenceValue.turnId), undefined);
});

test("fresh-process recovery: null -> live-resume transition composes ONLY after a valid no_active proof; ZERO #active/effect", async () => {
  const { m, journal } = freshMachine();
  const watermark = { nextOrdinal: 4, lastEventDigest: digest("b") };
  const claim = new FakeCompositeClaim(watermark, 2);
  const ports = recoverPorts({ claim, recovery: null });

  const outcome = await m.recoverFreshProcess(ports);
  assert.equal(outcome.kind, "live_resume");
  if (outcome.kind !== "live_resume") return;
  // The three one-shot arms fired exactly once, in order.
  assert.equal(ports.live.releases, 1);
  assert.equal(ports.live.begins, 1);
  assert.equal(ports.live.liveClaims, 1);
  // releaseReplayAsNoActive ABORTED the retained_only claim (no live #active).
  assert.equal(claim.aborts, 1);
  assert.equal(m.activeTurnId, null);
  // ZERO journal/cursor/queue effect.
  assert.equal(journal.log.length, 0);
  assert.equal(journal.cursorNextOrdinal, 0);
  assert.equal(m.queuedCount, 0);
  // The fresh live claim is handed back for the caller to wire the live pump.
  assert.equal(outcome.claim, ports.live.liveClaim);
});

for (const reason of ["PRE_MODEL_VISIBLE_EFFECT_UNKNOWN", "ACTIVE_AMBIGUOUS"] as const) {
  test(`fresh-process recovery: held_ambiguous(${reason}) -> abort claim, ZERO effect, NO #active`, async () => {
    const { m, journal } = freshMachine();
    const claim = new FakeCompositeClaim({ nextOrdinal: 0, lastEventDigest: null }, 1);
    const ports = recoverPorts({ claim, recovery: { kind: "held_ambiguous", reason } });

    const outcome = await m.recoverFreshProcess(ports);
    assert.equal(outcome.kind, "held_ambiguous");
    if (outcome.kind !== "held_ambiguous") return;
    assert.equal(outcome.reason, reason);
    // Composite claim aborted; NO #active; ZERO journal/cursor/live-resume effect.
    assert.equal(claim.aborts, 1);
    assert.equal(claim.releases, 0);
    assert.equal(m.activeTurnId, null);
    assert.equal(journal.log.length, 0);
    assert.equal(ports.live.releases, 0);
    assert.equal(ports.live.begins, 0);
  });
}

test("fresh-process recovery: readTurnRecovery ERROR -> abort claim, re-throw, NO #active, ZERO effect", async () => {
  const { m, journal } = freshMachine();
  const claim = new FakeCompositeClaim({ nextOrdinal: 1, lastEventDigest: digest("a") }, 1);
  const ports = recoverPorts({
    claim,
    recovery: () => {
      throw new TurnError("ACTIVE_TURN_CONFLICT", "ambiguous_multi_candidate");
    },
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "ACTIVE_TURN_CONFLICT",
  );
  assert.equal(claim.aborts, 1);
  assert.equal(m.activeTurnId, null);
  assert.equal(journal.log.length, 0);
  assert.equal(ports.live.releases, 0);
});

test("fresh-process recovery: cursor-pair divergence from the claim watermark -> fail-closed abort, NO #active", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  // The claim's authority watermark and the recovery cursor pair DISAGREE.
  const claim = new FakeCompositeClaim({ nextOrdinal: 2, lastEventDigest: digest("a") }, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: { nextOrdinal: 5, lastEventDigest: digest("f") } },
    basis,
    suffix: fullSuffix,
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(claim.aborts, 1);
  assert.equal(m.activeTurnId, null);
  // The retained source was NEVER opened (fail-closed BEFORE any replay).
  assert.equal(ports.source?.opens, 0);
});

test("fresh-process recovery: basis bindingDigest that does NOT match the re-verified protocol digest -> fail-closed abort", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = { ...replayableBasis(fenceValue, ids), bindingDigest: digest("e") };
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    suffix: fullSuffix,
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "WRITE_STARTED_BINDING_MISMATCH",
  );
  assert.equal(claim.aborts, 1);
  assert.equal(m.activeTurnId, null);
  assert.equal(ports.source?.opens, 0);
});

test("fresh-process recovery: retained suffix opened from the EXACT watermark under the four expectation fences", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  let seen: RetainedReplayExpectation | undefined;
  const source = new FakeRetainedSource(basis, fullSuffix, {
    expectationSpy: (e) => {
      seen = e;
    },
  });
  const live = new FakeLiveResumePort();
  await m.recoverFreshProcess({
    claim: new FakeRecoveryClaimPort(claim),
    recovery: new FakeRecoveryReadPort({ kind: "replayable", basis, cursor: watermark }),
    retained: source,
    liveResume: live,
  });
  assert.ok(seen);
  assert.equal(seen?.expectedTurnId, fenceValue.turnId);
  assert.equal(seen?.expectedBindingDigest, basis.bindingDigest);
  // The resolved waiter is the invocationId; the source message is authenticated.
  assert.equal(seen?.expectedResolvedWaiterId, ids.invocationId);
  assert.equal(seen?.expectedSourceMessageId, RECOVER_SOURCE_MESSAGE);
});

test("fresh-process recovery: below-watermark retained record -> rejected fail-closed, effect-free (NO #active)", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 5, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    // First record at ordinal 4 (BELOW the watermark 5): must be rejected.
    suffix: (lease, b) => [
      retainedRecord(lease, b, lease.nextOrdinal - 1, null, {
        kind: "assistant_reply",
        turnId: b.protocolTurnId,
        text: "stale",
      }),
    ],
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(m.activeTurnId, null);
  // The suffix is materialized+validated BEFORE any machine mutation -> NO #active.
  assert.equal(m.replyCount, 0);
});

test("fresh-process recovery: cross-fenced retained record (wrong bindingDigest) -> rejected fail-closed, effect-free", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    suffix: (lease, b) => [
      {
        ...retainedRecord(lease, b, lease.nextOrdinal, null, {
          kind: "assistant_reply",
          turnId: b.protocolTurnId,
          text: "x",
        }),
        // A stale/old binding cross-fence: not the recovered contribution's fence.
        bindingDigest: digest("e"),
      },
    ],
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(m.activeTurnId, null);
  assert.equal(m.replyCount, 0);
});

test("fresh-process recovery: missing/incomplete retained suffix (gap before snapshot head) -> rejected fail-closed", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    // Suffix has ONE record but the private snapshot head is 3 ordinals ahead:
    // a gap. Force snapshotHeadNextOrdinal beyond the delivered records.
    suffix: (lease, b) => [
      retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "assistant_reply",
        turnId: b.protocolTurnId,
        text: "x",
      }),
    ],
    retainedOpts: { leaseOverrides: { snapshotHeadNextOrdinal: watermark.nextOrdinal + 3 } },
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_ORDER_INVALID",
  );
  assert.equal(m.activeTurnId, null);
});

test("fresh-process recovery: retained openReplay error -> fail-closed, NO #active (no provider re-execution)", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    suffix: fullSuffix,
    retainedOpts: { throwOnOpen: true },
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "DRIVER_EVENT_ORDER_INVALID",
  );
  assert.equal(claim.aborts, 1);
  assert.equal(m.activeTurnId, null);
});

test("fresh-process recovery: an impossible pre-watermark event kind in the suffix -> fail-closed", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    // model_visible belongs to the PRE-watermark prefix; a suffix record of this
    // kind is an impossible/cross-fenced cell -> fail-closed.
    suffix: (lease, b) => [
      retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "model_visible",
        turnId: b.protocolTurnId,
        visibilityEventId: ids.visibilityEventId,
      }),
    ],
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_STATE_TRANSITION",
  );
  assert.equal(m.activeTurnId, null);
});

test("fresh-process recovery: recovery on a NON-fresh machine (already active) -> fail-closed, never invents", async () => {
  const { m } = machine([{ kind: "written" }]);
  m.claimReader("start");
  const out = await m.submit(submission({ kind: "ordinary" }));
  assert.equal(out.kind, "written");
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    suffix: fullSuffix,
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "INVALID_STATE_TRANSITION",
  );
  // The claim was never even opened (precondition fails first).
  assert.equal(ports.claimPort.claims, 0);
});

test("fresh-process recovery: recovered contribution re-verifies via buildContributionBinding AND Lane D can commit its terminal", async () => {
  const { m, journal } = freshMachine();
  const fenceValue = recoverFence();
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    suffix: fullSuffix,
  });
  await m.recoverFreshProcess(ports);
  // The staged terminal from the recovered+replayed contribution is a fully
  // verified ContributionBinding (verifyContributionBinding inside terminalDraft).
  const stage = m.terminalDraft();
  assert.equal(stage.contribution.contributionBindingDigest, basis.bindingDigest);
  assert.equal(stage.contribution.runtimeWriteId, ids.runtimeWriteId);
  assert.equal(stage.contribution.visibilityEventId, ids.visibilityEventId);
  assert.equal(stage.contribution.permitId, ids.permitId);
  assert.equal(stage.contribution.invocationId, ids.invocationId);
  // The command-id-free reply leg + the coordination leg (with commandDigest) are
  // rebuilt in memory; the completed leg is contiguous.
  assert.equal(stage.basis.completed.ordinal, watermark.nextOrdinal + 2);
  assert.ok(stage.basis.coordination);
  assert.equal(journal.terminalCommitCalls, 0);
});

// The legal finite prefixes the retained-replay port accepts, each of which the
// source finalizer classifies "active" (still steerable/model_visible), so the
// machine keeps the recovered turn alive and re-verifies its binding in memory.
for (const legal of [
  {
    name: "empty",
    suffix: () => [] as readonly RetainedTurnEventRecord[],
    reply: false,
  },
  {
    name: "boundary-only",
    suffix: (lease: RetainedEventLease, b: ReplayableTurnRecoveryBasis) => [
      retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "turn_boundary",
        turnId: b.protocolTurnId,
        boundary: "tool",
        steerable: true,
      }),
    ],
    reply: false,
  },
  {
    name: "reply and coordination",
    suffix: (lease: RetainedEventLease, b: ReplayableTurnRecoveryBasis) => {
      const r0 = retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "assistant_reply",
        turnId: b.protocolTurnId,
        text: "active-reply",
      });
      const r1 = retainedRecord(lease, b, lease.nextOrdinal + 1, r0.recordDigest, {
        kind: "coordination_call",
        turnId: b.protocolTurnId,
        commandId: opaque("cmd", 31) as CommandId,
        command: { protocolVersion: version, title: "active", sourceMessageId: opaque("msg", 6) as never },
      });
      return [r0, r1];
    },
    reply: true,
  },
] as const) {
  test(`fresh-process recovery: legal ACTIVE prefix (${legal.name}) keeps the recovered turn steerable/model_visible`, async () => {
    const { m, journal } = freshMachine();
    const fenceValue = recoverFence(7);
    const ids = recoveredIds();
    const basis = replayableBasis(fenceValue, ids);
    const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
    const claim = new FakeCompositeClaim(watermark, 3);
    const ports = recoverPorts({
      claim,
      recovery: { kind: "replayable", basis, cursor: watermark },
      basis,
      suffix: legal.suffix,
    });
    const outcome = await m.recoverFreshProcess(ports);
    assert.equal(outcome.kind, "recovered");
    if (outcome.kind !== "recovered") return;
    // Still active at a steerable model_visible boundary; NOT staged-terminal.
    assert.equal(outcome.terminalStaged, false);
    assert.equal(m.activeTurnId, fenceValue.turnId);
    assert.equal(m.activePhase, "model_visible");
    assert.equal(m.activeBindingDigest, basis.bindingDigest);
    assert.equal(outcome.replyStaged, legal.reply);
    assert.equal(m.replyCount, legal.reply ? 1 : 0);
    // The source finalizer classified an "active" prefix; NOTHING entered storage.
    assert.equal(ports.source?.lastReplay?.outcome, "active");
    assert.equal(journal.log.length, 0);
    assert.equal(ports.source?.lastReplay?.closes, 1);
    assert.equal(claim.releases, 1);
    assert.equal(claim.aborts, 0);
  });
}

test("fresh-process recovery: a completed prefix classifies COMPLETED and stages the recovered terminal", async () => {
  const { m } = freshMachine();
  const fenceValue = recoverFence(7);
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    // reply -> turn_completed (no coordination): a legal COMPLETED prefix.
    suffix: (lease, b) => {
      const r0 = retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "assistant_reply",
        turnId: b.protocolTurnId,
        text: "done",
      });
      const r1 = retainedRecord(lease, b, lease.nextOrdinal + 1, r0.recordDigest, {
        kind: "turn_completed",
        turnId: b.protocolTurnId,
      });
      return [r0, r1];
    },
  });
  const outcome = await m.recoverFreshProcess(ports);
  assert.equal(outcome.kind, "recovered");
  if (outcome.kind !== "recovered") return;
  assert.equal(outcome.terminalStaged, true);
  assert.equal(m.activeTurnId, null);
  // The finalizer classified the completed cut; the terminal stage is exposable.
  assert.equal(ports.source?.lastReplay?.outcome, "completed");
  const stage = m.terminalDraft();
  assert.equal(stage.contribution.fence.turnId, fenceValue.turnId);
  assert.equal(stage.coordinationRequest.kind, "not_requested");
});

// B1 real-port parity: the FAITHFUL retained source rejects an illegal suffix (a
// coordination_call before any reply) at retained-prefix finalization, BEFORE it
// exposes any record — exactly as the promoted validateRetainedSnapshot does.
// The faithful fake has no bypass, so this IS the production path a fresh process
// actually sees; such a suffix can never reach the machine reducer through the
// promoted contract.
test("fresh-process recovery: faithful source rejects a coordination-before-reply suffix before first yield (real-port parity)", async () => {
  const { m, journal } = freshMachine();
  const fenceValue = recoverFence(7);
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const ports = recoverPorts({
    claim,
    recovery: { kind: "replayable", basis, cursor: watermark },
    basis,
    // A coordination_call with no preceding reply: structurally well-formed but an
    // illegal retained prefix. The faithful source finalizes at openReplay and
    // rejects it before exposing any record.
    suffix: (lease, b) => [
      retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "coordination_call",
        turnId: b.protocolTurnId,
        commandId: opaque("cmd", 32) as CommandId,
        command: { protocolVersion: version, title: "early", sourceMessageId: opaque("msg", 6) as never },
      }),
    ],
  });
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE",
  );
  // Rejected at the SOURCE (finalization threw inside openReplay before any replay
  // was returned); the composite claim is aborted; ZERO live machine mutation.
  assert.equal(ports.source?.opens, 1);
  assert.equal(ports.source?.lastReplay, undefined);
  assert.equal(claim.aborts, 1);
  assert.equal(claim.releases, 0);
  assert.equal(m.activeTurnId, null);
  assert.equal(m.activeGeneration, 0);
  assert.equal(journal.cursorNextOrdinal, 0);
  assert.equal(journal.log.length, 0);
  assert.equal((await m.interrupt(fenceValue.turnId)).kind, "uncorrelated");
});

// B2 defense-in-depth (NOT the faithful path): a SEPARATE, explicitly contract-
// violating adversarial double delivers the same reducer-invalid suffix WITHOUT
// source finalization, so the record reaches #reduceRetainedInto. This proves the
// machine's own reducer is an INDEPENDENT fail-closed authority even if a future
// source regression exposed such a suffix: the whole image is built OFF-SIDE, so
// the reducer throw leaves ZERO live mutation and the replay is CLOSED (claim
// released) via the guaranteed teardown. The faithful production source already
// rejects this suffix at the source (control above).
test("fresh-process recovery: adversarial unfinalized reducer-invalid suffix -> machine reducer fails closed, byte-unchanged, replay CLOSED + claim released", async () => {
  const { m, journal } = freshMachine();
  const fenceValue = recoverFence(7);
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const advSource = new AdversarialUnfinalizedSource(basis, (lease, b) => [
    retainedRecord(lease, b, lease.nextOrdinal, null, {
      kind: "coordination_call",
      turnId: b.protocolTurnId,
      commandId: opaque("cmd", 32) as CommandId,
      command: { protocolVersion: version, title: "early", sourceMessageId: opaque("msg", 6) as never },
    }),
  ]);
  const ports = {
    ...recoverPorts({ claim, recovery: { kind: "replayable", basis, cursor: watermark }, basis, suffix: () => [] }),
    retained: advSource,
  };
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) => e instanceof TurnError && e.code === "ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE",
  );
  // Byte-unchanged machine state (the off-side recovered image is discarded).
  assert.equal(m.activeTurnId, null);
  assert.equal(m.activePhase, null);
  assert.equal(m.activeBindingDigest, null);
  assert.equal(m.activeGeneration, 0);
  assert.equal(m.replyCount, 0);
  assert.equal(m.queuedCount, 0);
  assert.equal(journal.cursorNextOrdinal, 0);
  assert.equal(journal.log.length, 0);
  assert.equal((await m.interrupt(fenceValue.turnId)).kind, "uncorrelated");
  assert.throws(
    () => m.terminalDraft(),
    (e: unknown) => e instanceof TurnError && e.code === "INVOCATION_STATE_CONFLICT",
  );
  // The adversarial replay was opened then CLOSED via the guaranteed teardown; the
  // composite claim was released (not aborted).
  assert.equal(advSource.opens, 1);
  assert.equal(advSource.lastReplay?.closes, 1);
  assert.equal(claim.releases, 1);
  assert.equal(claim.aborts, 0);
});

// B2 dual-failure: a reducer failure AND a teardown (close) failure must BOTH
// surface — the guaranteed close must NEVER mask the primary reduction failure.
// The adversarial double raises the reducer error and also fails close(); the
// machine raises an AggregateError carrying both causes, and leaves ZERO live
// state.
test("fresh-process recovery: reducer failure + close failure surface BOTH causes (teardown never masks primary), byte-unchanged", async () => {
  const { m, journal } = freshMachine();
  const fenceValue = recoverFence(7);
  const ids = recoveredIds();
  const basis = replayableBasis(fenceValue, ids);
  const watermark = { nextOrdinal: 2, lastEventDigest: digest("a") };
  const claim = new FakeCompositeClaim(watermark, 3);
  const advSource = new AdversarialUnfinalizedSource(
    basis,
    (lease, b) => [
      retainedRecord(lease, b, lease.nextOrdinal, null, {
        kind: "coordination_call",
        turnId: b.protocolTurnId,
        commandId: opaque("cmd", 32) as CommandId,
        command: { protocolVersion: version, title: "early", sourceMessageId: opaque("msg", 6) as never },
      }),
    ],
    { closeThrows: true },
  );
  const ports = {
    ...recoverPorts({ claim, recovery: { kind: "replayable", basis, cursor: watermark }, basis, suffix: () => [] }),
    retained: advSource,
  };
  await assert.rejects(
    m.recoverFreshProcess(ports),
    (e: unknown) =>
      e instanceof AggregateError &&
      e.errors.length === 2 &&
      e.errors.some((x) => x instanceof TurnError && x.code === "ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE") &&
      e.errors.some((x) => x instanceof TurnError && x.detail === "adversarial_teardown_failure"),
  );
  // Both the reduce error and the close error are preserved; ZERO live mutation.
  assert.equal(m.activeTurnId, null);
  assert.equal(m.activeGeneration, 0);
  assert.equal(journal.cursorNextOrdinal, 0);
  assert.equal(journal.log.length, 0);
  assert.equal(advSource.opens, 1);
  assert.equal(advSource.lastReplay?.closes, 1);
  assert.equal(claim.releases, 1);
});
