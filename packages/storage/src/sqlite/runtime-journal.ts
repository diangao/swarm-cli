import { createHash } from "node:crypto";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";

import {
  buildContributionBinding,
  canonicalProtocolJson,
  verifyTurnCompletionEvidence,
  type DeliveryFence,
  type InvocationJournalEntry,
  type TurnCompletionEvidence,
} from "@swarm/protocol";
import type {
  AgentId,
  ArtifactDigest,
  CommandId,
  DeliveryId,
  LaunchId,
  MachineId,
  MessageId,
  ProducerFactId,
  ReceiptId,
  RuntimeKind,
  SessionId,
  StateInstanceId,
  Target,
  TurnId,
} from "@swarm/protocol";

import { storageFail } from "../errors.js";
import {
  assertArtifactDigest,
  assertProtocolId,
  canonicalTargetKey,
  targetColumns,
} from "../protocol.js";

function run(
  database: DatabaseSync,
  sql: string,
  values: readonly SQLInputValue[] = [],
): { changes: number | bigint; lastInsertRowid: number | bigint } {
  return database.prepare(sql).run(...values);
}

function one<T extends Record<string, unknown>>(
  database: DatabaseSync,
  sql: string,
  values: readonly SQLInputValue[] = [],
): T | undefined {
  return database.prepare(sql).get(...values) as T | undefined;
}

function all<T extends Record<string, unknown>>(
  database: DatabaseSync,
  sql: string,
  values: readonly SQLInputValue[] = [],
): T[] {
  return database.prepare(sql).all(...values) as T[];
}

function equalNullable(left: string | null, right: string | null): boolean {
  return left === right;
}

function digestCanonical(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

const DELIVERY_FENCE_KEYS = [
  "protocolVersion",
  "deliveryId",
  "attempt",
  "producerFactId",
  "agentId",
  "machineId",
  "launchId",
  "membershipEpoch",
  "routingGeneration",
  "routeVersion",
  "sessionId",
  "turnId",
] as const satisfies readonly (keyof DeliveryFence)[];

export type ReserveLaunchInput = {
  agentId: AgentId;
  machineId: MachineId;
  launchId: LaunchId;
  runtime: RuntimeKind | "scripted_fake";
  routingGeneration: number;
  workspaceGeneration: number;
  stopEpoch: number;
  queueOrdinal: number;
  driverIdentityDigest: ArtifactDigest;
  queuedAt: string;
};

export type ObservedModelVisibleAck = {
  observed: true;
  receiptId: ReceiptId;
};

export type CommitVisibleMessageInput = {
  sessionId: SessionId;
  target: Target;
  messageId: MessageId;
  deliveryId: DeliveryId;
  attempt: number;
  serverSeq: number;
  modelVisibleAck: ObservedModelVisibleAck;
  visibleAt: string;
};

export type CommitNoticeVisibilityInput = {
  sessionId: SessionId;
  target: Target;
  membershipEpoch: number;
  firstMessageId: MessageId;
  latestMessageId: MessageId;
  firstServerSeq: number;
  latestServerSeq: number;
  inputDeliveryId: DeliveryId;
  inputAttempt: number;
  committedAt: string;
};

export type CompareNoticeVisibilityInput = {
  sessionId: SessionId;
  target: Target;
  membershipEpoch: number;
  firstMessageId: MessageId;
  latestMessageId: MessageId;
  firstServerSeq: number;
  latestServerSeq: number;
  inputDeliveryId: DeliveryId;
  inputAttempt: number;
};

export type PrepareTurnInput = {
  protocolTurnId: TurnId;
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  rootProducerFactId: ProducerFactId;
  inputOrdinal: number;
  driverTurnRefDigest: ArtifactDigest;
  mode: "ordinary" | "steer";
  expectedTurnId?: TurnId;
  queuedAt: string;
};

export type LocalTurnState =
  | "queued"
  | "write_started"
  | "input_written"
  | "model_visible"
  | "completed"
  | "ambiguous"
  | "interrupted"
  | "terminal_error";

export type AdvanceTurnInput = {
  protocolTurnId: TurnId;
  inputOrdinal: number;
  expectedState: LocalTurnState;
  nextState: LocalTurnState;
  updatedAt: string;
};

export type BindNativeAttemptInput = {
  fence: DeliveryFence;
  stateInstanceId: StateInstanceId;
};

export type AppendNativeInvocationEntryInput = {
  entry: InvocationJournalEntry<
    "permit_recorded" | "write_started" | "input_written" | "model_visible"
  >;
};

export type DriverEventReaderClaim = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ownerToken: ArtifactDigest;
  mode: "start" | "resume" | "subscribe";
  claimedAt: string;
};

export type CommitDriverEventInput = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
  ordinal: number;
  eventDigest: ArtifactDigest;
  turnId?: TurnId;
  bindingDigest?: ArtifactDigest;
  recordedAt: string;
};

export type NativeAttemptState =
  | "accepted"
  | "pre_permit_disconnect"
  | "permit_recorded"
  | "write_started"
  | "not_written"
  | "input_written"
  | "model_visible"
  | "ambiguous"
  | "suppressed"
  | "consumed";

export type TransitionNativeAttemptInput = {
  fence: DeliveryFence;
  stateInstanceId: StateInstanceId;
  expectedState: NativeAttemptState;
  nextState: NativeAttemptState;
  permitId?: CommandId;
  invocationGeneration?: number;
  invocationId?: CommandId;
  bodyDigest?: ArtifactDigest;
  notWrittenProofJson?: string;
  expectedPreviousProofDigest?: ArtifactDigest;
  disconnectId?: CommandId;
  suppressionReason?: string;
};

export type ReadVisibleMessageInput = {
  sessionId: SessionId;
  target: Target;
  messageId: MessageId;
};

export type RecordAttemptCompletionInput = {
  fence: DeliveryFence;
  stateInstanceId: StateInstanceId;
  evidence: TurnCompletionEvidence;
  recordedAt: string;
};

export type TurnStepKind =
  | "turn_started"
  | "input_written"
  | "model_visible"
  | "turn_boundary";

export type TurnSettleKind = "terminal_error" | "interrupted" | "ambiguous";

export type DurableTurnState = {
  protocolTurnId: TurnId;
  phase: LocalTurnState;
  inputOrdinal: number;
  bindingDigest: ArtifactDigest | null;
  steerable: boolean;
  replyCommitted: boolean;
};

export type TurnEventRecordInput = {
  ordinal: number;
  eventDigest: ArtifactDigest;
  turnId: TurnId;
  bindingDigest: ArtifactDigest;
};

export type TurnTerminalCommitBasis = {
  reply: TurnEventRecordInput & { replyCommandId: CommandId };
  coordination?: TurnEventRecordInput & {
    commandId: CommandId;
    commandDigest: ArtifactDigest;
  };
  completed: TurnEventRecordInput;
};

export type TurnAdmissionMode =
  | { kind: "ordinary" }
  | { kind: "steer"; expectedTurnId: TurnId };

export type TurnReaderFence = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
};

export type BeginTurnContributionInput = TurnReaderFence & {
  protocolTurnId: TurnId;
  launchId: LaunchId;
  rootProducerFactId: ProducerFactId;
  inputOrdinal: number;
  driverTurnRefDigest: ArtifactDigest;
  mode: TurnAdmissionMode;
  bindingDigest: ArtifactDigest;
  expected: DurableTurnState | null;
  next: DurableTurnState;
  recordedAt: string;
};

export type CommitTurnStepInput = TurnReaderFence & {
  event: TurnEventRecordInput;
  kind: TurnStepKind;
  expected: DurableTurnState;
  next: DurableTurnState;
  recordedAt: string;
};

export type CommitTurnTerminalInput = TurnReaderFence & {
  basis: TurnTerminalCommitBasis;
  evidence: TurnCompletionEvidence;
  expected: DurableTurnState;
  next: DurableTurnState;
  recordedAt: string;
};

export type SettleTurnContributionInput = TurnReaderFence & {
  protocolTurnId: TurnId;
  inputOrdinal: number;
  kind: TurnSettleKind;
  expected: DurableTurnState;
  next: DurableTurnState;
  recordedAt: string;
};

export type TurnMutationResult = { applied: boolean; durable: DurableTurnState };
export type TurnStepResult = TurnMutationResult & { nextOrdinal: number };

type TurnRowShape = {
  protocol_turn_id: string;
  launch_id: string;
  state_instance_id: string;
  session_id: string;
  input_ordinal: number;
  state: string;
  binding_digest: string | null;
  steerable: number;
  operation_digest: string | null;
};

export type VisibleMessageRow = {
  deliveryId: DeliveryId;
  attempt: number;
  serverSeq: number;
  modelVisibleReceiptId: ReceiptId;
  visibleAt: string;
};

export type NativeAttemptRow = {
  deliveryId: DeliveryId;
  attempt: number;
  state: NativeAttemptState;
  permitId: CommandId | null;
  invocationGeneration: number | null;
  invocationId: CommandId | null;
  bodyDigest: ArtifactDigest | null;
  previousInvocationGeneration: number | null;
  previousProofDigest: ArtifactDigest | null;
  disconnectId: CommandId | null;
  suppressionReason: string | null;
};


export class RuntimeJournalTransaction {
  readonly #database: DatabaseSync;

  constructor(database: DatabaseSync) {
    this.#database = database;
  }

  reserveLaunch(input: ReserveLaunchInput): { applied: boolean } {
    assertProtocolId(input.agentId, "agt");
    assertProtocolId(input.machineId, "mch");
    assertProtocolId(input.launchId, "lnc");
    assertArtifactDigest(input.driverIdentityDigest);
    if (!Number.isSafeInteger(input.queueOrdinal) || input.queueOrdinal < 1) {
      storageFail("INVALID_STATE_TRANSITION", input.queueOrdinal);
    }
    const existing = one<{
      agent_id: string;
      machine_id: string;
      runtime_kind: string;
      routing_generation: number;
      workspace_generation: number;
      stop_epoch: number;
      queue_ordinal: number;
      state: string;
      driver_identity_digest: string;
      queued_at: string;
    }>(
      this.#database,
      `SELECT agent_id, machine_id, runtime_kind, routing_generation,
              workspace_generation, stop_epoch, queue_ordinal, state,
              driver_identity_digest, queued_at
       FROM local_launches WHERE launch_id = ?`,
      [input.launchId],
    );
    if (existing !== undefined) {
      if (
        existing.agent_id === input.agentId &&
        existing.machine_id === input.machineId &&
        existing.runtime_kind === input.runtime &&
        Number(existing.routing_generation) === input.routingGeneration &&
        Number(existing.workspace_generation) === input.workspaceGeneration &&
        Number(existing.stop_epoch) === input.stopEpoch &&
        Number(existing.queue_ordinal) === input.queueOrdinal &&
        existing.state === "queued" &&
        existing.driver_identity_digest === input.driverIdentityDigest &&
        existing.queued_at === input.queuedAt
      ) {
        return { applied: false };
      }
      storageFail("STALE_LAUNCH_FENCE", input.launchId);
    }

    run(
      this.#database,
      `INSERT INTO local_agent_slots (
         agent_id, machine_id, state, stop_epoch, current_launch_id,
         queue_ordinal, updated_at
       ) VALUES (?, ?, 'queued', ?, ?, ?, ?)
       ON CONFLICT(agent_id) DO NOTHING`,
      [
        input.agentId,
        input.machineId,
        input.stopEpoch,
        input.launchId,
        input.queueOrdinal,
        input.queuedAt,
      ],
    );
    const slot = one<{
      machine_id: string;
      state: string;
      stop_epoch: number;
      current_launch_id: string | null;
      queue_ordinal: number | null;
    }>(
      this.#database,
      `SELECT machine_id, state, stop_epoch, current_launch_id, queue_ordinal
       FROM local_agent_slots WHERE agent_id = ?`,
      [input.agentId],
    );
    if (
      slot === undefined ||
      slot.machine_id !== input.machineId ||
      Number(slot.stop_epoch) !== input.stopEpoch ||
      slot.state !== "queued" ||
      slot.current_launch_id !== input.launchId ||
      Number(slot.queue_ordinal) !== input.queueOrdinal
    ) {
      storageFail("START_SLOT_CONFLICT", input.agentId);
    }
    run(
      this.#database,
      `INSERT INTO local_launches (
         launch_id, agent_id, machine_id, runtime_kind, routing_generation,
         workspace_generation, stop_epoch, queue_ordinal, state,
         driver_identity_digest, queued_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`,
      [
        input.launchId,
        input.agentId,
        input.machineId,
        input.runtime,
        input.routingGeneration,
        input.workspaceGeneration,
        input.stopEpoch,
        input.queueOrdinal,
        input.driverIdentityDigest,
        input.queuedAt,
      ],
    );
    return { applied: true };
  }

  bindNativeAttempt(input: BindNativeAttemptInput): { applied: boolean } {
    assertProtocolId(input.fence.deliveryId, "dlv");
    assertProtocolId(input.fence.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.fence.sessionId, "ses");
    const fenceJson = new TextDecoder().decode(canonicalProtocolJson(input.fence));
    const delivery = one<{
      attempt: number;
      launch_id: string;
      state_instance_id: string;
      session_id: string;
    }>(
      this.#database,
      `SELECT attempt, launch_id, state_instance_id, session_id
       FROM pending_deliveries WHERE delivery_id = ?`,
      [input.fence.deliveryId],
    );
    if (
      delivery === undefined ||
      Number(delivery.attempt) !== input.fence.attempt ||
      delivery.launch_id !== input.fence.launchId ||
      delivery.state_instance_id !== input.stateInstanceId ||
      delivery.session_id !== input.fence.sessionId
    ) {
      storageFail("STALE_DELIVERY_FENCE", input.fence.deliveryId);
    }
    const existing = one<{
      fence_json: string;
      launch_id: string | null;
      state_instance_id: string | null;
      session_id: string | null;
    }>(
      this.#database,
      `SELECT fence_json, launch_id, state_instance_id, session_id
       FROM native_attempts WHERE delivery_id = ? AND attempt = ?`,
      [input.fence.deliveryId, input.fence.attempt],
    );
    if (existing !== undefined) {
      if (
        existing.fence_json === fenceJson &&
        existing.launch_id === input.fence.launchId &&
        existing.state_instance_id === input.stateInstanceId &&
        existing.session_id === input.fence.sessionId
      ) {
        return { applied: false };
      }
      storageFail("STALE_DELIVERY_FENCE", input.fence.deliveryId);
    }
    run(
      this.#database,
      `INSERT INTO native_attempts (
         delivery_id, attempt, fence_json, state, launch_id,
         state_instance_id, session_id
       ) VALUES (?, ?, ?, 'accepted', ?, ?, ?)`,
      [
        input.fence.deliveryId,
        input.fence.attempt,
        fenceJson,
        input.fence.launchId,
        input.stateInstanceId,
        input.fence.sessionId,
      ],
    );
    return { applied: true };
  }

  appendNativeInvocationEntry(
    input: AppendNativeInvocationEntryInput,
  ): { applied: boolean } {
    const entry = input.entry;
    assertProtocolId(entry.deliveryId, "dlv");
    assertProtocolId(entry.invocationId, "cmd");
    assertArtifactDigest(entry.entryDigest);
    if (!Number.isSafeInteger(entry.sequence) || entry.sequence < 1) {
      storageFail("INVALID_JOURNAL_CHAIN", entry.sequence);
    }
    const unsignedEntry = Object.fromEntries(
      Object.entries(entry).filter(([key]) => key !== "entryDigest"),
    );
    if (digestCanonical(unsignedEntry) !== entry.entryDigest) {
      storageFail("INVALID_JOURNAL_CHAIN", entry.entryId);
    }
    const entryJson = new TextDecoder().decode(canonicalProtocolJson(entry));
    const existing = one<{ entry_json: string }>(
      this.#database,
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = ? AND invocation_generation = ? AND kind = ?`,
      [entry.deliveryId, entry.attempt, entry.invocationGeneration, entry.kind],
    );
    if (existing !== undefined) {
      if (existing.entry_json === entryJson) return { applied: false };
      storageFail("WRITE_STARTED_BINDING_MISMATCH", entry.entryId);
    }
    const previous = one<{ sequence: number; entry_json: string }>(
      this.#database,
      `SELECT sequence, entry_json FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = ?
       ORDER BY sequence DESC LIMIT 1`,
      [entry.deliveryId, entry.attempt],
    );
    const previousEntry = previous === undefined
      ? undefined
      : JSON.parse(previous.entry_json) as InvocationJournalEntry<string>;
    if (
      entry.sequence !== Number(previous?.sequence ?? 0) + 1 ||
      entry.previousEntryDigest !== (previousEntry?.entryDigest ?? null)
    ) {
      storageFail("INVALID_JOURNAL_CHAIN", entry.entryId);
    }
    run(
      this.#database,
      `INSERT INTO native_invocation_entries (
         delivery_id, attempt, invocation_generation, sequence, kind, entry_json
       ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        entry.deliveryId,
        entry.attempt,
        entry.invocationGeneration,
        entry.sequence,
        entry.kind,
        entryJson,
      ],
    );
    return { applied: true };
  }

  transitionNativeAttempt(
    input: TransitionNativeAttemptInput,
  ): { applied: boolean } {
    assertProtocolId(input.fence.deliveryId, "dlv");
    assertProtocolId(input.fence.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.fence.sessionId, "ses");
    const transition = `${input.expectedState}:${input.nextState}`;
    const allowed = new Set([
      "accepted:pre_permit_disconnect",
      "accepted:permit_recorded",
      "accepted:suppressed",
      "permit_recorded:suppressed",
      "permit_recorded:write_started",
      "write_started:input_written",
      "write_started:not_written",
      "write_started:ambiguous",
      "not_written:write_started",
      "input_written:model_visible",
      "model_visible:consumed",
      "ambiguous:suppressed",
    ]);
    if (!allowed.has(transition)) {
      storageFail("INVALID_STATE_TRANSITION", transition);
    }
    const row = one<{
      state: string;
      fence_json: string;
      launch_id: string | null;
      state_instance_id: string | null;
      session_id: string | null;
      permit_id: string | null;
      invocation_generation: number | null;
      invocation_id: string | null;
      body_digest: string | null;
      previous_invocation_generation: number | null;
      previous_proof_digest: string | null;
      proof_json: string | null;
      disconnect_id: string | null;
      suppression_reason: string | null;
    }>(
      this.#database,
      `SELECT state, fence_json, launch_id, state_instance_id, session_id,
              permit_id, invocation_generation, invocation_id, body_digest,
              previous_invocation_generation, previous_proof_digest, proof_json,
              disconnect_id, suppression_reason
       FROM native_attempts WHERE delivery_id = ? AND attempt = ?`,
      [input.fence.deliveryId, input.fence.attempt],
    );
    if (row === undefined) {
      storageFail("STALE_DELIVERY_FENCE", input.fence.deliveryId);
    }
    const fenceJson = new TextDecoder().decode(canonicalProtocolJson(input.fence));
    if (
      row.fence_json !== fenceJson ||
      row.launch_id !== input.fence.launchId ||
      row.state_instance_id !== input.stateInstanceId ||
      row.session_id !== input.fence.sessionId
    ) {
      storageFail("STALE_DELIVERY_FENCE", input.fence.deliveryId);
    }

    const assignments: string[] = [];
    const values: SQLInputValue[] = [];
    const loadEntry = (
      kind: string,
      generation: number,
    ): Record<string, unknown> => {
      const entry = one<{ entry_json: string }>(
        this.#database,
        `SELECT entry_json FROM native_invocation_entries
         WHERE delivery_id = ? AND attempt = ? AND invocation_generation = ?
           AND kind = ? LIMIT 1`,
        [input.fence.deliveryId, input.fence.attempt, generation, kind],
      );
      if (entry === undefined) {
        storageFail("INVALID_JOURNAL_CHAIN", {
          deliveryId: input.fence.deliveryId,
          attempt: input.fence.attempt,
          kind,
          generation,
        });
      }
      return JSON.parse(entry.entry_json) as Record<string, unknown>;
    };
    const requireEntryFence = (entry: Record<string, unknown>): void => {
      const entryFence: Record<string, unknown> = {};
      for (const key of DELIVERY_FENCE_KEYS) {
        if (entry[key] === undefined) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", {
            deliveryId: input.fence.deliveryId,
            attempt: input.fence.attempt,
            missingFenceField: key,
          });
        }
        entryFence[key] = entry[key];
      }
      const entryFenceJson = new TextDecoder().decode(
        canonicalProtocolJson(entryFence),
      );
      if (entryFenceJson !== row.fence_json) {
        storageFail("WRITE_STARTED_BINDING_MISMATCH", {
          deliveryId: input.fence.deliveryId,
          attempt: input.fence.attempt,
        });
      }
    };

    switch (input.nextState) {
      case "pre_permit_disconnect":
      case "ambiguous": {
        if (input.disconnectId === undefined) {
          storageFail("INVALID_STATE_TRANSITION", transition);
        }
        assertProtocolId(input.disconnectId, "cmd");
        assignments.push("disconnect_id = ?");
        values.push(input.disconnectId);
        break;
      }
      case "permit_recorded": {
        if (input.permitId === undefined) {
          storageFail("INVALID_STATE_TRANSITION", transition);
        }
        assertProtocolId(input.permitId, "cmd");
        const entry = loadEntry("permit_recorded", input.invocationGeneration ?? 1);
        requireEntryFence(entry);
        if (entry.permitId !== input.permitId) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", input.permitId);
        }
        assignments.push("permit_id = ?");
        values.push(input.permitId);
        break;
      }
      case "write_started": {
        if (
          input.invocationGeneration === undefined ||
          !Number.isSafeInteger(input.invocationGeneration) ||
          input.invocationGeneration < 1 ||
          input.invocationId === undefined ||
          input.bodyDigest === undefined
        ) {
          storageFail("INVALID_STATE_TRANSITION", transition);
        }
        assertProtocolId(input.invocationId, "cmd");
        assertArtifactDigest(input.bodyDigest);
        if (input.expectedState === "permit_recorded") {
          if (input.invocationGeneration !== 1) {
            storageFail("STALE_INVOCATION_GENERATION", input.invocationGeneration);
          }
        } else {
          const storedGeneration = Number(row.invocation_generation ?? 0);
          if (input.invocationGeneration !== storedGeneration + 1) {
            storageFail("STALE_INVOCATION_GENERATION", input.invocationGeneration);
          }
          if (
            input.expectedPreviousProofDigest === undefined ||
            row.previous_proof_digest === null ||
            input.expectedPreviousProofDigest !== row.previous_proof_digest
          ) {
            storageFail("STALE_INVOCATION_GENERATION", {
              deliveryId: input.fence.deliveryId,
              attempt: input.fence.attempt,
            });
          }
        }
        const entry = loadEntry("write_started", input.invocationGeneration);
        requireEntryFence(entry);
        if (
          entry.invocationId !== input.invocationId ||
          Number(entry.invocationGeneration) !== input.invocationGeneration ||
          entry.inputDigest !== input.bodyDigest
        ) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", {
            deliveryId: input.fence.deliveryId,
            attempt: input.fence.attempt,
            invocationGeneration: input.invocationGeneration,
          });
        }
        assignments.push(
          "invocation_generation = ?",
          "invocation_id = ?",
          "body_digest = ?",
        );
        values.push(input.invocationGeneration, input.invocationId, input.bodyDigest);
        break;
      }
      case "not_written": {
        if (input.notWrittenProofJson === undefined) {
          storageFail("FAKE_NOT_WRITTEN_PROOF_REQUIRED", transition);
        }
        const generation = Number(row.invocation_generation ?? 0);
        if (generation < 1) {
          storageFail("STALE_INVOCATION_GENERATION", generation);
        }
        let parsedProof: Record<string, unknown>;
        try {
          parsedProof = JSON.parse(input.notWrittenProofJson) as Record<string, unknown>;
        } catch {
          storageFail("FAKE_NOT_WRITTEN_PROOF_REQUIRED", transition);
        }
        const proofKeys = [
          "driverKind", "fixtureId", "scriptDigest", "invocationId",
          "invocationGeneration", "writeStartedEntryId", "writeStartedEntryDigest",
          "outcomeOrdinal", "outcome", "proofDigest",
        ];
        const presentKeys = Object.keys(parsedProof).sort();
        if (
          presentKeys.length !== proofKeys.length ||
          presentKeys.join(",") !== [...proofKeys].sort().join(",")
        ) {
          storageFail("FAKE_NOT_WRITTEN_PROOF_REQUIRED", presentKeys);
        }
        if (parsedProof.driverKind === "native_process") {
          storageFail("REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN", input.fence.deliveryId);
        }
        if (parsedProof.driverKind !== "scripted_fake") {
          storageFail("FAKE_NOT_WRITTEN_PROOF_REQUIRED", parsedProof.driverKind);
        }
        const launchRuntime = one<{ runtime_kind: string }>(
          this.#database,
          `SELECT runtime_kind FROM local_launches WHERE launch_id = ?`,
          [input.fence.launchId],
        );
        if (launchRuntime === undefined || launchRuntime.runtime_kind !== "scripted_fake") {
          storageFail("REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN", input.fence.launchId);
        }
        assertProtocolId(String(parsedProof.fixtureId), "cmd");
        assertProtocolId(String(parsedProof.invocationId), "cmd");
        assertProtocolId(String(parsedProof.writeStartedEntryId), "cmd");
        assertArtifactDigest(String(parsedProof.scriptDigest) as ArtifactDigest);
        assertArtifactDigest(String(parsedProof.writeStartedEntryDigest) as ArtifactDigest);
        assertArtifactDigest(String(parsedProof.proofDigest) as ArtifactDigest);
        if (
          parsedProof.outcome !== "not_written" ||
          !Number.isSafeInteger(parsedProof.outcomeOrdinal) ||
          Number(parsedProof.outcomeOrdinal) < 1
        ) {
          storageFail("FAKE_NOT_WRITTEN_PROOF_REQUIRED", parsedProof.outcome);
        }
        if (
          parsedProof.invocationId !== row.invocation_id ||
          Number(parsedProof.invocationGeneration) !== generation
        ) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", {
            deliveryId: input.fence.deliveryId,
            invocationGeneration: parsedProof.invocationGeneration,
          });
        }
        const startedEntry = loadEntry("write_started", generation);
        requireEntryFence(startedEntry);
        if (
          parsedProof.writeStartedEntryId !== startedEntry.entryId ||
          parsedProof.writeStartedEntryDigest !== startedEntry.entryDigest
        ) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", {
            deliveryId: input.fence.deliveryId,
            writeStartedEntryId: parsedProof.writeStartedEntryId,
          });
        }
        const unsignedProof = Object.fromEntries(
          Object.entries(parsedProof).filter(([key]) => key !== "proofDigest"),
        );
        if (digestCanonical(unsignedProof) !== parsedProof.proofDigest) {
          storageFail("INVALID_JOURNAL_CHAIN", parsedProof.proofDigest);
        }
        assignments.push(
          "proof_json = ?",
          "previous_invocation_generation = ?",
          "previous_proof_digest = ?",
        );
        values.push(
          input.notWrittenProofJson,
          generation,
          String(parsedProof.proofDigest),
        );
        break;
      }
      case "input_written":
      case "model_visible": {
        const generation = Number(row.invocation_generation ?? 0);
        if (generation < 1) {
          storageFail("STALE_INVOCATION_GENERATION", generation);
        }
        const entry = loadEntry(input.nextState, generation);
        requireEntryFence(entry);
        if (
          Number(entry.invocationGeneration) !== generation ||
          entry.invocationId !== row.invocation_id ||
          typeof entry.runtimeWriteId !== "string"
        ) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", {
            deliveryId: input.fence.deliveryId,
            kind: input.nextState,
          });
        }
        if (input.nextState === "model_visible") {
          const writtenEntry = loadEntry("input_written", generation);
          if (
            entry.runtimeWriteId !== writtenEntry.runtimeWriteId ||
            typeof entry.visibilityEventId !== "string"
          ) {
            storageFail("WRITE_STARTED_BINDING_MISMATCH", {
              deliveryId: input.fence.deliveryId,
              kind: "model_visible",
            });
          }
        }
        break;
      }
      case "consumed": {
        const ledger = one<{ found: number }>(
          this.#database,
          `SELECT 1 AS found FROM visible_message_ids
           WHERE delivery_id = ? AND attempt = ? LIMIT 1`,
          [input.fence.deliveryId, input.fence.attempt],
        );
        if (ledger === undefined) {
          storageFail(
            "MODEL_VISIBLE_PREDECESSOR_REQUIRED",
            input.fence.deliveryId,
          );
        }
        const completion = one<{ contribution_binding_digest: string }>(
          this.#database,
          `SELECT contribution_binding_digest FROM native_attempt_completions
           WHERE delivery_id = ? AND attempt = ?`,
          [input.fence.deliveryId, input.fence.attempt],
        );
        if (completion === undefined) {
          storageFail("ACK_PREDECESSOR_REQUIRED", input.fence.deliveryId);
        }
        // Rebuild the contribution binding purely from stored journal truth
        // (fence bytes, attempt binding, entry chain, terminal turn record) via
        // the protocol SSOT builder and require the stored completion to join
        // on the recomputed digest — never on the caller's or row's say-so.
        const generation = row.invocation_generation ?? 1;
        const writtenEntry = loadEntry("input_written", generation);
        const visibleEntry = loadEntry("model_visible", generation);
        requireEntryFence(writtenEntry);
        requireEntryFence(visibleEntry);
        const turn = one<{
          launch_id: string;
          session_id: string;
          input_ordinal: number;
          state: string;
        }>(
          this.#database,
          `SELECT launch_id, session_id, input_ordinal, state
           FROM local_turns WHERE protocol_turn_id = ?`,
          [input.fence.turnId],
        );
        if (
          turn === undefined ||
          turn.state !== "completed" ||
          turn.launch_id !== input.fence.launchId ||
          turn.session_id !== input.fence.sessionId
        ) {
          storageFail("INVALID_JOURNAL_CHAIN", {
            deliveryId: input.fence.deliveryId,
            attempt: input.fence.attempt,
            turnId: input.fence.turnId,
          });
        }
        let expectedDigest: string;
        try {
          expectedDigest = buildContributionBinding({
            fence: JSON.parse(row.fence_json) as DeliveryFence,
            stateInstanceId: input.stateInstanceId,
            inputOrdinal: Number(turn.input_ordinal),
            invocationId: (row.invocation_id ?? "") as CommandId,
            invocationGeneration: generation,
            permitId: (row.permit_id ?? "") as CommandId,
            runtimeWriteId: visibleEntry.runtimeWriteId as CommandId,
            visibilityEventId: visibleEntry.visibilityEventId as CommandId,
          }).contributionBindingDigest;
        } catch {
          storageFail("INVALID_JOURNAL_CHAIN", {
            deliveryId: input.fence.deliveryId,
            attempt: input.fence.attempt,
          });
        }
        if (completion.contribution_binding_digest !== expectedDigest) {
          storageFail("WRITE_STARTED_BINDING_MISMATCH", {
            deliveryId: input.fence.deliveryId,
            attempt: input.fence.attempt,
          });
        }
        break;
      }
      case "suppressed": {
        if (
          input.suppressionReason === undefined ||
          input.suppressionReason.trim().length === 0
        ) {
          storageFail("INVALID_STATE_TRANSITION", transition);
        }
        assignments.push("suppression_reason = ?");
        values.push(input.suppressionReason);
        break;
      }
      default:
        storageFail("INVALID_STATE_TRANSITION", transition);
    }

    const setClause = ["state = ?", ...assignments].join(", ");
    const result = run(
      this.#database,
      `UPDATE native_attempts SET ${setClause}
       WHERE delivery_id = ? AND attempt = ? AND state = ?`,
      [
        input.nextState,
        ...values,
        input.fence.deliveryId,
        input.fence.attempt,
        input.expectedState,
      ],
    );
    if (Number(result.changes) === 1) return { applied: true };

    const current = one<{
      state: string;
      permit_id: string | null;
      invocation_generation: number | null;
      invocation_id: string | null;
      body_digest: string | null;
      proof_json: string | null;
      disconnect_id: string | null;
      suppression_reason: string | null;
    }>(
      this.#database,
      `SELECT state, permit_id, invocation_generation, invocation_id,
              body_digest, proof_json, disconnect_id, suppression_reason
       FROM native_attempts WHERE delivery_id = ? AND attempt = ?`,
      [input.fence.deliveryId, input.fence.attempt],
    );
    if (
      current !== undefined &&
      current.state === input.nextState &&
      equalNullable(current.permit_id, input.permitId ?? current.permit_id) &&
      Number(current.invocation_generation ?? 0) ===
        Number(input.invocationGeneration ?? current.invocation_generation ?? 0) &&
      equalNullable(
        current.invocation_id,
        input.invocationId ?? current.invocation_id,
      ) &&
      equalNullable(current.body_digest, input.bodyDigest ?? current.body_digest) &&
      equalNullable(
        current.proof_json,
        input.notWrittenProofJson ?? current.proof_json,
      ) &&
      equalNullable(
        current.disconnect_id,
        input.disconnectId ?? current.disconnect_id,
      ) &&
      equalNullable(
        current.suppression_reason,
        input.suppressionReason ?? current.suppression_reason,
      )
    ) {
      return { applied: false };
    }
    storageFail("INVOCATION_STATE_CONFLICT", {
      deliveryId: input.fence.deliveryId,
      attempt: input.fence.attempt,
      expectedState: input.expectedState,
    });
  }

  recordAttemptCompletion(
    input: RecordAttemptCompletionInput,
  ): { applied: boolean } {
    assertProtocolId(input.fence.deliveryId, "dlv");
    assertProtocolId(input.fence.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.fence.sessionId, "ses");
    const row = one<{
      state: string;
      fence_json: string;
      launch_id: string | null;
      state_instance_id: string | null;
      session_id: string | null;
      permit_id: string | null;
      invocation_id: string | null;
      invocation_generation: number | null;
    }>(
      this.#database,
      `SELECT state, fence_json, launch_id, state_instance_id, session_id,
              permit_id, invocation_id, invocation_generation
       FROM native_attempts WHERE delivery_id = ? AND attempt = ?`,
      [input.fence.deliveryId, input.fence.attempt],
    );
    if (
      row === undefined ||
      row.fence_json !==
        new TextDecoder().decode(canonicalProtocolJson(input.fence)) ||
      row.launch_id !== input.fence.launchId ||
      row.state_instance_id !== input.stateInstanceId ||
      row.session_id !== input.fence.sessionId
    ) {
      storageFail("STALE_DELIVERY_FENCE", input.fence.deliveryId);
    }
    if (row.state !== "model_visible") {
      storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.fence.deliveryId);
    }
    const ledger = one<{ found: number }>(
      this.#database,
      `SELECT 1 AS found FROM visible_message_ids
       WHERE delivery_id = ? AND attempt = ? LIMIT 1`,
      [input.fence.deliveryId, input.fence.attempt],
    );
    if (ledger === undefined) {
      storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.fence.deliveryId);
    }

    let evidence: TurnCompletionEvidence;
    try {
      evidence = verifyTurnCompletionEvidence(input.evidence, {
        expectedFence: input.fence,
      });
    } catch {
      storageFail("INVALID_JOURNAL_CHAIN", {
        deliveryId: input.fence.deliveryId,
        attempt: input.fence.attempt,
      });
    }
    this.#requireContributionJoin(input.fence, row, evidence);

    const { reply, coordination, contribution } = evidence;
    const coordinationColumns =
      coordination.kind === "not_requested"
        ? {
            terminalTurnId: coordination.terminalTurnId,
            commandId: null,
            receiptId: null,
            resultDigest: null,
          }
        : {
            terminalTurnId: null,
            commandId: coordination.commandId,
            receiptId: coordination.receiptId,
            resultDigest: coordination.resultDigest,
          };
    const existing = one<{
      reply_receipt_id: string;
      reply_result_digest: string;
      coordination_kind: string;
      coordination_terminal_turn_id: string | null;
      coordination_command_id: string | null;
      coordination_receipt_id: string | null;
      coordination_result_digest: string | null;
      contribution_binding_digest: string;
      recorded_at: string;
    }>(
      this.#database,
      `SELECT reply_receipt_id, reply_result_digest, coordination_kind,
              coordination_terminal_turn_id, coordination_command_id,
              coordination_receipt_id, coordination_result_digest,
              contribution_binding_digest, recorded_at
       FROM native_attempt_completions WHERE delivery_id = ? AND attempt = ?`,
      [input.fence.deliveryId, input.fence.attempt],
    );
    if (existing !== undefined) {
      if (
        existing.reply_receipt_id === reply.receiptId &&
        existing.reply_result_digest === reply.resultDigest &&
        existing.coordination_kind === coordination.kind &&
        equalNullable(
          existing.coordination_terminal_turn_id,
          coordinationColumns.terminalTurnId,
        ) &&
        equalNullable(
          existing.coordination_command_id,
          coordinationColumns.commandId,
        ) &&
        equalNullable(
          existing.coordination_receipt_id,
          coordinationColumns.receiptId,
        ) &&
        equalNullable(
          existing.coordination_result_digest,
          coordinationColumns.resultDigest,
        ) &&
        existing.contribution_binding_digest ===
          contribution.contributionBindingDigest &&
        existing.recorded_at === input.recordedAt
      ) {
        return { applied: false };
      }
      storageFail("INVOCATION_STATE_CONFLICT", input.fence.deliveryId);
    }
    run(
      this.#database,
      `INSERT INTO native_attempt_completions (
         delivery_id, attempt, reply_receipt_id, reply_result_digest,
         coordination_kind, coordination_terminal_turn_id,
         coordination_command_id, coordination_receipt_id,
         coordination_result_digest, contribution_binding_digest, recorded_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        input.fence.deliveryId,
        input.fence.attempt,
        reply.receiptId,
        reply.resultDigest,
        coordination.kind,
        coordinationColumns.terminalTurnId,
        coordinationColumns.commandId,
        coordinationColumns.receiptId,
        coordinationColumns.resultDigest,
        contribution.contributionBindingDigest,
        input.recordedAt,
      ],
    );
    return { applied: true };
  }

  /**
   * Join every contribution field against storage's own journal truth: the
   * hardened attempt fence, the attempt row's permit/invocation binding, the
   * input_written↔model_visible entry chain, and the normalized terminal turn
   * record. A valid sibling-turn or sibling-contribution receipt fails here even
   * when its digests are internally consistent.
   */
  #requireContributionJoin(
    fence: DeliveryFence,
    row: {
      permit_id: string | null;
      invocation_id: string | null;
      invocation_generation: number | null;
      state_instance_id: string | null;
    },
    evidence: TurnCompletionEvidence,
  ): void {
    const { contribution } = evidence;
    const mismatch = (): never =>
      storageFail("WRITE_STARTED_BINDING_MISMATCH", {
        deliveryId: fence.deliveryId,
        attempt: fence.attempt,
      });
    if (
      contribution.stateInstanceId !== row.state_instance_id ||
      contribution.permitId !== row.permit_id ||
      contribution.invocationId !== row.invocation_id ||
      contribution.invocationGeneration !== row.invocation_generation
    ) {
      mismatch();
    }
    const entries = all<{ kind: string; entry_json: string }>(
      this.#database,
      `SELECT kind, entry_json FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = ? AND invocation_generation = ?
         AND kind IN ('input_written', 'model_visible')`,
      [fence.deliveryId, fence.attempt, contribution.invocationGeneration],
    );
    const byKind = new Map(
      entries.map((entry) => [
        entry.kind,
        JSON.parse(entry.entry_json) as Record<string, unknown>,
      ]),
    );
    const written = byKind.get("input_written");
    const visible = byKind.get("model_visible");
    if (written === undefined || visible === undefined) {
      storageFail("INVALID_JOURNAL_CHAIN", {
        deliveryId: fence.deliveryId,
        attempt: fence.attempt,
      });
    }
    if (
      written.runtimeWriteId !== contribution.runtimeWriteId ||
      visible.runtimeWriteId !== contribution.runtimeWriteId ||
      visible.visibilityEventId !== contribution.visibilityEventId
    ) {
      mismatch();
    }
    const turn = one<{
      launch_id: string;
      session_id: string;
      input_ordinal: number;
      state: string;
    }>(
      this.#database,
      `SELECT launch_id, session_id, input_ordinal, state
       FROM local_turns WHERE protocol_turn_id = ?`,
      [fence.turnId],
    );
    if (
      turn === undefined ||
      turn.state !== "completed" ||
      turn.launch_id !== fence.launchId ||
      turn.session_id !== fence.sessionId
    ) {
      storageFail("INVALID_JOURNAL_CHAIN", {
        deliveryId: fence.deliveryId,
        attempt: fence.attempt,
        turnId: fence.turnId,
      });
    }
    if (Number(turn.input_ordinal) !== contribution.inputOrdinal) {
      mismatch();
    }
  }

  // --- AtomicTurnJournalPort (frozen contract, #creative-projects:a52e4ead msg bbfcd8de) ---

  #requireTurnReaderFence(fence: TurnReaderFence): {
    next_ordinal: number;
  } {
    assertProtocolId(fence.stateInstanceId, "sti");
    assertProtocolId(fence.sessionId, "ses");
    assertArtifactDigest(fence.ownerToken);
    if (!Number.isSafeInteger(fence.readerEpoch) || fence.readerEpoch < 1) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", fence.stateInstanceId);
    }
    const cursor = one<{
      session_id: string;
      next_ordinal: number;
      reader_owner_token: string | null;
      reader_epoch: number;
    }>(
      this.#database,
      `SELECT session_id, next_ordinal, reader_owner_token, reader_epoch
       FROM driver_event_cursor WHERE state_instance_id = ?`,
      [fence.stateInstanceId],
    );
    if (
      cursor === undefined ||
      cursor.session_id !== fence.sessionId ||
      cursor.reader_owner_token !== fence.ownerToken ||
      Number(cursor.reader_epoch) !== fence.readerEpoch
    ) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", fence.stateInstanceId);
    }
    return { next_ordinal: Number(cursor.next_ordinal) };
  }

  #turnRow(protocolTurnId: TurnId): TurnRowShape | undefined {
    return one<TurnRowShape>(
      this.#database,
      `SELECT protocol_turn_id, launch_id, state_instance_id, session_id,
              input_ordinal, state, binding_digest, steerable, operation_digest
       FROM local_turns WHERE protocol_turn_id = ?`,
      [protocolTurnId],
    );
  }

  #derivedReplyCommitted(protocolTurnId: TurnId): boolean {
    const row = one<{ found: number }>(
      this.#database,
      `SELECT 1 AS found FROM native_attempt_completions c
       JOIN native_attempts a
         ON a.delivery_id = c.delivery_id AND a.attempt = c.attempt
       WHERE json_extract(a.fence_json, '$.turnId') = ? LIMIT 1`,
      [protocolTurnId],
    );
    return row !== undefined;
  }

  #durableTurnState(row: TurnRowShape): DurableTurnState {
    return {
      protocolTurnId: row.protocol_turn_id as TurnId,
      phase: row.state as LocalTurnState,
      inputOrdinal: Number(row.input_ordinal),
      bindingDigest: (row.binding_digest ?? null) as ArtifactDigest | null,
      steerable: Number(row.steerable) === 1,
      replyCommitted: this.#derivedReplyCommitted(row.protocol_turn_id as TurnId),
    };
  }

  #assertDurableShape(state: DurableTurnState): void {
    assertProtocolId(state.protocolTurnId, "trn");
    if (!Number.isSafeInteger(state.inputOrdinal) || state.inputOrdinal < 0) {
      storageFail("INVALID_STATE_TRANSITION", state.inputOrdinal);
    }
    if (state.bindingDigest !== null) assertArtifactDigest(state.bindingDigest);
    if (typeof state.steerable !== "boolean" || typeof state.replyCommitted !== "boolean") {
      storageFail("INVALID_STATE_TRANSITION", state.protocolTurnId);
    }
  }

  #requireExpectedTurnState(row: TurnRowShape, expected: DurableTurnState): void {
    const actual = this.#durableTurnState(row);
    if (
      actual.protocolTurnId !== expected.protocolTurnId ||
      actual.phase !== expected.phase ||
      actual.inputOrdinal !== expected.inputOrdinal ||
      actual.bindingDigest !== expected.bindingDigest ||
      actual.steerable !== expected.steerable ||
      actual.replyCommitted !== expected.replyCommitted
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", expected.protocolTurnId);
    }
  }

  /**
   * Typed full validation of the stored terminal entry chain. No exported
   * protocol verifier exists for InvocationJournalEntry (types only), so this
   * validates with the same canonical primitives the append path uses: exact
   * key sets, brand checks, full embedded-fence binding to the hardened
   * attempt fence, identity binding to the attempt row, recomputed
   * entryDigest, and the input_written→model_visible predecessor chain.
   * Missing/malformed/digest-corrupt/predecessor-corrupt → INVALID_JOURNAL_
   * CHAIN; cross-fence or identity divergence → WRITE_STARTED_BINDING_
   * MISMATCH. Zero mutation in every path. Entries were persisted via
   * canonicalProtocolJson, so JSON.parse yields enumerable string keys only
   * and Object.keys exactness is complete.
   */
  #requireTerminalEntryChain(
    deliveryId: DeliveryId,
    attempt: number,
    generation: number,
    fenceJson: string,
    invocationId: string | null,
    permitId: string | null,
    bodyDigest: string | null,
  ): {
    writtenEntry: { runtimeWriteId: CommandId; sequence: number; entryDigest: ArtifactDigest };
    visibleEntry: { runtimeWriteId: CommandId; visibilityEventId: CommandId };
  } {
    const chainFail = (): never =>
      storageFail("INVALID_JOURNAL_CHAIN", { deliveryId, attempt });
    const bindingFail = (): never =>
      storageFail("WRITE_STARTED_BINDING_MISMATCH", { deliveryId, attempt });
    const BASE_KEYS = [
      "journalId",
      "entryId",
      "sequence",
      "kind",
      "previousEntryDigest",
      "entryDigest",
      ...DELIVERY_FENCE_KEYS,
      "invocationGeneration",
      "invocationId",
      "permitId",
    ];
    const validate = (
      kind: "permit_recorded" | "write_started" | "input_written" | "model_visible",
      extraKeys: readonly string[],
    ): Record<string, unknown> => {
      const stored = one<{ entry_json: string; sequence: number }>(
        this.#database,
        `SELECT entry_json, sequence FROM native_invocation_entries
         WHERE delivery_id = ? AND attempt = ? AND invocation_generation = ?
           AND kind = ?`,
        [deliveryId, attempt, generation, kind],
      );
      if (stored === undefined) return chainFail();
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(stored.entry_json) as Record<string, unknown>;
      } catch {
        return chainFail();
      }
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        chainFail();
      }
      const expectedKeys = [...BASE_KEYS, ...extraKeys].sort();
      const actualKeys = Object.keys(parsed).sort();
      if (
        expectedKeys.length !== actualKeys.length ||
        expectedKeys.some((key, index) => key !== actualKeys[index])
      ) {
        chainFail();
      }
      try {
        assertProtocolId(parsed.journalId as string, "cmd");
        assertProtocolId(parsed.entryId as string, "cmd");
        assertProtocolId(parsed.invocationId as string, "cmd");
        assertProtocolId(parsed.permitId as string, "cmd");
        if (kind === "write_started") {
          assertArtifactDigest(parsed.inputDigest as string);
        } else if (kind !== "permit_recorded") {
          assertProtocolId(parsed.runtimeWriteId as string, "cmd");
        }
        if (kind === "model_visible") {
          assertProtocolId(parsed.visibilityEventId as string, "cmd");
        }
        assertArtifactDigest(parsed.entryDigest as string);
        if (kind === "permit_recorded") {
          if (parsed.previousEntryDigest !== null) return chainFail();
        } else {
          assertArtifactDigest(parsed.previousEntryDigest as string);
        }
      } catch {
        return chainFail();
      }
      if (
        parsed.kind !== kind ||
        !Number.isSafeInteger(parsed.sequence) ||
        (parsed.sequence as number) < 1 ||
        (parsed.sequence as number) !== Number(stored.sequence) ||
        parsed.invocationGeneration !== generation ||
        parsed.journalId !== parsed.invocationId
      ) {
        chainFail();
      }
      const fenceProjection: Record<string, unknown> = {};
      for (const key of DELIVERY_FENCE_KEYS) {
        fenceProjection[key] = parsed[key];
      }
      let projectionJson: string;
      try {
        projectionJson = new TextDecoder().decode(
          canonicalProtocolJson(fenceProjection),
        );
      } catch {
        return chainFail();
      }
      if (projectionJson !== fenceJson) bindingFail();
      if (parsed.invocationId !== invocationId || parsed.permitId !== permitId) {
        bindingFail();
      }
      const unsigned = Object.fromEntries(
        Object.entries(parsed).filter(([key]) => key !== "entryDigest"),
      );
      if (digestCanonical(unsigned) !== parsed.entryDigest) chainFail();
      return parsed;
    };
    const permitRecorded = validate("permit_recorded", []);
    const started = validate("write_started", ["inputDigest"]);
    const written = validate("input_written", ["runtimeWriteId"]);
    const visible = validate("model_visible", ["runtimeWriteId", "visibilityEventId"]);
    if (
      (permitRecorded.sequence as number) !== 1 ||
      (started.sequence as number) !== 2 ||
      started.previousEntryDigest !== permitRecorded.entryDigest ||
      written.previousEntryDigest !== started.entryDigest ||
      (written.sequence as number) !== (started.sequence as number) + 1 ||
      visible.previousEntryDigest !== written.entryDigest ||
      (visible.sequence as number) !== (written.sequence as number) + 1
    ) {
      chainFail();
    }
    if (bodyDigest === null || started.inputDigest !== bodyDigest) {
      bindingFail();
    }
    return {
      writtenEntry: {
        runtimeWriteId: written.runtimeWriteId as CommandId,
        sequence: written.sequence as number,
        entryDigest: written.entryDigest as ArtifactDigest,
      },
      visibleEntry: {
        runtimeWriteId: visible.runtimeWriteId as CommandId,
        visibilityEventId: visible.visibilityEventId as CommandId,
      },
    };
  }

  beginTurnContribution(input: BeginTurnContributionInput): TurnMutationResult {
    this.#requireTurnReaderFence(input);
    assertProtocolId(input.protocolTurnId, "trn");
    assertProtocolId(input.launchId, "lnc");
    assertProtocolId(input.rootProducerFactId, "fac");
    assertArtifactDigest(input.driverTurnRefDigest);
    assertArtifactDigest(input.bindingDigest);
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
      storageFail("INVALID_STATE_TRANSITION", input.protocolTurnId);
    }
    if (input.mode.kind === "ordinary") {
      if (input.expected !== null || input.inputOrdinal !== 0) {
        storageFail("INVALID_STATE_TRANSITION", input.protocolTurnId);
      }
    } else {
      if (
        input.expected === null ||
        input.mode.expectedTurnId !== input.protocolTurnId ||
        input.expected.protocolTurnId !== input.protocolTurnId ||
        input.expected.steerable !== true ||
        input.inputOrdinal < 1
      ) {
        storageFail("INVALID_STATE_TRANSITION", input.protocolTurnId);
      }
    }
    const operationDigest = digestCanonical({
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
      expected: input.expected,
      next: input.next,
    });
    const row = this.#turnRow(input.protocolTurnId);
    if (row !== undefined && row.operation_digest === operationDigest) {
      return { applied: false, durable: this.#durableTurnState(row) };
    }
    if (input.mode.kind === "ordinary") {
      if (row !== undefined) {
        storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
      try {
        run(
          this.#database,
          `INSERT INTO local_turns (
             protocol_turn_id, launch_id, state_instance_id, session_id,
             root_producer_fact_id, input_ordinal, driver_turn_ref_digest,
             mode, expected_turn_id, state, binding_digest, steerable,
             operation_digest, queued_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ordinary', NULL, 'write_started', ?, 0, ?, ?, ?)`,
          [
            input.protocolTurnId,
            input.launchId,
            input.stateInstanceId,
            input.sessionId,
            input.rootProducerFactId,
            input.inputOrdinal,
            input.driverTurnRefDigest,
            input.bindingDigest,
            operationDigest,
            input.recordedAt,
            input.recordedAt,
          ],
        );
      } catch {
        storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
    } else {
      if (
        row === undefined ||
        row.state_instance_id !== input.stateInstanceId ||
        row.session_id !== input.sessionId ||
        row.launch_id !== input.launchId
      ) {
        storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
      this.#requireExpectedTurnState(row, input.expected as DurableTurnState);
      run(
        this.#database,
        `UPDATE local_turns
         SET mode = 'steer', expected_turn_id = protocol_turn_id,
             input_ordinal = ?, state = 'write_started', binding_digest = ?,
             steerable = 0, operation_digest = ?, updated_at = ?
         WHERE protocol_turn_id = ?`,
        [
          input.inputOrdinal,
          input.bindingDigest,
          operationDigest,
          input.recordedAt,
          input.protocolTurnId,
        ],
      );
    }
    const written = this.#turnRow(input.protocolTurnId);
    if (written === undefined) storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    return { applied: true, durable: this.#durableTurnState(written) };
  }

  commitTurnStep(input: CommitTurnStepInput): TurnStepResult {
    const cursor = this.#requireTurnReaderFence(input);
    assertProtocolId(input.event.turnId, "trn");
    assertArtifactDigest(input.event.eventDigest);
    assertArtifactDigest(input.event.bindingDigest);
    this.#assertDurableShape(input.expected);
    this.#assertDurableShape(input.next);
    const kinds: readonly TurnStepKind[] = [
      "turn_started",
      "input_written",
      "model_visible",
      "turn_boundary",
    ];
    if (!kinds.includes(input.kind)) {
      storageFail("INVALID_STATE_TRANSITION", input.kind);
    }
    if (
      input.event.turnId !== input.expected.protocolTurnId ||
      input.next.protocolTurnId !== input.expected.protocolTurnId ||
      input.next.inputOrdinal !== input.expected.inputOrdinal ||
      input.event.bindingDigest !== input.expected.bindingDigest ||
      input.next.replyCommitted !== input.expected.replyCommitted
    ) {
      storageFail("INVALID_STATE_TRANSITION", input.event.turnId);
    }
    const operationDigest = digestCanonical({
      method: "commitTurnStep",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      event: input.event,
      kind: input.kind,
      expected: input.expected,
      next: input.next,
    });
    const occupied = one<{ operation_digest: string | null }>(
      this.#database,
      `SELECT operation_digest FROM driver_event_records
       WHERE state_instance_id = ? AND session_id = ? AND ordinal = ?`,
      [input.stateInstanceId, input.sessionId, input.event.ordinal],
    );
    if (occupied !== undefined) {
      if (occupied.operation_digest === operationDigest) {
        const row = this.#turnRow(input.event.turnId);
        if (row === undefined) storageFail("ACTIVE_TURN_CONFLICT", input.event.turnId);
        return {
          applied: false,
          nextOrdinal: cursor.next_ordinal,
          durable: this.#durableTurnState(row),
        };
      }
      storageFail("ACTIVE_TURN_CONFLICT", input.event.ordinal);
    }
    const row = this.#turnRow(input.event.turnId);
    if (
      row === undefined ||
      row.state_instance_id !== input.stateInstanceId ||
      row.session_id !== input.sessionId
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", input.event.turnId);
    }
    this.#requireExpectedTurnState(row, input.expected);
    if (cursor.next_ordinal !== input.event.ordinal) {
      storageFail("DRIVER_EVENT_ORDER_INVALID", {
        expected: cursor.next_ordinal,
        actual: input.event.ordinal,
      });
    }
    run(
      this.#database,
      `INSERT INTO driver_event_records (
         state_instance_id, session_id, ordinal, event_digest, turn_id,
         binding_digest, operation_digest, recorded_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        input.stateInstanceId,
        input.sessionId,
        input.event.ordinal,
        input.event.eventDigest,
        input.event.turnId,
        input.event.bindingDigest,
        operationDigest,
        input.recordedAt,
      ],
    );
    run(
      this.#database,
      `UPDATE driver_event_cursor
       SET next_ordinal = ?, last_event_digest = ?, updated_at = ?
       WHERE state_instance_id = ?`,
      [input.event.ordinal + 1, input.event.eventDigest, input.recordedAt, input.stateInstanceId],
    );
    run(
      this.#database,
      `UPDATE local_turns
       SET state = ?, binding_digest = ?, steerable = ?, updated_at = ?
       WHERE protocol_turn_id = ?`,
      [
        input.next.phase,
        input.next.bindingDigest,
        input.next.steerable ? 1 : 0,
        input.recordedAt,
        input.event.turnId,
      ],
    );
    const written = this.#turnRow(input.event.turnId);
    if (written === undefined) storageFail("ACTIVE_TURN_CONFLICT", input.event.turnId);
    const durable = this.#durableTurnState(written);
    if (
      durable.phase !== input.next.phase ||
      durable.bindingDigest !== input.next.bindingDigest ||
      durable.steerable !== input.next.steerable ||
      durable.replyCommitted !== input.next.replyCommitted
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", input.event.turnId);
    }
    return { applied: true, nextOrdinal: input.event.ordinal + 1, durable };
  }

  commitTurnTerminal(input: CommitTurnTerminalInput): TurnStepResult {
    const cursor = this.#requireTurnReaderFence(input);
    this.#assertDurableShape(input.expected);
    this.#assertDurableShape(input.next);
    const members = [
      input.basis.reply,
      ...(input.basis.coordination ? [input.basis.coordination] : []),
      input.basis.completed,
    ];
    for (const member of members) {
      assertProtocolId(member.turnId, "trn");
      assertArtifactDigest(member.eventDigest);
      assertArtifactDigest(member.bindingDigest);
      if (
        member.turnId !== input.expected.protocolTurnId ||
        member.bindingDigest !== input.expected.bindingDigest
      ) {
        storageFail("INVALID_STATE_TRANSITION", member.turnId);
      }
    }
    assertProtocolId(input.basis.reply.replyCommandId, "cmd");
    if (input.basis.coordination !== undefined) {
      assertProtocolId(input.basis.coordination.commandId, "cmd");
      assertArtifactDigest(input.basis.coordination.commandDigest);
      if (input.basis.coordination.commandId === input.basis.reply.replyCommandId) {
        storageFail("SECOND_COORDINATION_CALL", input.basis.coordination.commandId);
      }
    }
    for (let index = 1; index < members.length; index += 1) {
      const current = members[index];
      const previous = members[index - 1];
      if (current === undefined || previous === undefined) {
        storageFail("INVALID_STATE_TRANSITION", index);
      }
      if (current.ordinal !== previous.ordinal + 1) {
        storageFail("DRIVER_EVENT_ORDER_INVALID", {
          expected: previous.ordinal + 1,
          actual: current.ordinal,
        });
      }
    }
    if (
      input.next.protocolTurnId !== input.expected.protocolTurnId ||
      input.next.inputOrdinal !== input.expected.inputOrdinal ||
      input.next.phase !== "completed" ||
      input.next.steerable !== false ||
      input.next.replyCommitted !== true ||
      input.next.bindingDigest !== input.expected.bindingDigest ||
      input.expected.replyCommitted !== false
    ) {
      storageFail("INVALID_STATE_TRANSITION", input.expected.protocolTurnId);
    }

    const attempt = one<{
      state: string;
      fence_json: string;
      state_instance_id: string | null;
      permit_id: string | null;
      invocation_id: string | null;
      invocation_generation: number | null;
      body_digest: string | null;
    }>(
      this.#database,
      `SELECT state, fence_json, state_instance_id, permit_id, invocation_id,
              invocation_generation, body_digest
       FROM native_attempts WHERE delivery_id = ? AND attempt = ?`,
      [input.evidence.contribution.fence.deliveryId, input.evidence.contribution.fence.attempt],
    );
    if (attempt === undefined) {
      storageFail("STALE_DELIVERY_FENCE", input.evidence.contribution.fence.deliveryId);
    }
    const storedFence = JSON.parse(attempt.fence_json) as DeliveryFence;
    if (storedFence.turnId !== input.expected.protocolTurnId) {
      storageFail("WRITE_STARTED_BINDING_MISMATCH", {
        deliveryId: storedFence.deliveryId,
        attempt: storedFence.attempt,
      });
    }
    let evidence: TurnCompletionEvidence;
    try {
      evidence = verifyTurnCompletionEvidence(input.evidence, {
        expectedFence: storedFence,
      });
    } catch {
      storageFail("INVALID_JOURNAL_CHAIN", {
        deliveryId: storedFence.deliveryId,
        attempt: storedFence.attempt,
      });
    }
    if (
      evidence.contribution.stateInstanceId !== input.stateInstanceId ||
      evidence.contribution.stateInstanceId !== attempt.state_instance_id ||
      evidence.contribution.permitId !== attempt.permit_id ||
      evidence.contribution.invocationId !== attempt.invocation_id ||
      evidence.contribution.invocationGeneration !== attempt.invocation_generation ||
      evidence.contribution.inputOrdinal !== input.expected.inputOrdinal
    ) {
      storageFail("WRITE_STARTED_BINDING_MISMATCH", {
        deliveryId: storedFence.deliveryId,
        attempt: storedFence.attempt,
      });
    }
    if (evidence.coordination.kind === "not_requested") {
      if (input.basis.coordination !== undefined) {
        storageFail("SECOND_COORDINATION_CALL", input.expected.protocolTurnId);
      }
    } else {
      if (
        input.basis.coordination === undefined ||
        input.basis.coordination.commandId !== evidence.coordination.commandId
      ) {
        storageFail("INVALID_JOURNAL_CHAIN", input.expected.protocolTurnId);
      }
    }

    const row = this.#turnRow(input.expected.protocolTurnId);
    if (
      row === undefined ||
      row.state_instance_id !== input.stateInstanceId ||
      row.session_id !== input.sessionId ||
      row.launch_id !== storedFence.launchId ||
      row.session_id !== storedFence.sessionId
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", input.expected.protocolTurnId);
    }
    // Stored-truth contribution join (runs BEFORE replay acceptance): the
    // evidence's runtime-write and visibility identities must equal the
    // committed invocation-entry chain, and the binding recomputed from stored
    // facts alone must equal the evidence binding. A forged self-consistent
    // contribution can therefore neither alias nor commit.
    const generation = attempt.invocation_generation ?? 1;
    const { writtenEntry, visibleEntry } = this.#requireTerminalEntryChain(
      storedFence.deliveryId,
      storedFence.attempt,
      generation,
      attempt.fence_json,
      attempt.invocation_id,
      attempt.permit_id,
      attempt.body_digest,
    );
    if (
      writtenEntry.runtimeWriteId !== evidence.contribution.runtimeWriteId ||
      visibleEntry.runtimeWriteId !== evidence.contribution.runtimeWriteId ||
      visibleEntry.visibilityEventId !== evidence.contribution.visibilityEventId
    ) {
      storageFail("WRITE_STARTED_BINDING_MISMATCH", {
        deliveryId: storedFence.deliveryId,
        attempt: storedFence.attempt,
      });
    }
    let storedBindingDigest: ArtifactDigest;
    try {
      storedBindingDigest = buildContributionBinding({
        fence: storedFence,
        stateInstanceId: attempt.state_instance_id as StateInstanceId,
        inputOrdinal: Number(row.input_ordinal),
        invocationId: attempt.invocation_id as CommandId,
        invocationGeneration: generation,
        permitId: attempt.permit_id as CommandId,
        runtimeWriteId: visibleEntry.runtimeWriteId as CommandId,
        visibilityEventId: visibleEntry.visibilityEventId as CommandId,
      }).contributionBindingDigest;
    } catch {
      storageFail("INVALID_JOURNAL_CHAIN", {
        deliveryId: storedFence.deliveryId,
        attempt: storedFence.attempt,
      });
    }
    if (storedBindingDigest !== evidence.contribution.contributionBindingDigest) {
      storageFail("WRITE_STARTED_BINDING_MISMATCH", {
        deliveryId: storedFence.deliveryId,
        attempt: storedFence.attempt,
      });
    }

    const operationDigest = digestCanonical({
      method: "commitTurnTerminal",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      basis: input.basis,
      evidence: input.evidence,
      expected: input.expected,
      next: input.next,
    });
    const existing = one<{ operation_digest: string | null }>(
      this.#database,
      `SELECT operation_digest FROM native_attempt_completions
       WHERE delivery_id = ? AND attempt = ?`,
      [storedFence.deliveryId, storedFence.attempt],
    );
    if (existing !== undefined) {
      // Replay identity is the logical operation digest alone: an exact replay
      // must alias even after the attempt legally advanced to consumed, so the
      // first-insert-only predecessor gates below never run on this path.
      if (existing.operation_digest === operationDigest) {
        return {
          applied: false,
          nextOrdinal: cursor.next_ordinal,
          durable: this.#durableTurnState(row),
        };
      }
      storageFail("INVOCATION_STATE_CONFLICT", storedFence.deliveryId);
    }
    // First-insert-only predecessor gates: attempt must still be model_visible
    // with the observed-ACK ledger row present, and the turn must match the
    // full pre-terminal expected state.
    if (attempt.state !== "model_visible") {
      storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", storedFence.deliveryId);
    }
    const ledger = one<{ found: number }>(
      this.#database,
      `SELECT 1 AS found FROM visible_message_ids
       WHERE delivery_id = ? AND attempt = ? LIMIT 1`,
      [storedFence.deliveryId, storedFence.attempt],
    );
    if (ledger === undefined) {
      storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", storedFence.deliveryId);
    }
    this.#requireExpectedTurnState(row, input.expected);
    if (cursor.next_ordinal !== input.basis.reply.ordinal) {
      storageFail("DRIVER_EVENT_ORDER_INVALID", {
        expected: cursor.next_ordinal,
        actual: input.basis.reply.ordinal,
      });
    }
    for (const member of members) {
      run(
        this.#database,
        `INSERT INTO driver_event_records (
           state_instance_id, session_id, ordinal, event_digest, turn_id,
           binding_digest, operation_digest, recorded_at
         ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`,
        [
          input.stateInstanceId,
          input.sessionId,
          member.ordinal,
          member.eventDigest,
          member.turnId,
          member.bindingDigest,
          input.recordedAt,
        ],
      );
    }
    const nextOrdinal = input.basis.completed.ordinal + 1;
    run(
      this.#database,
      `UPDATE driver_event_cursor
       SET next_ordinal = ?, last_event_digest = ?, updated_at = ?
       WHERE state_instance_id = ?`,
      [nextOrdinal, input.basis.completed.eventDigest, input.recordedAt, input.stateInstanceId],
    );
    const coordinationColumns =
      evidence.coordination.kind === "not_requested"
        ? {
            terminalTurnId: evidence.coordination.terminalTurnId,
            commandId: null,
            receiptId: null,
            resultDigest: null,
          }
        : {
            terminalTurnId: null,
            commandId: evidence.coordination.commandId,
            receiptId: evidence.coordination.receiptId,
            resultDigest: evidence.coordination.resultDigest,
          };
    run(
      this.#database,
      `INSERT INTO native_attempt_completions (
         delivery_id, attempt, reply_receipt_id, reply_result_digest,
         coordination_kind, coordination_terminal_turn_id,
         coordination_command_id, coordination_receipt_id,
         coordination_result_digest, contribution_binding_digest,
         reply_command_id, operation_digest, recorded_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        storedFence.deliveryId,
        storedFence.attempt,
        evidence.reply.receiptId,
        evidence.reply.resultDigest,
        evidence.coordination.kind,
        coordinationColumns.terminalTurnId,
        coordinationColumns.commandId,
        coordinationColumns.receiptId,
        coordinationColumns.resultDigest,
        evidence.contribution.contributionBindingDigest,
        input.basis.reply.replyCommandId,
        operationDigest,
        input.recordedAt,
      ],
    );
    run(
      this.#database,
      `UPDATE local_turns
       SET state = 'completed', binding_digest = ?, steerable = 0, updated_at = ?
       WHERE protocol_turn_id = ?`,
      [input.next.bindingDigest, input.recordedAt, input.expected.protocolTurnId],
    );
    const written = this.#turnRow(input.expected.protocolTurnId);
    if (written === undefined) storageFail("ACTIVE_TURN_CONFLICT", input.expected.protocolTurnId);
    const durable = this.#durableTurnState(written);
    if (durable.replyCommitted !== true || durable.phase !== "completed") {
      storageFail("ACTIVE_TURN_CONFLICT", input.expected.protocolTurnId);
    }
    return { applied: true, nextOrdinal, durable };
  }

  settleTurnContribution(input: SettleTurnContributionInput): TurnMutationResult {
    this.#requireTurnReaderFence(input);
    assertProtocolId(input.protocolTurnId, "trn");
    this.#assertDurableShape(input.expected);
    this.#assertDurableShape(input.next);
    const kinds: readonly TurnSettleKind[] = ["terminal_error", "interrupted", "ambiguous"];
    if (!kinds.includes(input.kind)) {
      storageFail("INVALID_STATE_TRANSITION", input.kind);
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
      storageFail("INVALID_STATE_TRANSITION", input.protocolTurnId);
    }
    const operationDigest = digestCanonical({
      method: "settleTurnContribution",
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      protocolTurnId: input.protocolTurnId,
      inputOrdinal: input.inputOrdinal,
      kind: input.kind,
      expected: input.expected,
      next: input.next,
    });
    const row = this.#turnRow(input.protocolTurnId);
    if (row !== undefined && row.operation_digest === operationDigest) {
      return { applied: false, durable: this.#durableTurnState(row) };
    }
    if (
      row === undefined ||
      row.state_instance_id !== input.stateInstanceId ||
      row.session_id !== input.sessionId
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    this.#requireExpectedTurnState(row, input.expected);
    run(
      this.#database,
      `UPDATE local_turns
       SET state = ?, binding_digest = ?, steerable = 0, operation_digest = ?,
           updated_at = ?
       WHERE protocol_turn_id = ?`,
      [
        input.next.phase,
        input.next.bindingDigest,
        operationDigest,
        input.recordedAt,
        input.protocolTurnId,
      ],
    );
    const written = this.#turnRow(input.protocolTurnId);
    if (written === undefined) storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    return { applied: true, durable: this.#durableTurnState(written) };
  }

  readDurableTurnState(
    input: TurnReaderFence & { protocolTurnId: TurnId },
  ): DurableTurnState | null {
    this.#requireTurnReaderFence(input);
    assertProtocolId(input.protocolTurnId, "trn");
    const row = this.#turnRow(input.protocolTurnId);
    if (row === undefined) return null;
    if (
      row.state_instance_id !== input.stateInstanceId ||
      row.session_id !== input.sessionId
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    return this.#durableTurnState(row);
  }

  readVisibleMessage(
    input: ReadVisibleMessageInput,
  ): VisibleMessageRow | undefined {
    assertProtocolId(input.sessionId, "ses");
    assertProtocolId(input.messageId, "msg");
    const targetKey = canonicalTargetKey(input.target);
    const row = one<{
      delivery_id: string;
      attempt: number;
      server_seq: number;
      model_visible_receipt_id: string;
      visible_at: string;
    }>(
      this.#database,
      `SELECT delivery_id, attempt, server_seq, model_visible_receipt_id, visible_at
       FROM visible_message_ids
       WHERE session_id = ? AND target_key = ? AND message_id = ?`,
      [input.sessionId, targetKey, input.messageId],
    );
    if (row === undefined) return undefined;
    return {
      deliveryId: row.delivery_id as DeliveryId,
      attempt: Number(row.attempt),
      serverSeq: Number(row.server_seq),
      modelVisibleReceiptId: row.model_visible_receipt_id as ReceiptId,
      visibleAt: row.visible_at,
    };
  }

  readNativeAttempt(
    deliveryId: DeliveryId,
    attempt: number,
  ): NativeAttemptRow | undefined {
    assertProtocolId(deliveryId, "dlv");
    if (!Number.isSafeInteger(attempt) || attempt < 1) {
      storageFail("STALE_DELIVERY_FENCE", attempt);
    }
    const row = one<{
      state: string;
      permit_id: string | null;
      invocation_generation: number | null;
      invocation_id: string | null;
      body_digest: string | null;
      previous_invocation_generation: number | null;
      previous_proof_digest: string | null;
      disconnect_id: string | null;
      suppression_reason: string | null;
    }>(
      this.#database,
      `SELECT state, permit_id, invocation_generation, invocation_id, body_digest,
              previous_invocation_generation, previous_proof_digest,
              disconnect_id, suppression_reason
       FROM native_attempts WHERE delivery_id = ? AND attempt = ?`,
      [deliveryId, attempt],
    );
    if (row === undefined) return undefined;
    return {
      deliveryId,
      attempt,
      state: row.state as NativeAttemptState,
      permitId: row.permit_id as CommandId | null,
      invocationGeneration:
        row.invocation_generation === null ? null : Number(row.invocation_generation),
      invocationId: row.invocation_id as CommandId | null,
      bodyDigest: row.body_digest as ArtifactDigest | null,
      previousInvocationGeneration:
        row.previous_invocation_generation === null
          ? null
          : Number(row.previous_invocation_generation),
      previousProofDigest: row.previous_proof_digest as ArtifactDigest | null,
      disconnectId: row.disconnect_id as CommandId | null,
      suppressionReason: row.suppression_reason,
    };
  }

  commitVisibleMessage(input: CommitVisibleMessageInput): { applied: boolean } {
    assertProtocolId(input.sessionId, "ses");
    assertProtocolId(input.messageId, "msg");
    assertProtocolId(input.deliveryId, "dlv");
    assertProtocolId(input.modelVisibleAck.receiptId, "rcp");
    if (input.modelVisibleAck.observed !== true) {
      storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.deliveryId);
    }
    const targetKey = canonicalTargetKey(input.target);
    const columns = targetColumns(input.target);
    const delivery = one<{
      attempt: number;
      message_id: string;
      target_key: string;
      server_seq: number;
      session_id: string;
      model_visible_at: string | null;
    }>(
      this.#database,
      `SELECT attempt, message_id, target_key, server_seq, session_id, model_visible_at
       FROM pending_deliveries WHERE delivery_id = ?`,
      [input.deliveryId],
    );
    if (
      delivery === undefined ||
      Number(delivery.attempt) !== input.attempt ||
      delivery.message_id !== input.messageId ||
      delivery.target_key !== targetKey ||
      Number(delivery.server_seq) !== input.serverSeq ||
      delivery.session_id !== input.sessionId ||
      delivery.model_visible_at === null
    ) {
      storageFail("MODEL_VISIBLE_PREDECESSOR_REQUIRED", input.deliveryId);
    }
    const existing = one<{
      delivery_id: string;
      attempt: number;
      server_seq: number;
      model_visible_receipt_id: string;
      visible_at: string;
    }>(
      this.#database,
      `SELECT delivery_id, attempt, server_seq, model_visible_receipt_id, visible_at
       FROM visible_message_ids
       WHERE session_id = ? AND target_key = ? AND message_id = ?`,
      [input.sessionId, targetKey, input.messageId],
    );
    if (existing !== undefined) {
      if (
        existing.delivery_id === input.deliveryId &&
        Number(existing.attempt) === input.attempt &&
        Number(existing.server_seq) === input.serverSeq &&
        existing.model_visible_receipt_id === input.modelVisibleAck.receiptId &&
        existing.visible_at === input.visibleAt
      ) {
        return { applied: false };
      }
      storageFail("VISIBILITY_LEDGER_CONFLICT", input.messageId);
    }
    try {
      run(
        this.#database,
        `INSERT INTO visible_message_ids (
           session_id, target_key, message_id, delivery_id, attempt, server_seq,
           model_visible_receipt_id, visible_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          input.sessionId,
          targetKey,
          input.messageId,
          input.deliveryId,
          input.attempt,
          input.serverSeq,
          input.modelVisibleAck.receiptId,
          input.visibleAt,
        ],
      );
    } catch (cause) {
      storageFail("VISIBILITY_LEDGER_CONFLICT", cause);
    }
    run(
      this.#database,
      `INSERT INTO visibility_checkpoints (
         session_id, target_key, target_kind, target_id, thread_root_message_id,
         highest_model_visible_server_seq, last_message_id, last_delivery_id
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(session_id, target_key) DO UPDATE SET
         highest_model_visible_server_seq = excluded.highest_model_visible_server_seq,
         last_message_id = excluded.last_message_id,
         last_delivery_id = excluded.last_delivery_id
       WHERE excluded.highest_model_visible_server_seq > visibility_checkpoints.highest_model_visible_server_seq`,
      [
        input.sessionId,
        targetKey,
        columns.kind,
        columns.ownerId,
        columns.threadRootMessageId,
        input.serverSeq,
        input.messageId,
        input.deliveryId,
      ],
    );
    return { applied: true };
  }

  commitNoticeVisibility(input: CommitNoticeVisibilityInput): { applied: boolean } {
    assertProtocolId(input.sessionId, "ses");
    assertProtocolId(input.firstMessageId, "msg");
    assertProtocolId(input.latestMessageId, "msg");
    assertProtocolId(input.inputDeliveryId, "dlv");
    const targetKey = canonicalTargetKey(input.target);
    const predecessor = one<{ found: number }>(
      this.#database,
      `SELECT 1 AS found FROM native_invocation_entries
       WHERE delivery_id = ? AND attempt = ? AND kind = 'input_written'
       LIMIT 1`,
      [input.inputDeliveryId, input.inputAttempt],
    );
    if (predecessor === undefined) {
      storageFail("NOTICE_DEDUPE_PREDECESSOR_REQUIRED", input.inputDeliveryId);
    }
    const existing = one<{
      first_message_id: string;
      latest_message_id: string;
      first_server_seq: number;
      latest_server_seq: number;
      input_delivery_id: string;
      input_attempt: number;
    }>(
      this.#database,
      `SELECT first_message_id, latest_message_id, first_server_seq,
              latest_server_seq, input_delivery_id, input_attempt
       FROM notice_visibility
       WHERE session_id = ? AND target_key = ? AND membership_epoch = ?`,
      [input.sessionId, targetKey, input.membershipEpoch],
    );
    if (existing !== undefined) {
      if (
        existing.first_message_id === input.firstMessageId &&
        existing.latest_message_id === input.latestMessageId &&
        Number(existing.first_server_seq) === input.firstServerSeq &&
        Number(existing.latest_server_seq) === input.latestServerSeq &&
        existing.input_delivery_id === input.inputDeliveryId &&
        Number(existing.input_attempt) === input.inputAttempt
      ) {
        return { applied: false };
      }
      storageFail("VISIBILITY_LEDGER_CONFLICT", {
        sessionId: input.sessionId,
        targetKey,
        membershipEpoch: input.membershipEpoch,
      });
    }
    run(
      this.#database,
      `INSERT INTO notice_visibility (
         session_id, target_key, membership_epoch, first_message_id,
         latest_message_id, first_server_seq, latest_server_seq,
         input_delivery_id, input_attempt, committed_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        input.sessionId,
        targetKey,
        input.membershipEpoch,
        input.firstMessageId,
        input.latestMessageId,
        input.firstServerSeq,
        input.latestServerSeq,
        input.inputDeliveryId,
        input.inputAttempt,
        input.committedAt,
      ],
    );
    return { applied: true };
  }

  compareNoticeVisibility(
    input: CompareNoticeVisibilityInput,
  ): "new" | "replay" {
    assertProtocolId(input.sessionId, "ses");
    assertProtocolId(input.firstMessageId, "msg");
    assertProtocolId(input.latestMessageId, "msg");
    assertProtocolId(input.inputDeliveryId, "dlv");
    const targetKey = canonicalTargetKey(input.target);
    const existing = one<{
      first_message_id: string;
      latest_message_id: string;
      first_server_seq: number;
      latest_server_seq: number;
      input_delivery_id: string;
      input_attempt: number;
    }>(
      this.#database,
      `SELECT first_message_id, latest_message_id, first_server_seq,
              latest_server_seq, input_delivery_id, input_attempt
       FROM notice_visibility
       WHERE session_id = ? AND target_key = ? AND membership_epoch = ?`,
      [input.sessionId, targetKey, input.membershipEpoch],
    );
    if (existing === undefined) return "new";
    if (
      existing.first_message_id === input.firstMessageId &&
      existing.latest_message_id === input.latestMessageId &&
      Number(existing.first_server_seq) === input.firstServerSeq &&
      Number(existing.latest_server_seq) === input.latestServerSeq &&
      existing.input_delivery_id === input.inputDeliveryId &&
      Number(existing.input_attempt) === input.inputAttempt
    ) {
      return "replay";
    }
    storageFail("VISIBILITY_LEDGER_CONFLICT", {
      sessionId: input.sessionId,
      targetKey,
      membershipEpoch: input.membershipEpoch,
    });
  }

  prepareTurn(input: PrepareTurnInput): { applied: boolean } {
    assertProtocolId(input.protocolTurnId, "trn");
    assertProtocolId(input.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertProtocolId(input.rootProducerFactId, "fac");
    assertArtifactDigest(input.driverTurnRefDigest);
    if (!Number.isSafeInteger(input.inputOrdinal) || input.inputOrdinal < 0) {
      storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    if (
      (input.mode === "ordinary" && (input.inputOrdinal !== 0 || input.expectedTurnId !== undefined)) ||
      (input.mode === "steer" && (input.inputOrdinal < 1 || input.expectedTurnId !== input.protocolTurnId))
    ) {
      storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    const expectedTurnId = input.expectedTurnId ?? null;
    const existing = one<{
      launch_id: string;
      state_instance_id: string;
      session_id: string;
      root_producer_fact_id: string;
      input_ordinal: number;
      driver_turn_ref_digest: string;
      mode: string;
      expected_turn_id: string | null;
      state: string;
      queued_at: string;
    }>(this.#database, "SELECT * FROM local_turns WHERE protocol_turn_id = ?", [input.protocolTurnId]);
    if (existing !== undefined) {
      if (
        existing.launch_id === input.launchId &&
        existing.state_instance_id === input.stateInstanceId &&
        existing.session_id === input.sessionId &&
        existing.root_producer_fact_id === input.rootProducerFactId &&
        Number(existing.input_ordinal) === input.inputOrdinal &&
        existing.driver_turn_ref_digest === input.driverTurnRefDigest &&
        existing.mode === input.mode &&
        equalNullable(existing.expected_turn_id, expectedTurnId) &&
        existing.queued_at === input.queuedAt
      ) {
        return { applied: false };
      }
      if (
        input.mode !== "steer" ||
        existing.launch_id !== input.launchId ||
        existing.state_instance_id !== input.stateInstanceId ||
        existing.session_id !== input.sessionId ||
        existing.root_producer_fact_id !== input.rootProducerFactId ||
        existing.driver_turn_ref_digest !== input.driverTurnRefDigest
      ) {
        storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
      if (existing.state !== "model_visible") {
        storageFail("TURN_INPUT_ALREADY_IN_FLIGHT", input.protocolTurnId);
      }
      if (input.inputOrdinal !== Number(existing.input_ordinal) + 1) {
        storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
      }
      const result = run(
        this.#database,
        `UPDATE local_turns
         SET input_ordinal = ?, mode = 'steer', expected_turn_id = ?,
             state = 'queued', queued_at = ?, updated_at = ?
         WHERE protocol_turn_id = ? AND input_ordinal = ? AND state = 'model_visible'`,
        [
          input.inputOrdinal,
          expectedTurnId,
          input.queuedAt,
          input.queuedAt,
          input.protocolTurnId,
          existing.input_ordinal,
        ],
      );
      if (Number(result.changes) !== 1) {
        storageFail("TURN_INPUT_ALREADY_IN_FLIGHT", input.protocolTurnId);
      }
      return { applied: true };
    }
    if (input.mode !== "ordinary") {
      storageFail("ACTIVE_TURN_REQUIRED", input.protocolTurnId);
    }
    const active = one<{ protocol_turn_id: string }>(
      this.#database,
      `SELECT protocol_turn_id FROM local_turns
       WHERE session_id = ?
         AND state IN ('queued', 'write_started', 'input_written', 'model_visible', 'ambiguous')
       LIMIT 1`,
      [input.sessionId],
    );
    if (active !== undefined) storageFail("ACTIVE_TURN_CONFLICT", active.protocol_turn_id);
    run(
      this.#database,
      `INSERT INTO local_turns (
         protocol_turn_id, launch_id, state_instance_id, session_id,
         root_producer_fact_id, input_ordinal, driver_turn_ref_digest,
         mode, expected_turn_id, state, queued_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`,
      [
        input.protocolTurnId,
        input.launchId,
        input.stateInstanceId,
        input.sessionId,
        input.rootProducerFactId,
        input.inputOrdinal,
        input.driverTurnRefDigest,
        input.mode,
        expectedTurnId,
        input.queuedAt,
        input.queuedAt,
      ],
    );
    return { applied: true };
  }

  advanceTurn(input: AdvanceTurnInput): { applied: boolean } {
    assertProtocolId(input.protocolTurnId, "trn");
    if (!Number.isSafeInteger(input.inputOrdinal) || input.inputOrdinal < 0) {
      storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
    }
    const allowed = new Set([
      "queued:write_started",
      "queued:ambiguous",
      "queued:interrupted",
      "queued:terminal_error",
      "write_started:input_written",
      "write_started:ambiguous",
      "write_started:interrupted",
      "write_started:terminal_error",
      "input_written:model_visible",
      "input_written:ambiguous",
      "input_written:interrupted",
      "input_written:terminal_error",
      "model_visible:completed",
      "model_visible:interrupted",
      "model_visible:terminal_error",
      "ambiguous:terminal_error",
    ]);
    if (!allowed.has(`${input.expectedState}:${input.nextState}`)) {
      storageFail("INVALID_STATE_TRANSITION", input);
    }
    const result = run(
      this.#database,
      `UPDATE local_turns SET state = ?, updated_at = ?
       WHERE protocol_turn_id = ? AND input_ordinal = ? AND state = ?`,
      [
        input.nextState,
        input.updatedAt,
        input.protocolTurnId,
        input.inputOrdinal,
        input.expectedState,
      ],
    );
    if (Number(result.changes) === 1) return { applied: true };
    const existing = one<{ input_ordinal: number; state: string; updated_at: string }>(
      this.#database,
      `SELECT input_ordinal, state, updated_at FROM local_turns
       WHERE protocol_turn_id = ?`,
      [input.protocolTurnId],
    );
    if (
      existing !== undefined &&
      Number(existing.input_ordinal) === input.inputOrdinal &&
      existing.state === input.nextState &&
      existing.updated_at === input.updatedAt
    ) {
      return { applied: false };
    }
    storageFail("ACTIVE_TURN_CONFLICT", input.protocolTurnId);
  }

  claimDriverEventReader(input: DriverEventReaderClaim): { readerEpoch: number } {
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.ownerToken);
    const existing = one<{
      session_id: string;
      reader_owner_token: string | null;
      reader_epoch: number;
    }>(
      this.#database,
      `SELECT session_id, reader_owner_token, reader_epoch FROM driver_event_cursor
       WHERE state_instance_id = ?`,
      [input.stateInstanceId],
    );
    if (existing === undefined) {
      if (input.mode !== "start") {
        storageFail(
          input.mode === "resume" ? "DRIVER_EVENT_FENCE_MISMATCH" : "DRIVER_EVENT_READER_CONFLICT",
          input.stateInstanceId,
        );
      }
      run(
        this.#database,
        `INSERT INTO driver_event_cursor (
           state_instance_id, session_id, next_ordinal, reader_owner_token,
           reader_epoch, updated_at
         ) VALUES (?, ?, 0, ?, 1, ?)`,
        [input.stateInstanceId, input.sessionId, input.ownerToken, input.claimedAt],
      );
      return { readerEpoch: 1 };
    }
    if (existing.session_id !== input.sessionId) {
      storageFail("DRIVER_EVENT_READER_CONFLICT", input.stateInstanceId);
    }
    if (input.mode !== "resume") {
      storageFail("DRIVER_EVENT_READER_CONFLICT", input.stateInstanceId);
    }
    if (existing.reader_owner_token !== null) {
      storageFail("DRIVER_RESUME_OVERLAP", input.stateInstanceId);
    }
    const nextEpoch = Number(existing.reader_epoch) + 1;
    const result = run(
      this.#database,
      `UPDATE driver_event_cursor
       SET reader_owner_token = ?, reader_epoch = ?, updated_at = ?
       WHERE state_instance_id = ? AND session_id = ? AND reader_owner_token IS NULL
         AND reader_epoch = ?`,
      [
        input.ownerToken,
        nextEpoch,
        input.claimedAt,
        input.stateInstanceId,
        input.sessionId,
        existing.reader_epoch,
      ],
    );
    if (Number(result.changes) !== 1) storageFail("DRIVER_RESUME_OVERLAP", input.stateInstanceId);
    return { readerEpoch: nextEpoch };
  }

  releaseDriverEventReader(input: {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    readerEpoch: number;
    releasedAt: string;
  }): void {
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.ownerToken);
    if (!Number.isSafeInteger(input.readerEpoch) || input.readerEpoch < 1) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
    }
    const result = run(
      this.#database,
      `UPDATE driver_event_cursor SET reader_owner_token = NULL, updated_at = ?
       WHERE state_instance_id = ? AND session_id = ? AND reader_owner_token = ?
         AND reader_epoch = ?`,
      [
        input.releasedAt,
        input.stateInstanceId,
        input.sessionId,
        input.ownerToken,
        input.readerEpoch,
      ],
    );
    if (Number(result.changes) !== 1) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
    }
  }

  commitDriverEvent(input: CommitDriverEventInput): { applied: boolean; nextOrdinal: number } {
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.ownerToken);
    if (!Number.isSafeInteger(input.readerEpoch) || input.readerEpoch < 1) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
    }
    assertArtifactDigest(input.eventDigest);
    if (input.turnId !== undefined) assertProtocolId(input.turnId, "trn");
    if (input.bindingDigest !== undefined) assertArtifactDigest(input.bindingDigest);
    const turnId = input.turnId ?? null;
    const bindingDigest = input.bindingDigest ?? null;
    const cursor = one<{
      session_id: string;
      next_ordinal: number;
      reader_owner_token: string | null;
      reader_epoch: number;
    }>(
      this.#database,
      `SELECT session_id, next_ordinal, reader_owner_token, reader_epoch
       FROM driver_event_cursor WHERE state_instance_id = ?`,
      [input.stateInstanceId],
    );
    if (
      cursor === undefined ||
      cursor.session_id !== input.sessionId ||
      cursor.reader_owner_token !== input.ownerToken ||
      Number(cursor.reader_epoch) !== input.readerEpoch
    ) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
    }
    const occupied = one<{
      event_digest: string;
      turn_id: string | null;
      binding_digest: string | null;
    }>(
      this.#database,
      `SELECT event_digest, turn_id, binding_digest FROM driver_event_records
       WHERE state_instance_id = ? AND session_id = ? AND ordinal = ?`,
      [input.stateInstanceId, input.sessionId, input.ordinal],
    );
    if (occupied !== undefined) {
      if (
        occupied.event_digest === input.eventDigest &&
        equalNullable(occupied.turn_id, turnId) &&
        equalNullable(occupied.binding_digest, bindingDigest)
      ) {
        return { applied: false, nextOrdinal: Number(cursor.next_ordinal) };
      }
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.ordinal);
    }
    if (Number(cursor.next_ordinal) !== input.ordinal) {
      storageFail("DRIVER_EVENT_ORDER_INVALID", {
        expected: Number(cursor.next_ordinal),
        actual: input.ordinal,
      });
    }
    run(
      this.#database,
      `INSERT INTO driver_event_records (
         state_instance_id, session_id, ordinal, event_digest, turn_id,
         binding_digest, recorded_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        input.stateInstanceId,
        input.sessionId,
        input.ordinal,
        input.eventDigest,
        turnId,
        bindingDigest,
        input.recordedAt,
      ],
    );
    const result = run(
      this.#database,
      `UPDATE driver_event_cursor
       SET next_ordinal = ?, last_event_digest = ?, updated_at = ?
       WHERE state_instance_id = ? AND session_id = ? AND next_ordinal = ?
         AND reader_owner_token = ? AND reader_epoch = ?`,
      [
        input.ordinal + 1,
        input.eventDigest,
        input.recordedAt,
        input.stateInstanceId,
        input.sessionId,
        input.ordinal,
        input.ownerToken,
        input.readerEpoch,
      ],
    );
    if (Number(result.changes) !== 1) storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.ordinal);
    return { applied: true, nextOrdinal: input.ordinal + 1 };
  }
}
