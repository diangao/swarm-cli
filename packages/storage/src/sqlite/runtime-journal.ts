import { createHash } from "node:crypto";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";

import {
  canonicalProtocolJson,
  type DeliveryFence,
  type InvocationJournalEntry,
} from "@swarm/protocol";
import type {
  AgentId,
  ArtifactDigest,
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

function equalNullable(left: string | null, right: string | null): boolean {
  return left === right;
}

function digestCanonical(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

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
