import { createHash, randomBytes } from "node:crypto";
import {
  chmodSync,
  closeSync,
  fstatSync,
  openSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename } from "node:path";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import { canonicalProtocolJson } from "@swarm/protocol";
import type {
  ArtifactDigest,
  DeliveryEnvelope,
  LaunchId,
  SessionId,
  StateInstanceId,
  Target,
  TurnId,
} from "@swarm/protocol";
import { storageFail } from "../errors.js";
import { assertSqliteMigrationContract } from "../contracts.js";
import {
  checksumMigration,
  locateMigrationDirectory,
  type MigrationReceipt,
} from "../migrations.js";
import {
  assertArtifactDigest,
  assertProtocolId,
  canonicalTargetKey,
  parseFrozenDelivery,
  targetColumns,
} from "../protocol.js";
import {
  RuntimeJournalTransaction,
  type DriverEventReaderClaim,
  type DriverEventReaderClaimResult,
  type DriverEventReaderOrphanTakeover,
  type DriverEventReaderTakeoverResult,
} from "./runtime-journal.js";

type DeliveryBinding = {
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  turnId: TurnId;
  envelopeDigest: ArtifactDigest;
  receivedAt: string;
};

type LocalOperationInput = {
  operationId: string;
  operationKind: string;
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  turnId?: TurnId;
  sessionId?: SessionId;
  idempotencyKey: string;
  payloadDigest: ArtifactDigest;
  preparedAt: string;
  detailJson: string;
};

export type RecoveryEvidence = {
  kind: "operation" | "delivery" | "intent";
  identity: string;
  state: string;
  ambiguousBoundary: boolean;
  suppressInputReplay: boolean;
};

function changes(result: { changes: number | bigint }): number {
  return Number(result.changes);
}

function digestBytes(value: Uint8Array): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

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

function acquireJournalLock(lockPath: string): number {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const descriptor = openSync(lockPath, "wx", 0o600);
      try {
        writeFileSync(descriptor, `${process.pid}\n`, "utf8");
        return descriptor;
      } catch (error) {
        closeSync(descriptor);
        rmSync(lockPath, { force: true });
        throw error;
      }
    } catch (error) {
      const failure = error as NodeJS.ErrnoException;
      if (failure.code !== "EEXIST" || attempt > 0) {
        return storageFail("MACHINE_JOURNAL_LOCKED", error);
      }
      let ownerPid: number;
      try {
        ownerPid = Number(readFileSync(lockPath, "utf8").trim());
      } catch (readError) {
        return storageFail("MACHINE_JOURNAL_LOCKED", readError);
      }
      if (!Number.isSafeInteger(ownerPid) || ownerPid <= 0) {
        return storageFail("MACHINE_JOURNAL_LOCKED", "invalid lock owner");
      }
      try {
        process.kill(ownerPid, 0);
        return storageFail("MACHINE_JOURNAL_LOCKED", { ownerPid });
      } catch (probeError) {
        if ((probeError as NodeJS.ErrnoException).code !== "ESRCH") {
          return storageFail("MACHINE_JOURNAL_LOCKED", probeError);
        }
      }
      rmSync(lockPath, { force: true });
    }
  }
  return storageFail("MACHINE_JOURNAL_LOCKED");
}

const JOURNAL_INSTANCE_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz";

function mintJournalInstanceId(): string {
  const bytes = randomBytes(26);
  let suffix = "";
  for (let index = 0; index < 26; index += 1) {
    suffix += JOURNAL_INSTANCE_ALPHABET[(bytes[index] as number) % 32];
  }
  return `cmd_${suffix}`;
}

function openJournalDatabase(path: string): DatabaseSync {
  const descriptor = openSync(path, "a", 0o600);
  closeSync(descriptor);
  chmodSync(path, 0o600);
  return new DatabaseSync(path);
}

export class JournalTransaction extends RuntimeJournalTransaction {
  readonly #database: DatabaseSync;

  constructor(database: DatabaseSync) {
    super(database);
    this.#database = database;
  }

  prepareOperation(input: LocalOperationInput): void {
    assertProtocolId(input.operationId, "cmd");
    assertProtocolId(input.launchId, "lnc");
    assertProtocolId(input.stateInstanceId, "sti");
    if (input.turnId !== undefined) assertProtocolId(input.turnId, "trn");
    if (input.sessionId !== undefined) assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.payloadDigest);
    run(
      this.#database,
      `INSERT INTO local_operations (
        operation_id, operation_kind, launch_id, state_instance_id, turn_id,
        session_id, idempotency_key, payload_digest, state, prepared_at, detail_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?)`,
      [
        input.operationId,
        input.operationKind,
        input.launchId,
        input.stateInstanceId,
        input.turnId ?? null,
        input.sessionId ?? null,
        input.idempotencyKey,
        input.payloadDigest,
        input.preparedAt,
        input.detailJson,
      ],
    );
  }

  recordDelivery(envelopeBytes: Uint8Array, binding: DeliveryBinding): DeliveryEnvelope {
    const envelope = parseFrozenDelivery(envelopeBytes);
    assertProtocolId(binding.launchId, "lnc");
    assertProtocolId(binding.stateInstanceId, "sti");
    assertProtocolId(binding.sessionId, "ses");
    assertProtocolId(binding.turnId, "trn");
    assertArtifactDigest(binding.envelopeDigest);
    const target = targetColumns(envelope.target);
    const targetKey = canonicalTargetKey(envelope.target);
    if (digestBytes(canonicalProtocolJson(envelope)) !== binding.envelopeDigest) {
      storageFail("IDEMPOTENCY_CONFLICT", envelope.deliveryId);
    }
    if (
      envelope.expectedLaunchId !== undefined &&
      envelope.expectedLaunchId !== binding.launchId
    ) {
      storageFail("STALE_FENCE", envelope.deliveryId);
    }
    if (envelope.replayOf !== undefined) {
      const prior = one<{
        attempt: number;
        message_id: string;
        target_key: string;
        server_seq: number;
        agent_id: string;
        producer_fact_id: string;
      }>(
        this.#database,
        `SELECT attempt, message_id, target_key, server_seq, agent_id, producer_fact_id
         FROM pending_deliveries WHERE delivery_id = ?`,
        [envelope.replayOf],
      );
      if (
        prior === undefined ||
        Number(prior.attempt) !== envelope.attempt - 1 ||
        prior.message_id !== envelope.messageId ||
        prior.target_key !== targetKey ||
        Number(prior.server_seq) !== envelope.serverSeq ||
        prior.agent_id !== envelope.agentId ||
        prior.producer_fact_id !== envelope.producerFactId
      ) {
        storageFail("INVALID_STATE_TRANSITION", envelope.deliveryId);
      }
    }
    run(
      this.#database,
      `INSERT INTO pending_deliveries (
        delivery_id, attempt, replay_of, message_id, producer_fact_id, agent_id,
        machine_id, expected_launch_id, envelope_digest, target_key, target_kind,
        target_id, thread_root_message_id, launch_id, state_instance_id,
        session_id, turn_id, server_seq, received_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        envelope.deliveryId,
        envelope.attempt,
        envelope.replayOf ?? null,
        envelope.messageId,
        envelope.producerFactId,
        envelope.agentId,
        envelope.machineId,
        envelope.expectedLaunchId ?? null,
        binding.envelopeDigest,
        targetKey,
        target.kind,
        target.ownerId,
        target.threadRootMessageId,
        binding.launchId,
        binding.stateInstanceId,
        binding.sessionId,
        binding.turnId,
        envelope.serverSeq,
        binding.receivedAt,
      ],
    );
    return envelope;
  }

  markInputWritten(deliveryId: string, occurredAt: string, detailDigest: ArtifactDigest): void {
    assertProtocolId(deliveryId, "dlv");
    assertArtifactDigest(detailDigest);
    const result = run(
      this.#database,
      `UPDATE pending_deliveries
       SET input_written_at = ?
       WHERE delivery_id = ? AND input_written_at IS NULL
         AND model_visible_at IS NULL AND canceled_at IS NULL`,
      [occurredAt, deliveryId],
    );
    if (changes(result) !== 1) storageFail("INVALID_STATE_TRANSITION", deliveryId);
    this.#appendDeliveryReceipt(deliveryId, "input_written", occurredAt, detailDigest);
  }

  markModelVisible(deliveryId: string, occurredAt: string, detailDigest: ArtifactDigest): void {
    assertProtocolId(deliveryId, "dlv");
    assertArtifactDigest(detailDigest);
    const result = run(
      this.#database,
      `UPDATE pending_deliveries
       SET model_visible_at = ?
       WHERE delivery_id = ? AND input_written_at IS NOT NULL
         AND model_visible_at IS NULL AND canceled_at IS NULL`,
      [occurredAt, deliveryId],
    );
    if (changes(result) !== 1) storageFail("INVALID_STATE_TRANSITION", deliveryId);
    this.#appendDeliveryReceipt(
      deliveryId,
      "model_visible",
      occurredAt,
      detailDigest,
    );
  }

  markAckIntent(deliveryId: string, occurredAt: string, detailDigest: ArtifactDigest): void {
    assertProtocolId(deliveryId, "dlv");
    const result = run(
      this.#database,
      `UPDATE pending_deliveries SET ack_intent_at = ?
       WHERE delivery_id = ? AND model_visible_at IS NOT NULL
         AND ack_intent_at IS NULL AND canceled_at IS NULL`,
      [occurredAt, deliveryId],
    );
    if (changes(result) !== 1) storageFail("INVALID_STATE_TRANSITION", deliveryId);
    this.#appendDeliveryReceipt(deliveryId, "ack_intent", occurredAt, detailDigest);
  }

  markConsumed(deliveryId: string, occurredAt: string, detailDigest: ArtifactDigest): void {
    assertProtocolId(deliveryId, "dlv");
    const result = run(
      this.#database,
      `UPDATE pending_deliveries SET consumed_at = ?
       WHERE delivery_id = ? AND ack_intent_at IS NOT NULL
         AND consumed_at IS NULL AND canceled_at IS NULL`,
      [occurredAt, deliveryId],
    );
    if (changes(result) !== 1) storageFail("INVALID_STATE_TRANSITION", deliveryId);
    this.#appendDeliveryReceipt(deliveryId, "consumed", occurredAt, detailDigest);
  }

  prepareIntent(input: {
    intentId: string;
    commandKind: string;
    requestId: string;
    payloadDigest: ArtifactDigest;
    preparedAt: string;
  }): void {
    assertProtocolId(input.intentId, "cmd");
    const prefix = input.requestId.startsWith("cmd_") ? "cmd" : "fac";
    assertProtocolId(input.requestId, prefix);
    assertArtifactDigest(input.payloadDigest);
    run(
      this.#database,
      `INSERT INTO outbound_intents (
        intent_id, command_kind, request_id, payload_digest, state, prepared_at
      ) VALUES (?, ?, ?, ?, 'prepared', ?)`,
      [input.intentId, input.commandKind, input.requestId, input.payloadDigest, input.preparedAt],
    );
  }

  confirmIntent(input: {
    intentId: string;
    producerFactId: string;
    receiptId: string;
    confirmedAt: string;
  }): void {
    assertProtocolId(input.intentId, "cmd");
    assertProtocolId(input.producerFactId, "fac");
    assertProtocolId(input.receiptId, "rcp");
    const result = run(
      this.#database,
      `UPDATE outbound_intents
       SET state = 'server_confirmed', server_producer_fact_id = ?,
           server_receipt_id = ?, confirmed_at = ?
       WHERE intent_id = ? AND state IN ('prepared', 'sent')`,
      [input.producerFactId, input.receiptId, input.confirmedAt, input.intentId],
    );
    if (changes(result) !== 1) storageFail("INVALID_STATE_TRANSITION", input.intentId);
  }

  cancelIntent(intentId: string, canceledAt: string): void {
    assertProtocolId(intentId, "cmd");
    const result = run(
      this.#database,
      `UPDATE outbound_intents SET state = 'canceled', canceled_at = ?
       WHERE intent_id = ? AND state <> 'server_confirmed' AND state <> 'canceled'`,
      [canceledAt, intentId],
    );
    if (changes(result) !== 1) storageFail("INVALID_STATE_TRANSITION", intentId);
  }

  #appendDeliveryReceipt(
    deliveryId: string,
    kind: string,
    occurredAt: string,
    detailDigest: ArtifactDigest,
  ): number {
    assertArtifactDigest(detailDigest);
    const delivery = one<{ launch_id: string; state_instance_id: string }>(
      this.#database,
      "SELECT launch_id, state_instance_id FROM pending_deliveries WHERE delivery_id = ?",
      [deliveryId],
    );
    if (delivery === undefined) storageFail("INVALID_STATE_TRANSITION", deliveryId);
    const result = run(
      this.#database,
      `INSERT INTO local_receipts (
        delivery_id, boundary_kind, launch_id, state_instance_id, occurred_at,
        detail_digest
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        deliveryId,
        kind,
        delivery.launch_id,
        delivery.state_instance_id,
        occurredAt,
        detailDigest,
      ],
    );
    return Number(result.lastInsertRowid);
  }
}

export class DaemonJournal {
  readonly #path: string;
  readonly #lockPath: string;
  #database!: DatabaseSync;
  #lockDescriptor!: number;
  #closed = false;
  // Hidden per-open journal generation: minted after the machine lock is won,
  // never caller-supplied, never exposed as reader authority, and synchronously
  // invalidated before SQLite close or lock release on every close/reset path.
  #journalInstanceId: string | null = null;

  private constructor(path: string) {
    this.#path = path;
    this.#lockPath = `${path}.lock`;
    this.#lockDescriptor = acquireJournalLock(this.#lockPath);
    try {
      this.#database = openJournalDatabase(path);
      this.#configure();
      this.#journalInstanceId = mintJournalInstanceId();
    } catch (error) {
      this.#journalInstanceId = null;
      try {
        this.#database?.close();
      } catch {
        // The original open/configuration error is authoritative.
      }
      closeSync(this.#lockDescriptor);
      rmSync(this.#lockPath, { force: true });
      throw error;
    }
  }

  static open(path: string): DaemonJournal {
    return new DaemonJournal(path);
  }

  migrate(): MigrationReceipt[] {
    this.#assertOpen();
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS journal_migrations (
        version TEXT PRIMARY KEY,
        checksum TEXT NOT NULL CHECK (
          length(checksum) = 64 AND checksum NOT GLOB '*[^0-9a-f]*'
        ),
        applied_at TEXT NOT NULL
      ) STRICT;
    `);
    const directory = locateMigrationDirectory("sqlite", import.meta.url);
    const names = readdirSync(directory)
      .filter((name) => /^\d{4}_[a-z0-9_]+\.up\.sql$/u.test(name))
      .sort();
    const receipts: MigrationReceipt[] = [];
    for (const name of names) {
      const version = name.slice(0, 4);
      const sql = readFileSync(new URL(name, directory), "utf8");
      assertSqliteMigrationContract(sql, version);
      const checksum = checksumMigration(sql);
      const existing = one<{ checksum: string }>(
        this.#database,
        "SELECT checksum FROM journal_migrations WHERE version = ?",
        [version],
      );
      if (existing !== undefined) {
        if (existing.checksum !== checksum) {
          storageFail("MIGRATION_CHECKSUM_MISMATCH", version);
        }
        receipts.push({ version, checksum, applied: false });
        continue;
      }
      this.transaction(() => {
        this.#database.exec(sql);
        run(
          this.#database,
          "INSERT INTO journal_migrations(version, checksum, applied_at) VALUES (?, ?, ?)",
          [version, checksum, new Date().toISOString()],
        );
      });
      receipts.push({ version, checksum, applied: true });
    }
    return receipts;
  }

  transaction<T>(body: (transaction: JournalTransaction) => T): T {
    this.#assertOpen();
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = body(new JournalTransaction(this.#database));
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      this.#database.exec("ROLLBACK");
      throw error;
    }
  }

  listRecoveryEvidence(): RecoveryEvidence[] {
    this.#assertOpen();
    const evidence: RecoveryEvidence[] = [];
    for (const row of all<{ operation_id: string; state: string }>(
      this.#database,
      "SELECT operation_id, state FROM local_operations WHERE state NOT IN ('terminal', 'canceled') ORDER BY prepared_at",
    )) {
      evidence.push({
        kind: "operation",
        identity: row.operation_id,
        state: row.state,
        ambiguousBoundary: row.state === "effect_started",
        suppressInputReplay: row.state === "effect_observed",
      });
    }
    for (const row of all<{
      delivery_id: string;
      input_written_at: string | null;
      model_visible_at: string | null;
    }>(
      this.#database,
      `SELECT delivery_id, input_written_at, model_visible_at
       FROM pending_deliveries
       WHERE consumed_at IS NULL AND canceled_at IS NULL ORDER BY received_at`,
    )) {
      evidence.push({
        kind: "delivery",
        identity: row.delivery_id,
        state:
          row.model_visible_at !== null
            ? "model_visible"
            : row.input_written_at !== null
              ? "input_written"
              : "received",
        ambiguousBoundary: row.input_written_at !== null && row.model_visible_at === null,
        suppressInputReplay: row.model_visible_at !== null,
      });
    }
    for (const row of all<{ intent_id: string; state: string }>(
      this.#database,
      "SELECT intent_id, state FROM outbound_intents WHERE state NOT IN ('server_confirmed', 'canceled') ORDER BY prepared_at",
    )) {
      evidence.push({
        kind: "intent",
        identity: row.intent_id,
        state: row.state,
        ambiguousBoundary: row.state === "sent",
        suppressInputReplay: true,
      });
    }
    return evidence;
  }

  checkpoint(target: Target, sessionId: SessionId): { sequence: number; deliveryId: string } | undefined {
    this.#assertOpen();
    assertProtocolId(sessionId, "ses");
    const key = canonicalTargetKey(target);
    const columns = targetColumns(target);
    const result = one<{
      target_kind: string;
      target_id: string;
      thread_root_message_id: string | null;
      highest_model_visible_server_seq: number;
      last_delivery_id: string;
    }>(
      this.#database,
      `SELECT target_kind, target_id, thread_root_message_id,
              highest_model_visible_server_seq, last_delivery_id
       FROM visibility_checkpoints WHERE session_id = ? AND target_key = ?`,
      [sessionId, key],
    );
    if (result === undefined) return undefined;
    if (
      result.target_kind !== columns.kind ||
      result.target_id !== columns.ownerId ||
      result.thread_root_message_id !== columns.threadRootMessageId
    ) {
      storageFail("MIGRATION_CHECKSUM_MISMATCH", "canonical target key mismatch");
    }
    return {
      sequence: Number(result.highest_model_visible_server_seq),
      deliveryId: result.last_delivery_id,
    };
  }

  resetForTests(): MigrationReceipt[] {
    this.#assertOpen();
    if (process.env.NODE_ENV !== "test" || !basename(this.#path).startsWith("swarm-storage-test-")) {
      storageFail("INVALID_DATABASE_TARGET", this.#path);
    }
    this.#journalInstanceId = null;
    this.#database.close();
    this.#closed = true;
    closeSync(this.#lockDescriptor);
    rmSync(this.#lockPath, { force: true });
    rmSync(this.#path, { force: true });
    rmSync(`${this.#path}-wal`, { force: true });
    rmSync(`${this.#path}-shm`, { force: true });
    this.#lockDescriptor = acquireJournalLock(this.#lockPath);
    this.#database = openJournalDatabase(this.#path);
    this.#closed = false;
    this.#configure();
    this.#journalInstanceId = mintJournalInstanceId();
    return this.migrate();
  }

  close(): void {
    if (this.#closed) return;
    this.#journalInstanceId = null;
    try {
      this.#database.close();
    } finally {
      closeSync(this.#lockDescriptor);
      rmSync(this.#lockPath, { force: true });
      this.#closed = true;
    }
  }

  /**
   * Direct lock-owning claim surface. Every method first proves the journal is
   * open, the retained lock descriptor is still the acquired on-disk lock, and
   * the hidden per-open journal generation is live; a stale method reference
   * after close/reset fails the existing closed-journal error and can never
   * reuse the generation.
   */
  claimDriverEventReader(input: DriverEventReaderClaim): DriverEventReaderClaimResult {
    const journalInstanceId = this.#requireDirectClaimReceiver();
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.ownerToken);
    if (input.mode !== "start" && input.mode !== "resume") {
      storageFail("DRIVER_EVENT_READER_CONFLICT", input.stateInstanceId);
    }
    return this.transaction(() => {
      const existing = one<{
        session_id: string;
        next_ordinal: number;
        last_event_digest: string | null;
        reader_owner_token: string | null;
        reader_epoch: number;
        reader_journal_instance_id: string | null;
      }>(
        this.#database,
        `SELECT session_id, next_ordinal, last_event_digest, reader_owner_token,
                reader_epoch, reader_journal_instance_id
         FROM driver_event_cursor WHERE state_instance_id = ?`,
        [input.stateInstanceId],
      );
      if (existing === undefined) {
        if (input.mode !== "start") {
          storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
        }
        run(
          this.#database,
          `INSERT INTO driver_event_cursor (
             state_instance_id, session_id, next_ordinal, reader_owner_token,
             reader_epoch, reader_journal_instance_id, updated_at
           ) VALUES (?, ?, 0, ?, 1, ?, ?)`,
          [
            input.stateInstanceId,
            input.sessionId,
            input.ownerToken,
            journalInstanceId,
            input.claimedAt,
          ],
        );
        return { readerEpoch: 1, nextOrdinal: 0, lastEventDigest: null };
      }
      if (existing.session_id !== input.sessionId) {
        storageFail("DRIVER_EVENT_READER_CONFLICT", input.stateInstanceId);
      }
      if (input.mode !== "resume") {
        storageFail("DRIVER_EVENT_READER_CONFLICT", input.stateInstanceId);
      }
      if (existing.reader_owner_token !== null) {
        if (existing.reader_journal_instance_id === null) {
          // Upgraded pre-0003 row with an owner but no journal generation: it is
          // not guessed stale and cannot be resumed or taken over normally.
          storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
        }
        storageFail("DRIVER_RESUME_OVERLAP", input.stateInstanceId);
      }
      if (existing.reader_journal_instance_id !== null) {
        // Pair invariant broken out-of-band: owner null requires instance null.
        storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
      }
      const nextEpoch = Number(existing.reader_epoch) + 1;
      const result = run(
        this.#database,
        `UPDATE driver_event_cursor
         SET reader_owner_token = ?, reader_epoch = ?,
             reader_journal_instance_id = ?, updated_at = ?
         WHERE state_instance_id = ? AND session_id = ?
           AND reader_owner_token IS NULL AND reader_journal_instance_id IS NULL
           AND reader_epoch = ?`,
        [
          input.ownerToken,
          nextEpoch,
          journalInstanceId,
          input.claimedAt,
          input.stateInstanceId,
          input.sessionId,
          existing.reader_epoch,
        ],
      );
      if (changes(result) !== 1) {
        storageFail("DRIVER_RESUME_OVERLAP", input.stateInstanceId);
      }
      return {
        readerEpoch: nextEpoch,
        nextOrdinal: Number(existing.next_ordinal),
        lastEventDigest: (existing.last_event_digest ?? null) as DriverEventReaderClaimResult["lastEventDigest"],
      };
    });
  }

  claimOrphanedDriverEventReader(
    input: DriverEventReaderOrphanTakeover,
  ): DriverEventReaderTakeoverResult {
    const journalInstanceId = this.#requireDirectClaimReceiver();
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.ownerToken);
    return this.transaction(() => {
      const existing = one<{
        session_id: string;
        next_ordinal: number;
        last_event_digest: string | null;
        reader_owner_token: string | null;
        reader_epoch: number;
        reader_journal_instance_id: string | null;
      }>(
        this.#database,
        `SELECT session_id, next_ordinal, last_event_digest, reader_owner_token,
                reader_epoch, reader_journal_instance_id
         FROM driver_event_cursor WHERE state_instance_id = ?`,
        [input.stateInstanceId],
      );
      if (existing === undefined || existing.session_id !== input.sessionId) {
        storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
      }
      if (
        existing.reader_owner_token === null ||
        existing.reader_journal_instance_id === null
      ) {
        // No orphan: either nothing is claimed (normal resume path) or the row
        // is an unsupported pre-0003 owned row without a journal generation.
        storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
      }
      if (existing.reader_journal_instance_id === journalInstanceId) {
        // The current process cannot steal its own live reader.
        storageFail("DRIVER_RESUME_OVERLAP", input.stateInstanceId);
      }
      const nextEpoch = Number(existing.reader_epoch) + 1;
      const result = run(
        this.#database,
        `UPDATE driver_event_cursor
         SET reader_owner_token = ?, reader_epoch = ?,
             reader_journal_instance_id = ?, updated_at = ?
         WHERE state_instance_id = ? AND session_id = ?
           AND reader_owner_token = ? AND reader_epoch = ?
           AND reader_journal_instance_id = ?`,
        [
          input.ownerToken,
          nextEpoch,
          journalInstanceId,
          input.claimedAt,
          input.stateInstanceId,
          input.sessionId,
          existing.reader_owner_token,
          existing.reader_epoch,
          existing.reader_journal_instance_id,
        ],
      );
      if (changes(result) !== 1) {
        storageFail("DRIVER_RESUME_OVERLAP", input.stateInstanceId);
      }
      return {
        readerEpoch: nextEpoch,
        nextOrdinal: Number(existing.next_ordinal),
        lastEventDigest: (existing.last_event_digest ?? null) as DriverEventReaderTakeoverResult["lastEventDigest"],
        supersededOwnerToken: existing.reader_owner_token as DriverEventReaderTakeoverResult["supersededOwnerToken"],
        supersededReaderEpoch: Number(existing.reader_epoch),
      };
    });
  }

  releaseDriverEventReader(input: {
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    readerEpoch: number;
    releasedAt: string;
  }): void {
    this.#requireDirectClaimReceiver();
    assertProtocolId(input.stateInstanceId, "sti");
    assertProtocolId(input.sessionId, "ses");
    assertArtifactDigest(input.ownerToken);
    if (!Number.isSafeInteger(input.readerEpoch) || input.readerEpoch < 1) {
      storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
    }
    this.transaction(() => {
      const result = run(
        this.#database,
        `UPDATE driver_event_cursor
         SET reader_owner_token = NULL, reader_journal_instance_id = NULL,
             updated_at = ?
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
      if (changes(result) !== 1) {
        storageFail("DRIVER_EVENT_FENCE_MISMATCH", input.stateInstanceId);
      }
    });
  }

  #requireDirectClaimReceiver(): string {
    this.#assertOpen();
    if (this.#journalInstanceId === null) {
      storageFail("JOURNAL_LOCKED", "journal generation is invalidated");
    }
    let descriptorInode: number | bigint;
    let lockInode: number | bigint;
    try {
      descriptorInode = fstatSync(this.#lockDescriptor).ino;
      lockInode = statSync(this.#lockPath).ino;
    } catch (error) {
      return storageFail("JOURNAL_LOCKED", error);
    }
    if (descriptorInode !== lockInode) {
      storageFail("JOURNAL_LOCKED", "machine lock descriptor superseded");
    }
    return this.#journalInstanceId;
  }

  #configure(): void {
    this.#database.exec("PRAGMA journal_mode=WAL");
    this.#database.exec("PRAGMA foreign_keys=ON");
    this.#database.exec("PRAGMA synchronous=FULL");
    this.#database.exec("PRAGMA busy_timeout=5000");
  }

  #assertOpen(): void {
    if (this.#closed) storageFail("JOURNAL_LOCKED", "journal is closed");
  }
}
