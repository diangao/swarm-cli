import {
  closeSync,
  constants,
  existsSync,
  fchmodSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  linkSync,
  mkdirSync,
  openSync,
  readSync,
  realpathSync,
  unlinkSync,
  writeSync,
} from "node:fs";
import { execFileSync } from "node:child_process";
import { join, relative, resolve, sep } from "node:path";
import { homedir } from "node:os";
import {
  createCipheriv,
  createDecipheriv,
  randomBytes,
  randomUUID,
} from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import {
  canonicalProtocolJson,
  parseNormalizedDriverEvent,
  type ArtifactDigest,
  type CommandId,
  type LaunchId,
  type ProtocolVersion,
} from "@swarm/protocol";
import {
  DriverRetentionError,
  FiniteDriverRetainedReplay,
  driverArtifactDigest,
  retainedEventAad,
  retainedEventDigest,
  retainedRecordDigest,
  type DriverDurableAck,
  type DriverEventObservation,
  type DriverPrivateClaimAttempt,
  type DriverPrivateEventRetentionPort,
  type DriverRetainedEventSource,
  type DriverRetainedReplay,
  type DriverRetainedReplayExpectation,
  type PreparedRetainedDriverEventEnvelope,
  type RetainedAppendResult,
  type RetainedDriverEventLease,
  type RetainedDriverEventRecord,
} from "@swarm/drivers";

const KEY_MAGIC = Buffer.from("SWRLWK01", "ascii");
const KEY_BYTES = 100;
const FORMAT_VERSION = 1;
const KEY_GENERATION = 1;
const SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const PRIVATE_SCHEMA = `
CREATE TABLE IF NOT EXISTS private_driver_event_meta (
  launch_id TEXT NOT NULL,
  state_instance_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  protocol_version INTEGER NOT NULL CHECK (protocol_version >= 1),
  format_version INTEGER NOT NULL CHECK (format_version = 1),
  key_format_version INTEGER NOT NULL CHECK (key_format_version = 1),
  key_id TEXT NOT NULL,
  key_generation INTEGER NOT NULL CHECK (key_generation = 1),
  nonce_prefix BLOB NOT NULL CHECK (length(nonce_prefix) = 4),
  key_wrap_nonce BLOB NOT NULL CHECK (length(key_wrap_nonce) = 12),
  key_wrap_tag BLOB NOT NULL CHECK (length(key_wrap_tag) = 16),
  wrapped_data_key BLOB NOT NULL CHECK (length(wrapped_data_key) = 32),
  head_next_ordinal INTEGER NOT NULL CHECK (head_next_ordinal >= 0),
  head_record_digest TEXT,
  ack_next_ordinal INTEGER NOT NULL CHECK (ack_next_ordinal >= 0),
  ack_last_event_digest TEXT,
  ack_last_record_digest TEXT,
  reader_owner_token TEXT,
  reader_epoch INTEGER NOT NULL DEFAULT 0 CHECK (reader_epoch >= 0),
  active_claim_id TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (launch_id, state_instance_id, session_id),
  UNIQUE (state_instance_id),
  UNIQUE (launch_id, state_instance_id, session_id, protocol_version, key_generation),
  CHECK (length(launch_id)=30 AND substr(launch_id,1,4)='lnc_' AND substr(launch_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(state_instance_id)=30 AND substr(state_instance_id,1,4)='sti_' AND substr(state_instance_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(session_id)=30 AND substr(session_id,1,4)='ses_' AND substr(session_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(key_id)=30 AND substr(key_id,1,4)='cmd_' AND substr(key_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (reader_owner_token IS NULL OR (length(reader_owner_token)=71 AND substr(reader_owner_token,1,7)='sha256:' AND substr(reader_owner_token,8) NOT GLOB '*[^0-9a-f]*')),
  CHECK (active_claim_id IS NULL OR (length(active_claim_id)=30 AND substr(active_claim_id,1,4)='cmd_' AND substr(active_claim_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  CHECK (head_record_digest IS NULL OR (length(head_record_digest)=71 AND substr(head_record_digest,1,7)='sha256:' AND substr(head_record_digest,8) NOT GLOB '*[^0-9a-f]*')),
  CHECK (ack_last_event_digest IS NULL OR (length(ack_last_event_digest)=71 AND substr(ack_last_event_digest,1,7)='sha256:' AND substr(ack_last_event_digest,8) NOT GLOB '*[^0-9a-f]*')),
  CHECK (ack_last_record_digest IS NULL OR (length(ack_last_record_digest)=71 AND substr(ack_last_record_digest,1,7)='sha256:' AND substr(ack_last_record_digest,8) NOT GLOB '*[^0-9a-f]*')),
  CHECK (ack_next_ordinal <= head_next_ordinal),
  CHECK ((reader_owner_token IS NULL) = (active_claim_id IS NULL)),
  CHECK ((head_next_ordinal = 0) = (head_record_digest IS NULL)),
  CHECK ((ack_next_ordinal = 0) = (ack_last_event_digest IS NULL AND ack_last_record_digest IS NULL))
) STRICT;

CREATE TABLE IF NOT EXISTS private_driver_event_records (
  launch_id TEXT NOT NULL,
  state_instance_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  protocol_version INTEGER NOT NULL CHECK (protocol_version >= 1),
  key_generation INTEGER NOT NULL CHECK (key_generation = 1),
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  previous_record_digest TEXT,
  record_digest TEXT NOT NULL,
  event_digest TEXT NOT NULL,
  payload_cipher_digest TEXT NOT NULL,
  resolved_waiter_id TEXT NOT NULL,
  source_message_id TEXT NOT NULL,
  binding_digest TEXT NOT NULL,
  turn_id TEXT NOT NULL,
  event_kind TEXT NOT NULL CHECK (event_kind IN ('turn_started','input_written','model_visible','turn_boundary','assistant_reply','coordination_call','turn_completed')),
  nonce BLOB NOT NULL CHECK (length(nonce) = 12),
  auth_tag BLOB NOT NULL CHECK (length(auth_tag) = 16),
  ciphertext BLOB NOT NULL CHECK (length(ciphertext) > 0),
  appended_at TEXT NOT NULL,
  PRIMARY KEY (launch_id, state_instance_id, session_id, ordinal),
  UNIQUE (launch_id, record_digest),
  UNIQUE (launch_id, state_instance_id, session_id, key_generation, nonce),
  FOREIGN KEY (launch_id, state_instance_id, session_id, protocol_version, key_generation)
    REFERENCES private_driver_event_meta(launch_id, state_instance_id, session_id, protocol_version, key_generation),
  CHECK (length(launch_id)=30 AND substr(launch_id,1,4)='lnc_' AND substr(launch_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(state_instance_id)=30 AND substr(state_instance_id,1,4)='sti_' AND substr(state_instance_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(session_id)=30 AND substr(session_id,1,4)='ses_' AND substr(session_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(resolved_waiter_id)=30 AND substr(resolved_waiter_id,1,4)='cmd_' AND substr(resolved_waiter_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(source_message_id)=30 AND substr(source_message_id,1,4)='msg_' AND substr(source_message_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(turn_id)=30 AND substr(turn_id,1,4)='trn_' AND substr(turn_id,5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  CHECK (length(record_digest)=71 AND substr(record_digest,1,7)='sha256:' AND substr(record_digest,8) NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(event_digest)=71 AND substr(event_digest,1,7)='sha256:' AND substr(event_digest,8) NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(payload_cipher_digest)=71 AND substr(payload_cipher_digest,1,7)='sha256:' AND substr(payload_cipher_digest,8) NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(binding_digest)=71 AND substr(binding_digest,1,7)='sha256:' AND substr(binding_digest,8) NOT GLOB '*[^0-9a-f]*'),
  CHECK (previous_record_digest IS NULL OR (length(previous_record_digest)=71 AND substr(previous_record_digest,1,7)='sha256:' AND substr(previous_record_digest,8) NOT GLOB '*[^0-9a-f]*')),
  CHECK ((ordinal=0 AND previous_record_digest IS NULL) OR (ordinal>0 AND previous_record_digest IS NOT NULL))
) STRICT;
`;

type MetaRow = {
  launch_id: string;
  state_instance_id: string;
  session_id: string;
  protocol_version: number;
  format_version: number;
  key_format_version: number;
  key_id: string;
  key_generation: number;
  nonce_prefix: Uint8Array;
  key_wrap_nonce: Uint8Array;
  key_wrap_tag: Uint8Array;
  wrapped_data_key: Uint8Array;
  head_next_ordinal: number;
  head_record_digest: string | null;
  ack_next_ordinal: number;
  ack_last_event_digest: string | null;
  ack_last_record_digest: string | null;
  reader_owner_token: string | null;
  reader_epoch: number;
  active_claim_id: string | null;
};

type RecordRow = {
  launch_id: string;
  state_instance_id: string;
  session_id: string;
  protocol_version: number;
  key_generation: number;
  ordinal: number;
  previous_record_digest: string | null;
  record_digest: string;
  event_digest: string;
  payload_cipher_digest: string;
  resolved_waiter_id: string;
  source_message_id: string;
  binding_digest: string;
  turn_id: string;
  event_kind: RetainedDriverEventRecord["eventKind"];
  nonce: Uint8Array;
  auth_tag: Uint8Array;
  ciphertext: Uint8Array;
};

export type SqlitePrivateDriverEventRetentionOptions = {
  launchRoot: string;
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  sourceWorkspace?: string;
};

export class SqlitePrivateDriverEventRetention
implements DriverPrivateEventRetentionPort, DriverRetainedEventSource {
  readonly #database: DatabaseSync;
  readonly #launchRoot: string;
  readonly #protocolVersion: ProtocolVersion;
  readonly #launchId: LaunchId;
  readonly #keyPath: string;
  readonly #dataKeys = new Map<string, Buffer>();
  #closed = false;

  constructor(options: SqlitePrivateDriverEventRetentionOptions) {
    this.#protocolVersion = options.protocolVersion;
    this.#launchId = options.launchId;
    this.#launchRoot = preparePrivateRoot(options.launchRoot, options.sourceWorkspace);
    this.#keyPath = join(this.#launchRoot, "launch-key.v1");
    const databasePath = join(this.#launchRoot, "driver-events.sqlite");
    ensurePrivateDatabaseFile(databasePath);
    this.#database = new DatabaseSync(databasePath);
    this.#database.exec(
      "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA trusted_schema=OFF; PRAGMA busy_timeout=5000;",
    );
    this.#database.exec(PRIVATE_SCHEMA);
    chmodPrivateDatabaseFiles(databasePath);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const key of this.#dataKeys.values()) key.fill(0);
    this.#dataKeys.clear();
    this.#database.close();
  }

  async claim(input: DriverPrivateClaimAttempt): Promise<RetainedDriverEventLease> {
    this.#assertOpen();
    assertClaimShape(input, this.#protocolVersion, this.#launchId);
    let launchKey: Buffer | undefined;
    let dataKey: Buffer | undefined;
    try {
      let row = this.#meta(input.stateInstanceId);
      if (row === undefined) {
        if (input.processMode !== "start" || input.nextOrdinal !== 0 || input.lastEventDigest !== null) {
          throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
        }
        launchKey = this.#loadOrCreateLaunchKey();
        this.#beginImmediate();
        if (this.#meta(input.stateInstanceId) !== undefined) {
          throw new DriverRetentionError("DRIVER_RESUME_OVERLAP");
        }
        const inserted = this.#insertMeta(input, launchKey);
        row = inserted.row;
        dataKey = inserted.dataKey;
      } else {
        this.#beginImmediate();
        row = this.#requiredMeta(input.stateInstanceId);
        this.#assertStableMeta(row, input);
        row = this.#reconcileClaimWatermark(row, input);
        if (
          row.reader_owner_token === input.ownerToken
          && row.reader_epoch === input.readerEpoch
          && row.active_claim_id === input.claimAttemptId
        ) {
          // Unknown-result exact retry aliases the already committed private claim.
        } else if (row.reader_owner_token === null) {
          if (row.active_claim_id !== null || input.readerEpoch <= row.reader_epoch) {
            throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
          }
          this.#claimMeta(input, row.reader_epoch);
        } else if (input.readerEpoch > row.reader_epoch) {
          this.#claimMeta(input, row.reader_epoch);
        } else {
          throw new DriverRetentionError("DRIVER_RESUME_OVERLAP");
        }
        row = this.#requiredMeta(input.stateInstanceId);
      }
      this.#commit();
      if (dataKey === undefined) {
        // Existing sessions establish current owner/epoch authority before any
        // wrapping-key read or DEK unwrap.
        launchKey = this.#loadOrCreateLaunchKey();
        dataKey = unwrapDataKey(row, launchKey, input);
      }
      this.#replaceCachedKey(input, dataKey);
      dataKey = undefined;
      return {
        ...input,
        snapshotHeadNextOrdinal: row.head_next_ordinal,
      };
    } catch (error) {
      this.#rollbackIfActive();
      throw sanitizePrivateError(error);
    } finally {
      dataKey?.fill(0);
      launchKey?.fill(0);
    }
  }

  async releaseAttempt(input: DriverPrivateClaimAttempt): Promise<{ applied: boolean }> {
    this.#assertOpen();
    assertClaimShape(input, this.#protocolVersion, this.#launchId);
    this.#beginImmediate();
    try {
      const result = this.#database.prepare(
        `UPDATE private_driver_event_meta
         SET reader_owner_token=NULL, active_claim_id=NULL, updated_at=?
         WHERE launch_id=? AND state_instance_id=? AND session_id=?
           AND reader_owner_token=? AND reader_epoch=? AND active_claim_id=?`,
      ).run(
        new Date().toISOString(), input.launchId, input.stateInstanceId, input.sessionId,
        input.ownerToken, input.readerEpoch, input.claimAttemptId,
      );
      const applied = Number(result.changes) === 1;
      if (!applied) {
        const row = this.#meta(input.stateInstanceId);
        if (
          row !== undefined
          && (row.reader_owner_token !== null || row.active_claim_id !== null)
          && row.reader_epoch <= input.readerEpoch
        ) {
          throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
        }
      }
      this.#commit();
      this.#deleteCachedKey(input);
      return { applied };
    } catch (error) {
      this.#rollbackIfActive();
      throw sanitizePrivateError(error);
    }
  }

  prepareAppend(
    lease: RetainedDriverEventLease,
    observation: DriverEventObservation,
  ): PreparedRetainedDriverEventEnvelope {
    this.#assertLease(lease, true);
    if (lease.replayMode !== "live") {
      throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
    }
    const row = this.#requiredMeta(lease.stateInstanceId);
    const ordinal = row.head_next_ordinal;
    assertSafe(ordinal);
    if (this.#record(lease, ordinal) !== undefined) {
      throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
    }
    const key = this.#requiredCachedKey(lease);
    const nonce = Buffer.alloc(12);
    Buffer.from(row.nonce_prefix).copy(nonce, 0);
    nonce.writeBigUInt64BE(BigInt(ordinal), 4);
    const base = {
      protocolVersion: lease.protocolVersion,
      launchId: lease.launchId,
      stateInstanceId: lease.stateInstanceId,
      sessionId: lease.sessionId,
      keyGeneration: KEY_GENERATION,
      ordinal,
      previousRecordDigest: row.head_record_digest as ArtifactDigest | null,
      resolvedWaiterId: observation.resolvedWaiterId,
      sourceMessageId: observation.sourceMessageId,
      turnId: observation.event.turnId,
      bindingDigest: observation.bindingDigest,
      eventKind: observation.event.kind,
    } as const;
    const cipher = createCipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(canonicalProtocolJson(retainedEventAad(base)));
    const ciphertext = Buffer.concat([
      cipher.update(canonicalProtocolJson(observation.event)),
      cipher.final(),
    ]);
    const authTag = cipher.getAuthTag();
    const payloadCipherDigest = driverArtifactDigest(Buffer.concat([nonce, authTag, ciphertext]));
    const eventDigest = retainedEventDigest({ ...base, payloadCipherDigest });
    const recordDigest = retainedRecordDigest({ ...base, payloadCipherDigest, eventDigest });
    return {
      ...base,
      payloadCipherDigest,
      eventDigest,
      recordDigest,
      nonce,
      authTag,
      ciphertext,
    };
  }

  appendPrepared(
    lease: RetainedDriverEventLease,
    envelope: PreparedRetainedDriverEventEnvelope,
  ): RetainedAppendResult {
    this.#assertLease(lease, true);
    this.#assertEnvelopeAuthority(lease, envelope);
    this.#beginImmediate();
    try {
      const occupied = this.#record(lease, envelope.ordinal);
      if (occupied !== undefined) {
        if (!sameEnvelopeRow(occupied, envelope)) {
          throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
        }
        const record = this.#decryptRow(lease, occupied);
        this.#commit();
        return { applied: false, record };
      }
      const meta = this.#requiredMeta(lease.stateInstanceId);
      this.#assertClaimedMeta(meta, lease);
      if (
        meta.head_next_ordinal !== envelope.ordinal
        || meta.head_record_digest !== envelope.previousRecordDigest
      ) {
        throw new DriverRetentionError(
          meta.head_next_ordinal < envelope.ordinal
            ? "DRIVER_RETAINED_EVENT_GAP"
            : "DRIVER_RETAINED_EVENT_CONFLICT",
        );
      }
      this.#database.prepare(
        `INSERT INTO private_driver_event_records (
           launch_id,state_instance_id,session_id,protocol_version,key_generation,
           ordinal,previous_record_digest,record_digest,event_digest,payload_cipher_digest,
           resolved_waiter_id,source_message_id,binding_digest,turn_id,event_kind,
           nonce,auth_tag,ciphertext,appended_at
         ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      ).run(
        envelope.launchId, envelope.stateInstanceId, envelope.sessionId,
        envelope.protocolVersion, envelope.keyGeneration, envelope.ordinal,
        envelope.previousRecordDigest, envelope.recordDigest, envelope.eventDigest,
        envelope.payloadCipherDigest, envelope.resolvedWaiterId, envelope.sourceMessageId,
        envelope.bindingDigest, envelope.turnId, envelope.eventKind,
        envelope.nonce, envelope.authTag, envelope.ciphertext, new Date().toISOString(),
      );
      const update = this.#database.prepare(
        `UPDATE private_driver_event_meta
         SET head_next_ordinal=?, head_record_digest=?, updated_at=?
         WHERE launch_id=? AND state_instance_id=? AND session_id=?
           AND head_next_ordinal=? AND reader_owner_token=? AND reader_epoch=? AND active_claim_id=?`,
      ).run(
        envelope.ordinal + 1, envelope.recordDigest, new Date().toISOString(),
        lease.launchId, lease.stateInstanceId, lease.sessionId, envelope.ordinal,
        lease.ownerToken, lease.readerEpoch, lease.claimAttemptId,
      );
      if (Number(update.changes) !== 1) {
        throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
      }
      const row = this.#record(lease, envelope.ordinal);
      if (row === undefined) throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
      const record = this.#decryptRow(lease, row);
      this.#commit();
      return { applied: true, record };
    } catch (error) {
      this.#rollbackIfActive();
      throw sanitizePrivateError(error);
    }
  }

  read(lease: RetainedDriverEventLease, ordinal: number): RetainedDriverEventRecord | null {
    this.#assertLease(lease, false);
    assertSafe(ordinal);
    if (lease.replayMode === "retained_only" && ordinal >= lease.snapshotHeadNextOrdinal) return null;
    const meta = this.#requiredMeta(lease.stateInstanceId);
    if (ordinal < meta.ack_next_ordinal) return null;
    const row = this.#record(lease, ordinal);
    return row === undefined ? null : this.#decryptRow(lease, row);
  }

  acknowledgeCommitted(
    lease: RetainedDriverEventLease,
    ack: DriverDurableAck,
  ): { applied: boolean } {
    this.#assertLease(lease, false);
    if (ack.stateInstanceId !== lease.stateInstanceId || ack.sessionId !== lease.sessionId) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
    assertSafe(ack.nextOrdinal);
    if ((ack.nextOrdinal === 0) !== (ack.lastEventDigest === null)) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
    this.#beginImmediate();
    try {
      const meta = this.#requiredMeta(lease.stateInstanceId);
      this.#assertClaimedMeta(meta, lease);
      if (ack.nextOrdinal > meta.head_next_ordinal || ack.nextOrdinal < meta.ack_next_ordinal) {
        throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
      }
      if (ack.nextOrdinal === meta.ack_next_ordinal) {
        if (ack.lastEventDigest !== meta.ack_last_event_digest) {
          throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
        }
        this.#commit();
        return { applied: false };
      }
      const predecessor = this.#record(lease, ack.nextOrdinal - 1);
      if (predecessor === undefined || predecessor.event_digest !== ack.lastEventDigest) {
        throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
      }
      this.#database.prepare(
        `UPDATE private_driver_event_meta
         SET ack_next_ordinal=?, ack_last_event_digest=?, ack_last_record_digest=?, updated_at=?
         WHERE launch_id=? AND state_instance_id=? AND session_id=?
           AND reader_owner_token=? AND reader_epoch=? AND active_claim_id=?`,
      ).run(
        ack.nextOrdinal, ack.lastEventDigest, predecessor.record_digest, new Date().toISOString(),
        lease.launchId, lease.stateInstanceId, lease.sessionId,
        lease.ownerToken, lease.readerEpoch, lease.claimAttemptId,
      );
      this.#database.prepare(
        `DELETE FROM private_driver_event_records
         WHERE launch_id=? AND state_instance_id=? AND session_id=? AND ordinal < ?`,
      ).run(lease.launchId, lease.stateInstanceId, lease.sessionId, ack.nextOrdinal);
      this.#commit();
      return { applied: true };
    } catch (error) {
      this.#rollbackIfActive();
      throw sanitizePrivateError(error);
    }
  }

  async openReplay(
    input: DriverRetainedReplayExpectation & {
      claim: import("@swarm/drivers").DriverCompositeCursorClaimHandle;
    },
  ): Promise<DriverRetainedReplay> {
    const lease = input.claim.privateLease;
    this.#assertLease(lease, false);
    if (lease.replayMode !== "retained_only") {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
    const records: RetainedDriverEventRecord[] = [];
    for (let ordinal = lease.nextOrdinal; ordinal < lease.snapshotHeadNextOrdinal; ordinal += 1) {
      const record = this.read(lease, ordinal);
      if (record === null) throw new DriverRetentionError("DRIVER_RETAINED_EVENT_GAP");
      records.push(record);
    }
    return new FiniteDriverRetainedReplay(input.claim, records, input);
  }

  retireIfTerminal(input: {
    lease: RetainedDriverEventLease;
    storageTerminal: boolean;
  }): { applied: boolean } {
    this.#assertOpen();
    assertClaimShape(input.lease, this.#protocolVersion, this.#launchId);
    const meta = this.#requiredMeta(input.lease.stateInstanceId);
    this.#assertStableMeta(meta, input.lease);
    if (
      !input.storageTerminal
      || meta.ack_next_ordinal !== meta.head_next_ordinal
      || meta.reader_owner_token !== null
      || meta.active_claim_id !== null
      || meta.reader_epoch !== input.lease.readerEpoch
    ) return { applied: false };
    this.#beginImmediate();
    try {
      this.#database.prepare(
        "DELETE FROM private_driver_event_records WHERE launch_id=? AND state_instance_id=? AND session_id=?",
      ).run(input.lease.launchId, input.lease.stateInstanceId, input.lease.sessionId);
      this.#database.prepare(
        "DELETE FROM private_driver_event_meta WHERE launch_id=? AND state_instance_id=? AND session_id=?",
      ).run(input.lease.launchId, input.lease.stateInstanceId, input.lease.sessionId);
      const remaining = this.#database.prepare(
        "SELECT COUNT(*) AS count FROM private_driver_event_meta WHERE launch_id=?",
      ).get(input.lease.launchId) as { count: number };
      this.#commit();
      if (Number(remaining.count) === 0 && existsSync(this.#keyPath)) {
        unlinkSync(this.#keyPath);
        fsyncDirectory(this.#launchRoot);
      }
      return { applied: true };
    } catch (error) {
      this.#rollbackIfActive();
      throw sanitizePrivateError(error);
    }
  }

  #insertMeta(
    input: DriverPrivateClaimAttempt,
    launchKey: Buffer,
  ): { row: MetaRow; dataKey: Buffer } {
    const keyId = randomCommandId();
    const dataKey = randomBytes(32);
    const noncePrefix = randomBytes(4);
    const wrapNonce = randomBytes(12);
    const aad = keyWrapAad(input, keyId, noncePrefix);
    const cipher = createCipheriv("aes-256-gcm", launchKey, wrapNonce);
    cipher.setAAD(aad);
    const wrapped = Buffer.concat([cipher.update(dataKey), cipher.final()]);
    const tag = cipher.getAuthTag();
    this.#database.prepare(
      `INSERT INTO private_driver_event_meta (
         launch_id,state_instance_id,session_id,protocol_version,format_version,
         key_format_version,key_id,key_generation,nonce_prefix,key_wrap_nonce,key_wrap_tag,
         wrapped_data_key,head_next_ordinal,head_record_digest,ack_next_ordinal,
         ack_last_event_digest,ack_last_record_digest,reader_owner_token,reader_epoch,
         active_claim_id,updated_at
       ) VALUES (?,?,?,?,1,1,?,1,?,?,?,?,0,NULL,0,NULL,NULL,?,?,?,?)`,
    ).run(
      input.launchId, input.stateInstanceId, input.sessionId, input.protocolVersion,
      keyId, noncePrefix, wrapNonce, tag, wrapped, input.ownerToken,
      input.readerEpoch, input.claimAttemptId, new Date().toISOString(),
    );
    return { row: this.#requiredMeta(input.stateInstanceId), dataKey };
  }

  #claimMeta(input: DriverPrivateClaimAttempt, previousEpoch: number): void {
    const result = this.#database.prepare(
      `UPDATE private_driver_event_meta
       SET reader_owner_token=?, reader_epoch=?, active_claim_id=?, updated_at=?
       WHERE launch_id=? AND state_instance_id=? AND session_id=? AND reader_epoch=?`,
    ).run(
      input.ownerToken, input.readerEpoch, input.claimAttemptId, new Date().toISOString(),
      input.launchId, input.stateInstanceId, input.sessionId, previousEpoch,
    );
    if (Number(result.changes) !== 1) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
  }

  #reconcileClaimWatermark(
    row: MetaRow,
    input: DriverPrivateClaimAttempt,
  ): MetaRow {
    if (input.nextOrdinal === row.ack_next_ordinal) {
      if (input.lastEventDigest !== row.ack_last_event_digest) {
        throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
      }
      return row;
    }
    if (input.nextOrdinal < row.ack_next_ordinal || input.nextOrdinal > row.head_next_ordinal) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }

    let previousRecordDigest = row.ack_last_record_digest;
    let predecessor: RecordRow | undefined;
    for (let ordinal = row.ack_next_ordinal; ordinal < input.nextOrdinal; ordinal += 1) {
      const record = this.#record(input, ordinal);
      if (record === undefined || record.previous_record_digest !== previousRecordDigest) {
        throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
      }
      this.#assertPrefixRecordIntegrity(record, input, ordinal);
      previousRecordDigest = record.record_digest;
      predecessor = record;
    }
    if (predecessor === undefined || predecessor.event_digest !== input.lastEventDigest) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }

    const result = this.#database.prepare(
      `UPDATE private_driver_event_meta
       SET ack_next_ordinal=?, ack_last_event_digest=?, ack_last_record_digest=?, updated_at=?
       WHERE launch_id=? AND state_instance_id=? AND session_id=?
         AND ack_next_ordinal=? AND ack_last_event_digest IS ? AND ack_last_record_digest IS ?`,
    ).run(
      input.nextOrdinal, input.lastEventDigest, predecessor.record_digest, new Date().toISOString(),
      input.launchId, input.stateInstanceId, input.sessionId,
      row.ack_next_ordinal, row.ack_last_event_digest, row.ack_last_record_digest,
    );
    if (Number(result.changes) !== 1) {
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
    this.#database.prepare(
      `DELETE FROM private_driver_event_records
       WHERE launch_id=? AND state_instance_id=? AND session_id=? AND ordinal < ?`,
    ).run(input.launchId, input.stateInstanceId, input.sessionId, input.nextOrdinal);
    return this.#requiredMeta(input.stateInstanceId);
  }

  #assertPrefixRecordIntegrity(
    row: RecordRow,
    input: DriverPrivateClaimAttempt,
    ordinal: number,
  ): void {
    if (
      row.launch_id !== input.launchId
      || row.state_instance_id !== input.stateInstanceId
      || row.session_id !== input.sessionId
      || row.protocol_version !== input.protocolVersion
      || row.key_generation !== KEY_GENERATION
      || row.ordinal !== ordinal
    ) this.#corrupt();
    const base = {
      protocolVersion: row.protocol_version as ProtocolVersion,
      launchId: row.launch_id as LaunchId,
      stateInstanceId: row.state_instance_id as RetainedDriverEventLease["stateInstanceId"],
      sessionId: row.session_id as RetainedDriverEventLease["sessionId"],
      keyGeneration: row.key_generation,
      ordinal: row.ordinal,
      previousRecordDigest: row.previous_record_digest as ArtifactDigest | null,
      resolvedWaiterId: row.resolved_waiter_id as CommandId,
      sourceMessageId: row.source_message_id as RetainedDriverEventRecord["sourceMessageId"],
      turnId: row.turn_id as RetainedDriverEventRecord["turnId"],
      bindingDigest: row.binding_digest as ArtifactDigest,
      eventKind: row.event_kind,
    } as const;
    const payloadCipherDigest = driverArtifactDigest(Buffer.concat([
      Buffer.from(row.nonce), Buffer.from(row.auth_tag), Buffer.from(row.ciphertext),
    ]));
    if (payloadCipherDigest !== row.payload_cipher_digest) this.#corrupt();
    const eventDigest = retainedEventDigest({ ...base, payloadCipherDigest });
    if (eventDigest !== row.event_digest) this.#corrupt();
    const recordDigest = retainedRecordDigest({ ...base, payloadCipherDigest, eventDigest });
    if (recordDigest !== row.record_digest) this.#corrupt();
  }

  #assertStableMeta(row: MetaRow, input: DriverPrivateClaimAttempt): void {
    if (
      row.launch_id !== input.launchId
      || row.state_instance_id !== input.stateInstanceId
      || row.session_id !== input.sessionId
      || row.protocol_version !== input.protocolVersion
      || row.format_version !== FORMAT_VERSION
      || row.key_format_version !== FORMAT_VERSION
      || row.key_generation !== KEY_GENERATION
    ) throw new DriverRetentionError("DRIVER_RETAINED_VERSION_UNSUPPORTED");
    assertSafe(row.head_next_ordinal);
    assertSafe(row.ack_next_ordinal);
  }

  #assertLease(lease: RetainedDriverEventLease, requireCurrentHead: boolean): void {
    this.#assertOpen();
    assertClaimShape(lease, this.#protocolVersion, this.#launchId);
    assertSafe(lease.snapshotHeadNextOrdinal);
    const meta = this.#requiredMeta(lease.stateInstanceId);
    this.#assertStableMeta(meta, lease);
    this.#assertClaimedMeta(meta, lease);
    if (requireCurrentHead && lease.replayMode !== "live") {
      throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
    }
  }

  #assertClaimedMeta(meta: MetaRow, lease: DriverPrivateClaimAttempt): void {
    if (
      meta.reader_owner_token !== lease.ownerToken
      || meta.reader_epoch !== lease.readerEpoch
      || meta.active_claim_id !== lease.claimAttemptId
    ) throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
  }

  #assertEnvelopeAuthority(
    lease: RetainedDriverEventLease,
    envelope: PreparedRetainedDriverEventEnvelope,
  ): void {
    if (
      envelope.protocolVersion !== lease.protocolVersion
      || envelope.launchId !== lease.launchId
      || envelope.stateInstanceId !== lease.stateInstanceId
      || envelope.sessionId !== lease.sessionId
      || envelope.keyGeneration !== KEY_GENERATION
    ) throw new DriverRetentionError("DRIVER_RETAINED_EVENT_CONFLICT");
    assertSafe(envelope.ordinal);
  }

  #decryptRow(lease: RetainedDriverEventLease, row: RecordRow): RetainedDriverEventRecord {
    this.#assertRecordAuthority(lease, row);
    const base = {
      protocolVersion: row.protocol_version as ProtocolVersion,
      launchId: row.launch_id as LaunchId,
      stateInstanceId: row.state_instance_id as RetainedDriverEventLease["stateInstanceId"],
      sessionId: row.session_id as RetainedDriverEventLease["sessionId"],
      keyGeneration: row.key_generation,
      ordinal: row.ordinal,
      previousRecordDigest: row.previous_record_digest as ArtifactDigest | null,
      resolvedWaiterId: row.resolved_waiter_id as CommandId,
      sourceMessageId: row.source_message_id as RetainedDriverEventRecord["sourceMessageId"],
      turnId: row.turn_id as RetainedDriverEventRecord["turnId"],
      bindingDigest: row.binding_digest as ArtifactDigest,
      eventKind: row.event_kind,
    } as const;
    const payloadCipherDigest = driverArtifactDigest(Buffer.concat([
      Buffer.from(row.nonce), Buffer.from(row.auth_tag), Buffer.from(row.ciphertext),
    ]));
    if (payloadCipherDigest !== row.payload_cipher_digest) this.#corrupt();
    const eventDigest = retainedEventDigest({ ...base, payloadCipherDigest });
    if (eventDigest !== row.event_digest) this.#corrupt();
    const recordDigest = retainedRecordDigest({ ...base, payloadCipherDigest, eventDigest });
    if (recordDigest !== row.record_digest) this.#corrupt();
    const decipher = createDecipheriv("aes-256-gcm", this.#requiredCachedKey(lease), row.nonce);
    decipher.setAAD(canonicalProtocolJson(retainedEventAad(base)));
    decipher.setAuthTag(row.auth_tag);
    let plaintext: Buffer;
    try {
      plaintext = Buffer.concat([decipher.update(row.ciphertext), decipher.final()]);
    } catch {
      return this.#corrupt();
    }
    const event = parseNormalizedDriverEvent(plaintext, lease.protocolVersion);
    plaintext.fill(0);
    if (!("turnId" in event) || event.turnId !== base.turnId || event.kind !== base.eventKind) {
      this.#corrupt();
    }
    return {
      stream: "turn",
      ...base,
      readerEpoch: lease.readerEpoch,
      payloadCipherDigest,
      eventDigest,
      recordDigest,
      event,
    } as RetainedDriverEventRecord;
  }

  #assertRecordAuthority(lease: RetainedDriverEventLease, row: RecordRow): void {
    if (
      row.launch_id !== lease.launchId
      || row.state_instance_id !== lease.stateInstanceId
      || row.session_id !== lease.sessionId
      || row.protocol_version !== lease.protocolVersion
      || row.key_generation !== KEY_GENERATION
    ) this.#corrupt();
    assertSafe(row.ordinal);
    const meta = this.#requiredMeta(lease.stateInstanceId);
    if (row.ordinal < meta.ack_next_ordinal) this.#corrupt();
    if (row.ordinal === meta.ack_next_ordinal) {
      if (row.previous_record_digest !== meta.ack_last_record_digest) this.#corrupt();
      return;
    }
    const predecessor = this.#record(lease, row.ordinal - 1);
    if (predecessor === undefined || row.previous_record_digest !== predecessor.record_digest) {
      this.#corrupt();
    }
  }

  #loadOrCreateLaunchKey(): Buffer {
    if (existsSync(this.#keyPath)) return validateKeyFile(this.#keyPath, this.#launchId);
    const keyId = randomCommandId();
    const key = randomBytes(32);
    const bytes = Buffer.concat([
      KEY_MAGIC,
      Buffer.from(this.#launchId, "ascii"),
      Buffer.from(keyId, "ascii"),
      key,
    ]);
    if (bytes.length !== KEY_BYTES) this.#corrupt();
    const temporary = join(this.#launchRoot, `.launch-key.${randomUUID()}.tmp`);
    let descriptor: number | undefined;
    try {
      descriptor = openSync(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
      fchmodSync(descriptor, 0o600);
      const written = writeSync(descriptor, bytes, 0, bytes.length, 0);
      if (written !== bytes.length) this.#corrupt();
      fsyncSync(descriptor);
      closeSync(descriptor);
      descriptor = undefined;
      if (process.platform === "win32") restrictWindowsAcl(temporary, false);
      try {
        linkSync(temporary, this.#keyPath);
      } catch (error) {
        if (isCode(error, "EEXIST")) {
          throw new DriverRetentionError("DRIVER_RESUME_OVERLAP");
        }
        throw error;
      }
      fsyncDirectory(this.#launchRoot);
      unlinkSync(temporary);
      fsyncDirectory(this.#launchRoot);
      const validated = validateKeyFile(this.#keyPath, this.#launchId);
      key.fill(0);
      bytes.fill(0);
      return validated;
    } catch (error) {
      if (descriptor !== undefined) closeSync(descriptor);
      if (existsSync(temporary)) unlinkSync(temporary);
      key.fill(0);
      bytes.fill(0);
      throw sanitizePrivateError(error);
    }
  }

  #meta(stateInstanceId: string): MetaRow | undefined {
    return this.#database.prepare(
      "SELECT * FROM private_driver_event_meta WHERE state_instance_id=?",
    ).get(stateInstanceId) as MetaRow | undefined;
  }

  #requiredMeta(stateInstanceId: string): MetaRow {
    const row = this.#meta(stateInstanceId);
    if (row === undefined) throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
    return row;
  }

  #record(
    lease: Pick<DriverPrivateClaimAttempt, "launchId" | "stateInstanceId" | "sessionId">,
    ordinal: number,
  ): RecordRow | undefined {
    return this.#database.prepare(
      `SELECT * FROM private_driver_event_records
       WHERE launch_id=? AND state_instance_id=? AND session_id=? AND ordinal=?`,
    ).get(lease.launchId, lease.stateInstanceId, lease.sessionId, ordinal) as RecordRow | undefined;
  }

  #replaceCachedKey(input: DriverPrivateClaimAttempt, key: Buffer): void {
    const cacheKey = claimCacheKey(input);
    const previous = this.#dataKeys.get(cacheKey);
    previous?.fill(0);
    this.#dataKeys.set(cacheKey, Buffer.from(key));
    key.fill(0);
  }

  #deleteCachedKey(input: DriverPrivateClaimAttempt): void {
    const cacheKey = claimCacheKey(input);
    this.#dataKeys.get(cacheKey)?.fill(0);
    this.#dataKeys.delete(cacheKey);
  }

  #requiredCachedKey(input: DriverPrivateClaimAttempt): Buffer {
    const key = this.#dataKeys.get(claimCacheKey(input));
    if (key === undefined) throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
    return key;
  }

  #beginImmediate(): void {
    this.#database.exec("BEGIN IMMEDIATE");
  }

  #commit(): void {
    this.#database.exec("COMMIT");
  }

  #rollbackIfActive(): void {
    try {
      this.#database.exec("ROLLBACK");
    } catch {
      // No active transaction is an allowed cleanup state.
    }
  }

  #assertOpen(): void {
    if (this.#closed) throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
  }

  #corrupt(): never {
    throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
  }
}

function preparePrivateRoot(path: string, sourceWorkspace?: string): string {
  const absolute = resolve(path);
  if (absolute === resolve(homedir())) privateBoundary();
  if (
    sourceWorkspace !== undefined
    && (within(absolute, resolve(sourceWorkspace)) || within(resolve(sourceWorkspace), absolute))
  ) {
    throw new DriverRetentionError("DRIVER_PRIVATE_BOUNDARY_VIOLATION");
  }
  mkdirSync(absolute, { recursive: true, mode: 0o700 });
  const stats = lstatSync(absolute);
  if (!stats.isDirectory() || stats.isSymbolicLink()) privateBoundary();
  if (process.platform !== "win32") {
    if ((stats.mode & 0o777) !== 0o700) privateBoundary();
    if (typeof process.getuid === "function" && stats.uid !== process.getuid()) privateBoundary();
  } else {
    restrictWindowsAcl(absolute, true);
  }
  return realpathSync(absolute);
}

function ensurePrivateDatabaseFile(path: string): void {
  if (existsSync(path)) {
    validatePrivateFile(path);
    return;
  }
  const noFollow = "O_NOFOLLOW" in constants
    ? (constants as typeof constants & { O_NOFOLLOW: number }).O_NOFOLLOW
    : 0;
  const descriptor = openSync(
    path,
    constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | noFollow,
    0o600,
  );
  try {
    if (process.platform !== "win32") fchmodSync(descriptor, 0o600);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  if (process.platform === "win32") restrictWindowsAcl(path, false);
  fsyncDirectory(resolve(path, ".."));
}

function chmodPrivateDatabaseFiles(path: string): void {
  for (const candidate of [path, `${path}-wal`, `${path}-shm`]) {
    if (!existsSync(candidate)) continue;
    if (process.platform === "win32") {
      restrictWindowsAcl(candidate, false);
      continue;
    }
    const descriptor = openSync(candidate, constants.O_RDONLY);
    try {
      const stats = fstatSync(descriptor);
      if (!stats.isFile() || stats.uid !== process.getuid?.()) privateBoundary();
      fchmodSync(descriptor, 0o600);
    } finally {
      closeSync(descriptor);
    }
  }
}

function validatePrivateFile(path: string): void {
  const stats = lstatSync(path);
  if (!stats.isFile() || stats.isSymbolicLink()) privateBoundary();
  if (process.platform !== "win32") {
    if ((stats.mode & 0o777) !== 0o600) privateBoundary();
    if (typeof process.getuid === "function" && stats.uid !== process.getuid()) privateBoundary();
  } else {
    restrictWindowsAcl(path, false);
  }
}

function restrictWindowsAcl(path: string, directory: boolean): void {
  if (process.platform !== "win32") return;
  const principal = process.env.USERNAME;
  if (principal === undefined || principal.length === 0) privateBoundary();
  try {
    execFileSync("icacls.exe", [
      path,
      "/inheritance:r",
      "/grant:r",
      `${principal}:${directory ? "(OI)(CI)F" : "F"}`,
    ], { windowsHide: true, stdio: "ignore" });
  } catch {
    privateBoundary();
  }
}

function validateKeyFile(path: string, launchId: LaunchId): Buffer {
  const stats = lstatSync(path);
  if (!stats.isFile() || stats.isSymbolicLink()) privateBoundary();
  if (process.platform !== "win32") {
    if ((stats.mode & 0o777) !== 0o600) privateBoundary();
    if (typeof process.getuid === "function" && stats.uid !== process.getuid()) privateBoundary();
  } else {
    restrictWindowsAcl(path, false);
  }
  const descriptor = openSync(path, constants.O_RDONLY);
  try {
    const bytes = Buffer.alloc(KEY_BYTES + 1);
    const count = readSync(descriptor, bytes, 0, bytes.length, 0);
    if (
      count !== KEY_BYTES
      || !bytes.subarray(0, 8).equals(KEY_MAGIC)
      || bytes.subarray(8, 38).toString("ascii") !== launchId
      || !/^cmd_[0-9a-hjkmnp-tv-z]{26}$/u.test(bytes.subarray(38, 68).toString("ascii"))
    ) throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
    return Buffer.from(bytes.subarray(68, 100));
  } finally {
    closeSync(descriptor);
  }
}

function unwrapDataKey(row: MetaRow, launchKey: Buffer, input: DriverPrivateClaimAttempt): Buffer {
  const decipher = createDecipheriv("aes-256-gcm", launchKey, row.key_wrap_nonce);
  decipher.setAAD(keyWrapAad(input, row.key_id as CommandId, row.nonce_prefix));
  decipher.setAuthTag(row.key_wrap_tag);
  try {
    const key = Buffer.concat([decipher.update(row.wrapped_data_key), decipher.final()]);
    if (key.length !== 32) throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
    return key;
  } catch {
    throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
  }
}

function keyWrapAad(
  input: DriverPrivateClaimAttempt,
  keyId: CommandId,
  noncePrefix: Uint8Array,
): Uint8Array {
  return canonicalProtocolJson({
    schema: "swarm.private-driver-key",
    version: 1,
    protocolVersion: input.protocolVersion,
    launchId: input.launchId,
    stateInstanceId: input.stateInstanceId,
    sessionId: input.sessionId,
    keyId,
    keyGeneration: KEY_GENERATION,
    noncePrefix: Buffer.from(noncePrefix).toString("hex"),
  });
}

function assertClaimShape(
  input: DriverPrivateClaimAttempt,
  protocolVersion: ProtocolVersion,
  launchId: LaunchId,
): void {
  if (
    input.protocolVersion !== protocolVersion
    || input.launchId !== launchId
    || !/^sti_[0-9a-hjkmnp-tv-z]{26}$/u.test(input.stateInstanceId)
    || !/^ses_[0-9a-hjkmnp-tv-z]{26}$/u.test(input.sessionId)
    || !/^sha256:[0-9a-f]{64}$/u.test(input.ownerToken)
    || !/^cmd_[0-9a-hjkmnp-tv-z]{26}$/u.test(input.claimAttemptId)
    || !Number.isSafeInteger(input.readerEpoch)
    || input.readerEpoch < 1
    || !Number.isSafeInteger(input.nextOrdinal)
    || input.nextOrdinal < 0
    || ((input.nextOrdinal === 0) !== (input.lastEventDigest === null))
  ) throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
}

function sameEnvelopeRow(row: RecordRow, envelope: PreparedRetainedDriverEventEnvelope): boolean {
  return row.record_digest === envelope.recordDigest
    && row.event_digest === envelope.eventDigest
    && row.payload_cipher_digest === envelope.payloadCipherDigest
    && row.previous_record_digest === envelope.previousRecordDigest
    && row.resolved_waiter_id === envelope.resolvedWaiterId
    && row.source_message_id === envelope.sourceMessageId
    && row.binding_digest === envelope.bindingDigest
    && row.turn_id === envelope.turnId
    && row.event_kind === envelope.eventKind
    && Buffer.from(row.nonce).equals(Buffer.from(envelope.nonce))
    && Buffer.from(row.auth_tag).equals(Buffer.from(envelope.authTag))
    && Buffer.from(row.ciphertext).equals(Buffer.from(envelope.ciphertext));
}

function claimCacheKey(input: DriverPrivateClaimAttempt): string {
  return `${input.stateInstanceId}:${input.sessionId}:${input.readerEpoch}:${input.claimAttemptId}`;
}

function randomCommandId(): CommandId {
  const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
  const bytes = randomBytes(26);
  let value = "";
  for (const byte of bytes) value += alphabet[byte & 31];
  return `cmd_${value}` as CommandId;
}

function assertSafe(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > SAFE_INTEGER) {
    throw new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
  }
}

function fsyncDirectory(path: string): void {
  if (process.platform === "win32") return;
  const descriptor = openSync(path, constants.O_RDONLY);
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

function within(path: string, parent: string): boolean {
  const value = relative(parent, path);
  return value === "" || (!value.startsWith(`..${sep}`) && value !== "..");
}

function privateBoundary(): never {
  throw new DriverRetentionError("DRIVER_PRIVATE_BOUNDARY_VIOLATION");
}

function sanitizePrivateError(error: unknown): Error {
  if (error instanceof DriverRetentionError) return error;
  if (isCode(error, "SQLITE_CONSTRAINT")) {
    return new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
  }
  return new DriverRetentionError("DRIVER_RETAINED_LOG_CORRUPT");
}

function isCode(error: unknown, code: string): boolean {
  return typeof error === "object" && error !== null && "code" in error
    && (error as { code?: unknown }).code === code;
}
