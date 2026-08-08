import { DatabaseSync, type SQLInputValue } from "node:sqlite";

import {
  canonicalProtocolJson,
  parseReconcileDeliveryAttempt,
  type ArtifactDigest,
  type CommandId,
  type ConsumePermit,
  type DeliveryFence,
  type InputWrittenJournalEntry,
  type InvocationJournalEntry,
  type ModelVisibleJournalEntry,
  type NativeDeliveryEnvelope,
  type NativeInvocationFence,
  type ReconcileDeliveryAttempt,
  type ReconcileEvidence,
  type ScriptedNotWrittenProof,
  type WriteStartedJournalEntry,
} from "@swarm/protocol";
import type {
  JournalRecovery,
  NativeJournalPort,
} from "@swarm/daemon-core";
import type { NativeRuntimePort } from "@swarm/drivers";
import { protocolDigest } from "@swarm/runtime-contract";

import { deterministicCommandId } from "./ids.js";

type AttemptRow = {
  fence_json: string;
  state: string;
  permit_id: string | null;
  invocation_generation: number | null;
  invocation_id: string | null;
  body_digest: string | null;
  previous_invocation_generation: number | null;
  previous_proof_digest: string | null;
  proof_json: string | null;
  disconnect_id: string | null;
  suppression_reason: string | null;
};

type EntryRow = { entry_json: string };

function stringify(value: unknown): string {
  return new TextDecoder().decode(canonicalProtocolJson(value));
}

function parse<Value>(value: string): Value {
  return JSON.parse(value) as Value;
}

function requestWithDigest<Value extends Record<string, unknown>>(
  value: Value,
): Value & { requestDigest: ArtifactDigest } {
  return { ...value, requestDigest: protocolDigest(value) };
}

export class NativeSqliteJournal implements NativeJournalPort {
  readonly #database: DatabaseSync;
  readonly #driverKind: NativeRuntimePort["driverKind"];

  constructor(path: string, driverKind: NativeRuntimePort["driverKind"]) {
    this.#database = new DatabaseSync(path);
    this.#driverKind = driverKind;
    this.#database.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS native_attempts (
        delivery_id TEXT NOT NULL,
        attempt INTEGER NOT NULL CHECK (attempt >= 1),
        fence_json TEXT NOT NULL,
        state TEXT NOT NULL,
        permit_id TEXT,
        invocation_generation INTEGER,
        invocation_id TEXT,
        body_digest TEXT,
        previous_invocation_generation INTEGER,
        previous_proof_digest TEXT,
        proof_json TEXT,
        disconnect_id TEXT,
        suppression_reason TEXT,
        PRIMARY KEY (delivery_id, attempt)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS native_invocation_entries (
        delivery_id TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        invocation_generation INTEGER NOT NULL,
        sequence INTEGER NOT NULL,
        kind TEXT NOT NULL,
        entry_json TEXT NOT NULL,
        PRIMARY KEY (delivery_id, attempt, invocation_generation, sequence),
        UNIQUE (delivery_id, attempt, invocation_generation, kind),
        FOREIGN KEY (delivery_id, attempt)
          REFERENCES native_attempts(delivery_id, attempt)
      ) STRICT;
    `);
  }

  close(): void {
    this.#database.close();
  }

  async recordDelivery(delivery: NativeDeliveryEnvelope, fence: DeliveryFence): Promise<void> {
    const fenceJson = stringify(fence);
    this.#run(
      `INSERT INTO native_attempts(delivery_id, attempt, fence_json, state)
       VALUES (?, ?, ?, 'accepted') ON CONFLICT(delivery_id, attempt) DO NOTHING`,
      delivery.deliveryId,
      delivery.attempt,
      fenceJson,
    );
    const row = this.#attempt(fence);
    if (row.fence_json !== fenceJson) throw new Error("STALE_DELIVERY_FENCE");
  }

  async recoveryFor(fence: DeliveryFence): Promise<JournalRecovery | null> {
    const row = this.#attempt(fence);
    if (row.state === "accepted") return null;
    if (row.state === "consumed") return { kind: "consumed" };
    if (row.state === "ambiguous") return { kind: "held_ambiguous" };
    if (row.state === "suppressed") {
      const reason = row.suppression_reason;
      if (reason !== "MEMBERSHIP_REVOKED_BEFORE_CONSUME" && reason !== "ROUTE_SUPERSEDED_BEFORE_CONSUME") {
        throw new Error("INVALID_JOURNAL_CHAIN");
      }
      return { kind: "terminal_suppression", reason };
    }

    let permitId: CommandId | null = null;
    let invocation: NativeInvocationFence | null = null;
    let evidence: ReconcileEvidence;
    if (row.state === "pre_permit_disconnect") {
      if (row.disconnect_id === null) throw new Error("INVALID_JOURNAL_CHAIN");
      evidence = {
        kind: "pre_permit_disconnect",
        disconnectId: row.disconnect_id as CommandId,
      };
    } else {
      if (row.permit_id === null || row.invocation_generation === null || row.invocation_id === null) {
        throw new Error("PERMIT_REQUIRED");
      }
      permitId = row.permit_id as CommandId;
      invocation = {
        invocationGeneration: row.invocation_generation,
        invocationId: row.invocation_id as CommandId,
      };
      const permitRecorded = this.#entry<InvocationJournalEntry<"permit_recorded">>(fence, invocation, "permit_recorded");
      if (row.state === "permit_recorded") {
        evidence = { kind: "permit_recorded_write_not_started", permitRecorded };
      } else {
        const writeStarted = this.#entry<WriteStartedJournalEntry>(fence, invocation, "write_started");
        if (row.state === "not_written") {
          if (row.proof_json === null) throw new Error("FAKE_NOT_WRITTEN_PROOF_REQUIRED");
          evidence = {
            kind: "scripted_not_written",
            permitRecorded,
            writeStarted,
            proof: parse<ScriptedNotWrittenProof>(row.proof_json),
          };
        } else if (row.state === "write_started") {
          evidence = { kind: "write_started_ambiguous", permitRecorded, writeStarted, driverKind: this.#driverKind };
        } else {
          const inputWritten = this.#entry<InputWrittenJournalEntry>(fence, invocation, "input_written");
          if (row.state === "input_written") {
            evidence = {
              kind: "input_written",
              permitRecorded,
              writeStarted,
              inputWritten,
            };
          } else if (row.state === "model_visible") {
            evidence = {
              kind: "model_visible",
              permitRecorded,
              writeStarted,
              inputWritten,
              modelVisible: this.#entry<ModelVisibleJournalEntry>(fence, invocation, "model_visible"),
            };
          } else {
            throw new Error("INVALID_JOURNAL_CHAIN");
          }
        }
      }
    }
    const evidenceDigest = protocolDigest(evidence);
    const base = {
      ...fence,
      commandId: deterministicCommandId(`${fence.deliveryId}:${fence.attempt}:${row.state}:${evidenceDigest}`),
      permitId,
      invocation,
      evidenceDigest,
      evidence,
    };
    const command = requestWithDigest(base) as ReconcileDeliveryAttempt;
    return {
      kind: "reconcile",
      command: parseReconcileDeliveryAttempt(canonicalProtocolJson(command), fence.protocolVersion),
    };
  }

  async recordPrePermitDisconnect(fence: DeliveryFence, disconnectId: CommandId): Promise<void> {
    this.#transition(fence, ["accepted"], "pre_permit_disconnect", {
      disconnect_id: disconnectId,
    });
  }

  async recordPermit(permit: ConsumePermit): Promise<InvocationJournalEntry<"permit_recorded">> {
    const fence = this.#fenceFromPermit(permit);
    return this.#transaction(() => {
      const current = this.#attempt(fence);
      const bodyDigest = protocolDigest(permit.body);
      if (current.body_digest !== null && current.body_digest !== bodyDigest) {
        throw new Error("IDEMPOTENCY_CONFLICT");
      }
      if (current.state === "accepted" && permit.invocationGeneration !== 1) {
        throw new Error("STALE_INVOCATION_GENERATION");
      }
      if (
        current.state === "permit_recorded"
        && (
          current.invocation_generation !== permit.invocationGeneration
          || current.invocation_id !== permit.invocationId
          || current.permit_id !== permit.permitId
        )
      ) {
        throw new Error("INVOCATION_STATE_CONFLICT");
      }
      let previousGeneration = current.previous_invocation_generation;
      let previousProofDigest = current.previous_proof_digest;
      if (current.state === "not_written") {
        if (
          current.invocation_generation === null
          || permit.invocationGeneration !== current.invocation_generation + 1
          || current.permit_id !== permit.permitId
          || current.proof_json === null
        ) {
          throw new Error("STALE_INVOCATION_GENERATION");
        }
        previousGeneration = current.invocation_generation;
        previousProofDigest = parse<ScriptedNotWrittenProof>(current.proof_json).proofDigest;
      }
      const entry = this.#appendEntry("permit_recorded", fence, permit, {});
      const result = this.#database.prepare(
        `UPDATE native_attempts SET state='permit_recorded', permit_id=?,
         invocation_generation=?, invocation_id=?, body_digest=?,
         previous_invocation_generation=?, previous_proof_digest=?
         WHERE delivery_id=? AND attempt=? AND state IN ('accepted','permit_recorded','not_written')`,
      ).run(
        permit.permitId,
        permit.invocationGeneration,
        permit.invocationId,
        bodyDigest,
        previousGeneration,
        previousProofDigest,
        permit.deliveryId,
        permit.attempt,
      );
      if (result.changes !== 1) throw new Error("INVOCATION_STATE_CONFLICT");
      return entry as InvocationJournalEntry<"permit_recorded">;
    });
  }

  async recordWriteStarted(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    inputDigest: ArtifactDigest;
  }): Promise<WriteStartedJournalEntry> {
    return this.#transaction(() => {
      const entry = this.#appendEntry("write_started", input.fence, {
        ...input.invocation,
        permitId: input.permitId,
      }, { inputDigest: input.inputDigest }) as WriteStartedJournalEntry;
      this.#transition(input.fence, ["permit_recorded", "write_started"], "write_started");
      return entry;
    });
  }

  async recordNotWritten(proof: ScriptedNotWrittenProof): Promise<void> {
    const { proofDigest, ...withoutDigest } = proof;
    if (protocolDigest(withoutDigest) !== proofDigest) {
      throw new Error("INVALID_JOURNAL_CHAIN");
    }
    const row = this.#database.prepare(
      `SELECT delivery_id, attempt FROM native_attempts
       WHERE invocation_id=? AND invocation_generation=? AND state='write_started'`,
    ).get(proof.invocationId, proof.invocationGeneration) as { delivery_id: string; attempt: number } | undefined;
    if (row === undefined) throw new Error("INVOCATION_STATE_CONFLICT");
    const started = this.#database.prepare(
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id=? AND attempt=? AND invocation_generation=? AND kind='write_started'`,
    ).get(row.delivery_id, row.attempt, proof.invocationGeneration) as EntryRow | undefined;
    if (started === undefined) throw new Error("INVALID_JOURNAL_CHAIN");
    const entry = parse<WriteStartedJournalEntry>(started.entry_json);
    if (proof.writeStartedEntryId !== entry.entryId || proof.writeStartedEntryDigest !== entry.entryDigest) {
      throw new Error("INVALID_JOURNAL_CHAIN");
    }
    const result = this.#database.prepare(
      `UPDATE native_attempts SET state='not_written', proof_json=?
       WHERE delivery_id=? AND attempt=? AND state='write_started'`,
    ).run(
      stringify(proof),
      row.delivery_id,
      row.attempt,
    );
    if (result.changes !== 1) throw new Error("INVOCATION_STATE_CONFLICT");
  }

  async recordInputWritten(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    runtimeWriteId: CommandId;
  }): Promise<InputWrittenJournalEntry> {
    return this.#transaction(() => {
      const entry = this.#appendEntry("input_written", input.fence, {
        ...input.invocation,
        permitId: input.permitId,
      }, { runtimeWriteId: input.runtimeWriteId }) as InputWrittenJournalEntry;
      this.#transition(input.fence, ["write_started", "input_written"], "input_written");
      return entry;
    });
  }

  async recordModelVisible(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    runtimeWriteId: CommandId;
    visibilityEventId: CommandId;
  }): Promise<ModelVisibleJournalEntry> {
    return this.#transaction(() => {
      const entry = this.#appendEntry("model_visible", input.fence, {
        ...input.invocation,
        permitId: input.permitId,
      }, { runtimeWriteId: input.runtimeWriteId, visibilityEventId: input.visibilityEventId }) as ModelVisibleJournalEntry;
      this.#transition(input.fence, ["input_written", "model_visible"], "model_visible");
      return entry;
    });
  }

  async recordSuppressed(
    fence: DeliveryFence,
    reason: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME",
  ): Promise<void> {
    this.#transition(fence, ["accepted"], "suppressed", { suppression_reason: reason });
  }

  async markAmbiguous(fence: DeliveryFence, _invocation: NativeInvocationFence): Promise<void> {
    this.#transition(fence, ["write_started", "input_written"], "ambiguous");
  }

  async markConsumed(fence: DeliveryFence, _invocation: NativeInvocationFence): Promise<void> {
    this.#transition(fence, ["model_visible"], "consumed");
  }

  #appendEntry(
    kind: "permit_recorded" | "write_started" | "input_written" | "model_visible",
    fence: DeliveryFence,
    invocation: NativeInvocationFence & { permitId: CommandId },
    extra: Record<string, unknown>,
  ): InvocationJournalEntry<string> {
    const existing = this.#database.prepare(
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id=? AND attempt=? AND invocation_generation=? AND kind=?`,
    ).get(fence.deliveryId, fence.attempt, invocation.invocationGeneration, kind) as EntryRow | undefined;
    if (existing !== undefined) {
      const entry = parse<InvocationJournalEntry<string> & Record<string, unknown>>(existing.entry_json);
      const expected = {
        ...fence,
        invocationGeneration: invocation.invocationGeneration,
        invocationId: invocation.invocationId,
        permitId: invocation.permitId,
        ...extra,
      };
      for (const [key, value] of Object.entries(expected)) {
        if (entry[key] !== value) throw new Error("WRITE_STARTED_BINDING_MISMATCH");
      }
      return entry;
    }
    const previous = this.#database.prepare(
      `SELECT sequence, entry_json FROM native_invocation_entries
       WHERE delivery_id=? AND attempt=? AND invocation_generation=? ORDER BY sequence DESC LIMIT 1`,
    ).get(fence.deliveryId, fence.attempt, invocation.invocationGeneration) as (EntryRow & { sequence: number }) | undefined;
    const previousEntry = previous === undefined
      ? undefined
      : parse<InvocationJournalEntry<string>>(previous.entry_json);
    const withoutDigest = {
      journalId: invocation.invocationId,
      entryId: deterministicCommandId(`${fence.deliveryId}:${fence.attempt}:${invocation.invocationGeneration}:${kind}`),
      sequence: (previous?.sequence ?? 0) + 1,
      kind,
      previousEntryDigest: previousEntry?.entryDigest ?? null,
      ...fence,
      invocationGeneration: invocation.invocationGeneration,
      invocationId: invocation.invocationId,
      permitId: invocation.permitId,
      ...extra,
    };
    const entry = { ...withoutDigest, entryDigest: protocolDigest(withoutDigest) } as InvocationJournalEntry<string>;
    this.#run(
      `INSERT INTO native_invocation_entries(
         delivery_id, attempt, invocation_generation, sequence, kind, entry_json
       ) VALUES (?, ?, ?, ?, ?, ?)`,
      fence.deliveryId,
      fence.attempt,
      invocation.invocationGeneration,
      entry.sequence,
      kind,
      stringify(entry),
    );
    return entry;
  }

  #entry<Value>(fence: DeliveryFence, invocation: NativeInvocationFence, kind: string): Value {
    const row = this.#database.prepare(
      `SELECT entry_json FROM native_invocation_entries
       WHERE delivery_id=? AND attempt=? AND invocation_generation=? AND kind=?`,
    ).get(fence.deliveryId, fence.attempt, invocation.invocationGeneration, kind) as EntryRow | undefined;
    if (row === undefined) throw new Error("INVALID_JOURNAL_CHAIN");
    return parse<Value>(row.entry_json);
  }

  #fenceFromPermit(permit: ConsumePermit): DeliveryFence {
    return {
      protocolVersion: permit.protocolVersion,
      deliveryId: permit.deliveryId,
      attempt: permit.attempt,
      producerFactId: permit.producerFactId,
      agentId: permit.agentId,
      machineId: permit.machineId,
      launchId: permit.launchId,
      membershipEpoch: permit.membershipEpoch,
      routingGeneration: permit.routingGeneration,
      routeVersion: permit.routeVersion,
      sessionId: permit.sessionId,
      turnId: permit.turnId,
    };
  }

  #attempt(fence: DeliveryFence): AttemptRow {
    const row = this.#database.prepare(
      `SELECT fence_json, state, permit_id, invocation_generation, invocation_id,
              body_digest, previous_invocation_generation, previous_proof_digest,
              proof_json, disconnect_id, suppression_reason
       FROM native_attempts WHERE delivery_id=? AND attempt=?`,
    ).get(fence.deliveryId, fence.attempt) as AttemptRow | undefined;
    if (row === undefined) throw new Error("STALE_DELIVERY_FENCE");
    return row;
  }

  #transition(
    fence: DeliveryFence,
    allowed: readonly string[],
    next: string,
    columns: Record<string, SQLInputValue> = {},
  ): void {
    const names = Object.keys(columns);
    const assignments = ["state=?", ...names.map((name) => `${name}=?`)];
    const result = this.#database.prepare(
      `UPDATE native_attempts SET ${assignments.join(",")}
       WHERE delivery_id=? AND attempt=? AND state IN (${allowed.map(() => "?").join(",")})`,
    ).run(next, ...names.map((name) => columns[name] ?? null), fence.deliveryId, fence.attempt, ...allowed);
    if (result.changes !== 1) throw new Error("INVOCATION_STATE_CONFLICT");
  }

  #run(sql: string, ...values: SQLInputValue[]): void {
    this.#database.prepare(sql).run(...values);
  }

  #transaction<Value>(body: () => Value): Value {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const value = body();
      this.#database.exec("COMMIT");
      return value;
    } catch (error) {
      this.#database.exec("ROLLBACK");
      throw error;
    }
  }
}
