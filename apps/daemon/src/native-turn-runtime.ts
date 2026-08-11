import {
  DriverRetentionError,
  type DriverCompositeClaimCloseResult,
  type DriverCompositeCursorClaimHandle,
  type DriverCursorAuthority,
  type DriverCursorClaimCoordinator,
  type DriverEventPump,
  type DriverLiveResumeAuthorization,
  type DriverLiveResumeTicket,
  type DriverPrivateClaimAttempt,
  type DriverRetainedReplay,
  type NativeProcessDriver,
  type RetainedDriverEventLease,
  type SpawnHandle,
} from "@swarm/drivers";
import {
  StorageError,
  type DaemonJournal,
  type TurnRecoveryReadResult,
} from "@swarm/storage";
import type {
  ArtifactDigest,
  CommandId,
  LaunchId,
  ProtocolVersion,
  SessionId,
  StateInstanceId,
} from "@swarm/protocol";

import { RandomCommandIdSource } from "./ids.js";

export type NativeCursorClaimCoordinatorOptions = {
  journal: DaemonJournal;
  now?: () => string;
  nextClaimAttemptId?: () => CommandId;
};

export class NativeCursorClaimCoordinator implements DriverCursorClaimCoordinator {
  readonly #journal: DaemonJournal;
  readonly #now: () => string;
  readonly #nextClaimAttemptId: () => CommandId;
  readonly #liveResumeAuthorizations = new WeakSet<DriverLiveResumeAuthorization>();
  readonly #liveResumeTickets = new WeakSet<DriverLiveResumeTicket>();

  constructor(options: NativeCursorClaimCoordinatorOptions) {
    this.#journal = options.journal;
    this.#now = options.now ?? (() => new Date().toISOString());
    const ids = new RandomCommandIdSource();
    this.#nextClaimAttemptId = options.nextClaimAttemptId ?? (() => ids.nextCommandId());
  }

  claimAfterSpawn(input: {
    spec: import("@swarm/drivers").DriverLaunchSpec;
    process: SpawnHandle;
    pump: DriverEventPump;
    cursorOwnerToken: ArtifactDigest;
  }): Promise<DriverCompositeCursorClaimHandle> {
    if (
      input.process.launchId !== input.spec.launch.launchId
      || input.process.stateInstanceId !== input.pump.stateInstanceId
    ) return Promise.reject(new DriverRetentionError("DRIVER_START_AUTHORITY_REQUIRED"));
    return this.#claim({
      protocolVersion: input.spec.launch.protocolVersion,
      launchId: input.spec.launch.launchId,
      stateInstanceId: input.process.stateInstanceId,
      sessionId: input.spec.sessionId,
      cursorOwnerToken: input.cursorOwnerToken,
      pump: input.pump,
      processMode: "start",
      replayMode: "live",
    });
  }

  claimForReplay(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    pump: DriverEventPump;
  }): Promise<DriverCompositeCursorClaimHandle> {
    if (input.stateInstanceId !== input.pump.stateInstanceId) {
      return Promise.reject(new DriverRetentionError("DRIVER_EVENT_FENCE_MISMATCH"));
    }
    return this.#claim({ ...input, processMode: "resume", replayMode: "retained_only" });
  }

  beginLiveResume(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    authorization: DriverLiveResumeAuthorization;
  }): DriverLiveResumeTicket {
    if (!this.#liveResumeAuthorizations.has(input.authorization)) {
      throw new DriverRetentionError("DRIVER_START_AUTHORITY_REQUIRED");
    }
    this.#liveResumeAuthorizations.delete(input.authorization);
    if (
      input.authorization.protocolVersion !== input.protocolVersion
      || input.authorization.launchId !== input.launchId
      || input.authorization.stateInstanceId !== input.stateInstanceId
      || input.authorization.sessionId !== input.sessionId
      || input.authorization.ownerToken !== input.cursorOwnerToken
    ) {
      throw new DriverRetentionError("DRIVER_EVENT_FENCE_MISMATCH");
    }
    const ticket: DriverLiveResumeTicket = Object.freeze({
      protocolVersion: input.authorization.protocolVersion,
      launchId: input.authorization.launchId,
      stateInstanceId: input.authorization.stateInstanceId,
      sessionId: input.authorization.sessionId,
      ownerToken: input.authorization.ownerToken,
      nextOrdinal: input.authorization.nextOrdinal,
      lastEventDigest: input.authorization.lastEventDigest,
    });
    this.#liveResumeTickets.add(ticket);
    return ticket;
  }

  async claimForLiveResume(input: {
    ticket: DriverLiveResumeTicket;
    pump: DriverEventPump;
  }): Promise<DriverCompositeCursorClaimHandle> {
    if (!this.#liveResumeTickets.has(input.ticket)) {
      throw new DriverRetentionError("DRIVER_START_AUTHORITY_REQUIRED");
    }
    this.#liveResumeTickets.delete(input.ticket);
    if (input.ticket.stateInstanceId !== input.pump.stateInstanceId) {
      throw new DriverRetentionError("DRIVER_EVENT_FENCE_MISMATCH");
    }
    const claim = await this.#claim({
      protocolVersion: input.ticket.protocolVersion,
      launchId: input.ticket.launchId,
      stateInstanceId: input.ticket.stateInstanceId,
      sessionId: input.ticket.sessionId,
      cursorOwnerToken: input.ticket.ownerToken,
      pump: input.pump,
      processMode: "resume",
      replayMode: "live",
    });
    try {
      if (
        claim.authority.nextOrdinal !== input.ticket.nextOrdinal
        || claim.authority.lastEventDigest !== input.ticket.lastEventDigest
        || claim.privateLease.snapshotHeadNextOrdinal !== claim.authority.nextOrdinal
      ) {
        throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
      }
      return claim;
    } catch (cause) {
      try {
        await claim.abort();
      } catch (cleanup) {
        throw new AggregateError([cause, cleanup], "DRIVER_CURSOR_CLEANUP_FAILED");
      }
      throw cause;
    }
  }

  cancelLiveResume(ticket: DriverLiveResumeTicket): void {
    this.#liveResumeTickets.delete(ticket);
  }

  async releaseReplayAsNoActive(
    claim: DriverCompositeCursorClaimHandle,
  ): Promise<DriverLiveResumeAuthorization> {
    if (
      claim.privateLease.processMode !== "resume"
      || claim.privateLease.replayMode !== "retained_only"
      || claim.privateLease.protocolVersion !== claim.authority.protocolVersion
      || claim.privateLease.launchId !== claim.authority.launchId
      || claim.privateLease.stateInstanceId !== claim.authority.stateInstanceId
      || claim.privateLease.sessionId !== claim.authority.sessionId
      || claim.privateLease.ownerToken !== claim.authority.ownerToken
      || claim.privateLease.readerEpoch !== claim.authority.readerEpoch
      || claim.privateLease.nextOrdinal !== claim.authority.nextOrdinal
      || claim.privateLease.lastEventDigest !== claim.authority.lastEventDigest
    ) throw new DriverRetentionError("DRIVER_EVENT_FENCE_MISMATCH");
    await claim.abort();
    const authorization: DriverLiveResumeAuthorization = Object.freeze({
      protocolVersion: claim.authority.protocolVersion,
      launchId: claim.authority.launchId,
      stateInstanceId: claim.authority.stateInstanceId,
      sessionId: claim.authority.sessionId,
      ownerToken: claim.authority.ownerToken,
      provedReaderEpoch: claim.authority.readerEpoch,
      nextOrdinal: claim.authority.nextOrdinal,
      lastEventDigest: claim.authority.lastEventDigest,
    });
    this.#liveResumeAuthorizations.add(authorization);
    return authorization;
  }

  async #claim(input: {
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    pump: DriverEventPump;
    processMode: "start" | "resume";
    replayMode: "live" | "retained_only";
  }): Promise<DriverCompositeCursorClaimHandle> {
    let storage: { readerEpoch: number; nextOrdinal: number; lastEventDigest: ArtifactDigest | null };
    if (input.processMode === "start") {
      storage = this.#journal.claimDriverEventReader({
        stateInstanceId: input.stateInstanceId,
        sessionId: input.sessionId,
        ownerToken: input.cursorOwnerToken,
        mode: "start",
        claimedAt: this.#now(),
      });
    } else {
      try {
        storage = this.#journal.claimDriverEventReader({
          stateInstanceId: input.stateInstanceId,
          sessionId: input.sessionId,
          ownerToken: input.cursorOwnerToken,
          mode: "resume",
          claimedAt: this.#now(),
        });
      } catch (error) {
        if (!(error instanceof StorageError) || error.code !== "DRIVER_RESUME_OVERLAP") throw error;
        storage = this.#journal.claimOrphanedDriverEventReader({
          stateInstanceId: input.stateInstanceId,
          sessionId: input.sessionId,
          ownerToken: input.cursorOwnerToken,
          claimedAt: this.#now(),
        });
      }
    }
    const authority: DriverCursorAuthority = {
      protocolVersion: input.protocolVersion,
      launchId: input.launchId,
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      ownerToken: input.cursorOwnerToken,
      readerEpoch: storage.readerEpoch,
      nextOrdinal: storage.nextOrdinal,
      lastEventDigest: storage.lastEventDigest,
    };
    const attempt: DriverPrivateClaimAttempt = {
      ...authority,
      claimAttemptId: this.#nextClaimAttemptId(),
      processMode: input.processMode,
      replayMode: input.replayMode,
    };
    let privateLease: RetainedDriverEventLease | undefined;
    try {
      privateLease = await input.pump.claimCursor(attempt);
      assertPrivateLease(privateLease, attempt);
      return new OwnedCompositeCursorClaimHandle(
        this.#journal,
        input.pump,
        authority,
        attempt,
        privateLease,
        this.#now,
      );
    } catch (cause) {
      const failures: unknown[] = [cause];
      try {
        if (privateLease !== undefined) await input.pump.closeLeaseObservers(privateLease);
      } catch (error) {
        failures.push(error);
      }
      let storageReleased = false;
      try {
        this.#journal.releaseDriverEventReader({
          stateInstanceId: authority.stateInstanceId,
          sessionId: authority.sessionId,
          ownerToken: authority.ownerToken,
          readerEpoch: authority.readerEpoch,
          releasedAt: this.#now(),
        });
        storageReleased = true;
      } catch (error) {
        failures.push(error);
      }
      if (storageReleased) {
        try {
          await input.pump.releaseClaimAttempt(attempt);
        } catch (error) {
          failures.push(error);
        }
      }
      if (failures.length > 1) {
        throw new AggregateError(failures, "DRIVER_CURSOR_CLEANUP_FAILED");
      }
      throw cause;
    }
  }
}

class OwnedCompositeCursorClaimHandle implements DriverCompositeCursorClaimHandle {
  readonly authority: DriverCursorAuthority;
  readonly privateLease: RetainedDriverEventLease;
  readonly #journal: DaemonJournal;
  readonly #pump: DriverEventPump;
  readonly #attempt: DriverPrivateClaimAttempt;
  readonly #now: () => string;
  #closePromise: Promise<DriverCompositeClaimCloseResult> | undefined;

  constructor(
    journal: DaemonJournal,
    pump: DriverEventPump,
    authority: DriverCursorAuthority,
    attempt: DriverPrivateClaimAttempt,
    privateLease: RetainedDriverEventLease,
    now: () => string,
  ) {
    this.#journal = journal;
    this.#pump = pump;
    this.authority = authority;
    this.#attempt = attempt;
    this.privateLease = privateLease;
    this.#now = now;
  }

  abort(): Promise<DriverCompositeClaimCloseResult> {
    return this.#close();
  }

  release(): Promise<DriverCompositeClaimCloseResult> {
    return this.#close();
  }

  #close(): Promise<DriverCompositeClaimCloseResult> {
    this.#closePromise ??= this.#closeInternal();
    return this.#closePromise;
  }

  async #closeInternal(): Promise<DriverCompositeClaimCloseResult> {
    const failures: unknown[] = [];
    try {
      await this.#pump.closeLeaseObservers(this.privateLease);
    } catch (error) {
      failures.push(error);
    }
    let storageReleased = false;
    let privateReleased = false;
    try {
      this.#journal.releaseDriverEventReader({
        stateInstanceId: this.authority.stateInstanceId,
        sessionId: this.authority.sessionId,
        ownerToken: this.authority.ownerToken,
        readerEpoch: this.authority.readerEpoch,
        releasedAt: this.#now(),
      });
      storageReleased = true;
    } catch (error) {
      failures.push(error);
    }
    if (storageReleased) {
      try {
        await this.#pump.releaseClaimAttempt(this.#attempt);
        privateReleased = true;
      } catch (error) {
        failures.push(error);
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, "DRIVER_CURSOR_CLEANUP_FAILED");
    }
    return { applied: true, storageReleased, privateReleased };
  }
}

export type FreshTurnRecoveryDisposition =
  | { kind: "no_active"; liveResumeAuthorization: DriverLiveResumeAuthorization }
  | { kind: "replay_retained_suffix"; replay: DriverRetainedReplay }
  | {
      kind: "held_ambiguous";
      reason:
        | "PRE_MODEL_VISIBLE_EFFECT_UNKNOWN"
        | "ACTIVE_AMBIGUOUS"
        | "RETAINED_SUFFIX_INCOMPLETE";
    };

export class NativeTurnRuntime {
  readonly #journal: DaemonJournal;
  readonly #coordinator: DriverCursorClaimCoordinator;
  readonly #drivers: Readonly<Record<"codex" | "claude", NativeProcessDriver>>;

  constructor(input: {
    journal: DaemonJournal;
    coordinator: DriverCursorClaimCoordinator;
    codex: NativeProcessDriver;
    claude: NativeProcessDriver;
  }) {
    this.#journal = input.journal;
    this.#coordinator = input.coordinator;
    this.#drivers = { codex: input.codex, claude: input.claude };
  }

  async recoverRetainedTurn(input: {
    runtime: "codex" | "claude";
    protocolVersion: ProtocolVersion;
    launchId: LaunchId;
    stateInstanceId: StateInstanceId;
    sessionId: SessionId;
    cursorOwnerToken: ArtifactDigest;
    pump: DriverEventPump;
  }): Promise<FreshTurnRecoveryDisposition> {
    const claim = await this.#coordinator.claimForReplay(input);
    let recovery: TurnRecoveryReadResult | null;
    try {
      recovery = this.#journal.transaction((transaction) => transaction.readTurnRecovery({
        stateInstanceId: claim.authority.stateInstanceId,
        sessionId: claim.authority.sessionId,
        ownerToken: claim.authority.ownerToken,
        readerEpoch: claim.authority.readerEpoch,
      }));
    } catch (error) {
      await claim.abort();
      throw error;
    }
    if (recovery === null) {
      const liveResumeAuthorization = await this.#coordinator.releaseReplayAsNoActive(claim);
      return { kind: "no_active", liveResumeAuthorization };
    }
    if (recovery.kind === "held_ambiguous") {
      await claim.abort();
      return recovery;
    }
    if (
      recovery.cursor.nextOrdinal !== claim.authority.nextOrdinal
      || recovery.cursor.lastEventDigest !== claim.authority.lastEventDigest
    ) {
      await claim.abort();
      throw new DriverRetentionError("DRIVER_RETAINED_WATERMARK_MISMATCH");
    }
    try {
      const replay = await this.#drivers[input.runtime].recoverEvents({
        claim,
        expectedTurnId: recovery.basis.protocolTurnId,
        expectedBindingDigest: recovery.basis.bindingDigest,
        expectedResolvedWaiterId: recovery.basis.invocationId,
        expectedSourceMessageId: recovery.basis.sourceMessageId,
      });
      return { kind: "replay_retained_suffix", replay };
    } catch (error) {
      await claim.abort();
      if (error instanceof DriverRetentionError && error.code === "DRIVER_RETAINED_EVENT_GAP") {
        return { kind: "held_ambiguous", reason: "RETAINED_SUFFIX_INCOMPLETE" };
      }
      throw error;
    }
  }
}

function assertPrivateLease(
  lease: RetainedDriverEventLease,
  attempt: DriverPrivateClaimAttempt,
): void {
  if (
    lease.protocolVersion !== attempt.protocolVersion
    || lease.launchId !== attempt.launchId
    || lease.stateInstanceId !== attempt.stateInstanceId
    || lease.sessionId !== attempt.sessionId
    || lease.ownerToken !== attempt.ownerToken
    || lease.readerEpoch !== attempt.readerEpoch
    || lease.nextOrdinal !== attempt.nextOrdinal
    || lease.lastEventDigest !== attempt.lastEventDigest
    || lease.claimAttemptId !== attempt.claimAttemptId
    || lease.processMode !== attempt.processMode
    || lease.replayMode !== attempt.replayMode
  ) throw new DriverRetentionError("DRIVER_EVENT_FENCE_MISMATCH");
}
