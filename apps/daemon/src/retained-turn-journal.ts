import type { RuntimeJournalTransaction, TurnStepResult } from "@swarm/storage";
import {
  DriverRetentionError,
  type DriverPrivateEventRetentionPort,
  type RetainedDriverEventLease,
} from "@swarm/drivers";

/**
 * Sole app owner of the storage-commit -> private-ACK ordering.  In particular,
 * it ACKs the cursor returned by storage, never a cursor reconstructed from an
 * old replay input.
 */
export class RetainedTurnJournal {
  readonly #retention: DriverPrivateEventRetentionPort;

  constructor(retention: DriverPrivateEventRetentionPort) {
    this.#retention = retention;
  }

  commitStep(
    transaction: RuntimeJournalTransaction,
    lease: RetainedDriverEventLease,
    input: Parameters<RuntimeJournalTransaction["commitTurnStep"]>[0],
  ): TurnStepResult {
    assertLeaseFence(lease, input);
    const result = transaction.commitTurnStep(input);
    this.#retention.acknowledgeCommitted(lease, {
      stateInstanceId: lease.stateInstanceId,
      sessionId: lease.sessionId,
      nextOrdinal: result.nextOrdinal,
      lastEventDigest: result.lastEventDigest,
    });
    return result;
  }

  commitTerminal(
    transaction: RuntimeJournalTransaction,
    lease: RetainedDriverEventLease,
    input: Parameters<RuntimeJournalTransaction["commitTurnTerminal"]>[0],
  ): TurnStepResult {
    assertLeaseFence(lease, input);
    const result = transaction.commitTurnTerminal(input);
    this.#retention.acknowledgeCommitted(lease, {
      stateInstanceId: lease.stateInstanceId,
      sessionId: lease.sessionId,
      nextOrdinal: result.nextOrdinal,
      lastEventDigest: result.lastEventDigest,
    });
    return result;
  }
}

function assertLeaseFence(
  lease: RetainedDriverEventLease,
  input: { stateInstanceId: string; sessionId: string; ownerToken: string; readerEpoch: number },
): void {
  if (
    input.stateInstanceId !== lease.stateInstanceId
    || input.sessionId !== lease.sessionId
    || input.ownerToken !== lease.ownerToken
    || input.readerEpoch !== lease.readerEpoch
  ) throw new DriverRetentionError("DRIVER_EVENT_FENCE_MISMATCH");
}
