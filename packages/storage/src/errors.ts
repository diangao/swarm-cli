export type StorageErrorCode =
  | "DATABASE_UNAVAILABLE"
  | "DATABASE_VERSION_UNSUPPORTED"
  | "IDEMPOTENCY_CONFLICT"
  | "INVALID_DATABASE_TARGET"
  | "INVALID_IDENTIFIER"
  | "INVALID_MIGRATION"
  | "INVALID_STATE_TRANSITION"
  | "JOURNAL_LOCKED"
  | "MIGRATION_CHECKSUM_MISMATCH"
  | "MIGRATION_FAILED"
  | "OUTBOX_STALE_ATTEMPT"
  | "STALE_FENCE"
  | "EMPTY_MESSAGE"
  | "STALE_DELIVERY_FENCE"
  | "PERMIT_REQUIRED"
  | "PERMIT_MISMATCH"
  | "INVALID_JOURNAL_CHAIN"
  | "FAKE_NOT_WRITTEN_PROOF_REQUIRED"
  | "REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN"
  | "BOUNDARY_REGRESSION"
  | "RECONCILIATION_STATE_CONFLICT"
  | "STALE_INVOCATION_GENERATION"
  | "INVOCATION_STATE_CONFLICT"
  | "WRITE_STARTED_BINDING_MISMATCH"
  | "ACK_PREDECESSOR_REQUIRED"
  | "MODEL_VISIBLE_PREDECESSOR_REQUIRED"
  | "MEMBERSHIP_REVOKED_BEFORE_CONSUME"
  | "ROUTE_SUPERSEDED_BEFORE_CONSUME"
  | "SECOND_COORDINATION_CALL"
  | "STALE_REMINDER_GENERATION"
  | "TASK_GRAPH_CYCLE";

export class StorageError extends Error {
  readonly code: StorageErrorCode;
  readonly causeDetail?: unknown;

  constructor(code: StorageErrorCode, causeDetail?: unknown) {
    super(code);
    this.name = "StorageError";
    this.code = code;
    this.causeDetail = causeDetail;
  }
}

export function storageFail(code: StorageErrorCode, causeDetail?: unknown): never {
  throw new StorageError(code, causeDetail);
}
