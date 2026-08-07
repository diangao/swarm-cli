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
