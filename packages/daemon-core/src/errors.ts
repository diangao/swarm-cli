export class PrePermitDisconnectError extends Error {
  constructor(message = "PRE_PERMIT_DISCONNECT") {
    super(message);
    this.name = "PrePermitDisconnectError";
  }
}

export type DaemonCoreErrorCode =
  | "PERMIT_FENCE_MISMATCH"
  | "ACK_RESULT_MISMATCH"
  | "RECONCILIATION_RESULT_MISMATCH"
  | "RECONCILIATION_EVIDENCE_REQUIRED"
  | "TOO_MANY_INVOCATION_GENERATIONS";

export class DaemonCoreError extends Error {
  readonly code: DaemonCoreErrorCode;

  constructor(code: DaemonCoreErrorCode) {
    super(code);
    this.name = "DaemonCoreError";
    this.code = code;
  }
}
