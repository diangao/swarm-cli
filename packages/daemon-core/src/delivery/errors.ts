export type DeliveryKernelErrorCode =
  | "ACTIVATION_PREDECESSOR_REQUIRED"
  | "DELIVERY_ORDER_INVALID"
  | "STALE_DELIVERY_FENCE"
  | "DELIVERY_EXECUTION_FENCE_MISMATCH"
  | "MODEL_VISIBLE_PREDECESSOR_REQUIRED"
  | "VISIBLE_MESSAGE_REPLAY_CONFLICT"
  | "SERVER_COMMIT_EVIDENCE_MISMATCH"
  | "EMPTY_NATIVE_REPLY";

export class DeliveryKernelError extends Error {
  readonly code: DeliveryKernelErrorCode;

  constructor(code: DeliveryKernelErrorCode) {
    super(code);
    this.name = "DeliveryKernelError";
    this.code = code;
  }
}
