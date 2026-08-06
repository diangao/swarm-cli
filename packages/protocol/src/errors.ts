export type ProtocolErrorCode =
  | "INVALID_JSON"
  | "PAYLOAD_TOO_LARGE"
  | "DUPLICATE_KEY"
  | "UNKNOWN_FIELD"
  | "INVALID_SCALAR"
  | "UNSUPPORTED_VARIANT"
  | "INVARIANT_VIOLATION"
  | "PROTOCOL_VERSION_UNSUPPORTED"
  | "PROTOCOL_VERSION_NOT_NEGOTIATED"
  | "IDEMPOTENCY_CONFLICT";

export function fail(code: ProtocolErrorCode): never {
  const error = new Error(code);
  error.name = "ProtocolError";
  throw error;
}
