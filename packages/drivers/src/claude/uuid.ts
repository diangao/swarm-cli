import { createHash } from "node:crypto";

import { canonicalProtocolJson, type DriverTurnBinding, type SessionId } from "@swarm/protocol";

import { DriverNormalizationError } from "../normalizer.js";
import { CLAUDE_INPUT_UUID_NAMESPACE } from "./constants.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function claudeInputUuid(sessionId: SessionId, binding: DriverTurnBinding): string {
  const name = canonicalProtocolJson({
    attempt: binding.delivery.attempt,
    deliveryId: binding.delivery.deliveryId,
    inputDigest: binding.inputDigest,
    inputOrdinal: binding.inputOrdinal,
    invocationId: binding.invocation.invocationId,
    sessionId,
  });
  return uuidV5(CLAUDE_INPUT_UUID_NAMESPACE, decoder.decode(name));
}

export function claudeRuntimeSessionUuid(sessionId: SessionId): string {
  return uuidV5(CLAUDE_INPUT_UUID_NAMESPACE, `runtime-session:${sessionId}`);
}

export function uuidV5(namespace: string, name: string): string {
  const namespaceBytes = parseUuid(namespace);
  const digest = createHash("sha1").update(namespaceBytes).update(encoder.encode(name)).digest();
  const bytes = new Uint8Array(digest.subarray(0, 16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Buffer.from(bytes).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseUuid(value: string): Uint8Array {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value)) {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
  return new Uint8Array(Buffer.from(value.replaceAll("-", ""), "hex"));
}
