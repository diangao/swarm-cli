import {
  canonicalProtocolJson,
  parseDeliveryEnvelope,
  parseTaskLease,
  parseTransitionReceipt,
  type ArtifactDigest,
  type DeliveryEnvelope,
  type ProtocolVersion,
  type Target,
  type TaskLease,
  type TransitionReceipt,
} from "@swarm/protocol";
import { storageFail } from "./errors.js";

const ID_PREFIXES = [
  "srv",
  "mch",
  "agt",
  "chn",
  "cvs",
  "msg",
  "dlv",
  "fac",
  "tsk",
  "clm",
  "lse",
  "lnc",
  "cmd",
  "rcp",
  "sti",
  "trn",
  "ses",
] as const;

export type ProtocolIdPrefix = (typeof ID_PREFIXES)[number];

const ID_PATTERN = /^(srv|mch|agt|chn|cvs|msg|dlv|fac|tsk|clm|lse|lnc|cmd|rcp|sti|trn|ses)_[0-9a-hjkmnp-tv-z]{26}$/u;
const FENCE_PATTERN = /^fnc_[0-9a-hjkmnp-tv-z]{26}$/u;
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const SYNTHETIC = "00000000000000000000000000";

export function assertProtocolId(value: string, prefix: ProtocolIdPrefix): string {
  if (!ID_PATTERN.test(value) || !value.startsWith(`${prefix}_`)) {
    return storageFail("INVALID_IDENTIFIER", { value, prefix });
  }
  return value;
}

export function assertFenceToken(value: string): string {
  if (!FENCE_PATTERN.test(value)) return storageFail("INVALID_IDENTIFIER", { value });
  return value;
}

export function assertArtifactDigest(value: string): ArtifactDigest {
  if (!DIGEST_PATTERN.test(value)) return storageFail("INVALID_IDENTIFIER", { value });
  return value as ArtifactDigest;
}

export function parseStrictTarget(value: Target): Target {
  const envelope = {
    protocolVersion: 1,
    deliveryId: `dlv_${SYNTHETIC}`,
    attempt: 1,
    messageId: `msg_${SYNTHETIC}`,
    target: value,
    serverSeq: 1,
    producerFactId: `fac_${SYNTHETIC}`,
    agentId: `agt_${SYNTHETIC}`,
    machineId: `mch_${SYNTHETIC}`,
  };
  return parseDeliveryEnvelope(
    canonicalProtocolJson(envelope),
    1 as ProtocolVersion,
  ).target;
}

export function canonicalTargetKey(value: Target): string {
  const parsed = parseStrictTarget(value);
  return new TextDecoder().decode(canonicalProtocolJson(parsed));
}

export function parseFrozenDelivery(input: Uint8Array): DeliveryEnvelope {
  return parseDeliveryEnvelope(input, 1 as ProtocolVersion);
}

export function parseFrozenTaskLease(input: Uint8Array): TaskLease {
  return parseTaskLease(input);
}

export function parseFrozenTransitionReceipt(input: Uint8Array): TransitionReceipt {
  return parseTransitionReceipt(input, 1 as ProtocolVersion);
}

export function targetColumns(target: Target): {
  kind: "channel" | "direct";
  ownerId: string;
  threadRootMessageId: string | null;
} {
  const parsed = parseStrictTarget(target);
  return parsed.kind === "channel"
    ? {
        kind: parsed.kind,
        ownerId: parsed.channelId,
        threadRootMessageId: parsed.threadRootMessageId ?? null,
      }
    : {
        kind: parsed.kind,
        ownerId: parsed.conversationId,
        threadRootMessageId: parsed.threadRootMessageId ?? null,
      };
}
