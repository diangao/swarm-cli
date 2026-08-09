// Shared protocol SSOT for the turn/delivery completion envelope. This is the
// single common authority both the daemon-core and storage packages import so
// neither duplicates the binding/digest logic nor imports the other (a package
// boundary violation). Only the contribution builder computes digests; every
// other export is a fail-closed validator over neutral fields.

import { createHash } from "node:crypto";

import { fail } from "./errors.js";
import { canonicalProtocolJson, type JsonObject, type JsonValue } from "./json.js";
import type {
  AgentId,
  ArtifactDigest,
  CommandId,
  DeliveryFence,
  DeliveryId,
  LaunchId,
  MachineId,
  ProducerFactId,
  ProtocolVersion,
  ReceiptId,
  SessionId,
  StateInstanceId,
  TurnId,
} from "./types.js";

// --- neutral primitives (self-contained on json.js + errors.js, mirroring the
// other validate modules in this package) --------------------------------------
const ID_PATTERN =
  /^(srv|mch|agt|hum|chn|cvs|msg|dlv|fac|tsk|clm|lse|lnc|cmd|rcp|sti|trn|ses)_[0-9a-hjkmnp-tv-z]{26}$/u;
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const MAX_ATTEMPT = 2_147_483_647;
const MAX_SAFE = Number.MAX_SAFE_INTEGER;
type JsonInput = JsonValue | undefined;

function object(
  value: JsonInput,
  allowed: readonly string[],
  required: readonly string[] = allowed,
): JsonObject {
  if (value === null || Array.isArray(value) || typeof value !== "object") {
    return fail("INVALID_SCALAR");
  }
  const allowedSet = new Set(allowed);
  // Reflect.ownKeys covers string AND symbol keys, enumerable AND non-enumerable,
  // so a hidden symbol/non-enumerable extra cannot slip past the exact-key check.
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !allowedSet.has(key)) fail("UNKNOWN_FIELD");
  }
  for (const key of required) {
    if (!Object.hasOwn(value, key)) fail("INVALID_SCALAR");
  }
  return value;
}

function text(value: JsonInput): string {
  if (typeof value !== "string" || value.length === 0) return fail("INVALID_SCALAR");
  return value;
}

function integer(value: JsonInput, minimum: number, maximum = MAX_SAFE): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) return fail("INVALID_SCALAR");
  if (value < minimum || value > maximum) return fail("INVALID_SCALAR");
  return value;
}

function id<Id extends string>(value: JsonInput, prefix: string): Id {
  const parsed = text(value);
  if (!ID_PATTERN.test(parsed) || !parsed.startsWith(`${prefix}_`)) return fail("INVALID_SCALAR");
  return parsed as Id;
}

function digest(value: JsonInput): ArtifactDigest {
  const parsed = text(value);
  if (!DIGEST_PATTERN.test(parsed)) return fail("INVALID_SCALAR");
  return parsed as ArtifactDigest;
}

function digestValue(value: unknown): ArtifactDigest {
  const hash = createHash("sha256").update(canonicalProtocolJson(value)).digest("hex");
  return `sha256:${hash}` as ArtifactDigest;
}

function protocolVersion(value: JsonInput): ProtocolVersion {
  return integer(value, 1, 999_999) as ProtocolVersion;
}

const FENCE_KEYS = [
  "protocolVersion",
  "deliveryId",
  "attempt",
  "producerFactId",
  "agentId",
  "machineId",
  "launchId",
  "membershipEpoch",
  "routingGeneration",
  "routeVersion",
  "sessionId",
  "turnId",
] as const;

/** Exact 12-field brand/shape validation of a DeliveryFence. */
function deliveryFence(value: JsonInput): DeliveryFence {
  const parsed = object(value, FENCE_KEYS);
  return {
    protocolVersion: protocolVersion(parsed.protocolVersion),
    deliveryId: id<DeliveryId>(parsed.deliveryId, "dlv"),
    attempt: integer(parsed.attempt, 1, MAX_ATTEMPT),
    producerFactId: id<ProducerFactId>(parsed.producerFactId, "fac"),
    agentId: id<AgentId>(parsed.agentId, "agt"),
    machineId: id<MachineId>(parsed.machineId, "mch"),
    launchId: id<LaunchId>(parsed.launchId, "lnc"),
    membershipEpoch: integer(parsed.membershipEpoch, 1),
    routingGeneration: integer(parsed.routingGeneration, 0),
    routeVersion: integer(parsed.routeVersion, 1),
    sessionId: id<SessionId>(parsed.sessionId, "ses"),
    turnId: id<TurnId>(parsed.turnId, "trn"),
  };
}

// --- Contribution binding ------------------------------------------------------
export type ContributionBinding = {
  fence: DeliveryFence;
  deliveryFenceDigest: ArtifactDigest;
  stateInstanceId: StateInstanceId;
  inputOrdinal: number;
  invocationId: CommandId;
  invocationGeneration: number;
  permitId: CommandId;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
  contributionBindingDigest: ArtifactDigest;
};

/** The builder's input: the binding fields with neither digest supplied. */
export type ContributionBindingInput = Omit<
  ContributionBinding,
  "deliveryFenceDigest" | "contributionBindingDigest"
>;

/**
 * Compute the exact 8-key projection the contribution digest binds over. The
 * fence contributes ONLY through its own digest, so any fence field change (e.g.
 * a different session/launch/route/turn) changes deliveryFenceDigest and hence
 * this digest. Canonical JSON makes the digest independent of key order.
 */
function contributionDigest(binding: {
  deliveryFenceDigest: ArtifactDigest;
  stateInstanceId: StateInstanceId;
  inputOrdinal: number;
  invocationId: CommandId;
  invocationGeneration: number;
  permitId: CommandId;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
}): ArtifactDigest {
  return digestValue({
    deliveryFenceDigest: binding.deliveryFenceDigest,
    stateInstanceId: binding.stateInstanceId,
    inputOrdinal: binding.inputOrdinal,
    invocationId: binding.invocationId,
    invocationGeneration: binding.invocationGeneration,
    permitId: binding.permitId,
    runtimeWriteId: binding.runtimeWriteId,
    visibilityEventId: binding.visibilityEventId,
  });
}

/**
 * The ONE canonical builder. Validates brands, a safe-positive invocation
 * generation, and a nonnegative safe input ordinal, then computes both digests.
 */
export function buildContributionBinding(input: ContributionBindingInput): ContributionBinding {
  const fence = deliveryFence(input.fence as unknown as JsonValue);
  const stateInstanceId = id<StateInstanceId>(input.stateInstanceId, "sti");
  const inputOrdinal = integer(input.inputOrdinal, 0);
  const invocationId = id<CommandId>(input.invocationId, "cmd");
  const invocationGeneration = integer(input.invocationGeneration, 1);
  const permitId = id<CommandId>(input.permitId, "cmd");
  const runtimeWriteId = id<CommandId>(input.runtimeWriteId, "cmd");
  const visibilityEventId = id<CommandId>(input.visibilityEventId, "cmd");
  const deliveryFenceDigest = digestValue(fence);
  const contributionBindingDigest = contributionDigest({
    deliveryFenceDigest,
    stateInstanceId,
    inputOrdinal,
    invocationId,
    invocationGeneration,
    permitId,
    runtimeWriteId,
    visibilityEventId,
  });
  return {
    fence,
    deliveryFenceDigest,
    stateInstanceId,
    inputOrdinal,
    invocationId,
    invocationGeneration,
    permitId,
    runtimeWriteId,
    visibilityEventId,
    contributionBindingDigest,
  };
}

const CONTRIBUTION_KEYS = [
  "fence",
  "deliveryFenceDigest",
  "stateInstanceId",
  "inputOrdinal",
  "invocationId",
  "invocationGeneration",
  "permitId",
  "runtimeWriteId",
  "visibilityEventId",
  "contributionBindingDigest",
] as const;

export type VerifyOptions = {
  /**
   * An independently stored expected fence. When present, a binding whose fence
   * is internally consistent but does not equal this fence is rejected — a forged
   * fence cannot pass by carrying its own matching digests.
   */
  expectedFence?: DeliveryFence;
};

/**
 * Fail-closed verifier: exact keys, brand checks, and BOTH digests recomputed
 * from the fields (so a tampered projection field or a forged stored digest is
 * rejected). With an expected fence, a consistent-but-wrong fence is rejected.
 */
export function verifyContributionBinding(
  value: unknown,
  options: VerifyOptions = {},
): ContributionBinding {
  const parsed = object(value as JsonValue, CONTRIBUTION_KEYS);
  const fence = deliveryFence(parsed.fence);
  const storedFenceDigest = digest(parsed.deliveryFenceDigest);
  const stateInstanceId = id<StateInstanceId>(parsed.stateInstanceId, "sti");
  const inputOrdinal = integer(parsed.inputOrdinal, 0);
  const invocationId = id<CommandId>(parsed.invocationId, "cmd");
  const invocationGeneration = integer(parsed.invocationGeneration, 1);
  const permitId = id<CommandId>(parsed.permitId, "cmd");
  const runtimeWriteId = id<CommandId>(parsed.runtimeWriteId, "cmd");
  const visibilityEventId = id<CommandId>(parsed.visibilityEventId, "cmd");
  const storedBindingDigest = digest(parsed.contributionBindingDigest);

  const deliveryFenceDigest = digestValue(fence);
  if (deliveryFenceDigest !== storedFenceDigest) fail("INVARIANT_VIOLATION");
  const contributionBindingDigest = contributionDigest({
    deliveryFenceDigest,
    stateInstanceId,
    inputOrdinal,
    invocationId,
    invocationGeneration,
    permitId,
    runtimeWriteId,
    visibilityEventId,
  });
  if (contributionBindingDigest !== storedBindingDigest) fail("INVARIANT_VIOLATION");

  if (options.expectedFence !== undefined) {
    const expectedDigest = digestValue(deliveryFence(options.expectedFence as unknown as JsonValue));
    if (expectedDigest !== deliveryFenceDigest) fail("INVARIANT_VIOLATION");
  }

  return {
    fence,
    deliveryFenceDigest,
    stateInstanceId,
    inputOrdinal,
    invocationId,
    invocationGeneration,
    permitId,
    runtimeWriteId,
    visibilityEventId,
    contributionBindingDigest,
  };
}

// --- Adjacent completion types (shared so downstream lanes don't duplicate) ----
export type TurnReplyResult = {
  receiptId: ReceiptId;
  resultDigest: ArtifactDigest;
};

export type TurnCoordinationDisposition =
  | { kind: "not_requested"; terminalTurnId: TurnId }
  | { kind: "committed"; commandId: CommandId; receiptId: ReceiptId; resultDigest: ArtifactDigest }
  | { kind: "terminal_replay"; commandId: CommandId; receiptId: ReceiptId; resultDigest: ArtifactDigest };

export type TurnCompletionEvidence = {
  contribution: ContributionBinding;
  reply: TurnReplyResult;
  coordination: TurnCoordinationDisposition;
};

/** Exact-key validation of the reply receipt/result pair. */
export function parseTurnReplyResult(value: unknown): TurnReplyResult {
  const parsed = object(value as JsonValue, ["receiptId", "resultDigest"]);
  return {
    receiptId: id<ReceiptId>(parsed.receiptId, "rcp"),
    resultDigest: digest(parsed.resultDigest),
  };
}

/**
 * Exact-key/tagged-union validation. A command id is present ONLY on the
 * committed/terminal_replay variants; the receipt is a receipt-branded id (a
 * CommandId-shaped value is rejected); an unknown kind fails closed.
 */
export function parseTurnCoordinationDisposition(value: unknown): TurnCoordinationDisposition {
  const discriminant = object(
    value as JsonValue,
    ["kind", "terminalTurnId", "commandId", "receiptId", "resultDigest"],
    ["kind"],
  );
  const kind = text(discriminant.kind);
  if (kind === "not_requested") {
    const parsed = object(value as JsonValue, ["kind", "terminalTurnId"]);
    return { kind, terminalTurnId: id<TurnId>(parsed.terminalTurnId, "trn") };
  }
  if (kind === "committed" || kind === "terminal_replay") {
    const parsed = object(value as JsonValue, ["kind", "commandId", "receiptId", "resultDigest"]);
    return {
      kind,
      commandId: id<CommandId>(parsed.commandId, "cmd"),
      receiptId: id<ReceiptId>(parsed.receiptId, "rcp"),
      resultDigest: digest(parsed.resultDigest),
    };
  }
  return fail("UNSUPPORTED_VARIANT");
}

/**
 * Validate a full completion envelope. The contribution is verified through the
 * contribution verifier (optionally against an independently expected fence); the
 * reply and coordination are validated by their exact-key validators.
 *
 * The completion envelope additionally enforces the same-terminal-turn fence: a
 * `not_requested` coordination must name the SAME turn the contribution's fence
 * is bound to, so a sibling terminal turn cannot ride a valid contribution. The
 * coordination parser stays structural and independent of this cross-field rule.
 */
export function verifyTurnCompletionEvidence(
  value: unknown,
  options: VerifyOptions = {},
): TurnCompletionEvidence {
  const parsed = object(value as JsonValue, ["contribution", "reply", "coordination"]);
  const contribution = verifyContributionBinding(parsed.contribution, options);
  const reply = parseTurnReplyResult(parsed.reply);
  const coordination = parseTurnCoordinationDisposition(parsed.coordination);
  if (
    coordination.kind === "not_requested" &&
    coordination.terminalTurnId !== contribution.fence.turnId
  ) {
    fail("INVARIANT_VIOLATION");
  }
  return { contribution, reply, coordination };
}
