import { fail } from "./errors.js";
import { parseProtocolJson, type JsonObject, type JsonValue } from "./json.js";
import type {
  AgentId,
  ArtifactDigest,
  ChannelId,
  ClaimId,
  CommandId,
  ConversationId,
  DeliveryEnvelope,
  DeliveryId,
  FenceToken,
  LaunchCommand,
  LaunchId,
  LeaseId,
  MachineId,
  MessageId,
  ProducerFactId,
  ProtocolSupport,
  ProtocolVersion,
  ReceiptId,
  RuntimeKind,
  ServerId,
  SessionId,
  StateInstanceId,
  Target,
  TaskId,
  TaskLease,
  TransitionReceipt,
  TurnId,
} from "./types.js";

const ID_PATTERN = /^(srv|mch|agt|chn|cvs|msg|dlv|fac|tsk|clm|lse|lnc|cmd|rcp|sti|trn|ses)_[0-9a-hjkmnp-tv-z]{26}$/u;
const FENCE_PATTERN = /^fnc_[0-9a-hjkmnp-tv-z]{26}$/u;
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const TIMESTAMP_PATTERN = /^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}Z$/u;
const MAX_SAFE = Number.MAX_SAFE_INTEGER;
const MAX_ATTEMPT = 2_147_483_647;

type StrictObject<Allowed extends string, Required extends Allowed> = JsonObject &
  Record<Required, JsonValue> &
  Partial<Record<Exclude<Allowed, Required>, JsonValue>>;

function object<Allowed extends string>(
  value: JsonValue,
  allowed: readonly Allowed[],
): StrictObject<Allowed, Allowed>;
function object<Allowed extends string, Required extends Allowed>(
  value: JsonValue,
  allowed: readonly Allowed[],
  required: readonly Required[],
): StrictObject<Allowed, Required>;
function object(
  value: JsonValue,
  allowed: readonly string[],
  required: readonly string[] = allowed,
): JsonObject {
  if (value === null || Array.isArray(value) || typeof value !== "object") {
    return fail("INVALID_SCALAR");
  }
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(value)) {
    if (!allowedSet.has(key)) fail("UNKNOWN_FIELD");
  }
  for (const key of required) {
    if (!Object.hasOwn(value, key)) fail("INVALID_SCALAR");
  }
  return value;
}

function present(value: JsonValue | undefined): JsonValue {
  if (value === undefined) return fail("INVALID_SCALAR");
  return value;
}

function string(value: JsonValue): string {
  if (typeof value !== "string") return fail("INVALID_SCALAR");
  return value;
}

function integer(value: JsonValue, minimum: number, maximum = MAX_SAFE): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    return fail("INVALID_SCALAR");
  }
  if (value < minimum || value > maximum) return fail("INVALID_SCALAR");
  return value;
}

function id<Id extends string>(value: JsonValue, prefix: string): Id {
  const parsed = string(value);
  if (!ID_PATTERN.test(parsed) || !parsed.startsWith(`${prefix}_`)) {
    return fail("INVALID_SCALAR");
  }
  return parsed as Id;
}

function fenceToken(value: JsonValue): FenceToken {
  const parsed = string(value);
  if (!FENCE_PATTERN.test(parsed)) return fail("INVALID_SCALAR");
  return parsed as FenceToken;
}

function artifactDigest(value: JsonValue): ArtifactDigest {
  const parsed = string(value);
  if (!DIGEST_PATTERN.test(parsed)) return fail("INVALID_SCALAR");
  return parsed as ArtifactDigest;
}

function timestamp(value: JsonValue): string {
  const parsed = string(value);
  if (!TIMESTAMP_PATTERN.test(parsed)) return fail("INVALID_SCALAR");
  const date = new Date(parsed);
  if (!Number.isFinite(date.valueOf()) || date.toISOString() !== parsed) {
    return fail("INVALID_SCALAR");
  }
  return parsed;
}

function protocolVersion(value: JsonValue): ProtocolVersion {
  const parsed = integer(value, 1, 999_999);
  const major = Math.floor(parsed / 1000);
  const minor = parsed % 1000;
  if (major > 999 || minor > 999) return fail("INVALID_SCALAR");
  return parsed as ProtocolVersion;
}

function enforceNegotiated(
  actual: ProtocolVersion,
  negotiatedVersion: ProtocolVersion,
): void {
  if (negotiatedVersion !== 1) fail("PROTOCOL_VERSION_UNSUPPORTED");
  if (actual !== negotiatedVersion) {
    fail("PROTOCOL_VERSION_NOT_NEGOTIATED");
  }
}

function target(value: JsonValue): Target {
  const base = object(
    value,
    ["kind", "channelId", "conversationId", "threadRootMessageId"],
    ["kind"],
  );
  const kind = string(base.kind);
  if (kind === "channel") {
    const parsed = object(
      value,
      ["kind", "channelId", "threadRootMessageId"],
      ["kind", "channelId"],
    );
    const channelId = id<ChannelId>(parsed.channelId, "chn");
    if (Object.hasOwn(parsed, "threadRootMessageId")) {
      return {
        kind,
        channelId,
        threadRootMessageId: id<MessageId>(present(parsed.threadRootMessageId), "msg"),
      };
    }
    return { kind, channelId };
  }
  if (kind === "direct") {
    const parsed = object(
      value,
      ["kind", "conversationId", "threadRootMessageId"],
      ["kind", "conversationId"],
    );
    const conversationId = id<ConversationId>(parsed.conversationId, "cvs");
    if (Object.hasOwn(parsed, "threadRootMessageId")) {
      return {
        kind,
        conversationId,
        threadRootMessageId: id<MessageId>(present(parsed.threadRootMessageId), "msg"),
      };
    }
    return { kind, conversationId };
  }
  return fail("UNSUPPORTED_VARIANT");
}

export function parseDeliveryEnvelope(
  input: Uint8Array,
  negotiatedVersion: ProtocolVersion,
): DeliveryEnvelope {
  const parsed = object(
    parseProtocolJson(input),
    [
      "protocolVersion",
      "deliveryId",
      "attempt",
      "messageId",
      "target",
      "serverSeq",
      "producerFactId",
      "agentId",
      "machineId",
      "expectedLaunchId",
      "replayOf",
    ],
    [
      "protocolVersion",
      "deliveryId",
      "attempt",
      "messageId",
      "target",
      "serverSeq",
      "producerFactId",
      "agentId",
      "machineId",
    ],
  );
  const version = protocolVersion(parsed.protocolVersion);
  enforceNegotiated(version, negotiatedVersion);
  const deliveryId = id<DeliveryId>(parsed.deliveryId, "dlv");
  const attempt = integer(parsed.attempt, 1, MAX_ATTEMPT);
  const replayOf = Object.hasOwn(parsed, "replayOf")
    ? id<DeliveryId>(present(parsed.replayOf), "dlv")
    : undefined;
  if ((attempt === 1 && replayOf !== undefined) || (attempt > 1 && replayOf === undefined)) {
    fail("INVARIANT_VIOLATION");
  }
  if (replayOf === deliveryId) fail("INVARIANT_VIOLATION");
  const result: DeliveryEnvelope = {
    protocolVersion: version,
    deliveryId,
    attempt,
    messageId: id<MessageId>(parsed.messageId, "msg"),
    target: target(parsed.target),
    serverSeq: integer(parsed.serverSeq, 1),
    producerFactId: id<ProducerFactId>(parsed.producerFactId, "fac"),
    agentId: id<AgentId>(parsed.agentId, "agt"),
    machineId: id<MachineId>(parsed.machineId, "mch"),
  };
  if (Object.hasOwn(parsed, "expectedLaunchId")) {
    result.expectedLaunchId = id<LaunchId>(present(parsed.expectedLaunchId), "lnc");
  }
  if (replayOf !== undefined) result.replayOf = replayOf;
  return result;
}

export function parseTaskLease(input: Uint8Array): TaskLease {
  const parsed = object(parseProtocolJson(input), [
    "taskId",
    "claimId",
    "leaseId",
    "leaseEpoch",
    "fenceToken",
    "attempt",
    "acquiredAt",
    "expiresAt",
  ]);
  const acquiredAt = timestamp(parsed.acquiredAt);
  const expiresAt = timestamp(parsed.expiresAt);
  if (expiresAt <= acquiredAt) fail("INVARIANT_VIOLATION");
  return {
    taskId: id<TaskId>(parsed.taskId, "tsk"),
    claimId: id<ClaimId>(parsed.claimId, "clm"),
    leaseId: id<LeaseId>(parsed.leaseId, "lse"),
    leaseEpoch: integer(parsed.leaseEpoch, 1),
    fenceToken: fenceToken(parsed.fenceToken),
    attempt: integer(parsed.attempt, 1, MAX_ATTEMPT),
    acquiredAt,
    expiresAt,
  };
}

function runtimeKind(value: JsonValue): RuntimeKind {
  const parsed = string(value);
  if (parsed !== "codex" && parsed !== "claude") return fail("UNSUPPORTED_VARIANT");
  return parsed;
}

export function parseLaunchCommand(
  input: Uint8Array,
  negotiatedVersion: ProtocolVersion,
): LaunchCommand {
  const parsed = object(
    parseProtocolJson(input),
    [
      "protocolVersion",
      "commandId",
      "agentId",
      "machineId",
      "launchId",
      "runtime",
      "workspaceGeneration",
      "wakeDeliveryId",
    ],
    [
      "protocolVersion",
      "commandId",
      "agentId",
      "machineId",
      "launchId",
      "runtime",
      "workspaceGeneration",
    ],
  );
  const version = protocolVersion(parsed.protocolVersion);
  enforceNegotiated(version, negotiatedVersion);
  const result: LaunchCommand = {
    protocolVersion: version,
    commandId: id<CommandId>(parsed.commandId, "cmd"),
    agentId: id<AgentId>(parsed.agentId, "agt"),
    machineId: id<MachineId>(parsed.machineId, "mch"),
    launchId: id<LaunchId>(parsed.launchId, "lnc"),
    runtime: runtimeKind(parsed.runtime),
    workspaceGeneration: integer(parsed.workspaceGeneration, 1),
  };
  if (Object.hasOwn(parsed, "wakeDeliveryId")) {
    result.wakeDeliveryId = id<DeliveryId>(present(parsed.wakeDeliveryId), "dlv");
  }
  return result;
}

type ServerActorValue = { serverId: ServerId };
type ClaimActorValue = { serverId: ServerId; agentId: AgentId };
type DaemonActorValue = { machineId: MachineId; agentId: AgentId };
type EmptyFenceValue = Record<string, never>;
type LeaseFenceValue = { leaseEpoch: number; fenceToken: FenceToken };
type LaunchFenceValue = { launchId: LaunchId; stateInstanceId: StateInstanceId };
type SessionFenceValue = LaunchFenceValue & { sessionId: SessionId };
type TurnFenceValue = SessionFenceValue & { turnId: TurnId };
type LeaseTurnFenceValue = LeaseFenceValue & TurnFenceValue;

function serverActor(value: JsonValue): ServerActorValue {
  const parsed = object(value, ["serverId"]);
  return { serverId: id<ServerId>(parsed.serverId, "srv") };
}

function claimActor(value: JsonValue): ClaimActorValue {
  const parsed = object(value, ["serverId", "agentId"]);
  return {
    serverId: id<ServerId>(parsed.serverId, "srv"),
    agentId: id<AgentId>(parsed.agentId, "agt"),
  };
}

function daemonActor(value: JsonValue): DaemonActorValue {
  const parsed = object(value, ["machineId", "agentId"]);
  return {
    machineId: id<MachineId>(parsed.machineId, "mch"),
    agentId: id<AgentId>(parsed.agentId, "agt"),
  };
}

function emptyFence(value: JsonValue): EmptyFenceValue {
  object(value, [], []);
  return {};
}

function leaseFence(value: JsonValue): LeaseFenceValue {
  const parsed = object(value, ["leaseEpoch", "fenceToken"]);
  return {
    leaseEpoch: integer(parsed.leaseEpoch, 1),
    fenceToken: fenceToken(parsed.fenceToken),
  };
}

function launchFence(value: JsonValue): LaunchFenceValue {
  const parsed = object(value, ["launchId", "stateInstanceId"]);
  return {
    launchId: id<LaunchId>(parsed.launchId, "lnc"),
    stateInstanceId: id<StateInstanceId>(parsed.stateInstanceId, "sti"),
  };
}

function sessionFence(value: JsonValue): SessionFenceValue {
  const parsed = object(value, ["launchId", "stateInstanceId", "sessionId"]);
  return {
    launchId: id<LaunchId>(parsed.launchId, "lnc"),
    stateInstanceId: id<StateInstanceId>(parsed.stateInstanceId, "sti"),
    sessionId: id<SessionId>(parsed.sessionId, "ses"),
  };
}

function turnFence(value: JsonValue): TurnFenceValue {
  const parsed = object(value, ["launchId", "stateInstanceId", "turnId", "sessionId"]);
  return {
    launchId: id<LaunchId>(parsed.launchId, "lnc"),
    stateInstanceId: id<StateInstanceId>(parsed.stateInstanceId, "sti"),
    turnId: id<TurnId>(parsed.turnId, "trn"),
    sessionId: id<SessionId>(parsed.sessionId, "ses"),
  };
}

function leaseTurnFence(value: JsonValue): LeaseTurnFenceValue {
  const parsed = object(value, [
    "leaseEpoch",
    "fenceToken",
    "launchId",
    "stateInstanceId",
    "turnId",
    "sessionId",
  ]);
  return {
    leaseEpoch: integer(parsed.leaseEpoch, 1),
    fenceToken: fenceToken(parsed.fenceToken),
    launchId: id<LaunchId>(parsed.launchId, "lnc"),
    stateInstanceId: id<StateInstanceId>(parsed.stateInstanceId, "sti"),
    turnId: id<TurnId>(parsed.turnId, "trn"),
    sessionId: id<SessionId>(parsed.sessionId, "ses"),
  };
}

function sideEffectFence(value: JsonValue): TurnFenceValue | LeaseTurnFenceValue {
  const parsed = object(
    value,
    ["leaseEpoch", "fenceToken", "launchId", "stateInstanceId", "turnId", "sessionId"],
    ["launchId", "stateInstanceId", "turnId", "sessionId"],
  );
  const hasEpoch = Object.hasOwn(parsed, "leaseEpoch");
  const hasToken = Object.hasOwn(parsed, "fenceToken");
  if (hasEpoch !== hasToken) fail("INVARIANT_VIOLATION");
  return hasEpoch ? leaseTurnFence(value) : turnFence(value);
}

export function parseTransitionReceipt(
  input: Uint8Array,
  negotiatedVersion: ProtocolVersion,
): TransitionReceipt {
  const parsed = object(
    parseProtocolJson(input),
    [
      "protocolVersion",
      "receiptId",
      "kind",
      "producerFactId",
      "actor",
      "fence",
      "artifactDigest",
      "occurredAt",
    ],
    ["protocolVersion", "receiptId", "kind", "producerFactId", "actor", "fence", "occurredAt"],
  );
  const version = protocolVersion(parsed.protocolVersion);
  enforceNegotiated(version, negotiatedVersion);
  const common = {
    protocolVersion: version,
    receiptId: id<ReceiptId>(parsed.receiptId, "rcp"),
    producerFactId: id<ProducerFactId>(parsed.producerFactId, "fac"),
    occurredAt: timestamp(parsed.occurredAt),
  };
  const kind = string(parsed.kind);
  const hasDigest = Object.hasOwn(parsed, "artifactDigest");
  switch (kind) {
    case "server_accepted":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: serverActor(parsed.actor), fence: emptyFence(parsed.fence) };
    case "claim_won":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: claimActor(parsed.actor), fence: leaseFence(parsed.fence) };
    case "daemon_accepted":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: daemonActor(parsed.actor), fence: emptyFence(parsed.fence) };
    case "process_spawned":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: daemonActor(parsed.actor), fence: launchFence(parsed.fence) };
    case "runtime_ready":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: daemonActor(parsed.actor), fence: sessionFence(parsed.fence) };
    case "input_written":
    case "model_visible":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: daemonActor(parsed.actor), fence: turnFence(parsed.fence) };
    case "side_effect_applied":
      if (hasDigest) fail("INVARIANT_VIOLATION");
      return { ...common, kind, actor: daemonActor(parsed.actor), fence: sideEffectFence(parsed.fence) };
    case "artifact_published":
    case "review_verdict":
      if (!hasDigest) fail("INVALID_SCALAR");
      return {
        ...common,
        kind,
        actor: daemonActor(parsed.actor),
        fence: leaseTurnFence(parsed.fence),
        artifactDigest: artifactDigest(present(parsed.artifactDigest)),
      } as TransitionReceipt;
    default:
      return fail("UNSUPPORTED_VARIANT");
  }
}

function validSupport(value: ProtocolSupport): boolean {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  if (keys.length !== 3 || !keys.includes("major") || !keys.includes("minMinor") || !keys.includes("maxMinor")) {
    return false;
  }
  const components = [value.major, value.minMinor, value.maxMinor];
  if (!components.every((item) => Number.isSafeInteger(item) && item >= 0 && item <= 999)) {
    return false;
  }
  if (value.minMinor > value.maxMinor) return false;
  return value.major * 1000 + value.minMinor !== 0 && value.major * 1000 + value.maxMinor !== 0;
}

export function negotiateProtocolVersion(
  local: ProtocolSupport,
  remote: ProtocolSupport,
): ProtocolVersion {
  if (!validSupport(local) || !validSupport(remote) || local.major !== remote.major) {
    return fail("PROTOCOL_VERSION_UNSUPPORTED");
  }
  const minimum = Math.max(local.minMinor, remote.minMinor);
  const maximum = Math.min(local.maxMinor, remote.maxMinor);
  if (minimum > maximum) return fail("PROTOCOL_VERSION_UNSUPPORTED");
  return (local.major * 1000 + maximum) as ProtocolVersion;
}
