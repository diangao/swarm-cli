import { messageBodyHasContent } from "./content.js";
import { fail } from "./errors.js";
import { canonicalProtocolJson, parseProtocolJson, type JsonObject, type JsonValue } from "./json.js";
import type {
  AgentId,
  ArtifactDigest,
  CommandId,
  DeliveryFence,
  DeliveryId,
  DriverCapability,
  DriverIdentity,
  DriverInputMode,
  DriverTurnBinding,
  LaunchId,
  LaunchTransition,
  LocalLaunchFence,
  MachineId,
  NormalizedDriverEvent,
  ProducerFactId,
  ProtocolVersion,
  ReadyLaunchFence,
  SessionId,
  SpawnedLaunchFence,
  StateInstanceId,
  StopReason,
  TerminalReason,
  TurnId,
} from "./types.js";
import { parseSimpleTaskCommand } from "./wave1-validate.js";

const ID_PATTERN = /^(srv|mch|agt|hum|chn|cvs|msg|dlv|fac|tsk|clm|lse|lnc|cmd|rcp|sti|trn|ses)_[0-9a-hjkmnp-tv-z]{26}$/u;
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
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
  for (const key of Object.keys(value)) {
    if (!allowedSet.has(key)) fail("UNKNOWN_FIELD");
  }
  for (const key of required) {
    if (!Object.hasOwn(value, key)) fail("INVALID_SCALAR");
  }
  return value;
}

function text(value: JsonInput, maximum = 512): string {
  if (typeof value !== "string" || value.length === 0 || value.length > maximum) {
    return fail("INVALID_SCALAR");
  }
  return value;
}

function printableText(value: JsonInput, maximum: number): string {
  const parsed = text(value, maximum);
  for (const character of parsed) {
    const code = character.codePointAt(0) ?? 0;
    if (code < 0x20 || code > 0x7e) return fail("INVALID_SCALAR");
  }
  return parsed;
}

function boolean(value: JsonInput): boolean {
  if (typeof value !== "boolean") return fail("INVALID_SCALAR");
  return value;
}

function integer(value: JsonInput, minimum: number, maximum = MAX_SAFE): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    return fail("INVALID_SCALAR");
  }
  if (value < minimum || value > maximum) return fail("INVALID_SCALAR");
  return value;
}

function id<Id extends string>(value: JsonInput, prefix: string): Id {
  const parsed = text(value, 30);
  if (!ID_PATTERN.test(parsed) || !parsed.startsWith(`${prefix}_`)) {
    return fail("INVALID_SCALAR");
  }
  return parsed as Id;
}

function digest(value: JsonInput): ArtifactDigest {
  const parsed = text(value, 71);
  if (!DIGEST_PATTERN.test(parsed)) return fail("INVALID_SCALAR");
  return parsed as ArtifactDigest;
}

function literal<T extends string>(value: JsonInput, choices: readonly T[]): T {
  const parsed = text(value);
  if (!(choices as readonly string[]).includes(parsed)) return fail("UNSUPPORTED_VARIANT");
  return parsed as T;
}

function version(value: JsonInput, negotiated: ProtocolVersion): ProtocolVersion {
  const parsed = integer(value, 1, 999_999) as ProtocolVersion;
  if (!Number.isSafeInteger(negotiated) || negotiated < 1 || negotiated > 999_999) {
    fail("PROTOCOL_VERSION_UNSUPPORTED");
  }
  if (parsed < 2) fail("PROTOCOL_VERSION_UNSUPPORTED");
  if (parsed !== negotiated) fail("PROTOCOL_VERSION_NOT_NEGOTIATED");
  return parsed;
}

function capability(value: JsonInput): DriverCapability {
  const parsed = object(value, [
    "start",
    "resume",
    "steer",
    "interrupt",
    "reviewBoundary",
    "compactionBoundary",
  ]);
  if (parsed.start !== true) fail("DRIVER_CAPABILITY_MISMATCH");
  return {
    start: true,
    resume: boolean(parsed.resume),
    steer: boolean(parsed.steer),
    interrupt: boolean(parsed.interrupt),
    reviewBoundary: boolean(parsed.reviewBoundary),
    compactionBoundary: boolean(parsed.compactionBoundary),
  };
}

function driverIdentity(value: JsonInput, negotiated: ProtocolVersion): DriverIdentity {
  const parsed = object(value, [
    "protocolVersion",
    "runtime",
    "executableDigest",
    "version",
    "wireProtocolDigest",
    "capability",
  ]);
  return {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    runtime: literal(parsed.runtime, ["codex", "claude", "scripted_fake"] as const),
    executableDigest: digest(parsed.executableDigest),
    version: printableText(parsed.version, 128),
    wireProtocolDigest: digest(parsed.wireProtocolDigest),
    capability: capability(parsed.capability),
  };
}

export function parseDriverIdentity(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): DriverIdentity {
  return driverIdentity(parseProtocolJson(input), negotiated);
}

const LOCAL_FENCE_KEYS = [
  "protocolVersion",
  "agentId",
  "machineId",
  "launchId",
  "routingGeneration",
  "workspaceGeneration",
  "stopEpoch",
] as const;

function localFence(value: JsonObject, negotiated: ProtocolVersion): LocalLaunchFence {
  return {
    protocolVersion: version(value.protocolVersion, negotiated),
    agentId: id<AgentId>(value.agentId, "agt"),
    machineId: id<MachineId>(value.machineId, "mch"),
    launchId: id<LaunchId>(value.launchId, "lnc"),
    routingGeneration: integer(value.routingGeneration, 0),
    workspaceGeneration: integer(value.workspaceGeneration, 1),
    stopEpoch: integer(value.stopEpoch, 0),
  };
}

function spawnedFence(value: JsonObject, negotiated: ProtocolVersion): SpawnedLaunchFence {
  return {
    ...localFence(value, negotiated),
    stateInstanceId: id<StateInstanceId>(value.stateInstanceId, "sti"),
  };
}

function readyFence(value: JsonObject, negotiated: ProtocolVersion): ReadyLaunchFence {
  return {
    ...spawnedFence(value, negotiated),
    sessionId: id<SessionId>(value.sessionId, "ses"),
  };
}

const STOP_REASONS = [
  "explicit_stop",
  "nonresident_idle",
  "route_superseded",
  "daemon_shutdown",
  "driver_protocol_error",
] as const;
const TERMINAL_REASONS = [
  ...STOP_REASONS,
  "spawn_failed",
  "readiness_failed",
  "late_spawn_invalidated",
  "process_exited",
  "security_gate_failed",
] as const;

function transition(value: JsonObject, negotiated: ProtocolVersion): LaunchTransition {
  const kind = literal(value.kind, [
    "start_queued",
    "start_begun",
    "process_spawned",
    "runtime_ready",
    "activated",
    "stop_requested",
    "terminal",
  ] as const);
  if (kind === "start_queued" || kind === "start_begun") {
    const parsed = object(value, [...LOCAL_FENCE_KEYS, "kind"]);
    return { ...localFence(parsed, negotiated), kind };
  }
  if (kind === "process_spawned") {
    const parsed = object(value, [
      ...LOCAL_FENCE_KEYS,
      "kind",
      "stateInstanceId",
      "processHandleDigest",
      "driverIdentityDigest",
    ]);
    return {
      ...spawnedFence(parsed, negotiated),
      kind,
      processHandleDigest: digest(parsed.processHandleDigest),
      driverIdentityDigest: digest(parsed.driverIdentityDigest),
    };
  }
  if (kind === "runtime_ready") {
    const parsed = object(value, [
      ...LOCAL_FENCE_KEYS,
      "kind",
      "stateInstanceId",
      "sessionId",
      "runtimeSessionRefDigest",
      "manifestDigest",
    ]);
    return {
      ...readyFence(parsed, negotiated),
      kind,
      runtimeSessionRefDigest: digest(parsed.runtimeSessionRefDigest),
      manifestDigest: digest(parsed.manifestDigest),
    };
  }
  if (kind === "activated") {
    const parsed = object(value, [...LOCAL_FENCE_KEYS, "kind", "stateInstanceId", "sessionId"]);
    return { ...readyFence(parsed, negotiated), kind };
  }
  if (kind === "stop_requested") {
    const parsed = object(
      value,
      [...LOCAL_FENCE_KEYS, "kind", "stateInstanceId", "reason", "invalidatedByStopEpoch"],
      [...LOCAL_FENCE_KEYS, "kind", "reason", "invalidatedByStopEpoch"],
    );
    const fence = localFence(parsed, negotiated);
    const result: Extract<LaunchTransition, { kind: "stop_requested" }> = {
      ...fence,
      kind,
      reason: literal(parsed.reason, STOP_REASONS) as StopReason,
      invalidatedByStopEpoch: integer(parsed.invalidatedByStopEpoch, 1),
    };
    if (result.invalidatedByStopEpoch !== fence.stopEpoch + 1) fail("STALE_STOP_EPOCH");
    if (Object.hasOwn(parsed, "stateInstanceId")) {
      result.stateInstanceId = id(parsed.stateInstanceId, "sti");
    }
    return result;
  }
  const parsed = object(
    value,
    [...LOCAL_FENCE_KEYS, "kind", "stateInstanceId", "reason", "invalidatedByStopEpoch"],
    [...LOCAL_FENCE_KEYS, "kind", "reason"],
  );
  const fence = localFence(parsed, negotiated);
  const result: Extract<LaunchTransition, { kind: "terminal" }> = {
    ...fence,
    kind: "terminal",
    reason: literal(parsed.reason, TERMINAL_REASONS) as TerminalReason,
  };
  if (Object.hasOwn(parsed, "stateInstanceId")) {
    result.stateInstanceId = id(parsed.stateInstanceId, "sti");
  }
  if (Object.hasOwn(parsed, "invalidatedByStopEpoch")) {
    result.invalidatedByStopEpoch = integer(parsed.invalidatedByStopEpoch, 1);
    if (result.invalidatedByStopEpoch !== fence.stopEpoch + 1) fail("STALE_STOP_EPOCH");
  }
  if (result.reason === "late_spawn_invalidated" && result.invalidatedByStopEpoch === undefined) {
    fail("STALE_STOP_EPOCH");
  }
  return result;
}

export function parseLaunchTransition(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): LaunchTransition {
  return transition(parseProtocolJson(input), negotiated);
}

function deliveryFence(value: JsonInput, negotiated: ProtocolVersion): DeliveryFence {
  const parsed = object(value, [
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
  ]);
  return {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    deliveryId: id<DeliveryId>(parsed.deliveryId, "dlv"),
    attempt: integer(parsed.attempt, 1, 2_147_483_647),
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

function inputMode(value: JsonInput): DriverInputMode {
  const parsed = object(value, ["kind", "expectedTurnId"], ["kind"]);
  const kind = literal(parsed.kind, ["ordinary", "steer"] as const);
  if (kind === "ordinary") {
    object(value, ["kind"]);
    return { kind };
  }
  const exact = object(value, ["kind", "expectedTurnId"]);
  return { kind, expectedTurnId: id(exact.expectedTurnId, "trn") };
}

export function parseDriverTurnBinding(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): DriverTurnBinding {
  const parsed = object(parseProtocolJson(input), [
    "protocolTurnId",
    "rootProducerFactId",
    "inputOrdinal",
    "mode",
    "driverTurnRefDigest",
    "delivery",
    "invocation",
    "permitId",
    "runtimeWriteId",
    "visibilityEventId",
    "inputDigest",
  ]);
  const protocolTurnId = id<TurnId>(parsed.protocolTurnId, "trn");
  const mode = inputMode(parsed.mode);
  const inputOrdinal = integer(parsed.inputOrdinal, 0);
  if (mode.kind === "ordinary") {
    if (inputOrdinal !== 0) fail("INVARIANT_VIOLATION");
  } else if (inputOrdinal < 1 || mode.expectedTurnId !== protocolTurnId) {
    fail("ACTIVE_TURN_CONFLICT");
  }
  const delivery = deliveryFence(parsed.delivery, negotiated);
  if (delivery.turnId !== protocolTurnId) fail("DRIVER_EVENT_FENCE_MISMATCH");
  const invocation = object(parsed.invocation, ["invocationGeneration", "invocationId"]);
  const invocationId = id<CommandId>(invocation.invocationId, "cmd");
  const permitId = id<CommandId>(parsed.permitId, "cmd");
  const runtimeWriteId = id<CommandId>(parsed.runtimeWriteId, "cmd");
  const visibilityEventId = id<CommandId>(parsed.visibilityEventId, "cmd");
  if (new Set([invocationId, permitId, runtimeWriteId, visibilityEventId]).size !== 4) {
    fail("INVARIANT_VIOLATION");
  }
  return {
    protocolTurnId,
    rootProducerFactId: id(parsed.rootProducerFactId, "fac"),
    inputOrdinal,
    mode,
    driverTurnRefDigest: digest(parsed.driverTurnRefDigest),
    delivery,
    invocation: {
      invocationGeneration: integer(invocation.invocationGeneration, 1),
      invocationId,
    },
    permitId,
    runtimeWriteId,
    visibilityEventId,
    inputDigest: digest(parsed.inputDigest),
  };
}

export function parseNormalizedDriverEvent(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): NormalizedDriverEvent {
  const value = parseProtocolJson(input);
  const kind = literal(value.kind, [
    "runtime_ready",
    "turn_started",
    "input_written",
    "model_visible",
    "turn_boundary",
    "assistant_reply",
    "coordination_call",
    "turn_completed",
    "runtime_terminal",
  ] as const);
  if (kind === "runtime_ready") {
    const parsed = object(value, ["kind", "runtimeSessionRef", "runtimeSessionRefDigest"]);
    return {
      kind,
      runtimeSessionRef: text(parsed.runtimeSessionRef, 2_048),
      runtimeSessionRefDigest: digest(parsed.runtimeSessionRefDigest),
    };
  }
  if (kind === "turn_started") {
    const parsed = object(value, ["kind", "turnId", "driverTurnRefDigest"]);
    return { kind, turnId: id(parsed.turnId, "trn"), driverTurnRefDigest: digest(parsed.driverTurnRefDigest) };
  }
  if (kind === "input_written") {
    const parsed = object(value, ["kind", "turnId", "runtimeWriteId"]);
    return { kind, turnId: id(parsed.turnId, "trn"), runtimeWriteId: id(parsed.runtimeWriteId, "cmd") };
  }
  if (kind === "model_visible") {
    const parsed = object(value, ["kind", "turnId", "visibilityEventId"]);
    return { kind, turnId: id(parsed.turnId, "trn"), visibilityEventId: id(parsed.visibilityEventId, "cmd") };
  }
  if (kind === "turn_boundary") {
    const parsed = object(value, ["kind", "turnId", "boundary", "steerable"]);
    return {
      kind,
      turnId: id(parsed.turnId, "trn"),
      boundary: literal(parsed.boundary, ["tool", "review", "compaction", "continuation"] as const),
      steerable: boolean(parsed.steerable),
    };
  }
  if (kind === "assistant_reply") {
    const parsed = object(value, ["kind", "turnId", "text"]);
    const reply = text(parsed.text, 65_536);
    if (!messageBodyHasContent(reply)) fail("EMPTY_MESSAGE");
    return { kind, turnId: id(parsed.turnId, "trn"), text: reply };
  }
  if (kind === "coordination_call") {
    const parsed = object(value, ["kind", "turnId", "commandId", "command"]);
    return {
      kind,
      turnId: id(parsed.turnId, "trn"),
      commandId: id<CommandId>(parsed.commandId, "cmd"),
      command: parseSimpleTaskCommand(canonicalProtocolJson(parsed.command), negotiated),
    };
  }
  if (kind === "turn_completed") {
    const parsed = object(value, ["kind", "turnId"]);
    return { kind, turnId: id(parsed.turnId, "trn") };
  }
  const parsed = object(value, ["kind", "reason"]);
  return { kind: "runtime_terminal", reason: literal(parsed.reason, TERMINAL_REASONS) as TerminalReason };
}
