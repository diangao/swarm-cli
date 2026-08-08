import { createHash } from "node:crypto";

import { messageBodyHasContent } from "./content.js";
import { fail } from "./errors.js";
import {
  canonicalProtocolJson,
  parseProtocolJson,
  type JsonObject,
  type JsonValue,
} from "./json.js";
import type {
  AcquireConsumePermit,
  AgentId,
  ArtifactDigest,
  AttentionNotice,
  BeginNativeWrite,
  ChannelId,
  CommandId,
  ConsumePermit,
  ConversationId,
  CurrentMessageInput,
  DeliveryAck,
  DeliveryAckResult,
  DeliveryFence,
  DeliveryId,
  FrozenStandingManifest,
  HumanId,
  InputWrittenJournalEntry,
  InvocationJournalEntry,
  LaunchId,
  MachineId,
  MessageId,
  ModelVisibleJournalEntry,
  NativeDeliveryEnvelope,
  NativeInvocationFence,
  NativeRuntimeEvent,
  NativeTurnInput,
  ObservationCursorAck,
  ProducerFactId,
  ProtocolVersion,
  ReceiptId,
  ReconcileDeliveryAttempt,
  ReconcileDeliveryResult,
  ReconcileEvidence,
  ResumeConsumePermit,
  RuntimeKind,
  ScriptedNotWrittenProof,
  SessionId,
  SimpleTaskCommand,
  StandingManifest,
  Target,
  TurnId,
  WriteStartedJournalEntry,
} from "./types.js";

const ID_PATTERN = /^(srv|mch|agt|hum|chn|cvs|msg|dlv|fac|tsk|clm|lse|lnc|cmd|rcp|sti|trn|ses)_[0-9a-hjkmnp-tv-z]{26}$/u;
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
  for (const key of Object.keys(value)) {
    if (!allowedSet.has(key)) fail("UNKNOWN_FIELD");
  }
  for (const key of required) {
    if (!Object.hasOwn(value, key)) fail("INVALID_SCALAR");
  }
  return value;
}

function array(value: JsonInput): JsonValue[] {
  if (!Array.isArray(value)) return fail("INVALID_SCALAR");
  return value;
}

function required(value: JsonValue | undefined): JsonValue {
  if (value === undefined) return fail("INVALID_SCALAR");
  return value;
}

function text(value: JsonInput, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    return fail("INVALID_SCALAR");
  }
  return value;
}

function integer(value: JsonInput, minimum: number, maximum = MAX_SAFE): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    return fail("INVALID_SCALAR");
  }
  if (value < minimum || value > maximum) return fail("INVALID_SCALAR");
  return value;
}

function nullable<T>(value: JsonInput, parser: (item: JsonValue) => T): T | null {
  if (value === undefined) return fail("INVALID_SCALAR");
  return value === null ? null : parser(value);
}

function literal<T extends string>(value: JsonInput, choices: readonly T[]): T {
  const parsed = text(value);
  if (!(choices as readonly string[]).includes(parsed)) return fail("UNSUPPORTED_VARIANT");
  return parsed as T;
}

function id<Id extends string>(value: JsonInput, prefix: string): Id {
  const parsed = text(value);
  if (!ID_PATTERN.test(parsed) || !parsed.startsWith(`${prefix}_`)) {
    return fail("INVALID_SCALAR");
  }
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

function version(value: JsonInput, negotiated: ProtocolVersion): ProtocolVersion {
  const parsed = integer(value, 1, 999_999) as ProtocolVersion;
  if (negotiated !== 1) fail("PROTOCOL_VERSION_UNSUPPORTED");
  if (parsed !== negotiated) fail("PROTOCOL_VERSION_NOT_NEGOTIATED");
  return parsed;
}

function target(value: JsonInput): Target {
  const discriminant = object(
    value,
    ["kind", "channelId", "conversationId", "threadRootMessageId"],
    ["kind"],
  );
  const kind = text(discriminant.kind);
  if (kind === "channel") {
    const parsed = object(value, ["kind", "channelId", "threadRootMessageId"], ["kind", "channelId"]);
    const result: { kind: "channel"; channelId: ChannelId; threadRootMessageId?: MessageId } = {
      kind,
      channelId: id<ChannelId>(parsed.channelId, "chn"),
    };
    if (Object.hasOwn(parsed, "threadRootMessageId")) {
      result.threadRootMessageId = id<MessageId>(required(parsed.threadRootMessageId), "msg");
    }
    return result;
  }
  if (kind === "direct") {
    const parsed = object(value, ["kind", "conversationId", "threadRootMessageId"], ["kind", "conversationId"]);
    const result: { kind: "direct"; conversationId: ConversationId; threadRootMessageId?: MessageId } = {
      kind,
      conversationId: id<ConversationId>(parsed.conversationId, "cvs"),
    };
    if (Object.hasOwn(parsed, "threadRootMessageId")) {
      result.threadRootMessageId = id<MessageId>(required(parsed.threadRootMessageId), "msg");
    }
    return result;
  }
  return fail("UNSUPPORTED_VARIANT");
}

function runtime(value: JsonInput): RuntimeKind {
  return literal(value, ["codex", "claude"] as const);
}

const DELIVERY_KEYS = [
  "protocolVersion", "deliveryId", "attempt", "messageId", "target", "serverSeq",
  "producerFactId", "agentId", "machineId", "expectedLaunchId", "replayOf",
] as const;
const NATIVE_DELIVERY_KEYS = [
  ...DELIVERY_KEYS, "membershipEpoch", "routingGeneration", "routeVersion",
] as const;

function nativeDelivery(value: JsonInput, negotiated: ProtocolVersion): NativeDeliveryEnvelope {
  const parsed = object(value, NATIVE_DELIVERY_KEYS, [
    "protocolVersion", "deliveryId", "attempt", "messageId", "target", "serverSeq",
    "producerFactId", "agentId", "machineId", "expectedLaunchId", "membershipEpoch",
    "routingGeneration", "routeVersion",
  ]);
  const deliveryId = id<DeliveryId>(parsed.deliveryId, "dlv");
  const attempt = integer(parsed.attempt, 1, MAX_ATTEMPT);
  const replayOf = Object.hasOwn(parsed, "replayOf")
    ? id<DeliveryId>(required(parsed.replayOf), "dlv")
    : undefined;
  if ((attempt === 1) === (replayOf !== undefined) || replayOf === deliveryId) {
    fail("INVARIANT_VIOLATION");
  }
  const result: NativeDeliveryEnvelope = {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    deliveryId,
    attempt,
    messageId: id<MessageId>(parsed.messageId, "msg"),
    target: target(parsed.target),
    serverSeq: integer(parsed.serverSeq, 1),
    producerFactId: id<ProducerFactId>(parsed.producerFactId, "fac"),
    agentId: id<AgentId>(parsed.agentId, "agt"),
    machineId: id<MachineId>(parsed.machineId, "mch"),
    expectedLaunchId: id<LaunchId>(parsed.expectedLaunchId, "lnc"),
    membershipEpoch: integer(parsed.membershipEpoch, 1),
    routingGeneration: integer(parsed.routingGeneration, 0),
    routeVersion: integer(parsed.routeVersion, 1),
  };
  if (replayOf !== undefined) result.replayOf = replayOf;
  return result;
}

const FENCE_KEYS = [
  "protocolVersion", "deliveryId", "attempt", "producerFactId", "agentId",
  "machineId", "launchId", "membershipEpoch", "routingGeneration", "routeVersion",
  "sessionId", "turnId",
] as const;

function deliveryFence(value: JsonObject, negotiated: ProtocolVersion): DeliveryFence {
  return {
    protocolVersion: version(value.protocolVersion, negotiated),
    deliveryId: id<DeliveryId>(value.deliveryId, "dlv"),
    attempt: integer(value.attempt, 1, MAX_ATTEMPT),
    producerFactId: id<ProducerFactId>(value.producerFactId, "fac"),
    agentId: id<AgentId>(value.agentId, "agt"),
    machineId: id<MachineId>(value.machineId, "mch"),
    launchId: id<LaunchId>(value.launchId, "lnc"),
    membershipEpoch: integer(value.membershipEpoch, 1),
    routingGeneration: integer(value.routingGeneration, 0),
    routeVersion: integer(value.routeVersion, 1),
    sessionId: id<SessionId>(value.sessionId, "ses"),
    turnId: id<TurnId>(value.turnId, "trn"),
  };
}

function invocation(value: JsonInput): NativeInvocationFence {
  const parsed = object(value, ["invocationGeneration", "invocationId"]);
  return {
    invocationGeneration: integer(parsed.invocationGeneration, 1),
    invocationId: id<CommandId>(parsed.invocationId, "cmd"),
  };
}

function inlineInvocation(value: JsonObject): NativeInvocationFence {
  return {
    invocationGeneration: integer(value.invocationGeneration, 1),
    invocationId: id<CommandId>(value.invocationId, "cmd"),
  };
}

function assertRequestDigest(raw: JsonObject, actual: ArtifactDigest): void {
  const without = { ...raw };
  delete without.requestDigest;
  if (digestValue(without) !== actual) fail("INVARIANT_VIOLATION");
}

export function parseAttentionNotice(input: Uint8Array, negotiated: ProtocolVersion): AttentionNotice {
  const parsed = object(parseProtocolJson(input), [
    "protocolVersion", "target", "pendingCount", "firstMessageId", "latestMessageId",
    "firstServerSeq", "latestServerSeq",
  ]);
  const result: AttentionNotice = {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    target: target(parsed.target),
    pendingCount: integer(parsed.pendingCount, 1),
    firstMessageId: id<MessageId>(parsed.firstMessageId, "msg"),
    latestMessageId: id<MessageId>(parsed.latestMessageId, "msg"),
    firstServerSeq: integer(parsed.firstServerSeq, 1),
    latestServerSeq: integer(parsed.latestServerSeq, 1),
  };
  if (result.firstServerSeq > result.latestServerSeq) fail("INVARIANT_VIOLATION");
  const equalIds = result.firstMessageId === result.latestMessageId;
  const equalSequences = result.firstServerSeq === result.latestServerSeq;
  if (result.pendingCount === 1 ? (!equalIds || !equalSequences) : (equalIds || !equalSequences)) {
    fail("INVARIANT_VIOLATION");
  }
  return result;
}

function standingManifest(value: JsonInput, negotiated: ProtocolVersion): StandingManifest {
  const parsed = object(value, [
    "protocolVersion", "agentId", "runtime", "workspaceGeneration", "identityDigest",
    "memoryDigest", "cliContractDigest", "capabilityDigest",
  ]);
  return {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    agentId: id<AgentId>(parsed.agentId, "agt"),
    runtime: runtime(parsed.runtime),
    workspaceGeneration: integer(parsed.workspaceGeneration, 1),
    identityDigest: digest(parsed.identityDigest),
    memoryDigest: digest(parsed.memoryDigest),
    cliContractDigest: digest(parsed.cliContractDigest),
    capabilityDigest: digest(parsed.capabilityDigest),
  };
}

export function parseStandingManifest(input: Uint8Array, negotiated: ProtocolVersion): StandingManifest {
  return standingManifest(parseProtocolJson(input), negotiated);
}

export function parseFrozenStandingManifest(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): FrozenStandingManifest {
  const parsed = object(parseProtocolJson(input), ["manifest", "manifestDigest"]);
  const manifest = standingManifest(parsed.manifest, negotiated);
  const manifestDigest = digest(parsed.manifestDigest);
  if (digestValue(manifest) !== manifestDigest) fail("INVARIANT_VIOLATION");
  return { manifest, manifestDigest };
}

export function parseNativeDeliveryEnvelope(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): NativeDeliveryEnvelope {
  return nativeDelivery(parseProtocolJson(input), negotiated);
}

function attention(value: JsonInput, negotiated: ProtocolVersion): AttentionNotice {
  return parseAttentionNotice(canonicalProtocolJson(value), negotiated);
}

export function parseNativeTurnInput(input: Uint8Array, negotiated: ProtocolVersion): NativeTurnInput {
  const parsed = object(parseProtocolJson(input), ["protocolVersion", "manifestDigest", "current", "attention"]);
  const currentValue = object(parsed.current, ["delivery", "body"]);
  const body = text(currentValue.body, true);
  if (!messageBodyHasContent(body)) fail("EMPTY_MESSAGE");
  const current: CurrentMessageInput = {
    delivery: nativeDelivery(currentValue.delivery, negotiated),
    body,
  };
  return {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    manifestDigest: digest(parsed.manifestDigest),
    current,
    attention: array(parsed.attention).map((item) => attention(item, negotiated)),
  };
}

function simpleTask(value: JsonInput, negotiated: ProtocolVersion): SimpleTaskCommand {
  const parsed = object(value, ["protocolVersion", "title", "sourceMessageId"]);
  return {
    protocolVersion: version(parsed.protocolVersion, negotiated),
    title: text(parsed.title),
    sourceMessageId: id<MessageId>(parsed.sourceMessageId, "msg"),
  };
}

export function parseSimpleTaskCommand(input: Uint8Array, negotiated: ProtocolVersion): SimpleTaskCommand {
  return simpleTask(parseProtocolJson(input), negotiated);
}

export function parseNativeRuntimeEvent(input: Uint8Array, negotiated: ProtocolVersion): NativeRuntimeEvent {
  const base = object(parseProtocolJson(input), ["kind", "text", "commandId", "command"], ["kind"]);
  const kind = text(base.kind);
  if (kind === "assistant_reply") {
    const parsed = object(base, ["kind", "text"]);
    const reply = text(parsed.text, true);
    if (!messageBodyHasContent(reply)) fail("EMPTY_MESSAGE");
    return { kind, text: reply };
  }
  if (kind === "coordination_call") {
    const parsed = object(base, ["kind", "commandId", "command"]);
    return {
      kind,
      commandId: id<CommandId>(parsed.commandId, "cmd"),
      command: simpleTask(parsed.command, negotiated),
    };
  }
  if (kind === "turn_complete") {
    object(base, ["kind"]);
    return { kind };
  }
  return fail("UNSUPPORTED_VARIANT");
}

export function parseAcquireConsumePermit(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): AcquireConsumePermit {
  const raw = object(parseProtocolJson(input), [...FENCE_KEYS, "commandId", "requestDigest", "boundary"]);
  const result: AcquireConsumePermit = {
    ...deliveryFence(raw, negotiated),
    commandId: id<CommandId>(raw.commandId, "cmd"),
    requestDigest: digest(raw.requestDigest),
    boundary: literal(raw.boundary, ["daemon_accepted"] as const),
  };
  assertRequestDigest(raw, result.requestDigest);
  return result;
}

export function parseConsumePermit(input: Uint8Array, negotiated: ProtocolVersion): ConsumePermit {
  const raw = object(parseProtocolJson(input), [...FENCE_KEYS, "invocationGeneration", "invocationId", "permitId", "body"]);
  const body = text(raw.body, true);
  if (!messageBodyHasContent(body)) fail("EMPTY_MESSAGE");
  return {
    ...deliveryFence(raw, negotiated),
    ...inlineInvocation(raw),
    permitId: id<CommandId>(raw.permitId, "cmd"),
    body,
  };
}

export function parseResumeConsumePermit(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): ResumeConsumePermit {
  const raw = object(parseProtocolJson(input), [
    ...FENCE_KEYS, "commandId", "requestDigest", "permitId",
    "expectedActiveInvocationGeneration", "resumeMode", "boundary",
  ]);
  const result: ResumeConsumePermit = {
    ...deliveryFence(raw, negotiated),
    commandId: id<CommandId>(raw.commandId, "cmd"),
    requestDigest: digest(raw.requestDigest),
    permitId: id<CommandId>(raw.permitId, "cmd"),
    expectedActiveInvocationGeneration: integer(raw.expectedActiveInvocationGeneration, 1),
    resumeMode: literal(raw.resumeMode, ["same_invocation_before_begin", "next_after_not_written"] as const),
    boundary: literal(raw.boundary, ["daemon_accepted"] as const),
  };
  assertRequestDigest(raw, result.requestDigest);
  return result;
}

export function parseBeginNativeWrite(input: Uint8Array, negotiated: ProtocolVersion): BeginNativeWrite {
  const raw = object(parseProtocolJson(input), [
    ...FENCE_KEYS, "invocationGeneration", "invocationId", "commandId", "requestDigest",
    "permitId", "boundary", "inputDigest", "writeStartedEntryId", "writeStartedEntryDigest",
  ]);
  const result: BeginNativeWrite = {
    ...deliveryFence(raw, negotiated),
    ...inlineInvocation(raw),
    commandId: id<CommandId>(raw.commandId, "cmd"),
    requestDigest: digest(raw.requestDigest),
    permitId: id<CommandId>(raw.permitId, "cmd"),
    boundary: literal(raw.boundary, ["write_started"] as const),
    inputDigest: digest(raw.inputDigest),
    writeStartedEntryId: id<CommandId>(raw.writeStartedEntryId, "cmd"),
    writeStartedEntryDigest: digest(raw.writeStartedEntryDigest),
  };
  assertRequestDigest(raw, result.requestDigest);
  return result;
}

export function parseDeliveryAck(input: Uint8Array, negotiated: ProtocolVersion): DeliveryAck {
  const raw = object(parseProtocolJson(input), [
    ...FENCE_KEYS, "invocationGeneration", "invocationId", "commandId", "requestDigest", "permitId", "boundary",
  ]);
  const result: DeliveryAck = {
    ...deliveryFence(raw, negotiated),
    ...inlineInvocation(raw),
    commandId: id<CommandId>(raw.commandId, "cmd"),
    requestDigest: digest(raw.requestDigest),
    permitId: id<CommandId>(raw.permitId, "cmd"),
    boundary: literal(raw.boundary, ["input_written", "model_visible"] as const),
  };
  assertRequestDigest(raw, result.requestDigest);
  return result;
}

export function parseDeliveryAckResult(input: Uint8Array): DeliveryAckResult {
  const raw = object(parseProtocolJson(input), ["boundary", "receiptId", "invocation", "jobState"]);
  const boundary = literal(raw.boundary, ["input_written", "model_visible"] as const);
  const common = {
    receiptId: id<ReceiptId>(raw.receiptId, "rcp"),
    invocation: invocation(raw.invocation),
  };
  if (boundary === "input_written") {
    return { boundary, ...common, jobState: literal(raw.jobState, ["held/INPUT_WRITTEN"] as const) };
  }
  return { boundary, ...common, jobState: literal(raw.jobState, ["acked/MODEL_VISIBLE"] as const) };
}

const JOURNAL_KEYS = [
  "journalId", "entryId", "sequence", "kind", "previousEntryDigest", "entryDigest",
  ...FENCE_KEYS, "invocationGeneration", "invocationId", "permitId",
] as const;

function journal<K extends "permit_recorded" | "write_started" | "input_written" | "model_visible">(
  value: JsonInput,
  kind: K,
  negotiated: ProtocolVersion,
): InvocationJournalEntry<K> | WriteStartedJournalEntry | InputWrittenJournalEntry | ModelVisibleJournalEntry {
  const extra = kind === "write_started"
    ? ["inputDigest"]
    : kind === "input_written"
      ? ["runtimeWriteId"]
      : kind === "model_visible"
        ? ["runtimeWriteId", "visibilityEventId"]
        : [];
  const raw = object(value, [...JOURNAL_KEYS, ...extra]);
  const entry: InvocationJournalEntry<K> & Record<string, unknown> = {
    ...deliveryFence(raw, negotiated),
    ...inlineInvocation(raw),
    journalId: id<CommandId>(raw.journalId, "cmd"),
    entryId: id<CommandId>(raw.entryId, "cmd"),
    sequence: integer(raw.sequence, 1),
    kind: literal(raw.kind, [kind] as const),
    previousEntryDigest: nullable(raw.previousEntryDigest, digest),
    entryDigest: digest(raw.entryDigest),
    permitId: id<CommandId>(raw.permitId, "cmd"),
  };
  if (entry.journalId !== entry.invocationId) fail("INVALID_JOURNAL_CHAIN");
  if (digestValue(Object.fromEntries(Object.entries(raw).filter(([key]) => key !== "entryDigest"))) !== entry.entryDigest) {
    fail("INVALID_JOURNAL_CHAIN");
  }
  if (kind === "write_started") entry.inputDigest = digest(raw.inputDigest);
  if (kind === "input_written" || kind === "model_visible") {
    entry.runtimeWriteId = id<CommandId>(raw.runtimeWriteId, "cmd");
  }
  if (kind === "model_visible") {
    entry.visibilityEventId = id<CommandId>(raw.visibilityEventId, "cmd");
  }
  return entry as InvocationJournalEntry<K> | WriteStartedJournalEntry | InputWrittenJournalEntry | ModelVisibleJournalEntry;
}

function proof(value: JsonInput): ScriptedNotWrittenProof {
  const raw = object(value, [
    "driverKind", "fixtureId", "scriptDigest", "invocationId", "invocationGeneration",
    "writeStartedEntryId", "writeStartedEntryDigest", "outcomeOrdinal", "outcome", "proofDigest",
  ]);
  const driverKind = text(raw.driverKind);
  if (driverKind === "native_process") fail("REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN");
  if (driverKind !== "scripted_fake") fail("UNSUPPORTED_VARIANT");
  const result: ScriptedNotWrittenProof = {
    driverKind,
    fixtureId: id<CommandId>(raw.fixtureId, "cmd"),
    scriptDigest: digest(raw.scriptDigest),
    invocationId: id<CommandId>(raw.invocationId, "cmd"),
    invocationGeneration: integer(raw.invocationGeneration, 1),
    writeStartedEntryId: id<CommandId>(raw.writeStartedEntryId, "cmd"),
    writeStartedEntryDigest: digest(raw.writeStartedEntryDigest),
    outcomeOrdinal: integer(raw.outcomeOrdinal, 1),
    outcome: literal(raw.outcome, ["not_written"] as const),
    proofDigest: digest(raw.proofDigest),
  };
  if (digestValue(Object.fromEntries(Object.entries(raw).filter(([key]) => key !== "proofDigest"))) !== result.proofDigest) {
    fail("INVALID_JOURNAL_CHAIN");
  }
  return result;
}

function sameFence(entry: InvocationJournalEntry<string>, fence: DeliveryFence, permitId: CommandId, inv: NativeInvocationFence): boolean {
  return FENCE_KEYS.every((key) => entry[key] === fence[key])
    && entry.permitId === permitId
    && entry.invocationGeneration === inv.invocationGeneration
    && entry.invocationId === inv.invocationId;
}

function evidence(value: JsonInput, negotiated: ProtocolVersion): ReconcileEvidence {
  const base = object(value, [
    "kind", "disconnectId", "permitRecorded", "writeStarted", "proof", "driverKind",
    "inputWritten", "modelVisible",
  ], ["kind"]);
  const kind = text(base.kind);
  switch (kind) {
    case "pre_permit_disconnect": {
      const raw = object(base, ["kind", "disconnectId"]);
      return { kind, disconnectId: id<CommandId>(raw.disconnectId, "cmd") };
    }
    case "permit_recorded_write_not_started": {
      const raw = object(base, ["kind", "permitRecorded"]);
      return { kind, permitRecorded: journal(raw.permitRecorded, "permit_recorded", negotiated) as InvocationJournalEntry<"permit_recorded"> };
    }
    case "scripted_not_written": {
      const raw = object(
        base,
        ["kind", "permitRecorded", "writeStarted", "proof"],
        ["kind", "permitRecorded", "writeStarted"],
      );
      if (!Object.hasOwn(raw, "proof")) fail("FAKE_NOT_WRITTEN_PROOF_REQUIRED");
      return {
        kind,
        permitRecorded: journal(raw.permitRecorded, "permit_recorded", negotiated) as InvocationJournalEntry<"permit_recorded">,
        writeStarted: journal(raw.writeStarted, "write_started", negotiated) as WriteStartedJournalEntry,
        proof: proof(raw.proof),
      };
    }
    case "write_started_ambiguous": {
      const raw = object(base, ["kind", "permitRecorded", "writeStarted", "driverKind"]);
      return {
        kind,
        permitRecorded: journal(raw.permitRecorded, "permit_recorded", negotiated) as InvocationJournalEntry<"permit_recorded">,
        writeStarted: journal(raw.writeStarted, "write_started", negotiated) as WriteStartedJournalEntry,
        driverKind: literal(raw.driverKind, ["native_process", "scripted_fake"] as const),
      };
    }
    case "input_written": {
      const raw = object(base, ["kind", "permitRecorded", "writeStarted", "inputWritten"]);
      return {
        kind,
        permitRecorded: journal(raw.permitRecorded, "permit_recorded", negotiated) as InvocationJournalEntry<"permit_recorded">,
        writeStarted: journal(raw.writeStarted, "write_started", negotiated) as WriteStartedJournalEntry,
        inputWritten: journal(raw.inputWritten, "input_written", negotiated) as InputWrittenJournalEntry,
      };
    }
    case "model_visibility_ambiguous": {
      const raw = object(base, ["kind", "permitRecorded", "writeStarted", "inputWritten", "driverKind"]);
      return {
        kind,
        permitRecorded: journal(raw.permitRecorded, "permit_recorded", negotiated) as InvocationJournalEntry<"permit_recorded">,
        writeStarted: journal(raw.writeStarted, "write_started", negotiated) as WriteStartedJournalEntry,
        inputWritten: journal(raw.inputWritten, "input_written", negotiated) as InputWrittenJournalEntry,
        driverKind: literal(raw.driverKind, ["native_process", "scripted_fake"] as const),
      };
    }
    case "model_visible": {
      const raw = object(base, ["kind", "permitRecorded", "writeStarted", "inputWritten", "modelVisible"]);
      return {
        kind,
        permitRecorded: journal(raw.permitRecorded, "permit_recorded", negotiated) as InvocationJournalEntry<"permit_recorded">,
        writeStarted: journal(raw.writeStarted, "write_started", negotiated) as WriteStartedJournalEntry,
        inputWritten: journal(raw.inputWritten, "input_written", negotiated) as InputWrittenJournalEntry,
        modelVisible: journal(raw.modelVisible, "model_visible", negotiated) as ModelVisibleJournalEntry,
      };
    }
    default:
      return fail("UNSUPPORTED_VARIANT");
  }
}

function evidenceEntries(value: ReconcileEvidence): InvocationJournalEntry<string>[] {
  if (value.kind === "pre_permit_disconnect") return [];
  const entries: InvocationJournalEntry<string>[] = [value.permitRecorded];
  if ("writeStarted" in value) entries.push(value.writeStarted);
  if ("inputWritten" in value) entries.push(value.inputWritten);
  if ("modelVisible" in value) entries.push(value.modelVisible);
  return entries;
}

export function parseReconcileDeliveryAttempt(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): ReconcileDeliveryAttempt {
  const raw = object(parseProtocolJson(input), [
    ...FENCE_KEYS, "commandId", "requestDigest", "permitId", "invocation",
    "evidenceDigest", "evidence",
  ]);
  const parsedEvidence = evidence(raw.evidence, negotiated);
  const parsedInvocation = nullable(raw.invocation, invocation);
  const permitId = nullable(raw.permitId, (item) => id<CommandId>(item, "cmd"));
  const prePermit = parsedEvidence.kind === "pre_permit_disconnect";
  if (prePermit !== (parsedInvocation === null) || prePermit !== (permitId === null)) {
    fail("PERMIT_REQUIRED");
  }
  const result: ReconcileDeliveryAttempt = {
    ...deliveryFence(raw, negotiated),
    commandId: id<CommandId>(raw.commandId, "cmd"),
    requestDigest: digest(raw.requestDigest),
    permitId,
    invocation: parsedInvocation,
    evidenceDigest: digest(raw.evidenceDigest),
    evidence: parsedEvidence,
  };
  assertRequestDigest(raw, result.requestDigest);
  if (digestValue(raw.evidence) !== result.evidenceDigest) fail("INVALID_JOURNAL_CHAIN");
  if (!prePermit && permitId !== null && parsedInvocation !== null) {
    const entries = evidenceEntries(parsedEvidence);
    if (!entries.every((entry) => sameFence(entry, result, permitId, parsedInvocation))) {
      fail("PERMIT_MISMATCH");
    }
    for (let index = 0; index < entries.length; index += 1) {
      const entry = entries[index];
      const previous = entries[index - 1];
      if (entry === undefined || entry.sequence !== index + 1) fail("INVALID_JOURNAL_CHAIN");
      if (index === 0 ? entry.previousEntryDigest !== null : entry.previousEntryDigest !== previous?.entryDigest) {
        fail("INVALID_JOURNAL_CHAIN");
      }
    }
    if (parsedEvidence.kind === "scripted_not_written") {
      const started = parsedEvidence.writeStarted;
      const negative = parsedEvidence.proof;
      if (
        negative.invocationId !== parsedInvocation.invocationId
        || negative.invocationGeneration !== parsedInvocation.invocationGeneration
        || negative.writeStartedEntryId !== started.entryId
        || negative.writeStartedEntryDigest !== started.entryDigest
      ) fail("INVALID_JOURNAL_CHAIN");
    }
  }
  return result;
}

export function parseReconcileDeliveryResult(input: Uint8Array): ReconcileDeliveryResult {
  const base = object(parseProtocolJson(input), [
    "kind", "jobState", "replayOfAttempt", "nextAttempt", "attempt", "permitId",
    "resumeMode", "expectedActiveInvocationGeneration", "nextInvocationGeneration",
    "repaired", "invocation",
  ], ["kind", "jobState"]);
  const kind = text(base.kind);
  if (kind === "pre_permit_requeued") {
    const raw = object(base, ["kind", "jobState", "replayOfAttempt", "nextAttempt"]);
    const replayOfAttempt = integer(raw.replayOfAttempt, 1, MAX_ATTEMPT);
    const nextAttempt = integer(raw.nextAttempt, 2, MAX_ATTEMPT);
    if (nextAttempt !== replayOfAttempt + 1) fail("INVARIANT_VIOLATION");
    return { kind, jobState: literal(raw.jobState, ["pending"] as const), replayOfAttempt, nextAttempt };
  }
  if (kind === "same_attempt_resumable") {
    const raw = object(base, [
      "kind", "jobState", "attempt", "permitId", "resumeMode",
      "expectedActiveInvocationGeneration", "nextInvocationGeneration",
    ]);
    const resumeMode = literal(raw.resumeMode, ["same_invocation_before_begin", "next_after_not_written"] as const);
    const expected = integer(raw.expectedActiveInvocationGeneration, 1);
    const next = integer(raw.nextInvocationGeneration, 1);
    if (resumeMode === "same_invocation_before_begin" ? next !== expected : next !== expected + 1) {
      fail("INVARIANT_VIOLATION");
    }
    return {
      kind,
      jobState: literal(raw.jobState, ["held/CONSUME_PERMITTED"] as const),
      attempt: integer(raw.attempt, 1, MAX_ATTEMPT),
      permitId: id<CommandId>(raw.permitId, "cmd"),
      resumeMode,
      expectedActiveInvocationGeneration: expected,
      nextInvocationGeneration: next,
    };
  }
  if (kind === "boundary_repaired") {
    const raw = object(base, ["kind", "jobState", "attempt", "permitId", "repaired", "invocation"]);
    const repaired = array(raw.repaired).map((item) => text(item));
    const valid = repaired.length === 1 && repaired[0] === "input_written"
      || repaired.length === 2 && repaired[0] === "input_written" && repaired[1] === "model_visible";
    if (!valid) fail("INVARIANT_VIOLATION");
    const jobState = literal(raw.jobState, ["held/INPUT_WRITTEN", "acked/MODEL_VISIBLE"] as const);
    if ((repaired.length === 1) !== (jobState === "held/INPUT_WRITTEN")) fail("INVARIANT_VIOLATION");
    return {
      kind,
      jobState,
      attempt: integer(raw.attempt, 1, MAX_ATTEMPT),
      permitId: id<CommandId>(raw.permitId, "cmd"),
      repaired: repaired as ["input_written"] | ["input_written", "model_visible"],
      invocation: invocation(raw.invocation),
    };
  }
  if (kind === "held_ambiguous") {
    const raw = object(base, ["kind", "jobState", "attempt", "permitId", "invocation"]);
    return {
      kind,
      jobState: literal(raw.jobState, ["held/AMBIGUOUS_NATIVE_WRITE"] as const),
      attempt: integer(raw.attempt, 1, MAX_ATTEMPT),
      permitId: id<CommandId>(raw.permitId, "cmd"),
      invocation: invocation(raw.invocation),
    };
  }
  return fail("UNSUPPORTED_VARIANT");
}

export function parseObservationCursorAck(
  input: Uint8Array,
  negotiated: ProtocolVersion,
): ObservationCursorAck {
  const raw = object(parseProtocolJson(input), [
    "protocolVersion", "actorId", "stream", "target", "membershipEpoch", "serverSeq",
  ]);
  const actorText = text(raw.actorId);
  const actorId = actorText.startsWith("hum_")
    ? id<HumanId>(actorText, "hum")
    : id<AgentId>(actorText, "agt");
  return {
    protocolVersion: version(raw.protocolVersion, negotiated),
    actorId,
    stream: literal(raw.stream, ["client_message", "agent_attention"] as const),
    target: target(raw.target),
    membershipEpoch: integer(raw.membershipEpoch, 1),
    serverSeq: integer(raw.serverSeq, 1),
  };
}
