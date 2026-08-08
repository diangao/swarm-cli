import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  MESSAGE_BODY_CONTENT_FIXTURES,
  canonicalProtocolJson,
  messageBodyHasContent,
  parseAcquireConsumePermit,
  parseAttentionNotice,
  parseBeginNativeWrite,
  parseConsumePermit,
  parseDeliveryAck,
  parseDeliveryAckResult,
  parseFrozenStandingManifest,
  parseNativeDeliveryEnvelope,
  parseNativeRuntimeEvent,
  parseNativeTurnInput,
  parseObservationCursorAck,
  parseReconcileDeliveryAttempt,
  parseReconcileDeliveryResult,
  parseResumeConsumePermit,
  parseSimpleTaskCommand,
  parseStandingManifest,
  type ArtifactDigest,
  type ProtocolVersion,
} from "../src/index.js";

const v1 = 1 as ProtocolVersion;

function id(prefix: string, character: string): string {
  return `${prefix}_${character.repeat(26)}`;
}

function digest(value: unknown): ArtifactDigest {
  return `sha256:${createHash("sha256").update(canonicalProtocolJson(value)).digest("hex")}` as ArtifactDigest;
}

function bytes(value: unknown): Uint8Array {
  return canonicalProtocolJson(value);
}

function signed<T extends Record<string, unknown>>(value: T): T & { requestDigest: ArtifactDigest } {
  return { ...value, requestDigest: digest(value) };
}

function journal(
  kind: "permit_recorded" | "write_started" | "input_written" | "model_visible",
  sequence: number,
  previousEntryDigest: ArtifactDigest | null,
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  const raw = {
    journalId: id("cmd", "b"),
    entryId: id("cmd", String.fromCharCode(99 + sequence)),
    sequence,
    kind,
    previousEntryDigest,
    ...fence,
    invocationGeneration: 1,
    invocationId: id("cmd", "b"),
    permitId: id("cmd", "a"),
    ...extra,
  };
  return { ...raw, entryDigest: digest(raw) };
}

function proof(writeStarted: Record<string, unknown>): Record<string, unknown> {
  const raw = {
    driverKind: "scripted_fake",
    fixtureId: id("cmd", "h"),
    scriptDigest: digest({ script: 1 }),
    invocationId: id("cmd", "b"),
    invocationGeneration: 1,
    writeStartedEntryId: writeStarted.entryId,
    writeStartedEntryDigest: writeStarted.entryDigest,
    outcomeOrdinal: 1,
    outcome: "not_written",
  };
  return { ...raw, proofDigest: digest(raw) };
}

function expectProtocolError(action: () => unknown, code: string): void {
  assert.throws(action, (error: unknown) => {
    assert(error instanceof Error);
    assert.equal(error.name, "ProtocolError");
    assert.equal(error.message, code);
    return true;
  });
}

const target = { kind: "channel", channelId: id("chn", "a") };
const delivery = {
  protocolVersion: 1,
  deliveryId: id("dlv", "a"),
  attempt: 1,
  messageId: id("msg", "a"),
  target,
  serverSeq: 1,
  producerFactId: id("fac", "a"),
  agentId: id("agt", "a"),
  machineId: id("mch", "a"),
  expectedLaunchId: id("lnc", "a"),
  membershipEpoch: 1,
  routingGeneration: 0,
  routeVersion: 1,
};
const fence = {
  protocolVersion: 1,
  deliveryId: delivery.deliveryId,
  attempt: 1,
  producerFactId: delivery.producerFactId,
  agentId: delivery.agentId,
  machineId: delivery.machineId,
  launchId: delivery.expectedLaunchId,
  membershipEpoch: 1,
  routingGeneration: 0,
  routeVersion: 1,
  sessionId: id("ses", "a"),
  turnId: id("trn", "a"),
};
const manifest = {
  protocolVersion: 1,
  agentId: delivery.agentId,
  runtime: "codex",
  workspaceGeneration: 1,
  identityDigest: digest({ identity: 1 }),
  memoryDigest: digest({ memory: 1 }),
  cliContractDigest: digest({ cli: 1 }),
  capabilityDigest: digest({ capabilities: 1 }),
};

test("frozen message-content table is shared and preserves valid bytes", () => {
  for (const fixture of MESSAGE_BODY_CONTENT_FIXTURES) {
    assert.equal(messageBodyHasContent(fixture.body), fixture.hasContent, fixture.name);
  }
  const body = " \nélan\t";
  const input = {
    protocolVersion: 1,
    manifestDigest: digest(manifest),
    current: { delivery, body },
    attention: [],
  };
  assert.equal(parseNativeTurnInput(bytes(input), v1).current.body, body);
  for (const empty of MESSAGE_BODY_CONTENT_FIXTURES.filter((fixture) => !fixture.hasContent)) {
    expectProtocolError(
      () => parseNativeTurnInput(bytes({ ...input, current: { delivery, body: empty.body } }), v1),
      "EMPTY_MESSAGE",
    );
  }
});

test("native delivery, attention, manifest, turn, and runtime unions round-trip", () => {
  assert.deepEqual(parseNativeDeliveryEnvelope(bytes(delivery), v1), delivery);

  const one = {
    protocolVersion: 1,
    target,
    pendingCount: 1,
    firstMessageId: id("msg", "b"),
    latestMessageId: id("msg", "b"),
    firstServerSeq: 2,
    latestServerSeq: 2,
  };
  assert.deepEqual(parseAttentionNotice(bytes(one), v1), one);
  expectProtocolError(
    () => parseAttentionNotice(bytes({ ...one, latestMessageId: id("msg", "c") }), v1),
    "INVARIANT_VIOLATION",
  );

  assert.deepEqual(parseStandingManifest(bytes(manifest), v1), manifest);
  const frozen = { manifest, manifestDigest: digest(manifest) };
  assert.deepEqual(parseFrozenStandingManifest(bytes(frozen), v1), frozen);
  expectProtocolError(
    () => parseFrozenStandingManifest(bytes({ ...frozen, manifestDigest: digest({ wrong: true }) }), v1),
    "INVARIANT_VIOLATION",
  );

  const turn = {
    protocolVersion: 1,
    manifestDigest: frozen.manifestDigest,
    current: { delivery, body: "answer this" },
    attention: [one],
  };
  assert.deepEqual(parseNativeTurnInput(bytes(turn), v1), turn);

  const task = { protocolVersion: 1, title: "Follow up", sourceMessageId: delivery.messageId };
  assert.deepEqual(parseSimpleTaskCommand(bytes(task), v1), task);
  for (const event of [
    { kind: "assistant_reply", text: "A useful answer" },
    { kind: "coordination_call", commandId: id("cmd", "a"), command: task },
    { kind: "turn_complete" },
  ]) {
    assert.deepEqual(parseNativeRuntimeEvent(bytes(event), v1), event);
  }
  expectProtocolError(
    () => parseNativeRuntimeEvent(bytes({ kind: "assistant_reply", text: "\u3000" }), v1),
    "EMPTY_MESSAGE",
  );
});

test("permit, begin, ACK, result, and cursor decoders enforce exact bindings", () => {
  const acquire = signed({ ...fence, commandId: id("cmd", "c"), boundary: "daemon_accepted" });
  assert.deepEqual(parseAcquireConsumePermit(bytes(acquire), v1), acquire);
  expectProtocolError(
    () => parseAcquireConsumePermit(bytes({ ...acquire, attempt: 2 }), v1),
    "INVARIANT_VIOLATION",
  );

  const consume = {
    ...fence,
    invocationGeneration: 1,
    invocationId: id("cmd", "b"),
    permitId: id("cmd", "a"),
    body: "private current body",
  };
  assert.deepEqual(parseConsumePermit(bytes(consume), v1), consume);

  const resume = signed({
    ...fence,
    commandId: id("cmd", "d"),
    permitId: id("cmd", "a"),
    expectedActiveInvocationGeneration: 1,
    resumeMode: "same_invocation_before_begin",
    boundary: "daemon_accepted",
  });
  assert.deepEqual(parseResumeConsumePermit(bytes(resume), v1), resume);

  const started = journal("write_started", 2, digest({ previous: 1 }), { inputDigest: digest({ input: 1 }) });
  const begin = signed({
    ...fence,
    invocationGeneration: 1,
    invocationId: id("cmd", "b"),
    commandId: id("cmd", "e"),
    permitId: id("cmd", "a"),
    boundary: "write_started",
    inputDigest: started.inputDigest,
    writeStartedEntryId: started.entryId,
    writeStartedEntryDigest: started.entryDigest,
  });
  assert.deepEqual(parseBeginNativeWrite(bytes(begin), v1), begin);

  for (const boundary of ["input_written", "model_visible"] as const) {
    const ack = signed({
      ...fence,
      invocationGeneration: 1,
      invocationId: id("cmd", "b"),
      commandId: boundary === "input_written" ? id("cmd", "f") : id("cmd", "g"),
      permitId: id("cmd", "a"),
      boundary,
    });
    assert.deepEqual(parseDeliveryAck(bytes(ack), v1), ack);
    const result = {
      boundary,
      receiptId: boundary === "input_written" ? id("rcp", "a") : id("rcp", "b"),
      invocation: { invocationGeneration: 1, invocationId: id("cmd", "b") },
      jobState: boundary === "input_written" ? "held/INPUT_WRITTEN" : "acked/MODEL_VISIBLE",
    };
    assert.deepEqual(parseDeliveryAckResult(bytes(result)), result);
  }

  for (const actorId of [id("hum", "a"), id("agt", "a")]) {
    const cursor = {
      protocolVersion: 1,
      actorId,
      stream: actorId.startsWith("hum_") ? "client_message" : "agent_attention",
      target,
      membershipEpoch: 1,
      serverSeq: 3,
    };
    assert.deepEqual(parseObservationCursorAck(bytes(cursor), v1), cursor);
  }
});

test("all reconciliation evidence and result variants round-trip", () => {
  const permitRecorded = journal("permit_recorded", 1, null);
  const writeStarted = journal("write_started", 2, permitRecorded.entryDigest as ArtifactDigest, {
    inputDigest: digest({ input: 1 }),
  });
  const inputWritten = journal("input_written", 3, writeStarted.entryDigest as ArtifactDigest, {
    runtimeWriteId: id("cmd", "f"),
  });
  const modelVisible = journal("model_visible", 4, inputWritten.entryDigest as ArtifactDigest, {
    runtimeWriteId: id("cmd", "f"),
    visibilityEventId: id("cmd", "g"),
  });
  const variants = [
    { kind: "permit_recorded_write_not_started", permitRecorded },
    { kind: "scripted_not_written", permitRecorded, writeStarted, proof: proof(writeStarted) },
    { kind: "write_started_ambiguous", permitRecorded, writeStarted, driverKind: "native_process" },
    { kind: "input_written", permitRecorded, writeStarted, inputWritten },
    { kind: "model_visibility_ambiguous", permitRecorded, writeStarted, inputWritten, driverKind: "scripted_fake" },
    { kind: "model_visible", permitRecorded, writeStarted, inputWritten, modelVisible },
  ];

  for (const [index, evidence] of variants.entries()) {
    const request = signed({
      ...fence,
      commandId: id("cmd", String.fromCharCode(99 + index)),
      permitId: id("cmd", "a"),
      invocation: { invocationGeneration: 1, invocationId: id("cmd", "b") },
      evidenceDigest: digest(evidence),
      evidence,
    });
    assert.deepEqual(parseReconcileDeliveryAttempt(bytes(request), v1), request);
  }

  const prePermitEvidence = { kind: "pre_permit_disconnect", disconnectId: id("cmd", "h") };
  const prePermit = signed({
    ...fence,
    commandId: id("cmd", "h"),
    permitId: null,
    invocation: null,
    evidenceDigest: digest(prePermitEvidence),
    evidence: prePermitEvidence,
  });
  assert.deepEqual(parseReconcileDeliveryAttempt(bytes(prePermit), v1), prePermit);

  const results = [
    { kind: "pre_permit_requeued", jobState: "pending", replayOfAttempt: 1, nextAttempt: 2 },
    {
      kind: "same_attempt_resumable",
      jobState: "held/CONSUME_PERMITTED",
      attempt: 1,
      permitId: id("cmd", "a"),
      resumeMode: "same_invocation_before_begin",
      expectedActiveInvocationGeneration: 1,
      nextInvocationGeneration: 1,
    },
    {
      kind: "same_attempt_resumable",
      jobState: "held/CONSUME_PERMITTED",
      attempt: 1,
      permitId: id("cmd", "a"),
      resumeMode: "next_after_not_written",
      expectedActiveInvocationGeneration: 1,
      nextInvocationGeneration: 2,
    },
    {
      kind: "boundary_repaired",
      jobState: "held/INPUT_WRITTEN",
      attempt: 1,
      permitId: id("cmd", "a"),
      repaired: ["input_written"],
      invocation: { invocationGeneration: 1, invocationId: id("cmd", "b") },
    },
    {
      kind: "boundary_repaired",
      jobState: "acked/MODEL_VISIBLE",
      attempt: 1,
      permitId: id("cmd", "a"),
      repaired: ["input_written", "model_visible"],
      invocation: { invocationGeneration: 1, invocationId: id("cmd", "b") },
    },
    {
      kind: "held_ambiguous",
      jobState: "held/AMBIGUOUS_NATIVE_WRITE",
      attempt: 1,
      permitId: id("cmd", "a"),
      invocation: { invocationGeneration: 1, invocationId: id("cmd", "b") },
    },
  ];
  for (const result of results) {
    assert.deepEqual(parseReconcileDeliveryResult(bytes(result)), result);
  }
});

test("reconciliation seeded defects return the stable fail-closed error", () => {
  const permitRecorded = journal("permit_recorded", 1, null);
  const writeStarted = journal("write_started", 2, permitRecorded.entryDigest as ArtifactDigest, {
    inputDigest: digest({ input: 1 }),
  });

  function requestFor(evidence: Record<string, unknown>): Record<string, unknown> {
    return signed({
      ...fence,
      commandId: id("cmd", "h"),
      permitId: id("cmd", "a"),
      invocation: { invocationGeneration: 1, invocationId: id("cmd", "b") },
      evidenceDigest: digest(evidence),
      evidence,
    });
  }

  expectProtocolError(
    () => parseReconcileDeliveryAttempt(bytes(requestFor({ kind: "permit_recorded_write_not_started", permitRecorded, extra: true })), v1),
    "UNKNOWN_FIELD",
  );

  const wrongFenceRaw = { ...permitRecorded, producerFactId: id("fac", "b") };
  const wrongFenceWithoutDigest: Record<string, unknown> = { ...wrongFenceRaw };
  delete wrongFenceWithoutDigest.entryDigest;
  const wrongFence = { ...wrongFenceWithoutDigest, entryDigest: digest(wrongFenceWithoutDigest) };
  expectProtocolError(
    () => parseReconcileDeliveryAttempt(bytes(requestFor({ kind: "permit_recorded_write_not_started", permitRecorded: wrongFence })), v1),
    "PERMIT_MISMATCH",
  );

  const brokenOrderRaw = { ...permitRecorded, sequence: 2 };
  const brokenOrderWithoutDigest: Record<string, unknown> = { ...brokenOrderRaw };
  delete brokenOrderWithoutDigest.entryDigest;
  const brokenOrder = { ...brokenOrderWithoutDigest, entryDigest: digest(brokenOrderWithoutDigest) };
  expectProtocolError(
    () => parseReconcileDeliveryAttempt(bytes(requestFor({ kind: "permit_recorded_write_not_started", permitRecorded: brokenOrder })), v1),
    "INVALID_JOURNAL_CHAIN",
  );

  expectProtocolError(
    () => parseReconcileDeliveryAttempt(bytes(requestFor({
      kind: "scripted_not_written",
      permitRecorded,
      writeStarted,
    })), v1),
    "FAKE_NOT_WRITTEN_PROOF_REQUIRED",
  );

  const realProof = { ...proof(writeStarted), driverKind: "native_process" };
  expectProtocolError(
    () => parseReconcileDeliveryAttempt(bytes(requestFor({
      kind: "scripted_not_written",
      permitRecorded,
      writeStarted,
      proof: realProof,
    })), v1),
    "REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN",
  );
});
