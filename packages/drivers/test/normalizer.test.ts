import assert from "node:assert/strict";
import { test } from "node:test";

import type {
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  MessageId,
  ProtocolVersion,
  SessionId,
  StateInstanceId,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import {
  DriverEventStreamNormalizer,
  DriverNormalizationError,
  NativeEventError,
  NativeEventNormalizer,
  assertDriverCapability,
  assertWaiterBeforeWrite,
  requireObservedDriverEvent,
} from "../src/index.js";
import { ScriptedDriver } from "../src/scripted-fake.js";

const token = "01j00000000000000000000000";
const messageId = `msg_${token}` as MessageId;
const version = 1 as ProtocolVersion;
const commandId = `cmd_${token}` as CommandId;

function code(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    assert.ok(error instanceof NativeEventError);
    return error.code;
  }
  return "NO_ERROR";
}

test("normalizer accepts reply then one source-bound coordination then completion", () => {
  const normalizer = new NativeEventNormalizer(messageId);
  assert.equal(normalizer.accept({ kind: "assistant_reply", text: "Done." }).kind, "reply");
  assert.equal(normalizer.accept({
    kind: "coordination_call",
    commandId,
    command: { protocolVersion: version, title: "Follow up", sourceMessageId: messageId },
  }).kind, "coordination");
  assert.equal(normalizer.accept({ kind: "turn_complete" }).kind, "complete");
  normalizer.finish();
});

test("normalizer proves ordering, cardinality, source, and completion failures", () => {
  const beforeReply = new NativeEventNormalizer(messageId);
  assert.equal(code(() => beforeReply.accept({
    kind: "coordination_call",
    commandId,
    command: { protocolVersion: version, title: "bad", sourceMessageId: messageId },
  })), "COORDINATION_BEFORE_REPLY");

  const duplicate = new NativeEventNormalizer(messageId);
  duplicate.accept({ kind: "assistant_reply", text: "First" });
  duplicate.accept({
    kind: "coordination_call",
    commandId,
    command: { protocolVersion: version, title: "one", sourceMessageId: messageId },
  });
  assert.equal(code(() => duplicate.accept({
    kind: "coordination_call",
    commandId: `cmd_${"02j00000000000000000000000"}` as CommandId,
    command: { protocolVersion: version, title: "two", sourceMessageId: messageId },
  })), "SECOND_COORDINATION_CALL");

  const unfinished = new NativeEventNormalizer(messageId);
  unfinished.accept({ kind: "assistant_reply", text: "reply" });
  assert.equal(code(() => unfinished.finish()), "TURN_COMPLETION_REQUIRED");
});

test("normalizer rejects wrong source, duplicate reply, and events after completion", () => {
  const wrongSource = new NativeEventNormalizer(messageId);
  wrongSource.accept({ kind: "assistant_reply", text: "reply" });
  assert.equal(code(() => wrongSource.accept({
    kind: "coordination_call",
    commandId,
    command: {
      protocolVersion: version,
      title: "bad source",
      sourceMessageId: `msg_${"02j00000000000000000000000"}` as MessageId,
    },
  })), "SOURCE_MESSAGE_MISMATCH");

  const duplicateReply = new NativeEventNormalizer(messageId);
  duplicateReply.accept({ kind: "assistant_reply", text: "first" });
  assert.equal(code(() => duplicateReply.accept({ kind: "assistant_reply", text: "second" })), "MULTIPLE_ASSISTANT_REPLIES");

  const afterComplete = new NativeEventNormalizer(messageId);
  afterComplete.accept({ kind: "assistant_reply", text: "reply" });
  afterComplete.accept({ kind: "turn_complete" });
  assert.equal(code(() => afterComplete.accept({ kind: "turn_complete" })), "EVENT_AFTER_COMPLETION");

  const unknown = new NativeEventNormalizer(messageId);
  assert.equal(code(() => unknown.accept({ kind: "unsupported" } as never)), "UNSUPPORTED_RUNTIME_EVENT");
});

test("scripted not-written proof binds the exact preallocated invocation", async () => {
  const driver = new ScriptedDriver([{
    kind: "not_written",
    fixtureId: commandId,
    scriptDigest: `sha256:${"1".repeat(64)}` as ArtifactDigest,
    outcomeOrdinal: 1,
  }]);
  const binding = {
    invocationId: `cmd_${"02j00000000000000000000000"}` as CommandId,
    invocationGeneration: 2,
    writeStartedEntryId: `cmd_${"03j00000000000000000000000"}` as CommandId,
    writeStartedEntryDigest: `sha256:${"2".repeat(64)}` as ArtifactDigest,
  };
  const outcome = await driver.writeTurn({} as CompiledNativeTurn, binding);
  assert.equal(outcome.kind, "not_written");
  if (outcome.kind === "not_written") {
    assert.equal(outcome.proof.invocationId, binding.invocationId);
    assert.equal(outcome.proof.invocationGeneration, 2);
    assert.equal(outcome.proof.writeStartedEntryId, binding.writeStartedEntryId);
    assert.match(outcome.proof.proofDigest, /^sha256:[0-9a-f]{64}$/u);
  }
});

function driverCode(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    assert.ok(error instanceof DriverNormalizationError);
    return error.code;
  }
  return "NO_ERROR";
}

const turnId = `trn_${token}` as TurnId;
const identity: DriverIdentity = {
  protocolVersion: version,
  runtime: "claude",
  executableDigest: `sha256:${"1".repeat(64)}` as ArtifactDigest,
  version: "2.1.226",
  wireProtocolDigest: `sha256:${"2".repeat(64)}` as ArtifactDigest,
  capability: {
    start: true,
    resume: true,
    steer: false,
    interrupt: true,
    reviewBoundary: false,
    compactionBoundary: false,
  },
};

test("capability absence and provider queue states fail closed", () => {
  assertDriverCapability(identity, "resume");
  assert.equal(
    driverCode(() => assertDriverCapability(identity, "steer")),
    "DRIVER_CAPABILITY_MISMATCH",
  );
  assert.equal(
    driverCode(() => requireObservedDriverEvent("still_queued")),
    "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(
    driverCode(() => requireObservedDriverEvent("cancelled")),
    "DRIVER_EVENT_FENCE_MISMATCH",
  );
  assert.equal(
    driverCode(() => requireObservedDriverEvent(undefined)),
    "DRIVER_PROTOCOL_UNSUPPORTED",
  );
});

test("waiter proof is structurally required before a transport write", () => {
  assert.equal(
    driverCode(() => assertWaiterBeforeWrite(undefined)),
    "DRIVER_WAITER_PREDECESSOR_REQUIRED",
  );
  assert.doesNotThrow(() => assertWaiterBeforeWrite({
    kind: "turn",
    stateInstanceId: `sti_${token}` as StateInstanceId,
    sessionId: `ses_${token}` as SessionId,
    turnId,
    bindingDigest: `sha256:${"3".repeat(64)}` as ArtifactDigest,
    waiterId: commandId,
    registeredBeforeWrite: true,
    registeredAtOrdinal: 0,
  }));
});

test("driver stream normalizer splits turn identity from temporal order", () => {
  const normalizer = new DriverEventStreamNormalizer();
  normalizer.accept({
    kind: "runtime_ready",
    runtimeSessionRef: "private-thread",
    runtimeSessionRefDigest: `sha256:${"4".repeat(64)}` as ArtifactDigest,
  });
  normalizer.accept({
    kind: "turn_started",
    turnId,
    driverTurnRefDigest: `sha256:${"5".repeat(64)}` as ArtifactDigest,
  });
  assert.equal(driverCode(() => normalizer.accept({
    kind: "model_visible",
    turnId,
    visibilityEventId: commandId,
  })), "DRIVER_EVENT_ORDER_INVALID");
  assert.equal(driverCode(() => normalizer.accept({
    kind: "input_written",
    turnId: `trn_${"02j00000000000000000000000"}` as TurnId,
    runtimeWriteId: commandId,
  })), "DRIVER_EVENT_FENCE_MISMATCH");
});

test("driver stream accepts one visible reply and completion", () => {
  const normalizer = new DriverEventStreamNormalizer();
  normalizer.accept({
    kind: "runtime_ready",
    runtimeSessionRef: "private-thread",
    runtimeSessionRefDigest: `sha256:${"6".repeat(64)}` as ArtifactDigest,
  });
  normalizer.accept({
    kind: "turn_started",
    turnId,
    driverTurnRefDigest: `sha256:${"7".repeat(64)}` as ArtifactDigest,
  });
  normalizer.accept({ kind: "input_written", turnId, runtimeWriteId: commandId });
  normalizer.accept({ kind: "model_visible", turnId, visibilityEventId: commandId });
  normalizer.accept({ kind: "assistant_reply", turnId, text: "Done." });
  normalizer.accept({ kind: "turn_completed", turnId });
  normalizer.finish();
});
