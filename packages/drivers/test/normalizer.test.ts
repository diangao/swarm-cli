import assert from "node:assert/strict";
import { test } from "node:test";

import type { ArtifactDigest, CommandId, MessageId, ProtocolVersion } from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import { NativeEventError, NativeEventNormalizer } from "../src/index.js";
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
