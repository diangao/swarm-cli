import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type {
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import { DriverNormalizationError } from "../src/normalizer.js";
import { CLAUDE_INPUT_UUID_NAMESPACE } from "../src/claude/constants.js";
import { ClaudeAdapterError, ClaudeWireState, ndjson } from "../src/claude/wire.js";
import { claudeInputUuid, claudeRuntimeSessionUuid, uuidV5 } from "../src/claude/uuid.js";

const positive = JSON.parse(readFileSync(
  new URL("../../../../contracts/protocol/fixtures/wave2-positive.json", import.meta.url),
  "utf8",
)) as { ordinaryBinding: DriverTurnBinding; driverIdentity: DriverIdentity };
const binding = positive.ordinaryBinding;
const runtimeSessionRef = "50e07952-e0fb-535e-9531-531bb1966dc8";
const inputUuid = "edec8bd3-fb21-5e99-9d3c-5399f0037435";
const claudeIdentity = {
  ...positive.driverIdentity,
  runtime: "claude",
  version: "2.1.226",
  capability: {
    start: true,
    resume: true,
    steer: false,
    interrupt: true,
    reviewBoundary: false,
    compactionBoundary: false,
  },
} as DriverIdentity;
const session = {
  launch: {
    protocolVersion: claudeIdentity.protocolVersion,
    agentId: binding.delivery.agentId,
    machineId: binding.delivery.machineId,
    launchId: binding.delivery.launchId,
    routingGeneration: binding.delivery.routingGeneration,
    workspaceGeneration: 1,
    stopEpoch: 0,
    stateInstanceId: `sti_${"0".repeat(26)}`,
    sessionId: binding.delivery.sessionId,
  },
  driverIdentity: claudeIdentity,
  runtimeSessionRef,
} as DriverSession;
const input = {
  input: {} as CompiledNativeTurn["input"],
  bytes: new TextEncoder().encode("permitted input"),
  inputDigest: binding.inputDigest,
} satisfies CompiledNativeTurn;

test("Claude UUIDv5 and canonical NDJSON bind only the frozen protocol fields", () => {
  assert.equal(CLAUDE_INPUT_UUID_NAMESPACE, "f61b0c53-2d3d-5ed4-9bbf-d8d9fab5d3bd");
  assert.equal(claudeInputUuid(binding.delivery.sessionId, binding), inputUuid);
  assert.equal(claudeRuntimeSessionUuid(binding.delivery.sessionId), runtimeSessionRef);
  assert.equal(uuidV5(CLAUDE_INPUT_UUID_NAMESPACE, "known"), "a2bc299a-08e0-509a-99b1-55c6ca248987");

  const wire = readyWire();
  const prepared = wire.beginTurn(session, input, binding);
  assert.equal(prepared.input.uuid, inputUuid);
  assert.deepEqual(prepared.input, {
    type: "user",
    message: { role: "user", content: [{ type: "text", text: "permitted input" }] },
    parent_tool_use_id: null,
    session_id: runtimeSessionRef,
    uuid: inputUuid,
  });
  const line = new TextDecoder().decode(prepared.line);
  assert.equal(line.at(-1), "\n");
  assert.equal(line, new TextDecoder().decode(ndjson(prepared.input)));
  assert.doesNotMatch(line, /invocationGeneration|permitId|rootProducerFactId/u);
});

test("Claude exact replay is the sole model-visible boundary and exact duplicates alias", () => {
  const { wire, prepared } = begunWire();
  const written = wire.markInputWritten(prepared.input.uuid);
  assert.equal(written[0]?.kind, "turn_started");
  if (written[0]?.kind === "turn_started") {
    assert.equal(written[0].turnId, binding.protocolTurnId);
    assert.match(written[0].driverTurnRefDigest, /^sha256:[0-9a-f]{64}$/u);
  }
  assert.deepEqual(written[1], {
    kind: "input_written",
    turnId: binding.protocolTurnId,
    runtimeWriteId: binding.runtimeWriteId,
  });
  const replay = replayOf(prepared.input);
  assert.deepEqual(wire.accept(replay), [{
    kind: "model_visible",
    turnId: binding.protocolTurnId,
    visibilityEventId: binding.visibilityEventId,
  }]);
  assert.deepEqual(wire.accept(replay), []);
  assert.deepEqual(wire.accept(assistant("Done.")), [{
    kind: "assistant_reply",
    turnId: binding.protocolTurnId,
    text: "Done.",
  }]);
  assert.deepEqual(wire.accept(result()), [{ kind: "turn_completed", turnId: binding.protocolTurnId }]);
  assert.deepEqual(wire.accept(result()), [], "exact result replay aliases");
});

test("Claude replay identity defects fence and replay-before-write is temporal", () => {
  const cases: Array<(replay: Record<string, unknown>) => void> = [
    (replay) => { delete replay["uuid"]; },
    (replay) => { replay["uuid"] = "1b75eb6d-c92f-5ec2-8b03-fd4ab34e164f"; },
    (replay) => { replay["session_id"] = "c17793c9-639d-5185-a8e1-01c782b467e8"; },
    (replay) => { replay["message"] = { role: "user", content: [{ type: "text", text: "changed" }] }; },
  ];
  for (const mutate of cases) {
    const { wire, prepared } = begunWire();
    wire.markInputWritten(prepared.input.uuid);
    const replay = structuredClone(replayOf(prepared.input));
    mutate(replay);
    assert.equal(driverCode(() => wire.accept(replay)), "DRIVER_EVENT_FENCE_MISMATCH");
  }

  const before = begunWire();
  assert.equal(
    driverCode(() => before.wire.accept(replayOf(before.prepared.input))),
    "DRIVER_EVENT_ORDER_INVALID",
  );
});

test("Claude rejects a second UUID in both anti-batching windows and releases only on result", () => {
  const beforeReplay = begunWire();
  assert.equal(
    claudeCode(() => beforeReplay.wire.beginTurn(session, input, nextBinding())),
    "TURN_INPUT_ALREADY_IN_FLIGHT",
  );

  const afterReplay = begunWire();
  afterReplay.wire.markInputWritten(afterReplay.prepared.input.uuid);
  afterReplay.wire.accept(replayOf(afterReplay.prepared.input));
  assert.equal(
    claudeCode(() => afterReplay.wire.beginTurn(session, input, nextBinding())),
    "TURN_INPUT_ALREADY_IN_FLIGHT",
  );
  afterReplay.wire.accept(assistant("Done."));
  afterReplay.wire.accept(result());
  assert.doesNotThrow(() => afterReplay.wire.beginTurn(session, input, nextBinding()));
});

test("Claude feature-detected exact-turn interrupt requires one correlated empty-queue receipt", () => {
  const { wire, prepared } = begunWire();
  wire.markInputWritten(prepared.input.uuid);
  wire.accept(replayOf(prepared.input));
  const request = wire.interruptRequest(binding.protocolTurnId);
  assert.deepEqual(request.request, {
    type: "control_request",
    request_id: binding.permitId,
    request: { subtype: "interrupt" },
  });
  const receipt = {
    type: "control_response",
    request_id: binding.permitId,
    response: { subtype: "success", still_queued: [], cancelled: [] },
  };
  assert.doesNotThrow(() => wire.acceptInterruptReceipt(receipt, binding.permitId));

  assert.equal(driverCode(() => wire.acceptInterruptReceipt({
    ...receipt,
    request_id: "wrong",
  }, binding.permitId)), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(driverCode(() => wire.acceptInterruptReceipt({
    ...receipt,
    response: { ...receipt.response, still_queued: [inputUuid] },
  }, binding.permitId)), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(driverCode(() => wire.acceptInterruptReceipt({
    ...receipt,
    response: { subtype: "success" },
  }, binding.permitId)), "DRIVER_PROTOCOL_UNSUPPORTED");

  wire.accept(assistant("Done."));
  wire.accept(result());
  assert.equal(claudeCode(() => wire.interruptRequest(binding.protocolTurnId)), "ACTIVE_TURN_CONFLICT");
});

test("Claude missing interrupt receipt capability and reordered lifecycle fail closed", () => {
  const missing = new ClaudeWireState(binding.delivery.sessionId, runtimeSessionRef);
  missing.accept(init({ interrupt: true, queue_receipt: false }));
  const prepared = missing.beginTurn(session, input, binding);
  missing.markInputWritten(prepared.input.uuid);
  missing.accept(replayOf(prepared.input));
  assert.equal(driverCode(() => missing.interruptRequest(binding.protocolTurnId)), "DRIVER_PROTOCOL_UNSUPPORTED");

  const notReady = new ClaudeWireState(binding.delivery.sessionId, runtimeSessionRef);
  assert.equal(
    driverCode(() => notReady.beginTurn(session, input, binding)),
    "DRIVER_EVENT_ORDER_INVALID",
  );
  const wrongBoundary = readyWire();
  assert.equal(driverCode(() => wrongBoundary.accept({
    type: "system",
    subtype: "compact_boundary",
    session_id: "wrong-session",
  })), "DRIVER_EVENT_FENCE_MISMATCH");
  assert.equal(driverCode(() => readyWire().accept({ type: "invented" })), "DRIVER_PROTOCOL_UNSUPPORTED");

  const failedResult = begunWire();
  failedResult.wire.markInputWritten(failedResult.prepared.input.uuid);
  failedResult.wire.accept(replayOf(failedResult.prepared.input));
  failedResult.wire.accept(assistant("partial"));
  assert.equal(driverCode(() => failedResult.wire.accept({
    type: "result",
    session_id: runtimeSessionRef,
    subtype: "error",
  })), "DRIVER_PROTOCOL_UNSUPPORTED");
  assert.equal(
    claudeCode(() => failedResult.wire.beginTurn(session, input, nextBinding())),
    "TURN_INPUT_ALREADY_IN_FLIGHT",
    "a non-success result cannot silently release the no-batching window",
  );
});

function readyWire(): ClaudeWireState {
  const wire = new ClaudeWireState(binding.delivery.sessionId, runtimeSessionRef);
  const events = wire.accept(init({ interrupt: true, queue_receipt: true }));
  assert.equal(events[0]?.kind, "runtime_ready");
  return wire;
}

function begunWire() {
  const wire = readyWire();
  return { wire, prepared: wire.beginTurn(session, input, binding) };
}

function init(control: { interrupt: boolean; queue_receipt: boolean }) {
  return {
    type: "system",
    subtype: "init",
    session_id: runtimeSessionRef,
    capabilities: { control_requests: control },
  };
}

function replayOf(wireInput: {
  readonly type: "user";
  readonly message: unknown;
  readonly parent_tool_use_id: null;
  readonly session_id: string;
  readonly uuid: string;
}): Record<string, unknown> {
  return { ...wireInput, isReplay: true };
}

function assistant(text: string) {
  return {
    type: "assistant",
    session_id: runtimeSessionRef,
    message: { content: [{ type: "text", text }] },
  };
}

function result() {
  return { type: "result", session_id: runtimeSessionRef, subtype: "success" };
}

function nextBinding(): DriverTurnBinding {
  return {
    ...binding,
    protocolTurnId: `trn_${"2".repeat(26)}` as TurnId,
    delivery: { ...binding.delivery, turnId: `trn_${"2".repeat(26)}` as TurnId },
    invocation: {
      invocationGeneration: 2,
      invocationId: `cmd_${"4".repeat(26)}` as CommandId,
    },
    permitId: `cmd_${"5".repeat(26)}` as CommandId,
    runtimeWriteId: `cmd_${"6".repeat(26)}` as CommandId,
    visibilityEventId: `cmd_${"7".repeat(26)}` as CommandId,
  };
}

function driverCode(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    assert.ok(error instanceof DriverNormalizationError);
    return error.code;
  }
  return "NO_ERROR";
}

function claudeCode(action: () => unknown): string {
  try {
    action();
  } catch (error) {
    assert.ok(error instanceof ClaudeAdapterError);
    return error.code;
  }
  return "NO_ERROR";
}
