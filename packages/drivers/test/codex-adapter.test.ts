import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type {
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  ProducerFactId,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import { DriverNormalizationError } from "../src/normalizer.js";
import { CODEX_CLIENT_INFO, CODEX_METHOD } from "../src/codex/constants.js";
import { CodexWireState } from "../src/codex/wire.js";

const positive = JSON.parse(readFileSync(
  new URL("../../../../contracts/protocol/fixtures/wave2-positive.json", import.meta.url),
  "utf8",
)) as { ordinaryBinding: DriverTurnBinding; driverIdentity: DriverIdentity };
const binding = positive.ordinaryBinding;
const sessionId = binding.delivery.sessionId;
const runtimeThread = "codex-thread-private";
const runtimeTurn = "codex-turn-private";
const input = {
  input: {} as CompiledNativeTurn["input"],
  bytes: new TextEncoder().encode("permitted input"),
  inputDigest: binding.inputDigest,
} satisfies CompiledNativeTurn;
const session = {
  launch: {
    protocolVersion: positive.driverIdentity.protocolVersion,
    agentId: binding.delivery.agentId,
    machineId: binding.delivery.machineId,
    launchId: binding.delivery.launchId,
    routingGeneration: binding.delivery.routingGeneration,
    workspaceGeneration: 1,
    stopEpoch: 0,
    stateInstanceId: `sti_${"0".repeat(26)}`,
    sessionId,
  },
  driverIdentity: positive.driverIdentity,
  runtimeSessionRef: runtimeThread,
} as DriverSession;

test("Codex handshake and ordinary turn bind the canonical v2 requests and visibility witness", () => {
  const wire = readyWire();
  const request = wire.turnStartRequest(binding.invocation.invocationId, session, input, binding);
  assert.deepEqual(request, {
    jsonrpc: "2.0",
    id: binding.invocation.invocationId,
    method: CODEX_METHOD.turnStart,
    params: {
      threadId: runtimeThread,
      input: [{ type: "text", text: "permitted input" }],
      clientUserMessageId: binding.invocation.invocationId,
    },
  });

  wire.markRequestWritten(binding.invocation.invocationId);
  const written = wire.accept(turnResponse(binding.invocation.invocationId, runtimeTurn));
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
  assert.deepEqual(wire.accept(started(binding.invocation.invocationId)), [{
    kind: "model_visible",
    turnId: binding.protocolTurnId,
    visibilityEventId: binding.visibilityEventId,
  }]);
  assert.deepEqual(wire.accept(reply("reply-1", "Done.")), [{
    kind: "assistant_reply",
    turnId: binding.protocolTurnId,
    text: "Done.",
  }]);
  assert.deepEqual(wire.accept(completed()), [{
    kind: "turn_completed",
    turnId: binding.protocolTurnId,
  }]);
  assert.deepEqual(wire.accept(completed()), [], "exact terminal replay aliases");
});

test("Codex initialization is ordered and thread start emits one session-bound ready fact", () => {
  const wire = new CodexWireState();
  assert.equal(driverCode(() => wire.initializedNotification()), "DRIVER_EVENT_ORDER_INVALID");
  assert.equal(
    driverCode(() => wire.threadStartRequest("thread-before-init", sessionId)),
    "DRIVER_EVENT_ORDER_INVALID",
  );

  assert.deepEqual(wire.initializeRequest("initialize"), {
    jsonrpc: "2.0",
    id: "initialize",
    method: CODEX_METHOD.initialize,
    params: { clientInfo: CODEX_CLIENT_INFO, capabilities: { experimentalApi: true } },
  });
  assert.deepEqual(wire.accept({ jsonrpc: "2.0", id: "initialize", result: {} }), []);
  assert.deepEqual(wire.initializedNotification(), {
    jsonrpc: "2.0",
    method: CODEX_METHOD.initialized,
    params: {},
  });
  assert.equal(driverCode(() => wire.initializedNotification()), "DRIVER_EVENT_ORDER_INVALID");
  const start = wire.threadStartRequest("thread", sessionId);
  assert.deepEqual(start.params, { ephemeral: false, experimentalRawEvents: false });
  const ready = wire.accept({
    jsonrpc: "2.0",
    id: "thread",
    result: { thread: { id: runtimeThread } },
  });
  assert.equal(ready[0]?.kind, "runtime_ready");
  assert.equal(ready[0]?.kind === "runtime_ready" && ready[0].runtimeSessionRef, runtimeThread);
});

test("Codex identity defects fence while temporal defects stay order errors", () => {
  for (const clientId of [undefined, null, "wrong-client"] as const) {
    const wire = turnReady();
    assert.equal(driverCode(() => wire.accept(started(clientId))), "DRIVER_EVENT_FENCE_MISMATCH");
  }

  const wrongTurn = turnReady();
  assert.equal(driverCode(() => wrongTurn.accept(started(
    binding.invocation.invocationId,
    "wrong-runtime-turn",
  ))), "DRIVER_EVENT_FENCE_MISMATCH");

  const beforeInput = readyWire();
  beforeInput.turnStartRequest(binding.invocation.invocationId, session, input, binding);
  beforeInput.markRequestWritten(binding.invocation.invocationId);
  assert.equal(
    driverCode(() => beforeInput.accept(started(binding.invocation.invocationId))),
    "DRIVER_EVENT_ORDER_INVALID",
  );

  const beforeVisible = turnReady();
  assert.equal(driverCode(() => beforeVisible.accept(completed())), "DRIVER_EVENT_ORDER_INVALID");

  const wrongResume = initializedWire();
  wrongResume.threadResumeRequest("resume", sessionId, runtimeThread);
  assert.equal(driverCode(() => wrongResume.accept({
    jsonrpc: "2.0",
    id: "resume",
    result: { thread: { id: "wrong-thread" } },
  })), "DRIVER_EVENT_FENCE_MISMATCH");
});

test("Codex exact event aliases survive steer while changed identities and bytes fail closed", () => {
  const wire = visibleWire();
  const originalStarted = started(binding.invocation.invocationId);
  assert.deepEqual(wire.accept(originalStarted), [], "exact turn/started replay aliases");
  assert.equal(driverCode(() => wire.accept({
    ...originalStarted,
    params: { ...originalStarted.params, unexpected: true },
  })), "DRIVER_EVENT_FENCE_MISMATCH");

  const steer = steerBinding();
  const steerInput = { ...input, inputDigest: steer.inputDigest };
  const request = wire.turnSteerRequest(
    steer.invocation.invocationId,
    session,
    steerInput,
    steer,
  );
  assert.deepEqual(request.params, {
    threadId: runtimeThread,
    expectedTurnId: runtimeTurn,
    input: [{ type: "text", text: "permitted input" }],
    clientUserMessageId: steer.invocation.invocationId,
  });
  wire.markRequestWritten(steer.invocation.invocationId);
  assert.deepEqual(wire.accept({
    jsonrpc: "2.0",
    id: steer.invocation.invocationId,
    result: { turnId: runtimeTurn },
  }), [{
    kind: "input_written",
    turnId: binding.protocolTurnId,
    runtimeWriteId: steer.runtimeWriteId,
  }]);
  assert.deepEqual(wire.accept(startedWithClients([
    binding.invocation.invocationId,
    steer.invocation.invocationId,
  ])), [{
    kind: "model_visible",
    turnId: binding.protocolTurnId,
    visibilityEventId: steer.visibilityEventId,
  }]);
  assert.deepEqual(wire.accept(originalStarted), [], "older exact input replay still aliases");

  const interrupt = wire.interruptRequest("interrupt-command", session, binding.protocolTurnId);
  assert.deepEqual(interrupt.params, { threadId: runtimeThread, turnId: runtimeTurn });
  assert.deepEqual(wire.accept({ jsonrpc: "2.0", id: "interrupt-command", result: {} }), []);
});

test("Codex steer preserves the root fact and advances the exact next input ordinal", () => {
  for (const invalid of [
    { ...steerBinding(), inputOrdinal: 0 },
    { ...steerBinding(), inputOrdinal: 2 },
    {
      ...steerBinding(),
      rootProducerFactId: `fac_${"1".repeat(26)}` as ProducerFactId,
    },
  ]) {
    const wire = visibleWire();
    assert.equal(
      driverCode(() => wire.turnSteerRequest(
        invalid.invocation.invocationId,
        session,
        { ...input, inputDigest: invalid.inputDigest },
        invalid,
      )),
      "DRIVER_EVENT_FENCE_MISMATCH",
    );
  }
});

test("Codex notification parsing rejects unknown status and changed terminal replays", () => {
  const active = readyWire();
  assert.deepEqual(active.accept(status({ type: "active", activeFlags: ["waitingOnUserInput"] })), []);
  assert.deepEqual(active.accept(status({ type: "idle" })), []);
  assert.equal(driverCode(() => active.accept(status({ type: "invented" }))), "DRIVER_PROTOCOL_UNSUPPORTED");

  const wire = visibleWire();
  wire.accept(reply("reply-1", "Done."));
  wire.accept(completed());
  assert.equal(driverCode(() => wire.accept({
    ...completed(),
    params: { ...completed().params, unexpected: true },
  })), "DRIVER_EVENT_FENCE_MISMATCH");
});

function initializedWire(): CodexWireState {
  const wire = new CodexWireState();
  wire.initializeRequest("initialize");
  wire.accept({ jsonrpc: "2.0", id: "initialize", result: {} });
  wire.initializedNotification();
  return wire;
}

function readyWire(): CodexWireState {
  const wire = initializedWire();
  wire.threadStartRequest("thread", sessionId);
  wire.accept({ jsonrpc: "2.0", id: "thread", result: { thread: { id: runtimeThread } } });
  return wire;
}

function turnReady(): CodexWireState {
  const wire = readyWire();
  wire.turnStartRequest(binding.invocation.invocationId, session, input, binding);
  wire.markRequestWritten(binding.invocation.invocationId);
  wire.accept(turnResponse(binding.invocation.invocationId, runtimeTurn));
  return wire;
}

function visibleWire(): CodexWireState {
  const wire = turnReady();
  wire.accept(started(binding.invocation.invocationId));
  return wire;
}

function turnResponse(id: CommandId, turnId: string): unknown {
  return { jsonrpc: "2.0", id, result: { turn: { id: turnId } } };
}

function started(clientId: unknown, turnId = runtimeTurn) {
  return startedWithClients([clientId], turnId);
}

function startedWithClients(clientIds: readonly unknown[], turnId = runtimeTurn) {
  return {
    jsonrpc: "2.0",
    method: CODEX_METHOD.turnStarted,
    params: {
      threadId: runtimeThread,
      turn: {
        id: turnId,
        items: clientIds.map((clientId, index) => clientId === undefined
          ? { type: "userMessage", id: `user-${index}` }
          : { type: "userMessage", id: `user-${index}`, clientId }),
      },
    },
  };
}

function reply(id: string, text: string) {
  return {
    jsonrpc: "2.0",
    method: CODEX_METHOD.itemCompleted,
    params: { threadId: runtimeThread, turnId: runtimeTurn, item: { type: "agentMessage", id, text } },
  };
}

function completed() {
  return {
    jsonrpc: "2.0",
    method: CODEX_METHOD.turnCompleted,
    params: { threadId: runtimeThread, turn: { id: runtimeTurn } },
  };
}

function status(value: unknown) {
  return {
    jsonrpc: "2.0",
    method: CODEX_METHOD.threadStatusChanged,
    params: { threadId: runtimeThread, status: value },
  };
}

function steerBinding(): DriverTurnBinding & { expectedTurnId: TurnId } {
  return {
    ...binding,
    inputOrdinal: 1,
    mode: { kind: "steer", expectedTurnId: binding.protocolTurnId },
    expectedTurnId: binding.protocolTurnId,
    invocation: {
      ...binding.invocation,
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
