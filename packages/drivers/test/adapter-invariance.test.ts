import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type {
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  MessageId,
  NativeRuntimeEvent,
  NormalizedDriverEvent,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import { NativeEventNormalizer } from "../src/normalizer.js";
import { CODEX_METHOD } from "../src/codex/constants.js";
import { CodexWireState } from "../src/codex/wire.js";
import { ClaudeWireState } from "../src/claude/wire.js";
import { claudeRuntimeSessionUuid } from "../src/claude/uuid.js";

const positive = JSON.parse(readFileSync(
  new URL("../../../../contracts/protocol/fixtures/wave2-positive.json", import.meta.url),
  "utf8",
)) as { ordinaryBinding: DriverTurnBinding; driverIdentity: DriverIdentity };
const binding = positive.ordinaryBinding;
const input = {
  input: {} as CompiledNativeTurn["input"],
  bytes: new TextEncoder().encode("same permitted input"),
  inputDigest: binding.inputDigest,
} satisfies CompiledNativeTurn;
const sourceMessageId = `msg_${"0".repeat(26)}` as MessageId;

test("scripted, Codex, and Claude ordinary replies project to byte-identical server actions", () => {
  const expected: readonly NativeRuntimeEvent[] = [
    { kind: "assistant_reply", text: "Same reply." },
    { kind: "turn_complete" },
  ];
  const codex = codexEvents();
  const claude = claudeEvents();
  assert.deepEqual(codex, expected);
  assert.deepEqual(claude, expected);
  assert.deepEqual(project(expected), project(codex));
  assert.deepEqual(project(expected), project(claude));
  assert.equal(JSON.stringify(project(codex)), JSON.stringify(project(claude)));

  const publicBytes = JSON.stringify({ codex: project(codex), claude: project(claude) });
  assert.doesNotMatch(publicBytes, /codex-thread|codex-turn|session_id|uuid|jsonrpc/u);
});

test("scripted, Codex, and Claude echo the same distinct binding write and visibility ids", () => {
  const expected = [
    { kind: "input_written", turnId: binding.protocolTurnId, runtimeWriteId: binding.runtimeWriteId },
    {
      kind: "model_visible",
      turnId: binding.protocolTurnId,
      visibilityEventId: binding.visibilityEventId,
    },
  ];
  const fixture = JSON.parse(readFileSync(
    new URL("../../../../contracts/protocol/fixtures/wave2-positive.json", import.meta.url),
    "utf8",
  )) as { events: readonly NormalizedDriverEvent[] };

  assert.notEqual(binding.invocation.invocationId, binding.runtimeWriteId);
  assert.notEqual(binding.permitId, binding.visibilityEventId);
  assert.deepEqual(identityWitness(fixture.events), expected, "scripted fixture witness");
  assert.deepEqual(identityWitness(codexTrace()), expected, "Codex witness");
  assert.deepEqual(identityWitness(claudeTrace()), expected, "Claude witness");
});

function codexEvents(): readonly NativeRuntimeEvent[] {
  return native(codexTrace());
}

function codexTrace(): readonly NormalizedDriverEvent[] {
  const wire = new CodexWireState();
  wire.initializeRequest("initialize");
  wire.accept({ jsonrpc: "2.0", id: "initialize", result: {} });
  wire.initializedNotification();
  wire.threadStartRequest("thread", binding.delivery.sessionId);
  wire.accept({ jsonrpc: "2.0", id: "thread", result: { thread: { id: "codex-thread" } } });
  const session = driverSession("codex", "codex-thread");
  wire.turnStartRequest(binding.invocation.invocationId, session, input, binding);
  wire.markRequestWritten(binding.invocation.invocationId);
  const written = wire.accept({
    jsonrpc: "2.0",
    id: binding.invocation.invocationId,
    result: { turn: { id: "codex-turn" } },
  });
  const visible = wire.accept({
    jsonrpc: "2.0",
    method: CODEX_METHOD.turnStarted,
    params: {
      threadId: "codex-thread",
      turn: {
        id: "codex-turn",
        items: [{ type: "userMessage", clientId: binding.invocation.invocationId }],
      },
    },
  });
  const reply = wire.accept({
    jsonrpc: "2.0",
    method: CODEX_METHOD.itemCompleted,
    params: {
      threadId: "codex-thread",
      turnId: "codex-turn",
      item: { type: "agentMessage", id: "reply", text: "Same reply." },
    },
  });
  const complete = wire.accept({
    jsonrpc: "2.0",
    method: CODEX_METHOD.turnCompleted,
    params: { threadId: "codex-thread", turn: { id: "codex-turn" } },
  });
  return [...written, ...visible, ...reply, ...complete];
}

function claudeEvents(): readonly NativeRuntimeEvent[] {
  return native(claudeTrace());
}

function claudeTrace(): readonly NormalizedDriverEvent[] {
  const runtimeSessionRef = claudeRuntimeSessionUuid(binding.delivery.sessionId);
  const wire = new ClaudeWireState(binding.delivery.sessionId, runtimeSessionRef);
  wire.accept({
    type: "system",
    subtype: "init",
    session_id: runtimeSessionRef,
    capabilities: { control_requests: { interrupt: true, queue_receipt: true } },
  });
  const prepared = wire.beginTurn(driverSession("claude", runtimeSessionRef), input, binding);
  const written = wire.markInputWritten(prepared.input.uuid);
  const visible = wire.accept({ ...prepared.input, isReplay: true });
  const reply = wire.accept({
    type: "assistant",
    session_id: runtimeSessionRef,
    message: { content: [{ type: "text", text: "Same reply." }] },
  });
  const complete = wire.accept({ type: "result", session_id: runtimeSessionRef, subtype: "success" });
  return [...written, ...visible, ...reply, ...complete];
}

function identityWitness(events: readonly NormalizedDriverEvent[]) {
  return events.filter((event) => event.kind === "input_written" || event.kind === "model_visible");
}

function driverSession(runtime: "codex" | "claude", runtimeSessionRef: string): DriverSession {
  const identity = runtime === "codex"
    ? positive.driverIdentity
    : {
      ...positive.driverIdentity,
      runtime: "claude" as const,
      version: "2.1.226",
      capability: {
        start: true as const,
        resume: true,
        steer: false,
        interrupt: true,
        reviewBoundary: false,
        compactionBoundary: false,
      },
    };
  return {
    launch: {
      protocolVersion: identity.protocolVersion,
      agentId: binding.delivery.agentId,
      machineId: binding.delivery.machineId,
      launchId: binding.delivery.launchId,
      routingGeneration: binding.delivery.routingGeneration,
      workspaceGeneration: 1,
      stopEpoch: 0,
      stateInstanceId: `sti_${"0".repeat(26)}`,
      sessionId: binding.delivery.sessionId,
    },
    driverIdentity: identity,
    runtimeSessionRef,
  } as DriverSession;
}

function native(events: readonly { kind: string; text?: string }[]): readonly NativeRuntimeEvent[] {
  return events.flatMap((event): readonly NativeRuntimeEvent[] => {
    if (event.kind === "assistant_reply") return [{ kind: "assistant_reply", text: event.text! }];
    if (event.kind === "turn_completed") return [{ kind: "turn_complete" }];
    return [];
  });
}

function project(events: readonly NativeRuntimeEvent[]) {
  const normalizer = new NativeEventNormalizer(sourceMessageId);
  const actions = events.map((event) => normalizer.accept(event));
  normalizer.finish();
  return actions;
}
