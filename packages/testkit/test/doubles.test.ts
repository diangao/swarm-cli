import assert from "node:assert/strict";
import { test } from "node:test";

import {
  ACCELERATED_PROFILE,
  DeterministicFaults,
  FakeClock,
  FakeRuntime,
  FakeSocket,
  FaultRegistry,
  ModelSeenRecorder,
} from "../src/index.js";
import { resetIdCounters, mintTurnId } from "../src/ids.js";

const AGENT_PATTERN = /^agt_[0-9a-hjkmnp-tv-z]{26}$/u;

// ---------------------------------------------------------------------------
// FakeClock: advance/now monotonic.
// ---------------------------------------------------------------------------
test("FakeClock: now and monotonic advance forward, never backward", () => {
  const clock = new FakeClock(ACCELERATED_PROFILE, 1000);
  assert.equal(clock.now(), 1000);
  assert.equal(clock.monotonic(), 0);
  clock.advance(250);
  assert.equal(clock.now(), 1250);
  assert.equal(clock.monotonic(), 250);
  clock.advanceTo(1600);
  assert.equal(clock.now(), 1600);
  assert.equal(clock.monotonic(), 600);
  assert.throws(() => clock.advance(-1), RangeError);
  assert.throws(() => clock.advanceTo(1000), RangeError);
});

// ---------------------------------------------------------------------------
// FakeRuntime: launch -> resume continuity + grammar-valid agentId.
// ---------------------------------------------------------------------------
test("FakeRuntime: launch->resume preserves session id and agent identity", () => {
  resetIdCounters();
  const clock = new FakeClock();
  const runtime = new FakeRuntime(clock);
  const session = runtime.launch("owner", { turnDurationMs: 100 });

  // agentId must be a grammar-valid Protocol AgentId (not "agent-owner").
  assert.match(session.agentId, AGENT_PATTERN, `bad agentId: ${session.agentId}`);

  const turn1 = runtime.resume(session);
  assert.equal(turn1.sessionId, session.sessionId, "turn keeps session id");
  const turn2 = runtime.resume(session);
  assert.equal(turn2.sessionId, session.sessionId, "resume keeps session id");
  assert.equal(turn2.ordinal, 2, "ordinal advances across resumes");
  assert.notEqual(turn1.turnId, turn2.turnId, "each turn gets a fresh turn id");
});

test("FakeRuntime: agent id is stable per seat, distinct across seats", () => {
  resetIdCounters();
  const runtime = new FakeRuntime(new FakeClock());
  const owner1 = runtime.launch("owner", { turnDurationMs: 10 });
  const owner2 = runtime.launch("owner", { turnDurationMs: 10 });
  const worker = runtime.launch("worker-1", { turnDurationMs: 10 });
  assert.equal(owner1.agentId, owner2.agentId, "same seat -> same agent id");
  assert.notEqual(owner1.agentId, worker.agentId, "different seats differ");
  assert.match(worker.agentId, AGENT_PATTERN);
});

// ---------------------------------------------------------------------------
// FakeSocket: notice metadata is content-free (no body field).
// ---------------------------------------------------------------------------
test("FakeSocket: deliverNotice carries metadata only (no body field)", () => {
  const socket = new FakeSocket();
  const event = socket.deliverNotice("worker-1", {
    target: "#eval",
    count: 3,
    marker: "m1",
  });
  assert.ok(event, "notice should deliver when not held");
  assert.equal(event.seat, "worker-1");
  assert.equal(event.meta.count, 3);
  // Structural: the notice meta object exposes no "body" key.
  assert.ok(!("body" in event.meta), "notice metadata must not carry a body");
  assert.equal(socket.delivered().length, 1);
});

test("FakeSocket: held deliveries buffer until release", () => {
  const socket = new FakeSocket();
  socket.holdDelivery("worker-1");
  const held = socket.deliverNotice("worker-1", {
    target: "#eval",
    count: 1,
    marker: "m",
  });
  assert.equal(held, undefined, "held delivery returns undefined");
  assert.equal(socket.delivered().length, 0);
  const flushed = socket.release();
  assert.equal(flushed.length, 1);
  assert.equal(socket.delivered().length, 1);
});

// ---------------------------------------------------------------------------
// DeterministicFaults: registration + lookup.
// ---------------------------------------------------------------------------
test("DeterministicFaults: registry registers and addresses faults by name", () => {
  const registry = new FaultRegistry();
  registry.register(DeterministicFaults.leaseExpiryUnderRunningTurn());
  registry.register(DeterministicFaults.concurrentWakeDoubleSpawn());
  assert.ok(registry.has("leaseExpiryUnderRunningTurn"));
  assert.ok(registry.has("concurrentWakeDoubleSpawn"));
  assert.equal(registry.get("leaseExpiryUnderRunningTurn")?.name, "leaseExpiryUnderRunningTurn");
  assert.equal(registry.names().length, 2);

  const parametrized = DeterministicFaults.processKillMidTurn("after_owner_read");
  assert.equal(parametrized.name, "processKillMidTurn");
  assert.equal(
    parametrized.name === "processKillMidTurn" ? parametrized.point : undefined,
    "after_owner_read",
  );
});

// ---------------------------------------------------------------------------
// ModelSeenRecorder: metadata vs body facts.
// ---------------------------------------------------------------------------
test("ModelSeenRecorder: distinguishes metadata-only from body-read", () => {
  resetIdCounters();
  const recorder = new ModelSeenRecorder();
  const turnA = mintTurnId();
  const turnB = mintTurnId();
  recorder.recordMetadataOnly({ turnId: turnA, steerIncluded: false, noticeCount: 2 });
  assert.ok(recorder.bodyWithheldEverywhere(), "metadata-only means body withheld");

  recorder.recordBodyRead({ turnId: turnB, steerIncluded: false, bodyHash: "h" });
  assert.ok(!recorder.bodyWithheldEverywhere(), "a body read breaks the withheld invariant");

  const factsA = recorder.factsForTurn(turnA);
  assert.equal(factsA.length, 1);
  assert.equal(factsA[0]?.kind, "metadata_only");
  const factsB = recorder.factsForTurn(turnB);
  assert.equal(factsB[0]?.kind, "body_read");
});
