import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  canonicalProtocolJson,
  parseDriverIdentity,
  parseDriverTurnBinding,
  parseLaunchTransition,
  parseNormalizedDriverEvent,
  type ProtocolVersion,
} from "../src/index.js";

const v2 = 2 as ProtocolVersion;
const positive = JSON.parse(readFileSync(
  new URL("../../../contracts/protocol/fixtures/wave2-positive.json", import.meta.url),
  "utf8",
)) as Record<string, any>;
const seeded = JSON.parse(readFileSync(
  new URL("../../../contracts/protocol/fixtures/wave2-seeded-controls.json", import.meta.url),
  "utf8",
)) as Array<{ seed: string; expected: string }>;

function bytes(value: unknown): Uint8Array {
  return canonicalProtocolJson(value);
}

function error(action: () => unknown): string {
  try {
    action();
  } catch (caught) {
    assert(caught instanceof Error);
    assert.equal(caught.name, "ProtocolError");
    return caught.message;
  }
  return "NO_ERROR";
}

test("driver identity is exact, printable, and capability-frozen", () => {
  assert.deepEqual(parseDriverIdentity(bytes(positive.driverIdentity), v2), positive.driverIdentity);
  assert.equal(error(() => parseDriverIdentity(bytes({
    ...positive.driverIdentity,
    protocolVersion: 1,
  }), 1 as ProtocolVersion)), "PROTOCOL_VERSION_UNSUPPORTED");
  assert.equal(error(() => parseDriverIdentity(bytes({
    ...positive.driverIdentity,
    capability: { ...positive.driverIdentity.capability, start: false },
  }), v2)), "DRIVER_CAPABILITY_MISMATCH");
  assert.equal(error(() => parseDriverIdentity(bytes({
    ...positive.driverIdentity,
    providerSessionId: "private",
  }), v2)), "UNKNOWN_FIELD");
  assert.equal(error(() => parseDriverIdentity(bytes({
    ...positive.driverIdentity,
    version: "0.145.0\n",
  }), v2)), "INVALID_SCALAR");
});

test("launch transitions preserve immutable stop epochs and private-data exclusion", () => {
  for (const value of Object.values(positive.launchTransitions)) {
    assert.deepEqual(parseLaunchTransition(bytes(value), v2), value);
  }
  assert.equal(error(() => parseLaunchTransition(bytes({
    ...positive.launchTransitions.stopRequested,
    invalidatedByStopEpoch: 9,
  }), v2)), "STALE_STOP_EPOCH");
  assert.equal(error(() => parseLaunchTransition(bytes({
    ...positive.launchTransitions.spawned,
    processId: 123,
  }), v2)), "UNKNOWN_FIELD");
});

test("turn bindings split active-turn identity from ordinal invariants", () => {
  const ordinary = positive.ordinaryBinding;
  assert.deepEqual(parseDriverTurnBinding(bytes(ordinary), v2), ordinary);
  assert.equal(error(() => parseDriverTurnBinding(bytes({
    ...ordinary,
    inputOrdinal: 1,
  }), v2)), "INVARIANT_VIOLATION");

  const steer = {
    ...ordinary,
    inputOrdinal: 1,
    mode: { kind: "steer", expectedTurnId: ordinary.protocolTurnId },
  };
  assert.deepEqual(parseDriverTurnBinding(bytes(steer), v2), steer);
  assert.equal(error(() => parseDriverTurnBinding(bytes({
    ...steer,
    mode: { kind: "steer", expectedTurnId: "trn_11111111111111111111111111" },
  }), v2)), "ACTIVE_TURN_CONFLICT");
  assert.equal(error(() => parseDriverTurnBinding(bytes({
    ...ordinary,
    delivery: { ...ordinary.delivery, turnId: "trn_11111111111111111111111111" },
  }), v2)), "DRIVER_EVENT_FENCE_MISMATCH");
});

test("normalized driver events reject absence and provider queue states", () => {
  for (const value of positive.events) {
    assert.deepEqual(parseNormalizedDriverEvent(bytes(value), v2), value);
  }
  assert.equal(
    error(() => parseNormalizedDriverEvent(bytes({ kind: "still_queued" }), v2)),
    "UNSUPPORTED_VARIANT",
  );
  assert.equal(error(() => parseNormalizedDriverEvent(bytes({
    kind: "assistant_reply",
    turnId: "trn_00000000000000000000000000",
    text: "\u3000",
  }), v2)), "EMPTY_MESSAGE");
});

test("every Wave 2 structural seed is killed by its frozen stable category", () => {
  const ordinary = positive.ordinaryBinding;
  const actions = new Map<string, () => unknown>([
    ["driver-capability-start-false", () => parseDriverIdentity(bytes({
      ...positive.driverIdentity,
      capability: { ...positive.driverIdentity.capability, start: false },
    }), v2)],
    ["driver-identity-unknown-field", () => parseDriverIdentity(bytes({
      ...positive.driverIdentity,
      providerSessionId: "private",
    }), v2)],
    ["launch-invalidated-epoch-mismatch", () => parseLaunchTransition(bytes({
      ...positive.launchTransitions.stopRequested,
      invalidatedByStopEpoch: 9,
    }), v2)],
    ["ordinary-positive-input-ordinal", () => parseDriverTurnBinding(bytes({
      ...ordinary,
      inputOrdinal: 1,
    }), v2)],
    ["steer-turn-identity-mismatch", () => parseDriverTurnBinding(bytes({
      ...ordinary,
      inputOrdinal: 1,
      mode: { kind: "steer", expectedTurnId: "trn_11111111111111111111111111" },
    }), v2)],
    ["delivery-turn-binding-mismatch", () => parseDriverTurnBinding(bytes({
      ...ordinary,
      delivery: { ...ordinary.delivery, turnId: "trn_11111111111111111111111111" },
    }), v2)],
    ["provider-still-queued-as-event", () => parseNormalizedDriverEvent(bytes({
      kind: "still_queued",
    }), v2)],
    ["empty-driver-assistant-reply", () => parseNormalizedDriverEvent(bytes({
      kind: "assistant_reply",
      turnId: ordinary.protocolTurnId,
      text: "\u3000",
    }), v2)],
  ]);
  assert.equal(actions.size, seeded.length);
  for (const control of seeded) {
    const action = actions.get(control.seed);
    if (action === undefined) assert.fail(`missing seed action: ${control.seed}`);
    assert.equal(error(action), control.expected, control.seed);
  }
});
