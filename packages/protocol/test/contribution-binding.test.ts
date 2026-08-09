import assert from "node:assert/strict";
import test from "node:test";

import {
  buildContributionBinding,
  parseTurnCoordinationDisposition,
  parseTurnReplyResult,
  verifyContributionBinding,
  verifyTurnCompletionEvidence,
  type ContributionBindingInput,
  type DeliveryFence,
} from "../src/index.js";

// ID_PATTERN = prefix + 26 chars from [0-9a-hjkmnp-tv-z]; "b".."e","z" are valid.
function pid(prefix: string, ch: string): string {
  return `${prefix}_${ch.repeat(26)}`;
}
const SHA = `sha256:${"a".repeat(64)}`;

function fenceFixture(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    protocolVersion: 1,
    deliveryId: pid("dlv", "b"),
    attempt: 1,
    producerFactId: pid("fac", "b"),
    agentId: pid("agt", "b"),
    machineId: pid("mch", "b"),
    launchId: pid("lnc", "b"),
    membershipEpoch: 3,
    routingGeneration: 2,
    routeVersion: 1,
    sessionId: pid("ses", "b"),
    turnId: pid("trn", "b"),
    ...overrides,
  };
}
function inputFixture(overrides: Record<string, unknown> = {}): ContributionBindingInput {
  return {
    fence: fenceFixture(),
    stateInstanceId: pid("sti", "b"),
    inputOrdinal: 0,
    invocationId: pid("cmd", "b"),
    invocationGeneration: 1,
    permitId: pid("cmd", "c"),
    runtimeWriteId: pid("cmd", "d"),
    visibilityEventId: pid("cmd", "e"),
    ...overrides,
  } as unknown as ContributionBindingInput;
}
function expectFail(action: () => unknown): void {
  assert.throws(action, (error: unknown) => {
    assert(error instanceof Error);
    assert.equal(error.name, "ProtocolError");
    return true;
  });
}

test("contribution binding builds, verifies, and binds the fence by digest", () => {
  const built = buildContributionBinding(inputFixture());
  assert.deepEqual(verifyContributionBinding(built), built);
  assert.match(built.deliveryFenceDigest, /^sha256:[0-9a-f]{64}$/u);
  assert.match(built.contributionBindingDigest, /^sha256:[0-9a-f]{64}$/u);
  // Expected-fence check passes for the true fence.
  assert.deepEqual(verifyContributionBinding(built, { expectedFence: built.fence as DeliveryFence }), built);

  // Canonical-order invariance: the same fence values in a different key order
  // produce identical digests.
  const src = fenceFixture();
  const reordered: Record<string, unknown> = {};
  for (const key of Object.keys(src).reverse()) reordered[key] = src[key];
  const other = buildContributionBinding(inputFixture({ fence: reordered }));
  assert.equal(other.deliveryFenceDigest, built.deliveryFenceDigest);
  assert.equal(other.contributionBindingDigest, built.contributionBindingDigest);
});

test("builder fails closed on bad brands, unsafe/zero generation, negative ordinal", () => {
  expectFail(() => buildContributionBinding(inputFixture({ invocationGeneration: 0 })));
  expectFail(() => buildContributionBinding(inputFixture({ invocationGeneration: 1.5 })));
  expectFail(() => buildContributionBinding(inputFixture({ inputOrdinal: -1 })));
  expectFail(() => buildContributionBinding(inputFixture({ inputOrdinal: 2.5 })));
  expectFail(() => buildContributionBinding(inputFixture({ invocationId: pid("rcp", "b") })));
  expectFail(() => buildContributionBinding(inputFixture({ permitId: `cmd_${"i".repeat(26)}` })));
  expectFail(() => buildContributionBinding(inputFixture({ fence: fenceFixture({ sessionId: pid("cmd", "b") }) })));
});

test("verifier rejects each independently mutated projection field", () => {
  const built = buildContributionBinding(inputFixture());
  // A changed projection field with the stored digest kept => recompute mismatch.
  expectFail(() => verifyContributionBinding({ ...built, stateInstanceId: pid("sti", "c") }));
  expectFail(() => verifyContributionBinding({ ...built, inputOrdinal: 7 }));
  expectFail(() => verifyContributionBinding({ ...built, invocationId: pid("cmd", "z") }));
  expectFail(() => verifyContributionBinding({ ...built, invocationGeneration: 2 }));
  expectFail(() => verifyContributionBinding({ ...built, permitId: pid("cmd", "z") }));
  expectFail(() => verifyContributionBinding({ ...built, runtimeWriteId: pid("cmd", "z") }));
  expectFail(() => verifyContributionBinding({ ...built, visibilityEventId: pid("cmd", "z") }));
  // A changed fence with the stale stored fence digest kept => wrong fence digest.
  expectFail(() =>
    verifyContributionBinding({ ...built, fence: fenceFixture({ sessionId: pid("ses", "c") }) }),
  );
});

test("verifier rejects forged stored digests, missing/extra keys, bad brands", () => {
  const built = buildContributionBinding(inputFixture());
  const otherSha = `sha256:${"b".repeat(64)}`;
  expectFail(() => verifyContributionBinding({ ...built, deliveryFenceDigest: otherSha }));
  expectFail(() => verifyContributionBinding({ ...built, contributionBindingDigest: otherSha }));
  expectFail(() => verifyContributionBinding({ ...built, contributionBindingDigest: "not-a-digest" }));
  const { fence: _drop, ...missing } = built;
  expectFail(() => verifyContributionBinding(missing));
  expectFail(() => verifyContributionBinding({ ...built, extra: 1 }));
  expectFail(() => verifyContributionBinding({ ...built, invocationId: `cmd_${"i".repeat(26)}` }));
});

test("an internally consistent forged fence is rejected against the expected true fence", () => {
  const real = buildContributionBinding(inputFixture());
  const forged = buildContributionBinding(inputFixture({ fence: fenceFixture({ sessionId: pid("ses", "z") }) }));
  // The forged binding is self-consistent (verifies without an expected fence)...
  assert.deepEqual(verifyContributionBinding(forged), forged);
  // ...but is rejected when checked against the real fence.
  expectFail(() => verifyContributionBinding(forged, { expectedFence: real.fence as DeliveryFence }));
});

test("changed session/launch/route/turn produce a distinct contribution digest", () => {
  const base = buildContributionBinding(inputFixture());
  for (const override of [
    { sessionId: pid("ses", "c") },
    { launchId: pid("lnc", "c") },
    { routingGeneration: 9 },
    { routeVersion: 4 },
    { turnId: pid("trn", "c") },
  ]) {
    const other = buildContributionBinding(inputFixture({ fence: fenceFixture(override) }));
    assert.notEqual(other.contributionBindingDigest, base.contributionBindingDigest);
    assert.notEqual(other.deliveryFenceDigest, base.deliveryFenceDigest);
  }
  // Sibling contribution differing only by a projection id is distinct too.
  const sibling = buildContributionBinding(inputFixture({ permitId: pid("cmd", "z") }));
  assert.notEqual(sibling.contributionBindingDigest, base.contributionBindingDigest);
});

test("turn reply result validates exact keys and brands", () => {
  const reply = { receiptId: pid("rcp", "b"), resultDigest: SHA };
  assert.deepEqual(parseTurnReplyResult(reply), reply);
  expectFail(() => parseTurnReplyResult({ receiptId: pid("rcp", "b") }));
  expectFail(() => parseTurnReplyResult({ ...reply, extra: 1 }));
  expectFail(() => parseTurnReplyResult({ receiptId: pid("cmd", "b"), resultDigest: SHA }));
  expectFail(() => parseTurnReplyResult({ receiptId: pid("rcp", "b"), resultDigest: "nope" }));
});

test("turn coordination disposition enforces per-variant exact keys", () => {
  const notRequested = { kind: "not_requested", terminalTurnId: pid("trn", "b") };
  const committed = { kind: "committed", commandId: pid("cmd", "b"), receiptId: pid("rcp", "b"), resultDigest: SHA };
  const replay = { kind: "terminal_replay", commandId: pid("cmd", "c"), receiptId: pid("rcp", "c"), resultDigest: SHA };
  assert.deepEqual(parseTurnCoordinationDisposition(notRequested), notRequested);
  assert.deepEqual(parseTurnCoordinationDisposition(committed), committed);
  assert.deepEqual(parseTurnCoordinationDisposition(replay), replay);

  // command present on the wrong variant / absent on the right variant.
  expectFail(() => parseTurnCoordinationDisposition({ ...notRequested, commandId: pid("cmd", "b") }));
  expectFail(() => parseTurnCoordinationDisposition({ kind: "committed", receiptId: pid("rcp", "b"), resultDigest: SHA }));
  // receipt typed as a CommandId-shaped value is rejected.
  expectFail(() => parseTurnCoordinationDisposition({ ...committed, receiptId: pid("cmd", "b") }));
  // unknown disposition kind.
  expectFail(() => parseTurnCoordinationDisposition({ kind: "surprise", terminalTurnId: pid("trn", "b") }));
});

test("turn completion evidence validates the whole envelope and defers to the contribution verifier", () => {
  const contribution = buildContributionBinding(inputFixture());
  const reply = { receiptId: pid("rcp", "b"), resultDigest: SHA };
  const coordination = { kind: "not_requested", terminalTurnId: pid("trn", "b") };
  const evidence = { contribution, reply, coordination };
  assert.deepEqual(verifyTurnCompletionEvidence(evidence), evidence);
  assert.deepEqual(
    verifyTurnCompletionEvidence(evidence, { expectedFence: contribution.fence as DeliveryFence }),
    evidence,
  );
  // Wrong expected fence rejects a forged-fence contribution nested in the envelope.
  const forged = buildContributionBinding(inputFixture({ fence: fenceFixture({ sessionId: pid("ses", "z") }) }));
  expectFail(() =>
    verifyTurnCompletionEvidence(
      { contribution: forged, reply, coordination },
      { expectedFence: contribution.fence as DeliveryFence },
    ),
  );
  // Missing/extra envelope keys and a tampered contribution fail closed.
  expectFail(() => verifyTurnCompletionEvidence({ contribution, reply }));
  expectFail(() => verifyTurnCompletionEvidence({ contribution, reply, coordination, extra: 1 }));
  expectFail(() =>
    verifyTurnCompletionEvidence({ contribution: { ...contribution, inputOrdinal: 9 }, reply, coordination }),
  );
});

test("exact-key check rejects hidden symbol and non-enumerable extras", () => {
  const built = buildContributionBinding(inputFixture());
  expectFail(() => verifyContributionBinding({ ...built, [Symbol("x")]: 1 }));
  const hidden: Record<string, unknown> = { ...built };
  Object.defineProperty(hidden, "shadow", { value: 1, enumerable: false });
  expectFail(() => verifyContributionBinding(hidden));

  const evidence = {
    contribution: built,
    reply: { receiptId: pid("rcp", "b"), resultDigest: SHA },
    coordination: { kind: "not_requested", terminalTurnId: built.fence.turnId },
  };
  expectFail(() => verifyTurnCompletionEvidence({ ...evidence, [Symbol("y")]: 1 }));
  const hiddenEnvelope: Record<string, unknown> = { ...evidence };
  Object.defineProperty(hiddenEnvelope, "shadow", { value: 1, enumerable: false });
  expectFail(() => verifyTurnCompletionEvidence(hiddenEnvelope));
});

test("completion binds a not_requested disposition to the contribution's terminal turn", () => {
  const contribution = buildContributionBinding(inputFixture());
  const reply = { receiptId: pid("rcp", "b"), resultDigest: SHA };
  const matching = {
    contribution,
    reply,
    coordination: { kind: "not_requested", terminalTurnId: contribution.fence.turnId },
  };
  assert.deepEqual(verifyTurnCompletionEvidence(matching), matching);
  // A sibling terminal turn on an otherwise valid contribution is rejected.
  const sibling = {
    contribution,
    reply,
    coordination: { kind: "not_requested", terminalTurnId: pid("trn", "z") },
  };
  expectFail(() => verifyTurnCompletionEvidence(sibling));
});
