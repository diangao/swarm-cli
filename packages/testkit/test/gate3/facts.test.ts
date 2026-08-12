import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import test from "node:test";

import {
  createGroupMutation,
  createNegativeGate3Facts,
  createPositiveGate3Facts,
  executeNegativeDefect,
  expectedOutcomeForDefectAction,
  GATE3_ASSERTION_PRODUCER_IDS,
} from "../../src/gate3/index.js";
import type { Gate3PlanShape } from "../../src/gate3/index.js";

type ExternalPlan = Gate3PlanShape & {
  readonly scenarios: readonly (Gate3PlanShape["scenarios"][number] & {
    readonly negativeSeeds: readonly {
      readonly id: string;
      readonly defect: string;
      readonly unchanged: readonly string[];
      readonly expectError?: string;
      readonly verifierMustObserve?: string;
    }[];
  })[];
};

async function plan(): Promise<ExternalPlan> {
  const path = resolve(
    process.cwd(),
    "../verifiers/test/gate3/wave3-gate3-scenario-plan-v0.8.json",
  );
  return JSON.parse(await readFile(path, "utf8")) as ExternalPlan;
}

function fakeInput(external: ExternalPlan): Gate3PlanShape {
  return {
    scenarios: external.scenarios.map((scenario) => ({
      id: scenario.id,
      fixture: scenario.fixture,
      expectedFacts: scenario.expectedFacts,
      assertions: scenario.assertions,
      negativeSeeds: scenario.negativeSeeds.map(({ id, defect, unchanged }) => ({
        id,
        defect,
        unchanged,
      })),
    })),
  };
}

test("Gate 3 fake creates 98 typed exact-pass facts and 12 genuine group mutations", async () => {
  const external = await plan();
  const projected = fakeInput(external);
  const baseline = createPositiveGate3Facts(projected);
  assert.equal(baseline.groups.length, 12);
  assert.equal(baseline.groups.flatMap((group) => group.assertions).length, 98);
  assert.equal(GATE3_ASSERTION_PRODUCER_IDS.length, 98);
  assert.deepEqual(
    new Set(GATE3_ASSERTION_PRODUCER_IDS),
    new Set(external.scenarios.flatMap((scenario) => scenario.assertions)),
  );
  assert.doesNotMatch(JSON.stringify(baseline), /"expected"|"expectError"|"verifierMustObserve"/u);

  for (const scenario of external.scenarios) {
    const mutation = createGroupMutation(projected, scenario.id);
    const baselineById = new Map(
      baseline.groups.flatMap((group) => group.assertions).map((fact) => [fact.assertionId, fact]),
    );
    const changed = mutation.groups
      .flatMap((group) => group.assertions)
      .filter((fact) => baselineById.get(fact.assertionId)!.observationDigest !== fact.observationDigest);
    assert.equal(changed.length, 1, scenario.id);
    assert.equal(changed[0]!.groupId, scenario.id);
    assert.equal(
      mutation.groups.flatMap((group) => group.assertions).length,
      98,
      `${scenario.id} still evaluates all siblings`,
    );
  }
});

test("Gate 3 fake independently resolves all 122 defect actions and preserves every sibling", async () => {
  const external = await plan();
  const bundle = createNegativeGate3Facts(fakeInput(external));
  assert.equal(bundle.negatives.length, 122);
  assert.doesNotMatch(JSON.stringify(bundle), /"expectError"|"verifierMustObserve"/u);
  for (const scenario of external.scenarios) {
    for (const seed of scenario.negativeSeeds) {
      const fact = bundle.negatives.find((candidate) => candidate.seedId === seed.id);
      assert.ok(fact, seed.id);
      assert.equal(fact.groupId, scenario.id);
      assert.equal(fact.observedOutcome, expectedOutcomeForDefectAction(seed.defect));
      assert.equal(
        fact.observedOutcome,
        seed.expectError ?? seed.verifierMustObserve,
        `${seed.id} independent registry agrees with plan`,
      );
      assert.deepEqual(fact.siblingsAfter, fact.siblingsBefore, seed.id);
      assert.deepEqual(
        fact.siblingsBefore.map((sibling) => sibling.name),
        seed.unchanged,
        seed.id,
      );
    }
  }
});

test("defect actions select behavior while seed IDs bind only identity", async () => {
  const external = await plan();
  const scenario = external.scenarios[0]!;
  const seed = scenario.negativeSeeds[0]!;
  const alternate = scenario.negativeSeeds[1]!;
  const baseline = executeNegativeDefect(scenario.id, seed);
  assert.equal(baseline.observedOutcome, expectedOutcomeForDefectAction(seed.defect));
  const sameIdChangedAction = executeNegativeDefect(scenario.id, {
    ...seed,
    defect: alternate.defect,
  });
  assert.equal(sameIdChangedAction.seedId, seed.id);
  assert.equal(
    sameIdChangedAction.observedOutcome,
    expectedOutcomeForDefectAction(alternate.defect),
  );
  assert.notEqual(sameIdChangedAction.observedOutcome, baseline.observedOutcome);
  assert.throws(
    () => executeNegativeDefect(scenario.id, {
      ...seed,
      defect: `${seed.defect}_changed_under_same_seed_id`,
    }),
    /gate3_unregistered_defect_action/u,
  );
});
