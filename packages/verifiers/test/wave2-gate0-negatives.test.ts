import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";

import { Wave2Fake } from "@swarm/testkit";

import { loadGate2Plan } from "../src/wave2/plan.js";
import { evaluateGroup } from "../src/wave2/oracle.js";

const PLAN_PATH = process.env.WAVE2_GATE2_PLAN_PATH ?? "/tmp/wave2-gate2-plan-v0.2.json";
const havePlan = existsSync(PLAN_PATH);
const fake = new Wave2Fake();

// Gate-0 "prove it can fail": every one of the 59 seeded defects must be exposed
// by the canonical fake (defect key only) and killed by the oracle, with its
// named siblings byte-unchanged.
test("all 59 seeded negatives are fake-exposed and oracle-killed", { skip: !havePlan }, () => {
  const { plan } = loadGate2Plan(readFileSync(PLAN_PATH, "utf8"), PLAN_PATH);
  let total = 0;
  const failures: string[] = [];
  for (const group of plan.scenarios) {
    for (const seed of group.negativeSeeds) {
      total += 1;
      const defect = fake.withDefect(group.id, seed.defect);
      const result = evaluateGroup(
        { ...group, assertions: [], negativeSeeds: [seed] },
        { healthy: fake.healthy(group.id), defects: { [seed.id]: defect } },
      );
      const v = result.negativeVerdicts[0]?.verdict;
      if (v?.ok !== true) failures.push(`${group.id}/${seed.id}: ${v?.reason}`);
    }
  }
  assert.equal(total, 59, "exactly 59 seeded negatives");
  assert.deepEqual(failures, [], `all negatives must be killed; failures:\n${failures.join("\n")}`);
});
