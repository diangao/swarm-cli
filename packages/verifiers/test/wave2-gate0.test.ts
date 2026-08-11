import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";

import { Wave2Fake } from "@swarm/testkit";

import { loadGate2Plan } from "../src/wave2/plan.js";
import { assertPlanCovered, evaluateGroup } from "../src/wave2/oracle.js";

// The frozen Gate 2 plan is an EXTERNAL untracked input (loader boundary). The
// committed full Gate 0 proof runs with WAVE2_GATE2_PLAN_PATH at the exact frozen
// plan; absent it, the proof is skipped (never silently passing).
const PLAN_PATH = process.env.WAVE2_GATE2_PLAN_PATH ?? "/tmp/wave2-gate2-plan-v0.2.json";
const havePlan = existsSync(PLAN_PATH);
const fake = new Wave2Fake();

test("Gate 0: registries are exhaustive over the frozen plan (assertPlanCovered)", { skip: !havePlan }, () => {
  const { plan, inputDigest } = loadGate2Plan(readFileSync(PLAN_PATH, "utf8"), PLAN_PATH);
  // Fail-closed: throws on any plan assertion/negative name with no checker.
  assertPlanCovered(plan);
  // Record the exact proof input digest.
  assert.equal(inputDigest.length, 64);
});

test("Gate 0: every group's healthy passes all assertions and every negative is killed", { skip: !havePlan }, () => {
  const { plan } = loadGate2Plan(readFileSync(PLAN_PATH, "utf8"), PLAN_PATH);
  let assertionCount = 0;
  let negativeCount = 0;
  const failures: string[] = [];
  for (const group of plan.scenarios) {
    const defects: Record<string, ReturnType<Wave2Fake["healthy"]>> = {};
    for (const seed of group.negativeSeeds) defects[seed.id] = fake.withDefect(group.id, seed.defect);
    const result = evaluateGroup(group, { healthy: fake.healthy(group.id), defects });
    for (const a of result.assertionVerdicts) {
      assertionCount += 1;
      if (!a.verdict.ok) failures.push(`${group.id} assertion ${a.name}: ${a.verdict.reason}`);
    }
    for (const n of result.negativeVerdicts) {
      negativeCount += 1;
      if (!n.verdict.ok) failures.push(`${group.id} negative ${n.id}: ${n.verdict.reason}`);
    }
  }
  assert.deepEqual(failures, [], `Gate 0 failures:\n${failures.join("\n")}`);
  assert.equal(negativeCount, 59, "exactly 59 seeded negatives");
  assert.ok(assertionCount >= 80, `expected >=80 assertions, got ${assertionCount}`);
});
