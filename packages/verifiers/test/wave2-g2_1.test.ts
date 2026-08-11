import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";

import { Wave2Fake } from "@swarm/testkit";
import type { Wave2Ledger } from "@swarm/testkit";

import { loadGate2Plan, type NegativeSeed, type ScenarioGroup } from "../src/wave2/plan.js";
import { evaluateGroup, type GroupLedgers } from "../src/wave2/oracle.js";

// The frozen Gate 2 plan is an EXTERNAL untracked input (loader boundary). The
// Gate 0 proof runs with WAVE2_GATE2_PLAN_PATH pointed at the exact frozen plan;
// absent that, these architecture tests are skipped (never silently passing).
const PLAN_PATH =
  process.env.WAVE2_GATE2_PLAN_PATH ?? "/tmp/wave2-gate2-plan-v0.2.json";
const havePlan = existsSync(PLAN_PATH);

const fake = new Wave2Fake();

function loadGroup(groupId: string): ScenarioGroup {
  const { plan } = loadGate2Plan(readFileSync(PLAN_PATH, "utf8"), PLAN_PATH);
  const g = plan.scenarios.find((s) => s.id === groupId);
  assert.ok(g, `${groupId} present in the frozen plan`);
  return g;
}

/**
 * The fake exposes a defect by its DEFECT KEY only. The seed's expected outcome
 * (expectError / observe) is NEVER passed to the fake — the fake emits from its
 * own independent model, and the oracle checks agreement with the plan. This is
 * what makes the Gate 0 proof non-tautological.
 */
function defectLedger(groupId: string, seed: NegativeSeed): Wave2Ledger {
  return fake.withDefect(groupId, seed.defect);
}

// --- g2_1: full group, all facts produced by the canonical fake ---
test("g2_1: fake-produced healthy passes all assertions and oracle kills all 3 negatives", { skip: !havePlan }, () => {
  const group = loadGroup("g2_1_machine_lock_and_start_dedupe");
  const defects: Record<string, Wave2Ledger> = {};
  for (const seed of group.negativeSeeds) defects[seed.id] = defectLedger(group.id, seed);
  const ledgers: GroupLedgers = { healthy: fake.healthy(group.id), defects };
  const result = evaluateGroup(group, ledgers);
  for (const a of result.assertionVerdicts) {
    assert.ok(a.verdict.ok, `assertion ${a.name}: ${a.verdict.reason}`);
  }
  for (const n of result.negativeVerdicts) {
    assert.ok(n.verdict.ok, `negative ${n.id}: ${n.verdict.reason}`);
  }
  assert.ok(result.ok);
});

// --- 3 hard-domain seeds, each fake-exposed and oracle-killed with siblings ---
const HARD: ReadonlyArray<{ group: string; seedId: string }> = [
  { group: "g2_4_readiness_and_activation_separation", seedId: "second_process_pump" },
  { group: "g2_7_notice_dedupe_and_visibility_ledger", seedId: "notice_range_compare_removed" },
  { group: "g2_9_adapter_invariance", seedId: "codex_model_visible_clientid_mismatch" },
];

for (const { group: groupId, seedId } of HARD) {
  test(`hard seed ${seedId}: fake exposes, oracle kills, siblings unchanged`, { skip: !havePlan }, () => {
    const group = loadGroup(groupId);
    const seed = group.negativeSeeds.find((s) => s.id === seedId);
    assert.ok(seed, `${seedId} present in ${groupId}`);
    const oneSeed: ScenarioGroup = { ...group, assertions: [], negativeSeeds: [seed] };
    const ledgers: GroupLedgers = {
      healthy: fake.healthy(groupId),
      defects: { [seedId]: defectLedger(groupId, seed) },
    };
    const result = evaluateGroup(oneSeed, ledgers);
    assert.ok(result.negativeVerdicts[0]?.verdict.ok, result.negativeVerdicts[0]?.verdict.reason);
  });

  test(`hard seed ${seedId}: unfenced defect is NOT silently killed (fail-closed)`, { skip: !havePlan }, () => {
    const group = loadGroup(groupId);
    const seed = group.negativeSeeds.find((s) => s.id === seedId);
    assert.ok(seed);
    const oneSeed: ScenarioGroup = { ...group, assertions: [], negativeSeeds: [seed] };
    // Defect ledger with NO fence/observe: the healthy shape only.
    const ledgers: GroupLedgers = {
      healthy: fake.healthy(groupId),
      defects: { [seedId]: fake.healthy(groupId) },
    };
    const result = evaluateGroup(oneSeed, ledgers);
    assert.equal(result.negativeVerdicts[0]?.verdict.ok, false);
  });

  test(`hard seed ${seedId}: a fake/plan DISAGREEMENT is caught (anti-tautology)`, { skip: !havePlan }, () => {
    const group = loadGroup(groupId);
    const seed = group.negativeSeeds.find((s) => s.id === seedId);
    assert.ok(seed);
    const oneSeed: ScenarioGroup = { ...group, assertions: [], negativeSeeds: [seed] };
    // Simulate a fake whose independent exposure DISAGREES with the plan: same
    // shape but a deliberately wrong fence/observe token. The oracle (reading the
    // plan's expectation) must NOT pass the negative — proving the check is not a
    // tautology of the fake echoing the plan.
    const base = fake.healthy(groupId);
    const wrong: Wave2Ledger = {
      events: [
        ...base.events,
        { kind: "defect_attempt", defect: seed.defect },
        "expectError" in seed || "expectErrors" in seed
          ? { kind: "fence", defect: seed.defect, error: "WRONG_INDEPENDENT_CODE" }
          : { kind: "observe", defect: seed.defect, observe: "wrong_independent_observation" },
      ],
      siblings: { ...base.siblings },
    };
    const result = evaluateGroup(oneSeed, { healthy: base, defects: { [seedId]: wrong } });
    assert.equal(result.negativeVerdicts[0]?.verdict.ok, false, "fake/plan disagreement must fail the negative");
  });
}
