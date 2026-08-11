import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";

import { Wave2Fake } from "@swarm/testkit";
import type { LedgerEvent, Wave2Ledger } from "@swarm/testkit";

import { loadGate2Plan } from "../src/wave2/plan.js";
import { evaluateGroup } from "../src/wave2/oracle.js";

// Proves the assertion checkers inspect TYPED DOMAIN FACTS (not a name->boolean
// map or a plan-fed truth value): for every group, mutating one underlying
// healthy fact makes at least one named assertion in that group FAIL. If a
// checker were trivial/plan-fed, no mutation of the ledger would ever flip it.
const PLAN_PATH = process.env.WAVE2_GATE2_PLAN_PATH ?? "/tmp/wave2-gate2-plan-v0.2.json";
const havePlan = existsSync(PLAN_PATH);
const fake = new Wave2Fake();

/** Per-group mutation of one underlying healthy fact known to break >=1 assertion. */
const MUTATE: Record<string, (h: Wave2Ledger) => Wave2Ledger> = {
  g2_1_machine_lock_and_start_dedupe: (h) => addEvent(h, { kind: "process_spawned", launchId: "lnc_dup" }),
  g2_2_bounded_starts_and_fairness: (h) => addEvent(h, { kind: "start_begun", agentId: "agt_x" }, { kind: "start_begun", agentId: "agt_y" }),
  g2_3_stop_during_spawn: (h) => setSibling(h, "slot_state", "resident"),
  g2_4_readiness_and_activation_separation: (h) => addEvent(h, { kind: "adapter_pump", stateInstance: "si_2", owner: "pump_2" }),
  g2_5_delivery_visible_once_after_launch: (h) => addEvent(h, { kind: "input_written", runtimeWriteId: "rw_dup" }),
  g2_6_failed_ambiguous_and_lost_ack_writes: (h) => setSibling(h, "invocation_generation", 0),
  g2_7_notice_dedupe_and_visibility_ledger: (h) => dropKind(h, "notice_visibility"),
  g2_8_turn_state_and_steer_fences: (h) => setSibling(h, "active_turn_state", "active"),
  g2_9_adapter_invariance: (h) => stripField(h, "model_visible", "clientId"),
  g2_10_manifest_and_driver_protocol_identity: (h) => setSibling(h, "canonical_wire_artifact", "sha256:MUTATED"),
  g2_11_launch_environment: (h) => setSibling(h, "transport_contents", "raw_secret"),
  g2_12_carry_gates: (h) => setSibling(h, "declared_toolchain", 0 as unknown as string),
};

function addEvent(h: Wave2Ledger, ...ev: LedgerEvent[]): Wave2Ledger {
  return { events: [...h.events, ...ev], siblings: { ...h.siblings } };
}
function setSibling(h: Wave2Ledger, name: string, value: number | string): Wave2Ledger {
  return { events: [...h.events], siblings: { ...h.siblings, [name]: value } };
}
function dropKind(h: Wave2Ledger, kind: string): Wave2Ledger {
  return { events: h.events.filter((e) => e.kind !== kind), siblings: { ...h.siblings } };
}
function stripField(h: Wave2Ledger, kind: string, field: string): Wave2Ledger {
  return {
    events: h.events.map((e) => {
      if (e.kind !== kind) return e;
      const { [field]: _drop, ...rest } = e;
      return rest as LedgerEvent;
    }),
    siblings: { ...h.siblings },
  };
}

test("per-group healthy-fact mutation makes at least one assertion fail (12 groups)", { skip: !havePlan }, () => {
  const { plan } = loadGate2Plan(readFileSync(PLAN_PATH, "utf8"), PLAN_PATH);
  const notProven: string[] = [];
  for (const group of plan.scenarios) {
    const mutate = MUTATE[group.id];
    assert.ok(mutate, `mutation defined for ${group.id}`);
    const mutated = mutate(fake.healthy(group.id));
    const result = evaluateGroup({ ...group, negativeSeeds: [] }, { healthy: mutated, defects: {} });
    const anyFailed = result.assertionVerdicts.some((a) => !a.verdict.ok);
    if (!anyFailed) notProven.push(group.id);
  }
  assert.deepEqual(notProven, [], `these groups had no assertion fail under mutation: ${notProven.join(", ")}`);
});
