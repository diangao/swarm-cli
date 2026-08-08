import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

// Regression guard for the canonical chat-task-orchestration scenario JSON's
// seven-row §5.4 placeholder disposition. The Wave-1 verifier span atomically
// corrects the stale labels: every row STAYS placeholder and fail-closed, and
// its entry_condition carries the corrected earliest-honest-bind wave. This test
// asserts the COMPLETE exact disposition so a stale "binds when Wave-1 ..." label
// (which would imply a Wave-1 bind that does not exist) cannot silently recur.

type ScenarioCondition = {
  readonly id: string;
  readonly status: string;
  readonly entry_condition?: string;
};
type ScenarioDoc = {
  readonly scenario_id: string;
  readonly conditions: readonly ScenarioCondition[];
};

const SCENARIO: ScenarioDoc = JSON.parse(
  readFileSync(
    fileURLToPath(
      new URL(
        "../../../../contracts/scenarios/chat-task-orchestration.v0.json",
        import.meta.url,
      ),
    ),
    "utf8",
  ),
) as ScenarioDoc;

// The eight asserted conditions (bound Wave-0 chat oracle), in order.
const ASSERTED_IDS = [
  "decomposed_by_runtime",
  "routed_by_capability",
  "single_owner_no_duplicate",
  "restart_no_reexecution",
  "steer_honored_precommit",
  "wake_starts_turn",
  "notice_first_body_withheld",
  "owner_only_body_read",
];

// The complete seven-row §5.4 placeholder disposition with each row's corrected
// earliest-honest-bind wave. Every row is placeholder and fail-closed.
const SEVEN_ROW_DISPOSITION: ReadonlyArray<{ id: string; earliestWave: string }> = [
  { id: "thread_owner_status_receipt", earliestWave: "Wave 3" },
  { id: "lifecycle_and_illegal_transition", earliestWave: "Wave 3" },
  { id: "task_status_freshness_hold", earliestWave: "Wave 3" },
  { id: "delivery_reply_trailer", earliestWave: "Wave 3" },
  { id: "coordination_slos", earliestWave: "Wave 5" },
  { id: "channel_navigation_exact_target", earliestWave: "Wave 5" },
  { id: "lane_role_fidelity", earliestWave: "Wave 3" },
];

function condition(id: string): ScenarioCondition {
  const c = SCENARIO.conditions.find((x) => x.id === id);
  assert.ok(c, `condition ${id} present in the canonical scenario JSON`);
  return c;
}

test("canonical scenario JSON has exactly 8 asserted + 7 placeholder = 15 conditions", () => {
  assert.equal(SCENARIO.conditions.length, 15);
  const asserted = SCENARIO.conditions.filter((c) => c.status === "asserted");
  const placeholder = SCENARIO.conditions.filter((c) => c.status === "placeholder");
  assert.equal(asserted.length, 8);
  assert.equal(placeholder.length, 7);
});

test("the eight asserted conditions are exactly the bound chat-oracle set", () => {
  const asserted = SCENARIO.conditions
    .filter((c) => c.status === "asserted")
    .map((c) => c.id);
  assert.deepEqual(asserted, ASSERTED_IDS);
});

test("the seven placeholder rows are exactly the §5.4 disposition set", () => {
  const placeholder = new Set(
    SCENARIO.conditions.filter((c) => c.status === "placeholder").map((c) => c.id),
  );
  const expected = new Set(SEVEN_ROW_DISPOSITION.map((r) => r.id));
  assert.deepEqual(placeholder, expected);
});

test("every §5.4 row stays placeholder/fail-closed with its corrected wave label", () => {
  for (const row of SEVEN_ROW_DISPOSITION) {
    const c = condition(row.id);
    assert.equal(c.status, "placeholder", `${row.id} must remain placeholder`);
    assert.ok(
      c.entry_condition !== undefined && c.entry_condition.length > 0,
      `${row.id} must carry an entry_condition`,
    );
    assert.ok(
      c.entry_condition.includes(row.earliestWave),
      `${row.id} entry_condition must carry corrected earliest-bind ${row.earliestWave}`,
    );
    // No stale "binds when Wave-1 ..." label may recur: the corrected form
    // states Wave 1's ABSENCE of the surface, never a Wave-1 bind.
    assert.ok(
      !/binds when wave[\s-]?1/iu.test(c.entry_condition),
      `${row.id} entry_condition must not carry a stale Wave-1 bind label`,
    );
  }
});
