import assert from "node:assert/strict";
import test from "node:test";
import {
  GATE3_FACT_KINDS,
  GATE3_SCENARIO_IDS,
  Gate3FactRecorder,
  createTaskLeaseV3Fixture,
  mintArtifactId,
  mintRepositoryId,
  resetTaskV3Ids,
} from "../src/index.js";

test("Gate 3 fact vocabulary is frozen and unique", () => {
  assert.equal(GATE3_SCENARIO_IDS.length, 12);
  assert.equal(GATE3_FACT_KINDS.length, 26);
  assert.equal(new Set(GATE3_SCENARIO_IDS).size, 12);
  assert.equal(new Set(GATE3_FACT_KINDS).size, 26);
});

test("Wave 3 ids and lease fixtures use the production grammar", () => {
  resetTaskV3Ids();
  assert.match(mintRepositoryId(), /^rpo_[0-9a-hjkmnp-tv-z]{26}$/u);
  assert.match(mintArtifactId(), /^art_[0-9a-hjkmnp-tv-z]{26}$/u);
  const lease = createTaskLeaseV3Fixture();
  assert.equal(lease.leaseRevision, 1);
  assert.ok(lease.expiresAt > lease.acquiredAt);
});

test("fact recorder preserves one canonical ordinal stream", () => {
  const recorder = new Gate3FactRecorder();
  const first = recorder.record({ scenarioId: "g3_1_atomic_claim_contention", kind: "claim_won",
    at: "2026-08-11T16:00:00.000Z" as never, identifiers: { contenders: 5 } });
  const second = recorder.record({ scenarioId: "g3_1_atomic_claim_contention", kind: "task_status_changed",
    at: "2026-08-11T16:00:00.001Z" as never, identifiers: { status: "in_progress" } });
  assert.deepEqual([first.ordinal, second.ordinal], [1, 2]);
  assert.equal(recorder.count("claim_won"), 1);
  assert.ok(Object.isFrozen(recorder.snapshot()));
});
