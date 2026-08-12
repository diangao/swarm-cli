import { createHash } from "node:crypto";

import { Gate3OracleError } from "./types.js";
import {
  assertionPasses,
  validateDomainObservation,
} from "./predicates.js";
import type {
  Gate3AssertionFact,
  Gate3AssertionResult,
  Gate3FactBundle,
  Gate3Json,
  Gate3NegativeResult,
  Gate3Plan,
  Gate3Run,
  Gate3SiblingFact,
} from "./types.js";

function canonicalJson(value: Gate3Json): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const object = value as Readonly<Record<string, Gate3Json>>;
  return `{${Object.keys(object)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key]!)}`)
    .join(",")}}`;
}

function digest(value: Gate3Json): string {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest("hex");
}

function exactOwnKeys(value: unknown, expected: readonly string[], label: string): void {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} object`);
  }
  const actual = Reflect.ownKeys(value);
  if (
    actual.some((key) => typeof key !== "string") ||
    actual.length !== expected.length ||
    expected.some((key) => !Object.hasOwn(value, key))
  ) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} exact keys`);
  }
}

function verifyFactDigest(fact: Gate3AssertionFact): void {
  exactOwnKeys(
    fact,
    ["groupId", "assertionId", "evidenceKinds", "observation", "observationDigest"],
    fact.assertionId,
  );
  validateDomainObservation(fact.observation, `${fact.assertionId}.observation`);
  const expected = digest({
    groupId: fact.groupId,
    assertionId: fact.assertionId,
    evidenceKinds: [...fact.evidenceKinds],
    observation: fact.observation,
  });
  if (fact.observationDigest !== expected) {
    throw new Gate3OracleError("GATE3_FACT_DIGEST_MISMATCH", fact.assertionId);
  }
}

function expectedOutcome(seed: {
  readonly expectError?: string;
  readonly verifierMustObserve?: string;
}): string {
  const outcome = seed.expectError ?? seed.verifierMustObserve;
  if (outcome === undefined) {
    throw new Gate3OracleError("GATE3_PLAN_EXPECTATION_MISSING", "negative seed");
  }
  return outcome;
}

function siblingMap(
  siblings: readonly Gate3SiblingFact[],
  label: string,
): ReadonlyMap<string, Gate3SiblingFact> {
  const map = new Map<string, Gate3SiblingFact>();
  for (const sibling of siblings) {
    exactOwnKeys(sibling, ["name", "revision", "valueDigest"], `${label}.sibling`);
    if (
      typeof sibling.name !== "string" ||
      !Number.isSafeInteger(sibling.revision) ||
      sibling.revision < 0 ||
      sibling.valueDigest !== digest({ name: sibling.name, revision: sibling.revision })
    ) {
      throw new Gate3OracleError("GATE3_NEGATIVE_SIBLING_INVALID", `${label}:${sibling.name}`);
    }
    if (map.has(sibling.name)) {
      throw new Gate3OracleError("GATE3_NEGATIVE_DUPLICATE_SIBLING", `${label}:${sibling.name}`);
    }
    map.set(sibling.name, sibling);
  }
  return map;
}

function assertExactNames(actual: readonly string[], expected: readonly string[], label: string): void {
  if (
    actual.length !== expected.length ||
    canonicalJson([...actual].sort()) !== canonicalJson([...expected].sort())
  ) {
    throw new Gate3OracleError("GATE3_FACT_INVENTORY_MISMATCH", label);
  }
}

export function runGate3Positive(plan: Gate3Plan, bundle: Gate3FactBundle): Gate3Run {
  exactOwnKeys(bundle, ["schemaVersion", "groups", "negatives"], "bundle");
  if (bundle.schemaVersion !== 1 || bundle.negatives.length !== 0) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", "positive bundle envelope");
  }
  const plannedGroups = new Map(plan.scenarios.map((scenario) => [scenario.id, scenario]));
  assertExactNames(
    bundle.groups.map((group) => group.groupId),
    plan.scenarios.map((scenario) => scenario.id),
    "groups",
  );
  const assertionResults: Gate3AssertionResult[] = [];
  const executions: Record<string, number> = {};
  for (const group of bundle.groups) {
    exactOwnKeys(group, ["groupId", "assertions"], `group:${group.groupId}`);
    const scenario = plannedGroups.get(group.groupId);
    if (scenario === undefined) {
      throw new Gate3OracleError("GATE3_FACT_GROUP_UNKNOWN", group.groupId);
    }
    assertExactNames(
      group.assertions.map((fact) => fact.assertionId),
      scenario.assertions,
      `${group.groupId}.assertions`,
    );
    for (const fact of group.assertions) {
      if (fact.groupId !== group.groupId) {
        throw new Gate3OracleError("GATE3_FACT_GROUP_MISMATCH", fact.assertionId);
      }
      assertExactNames(fact.evidenceKinds, scenario.expectedFacts, `${fact.assertionId}.evidence`);
      verifyFactDigest(fact);
      executions[fact.assertionId] = (executions[fact.assertionId] ?? 0) + 1;
      assertionResults.push({
        groupId: group.groupId,
        assertionId: fact.assertionId,
        passed: assertionPasses(fact.assertionId, fact.observation, scenario.fixture),
        observationDigest: fact.observationDigest,
      });
    }
  }
  if (Object.values(executions).some((count) => count !== 1)) {
    throw new Gate3OracleError("GATE3_ASSERTION_EXECUTION_COUNT", "not exact-once");
  }
  return {
    passed: assertionResults.every((result) => result.passed),
    assertionResults,
    negativeResults: [],
    assertionExecutions: executions,
    negativeExecutions: {},
  };
}

export function runGate3Negatives(plan: Gate3Plan, bundle: Gate3FactBundle): Gate3Run {
  exactOwnKeys(bundle, ["schemaVersion", "groups", "negatives"], "bundle");
  if (bundle.schemaVersion !== 1 || bundle.groups.length !== 0) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", "negative bundle envelope");
  }
  const planned = plan.scenarios.flatMap((scenario) =>
    scenario.negativeSeeds.map((seed) => ({ groupId: scenario.id, seed })),
  );
  assertExactNames(
    bundle.negatives.map((fact) => fact.seedId),
    planned.map(({ seed }) => seed.id),
    "negative seeds",
  );
  const factBySeed = new Map(bundle.negatives.map((fact) => [fact.seedId, fact]));
  const results: Gate3NegativeResult[] = [];
  const executions: Record<string, number> = {};
  for (const { groupId, seed } of planned) {
    const fact = factBySeed.get(seed.id);
    if (fact === undefined) {
      throw new Gate3OracleError("GATE3_NEGATIVE_MISSING", seed.id);
    }
    exactOwnKeys(
      fact,
      [
        "groupId",
        "seedId",
        "defectActionDigest",
        "observedOutcome",
        "outcomeSource",
        "siblingsBefore",
        "siblingsAfter",
      ],
      `negative:${seed.id}`,
    );
    if (fact.groupId !== groupId || fact.outcomeSource !== "independent_defect_registry") {
      throw new Gate3OracleError("GATE3_NEGATIVE_BINDING_MISMATCH", seed.id);
    }
    const expectedActionDigest = digest({ id: seed.id, action: seed.defect });
    if (fact.defectActionDigest !== expectedActionDigest) {
      throw new Gate3OracleError("GATE3_NEGATIVE_ACTION_MISMATCH", seed.id);
    }
    const before = siblingMap(fact.siblingsBefore, `${seed.id}.before`);
    const after = siblingMap(fact.siblingsAfter, `${seed.id}.after`);
    assertExactNames([...before.keys()], seed.unchanged, `${seed.id}.before`);
    assertExactNames([...after.keys()], seed.unchanged, `${seed.id}.after`);
    const unchanged = seed.unchanged.every((name) =>
      canonicalJson(before.get(name) as unknown as Gate3Json) ===
        canonicalJson(after.get(name) as unknown as Gate3Json),
    );
    const plannedOutcome = expectedOutcome(seed);
    executions[seed.id] = (executions[seed.id] ?? 0) + 1;
    results.push({
      groupId,
      seedId: seed.id,
      passed: fact.observedOutcome === plannedOutcome && unchanged,
      observedOutcome: fact.observedOutcome,
      expectedOutcome: plannedOutcome,
      unchangedSiblingNames: seed.unchanged,
    });
  }
  if (Object.values(executions).some((count) => count !== 1)) {
    throw new Gate3OracleError("GATE3_NEGATIVE_EXECUTION_COUNT", "not exact-once");
  }
  return {
    passed: results.every((result) => result.passed),
    assertionResults: [],
    negativeResults: results,
    assertionExecutions: {},
    negativeExecutions: executions,
  };
}
