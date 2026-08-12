import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { exactKeys, isRecord, parseStrictJson } from "./json.js";
import {
  GATE3_ASSERTION_COUNT,
  GATE3_BASE_COMMIT,
  GATE3_BASE_TREE,
  GATE3_EXTERNAL_PLAN_SHA256,
  GATE3_GROUP_COUNT,
  GATE3_INTERNAL_PLAN_DIGEST,
  GATE3_NEGATIVE_COUNT,
  Gate3PlanError,
} from "./types.js";
import type {
  Gate3Json,
  Gate3NegativeSeed,
  Gate3Plan,
  Gate3ScenarioPlan,
} from "./types.js";

const TOP_KEYS = [
  "schemaVersion",
  "artifactKind",
  "status",
  "baseCommit",
  "baseTree",
  "contractFile",
  "wave2ContractFile",
  "wave2ContractDigest",
  "gate2PlanDigest",
  "planDigest",
  "clock",
  "limits",
  "oraclePolicy",
  "scenarios",
] as const;

const ORACLE_POLICY_KEYS = [
  "canonicalJson",
  "unknownKeysRejected",
  "everyNegativeAssertsUnchangedSiblings",
  "everyScenarioHasTargetedMutation",
  "fakeAndPlanDeriveExpectedErrorsIndependently",
  "realProviderCallsRequired",
  "existingWave0Wave1Wave2GatesRequired",
  "deferredScenarioDisposition",
] as const;

const LIMIT_KEYS = [
  "leaseDurationMs",
  "renewalLeadMs",
  "maxProposalChildren",
  "maxGraphNodes",
  "maxGraphEdges",
  "maxGraphDepth",
  "maxOpenLeaves",
  "maxDependenciesPerTask",
  "maxReviewSeats",
  "maxCapabilityKeys",
  "maxPathClaims",
  "maxTitleUtf8Bytes",
  "titleSourceReuseMinUtf8Bytes",
] as const;

function strings(value: Gate3Json | undefined, label: string): readonly string[] {
  if (!Array.isArray(value) || value.length === 0 || value.some((v) => typeof v !== "string" || v.length === 0)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${label} must be a nonempty string array`);
  }
  return value as readonly string[];
}

function unique(values: readonly string[], label: string): void {
  if (new Set(values).size !== values.length) {
    throw new Gate3PlanError("GATE3_PLAN_DUPLICATE_ID", label);
  }
}

function seed(value: Gate3Json, groupId: string): Gate3NegativeSeed {
  if (!isRecord(value)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${groupId} seed must be object`);
  }
  exactKeys(value, ["id", "defect", "unchanged"], ["expectError", "verifierMustObserve"], `${groupId}.seed`);
  const { id, defect, expectError, verifierMustObserve } = value;
  if (typeof id !== "string" || id.length === 0 || typeof defect !== "string" || defect.length === 0) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${groupId} seed identity/defect`);
  }
  if ((typeof expectError === "string") === (typeof verifierMustObserve === "string")) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${groupId}.${id} needs exactly one expected outcome`);
  }
  const unchanged = strings(value.unchanged, `${groupId}.${id}.unchanged`);
  unique(unchanged, `${groupId}.${id}.unchanged`);
  return {
    id,
    defect,
    ...(typeof expectError === "string" ? { expectError } : {}),
    ...(typeof verifierMustObserve === "string" ? { verifierMustObserve } : {}),
    unchanged,
  };
}

function scenario(value: Gate3Json): Gate3ScenarioPlan {
  if (!isRecord(value)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", "scenario must be object");
  }
  exactKeys(value, ["id", "fixture", "expectedFacts", "assertions", "negativeSeeds"], ["forbiddenFacts"], "scenario");
  if (typeof value.id !== "string" || !/^g3_(?:[1-9]|1[0-2])_/u.test(value.id)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", "scenario id");
  }
  if (!isRecord(value.fixture)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${value.id}.fixture`);
  }
  const expectedFacts = strings(value.expectedFacts, `${value.id}.expectedFacts`);
  const assertions = strings(value.assertions, `${value.id}.assertions`);
  if (!Array.isArray(value.negativeSeeds) || value.negativeSeeds.length === 0) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${value.id}.negativeSeeds`);
  }
  unique(expectedFacts, `${value.id}.expectedFacts`);
  unique(assertions, `${value.id}.assertions`);
  const negativeSeeds = value.negativeSeeds.map((entry) => seed(entry, value.id as string));
  unique(negativeSeeds.map((entry) => entry.id), `${value.id}.negativeSeeds`);
  const forbiddenFacts = value.forbiddenFacts === undefined
    ? undefined
    : strings(value.forbiddenFacts, `${value.id}.forbiddenFacts`);
  return {
    id: value.id,
    fixture: value.fixture,
    expectedFacts,
    assertions,
    ...(forbiddenFacts === undefined ? {} : { forbiddenFacts }),
    negativeSeeds,
  };
}

function validateBoundShape(root: Record<string, Gate3Json>): Gate3Plan {
  exactKeys(root, TOP_KEYS, [], "plan");
  if (
    root.schemaVersion !== 1 ||
    root.artifactKind !== "wave3_gate3_scenario_plan" ||
    root.status !== "eighth_review_candidate" ||
    root.baseCommit !== GATE3_BASE_COMMIT ||
    root.baseTree !== GATE3_BASE_TREE ||
    root.planDigest !== GATE3_INTERNAL_PLAN_DIGEST
  ) {
    throw new Gate3PlanError("GATE3_PLAN_BINDING_MISMATCH", "identity or base");
  }
  for (const key of ["contractFile", "wave2ContractFile", "wave2ContractDigest", "gate2PlanDigest"] as const) {
    if (typeof root[key] !== "string" || root[key].length === 0) {
      throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", key);
    }
  }
  if (
    root.clock !== "deterministic_server_clock_with_fake_local_clock" ||
    !isRecord(root.limits) ||
    !isRecord(root.oraclePolicy)
  ) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", "clock/limits/oraclePolicy");
  }
  const oraclePolicy = root.oraclePolicy;
  exactKeys(root.limits, LIMIT_KEYS, [], "limits");
  if (Object.values(root.limits).some((v) => typeof v !== "number" || !Number.isSafeInteger(v) || v <= 0)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", "limits values");
  }
  exactKeys(root.oraclePolicy, ORACLE_POLICY_KEYS, [], "oraclePolicy");
  const requiredTrue = ORACLE_POLICY_KEYS.filter((key) =>
    !["realProviderCallsRequired", "deferredScenarioDisposition"].includes(key),
  );
  if (
    requiredTrue.some((key) => oraclePolicy[key] !== true) ||
    oraclePolicy.realProviderCallsRequired !== false ||
    oraclePolicy.deferredScenarioDisposition !== "unchanged_fail_closed"
  ) {
    throw new Gate3PlanError("GATE3_PLAN_POLICY_MISMATCH", "oraclePolicy");
  }
  if (!Array.isArray(root.scenarios)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", "scenarios");
  }
  const scenarios = root.scenarios.map(scenario);
  const groupIds = scenarios.map((entry) => entry.id);
  const assertions = scenarios.flatMap((entry) => entry.assertions);
  const negatives = scenarios.flatMap((entry) => entry.negativeSeeds.map((seedEntry) => seedEntry.id));
  unique(groupIds, "scenario ids");
  unique(assertions, "assertion ids");
  unique(negatives, "negative seed ids");
  if (
    scenarios.length !== GATE3_GROUP_COUNT ||
    assertions.length !== GATE3_ASSERTION_COUNT ||
    negatives.length !== GATE3_NEGATIVE_COUNT
  ) {
    throw new Gate3PlanError(
      "GATE3_PLAN_INVENTORY_MISMATCH",
      `${scenarios.length}/${assertions.length}/${negatives.length}`,
    );
  }
  return { ...(root as Omit<Gate3Plan, "scenarios">), scenarios } as Gate3Plan;
}

export function loadGate3PlanBytes(
  bytes: Uint8Array,
): Gate3Plan {
  const digest = createHash("sha256").update(bytes).digest("hex");
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new Gate3PlanError("GATE3_PLAN_JSON_INVALID", "invalid UTF-8");
  }
  const parsed = parseStrictJson(text);
  if (!isRecord(parsed)) {
    throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", "root must be object");
  }
  const plan = validateBoundShape(parsed);
  if (digest !== GATE3_EXTERNAL_PLAN_SHA256) {
    throw new Gate3PlanError(
      "GATE3_PLAN_DIGEST_MISMATCH",
      `${digest} != ${GATE3_EXTERNAL_PLAN_SHA256}`,
    );
  }
  return plan;
}

export async function loadGate3PlanFile(
  path: string,
): Promise<Gate3Plan> {
  return loadGate3PlanBytes(await readFile(path));
}
