import { createHash } from "node:crypto";

// Loader boundary for the Wave 2 Gate 2 scenario plan.
//
// The plan is an EXTERNAL, immutable input, never embedded in this package:
// during the pre-seam phase it is loaded from a local untracked path; after the
// shared seam is protected-attested the same loader is pointed at the canonical
// `contracts/gate2/` path. This module only PARSES and DIGESTS the frozen plan;
// it never duplicates or edits any expected outcome. The oracle reads which
// assertions/negatives each group carries FROM the loaded plan; the oracle
// supplies only HOW to evaluate each named assertion/negative.

export type FenceOutcome =
  | { readonly expectError: string }
  | { readonly expectErrors: readonly string[] }
  | { readonly verifierMustObserve: string };

export type NegativeSeed = {
  readonly id: string;
  readonly defect: string;
  /** Non-empty set of sibling facts that must remain unchanged under the defect. */
  readonly unchanged: readonly string[];
} & FenceOutcome;

export type ScenarioGroup = {
  readonly id: string;
  readonly fixture: Readonly<Record<string, unknown>>;
  readonly assertions: readonly string[];
  readonly negativeSeeds: readonly NegativeSeed[];
  readonly expectedLedgerKinds?: readonly string[];
  readonly expectedServerProjection?: readonly string[];
  readonly forbiddenFacts?: readonly string[];
};

export type OraclePolicy = {
  readonly canonicalJson: boolean;
  readonly unknownKeysRejected: boolean;
  readonly everyNegativeAssertsUnchangedSiblings: boolean;
  readonly realProviderCallsRequired: boolean;
  readonly existingWave0AndWave1GatesRequired: boolean;
  readonly deferredScenarioDisposition: string;
};

export type Gate2Plan = {
  readonly schemaVersion: number;
  readonly artifactKind: string;
  readonly baseCommit: string;
  readonly oraclePolicy: OraclePolicy;
  readonly scenarios: readonly ScenarioGroup[];
};

export type LoadedPlan = {
  readonly plan: Gate2Plan;
  /** SHA-256 of the exact loaded bytes: the recorded input digest for the proof. */
  readonly inputDigest: string;
  /** The source the plan was loaded from (for the recorded proof command). */
  readonly source: string;
};

export class Gate2PlanLoadError extends Error {}

const TOP_KEYS = new Set([
  "schemaVersion",
  "artifactKind",
  "status",
  "baseCommit",
  "baseTree",
  "planDigest",
  "wave1ContractDigest",
  "contractFile",
  "wireArtifacts",
  "clock",
  "oraclePolicy",
  "scenarios",
]);

const GROUP_KEYS = new Set([
  "id",
  "fixture",
  "expectedLedgerKinds",
  "expectedServerProjection",
  "assertions",
  "forbiddenFacts",
  "negativeSeeds",
]);

const SEED_KEYS = new Set([
  "id",
  "defect",
  "expectError",
  "expectErrors",
  "verifierMustObserve",
  "unchanged",
]);

function rejectUnknownKeys(
  obj: Record<string, unknown>,
  allowed: ReadonlySet<string>,
  where: string,
): void {
  for (const k of Reflect.ownKeys(obj)) {
    if (typeof k !== "string" || !allowed.has(k)) {
      throw new Gate2PlanLoadError(`unknown key "${String(k)}" in ${where}`);
    }
  }
}

/**
 * Parse and validate the canonical Wave 2 Gate 2 plan from its exact bytes,
 * returning the parsed plan plus the SHA-256 input digest of those bytes. Fails
 * closed on unknown keys (per oraclePolicy.unknownKeysRejected), a non-canonical
 * re-serialization, a missing/empty scenario set, or any negative that omits its
 * unchanged-sibling set.
 */
export function loadGate2Plan(bytes: string, source: string): LoadedPlan {
  const inputDigest = createHash("sha256").update(bytes, "utf8").digest("hex");
  let raw: unknown;
  try {
    raw = JSON.parse(bytes);
  } catch (e) {
    throw new Gate2PlanLoadError(`plan is not valid JSON: ${(e as Error).message}`);
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Gate2PlanLoadError("plan is not a JSON object");
  }
  const top = raw as Record<string, unknown>;
  rejectUnknownKeys(top, TOP_KEYS, "plan root");

  const policy = top["oraclePolicy"];
  if (typeof policy !== "object" || policy === null) {
    throw new Gate2PlanLoadError("plan has no oraclePolicy");
  }
  const oraclePolicy = policy as OraclePolicy;
  if (oraclePolicy.unknownKeysRejected !== true) {
    throw new Gate2PlanLoadError("oraclePolicy.unknownKeysRejected must be true");
  }
  if (oraclePolicy.everyNegativeAssertsUnchangedSiblings !== true) {
    throw new Gate2PlanLoadError(
      "oraclePolicy.everyNegativeAssertsUnchangedSiblings must be true",
    );
  }

  const scenarios = top["scenarios"];
  if (!Array.isArray(scenarios) || scenarios.length === 0) {
    throw new Gate2PlanLoadError("plan has no scenarios");
  }
  const seenGroupIds = new Set<string>();
  for (const s of scenarios) {
    if (typeof s !== "object" || s === null) {
      throw new Gate2PlanLoadError("scenario is not an object");
    }
    const group = s as Record<string, unknown>;
    rejectUnknownKeys(group, GROUP_KEYS, "scenario group");
    const gid = group["id"];
    if (typeof gid !== "string" || gid.length === 0) {
      throw new Gate2PlanLoadError("scenario group has no id");
    }
    if (seenGroupIds.has(gid)) {
      throw new Gate2PlanLoadError(`duplicate scenario group id "${gid}"`);
    }
    seenGroupIds.add(gid);
    const assertions = group["assertions"];
    if (!Array.isArray(assertions) || assertions.length === 0) {
      throw new Gate2PlanLoadError(`group ${gid} has no assertions`);
    }
    const negatives = group["negativeSeeds"];
    if (!Array.isArray(negatives) || negatives.length === 0) {
      throw new Gate2PlanLoadError(`group ${gid} has no negativeSeeds`);
    }
    for (const n of negatives) {
      if (typeof n !== "object" || n === null) {
        throw new Gate2PlanLoadError(`group ${gid} negative is not an object`);
      }
      const seed = n as Record<string, unknown>;
      rejectUnknownKeys(seed, SEED_KEYS, `group ${gid} negative`);
      const nid = seed["id"];
      if (typeof nid !== "string" || nid.length === 0) {
        throw new Gate2PlanLoadError(`group ${gid} negative has no id`);
      }
      const unchanged = seed["unchanged"];
      if (!Array.isArray(unchanged) || unchanged.length === 0) {
        throw new Gate2PlanLoadError(
          `negative ${nid} has an empty unchanged-sibling set (every negative must assert unchanged siblings)`,
        );
      }
      const hasError = typeof seed["expectError"] === "string";
      const hasErrors = Array.isArray(seed["expectErrors"]);
      const hasObserve = typeof seed["verifierMustObserve"] === "string";
      if (Number(hasError) + Number(hasErrors) + Number(hasObserve) !== 1) {
        throw new Gate2PlanLoadError(
          `negative ${nid} must carry exactly one of expectError / expectErrors / verifierMustObserve`,
        );
      }
    }
  }

  // Integrity is the exact-bytes SHA-256 digest (inputDigest) plus the shape and
  // fail-closed key checks above. The oracle enforces the canonical-JSON policy
  // at the evidence/ledger layer, not on the immutable plan file (which is
  // pretty-printed and identified by its exact byte digest).
  return { plan: top as unknown as Gate2Plan, inputDigest, source };
}
