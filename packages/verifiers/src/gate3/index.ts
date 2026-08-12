export {
  canonicalBlockedClosureOracleInput,
  decodeBlockedClosureOracleInput,
  runBlockedClosureOracle,
  BlockedClosureInputError,
} from "./blocked-closure.js";
export type { BlockedClosureOracleInput } from "./blocked-closure.js";
export { loadGate3PlanBytes, loadGate3PlanFile } from "./loader.js";
export { runGate3Negatives, runGate3Positive } from "./oracle.js";
export { GATE3_ASSERTION_PREDICATE_IDS } from "./predicates.js";
export {
  GATE3_ASSERTION_COUNT,
  GATE3_BASE_COMMIT,
  GATE3_BASE_TREE,
  GATE3_EXTERNAL_PLAN_SHA256,
  GATE3_GROUP_COUNT,
  GATE3_INTERNAL_PLAN_DIGEST,
  GATE3_NEGATIVE_COUNT,
  Gate3OracleError,
  Gate3PlanError,
} from "./types.js";
export type {
  Gate3AssertionFact,
  Gate3AssertionObservation,
  Gate3AssertionResult,
  Gate3FactBundle,
  Gate3Json,
  Gate3NegativeResult,
  Gate3NegativeSeed,
  Gate3Plan,
  Gate3Run,
  Gate3ScenarioPlan,
  Gate3SiblingFact,
} from "./types.js";
