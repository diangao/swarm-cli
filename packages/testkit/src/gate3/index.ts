export {
  canonicalJson,
  digestGate3Json,
  snapshotSibling,
} from "./facts.js";
export type {
  Gate3AssertionFact,
  Gate3AssertionObservation,
  Gate3FactBundle,
  Gate3GroupFacts,
  Gate3Json,
  Gate3NegativeFact,
  Gate3PlanScenarioShape,
  Gate3PlanShape,
  Gate3Scalar,
  Gate3SiblingFact,
} from "./facts.js";
export { GATE3_ASSERTION_PRODUCER_IDS } from "./domain.js";
export {
  corruptObservation,
  corruptScalar,
  createGroupMutation,
  createNegativeGate3Facts,
  createPositiveGate3Facts,
  executeNegativeDefect,
  expectedOutcomeForDefectAction,
} from "./simulator.js";
