import type { EvidenceLedger } from "./evidence.js";

/** A condition either passes, fails, or is an unbound Wave-1 placeholder. */
export type ConditionStatus = "pass" | "fail" | "placeholder";

export type ConditionVerdict = {
  readonly conditionId: string;
  readonly status: ConditionStatus;
  readonly reason: string;
};

export type Condition = {
  readonly id: string;
  /** The neutral evidence kinds this condition reads. */
  readonly evidence: readonly string[];
  readonly predicate: (ledger: EvidenceLedger) => ConditionVerdict;
};

export type Scenario = {
  readonly scenarioId: string;
  readonly conditions: readonly Condition[];
};
