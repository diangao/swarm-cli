import { revalidateLedgerIngestion } from "./evidence.js";
import type { EvidenceLedger } from "./evidence.js";
import type { ConditionVerdict, Scenario } from "./scenario.js";

export type ScenarioRun = {
  readonly scenarioId: string;
  /** Every condition's verdict, in scenario order. */
  readonly verdicts: readonly ConditionVerdict[];
  /** The asserted (non-placeholder) verdicts. */
  readonly asserted: readonly ConditionVerdict[];
  /** The placeholder verdicts. */
  readonly placeholders: readonly ConditionVerdict[];
  /**
   * "pass" iff every ASSERTED condition passed. Placeholders never cause pass
   * or fail. Advisory: runner verdicts are advisory; the gate is authoritative.
   */
  readonly advisory: "pass" | "fail";
};

export function runScenario(
  scenario: Scenario,
  ledger: EvidenceLedger,
): ScenarioRun {
  // Defense-in-depth: a plain-object ledger that bypassed the builder is
  // revalidated here, so a body-bearing / grammar-invalid row throws
  // LedgerIngestError before any condition verdict is produced.
  revalidateLedgerIngestion(ledger);
  const verdicts: ConditionVerdict[] = scenario.conditions.map((condition) =>
    condition.predicate(ledger),
  );
  const asserted = verdicts.filter((v) => v.status !== "placeholder");
  const placeholders = verdicts.filter((v) => v.status === "placeholder");
  const advisory: "pass" | "fail" = asserted.every((v) => v.status === "pass")
    ? "pass"
    : "fail";
  return {
    scenarioId: scenario.scenarioId,
    verdicts,
    asserted,
    placeholders,
    advisory,
  };
}
