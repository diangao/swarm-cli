// Turn-machine error codes. Every code below is the EXACT string used by the
// real seam it corresponds to:
//   - storage:  packages/storage/src/errors.ts (StorageErrorCode), surfaced by
//               the promoted AtomicTurnJournalPort in runtime-journal.ts (e.g.
//               INVALID_JOURNAL_CHAIN / WRITE_STARTED_BINDING_MISMATCH /
//               INVOCATION_STATE_CONFLICT / DRIVER_EVENT_* / ACTIVE_TURN_*).
//   - drivers:  packages/drivers/src/normalizer.ts (Native/Driver error codes).
// The turn module is standalone (no @swarm/storage import is permitted by the
// package-boundary policy for daemon-core), so the code strings are re-declared
// here verbatim rather than imported. Tests assert on `.code`.
//
// Invariant: every code DECLARED here is THROWN somewhere in machine.ts and
// EXERCISED by a killing control in test/turn.test.ts. A declared-but-unthrown
// code is a wiring defect.

export type TurnErrorCode =
  // §8.5b invocation-generation gating (distinct, never collapsed):
  | "STALE_INVOCATION_GENERATION"
  | "INVOCATION_STATE_CONFLICT"
  // driver-event cursor / per-contribution fence (promoted port surfaces these
  // on the reader-fence + cursor-ordinal boundary):
  | "DRIVER_EVENT_FENCE_MISMATCH"
  | "DRIVER_EVENT_ORDER_INVALID"
  // active-turn admission / steer gating (promoted beginTurnContribution CAS):
  | "ACTIVE_TURN_CONFLICT"
  | "ACTIVE_TURN_NOT_STEERABLE"
  // ambiguous write hold:
  | "AMBIGUOUS_NATIVE_WRITE"
  // fail-closed boundary predecessors (reply/completion ordering):
  | "MULTIPLE_ASSISTANT_REPLIES"
  | "ASSISTANT_REPLY_BEFORE_MODEL_VISIBLE"
  | "TURN_COMPLETION_WITHOUT_REPLY"
  | "SECOND_COORDINATION_CALL"
  // promoted-port stored-truth join / chain-anchor surfaces the machine drives:
  | "INVALID_JOURNAL_CHAIN"
  | "WRITE_STARTED_BINDING_MISMATCH"
  // invariant guard (should never be reachable through validated input):
  | "INVALID_STATE_TRANSITION";

export class TurnError extends Error {
  readonly code: TurnErrorCode;
  readonly detail?: unknown;

  constructor(code: TurnErrorCode, detail?: unknown) {
    super(code);
    this.name = "TurnError";
    this.code = code;
    if (detail !== undefined) this.detail = detail;
  }
}

export function turnFail(code: TurnErrorCode, detail?: unknown): never {
  throw new TurnError(code, detail);
}
