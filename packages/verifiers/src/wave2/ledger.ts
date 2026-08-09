// Neutral Wave 2 evidence model. The canonical fake (@swarm/testkit) emits a
// `Wave2Ledger` (events plus a sibling-fact snapshot); the oracle's
// assertion/negative checkers consume it. The shared shape lives in the testkit
// (the fact producer); this module re-exports it and provides read helpers.
// Every field is neutral: kinds, ids, epochs, counts, flags, digests — never a
// body / prompt / raw vendor payload / secret.

import type { LedgerEvent, Wave2Ledger } from "@swarm/testkit";

export type { LedgerEvent, Wave2Ledger };

export function events(ledger: Wave2Ledger, kind: string): readonly LedgerEvent[] {
  return ledger.events.filter((e) => e.kind === kind);
}

export function countKind(ledger: Wave2Ledger, kind: string): number {
  return events(ledger, kind).length;
}

export function hasKind(ledger: Wave2Ledger, kind: string): boolean {
  return ledger.events.some((e) => e.kind === kind);
}

/** Ordinal index of the first event of `kind`, or -1 if absent. */
export function firstOrdinal(ledger: Wave2Ledger, kind: string): number {
  return ledger.events.findIndex((e) => e.kind === kind);
}

/**
 * The exact set of sibling names named by a negative seed must be byte-unchanged
 * between the healthy baseline and the defect ledger. Returns the first sibling
 * whose value changed (or that is missing on either side), else undefined.
 */
export function firstChangedSibling(
  baseline: Wave2Ledger,
  defect: Wave2Ledger,
  names: readonly string[],
): string | undefined {
  for (const name of names) {
    const a = baseline.siblings[name];
    const b = defect.siblings[name];
    if (a === undefined || b === undefined || a !== b) return name;
  }
  return undefined;
}
