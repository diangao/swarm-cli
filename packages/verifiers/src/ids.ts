import type {
  AgentId,
  DeliveryId,
  MessageId,
  ProducerFactId,
  ReceiptId,
  ServerId,
  TaskId,
  TurnId,
} from "@swarm/protocol";

// Grammar-valid id minting for verifier fixtures and evidence.
//
// The protocol wire parser enforces the exact grammar
// `^(prefix)_[0-9a-hjkmnp-tv-z]{26}$` (lowercase Crockford Base32, 26 chars).
// Casting arbitrary strings (e.g. "task-1") into branded ids would produce
// values the REAL parser rejects, so a verifier that round-trips ids through
// the parser could pass on ids that can never occur in production.
//
// This module mints ids that are byte-for-byte valid under that grammar,
// derived deterministically from a per-prefix counter. It is a self-contained
// copy of the testkit id-minting behavior (testkit does NOT re-export these
// minters from its public `index.ts`, and its `exports` map exposes no `ids`
// subpath, so they are not reachable from this package without editing testkit,
// which is forbidden). Determinism is intentional so fixtures are reproducible.

const CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"; // lowercase, matches [0-9a-hjkmnp-tv-z]

function body(seed: number): string {
  let n = seed + 1;
  const out: string[] = [];
  for (let i = 0; i < 26; i += 1) {
    out.push(CROCKFORD[n % 32] as string);
    n = Math.floor(n / 32) + (i + 1) * 2654435761;
    n = n >>> 0;
  }
  return out.join("");
}

const counters = new Map<string, number>();

function mint<Id extends string>(prefix: string): Id {
  const next = (counters.get(prefix) ?? 0) + 1;
  counters.set(prefix, next);
  return `${prefix}_${body(next)}` as Id;
}

// Reset all counters between scenarios/tests for reproducible fixtures.
export function resetIdCounters(): void {
  counters.clear();
}

export const mintServerId = (): ServerId => mint<ServerId>("srv");
export const mintAgentId = (): AgentId => mint<AgentId>("agt");
export const mintDeliveryId = (): DeliveryId => mint<DeliveryId>("dlv");
export const mintMessageId = (): MessageId => mint<MessageId>("msg");
export const mintProducerFactId = (): ProducerFactId =>
  mint<ProducerFactId>("fac");
export const mintTaskId = (): TaskId => mint<TaskId>("tsk");
export const mintReceiptId = (): ReceiptId => mint<ReceiptId>("rcp");
export const mintTurnId = (): TurnId => mint<TurnId>("trn");
