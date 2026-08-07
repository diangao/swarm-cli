import { createHash } from "node:crypto";
import type {
  AgentId,
  ArtifactDigest,
  ChannelId,
  ClaimId,
  CommandId,
  ConversationId,
  DeliveryId,
  FenceToken,
  LaunchId,
  LeaseId,
  MachineId,
  MessageId,
  ProducerFactId,
  ReceiptId,
  ServerId,
  SessionId,
  StateInstanceId,
  TaskId,
  TurnId,
} from "@swarm/protocol";

// Deterministic grammar-valid id minting for test doubles.
//
// Protocol ids are opaque branded strings, but the wire parser enforces the
// exact grammar `^(prefix)_[0-9a-hjkmnp-tv-z]{26}$` (lowercase Crockford
// Base32, 26 chars). Fixtures that cast arbitrary strings (e.g. "session-1")
// into branded ids would produce values the REAL parser rejects, so a scenario
// whose evidence round-trips an id through the parser could pass on malformed
// ids that could never occur in production. That is a latent false-negative in
// the verifier itself.
//
// This module mints ids that are byte-for-byte valid under the protocol
// grammar, derived deterministically from a per-prefix counter, so fixture ids
// are indistinguishable from real ones to the validator and round-trip cleanly.
// No production code should import this module.

const CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"; // lowercase, matches [0-9a-hjkmnp-tv-z]

// Deterministic 26-char Crockford body from a monotone seed. Not
// cryptographic; determinism is the point so fixtures are reproducible.
function body(seed: number): string {
  let n = seed + 1; // avoid all-zero body being visually confusing
  const out: string[] = [];
  for (let i = 0; i < 26; i += 1) {
    out.push(CROCKFORD[n % 32] as string);
    n = Math.floor(n / 32) + (i + 1) * 2654435761;
    n = n >>> 0;
  }
  return out.join("");
}

const counters = new Map<string, number>();

/** Advance the per-prefix counter and return the new value. */
function next(prefix: string): number {
  const value = (counters.get(prefix) ?? 0) + 1;
  counters.set(prefix, value);
  return value;
}

function mint<Id extends string>(prefix: string): Id {
  return `${prefix}_${body(next(prefix))}` as Id;
}

// Reset all counters between scenarios for reproducible fixtures.
export function resetIdCounters(): void {
  counters.clear();
}

export const mintServerId = (): ServerId => mint<ServerId>("srv");
export const mintMachineId = (): MachineId => mint<MachineId>("mch");
export const mintAgentId = (): AgentId => mint<AgentId>("agt");
export const mintChannelId = (): ChannelId => mint<ChannelId>("chn");
export const mintConversationId = (): ConversationId => mint<ConversationId>("cvs");
export const mintMessageId = (): MessageId => mint<MessageId>("msg");
export const mintDeliveryId = (): DeliveryId => mint<DeliveryId>("dlv");
export const mintProducerFactId = (): ProducerFactId => mint<ProducerFactId>("fac");
export const mintTaskId = (): TaskId => mint<TaskId>("tsk");
export const mintClaimId = (): ClaimId => mint<ClaimId>("clm");
export const mintLeaseId = (): LeaseId => mint<LeaseId>("lse");
export const mintLaunchId = (): LaunchId => mint<LaunchId>("lnc");
export const mintCommandId = (): CommandId => mint<CommandId>("cmd");
export const mintReceiptId = (): ReceiptId => mint<ReceiptId>("rcp");
export const mintStateInstanceId = (): StateInstanceId => mint<StateInstanceId>("sti");
export const mintTurnId = (): TurnId => mint<TurnId>("trn");
export const mintSessionId = (): SessionId => mint<SessionId>("ses");

// FenceToken shares the `_[0-9a-hjkmnp-tv-z]{26}` body grammar under an `fnc_`
// prefix. It routes through the SAME counter/mint helper so consecutive calls
// increment and never collide (the previous read-without-increment bug always
// returned body(1)).
export const mintFenceToken = (): FenceToken =>
  `fnc_${body(next("fnc"))}` as FenceToken;

// ArtifactDigest is NOT the branded-id grammar: Protocol requires
// `^sha256:[0-9a-f]{64}$` (64 lowercase hex). We emit a real SHA-256 over a
// deterministic monotone counter, so every digest is valid hex, unique across
// consecutive calls, and reproducible for a given call ordinal.
export const mintArtifactDigest = (): ArtifactDigest => {
  const ordinal = next("sha256");
  const hex = createHash("sha256")
    .update(`artifact-digest:${ordinal}`)
    .digest("hex");
  return `sha256:${hex}` as ArtifactDigest;
};
