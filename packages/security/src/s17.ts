/**
 * S17 mutual-bind: the single source of truth shared by the security lane
 * (this package) and the verifier lane (@swarm/testkit scenario S17).
 *
 * The security lane owns this module. The verifier lane imports it; it must not
 * hardcode the finding-kind -> condition-id correspondence, and it must not
 * commit a tracked marker fixture. Both lanes bind to the same generator output
 * and the same table entry: one runtime-generated marker, both gates must fire,
 * and a marker that passes either gate is a Gate 0 failure on both.
 *
 * The marker is a synthetic machine-local-path shape. It carries no real name,
 * handle, task number, channel reference, or private lineage term. It is
 * produced at test runtime from a caller seed, never stored as a tracked blob,
 * so the promoted full-history publication gate can never see it in history.
 */

/**
 * Finding kinds emitted by the promoted publication gate
 * (scripts/publication_gate.py) that the security lane maps to verifier
 * condition ids. This is an allowlist: only kinds listed here participate in
 * the mutual bind. Additions are additive and independently reviewed.
 */
export type PublicationFindingKind = "machine-local-path";

/** Verifier scenario condition ids driven by publication findings. */
export type VerifierConditionId = "publication_internal_information_leak";

/**
 * The single source of truth for the finding-kind -> condition-id
 * correspondence. Pure data, no logic. The verifier lane imports this exact
 * table; hardcoding the correspondence on either side is a review block.
 */
export const FINDING_KIND_TO_CONDITION_ID: Readonly<
  Record<PublicationFindingKind, VerifierConditionId>
> = Object.freeze({
  "machine-local-path": "publication_internal_information_leak",
});

/** The finding kind the S17 marker is engineered to trigger. */
export const S17_FINDING_KIND: PublicationFindingKind = "machine-local-path";

/** The condition id both lanes assert for the S17 seed. */
export const S17_CONDITION_ID: VerifierConditionId =
  FINDING_KIND_TO_CONDITION_ID[S17_FINDING_KIND];

/**
 * Translate a promoted-gate finding kind to a verifier condition id. Returns
 * undefined for a kind outside the shared allowlist rather than guessing.
 */
export function conditionIdForFinding(
  kind: string,
): VerifierConditionId | undefined {
  return Object.prototype.hasOwnProperty.call(
    FINDING_KIND_TO_CONDITION_ID,
    kind,
  )
    ? FINDING_KIND_TO_CONDITION_ID[kind as PublicationFindingKind]
    : undefined;
}

const FNV_OFFSET_BASIS = 0x811c9dc5;
const FNV_PRIME = 0x01000193;

/** Pure, dependency-free FNV-1a over the seed for a stable synthetic nonce. */
function seedNonce(seed: string): string {
  let hash = FNV_OFFSET_BASIS;
  for (let index = 0; index < seed.length; index += 1) {
    hash ^= seed.charCodeAt(index) & 0xff;
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

// Built by concatenation so this source file does not itself contain a
// contiguous home-path shape that the publication gate would flag. The runtime
// value is the leading-slash form the gate detects; the source is not.
const HOME_ROOT = "/" + "Users";

/**
 * Produce the S17 marker for a caller seed. Deterministic per seed and distinct
 * across seeds, so a scenario can generate many. The runtime value is a
 * synthetic developer home path — exactly the internal-information leak the
 * promoted gate exists to catch — with no real identity or lineage term.
 *
 * Distinct seeds must yield distinct markers; identical seeds must be stable.
 */
export function generateS17Marker(seed: string): string {
  return `${HOME_ROOT}/swarm-fixture-${seedNonce(seed)}/internal/lineage-marker`;
}

/**
 * The expected mutual-bind outcome for a generated marker: the promoted gate
 * must BLOCK it with S17_FINDING_KIND, which maps to S17_CONDITION_ID. Both
 * lanes assert against this without re-deriving the correspondence.
 */
export function expectedS17Outcome(): {
  readonly blocked: true;
  readonly findingKind: PublicationFindingKind;
  readonly conditionId: VerifierConditionId;
} {
  return Object.freeze({
    blocked: true as const,
    findingKind: S17_FINDING_KIND,
    conditionId: S17_CONDITION_ID,
  });
}
