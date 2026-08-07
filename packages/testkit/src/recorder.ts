import type { TurnId } from "@swarm/protocol";

// Records what actually crossed into a model turn's input, as structured facts.
// This is the assertion surface for notice-first / body-withheld checks. It NEVER
// stores raw prompt text or body content: only hashes, flags, and counts.

/** The model saw notice metadata only; no body was read into the turn. */
export type MetadataOnlySeen = {
  readonly kind: "metadata_only";
  readonly turnId: TurnId;
  readonly steerIncluded: boolean;
  /** Optional hash of the frozen manifest the turn compiled against. */
  readonly manifestHash?: string;
  /** How many notices were surfaced (metadata rows), never their content. */
  readonly noticeCount: number;
};

/** The model read a body into the turn; recorded as a hash, never the text. */
export type BodyReadSeen = {
  readonly kind: "body_read";
  readonly turnId: TurnId;
  readonly steerIncluded: boolean;
  readonly manifestHash?: string;
  /** Hash of the body content that was read; the raw body is never stored. */
  readonly bodyHash: string;
};

/** Discriminated record of a single model-input observation. */
export type ModelSeenFact = MetadataOnlySeen | BodyReadSeen;

/** Input shape for a metadata-only observation. */
export type RecordMetadataOnlyInput = {
  readonly turnId: TurnId;
  readonly steerIncluded: boolean;
  readonly noticeCount: number;
  readonly manifestHash?: string;
};

/** Input shape for a body-read observation. */
export type RecordBodyReadInput = {
  readonly turnId: TurnId;
  readonly steerIncluded: boolean;
  readonly bodyHash: string;
  readonly manifestHash?: string;
};

export class ModelSeenRecorder {
  readonly #facts: ModelSeenFact[] = [];

  /** Record that a turn saw notice metadata only (no body). */
  recordMetadataOnly(input: RecordMetadataOnlyInput): MetadataOnlySeen {
    const fact: MetadataOnlySeen = {
      kind: "metadata_only",
      turnId: input.turnId,
      steerIncluded: input.steerIncluded,
      noticeCount: input.noticeCount,
      ...(input.manifestHash === undefined
        ? {}
        : { manifestHash: input.manifestHash }),
    };
    this.#facts.push(fact);
    return fact;
  }

  /** Record that a turn read a body (stored as a hash, never raw text). */
  recordBodyRead(input: RecordBodyReadInput): BodyReadSeen {
    const fact: BodyReadSeen = {
      kind: "body_read",
      turnId: input.turnId,
      steerIncluded: input.steerIncluded,
      bodyHash: input.bodyHash,
      ...(input.manifestHash === undefined
        ? {}
        : { manifestHash: input.manifestHash }),
    };
    this.#facts.push(fact);
    return fact;
  }

  /** All recorded facts in observation order. */
  facts(): readonly ModelSeenFact[] {
    return this.#facts;
  }

  /** Facts scoped to a single turn. */
  factsForTurn(turnId: TurnId): readonly ModelSeenFact[] {
    return this.#facts.filter((fact) => fact.turnId === turnId);
  }

  /** True iff no body was ever read across all recorded turns. */
  bodyWithheldEverywhere(): boolean {
    return this.#facts.every((fact) => fact.kind === "metadata_only");
  }
}
