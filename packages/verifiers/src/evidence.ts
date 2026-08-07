import type {
  DeliveryId,
  MessageId,
  ReceiptId,
  TaskId,
  TransitionReceipt,
  TurnId,
} from "@swarm/protocol";

// Durable, neutral evidence model.
//
// CRITICAL INVARIANT: the ledger holds ONLY durable, neutral evidence:
// protocol receipts and typed ledger-fact rows that carry ids / hashes / refs /
// counts / timestamps. It NEVER holds message bodies, prompt text,
// chain-of-thought, real names, file paths, or credentials.
//
// Actors are conceptually `owner` / `worker-1..N` / `verifier`. Seats are
// referenced as opaque neutral labels (a `Seat` string such as "worker-1"),
// never a real name. All protocol ids are grammar-valid branded ids minted via
// the minters in `./ids.ts`.

/** Neutral seat label, e.g. "owner" | "worker-1" | "verifier". Not a real name. */
export type Seat = string;

/** A capability/lane tag a subtask is typed with, e.g. "capability-A". */
export type Capability = string;

/** Provenance of a decomposition: a runtime (model) turn, or a human author. */
export type DecompositionAuthor = "runtime" | "human";

export type DecompositionSubtask = {
  readonly taskId: TaskId;
  readonly capability: Capability;
};

/**
 * Parent task decomposed into subtasks. A runtime decomposition carries a
 * `modelTurnId` (model provenance); a stub/checklist-authored one does not.
 */
export type DecompositionFact = {
  readonly parentTaskId: TaskId;
  readonly subtasks: readonly DecompositionSubtask[];
  readonly authoredBy: DecompositionAuthor;
  readonly modelTurnId?: TurnId;
};

/** Routing decision for a subtask: which capability it was routed to and, if a
 * matching worker seat exists, that seat. Absence of `matchedWorkerSeat` means
 * unroutable at route time. */
export type RouteFact = {
  readonly subtaskId: TaskId;
  readonly routedCapability: Capability;
  readonly matchedWorkerSeat?: Seat;
};

export type ClaimOutcome = "won" | "lost" | "conflict_stop";

export type ClaimAttemptFact = {
  readonly subtaskId: TaskId;
  readonly seat: Seat;
  readonly outcome: ClaimOutcome;
  /**
   * Monotonic evidence ordinal placing this claim in the global order the
   * delivery contract requires: body durability < every NOTICE < every claim <
   * every explicit owner body query. Lets the delivery predicates prove
   * durable-body-before-fanout and post-claim querying rather than surrogate
   * per-lane insertion order.
   */
  readonly ordinal: number;
};

/** Execution record for a subtask by a seat, with an execution count that
 * spans restarts (a completed subtask re-executed after restart shows > 1). */
export type ExecutionFact = {
  readonly subtaskId: TaskId;
  readonly seat: Seat;
  readonly executionCount: number;
};

/** Canonical subtask lifecycle status posted into the thread. */
export type ThreadStatus =
  | "todo"
  | "in_progress"
  | "in_review"
  | "done"
  | "closed";

/** Per-subtask status/owner row with a reference to a durable server receipt. */
export type ThreadStatusFact = {
  readonly subtaskId: TaskId;
  readonly ownerSeat: Seat;
  readonly status: ThreadStatus;
  readonly receiptId: ReceiptId;
};

/** Provenance of a worker turn, joined to the canonical subtask/owner lane it
 * belongs to. A turn with no `wakeDeliveryId` originates from a resident polling
 * loop rather than an external wake event. `subtaskId` binds the wake to a
 * canonical winning owner so wake provenance can be required per winning lane. */
export type WakeProvenanceFact = {
  readonly turnId: TurnId;
  readonly subtaskId: TaskId;
  readonly wakeDeliveryId?: DeliveryId;
};

/**
 * Durable-store receipt for the canonical goal body: proof the full body was
 * committed to durable storage (with a content digest and positive byte size)
 * BEFORE any content-free NOTICE fanned out. Its ordinal anchors the global
 * evidence order (durability < notices < claims < body queries). Without it the
 * delivery predicates cannot prove durable-body-before-fanout, only weaker
 * per-lane surrogates.
 */
export type BodyDurabilityFact = {
  readonly parentTaskId: TaskId;
  /** Validated lowercase-hex SHA-256 digest of the durable body (64 chars). */
  readonly bodyDigest: string;
  /** Byte size of the durable body; must be > 0. */
  readonly bodyBytes: number;
  /** Durable-store receipt id. */
  readonly receiptId: ReceiptId;
  /** Monotonic evidence ordinal; must precede every NOTICE. */
  readonly ordinal: number;
};

export type DeliveryObservationKind = "notice_metadata" | "body_read";

/**
 * A content-free NOTICE delivery: metadata only, body withheld. Carries the
 * validated notice fields the frozen contract requires (target ref, unread
 * count, first/latest message ids) and asserts bodyPresent:false. Its ordinal
 * must follow durability and precede that lane's claim and any body query.
 */
export type NoticeDeliveryFact = {
  readonly kind: "notice_metadata";
  readonly seat: Seat;
  readonly deliveryId: DeliveryId;
  readonly subtaskId: TaskId;
  /** A NOTICE never carries the body. */
  readonly bodyPresent: false;
  /** Validated delivery target ref (the channel/thread the notice pertains to). */
  readonly target: string;
  /** Unread count carried by the notice metadata. */
  readonly count: number;
  /** First and latest message ids the notice spans (grammar-valid MessageIds). */
  readonly firstMessageId: MessageId;
  readonly latestMessageId: MessageId;
  /** Monotonic evidence ordinal. */
  readonly ordinal: number;
};

/**
 * An explicit post-claim owner body query: the winner deliberately queried the
 * full body AFTER claiming, on its own wake turn. Carries the query target,
 * queried message id, and the turn the query ran on. Its ordinal must follow
 * the winner's claim ordinal.
 */
export type BodyQueryDeliveryFact = {
  readonly kind: "body_read";
  readonly seat: Seat;
  readonly deliveryId: DeliveryId;
  readonly subtaskId: TaskId;
  /** A body read is only sound as an explicit query, never an ambient push. */
  readonly explicitQuery: true;
  /** Validated query target ref. */
  readonly queryTarget: string;
  /** The message id whose body was queried (grammar-valid MessageId). */
  readonly queriedMessageId: MessageId;
  /** The turn the query ran on; must equal the subtask's external-wake turn. */
  readonly queryTurnId: TurnId;
  /** Monotonic evidence ordinal; must follow the winner's claim. */
  readonly ordinal: number;
};

/** What crossed into a seat: content-free NOTICE metadata or an explicit
 * post-claim body query. A strict discriminated union so a NOTICE can never
 * silently carry body fields and a body read can never be an ambient push. */
export type DeliveryObservationFact = NoticeDeliveryFact | BodyQueryDeliveryFact;

/** A human steer against an in-flight subtask, tied to an expected turn. */
export type SteerFact = {
  readonly subtaskId: TaskId;
  readonly expectedTurnId: TurnId;
  readonly appliedBeforeCommit: boolean;
  readonly committedStale: boolean;
  readonly requiresReplanHold: boolean;
};

/**
 * Evidence that a restart/replay occurred for a subtask. Its presence is what
 * lets `restart_no_reexecution` fail closed on an empty ledger: without at least
 * one restart marker there is nothing proving the restart leg ran, so the
 * condition cannot vacuously pass. `replayOf` references the delivery/wake the
 * restart replayed (a neutral id ref), never a body.
 */
export type RestartFact = {
  readonly subtaskId: TaskId;
  /** A neutral marker id for the restart/replay episode. */
  readonly restartMarker: DeliveryId;
  /** The delivery this restart replayed, if any (idempotency-key style ref). */
  readonly replayOf?: DeliveryId;
};

/**
 * Neutral per-seat activity for a subtask a seat did NOT win. Lets
 * `owner_only_body_read` prove the structural zeros (loser replies/executions)
 * without any body content: it carries counts and refs only.
 */
export type LoserActivityFact = {
  readonly subtaskId: TaskId;
  readonly seat: Seat;
  /** Outward replies this loser emitted for the subtask (must be 0). */
  readonly replyCount: number;
  /** Executions this loser ran for the subtask (must be 0). */
  readonly executionCount: number;
};

/** The read-only durable evidence the oracle predicates read. */
export type EvidenceLedger = {
  readonly receipts: readonly TransitionReceipt[];
  readonly decompositions: readonly DecompositionFact[];
  readonly routes: readonly RouteFact[];
  readonly claims: readonly ClaimAttemptFact[];
  readonly executions: readonly ExecutionFact[];
  readonly threadStatuses: readonly ThreadStatusFact[];
  readonly wakes: readonly WakeProvenanceFact[];
  readonly bodyDurability: readonly BodyDurabilityFact[];
  readonly deliveries: readonly DeliveryObservationFact[];
  readonly steers: readonly SteerFact[];
  readonly restarts: readonly RestartFact[];
  readonly loserActivity: readonly LoserActivityFact[];
  /** The set of worker seats considered available for routing checks. */
  readonly availableWorkerSeats: readonly Seat[];
};

/**
 * Mutable builder for an {@link EvidenceLedger}. `build()` snapshots the
 * accumulated rows into frozen readonly arrays; the returned ledger is
 * read-only to the oracle.
 */
export class EvidenceLedgerBuilder {
  private readonly receipts: TransitionReceipt[] = [];
  private readonly decompositions: DecompositionFact[] = [];
  private readonly routes: RouteFact[] = [];
  private readonly claims: ClaimAttemptFact[] = [];
  private readonly executions: ExecutionFact[] = [];
  private readonly threadStatuses: ThreadStatusFact[] = [];
  private readonly wakes: WakeProvenanceFact[] = [];
  private readonly bodyDurability: BodyDurabilityFact[] = [];
  private readonly deliveries: DeliveryObservationFact[] = [];
  private readonly steers: SteerFact[] = [];
  private readonly restarts: RestartFact[] = [];
  private readonly loserActivity: LoserActivityFact[] = [];
  private readonly availableWorkerSeats: Seat[] = [];

  addReceipt(receipt: TransitionReceipt): this {
    this.receipts.push(receipt);
    return this;
  }

  addDecomposition(fact: DecompositionFact): this {
    this.decompositions.push(fact);
    return this;
  }

  addRoute(fact: RouteFact): this {
    this.routes.push(fact);
    return this;
  }

  addClaim(fact: ClaimAttemptFact): this {
    this.claims.push(fact);
    return this;
  }

  addExecution(fact: ExecutionFact): this {
    this.executions.push(fact);
    return this;
  }

  addThreadStatus(fact: ThreadStatusFact): this {
    this.threadStatuses.push(fact);
    return this;
  }

  addWake(fact: WakeProvenanceFact): this {
    this.wakes.push(fact);
    return this;
  }

  addBodyDurability(fact: BodyDurabilityFact): this {
    // Fail-closed ingestion seam: the public builder cannot retain a raw,
    // body-bearing, or grammar-invalid durability row. Parse + store sanitized.
    this.bodyDurability.push(parseBodyDurability(fact));
    return this;
  }

  addDelivery(fact: DeliveryObservationFact): this {
    // Fail-closed ingestion seam: parse + store the sanitized delivery so a
    // body-bearing / unknown-field / grammar-invalid row throws before storage.
    this.deliveries.push(parseDeliveryObservation(fact));
    return this;
  }

  addSteer(fact: SteerFact): this {
    this.steers.push(fact);
    return this;
  }

  addRestart(fact: RestartFact): this {
    this.restarts.push(fact);
    return this;
  }

  addLoserActivity(fact: LoserActivityFact): this {
    this.loserActivity.push(fact);
    return this;
  }

  addAvailableWorkerSeat(seat: Seat): this {
    this.availableWorkerSeats.push(seat);
    return this;
  }

  build(): EvidenceLedger {
    return {
      receipts: Object.freeze([...this.receipts]),
      decompositions: Object.freeze([...this.decompositions]),
      routes: Object.freeze([...this.routes]),
      claims: Object.freeze([...this.claims]),
      executions: Object.freeze([...this.executions]),
      threadStatuses: Object.freeze([...this.threadStatuses]),
      wakes: Object.freeze([...this.wakes]),
      bodyDurability: Object.freeze([...this.bodyDurability]),
      deliveries: Object.freeze([...this.deliveries]),
      steers: Object.freeze([...this.steers]),
      restarts: Object.freeze([...this.restarts]),
      loserActivity: Object.freeze([...this.loserActivity]),
      availableWorkerSeats: Object.freeze([...this.availableWorkerSeats]),
    };
  }
}

// ---------------------------------------------------------------------------
// Runtime fail-closed ingestion for the body-bearing facts.
//
// TypeScript shapes are erased at runtime, so a raw row that smuggles a body
// field (or any unknown field) would type-check yet reach the oracle. The
// durability/NOTICE/body-query facts are the only ones that touch message
// bodies, so their ingestion is guarded here: exactly the allowed keys, no
// extras, and NEVER any body-bearing field. Anything else throws
// `LedgerIngestError` — the ledger never holds a body.
// ---------------------------------------------------------------------------

/** Thrown when a raw body-bearing row carries an unknown, extra, body-bearing,
 * or malformed field. Fail-closed: ingestion rejects rather than sanitizes. */
export class LedgerIngestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LedgerIngestError";
  }
}

// Field names that would carry (or imply) a message body. A NOTICE is metadata
// only and a body query references an id, never inlines content, so ANY of
// these on a delivery/durability row is a fail-closed rejection even though no
// typed variant declares them.
const BODY_BEARING_KEYS: ReadonlySet<string> = new Set([
  "body",
  "bodyText",
  "bodyContent",
  "bodyString",
  "content",
  "payload",
  "rawBody",
  "messageBody",
  "text",
  "html",
]);

function asRecord(raw: unknown, what: string): Record<string, unknown> {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new LedgerIngestError(`${what} must be a plain object`);
  }
  return raw as Record<string, unknown>;
}

function assertExactKeys(
  obj: Record<string, unknown>,
  allowed: readonly string[],
  what: string,
): void {
  const allowedSet = new Set(allowed);
  // Reflect.ownKeys (not Object.keys) so a NON-ENUMERABLE own property or a
  // symbol key — e.g. a body smuggled via Object.defineProperty — is also seen
  // and rejected. JSON objects only ever carry enumerable string keys, so
  // grammar-valid positives are unaffected.
  for (const key of Reflect.ownKeys(obj)) {
    if (typeof key !== "string") {
      throw new LedgerIngestError(
        `${what} has a non-string (symbol) key`,
      );
    }
    if (BODY_BEARING_KEYS.has(key)) {
      throw new LedgerIngestError(
        `${what} carries body-bearing field "${key}" (the ledger never holds a body)`,
      );
    }
    if (!allowedSet.has(key)) {
      throw new LedgerIngestError(`${what} has unknown field "${key}"`);
    }
  }
  for (const key of allowed) {
    // Object.hasOwn (not `in`) so a required field satisfied only via the
    // prototype chain does not count as present.
    if (!Object.hasOwn(obj, key)) {
      throw new LedgerIngestError(`${what} is missing required field "${key}"`);
    }
  }
}

function reqString(obj: Record<string, unknown>, key: string, what: string): string {
  const v = obj[key];
  if (typeof v !== "string" || v.length === 0) {
    throw new LedgerIngestError(`${what} field "${key}" must be a non-empty string`);
  }
  return v;
}

function reqOrdinal(obj: Record<string, unknown>, what: string): number {
  const v = obj["ordinal"];
  if (typeof v !== "number" || !Number.isInteger(v) || v < 0) {
    throw new LedgerIngestError(`${what} ordinal must be a non-negative integer`);
  }
  return v;
}

const SHA256_HEX = /^[0-9a-f]{64}$/u;

// Branded-id grammar: `<prefix>_<26 lowercase Crockford base32>` — the exact
// shape the protocol wire parser enforces and the id minters produce. Every id
// field is validated by its prefix so a wrong-prefix or malformed id fails
// ingestion rather than being cast into a branded type it can never satisfy.
const ID_BODY = "[0-9a-hjkmnp-tv-z]{26}";
function idPattern(prefix: string): RegExp {
  return new RegExp(`^${prefix}_${ID_BODY}$`, "u");
}

// A supported delivery target: a channel (`#channel`) or channel-thread
// (`#channel:thread`) ref. Not arbitrary text.
const CHANNEL_TARGET = /^#[^\s#:]+(?::[^\s#:]+)?$/u;

// Single source of truth for the identifier grammar, exported so the oracle's
// defensive re-check reuses THIS validator rather than a duplicated regex that
// could drift from ingestion.
export function isValidBrandedId(value: unknown, prefix: string): boolean {
  return typeof value === "string" && idPattern(prefix).test(value);
}
export function isChannelTarget(value: unknown): boolean {
  return typeof value === "string" && CHANNEL_TARGET.test(value);
}

function reqId(
  obj: Record<string, unknown>,
  key: string,
  prefix: string,
  what: string,
): string {
  const v = obj[key];
  if (!isValidBrandedId(v, prefix)) {
    throw new LedgerIngestError(
      `${what} field "${key}" must be a grammar-valid ${prefix}_ id`,
    );
  }
  return v as string;
}

function reqChannelTarget(
  obj: Record<string, unknown>,
  key: string,
  what: string,
): string {
  const v = obj[key];
  if (!isChannelTarget(v)) {
    throw new LedgerIngestError(
      `${what} field "${key}" must be a channel target (#channel or #channel:thread)`,
    );
  }
  return v as string;
}

/** Parse one raw body-durability row, fail-closed. */
export function parseBodyDurability(raw: unknown): BodyDurabilityFact {
  const what = "body durability fact";
  const obj = asRecord(raw, what);
  assertExactKeys(
    obj,
    ["parentTaskId", "bodyDigest", "bodyBytes", "receiptId", "ordinal"],
    what,
  );
  const bodyDigest = reqString(obj, "bodyDigest", what);
  if (!SHA256_HEX.test(bodyDigest)) {
    throw new LedgerIngestError(
      `${what} bodyDigest must be a lowercase-hex SHA-256 digest`,
    );
  }
  const bodyBytes = obj["bodyBytes"];
  if (typeof bodyBytes !== "number" || !Number.isInteger(bodyBytes) || bodyBytes <= 0) {
    throw new LedgerIngestError(`${what} bodyBytes must be a positive integer`);
  }
  return {
    parentTaskId: reqId(obj, "parentTaskId", "tsk", what) as TaskId,
    bodyDigest,
    bodyBytes,
    receiptId: reqId(obj, "receiptId", "rcp", what) as ReceiptId,
    ordinal: reqOrdinal(obj, what),
  };
}

/** Parse one raw delivery observation row (NOTICE or body query), fail-closed. */
export function parseDeliveryObservation(raw: unknown): DeliveryObservationFact {
  const obj = asRecord(raw, "delivery observation");
  const kind = obj["kind"];
  if (kind === "notice_metadata") {
    const what = "notice delivery";
    assertExactKeys(
      obj,
      [
        "kind",
        "seat",
        "deliveryId",
        "subtaskId",
        "bodyPresent",
        "target",
        "count",
        "firstMessageId",
        "latestMessageId",
        "ordinal",
      ],
      what,
    );
    if (obj["bodyPresent"] !== false) {
      throw new LedgerIngestError(`${what} must assert bodyPresent:false`);
    }
    const count = obj["count"];
    if (typeof count !== "number" || !Number.isInteger(count) || count < 0) {
      throw new LedgerIngestError(`${what} count must be a non-negative integer`);
    }
    return {
      kind: "notice_metadata",
      seat: reqString(obj, "seat", what),
      deliveryId: reqId(obj, "deliveryId", "dlv", what) as DeliveryId,
      subtaskId: reqId(obj, "subtaskId", "tsk", what) as TaskId,
      bodyPresent: false,
      target: reqChannelTarget(obj, "target", what),
      count,
      firstMessageId: reqId(obj, "firstMessageId", "msg", what) as MessageId,
      latestMessageId: reqId(obj, "latestMessageId", "msg", what) as MessageId,
      ordinal: reqOrdinal(obj, what),
    };
  }
  if (kind === "body_read") {
    const what = "body query delivery";
    assertExactKeys(
      obj,
      [
        "kind",
        "seat",
        "deliveryId",
        "subtaskId",
        "explicitQuery",
        "queryTarget",
        "queriedMessageId",
        "queryTurnId",
        "ordinal",
      ],
      what,
    );
    if (obj["explicitQuery"] !== true) {
      throw new LedgerIngestError(`${what} must assert explicitQuery:true`);
    }
    return {
      kind: "body_read",
      seat: reqString(obj, "seat", what),
      deliveryId: reqId(obj, "deliveryId", "dlv", what) as DeliveryId,
      subtaskId: reqId(obj, "subtaskId", "tsk", what) as TaskId,
      explicitQuery: true,
      queryTarget: reqChannelTarget(obj, "queryTarget", what),
      queriedMessageId: reqId(obj, "queriedMessageId", "msg", what) as MessageId,
      queryTurnId: reqId(obj, "queryTurnId", "trn", what) as TurnId,
      ordinal: reqOrdinal(obj, what),
    };
  }
  throw new LedgerIngestError(
    `delivery observation has unknown kind "${String(kind)}"`,
  );
}

/**
 * Defense-in-depth revalidation for a plain-object ledger that did NOT come
 * through the builder (e.g. an object literal passed straight to the runner).
 * Re-runs the fail-closed parser over every body-bearing fact so a malformed,
 * body-bearing, or grammar-invalid row throws `LedgerIngestError` before any
 * condition verdict is produced. On a builder-produced ledger this is a no-op
 * (the rows are already sanitized), so it is idempotent and cheap.
 */
export function revalidateLedgerIngestion(ledger: EvidenceLedger): void {
  for (const fact of ledger.bodyDurability) parseBodyDurability(fact);
  for (const fact of ledger.deliveries) parseDeliveryObservation(fact);
}
