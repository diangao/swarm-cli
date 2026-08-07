/**
 * Security audit fact: the product-neutral durable record emitted when startup
 * reconciliation reaps an orphaned launch transport. This is NOT a protocol
 * TransitionReceipt — reconciliation has no legitimate turn/session fence, and
 * the frozen Protocol V0 vocabulary contains no cleanup/audit receipt kind.
 *
 * This package owns only the CONTRACT: the field schema, the idempotency key,
 * and the validator. The durable store and persistence are owned by the
 * daemon/storage lane. Re-running reconciliation over the same orphan must
 * yield one logical audit fact (same key), never duplicates.
 */

export type SecurityAuditKind = "transport_reaped";

export type SecurityAuditFact = {
  readonly auditKind: SecurityAuditKind;
  readonly launchId: string;
  readonly machineId: string;
  /** ISO-8601 timestamp; supplied by the daemon, not synthesized here. */
  readonly occurredAt: string;
  readonly idempotencyKey: string;
};

const AUDIT_KINDS: ReadonlySet<string> = new Set<SecurityAuditKind>([
  "transport_reaped",
]);

const ISO_8601 =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/u;

/**
 * The canonical idempotency key for an audit fact. Deterministic in its three
 * inputs, so a repeated reconciliation collapses to one logical fact.
 */
export function auditIdempotencyKey(
  machineId: string,
  launchId: string,
  auditKind: SecurityAuditKind,
): string {
  const part = (value: string): string => `${value.length}:${value}`;
  return `${part(machineId)}${part(launchId)}${part(auditKind)}`;
}

export type AuditValidation =
  | { readonly ok: true; readonly fact: SecurityAuditFact }
  | { readonly ok: false; readonly reason: string };

/**
 * Validate an audit fact fail-closed: unknown kind, missing/empty fields,
 * malformed timestamp, or an idempotency key that does not match its own
 * declared fields all reject rather than persist.
 */
export function validateAuditFact(candidate: unknown): AuditValidation {
  if (typeof candidate !== "object" || candidate === null) {
    return { ok: false, reason: "not-an-object" };
  }
  const record = candidate as Record<string, unknown>;
  const auditKind = record["auditKind"];
  const launchId = record["launchId"];
  const machineId = record["machineId"];
  const occurredAt = record["occurredAt"];
  const idempotencyKey = record["idempotencyKey"];

  if (typeof auditKind !== "string" || !AUDIT_KINDS.has(auditKind)) {
    return { ok: false, reason: "unknown-audit-kind" };
  }
  if (typeof launchId !== "string" || launchId.length === 0) {
    return { ok: false, reason: "missing-launch-id" };
  }
  if (typeof machineId !== "string" || machineId.length === 0) {
    return { ok: false, reason: "missing-machine-id" };
  }
  if (typeof occurredAt !== "string" || !ISO_8601.test(occurredAt)) {
    return { ok: false, reason: "malformed-occurred-at" };
  }
  if (typeof idempotencyKey !== "string") {
    return { ok: false, reason: "missing-idempotency-key" };
  }
  const expectedKey = auditIdempotencyKey(
    machineId,
    launchId,
    auditKind as SecurityAuditKind,
  );
  if (idempotencyKey !== expectedKey) {
    return { ok: false, reason: "idempotency-key-mismatch" };
  }
  return {
    ok: true,
    fact: {
      auditKind: auditKind as SecurityAuditKind,
      launchId,
      machineId,
      occurredAt,
      idempotencyKey,
    },
  };
}

/**
 * Collapse a batch of audit facts to one per idempotency key, modelling the
 * durable store's dedupe. A duplicate never produces a second logical fact.
 */
export function dedupeAuditFacts(
  facts: readonly SecurityAuditFact[],
): readonly SecurityAuditFact[] {
  const byKey = new Map<string, SecurityAuditFact>();
  for (const fact of facts) {
    if (!byKey.has(fact.idempotencyKey)) byKey.set(fact.idempotencyKey, fact);
  }
  return [...byKey.values()];
}
