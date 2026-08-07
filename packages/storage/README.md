# `@swarm/storage`

Storage has two intentionally separate authority surfaces:

- `SharedStore` persists canonical shared facts in PostgreSQL 16. Each callback
  transaction owns one persistent `psql` session from `BEGIN` through `COMMIT`;
  repository calls inside that callback therefore share one physical database
  connection. A session error, timeout, or premature exit rolls back on
  disconnect and is never retried under a fresh request key.
- `DaemonJournal` persists local recovery evidence in SQLite. Reopened rows are
  evidence for server/runtime reconciliation only. They do not restore claims,
  task status, delivery ownership, launch generations, or publication rights.

The package has no external database driver dependency. PostgreSQL uses the
existing `psql` executable via `node:child_process`; SQLite uses Node 24.19's
built-in `node:sqlite`. Gate 0 requires an explicit disposable PostgreSQL 16
`DATABASE_URL` and never substitutes an in-memory or alternate SQL dialect.

Command idempotency accepts only the exported, versioned
`IDEMPOTENCY_SCOPES`; callers cannot create ad hoc correctness namespaces.
Outbox namespaces come from the closed event-kind/version surface, and outbox
keys are derived from canonical producer-fact and payload bytes. Receipt
content digests intentionally exclude the replaceable wire `receiptId`, so a
fresh receipt identity for the same logical transition returns the committed
winner while changed content conflicts.

Production migrations are forward-only and checksum verified. Test reset is
guarded by both `NODE_ENV=test` and a `swarm_storage_test_*` database name for
PostgreSQL, or a `swarm-storage-test-*` journal filename for SQLite.
