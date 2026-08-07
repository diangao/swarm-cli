# Launch-environment policy (V0)

Normative contract for how the daemon prepares the environment of a launched
agent child. The Wave 2 driver lane consumes it; the `@swarm/security` package
provides the pure validators (`validateChildEnv`, `validatePosixTransportMode`,
`validateWindowsTransportDacl`, `validateLaunchCredential`, `validateDistinctHomes`)
that decide PASS/FAIL over plain data. Enforcement — spawning, `chmod`, wiring
the capability — belongs to the daemon; this package owns only the contract.

## Threat model and honest guarantee boundary

V0 assumes the daemon and every child it launches run under the **same OS uid**,
with no per-launch uid, sandbox, or container. Under that assumption a same-uid
process can reach another child's resources through `/proc`, a debugger, or file
descriptor duplication, and a compromised child can relay its own capability.
Therefore neither filesystem permissions nor a launch-bound capability is
enforceable confidentiality against a hostile or compromised same-uid sibling.

**V0 does not claim hostile same-uid sibling isolation.** V0 guarantees, and the
validators prove, three things:

1. **Accidental cross-launch leakage prevention** — a correctly-behaving child
   receives only its own launch's context; nothing from another launch appears
   in its environment, argv, or transport.
2. **Ambient-secret minimization** — no reusable long-lived bearer secret is
   placed in the child's environment, argv, or transport directory.
3. **Blast-radius reduction** — credentials are launch-scoped and revoked at
   launch close.

Hostile same-uid sibling isolation requires an OS-enforced binding (per-launch
uid, sandbox, or container). That is the **V1** hardening axis and is out of V0
scope. It is recorded as a known-unguarded gap, not silently implied as covered.

## Transport directory

- One directory per launch, path bound to the launch id; created before spawn,
  never reused.
- POSIX: directory `0700`, files `0600`. Windows: a restricted DACL granting
  only the daemon identity, no inherited ACEs. These bound **other-uid /
  other-identity** access only — per the threat model, not a same-uid isolation
  claim. POSIX mode and Windows DACL are two separate contracts, each with its
  own seeded negative.
- Contents are non-reusable references only: the launch-bound credential
  capability, the standing-prompt reference, thin CLI wrappers. No raw secret
  value and no reusable bearer token in the directory or argv.
- Cleanup: idempotent revoke and remove on close, stop, and crash. Startup
  reconciliation reaps an orphan and emits a security audit fact (below), not a
  protocol receipt.

## Child environment

Default-deny: the environment is built from an explicit allowlist.

- **Context allowlist**: agent identity context, server endpoint, machine id,
  workspace path, protocol version, transport-dir path, the launch-bound
  credential capability handle.
- **Platform baseline** (synthesized per launch, never inherited from the host
  process): `HOME` is a dedicated per-launch, untracked, private home directory,
  unique per launch and separate from the source workspace (which may be shared
  or git-tracked and must never serve as `HOME`); `XDG_STATE_HOME` and `TMPDIR`
  live under that private home; `PATH` is a fixed minimal value from reviewed
  daemon configuration, not a filtered host `PATH`; locale is fixed minimal.
- **Denylist** (defense-in-depth, name pattern only, never values): `*_API_KEY`,
  `*_TOKEN`, `*_SECRET`, `*_PASSWORD`, cloud-credential names, VCS token
  variables, `SSH_AUTH_SOCK`, agent-credential variables. A denylisted name that
  survives allowlisting is a build error, not a silent strip.
- Secrets travel only by the launch-bound capability, never as environment
  values and never via argv.

## Credential capability

The daemon hands the child a launch-bound capability — an inherited file
descriptor to an already-authenticated channel, or a local proxy endpoint scoped
to the launch id — carrying no long-lived bearer value and revoked at launch
close. This bounds the value and lifetime of credential material; per the threat
model it does not prevent a hostile same-uid sibling from duplicating a
descriptor or relaying a capability.

## Security audit fact (cleanup / reconciliation)

A product-neutral durable record, not a protocol receipt. Fields: `auditKind`
(e.g. `transport_reaped`), `launchId`, `machineId`, `occurredAt`, and
`idempotencyKey`. The idempotency key MUST be produced by the single canonical
`auditIdempotencyKey(machineId, launchId, auditKind)` function exported from
`@swarm/security`; implementations must not reconstruct it by concatenating the
fields. That function uses an injective length-prefixed encoding (`len:value`
per field) so distinct tuples can never collide onto one key — a naive
`machineId + launchId + auditKind` concatenation is not injective and is
forbidden. The daemon/storage lane owns the durable store and persistence;
`@swarm/security` owns only the schema, the canonical key function, and the
validator. Re-running reconciliation over one orphan yields a single logical
fact.

## Gate 0 (prove-it-can-fail)

Each invariant ships a seeded defect that must FAIL and a healthy control that
must PASS: secret value or name in env, non-allowlisted var, a baseline field
missing or diverging from the (independently validated) baseline, `HOME` that is
a host-home shape or under the source workspace (POSIX and Windows separators),
a malformed baseline, reusable bearer or a capability smuggling secret material,
secret material in a transport file, a write outside the private home, two
launches sharing a home, POSIX mode that is not exactly `0700`/`0600` (including
`0000`), a Windows DACL that is empty / inherited / deny / foreign / missing
required rights, secret via argv, an orphan transport surviving reconciliation,
a missing wrapper of the POSIX+PowerShell pair, colliding audit-key tuples, and
the S17 marker passing the promoted publication gate. The hostile same-uid
sibling gap is recorded as an explicit, testable known-unguarded fact
(`KNOWN_UNGUARDED_V0`), never a passing isolation claim.

Permission and env validators are fail-closed: they enforce the exact frozen
contract (all baseline fields present and equal, exact `0700`/`0600`, an
explicit daemon-only allow DACL with the required rights) rather than only
rejecting a few known-bad shapes.

## S17 mutual bind

`@swarm/security/s17` is the single source for the finding-kind to condition-id
correspondence and the runtime marker generator that the verifier lane imports.
The marker is generated at test runtime, never committed as a tracked fixture,
and carries no real identity or lineage term.
