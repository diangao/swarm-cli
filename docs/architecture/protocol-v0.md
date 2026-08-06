# Protocol V0

`@swarm/protocol` is the product-neutral wire-contract package for runtime V0. It owns validation, canonical JSON, version negotiation, opaque identifiers, causal request identity, and narrowly named transition receipts. It does not own storage, transport, scheduling, process supervision, task policy, review independence, or exactly-once external effects.

## Compatibility contract

- Current wire version is `1`, encoded as major `0`, minor `1`.
- Peers exchange one strict `{ major, minMinor, maxMinor }` range, require the same major, and select the highest overlapping minor.
- A versioned-frame parser requires the negotiated version and the frame must match it exactly. Compatibility comes from negotiation and encoding the selected older schema, never from ignoring fields.
- Every schema and nested union branch rejects unknown fields, `null`, coercion, aliases, unsafe integers, and noncanonical identifiers or timestamps.

## Identity and receipts

Opaque branded identifiers carry a fixed prefix plus 26 Crockford Base32 characters. `commandId` is the launch request identity: replaying the same canonical command is idempotent, while reusing an ID with another canonical payload is a conflict. A `TaskLease` carries its complete mutation fence; no member is inferred.

Receipts prove only the boundary named by their kind. In particular, `input_written` is weaker than `model_visible`, and artifact or review receipts bind an exact SHA-256 digest. Schema validity does not prove current authority, durable state, or reviewer independence.

## Parser and canonical form

The parser is duplicate-aware before object construction. It rejects payloads above 65,536 bytes, invalid UTF-8, BOMs, trailing data, duplicate keys at any depth, nesting beyond 16 levels, and non-object roots. Canonical serialization emits UTF-8 JSON with sorted object keys, no insignificant whitespace, canonical strings, and plain safe integers.

The committed fixture registry covers target variants, delivery replay, lease renewal, launch wake/no-wake, every receipt kind, both side-effect fence shapes, negotiation downgrade, malformed inputs, and eight seeded implementation defects. `scripts/check-package-boundaries.mjs` separately proves that the protocol package has no dependency on persistence, process, filesystem, runtime-driver, or application layers.

## Toolchain and gates

The workspace freezes Node.js `24.19.0`, pnpm `10.15.1`, and the integrity-qualified `packageManager` declaration in the root manifest. CI runs the frozen install, typecheck, tests, positive boundary scan, and seeded-negative boundary proof inside the existing required `regression` job before the unchanged serialized Python regression suite.
