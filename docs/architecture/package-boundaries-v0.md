# Package boundaries V0

The runtime packages form an inward-only dependency graph. The repository gate
discovers every `packages/*/src` tree and rejects package names that do not have
an explicit policy entry.

| Package | Allowed workspace imports |
| --- | --- |
| `protocol` | none |
| `storage` | `protocol` |
| `security` | `protocol` |
| `testkit` | `protocol`, `security` |
| `verifiers` | `protocol`, `security`, `testkit` |

An import of an allowed workspace package must also be declared in the
importing package's runtime dependency metadata. Imports of external packages
must be declared too. Relative imports may not escape the owning package.

The protocol and testkit layers additionally reject high-authority Node.js
built-ins such as filesystem, process, networking, subprocess, and worker
modules. The verifier layer rejects subprocess, cluster, networking, and
worker modules. Storage and security have explicit infrastructure duties, so
their required built-ins are reviewed at the package level rather than denied
as a class.

The positive fixture proves every allowed edge. Seeded negative fixtures prove
that the gate catches upward or lateral workspace imports, undeclared
dependencies, high-authority imports in restricted layers, unknown packages,
absolute imports, and relative cross-package escapes.

Adding a package or dependency edge is a versioned architecture change. It
requires updating this policy and its positive and seeded-negative controls in
the same independently reviewed change.
