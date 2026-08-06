# Repository policy

## Purpose

This repository is the product source of truth. Product code, public contracts,
tests, and independently created design documentation belong here. Private
research material and machine-local evidence do not.

## Authority and branch policy

- `main` is the protected release-history branch.
- `integration/runtime-v0` is the protected integration branch for the first
  distributed runtime milestone.
- Builders work in independent branches and worktrees.
- Every pull request identifies its exact head SHA and intended base.
- Required checks run on the exact proposed artifact.
- Before merge, independent review binds to the exact pull-request head and a
  fresh potential-merge artifact. The recorded prediction includes the exact
  base and head parents, tree hash, product-safe author and committer
  identities, and merge-message template.
- The protected base must still equal the reviewed base at merge time. Strict
  required checks enforce this mechanically; if the base moves, regenerate the
  potential-merge artifact and repeat review.
- The merge executor fixes the merge method and supplies the exact reviewed
  head SHA as a compare-and-swap condition. Auto-merge and merge-queue flows
  are not used for this attestation window because they may change the base,
  head, or method after review.
- GitHub assigns the final merge SHA only after it updates `main`. Immediately
  after that update, wait for the push-to-main hosted gates to become terminal
  green. An independent reviewer who did not execute the merge then compares
  the actual parents, tree, identities, and message with the recorded
  prediction and reruns the repository gates on the actual artifact.
- Until that post-merge attestation is green, no process may create or advance
  an `integration/*` branch, tag the commit, make a release clone, or add a new
  commit on top of `main`.
- Any mismatch is an incident and a hard hold. If the mismatch is limited to
  content or behavior that is safe to retain in public history, remediation
  uses a reviewed revert pull request through the same checks and review gates.
- If the actual artifact exposes personal data, a credential, or private
  provenance in public metadata or history, a revert is not sufficient because
  the object remains reachable. Freeze all downstream and ref publication, and
  require explicit repository-owner authorization for history remediation or
  repository replacement. Only a fresh-clone, full-history review can lift
  that publication/security hold.

## Publication boundary

The publication gate covers tracked paths and content, reachable commit
messages and refs, author/committer/tagger identities, and GitHub event
metadata. It rejects private provenance
markers through a one-way digest list, high-confidence credential shapes,
machine-local home paths, local databases, environment files, logs, and other
development-only output.

The public digest list is appropriate only for non-PII provenance markers. A
plain digest does not protect low-entropy or enumerable values from dictionary
recovery, so names, email addresses, phone numbers, account identifiers, and
other personal data must never be added to it. The current marker normalizer
matches lowercase ASCII word sequences; non-ASCII markers require a separate
private-side review until that normalizer is deliberately expanded.

Public prose should explain product behavior directly. It should not describe
private investigation methods, internal chat history, machine topology, or
where an idea was first observed.

## Ownership

CODEOWNERS routes every change to the repository owner. Independent review may
happen outside GitHub, but the review receipt must bind to the exact commit SHA
being promoted. GitHub checks remain mandatory even when a separate reviewer
has already approved the design.
