# Contributing

## Repository boundaries

This repository contains product code and public product documentation only.
Do not add private conversations, machine-local paths, credentials, internal
coordination receipts, or unpublished third-party material.

## Branches and worktrees

- `main` is protected and receives pull requests only.
- `integration/runtime-v0` is the reviewed integration branch for the first
  distributed runtime milestone.
- Use one worktree and one branch per independent lane.
- Use `agent/*` for automated repository-maintenance patches and
  `feat/*`, `fix/*`, `test/*`, or `docs/*` for product work.
- Never mix unrelated lanes in one pull request.

## Local gates

Run the publication gate before committing:

```bash
python3 scripts/publication_gate_test.py
python3 scripts/publication_gate.py --all
```

To install the staged-file hook with `pre-commit`:

```bash
pre-commit install
```

Run the relevant product probes before pushing. The hosted regression job runs
the complete suite serially because some probes intentionally use overlapping
local resources.

## Review and integration

Open draft pull requests first. Freeze the head SHA before independent review
and record that SHA in the PR description. Before merge, record a reviewed
potential-merge prediction covering the unchanged base, exact parents, tree,
identities, and message template. If the base moves, regenerate that artifact
and repeat the review.

Execute the reviewed merge method with the exact reviewed head SHA as a
compare-and-swap condition. Do not use auto-merge or a merge queue during this
attestation window; either could change the base, head, or method after review.

GitHub assigns the final merge SHA only when it updates the protected branch.
Treat the interval until post-merge attestation as a hard downstream fence: do
not create or advance integration branches, tags, release clones, or new main
commits. Wait for the actual SHA's push-to-main hosted gates to become terminal
green. A reviewer who did not execute the merge must then rerun the gates on
the actual artifact and compare it with the prediction.

Resolve a content or behavior mismatch that is safe to retain in public
history with a reviewed revert pull request. A personal-data, credential, or
private-provenance leak in public metadata or history is a publication/security
incident, not a revert-only case: freeze downstream and ref publication and
obtain explicit repository-owner authorization for history remediation or
repository replacement, followed by a fresh-clone full-history review.

Commits and public metadata must use the repository owner's product identity.
Do not add automation-agent names, chat handles, or internal receipt IDs to
authors, trailers, branch names, commit messages, issues, pull requests, or
release notes.
