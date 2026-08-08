#!/usr/bin/env python3
"""Unit checks for the repository publication gate."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import publication_gate


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def kinds(findings: set[publication_gate.Finding]) -> set[str]:
    return {finding.kind for finding in findings}


def main() -> int:
    marker = "blocked phrase"
    denylist = {hashlib.sha256(marker.encode()).hexdigest()}

    require(not publication_gate.scan_text("safe", "ordinary product prose", denylist), "safe text blocked")
    require(
        "private-provenance-marker" in kinds(publication_gate.scan_text("marker", marker, denylist)),
        "digest marker was not blocked",
    )

    local_path = "/" + "Users/" + "example/project"
    require(
        "machine-local-path" in kinds(publication_gate.scan_text("path", local_path, set())),
        "machine-local path was not blocked",
    )

    fake_tokens = {
        "GitHub": "ghp_" + ("A" * 36),
        "Anthropic": "sk" + "-ant-oat01-" + ("A" * 24),
        "agent": "sk" + "_agent_" + ("A" * 24),
        "machine": "sk" + "_machine_" + ("A" * 24),
    }
    for label, fake_token in fake_tokens.items():
        require(
            "credential-shape" in kinds(publication_gate.scan_text("secret", fake_token, set())),
            f"{label} credential-shaped value was not blocked",
        )

    for safe_prefix in ("sk-ant-short", "sk_agent_short", "sk_machine_short"):
        require(
            "credential-shape" not in kinds(publication_gate.scan_text("safe-prefix", safe_prefix, set())),
            f"short non-credential prefix was blocked: {safe_prefix}",
        )

    restricted_fields = (
        "_".join(("hidden", "prompt")),
        "_".join(("canonical", "contract", "prose")),
        "_".join(("evidence", "only", "copy")),
    )
    for field in restricted_fields:
        fixture = "{\"" + field + "\":\"seed\"}"
        require(
            "restricted-provenance-field" in kinds(publication_gate.scan_text("restricted", fixture, set())),
            f"restricted provenance field was not blocked: {field}",
        )
    require(
        "restricted-provenance-field" not in kinds(
            publication_gate.scan_text("reviewed", '{"contractDigest":"sha256:reviewed"}', set())
        ),
        "reviewed digest field was blocked",
    )

    require(publication_gate.path_is_blocked("state.sqlite3"), "database path was not blocked")
    require(publication_gate.path_is_blocked("state.sqlite3-wal"), "database sidecar was not blocked")
    require(publication_gate.path_is_blocked("logs/runtime.txt"), "log directory was not blocked")
    require(not publication_gate.path_is_blocked("docs/runtime.md"), "safe documentation path blocked")

    with tempfile.TemporaryDirectory() as directory:
        denylist_path = Path(directory) / "denylist.sha256"
        denylist_path.write_text(next(iter(denylist)) + "\n", encoding="utf-8")
        require(publication_gate.load_denylist(denylist_path) == denylist, "denylist did not round-trip")

    oid = "a" * 40
    product_email = publication_gate.PRODUCT_IDENTITY_EMAIL
    healthy = publication_gate.CommitIdentity(
        oid,
        "dian g",
        product_email,
        "GitHub",
        "noreply@github.com",
    )
    require(not publication_gate.scan_commit_identity(healthy), "healthy web-flow identity was blocked")

    product_committer = publication_gate.CommitIdentity(
        oid,
        "dian gao",
        product_email,
        "dian g",
        product_email,
    )
    require(not publication_gate.scan_commit_identity(product_committer), "product identity was blocked")

    identity_defects = (
        publication_gate.CommitIdentity(oid, "dian g", "owner@example.invalid", "GitHub", "noreply@github.com"),
        publication_gate.CommitIdentity(oid, "dian g", product_email, "builder", "builder@example.invalid"),
        publication_gate.CommitIdentity(oid, "Example Person", product_email, "GitHub", "noreply@github.com"),
        publication_gate.CommitIdentity(
            oid,
            "dian g",
            "999999999+other@users.noreply.github.com",
            "GitHub",
            "noreply@github.com",
        ),
    )
    for identity in identity_defects:
        require(
            "disallowed-commit-identity" in kinds(publication_gate.scan_commit_identity(identity)),
            "disallowed commit identity was not blocked",
        )

    safe_tag = publication_gate.TagIdentity(oid, "dian g", product_email)
    require(not publication_gate.scan_tag_identity(safe_tag), "healthy tag identity was blocked")
    unsafe_tag = publication_gate.TagIdentity(oid, "release bot", "release@example.invalid")
    require(
        "disallowed-tag-identity" in kinds(publication_gate.scan_tag_identity(unsafe_tag)),
        "disallowed tag identity was not blocked",
    )

    valid_record = publication_gate.parse_commit_identities(
        f"{oid}\x1fdian g\x1f{product_email}\x1fGitHub\x1fnoreply@github.com\n".encode()
    )
    require(valid_record == [healthy], "commit identity record did not parse")

    malformed_records = (
        f"{oid}\x1fdian g\x1f\x1fGitHub\x1fnoreply@github.com\n".encode(),
        b"not-an-object-id\x1fdian g\x1fmail\x1fdian g\x1fmail\n",
        b"\xff\xfe",
    )
    for record in malformed_records:
        try:
            publication_gate.parse_commit_identities(record)
        except RuntimeError:
            pass
        else:
            raise AssertionError("malformed identity extraction did not fail closed")

    print("publication gate tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
