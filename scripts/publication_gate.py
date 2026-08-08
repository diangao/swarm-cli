#!/usr/bin/env python3
"""Fail closed on repository content that is unsafe to publish."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DENYLIST = ROOT / ".github" / "publication-denylist.sha256"
WORD_RE = re.compile(r"[a-z0-9]+")
HEX_RE = re.compile(r"^[0-9a-f]{64}$")
OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")

PRODUCT_IDENTITY_NAMES = frozenset({"dian g", "dian gao"})
PRODUCT_IDENTITY_EMAIL = "123671200+diangao@users.noreply.github.com"
GITHUB_COMMITTER_IDENTITY = ("GitHub", "noreply@github.com")

SECRET_PATTERNS = (
    re.compile(r"gh[pousr]_[A-Za-z0-9]{30,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{40,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{20,}"),
    re.compile(r"xapp-[A-Za-z0-9-]{20,}"),
    re.compile(r"sk-(?:proj|live|test)-[A-Za-z0-9_-]{20,}"),
    re.compile(r"sk-ant-[A-Za-z0-9_-]{10,}"),
    re.compile(r"sk_(?:agent|machine)_[A-Za-z0-9]{10,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
)

# Fail closed on structured fields that attempt to publish private source
# material instead of the reviewed product contract or its public digest.
# The fragments are intentionally assembled so the scanner source does not
# contain a self-triggering field assignment.
RESTRICTED_PROVENANCE_FIELD = re.compile(
    r"[\"']?(?:"
    + "hidden" + r"[_-]?" + "prompt"
    + r"|"
    + "canonical" + r"[_-]?" + "contract" + r"[_-]?" + "prose"
    + r"|"
    + "evidence" + r"[_-]?" + "only" + r"[_-]?" + "copy"
    + r")[\"']?\s*[:=]",
    re.IGNORECASE,
)

BLOCKED_SUFFIXES = (
    ".db",
    ".log",
    ".pem",
    ".p12",
    ".sqlite",
    ".sqlite3",
)


@dataclass(frozen=True, order=True)
class Finding:
    kind: str
    source_id: str


@dataclass(frozen=True)
class CommitIdentity:
    object_id: str
    author_name: str
    author_email: str
    committer_name: str
    committer_email: str


@dataclass(frozen=True)
class TagIdentity:
    object_id: str
    tagger_name: str
    tagger_email: str


def run_git(*args: str) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"git command failed: {message}")
    return result.stdout


def source_id(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8", "replace")).hexdigest()[:12]


def decode_identity_lines(data: bytes, label: str) -> list[list[str]]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{label} identity output is not valid UTF-8") from exc

    records: list[list[str]] = []
    for line in text.splitlines():
        if not line:
            continue
        records.append(line.split("\x1f"))
    return records


def parse_commit_identities(data: bytes) -> list[CommitIdentity]:
    identities: list[CommitIdentity] = []
    for fields in decode_identity_lines(data, "commit"):
        if len(fields) != 5 or any(not field for field in fields):
            raise RuntimeError("commit identity output is malformed or incomplete")
        object_id, author_name, author_email, committer_name, committer_email = fields
        if not OBJECT_ID_RE.fullmatch(object_id):
            raise RuntimeError("commit identity output contains an invalid object id")
        identities.append(
            CommitIdentity(
                object_id,
                author_name,
                author_email,
                committer_name,
                committer_email,
            )
        )
    return identities


def parse_tag_identities(data: bytes) -> list[TagIdentity]:
    identities: list[TagIdentity] = []
    for fields in decode_identity_lines(data, "tag"):
        if len(fields) != 4:
            raise RuntimeError("tag identity output is malformed")
        object_type, object_id, tagger_name, tagger_email = fields
        if not OBJECT_ID_RE.fullmatch(object_id):
            raise RuntimeError("tag identity output contains an invalid object id")
        if object_type != "tag":
            if tagger_name or tagger_email:
                raise RuntimeError("lightweight tag unexpectedly contains tagger identity")
            continue
        if not tagger_name or not tagger_email:
            raise RuntimeError("annotated tag identity is incomplete")
        identities.append(TagIdentity(object_id, tagger_name, tagger_email))
    return identities


def is_product_identity(name: str, email: str) -> bool:
    return name in PRODUCT_IDENTITY_NAMES and email == PRODUCT_IDENTITY_EMAIL


def scan_commit_identity(identity: CommitIdentity) -> set[Finding]:
    findings: set[Finding] = set()
    if not is_product_identity(identity.author_name, identity.author_email):
        findings.add(Finding("disallowed-commit-identity", source_id(f"{identity.object_id}:author")))
    committer = (identity.committer_name, identity.committer_email)
    if not is_product_identity(*committer) and committer != GITHUB_COMMITTER_IDENTITY:
        findings.add(Finding("disallowed-commit-identity", source_id(f"{identity.object_id}:committer")))
    return findings


def scan_tag_identity(identity: TagIdentity) -> set[Finding]:
    if is_product_identity(identity.tagger_name, identity.tagger_email):
        return set()
    return {Finding("disallowed-tag-identity", source_id(f"{identity.object_id}:tagger"))}


def scan_authorship() -> tuple[set[Finding], int]:
    commit_data = run_git(
        "log",
        "--all",
        "--format=%H%x1f%an%x1f%ae%x1f%cn%x1f%ce",
    )
    tag_data = run_git(
        "for-each-ref",
        "refs/tags",
        "--format=%(objecttype)%1f%(objectname)%1f%(taggername)%1f%(taggeremail:trim)",
    )

    findings: set[Finding] = set()
    count = 0
    for identity in parse_commit_identities(commit_data):
        count += 2
        findings.update(scan_commit_identity(identity))
    for identity in parse_tag_identities(tag_data):
        count += 1
        findings.update(scan_tag_identity(identity))
    return findings, count


def load_denylist(path: Path) -> set[str]:
    if not path.is_file():
        raise RuntimeError(f"denylist is missing: {path}")
    values = {
        line.strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    invalid = sorted(value for value in values if not HEX_RE.fullmatch(value))
    if invalid or not values:
        raise RuntimeError("denylist must contain one or more SHA-256 digests")
    return values


def ngram_digests(text: str, max_words: int = 4) -> Iterable[str]:
    words = WORD_RE.findall(text.casefold())
    for size in range(1, min(max_words, len(words)) + 1):
        for start in range(0, len(words) - size + 1):
            value = " ".join(words[start : start + size])
            yield hashlib.sha256(value.encode("utf-8")).hexdigest()


def has_local_home_path(text: str) -> bool:
    unix_patterns = (
        re.compile("/" + r"Users/[^/\s]+/"),
        re.compile("/" + r"home/[^/\s]+/"),
    )
    windows_pattern = re.compile("C:" + r"\\Users\\[^\\\s]+\\", re.IGNORECASE)
    return any(pattern.search(text) for pattern in unix_patterns) or bool(windows_pattern.search(text))


def scan_text(label: str, text: str, denylist: set[str]) -> set[Finding]:
    findings: set[Finding] = set()
    marker = source_id(label)
    if any(digest in denylist for digest in ngram_digests(text)):
        findings.add(Finding("private-provenance-marker", marker))
    if any(pattern.search(text) for pattern in SECRET_PATTERNS):
        findings.add(Finding("credential-shape", marker))
    if has_local_home_path(text):
        findings.add(Finding("machine-local-path", marker))
    if RESTRICTED_PROVENANCE_FIELD.search(text):
        findings.add(Finding("restricted-provenance-field", marker))
    return findings


def path_is_blocked(path: str) -> bool:
    normalized = path.replace("\\", "/").lower()
    name = normalized.rsplit("/", 1)[-1]
    if name == ".env" or (name.startswith(".env.") and name != ".env.example"):
        return True
    if any(name.endswith(suffix) or f"{suffix}-" in name for suffix in BLOCKED_SUFFIXES):
        return True
    return any(component in {"logs", ".private"} for component in normalized.split("/")) or normalized.startswith(
        "artifacts/private/"
    )


def decode_text(data: bytes) -> str | None:
    if b"\x00" in data:
        return None
    return data.decode("utf-8", "replace")


def scan_blob(label: str, path: str, data: bytes, denylist: set[str]) -> set[Finding]:
    findings: set[Finding] = set()
    marker = source_id(label)
    if path_is_blocked(path):
        findings.add(Finding("blocked-tracked-path", marker))
    text = decode_text(data)
    if text is None:
        findings.add(Finding("unapproved-binary", marker))
        return findings
    findings.update(scan_text(label, text, denylist))
    return findings


def nul_paths(data: bytes) -> list[str]:
    return [item.decode("utf-8", "surrogateescape") for item in data.split(b"\x00") if item]


def worktree_paths() -> list[str]:
    return sorted(
        set(nul_paths(run_git("ls-files", "-z")))
        | set(nul_paths(run_git("ls-files", "--others", "--exclude-standard", "-z")))
    )


def staged_paths() -> list[str]:
    return nul_paths(run_git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z"))


def scan_worktree(denylist: set[str]) -> tuple[set[Finding], int]:
    findings: set[Finding] = set()
    count = 0
    for path in worktree_paths():
        full_path = ROOT / path
        if not full_path.is_file():
            continue
        count += 1
        findings.update(scan_blob(f"worktree:{path}", path, full_path.read_bytes(), denylist))
    return findings, count


def scan_staged(denylist: set[str]) -> tuple[set[Finding], int]:
    findings: set[Finding] = set()
    count = 0
    for path in staged_paths():
        count += 1
        data = run_git("show", f":{path}")
        findings.update(scan_blob(f"staged:{path}", path, data, denylist))
    return findings, count


def scan_history(denylist: set[str]) -> tuple[set[Finding], int]:
    findings: set[Finding] = set()
    surfaces = (
        ("history", run_git("log", "--all", "--format=%H%n%B%x00")),
        ("refs", run_git("for-each-ref", "--format=%(refname)")),
    )
    count = 0
    for label, data in surfaces:
        count += 1
        findings.update(scan_text(label, data.decode("utf-8", "replace"), denylist))

    identity_findings, identity_count = scan_authorship()
    findings.update(identity_findings)
    count += identity_count

    historical_entries: set[tuple[str, str]] = set()
    for commit in run_git("rev-list", "--all").splitlines():
        tree = run_git("ls-tree", "-r", "-z", commit.decode("ascii"))
        for record in tree.split(b"\x00"):
            if not record:
                continue
            metadata, raw_path = record.split(b"\t", 1)
            _mode, object_type, raw_oid = metadata.split(b" ", 2)
            if object_type != b"blob":
                continue
            historical_entries.add(
                (raw_oid.decode("ascii"), raw_path.decode("utf-8", "surrogateescape"))
            )

    blob_cache: dict[str, bytes] = {}
    for oid, path in sorted(historical_entries):
        if oid not in blob_cache:
            blob_cache[oid] = run_git("cat-file", "blob", oid)
        data = blob_cache[oid]
        count += 1
        findings.update(scan_blob(f"history-blob:{oid}:{path}", path, data, denylist))

    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if event_path:
        event = Path(event_path)
        if not event.is_file():
            raise RuntimeError("GITHUB_EVENT_PATH does not point to a file")
        count += 1
        findings.update(scan_text("github-event", event.read_text("utf-8"), denylist))
    return findings, count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--all", action="store_true", help="scan the worktree and history")
    mode.add_argument("--staged", action="store_true", help="scan staged additions and modifications")
    parser.add_argument("--denylist", type=Path, default=DEFAULT_DENYLIST)
    args = parser.parse_args()

    try:
        denylist = load_denylist(args.denylist)
        if args.staged:
            findings, count = scan_staged(denylist)
        else:
            findings, count = scan_worktree(denylist)
            history_findings, history_count = scan_history(denylist)
            findings.update(history_findings)
            count += history_count
    except (OSError, RuntimeError, UnicodeError) as exc:
        print(f"publication gate error: {exc}", file=sys.stderr)
        return 2

    if findings:
        for finding in sorted(findings):
            print(f"BLOCK {finding.kind} source_id={finding.source_id}", file=sys.stderr)
        print(f"publication gate blocked {len(findings)} finding(s)", file=sys.stderr)
        return 1

    print(f"publication gate passed: {count} surface(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
