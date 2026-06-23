#!/usr/bin/env python3
"""Dynamic anti-stub probes for the minimal Swarm candidate CLI."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLI = ROOT / "swarm"
STATE_FILE = "state.json"


class ProbeFailure(AssertionError):
    pass


def fail(message: str) -> None:
    raise ProbeFailure(message)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def run(cli: Path, state_dir: Path, *args: str, stdin: str | None = None, expected: int = 0) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    proc = subprocess.run(
        [str(cli), *args],
        input=stdin,
        text=True,
        capture_output=True,
        env=env,
        timeout=20,
        check=False,
    )
    if proc.returncode != expected:
        fail(
            f"{' '.join(args)} exited {proc.returncode}, expected {expected}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def read_state(state_dir: Path) -> dict:
    path = state_dir / STATE_FILE
    require(path.exists(), f"state file not created: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_state(state_dir: Path, state: dict) -> None:
    (state_dir / STATE_FILE).write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def parse_message_id(output: str) -> str:
    match = re.search(r"Message ID: ([0-9a-f-]{36})", output)
    require(match is not None, f"missing sent message id in output:\n{output}")
    return match.group(1)


def probe_inbox(cli: Path, state_dir: Path) -> None:
    first = run(cli, state_dir, "message", "check").stdout
    require("please check the fixture" in first, "first check did not display seeded pending inbox")
    require("No more new messages." in first, "pending check missing drain footer")

    second = run(cli, state_dir, "message", "check").stdout
    require(second == "No new messages.\n", f"second check should be empty after drain, got:\n{second}")

    custom_body = f"anti-stub inbox {uuid.uuid4()}"
    state = read_state(state_dir)
    state.setdefault("local_inbox", []).append(
        {
            "target": "#probe-inbox",
            "message_id": str(uuid.uuid4()),
            "time": "2026-03-15T02:00:00",
            "type": "human",
            "author": "verifier",
            "body": custom_body,
        }
    )
    write_state(state_dir, state)

    custom = run(cli, state_dir, "message", "check").stdout
    require(custom_body in custom, "custom local inbox item was not emitted")
    require("please check the fixture" not in custom, "custom local inbox emitted canned fixture text")
    drained = run(cli, state_dir, "message", "check").stdout
    require(drained == "No new messages.\n", "custom local inbox did not drain")


def probe_send_read_and_routes(cli: Path, state_dir: Path) -> None:
    thread_body = f"anti-stub thread body {uuid.uuid4()}\nline two"
    sent = run(cli, state_dir, "message", "send", "--target", "#general:00000000", stdin=thread_body).stdout
    sent_id = parse_message_id(sent)

    thread_history = run(cli, state_dir, "message", "read", "--channel", "#general:00000000").stdout
    require(thread_body in thread_history, "thread send body was not read back verbatim")
    require(sent_id in thread_history, "thread read did not include generated message id")

    parent_history = run(cli, state_dir, "message", "read", "--channel", "#general").stdout
    require(thread_body not in parent_history, "thread message leaked into parent channel history")

    alpha_target = "#probe-alpha:aaaa1111"
    beta_target = "#probe-beta:bbbb2222"
    alpha_body = f"alpha isolated body {uuid.uuid4()}"
    beta_body = f"beta isolated body {uuid.uuid4()}"
    run(cli, state_dir, "message", "send", "--target", alpha_target, stdin=alpha_body)
    run(cli, state_dir, "message", "send", "--target", beta_target, stdin=beta_body)

    alpha_history = run(cli, state_dir, "message", "read", "--channel", alpha_target).stdout
    beta_history = run(cli, state_dir, "message", "read", "--channel", beta_target).stdout
    require(alpha_body in alpha_history, "alpha target did not retain its own message")
    require(beta_body not in alpha_history, "beta message leaked into alpha target")
    require(beta_body in beta_history, "beta target did not retain its own message")
    require(alpha_body not in beta_history, "alpha message leaked into beta target")


def probe_freshness_cursor(cli: Path, state_dir: Path) -> None:
    held_body = f"held draft body {uuid.uuid4()}"
    hold = run(cli, state_dir, "message", "send", "--target", "#general", stdin=held_body).stdout
    require("Freshness hold:" in hold, "channel send did not hit freshness hold before cursor review")
    require("saved as a draft" in hold, "freshness hold did not save draft")

    before_send_draft = run(cli, state_dir, "message", "read", "--channel", "#general").stdout
    require(held_body not in before_send_draft, "draft was appended before send-draft")

    draft_sent = run(cli, state_dir, "message", "send", "--send-draft", "--target", "#general").stdout
    parse_message_id(draft_sent)
    after_send_draft = run(cli, state_dir, "message", "read", "--channel", "#general").stdout
    require(held_body in after_send_draft, "send-draft did not append saved draft")

    direct_body = f"post-cursor direct body {uuid.uuid4()}"
    direct = run(cli, state_dir, "message", "send", "--target", "#general", stdin=direct_body).stdout
    require("Freshness hold:" not in direct, "freshness cursor did not advance after send-draft")
    require("Message sent to #general." in direct, "post-cursor channel send did not succeed")
    final_history = run(cli, state_dir, "message", "read", "--channel", "#general").stdout
    require(direct_body in final_history, "post-cursor direct send was not read back")


def main() -> int:
    cli = Path(os.environ.get("SWARM_CLI", DEFAULT_CLI)).resolve()
    require(cli.exists(), f"SWARM_CLI does not exist: {cli}")

    with tempfile.TemporaryDirectory(prefix="swarm-anti-stub-") as tmp:
        state_dir = Path(tmp)
        probe_inbox(cli, state_dir)
        probe_send_read_and_routes(cli, state_dir)
        probe_freshness_cursor(cli, state_dir)

    print("anti-stub probe ok: dynamic inbox, send/read, routing, and freshness cursor")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeFailure as exc:
        print(f"anti-stub probe failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
