#!/usr/bin/env python3
"""Dynamic anti-stub probes for the minimal Swarm candidate CLI."""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLI = ROOT / "swarm"
STATE_FILE = "state.sqlite3"


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


def connect_state(state_dir: Path) -> sqlite3.Connection:
    path = state_dir / STATE_FILE
    require(path.exists(), f"state file not created: {path}")
    conn = sqlite3.connect(path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def require_sqlite_store(state_dir: Path) -> None:
    require((state_dir / STATE_FILE).exists(), "SQLite state file was not created")
    require(not (state_dir / "state.json").exists(), "legacy JSON state file should not be used")


def insert_local_inbox(state_dir: Path, body: str) -> None:
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO inbox(kind, target, message_id, time, type, author, body)
                VALUES ('local', ?, ?, ?, ?, ?, ?)
                """,
                ("#probe-inbox", str(uuid.uuid4()), "2026-03-15T02:00:00", "human", "verifier", body),
            )
    finally:
        conn.close()


def insert_freshness_blocker(state_dir: Path, target: str, body: str) -> None:
    conn = connect_state(state_dir)
    try:
        with conn:
            row = conn.execute("SELECT value FROM meta WHERE key = 'next_seq'").fetchone()
            require(row is not None, "state meta missing next_seq")
            seq = int(row["value"])
            conn.execute("UPDATE meta SET value = ? WHERE key = 'next_seq'", (str(seq + 1),))
            conn.execute(
                """
                INSERT INTO messages(seq, target, id, time, type, author, body)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (seq, target, str(uuid.uuid4()), "2026-03-15 03:00:00", "human", "verifier", body),
            )
            conn.execute(
                """
                INSERT INTO freshness(target, cursor, draft)
                VALUES (?, ?, NULL)
                ON CONFLICT(target) DO UPDATE SET cursor = excluded.cursor, draft = NULL
                """,
                (target, seq - 1),
            )
    finally:
        conn.close()


def parse_message_id(output: str) -> str:
    match = re.search(r"Message ID: ([0-9a-f-]{36})", output)
    require(match is not None, f"missing sent message id in output:\n{output}")
    return match.group(1)


def parse_task_number(output: str) -> int:
    match = re.search(r"Task #(\d+)", output)
    require(match is not None, f"missing task number in output:\n{output}")
    return int(match.group(1))


def parse_reminder_id(output: str) -> str:
    match = re.search(r"Reminder (rem_[0-9a-f]{8})", output)
    require(match is not None, f"missing reminder id in output:\n{output}")
    return match.group(1)


def timestamp_for_body(history: str, body: str) -> datetime:
    for line in history.splitlines():
        if body in line:
            match = re.search(r"time=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            require(match is not None, f"missing timestamp for body line:\n{line}")
            return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    fail(f"body not found in history for timestamp check: {body}")


def probe_inbox(cli: Path, state_dir: Path) -> None:
    first = run(cli, state_dir, "message", "check").stdout
    require_sqlite_store(state_dir)
    require("please check the fixture" in first, "first check did not display seeded pending inbox")
    require("No more new messages." in first, "pending check missing drain footer")

    second = run(cli, state_dir, "message", "check").stdout
    require(second == "No new messages.\n", f"second check should be empty after drain, got:\n{second}")

    custom_body = f"anti-stub inbox {uuid.uuid4()}"
    insert_local_inbox(state_dir, custom_body)

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

    dm_target = f"dm:@probe-{uuid.uuid4().hex[:8]}"
    dm_body = f"dm isolated body {uuid.uuid4()}"
    dm_sent = run(cli, state_dir, "message", "send", "--target", dm_target, stdin=dm_body).stdout
    dm_id = parse_message_id(dm_sent)
    require(f"Message sent to {dm_target}." in dm_sent, "DM send did not succeed")
    dm_history = run(cli, state_dir, "message", "read", "--channel", dm_target).stdout
    require(dm_body in dm_history, "DM body was not read back")
    require(dm_id in dm_history, "DM read did not include generated message id")
    require(dm_body not in alpha_history, "DM message leaked into channel history")


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

    fresh_target = f"#fresh-{uuid.uuid4().hex[:8]}"
    fresh_body = f"any-channel fresh incoming {uuid.uuid4()}"
    insert_freshness_blocker(state_dir, fresh_target, fresh_body)

    held_any_body = f"any-channel held draft {uuid.uuid4()}"
    hold_any = run(cli, state_dir, "message", "send", "--target", fresh_target, stdin=held_any_body).stdout
    require("Freshness hold:" in hold_any, "non-#general target did not hit freshness hold")
    require(fresh_body in hold_any, "freshness hold did not show target-specific newer message")
    run(cli, state_dir, "message", "send", "--send-draft", "--target", fresh_target)
    fresh_history = run(cli, state_dir, "message", "read", "--channel", fresh_target).stdout
    require(held_any_body in fresh_history, "any-channel send-draft did not append saved draft")


def probe_wall_clock_timestamps(cli: Path, state_dir: Path) -> None:
    target = f"#timestamp-{uuid.uuid4().hex[:8]}"
    body = f"timestamp body {uuid.uuid4()}"
    before = datetime.now()
    run(cli, state_dir, "message", "send", "--target", target, stdin=body)
    after = datetime.now()
    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    observed = timestamp_for_body(history, body)
    require(before.replace(microsecond=0) <= observed <= after.replace(microsecond=0), "sent timestamp is not current wall-clock time")


def probe_cross_process_locking(cli: Path, state_dir: Path) -> None:
    target = f"#concurrent-{uuid.uuid4().hex[:8]}"
    bodies = [f"concurrent body {idx} {uuid.uuid4()}" for idx in range(4)]

    with ThreadPoolExecutor(max_workers=len(bodies)) as pool:
        futures = [
            pool.submit(run, cli, state_dir, "message", "send", "--target", target, stdin=body)
            for body in bodies
        ]
        for future in as_completed(futures):
            output = future.result().stdout
            require(f"Message sent to {target}." in output, f"concurrent send failed:\n{output}")

    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    for body in bodies:
        require(body in history, f"concurrent body missing after serialized SQLite writes: {body}")


def probe_task_lifecycle(cli: Path, state_dir: Path) -> None:
    seed_board = run(cli, state_dir, "task", "list", "--channel", "#general").stdout
    require("## Task Board for #general (2 tasks)" in seed_board, "seed task board did not render")
    require("#1 [in_progress] fixture task title" in seed_board, "seed task #1 missing")

    target = f"#tasks-{uuid.uuid4().hex[:8]}"
    title = f"task lifecycle {uuid.uuid4()}"
    created = run(cli, state_dir, "task", "create", "--channel", target, "--title", title).stdout
    number = parse_task_number(created)
    require(f"Task #{number} created in {target}." in created, "task create did not acknowledge target")

    board = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require(f"#{number} [todo] {title} (by @candidate)" in board, "created task did not appear as todo")

    claimed = run(cli, state_dir, "task", "claim", "--channel", target, "--number", str(number)).stdout
    require(f"Task #{number} claimed by @candidate." in claimed, "task claim did not acknowledge candidate assignee")

    claimed_board = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require(f"#{number} [in_progress] {title} → @candidate" in claimed_board, "claimed task did not become in_progress")

    reviewed = run(cli, state_dir, "task", "update", "--channel", target, "--number", str(number), "--status", "in_review").stdout
    require(f"Task #{number} updated: in_progress -> in_review." in reviewed, "task update to in_review failed")

    done = run(cli, state_dir, "task", "update", "--channel", target, "--number", str(number), "--status", "done").stdout
    require(f"Task #{number} updated: in_review -> done." in done, "task update to done failed")

    rejected = run(
        cli,
        state_dir,
        "task",
        "update",
        "--channel",
        target,
        "--number",
        str(number),
        "--status",
        "done",
        expected=1,
    ).stderr
    require("cannot transition from done to done" in rejected, "idempotent done update was not rejected")


def probe_reminder_lifecycle(cli: Path, state_dir: Path) -> None:
    empty = run(cli, state_dir, "reminder", "list").stdout
    require(empty == "No reminders.\n", f"empty reminder list rendered unexpectedly:\n{empty}")

    title = f"reminder lifecycle {uuid.uuid4()}"
    scheduled = run(
        cli,
        state_dir,
        "reminder",
        "schedule",
        "--target",
        "#general:00000000",
        "--title",
        title,
        "--at",
        "2031-04-05T06:07:08",
    ).stdout
    reminder_id = parse_reminder_id(scheduled)
    require("scheduled for 2031-04-05 06:07:08" in scheduled, "schedule did not normalize timestamp")

    listed = run(cli, state_dir, "reminder", "list").stdout
    require(reminder_id in listed, "scheduled reminder missing from list")
    require(title in listed, "scheduled reminder title missing from list")
    require("#general:00000000" in listed, "scheduled reminder target missing from list")

    snoozed = run(cli, state_dir, "reminder", "snooze", "--id", reminder_id, "--until", "2031-04-06T06:07:08").stdout
    require("snoozed: 2031-04-05 06:07:08 -> 2031-04-06 06:07:08" in snoozed, "snooze did not update fire time")
    snoozed_list = run(cli, state_dir, "reminder", "list").stdout
    require(f"{reminder_id} [snoozed]" in snoozed_list, "snoozed status missing from list")
    require("next=2031-04-06 06:07:08" in snoozed_list, "snoozed next fire missing from list")

    updated_title = f"updated reminder {uuid.uuid4()}"
    updated = run(
        cli,
        state_dir,
        "reminder",
        "update",
        "--id",
        reminder_id,
        "--title",
        updated_title,
        "--at",
        "2031-04-07T06:07:08",
        "--every",
        "1d",
    ).stdout
    require(f"Reminder {reminder_id} updated." in updated, "update did not acknowledge reminder")
    updated_list = run(cli, state_dir, "reminder", "list").stdout
    require(f"{reminder_id} [scheduled]" in updated_list, "updated snoozed reminder did not return to scheduled")
    require(updated_title in updated_list, "updated title missing from list")
    require("next=2031-04-07 06:07:08" in updated_list, "updated next fire missing from list")
    require("every=1d" in updated_list, "updated recurrence missing from list")

    log = run(cli, state_dir, "reminder", "log", "--id", reminder_id).stdout
    require("scheduled:" in log and "snoozed:" in log and "updated:" in log, "reminder log missing lifecycle events")
    require(updated_title in log, "reminder log missing current reminder title")

    canceled = run(cli, state_dir, "reminder", "cancel", "--id", reminder_id).stdout
    require(f"Reminder {reminder_id} canceled." in canceled, "cancel did not acknowledge reminder")
    active_list = run(cli, state_dir, "reminder", "list").stdout
    require(reminder_id not in active_list, "canceled reminder should be hidden from active list")
    all_list = run(cli, state_dir, "reminder", "list", "--all").stdout
    require(f"{reminder_id} [canceled]" in all_list, "canceled reminder missing from --all list")
    rejected = run(cli, state_dir, "reminder", "snooze", "--id", reminder_id, "--for", "5m", expected=1).stderr
    require("canceled reminders cannot be snoozed" in rejected, "canceled snooze was not rejected")


def main() -> int:
    cli = Path(os.environ.get("SWARM_CLI", DEFAULT_CLI)).resolve()
    require(cli.exists(), f"SWARM_CLI does not exist: {cli}")

    with tempfile.TemporaryDirectory(prefix="swarm-anti-stub-") as tmp:
        state_dir = Path(tmp)
        probe_inbox(cli, state_dir)
        probe_send_read_and_routes(cli, state_dir)
        probe_freshness_cursor(cli, state_dir)
        probe_wall_clock_timestamps(cli, state_dir)
        probe_cross_process_locking(cli, state_dir)
        probe_task_lifecycle(cli, state_dir)
        probe_reminder_lifecycle(cli, state_dir)

    print("anti-stub probe ok: dynamic inbox, send/read, routing, freshness cursor, DM, timestamps, SQLite locking, tasks, and reminders")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeFailure as exc:
        print(f"anti-stub probe failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
