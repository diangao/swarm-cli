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


def run(
    cli: Path,
    state_dir: Path,
    *args: str,
    stdin: str | None = None,
    expected: int = 0,
    seed_fixtures: bool = True,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    if seed_fixtures:
        env["SWARM_CANDIDATE_SEED_FIXTURES"] = "1"
    else:
        env.pop("SWARM_CANDIDATE_SEED_FIXTURES", None)
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


def probe_fresh_store_empty(cli: Path) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        check = run(cli, state_dir, "message", "check", seed_fixtures=False).stdout
        require(check == "No new messages.\n", f"fresh unseeded inbox was not empty:\n{check}")

        history = run(cli, state_dir, "message", "read", "--channel", "#general", seed_fixtures=False).stdout
        require("## Message History for #general (0 messages)" in history, "fresh #general read was not empty")
        require("alice" not in history, "fresh #general leaked fixture user")
        require("parent message" not in history, "fresh #general leaked fixture message")

        search = run(
            cli,
            state_dir,
            "message",
            "search",
            "--query",
            "parent",
            "--channel",
            "#general",
            seed_fixtures=False,
        ).stdout
        require("(0 results)" in search, "fresh #general search leaked seeded records")


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


def parse_attachment_id(output: str) -> str:
    match = re.search(r"Attachment ID: ([0-9a-f-]{36})", output)
    require(match is not None, f"missing attachment id in output:\n{output}")
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


def probe_read_pagination(cli: Path, state_dir: Path) -> None:
    target = f"#page-{uuid.uuid4().hex[:8]}"
    bodies = [f"paged body {idx} {uuid.uuid4()}" for idx in range(4)]
    sent_ids = []
    for body in bodies:
        sent_ids.append(parse_message_id(run(cli, state_dir, "message", "send", "--target", target, stdin=body).stdout))

    full = run(cli, state_dir, "message", "read", "--channel", target).stdout
    for body in bodies:
        require(body in full, f"full paged read missing body: {body}")

    before = run(cli, state_dir, "message", "read", "--channel", target, "--before", sent_ids[2][:8]).stdout
    require(bodies[0] in before and bodies[1] in before, "--before short id did not show earlier records")
    require(bodies[2] not in before and bodies[3] not in before, "--before included anchor or later records")

    after = run(cli, state_dir, "message", "read", "--channel", target, "--after", sent_ids[1]).stdout
    require(bodies[2] in after and bodies[3] in after, "--after full id did not show later records")
    require(bodies[0] not in after and bodies[1] not in after, "--after included anchor or earlier records")

    around = run(cli, state_dir, "message", "read", "--channel", target, "--around", sent_ids[2]).stdout
    require(bodies[2] in around, "--around did not include anchor record")
    require("---" in around, "--around missing read footer")

    missing = run(cli, state_dir, "message", "read", "--channel", target, "--around", "does-not-exist", expected=1).stderr
    require("anchor not found" in missing, "missing read anchor did not fail closed")


def probe_search_and_resolve(cli: Path, state_dir: Path) -> None:
    target = f"#search-{uuid.uuid4().hex[:8]}"
    needle = f"needle-{uuid.uuid4().hex}"
    body = f"searchable message {needle} inside local persisted body"
    other_target = f"#search-other-{uuid.uuid4().hex[:8]}"
    other_body = f"other target also mentions {needle}"

    sent_id = parse_message_id(run(cli, state_dir, "message", "send", "--target", target, stdin=body).stdout)
    run(cli, state_dir, "message", "send", "--target", other_target, stdin=other_body)

    search = run(cli, state_dir, "message", "search", "--query", needle, "--channel", target).stdout
    require(f"Search results for: \"{needle}\" (1 results)" in search, "channel search did not return exactly one result")
    require(sent_id in search, "search result missing generated message id")
    require("<match>" in search and "</match>" in search, "search preview did not highlight query")
    require("searchable message" in search, "search preview missing persisted prefix text")
    require("inside local persisted body" in search, "search preview missing persisted suffix text")
    require(other_body not in search, "channel-restricted search leaked other target")

    sender_filtered = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        needle,
        "--sender",
        "@candidate",
    ).stdout
    require(sent_id in sender_filtered, "sender-filtered search missing candidate-authored message")
    sender_empty = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        needle,
        "--sender",
        "@alice",
    ).stdout
    require("(0 results)" in sender_empty, "sender filter did not exclude non-matching author")

    resolved_full = run(cli, state_dir, "message", "resolve", sent_id).stdout
    require(f"msg={sent_id[:8]}" in resolved_full, "resolve full id did not print short canonical id")
    require(f"target={target}" in resolved_full, "resolve full id missing target")
    require(body in resolved_full, "resolve full id missing body")

    resolved_short = run(cli, state_dir, "message", "resolve", sent_id[:8]).stdout
    require(resolved_short == resolved_full, "resolve short id did not match full id output")
    missing = run(cli, state_dir, "message", "resolve", "doesnotexist", expected=1).stderr
    require("Error: Message not found" in missing, "resolve missing id did not fail closed")
    require("Next action:" in missing, "resolve missing id did not include recovery hint")
    require("doesnotexist" not in missing, "resolve missing id echoed unknown id")


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

    unclaimed = run(cli, state_dir, "task", "unclaim", "--channel", target, "--number", str(number)).stdout
    require(f"Task #{number} unclaimed by @candidate." in unclaimed, "task unclaim did not acknowledge candidate")

    unclaimed_board = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require(
        f"#{number} [in_progress] {title} (by @candidate)" in unclaimed_board,
        "unclaimed task did not remove assignee while preserving status",
    )

    rejected_unclaim = run(
        cli,
        state_dir,
        "task",
        "unclaim",
        "--channel",
        target,
        "--number",
        str(number),
        expected=1,
    ).stderr
    require("is not claimed by @candidate" in rejected_unclaim, "unclaim of unclaimed task did not fail closed")

    reclaimed = run(cli, state_dir, "task", "claim", "--channel", target, "--number", str(number)).stdout
    require(f"Task #{number} claimed by @candidate." in reclaimed, "task reclaim after unclaim failed")

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

    done_unclaim = run(
        cli,
        state_dir,
        "task",
        "unclaim",
        "--channel",
        target,
        "--number",
        str(number),
        expected=1,
    ).stderr
    require("done tasks cannot be unclaimed" in done_unclaim, "unclaim of done task did not fail closed")

    batch_title_1 = f"batch task one {uuid.uuid4()}"
    batch_title_2 = f"batch task two {uuid.uuid4()}"
    batch_one = run(cli, state_dir, "task", "create", "--channel", target, "--title", batch_title_1).stdout
    batch_two = run(cli, state_dir, "task", "create", "--channel", target, "--title", batch_title_2).stdout
    batch_number_1 = parse_task_number(batch_one)
    batch_number_2 = parse_task_number(batch_two)
    batch_claimed = run(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        target,
        "--number",
        str(batch_number_1),
        "--number",
        str(batch_number_2),
    ).stdout
    require(f"Task #{batch_number_1} claimed by @candidate." in batch_claimed, "batch claim missing first task")
    require(f"Task #{batch_number_2} claimed by @candidate." in batch_claimed, "batch claim missing second task")
    batch_board = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require(
        f"#{batch_number_1} [in_progress] {batch_title_1} → @candidate" in batch_board,
        "batch claim did not persist first assignee",
    )
    require(
        f"#{batch_number_2} [in_progress] {batch_title_2} → @candidate" in batch_board,
        "batch claim did not persist second assignee",
    )
    batch_unclaimed = run(
        cli,
        state_dir,
        "task",
        "unclaim",
        "--channel",
        target,
        "--number",
        str(batch_number_1),
        "--number",
        str(batch_number_2),
    ).stdout
    require(f"Task #{batch_number_1} unclaimed by @candidate." in batch_unclaimed, "batch unclaim missing first task")
    require(f"Task #{batch_number_2} unclaimed by @candidate." in batch_unclaimed, "batch unclaim missing second task")
    batch_unclaimed_board = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require(
        f"#{batch_number_1} [in_progress] {batch_title_1} (by @candidate)" in batch_unclaimed_board,
        "batch unclaim did not clear first assignee",
    )
    require(
        f"#{batch_number_2} [in_progress] {batch_title_2} (by @candidate)" in batch_unclaimed_board,
        "batch unclaim did not clear second assignee",
    )


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


def probe_navigation_surfaces(cli: Path, state_dir: Path) -> None:
    server_info = run(cli, state_dir, "server", "info").stdout
    require("### Channels" in server_info, "server info missing Channels section")
    require("### Agents" in server_info, "server info missing Agents section")
    require("### Humans" in server_info, "server info missing Humans section")
    require("#general" in server_info, "server info missing seeded channel")

    members = run(cli, state_dir, "channel", "members", "#general").stdout
    require("## Channel Members" in members, "channel members missing heading")
    require("@candidate" in members, "channel members missing local agent")
    require("@alice" in members, "channel members missing seeded human")

    private_members = run(cli, state_dir, "channel", "members", "#private-fixture").stdout
    require("Channel: #private-fixture (private)" in private_members, "private channel visibility label missing")
    require("Members means join/post authority" in private_members, "private channel boundary text missing")

    profile = run(cli, state_dir, "profile", "show").stdout
    require("## Profile" in profile, "profile show missing heading")
    require("@candidate" in profile, "profile show missing local profile name")
    rejected = run(cli, state_dir, "profile", "show", "@other", expected=1).stderr
    require("machine API key required" in rejected, "profile show other did not fail closed")

    display_name = f"Candidate {uuid.uuid4().hex[:6]}"
    description = f"profile update {uuid.uuid4()}"
    updated = run(
        cli,
        state_dir,
        "profile",
        "update",
        "--display-name",
        display_name,
        "--description",
        description,
    ).stdout
    require(updated == "Profile updated.\n", "profile update did not acknowledge write")
    shown = run(cli, state_dir, "profile", "show").stdout
    require(f"display_name: {display_name}" in shown, "profile show did not render updated display name")
    require(f"description: {description}" in shown, "profile show did not render updated description")
    server_after_profile = run(cli, state_dir, "server", "info").stdout
    require(f"@candidate ({display_name})" in server_after_profile, "server info did not use updated display name")
    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            "SELECT display_name, description FROM profiles WHERE name = 'candidate'"
        ).fetchone()
        require(row is not None, "updated profile row missing from SQLite")
        require(row["display_name"] == display_name, "profile display name not persisted")
        require(row["description"] == description, "profile description not persisted")
    finally:
        conn.close()

    dynamic_channel = f"#catalog-{uuid.uuid4().hex[:8]}"
    dynamic_body = f"catalog body {uuid.uuid4()}"
    run(cli, state_dir, "message", "send", "--target", dynamic_channel, stdin=dynamic_body)
    updated_server_info = run(cli, state_dir, "server", "info").stdout
    require(dynamic_channel in updated_server_info, "server info did not include dynamic local channel")
    dynamic_members = run(cli, state_dir, "channel", "members", dynamic_channel).stdout
    require("@candidate" in dynamic_members, "dynamic channel did not include candidate member")


def probe_membership_attention(cli: Path, state_dir: Path) -> None:
    target = f"#join-{uuid.uuid4().hex[:8]}"
    joined = run(cli, state_dir, "channel", "join", target).stdout
    require(f"Joined {target}." in joined, "channel join did not acknowledge target")
    info = run(cli, state_dir, "server", "info").stdout
    require(f"{target} (public, joined)" in info, "joined channel missing from server info")

    left = run(cli, state_dir, "channel", "leave", target).stdout
    require(f"Left {target}." in left, "channel leave did not acknowledge target")
    after_leave_info = run(cli, state_dir, "server", "info").stdout
    require(f"{target} (public, not joined)" in after_leave_info, "left channel did not become not joined")
    rejected_send = run(
        cli,
        state_dir,
        "message",
        "send",
        "--target",
        target,
        stdin="should not send while left",
        expected=1,
    ).stderr
    require("Not joined to target" in rejected_send, "send to left channel did not fail closed")

    run(cli, state_dir, "channel", "join", target)
    accepted = run(cli, state_dir, "message", "send", "--target", target, stdin=f"after rejoin {uuid.uuid4()}").stdout
    require(f"Message sent to {target}." in accepted, "send after rejoin did not succeed")

    private_rejected = run(cli, state_dir, "channel", "leave", "#private-fixture", expected=1).stderr
    require("private channel membership is managed by the server" in private_rejected, "private leave did not fail closed")

    thread_target = f"{target}:abcd1234"
    unfollowed = run(cli, state_dir, "thread", "unfollow", "--target", thread_target).stdout
    require(f"Unfollowed {thread_target}." in unfollowed, "thread unfollow did not acknowledge target")
    conn = connect_state(state_dir)
    try:
        row = conn.execute("SELECT followed FROM thread_attention WHERE target = ?", (thread_target,)).fetchone()
        require(row is not None, "thread unfollow did not persist attention row")
        require(row["followed"] == 0, "thread unfollow row did not mark followed=false")
    finally:
        conn.close()


def probe_attachments(cli: Path, state_dir: Path) -> None:
    payload = f"attachment payload {uuid.uuid4()}\nsecond line\n".encode("utf-8")
    source = state_dir / "source-attachment.txt"
    source.write_bytes(payload)

    uploaded = run(
        cli,
        state_dir,
        "attachment",
        "upload",
        "--path",
        str(source),
        "--channel",
        "#attachments",
        "--mime-type",
        "text/plain",
    ).stdout
    attachment_id = parse_attachment_id(uploaded)
    require("filename: source-attachment.txt" in uploaded, "attachment upload missing filename metadata")
    require("mime_type: text/plain" in uploaded, "attachment upload missing mime metadata")
    require(f"size_bytes: {len(payload)}" in uploaded, "attachment upload missing byte size")
    require("sha256:" in uploaded, "attachment upload missing digest")

    output = state_dir / "downloaded" / "out.txt"
    viewed = run(
        cli,
        state_dir,
        "attachment",
        "view",
        "--id",
        attachment_id,
        "--output",
        str(output),
    ).stdout
    require(f"Attachment {attachment_id} saved" in viewed, "attachment view did not acknowledge saved file")
    require(output.read_bytes() == payload, "attachment view bytes did not match uploaded source")

    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            "SELECT channel, filename, mime_type, size_bytes, stored_path FROM attachments WHERE id = ?",
            (attachment_id,),
        ).fetchone()
        require(row is not None, "attachment metadata was not persisted")
        require(row["channel"] == "#attachments", "attachment channel metadata mismatch")
        require(row["filename"] == "source-attachment.txt", "attachment filename metadata mismatch")
        require(row["mime_type"] == "text/plain", "attachment MIME metadata mismatch")
        require(row["size_bytes"] == len(payload), "attachment size metadata mismatch")
        require((state_dir / row["stored_path"]).read_bytes() == payload, "stored attachment bytes mismatch")
    finally:
        conn.close()

    message_body = f"message carrying attachment {uuid.uuid4()}"
    sent = run(
        cli,
        state_dir,
        "message",
        "send",
        "--target",
        "#attachments",
        "--attachment-id",
        attachment_id,
        stdin=message_body,
    ).stdout
    message_id = parse_message_id(sent)
    history = run(cli, state_dir, "message", "read", "--channel", "#attachments").stdout
    require(message_body in history, "attachment message body was not readable")
    require(f"id:{attachment_id}" in history, "message read did not render attachment id")
    require("source-attachment.txt" in history, "message read did not render attachment filename")

    resolved = run(cli, state_dir, "message", "resolve", message_id).stdout
    require(f"id:{attachment_id}" in resolved, "message resolve did not render attachment id")

    conn = connect_state(state_dir)
    try:
        link = conn.execute(
            "SELECT attachment_id, ordinal FROM message_attachments WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        require(link is not None, "message attachment link was not persisted")
        require(link["attachment_id"] == attachment_id, "message attachment link id mismatch")
        require(link["ordinal"] == 1, "message attachment link ordinal mismatch")
    finally:
        conn.close()

    missing = run(cli, state_dir, "attachment", "view", "--id", str(uuid.uuid4()), "--output", str(output), expected=1).stderr
    require("Attachment not found" in missing, "unknown attachment id did not fail closed")

    missing_file = run(
        cli,
        state_dir,
        "attachment",
        "upload",
        "--path",
        str(state_dir / "missing.txt"),
        "--channel",
        "#attachments",
        expected=1,
    ).stderr
    require("File not found" in missing_file, "missing upload path did not fail closed")


def main() -> int:
    cli = Path(os.environ.get("SWARM_CLI", DEFAULT_CLI)).resolve()
    require(cli.exists(), f"SWARM_CLI does not exist: {cli}")
    probe_fresh_store_empty(cli)

    with tempfile.TemporaryDirectory(prefix="swarm-anti-stub-") as tmp:
        state_dir = Path(tmp)
        probe_inbox(cli, state_dir)
        probe_send_read_and_routes(cli, state_dir)
        probe_read_pagination(cli, state_dir)
        probe_search_and_resolve(cli, state_dir)
        probe_freshness_cursor(cli, state_dir)
        probe_wall_clock_timestamps(cli, state_dir)
        probe_cross_process_locking(cli, state_dir)
        probe_task_lifecycle(cli, state_dir)
        probe_reminder_lifecycle(cli, state_dir)
        probe_navigation_surfaces(cli, state_dir)
        probe_membership_attention(cli, state_dir)
        probe_attachments(cli, state_dir)

    print("anti-stub probe ok: empty fresh store, dynamic inbox, send/read, pagination, search/resolve, routing, freshness cursor, DM, timestamps, SQLite locking, tasks, reminders, navigation, membership, and attachments")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeFailure as exc:
        print(f"anti-stub probe failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
