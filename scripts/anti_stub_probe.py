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
import json
import importlib.machinery
import importlib.util
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs


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


def load_cli_module(cli: Path) -> object:
    loader = importlib.machinery.SourceFileLoader("swarm_under_probe", str(cli))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    require(spec is not None, "could not load swarm CLI module spec")
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def run(
    cli: Path,
    state_dir: Path,
    *args: str,
    stdin: str | None = None,
    expected: int = 0,
    seed_fixtures: bool = True,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    if seed_fixtures:
        env["SWARM_CANDIDATE_SEED_FIXTURES"] = "1"
    else:
        env.pop("SWARM_CANDIDATE_SEED_FIXTURES", None)
    if env_overrides:
        env.update(env_overrides)
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


def parse_task_numbers(output: str) -> list[int]:
    numbers = [int(value) for value in re.findall(r"Task #(\d+)", output)]
    require(numbers, f"missing task numbers in output:\n{output}")
    return numbers


def parse_reminder_id(output: str) -> str:
    match = re.search(r"Reminder (rem_[0-9a-f]{8})", output)
    require(match is not None, f"missing reminder id in output:\n{output}")
    return match.group(1)


def parse_action_id(output: str) -> str:
    match = re.search(r"Action ID: (act_[0-9a-f]{8})", output)
    require(match is not None, f"missing action id in output:\n{output}")
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
    channel_target = f"#replyhint-{uuid.uuid4().hex[:8]}"
    channel_body = f"top-level reply hint body {uuid.uuid4()}"
    channel_sent = run(cli, state_dir, "message", "send", "--target", channel_target, stdin=channel_body).stdout
    channel_id = parse_message_id(channel_sent)
    require(
        f'use target "{channel_target}:{channel_id[:8]}"' in channel_sent,
        "top-level channel send did not render message-root thread target hint",
    )

    thread_body = f"anti-stub thread body {uuid.uuid4()}\nline two"
    sent = run(cli, state_dir, "message", "send", "--target", "#general:00000000", stdin=thread_body).stdout
    sent_id = parse_message_id(sent)
    require(
        'use target "#general:00000000"' in sent,
        "thread send rendered nested thread target hint instead of staying in thread",
    )

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
    require(
        f'use target "{dm_target}:{dm_id[:8]}"' in dm_sent,
        "top-level DM send did not render DM thread target hint",
    )
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

    limited = run(cli, state_dir, "message", "read", "--channel", target, "--limit", "2").stdout
    require(bodies[2] in limited and bodies[3] in limited, "--limit 2 did not show the newest two records")
    require(bodies[0] not in limited and bodies[1] not in limited, "--limit 2 included records outside the window")
    require("2 messages shown. 2 of 4 total." in limited, "--limit 2 did not disclose shown/total count")

    before = run(cli, state_dir, "message", "read", "--channel", target, "--before", sent_ids[2][:8]).stdout
    require(bodies[0] in before and bodies[1] in before, "--before short id did not show earlier records")
    require(bodies[2] not in before and bodies[3] not in before, "--before included anchor or later records")

    before_limited = run(
        cli,
        state_dir,
        "message",
        "read",
        "--channel",
        target,
        "--before",
        sent_ids[3],
        "--limit",
        "1",
    ).stdout
    require(bodies[2] in before_limited, "--before with --limit 1 did not show the nearest earlier record")
    require(bodies[0] not in before_limited and bodies[1] not in before_limited, "--before limit leaked older records")

    after = run(cli, state_dir, "message", "read", "--channel", target, "--after", sent_ids[1]).stdout
    require(bodies[2] in after and bodies[3] in after, "--after full id did not show later records")
    require(bodies[0] not in after and bodies[1] not in after, "--after included anchor or earlier records")
    require("2 messages shown. 2 of 4 total." in after, "--after did not disclose shown/total count")

    around = run(cli, state_dir, "message", "read", "--channel", target, "--around", sent_ids[2]).stdout
    require(bodies[2] in around, "--around did not include anchor record")
    require("---" in around, "--around missing read footer")

    missing = run(cli, state_dir, "message", "read", "--channel", target, "--around", "does-not-exist", expected=1).stderr
    require("anchor not found" in missing, "missing read anchor did not fail closed")

    invalid_limit = run(cli, state_dir, "message", "read", "--channel", target, "--limit", "0", expected=1).stderr
    require("--limit must be a positive integer" in invalid_limit, "invalid read limit did not fail closed")

    overflow_target = f"#page-overflow-{uuid.uuid4().hex[:8]}"
    overflow_bodies = [f"overflow body {idx:02d} {uuid.uuid4()}" for idx in range(23)]
    for body in overflow_bodies:
        run(cli, state_dir, "message", "send", "--target", overflow_target, stdin=body)
    with connect_state(state_dir) as conn:
        stored = conn.execute("SELECT COUNT(*) AS count FROM messages WHERE target = ?", (overflow_target,)).fetchone()["count"]
        require(stored == 23, "overflow pagination fixture did not persist all messages")

    default_window = run(cli, state_dir, "message", "read", "--channel", overflow_target).stdout
    require("20 messages shown. 20 of 23 total." in default_window, "default read did not disclose truncated total")
    require(overflow_bodies[0] not in default_window and overflow_bodies[2] not in default_window, "default read leaked older overflow records")
    require(overflow_bodies[3] in default_window and overflow_bodies[22] in default_window, "default read did not show newest overflow window")


def probe_read_known_empty_surfaces(cli: Path, state_dir: Path) -> None:
    empty_target = f"#empty-{uuid.uuid4().hex[:8]}"
    run(cli, state_dir, "channel", "join", "--target", empty_target)
    with connect_state(state_dir) as conn:
        row = conn.execute("SELECT joined FROM channels WHERE name = ?", (empty_target,)).fetchone()
        require(row is not None and row["joined"] == 1, "joined empty channel was not persisted")

    empty_history = run(cli, state_dir, "message", "read", "--channel", empty_target).stdout
    require(f"## Message History for {empty_target} (0 messages)" in empty_history, "empty known channel did not render zero history")
    require("--- 0 messages shown. ---" in empty_history, "empty known channel missing zero-history footer")

    missing_target = f"#missing-{uuid.uuid4().hex[:8]}"
    missing = run(cli, state_dir, "message", "read", "--channel", missing_target, expected=1).stderr
    require("Channel not found" in missing, "unknown channel read did not fail closed")

    thread_channel = f"#parentonly-{uuid.uuid4().hex[:8]}"
    parent_body = f"parent-only thread root {uuid.uuid4()}"
    parent_id = parse_message_id(run(cli, state_dir, "message", "send", "--target", thread_channel, stdin=parent_body).stdout)
    parent_thread = f"{thread_channel}:{parent_id[:8]}"
    parent_thread_history = run(cli, state_dir, "message", "read", "--channel", parent_thread).stdout
    require(parent_body in parent_thread_history, "parent-only thread read did not include parent message")
    require(f"## Message History for {parent_thread} (1 messages)" in parent_thread_history, "parent-only thread count was not one")

    missing_thread = run(
        cli,
        state_dir,
        "message",
        "read",
        "--channel",
        f"{thread_channel}:deadbeef",
        expected=1,
    ).stderr
    require("Channel not found" in missing_thread, "thread without a parent did not fail closed")


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

    limit_target = f"#search-limit-{uuid.uuid4().hex[:8]}"
    limit_needle = f"searchlimit-{uuid.uuid4().hex}"
    limit_bodies = [f"bounded search body {idx} {limit_needle} {uuid.uuid4()}" for idx in range(5)]
    for body_text in limit_bodies:
        run(cli, state_dir, "message", "send", "--target", limit_target, stdin=body_text)
    with connect_state(state_dir) as conn:
        before_invalid = conn.execute("SELECT COUNT(*) AS count FROM messages WHERE target = ?", (limit_target,)).fetchone()["count"]
        require(before_invalid == 5, "search limit fixture did not persist all messages")

    limited = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--limit",
        "2",
    ).stdout
    require(f"Search results for: \"{limit_needle}\" (2 results, 2 of 5 total)" in limited, "bounded search did not disclose total")
    require("bounded search body 4" in limited and "bounded search body 3" in limited, "bounded search did not include newest matching rows")
    require("bounded search body 0" not in limited and "bounded search body 2" not in limited, "bounded search leaked rows outside limit")

    recent_sorted = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--sort",
        "recent",
        "--limit",
        "2",
    ).stdout
    require(f"Search results for: \"{limit_needle}\" (2 results, 2 of 5 total)" in recent_sorted, "recent search did not disclose total")
    require("bounded search body 4" in recent_sorted and "bounded search body 3" in recent_sorted, "recent search did not return newest rows")

    invalid_sort = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--sort",
        "alphabetical",
        expected=1,
    ).stderr
    require("--sort must be one of: relevance, recent" in invalid_sort, "invalid search sort did not fail closed")
    missing_sort = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--sort",
        expected=1,
    ).stderr
    require("--sort must be one of: relevance, recent" in missing_sort, "missing search sort value did not fail closed")

    invalid_limit = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--limit",
        "0",
        expected=1,
    ).stderr
    require("--limit must be a positive integer" in invalid_limit, "search limit 0 did not fail closed")
    non_integer_limit = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--limit",
        "many",
        expected=1,
    ).stderr
    require("--limit must be a positive integer" in non_integer_limit, "non-integer search limit did not fail closed")
    too_large_limit = run(
        cli,
        state_dir,
        "message",
        "search",
        "--query",
        limit_needle,
        "--channel",
        limit_target,
        "--limit",
        "51",
        expected=1,
    ).stderr
    require("--limit must be at most 50" in too_large_limit, "oversized search limit did not fail closed")
    with connect_state(state_dir) as conn:
        after_invalid = conn.execute("SELECT COUNT(*) AS count FROM messages WHERE target = ?", (limit_target,)).fetchone()["count"]
        require(after_invalid == before_invalid, "invalid search limit mutated message rows")


def probe_message_reactions(cli: Path, state_dir: Path) -> None:
    target = f"#react-{uuid.uuid4().hex[:8]}"
    body = f"reactable body {uuid.uuid4()}"
    msg_id = parse_message_id(run(cli, state_dir, "message", "send", "--target", target, stdin=body).stdout)

    added = run(cli, state_dir, "message", "react", "--message-id", msg_id, "--emoji", "+1").stdout
    require(f"Reaction +1 added to {msg_id[:8]}." in added, "message react did not acknowledge add")
    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require("[reactions:" in history, "message read did not render reaction summary")
    require("+1 x1 @candidate" in history, "message read did not render candidate reaction")
    resolved = run(cli, state_dir, "message", "resolve", msg_id[:8]).stdout
    require("+1 x1 @candidate" in resolved, "message resolve did not render reaction summary")

    duplicate = run(cli, state_dir, "message", "react", "--message-id", msg_id, "--emoji", "+1").stdout
    require("already present" in duplicate, "duplicate reaction add was not idempotent")
    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM message_reactions
            WHERE message_id = ? AND emoji = '+1' AND author = 'candidate'
            """,
            (msg_id,),
        ).fetchone()
        require(row is not None and row["count"] == 1, "duplicate reaction add created multiple rows")
    finally:
        conn.close()

    removed = run(cli, state_dir, "message", "react", "--message-id", msg_id, "--emoji", "+1", "--remove").stdout
    require(f"Reaction +1 removed from {msg_id[:8]}." in removed, "message react did not acknowledge remove")
    after_remove = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require("+1 x1 @candidate" not in after_remove, "removed reaction still rendered in read output")

    missing_reaction = run(
        cli,
        state_dir,
        "message",
        "react",
        "--message-id",
        msg_id,
        "--emoji",
        "+1",
        "--remove",
        expected=1,
    ).stderr
    require("Reaction not found" in missing_reaction, "removing absent reaction did not fail closed")
    missing_message = run(
        cli,
        state_dir,
        "message",
        "react",
        "--message-id",
        "doesnotexist",
        "--emoji",
        "+1",
        expected=1,
    ).stderr
    require("Message not found" in missing_message, "reacting to unknown message did not fail closed")


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

    left_target = f"#left-draft-{uuid.uuid4().hex[:8]}"
    run(cli, state_dir, "channel", "join", left_target)
    left_fresh_body = f"left-channel fresh incoming {uuid.uuid4()}"
    insert_freshness_blocker(state_dir, left_target, left_fresh_body)
    left_draft_body = f"left-channel saved draft {uuid.uuid4()}"
    left_hold = run(cli, state_dir, "message", "send", "--target", left_target, stdin=left_draft_body).stdout
    require("Freshness hold:" in left_hold and "saved as a draft" in left_hold, "left-channel setup did not save draft")
    run(cli, state_dir, "channel", "leave", left_target)
    conn = connect_state(state_dir)
    try:
        before_left_draft_send = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE target = ? AND body = ?",
            (left_target, left_draft_body),
        ).fetchone()["count"]
    finally:
        conn.close()
    rejected_left_draft = run(
        cli,
        state_dir,
        "message",
        "send",
        "--send-draft",
        "--target",
        left_target,
        expected=1,
    ).stderr
    require("Not joined to target" in rejected_left_draft, "send-draft to left channel did not fail closed")
    conn = connect_state(state_dir)
    try:
        after_left_draft_send = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE target = ? AND body = ?",
            (left_target, left_draft_body),
        ).fetchone()["count"]
        draft_row = conn.execute("SELECT draft FROM freshness WHERE target = ?", (left_target,)).fetchone()
        require(after_left_draft_send == before_left_draft_send, "failed send-draft appended message while channel was left")
        require(draft_row is not None and draft_row["draft"], "failed send-draft cleared the saved draft")
    finally:
        conn.close()
    run(cli, state_dir, "channel", "join", left_target)
    sent_left_draft = run(cli, state_dir, "message", "send", "--send-draft", "--target", left_target).stdout
    require(f"Message sent to {left_target}." in sent_left_draft, "send-draft after rejoin did not succeed")
    left_history = run(cli, state_dir, "message", "read", "--channel", left_target).stdout
    require(left_draft_body in left_history, "saved draft was not preserved after failed send-draft")


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
    batch_created = run(
        cli,
        state_dir,
        "task",
        "create",
        "--channel",
        target,
        "--title",
        batch_title_1,
        "--title",
        batch_title_2,
    ).stdout
    batch_number_1, batch_number_2 = parse_task_numbers(batch_created)
    require(f"Task #{batch_number_1} created in {target}." in batch_created, "batch create missing first task")
    require(f"Task #{batch_number_2} created in {target}." in batch_created, "batch create missing second task")
    batch_created_board = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require(f"#{batch_number_1} [todo] {batch_title_1} (by @candidate)" in batch_created_board, "batch create did not persist first task")
    require(f"#{batch_number_2} [todo] {batch_title_2} (by @candidate)" in batch_created_board, "batch create did not persist second task")
    rejected_batch_create = run(
        cli,
        state_dir,
        "task",
        "create",
        "--channel",
        target,
        "--title",
        "",
        "--title",
        f"should not create {uuid.uuid4()}",
        expected=1,
    ).stderr
    require("task title is required" in rejected_batch_create, "batch create with empty title did not fail closed")
    after_rejected_batch = run(cli, state_dir, "task", "list", "--channel", target).stdout
    require("should not create" not in after_rejected_batch, "invalid batch create partially created a task")
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

    conn = connect_state(state_dir)
    try:
        before_filter_rows = [
            (row["number"], row["status"], row["assignee"])
            for row in conn.execute(
                "SELECT number, status, assignee FROM tasks WHERE channel = ? ORDER BY number",
                (target,),
            ).fetchall()
        ]
    finally:
        conn.close()

    done_filter = run(cli, state_dir, "task", "list", "--channel", target, "--status", "done").stdout
    require(f"#{number} [done] {title} → @candidate" in done_filter, "status=done filter missed done task")
    require(batch_title_1 not in done_filter and batch_title_2 not in done_filter, "status=done filter leaked in-progress tasks")

    unassigned_filter = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--status",
        "in_progress",
        "--unassigned",
    ).stdout
    require(
        f"#{batch_number_1} [in_progress] {batch_title_1} (by @candidate)" in unassigned_filter,
        "unassigned filter missed first unassigned in-progress task",
    )
    require(
        f"#{batch_number_2} [in_progress] {batch_title_2} (by @candidate)" in unassigned_filter,
        "unassigned filter missed second unassigned in-progress task",
    )
    require(title not in unassigned_filter, "unassigned filter leaked assigned task")

    mine_filter = run(cli, state_dir, "task", "list", "--channel", target, "--mine").stdout
    require(f"#{number} [done] {title} → @candidate" in mine_filter, "mine filter missed candidate task")
    require(batch_title_1 not in mine_filter and batch_title_2 not in mine_filter, "mine filter leaked unassigned tasks")

    assignee_filter = run(cli, state_dir, "task", "list", "--channel", target, "--assignee", "@candidate").stdout
    require(assignee_filter == mine_filter, "assignee @candidate filter did not match --mine")

    repeated_status_filter = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--status",
        "done",
        "--status",
        "in_progress",
    ).stdout
    require(f"## Task Board for {target} (3 tasks)" in repeated_status_filter, "repeated status filter did not union statuses")
    require(title in repeated_status_filter and batch_title_1 in repeated_status_filter and batch_title_2 in repeated_status_filter, "repeated status filter omitted a matching task")

    no_match_filter = run(cli, state_dir, "task", "list", "--channel", target, "--status", "todo", "--mine").stdout
    require("## Task Board" in no_match_filter and "No matching tasks." in no_match_filter, "filtered empty board did not distinguish no matches")

    invalid_filter = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--status",
        "blocked",
        expected=1,
    ).stderr
    require("--status must be one of" in invalid_filter, "invalid task list status did not fail closed")

    missing_status_value = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--status",
        expected=1,
    ).stderr
    require("--status must be one of" in missing_status_value, "missing task list status value did not fail closed")

    missing_assignee_value = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--assignee",
        expected=1,
    ).stderr
    require("--assignee requires a value" in missing_assignee_value, "missing task list assignee value did not fail closed")

    conflicting_filter = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--mine",
        "--unassigned",
        expected=1,
    ).stderr
    require("--mine cannot be combined with --unassigned" in conflicting_filter, "conflicting task list filters did not fail closed")

    conflicting_assignee_filter = run(
        cli,
        state_dir,
        "task",
        "list",
        "--channel",
        target,
        "--assignee",
        "@candidate",
        "--unassigned",
        expected=1,
    ).stderr
    require(
        "--assignee cannot be combined with --unassigned" in conflicting_assignee_filter,
        "assignee/unassigned task list filters did not fail closed",
    )

    conn = connect_state(state_dir)
    try:
        after_filter_rows = [
            (row["number"], row["status"], row["assignee"])
            for row in conn.execute(
                "SELECT number, status, assignee FROM tasks WHERE channel = ? ORDER BY number",
                (target,),
            ).fetchall()
        ]
        require(after_filter_rows == before_filter_rows, "task list filters mutated task rows")
        rows = conn.execute(
            """
            SELECT status, assignee, COUNT(*) AS count
            FROM tasks
            WHERE channel = ?
            GROUP BY status, assignee
            ORDER BY status, assignee
            """,
            (target,),
        ).fetchall()
        counts = {(row["status"], row["assignee"]): row["count"] for row in rows}
        require(counts.get(("done", "candidate")) == 1, "SQLite ground truth missing one done candidate task")
        require(counts.get(("in_progress", None)) == 2, "SQLite ground truth missing two unassigned in-progress tasks")
    finally:
        conn.close()

    regular_body = f"claim this regular message {uuid.uuid4()}"
    regular_id = parse_message_id(run(cli, state_dir, "message", "send", "--target", target, stdin=regular_body).stdout)
    conn = connect_state(state_dir)
    try:
        before_message_claim_messages = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE target = ?",
            (target,),
        ).fetchone()["count"]
        before_message_claim_tasks = conn.execute(
            "SELECT COUNT(*) AS count FROM tasks WHERE channel = ?",
            (target,),
        ).fetchone()["count"]
    finally:
        conn.close()

    claimed_from_message = run(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        target,
        "--message-id",
        regular_id[:8],
    ).stdout
    message_task_number = parse_task_number(claimed_from_message)
    require(
        f"Task #{message_task_number} claimed by @candidate." in claimed_from_message,
        "claiming a regular message did not acknowledge the created task",
    )
    converted_board = run(cli, state_dir, "task", "list", "--channel", target, "--mine").stdout
    require(
        f"#{message_task_number} [in_progress] {regular_body} → @candidate" in converted_board,
        "task converted from regular message did not appear as claimed in board",
    )
    conn = connect_state(state_dir)
    try:
        after_message_claim_messages = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE target = ?",
            (target,),
        ).fetchone()["count"]
        after_message_claim_tasks = conn.execute(
            "SELECT COUNT(*) AS count FROM tasks WHERE channel = ?",
            (target,),
        ).fetchone()["count"]
        converted = conn.execute(
            """
            SELECT number, title, status, creator, assignee, message_id
            FROM tasks
            WHERE channel = ? AND message_id = ?
            """,
            (target, regular_id),
        ).fetchone()
        require(
            after_message_claim_messages == before_message_claim_messages,
            "claiming a regular message appended an extra message instead of reusing the source message",
        )
        require(
            after_message_claim_tasks == before_message_claim_tasks + 1,
            "claiming a regular message did not create exactly one task row",
        )
        require(converted is not None, "converted task row missing from SQLite")
        require(converted["number"] == message_task_number, "converted task number did not match acknowledgement")
        require(converted["title"] == regular_body, "converted task title did not come from message body")
        require(converted["status"] == "in_progress", "converted task was not claimed into in_progress")
        require(converted["creator"] == "candidate", "converted task creator did not preserve message author")
        require(converted["assignee"] == "candidate", "converted task assignee missing candidate")
    finally:
        conn.close()

    duplicate_message_claim = run(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        target,
        "--message-id",
        regular_id,
    ).stdout
    require(
        f"Task #{message_task_number} claimed by @candidate." in duplicate_message_claim,
        "reclaiming converted message did not resolve existing task",
    )
    conn = connect_state(state_dir)
    try:
        duplicate_count = conn.execute(
            "SELECT COUNT(*) AS count FROM tasks WHERE channel = ? AND message_id = ?",
            (target, regular_id),
        ).fetchone()
        require(duplicate_count["count"] == 1, "reclaiming converted message duplicated the task row")
    finally:
        conn.close()

    thread_body = f"thread message should not become task {uuid.uuid4()}"
    thread_target = f"{target}:{regular_id[:8]}"
    thread_id = parse_message_id(run(cli, state_dir, "message", "send", "--target", thread_target, stdin=thread_body).stdout)
    conn = connect_state(state_dir)
    try:
        before_thread_claim_tasks = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE channel = ?", (target,)).fetchone()["count"]
    finally:
        conn.close()
    rejected_thread_claim = run(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        target,
        "--message-id",
        thread_id,
        expected=1,
    ).stderr
    require("Task not found" in rejected_thread_claim, "thread message claim did not fail closed")
    conn = connect_state(state_dir)
    try:
        after_thread_claim_tasks = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE channel = ?", (target,)).fetchone()["count"]
        thread_task = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE message_id = ?", (thread_id,)).fetchone()
        require(after_thread_claim_tasks == before_thread_claim_tasks, "thread message claim changed task count")
        require(thread_task["count"] == 0, "thread message claim created a task row")
    finally:
        conn.close()

    conflict_body = f"conflict conversion must stay atomic {uuid.uuid4()}"
    conflict_msg_id = parse_message_id(run(cli, state_dir, "message", "send", "--target", target, stdin=conflict_body).stdout)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                "UPDATE tasks SET assignee = 'alice' WHERE channel = ? AND number = ?",
                (target, batch_number_1),
            )
        before_conflict_tasks = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE channel = ?", (target,)).fetchone()["count"]
    finally:
        conn.close()
    rejected_conflict_claim = run(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        target,
        "--message-id",
        conflict_msg_id,
        "--number",
        str(batch_number_1),
        expected=1,
    ).stderr
    require("already claimed by @alice" in rejected_conflict_claim, "conflicting batch claim did not fail on existing assignee")
    conn = connect_state(state_dir)
    try:
        after_conflict_tasks = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE channel = ?", (target,)).fetchone()["count"]
        conflict_task = conn.execute("SELECT COUNT(*) AS count FROM tasks WHERE message_id = ?", (conflict_msg_id,)).fetchone()
        require(after_conflict_tasks == before_conflict_tasks, "conflicting batch claim converted a message before failing")
        require(conflict_task["count"] == 0, "conflicting batch claim left a converted task row")
    finally:
        conn.close()


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


def probe_daemon_reminder_fire(cli: Path, state_dir: Path) -> None:
    target = f"#daemon-{uuid.uuid4().hex[:8]}"
    title = f"daemon reminder {uuid.uuid4()}"
    scheduled = run(
        cli,
        state_dir,
        "reminder",
        "schedule",
        "--target",
        target,
        "--title",
        title,
        "--in",
        "0s",
    ).stdout
    reminder_id = parse_reminder_id(scheduled)

    first = run(cli, state_dir, "daemon", "run", "--once").stdout
    require("Daemon processed 1 reminder(s) in 1 iteration(s)." in first, "daemon did not fire due reminder")

    log = run(cli, state_dir, "reminder", "log", "--id", reminder_id).stdout
    require("fired:" in log, "daemon fire did not append reminder log event")
    require(f"{reminder_id} [fired]" in log, "fired reminder status missing from log")

    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require(f"Reminder {reminder_id}: {title}" in history, "daemon did not append reminder target message")
    require("type=system" in history and "@system" in history, "daemon reminder message was not a system message")

    inbox = run(cli, state_dir, "message", "check").stdout
    require(f"Reminder {reminder_id}: {title}" in inbox, "daemon reminder did not enqueue local inbox delivery")

    second = run(cli, state_dir, "daemon", "run", "--once").stdout
    require("Daemon processed 0 reminder(s) in 1 iteration(s)." in second, "daemon fired one-shot reminder twice")

    conn = connect_state(state_dir)
    try:
        message_count = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE body = ?",
            (f"Reminder {reminder_id}: {title}",),
        ).fetchone()
        fire_count = conn.execute(
            "SELECT COUNT(*) AS count FROM reminder_events WHERE reminder_id = ? AND event = 'fired'",
            (reminder_id,),
        ).fetchone()
        require(message_count is not None and message_count["count"] == 1, "daemon duplicated reminder message")
        require(fire_count is not None and fire_count["count"] == 1, "daemon duplicated fired event")
    finally:
        conn.close()

    recurring_title = f"recurring daemon reminder {uuid.uuid4()}"
    recurring = run(
        cli,
        state_dir,
        "reminder",
        "schedule",
        "--target",
        target,
        "--title",
        recurring_title,
        "--at",
        "2001-01-01T00:00:00",
        "--every",
        "1d",
    ).stdout
    recurring_id = parse_reminder_id(recurring)
    catch_up = run(cli, state_dir, "daemon", "run", "--once").stdout
    require("Daemon processed 1 reminder(s) in 1 iteration(s)." in catch_up, "daemon did not catch up overdue recurring reminder")
    recurring_list = run(cli, state_dir, "reminder", "list").stdout
    require(f"{recurring_id} [scheduled]" in recurring_list, "recurring reminder was not rescheduled")
    require("every=1d" in recurring_list, "recurring reminder lost recurrence")
    conn = connect_state(state_dir)
    try:
        row = conn.execute("SELECT next_fire_at FROM reminders WHERE id = ?", (recurring_id,)).fetchone()
        require(row is not None, "recurring reminder missing after daemon fire")
        next_fire = datetime.strptime(row["next_fire_at"], "%Y-%m-%d %H:%M:%S")
        require(next_fire > datetime.now(), "recurring reminder next fire did not advance past now")
        fire_count = conn.execute(
            "SELECT COUNT(*) AS count FROM reminder_events WHERE reminder_id = ? AND event = 'fired'",
            (recurring_id,),
        ).fetchone()
        require(fire_count is not None and fire_count["count"] == 1, "recurring daemon fire count mismatch")
    finally:
        conn.close()

    race_target = f"#daemon-race-{uuid.uuid4().hex[:8]}"
    race_titles = [f"daemon race reminder {index} {uuid.uuid4()}" for index in range(5)]
    for title_item in race_titles:
        run(
            cli,
            state_dir,
            "reminder",
            "schedule",
            "--target",
            race_target,
            "--title",
            title_item,
            "--in",
            "0s",
        )
    cli_target = f"#daemon-race-cli-{uuid.uuid4().hex[:8]}"
    cli_bodies = [f"daemon race cli body {index} {uuid.uuid4()}" for index in range(10)]

    def send_cli_body(body: str) -> None:
        sent = run(cli, state_dir, "message", "send", "--target", cli_target, stdin=body).stdout
        require(f"Message sent to {cli_target}." in sent, "concurrent CLI send hit freshness hold or failed")

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(send_cli_body, body) for body in cli_bodies]
        futures.append(executor.submit(run, cli, state_dir, "daemon", "run", "--once"))
        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, subprocess.CompletedProcess):
                require(
                    "Daemon processed 5 reminder(s) in 1 iteration(s)." in result.stdout,
                    "concurrent daemon run did not fire all race reminders",
                )

    conn = connect_state(state_dir)
    try:
        for body in cli_bodies:
            row = conn.execute("SELECT COUNT(*) AS count FROM messages WHERE target = ? AND body = ?", (cli_target, body)).fetchone()
            require(row is not None and row["count"] == 1, "concurrent CLI write lost or duplicated a message")
        for title_item in race_titles:
            reminder_body = conn.execute(
                "SELECT COUNT(*) AS count FROM messages WHERE target = ? AND body LIKE ?",
                (race_target, f"%{title_item}%"),
            ).fetchone()
            require(reminder_body is not None and reminder_body["count"] == 1, "concurrent daemon write lost or duplicated a reminder")
        cli_total = conn.execute("SELECT COUNT(*) AS count FROM messages WHERE target = ?", (cli_target,)).fetchone()
        reminder_total = conn.execute("SELECT COUNT(*) AS count FROM messages WHERE target = ?", (race_target,)).fetchone()
        require(cli_total is not None and cli_total["count"] == len(cli_bodies), "daemon/CLI concurrent CLI total mismatch")
        require(reminder_total is not None and reminder_total["count"] == len(race_titles), "daemon/CLI concurrent reminder total mismatch")
    finally:
        conn.close()


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

    avatar_url = f"pixel:random:{uuid.uuid4().hex[:8]}"
    avatar_updated = run(cli, state_dir, "profile", "update", "--avatar-url", avatar_url).stdout
    require(avatar_updated == "Profile updated.\n", "profile avatar-url update did not acknowledge write")
    shown_avatar_url = run(cli, state_dir, "profile", "show").stdout
    require(f"avatar_url: {avatar_url}" in shown_avatar_url, "profile show did not render avatar URL")

    avatar_payload = f"avatar bytes {uuid.uuid4()}".encode("utf-8")
    avatar_source = state_dir / "avatar.txt"
    avatar_source.write_bytes(avatar_payload)
    avatar_file_updated = run(
        cli,
        state_dir,
        "profile",
        "update",
        "--avatar-file",
        str(avatar_source),
    ).stdout
    require(avatar_file_updated == "Profile updated.\n", "profile avatar-file update did not acknowledge write")
    shown_avatar_file = run(cli, state_dir, "profile", "show").stdout
    require("avatar_file: profile-avatars/" in shown_avatar_file, "profile show did not render stored avatar file")
    require("avatar_url:" not in shown_avatar_file, "avatar-file update did not clear avatar URL")
    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            "SELECT display_name, description, avatar_url, avatar_file FROM profiles WHERE name = 'candidate'"
        ).fetchone()
        require(row is not None, "updated profile row missing from SQLite")
        require(row["display_name"] == display_name, "profile display name not persisted")
        require(row["description"] == description, "profile description not persisted")
        require(row["avatar_url"] is None, "avatar-file update did not clear persisted avatar URL")
        require(row["avatar_file"], "avatar file path not persisted")
        require((state_dir / row["avatar_file"]).read_bytes() == avatar_payload, "stored avatar bytes mismatch")
    finally:
        conn.close()
    missing_avatar = run(
        cli,
        state_dir,
        "profile",
        "update",
        "--avatar-file",
        str(state_dir / "missing-avatar.png"),
        expected=1,
    ).stderr
    require("Avatar file not found" in missing_avatar, "missing profile avatar file did not fail closed")

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


def probe_action_prepare(cli: Path, state_dir: Path) -> None:
    target = f"#actions-{uuid.uuid4().hex[:8]}"
    channel_name = f"#prepared-{uuid.uuid4().hex[:8]}"
    channel_payload = {
        "type": "channel:create",
        "name": channel_name,
        "description": f"prepared channel {uuid.uuid4()}",
        "private": False,
    }
    prepared = run(
        cli,
        state_dir,
        "action",
        "prepare",
        "--target",
        target,
        stdin=json.dumps(channel_payload),
    ).stdout
    action_id = parse_action_id(prepared)
    message_id = parse_message_id(prepared)
    require(f"Action prepared in {target}." in prepared, "action prepare did not acknowledge target")

    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require(f"Action prepared {action_id} [channel:create]" in history, "action card message missing from history")
    require(f"create public channel {channel_name}" in history, "action card summary missing channel target")
    require("pending human commit" in history, "action card message missing pending status")

    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            """
            SELECT target, variant, payload_json, status, creator, message_id
            FROM action_cards
            WHERE id = ?
            """,
            (action_id,),
        ).fetchone()
        require(row is not None, "action card was not persisted")
        require(row["target"] == target, "action card target mismatch")
        require(row["variant"] == "channel:create", "action card variant mismatch")
        require(row["status"] == "pending", "action card status mismatch")
        require(row["creator"] == "candidate", "action card creator mismatch")
        require(row["message_id"] == message_id, "action card message link mismatch")
        payload = json.loads(row["payload_json"])
        require(payload["type"] == "channel:create", "persisted action payload missing normalized type")
        require(payload["name"] == channel_name, "persisted channel action payload name mismatch")
        message = conn.execute("SELECT body FROM messages WHERE id = ?", (message_id,)).fetchone()
        require(message is not None and action_id in message["body"], "action card message body was not persisted")
        created_channel = conn.execute("SELECT name FROM channels WHERE name = ?", (channel_name,)).fetchone()
        require(created_channel is None, "prepared channel:create action executed instead of staying pending")
    finally:
        conn.close()

    agent_payload = {
        "variant": "agent:create",
        "name": "@artifact-bot",
        "description": "state-backed action card probe",
    }
    agent_prepared = run(
        cli,
        state_dir,
        "action",
        "prepare",
        "--target",
        target,
        stdin=json.dumps(agent_payload),
    ).stdout
    agent_action_id = parse_action_id(agent_prepared)
    agent_history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require(f"Action prepared {agent_action_id} [agent:create]" in agent_history, "agent action card missing from history")
    require("create agent @artifact-bot" in agent_history, "agent action summary missing normalized agent name")
    conn = connect_state(state_dir)
    try:
        row = conn.execute("SELECT payload_json FROM action_cards WHERE id = ?", (agent_action_id,)).fetchone()
        require(row is not None, "agent action card was not persisted")
        payload = json.loads(row["payload_json"])
        require(payload["type"] == "agent:create", "agent action payload missing normalized type")
        require(payload["name"] == "artifact-bot", "agent action payload did not strip @ prefix")
        created_agent = conn.execute("SELECT name FROM profiles WHERE name = 'artifact-bot'").fetchone()
        require(created_agent is None, "prepared agent:create action executed instead of staying pending")
    finally:
        conn.close()

    invalid_variant = run(
        cli,
        state_dir,
        "action",
        "prepare",
        "--target",
        target,
        stdin=json.dumps({"type": "workspace:delete", "name": "bad"}),
        expected=1,
    ).stderr
    require("action type must be channel:create or agent:create" in invalid_variant, "invalid action variant did not fail closed")
    invalid_json = run(
        cli,
        state_dir,
        "action",
        "prepare",
        "--target",
        target,
        stdin="{not json",
        expected=1,
    ).stderr
    require("invalid action JSON" in invalid_json, "invalid action JSON did not fail closed")
    conn = connect_state(state_dir)
    try:
        bad_rows = conn.execute(
            "SELECT COUNT(*) AS count FROM action_cards WHERE payload_json LIKE '%workspace:delete%'"
        ).fetchone()
        require(bad_rows is not None and bad_rows["count"] == 0, "invalid action prepare persisted a rejected card")
    finally:
        conn.close()


def probe_integration_local_login(cli: Path, state_dir: Path) -> None:
    initial = run(cli, state_dir, "integration", "list").stdout
    require("github" in initial, "integration list did not render registered services")
    require("no local record" in initial, "initial integration list did not show missing local login state")
    require("remote authentication" in initial, "integration list did not state local-only auth boundary")

    missing_env = run(
        cli,
        state_dir,
        "integration",
        "env",
        "--service",
        "github",
        expected=1,
    ).stderr
    require("Local integration record not found for github" in missing_env, "env before login did not fail closed")

    before_unknown_rows = None
    conn = connect_state(state_dir)
    try:
        before_unknown_rows = conn.execute("SELECT COUNT(*) AS count FROM integration_logins").fetchone()["count"]
    finally:
        conn.close()

    unknown = run(
        cli,
        state_dir,
        "integration",
        "login",
        "--service",
        "unknown-service",
        expected=1,
    ).stderr
    require("Unknown integration service" in unknown, "unknown integration login did not fail closed")
    conn = connect_state(state_dir)
    try:
        after_unknown_rows = conn.execute("SELECT COUNT(*) AS count FROM integration_logins").fetchone()["count"]
        require(after_unknown_rows == before_unknown_rows, "unknown integration login persisted state")
    finally:
        conn.close()

    missing_service = run(cli, state_dir, "integration", "login", expected=1).stderr
    require("--service is required" in missing_service, "missing integration service did not fail closed")

    account = f"acct-{uuid.uuid4().hex[:8]}"
    login = run(
        cli,
        state_dir,
        "integration",
        "login",
        "--service",
        "github",
        "--account",
        account,
    ).stdout
    require("Local integration record created for github" in login, "login did not create local integration record")
    require("Remote authentication was not performed" in login, "login output did not state no remote auth")
    require("login ready" not in login.lower(), "login output implied real auth readiness")
    require("logged in" not in login.lower(), "login output implied real auth happened")

    listed = run(cli, state_dir, "integration", "list").stdout
    require(f"github (third_party, local record present, account={account}" in listed, "list did not read login state")
    require("no local record" in listed, "list should still show unlogged services distinctly")

    env = run(cli, state_dir, "integration", "env", "--service", "github").stdout
    require("no remote credentials are provisioned" in env, "env output did not state local-only boundary")
    require("SWARM_INTEGRATION_SERVICE=github" in env, "env did not read service login state")
    require("SWARM_INTEGRATION_STATUS=local_placeholder" in env, "env did not read login status")
    require(f"SWARM_INTEGRATION_ACCOUNT={account}" in env, "env did not read login account")
    require("XDG_CONFIG_HOME=" in env and "XDG_DATA_HOME=" in env, "env missing isolated XDG paths")

    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            """
            SELECT service, status, account, local_home, created_at, updated_at
            FROM integration_logins
            WHERE service = 'github'
            """
        ).fetchone()
        require(row is not None, "integration login row was not persisted")
        require(row["status"] == "local_placeholder", "integration login status mismatch")
        require(row["account"] == account, "integration login account mismatch")
        require(Path(row["local_home"]).is_dir(), "integration local HOME directory was not created")
        require((Path(row["local_home"]).parent / "config").is_dir(), "integration config directory missing")
        first_created = row["created_at"]
    finally:
        conn.close()

    repeat = run(
        cli,
        state_dir,
        "integration",
        "login",
        "--service",
        "github",
        "--account",
        account,
    ).stdout
    require("already exists" in repeat, "repeat login was not idempotent")
    conn = connect_state(state_dir)
    try:
        rows = conn.execute(
            "SELECT COUNT(*) AS count, MIN(created_at) AS created FROM integration_logins WHERE service = 'github'"
        ).fetchone()
        require(rows["count"] == 1, "repeat login duplicated integration state")
        require(rows["created"] == first_created, "repeat login rewrote created_at")
    finally:
        conn.close()

    conflict = run(
        cli,
        state_dir,
        "integration",
        "login",
        "--service",
        "github",
        "--account",
        f"other-{uuid.uuid4().hex[:6]}",
        expected=1,
    ).stderr
    require("already exists for github" in conflict, "conflicting relogin did not fail closed")
    conn = connect_state(state_dir)
    try:
        rows = conn.execute("SELECT COUNT(*) AS count FROM integration_logins WHERE service = 'github'").fetchone()
        require(rows["count"] == 1, "conflicting relogin changed integration row count")
    finally:
        conn.close()


def probe_agent_registry(cli: Path, state_dir: Path) -> None:
    name = "curator"
    display_name = f"Curator {uuid.uuid4().hex[:6]}"
    avatar_url = f"https://example.com/avatar/{uuid.uuid4().hex}.png"

    def registered_agent_count() -> int:
        conn = connect_state(state_dir)
        try:
            row = conn.execute("SELECT COUNT(*) AS total FROM agents").fetchone()
            require(row is not None, "agent count query returned no row")
            return int(row["total"])
        finally:
            conn.close()

    registered = run(
        cli,
        state_dir,
        "agent",
        "register",
        "--name",
        name,
        "--display-name",
        display_name,
        "--runtime",
        "codex",
        "--workspace",
        f"agents/{name}",
        "--avatar-url",
        avatar_url,
        "--capability",
        "triage",
        "--capability",
        "write",
    ).stdout
    require(f"Agent @{name} registered." in registered, "agent register did not acknowledge agent")
    require(f"Display name: {display_name}" in registered, "agent register did not render display name")
    listed = run(cli, state_dir, "agent", "list").stdout
    require(f"@{name} ({display_name}) runtime=codex" in listed, "agent list did not render registered agent")
    require("capabilities=triage,write" in listed, "agent list did not render capabilities")
    list_unknown = run(cli, state_dir, "agent", "list", "--bogus", expected=1).stderr
    require("unknown agent list flag: --bogus" in list_unknown, "agent list did not reject unknown flags")
    server_info = run(cli, state_dir, "server", "info").stdout
    require(f"@{name} ({display_name})" in server_info, "server info did not include registered agent profile")

    count_after_register = registered_agent_count()
    register_bogus = run(
        cli,
        state_dir,
        "agent",
        "register",
        "--name",
        "bogus",
        "--display-name",
        "Bogus",
        "--runtime",
        "codex",
        "--totally-bogus-flag",
        "value",
        expected=1,
    ).stderr
    require(
        "unknown agent register flag: --totally-bogus-flag" in register_bogus,
        "agent register did not reject arbitrary unknown flag",
    )
    register_capabilities = run(
        cli,
        state_dir,
        "agent",
        "register",
        "--name",
        "typo-caps",
        "--display-name",
        "Typo Caps",
        "--runtime",
        "codex",
        "--capabilities",
        "triage",
        expected=1,
    ).stderr
    require(
        "unknown agent register flag: --capabilities" in register_capabilities,
        "agent register did not reject --capabilities typo",
    )
    register_avatar = run(
        cli,
        state_dir,
        "agent",
        "register",
        "--name",
        "typo-avatar",
        "--display-name",
        "Typo Avatar",
        "--runtime",
        "codex",
        "--avatar",
        avatar_url,
        expected=1,
    ).stderr
    require(
        "unknown agent register flag: --avatar" in register_avatar,
        "agent register did not reject --avatar typo",
    )
    require(registered_agent_count() == count_after_register, "unknown register flags mutated agent registry")

    seed_dir = state_dir / "agent-seed"
    seeded = run(cli, state_dir, "agent", "seed", "--name", name, "--output-dir", str(seed_dir)).stdout
    require("Seed workspace skeleton written" in seeded, "agent seed did not acknowledge skeleton write")
    seed_unknown = run(cli, state_dir, "agent", "seed", "--name", name, "--output-dir", str(seed_dir), "--bogus", expected=1).stderr
    require("unknown agent seed flag: --bogus" in seed_unknown, "agent seed did not reject unknown flags")
    required_files = ["seed.json", "MEMORY.md", "README.md", ".gitignore"]
    for filename in required_files:
        require((seed_dir / filename).exists(), f"agent seed missing {filename}")
    seed_payload = json.loads((seed_dir / "seed.json").read_text())
    require(seed_payload["format"] == "swarm-agent-redacted-seed", "agent seed format mismatch")
    require(seed_payload["status"] == "format_only_waiting_for_selected_sources", "agent seed was not format-only")
    require(seed_payload["source_policy"]["automatic_chat_dump"] is False, "agent seed allowed automatic chat dump")
    require(seed_payload["scrub_gates"]["internal_demo"] == ["credentials", "other_people_dm_originals"], "agent seed internal gate mismatch")
    require(
        seed_payload["scrub_gates"]["possibly_public"] == [
            "credentials",
            "other_people_dm_originals",
            "pii",
            "org_or_client_ip",
        ],
        "agent seed public gate mismatch",
    )
    seed_text = "\n".join((seed_dir / filename).read_text() for filename in required_files)
    secret_markers = [
        "xox" + "b-",
        "xox" + "p-",
        "sk" + "_agent_",
        "sk" + "_machine_",
        "raft" + "_secret_",
        "slock" + "_secret_",
        "credential.json contents",
    ]
    for marker in secret_markers:
        require(marker not in seed_text, f"agent seed contained blocked marker: {marker}")

    default_seeded = run(cli, state_dir, "agent", "seed", "--name", name).stdout
    require("Seed workspace skeleton written" in default_seeded, "agent default seed did not acknowledge skeleton write")
    worker_once = run(cli, state_dir, "agent", "worker", "--name", name, "--once", "--require-seed").stdout
    require(f"Worker heartbeat recorded for @{name}" in worker_once, "agent worker once did not record heartbeat")
    worker_unknown = run(cli, state_dir, "agent", "worker", "--name", name, "--bogus", expected=1).stderr
    require("unknown agent worker flag: --bogus" in worker_unknown, "agent worker did not reject unknown flags")

    heartbeat = run(
        cli,
        state_dir,
        "agent",
        "heartbeat",
        "--name",
        name,
        "--status",
        "alive",
        "--pid",
        "4242",
        "--session-id",
        "session-abc",
        "--detail",
        "ready",
    ).stdout
    require(f"Heartbeat recorded for @{name}: alive" in heartbeat, "agent heartbeat did not acknowledge update")
    heartbeat_unknown = run(cli, state_dir, "agent", "heartbeat", "--name", name, "--bogus", expected=1).stderr
    require("unknown agent heartbeat flag: --bogus" in heartbeat_unknown, "agent heartbeat did not reject unknown flags")
    supervisor = run(cli, state_dir, "agent", "supervisor-plan").stdout
    plans = [json.loads(line) for line in supervisor.splitlines() if line.startswith("{")]
    require(len(plans) == 1, "agent supervisor plan did not render one plan")
    require(plans[0]["agent"] == name and plans[0]["workspace"] == f"agents/{name}", "agent supervisor plan fields mismatch")
    require("worker_command" in plans[0] and "agent worker --name curator" in plans[0]["worker_command"], "agent supervisor plan missing worker command")
    require("No worker process was started" in supervisor, "agent supervisor plan did not state no-start boundary")
    supervisor_unknown = run(cli, state_dir, "agent", "supervisor-plan", "--bogus", expected=1).stderr
    require(
        "unknown agent supervisor-plan flag: --bogus" in supervisor_unknown,
        "agent supervisor-plan did not reject unknown flags",
    )

    target = f"#agent-author-{uuid.uuid4().hex[:8]}"
    body = f"registered author body {uuid.uuid4()}"
    sent = run(cli, state_dir, "message", "send", "--target", target, "--author", name, stdin=body).stdout
    parse_message_id(sent)
    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require(f"@{name}: {body}" in history, "message send --author did not persist registered author")
    rejected_author = run(
        cli,
        state_dir,
        "message",
        "send",
        "--target",
        target,
        "--author",
        "missing-agent",
        stdin="should fail",
        expected=1,
    ).stderr
    require("Author not found" in rejected_author, "message send with missing author did not fail closed")

    for other_name, other_display in (("worker", "Worker"), ("verifier", "Verifier")):
        run(
            cli,
            state_dir,
            "agent",
            "register",
            "--name",
            other_name,
            "--display-name",
            other_display,
            "--runtime",
            "codex",
            "--workspace",
            f"agents/{other_name}",
        )
        run(cli, state_dir, "agent", "seed", "--name", other_name)
    collab = run(
        cli,
        state_dir,
        "agent",
        "collab-smoke",
        "--channel",
        "#agent-collab",
        "--task-author",
        name,
        "--worker",
        "worker",
        "--verifier",
        "verifier",
        "--title",
        "three-agent smoke",
    ).stdout
    require("## Agent Collab Smoke" in collab, "agent collab-smoke did not render header")
    collab_payload = next(json.loads(line) for line in collab.splitlines() if line.startswith("{"))
    require(collab_payload["task_author"] == name, "collab smoke task author mismatch")
    require(collab_payload["worker"] == "worker", "collab smoke worker mismatch")
    require(collab_payload["verifier"] == "verifier", "collab smoke verifier mismatch")
    require(collab_payload["status"] == "in_review", "collab smoke did not move task to in_review")
    collab_unknown = run(cli, state_dir, "agent", "collab-smoke", "--bogus", expected=1).stderr
    require("unknown agent collab-smoke flag: --bogus" in collab_unknown, "agent collab-smoke did not reject unknown flags")
    agent_claim_title = f"agent claim task {uuid.uuid4()}"
    claim_created = run(cli, state_dir, "task", "create", "--channel", "#agent-collab", "--title", agent_claim_title).stdout
    claim_number = parse_task_number(claim_created)
    agent_claim = run(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        "#agent-collab",
        "--number",
        str(claim_number),
        "--assignee",
        "worker",
    ).stdout
    require(f"Task #{claim_number} claimed by @worker." in agent_claim, "task claim did not accept registered agent assignee")

    conn = connect_state(state_dir)
    try:
        agent_row = conn.execute(
            "SELECT display_name, runtime, workspace_path, capabilities_json, avatar_url FROM agents WHERE name = ?",
            (name,),
        ).fetchone()
        require(agent_row is not None, "agent registry row missing from SQLite")
        require(agent_row["display_name"] == display_name, "agent registry display name mismatch")
        require(agent_row["runtime"] == "codex", "agent registry runtime mismatch")
        require(agent_row["workspace_path"] == f"agents/{name}", "agent registry workspace mismatch")
        require(json.loads(agent_row["capabilities_json"]) == ["triage", "write"], "agent capabilities JSON mismatch")
        require(agent_row["avatar_url"] == avatar_url, "agent avatar URL mismatch")
        heartbeat_row = conn.execute(
            "SELECT status, pid, session_id, detail FROM agent_heartbeats WHERE name = ?",
            (name,),
        ).fetchone()
        require(heartbeat_row is not None, "agent heartbeat row missing from SQLite")
        require(
            heartbeat_row["status"] == "alive"
            and str(heartbeat_row["session_id"]).startswith("collab-smoke-"),
            "agent heartbeat fields mismatch",
        )
        worker_task_row = conn.execute(
            "SELECT assignee, status FROM tasks WHERE number = ?",
            (claim_number,),
        ).fetchone()
        require(worker_task_row is not None, "registered-agent claim task row missing")
        require(worker_task_row["assignee"] == "worker", "registered-agent task assignee mismatch")
        profile_row = conn.execute("SELECT kind, avatar_url FROM profiles WHERE name = ?", (name,)).fetchone()
        require(profile_row is not None and profile_row["kind"] == "agent", "registered agent profile missing")
        require(profile_row["avatar_url"] == avatar_url, "registered agent profile avatar mismatch")
    finally:
        conn.close()


def probe_slack_adapter(cli: Path, state_dir: Path) -> None:
    workspace = f"T{uuid.uuid4().hex[:8].upper()}"
    channel_id = f"C{uuid.uuid4().hex[:8].upper()}"
    channel_name = f"probe-{uuid.uuid4().hex[:6]}"
    target = f"#slack-{channel_id.lower()}"
    bot_token_env = f"SWARM_SLACK_BOT_{uuid.uuid4().hex[:8].upper()}"
    signing_env = f"SWARM_SLACK_SIGNING_{uuid.uuid4().hex[:8].upper()}"

    def slack_ts(seconds: int) -> str:
        return f"{seconds}.{uuid.uuid4().int % 1_000_000:06d}"

    def outbound_plans(output: str) -> list[dict[str, object]]:
        return [json.loads(line) for line in output.splitlines() if line.startswith("{")]

    root_ts = slack_ts(1762000000)
    reply_ts = slack_ts(1762000010)
    root_body = f"slack adapter root {uuid.uuid4()}"
    reply_body = f"slack adapter reply {uuid.uuid4()}"

    configured = run(
        cli,
        state_dir,
        "slack",
        "configure",
        "--workspace",
        workspace,
        "--bot-token-env",
        bot_token_env,
        "--signing-secret-env",
        signing_env,
    ).stdout
    require("Slack workspace configured." in configured, "slack configure did not acknowledge workspace")
    require("No Slack secret values were stored." in configured, "slack configure did not state secret boundary")
    env = run(cli, state_dir, "slack", "env", "--workspace", workspace).stdout
    require(bot_token_env in env and signing_env in env, "slack env did not render configured env names")
    require("secret values are never stored" in env, "slack env did not state secret boundary")
    invalid_config = run(
        cli,
        state_dir,
        "slack",
        "configure",
        "--workspace",
        f"T{uuid.uuid4().hex[:6].upper()}",
        "--bot-token-env",
        "1BAD_ENV",
        expected=1,
    ).stderr
    require("valid environment variable name" in invalid_config, "invalid Slack env var did not fail closed")

    cli_module = load_cli_module(cli)
    captured_requests: list[object] = []

    class FakeSlackResponse:
        def __enter__(self) -> "FakeSlackResponse":
            return self

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"ok":true,"messages":[],"has_more":false}'

    def fake_urlopen(request: object, timeout: float) -> FakeSlackResponse:
        captured_requests.append(request)
        return FakeSlackResponse()

    original_urlopen = cli_module.urllib.request.urlopen
    try:
        cli_module.urllib.request.urlopen = fake_urlopen
        response, response_error = cli_module.slack_web_api_call(
            "test-token",
            "conversations.replies",
            {"channel": "CEXPORT", "ts": "1761999900.000001", "limit": 200},
            3.0,
        )
    finally:
        cli_module.urllib.request.urlopen = original_urlopen
    require(response_error is None and response is not None, "slack history transport probe failed")
    require(len(captured_requests) == 1, "slack history transport did not issue one request")
    request = captured_requests[0]
    require(request.get_method() == "POST", "slack history transport did not use POST")
    content_type = request.get_header("Content-type") or request.get_header("Content-Type")
    require(
        content_type == "application/x-www-form-urlencoded; charset=utf-8",
        f"slack history transport used wrong content type: {content_type}",
    )
    encoded = request.data.decode("utf-8")
    parsed = parse_qs(encoded)
    require(parsed.get("channel") == ["CEXPORT"], "slack history transport did not encode channel")
    require(parsed.get("ts") == ["1761999900.000001"], "slack history transport did not encode ts")
    require(not encoded.startswith("{"), "slack history transport still used JSON body")

    export_channel_id = f"C{uuid.uuid4().hex[:8].upper()}"
    export_channel_name = f"export-{uuid.uuid4().hex[:6]}"
    export_target = f"#slack-{export_channel_id.lower()}"
    export_root_ts = slack_ts(1761999900)
    export_reply_ts = slack_ts(1761999910)
    export_join_ts = slack_ts(1761999890)
    export_root_body = f"exported slack root {uuid.uuid4()}"
    export_reply_body = f"exported slack reply {uuid.uuid4()}"
    mock_api_path = state_dir / "slack-export-mock.json"
    mock_api_path.write_text(
        json.dumps(
            {
                "histories": {
                    export_channel_id: {
                        "ok": True,
                        "messages": [
                            {
                                "type": "message",
                                "subtype": "channel_join",
                                "user": "UEXPORTJOIN",
                                "text": "joined the channel",
                                "ts": export_join_ts,
                            },
                            {
                                "type": "message",
                                "user": "UEXPORTROOT",
                                "text": export_root_body,
                                "ts": export_root_ts,
                                "reply_count": 1,
                            }
                        ],
                        "has_more": False,
                    }
                },
                "replies": {
                    f"{export_channel_id}:{export_root_ts}": {
                        "ok": True,
                        "messages": [
                            {
                                "type": "message",
                                "user": "UEXPORTROOT",
                                "text": export_root_body,
                                "ts": export_root_ts,
                            },
                            {
                                "type": "message",
                                "user": "UEXPORTREPLY",
                                "text": export_reply_body,
                                "ts": export_reply_ts,
                                "thread_ts": export_root_ts,
                            },
                        ],
                        "has_more": False,
                    }
                },
            }
        )
    )
    conn = connect_state(state_dir)
    try:
        before_export_rows = conn.execute("SELECT COUNT(*) AS count FROM slack_messages").fetchone()["count"]
    finally:
        conn.close()
    exported_proc = run(
        cli,
        state_dir,
        "slack",
        "export-history",
        "--workspace",
        workspace,
        "--channel-id",
        export_channel_id,
        "--channel-name",
        export_channel_name,
        "--include-replies",
        "--mock-api-file",
        str(mock_api_path),
    )
    require("Read-only export" in exported_proc.stderr, "slack export did not report read-only boundary")
    require("Mock Slack API responses used" in exported_proc.stderr, "slack export did not report mock boundary")
    require("Skipped 1 history rows" in exported_proc.stderr, "slack export did not skip unsupported history rows")
    exported_lines = [line for line in exported_proc.stdout.splitlines() if line.strip()]
    require(len(exported_lines) == 2, f"slack export rendered wrong row count:\n{exported_proc.stdout}")
    require(all(line.startswith("{") for line in exported_lines), "slack export stdout was not pure event JSON rows")
    exported_events = [json.loads(line) for line in exported_lines]
    require(
        [event["event"]["ts"] for event in exported_events] == [export_root_ts, export_reply_ts],
        "slack export did not emit root before reply",
    )
    require(exported_events[0]["team_id"] == workspace, "slack export did not preserve workspace")
    require(exported_events[0]["event"]["channel_name"] == export_channel_name, "slack export did not preserve channel name")
    conn = connect_state(state_dir)
    try:
        after_export_rows = conn.execute("SELECT COUNT(*) AS count FROM slack_messages").fetchone()["count"]
    finally:
        conn.close()
    require(before_export_rows == after_export_rows, "slack export mutated SQLite state")

    export_root_import = run(cli, state_dir, "slack", "ingest", stdin=exported_lines[0]).stdout
    export_root_id = parse_message_id(export_root_import)
    require(export_root_body in export_root_import, "exported root did not ingest through existing path")
    export_reply_import = run(cli, state_dir, "slack", "ingest", stdin=exported_lines[1]).stdout
    export_reply_id = parse_message_id(export_reply_import)
    require(f"Thread ts: {export_root_ts}" in export_reply_import, "exported reply did not map to Slack thread")
    export_thread_target = f"{export_target}:{export_root_id[:8]}"
    require(f"Target: {export_thread_target}" in export_reply_import, "exported reply target mismatch")
    export_thread_history = run(cli, state_dir, "message", "read", "--channel", export_thread_target).stdout
    require(export_root_body in export_thread_history and export_reply_body in export_thread_history, "exported thread not readable after ingest")
    export_resolved = run(
        cli,
        state_dir,
        "slack",
        "resolve",
        "--workspace",
        workspace,
        "--channel-id",
        export_channel_id,
        "--ts",
        export_reply_ts,
    ).stdout
    require(export_reply_id in export_resolved, "exported reply mapping did not resolve")
    export_inbox = run(cli, state_dir, "message", "check").stdout
    require(export_root_body in export_inbox and export_reply_body in export_inbox, "exported ingest did not enqueue inbox delivery")

    root_event = {
        "team_id": workspace,
        "event": {
            "type": "message",
            "channel": channel_id,
            "channel_name": channel_name,
            "user": "U123ROOT",
            "text": root_body,
            "ts": root_ts,
        },
    }
    imported = run(cli, state_dir, "slack", "ingest", stdin=json.dumps(root_event)).stdout
    root_id = parse_message_id(imported)
    require("Slack event imported." in imported, "slack root ingest did not acknowledge import")
    require(f"Workspace: {workspace}" in imported, "slack root ingest missing workspace")
    require(f"Channel: {channel_id} ({channel_name}) -> {target}" in imported, "slack root ingest did not map channel")
    require(f"Target: {target}" in imported, "slack root ingest missing swarm target")
    require(root_body in imported, "slack root ingest missing message body")
    require("Source of truth: swarm SQLite state" in imported, "slack ingest did not state source-of-truth boundary")

    inbox = run(cli, state_dir, "message", "check").stdout
    require(root_body in inbox, "slack root ingest did not enqueue inbox delivery")
    require(f"[target={target} msg={root_id[:8]}" in inbox, "slack root inbox row missing mapped target/id")

    duplicate = run(cli, state_dir, "slack", "ingest", stdin=json.dumps(root_event)).stdout
    duplicate_id = parse_message_id(duplicate)
    require("Slack event already imported." in duplicate, "duplicate slack ingest was not idempotent")
    require(duplicate_id == root_id, "duplicate slack ingest returned a different swarm message id")
    drained = run(cli, state_dir, "message", "check").stdout
    require(drained == "No new messages.\n", "duplicate slack ingest enqueued another inbox delivery")

    resolved = run(
        cli,
        state_dir,
        "slack",
        "resolve",
        "--workspace",
        workspace,
        "--channel-id",
        channel_id,
        "--ts",
        root_ts,
    ).stdout
    require("Slack message mapping." in resolved, "slack resolve missing mapping heading")
    require(root_id in resolved and f"Target: {target}" in resolved, "slack resolve did not return persisted root mapping")
    require(root_body in resolved, "slack resolve did not read canonical swarm message body")

    history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require(root_body in history, "slack root message was not visible in swarm channel history")
    require(history.count(root_body) == 1, "duplicate slack ingest created multiple root messages")
    require(f"@slack:U123ROOT" in history, "slack user was not preserved as message author")

    server_info = run(cli, state_dir, "server", "info").stdout
    require(f"{target} (public, joined) - Slack channel {channel_id} ({channel_name})" in server_info, "slack ingest did not catalog mapped channel")

    reply_event = {
        "team_id": workspace,
        "event": {
            "type": "message",
            "channel": channel_id,
            "channel_name": channel_name,
            "user": "U456REPLY",
            "text": reply_body,
            "ts": reply_ts,
            "thread_ts": root_ts,
        },
    }
    reply_imported = run(cli, state_dir, "slack", "ingest", stdin=json.dumps(reply_event)).stdout
    reply_id = parse_message_id(reply_imported)
    thread_target = f"{target}:{root_id[:8]}"
    require("Slack event imported." in reply_imported, "slack thread reply ingest did not acknowledge import")
    require(f"Thread ts: {root_ts}" in reply_imported, "slack thread reply output missing thread root ts")
    require(f"Target: {thread_target}" in reply_imported, "slack reply did not map to swarm thread target")

    thread_history = run(cli, state_dir, "message", "read", "--channel", thread_target).stdout
    require(root_body in thread_history and reply_body in thread_history, "slack thread history did not include root and reply")
    parent_history = run(cli, state_dir, "message", "read", "--channel", target).stdout
    require(root_body in parent_history, "slack parent history lost root message")
    require(reply_body not in parent_history, "slack thread reply leaked into parent channel history")

    reply_inbox = run(cli, state_dir, "message", "check").stdout
    require(reply_body in reply_inbox, "slack reply ingest did not enqueue inbox delivery")
    require(f"[target={thread_target} msg={reply_id[:8]}" in reply_inbox, "slack reply inbox row missing thread target/id")

    custom_agent_name = f"curator-{uuid.uuid4().hex[:6]}"
    custom_display_name = f"Curator {uuid.uuid4().hex[:6]}"
    custom_avatar_url = f"https://example.com/slack-avatar/{uuid.uuid4().hex}.png"
    run(
        cli,
        state_dir,
        "agent",
        "register",
        "--name",
        custom_agent_name,
        "--display-name",
        custom_display_name,
        "--runtime",
        "codex",
        "--avatar-url",
        custom_avatar_url,
        "--capability",
        "slack-render",
    )
    custom_body = f"slack customized author {uuid.uuid4()}"
    custom_attempt = run(
        cli,
        state_dir,
        "message",
        "send",
        "--target",
        target,
        "--author",
        custom_agent_name,
        stdin=custom_body,
    ).stdout
    if "Freshness hold:" in custom_attempt:
        custom_sent = run(
            cli,
            state_dir,
            "message",
            "send",
            "--send-draft",
            "--target",
            target,
            "--author",
            custom_agent_name,
        ).stdout
    else:
        custom_sent = custom_attempt
    custom_id = parse_message_id(custom_sent)
    rendered_custom = run(
        cli,
        state_dir,
        "slack",
        "outbound",
        "--workspace",
        workspace,
        "--message-id",
        custom_id,
    ).stdout
    custom_plans = outbound_plans(rendered_custom)
    require(len(custom_plans) == 1, "slack customized author plan count mismatch")
    custom_plan = custom_plans[0]
    require(custom_plan["swarm_author"] == custom_agent_name, "slack customized plan missing swarm author")
    require(custom_plan["username"] == custom_display_name, "slack customized plan missing username")
    require(custom_plan["icon_url"] == custom_avatar_url, "slack customized plan missing icon_url")
    sendable = cli_module.slack_sendable_payload(custom_plan)
    require(sendable["username"] == custom_display_name, "slack sendable payload missing username")
    require(sendable["icon_url"] == custom_avatar_url, "slack sendable payload missing icon_url")
    custom_sent_ts = slack_ts(1762000035)
    run(
        cli,
        state_dir,
        "slack",
        "mark-sent",
        "--workspace",
        workspace,
        "--message-id",
        custom_id,
        "--ts",
        custom_sent_ts,
    )

    top_outbound_body = f"slack outbound top {uuid.uuid4()}"
    top_hold = run(cli, state_dir, "message", "send", "--target", target, stdin=top_outbound_body).stdout
    if "Freshness hold:" in top_hold:
        top_sent = run(cli, state_dir, "message", "send", "--send-draft", "--target", target).stdout
    else:
        top_sent = top_hold
    top_outbound_id = parse_message_id(top_sent)
    rendered_top = run(cli, state_dir, "slack", "outbound", "--workspace", workspace, "--target", target).stdout
    top_plans = outbound_plans(rendered_top)
    require(len(top_plans) == 1, f"slack outbound top rendered wrong plan count:\n{rendered_top}")
    top_plan = top_plans[0]
    require(top_plan["method"] == "chat.postMessage", "slack outbound top method mismatch")
    require(top_plan["channel"] == channel_id, "slack outbound top channel mismatch")
    require(top_plan["text"] == top_outbound_body, "slack outbound top text mismatch")
    require(top_plan["client_msg_id"] == top_outbound_id, "slack outbound top client_msg_id mismatch")
    require("thread_ts" not in top_plan, "top-level outbound plan unexpectedly included thread_ts")
    require("No network request was sent" in rendered_top, "slack outbound did not state no-network boundary")

    top_sent_ts = slack_ts(1762000040)
    top_marked = run(
        cli,
        state_dir,
        "slack",
        "mark-sent",
        "--workspace",
        workspace,
        "--message-id",
        top_outbound_id[:8],
        "--ts",
        top_sent_ts,
    ).stdout
    require("Slack sent mapping recorded." in top_marked, "slack mark-sent did not acknowledge top mapping")
    resolved_outbound = run(
        cli,
        state_dir,
        "slack",
        "resolve",
        "--workspace",
        workspace,
        "--channel-id",
        channel_id,
        "--ts",
        top_sent_ts,
    ).stdout
    require(top_outbound_id in resolved_outbound, "slack resolve did not find marked sent top message")
    rendered_after_mark = run(cli, state_dir, "slack", "outbound", "--workspace", workspace, "--target", target).stdout
    require("## Slack Outbound Plan (0 requests)" in rendered_after_mark, "mark-sent top message was still rendered outbound")
    repeated_mark = run(
        cli,
        state_dir,
        "slack",
        "mark-sent",
        "--workspace",
        workspace,
        "--message-id",
        top_outbound_id,
        "--ts",
        top_sent_ts,
    ).stdout
    require("already recorded" in repeated_mark, "slack mark-sent repeat was not idempotent")

    thread_outbound_body = f"slack outbound thread {uuid.uuid4()}"
    thread_hold = run(cli, state_dir, "message", "send", "--target", thread_target, stdin=thread_outbound_body).stdout
    require("Freshness hold:" in thread_hold, "slack outbound thread setup should first hit freshness hold")
    thread_sent = run(cli, state_dir, "message", "send", "--send-draft", "--target", thread_target).stdout
    thread_outbound_id = parse_message_id(thread_sent)
    rendered_thread = run(
        cli,
        state_dir,
        "slack",
        "outbound",
        "--workspace",
        workspace,
        "--message-id",
        thread_outbound_id,
    ).stdout
    thread_plans = outbound_plans(rendered_thread)
    require(len(thread_plans) == 1, f"slack outbound thread rendered wrong plan count:\n{rendered_thread}")
    thread_plan = thread_plans[0]
    require(thread_plan["channel"] == channel_id, "slack outbound thread channel mismatch")
    require(thread_plan["thread_ts"] == root_ts, "slack outbound thread did not use root Slack ts")
    require(thread_plan["text"] == thread_outbound_body, "slack outbound thread text mismatch")
    thread_sent_ts = slack_ts(1762000050)
    thread_marked = run(
        cli,
        state_dir,
        "slack",
        "mark-sent",
        "--workspace",
        workspace,
        "--message-id",
        thread_outbound_id,
        "--ts",
        thread_sent_ts,
    ).stdout
    require(f"Thread ts: {root_ts}" in thread_marked, "slack mark-sent thread did not persist root thread ts")

    send_body = f"slack real-send seam {uuid.uuid4()}"
    send_attempt = run(cli, state_dir, "message", "send", "--target", target, stdin=send_body).stdout
    if "Freshness hold:" in send_attempt:
        send_result = run(cli, state_dir, "message", "send", "--send-draft", "--target", target).stdout
    else:
        send_result = send_attempt
    send_message_id = parse_message_id(send_result)
    missing_token = run(
        cli,
        state_dir,
        "slack",
        "send",
        "--workspace",
        workspace,
        "--message-id",
        send_message_id,
        expected=1,
    ).stderr
    require("SLACK_TOKEN_MISSING" in missing_token, "slack send without token did not fail closed")
    require(bot_token_env in missing_token, "slack send missing-token error did not name env var")

    send_ts = slack_ts(1762000060)
    mock_ok_path = state_dir / "slack-send-ok.json"
    mock_ok_path.write_text(json.dumps({"ok": True, "channel": channel_id, "ts": send_ts}))
    fake_token = f"dummy-slack-token-{uuid.uuid4()}"
    sent = run(
        cli,
        state_dir,
        "slack",
        "send",
        "--workspace",
        workspace,
        "--message-id",
        send_message_id,
        "--mock-response-file",
        str(mock_ok_path),
        env_overrides={bot_token_env: fake_token},
    ).stdout
    require("## Slack Send (1 requests)" in sent, "slack send did not acknowledge one mocked send")
    require("Mock Slack responses used; no network request was sent." in sent, "slack send did not report mock boundary")
    require(f"Token source: {bot_token_env} (value not shown)" in sent, "slack send did not report token env name")
    require(fake_token not in sent, "slack send leaked token value in stdout")
    require(send_ts in sent and send_message_id in sent, "slack send did not report mapped message and ts")
    after_send = run(
        cli,
        state_dir,
        "slack",
        "outbound",
        "--workspace",
        workspace,
        "--message-id",
        send_message_id,
    ).stdout
    require("## Slack Outbound Plan (0 requests)" in after_send, "slack send did not record mark-sent ledger")
    send_resolved = run(
        cli,
        state_dir,
        "slack",
        "resolve",
        "--workspace",
        workspace,
        "--channel-id",
        channel_id,
        "--ts",
        send_ts,
    ).stdout
    require(send_message_id in send_resolved, "slack send mapping did not resolve by Slack ts")

    failed_send_body = f"slack failed send seam {uuid.uuid4()}"
    failed_attempt = run(cli, state_dir, "message", "send", "--target", target, stdin=failed_send_body).stdout
    if "Freshness hold:" in failed_attempt:
        failed_result = run(cli, state_dir, "message", "send", "--send-draft", "--target", target).stdout
    else:
        failed_result = failed_attempt
    failed_send_id = parse_message_id(failed_result)
    mock_fail_path = state_dir / "slack-send-fail.json"
    mock_fail_path.write_text(json.dumps({"ok": False, "error": "ratelimited"}))
    failed_send = run(
        cli,
        state_dir,
        "slack",
        "send",
        "--workspace",
        workspace,
        "--message-id",
        failed_send_id,
        "--mock-response-file",
        str(mock_fail_path),
        expected=1,
        env_overrides={bot_token_env: fake_token},
    ).stderr
    require("SLACK_SEND_FAILED" in failed_send and "ratelimited" in failed_send, "slack send failure did not surface Slack error")
    require(fake_token not in failed_send, "slack send leaked token value in stderr")
    failed_after = run(
        cli,
        state_dir,
        "slack",
        "outbound",
        "--workspace",
        workspace,
        "--message-id",
        failed_send_id,
    ).stdout
    require(len(outbound_plans(failed_after)) == 1, "failed slack send polluted mark-sent ledger")

    unmapped_target = f"#slack-unmapped-{uuid.uuid4().hex[:8]}"
    unmapped_body = f"unmapped slack outbound {uuid.uuid4()}"
    run(cli, state_dir, "channel", "join", unmapped_target)
    run(cli, state_dir, "message", "send", "--target", unmapped_target, stdin=unmapped_body)
    unmapped = run(
        cli,
        state_dir,
        "slack",
        "outbound",
        "--workspace",
        workspace,
        "--target",
        unmapped_target,
        expected=1,
    ).stderr
    require("Slack channel mapping not found" in unmapped, "slack outbound without channel mapping did not fail closed")

    missing_root_body = f"missing root should not persist {uuid.uuid4()}"
    missing_root_event = {
        "team_id": workspace,
        "event": {
            "type": "message",
            "channel": channel_id,
            "user": "U789MISS",
            "text": missing_root_body,
            "ts": slack_ts(1762000020),
            "thread_ts": slack_ts(1761999999),
        },
    }
    rejected_missing_root = run(
        cli,
        state_dir,
        "slack",
        "ingest",
        stdin=json.dumps(missing_root_event),
        expected=1,
    ).stderr
    require("Slack thread root has not been imported" in rejected_missing_root, "slack missing thread root did not fail closed")

    invalid_event = {
        "team_id": workspace,
        "event": {
            "type": "reaction_added",
            "channel": channel_id,
            "user": "UINVALID",
            "ts": slack_ts(1762000030),
        },
    }
    invalid = run(cli, state_dir, "slack", "ingest", stdin=json.dumps(invalid_event), expected=1).stderr
    require("only Slack message events are supported" in invalid, "unsupported Slack event did not fail closed")

    conn = connect_state(state_dir)
    try:
        root_rows = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE id = ? AND target = ? AND body = ?",
            (root_id, target, root_body),
        ).fetchone()
        require(root_rows is not None and root_rows["count"] == 1, "slack root message was not persisted exactly once")
        reply_rows = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE id = ? AND target = ? AND body = ?",
            (reply_id, thread_target, reply_body),
        ).fetchone()
        require(reply_rows is not None and reply_rows["count"] == 1, "slack reply message was not persisted exactly once")
        mappings = conn.execute(
            """
            SELECT slack_ts, thread_ts, target, message_id
            FROM slack_messages
            WHERE workspace = ? AND channel_id = ? AND message_id IN (?, ?)
            ORDER BY slack_ts
            """,
            (workspace, channel_id, root_id, reply_id),
        ).fetchall()
        require(len(mappings) == 2, "slack mapping table did not persist root and reply only")
        require(
            any(row["slack_ts"] == root_ts and row["message_id"] == root_id for row in mappings),
            "slack root mapping mismatch",
        )
        require(
            any(row["thread_ts"] == root_ts and row["message_id"] == reply_id for row in mappings),
            "slack reply mapping mismatch",
        )
        outbound_mappings = conn.execute(
            """
            SELECT slack_ts, thread_ts, message_id
            FROM slack_messages
            WHERE workspace = ? AND channel_id = ? AND message_id IN (?, ?)
            ORDER BY message_id
            """,
            (workspace, channel_id, top_outbound_id, thread_outbound_id),
        ).fetchall()
        require(len(outbound_mappings) == 2, "slack mark-sent did not persist two outbound mappings")
        require(
            any(row["slack_ts"] == top_sent_ts and row["message_id"] == top_outbound_id for row in outbound_mappings),
            "slack top outbound mapping mismatch",
        )
        require(
            any(row["thread_ts"] == root_ts and row["message_id"] == thread_outbound_id for row in outbound_mappings),
            "slack thread outbound mapping mismatch",
        )
        send_mapping = conn.execute(
            """
            SELECT slack_ts, message_id
            FROM slack_messages
            WHERE workspace = ? AND channel_id = ? AND message_id = ?
            """,
            (workspace, channel_id, send_message_id),
        ).fetchone()
        require(send_mapping is not None and send_mapping["slack_ts"] == send_ts, "slack send did not persist successful mapping")
        failed_mapping = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM slack_messages
            WHERE workspace = ? AND message_id = ?
            """,
            (workspace, failed_send_id),
        ).fetchone()
        require(failed_mapping is not None and failed_mapping["count"] == 0, "failed slack send persisted a mapping")
        config = conn.execute(
            """
            SELECT bot_token_env, signing_secret_env
            FROM slack_workspaces
            WHERE workspace = ?
            """,
            (workspace,),
        ).fetchone()
        require(config is not None, "slack workspace config was not persisted")
        require(config["bot_token_env"] == bot_token_env, "slack workspace bot env mismatch")
        require(config["signing_secret_env"] == signing_env, "slack workspace signing env mismatch")
        missing_rows = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE body = ?",
            (missing_root_body,),
        ).fetchone()
        require(missing_rows is not None and missing_rows["count"] == 0, "failed slack ingest persisted a missing-root message")
    finally:
        conn.close()


def main() -> int:
    cli = Path(os.environ.get("SWARM_CLI", DEFAULT_CLI)).resolve()
    require(cli.exists(), f"SWARM_CLI does not exist: {cli}")
    probe_fresh_store_empty(cli)

    with tempfile.TemporaryDirectory(prefix="swarm-anti-stub-") as tmp:
        state_dir = Path(tmp)
        probe_inbox(cli, state_dir)
        probe_send_read_and_routes(cli, state_dir)
        probe_read_pagination(cli, state_dir)
        probe_read_known_empty_surfaces(cli, state_dir)
        probe_search_and_resolve(cli, state_dir)
        probe_message_reactions(cli, state_dir)
        probe_freshness_cursor(cli, state_dir)
        probe_wall_clock_timestamps(cli, state_dir)
        probe_cross_process_locking(cli, state_dir)
        probe_task_lifecycle(cli, state_dir)
        probe_reminder_lifecycle(cli, state_dir)
        probe_daemon_reminder_fire(cli, state_dir)
        probe_navigation_surfaces(cli, state_dir)
        probe_membership_attention(cli, state_dir)
        probe_integration_local_login(cli, state_dir)
        probe_agent_registry(cli, state_dir)
        probe_attachments(cli, state_dir)
        probe_action_prepare(cli, state_dir)
        probe_slack_adapter(cli, state_dir)

    print("anti-stub probe ok: empty fresh store, dynamic inbox, send/read, reply hints, pagination/read limits, known empty surfaces, search/resolve, reactions, routing, freshness cursor/draft membership, DM, timestamps, SQLite locking, tasks/task filters/message-id claims, reminders/daemon fire, navigation, profile avatars, membership, integrations, agent registry/seed/heartbeat, attachments, action prepare, and Slack adapter ingest/resolve/outbound/send/customize")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeFailure as exc:
        print(f"anti-stub probe failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
