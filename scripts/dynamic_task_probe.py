#!/usr/bin/env python3
"""Focused, real-process probe for Swarm dynamic task formation v1."""

from __future__ import annotations

import json
import os
import re
import runpy
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
CLI = REPO / "swarm"


def run(
    *args: str,
    state_dir: Path,
    stdin: str | None = None,
    expected: int = 0,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        [str(CLI), *args],
        input=stdin,
        text=True,
        capture_output=True,
        cwd=REPO,
        env=env,
        check=False,
    )
    if result.returncode != expected:
        raise AssertionError(
            f"{' '.join(args)} returned {result.returncode}, expected {expected}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def write_runtime(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys

prompt = sys.stdin.read()
run_id = re.search(r"run_id[\\\"=: ]+(orch_[a-z0-9]+)", prompt).group(1)
root_id = re.search(r"root_message_id[\\\"`: ]+([0-9a-f-]{36})", prompt).group(1)
attempt = int(re.search(r"claim_attempt[\\\"=: ]+(\\d+)", prompt).group(1))
target = re.search(r"thread[\\\"=: ]+(#[^\\\"\\n ]+)", prompt).group(1)
task_key = re.search(r"task_key[\\\"=: ]+([a-z0-9_-]+)", prompt).group(1)
subprocess.run(
    ["swarm", "message", "read", "--channel", target, "--around", root_id[:8]],
    check=True,
    stdout=subprocess.DEVNULL,
)
if task_key == "plan":
    payload = {
        "schema": "swarm.dynamic-tasks.v1",
        "run_id": run_id,
        "root_message_id": root_id,
        "graph_version": 1,
        "tasks": [
            {
                "task_key": "assemble-checklist",
                "title": "Assemble the checklist",
                "objective": "Produce the requested checklist artifact.",
                "required_capabilities": ["worker"],
                "acceptance": ["A concrete checklist result is returned."],
                "phase": 0,
                "delivery_target": target,
            },
            {
                "task_key": "inspect-usability",
                "title": "Inspect checklist usability",
                "objective": "Inspect the requested checklist for usability.",
                "required_capabilities": ["worker"],
                "acceptance": ["A concrete usability result is returned."],
                "phase": 0,
                "delivery_target": target,
            },
            {
                "task_key": "publish-verdict",
                "title": "Publish the verification verdict",
                "objective": "Report the exact final verification verdict.",
                "required_capabilities": ["worker"],
                "acceptance": ["A concrete verification result is returned."],
                "phase": 1,
                "delivery_target": target,
            },
        ],
    }
    completed = subprocess.run(
        ["swarm", "agent", "plan-commit", "--run-id", run_id, "--attempt", str(attempt)],
        input=json.dumps(payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        raise SystemExit(completed.returncode)
    print("SILENT")
else:
    print(f"completed task={task_key} owner={os.environ['SWARM_AGENT_NAME']}")
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def write_invalid_runtime(path: Path, mode: str = "empty") -> None:
    source = """#!/usr/bin/env python3
import json
import re
import subprocess
import sys

mode = "__MODE__"
prompt = sys.stdin.read()
run_id = re.search(r"run_id[\\\"=: ]+(orch_[a-z0-9]+)", prompt).group(1)
root_id = re.search(r"root_message_id[\\\"`: ]+([0-9a-f-]{36})", prompt).group(1)
attempt = int(re.search(r"claim_attempt[\\\"=: ]+(\\d+)", prompt).group(1))
target = re.search(r"thread[\\\"=: ]+(#[^\\\"\\n ]+)", prompt).group(1)
subprocess.run(
    ["swarm", "message", "read", "--channel", target, "--around", root_id[:8]],
    check=True,
    stdout=subprocess.DEVNULL,
)
task = {
    "task_key": "duplicate-key",
    "title": "Duplicate key test",
    "objective": "Exercise duplicate-key rejection.",
    "required_capabilities": ["worker"],
    "acceptance": ["The invalid plan is rejected."],
    "phase": 0,
    "delivery_target": target,
}
base = {
    "schema": "swarm.dynamic-tasks.v1",
    "run_id": run_id,
    "root_message_id": root_id,
    "graph_version": 1,
}
if mode == "empty":
    raw = json.dumps({**base, "tasks": []}, separators=(",", ":"))
    expected_code = "PLAN_TASK_COUNT_INVALID"
elif mode == "duplicate":
    raw = json.dumps({**base, "tasks": [task, task]}, separators=(",", ":"))
    expected_code = "PLAN_TASK_KEY_DUPLICATE"
elif mode == "malformed":
    raw = "{"
    expected_code = "PLAN_JSON_INVALID"
elif mode == "over-budget":
    raw = json.dumps({**base, "tasks": [task], "padding": "x" * 40000})
    expected_code = "PLAN_BUDGET_EXCEEDED"
else:
    raise SystemExit(f"unknown invalid-plan mode: {mode}")
completed = subprocess.run(
    ["swarm", "agent", "plan-commit", "--run-id", run_id, "--attempt", str(attempt)],
    input=raw,
    text=True,
    capture_output=True,
    check=False,
)
if completed.returncode == 0 or expected_code not in completed.stderr:
    sys.stderr.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    raise SystemExit(1)
print("SILENT")
"""
    path.write_text(source.replace("__MODE__", mode), encoding="utf-8")
    path.chmod(0o755)


def write_freshness_runtime(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

prompt = sys.stdin.read()
run_id = re.search(r"run_id[\\\"=: ]+(orch_[a-z0-9]+)", prompt).group(1)
root_id = re.search(r"root_message_id[\\\"`: ]+([0-9a-f-]{36})", prompt).group(1)
attempt = int(re.search(r"claim_attempt[\\\"=: ]+(\\d+)", prompt).group(1))
target = re.search(r"thread[\\\"=: ]+(#[^\\\"\\n ]+)", prompt).group(1)
task_key = re.search(r"task_key[\\\"=: ]+([a-z0-9_-]+)", prompt).group(1)
subprocess.run(
    ["swarm", "message", "read", "--channel", target, "--around", root_id[:8]],
    check=True,
    stdout=subprocess.DEVNULL,
)
if task_key == "plan":
    payload = {
        "schema": "swarm.dynamic-tasks.v1",
        "run_id": run_id,
        "root_message_id": root_id,
        "graph_version": 1,
        "tasks": [
            {
                "task_key": "freshness-task",
                "title": "Freshness-aware task",
                "objective": "Reread and complete against the newest human context.",
                "required_capabilities": ["worker"],
                "acceptance": ["Only the fresh rerun result is committed."],
                "phase": 0,
                "delivery_target": target,
            }
        ],
    }
    completed = subprocess.run(
        ["swarm", "agent", "plan-commit", "--run-id", run_id, "--attempt", str(attempt)],
        input=json.dumps(payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        raise SystemExit(completed.returncode)
    print("SILENT")
else:
    state_dir = Path(os.environ["SWARM_CANDIDATE_STATE_DIR"])
    marker = state_dir / "freshness-injected"
    if not marker.exists():
        sent = subprocess.run(
            ["swarm", "message", "send", "--target", target],
            input="Task-local clarification landed during the owner turn.\\n",
            text=True,
            capture_output=True,
            check=True,
        )
        message_match = re.search(
            r"Message ID: ([0-9a-f-]{36})",
            sent.stdout,
        )
        if message_match is None:
            sent = subprocess.run(
                ["swarm", "message", "send", "--send-draft", "--target", target],
                text=True,
                capture_output=True,
                check=True,
            )
            message_match = re.search(
                r"Message ID: ([0-9a-f-]{36})",
                sent.stdout,
            )
        if message_match is None:
            sys.stderr.write(sent.stdout)
            sys.stderr.write(sent.stderr)
            raise SystemExit(1)
        message_id = message_match.group(1)
        with sqlite3.connect(state_dir / "state.sqlite3") as conn:
            conn.execute(
                "UPDATE messages SET type='human', author='owner' WHERE id=?",
                (message_id,),
            )
            conn.commit()
        marker.write_text(message_id, encoding="utf-8")
        print("stale dynamic result must not commit")
    else:
        print("fresh dynamic result committed after reread")
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def rows(conn: sqlite3.Connection, query: str, params: tuple = ()) -> list[sqlite3.Row]:
    return list(conn.execute(query, params).fetchall())


def event_details(conn: sqlite3.Connection, event: str) -> list[dict]:
    return [
        json.loads(row["detail"])
        for row in rows(
            conn,
            "SELECT detail FROM daemon_events WHERE event=? ORDER BY ordinal",
            (event,),
        )
    ]


def runtime_api(state_dir: Path) -> dict[str, object]:
    os.environ["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    return runpy.run_path(str(CLI), run_name="swarm_dynamic_probe_api")


def send_human_root(state_dir: Path, body: str) -> str:
    sent = run(
        "message",
        "send",
        "--target",
        "#general",
        stdin=f"{body}\n",
        state_dir=state_dir,
    )
    root_id = re.search(r"Message ID: ([0-9a-f-]{36})", sent.stdout).group(1)
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.execute(
            "UPDATE messages SET type='human', author='owner' WHERE id=?",
            (root_id,),
        )
        conn.commit()
    return root_id


def register_agent(
    state_dir: Path,
    name: str,
    runtime_command: str,
    *capabilities: str,
) -> None:
    args = [
        "agent",
        "register",
        "--name",
        name,
        "--display-name",
        name,
        "--runtime",
        runtime_command,
        "--workspace",
        f"agents/{name}",
    ]
    for capability in capabilities:
        args.extend(("--capability", capability))
    run(*args, state_dir=state_dir)


def probe_invalid_plan(parent_dir: Path) -> dict[str, object]:
    state_dir = parent_dir / "invalid-plan"
    state_dir.mkdir()
    runtime = state_dir / "invalid-runtime.py"
    write_invalid_runtime(runtime)
    register_agent(
        state_dir,
        "planner-seat",
        f"{sys.executable} {runtime}",
        "planner",
        "dissect",
    )
    root_id = send_human_root(state_dir, "Create a dynamic plan from this goal.")
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#general",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
    register_agent(
        state_dir,
        "worker-only",
        f"{sys.executable} {runtime}",
        "worker",
    )
    mismatched = run(
        "agent",
        "task-claim",
        "--run-id",
        run_id,
        "--graph-version",
        "1",
        "--task-key",
        "plan",
        "--expected-attempt",
        "0",
        "--agent",
        "worker-only",
        state_dir=state_dir,
        expected=1,
    )
    assert "CAPABILITY_MISMATCH" in mismatched.stderr

    # Malformed JSON from a non-owner/stale turn must not be able to hold a
    # planning run. Provenance/fencing checks precede validation side effects.
    unauthorized = run(
        "agent",
        "plan-commit",
        "--run-id",
        run_id,
        "--attempt",
        "1",
        stdin="{",
        state_dir=state_dir,
        expected=1,
        extra_env={
            "SWARM_AGENT_NAME": "intruder",
            "SWARM_TURN_ID": "999999",
        },
    )
    assert "PLAN_FENCE_MISMATCH" in unauthorized.stderr
    db = state_dir / "state.sqlite3"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()[0] == "planning"
        assert conn.execute(
            "SELECT COUNT(*) FROM orchestration_receipts WHERE run_id=?",
            (run_id,),
        ).fetchone()[0] == 0

    run(
        "daemon",
        "resident",
        "--loops",
        "1",
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
        state_dir=state_dir,
    )
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        assert rows(
            conn,
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        )[0]["state"] == "held"
        assert rows(
            conn,
            "SELECT COUNT(*) AS count FROM orchestration_tasks WHERE run_id=? AND task_key!='plan'",
            (run_id,),
        )[0]["count"] == 0
        rejected = rows(
            conn,
            "SELECT * FROM orchestration_receipts WHERE run_id=? AND receipt_kind='plan_rejected'",
            (run_id,),
        )
        assert len(rejected) == 1
        visible = rows(
            conn,
            "SELECT body FROM messages WHERE id=?",
            (rejected[0]["message_id"],),
        )
        assert len(visible) == 1
        assert "PLAN_TASK_COUNT_INVALID" in visible[0]["body"]
        details = event_details(conn, "orchestration_dynamic_plan_rejected")
        assert len(details) == 1
        assert details[0]["tasks_created"] == 0
    return {
        "capability_mismatch_zero_turn": "PASS",
        "invalid_plan_fail_closed": "PASS",
        "unauthorized_malformed_plan_fenced": "PASS",
    }


def probe_no_eligible_capability(parent_dir: Path) -> dict[str, object]:
    state_dir = parent_dir / "no-eligible"
    state_dir.mkdir()
    runtime = state_dir / "dynamic-runtime.py"
    write_runtime(runtime)
    register_agent(
        state_dir,
        "planner-seat",
        f"{sys.executable} {runtime}",
        "planner",
        "dissect",
    )
    root_id = send_human_root(
        state_dir,
        "Create a worker task even though no worker seat is registered.",
    )
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#general",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
    run(
        "daemon",
        "resident",
        "--loops",
        "1",
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
        state_dir=state_dir,
    )
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        assert rows(
            conn,
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        )[0]["state"] == "held"
        escalations = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_escalation",
            )
            if detail.get("run_id") == run_id
            and detail.get("reason") == "no_eligible_capability"
        ]
        assert len(escalations) == 1
        receipts = rows(
            conn,
            """
            SELECT * FROM orchestration_receipts
            WHERE run_id=? AND receipt_kind='no_eligible_capability'
            """,
            (run_id,),
        )
        assert len(receipts) == 1
        assert rows(
            conn,
            "SELECT COUNT(*) AS count FROM orchestration_tasks WHERE run_id=? AND task_key!='plan' AND owner IS NOT NULL",
            (run_id,),
        )[0]["count"] == 0
    return {"no_eligible_capability_hold": "PASS"}


def probe_rejected_plan_variants(parent_dir: Path) -> dict[str, object]:
    expected_codes = {
        "duplicate": "PLAN_TASK_KEY_DUPLICATE",
        "malformed": "PLAN_JSON_INVALID",
        "over-budget": "PLAN_BUDGET_EXCEEDED",
    }
    for mode, expected_code in expected_codes.items():
        state_dir = parent_dir / f"invalid-{mode}"
        state_dir.mkdir()
        runtime = state_dir / "invalid-runtime.py"
        write_invalid_runtime(runtime, mode)
        register_agent(
            state_dir,
            "planner-seat",
            f"{sys.executable} {runtime}",
            "planner",
            "dissect",
        )
        root_id = send_human_root(
            state_dir,
            f"Exercise the {mode} dynamic-plan rejection boundary.",
        )
        started = run(
            "agent",
            "dynamic-start",
            "--channel",
            "#general",
            "--message-id",
            root_id[:8],
            state_dir=state_dir,
        )
        run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
        run(
            "daemon",
            "resident",
            "--loops",
            "1",
            "--idle-interval",
            "0s",
            "--turn-timeout",
            "10s",
            state_dir=state_dir,
        )
        with sqlite3.connect(state_dir / "state.sqlite3") as conn:
            conn.row_factory = sqlite3.Row
            assert rows(
                conn,
                "SELECT state FROM orchestration_runs WHERE run_id=?",
                (run_id,),
            )[0]["state"] == "held"
            assert rows(
                conn,
                """
                SELECT COUNT(*) AS count FROM orchestration_tasks
                WHERE run_id=? AND task_key!='plan'
                """,
                (run_id,),
            )[0]["count"] == 0
            rejected = rows(
                conn,
                """
                SELECT * FROM orchestration_receipts
                WHERE run_id=? AND receipt_kind='plan_rejected'
                """,
                (run_id,),
            )
            assert len(rejected) == 1
            visible = rows(
                conn,
                "SELECT body FROM messages WHERE id=?",
                (rejected[0]["message_id"],),
            )
            assert len(visible) == 1
            assert expected_code in visible[0]["body"]
    return {
        "duplicate_task_key_fail_closed": "PASS",
        "malformed_current_plan_fail_closed": "PASS",
        "over_budget_plan_fail_closed": "PASS",
    }


def probe_shape_changing_steer(parent_dir: Path) -> dict[str, object]:
    state_dir = parent_dir / "shape-steer"
    state_dir.mkdir()
    runtime = state_dir / "dynamic-runtime.py"
    write_runtime(runtime)
    register_agent(
        state_dir,
        "shape-seat",
        f"{sys.executable} {runtime}",
        "planner",
        "dissect",
        "worker",
    )
    root_id = send_human_root(
        state_dir,
        "Build the smallest useful checklist and verification result.",
    )
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#general",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
    run(
        "daemon",
        "resident",
        "--loops",
        "1",
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
        state_dir=state_dir,
    )
    db = state_dir / "state.sqlite3"
    with sqlite3.connect(db) as conn:
        immutable_before = (
            conn.execute(
                "SELECT graph_version,plan_sha256 FROM orchestration_runs WHERE run_id=?",
                (run_id,),
            ).fetchone(),
            list(
                conn.execute(
                    """
                    SELECT task_key,phase,state,owner,attempt,plan_sha256
                    FROM orchestration_tasks
                    WHERE run_id=?
                    ORDER BY task_key
                    """,
                    (run_id,),
                )
            ),
        )
    api = runtime_api(state_dir)

    def inject_steer(state: dict[str, object]) -> list[dict[str, str]]:
        run_row = next(
            row
            for row in state["orchestration_runs"]
            if row["run_id"] == run_id
        )
        target = f"#general:{root_id[:8]}"
        record = api["append_message"](
            state,
            target,
            "Please add another task and change the plan.",
            author="owner",
        )
        record["type"] = "human"
        os.environ["SWARM_DYNAMIC_TASKS_V1"] = "1"
        return api["route_message_wakes"](state, record)

    routed = api["with_mutable_state_value"](inject_steer)
    os.environ.pop("SWARM_DYNAMIC_TASKS_V1", None)
    assert routed == []
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        run_row = rows(
            conn,
            "SELECT * FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        )[0]
        assert run_row["state"] == "held"
        immutable_after = (
            (run_row["graph_version"], run_row["plan_sha256"]),
            [
                tuple(row)
                for row in rows(
                    conn,
                    """
                    SELECT task_key,phase,state,owner,attempt,plan_sha256
                    FROM orchestration_tasks
                    WHERE run_id=?
                    ORDER BY task_key
                    """,
                    (run_id,),
                )
            ],
        )
        assert (
            tuple(immutable_before[0]),
            [tuple(row) for row in immutable_before[1]],
        ) == immutable_after
        replans = [
            detail
            for detail in event_details(
                conn,
                "orchestration_replan_required",
            )
            if detail.get("run_id") == run_id
        ]
        assert len(replans) == 1
        assert replans[0]["graph_mutated"] is False
        receipts = rows(
            conn,
            """
            SELECT * FROM orchestration_receipts
            WHERE run_id=? AND receipt_kind='replan_required'
            """,
            (run_id,),
        )
        assert len(receipts) == 1
    return {"shape_changing_steer_holds_without_graph_mutation": "PASS"}


def probe_timeout_takeover_fence(parent_dir: Path) -> dict[str, object]:
    state_dir = parent_dir / "timeout-fence"
    state_dir.mkdir()
    runtime = state_dir / "dynamic-runtime.py"
    write_runtime(runtime)
    runtime_command = f"{sys.executable} {runtime}"
    register_agent(
        state_dir,
        "lease-a",
        runtime_command,
        "planner",
        "dissect",
        "worker",
    )
    register_agent(state_dir, "lease-b", runtime_command, "worker")
    root_id = send_human_root(
        state_dir,
        "Build and inspect a checklist with a takeover-safe task.",
    )
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#general",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
    run(
        "daemon",
        "resident",
        "--loops",
        "1",
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
        state_dir=state_dir,
    )
    task_key = "assemble-checklist"
    run(
        "agent",
        "task-claim",
        "--run-id",
        run_id,
        "--graph-version",
        "1",
        "--task-key",
        task_key,
        "--expected-attempt",
        "0",
        "--agent",
        "lease-a",
        state_dir=state_dir,
    )
    db = state_dir / "state.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        stale_task = dict(
            rows(
                conn,
                """
                SELECT * FROM orchestration_tasks
                WHERE run_id=? AND task_key=?
                """,
                (run_id, task_key),
            )[0]
        )
        conn.execute(
            """
            UPDATE orchestration_tasks
            SET lease_expires_at='2000-01-01 00:00:00'
            WHERE run_id=? AND task_key=?
            """,
            (run_id, task_key),
        )
        conn.commit()
    register_agent(
        state_dir,
        "lease-a",
        runtime_command,
        "planner",
        "dissect",
    )
    api = runtime_api(state_dir)
    assert api["process_dynamic_timeouts_once"]() >= 1
    takeover = run(
        "agent",
        "task-claim",
        "--run-id",
        run_id,
        "--graph-version",
        "1",
        "--task-key",
        task_key,
        "--expected-attempt",
        "1",
        "--agent",
        "lease-b",
        state_dir=state_dir,
    )
    assert "Fencing attempt: 2" in takeover.stdout

    def stale_finalize(state: dict[str, object]) -> dict[str, object]:
        run_row = next(
            row
            for row in state["orchestration_runs"]
            if row["run_id"] == run_id
        )
        turn_id = 999999
        api["append_daemon_event"](
            state,
            "orchestration_dynamic_body_read",
            {
                "run_id": run_id,
                "graph_version": 1,
                "task_key": task_key,
                "agent": "lease-a",
                "attempt": 1,
                "query_turn_id": str(turn_id),
                "explicit_query": True,
                "after_claim": True,
                "body_present": False,
            },
        )
        turn = {"id": turn_id}
        return api["finalize_dynamic_daemon_turn"](
            state,
            turn,
            run_row,
            stale_task,
            "lease-a",
            f"#general:{root_id[:8]}",
            0,
            False,
            "done",
            0,
            "stale result must not commit",
            "",
            None,
            None,
        )

    rejected = api["with_mutable_state_value"](stale_finalize)
    assert rejected["code"] == "DYNAMIC_TURN_REJECTED"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        current = rows(
            conn,
            """
            SELECT owner,attempt,state,result_sha256
            FROM orchestration_tasks
            WHERE run_id=? AND task_key=?
            """,
            (run_id, task_key),
        )[0]
        assert (
            current["owner"],
            current["attempt"],
            current["state"],
            current["result_sha256"],
        ) == ("lease-b", 2, "claimed", None)
        assert rows(
            conn,
            """
            SELECT COUNT(*) AS count FROM orchestration_receipts
            WHERE run_id=? AND task_key=? AND receipt_kind='result'
            """,
            (run_id, task_key),
        )[0]["count"] == 0
        expired = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_lease_expired",
            )
            if detail.get("run_id") == run_id
            and detail.get("task_key") == task_key
        ]
        assert len(expired) == 1
        assert expired[0]["expired_attempt"] == 1
        rejects = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_commit_rejected",
            )
            if detail.get("run_id") == run_id
            and detail.get("task_key") == task_key
        ]
        assert len(rejects) == 1
        assert rejects[0]["result_committed"] is False
    return {"timeout_takeover_and_stale_fence": "PASS"}


def probe_dynamic_freshness_rerun(parent_dir: Path) -> dict[str, object]:
    state_dir = parent_dir / "freshness-rerun"
    state_dir.mkdir()
    runtime = state_dir / "freshness-runtime.py"
    write_freshness_runtime(runtime)
    register_agent(
        state_dir,
        "fresh-seat",
        f"{sys.executable} {runtime}",
        "planner",
        "dissect",
        "worker",
    )
    root_id = send_human_root(
        state_dir,
        "Complete one task while respecting a mid-turn clarification.",
    )
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#general",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
    run(
        "daemon",
        "resident",
        "--loops",
        "3",
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
        state_dir=state_dir,
    )
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        run_row = rows(
            conn,
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        )[0]
        if run_row["state"] != "done":
            raise AssertionError(
                json.dumps(
                    {
                        "run": dict(run_row),
                        "tasks": [
                            dict(row)
                            for row in rows(
                                conn,
                                "SELECT * FROM orchestration_tasks WHERE run_id=?",
                                (run_id,),
                            )
                        ],
                        "turns": [
                            dict(row)
                            for row in rows(
                                conn,
                                "SELECT * FROM daemon_turns ORDER BY id",
                            )
                        ],
                        "events": [
                            dict(row)
                            for row in rows(
                                conn,
                                """
                                SELECT ordinal,event,detail
                                FROM daemon_events
                                WHERE detail LIKE ?
                                ORDER BY ordinal
                                """,
                                (f'%"run_id":"{run_id}"%',),
                            )
                        ],
                    },
                    default=str,
                )
            )
        task = rows(
            conn,
            """
            SELECT state,owner,attempt,result_sha256
            FROM orchestration_tasks
            WHERE run_id=? AND task_key='freshness-task'
            """,
            (run_id,),
        )[0]
        assert (task["state"], task["owner"], task["attempt"]) == (
            "done",
            "fresh-seat",
            1,
        )
        holds = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_freshness_hold",
            )
            if detail.get("run_id") == run_id
        ]
        assert len(holds) == 1
        assert holds[0]["result_committed"] is False
        executions = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_execution",
            )
            if detail.get("run_id") == run_id
            and detail.get("task_key") == "freshness-task"
        ]
        assert len(executions) == 1
        reads = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_body_read",
            )
            if detail.get("run_id") == run_id
            and detail.get("task_key") == "freshness-task"
        ]
        assert len(reads) == 2
        assert {int(detail["attempt"]) for detail in reads} == {1}
        receipts = rows(
            conn,
            """
            SELECT m.body
            FROM orchestration_receipts r
            JOIN messages m ON m.id=r.message_id
            WHERE r.run_id=? AND r.task_key='freshness-task'
              AND r.receipt_kind='result'
            """,
            (run_id,),
        )
        assert len(receipts) == 1
        assert "fresh dynamic result committed after reread" in receipts[0]["body"]
        assert "stale dynamic result must not commit" not in receipts[0]["body"]
    return {"dynamic_freshness_hold_and_rerun": "PASS"}


def probe_failed_phase_holds(parent_dir: Path) -> dict[str, object]:
    state_dir = parent_dir / "phase-failure"
    state_dir.mkdir()
    runtime = state_dir / "dynamic-runtime.py"
    write_runtime(runtime)
    register_agent(
        state_dir,
        "phase-seat",
        f"{sys.executable} {runtime}",
        "planner",
        "dissect",
        "worker",
    )
    root_id = send_human_root(
        state_dir,
        "Build a two-phase checklist and hold later work if phase zero fails.",
    )
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#general",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
    run(
        "daemon",
        "resident",
        "--loops",
        "1",
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
        state_dir=state_dir,
    )
    api = runtime_api(state_dir)

    def fail_phase_zero(state: dict[str, object]) -> None:
        run_row = next(
            row
            for row in state["orchestration_runs"]
            if row["run_id"] == run_id
        )
        failed = next(
            row
            for row in state["orchestration_tasks"]
            if row["run_id"] == run_id
            and row["task_key"] == "assemble-checklist"
        )
        failed["state"] = "failed"
        api["advance_dynamic_phase_barrier"](state, run_row)

    api["with_mutable_state_value"](fail_phase_zero)
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        assert rows(
            conn,
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        )[0]["state"] == "held"
        later = rows(
            conn,
            """
            SELECT state,owner FROM orchestration_tasks
            WHERE run_id=? AND task_key='publish-verdict'
            """,
            (run_id,),
        )[0]
        assert (later["state"], later["owner"]) == ("blocked", None)
        receipts = rows(
            conn,
            """
            SELECT * FROM orchestration_receipts
            WHERE run_id=? AND receipt_kind='phase_failed'
            """,
            (run_id,),
        )
        assert len(receipts) == 1
        held = [
            detail
            for detail in event_details(
                conn,
                "orchestration_dynamic_phase_held",
            )
            if detail.get("run_id") == run_id
        ]
        assert len(held) == 1
        assert held[0]["reason"] == "prior_phase_failed"
    return {"failed_prior_phase_holds_later_phase": "PASS"}


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="swarm-dynamic-probe-") as raw_dir:
        state_dir = Path(raw_dir)
        runtime = state_dir / "dynamic-runtime.py"
        write_runtime(runtime)
        runtime_command = f"{sys.executable} {runtime}"
        for name in ("seat-a", "seat-b"):
            register_agent(
                state_dir,
                name,
                runtime_command,
                "planner",
                "dissect",
                "worker",
            )
        root_id = send_human_root(
            state_dir,
            "Build a tiny checklist, inspect it, and report the verified result.",
        )
        db = state_dir / "state.sqlite3"
        started = run(
            "agent",
            "dynamic-start",
            "--channel",
            "#general",
            "--message-id",
            root_id[:8],
            state_dir=state_dir,
        )
        run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)
        run(
            "daemon",
            "resident",
            "--loops",
            "3",
            "--idle-interval",
            "0s",
            "--turn-timeout",
            "10s",
            state_dir=state_dir,
        )
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            run_row = rows(
                conn,
                "SELECT * FROM orchestration_runs WHERE run_id=?",
                (run_id,),
            )[0]
            assert run_row["state"] == "done"
            dynamic_tasks = rows(
                conn,
                """
                SELECT * FROM orchestration_tasks
                WHERE run_id=? AND task_key!='plan'
                ORDER BY phase, task_key
                """,
                (run_id,),
            )
            assert len(dynamic_tasks) == 3
            assert {row["task_key"] for row in dynamic_tasks}.isdisjoint(
                {"dissect", "research", "execute", "verify", "receipt"}
            )
            assert all(row["state"] == "done" for row in dynamic_tasks)
            assert {row["owner"] for row in dynamic_tasks} == {"seat-a", "seat-b"}
            receipts = rows(
                conn,
                "SELECT * FROM orchestration_receipts WHERE run_id=?",
                (run_id,),
            )
            assert len(receipts) == 4
            assert len({row["message_id"] for row in receipts}) == 4
            claims = event_details(conn, "orchestration_dynamic_claim")
            collision_groups: dict[str, list[dict]] = {}
            for claim in claims:
                window = claim.get("contention_window_id")
                if window:
                    collision_groups.setdefault(window, []).append(claim)
            real_races = [
                group
                for group in collision_groups.values()
                if {row.get("outcome") for row in group}
                >= {"winner", "conflict_stop"}
                and len(group) >= 2
                and all(int(row.get("barrier_participant_count", 0)) >= 2 for row in group)
                and max(int(row["barrier_ready_epoch_ms"]) for row in group)
                <= min(
                    int(row.get("winner_commit_epoch_ms", 2**63 - 1))
                    for row in group
                    if row.get("outcome") == "winner"
                )
            ]
            assert real_races
            losers = [
                row
                for row in claims
                if row.get("outcome") == "conflict_stop"
            ]
            assert losers
            assert all(
                (
                    row.get("body_reads"),
                    row.get("full_model_turns"),
                    row.get("outward_replies"),
                    row.get("executions"),
                )
                == (0, 0, 0, 0)
                for row in losers
            )
            body_reads = event_details(conn, "orchestration_dynamic_body_read")
            task_winners = {
                (row["task_key"], row["owner"], int(row["attempt"]))
                for row in rows(
                    conn,
                    "SELECT task_key,owner,attempt FROM orchestration_tasks WHERE run_id=?",
                    (run_id,),
                )
            }
            assert {
                (
                    detail["task_key"],
                    detail["agent"],
                    int(detail["attempt"]),
                )
                for detail in body_reads
            } == task_winners
            phase_opened = event_details(
                conn,
                "orchestration_dynamic_phase_opened",
            )
            assert [int(row["phase"]) for row in phase_opened] == [1]
            trace_text = "\n".join(
                row["detail"]
                for row in rows(
                    conn,
                    "SELECT detail FROM daemon_events WHERE detail LIKE ?",
                    (f'%"run_id":"{run_id}"%',),
                )
            )
            root_body = "Build a tiny checklist, inspect it, and report the verified result."
            assert root_body not in trace_text
            assert '"objective":' not in trace_text
            assert '"acceptance":' not in trace_text
            before_counts = (
                len(dynamic_tasks),
                len(receipts),
                len(
                    rows(
                        conn,
                        "SELECT * FROM daemon_turns WHERE input_message_id=?",
                        (root_id,),
                    )
                ),
            )
            planner_task = rows(
                conn,
                """
                SELECT owner,attempt,planner_turn_id
                FROM orchestration_tasks
                WHERE run_id=? AND task_key='plan'
                """,
                (run_id,),
            )[0]
            planner_owner = str(planner_task["owner"])
            planner_attempt = int(planner_task["attempt"])
            planner_turn_id = int(planner_task["planner_turn_id"])
            plan_json = str(run_row["plan_json"])
            immutable_before = (
                run_row["state"],
                run_row["plan_sha256"],
                len(rows(conn, "SELECT * FROM orchestration_tasks WHERE run_id=?", (run_id,))),
                len(receipts),
                len(
                    rows(
                        conn,
                        "SELECT * FROM daemon_turns WHERE input_message_id=?",
                        (root_id,),
                    )
                ),
            )
        plan_env = {
            "SWARM_AGENT_NAME": planner_owner,
            "SWARM_TURN_ID": str(planner_turn_id),
        }
        idempotent = run(
            "agent",
            "plan-commit",
            "--run-id",
            run_id,
            "--attempt",
            str(planner_attempt),
            stdin=plan_json,
            state_dir=state_dir,
            extra_env=plan_env,
        )
        assert "Outcome: idempotent" in idempotent.stdout
        conflicting = json.loads(plan_json)
        conflicting["tasks"][0]["title"] += " conflict"
        conflict = run(
            "agent",
            "plan-commit",
            "--run-id",
            run_id,
            "--attempt",
            str(planner_attempt),
            stdin=json.dumps(conflicting, separators=(",", ":")),
            state_dir=state_dir,
            expected=1,
            extra_env=plan_env,
        )
        assert "GRAPH_VERSION_CONFLICT" in conflict.stderr
        malformed = run(
            "agent",
            "plan-commit",
            "--run-id",
            run_id,
            "--attempt",
            str(planner_attempt),
            stdin="{",
            state_dir=state_dir,
            expected=1,
            extra_env=plan_env,
        )
        assert "GRAPH_VERSION_CONFLICT" in malformed.stderr
        with sqlite3.connect(db) as conn:
            immutable_after = (
                conn.execute(
                    "SELECT state FROM orchestration_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()[0],
                conn.execute(
                    "SELECT plan_sha256 FROM orchestration_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM orchestration_tasks WHERE run_id=?",
                    (run_id,),
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM orchestration_receipts WHERE run_id=?",
                    (run_id,),
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM daemon_turns WHERE input_message_id=?",
                    (root_id,),
                ).fetchone()[0],
            )
        assert immutable_after == immutable_before
        replay = run(
            "agent",
            "dynamic-start",
            "--channel",
            "#general",
            "--message-id",
            root_id[:8],
            state_dir=state_dir,
        )
        assert "already exists" in replay.stdout
        with sqlite3.connect(db) as conn:
            after_counts = (
                conn.execute(
                    "SELECT COUNT(*) FROM orchestration_tasks WHERE run_id=? AND task_key!='plan'",
                    (run_id,),
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM orchestration_receipts WHERE run_id=?",
                    (run_id,),
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM daemon_turns WHERE input_message_id=?",
                    (root_id,),
                ).fetchone()[0],
            )
        assert after_counts == before_counts
        negative_results = {
            **probe_invalid_plan(state_dir),
            **probe_rejected_plan_variants(state_dir),
            **probe_no_eligible_capability(state_dir),
            **probe_shape_changing_steer(state_dir),
            **probe_timeout_takeover_fence(state_dir),
            **probe_dynamic_freshness_rerun(state_dir),
            **probe_failed_phase_holds(state_dir),
        }
        print(
            json.dumps(
                {
                    "scenario": "dynamic-task-probe",
                    "run_id": run_id,
                    "status": "PASS",
                    "task_count": 3,
                    "real_claim_races": len(real_races),
                    "loser_economics": "0/0/0/0",
                    "phase_barrier": "PASS",
                    "exact_once_receipts": "PASS",
                    "restart_idempotency": "PASS",
                    "same_hash_plan_retry": "PASS",
                    "conflicting_plan_immutable": "PASS",
                    **negative_results,
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
