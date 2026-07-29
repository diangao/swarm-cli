#!/usr/bin/env python3
"""Kill a resident mid-turn and prove atomic startup reconciliation."""

from __future__ import annotations

import json
import os
import re
import runpy
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable


REPO = Path(__file__).resolve().parents[1]
CLI = REPO / "swarm"
RESTART_ERROR = "resident restarted before turn completion"


def probe_env(state_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    env["SWARM_DYNAMIC_TASKS_V1"] = "1"
    return env


def run(
    *args: str,
    state_dir: Path,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [str(CLI), *args],
        input=stdin,
        text=True,
        capture_output=True,
        cwd=REPO,
        env=probe_env(state_dir),
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"{' '.join(args)} failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def write_runtime(path: Path) -> None:
    path.write_text(
        r'''#!/usr/bin/env python3
import json
import re
import subprocess
import sys
import time

prompt = sys.stdin.read()
run_id = re.search(r'run_id[\"=: ]+(orch_[a-z0-9]+)', prompt).group(1)
root_id = re.search(r'root_message_id[\"`: ]+([0-9a-f-]{36})', prompt).group(1)
attempt = int(re.search(r'claim_attempt[\"=: ]+(\d+)', prompt).group(1))
target = re.search(r'thread[\"=: ]+(#[^\"\n ]+)', prompt).group(1)
task_key = re.search(r'task_key[\"=: ]+([a-z0-9_-]+)', prompt).group(1)
graph_version = int(re.search(r'graph_version[\"=: ]+(\d+)', prompt).group(1))
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
        "graph_version": graph_version,
        "tasks": [
            {
                "task_key": "restart-work",
                "title": "Recover one interrupted native turn",
                "objective": "Resume from the durable task after a real resident crash.",
                "required_capabilities": ["worker"],
                "acceptance": [
                    "The replacement attempt completes after startup reconciliation."
                ],
                "phase": 0,
                "delivery_target": target,
            }
        ],
    }
    command = [
        "swarm", "agent", "plan-commit",
        "--run-id", run_id,
        "--attempt", str(attempt),
    ]
else:
    if attempt == 1:
        time.sleep(30)
    else:
        time.sleep(0.2)
    payload = {
        "schema": "swarm.dynamic-task-progress.v1",
        "run_id": run_id,
        "graph_version": graph_version,
        "task_key": task_key,
        "attempt": attempt,
        "status": "complete",
        "summary": "replacement attempt completed after atomic startup reconciliation",
        "checkpoint": {
            "acceptance_evidence": [
                {
                    "criterion_index": 0,
                    "evidence_ref": "trace://startup-reconciliation-replacement",
                }
            ]
        },
        "next_action": "",
    }
    command = [
        "swarm", "agent", "task-progress-commit",
        "--run-id", run_id,
        "--graph-version", str(graph_version),
        "--task-key", task_key,
        "--attempt", str(attempt),
    ]

completed = subprocess.run(
    command,
    input=json.dumps(payload, separators=(",", ":")),
    text=True,
    capture_output=True,
    check=False,
)
if completed.returncode != 0:
    sys.stderr.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    raise SystemExit(completed.returncode)
print("SILENT")
''',
        encoding="utf-8",
    )
    path.chmod(0o755)


def register_agent(
    state_dir: Path,
    name: str,
    runtime: Path,
    capability: str,
) -> None:
    run(
        "agent",
        "register",
        "--name",
        name,
        "--display-name",
        name,
        "--runtime",
        f"{sys.executable} {runtime}",
        "--workspace",
        f"agents/{name}",
        "--capability",
        capability,
        state_dir=state_dir,
    )


def wait_until(
    description: str,
    predicate: Callable[[], object | None],
    timeout: float = 15,
) -> object:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}")


def event_rows(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, str]]:
    return [
        (str(row[0]), str(row[1]), str(row[2]))
        for row in conn.execute(
            "SELECT time, event, detail FROM daemon_events ORDER BY ordinal"
        ).fetchall()
    ]


def event_details(
    conn: sqlite3.Connection,
    event: str,
    run_id: str | None = None,
) -> list[tuple[int, dict[str, object]]]:
    rows = conn.execute(
        "SELECT ordinal, detail FROM daemon_events WHERE event=? ORDER BY ordinal",
        (event,),
    ).fetchall()
    decoded = [(int(row[0]), json.loads(row[1])) for row in rows]
    if run_id is None:
        return decoded
    return [
        (ordinal, detail)
        for ordinal, detail in decoded
        if detail.get("run_id") == run_id
        or detail.get("dynamic_run_id") == run_id
    ]


def kill_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=5)


def start_resident(
    state_dir: Path,
    *extra: str,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            str(CLI),
            "daemon",
            "resident",
            "--idle-interval",
            "0s",
            "--turn-timeout",
            "45s",
            *extra,
        ],
        cwd=REPO,
        env=probe_env(state_dir),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def enqueue_persisted_candidate(
    state_dir: Path,
    run_id: str,
    root_id: str,
) -> None:
    os.environ["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    os.environ["SWARM_DYNAMIC_TASKS_V1"] = "1"
    api = runpy.run_path(
        str(CLI),
        run_name="swarm_startup_reconciliation_probe_api",
    )

    def action(state: dict[str, object]) -> str:
        reason = api["dynamic_notice_reason"](
            run_id,
            1,
            "restart-work",
            1,
        )
        return api["enqueue_daemon_wake"](
            state,
            "worker",
            reason,
            root_id,
        )

    outcome = api["with_mutable_state_value"](action)
    assert outcome == "pending"


def running_worker_turn(
    state_dir: Path,
    run_id: str,
) -> tuple[int, int] | None:
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        row = conn.execute(
            """
            SELECT dt.id, ot.attempt
            FROM daemon_turns dt
            JOIN orchestration_runs r
              ON r.root_message_id=dt.input_message_id
            JOIN orchestration_tasks ot
              ON ot.run_id=r.run_id
             AND ot.task_key='restart-work'
             AND ot.owner=dt.agent
            WHERE r.run_id=?
              AND dt.agent='worker'
              AND dt.status='running'
            ORDER BY dt.id DESC LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        turn_id, attempt = int(row[0]), int(row[1])
        body_read = any(
            detail.get("task_key") == "restart-work"
            and detail.get("agent") == "worker"
            and int(detail.get("attempt") or 0) == attempt
            and int(detail.get("query_turn_id") or 0) == turn_id
            for _, detail in event_details(
                conn,
                "orchestration_dynamic_body_read",
                run_id,
            )
        )
        return (turn_id, attempt) if body_read else None


def completed_replacement(
    state_dir: Path,
    run_id: str,
) -> tuple[int, int] | None:
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        task = conn.execute(
            """
            SELECT attempt, state, progress_turn_id
            FROM orchestration_tasks
            WHERE run_id=? AND task_key='restart-work'
            """,
            (run_id,),
        ).fetchone()
        if (
            task is None
            or str(task[1]) != "done"
            or int(task[0]) != 2
            or task[2] is None
        ):
            return None
        return int(task[0]), int(task[2])


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="swarm-startup-reconcile-") as raw:
        state_dir = Path(raw)
        runtime = state_dir / "runtime.py"
        write_runtime(runtime)
        register_agent(state_dir, "planner", runtime, "planner")
        register_agent(state_dir, "worker", runtime, "worker")

        sent = run(
            "message",
            "send",
            "--target",
            "#work",
            stdin="Prove one interrupted task resumes safely after restart.\n",
            state_dir=state_dir,
        )
        root_id = re.search(
            r"Message ID: ([0-9a-f-]{36})",
            sent.stdout,
        ).group(1)
        with sqlite3.connect(state_dir / "state.sqlite3") as conn:
            conn.execute(
                "UPDATE messages SET type='human', author='owner' WHERE id=?",
                (root_id,),
            )
            conn.commit()
        started = run(
            "agent",
            "dynamic-start",
            "--channel",
            "#work",
            "--message-id",
            root_id[:8],
            state_dir=state_dir,
        )
        run_id = re.search(
            r"Run ID: (orch_[a-z0-9]+)",
            started.stdout,
        ).group(1)

        first = start_resident(state_dir)
        second: subprocess.Popen[str] | None = None
        try:
            old_turn_id, old_attempt = wait_until(
                "attempt-one worker turn and explicit owner read",
                lambda: running_worker_turn(state_dir, run_id),
            )
            assert old_attempt == 1
            # Model the exact persisted shape from the live blocker: an old
            # timeout notice was pending behind the still-active owner wake.
            # Startup must drain that stale promoted candidate before emitting
            # one fresh notice, or the replacement completes into a wake loop.
            enqueue_persisted_candidate(state_dir, run_id, root_id)
            kill_process_group(first)

            with sqlite3.connect(state_dir / "state.sqlite3") as conn:
                before_events = event_rows(conn)
                old_task = conn.execute(
                    """
                    SELECT state, owner, attempt, progress_turn_id
                    FROM orchestration_tasks
                    WHERE run_id=? AND task_key='restart-work'
                    """,
                    (run_id,),
                ).fetchone()
                old_turn = conn.execute(
                    """
                    SELECT status, error FROM daemon_turns WHERE id=?
                    """,
                    (old_turn_id,),
                ).fetchone()
                owner_wake = conn.execute(
                    """
                    SELECT running, active_reason, pending_count, pending_reason
                    FROM daemon_wakes WHERE agent_name='worker'
                    """
                ).fetchone()
            assert old_task == ("running", "worker", 1, None)
            assert old_turn == ("running", None)
            assert int(owner_wake[0]) == 1
            assert str(owner_wake[1]).endswith(":restart-work:1")
            assert int(owner_wake[2]) == 1
            assert str(owner_wake[3]).endswith(":restart-work:1")

            second = start_resident(state_dir, "--loops", "5")
            wait_until(
                "attempt-two replacement completion",
                lambda: completed_replacement(state_dir, run_id),
                timeout=20,
            )
            second.wait(timeout=10)
            assert second.returncode == 0

            with sqlite3.connect(state_dir / "state.sqlite3") as conn:
                conn.row_factory = sqlite3.Row
                after_events = event_rows(conn)
                old_turn = conn.execute(
                    "SELECT * FROM daemon_turns WHERE id=?",
                    (old_turn_id,),
                ).fetchone()
                task = conn.execute(
                    """
                    SELECT * FROM orchestration_tasks
                    WHERE run_id=? AND task_key='restart-work'
                    """,
                    (run_id,),
                ).fetchone()
                replacement_turn = conn.execute(
                    "SELECT * FROM daemon_turns WHERE id=?",
                    (int(task["progress_turn_id"]),),
                ).fetchone()
                wake = conn.execute(
                    """
                    SELECT running, active_reason, pending_count, pending_reason
                    FROM daemon_wakes WHERE agent_name='worker'
                    """
                ).fetchone()
                summaries = event_details(
                    conn,
                    "resident_startup_reconciled",
                )
                turn_reconciled = event_details(
                    conn,
                    "resident_startup_turn_reconciled",
                    run_id,
                )
                wake_reconciled = event_details(
                    conn,
                    "resident_startup_wake_reconciled",
                )
                claims = [
                    (ordinal, detail)
                    for ordinal, detail in event_details(
                        conn,
                        "orchestration_dynamic_claim",
                        run_id,
                    )
                    if detail.get("task_key") == "restart-work"
                    and detail.get("outcome") == "winner"
                ]
                resident_started = event_details(
                    conn,
                    "resident_started",
                )
                resident_errors = [
                    detail
                    for _, detail in event_details(conn, "resident_turn")
                    if detail.get("error")
                    == "dynamic owner wake has no current fenced claim"
                ]
                quarantines = event_details(
                    conn,
                    "resident_agent_quarantined",
                )
                restart_claim_timeouts = [
                    detail
                    for _, detail in event_details(
                        conn,
                        "orchestration_dynamic_claim_timeout",
                        run_id,
                    )
                    if detail.get("task_key") == "restart-work"
                ]
                old_bindings = [
                    detail
                    for _, detail in event_details(
                        conn,
                        "orchestration_dynamic_turn_bound",
                        run_id,
                    )
                    if detail.get("task_key") == "restart-work"
                    and int(detail.get("attempt") or 0) == 1
                ]
                executions = [
                    detail
                    for _, detail in event_details(
                        conn,
                        "orchestration_dynamic_execution",
                        run_id,
                    )
                    if detail.get("task_key") == "restart-work"
                ]

            assert after_events[: len(before_events)] == before_events
            assert old_turn["status"] == "failed"
            assert old_turn["error"] == RESTART_ERROR
            assert old_turn["finished_at"] is not None
            assert task["state"] == "done"
            assert task["owner"] == "worker"
            assert task["attempt"] == 2
            assert replacement_turn["status"] == "done"
            assert replacement_turn["agent"] == "worker"
            assert tuple(wake) == (0, None, 0, None)

            matching_summaries = [
                (ordinal, detail)
                for ordinal, detail in summaries
                if int(detail.get("stale_turns_terminated") or 0) == 1
            ]
            assert len(matching_summaries) == 1
            summary_ordinal, summary = matching_summaries[0]
            assert summary == {
                "body_present": False,
                "dispatches_requeued": 0,
                "orphan_owner_wakes": 0,
                "owner_wakes_retired": 2,
                "remaining_running_turns": 0,
                "scheduling_started": False,
                "stale_turns_terminated": 1,
                "tasks_reopened": 1,
            }
            assert len(turn_reconciled) == 1
            assert turn_reconciled[0][1]["turn_id"] == old_turn_id
            assert turn_reconciled[0][1]["attempt"] == 1
            assert turn_reconciled[0][1]["body_present"] is False
            assert len(wake_reconciled) == 2
            assert all(
                detail["body_present"] is False
                for _, detail in wake_reconciled
            )
            assert [detail["attempt"] for _, detail in claims] == [1, 2]
            replacement_claim_ordinal = claims[-1][0]
            second_start_ordinal = [
                ordinal
                for ordinal, _detail in resident_started
                if ordinal > summary_ordinal
            ][0]
            assert summary_ordinal < second_start_ordinal
            assert second_start_ordinal < replacement_claim_ordinal
            assert resident_errors == []
            assert quarantines == []
            assert restart_claim_timeouts == []
            assert len(old_bindings) == 1
            assert old_bindings[0]["turn_id"] == old_turn_id
            assert old_bindings[0]["binding_persisted"] is True
            assert len(executions) == 1
            assert executions[0]["attempt"] == 2
            assert executions[0]["turn_id"] == replacement_turn["id"]
            assert all(
                detail.get("body_present") is False
                for _, detail in turn_reconciled + wake_reconciled
            )

            print(
                json.dumps(
                    {
                        "scenario": "resident-startup-reconciliation",
                        "run_id": run_id,
                        "killed_turn_id": old_turn_id,
                        "killed_attempt": 1,
                        "replacement_turn_id": replacement_turn["id"],
                        "replacement_attempt": task["attempt"],
                        "historical_event_prefix_preserved": True,
                        "stale_turns_terminated": 1,
                        "owner_wakes_retired": 2,
                        "remaining_running_turns": 0,
                        "orphan_owner_wakes": 0,
                        "wake_loop_errors": 0,
                        "quarantines": 0,
                        "status": "PASS",
                    },
                    sort_keys=True,
                )
            )
        finally:
            kill_process_group(first)
            if second is not None:
                kill_process_group(second)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
