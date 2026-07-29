#!/usr/bin/env python3
"""Exercise dynamic native-turn lease renewal across the original claim TTL."""

from __future__ import annotations

import argparse
import json
import os
import re
import runpy
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
CLI = REPO / "swarm"


def run(
    *args: str,
    state_dir: Path,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    completed = subprocess.run(
        [str(CLI), *args],
        input=stdin,
        text=True,
        capture_output=True,
        cwd=REPO,
        env=env,
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

delay_seconds = float(sys.argv[1])
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
                "task_key": "slow-work",
                "title": "Complete one long native turn",
                "objective": "Stay alive beyond the original claim lease and commit once.",
                "required_capabilities": ["worker"],
                "acceptance": [
                    "The task completes under attempt one with visible lease renewals."
                ],
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
else:
    time.sleep(delay_seconds)
    payload = {
        "schema": "swarm.dynamic-task-progress.v1",
        "run_id": run_id,
        "graph_version": graph_version,
        "task_key": task_key,
        "attempt": attempt,
        "status": "complete",
        "summary": "long native turn completed under its original fenced claim",
        "checkpoint": {
            "acceptance_evidence": [
                {
                    "criterion_index": 0,
                    "evidence_ref": "trace://dynamic-turn-lease-renewed",
                }
            ]
        },
        "next_action": "",
    }
    completed = subprocess.run(
        [
            "swarm",
            "agent",
            "task-progress-commit",
            "--run-id",
            run_id,
            "--graph-version",
            str(graph_version),
            "--task-key",
            task_key,
            "--attempt",
            str(attempt),
        ],
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
    delay_seconds: float,
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
        f"{sys.executable} {runtime} {delay_seconds}",
        "--workspace",
        f"agents/{name}",
        "--capability",
        capability,
        state_dir=state_dir,
    )


def event_details(
    conn: sqlite3.Connection,
    event: str,
    run_id: str,
) -> list[dict[str, object]]:
    return [
        json.loads(row[0])
        for row in conn.execute(
            """
            SELECT detail FROM daemon_events
            WHERE event=? AND detail LIKE ?
            ORDER BY ordinal
            """,
            (event, f'%"run_id":"{run_id}"%'),
        ).fetchall()
    ]


def runtime_api(state_dir: Path) -> dict[str, object]:
    os.environ["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    return runpy.run_path(str(CLI), run_name="swarm_dynamic_lease_probe_api")


def active_owner(state_dir: Path, task_key: str) -> str:
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        rows = conn.execute(
            """
            SELECT agent_name FROM daemon_wakes
            WHERE running=1 AND active_reason LIKE ?
            """,
            (f"orchestration_dynamic_owner:%:{task_key}:%",),
        ).fetchall()
    if len(rows) != 1:
        raise AssertionError(
            f"expected one active owner for {task_key}, observed {rows}"
        )
    return str(rows[0][0])


def wait_for_running_task_turn(
    state_dir: Path,
    root_id: str,
    planner_turn_id: int,
) -> int:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with sqlite3.connect(state_dir / "state.sqlite3") as conn:
            row = conn.execute(
                """
                SELECT id FROM daemon_turns
                WHERE input_message_id=? AND id!=? AND status='running'
                ORDER BY id DESC LIMIT 1
                """,
                (root_id, planner_turn_id),
            ).fetchone()
        if row is not None:
            return int(row[0])
        time.sleep(0.05)
    raise AssertionError("long task turn did not enter running state")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--real-ttl",
        action="store_true",
        help="run for 65s with the production 60s lease and 15s heartbeat",
    )
    args = parser.parse_args()
    lease_seconds = 60 if args.real_ttl else 2
    heartbeat_seconds = 15 if args.real_ttl else 0.5
    delay_seconds = 65 if args.real_ttl else 3
    timeout_seconds = delay_seconds + 20

    with tempfile.TemporaryDirectory(prefix="swarm-dynamic-lease-") as raw:
        state_dir = Path(raw)
        runtime = state_dir / "runtime.py"
        write_runtime(runtime)
        register_agent(state_dir, "planner", runtime, delay_seconds, "planner")
        register_agent(state_dir, "worker", runtime, delay_seconds, "worker")
        sent = run(
            "message",
            "send",
            "--target",
            "#work",
            stdin="Exercise one task beyond the original claim lease.\n",
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
        run_id = re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)

        api = runtime_api(state_dir)
        runtime_globals = api["execute_daemon_turn"].__globals__
        runtime_globals["DYNAMIC_CLAIM_LEASE_SECONDS"] = lease_seconds
        runtime_globals[
            "DYNAMIC_TURN_HEARTBEAT_INTERVAL_SECONDS"
        ] = heartbeat_seconds
        assert api["settle_dynamic_candidate_wakes"]() >= 1
        planner_owner = active_owner(state_dir, "plan")
        planned = api["execute_daemon_turn"](
            planner_owner,
            None,
            None,
            None,
            None,
            None,
            timeout_seconds,
        )
        assert planned["ok"] is True
        planner_turn_id = int(planned["turn_id"])

        assert api["settle_dynamic_candidate_wakes"]() >= 1
        assert active_owner(state_dir, "slow-work") == "worker"
        result_box: dict[str, object] = {}

        def execute_slow_turn() -> None:
            result_box["result"] = api["execute_daemon_turn"](
                "worker",
                None,
                None,
                None,
                None,
                None,
                timeout_seconds,
            )

        turn_thread = threading.Thread(target=execute_slow_turn)
        turn_thread.start()
        task_turn_id = wait_for_running_task_turn(
            state_dir,
            root_id,
            planner_turn_id,
        )
        wrong_fence = api["renew_dynamic_turn_lease"](
            task_turn_id,
            "worker",
            run_id,
            1,
            "slow-work",
            2,
        )
        assert wrong_fence == {
            "renewed": False,
            "reason": "attempt_mismatch",
        }
        turn_thread.join(timeout=timeout_seconds + 5)
        assert not turn_thread.is_alive()
        assert result_box["result"]["ok"] is True

        with sqlite3.connect(state_dir / "state.sqlite3") as conn:
            conn.row_factory = sqlite3.Row
            task = conn.execute(
                """
                SELECT * FROM orchestration_tasks
                WHERE run_id=? AND task_key='slow-work'
                """,
                (run_id,),
            ).fetchone()
            turn = conn.execute(
                "SELECT * FROM daemon_turns WHERE id=?",
                (task_turn_id,),
            ).fetchone()
            renewals = event_details(
                conn,
                "orchestration_dynamic_turn_lease_renewed",
                run_id,
            )
            bindings = [
                detail
                for detail in event_details(
                    conn,
                    "orchestration_dynamic_turn_bound",
                    run_id,
                )
                if detail.get("task_key") == "slow-work"
            ]
            stopped = event_details(
                conn,
                "orchestration_dynamic_turn_lease_renewal_stopped",
                run_id,
            )
            renewal_errors = event_details(
                conn,
                "orchestration_dynamic_turn_lease_renewal_error",
                run_id,
            )
            expired = event_details(
                conn,
                "orchestration_dynamic_lease_expired",
                run_id,
            )
            claims = [
                detail
                for detail in event_details(
                    conn,
                    "orchestration_dynamic_claim",
                    run_id,
                )
                if detail.get("task_key") == "slow-work"
                and detail.get("outcome") == "winner"
            ]
            progress_rejections = event_details(
                conn,
                "orchestration_dynamic_progress_rejected",
                run_id,
            )

        minimum_renewals = 4 if args.real_ttl else 3
        assert task["state"] == "done"
        assert task["owner"] == "worker"
        assert task["attempt"] == 1
        assert task["turn_count"] == 1
        assert turn["status"] == "done"
        assert len(bindings) == 1
        assert bindings[0]["agent"] == "worker"
        assert bindings[0]["attempt"] == 1
        assert bindings[0]["turn_id"] == task_turn_id
        assert bindings[0]["binding_persisted"] is True
        assert bindings[0]["body_present"] is False
        assert len(renewals) >= minimum_renewals, (
            f"expected at least {minimum_renewals} renewals, observed "
            f"{len(renewals)}: {renewals}; stopped={stopped}; "
            f"errors={renewal_errors}"
        )
        assert {
            (row["agent"], row["attempt"], row["turn_id"])
            for row in renewals
        } == {("worker", 1, task_turn_id)}
        assert all(
            row["same_owner_attempt"] is True
            and row["turn_live"] is True
            and row["body_present"] is False
            for row in renewals
        )
        assert any(
            row["reason"] == "attempt_mismatch"
            and row["lease_mutated"] is False
            for row in stopped
        )
        assert expired == []
        assert len(claims) == 1
        assert claims[0]["attempt"] == 1
        assert progress_rejections == []

        # Restart recovery must finalize against the immutable binding created
        # with the turn, not a newer owner/attempt read from the task row.  A
        # copied state keeps the primary PASS trace intact while simulating a
        # takeover before the original process reports its late completion.
        with tempfile.TemporaryDirectory(
            prefix="swarm-dynamic-lease-fence-"
        ) as fence_raw:
            fence_state_dir = Path(fence_raw)
            shutil.copy2(
                state_dir / "state.sqlite3",
                fence_state_dir / "state.sqlite3",
            )
            with sqlite3.connect(
                fence_state_dir / "state.sqlite3"
            ) as fence_conn:
                fence_conn.execute(
                    """
                    UPDATE orchestration_tasks
                    SET owner='planner',
                        attempt=2,
                        state='running',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE run_id=? AND task_key='slow-work'
                    """,
                    (run_id,),
                )
                fence_conn.commit()
            fence_api = runtime_api(fence_state_dir)
            late_finalize = fence_api["finalize_daemon_turn"](
                task_turn_id,
                "worker",
                None,
                None,
                str(turn["target"]),
                0,
                False,
                "done",
                0,
                "SILENT",
                "",
                None,
                str(turn["session_id"]),
            )
            assert late_finalize["code"] == "DYNAMIC_TURN_REJECTED"
            assert (
                late_finalize["error"]
                == "task result rejected by current owner/attempt fence"
            )
            with sqlite3.connect(
                fence_state_dir / "state.sqlite3"
            ) as fence_conn:
                fence_conn.row_factory = sqlite3.Row
                taken_over = fence_conn.execute(
                    """
                    SELECT owner, attempt, state
                    FROM orchestration_tasks
                    WHERE run_id=? AND task_key='slow-work'
                    """,
                    (run_id,),
                ).fetchone()
                late_rejections = event_details(
                    fence_conn,
                    "orchestration_dynamic_commit_rejected",
                    run_id,
                )
            assert dict(taken_over) == {
                "owner": "planner",
                "attempt": 2,
                "state": "running",
            }
            assert late_rejections[-1]["attempt"] == 1
            assert late_rejections[-1]["turn_id"] == task_turn_id
            assert late_rejections[-1]["result_committed"] is False

        print(
            json.dumps(
                {
                    "scenario": "dynamic-turn-lease-renewal",
                    "mode": "real-ttl" if args.real_ttl else "accelerated",
                    "run_id": run_id,
                    "task_turn_id": task_turn_id,
                    "sleep_seconds": delay_seconds,
                    "lease_seconds": lease_seconds,
                    "heartbeat_seconds": heartbeat_seconds,
                    "renewal_count": len(renewals),
                    "attempt": task["attempt"],
                    "attempt_churn": 0,
                    "lease_expiry_takeovers": 0,
                    "stale_fence_mutations": 0,
                    "late_bound_attempt_finalize_fenced": True,
                    "status": "PASS",
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
