#!/usr/bin/env python3
"""Neutral probe for resumable owners and append-only task creation."""

from __future__ import annotations

import json
import os
import re
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
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
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
        r'''#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys

prompt = sys.stdin.read()
mode = os.environ["PROBE_MODE"]
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
    first_key = {
        "append": "research-work",
        "budget": "budget-work",
        "graph-budget": "graph-budget-parent",
    }[mode]
    payload = {
        "schema": "swarm.dynamic-tasks.v1",
        "run_id": run_id,
        "root_message_id": root_id,
        "graph_version": graph_version,
        "tasks": [
            {
                "task_key": first_key,
                "title": (
                    "Research the evidence"
                    if mode == "append"
                    else "Complete the bounded investigation"
                ),
                "objective": "Produce a verified result through resumable native turns.",
                "required_capabilities": (
                    ["researcher"] if mode == "append" else ["worker"]
                ),
                "acceptance": ["The final result is backed by durable milestone evidence."],
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
    raise SystemExit(0)

completed_turns = int(
    re.search(r"Completed native turns: (\d+)/(\d+)", prompt).group(1)
)
if mode == "append" and task_key == "research-work" and completed_turns == 1:
    create_payload = {
        "schema": "swarm.dynamic-task-create.v1",
        "run_id": run_id,
        "graph_version": graph_version,
        "parent_task_key": task_key,
        "tasks": [
            {
                "task_key": "verification-work",
                "title": "Verify the discovered evidence",
                "objective": "Independently verify the evidence discovered by the research owner.",
                "required_capabilities": ["verifier"],
                "acceptance": ["A concrete verification result is returned."],
                "phase": 1,
                "delivery_target": target,
            }
        ],
    }
    command = [
        "swarm", "agent", "task-create",
        "--run-id", run_id,
        "--graph-version", str(graph_version),
        "--parent-task-key", task_key,
        "--attempt", str(attempt),
    ]
    for expected_outcome in ("committed", "idempotent"):
        created = subprocess.run(
            command,
            input=json.dumps(create_payload, separators=(",", ":")),
            text=True,
            capture_output=True,
            check=False,
        )
        if created.returncode != 0 or f"Outcome: {expected_outcome}" not in created.stdout:
            sys.stderr.write(created.stdout)
            sys.stderr.write(created.stderr)
            raise SystemExit(1)
    conflicting_payload = json.loads(json.dumps(create_payload))
    conflicting_payload["tasks"][0]["objective"] = "A conflicting objective must not mutate the child."
    conflict = subprocess.run(
        command,
        input=json.dumps(conflicting_payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if conflict.returncode == 0 or "Code: TASK_CREATE_KEY_CONFLICT" not in conflict.stderr:
        sys.stderr.write(conflict.stdout)
        sys.stderr.write(conflict.stderr)
        raise SystemExit(1)
    different_payload = json.loads(json.dumps(create_payload))
    different_payload["tasks"][0]["task_key"] = "unexpected-same-turn-work"
    turn_conflict = subprocess.run(
        command,
        input=json.dumps(different_payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if (
        turn_conflict.returncode == 0
        or "Code: TASK_CREATE_TURN_CONFLICT" not in turn_conflict.stderr
    ):
        sys.stderr.write(turn_conflict.stdout)
        sys.stderr.write(turn_conflict.stderr)
        raise SystemExit(1)

if mode == "graph-budget":
    batch_index = completed_turns
    create_payload = {
        "schema": "swarm.dynamic-task-create.v1",
        "run_id": run_id,
        "graph_version": graph_version,
        "parent_task_key": task_key,
        "tasks": [
            {
                "task_key": f"queued-{batch_index}-{item}",
                "title": f"Queued verification {batch_index}-{item}",
                "objective": "Remain blocked behind the parent while graph bounds are verified.",
                "required_capabilities": ["worker"],
                "acceptance": ["A bounded verification result is returned."],
                "phase": 1,
                "delivery_target": target,
            }
            for item in range(8)
        ],
    }
    create_command = [
        "swarm", "agent", "task-create",
        "--run-id", run_id,
        "--graph-version", str(graph_version),
        "--parent-task-key", task_key,
        "--attempt", str(attempt),
    ]
    created = subprocess.run(
        create_command,
        input=json.dumps(create_payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed_turns < 3:
        if created.returncode != 0 or "Outcome: committed" not in created.stdout:
            sys.stderr.write(created.stdout)
            sys.stderr.write(created.stderr)
            raise SystemExit(1)
        status = "continue"
        summary = f"accepted bounded append batch {batch_index + 1}"
        next_action = f"append bounded batch {batch_index + 2}"
    else:
        if (
            created.returncode == 0
            or "Code: TASK_CREATE_GRAPH_BUDGET_EXCEEDED" not in created.stderr
        ):
            sys.stderr.write(created.stdout)
            sys.stderr.write(created.stderr)
            raise SystemExit(1)
        status = "held"
        summary = "global append budget rejected without graph mutation"
        next_action = "operator should review the bounded graph"
    checkpoint = {
        "completed_milestones": [f"append-batch-{batch_index + 1}"],
        "refs": [f"event://append-batch-{batch_index + 1}"],
    }
elif mode == "budget":
    status = "continue"
    summary = f"bounded milestone {completed_turns + 1}"
    next_action = f"continue with milestone {completed_turns + 2}"
    checkpoint = {
        "completed_milestones": [f"milestone-{completed_turns + 1}"],
        "refs": [f"artifact://milestone-{completed_turns + 1}"],
    }
elif task_key == "research-work" and completed_turns < 2:
    status = "continue"
    summary = f"research milestone {completed_turns + 1}"
    next_action = (
        "inspect the evidence gap and create verification work"
        if completed_turns == 0
        else "finish the research result"
    )
    checkpoint = {
        "completed_milestones": [f"research-{completed_turns + 1}"],
        "refs": [f"artifact://research-{completed_turns + 1}"],
    }
else:
    status = "complete"
    summary = f"verified completion for {task_key}"
    next_action = ""
    checkpoint = {
        "completed_milestones": [f"{task_key}-accepted"],
        "refs": [f"artifact://{task_key}-result"],
        "acceptance_evidence": [
            {
                "criterion_index": 0,
                "evidence_ref": f"artifact://{task_key}-result",
            }
        ],
    }

progress = {
    "schema": "swarm.dynamic-task-progress.v1",
    "run_id": run_id,
    "graph_version": graph_version,
    "task_key": task_key,
    "attempt": attempt,
    "status": status,
    "summary": summary,
    "checkpoint": checkpoint,
    "next_action": next_action,
}
command = [
    "swarm", "agent", "task-progress-commit",
    "--run-id", run_id,
    "--graph-version", str(graph_version),
    "--task-key", task_key,
    "--attempt", str(attempt),
]
if status == "complete":
    invalid_progress = json.loads(json.dumps(progress))
    invalid_progress["checkpoint"].pop("acceptance_evidence")
    rejected = subprocess.run(
        command,
        input=json.dumps(invalid_progress, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if (
        rejected.returncode == 0
        or "Code: PROGRESS_ACCEPTANCE_EVIDENCE_REQUIRED" not in rejected.stderr
    ):
        sys.stderr.write(rejected.stdout)
        sys.stderr.write(rejected.stderr)
        raise SystemExit(1)
for index in range(2):
    committed = subprocess.run(
        command,
        input=json.dumps(progress, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
    )
    if committed.returncode != 0:
        sys.stderr.write(committed.stdout)
        sys.stderr.write(committed.stderr)
        raise SystemExit(committed.returncode)
    expected_outcome = "idempotent" if index else None
    if expected_outcome and f"Outcome: {expected_outcome}" not in committed.stdout:
        sys.stderr.write(committed.stdout)
        raise SystemExit(1)
print("SILENT")
''',
        encoding="utf-8",
    )
    path.chmod(0o755)


def register_agent(
    state_dir: Path,
    name: str,
    runtime: Path,
    mode: str,
    capabilities: tuple[str, ...] = ("worker",),
) -> None:
    argv = [
        "agent",
        "register",
        "--name",
        name,
        "--display-name",
        name,
        "--runtime",
        f"env PROBE_MODE={mode} {sys.executable} {runtime}",
        "--workspace",
        f"agents/{name}",
    ]
    for capability in capabilities:
        argv.extend(["--capability", capability])
    run(*argv, state_dir=state_dir)


def send_root(state_dir: Path, body: str) -> str:
    sent = run(
        "message",
        "send",
        "--target",
        "#work",
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


def start_run(state_dir: Path, root_id: str) -> str:
    started = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#work",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    return re.search(r"Run ID: (orch_[a-z0-9]+)", started.stdout).group(1)


def resident(state_dir: Path, loops: int) -> None:
    run(
        "daemon",
        "resident",
        "--loops",
        str(loops),
        "--idle-interval",
        "0s",
        "--turn-timeout",
        "10s",
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
            "SELECT detail FROM daemon_events WHERE event=? ORDER BY ordinal",
            (event,),
        ).fetchall()
        if json.loads(row[0]).get("run_id") == run_id
    ]


def probe_append_and_resume(parent: Path, runtime: Path) -> dict[str, object]:
    state_dir = parent / "append-and-resume"
    state_dir.mkdir()
    register_agent(
        state_dir,
        "worker-1",
        runtime,
        "append",
        ("worker", "researcher"),
    )
    for name in ("worker-2", "worker-3"):
        register_agent(
            state_dir,
            name,
            runtime,
            "append",
            ("worker", "verifier"),
        )
    root_id = send_root(
        state_dir,
        "Research the evidence, create any verification work you discover, and finish.",
    )
    run_id = start_run(state_dir, root_id)

    # Separate resident invocations prove process restart does not discard
    # the task-owned session or checkpoint.
    resident(state_dir, 2)
    immutable_fields = (
        "task_key",
        "task_number",
        "title",
        "objective",
        "phase",
        "delivery_target",
        "required_capabilities_json",
        "acceptance_json",
        "max_turns",
        "parent_task_key",
        "creator_agent",
        "creator_turn_id",
        "creation_sha256",
        "created_at",
    )
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        parent_before = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key='research-work'
            """,
            (run_id,),
        ).fetchone()
        immutable_parent_before = tuple(parent_before[field] for field in immutable_fields)
        assert parent_before["turn_count"] == 1

    # This invocation appends a phase-1 child while the phase-0 parent remains
    # active, making the barrier state observable before either can finish.
    resident(state_dir, 1)
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        parent_after_create = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key='research-work'
            """,
            (run_id,),
        ).fetchone()
        child_before_unblock = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key='verification-work'
            """,
            (run_id,),
        ).fetchone()
        assert tuple(
            parent_after_create[field] for field in immutable_fields
        ) == immutable_parent_before
        assert child_before_unblock["phase"] == 1
        assert child_before_unblock["state"] == "blocked"
        assert child_before_unblock["owner"] is None
        assert event_details(
            conn,
            "orchestration_dynamic_notice",
            run_id,
        )[-1]["task_key"] != "verification-work"

    resident(state_dir, 8)

    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        run_row = conn.execute(
            "SELECT * FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        assert run_row["state"] == "done"
        tasks = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key!='plan'
            ORDER BY task_number
            """,
            (run_id,),
        ).fetchall()
        assert [row["task_key"] for row in tasks] == [
            "research-work",
            "verification-work",
        ]
        research, verification = tasks
        assert research["turn_count"] == 3
        assert research["state"] == "done"
        assert verification["state"] == "done"
        assert verification["parent_task_key"] == "research-work"
        assert verification["creator_agent"] == research["owner"]
        assert verification["owner"] != verification["creator_agent"]
        assert verification["attempt"] == 1
        assert verification["creation_sha256"]

        research_turns = conn.execute(
            """
            SELECT id,agent,session_id,status FROM daemon_turns
            WHERE input_message_id=? AND agent=? AND id!=?
            ORDER BY id
            """,
            (root_id, research["owner"], run_row["planner_turn_id"]),
        ).fetchall()
        research_turns = [
            row
            for row in research_turns
            if any(
                int(detail.get("turn_id") or 0) == int(row["id"])
                and detail.get("task_key") == "research-work"
                for detail in event_details(
                    conn,
                    "orchestration_dynamic_progress_committed",
                    run_id,
                )
            )
        ]
        assert len(research_turns) == 3
        assert len({row["session_id"] for row in research_turns}) == 1
        assert research["session_id"] == research_turns[0]["session_id"]
        assert len({research["owner"], verification["owner"]}) == 2

        progress_receipts = conn.execute(
            """
            SELECT receipt_kind FROM orchestration_receipts
            WHERE run_id=? AND task_key='research-work'
            ORDER BY committed_at,receipt_kind
            """,
            (run_id,),
        ).fetchall()
        kinds = [row["receipt_kind"] for row in progress_receipts]
        assert kinds.count("progress:1") == 1
        assert kinds.count("progress:2") == 1
        assert kinds.count("result") == 1
        assert sum(kind.startswith("tasks_created:") for kind in kinds) == 1

        created_events = event_details(
            conn,
            "orchestration_dynamic_tasks_created",
            run_id,
        )
        assert len(created_events) == 1
        assert created_events[0]["creator_agent"] == research["owner"]
        assert created_events[0]["creator_claimed_children"] == 0
        assert created_events[0]["append_only"] is True
        rejected_create = event_details(
            conn,
            "orchestration_dynamic_task_create_rejected",
            run_id,
        )
        assert [row["error_code"] for row in rejected_create] == [
            "TASK_CREATE_KEY_CONFLICT",
            "TASK_CREATE_TURN_CONFLICT",
        ]
        assert all(
            row["graph_mutated"] is False and row["body_present"] is False
            for row in rejected_create
        )
        progress_rejections = event_details(
            conn,
            "orchestration_dynamic_progress_rejected",
            run_id,
        )
        assert len(progress_rejections) == 2
        assert all(
            row["error_code"] == "PROGRESS_ACCEPTANCE_EVIDENCE_REQUIRED"
            and row["checkpoint_mutated"] is False
            and row["task_state_mutated"] is False
            and row["body_present"] is False
            for row in progress_rejections
        )

        child_notices = [
            row
            for row in event_details(
                conn,
                "orchestration_dynamic_notice",
                run_id,
            )
            if row.get("task_key") == "verification-work"
            and row.get("delivery_kind") == "metadata_only_open_task_notice"
        ]
        assert {row["agent"] for row in child_notices} == {
            "worker-2",
            "worker-3",
        }
        assert all(
            row["body_present"] is False
            and row["title_present"] is False
            and row["objective_present"] is False
            and row["acceptance_present"] is False
            for row in child_notices
        )
        losing_claims = [
            row
            for row in event_details(
                conn,
                "orchestration_dynamic_claim",
                run_id,
            )
            if row.get("task_key") == "verification-work"
            and row.get("agent") != verification["owner"]
            and row.get("outcome") == "conflict_stop"
        ]
        assert losing_claims
        assert all(
            row["body_reads"] == 0
            and row["full_model_turns"] == 0
            and row["outward_replies"] == 0
            and row["executions"] == 0
            for row in losing_claims
        )

        generic = conn.execute(
            "SELECT * FROM tasks WHERE number=?",
            (verification["task_number"],),
        ).fetchone()
        visible = conn.execute(
            "SELECT target,body FROM messages WHERE id=?",
            (generic["message_id"],),
        ).fetchone()
        assert visible["target"] == "#work"
        assert visible["body"] == "Verify the discovered evidence"
        assert generic["assignee"] == verification["owner"]
        assert generic["status"] == "in_review"

        child_result_receipts = conn.execute(
            """
            SELECT * FROM orchestration_receipts
            WHERE run_id=? AND task_key='verification-work'
              AND receipt_kind='result'
            """,
            (run_id,),
        ).fetchall()
        assert len(child_result_receipts) == 1
        assert child_result_receipts[0]["attempt"] == verification["attempt"]
        assert child_result_receipts[0]["author"] == verification["owner"]
        assert child_result_receipts[0]["delivery_target"] == f"#work:{root_id[:8]}"
        assert child_result_receipts[0]["root_message_id"] == root_id
        assert verification["result_sha256"]

        create_events_before = len(created_events)
        task_count_before = len(tasks)
    replay = run(
        "agent",
        "dynamic-start",
        "--channel",
        "#work",
        "--message-id",
        root_id[:8],
        state_dir=state_dir,
    )
    assert "already exists" in replay.stdout
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM orchestration_tasks
            WHERE run_id=? AND task_key!='plan'
            """,
            (run_id,),
        ).fetchone()[0] == task_count_before
        assert len(
            event_details(
                conn,
                "orchestration_dynamic_tasks_created",
                run_id,
            )
        ) == create_events_before
    return {
        "same_owner_attempt_three_turns": "PASS",
        "task_owned_session_survives_restart": "PASS",
        "typed_progress_idempotency": "PASS",
        "non_planner_owner_created_child": "PASS",
        "create_claim_separation": "PASS",
        "different_owner_won_child": "PASS",
        "append_replay_idempotency": "PASS",
        "visible_message_backed_child": "PASS",
        "complete_requires_acceptance_evidence": "PASS",
        "append_conflicts_rejected_and_audited": "PASS",
        "append_notice_fanout_metadata_only": "PASS",
        "declining_candidate_economics_zero": "PASS",
        "phase_barrier_observed": "PASS",
        "append_does_not_mutate_parent": "PASS",
        "child_fence_and_exact_receipt": "PASS",
    }


def probe_budget_hold(parent: Path, runtime: Path) -> dict[str, object]:
    state_dir = parent / "budget-hold"
    state_dir.mkdir()
    register_agent(state_dir, "worker-1", runtime, "budget")
    root_id = send_root(
        state_dir,
        "Investigate until the bounded continuation budget is exhausted.",
    )
    run_id = start_run(state_dir, root_id)
    resident(state_dir, 2)
    resident(state_dir, 10)
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        task = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key='budget-work'
            """,
            (run_id,),
        ).fetchone()
        run_row = conn.execute(
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        assert task["state"] == "held"
        assert task["progress_status"] == "held"
        assert task["turn_count"] == 8
        assert task["max_turns"] == 8
        assert run_row["state"] == "held"
        turns = [
            row
            for row in conn.execute(
                """
                SELECT id,agent,session_id FROM daemon_turns
                WHERE input_message_id=? AND agent=?
                ORDER BY id
                """,
                (root_id, task["owner"]),
            ).fetchall()
            if any(
                int(detail.get("turn_id") or 0) == int(row["id"])
                and detail.get("task_key") == "budget-work"
                for detail in event_details(
                    conn,
                    "orchestration_dynamic_progress_committed",
                    run_id,
                )
            )
        ]
        assert len(turns) == 8
        assert len({row["session_id"] for row in turns}) == 1
        receipts = conn.execute(
            """
            SELECT receipt_kind FROM orchestration_receipts
            WHERE run_id=? AND task_key='budget-work'
            ORDER BY committed_at,receipt_kind
            """,
            (run_id,),
        ).fetchall()
        kinds = [row["receipt_kind"] for row in receipts]
        assert sum(kind.startswith("progress:") for kind in kinds) == 7
        assert kinds.count("long_horizon_budget_exhausted") == 1
        held_events = event_details(
            conn,
            "orchestration_dynamic_task_held",
            run_id,
        )
        assert len(held_events) == 1
        assert held_events[0]["budget_exhausted"] is True
    return {
        "eight_turn_budget_holds_explicitly": "PASS",
        "budget_chain_same_session": "PASS",
        "one_terminal_budget_receipt": "PASS",
    }


def probe_append_graph_budget(parent: Path, runtime: Path) -> dict[str, object]:
    state_dir = parent / "append-graph-budget"
    state_dir.mkdir()
    register_agent(state_dir, "worker-1", runtime, "graph-budget")
    root_id = send_root(
        state_dir,
        "Grow a bounded task board and hold if the global append budget is reached.",
    )
    run_id = start_run(state_dir, root_id)
    resident(state_dir, 2)
    resident(state_dir, 5)
    with sqlite3.connect(state_dir / "state.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        parent_task = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key='graph-budget-parent'
            """,
            (run_id,),
        ).fetchone()
        run_row = conn.execute(
            "SELECT state FROM orchestration_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        graph_rows = conn.execute(
            """
            SELECT * FROM orchestration_tasks
            WHERE run_id=? AND task_key!='plan'
            ORDER BY task_number
            """,
            (run_id,),
        ).fetchall()
        assert parent_task["state"] == "held"
        assert parent_task["turn_count"] == 4
        assert run_row["state"] == "held"
        assert len(graph_rows) == 25
        assert sum(row["task_key"].startswith("queued-") for row in graph_rows) == 24
        assert all(
            row["state"] == "blocked" and row["owner"] is None
            for row in graph_rows
            if row["task_key"].startswith("queued-")
        )
        created_events = event_details(
            conn,
            "orchestration_dynamic_tasks_created",
            run_id,
        )
        assert len(created_events) == 3
        assert [row["graph_count"] for row in created_events] == [9, 17, 25]
        rejected = event_details(
            conn,
            "orchestration_dynamic_task_create_rejected",
            run_id,
        )
        budget_rejections = [
            row
            for row in rejected
            if row["error_code"] == "TASK_CREATE_GRAPH_BUDGET_EXCEEDED"
        ]
        assert len(budget_rejections) == 1
        assert budget_rejections[0]["graph_mutated"] is False
        assert budget_rejections[0]["tasks_created"] == 0
        assert budget_rejections[0]["body_present"] is False
        assert not conn.execute(
            """
            SELECT 1 FROM orchestration_tasks
            WHERE run_id=? AND task_key LIKE 'queued-3-%'
            """,
            (run_id,),
        ).fetchone()
    return {
        "append_global_budget_rejected": "PASS",
        "append_budget_rejection_audited": "PASS",
        "over_budget_append_mutates_zero_tasks": "PASS",
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="swarm-resumable-emergent-") as raw:
        parent = Path(raw)
        runtime = parent / "runtime.py"
        write_runtime(runtime)
        result = {
            "scenario": "resumable-emergent-task-board",
            **probe_append_and_resume(parent, runtime),
            **probe_budget_hold(parent, runtime),
            **probe_append_graph_budget(parent, runtime),
            "status": "PASS",
        }
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
