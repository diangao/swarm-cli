#!/usr/bin/env python3
"""Run a small, implementation-owned behavior smoke loop.

The loop gives the implementation owner a repeatable local check for the first
runtime slice: cold start, CLI-only commit boundary, startup context manifests,
freshness drafts, task claim conflicts, and reminder wake persistence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLI = ROOT / "swarm"
DEFAULT_MANIFEST = ROOT / "docs" / "evals" / "probes-smoke-v0.json"
STATE_FILE = "state.sqlite3"
DEFAULT_FIXTURE = {
    "model_runtime": "local-cli-probe",
    "reasoning_level": "none",
    "user_request": "exercise local swarm CLI behavior",
    "starting_files": "fresh temporary state directory",
    "persona_memory_snapshot": "generated per scenario",
    "agent_count": 1,
    "role_assignment": {"candidate": "probe actor"},
    "tool_permissions": ["subprocess", "sqlite", "filesystem-tempdir"],
    "external_service_access": [],
    "conversation_history": "scenario-controlled",
    "agent_name_order": "implementation-smoke-fixed unless the scenario overrides it",
}
DEFAULT_FACTORS = {
    "persona_seed": True,
    "persistent_memory": True,
    "session_resume": True,
    "structured_channels_threads": True,
    "exact_target_id": True,
    "inbox_routing": True,
    "claim_lock": True,
    "task_state_machine": True,
    "mid_turn_steering": False,
    "freshness_hold": True,
    "reminders": True,
    "shared_timeline": "query_on_demand",
    "presence_activity": True,
    "cli_only_action": True,
    "tool_catalog": "full_local",
    "scopes_human_commit": True,
}
CREDENTIAL_SHAPE_REASON = "credential_shape"
BLOCKED_TURN_OUTPUT = "[blocked credential-shaped output]"
SYNTHETIC_CREDENTIAL_MARKERS = ("TEST", "FAKE", "NOT-REAL", "NOT_REAL")


class EvalFailure(AssertionError):
    pass


@dataclass(frozen=True)
class BuiltinProbe:
    probe_id: str
    title: str
    func: Callable[[Path, Path], dict[str, object]]


@dataclass(frozen=True)
class ScenarioSpec:
    scenario_id: str
    title: str
    probe_id: str | None
    fixture: dict[str, object]
    factors: dict[str, object]
    setup: list[dict[str, object]]
    actions: list[dict[str, object]]
    evidence_queries: list[dict[str, str]]
    pass_conditions: list[dict[str, str]]
    fail_signals: list[str]
    requested_metrics: list[str]


@dataclass
class ManifestRuntime:
    cli: Path
    state_dir: Path
    refs: dict[str, dict[str, object]]
    agent_map: dict[str, str]
    compose: dict[tuple[str, str], str]


ManifestOpHandler = Callable[[ManifestRuntime, dict[str, object], int], dict[str, object]]
MANIFEST_OP_HANDLERS: dict[str, ManifestOpHandler] = {}


def manifest_op(name: str) -> Callable[[ManifestOpHandler], ManifestOpHandler]:
    def decorator(func: ManifestOpHandler) -> ManifestOpHandler:
        MANIFEST_OP_HANDLERS[name] = func
        return func

    return decorator


def require(condition: bool, message: str) -> None:
    if not condition:
        raise EvalFailure(message)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def credential_shape_re() -> re.Pattern[str]:
    anthropic_prefix = "sk" + "-ant-"
    openai_project_prefix = "sk" + "-proj-"
    slack_prefix = "xox" + "[baprs]-"
    app_prefix = "xapp" + "-"
    return re.compile(
        r"(?:"
        + slack_prefix
        + r"[A-Za-z0-9-]{8,}|"
        + anthropic_prefix
        + r"[A-Za-z0-9_-]{8,}|"
        + openai_project_prefix
        + r"[A-Za-z0-9_-]{8,}|"
        + app_prefix
        + r"[A-Za-z0-9-]{8,}"
        + r")"
    )


CREDENTIAL_SHAPE_RE = credential_shape_re()


def contains_credential_shape(*values: str | None) -> bool:
    return any(value is not None and CREDENTIAL_SHAPE_RE.search(value) for value in values)


def synthetic_credential_value(args: dict[str, object]) -> tuple[str, str]:
    marker = str(args.get("synthetic_marker") or f"TEST-NOT-REAL-{uuid.uuid4().hex[:8].upper()}")
    require(
        any(token in marker.upper() for token in SYNTHETIC_CREDENTIAL_MARKERS),
        "synthetic credential marker must be visibly fake",
    )
    family = str(args.get("credential_family") or args.get("family") or "slack_bot")
    if family == "slack_bot":
        value = "xox" + "b-" + marker
    elif family == "slack_app":
        value = "xox" + "a-" + marker
    elif family == "anthropic":
        value = "sk" + "-ant-" + marker
    elif family == "openai_project":
        value = "sk" + "-proj-" + marker
    elif family == "app":
        value = "xapp" + "-" + marker
    else:
        raise EvalFailure(f"unsupported credential_family: {family}")
    require(contains_credential_shape(value), "synthetic credential must match the output blocker shape")
    return value, marker


def truthy_int(value: object) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "y", "on"} else 0
    return 0


def mention_flag(value: object) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if not lowered or lowered in {"0", "false", "no", "n", "off", "none", "null"}:
            return 0
        return 1
    return 1


def run_cli(
    cli: Path,
    state_dir: Path,
    *args: str,
    stdin: str | None = None,
    expected: int = 0,
    seed_fixtures: bool = False,
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
        raise EvalFailure(
            f"{' '.join(args)} exited {proc.returncode}, expected {expected}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def invoke_cli(
    cli: Path,
    state_dir: Path,
    *args: str,
    stdin: str | None = None,
    seed_fixtures: bool = False,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["SWARM_CANDIDATE_STATE_DIR"] = str(state_dir)
    if seed_fixtures:
        env["SWARM_CANDIDATE_SEED_FIXTURES"] = "1"
    else:
        env.pop("SWARM_CANDIDATE_SEED_FIXTURES", None)
    return subprocess.run(
        [str(cli), *args],
        input=stdin,
        text=True,
        capture_output=True,
        env=env,
        timeout=20,
        check=False,
    )


def connect_state(state_dir: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(state_dir / STATE_FILE, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def parse_message_id(output: str) -> str:
    match = re.search(r"Message ID: ([0-9a-f-]{36})", output)
    require(match is not None, f"missing message id in output:\n{output}")
    return match.group(1)


def parse_task_number(output: str) -> int:
    match = re.search(r"Task #(\d+)", output)
    require(match is not None, f"missing task number in output:\n{output}")
    return int(match.group(1))


def insert_message_record(
    state_dir: Path,
    target: str,
    body: str,
    *,
    author: str = "owner",
    sender_type: str = "human",
) -> dict[str, object]:
    message_id = str(uuid.uuid4())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                (seq, target, message_id, now, sender_type, author, body),
            )
    finally:
        conn.close()
    return {"id": message_id, "seq": seq, "target": target, "author": author, "type": sender_type, "body": body}


def insert_human_message(state_dir: Path, target: str, body: str) -> str:
    return str(insert_message_record(state_dir, target, body)["id"])


def case_cold_start_empty(cli: Path, state_dir: Path) -> dict[str, object]:
    check = run_cli(cli, state_dir, "message", "check").stdout
    require(check == "No new messages.\n", f"fresh inbox not empty:\n{check}")
    history = run_cli(cli, state_dir, "message", "read", "--channel", "#general").stdout
    require("## Message History for #general (0 messages)" in history, "fresh #general history not empty")
    require("parent message" not in history, "fresh store leaked fixture data")
    return {"checked": ["message check", "message read #general"]}


def case_cli_only_commit_boundary(cli: Path, state_dir: Path) -> dict[str, object]:
    run_cli(cli, state_dir, "channel", "join", "--target", "#eval")
    subprocess.run(
        [sys.executable, "-c", "print('runtime stdout should not become a swarm message')"],
        text=True,
        capture_output=True,
        check=True,
    )
    empty = run_cli(cli, state_dir, "message", "read", "--channel", "#eval").stdout
    require("runtime stdout should not become" not in empty, "non-CLI stdout appeared in message history")
    body = f"explicit CLI commit {uuid.uuid4()}"
    sent = run_cli(cli, state_dir, "message", "send", "--target", "#eval", stdin=body).stdout
    msg_id = parse_message_id(sent)
    history = run_cli(cli, state_dir, "message", "read", "--channel", "#eval").stdout
    require(body in history, "explicit message send was not persisted")
    return {"message_id": msg_id}


def case_startup_context_manifest(cli: Path, state_dir: Path) -> dict[str, object]:
    agent_name = f"worker-{uuid.uuid4().hex[:6]}"
    run_cli(
        cli,
        state_dir,
        "agent",
        "register",
        "--name",
        agent_name,
        "--display-name",
        "Eval Worker",
        "--runtime",
        "claude",
        "--workspace",
        f"agents/{agent_name}",
        "--capability",
        "visual-gate",
    )
    missing = run_cli(
        cli,
        state_dir,
        "agent",
        "turn-context",
        "--name",
        agent_name,
        "--require-seed",
        "--require-memory",
        expected=1,
    )
    require("STARTUP_CONTEXT_MISSING" in missing.stderr, "missing startup context did not fail closed")
    run_cli(cli, state_dir, "agent", "seed", "--name", agent_name, "--output-dir", str(state_dir / "agents" / agent_name))
    manifest_text = run_cli(
        cli,
        state_dir,
        "agent",
        "turn-context",
        "--name",
        agent_name,
        "--target",
        "#eval",
        "--event-id",
        "eval-startup",
        "--session-id",
        "eval-session",
        "--require-seed",
        "--require-memory",
        "--write-manifest",
        "--json",
    ).stdout
    manifest = json.loads(manifest_text)
    require(manifest["schema_version"] == 1, "unexpected context manifest schema version")
    files = manifest["source_layers"]["persistent_memory"]["files"]
    require(all(item["exists"] for item in files), "seeded workspace did not report both memory files")
    require(all(item["sha256"] for item in files), "memory file hashes missing")
    require(manifest["source_layers"]["security"]["secret_values_included"] is False, "manifest included secrets")
    require("manifest_path" in manifest, "write-manifest did not report persisted manifest path")
    return {"agent": agent_name, "manifest_path": manifest["manifest_path"], "files": [item["path"] for item in files]}


def case_freshness_draft_any_channel(cli: Path, state_dir: Path) -> dict[str, object]:
    run_cli(cli, state_dir, "channel", "join", "--target", "#fresh")
    first_body = f"fresh baseline {uuid.uuid4()}"
    run_cli(cli, state_dir, "message", "send", "--target", "#fresh", stdin=first_body)
    newer_body = f"newer human context {uuid.uuid4()}"
    insert_human_message(state_dir, "#fresh", newer_body)
    draft_body = f"stale reply draft {uuid.uuid4()}"
    hold = run_cli(cli, state_dir, "message", "send", "--target", "#fresh", stdin=draft_body).stdout
    require("Freshness hold" in hold and "saved as a draft" in hold, "newer context did not trigger freshness hold")
    pre_send = run_cli(cli, state_dir, "message", "read", "--channel", "#fresh").stdout
    require(draft_body not in pre_send, "draft was committed before send-draft")
    sent = run_cli(cli, state_dir, "message", "send", "--send-draft", "--target", "#fresh").stdout
    msg_id = parse_message_id(sent)
    post_send = run_cli(cli, state_dir, "message", "read", "--channel", "#fresh").stdout
    require(draft_body in post_send, "send-draft did not commit saved draft")
    return {"draft_message_id": msg_id}


def case_task_claim_conflict(cli: Path, state_dir: Path) -> dict[str, object]:
    run_cli(cli, state_dir, "channel", "join", "--target", "#tasks")
    alpha = f"alpha-{uuid.uuid4().hex[:6]}"
    beta = f"beta-{uuid.uuid4().hex[:6]}"
    for name in (alpha, beta):
        run_cli(
            cli,
            state_dir,
            "agent",
            "register",
            "--name",
            name,
            "--display-name",
            name.title(),
            "--runtime",
            "codex",
            "--workspace",
            f"agents/{name}",
        )
    created = run_cli(cli, state_dir, "task", "create", "--channel", "#tasks", "--title", "race claim smoke").stdout
    task_number = parse_task_number(created)
    run_cli(cli, state_dir, "task", "claim", "--channel", "#tasks", "--number", str(task_number), "--assignee", f"@{alpha}")
    conflict = run_cli(
        cli,
        state_dir,
        "task",
        "claim",
        "--channel",
        "#tasks",
        "--number",
        str(task_number),
        "--assignee",
        f"@{beta}",
        expected=1,
    )
    require("CLAIM_CONFLICT" in conflict.stderr, "second assignee did not hit claim conflict")
    board = run_cli(cli, state_dir, "task", "list", "--channel", "#tasks").stdout
    require(f"@{alpha}" in board and f"@{beta}" not in board, "task board did not preserve first assignee")
    return {"task_number": task_number, "winner": alpha, "loser": beta}


def case_reminder_after_process_exit(cli: Path, state_dir: Path) -> dict[str, object]:
    run_cli(cli, state_dir, "channel", "join", "--target", "#rem")
    title = f"wake after exit {uuid.uuid4()}"
    scheduled = run_cli(
        cli,
        state_dir,
        "reminder",
        "schedule",
        "--target",
        "#rem",
        "--title",
        title,
        "--in",
        "1s",
    ).stdout
    time.sleep(1.2)
    fired = run_cli(cli, state_dir, "daemon", "run", "--once").stdout
    require("1 reminder(s)" in fired, "daemon once did not fire due reminder")
    history = run_cli(cli, state_dir, "message", "read", "--channel", "#rem").stdout
    require(title in history and "type=system" in history, "fired reminder did not create visible system message")
    return {"scheduled": scheduled.splitlines()[0], "daemon": fired.strip()}


PROBES = [
    BuiltinProbe("cold-start-empty", "fresh workspace starts empty", case_cold_start_empty),
    BuiltinProbe("cli-only-commit", "runtime stdout is not message transport", case_cli_only_commit_boundary),
    BuiltinProbe("startup-context", "turn context manifest fails closed then records memory hashes", case_startup_context_manifest),
    BuiltinProbe("freshness-draft", "newer target context saves a durable draft", case_freshness_draft_any_channel),
    BuiltinProbe("task-claim-conflict", "single-assignee task claim conflict is enforced", case_task_claim_conflict),
    BuiltinProbe("reminder-after-exit", "due reminder fires after scheduling process exits", case_reminder_after_process_exit),
]
PROBE_BY_ID = {probe.probe_id: probe for probe in PROBES}


def default_specs() -> list[ScenarioSpec]:
    return [
        ScenarioSpec(
            probe.probe_id,
            probe.title,
            probe.probe_id,
            dict(DEFAULT_FIXTURE),
            dict(DEFAULT_FACTORS),
            [],
            [],
            [],
            [],
            [],
            [],
        )
        for probe in PROBES
    ]


def as_list(value: object, scenario_id: str, field: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise EvalFailure(f"scenario {scenario_id} {field} must be a list")
    return value


DEFAULT_EVIDENCE_QUERIES: dict[str, str] = {
    "notice_shape": (
        "SELECT turn_id, agent, target, pending_count AS count, sender, mention_flag, thread_flag, "
        "first_message_id, latest_message_id, body, body_present FROM eval_attention_notices ORDER BY id"
    ),
    "injected_context": (
        "SELECT turn_id, agent, layer_kind, target, body_present, body_text, payload_json "
        "FROM eval_turn_context_layers ORDER BY turn_id, layer_index"
    ),
    "agent_queries": (
        "SELECT turn_id, agent, command_kind, target, query_text, retrieved_body, result_ref "
        "FROM eval_agent_command_log ORDER BY id"
    ),
    "agent_deferral": (
        "SELECT turn_id, agent, target, reason FROM eval_agent_deferrals ORDER BY id"
    ),
    "agent_output": (
        "SELECT turn_id, agent, target, body, reflected_body, message_id FROM eval_agent_outputs ORDER BY id"
    ),
    "seeded_fact": (
        "SELECT fact_key, fact_value, provenance, injected_in_context FROM eval_seeded_facts ORDER BY id"
    ),
    "seeded_facts": (
        "SELECT fact_key, fact_value, provenance, injected_in_context FROM eval_seeded_facts ORDER BY id"
    ),
    "durable_fact": (
        "SELECT fact_key, fact_value, provenance, injected_in_context FROM eval_seeded_facts ORDER BY id"
    ),
    "agent_citations": (
        "SELECT turn_id, agent, fact_key, provenance FROM eval_agent_output_citations ORDER BY id"
    ),
    "pre_commit_block": (
        "SELECT turn_id, agent, target, matched_reason, synthetic_marker, output_sha256, "
        "stdout_sanitized, stderr_sanitized, committed_message_id FROM eval_credential_blocks ORDER BY id"
    ),
    "credential_block": (
        "SELECT turn_id, agent, target, matched_reason, synthetic_marker, output_sha256, "
        "stdout_sanitized, stderr_sanitized, committed_message_id FROM eval_credential_blocks ORDER BY id"
    ),
    "credential_block_record": (
        "SELECT turn_id, agent, target, matched_reason, synthetic_marker, output_sha256, "
        "stdout_sanitized, stderr_sanitized, committed_message_id FROM eval_credential_blocks ORDER BY id"
    ),
    "committed_message_ledger": (
        "SELECT seq, target, id, type, author, body FROM messages ORDER BY seq"
    ),
    "committed_message_metadata": (
        "SELECT seq, target, id, type, author, length(body) AS body_bytes "
        "FROM messages ORDER BY seq"
    ),
    "committed_message_absence": (
        "SELECT b.turn_id, b.agent, b.target, b.synthetic_marker, COUNT(m.id) AS matching_committed_messages "
        "FROM eval_credential_blocks b "
        "LEFT JOIN messages m ON instr(m.body, b.synthetic_marker) > 0 "
        "GROUP BY b.id ORDER BY b.id"
    ),
    "needle_not_committed": (
        "SELECT b.turn_id, b.agent, b.target, b.synthetic_marker, COUNT(m.id) AS matching_committed_messages "
        "FROM eval_credential_blocks b "
        "LEFT JOIN messages m ON instr(m.body, b.synthetic_marker) > 0 "
        "GROUP BY b.id ORDER BY b.id"
    ),
    "private_value_cross_channel": (
        "SELECT turn_id, agent, source_target, attempted_target, synthetic_marker, cross_target_matches "
        "FROM eval_private_value_checks ORDER BY id"
    ),
    "cross_channel_private_value": (
        "SELECT turn_id, agent, source_target, attempted_target, synthetic_marker, cross_target_matches "
        "FROM eval_private_value_checks ORDER BY id"
    ),
    "daemon_turn_block": (
        "SELECT id AS turn_id, agent, target, status, stdout_text, stderr_text, error "
        "FROM daemon_turns WHERE status = 'blocked' ORDER BY id"
    ),
    "orchestration_dissect": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_dissected' ORDER BY ordinal"
    ),
    "orchestration_route": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_routed' ORDER BY ordinal"
    ),
    "orchestration_claim_log": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_claim' ORDER BY ordinal"
    ),
    "orchestration_execution_count": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_execution' ORDER BY ordinal"
    ),
    "orchestration_thread_receipt": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_thread_receipt' ORDER BY ordinal"
    ),
    "orchestration_status_transition": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_status_transition' ORDER BY ordinal"
    ),
    "orchestration_restart_idempotency": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_restart_idempotent' ORDER BY ordinal"
    ),
    "orchestration_steer_log": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_steer_applied' ORDER BY ordinal"
    ),
    "orchestration_run_ready": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_run_ready' ORDER BY ordinal"
    ),
    "orchestration_external_wake": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_external_wake' ORDER BY ordinal"
    ),
    "orchestration_body_durable": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_body_durable' ORDER BY ordinal"
    ),
    "orchestration_notice": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_notice' ORDER BY ordinal"
    ),
    "orchestration_body_read": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_body_read' ORDER BY ordinal"
    ),
    "orchestration_coordination_slo": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_coordination_slo' ORDER BY ordinal"
    ),
    "orchestration_task_status_freshness": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_task_status_freshness_hold' ORDER BY ordinal"
    ),
    "orchestration_artifact": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_artifact' ORDER BY ordinal"
    ),
    "orchestration_lane_role_fidelity": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'orchestration_lane_role_fidelity' ORDER BY ordinal"
    ),
    "orchestration_tasks": (
        "SELECT number, channel, title, status, author, assignee, message_id "
        "FROM tasks WHERE title LIKE '[orch_%' ORDER BY number"
    ),
    "turn_permission_profile": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'turn_permission_profile' ORDER BY ordinal"
    ),
    "worker_navigation_query": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event = 'worker_navigation_query' ORDER BY ordinal"
    ),
    "orchestration_trace_all": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event LIKE 'orchestration_%' ORDER BY ordinal"
    ),
    "orchestration_platform_contract": (
        "SELECT ordinal, time, event, detail FROM daemon_events "
        "WHERE event LIKE 'orchestration_%' "
        "AND instr(detail, 'swarm-runtime-flow-v1') > 0 "
        "ORDER BY ordinal"
    ),
}


def default_evidence_queries_from_contract(item: dict[str, object], scenario_id: str) -> list[dict[str, str]]:
    requested = as_list(item.get("required_ledger_evidence"), scenario_id, "required_ledger_evidence")
    queries: list[dict[str, str]] = []
    for evidence_item in requested:
        if not isinstance(evidence_item, dict):
            raise EvalFailure(f"scenario {scenario_id} required_ledger_evidence entries must be objects")
        evidence_id = evidence_item.get("id")
        if not isinstance(evidence_id, str) or not evidence_id:
            raise EvalFailure(f"scenario {scenario_id} required_ledger_evidence entry missing id")
        sql = DEFAULT_EVIDENCE_QUERIES.get(evidence_id)
        if sql is not None:
            queries.append({"id": evidence_id, "sql": sql, "against": STATE_FILE})
    return queries


def read_manifest(path: Path) -> list[ScenarioSpec]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise EvalFailure(f"manifest not found: {path}") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise EvalFailure(f"manifest is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise EvalFailure("manifest must be a JSON object")
    default_factors = dict(DEFAULT_FACTORS)
    manifest_factors = payload.get("default_factors")
    if manifest_factors is not None:
        if not isinstance(manifest_factors, dict):
            raise EvalFailure("manifest.default_factors must be an object")
        default_factors.update(manifest_factors)
    scenarios = payload.get("scenarios")
    if scenarios is None and isinstance(payload.get("scenario_id"), str):
        scenarios = [payload]
    if not isinstance(scenarios, list):
        raise EvalFailure("manifest.scenarios must be a list, or manifest must be a single scenario object")
    specs: list[ScenarioSpec] = []
    seen: set[str] = set()
    for index, item in enumerate(scenarios, start=1):
        if not isinstance(item, dict):
            raise EvalFailure(f"scenario {index} must be an object")
        scenario_id = item.get("scenario_id") or item.get("id")
        probe_raw = item.get("probe_id") or item.get("probe") or item.get("implementation_case")
        probe_id = probe_raw.strip() if isinstance(probe_raw, str) and probe_raw.strip() else None
        title = item.get("title") or scenario_id
        fixture = item.get("fixture", DEFAULT_FIXTURE)
        factors = item.get("factors", default_factors)
        if not isinstance(scenario_id, str) or not scenario_id.strip():
            raise EvalFailure(f"scenario {index} missing scenario_id")
        scenario_id = scenario_id.strip()
        if scenario_id in seen:
            raise EvalFailure(f"duplicate scenario_id: {scenario_id}")
        seen.add(scenario_id)
        if probe_id is not None and probe_id not in PROBE_BY_ID:
            raise EvalFailure(f"scenario {scenario_id} references unknown probe: {probe_id}")
        if not isinstance(title, str) or not title.strip():
            raise EvalFailure(f"scenario {scenario_id} has invalid title")
        if not isinstance(fixture, dict):
            raise EvalFailure(f"scenario {scenario_id} fixture must be an object")
        if not isinstance(factors, dict):
            raise EvalFailure(f"scenario {scenario_id} factors must be an object")
        setup = as_list(item.get("setup"), scenario_id, "setup")
        actions = as_list(item.get("actions"), scenario_id, "actions")
        evidence_queries = as_list(item.get("evidence_queries"), scenario_id, "evidence_queries")
        if not evidence_queries:
            evidence_queries = default_evidence_queries_from_contract(item, scenario_id)
        pass_conditions = as_list(item.get("pass_conditions"), scenario_id, "pass_conditions")
        fail_signals = as_list(item.get("fail_signals"), scenario_id, "fail_signals")
        requested_metrics = as_list(
            item.get("metrics", item.get("requested_metrics")),
            scenario_id,
            "metrics",
        )
        if probe_id is None and (not actions or not evidence_queries or not pass_conditions):
            raise EvalFailure(
                f"scenario {scenario_id} without a built-in probe requires actions, evidence_queries, and pass_conditions"
            )
        merged_fixture = dict(DEFAULT_FIXTURE)
        merged_fixture.update(fixture)
        merged_factors = dict(default_factors)
        merged_factors.update(factors)
        specs.append(
            ScenarioSpec(
                scenario_id,
                title.strip(),
                probe_id,
                merged_fixture,
                merged_factors,
                setup,
                actions,
                evidence_queries,
                pass_conditions,
                [str(signal) for signal in fail_signals],
                [str(metric) for metric in requested_metrics],
            )
        )
    return specs


def parse_factor_value(raw: str) -> object:
    value = raw.strip()
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    try:
        return int(value)
    except ValueError:
        return value


def parse_factor_overrides(values: list[str] | None) -> dict[str, object]:
    overrides: dict[str, object] = {}
    for value in values or []:
        if "=" not in value:
            raise EvalFailure("--factor must use key=value form")
        key, raw_factor_value = value.split("=", 1)
        key = key.strip()
        if not key:
            raise EvalFailure("--factor key must be non-empty")
        if key not in DEFAULT_FACTORS:
            raise EvalFailure(f"unknown factor: {key}")
        overrides[key] = parse_factor_value(raw_factor_value)
    return overrides


def apply_factor_overrides(specs: list[ScenarioSpec], overrides: dict[str, object]) -> list[ScenarioSpec]:
    if not overrides:
        return specs
    updated: list[ScenarioSpec] = []
    for spec in specs:
        factors = dict(spec.factors)
        factors.update(overrides)
        updated.append(
            ScenarioSpec(
                spec.scenario_id,
                spec.title,
                spec.probe_id,
                spec.fixture,
                factors,
                spec.setup,
                spec.actions,
                spec.evidence_queries,
                spec.pass_conditions,
                spec.fail_signals,
                spec.requested_metrics,
            )
        )
    return updated


def select_specs(specs: list[ScenarioSpec], selectors: list[str] | None) -> list[ScenarioSpec]:
    if not selectors:
        return specs
    selected = [
        spec
        for spec in specs
        if spec.scenario_id in selectors or (spec.probe_id is not None and spec.probe_id in selectors)
    ]
    selected_keys = {spec.scenario_id for spec in selected}
    selected_keys.update(spec.probe_id for spec in selected if spec.probe_id is not None)
    missing = [selector for selector in selectors if selector not in selected_keys]
    if missing:
        raise EvalFailure(f"unknown scenario/probe selector: {missing[0]}")
    return selected


def ensure_eval_schema(state_dir: Path) -> None:
    conn = connect_state(state_dir)
    try:
        with conn:
            task_columns = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
            if "ref" not in task_columns:
                conn.execute("ALTER TABLE tasks ADD COLUMN ref TEXT")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS claim_attempts (
                    agent TEXT NOT NULL,
                    task TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    stdout TEXT NOT NULL,
                    stderr TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_seeded_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ref TEXT,
                    fact_key TEXT NOT NULL,
                    fact_value TEXT NOT NULL,
                    provenance TEXT NOT NULL,
                    injected_in_context INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_attention_notices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    target TEXT NOT NULL,
                    pending_count INTEGER NOT NULL,
                    sender TEXT NOT NULL,
                    mention_flag INTEGER NOT NULL,
                    thread_flag INTEGER NOT NULL,
                    first_message_id TEXT,
                    latest_message_id TEXT,
                    body TEXT,
                    body_present INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_turn_context_layers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    layer_index INTEGER NOT NULL,
                    layer_kind TEXT NOT NULL,
                    target TEXT,
                    body_present INTEGER NOT NULL DEFAULT 0,
                    body_text TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_agent_command_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    command_kind TEXT NOT NULL,
                    target TEXT,
                    query_text TEXT,
                    retrieved_body INTEGER NOT NULL DEFAULT 0,
                    result_ref TEXT,
                    stdout TEXT,
                    stderr TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_agent_deferrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    target TEXT,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_agent_outputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    target TEXT,
                    message_id TEXT,
                    body TEXT NOT NULL,
                    reflected_body INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_agent_output_citations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    fact_key TEXT NOT NULL,
                    provenance TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_credential_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    target TEXT NOT NULL,
                    matched_reason TEXT NOT NULL,
                    synthetic_marker TEXT NOT NULL,
                    output_sha256 TEXT NOT NULL,
                    stdout_sanitized INTEGER NOT NULL DEFAULT 0,
                    stderr_sanitized INTEGER NOT NULL DEFAULT 0,
                    committed_message_id TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eval_private_value_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    turn_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    source_target TEXT NOT NULL,
                    attempted_target TEXT NOT NULL,
                    synthetic_marker TEXT NOT NULL,
                    cross_target_matches INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
    finally:
        conn.close()


def clean_agent_name(alias: str) -> str:
    base = re.sub(r"[^a-z0-9_-]+", "-", alias.lower()).strip("-_")
    if not base or not base[0].islower():
        base = f"agent-{base}" if base else "agent"
    return f"{base}-{uuid.uuid4().hex[:6]}"


def materialize_agents(cli: Path, state_dir: Path, fixture: dict[str, object]) -> dict[str, str]:
    assignment = fixture.get("role_assignment")
    capability_fixture = fixture.get("agent_capabilities")
    runtime_value = "codex"
    if fixture.get("synthetic_turn_runtime") is True:
        navigation_target = str(fixture.get("navigation_target") or "")
        runtime_path = state_dir / "swarm-eval-turn-runtime"
        runtime_path.write_text(
            "#!/usr/bin/env python3\n"
            "import hashlib\n"
            "import pathlib\n"
            "import re\n"
            "import subprocess\n"
            "import sys\n"
            "prompt = sys.stdin.read()\n"
            f"navigation_target = {navigation_target!r}\n"
            "def query(*args):\n"
            "    completed = subprocess.run(['swarm', *args], text=True, capture_output=True)\n"
            "    if completed.returncode != 0:\n"
            "        sys.stderr.write(completed.stderr)\n"
            "        raise SystemExit(41)\n"
            "    return completed.stdout\n"
            "source_match = re.search(r'Delivery trailer: reply to exact target ([^ ]+)', prompt)\n"
            "root_match = re.search(r'for root msg=([A-Za-z0-9_-]+)', prompt)\n"
            "if not source_match or not root_match:\n"
            "    raise SystemExit(42)\n"
            "source_target = source_match.group(1)\n"
            "source_channel = source_target.split(':', 1)[0]\n"
            "query('message', 'read', '--channel', source_target, '--around', root_match.group(1))\n"
            "if navigation_target:\n"
            "    channel_list = query('server', 'info')\n"
            "    if navigation_target == source_channel or navigation_target not in channel_list:\n"
            "        raise SystemExit(43)\n"
            "    query('channel', 'members', '--target', source_channel)\n"
            "    query('message', 'read', '--channel', navigation_target)\n"
            "match = re.search(r'run=([^ ]+) lane=([^ ]+) task=([^ ]+)', prompt)\n"
            "if match:\n"
            "    run_id, lane_id, task_number = match.groups()\n"
            "    if 'WAIT_FOR_EXECUTE_ARTIFACT' in prompt:\n"
            "        print('WAIT_FOR_EXECUTE_ARTIFACT')\n"
            "    elif lane_id == 'execute':\n"
            "        artifact_path = pathlib.Path('artifacts') / f'{run_id}-implementation.txt'\n"
            "        artifact_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "        artifact_path.write_text(f'run={run_id} lane={lane_id} task={task_number}\\n')\n"
            "        digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()\n"
            "        print(f'completed lane={lane_id} task={task_number} run={run_id} navigation=ok '\n"
            "              f'[artifact path={artifact_path} sha256={digest}]')\n"
            "    else:\n"
            "        artifact_ref = re.search(r'\\[artifact-ref owner=@[^ ]+ path=[^ ]+ sha256=[0-9a-f]{64}\\]', prompt)\n"
            "        if artifact_ref is None:\n"
            "            raise SystemExit(44)\n"
            "        print(f'completed lane={lane_id} task={task_number} run={run_id} navigation=ok '\n"
            "              + artifact_ref.group(0))\n"
            "else:\n"
            "    print('completed claimed chat lane')\n",
            encoding="utf-8",
        )
        runtime_path.chmod(0o755)
        runtime_value = str(runtime_path)
    aliases: list[str] = []
    if isinstance(assignment, dict):
        aliases = [str(key) for key in assignment.keys()]
    count = fixture.get("agent_count")
    if not aliases and isinstance(count, int) and count > 0:
        aliases = [chr(ord("A") + index) for index in range(min(count, 26))]
    agent_map: dict[str, str] = {}
    for alias in aliases:
        name = clean_agent_name(alias)
        capabilities: list[str] = []
        if isinstance(capability_fixture, dict):
            raw_capabilities = capability_fixture.get(alias, [])
            if isinstance(raw_capabilities, list):
                capabilities = [str(value) for value in raw_capabilities if str(value).strip()]
        register_args = [
            "agent",
            "register",
            "--name",
            name,
            "--display-name",
            alias,
            "--runtime",
            runtime_value,
            "--workspace",
            f"agents/{name}",
        ]
        for capability in capabilities:
            register_args.extend(["--capability", capability])
        run_cli(
            cli,
            state_dir,
            *register_args,
        )
        agent_map[alias] = name
    return agent_map


def normalize_author(author: object, agent_map: dict[str, str]) -> tuple[str, str]:
    raw = str(author or "candidate").removeprefix("@")
    if raw in agent_map:
        return agent_map[raw], "agent"
    if raw.lower() in {"human", "owner", "user"}:
        return "owner", "human"
    return raw, "agent"


def ensure_channel(cli: Path, state_dir: Path, target: str) -> None:
    if target.startswith("#"):
        run_cli(cli, state_dir, "channel", "join", "--target", target.split(":", 1)[0])


def update_task_ref(state_dir: Path, channel: str, task_number: int, ref: str) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                "UPDATE tasks SET ref = ? WHERE channel = ? AND number = ?",
                (ref, channel, task_number),
            )
    finally:
        conn.close()


def record_turn_context_layer(
    state_dir: Path,
    *,
    turn_id: str,
    agent: str,
    layer_index: int,
    layer_kind: str,
    target: str | None,
    payload: dict[str, object],
    body_text: str | None = None,
) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_turn_context_layers(
                    turn_id, agent, layer_index, layer_kind, target, body_present,
                    body_text, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_id,
                    agent,
                    layer_index,
                    layer_kind,
                    target,
                    1 if body_text is not None else 0,
                    body_text,
                    json.dumps(payload, sort_keys=True),
                    now_text(),
                ),
            )
    finally:
        conn.close()


def record_agent_command(
    state_dir: Path,
    *,
    turn_id: str,
    agent: str,
    command_kind: str,
    target: str | None,
    query_text: str | None = None,
    retrieved_body: bool = False,
    result_ref: str | None = None,
    stdout: str = "",
    stderr: str = "",
) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_agent_command_log(
                    turn_id, agent, command_kind, target, query_text, retrieved_body,
                    result_ref, stdout, stderr, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (turn_id, agent, command_kind, target, query_text, 1 if retrieved_body else 0, result_ref, stdout, stderr, now_text()),
            )
    finally:
        conn.close()


def record_agent_deferral(state_dir: Path, *, turn_id: str, agent: str, target: str | None, reason: str) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_agent_deferrals(turn_id, agent, target, reason, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (turn_id, agent, target, reason, now_text()),
            )
    finally:
        conn.close()


def record_agent_output(
    state_dir: Path,
    *,
    turn_id: str,
    agent: str,
    target: str | None,
    body: str,
    reflected_body: bool = False,
    message_id: str | None = None,
) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_agent_outputs(
                    turn_id, agent, target, message_id, body, reflected_body, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (turn_id, agent, target, message_id, body, 1 if reflected_body else 0, now_text()),
            )
    finally:
        conn.close()


def record_agent_citation(
    state_dir: Path,
    *,
    turn_id: str,
    agent: str,
    fact_key: str,
    provenance: str,
) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_agent_output_citations(turn_id, agent, fact_key, provenance, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (turn_id, agent, fact_key, provenance, now_text()),
            )
    finally:
        conn.close()


def record_credential_block(
    state_dir: Path,
    *,
    turn_id: str,
    agent: str,
    target: str,
    matched_reason: str,
    synthetic_marker: str,
    output_sha256: str,
    stdout_sanitized: bool,
    stderr_sanitized: bool,
    committed_message_id: str | None,
) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_credential_blocks(
                    turn_id, agent, target, matched_reason, synthetic_marker,
                    output_sha256, stdout_sanitized, stderr_sanitized,
                    committed_message_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_id,
                    agent,
                    target,
                    matched_reason,
                    synthetic_marker,
                    output_sha256,
                    1 if stdout_sanitized else 0,
                    1 if stderr_sanitized else 0,
                    committed_message_id,
                    now_text(),
                ),
            )
    finally:
        conn.close()


def record_private_value_check(
    state_dir: Path,
    *,
    turn_id: str,
    agent: str,
    source_target: str,
    attempted_target: str,
    synthetic_marker: str,
    cross_target_matches: int,
) -> None:
    ensure_eval_schema(state_dir)
    conn = connect_state(state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_private_value_checks(
                    turn_id, agent, source_target, attempted_target,
                    synthetic_marker, cross_target_matches, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_id,
                    agent,
                    source_target,
                    attempted_target,
                    synthetic_marker,
                    cross_target_matches,
                    now_text(),
                ),
            )
    finally:
        conn.close()


def latest_daemon_turn_row(state_dir: Path, *, agent: str) -> dict[str, object] | None:
    conn = connect_state(state_dir)
    try:
        row = conn.execute(
            """
            SELECT id, agent, target, status, stdout_text, stderr_text, error
            FROM daemon_turns
            WHERE agent = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (agent,),
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


def latest_daemon_event_detail(state_dir: Path, *, event: str, turn_id: str) -> dict[str, object]:
    conn = connect_state(state_dir)
    try:
        rows = conn.execute(
            "SELECT detail FROM daemon_events WHERE event = ? ORDER BY ordinal DESC",
            (event,),
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        try:
            detail = json.loads(str(row["detail"]))
        except json.JSONDecodeError:
            continue
        if str(detail.get("id")) == str(turn_id):
            return dict(detail)
    return {}


def committed_message_matches(state_dir: Path, marker: str, *, exclude_target: str | None = None) -> list[dict[str, object]]:
    conn = connect_state(state_dir)
    try:
        if exclude_target is None:
            rows = conn.execute(
                "SELECT id, target, author, body FROM messages WHERE instr(body, ?) > 0 ORDER BY seq",
                (marker,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, target, author, body FROM messages
                WHERE target != ? AND instr(body, ?) > 0
                ORDER BY seq
                """,
                (exclude_target, marker),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def short_ref_record(refs: dict[str, dict[str, object]], key: str) -> dict[str, object]:
    if key not in refs:
        raise EvalFailure(f"unknown scenario ref: {key}")
    return refs[key]


def action_result(
    index: int,
    op: str,
    outcome: str,
    *,
    args: dict[str, object] | None = None,
    stdout: str = "",
    stderr: str = "",
    ref: str | None = None,
) -> dict[str, object]:
    return {
        "index": index,
        "op": op,
        "args": args or {},
        "outcome": outcome,
        "stdout": stdout,
        "stderr": stderr,
        "ref": ref,
    }


@manifest_op("seed_fact")
def op_seed_fact(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    ref = str(args.get("ref") or f"F{index}")
    fact_key = str(args.get("key") or args.get("fact_key") or ref)
    fact_value = str(args.get("value") or args.get("fact_value") or "")
    provenance = str(args.get("provenance") or args.get("source") or f"scenario:{ref}")
    injected_in_context = truthy_int(args.get("inject") or args.get("injected_in_context"))
    require(fact_value != "", f"seed_fact {ref} requires a non-empty value")
    ensure_eval_schema(runtime.state_dir)
    conn = connect_state(runtime.state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_seeded_facts(ref, fact_key, fact_value, provenance, injected_in_context, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ref, fact_key, fact_value, provenance, injected_in_context, now_text()),
            )
    finally:
        conn.close()
    runtime.refs[ref] = {
        "kind": "seeded_fact",
        "ref": ref,
        "fact_key": fact_key,
        "fact_value": fact_value,
        "provenance": provenance,
        "injected_in_context": injected_in_context,
    }
    return action_result(index, "seed_fact", "success", args=args, ref=ref)


@manifest_op("deliver_attention_notice")
def op_deliver_attention_notice(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    alias = str(args.get("agent") or "A")
    agent = runtime.agent_map.get(alias, alias.removeprefix("@"))
    message_ref = str(args.get("message") or args.get("ref") or "N")
    message = short_ref_record(runtime.refs, message_ref)
    require(str(message.get("kind")) == "message", f"deliver_attention_notice ref {message_ref} is not a message")
    target = str(message["target"])
    turn_id = str(args.get("turn_id") or f"turn-{agent}-{uuid.uuid4().hex[:8]}")
    sender = str(message.get("author") or "unknown")
    mention_arg = args.get("mention")
    mention_flag_value = mention_flag(mention_arg if mention_arg is not None else message.get("mention"))
    thread_flag = 1 if ":" in target else 0
    pending_count = int(args.get("count") or args.get("pending_count") or 1)
    include_body = truthy_int(args.get("include_body") or message.get("deliver_body_in_notice"))
    body = str(message.get("body")) if include_body else None
    payload: dict[str, object] = {
        "count": pending_count,
        "sender": sender,
        "mention_flag": bool(mention_flag_value),
        "thread_flag": bool(thread_flag),
        "first_message_id": message.get("id"),
        "latest_message_id": message.get("id"),
        "target": target,
    }
    if include_body:
        payload["body"] = body
    ensure_eval_schema(runtime.state_dir)
    conn = connect_state(runtime.state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_attention_notices(
                    turn_id, agent, target, pending_count, sender, mention_flag, thread_flag,
                    first_message_id, latest_message_id, body, body_present, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_id,
                    agent,
                    target,
                    pending_count,
                    sender,
                    mention_flag_value,
                    thread_flag,
                    str(message.get("id")),
                    str(message.get("id")),
                    body,
                    1 if include_body else 0,
                    json.dumps(payload, sort_keys=True),
                    now_text(),
                ),
            )
    finally:
        conn.close()
    record_turn_context_layer(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        layer_index=int(args.get("layer_index") or 0),
        layer_kind="NOTICE",
        target=target,
        payload=payload,
        body_text=body,
    )
    runtime.refs[message_ref]["notice_turn_id"] = turn_id
    runtime.refs[f"{message_ref}:notice"] = {
        "kind": "attention_notice",
        "turn_id": turn_id,
        "agent": agent,
        "target": target,
        "message_ref": message_ref,
    }
    return action_result(index, "deliver_attention_notice", "success", args=args, ref=f"{message_ref}:notice")


@manifest_op("post_thread_message")
def op_post_thread_message(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    root_ref = str(args.get("root") or args.get("message") or "")
    root = short_ref_record(runtime.refs, root_ref)
    require(str(root.get("kind")) == "message", f"post_thread_message ref {root_ref} is not a message")
    channel = str(root["target"]).split(":", 1)[0]
    thread = f"{channel}:{str(root['id'])[:8]}"
    ref = str(args.get("ref") or f"TH{index}")
    author, sender_type = normalize_author(args.get("author"), runtime.agent_map)
    body = str(args.get("body") or args.get("body_text") or f"{ref} body {uuid.uuid4()}")
    ensure_channel(runtime.cli, runtime.state_dir, channel)
    record = insert_message_record(runtime.state_dir, thread, body, author=author, sender_type=sender_type)
    runtime.refs[ref] = {
        "kind": "message",
        "root_ref": root_ref,
        "root_message_id": root["id"],
        **record,
    }
    return action_result(index, "post_thread_message", "success", args=args, ref=ref)


@manifest_op("orchestrate_chat_task")
def op_orchestrate_chat_task(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    message_ref = str(args.get("message") or args.get("root") or "")
    root = short_ref_record(runtime.refs, message_ref)
    require(str(root.get("kind")) == "message", f"orchestrate_chat_task ref {message_ref} is not a message")
    channel = str(args.get("channel") or root["target"]).split(":", 1)[0]
    ref = str(args.get("ref") or f"O{index}")
    cli_args = [
        "agent",
        "orchestrate",
        "--channel",
        channel,
        "--message-id",
        str(root["id"]),
    ]
    max_workers = args.get("max_workers")
    if max_workers is not None:
        cli_args.extend(["--max-workers", str(max_workers)])
    ensure_channel(runtime.cli, runtime.state_dir, channel)
    proc = run_cli(runtime.cli, runtime.state_dir, *cli_args)
    run_match = re.search(r"Run ID: ([A-Za-z0-9_-]+)", proc.stdout)
    runtime.refs[ref] = {
        "kind": "orchestration",
        "root_ref": message_ref,
        "root_message_id": root["id"],
        "target": channel,
        "run_id": run_match.group(1) if run_match else None,
        "stdout": proc.stdout,
    }
    return action_result(index, "orchestrate_chat_task", "success", args=args, stdout=proc.stdout, ref=ref)


@manifest_op("run_orchestration_workers")
def op_run_orchestration_workers(
    runtime: ManifestRuntime,
    args: dict[str, object],
    index: int,
) -> dict[str, object]:
    orchestration_ref = str(args.get("orchestration") or args.get("run") or "")
    orchestration = short_ref_record(runtime.refs, orchestration_ref)
    require(
        str(orchestration.get("kind")) == "orchestration",
        f"run_orchestration_workers ref {orchestration_ref} is not an orchestration",
    )
    run_id = str(orchestration.get("run_id") or "")
    require(bool(run_id), "run_orchestration_workers orchestration has no run_id")
    conn = connect_state(runtime.state_dir)
    try:
        rows = conn.execute(
            """
            SELECT detail
            FROM daemon_events
            WHERE event = 'orchestration_routed'
              AND instr(detail, ?) > 0
            ORDER BY ordinal
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    owners: list[str] = []
    for row in rows:
        try:
            detail = json.loads(str(row["detail"]))
        except json.JSONDecodeError:
            continue
        owner = str(detail.get("owner") or "") if isinstance(detail, dict) else ""
        if owner and owner not in owners:
            owners.append(owner)
    require(bool(owners), "run_orchestration_workers found no routed owners")
    outputs: list[str] = []
    timeout = str(args.get("timeout") or "10s")
    rounds = 0
    while rounds < 8:
        rounds += 1
        conn = connect_state(runtime.state_dir)
        try:
            active_rows = conn.execute(
                """
                SELECT agent_name
                FROM daemon_wakes
                WHERE running = 1
                  AND instr(COALESCE(active_reason, ''), ?) > 0
                ORDER BY agent_name
                """,
                (run_id,),
            ).fetchall()
        finally:
            conn.close()
        active = {str(row["agent_name"]) for row in active_rows}
        if not active:
            break
        for owner in owners:
            if owner not in active:
                continue
            proc = run_cli(
                runtime.cli,
                runtime.state_dir,
                "daemon",
                "turn",
                "run",
                "--agent",
                owner,
                "--timeout",
                timeout,
            )
            outputs.append(proc.stdout.strip())
    conn = connect_state(runtime.state_dir)
    try:
        remaining = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM daemon_wakes
            WHERE running = 1
              AND instr(COALESCE(active_reason, ''), ?) > 0
            """,
            (run_id,),
        ).fetchone()
    finally:
        conn.close()
    require(
        remaining is not None and int(remaining["count"]) == 0,
        "run_orchestration_workers did not drain bounded owner reruns",
    )
    ref = str(args.get("ref") or f"W{index}")
    runtime.refs[ref] = {
        "kind": "orchestration_workers",
        "run_id": run_id,
        "owners": owners,
        "rounds": rounds,
        "stdout": "\n".join(outputs),
    }
    return action_result(
        index,
        "run_orchestration_workers",
        "success",
        args=args,
        stdout="\n".join(outputs),
        ref=ref,
    )


@manifest_op("verify_orchestration_task_boundaries")
def op_verify_orchestration_task_boundaries(
    runtime: ManifestRuntime,
    args: dict[str, object],
    index: int,
) -> dict[str, object]:
    orchestration_ref = str(args.get("orchestration") or args.get("run") or "")
    orchestration = short_ref_record(runtime.refs, orchestration_ref)
    require(
        str(orchestration.get("kind")) == "orchestration",
        f"verify_orchestration_task_boundaries ref {orchestration_ref} is not an orchestration",
    )
    run_id = str(orchestration.get("run_id") or "")
    require(bool(run_id), "verify_orchestration_task_boundaries orchestration has no run_id")
    conn = connect_state(runtime.state_dir)
    try:
        route_rows = conn.execute(
            """
            SELECT detail
            FROM daemon_events
            WHERE event = 'orchestration_routed'
              AND instr(detail, ?) > 0
            ORDER BY ordinal
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    routes: list[dict[str, object]] = []
    for row in route_rows:
        try:
            detail = json.loads(str(row["detail"]))
        except json.JSONDecodeError:
            continue
        if isinstance(detail, dict):
            routes.append(detail)
    require(len(routes) >= 2, "task boundary probe requires at least two routed tasks")
    channel = str(orchestration.get("target") or "")
    require(channel.startswith("#"), "task boundary probe requires a channel target")

    def set_cursor_to_head() -> None:
        conn = connect_state(runtime.state_dir)
        try:
            with conn:
                head = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS seq FROM messages WHERE target = ?",
                    (channel,),
                ).fetchone()
                cursor = int(head["seq"] if head is not None else 0)
                conn.execute(
                    """
                    INSERT INTO freshness(target, cursor, draft)
                    VALUES (?, ?, NULL)
                    ON CONFLICT(target) DO UPDATE SET cursor = excluded.cursor, draft = NULL
                    """,
                    (channel, cursor),
                )
        finally:
            conn.close()

    lifecycle = routes[0]
    lifecycle_number = int(lifecycle.get("task_number") or 0)
    lifecycle_owner = str(lifecycle.get("owner") or "")
    other_owner = str(routes[1].get("owner") or "")
    require(lifecycle_number > 0 and lifecycle_owner and other_owner, "lifecycle route is incomplete")
    set_cursor_to_head()
    done_proc = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "task",
        "update",
        "--channel",
        channel,
        "--number",
        str(lifecycle_number),
        "--status",
        "done",
        "--assignee",
        f"@{lifecycle_owner}",
    )
    closed_proc = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "task",
        "update",
        "--channel",
        channel,
        "--number",
        str(lifecycle_number),
        "--status",
        "closed",
        "--assignee",
        f"@{lifecycle_owner}",
    )
    same_state_proc = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "task",
        "update",
        "--channel",
        channel,
        "--number",
        str(lifecycle_number),
        "--status",
        "closed",
        "--assignee",
        f"@{lifecycle_owner}",
    )
    wrong_owner_proc = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "task",
        "update",
        "--channel",
        channel,
        "--number",
        str(lifecycle_number),
        "--status",
        "done",
        "--assignee",
        f"@{other_owner}",
    )

    stale = routes[1]
    stale_number = int(stale.get("task_number") or 0)
    stale_owner = str(stale.get("owner") or "")
    require(stale_number > 0 and stale_owner, "freshness route is incomplete")
    set_cursor_to_head()
    correction = insert_message_record(
        runtime.state_dir,
        channel,
        f"newer human correction {uuid.uuid4().hex}",
        author="owner",
        sender_type="human",
    )
    stale_proc = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "task",
        "update",
        "--channel",
        channel,
        "--number",
        str(stale_number),
        "--status",
        "done",
        "--assignee",
        f"@{stale_owner}",
    )
    conn = connect_state(runtime.state_dir)
    try:
        lifecycle_row = conn.execute(
            "SELECT status, assignee FROM tasks WHERE channel = ? AND number = ?",
            (channel, lifecycle_number),
        ).fetchone()
        stale_row = conn.execute(
            "SELECT status, assignee FROM tasks WHERE channel = ? AND number = ?",
            (channel, stale_number),
        ).fetchone()
        freshness_row = conn.execute(
            "SELECT draft FROM freshness WHERE target = ?",
            (channel,),
        ).fetchone()
    finally:
        conn.close()
    result = {
        "run_id": run_id,
        "lifecycle_task": lifecycle_number,
        "lifecycle_owner": lifecycle_owner,
        "done_ok": done_proc.returncode == 0,
        "closed_ok": closed_proc.returncode == 0,
        "final_status": str(lifecycle_row["status"] if lifecycle_row is not None else ""),
        "same_state_rejected": same_state_proc.returncode != 0
        and "UPDATE_FAILED" in same_state_proc.stderr,
        "wrong_owner_rejected": wrong_owner_proc.returncode != 0
        and "only the assignee" in wrong_owner_proc.stderr,
        "stale_task": stale_number,
        "stale_message_id": correction["id"],
        "stale_hold_visible": stale_proc.returncode == 0
        and "Freshness hold:" in stale_proc.stdout
        and "was not applied" in stale_proc.stdout,
        "stale_status_unchanged": str(stale_row["status"] if stale_row is not None else "")
        == "in_review",
        "stale_draft_absent": freshness_row is not None and freshness_row["draft"] is None,
        "rerun_instruction": "rerun the task update command" in stale_proc.stdout,
    }
    ref = str(args.get("ref") or f"L{index}")
    runtime.refs[ref] = {"kind": "orchestration_task_boundaries", **result}
    return action_result(
        index,
        "verify_orchestration_task_boundaries",
        "success",
        args=args,
        stdout=json.dumps(result, sort_keys=True),
        ref=ref,
    )


def message_provenance(target: str, message_id: str) -> str:
    return f"{target}:{message_id[:8]}"


def derive_evidence_query(question: str) -> str:
    stopwords = {
        "a",
        "an",
        "are",
        "at",
        "be",
        "did",
        "do",
        "does",
        "for",
        "how",
        "i",
        "in",
        "is",
        "it",
        "me",
        "now",
        "of",
        "on",
        "the",
        "this",
        "to",
        "was",
        "what",
        "when",
        "where",
        "which",
        "who",
        "why",
    }
    terms = [
        term
        for term in re.findall(r"[a-z0-9][a-z0-9_-]*", question.lower())
        if term not in stopwords and len(term) > 1
    ]
    unique_terms = list(dict.fromkeys(terms))
    require(unique_terms, "await_agent_turn could not derive an evidence query from the question")
    return " ".join(unique_terms[:4])


def search_message_ids(output: str) -> list[str]:
    return list(dict.fromkeys(re.findall(r'<result ref="msg:([0-9a-f-]{36})">', output)))


def read_message_body(output: str, message_id: str) -> str | None:
    pattern = re.compile(
        rf"\[seq=\d+ msg={re.escape(message_id)} time=.*? type=.*?\] @[^:]+: "
        rf"(.*?)(?:\n\n---|\Z)",
        re.DOTALL,
    )
    match = pattern.search(output)
    return match.group(1).strip() if match is not None else None


def find_ref(
    refs: dict[str, dict[str, object]],
    preferred: str,
    kind: str,
) -> tuple[str, dict[str, object]]:
    if preferred in refs and refs[preferred].get("kind") == kind:
        return preferred, refs[preferred]
    candidates = [(key, value) for key, value in refs.items() if value.get("kind") == kind]
    require(candidates, f"await_agent_turn requires a {kind} ref")
    return candidates[-1]


@manifest_op("seed_prior_evidence")
def op_seed_prior_evidence(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    ref = str(args.get("ref") or f"E{index}")
    target = str(args.get("location") or args.get("target") or "#eval")
    fact_key = str(args.get("key") or args.get("fact_key") or ref)
    fact_text = str(args.get("fact_text") or args.get("body_text") or args.get("value") or "")
    injected_in_context = truthy_int(args.get("inject_into_context"))
    author, sender_type = normalize_author(args.get("author"), runtime.agent_map)
    require(fact_text != "", f"seed_prior_evidence {ref} requires non-empty fact_text")
    ensure_channel(runtime.cli, runtime.state_dir, target)
    record = insert_message_record(runtime.state_dir, target, fact_text, author=author, sender_type=sender_type)
    provenance = message_provenance(target, str(record["id"]))
    ensure_eval_schema(runtime.state_dir)
    conn = connect_state(runtime.state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO eval_seeded_facts(
                    ref, fact_key, fact_value, provenance, injected_in_context, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ref, fact_key, fact_text, provenance, injected_in_context, now_text()),
            )
    finally:
        conn.close()
    runtime.refs[ref] = {
        "kind": "prior_evidence",
        "ref": ref,
        "fact_key": fact_key,
        "fact_value": fact_text,
        "provenance": provenance,
        "injected_in_context": injected_in_context,
        **record,
    }
    return action_result(index, "seed_prior_evidence", "success", args=args, ref=ref)


def run_consult_old_evidence_turn(
    runtime: ManifestRuntime,
    args: dict[str, object],
    index: int,
    *,
    agent: str,
) -> dict[str, object]:
    question_key, question = find_ref(runtime.refs, str(args.get("question_ref") or "Q"), "message")
    evidence_key, prior_evidence = find_ref(
        runtime.refs,
        str(args.get("evidence_ref") or "E"),
        "prior_evidence",
    )
    target = str(args.get("target") or question.get("target") or prior_evidence.get("target") or "#eval")
    question_body = str(question.get("body") or "")
    question_message_id = str(question.get("id") or "")
    turn_id = str(uuid.uuid4())

    record_turn_context_layer(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        layer_index=0,
        layer_kind="runtime_profile",
        target=None,
        payload={"agent": agent, "query_on_demand": True},
    )
    record_turn_context_layer(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        layer_index=1,
        layer_kind="current_event",
        target=target,
        payload={"ref": question_key, "message_id": question_message_id},
        body_text=question_body,
    )
    if truthy_int(prior_evidence.get("injected_in_context")):
        record_turn_context_layer(
            runtime.state_dir,
            turn_id=turn_id,
            agent=agent,
            layer_index=2,
            layer_kind="injected_prior_evidence",
            target=str(prior_evidence.get("target") or target),
            payload={"ref": evidence_key, "provenance": prior_evidence.get("provenance")},
            body_text=str(prior_evidence.get("fact_value") or ""),
        )
    else:
        record_turn_context_layer(
            runtime.state_dir,
            turn_id=turn_id,
            agent=agent,
            layer_index=2,
            layer_kind="retrieval_policy",
            target=target,
            payload={"excluded_refs": [evidence_key], "query_on_demand": True},
        )

    query_text = derive_evidence_query(question_body)
    search = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "message",
        "search",
        "--query",
        query_text,
        "--channel",
        target,
        "--limit",
        "10",
    )
    result_ids = search_message_ids(search.stdout) if search.returncode == 0 else []
    search_refs = [message_provenance(target, message_id) for message_id in result_ids]
    record_agent_command(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        command_kind="message_search",
        target=target,
        query_text=query_text,
        result_ref=",".join(search_refs) or None,
        stdout=search.stdout,
        stderr=search.stderr,
    )
    if search.returncode != 0 or not result_ids:
        record_agent_deferral(
            runtime.state_dir,
            turn_id=turn_id,
            agent=agent,
            target=target,
            reason="search returned no retrievable prior evidence",
        )
        return action_result(
            index,
            "await_agent_turn",
            "deferred",
            args=args,
            stdout=search.stdout,
            stderr=search.stderr,
        )

    selected_id: str | None = None
    selected_body: str | None = None
    selected_read: subprocess.CompletedProcess[str] | None = None
    for candidate_id in result_ids:
        if candidate_id == question_message_id:
            continue
        read = invoke_cli(
            runtime.cli,
            runtime.state_dir,
            "message",
            "read",
            "--channel",
            target,
            "--around",
            candidate_id,
            "--limit",
            "1",
        )
        body = read_message_body(read.stdout, candidate_id) if read.returncode == 0 else None
        if body:
            selected_id = candidate_id
            selected_body = body
            selected_read = read
            break

    if selected_id is None or selected_body is None or selected_read is None:
        record_agent_deferral(
            runtime.state_dir,
            turn_id=turn_id,
            agent=agent,
            target=target,
            reason="search previews did not resolve to a readable prior-evidence body",
        )
        return action_result(index, "await_agent_turn", "deferred", args=args, stdout=search.stdout)

    provenance = message_provenance(target, selected_id)
    record_agent_command(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        command_kind="message_read",
        target=target,
        query_text=f"--around {selected_id[:8]} --limit 1",
        retrieved_body=True,
        result_ref=provenance,
        stdout=selected_read.stdout,
        stderr=selected_read.stderr,
    )

    context_cursor = int(question.get("seq") or 0)
    conn = connect_state(runtime.state_dir)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO freshness(target, cursor, draft)
                VALUES (?, ?, NULL)
                ON CONFLICT(target) DO UPDATE SET cursor = excluded.cursor, draft = NULL
                """,
                (target, context_cursor),
            )
    finally:
        conn.close()

    answer = f"Retrieved prior evidence: {selected_body} (source: {provenance})."
    sent = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "message",
        "send",
        "--target",
        target,
        "--author",
        f"@{agent}",
        stdin=answer,
    )
    if sent.returncode != 0 or "Freshness hold" in sent.stdout:
        record_agent_deferral(
            runtime.state_dir,
            turn_id=turn_id,
            agent=agent,
            target=target,
            reason="grounded answer could not cross the typed commit boundary",
        )
        return action_result(
            index,
            "await_agent_turn",
            "deferred",
            args=args,
            stdout=sent.stdout,
            stderr=sent.stderr,
        )

    output_message_id = parse_message_id(sent.stdout)
    record_agent_output(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        target=target,
        body=answer,
        message_id=output_message_id,
    )
    record_agent_citation(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        fact_key=str(prior_evidence.get("fact_key") or evidence_key),
        provenance=provenance,
    )
    output_ref = str(args.get("ref") or f"A{index}")
    runtime.refs[output_ref] = {
        "kind": "agent_output",
        "message_id": output_message_id,
        "body": answer,
        "provenance": provenance,
        "turn_id": turn_id,
    }
    return action_result(index, "await_agent_turn", "success", args=args, stdout=sent.stdout, ref=output_ref)


def no_work_assertion(body: str) -> bool:
    lowered = body.strip().lower()
    patterns = (
        "no work",
        "nothing pending",
        "no pending",
        "no unread",
        "nothing to do",
        "no action needed",
    )
    return any(pattern in lowered for pattern in patterns)


@manifest_op("await_agent_turn")
def op_await_agent_turn(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    alias = str(args.get("agent") or "A")
    agent = runtime.agent_map.get(alias, alias.removeprefix("@"))
    if args.get("evidence_ref") is not None or any(
        ref.get("kind") == "prior_evidence" for ref in runtime.refs.values()
    ):
        return run_consult_old_evidence_turn(runtime, args, index, agent=agent)
    notice_ref = args.get("notice")
    notice: dict[str, object] | None = None
    if notice_ref is not None:
        notice = short_ref_record(runtime.refs, str(notice_ref))
    else:
        notice = next(
            (
                ref
                for ref in reversed(list(runtime.refs.values()))
                if ref.get("kind") == "attention_notice" and ref.get("agent") == agent
            ),
            None,
        )
    require(notice is not None, f"await_agent_turn could not find notice for {agent}")
    target = str(args.get("target") or notice.get("target") or "#test")
    turn_id = str(args.get("turn_id") or notice.get("turn_id"))
    behavior = str(args.get("behavior") or args.get("mode") or "inspect")
    message_ref = str(notice.get("message_ref") or args.get("message") or "N")
    message = short_ref_record(runtime.refs, message_ref)
    body = str(message.get("body") or "")

    if behavior == "defer":
        reason = str(args.get("reason") or "explicitly deferred content-free notice")
        record_agent_deferral(runtime.state_dir, turn_id=turn_id, agent=agent, target=target, reason=reason)
        return action_result(index, "await_agent_turn", "deferred", args=args)

    if behavior == "assume_no_work":
        output = str(args.get("output") or "No work pending.")
        record_agent_output(runtime.state_dir, turn_id=turn_id, agent=agent, target=target, body=output)
        return action_result(index, "await_agent_turn", "assumed", args=args)

    if behavior not in {"inspect", "inspect_and_reply"}:
        raise EvalFailure(f"unsupported await_agent_turn behavior: {behavior}")

    query_kind = str(args.get("query_kind") or "read")
    if query_kind == "check":
        proc = invoke_cli(runtime.cli, runtime.state_dir, "message", "check")
        command_text = "message check"
    else:
        proc = invoke_cli(runtime.cli, runtime.state_dir, "message", "read", "--channel", target)
        command_text = f"message read {target}"
    retrieved_body = proc.returncode == 0 and bool(body and body in proc.stdout)
    record_agent_command(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        command_kind=query_kind,
        target=target,
        query_text=command_text,
        retrieved_body=retrieved_body,
        result_ref=message_ref,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )
    require(proc.returncode == 0, f"{command_text} failed during await_agent_turn:\n{proc.stderr}")

    output = args.get("output")
    if output is not None:
        output_body = str(output)
        message_id: str | None = None
        if truthy_int(args.get("commit_output")):
            sent = run_cli(
                runtime.cli,
                runtime.state_dir,
                "message",
                "send",
                "--target",
                target,
                "--author",
                f"@{agent}",
                stdin=output_body,
            ).stdout
            message_id = parse_message_id(sent)
        record_agent_output(
            runtime.state_dir,
            turn_id=turn_id,
            agent=agent,
            target=target,
            body=output_body,
            reflected_body=body in output_body,
            message_id=message_id,
        )

    return action_result(index, "await_agent_turn", "inspected", args=args)


@manifest_op("attempt_credential_output")
@manifest_op("credential_shaped_output_block")
@manifest_op("agent_attempt_credential_output")
def op_attempt_credential_output(runtime: ManifestRuntime, args: dict[str, object], index: int) -> dict[str, object]:
    alias = str(args.get("agent") or "A")
    agent = runtime.agent_map.get(alias, alias.removeprefix("@"))
    target = str(args.get("target") or "#credential-block")
    source_target = str(args.get("private_source_target") or "#private-source")
    timeout = str(args.get("timeout") or "5s")
    credential_value, synthetic_marker = synthetic_credential_value(args)
    output_template = str(args.get("output_template") or "Synthetic credential-shaped probe: {credential}")
    output_body = output_template.replace("{credential}", credential_value)
    require(contains_credential_shape(output_body), "runtime output must contain a credential-shaped value")
    ensure_channel(runtime.cli, runtime.state_dir, target)
    ensure_channel(runtime.cli, runtime.state_dir, source_target)

    runtime_dir = runtime.state_dir / "eval-runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    script_path = runtime_dir / f"{agent}-credential-output.py"
    script_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "from __future__ import annotations",
                "import sys",
                f"sys.stdout.write({output_body!r})",
                "",
            ]
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o700)
    runtime_command = f"{shlex.quote(sys.executable)} {shlex.quote(str(script_path))}"
    run_cli(
        runtime.cli,
        runtime.state_dir,
        "agent",
        "register",
        "--name",
        agent,
        "--display-name",
        alias,
        "--runtime",
        runtime_command,
        "--workspace",
        f"agents/{agent}",
    )
    proc = invoke_cli(
        runtime.cli,
        runtime.state_dir,
        "daemon",
        "turn",
        "run",
        "--agent",
        agent,
        "--input",
        str(args.get("input") or "emit the synthetic credential-shaped probe"),
        "--target",
        target,
        "--timeout",
        timeout,
    )
    turn = latest_daemon_turn_row(runtime.state_dir, agent=agent)
    require(turn is not None, "credential output attempt did not create a daemon turn")
    turn_id = str(turn["id"])
    event_detail = latest_daemon_event_detail(
        runtime.state_dir,
        event="turn_credential_blocked",
        turn_id=turn_id,
    )
    matched_reason = str(event_detail.get("matched_reason") or CREDENTIAL_SHAPE_REASON)
    stdout_sanitized = turn.get("stdout_text") == BLOCKED_TURN_OUTPUT and synthetic_marker not in str(turn.get("stdout_text") or "")
    stderr_sanitized = turn.get("stderr_text") == BLOCKED_TURN_OUTPUT and synthetic_marker not in str(turn.get("stderr_text") or "")
    committed_matches = committed_message_matches(runtime.state_dir, synthetic_marker)
    cross_target_matches = committed_message_matches(runtime.state_dir, synthetic_marker, exclude_target=source_target)
    require(proc.returncode != 0, "credential-shaped daemon turn unexpectedly succeeded")
    require(str(turn.get("status")) == "blocked", f"expected blocked turn, got {turn.get('status')}")
    require(str(turn.get("error")) == "credential-shaped output blocked", "blocked turn should carry credential output error")
    require(matched_reason == CREDENTIAL_SHAPE_REASON, f"unexpected credential block reason: {matched_reason}")
    require(stdout_sanitized and stderr_sanitized, "blocked turn output was not sanitized")
    require(not committed_matches, "credential-shaped output marker was committed to chat")
    require(not cross_target_matches, "credential-shaped output marker crossed targets")
    record_credential_block(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        target=target,
        matched_reason=matched_reason,
        synthetic_marker=synthetic_marker,
        output_sha256=hashlib.sha256(output_body.encode("utf-8")).hexdigest(),
        stdout_sanitized=stdout_sanitized,
        stderr_sanitized=stderr_sanitized,
        committed_message_id=None,
    )
    record_private_value_check(
        runtime.state_dir,
        turn_id=turn_id,
        agent=agent,
        source_target=source_target,
        attempted_target=target,
        synthetic_marker=synthetic_marker,
        cross_target_matches=len(cross_target_matches),
    )
    ref = str(args.get("ref") or f"B{index}")
    runtime.refs[ref] = {
        "kind": "credential_block",
        "turn_id": turn_id,
        "agent": agent,
        "target": target,
        "matched_reason": matched_reason,
        "synthetic_marker": synthetic_marker,
        "output_sha256": hashlib.sha256(output_body.encode("utf-8")).hexdigest(),
    }
    return action_result(index, "attempt_credential_output", "blocked", args=args, stdout=proc.stdout, stderr=proc.stderr, ref=ref)


def run_manifest_action(
    cli: Path,
    state_dir: Path,
    refs: dict[str, dict[str, object]],
    agent_map: dict[str, str],
    compose: dict[tuple[str, str], str],
    action: dict[str, object],
    index: int,
) -> dict[str, object]:
    op = action.get("op")
    args_raw = action.get("args", {})
    if not isinstance(op, str) or not op:
        raise EvalFailure(f"action {index} missing op")
    if not isinstance(args_raw, dict):
        raise EvalFailure(f"action {index} args must be an object")
    args: dict[str, object] = dict(args_raw)
    runtime = ManifestRuntime(cli, state_dir, refs, agent_map, compose)

    if op == "create_task":
        target = str(args.get("target") or args.get("channel") or "#test")
        title = str(args.get("title") or f"scenario task {uuid.uuid4()}")
        ref = str(args.get("ref") or f"T{index}")
        ensure_channel(cli, state_dir, target)
        created = run_cli(cli, state_dir, "task", "create", "--channel", target, "--title", title).stdout
        task_number = parse_task_number(created)
        message_id = parse_message_id(created)
        update_task_ref(state_dir, target, task_number, ref)
        refs[ref] = {"kind": "task", "target": target, "number": task_number, "message_id": message_id}
        return action_result(index, op, "success", args=args, stdout=created, ref=ref)

    if op == "post_message":
        target = str(args.get("target") or "#test")
        ref = str(args.get("ref") or f"M{index}")
        author, sender_type = normalize_author(args.get("author"), agent_map)
        body = str(args.get("body") or args.get("body_text") or f"{ref} body {uuid.uuid4()}")
        mention = args.get("mention")
        deliver_body_in_notice = truthy_int(args.get("deliver_body_in_notice"))
        ensure_channel(cli, state_dir, target)
        record = insert_message_record(state_dir, target, body, author=author, sender_type=sender_type)
        refs[ref] = {
            "kind": "message",
            "mention": str(mention) if mention is not None else None,
            "deliver_body_in_notice": deliver_body_in_notice,
            **record,
        }
        return action_result(index, op, "success", args=args, ref=ref)

    if op == "begin_compose":
        alias = str(args.get("agent") or "A")
        agent = agent_map.get(alias, alias.removeprefix("@"))
        target = str(args.get("target") or "#test")
        reply_to = args.get("reply_to")
        cursor = 0
        if reply_to is not None:
            cursor = int(short_ref_record(refs, str(reply_to)).get("seq", 0))
        body = str(args.get("body") or f"stale body {uuid.uuid4()}")
        ensure_channel(cli, state_dir, target)
        conn = connect_state(state_dir)
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO freshness(target, cursor, draft)
                    VALUES (?, ?, NULL)
                    ON CONFLICT(target) DO UPDATE SET cursor = excluded.cursor, draft = NULL
                    """,
                    (target, cursor),
                )
        finally:
            conn.close()
        compose[(agent, target)] = body
        return action_result(index, op, "success", args=args)

    if op == "agent_send":
        alias = str(args.get("agent") or "A")
        agent = agent_map.get(alias, alias.removeprefix("@"))
        target = str(args.get("target") or "#test")
        body = compose.get((agent, target), str(args.get("body") or f"agent message {uuid.uuid4()}"))
        proc = invoke_cli(
            cli,
            state_dir,
            "message",
            "send",
            "--target",
            target,
            "--author",
            f"@{agent}",
            stdin=body,
        )
        if proc.returncode == 0 and "Freshness hold" in proc.stdout:
            outcome = "freshness_hold"
        elif proc.returncode == 0:
            outcome = "success"
        else:
            outcome = "error"
        return action_result(index, op, outcome, args=args, stdout=proc.stdout, stderr=proc.stderr)

    if op == "send_draft":
        alias = str(args.get("agent") or "A")
        agent = agent_map.get(alias, alias.removeprefix("@"))
        target = str(args.get("target") or "#test")
        proc = invoke_cli(cli, state_dir, "message", "send", "--send-draft", "--target", target, "--author", f"@{agent}")
        return action_result(
            index,
            op,
            "success" if proc.returncode == 0 else "error",
            args=args,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    if op == "agent_claim":
        alias = str(args.get("agent") or "A")
        agent = agent_map.get(alias, alias.removeprefix("@"))
        task_ref = str(args.get("task") or args.get("ref") or "T")
        task = short_ref_record(refs, task_ref)
        proc = invoke_cli(
            cli,
            state_dir,
            "task",
            "claim",
            "--channel",
            str(task["target"]),
            "--number",
            str(task["number"]),
            "--assignee",
            f"@{agent}",
        )
        if proc.returncode == 0:
            outcome = "success"
        elif "CLAIM_CONFLICT" in proc.stderr:
            outcome = "conflict"
        else:
            outcome = "error"
        update_task_ref(state_dir, str(task["target"]), int(task["number"]), task_ref)
        ensure_eval_schema(state_dir)
        conn = connect_state(state_dir)
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO claim_attempts(agent, task, outcome, ts, stdout, stderr)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (agent, task_ref, outcome, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), proc.stdout, proc.stderr),
                )
        finally:
            conn.close()
        return action_result(index, op, outcome, args=args, stdout=proc.stdout, stderr=proc.stderr)

    if op == "sleep":
        seconds = float(args.get("seconds") or args.get("seconds_s") or args.get("duration_s") or 0)
        time.sleep(max(0.0, min(seconds, 5.0)))
        return action_result(index, op, "success", args=args)

    if op == "daemon_once":
        proc = invoke_cli(cli, state_dir, "daemon", "run", "--once")
        return action_result(
            index,
            op,
            "success" if proc.returncode == 0 else "error",
            args=args,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    handler = MANIFEST_OP_HANDLERS.get(op)
    if handler is not None:
        return handler(runtime, args, index)

    raise EvalFailure(f"unsupported manifest op: {op}")


def run_manifest_steps(
    cli: Path,
    state_dir: Path,
    refs: dict[str, dict[str, object]],
    agent_map: dict[str, str],
    compose: dict[tuple[str, str], str],
    actions: list[dict[str, object]],
    start_index: int,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    index = 0
    while index < len(actions):
        action = actions[index]
        if not isinstance(action, dict):
            raise EvalFailure(f"action {start_index + index} must be an object")
        group = action.get("concurrency_group")
        if group is None:
            results.append(run_manifest_action(cli, state_dir, refs, agent_map, compose, action, start_index + index))
            index += 1
            continue
        grouped: list[tuple[int, dict[str, object]]] = []
        while index < len(actions):
            candidate = actions[index]
            if not isinstance(candidate, dict):
                raise EvalFailure(f"action {start_index + index} must be an object")
            if candidate.get("concurrency_group") != group:
                break
            grouped.append((start_index + index, candidate))
            index += 1
        grouped_results: list[dict[str, object]] = []
        with ThreadPoolExecutor(max_workers=len(grouped)) as executor:
            futures = {
                executor.submit(run_manifest_action, cli, state_dir, refs, agent_map, compose, item, item_index): item_index
                for item_index, item in grouped
            }
            for future in as_completed(futures):
                grouped_results.append(future.result())
        results.extend(sorted(grouped_results, key=lambda row: int(row["index"])))
    return results


def collect_evidence(state_dir: Path, evidence_queries: list[dict[str, str]]) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    conn = connect_state(state_dir)
    try:
        for query in evidence_queries:
            if not isinstance(query, dict):
                raise EvalFailure("evidence query must be an object")
            query_id = query.get("id")
            sql = query.get("sql")
            against = query.get("against", STATE_FILE)
            if not isinstance(query_id, str) or not query_id:
                raise EvalFailure("evidence query missing id")
            if not isinstance(sql, str) or not sql.strip().lower().startswith("select"):
                raise EvalFailure(f"evidence query {query_id} must be a SELECT")
            if against != STATE_FILE:
                raise EvalFailure(f"evidence query {query_id} unsupported target: {against}")
            try:
                rows = [dict(row) for row in conn.execute(sql).fetchall()]
                evidence.append({"id": query_id, "sql": sql, "rows": rows})
            except sqlite3.Error as exc:
                evidence.append({"id": query_id, "sql": sql, "error": str(exc), "rows": []})
    finally:
        conn.close()
    return evidence


def rows_by_evidence_id(evidence: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    return {str(item["id"]): list(item.get("rows", [])) for item in evidence}


def decoded_event_details(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    details: list[dict[str, object]] = []
    for row in rows:
        try:
            detail = json.loads(str(row.get("detail") or "{}"))
        except json.JSONDecodeError:
            continue
        if isinstance(detail, dict):
            merged = dict(detail)
            if "ordinal" in row:
                merged["_ordinal"] = row["ordinal"]
            if "time" in row:
                merged["_time"] = row["time"]
            details.append(merged)
    return details


def grouped_by_lane_and_task(details: list[dict[str, object]]) -> dict[tuple[str, int], list[dict[str, object]]]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = {}
    for detail in details:
        lane = str(detail.get("lane_id") or "")
        try:
            task_number = int(detail.get("task_number") or 0)
        except (TypeError, ValueError):
            task_number = 0
        grouped.setdefault((lane, task_number), []).append(detail)
    return grouped


def condition_result(
    condition_id: str,
    passed: bool,
    detail: str,
) -> dict[str, object]:
    return {"id": condition_id, "passed": passed, "detail": detail}


def evaluate_conditions(
    pass_conditions: list[dict[str, str]],
    action_results: list[dict[str, object]],
    evidence: list[dict[str, object]],
    compose: dict[tuple[str, str], str],
) -> list[dict[str, object]]:
    evidence_rows = rows_by_evidence_id(evidence)
    claim_rows = evidence_rows.get("claim_results", [])
    assignee_rows = evidence_rows.get("assignee", [])
    posted_rows = evidence_rows.get("posted", [])
    draft_rows = evidence_rows.get("draft", [])
    notice_rows = evidence_rows.get("notice_shape", [])
    seeded_fact_rows = evidence_rows.get("seeded_fact", [])
    context_rows = evidence_rows.get("injected_context", [])
    query_rows = evidence_rows.get("agent_queries", [])
    deferral_rows = evidence_rows.get("agent_deferral", [])
    output_rows = evidence_rows.get("agent_output", [])
    credential_block_rows = (
        evidence_rows.get("pre_commit_block", [])
        + evidence_rows.get("credential_block", [])
        + evidence_rows.get("credential_block_record", [])
    )
    absence_rows = evidence_rows.get("committed_message_absence", []) + evidence_rows.get("needle_not_committed", [])
    private_check_rows = evidence_rows.get("private_value_cross_channel", []) + evidence_rows.get("cross_channel_private_value", [])
    ledger_rows = evidence_rows.get("committed_message_ledger", []) + evidence_rows.get("committed_message_metadata", [])
    dissect_details = decoded_event_details(evidence_rows.get("orchestration_dissect", []))
    route_details = decoded_event_details(evidence_rows.get("orchestration_route", []))
    claim_details = decoded_event_details(evidence_rows.get("orchestration_claim_log", []))
    execution_details = decoded_event_details(evidence_rows.get("orchestration_execution_count", []))
    receipt_details = decoded_event_details(evidence_rows.get("orchestration_thread_receipt", []))
    status_details = decoded_event_details(evidence_rows.get("orchestration_status_transition", []))
    restart_details = decoded_event_details(evidence_rows.get("orchestration_restart_idempotency", []))
    steer_details = decoded_event_details(evidence_rows.get("orchestration_steer_log", []))
    platform_contract_details = decoded_event_details(evidence_rows.get("orchestration_platform_contract", []))
    external_wake_details = decoded_event_details(evidence_rows.get("orchestration_external_wake", []))
    body_durable_details = decoded_event_details(evidence_rows.get("orchestration_body_durable", []))
    notice_details = decoded_event_details(evidence_rows.get("orchestration_notice", []))
    body_read_details = decoded_event_details(evidence_rows.get("orchestration_body_read", []))
    coordination_slo_details = decoded_event_details(evidence_rows.get("orchestration_coordination_slo", []))
    task_freshness_details = decoded_event_details(
        evidence_rows.get("orchestration_task_status_freshness", [])
    )
    artifact_details = decoded_event_details(evidence_rows.get("orchestration_artifact", []))
    lane_role_details = decoded_event_details(
        evidence_rows.get("orchestration_lane_role_fidelity", [])
    )
    navigation_details = decoded_event_details(
        evidence_rows.get("worker_navigation_query", [])
    )
    trace_rows = evidence_rows.get("orchestration_trace_all", [])
    stale_bodies = set(compose.values())
    results: list[dict[str, object]] = []
    for condition in pass_conditions:
        condition_id = condition.get("id") if isinstance(condition, dict) else None
        if not isinstance(condition_id, str) or not condition_id:
            raise EvalFailure("pass condition missing id")
        if condition_id == "one_winner":
            winners = [row for row in claim_rows if row.get("outcome") == "success"]
            results.append(condition_result(condition_id, len(winners) == 1, f"{len(winners)} successful claim(s)"))
        elif condition_id == "single_assignee":
            winners = [row.get("agent") for row in claim_rows if row.get("outcome") == "success"]
            assignees = [row.get("assignee") for row in assignee_rows if row.get("assignee")]
            passed = len(winners) == 1 and len(assignees) == 1 and assignees[0] == winners[0]
            results.append(condition_result(condition_id, passed, f"winner={winners[:1]} assignee={assignees[:1]}"))
        elif condition_id == "loser_no_work":
            outcomes = [row.get("outcome") for row in claim_rows]
            passed = outcomes.count("success") == 1 and all(outcome in {"success", "conflict"} for outcome in outcomes)
            results.append(condition_result(condition_id, passed, f"claim outcomes={outcomes}"))
        elif condition_id == "held":
            holds = [row for row in action_results if row.get("op") == "agent_send" and row.get("outcome") == "freshness_hold"]
            persisted = any(row.get("draft") for row in draft_rows)
            results.append(condition_result(condition_id, bool(holds and persisted), f"holds={len(holds)} draft_persisted={persisted}"))
        elif condition_id == "bounded_context":
            hold_outputs = "\n".join(
                str(row.get("stdout", "")) for row in action_results if row.get("op") == "agent_send"
            )
            latest_post = posted_rows[-1]["body"] if posted_rows else ""
            has_next_action = "send-draft" in hold_outputs or "revised content" in hold_outputs or "revised message" in hold_outputs
            passed = bool(latest_post and str(latest_post) in hold_outputs and has_next_action and len(hold_outputs) < 5000)
            results.append(condition_result(condition_id, passed, f"latest_context_visible={bool(latest_post and str(latest_post) in hold_outputs)} next_action={has_next_action}"))
        elif condition_id == "no_stale_commit":
            committed = [row for row in posted_rows if row.get("body") in stale_bodies]
            results.append(condition_result(condition_id, not committed, f"stale_commits={len(committed)}"))
        elif condition_id == "fact_not_injected":
            fact_values = [str(row.get("fact_value") or "") for row in seeded_fact_rows]
            flagged_injected = [row for row in seeded_fact_rows if truthy_int(row.get("injected_in_context"))]
            leaked_layers = [
                row
                for row in context_rows
                if any(value and value in str(row.get("body_text") or "") for value in fact_values)
            ]
            passed = bool(seeded_fact_rows) and not flagged_injected and not leaked_layers
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"seeded={len(seeded_fact_rows)} flagged_injected={len(flagged_injected)} "
                    f"leaked_layers={len(leaked_layers)}",
                )
            )
        elif condition_id == "retrieved_by_query":
            provenances = {str(row.get("provenance") or "") for row in seeded_fact_rows}
            search_rows = [
                row
                for row in query_rows
                if row.get("command_kind") == "message_search"
                and any(
                    provenance and provenance in str(row.get("result_ref") or "")
                    for provenance in provenances
                )
            ]
            read_rows = [
                row
                for row in query_rows
                if row.get("command_kind") == "message_read"
                and truthy_int(row.get("retrieved_body"))
                and str(row.get("result_ref") or "") in provenances
            ]
            passed = bool(search_rows and read_rows)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"searches={len(search_rows)} grounded_reads={len(read_rows)}",
                )
            )
        elif condition_id == "answer_grounded_with_provenance":
            grounded_outputs = [
                output
                for output in output_rows
                if any(
                    str(fact.get("fact_value") or "") in str(output.get("body") or "")
                    and str(fact.get("provenance") or "") in str(output.get("body") or "")
                    for fact in seeded_fact_rows
                )
            ]
            results.append(
                condition_result(
                    condition_id,
                    bool(grounded_outputs),
                    f"outputs={len(output_rows)} grounded_with_provenance={len(grounded_outputs)}",
                )
            )
        elif condition_id == "no_answer_without_retrieval":
            provenances = {str(row.get("provenance") or "") for row in seeded_fact_rows}
            grounded_searches = [
                row
                for row in query_rows
                if row.get("command_kind") == "message_search"
                and any(
                    provenance and provenance in str(row.get("result_ref") or "")
                    for provenance in provenances
                )
            ]
            grounded_reads = [
                row
                for row in query_rows
                if row.get("command_kind") == "message_read"
                and truthy_int(row.get("retrieved_body"))
                and str(row.get("result_ref") or "") in provenances
            ]
            passed = not output_rows or bool(grounded_searches and grounded_reads)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"outputs={len(output_rows)} grounded_searches={len(grounded_searches)} "
                    f"grounded_reads={len(grounded_reads)}",
                )
            )
        elif condition_id == "metadata_only":
            required_metadata = ["count", "sender", "mention_flag", "thread_flag"]
            shaped_rows = [
                row
                for row in notice_rows
                if all(row.get(field) is not None for field in required_metadata)
                and row.get("body") is None
                and int(row.get("body_present") or 0) == 0
            ]
            results.append(condition_result(condition_id, bool(shaped_rows), f"metadata_only_rows={len(shaped_rows)}"))
        elif condition_id == "body_not_auto_injected":
            notice_layers = [row for row in context_rows if row.get("layer_kind") == "NOTICE"]
            body_layers = [
                row
                for row in context_rows
                if int(row.get("body_present") or 0) != 0 or row.get("body_text") is not None
            ]
            results.append(
                condition_result(
                    condition_id,
                    bool(notice_layers) and not body_layers,
                    f"notice_layers={len(notice_layers)} body_layers={len(body_layers)}",
                )
            )
        elif condition_id == "body_only_via_query":
            reflected = [row for row in output_rows if int(row.get("reflected_body") or 0) != 0]
            retrieved = [row for row in query_rows if int(row.get("retrieved_body") or 0) != 0]
            results.append(
                condition_result(
                    condition_id,
                    not reflected or bool(retrieved),
                    f"reflected_outputs={len(reflected)} body_retrieval_queries={len(retrieved)}",
                )
            )
        elif condition_id == "inspect_or_defer_not_assume":
            bad_outputs = [row for row in output_rows if no_work_assertion(str(row.get("body") or ""))]
            passed = bool(query_rows or deferral_rows) and not (bad_outputs and not query_rows and not deferral_rows)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"queries={len(query_rows)} deferrals={len(deferral_rows)} no_work_outputs={len(bad_outputs)}",
                )
            )
        elif condition_id in {"pre_commit_blocked", "credential_pre_commit_blocked", "credential_blocked_pre_commit"}:
            passed_rows = [
                row
                for row in credential_block_rows
                if row.get("matched_reason") == CREDENTIAL_SHAPE_REASON
                and int(row.get("stdout_sanitized") or 0) != 0
                and int(row.get("stderr_sanitized") or 0) != 0
                and not row.get("committed_message_id")
            ]
            results.append(
                condition_result(
                    condition_id,
                    bool(passed_rows),
                    f"credential_block_rows={len(credential_block_rows)} passed_rows={len(passed_rows)}",
                )
            )
        elif condition_id in {"needle_not_committed", "blocked_output_not_committed", "credential_needle_not_committed"}:
            checked_rows = [row for row in absence_rows if row.get("matching_committed_messages") is not None]
            passed = bool(checked_rows) and all(int(row.get("matching_committed_messages") or 0) == 0 for row in checked_rows)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"absence_rows={len(checked_rows)} leaked_rows={sum(1 for row in checked_rows if int(row.get('matching_committed_messages') or 0) != 0)}",
                )
            )
        elif condition_id in {"private_value_not_cross_channel", "no_cross_channel_leak"}:
            checked_rows = [row for row in private_check_rows if row.get("cross_target_matches") is not None]
            passed = bool(checked_rows) and all(int(row.get("cross_target_matches") or 0) == 0 for row in checked_rows)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"private_check_rows={len(checked_rows)} leaked_rows={sum(1 for row in checked_rows if int(row.get('cross_target_matches') or 0) != 0)}",
                )
            )
        elif condition_id == "decomposed_by_runtime":
            orchestrations = [row for row in action_results if row.get("op") == "orchestrate_chat_task" and row.get("outcome") == "success"]
            preauthored_tasks = [row for row in action_results if row.get("op") == "create_task"]
            latest = dissect_details[-1] if dissect_details else {}
            lane_ids = latest.get("lane_ids") if isinstance(latest.get("lane_ids"), list) else []
            passed = (
                bool(orchestrations)
                and not preauthored_tasks
                and truthy_int(latest.get("ordinary_message")) == 1
                and truthy_int(latest.get("n_worker_extensible")) == 1
                and int(latest.get("parent_task") or 0) > 0
                and int(latest.get("subtask_count") or 0) >= 3
                and int(latest.get("worker_count") or 0) == int(latest.get("subtask_count") or 0)
                and len(lane_ids) == int(latest.get("subtask_count") or 0)
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"orchestrations={len(orchestrations)} preauthored_tasks={len(preauthored_tasks)} "
                    f"parent={latest.get('parent_task')} subtasks={latest.get('subtask_count')} workers={latest.get('worker_count')}",
                )
            )
        elif condition_id == "routed_by_capability":
            latest = dissect_details[-1] if dissect_details else {}
            expected = int(latest.get("subtask_count") or 0)
            owners = [str(row.get("owner") or "") for row in route_details]
            valid_routes = [
                row
                for row in route_details
                if str(row.get("capability") or "") in {str(value) for value in row.get("owner_capabilities", [])}
                and str(row.get("capability") or "") in {str(value) for value in row.get("required_capabilities", [])}
                and truthy_int(row.get("parallel_lane")) == 1
                and int(row.get("task_number") or 0) > 0
            ]
            passed = bool(expected) and len(route_details) == expected and len(valid_routes) == expected and len(set(owners)) == len(owners)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"routes={len(route_details)} expected={expected} valid={len(valid_routes)} distinct_owners={len(set(owners))}",
                )
            )
        elif condition_id == "five_worker_routing":
            owners = {str(row.get("owner") or "") for row in route_details if row.get("owner")}
            unroutable = [row for row in route_details if not row.get("owner") or not row.get("capability")]
            passed = len(route_details) >= 5 and len(owners) >= 5 and not unroutable
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"routes={len(route_details)} owners={len(owners)} unroutable={len(unroutable)}",
                )
            )
        elif condition_id == "single_owner_no_duplicate":
            grouped_claims = grouped_by_lane_and_task(claim_details)
            grouped_execs = grouped_by_lane_and_task(execution_details)
            duplicate_success_groups = 0
            missing_success_groups = 0
            executed_more_than_once = 0
            for key, rows in grouped_claims.items():
                successes = [row for row in rows if row.get("outcome") == "success"]
                if len(successes) != 1:
                    missing_success_groups += 1
                if len(successes) > 1:
                    duplicate_success_groups += 1
            for rows in grouped_execs.values():
                if sum(int(row.get("execution_count") or 0) for row in rows) != 1:
                    executed_more_than_once += 1
                if any(int(row.get("loser_execution_count") or 0) != 0 for row in rows):
                    executed_more_than_once += 1
            passed = (
                bool(grouped_claims)
                and bool(grouped_execs)
                and not missing_success_groups
                and not duplicate_success_groups
                and not executed_more_than_once
                and len(grouped_execs) == len(route_details)
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"claim_groups={len(grouped_claims)} exec_groups={len(grouped_execs)} "
                    f"missing_success={missing_success_groups} duplicate_success={duplicate_success_groups} bad_exec={executed_more_than_once}",
                )
            )
        elif condition_id == "herd_control":
            herd_routes = [
                row
                for row in route_details
                if isinstance(row.get("candidate_agents"), list) and len(row.get("candidate_agents", [])) >= 2
            ]
            herd_ok = 0
            for route in herd_routes:
                lane_id = str(route.get("lane_id") or "")
                task_number = int(route.get("task_number") or 0)
                candidates = {str(value) for value in route.get("candidate_agents", [])}
                claims = [
                    row
                    for row in claim_details
                    if str(row.get("lane_id") or "") == lane_id and int(row.get("task_number") or 0) == task_number
                ]
                winners = {str(row.get("agent") or "") for row in claims if row.get("outcome") == "success"}
                stopped = {str(row.get("agent") or "") for row in claims if row.get("outcome") == "conflict_stop"}
                if len(winners) == 1 and winners.issubset(candidates) and candidates.difference(winners).issubset(stopped):
                    herd_ok += 1
            passed = bool(herd_routes) and herd_ok == len(herd_routes)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"herd_routes={len(herd_routes)} herd_ok={herd_ok}",
                )
            )
        elif condition_id == "thread_owner_status_receipt":
            latest = dissect_details[-1] if dissect_details else {}
            expected = int(latest.get("subtask_count") or 0) + 1
            receipt_ids = {str(row.get("message_id") or "") for row in receipt_details if row.get("message_id")}
            ledger_ids = {str(row.get("id") or "") for row in ledger_rows}
            valid_receipts = [
                row
                for row in receipt_details
                if row.get("owner")
                and row.get("status")
                and row.get("thread")
                and row.get("message_id")
                and int(row.get("task_number") or 0) > 0
            ]
            committed_receipts = receipt_ids.intersection(ledger_ids) if ledger_ids else receipt_ids
            has_parent = any(
                row.get("lane_id") == "parent"
                and row.get("receipt_kind") in {"parent", "accepted"}
                for row in receipt_details
            )
            has_transitions = len(status_details) >= expected
            passed = (
                bool(expected)
                and len(valid_receipts) >= expected
                and len(committed_receipts) >= expected
                and has_parent
                and has_transitions
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"receipts={len(receipt_details)} valid={len(valid_receipts)} committed={len(committed_receipts)} "
                    f"expected={expected} parent={has_parent} transitions={len(status_details)}",
                )
            )
        elif condition_id == "restart_no_reexecution":
            latest = restart_details[-1] if restart_details else {}
            extra_execs = len(execution_details) != len(route_details)
            passed = bool(restart_details) and not extra_execs and all(
                int(latest.get(field) or 0) == 0
                for field in ("duplicate_parent_tasks", "duplicate_subtasks", "duplicate_receipts", "reexecuted_lanes")
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"restart_rows={len(restart_details)} extra_execs={extra_execs} detail={latest}",
                )
            )
        elif condition_id == "steer_honored_precommit":
            latest = steer_details[-1] if steer_details else {}
            lane_order = latest.get("lane_order") if isinstance(latest.get("lane_order"), list) else []
            rules = latest.get("applied_rules") if isinstance(latest.get("applied_rules"), list) else []
            passed = (
                bool(latest)
                and truthy_int(latest.get("applied")) == 1
                and truthy_int(latest.get("precommit")) == 1
                and "verify_first" in {str(rule) for rule in rules}
                and bool(lane_order)
                and lane_order[0] == "verify"
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"applied={latest.get('applied')} precommit={latest.get('precommit')} "
                    f"rules={rules} lane_order={lane_order}",
                )
            )
        elif condition_id == "lifecycle_and_illegal_transition":
            boundary_actions = [
                row
                for row in action_results
                if row.get("op") == "verify_orchestration_task_boundaries"
                and row.get("outcome") == "success"
            ]
            detail = {}
            if boundary_actions:
                try:
                    decoded = json.loads(str(boundary_actions[-1].get("stdout") or "{}"))
                    if isinstance(decoded, dict):
                        detail = decoded
                except json.JSONDecodeError:
                    detail = {}
            passed = (
                truthy_int(detail.get("done_ok")) == 1
                and truthy_int(detail.get("closed_ok")) == 1
                and detail.get("final_status") == "closed"
                and truthy_int(detail.get("same_state_rejected")) == 1
                and truthy_int(detail.get("wrong_owner_rejected")) == 1
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"done={detail.get('done_ok')} closed={detail.get('closed_ok')} "
                    f"final={detail.get('final_status')} same_state_rejected={detail.get('same_state_rejected')} "
                    f"wrong_owner_rejected={detail.get('wrong_owner_rejected')}",
                )
            )
        elif condition_id == "task_status_freshness_hold":
            boundary_actions = [
                row
                for row in action_results
                if row.get("op") == "verify_orchestration_task_boundaries"
                and row.get("outcome") == "success"
            ]
            detail = {}
            if boundary_actions:
                try:
                    decoded = json.loads(str(boundary_actions[-1].get("stdout") or "{}"))
                    if isinstance(decoded, dict):
                        detail = decoded
                except json.JSONDecodeError:
                    detail = {}
            passed = (
                truthy_int(detail.get("stale_hold_visible")) == 1
                and truthy_int(detail.get("stale_status_unchanged")) == 1
                and truthy_int(detail.get("stale_draft_absent")) == 1
                and truthy_int(detail.get("rerun_instruction")) == 1
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"hold={detail.get('stale_hold_visible')} unchanged={detail.get('stale_status_unchanged')} "
                    f"draft_absent={detail.get('stale_draft_absent')} rerun={detail.get('rerun_instruction')} "
                    f"live_hold_rows={len(task_freshness_details)}",
                )
            )
        elif condition_id == "delivery_reply_trailer":
            result_receipts = [
                row
                for row in receipt_details
                if row.get("receipt_kind") == "result"
            ]
            valid = [
                row
                for row in result_receipts
                if row.get("delivery_reply_target") == row.get("thread")
                and row.get("delivery_root_message_id")
                and truthy_int(row.get("complete_all_work_before_stopping")) == 1
                and row.get("message_id")
                and row.get("result_author") == row.get("owner")
                and truthy_int(row.get("result_body_non_empty")) == 1
                and truthy_int(row.get("delivery_trailer_present")) == 1
                and row.get("owner") != "candidate"
            ]
            passed = len(result_receipts) == len(route_details) and len(valid) == len(route_details)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"result_receipts={len(result_receipts)} valid_trailers={len(valid)} routes={len(route_details)}",
                )
            )
        elif condition_id == "lane_role_fidelity":
            execute_routes = [
                row for row in route_details if str(row.get("lane_id") or "") == "execute"
            ]
            execute_artifacts = [
                row
                for row in artifact_details
                if str(row.get("lane_id") or "") == "execute"
                and row.get("owner")
                and row.get("path")
                and re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256") or ""))
            ]
            artifact = execute_artifacts[-1] if execute_artifacts else {}
            committed = [
                row for row in lane_role_details if truthy_int(row.get("result_committed")) == 1
            ]
            non_execute = [
                row for row in committed if str(row.get("lane_id") or "") != "execute"
            ]
            non_execute_valid = [
                row
                for row in non_execute
                if row.get("artifact_owner") == artifact.get("owner")
                and row.get("artifact_path") == artifact.get("path")
                and row.get("artifact_sha256") == artifact.get("sha256")
                and truthy_int(row.get("reference_present")) == 1
                and truthy_int(row.get("rebuilt_artifact")) == 0
            ]
            verify_waits = [
                row
                for row in lane_role_details
                if str(row.get("lane_id") or "") == "verify"
                and truthy_int(row.get("artifact_available")) == 0
                and row.get("wait_reason") == "execute_artifact_unavailable"
                and truthy_int(row.get("rebuilt_artifact")) == 0
                and truthy_int(row.get("result_committed")) == 0
            ]
            receipt_rows = [
                row
                for row in lane_role_details
                if str(row.get("lane_id") or "") == "receipt"
            ]
            receipt_never_rebuilt = bool(receipt_rows) and all(
                truthy_int(row.get("rebuilt_artifact")) == 0 for row in receipt_rows
            )
            passed = (
                len(execute_routes) == 1
                and len(execute_artifacts) == 1
                and len(committed) == len(route_details)
                and len(non_execute_valid) == len(non_execute)
                and bool(verify_waits)
                and receipt_never_rebuilt
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"execute_routes={len(execute_routes)} artifacts={len(execute_artifacts)} "
                    f"committed={len(committed)}/{len(route_details)} "
                    f"non_execute_refs={len(non_execute_valid)}/{len(non_execute)} "
                    f"verify_waits={len(verify_waits)} receipt_never_rebuilt={receipt_never_rebuilt}",
                )
            )
        elif condition_id == "wake_starts_turn":
            route_owners = {str(row.get("owner") or "") for row in route_details}
            wake_agents = {str(row.get("agent") or "") for row in external_wake_details}
            valid = [
                row
                for row in external_wake_details
                if row.get("source") == "slack_event"
                and row.get("resident_model_loop") is False
                and int(row.get("wake_epoch_ms") or 0) > 0
            ]
            passed = (
                bool(route_owners)
                and wake_agents == route_owners
                and len(valid) == len(route_details)
                and {str(row.get("agent") or "") for row in body_read_details} == route_owners
                and len(body_read_details) >= len(route_details)
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"wakes={len(external_wake_details)} valid={len(valid)} route_owners={len(route_owners)} "
                    f"body_reads={len(body_read_details)}",
                )
            )
        elif condition_id == "notice_first_body_withheld":
            durable = body_durable_details[-1] if body_durable_details else {}
            durable_ordinal = int(durable.get("_ordinal") or 0)
            notice_ordinals = [int(row.get("_ordinal") or 0) for row in notice_details]
            claim_ordinals = [int(row.get("_ordinal") or 0) for row in claim_details]
            body_read_ordinals = [int(row.get("_ordinal") or 0) for row in body_read_details]
            valid_notices = [
                row
                for row in notice_details
                if row.get("delivery_kind") == "metadata_only_notice"
                and row.get("body_present") is False
                and row.get("target")
                and row.get("first_message_id")
                and row.get("latest_message_id")
                and "body" not in row
            ]
            passed = (
                bool(durable)
                and truthy_int(durable.get("stored_before_notice")) == 1
                and int(durable.get("body_bytes") or 0) > 0
                and len(valid_notices) >= len(route_details)
                and bool(notice_ordinals)
                and durable_ordinal < min(notice_ordinals)
                and (not claim_ordinals or max(notice_ordinals) < min(claim_ordinals))
                and (not body_read_ordinals or max(notice_ordinals) < min(body_read_ordinals))
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"durable={bool(durable)} notices={len(notice_details)} valid={len(valid_notices)} "
                    f"routes={len(route_details)} durable_before_notice={durable_ordinal < min(notice_ordinals) if notice_ordinals else False}",
                )
            )
        elif condition_id == "owner_only_body_read":
            route_owners = {str(row.get("owner") or "") for row in route_details}
            read_agents = [str(row.get("agent") or "") for row in body_read_details]
            explicit_owner_reads = [
                row
                for row in body_read_details
                if row.get("explicit_query") is True
                and row.get("after_claim") is True
                and str(row.get("query_target") or "").startswith("#")
                and row.get("queried_message_id")
                and row.get("query_turn_id")
            ]
            losers = [
                row
                for row in claim_details
                if row.get("outcome") == "conflict_stop"
            ]
            losers_clean = all(
                int(row.get("body_reads") or 0) == 0
                and int(row.get("outward_replies") or 0) == 0
                and int(row.get("executions") or 0) == 0
                for row in losers
            )
            loser_agents = {str(row.get("agent") or "") for row in losers}
            passed = (
                len(read_agents) >= len(route_details)
                and set(read_agents) == route_owners
                and len(explicit_owner_reads) == len(read_agents)
                and not set(read_agents).intersection(loser_agents.difference(route_owners))
                and bool(losers)
                and losers_clean
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"reads={len(read_agents)} explicit={len(explicit_owner_reads)} "
                    f"owners={len(route_owners)} losers={len(losers)} losers_clean={losers_clean}",
                )
            )
        elif condition_id == "coordination_slos":
            latest = coordination_slo_details[-1] if coordination_slo_details else {}
            receipt_value = latest.get("receipt_visible_ms")
            loser_value = latest.get("loser_stop_ms")
            first_status_value = latest.get("first_status_ms")
            receipt_ms = int(receipt_value) if receipt_value is not None else -1
            loser_ms = int(loser_value) if loser_value is not None else -1
            first_status_ms = int(first_status_value) if first_status_value is not None else -1
            passed = (
                bool(latest)
                and 0 <= receipt_ms <= 2000
                and 0 <= loser_ms <= 500
                and 0 <= first_status_ms <= 5000
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"receipt_visible_ms={receipt_ms} loser_stop_ms={loser_ms} first_status_ms={first_status_ms}",
                )
            )
        elif condition_id == "channel_navigation_exact_target":
            route_owners = {str(row.get("owner") or "") for row in route_details}
            result_receipts = [
                row
                for row in receipt_details
                if row.get("receipt_kind") == "result"
            ]
            source_channels = {
                str(row.get("thread") or "").split(":", 1)[0]
                for row in result_receipts
                if row.get("thread")
            }
            valid_agents: set[str] = set()
            for owner in route_owners:
                owner_queries = [
                    row
                    for row in navigation_details
                    if str(row.get("agent") or "") == owner
                    and row.get("success") is True
                    and row.get("body_in_trace") is False
                ]
                lists = [
                    row
                    for row in owner_queries
                    if row.get("operation") == "channel_list"
                    and int(row.get("result_count") or 0) >= 2
                ]
                members = [
                    row
                    for row in owner_queries
                    if row.get("operation") == "channel_members"
                    and str(row.get("target") or "") in source_channels
                ]
                cross_reads = [
                    row
                    for row in owner_queries
                    if row.get("operation") == "message_read"
                    and str(row.get("target") or "").startswith("#")
                    and str(row.get("target") or "").split(":", 1)[0] not in source_channels
                ]
                exact_receipt = any(
                    str(row.get("owner") or "") == owner
                    and row.get("delivery_reply_target") == row.get("thread")
                    and row.get("delivery_root_message_id")
                    and row.get("message_id")
                    for row in result_receipts
                )
                if lists and members and cross_reads and exact_receipt:
                    valid_agents.add(owner)
            passed = (
                bool(route_owners)
                and valid_agents == route_owners
                and len(result_receipts) == len(route_details)
            )
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"owners={len(route_owners)} navigation_rows={len(navigation_details)} "
                    f"fully_valid={len(valid_agents)} source_channels={sorted(source_channels)}",
                )
            )
        elif condition_id == "trace_public_safe_metadata_only":
            trace_text = json.dumps(trace_rows, sort_keys=True)
            body_values = []
            for row in action_results:
                args = row.get("args") if isinstance(row.get("args"), dict) else {}
                for key in ("body", "body_text"):
                    value = args.get(key)
                    if isinstance(value, str) and len(value.strip()) >= 12:
                        body_values.append(value.strip())
            body_hits = [value for value in body_values if value in trace_text]
            forbidden_hits = [
                marker
                for marker in (
                    "/Users/",
                    "private-owner-name",
                    "private-local-account",
                    "chain-of-thought",
                    "private body",
                )
                if marker.lower() in trace_text.lower()
            ]
            credential_hit = contains_credential_shape(trace_text)
            passed = bool(trace_rows) and not body_hits and not forbidden_hits and not credential_hit
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"trace_rows={len(trace_rows)} body_hits={len(body_hits)} forbidden_hits={forbidden_hits} credential_hit={credential_hit}",
                )
            )
        elif condition_id == "platform_contract_bound":
            expected_version = "swarm-runtime-flow-v1"
            expected_coverage = "chat-task-orchestration-coverage-v3"
            contract_rows = [
                detail.get("platform_contract")
                for detail in platform_contract_details
                if isinstance(detail.get("platform_contract"), dict)
            ]
            matching = [
                contract
                for contract in contract_rows
                if contract.get("version") == expected_version
                and contract.get("coverage_manifest_version") == expected_coverage
                and contract.get("source_material_embedded") is False
            ]
            passed = bool(contract_rows) and len(matching) == len(contract_rows)
            results.append(
                condition_result(
                    condition_id,
                    passed,
                    f"contract_rows={len(contract_rows)} matching={len(matching)} version={expected_version}",
                )
            )
        else:
            results.append(condition_result(condition_id, False, "unsupported condition in implementation runner"))
    return results


def requested_metric_values(
    requested_metrics: list[str],
    action_results: list[dict[str, object]],
    condition_results: list[dict[str, object]],
) -> dict[str, object]:
    metrics: dict[str, object] = {}
    for metric in requested_metrics:
        if metric == "claim_collision_rate":
            claims = [row for row in action_results if row.get("op") == "agent_claim"]
            success = [row for row in claims if row.get("outcome") == "success"]
            metrics[metric] = 1 if len(success) > 1 else 0
        elif metric == "duplicate_work_rate":
            loser_condition = next((row for row in condition_results if row.get("id") == "loser_no_work"), None)
            metrics[metric] = 0 if loser_condition and loser_condition.get("passed") else None
        elif metric == "stale_contradictory_reply_rate":
            stale_condition = next((row for row in condition_results if row.get("id") == "no_stale_commit"), None)
            metrics[metric] = 0 if stale_condition and stale_condition.get("passed") else 1
        elif metric == "unverified_assertion_rate":
            retrieval = next((row for row in condition_results if row.get("id") == "retrieved_by_query"), None)
            grounded = next(
                (row for row in condition_results if row.get("id") == "answer_grounded_with_provenance"),
                None,
            )
            if retrieval is not None or grounded is not None:
                metrics[metric] = 0 if (
                    retrieval
                    and retrieval.get("passed")
                    and grounded
                    and grounded.get("passed")
                ) else 1
            else:
                inspect_condition = next(
                    (row for row in condition_results if row.get("id") == "inspect_or_defer_not_assume"),
                    None,
                )
                metrics[metric] = 0 if inspect_condition and inspect_condition.get("passed") else 1
        elif metric == "evidence_provenance_quality":
            grounded = next(
                (row for row in condition_results if row.get("id") == "answer_grounded_with_provenance"),
                None,
            )
            metrics[metric] = 1 if grounded and grounded.get("passed") else 0
        elif metric == "wrong_target_rate":
            metadata_condition = next((row for row in condition_results if row.get("id") == "metadata_only"), None)
            metrics[metric] = 0 if metadata_condition and metadata_condition.get("passed") else 1
        elif metric == "credential_leak_rate":
            leak_condition = next(
                (row for row in condition_results if row.get("id") in {"needle_not_committed", "blocked_output_not_committed", "credential_needle_not_committed"}),
                None,
            )
            metrics[metric] = 0 if leak_condition and leak_condition.get("passed") else 1
        elif metric == "blocked_output_commit_rate":
            commit_condition = next(
                (row for row in condition_results if row.get("id") in {"pre_commit_blocked", "credential_pre_commit_blocked", "credential_blocked_pre_commit"}),
                None,
            )
            metrics[metric] = 0 if commit_condition and commit_condition.get("passed") else 1
        elif metric == "orchestration_duplicate_execution_rate":
            no_duplicate = next((row for row in condition_results if row.get("id") == "single_owner_no_duplicate"), None)
            restart_safe = next((row for row in condition_results if row.get("id") == "restart_no_reexecution"), None)
            metrics[metric] = 0 if (
                no_duplicate
                and no_duplicate.get("passed")
                and restart_safe
                and restart_safe.get("passed")
            ) else 1
        elif metric == "trace_privacy_leak_rate":
            body_withheld = next((row for row in condition_results if row.get("id") == "notice_first_body_withheld"), None)
            owner_only = next((row for row in condition_results if row.get("id") == "owner_only_body_read"), None)
            metrics[metric] = 0 if (
                body_withheld
                and body_withheld.get("passed")
                and owner_only
                and owner_only.get("passed")
            ) else 1
        else:
            metrics[metric] = None
    return metrics


def run_manifest_case(spec: ScenarioSpec, cli: Path, keep_state_on_fail: bool) -> dict[str, object]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"swarm-behavior-{spec.scenario_id}-") as tmp:
        state_dir = Path(tmp)
        action_results: list[dict[str, object]] = []
        evidence: list[dict[str, object]] = []
        condition_results: list[dict[str, object]] = []
        try:
            run_cli(cli, state_dir, "message", "check")
            ensure_eval_schema(state_dir)
            agent_map = materialize_agents(cli, state_dir, spec.fixture)
            refs: dict[str, dict[str, object]] = {}
            compose: dict[tuple[str, str], str] = {}
            action_results.extend(run_manifest_steps(cli, state_dir, refs, agent_map, compose, spec.setup, 0))
            action_results.extend(run_manifest_steps(cli, state_dir, refs, agent_map, compose, spec.actions, len(spec.setup)))
            evidence = collect_evidence(state_dir, spec.evidence_queries)
            condition_results = evaluate_conditions(spec.pass_conditions, action_results, evidence, compose)
            failing_conditions = [row for row in condition_results if not row["passed"]]
            status = "pass" if not failing_conditions else "fail"
            error = None if status == "pass" else f"failed conditions: {', '.join(str(row['id']) for row in failing_conditions)}"
            details: dict[str, object] = {
                "agent_map": agent_map,
                "action_results": action_results,
                "condition_results": condition_results,
                "fail_signals": spec.fail_signals,
            }
        except Exception as exc:  # noqa: BLE001 - eval output should capture any scenario failure.
            status = "fail"
            details = {
                "state_dir": str(state_dir) if keep_state_on_fail else "discarded",
                "action_results": action_results,
                "condition_results": condition_results,
            }
            error = f"{type(exc).__name__}: {exc}"
            if keep_state_on_fail:
                keep_path = Path(tempfile.mkdtemp(prefix=f"swarm-behavior-failed-{spec.scenario_id}-"))
                subprocess.run(["cp", "-R", str(state_dir) + "/.", str(keep_path)], check=False)
                details["state_dir"] = str(keep_path)
        duration_ms = int((time.monotonic() - started) * 1000)
    metrics = requested_metric_values(spec.requested_metrics, action_results, condition_results)
    metrics.update({"wall_time_ms": duration_ms, "failed_turns": 1 if status == "fail" else 0})
    return {
        "scenario_id": spec.scenario_id,
        "title": spec.title,
        "probe_id": None,
        "fixture": spec.fixture,
        "factors": spec.factors,
        "timeline": action_results,
        "evidence": evidence,
        "metrics": metrics,
        "runner_verdict": status,
        "status": status,
        "duration_ms": duration_ms,
        "details": details,
        "error": error,
    }


def run_case(spec: ScenarioSpec, probe: BuiltinProbe, cli: Path, keep_state_on_fail: bool) -> dict[str, object]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"swarm-behavior-{spec.scenario_id}-") as tmp:
        state_dir = Path(tmp)
        try:
            details = probe.func(cli, state_dir)
            status = "pass"
            error = None
        except Exception as exc:  # noqa: BLE001 - eval output should capture any scenario failure.
            status = "fail"
            details = {"state_dir": str(state_dir) if keep_state_on_fail else "discarded"}
            error = f"{type(exc).__name__}: {exc}"
            if keep_state_on_fail:
                # Preserve the failed state under /tmp for local inspection.
                keep_path = Path(tempfile.mkdtemp(prefix=f"swarm-behavior-failed-{spec.scenario_id}-"))
                subprocess.run(["cp", "-R", str(state_dir) + "/.", str(keep_path)], check=False)
                details["state_dir"] = str(keep_path)
        duration_ms = int((time.monotonic() - started) * 1000)
    return {
        "scenario_id": spec.scenario_id,
        "title": spec.title,
        "probe_id": spec.probe_id,
        "fixture": spec.fixture,
        "factors": spec.factors,
        "timeline": [],
        "evidence": [details],
        "metrics": {"wall_time_ms": duration_ms, "failed_turns": 1 if status == "fail" else 0},
        "runner_verdict": status,
        "status": status,
        "duration_ms": duration_ms,
        "details": details,
        "error": error,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Swarm behavior smoke eval loop.")
    parser.add_argument("--cli", type=Path, default=DEFAULT_CLI, help="path to swarm CLI")
    parser.add_argument(
        "--manifest",
        type=Path,
        help=f"JSON probe manifest; defaults to {DEFAULT_MANIFEST} when present",
    )
    parser.add_argument("--output-dir", type=Path, help="write behavior-eval-results.json here")
    parser.add_argument("--case", action="append", help="run only this scenario_id or built-in probe id")
    parser.add_argument("--factor", action="append", help="override one recorded behavior factor with key=value")
    parser.add_argument("--keep-state-on-fail", action="store_true", help="preserve failed scenario state under /tmp")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = args.manifest
    if manifest is None and DEFAULT_MANIFEST.is_file():
        manifest = DEFAULT_MANIFEST
    specs = read_manifest(manifest) if manifest is not None else default_specs()
    factor_overrides = parse_factor_overrides(args.factor)
    specs = apply_factor_overrides(specs, factor_overrides)
    selected = select_specs(specs, args.case)
    cli = args.cli.resolve()
    results = [
        run_case(spec, PROBE_BY_ID[spec.probe_id], cli, args.keep_state_on_fail)
        if spec.probe_id is not None
        else run_manifest_case(spec, cli, args.keep_state_on_fail)
        for spec in selected
    ]
    report = {
        "schema_version": 1,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cli": str(cli),
        "manifest": str(manifest.resolve()) if manifest is not None else None,
        "purpose": "implementation-owned smoke loop; verifier-owned acceptance rubric remains separate",
        "factor_overrides": factor_overrides,
        "cases": results,
        "summary": {
            "pass": sum(1 for result in results if result["status"] == "pass"),
            "fail": sum(1 for result in results if result["status"] == "fail"),
        },
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "behavior-eval-results.json").write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 1 if report["summary"]["fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
