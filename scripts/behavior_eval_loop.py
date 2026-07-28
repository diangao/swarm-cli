#!/usr/bin/env python3
"""Run a small, implementation-owned behavior smoke loop.

The loop gives the implementation owner a repeatable local check for the first
runtime slice: cold start, CLI-only commit boundary, startup context manifests,
freshness drafts, task claim conflicts, and reminder wake persistence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
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


def truthy_int(value: object) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "y", "on"} else 0
    return 0


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
        requested_metrics = as_list(item.get("metrics"), scenario_id, "metrics")
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
    finally:
        conn.close()


def clean_agent_name(alias: str) -> str:
    base = re.sub(r"[^a-z0-9_-]+", "-", alias.lower()).strip("-_")
    if not base or not base[0].islower():
        base = f"agent-{base}" if base else "agent"
    return f"{base}-{uuid.uuid4().hex[:6]}"


def materialize_agents(cli: Path, state_dir: Path, fixture: dict[str, object]) -> dict[str, str]:
    assignment = fixture.get("role_assignment")
    aliases: list[str] = []
    if isinstance(assignment, dict):
        aliases = [str(key) for key in assignment.keys()]
    count = fixture.get("agent_count")
    if not aliases and isinstance(count, int) and count > 0:
        aliases = [chr(ord("A") + index) for index in range(min(count, 26))]
    agent_map: dict[str, str] = {}
    for alias in aliases:
        name = clean_agent_name(alias)
        run_cli(
            cli,
            state_dir,
            "agent",
            "register",
            "--name",
            name,
            "--display-name",
            alias,
            "--runtime",
            "codex",
            "--workspace",
            f"agents/{name}",
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
        body = str(args.get("body") or f"{ref} body {uuid.uuid4()}")
        ensure_channel(cli, state_dir, target)
        record = insert_message_record(state_dir, target, body, author=author, sender_type=sender_type)
        refs[ref] = {"kind": "message", **record}
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
