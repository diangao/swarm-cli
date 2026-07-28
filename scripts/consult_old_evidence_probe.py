#!/usr/bin/env python3
"""Anti-stub checks for the consult-old-evidence behavior slice."""

from __future__ import annotations

import copy
import json
import tempfile
import uuid
from pathlib import Path

import behavior_eval_loop as runner


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "docs" / "evals" / "scenario-consult-old-evidence.json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def load_payload() -> dict[str, object]:
    return json.loads(SCENARIO.read_text(encoding="utf-8"))


def run_payload(payload: dict[str, object]) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="swarm-consult-evidence-manifest-") as tmp:
        manifest = Path(tmp) / "scenario.json"
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        specs = runner.read_manifest(manifest)
        require(len(specs) == 1, "expected one scenario")
        return runner.run_manifest_case(specs[0], ROOT / "swarm", keep_state_on_fail=False)


def condition_map(result: dict[str, object]) -> dict[str, bool]:
    details = result.get("details")
    require(isinstance(details, dict), "missing result details")
    rows = details.get("condition_results")
    require(isinstance(rows, list), "missing condition results")
    return {str(row["id"]): bool(row["passed"]) for row in rows if isinstance(row, dict)}


def exact_contract_passes() -> None:
    result = run_payload(load_payload())
    require(result["status"] == "pass", f"exact verifier contract failed: {result.get('error')}")
    require(all(condition_map(result).values()), "exact verifier contract did not pass every condition")


def randomized_fact_is_retrieved_not_scripted() -> None:
    payload = load_payload()
    fact = f"the deploy window was moved to 17:23 UTC verification-{uuid.uuid4().hex}"
    setup = payload["setup"]
    require(isinstance(setup, list), "setup missing")
    seed_args = setup[0]["args"]
    require(isinstance(seed_args, dict), "seed args missing")
    seed_args["fact_text"] = fact
    result = run_payload(payload)
    require(result["status"] == "pass", f"randomized fact scenario failed: {result.get('error')}")
    evidence = result.get("evidence")
    require(isinstance(evidence, list), "randomized result missing evidence")
    outputs = next(item["rows"] for item in evidence if item.get("id") == "agent_output")
    require(len(outputs) == 1 and fact in outputs[0]["body"], "answer did not reflect the randomized durable fact")


def injected_fact_fails_closed() -> None:
    payload = load_payload()
    setup = payload["setup"]
    require(isinstance(setup, list), "setup missing")
    seed_args = setup[0]["args"]
    require(isinstance(seed_args, dict), "seed args missing")
    seed_args["inject_into_context"] = True
    result = run_payload(payload)
    require(result["status"] == "fail", "injected fact incorrectly passed")
    require(condition_map(result).get("fact_not_injected") is False, "injected fact did not trip the context gate")


def missing_query_fails_closed() -> None:
    payload = load_payload()
    payload["actions"] = [{"op": "sleep", "args": {"seconds": 0}}]
    result = run_payload(payload)
    require(result["status"] == "fail", "scenario with no retrieval turn incorrectly passed")
    conditions = condition_map(result)
    require(conditions.get("retrieved_by_query") is False, "missing query did not trip retrieval gate")
    require(conditions.get("answer_grounded_with_provenance") is False, "missing output did not trip answer gate")


def evaluator_rejects_missing_provenance_and_query() -> None:
    fact = {
        "fact_key": "E",
        "fact_value": "the deploy window was moved to 04:41 UTC",
        "provenance": "#eval:deadbeef",
        "injected_in_context": 0,
    }
    conditions = [
        {"id": "fact_not_injected"},
        {"id": "retrieved_by_query"},
        {"id": "answer_grounded_with_provenance"},
        {"id": "no_answer_without_retrieval"},
    ]
    no_provenance_evidence = [
        {"id": "seeded_fact", "rows": [fact]},
        {"id": "injected_context", "rows": []},
        {
            "id": "agent_queries",
            "rows": [
                {
                    "command_kind": "message_search",
                    "retrieved_body": 0,
                    "result_ref": "#eval:deadbeef",
                },
                {
                    "command_kind": "message_read",
                    "retrieved_body": 1,
                    "result_ref": "#eval:deadbeef",
                },
            ],
        },
        {
            "id": "agent_output",
            "rows": [{"body": "the deploy window was moved to 04:41 UTC", "message_id": "output"}],
        },
    ]
    no_provenance = runner.evaluate_conditions(conditions, [], no_provenance_evidence, {})
    no_provenance_map = {str(row["id"]): bool(row["passed"]) for row in no_provenance}
    require(
        no_provenance_map["answer_grounded_with_provenance"] is False,
        "answer without provenance incorrectly passed",
    )

    no_query_evidence = copy.deepcopy(no_provenance_evidence)
    next(item for item in no_query_evidence if item["id"] == "agent_queries")["rows"] = []
    next(item for item in no_query_evidence if item["id"] == "agent_output")["rows"][0][
        "body"
    ] = f"{fact['fact_value']} (source: {fact['provenance']})"
    no_query = runner.evaluate_conditions(conditions, [], no_query_evidence, {})
    no_query_map = {str(row["id"]): bool(row["passed"]) for row in no_query}
    require(no_query_map["retrieved_by_query"] is False, "answer without query incorrectly passed retrieval gate")
    require(
        no_query_map["no_answer_without_retrieval"] is False,
        "answer without query incorrectly passed anti-bypass gate",
    )

    unrelated_search_evidence = copy.deepcopy(no_provenance_evidence)
    unrelated_query_rows = next(
        item for item in unrelated_search_evidence if item["id"] == "agent_queries"
    )["rows"]
    unrelated_query_rows[0]["result_ref"] = "#eval:unrelated"
    next(item for item in unrelated_search_evidence if item["id"] == "agent_output")["rows"][0][
        "body"
    ] = f"{fact['fact_value']} (source: {fact['provenance']})"
    unrelated_search = runner.evaluate_conditions(
        conditions,
        [],
        unrelated_search_evidence,
        {},
    )
    unrelated_search_map = {
        str(row["id"]): bool(row["passed"]) for row in unrelated_search
    }
    require(
        unrelated_search_map["retrieved_by_query"] is False,
        "unrelated search plus direct read incorrectly passed retrieval gate",
    )
    require(
        unrelated_search_map["no_answer_without_retrieval"] is False,
        "unrelated search plus direct read incorrectly passed anti-bypass gate",
    )


def main() -> int:
    exact_contract_passes()
    randomized_fact_is_retrieved_not_scripted()
    injected_fact_fails_closed()
    missing_query_fails_closed()
    evaluator_rejects_missing_provenance_and_query()
    print(
        "consult-old-evidence probe ok: exact contract, randomized fact, "
        "injected-context fail-closed, missing-query fail-closed, "
        "missing-provenance, unrelated-search, and anti-bypass evaluator gates"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
