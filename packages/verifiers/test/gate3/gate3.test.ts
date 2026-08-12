import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";

import {
  canonicalBlockedClosureOracleInput,
  GATE3_ASSERTION_PREDICATE_IDS,
  GATE3_EXTERNAL_PLAN_SHA256,
  Gate3OracleError,
  Gate3PlanError,
  loadGate3PlanBytes,
  loadGate3PlanFile,
  runBlockedClosureOracle,
  runGate3Negatives,
  runGate3Positive,
} from "../../src/gate3/index.js";
import type {
  BlockedClosureOracleInput,
  Gate3FactBundle,
  Gate3Plan,
} from "../../src/gate3/index.js";

type Simulator = {
  readonly GATE3_ASSERTION_PRODUCER_IDS: readonly string[];
  readonly createPositiveGate3Facts: (plan: Gate3Plan) => Gate3FactBundle;
  readonly createGroupMutation: (plan: Gate3Plan, groupId: string) => Gate3FactBundle;
  readonly createNegativeGate3Facts: (plan: Gate3Plan) => Gate3FactBundle;
};

const planPath = resolve(process.cwd(), "test/gate3/wave3-gate3-scenario-plan-v0.8.json");

async function simulator(): Promise<Simulator> {
  const url = pathToFileURL(resolve(process.cwd(), "../testkit/dist/gate3/index.js")).href;
  return (await import(url)) as Simulator;
}

function fakeInput(plan: Gate3Plan): Gate3Plan {
  return {
    ...plan,
    scenarios: plan.scenarios.map((scenario) => ({
      ...scenario,
      negativeSeeds: scenario.negativeSeeds.map(({ id, defect, unchanged }) => ({
        id,
        defect,
        unchanged,
      })),
    })),
  };
}

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function siblingDigest(name: string, revision: number): string {
  return createHash("sha256")
    .update(JSON.stringify({ name, revision }), "utf8")
    .digest("hex");
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record).sort().map((key) =>
    `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
}

const ASSERTION_MUTATION_FIELD: Readonly<Record<string, string>> = {
  exactly_one_claim_winner_for_two_three_and_five_contenders: "winnerCounts",
  winner_and_losers_share_one_locked_task_revision: "lockedRevisionCountPerRound",
  winner_status_and_claim_insert_commit_atomically: "statusAndClaimAtomic",
  losers_receive_conflict_without_claim_or_status_effect: "loserSharedMutationCount",
  lease_epoch_is_previous_max_plus_one: "winnerEpoch",
  claim_command_exact_replay_aliases_after_status_advances: "exactReplayAliasedAfterStatusAdvance",
  renewal_uses_exact_full_lease_and_expected_revision: "fullLeaseMatched",
  renewal_keeps_epoch_and_fence_token_but_advances_revision: "currentLeaseRevision",
  renewed_expiry_is_strictly_later_and_server_clock_authoritative: "serverClockAuthoritative",
  long_native_turn_renews_without_reclaim_churn: "reclaimCountDuringTurn",
  expired_lease_cannot_renew_or_publish: "expiredRenewRejected",
  server_expiry_closes_exact_claim_and_reservations_then_reopens_task_for_epoch_plus_one_reclaim: "serverExpiryAtomic",
  standalone_release_atomically_closes_current_claim_reservations_and_returns_task_to_todo: "standaloneReleaseAtomic",
  released_or_expired_task_reclaims_with_next_history_inclusive_epoch: "reclaimedEpoch",
  daemon_never_dispatches_renewal_before_window_and_retries_fresh_command_after_too_early: "retryUsesFreshCommand",
  too_early_command_replays_stored_rejection_while_fresh_retry_waits_for_retry_not_before: "earlyReplayCode",
  every_task_artifact_and_review_request_mutation_checks_full_current_lease: "allFullLeaseChecksPassed",
  stale_owner_has_zero_shared_mutation: "staleOwnerSharedMutationCount",
  owner_loss_requests_local_interrupt_before_any_later_effect: "ownershipLossSequence",
  late_native_result_is_observation_only_after_owner_loss: "lateResultDisposition",
  new_epoch_owner_is_not_affected_by_stale_cleanup: "staleCleanupAffectedReplacement",
  matching_current_execution_renews_before_resume: "matchingCurrentSequence",
  expired_or_replaced_execution_stops_and_suppresses_publication: "expiredAndReplacedStop",
  local_only_execution_is_stopped_without_shared_mutation: "localOnlySharedMutationCount",
  shared_only_claim_is_relinquished_or_left_for_server_expiry_without_fabricated_process: "fabricatedProcessCount",
  shared_only_relinquish_closes_exact_claim_and_reservation_before_todo_reclaim: "relinquishAtomic",
  wave3_reconciliation_does_not_claim_full_wave4_crash_recovery: "fullCrashRecoveryDeferred",
  one_logical_coordination_effect_atomically_creates_children_edges_and_one_graph_revision: "childrenEdgesAndRevisionAtomic",
  proposal_replay_aliases_exact_stored_result: "exactReplayAliased",
  changed_payload_under_same_command_conflicts: "changedPayloadOutcome",
  source_message_turn_producer_and_current_lease_join_before_graph_write: "sourceJoinPassed",
  parent_transitions_to_waiting_in_same_transaction: "parentStatusAfter",
  one_reply_permits_one_logical_coordination_effect_with_same_command_replay_only: "distinctSecondCommandOutcome",
  contains_and_dependency_edges_are_distinct_and_combined_lifecycle_wait_graph_is_acyclic: "combinedWaitGraphAcyclic",
  only_leaf_with_all_dependencies_done_is_claimable: "onlyReadyLeafClaimable",
  dependency_completion_atomically_recomputes_ready_set: "dependencyCompletionAtomic",
  blocked_dependency_fail_closes_dependents: "blockedDependencyFailClosed",
  cross_root_or_cross_server_edges_are_rejected: "crossScopeEdgesRejected",
  concurrent_endpoint_or_ancestor_mutation_serializes_before_cycle_and_readiness_commit: "bothSerialOrdersObserved",
  blocked_prerequisite_transitively_blocks_dependents_and_waiting_ancestors_atomically: "allAffectedBlocked",
  blocked_propagation_derives_complete_affected_closure_from_exact_stored_topology: "closureDerivedFromStoredTopology",
  blocked_propagation_records_exact_graph_blocked_lease_reason_and_typed_before_after_rows: "graphBlockedLeaseReasonExact",
  repository_first_global_lock_order_forces_both_serial_orders_and_prevents_claim_propagation_deadlock: "deadlockCount",
  proposal_and_claim_validate_capabilities_from_current_server_registry: "registryValidatedAtProposalAndClaim",
  all_graph_limits_apply_before_any_insert: "allLimitsCheckedBeforeInsert",
  limit_policy_digest_is_stored_with_graph_revision: "limitPolicyDigestStored",
  titles_and_capability_keys_are_canonical_bounded_values: "canonicalTitlesAndCapabilities",
  delegated_title_privacy_gate_runs_before_graph_insert_without_body_egress: "bodyEgressCount",
  whole_short_source_body_is_rejected_even_when_wrapped: "wrappedWholeBodyRejected",
  graph_persists_exact_title_and_current_classifier_policy_digests: "classifierPolicyDigestStored",
  normal_conversation_requires_no_global_planner: "globalPlannerRequired",
  each_task_binds_exact_base_workspace_generation_and_allowed_paths: "exactWorkspaceBinding",
  path_claims_are_normalized_relative_and_non_overlapping: "pathsNormalizedRelativeNonoverlap",
  repository_scoped_serialization_gives_one_cross_root_overlap_winner: "crossRootWinnerCounts",
  shared_file_collision_rejected_or_owned_by_one_integration_task: "sharedCollisionDisposition",
  child_worktrees_cannot_mutate_integration_owner_paths: "childIntegrationOwnerMutationCount",
  workspace_contract_digest_changes_on_any_policy_member: "digestSensitiveMembers",
  artifact_contract_template_is_selected_by_current_server_policy_not_builder: "templateAuthority",
  artifact_contract_template_materializes_non_circular_exact_task_workspace_preimage: "nonCircularTaskWorkspacePreimage",
  artifact_bytes_commit_tree_parent_scope_and_contract_bind_before_publication: "boundObjectMembers",
  published_artifact_material_remains_resolvable_after_private_worktree_disappears: "materialResolution",
  publication_and_task_in_review_transition_are_atomic: "publicationTaskTransitionAtomic",
  exact_replay_aliases_one_immutable_artifact: "exactReplayAliased",
  same_attempt_divergence_conflicts_without_replacing_artifact: "divergentAttemptOutcome",
  builder_and_contributors_derive_from_server_attempt_facts: "contributorLedgerAuthority",
  contributor_ledger_is_server_written_and_transitively_expands_accepted_upstream_artifacts: "contributorLedgerTransitive",
  accepted_artifact_consumption_locks_source_barrier_material_and_target_ledger_before_byte_grant: "materialGrantedAfterLedgerCommit",
  accepted_artifact_consumption_transitively_expands_contributors_and_source_set_atomically: "sourceSetExpandedAtomically",
  consumption_replay_aliases_one_grant_and_ledger_revision: "consumptionReplayAliased",
  artifact_metadata_contains_no_body_credential_or_private_path: "forbiddenMetadataFieldCount",
  builder_cannot_claim_or_submit_any_required_reviewer_seat: "builderSeatMutationCount",
  distinct_required_seats_use_distinct_reviewer_agents: "distinctReviewerCount",
  publication_mints_one_stable_unassigned_review_task_per_required_seat: "stableReviewTaskCount",
  assignment_and_reassignment_are_revisioned_atomic_and_unique: "assignmentUnique",
  reviewer_receives_only_exact_frozen_artifact_contract_and_scenario: "reviewerInputMembers",
  review_verdict_attempt_equals_current_reviewer_lease_attempt: "verdictAttempt",
  replacement_reviewer_new_attempt_can_submit_only_the_exact_current_verdict: "replacementAttemptAccepted",
  each_required_seat_has_one_current_terminal_verdict: "terminalVerdictCountPerSeat",
  terminal_verdict_closes_assignment_and_prevents_reassignment: "terminalAssignmentClosed",
  all_required_go_verdicts_same_artifact_unlock_barrier: "allGoUnlocksBarrier",
  one_block_or_missing_seat_keeps_barrier_closed: "blockOrMissingKeepsClosed",
  one_block_revokes_nonterminal_sibling_review_tasks_and_late_verdicts: "lateSiblingVerdictRejected",
  barrier_block_closes_sibling_review_lease_with_exact_review_barrier_blocked_reason: "blockedLeaseReason",
  integration_owner_alone_may_consume_reviewed_child_artifacts: "integrationOwnerOnly",
  fast_forward_requires_exact_reviewed_commit_chain_without_rewrite: "fastForwardNoRewrite",
  new_integration_sha_reenters_full_review_barrier: "newCommitRequiresFullReview",
  blocked_or_stale_child_never_enters_integration_candidate: "blockedOrStaleIncludedCount",
  integration_candidate_uses_authoritative_consumed_source_set_and_current_ledger_revision: "currentLedgerRevisionChecked",
  protected_publication_stays_outside_task_execution_transaction: "protectedPublicationInsideTaskTransaction",
  all_wave0_wave1_and_wave2_gates_remain_green: "wave0Wave1Wave2Gates",
  pure_question_still_has_one_normal_reply_and_zero_task_effect: "pureQuestionTaskEffectCount",
  task_graph_coordination_still_has_committed_reply_predecessor_and_one_logical_effect: "coordinationReplyPredecessorCommitted",
  five_wave3_deferred_rows_bind_only_to_real_task_artifact_and_freshness_facts: "wave3RowsBoundToMachineFacts",
  coordination_slo_and_channel_navigation_remain_wave5_fail_closed: "wave5DeferredRows",
  seed_m2_binds_exact_execute_owner_artifact_path_and_digest: "seedM2ExactArtifactBinding",
  exact_new_package_import_builtin_and_owner_boundaries_pass_with_complete_seeded_negatives: "boundaryPolicyCount",
  new_package_manifests_match_exact_workspace_dependencies_and_empty_external_allow_sets: "externalAllowSetsEmpty",
  all_wave3_forbidden_facts_absent_from_product_fixture_log_and_receipt: "forbiddenFactCount",
};

function mutateAssertionObservation(
  assertionId: string,
  observation: object,
): object {
  const field = ASSERTION_MUTATION_FIELD[assertionId];
  assert.ok(field, assertionId);
  const changed = clone(observation) as Record<string, unknown>;
  const value = changed[field];
  if (Array.isArray(value)) changed[field] = [];
  else if (typeof value === "boolean") changed[field] = !value;
  else if (typeof value === "number") changed[field] = value + 1;
  else if (typeof value === "string") changed[field] = `${value}_mutated`;
  else assert.fail(`${assertionId}:${field}:unsupported mutation`);
  return changed;
}

test("external plan loader binds exact bytes, schema, policy, and 12/98/122 inventory", async () => {
  const bytes = await readFile(planPath);
  assert.equal(sha256(bytes), GATE3_EXTERNAL_PLAN_SHA256);
  const plan = await loadGate3PlanFile(planPath);
  assert.equal(plan.scenarios.length, 12);
  assert.equal(plan.scenarios.flatMap((scenario) => scenario.assertions).length, 98);
  assert.equal(plan.scenarios.flatMap((scenario) => scenario.negativeSeeds).length, 122);

  const changed = Buffer.concat([bytes, Buffer.from("\n")]);
  assert.throws(
    () => loadGate3PlanBytes(changed),
    (error: unknown) => error instanceof Gate3PlanError && error.code === "GATE3_PLAN_DIGEST_MISMATCH",
  );
});

test("Wave 0/1/2 carry and all seven deferred rows stay exact and fail-closed", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const scenario = plan.scenarios.find((entry) => entry.id === "g3_12_wave_carry_and_deferred_truth")!;
  const fixture = scenario.fixture;
  assert.equal(fixture["wave0Wave1Wave2Gates"], "green");
  assert.deepEqual(fixture["wave1DeferredRows"], {
    thread_owner_status: "wave3_candidate",
    task_lifecycle: "wave3_candidate",
    task_status_freshness_hold: "wave3_candidate",
    reply_trailer: "wave3_candidate",
    lane_role_fidelity: "wave3_candidate",
    coordination_slo: "wave5_deferred",
    channel_navigation: "wave5_deferred",
  });
  assert.equal(fixture["seedM2"], "wave3_candidate");
  assert.deepEqual(fixture["requiredBoundaryNegativeKinds"], [
    "forbidden_workspace_import",
    "forbidden_builtin",
    "allowed_builtin_subpath_escape",
    "undeclared_workspace_import",
    "undeclared_external_import",
    "unauthorized_declared_external_import",
    "absolute_import",
    "relative_package_escape",
    "forbidden_concrete_driver_import",
    "unknown_package_or_app",
    "integration_owner_root_file_violation",
  ]);
  assert.deepEqual(fixture["wave3BoundaryPolicies"], [
    {
      root: "packages/task-engine",
      policyKey: "task-engine",
      packageName: "@swarm/task-engine",
      workspaceImports: ["protocol"],
      externalPackageSpecifiers: [],
      builtinPolicy: "all_high_authority_forbidden",
    },
    {
      root: "packages/artifacts",
      policyKey: "artifacts",
      packageName: "@swarm/artifacts",
      workspaceImports: ["protocol", "security"],
      externalPackageSpecifiers: [],
      builtinPolicy: "child_process_only_src_git_fs_process_only_src_git_or_src_store_cluster_net_worker_threads_forbidden",
    },
    {
      root: "apps/server",
      policyKey: "app:server",
      packageName: "@swarm/app-server",
      workspaceImports: ["protocol", "storage", "security", "artifacts", "task-engine"],
      externalPackageSpecifiers: [],
      builtinPolicy: "cluster_and_worker_threads_forbidden",
    },
  ]);
});

test("loader rejects unknown keys, duplicate names, duplicate JSON keys, and empty unchanged sets", async () => {
  const original = JSON.parse(await readFile(planPath, "utf8")) as Record<string, unknown>;
  const cases: readonly [string, (plan: Record<string, unknown>) => void, string][] = [
    ["unknown", (plan) => { plan["unexpected"] = true; }, "GATE3_PLAN_UNKNOWN_KEY"],
    [
      "duplicate assertion",
      (plan) => {
        const scenarios = plan["scenarios"] as Record<string, unknown>[];
        const assertions = scenarios[0]!["assertions"] as string[];
        assertions[1] = assertions[0]!;
      },
      "GATE3_PLAN_DUPLICATE_ID",
    ],
    [
      "empty unchanged",
      (plan) => {
        const scenarios = plan["scenarios"] as Record<string, unknown>[];
        const seeds = scenarios[0]!["negativeSeeds"] as Record<string, unknown>[];
        seeds[0]!["unchanged"] = [];
      },
      "GATE3_PLAN_SCHEMA_INVALID",
    ],
  ];
  for (const [name, mutate, code] of cases) {
    const candidate = clone(original);
    mutate(candidate);
    const bytes = Buffer.from(JSON.stringify(candidate));
    assert.throws(
      () => loadGate3PlanBytes(bytes),
      (error: unknown) => error instanceof Gate3PlanError && error.code === code,
      name,
    );
  }
  const duplicateKey = Buffer.from('{"schemaVersion":1,"schemaVersion":1}');
  assert.throws(
    () => loadGate3PlanBytes(duplicateKey),
    (error: unknown) => error instanceof Gate3PlanError && error.code === "GATE3_PLAN_JSON_INVALID",
  );
});

test("positive oracle executes all 98 assertions exactly once and every group mutation kills only its target", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const fake = await simulator();
  const projected = fakeInput(plan);
  const baseline = runGate3Positive(plan, fake.createPositiveGate3Facts(projected));
  assert.equal(baseline.passed, true);
  assert.equal(baseline.assertionResults.length, 98);
  assert.deepEqual(new Set(Object.values(baseline.assertionExecutions)), new Set([1]));

  for (const scenario of plan.scenarios) {
    const result = runGate3Positive(plan, fake.createGroupMutation(projected, scenario.id));
    assert.equal(result.passed, false, scenario.id);
    assert.equal(result.assertionResults.length, 98, scenario.id);
    const failed = result.assertionResults.filter((entry) => !entry.passed);
    assert.equal(failed.length, 1, scenario.id);
    assert.equal(failed[0]!.groupId, scenario.id);
    assert.deepEqual(new Set(Object.values(result.assertionExecutions)), new Set([1]));
  }
});

test("all 98 assertion predicates preserve meaning when plan assertion order changes", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const fake = await simulator();
  const planAssertionIds = plan.scenarios.flatMap((scenario) => scenario.assertions);
  assert.equal(fake.GATE3_ASSERTION_PRODUCER_IDS.length, 98);
  assert.equal(GATE3_ASSERTION_PREDICATE_IDS.length, 98);
  assert.deepEqual(new Set(fake.GATE3_ASSERTION_PRODUCER_IDS), new Set(planAssertionIds));
  assert.deepEqual(new Set(GATE3_ASSERTION_PREDICATE_IDS), new Set(planAssertionIds));
  const original = runGate3Positive(plan, fake.createPositiveGate3Facts(fakeInput(plan)));
  const reordered = fakeInput(plan);
  (reordered as unknown as { scenarios: { assertions: string[] }[] }).scenarios.forEach((scenario) => {
    scenario.assertions.reverse();
  });
  const result = runGate3Positive(plan, fake.createPositiveGate3Facts(reordered));
  assert.equal(result.passed, true);
  assert.equal(result.assertionResults.length, 98);
  assert.deepEqual(new Set(Object.values(result.assertionExecutions)), new Set([1]));
  const ownResultMapping = (run: typeof result) => run.assertionResults
    .map(({ assertionId, observationDigest, passed }) => ({ assertionId, observationDigest, passed }))
    .sort((left, right) => left.assertionId.localeCompare(right.assertionId));
  assert.deepEqual(
    ownResultMapping(result),
    ownResultMapping(original),
    "reordering preserves every assertion's own observation/result digest mapping",
  );
});

test("every one of the 98 named predicates kills its own relevant fact mutation", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const fake = await simulator();
  const baseline = fake.createPositiveGate3Facts(fakeInput(plan));
  assert.deepEqual(
    new Set(Object.keys(ASSERTION_MUTATION_FIELD)),
    new Set(plan.scenarios.flatMap((scenario) => scenario.assertions)),
  );
  for (const fact of baseline.groups.flatMap((group) => group.assertions)) {
    const mutant = clone(baseline);
    const target = mutant.groups
      .flatMap((group) => group.assertions)
      .find((candidate) => candidate.assertionId === fact.assertionId)!;
    (target as unknown as { observation: object }).observation =
      mutateAssertionObservation(fact.assertionId, target.observation);
    (target as unknown as { observationDigest: string }).observationDigest = sha256(
      Buffer.from(canonicalJson({
        groupId: target.groupId,
        assertionId: target.assertionId,
        evidenceKinds: [...target.evidenceKinds],
        observation: target.observation,
      })),
    );
    const result = runGate3Positive(plan, mutant);
    assert.deepEqual(
      result.assertionResults.filter((entry) => !entry.passed).map((entry) => entry.assertionId),
      [fact.assertionId],
      fact.assertionId,
    );
  }
});

test("all 122 independent negatives kill the planned condition with byte-identical siblings", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const fake = await simulator();
  const bundle = fake.createNegativeGate3Facts(fakeInput(plan));
  const result = runGate3Negatives(plan, bundle);
  assert.equal(result.passed, true);
  assert.equal(result.negativeResults.length, 122);
  assert.deepEqual(new Set(Object.values(result.negativeExecutions)), new Set([1]));

  for (const fact of bundle.negatives) {
    const wrongOutcome = clone(bundle);
    const target = wrongOutcome.negatives.find((entry) => entry.seedId === fact.seedId)!;
    (target as { observedOutcome: string }).observedOutcome = "mutant_survived";
    const killed = runGate3Negatives(plan, wrongOutcome);
    assert.equal(
      killed.negativeResults.filter((entry) => !entry.passed).map((entry) => entry.seedId).join(),
      fact.seedId,
    );

    const changedSibling = clone(bundle);
    const siblingTarget = changedSibling.negatives.find((entry) => entry.seedId === fact.seedId)!;
    const first = siblingTarget.siblingsAfter[0]!;
    (first as { revision: number }).revision += 1;
    (first as { valueDigest: string }).valueDigest = siblingDigest(first.name, first.revision);
    const siblingKilled = runGate3Negatives(plan, changedSibling);
    assert.equal(
      siblingKilled.negativeResults.filter((entry) => !entry.passed).map((entry) => entry.seedId).join(),
      fact.seedId,
    );
  }
});

test("fact inventory, evidence, and observation digests fail closed", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const fake = await simulator();
  const missing = clone(fake.createPositiveGate3Facts(fakeInput(plan)));
  const group = missing.groups[0]!;
  (group.assertions as Gate3FactBundle["groups"][number]["assertions"] extends readonly (infer U)[] ? U[] : never).pop();
  assert.throws(
    () => runGate3Positive(plan, missing),
    (error: unknown) => error instanceof Gate3OracleError && error.code === "GATE3_FACT_INVENTORY_MISMATCH",
  );

  const corrupt = clone(fake.createPositiveGate3Facts(fakeInput(plan)));
  const fact = corrupt.groups[0]!.assertions[0]!;
  (fact as { observationDigest: string }).observationDigest = "0".repeat(64);
  assert.throws(
    () => runGate3Positive(plan, corrupt),
    (error: unknown) => error instanceof Gate3OracleError && error.code === "GATE3_FACT_DIGEST_MISMATCH",
  );

  const unknown = clone(fake.createPositiveGate3Facts(fakeInput(plan)));
  (unknown.groups[0]!.assertions[0] as unknown as Record<string, unknown>)["expected"] = true;
  assert.throws(
    () => runGate3Positive(plan, unknown),
    (error: unknown) => error instanceof Gate3OracleError && error.code === "GATE3_FACT_SCHEMA_INVALID",
  );
});

test("isolated three-key blocked-closure oracle ignores four poison views and derives the full closure", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const scenario = plan.scenarios.find((entry) => entry.id === "g3_6_cycles_dependencies_and_ready_leaves")!;
  const chain = scenario.fixture["transitiveBlockedChain"] as Record<string, unknown>;
  const harness = chain["metamorphicOracleHarness"] as Record<string, unknown>;
  const invocations = harness["invocations"] as readonly Record<string, unknown>[];
  const expected = [
    "tsk_00000000000000000000000021",
    "tsk_00000000000000000000000022",
    "tsk_00000000000000000000000023",
  ];
  const serialized = invocations.map((invocation) =>
    canonicalBlockedClosureOracleInput(invocation["oracleInput"] as BlockedClosureOracleInput),
  );
  const topologyBytes = invocations.map((invocation) =>
    JSON.stringify((invocation["oracleInput"] as BlockedClosureOracleInput).storedTopology),
  );
  assert.equal(new Set(topologyBytes).size, 1, "all four calls carry byte-identical topology");
  for (const bytes of serialized) {
    assert.deepEqual(runBlockedClosureOracle(bytes), {
      oracleExecuted: true,
      affectedTaskIds: expected,
    });
  }

  const oneHop = clone(invocations[0]!["oracleInput"] as BlockedClosureOracleInput);
  (oneHop.storedTopology.contains as { parentTaskId: string; childTaskId: string }[]).shift();
  assert.deepEqual(
    runBlockedClosureOracle(canonicalBlockedClosureOracleInput(oneHop)).affectedTaskIds,
    expected.slice(0, 2),
  );

  for (const forbiddenKey of ["before", "after", "parentFixture", "expectedAffectedTaskIds", "verifierExpectation"]) {
    const injected = clone(invocations[0]!["oracleInput"] as unknown as Record<string, unknown>);
    injected[forbiddenKey] = expected;
    assert.throws(
      () => runBlockedClosureOracle(JSON.stringify(injected)),
      /oracle_input_projection_violation/u,
      forbiddenKey,
    );
  }
  const nested = clone(invocations[0]!["oracleInput"] as unknown as Record<string, unknown>);
  (nested["poisonedView"] as Record<string, unknown>)["expectedAffectedTaskIds"] = expected;
  assert.throws(
    () => runBlockedClosureOracle(JSON.stringify(nested)),
    /oracle_input_projection_violation/u,
  );
});

test("permission-isolated child has only the zero-import oracle and projected stdin", async () => {
  const plan = await loadGate3PlanFile(planPath);
  const scenario = plan.scenarios.find((entry) => entry.id === "g3_6_cycles_dependencies_and_ready_leaves")!;
  const chain = scenario.fixture["transitiveBlockedChain"] as Record<string, unknown>;
  const harness = chain["metamorphicOracleHarness"] as Record<string, unknown>;
  const invocations = harness["invocations"] as readonly Record<string, unknown>[];
  const payload = invocations.map((invocation) =>
    canonicalBlockedClosureOracleInput(invocation["oracleInput"] as BlockedClosureOracleInput),
  );
  const modulePath = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "../../src/gate3/blocked-closure.js",
  );
  const packagePath = resolve(process.cwd(), "package.json");
  const script = [
    "for (const key of Object.keys(process.env)) delete process.env[key];",
    "const chunks=[];",
    "for await (const chunk of process.stdin) chunks.push(chunk);",
    "const inputs=JSON.parse(Buffer.concat(chunks).toString('utf8'));",
    "const mod=await import(process.argv[1]);",
    "process.stdout.write(JSON.stringify({env:Object.keys(process.env),results:inputs.map(mod.runBlockedClosureOracle)}));",
  ].join("");
  const child = spawnSync(
    process.execPath,
    [
      "--permission",
      `--allow-fs-read=${modulePath}`,
      `--allow-fs-read=${packagePath}`,
      "--input-type=module",
      "-e",
      script,
      pathToFileURL(modulePath).href,
    ],
    { input: JSON.stringify(payload), encoding: "utf8", env: {} },
  );
  assert.equal(child.status, 0, child.stderr);
  const output = JSON.parse(child.stdout) as {
    readonly env: readonly string[];
    readonly results: readonly { readonly affectedTaskIds: readonly string[] }[];
  };
  assert.deepEqual(output.env, []);
  assert.equal(output.results.length, 4);
  assert.ok(output.results.every((result) => result.affectedTaskIds.join() === [
    "tsk_00000000000000000000000021",
    "tsk_00000000000000000000000022",
    "tsk_00000000000000000000000023",
  ].join()));

  const temporary = await mkdtemp(resolve(tmpdir(), "gate3-parent-import-"));
  try {
    const mutant = resolve(temporary, "mutant.mjs");
    await writeFile(mutant, `await import(${JSON.stringify(pathToFileURL(planPath).href)});\n`, "utf8");
    const denied = spawnSync(
      process.execPath,
      ["--permission", `--allow-fs-read=${mutant}`, "--input-type=module", "-e", "await import(process.argv[1]);", pathToFileURL(mutant).href],
      { encoding: "utf8", env: {} },
    );
    assert.notEqual(denied.status, 0);
    assert.match(denied.stderr, /ERR_ACCESS_DENIED|AccessDenied/u);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});
