import {
  digestGate3Json,
  snapshotSibling,
} from "./facts.js";
import {
  createAssertionDomainObservation,
  mutateGroupDomainObservation,
} from "./domain.js";
import type {
  Gate3AssertionFact,
  Gate3AssertionObservation,
  Gate3FactBundle,
  Gate3GroupFacts,
  Gate3NegativeFact,
  Gate3PlanShape,
  Gate3Scalar,
} from "./facts.js";

const TARGET_ASSERTION_BY_GROUP: Readonly<Record<string, string>> = {
  g3_1_atomic_claim_contention:
    "exactly_one_claim_winner_for_two_three_and_five_contenders",
  g3_2_lease_renewal_and_expiry:
    "renewal_uses_exact_full_lease_and_expected_revision",
  g3_3_guarded_mutations_and_owner_loss:
    "every_task_artifact_and_review_request_mutation_checks_full_current_lease",
  g3_4_startup_reconciliation:
    "matching_current_execution_renews_before_resume",
  g3_5_atomic_graph_proposal:
    "one_logical_coordination_effect_atomically_creates_children_edges_and_one_graph_revision",
  g3_6_cycles_dependencies_and_ready_leaves:
    "contains_and_dependency_edges_are_distinct_and_combined_lifecycle_wait_graph_is_acyclic",
  g3_7_capability_and_bounds:
    "proposal_and_claim_validate_capabilities_from_current_server_registry",
  g3_8_workspace_and_path_ownership:
    "each_task_binds_exact_base_workspace_generation_and_allowed_paths",
  g3_9_immutable_artifact_publication:
    "artifact_contract_template_is_selected_by_current_server_policy_not_builder",
  g3_10_exact_artifact_review_barrier:
    "builder_cannot_claim_or_submit_any_required_reviewer_seat",
  g3_11_reviewed_integration:
    "integration_owner_alone_may_consume_reviewed_child_artifacts",
  g3_12_wave_carry_and_deferred_truth:
    "all_wave0_wave1_and_wave2_gates_remain_green",
};

function assertionFact(
  groupId: string,
  assertionId: string,
  evidenceKinds: readonly string[],
  observation: Gate3AssertionObservation,
): Gate3AssertionFact {
  const digestInput = {
    groupId,
    assertionId,
    evidenceKinds: [...evidenceKinds],
    observation,
  };
  return {
    ...digestInput,
    observationDigest: digestGate3Json(digestInput),
  };
}

export function createPositiveGate3Facts(plan: Gate3PlanShape): Gate3FactBundle {
  const groups = plan.scenarios.map((scenario): Gate3GroupFacts => {
    const target = TARGET_ASSERTION_BY_GROUP[scenario.id];
    if (target === undefined || !scenario.assertions.includes(target)) {
      throw new Error(`gate3_unknown_group_or_target:${scenario.id}`);
    }
    return {
      groupId: scenario.id,
      assertions: scenario.assertions.map((assertionId) =>
        assertionFact(
          scenario.id,
          assertionId,
          scenario.expectedFacts,
          createAssertionDomainObservation(scenario.id, assertionId, scenario.fixture),
        ),
      ),
    };
  });
  return { schemaVersion: 1, groups, negatives: [] };
}

export function createGroupMutation(
  plan: Gate3PlanShape,
  groupId: string,
): Gate3FactBundle {
  const baseline = createPositiveGate3Facts(plan);
  const target = TARGET_ASSERTION_BY_GROUP[groupId];
  if (target === undefined) throw new Error(`gate3_unknown_group:${groupId}`);
  return {
    ...baseline,
    groups: baseline.groups.map((group) => {
      if (group.groupId !== groupId) return group;
      return {
        ...group,
        assertions: group.assertions.map((fact) =>
          fact.assertionId === target
            ? assertionFact(
                fact.groupId,
                fact.assertionId,
                fact.evidenceKinds,
                mutateGroupDomainObservation(groupId, fact.observation),
              )
            : fact,
        ),
      };
    }),
  };
}

// This executable registry is intentionally independent of the plan's
// expectError/verifierMustObserve fields. The exact defect action selects the
// simulated behavior; seed identity is only an inventory/digest binding.
const OUTCOME_BY_DEFECT_ACTION: Readonly<Record<string, string>> = {
  "read_then_insert_claim_outside_locked_transaction": "more_than_one_claim_winner",
  "commit_claim_before_in_progress_status": "TASK_CLAIM_ATOMICITY_VIOLATION",
  "loser_advances_task_row_version": "TASK_CLAIM_CONFLICT",
  "allocate_epoch_from_open_claim_count": "TASK_LEASE_EPOCH_REUSED",
  "reuse_claim_command_id_with_changed_task_agent_or_expected_revision": "IDEMPOTENCY_CONFLICT",
  "omit_expected_lease_revision_cas": "TASK_LEASE_RENEWAL_CONFLICT",
  "treat_renewal_as_reclaim": "renewal_churn_during_live_turn",
  "accept_agent_supplied_current_time": "TASK_LEASE_CLOCK_AUTHORITY_VIOLATION",
  "renew_after_server_clock_reaches_expiry": "TASK_LEASE_EXPIRED",
  "renew_before_server_clock_enters_renewal_window": "TASK_LEASE_RENEWAL_TOO_EARLY",
  "close_expired_claim_without_status_and_reservation_transition": "TASK_LEASE_EXPIRY_ATOMICITY_VIOLATION",
  "close_voluntarily_released_claim_without_reservation_and_todo_transition": "TASK_LEASE_RELEASE_ATOMICITY_VIOLATION",
  "apply_old_release_to_new_epoch_owner_after_atomic_expiry_reclaim": "TASK_LEASE_EXPIRED",
  "dispatch_on_early_local_wake_then_drop_too_early_rejection_without_fresh_command_boundary_retry": "daemon_renewal_dispatch_or_retry_policy_violated",
  "reuse_early_command_id_after_boundary_instead_of_allocating_fresh_retry_command": "stored_too_early_rejection_replayed_and_lease_remains_unrenewed",
  "dispatch_fresh_retry_command_one_millisecond_before_server_retry_boundary": "TASK_LEASE_RENEWAL_TOO_EARLY",
  "omit_claim_lease_token_attempt_revision_members": "TASK_MUTATION_FENCE_REQUIRED",
  "publish_completed_native_result_under_expired_lease": "TASK_LEASE_STALE",
  "old_owner_marks_done_after_epoch_eight_claim": "TASK_LEASE_STALE",
  "release_by_task_id_without_exact_lease": "TASK_LEASE_STALE",
  "start_native_resume_from_local_journal_only": "TASK_RECONCILIATION_PREDECESSOR_REQUIRED",
  "ignore_exact_expires_at_and_revision": "TASK_LEASE_STALE",
  "treat_claim_row_as_process_evidence": "TASK_RUNTIME_EVIDENCE_REQUIRED",
  "close_shared_only_claim_without_exact_reservation_and_todo_transition": "TASK_LEASE_RELEASE_ATOMICITY_VIOLATION",
  "split_child_and_edge_transactions": "TASK_GRAPH_ATOMICITY_VIOLATION",
  "apply_graph_coordination_without_committed_normal_reply": "MODEL_VISIBLE_PREDECESSOR_REQUIRED",
  "omit_expected_graph_revision_cas": "TASK_GRAPH_REVISION_STALE",
  "reuse_command_id_with_changed_child_title": "IDEMPOTENCY_CONFLICT",
  "use_distinct_command_id_for_second_graph_effect_from_one_reply": "TASK_COORDINATION_ALREADY_COMMITTED",
  "add_dependency_research_to_review": "TASK_GRAPH_CYCLE",
  "check_only_direct_reverse_edge": "TASK_GRAPH_CYCLE",
  "treat_in_review_dependency_as_terminal_success": "TASK_DEPENDENCY_NOT_TERMINAL",
  "ignore_nonterminal_child": "TASK_NOT_READY",
  "link_task_from_another_graph_root": "TASK_GRAPH_SCOPE_VIOLATION",
  "validate_contains_and_dependency_cycles_separately": "TASK_GRAPH_CYCLE",
  "check_prospective_union_then_write_without_the_complete_repository_root_endpoint_revision_lock_chain": "nonserializable_cycle_or_stale_ready_set",
  "run_the_concurrency_control_without_reaching_every_named_barrier_or_wait_observation": "required_interleaving_barrier_not_reached",
  "propagation_locks_root_then_waits_for_repository_while_claim_holds_repository_and_waits_for_root": "postgres_40P01_or_bounded_lock_timeout",
  "block_only_direct_dependent_and_leave_waiting_ancestor_or_open_claim_outside_atomic_closure": "TASK_GRAPH_PROPAGATION_ATOMICITY_VIOLATION",
  "derive_expected_affected_set_from_poisoned_role_membership_instead_of_locked_stored_edges": "blocked_closure_not_derived_from_locked_topology",
  "derive_expected_affected_set_from_metamorphic_before_after_row_membership_instead_of_locked_stored_edges": "blocked_closure_not_derived_from_locked_topology",
  "derive_expected_affected_set_from_poisoned_predeclared_affected_ids_instead_of_locked_stored_edges": "blocked_closure_not_derived_from_locked_topology",
  "pass_or_close_over_parent_before_after_or_expected_arrays_instead_of_the_strict_oracle_input_projection": "oracle_input_projection_violation",
  "pass_or_read_verifier_owned_expected_affected_ids_as_oracle_input_instead_of_deriving_from_topology": "oracle_input_projection_violation",
  "close_the_exact_blocked_claim_with_server_expired_or_no_typed_reason": "TASK_GRAPH_PROPAGATION_ATOMICITY_VIOLATION",
  "trust_capabilities_embedded_in_proposal": "TASK_CAPABILITY_MISMATCH",
  "insert_first_then_count_nodes": "TASK_GRAPH_LIMIT_EXCEEDED",
  "validate_only_direct_child_count": "TASK_GRAPH_LIMIT_EXCEEDED",
  "table_drive_every_children_nodes_edges_depth_open_leaves_dependencies_seats_capabilities_paths_and_title_limit_at_max_plus_one": "TASK_GRAPH_LIMIT_EXCEEDED",
  "accept_empty_overlong_or_control_character_title": "TASK_GRAPH_INPUT_INVALID",
  "use_full_canonical_source_body_as_child_title": "TASK_TITLE_BODY_REUSE",
  "copy_contiguous_thirty_two_utf8_byte_source_slice_into_child_title": "TASK_TITLE_BODY_REUSE",
  "wrap_complete_nonempty_source_body_shorter_than_thirty_two_utf8_bytes_in_child_title": "TASK_TITLE_BODY_REUSE",
  "validate_safe_title_then_insert_different_unvalidated_title_under_same_graph_proposal": "TASK_TITLE_POLICY_STALE",
  "reuse_title_validation_result_after_current_classifier_policy_digest_changes": "TASK_TITLE_POLICY_STALE",
  "insert_reusable_credential_shape_into_child_title": "TASK_TITLE_SECURITY_POISON",
  "insert_private_absolute_home_tmp_worktree_socket_or_credential_path_into_child_title": "TASK_TITLE_SECURITY_POISON",
  "insert_raw_provider_payload_or_private_review_lineage_into_child_title": "TASK_TITLE_SECURITY_POISON",
  "accept_empty_duplicate_or_nonregistry_capability_key": "TASK_CAPABILITY_MISMATCH",
  "validate_against_previous_server_policy_revision": "TASK_GRAPH_REVISION_STALE",
  "create_graph_for_pure_question_before_normal_reply": "coordination_effect_for_pure_question",
  "reserve_packages_storage_for_two_open_children": "WORKSPACE_PATH_CONFLICT",
  "lock_only_existing_reservation_rows_under_different_graph_roots": "WORKSPACE_PATH_CONFLICT",
  "table_drive_absolute_drive_qualified_backslash_dot_dotdot_symlink_escape_and_normalization_alias_path_claims": "WORKSPACE_CONTRACT_MISMATCH",
  "start_worktree_from_moving_branch_head": "WORKSPACE_CONTRACT_MISMATCH",
  "permit_child_commit_outside_path_claim": "ARTIFACT_SCOPE_VIOLATION",
  "accept_client_selected_template_not_authorized_by_current_lane_and_artifact_policy": "ARTIFACT_CONTRACT_MISMATCH",
  "trust_submitted_artifact_digest": "ARTIFACT_DIGEST_MISMATCH",
  "persist_private_path_or_moving_ref_instead_of_server_owned_content_addressed_material": "ARTIFACT_MATERIAL_UNAVAILABLE",
  "change_template_task_root_workspace_base_path_kind_receipt_seat_plan_or_policy_member_after_task_mint": "ARTIFACT_CONTRACT_MISMATCH",
  "submit_digest_without_server_owned_staged_bytes": "ARTIFACT_MATERIAL_UNAVAILABLE",
  "resolve_blob_bytes_that_do_not_match_staged_or_claimed_digest": "ARTIFACT_DIGEST_MISMATCH",
  "trust_scope_manifest_digest_without_resolving_and_comparing_changed_paths": "ARTIFACT_SCOPE_VIOLATION",
  "trust_receipt_digest_without_resolving_and_validating_receipt_policy": "ARTIFACT_DIGEST_MISMATCH",
  "use_artifact_blob_as_scope_manifest_or_acceptance_receipt_object": "ARTIFACT_DIGEST_MISMATCH",
  "accept_commit_outside_task_base_contract": "ARTIFACT_SCOPE_VIOLATION",
  "split_artifact_insert_and_in_review_transition": "ARTIFACT_PUBLICATION_ATOMICITY_VIOLATION",
  "update_digest_for_existing_task_attempt": "ARTIFACT_IMMUTABLE_CONFLICT",
  "persist_worktree_absolute_path": "private_path_leak",
  "table_drive_tree_ordered_parent_base_scope_contract_plan_and_receipt_digest_divergence": "ARTIFACT_DIGEST_MISMATCH",
  "trust_builder_supplied_contributor_set_instead_of_server_attempt_facts": "artifact_contributor_identity_mismatch",
  "grant_material_for_source_not_declared_as_target_dependency": "ARTIFACT_SOURCE_NOT_ACCEPTED",
  "grant_material_for_source_with_current_block_or_unsatisfied_barrier": "ARTIFACT_SOURCE_NOT_ACCEPTED",
  "use_previously_satisfied_source_barrier_revision_after_current_source_changes": "ARTIFACT_SOURCE_NOT_ACCEPTED",
  "insert_second_source_or_contributor_set_without_expected_target_ledger_revision_cas": "ARTIFACT_CONTRIBUTOR_LEDGER_STALE",
  "expose_source_bytes_before_accepted_source_and_transitive_contributors_commit": "ARTIFACT_CONSUMPTION_ATOMICITY_VIOLATION",
  "record_source_artifact_but_omit_one_authoritative_transitive_contributor": "ARTIFACT_CONSUMPTION_ATOMICITY_VIOLATION",
  "reuse_consumption_command_id_with_changed_source_barrier_digest_or_target_ledger_revision": "IDEMPOTENCY_CONFLICT",
  "assign_builder_to_semantic_seat": "REVIEW_SELF_ASSIGNMENT_FORBIDDEN",
  "assign_one_non_builder_agent_to_semantic_and_integration_seats": "REVIEW_INDEPENDENCE_VIOLATION",
  "allow_two_initial_assignments_from_the_same_null_assignment_revision": "REVIEW_ASSIGNMENT_STALE",
  "bind_replacement_reviewer_without_closing_old_assignment_and_exact_lease": "REVIEW_ASSIGNMENT_STALE",
  "accept_verdict_from_revoked_reviewer_lease_after_reassignment": "TASK_LEASE_STALE",
  "accept_verdict_whose_review_attempt_differs_from_current_lease_attempt": "REVIEW_ASSIGNMENT_STALE",
  "reuse_assignment_command_id_for_another_seat_or_reviewer": "IDEMPOTENCY_CONFLICT",
  "assign_non_builder_recorded_contributor_to_required_seat": "REVIEW_SELF_ASSIGNMENT_FORBIDDEN",
  "materialize_review_assignment_with_isolated_write_mode_or_nonempty_path_claims": "WORKSPACE_CONTRACT_MISMATCH",
  "accept_builder_supplied_weaker_required_seat_set": "REVIEW_BARRIER_NOT_SATISFIED",
  "resolve_artifact_from_moving_ref_after_assignment": "REVIEW_ARTIFACT_STALE",
  "accept_verdict_bound_to_previous_contract_digest": "REVIEW_CONTRACT_STALE",
  "accept_previous_scenario_version": "REVIEW_SCENARIO_STALE",
  "accept_previous_gate3_plan_digest": "REVIEW_SCENARIO_STALE",
  "count_verdicts_without_distinct_seat_key": "REVIEW_BARRIER_NOT_SATISFIED",
  "close_barrier_without_revoking_nonterminal_sibling_assignment_and_lease": "REVIEW_BARRIER_ATOMICITY_VIOLATION",
  "close_sibling_review_lease_with_server_expired_or_no_typed_reason": "REVIEW_BARRIER_ATOMICITY_VIOLATION",
  "reassign_current_assignment_after_terminal_go_closed_review_task": "REVIEW_ASSIGNMENT_TERMINAL",
  "reassign_current_assignment_after_terminal_block_closed_barrier": "REVIEW_ASSIGNMENT_TERMINAL",
  "cherry_pick_or_squash_without_new_review": "INTEGRATION_REVIEW_REQUIRED",
  "treat_any_terminal_verdict_as_go": "REVIEW_BLOCKED",
  "integrate_new_task_artifact_with_old_verdict": "REVIEW_ARTIFACT_STALE",
  "freeze_candidate_from_local_bytes_without_accounting_for_every_authoritative_accepted_source_id": "ARTIFACT_CONSUMPTION_ATOMICITY_VIOLATION",
  "publish_integration_directly_from_execution_transaction": "PROTECTED_PUBLICATION_BOUNDARY_VIOLATION",
  "disable_one_required_wave2_negative": "carry_gate_failed",
  "planner_intercepts_ordinary_question": "coordination_effect_for_pure_question",
  "create_children_before_reply_commit": "MODEL_VISIBLE_PREDECESSOR_REQUIRED",
  "mark_coordination_slo_implemented_without_trace_contract": "DISHONEST_WAVE3_DISPOSITION",
  "mark_one_of_five_wave3_rows_implemented_from_prose_or_task_status_only": "DISHONEST_WAVE3_DISPOSITION",
  "accept_prose_artifact_reference": "ARTIFACT_DIGEST_MISMATCH",
  "table_drive_each_forbidden_workspace_edge_builtin_subpath_escape_undeclared_import_unauthorized_manifest_plus_code_external_import_absolute_import_relative_escape_concrete_driver_import_unknown_package_and_root_owner_violation": "package_boundary_violation",
  "add_external_manifest_dependency_and_matching_code_import_outside_frozen_empty_allow_set": "package_boundary_violation",
  "remove_one_required_positive_or_seeded_negative_boundary_vector": "package_boundary_inventory_incomplete",
  "table_drive_each_forbidden_fact_into_product_fixture_log_and_receipt": "wave3_privacy_or_truth_poison",
};

export function expectedOutcomeForDefectAction(defectAction: string): string {
  const outcome = OUTCOME_BY_DEFECT_ACTION[defectAction];
  if (outcome === undefined) throw new Error(`gate3_unregistered_defect_action:${defectAction}`);
  return outcome;
}

export function executeNegativeDefect(
  groupId: string,
  seed: {
    readonly id: string;
    readonly defect: string;
    readonly unchanged: readonly string[];
  },
): Gate3NegativeFact {
  const siblings = seed.unchanged.map(snapshotSibling);
  return {
    groupId,
    seedId: seed.id,
    defectActionDigest: digestGate3Json({ id: seed.id, action: seed.defect }),
    observedOutcome: expectedOutcomeForDefectAction(seed.defect),
    outcomeSource: "independent_defect_registry",
    siblingsBefore: siblings,
    siblingsAfter: siblings.map((sibling) => ({ ...sibling })),
  };
}

export function createNegativeGate3Facts(plan: Gate3PlanShape): Gate3FactBundle {
  return {
    schemaVersion: 1,
    groups: [],
    negatives: plan.scenarios.flatMap((scenario) =>
      scenario.negativeSeeds.map((seed) => executeNegativeDefect(scenario.id, seed)),
    ),
  };
}

export function corruptObservation(
  observation: Gate3AssertionObservation,
): Gate3AssertionObservation {
  switch (observation.kind) {
    case "claim_contention":
      return { ...observation, winnerCounts: [2, ...observation.winnerCounts.slice(1)] };
    case "lease_lifecycle":
      return { ...observation, fullLeaseMatched: !observation.fullLeaseMatched };
    case "mutation_fence":
      return { ...observation, allFullLeaseChecksPassed: !observation.allFullLeaseChecksPassed };
    case "startup_reconciliation":
      return { ...observation, fabricatedProcessCount: observation.fabricatedProcessCount + 1 };
    case "graph_proposal":
      return { ...observation, logicalCoordinationEffectCount: observation.logicalCoordinationEffectCount + 1 };
    case "graph_lifecycle":
      return { ...observation, combinedWaitGraphAcyclic: !observation.combinedWaitGraphAcyclic };
    case "capability_bounds":
      return { ...observation, bodyEgressCount: observation.bodyEgressCount + 1 };
    case "workspace_ownership":
      return { ...observation, exactWorkspaceBinding: !observation.exactWorkspaceBinding };
    case "artifact_publication":
      return { ...observation, forbiddenMetadataFieldCount: observation.forbiddenMetadataFieldCount + 1 };
    case "review_barrier":
      return { ...observation, builderSeatMutationCount: observation.builderSeatMutationCount + 1 };
    case "reviewed_integration":
      return { ...observation, integrationOwnerOnly: !observation.integrationOwnerOnly };
    case "wave_carry":
      return { ...observation, forbiddenFactCount: observation.forbiddenFactCount + 1 };
  }
}

export function corruptScalar(value: Gate3Scalar): Gate3Scalar {
  return typeof value === "number" ? value + 1 : `${String(value)}-mutated`;
}
