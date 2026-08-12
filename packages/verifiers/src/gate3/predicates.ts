import { Gate3OracleError } from "./types.js";
import type {
  Gate3AssertionObservation,
  Gate3Json,
} from "./types.js";

type Fixture = Readonly<Record<string, Gate3Json>>;

// Verifier-owned and independent from the producer registry.
export const GATE3_ASSERTION_PREDICATE_IDS = Object.freeze([
  "exactly_one_claim_winner_for_two_three_and_five_contenders",
  "winner_and_losers_share_one_locked_task_revision",
  "winner_status_and_claim_insert_commit_atomically",
  "losers_receive_conflict_without_claim_or_status_effect",
  "lease_epoch_is_previous_max_plus_one",
  "claim_command_exact_replay_aliases_after_status_advances",
  "renewal_uses_exact_full_lease_and_expected_revision",
  "renewal_keeps_epoch_and_fence_token_but_advances_revision",
  "renewed_expiry_is_strictly_later_and_server_clock_authoritative",
  "long_native_turn_renews_without_reclaim_churn",
  "expired_lease_cannot_renew_or_publish",
  "server_expiry_closes_exact_claim_and_reservations_then_reopens_task_for_epoch_plus_one_reclaim",
  "standalone_release_atomically_closes_current_claim_reservations_and_returns_task_to_todo",
  "released_or_expired_task_reclaims_with_next_history_inclusive_epoch",
  "daemon_never_dispatches_renewal_before_window_and_retries_fresh_command_after_too_early",
  "too_early_command_replays_stored_rejection_while_fresh_retry_waits_for_retry_not_before",
  "every_task_artifact_and_review_request_mutation_checks_full_current_lease",
  "stale_owner_has_zero_shared_mutation",
  "owner_loss_requests_local_interrupt_before_any_later_effect",
  "late_native_result_is_observation_only_after_owner_loss",
  "new_epoch_owner_is_not_affected_by_stale_cleanup",
  "matching_current_execution_renews_before_resume",
  "expired_or_replaced_execution_stops_and_suppresses_publication",
  "local_only_execution_is_stopped_without_shared_mutation",
  "shared_only_claim_is_relinquished_or_left_for_server_expiry_without_fabricated_process",
  "shared_only_relinquish_closes_exact_claim_and_reservation_before_todo_reclaim",
  "wave3_reconciliation_does_not_claim_full_wave4_crash_recovery",
  "one_logical_coordination_effect_atomically_creates_children_edges_and_one_graph_revision",
  "proposal_replay_aliases_exact_stored_result",
  "changed_payload_under_same_command_conflicts",
  "source_message_turn_producer_and_current_lease_join_before_graph_write",
  "parent_transitions_to_waiting_in_same_transaction",
  "one_reply_permits_one_logical_coordination_effect_with_same_command_replay_only",
  "contains_and_dependency_edges_are_distinct_and_combined_lifecycle_wait_graph_is_acyclic",
  "only_leaf_with_all_dependencies_done_is_claimable",
  "dependency_completion_atomically_recomputes_ready_set",
  "blocked_dependency_fail_closes_dependents",
  "cross_root_or_cross_server_edges_are_rejected",
  "concurrent_endpoint_or_ancestor_mutation_serializes_before_cycle_and_readiness_commit",
  "blocked_prerequisite_transitively_blocks_dependents_and_waiting_ancestors_atomically",
  "blocked_propagation_derives_complete_affected_closure_from_exact_stored_topology",
  "blocked_propagation_records_exact_graph_blocked_lease_reason_and_typed_before_after_rows",
  "repository_first_global_lock_order_forces_both_serial_orders_and_prevents_claim_propagation_deadlock",
  "proposal_and_claim_validate_capabilities_from_current_server_registry",
  "all_graph_limits_apply_before_any_insert",
  "limit_policy_digest_is_stored_with_graph_revision",
  "titles_and_capability_keys_are_canonical_bounded_values",
  "delegated_title_privacy_gate_runs_before_graph_insert_without_body_egress",
  "whole_short_source_body_is_rejected_even_when_wrapped",
  "graph_persists_exact_title_and_current_classifier_policy_digests",
  "normal_conversation_requires_no_global_planner",
  "each_task_binds_exact_base_workspace_generation_and_allowed_paths",
  "path_claims_are_normalized_relative_and_non_overlapping",
  "repository_scoped_serialization_gives_one_cross_root_overlap_winner",
  "shared_file_collision_rejected_or_owned_by_one_integration_task",
  "child_worktrees_cannot_mutate_integration_owner_paths",
  "workspace_contract_digest_changes_on_any_policy_member",
  "artifact_contract_template_is_selected_by_current_server_policy_not_builder",
  "artifact_contract_template_materializes_non_circular_exact_task_workspace_preimage",
  "artifact_bytes_commit_tree_parent_scope_and_contract_bind_before_publication",
  "published_artifact_material_remains_resolvable_after_private_worktree_disappears",
  "publication_and_task_in_review_transition_are_atomic",
  "exact_replay_aliases_one_immutable_artifact",
  "same_attempt_divergence_conflicts_without_replacing_artifact",
  "builder_and_contributors_derive_from_server_attempt_facts",
  "contributor_ledger_is_server_written_and_transitively_expands_accepted_upstream_artifacts",
  "accepted_artifact_consumption_locks_source_barrier_material_and_target_ledger_before_byte_grant",
  "accepted_artifact_consumption_transitively_expands_contributors_and_source_set_atomically",
  "consumption_replay_aliases_one_grant_and_ledger_revision",
  "artifact_metadata_contains_no_body_credential_or_private_path",
  "builder_cannot_claim_or_submit_any_required_reviewer_seat",
  "distinct_required_seats_use_distinct_reviewer_agents",
  "publication_mints_one_stable_unassigned_review_task_per_required_seat",
  "assignment_and_reassignment_are_revisioned_atomic_and_unique",
  "reviewer_receives_only_exact_frozen_artifact_contract_and_scenario",
  "review_verdict_attempt_equals_current_reviewer_lease_attempt",
  "replacement_reviewer_new_attempt_can_submit_only_the_exact_current_verdict",
  "each_required_seat_has_one_current_terminal_verdict",
  "terminal_verdict_closes_assignment_and_prevents_reassignment",
  "all_required_go_verdicts_same_artifact_unlock_barrier",
  "one_block_or_missing_seat_keeps_barrier_closed",
  "one_block_revokes_nonterminal_sibling_review_tasks_and_late_verdicts",
  "barrier_block_closes_sibling_review_lease_with_exact_review_barrier_blocked_reason",
  "integration_owner_alone_may_consume_reviewed_child_artifacts",
  "fast_forward_requires_exact_reviewed_commit_chain_without_rewrite",
  "new_integration_sha_reenters_full_review_barrier",
  "blocked_or_stale_child_never_enters_integration_candidate",
  "integration_candidate_uses_authoritative_consumed_source_set_and_current_ledger_revision",
  "protected_publication_stays_outside_task_execution_transaction",
  "all_wave0_wave1_and_wave2_gates_remain_green",
  "pure_question_still_has_one_normal_reply_and_zero_task_effect",
  "task_graph_coordination_still_has_committed_reply_predecessor_and_one_logical_effect",
  "five_wave3_deferred_rows_bind_only_to_real_task_artifact_and_freshness_facts",
  "coordination_slo_and_channel_navigation_remain_wave5_fail_closed",
  "seed_m2_binds_exact_execute_owner_artifact_path_and_digest",
  "exact_new_package_import_builtin_and_owner_boundaries_pass_with_complete_seeded_negatives",
  "new_package_manifests_match_exact_workspace_dependencies_and_empty_external_allow_sets",
  "all_wave3_forbidden_facts_absent_from_product_fixture_log_and_receipt",
] as const);

type ObservationSchema = {
  readonly keys: readonly string[];
  readonly strings?: readonly string[];
  readonly integers?: readonly string[];
  readonly booleans?: readonly string[];
  readonly stringArrays?: readonly string[];
  readonly numberArrays?: readonly string[];
  readonly jsonArrays?: readonly string[];
};

const SCHEMA: Readonly<Record<Gate3AssertionObservation["kind"], ObservationSchema>> = {
  claim_contention: {
    keys: ["kind", "taskId", "initialStatus", "dependencyState", "contenderCounts", "winnerCounts", "lockedRevisionCountPerRound", "statusAndClaimAtomic", "loserOutcome", "loserSharedMutationCount", "previousMaxEpoch", "winnerEpoch", "exactReplayAliasedAfterStatusAdvance"],
    strings: ["taskId", "initialStatus", "dependencyState", "loserOutcome"],
    integers: ["loserSharedMutationCount", "previousMaxEpoch", "winnerEpoch"],
    booleans: ["statusAndClaimAtomic", "exactReplayAliasedAfterStatusAdvance"],
    numberArrays: ["contenderCounts", "winnerCounts", "lockedRevisionCountPerRound"],
  },
  lease_lifecycle: {
    keys: ["kind", "leaseEpoch", "previousLeaseRevision", "currentLeaseRevision", "fullLeaseMatched", "epochStable", "fenceTokenStable", "acquiredAt", "expiresAt", "renewedExpiresAt", "serverClockAuthoritative", "nativeTurnDurationMs", "reclaimCountDuringTurn", "expiredRenewRejected", "expiredPublishRejected", "serverExpiryAtomic", "standaloneReleaseAtomic", "reclaimedEpoch", "earlyCommandId", "retryCommandId", "earlyDispatchSuppressed", "retryDispatchAt", "retryNotBefore", "earlyReplayCode", "retryUsesFreshCommand"],
    strings: ["acquiredAt", "expiresAt", "renewedExpiresAt", "earlyCommandId", "retryCommandId", "retryDispatchAt", "retryNotBefore", "earlyReplayCode"],
    integers: ["leaseEpoch", "previousLeaseRevision", "currentLeaseRevision", "nativeTurnDurationMs", "reclaimCountDuringTurn", "reclaimedEpoch"],
    booleans: ["fullLeaseMatched", "epochStable", "fenceTokenStable", "serverClockAuthoritative", "expiredRenewRejected", "expiredPublishRejected", "serverExpiryAtomic", "standaloneReleaseAtomic", "earlyDispatchSuppressed", "retryUsesFreshCommand"],
  },
  mutation_fence: {
    keys: ["kind", "taskStatus", "leaseEpoch", "leaseRevision", "checkedMutationKinds", "allFullLeaseChecksPassed", "staleOwnerSharedMutationCount", "ownershipLossSequence", "lateResultDisposition", "replacementEpoch", "staleCleanupAffectedReplacement"],
    strings: ["taskStatus", "lateResultDisposition"],
    integers: ["leaseEpoch", "leaseRevision", "staleOwnerSharedMutationCount", "replacementEpoch"],
    booleans: ["allFullLeaseChecksPassed", "staleCleanupAffectedReplacement"],
    stringArrays: ["checkedMutationKinds", "ownershipLossSequence"],
  },
  startup_reconciliation: {
    keys: ["kind", "cases", "matchingCurrentSequence", "expiredAndReplacedStop", "expiredAndReplacedPublicationCount", "localOnlyStopped", "localOnlySharedMutationCount", "sharedOnlyDisposition", "fabricatedProcessCount", "relinquishAtomic", "fullCrashRecoveryDeferred"],
    strings: ["sharedOnlyDisposition"],
    integers: ["expiredAndReplacedPublicationCount", "localOnlySharedMutationCount", "fabricatedProcessCount"],
    booleans: ["expiredAndReplacedStop", "localOnlyStopped", "relinquishAtomic", "fullCrashRecoveryDeferred"],
    stringArrays: ["cases", "matchingCurrentSequence"],
  },
  graph_proposal: {
    keys: ["kind", "rootTaskId", "childKeys", "expectedGraphRevision", "committedReplyPresent", "logicalCoordinationEffectCount", "childrenEdgesAndRevisionAtomic", "exactReplayAliased", "changedPayloadOutcome", "sourceJoinMembers", "sourceJoinPassed", "parentStatusAfter", "distinctSecondCommandOutcome"],
    strings: ["rootTaskId", "changedPayloadOutcome", "parentStatusAfter", "distinctSecondCommandOutcome"],
    integers: ["expectedGraphRevision", "logicalCoordinationEffectCount"],
    booleans: ["committedReplyPresent", "childrenEdgesAndRevisionAtomic", "exactReplayAliased", "sourceJoinPassed"],
    stringArrays: ["childKeys", "sourceJoinMembers"],
  },
  graph_lifecycle: {
    keys: ["kind", "taskNames", "containsEdges", "dependencyEdges", "edgeKindsDistinct", "combinedWaitGraphAcyclic", "onlyReadyLeafClaimable", "dependencyCompletionAtomic", "blockedDependencyFailClosed", "crossScopeEdgesRejected", "reachedPgLatches", "pgSchedules", "blockedAffectedTaskIds", "closureDerivedFromStoredTopology", "allAffectedBlocked", "graphBlockedLeaseReasonExact", "typedBeforeAfterRowsExact", "lockOrder", "bothSerialOrdersObserved", "deadlockCount"],
    strings: ["lockOrder"],
    integers: ["deadlockCount"],
    booleans: ["edgeKindsDistinct", "combinedWaitGraphAcyclic", "onlyReadyLeafClaimable", "dependencyCompletionAtomic", "blockedDependencyFailClosed", "crossScopeEdgesRejected", "closureDerivedFromStoredTopology", "allAffectedBlocked", "graphBlockedLeaseReasonExact", "typedBeforeAfterRowsExact", "bothSerialOrdersObserved"],
    stringArrays: ["taskNames", "reachedPgLatches", "pgSchedules", "blockedAffectedTaskIds"],
    jsonArrays: ["containsEdges", "dependencyEdges"],
  },
  capability_bounds: {
    keys: ["kind", "requiredCapabilities", "agentCapabilities", "registryValidatedAtProposalAndClaim", "limitsSource", "allLimitsCheckedBeforeInsert", "limitPolicyDigestStored", "canonicalTitlesAndCapabilities", "titlePrivacyGateDelegated", "bodyEgressCount", "shortSourceBody", "wrappedShortTitle", "wrappedWholeBodyRejected", "titleDigestStored", "classifierPolicyDigest", "classifierPolicyDigestStored", "globalPlannerRequired"],
    strings: ["limitsSource", "shortSourceBody", "wrappedShortTitle", "classifierPolicyDigest"],
    integers: ["bodyEgressCount"],
    booleans: ["registryValidatedAtProposalAndClaim", "allLimitsCheckedBeforeInsert", "limitPolicyDigestStored", "canonicalTitlesAndCapabilities", "titlePrivacyGateDelegated", "wrappedWholeBodyRejected", "titleDigestStored", "classifierPolicyDigestStored", "globalPlannerRequired"],
    stringArrays: ["requiredCapabilities", "agentCapabilities"],
  },
  workspace_ownership: {
    keys: ["kind", "baseCommit", "workspaceGeneration", "childPathClaims", "integrationOwnerPathClaims", "exactWorkspaceBinding", "pathsNormalizedRelativeNonoverlap", "crossRootContenderCounts", "crossRootWinnerCounts", "sharedCollisionDisposition", "childIntegrationOwnerMutationCount", "digestSensitiveMembers"],
    strings: ["baseCommit", "sharedCollisionDisposition"],
    integers: ["workspaceGeneration", "childIntegrationOwnerMutationCount"],
    booleans: ["exactWorkspaceBinding", "pathsNormalizedRelativeNonoverlap"],
    stringArrays: ["digestSensitiveMembers"],
    numberArrays: ["crossRootContenderCounts", "crossRootWinnerCounts"],
    jsonArrays: ["childPathClaims", "integrationOwnerPathClaims"],
  },
  artifact_publication: {
    keys: ["kind", "artifactKinds", "stagedRoles", "templateAuthority", "nonCircularTaskWorkspacePreimage", "boundObjectMembers", "materialResolution", "publicationTaskTransitionAtomic", "exactReplayAliased", "divergentAttemptOutcome", "contributorSources", "contributorLedgerAuthority", "contributorLedgerTransitive", "consumptionLockOrder", "materialGrantedAfterLedgerCommit", "sourceSetExpandedAtomically", "consumptionReplayAliased", "targetContributorLedgerRevision", "forbiddenMetadataFieldCount"],
    strings: ["templateAuthority", "materialResolution", "divergentAttemptOutcome", "contributorLedgerAuthority"],
    integers: ["targetContributorLedgerRevision", "forbiddenMetadataFieldCount"],
    booleans: ["nonCircularTaskWorkspacePreimage", "publicationTaskTransitionAtomic", "exactReplayAliased", "contributorLedgerTransitive", "materialGrantedAfterLedgerCommit", "sourceSetExpandedAtomically", "consumptionReplayAliased"],
    stringArrays: ["artifactKinds", "stagedRoles", "boundObjectMembers", "contributorSources", "consumptionLockOrder"],
  },
  review_barrier: {
    keys: ["kind", "requiredSeats", "builderAgentId", "builderSeatMutationCount", "distinctReviewerCount", "stableReviewTaskCount", "assignmentRevision", "assignmentUnique", "reviewerInputMembers", "verdictAttempt", "currentReviewAttempt", "replacementAttemptAccepted", "terminalVerdictCountPerSeat", "terminalAssignmentClosed", "terminalReassignmentRejected", "allGoUnlocksBarrier", "blockOrMissingKeepsClosed", "blockRevokesSiblingTasks", "lateSiblingVerdictRejected", "blockedLeaseReason"],
    strings: ["builderAgentId", "blockedLeaseReason"],
    integers: ["builderSeatMutationCount", "distinctReviewerCount", "stableReviewTaskCount", "assignmentRevision", "verdictAttempt", "currentReviewAttempt", "terminalVerdictCountPerSeat"],
    booleans: ["assignmentUnique", "replacementAttemptAccepted", "terminalAssignmentClosed", "terminalReassignmentRejected", "allGoUnlocksBarrier", "blockOrMissingKeepsClosed", "blockRevokesSiblingTasks", "lateSiblingVerdictRejected"],
    stringArrays: ["requiredSeats", "reviewerInputMembers"],
  },
  reviewed_integration: {
    keys: ["kind", "childCommits", "childBarriers", "integrationModes", "integrationOwnerOnly", "fastForwardNoRewrite", "newCommitRequiresFullReview", "blockedOrStaleIncludedCount", "authoritativeConsumedSourceSet", "currentLedgerRevisionChecked", "protectedPublicationInsideTaskTransaction"],
    strings: ["childBarriers"],
    integers: ["blockedOrStaleIncludedCount"],
    booleans: ["integrationOwnerOnly", "fastForwardNoRewrite", "newCommitRequiresFullReview", "authoritativeConsumedSourceSet", "currentLedgerRevisionChecked", "protectedPublicationInsideTaskTransaction"],
    stringArrays: ["childCommits", "integrationModes"],
  },
  wave_carry: {
    keys: ["kind", "wave0Wave1Wave2Gates", "pureQuestionReplyCount", "pureQuestionTaskEffectCount", "coordinationReplyPredecessorCommitted", "logicalCoordinationEffectCount", "wave3CandidateRows", "wave3RowsBoundToMachineFacts", "wave5DeferredRows", "seedM2Disposition", "seedM2ExactArtifactBinding", "boundaryPolicyCount", "requiredBoundaryNegativeKinds", "manifestWorkspaceDependenciesExact", "externalAllowSetsEmpty", "forbiddenFactCount"],
    strings: ["wave0Wave1Wave2Gates", "seedM2Disposition"],
    integers: ["pureQuestionReplyCount", "pureQuestionTaskEffectCount", "logicalCoordinationEffectCount", "boundaryPolicyCount", "forbiddenFactCount"],
    booleans: ["coordinationReplyPredecessorCommitted", "wave3RowsBoundToMachineFacts", "seedM2ExactArtifactBinding", "manifestWorkspaceDependenciesExact", "externalAllowSetsEmpty"],
    stringArrays: ["wave3CandidateRows", "wave5DeferredRows", "requiredBoundaryNegativeKinds"],
  },
};

function exactOwnKeys(value: object, expected: readonly string[], label: string): void {
  const actual = Reflect.ownKeys(value);
  if (
    actual.some((key) => typeof key !== "string") ||
    actual.length !== expected.length ||
    expected.some((key) => !Object.hasOwn(value, key))
  ) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} exact keys`);
  }
}

function isJson(value: unknown): value is Gate3Json {
  if (value === null || ["string", "number", "boolean"].includes(typeof value)) return true;
  if (Array.isArray(value)) return value.every(isJson);
  return typeof value === "object" && Reflect.ownKeys(value).every((key) =>
    typeof key === "string" && isJson((value as Record<string, unknown>)[key]),
  );
}

export function validateDomainObservation(
  observation: Gate3AssertionObservation,
  label: string,
): void {
  if (typeof observation !== "object" || observation === null || Array.isArray(observation)) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} object`);
  }
  const schema = SCHEMA[observation.kind];
  if (schema === undefined) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} kind`);
  }
  exactOwnKeys(observation, schema.keys, label);
  const record = observation as unknown as Record<string, unknown>;
  if (schema.strings !== undefined && !schema.strings.every((key) => typeof record[key] === "string")) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} strings`);
  }
  if (schema.integers !== undefined && !schema.integers.every((key) => Number.isSafeInteger(record[key]) && (record[key] as number) >= 0)) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} integers`);
  }
  if (schema.booleans !== undefined && !schema.booleans.every((key) => typeof record[key] === "boolean")) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} booleans`);
  }
  if (schema.stringArrays !== undefined && !schema.stringArrays.every((key) => Array.isArray(record[key]) && (record[key] as unknown[]).every((entry) => typeof entry === "string"))) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} string arrays`);
  }
  if (schema.numberArrays !== undefined && !schema.numberArrays.every((key) => Array.isArray(record[key]) && (record[key] as unknown[]).every((entry) => Number.isSafeInteger(entry) && (entry as number) >= 0))) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} number arrays`);
  }
  if (schema.jsonArrays !== undefined && !schema.jsonArrays.every((key) => Array.isArray(record[key]) && (record[key] as unknown[]).every(isJson))) {
    throw new Gate3OracleError("GATE3_FACT_SCHEMA_INVALID", `${label} JSON arrays`);
  }
}

function canonical(value: Gate3Json): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const record = value as Readonly<Record<string, Gate3Json>>;
  return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonical(record[key]!)}`).join(",")}}`;
}

function equalJson(left: Gate3Json, right: Gate3Json): boolean {
  return canonical(left) === canonical(right);
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return new Set(left).size === left.length &&
    new Set(right).size === right.length &&
    equalJson([...left].sort(), [...right].sort());
}

function sameNumbers(left: readonly number[], right: readonly number[]): boolean {
  return equalJson(left, right);
}

function fixtureValue(fixture: Fixture, key: string): Gate3Json {
  const result = fixture[key];
  if (result === undefined) throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  return result as Readonly<Record<string, Gate3Json>>;
}

function fixtureText(fixture: Fixture, key: string): string {
  const result = fixtureValue(fixture, key);
  if (typeof result !== "string") throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  return result;
}

function fixtureInteger(fixture: Fixture, key: string): number {
  const result = fixtureValue(fixture, key);
  if (!Number.isSafeInteger(result)) throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  return result as number;
}

function fixtureStrings(fixture: Fixture, key: string): readonly string[] {
  const result = fixtureValue(fixture, key);
  if (!Array.isArray(result) || !result.every((entry) => typeof entry === "string")) {
    throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  }
  return result;
}

function fixtureNumbers(fixture: Fixture, key: string): readonly number[] {
  const result = fixtureValue(fixture, key);
  if (!Array.isArray(result) || !result.every((entry) => Number.isSafeInteger(entry))) {
    throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  }
  return result as readonly number[];
}

function fixtureRecord(fixture: Fixture, key: string): Readonly<Record<string, Gate3Json>> {
  const result = fixtureValue(fixture, key);
  if (typeof result !== "object" || result === null || Array.isArray(result)) {
    throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  }
  return result as Readonly<Record<string, Gate3Json>>;
}

function recordValue(record: Readonly<Record<string, Gate3Json>>, key: string): Gate3Json {
  const result = record[key];
  if (result === undefined) throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  return result;
}

function recordText(record: Readonly<Record<string, Gate3Json>>, key: string): string {
  const result = recordValue(record, key);
  if (typeof result !== "string") throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  return result;
}

function recordArray(record: Readonly<Record<string, Gate3Json>>, key: string): readonly Gate3Json[] {
  const result = recordValue(record, key);
  if (!Array.isArray(result)) throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  return result;
}

function isLater(later: string, earlier: string): boolean {
  const left = Date.parse(later);
  const right = Date.parse(earlier);
  return Number.isFinite(left) && Number.isFinite(right) && left > right;
}

function everyOne(values: readonly number[]): boolean {
  return values.length > 0 && values.every((value) => value === 1);
}

function rowsFrom(record: Readonly<Record<string, Gate3Json>>, key: string): readonly Readonly<Record<string, Gate3Json>>[] {
  const rows = recordArray(record, key);
  if (!rows.every((row) => typeof row === "object" && row !== null && !Array.isArray(row))) {
    throw new Gate3OracleError("GATE3_PLAN_FIXTURE_INVALID", key);
  }
  return rows as readonly Readonly<Record<string, Gate3Json>>[];
}

export function assertionPasses(
  assertionId: string,
  observation: Gate3AssertionObservation,
  fixture: Fixture,
): boolean {
  switch (assertionId) {
    case "exactly_one_claim_winner_for_two_three_and_five_contenders":
      return observation.kind === "claim_contention" && sameNumbers(observation.contenderCounts, fixtureNumbers(fixture, "contenderCounts")) && everyOne(observation.winnerCounts);
    case "winner_and_losers_share_one_locked_task_revision":
      return observation.kind === "claim_contention" && observation.taskId === fixtureText(fixture, "taskId") && everyOne(observation.lockedRevisionCountPerRound) && observation.lockedRevisionCountPerRound.length === observation.contenderCounts.length;
    case "winner_status_and_claim_insert_commit_atomically":
      return observation.kind === "claim_contention" && observation.initialStatus === fixtureText(fixture, "taskStatus") && observation.dependencyState === fixtureText(fixture, "dependencyState") && observation.statusAndClaimAtomic;
    case "losers_receive_conflict_without_claim_or_status_effect":
      return observation.kind === "claim_contention" && observation.loserOutcome === "TASK_CLAIM_CONFLICT" && observation.loserSharedMutationCount === 0;
    case "lease_epoch_is_previous_max_plus_one":
      return observation.kind === "claim_contention" && observation.winnerEpoch === observation.previousMaxEpoch + 1 && fixtureText(fixture, "taskStatus") === "todo";
    case "claim_command_exact_replay_aliases_after_status_advances":
      return observation.kind === "claim_contention" && observation.exactReplayAliasedAfterStatusAdvance && observation.taskId === fixtureText(fixture, "taskId");

    case "renewal_uses_exact_full_lease_and_expected_revision":
      return observation.kind === "lease_lifecycle" && observation.fullLeaseMatched && observation.leaseEpoch === fixtureInteger(fixture, "leaseEpoch") && observation.previousLeaseRevision === fixtureInteger(fixture, "leaseRevision");
    case "renewal_keeps_epoch_and_fence_token_but_advances_revision":
      return observation.kind === "lease_lifecycle" && observation.epochStable && observation.fenceTokenStable && observation.currentLeaseRevision === observation.previousLeaseRevision + 1;
    case "renewed_expiry_is_strictly_later_and_server_clock_authoritative":
      return observation.kind === "lease_lifecycle" && observation.expiresAt === fixtureText(fixture, "expiresAt") && observation.renewedExpiresAt === fixtureText(fixture, "renewedExpiresAt") && isLater(observation.renewedExpiresAt, observation.expiresAt) && observation.serverClockAuthoritative;
    case "long_native_turn_renews_without_reclaim_churn":
      return observation.kind === "lease_lifecycle" && observation.nativeTurnDurationMs === fixtureInteger(fixture, "nativeTurnDurationMs") && observation.nativeTurnDurationMs > Date.parse(observation.expiresAt) - Date.parse(observation.acquiredAt) && observation.reclaimCountDuringTurn === 0;
    case "expired_lease_cannot_renew_or_publish":
      return observation.kind === "lease_lifecycle" && observation.expiredRenewRejected && observation.expiredPublishRejected && observation.leaseEpoch === fixtureInteger(fixture, "leaseEpoch");
    case "server_expiry_closes_exact_claim_and_reservations_then_reopens_task_for_epoch_plus_one_reclaim":
      return observation.kind === "lease_lifecycle" && observation.serverExpiryAtomic && observation.reclaimedEpoch === fixtureInteger(fixture, "leaseEpoch") + 1;
    case "standalone_release_atomically_closes_current_claim_reservations_and_returns_task_to_todo":
      return observation.kind === "lease_lifecycle" && observation.standaloneReleaseAtomic && observation.fullLeaseMatched;
    case "released_or_expired_task_reclaims_with_next_history_inclusive_epoch":
      return observation.kind === "lease_lifecycle" && observation.reclaimedEpoch === observation.leaseEpoch + 1 && observation.leaseEpoch === fixtureInteger(fixture, "leaseEpoch");
    case "daemon_never_dispatches_renewal_before_window_and_retries_fresh_command_after_too_early":
      return observation.kind === "lease_lifecycle" && observation.earlyDispatchSuppressed && observation.retryUsesFreshCommand && observation.earlyCommandId === fixtureText(fixture, "earlyCommandId") && observation.retryCommandId === fixtureText(fixture, "retryCommandId") && observation.earlyCommandId !== observation.retryCommandId && Date.parse(observation.retryDispatchAt) >= Date.parse(observation.retryNotBefore);
    case "too_early_command_replays_stored_rejection_while_fresh_retry_waits_for_retry_not_before": {
      const receipt = fixtureRecord(fixture, "earlyStoredReceipt");
      return observation.kind === "lease_lifecycle" && observation.earlyReplayCode === recordText(receipt, "code") && observation.retryNotBefore === fixtureText(fixture, "retryNotBefore") && Date.parse(observation.retryDispatchAt) >= Date.parse(observation.retryNotBefore);
    }

    case "every_task_artifact_and_review_request_mutation_checks_full_current_lease":
      return observation.kind === "mutation_fence" && observation.allFullLeaseChecksPassed && sameStrings(observation.checkedMutationKinds, fixtureStrings(fixture, "mutationKinds")) && observation.leaseRevision === fixtureInteger(fixture, "leaseRevision");
    case "stale_owner_has_zero_shared_mutation":
      return observation.kind === "mutation_fence" && observation.staleOwnerSharedMutationCount === 0 && observation.taskStatus === fixtureText(fixture, "taskStatus");
    case "owner_loss_requests_local_interrupt_before_any_later_effect":
      return observation.kind === "mutation_fence" && equalJson(observation.ownershipLossSequence, ["lease_lost", "interrupt_requested", "later_effect_suppressed"]);
    case "late_native_result_is_observation_only_after_owner_loss":
      return observation.kind === "mutation_fence" && observation.lateResultDisposition === "observation_only" && observation.staleOwnerSharedMutationCount === 0;
    case "new_epoch_owner_is_not_affected_by_stale_cleanup":
      return observation.kind === "mutation_fence" && observation.replacementEpoch === fixtureInteger(fixture, "replacementEpoch") && observation.replacementEpoch === observation.leaseEpoch + 1 && !observation.staleCleanupAffectedReplacement;

    case "matching_current_execution_renews_before_resume":
      return observation.kind === "startup_reconciliation" && observation.cases.includes("matching_current") && equalJson(observation.matchingCurrentSequence, ["read", "renew", "resume"]);
    case "expired_or_replaced_execution_stops_and_suppresses_publication":
      return observation.kind === "startup_reconciliation" && observation.cases.includes("shared_expired") && observation.cases.includes("shared_replaced") && observation.expiredAndReplacedStop && observation.expiredAndReplacedPublicationCount === 0;
    case "local_only_execution_is_stopped_without_shared_mutation":
      return observation.kind === "startup_reconciliation" && observation.cases.includes("local_only") && observation.localOnlyStopped && observation.localOnlySharedMutationCount === 0;
    case "shared_only_claim_is_relinquished_or_left_for_server_expiry_without_fabricated_process":
      return observation.kind === "startup_reconciliation" && observation.cases.includes("shared_only") && observation.sharedOnlyDisposition === "relinquish_exact_or_server_expiry" && observation.fabricatedProcessCount === 0;
    case "shared_only_relinquish_closes_exact_claim_and_reservation_before_todo_reclaim":
      return observation.kind === "startup_reconciliation" && observation.relinquishAtomic && observation.sharedOnlyDisposition === "relinquish_exact_or_server_expiry";
    case "wave3_reconciliation_does_not_claim_full_wave4_crash_recovery":
      return observation.kind === "startup_reconciliation" && observation.fullCrashRecoveryDeferred === fixtureValue(fixture, "fullCrashRecoveryDeferred") && observation.fullCrashRecoveryDeferred;

    case "one_logical_coordination_effect_atomically_creates_children_edges_and_one_graph_revision":
      return observation.kind === "graph_proposal" && observation.rootTaskId === fixtureText(fixture, "rootTaskId") && sameStrings(observation.childKeys, fixtureStrings(fixture, "childKeys")) && observation.logicalCoordinationEffectCount === 1 && observation.childrenEdgesAndRevisionAtomic && observation.expectedGraphRevision === fixtureInteger(fixture, "expectedGraphRevision");
    case "proposal_replay_aliases_exact_stored_result":
      return observation.kind === "graph_proposal" && observation.exactReplayAliased && fixtureValue(fixture, "sameCommandTransportReplayAllowed") === true;
    case "changed_payload_under_same_command_conflicts":
      return observation.kind === "graph_proposal" && observation.changedPayloadOutcome === "IDEMPOTENCY_CONFLICT" && observation.expectedGraphRevision === fixtureInteger(fixture, "expectedGraphRevision");
    case "source_message_turn_producer_and_current_lease_join_before_graph_write":
      return observation.kind === "graph_proposal" && sameStrings(observation.sourceJoinMembers, ["message", "turn", "producer_fact", "committed_reply", "current_lease"]) && observation.sourceJoinPassed;
    case "parent_transitions_to_waiting_in_same_transaction":
      return observation.kind === "graph_proposal" && observation.parentStatusAfter === "waiting" && observation.childrenEdgesAndRevisionAtomic;
    case "one_reply_permits_one_logical_coordination_effect_with_same_command_replay_only":
      return observation.kind === "graph_proposal" && observation.committedReplyPresent === fixtureValue(fixture, "sourceTurnHasCommittedReply") && observation.logicalCoordinationEffectCount === 1 && observation.exactReplayAliased && observation.distinctSecondCommandOutcome === "TASK_COORDINATION_ALREADY_COMMITTED";

    case "contains_and_dependency_edges_are_distinct_and_combined_lifecycle_wait_graph_is_acyclic":
      return observation.kind === "graph_lifecycle" && equalJson(observation.containsEdges, fixtureValue(fixture, "contains")) && equalJson(observation.dependencyEdges, fixtureValue(fixture, "dependsOn")) && observation.edgeKindsDistinct && observation.combinedWaitGraphAcyclic;
    case "only_leaf_with_all_dependencies_done_is_claimable":
      return observation.kind === "graph_lifecycle" && observation.onlyReadyLeafClaimable && sameStrings(observation.taskNames, fixtureStrings(fixture, "tasks"));
    case "dependency_completion_atomically_recomputes_ready_set":
      return observation.kind === "graph_lifecycle" && observation.dependencyCompletionAtomic && observation.dependencyEdges.length === (fixtureValue(fixture, "dependsOn") as readonly Gate3Json[]).length;
    case "blocked_dependency_fail_closes_dependents":
      return observation.kind === "graph_lifecycle" && observation.blockedDependencyFailClosed && observation.blockedAffectedTaskIds.length > 1;
    case "cross_root_or_cross_server_edges_are_rejected":
      return observation.kind === "graph_lifecycle" && observation.crossScopeEdgesRejected && fixtureText(fixtureRecord(fixture, "realPgEndpointAncestorInterleaving"), "repositoryId") !== "";
    case "concurrent_endpoint_or_ancestor_mutation_serializes_before_cycle_and_readiness_commit": {
      const pg = fixtureRecord(fixture, "realPgEndpointAncestorInterleaving");
      return observation.kind === "graph_lifecycle" && sameStrings(observation.reachedPgLatches, recordArray(pg, "latches") as readonly string[]) && observation.pgSchedules.length === recordArray(pg, "forcedSchedules").length && observation.bothSerialOrdersObserved;
    }
    case "blocked_prerequisite_transitively_blocks_dependents_and_waiting_ancestors_atomically":
      return observation.kind === "graph_lifecycle" && observation.allAffectedBlocked && sameStrings(observation.blockedAffectedTaskIds, recordArray(fixtureRecord(fixture, "transitiveBlockedChain"), "expectedAffectedTaskIds") as readonly string[]);
    case "blocked_propagation_derives_complete_affected_closure_from_exact_stored_topology":
      return observation.kind === "graph_lifecycle" && observation.closureDerivedFromStoredTopology && sameStrings(observation.blockedAffectedTaskIds, recordArray(fixtureRecord(fixture, "transitiveBlockedChain"), "expectedAffectedTaskIds") as readonly string[]);
    case "blocked_propagation_records_exact_graph_blocked_lease_reason_and_typed_before_after_rows":
      return observation.kind === "graph_lifecycle" && observation.graphBlockedLeaseReasonExact && observation.typedBeforeAfterRowsExact && rowsFrom(fixtureRecord(fixture, "transitiveBlockedChain"), "before").length === rowsFrom(fixtureRecord(fixture, "transitiveBlockedChain"), "after").length;
    case "repository_first_global_lock_order_forces_both_serial_orders_and_prevents_claim_propagation_deadlock": {
      const pg = fixtureRecord(fixture, "realPgEndpointAncestorInterleaving");
      return observation.kind === "graph_lifecycle" && observation.lockOrder === recordText(pg, "lockOrder") && observation.lockOrder.startsWith("repository_then_root") && observation.bothSerialOrdersObserved && observation.deadlockCount === 0;
    }

    case "proposal_and_claim_validate_capabilities_from_current_server_registry":
      return observation.kind === "capability_bounds" && sameStrings(observation.requiredCapabilities, fixtureStrings(fixture, "requiredCapabilities")) && observation.requiredCapabilities.every((capability) => observation.agentCapabilities.includes(capability)) && observation.registryValidatedAtProposalAndClaim;
    case "all_graph_limits_apply_before_any_insert":
      return observation.kind === "capability_bounds" && observation.allLimitsCheckedBeforeInsert && observation.limitsSource === fixtureText(fixture, "limitsSource");
    case "limit_policy_digest_is_stored_with_graph_revision":
      return observation.kind === "capability_bounds" && observation.limitPolicyDigestStored && observation.limitsSource === "server_policy_digest";
    case "titles_and_capability_keys_are_canonical_bounded_values":
      return observation.kind === "capability_bounds" && observation.canonicalTitlesAndCapabilities && sameStrings(observation.requiredCapabilities, fixtureStrings(fixture, "requiredCapabilities"));
    case "delegated_title_privacy_gate_runs_before_graph_insert_without_body_egress":
      return observation.kind === "capability_bounds" && observation.titlePrivacyGateDelegated && observation.bodyEgressCount === 0;
    case "whole_short_source_body_is_rejected_even_when_wrapped":
      return observation.kind === "capability_bounds" && observation.shortSourceBody === fixtureText(fixture, "shortSourceBody") && observation.wrappedShortTitle === fixtureText(fixture, "wrappedShortTitle") && observation.wrappedWholeBodyRejected;
    case "graph_persists_exact_title_and_current_classifier_policy_digests":
      return observation.kind === "capability_bounds" && observation.titleDigestStored && observation.classifierPolicyDigestStored && observation.classifierPolicyDigest === fixtureText(fixture, "titleClassifierPolicyDigest");
    case "normal_conversation_requires_no_global_planner":
      return observation.kind === "capability_bounds" && !observation.globalPlannerRequired && observation.bodyEgressCount === 0;

    case "each_task_binds_exact_base_workspace_generation_and_allowed_paths":
      return observation.kind === "workspace_ownership" && observation.baseCommit === fixtureText(fixture, "baseCommit") && observation.workspaceGeneration === fixtureInteger(fixture, "workspaceGeneration") && equalJson(observation.childPathClaims, fixtureValue(fixture, "childPathClaims")) && observation.exactWorkspaceBinding;
    case "path_claims_are_normalized_relative_and_non_overlapping":
      return observation.kind === "workspace_ownership" && observation.pathsNormalizedRelativeNonoverlap && observation.childPathClaims.length === (fixtureValue(fixture, "childPathClaims") as readonly Gate3Json[]).length;
    case "repository_scoped_serialization_gives_one_cross_root_overlap_winner":
      return observation.kind === "workspace_ownership" && sameNumbers(observation.crossRootContenderCounts, fixtureNumbers(fixture, "crossRootContenderCounts")) && everyOne(observation.crossRootWinnerCounts);
    case "shared_file_collision_rejected_or_owned_by_one_integration_task":
      return observation.kind === "workspace_ownership" && observation.sharedCollisionDisposition === "rejected_or_single_integration_owner" && equalJson(observation.integrationOwnerPathClaims, fixtureValue(fixture, "integrationOwnerPathClaims"));
    case "child_worktrees_cannot_mutate_integration_owner_paths":
      return observation.kind === "workspace_ownership" && observation.childIntegrationOwnerMutationCount === 0 && observation.integrationOwnerPathClaims.length > 0;
    case "workspace_contract_digest_changes_on_any_policy_member":
      return observation.kind === "workspace_ownership" && sameStrings(observation.digestSensitiveMembers, ["baseCommit", "workspaceGeneration", "allowedPaths", "executionMode"]) && fixtureText(fixture, "executionMode") === "isolated_worktree";

    case "artifact_contract_template_is_selected_by_current_server_policy_not_builder":
      return observation.kind === "artifact_publication" && observation.templateAuthority === "current_server_policy" && observation.artifactKinds.length === fixtureStrings(fixture, "artifactKinds").length;
    case "artifact_contract_template_materializes_non_circular_exact_task_workspace_preimage":
      return observation.kind === "artifact_publication" && observation.nonCircularTaskWorkspacePreimage && sameStrings(observation.stagedRoles, fixtureStrings(fixture, "stagedRoles"));
    case "artifact_bytes_commit_tree_parent_scope_and_contract_bind_before_publication":
      return observation.kind === "artifact_publication" && sameStrings(observation.boundObjectMembers, ["bytes", "commit", "tree", "parent", "scope", "contract"]) && fixtureValue(fixture, "contractDigestBound") === true;
    case "published_artifact_material_remains_resolvable_after_private_worktree_disappears":
      return observation.kind === "artifact_publication" && observation.materialResolution === fixtureText(fixture, "materialResolution") && observation.materialResolution === "server_owned_content_addressed";
    case "publication_and_task_in_review_transition_are_atomic":
      return observation.kind === "artifact_publication" && observation.publicationTaskTransitionAtomic && fixtureText(fixture, "taskStatus") === "in_progress" && fixtureValue(fixture, "leaseCurrent") === true;
    case "exact_replay_aliases_one_immutable_artifact":
      return observation.kind === "artifact_publication" && observation.exactReplayAliased && sameStrings(observation.artifactKinds, fixtureStrings(fixture, "artifactKinds"));
    case "same_attempt_divergence_conflicts_without_replacing_artifact":
      return observation.kind === "artifact_publication" && observation.divergentAttemptOutcome === "ARTIFACT_IMMUTABLE_CONFLICT" && observation.exactReplayAliased;
    case "builder_and_contributors_derive_from_server_attempt_facts":
      return observation.kind === "artifact_publication" && observation.contributorLedgerAuthority === "server_attempt_facts" && sameStrings(observation.contributorSources, fixtureStrings(fixture, "contributorSources"));
    case "contributor_ledger_is_server_written_and_transitively_expands_accepted_upstream_artifacts":
      return observation.kind === "artifact_publication" && observation.contributorLedgerAuthority === "server_attempt_facts" && observation.contributorLedgerTransitive && fixtureValue(fixture, "acceptedSourceSet") instanceof Array;
    case "accepted_artifact_consumption_locks_source_barrier_material_and_target_ledger_before_byte_grant":
      return observation.kind === "artifact_publication" && equalJson(observation.consumptionLockOrder, ["source_barrier", "source_material", "target_ledger"]) && observation.materialGrantedAfterLedgerCommit && fixtureInteger(fixture, "sourceBarrierRevision") > 0;
    case "accepted_artifact_consumption_transitively_expands_contributors_and_source_set_atomically":
      return observation.kind === "artifact_publication" && observation.contributorLedgerTransitive && observation.sourceSetExpandedAtomically && observation.targetContributorLedgerRevision === fixtureInteger(fixture, "targetContributorLedgerRevision");
    case "consumption_replay_aliases_one_grant_and_ledger_revision":
      return observation.kind === "artifact_publication" && observation.consumptionReplayAliased && observation.targetContributorLedgerRevision === fixtureInteger(fixture, "targetContributorLedgerRevision");
    case "artifact_metadata_contains_no_body_credential_or_private_path":
      return observation.kind === "artifact_publication" && observation.forbiddenMetadataFieldCount === 0 && observation.materialResolution === fixtureText(fixture, "materialResolution");

    case "builder_cannot_claim_or_submit_any_required_reviewer_seat":
      return observation.kind === "review_barrier" && observation.builderAgentId === fixtureText(fixture, "builderAgentId") && observation.builderSeatMutationCount === 0;
    case "distinct_required_seats_use_distinct_reviewer_agents":
      return observation.kind === "review_barrier" && sameStrings(observation.requiredSeats, fixtureStrings(fixture, "requiredSeats")) && observation.distinctReviewerCount === observation.requiredSeats.length;
    case "publication_mints_one_stable_unassigned_review_task_per_required_seat":
      return observation.kind === "review_barrier" && observation.stableReviewTaskCount === fixtureStrings(fixture, "requiredSeats").length;
    case "assignment_and_reassignment_are_revisioned_atomic_and_unique":
      return observation.kind === "review_barrier" && observation.assignmentRevision === fixtureInteger(fixture, "replacementAssignmentRevision") && observation.assignmentUnique;
    case "reviewer_receives_only_exact_frozen_artifact_contract_and_scenario":
      return observation.kind === "review_barrier" && sameStrings(observation.reviewerInputMembers, ["artifact", "contract", "scenario"]) && /^sha256:[0-9a-f]{64}$/u.test(fixtureText(fixture, "artifactDigest")) && /^sha256:[0-9a-f]{64}$/u.test(fixtureText(fixture, "contractDigest"));
    case "review_verdict_attempt_equals_current_reviewer_lease_attempt":
      return observation.kind === "review_barrier" && observation.verdictAttempt === observation.currentReviewAttempt && observation.currentReviewAttempt === fixtureInteger(fixture, "replacementReviewAttempt");
    case "replacement_reviewer_new_attempt_can_submit_only_the_exact_current_verdict":
      return observation.kind === "review_barrier" && observation.replacementAttemptAccepted && observation.currentReviewAttempt === fixtureInteger(fixture, "replacementReviewAttempt") && observation.currentReviewAttempt > fixtureInteger(fixture, "oldReviewAttempt");
    case "each_required_seat_has_one_current_terminal_verdict":
      return observation.kind === "review_barrier" && observation.terminalVerdictCountPerSeat === 1 && observation.requiredSeats.length === fixtureStrings(fixture, "requiredSeats").length;
    case "terminal_verdict_closes_assignment_and_prevents_reassignment":
      return observation.kind === "review_barrier" && observation.terminalAssignmentClosed && observation.terminalReassignmentRejected;
    case "all_required_go_verdicts_same_artifact_unlock_barrier":
      return observation.kind === "review_barrier" && observation.allGoUnlocksBarrier && fixtureInteger(fixture, "scenarioVersion") === 8;
    case "one_block_or_missing_seat_keeps_barrier_closed":
      return observation.kind === "review_barrier" && observation.blockOrMissingKeepsClosed && observation.requiredSeats.length > 1;
    case "one_block_revokes_nonterminal_sibling_review_tasks_and_late_verdicts":
      return observation.kind === "review_barrier" && observation.blockRevokesSiblingTasks && observation.lateSiblingVerdictRejected;
    case "barrier_block_closes_sibling_review_lease_with_exact_review_barrier_blocked_reason":
      return observation.kind === "review_barrier" && observation.blockedLeaseReason === "review_barrier_blocked" && observation.blockedLeaseReason === recordText(fixtureRecord(fixture, "barrierBlockedLease"), "leaseCloseReason");

    case "integration_owner_alone_may_consume_reviewed_child_artifacts":
      return observation.kind === "reviewed_integration" && observation.integrationOwnerOnly && sameStrings(observation.childCommits, fixtureStrings(fixture, "childCommits"));
    case "fast_forward_requires_exact_reviewed_commit_chain_without_rewrite":
      return observation.kind === "reviewed_integration" && observation.fastForwardNoRewrite && observation.integrationModes.includes("fast_forward") && fixtureText(fixture, "childBarriers") === "satisfied";
    case "new_integration_sha_reenters_full_review_barrier":
      return observation.kind === "reviewed_integration" && observation.newCommitRequiresFullReview && observation.integrationModes.includes("new_commit");
    case "blocked_or_stale_child_never_enters_integration_candidate":
      return observation.kind === "reviewed_integration" && observation.blockedOrStaleIncludedCount === 0 && observation.childBarriers === fixtureText(fixture, "childBarriers");
    case "integration_candidate_uses_authoritative_consumed_source_set_and_current_ledger_revision":
      return observation.kind === "reviewed_integration" && observation.authoritativeConsumedSourceSet && observation.currentLedgerRevisionChecked && observation.childCommits.length === fixtureStrings(fixture, "childCommits").length;
    case "protected_publication_stays_outside_task_execution_transaction":
      return observation.kind === "reviewed_integration" && !observation.protectedPublicationInsideTaskTransaction && observation.childBarriers === "satisfied";

    case "all_wave0_wave1_and_wave2_gates_remain_green":
      return observation.kind === "wave_carry" && observation.wave0Wave1Wave2Gates === fixtureText(fixture, "wave0Wave1Wave2Gates") && observation.wave0Wave1Wave2Gates === "green";
    case "pure_question_still_has_one_normal_reply_and_zero_task_effect":
      return observation.kind === "wave_carry" && observation.pureQuestionReplyCount === 1 && observation.pureQuestionTaskEffectCount === 0 && fixtureText(fixture, "wave0Wave1Wave2Gates") === "green";
    case "task_graph_coordination_still_has_committed_reply_predecessor_and_one_logical_effect":
      return observation.kind === "wave_carry" && observation.coordinationReplyPredecessorCommitted && observation.logicalCoordinationEffectCount === 1 && fixtureText(fixture, "wave0Wave1Wave2Gates") === "green";
    case "five_wave3_deferred_rows_bind_only_to_real_task_artifact_and_freshness_facts":
      return observation.kind === "wave_carry" && observation.wave3CandidateRows.length === 5 && observation.wave3RowsBoundToMachineFacts && Object.values(fixtureRecord(fixture, "wave1DeferredRows")).filter((value) => value === "wave3_candidate").length === 5;
    case "coordination_slo_and_channel_navigation_remain_wave5_fail_closed":
      return observation.kind === "wave_carry" && sameStrings(observation.wave5DeferredRows, ["coordination_slo", "channel_navigation"]) && Object.values(fixtureRecord(fixture, "wave1DeferredRows")).filter((value) => value === "wave5_deferred").length === 2;
    case "seed_m2_binds_exact_execute_owner_artifact_path_and_digest":
      return observation.kind === "wave_carry" && observation.seedM2Disposition === fixtureText(fixture, "seedM2") && observation.seedM2Disposition === "wave3_candidate" && observation.seedM2ExactArtifactBinding;
    case "exact_new_package_import_builtin_and_owner_boundaries_pass_with_complete_seeded_negatives":
      return observation.kind === "wave_carry" && observation.boundaryPolicyCount === (fixtureValue(fixture, "wave3BoundaryPolicies") as readonly Gate3Json[]).length && sameStrings(observation.requiredBoundaryNegativeKinds, fixtureStrings(fixture, "requiredBoundaryNegativeKinds"));
    case "new_package_manifests_match_exact_workspace_dependencies_and_empty_external_allow_sets":
      return observation.kind === "wave_carry" && observation.manifestWorkspaceDependenciesExact && observation.externalAllowSetsEmpty && observation.boundaryPolicyCount === 3;
    case "all_wave3_forbidden_facts_absent_from_product_fixture_log_and_receipt":
      return observation.kind === "wave_carry" && observation.forbiddenFactCount === 0 && observation.requiredBoundaryNegativeKinds.length === fixtureStrings(fixture, "requiredBoundaryNegativeKinds").length;
    default:
      throw new Gate3OracleError("GATE3_ASSERTION_PREDICATE_UNKNOWN", assertionId);
  }
}
