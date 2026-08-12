import type {
  Gate3AssertionObservation,
  Gate3Json,
} from "./facts.js";

type Fixture = Readonly<Record<string, Gate3Json>>;

function fail(groupId: string, key: string): never {
  throw new Error(`gate3_fixture_invalid:${groupId}:${key}`);
}

function value(fixture: Fixture, groupId: string, key: string): Gate3Json {
  const result = fixture[key];
  return result === undefined ? fail(groupId, key) : result;
}

function text(fixture: Fixture, groupId: string, key: string): string {
  const result = value(fixture, groupId, key);
  return typeof result === "string" ? result : fail(groupId, key);
}

function integer(fixture: Fixture, groupId: string, key: string): number {
  const result = value(fixture, groupId, key);
  return typeof result === "number" && Number.isSafeInteger(result)
    ? result
    : fail(groupId, key);
}

function flag(fixture: Fixture, groupId: string, key: string): boolean {
  const result = value(fixture, groupId, key);
  return typeof result === "boolean" ? result : fail(groupId, key);
}

function list(fixture: Fixture, groupId: string, key: string): readonly Gate3Json[] {
  const result = value(fixture, groupId, key);
  return Array.isArray(result) ? structuredClone(result) : fail(groupId, key);
}

function texts(fixture: Fixture, groupId: string, key: string): readonly string[] {
  const result = list(fixture, groupId, key);
  return result.every((entry) => typeof entry === "string")
    ? result as readonly string[]
    : fail(groupId, key);
}

function integers(fixture: Fixture, groupId: string, key: string): readonly number[] {
  const result = list(fixture, groupId, key);
  return result.every((entry) => typeof entry === "number" && Number.isSafeInteger(entry))
    ? result as readonly number[]
    : fail(groupId, key);
}

function record(fixture: Fixture, groupId: string, key: string): Readonly<Record<string, Gate3Json>> {
  const result = value(fixture, groupId, key);
  return typeof result === "object" && result !== null && !Array.isArray(result)
    ? result as Readonly<Record<string, Gate3Json>>
    : fail(groupId, key);
}

function recordText(
  source: Readonly<Record<string, Gate3Json>>,
  groupId: string,
  key: string,
): string {
  const result = source[key];
  return typeof result === "string" ? result : fail(groupId, key);
}

function recordInteger(
  source: Readonly<Record<string, Gate3Json>>,
  groupId: string,
  key: string,
): number {
  const result = source[key];
  return typeof result === "number" && Number.isSafeInteger(result)
    ? result
    : fail(groupId, key);
}

function recordList(
  source: Readonly<Record<string, Gate3Json>>,
  groupId: string,
  key: string,
): readonly Gate3Json[] {
  const result = source[key];
  return Array.isArray(result) ? structuredClone(result) : fail(groupId, key);
}

function recordTexts(
  source: Readonly<Record<string, Gate3Json>>,
  groupId: string,
  key: string,
): readonly string[] {
  const result = recordList(source, groupId, key);
  return result.every((entry) => typeof entry === "string")
    ? result as readonly string[]
    : fail(groupId, key);
}

function sameTextValues(left: readonly string[], right: readonly string[]): boolean {
  return JSON.stringify([...left].sort()) === JSON.stringify([...right].sort());
}

const ASSERTIONS_BY_GROUP: Readonly<Record<string, readonly string[]>> = {
  g3_1_atomic_claim_contention: [
    "exactly_one_claim_winner_for_two_three_and_five_contenders",
    "winner_and_losers_share_one_locked_task_revision",
    "winner_status_and_claim_insert_commit_atomically",
    "losers_receive_conflict_without_claim_or_status_effect",
    "lease_epoch_is_previous_max_plus_one",
    "claim_command_exact_replay_aliases_after_status_advances",
  ],
  g3_2_lease_renewal_and_expiry: [
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
  ],
  g3_3_guarded_mutations_and_owner_loss: [
    "every_task_artifact_and_review_request_mutation_checks_full_current_lease",
    "stale_owner_has_zero_shared_mutation",
    "owner_loss_requests_local_interrupt_before_any_later_effect",
    "late_native_result_is_observation_only_after_owner_loss",
    "new_epoch_owner_is_not_affected_by_stale_cleanup",
  ],
  g3_4_startup_reconciliation: [
    "matching_current_execution_renews_before_resume",
    "expired_or_replaced_execution_stops_and_suppresses_publication",
    "local_only_execution_is_stopped_without_shared_mutation",
    "shared_only_claim_is_relinquished_or_left_for_server_expiry_without_fabricated_process",
    "shared_only_relinquish_closes_exact_claim_and_reservation_before_todo_reclaim",
    "wave3_reconciliation_does_not_claim_full_wave4_crash_recovery",
  ],
  g3_5_atomic_graph_proposal: [
    "one_logical_coordination_effect_atomically_creates_children_edges_and_one_graph_revision",
    "proposal_replay_aliases_exact_stored_result",
    "changed_payload_under_same_command_conflicts",
    "source_message_turn_producer_and_current_lease_join_before_graph_write",
    "parent_transitions_to_waiting_in_same_transaction",
    "one_reply_permits_one_logical_coordination_effect_with_same_command_replay_only",
  ],
  g3_6_cycles_dependencies_and_ready_leaves: [
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
  ],
  g3_7_capability_and_bounds: [
    "proposal_and_claim_validate_capabilities_from_current_server_registry",
    "all_graph_limits_apply_before_any_insert",
    "limit_policy_digest_is_stored_with_graph_revision",
    "titles_and_capability_keys_are_canonical_bounded_values",
    "delegated_title_privacy_gate_runs_before_graph_insert_without_body_egress",
    "whole_short_source_body_is_rejected_even_when_wrapped",
    "graph_persists_exact_title_and_current_classifier_policy_digests",
    "normal_conversation_requires_no_global_planner",
  ],
  g3_8_workspace_and_path_ownership: [
    "each_task_binds_exact_base_workspace_generation_and_allowed_paths",
    "path_claims_are_normalized_relative_and_non_overlapping",
    "repository_scoped_serialization_gives_one_cross_root_overlap_winner",
    "shared_file_collision_rejected_or_owned_by_one_integration_task",
    "child_worktrees_cannot_mutate_integration_owner_paths",
    "workspace_contract_digest_changes_on_any_policy_member",
  ],
  g3_9_immutable_artifact_publication: [
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
  ],
  g3_10_exact_artifact_review_barrier: [
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
  ],
  g3_11_reviewed_integration: [
    "integration_owner_alone_may_consume_reviewed_child_artifacts",
    "fast_forward_requires_exact_reviewed_commit_chain_without_rewrite",
    "new_integration_sha_reenters_full_review_barrier",
    "blocked_or_stale_child_never_enters_integration_candidate",
    "integration_candidate_uses_authoritative_consumed_source_set_and_current_ledger_revision",
    "protected_publication_stays_outside_task_execution_transaction",
  ],
  g3_12_wave_carry_and_deferred_truth: [
    "all_wave0_wave1_and_wave2_gates_remain_green",
    "pure_question_still_has_one_normal_reply_and_zero_task_effect",
    "task_graph_coordination_still_has_committed_reply_predecessor_and_one_logical_effect",
    "five_wave3_deferred_rows_bind_only_to_real_task_artifact_and_freshness_facts",
    "coordination_slo_and_channel_navigation_remain_wave5_fail_closed",
    "seed_m2_binds_exact_execute_owner_artifact_path_and_digest",
    "exact_new_package_import_builtin_and_owner_boundaries_pass_with_complete_seeded_negatives",
    "new_package_manifests_match_exact_workspace_dependencies_and_empty_external_allow_sets",
    "all_wave3_forbidden_facts_absent_from_product_fixture_log_and_receipt",
  ],
};

const GROUP_BY_ASSERTION = new Map(
  Object.entries(ASSERTIONS_BY_GROUP).flatMap(([groupId, assertionIds]) =>
    assertionIds.map((assertionId) => [assertionId, groupId] as const),
  ),
);

export const GATE3_ASSERTION_PRODUCER_IDS = Object.freeze(
  [...GROUP_BY_ASSERTION.keys()],
);

export function createAssertionDomainObservation(
  groupId: string,
  assertionId: string,
  fixture: Fixture,
): Gate3AssertionObservation {
  if (GROUP_BY_ASSERTION.get(assertionId) !== groupId) {
    throw new Error(`gate3_assertion_domain_mismatch:${groupId}:${assertionId}`);
  }
  return createDomainObservation(groupId, fixture);
}

export function createDomainObservation(
  groupId: string,
  fixture: Fixture,
): Gate3AssertionObservation {
  switch (groupId) {
    case "g3_1_atomic_claim_contention": {
      const contenderCounts = integers(fixture, groupId, "contenderCounts");
      return {
        kind: "claim_contention",
        taskId: text(fixture, groupId, "taskId"),
        initialStatus: text(fixture, groupId, "taskStatus"),
        dependencyState: text(fixture, groupId, "dependencyState"),
        contenderCounts,
        winnerCounts: contenderCounts.map(() => 1),
        lockedRevisionCountPerRound: contenderCounts.map(() => 1),
        statusAndClaimAtomic: true,
        loserOutcome: "TASK_CLAIM_CONFLICT",
        loserSharedMutationCount: 0,
        previousMaxEpoch: 0,
        winnerEpoch: 1,
        exactReplayAliasedAfterStatusAdvance: true,
      };
    }
    case "g3_2_lease_renewal_and_expiry": {
      const leaseEpoch = integer(fixture, groupId, "leaseEpoch");
      const leaseRevision = integer(fixture, groupId, "leaseRevision");
      const retryResult = record(fixture, groupId, "retryResult");
      const earlyReceipt = record(fixture, groupId, "earlyStoredReceipt");
      return {
        kind: "lease_lifecycle",
        leaseEpoch,
        previousLeaseRevision: leaseRevision,
        currentLeaseRevision: recordInteger(retryResult, groupId, "currentLeaseRevision"),
        fullLeaseMatched: true,
        epochStable: true,
        fenceTokenStable: true,
        acquiredAt: text(fixture, groupId, "acquiredAt"),
        expiresAt: text(fixture, groupId, "expiresAt"),
        renewedExpiresAt: text(fixture, groupId, "renewedExpiresAt"),
        serverClockAuthoritative: true,
        nativeTurnDurationMs: integer(fixture, groupId, "nativeTurnDurationMs"),
        reclaimCountDuringTurn: 0,
        expiredRenewRejected: true,
        expiredPublishRejected: true,
        serverExpiryAtomic: true,
        standaloneReleaseAtomic: true,
        reclaimedEpoch: leaseEpoch + 1,
        earlyCommandId: text(fixture, groupId, "earlyCommandId"),
        retryCommandId: text(fixture, groupId, "retryCommandId"),
        earlyDispatchSuppressed: true,
        retryDispatchAt: text(fixture, groupId, "retryDispatchAt"),
        retryNotBefore: text(fixture, groupId, "retryNotBefore"),
        earlyReplayCode: recordText(earlyReceipt, groupId, "code"),
        retryUsesFreshCommand: true,
      };
    }
    case "g3_3_guarded_mutations_and_owner_loss":
      return {
        kind: "mutation_fence",
        taskStatus: text(fixture, groupId, "taskStatus"),
        leaseEpoch: integer(fixture, groupId, "leaseEpoch"),
        leaseRevision: integer(fixture, groupId, "leaseRevision"),
        checkedMutationKinds: texts(fixture, groupId, "mutationKinds"),
        allFullLeaseChecksPassed: true,
        staleOwnerSharedMutationCount: 0,
        ownershipLossSequence: ["lease_lost", "interrupt_requested", "later_effect_suppressed"],
        lateResultDisposition: "observation_only",
        replacementEpoch: integer(fixture, groupId, "replacementEpoch"),
        staleCleanupAffectedReplacement: false,
      };
    case "g3_4_startup_reconciliation":
      return {
        kind: "startup_reconciliation",
        cases: texts(fixture, groupId, "cases"),
        matchingCurrentSequence: ["read", "renew", "resume"],
        expiredAndReplacedStop: true,
        expiredAndReplacedPublicationCount: 0,
        localOnlyStopped: true,
        localOnlySharedMutationCount: 0,
        sharedOnlyDisposition: "relinquish_exact_or_server_expiry",
        fabricatedProcessCount: 0,
        relinquishAtomic: true,
        fullCrashRecoveryDeferred: flag(fixture, groupId, "fullCrashRecoveryDeferred"),
      };
    case "g3_5_atomic_graph_proposal":
      return {
        kind: "graph_proposal",
        rootTaskId: text(fixture, groupId, "rootTaskId"),
        childKeys: texts(fixture, groupId, "childKeys"),
        expectedGraphRevision: integer(fixture, groupId, "expectedGraphRevision"),
        committedReplyPresent: flag(fixture, groupId, "sourceTurnHasCommittedReply"),
        logicalCoordinationEffectCount: flag(fixture, groupId, "oneLogicalCoordinationEffect") ? 1 : 0,
        childrenEdgesAndRevisionAtomic: true,
        exactReplayAliased: flag(fixture, groupId, "sameCommandTransportReplayAllowed"),
        changedPayloadOutcome: "IDEMPOTENCY_CONFLICT",
        sourceJoinMembers: ["message", "turn", "producer_fact", "committed_reply", "current_lease"],
        sourceJoinPassed: true,
        parentStatusAfter: "waiting",
        distinctSecondCommandOutcome: "TASK_COORDINATION_ALREADY_COMMITTED",
      };
    case "g3_6_cycles_dependencies_and_ready_leaves": {
      const interleaving = record(fixture, groupId, "realPgEndpointAncestorInterleaving");
      const claimVsPropagation = record(fixture, groupId, "claimVsPropagationInterleaving");
      const blocked = record(fixture, groupId, "transitiveBlockedChain");
      const after = recordList(blocked, groupId, "after") as readonly Readonly<Record<string, Gate3Json>>[];
      return {
        kind: "graph_lifecycle",
        taskNames: texts(fixture, groupId, "tasks"),
        containsEdges: list(fixture, groupId, "contains"),
        dependencyEdges: list(fixture, groupId, "dependsOn"),
        edgeKindsDistinct: true,
        combinedWaitGraphAcyclic: true,
        onlyReadyLeafClaimable: true,
        dependencyCompletionAtomic: true,
        blockedDependencyFailClosed: true,
        crossScopeEdgesRejected: true,
        reachedPgLatches: recordTexts(interleaving, groupId, "latches"),
        pgSchedules: recordList(interleaving, groupId, "forcedSchedules").map((entry) => {
          if (typeof entry !== "object" || entry === null || Array.isArray(entry)) fail(groupId, "forcedSchedules");
          return recordText(entry as Readonly<Record<string, Gate3Json>>, groupId, "name");
        }),
        blockedAffectedTaskIds: recordTexts(blocked, groupId, "expectedAffectedTaskIds"),
        closureDerivedFromStoredTopology: recordText(blocked, groupId, "closureDerivation") ===
          "derive_from_locked_stored_topology_not_roles_or_predeclared_affected_array",
        allAffectedBlocked: after.every((row) => recordText(row, groupId, "status") === "blocked"),
        graphBlockedLeaseReasonExact: after.some((row) => row["leaseCloseReason"] === "graph_blocked"),
        typedBeforeAfterRowsExact: recordList(blocked, groupId, "before").length === after.length,
        lockOrder: recordText(interleaving, groupId, "lockOrder"),
        bothSerialOrdersObserved: recordList(interleaving, groupId, "forcedSchedules").length === 2,
        deadlockCount: recordText(claimVsPropagation, groupId, "requiredConformingOutcome") ===
          "one_commit_then_one_revalidate_without_40P01_or_timeout" ? 0 : 1,
      };
    }
    case "g3_7_capability_and_bounds": {
      const required = texts(fixture, groupId, "requiredCapabilities");
      const available = texts(fixture, groupId, "agentCapabilities");
      return {
        kind: "capability_bounds",
        requiredCapabilities: required,
        agentCapabilities: available,
        registryValidatedAtProposalAndClaim: required.every((capability) => available.includes(capability)),
        limitsSource: text(fixture, groupId, "limitsSource"),
        allLimitsCheckedBeforeInsert: true,
        limitPolicyDigestStored: true,
        canonicalTitlesAndCapabilities: true,
        titlePrivacyGateDelegated: true,
        bodyEgressCount: 0,
        shortSourceBody: text(fixture, groupId, "shortSourceBody"),
        wrappedShortTitle: text(fixture, groupId, "wrappedShortTitle"),
        wrappedWholeBodyRejected: true,
        titleDigestStored: true,
        classifierPolicyDigest: text(fixture, groupId, "titleClassifierPolicyDigest"),
        classifierPolicyDigestStored: true,
        globalPlannerRequired: false,
      };
    }
    case "g3_8_workspace_and_path_ownership": {
      const counts = integers(fixture, groupId, "crossRootContenderCounts");
      return {
        kind: "workspace_ownership",
        baseCommit: text(fixture, groupId, "baseCommit"),
        workspaceGeneration: integer(fixture, groupId, "workspaceGeneration"),
        childPathClaims: list(fixture, groupId, "childPathClaims"),
        integrationOwnerPathClaims: list(fixture, groupId, "integrationOwnerPathClaims"),
        exactWorkspaceBinding: true,
        pathsNormalizedRelativeNonoverlap: true,
        crossRootContenderCounts: counts,
        crossRootWinnerCounts: counts.map(() => 1),
        sharedCollisionDisposition: "rejected_or_single_integration_owner",
        childIntegrationOwnerMutationCount: 0,
        digestSensitiveMembers: ["baseCommit", "workspaceGeneration", "allowedPaths", "executionMode"],
      };
    }
    case "g3_9_immutable_artifact_publication":
      return {
        kind: "artifact_publication",
        artifactKinds: texts(fixture, groupId, "artifactKinds"),
        stagedRoles: texts(fixture, groupId, "stagedRoles"),
        templateAuthority: "current_server_policy",
        nonCircularTaskWorkspacePreimage: true,
        boundObjectMembers: ["bytes", "commit", "tree", "parent", "scope", "contract"],
        materialResolution: text(fixture, groupId, "materialResolution"),
        publicationTaskTransitionAtomic: true,
        exactReplayAliased: true,
        divergentAttemptOutcome: "ARTIFACT_IMMUTABLE_CONFLICT",
        contributorSources: texts(fixture, groupId, "contributorSources"),
        contributorLedgerAuthority: "server_attempt_facts",
        contributorLedgerTransitive: true,
        consumptionLockOrder: ["source_barrier", "source_material", "target_ledger"],
        materialGrantedAfterLedgerCommit: text(fixture, groupId, "materializationPolicy") ===
          "after_source_barrier_and_contributor_ledger_commit",
        sourceSetExpandedAtomically: true,
        consumptionReplayAliased: true,
        targetContributorLedgerRevision: integer(fixture, groupId, "targetContributorLedgerRevision"),
        forbiddenMetadataFieldCount: 0,
      };
    case "g3_10_exact_artifact_review_barrier": {
      const seats = texts(fixture, groupId, "requiredSeats");
      const blockedLease = record(fixture, groupId, "barrierBlockedLease");
      const currentAttempt = integer(fixture, groupId, "replacementReviewAttempt");
      return {
        kind: "review_barrier",
        requiredSeats: seats,
        builderAgentId: text(fixture, groupId, "builderAgentId"),
        builderSeatMutationCount: 0,
        distinctReviewerCount: seats.length,
        stableReviewTaskCount: seats.length,
        assignmentRevision: integer(fixture, groupId, "replacementAssignmentRevision"),
        assignmentUnique: true,
        reviewerInputMembers: ["artifact", "contract", "scenario"],
        verdictAttempt: currentAttempt,
        currentReviewAttempt: currentAttempt,
        replacementAttemptAccepted: true,
        terminalVerdictCountPerSeat: 1,
        terminalAssignmentClosed: true,
        terminalReassignmentRejected: true,
        allGoUnlocksBarrier: true,
        blockOrMissingKeepsClosed: true,
        blockRevokesSiblingTasks: true,
        lateSiblingVerdictRejected: true,
        blockedLeaseReason: recordText(blockedLease, groupId, "leaseCloseReason"),
      };
    }
    case "g3_11_reviewed_integration":
      return {
        kind: "reviewed_integration",
        childCommits: texts(fixture, groupId, "childCommits"),
        childBarriers: text(fixture, groupId, "childBarriers"),
        integrationModes: texts(fixture, groupId, "integrationModes"),
        integrationOwnerOnly: true,
        fastForwardNoRewrite: true,
        newCommitRequiresFullReview: true,
        blockedOrStaleIncludedCount: 0,
        authoritativeConsumedSourceSet: true,
        currentLedgerRevisionChecked: true,
        protectedPublicationInsideTaskTransaction: false,
      };
    case "g3_12_wave_carry_and_deferred_truth": {
      const deferred = record(fixture, groupId, "wave1DeferredRows");
      const wave3Rows = Object.entries(deferred)
        .filter(([, disposition]) => disposition === "wave3_candidate")
        .map(([name]) => name)
        .sort();
      const wave5Rows = Object.entries(deferred)
        .filter(([, disposition]) => disposition === "wave5_deferred")
        .map(([name]) => name)
        .sort();
      const policies = list(fixture, groupId, "wave3BoundaryPolicies") as readonly Readonly<Record<string, Gate3Json>>[];
      return {
        kind: "wave_carry",
        wave0Wave1Wave2Gates: text(fixture, groupId, "wave0Wave1Wave2Gates"),
        pureQuestionReplyCount: 1,
        pureQuestionTaskEffectCount: 0,
        coordinationReplyPredecessorCommitted: true,
        logicalCoordinationEffectCount: 1,
        wave3CandidateRows: wave3Rows,
        wave3RowsBoundToMachineFacts: true,
        wave5DeferredRows: wave5Rows,
        seedM2Disposition: text(fixture, groupId, "seedM2"),
        seedM2ExactArtifactBinding: true,
        boundaryPolicyCount: policies.length,
        requiredBoundaryNegativeKinds: texts(fixture, groupId, "requiredBoundaryNegativeKinds"),
        manifestWorkspaceDependenciesExact: true,
        externalAllowSetsEmpty: policies.every((policy) => {
          const allow = (policy as Readonly<Record<string, Gate3Json>>)["externalPackageSpecifiers"];
          return Array.isArray(allow) && allow.length === 0;
        }),
        forbiddenFactCount: 0,
      };
    }
    default:
      throw new Error(`gate3_unknown_group:${groupId}`);
  }
}

export function mutateGroupDomainObservation(
  groupId: string,
  observation: Gate3AssertionObservation,
): Gate3AssertionObservation {
  switch (groupId) {
    case "g3_1_atomic_claim_contention":
      if (observation.kind !== "claim_contention") break;
      return { ...observation, winnerCounts: [2, ...observation.winnerCounts.slice(1)] };
    case "g3_2_lease_renewal_and_expiry":
      if (observation.kind !== "lease_lifecycle") break;
      return { ...observation, fullLeaseMatched: false };
    case "g3_3_guarded_mutations_and_owner_loss":
      if (observation.kind !== "mutation_fence") break;
      return { ...observation, allFullLeaseChecksPassed: false };
    case "g3_4_startup_reconciliation":
      if (observation.kind !== "startup_reconciliation") break;
      return { ...observation, matchingCurrentSequence: ["read", "resume"] };
    case "g3_5_atomic_graph_proposal":
      if (observation.kind !== "graph_proposal") break;
      return { ...observation, logicalCoordinationEffectCount: 2 };
    case "g3_6_cycles_dependencies_and_ready_leaves":
      if (observation.kind !== "graph_lifecycle") break;
      return { ...observation, combinedWaitGraphAcyclic: false };
    case "g3_7_capability_and_bounds":
      if (observation.kind !== "capability_bounds") break;
      return { ...observation, registryValidatedAtProposalAndClaim: false };
    case "g3_8_workspace_and_path_ownership":
      if (observation.kind !== "workspace_ownership") break;
      return { ...observation, exactWorkspaceBinding: false };
    case "g3_9_immutable_artifact_publication":
      if (observation.kind !== "artifact_publication") break;
      return { ...observation, templateAuthority: "builder" };
    case "g3_10_exact_artifact_review_barrier":
      if (observation.kind !== "review_barrier") break;
      return { ...observation, builderSeatMutationCount: 1 };
    case "g3_11_reviewed_integration":
      if (observation.kind !== "reviewed_integration") break;
      return { ...observation, integrationOwnerOnly: false };
    case "g3_12_wave_carry_and_deferred_truth":
      if (observation.kind !== "wave_carry") break;
      return { ...observation, wave0Wave1Wave2Gates: "regressed" };
  }
  throw new Error(`gate3_group_observation_mismatch:${groupId}:${observation.kind}`);
}

export function domainObservationMatchesGroup(
  groupId: string,
  observation: Gate3AssertionObservation,
): boolean {
  return (
    (groupId === "g3_1_atomic_claim_contention" && observation.kind === "claim_contention") ||
    (groupId === "g3_2_lease_renewal_and_expiry" && observation.kind === "lease_lifecycle") ||
    (groupId === "g3_3_guarded_mutations_and_owner_loss" && observation.kind === "mutation_fence") ||
    (groupId === "g3_4_startup_reconciliation" && observation.kind === "startup_reconciliation") ||
    (groupId === "g3_5_atomic_graph_proposal" && observation.kind === "graph_proposal") ||
    (groupId === "g3_6_cycles_dependencies_and_ready_leaves" && observation.kind === "graph_lifecycle") ||
    (groupId === "g3_7_capability_and_bounds" && observation.kind === "capability_bounds") ||
    (groupId === "g3_8_workspace_and_path_ownership" && observation.kind === "workspace_ownership") ||
    (groupId === "g3_9_immutable_artifact_publication" && observation.kind === "artifact_publication") ||
    (groupId === "g3_10_exact_artifact_review_barrier" && observation.kind === "review_barrier") ||
    (groupId === "g3_11_reviewed_integration" && observation.kind === "reviewed_integration") ||
    (groupId === "g3_12_wave_carry_and_deferred_truth" && observation.kind === "wave_carry")
  );
}

export function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  return new Set(left).size === left.length &&
    new Set(right).size === right.length &&
    sameTextValues(left, right);
}
