export const GATE3_EXTERNAL_PLAN_SHA256 =
  "fad5eb57e445acf73271636b782cf5db435163bb9d37c6476691ab3214f870bb";
export const GATE3_BASE_COMMIT =
  "59c7d42495aac97855fc2a248bb72cd2dd70c8dd";
export const GATE3_BASE_TREE =
  "2d53b380ea77eeae81200762f77ce92d792abf5d";
export const GATE3_INTERNAL_PLAN_DIGEST =
  "378f27b8e2669dd59643325764b47960e620e72e110d628e21f3926e05bd3b88";
export const GATE3_GROUP_COUNT = 12;
export const GATE3_ASSERTION_COUNT = 98;
export const GATE3_NEGATIVE_COUNT = 122;

export type Gate3Json =
  | string
  | number
  | boolean
  | null
  | readonly Gate3Json[]
  | { readonly [key: string]: Gate3Json };

export type Gate3NegativeSeed = {
  readonly id: string;
  readonly defect: string;
  readonly expectError?: string;
  readonly verifierMustObserve?: string;
  readonly unchanged: readonly string[];
};

export type Gate3ScenarioPlan = {
  readonly id: string;
  readonly fixture: Readonly<Record<string, Gate3Json>>;
  readonly expectedFacts: readonly string[];
  readonly assertions: readonly string[];
  readonly forbiddenFacts?: readonly string[];
  readonly negativeSeeds: readonly Gate3NegativeSeed[];
};

export type Gate3Plan = {
  readonly schemaVersion: 1;
  readonly artifactKind: "wave3_gate3_scenario_plan";
  readonly status: "eighth_review_candidate";
  readonly baseCommit: string;
  readonly baseTree: string;
  readonly contractFile: string;
  readonly wave2ContractFile: string;
  readonly wave2ContractDigest: string;
  readonly gate2PlanDigest: string;
  readonly planDigest: string;
  readonly clock: "deterministic_server_clock_with_fake_local_clock";
  readonly limits: Readonly<Record<string, number>>;
  readonly oraclePolicy: Readonly<Record<string, Gate3Json>>;
  readonly scenarios: readonly Gate3ScenarioPlan[];
};

export type Gate3AssertionObservation =
  | { readonly kind: "claim_contention"; readonly taskId: string; readonly initialStatus: string; readonly dependencyState: string; readonly contenderCounts: readonly number[]; readonly winnerCounts: readonly number[]; readonly lockedRevisionCountPerRound: readonly number[]; readonly statusAndClaimAtomic: boolean; readonly loserOutcome: string; readonly loserSharedMutationCount: number; readonly previousMaxEpoch: number; readonly winnerEpoch: number; readonly exactReplayAliasedAfterStatusAdvance: boolean }
  | { readonly kind: "lease_lifecycle"; readonly leaseEpoch: number; readonly previousLeaseRevision: number; readonly currentLeaseRevision: number; readonly fullLeaseMatched: boolean; readonly epochStable: boolean; readonly fenceTokenStable: boolean; readonly acquiredAt: string; readonly expiresAt: string; readonly renewedExpiresAt: string; readonly serverClockAuthoritative: boolean; readonly nativeTurnDurationMs: number; readonly reclaimCountDuringTurn: number; readonly expiredRenewRejected: boolean; readonly expiredPublishRejected: boolean; readonly serverExpiryAtomic: boolean; readonly standaloneReleaseAtomic: boolean; readonly reclaimedEpoch: number; readonly earlyCommandId: string; readonly retryCommandId: string; readonly earlyDispatchSuppressed: boolean; readonly retryDispatchAt: string; readonly retryNotBefore: string; readonly earlyReplayCode: string; readonly retryUsesFreshCommand: boolean }
  | { readonly kind: "mutation_fence"; readonly taskStatus: string; readonly leaseEpoch: number; readonly leaseRevision: number; readonly checkedMutationKinds: readonly string[]; readonly allFullLeaseChecksPassed: boolean; readonly staleOwnerSharedMutationCount: number; readonly ownershipLossSequence: readonly string[]; readonly lateResultDisposition: string; readonly replacementEpoch: number; readonly staleCleanupAffectedReplacement: boolean }
  | { readonly kind: "startup_reconciliation"; readonly cases: readonly string[]; readonly matchingCurrentSequence: readonly string[]; readonly expiredAndReplacedStop: boolean; readonly expiredAndReplacedPublicationCount: number; readonly localOnlyStopped: boolean; readonly localOnlySharedMutationCount: number; readonly sharedOnlyDisposition: string; readonly fabricatedProcessCount: number; readonly relinquishAtomic: boolean; readonly fullCrashRecoveryDeferred: boolean }
  | { readonly kind: "graph_proposal"; readonly rootTaskId: string; readonly childKeys: readonly string[]; readonly expectedGraphRevision: number; readonly committedReplyPresent: boolean; readonly logicalCoordinationEffectCount: number; readonly childrenEdgesAndRevisionAtomic: boolean; readonly exactReplayAliased: boolean; readonly changedPayloadOutcome: string; readonly sourceJoinMembers: readonly string[]; readonly sourceJoinPassed: boolean; readonly parentStatusAfter: string; readonly distinctSecondCommandOutcome: string }
  | { readonly kind: "graph_lifecycle"; readonly taskNames: readonly string[]; readonly containsEdges: readonly Gate3Json[]; readonly dependencyEdges: readonly Gate3Json[]; readonly edgeKindsDistinct: boolean; readonly combinedWaitGraphAcyclic: boolean; readonly onlyReadyLeafClaimable: boolean; readonly dependencyCompletionAtomic: boolean; readonly blockedDependencyFailClosed: boolean; readonly crossScopeEdgesRejected: boolean; readonly reachedPgLatches: readonly string[]; readonly pgSchedules: readonly string[]; readonly blockedAffectedTaskIds: readonly string[]; readonly closureDerivedFromStoredTopology: boolean; readonly allAffectedBlocked: boolean; readonly graphBlockedLeaseReasonExact: boolean; readonly typedBeforeAfterRowsExact: boolean; readonly lockOrder: string; readonly bothSerialOrdersObserved: boolean; readonly deadlockCount: number }
  | { readonly kind: "capability_bounds"; readonly requiredCapabilities: readonly string[]; readonly agentCapabilities: readonly string[]; readonly registryValidatedAtProposalAndClaim: boolean; readonly limitsSource: string; readonly allLimitsCheckedBeforeInsert: boolean; readonly limitPolicyDigestStored: boolean; readonly canonicalTitlesAndCapabilities: boolean; readonly titlePrivacyGateDelegated: boolean; readonly bodyEgressCount: number; readonly shortSourceBody: string; readonly wrappedShortTitle: string; readonly wrappedWholeBodyRejected: boolean; readonly titleDigestStored: boolean; readonly classifierPolicyDigest: string; readonly classifierPolicyDigestStored: boolean; readonly globalPlannerRequired: boolean }
  | { readonly kind: "workspace_ownership"; readonly baseCommit: string; readonly workspaceGeneration: number; readonly childPathClaims: readonly Gate3Json[]; readonly integrationOwnerPathClaims: readonly Gate3Json[]; readonly exactWorkspaceBinding: boolean; readonly pathsNormalizedRelativeNonoverlap: boolean; readonly crossRootContenderCounts: readonly number[]; readonly crossRootWinnerCounts: readonly number[]; readonly sharedCollisionDisposition: string; readonly childIntegrationOwnerMutationCount: number; readonly digestSensitiveMembers: readonly string[] }
  | { readonly kind: "artifact_publication"; readonly artifactKinds: readonly string[]; readonly stagedRoles: readonly string[]; readonly templateAuthority: string; readonly nonCircularTaskWorkspacePreimage: boolean; readonly boundObjectMembers: readonly string[]; readonly materialResolution: string; readonly publicationTaskTransitionAtomic: boolean; readonly exactReplayAliased: boolean; readonly divergentAttemptOutcome: string; readonly contributorSources: readonly string[]; readonly contributorLedgerAuthority: string; readonly contributorLedgerTransitive: boolean; readonly consumptionLockOrder: readonly string[]; readonly materialGrantedAfterLedgerCommit: boolean; readonly sourceSetExpandedAtomically: boolean; readonly consumptionReplayAliased: boolean; readonly targetContributorLedgerRevision: number; readonly forbiddenMetadataFieldCount: number }
  | { readonly kind: "review_barrier"; readonly requiredSeats: readonly string[]; readonly builderAgentId: string; readonly builderSeatMutationCount: number; readonly distinctReviewerCount: number; readonly stableReviewTaskCount: number; readonly assignmentRevision: number; readonly assignmentUnique: boolean; readonly reviewerInputMembers: readonly string[]; readonly verdictAttempt: number; readonly currentReviewAttempt: number; readonly replacementAttemptAccepted: boolean; readonly terminalVerdictCountPerSeat: number; readonly terminalAssignmentClosed: boolean; readonly terminalReassignmentRejected: boolean; readonly allGoUnlocksBarrier: boolean; readonly blockOrMissingKeepsClosed: boolean; readonly blockRevokesSiblingTasks: boolean; readonly lateSiblingVerdictRejected: boolean; readonly blockedLeaseReason: string }
  | { readonly kind: "reviewed_integration"; readonly childCommits: readonly string[]; readonly childBarriers: string; readonly integrationModes: readonly string[]; readonly integrationOwnerOnly: boolean; readonly fastForwardNoRewrite: boolean; readonly newCommitRequiresFullReview: boolean; readonly blockedOrStaleIncludedCount: number; readonly authoritativeConsumedSourceSet: boolean; readonly currentLedgerRevisionChecked: boolean; readonly protectedPublicationInsideTaskTransaction: boolean }
  | { readonly kind: "wave_carry"; readonly wave0Wave1Wave2Gates: string; readonly pureQuestionReplyCount: number; readonly pureQuestionTaskEffectCount: number; readonly coordinationReplyPredecessorCommitted: boolean; readonly logicalCoordinationEffectCount: number; readonly wave3CandidateRows: readonly string[]; readonly wave3RowsBoundToMachineFacts: boolean; readonly wave5DeferredRows: readonly string[]; readonly seedM2Disposition: string; readonly seedM2ExactArtifactBinding: boolean; readonly boundaryPolicyCount: number; readonly requiredBoundaryNegativeKinds: readonly string[]; readonly manifestWorkspaceDependenciesExact: boolean; readonly externalAllowSetsEmpty: boolean; readonly forbiddenFactCount: number };

export type Gate3AssertionFact = {
  readonly groupId: string;
  readonly assertionId: string;
  readonly evidenceKinds: readonly string[];
  readonly observation: Gate3AssertionObservation;
  readonly observationDigest: string;
};

export type Gate3SiblingFact = {
  readonly name: string;
  readonly revision: number;
  readonly valueDigest: string;
};

export type Gate3FactBundle = {
  readonly schemaVersion: 1;
  readonly groups: readonly {
    readonly groupId: string;
    readonly assertions: readonly Gate3AssertionFact[];
  }[];
  readonly negatives: readonly {
    readonly groupId: string;
    readonly seedId: string;
    readonly defectActionDigest: string;
    readonly observedOutcome: string;
    readonly outcomeSource: "independent_defect_registry";
    readonly siblingsBefore: readonly Gate3SiblingFact[];
    readonly siblingsAfter: readonly Gate3SiblingFact[];
  }[];
};

export type Gate3AssertionResult = {
  readonly groupId: string;
  readonly assertionId: string;
  readonly passed: boolean;
  readonly observationDigest: string;
};

export type Gate3NegativeResult = {
  readonly groupId: string;
  readonly seedId: string;
  readonly passed: boolean;
  readonly observedOutcome: string;
  readonly expectedOutcome: string;
  readonly unchangedSiblingNames: readonly string[];
};

export type Gate3Run = {
  readonly passed: boolean;
  readonly assertionResults: readonly Gate3AssertionResult[];
  readonly negativeResults: readonly Gate3NegativeResult[];
  readonly assertionExecutions: Readonly<Record<string, number>>;
  readonly negativeExecutions: Readonly<Record<string, number>>;
};

export class Gate3PlanError extends Error {
  public readonly code: string;

  public constructor(code: string, detail: string) {
    super(`${code}:${detail}`);
    this.name = "Gate3PlanError";
    this.code = code;
  }
}

export class Gate3OracleError extends Error {
  public readonly code: string;

  public constructor(code: string, detail: string) {
    super(`${code}:${detail}`);
    this.name = "Gate3OracleError";
    this.code = code;
  }
}
