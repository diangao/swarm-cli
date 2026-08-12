import assert from "node:assert/strict";
import test from "node:test";
import {
  parseProposeTaskGraphInputV3,
  parseProposedWorkspaceV3,
  parseRenewTaskLeaseResultV3,
  parseTaskLeaseV3,
  repositoryPathClaimsOverlapV3,
  TASK_V3_LIMITS,
} from "../src/index.js";

const id = (prefix: string, ch: string) => `${prefix}_${ch.repeat(26)}`;
const digest = (ch = "a") => `sha256:${ch.repeat(64)}`;
const lease = () => ({
  protocolVersion: 1,
  serverId: id("srv", "a"),
  rootTaskId: id("tsk", "a"),
  taskId: id("tsk", "b"),
  claimId: id("clm", "a"),
  leaseId: id("lse", "a"),
  ownerAgentId: id("agt", "a"),
  attempt: 1,
  leaseEpoch: 1,
  leaseRevision: 1,
  fenceToken: id("fnc", "a"),
  acquiredAt: "2026-08-11T16:00:00.000Z",
  expiresAt: "2026-08-11T16:00:30.000Z",
  taskRowVersion: 2,
  graphRevision: 0,
});

test("TaskLeaseV3 decoder rejects unknown, hidden, and stale-shaped members", () => {
  assert.deepEqual(parseTaskLeaseV3(lease()), lease());
  assert.throws(() => parseTaskLeaseV3({ ...lease(), surprise: true }));
  const hidden = { ...lease() };
  Object.defineProperty(hidden, "shadow", { value: 1, enumerable: false });
  assert.throws(() => parseTaskLeaseV3(hidden));
  const accessor = { ...lease() };
  Object.defineProperty(accessor, "attempt", {
    enumerable: true,
    get: () => 1,
  });
  assert.throws(() => parseTaskLeaseV3(accessor));
  assert.throws(() =>
    parseTaskLeaseV3({ ...lease(), expiresAt: lease().acquiredAt }),
  );
  assert.throws(() => parseTaskLeaseV3({ ...lease(), leaseRevision: 0 }));
});

test("renewed result advances exactly one lease revision without changing ownership", () => {
  const previousLease = lease();
  const currentLease = {
    ...previousLease,
    leaseRevision: 2,
    taskRowVersion: 3,
    expiresAt: "2026-08-11T16:00:50.000Z",
  };
  const result = parseRenewTaskLeaseResultV3({
    kind: "renewed",
    previousLease,
    currentLease,
    serverObservedAt: "2026-08-11T16:00:20.000Z",
  });
  assert.equal(result.kind, "renewed");
  if (result.kind === "renewed")
    assert.equal(result.currentLease.leaseRevision, 2);
  assert.throws(() =>
    parseRenewTaskLeaseResultV3({
      kind: "renewed",
      previousLease,
      currentLease: { ...currentLease, leaseEpoch: 2 },
      serverObservedAt: "2026-08-11T16:00:20.000Z",
    }),
  );
});

test("workspace decoder enforces normalized canonical nonoverlapping claims", () => {
  const workspace = {
    repositoryId: id("rpo", "a"),
    baseCommit: "1".repeat(40),
    baseTree: "2".repeat(40),
    workspaceGeneration: 1,
    executionMode: "isolated_worktree",
    pathClaims: [{ kind: "file", path: "src/a.ts" }],
    readArtifactDigest: null,
    integrationOwnerTaskId: id("tsk", "c"),
    artifactContractTemplateDigest: digest("b"),
    policyDigest: digest("c"),
  };
  assert.deepEqual(parseProposedWorkspaceV3(workspace), workspace);
  for (const path of ["/src/a.ts", "src\\a.ts", "src/../a.ts", "src//a.ts"]) {
    assert.throws(() =>
      parseProposedWorkspaceV3({
        ...workspace,
        pathClaims: [{ kind: "file", path }],
      }),
    );
  }
  assert.throws(() =>
    parseProposedWorkspaceV3({
      ...workspace,
      pathClaims: [
        { kind: "subtree", path: "src" },
        { kind: "file", path: "src/a.ts" },
      ],
    }),
  );
  const claimsWithExtra = [{ kind: "file", path: "src/a.ts" }];
  Object.defineProperty(claimsWithExtra, "shadow", {
    value: true,
    enumerable: false,
  });
  assert.throws(() =>
    parseProposedWorkspaceV3({ ...workspace, pathClaims: claimsWithExtra }),
  );
  assert.equal(
    repositoryPathClaimsOverlapV3(
      { kind: "subtree", path: "src" },
      { kind: "file", path: "src/a.ts" },
    ),
    true,
  );
  assert.equal(TASK_V3_LIMITS.maxPathClaims, 32);
});

test("read-only artifact workspaces carry one exact artifact and no path authority", () => {
  const workspace = {
    repositoryId: id("rpo", "a"),
    baseCommit: "1".repeat(40),
    baseTree: "2".repeat(40),
    workspaceGeneration: 1,
    executionMode: "read_only_artifact",
    pathClaims: [],
    readArtifactDigest: digest("d"),
    integrationOwnerTaskId: id("tsk", "c"),
    artifactContractTemplateDigest: digest("b"),
    policyDigest: digest("c"),
  };
  assert.deepEqual(parseProposedWorkspaceV3(workspace), workspace);
  assert.throws(() =>
    parseProposedWorkspaceV3({
      ...workspace,
      pathClaims: [{ kind: "file", path: "src/a.ts" }],
    }),
  );
  assert.throws(() =>
    parseProposedWorkspaceV3({ ...workspace, readArtifactDigest: null }),
  );
});

test("graph decoder rejects duplicate and noncanonical dependency commands", () => {
  const child = (clientKey: string, ch: string) => ({
    clientKey,
    title: `child ${clientKey}`,
    titleDigest: digest(ch),
    titleClassifierPolicyDigest: digest("9"),
    laneRole: "storage",
    requiredCapabilities: [],
    workspace: {
      repositoryId: id("rpo", "a"),
      baseCommit: "1".repeat(40),
      baseTree: "2".repeat(40),
      workspaceGeneration: 1,
      executionMode: "isolated_worktree",
      pathClaims: [{ kind: "file", path: `src/${clientKey}.ts` }],
      readArtifactDigest: null,
      integrationOwnerTaskId: id("tsk", "a"),
      artifactContractTemplateDigest: digest("b"),
      policyDigest: digest("c"),
    },
  });
  const first = {
    prerequisite: { kind: "child", clientKey: "a" },
    dependent: { kind: "task", taskId: id("tsk", "a") },
  };
  const second = {
    prerequisite: { kind: "child", clientKey: "b" },
    dependent: { kind: "task", taskId: id("tsk", "a") },
  };
  const command = {
    commandId: id("cmd", "a"),
    expectedLease: lease(),
    expectedTaskStatus: "in_progress",
    expectedTaskRowVersion: 2,
    rootTaskId: id("tsk", "a"),
    sourceMessageId: id("msg", "a"),
    sourceTurnId: id("trn", "a"),
    sourceProducerFactId: id("fac", "a"),
    committedReplyMessageId: id("msg", "b"),
    expectedGraphRevision: 0,
    expectedPolicyDigest: digest("a"),
    expectedTitleClassifierPolicyDigest: digest("9"),
    children: [child("a", "1"), child("b", "2")],
    dependencies: [first, second],
  };
  assert.equal(parseProposeTaskGraphInputV3(command).dependencies.length, 2);
  assert.throws(() =>
    parseProposeTaskGraphInputV3({
      ...command,
      dependencies: [first, first],
    }),
  );
  assert.throws(() =>
    parseProposeTaskGraphInputV3({
      ...command,
      dependencies: [second, first],
    }),
  );
});
