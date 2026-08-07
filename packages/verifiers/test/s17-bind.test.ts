import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import {
  S17_CONDITION_ID,
  S17_FINDING_KIND,
  conditionIdForFinding,
  expectedS17Outcome,
  generateS17Marker,
} from "@swarm/security";

// ---------------------------------------------------------------------------
// S17 mutual-bind Gate 0 control (verifier side).
//
// This is the verifier-lane half of the S17 cross-lane bind. It mirrors the
// mechanism of packages/security/scripts/verify-s17-bind.mjs, but as an
// asserting node:test control. It proves three things about the marker the
// verifier lane would embed for scenario S17:
//
//   1. NEGATIVE / prove-it-can-fail: the runtime-generated marker, when copied
//      into a temp git tree and scanned by the PROMOTED publication gate, is
//      BLOCKED (nonzero exit). The condition id both lanes assert is derived
//      from the shared table, never hardcoded here.
//   2. POSITIVE: a neutral fixture with NO marker PASSES the same gate (zero
//      exit), so the control is not trivially always-blocking.
//   3. MAPPING INTEGRITY: the finding-kind -> condition-id table is an
//      allowlist (a mapped kind resolves; an unknown kind returns undefined).
//
// HARDCODE NOTHING: the finding kind and condition id are imported from
// @swarm/security; the only leak-shaped string anywhere is the one
// generateS17Marker produces at runtime. No real path/name/lineage literal is
// written into this source.
// ---------------------------------------------------------------------------

// Locate the repo root by walking up until the promoted gate is found, rather
// than counting a fixed number of parent hops. The compiled test file's depth
// depends on the test build's outDir, so a hardcoded hop count is fragile;
// walking up to `scripts/publication_gate.py` is resolution-depth-independent.
const thisDir = dirname(fileURLToPath(import.meta.url));

function findRepoRoot(start: string): string {
  let dir = start;
  for (let hop = 0; hop < 12; hop += 1) {
    if (existsSync(join(dir, "scripts", "publication_gate.py"))) return dir;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(
    `could not locate repo root (scripts/publication_gate.py) walking up from ${start}`,
  );
}

const repoRoot = findRepoRoot(thisDir);
const gatePath = join(repoRoot, "scripts", "publication_gate.py");
const denylistPath = join(repoRoot, ".github", "publication-denylist.sha256");

/** Assert a required external tool is present; FAIL loudly (never skip). */
function requireTool(command: string, probeArg: string): void {
  const probe = spawnSync(command, [probeArg], { encoding: "utf8" });
  assert.equal(
    probe.error,
    undefined,
    `required tool "${command}" is not available on PATH (S17 bind cannot run): ${String(probe.error)}`,
  );
}

/**
 * The `--all` scan checks commit identity as well as content. So the temp
 * commit must carry the identity the gate accepts, otherwise a marker-free
 * fixture still blocks on `disallowed-commit-identity`. Rather than write that
 * identity as a literal into this test source (which would be an identity
 * literal we must not embed), derive it at runtime from the gate file itself.
 */
function acceptedCommitIdentity(): { name: string; email: string } {
  const gateSource = readFileSync(gatePath, "utf8");
  const nameMatch = /PRODUCT_IDENTITY_NAMES\s*=\s*frozenset\(\{([^}]*)\}/u.exec(
    gateSource,
  );
  const emailMatch = /PRODUCT_IDENTITY_EMAIL\s*=\s*"([^"]+)"/u.exec(gateSource);
  assert.ok(nameMatch, "could not derive accepted commit name from the gate");
  assert.ok(emailMatch, "could not derive accepted commit email from the gate");
  const firstName = /"([^"]+)"/u.exec(nameMatch[1] ?? "");
  assert.ok(firstName, "could not parse an accepted commit name from the gate");
  return { name: firstName[1] as string, email: emailMatch[1] as string };
}

/**
 * Build a self-contained temp git tree containing the promoted gate + its
 * denylist, write `fixtureBody` as the scanned file, commit, and run
 * `publication_gate.py --all` in that tree. Returns the gate exit status and
 * combined output. Mirrors verify-s17-bind.mjs so both lanes exercise the same
 * mechanism.
 */
function runGateOverFixture(fixtureBody: string): {
  status: number | null;
  output: string;
} {
  const work = mkdtempSync(join(tmpdir(), "s17-verifier-bind-"));
  try {
    mkdirSync(join(work, ".github"), { recursive: true });
    mkdirSync(join(work, "scripts"), { recursive: true });
    cpSync(denylistPath, join(work, ".github", "publication-denylist.sha256"));
    cpSync(gatePath, join(work, "scripts", "publication_gate.py"));
    writeFileSync(join(work, "fixture.txt"), fixtureBody + "\n");

    const init = spawnSync("git", ["init", "-q", "."], { cwd: work });
    assert.equal(init.status, 0, "git init failed in S17 bind temp tree");
    spawnSync("git", ["add", "-A"], { cwd: work });
    // Commit with the gate-accepted identity (derived from the gate file, not a
    // literal), so `--all` identity scanning does not confound the content
    // signal: the only reason a fixture can block is its content.
    const identity = acceptedCommitIdentity();
    const commit = spawnSync(
      "git",
      [
        "-c",
        `user.name=${identity.name}`,
        "-c",
        `user.email=${identity.email}`,
        "commit",
        "-qm",
        "s17",
      ],
      { cwd: work },
    );
    assert.equal(commit.status, 0, "git commit failed in S17 bind temp tree");

    const scan = spawnSync(
      "python3",
      [join(work, "scripts", "publication_gate.py"), "--all"],
      { cwd: work, encoding: "utf8" },
    );
    return {
      status: scan.status,
      output: `${scan.stdout ?? ""}${scan.stderr ?? ""}`,
    };
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

test("S17 required tooling (python3 + git) is present, else fail loudly", () => {
  requireTool("python3", "--version");
  requireTool("git", "--version");
});

test("S17 negative control: promoted gate BLOCKs the runtime-generated marker", () => {
  const marker = generateS17Marker("s17-verifier-bind");
  const { status, output } = runGateOverFixture(marker);

  // The gate must FAIL closed (nonzero exit) on the leak-shaped marker.
  assert.notEqual(
    status,
    0,
    `expected the publication gate to BLOCK the S17 marker (nonzero exit), got exit ${status}\n${output}`,
  );
  // And it must attribute the block to the shared finding kind (derived from
  // the import, not a literal).
  assert.ok(
    output.includes(`BLOCK ${S17_FINDING_KIND}`),
    `expected gate output to attribute the block to "${S17_FINDING_KIND}"\n${output}`,
  );

  // The condition id both lanes assert is table-derived, not hardcoded.
  assert.equal(conditionIdForFinding(S17_FINDING_KIND), S17_CONDITION_ID);
  assert.equal(expectedS17Outcome().conditionId, S17_CONDITION_ID);
  assert.equal(conditionIdForFinding(S17_FINDING_KIND), expectedS17Outcome().conditionId);
  assert.equal(expectedS17Outcome().blocked, true);
  assert.equal(expectedS17Outcome().findingKind, S17_FINDING_KIND);
});

test("S17 positive control: a neutral fixture with no marker PASSES the gate", () => {
  const { status, output } = runGateOverFixture("hello world");
  assert.equal(
    status,
    0,
    `expected a marker-free fixture to PASS the gate (exit 0), got exit ${status}\n${output}`,
  );
});

test("S17 mapping integrity: allowlist maps the kind and rejects unknown kinds", () => {
  // A mapped kind resolves to its condition id (derived, not hardcoded).
  assert.equal(conditionIdForFinding(S17_FINDING_KIND), S17_CONDITION_ID);
  // An unknown kind returns undefined rather than guessing.
  assert.equal(conditionIdForFinding("not-a-kind"), undefined);
});
