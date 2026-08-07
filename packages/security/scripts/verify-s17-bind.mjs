#!/usr/bin/env node
// Reproducible S17 mutual-bind verification.
//
// Proves the cross-lane contract without asserting it: the runtime-generated
// S17 marker, when scanned by the PROMOTED publication gate
// (scripts/publication_gate.py), is BLOCKED with exactly the finding kind that
// the single-source mapping table maps to the verifier condition id. Both the
// security lane (this package) and the verifier lane (@swarm/testkit) bind to
// the same generator output and the same table, so a marker that passes the
// gate is a Gate 0 failure on both.
//
// Cross-language (Node generator + Python gate), so it is a standalone check,
// not a pnpm unit test. Requires python3 and git on PATH. Run from the repo
// root after `pnpm --filter @swarm/security build`:
//   node packages/security/scripts/verify-s17-bind.mjs

import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, mkdirSync, cpSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  S17_CONDITION_ID,
  S17_FINDING_KIND,
  conditionIdForFinding,
  generateS17Marker,
} from "../dist/s17.js";

function fail(message) {
  console.error(`S17 bind FAILED: ${message}`);
  process.exit(1);
}

const repoRoot = new URL("../../../", import.meta.url).pathname;
const gate = join(repoRoot, "scripts", "publication_gate.py");
const denylist = join(repoRoot, ".github", "publication-denylist.sha256");

const marker = generateS17Marker("s17-bind-verify");
const work = mkdtempSync(join(tmpdir(), "s17-bind-"));
try {
  // publication_gate.py resolves its scan root from its own file location
  // (parents[1]), not cwd — so the gate and its denylist are copied into the
  // temp tree, making the temp dir the scan root.
  mkdirSync(join(work, ".github"), { recursive: true });
  mkdirSync(join(work, "scripts"), { recursive: true });
  cpSync(denylist, join(work, ".github", "publication-denylist.sha256"));
  cpSync(gate, join(work, "scripts", "publication_gate.py"));
  writeFileSync(join(work, "fixture.txt"), marker + "\n");

  const init = spawnSync("git", ["init", "-q", "."], { cwd: work });
  if (init.status !== 0) fail("git init");
  spawnSync("git", ["add", "-A"], { cwd: work });
  spawnSync(
    "git",
    ["-c", "user.name=t", "-c", "user.email=t@t.invalid", "commit", "-qm", "s17"],
    { cwd: work },
  );

  const scan = spawnSync(
    "python3",
    [join(work, "scripts", "publication_gate.py"), "--all"],
    { cwd: work, encoding: "utf8" },
  );
  const output = `${scan.stdout ?? ""}${scan.stderr ?? ""}`;

  if (scan.status !== 1) {
    fail(`expected BLOCK (exit 1), got exit ${scan.status}\n${output}`);
  }
  if (!output.includes(`BLOCK ${S17_FINDING_KIND}`)) {
    fail(`expected finding kind ${S17_FINDING_KIND} in gate output\n${output}`);
  }
  if (conditionIdForFinding(S17_FINDING_KIND) !== S17_CONDITION_ID) {
    fail("mapping table does not resolve the finding kind to the condition id");
  }

  console.log(
    `S17 bind OK: promoted gate BLOCKs the marker with ${S17_FINDING_KIND} -> ${S17_CONDITION_ID}`,
  );
} finally {
  rmSync(work, { recursive: true, force: true });
}
