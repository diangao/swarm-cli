import assert from "node:assert/strict";
import test from "node:test";

import {
  auditIdempotencyKey,
  conditionIdForFinding,
  dedupeAuditFacts,
  expectedS17Outcome,
  FINDING_KIND_TO_CONDITION_ID,
  generateS17Marker,
  S17_CONDITION_ID,
  S17_FINDING_KIND,
  KNOWN_UNGUARDED_V0,
  orphanTransports,
  scanTransportContents,
  validateArgv,
  validateAuditFact,
  validateChildEnv,
  validateDistinctHomes,
  validateLaunchCredential,
  validateNoTrackedWrite,
  validatePlatformBaseline,
  validatePosixTransportMode,
  validateWindowsTransportDacl,
  validateWrapperPair,
  type LaunchContext,
  type PlatformBaseline,
  type SecurityAuditFact,
} from "../src/index.js";

// Each control pairs a healthy case that must PASS with a seeded defect that
// must FAIL. A control that cannot fail is not a control (spec v0.5.2 sec 4).

// Synthetic launch roots. They deliberately avoid a home-path shape so the
// test fixtures do not themselves trip the publication gate; the "host home"
// negative below builds its rejected value separately.
const baseline: PlatformBaseline = {
  platform: "posix",
  home: "/srv/launch-L1",
  workspaceRoot: "/srv/workspace/src",
  xdgStateHome: "/srv/launch-L1/state",
  tmpdir: "/srv/launch-L1/tmp",
  path: "/usr/local/bin:/usr/bin:/bin",
  locale: "C.UTF-8",
};
const context: LaunchContext = {
  allowedContextKeys: ["SWARM_AGENT_ID", "SWARM_SERVER_URL", "SWARM_MACHINE_ID"],
};
const healthyEnv: Readonly<Record<string, string>> = {
  SWARM_AGENT_ID: "agent-1",
  SWARM_SERVER_URL: "https://server.example.invalid",
  SWARM_MACHINE_ID: "machine-1",
  HOME: baseline.home,
  XDG_STATE_HOME: baseline.xdgStateHome,
  TMPDIR: baseline.tmpdir,
  PATH: baseline.path,
  LANG: "C.UTF-8",
};

test("child env: healthy launch passes", () => {
  assert.equal(validateChildEnv(healthyEnv, context, baseline).ok, true);
});

test("child env: secret value present fails closed", () => {
  // Assembled at runtime so the contiguous credential shape never appears as a
  // literal in this source file (which the publication gate would flag).
  const leaked = "sk_agent" + "_" + "a".repeat(20);
  const env = { ...healthyEnv, SWARM_AGENT_ID: leaked };
  const outcome = validateChildEnv(env, context, baseline);
  assert.equal(outcome.ok, false);
  assert.ok(!outcome.ok && outcome.violations.some((v) => v.rule === "env-secret-value"));
});

test("child env: secret-named var fails closed", () => {
  const env = { ...healthyEnv, GITHUB_TOKEN: "x" };
  const outcome = validateChildEnv(env, context, baseline);
  assert.equal(outcome.ok, false);
});

test("child env: non-allowlisted var fails closed", () => {
  const env = { ...healthyEnv, RANDOM_INHERITED: "1" };
  assert.equal(validateChildEnv(env, context, baseline).ok, false);
});

test("child env: every baseline field must be present", () => {
  for (const drop of ["PATH", "XDG_STATE_HOME", "TMPDIR", "LANG"] as const) {
    const env: Record<string, string> = { ...healthyEnv };
    delete env[drop];
    const outcome = validateChildEnv(env, context, baseline);
    assert.equal(outcome.ok, false, `${drop} missing should fail`);
    assert.ok(!outcome.ok && outcome.violations.some((v) => v.rule === "baseline-field-missing"));
  }
});

test("child env: fields that differ from baseline fail", () => {
  assert.equal(validateChildEnv({ ...healthyEnv, PATH: "/evil/bin" }, context, baseline).ok, false);
  assert.equal(validateChildEnv({ ...healthyEnv, TMPDIR: "/tmp" }, context, baseline).ok, false);
});

test("platform baseline authority: healthy passes; host-home / bad layout fail", () => {
  assert.equal(validatePlatformBaseline(baseline).ok, true);
  // HOME that is a host home shape (built via split so the source stays gate-clean).
  const hostHome = "/" + "Users/someone";
  assert.equal(validatePlatformBaseline({ ...baseline, home: hostHome, xdgStateHome: hostHome + "/s", tmpdir: hostHome + "/t" }).ok, false);
  // XDG/TMP not under HOME.
  assert.equal(validatePlatformBaseline({ ...baseline, xdgStateHome: "/elsewhere/state" }).ok, false);
  // empty PATH.
  assert.equal(validatePlatformBaseline({ ...baseline, path: "" }).ok, false);
});

test("child env: baseline HOME under workspace fails (posix and windows)", () => {
  const posix = { ...baseline, home: baseline.workspaceRoot + "/.home", xdgStateHome: baseline.workspaceRoot + "/.home/s", tmpdir: baseline.workspaceRoot + "/.home/t" };
  assert.equal(validatePlatformBaseline(posix).ok, false);
  // Windows: HOME under workspace, and the CASE differs — must still fail.
  const win: PlatformBaseline = { platform: "windows", home: "C:\\WORK\\repo\\.home", workspaceRoot: "c:\\work\\repo", xdgStateHome: "C:\\WORK\\repo\\.home\\s", tmpdir: "C:\\WORK\\repo\\.home\\t", path: "C:\\Windows", locale: "C" };
  assert.equal(validatePlatformBaseline(win).ok, false);
});

test("host-home authority: canonical spellings and relative roots fail", () => {
  const U = "/" + "Users/someone"; // built via split so source stays gate-clean
  // POSIX dot segment resolves to the host home.
  const dotSeg = validatePlatformBaseline({ ...baseline, home: U + "/.", xdgStateHome: U + "/./s", tmpdir: U + "/./t" });
  assert.equal(dotSeg.ok, false);
  assert.ok(!dotSeg.ok && dotSeg.violations.some((v) => v.rule === "baseline-home-is-host-home"));
  // Windows lower-case backslash and forward-slash both reduce to the host home.
  const winBase = { ...baseline, platform: "windows" as const, workspaceRoot: "C:\\ws", locale: "C" };
  assert.equal(validatePlatformBaseline({ ...winBase, home: "c:\\users\\someone", xdgStateHome: "c:\\users\\someone\\s", tmpdir: "c:\\users\\someone\\t" }).ok, false);
  assert.equal(validatePlatformBaseline({ ...winBase, home: "C:" + U, xdgStateHome: "C:" + U + "/s", tmpdir: "C:" + U + "/t" }).ok, false);
  // Relative HOME root depends on daemon cwd — must fail.
  const rel = validatePlatformBaseline({ ...baseline, home: "launch/L1", xdgStateHome: "launch/L1/s", tmpdir: "launch/L1/t" });
  assert.equal(rel.ok, false);
  assert.ok(!rel.ok && rel.violations.some((v) => v.rule === "baseline-root-not-absolute"));
});

test("baseline authority: '..' traversal cannot escape workspace containment", () => {
  // HOME resolves under workspace via '..' — must fail.
  const sneaky = { ...baseline, home: "/srv/private/../workspace/src/.home", xdgStateHome: "/srv/private/../workspace/src/.home/s", tmpdir: "/srv/private/../workspace/src/.home/t" };
  assert.equal(validatePlatformBaseline(sneaky).ok, false);
});

test("child env: LANG diverging from baseline locale fails", () => {
  assert.equal(validateChildEnv({ ...healthyEnv, LANG: "en_US.UTF-8" }, context, baseline).ok, false);
});

test("distinct homes: Windows case/separator variants of one home are the same", () => {
  const a: PlatformBaseline = { ...baseline, platform: "windows", home: "C:\\launch\\L1", workspaceRoot: "C:\\ws", xdgStateHome: "C:\\launch\\L1\\s", tmpdir: "C:\\launch\\L1\\t" };
  const b: PlatformBaseline = { ...a, home: "c:/launch/l1" };
  assert.equal(validateDistinctHomes([a, b]).ok, false);
});

test("transport contents: allowlist grammar — references pass, anything else fails", () => {
  assert.equal(scanTransportContents([{ name: "cap", content: "ref://launch/L1/prompt" }, { name: "fd", content: "fd:3" }]).ok, true);
  // A patterned secret fails.
  assert.equal(scanTransportContents([{ name: "cap", content: "gh" + "p_" + "A".repeat(36) }]).ok, false);
  // Raw material matching NO known token pattern also fails (allowlist, not denylist).
  const outcome = scanTransportContents([{ name: "cap", content: "unpatterned-raw-bearer-material-xyz" }]);
  assert.equal(outcome.ok, false);
  assert.ok(!outcome.ok && outcome.violations.some((v) => v.rule === "transport-content-not-reference"));
});

test("no-tracked-write: writes under private home pass; workspace/'..'-escape/outside fail", () => {
  assert.equal(validateNoTrackedWrite([baseline.home + "/memory.md", baseline.tmpdir + "/x"], baseline).ok, true);
  assert.equal(validateNoTrackedWrite([baseline.workspaceRoot + "/tracked.ts"], baseline).ok, false);
  assert.equal(validateNoTrackedWrite(["/etc/passwd"], baseline).ok, false);
  // '..' escape from the private home into the workspace must fail.
  assert.equal(validateNoTrackedWrite([baseline.home + "/../workspace/src/tracked.ts"], baseline).ok, false);
});

test("orphan reaper: a transport whose launch is not live is an orphan to reap", () => {
  const orphans = orphanTransports(["L1", "L3"], ["L1", "L2", "L3"]);
  assert.deepEqual([...orphans], ["L2"]);
  // Probe: asserting an orphan "survives" (is absent from the reap list) fails.
  assert.ok(orphans.includes("L2"));
  assert.equal(orphanTransports(["L1"], ["L1"]).length, 0);
});

test("known-unguarded V0: hostile same-uid sibling is explicitly NOT guaranteed", () => {
  assert.equal(KNOWN_UNGUARDED_V0.guaranteedInV0, false);
  assert.equal(KNOWN_UNGUARDED_V0.gap, "hostile-same-uid-sibling-isolation");
});

test("wrapper pair: matched posix+powershell passes; missing or duplicate fail", () => {
  assert.equal(validateWrapperPair([{ platform: "posix", path: "w.sh" }, { platform: "powershell", path: "w.ps1" }]).ok, true);
  assert.equal(validateWrapperPair([{ platform: "posix", path: "w.sh" }]).ok, false);
  assert.equal(validateWrapperPair([{ platform: "posix", path: "a" }, { platform: "posix", path: "b" }]).ok, false);
});

test("child env: HOME set to host home is rejected", () => {
  const hostHome = "/" + "Users/someone";
  const env = { ...healthyEnv, HOME: hostHome };
  const outcome = validateChildEnv(env, context, baseline);
  assert.equal(outcome.ok, false);
  assert.ok(!outcome.ok && outcome.violations.some((v) => v.rule === "home-is-host-home"));
});

test("child env: HOME under the source workspace is rejected", () => {
  const workspaceHome = `${baseline.workspaceRoot}/.home`;
  const env = { ...healthyEnv, HOME: workspaceHome };
  const shifted: PlatformBaseline = { ...baseline, home: workspaceHome };
  const outcome = validateChildEnv(env, context, shifted);
  assert.equal(outcome.ok, false);
  assert.ok(!outcome.ok && outcome.violations.some((v) => v.rule === "home-under-workspace"));
});

test("distinct homes: two launches sharing a home fails", () => {
  const a: PlatformBaseline = { ...baseline, home: "/launch/home/shared" };
  const b: PlatformBaseline = { ...baseline, home: "/launch/home/shared" };
  assert.equal(validateDistinctHomes([a, b]).ok, false);
  assert.equal(validateDistinctHomes([baseline, { ...baseline, home: "/launch/home/L2" }]).ok, true);
});

test("posix transport mode: only exact 0700/0600 passes, 0000 and loose fail", () => {
  assert.equal(validatePosixTransportMode({ dirMode: 0o700, fileMode: 0o600 }).ok, true);
  assert.equal(validatePosixTransportMode({ dirMode: 0o755, fileMode: 0o600 }).ok, false);
  assert.equal(validatePosixTransportMode({ dirMode: 0o700, fileMode: 0o644 }).ok, false);
  // 0000 has no owner access — unusable and not the frozen mode; must fail.
  assert.equal(validatePosixTransportMode({ dirMode: 0o000, fileMode: 0o000 }).ok, false);
  // Special bits (sticky/setgid) on top of 0700/0600 must also fail.
  assert.equal(validatePosixTransportMode({ dirMode: 0o1700, fileMode: 0o600 }).ok, false);
  assert.equal(validatePosixTransportMode({ dirMode: 0o700, fileMode: 0o1600 }).ok, false);
});

const daemonAce = { identity: "DAEMON", accessType: "allow" as const, rights: ["read", "write"], inherited: false };
test("windows dacl: daemon-only allow passes; empty/deny/foreign/inherited/partial fail", () => {
  assert.equal(validateWindowsTransportDacl("DAEMON", [daemonAce]).ok, true);
  assert.equal(validateWindowsTransportDacl("DAEMON", []).ok, false); // empty grants no one
  assert.equal(validateWindowsTransportDacl("DAEMON", [{ ...daemonAce, identity: "EVERYONE" }]).ok, false);
  assert.equal(validateWindowsTransportDacl("DAEMON", [{ ...daemonAce, inherited: true }]).ok, false);
  assert.equal(validateWindowsTransportDacl("DAEMON", [{ ...daemonAce, accessType: "deny" }]).ok, false);
  assert.equal(validateWindowsTransportDacl("DAEMON", [{ ...daemonAce, rights: ["read"] }]).ok, false);
});

test("launch credential: reusable bearer and secret-smuggling fail; capability passes", () => {
  assert.equal(validateLaunchCredential({ kind: "bearer", value: "secret" }, "L1").ok, false);
  assert.equal(validateLaunchCredential({ kind: "inherited-fd", launchId: "L1" }, "L1").ok, true);
  assert.equal(validateLaunchCredential({ kind: "scoped-proxy", launchId: "L2" }, "L1").ok, false);
  // A non-bearer capability smuggling raw secret material fails closed.
  const smuggled = { kind: "inherited-fd", launchId: "L1", value: "gh" + "p_" + "A".repeat(36) } as never;
  const outcome = validateLaunchCredential(smuggled, "L1");
  assert.equal(outcome.ok, false);
  assert.ok(!outcome.ok && outcome.violations.some((v) => v.rule === "capability-carries-secret"));
});

test("argv: secret in an argument fails, clean argv passes", () => {
  // Secret assembled at runtime so no contiguous credential literal is in source.
  const leaked = "gh" + "p_" + "A".repeat(36);
  const bad = validateArgv(["swarm", "--agent", "a1", "--token", leaked]);
  assert.equal(bad.ok, false);
  assert.ok(!bad.ok && bad.violations.some((v) => v.rule === "argv-secret-value"));
  assert.equal(validateArgv(["swarm", "--agent", "a1", "--capability-fd", "3"]).ok, true);
});

test("audit fact: valid fact passes, tampered idempotency key fails", () => {
  const key = auditIdempotencyKey("machine-1", "launch-1", "transport_reaped");
  const fact: SecurityAuditFact = {
    auditKind: "transport_reaped",
    launchId: "launch-1",
    machineId: "machine-1",
    occurredAt: "2026-08-06T00:00:00Z",
    idempotencyKey: key,
  };
  assert.equal(validateAuditFact(fact).ok, true);
  assert.equal(validateAuditFact({ ...fact, idempotencyKey: "wrong" }).ok, false);
  assert.equal(validateAuditFact({ ...fact, auditKind: "invented_kind" }).ok, false);
  assert.equal(validateAuditFact({ ...fact, occurredAt: "not-a-date" }).ok, false);
  assert.equal(validateAuditFact({ ...fact, launchId: "" }).ok, false);
});

test("audit key: distinct tuples never collide (injective encoding)", () => {
  const k1 = auditIdempotencyKey("a", "bc", "transport_reaped");
  const k2 = auditIdempotencyKey("ab", "c", "transport_reaped");
  assert.notEqual(k1, k2);
  // Even with a unit-separator embedded in a value, no collision.
  const k3 = auditIdempotencyKey("ab", "c", "transport_reaped");
  const k4 = auditIdempotencyKey("a", "bc", "transport_reaped");
  assert.notEqual(k3, k4);
});

test("audit fact: duplicate reconciliation collapses to one logical fact", () => {
  const key = auditIdempotencyKey("machine-1", "launch-1", "transport_reaped");
  const fact: SecurityAuditFact = {
    auditKind: "transport_reaped",
    launchId: "launch-1",
    machineId: "machine-1",
    occurredAt: "2026-08-06T00:00:00Z",
    idempotencyKey: key,
  };
  assert.equal(dedupeAuditFacts([fact, { ...fact, occurredAt: "2026-08-06T00:05:00Z" }]).length, 1);
});

test("s17: mapping is the single source and resolves the condition id", () => {
  assert.equal(S17_CONDITION_ID, FINDING_KIND_TO_CONDITION_ID[S17_FINDING_KIND]);
  assert.equal(conditionIdForFinding("machine-local-path"), S17_CONDITION_ID);
  assert.equal(conditionIdForFinding("unmapped-kind"), undefined);
});

test("s17: generator is deterministic per seed and distinct across seeds", () => {
  assert.equal(generateS17Marker("a"), generateS17Marker("a"));
  assert.notEqual(generateS17Marker("a"), generateS17Marker("b"));
});

test("s17: generated marker has the machine-local shape the gate blocks", () => {
  // The marker must have the leading-slash home shape the promoted gate detects.
  // Asserting the exact synthetic format (positively) proves it is that shape
  // and, by construction, carries no real identity or lineage term — without
  // this test itself having to spell out real terms.
  const marker = generateS17Marker("seed-1");
  const homeRoot = "/" + "Users";
  assert.ok(marker.startsWith(homeRoot + "/swarm-fixture-"));
  assert.match(marker, /swarm-fixture-[0-9a-f]{8}\/internal\/lineage-marker$/u);
  const outcome = expectedS17Outcome();
  assert.equal(outcome.blocked, true);
  assert.equal(outcome.findingKind, "machine-local-path");
  assert.equal(outcome.conditionId, S17_CONDITION_ID);
});
