/**
 * Launch-environment policy validators (pure contract functions consumed by the
 * Wave 2 driver lane). These functions decide PASS/FAIL over plain data; they
 * perform no process, filesystem, or network effect. Enforcement (spawning,
 * chmod, capability wiring) belongs to the daemon; this package owns only the
 * contract the daemon must satisfy.
 *
 * The V0 threat model (see docs/security/launch-environment-policy.md): the
 * daemon and every child it launches run under the SAME OS uid. Filesystem
 * modes bound OTHER-uid access only. V0 does not claim hostile same-uid sibling
 * isolation; it guarantees accidental-cross-launch-leakage prevention,
 * ambient-secret minimization, and blast-radius reduction. Those three are what
 * these validators check.
 */

/** A single policy violation. `detail` names the shape, never a secret value. */
export type Violation = {
  readonly rule: string;
  readonly detail: string;
};

export type PolicyResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly violations: readonly Violation[] };

function result(violations: Violation[]): PolicyResult {
  return violations.length === 0 ? { ok: true } : { ok: false, violations };
}

/** Env variable name patterns that must never reach a launched child. */
const SECRET_NAME_PATTERNS: readonly RegExp[] = [
  /_API_KEY$/u,
  /_TOKEN$/u,
  /_SECRET$/u,
  /_PASSWORD$/u,
  /^AWS_(?:ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)$/u,
  /^GOOGLE_APPLICATION_CREDENTIALS$/u,
  /^SSH_AUTH_SOCK$/u,
  /^GH_TOKEN$/u,
  /^GITHUB_TOKEN$/u,
];

/** Value shapes that indicate a raw secret leaked into an env value. */
const SECRET_VALUE_PATTERNS: readonly RegExp[] = [
  /gh[pousr]_[A-Za-z0-9]{30,}/u,
  /sk-ant-[A-Za-z0-9_-]{10,}/u,
  /sk_(?:agent|machine)_[A-Za-z0-9]{10,}/u,
  /xox[baprs]-[0-9]/u,
  /AKIA[0-9A-Z]{16}/u,
];

/** The env keys a child is allowed to receive beyond the platform baseline. */
export type LaunchContext = {
  readonly allowedContextKeys: readonly string[];
};

/** The synthesized platform baseline a launch must carry (never inherited). */
export type PlatformBaseline = {
  /** OS family the launch runs on; governs path canonicalization. */
  readonly platform: "posix" | "windows";
  /** Per-launch private home, distinct from the source workspace. */
  readonly home: string;
  /** The source workspace/worktree root, used only to prove HOME != workspace. */
  readonly workspaceRoot: string;
  readonly xdgStateHome: string;
  readonly tmpdir: string;
  readonly path: string;
  /** Fixed minimal locale (LANG); enforced by exact equality like other fields. */
  readonly locale: string;
};

/**
 * Canonical path identity for containment/distinct/write checks: normalize
 * separators to `/`, resolve `.` and `..` segments, and on Windows case-fold so
 * the case-insensitive filesystem cannot be used to escape a check.
 */
function canonicalPath(path: string, platform: "posix" | "windows"): string {
  const slashed = path.replace(/\\/gu, "/");
  // Separate a Windows drive prefix (`C:`) so it is not treated as a segment
  // and does not receive a leading root slash.
  const driveMatch = /^([A-Za-z]:)(\/.*)?$/u.exec(slashed);
  const drive = driveMatch ? driveMatch[1] : "";
  const body = driveMatch ? (driveMatch[2] ?? "") : slashed;
  const isAbsolute = drive !== "" || body.startsWith("/");
  const segments: string[] = [];
  for (const segment of body.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") {
      const last = segments[segments.length - 1];
      if (segments.length > 0 && last !== "..") {
        segments.pop();
      } else if (!isAbsolute) {
        segments.push("..");
      }
      continue;
    }
    segments.push(segment);
  }
  const rootSlash = isAbsolute ? "/" : "";
  const joined = drive + rootSlash + segments.join("/");
  return platform === "windows" ? joined.toLowerCase() : joined;
}

/** Containment check on canonical paths, so `..` and Windows case cannot escape. */
function isUnder(
  child: string,
  parent: string,
  platform: "posix" | "windows",
): boolean {
  const nc = canonicalPath(child, platform);
  const np = canonicalPath(parent, platform);
  const withSep = np.endsWith("/") ? np : `${np}/`;
  return nc === np || nc.startsWith(withSep);
}

// Host-home shapes on the CANONICAL path (see canonicalPath). POSIX keeps case;
// Windows canonical is lowercase with forward slashes, so a lower/forward/back
// spelling all reduce to the same form before classification.
const POSIX_HOST_HOME = new RegExp("^/(?:" + "Users|home)/[^/]+$", "u");
const WINDOWS_HOST_HOME = /^[a-z]:\/users\/[^/]+$/u;

function looksLikeHostHome(path: string, platform: "posix" | "windows"): boolean {
  const canonical = canonicalPath(path, platform);
  return platform === "windows"
    ? WINDOWS_HOST_HOME.test(canonical)
    : POSIX_HOST_HOME.test(canonical);
}

/** A launch root must be absolute so it never depends on the daemon cwd. */
function isAbsolutePath(path: string, platform: "posix" | "windows"): boolean {
  const slashed = path.replace(/\\/gu, "/");
  return platform === "windows"
    ? /^[A-Za-z]:\//u.test(slashed)
    : slashed.startsWith("/");
}

/**
 * Validate the synthesized platform baseline itself before trusting it as the
 * authority the child env is checked against. The private HOME must not be a
 * host-home shape and must not sit under the source workspace; XDG state and
 * TMPDIR must live under HOME; PATH must be non-empty.
 */
export function validatePlatformBaseline(baseline: PlatformBaseline): PolicyResult {
  const violations: Violation[] = [];
  const p = baseline.platform;
  if (baseline.home.length === 0) {
    violations.push({ rule: "baseline-home-empty", detail: "home" });
  }
  // Every launch root must be absolute (never cwd-relative).
  for (const [field, value] of [
    ["home", baseline.home],
    ["workspaceRoot", baseline.workspaceRoot],
    ["xdgStateHome", baseline.xdgStateHome],
    ["tmpdir", baseline.tmpdir],
  ] as const) {
    if (!isAbsolutePath(value, p)) {
      violations.push({ rule: "baseline-root-not-absolute", detail: field });
    }
  }
  if (looksLikeHostHome(baseline.home, p)) {
    violations.push({ rule: "baseline-home-is-host-home", detail: "home" });
  }
  if (isUnder(baseline.home, baseline.workspaceRoot, p)) {
    violations.push({ rule: "baseline-home-under-workspace", detail: "home" });
  }
  if (!isUnder(baseline.xdgStateHome, baseline.home, p)) {
    violations.push({ rule: "baseline-xdg-not-under-home", detail: "xdgStateHome" });
  }
  if (!isUnder(baseline.tmpdir, baseline.home, p)) {
    violations.push({ rule: "baseline-tmpdir-not-under-home", detail: "tmpdir" });
  }
  if (baseline.path.length === 0) {
    violations.push({ rule: "baseline-path-empty", detail: "path" });
  }
  if (baseline.locale.length === 0) {
    violations.push({ rule: "baseline-locale-empty", detail: "locale" });
  }
  return result(violations);
}

/**
 * Validate the environment handed to a launched child. Default-deny: only the
 * allowlisted context keys plus the synthesized platform baseline may appear;
 * every baseline field must be present and equal the (independently validated)
 * baseline; any secret-shaped name or value fails closed.
 */
export function validateChildEnv(
  env: Readonly<Record<string, string>>,
  context: LaunchContext,
  baseline: PlatformBaseline,
): PolicyResult {
  const violations: Violation[] = [];
  // The baseline is the authority; reject a malformed one before comparing.
  const baselineCheck = validatePlatformBaseline(baseline);
  if (!baselineCheck.ok) violations.push(...baselineCheck.violations);

  const baselineKeys = new Set([
    "HOME",
    "XDG_STATE_HOME",
    "TMPDIR",
    "PATH",
    "LANG",
  ]);
  const allowed = new Set([...context.allowedContextKeys, ...baselineKeys]);

  for (const [name, value] of Object.entries(env)) {
    if (!allowed.has(name)) {
      violations.push({ rule: "env-not-allowlisted", detail: name });
    }
    if (SECRET_NAME_PATTERNS.some((pattern) => pattern.test(name))) {
      violations.push({ rule: "env-secret-name", detail: name });
    }
    if (SECRET_VALUE_PATTERNS.some((pattern) => pattern.test(value))) {
      violations.push({ rule: "env-secret-value", detail: `${name}=<redacted>` });
    }
  }

  // Every baseline field must be present AND equal the validated baseline.
  const expected: ReadonlyArray<readonly [string, string]> = [
    ["HOME", baseline.home],
    ["XDG_STATE_HOME", baseline.xdgStateHome],
    ["TMPDIR", baseline.tmpdir],
    ["PATH", baseline.path],
    ["LANG", baseline.locale],
  ];
  for (const [name, wanted] of expected) {
    const actual = env[name];
    if (actual === undefined) {
      violations.push({ rule: "baseline-field-missing", detail: name });
    } else if (actual !== wanted) {
      violations.push({ rule: "baseline-field-not-baseline", detail: name });
    }
  }
  // Defense in depth: even matching the baseline, HOME must not be host/workspace.
  const home = env["HOME"];
  if (home !== undefined) {
    if (looksLikeHostHome(home, baseline.platform)) {
      violations.push({ rule: "home-is-host-home", detail: "HOME" });
    }
    if (isUnder(home, baseline.workspaceRoot, baseline.platform)) {
      violations.push({ rule: "home-under-workspace", detail: "HOME" });
    }
  }

  return result(violations);
}

/**
 * Assert two launches receive distinct private homes. A shared home is a
 * cross-launch-leakage failure even when each home is otherwise well formed.
 */
export function validateDistinctHomes(
  baselines: readonly PlatformBaseline[],
): PolicyResult {
  const seen = new Set<string>();
  const violations: Violation[] = [];
  for (const baseline of baselines) {
    // Canonical identity so case/separator variants of one home collapse.
    const key = canonicalPath(baseline.home, baseline.platform);
    if (seen.has(key)) {
      violations.push({ rule: "shared-launch-home", detail: "HOME" });
    }
    seen.add(key);
  }
  return result(violations);
}

export type TransportPermissions = {
  /** POSIX octal mode of the transport directory, e.g. 0o700. */
  readonly dirMode: number;
  /** POSIX octal mode of each transport file, e.g. 0o600. */
  readonly fileMode: number;
};

/**
 * Validate POSIX transport permissions fail-closed against the frozen contract:
 * the directory must be exactly `0700` and each file exactly `0600`. Anything
 * else — group/other access OR an owner that cannot use the transport (e.g.
 * `0000`) — fails. Modes bound OTHER-uid access only (V0 threat model), not
 * same-uid isolation.
 */
export function validatePosixTransportMode(
  permissions: TransportPermissions,
): PolicyResult {
  const violations: Violation[] = [];
  // Mask includes the special bits (setuid/setgid/sticky) so 0o1700 / 0o1600
  // are rejected too — the frozen contract is exactly 0700 / 0600.
  if ((permissions.dirMode & 0o7777) !== 0o700) {
    violations.push({ rule: "transport-dir-mode-not-0700", detail: "dir" });
  }
  if ((permissions.fileMode & 0o7777) !== 0o600) {
    violations.push({ rule: "transport-file-mode-not-0600", detail: "file" });
  }
  return result(violations);
}

/**
 * A Windows DACL access-control entry: identity, allow-vs-deny, the granted
 * rights, and whether it was inherited.
 */
export type WindowsAce = {
  readonly identity: string;
  readonly accessType: "allow" | "deny";
  readonly rights: readonly string[];
  readonly inherited: boolean;
};

const REQUIRED_TRANSPORT_RIGHTS: readonly string[] = ["read", "write"];

/**
 * Validate a Windows transport DACL fail-closed: it must grant the daemon
 * identity (and only it) the required rights via exactly one non-inherited
 * allow ACE, with no inherited ACEs, no foreign ACEs, and no deny ACEs. An
 * empty DACL (grants no one, so the daemon cannot prove access) fails.
 */
export function validateWindowsTransportDacl(
  daemonIdentity: string,
  aces: readonly WindowsAce[],
): PolicyResult {
  const violations: Violation[] = [];
  let daemonGrant = 0;
  for (const ace of aces) {
    if (ace.inherited) {
      violations.push({ rule: "transport-inherited-ace", detail: ace.identity });
      continue;
    }
    if (ace.accessType === "deny") {
      violations.push({ rule: "transport-deny-ace", detail: ace.identity });
      continue;
    }
    if (ace.identity !== daemonIdentity) {
      violations.push({ rule: "transport-foreign-ace", detail: ace.identity });
      continue;
    }
    const grantsAll = REQUIRED_TRANSPORT_RIGHTS.every((right) =>
      ace.rights.includes(right),
    );
    if (!grantsAll) {
      violations.push({ rule: "transport-daemon-rights-incomplete", detail: "daemon" });
    } else {
      daemonGrant += 1;
    }
  }
  if (daemonGrant === 0) {
    violations.push({ rule: "transport-no-daemon-grant", detail: "daemon" });
  }
  return result(violations);
}

/**
 * Validate that a launch credential is a launch-bound capability, not a
 * reusable bearer secret written into the transport. A capability reduces
 * blast radius; it does not confer same-uid confidentiality (threat model).
 * A non-bearer capability that smuggles raw secret material also fails closed.
 */
export type LaunchCredential =
  | { readonly kind: "inherited-fd"; readonly launchId: string }
  | { readonly kind: "scoped-proxy"; readonly launchId: string }
  | { readonly kind: "bearer"; readonly value: string };

export function validateLaunchCredential(
  credential: LaunchCredential,
  launchId: string,
): PolicyResult {
  if (credential.kind === "bearer") {
    return result([{ rule: "reusable-bearer-credential", detail: credential.kind }]);
  }
  if (credential.launchId !== launchId) {
    return result([{ rule: "credential-launch-mismatch", detail: credential.kind }]);
  }
  // A launch-bound capability carries no raw secret material. Reject any extra
  // secret-shaped value smuggled onto the record beyond the declared shape.
  const extra = credential as Record<string, unknown>;
  for (const [key, value] of Object.entries(extra)) {
    if (key === "kind" || key === "launchId") continue;
    if (
      typeof value === "string" &&
      SECRET_VALUE_PATTERNS.some((pattern) => pattern.test(value))
    ) {
      return result([{ rule: "capability-carries-secret", detail: key }]);
    }
    return result([{ rule: "capability-unexpected-field", detail: key }]);
  }
  return result([]);
}

/**
 * Validate the argv a child is launched with. argv is world-readable via process
 * listings, so no secret-shaped token may appear in any argument. Secrets travel
 * only by the launch-bound capability (see the policy doc). Reports the argument
 * index, never the value.
 */
export function validateArgv(argv: readonly string[]): PolicyResult {
  const violations: Violation[] = [];
  argv.forEach((arg, index) => {
    if (SECRET_VALUE_PATTERNS.some((pattern) => pattern.test(arg))) {
      violations.push({ rule: "argv-secret-value", detail: `argv[${index}]` });
    }
  });
  return result(violations);
}

/** A file placed in a launch transport directory. */
export type TransportFile = {
  readonly name: string;
  readonly content: string;
};

/**
 * Allowed transport reference shapes. Only non-reusable references belong in a
 * transport file: a launch-scoped capability reference (`ref://…`), a file
 * descriptor handle (`fd:N`), or a wrapper path reference (`wrapper://…`). This
 * is an ALLOWLIST grammar, not a secret denylist — anything that is not a
 * recognized reference fails, so raw secret material that matches no known
 * token pattern is still rejected.
 */
const TRANSPORT_REFERENCE_GRAMMAR: readonly RegExp[] = [
  /^ref:\/\/[A-Za-z0-9._\-/]+$/u,
  /^fd:\d+$/u,
  /^wrapper:\/\/[A-Za-z0-9._\-/]+$/u,
];

/**
 * Scan transport directory contents against the reference allowlist. A file
 * whose (trimmed) content is not exactly one allowed reference is a leak.
 * Reports the file name, never the value.
 */
export function scanTransportContents(
  files: readonly TransportFile[],
): PolicyResult {
  const violations: Violation[] = [];
  for (const file of files) {
    const content = file.content.trim();
    const isReference = TRANSPORT_REFERENCE_GRAMMAR.some((pattern) =>
      pattern.test(content),
    );
    if (!isReference) {
      violations.push({ rule: "transport-content-not-reference", detail: file.name });
    }
  }
  return result(violations);
}

/**
 * A write the launch would perform. Writes must stay within the per-launch
 * private HOME / transport root and must never touch the (possibly git-tracked)
 * source workspace.
 */
export function validateNoTrackedWrite(
  writePaths: readonly string[],
  baseline: PlatformBaseline,
): PolicyResult {
  const violations: Violation[] = [];
  const p = baseline.platform;
  for (const path of writePaths) {
    const insidePrivate =
      isUnder(path, baseline.home, p) || isUnder(path, baseline.tmpdir, p);
    if (isUnder(path, baseline.workspaceRoot, p) || !insidePrivate) {
      violations.push({ rule: "write-outside-private-home", detail: path });
    }
  }
  return result(violations);
}

/**
 * Reconciliation reaper: given the launch ids that are live and the launch ids
 * that still own a transport directory on disk, an orphan is a transport whose
 * launch is no longer live. Returns the orphans that MUST be reaped; a nonzero
 * list means reconciliation has work, and a probe asserting an orphan survives
 * (i.e. is absent from this list) fails.
 */
export function orphanTransports(
  liveLaunchIds: readonly string[],
  transportLaunchIds: readonly string[],
): readonly string[] {
  const live = new Set(liveLaunchIds);
  return transportLaunchIds.filter((id) => !live.has(id));
}

/**
 * The V0 known-unguarded gap, exposed as an explicit, testable fact rather than
 * an implicit assumption. A hostile or compromised SAME-uid sibling is NOT
 * isolated in V0; that requires an OS-enforced binding (per-launch uid,
 * sandbox, container) which is the V1 axis. Any code or test that treats
 * same-uid sibling isolation as guaranteed is wrong.
 */
export const KNOWN_UNGUARDED_V0 = Object.freeze({
  gap: "hostile-same-uid-sibling-isolation",
  guaranteedInV0: false,
  requires: "os-enforced-per-launch-uid-or-sandbox-or-container",
} as const);

/** A launch wrapper produced for one platform. */
export type LaunchWrapper = {
  readonly platform: "posix" | "powershell";
  readonly path: string;
};

/**
 * A launch must ship a matched POSIX + PowerShell wrapper pair so both platforms
 * receive the same isolation contract. Missing or duplicated platforms fail.
 */
export function validateWrapperPair(
  wrappers: readonly LaunchWrapper[],
): PolicyResult {
  const platforms = wrappers.map((wrapper) => wrapper.platform);
  const violations: Violation[] = [];
  for (const required of ["posix", "powershell"] as const) {
    const count = platforms.filter((platform) => platform === required).length;
    if (count !== 1) {
      violations.push({ rule: "wrapper-pair-incomplete", detail: required });
    }
  }
  return result(violations);
}
