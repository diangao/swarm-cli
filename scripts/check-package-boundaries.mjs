import { readFile, readdir } from "node:fs/promises";
import { builtinModules } from "node:module";
import {
  dirname,
  extname,
  isAbsolute,
  join,
  relative,
  resolve,
} from "node:path";
import process from "node:process";

const root = new URL("../", import.meta.url);
const packagesRoot = new URL("../packages/", import.meta.url);
const appsRoot = new URL("../apps/", import.meta.url);
const positiveFixture = new URL(
  "../contracts/protocol/boundary-positive.ts",
  import.meta.url,
);
const legacyNegativeFixture = new URL(
  "../contracts/protocol/boundary-negative.seed.ts",
  import.meta.url,
);
const positiveVectors = new URL(
  "../contracts/package-boundaries/positive.json",
  import.meta.url,
);
const negativeVectors = new URL(
  "../contracts/package-boundaries/negative.seed.json",
  import.meta.url,
);

const highAuthorityBuiltins = new Set([
  "child_process",
  "cluster",
  "fs",
  "net",
  "process",
  "worker_threads",
]);

const policies = new Map([
  [
    "protocol",
    {
      workspace: new Set(),
      forbiddenBuiltins: highAuthorityBuiltins,
    },
  ],
  [
    "storage",
    {
      workspace: new Set(["protocol"]),
      forbiddenBuiltins: new Set(),
    },
  ],
  [
    "runtime-contract",
    {
      workspace: new Set(["protocol"]),
      forbiddenBuiltins: highAuthorityBuiltins,
    },
  ],
  [
    "drivers",
    {
      workspace: new Set(["protocol", "runtime-contract"]),
      forbiddenBuiltins: highAuthorityBuiltins,
      allowedBuiltinSubpaths: new Map([
        ["child_process", new Set(["src/codex", "src/claude"])],
        ["fs", new Set(["src/codex", "src/claude"])],
        ["net", new Set(["src/codex", "src/claude"])],
        ["process", new Set(["src/codex", "src/claude"])],
      ]),
    },
  ],
  [
    "daemon-core",
    {
      workspace: new Set(["protocol", "runtime-contract", "drivers"]),
      forbiddenBuiltins: highAuthorityBuiltins,
      forbiddenWorkspaceSubpaths: new Set(["drivers"]),
    },
  ],
  [
    "security",
    {
      workspace: new Set(["protocol"]),
      forbiddenBuiltins: new Set(),
    },
  ],
  [
    "testkit",
    {
      workspace: new Set(["protocol", "security"]),
      forbiddenBuiltins: highAuthorityBuiltins,
    },
  ],
  [
    "verifiers",
    {
      workspace: new Set(["protocol", "security", "testkit"]),
      forbiddenBuiltins: new Set([
        "child_process",
        "cluster",
        "net",
        "worker_threads",
      ]),
    },
  ],
  [
    "task-engine",
    {
      workspace: new Set(["protocol"]),
      forbiddenBuiltins: highAuthorityBuiltins,
      closedManifest: true,
      allowMissingSource: true,
    },
  ],
  [
    "artifacts",
    {
      workspace: new Set(["protocol", "security"]),
      forbiddenBuiltins: new Set([
        "child_process",
        "cluster",
        "fs",
        "net",
        "process",
        "worker_threads",
      ]),
      allowedBuiltinSubpaths: new Map([
        ["child_process", new Set(["src/git"])],
        ["fs", new Set(["src/git", "src/store"])],
        ["process", new Set(["src/git", "src/store"])],
      ]),
      closedManifest: true,
      allowMissingSource: true,
    },
  ],
  [
    "app:daemon",
    {
      workspace: new Set([
        "protocol",
        "storage",
        "runtime-contract",
        "drivers",
        "daemon-core",
      ]),
      forbiddenBuiltins: new Set(["cluster", "worker_threads"]),
    },
  ],
  [
    "app:server",
    {
      workspace: new Set([
        "protocol",
        "storage",
        "security",
        "artifacts",
        "task-engine",
      ]),
      forbiddenBuiltins: new Set(["cluster", "worker_threads"]),
      closedManifest: true,
      allowMissingSource: true,
    },
  ],
]);

const builtins = new Set(
  builtinModules.map((name) => name.replace(/^node:/u, "").split("/")[0]),
);

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await sourceFiles(path)));
    if (
      entry.isFile() &&
      [".ts", ".mts", ".cts"].includes(extname(entry.name))
    ) {
      files.push(path);
    }
  }
  return files;
}

function imports(source) {
  const found = [];
  const pattern =
    /(?:import|export)\s+(?:type\s+)?(?:[^"']*?\s+from\s+)?["']([^"']+)["']|import\s*\(\s*["']([^"']+)["']\s*\)|require\s*\(\s*["']([^"']+)["']\s*\)/gu;
  for (const match of source.matchAll(pattern)) {
    found.push(match[1] ?? match[2] ?? match[3]);
  }
  return found.filter((specifier) => specifier !== undefined);
}

function workspaceName(specifier) {
  const match = /^@swarm\/([^/]+)(?:\/|$)/u.exec(specifier);
  return match?.[1];
}

function dependencyName(specifier) {
  if (specifier.startsWith("@"))
    return specifier.split("/").slice(0, 2).join("/");
  return specifier.split("/")[0];
}

function isInside(parent, child) {
  const path = relative(parent, child);
  return path === "" || (!path.startsWith("..") && !isAbsolute(path));
}

function violation(kind, path, specifier) {
  return {
    kind,
    message: `${relative(root.pathname, path)} ${kind} ${specifier}`,
  };
}

function builtinAllowedAtPath(policy, packageRoot, path, builtin) {
  const subpaths = policy.allowedBuiltinSubpaths?.get(builtin);
  if (subpaths === undefined) return false;
  return [...subpaths].some((subpath) =>
    isInside(join(packageRoot, subpath), path),
  );
}

function violationsForSource({
  packageName,
  packageRoot,
  path,
  source,
  dependencies = {},
}) {
  const policy = policies.get(packageName);
  if (policy === undefined) {
    return [violation("unknown-package", path, packageName)];
  }

  const declared = new Set([
    ...Object.keys(dependencies.dependencies ?? {}),
    ...Object.keys(dependencies.optionalDependencies ?? {}),
    ...Object.keys(dependencies.peerDependencies ?? {}),
  ]);
  const found = [];

  for (const specifier of imports(source)) {
    if (isAbsolute(specifier)) {
      found.push(violation("absolute-import", path, specifier));
      continue;
    }

    if (specifier.startsWith(".")) {
      const importedPath = resolve(dirname(path), specifier);
      if (!isInside(packageRoot, importedPath)) {
        found.push(violation("relative-package-escape", path, specifier));
      }
      continue;
    }

    const withoutNodePrefix = specifier.replace(/^node:/u, "");
    const builtin = withoutNodePrefix.split("/")[0];
    if (builtins.has(builtin)) {
      if (
        policy.forbiddenBuiltins.has(builtin) &&
        !builtinAllowedAtPath(policy, packageRoot, path, builtin)
      ) {
        found.push(violation("forbidden-builtin", path, specifier));
      }
      continue;
    }

    const target = workspaceName(specifier);
    if (target !== undefined) {
      if (target !== packageName && !policy.workspace.has(target)) {
        found.push(violation("forbidden-workspace-import", path, specifier));
      } else if (
        policy.forbiddenWorkspaceSubpaths?.has(target) &&
        specifier !== `@swarm/${target}`
      ) {
        found.push(
          violation("forbidden-concrete-driver-import", path, specifier),
        );
      } else if (target !== packageName && !declared.has(`@swarm/${target}`)) {
        found.push(violation("undeclared-workspace-import", path, specifier));
      }
      continue;
    }

    const dependency = dependencyName(specifier);
    if (!declared.has(dependency)) {
      found.push(violation("undeclared-external-import", path, specifier));
    }
  }

  return found;
}

function manifestViolations(packageName, packageRoot, metadata) {
  const policy = policies.get(packageName);
  if (policy?.closedManifest !== true) return [];
  const metadataPath = join(packageRoot, "package.json");
  const found = [];
  for (const section of [
    "devDependencies",
    "peerDependencies",
    "optionalDependencies",
    "bundledDependencies",
    "bundleDependencies",
  ]) {
    const value = metadata[section];
    if (
      value !== undefined &&
      (Array.isArray(value) ? value.length > 0 : Object.keys(value).length > 0)
    ) {
      found.push(
        violation("forbidden-manifest-section", metadataPath, section),
      );
    }
  }
  for (const [dependency, version] of Object.entries(
    metadata.dependencies ?? {},
  )) {
    const target = workspaceName(dependency);
    if (target === undefined || !policy.workspace.has(target)) {
      found.push(
        violation("unauthorized-manifest-dependency", metadataPath, dependency),
      );
    } else if (version !== "workspace:*") {
      found.push(
        violation(
          "invalid-workspace-version",
          metadataPath,
          `${dependency}@${version}`,
        ),
      );
    }
  }
  return found;
}

async function readVectors(url) {
  const parsed = JSON.parse(await readFile(url, "utf8"));
  if (!Array.isArray(parsed))
    throw new Error(`${url.pathname} must contain an array`);
  return parsed;
}

function evaluateVector(vector) {
  const isApp = typeof vector.app === "string";
  const packageName = isApp ? `app:${vector.app}` : vector.package;
  const packageRoot = isApp
    ? join(root.pathname, "apps", vector.app)
    : join(root.pathname, "packages", vector.package);
  const path = join(packageRoot, "src", vector.path ?? `${vector.name}.ts`);
  if (isApp && !policies.has(packageName)) {
    return [violation("unknown-app", path, vector.app)];
  }
  const sourceViolations = violationsForSource({
    packageName,
    packageRoot,
    path,
    source: vector.source,
    dependencies: vector.packageJson ?? {},
  });
  return [
    ...manifestViolations(packageName, packageRoot, vector.packageJson ?? {}),
    ...sourceViolations,
  ];
}

function generatedPolicyNegativeVectors() {
  const workspaceTargets = [
    ...[...policies.keys()].filter((name) => !name.startsWith("app:")),
    ...[...policies.keys()]
      .filter((name) => name.startsWith("app:"))
      .map((name) => `app-${name.slice("app:".length)}`),
  ].sort();
  const vectors = [];
  for (const [policyName, policy] of [...policies.entries()].sort(
    ([left], [right]) => left.localeCompare(right),
  )) {
    const app = policyName.startsWith("app:")
      ? policyName.slice("app:".length)
      : undefined;
    const ownImportName = app === undefined ? policyName : `app-${app}`;
    const target = app === undefined ? { package: policyName } : { app };
    for (const workspaceTarget of workspaceTargets) {
      if (
        workspaceTarget === ownImportName ||
        policy.workspace.has(workspaceTarget)
      )
        continue;
      vectors.push({
        name: `generated-${policyName}-forbids-${workspaceTarget}`,
        ...target,
        source: `import "@swarm/${workspaceTarget}";`,
        expectedKind: "forbidden-workspace-import",
      });
    }
    for (const builtin of [...policy.forbiddenBuiltins].sort()) {
      for (const prefix of ["", "node:"]) {
        vectors.push({
          name: `generated-${policyName}-forbids-${prefix === "" ? "bare" : "node"}-${builtin}`,
          ...target,
          source: `import "${prefix}${builtin}";`,
          expectedKind: "forbidden-builtin",
        });
      }
    }
    for (const [builtin, subpaths] of policy.allowedBuiltinSubpaths ?? []) {
      for (const subpath of [...subpaths].sort()) {
        vectors.push({
          name: `generated-${policyName}-${builtin}-escapes-${subpath.replaceAll("/", "-")}`,
          ...target,
          path: `${subpath.replace(/^src\//u, "")}-escape/forbidden.ts`,
          source: `import "node:${builtin}";`,
          expectedKind: "forbidden-builtin",
        });
      }
    }
  }
  return vectors;
}

async function scanApps() {
  let entries;
  try {
    entries = await readdir(appsRoot, { withFileTypes: true });
  } catch (error) {
    if (error?.code === "ENOENT") return { appCount: 0, fileCount: 0 };
    throw error;
  }
  const found = [];
  let appCount = 0;
  let fileCount = 0;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    appCount += 1;
    const appRoot = join(appsRoot.pathname, entry.name);
    const policyName = `app:${entry.name}`;
    if (!policies.has(policyName)) {
      found.push(
        violation("unknown-app", join(appRoot, "package.json"), entry.name),
      );
      continue;
    }
    const metadataPath = join(appRoot, "package.json");
    let metadata;
    try {
      metadata = JSON.parse(await readFile(metadataPath, "utf8"));
    } catch {
      found.push(violation("invalid-app-metadata", metadataPath, entry.name));
      continue;
    }
    if (metadata.name !== `@swarm/app-${entry.name}`) {
      found.push(
        violation("app-name-mismatch", metadataPath, String(metadata.name)),
      );
    }
    found.push(...manifestViolations(policyName, appRoot, metadata));
    const sourceRoot = join(appRoot, "src");
    let files;
    try {
      files = await sourceFiles(sourceRoot);
    } catch {
      if (policies.get(policyName)?.allowMissingSource !== true) {
        found.push(violation("missing-app-source", sourceRoot, entry.name));
      }
      continue;
    }
    fileCount += files.length;
    for (const path of files) {
      found.push(
        ...violationsForSource({
          packageName: policyName,
          packageRoot: appRoot,
          path,
          source: await readFile(path, "utf8"),
          dependencies: metadata,
        }),
      );
    }
  }
  if (found.length > 0)
    throw new Error(found.map(({ message }) => message).join("\n"));
  return { appCount, fileCount };
}

async function proveSeededNegatives() {
  const legacy = violationsForSource({
    packageName: "protocol",
    packageRoot: join(packagesRoot.pathname, "protocol"),
    path: legacyNegativeFixture.pathname,
    source: await readFile(legacyNegativeFixture, "utf8"),
  });
  const legacyKinds = legacy.map(({ kind }) => kind).sort();
  const expectedLegacy = ["forbidden-builtin", "forbidden-workspace-import"];
  if (JSON.stringify(legacyKinds) !== JSON.stringify(expectedLegacy)) {
    throw new Error(
      `legacy seeded fixture expected ${expectedLegacy.join(",")}, found ${legacyKinds.join(",")}`,
    );
  }

  const vectors = [
    ...(await readVectors(negativeVectors)),
    ...generatedPolicyNegativeVectors(),
  ];
  for (const vector of vectors) {
    const kinds = evaluateVector(vector).map(({ kind }) => kind);
    if (kinds.length !== 1 || kinds[0] !== vector.expectedKind) {
      throw new Error(
        `${vector.name} expected ${vector.expectedKind}, found ${kinds.join(",") || "none"}`,
      );
    }
  }

  process.stdout.write(
    `seeded package-boundary violations detected (${legacy.length + vectors.length} controls)\n`,
  );
}

async function provePositiveVectors() {
  const vectors = await readVectors(positiveVectors);
  for (const vector of vectors) {
    const found = evaluateVector(vector);
    if (found.length > 0) {
      throw new Error(
        `${vector.name} unexpectedly failed: ${found.map(({ message }) => message).join("; ")}`,
      );
    }
  }

  const legacy = violationsForSource({
    packageName: "protocol",
    packageRoot: join(packagesRoot.pathname, "protocol"),
    path: positiveFixture.pathname,
    source: await readFile(positiveFixture, "utf8"),
  });
  if (legacy.length > 0)
    throw new Error(legacy.map(({ message }) => message).join("\n"));
}

async function scanPackages() {
  const entries = await readdir(packagesRoot, { withFileTypes: true });
  const found = [];
  let fileCount = 0;
  let packageCount = 0;

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    packageCount += 1;
    const packageName = entry.name;
    const packageRoot = join(packagesRoot.pathname, packageName);
    const policy = policies.get(packageName);
    if (policy === undefined) {
      found.push(
        violation(
          "unknown-package",
          join(packageRoot, "package.json"),
          packageName,
        ),
      );
      continue;
    }

    const metadataPath = join(packageRoot, "package.json");
    let metadata;
    try {
      metadata = JSON.parse(await readFile(metadataPath, "utf8"));
    } catch {
      found.push(
        violation("invalid-package-metadata", metadataPath, packageName),
      );
      continue;
    }
    if (metadata.name !== `@swarm/${packageName}`) {
      found.push(
        violation("package-name-mismatch", metadataPath, String(metadata.name)),
      );
    }
    found.push(...manifestViolations(packageName, packageRoot, metadata));

    const sourceRoot = join(packageRoot, "src");
    let files;
    try {
      files = await sourceFiles(sourceRoot);
    } catch {
      if (policy.allowMissingSource !== true) {
        found.push(
          violation("missing-package-source", sourceRoot, packageName),
        );
      }
      continue;
    }

    fileCount += files.length;
    for (const path of files) {
      found.push(
        ...violationsForSource({
          packageName,
          packageRoot,
          path,
          source: await readFile(path, "utf8"),
          dependencies: metadata,
        }),
      );
    }
  }

  if (found.length > 0)
    throw new Error(found.map(({ message }) => message).join("\n"));
  process.stdout.write(
    `package boundaries clean (${packageCount} packages, ${fileCount} source files)\n`,
  );
}

if (process.argv.includes("--seeded-negative")) {
  await proveSeededNegatives();
} else {
  await provePositiveVectors();
  await scanPackages();
  const apps = await scanApps();
  process.stdout.write(
    `app boundaries clean (${apps.appCount} apps, ${apps.fileCount} source files)\n`,
  );
}
