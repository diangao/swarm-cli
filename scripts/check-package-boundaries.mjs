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
      forbiddenBuiltins: new Set(["child_process", "cluster", "net", "worker_threads"]),
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
    if (entry.isFile() && [".ts", ".mts", ".cts"].includes(extname(entry.name))) {
      files.push(path);
    }
  }
  return files;
}

function imports(source) {
  const found = [];
  const pattern = /(?:import|export)\s+(?:type\s+)?(?:[^"']*?\s+from\s+)?["']([^"']+)["']|import\s*\(\s*["']([^"']+)["']\s*\)|require\s*\(\s*["']([^"']+)["']\s*\)/gu;
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
  if (specifier.startsWith("@")) return specifier.split("/").slice(0, 2).join("/");
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
      if (policy.forbiddenBuiltins.has(builtin)) {
        found.push(violation("forbidden-builtin", path, specifier));
      }
      continue;
    }

    const target = workspaceName(specifier);
    if (target !== undefined) {
      if (target !== packageName && !policy.workspace.has(target)) {
        found.push(violation("forbidden-workspace-import", path, specifier));
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

async function readVectors(url) {
  const parsed = JSON.parse(await readFile(url, "utf8"));
  if (!Array.isArray(parsed)) throw new Error(`${url.pathname} must contain an array`);
  return parsed;
}

function evaluateVector(vector) {
  const packageRoot = join(root.pathname, "packages", vector.package);
  const path = join(packageRoot, "src", `${vector.name}.ts`);
  return violationsForSource({
    packageName: vector.package,
    packageRoot,
    path,
    source: vector.source,
    dependencies: vector.packageJson ?? {},
  });
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

  const vectors = await readVectors(negativeVectors);
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
      throw new Error(`${vector.name} unexpectedly failed: ${found.map(({ message }) => message).join("; ")}`);
    }
  }

  const legacy = violationsForSource({
    packageName: "protocol",
    packageRoot: join(packagesRoot.pathname, "protocol"),
    path: positiveFixture.pathname,
    source: await readFile(positiveFixture, "utf8"),
  });
  if (legacy.length > 0) throw new Error(legacy.map(({ message }) => message).join("\n"));
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
      found.push(violation("unknown-package", join(packageRoot, "package.json"), packageName));
      continue;
    }

    const metadataPath = join(packageRoot, "package.json");
    let metadata;
    try {
      metadata = JSON.parse(await readFile(metadataPath, "utf8"));
    } catch {
      found.push(violation("invalid-package-metadata", metadataPath, packageName));
      continue;
    }
    if (metadata.name !== `@swarm/${packageName}`) {
      found.push(violation("package-name-mismatch", metadataPath, String(metadata.name)));
    }

    const sourceRoot = join(packageRoot, "src");
    let files;
    try {
      files = await sourceFiles(sourceRoot);
    } catch {
      found.push(violation("missing-package-source", sourceRoot, packageName));
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

  if (found.length > 0) throw new Error(found.map(({ message }) => message).join("\n"));
  process.stdout.write(`package boundaries clean (${packageCount} packages, ${fileCount} source files)\n`);
}

if (process.argv.includes("--seeded-negative")) {
  await proveSeededNegatives();
} else {
  await provePositiveVectors();
  await scanPackages();
}
