import { readFile, readdir } from "node:fs/promises";
import { extname, join, relative } from "node:path";
import process from "node:process";

const root = new URL("../", import.meta.url);
const sourceRoot = new URL("../packages/protocol/src/", import.meta.url);
const positiveFixture = new URL(
  "../contracts/protocol/boundary-positive.ts",
  import.meta.url,
);
const negativeFixture = new URL(
  "../contracts/protocol/boundary-negative.seed.ts",
  import.meta.url,
);

const forbidden = [
  /^node:(?:child_process|cluster|fs|net|process|worker_threads)(?:\/|$)/u,
  /^@swarm\/(?:application|daemon|filesystem|persistence|process|runtime|runtime-driver|storage)(?:\/|$)/u,
  /(?:^|\/)(?:application|daemon|filesystem|persistence|process|runtime|runtime-driver|storage)(?:\/|$)/u,
];

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
  const pattern = /(?:import|export)\s+(?:type\s+)?(?:[^"']*?\s+from\s+)?["']([^"']+)["']|import\s*\(\s*["']([^"']+)["']\s*\)/gu;
  for (const match of source.matchAll(pattern)) found.push(match[1] ?? match[2]);
  return found.filter((specifier) => specifier !== undefined);
}

function violations(path, source) {
  return imports(source)
    .filter((specifier) => forbidden.some((rule) => rule.test(specifier)))
    .map((specifier) => `${relative(root.pathname, path)} imports ${specifier}`);
}

if (process.argv.includes("--seeded-negative")) {
  const source = await readFile(negativeFixture, "utf8");
  const found = violations(negativeFixture.pathname, source);
  if (found.length !== 2) {
    throw new Error(`seeded boundary fixture expected 2 violations, found ${found.length}`);
  }
  process.stdout.write("seeded package-boundary violations detected\n");
  process.exit(0);
}

const files = await sourceFiles(sourceRoot.pathname);
const found = [];
for (const path of files) found.push(...violations(path, await readFile(path, "utf8")));
if (found.length > 0) throw new Error(found.join("\n"));

const positive = await readFile(positiveFixture, "utf8");
if (!imports(positive).includes("@swarm/protocol")) {
  throw new Error("positive boundary fixture must import @swarm/protocol");
}

process.stdout.write(`protocol package boundary clean (${files.length} source files)\n`);
