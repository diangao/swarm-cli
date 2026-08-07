import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

// ---------------------------------------------------------------------------
// Committed neutrality regression scan (bounded package scope).
//
// Guards the verifier lane's public surfaces against internal coordination
// references leaking back in. It checks two dimensions:
//
//   1. STRUCTURAL forms: mention handles, "task #N" / "gap-diff #N" reference
//      numbers, "Finding #N" review-finding labels, and bare 8-hex
//      receipt/message short-id anchors.
//   2. INTERNAL-LINEAGE terms: any 1-4 word n-gram whose SHA-256 is in the
//      promoted publication denylist. This catches named actors, platform
//      terms, and lineage words WITHOUT this scanner embedding any of them as
//      plaintext (which would itself be a leak, and would trip the publication
//      gate on this file).
//
// The publication gate permits some of these shapes and this scan is stricter;
// both are prove-it-can-fail below. Scope: packages/{testkit,verifiers} src+test
// and contracts/scenarios. This scanner file excludes itself from the file
// walk; its structural pattern literals are not violations.
// ---------------------------------------------------------------------------

const thisDir = dirname(fileURLToPath(import.meta.url));

function findRepoRoot(start: string): string {
  let dir = start;
  for (let hop = 0; hop < 12; hop += 1) {
    if (existsSync(join(dir, "scripts", "publication_gate.py"))) return dir;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(`could not locate repo root from ${start}`);
}

const repoRoot = findRepoRoot(thisDir);
const SELF = "neutrality-scan.test.ts";

const SCAN_DIRS = [
  "packages/testkit/src",
  "packages/testkit/test",
  "packages/verifiers/src",
  "packages/verifiers/test",
  "contracts/scenarios",
];

function loadDenylist(): ReadonlySet<string> {
  const p = join(repoRoot, ".github", "publication-denylist.sha256");
  if (!existsSync(p)) return new Set();
  return new Set(readFileSync(p, "utf8").split(/\s+/u).filter(Boolean));
}

const WORD_RE = /[A-Za-z0-9]+/gu;
function ngramDigests(text: string, maxWords = 4): Set<string> {
  const words = (text.toLowerCase().match(WORD_RE) ?? []) as string[];
  const out = new Set<string>();
  for (let size = 1; size <= Math.min(maxWords, words.length); size += 1) {
    for (let start = 0; start + size <= words.length; start += 1) {
      const value = words.slice(start, start + size).join(" ");
      out.add(createHash("sha256").update(value).digest("hex"));
    }
  }
  return out;
}

// JSDoc tags are not mention handles.
const JSDOC_TAGS = new Set([
  "param", "returns", "return", "see", "example", "throws", "template",
  "typeParam", "remarks", "internal", "public", "deprecated", "link",
  "packageDocumentation", "defaultValue", "typedef", "property",
]);

type Violation = { file: string; line: number; kind: string; text: string };

// Detect structural + denylisted-lineage leaks in one text blob. Exported shape
// for the prove-it-can-fail controls below (a synthetic denylist can be passed).
function scanText(
  rel: string,
  text: string,
  denylist: ReadonlySet<string>,
): Violation[] {
  const violations: Violation[] = [];
  const push = (line: number, kind: string, tx: string): void => {
    violations.push({ file: rel, line, kind, text: tx });
  };
  text.split("\n").forEach((line, i) => {
    const ln = i + 1;
    if (/\btask\s+#\d+/iu.test(line)) push(ln, "task #N", line.trim());
    if (/\bgap-diff\s+#\d+/iu.test(line)) push(ln, "gap-diff #N", line.trim());
    // Review-finding labels: the word followed by a number across any separator
    // (space, hyphen, underscore, or an optional hash), matched case-insensitively.
    if (/\bfinding[\s#_-]*\d+/iu.test(line)) push(ln, "review-finding", line.trim());
    for (const h of line.match(/\b[0-9a-f]{8}\b/gu) ?? []) {
      push(ln, "8-hex anchor", h);
    }
    for (const h of line.match(/@[A-Za-z][A-Za-z0-9_-]+/gu) ?? []) {
      if (h.startsWith("@swarm")) continue;
      if (JSDOC_TAGS.has(h.slice(1))) continue;
      push(ln, "handle", h);
    }
    // Internal-lineage terms via the promoted denylist (named actors, platform
    // and lineage words), matched by hash so none are embedded here.
    if (denylist.size > 0) {
      const digests = ngramDigests(line);
      for (const d of digests) {
        if (denylist.has(d)) {
          push(ln, "internal-lineage", "(denylisted n-gram)");
          break;
        }
      }
    }
  });
  return violations;
}

function collectFiles(absDir: string): string[] {
  if (!existsSync(absDir)) return [];
  const out: string[] = [];
  for (const name of readdirSync(absDir)) {
    const p = join(absDir, name);
    if (statSync(p).isDirectory()) {
      out.push(...collectFiles(p));
      continue;
    }
    if (/\.(ts|json)$/u.test(name) && name !== SELF) out.push(p);
  }
  return out;
}

test("package surfaces carry no handles / task-#N / gap-diff-#N / Finding-#N / 8-hex / internal-lineage terms", () => {
  const denylist = loadDenylist();
  const files = SCAN_DIRS.flatMap((d) => collectFiles(join(repoRoot, d)));
  assert.ok(files.length > 0, "scan found no files (path wrong)");
  const violations = files.flatMap((f) =>
    scanText(f.slice(repoRoot.length + 1), readFileSync(f, "utf8"), denylist),
  );
  assert.equal(
    violations.length,
    0,
    `neutrality violations:\n${violations
      .map((v) => `  ${v.file}:${v.line} [${v.kind}] ${v.text}`)
      .join("\n")}`,
  );
});

test("scanner PROVE-IT-CAN-FAIL: every structural leak class is detected", () => {
  // Built by concatenation so this file's own source stays neutral.
  const handle = "@" + "some-external-mention";
  const taskRef = "task " + "#7";
  const gapRef = "gap-diff " + "#3";
  const findingRef = "Finding " + "9";
  const hexAnchor = "0a1b" + "2c3d"; // synthetic 8-hex, not a real anchor
  const seeded = `${handle} ${taskRef} ${gapRef} ${findingRef} ${hexAnchor}`;
  const kinds = new Set(scanText("synthetic", seeded, new Set()).map((v) => v.kind));
  for (const expected of ["handle", "task #N", "gap-diff #N", "review-finding", "8-hex anchor"]) {
    assert.ok(kinds.has(expected), `scanner must detect ${expected}`);
  }
  // Review-finding must be caught across separators, not only whitespace: a
  // hyphenated review label is the exact miss a whitespace-only detector let
  // through. Built by concatenation so this source stays neutral.
  for (const variant of ["Find" + "ing-2", "Find" + "ing_3", "Find" + "ing #4", "Find" + "ing5"]) {
    assert.ok(
      scanText("synthetic", variant, new Set()).some((v) => v.kind === "review-finding"),
      `review-finding must catch separator variant "${variant}"`,
    );
  }
});

test("scanner PROVE-IT-CAN-FAIL: an internal-lineage (denylisted) term is detected", () => {
  // A synthetic token hashed into a test-only denylist proves the lineage
  // branch discriminates, without embedding any real denylisted term.
  // Single lowercase word so it tokenizes to itself (the n-gram unit).
  const token = "zzsyntheticlineagetokenzz";
  const deny = new Set([createHash("sha256").update(token).digest("hex")]);
  const found = scanText("synthetic", `the ${token} appears here`, deny);
  assert.ok(
    found.some((v) => v.kind === "internal-lineage"),
    "scanner must detect a denylisted-lineage n-gram",
  );
});

test("scanner FALSE-POSITIVE resistance: neutral labels / hashes / ids PASS", () => {
  const denylist = loadDenylist();
  const neutral = [
    "owner worker-1 worker-2 verifier server agent",
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "agt_2hfcr767452zzwxtqrnjkghebc tsk_3hfcr767452zzwxtqrnjkghebc",
    "capability-A capability-B routed by capability",
  ].join("\n");
  assert.equal(
    scanText("neutral", neutral, denylist).length,
    0,
    "neutral role labels, hashes, and grammar-valid ids must not be flagged",
  );
});
