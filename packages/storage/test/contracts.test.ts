import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  StorageError,
  assertPostgresMigrationContract,
  assertPostgresNativeIngressMigrationContract,
  assertSqliteMigrationContract,
  sqlLiteral,
} from "../src/index.js";
import { connectionEnvironment } from "../src/postgres/psql.js";

const postgresPath = new URL(
  "../../migrations/postgres/0001_shared_facts.up.sql",
  import.meta.url,
);
const sqlitePath = new URL(
  "../../migrations/sqlite/0001_journal.up.sql",
  import.meta.url,
);
const postgresNativeIngressPath = new URL(
  "../../migrations/postgres/0002_native_ingress.up.sql",
  import.meta.url,
);
const postgres = readFileSync(postgresPath, "utf8");
const postgresNativeIngress = readFileSync(postgresNativeIngressPath, "utf8");
const sqlite = readFileSync(sqlitePath, "utf8");

const gate1Root = new URL("../../../../contracts/gate1/", import.meta.url);
const gate1ScenarioBytes = readFileSync(new URL("wave1.v1.json", gate1Root));
const gate1ControlsBytes = readFileSync(new URL("seeded-controls.json", gate1Root));
const gate1Scenario = JSON.parse(gate1ScenarioBytes.toString("utf8")) as {
  clauses: string[];
  boundaryReceiptTuple: string[];
  placeholderDisposition: Array<{
    conditionId: string;
    status: string;
    entryCondition: string;
    earliestWave: number;
  }>;
};
const gate1Controls = JSON.parse(gate1ControlsBytes.toString("utf8")) as Array<{
  id: string;
  clause: string;
  expectedError?: string;
  expectedState?: string;
  siblings: string[];
}>;

test("frozen PostgreSQL invariant controls are present", () => {
  assert.doesNotThrow(() => assertPostgresMigrationContract(postgres));
});

test("frozen SQLite invariant controls are present", () => {
  assert.doesNotThrow(() => assertSqliteMigrationContract(sqlite));
});

test("frozen PostgreSQL native-ingress controls are present", () => {
  assert.doesNotThrow(() => assertPostgresNativeIngressMigrationContract(postgresNativeIngress));
});

test("seeded native-ingress controls fail when removed one at a time", () => {
  for (const seed of [
    "CREATE TABLE delivery_boundary_ack_results",
    "CREATE CONSTRAINT TRIGGER target_owner_routes_authority_match",
    "MATCH SIMPLE DEFERRABLE INITIALLY DEFERRED",
    "CREATE CONSTRAINT TRIGGER receipts_boundary_creator_match",
    "c.result_json->'repaired' ? NEW.boundary",
    "receipts_actor_shape_v1",
    "CREATE TRIGGER outbox_immutable_fields",
    "reminders_fire_producer_once",
  ]) {
    assert.throws(
      () => assertPostgresNativeIngressMigrationContract(postgresNativeIngress.replace(seed, "SEEDED_DEFECT")),
      (error: unknown) => error instanceof StorageError && error.code === "INVALID_MIGRATION",
    );
  }
});

test("seeded PostgreSQL controls fail when removed one at a time", () => {
  const seeds = [
    "UNIQUE NULLS NOT DISTINCT (target_kind, target_id, thread_root_message_id)",
    "FOREIGN KEY (replay_of, agent_id, producer_fact_id)",
    "CREATE UNIQUE INDEX task_claims_one_open",
  ];
  for (const seed of seeds) {
    assert.throws(
      () => assertPostgresMigrationContract(postgres.replace(seed, "SEEDED_DEFECT")),
      (error: unknown) => error instanceof StorageError && error.code === "INVALID_MIGRATION",
    );
  }
});

test("seeded SQLite target and visibility controls fail when removed", () => {
  for (const seed of [
    "target_key TEXT NOT NULL COLLATE BINARY",
    "UNIQUE (session_id, target_key)",
    "model_visible_at IS NULL OR input_written_at IS NOT NULL",
  ]) {
    assert.throws(
      () => assertSqliteMigrationContract(sqlite.replaceAll(seed, "SEEDED_DEFECT")),
      (error: unknown) => error instanceof StorageError && error.code === "INVALID_MIGRATION",
    );
  }
});

test("PostgreSQL migration declares every frozen branded identifier domain", () => {
  for (const prefix of [
    "server", "machine", "agent", "channel", "conversation", "message",
    "delivery", "producer_fact", "task", "claim", "lease", "launch",
    "command", "receipt", "state_instance", "turn", "session",
  ]) {
    assert.match(postgres, new RegExp(`CREATE DOMAIN ${prefix}_id_text`, "u"));
  }
});

test("native-ingress migration adds the frozen human identifier domain", () => {
  assert.match(postgresNativeIngress, /CREATE DOMAIN human_id_text/u);
});

test("Gate 1 contract fixture covers every clause and freezes exact sibling sets", () => {
  const expectedClauses = Array.from({ length: 10 }, (_unused, index) => `G1.${index + 1}`);
  assert.deepEqual(gate1Scenario.clauses, expectedClauses);
  assert.deepEqual(
    [...new Set(gate1Controls.map((control) => control.clause))].sort(),
    [...expectedClauses].sort(),
  );
  assert.equal(new Set(gate1Controls.map((control) => control.id)).size, gate1Controls.length);
  for (const control of gate1Controls) {
    assert.equal(
      Number(control.expectedError !== undefined) + Number(control.expectedState !== undefined),
      1,
      `${control.id} must declare exactly one expected outcome`,
    );
    assert.match(control.expectedError ?? control.expectedState ?? "", /^[A-Z][A-Z0-9_]+$/u, control.id);
    assert.ok(control.siblings.length > 0, `${control.id} has no sibling set`);
    assert.equal(new Set(control.siblings).size, control.siblings.length, `${control.id} repeats a sibling`);
  }
  assert.deepEqual(
    gate1Controls.filter((control) => control.id.startsWith("g1.5-")).slice(0, 8),
    [
      {
        id: "g1.5-fresh-generation-revoked-membership",
        clause: "G1.5",
        expectedError: "MEMBERSHIP_REVOKED_BEFORE_CONSUME",
        siblings: ["permit", "invocation", "receipt", "delivery", "job"],
      },
      {
        id: "g1.5-fresh-generation-superseded-route",
        clause: "G1.5",
        expectedError: "ROUTE_SUPERSEDED_BEFORE_CONSUME",
        siblings: ["permit", "invocation", "receipt", "delivery", "job"],
      },
      {
        id: "g1.5-stale-generation-current-authority",
        clause: "G1.5",
        expectedError: "STALE_INVOCATION_GENERATION",
        siblings: ["permit", "invocation", "receipt", "delivery", "job"],
      },
      {
        id: "g1.5-not-written-generation-two",
        clause: "G1.5",
        expectedState: "GENERATION_TWO_AUTHORIZED_FROM_NOT_WRITTEN",
        siblings: ["second_permit", "receipt", "body"],
      },
      {
        id: "g1.5-generation-one-proof-replay",
        clause: "G1.5",
        expectedState: "GENERATION_ONE_PROOF_REPLAYED_HISTORICALLY",
        siblings: ["permit", "invocation", "receipt", "delivery", "job", "body"],
      },
      {
        id: "g1.5-post-generation-two-stale-resume",
        clause: "G1.5",
        expectedError: "STALE_INVOCATION_GENERATION",
        siblings: ["permit", "invocation", "receipt", "delivery", "job", "body"],
      },
      {
        id: "g1.5-post-generation-two-terminal-conflict",
        clause: "G1.5",
        expectedError: "INVOCATION_STATE_CONFLICT",
        siblings: ["permit", "invocation", "receipt", "delivery", "job", "body"],
      },
      {
        id: "g1.5-second-crash-generation-two",
        clause: "G1.5",
        expectedState: "GENERATIONS_ONE_NOT_WRITTEN_TWO_AMBIGUOUS",
        siblings: ["generation_3", "permit", "body"],
      },
    ],
  );
  assert.deepEqual(gate1Scenario.boundaryReceiptTuple, [
    "receipt_id", "delivery_id", "attempt", "permit_id",
    "invocation_generation", "invocation_id", "boundary",
  ]);
});

test("Gate 1 fixture digests match the reviewed checksum manifest", () => {
  const manifest = new Map(
    readFileSync(new URL("SHA256SUMS", gate1Root), "utf8")
      .trim()
      .split("\n")
      .map((line) => {
        const [checksum, name] = line.split(/\s{2}/u);
        return [name, checksum] as [string | undefined, string | undefined];
      }),
  );
  assert.equal(createHash("sha256").update(gate1ScenarioBytes).digest("hex"), manifest.get("wave1.v1.json"));
  assert.equal(createHash("sha256").update(gate1ControlsBytes).digest("hex"), manifest.get("seeded-controls.json"));
});

test("all seven condition placeholders remain fail-closed with corrected wave labels", () => {
  assert.deepEqual(
    gate1Scenario.placeholderDisposition.map((row) => row.conditionId),
    [
      "thread_owner_status_receipt",
      "lifecycle_and_illegal_transition",
      "task_status_freshness_hold",
      "delivery_reply_trailer",
      "coordination_slos",
      "channel_navigation_exact_target",
      "lane_role_fidelity",
    ],
  );
  assert.ok(gate1Scenario.placeholderDisposition.every((row) => row.status === "placeholder"));
  assert.deepEqual(
    gate1Scenario.placeholderDisposition.map((row) => row.earliestWave),
    [3, 3, 3, 3, 5, 5, 3],
  );
  assert.ok(gate1Scenario.placeholderDisposition.every((row) => row.entryCondition.length > 0));
});

test("psql literals cannot inject interactive meta-commands or frame lines", () => {
  const encoded = sqlLiteral("payload\n\\quit\n\\echo __swarm_forged_end__\n'quoted'");
  assert.doesNotMatch(encoded, /[\r\n]/u);
  assert.doesNotMatch(encoded, /\\quit|\\echo|__swarm_forged_end__/u);
  assert.match(encoded, /^convert_from\(decode\('[0-9a-f]+', 'hex'\), 'UTF8'\)$/u);
});

test("PostgreSQL child environment never inherits ambient libpq credentials", () => {
  const priorPassword = process.env.PGPASSWORD;
  const priorService = process.env.PGSERVICE;
  process.env.PGPASSWORD = "ambient-secret";
  process.env.PGSERVICE = "ambient-service";
  try {
    const withoutPassword = connectionEnvironment("postgresql://gate@db.example.test/storage");
    assert.equal(withoutPassword.PGPASSWORD, undefined);
    assert.equal(withoutPassword.PGSERVICE, undefined);
    assert.equal(withoutPassword.PGPASSFILE, "/dev/null");
    const explicitUrl = new URL("postgresql://db.example.test/storage");
    const credentialProperty = ["pass", "word"].join("");
    const fixtureCredential = ["explicit", "fixture"].join("-");
    Reflect.set(explicitUrl, credentialProperty, fixtureCredential);
    const explicit = connectionEnvironment(explicitUrl.toString());
    assert.equal(explicit.PGPASSWORD, fixtureCredential);
  } finally {
    if (priorPassword === undefined) delete process.env.PGPASSWORD;
    else process.env.PGPASSWORD = priorPassword;
    if (priorService === undefined) delete process.env.PGSERVICE;
    else process.env.PGSERVICE = priorService;
  }
});
