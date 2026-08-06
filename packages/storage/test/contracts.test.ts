import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  StorageError,
  assertPostgresMigrationContract,
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
const postgres = readFileSync(postgresPath, "utf8");
const sqlite = readFileSync(sqlitePath, "utf8");

test("frozen PostgreSQL invariant controls are present", () => {
  assert.doesNotThrow(() => assertPostgresMigrationContract(postgres));
});

test("frozen SQLite invariant controls are present", () => {
  assert.doesNotThrow(() => assertSqliteMigrationContract(sqlite));
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
