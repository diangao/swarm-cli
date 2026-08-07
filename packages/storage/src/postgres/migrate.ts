import {
  locateMigrationDirectory,
  readMigrations,
  type MigrationReceipt,
} from "../migrations.js";
import { assertPostgresMigrationContract } from "../contracts.js";
import { storageFail } from "../errors.js";
import { parseSingleJson, runPsql } from "./psql.js";
import { PsqlSession, sqlLiteral } from "./session.js";

const IDENTIFIER = /^[a-z][a-z0-9_]{0,62}$/u;

type AppliedMigration = { version: string; checksum: string };

export class PostgresMigrator {
  readonly #databaseUrl: string;
  readonly #schema: string;

  constructor(databaseUrl: string, schema = "swarm_storage") {
    if (!IDENTIFIER.test(schema)) storageFail("INVALID_IDENTIFIER", schema);
    this.#databaseUrl = databaseUrl;
    this.#schema = schema;
  }

  async assertGate0Version(): Promise<void> {
    const result = parseSingleJson<{ serverVersionNum: number }>(
      await runPsql({
        databaseUrl: this.#databaseUrl,
        sql: "SELECT json_build_object('serverVersionNum', current_setting('server_version_num')::integer);\n",
      }),
    );
    if (result.serverVersionNum < 160000 || result.serverVersionNum >= 170000) {
      storageFail("DATABASE_VERSION_UNSUPPORTED", result.serverVersionNum);
    }
  }

  async migrate(): Promise<MigrationReceipt[]> {
    await this.assertGate0Version();
    const migrations = await readMigrations(locateMigrationDirectory("postgres", import.meta.url));
    migrations.forEach((migration) => assertPostgresMigrationContract(migration.sql));
    const session = await PsqlSession.open(this.#databaseUrl);
    const receipts: MigrationReceipt[] = [];
    try {
      await session.execute(
        `SELECT pg_advisory_lock(hashtextextended('storage_migration:' || ${sqlLiteral(this.#schema)}, 0));
         CREATE SCHEMA IF NOT EXISTS ${this.#schema};
         CREATE TABLE IF NOT EXISTS ${this.#schema}.schema_migrations (
           version text PRIMARY KEY,
           checksum text NOT NULL CHECK (checksum COLLATE "C" ~ '^[0-9a-f]{64}$'),
           applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
         );
         SET search_path TO ${this.#schema}, pg_catalog;`,
      );
      const applied = await session.queryJson<AppliedMigration[]>(
        "SELECT coalesce(json_agg(row_to_json(m) ORDER BY version), '[]'::json) FROM schema_migrations AS m;",
      );
      const byVersion = new Map(applied.map((entry) => [entry.version, entry.checksum]));
      for (const migration of migrations) {
        const existing = byVersion.get(migration.version);
        if (existing !== undefined) {
          if (existing !== migration.checksum) {
            storageFail("MIGRATION_CHECKSUM_MISMATCH", migration.version);
          }
          receipts.push({ version: migration.version, checksum: migration.checksum, applied: false });
          continue;
        }
        await session.execute(
          `BEGIN;
           SET LOCAL search_path TO ${this.#schema}, pg_catalog;
           ${migration.sql}
           INSERT INTO schema_migrations(version, checksum)
             VALUES (${sqlLiteral(migration.version)}, ${sqlLiteral(migration.checksum)});
           COMMIT;`,
          60_000,
        );
        receipts.push({ version: migration.version, checksum: migration.checksum, applied: true });
      }
      await session.execute(
        `SELECT pg_advisory_unlock(hashtextextended('storage_migration:' || ${sqlLiteral(this.#schema)}, 0));`,
      );
      await session.close();
      return receipts;
    } catch (error) {
      await session.rollbackAndClose();
      throw error;
    }
  }

  async resetForTests(): Promise<void> {
    const databaseName = parseSingleJson<{ databaseName: string }>(
      await runPsql({
        databaseUrl: this.#databaseUrl,
        sql: "SELECT json_build_object('databaseName', current_database());\n",
      }),
    ).databaseName;
    if (process.env.NODE_ENV !== "test" || !/^swarm_storage_test_[a-z0-9_]+$/u.test(databaseName)) {
      storageFail("INVALID_DATABASE_TARGET", databaseName);
    }
    await runPsql({
      databaseUrl: this.#databaseUrl,
      variables: { schema: this.#schema },
      sql: "DROP SCHEMA IF EXISTS :\"schema\" CASCADE;\n",
    });
    await this.migrate();
  }
}
