import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";

export type Migration = {
  version: string;
  checksum: string;
  sql: string;
};

export type MigrationReceipt = {
  version: string;
  checksum: string;
  applied: boolean;
};

export function checksumMigration(sql: string): string {
  return createHash("sha256").update(sql, "utf8").digest("hex");
}

export async function readMigrations(directory: URL): Promise<Migration[]> {
  const names = (await readdir(directory))
    .filter((name) => /^\d{4}_[a-z0-9_]+\.up\.sql$/u.test(name))
    .sort();
  const migrations: Migration[] = [];
  for (const name of names) {
    const sql = await readFile(new URL(name, directory), "utf8");
    migrations.push({
      version: name.slice(0, 4),
      checksum: checksumMigration(sql),
      sql,
    });
  }
  return migrations;
}

export function locateMigrationDirectory(
  dialect: "postgres" | "sqlite",
  moduleUrl: string,
): URL {
  const candidates = [
    new URL(`../../migrations/${dialect}/`, moduleUrl),
    new URL(`../../../migrations/${dialect}/`, moduleUrl),
  ];
  const found = candidates.find((candidate) => existsSync(candidate));
  if (found === undefined) throw new Error(`missing ${dialect} migration directory`);
  return found;
}
