import { spawn } from "node:child_process";
import { once } from "node:events";
import { storageFail } from "../errors.js";

export type SqlVariables = Readonly<Record<string, string | number>>;

export type PsqlOptions = {
  databaseUrl: string;
  sql: string;
  variables?: SqlVariables;
};

export function connectionEnvironment(databaseUrl: string): NodeJS.ProcessEnv {
  let parsed: URL;
  try {
    parsed = new URL(databaseUrl);
  } catch (error) {
    return storageFail("INVALID_DATABASE_TARGET", error);
  }
  if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
    return storageFail("INVALID_DATABASE_TARGET");
  }
  if (parsed.hostname.length === 0 || parsed.pathname.length <= 1) {
    return storageFail("INVALID_DATABASE_TARGET");
  }
  const environment: NodeJS.ProcessEnv = Object.fromEntries(
    Object.entries(process.env).filter(([name]) => !name.startsWith("PG") && name !== "PSQLRC"),
  );
  Object.assign(environment, {
    PGHOST: decodeURIComponent(parsed.hostname),
    PGPORT: parsed.port || "5432",
    PGDATABASE: decodeURIComponent(parsed.pathname.slice(1)),
    PGAPPNAME: "swarm-storage-gate0",
    PGCONNECT_TIMEOUT: "5",
    PGPASSFILE: "/dev/null",
    PSQLRC: "/dev/null",
  });
  if (parsed.username.length > 0) environment.PGUSER = decodeURIComponent(parsed.username);
  if (parsed.password.length > 0) environment.PGPASSWORD = decodeURIComponent(parsed.password);
  const sslmode = parsed.searchParams.get("sslmode");
  if (sslmode !== null) environment.PGSSLMODE = sslmode;
  return environment;
}

export async function runPsql(options: PsqlOptions): Promise<string> {
  const args = [
    "--no-psqlrc",
    "--set=ON_ERROR_STOP=1",
    "--quiet",
    "--tuples-only",
    "--no-align",
  ];
  for (const [name, value] of Object.entries(options.variables ?? {})) {
    if (!/^[a-z][a-z0-9_]*$/u.test(name)) return storageFail("INVALID_IDENTIFIER", name);
    args.push(`--set=${name}=${String(value)}`);
  }

  let child;
  try {
    child = spawn("psql", args, {
      env: connectionEnvironment(options.databaseUrl),
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch (error) {
    return storageFail("DATABASE_UNAVAILABLE", error);
  }

  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
  child.stdin.end(options.sql);
  let code: number | null;
  try {
    [code] = (await once(child, "close")) as [number | null];
  } catch (error) {
    return storageFail("DATABASE_UNAVAILABLE", error);
  }
  if (code !== 0) {
    return storageFail("MIGRATION_FAILED", Buffer.concat(stderr).toString("utf8").trim());
  }
  return Buffer.concat(stdout).toString("utf8").trim();
}

export function parseSingleJson<T>(output: string): T {
  const lines = output.split("\n").map((line) => line.trim()).filter(Boolean);
  const last = lines.at(-1);
  if (last === undefined) return storageFail("MIGRATION_FAILED", "missing JSON result");
  try {
    return JSON.parse(last) as T;
  } catch (error) {
    return storageFail("MIGRATION_FAILED", { output, error });
  }
}
