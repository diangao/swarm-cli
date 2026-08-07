import { randomBytes } from "node:crypto";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface, type Interface } from "node:readline";
import { storageFail, StorageError } from "../errors.js";
import { connectionEnvironment } from "./psql.js";

type PendingFrame = {
  nonce: string;
  started: boolean;
  lines: string[];
  resolve: (value: string) => void;
  reject: (reason: unknown) => void;
  timeout: NodeJS.Timeout;
};

export function sqlLiteral(value: string | number | bigint | boolean | null): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) storageFail("INVALID_IDENTIFIER", value);
    return String(value);
  }
  if (typeof value === "bigint") return value.toString(10);
  if (value.includes("\0")) storageFail("INVALID_IDENTIFIER", "NUL byte");
  // psql meta-commands are line-oriented. Encoding every untrusted string as a
  // single-line hex expression prevents embedded newlines or backslashes from
  // ever becoming interactive client commands while keeping the SQL UTF-8.
  return `convert_from(decode('${Buffer.from(value, "utf8").toString("hex")}', 'hex'), 'UTF8')`;
}

export class PsqlSession {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #reader: Interface;
  readonly #stderr: string[] = [];
  #pending: PendingFrame | undefined;
  #tail: Promise<unknown> = Promise.resolve();
  #closed = false;

  private constructor(databaseUrl: string) {
    this.#child = spawn(
      "psql",
      [
        "--no-psqlrc",
        "--set=ON_ERROR_STOP=1",
        "--quiet",
        "--tuples-only",
        "--no-align",
      ],
      {
        env: connectionEnvironment(databaseUrl),
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    this.#reader = createInterface({ input: this.#child.stdout, crlfDelay: Infinity });
    this.#reader.on("line", (line) => this.#onLine(line));
    this.#child.stderr.setEncoding("utf8");
    this.#child.stderr.on("data", (chunk: string) => this.#stderr.push(chunk));
    this.#child.on("error", (error) => this.#fail(error));
    this.#child.on("close", (code) => {
      this.#closed = true;
      if (this.#pending !== undefined) {
        this.#fail(
          new StorageError("DATABASE_UNAVAILABLE", {
            code,
            stderr: this.#stderr.join("").trim(),
          }),
        );
      }
    });
  }

  static async open(databaseUrl: string): Promise<PsqlSession> {
    const session = new PsqlSession(databaseUrl);
    await session.execute("SELECT 1;");
    return session;
  }

  execute(sql: string, timeoutMs = 30_000): Promise<string> {
    const operation = this.#tail.then(() => this.#executeFrame(sql, timeoutMs));
    this.#tail = operation.catch(() => undefined);
    return operation;
  }

  async queryJson<T>(sql: string, timeoutMs = 30_000): Promise<T> {
    const output = await this.execute(sql, timeoutMs);
    const lines = output.split("\n").map((line) => line.trim()).filter(Boolean);
    const last = lines.at(-1);
    if (last === undefined) storageFail("MIGRATION_FAILED", "missing JSON result");
    try {
      return JSON.parse(last) as T;
    } catch (error) {
      return storageFail("MIGRATION_FAILED", { output, error });
    }
  }

  async rollbackAndClose(): Promise<void> {
    if (!this.#closed) {
      try {
        await this.execute("ROLLBACK;", 5_000);
      } catch {
        // A failed ON_ERROR_STOP session already rolls back on disconnect.
      }
    }
    await this.close();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    const closed = new Promise<void>((resolve) => this.#child.once("close", () => resolve()));
    this.#child.stdin.end("\\quit\n");
    const timer = setTimeout(() => this.#child.kill("SIGKILL"), 5_000);
    await closed;
    clearTimeout(timer);
    this.#reader.close();
    this.#closed = true;
  }

  #executeFrame(sql: string, timeoutMs: number): Promise<string> {
    if (this.#closed || this.#pending !== undefined) {
      return Promise.reject(new StorageError("DATABASE_UNAVAILABLE", "session unavailable"));
    }
    const nonce = randomBytes(16).toString("hex");
    const begin = `__swarm_${nonce}_begin__`;
    const end = `__swarm_${nonce}_end__`;
    return new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#fail(new StorageError("DATABASE_UNAVAILABLE", "psql command timeout"));
        this.#child.kill("SIGKILL");
      }, timeoutMs);
      this.#pending = { nonce, started: false, lines: [], resolve, reject, timeout };
      this.#child.stdin.write(`\\echo ${begin}\n${sql}\n\\echo ${end}\n`);
    });
  }

  #onLine(line: string): void {
    const pending = this.#pending;
    if (pending === undefined) return;
    const begin = `__swarm_${pending.nonce}_begin__`;
    const end = `__swarm_${pending.nonce}_end__`;
    if (!pending.started) {
      if (line === begin) pending.started = true;
      return;
    }
    if (line === end) {
      clearTimeout(pending.timeout);
      this.#pending = undefined;
      pending.resolve(pending.lines.join("\n").trim());
      return;
    }
    pending.lines.push(line);
  }

  #fail(error: unknown): void {
    const pending = this.#pending;
    if (pending === undefined) return;
    clearTimeout(pending.timeout);
    this.#pending = undefined;
    pending.reject(error);
  }
}
