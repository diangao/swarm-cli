import { Gate3PlanError } from "./types.js";
import type { Gate3Json } from "./types.js";

class StrictJsonParser {
  readonly #source: string;
  #offset = 0;

  public constructor(source: string) {
    this.#source = source;
  }

  public parse(): Gate3Json {
    this.#space();
    const value = this.#value();
    this.#space();
    if (this.#offset !== this.#source.length) this.#fail("trailing input");
    return value;
  }

  #value(): Gate3Json {
    const token = this.#source[this.#offset];
    if (token === "{") return this.#object();
    if (token === "[") return this.#array();
    if (token === '"') return this.#string();
    if (token === "t") return this.#literal("true", true);
    if (token === "f") return this.#literal("false", false);
    if (token === "n") return this.#literal("null", null);
    if (token === "-" || (token !== undefined && /[0-9]/u.test(token))) {
      return this.#number();
    }
    this.#fail("expected JSON value");
  }

  #object(): Gate3Json {
    this.#offset += 1;
    this.#space();
    const result: Record<string, Gate3Json> = {};
    const keys = new Set<string>();
    if (this.#source[this.#offset] === "}") {
      this.#offset += 1;
      return result;
    }
    for (;;) {
      if (this.#source[this.#offset] !== '"') this.#fail("expected object key");
      const key = this.#string();
      if (keys.has(key)) this.#fail(`duplicate object key ${key}`);
      keys.add(key);
      this.#space();
      if (this.#source[this.#offset] !== ":") this.#fail("expected colon");
      this.#offset += 1;
      this.#space();
      result[key] = this.#value();
      this.#space();
      const separator = this.#source[this.#offset];
      if (separator === "}") {
        this.#offset += 1;
        return result;
      }
      if (separator !== ",") this.#fail("expected comma or object close");
      this.#offset += 1;
      this.#space();
    }
  }

  #array(): Gate3Json {
    this.#offset += 1;
    this.#space();
    const result: Gate3Json[] = [];
    if (this.#source[this.#offset] === "]") {
      this.#offset += 1;
      return result;
    }
    for (;;) {
      result.push(this.#value());
      this.#space();
      const separator = this.#source[this.#offset];
      if (separator === "]") {
        this.#offset += 1;
        return result;
      }
      if (separator !== ",") this.#fail("expected comma or array close");
      this.#offset += 1;
      this.#space();
    }
  }

  #string(): string {
    const start = this.#offset;
    this.#offset += 1;
    let escaped = false;
    for (; this.#offset < this.#source.length; this.#offset += 1) {
      const character = this.#source[this.#offset]!;
      if (!escaped && character === '"') {
        this.#offset += 1;
        const encoded = this.#source.slice(start, this.#offset);
        try {
          return JSON.parse(encoded) as string;
        } catch {
          this.#fail("invalid string");
        }
      }
      if (!escaped && character.charCodeAt(0) < 0x20) {
        this.#fail("unescaped control character");
      }
      if (!escaped && character === "\\") {
        escaped = true;
      } else {
        escaped = false;
      }
    }
    this.#fail("unterminated string");
  }

  #number(): number {
    const rest = this.#source.slice(this.#offset);
    const match = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/u.exec(rest);
    if (match === null) this.#fail("invalid number");
    this.#offset += match[0].length;
    const value = Number(match[0]);
    if (!Number.isFinite(value)) this.#fail("non-finite number");
    return value;
  }

  #literal<T extends true | false | null>(token: string, value: T): T {
    if (!this.#source.startsWith(token, this.#offset)) this.#fail(`invalid ${token}`);
    this.#offset += token.length;
    return value;
  }

  #space(): void {
    while (/\s/u.test(this.#source[this.#offset] ?? "")) this.#offset += 1;
  }

  #fail(detail: string): never {
    throw new Gate3PlanError("GATE3_PLAN_JSON_INVALID", `${detail} at byte ${this.#offset}`);
  }
}

export function parseStrictJson(source: string): Gate3Json {
  return new StrictJsonParser(source).parse();
}

export function exactKeys(
  value: Readonly<Record<string, unknown>>,
  required: readonly string[],
  optional: readonly string[] = [],
  label = "object",
): void {
  const allowed = new Set([...required, ...optional]);
  const actual = Object.keys(value);
  for (const key of required) {
    if (!Object.hasOwn(value, key)) {
      throw new Gate3PlanError("GATE3_PLAN_SCHEMA_INVALID", `${label} missing ${key}`);
    }
  }
  for (const key of actual) {
    if (!allowed.has(key)) {
      throw new Gate3PlanError("GATE3_PLAN_UNKNOWN_KEY", `${label}.${key}`);
    }
  }
}

export function isRecord(value: unknown): value is Record<string, Gate3Json> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
