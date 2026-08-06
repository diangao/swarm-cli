import { fail } from "./errors.js";

export type JsonObject = { [key: string]: JsonValue };
export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;

const MAX_PAYLOAD_BYTES = 65_536;
const MAX_DEPTH = 16;

function hasUnpairedSurrogate(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return true;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

class DuplicateAwareJsonReader {
  readonly #source: string;
  #offset = 0;

  constructor(source: string) {
    this.#source = source;
  }

  parseObjectRoot(): JsonObject {
    this.#skipWhitespace();
    if (this.#peek() !== "{") fail("INVALID_JSON");
    const value = this.#parseObject(1);
    this.#skipWhitespace();
    if (this.#offset !== this.#source.length) fail("INVALID_JSON");
    return value;
  }

  #parseValue(depth: number): JsonValue {
    const next = this.#peek();
    if (next === "{") return this.#parseObject(depth);
    if (next === "[") return this.#parseArray(depth);
    if (next === '"') return this.#parseString();
    if (next === "t") return this.#parseLiteral("true", true);
    if (next === "f") return this.#parseLiteral("false", false);
    if (next === "n") return this.#parseLiteral("null", null);
    if (next === "-" || (next >= "0" && next <= "9")) return this.#parseNumber();
    return fail("INVALID_JSON");
  }

  #parseObject(depth: number): JsonObject {
    if (depth > MAX_DEPTH) fail("INVALID_JSON");
    this.#consume("{");
    const value: JsonObject = Object.create(null) as JsonObject;
    const keys = new Set<string>();
    this.#skipWhitespace();
    if (this.#peek() === "}") {
      this.#offset += 1;
      return value;
    }
    while (true) {
      if (this.#peek() !== '"') fail("INVALID_JSON");
      const key = this.#parseString();
      if (keys.has(key)) fail("DUPLICATE_KEY");
      keys.add(key);
      this.#skipWhitespace();
      this.#consume(":");
      this.#skipWhitespace();
      value[key] = this.#parseValue(depth + 1);
      this.#skipWhitespace();
      const delimiter = this.#peek();
      if (delimiter === "}") {
        this.#offset += 1;
        return value;
      }
      this.#consume(",");
      this.#skipWhitespace();
    }
  }

  #parseArray(depth: number): JsonValue[] {
    if (depth > MAX_DEPTH) fail("INVALID_JSON");
    this.#consume("[");
    const value: JsonValue[] = [];
    this.#skipWhitespace();
    if (this.#peek() === "]") {
      this.#offset += 1;
      return value;
    }
    while (true) {
      value.push(this.#parseValue(depth + 1));
      this.#skipWhitespace();
      const delimiter = this.#peek();
      if (delimiter === "]") {
        this.#offset += 1;
        return value;
      }
      this.#consume(",");
      this.#skipWhitespace();
    }
  }

  #parseString(): string {
    const start = this.#offset;
    this.#offset += 1;
    let escaped = false;
    while (this.#offset < this.#source.length) {
      const code = this.#source.charCodeAt(this.#offset);
      if (!escaped && code === 0x22) {
        this.#offset += 1;
        let decoded: unknown;
        try {
          decoded = JSON.parse(this.#source.slice(start, this.#offset)) as unknown;
        } catch {
          return fail("INVALID_JSON");
        }
        if (typeof decoded !== "string" || hasUnpairedSurrogate(decoded)) {
          return fail("INVALID_JSON");
        }
        return decoded;
      }
      if (!escaped && code < 0x20) fail("INVALID_JSON");
      if (!escaped && code === 0x5c) {
        escaped = true;
        this.#offset += 1;
        continue;
      }
      escaped = false;
      this.#offset += 1;
    }
    return fail("INVALID_JSON");
  }

  #parseNumber(): number {
    const remainder = this.#source.slice(this.#offset);
    const match = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/u.exec(remainder);
    if (match === null) return fail("INVALID_JSON");
    this.#offset += match[0].length;
    const value = Number(match[0]);
    if (!Number.isFinite(value)) return fail("INVALID_JSON");
    if (!Number.isSafeInteger(value)) return fail("INVALID_SCALAR");
    return value;
  }

  #parseLiteral<T extends JsonValue>(token: string, value: T): T {
    if (!this.#source.startsWith(token, this.#offset)) return fail("INVALID_JSON");
    this.#offset += token.length;
    return value;
  }

  #consume(expected: string): void {
    if (this.#peek() !== expected) fail("INVALID_JSON");
    this.#offset += 1;
  }

  #peek(): string {
    return this.#source[this.#offset] ?? "";
  }

  #skipWhitespace(): void {
    while (true) {
      const next = this.#peek();
      if (next !== " " && next !== "\n" && next !== "\r" && next !== "\t") return;
      this.#offset += 1;
    }
  }
}

export function parseProtocolJson(input: Uint8Array): JsonObject {
  if (!(input instanceof Uint8Array)) fail("INVALID_SCALAR");
  if (input.byteLength > MAX_PAYLOAD_BYTES) fail("PAYLOAD_TOO_LARGE");
  if (input.byteLength >= 3 && input[0] === 0xef && input[1] === 0xbb && input[2] === 0xbf) {
    fail("INVALID_JSON");
  }
  let source: string;
  try {
    source = new TextDecoder("utf-8", { fatal: true }).decode(input);
  } catch {
    return fail("INVALID_JSON");
  }
  return new DuplicateAwareJsonReader(source).parseObjectRoot();
}

function canonicalize(value: unknown, ancestors: Set<object>): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") {
    if (hasUnpairedSurrogate(value)) fail("INVALID_SCALAR");
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) fail("INVALID_SCALAR");
    return value.toString(10);
  }
  if (typeof value !== "object" || value === undefined) fail("INVALID_SCALAR");
  if (ancestors.has(value)) fail("INVALID_SCALAR");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return `[${value.map((item) => canonicalize(item, ancestors)).join(",")}]`;
    }
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    const members = keys.map((key) => {
      const item = record[key];
      if (item === undefined) fail("INVALID_SCALAR");
      return `${JSON.stringify(key)}:${canonicalize(item, ancestors)}`;
    });
    return `{${members.join(",")}}`;
  } finally {
    ancestors.delete(value);
  }
}

export function canonicalProtocolJson(value: unknown): Uint8Array {
  return new TextEncoder().encode(canonicalize(value, new Set<object>()));
}
