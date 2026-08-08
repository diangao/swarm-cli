/**
 * Frozen ECMAScript WhiteSpace and LineTerminator code points.
 * Keep the generated Gate 1 table and PostgreSQL fixture bound to this list.
 */
export const MESSAGE_BODY_WHITESPACE_CODE_POINTS = Object.freeze([
  0x0009, 0x000a, 0x000b, 0x000c, 0x000d, 0x0020, 0x00a0, 0x1680,
  0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007,
  0x2008, 0x2009, 0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000,
  0xfeff,
] as const);

const BODY_WHITESPACE = new Set<number>(MESSAGE_BODY_WHITESPACE_CODE_POINTS);

export function messageBodyHasContent(value: string): boolean {
  for (const scalar of value) {
    const point = scalar.codePointAt(0);
    if (point !== undefined && !BODY_WHITESPACE.has(point)) return true;
  }
  return false;
}

export const MESSAGE_BODY_CONTENT_FIXTURES = Object.freeze([
  Object.freeze({ name: "empty", body: "", hasContent: false }),
  Object.freeze({ name: "ascii-space", body: " \t\r\n", hasContent: false }),
  Object.freeze({ name: "unicode-space", body: "\u00a0\u1680\u2007\u2028\u2029\u202f\u205f\u3000\ufeff", hasContent: false }),
  Object.freeze({ name: "zero-width-space", body: "\u200b", hasContent: true }),
  Object.freeze({ name: "non-ascii", body: "你好", hasContent: true }),
  Object.freeze({ name: "preserve-surrounding", body: " \nélan\t", hasContent: true }),
] as const);
