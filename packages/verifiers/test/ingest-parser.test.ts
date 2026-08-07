import assert from "node:assert/strict";
import { test } from "node:test";

import {
  LedgerIngestError,
  parseBodyDurability,
  parseDeliveryObservation,
} from "../src/index.js";
import {
  mintDeliveryId,
  mintMessageId,
  mintReceiptId,
  mintTaskId,
  mintTurnId,
} from "../src/ids.js";

// The runtime fail-closed ingestion parser is the boundary that keeps the
// ledger body-free: TypeScript shapes are erased at runtime, so a raw row that
// smuggles a body field (or any unknown/malformed field) must be REJECTED, not
// silently accepted or sanitized. These controls prove the parser discriminates
// (valid rows parse) and fails closed (malformed/body-bearing rows throw).

const VALID_DIGEST = "a1b2c3d4e5f60718293a4b5c6d7e8f90".repeat(2);

function validNotice(): Record<string, unknown> {
  return {
    kind: "notice_metadata",
    seat: "worker-1",
    deliveryId: mintDeliveryId(),
    subtaskId: mintTaskId(),
    bodyPresent: false,
    target: "#lane",
    count: 1,
    firstMessageId: mintMessageId(),
    latestMessageId: mintMessageId(),
    ordinal: 10,
  };
}

function validBodyQuery(): Record<string, unknown> {
  return {
    kind: "body_read",
    seat: "worker-1",
    deliveryId: mintDeliveryId(),
    subtaskId: mintTaskId(),
    explicitQuery: true,
    queryTarget: "#lane",
    queriedMessageId: mintMessageId(),
    queryTurnId: mintTurnId(),
    ordinal: 30,
  };
}

function validDurability(): Record<string, unknown> {
  return {
    parentTaskId: mintTaskId(),
    bodyDigest: VALID_DIGEST,
    bodyBytes: 2048,
    receiptId: mintReceiptId(),
    ordinal: 1,
  };
}

test("parser: valid NOTICE / body-query / durability rows parse", () => {
  const notice = parseDeliveryObservation(validNotice());
  assert.equal(notice.kind, "notice_metadata");
  const query = parseDeliveryObservation(validBodyQuery());
  assert.equal(query.kind, "body_read");
  const dur = parseBodyDurability(validDurability());
  assert.equal(dur.bodyBytes, 2048);
});

test("parser: a NOTICE carrying a body-bearing field is rejected", () => {
  for (const key of ["body", "content", "payload", "text", "rawBody"]) {
    assert.throws(
      () => parseDeliveryObservation({ ...validNotice(), [key]: "leak" }),
      LedgerIngestError,
      `NOTICE with "${key}" must be rejected`,
    );
  }
});

test("parser: a body query carrying a body-bearing field is rejected", () => {
  assert.throws(
    () => parseDeliveryObservation({ ...validBodyQuery(), bodyContent: "leak" }),
    LedgerIngestError,
  );
});

test("parser: an unknown extra field is rejected", () => {
  assert.throws(
    () => parseDeliveryObservation({ ...validNotice(), surprise: 1 }),
    LedgerIngestError,
  );
});

test("parser: a NOTICE asserting bodyPresent:true is rejected", () => {
  assert.throws(
    () => parseDeliveryObservation({ ...validNotice(), bodyPresent: true }),
    LedgerIngestError,
  );
});

test("parser: a NOTICE missing target / first / latest is rejected", () => {
  for (const key of ["target", "firstMessageId", "latestMessageId"]) {
    const row = validNotice();
    delete row[key];
    assert.throws(() => parseDeliveryObservation(row), LedgerIngestError);
  }
});

test("parser: a body query that is not explicit is rejected", () => {
  assert.throws(
    () => parseDeliveryObservation({ ...validBodyQuery(), explicitQuery: false }),
    LedgerIngestError,
  );
});

test("parser: a body query missing target / message / turn is rejected", () => {
  for (const key of ["queryTarget", "queriedMessageId", "queryTurnId"]) {
    const row = validBodyQuery();
    delete row[key];
    assert.throws(() => parseDeliveryObservation(row), LedgerIngestError);
  }
});

test("parser: an unknown delivery kind is rejected", () => {
  assert.throws(
    () => parseDeliveryObservation({ ...validNotice(), kind: "push_body" }),
    LedgerIngestError,
  );
});

test("parser: durability with non-positive bytes is rejected", () => {
  assert.throws(
    () => parseBodyDurability({ ...validDurability(), bodyBytes: 0 }),
    LedgerIngestError,
  );
});

test("parser: durability with a non-SHA-256 digest is rejected", () => {
  assert.throws(
    () => parseBodyDurability({ ...validDurability(), bodyDigest: "not-a-digest" }),
    LedgerIngestError,
  );
});

test("parser: durability carrying a raw body field is rejected", () => {
  assert.throws(
    () => parseBodyDurability({ ...validDurability(), body: "leak" }),
    LedgerIngestError,
  );
});

test("parser: a NON-ENUMERABLE body-bearing property is still rejected", () => {
  // Object.keys would miss this; Reflect.ownKeys must catch it.
  const notice = validNotice();
  Object.defineProperty(notice, "body", { value: "secret", enumerable: false });
  assert.throws(() => parseDeliveryObservation(notice), LedgerIngestError);

  const dur = validDurability();
  Object.defineProperty(dur, "rawBody", { value: "secret", enumerable: false });
  assert.throws(() => parseBodyDurability(dur), LedgerIngestError);
});

test("parser: a non-enumerable UNKNOWN property is still rejected", () => {
  const notice = validNotice();
  Object.defineProperty(notice, "surprise", { value: 1, enumerable: false });
  assert.throws(() => parseDeliveryObservation(notice), LedgerIngestError);
});

test("parser: a symbol-keyed property is rejected", () => {
  const notice = validNotice();
  (notice as Record<PropertyKey, unknown>)[Symbol("body")] = "secret";
  assert.throws(() => parseDeliveryObservation(notice), LedgerIngestError);
});

test("parser: durability missing the durable receipt id is rejected", () => {
  const row = validDurability();
  delete row["receiptId"];
  assert.throws(() => parseBodyDurability(row), LedgerIngestError);
});

// Frozen identifier-grammar controls.
test("parser: a non-channel NOTICE/body-query target is rejected", () => {
  assert.throws(
    () => parseDeliveryObservation({ ...validNotice(), target: "not-a-channel-target" }),
    LedgerIngestError,
  );
  assert.throws(
    () =>
      parseDeliveryObservation({ ...validBodyQuery(), queryTarget: "not-a-channel-target" }),
    LedgerIngestError,
  );
});

test("parser: a bare '#' or whitespace target is rejected", () => {
  for (const bad of ["#", "# lane", "#lane #x"]) {
    assert.throws(
      () => parseDeliveryObservation({ ...validNotice(), target: bad }),
      LedgerIngestError,
    );
  }
});

test("parser: a channel-thread target is accepted", () => {
  const notice = parseDeliveryObservation({ ...validNotice(), target: "#lane:thread" });
  assert.equal(notice.kind, "notice_metadata");
});

test("parser: message refs must be msg_ ids, not dlv_ or arbitrary strings", () => {
  for (const key of ["firstMessageId", "latestMessageId"]) {
    assert.throws(
      () => parseDeliveryObservation({ ...validNotice(), [key]: "not-a-message-id" }),
      LedgerIngestError,
      `NOTICE ${key} must reject a non-message-id`,
    );
    assert.throws(
      () => parseDeliveryObservation({ ...validNotice(), [key]: mintDeliveryId() }),
      LedgerIngestError,
      `NOTICE ${key} must reject a dlv_ id`,
    );
  }
  assert.throws(
    () => parseDeliveryObservation({ ...validBodyQuery(), queriedMessageId: mintDeliveryId() }),
    LedgerIngestError,
  );
});

test("parser: wrong-prefix or malformed branded ids are rejected", () => {
  // deliveryId must be dlv_, not a tsk_ id.
  assert.throws(
    () => parseDeliveryObservation({ ...validNotice(), deliveryId: mintTaskId() }),
    LedgerIngestError,
  );
  // queryTurnId must be trn_, not a dlv_ id.
  assert.throws(
    () => parseDeliveryObservation({ ...validBodyQuery(), queryTurnId: mintDeliveryId() }),
    LedgerIngestError,
  );
  // A structurally malformed id (right prefix, wrong body) is rejected.
  assert.throws(
    () => parseDeliveryObservation({ ...validNotice(), deliveryId: "dlv_short" }),
    LedgerIngestError,
  );
  // durability parent/receipt ids must match their prefixes.
  assert.throws(
    () => parseBodyDurability({ ...validDurability(), parentTaskId: mintReceiptId() }),
    LedgerIngestError,
  );
});
