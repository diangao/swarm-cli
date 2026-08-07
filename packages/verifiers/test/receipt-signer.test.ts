import assert from "node:assert/strict";
import { test } from "node:test";

import type { ProtocolVersion, TransitionReceipt } from "@swarm/protocol";

import { createDeterministicReceiptSigner } from "../src/index.js";
import {
  mintProducerFactId,
  mintReceiptId,
  mintServerId,
  resetIdCounters,
} from "../src/ids.js";

function receipt(occurredAt = "2026-01-01T00:00:00.000Z"): TransitionReceipt {
  return {
    protocolVersion: 1 as ProtocolVersion,
    receiptId: mintReceiptId(),
    kind: "server_accepted",
    producerFactId: mintProducerFactId(),
    actor: { serverId: mintServerId() },
    fence: {},
    occurredAt,
  };
}

test("receipt signer produces a deterministic signature ref for the same receipt", () => {
  resetIdCounters();
  const signer = createDeterministicReceiptSigner();
  const r = receipt();
  const a = signer.sign(r);
  const b = signer.sign(r);
  assert.equal(a.signatureRef, b.signatureRef, "same receipt -> same signature ref");
  assert.match(a.signatureRef, /^sigref:[0-9a-f]{64}$/u, "signature ref is a neutral hex ref");
  assert.equal(a.receiptId, r.receiptId);
});

test("different receipts get different signature refs", () => {
  resetIdCounters();
  const signer = createDeterministicReceiptSigner();
  const a = signer.sign(receipt("2026-01-01T00:00:00.000Z"));
  const b = signer.sign(receipt("2026-01-01T00:00:01.000Z"));
  assert.notEqual(a.signatureRef, b.signatureRef);
});

test("verify accepts a matching ref and rejects a mismatched one", () => {
  resetIdCounters();
  const signer = createDeterministicReceiptSigner();
  const r = receipt();
  const signed = signer.sign(r);
  assert.ok(signer.verify(r, signed.signatureRef));
  assert.ok(!signer.verify(r, "sigref:0000"));
});

test("signature ref carries no body/key material (neutral ref only)", () => {
  resetIdCounters();
  const signer = createDeterministicReceiptSigner();
  const signed = signer.sign(receipt());
  // Structurally, the only fields are receiptId + signatureRef.
  assert.deepEqual(Object.keys(signed).sort(), ["receiptId", "signatureRef"]);
});
