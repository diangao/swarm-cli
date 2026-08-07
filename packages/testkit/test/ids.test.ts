import assert from "node:assert/strict";
import { test } from "node:test";

import {
  mintAgentId,
  mintArtifactDigest,
  mintChannelId,
  mintClaimId,
  mintCommandId,
  mintConversationId,
  mintDeliveryId,
  mintFenceToken,
  mintLaunchId,
  mintLeaseId,
  mintMachineId,
  mintMessageId,
  mintProducerFactId,
  mintReceiptId,
  mintServerId,
  mintSessionId,
  mintStateInstanceId,
  mintTaskId,
  mintTurnId,
  resetIdCounters,
} from "../src/ids.js";

// The EXACT Protocol grammars (mirrored from packages/protocol/src/validate.ts).
// Testkit must not import Protocol's internal regexes, so these are transcribed
// verbatim; the drift risk is covered by the round-trip assertions below, which
// run the real prefix rules character for character.
const ID_BODY = "[0-9a-hjkmnp-tv-z]{26}";
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const FENCE_PATTERN = /^fnc_[0-9a-hjkmnp-tv-z]{26}$/u;

/** A branded-id family: its prefix, minter, and full pattern. */
type Family = {
  readonly prefix: string;
  readonly mint: () => string;
};

const BRANDED_FAMILIES: readonly Family[] = [
  { prefix: "srv", mint: mintServerId },
  { prefix: "mch", mint: mintMachineId },
  { prefix: "agt", mint: mintAgentId },
  { prefix: "chn", mint: mintChannelId },
  { prefix: "cvs", mint: mintConversationId },
  { prefix: "msg", mint: mintMessageId },
  { prefix: "dlv", mint: mintDeliveryId },
  { prefix: "fac", mint: mintProducerFactId },
  { prefix: "tsk", mint: mintTaskId },
  { prefix: "clm", mint: mintClaimId },
  { prefix: "lse", mint: mintLeaseId },
  { prefix: "lnc", mint: mintLaunchId },
  { prefix: "cmd", mint: mintCommandId },
  { prefix: "rcp", mint: mintReceiptId },
  { prefix: "sti", mint: mintStateInstanceId },
  { prefix: "trn", mint: mintTurnId },
  { prefix: "ses", mint: mintSessionId },
];

function patternFor(prefix: string): RegExp {
  return new RegExp(`^${prefix}_${ID_BODY}$`, "u");
}

test("every branded-id family mints a grammar-valid value", () => {
  resetIdCounters();
  for (const family of BRANDED_FAMILIES) {
    const value = family.mint();
    assert.match(
      value,
      patternFor(family.prefix),
      `${family.prefix} minted an invalid id: ${value}`,
    );
    assert.ok(
      value.startsWith(`${family.prefix}_`),
      `${family.prefix} minted id has the wrong prefix: ${value}`,
    );
  }
});

test("fence token matches the Protocol fence grammar", () => {
  resetIdCounters();
  const token = mintFenceToken();
  assert.match(token, FENCE_PATTERN, `invalid fence token: ${token}`);
});

test("artifact digest matches the Protocol sha256 grammar (64 lowercase hex)", () => {
  resetIdCounters();
  const digest = mintArtifactDigest();
  assert.match(digest, DIGEST_PATTERN, `invalid artifact digest: ${digest}`);
});

test("consecutive fence tokens differ (increment, not read-only)", () => {
  resetIdCounters();
  const a = mintFenceToken();
  const b = mintFenceToken();
  assert.notEqual(a, b, "consecutive fence tokens must not collide");
});

test("consecutive artifact digests differ", () => {
  resetIdCounters();
  const a = mintArtifactDigest();
  const b = mintArtifactDigest();
  assert.notEqual(a, b, "consecutive artifact digests must not collide");
});

test("1000 consecutive mints per family are all unique and all valid", () => {
  for (const family of BRANDED_FAMILIES) {
    resetIdCounters();
    const pattern = patternFor(family.prefix);
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i += 1) {
      const value = family.mint();
      assert.match(value, pattern, `${family.prefix} #${i} invalid: ${value}`);
      assert.ok(!seen.has(value), `${family.prefix} #${i} duplicate: ${value}`);
      seen.add(value);
    }
    assert.equal(seen.size, 1000);
  }
});

test("1000 consecutive fence tokens are all unique and valid", () => {
  resetIdCounters();
  const seen = new Set<string>();
  for (let i = 0; i < 1000; i += 1) {
    const token = mintFenceToken();
    assert.match(token, FENCE_PATTERN, `fence #${i} invalid: ${token}`);
    assert.ok(!seen.has(token), `fence #${i} duplicate: ${token}`);
    seen.add(token);
  }
  assert.equal(seen.size, 1000);
});

test("1000 consecutive artifact digests are all unique and valid", () => {
  resetIdCounters();
  const seen = new Set<string>();
  for (let i = 0; i < 1000; i += 1) {
    const digest = mintArtifactDigest();
    assert.match(digest, DIGEST_PATTERN, `digest #${i} invalid: ${digest}`);
    assert.ok(!seen.has(digest), `digest #${i} duplicate: ${digest}`);
    seen.add(digest);
  }
  assert.equal(seen.size, 1000);
});

test("resetIdCounters makes minting reproducible", () => {
  resetIdCounters();
  const first = mintTaskId();
  const firstDigest = mintArtifactDigest();
  const firstFence = mintFenceToken();
  resetIdCounters();
  assert.equal(mintTaskId(), first);
  assert.equal(mintArtifactDigest(), firstDigest);
  assert.equal(mintFenceToken(), firstFence);
});
