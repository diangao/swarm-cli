import assert from "node:assert/strict";
import { test } from "node:test";

import {
  parseDeliveryEnvelope,
  parseTaskLease,
  parseTransitionReceipt,
} from "@swarm/protocol";
import type { ProtocolVersion } from "@swarm/protocol";

import {
  mintAgentId,
  mintChannelId,
  mintClaimId,
  mintDeliveryId,
  mintFenceToken,
  mintLeaseId,
  mintMachineId,
  mintMessageId,
  mintProducerFactId,
  mintReceiptId,
  mintServerId,
  mintTaskId,
  resetIdCounters,
} from "../src/ids.js";

// Round-trip minted ids through the REAL Protocol parser. If any minter emitted
// a value the wire parser rejects, these constructions throw INVALID_SCALAR and
// the test fails. This is the strongest available proof that minted scalars are
// grammar-valid, since it runs the production validator character for character.

const V1 = 1 as ProtocolVersion;

function encode(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

test("minted ids round-trip through parseDeliveryEnvelope", () => {
  resetIdCounters();
  const envelope = {
    protocolVersion: 1,
    deliveryId: mintDeliveryId(),
    attempt: 1,
    messageId: mintMessageId(),
    target: { kind: "channel", channelId: mintChannelId() },
    serverSeq: 1,
    producerFactId: mintProducerFactId(),
    agentId: mintAgentId(),
    machineId: mintMachineId(),
  };
  const parsed = parseDeliveryEnvelope(encode(envelope), V1);
  assert.equal(parsed.deliveryId, envelope.deliveryId);
  assert.equal(parsed.agentId, envelope.agentId);
  assert.equal(parsed.messageId, envelope.messageId);
});

test("minted ids + fence round-trip through parseTaskLease", () => {
  resetIdCounters();
  const lease = {
    taskId: mintTaskId(),
    claimId: mintClaimId(),
    leaseId: mintLeaseId(),
    leaseEpoch: 1,
    fenceToken: mintFenceToken(),
    attempt: 1,
    acquiredAt: "2026-01-01T00:00:00.000Z",
    expiresAt: "2026-01-01T00:01:00.000Z",
  };
  const parsed = parseTaskLease(encode(lease));
  assert.equal(parsed.taskId, lease.taskId);
  assert.equal(parsed.fenceToken, lease.fenceToken);
  assert.equal(parsed.leaseId, lease.leaseId);
});

test("minted receipt ids round-trip through parseTransitionReceipt", () => {
  resetIdCounters();
  const receipt = {
    protocolVersion: 1,
    receiptId: mintReceiptId(),
    kind: "server_accepted",
    producerFactId: mintProducerFactId(),
    actor: { serverId: mintServerId() },
    fence: {},
    occurredAt: "2026-01-01T00:00:00.000Z",
  };
  const parsed = parseTransitionReceipt(encode(receipt), V1);
  assert.equal(parsed.receiptId, receipt.receiptId);
  assert.equal(parsed.producerFactId, receipt.producerFactId);
});
