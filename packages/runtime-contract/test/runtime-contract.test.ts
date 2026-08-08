import assert from "node:assert/strict";
import { test } from "node:test";

import type {
  AgentId,
  ArtifactDigest,
  AttentionNotice,
  ChannelId,
  DeliveryId,
  LaunchId,
  MachineId,
  MessageId,
  NativeDeliveryEnvelope,
  ProducerFactId,
  ProtocolVersion,
  StandingManifest,
} from "@swarm/protocol";

import {
  compileNativeTurnInput,
  freezeStandingManifest,
  serializeNativeTurnInput,
} from "../src/index.js";

const token = "01j00000000000000000000000";
const version = 1 as ProtocolVersion;
const digest = (fill: string): ArtifactDigest => `sha256:${fill.repeat(64)}` as ArtifactDigest;

function manifest(overrides: Partial<StandingManifest> = {}): StandingManifest {
  return {
    protocolVersion: version,
    agentId: `agt_${token}` as AgentId,
    runtime: "codex",
    workspaceGeneration: 7,
    identityDigest: digest("1"),
    memoryDigest: digest("2"),
    cliContractDigest: digest("3"),
    capabilityDigest: digest("4"),
    ...overrides,
  };
}

function delivery(serverSeq = 21): NativeDeliveryEnvelope {
  return {
    protocolVersion: version,
    deliveryId: `dlv_${token}` as DeliveryId,
    attempt: 1,
    messageId: `msg_${token}` as MessageId,
    target: { kind: "channel", channelId: `chn_${token}` as ChannelId },
    serverSeq,
    producerFactId: `fac_${token}` as ProducerFactId,
    agentId: `agt_${token}` as AgentId,
    machineId: `mch_${token}` as MachineId,
    expectedLaunchId: `lnc_${token}` as LaunchId,
    membershipEpoch: 3,
    routingGeneration: 2,
    routeVersion: 5,
  };
}

test("manifest digest is wake-independent, component-sensitive, and deeply frozen", () => {
  const source = manifest();
  const first = freezeStandingManifest(source);
  const second = freezeStandingManifest({ ...source });
  assert.equal(first.manifestDigest, second.manifestDigest);
  assert.ok(Object.isFrozen(first));
  assert.ok(Object.isFrozen(first.manifest));

  source.workspaceGeneration = 99;
  assert.equal(first.manifest.workspaceGeneration, 7);
  assert.notEqual(first.manifestDigest, freezeStandingManifest(source).manifestDigest);

  for (const field of ["identityDigest", "memoryDigest", "cliContractDigest", "capabilityDigest"] as const) {
    const changed = manifest({ [field]: digest("f") });
    assert.notEqual(first.manifestDigest, freezeStandingManifest(changed).manifestDigest, field);
  }
});

test("native input contains the exact current body once and metadata-only attention", () => {
  const body = "  keep exact 🙂 bytes  ";
  const notice: AttentionNotice = {
    protocolVersion: version,
    target: { kind: "channel", channelId: `chn_${token}` as ChannelId },
    pendingCount: 1,
    firstMessageId: `msg_${"02j00000000000000000000000"}` as MessageId,
    latestMessageId: `msg_${"02j00000000000000000000000"}` as MessageId,
    firstServerSeq: 20,
    latestServerSeq: 20,
  };
  const compiled = compileNativeTurnInput({
    frozenManifest: freezeStandingManifest(manifest()),
    delivery: delivery(),
    body,
    attention: [notice],
  });
  const serialized = new TextDecoder().decode(serializeNativeTurnInput(compiled));
  compiled.bytes.fill(0);
  assert.equal(compiled.input.current.body, body);
  assert.equal(serialized.split(JSON.stringify(body)).length - 1, 1);
  assert.equal(new TextDecoder().decode(serializeNativeTurnInput(compiled)), serialized);
  assert.equal(serialized.includes("hidden prior body"), false);
  assert.deepEqual(Object.keys(compiled.input.attention[0] ?? {}).sort(), [
    "firstMessageId",
    "firstServerSeq",
    "latestMessageId",
    "latestServerSeq",
    "pendingCount",
    "protocolVersion",
    "target",
  ]);
});
