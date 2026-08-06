import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  canonicalProtocolJson,
  negotiateProtocolVersion,
  parseDeliveryEnvelope,
  parseLaunchCommand,
  parseProtocolJson,
  parseTaskLease,
  parseTransitionReceipt,
  type ProtocolVersion,
} from "../src/index.js";
import { recordCommandIdentity } from "../src/internal/idempotency.js";

type PositiveRegistry = {
  targets: Record<string, unknown>;
  deliveries: Record<string, unknown>;
  leases: Record<string, unknown>;
  launches: Record<string, unknown>;
  receipts: Record<string, unknown>;
  negotiation: Record<string, [unknown, unknown, number]>;
};

type Seed = { seed: string; expected: string };

const fixtureRoot = "../../contracts/protocol/fixtures";
const positive = JSON.parse(
  await readFile(`${fixtureRoot}/positive.json`, "utf8"),
) as PositiveRegistry;
const seeds = JSON.parse(
  await readFile(`${fixtureRoot}/seeded-controls.json`, "utf8"),
) as Seed[];
const encoder = new TextEncoder();
const v1 = negotiateProtocolVersion(
  { major: 0, minMinor: 1, maxMinor: 1 },
  { major: 0, minMinor: 1, maxMinor: 1 },
);

function bytes(value: unknown): Uint8Array {
  return canonicalProtocolJson(value);
}

function expectProtocolError(action: () => unknown, code: string): void {
  assert.throws(action, (error: unknown) => {
    assert(error instanceof Error);
    assert.equal(error.name, "ProtocolError");
    assert.equal(error.message, code);
    return true;
  });
}

test("positive compatibility registry parses and round-trips", () => {
  for (const value of Object.values(positive.deliveries)) {
    const parsed = parseDeliveryEnvelope(bytes(value), v1);
    assert.deepEqual(parseDeliveryEnvelope(canonicalProtocolJson(parsed), v1), parsed);
  }
  for (const value of Object.values(positive.leases)) {
    const parsed = parseTaskLease(bytes(value));
    assert.deepEqual(parseTaskLease(canonicalProtocolJson(parsed)), parsed);
  }
  for (const value of Object.values(positive.launches)) {
    const parsed = parseLaunchCommand(bytes(value), v1);
    assert.deepEqual(parseLaunchCommand(canonicalProtocolJson(parsed), v1), parsed);
  }
  for (const value of Object.values(positive.receipts)) {
    const parsed = parseTransitionReceipt(bytes(value), v1);
    assert.deepEqual(parseTransitionReceipt(canonicalProtocolJson(parsed), v1), parsed);
  }
});

test("all target variants parse inside an envelope", () => {
  const base = positive.deliveries.first as Record<string, unknown>;
  for (const value of Object.values(positive.targets)) {
    const delivery = { ...base, target: value };
    assert.deepEqual(parseDeliveryEnvelope(bytes(delivery), v1).target, value);
  }
});

test("lease renewal preserves complete fence identity", () => {
  const live = parseTaskLease(bytes(positive.leases.live));
  const renewal = parseTaskLease(bytes(positive.leases.renewal));
  assert.deepEqual(
    {
      taskId: renewal.taskId,
      claimId: renewal.claimId,
      leaseId: renewal.leaseId,
      leaseEpoch: renewal.leaseEpoch,
      fenceToken: renewal.fenceToken,
      attempt: renewal.attempt,
    },
    {
      taskId: live.taskId,
      claimId: live.claimId,
      leaseId: live.leaseId,
      leaseEpoch: live.leaseEpoch,
      fenceToken: live.fenceToken,
      attempt: live.attempt,
    },
  );
  assert(renewal.expiresAt > live.expiresAt);
});

test("version negotiation selects the highest overlap", () => {
  for (const [local, remote, expected] of Object.values(positive.negotiation)) {
    assert.equal(
      negotiateProtocolVersion(
        local as { major: number; minMinor: number; maxMinor: number },
        remote as { major: number; minMinor: number; maxMinor: number },
      ),
      expected,
    );
  }
  assert.equal(
    negotiateProtocolVersion(
      { major: 1, minMinor: 0, maxMinor: 2 },
      { major: 1, minMinor: 0, maxMinor: 1 },
    ),
    1001,
  );
  expectProtocolError(
    () =>
      negotiateProtocolVersion(
        { major: 0, minMinor: 1, maxMinor: 1 },
        { major: 0, minMinor: 2, maxMinor: 2 },
      ),
    "PROTOCOL_VERSION_UNSUPPORTED",
  );
  expectProtocolError(
    () =>
      negotiateProtocolVersion(
        { major: 0, minMinor: 1, maxMinor: 2 },
        { major: 1, minMinor: 0, maxMinor: 0 },
      ),
    "PROTOCOL_VERSION_UNSUPPORTED",
  );
  expectProtocolError(
    () =>
      negotiateProtocolVersion(
        { major: 0, minMinor: 0, maxMinor: 0 },
        { major: 0, minMinor: 0, maxMinor: 0 },
      ),
    "PROTOCOL_VERSION_UNSUPPORTED",
  );
});

test("canonical JSON is stable, ordered, and plain-integer", () => {
  const canonical = new TextDecoder().decode(
    canonicalProtocolJson({ z: 1e3, a: { y: true, x: "é" }, list: [3, 2, 1] }),
  );
  assert.equal(canonical, '{"a":{"x":"é","y":true},"list":[3,2,1],"z":1000}');
  assert.equal(
    new TextDecoder().decode(canonicalProtocolJson(parseProtocolJson(encoder.encode(canonical)))),
    canonical,
  );
  expectProtocolError(() => canonicalProtocolJson({ unsafe: Number.MAX_SAFE_INTEGER + 1 }), "INVALID_SCALAR");
  expectProtocolError(() => canonicalProtocolJson({ fractional: 1.5 }), "INVALID_SCALAR");
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  expectProtocolError(() => canonicalProtocolJson(cyclic), "INVALID_SCALAR");
});

test("raw reader rejects duplicate keys and unsafe wire framing", () => {
  expectProtocolError(
    () => parseProtocolJson(encoder.encode('{"outer":{"same":1,"same":2}}')),
    "DUPLICATE_KEY",
  );
  expectProtocolError(() => parseProtocolJson(new Uint8Array(65_537)), "PAYLOAD_TOO_LARGE");
  expectProtocolError(() => parseProtocolJson(Uint8Array.of(0xef, 0xbb, 0xbf, 0x7b, 0x7d)), "INVALID_JSON");
  expectProtocolError(() => parseProtocolJson(Uint8Array.of(0xc3, 0x28)), "INVALID_JSON");
  expectProtocolError(() => parseProtocolJson(encoder.encode("[]")), "INVALID_JSON");
  expectProtocolError(() => parseProtocolJson(encoder.encode("{} trailing")), "INVALID_JSON");
  expectProtocolError(
    () => parseProtocolJson(encoder.encode('{"unsafe":9007199254740992}')),
    "INVALID_SCALAR",
  );
  expectProtocolError(
    () => parseProtocolJson(encoder.encode('{"fractional":1.5}')),
    "INVALID_SCALAR",
  );
  let deep = "0";
  for (let index = 0; index < 17; index += 1) deep = `{"a":${deep}}`;
  expectProtocolError(() => parseProtocolJson(encoder.encode(deep)), "INVALID_JSON");
});

test("identifiers, numbers, timestamps, and replay invariants fail closed", () => {
  const base = positive.deliveries.first as Record<string, unknown>;
  for (const deliveryId of [
    "",
    " dlv_00000000000000000000000000",
    "DLV_00000000000000000000000000",
    "msg_00000000000000000000000000",
    "dlv_human-readable-identifier",
  ]) {
    expectProtocolError(
      () => parseDeliveryEnvelope(bytes({ ...base, deliveryId }), v1),
      "INVALID_SCALAR",
    );
  }
  for (const attempt of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "1"]) {
    expectProtocolError(
      () => parseDeliveryEnvelope(bytes({ ...base, attempt }), v1),
      "INVALID_SCALAR",
    );
  }
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...base, replayOf: "dlv_11111111111111111111111111" }), v1),
    "INVARIANT_VIOLATION",
  );
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...base, attempt: 2 }), v1),
    "INVARIANT_VIOLATION",
  );
  expectProtocolError(
    () =>
      parseDeliveryEnvelope(
        bytes({ ...base, attempt: 2, replayOf: base.deliveryId }),
        v1,
      ),
    "INVARIANT_VIOLATION",
  );

  const lease = positive.leases.live as Record<string, unknown>;
  for (const acquiredAt of [
    "2026-08-06T08:00:00Z",
    "2026-08-06T08:00:00.000+00:00",
    "2026-02-30T08:00:00.000Z",
    "2026-08-06T08:00:60.000Z",
  ]) {
    expectProtocolError(() => parseTaskLease(bytes({ ...lease, acquiredAt })), "INVALID_SCALAR");
  }
  expectProtocolError(
    () => parseTaskLease(bytes({ ...lease, expiresAt: lease.acquiredAt })),
    "INVARIANT_VIOLATION",
  );
  const { fenceToken: _fenceToken, ...partialLease } = lease;
  void _fenceToken;
  expectProtocolError(() => parseTaskLease(bytes(partialLease)), "INVALID_SCALAR");
});

test("strict objects and discriminated unions reject unsafe shapes", () => {
  const delivery = positive.deliveries.first as Record<string, unknown>;
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, ignored: true }), v1),
    "UNKNOWN_FIELD",
  );
  expectProtocolError(
    () =>
      parseDeliveryEnvelope(
        bytes({
          ...delivery,
          target: {
            kind: "channel",
            channelId: "chn_00000000000000000000000000",
            conversationId: "cvs_00000000000000000000000000",
          },
        }),
        v1,
      ),
    "UNKNOWN_FIELD",
  );
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, target: { kind: "broadcast" } }), v1),
    "UNSUPPORTED_VARIANT",
  );
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, target: null }), v1),
    "INVALID_SCALAR",
  );

  const launch = positive.launches.withoutWake as Record<string, unknown>;
  expectProtocolError(
    () => parseLaunchCommand(bytes({ ...launch, runtime: "future-runtime" }), v1),
    "UNSUPPORTED_VARIANT",
  );
  expectProtocolError(
    () => parseLaunchCommand(bytes({ ...launch, workspaceGeneration: "1" }), v1),
    "INVALID_SCALAR",
  );
});

test("every receipt rejects wrong actor, wrong fence, and digest placement", () => {
  const knownActors = [
    (positive.receipts.serverAccepted as Record<string, unknown>).actor,
    (positive.receipts.claimWon as Record<string, unknown>).actor,
    (positive.receipts.daemonAccepted as Record<string, unknown>).actor,
  ];
  const knownFences = [
    (positive.receipts.serverAccepted as Record<string, unknown>).fence,
    (positive.receipts.claimWon as Record<string, unknown>).fence,
    (positive.receipts.processSpawned as Record<string, unknown>).fence,
    (positive.receipts.runtimeReady as Record<string, unknown>).fence,
    (positive.receipts.inputWritten as Record<string, unknown>).fence,
    (positive.receipts.artifactPublished as Record<string, unknown>).fence,
  ];
  const actorKeys: Record<string, readonly string[]> = {
    server_accepted: ["serverId"],
    claim_won: ["serverId", "agentId"],
    daemon_accepted: ["machineId", "agentId"],
    process_spawned: ["machineId", "agentId"],
    runtime_ready: ["machineId", "agentId"],
    input_written: ["machineId", "agentId"],
    model_visible: ["machineId", "agentId"],
    side_effect_applied: ["machineId", "agentId"],
    artifact_published: ["machineId", "agentId"],
    review_verdict: ["machineId", "agentId"],
  };
  const fenceKeys: Record<string, readonly string[]> = {
    server_accepted: [],
    claim_won: ["leaseEpoch", "fenceToken"],
    daemon_accepted: [],
    process_spawned: ["launchId", "stateInstanceId"],
    runtime_ready: ["launchId", "stateInstanceId", "sessionId"],
    input_written: ["launchId", "stateInstanceId", "turnId", "sessionId"],
    model_visible: ["launchId", "stateInstanceId", "turnId", "sessionId"],
    artifact_published: [
      "leaseEpoch",
      "fenceToken",
      "launchId",
      "stateInstanceId",
      "turnId",
      "sessionId",
    ],
    review_verdict: [
      "leaseEpoch",
      "fenceToken",
      "launchId",
      "stateInstanceId",
      "turnId",
      "sessionId",
    ],
  };
  const canonical = (value: unknown): string => new TextDecoder().decode(bytes(value));
  const strictShapeError = (
    candidate: unknown,
    expectedKeys: readonly string[],
  ): "UNKNOWN_FIELD" | "INVALID_SCALAR" => {
    const keys = Object.keys(candidate as Record<string, unknown>);
    return keys.some((key) => !expectedKeys.includes(key))
      ? "UNKNOWN_FIELD"
      : "INVALID_SCALAR";
  };

  for (const value of Object.values(positive.receipts)) {
    const receipt = value as Record<string, unknown>;
    const kind = receipt.kind as string;
    const actor = receipt.actor as Record<string, unknown>;
    const wrongActor = Object.hasOwn(actor, "serverId")
      ? {
          machineId: "mch_00000000000000000000000000",
          agentId: "agt_00000000000000000000000000",
        }
      : { serverId: "srv_00000000000000000000000000" };
    expectProtocolError(
      () => parseTransitionReceipt(bytes({ ...receipt, actor: wrongActor }), v1),
      "UNKNOWN_FIELD",
    );
    for (const knownActor of knownActors) {
      if (canonical(knownActor) === canonical(actor)) continue;
      expectProtocolError(
        () => parseTransitionReceipt(bytes({ ...receipt, actor: knownActor }), v1),
        strictShapeError(knownActor, actorKeys[kind] ?? []),
      );
    }
    expectProtocolError(
      () =>
        parseTransitionReceipt(
          bytes({
            ...receipt,
            actor: {
              ...actor,
              serverId: "srv_00000000000000000000000000",
              machineId: "mch_00000000000000000000000000",
            },
          }),
          v1,
        ),
      "UNKNOWN_FIELD",
    );
    expectProtocolError(
      () => parseTransitionReceipt(bytes({ ...receipt, fence: { forbidden: true } }), v1),
      "UNKNOWN_FIELD",
    );
    const fence = receipt.fence as Record<string, unknown>;
    for (const knownFence of knownFences) {
      const knownCanonical = canonical(knownFence);
      if (kind === "side_effect_applied") {
        const turnCanonical = canonical(
          (positive.receipts.sideEffectTurn as Record<string, unknown>).fence,
        );
        const leaseTurnCanonical = canonical(
          (positive.receipts.sideEffectLeaseTurn as Record<string, unknown>).fence,
        );
        if (knownCanonical === turnCanonical || knownCanonical === leaseTurnCanonical) continue;
        expectProtocolError(
          () => parseTransitionReceipt(bytes({ ...receipt, fence: knownFence }), v1),
          "INVALID_SCALAR",
        );
      } else {
        if (knownCanonical === canonical(fence)) continue;
        expectProtocolError(
          () => parseTransitionReceipt(bytes({ ...receipt, fence: knownFence }), v1),
          strictShapeError(knownFence, fenceKeys[kind] ?? []),
        );
      }
    }
    expectProtocolError(
      () =>
        parseTransitionReceipt(
          bytes({ ...receipt, fence: { ...fence, unexpectedFenceMember: true } }),
          v1,
        ),
      "UNKNOWN_FIELD",
    );
    const firstFenceKey = Object.keys(fence)[0];
    if (firstFenceKey !== undefined) {
      const subset = { ...fence };
      delete subset[firstFenceKey];
      expectProtocolError(
        () => parseTransitionReceipt(bytes({ ...receipt, fence: subset }), v1),
        receipt.kind === "side_effect_applied" && firstFenceKey === "leaseEpoch"
          ? "INVARIANT_VIOLATION"
          : "INVALID_SCALAR",
      );
    }
    if (receipt.kind !== "artifact_published" && receipt.kind !== "review_verdict") {
      expectProtocolError(
        () =>
          parseTransitionReceipt(
            bytes({
              ...receipt,
              artifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            }),
            v1,
          ),
        "INVARIANT_VIOLATION",
      );
    }
  }
  const published = positive.receipts.artifactPublished as Record<string, unknown>;
  const { artifactDigest: _artifactDigest, ...missingDigest } = published;
  void _artifactDigest;
  expectProtocolError(() => parseTransitionReceipt(bytes(missingDigest), v1), "INVALID_SCALAR");
  const sideEffect = positive.receipts.sideEffectTurn as Record<string, unknown>;
  expectProtocolError(
    () =>
      parseTransitionReceipt(
        bytes({
          ...sideEffect,
          fence: {
            ...(sideEffect.fence as Record<string, unknown>),
            leaseEpoch: 1,
          },
        }),
        v1,
      ),
    "INVARIANT_VIOLATION",
  );
});

test("all eight seeded defects are caught by stable categories", () => {
  assert.deepEqual(
    seeds.map(({ seed }) => seed),
    [
      "unknown-field-stripping",
      "permissive-enum",
      "mixed-union",
      "partial-fence",
      "number-coercion",
      "id-only-retry",
      "duplicate-key-reader",
      "same-major-version",
    ],
  );
  const expected = Object.fromEntries(seeds.map((seed) => [seed.seed, seed.expected]));
  const delivery = positive.deliveries.first as Record<string, unknown>;
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, extra: true }), v1),
    expected["unknown-field-stripping"] ?? "",
  );
  const launch = positive.launches.withoutWake as Record<string, unknown>;
  expectProtocolError(
    () => parseLaunchCommand(bytes({ ...launch, runtime: "anything" }), v1),
    expected["permissive-enum"] ?? "",
  );
  expectProtocolError(
    () =>
      parseDeliveryEnvelope(
        bytes({
          ...delivery,
          target: {
            kind: "channel",
            channelId: "chn_00000000000000000000000000",
            conversationId: "cvs_00000000000000000000000000",
          },
        }),
        v1,
      ),
    expected["mixed-union"] ?? "",
  );
  const sideEffect = positive.receipts.sideEffectTurn as Record<string, unknown>;
  expectProtocolError(
    () =>
      parseTransitionReceipt(
        bytes({
          ...sideEffect,
          fence: { ...(sideEffect.fence as Record<string, unknown>), fenceToken: "fnc_00000000000000000000000000" },
        }),
        v1,
      ),
    expected["partial-fence"] ?? "",
  );
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, attempt: "1" }), v1),
    expected["number-coercion"] ?? "",
  );

  const command = parseLaunchCommand(bytes(launch), v1);
  const seen = new Map<string, string>();
  assert.equal(recordCommandIdentity(seen, command), "new");
  assert.equal(recordCommandIdentity(seen, command), "replay");
  const conflicting = parseLaunchCommand(bytes({ ...launch, workspaceGeneration: 2 }), v1);
  expectProtocolError(
    () => recordCommandIdentity(seen, conflicting),
    expected["id-only-retry"] ?? "",
  );
  expectProtocolError(
    () => parseProtocolJson(encoder.encode('{"same":1,"same":2}')),
    expected["duplicate-key-reader"] ?? "",
  );
  const v2 = 2 as ProtocolVersion;
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, protocolVersion: 2 }), v1),
    expected["same-major-version"] ?? "",
  );
  expectProtocolError(
    () => parseDeliveryEnvelope(bytes({ ...delivery, protocolVersion: 2 }), v2),
    "PROTOCOL_VERSION_UNSUPPORTED",
  );
});
