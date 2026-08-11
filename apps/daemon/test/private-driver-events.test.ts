import assert from "node:assert/strict";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";

import {
  DriverRetentionError,
  type DriverCompositeCursorClaimHandle,
  type DriverEventObservation,
  type DriverPrivateClaimAttempt,
  type RetainedDriverEventLease,
} from "@swarm/drivers";
import type {
  ArtifactDigest,
  CommandId,
  LaunchId,
  MessageId,
  ProtocolVersion,
  SessionId,
  StateInstanceId,
  TurnId,
} from "@swarm/protocol";

import { SqlitePrivateDriverEventRetention } from "../src/private-driver-events.js";

const version = 2 as ProtocolVersion;
const launchId = id("lnc", "1") as LaunchId;
const stateInstanceId = id("sti", "2") as StateInstanceId;
const sessionId = id("ses", "3") as SessionId;
const turnId = id("trn", "4") as TurnId;
const waiterId = id("cmd", "5") as CommandId;
const sourceMessageId = id("msg", "6") as MessageId;
const bindingDigest = digest("7");

test("private SQLite encrypts retained events and a new epoch replays identical ciphertext without provider work", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-"));
  const launchRoot = join(temporary, "private-launch");
  const retention = new SqlitePrivateDriverEventRetention({
    launchRoot,
    protocolVersion: version,
    launchId,
    sourceWorkspace: join(temporary, "source"),
  });
  try {
    const firstAttempt = attempt(1, "8", "start", "live", 0, null);
    const first = await retention.claim(firstAttempt);
    const reply = retention.appendPrepared(first, retention.prepareAppend(first, observation({
      kind: "assistant_reply",
      turnId,
      text: "SENSITIVE_REPLY_MARKER_336",
    })));
    assert.equal(reply.applied, true);
    const completion = retention.appendPrepared(first, retention.prepareAppend(first, observation({
      kind: "turn_completed",
      turnId,
    })));
    assert.equal(completion.applied, true);

    assertPrivateFilesExclude(launchRoot, "SENSITIVE_REPLY_MARKER_336");
    const cipherBefore = Buffer.from((retention.prepareAppend(first, observation({
      kind: "turn_boundary",
      turnId,
      boundary: "continuation",
      steerable: false,
    }))).ciphertext);
    assert.ok(cipherBefore.length > 0);

    assert.deepEqual(await retention.releaseAttempt(firstAttempt), { applied: true });
    await assert.rejects(async () => retention.read(first, 0), hasCode("DRIVER_RETAINED_WATERMARK_MISMATCH"));

    const secondAttempt = attempt(2, "9", "resume", "retained_only", 0, null);
    const second = await retention.claim(secondAttempt);
    assert.equal(second.snapshotHeadNextOrdinal, 2);
    assert.throws(
      () => retention.prepareAppend(second, observation({ kind: "turn_completed", turnId })),
      hasCode("DRIVER_RETAINED_EVENT_CONFLICT"),
    );
    assert.equal(retention.read(second, 0)?.event.kind, "assistant_reply");
    assert.equal(retention.read(second, 1)?.event.kind, "turn_completed");
    const replay = await retention.openReplay({
      claim: claim(second, secondAttempt, retention),
      expectedTurnId: turnId,
      expectedBindingDigest: bindingDigest,
      expectedResolvedWaiterId: waiterId,
      expectedSourceMessageId: sourceMessageId,
    });
    assert.deepEqual((await collect(replay.records)).map((record) => record.event.kind), [
      "assistant_reply",
      "turn_completed",
    ]);
    await replay.close();

    const thirdAttempt = attempt(3, "a", "resume", "retained_only", 0, null);
    const third = await retention.claim(thirdAttempt);
    assert.deepEqual(retention.acknowledgeCommitted(third, {
      stateInstanceId,
      sessionId,
      nextOrdinal: 2,
      lastEventDigest: completion.record.eventDigest,
    }), { applied: true });
    assert.equal(retention.read(third, 0), null);
    assert.deepEqual(await retention.releaseAttempt(thirdAttempt), { applied: true });
    assert.deepEqual(retention.retireIfTerminal({ lease: third, storageTerminal: true }), { applied: true });
  } finally {
    retention.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("claim reconciles an exact storage-ahead prefix after commit-before-ACK crash", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-reconcile-"));
  const retention = new SqlitePrivateDriverEventRetention({
    launchRoot: join(temporary, "private-launch"),
    protocolVersion: version,
    launchId,
  });
  try {
    const crashedAttempt = attempt(1, "8", "start", "live", 0, null);
    const crashed = await retention.claim(crashedAttempt);
    const reply = retention.appendPrepared(crashed, retention.prepareAppend(crashed, observation({
      kind: "assistant_reply",
      turnId,
      text: "committed before private ACK",
    })));
    const completion = retention.appendPrepared(crashed, retention.prepareAppend(crashed, observation({
      kind: "turn_completed",
      turnId,
    })));

    // Storage committed through the completion and the process died before
    // private ACK/release. The new storage epoch is the sole recovery authority.
    const resumedAttempt = attempt(
      2,
      "9",
      "resume",
      "retained_only",
      2,
      completion.record.eventDigest,
    );
    const resumed = await retention.claim(resumedAttempt);
    assert.equal(resumed.snapshotHeadNextOrdinal, 2);
    assert.equal(retention.read(resumed, 0), null, "reconciled server effects never replay");
    assert.equal(retention.read(resumed, 1), null, "the full committed prefix is GC eligible");
    assert.deepEqual(await retention.releaseAttempt(resumedAttempt), { applied: true });

    await assert.rejects(
      retention.claim(attempt(3, "a", "resume", "retained_only", 1, reply.record.eventDigest)),
      hasCode("DRIVER_RETAINED_WATERMARK_MISMATCH"),
    );
    await assert.rejects(
      retention.claim(attempt(3, "b", "resume", "retained_only", 2, digest("f"))),
      hasCode("DRIVER_RETAINED_WATERMARK_MISMATCH"),
    );
  } finally {
    retention.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("claim-time reconciliation rejects a missing or divergent retained prefix without advancing ACK", async () => {
  for (const defect of ["missing", "divergent"] as const) {
    const temporary = await mkdtemp(join(tmpdir(), `swarm-retained-${defect}-`));
    const launchRoot = join(temporary, "private-launch");
    let retention: SqlitePrivateDriverEventRetention | undefined = new SqlitePrivateDriverEventRetention({
      launchRoot,
      protocolVersion: version,
      launchId,
    });
    try {
      const crashedAttempt = attempt(1, "8", "start", "live", 0, null);
      const crashed = await retention.claim(crashedAttempt);
      retention.appendPrepared(crashed, retention.prepareAppend(crashed, observation({
        kind: "assistant_reply",
        turnId,
        text: "retained prefix",
      })));
      const completion = retention.appendPrepared(crashed, retention.prepareAppend(crashed, observation({
        kind: "turn_completed",
        turnId,
      })));
      retention.close();
      retention = undefined;

      const database = new DatabaseSync(join(launchRoot, "driver-events.sqlite"));
      if (defect === "missing") {
        database.exec("DELETE FROM private_driver_event_records WHERE ordinal=0");
      } else {
        database.prepare(
          "UPDATE private_driver_event_records SET previous_record_digest=? WHERE ordinal=1",
        ).run(digest("f"));
      }
      database.close();

      retention = new SqlitePrivateDriverEventRetention({ launchRoot, protocolVersion: version, launchId });
      await assert.rejects(
        retention.claim(attempt(
          2,
          "9",
          "resume",
          "retained_only",
          2,
          completion.record.eventDigest,
        )),
        hasCode("DRIVER_RETAINED_WATERMARK_MISMATCH"),
      );
      retention.close();
      retention = undefined;

      const after = new DatabaseSync(join(launchRoot, "driver-events.sqlite"));
      const meta = after.prepare(
        "SELECT ack_next_ordinal FROM private_driver_event_meta WHERE state_instance_id=?",
      ).get(stateInstanceId) as { ack_next_ordinal: number };
      const count = after.prepare("SELECT COUNT(*) AS count FROM private_driver_event_records").get() as {
        count: number;
      };
      after.close();
      assert.equal(meta.ack_next_ordinal, 0);
      assert.equal(count.count, defect === "missing" ? 1 : 2);
    } finally {
      retention?.close();
      await rm(temporary, { recursive: true, force: true });
    }
  }
});

test("ciphertext corruption is rejected before a retained record can be yielded", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-"));
  const launchRoot = join(temporary, "private-launch");
  let retention: SqlitePrivateDriverEventRetention | undefined = new SqlitePrivateDriverEventRetention({
    launchRoot,
    protocolVersion: version,
    launchId,
  });
  try {
    const firstAttempt = attempt(1, "1", "start", "live", 0, null);
    const first = await retention.claim(firstAttempt);
    retention.appendPrepared(first, retention.prepareAppend(first, observation({
      kind: "assistant_reply",
      turnId,
      text: "corruption sentinel",
    })));
    assert.deepEqual(await retention.releaseAttempt(firstAttempt), { applied: true });
    retention.close();
    retention = undefined;

    const database = new DatabaseSync(join(launchRoot, "driver-events.sqlite"));
    database.exec("UPDATE private_driver_event_records SET auth_tag=zeroblob(16) WHERE ordinal=0");
    database.close();

    retention = new SqlitePrivateDriverEventRetention({
      launchRoot,
      protocolVersion: version,
      launchId,
    });
    const secondAttempt = attempt(2, "2", "resume", "retained_only", 0, null);
    const second = await retention.claim(secondAttempt);
    assert.throws(
      () => retention?.read(second, 0),
      hasCode("DRIVER_RETAINED_LOG_CORRUPT"),
    );
  } finally {
    retention?.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("unknown-result claim aliases exact attempt while newer epoch fences every old-key operation", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-"));
  const retention = new SqlitePrivateDriverEventRetention({
    launchRoot: join(temporary, "private-launch"),
    protocolVersion: version,
    launchId,
  });
  try {
    const exact = attempt(1, "b", "start", "live", 0, null);
    const first = await retention.claim(exact);
    assert.deepEqual(await retention.claim(exact), first);
    const newer = attempt(2, "c", "resume", "retained_only", 0, null);
    const second = await retention.claim(newer);
    await assert.rejects(async () => retention.read(first, 0), hasCode("DRIVER_RETAINED_WATERMARK_MISMATCH"));
    assert.equal(retention.read(second, 0), null);
    assert.deepEqual(await retention.releaseAttempt(exact), { applied: false });
    assert.deepEqual(await retention.releaseAttempt(newer), { applied: true });
  } finally {
    retention.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("same ordinal divergence, gap, wrong ACK and stale cleanup fail without changing the healthy row", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-"));
  const retention = new SqlitePrivateDriverEventRetention({
    launchRoot: join(temporary, "private-launch"),
    protocolVersion: version,
    launchId,
  });
  try {
    const exact = attempt(1, "d", "start", "live", 0, null);
    const lease = await retention.claim(exact);
    const prepared = retention.prepareAppend(lease, observation({ kind: "turn_completed", turnId }));
    const stored = retention.appendPrepared(lease, prepared);
    assert.equal(retention.appendPrepared(lease, prepared).applied, false);
    const divergent = { ...prepared, ciphertext: Uint8Array.from([...prepared.ciphertext, 0]) };
    assert.throws(() => retention.appendPrepared(lease, divergent), hasCode("DRIVER_RETAINED_EVENT_CONFLICT"));
    assert.throws(() => retention.acknowledgeCommitted(lease, {
      stateInstanceId,
      sessionId,
      nextOrdinal: 1,
      lastEventDigest: digest("e"),
    }), hasCode("DRIVER_RETAINED_WATERMARK_MISMATCH"));
    assert.equal(retention.read(lease, 0)?.recordDigest, stored.record.recordDigest);
    assert.deepEqual(retention.retireIfTerminal({ lease, storageTerminal: true }), { applied: false });
  } finally {
    retention.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("a truncated preexisting launch key fails closed and is never overwritten", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-"));
  const launchRoot = join(temporary, "private-launch");
  mkdirSync(launchRoot, { mode: 0o700 });
  const keyPath = join(launchRoot, "launch-key.v1");
  const original = Buffer.from("truncated-key");
  writeFileSync(keyPath, original, { mode: 0o600 });
  const retention = new SqlitePrivateDriverEventRetention({
    launchRoot,
    protocolVersion: version,
    launchId,
  });
  try {
    await assert.rejects(
      retention.claim(attempt(1, "f", "start", "live", 0, null)),
      hasCode("DRIVER_RETAINED_LOG_CORRUPT"),
    );
    assert.deepEqual(readFileSync(keyPath), original);
  } finally {
    retention.close();
    await rm(temporary, { recursive: true, force: true });
  }
});

test("private boundary rejects workspace nesting, symlink roots, and wrong POSIX mode before database open", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "swarm-retained-"));
  try {
    const source = join(temporary, "source");
    mkdirSync(source, { mode: 0o700 });
    assert.throws(() => new SqlitePrivateDriverEventRetention({
      launchRoot: join(source, "private"),
      sourceWorkspace: source,
      protocolVersion: version,
      launchId,
    }), hasCode("DRIVER_PRIVATE_BOUNDARY_VIOLATION"));
    assert.throws(() => new SqlitePrivateDriverEventRetention({
      launchRoot: temporary,
      sourceWorkspace: source,
      protocolVersion: version,
      launchId,
    }), hasCode("DRIVER_PRIVATE_BOUNDARY_VIOLATION"));
    const target = join(temporary, "target");
    mkdirSync(target, { mode: 0o700 });
    const linked = join(temporary, "linked");
    symlinkSync(target, linked);
    assert.throws(() => new SqlitePrivateDriverEventRetention({
      launchRoot: linked,
      protocolVersion: version,
      launchId,
    }), hasCode("DRIVER_PRIVATE_BOUNDARY_VIOLATION"));
    if (process.platform !== "win32") {
      const broad = join(temporary, "broad");
      mkdirSync(broad, { mode: 0o700 });
      chmodSync(broad, 0o755);
      assert.throws(() => new SqlitePrivateDriverEventRetention({
        launchRoot: broad,
        protocolVersion: version,
        launchId,
      }), hasCode("DRIVER_PRIVATE_BOUNDARY_VIOLATION"));

      const wrongFileRoot = join(temporary, "wrong-file-mode");
      mkdirSync(wrongFileRoot, { mode: 0o700 });
      const wrongDatabase = join(wrongFileRoot, "driver-events.sqlite");
      writeFileSync(wrongDatabase, "");
      chmodSync(wrongDatabase, 0o644);
      assert.throws(() => new SqlitePrivateDriverEventRetention({
        launchRoot: wrongFileRoot,
        protocolVersion: version,
        launchId,
      }), hasCode("DRIVER_PRIVATE_BOUNDARY_VIOLATION"));
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});

function attempt(
  readerEpoch: number,
  marker: string,
  processMode: "start" | "resume",
  replayMode: "live" | "retained_only",
  nextOrdinal: number,
  lastEventDigest: ArtifactDigest | null,
): DriverPrivateClaimAttempt {
  return {
    protocolVersion: version,
    launchId,
    stateInstanceId,
    sessionId,
    ownerToken: digest(marker),
    readerEpoch,
    nextOrdinal,
    lastEventDigest,
    claimAttemptId: id("cmd", marker) as CommandId,
    processMode,
    replayMode,
  };
}

function observation(event: DriverEventObservation["event"]): DriverEventObservation {
  return { stream: "turn", resolvedWaiterId: waiterId, sourceMessageId, bindingDigest, event };
}

function claim(
  lease: RetainedDriverEventLease,
  claimAttempt: DriverPrivateClaimAttempt,
  retention: SqlitePrivateDriverEventRetention,
): DriverCompositeCursorClaimHandle {
  let closePromise: Promise<ReturnType<typeof result>> | undefined;
  const close = () => closePromise ??= retention.releaseAttempt(claimAttempt).then(({ applied }) => result(applied));
  return { authority: lease, privateLease: lease, abort: close, release: close };
}

function result(applied: boolean) {
  return { applied, storageReleased: true, privateReleased: true };
}

function id(prefix: string, marker: string): string {
  return `${prefix}_${marker.repeat(26)}`;
}

function digest(marker: string): ArtifactDigest {
  return `sha256:${marker.repeat(64)}` as ArtifactDigest;
}

function hasCode(code: string): (error: unknown) => boolean {
  return (error) => error instanceof DriverRetentionError && error.code === code;
}

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const value of values) result.push(value);
  return result;
}

function assertPrivateFilesExclude(launchRoot: string, marker: string): void {
  const needle = Buffer.from(marker);
  for (const name of [
    "driver-events.sqlite",
    "driver-events.sqlite-wal",
    "driver-events.sqlite-shm",
    "launch-key.v1",
  ]) {
    const path = join(launchRoot, name);
    if (existsSync(path)) {
      assert.equal(readFileSync(path).includes(needle), false, `${name} contains plaintext marker`);
    }
  }
}
