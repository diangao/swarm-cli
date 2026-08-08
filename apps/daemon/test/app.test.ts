import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type {
  AcquireConsumePermit,
  AgentId,
  ArtifactDigest,
  BeginNativeWrite,
  ChannelId,
  CommandId,
  ConsumePermit,
  DeliveryAck,
  DeliveryAckResult,
  DeliveryFence,
  DeliveryId,
  LaunchId,
  MachineId,
  MessageId,
  NativeDeliveryEnvelope,
  ProducerFactId,
  ProtocolVersion,
  ReceiptId,
  ReconcileDeliveryAttempt,
  ReconcileDeliveryResult,
  ResumeConsumePermit,
  SessionId,
  StandingManifest,
  TaskId,
  TurnId,
} from "@swarm/protocol";
import { ScriptedDriver } from "@swarm/drivers/testing";
import {
  PrePermitDisconnectError,
  type NativeServerPort,
  type ReplyCommitCommand,
  type ReplyCommitResult,
  type TaskCommitCommand,
  type TaskCommitResult,
} from "@swarm/daemon-core";

import {
  closeDaemonApp,
  createInProcessLoopbackDaemon,
  LoopbackNativeServer,
  LoopbackServerConnection,
  NativeSqliteJournal,
} from "../src/index.js";

const token = "01j00000000000000000000000";
const version = 1 as ProtocolVersion;
const digest = (fill: string): ArtifactDigest => `sha256:${fill.repeat(64)}` as ArtifactDigest;
const id = <Value extends string>(prefix: string, digit: string): Value => `${prefix}_${digit.repeat(26)}` as Value;

function fixture(): { delivery: NativeDeliveryEnvelope; fence: DeliveryFence; permit: ConsumePermit } {
  const delivery: NativeDeliveryEnvelope = {
    protocolVersion: version,
    deliveryId: `dlv_${token}` as DeliveryId,
    attempt: 1,
    messageId: `msg_${token}` as MessageId,
    target: { kind: "channel", channelId: `chn_${token}` as ChannelId },
    serverSeq: 1,
    producerFactId: `fac_${token}` as ProducerFactId,
    agentId: `agt_${token}` as AgentId,
    machineId: `mch_${token}` as MachineId,
    expectedLaunchId: `lnc_${token}` as LaunchId,
    membershipEpoch: 1,
    routingGeneration: 1,
    routeVersion: 1,
  };
  const fence: DeliveryFence = {
    protocolVersion: version,
    deliveryId: delivery.deliveryId,
    attempt: 1,
    producerFactId: delivery.producerFactId,
    agentId: delivery.agentId,
    machineId: delivery.machineId,
    launchId: delivery.expectedLaunchId,
    membershipEpoch: 1,
    routingGeneration: 1,
    routeVersion: 1,
    sessionId: id<SessionId>("ses", "2"),
    turnId: id<TurnId>("trn", "3"),
  };
  return {
    delivery,
    fence,
    permit: {
      ...fence,
      permitId: id<CommandId>("cmd", "4"),
      invocationGeneration: 1,
      invocationId: id<CommandId>("cmd", "5"),
      body: "private-current-body",
    },
  };
}

test("SQLite journal persists a canonical identifier-only recovery chain", async () => {
  const directory = await mkdtemp(join(tmpdir(), "swarm-daemon-app-"));
  const path = join(directory, "native.sqlite");
  try {
    const { delivery, fence, permit } = fixture();
    const journal = new NativeSqliteJournal(path, "scripted_fake");
    await journal.recordDelivery(delivery, fence);
    await journal.recordPermit(permit);
    await assert.rejects(
      journal.recordPermit({ ...permit, body: "changed-private-body" }),
      /IDEMPOTENCY_CONFLICT/u,
    );
    const started = await journal.recordWriteStarted({
      fence,
      invocation: { invocationGeneration: 1, invocationId: permit.invocationId },
      permitId: permit.permitId,
      inputDigest: digest("6"),
    });
    assert.equal(started.sequence, 2);
    await assert.rejects(
      journal.recordWriteStarted({
        fence,
        invocation: { invocationGeneration: 1, invocationId: permit.invocationId },
        permitId: permit.permitId,
        inputDigest: digest("9"),
      }),
      /WRITE_STARTED_BINDING_MISMATCH/u,
    );
    await journal.recordInputWritten({
      fence,
      invocation: { invocationGeneration: 1, invocationId: permit.invocationId },
      permitId: permit.permitId,
      runtimeWriteId: id<CommandId>("cmd", "7"),
    });
    await journal.recordModelVisible({
      fence,
      invocation: { invocationGeneration: 1, invocationId: permit.invocationId },
      permitId: permit.permitId,
      runtimeWriteId: id<CommandId>("cmd", "7"),
      visibilityEventId: id<CommandId>("cmd", "8"),
    });
    const recovery = await journal.recoveryFor(fence);
    assert.equal(recovery?.kind, "reconcile");
    if (recovery?.kind === "reconcile") {
      assert.equal(recovery.command.evidence.kind, "model_visible");
      assert.equal(recovery.command.evidenceDigest.startsWith("sha256:"), true);
    }
    journal.close();

    const bytes = await readFile(path);
    assert.equal(bytes.includes(Buffer.from(permit.body)), false);

    const reopened = new NativeSqliteJournal(path, "scripted_fake");
    const reopenedRecovery = await reopened.recoveryFor(fence);
    assert.equal(reopenedRecovery?.kind, "reconcile");
    await reopened.markConsumed(fence, {
      invocationGeneration: 1,
      invocationId: permit.invocationId,
    });
    assert.deepEqual(await reopened.recoveryFor(fence), { kind: "consumed" });
    reopened.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

class EchoHandler implements NativeServerPort {
  readonly permit: ConsumePermit;

  constructor(permit: ConsumePermit) {
    this.permit = permit;
  }
  async acquireConsumePermit(_command: AcquireConsumePermit): Promise<ConsumePermit> { return this.permit; }
  async resumeConsumePermit(_command: ResumeConsumePermit): Promise<ConsumePermit> { return this.permit; }
  async beginNativeWrite(_command: BeginNativeWrite): Promise<void> {}
  async acknowledgeDelivery(command: DeliveryAck): Promise<DeliveryAckResult> {
    return command.boundary === "input_written"
      ? {
          boundary: "input_written",
          receiptId: id<ReceiptId>("rcp", "7"),
          invocation: { invocationGeneration: command.invocationGeneration, invocationId: command.invocationId },
          jobState: "held/INPUT_WRITTEN",
        }
      : {
          boundary: "model_visible",
          receiptId: id<ReceiptId>("rcp", "8"),
          invocation: { invocationGeneration: command.invocationGeneration, invocationId: command.invocationId },
          jobState: "acked/MODEL_VISIBLE",
        };
  }
  async reconcileDeliveryAttempt(_command: ReconcileDeliveryAttempt): Promise<ReconcileDeliveryResult> {
    return { kind: "pre_permit_requeued", jobState: "pending", replayOfAttempt: 1, nextAttempt: 2 };
  }
  async appendReply(_command: ReplyCommitCommand): Promise<ReplyCommitResult> {
    return {
      replyMessageId: id<MessageId>("msg", "8"),
      receiptId: id<ReceiptId>("rcp", "9"),
      causalOrder: 1,
    };
  }
  async createTask(_command: TaskCommitCommand): Promise<TaskCommitResult> {
    return {
      taskId: id<TaskId>("tsk", "8"),
      receiptId: id<ReceiptId>("rcp", "a"),
      causalOrder: 2,
    };
  }
}

test("loopback WebSocket disconnect is explicit and the next command reconnects", async () => {
  const { fence, permit } = fixture();
  const server = new LoopbackNativeServer(new EchoHandler(permit));
  const connection = new LoopbackServerConnection(await server.start());
  const acquire = {
    ...fence,
    commandId: id<CommandId>("cmd", "a"),
    requestDigest: digest("b"),
    boundary: "daemon_accepted" as const,
  };
  try {
    server.disconnectBeforeNext("acquireConsumePermit");
    await assert.rejects(
      connection.acquireConsumePermit(acquire),
      (error: unknown) => error instanceof PrePermitDisconnectError,
    );
    const replay = await connection.acquireConsumePermit(acquire);
    assert.equal(replay.permitId, permit.permitId);
    assert.equal(replay.body, permit.body);
  } finally {
    connection.close();
    await server.close();
  }
});

test("in-process composition runs the native path through WebSocket and both SQLite journals", async () => {
  const directory = await mkdtemp(join(tmpdir(), "swarm-daemon-composition-"));
  const { delivery, fence, permit } = fixture();
  const manifest: StandingManifest = {
    protocolVersion: version,
    agentId: delivery.agentId,
    runtime: "codex",
    workspaceGeneration: 1,
    identityDigest: digest("1"),
    memoryDigest: digest("2"),
    cliContractDigest: digest("3"),
    capabilityDigest: digest("4"),
  };
  const driver = new ScriptedDriver([{
    kind: "written",
    runtimeWriteId: id<CommandId>("cmd", "7"),
    visibilityEventId: id<CommandId>("cmd", "8"),
    events: [
      { kind: "assistant_reply", text: "Native path complete." },
      { kind: "turn_complete" },
    ],
  }]);
  let app: Awaited<ReturnType<typeof createInProcessLoopbackDaemon>> | undefined;
  try {
    app = await createInProcessLoopbackDaemon({
      sqlitePath: join(directory, "native.sqlite"),
      waveZeroSqlitePath: join(directory, "wave-zero.sqlite"),
      driver,
      handler: new EchoHandler(permit),
    });
    const result = await app.core.runTurn({
      delivery,
      standingManifest: manifest,
      attention: [],
      sessionId: fence.sessionId,
      turnId: fence.turnId,
    });
    assert.equal(result.kind, "completed");
    assert.equal(driver.writes.length, 1);
    assert.equal(driver.writes[0]?.turn.input.current.body, permit.body);
    assert.deepEqual(await app.journal.recoveryFor(fence), { kind: "consumed" });
    assert.deepEqual(app.waveZeroJournal.listRecoveryEvidence(), []);
  } finally {
    if (app !== undefined) await closeDaemonApp(app);
    await rm(directory, { recursive: true, force: true });
  }
});
