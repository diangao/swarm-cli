import assert from "node:assert/strict";
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
  InputWrittenJournalEntry,
  InvocationJournalEntry,
  LaunchId,
  MachineId,
  MessageId,
  ModelVisibleJournalEntry,
  NativeDeliveryEnvelope,
  NativeInvocationFence,
  NativeRuntimeEvent,
  ProducerFactId,
  ProtocolVersion,
  ReceiptId,
  ReconcileDeliveryAttempt,
  ReconcileDeliveryResult,
  ResumeConsumePermit,
  ScriptedNotWrittenProof,
  SessionId,
  StandingManifest,
  TaskId,
  TurnId,
  WriteStartedJournalEntry,
} from "@swarm/protocol";
import type {
  NativeRuntimePort,
  NativeWriteBinding,
  NativeWriteOutcome,
} from "@swarm/drivers";
import { NativeEventError } from "@swarm/drivers";
import { ScriptedDriver } from "@swarm/drivers/testing";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

import {
  DaemonCore,
  PrePermitDisconnectError,
  type JournalRecovery,
  type NativeCommandIdSource,
  type NativeJournalPort,
  type NativeServerPort,
  type ReplyCommitCommand,
  type ReplyCommitResult,
  type TaskCommitCommand,
  type TaskCommitResult,
} from "../src/index.js";

const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
const version = 1 as ProtocolVersion;
const digest = (fill: string): ArtifactDigest => `sha256:${fill.repeat(64)}` as ArtifactDigest;

function opaque(prefix: string, value: number): string {
  return `${prefix}_${alphabet[value % alphabet.length]?.repeat(26)}`;
}

class Ids implements NativeCommandIdSource {
  #next = 1;
  nextCommandId(): CommandId {
    return opaque("cmd", this.#next++) as CommandId;
  }
}

function fixtureDelivery(): NativeDeliveryEnvelope {
  return {
    protocolVersion: version,
    deliveryId: opaque("dlv", 1) as DeliveryId,
    attempt: 1,
    messageId: opaque("msg", 1) as MessageId,
    target: { kind: "channel", channelId: opaque("chn", 1) as ChannelId },
    serverSeq: 1,
    producerFactId: opaque("fac", 1) as ProducerFactId,
    agentId: opaque("agt", 1) as AgentId,
    machineId: opaque("mch", 1) as MachineId,
    expectedLaunchId: opaque("lnc", 1) as LaunchId,
    membershipEpoch: 1,
    routingGeneration: 2,
    routeVersion: 3,
  };
}

function fixtureManifest(): StandingManifest {
  return {
    protocolVersion: version,
    agentId: opaque("agt", 1) as AgentId,
    runtime: "codex",
    workspaceGeneration: 1,
    identityDigest: digest("1"),
    memoryDigest: digest("2"),
    cliContractDigest: digest("3"),
    capabilityDigest: digest("4"),
  };
}

class QueueDriver implements NativeRuntimePort {
  readonly driverKind = "scripted_fake" as const;
  readonly writes: Array<{ turn: CompiledNativeTurn; binding: NativeWriteBinding }> = [];
  readonly #outcomes: NativeWriteOutcome[];

  constructor(outcomes: readonly NativeWriteOutcome[]) {
    this.#outcomes = [...outcomes];
  }

  async writeTurn(turn: CompiledNativeTurn, binding: NativeWriteBinding): Promise<NativeWriteOutcome> {
    this.writes.push({ turn, binding });
    return this.#outcomes.shift() ?? { kind: "ambiguous" };
  }
}

async function* events(values: readonly NativeRuntimeEvent[]): AsyncIterable<NativeRuntimeEvent> {
  for (const value of values) yield value;
}

class MemoryJournal implements NativeJournalPort {
  readonly log: string[] = [];
  recovery: JournalRecovery | null = null;
  permit?: ConsumePermit;
  permitRecorded?: InvocationJournalEntry<"permit_recorded">;
  writeStarted?: WriteStartedJournalEntry;
  inputWritten?: InputWrittenJournalEntry;
  modelVisible?: ModelVisibleJournalEntry;
  proof?: ScriptedNotWrittenProof;
  #sequence = 0;

  async recordDelivery(_delivery: NativeDeliveryEnvelope, _fence: DeliveryFence): Promise<void> {
    this.log.push("delivery");
  }

  async recoveryFor(fence: DeliveryFence): Promise<JournalRecovery | null> {
    if (this.recovery !== null) return this.recovery;
    if (
      this.permit !== undefined
      && this.permitRecorded !== undefined
      && this.writeStarted !== undefined
      && this.modelVisible !== undefined
    ) {
      const evidence = {
        kind: "model_visible" as const,
        permitRecorded: this.permitRecorded,
        writeStarted: this.writeStarted,
        inputWritten: this.inputWritten as InputWrittenJournalEntry,
        modelVisible: this.modelVisible,
      };
      return {
        kind: "reconcile",
        command: {
          ...fence,
          commandId: opaque("cmd", 27) as CommandId,
          requestDigest: digest("a"),
          permitId: this.permit.permitId,
          invocation: {
            invocationGeneration: this.permit.invocationGeneration,
            invocationId: this.permit.invocationId,
          },
          evidenceDigest: digest("b"),
          evidence,
        },
      };
    }
    if (
      this.permit !== undefined
      && this.permitRecorded !== undefined
      && this.writeStarted !== undefined
      && this.inputWritten !== undefined
    ) {
      const evidence = {
        kind: "input_written" as const,
        permitRecorded: this.permitRecorded,
        writeStarted: this.writeStarted,
        inputWritten: this.inputWritten,
      };
      return {
        kind: "reconcile",
        command: {
          ...fence,
          commandId: opaque("cmd", 26) as CommandId,
          requestDigest: digest("a"),
          permitId: this.permit.permitId,
          invocation: {
            invocationGeneration: this.permit.invocationGeneration,
            invocationId: this.permit.invocationId,
          },
          evidenceDigest: digest("b"),
          evidence,
        },
      };
    }
    if (this.proof !== undefined && this.permit !== undefined && this.permitRecorded !== undefined && this.writeStarted !== undefined) {
      const evidence = {
        kind: "scripted_not_written" as const,
        permitRecorded: this.permitRecorded,
        writeStarted: this.writeStarted,
        proof: this.proof,
      };
      return {
        kind: "reconcile",
        command: {
          ...fence,
          commandId: opaque("cmd", 25) as CommandId,
          requestDigest: digest("a"),
          permitId: this.permit.permitId,
          invocation: {
            invocationGeneration: this.permit.invocationGeneration,
            invocationId: this.permit.invocationId,
          },
          evidenceDigest: digest("b"),
          evidence,
        },
      };
    }
    return null;
  }

  async recordPrePermitDisconnect(fence: DeliveryFence, disconnectId: CommandId): Promise<void> {
    this.log.push("pre_permit_disconnect");
    this.recovery = {
      kind: "reconcile",
      command: {
        ...fence,
        commandId: opaque("cmd", 24) as CommandId,
        requestDigest: digest("a"),
        permitId: null,
        invocation: null,
        evidenceDigest: digest("b"),
        evidence: { kind: "pre_permit_disconnect", disconnectId },
      },
    };
  }

  async recordPermit(permit: ConsumePermit): Promise<InvocationJournalEntry<"permit_recorded">> {
    this.permit = permit;
    this.log.push(`permit:${permit.invocationGeneration}`);
    this.permitRecorded = this.entry("permit_recorded", permit, permit, permit.invocationGeneration);
    return this.permitRecorded;
  }

  async recordWriteStarted(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    inputDigest: ArtifactDigest;
  }): Promise<WriteStartedJournalEntry> {
    const entry: WriteStartedJournalEntry = {
      ...this.entry("write_started", input.fence, { permitId: input.permitId }, input.invocation.invocationGeneration, input.invocation.invocationId),
      inputDigest: input.inputDigest,
    };
    this.writeStarted = entry;
    this.log.push(`write_started:${input.invocation.invocationGeneration}`);
    return entry;
  }

  async recordNotWritten(proof: ScriptedNotWrittenProof): Promise<void> {
    this.proof = proof;
    this.log.push(`not_written:${proof.invocationGeneration}`);
  }

  async recordInputWritten(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    runtimeWriteId: CommandId;
  }): Promise<InputWrittenJournalEntry> {
    this.log.push("input_written");
    this.inputWritten = {
      ...this.entry("input_written", input.fence, { permitId: input.permitId }, input.invocation.invocationGeneration, input.invocation.invocationId),
      runtimeWriteId: input.runtimeWriteId,
    };
    return this.inputWritten;
  }

  async recordModelVisible(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    runtimeWriteId: CommandId;
    visibilityEventId: CommandId;
  }): Promise<ModelVisibleJournalEntry> {
    this.log.push("model_visible");
    this.modelVisible = {
      ...this.entry("model_visible", input.fence, { permitId: input.permitId }, input.invocation.invocationGeneration, input.invocation.invocationId),
      runtimeWriteId: input.runtimeWriteId,
      visibilityEventId: input.visibilityEventId,
    };
    return this.modelVisible;
  }

  async recordSuppressed(_fence: DeliveryFence, reason: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME"): Promise<void> {
    this.log.push(`suppressed:${reason}`);
  }

  async markAmbiguous(_fence: DeliveryFence, invocation: NativeInvocationFence): Promise<void> {
    this.log.push(`ambiguous:${invocation.invocationGeneration}`);
  }

  async markConsumed(_fence: DeliveryFence, _invocation: NativeInvocationFence): Promise<void> {
    this.log.push("consumed");
  }

  entry<K extends string>(
    kind: K,
    fence: DeliveryFence,
    permit: { permitId: CommandId },
    generation: number,
    invocationId = this.permit?.invocationId ?? opaque("cmd", 19) as CommandId,
  ): InvocationJournalEntry<K> {
    this.#sequence += 1;
    return {
      ...fence,
      journalId: opaque("cmd", 18) as CommandId,
      entryId: opaque("cmd", 20 + this.#sequence) as CommandId,
      sequence: this.#sequence,
      kind,
      previousEntryDigest: this.#sequence === 1 ? null : digest("c"),
      entryDigest: digest("d"),
      invocationGeneration: generation,
      invocationId,
      permitId: permit.permitId,
    };
  }
}

class FakeServer implements NativeServerPort {
  readonly log: string[] = [];
  readonly replies: ReplyCommitCommand[] = [];
  readonly tasks: TaskCommitCommand[] = [];
  body = "What is the answer?";
  acquireFailure?: Error;
  ackFailureOnce: "input_written" | "model_visible" | undefined;
  malformedAckOnce = false;
  malformedReconcileOnce = false;

  permit(command: AcquireConsumePermit | ResumeConsumePermit, generation: number): ConsumePermit {
    return {
      protocolVersion: command.protocolVersion,
      deliveryId: command.deliveryId,
      attempt: command.attempt,
      producerFactId: command.producerFactId,
      agentId: command.agentId,
      machineId: command.machineId,
      launchId: command.launchId,
      membershipEpoch: command.membershipEpoch,
      routingGeneration: command.routingGeneration,
      routeVersion: command.routeVersion,
      sessionId: command.sessionId,
      turnId: command.turnId,
      permitId: opaque("cmd", 10) as CommandId,
      invocationGeneration: generation,
      invocationId: opaque("cmd", 10 + generation) as CommandId,
      body: this.body,
    };
  }

  async acquireConsumePermit(command: AcquireConsumePermit): Promise<ConsumePermit> {
    this.log.push("acquire");
    if (this.acquireFailure !== undefined) throw this.acquireFailure;
    return this.permit(command, 1);
  }

  async resumeConsumePermit(command: ResumeConsumePermit): Promise<ConsumePermit> {
    this.log.push(`resume:${command.expectedActiveInvocationGeneration}`);
    return this.permit(
      command,
      command.resumeMode === "next_after_not_written"
        ? command.expectedActiveInvocationGeneration + 1
        : command.expectedActiveInvocationGeneration,
    );
  }

  async beginNativeWrite(_command: BeginNativeWrite): Promise<void> {
    this.log.push("begin");
  }

  async acknowledgeDelivery(command: DeliveryAck): Promise<DeliveryAckResult> {
    this.log.push(`ack:${command.boundary}`);
    if (this.malformedAckOnce) {
      this.malformedAckOnce = false;
      const invocation = {
        invocationGeneration: command.invocationGeneration,
        invocationId: command.invocationId,
      };
      return command.boundary === "input_written"
        ? { boundary: "input_written", receiptId: "invalid" as ReceiptId, invocation, jobState: "held/INPUT_WRITTEN" }
        : { boundary: "model_visible", receiptId: "invalid" as ReceiptId, invocation, jobState: "acked/MODEL_VISIBLE" };
    }
    if (this.ackFailureOnce === command.boundary) {
      this.ackFailureOnce = undefined;
      throw new Error("LOST_ACK_RESPONSE");
    }
    return command.boundary === "input_written"
      ? {
          boundary: "input_written",
          receiptId: opaque("rcp", 1) as ReceiptId,
          invocation: { invocationGeneration: command.invocationGeneration, invocationId: command.invocationId },
          jobState: "held/INPUT_WRITTEN",
        }
      : {
          boundary: "model_visible",
          receiptId: opaque("rcp", 2) as ReceiptId,
          invocation: { invocationGeneration: command.invocationGeneration, invocationId: command.invocationId },
          jobState: "acked/MODEL_VISIBLE",
        };
  }

  async reconcileDeliveryAttempt(command: ReconcileDeliveryAttempt): Promise<ReconcileDeliveryResult> {
    this.log.push(`reconcile:${command.evidence.kind}`);
    if (this.malformedReconcileOnce) {
      this.malformedReconcileOnce = false;
      return { kind: "boundary_repaired", repaired: [], jobState: "acked/MODEL_VISIBLE" } as unknown as ReconcileDeliveryResult;
    }
    if (command.evidence.kind === "pre_permit_disconnect") {
      return { kind: "pre_permit_requeued", jobState: "pending", replayOfAttempt: 1, nextAttempt: 2 };
    }
    if (command.evidence.kind === "input_written") {
      return {
        kind: "boundary_repaired",
        repaired: ["input_written"],
        jobState: "held/INPUT_WRITTEN",
        attempt: command.attempt,
        permitId: command.permitId as CommandId,
        invocation: command.invocation as NativeInvocationFence,
      };
    }
    if (command.evidence.kind === "model_visible") {
      return {
        kind: "boundary_repaired",
        repaired: ["input_written", "model_visible"],
        jobState: "acked/MODEL_VISIBLE",
        attempt: command.attempt,
        permitId: command.permitId as CommandId,
        invocation: command.invocation as NativeInvocationFence,
      };
    }
    assert.notEqual(command.permitId, null);
    assert.notEqual(command.invocation, null);
    return {
      kind: "same_attempt_resumable",
      jobState: "held/CONSUME_PERMITTED",
      attempt: 1,
      permitId: command.permitId as CommandId,
      resumeMode: command.evidence.kind === "scripted_not_written"
        ? "next_after_not_written"
        : "same_invocation_before_begin",
      expectedActiveInvocationGeneration: command.invocation?.invocationGeneration ?? 1,
      nextInvocationGeneration: command.evidence.kind === "scripted_not_written"
        ? (command.invocation?.invocationGeneration ?? 1) + 1
        : command.invocation?.invocationGeneration ?? 1,
    };
  }

  async appendReply(command: ReplyCommitCommand): Promise<ReplyCommitResult> {
    assert.ok(this.log.includes("ack:model_visible"));
    this.log.push("reply");
    this.replies.push(command);
    return {
      replyMessageId: opaque("msg", 2) as MessageId,
      receiptId: opaque("rcp", 3) as ReceiptId,
      causalOrder: 5,
    };
  }

  async createTask(command: TaskCommitCommand): Promise<TaskCommitResult> {
    assert.ok(this.log.includes("reply"));
    this.log.push("task");
    this.tasks.push(command);
    return {
      taskId: opaque("tsk", 1) as TaskId,
      receiptId: opaque("rcp", 4) as ReceiptId,
      causalOrder: 6,
    };
  }
}

function core(server: FakeServer, journal: MemoryJournal, driver: NativeRuntimePort): DaemonCore {
  return new DaemonCore({ server, journal, driver, ids: new Ids() });
}

function request() {
  return {
    delivery: fixtureDelivery(),
    standingManifest: fixtureManifest(),
    attention: [],
    sessionId: opaque("ses", 1) as SessionId,
    turnId: opaque("trn", 1) as TurnId,
  };
}

function written(eventList: readonly NativeRuntimeEvent[]): NativeWriteOutcome {
  return {
    kind: "written",
    runtimeWriteId: opaque("cmd", 14) as CommandId,
    visibilityEventId: opaque("cmd", 15) as CommandId,
    events: events(eventList),
  };
}

test("pure question is native-first and commits one reply with zero tasks", async () => {
  const server = new FakeServer();
  const journal = new MemoryJournal();
  const driver = new QueueDriver([written([
    { kind: "assistant_reply", text: "The answer is 42." },
    { kind: "turn_complete" },
  ])]);
  const result = await core(server, journal, driver).runTurn(request());
  assert.equal(result.kind, "completed");
  assert.equal(server.replies.length, 1);
  assert.equal(server.tasks.length, 0);
  assert.equal(driver.writes.length, 1);
  assert.equal(driver.writes[0]?.turn.input.current.body, server.body);
  assert.ok(server.log.indexOf("ack:model_visible") < server.log.indexOf("reply"));
  assert.equal(journal.log.at(-1), "consumed");
});

test("execution request commits reply before its one source-bound task", async () => {
  const server = new FakeServer();
  const journal = new MemoryJournal();
  const delivery = fixtureDelivery();
  const driver = new QueueDriver([written([
    { kind: "assistant_reply", text: "I will do that." },
    {
      kind: "coordination_call",
      commandId: opaque("cmd", 16) as CommandId,
      command: {
        protocolVersion: version,
        title: "Do the work",
        sourceMessageId: delivery.messageId,
      },
    },
    { kind: "turn_complete" },
  ])]);
  const result = await core(server, journal, driver).runTurn({ ...request(), delivery });
  assert.equal(result.kind, "completed");
  assert.deepEqual(server.log.filter((item) => item === "reply" || item === "task"), ["reply", "task"]);
  assert.equal(server.tasks[0]?.command.sourceMessageId, delivery.messageId);
});

test("privacy suppression occurs before body compilation or runtime write", async () => {
  for (const reason of ["MEMBERSHIP_REVOKED_BEFORE_CONSUME", "ROUTE_SUPERSEDED_BEFORE_CONSUME"] as const) {
    const server = new FakeServer();
    server.acquireFailure = new Error(reason);
    const journal = new MemoryJournal();
    const driver = new QueueDriver([]);
    const result = await core(server, journal, driver).runTurn(request());
    assert.deepEqual(result, { kind: "suppressed", reason });
    assert.equal(driver.writes.length, 0);
    assert.equal(journal.log.some((entry) => entry.includes(server.body)), false);
    assert.equal(server.replies.length, 0);
    assert.equal(server.tasks.length, 0);
  }
});

test("lost boundary ACK responses reconcile before any reply", async () => {
  for (const boundary of ["input_written", "model_visible"] as const) {
    const server = new FakeServer();
    server.ackFailureOnce = boundary;
    const journal = new MemoryJournal();
    const driver = new QueueDriver([written([
      { kind: "assistant_reply", text: `Recovered ${boundary}.` },
      { kind: "turn_complete" },
    ])]);

    const result = await core(server, journal, driver).runTurn(request());

    assert.equal(result.kind, "completed");
    assert.equal(server.replies.length, 1);
    assert.equal(server.tasks.length, 0);
    assert.ok(server.log.indexOf(`reconcile:${boundary}`) < server.log.indexOf("reply"));
    assert.equal(journal.log.at(-1), "consumed");
  }
});

test("malformed ACK and reconciliation responses fail closed before output", async () => {
  for (const malformed of ["ack", "reconcile"] as const) {
    const server = new FakeServer();
    if (malformed === "ack") server.malformedAckOnce = true;
    else {
      server.ackFailureOnce = "input_written";
      server.malformedReconcileOnce = true;
    }
    const driver = new QueueDriver([written([
      { kind: "assistant_reply", text: "Must not commit." },
      { kind: "turn_complete" },
    ])]);

    await assert.rejects(core(server, new MemoryJournal(), driver).runTurn(request()));
    assert.equal(server.replies.length, 0);
    assert.equal(server.tasks.length, 0);
  }
});

test("malformed coordination streams commit neither a reply nor a task", async () => {
  const beforeServer = new FakeServer();
  const delivery = fixtureDelivery();
  const beforeDriver = new QueueDriver([written([
    {
      kind: "coordination_call",
      commandId: opaque("cmd", 16) as CommandId,
      command: { protocolVersion: version, title: "bad", sourceMessageId: delivery.messageId },
    },
  ])]);
  await assert.rejects(
    core(beforeServer, new MemoryJournal(), beforeDriver).runTurn({ ...request(), delivery }),
    (error: unknown) => error instanceof NativeEventError && error.code === "COORDINATION_BEFORE_REPLY",
  );
  assert.equal(beforeServer.replies.length, 0);
  assert.equal(beforeServer.tasks.length, 0);

  const secondServer = new FakeServer();
  const secondDriver = new QueueDriver([written([
    { kind: "assistant_reply", text: "reply" },
    {
      kind: "coordination_call",
      commandId: opaque("cmd", 16) as CommandId,
      command: { protocolVersion: version, title: "first", sourceMessageId: delivery.messageId },
    },
    {
      kind: "coordination_call",
      commandId: opaque("cmd", 17) as CommandId,
      command: { protocolVersion: version, title: "second", sourceMessageId: delivery.messageId },
    },
  ])]);
  await assert.rejects(
    core(secondServer, new MemoryJournal(), secondDriver).runTurn({ ...request(), delivery }),
    (error: unknown) => error instanceof NativeEventError && error.code === "SECOND_COORDINATION_CALL",
  );
  assert.equal(secondServer.replies.length, 0);
  assert.equal(secondServer.tasks.length, 0);
});

test("pre-permit disconnect requeues; proven not-written resumes same attempt at generation two", async () => {
  const disconnected = new FakeServer();
  disconnected.acquireFailure = new PrePermitDisconnectError();
  const result = await core(disconnected, new MemoryJournal(), new QueueDriver([])).runTurn(request());
  assert.deepEqual(result, { kind: "requeued", nextAttempt: 2 });
  assert.deepEqual(disconnected.log, ["acquire", "reconcile:pre_permit_disconnect"]);

  const server = new FakeServer();
  const journal = new MemoryJournal();
  const driver = new ScriptedDriver([
    {
      kind: "not_written",
      fixtureId: opaque("cmd", 20) as CommandId,
      scriptDigest: digest("5"),
      outcomeOrdinal: 1,
    },
    {
      kind: "written",
      runtimeWriteId: opaque("cmd", 14) as CommandId,
      visibilityEventId: opaque("cmd", 15) as CommandId,
      events: [{ kind: "assistant_reply", text: "Recovered." }, { kind: "turn_complete" }],
    },
  ]);
  const resumed = await core(server, journal, driver).runTurn(request());
  assert.equal(resumed.kind, "completed");
  assert.equal(driver.writes.length, 2);
  assert.deepEqual(driver.writes.map((write) => write.binding.invocationGeneration), [1, 2]);
  assert.equal(fixtureDelivery().attempt, 1);
  assert.ok(server.log.includes("resume:1"));
});
