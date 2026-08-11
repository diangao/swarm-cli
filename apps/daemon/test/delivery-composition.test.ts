import assert from "node:assert/strict";
import { test } from "node:test";

import {
  buildContributionBinding,
  canonicalProtocolJson,
  type AgentId,
  type ArtifactDigest,
  type ChannelId,
  type CommandId,
  type ContributionBinding,
  type DeliveryId,
  type LaunchId,
  type MachineId,
  type MessageId,
  type NativeDeliveryEnvelope,
  type NativeInvocationFence,
  type ProducerFactId,
  type ProtocolVersion,
  type ReceiptId,
  type SessionId,
  type StateInstanceId,
  type TurnId,
} from "@swarm/protocol";
import type {
  DeliveryClock,
  DeliveryExecutionPort,
  DeliveryJournalPort,
  DeliveryServerCommitPort,
} from "@swarm/daemon-core";
import { protocolDigest } from "@swarm/runtime-contract";

import { createDeliveryKernel } from "../src/index.js";
import {
  AppDeliveryCommandIdDerivation,
  deterministicCommandId,
} from "../src/ids.js";

const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
const version = 1 as ProtocolVersion;
const launchId = opaque("lnc", 1) as LaunchId;
const stateInstanceId = opaque("sti", 1) as StateInstanceId;
const sessionId = opaque("ses", 1) as SessionId;
const agentId = opaque("agt", 1) as AgentId;
const machineId = opaque("mch", 1) as MachineId;
const replyText = "private reply text that never enters the command seed or journal";

type DeliveryActivation = Parameters<DeliveryJournalPort["pendingFor"]>[0];
type PendingDelivery = Awaited<ReturnType<DeliveryJournalPort["pendingFor"]>>[number];
type VisibleMessageRecord = Exclude<
  Awaited<ReturnType<DeliveryJournalPort["findVisible"]>>,
  null
>;
type DeliveryExecutionInput = Parameters<DeliveryExecutionPort["execute"]>[0];
type DeliveryExecutionResult = Awaited<ReturnType<DeliveryExecutionPort["execute"]>>;
type VisibleExecutionResult = Extract<DeliveryExecutionResult, { kind: "model_visible" }>;
type TerminalPort = VisibleExecutionResult["terminal"];
type TerminalStage = ReturnType<TerminalPort["terminalDraft"]>;
type TerminalResult = ReturnType<TerminalPort["terminalResult"]>;
type CommitTurnTerminalInput = Parameters<DeliveryJournalPort["commitTurnTerminal"]>[0];
type TurnStepResult = ReturnType<DeliveryJournalPort["commitTurnTerminal"]>;

function opaque(prefix: string, value: number): string {
  return `${prefix}_${alphabet[value % alphabet.length]?.repeat(26)}`;
}

function digest(fill: string): ArtifactDigest {
  return `sha256:${fill.repeat(64)}` as ArtifactDigest;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const nested of Object.values(value as Record<string, unknown>)) {
      deepFreeze(nested);
    }
    Object.freeze(value);
  }
  return value;
}

function delivery(): NativeDeliveryEnvelope {
  return {
    protocolVersion: version,
    deliveryId: opaque("dlv", 2) as DeliveryId,
    attempt: 1,
    messageId: opaque("msg", 2) as MessageId,
    target: {
      kind: "channel",
      channelId: opaque("chn", 2) as ChannelId,
    },
    serverSeq: 1,
    producerFactId: opaque("fac", 2) as ProducerFactId,
    agentId,
    machineId,
    expectedLaunchId: launchId,
    membershipEpoch: 1,
    routingGeneration: 1,
    routeVersion: 1,
  };
}

function pending(record: NativeDeliveryEnvelope): PendingDelivery {
  return {
    delivery: record,
    stateInstanceId,
    sessionId,
    turnId: opaque("trn", 2) as TurnId,
    targetKey: `channel:${record.target.kind === "channel" ? record.target.channelId : ""}:`,
    receiveOrdinal: 1,
    state: "pending",
    attention: [],
  };
}

function activation(): DeliveryActivation {
  return {
    agentId,
    machineId,
    launchId,
    stateInstanceId,
    sessionId,
    localState: "activated",
    serverState: "activated",
  };
}

function invocation(): NativeInvocationFence {
  return {
    invocationGeneration: 1,
    invocationId: opaque("cmd", 6) as CommandId,
  };
}

function contribution(
  input: DeliveryExecutionInput,
  fencedInvocation: NativeInvocationFence,
): ContributionBinding {
  return buildContributionBinding({
    fence: input.fence,
    stateInstanceId: input.stateInstanceId,
    inputOrdinal: 1,
    invocationId: fencedInvocation.invocationId,
    invocationGeneration: fencedInvocation.invocationGeneration,
    permitId: opaque("cmd", 7) as CommandId,
    runtimeWriteId: opaque("cmd", 8) as CommandId,
    visibilityEventId: opaque("cmd", 9) as CommandId,
  });
}

class FixedTerminalPort implements TerminalPort {
  readonly stage: TerminalStage;

  constructor(binding: ContributionBinding) {
    const bindingDigest = digest("9");
    this.stage = deepFreeze({
      contribution: binding,
      coordinationRequest: {
        kind: "not_requested",
        terminalTurnId: binding.fence.turnId,
      },
      basis: {
        reply: {
          ordinal: 3,
          eventDigest: digest("3"),
          turnId: binding.fence.turnId,
          bindingDigest,
        },
        completed: {
          ordinal: 4,
          eventDigest: digest("4"),
          turnId: binding.fence.turnId,
          bindingDigest,
        },
      },
      readerFence: {
        stateInstanceId: binding.stateInstanceId,
        sessionId: binding.fence.sessionId,
        ownerToken: digest("a"),
        readerEpoch: 1,
      },
      expected: {
        protocolTurnId: binding.fence.turnId,
        phase: "model_visible",
        inputOrdinal: binding.inputOrdinal,
        bindingDigest,
        steerable: true,
        replyCommitted: false,
      },
      next: {
        protocolTurnId: binding.fence.turnId,
        phase: "completed",
        inputOrdinal: binding.inputOrdinal,
        bindingDigest,
        steerable: false,
        replyCommitted: true,
      },
    });
  }

  terminalDraft(): TerminalStage {
    return this.stage;
  }

  bindCoordinationResult(
    _disposition: Parameters<TerminalPort["bindCoordinationResult"]>[0],
  ): TerminalResult {
    throw new Error("coordination is not requested");
  }

  terminalResult(): TerminalResult {
    return deepFreeze({
      contribution: this.stage.contribution,
      coordination: {
        kind: "not_requested",
        terminalTurnId: this.stage.contribution.fence.turnId,
      },
    });
  }
}

class RepairableExecution implements DeliveryExecutionPort {
  bodyReads = 0;
  driverWrites = 0;
  lastStage: TerminalStage | undefined;

  async execute(input: DeliveryExecutionInput): Promise<DeliveryExecutionResult> {
    if (input.mode.kind === "new_input") {
      this.bodyReads += 1;
      this.driverWrites += 1;
    }
    const fencedInvocation = invocation();
    const terminal = new FixedTerminalPort(contribution(input, fencedInvocation));
    this.lastStage = terminal.stage;
    const ackResult = {
      boundary: "model_visible" as const,
      receiptId: opaque("rcp", 3) as ReceiptId,
      invocation: fencedInvocation,
      jobState: "acked/MODEL_VISIBLE" as const,
    };
    return {
      kind: "model_visible",
      fence: input.fence,
      invocation: fencedInvocation,
      inputWritten: {
        entryId: opaque("cmd", 10) as CommandId,
        entryDigest: digest("1"),
        runtimeWriteId: opaque("cmd", 8) as CommandId,
      },
      modelVisible: {
        entryId: opaque("cmd", 11) as CommandId,
        entryDigest: digest("2"),
        runtimeWriteId: opaque("cmd", 8) as CommandId,
        visibilityEventId: opaque("cmd", 9) as CommandId,
      },
      modelVisibleAck: {
        observed: true,
        result: ackResult,
        resultDigest: protocolDigest(ackResult),
        disposition: "committed",
      },
      terminal,
      reply: { text: replyText },
    };
  }
}

class MemoryJournal implements DeliveryJournalPort {
  pending: PendingDelivery[];
  readonly visible = new Map<MessageId, VisibleMessageRecord>();
  readonly terminalInputs: CommitTurnTerminalInput[] = [];

  constructor(record: PendingDelivery) {
    this.pending = [record];
  }

  async acceptNotice(): Promise<"inserted"> {
    return "inserted";
  }

  async pendingFor(): Promise<readonly PendingDelivery[]> {
    return this.pending;
  }

  async findVisible(input: Parameters<DeliveryJournalPort["findVisible"]>[0]) {
    return this.visible.get(input.messageId) ?? null;
  }

  async compareNoticeVisibility(): Promise<"new"> {
    return "new";
  }

  async commitNoticeVisibility(): Promise<void> {}

  async commitVisibleMessage(
    input: Parameters<DeliveryJournalPort["commitVisibleMessage"]>[0],
  ): Promise<void> {
    this.visible.set(input.messageId, {
      deliveryId: input.deliveryId,
      attempt: input.attempt,
      serverSeq: input.serverSeq,
      modelVisibleReceiptId: input.modelVisibleAck.receiptId,
      visibleAt: input.visibleAt,
    });
  }

  async holdAmbiguous(): Promise<void> {
    throw new Error("unexpected ambiguous write");
  }

  commitTurnTerminal(input: CommitTurnTerminalInput): TurnStepResult {
    this.terminalInputs.push(input);
    this.pending = [];
    return {
      applied: true,
      durable: input.next,
      nextOrdinal: input.basis.completed.ordinal + 1,
      lastEventDigest: input.basis.completed.eventDigest,
    };
  }
}

class LostReplyResponseServer implements DeliveryServerCommitPort {
  readonly commandIds: CommandId[] = [];
  readonly dispositions: Array<"committed" | "terminal_replay"> = [];
  readonly #committed = new Set<CommandId>();

  async appendReply(input: Parameters<DeliveryServerCommitPort["appendReply"]>[0]) {
    assert.equal(input.text, replyText);
    const replay = this.#committed.has(input.commandId);
    this.#committed.add(input.commandId);
    this.commandIds.push(input.commandId);
    this.dispositions.push(replay ? "terminal_replay" : "committed");
    const result = {
      replyMessageId: opaque("msg", 12) as MessageId,
      receiptId: opaque("rcp", 12) as ReceiptId,
      causalOrder: 1,
    };
    if (!replay) throw new Error("reply response lost");
    return {
      result,
      resultDigest: protocolDigest(result),
      disposition: "terminal_replay" as const,
    };
  }

  async applyCoordination(): ReturnType<DeliveryServerCommitPort["applyCoordination"]> {
    throw new Error("coordination is not requested");
  }
}

class FixedClock implements DeliveryClock {
  #tick = 0;

  now(): string {
    this.#tick += 1;
    return `2026-08-11T08:00:${String(this.#tick).padStart(2, "0")}.000Z`;
  }
}

type ReplySeedFields = {
  contributionBindingDigest: ArtifactDigest;
  ordinal: number;
  eventDigest: ArtifactDigest;
  turnId: TurnId;
  bindingDigest: ArtifactDigest;
};

function replySeed(fields: ReplySeedFields): string {
  return new TextDecoder().decode(canonicalProtocolJson({
    kind: "turn_reply_v1",
    ...fields,
  }));
}

test("delivery command IDs delegate the opaque seed and change with every seed field", () => {
  const subject = new AppDeliveryCommandIdDerivation();
  const fields: ReplySeedFields = {
    contributionBindingDigest: digest("1"),
    ordinal: 3,
    eventDigest: digest("2"),
    turnId: opaque("trn", 3) as TurnId,
    bindingDigest: digest("3"),
  };
  const seed = replySeed(fields);
  const expected = deterministicCommandId(seed);

  assert.equal(subject.deterministicCommandId(seed), expected);
  assert.equal(seed.includes(replyText), false);
  assert.equal(seed.includes("text"), false);

  const mutations: Array<[keyof ReplySeedFields, ReplySeedFields]> = [
    ["contributionBindingDigest", { ...fields, contributionBindingDigest: digest("4") }],
    ["ordinal", { ...fields, ordinal: fields.ordinal + 1 }],
    ["eventDigest", { ...fields, eventDigest: digest("5") }],
    ["turnId", { ...fields, turnId: opaque("trn", 4) as TurnId }],
    ["bindingDigest", { ...fields, bindingDigest: digest("6") }],
  ];
  for (const [field, mutated] of mutations) {
    assert.notEqual(
      subject.deterministicCommandId(replySeed(mutated)),
      expected,
      field,
    );
  }
});

test("fresh kernel reconstruction reuses the exact reply command and receives terminal replay", async () => {
  const envelope = delivery();
  const journal = new MemoryJournal(pending(envelope));
  const execution = new RepairableExecution();
  const server = new LostReplyResponseServer();
  const clock = new FixedClock();

  const first = createDeliveryKernel({ journal, execution, server, clock });
  await assert.rejects(first.drainOne(activation()), /reply response lost/u);
  assert.equal(journal.visible.size, 1);
  assert.equal(journal.pending.length, 1);
  assert.equal(journal.terminalInputs.length, 0);
  assert.equal(execution.bodyReads, 1);
  assert.equal(execution.driverWrites, 1);

  const second = createDeliveryKernel({ journal, execution, server, clock });
  assert.deepEqual(await second.drainOne(activation()), {
    kind: "replayed",
    deliveryId: envelope.deliveryId,
  });

  const stage = execution.lastStage;
  assert.ok(stage);
  const expectedId = deterministicCommandId(replySeed({
    contributionBindingDigest: stage.contribution.contributionBindingDigest,
    ordinal: stage.basis.reply.ordinal,
    eventDigest: stage.basis.reply.eventDigest,
    turnId: stage.basis.reply.turnId,
    bindingDigest: stage.basis.reply.bindingDigest,
  }));
  assert.deepEqual(server.commandIds, [expectedId, expectedId]);
  assert.deepEqual(server.dispositions, ["committed", "terminal_replay"]);
  assert.equal(execution.bodyReads, 1);
  assert.equal(execution.driverWrites, 1);
  assert.equal(journal.pending.length, 0);
  assert.equal(journal.terminalInputs.length, 1);

  const persisted = JSON.stringify({
    visible: [...journal.visible.values()],
    terminal: journal.terminalInputs,
  });
  for (const forbidden of [replyText, "body", "credential", "/Users/"]) {
    assert.equal(persisted.includes(forbidden), false, forbidden);
  }
});
