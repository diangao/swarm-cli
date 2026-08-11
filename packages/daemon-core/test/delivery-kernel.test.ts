import assert from "node:assert/strict";
import { test } from "node:test";

import { buildContributionBinding } from "@swarm/protocol";
import type {
  ContributionBinding,
  AgentId,
  ArtifactDigest,
  AttentionNotice,
  ChannelId,
  CommandId,
  ConversationId,
  DeliveryFence,
  DeliveryId,
  LaunchId,
  MachineId,
  MessageId,
  NativeDeliveryEnvelope,
  NativeInvocationFence,
  ProducerFactId,
  ProtocolVersion,
  ReceiptId,
  SessionId,
  SimpleTaskCommand,
  StateInstanceId,
  Target,
  TaskId,
  TurnCoordinationDisposition,
  TurnId,
} from "@swarm/protocol";
import { protocolDigest } from "@swarm/runtime-contract";

import {
  TurnError,
  type CommitTurnTerminalInput,
  type TurnCoordinationRequest,
  type TurnStepResult,
  type TurnTerminalBindingPort,
  type TurnTerminalStage,
  type TurnTerminalResult,
} from "../src/turn/index.js";
import {
  DeliveryKernel,
  DeliveryKernelError,
  type DeliveryActivation,
  type DeliveryClock,
  type DeliveryCommandIdDerivationPort,
  type DeliveryExecutionInput,
  type DeliveryExecutionPort,
  type DeliveryExecutionResult,
  type DeliveryJournalPort,
  type DeliveryServerCommitPort,
  type NoticeVisibilityInput,
  type PendingDelivery,
  type VisibleMessageRecord,
} from "../src/delivery/index.js";

const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
const version = 1 as ProtocolVersion;
const launchId = opaque("lnc", 1) as LaunchId;
const stateInstanceId = opaque("sti", 1) as StateInstanceId;
const sessionId = opaque("ses", 1) as SessionId;
const agentId = opaque("agt", 1) as AgentId;
const machineId = opaque("mch", 1) as MachineId;

function opaque(prefix: string, value: number): string {
  return `${prefix}_${alphabet[value % alphabet.length]?.repeat(26)}`;
}

function digest(fill: string): ArtifactDigest {
  return `sha256:${fill.repeat(64)}` as ArtifactDigest;
}

function targetKey(target: Target): string {
  if (target.kind === "channel") {
    return `channel:${target.channelId}:${target.threadRootMessageId ?? ""}`;
  }
  return `direct:${target.conversationId}:${target.threadRootMessageId ?? ""}`;
}

function channel(value: number, thread?: number): Target {
  const result: Target = {
    kind: "channel",
    channelId: opaque("chn", value) as ChannelId,
  };
  if (thread !== undefined) {
    result.threadRootMessageId = opaque("msg", thread) as MessageId;
  }
  return result;
}

function direct(value: number): Target {
  return {
    kind: "direct",
    conversationId: opaque("cvs", value) as ConversationId,
  };
}

function delivery(input: {
  id: number;
  target: Target;
  serverSeq: number;
  membershipEpoch?: number;
}): NativeDeliveryEnvelope {
  return {
    protocolVersion: version,
    deliveryId: opaque("dlv", input.id) as DeliveryId,
    attempt: 1,
    messageId: opaque("msg", input.id) as MessageId,
    target: input.target,
    serverSeq: input.serverSeq,
    producerFactId: opaque("fac", input.id) as ProducerFactId,
    agentId,
    machineId,
    expectedLaunchId: launchId,
    membershipEpoch: input.membershipEpoch ?? 1,
    routingGeneration: 2,
    routeVersion: 3,
  };
}

function notice(input: {
  target: Target;
  first: number;
  latest: number;
  firstSeq: number;
  latestSeq: number;
}): AttentionNotice {
  return {
    protocolVersion: version,
    target: input.target,
    pendingCount: input.latestSeq - input.firstSeq + 1,
    firstMessageId: opaque("msg", input.first) as MessageId,
    latestMessageId: opaque("msg", input.latest) as MessageId,
    firstServerSeq: input.firstSeq,
    latestServerSeq: input.latestSeq,
  };
}

function pending(input: {
  delivery: NativeDeliveryEnvelope;
  receiveOrdinal: number;
  attention?: readonly AttentionNotice[];
  session?: SessionId;
  state?: "pending" | "held_ambiguous";
}): PendingDelivery {
  return {
    delivery: input.delivery,
    stateInstanceId,
    sessionId: input.session ?? sessionId,
    turnId: opaque("trn", input.receiveOrdinal) as TurnId,
    targetKey: targetKey(input.delivery.target),
    receiveOrdinal: input.receiveOrdinal,
    state: input.state ?? "pending",
    attention: input.attention ?? [],
  };
}

function activation(input: Partial<DeliveryActivation> = {}): DeliveryActivation {
  return {
    agentId,
    machineId,
    launchId,
    stateInstanceId,
    sessionId,
    localState: "activated",
    serverState: "activated",
    ...input,
  };
}

function ingress() {
  return { agentId, machineId, launchId };
}

function invocation(value = 1): NativeInvocationFence {
  return {
    invocationGeneration: value,
    invocationId: opaque("cmd", value) as CommandId,
  };
}

function bindingFor(
  input: DeliveryExecutionInput,
  fencedInvocation: NativeInvocationFence,
  overrides: Partial<{
    fence: DeliveryFence;
    stateInstanceId: StateInstanceId;
    inputOrdinal: number;
    invocationId: CommandId;
    invocationGeneration: number;
    permitId: CommandId;
    runtimeWriteId: CommandId;
    visibilityEventId: CommandId;
  }> = {},
): ContributionBinding {
  return buildContributionBinding({
    fence: input.fence,
    stateInstanceId: input.stateInstanceId,
    inputOrdinal: 1,
    invocationId: fencedInvocation.invocationId,
    invocationGeneration: fencedInvocation.invocationGeneration,
    permitId: opaque("cmd", 15) as CommandId,
    runtimeWriteId: opaque("cmd", 12) as CommandId,
    visibilityEventId: opaque("cmd", 14) as CommandId,
    ...overrides,
  });
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

class FakeTerminalPort implements TurnTerminalBindingPort {
  readonly draft: TurnTerminalStage;
  bindCalls = 0;
  terminalResultCalls = 0;
  failBeforeNextBind = false;
  #bound: Extract<
    TurnCoordinationDisposition,
    { kind: "committed" | "terminal_replay" }
  > | null = null;

  constructor(
    contribution: ContributionBinding,
    coordinationRequest: TurnCoordinationRequest,
    bound?: Extract<
      TurnCoordinationDisposition,
      { kind: "committed" | "terminal_replay" }
    >,
  ) {
    const bindingDigest = digest("9");
    const reply = {
      ordinal: 3,
      eventDigest: digest("3"),
      turnId: contribution.fence.turnId,
      bindingDigest,
    };
    const coordination = coordinationRequest.kind === "requested"
      ? {
          ordinal: 4,
          eventDigest: digest("4"),
          turnId: contribution.fence.turnId,
          bindingDigest,
          commandId: coordinationRequest.commandId,
          commandDigest: protocolDigest(coordinationRequest.command),
        }
      : undefined;
    this.draft = deepFreeze({
      contribution,
      coordinationRequest,
      basis: {
        reply,
        ...(coordination === undefined ? {} : { coordination }),
        completed: {
          ordinal: coordination === undefined ? 4 : 5,
          eventDigest: digest("5"),
          turnId: contribution.fence.turnId,
          bindingDigest,
        },
      },
      readerFence: {
        stateInstanceId: contribution.stateInstanceId,
        sessionId: contribution.fence.sessionId,
        ownerToken: digest("a"),
        readerEpoch: 1,
      },
      expected: {
        protocolTurnId: contribution.fence.turnId,
        phase: "model_visible",
        inputOrdinal: contribution.inputOrdinal,
        bindingDigest,
        steerable: true,
        replyCommitted: false,
      },
      next: {
        protocolTurnId: contribution.fence.turnId,
        phase: "completed",
        inputOrdinal: contribution.inputOrdinal,
        bindingDigest,
        steerable: false,
        replyCommitted: true,
      },
    });
    if (bound !== undefined) this.#bound = bound;
  }

  terminalDraft(): TurnTerminalStage {
    return this.draft;
  }

  bindCoordinationResult(
    disposition: Extract<
      TurnCoordinationDisposition,
      { kind: "committed" | "terminal_replay" }
    >,
  ): TurnTerminalResult {
    this.bindCalls += 1;
    if (this.failBeforeNextBind) {
      this.failBeforeNextBind = false;
      throw new Error("bind unavailable");
    }
    if (
      this.draft.coordinationRequest.kind !== "requested" ||
      disposition.commandId !== this.draft.coordinationRequest.commandId
    ) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "coordination_command_id");
    }
    if (
      this.#bound !== null &&
      protocolDigest(this.#bound) !== protocolDigest(disposition)
    ) {
      throw new TurnError("DRIVER_EVENT_FENCE_MISMATCH", "coordination_result_conflict");
    }
    if (this.#bound === null) this.#bound = disposition;
    return this.terminalResult();
  }

  terminalResult(): TurnTerminalResult {
    this.terminalResultCalls += 1;
    if (this.draft.coordinationRequest.kind === "not_requested") {
      return deepFreeze({
        contribution: this.draft.contribution,
        coordination: {
          kind: "not_requested",
          terminalTurnId: this.draft.coordinationRequest.terminalTurnId,
        },
      });
    }
    if (this.#bound === null) {
      throw new TurnError("INVOCATION_STATE_CONFLICT", "coordination_unresolved");
    }
    return deepFreeze({ contribution: this.draft.contribution, coordination: this.#bound });
  }
}

function visibleResult(
  input: DeliveryExecutionInput,
  options: {
    ackReceiptId?: ReceiptId;
    ackResultDigest?: ArtifactDigest;
    coordinationCommand?: SimpleTaskCommand;
    invocation?: NativeInvocationFence;
    contribution?: ContributionBinding;
    terminal?: TurnTerminalBindingPort;
  } = {},
): Extract<DeliveryExecutionResult, { kind: "model_visible" }> {
  const fencedInvocation = options.invocation ?? invocation(1);
  const ackResult = {
    boundary: "model_visible" as const,
    receiptId: options.ackReceiptId ?? opaque("rcp", 1) as ReceiptId,
    invocation: fencedInvocation,
    jobState: "acked/MODEL_VISIBLE" as const,
  };
  const result: Extract<DeliveryExecutionResult, { kind: "model_visible" }> = {
    kind: "model_visible",
    fence: input.fence,
    invocation: fencedInvocation,
    inputWritten: {
      entryId: opaque("cmd", 11) as CommandId,
      entryDigest: digest("1"),
      runtimeWriteId: opaque("cmd", 12) as CommandId,
    },
    modelVisible: {
      entryId: opaque("cmd", 13) as CommandId,
      entryDigest: digest("2"),
      runtimeWriteId: opaque("cmd", 12) as CommandId,
      visibilityEventId: opaque("cmd", 14) as CommandId,
    },
    modelVisibleAck: {
      observed: true,
      result: ackResult,
      resultDigest: options.ackResultDigest ?? protocolDigest(ackResult),
      disposition: "committed",
    },
    terminal: options.terminal ?? new FakeTerminalPort(
      options.contribution ?? bindingFor(input, fencedInvocation),
      options.coordinationCommand === undefined
        ? { kind: "not_requested", terminalTurnId: input.fence.turnId }
        : {
            kind: "requested",
            commandId: opaque("cmd", 21) as CommandId,
            command: options.coordinationCommand,
          },
    ),
    reply: {
      text: "normal reply",
    },
  };
  return result;
}

function ackPendingResult(
  input: DeliveryExecutionInput,
  fencedInvocation: NativeInvocationFence = invocation(1),
): Extract<DeliveryExecutionResult, { kind: "model_visible_ack_pending" }> {
  return {
    kind: "model_visible_ack_pending",
    fence: input.fence,
    invocation: fencedInvocation,
    inputWritten: {
      entryId: opaque("cmd", 11) as CommandId,
      entryDigest: digest("1"),
      runtimeWriteId: opaque("cmd", 12) as CommandId,
    },
    modelVisible: {
      entryId: opaque("cmd", 13) as CommandId,
      entryDigest: digest("2"),
      runtimeWriteId: opaque("cmd", 12) as CommandId,
      visibilityEventId: opaque("cmd", 14) as CommandId,
    },
  };
}

class FakeClock implements DeliveryClock {
  #next = 0;

  now(): string {
    this.#next += 1;
    return `2026-08-09T09:00:${String(this.#next).padStart(2, "0")}.000Z`;
  }
}

class FakeIds implements DeliveryCommandIdDerivationPort {
  readonly seeds: string[] = [];

  deterministicCommandId(seed: string): CommandId {
    this.seeds.push(seed);
    const source = protocolDigest(seed).slice("sha256:".length);
    const suffix = [...source.slice(0, 26)]
      .map((character) => alphabet[Number.parseInt(character, 16)] ?? "0")
      .join("");
    return `cmd_${suffix}` as CommandId;
  }
}

class FixedIds implements DeliveryCommandIdDerivationPort {
  constructor(readonly commandId: CommandId) {}

  deterministicCommandId(_seed: string): CommandId {
    return this.commandId;
  }
}

class FakeJournal implements DeliveryJournalPort {
  pending: PendingDelivery[] = [];
  readonly accepted: NativeDeliveryEnvelope[] = [];
  readonly completions: CommitTurnTerminalInput[] = [];
  readonly events: string[];
  readonly visible = new Map<string, VisibleMessageRecord>();
  readonly notices = new Map<string, NoticeVisibilityInput>();
  terminalCommitAttempts = 0;
  failTerminalBeforeCommitOnce = false;
  wrongTerminalCursorDigestOnce = false;
  readonly #terminalDigests = new Map<string, ArtifactDigest>();
  highWater = 0;

  constructor(events: string[] = []) {
    this.events = events;
  }

  async acceptNotice(input: {
    delivery: NativeDeliveryEnvelope;
    receivedAt: string;
  }): Promise<"inserted" | "replayed"> {
    this.events.push(`accept:${input.receivedAt}`);
    const existing = this.accepted.find((candidate) =>
      candidate.deliveryId === input.delivery.deliveryId
    );
    if (existing !== undefined) {
      assert.deepEqual(existing, input.delivery);
      return "replayed";
    }
    this.accepted.push(input.delivery);
    return "inserted";
  }

  async pendingFor(_activation: DeliveryActivation): Promise<readonly PendingDelivery[]> {
    return this.pending;
  }

  async findVisible(input: {
    sessionId: SessionId;
    target: Target;
    messageId: MessageId;
  }): Promise<VisibleMessageRecord | null> {
    return this.visible.get(
      `${input.sessionId}:${targetKey(input.target)}:${input.messageId}`,
    ) ?? null;
  }

  async compareNoticeVisibility(input: NoticeVisibilityInput): Promise<"new" | "replay"> {
    this.events.push("compare_notice");
    const key = this.#noticeKey(input);
    const existing = this.notices.get(key);
    if (existing === undefined) return "new";
    if (
      existing.firstMessageId === input.firstMessageId &&
      existing.latestMessageId === input.latestMessageId &&
      existing.firstServerSeq === input.firstServerSeq &&
      existing.latestServerSeq === input.latestServerSeq &&
      existing.inputDeliveryId === input.inputDeliveryId &&
      existing.inputAttempt === input.inputAttempt
    ) {
      return "replay";
    }
    throw new Error("VISIBILITY_LEDGER_CONFLICT");
  }

  async commitNoticeVisibility(
    input: NoticeVisibilityInput & {
      committedAt: string;
    },
  ): Promise<void> {
    this.events.push("commit_notice");
    this.notices.set(this.#noticeKey(input), input);
  }

  async commitVisibleMessage(input: {
    sessionId: SessionId;
    target: Target;
    messageId: MessageId;
    deliveryId: DeliveryId;
    attempt: number;
    serverSeq: number;
    modelVisibleAck: {
      observed: true;
      receiptId: ReceiptId;
    };
    visibleAt: string;
  }): Promise<void> {
    this.events.push("commit_visible");
    assert.equal(input.modelVisibleAck.observed, true);
    const key = targetKey(input.target);
    this.visible.set(
      `${input.sessionId}:${key}:${input.messageId}`,
      {
        deliveryId: input.deliveryId,
        attempt: input.attempt,
        serverSeq: input.serverSeq,
        modelVisibleReceiptId: input.modelVisibleAck.receiptId,
        visibleAt: input.visibleAt,
      },
    );
  }

  async holdAmbiguous(input: {
    fence: DeliveryFence;
    stateInstanceId: StateInstanceId;
    invocation: NativeInvocationFence;
    heldAt: string;
  }): Promise<void> {
    this.events.push("hold_ambiguous");
    const record = this.pending.find((candidate) =>
      candidate.delivery.deliveryId === input.fence.deliveryId
    );
    assert.ok(record);
    record.state = "held_ambiguous";
  }

  commitTurnTerminal(input: CommitTurnTerminalInput): TurnStepResult {
    this.terminalCommitAttempts += 1;
    if (this.failTerminalBeforeCommitOnce) {
      this.failTerminalBeforeCommitOnce = false;
      throw new Error("terminal commit unavailable");
    }
    const visible = [...this.visible.values()].find((candidate) =>
      candidate.deliveryId === input.evidence.contribution.fence.deliveryId &&
      candidate.attempt === input.evidence.contribution.fence.attempt
    );
    assert.ok(visible, "completion requires exact visible-message predecessor");
    assert.equal(input.evidence.contribution.stateInstanceId, input.stateInstanceId);
    assert.match(input.basis.reply.replyCommandId, /^cmd_/u);
    assert.match(input.evidence.reply.receiptId, /^rcp_/u);
    assert.match(input.evidence.reply.resultDigest, /^sha256:/u);
    if (input.evidence.coordination.kind === "not_requested") {
      assert.equal(
        input.evidence.coordination.terminalTurnId,
        input.evidence.contribution.fence.turnId,
      );
      assert.equal(input.basis.coordination, undefined);
    } else {
      assert.match(input.evidence.coordination.commandId, /^cmd_/u);
      assert.match(input.evidence.coordination.receiptId, /^rcp_/u);
      assert.match(input.evidence.coordination.resultDigest, /^sha256:/u);
      assert.equal(input.basis.coordination?.commandId, input.evidence.coordination.commandId);
    }
    const operationDigest = protocolDigest({
      stateInstanceId: input.stateInstanceId,
      sessionId: input.sessionId,
      basis: input.basis,
      evidence: input.evidence,
      expected: input.expected,
      next: input.next,
    });
    const terminalKey = `${input.stateInstanceId}:${input.sessionId}:${input.next.protocolTurnId}`;
    if (this.wrongTerminalCursorDigestOnce) {
      this.wrongTerminalCursorDigestOnce = false;
      const wrongEventDigest = protocolDigest({ kind: "wrong_terminal_cursor_digest" });
      assert.notEqual(wrongEventDigest, input.basis.completed.eventDigest);
      return {
        applied: false,
        durable: input.next,
        nextOrdinal: input.basis.completed.ordinal + 1,
        lastEventDigest: wrongEventDigest,
      };
    }
    const priorDigest = this.#terminalDigests.get(terminalKey);
    if (priorDigest !== undefined) {
      if (priorDigest !== operationDigest) {
        throw new Error("TERMINAL_REPLAY_CONFLICT");
      }
      return {
        applied: false,
        durable: input.next,
        nextOrdinal: input.basis.completed.ordinal + 1,
        lastEventDigest: input.basis.completed.eventDigest,
      };
    }
    this.#terminalDigests.set(terminalKey, operationDigest);
    this.completions.push(input);
    this.events.push("consumed");
    this.pending = this.pending.filter((candidate) =>
      candidate.delivery.deliveryId !== input.evidence.contribution.fence.deliveryId
    );
    return {
      applied: true,
      durable: input.next,
      nextOrdinal: input.basis.completed.ordinal + 1,
      lastEventDigest: input.basis.completed.eventDigest,
    };
  }

  #noticeKey(input: NoticeVisibilityInput): string {
    return `${input.sessionId}:${targetKey(input.target)}:${input.membershipEpoch}`;
  }
}

class FakeExecution implements DeliveryExecutionPort {
  readonly calls: DeliveryExecutionInput[] = [];
  driverWrites = 0;
  bodyReads = 0;
  readonly #factory: (input: DeliveryExecutionInput) => DeliveryExecutionResult;

  constructor(factory: (input: DeliveryExecutionInput) => DeliveryExecutionResult) {
    this.#factory = factory;
  }

  async execute(input: DeliveryExecutionInput): Promise<DeliveryExecutionResult> {
    this.calls.push(input);
    const result = this.#factory(input);
    if (input.mode.kind === "new_input") {
      if (result.kind !== "rejected_before_write") this.bodyReads += 1;
      if (result.kind !== "rejected_before_write") this.driverWrites += 1;
    }
    return result;
  }
}

class LostAckExecution implements DeliveryExecutionPort {
  readonly calls: DeliveryExecutionInput[] = [];
  bodyReads = 0;
  driverWrites = 0;

  async execute(input: DeliveryExecutionInput): Promise<DeliveryExecutionResult> {
    this.calls.push(input);
    assert.equal(input.mode.kind, "new_input");
    if (this.calls.length === 1) {
      this.bodyReads += 1;
      this.driverWrites += 1;
      return ackPendingResult(input, invocation(17));
    }
    return visibleResult(input, { invocation: invocation(17) });
  }
}

class FakeServer implements DeliveryServerCommitPort {
  readonly events: string[];
  readonly committedReplyCommands = new Set<CommandId>();
  readonly committedCoordinationCommands = new Set<CommandId>();
  lastReplyCommandId: CommandId | undefined;

  constructor(events: string[] = []) {
    this.events = events;
  }

  async appendReply(input: {
    fence: DeliveryFence;
    commandId: CommandId;
    text: string;
  }) {
    this.events.push("reply");
    this.lastReplyCommandId = input.commandId;
    assert.match(input.commandId, /^cmd_/u);
    assert.equal(input.text, "normal reply");
    const replay = this.committedReplyCommands.has(input.commandId);
    this.committedReplyCommands.add(input.commandId);
    const result = {
      replyMessageId: opaque("msg", 25) as MessageId,
      receiptId: opaque("rcp", 25) as ReceiptId,
      causalOrder: 1,
    };
    return {
      result,
      resultDigest: protocolDigest(result),
      disposition: replay ? "terminal_replay" as const : "committed" as const,
    };
  }

  async applyCoordination(input: {
    fence: DeliveryFence;
    commandId: CommandId;
    command: SimpleTaskCommand;
    replyReceiptId: ReceiptId;
  }) {
    this.events.push("coordination");
    assert.match(input.commandId, /^cmd_/u);
    assert.match(input.replyReceiptId, /^rcp_/u);
    const replay = this.committedCoordinationCommands.has(input.commandId);
    this.committedCoordinationCommands.add(input.commandId);
    const result = {
      taskId: opaque("tsk", 1) as TaskId,
      receiptId: opaque("rcp", 26) as ReceiptId,
      causalOrder: 2,
    };
    return {
      result,
      resultDigest: protocolDigest(result),
      disposition: replay ? "terminal_replay" as const : "committed" as const,
    };
  }
}

class FailOnceReplyServer extends FakeServer {
  attempts = 0;
  successfulReplies = 0;

  override async appendReply(input: {
    fence: DeliveryFence;
    commandId: CommandId;
    text: string;
  }) {
    this.attempts += 1;
    if (this.attempts === 1) {
      await super.appendReply(input);
      this.events.push("reply_disconnect");
      throw new Error("reply response lost");
    }
    const result = await super.appendReply(input);
    this.successfulReplies += 1;
    return result;
  }
}

class FailOnceCoordinationResponseServer extends FakeServer {
  attempts = 0;

  override async applyCoordination(
    input: Parameters<FakeServer["applyCoordination"]>[0],
  ) {
    this.attempts += 1;
    const evidence = await super.applyCoordination(input);
    if (this.attempts === 1) {
      this.events.push("coordination_disconnect");
      throw new Error("coordination response lost");
    }
    return evidence;
  }
}

class WrongReplyDigestServer extends FakeServer {
  override async appendReply(
    input: Parameters<FakeServer["appendReply"]>[0],
  ) {
    const result = await super.appendReply(input);
    return { ...result, resultDigest: digest("c") };
  }
}

class WrongReplyReceiptServer extends FakeServer {
  override async appendReply(
    input: Parameters<FakeServer["appendReply"]>[0],
  ) {
    const evidence = await super.appendReply(input);
    const result = {
      ...evidence.result,
      receiptId: opaque("cmd", 33) as unknown as ReceiptId,
    };
    return { ...evidence, result, resultDigest: protocolDigest(result) };
  }
}

class WrongCoordinationReceiptServer extends FakeServer {
  override async applyCoordination(
    input: Parameters<FakeServer["applyCoordination"]>[0],
  ) {
    const evidence = await super.applyCoordination(input);
    const result = {
      ...evidence.result,
      receiptId: opaque("cmd", 34) as unknown as ReceiptId,
    };
    return { ...evidence, result, resultDigest: protocolDigest(result) };
  }
}

class WrongCausalOrderServer extends FakeServer {
  override async applyCoordination(
    input: Parameters<FakeServer["applyCoordination"]>[0],
  ) {
    const evidence = await super.applyCoordination(input);
    const result = { ...evidence.result, causalOrder: 1 };
    return { ...evidence, result, resultDigest: protocolDigest(result) };
  }
}

function kernel(input: {
  journal: FakeJournal;
  execution: DeliveryExecutionPort;
  server?: FakeServer;
  ids?: DeliveryCommandIdDerivationPort;
  clock?: FakeClock;
}): DeliveryKernel {
  return new DeliveryKernel({
    journal: input.journal,
    execution: input.execution,
    server: input.server ?? new FakeServer(input.journal.events),
    ids: input.ids ?? new FakeIds(),
    clock: input.clock ?? new FakeClock(),
  });
}

test("NOTICE-first acceptance rejects body fields and activation holds read no body", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 1, target: channel(1), serverSeq: 1 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const execution = new FakeExecution((input) => visibleResult(input));
  const subject = kernel({ journal, execution });

  assert.equal(await subject.acceptNotice(envelope, ingress()), "inserted");
  assert.equal(await subject.acceptNotice(envelope, ingress()), "replayed");
  await assert.rejects(
    subject.acceptNotice(
      { ...envelope, serverSeq: envelope.serverSeq + 1 },
      ingress(),
    ),
  );
  await assert.rejects(
    subject.acceptNotice(
      { ...envelope, body: "private" } as NativeDeliveryEnvelope,
      ingress(),
    ),
  );
  await assert.rejects(
    subject.acceptNotice(envelope, { ...ingress(), launchId: opaque("lnc", 2) as LaunchId }),
    (error: unknown) =>
      error instanceof DeliveryKernelError && error.code === "STALE_DELIVERY_FENCE",
  );
  assert.equal(journal.accepted.length, 1);

  assert.deepEqual(
    await subject.drainOne(activation({ localState: "starting" })),
    { kind: "held", reason: "ACTIVATION_PREDECESSOR_REQUIRED" },
  );
  assert.deepEqual(
    await subject.drainOne(activation({ serverState: "ready" })),
    { kind: "held", reason: "ACTIVATION_PREDECESSOR_REQUIRED" },
  );
  assert.equal(execution.bodyReads, 0);
  assert.equal(execution.driverWrites, 0);
});

test("drain preserves per-target sequence and commits reply before coordination", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const targetA = channel(1);
  const targetB = direct(1);
  const blockedEarly = delivery({ id: 1, target: targetA, serverSeq: 2 });
  const oldestEligible = delivery({ id: 2, target: targetB, serverSeq: 1 });
  const targetAHead = delivery({ id: 3, target: targetA, serverSeq: 1 });
  const attention = notice({ target: channel(2), first: 10, latest: 10, firstSeq: 10, latestSeq: 10 });
  journal.pending.push(
    pending({ delivery: blockedEarly, receiveOrdinal: 1 }),
    pending({ delivery: oldestEligible, receiveOrdinal: 2, attention: [attention] }),
    pending({ delivery: targetAHead, receiveOrdinal: 3 }),
  );
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "follow up",
    sourceMessageId: oldestEligible.messageId,
  };
  const execution = new FakeExecution((input) => {
    events.push("execute");
    return visibleResult(input, { coordinationCommand: coordination });
  });
  const ids = new FakeIds();
  const server = new FakeServer(events);
  const subject = kernel({ journal, execution, server, ids });

  const result = await subject.drainOne(activation());

  assert.equal(result.kind, "completed");
  if (result.kind === "completed") assert.equal(result.deliveryId, oldestEligible.deliveryId);
  assert.equal(execution.calls[0]?.delivery.deliveryId, oldestEligible.deliveryId);
  assert.deepEqual(events.slice(-7), [
    "compare_notice",
    "execute",
    "commit_notice",
    "commit_visible",
    "reply",
    "coordination",
    "consumed",
  ]);
  assert.equal(journal.completions.length, 1);
  const committedTaskResult = {
    taskId: opaque("tsk", 1) as TaskId,
    receiptId: opaque("rcp", 26) as ReceiptId,
    causalOrder: 2,
  };
  assert.deepEqual(journal.completions[0]?.evidence.coordination, {
    kind: "committed",
    commandId: opaque("cmd", 21),
    receiptId: committedTaskResult.receiptId,
    resultDigest: protocolDigest(committedTaskResult),
  });
  assert.equal(journal.terminalCommitAttempts, 1);
  assert.equal(journal.completions[0]?.basis.reply.replyCommandId, server.lastReplyCommandId);
  assert.deepEqual(JSON.parse(ids.seeds[0] ?? "null"), {
    bindingDigest: digest("9"),
    contributionBindingDigest:
      journal.completions[0]?.evidence.contribution.contributionBindingDigest,
    eventDigest: digest("3"),
    kind: "turn_reply_v1",
    ordinal: 3,
    turnId: opaque("trn", 2),
  });
  const terminalBytes = JSON.stringify(journal.completions[0]);
  assert.equal(terminalBytes.includes("normal reply"), false);
  assert.equal(terminalBytes.includes('"text"'), false);
});

test("parent target and sibling thread keep distinct visibility and drain order", async () => {
  const journal = new FakeJournal();
  const parent = delivery({ id: 27, target: channel(27), serverSeq: 1 });
  const thread = delivery({ id: 28, target: channel(27, 28), serverSeq: 1 });
  journal.pending.push(
    pending({ delivery: parent, receiveOrdinal: 1 }),
    pending({ delivery: thread, receiveOrdinal: 2 }),
  );
  const execution = new FakeExecution((input) => visibleResult(input));
  const subject = kernel({ journal, execution });

  assert.equal((await subject.drainOne(activation())).kind, "completed");
  assert.equal((await subject.drainOne(activation())).kind, "completed");
  assert.deepEqual(
    execution.calls.map((call) => call.delivery.deliveryId),
    [parent.deliveryId, thread.deliveryId],
  );
  assert.equal(execution.driverWrites, 2);
  assert.equal(journal.visible.size, 2);
});

test("exact visible replay repairs outputs without another body read or native write", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 4, target: channel(4), serverSeq: 4 });
  const record = pending({ delivery: envelope, receiveOrdinal: 1 });
  journal.pending.push(record);
  const existingInvocation = invocation(3);
  const existing: VisibleMessageRecord = {
    deliveryId: envelope.deliveryId,
    attempt: envelope.attempt,
    serverSeq: envelope.serverSeq,
    modelVisibleReceiptId: opaque("rcp", 3) as ReceiptId,
    visibleAt: "2026-08-09T08:00:00.000Z",
  };
  journal.visible.set(`${sessionId}:${record.targetKey}:${envelope.messageId}`, existing);
  const execution = new FakeExecution((input) => {
    assert.equal(input.mode.kind, "repair_visible");
    events.push("reconcile_visible");
    return visibleResult(input, {
      ackReceiptId: existing.modelVisibleReceiptId,
      invocation: existingInvocation,
    });
  });
  const subject = kernel({ journal, execution, server: new FakeServer(events) });

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "replayed",
    deliveryId: envelope.deliveryId,
  });
  assert.equal(execution.bodyReads, 0);
  assert.equal(execution.driverWrites, 0);
  assert.deepEqual(events, ["reconcile_visible", "reply", "consumed"]);
  assert.deepEqual(journal.completions[0]?.evidence.coordination, {
    kind: "not_requested",
    terminalTurnId: opaque("trn", 1),
  });
});

test("visible replay with wrong ACK result digest completes nothing", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 23, target: channel(23), serverSeq: 23 });
  const record = pending({ delivery: envelope, receiveOrdinal: 1 });
  journal.pending.push(record);
  journal.visible.set(
    `${sessionId}:${record.targetKey}:${envelope.messageId}`,
    {
      deliveryId: envelope.deliveryId,
      attempt: envelope.attempt,
      serverSeq: envelope.serverSeq,
      modelVisibleReceiptId: opaque("rcp", 23) as ReceiptId,
      visibleAt: "2026-08-09T08:00:00.000Z",
    },
  );
  const execution = new FakeExecution((input) => {
    events.push("reconcile_visible");
    return visibleResult(input, {
      ackReceiptId: opaque("rcp", 23) as ReceiptId,
      ackResultDigest: digest("e"),
      invocation: invocation(23),
    });
  });
  const subject = kernel({
    journal,
    execution,
    server: new FakeServer(events),
  });

  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "SERVER_COMMIT_EVIDENCE_MISMATCH",
  );
  assert.deepEqual(events, ["reconcile_visible"]);
  assert.equal(execution.calls.length, 1);
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
});

test("wrong reply result digest leaves visible delivery unconsumed", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 22, target: channel(22), serverSeq: 22 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const execution = new FakeExecution((input) => visibleResult(input));
  const subject = kernel({
    journal,
    execution,
    server: new WrongReplyDigestServer(events),
  });

  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "SERVER_COMMIT_EVIDENCE_MISMATCH",
  );
  assert.deepEqual(events, ["commit_visible", "reply"]);
  assert.equal(journal.visible.size, 1);
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
});

test("wrong reply receipt brand cannot unlock coordination or completion", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 35, target: channel(35), serverSeq: 35 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "must stay blocked",
    sourceMessageId: envelope.messageId,
  };
  const execution = new FakeExecution((input) => visibleResult(input, { coordinationCommand: coordination }));
  const subject = kernel({
    journal,
    execution,
    server: new WrongReplyReceiptServer(events),
  });

  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "SERVER_COMMIT_EVIDENCE_MISMATCH",
  );
  assert.deepEqual(events, ["commit_visible", "reply"]);
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
});

test("wrong coordination receipt brand leaves delivery unconsumed", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 36, target: channel(36), serverSeq: 36 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "bad result brand",
    sourceMessageId: envelope.messageId,
  };
  const execution = new FakeExecution((input) => visibleResult(input, { coordinationCommand: coordination }));
  const subject = kernel({
    journal,
    execution,
    server: new WrongCoordinationReceiptServer(events),
  });

  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "SERVER_COMMIT_EVIDENCE_MISMATCH",
  );
  assert.deepEqual(events, ["commit_visible", "reply", "coordination"]);
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
});

test("reply crash repair converges with one native write and one successful normal reply", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 24, target: channel(24), serverSeq: 24 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const execution = new FakeExecution((input) => {
    if (input.mode.kind === "repair_visible") {
      assert.equal(input.attention.length, 0);
      events.push("reconcile_visible");
    }
    return visibleResult(input, { invocation: invocation(24) });
  });
  const server = new FailOnceReplyServer(events);
  const subject = kernel({ journal, execution, server });

  await assert.rejects(subject.drainOne(activation()), /reply response lost/u);
  assert.equal(journal.visible.size, 1);
  assert.equal(journal.pending.length, 1);
  assert.equal(journal.completions.length, 0);
  assert.equal(execution.driverWrites, 1);

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "replayed",
    deliveryId: envelope.deliveryId,
  });
  assert.equal(execution.driverWrites, 1);
  assert.equal(execution.bodyReads, 1);
  assert.equal(server.committedReplyCommands.size, 1);
  assert.equal(journal.pending.length, 0);
  assert.deepEqual(events, [
    "commit_visible",
    "reply",
    "reply_disconnect",
    "reconcile_visible",
    "reply",
    "consumed",
  ]);
});

test("coordination response loss replays the same server command and binds once", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 41, target: channel(41), serverSeq: 41 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "repair one coordination",
    sourceMessageId: envelope.messageId,
  };
  let terminal: FakeTerminalPort | undefined;
  const execution = new FakeExecution((input) => {
    if (input.mode.kind === "repair_visible") events.push("reconcile_visible");
    const invocationFence = invocation(41);
    terminal ??= new FakeTerminalPort(bindingFor(input, invocationFence), {
      kind: "requested",
      commandId: opaque("cmd", 21) as CommandId,
      command: coordination,
    });
    return visibleResult(input, { invocation: invocationFence, terminal });
  });
  const server = new FailOnceCoordinationResponseServer(events);
  const subject = kernel({ journal, execution, server });

  await assert.rejects(subject.drainOne(activation()), /coordination response lost/u);
  assert.equal(journal.visible.size, 1);
  assert.equal(journal.completions.length, 0);
  assert.equal(terminal?.bindCalls, 0);

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "replayed",
    deliveryId: envelope.deliveryId,
  });
  assert.equal(execution.driverWrites, 1);
  assert.equal(execution.bodyReads, 1);
  assert.equal(server.attempts, 2);
  assert.equal(server.committedCoordinationCommands.size, 1);
  assert.equal(terminal?.bindCalls, 1);
  assert.deepEqual(journal.completions[0]?.evidence.coordination, {
    kind: "terminal_replay",
    commandId: opaque("cmd", 21),
    receiptId: opaque("rcp", 26),
    resultDigest: protocolDigest({
      taskId: opaque("tsk", 1) as TaskId,
      receiptId: opaque("rcp", 26) as ReceiptId,
      causalOrder: 2,
    }),
  });
  assert.deepEqual(events, [
    "commit_visible",
    "reply",
    "coordination",
    "coordination_disconnect",
    "reconcile_visible",
    "reply",
    "coordination",
    "consumed",
  ]);
});

test("terminal bind failure repairs before the sole durable commit", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 42, target: channel(42), serverSeq: 42 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "preserve bound result",
    sourceMessageId: envelope.messageId,
  };
  let terminal: FakeTerminalPort | undefined;
  const execution = new FakeExecution((input) => {
    if (input.mode.kind === "repair_visible") events.push("reconcile_visible");
    const invocationFence = invocation(42);
    if (terminal === undefined) {
      terminal = new FakeTerminalPort(bindingFor(input, invocationFence), {
        kind: "requested",
        commandId: opaque("cmd", 21) as CommandId,
        command: coordination,
      });
      terminal.failBeforeNextBind = true;
    }
    return visibleResult(input, { invocation: invocationFence, terminal });
  });
  const server = new FakeServer(events);
  const subject = kernel({ journal, execution, server });

  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
  );
  assert.equal(journal.visible.size, 1);
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
  assert.equal(terminal?.bindCalls, 1);

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "replayed",
    deliveryId: envelope.deliveryId,
  });
  assert.equal(execution.driverWrites, 1);
  assert.equal(server.committedReplyCommands.size, 1);
  assert.equal(server.committedCoordinationCommands.size, 1);
  assert.equal(terminal?.bindCalls, 2);
  assert.deepEqual(journal.completions[0]?.evidence.coordination, {
    kind: "terminal_replay",
    commandId: opaque("cmd", 21),
    receiptId: opaque("rcp", 26),
    resultDigest: protocolDigest({
      taskId: opaque("tsk", 1) as TaskId,
      receiptId: opaque("rcp", 26) as ReceiptId,
      causalOrder: 2,
    }),
  });
  assert.deepEqual(events, [
    "commit_visible",
    "reply",
    "coordination",
    "reconcile_visible",
    "reply",
    "coordination",
    "consumed",
  ]);
});

test("lost model-visible ACK reconciles without another body read or driver write", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 17, target: channel(17), serverSeq: 17 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const execution = new LostAckExecution();
  const server = new FakeServer(events);
  const subject = kernel({ journal, execution, server });

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "deferred",
    reason: "MODEL_VISIBLE_ACK_PENDING",
  });
  assert.equal(execution.bodyReads, 1);
  assert.equal(execution.driverWrites, 1);
  assert.equal(journal.pending.length, 1);

  assert.equal((await subject.drainOne(activation())).kind, "completed");
  assert.equal(execution.bodyReads, 1);
  assert.equal(execution.driverWrites, 1);
  assert.equal(server.committedReplyCommands.size, 1);
  assert.equal(journal.completions.length, 1);
});

test("terminal commit failure replays reply, reuses bound coordination, then commits once", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 16, target: channel(16), serverSeq: 16 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  journal.failTerminalBeforeCommitOnce = true;
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "one task",
    sourceMessageId: envelope.messageId,
  };
  let terminal: FakeTerminalPort | undefined;
  const execution = new FakeExecution((input) => {
    const invocationFence = invocation(16);
    terminal ??= new FakeTerminalPort(
      bindingFor(input, invocationFence),
      {
        kind: "requested",
        commandId: opaque("cmd", 21) as CommandId,
        command: coordination,
      },
    );
    return visibleResult(input, {
      invocation: invocationFence,
      terminal,
    });
  });
  const server = new FakeServer(events);
  const subject = kernel({ journal, execution, server });

  await assert.rejects(subject.drainOne(activation()), /terminal commit unavailable/u);
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
  assert.equal((await subject.drainOne(activation())).kind, "replayed");

  assert.equal(execution.driverWrites, 1);
  assert.equal(execution.bodyReads, 1);
  assert.equal(server.committedReplyCommands.size, 1);
  assert.equal(server.committedCoordinationCommands.size, 1);
  assert.equal(terminal?.bindCalls, 1);
  assert.equal(journal.terminalCommitAttempts, 2);
  assert.equal(journal.completions.length, 1);
  const replayedTaskResult = {
    taskId: opaque("tsk", 1) as TaskId,
    receiptId: opaque("rcp", 26) as ReceiptId,
    causalOrder: 2,
  };
  assert.deepEqual(journal.completions[0]?.evidence.coordination, {
    kind: "committed",
    commandId: opaque("cmd", 21),
    receiptId: replayedTaskResult.receiptId,
    resultDigest: protocolDigest(replayedTaskResult),
  });
});

test("wrong terminal cursor digest rejects, then exact replay commits without new effects", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 18, target: channel(18), serverSeq: 18 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  journal.wrongTerminalCursorDigestOnce = true;
  const coordination: SimpleTaskCommand = {
    protocolVersion: version,
    title: "one task",
    sourceMessageId: envelope.messageId,
  };
  let terminal: FakeTerminalPort | undefined;
  const execution = new FakeExecution((input) => {
    const invocationFence = invocation(18);
    terminal ??= new FakeTerminalPort(
      bindingFor(input, invocationFence),
      {
        kind: "requested",
        commandId: opaque("cmd", 21) as CommandId,
        command: coordination,
      },
    );
    return visibleResult(input, {
      invocation: invocationFence,
      terminal,
    });
  });
  const server = new FakeServer(events);
  const subject = kernel({ journal, execution, server });

  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
  );
  assert.equal(journal.completions.length, 0);
  assert.equal(journal.pending.length, 1);
  assert.equal((await subject.drainOne(activation())).kind, "replayed");

  assert.equal(execution.driverWrites, 1);
  assert.equal(execution.bodyReads, 1);
  assert.equal(server.committedReplyCommands.size, 1);
  assert.equal(server.committedCoordinationCommands.size, 1);
  assert.equal(terminal?.bindCalls, 1);
  assert.equal(journal.terminalCommitAttempts, 2);
  assert.equal(journal.completions.length, 1);
});

test("high-water alone never suppresses an exact message gap", async () => {
  const journal = new FakeJournal();
  journal.highWater = 99;
  const envelope = delivery({ id: 5, target: channel(5), serverSeq: 5 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const execution = new FakeExecution((input) => visibleResult(input));
  const subject = kernel({ journal, execution });

  const result = await subject.drainOne(activation());
  assert.equal(result.kind, "completed");
  assert.equal(execution.driverWrites, 1);
});

test("multi-message notice range commits at input-written while model ACK is pending", async () => {
  const events: string[] = [];
  const journal = new FakeJournal(events);
  const envelope = delivery({ id: 6, target: channel(6), serverSeq: 6 });
  const attention = notice({
    target: direct(6),
    first: 101,
    latest: 104,
    firstSeq: 101,
    latestSeq: 104,
  });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1, attention: [attention] }));
  const execution = new FakeExecution((input) => {
    events.push("execute");
    return ackPendingResult(input);
  });
  const subject = kernel({ journal, execution, server: new FakeServer(events) });

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "deferred",
    reason: "MODEL_VISIBLE_ACK_PENDING",
  });
  assert.deepEqual(events, ["compare_notice", "execute", "commit_notice"]);
  assert.deepEqual(
    journal.notices.get(`${sessionId}:${targetKey(attention.target)}:1`),
    {
      ...noticeInputForTest(journal.pending[0]!, attention),
      committedAt: "2026-08-09T09:00:01.000Z",
    },
  );
  assert.equal(journal.visible.size, 0);
  assert.equal(journal.pending.length, 1);
});

test("model-visible evidence and normalized output fences block illegal successors", async (context) => {
  const contributionCases: readonly {
    name: string;
    build: (
      input: DeliveryExecutionInput,
      fencedInvocation: NativeInvocationFence,
    ) => ContributionBinding;
  }[] = [
    {
      name: "wrong contribution state instance",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        stateInstanceId: opaque("sti", 2) as StateInstanceId,
      }),
    },
    {
      name: "internally consistent sibling-turn fence",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        fence: {
          ...input.fence,
          turnId: opaque("trn", 30) as TurnId,
        },
      }),
    },
    {
      name: "internally consistent sibling-session full fence",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        fence: {
          ...input.fence,
          sessionId: opaque("ses", 30) as SessionId,
        },
      }),
    },
    {
      name: "sibling-steer invocation binding",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        invocationId: opaque("cmd", 30) as CommandId,
      }),
    },
    {
      name: "sibling-steer invocation generation",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        invocationGeneration: fencedInvocation.invocationGeneration + 1,
      }),
    },
    {
      name: "sibling-steer runtime-write binding",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        runtimeWriteId: opaque("cmd", 30) as CommandId,
      }),
    },
    {
      name: "sibling-steer visibility-event binding",
      build: (input, fencedInvocation) => bindingFor(input, fencedInvocation, {
        visibilityEventId: opaque("cmd", 30) as CommandId,
      }),
    },
  ];

  for (const [index, contributionCase] of contributionCases.entries()) {
    await context.test(contributionCase.name, async () => {
      const events: string[] = [];
      const journal = new FakeJournal(events);
      const envelope = delivery({
        id: 40 + index,
        target: channel(40 + index),
        serverSeq: 40 + index,
      });
      journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
      const fencedInvocation = invocation(40 + index);
      const execution = new FakeExecution((input) => visibleResult(input, {
        invocation: fencedInvocation,
        contribution: contributionCase.build(input, fencedInvocation),
      }));
      const subject = kernel({ journal, execution, server: new FakeServer(events) });

      await assert.rejects(
        subject.drainOne(activation()),
        (error: unknown) =>
          error instanceof DeliveryKernelError &&
          error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
      );
      assert.deepEqual(events, []);
      assert.equal(journal.visible.size, 0);
      assert.equal(journal.completions.length, 0);
    });
  }

  for (const malformedStage of [
    {
      name: "terminal stage has an extra top-level field",
      build: (draft: TurnTerminalStage): TurnTerminalStage => deepFreeze({
        ...draft,
        unexpected: true,
      }) as unknown as TurnTerminalStage,
    },
    {
      name: "terminal stage has a mutable nested reader fence",
      build: (draft: TurnTerminalStage): TurnTerminalStage => Object.freeze({
        ...draft,
        readerFence: { ...draft.readerFence },
      }),
    },
    {
      name: "terminal stage has a non-enumerable extra field",
      build: (draft: TurnTerminalStage): TurnTerminalStage => {
        const candidate = { ...draft };
        Object.defineProperty(candidate, "hidden", {
          configurable: false,
          enumerable: false,
          value: true,
          writable: false,
        });
        return deepFreeze(candidate) as TurnTerminalStage;
      },
    },
    {
      name: "terminal stage expected steerability is not a boolean",
      build: (draft: TurnTerminalStage): TurnTerminalStage => deepFreeze({
        ...draft,
        expected: {
          ...draft.expected,
          steerable: "true",
        },
      }) as unknown as TurnTerminalStage,
    },
  ] as const) {
    await context.test(malformedStage.name, async () => {
      const events: string[] = [];
      const journal = new FakeJournal(events);
      const envelope = delivery({ id: 54, target: channel(54), serverSeq: 54 });
      journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
      const execution = new FakeExecution((input) => {
        const result = visibleResult(input);
        const sourceTerminal = result.terminal;
        const draft = malformedStage.build(sourceTerminal.terminalDraft());
        const terminal: TurnTerminalBindingPort = {
          terminalDraft: () => draft,
          terminalResult: () => sourceTerminal.terminalResult(),
          bindCoordinationResult: (disposition) =>
            sourceTerminal.bindCoordinationResult(disposition),
        };
        return { ...result, terminal };
      });
      const subject = kernel({ journal, execution, server: new FakeServer(events) });

      await assert.rejects(
        subject.drainOne(activation()),
        (error: unknown) =>
          error instanceof DeliveryKernelError &&
          error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
      );
      assert.deepEqual(events, []);
      assert.equal(journal.visible.size, 0);
      assert.equal(journal.completions.length, 0);
    });
  }

  await context.test("runtime-write cross-binding mismatch", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 29, target: channel(29), serverSeq: 29 });
    const attention = notice({
      target: direct(29),
      first: 29,
      latest: 29,
      firstSeq: 29,
      latestSeq: 29,
    });
    journal.pending.push(pending({
      delivery: envelope,
      receiveOrdinal: 1,
      attention: [attention],
    }));
    const execution = new FakeExecution((input) => {
      const result = visibleResult(input);
      return {
        ...result,
        modelVisible: {
          ...result.modelVisible,
          runtimeWriteId: opaque("cmd", 30) as CommandId,
        },
      };
    });
    const subject = kernel({ journal, execution, server: new FakeServer(events) });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "MODEL_VISIBLE_PREDECESSOR_REQUIRED",
    );
    assert.deepEqual(events, ["compare_notice"]);
    assert.equal(journal.notices.size, 0);
    assert.equal(journal.visible.size, 0);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("terminal ACK invocation mismatch", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 32, target: channel(32), serverSeq: 32 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const execution = new FakeExecution((input) => {
      const result = visibleResult(input, { invocation: invocation(32) });
      const ackResult = {
        ...result.modelVisibleAck.result,
        invocation: invocation(31),
      };
      return {
        ...result,
        modelVisibleAck: {
          ...result.modelVisibleAck,
          result: ackResult,
          resultDigest: protocolDigest(ackResult),
        },
      };
    });
    const subject = kernel({ journal, execution, server: new FakeServer(events) });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "MODEL_VISIBLE_PREDECESSOR_REQUIRED",
    );
    assert.deepEqual(events, []);
    assert.equal(journal.visible.size, 0);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("requested draft cannot carry pre-bound not-requested terminal truth", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 37, target: channel(37), serverSeq: 37 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const coordination: SimpleTaskCommand = {
      protocolVersion: version,
      title: "must have exact result truth",
      sourceMessageId: envelope.messageId,
    };
    const execution = new FakeExecution((input) => {
      const result = visibleResult(input, { coordinationCommand: coordination });
      const draft = result.terminal.terminalDraft();
      return {
        ...result,
        terminal: {
          terminalDraft: () => draft,
          terminalResult: () => ({
            contribution: draft.contribution,
            coordination: {
              kind: "not_requested",
              terminalTurnId: input.fence.turnId,
            },
          }),
          bindCoordinationResult: () => {
            throw new Error("must not bind inconsistent pre-bound result");
          },
        },
      };
    });
    const subject = kernel({ journal, execution, server: new FakeServer(events) });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
    );
    assert.deepEqual(events, ["commit_visible"]);
    assert.equal(journal.visible.size, 1);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("not-requested output cannot carry committed terminal truth", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 38, target: channel(38), serverSeq: 38 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const taskResult = {
      taskId: opaque("tsk", 1) as TaskId,
      receiptId: opaque("rcp", 26) as ReceiptId,
      causalOrder: 2,
    };
    const execution = new FakeExecution((input) => {
      const result = visibleResult(input);
      const draft = result.terminal.terminalDraft();
      const committed = {
        kind: "committed" as const,
        commandId: opaque("cmd", 21) as CommandId,
        receiptId: taskResult.receiptId,
        resultDigest: protocolDigest(taskResult),
      };
      return {
        ...result,
        terminal: {
          terminalDraft: () => draft,
          terminalResult: () => ({
            contribution: draft.contribution,
            coordination: committed,
          }),
          bindCoordinationResult: () => ({
            contribution: draft.contribution,
            coordination: committed,
          }),
        },
      };
    });
    const subject = kernel({ journal, execution, server: new FakeServer(events) });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
    );
    assert.deepEqual(events, ["commit_visible"]);
    assert.equal(journal.visible.size, 1);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("terminal disposition must equal the server result disposition", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 39, target: channel(39), serverSeq: 39 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const coordination: SimpleTaskCommand = {
      protocolVersion: version,
      title: "wrong terminal disposition",
      sourceMessageId: envelope.messageId,
    };
    const taskResult = {
      taskId: opaque("tsk", 1) as TaskId,
      receiptId: opaque("rcp", 26) as ReceiptId,
      causalOrder: 2,
    };
    const execution = new FakeExecution((input) => {
      const result = visibleResult(input, { coordinationCommand: coordination });
      const draft = result.terminal.terminalDraft();
      const wrong = {
        kind: "terminal_replay" as const,
        commandId: opaque("cmd", 21) as CommandId,
        receiptId: taskResult.receiptId,
        resultDigest: protocolDigest(taskResult),
      };
      return {
        ...result,
        terminal: {
          terminalDraft: () => draft,
          terminalResult: () => {
            throw new TurnError("INVOCATION_STATE_CONFLICT", "coordination_unresolved");
          },
          bindCoordinationResult: () => deepFreeze({
            contribution: draft.contribution,
            coordination: wrong,
          }),
        },
      };
    });
    const subject = kernel({ journal, execution, server: new FakeServer(events) });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "SERVER_COMMIT_EVIDENCE_MISMATCH",
    );
    assert.deepEqual(events, ["commit_visible", "reply", "coordination"]);
    assert.equal(journal.visible.size, 1);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("reply and coordination command alias", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 30, target: channel(30), serverSeq: 30 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const coordination: SimpleTaskCommand = {
      protocolVersion: version,
      title: "aliased command",
      sourceMessageId: envelope.messageId,
    };
    const coordinationCommandId = opaque("cmd", 21) as CommandId;
    const execution = new FakeExecution((input) => visibleResult(input, {
      coordinationCommand: coordination,
    }));
    const subject = kernel({
      journal,
      execution,
      server: new FakeServer(events),
      ids: new FixedIds(coordinationCommandId),
    });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
    );
    assert.deepEqual(events, ["commit_visible"]);
    assert.equal(journal.visible.size, 1);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("coordination request must name the delivered source message", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 43, target: channel(43), serverSeq: 43 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const execution = new FakeExecution((input) => {
      const result = visibleResult(input);
      const draft = result.terminal.terminalDraft();
      return {
        ...result,
        terminal: new FakeTerminalPort(draft.contribution, {
          kind: "requested",
          commandId: opaque("cmd", 21) as CommandId,
          command: {
            protocolVersion: version,
            title: "wrong source",
            sourceMessageId: opaque("msg", 44) as MessageId,
          },
        }),
      };
    });
    const subject = kernel({ journal, execution, server: new FakeServer(events) });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
    );
    assert.deepEqual(events, []);
    assert.equal(journal.visible.size, 0);
    assert.equal(journal.completions.length, 0);
  });

  await context.test("coordination causal order does not follow reply", async () => {
    const events: string[] = [];
    const journal = new FakeJournal(events);
    const envelope = delivery({ id: 31, target: channel(31), serverSeq: 31 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const coordination: SimpleTaskCommand = {
      protocolVersion: version,
      title: "bad causal order",
      sourceMessageId: envelope.messageId,
    };
    const execution = new FakeExecution((input) => visibleResult(input, { coordinationCommand: coordination }));
    const subject = kernel({
      journal,
      execution,
      server: new WrongCausalOrderServer(events),
    });

    await assert.rejects(
      subject.drainOne(activation()),
      (error: unknown) =>
        error instanceof DeliveryKernelError &&
        error.code === "SERVER_COMMIT_EVIDENCE_MISMATCH",
    );
    assert.deepEqual(events, ["commit_visible", "reply", "coordination"]);
    assert.equal(journal.visible.size, 1);
    assert.equal(journal.completions.length, 0);
  });
});

test("failed, proven-not-written, and ambiguous outcomes never retry in one drain", async (context) => {
  await context.test("preflight rejection", async () => {
    const journal = new FakeJournal();
    const envelope = delivery({ id: 7, target: channel(7), serverSeq: 7 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const execution = new FakeExecution(() => ({
      kind: "rejected_before_write",
      reason: "preflight",
    }));
    const subject = kernel({ journal, execution });
    assert.deepEqual(await subject.drainOne(activation()), {
      kind: "deferred",
      reason: "REJECTED_BEFORE_WRITE",
    });
    assert.equal(execution.calls.length, 1);
    assert.equal(journal.visible.size, 0);
  });

  await context.test("scripted not written", async () => {
    const journal = new FakeJournal();
    const envelope = delivery({ id: 8, target: channel(8), serverSeq: 8 });
    journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
    const execution = new FakeExecution((input) => ({
      kind: "not_written",
      fence: input.fence,
      invocation: invocation(8),
      proofDigest: digest("8"),
    }));
    const subject = kernel({ journal, execution });
    assert.deepEqual(await subject.drainOne(activation()), {
      kind: "deferred",
      reason: "PROVEN_NOT_WRITTEN",
    });
    assert.equal(execution.calls.length, 1);
    assert.equal(journal.visible.size, 0);
  });

  await context.test("ambiguous hold", async () => {
    const journal = new FakeJournal();
    const envelope = delivery({ id: 9, target: channel(9), serverSeq: 9 });
    const attention = notice({
      target: direct(9),
      first: 9,
      latest: 9,
      firstSeq: 9,
      latestSeq: 9,
    });
    journal.pending.push(pending({
      delivery: envelope,
      receiveOrdinal: 1,
      attention: [attention],
    }));
    const execution = new FakeExecution((input) => ({
      kind: "ambiguous",
      fence: input.fence,
      invocation: invocation(9),
    }));
    const subject = kernel({ journal, execution });
    assert.deepEqual(await subject.drainOne(activation()), {
      kind: "held",
      reason: "AMBIGUOUS_NATIVE_WRITE",
    });
    assert.deepEqual(await subject.drainOne(activation()), {
      kind: "held",
      reason: "AMBIGUOUS_NATIVE_WRITE",
    });
    assert.equal(execution.calls.length, 1);
    assert.equal(journal.visible.size, 0);
    assert.equal(journal.notices.size, 0);
  });

  await context.test("an ambiguous attempt blocks every later ordinary input", async () => {
    const journal = new FakeJournal();
    const ambiguous = delivery({ id: 18, target: channel(18), serverSeq: 18 });
    const later = delivery({ id: 19, target: direct(19), serverSeq: 19 });
    journal.pending.push(
      pending({
        delivery: ambiguous,
        receiveOrdinal: 1,
        state: "held_ambiguous",
      }),
      pending({ delivery: later, receiveOrdinal: 2 }),
    );
    const execution = new FakeExecution((input) => visibleResult(input));
    const subject = kernel({ journal, execution });

    assert.deepEqual(await subject.drainOne(activation()), {
      kind: "held",
      reason: "AMBIGUOUS_NATIVE_WRITE",
    });
    assert.equal(execution.calls.length, 0);
    assert.equal(journal.pending.length, 2);
  });
});

test("notice replay filters metadata and changed range conflicts before body read", async () => {
  const journal = new FakeJournal();
  const envelope = delivery({ id: 10, target: channel(10), serverSeq: 10 });
  const firstNotice = notice({ target: channel(11), first: 30, latest: 30, firstSeq: 30, latestSeq: 30 });
  const record = pending({ delivery: envelope, receiveOrdinal: 1, attention: [firstNotice] });
  journal.pending.push(record);
  const exactInput = noticeInputForTest(record, firstNotice);
  journal.notices.set(`${sessionId}:${targetKey(firstNotice.target)}:1`, exactInput);
  const execution = new FakeExecution((input) => {
    assert.deepEqual(input.attention, []);
    return visibleResult(input);
  });
  const subject = kernel({ journal, execution });
  assert.equal((await subject.drainOne(activation())).kind, "completed");
  assert.equal(execution.bodyReads, 1);

  const changedJournal = new FakeJournal();
  const changedEnvelope = delivery({ id: 11, target: channel(12), serverSeq: 11 });
  const changedNotice = notice({
    target: firstNotice.target,
    first: 31,
    latest: 31,
    firstSeq: 31,
    latestSeq: 31,
  });
  const changedRecord = pending({
    delivery: changedEnvelope,
    receiveOrdinal: 1,
    attention: [changedNotice],
  });
  changedJournal.pending.push(changedRecord);
  changedJournal.notices.set(
    `${sessionId}:${targetKey(firstNotice.target)}:1`,
    noticeInputForTest(changedRecord, firstNotice),
  );
  const blockedExecution = new FakeExecution((input) => visibleResult(input));
  const blocked = kernel({ journal: changedJournal, execution: blockedExecution });
  await assert.rejects(blocked.drainOne(activation()), /VISIBILITY_LEDGER_CONFLICT/u);
  assert.equal(blockedExecution.bodyReads, 0);
  assert.equal(blockedExecution.driverWrites, 0);
});

test("notice privacy poison and failed preflight never consume notice eligibility", async () => {
  const target = channel(20);
  const attention = notice({
    target,
    first: 50,
    latest: 50,
    firstSeq: 50,
    latestSeq: 50,
  });
  const poisonedJournal = new FakeJournal();
  const poisonedDelivery = delivery({ id: 20, target: direct(20), serverSeq: 20 });
  poisonedJournal.pending.push(pending({
    delivery: poisonedDelivery,
    receiveOrdinal: 1,
    attention: [{ ...attention, author: "private", body: "private" } as AttentionNotice],
  }));
  const poisonedExecution = new FakeExecution((input) => visibleResult(input));
  const poisoned = kernel({ journal: poisonedJournal, execution: poisonedExecution });
  await assert.rejects(poisoned.drainOne(activation()));
  assert.equal(poisonedExecution.bodyReads, 0);
  assert.equal(poisonedExecution.driverWrites, 0);
  assert.equal(poisonedJournal.notices.size, 0);

  const journal = new FakeJournal();
  const envelope = delivery({ id: 21, target: direct(21), serverSeq: 21 });
  journal.pending.push(pending({
    delivery: envelope,
    receiveOrdinal: 1,
    attention: [attention],
  }));
  let executionAttempt = 0;
  const execution = new FakeExecution((input) => {
    executionAttempt += 1;
    return executionAttempt === 1
      ? { kind: "rejected_before_write", reason: "preflight" }
      : visibleResult(input);
  });
  const subject = kernel({ journal, execution });

  assert.deepEqual(await subject.drainOne(activation()), {
    kind: "deferred",
    reason: "REJECTED_BEFORE_WRITE",
  });
  assert.equal(execution.bodyReads, 0);
  assert.equal(execution.driverWrites, 0);
  assert.equal(journal.notices.size, 0);

  assert.equal((await subject.drainOne(activation())).kind, "completed");
  assert.equal(execution.bodyReads, 1);
  assert.equal(execution.driverWrites, 1);
  assert.equal(journal.notices.size, 1);
});

test("session and membership epoch changes never alias notice visibility", async () => {
  const sharedTarget = channel(13);
  const attention = notice({ target: sharedTarget, first: 40, latest: 40, firstSeq: 40, latestSeq: 40 });
  const journal = new FakeJournal();
  const firstEnvelope = delivery({ id: 12, target: direct(12), serverSeq: 12 });
  const first = pending({ delivery: firstEnvelope, receiveOrdinal: 1, attention: [attention] });
  journal.notices.set(
    `${sessionId}:${targetKey(sharedTarget)}:1`,
    noticeInputForTest(first, attention),
  );

  const nextSession = opaque("ses", 2) as SessionId;
  const nextEnvelope = delivery({
    id: 13,
    target: direct(13),
    serverSeq: 13,
    membershipEpoch: 2,
  });
  journal.pending.push(pending({
    delivery: nextEnvelope,
    receiveOrdinal: 1,
    attention: [attention],
    session: nextSession,
  }));
  const execution = new FakeExecution((input) => {
    assert.equal(input.attention.length, 1);
    return visibleResult(input);
  });
  const subject = kernel({ journal, execution });

  assert.equal((await subject.drainOne(activation({ sessionId: nextSession }))).kind, "completed");
  assert.equal(journal.notices.size, 2);
});

test("execution fence mismatch and visible tuple conflict mutate no local boundary", async () => {
  const journal = new FakeJournal();
  const envelope = delivery({ id: 14, target: channel(14), serverSeq: 14 });
  journal.pending.push(pending({ delivery: envelope, receiveOrdinal: 1 }));
  const execution = new FakeExecution((input) => {
    const result = visibleResult(input);
    return {
      ...result,
      fence: { ...result.fence, routeVersion: result.fence.routeVersion + 1 },
    };
  });
  const subject = kernel({ journal, execution });
  await assert.rejects(
    subject.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "DELIVERY_EXECUTION_FENCE_MISMATCH",
  );
  assert.equal(journal.visible.size, 0);
  assert.equal(journal.pending.length, 1);

  const replayJournal = new FakeJournal();
  const replayRecord = pending({ delivery: envelope, receiveOrdinal: 1 });
  replayJournal.pending.push(replayRecord);
  replayJournal.visible.set(
    `${sessionId}:${replayRecord.targetKey}:${envelope.messageId}`,
    {
      deliveryId: opaque("dlv", 15) as DeliveryId,
      attempt: 1,
      serverSeq: envelope.serverSeq,
      modelVisibleReceiptId: opaque("rcp", 15) as ReceiptId,
      visibleAt: "2026-08-09T08:00:00.000Z",
    },
  );
  const replayExecution = new FakeExecution((input) => visibleResult(input));
  const replay = kernel({ journal: replayJournal, execution: replayExecution });
  await assert.rejects(
    replay.drainOne(activation()),
    (error: unknown) =>
      error instanceof DeliveryKernelError &&
      error.code === "VISIBLE_MESSAGE_REPLAY_CONFLICT",
  );
  assert.equal(replayExecution.calls.length, 0);
});

function noticeInputForTest(
  record: PendingDelivery,
  value: AttentionNotice,
): NoticeVisibilityInput {
  return {
    sessionId: record.sessionId,
    target: value.target,
    membershipEpoch: record.delivery.membershipEpoch,
    firstMessageId: value.firstMessageId,
    latestMessageId: value.latestMessageId,
    firstServerSeq: value.firstServerSeq,
    latestServerSeq: value.latestServerSeq,
    inputDeliveryId: record.delivery.deliveryId,
    inputAttempt: record.delivery.attempt,
  };
}
