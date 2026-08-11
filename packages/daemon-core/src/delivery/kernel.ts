import {
  canonicalProtocolJson,
  messageBodyHasContent,
  parseAttentionNotice,
  parseNativeDeliveryEnvelope,
  parseSimpleTaskCommand,
  parseTurnCoordinationDisposition,
  parseTurnReplyResult,
  verifyContributionBinding,
  verifyTurnCompletionEvidence,
  type AttentionNotice,
  type ContributionBinding,
  type DeliveryFence,
  type NativeDeliveryEnvelope,
  type NativeInvocationFence,
  type TurnCoordinationDisposition,
} from "@swarm/protocol";
import { protocolDigest } from "@swarm/runtime-contract";

import {
  TurnError,
  type DurableTurnState,
  type TurnEventRecordInput,
  type TurnTerminalCommitBasis,
  type TurnTerminalStage,
  type TurnTerminalResult,
} from "../turn/index.js";
import { DeliveryKernelError } from "./errors.js";
import type {
  DeliveryActivation,
  DeliveryClock,
  DeliveryCommandIdDerivationPort,
  DeliveryDrainResult,
  DeliveryExecutionPort,
  DeliveryExecutionResult,
  DeliveryIngressFence,
  DeliveryJournalPort,
  DeliveryServerCommitPort,
  NoticeVisibilityInput,
  ObservedModelVisibleAck,
  PendingDelivery,
  ServerResultEvidence,
  VisibleMessageRecord,
} from "./types.js";

const FENCE_KEYS = [
  "protocolVersion",
  "deliveryId",
  "attempt",
  "producerFactId",
  "agentId",
  "machineId",
  "launchId",
  "membershipEpoch",
  "routingGeneration",
  "routeVersion",
  "sessionId",
  "turnId",
] as const;

function sameFence(left: DeliveryFence, right: DeliveryFence): boolean {
  return FENCE_KEYS.every((key) => left[key] === right[key]);
}

function sameInvocation(left: NativeInvocationFence, right: NativeInvocationFence): boolean {
  return (
    left.invocationGeneration === right.invocationGeneration &&
    left.invocationId === right.invocationId
  );
}

function assertServerEvidence<Result>(evidence: ServerResultEvidence<Result>): void {
  if (
    (evidence.disposition !== "committed" &&
      evidence.disposition !== "terminal_replay") ||
    protocolDigest(evidence.result) !== evidence.resultDigest
  ) {
    throw new DeliveryKernelError("SERVER_COMMIT_EVIDENCE_MISMATCH");
  }
}

function assertReplyResultEvidence(
  evidence: Awaited<ReturnType<DeliveryServerCommitPort["appendReply"]>>,
): void {
  try {
    parseTurnReplyResult({
      receiptId: evidence.result.receiptId,
      resultDigest: evidence.resultDigest,
    });
  } catch {
    throw new DeliveryKernelError("SERVER_COMMIT_EVIDENCE_MISMATCH");
  }
}

function assertCoordinationResultEvidence(
  commandId: Extract<TurnCoordinationDisposition, { kind: "committed" }>["commandId"],
  evidence: Awaited<ReturnType<DeliveryServerCommitPort["applyCoordination"]>>,
): Extract<TurnCoordinationDisposition, { kind: "committed" | "terminal_replay" }> {
  try {
    return parseTurnCoordinationDisposition({
      kind: evidence.disposition,
      commandId,
      receiptId: evidence.result.receiptId,
      resultDigest: evidence.resultDigest,
    }) as Extract<
      TurnCoordinationDisposition,
      { kind: "committed" | "terminal_replay" }
    >;
  } catch {
    throw new DeliveryKernelError("SERVER_COMMIT_EVIDENCE_MISMATCH");
  }
}

function assertModelVisibleAck(
  ack: ObservedModelVisibleAck,
  invocation: NativeInvocationFence,
  expectedReceiptId?: VisibleMessageRecord["modelVisibleReceiptId"],
): void {
  assertServerEvidence(ack);
  if (
    ack.observed !== true ||
    ack.result.boundary !== "model_visible" ||
    ack.result.jobState !== "acked/MODEL_VISIBLE" ||
    !sameInvocation(ack.result.invocation, invocation) ||
    (expectedReceiptId !== undefined && ack.result.receiptId !== expectedReceiptId)
  ) {
    throw new DeliveryKernelError("MODEL_VISIBLE_PREDECESSOR_REQUIRED");
  }
}

function assertModelVisibleEvidence(
  result: Extract<
    DeliveryExecutionResult,
    { kind: "model_visible" | "model_visible_ack_pending" }
  >,
): void {
  if (
    result.inputWritten.entryId === result.modelVisible.entryId ||
    result.inputWritten.runtimeWriteId !== result.modelVisible.runtimeWriteId
  ) {
    throw new DeliveryKernelError("MODEL_VISIBLE_PREDECESSOR_REQUIRED");
  }
}

function assertContributionBinding(
  candidate: ContributionBinding,
  result: Extract<DeliveryExecutionResult, { kind: "model_visible" }>,
  expectedFence: DeliveryFence,
  expectedStateInstanceId: PendingDelivery["stateInstanceId"],
): ContributionBinding {
  let contribution;
  try {
    contribution = verifyContributionBinding(candidate, {
      expectedFence,
    });
  } catch {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  if (
    contribution.stateInstanceId !== expectedStateInstanceId ||
    contribution.invocationId !== result.invocation.invocationId ||
    contribution.invocationGeneration !== result.invocation.invocationGeneration ||
    contribution.runtimeWriteId !== result.inputWritten.runtimeWriteId ||
    contribution.runtimeWriteId !== result.modelVisible.runtimeWriteId ||
    contribution.visibilityEventId !== result.modelVisible.visibilityEventId
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  return contribution;
}

const ARTIFACT_DIGEST = /^sha256:[0-9a-f]{64}$/u;
const COMMAND_ID = /^cmd_[0-9a-hjkmnp-tv-z]{26}$/u;

function assertArtifactDigest(value: string): void {
  if (!ARTIFACT_DIGEST.test(value)) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
}

function assertTerminalRecord(
  record: TurnEventRecordInput,
  turnId: DeliveryFence["turnId"],
  bindingDigest: Exclude<DurableTurnState["bindingDigest"], null>,
  extraKeys: readonly string[] = [],
): TurnEventRecordInput {
  if (
    !hasExactKeys(record, [
      "ordinal",
      "eventDigest",
      "turnId",
      "bindingDigest",
      ...extraKeys,
    ]) ||
    !Number.isSafeInteger(record.ordinal) ||
    record.ordinal < 0 ||
    record.turnId !== turnId ||
    record.bindingDigest !== bindingDigest
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  assertArtifactDigest(record.eventDigest);
  return {
    ordinal: record.ordinal,
    eventDigest: record.eventDigest,
    turnId: record.turnId,
    bindingDigest: record.bindingDigest,
  };
}

function isRecursivelyFrozen(value: unknown, seen = new Set<object>()): boolean {
  if (value === null || typeof value !== "object") return true;
  if (seen.has(value)) return true;
  seen.add(value);
  if (!Object.isFrozen(value)) return false;
  return Object.values(value as Record<string, unknown>).every((nested) =>
    isRecursivelyFrozen(nested, seen)
  );
}

function hasExactKeys(value: unknown, expected: readonly string[]): boolean {
  if (value === null || typeof value !== "object") return false;
  const ownKeys = Reflect.ownKeys(value);
  if (ownKeys.some((key) => typeof key !== "string")) return false;
  const actual = (ownKeys as string[]).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}

function assertTerminalDraft(
  result: Extract<DeliveryExecutionResult, { kind: "model_visible" }>,
  pending: PendingDelivery,
  expectedFence: DeliveryFence,
): TurnTerminalStage {
  let draft;
  try {
    draft = result.terminal.terminalDraft();
  } catch {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  if (
    !hasExactKeys(draft, [
      "contribution",
      "coordinationRequest",
      "basis",
      "readerFence",
      "expected",
      "next",
    ]) ||
    !isRecursivelyFrozen(draft)
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  const contribution = assertContributionBinding(
    draft.contribution,
    result,
    expectedFence,
    pending.stateInstanceId,
  );
  if (
    !hasExactKeys(draft.readerFence, [
      "stateInstanceId",
      "sessionId",
      "ownerToken",
      "readerEpoch",
    ]) ||
    !hasExactKeys(draft.expected, [
      "protocolTurnId",
      "phase",
      "inputOrdinal",
      "bindingDigest",
      "steerable",
      "replyCommitted",
    ]) ||
    !hasExactKeys(draft.next, [
      "protocolTurnId",
      "phase",
      "inputOrdinal",
      "bindingDigest",
      "steerable",
      "replyCommitted",
    ]) ||
    draft.readerFence.stateInstanceId !== pending.stateInstanceId ||
    draft.readerFence.sessionId !== pending.sessionId ||
    !Number.isSafeInteger(draft.readerFence.readerEpoch) ||
    draft.readerFence.readerEpoch < 1
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  assertArtifactDigest(draft.readerFence.ownerToken);
  const expected = draft.expected;
  const next = draft.next;
  if (
    expected.protocolTurnId !== expectedFence.turnId ||
    expected.phase !== "model_visible" ||
    expected.inputOrdinal !== contribution.inputOrdinal ||
    expected.bindingDigest === null ||
    typeof expected.steerable !== "boolean" ||
    expected.replyCommitted !== false ||
    next.protocolTurnId !== expected.protocolTurnId ||
    next.phase !== "completed" ||
    next.inputOrdinal !== expected.inputOrdinal ||
    next.bindingDigest !== expected.bindingDigest ||
    next.steerable !== false ||
    next.replyCommitted !== true
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  assertArtifactDigest(expected.bindingDigest);
  const reply = assertTerminalRecord(
    draft.basis.reply,
    expectedFence.turnId,
    expected.bindingDigest,
  );
  const completed = assertTerminalRecord(
    draft.basis.completed,
    expectedFence.turnId,
    expected.bindingDigest,
  );
  const request = draft.coordinationRequest;
  if (request.kind === "not_requested") {
    if (
      !hasExactKeys(request, ["kind", "terminalTurnId"]) ||
      !hasExactKeys(draft.basis, ["reply", "completed"]) ||
      request.terminalTurnId !== expectedFence.turnId ||
      draft.basis.coordination !== undefined ||
      completed.ordinal !== reply.ordinal + 1
    ) {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
    return {
      contribution,
      coordinationRequest: request,
      basis: { reply, completed },
      readerFence: { ...draft.readerFence },
      expected: { ...expected },
      next: { ...next },
    };
  }
  if (
    !hasExactKeys(request, ["kind", "commandId", "command"]) ||
    !hasExactKeys(draft.basis, ["reply", "coordination", "completed"]) ||
    !COMMAND_ID.test(request.commandId) ||
    request.command.sourceMessageId !== pending.delivery.messageId
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  let command;
  try {
    command = parseSimpleTaskCommand(
      canonicalProtocolJson(request.command),
      expectedFence.protocolVersion,
    );
  } catch {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  const coordinationCandidate = draft.basis.coordination;
  if (coordinationCandidate === undefined) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  const coordination = assertTerminalRecord(
    coordinationCandidate,
    expectedFence.turnId,
    expected.bindingDigest,
    ["commandId", "commandDigest"],
  );
  if (
    coordinationCandidate.commandId !== request.commandId ||
    coordinationCandidate.commandDigest !== protocolDigest(command) ||
    coordination.ordinal !== reply.ordinal + 1 ||
    completed.ordinal !== coordination.ordinal + 1
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  return {
    contribution,
    coordinationRequest: {
      kind: "requested",
      commandId: request.commandId,
      command,
    },
    basis: {
      reply,
      coordination: {
        ...coordination,
        commandId: coordinationCandidate.commandId,
        commandDigest: coordinationCandidate.commandDigest,
      },
      completed,
    },
    readerFence: { ...draft.readerFence },
    expected: { ...expected },
    next: { ...next },
  };
}

function assertTerminalResult(
  candidate: TurnTerminalResult,
  draft: TurnTerminalStage,
  result: Extract<DeliveryExecutionResult, { kind: "model_visible" }>,
  pending: PendingDelivery,
  expectedFence: DeliveryFence,
  expectedCoordination?: Extract<
    TurnCoordinationDisposition,
    { kind: "committed" | "terminal_replay" }
  >,
): TurnTerminalResult {
  if (
    !hasExactKeys(candidate, ["contribution", "coordination"]) ||
    !isRecursivelyFrozen(candidate)
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  const contribution = assertContributionBinding(
    candidate.contribution,
    result,
    expectedFence,
    pending.stateInstanceId,
  );
  if (protocolDigest(contribution) !== protocolDigest(draft.contribution)) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  let coordination;
  try {
    coordination = parseTurnCoordinationDisposition(candidate.coordination);
  } catch {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  if (draft.coordinationRequest.kind === "not_requested") {
    if (
      coordination.kind !== "not_requested" ||
      coordination.terminalTurnId !== draft.coordinationRequest.terminalTurnId
    ) {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
  } else if (
    coordination.kind === "not_requested" ||
    coordination.commandId !== draft.coordinationRequest.commandId
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  if (
    expectedCoordination !== undefined &&
    protocolDigest(coordination) !== protocolDigest(expectedCoordination)
  ) {
    throw new DeliveryKernelError("SERVER_COMMIT_EVIDENCE_MISMATCH");
  }
  return { contribution, coordination };
}

function assertTerminalPredecessor(
  terminal: Extract<DeliveryExecutionResult, { kind: "model_visible" }>["terminal"],
  draft: TurnTerminalStage,
  result: Extract<DeliveryExecutionResult, { kind: "model_visible" }>,
  pending: PendingDelivery,
  fence: DeliveryFence,
): TurnTerminalResult | null {
  try {
    const candidate = terminal.terminalResult();
    return assertTerminalResult(candidate, draft, result, pending, fence);
  } catch (error) {
    if (
      draft.coordinationRequest.kind === "requested" &&
      error instanceof TurnError &&
      error.code === "INVOCATION_STATE_CONFLICT" &&
      error.detail === "coordination_unresolved"
    ) {
      return null;
    }
    if (error instanceof DeliveryKernelError) throw error;
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
}

function replyCommandSeed(draft: TurnTerminalStage): string {
  return new TextDecoder().decode(canonicalProtocolJson({
    kind: "turn_reply_v1",
    contributionBindingDigest: draft.contribution.contributionBindingDigest,
    ordinal: draft.basis.reply.ordinal,
    eventDigest: draft.basis.reply.eventDigest,
    turnId: draft.basis.reply.turnId,
    bindingDigest: draft.basis.reply.bindingDigest,
  }));
}

function assertTerminalCommitResult(
  result: Awaited<ReturnType<DeliveryJournalPort["commitTurnTerminal"]>>,
  draft: TurnTerminalStage,
): void {
  if (
    typeof result.applied !== "boolean" ||
    result.nextOrdinal !== draft.basis.completed.ordinal + 1 ||
    result.lastEventDigest !== draft.basis.completed.eventDigest ||
    protocolDigest(result.durable) !== protocolDigest(draft.next)
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
}

function parseDelivery(delivery: NativeDeliveryEnvelope): NativeDeliveryEnvelope {
  return parseNativeDeliveryEnvelope(
    canonicalProtocolJson(delivery),
    delivery.protocolVersion,
  );
}

function parseNotices(
  notices: readonly AttentionNotice[],
  delivery: NativeDeliveryEnvelope,
): readonly AttentionNotice[] {
  return notices.map((notice) =>
    parseAttentionNotice(canonicalProtocolJson(notice), delivery.protocolVersion)
  );
}

function fenceFor(pending: PendingDelivery): DeliveryFence {
  const delivery = pending.delivery;
  return {
    protocolVersion: delivery.protocolVersion,
    deliveryId: delivery.deliveryId,
    attempt: delivery.attempt,
    producerFactId: delivery.producerFactId,
    agentId: delivery.agentId,
    machineId: delivery.machineId,
    launchId: delivery.expectedLaunchId,
    membershipEpoch: delivery.membershipEpoch,
    routingGeneration: delivery.routingGeneration,
    routeVersion: delivery.routeVersion,
    sessionId: pending.sessionId,
    turnId: pending.turnId,
  };
}

function comparePending(left: PendingDelivery, right: PendingDelivery): number {
  return (
    left.receiveOrdinal - right.receiveOrdinal ||
    left.targetKey.localeCompare(right.targetKey) ||
    left.delivery.serverSeq - right.delivery.serverSeq ||
    left.delivery.deliveryId.localeCompare(right.delivery.deliveryId)
  );
}

function validatePending(
  pending: PendingDelivery,
  activation: DeliveryActivation,
): PendingDelivery {
  const delivery = parseDelivery(pending.delivery);
  if (
    delivery.agentId !== activation.agentId ||
    delivery.machineId !== activation.machineId ||
    delivery.expectedLaunchId !== activation.launchId ||
    pending.stateInstanceId !== activation.stateInstanceId ||
    pending.sessionId !== activation.sessionId
  ) {
    throw new DeliveryKernelError("STALE_DELIVERY_FENCE");
  }
  if (
    !Number.isSafeInteger(pending.receiveOrdinal) ||
    pending.receiveOrdinal < 1 ||
    pending.targetKey.length === 0
  ) {
    throw new DeliveryKernelError("DELIVERY_ORDER_INVALID");
  }
  return {
    ...pending,
    delivery,
    attention: parseNotices(pending.attention, delivery),
  };
}

function selectOldestEligible(pending: readonly PendingDelivery[]): PendingDelivery | undefined {
  const heads = new Map<string, PendingDelivery>();
  const seenDeliveries = new Set<string>();
  for (const candidate of pending) {
    if (seenDeliveries.has(candidate.delivery.deliveryId)) {
      throw new DeliveryKernelError("DELIVERY_ORDER_INVALID");
    }
    seenDeliveries.add(candidate.delivery.deliveryId);
    if (candidate.state !== "pending") continue;
    const current = heads.get(candidate.targetKey);
    if (
      current === undefined ||
      candidate.delivery.serverSeq < current.delivery.serverSeq ||
      (
        candidate.delivery.serverSeq === current.delivery.serverSeq &&
        comparePending(candidate, current) < 0
      )
    ) {
      heads.set(candidate.targetKey, candidate);
    }
  }
  return [...heads.values()].sort(comparePending)[0];
}

function assertVisibleTuple(
  visible: VisibleMessageRecord,
  pending: PendingDelivery,
): void {
  const delivery = pending.delivery;
  if (
    visible.deliveryId !== delivery.deliveryId ||
    visible.attempt !== delivery.attempt ||
    visible.serverSeq !== delivery.serverSeq
  ) {
    throw new DeliveryKernelError("VISIBLE_MESSAGE_REPLAY_CONFLICT");
  }
}

function assertExecutionFence(
  result: Exclude<DeliveryExecutionResult, { kind: "rejected_before_write" }>,
  fence: DeliveryFence,
): void {
  if (!sameFence(result.fence, fence)) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
  if (
    !Number.isSafeInteger(result.invocation.invocationGeneration) ||
    result.invocation.invocationGeneration < 1
  ) {
    throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
  }
}

function noticeInput(
  pending: PendingDelivery,
  notice: AttentionNotice,
): NoticeVisibilityInput {
  return {
    sessionId: pending.sessionId,
    target: notice.target,
    membershipEpoch: pending.delivery.membershipEpoch,
    firstMessageId: notice.firstMessageId,
    latestMessageId: notice.latestMessageId,
    firstServerSeq: notice.firstServerSeq,
    latestServerSeq: notice.latestServerSeq,
    inputDeliveryId: pending.delivery.deliveryId,
    inputAttempt: pending.delivery.attempt,
  };
}

export class DeliveryKernel {
  readonly #journal: DeliveryJournalPort;
  readonly #execution: DeliveryExecutionPort;
  readonly #server: DeliveryServerCommitPort;
  readonly #ids: DeliveryCommandIdDerivationPort;
  readonly #clock: DeliveryClock;

  constructor(input: {
    journal: DeliveryJournalPort;
    execution: DeliveryExecutionPort;
    server: DeliveryServerCommitPort;
    ids: DeliveryCommandIdDerivationPort;
    clock: DeliveryClock;
  }) {
    this.#journal = input.journal;
    this.#execution = input.execution;
    this.#server = input.server;
    this.#ids = input.ids;
    this.#clock = input.clock;
  }

  async acceptNotice(
    candidate: NativeDeliveryEnvelope,
    ingress: DeliveryIngressFence,
    receivedAt = this.#clock.now(),
  ): Promise<"inserted" | "replayed"> {
    const delivery = parseDelivery(candidate);
    if (
      delivery.agentId !== ingress.agentId ||
      delivery.machineId !== ingress.machineId ||
      delivery.expectedLaunchId !== ingress.launchId
    ) {
      throw new DeliveryKernelError("STALE_DELIVERY_FENCE");
    }
    return this.#journal.acceptNotice({ delivery, receivedAt });
  }

  async drainOne(activation: DeliveryActivation): Promise<DeliveryDrainResult> {
    if (activation.localState !== "activated" || activation.serverState !== "activated") {
      return { kind: "held", reason: "ACTIVATION_PREDECESSOR_REQUIRED" };
    }

    const records = (await this.#journal.pendingFor(activation)).map((pending) =>
      validatePending(pending, activation)
    );
    if (records.some((record) => record.state === "held_ambiguous")) {
      return { kind: "held", reason: "AMBIGUOUS_NATIVE_WRITE" };
    }
    const pending = selectOldestEligible(records);
    if (pending === undefined) {
      return { kind: "idle" };
    }

    const fence = fenceFor(pending);
    const visible = await this.#journal.findVisible({
      sessionId: pending.sessionId,
      target: pending.delivery.target,
      messageId: pending.delivery.messageId,
    });
    if (visible !== null) {
      assertVisibleTuple(visible, pending);
      return this.#repairVisible(pending, fence, visible);
    }

    const freshNotices: AttentionNotice[] = [];
    for (const notice of pending.attention) {
      const comparison = await this.#journal.compareNoticeVisibility(
        noticeInput(pending, notice),
      );
      if (comparison === "new") freshNotices.push(notice);
    }

    const result = await this.#execution.execute({
      delivery: pending.delivery,
      fence,
      stateInstanceId: pending.stateInstanceId,
      attention: freshNotices,
      mode: { kind: "new_input" },
    });
    if (result.kind === "rejected_before_write") {
      return { kind: "deferred", reason: "REJECTED_BEFORE_WRITE" };
    }
    assertExecutionFence(result, fence);
    if (result.kind === "not_written") {
      return { kind: "deferred", reason: "PROVEN_NOT_WRITTEN" };
    }
    if (result.kind === "ambiguous") {
      await this.#journal.holdAmbiguous({
        fence,
        stateInstanceId: pending.stateInstanceId,
        invocation: result.invocation,
        heldAt: this.#clock.now(),
      });
      return { kind: "held", reason: "AMBIGUOUS_NATIVE_WRITE" };
    }
    if (result.kind === "model_visible_ack_pending") {
      assertModelVisibleEvidence(result);
      await this.#commitFreshNotices(pending, result.inputWritten, freshNotices);
      return { kind: "deferred", reason: "MODEL_VISIBLE_ACK_PENDING" };
    }
    return this.#commitVisible(pending, fence, result, freshNotices);
  }

  async #repairVisible(
    pending: PendingDelivery,
    fence: DeliveryFence,
    visible: VisibleMessageRecord,
  ): Promise<DeliveryDrainResult> {
    const result = await this.#execution.execute({
      delivery: pending.delivery,
      fence,
      stateInstanceId: pending.stateInstanceId,
      attention: [],
      mode: { kind: "repair_visible", visible },
    });
    if (result.kind !== "model_visible") {
      throw new DeliveryKernelError("MODEL_VISIBLE_PREDECESSOR_REQUIRED");
    }
    assertExecutionFence(result, fence);
    assertModelVisibleEvidence(result);
    const draft = assertTerminalDraft(result, pending, fence);
    assertModelVisibleAck(
      result.modelVisibleAck,
      result.invocation,
      visible.modelVisibleReceiptId,
    );
    await this.#commitOutputs(pending, fence, result, draft);
    return { kind: "replayed", deliveryId: pending.delivery.deliveryId };
  }

  async #commitVisible(
    pending: PendingDelivery,
    fence: DeliveryFence,
    result: Extract<DeliveryExecutionResult, { kind: "model_visible" }>,
    freshNotices: readonly AttentionNotice[],
  ): Promise<DeliveryDrainResult> {
    assertModelVisibleEvidence(result);
    assertModelVisibleAck(result.modelVisibleAck, result.invocation);
    const draft = assertTerminalDraft(result, pending, fence);
    await this.#commitFreshNotices(pending, result.inputWritten, freshNotices);
    const visibleAt = this.#clock.now();
    await this.#journal.commitVisibleMessage({
      sessionId: pending.sessionId,
      target: pending.delivery.target,
      messageId: pending.delivery.messageId,
      deliveryId: pending.delivery.deliveryId,
      attempt: pending.delivery.attempt,
      serverSeq: pending.delivery.serverSeq,
      modelVisibleAck: {
        observed: true,
        receiptId: result.modelVisibleAck.result.receiptId,
      },
      visibleAt,
    });
    const committed = await this.#commitOutputs(pending, fence, result, draft);
    return committed.task === undefined
      ? {
          kind: "completed",
          deliveryId: pending.delivery.deliveryId,
          invocation: result.invocation,
          reply: committed.reply.result,
        }
      : {
          kind: "completed",
          deliveryId: pending.delivery.deliveryId,
          invocation: result.invocation,
          reply: committed.reply.result,
          task: committed.task.result,
        };
  }

  async #commitFreshNotices(
    pending: PendingDelivery,
    inputWritten: Extract<
      DeliveryExecutionResult,
      { kind: "model_visible" | "model_visible_ack_pending" }
    >["inputWritten"],
    freshNotices: readonly AttentionNotice[],
  ): Promise<void> {
    if (inputWritten.entryId.length === 0) {
      throw new DeliveryKernelError("MODEL_VISIBLE_PREDECESSOR_REQUIRED");
    }
    const now = this.#clock.now();
    for (const notice of freshNotices) {
      await this.#journal.commitNoticeVisibility({
        ...noticeInput(pending, notice),
        committedAt: now,
      });
    }
  }

  async #commitOutputs(
    pending: PendingDelivery,
    fence: DeliveryFence,
    result: Extract<DeliveryExecutionResult, { kind: "model_visible" }>,
    draft: TurnTerminalStage,
  ): Promise<{
    reply: Awaited<ReturnType<DeliveryServerCommitPort["appendReply"]>>;
    terminal: TurnTerminalResult;
    task?: Awaited<ReturnType<DeliveryServerCommitPort["applyCoordination"]>>;
  }> {
    if (!messageBodyHasContent(result.reply.text)) {
      throw new DeliveryKernelError("EMPTY_NATIVE_REPLY");
    }
    const terminalPredecessor = assertTerminalPredecessor(
      result.terminal,
      draft,
      result,
      pending,
      fence,
    );
    const replyCommandId = this.#ids.deterministicCommandId(replyCommandSeed(draft));
    if (
      !COMMAND_ID.test(replyCommandId) ||
      (draft.coordinationRequest.kind === "requested" &&
        replyCommandId === draft.coordinationRequest.commandId)
    ) {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
    const basis: TurnTerminalCommitBasis = {
      reply: { ...draft.basis.reply, replyCommandId },
      ...(draft.basis.coordination === undefined
        ? {}
        : { coordination: { ...draft.basis.coordination } }),
      completed: { ...draft.basis.completed },
    };
    const reply = await this.#server.appendReply({
      fence,
      commandId: replyCommandId,
      text: result.reply.text,
    });
    assertServerEvidence(reply);
    assertReplyResultEvidence(reply);
    if (
      !Number.isSafeInteger(reply.result.causalOrder) ||
      reply.result.causalOrder < 1
    ) {
      throw new DeliveryKernelError("SERVER_COMMIT_EVIDENCE_MISMATCH");
    }
    let task: Awaited<ReturnType<DeliveryServerCommitPort["applyCoordination"]>> | undefined;
    let coordination: TurnCoordinationDisposition = {
      kind: "not_requested",
      terminalTurnId: draft.contribution.fence.turnId,
    };
    if (draft.coordinationRequest.kind === "requested") {
      if (terminalPredecessor === null) {
        task = await this.#server.applyCoordination({
          fence,
          commandId: draft.coordinationRequest.commandId,
          command: draft.coordinationRequest.command,
          replyReceiptId: reply.result.receiptId,
        });
        assertServerEvidence(task);
        coordination = assertCoordinationResultEvidence(
          draft.coordinationRequest.commandId,
          task,
        );
        if (
          !Number.isSafeInteger(task.result.causalOrder) ||
          task.result.causalOrder <= reply.result.causalOrder
        ) {
          throw new DeliveryKernelError("SERVER_COMMIT_EVIDENCE_MISMATCH");
        }
      } else {
        coordination = terminalPredecessor.coordination;
      }
    }
    let bound: TurnTerminalResult;
    try {
      bound = draft.coordinationRequest.kind === "requested"
        ? terminalPredecessor === null
          ? result.terminal.bindCoordinationResult(
            coordination as Extract<
              TurnCoordinationDisposition,
              { kind: "committed" | "terminal_replay" }
            >,
          )
          : result.terminal.terminalResult()
        : result.terminal.terminalResult();
    } catch {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
    const terminal = assertTerminalResult(
      bound,
      draft,
      result,
      pending,
      fence,
      draft.coordinationRequest.kind === "requested"
        ? coordination as Extract<
            TurnCoordinationDisposition,
            { kind: "committed" | "terminal_replay" }
          >
        : undefined,
    );
    let reread;
    try {
      reread = result.terminal.terminalResult();
    } catch {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
    const stable = assertTerminalResult(
      reread,
      draft,
      result,
      pending,
      fence,
      draft.coordinationRequest.kind === "requested"
        ? coordination as Extract<
            TurnCoordinationDisposition,
            { kind: "committed" | "terminal_replay" }
          >
        : undefined,
    );
    if (protocolDigest(stable) !== protocolDigest(terminal)) {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
    let evidence;
    try {
      evidence = verifyTurnCompletionEvidence({
        contribution: stable.contribution,
        reply: {
          receiptId: reply.result.receiptId,
          resultDigest: reply.resultDigest,
        },
        coordination: stable.coordination,
      }, { expectedFence: fence });
    } catch {
      throw new DeliveryKernelError("DELIVERY_EXECUTION_FENCE_MISMATCH");
    }
    const committed = await this.#journal.commitTurnTerminal({
      ...draft.readerFence,
      basis,
      evidence,
      expected: draft.expected,
      next: draft.next,
      recordedAt: this.#clock.now(),
    });
    assertTerminalCommitResult(committed, draft);
    return task === undefined
      ? { reply, terminal: stable }
      : { reply, terminal: stable, task };
  }
}
