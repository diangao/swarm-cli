import {
  canonicalProtocolJson,
  parseConsumePermit,
  parseDeliveryAckResult,
  parseNativeDeliveryEnvelope,
  parseReconcileDeliveryResult,
  type AcquireConsumePermit,
  type ArtifactDigest,
  type BeginNativeWrite,
  type CommandId,
  type ConsumePermit,
  type DeliveryAck,
  type DeliveryAckResult,
  type DeliveryFence,
  type NativeInvocationFence,
  type ReconcileDeliveryResult,
  type ResumeConsumePermit,
} from "@swarm/protocol";
import {
  NativeEventNormalizer,
  type NativeRuntimePort,
} from "@swarm/drivers";
import {
  compileNativeTurnInput,
  freezeStandingManifest,
  protocolDigest,
} from "@swarm/runtime-contract";

import {
  DaemonCoreError,
  PrePermitDisconnectError,
} from "./errors.js";
import type {
  NativeCommandIdSource,
  NativeJournalPort,
  NativeServerPort,
  NativeTurnRequest,
  NativeTurnResult,
  ReplyCommitCommand,
  TaskCommitCommand,
} from "./ports.js";

const SUPPRESSION_CODES = new Set([
  "MEMBERSHIP_REVOKED_BEFORE_CONSUME",
  "ROUTE_SUPERSEDED_BEFORE_CONSUME",
] as const);

type SuppressionCode = "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME";

function digestRequest<Value extends Record<string, unknown>>(value: Value): ArtifactDigest {
  return protocolDigest(value);
}

function fenceFor(request: NativeTurnRequest): DeliveryFence {
  const delivery = request.delivery;
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
    sessionId: request.sessionId,
    turnId: request.turnId,
  };
}

function commandWithDigest<Value extends Record<string, unknown>>(
  value: Value,
): Value & { requestDigest: ArtifactDigest } {
  return { ...value, requestDigest: digestRequest(value) };
}

function sameInvocation(left: NativeInvocationFence, right: NativeInvocationFence): boolean {
  return left.invocationGeneration === right.invocationGeneration && left.invocationId === right.invocationId;
}

function assertPermitFence(permit: ConsumePermit, fence: DeliveryFence): void {
  for (const key of [
    "protocolVersion", "deliveryId", "attempt", "producerFactId", "agentId", "machineId",
    "launchId", "membershipEpoch", "routingGeneration", "routeVersion", "sessionId", "turnId",
  ] as const) {
    if (permit[key] !== fence[key]) throw new DaemonCoreError("PERMIT_FENCE_MISMATCH");
  }
}

function suppressionCode(error: unknown): SuppressionCode | undefined {
  if (!(error instanceof Error)) return undefined;
  return SUPPRESSION_CODES.has(error.message as SuppressionCode)
    ? error.message as SuppressionCode
    : undefined;
}

export class DaemonCore {
  readonly #server: NativeServerPort;
  readonly #journal: NativeJournalPort;
  readonly #driver: NativeRuntimePort;
  readonly #ids: NativeCommandIdSource;

  constructor(input: {
    server: NativeServerPort;
    journal: NativeJournalPort;
    driver: NativeRuntimePort;
    ids: NativeCommandIdSource;
  }) {
    this.#server = input.server;
    this.#journal = input.journal;
    this.#driver = input.driver;
    this.#ids = input.ids;
  }

  async runTurn(request: NativeTurnRequest): Promise<NativeTurnResult> {
    const delivery = parseNativeDeliveryEnvelope(
      canonicalProtocolJson(request.delivery),
      request.delivery.protocolVersion,
    );
    const stableRequest = { ...request, delivery };
    const fence = fenceFor(stableRequest);
    await this.#journal.recordDelivery(delivery, fence);

    const recovery = await this.#journal.recoveryFor(fence);
    if (recovery?.kind === "consumed") {
      return { kind: "held", reason: "BOUNDARY_REPAIRED" };
    }
    if (recovery?.kind === "held_ambiguous") {
      return { kind: "held", reason: "AMBIGUOUS_NATIVE_WRITE" };
    }
    if (recovery?.kind === "terminal_suppression") {
      return { kind: "suppressed", reason: recovery.reason };
    }

    let permit: ConsumePermit;
    if (recovery?.kind === "reconcile") {
      const reconciled = await this.#server.reconcileDeliveryAttempt(recovery.command);
      const outcome = await this.#resumeAfterReconcile(fence, reconciled);
      if ("result" in outcome) return outcome.result;
      permit = outcome.permit;
    } else {
      const acquireBase = {
        ...fence,
        commandId: this.#ids.nextCommandId(),
        boundary: "daemon_accepted" as const,
      };
      const acquire = commandWithDigest(acquireBase) as AcquireConsumePermit;
      try {
        permit = await this.#server.acquireConsumePermit(acquire);
      } catch (error) {
        const suppressed = suppressionCode(error);
        if (suppressed !== undefined) {
          await this.#journal.recordSuppressed(fence, suppressed);
          return { kind: "suppressed", reason: suppressed };
        }
        if (!(error instanceof PrePermitDisconnectError)) throw error;
        await this.#journal.recordPrePermitDisconnect(fence, this.#ids.nextCommandId());
        const evidence = await this.#journal.recoveryFor(fence);
        if (evidence?.kind !== "reconcile") {
          throw new DaemonCoreError("RECONCILIATION_EVIDENCE_REQUIRED");
        }
        const reconciled = parseReconcileDeliveryResult(canonicalProtocolJson(
          await this.#server.reconcileDeliveryAttempt(evidence.command),
        ));
        if (reconciled.kind !== "pre_permit_requeued") {
          throw new DaemonCoreError("RECONCILIATION_RESULT_MISMATCH");
        }
        return { kind: "requeued", nextAttempt: reconciled.nextAttempt };
      }
    }

    return this.#runPermitted(stableRequest, fence, permit);
  }

  async #resumeAfterReconcile(
    fence: DeliveryFence,
    candidate: ReconcileDeliveryResult,
  ): Promise<{ permit: ConsumePermit } | { result: NativeTurnResult }> {
    const result = parseReconcileDeliveryResult(canonicalProtocolJson(candidate));
    if (result.kind === "pre_permit_requeued") {
      return { result: { kind: "requeued", nextAttempt: result.nextAttempt } };
    }
    if (result.kind === "held_ambiguous") {
      return { result: { kind: "held", reason: "AMBIGUOUS_NATIVE_WRITE" } };
    }
    if (result.kind === "boundary_repaired") {
      return { result: { kind: "held", reason: "BOUNDARY_REPAIRED" } };
    }
    const resumeBase = {
      ...fence,
      commandId: this.#ids.nextCommandId(),
      permitId: result.permitId,
      expectedActiveInvocationGeneration: result.expectedActiveInvocationGeneration,
      resumeMode: result.resumeMode,
      boundary: "daemon_accepted" as const,
    };
    const resume = commandWithDigest(resumeBase) as ResumeConsumePermit;
    try {
      return { permit: await this.#server.resumeConsumePermit(resume) };
    } catch (error) {
      const suppressed = suppressionCode(error);
      if (suppressed === undefined) throw error;
      await this.#journal.recordSuppressed(fence, suppressed);
      return { result: { kind: "suppressed", reason: suppressed } };
    }
  }

  async #runPermitted(
    request: NativeTurnRequest,
    fence: DeliveryFence,
    initialPermit: ConsumePermit,
  ): Promise<NativeTurnResult> {
    let permit = initialPermit;
    for (let generationCount = 0; generationCount < 32; generationCount += 1) {
      permit = parseConsumePermit(canonicalProtocolJson(permit), permit.protocolVersion);
      assertPermitFence(permit, fence);
      const invocation: NativeInvocationFence = {
        invocationGeneration: permit.invocationGeneration,
        invocationId: permit.invocationId,
      };
      await this.#journal.recordPermit(permit);
      const frozenManifest = freezeStandingManifest(request.standingManifest);
      const turn = compileNativeTurnInput({
        frozenManifest,
        delivery: request.delivery,
        body: permit.body,
        attention: request.attention,
      });
      const writeStarted = await this.#journal.recordWriteStarted({
        fence,
        invocation,
        permitId: permit.permitId,
        inputDigest: turn.inputDigest,
      });
      const beginBase = {
        ...fence,
        ...invocation,
        commandId: this.#ids.nextCommandId(),
        permitId: permit.permitId,
        boundary: "write_started" as const,
        inputDigest: turn.inputDigest,
        writeStartedEntryId: writeStarted.entryId,
        writeStartedEntryDigest: writeStarted.entryDigest,
      };
      const begin = commandWithDigest(beginBase) as BeginNativeWrite;
      await this.#server.beginNativeWrite(begin);

      const write = await this.#driver.writeTurn(turn, {
        ...invocation,
        writeStartedEntryId: writeStarted.entryId,
        writeStartedEntryDigest: writeStarted.entryDigest,
      });
      if (write.kind === "ambiguous") {
        await this.#journal.markAmbiguous(fence, invocation);
        return { kind: "held", reason: "AMBIGUOUS_NATIVE_WRITE" };
      }
      if (write.kind === "not_written") {
        if (this.#driver.driverKind !== "scripted_fake") {
          throw new Error("REAL_DRIVER_NEGATIVE_PROOF_FORBIDDEN");
        }
        if (
          write.proof.invocationId !== invocation.invocationId
          || write.proof.invocationGeneration !== invocation.invocationGeneration
          || write.proof.writeStartedEntryId !== writeStarted.entryId
          || write.proof.writeStartedEntryDigest !== writeStarted.entryDigest
        ) {
          throw new Error("INVALID_JOURNAL_CHAIN");
        }
        await this.#journal.recordNotWritten(write.proof);
        const recovery = await this.#journal.recoveryFor(fence);
        if (recovery?.kind !== "reconcile") {
          throw new DaemonCoreError("RECONCILIATION_EVIDENCE_REQUIRED");
        }
        const reconciled = await this.#server.reconcileDeliveryAttempt(recovery.command);
        const resumed = await this.#resumeAfterReconcile(fence, reconciled);
        if ("result" in resumed) return resumed.result;
        permit = resumed.permit;
        continue;
      }

      await this.#journal.recordInputWritten({
        fence,
        invocation,
        permitId: permit.permitId,
        runtimeWriteId: write.runtimeWriteId,
      });
      await this.#acknowledge(fence, invocation, permit.permitId, "input_written");
      await this.#journal.recordModelVisible({
        fence,
        invocation,
        permitId: permit.permitId,
        runtimeWriteId: write.runtimeWriteId,
        visibilityEventId: write.visibilityEventId,
      });
      await this.#acknowledge(fence, invocation, permit.permitId, "model_visible");

      const normalizer = new NativeEventNormalizer(request.delivery.messageId);
      let replyText: string | undefined;
      let coordination: Extract<ReturnType<NativeEventNormalizer["accept"]>, { kind: "coordination" }> | undefined;
      for await (const event of write.events) {
        const action = normalizer.accept(event);
        if (action.kind === "reply") {
          replyText = action.text;
        } else if (action.kind === "coordination") {
          coordination = action;
        }
      }
      normalizer.finish();
      if (replyText === undefined) throw new DaemonCoreError("RECONCILIATION_RESULT_MISMATCH");
      const replyBase = {
        protocolVersion: fence.protocolVersion,
        commandId: this.#ids.nextCommandId(),
        incomingProducerFactId: fence.producerFactId,
        sourceMessageId: request.delivery.messageId,
        turnId: fence.turnId,
        text: replyText,
      };
      const reply = await this.#server.appendReply(
        commandWithDigest(replyBase) as ReplyCommitCommand,
      );
      let task: Awaited<ReturnType<NativeServerPort["createTask"]>> | undefined;
      if (coordination !== undefined) {
        const taskBase = {
          commandId: coordination.commandId,
          incomingProducerFactId: fence.producerFactId,
          turnId: fence.turnId,
          command: coordination.command,
        };
        task = await this.#server.createTask(
          commandWithDigest(taskBase) as TaskCommitCommand,
        );
      }
      await this.#journal.markConsumed(fence, invocation);
      return task === undefined
        ? { kind: "completed", invocation, reply }
        : { kind: "completed", invocation, reply, task };
    }
    throw new DaemonCoreError("TOO_MANY_INVOCATION_GENERATIONS");
  }

  async #acknowledge(
    fence: DeliveryFence,
    invocation: NativeInvocationFence,
    permitId: CommandId,
    boundary: "input_written" | "model_visible",
  ): Promise<void> {
    const ackBase = {
      ...fence,
      ...invocation,
      commandId: this.#ids.nextCommandId(),
      permitId,
      boundary,
    };
    const ack = commandWithDigest(ackBase) as DeliveryAck;
    let candidate: DeliveryAckResult;
    try {
      candidate = await this.#server.acknowledgeDelivery(ack);
    } catch (error) {
      const recovery = await this.#journal.recoveryFor(fence);
      if (recovery?.kind !== "reconcile") throw error;
      const reconciled = parseReconcileDeliveryResult(canonicalProtocolJson(
        await this.#server.reconcileDeliveryAttempt(recovery.command),
      ));
      if (
        reconciled.kind !== "boundary_repaired"
        || !reconciled.repaired.some((repaired) => repaired === boundary)
      ) {
        throw error;
      }
      return;
    }
    const result = parseDeliveryAckResult(canonicalProtocolJson(candidate));
    if (result.boundary !== boundary || !sameInvocation(result.invocation, invocation)) {
      throw new DaemonCoreError("ACK_RESULT_MISMATCH");
    }
  }
}
