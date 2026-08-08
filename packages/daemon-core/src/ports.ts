import type {
  AcquireConsumePermit,
  ArtifactDigest,
  AttentionNotice,
  BeginNativeWrite,
  CommandId,
  ConsumePermit,
  DeliveryAck,
  DeliveryAckResult,
  DeliveryFence,
  InputWrittenJournalEntry,
  InvocationJournalEntry,
  MessageId,
  ModelVisibleJournalEntry,
  NativeDeliveryEnvelope,
  NativeInvocationFence,
  ProducerFactId,
  ProtocolVersion,
  ReconcileDeliveryAttempt,
  ReconcileDeliveryResult,
  ReceiptId,
  ResumeConsumePermit,
  ScriptedNotWrittenProof,
  SessionId,
  SimpleTaskCommand,
  StandingManifest,
  TaskId,
  TurnId,
  WriteStartedJournalEntry,
} from "@swarm/protocol";

export type ReplyCommitCommand = {
  protocolVersion: ProtocolVersion;
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  incomingProducerFactId: ProducerFactId;
  sourceMessageId: MessageId;
  turnId: TurnId;
  text: string;
};

export type ReplyCommitResult = {
  replyMessageId: MessageId;
  receiptId: ReceiptId;
  causalOrder: number;
};

export type TaskCommitCommand = {
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  incomingProducerFactId: ProducerFactId;
  turnId: TurnId;
  command: SimpleTaskCommand;
};

export type TaskCommitResult = {
  taskId: TaskId;
  receiptId: ReceiptId;
  causalOrder: number;
};

export interface NativeServerPort {
  acquireConsumePermit(command: AcquireConsumePermit): Promise<ConsumePermit>;
  resumeConsumePermit(command: ResumeConsumePermit): Promise<ConsumePermit>;
  beginNativeWrite(command: BeginNativeWrite): Promise<void>;
  acknowledgeDelivery(command: DeliveryAck): Promise<DeliveryAckResult>;
  reconcileDeliveryAttempt(command: ReconcileDeliveryAttempt): Promise<ReconcileDeliveryResult>;
  appendReply(command: ReplyCommitCommand): Promise<ReplyCommitResult>;
  createTask(command: TaskCommitCommand): Promise<TaskCommitResult>;
}

export type JournalRecovery =
  | { kind: "consumed" }
  | { kind: "held_ambiguous" }
  | {
      kind: "terminal_suppression";
      reason: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME";
    }
  | { kind: "reconcile"; command: ReconcileDeliveryAttempt };

export interface NativeJournalPort {
  recordDelivery(delivery: NativeDeliveryEnvelope, fence: DeliveryFence): Promise<void>;
  recoveryFor(fence: DeliveryFence): Promise<JournalRecovery | null>;
  recordPrePermitDisconnect(fence: DeliveryFence, disconnectId: CommandId): Promise<void>;
  recordPermit(permit: ConsumePermit): Promise<InvocationJournalEntry<"permit_recorded">>;
  recordWriteStarted(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    inputDigest: ArtifactDigest;
  }): Promise<WriteStartedJournalEntry>;
  recordNotWritten(proof: ScriptedNotWrittenProof): Promise<void>;
  recordInputWritten(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    runtimeWriteId: CommandId;
  }): Promise<InputWrittenJournalEntry>;
  recordModelVisible(input: {
    fence: DeliveryFence;
    invocation: NativeInvocationFence;
    permitId: CommandId;
    runtimeWriteId: CommandId;
    visibilityEventId: CommandId;
  }): Promise<ModelVisibleJournalEntry>;
  recordSuppressed(
    fence: DeliveryFence,
    reason: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME",
  ): Promise<void>;
  markAmbiguous(fence: DeliveryFence, invocation: NativeInvocationFence): Promise<void>;
  markConsumed(fence: DeliveryFence, invocation: NativeInvocationFence): Promise<void>;
}

export interface NativeCommandIdSource {
  nextCommandId(): CommandId;
}

export type NativeTurnRequest = {
  delivery: NativeDeliveryEnvelope;
  standingManifest: StandingManifest;
  attention: readonly AttentionNotice[];
  sessionId: SessionId;
  turnId: TurnId;
};

export type NativeTurnResult =
  | {
      kind: "completed";
      invocation: NativeInvocationFence;
      reply: ReplyCommitResult;
      task?: TaskCommitResult;
    }
  | {
      kind: "suppressed";
      reason: "MEMBERSHIP_REVOKED_BEFORE_CONSUME" | "ROUTE_SUPERSEDED_BEFORE_CONSUME";
    }
  | { kind: "requeued"; nextAttempt: number }
  | { kind: "held"; reason: "AMBIGUOUS_NATIVE_WRITE" | "BOUNDARY_REPAIRED" };
