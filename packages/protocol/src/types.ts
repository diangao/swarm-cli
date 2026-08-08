declare const protocolBrand: unique symbol;

type Brand<Value, Name extends string> = Value & {
  readonly [protocolBrand]: Name;
};

export type ProtocolVersion = Brand<number, "ProtocolVersion">;
export type ServerId = Brand<string, "ServerId">;
export type MachineId = Brand<string, "MachineId">;
export type AgentId = Brand<string, "AgentId">;
export type HumanId = Brand<string, "HumanId">;
export type ChannelId = Brand<string, "ChannelId">;
export type ConversationId = Brand<string, "ConversationId">;
export type MessageId = Brand<string, "MessageId">;
export type DeliveryId = Brand<string, "DeliveryId">;
export type ProducerFactId = Brand<string, "ProducerFactId">;
export type TaskId = Brand<string, "TaskId">;
export type ClaimId = Brand<string, "ClaimId">;
export type LeaseId = Brand<string, "LeaseId">;
export type LaunchId = Brand<string, "LaunchId">;
export type CommandId = Brand<string, "CommandId">;
export type ReceiptId = Brand<string, "ReceiptId">;
export type StateInstanceId = Brand<string, "StateInstanceId">;
export type TurnId = Brand<string, "TurnId">;
export type SessionId = Brand<string, "SessionId">;
export type FenceToken = Brand<string, "FenceToken">;
export type ArtifactDigest = Brand<string, "ArtifactDigest">;

export type ProtocolSupport = {
  major: number;
  minMinor: number;
  maxMinor: number;
};

export type RuntimeKind = "codex" | "claude";

export type Target =
  | {
      kind: "channel";
      channelId: ChannelId;
      threadRootMessageId?: MessageId;
    }
  | {
      kind: "direct";
      conversationId: ConversationId;
      threadRootMessageId?: MessageId;
    };

export type DeliveryEnvelope = {
  protocolVersion: ProtocolVersion;
  deliveryId: DeliveryId;
  attempt: number;
  messageId: MessageId;
  target: Target;
  serverSeq: number;
  producerFactId: ProducerFactId;
  agentId: AgentId;
  machineId: MachineId;
  expectedLaunchId?: LaunchId;
  replayOf?: DeliveryId;
};

export type TaskLease = {
  taskId: TaskId;
  claimId: ClaimId;
  leaseId: LeaseId;
  leaseEpoch: number;
  fenceToken: FenceToken;
  attempt: number;
  acquiredAt: string;
  expiresAt: string;
};

export type LaunchCommand = {
  protocolVersion: ProtocolVersion;
  commandId: CommandId;
  agentId: AgentId;
  machineId: MachineId;
  launchId: LaunchId;
  runtime: RuntimeKind;
  workspaceGeneration: number;
  wakeDeliveryId?: DeliveryId;
};

export type ReceiptKind =
  | "server_accepted"
  | "claim_won"
  | "daemon_accepted"
  | "process_spawned"
  | "runtime_ready"
  | "input_written"
  | "model_visible"
  | "side_effect_applied"
  | "artifact_published"
  | "review_verdict";

export type ReceiptActor =
  | { serverId: ServerId }
  | { serverId: ServerId; agentId: AgentId }
  | { machineId: MachineId; agentId: AgentId };

export type ReceiptFence =
  | Record<string, never>
  | { leaseEpoch: number; fenceToken: FenceToken }
  | { launchId: LaunchId; stateInstanceId: StateInstanceId }
  | {
      launchId: LaunchId;
      stateInstanceId: StateInstanceId;
      sessionId: SessionId;
    }
  | {
      launchId: LaunchId;
      stateInstanceId: StateInstanceId;
      turnId: TurnId;
      sessionId: SessionId;
    }
  | {
      leaseEpoch: number;
      fenceToken: FenceToken;
      launchId: LaunchId;
      stateInstanceId: StateInstanceId;
      turnId: TurnId;
      sessionId: SessionId;
    };

type ReceiptBase<
  Kind extends ReceiptKind,
  Actor extends ReceiptActor,
  Fence extends ReceiptFence,
> = {
  protocolVersion: ProtocolVersion;
  receiptId: ReceiptId;
  kind: Kind;
  producerFactId: ProducerFactId;
  actor: Actor;
  fence: Fence;
  occurredAt: string;
};

type ServerActor = { serverId: ServerId };
type ClaimActor = { serverId: ServerId; agentId: AgentId };
type DaemonActor = { machineId: MachineId; agentId: AgentId };
type EmptyFence = Record<string, never>;
type LeaseFence = { leaseEpoch: number; fenceToken: FenceToken };
type LaunchFence = { launchId: LaunchId; stateInstanceId: StateInstanceId };
type SessionFence = LaunchFence & { sessionId: SessionId };
type TurnFence = SessionFence & { turnId: TurnId };
type LeaseTurnFence = LeaseFence & TurnFence;

export type TransitionReceipt =
  | ReceiptBase<"server_accepted", ServerActor, EmptyFence>
  | ReceiptBase<"claim_won", ClaimActor, LeaseFence>
  | ReceiptBase<"daemon_accepted", DaemonActor, EmptyFence>
  | ReceiptBase<"process_spawned", DaemonActor, LaunchFence>
  | ReceiptBase<"runtime_ready", DaemonActor, SessionFence>
  | ReceiptBase<"input_written", DaemonActor, TurnFence>
  | ReceiptBase<"model_visible", DaemonActor, TurnFence>
  | ReceiptBase<"side_effect_applied", DaemonActor, TurnFence | LeaseTurnFence>
  | (ReceiptBase<"artifact_published", DaemonActor, LeaseTurnFence> & {
      artifactDigest: ArtifactDigest;
    })
  | (ReceiptBase<"review_verdict", DaemonActor, LeaseTurnFence> & {
      artifactDigest: ArtifactDigest;
    });

export type AttentionNotice = {
  protocolVersion: ProtocolVersion;
  target: Target;
  pendingCount: number;
  firstMessageId: MessageId;
  latestMessageId: MessageId;
  firstServerSeq: number;
  latestServerSeq: number;
};

export type StandingManifest = {
  protocolVersion: ProtocolVersion;
  agentId: AgentId;
  runtime: RuntimeKind;
  workspaceGeneration: number;
  identityDigest: ArtifactDigest;
  memoryDigest: ArtifactDigest;
  cliContractDigest: ArtifactDigest;
  capabilityDigest: ArtifactDigest;
};

export type FrozenStandingManifest = {
  manifest: StandingManifest;
  manifestDigest: ArtifactDigest;
};

export type NativeDeliveryEnvelope = DeliveryEnvelope & {
  expectedLaunchId: LaunchId;
  membershipEpoch: number;
  routingGeneration: number;
  routeVersion: number;
};

export type CurrentMessageInput = {
  delivery: NativeDeliveryEnvelope;
  body: string;
};

export type NativeTurnInput = {
  protocolVersion: ProtocolVersion;
  manifestDigest: ArtifactDigest;
  current: CurrentMessageInput;
  attention: readonly AttentionNotice[];
};

export type SimpleTaskCommand = {
  protocolVersion: ProtocolVersion;
  title: string;
  sourceMessageId: MessageId;
};

export type NativeRuntimeEvent =
  | { kind: "assistant_reply"; text: string }
  | { kind: "coordination_call"; commandId: CommandId; command: SimpleTaskCommand }
  | { kind: "turn_complete" };

export type DeliveryFence = {
  protocolVersion: ProtocolVersion;
  deliveryId: DeliveryId;
  attempt: number;
  producerFactId: ProducerFactId;
  agentId: AgentId;
  machineId: MachineId;
  launchId: LaunchId;
  membershipEpoch: number;
  routingGeneration: number;
  routeVersion: number;
  sessionId: SessionId;
  turnId: TurnId;
};

export type NativeInvocationFence = {
  invocationGeneration: number;
  invocationId: CommandId;
};

export type AcquireConsumePermit = DeliveryFence & {
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  boundary: "daemon_accepted";
};

export type ConsumePermit = DeliveryFence & NativeInvocationFence & {
  permitId: CommandId;
  body: string;
};

export type ResumeConsumePermit = DeliveryFence & {
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  permitId: CommandId;
  expectedActiveInvocationGeneration: number;
  resumeMode: "same_invocation_before_begin" | "next_after_not_written";
  boundary: "daemon_accepted";
};

export type BeginNativeWrite = DeliveryFence & NativeInvocationFence & {
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  permitId: CommandId;
  boundary: "write_started";
  inputDigest: ArtifactDigest;
  writeStartedEntryId: CommandId;
  writeStartedEntryDigest: ArtifactDigest;
};

export type DeliveryAck = DeliveryFence & NativeInvocationFence & {
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  permitId: CommandId;
  boundary: "input_written" | "model_visible";
};

export type DeliveryAckResult =
  | {
      boundary: "input_written";
      receiptId: ReceiptId;
      invocation: NativeInvocationFence;
      jobState: "held/INPUT_WRITTEN";
    }
  | {
      boundary: "model_visible";
      receiptId: ReceiptId;
      invocation: NativeInvocationFence;
      jobState: "acked/MODEL_VISIBLE";
    };

export type LocalJournalEntry<K extends string> = {
  journalId: CommandId;
  entryId: CommandId;
  sequence: number;
  kind: K;
  previousEntryDigest: ArtifactDigest | null;
  entryDigest: ArtifactDigest;
};

export type InvocationJournalEntry<K extends string> = LocalJournalEntry<K> &
  DeliveryFence &
  NativeInvocationFence & {
    permitId: CommandId;
  };

export type WriteStartedJournalEntry = InvocationJournalEntry<"write_started"> & {
  inputDigest: ArtifactDigest;
};

export type InputWrittenJournalEntry = InvocationJournalEntry<"input_written"> & {
  runtimeWriteId: CommandId;
};

export type ModelVisibleJournalEntry = InvocationJournalEntry<"model_visible"> & {
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
};

export type ScriptedNotWrittenProof = {
  driverKind: "scripted_fake";
  fixtureId: CommandId;
  scriptDigest: ArtifactDigest;
  invocationId: CommandId;
  invocationGeneration: number;
  writeStartedEntryId: CommandId;
  writeStartedEntryDigest: ArtifactDigest;
  outcomeOrdinal: number;
  outcome: "not_written";
  proofDigest: ArtifactDigest;
};

export type ReconcileEvidence =
  | { kind: "pre_permit_disconnect"; disconnectId: CommandId }
  | {
      kind: "permit_recorded_write_not_started";
      permitRecorded: InvocationJournalEntry<"permit_recorded">;
    }
  | {
      kind: "scripted_not_written";
      permitRecorded: InvocationJournalEntry<"permit_recorded">;
      writeStarted: WriteStartedJournalEntry;
      proof: ScriptedNotWrittenProof;
    }
  | {
      kind: "write_started_ambiguous";
      permitRecorded: InvocationJournalEntry<"permit_recorded">;
      writeStarted: WriteStartedJournalEntry;
      driverKind: "native_process" | "scripted_fake";
    }
  | {
      kind: "input_written";
      permitRecorded: InvocationJournalEntry<"permit_recorded">;
      writeStarted: WriteStartedJournalEntry;
      inputWritten: InputWrittenJournalEntry;
    }
  | {
      kind: "model_visibility_ambiguous";
      permitRecorded: InvocationJournalEntry<"permit_recorded">;
      writeStarted: WriteStartedJournalEntry;
      inputWritten: InputWrittenJournalEntry;
      driverKind: "native_process" | "scripted_fake";
    }
  | {
      kind: "model_visible";
      permitRecorded: InvocationJournalEntry<"permit_recorded">;
      writeStarted: WriteStartedJournalEntry;
      inputWritten: InputWrittenJournalEntry;
      modelVisible: ModelVisibleJournalEntry;
    };

export type ReconcileDeliveryAttempt = DeliveryFence & {
  commandId: CommandId;
  requestDigest: ArtifactDigest;
  permitId: CommandId | null;
  invocation: NativeInvocationFence | null;
  evidenceDigest: ArtifactDigest;
  evidence: ReconcileEvidence;
};

export type ReconcileDeliveryResult =
  | {
      kind: "pre_permit_requeued";
      jobState: "pending";
      replayOfAttempt: number;
      nextAttempt: number;
    }
  | {
      kind: "same_attempt_resumable";
      jobState: "held/CONSUME_PERMITTED";
      attempt: number;
      permitId: CommandId;
      resumeMode: "same_invocation_before_begin" | "next_after_not_written";
      expectedActiveInvocationGeneration: number;
      nextInvocationGeneration: number;
    }
  | {
      kind: "boundary_repaired";
      repaired: readonly ["input_written"] | readonly ["input_written", "model_visible"];
      jobState: "held/INPUT_WRITTEN" | "acked/MODEL_VISIBLE";
      attempt: number;
      permitId: CommandId;
      invocation: NativeInvocationFence;
    }
  | {
      kind: "held_ambiguous";
      jobState: "held/AMBIGUOUS_NATIVE_WRITE";
      attempt: number;
      permitId: CommandId;
      invocation: NativeInvocationFence;
    };

export type ObservationCursorAck = {
  protocolVersion: ProtocolVersion;
  actorId: HumanId | AgentId;
  stream: "client_message" | "agent_attention";
  target: Target;
  membershipEpoch: number;
  serverSeq: number;
};
