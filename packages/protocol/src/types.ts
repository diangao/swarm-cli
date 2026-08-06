declare const protocolBrand: unique symbol;

type Brand<Value, Name extends string> = Value & {
  readonly [protocolBrand]: Name;
};

export type ProtocolVersion = Brand<number, "ProtocolVersion">;
export type ServerId = Brand<string, "ServerId">;
export type MachineId = Brand<string, "MachineId">;
export type AgentId = Brand<string, "AgentId">;
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
