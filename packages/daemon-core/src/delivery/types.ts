import type {
  AgentId,
  ArtifactDigest,
  AttentionNotice,
  CommandId,
  DeliveryFence,
  DeliveryAckResult,
  DeliveryId,
  MachineId,
  MessageId,
  NativeDeliveryEnvelope,
  NativeInvocationFence,
  ReceiptId,
  SessionId,
  SimpleTaskCommand,
  StateInstanceId,
  Target,
  TurnId,
} from "@swarm/protocol";

import type {
  ReplyCommitResult,
  TaskCommitResult,
} from "../ports.js";
import type {
  CommitTurnTerminalInput,
  TurnStepResult,
  TurnTerminalBindingPort,
} from "../turn/index.js";

export type DeliveryActivation = {
  agentId: AgentId;
  machineId: MachineId;
  launchId: NativeDeliveryEnvelope["expectedLaunchId"];
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  localState: "starting" | "spawned" | "ready" | "activated" | "stopping" | "terminal";
  serverState: "requested" | "ready" | "activated" | "terminal";
};

export type DeliveryIngressFence = Pick<
  DeliveryActivation,
  "agentId" | "machineId" | "launchId"
>;

export type PendingDelivery = {
  delivery: NativeDeliveryEnvelope;
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  turnId: TurnId;
  targetKey: string;
  receiveOrdinal: number;
  state: "pending" | "held_ambiguous";
  attention: readonly AttentionNotice[];
};

export type InputWrittenEvidence = {
  entryId: CommandId;
  entryDigest: ArtifactDigest;
  runtimeWriteId: CommandId;
};

export type ModelVisibleEvidence = {
  entryId: CommandId;
  entryDigest: ArtifactDigest;
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
};

export type ObservedModelVisibleAck = ServerResultEvidence<
  Extract<DeliveryAckResult, { boundary: "model_visible" }>
> & {
  observed: true;
};

export type ServerResultEvidence<Result> = {
  result: Result;
  resultDigest: ArtifactDigest;
  disposition: "committed" | "terminal_replay";
};

export type VisibleMessageRecord = {
  deliveryId: DeliveryId;
  attempt: number;
  serverSeq: number;
  modelVisibleReceiptId: ReceiptId;
  visibleAt: string;
};

export type NoticeVisibilityInput = {
  sessionId: SessionId;
  target: Target;
  membershipEpoch: number;
  firstMessageId: MessageId;
  latestMessageId: MessageId;
  firstServerSeq: number;
  latestServerSeq: number;
  inputDeliveryId: DeliveryId;
  inputAttempt: number;
};

export type DeliveryJournalPort = {
  acceptNotice(input: {
    delivery: NativeDeliveryEnvelope;
    receivedAt: string;
  }): Promise<"inserted" | "replayed">;
  pendingFor(activation: DeliveryActivation): Promise<readonly PendingDelivery[]>;
  findVisible(input: {
    sessionId: SessionId;
    target: Target;
    messageId: MessageId;
  }): Promise<VisibleMessageRecord | null>;
  compareNoticeVisibility(input: NoticeVisibilityInput): Promise<"new" | "replay">;
  commitNoticeVisibility(input: NoticeVisibilityInput & {
    committedAt: string;
  }): Promise<void>;
  commitVisibleMessage(input: {
    sessionId: SessionId;
    target: Target;
    messageId: MessageId;
    deliveryId: DeliveryId;
    attempt: number;
    serverSeq: number;
    modelVisibleAck: { observed: true; receiptId: ReceiptId };
    visibleAt: string;
  }): Promise<void>;
  holdAmbiguous(input: {
    fence: DeliveryFence;
    stateInstanceId: StateInstanceId;
    invocation: NativeInvocationFence;
    heldAt: string;
  }): Promise<void>;
  /**
   * Atomically commits the staged terminal event suffix, full durable-turn CAS,
   * and verified completion evidence. Server reply/coordination effects are
   * predecessors; local output intent and placeholder evidence are forbidden.
   */
  commitTurnTerminal(input: CommitTurnTerminalInput): TurnStepResult;
};

export type DeliveryExecutionInput = {
  delivery: NativeDeliveryEnvelope;
  fence: DeliveryFence;
  stateInstanceId: StateInstanceId;
  attention: readonly AttentionNotice[];
  mode:
    | { kind: "new_input" }
    | { kind: "repair_visible"; visible: VisibleMessageRecord };
};

type FencedExecution = {
  fence: DeliveryFence;
  invocation: NativeInvocationFence;
};

export type DeliveryExecutionResult =
  | {
      kind: "rejected_before_write";
      reason: string;
    }
  | (FencedExecution & {
      kind: "not_written";
      proofDigest: ArtifactDigest;
    })
  | (FencedExecution & {
      kind: "ambiguous";
    })
  | (FencedExecution & {
      kind: "model_visible_ack_pending";
      inputWritten: InputWrittenEvidence;
      modelVisible: ModelVisibleEvidence;
    })
  | (FencedExecution & {
      kind: "model_visible";
      inputWritten: InputWrittenEvidence;
      modelVisible: ModelVisibleEvidence;
      modelVisibleAck: ObservedModelVisibleAck;
      terminal: TurnTerminalBindingPort;
      reply: { text: string };
    });

export type DeliveryExecutionPort = {
  /**
   * In `new_input`, acquires or resumes the exact body permit after activation,
   * only after the session's ordinary-turn CAS is claimed, then performs at
   * most one native input attempt or reconciles a journaled boundary. In
   * `repair_visible`, replays only journaled normalized output and never reads
   * the body or calls the native input surface. `model_visible_ack_pending`
   * requires durable input-written and model-visible entries but no terminal
   * server ACK; `model_visible` additionally requires the exact observed ACK
   * evidence and one stable staged terminal snapshot. The kernel derives the
   * reply command ID from that snapshot; callers cannot preallocate it. The
   * body never crosses the delivery kernel boundary or enters the machine queue.
   */
  execute(input: DeliveryExecutionInput): Promise<DeliveryExecutionResult>;
};

export type DeliveryServerCommitPort = {
  appendReply(input: {
    fence: DeliveryFence;
    commandId: CommandId;
    text: string;
  }): Promise<ServerResultEvidence<ReplyCommitResult>>;
  applyCoordination(input: {
    fence: DeliveryFence;
    commandId: CommandId;
    command: SimpleTaskCommand;
    replyReceiptId: ReceiptId;
  }): Promise<ServerResultEvidence<TaskCommitResult>>;
};

/**
 * Daemon-core owns the deterministic reply-command seed, while the app layer
 * owns the concrete ID algorithm. Keeping that dependency behind this port
 * prevents the delivery kernel from importing app code or accepting a
 * caller-preallocated reply command ID.
 */
export type DeliveryCommandIdDerivationPort = {
  deterministicCommandId(seed: string): CommandId;
};

export type DeliveryClock = {
  now(): string;
};

export type DeliveryDrainResult =
  | { kind: "idle" }
  | {
      kind: "held";
      reason: "ACTIVATION_PREDECESSOR_REQUIRED" | "AMBIGUOUS_NATIVE_WRITE";
    }
  | {
      kind: "deferred";
      reason:
        | "REJECTED_BEFORE_WRITE"
        | "PROVEN_NOT_WRITTEN"
        | "MODEL_VISIBLE_ACK_PENDING";
    }
  | {
      kind: "replayed";
      deliveryId: DeliveryId;
    }
  | {
      kind: "completed";
      deliveryId: DeliveryId;
      invocation: NativeInvocationFence;
      reply: ReplyCommitResult;
      task?: TaskCommitResult;
    };
