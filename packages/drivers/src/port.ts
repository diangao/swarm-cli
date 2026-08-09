import type {
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  DriverSession,
  DriverTurnBinding,
  LaunchId,
  LocalLaunchFence,
  NativeRuntimeEvent,
  NormalizedDriverEvent,
  ProtocolVersion,
  ReadyLaunchFence,
  ScriptedNotWrittenProof,
  SessionId,
  SpawnedLaunchFence,
  StateInstanceId,
  StopReason,
  TerminalReason,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";

export type NativeWriteBinding = {
  invocationId: CommandId;
  invocationGeneration: number;
  writeStartedEntryId: CommandId;
  writeStartedEntryDigest: ArtifactDigest;
};

export type NativeWrittenTurn = {
  kind: "written";
  runtimeWriteId: CommandId;
  visibilityEventId: CommandId;
  events: AsyncIterable<NativeRuntimeEvent>;
};

export type NativeWriteOutcome =
  | NativeWrittenTurn
  | { kind: "not_written"; proof: ScriptedNotWrittenProof }
  | { kind: "ambiguous" };

export type NativeProcessWriteOutcome =
  | NativeWriteOutcome
  | { kind: "rejected_before_write"; proof: DriverPreflightProof };

export type DriverPreflightProof = {
  kind: "daemon_preflight_rejection";
  proofId: CommandId;
  requestDigest: ArtifactDigest;
  reason: "capability_absent" | "invalid_fence" | "waiter_not_registered";
  proofDigest: ArtifactDigest;
};

export type DriverProbeSpec = {
  protocolVersion: ProtocolVersion;
  runtime: "codex" | "claude" | "scripted_fake";
  executableRefPrivate: string;
  executableDigest: ArtifactDigest;
  wireProtocolDigest: ArtifactDigest;
};

export type DriverLaunchSpec = {
  launch: LocalLaunchFence;
  driverIdentity: DriverIdentity;
  transportDigest: ArtifactDigest;
  launchEnvironmentRefPrivate: string;
};

export type DriverResumeSpec = {
  launch: SpawnedLaunchFence;
  expectedSessionId: SessionId;
  runtimeSessionRefPrivate: string;
  cursorOwnerToken: ArtifactDigest;
};

export type SpawnHandle = {
  launchId: LaunchId;
  stateInstanceId: StateInstanceId;
  processHandleRefPrivate: string;
  processHandleDigest: ArtifactDigest;
  transportDigest: ArtifactDigest;
};

export type DriverStatus =
  | { kind: "spawning"; launch: SpawnedLaunchFence }
  | { kind: "ready"; launch: ReadyLaunchFence }
  | { kind: "running"; launch: ReadyLaunchFence; activeTurnId: TurnId }
  | { kind: "terminal"; reason: TerminalReason };

export type DriverEventPumpLease = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ownerToken: ArtifactDigest;
  readerEpoch: number;
};

export type DriverEventWaiterSpec = {
  kind: "initialize" | "resume" | "turn";
  stateInstanceId: StateInstanceId;
  sessionId?: SessionId;
  turnId?: TurnId;
  bindingDigest: ArtifactDigest;
};

export type DriverEventWaiter = DriverEventWaiterSpec & {
  waiterId: CommandId;
  registeredBeforeWrite: true;
  registeredAtOrdinal: number;
};

export type DriverEventRecord = {
  stateInstanceId: StateInstanceId;
  sessionId: SessionId;
  ordinal: number;
  eventDigest: ArtifactDigest;
  bindingDigest?: ArtifactDigest;
  event: NormalizedDriverEvent;
};

export interface DriverEventPump {
  readonly stateInstanceId: StateInstanceId;
  claimCursor(input: {
    sessionId: SessionId;
    ownerToken: ArtifactDigest;
    mode: "start" | "resume";
  }): Promise<DriverEventPumpLease>;
  registerWaiter(
    lease: DriverEventPumpLease,
    spec: DriverEventWaiterSpec,
  ): Promise<DriverEventWaiter>;
  subscribe(lease: DriverEventPumpLease): AsyncIterable<DriverEventRecord>;
  release(lease: DriverEventPumpLease): Promise<void>;
}

export interface NativeProcessDriver {
  probe(spec: DriverProbeSpec): Promise<DriverIdentity>;
  start(spec: DriverLaunchSpec): Promise<SpawnHandle>;
  resume(spec: DriverResumeSpec): Promise<SpawnHandle>;
  startTurn(
    process: SpawnHandle,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): Promise<NativeProcessWriteOutcome>;
  steerTurn(
    process: SpawnHandle,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): Promise<NativeProcessWriteOutcome>;
  interrupt(
    process: SpawnHandle,
    session: DriverSession,
    expectedTurnId: TurnId,
  ): Promise<void>;
  status(process: SpawnHandle, session?: DriverSession): Promise<DriverStatus>;
  events(process: SpawnHandle): AsyncIterable<NormalizedDriverEvent>;
  stop(process: SpawnHandle, reason: StopReason): Promise<void>;
}

export interface NativeRuntimePort {
  readonly driverKind: "native_process" | "scripted_fake";
  writeTurn(turn: CompiledNativeTurn, binding: NativeWriteBinding): Promise<NativeWriteOutcome>;
}
