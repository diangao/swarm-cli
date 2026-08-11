import type {
  ArtifactDigest,
  CommandId,
  DriverIdentity,
  MessageId,
  NormalizedDriverEvent,
  StopReason,
} from "@swarm/protocol";
import type {
  DriverEventPump,
  DriverLaunchSpec,
  DriverProbeSpec,
  DriverRegisteredEventWaiter,
  DriverResumeSpec,
  DriverStartWriteWitness,
  DriverStatus,
  SpawnHandle,
} from "../port.js";

export type ClaudeTextContent = Readonly<{ type: "text"; text: string }>;

export type ClaudeUserInput = Readonly<{
  type: "user";
  message: Readonly<{ role: "user"; content: readonly ClaudeTextContent[] }>;
  parent_tool_use_id: null;
  session_id: string;
  uuid: string;
}>;

export type ClaudeInterruptRequest = Readonly<{
  type: "control_request";
  request_id: CommandId;
  request: Readonly<{ subtype: "interrupt" }>;
}>;

export type ClaudeWritePredecessor = DriverStartWriteWitness | DriverRegisteredEventWaiter;
export type ClaudeTurnObservationCorrelation = Readonly<{
  sourceMessageId: MessageId;
  predecessor: DriverRegisteredEventWaiter;
}>;

export interface ClaudeTransport {
  begin(predecessor: DriverStartWriteWitness | DriverRegisteredEventWaiter): Promise<void>;
  writeLine(
    line: Uint8Array,
    predecessor: DriverRegisteredEventWaiter,
    onWritten: () => readonly NormalizedDriverEvent[],
    correlation: ClaudeTurnObservationCorrelation,
  ): Promise<void>;
  writeControl(
    line: Uint8Array,
    predecessor: DriverRegisteredEventWaiter,
  ): Promise<unknown>;
}

export type ClaudeRuntimeProbe = {
  versionOutput: string;
  executableDigest: ArtifactDigest;
  wireProtocolDigest: ArtifactDigest;
};

export type ClaudeWireConsumer = (message: unknown) => readonly NormalizedDriverEvent[];

export type ClaudeSpawnedRuntime = {
  process: SpawnHandle;
  pump: DriverEventPump;
  transport: ClaudeTransport;
  cursorOwnerToken: ArtifactDigest;
  initializeWaiterId: CommandId;
  initializeBindingDigest: ArtifactDigest;
};

export type ClaudeResumedRuntime = ClaudeSpawnedRuntime & {
  resumeWaiterId: CommandId;
  resumeBindingDigest: ArtifactDigest;
};

export interface ClaudeRuntimeHost {
  probe(spec: DriverProbeSpec): Promise<ClaudeRuntimeProbe>;
  spawn(
    spec: DriverLaunchSpec,
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: ClaudeWireConsumer },
  ): Promise<ClaudeSpawnedRuntime>;
  resume(
    spec: DriverResumeSpec,
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: ClaudeWireConsumer },
  ): Promise<ClaudeResumedRuntime>;
  status(process: SpawnHandle, identity: DriverIdentity): Promise<DriverStatus>;
  stop(process: SpawnHandle, reason: StopReason): Promise<void>;
}
