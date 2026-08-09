import type {
  ArtifactDigest,
  DriverIdentity,
  NormalizedDriverEvent,
  StopReason,
} from "@swarm/protocol";
import type {
  DriverEventPump,
  DriverRegisteredEventWaiter,
  DriverResumeSpec,
  DriverStartWriteWitness,
  DriverStatus,
  DriverLaunchSpec,
  SpawnHandle,
} from "../port.js";

export type CodexRequestMethod =
  | "initialize"
  | "thread/start"
  | "thread/resume"
  | "turn/start"
  | "turn/steer"
  | "turn/interrupt";

export type CodexNotificationMethod = "initialized";

export type CodexJsonRpcRequest = Readonly<{
  jsonrpc: "2.0";
  id: string;
  method: CodexRequestMethod;
  params: Readonly<Record<string, unknown>>;
}>;

export type CodexJsonRpcNotification = Readonly<{
  jsonrpc: "2.0";
  method: CodexNotificationMethod;
  params: Readonly<Record<string, never>>;
}>;

export type CodexWritePredecessor = DriverStartWriteWitness | DriverRegisteredEventWaiter;

/**
 * The transport owns JSON-RPC correlation through the sole adapter pump. A
 * resolved request means its response was consumed by that pump and normalized
 * through the callback installed at spawn/resume time.
 */
export interface CodexTransport {
  request(
    request: CodexJsonRpcRequest,
    predecessor: CodexWritePredecessor,
    onWritten?: () => void,
  ): Promise<void>;
  notify(notification: CodexJsonRpcNotification, predecessor: CodexWritePredecessor): Promise<void>;
}

export type CodexRuntimeProbe = {
  versionOutput: string;
  executableDigest: ArtifactDigest;
  wireProtocolDigest: ArtifactDigest;
};

export type CodexWireConsumer = (message: unknown) => readonly NormalizedDriverEvent[];

export type CodexSpawnedRuntime = {
  process: SpawnHandle;
  pump: DriverEventPump;
  transport: CodexTransport;
  cursorOwnerToken: ArtifactDigest;
  initializeWaiterId: import("@swarm/protocol").CommandId;
  initializeBindingDigest: ArtifactDigest;
};

export type CodexResumedRuntime = CodexSpawnedRuntime & {
  resumeWaiterId: import("@swarm/protocol").CommandId;
  resumeBindingDigest: ArtifactDigest;
};

export interface CodexRuntimeHost {
  probe(spec: import("../port.js").DriverProbeSpec): Promise<CodexRuntimeProbe>;
  spawn(
    spec: DriverLaunchSpec,
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: CodexWireConsumer },
  ): Promise<CodexSpawnedRuntime>;
  resume(
    spec: DriverResumeSpec,
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: CodexWireConsumer },
  ): Promise<CodexResumedRuntime>;
  status(process: SpawnHandle, identity: DriverIdentity): Promise<DriverStatus>;
  stop(process: SpawnHandle, reason: StopReason): Promise<void>;
}
