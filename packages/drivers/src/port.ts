import type {
  ArtifactDigest,
  CommandId,
  NativeRuntimeEvent,
  ScriptedNotWrittenProof,
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

export interface NativeRuntimePort {
  readonly driverKind: "native_process" | "scripted_fake";
  writeTurn(turn: CompiledNativeTurn, binding: NativeWriteBinding): Promise<NativeWriteOutcome>;
}
