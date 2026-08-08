import type {
  ArtifactDigest,
  CommandId,
  NativeRuntimeEvent,
} from "@swarm/protocol";

import type {
  NativeRuntimePort,
  NativeWriteBinding,
  NativeWriteOutcome,
} from "./port.js";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import { protocolDigest } from "@swarm/runtime-contract";

export type ScriptedTurn =
  | {
      kind: "written";
      runtimeWriteId: CommandId;
      visibilityEventId: CommandId;
      events: readonly NativeRuntimeEvent[];
    }
  | {
      kind: "not_written";
      fixtureId: CommandId;
      scriptDigest: ArtifactDigest;
      outcomeOrdinal: number;
    }
  | { kind: "ambiguous" };

async function* eventStream(events: readonly NativeRuntimeEvent[]): AsyncIterable<NativeRuntimeEvent> {
  for (const event of events) yield event;
}

export class ScriptedDriver implements NativeRuntimePort {
  readonly driverKind = "scripted_fake" as const;
  readonly writes: Array<{ turn: CompiledNativeTurn; binding: NativeWriteBinding }> = [];
  readonly #script: ScriptedTurn[];

  constructor(script: readonly ScriptedTurn[]) {
    this.#script = [...script];
  }

  async writeTurn(turn: CompiledNativeTurn, binding: NativeWriteBinding): Promise<NativeWriteOutcome> {
    this.writes.push({ turn, binding });
    const next = this.#script.shift();
    if (next === undefined) return { kind: "ambiguous" };
    if (next.kind === "ambiguous") return next;
    if (next.kind === "not_written") {
      const proof = {
        driverKind: "scripted_fake" as const,
        fixtureId: next.fixtureId,
        scriptDigest: next.scriptDigest,
        invocationId: binding.invocationId,
        invocationGeneration: binding.invocationGeneration,
        writeStartedEntryId: binding.writeStartedEntryId,
        writeStartedEntryDigest: binding.writeStartedEntryDigest,
        outcomeOrdinal: next.outcomeOrdinal,
        outcome: "not_written" as const,
      };
      return {
        kind: "not_written",
        proof: { ...proof, proofDigest: protocolDigest(proof) },
      };
    }
    return {
      kind: "written",
      runtimeWriteId: next.runtimeWriteId,
      visibilityEventId: next.visibilityEventId,
      events: eventStream(next.events),
    };
  }
}
