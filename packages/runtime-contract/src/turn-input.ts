import {
  canonicalProtocolJson,
  parseNativeTurnInput,
  type ArtifactDigest,
  type AttentionNotice,
  type FrozenStandingManifest,
  type NativeDeliveryEnvelope,
  type NativeTurnInput,
} from "@swarm/protocol";

import { protocolDigest } from "./digest.js";

export type CompiledNativeTurn = {
  readonly input: Readonly<NativeTurnInput>;
  readonly bytes: Uint8Array;
  readonly inputDigest: ArtifactDigest;
};

export type NativeTurnCompileRequest = {
  frozenManifest: Readonly<FrozenStandingManifest>;
  delivery: NativeDeliveryEnvelope;
  body: string;
  attention: readonly AttentionNotice[];
};

function deepFreeze<Value>(value: Value): Readonly<Value> {
  if (ArrayBuffer.isView(value)) return value;
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const key of Reflect.ownKeys(value)) {
      deepFreeze((value as Record<PropertyKey, unknown>)[key]);
    }
    Object.freeze(value);
  }
  return value;
}

export function compileNativeTurnInput(request: NativeTurnCompileRequest): CompiledNativeTurn {
  const candidate: NativeTurnInput = {
    protocolVersion: request.delivery.protocolVersion,
    manifestDigest: request.frozenManifest.manifestDigest,
    current: {
      delivery: request.delivery,
      body: request.body,
    },
    attention: [...request.attention],
  };
  const bytes = canonicalProtocolJson(candidate);
  const parsed = parseNativeTurnInput(bytes, request.delivery.protocolVersion);
  const stableBytes = canonicalProtocolJson(parsed);
  return deepFreeze({
    input: deepFreeze(parsed),
    get bytes(): Uint8Array {
      return new Uint8Array(stableBytes);
    },
    inputDigest: protocolDigest(parsed),
  });
}

export function serializeNativeTurnInput(turn: CompiledNativeTurn): Uint8Array {
  return new Uint8Array(turn.bytes);
}
