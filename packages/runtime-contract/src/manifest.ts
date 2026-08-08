import {
  canonicalProtocolJson,
  parseStandingManifest,
  type FrozenStandingManifest,
  type ProtocolVersion,
  type StandingManifest,
} from "@swarm/protocol";

import { protocolDigest } from "./digest.js";

function deepFreeze<Value>(value: Value): Readonly<Value> {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const key of Reflect.ownKeys(value)) {
      deepFreeze((value as Record<PropertyKey, unknown>)[key]);
    }
    Object.freeze(value);
  }
  return value;
}

export interface ManifestComposer {
  compose(manifest: StandingManifest): Readonly<FrozenStandingManifest>;
}

export class CanonicalManifestComposer implements ManifestComposer {
  compose(candidate: StandingManifest): Readonly<FrozenStandingManifest> {
    const version = candidate.protocolVersion as ProtocolVersion;
    const manifest = parseStandingManifest(canonicalProtocolJson(candidate), version);
    const frozen: FrozenStandingManifest = {
      manifest,
      manifestDigest: protocolDigest(manifest),
    };
    return deepFreeze(frozen);
  }
}

export function freezeStandingManifest(
  manifest: StandingManifest,
): Readonly<FrozenStandingManifest> {
  return new CanonicalManifestComposer().compose(manifest);
}
