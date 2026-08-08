import { createHash } from "node:crypto";

import { canonicalProtocolJson, type ArtifactDigest } from "@swarm/protocol";

export function protocolDigest(value: unknown): ArtifactDigest {
  const digest = createHash("sha256").update(canonicalProtocolJson(value)).digest("hex");
  return `sha256:${digest}` as ArtifactDigest;
}
