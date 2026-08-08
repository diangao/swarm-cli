import { createHash, randomBytes } from "node:crypto";

import type { CommandId } from "@swarm/protocol";
import type { NativeCommandIdSource } from "@swarm/daemon-core";

const ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz";

export function deterministicCommandId(seed: string): CommandId {
  const bytes = createHash("sha256").update(seed).digest();
  let value = "";
  for (let index = 0; index < 26; index += 1) {
    value += ALPHABET[(bytes[index % bytes.length] ?? 0) & 31];
  }
  return `cmd_${value}` as CommandId;
}

export class RandomCommandIdSource implements NativeCommandIdSource {
  nextCommandId(): CommandId {
    const bytes = randomBytes(26);
    let value = "";
    for (const byte of bytes) value += ALPHABET[byte & 31];
    return `cmd_${value}` as CommandId;
  }
}
