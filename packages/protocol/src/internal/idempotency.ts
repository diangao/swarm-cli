import { createHash } from "node:crypto";

import { fail } from "../errors.js";
import { canonicalProtocolJson } from "../json.js";
import type { LaunchCommand } from "../types.js";

export type CommandIdentityResult = "new" | "replay";

export function recordCommandIdentity(
  seen: Map<string, string>,
  command: LaunchCommand,
): CommandIdentityResult {
  const digest = `sha256:${createHash("sha256").update(canonicalProtocolJson(command)).digest("hex")}`;
  const prior = seen.get(command.commandId);
  if (prior === undefined) {
    seen.set(command.commandId, digest);
    return "new";
  }
  if (prior !== digest) return fail("IDEMPOTENCY_CONFLICT");
  return "replay";
}
