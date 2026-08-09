import type { DriverCapability } from "@swarm/protocol";

export const CLAUDE_RUNTIME_VERSION = "2.1.226";
export const CLAUDE_VERSION_OUTPUT = `${CLAUDE_RUNTIME_VERSION} (Claude Code)`;
export const CLAUDE_INPUT_UUID_NAMESPACE = "f61b0c53-2d3d-5ed4-9bbf-d8d9fab5d3bd";

export const CLAUDE_STREAM_ARGV = Object.freeze([
  "--print",
  "--input-format",
  "stream-json",
  "--output-format",
  "stream-json",
  "--verbose",
  "--replay-user-messages",
] as const);

export const CLAUDE_CAPABILITY: Readonly<DriverCapability> = Object.freeze({
  start: true,
  resume: true,
  steer: false,
  interrupt: true,
  reviewBoundary: false,
  compactionBoundary: false,
});
