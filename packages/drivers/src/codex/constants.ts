import type { ArtifactDigest, DriverCapability } from "@swarm/protocol";

export const CODEX_RUNTIME_VERSION = "0.145.0";
export const CODEX_VERSION_OUTPUT = `codex-cli ${CODEX_RUNTIME_VERSION}`;
export const CODEX_WIRE_PROTOCOL_DIGEST =
  "sha256:33e163c58a7e9c276f18e109d7ac361f01f8c2394881fc8e3f3177efeaed7cf3" as ArtifactDigest;
export const CODEX_WIRE_PROTOCOL_BYTES = 327_018;

export const CODEX_APP_SERVER_ARGV = Object.freeze(["app-server"] as const);
export const CODEX_SCHEMA_GENERATOR_ARGV = Object.freeze([
  "app-server",
  "generate-json-schema",
  "--experimental",
  "--out",
] as const);

export const CODEX_CAPABILITY: Readonly<DriverCapability> = Object.freeze({
  start: true,
  resume: true,
  steer: true,
  interrupt: true,
  reviewBoundary: true,
  compactionBoundary: true,
});

export const CODEX_METHOD = Object.freeze({
  initialize: "initialize",
  initialized: "initialized",
  threadStart: "thread/start",
  threadResume: "thread/resume",
  turnStart: "turn/start",
  turnSteer: "turn/steer",
  turnInterrupt: "turn/interrupt",
  turnStarted: "turn/started",
  turnCompleted: "turn/completed",
  itemCompleted: "item/completed",
  threadStatusChanged: "thread/status/changed",
} as const);

export const CODEX_CLIENT_INFO = Object.freeze({
  name: "swarm-daemon",
  title: "Swarm Daemon",
  version: "0.1.0",
});
