export { DaemonCore } from "./daemon.js";
export {
  DaemonCoreError,
  PrePermitDisconnectError,
  type DaemonCoreErrorCode,
} from "./errors.js";
export type {
  JournalRecovery,
  NativeCommandIdSource,
  NativeJournalPort,
  NativeServerPort,
  NativeTurnRequest,
  NativeTurnResult,
  ReplyCommitCommand,
  ReplyCommitResult,
  TaskCommitCommand,
  TaskCommitResult,
} from "./ports.js";
// Public delivery-kernel surface (the app layer wires these ports). Explicit
// re-exports only — the DeliveryKernel value plus the five consumer ports; no
// unrelated delivery internals are surfaced at the package root.
export { DeliveryKernel } from "./delivery/index.js";
export type {
  DeliveryClock,
  DeliveryCommandIdDerivationPort,
  DeliveryExecutionPort,
  DeliveryJournalPort,
  DeliveryServerCommitPort,
} from "./delivery/index.js";
