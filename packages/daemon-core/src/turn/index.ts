// Standalone turn/steer state machine module. It is NOT wired into daemon.ts and
// NOT re-exported from src/index.ts (that export is intentionally omitted as scope
// drift). Consumers import it directly from "@swarm/daemon-core/src/turn" or via
// tests.

export { TurnError, turnFail, type TurnErrorCode } from "./errors.js";
export {
  TurnMachine,
  type ApplyEventOutcome,
  type FreshProcessRecoveryOutcome,
  type FreshProcessRecoveryPorts,
  type InterruptOutcome,
  type MachineConfig,
  type SubmitOutcome,
  type TurnCoordinationRequest,
  type TurnTerminalBindingPort,
  type TurnTerminalDraftBasis,
  type TurnTerminalResult,
  type TurnTerminalStage,
} from "./machine.js";
export type {
  AtomicTurnJournalPort,
  BeginTurnContributionInput,
  CommitTurnStepInput,
  CommitTurnTerminalInput,
  DriverEventReaderClaim,
  DriverEventReaderPort,
  DurableTurnState,
  LocalTurnState,
  ReadDurableTurnStateInput,
  // Fresh-process recovery ports (durable turn-recovery + retained-replay abstractions).
  RecoveryCompositeClaimCloseResult,
  RecoveryCompositeCursorClaim,
  RecoveryCursorAuthority,
  RecoveryLiveResumeAuthorization,
  RecoveryLiveResumeTicket,
  ReplayableTurnRecoveryBasis,
  RetainedEventLease,
  RetainedReplayExpectation,
  RetainedReplayWatermark,
  RetainedTurnEvent,
  RetainedTurnEventRecord,
  RetainedTurnEventSource,
  RetainedTurnReplay,
  SettleTurnContributionInput,
  TurnAdmissionMode,
  TurnCommandIdSource,
  TurnDriverPort,
  TurnEventInput,
  TurnEventRecordInput,
  TurnLiveResumePort,
  TurnMutationResult,
  TurnReaderFence,
  TurnRecoveryClaimPort,
  TurnRecoveryCursorSnapshot,
  TurnRecoveryReadPort,
  TurnRecoveryReadResult,
  TurnSettleKind,
  TurnStepKind,
  TurnStepResult,
  TurnSubmission,
  TurnTerminalCommitBasis,
} from "./ports.js";
// The §7.9 TurnCoordinationDisposition is re-exported from the protocol SSOT for
// handoff consumers; this lane keeps no local alias.
export type { TurnCoordinationDisposition } from "@swarm/protocol";
