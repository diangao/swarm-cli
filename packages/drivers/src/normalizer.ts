import {
  messageBodyHasContent,
  type CommandId,
  type DriverIdentity,
  type MessageId,
  type NativeRuntimeEvent,
  type NormalizedDriverEvent,
  type SimpleTaskCommand,
  type TurnId,
} from "@swarm/protocol";
import type { DriverEventWaiter } from "./port.js";

export type NativeEventErrorCode =
  | "ASSISTANT_REPLY_REQUIRED"
  | "MULTIPLE_ASSISTANT_REPLIES"
  | "COORDINATION_BEFORE_REPLY"
  | "SECOND_COORDINATION_CALL"
  | "SOURCE_MESSAGE_MISMATCH"
  | "TURN_COMPLETION_REQUIRED"
  | "EVENT_AFTER_COMPLETION"
  | "EMPTY_ASSISTANT_REPLY"
  | "UNSUPPORTED_RUNTIME_EVENT";

export class NativeEventError extends Error {
  readonly code: NativeEventErrorCode;

  constructor(code: NativeEventErrorCode) {
    super(code);
    this.name = "NativeEventError";
    this.code = code;
  }
}

export type NormalizedNativeAction =
  | { kind: "reply"; text: string }
  | { kind: "coordination"; commandId: CommandId; command: SimpleTaskCommand }
  | { kind: "complete" };

export class NativeEventNormalizer {
  readonly #sourceMessageId: MessageId;
  #replySeen = false;
  #coordinationSeen = false;
  #completed = false;

  constructor(sourceMessageId: MessageId) {
    this.#sourceMessageId = sourceMessageId;
  }

  accept(event: NativeRuntimeEvent): NormalizedNativeAction {
    if (this.#completed) throw new NativeEventError("EVENT_AFTER_COMPLETION");
    if (event.kind === "assistant_reply") {
      if (!messageBodyHasContent(event.text)) throw new NativeEventError("EMPTY_ASSISTANT_REPLY");
      if (this.#replySeen) throw new NativeEventError("MULTIPLE_ASSISTANT_REPLIES");
      this.#replySeen = true;
      return { kind: "reply", text: event.text };
    }
    if (event.kind === "coordination_call") {
      if (!this.#replySeen) throw new NativeEventError("COORDINATION_BEFORE_REPLY");
      if (this.#coordinationSeen) throw new NativeEventError("SECOND_COORDINATION_CALL");
      if (event.command.sourceMessageId !== this.#sourceMessageId) {
        throw new NativeEventError("SOURCE_MESSAGE_MISMATCH");
      }
      this.#coordinationSeen = true;
      return { kind: "coordination", commandId: event.commandId, command: event.command };
    }
    if (event.kind === "turn_complete") {
      this.#completed = true;
      if (!this.#replySeen) throw new NativeEventError("ASSISTANT_REPLY_REQUIRED");
      return { kind: "complete" };
    }
    throw new NativeEventError("UNSUPPORTED_RUNTIME_EVENT");
  }

  finish(): void {
    if (!this.#completed) throw new NativeEventError("TURN_COMPLETION_REQUIRED");
  }
}

export type DriverNormalizationErrorCode =
  | "DRIVER_PROTOCOL_UNSUPPORTED"
  | "DRIVER_CAPABILITY_MISMATCH"
  | "DRIVER_WAITER_PREDECESSOR_REQUIRED"
  | "DRIVER_EVENT_ORDER_INVALID"
  | "DRIVER_EVENT_FENCE_MISMATCH";

export class DriverNormalizationError extends Error {
  readonly code: DriverNormalizationErrorCode;

  constructor(code: DriverNormalizationErrorCode) {
    super(code);
    this.name = "DriverNormalizationError";
    this.code = code;
  }
}

export function assertDriverCapability(
  identity: DriverIdentity,
  capability: keyof DriverIdentity["capability"],
): void {
  if (identity.capability[capability] !== true) {
    throw new DriverNormalizationError("DRIVER_CAPABILITY_MISMATCH");
  }
}

export function assertWaiterBeforeWrite(
  waiter: DriverEventWaiter | undefined,
): asserts waiter is DriverEventWaiter {
  if (waiter?.registeredBeforeWrite !== true) {
    throw new DriverNormalizationError("DRIVER_WAITER_PREDECESSOR_REQUIRED");
  }
}

export function requireObservedDriverEvent(
  value: NormalizedDriverEvent | "still_queued" | "cancelled" | null | undefined,
): NormalizedDriverEvent {
  if (value === "still_queued" || value === "cancelled") {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }
  if (value === null || value === undefined) {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
  return value;
}

type ActiveDriverTurn = {
  turnId: TurnId;
  phase: "started" | "input_written" | "model_visible";
  replySeen: boolean;
  coordinationSeen: boolean;
};

export class DriverEventStreamNormalizer {
  #ready = false;
  #terminal = false;
  #active: ActiveDriverTurn | null = null;

  accept(event: NormalizedDriverEvent): NormalizedDriverEvent {
    if (this.#terminal) this.#order();
    if (event.kind === "runtime_terminal") {
      this.#terminal = true;
      this.#active = null;
      return event;
    }
    if (event.kind === "runtime_ready") {
      if (this.#ready || this.#active !== null) this.#order();
      this.#ready = true;
      return event;
    }
    if (!this.#ready) this.#order();
    if (event.kind === "turn_started") {
      if (this.#active !== null) {
        if (this.#active.turnId !== event.turnId) this.#fence();
        this.#order();
      }
      this.#active = {
        turnId: event.turnId,
        phase: "started",
        replySeen: false,
        coordinationSeen: false,
      };
      return event;
    }
    const active = this.#activeFor(event.turnId);
    if (event.kind === "input_written") {
      if (active.phase !== "started") this.#order();
      active.phase = "input_written";
      return event;
    }
    if (event.kind === "model_visible") {
      if (active.phase !== "input_written") this.#order();
      active.phase = "model_visible";
      return event;
    }
    if (event.kind === "turn_boundary") {
      if (active.phase !== "model_visible") this.#order();
      return event;
    }
    if (event.kind === "assistant_reply") {
      if (active.phase !== "model_visible" || active.replySeen) this.#order();
      if (!messageBodyHasContent(event.text)) this.#protocol();
      active.replySeen = true;
      return event;
    }
    if (event.kind === "coordination_call") {
      if (!active.replySeen || active.coordinationSeen) this.#order();
      active.coordinationSeen = true;
      return event;
    }
    if (event.kind === "turn_completed") {
      if (active.phase !== "model_visible" || !active.replySeen) this.#order();
      this.#active = null;
      return event;
    }
    return this.#protocol();
  }

  finish(): void {
    if (!this.#terminal && this.#active !== null) this.#order();
  }

  #activeFor(turnId: TurnId): ActiveDriverTurn {
    if (this.#active === null) return this.#order();
    if (this.#active.turnId !== turnId) return this.#fence();
    return this.#active;
  }

  #order(): never {
    throw new DriverNormalizationError("DRIVER_EVENT_ORDER_INVALID");
  }

  #fence(): never {
    throw new DriverNormalizationError("DRIVER_EVENT_FENCE_MISMATCH");
  }

  #protocol(): never {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
}
