import {
  messageBodyHasContent,
  type CommandId,
  type MessageId,
  type NativeRuntimeEvent,
  type SimpleTaskCommand,
} from "@swarm/protocol";

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
