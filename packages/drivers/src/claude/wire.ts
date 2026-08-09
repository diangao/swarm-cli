import {
  canonicalProtocolJson,
  type ArtifactDigest,
  type CommandId,
  type DriverSession,
  type DriverTurnBinding,
  type NormalizedDriverEvent,
  type SessionId,
  type TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import { protocolDigest } from "@swarm/runtime-contract";

import { DriverNormalizationError } from "../normalizer.js";
import type { ClaudeInterruptRequest, ClaudeUserInput } from "./types.js";
import { claudeInputUuid } from "./uuid.js";

const decoder = new TextDecoder("utf-8", { fatal: true });

export type ClaudeAdapterErrorCode = "TURN_INPUT_ALREADY_IN_FLIGHT" | "ACTIVE_TURN_CONFLICT";

export class ClaudeAdapterError extends Error {
  readonly code: ClaudeAdapterErrorCode;

  constructor(code: ClaudeAdapterErrorCode) {
    super(code);
    this.name = "ClaudeAdapterError";
    this.code = code;
  }
}

type ActiveClaudeTurn = {
  readonly protocolTurnId: TurnId;
  readonly binding: DriverTurnBinding;
  readonly input: ClaudeUserInput;
  readonly inputDigest: ArtifactDigest;
  inputWritten: boolean;
  modelVisible: boolean;
  replySeen: boolean;
};

export class ClaudeWireState {
  readonly #sessionId: SessionId;
  readonly #runtimeSessionRef: string;
  readonly #seen = new Map<string, ArtifactDigest>();
  #initialized = false;
  #interruptReceiptCapability = false;
  #active: ActiveClaudeTurn | undefined;
  #completed: { uuid: string; digest: ArtifactDigest } | undefined;
  #terminal = false;

  constructor(sessionId: SessionId, runtimeSessionRef: string) {
    if (!isUuid(runtimeSessionRef)) this.#protocol();
    this.#sessionId = sessionId;
    this.#runtimeSessionRef = runtimeSessionRef;
  }

  beginTurn(
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): { readonly input: ClaudeUserInput; readonly line: Uint8Array } {
    this.#assertSession(session);
    if (!this.#initialized) this.#order();
    if (this.#active !== undefined) throw new ClaudeAdapterError("TURN_INPUT_ALREADY_IN_FLIGHT");
    if (
      binding.mode.kind !== "ordinary"
      || binding.inputOrdinal !== 0
      || binding.inputDigest !== input.inputDigest
    ) this.#fence();
    const uuid = claudeInputUuid(this.#sessionId, binding);
    const wireInput: ClaudeUserInput = Object.freeze({
      type: "user",
      message: Object.freeze({
        role: "user",
        content: Object.freeze([Object.freeze({ type: "text" as const, text: decoder.decode(input.bytes) })]),
      }),
      parent_tool_use_id: null,
      session_id: this.#runtimeSessionRef,
      uuid,
    });
    this.#active = {
      protocolTurnId: binding.protocolTurnId,
      binding,
      input: wireInput,
      inputDigest: protocolDigest(wireInput),
      inputWritten: false,
      modelVisible: false,
      replySeen: false,
    };
    return { input: wireInput, line: ndjson(wireInput) };
  }

  markInputWritten(uuid: string): readonly NormalizedDriverEvent[] {
    const active = this.#activeForUuid(uuid);
    if (active.inputWritten) this.#order();
    active.inputWritten = true;
    return [
      {
        kind: "turn_started",
        turnId: active.protocolTurnId,
        driverTurnRefDigest: protocolDigest({ sessionId: this.#sessionId, uuid }),
      },
      {
        kind: "input_written",
        turnId: active.protocolTurnId,
        runtimeWriteId: active.binding.runtimeWriteId,
      },
    ];
  }

  interruptRequest(expectedTurnId: TurnId): {
    readonly request: ClaudeInterruptRequest;
    readonly line: Uint8Array;
  } {
    const active = this.#active;
    if (active === undefined || active.protocolTurnId !== expectedTurnId) {
      throw new ClaudeAdapterError("ACTIVE_TURN_CONFLICT");
    }
    if (!this.#interruptReceiptCapability) this.#protocol();
    const request: ClaudeInterruptRequest = Object.freeze({
      type: "control_request",
      request_id: active.binding.permitId,
      request: Object.freeze({ subtype: "interrupt" as const }),
    });
    return { request, line: ndjson(request) };
  }

  acceptInterruptReceipt(raw: unknown, expectedRequestId: CommandId): void {
    if (!this.#interruptReceiptCapability) this.#protocol();
    const receipt = object(raw);
    if (receipt["type"] !== "control_response" || receipt["request_id"] !== expectedRequestId) {
      this.#fence();
    }
    const response = object(receipt["response"]);
    if (response["subtype"] !== "success") this.#fence();
    const stillQueued = response["still_queued"];
    const cancelled = response["cancelled"];
    if (!Array.isArray(stillQueued) || !Array.isArray(cancelled)) this.#protocol();
    if (stillQueued.length !== 0 || cancelled.length !== 0) this.#fence();
  }

  accept(raw: unknown): readonly NormalizedDriverEvent[] {
    if (this.#terminal) this.#order();
    const event = object(raw);
    const type = string(event["type"]);
    if (type === "system") return this.#system(event);
    if (type === "user") return this.#replay(event);
    if (type === "assistant") return this.#assistant(event);
    if (type === "result") return this.#result(event);
    return this.#protocol();
  }

  #system(event: Record<string, unknown>): readonly NormalizedDriverEvent[] {
    const subtype = string(event["subtype"]);
    if (subtype === "init") {
      this.#session(event);
      const capabilities = object(event["capabilities"]);
      const control = object(capabilities["control_requests"]);
      this.#interruptReceiptCapability = control["interrupt"] === true
        && control["queue_receipt"] === true;
      const digest = this.#aliasOrRemember(`system:init:${this.#runtimeSessionRef}`, event);
      if (digest === "alias") return [];
      if (this.#initialized) this.#fence();
      this.#initialized = true;
      return [{
        kind: "runtime_ready",
        runtimeSessionRef: this.#runtimeSessionRef,
        runtimeSessionRefDigest: protocolDigest({
          sessionId: this.#sessionId,
          runtimeSessionRef: this.#runtimeSessionRef,
        }),
      }];
    }
    if (subtype === "compact_boundary") {
      this.#session(event);
      const active = this.#active;
      if (active === undefined || !active.modelVisible) this.#order();
      return [{
        kind: "turn_boundary",
        turnId: active.protocolTurnId,
        boundary: "compaction",
        steerable: false,
      }];
    }
    return this.#protocol();
  }

  #replay(event: Record<string, unknown>): readonly NormalizedDriverEvent[] {
    this.#session(event);
    const active = this.#active;
    if (active === undefined) this.#fence();
    if (event["isReplay"] !== true) this.#protocol();
    if (
      event["uuid"] !== active.input.uuid
      || event["parent_tool_use_id"] !== null
      || protocolDigest({
        type: event["type"],
        message: event["message"],
        parent_tool_use_id: event["parent_tool_use_id"],
        session_id: event["session_id"],
        uuid: event["uuid"],
      }) !== active.inputDigest
    ) this.#fence();
    if (!active.inputWritten) this.#order();
    const digest = this.#aliasOrRemember(`user:replay:${active.input.uuid}`, event);
    if (digest === "alias") return [];
    active.modelVisible = true;
    return [{
      kind: "model_visible",
      turnId: active.protocolTurnId,
      visibilityEventId: active.binding.visibilityEventId,
    }];
  }

  #assistant(event: Record<string, unknown>): readonly NormalizedDriverEvent[] {
    this.#session(event);
    const active = this.#active;
    if (active === undefined || !active.modelVisible) this.#order();
    const message = object(event["message"]);
    const content = array(message["content"]);
    const textParts = content.map((part) => {
      const item = object(part);
      if (item["type"] !== "text") this.#protocol();
      return string(item["text"]);
    });
    const text = textParts.join("");
    const digest = this.#aliasOrRemember(`assistant:${active.input.uuid}`, event);
    if (digest === "alias") return [];
    if (active.replySeen) this.#order();
    active.replySeen = true;
    return [{ kind: "assistant_reply", turnId: active.protocolTurnId, text }];
  }

  #result(event: Record<string, unknown>): readonly NormalizedDriverEvent[] {
    this.#session(event);
    if (event["subtype"] !== "success") this.#protocol();
    const active = this.#active;
    if (active === undefined) {
      if (this.#completed?.digest === protocolDigest(event)) return [];
      this.#fence();
    }
    if (!active.modelVisible || !active.replySeen) this.#order();
    const digest = protocolDigest(event);
    this.#completed = { uuid: active.input.uuid, digest };
    this.#active = undefined;
    return [{ kind: "turn_completed", turnId: active.protocolTurnId }];
  }

  #activeForUuid(uuid: string): ActiveClaudeTurn {
    const active = this.#active;
    if (active === undefined || active.input.uuid !== uuid) this.#fence();
    return active;
  }

  #assertSession(session: DriverSession): void {
    if (
      session.driverIdentity.runtime !== "claude"
      || session.launch.sessionId !== this.#sessionId
      || session.runtimeSessionRef !== this.#runtimeSessionRef
    ) this.#fence();
  }

  #session(event: Record<string, unknown>): void {
    if (event["session_id"] !== this.#runtimeSessionRef) this.#fence();
  }

  #aliasOrRemember(key: string, value: unknown): "new" | "alias" {
    const digest = protocolDigest(value);
    const prior = this.#seen.get(key);
    if (prior === digest) return "alias";
    if (prior !== undefined) this.#fence();
    this.#seen.set(key, digest);
    return "new";
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

export function ndjson(value: unknown): Uint8Array {
  const json = canonicalProtocolJson(value);
  const line = new Uint8Array(json.length + 1);
  line.set(json);
  line[line.length - 1] = 0x0a;
  return line;
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
  return value as Record<string, unknown>;
}

function array(value: unknown): readonly unknown[] {
  if (!Array.isArray(value)) throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  return value;
}

function string(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
  return value;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value);
}
