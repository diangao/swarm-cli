import { TextDecoder } from "node:util";

import type {
  ArtifactDigest,
  DriverSession,
  DriverTurnBinding,
  NormalizedDriverEvent,
  SessionId,
  TurnId,
} from "@swarm/protocol";
import type { CompiledNativeTurn } from "@swarm/runtime-contract";
import { protocolDigest } from "@swarm/runtime-contract";

import { DriverNormalizationError } from "../normalizer.js";
import { CODEX_CLIENT_INFO, CODEX_METHOD } from "./constants.js";
import type { CodexJsonRpcNotification, CodexJsonRpcRequest } from "./types.js";

type PendingTurn = {
  readonly kind: "start" | "steer";
  readonly requestId: string;
  readonly protocolTurnId: TurnId;
  readonly clientUserMessageId: string;
  readonly binding: DriverTurnBinding;
  requestWritten: boolean;
  runtimeTurnId?: string;
  inputWritten: boolean;
  modelVisible: boolean;
  completed: boolean;
};

type PendingRequest =
  | { readonly kind: "initialize" }
  | { readonly kind: "thread_start"; readonly sessionId: SessionId }
  | { readonly kind: "thread_resume"; readonly sessionId: SessionId; readonly threadId: string }
  | { readonly kind: "turn"; readonly turn: PendingTurn }
  | { readonly kind: "interrupt"; readonly protocolTurnId: TurnId; readonly runtimeTurnId: string };

const decoder = new TextDecoder("utf-8", { fatal: true });

export class CodexWireState {
  readonly #pending = new Map<string, PendingRequest>();
  readonly #turns = new Map<TurnId, PendingTurn>();
  readonly #runtimeTurns = new Map<string, TurnId>();
  readonly #eventDigests = new Map<string, ArtifactDigest>();
  #threadId: string | undefined;
  #sessionId: SessionId | undefined;
  #initializeComplete = false;
  #clientInitialized = false;
  #terminal = false;

  initializeRequest(id: string): CodexJsonRpcRequest {
    this.#reserve(id, { kind: "initialize" });
    return request(id, CODEX_METHOD.initialize, {
      clientInfo: CODEX_CLIENT_INFO,
      capabilities: { experimentalApi: true },
    });
  }

  initializedNotification(): CodexJsonRpcNotification {
    if (!this.#initializeComplete || this.#clientInitialized) this.#order();
    this.#clientInitialized = true;
    return Object.freeze({
      jsonrpc: "2.0" as const,
      method: CODEX_METHOD.initialized,
      params: Object.freeze({}),
    });
  }

  threadStartRequest(id: string, sessionId: SessionId): CodexJsonRpcRequest {
    if (!this.#clientInitialized) this.#order();
    this.#reserve(id, { kind: "thread_start", sessionId });
    return request(id, CODEX_METHOD.threadStart, {
      ephemeral: false,
      experimentalRawEvents: false,
    });
  }

  threadResumeRequest(id: string, sessionId: SessionId, threadId: string): CodexJsonRpcRequest {
    if (!this.#clientInitialized) this.#order();
    requirePrivateRef(threadId);
    this.#reserve(id, { kind: "thread_resume", sessionId, threadId });
    return request(id, CODEX_METHOD.threadResume, { threadId, excludeTurns: true });
  }

  turnStartRequest(
    id: string,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
  ): CodexJsonRpcRequest {
    this.#assertSession(session);
    if (binding.mode.kind !== "ordinary" || binding.inputOrdinal !== 0) this.#fence();
    return this.#turnRequest("start", id, input, binding, {
      threadId: session.runtimeSessionRef,
      input: [{ type: "text", text: compiledText(input) }],
      clientUserMessageId: binding.invocation.invocationId,
    });
  }

  turnSteerRequest(
    id: string,
    session: DriverSession,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding & { expectedTurnId: TurnId },
  ): CodexJsonRpcRequest {
    this.#assertSession(session);
    const active = this.#turns.get(binding.expectedTurnId);
    if (
      active === undefined
      || active.completed
      || active.runtimeTurnId === undefined
      || binding.protocolTurnId !== binding.expectedTurnId
      || binding.mode.kind !== "steer"
      || binding.mode.expectedTurnId !== binding.expectedTurnId
      || binding.rootProducerFactId !== active.binding.rootProducerFactId
      || binding.inputOrdinal !== active.binding.inputOrdinal + 1
    ) this.#fence();
    return this.#turnRequest("steer", id, input, binding, {
      threadId: session.runtimeSessionRef,
      expectedTurnId: active.runtimeTurnId,
      input: [{ type: "text", text: compiledText(input) }],
      clientUserMessageId: binding.invocation.invocationId,
    });
  }

  interruptRequest(id: string, session: DriverSession, expectedTurnId: TurnId): CodexJsonRpcRequest {
    this.#assertSession(session);
    const active = this.#turns.get(expectedTurnId);
    if (active === undefined || active.completed || active.runtimeTurnId === undefined) this.#fence();
    this.#reserve(id, {
      kind: "interrupt",
      protocolTurnId: expectedTurnId,
      runtimeTurnId: active.runtimeTurnId,
    });
    return request(id, CODEX_METHOD.turnInterrupt, {
      threadId: session.runtimeSessionRef,
      turnId: active.runtimeTurnId,
    });
  }

  markRequestWritten(id: string): void {
    const pending = this.#pending.get(id);
    if (pending === undefined) this.#fence();
    if (pending.kind === "turn") pending.turn.requestWritten = true;
  }

  cancelPendingRequest(id: string): void {
    const pending = this.#pending.get(id);
    if (pending === undefined) return;
    this.#pending.delete(id);
    if (pending.kind === "turn" && pending.turn.kind === "start") {
      const current = this.#turns.get(pending.turn.protocolTurnId);
      if (current === pending.turn) this.#turns.delete(pending.turn.protocolTurnId);
    }
  }

  assertSession(session: DriverSession): void {
    this.#assertSession(session);
  }

  accept(message: unknown): readonly NormalizedDriverEvent[] {
    if (this.#terminal) this.#order();
    const record = object(message);
    if (record["jsonrpc"] !== "2.0") this.#protocol();
    if (typeof record["id"] === "string") return this.#acceptResponse(record);
    if (typeof record["method"] === "string") return this.#acceptNotification(record);
    return this.#protocol();
  }

  #turnRequest(
    kind: "start" | "steer",
    id: string,
    input: CompiledNativeTurn,
    binding: DriverTurnBinding,
    params: Readonly<Record<string, unknown>>,
  ): CodexJsonRpcRequest {
    if (binding.inputDigest !== input.inputDigest || this.#turns.has(binding.protocolTurnId) && kind === "start") {
      this.#fence();
    }
    const turn: PendingTurn = {
      kind,
      requestId: id,
      protocolTurnId: binding.protocolTurnId,
      clientUserMessageId: binding.invocation.invocationId,
      binding,
      requestWritten: false,
      inputWritten: false,
      modelVisible: false,
      completed: false,
    };
    this.#reserve(id, { kind: "turn", turn });
    if (kind === "start") this.#turns.set(binding.protocolTurnId, turn);
    return request(id, kind === "start" ? CODEX_METHOD.turnStart : CODEX_METHOD.turnSteer, params);
  }

  #acceptResponse(response: Record<string, unknown>): readonly NormalizedDriverEvent[] {
    const id = response["id"] as string;
    const pending = this.#pending.get(id);
    if (pending === undefined) this.#fence();
    if ("error" in response) this.#protocol();
    const result = object(response["result"]);
    this.#pending.delete(id);

    if (pending.kind === "initialize") {
      if (this.#initializeComplete) this.#order();
      this.#initializeComplete = true;
      return [];
    }
    if (pending.kind === "thread_start" || pending.kind === "thread_resume") {
      const thread = object(result["thread"]);
      const threadId = string(thread["id"]);
      if (pending.kind === "thread_resume" && threadId !== pending.threadId) this.#fence();
      this.#threadId = threadId;
      this.#sessionId = pending.sessionId;
      return [{
        kind: "runtime_ready",
        runtimeSessionRef: threadId,
        runtimeSessionRefDigest: protocolDigest({ sessionId: pending.sessionId, threadId }),
      }];
    }
    if (pending.kind === "interrupt") return [];

    const turn = pending.turn;
    if (!turn.requestWritten) this.#order();
    const runtimeTurnId = pending.turn.kind === "start"
      ? string(object(result["turn"])["id"])
      : string(result["turnId"]);
    if (turn.kind === "steer") {
      const active = this.#turns.get(turn.protocolTurnId);
      if (active?.runtimeTurnId !== runtimeTurnId) this.#fence();
      turn.runtimeTurnId = runtimeTurnId;
      turn.inputWritten = true;
      this.#turns.set(turn.protocolTurnId, turn);
      this.#runtimeTurns.set(runtimeTurnId, turn.protocolTurnId);
      return [{
        kind: "input_written",
        turnId: turn.protocolTurnId,
        runtimeWriteId: turn.binding.runtimeWriteId,
      }];
    }
    if (turn.runtimeTurnId !== undefined && turn.runtimeTurnId !== runtimeTurnId) this.#fence();
    turn.runtimeTurnId = runtimeTurnId;
    turn.inputWritten = true;
    this.#runtimeTurns.set(runtimeTurnId, turn.protocolTurnId);
    return [
      {
        kind: "turn_started",
        turnId: turn.protocolTurnId,
        driverTurnRefDigest: protocolDigest({ threadId: this.#threadId, runtimeTurnId }),
      },
      {
        kind: "input_written",
        turnId: turn.protocolTurnId,
        runtimeWriteId: turn.binding.runtimeWriteId,
      },
    ];
  }

  #acceptNotification(notification: Record<string, unknown>): readonly NormalizedDriverEvent[] {
    const method = string(notification["method"]);
    const params = object(notification["params"]);
    if (method === CODEX_METHOD.turnStarted) return this.#turnStarted(params, notification);
    if (method === CODEX_METHOD.itemCompleted) return this.#itemCompleted(params, notification);
    if (method === CODEX_METHOD.turnCompleted) return this.#turnCompleted(params, notification);
    if (method === CODEX_METHOD.threadStatusChanged) {
      const threadId = string(params["threadId"]);
      if (threadId !== this.#threadId) this.#fence();
      const status = object(params["status"]);
      const type = string(status["type"]);
      if (type === "notLoaded" || type === "systemError") {
        this.#terminal = true;
        return [{ kind: "runtime_terminal", reason: "process_exited" }];
      }
      if (type === "idle") return [];
      if (type === "active") {
        const flags = array(status["activeFlags"]);
        if (flags.some((flag) => flag !== "waitingOnApproval" && flag !== "waitingOnUserInput")) {
          this.#protocol();
        }
        return [];
      }
      return this.#protocol();
    }
    return this.#protocol();
  }

  #turnStarted(
    params: Record<string, unknown>,
    raw: Record<string, unknown>,
  ): readonly NormalizedDriverEvent[] {
    const threadId = string(params["threadId"]);
    const wireTurn = object(params["turn"]);
    const runtimeTurnId = string(wireTurn["id"]);
    if (threadId !== this.#threadId) this.#fence();
    const protocolTurnId = this.#runtimeTurns.get(runtimeTurnId);
    if (protocolTurnId === undefined) {
      const pending = [...this.#pending.values()].find((value) => value.kind === "turn");
      if (pending?.kind === "turn" && pending.turn.requestWritten) this.#order();
      this.#fence();
    }
    const turn = this.#turns.get(protocolTurnId);
    if (turn === undefined || !turn.inputWritten) this.#order();
    const items = array(wireTurn["items"]);
    const userMessages = items.filter((item) => object(item)["type"] === "userMessage");
    const digest = protocolDigest(raw);
    for (const userMessage of userMessages) {
      const clientId = object(userMessage)["clientId"];
      if (typeof clientId !== "string") continue;
      const prior = this.#eventDigests.get(
        `turn/started:${threadId}:${runtimeTurnId}:${clientId}`,
      );
      if (prior === digest) return [];
    }
    const matching = userMessages.filter(
      (item) => object(item)["clientId"] === turn.clientUserMessageId,
    );
    if (matching.length !== 1) this.#fence();
    const replay = this.#aliasOrRemember(
      `turn/started:${threadId}:${runtimeTurnId}:${turn.clientUserMessageId}`,
      raw,
    );
    if (replay === "alias") return [];
    turn.modelVisible = true;
    return [{
      kind: "model_visible",
      turnId: protocolTurnId,
      visibilityEventId: turn.binding.visibilityEventId,
    }];
  }

  #itemCompleted(
    params: Record<string, unknown>,
    raw: Record<string, unknown>,
  ): readonly NormalizedDriverEvent[] {
    const turn = this.#turnFromParams(params);
    if (!turn.modelVisible) this.#order();
    const item = object(params["item"]);
    if (item["type"] !== "agentMessage") return this.#protocol();
    const digest = this.#aliasOrRemember(`item/completed:${string(item["id"])}`, raw);
    if (digest === "alias") return [];
    return [{ kind: "assistant_reply", turnId: turn.protocolTurnId, text: string(item["text"]) }];
  }

  #turnCompleted(
    params: Record<string, unknown>,
    raw: Record<string, unknown>,
  ): readonly NormalizedDriverEvent[] {
    const turn = this.#turnFromParams(params, true);
    const wireTurn = object(params["turn"]);
    const digest = this.#aliasOrRemember(
      `turn/completed:${string(params["threadId"])}:${string(wireTurn["id"])}`,
      raw,
    );
    if (digest === "alias") return [];
    if (turn.completed || !turn.modelVisible) this.#order();
    turn.completed = true;
    return [{ kind: "turn_completed", turnId: turn.protocolTurnId }];
  }

  #turnFromParams(params: Record<string, unknown>, allowCompleted = false): PendingTurn {
    if (string(params["threadId"]) !== this.#threadId) this.#fence();
    const runtimeTurnId = typeof params["turnId"] === "string"
      ? params["turnId"]
      : string(object(params["turn"])["id"]);
    const protocolTurnId = this.#runtimeTurns.get(runtimeTurnId);
    if (protocolTurnId === undefined) this.#fence();
    const turn = this.#turns.get(protocolTurnId);
    if (turn === undefined || turn.completed && !allowCompleted) this.#order();
    return turn;
  }

  #assertSession(session: DriverSession): void {
    if (
      this.#threadId === undefined
      || session.runtimeSessionRef !== this.#threadId
      || session.launch.sessionId !== this.#sessionId
      || session.driverIdentity.runtime !== "codex"
    ) this.#fence();
  }

  #reserve(id: string, pending: PendingRequest): void {
    requirePrivateRef(id);
    if (this.#pending.has(id)) this.#fence();
    this.#pending.set(id, pending);
  }

  #aliasOrRemember(key: string, value: unknown): "new" | "alias" {
    const digest = protocolDigest(value);
    const prior = this.#eventDigests.get(key);
    if (prior === digest) return "alias";
    if (prior !== undefined) this.#fence();
    this.#eventDigests.set(key, digest);
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

function request(
  id: string,
  method: CodexJsonRpcRequest["method"],
  params: Readonly<Record<string, unknown>>,
): CodexJsonRpcRequest {
  requirePrivateRef(id);
  return Object.freeze({ jsonrpc: "2.0" as const, id, method, params: Object.freeze(params) });
}

function compiledText(input: CompiledNativeTurn): string {
  return decoder.decode(input.bytes);
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

function requirePrivateRef(value: string): void {
  if (value.length === 0 || /[\r\n\0]/u.test(value)) {
    throw new DriverNormalizationError("DRIVER_PROTOCOL_UNSUPPORTED");
  }
}
