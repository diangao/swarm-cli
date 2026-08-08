import { createHash } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { Duplex } from "node:stream";

import type {
  AcquireConsumePermit,
  BeginNativeWrite,
  ConsumePermit,
  DeliveryAck,
  DeliveryAckResult,
  ReconcileDeliveryAttempt,
  ReconcileDeliveryResult,
  ResumeConsumePermit,
} from "@swarm/protocol";
import {
  PrePermitDisconnectError,
  type NativeServerPort,
  type ReplyCommitCommand,
  type ReplyCommitResult,
  type TaskCommitCommand,
  type TaskCommitResult,
} from "@swarm/daemon-core";

type Method = keyof NativeServerPort;
type WireRequest = { id: number; method: Method; payload: unknown };
type WireResponse = { id: number; ok: true; result: unknown } | { id: number; ok: false; error: string };

function websocketAccept(key: string): string {
  return createHash("sha1").update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest("base64");
}

function serverTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, "utf8");
  if (payload.length < 126) return Buffer.concat([Buffer.from([0x81, payload.length]), payload]);
  if (payload.length <= 0xffff) {
    const header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(payload.length, 2);
    return Buffer.concat([header, payload]);
  }
  throw new Error("LOOPBACK_FRAME_TOO_LARGE");
}

function clientTextFrames(buffer: Buffer): { messages: string[]; remainder: Buffer } {
  const messages: string[] = [];
  let offset = 0;
  while (buffer.length - offset >= 2) {
    const first = buffer[offset] ?? 0;
    const second = buffer[offset + 1] ?? 0;
    const opcode = first & 0x0f;
    let length = second & 0x7f;
    let header = 2;
    if (length === 126) {
      if (buffer.length - offset < 4) break;
      length = buffer.readUInt16BE(offset + 2);
      header = 4;
    } else if (length === 127) {
      throw new Error("LOOPBACK_FRAME_TOO_LARGE");
    }
    const masked = (second & 0x80) !== 0;
    const maskLength = masked ? 4 : 0;
    if (buffer.length - offset < header + maskLength + length) break;
    if (opcode === 8) return { messages, remainder: Buffer.alloc(0) };
    if (opcode !== 1) throw new Error("LOOPBACK_TEXT_FRAME_REQUIRED");
    const maskStart = offset + header;
    const payloadStart = maskStart + maskLength;
    const payload = Buffer.from(buffer.subarray(payloadStart, payloadStart + length));
    if (masked) {
      for (let index = 0; index < payload.length; index += 1) {
        payload[index] = (payload[index] ?? 0) ^ (buffer[maskStart + (index % 4)] ?? 0);
      }
    }
    messages.push(payload.toString("utf8"));
    offset = payloadStart + length;
  }
  return { messages, remainder: buffer.subarray(offset) };
}

async function dispatch(handler: NativeServerPort, request: WireRequest): Promise<unknown> {
  switch (request.method) {
    case "acquireConsumePermit":
      return handler.acquireConsumePermit(request.payload as AcquireConsumePermit);
    case "resumeConsumePermit":
      return handler.resumeConsumePermit(request.payload as ResumeConsumePermit);
    case "beginNativeWrite":
      return handler.beginNativeWrite(request.payload as BeginNativeWrite);
    case "acknowledgeDelivery":
      return handler.acknowledgeDelivery(request.payload as DeliveryAck);
    case "reconcileDeliveryAttempt":
      return handler.reconcileDeliveryAttempt(request.payload as ReconcileDeliveryAttempt);
    case "appendReply":
      return handler.appendReply(request.payload as ReplyCommitCommand);
    case "createTask":
      return handler.createTask(request.payload as TaskCommitCommand);
  }
}

export class LoopbackNativeServer {
  readonly #handler: NativeServerPort;
  readonly #server: Server;
  readonly #sockets = new Set<Duplex>();
  #disconnectMethod: Method | undefined;
  #url: string | undefined;

  constructor(handler: NativeServerPort) {
    this.#handler = handler;
    this.#server = createServer();
    this.#server.on("upgrade", (request, socket) => {
      const key = request.headers["sec-websocket-key"];
      if (typeof key !== "string" || request.url !== "/native") {
        socket.destroy();
        return;
      }
      socket.write([
        "HTTP/1.1 101 Switching Protocols",
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Accept: ${websocketAccept(key)}`,
        "",
        "",
      ].join("\r\n"));
      this.#serveSocket(socket);
    });
  }

  async start(): Promise<string> {
    if (this.#url !== undefined) return this.#url;
    await new Promise<void>((resolve, reject) => {
      this.#server.once("error", reject);
      this.#server.listen(0, "127.0.0.1", () => resolve());
    });
    const address = this.#server.address();
    if (address === null || typeof address === "string") throw new Error("LOOPBACK_BIND_FAILED");
    this.#url = `ws://127.0.0.1:${address.port}/native`;
    return this.#url;
  }

  disconnectBeforeNext(method: Method): void {
    this.#disconnectMethod = method;
  }

  async close(): Promise<void> {
    for (const socket of this.#sockets) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.#server.close((error) => error === undefined ? resolve() : reject(error));
      this.#server.closeAllConnections();
    });
  }

  #serveSocket(socket: Duplex): void {
    this.#sockets.add(socket);
    socket.once("close", () => this.#sockets.delete(socket));
    let buffered: Buffer<ArrayBufferLike> = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      let parsed: { messages: string[]; remainder: Buffer };
      try {
        parsed = clientTextFrames(buffered);
      } catch {
        socket.destroy();
        return;
      }
      buffered = parsed.remainder;
      for (const message of parsed.messages) {
        void this.#handleMessage(socket, message);
      }
    });
  }

  async #handleMessage(socket: Duplex, message: string): Promise<void> {
    let request: WireRequest;
    try {
      request = JSON.parse(message) as WireRequest;
    } catch {
      socket.destroy();
      return;
    }
    if (this.#disconnectMethod === request.method) {
      this.#disconnectMethod = undefined;
      socket.destroy();
      return;
    }
    let response: WireResponse;
    try {
      response = { id: request.id, ok: true, result: await dispatch(this.#handler, request) };
    } catch (error) {
      response = { id: request.id, ok: false, error: error instanceof Error ? error.message : "LOOPBACK_HANDLER_ERROR" };
    }
    if (!socket.destroyed) socket.write(serverTextFrame(JSON.stringify(response)));
  }
}

export class LoopbackServerConnection implements NativeServerPort {
  readonly #url: string;
  #socket: WebSocket | undefined;
  #opening: Promise<WebSocket> | undefined;
  #nextId = 1;
  readonly #pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();

  constructor(url: string) {
    this.#url = url;
  }

  acquireConsumePermit(command: AcquireConsumePermit): Promise<ConsumePermit> {
    return this.#call("acquireConsumePermit", command, true) as Promise<ConsumePermit>;
  }
  resumeConsumePermit(command: ResumeConsumePermit): Promise<ConsumePermit> {
    return this.#call("resumeConsumePermit", command) as Promise<ConsumePermit>;
  }
  async beginNativeWrite(command: BeginNativeWrite): Promise<void> {
    await this.#call("beginNativeWrite", command);
  }
  acknowledgeDelivery(command: DeliveryAck): Promise<DeliveryAckResult> {
    return this.#call("acknowledgeDelivery", command) as Promise<DeliveryAckResult>;
  }
  reconcileDeliveryAttempt(command: ReconcileDeliveryAttempt): Promise<ReconcileDeliveryResult> {
    return this.#call("reconcileDeliveryAttempt", command) as Promise<ReconcileDeliveryResult>;
  }
  appendReply(command: ReplyCommitCommand): Promise<ReplyCommitResult> {
    return this.#call("appendReply", command) as Promise<ReplyCommitResult>;
  }
  createTask(command: TaskCommitCommand): Promise<TaskCommitResult> {
    return this.#call("createTask", command) as Promise<TaskCommitResult>;
  }

  close(): void {
    this.#socket?.close();
  }

  async #call(method: Method, payload: unknown, prePermit = false): Promise<unknown> {
    const socket = await this.#open();
    const id = this.#nextId++;
    return new Promise<unknown>((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      try {
        socket.send(JSON.stringify({ id, method, payload } satisfies WireRequest));
      } catch (error) {
        this.#pending.delete(id);
        reject(prePermit ? new PrePermitDisconnectError() : error as Error);
      }
      if (prePermit) {
        const pending = this.#pending.get(id);
        if (pending !== undefined) {
          this.#pending.set(id, {
            resolve: pending.resolve,
            reject: () => pending.reject(new PrePermitDisconnectError()),
          });
        }
      }
    });
  }

  #open(): Promise<WebSocket> {
    if (this.#socket?.readyState === WebSocket.OPEN) return Promise.resolve(this.#socket);
    if (this.#opening !== undefined) return this.#opening;
    this.#opening = new Promise<WebSocket>((resolve, reject) => {
      const socket = new WebSocket(this.#url);
      socket.addEventListener("open", () => {
        this.#socket = socket;
        this.#opening = undefined;
        resolve(socket);
      }, { once: true });
      socket.addEventListener("error", () => {
        this.#opening = undefined;
        reject(new Error("LOOPBACK_CONNECT_FAILED"));
      }, { once: true });
      socket.addEventListener("message", (event) => this.#message(event.data));
      socket.addEventListener("close", () => {
        if (this.#socket === socket) this.#socket = undefined;
        for (const pending of this.#pending.values()) pending.reject(new Error("LOOPBACK_DISCONNECTED"));
        this.#pending.clear();
      });
    });
    return this.#opening;
  }

  #message(data: unknown): void {
    if (typeof data !== "string") return;
    const response = JSON.parse(data) as WireResponse;
    const pending = this.#pending.get(response.id);
    if (pending === undefined) return;
    this.#pending.delete(response.id);
    if (response.ok) pending.resolve(response.result);
    else pending.reject(new Error(response.error));
  }
}
