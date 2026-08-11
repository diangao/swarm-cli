import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomBytes } from "node:crypto";

import {
  RetainedDriverEventPump,
  driverArtifactDigest,
  type CodexJsonRpcNotification,
  type CodexJsonRpcRequest,
  type CodexRuntimeHost,
  type CodexSpawnedRuntime,
  type CodexTransport,
  type CodexTurnObservationCorrelation,
  type CodexWireConsumer,
  type DriverLifecycleObservation,
  type DriverPrivateEventRetentionPort,
  type DriverPumpObservation,
  type DriverRegisteredEventWaiter,
  type DriverResumeSpec,
  type DriverStartWriteWitness,
  type DriverStatus,
  type DriverLaunchSpec,
  type SpawnHandle,
} from "@swarm/drivers";
import type {
  ArtifactDigest,
  CommandId,
  SpawnedLaunchFence,
  StateInstanceId,
  StopReason,
} from "@swarm/protocol";

export type CodexChildRuntimeHostOptions = {
  executable: string;
  prefixArgv?: readonly string[];
  cwd?: string;
  environment?: Readonly<Record<string, string>>;
  retention: DriverPrivateEventRetentionPort;
};

export class CodexChildRuntimeHost implements CodexRuntimeHost {
  readonly #options: CodexChildRuntimeHostOptions;
  readonly #children = new Map<string, {
    child: ChildProcessWithoutNullStreams;
    launch: SpawnedLaunchFence;
  }>();

  constructor(options: CodexChildRuntimeHostOptions) {
    this.#options = options;
  }

  async probe(spec: import("@swarm/drivers").DriverProbeSpec) {
    const versionOutput = execFileSync(
      this.#options.executable,
      [...(this.#options.prefixArgv ?? []), "--version"],
      { cwd: this.#options.cwd, env: { ...this.#options.environment }, encoding: "utf8" },
    ).trim();
    return {
      versionOutput,
      executableDigest: spec.executableDigest,
      wireProtocolDigest: spec.wireProtocolDigest,
    };
  }

  spawn(
    spec: DriverLaunchSpec,
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: CodexWireConsumer },
  ): Promise<CodexSpawnedRuntime> {
    return Promise.resolve(this.#open(
      spec.launch,
      nextStateId(),
      spec.transportDigest,
      input.argv,
      input.acceptWireMessage,
    ));
  }

  resume(
    spec: DriverResumeSpec,
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: CodexWireConsumer },
  ) {
    const opened = this.#open(
      spec.launch,
      spec.launch.stateInstanceId,
      driverArtifactDigest("codex:json-rpc:v2"),
      input.argv,
      input.acceptWireMessage,
    );
    return Promise.resolve({
      ...opened,
      resumeWaiterId: nextCommandId(),
      resumeBindingDigest: driverArtifactDigest("codex:resume"),
    });
  }

  async status(process: SpawnHandle): Promise<DriverStatus> {
    const state = this.#children.get(process.processHandleRefPrivate);
    return state === undefined || state.child.exitCode !== null
      ? { kind: "terminal", reason: "process_exited" }
      : { kind: "spawning", launch: state.launch };
  }

  async stop(process: SpawnHandle, _reason: StopReason): Promise<void> {
    const state = this.#children.get(process.processHandleRefPrivate);
    if (state === undefined) return;
    this.#children.delete(process.processHandleRefPrivate);
    await terminateChild(state.child, "CODEX_CHILD_STOP_FAILED");
  }

  async close(): Promise<void> {
    const children = [...this.#children.values()];
    this.#children.clear();
    const results = await Promise.allSettled(
      children.map(({ child }) => terminateChild(child, "CODEX_CHILD_STOP_FAILED")),
    );
    const failures = results.flatMap((result) => result.status === "rejected" ? [result.reason] : []);
    if (failures.length > 0) throw new AggregateError(failures, "CODEX_HOST_CLOSE_FAILED");
  }

  #open(
    launch: DriverLaunchSpec["launch"],
    stateInstanceId: StateInstanceId,
    transportDigest: ArtifactDigest,
    argv: readonly string[],
    acceptWireMessage: CodexWireConsumer,
  ): CodexSpawnedRuntime {
    const child = spawn(
      this.#options.executable,
      [...(this.#options.prefixArgv ?? []), ...argv],
      {
        cwd: this.#options.cwd,
        env: { ...this.#options.environment },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    child.stderr.resume();
    const processHandleRefPrivate = `child:${child.pid ?? "pending"}:${randomBytes(8).toString("hex")}`;
    const process: SpawnHandle = {
      launchId: launch.launchId,
      stateInstanceId,
      processHandleRefPrivate,
      processHandleDigest: driverArtifactDigest(processHandleRefPrivate),
      transportDigest,
    };
    const pump = new RetainedDriverEventPump(stateInstanceId, this.#options.retention);
    const transport = new CodexChildTransport(child, pump, acceptWireMessage);
    this.#children.set(processHandleRefPrivate, {
      child,
      launch: { ...launch, stateInstanceId },
    });
    return {
      process,
      pump,
      transport,
      cursorOwnerToken: randomDigest(),
      initializeWaiterId: nextCommandId(),
      initializeBindingDigest: driverArtifactDigest("codex:initialize"),
    };
  }
}

type Predecessor = DriverStartWriteWitness | DriverRegisteredEventWaiter;
type Pending = {
  predecessor: Predecessor;
  correlation: CodexTurnObservationCorrelation | undefined;
  resolve(): void;
  reject(error: unknown): void;
};

class CodexChildTransport implements CodexTransport {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #pump: RetainedDriverEventPump;
  readonly #accept: CodexWireConsumer;
  readonly #pending = new Map<string, Pending>();
  #lifecycle: DriverRegisteredEventWaiter | undefined;
  #active: CodexTurnObservationCorrelation | undefined;
  #buffer = "";
  #held: unknown[] = [];

  constructor(
    child: ChildProcessWithoutNullStreams,
    pump: RetainedDriverEventPump,
    accept: CodexWireConsumer,
  ) {
    this.#child = child;
    this.#pump = pump;
    this.#accept = accept;
    child.stdout.on("data", (chunk: Buffer) => this.#ingest(chunk));
    child.on("error", (error) => this.#failAll(error));
    child.on("exit", () => this.#failAll(new Error("CODEX_CHILD_EXITED")));
  }

  request(
    request: CodexJsonRpcRequest,
    predecessor: Predecessor,
    onWritten?: () => void,
    correlation?: CodexTurnObservationCorrelation,
  ): Promise<void> {
    if (this.#pending.has(request.id)) return Promise.reject(new Error("CODEX_REQUEST_DUPLICATE"));
    const waiter = predecessorWaiter(predecessor);
    if (waiter.stream === "lifecycle") this.#lifecycle = waiter;
    if (correlation !== undefined) this.#active = correlation;
    const result = new Promise<void>((resolve, reject) => {
      this.#pending.set(request.id, { predecessor, correlation, resolve, reject });
    });
    this.#write(request);
    onWritten?.();
    void this.#flushHeld();
    return result;
  }

  async notify(notification: CodexJsonRpcNotification, _predecessor: Predecessor): Promise<void> {
    this.#write(notification);
  }

  #write(value: unknown): void {
    const bytes = `${JSON.stringify(value)}\n`;
    if (!this.#child.stdin.write(bytes, "utf8")) {
      this.#child.stdin.once("drain", () => undefined);
    }
  }

  #ingest(chunk: Buffer): void {
    this.#buffer += chunk.toString("utf8");
    while (true) {
      const newline = this.#buffer.indexOf("\n");
      if (newline < 0) return;
      const line = this.#buffer.slice(0, newline);
      this.#buffer = this.#buffer.slice(newline + 1);
      if (line.length === 0) continue;
      let raw: unknown;
      try {
        raw = JSON.parse(line);
      } catch {
        this.#failAll(new Error("CODEX_WIRE_INVALID"));
        return;
      }
      if (this.#lifecycle === undefined && this.#active === undefined) this.#held.push(raw);
      else void this.#dispatch(raw);
    }
  }

  async #dispatch(raw: unknown): Promise<void> {
    try {
      const id = typeof raw === "object" && raw !== null && typeof (raw as { id?: unknown }).id === "string"
        ? (raw as { id: string }).id
        : undefined;
      const pending = id === undefined ? undefined : this.#pending.get(id);
      const correlation = pending?.correlation ?? this.#active;
      const predecessor = pending?.predecessor
        ?? correlation?.predecessor
        ?? this.#lifecycle;
      if (predecessor === undefined) {
        this.#held.push(raw);
        return;
      }
      const waiter = predecessorWaiter(predecessor);
      const lease = this.#pump.leaseForWaiter(waiter);
      const events = this.#accept(raw);
      const observations = observationsFor(events, waiter, correlation, this.#lifecycle);
      if (observations.length > 0) await this.#pump.enqueue(lease, observations);
      if (pending !== undefined && id !== undefined) {
        this.#pending.delete(id);
        pending.resolve();
      }
    } catch (error) {
      this.#failAll(error);
    }
  }

  #failAll(error: unknown): void {
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
  }

  async #flushHeld(): Promise<void> {
    const held = this.#held;
    this.#held = [];
    for (const raw of held) await this.#dispatch(raw);
  }
}

function observationsFor(
  events: readonly import("@swarm/protocol").NormalizedDriverEvent[],
  waiter: DriverRegisteredEventWaiter,
  correlation: CodexTurnObservationCorrelation | undefined,
  lifecycleWaiter: DriverRegisteredEventWaiter | undefined,
): readonly DriverPumpObservation[] {
  return events.map((event): DriverPumpObservation => {
    if (event.kind === "runtime_ready" || event.kind === "runtime_terminal") {
      const exactWaiter = lifecycleWaiter ?? waiter;
      const lifecycle: DriverLifecycleObservation = {
        stream: "lifecycle",
        resolvedWaiterId: exactWaiter.waiterId,
        bindingDigest: exactWaiter.bindingDigest,
        event,
      };
      return lifecycle;
    }
    if (correlation === undefined) throw new Error("CODEX_TURN_CORRELATION_MISSING");
    return {
      stream: "turn",
      resolvedWaiterId: correlation.predecessor.waiterId,
      sourceMessageId: correlation.sourceMessageId,
      bindingDigest: correlation.predecessor.bindingDigest,
      event,
    };
  });
}

function predecessorWaiter(predecessor: Predecessor): DriverRegisteredEventWaiter {
  return "lease" in predecessor
    ? (predecessor as DriverStartWriteWitness).waiter
    : predecessor as DriverRegisteredEventWaiter;
}

function nextStateId(): StateInstanceId {
  return `sti_${randomBytes(13).toString("hex")}` as StateInstanceId;
}

function nextCommandId(): CommandId {
  return `cmd_${randomBytes(13).toString("hex")}` as CommandId;
}

function randomDigest(): ArtifactDigest {
  return driverArtifactDigest(randomBytes(32));
}

async function terminateChild(
  child: ChildProcessWithoutNullStreams,
  failure: string,
): Promise<void> {
  if (child.exitCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const force = setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    }, 2_000);
    force.unref();
    const deadline = setTimeout(() => settle(new Error(failure)), 5_000);
    deadline.unref();
    const onExit = () => settle();
    const onError = (error: Error) => settle(error);
    const settle = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(force);
      clearTimeout(deadline);
      child.off("exit", onExit);
      child.off("error", onError);
      if (error === undefined) resolve();
      else reject(error);
    };
    child.once("exit", onExit);
    child.once("error", onError);
    child.kill("SIGTERM");
  });
}
