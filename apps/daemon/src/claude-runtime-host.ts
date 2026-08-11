import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomBytes } from "node:crypto";

import {
  RetainedDriverEventPump,
  driverArtifactDigest,
  type ClaudeRuntimeHost,
  type ClaudeSpawnedRuntime,
  type ClaudeTransport,
  type ClaudeTurnObservationCorrelation,
  type ClaudeWireConsumer,
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

export type ClaudeChildRuntimeHostOptions = {
  executable: string;
  prefixArgv?: readonly string[];
  cwd?: string;
  environment?: Readonly<Record<string, string>>;
  retention: DriverPrivateEventRetentionPort;
};

export class ClaudeChildRuntimeHost implements ClaudeRuntimeHost {
  readonly #options: ClaudeChildRuntimeHostOptions;
  readonly #children = new Map<string, {
    child: ChildProcessWithoutNullStreams;
    launch: SpawnedLaunchFence;
  }>();

  constructor(options: ClaudeChildRuntimeHostOptions) {
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
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: ClaudeWireConsumer },
  ): Promise<ClaudeSpawnedRuntime> {
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
    input: { readonly argv: readonly string[]; readonly acceptWireMessage: ClaudeWireConsumer },
  ) {
    const opened = this.#open(
      spec.launch,
      spec.launch.stateInstanceId,
      driverArtifactDigest("claude:stream-json:v1"),
      input.argv,
      input.acceptWireMessage,
    );
    return Promise.resolve({
      ...opened,
      resumeWaiterId: nextCommandId(),
      resumeBindingDigest: driverArtifactDigest("claude:resume"),
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
    await terminateChild(state.child, "CLAUDE_CHILD_STOP_FAILED");
  }

  async close(): Promise<void> {
    const children = [...this.#children.values()];
    this.#children.clear();
    const results = await Promise.allSettled(
      children.map(({ child }) => terminateChild(child, "CLAUDE_CHILD_STOP_FAILED")),
    );
    const failures = results.flatMap((result) => result.status === "rejected" ? [result.reason] : []);
    if (failures.length > 0) throw new AggregateError(failures, "CLAUDE_HOST_CLOSE_FAILED");
  }

  #open(
    launch: DriverLaunchSpec["launch"],
    stateInstanceId: StateInstanceId,
    transportDigest: ArtifactDigest,
    argv: readonly string[],
    acceptWireMessage: ClaudeWireConsumer,
  ): ClaudeSpawnedRuntime {
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
    const transport = new ClaudeChildTransport(child, pump, acceptWireMessage);
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
      initializeBindingDigest: driverArtifactDigest("claude:initialize"),
    };
  }
}

type Predecessor = DriverStartWriteWitness | DriverRegisteredEventWaiter;

class ClaudeChildTransport implements ClaudeTransport {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #pump: RetainedDriverEventPump;
  readonly #accept: ClaudeWireConsumer;
  #lifecycle: DriverRegisteredEventWaiter | undefined;
  #active: ClaudeTurnObservationCorrelation | undefined;
  #buffer = "";
  #held: unknown[] = [];
  #control: { resolve(value: unknown): void; reject(error: unknown): void } | undefined;

  constructor(
    child: ChildProcessWithoutNullStreams,
    pump: RetainedDriverEventPump,
    accept: ClaudeWireConsumer,
  ) {
    this.#child = child;
    this.#pump = pump;
    this.#accept = accept;
    child.stdout.on("data", (chunk: Buffer) => this.#ingest(chunk));
    child.on("error", (error) => this.#fail(error));
    child.on("exit", () => this.#fail(new Error("CLAUDE_CHILD_EXITED")));
  }

  async begin(predecessor: Predecessor): Promise<void> {
    this.#lifecycle = predecessorWaiter(predecessor);
    const held = this.#held;
    this.#held = [];
    for (const raw of held) await this.#dispatch(raw);
  }

  async writeLine(
    line: Uint8Array,
    predecessor: DriverRegisteredEventWaiter,
    onWritten: () => readonly import("@swarm/protocol").NormalizedDriverEvent[],
    correlation: ClaudeTurnObservationCorrelation,
  ): Promise<void> {
    this.#active = correlation;
    this.#child.stdin.write(line);
    const lease = this.#pump.leaseForWaiter(predecessor);
    const observations = observationsFor(onWritten(), predecessor, correlation, this.#lifecycle);
    if (observations.length > 0) await this.#pump.enqueue(lease, observations);
  }

  writeControl(line: Uint8Array, _predecessor: DriverRegisteredEventWaiter): Promise<unknown> {
    if (this.#control !== undefined) return Promise.reject(new Error("CLAUDE_CONTROL_OVERLAP"));
    const result = new Promise<unknown>((resolve, reject) => {
      this.#control = { resolve, reject };
    });
    this.#child.stdin.write(line);
    return result;
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
        this.#fail(new Error("CLAUDE_WIRE_INVALID"));
        return;
      }
      if (this.#lifecycle === undefined && this.#active === undefined) this.#held.push(raw);
      else void this.#dispatch(raw);
    }
  }

  async #dispatch(raw: unknown): Promise<void> {
    try {
      if (isControlResponse(raw)) {
        const control = this.#control;
        if (control === undefined) throw new Error("CLAUDE_CONTROL_UNCLAIMED");
        this.#control = undefined;
        control.resolve(raw);
        return;
      }
      const events = this.#accept(raw);
      const waiter = this.#active?.predecessor ?? this.#lifecycle;
      if (waiter === undefined) {
        this.#held.push(raw);
        return;
      }
      const observations = observationsFor(events, waiter, this.#active, this.#lifecycle);
      if (observations.length > 0) {
        await this.#pump.enqueue(this.#pump.leaseForWaiter(waiter), observations);
      }
    } catch (error) {
      this.#fail(error);
    }
  }

  #fail(error: unknown): void {
    this.#control?.reject(error);
    this.#control = undefined;
  }
}

function observationsFor(
  events: readonly import("@swarm/protocol").NormalizedDriverEvent[],
  waiter: DriverRegisteredEventWaiter,
  correlation: ClaudeTurnObservationCorrelation | undefined,
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
    if (correlation === undefined) throw new Error("CLAUDE_TURN_CORRELATION_MISSING");
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

function isControlResponse(value: unknown): boolean {
  return typeof value === "object" && value !== null
    && (value as { type?: unknown }).type === "control_response";
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
