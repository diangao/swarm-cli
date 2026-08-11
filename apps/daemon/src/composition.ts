import { DaemonCore, type NativeServerPort } from "@swarm/daemon-core";
import {
  ClaudeNativeProcessDriver,
  CodexNativeProcessDriver,
  type NativeRuntimePort,
} from "@swarm/drivers";
import { DaemonJournal } from "@swarm/storage";
import type { LaunchId, ProtocolVersion } from "@swarm/protocol";

import { RandomCommandIdSource } from "./ids.js";
import { LoopbackNativeServer, LoopbackServerConnection } from "./loopback.js";
import { NativeSqliteJournal } from "./native-journal.js";
import { ClaudeChildRuntimeHost } from "./claude-runtime-host.js";
import { CodexChildRuntimeHost } from "./codex-runtime-host.js";
import { NativeCursorClaimCoordinator, NativeTurnRuntime } from "./native-turn-runtime.js";
import { SqlitePrivateDriverEventRetention } from "./private-driver-events.js";
import { RetainedTurnJournal } from "./retained-turn-journal.js";

export type DaemonApp = {
  core: DaemonCore;
  journal: NativeSqliteJournal;
  connection: LoopbackServerConnection;
  waveZeroJournal: DaemonJournal;
  server?: LoopbackNativeServer;
};

export async function createDaemonApp(input: {
  sqlitePath: string;
  waveZeroSqlitePath: string;
  driver: NativeRuntimePort;
  serverUrl: string;
}): Promise<DaemonApp> {
  const journal = new NativeSqliteJournal(input.sqlitePath, input.driver.driverKind);
  let waveZeroJournal: DaemonJournal | undefined;
  try {
    waveZeroJournal = DaemonJournal.open(input.waveZeroSqlitePath);
    waveZeroJournal.migrate();
    const connection = new LoopbackServerConnection(input.serverUrl);
    return {
      core: new DaemonCore({
        server: connection,
        journal,
        driver: input.driver,
        ids: new RandomCommandIdSource(),
      }),
      journal,
      connection,
      waveZeroJournal,
    };
  } catch (error) {
    waveZeroJournal?.close();
    journal.close();
    throw error;
  }
}

export async function createInProcessLoopbackDaemon(input: {
  sqlitePath: string;
  waveZeroSqlitePath: string;
  driver: NativeRuntimePort;
  handler: NativeServerPort;
}): Promise<DaemonApp> {
  const server = new LoopbackNativeServer(input.handler);
  const serverUrl = await server.start();
  try {
    const app = await createDaemonApp({
      sqlitePath: input.sqlitePath,
      waveZeroSqlitePath: input.waveZeroSqlitePath,
      driver: input.driver,
      serverUrl,
    });
    return { ...app, server };
  } catch (error) {
    await server.close();
    throw error;
  }
}

export async function closeDaemonApp(app: DaemonApp): Promise<void> {
  app.connection.close();
  try {
    await app.server?.close();
  } finally {
    try {
      app.journal.close();
    } finally {
      app.waveZeroJournal.close();
    }
  }
}

export type NativeDriverRuntimeComposition = {
  journal: DaemonJournal;
  retention: SqlitePrivateDriverEventRetention;
  coordinator: NativeCursorClaimCoordinator;
  codex: CodexNativeProcessDriver;
  claude: ClaudeNativeProcessDriver;
  runtime: NativeTurnRuntime;
  turnJournal: RetainedTurnJournal;
  close(): Promise<void>;
};

export function createNativeDriverRuntime(input: {
  waveZeroSqlitePath: string;
  privateLaunchRoot: string;
  sourceWorkspace?: string;
  protocolVersion: ProtocolVersion;
  launchId: LaunchId;
  codex: {
    executable: string;
    prefixArgv?: readonly string[];
    cwd?: string;
    environment?: Readonly<Record<string, string>>;
  };
  claude: {
    executable: string;
    prefixArgv?: readonly string[];
    cwd?: string;
    environment?: Readonly<Record<string, string>>;
  };
}): NativeDriverRuntimeComposition {
  const journal = DaemonJournal.open(input.waveZeroSqlitePath);
  let retention: SqlitePrivateDriverEventRetention | undefined;
  try {
    journal.migrate();
    retention = new SqlitePrivateDriverEventRetention({
      launchRoot: input.privateLaunchRoot,
      protocolVersion: input.protocolVersion,
      launchId: input.launchId,
      ...(input.sourceWorkspace === undefined ? {} : { sourceWorkspace: input.sourceWorkspace }),
    });
    const privateRetention = retention;
    const coordinator = new NativeCursorClaimCoordinator({ journal });
    const codexHost = new CodexChildRuntimeHost({ ...input.codex, retention: privateRetention });
    const claudeHost = new ClaudeChildRuntimeHost({ ...input.claude, retention: privateRetention });
    const codex = new CodexNativeProcessDriver(codexHost, privateRetention, coordinator);
    const claude = new ClaudeNativeProcessDriver(claudeHost, privateRetention, coordinator);
    const runtime = new NativeTurnRuntime({ journal, coordinator, codex, claude });
    const turnJournal = new RetainedTurnJournal(privateRetention);
    let closed = false;
    return {
      journal,
      retention: privateRetention,
      coordinator,
      codex,
      claude,
      runtime,
      turnJournal,
      async close() {
        if (closed) return;
        closed = true;
        const hostResults = await Promise.allSettled([
          codexHost.close(),
          claudeHost.close(),
        ]);
        try {
          const failures = hostResults.flatMap(
            (result) => result.status === "rejected" ? [result.reason] : [],
          );
          if (failures.length > 0) throw new AggregateError(failures, "NATIVE_HOST_CLOSE_FAILED");
        } finally {
          try {
            journal.close();
          } finally {
            privateRetention.close();
          }
        }
      },
    };
  } catch (error) {
    retention?.close();
    journal.close();
    throw error;
  }
}
