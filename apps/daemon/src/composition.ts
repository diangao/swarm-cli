import { DaemonCore, type NativeServerPort } from "@swarm/daemon-core";
import type { NativeRuntimePort } from "@swarm/drivers";
import { DaemonJournal } from "@swarm/storage";

import { RandomCommandIdSource } from "./ids.js";
import { LoopbackNativeServer, LoopbackServerConnection } from "./loopback.js";
import { NativeSqliteJournal } from "./native-journal.js";

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
