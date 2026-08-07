import { storageFail } from "./errors.js";

const POSTGRES_CONTROLS = [
  "UNIQUE NULLS NOT DISTINCT (target_kind, target_id, thread_root_message_id)",
  "UNIQUE NULLS NOT DISTINCT (agent_id, target_kind, target_id, thread_root_message_id)",
  "FOREIGN KEY (server_id, parent_task_id) REFERENCES tasks(server_id, task_id)",
  "FOREIGN KEY (replay_of, agent_id, producer_fact_id)",
  "UNIQUE (producer_fact_id, event_kind, event_version)",
  "'receipt.record.v1', 'artifact.mutate.v1', 'outbox.mutate.v1'",
  "CREATE UNIQUE INDEX task_claims_one_open",
  "input_written_at IS NULL OR daemon_accepted_at IS NOT NULL",
] as const;

const SQLITE_CONTROLS = [
  "target_key TEXT NOT NULL COLLATE BINARY",
  "UNIQUE (session_id, target_key)",
  "FOREIGN KEY (replay_of, agent_id, producer_fact_id)",
  "model_visible_at IS NULL OR input_written_at IS NOT NULL",
  "state NOT IN ('server_confirmed', 'canceled')",
] as const;

function requireControls(sql: string, controls: readonly string[], dialect: string): void {
  for (const control of controls) {
    if (!sql.includes(control)) storageFail("INVALID_MIGRATION", { dialect, control });
  }
}

export function assertPostgresMigrationContract(sql: string): void {
  requireControls(sql, POSTGRES_CONTROLS, "postgres");
}

export function assertSqliteMigrationContract(sql: string): void {
  requireControls(sql, SQLITE_CONTROLS, "sqlite");
}
