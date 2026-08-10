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

const SQLITE_V1_CONTROLS = [
  "target_key TEXT NOT NULL COLLATE BINARY",
  "UNIQUE (session_id, target_key)",
  "FOREIGN KEY (replay_of, agent_id, producer_fact_id)",
  "model_visible_at IS NULL OR input_written_at IS NOT NULL",
  "state NOT IN ('server_confirmed', 'canceled')",
] as const;

const SQLITE_V2_CONTROLS = [
  "CREATE TABLE local_agent_slots",
  "CREATE TABLE local_launches",
  "CREATE UNIQUE INDEX local_launches_one_nonterminal_per_agent",
  "CREATE TABLE IF NOT EXISTS native_attempts",
  "CREATE TABLE IF NOT EXISTS native_invocation_entries",
  "UNIQUE (delivery_id, attempt, sequence)",
  "CREATE TABLE visible_message_ids",
  "PRIMARY KEY (session_id, target_key, message_id)",
  "UNIQUE (delivery_id, attempt)",
  "CREATE TABLE notice_visibility",
  "PRIMARY KEY (session_id, target_key, membership_epoch)",
  "CREATE TABLE local_turns",
  "CREATE UNIQUE INDEX local_turns_one_active_per_session",
  "CREATE TABLE driver_event_cursor",
  "UNIQUE (state_instance_id)",
  "CREATE TABLE driver_event_records",
] as const;

const SQLITE_V3_CONTROLS = [
  "ALTER TABLE driver_event_cursor ADD COLUMN reader_journal_instance_id TEXT CHECK (",
  "substr(reader_journal_instance_id, 1, 4) = 'cmd_'",
  "ALTER TABLE local_turns ADD COLUMN delivery_id TEXT CHECK (",
  "substr(delivery_id, 1, 4) = 'dlv_'",
  "ALTER TABLE local_turns ADD COLUMN attempt INTEGER CHECK (",
  "attempt IS NULL OR (attempt >= 1 AND attempt <= 2147483647)",
  "ALTER TABLE local_turns ADD COLUMN invocation_id TEXT CHECK (",
  "substr(invocation_id, 1, 4) = 'cmd_'",
  "ALTER TABLE local_turns ADD COLUMN invocation_generation INTEGER CHECK (",
  "invocation_generation >= 1 AND invocation_generation <= 9007199254740991",
  "ALTER TABLE local_turns ADD COLUMN permit_id TEXT CHECK (",
  "substr(permit_id, 1, 4) = 'cmd_'",
  "ALTER TABLE local_turns ADD COLUMN runtime_write_id TEXT CHECK (",
  "substr(runtime_write_id, 1, 4) = 'cmd_'",
  "ALTER TABLE local_turns ADD COLUMN visibility_event_id TEXT CHECK (",
  "substr(visibility_event_id, 1, 4) = 'cmd_'",
] as const;

const POSTGRES_NATIVE_INGRESS_CONTROLS = [
  "CREATE DOMAIN human_id_text",
  "CREATE FUNCTION message_body_has_content",
  "CREATE TABLE message_audience",
  "CREATE TABLE target_owner_routes",
  "CREATE CONSTRAINT TRIGGER target_owner_routes_authority_match",
  "CREATE TABLE message_owner_routes",
  "CREATE TABLE delivery_invocations",
  "CREATE TABLE delivery_permit_commands",
  "CREATE TABLE delivery_reconciliation_commands",
  "CREATE TABLE delivery_ack_commands",
  "CREATE TABLE delivery_boundary_ack_results",
  "UNIQUE (delivery_id, attempt, permit_id, invocation_generation, invocation_id)",
  "UNIQUE (receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary)",
  "MATCH SIMPLE DEFERRABLE INITIALLY DEFERRED",
  "CREATE CONSTRAINT TRIGGER receipts_boundary_creator_match",
  "c.result_json->'repaired' ? NEW.boundary",
  "receipts_actor_shape_v1",
  "CREATE TABLE task_commands",
  "CREATE TABLE observation_cursors",
  "CREATE TABLE reminder_heads",
  "CREATE TABLE reminder_fires",
  "reminders_fire_producer_once",
  "CREATE TRIGGER outbox_immutable_fields",
  "WHERE boundary = 'model_visible'",
] as const;

function requireControls(sql: string, controls: readonly string[], dialect: string): void {
  for (const control of controls) {
    if (!sql.includes(control)) storageFail("INVALID_MIGRATION", { dialect, control });
  }
}

export function assertPostgresMigrationContract(sql: string): void {
  requireControls(sql, POSTGRES_CONTROLS, "postgres");
}

export function assertSqliteMigrationContract(sql: string, version = "0001"): void {
  if (version === "0001") {
    requireControls(sql, SQLITE_V1_CONTROLS, "sqlite-0001");
    return;
  }
  if (version === "0002") {
    requireControls(sql, SQLITE_V2_CONTROLS, "sqlite-0002");
    return;
  }
  if (version === "0003") {
    requireControls(sql, SQLITE_V3_CONTROLS, "sqlite-0003");
    return;
  }
  storageFail("INVALID_MIGRATION", { dialect: "sqlite", version });
}

export function assertPostgresNativeIngressMigrationContract(sql: string): void {
  requireControls(sql, POSTGRES_NATIVE_INGRESS_CONTROLS, "postgres-native-ingress");
}
