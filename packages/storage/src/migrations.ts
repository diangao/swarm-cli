import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";

export type Migration = {
  version: string;
  checksum: string;
  sql: string;
};

export type MigrationReceipt = {
  version: string;
  checksum: string;
  applied: boolean;
};

export function checksumMigration(sql: string): string {
  return createHash("sha256").update(sql, "utf8").digest("hex");
}

// Wave 3 is intentionally embedded in the shared-seam file set. The frozen
// ownership contract does not permit an additional migration file, so the
// Wave 3 repository installs this idempotent extension after the existing
// Gate 0/Gate 1 migrations. It remains checksum-addressable and independently
// contract-checked.
export const WAVE3_POSTGRES_MIGRATION = String.raw`
CREATE TABLE IF NOT EXISTS workspace_repositories (
  repository_id text PRIMARY KEY CHECK (repository_id COLLATE "C" ~ '^rpo_[0-9a-hjkmnp-tv-z]{26}$'),
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  repository_digest artifact_digest_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS task_v3_graphs (
  root_task_id task_id_text PRIMARY KEY REFERENCES tasks(task_id),
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  repository_id text NOT NULL REFERENCES workspace_repositories(repository_id),
  graph_revision bigint NOT NULL DEFAULT 0 CHECK (graph_revision >= 0),
  scenario_version integer NOT NULL CHECK (scenario_version >= 1),
  policy_digest artifact_digest_text NOT NULL,
  title_classifier_policy_digest artifact_digest_text NOT NULL,
  UNIQUE (server_id, root_task_id)
);
CREATE TABLE IF NOT EXISTS task_v3_state (
  task_id task_id_text PRIMARY KEY REFERENCES tasks(task_id),
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  root_task_id task_id_text NOT NULL REFERENCES task_v3_graphs(root_task_id),
  parent_task_id task_id_text REFERENCES tasks(task_id),
  repository_id text NOT NULL REFERENCES workspace_repositories(repository_id),
  lane_role text NOT NULL CHECK (lane_role IN ('server','daemon','driver','storage','protocol','verifier','security','review','integration')),
  status text NOT NULL CHECK (status IN ('todo','in_progress','waiting','in_review','done','blocked')),
  row_version bigint NOT NULL CHECK (row_version >= 1),
  required_capabilities jsonb NOT NULL CHECK (jsonb_typeof(required_capabilities) = 'array'),
  workspace_contract_digest artifact_digest_text NOT NULL,
  artifact_contract_digest artifact_digest_text NOT NULL,
  title_digest artifact_digest_text NOT NULL,
  title_classifier_policy_digest artifact_digest_text NOT NULL,
  terminal_reason text,
  UNIQUE (root_task_id, task_id),
  UNIQUE (server_id, root_task_id, task_id)
);
CREATE TABLE IF NOT EXISTS agent_capabilities_v3 (
  agent_id agent_id_text PRIMARY KEY REFERENCES agents(agent_id),
  registry_revision bigint NOT NULL CHECK (registry_revision >= 1),
  capability_keys jsonb NOT NULL CHECK (jsonb_typeof(capability_keys) = 'array'),
  current boolean NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS task_v3_edges (
  root_task_id task_id_text NOT NULL REFERENCES task_v3_graphs(root_task_id),
  prerequisite_task_id task_id_text NOT NULL REFERENCES tasks(task_id),
  dependent_task_id task_id_text NOT NULL REFERENCES tasks(task_id),
  edge_kind text NOT NULL CHECK (edge_kind IN ('contains','depends_on')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (root_task_id, prerequisite_task_id, dependent_task_id, edge_kind),
  CHECK (prerequisite_task_id <> dependent_task_id),
  FOREIGN KEY (root_task_id, prerequisite_task_id)
    REFERENCES task_v3_state(root_task_id, task_id),
  FOREIGN KEY (root_task_id, dependent_task_id)
    REFERENCES task_v3_state(root_task_id, task_id)
);
CREATE TABLE IF NOT EXISTS workspace_contracts_v3 (
  workspace_contract_id text PRIMARY KEY CHECK (workspace_contract_id COLLATE "C" ~ '^wsc_[0-9a-hjkmnp-tv-z]{26}$'),
  task_id task_id_text NOT NULL UNIQUE REFERENCES tasks(task_id),
  root_task_id task_id_text NOT NULL REFERENCES task_v3_graphs(root_task_id),
  repository_id text NOT NULL REFERENCES workspace_repositories(repository_id),
  contract_digest artifact_digest_text NOT NULL UNIQUE,
  contract_json jsonb NOT NULL CHECK (jsonb_typeof(contract_json) = 'object'),
  workspace_generation bigint NOT NULL CHECK (workspace_generation >= 1),
  execution_mode text NOT NULL CHECK (execution_mode IN ('isolated_worktree','read_only_artifact'))
);
CREATE TABLE IF NOT EXISTS workspace_reservations_v3 (
  reservation_id text PRIMARY KEY CHECK (reservation_id COLLATE "C" ~ '^rsv_[0-9a-hjkmnp-tv-z]{26}$'),
  repository_id text NOT NULL REFERENCES workspace_repositories(repository_id),
  task_id task_id_text NOT NULL REFERENCES tasks(task_id),
  claim_id claim_id_text NOT NULL,
  lease_id lease_id_text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  lease_epoch bigint NOT NULL CHECK (lease_epoch >= 1),
  path_kind text NOT NULL CHECK (path_kind IN ('file','subtree')),
  path text NOT NULL CHECK (path <> '' AND path !~ '(^/|\\\\|(^|/)\.\.?(/|$))'),
  closed_at timestamptz,
  close_reason text,
  UNIQUE (repository_id, task_id, claim_id, lease_id, path)
);
CREATE INDEX IF NOT EXISTS workspace_reservations_v3_open
  ON workspace_reservations_v3(repository_id, path) WHERE closed_at IS NULL;
CREATE TABLE IF NOT EXISTS task_claims_v3 (
  claim_id claim_id_text PRIMARY KEY,
  root_task_id task_id_text NOT NULL REFERENCES task_v3_graphs(root_task_id),
  task_id task_id_text NOT NULL REFERENCES tasks(task_id),
  lease_id lease_id_text NOT NULL,
  owner_agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  attempt integer NOT NULL CHECK (attempt >= 1),
  lease_epoch bigint NOT NULL CHECK (lease_epoch >= 1),
  lease_revision bigint NOT NULL CHECK (lease_revision >= 1),
  fence_token fence_token_text NOT NULL UNIQUE,
  acquired_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL CHECK (expires_at > acquired_at),
  task_row_version bigint NOT NULL CHECK (task_row_version >= 1),
  graph_revision bigint NOT NULL CHECK (graph_revision >= 0),
  closed_at timestamptz,
  close_reason text CHECK (close_reason IS NULL OR close_reason IN ('work_yielded','graph_expanded','graph_blocked','artifact_published','owner_stopping','reconciliation_relinquish','review_barrier_blocked','server_expired')),
  UNIQUE (task_id, attempt), UNIQUE (task_id, lease_epoch),
  UNIQUE (claim_id, task_id, lease_id, lease_epoch, fence_token)
);
CREATE UNIQUE INDEX IF NOT EXISTS task_claims_v3_one_open ON task_claims_v3(task_id) WHERE closed_at IS NULL;
CREATE TABLE IF NOT EXISTS task_v3_command_receipts (
  command_id command_id_text PRIMARY KEY,
  request_digest artifact_digest_text NOT NULL,
  command_kind text NOT NULL,
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS task_v3_coordinations (
  committed_reply_message_id message_id_text PRIMARY KEY REFERENCES messages(message_id),
  command_id command_id_text NOT NULL UNIQUE,
  root_task_id task_id_text NOT NULL,
  task_id task_id_text NOT NULL,
  source_message_id message_id_text NOT NULL,
  source_producer_fact_id producer_fact_id_text NOT NULL,
  source_turn_id turn_id_text NOT NULL,
  current_owner_agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (root_task_id, task_id)
    REFERENCES task_v3_state(root_task_id, task_id),
  FOREIGN KEY (source_message_id, source_producer_fact_id)
    REFERENCES messages(message_id, producer_fact_id)
);
CREATE TABLE IF NOT EXISTS artifact_contract_templates_v3 (
  template_digest artifact_digest_text PRIMARY KEY,
  template_id text NOT NULL UNIQUE CHECK (template_id COLLATE "C" ~ '^act_[0-9a-hjkmnp-tv-z]{26}$'),
  template_json jsonb NOT NULL CHECK (jsonb_typeof(template_json) = 'object')
);
CREATE TABLE IF NOT EXISTS artifact_contracts_v3 (
  artifact_contract_digest artifact_digest_text PRIMARY KEY,
  artifact_contract_id text NOT NULL UNIQUE CHECK (artifact_contract_id COLLATE "C" ~ '^acc_[0-9a-hjkmnp-tv-z]{26}$'),
  task_id task_id_text NOT NULL UNIQUE REFERENCES tasks(task_id),
  contract_json jsonb NOT NULL CHECK (jsonb_typeof(contract_json) = 'object')
);
CREATE TABLE IF NOT EXISTS staged_artifact_materials_v3 (
  staged_object_id text PRIMARY KEY CHECK (staged_object_id COLLATE "C" ~ '^aob_[0-9a-hjkmnp-tv-z]{26}$'),
  role text NOT NULL CHECK (role IN ('artifact','scope_manifest','acceptance_receipt')),
  material_kind text NOT NULL CHECK (material_kind IN ('prerequisite_bound_git_bundle','digest_blob','canonical_json')),
  media_type text NOT NULL, byte_length bigint NOT NULL CHECK (byte_length >= 1),
  sha256 artifact_digest_text NOT NULL, prerequisite_commit text,
  expires_at timestamptz NOT NULL, sealed boolean NOT NULL DEFAULT true,
  UNIQUE (role, sha256, byte_length)
);
CREATE TABLE IF NOT EXISTS artifacts_v3 (
  artifact_id text PRIMARY KEY CHECK (artifact_id COLLATE "C" ~ '^art_[0-9a-hjkmnp-tv-z]{26}$'),
  artifact_digest artifact_digest_text NOT NULL UNIQUE,
  root_task_id task_id_text NOT NULL REFERENCES task_v3_graphs(root_task_id),
  task_id task_id_text NOT NULL REFERENCES tasks(task_id), attempt integer NOT NULL CHECK (attempt >= 1),
  builder_agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  artifact_contract_digest artifact_digest_text NOT NULL REFERENCES artifact_contracts_v3(artifact_contract_digest),
  descriptor_json jsonb NOT NULL CHECK (jsonb_typeof(descriptor_json) = 'object'),
  material_object_id text NOT NULL REFERENCES staged_artifact_materials_v3(staged_object_id),
  scope_manifest_object_id text NOT NULL REFERENCES staged_artifact_materials_v3(staged_object_id),
  acceptance_receipt_object_id text NOT NULL REFERENCES staged_artifact_materials_v3(staged_object_id),
  published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (task_id, attempt)
);
CREATE TABLE IF NOT EXISTS task_attempt_ledgers_v3 (
  task_id task_id_text NOT NULL REFERENCES tasks(task_id), attempt integer NOT NULL CHECK (attempt >= 1),
  ledger_revision bigint NOT NULL CHECK (ledger_revision >= 1), PRIMARY KEY (task_id, attempt)
);
CREATE TABLE IF NOT EXISTS task_attempt_contributors_v3 (
  contributor_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  task_id task_id_text NOT NULL, attempt integer NOT NULL, agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  source text NOT NULL CHECK (source IN ('claim_owner','accepted_upstream_artifact')),
  source_artifact_id text REFERENCES artifacts_v3(artifact_id), recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE NULLS NOT DISTINCT (task_id, attempt, agent_id, source, source_artifact_id),
  FOREIGN KEY (task_id, attempt) REFERENCES task_attempt_ledgers_v3(task_id, attempt)
);
CREATE TABLE IF NOT EXISTS artifact_consumptions_v3 (
  consumption_id text PRIMARY KEY CHECK (consumption_id COLLATE "C" ~ '^acm_[0-9a-hjkmnp-tv-z]{26}$'),
  grant_id text NOT NULL UNIQUE CHECK (grant_id COLLATE "C" ~ '^amg_[0-9a-hjkmnp-tv-z]{26}$'),
  target_task_id task_id_text NOT NULL REFERENCES tasks(task_id), target_attempt integer NOT NULL,
  target_lease_epoch bigint NOT NULL, target_fence_token fence_token_text NOT NULL,
  source_artifact_id text NOT NULL REFERENCES artifacts_v3(artifact_id), source_artifact_digest artifact_digest_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE (target_task_id,target_attempt,source_artifact_id)
);
CREATE TABLE IF NOT EXISTS review_barriers_v3 (
  artifact_id text PRIMARY KEY REFERENCES artifacts_v3(artifact_id),
  barrier_revision bigint NOT NULL DEFAULT 1 CHECK (barrier_revision >= 1),
  state text NOT NULL CHECK (state IN ('open','satisfied','blocked')),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS review_requirements_v3 (
  artifact_id text NOT NULL REFERENCES review_barriers_v3(artifact_id), seat_key text NOT NULL,
  artifact_digest artifact_digest_text NOT NULL, artifact_contract_digest artifact_digest_text NOT NULL,
  gate3_plan_digest artifact_digest_text NOT NULL, scenario_version integer NOT NULL CHECK (scenario_version >= 1),
  review_task_id task_id_text NOT NULL UNIQUE REFERENCES tasks(task_id), PRIMARY KEY (artifact_id, seat_key)
);
CREATE TABLE IF NOT EXISTS review_assignments_v3 (
  assignment_id text PRIMARY KEY CHECK (assignment_id COLLATE "C" ~ '^ras_[0-9a-hjkmnp-tv-z]{26}$'),
  artifact_id text NOT NULL, seat_key text NOT NULL, review_task_id task_id_text NOT NULL,
  reviewer_agent_id agent_id_text NOT NULL REFERENCES agents(agent_id), reviewer_registry_revision bigint NOT NULL,
  assignment_revision bigint NOT NULL CHECK (assignment_revision >= 1), assignment_status text NOT NULL CHECK (assignment_status IN ('current','closed')),
  closed_reason text CHECK (closed_reason IS NULL OR closed_reason IN ('reassigned','terminal_verdict','barrier_blocked')),
  FOREIGN KEY (artifact_id,seat_key) REFERENCES review_requirements_v3(artifact_id,seat_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS review_assignments_v3_current_seat ON review_assignments_v3(artifact_id,seat_key) WHERE assignment_status='current';
CREATE UNIQUE INDEX IF NOT EXISTS review_assignments_v3_current_reviewer ON review_assignments_v3(artifact_id,reviewer_agent_id) WHERE assignment_status='current';
CREATE TABLE IF NOT EXISTS review_verdicts_v3 (
  assignment_id text PRIMARY KEY REFERENCES review_assignments_v3(assignment_id),
  artifact_id text NOT NULL REFERENCES review_barriers_v3(artifact_id), seat_key text NOT NULL,
  review_attempt integer NOT NULL CHECK (review_attempt >= 1), verdict text NOT NULL CHECK (verdict IN ('GO','BLOCK')),
  findings_digest artifact_digest_text NOT NULL, evidence_receipt_digest artifact_digest_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
`;

export const WAVE3_POSTGRES_MIGRATION_CHECKSUM = checksumMigration(WAVE3_POSTGRES_MIGRATION);

export async function readMigrations(directory: URL): Promise<Migration[]> {
  const names = (await readdir(directory))
    .filter((name) => /^\d{4}_[a-z0-9_]+\.up\.sql$/u.test(name))
    .sort();
  const migrations: Migration[] = [];
  for (const name of names) {
    const sql = await readFile(new URL(name, directory), "utf8");
    migrations.push({
      version: name.slice(0, 4),
      checksum: checksumMigration(sql),
      sql,
    });
  }
  return migrations;
}

export function locateMigrationDirectory(
  dialect: "postgres" | "sqlite",
  moduleUrl: string,
): URL {
  const candidates = [
    new URL(`../../migrations/${dialect}/`, moduleUrl),
    new URL(`../../../migrations/${dialect}/`, moduleUrl),
  ];
  const found = candidates.find((candidate) => existsSync(candidate));
  if (found === undefined) throw new Error(`missing ${dialect} migration directory`);
  return found;
}
