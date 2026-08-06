DO $$
BEGIN
  IF current_setting('server_version_num')::integer < 150000 THEN
    RAISE EXCEPTION 'PostgreSQL 15 or newer is required' USING ERRCODE = 'feature_not_supported';
  END IF;
END
$$;

CREATE DOMAIN server_id_text AS text CHECK (VALUE COLLATE "C" ~ '^srv_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN machine_id_text AS text CHECK (VALUE COLLATE "C" ~ '^mch_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN agent_id_text AS text CHECK (VALUE COLLATE "C" ~ '^agt_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN channel_id_text AS text CHECK (VALUE COLLATE "C" ~ '^chn_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN conversation_id_text AS text CHECK (VALUE COLLATE "C" ~ '^cvs_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN message_id_text AS text CHECK (VALUE COLLATE "C" ~ '^msg_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN delivery_id_text AS text CHECK (VALUE COLLATE "C" ~ '^dlv_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN producer_fact_id_text AS text CHECK (VALUE COLLATE "C" ~ '^fac_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN task_id_text AS text CHECK (VALUE COLLATE "C" ~ '^tsk_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN claim_id_text AS text CHECK (VALUE COLLATE "C" ~ '^clm_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN lease_id_text AS text CHECK (VALUE COLLATE "C" ~ '^lse_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN launch_id_text AS text CHECK (VALUE COLLATE "C" ~ '^lnc_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN command_id_text AS text CHECK (VALUE COLLATE "C" ~ '^cmd_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN receipt_id_text AS text CHECK (VALUE COLLATE "C" ~ '^rcp_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN state_instance_id_text AS text CHECK (VALUE COLLATE "C" ~ '^sti_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN turn_id_text AS text CHECK (VALUE COLLATE "C" ~ '^trn_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN session_id_text AS text CHECK (VALUE COLLATE "C" ~ '^ses_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN fence_token_text AS text CHECK (VALUE COLLATE "C" ~ '^fnc_[0-9a-hjkmnp-tv-z]{26}$');
CREATE DOMAIN artifact_digest_text AS text CHECK (VALUE COLLATE "C" ~ '^sha256:[0-9a-f]{64}$');

CREATE FUNCTION strict_actor_ok(kind text, identifier text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE kind
    WHEN 'server' THEN identifier COLLATE "C" ~ '^srv_[0-9a-hjkmnp-tv-z]{26}$'
    WHEN 'machine' THEN identifier COLLATE "C" ~ '^mch_[0-9a-hjkmnp-tv-z]{26}$'
    WHEN 'agent' THEN identifier COLLATE "C" ~ '^agt_[0-9a-hjkmnp-tv-z]{26}$'
    ELSE false
  END
$$;

CREATE FUNCTION strict_target_ok(kind text, identifier text, thread_root text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE kind
    WHEN 'channel' THEN identifier COLLATE "C" ~ '^chn_[0-9a-hjkmnp-tv-z]{26}$'
    WHEN 'direct' THEN identifier COLLATE "C" ~ '^cvs_[0-9a-hjkmnp-tv-z]{26}$'
    ELSE false
  END
  AND (thread_root IS NULL OR thread_root COLLATE "C" ~ '^msg_[0-9a-hjkmnp-tv-z]{26}$')
$$;

CREATE FUNCTION protocol_envelope_ok(value jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT jsonb_typeof(value) = 'object'
    AND jsonb_typeof(value -> 'protocolVersion') = 'number'
    AND value ->> 'protocolVersion' ~ '^[1-9][0-9]{0,5}$'
    AND (value ->> 'protocolVersion')::integer <= 999999
$$;

CREATE TABLE servers (
  server_id server_id_text PRIMARY KEY,
  display_name text NOT NULL CHECK (btrim(display_name) <> ''),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE machines (
  machine_id machine_id_text PRIMARY KEY,
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  configuration jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(configuration) = 'object'),
  machine_generation bigint NOT NULL DEFAULT 0 CHECK (machine_generation >= 0),
  last_seen_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (server_id, machine_id)
);

CREATE TABLE agents (
  agent_id agent_id_text PRIMARY KEY,
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  configuration jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(configuration) = 'object'),
  routing_generation bigint NOT NULL DEFAULT 0 CHECK (routing_generation >= 0),
  last_activity_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (server_id, agent_id)
);

CREATE TABLE channels (
  channel_id channel_id_text PRIMARY KEY,
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  visibility text NOT NULL CHECK (visibility IN ('public', 'private')),
  name text NOT NULL CHECK (btrim(name) <> ''),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (server_id, channel_id)
);

CREATE TABLE memberships (
  channel_id channel_id_text NOT NULL REFERENCES channels(channel_id),
  actor_kind text NOT NULL,
  actor_id text NOT NULL,
  state text NOT NULL CHECK (state IN ('active', 'removed')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (channel_id, actor_kind, actor_id),
  CHECK (strict_actor_ok(actor_kind, actor_id))
);

CREATE TABLE command_requests (
  actor_kind text NOT NULL,
  actor_id text NOT NULL,
  scope text NOT NULL CHECK (scope IN (
    'message.append.v1', 'claim.mutate.v1', 'task_graph.mutate.v1',
    'reminder.mutate.v1', 'launch.mutate.v1', 'delivery.mutate.v1',
    'receipt.record.v1', 'artifact.mutate.v1', 'outbox.mutate.v1'
  )),
  request_kind text NOT NULL CHECK (request_kind IN ('command', 'producer_fact')),
  request_id text NOT NULL,
  request_digest artifact_digest_text NOT NULL,
  result_json jsonb NOT NULL CHECK (protocol_envelope_ok(result_json)),
  result_digest artifact_digest_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (actor_kind, actor_id, scope, request_kind, request_id),
  CHECK (strict_actor_ok(actor_kind, actor_id)),
  CHECK (
    (request_kind = 'command' AND request_id COLLATE "C" ~ '^cmd_[0-9a-hjkmnp-tv-z]{26}$') OR
    (request_kind = 'producer_fact' AND request_id COLLATE "C" ~ '^fac_[0-9a-hjkmnp-tv-z]{26}$')
  )
);

CREATE TABLE target_sequences (
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  next_seq bigint NOT NULL CHECK (next_seq >= 1),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  UNIQUE NULLS NOT DISTINCT (target_kind, target_id, thread_root_message_id)
);

CREATE TABLE messages (
  message_id message_id_text PRIMARY KEY,
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  author_kind text NOT NULL,
  author_id text NOT NULL,
  target_seq bigint NOT NULL CHECK (target_seq >= 1),
  body text NOT NULL CHECK (btrim(body) <> ''),
  parent_message_id message_id_text,
  producer_fact_id producer_fact_id_text NOT NULL UNIQUE,
  payload_digest artifact_digest_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  CHECK (strict_actor_ok(author_kind, author_id)),
  UNIQUE NULLS NOT DISTINCT (target_kind, target_id, thread_root_message_id, target_seq)
);

CREATE TABLE agent_message_state (
  state_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  pending_server_seq bigint CHECK (pending_server_seq >= 1),
  consumed_server_seq bigint CHECK (consumed_server_seq >= 1),
  row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  CHECK (consumed_server_seq IS NULL OR pending_server_seq IS NULL OR consumed_server_seq <= pending_server_seq),
  UNIQUE NULLS NOT DISTINCT (agent_id, target_kind, target_id, thread_root_message_id)
);

CREATE TABLE tasks (
  task_id task_id_text PRIMARY KEY,
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  task_number bigint NOT NULL CHECK (task_number >= 1),
  source_message_id message_id_text REFERENCES messages(message_id),
  status text NOT NULL CHECK (status IN ('todo', 'in_progress', 'in_review', 'done')),
  assignee_agent_id agent_id_text REFERENCES agents(agent_id),
  row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (server_id, task_number),
  UNIQUE (server_id, task_id)
);

CREATE TABLE task_graphs (
  server_id server_id_text PRIMARY KEY REFERENCES servers(server_id),
  graph_revision bigint NOT NULL DEFAULT 0 CHECK (graph_revision >= 0)
);

CREATE TABLE task_edges (
  server_id server_id_text NOT NULL,
  parent_task_id task_id_text NOT NULL,
  child_task_id task_id_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (server_id, parent_task_id, child_task_id),
  FOREIGN KEY (server_id, parent_task_id) REFERENCES tasks(server_id, task_id),
  FOREIGN KEY (server_id, child_task_id) REFERENCES tasks(server_id, task_id),
  CHECK (parent_task_id <> child_task_id)
);

CREATE TABLE task_claims (
  claim_id claim_id_text PRIMARY KEY,
  task_id task_id_text NOT NULL REFERENCES tasks(task_id),
  lease_id lease_id_text NOT NULL,
  lease_epoch bigint NOT NULL CHECK (lease_epoch >= 1),
  fence_token fence_token_text NOT NULL UNIQUE,
  attempt integer NOT NULL CHECK (attempt >= 1),
  owner_agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  expires_at timestamptz NOT NULL CHECK (expires_at > acquired_at),
  released_at timestamptz,
  terminal_reason text,
  UNIQUE (task_id, lease_epoch),
  UNIQUE (claim_id, task_id, lease_id, lease_epoch, fence_token)
);
CREATE UNIQUE INDEX task_claims_one_open ON task_claims(task_id) WHERE released_at IS NULL;

CREATE TABLE reminders (
  reminder_id command_id_text NOT NULL,
  owner_kind text NOT NULL,
  owner_id text NOT NULL,
  anchor_json jsonb NOT NULL CHECK (jsonb_typeof(anchor_json) = 'object'),
  schedule_json jsonb NOT NULL CHECK (protocol_envelope_ok(schedule_json)),
  generation bigint NOT NULL CHECK (generation >= 1),
  next_fire_at timestamptz,
  canceled_at timestamptz,
  worker_lease_id lease_id_text,
  worker_lease_until timestamptz,
  PRIMARY KEY (reminder_id, generation),
  CHECK (strict_actor_ok(owner_kind, owner_id))
);

CREATE TABLE agent_launches (
  launch_id launch_id_text PRIMARY KEY,
  machine_id machine_id_text NOT NULL REFERENCES machines(machine_id),
  agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  runtime_kind text NOT NULL CHECK (runtime_kind IN ('codex', 'claude')),
  workspace_generation bigint NOT NULL CHECK (workspace_generation >= 1),
  routing_generation bigint NOT NULL CHECK (routing_generation >= 0),
  state text NOT NULL CHECK (state IN ('requested', 'ready', 'activated', 'terminal')),
  requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  ready_at timestamptz,
  activated_at timestamptz,
  terminal_at timestamptz,
  terminal_reason text,
  UNIQUE (launch_id, machine_id, agent_id),
  CHECK (ready_at IS NULL OR state IN ('ready', 'activated', 'terminal')),
  CHECK (activated_at IS NULL OR state IN ('activated', 'terminal')),
  CHECK (terminal_at IS NULL OR state = 'terminal')
);
CREATE UNIQUE INDEX agent_launches_one_active_slot
  ON agent_launches(machine_id, agent_id)
  WHERE state IN ('requested', 'ready', 'activated');

CREATE TABLE deliveries (
  delivery_id delivery_id_text PRIMARY KEY,
  attempt integer NOT NULL CHECK (attempt >= 1),
  message_id message_id_text NOT NULL REFERENCES messages(message_id),
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  target_seq bigint NOT NULL CHECK (target_seq >= 1),
  producer_fact_id producer_fact_id_text NOT NULL REFERENCES messages(producer_fact_id),
  agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  machine_id machine_id_text NOT NULL REFERENCES machines(machine_id),
  expected_launch_id launch_id_text REFERENCES agent_launches(launch_id),
  replay_of delivery_id_text,
  status text NOT NULL CHECK (status IN ('pending', 'leased', 'daemon_accepted', 'input_written', 'model_visible', 'acked', 'canceled', 'dead')),
  worker_lease_id lease_id_text,
  worker_lease_until timestamptz,
  daemon_accepted_at timestamptz,
  input_written_at timestamptz,
  model_visible_at timestamptz,
  acked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  CHECK ((attempt = 1 AND replay_of IS NULL) OR (attempt > 1 AND replay_of IS NOT NULL AND replay_of <> delivery_id)),
  CHECK (input_written_at IS NULL OR daemon_accepted_at IS NOT NULL),
  CHECK (model_visible_at IS NULL OR input_written_at IS NOT NULL),
  CHECK (acked_at IS NULL OR model_visible_at IS NOT NULL),
  UNIQUE (agent_id, producer_fact_id, attempt),
  UNIQUE (delivery_id, agent_id, producer_fact_id),
  FOREIGN KEY (replay_of, agent_id, producer_fact_id)
    REFERENCES deliveries(delivery_id, agent_id, producer_fact_id),
  FOREIGN KEY (expected_launch_id, machine_id, agent_id)
    REFERENCES agent_launches(launch_id, machine_id, agent_id)
);

CREATE TABLE receipts (
  receipt_id receipt_id_text PRIMARY KEY,
  producer_fact_id producer_fact_id_text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('server_accepted', 'claim_won', 'daemon_accepted', 'process_spawned', 'runtime_ready', 'input_written', 'model_visible', 'side_effect_applied', 'artifact_published', 'review_verdict')),
  actor_server_id server_id_text REFERENCES servers(server_id),
  actor_machine_id machine_id_text REFERENCES machines(machine_id),
  actor_agent_id agent_id_text REFERENCES agents(agent_id),
  lease_epoch bigint CHECK (lease_epoch >= 1),
  fence_token fence_token_text,
  launch_id launch_id_text REFERENCES agent_launches(launch_id),
  state_instance_id state_instance_id_text,
  turn_id turn_id_text,
  session_id session_id_text,
  artifact_digest artifact_digest_text,
  occurred_at timestamptz NOT NULL,
  detail_json jsonb NOT NULL CHECK (protocol_envelope_ok(detail_json)),
  receipt_digest artifact_digest_text NOT NULL,
  CHECK (
    (kind = 'server_accepted' AND actor_server_id IS NOT NULL AND actor_machine_id IS NULL AND actor_agent_id IS NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
    (kind = 'claim_won' AND actor_server_id IS NOT NULL AND actor_machine_id IS NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NOT NULL AND fence_token IS NOT NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
    (kind = 'daemon_accepted' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
    (kind = 'process_spawned' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
    (kind = 'runtime_ready' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NULL AND session_id IS NOT NULL AND artifact_digest IS NULL) OR
    (kind IN ('input_written', 'model_visible') AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NOT NULL AND session_id IS NOT NULL AND artifact_digest IS NULL) OR
    (kind = 'side_effect_applied' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND ((lease_epoch IS NULL AND fence_token IS NULL) OR (lease_epoch IS NOT NULL AND fence_token IS NOT NULL)) AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NOT NULL AND session_id IS NOT NULL AND artifact_digest IS NULL) OR
    (kind IN ('artifact_published', 'review_verdict') AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NOT NULL AND fence_token IS NOT NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NOT NULL AND session_id IS NOT NULL AND artifact_digest IS NOT NULL)
  ),
  UNIQUE NULLS NOT DISTINCT (
    producer_fact_id, kind, actor_server_id, actor_machine_id, actor_agent_id,
    lease_epoch, fence_token, launch_id, state_instance_id, turn_id, session_id,
    artifact_digest
  ),
  FOREIGN KEY (launch_id, actor_machine_id, actor_agent_id)
    REFERENCES agent_launches(launch_id, machine_id, agent_id)
);

CREATE TABLE artifacts (
  artifact_digest artifact_digest_text PRIMARY KEY,
  commit_sha text CHECK (commit_sha IS NULL OR commit_sha COLLATE "C" ~ '^[0-9a-f]{40}$'),
  task_id task_id_text NOT NULL REFERENCES tasks(task_id),
  claim_id claim_id_text NOT NULL REFERENCES task_claims(claim_id),
  lease_id lease_id_text NOT NULL,
  lease_epoch bigint NOT NULL CHECK (lease_epoch >= 1),
  fence_token fence_token_text NOT NULL,
  published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (claim_id, task_id, lease_id, lease_epoch, fence_token)
    REFERENCES task_claims(claim_id, task_id, lease_id, lease_epoch, fence_token)
);
CREATE UNIQUE INDEX artifacts_commit_sha_unique
  ON artifacts(commit_sha) WHERE commit_sha IS NOT NULL;

CREATE TABLE reviews (
  review_id receipt_id_text PRIMARY KEY REFERENCES receipts(receipt_id),
  artifact_digest artifact_digest_text NOT NULL REFERENCES artifacts(artifact_digest),
  reviewer_agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  reviewer_seat text NOT NULL CHECK (btrim(reviewer_seat) <> ''),
  verdict text NOT NULL CHECK (verdict IN ('go', 'block')),
  scenario_version integer NOT NULL CHECK (scenario_version >= 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (artifact_digest, reviewer_agent_id, reviewer_seat, scenario_version)
);

CREATE TABLE outbox_jobs (
  job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  idempotency_namespace text NOT NULL CHECK (idempotency_namespace IN ('message_delivery.v1', 'receipt_delivery.v1', 'reminder_fire.v1', 'artifact_publication.v1')),
  idempotency_key text NOT NULL CHECK (btrim(idempotency_key) <> ''),
  producer_fact_id producer_fact_id_text NOT NULL,
  event_kind text NOT NULL CHECK (btrim(event_kind) <> ''),
  event_version integer NOT NULL CHECK (event_version >= 1),
  payload_json jsonb NOT NULL CHECK (protocol_envelope_ok(payload_json)),
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'leased', 'acked', 'dead')),
  due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  worker_lease_id lease_id_text,
  worker_lease_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (idempotency_namespace, idempotency_key),
  UNIQUE (producer_fact_id, event_kind, event_version),
  CHECK (idempotency_namespace = event_kind || '.v' || event_version::text),
  CHECK ((status = 'leased') = (worker_lease_id IS NOT NULL AND worker_lease_until IS NOT NULL))
);

CREATE INDEX outbox_jobs_due ON outbox_jobs(due_at, job_id) WHERE status = 'pending';
