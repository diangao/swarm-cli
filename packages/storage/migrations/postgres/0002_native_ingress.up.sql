DO $$
BEGIN
  IF current_setting('server_version_num')::integer < 160000 THEN
    RAISE EXCEPTION 'PostgreSQL 16 or newer is required' USING ERRCODE = 'feature_not_supported';
  END IF;
END
$$;

CREATE DOMAIN human_id_text AS text
  CHECK (VALUE COLLATE "C" ~ '^hum_[0-9a-hjkmnp-tv-z]{26}$');

CREATE OR REPLACE FUNCTION strict_actor_ok(kind text, identifier text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE kind
    WHEN 'server' THEN identifier COLLATE "C" ~ '^srv_[0-9a-hjkmnp-tv-z]{26}$'
    WHEN 'machine' THEN identifier COLLATE "C" ~ '^mch_[0-9a-hjkmnp-tv-z]{26}$'
    WHEN 'agent' THEN identifier COLLATE "C" ~ '^agt_[0-9a-hjkmnp-tv-z]{26}$'
    WHEN 'human' THEN identifier COLLATE "C" ~ '^hum_[0-9a-hjkmnp-tv-z]{26}$'
    ELSE false
  END
$$;

CREATE FUNCTION message_body_has_content(value text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT length(btrim(value, U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF')) > 0
$$;

CREATE TABLE humans (
  human_id human_id_text PRIMARY KEY,
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  display_name text NOT NULL CHECK (message_body_has_content(display_name)),
  row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (server_id, human_id)
);

CREATE TABLE conversations (
  conversation_id conversation_id_text PRIMARY KEY,
  server_id server_id_text NOT NULL REFERENCES servers(server_id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (server_id, conversation_id)
);

ALTER TABLE memberships
  ADD COLUMN membership_epoch bigint NOT NULL DEFAULT 1 CHECK (membership_epoch >= 1),
  ADD COLUMN row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  ADD COLUMN updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  ADD CONSTRAINT memberships_supported_actor CHECK (actor_kind IN ('human', 'agent'));

CREATE TABLE conversation_memberships (
  conversation_id conversation_id_text NOT NULL REFERENCES conversations(conversation_id),
  actor_kind text NOT NULL CHECK (actor_kind IN ('human', 'agent')),
  actor_id text NOT NULL,
  state text NOT NULL CHECK (state IN ('active', 'removed')),
  membership_epoch bigint NOT NULL CHECK (membership_epoch >= 1),
  row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (conversation_id, actor_kind, actor_id),
  CHECK (strict_actor_ok(actor_kind, actor_id))
);

ALTER TABLE agents
  ADD COLUMN presence text NOT NULL DEFAULT 'offline'
    CHECK (presence IN ('offline', 'online', 'busy')),
  ADD COLUMN presence_updated_at timestamptz,
  ADD COLUMN last_turn_at timestamptz,
  ADD COLUMN row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0);

CREATE TABLE target_owner_routes (
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  machine_id machine_id_text NOT NULL REFERENCES machines(machine_id),
  expected_launch_id launch_id_text NOT NULL,
  membership_epoch bigint NOT NULL CHECK (membership_epoch >= 1),
  routing_generation bigint NOT NULL CHECK (routing_generation >= 0),
  route_version bigint NOT NULL CHECK (route_version >= 1),
  row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  UNIQUE NULLS NOT DISTINCT (target_kind, target_id, thread_root_message_id),
  FOREIGN KEY (expected_launch_id, machine_id, agent_id)
    REFERENCES agent_launches(launch_id, machine_id, agent_id)
);

CREATE FUNCTION validate_target_owner_route() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  member_current boolean;
  launch_current boolean;
BEGIN
  IF NEW.target_kind = 'channel' THEN
    SELECT EXISTS (
      SELECT 1 FROM memberships m
      WHERE m.channel_id::text = NEW.target_id AND m.actor_kind = 'agent'
        AND m.actor_id = NEW.agent_id::text AND m.state = 'active'
        AND m.membership_epoch = NEW.membership_epoch
    ) INTO member_current;
  ELSE
    SELECT EXISTS (
      SELECT 1 FROM conversation_memberships m
      WHERE m.conversation_id::text = NEW.target_id AND m.actor_kind = 'agent'
        AND m.actor_id = NEW.agent_id::text AND m.state = 'active'
        AND m.membership_epoch = NEW.membership_epoch
    ) INTO member_current;
  END IF;
  SELECT EXISTS (
    SELECT 1 FROM agent_launches l
    WHERE l.launch_id = NEW.expected_launch_id AND l.machine_id = NEW.machine_id
      AND l.agent_id = NEW.agent_id AND l.state = 'activated'
      AND l.routing_generation = NEW.routing_generation
  ) INTO launch_current;
  IF NOT member_current OR NOT launch_current THEN
    RAISE EXCEPTION 'owner route authority mismatch' USING ERRCODE = 'foreign_key_violation';
  END IF;
  RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER target_owner_routes_authority_match
AFTER INSERT OR UPDATE ON target_owner_routes
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_target_owner_route();

ALTER TABLE messages
  ADD COLUMN caused_by_producer_fact_id producer_fact_id_text,
  ADD CONSTRAINT messages_body_content CHECK (message_body_has_content(body)),
  ADD CONSTRAINT messages_identity_pair UNIQUE (message_id, producer_fact_id),
  ADD CONSTRAINT messages_parent_fk FOREIGN KEY (parent_message_id)
    REFERENCES messages(message_id) DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT messages_thread_root_fk FOREIGN KEY (thread_root_message_id)
    REFERENCES messages(message_id) DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT messages_cause_fk FOREIGN KEY (caused_by_producer_fact_id)
    REFERENCES messages(producer_fact_id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE message_audience (
  message_id message_id_text NOT NULL REFERENCES messages(message_id) ON DELETE RESTRICT,
  actor_kind text NOT NULL CHECK (actor_kind IN ('human', 'agent')),
  actor_id text NOT NULL,
  membership_epoch bigint NOT NULL CHECK (membership_epoch >= 1),
  audience_mode text NOT NULL CHECK (audience_mode IN ('owner_body', 'member_body', 'attention_metadata')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (message_id, actor_kind, actor_id),
  CHECK (strict_actor_ok(actor_kind, actor_id)),
  CHECK (
    (actor_kind = 'human' AND audience_mode = 'member_body') OR
    (actor_kind = 'agent' AND audience_mode IN ('owner_body', 'attention_metadata'))
  )
);

CREATE TABLE message_owner_routes (
  producer_fact_id producer_fact_id_text PRIMARY KEY REFERENCES messages(producer_fact_id),
  message_id message_id_text NOT NULL,
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  agent_id agent_id_text NOT NULL REFERENCES agents(agent_id),
  machine_id machine_id_text NOT NULL REFERENCES machines(machine_id),
  expected_launch_id launch_id_text NOT NULL,
  membership_epoch bigint NOT NULL CHECK (membership_epoch >= 1),
  routing_generation bigint NOT NULL CHECK (routing_generation >= 0),
  route_version bigint NOT NULL CHECK (route_version >= 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  UNIQUE (producer_fact_id, message_id),
  FOREIGN KEY (message_id, producer_fact_id)
    REFERENCES messages(message_id, producer_fact_id),
  FOREIGN KEY (expected_launch_id, machine_id, agent_id)
    REFERENCES agent_launches(launch_id, machine_id, agent_id)
);

CREATE FUNCTION enforce_message_causality() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  parent_row messages%ROWTYPE;
  root_row messages%ROWTYPE;
BEGIN
  IF NEW.author_kind = 'human' AND (NEW.parent_message_id IS NOT NULL OR NEW.caused_by_producer_fact_id IS NOT NULL) THEN
    RAISE EXCEPTION 'human message cannot carry agent causality' USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.author_kind = 'agent' AND (NEW.parent_message_id IS NULL OR NEW.caused_by_producer_fact_id IS NULL) THEN
    RAISE EXCEPTION 'agent reply requires parent and cause' USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.parent_message_id IS NOT NULL THEN
    SELECT * INTO parent_row FROM messages WHERE message_id = NEW.parent_message_id;
    IF NOT FOUND OR parent_row.producer_fact_id <> NEW.caused_by_producer_fact_id OR
       parent_row.target_kind <> NEW.target_kind OR parent_row.target_id <> NEW.target_id OR
       parent_row.thread_root_message_id IS DISTINCT FROM NEW.thread_root_message_id THEN
      RAISE EXCEPTION 'reply parent/cause/target mismatch' USING ERRCODE = 'foreign_key_violation';
    END IF;
  END IF;
  IF NEW.thread_root_message_id IS NOT NULL THEN
    SELECT * INTO root_row FROM messages WHERE message_id = NEW.thread_root_message_id;
    IF NOT FOUND OR root_row.thread_root_message_id IS NOT NULL OR
       root_row.target_kind <> NEW.target_kind OR root_row.target_id <> NEW.target_id THEN
      RAISE EXCEPTION 'invalid thread root' USING ERRCODE = 'foreign_key_violation';
    END IF;
  END IF;
  RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER messages_causality_deferred
AFTER INSERT OR UPDATE ON messages
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_message_causality();

ALTER TABLE tasks
  ADD COLUMN title text NOT NULL DEFAULT 'legacy task' CHECK (message_body_has_content(title)),
  ADD COLUMN source_producer_fact_id producer_fact_id_text,
  ADD CONSTRAINT tasks_source_pair_presence CHECK (
    (source_message_id IS NULL) = (source_producer_fact_id IS NULL)
  ),
  ADD CONSTRAINT tasks_source_pair_fk FOREIGN KEY (source_message_id, source_producer_fact_id)
    REFERENCES messages(message_id, producer_fact_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE outbox_jobs DROP CONSTRAINT outbox_jobs_idempotency_namespace_check;
ALTER TABLE outbox_jobs DROP CONSTRAINT outbox_jobs_status_check;
ALTER TABLE outbox_jobs
  ADD COLUMN hold_reason text,
  ADD CONSTRAINT outbox_jobs_namespace_v1 CHECK (idempotency_namespace IN (
    'message_delivery.v1', 'client_message.v1', 'receipt_delivery.v1',
    'reminder_fire.v1', 'artifact_publication.v1'
  )),
  ADD CONSTRAINT outbox_jobs_status_v1 CHECK (status IN ('pending', 'leased', 'held', 'acked', 'dead')),
  ADD CONSTRAINT outbox_jobs_hold_reason_v1 CHECK (
    (status = 'held' AND hold_reason IS NOT NULL AND message_body_has_content(hold_reason)) OR
    (status <> 'held' AND hold_reason IS NULL)
  ),
  ADD CONSTRAINT outbox_jobs_unleased_terminal_v1 CHECK (
    status NOT IN ('held', 'dead') OR (worker_lease_id IS NULL AND worker_lease_until IS NULL)
  );

CREATE FUNCTION guard_outbox_immutable_fields() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(
    NEW.idempotency_namespace, NEW.idempotency_key, NEW.producer_fact_id,
    NEW.event_kind, NEW.event_version, NEW.payload_json, NEW.created_at
  ) IS DISTINCT FROM ROW(
    OLD.idempotency_namespace, OLD.idempotency_key, OLD.producer_fact_id,
    OLD.event_kind, OLD.event_version, OLD.payload_json, OLD.created_at
  ) THEN
    RAISE EXCEPTION 'immutable outbox identity or payload changed' USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER outbox_immutable_fields
BEFORE UPDATE ON outbox_jobs
FOR EACH ROW EXECUTE FUNCTION guard_outbox_immutable_fields();

ALTER TABLE deliveries
  ADD COLUMN outbox_job_id bigint REFERENCES outbox_jobs(job_id),
  ADD COLUMN membership_epoch bigint NOT NULL DEFAULT 1 CHECK (membership_epoch >= 1),
  ADD COLUMN routing_generation bigint NOT NULL DEFAULT 0 CHECK (routing_generation >= 0),
  ADD COLUMN route_version bigint NOT NULL DEFAULT 1 CHECK (route_version >= 1),
  ADD COLUMN consume_permit_id command_id_text,
  ADD COLUMN consume_permitted_at timestamptz,
  ADD COLUMN active_invocation_generation integer CHECK (active_invocation_generation >= 1),
  ADD CONSTRAINT deliveries_attempt_target UNIQUE (delivery_id, attempt),
  ADD CONSTRAINT deliveries_permit_target UNIQUE (delivery_id, attempt, consume_permit_id),
  ADD CONSTRAINT deliveries_permit_shape CHECK (
    (consume_permit_id IS NULL) = (consume_permitted_at IS NULL) AND
    (consume_permit_id IS NULL) = (active_invocation_generation IS NULL)
  );

CREATE TABLE delivery_invocations (
  delivery_id delivery_id_text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  invocation_generation integer NOT NULL CHECK (invocation_generation >= 1),
  invocation_id command_id_text NOT NULL UNIQUE,
  permit_id command_id_text NOT NULL,
  previous_invocation_generation integer,
  created_from_proof_digest artifact_digest_text,
  status text NOT NULL CHECK (status IN (
    'authorized', 'write_started', 'not_written', 'input_written', 'model_visible', 'ambiguous'
  )),
  begin_command_id command_id_text UNIQUE,
  begin_request_digest artifact_digest_text,
  input_digest artifact_digest_text,
  write_started_entry_id command_id_text,
  write_started_entry_digest artifact_digest_text,
  begin_result_json jsonb,
  not_written_proof_digest artifact_digest_text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (delivery_id, attempt, invocation_generation),
  UNIQUE (delivery_id, attempt, permit_id, invocation_generation, invocation_id),
  FOREIGN KEY (delivery_id, attempt, permit_id)
    REFERENCES deliveries(delivery_id, attempt, consume_permit_id),
  FOREIGN KEY (delivery_id, attempt, previous_invocation_generation)
    REFERENCES delivery_invocations(delivery_id, attempt, invocation_generation)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (
    (invocation_generation = 1 AND previous_invocation_generation IS NULL AND created_from_proof_digest IS NULL) OR
    (invocation_generation > 1 AND previous_invocation_generation = invocation_generation - 1 AND created_from_proof_digest IS NOT NULL)
  ),
  CHECK (
    (begin_command_id IS NULL AND begin_request_digest IS NULL AND input_digest IS NULL AND
      write_started_entry_id IS NULL AND write_started_entry_digest IS NULL AND begin_result_json IS NULL) OR
    (begin_command_id IS NOT NULL AND begin_request_digest IS NOT NULL AND input_digest IS NOT NULL AND
      write_started_entry_id IS NOT NULL AND write_started_entry_digest IS NOT NULL AND
      begin_result_json IS NOT NULL AND jsonb_typeof(begin_result_json) = 'object')
  ),
  CHECK (not_written_proof_digest IS NULL OR status = 'not_written')
);

ALTER TABLE deliveries
  ADD CONSTRAINT deliveries_active_invocation_fk
  FOREIGN KEY (delivery_id, attempt, active_invocation_generation)
  REFERENCES delivery_invocations(delivery_id, attempt, invocation_generation)
  DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE delivery_permit_commands (
  command_id command_id_text PRIMARY KEY,
  request_digest artifact_digest_text NOT NULL,
  command_kind text NOT NULL CHECK (command_kind IN ('acquire', 'resume_same', 'resume_next')),
  delivery_id delivery_id_text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  permit_id command_id_text NOT NULL,
  expected_invocation_generation integer NOT NULL CHECK (expected_invocation_generation >= 0),
  result_invocation_generation integer NOT NULL CHECK (result_invocation_generation >= 1),
  result_invocation_id command_id_text NOT NULL,
  result_json_without_body jsonb NOT NULL CHECK (jsonb_typeof(result_json_without_body) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (delivery_id, attempt, permit_id)
    REFERENCES deliveries(delivery_id, attempt, consume_permit_id),
  FOREIGN KEY (delivery_id, attempt, permit_id, result_invocation_generation, result_invocation_id)
    REFERENCES delivery_invocations(delivery_id, attempt, permit_id, invocation_generation, invocation_id),
  CHECK (
    (command_kind = 'acquire' AND expected_invocation_generation = 0 AND result_invocation_generation = 1) OR
    (command_kind = 'resume_same' AND expected_invocation_generation = result_invocation_generation) OR
    (command_kind = 'resume_next' AND result_invocation_generation = expected_invocation_generation + 1)
  )
);

CREATE UNIQUE INDEX delivery_permit_commands_generation_identity
  ON delivery_permit_commands(delivery_id, attempt, command_kind, expected_invocation_generation)
  WHERE command_kind IN ('acquire', 'resume_next');

CREATE TABLE delivery_reconciliation_commands (
  command_id command_id_text PRIMARY KEY,
  request_digest artifact_digest_text NOT NULL,
  delivery_id delivery_id_text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  permit_id command_id_text,
  invocation_generation integer,
  invocation_id command_id_text,
  evidence_kind text NOT NULL CHECK (evidence_kind IN (
    'pre_permit_disconnect', 'permit_recorded_write_not_started', 'scripted_not_written',
    'write_started_ambiguous', 'input_written', 'model_visibility_ambiguous', 'model_visible'
  )),
  evidence_digest artifact_digest_text NOT NULL,
  result_json jsonb NOT NULL CHECK (jsonb_typeof(result_json) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (delivery_id, attempt) REFERENCES deliveries(delivery_id, attempt),
  FOREIGN KEY (delivery_id, attempt, permit_id) REFERENCES deliveries(delivery_id, attempt, consume_permit_id),
  FOREIGN KEY (delivery_id, attempt, permit_id, invocation_generation, invocation_id)
    REFERENCES delivery_invocations(delivery_id, attempt, permit_id, invocation_generation, invocation_id),
  CHECK ((invocation_generation IS NULL) = (invocation_id IS NULL)),
  CHECK ((evidence_kind = 'pre_permit_disconnect') = (permit_id IS NULL AND invocation_generation IS NULL))
);

CREATE UNIQUE INDEX delivery_reconcile_pre_permit_evidence
  ON delivery_reconciliation_commands(delivery_id, attempt, evidence_kind, evidence_digest)
  WHERE invocation_generation IS NULL;
CREATE UNIQUE INDEX delivery_reconcile_invocation_evidence
  ON delivery_reconciliation_commands(delivery_id, attempt, invocation_generation, evidence_kind, evidence_digest)
  WHERE invocation_generation IS NOT NULL;

ALTER TABLE receipts
  ADD COLUMN causal_order bigint GENERATED ALWAYS AS IDENTITY,
  ADD COLUMN effect_kind text CHECK (effect_kind IN ('reply_committed', 'task_created', 'reminder_fired')),
  ADD COLUMN effect_message_id message_id_text REFERENCES messages(message_id),
  ADD COLUMN effect_task_id task_id_text REFERENCES tasks(task_id),
  ADD COLUMN effect_reminder_id command_id_text,
  ADD COLUMN effect_reminder_generation bigint,
  ADD COLUMN delivery_id delivery_id_text,
  ADD COLUMN attempt integer CHECK (attempt >= 1),
  ADD COLUMN permit_id command_id_text,
  ADD COLUMN invocation_generation integer CHECK (invocation_generation >= 1),
  ADD COLUMN invocation_id command_id_text,
  ADD COLUMN boundary text CHECK (boundary IN ('input_written', 'model_visible')),
  ADD COLUMN boundary_ack_command_id command_id_text,
  ADD COLUMN boundary_reconciliation_command_id command_id_text,
  ADD CONSTRAINT receipts_delivery_attempt_fk FOREIGN KEY (delivery_id, attempt)
    REFERENCES deliveries(delivery_id, attempt),
  ADD CONSTRAINT receipts_invocation_fk FOREIGN KEY (
    delivery_id, attempt, permit_id, invocation_generation, invocation_id
  ) REFERENCES delivery_invocations(
    delivery_id, attempt, permit_id, invocation_generation, invocation_id
  ),
  ADD CONSTRAINT receipts_boundary_tuple UNIQUE (
    receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary
  ),
  ADD CONSTRAINT receipts_effect_shape CHECK (
    (kind <> 'side_effect_applied' AND effect_kind IS NULL AND effect_message_id IS NULL AND
      effect_task_id IS NULL AND effect_reminder_id IS NULL AND effect_reminder_generation IS NULL) OR
    (kind = 'side_effect_applied' AND (
      (effect_kind = 'reply_committed' AND effect_message_id IS NOT NULL AND effect_task_id IS NULL AND effect_reminder_id IS NULL AND effect_reminder_generation IS NULL) OR
      (effect_kind = 'task_created' AND effect_message_id IS NULL AND effect_task_id IS NOT NULL AND effect_reminder_id IS NULL AND effect_reminder_generation IS NULL) OR
      (effect_kind = 'reminder_fired' AND effect_message_id IS NULL AND effect_task_id IS NULL AND effect_reminder_id IS NOT NULL AND effect_reminder_generation IS NOT NULL)
    ))
  ),
  ADD CONSTRAINT receipts_boundary_shape CHECK (
    (kind IN ('input_written', 'model_visible') AND delivery_id IS NOT NULL AND attempt IS NOT NULL AND
      permit_id IS NOT NULL AND invocation_generation IS NOT NULL AND invocation_id IS NOT NULL AND
      boundary = kind AND ((boundary_ack_command_id IS NOT NULL)::integer +
      (boundary_reconciliation_command_id IS NOT NULL)::integer) = 1) OR
    (kind NOT IN ('input_written', 'model_visible') AND delivery_id IS NULL AND attempt IS NULL AND
      permit_id IS NULL AND invocation_generation IS NULL AND invocation_id IS NULL AND boundary IS NULL AND
      boundary_ack_command_id IS NULL AND boundary_reconciliation_command_id IS NULL)
  );

ALTER TABLE receipts DROP CONSTRAINT receipts_check;
ALTER TABLE receipts ADD CONSTRAINT receipts_actor_shape_v1 CHECK (
  (kind = 'server_accepted' AND actor_server_id IS NOT NULL AND actor_machine_id IS NULL AND actor_agent_id IS NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
  (kind = 'claim_won' AND actor_server_id IS NOT NULL AND actor_machine_id IS NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NOT NULL AND fence_token IS NOT NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
  (kind = 'daemon_accepted' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
  (kind = 'process_spawned' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
  (kind = 'runtime_ready' AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NULL AND session_id IS NOT NULL AND artifact_digest IS NULL) OR
  (kind IN ('input_written', 'model_visible') AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NOT NULL AND session_id IS NOT NULL AND artifact_digest IS NULL) OR
  (kind = 'side_effect_applied' AND effect_kind IN ('reply_committed', 'task_created') AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND ((lease_epoch IS NULL AND fence_token IS NULL) OR (lease_epoch IS NOT NULL AND fence_token IS NOT NULL)) AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NOT NULL AND session_id IS NOT NULL AND artifact_digest IS NULL) OR
  (kind = 'side_effect_applied' AND effect_kind = 'reminder_fired' AND actor_server_id IS NOT NULL AND actor_machine_id IS NULL AND actor_agent_id IS NULL AND lease_epoch IS NULL AND fence_token IS NULL AND launch_id IS NULL AND state_instance_id IS NULL AND turn_id IS NULL AND session_id IS NULL AND artifact_digest IS NULL) OR
  (kind IN ('artifact_published', 'review_verdict') AND actor_server_id IS NULL AND actor_machine_id IS NOT NULL AND actor_agent_id IS NOT NULL AND lease_epoch IS NOT NULL AND fence_token IS NOT NULL AND launch_id IS NOT NULL AND state_instance_id IS NOT NULL AND turn_id IS NOT NULL AND session_id IS NOT NULL AND artifact_digest IS NOT NULL)
);

CREATE UNIQUE INDEX receipts_native_boundary_once
  ON receipts(producer_fact_id, turn_id, delivery_id, attempt, invocation_generation, boundary)
  WHERE boundary IS NOT NULL;
CREATE UNIQUE INDEX receipts_terminal_native_turn_once
  ON receipts(producer_fact_id, turn_id)
  WHERE boundary = 'model_visible';
CREATE UNIQUE INDEX receipts_native_effect_once
  ON receipts(producer_fact_id, turn_id, effect_kind)
  WHERE effect_kind IN ('reply_committed', 'task_created');

CREATE TABLE delivery_boundary_ack_results (
  receipt_id receipt_id_text PRIMARY KEY,
  delivery_id delivery_id_text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  permit_id command_id_text NOT NULL,
  invocation_generation integer NOT NULL CHECK (invocation_generation >= 1),
  invocation_id command_id_text NOT NULL,
  boundary text NOT NULL CHECK (boundary IN ('input_written', 'model_visible')),
  result_json_bytes bytea NOT NULL,
  result_digest artifact_digest_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary),
  FOREIGN KEY (receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary)
    REFERENCES receipts(receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE delivery_ack_commands (
  command_id command_id_text PRIMARY KEY,
  request_digest artifact_digest_text NOT NULL,
  delivery_id delivery_id_text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  permit_id command_id_text NOT NULL,
  invocation_generation integer NOT NULL CHECK (invocation_generation >= 1),
  invocation_id command_id_text NOT NULL,
  boundary text NOT NULL CHECK (boundary IN ('input_written', 'model_visible')),
  canonical_receipt_id receipt_id_text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (delivery_id, attempt, permit_id, invocation_generation, invocation_id)
    REFERENCES delivery_invocations(delivery_id, attempt, permit_id, invocation_generation, invocation_id),
  FOREIGN KEY (canonical_receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary)
    REFERENCES delivery_boundary_ack_results(
      receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary
    ) DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE receipts
  ADD CONSTRAINT receipts_boundary_ack_creator_fk
    FOREIGN KEY (boundary_ack_command_id) REFERENCES delivery_ack_commands(command_id)
    DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT receipts_boundary_reconcile_creator_fk
    FOREIGN KEY (boundary_reconciliation_command_id) REFERENCES delivery_reconciliation_commands(command_id)
    DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT receipts_boundary_projection_fk
    FOREIGN KEY (receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary)
    REFERENCES delivery_boundary_ack_results(
      receipt_id, delivery_id, attempt, permit_id, invocation_generation, invocation_id, boundary
    ) MATCH SIMPLE DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION validate_boundary_ack_result() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  decoded jsonb;
BEGIN
  IF NEW.result_digest::text <> ('sha256:' || encode(sha256(NEW.result_json_bytes), 'hex')) THEN
    RAISE EXCEPTION 'boundary result digest mismatch' USING ERRCODE = 'check_violation';
  END IF;
  BEGIN
    decoded := convert_from(NEW.result_json_bytes, 'UTF8')::jsonb;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'boundary result is not UTF-8 JSON' USING ERRCODE = 'invalid_text_representation';
  END;
  IF decoded ->> 'receiptId' <> NEW.receipt_id::text OR
     decoded ->> 'boundary' <> NEW.boundary OR
     (decoded #>> '{invocation,invocationGeneration}')::integer <> NEW.invocation_generation OR
     decoded #>> '{invocation,invocationId}' <> NEW.invocation_id::text OR
     decoded ->> 'jobState' <> (CASE NEW.boundary
       WHEN 'input_written' THEN 'held/INPUT_WRITTEN'
       ELSE 'acked/MODEL_VISIBLE'
     END) THEN
    RAISE EXCEPTION 'boundary result projection mismatch' USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER delivery_boundary_ack_results_validate
BEFORE INSERT OR UPDATE ON delivery_boundary_ack_results
FOR EACH ROW EXECUTE FUNCTION validate_boundary_ack_result();

CREATE FUNCTION immutable_boundary_ack_result() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'boundary result projections are immutable' USING ERRCODE = 'check_violation';
END
$$;

CREATE TRIGGER delivery_boundary_ack_results_immutable
BEFORE UPDATE OR DELETE ON delivery_boundary_ack_results
FOR EACH ROW EXECUTE FUNCTION immutable_boundary_ack_result();

CREATE FUNCTION validate_boundary_receipt_creator() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  matched boolean;
BEGIN
  IF NEW.boundary IS NULL THEN RETURN NULL; END IF;
  IF NEW.boundary_ack_command_id IS NOT NULL THEN
    SELECT true INTO matched FROM delivery_ack_commands c
      WHERE c.command_id = NEW.boundary_ack_command_id
        AND ROW(c.delivery_id,c.attempt,c.permit_id,c.invocation_generation,c.invocation_id,c.boundary)
          = ROW(NEW.delivery_id,NEW.attempt,NEW.permit_id,NEW.invocation_generation,NEW.invocation_id,NEW.boundary)
        AND c.canonical_receipt_id = NEW.receipt_id;
  ELSE
    SELECT true INTO matched FROM delivery_reconciliation_commands c
      WHERE c.command_id = NEW.boundary_reconciliation_command_id
        AND ROW(c.delivery_id,c.attempt,c.permit_id,c.invocation_generation,c.invocation_id)
          = ROW(NEW.delivery_id,NEW.attempt,NEW.permit_id,NEW.invocation_generation,NEW.invocation_id)
        AND c.result_json->>'kind' = 'boundary_repaired'
        AND c.result_json->'repaired' ? NEW.boundary;
  END IF;
  IF coalesce(matched, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'boundary receipt creator fence mismatch' USING ERRCODE = 'foreign_key_violation';
  END IF;
  RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER receipts_boundary_creator_match
AFTER INSERT OR UPDATE ON receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_boundary_receipt_creator();

ALTER TABLE tasks
  ADD CONSTRAINT tasks_command_target UNIQUE (task_id, source_message_id, source_producer_fact_id);

CREATE TABLE task_commands (
  command_id command_id_text PRIMARY KEY,
  request_digest artifact_digest_text NOT NULL,
  incoming_producer_fact_id producer_fact_id_text NOT NULL REFERENCES messages(producer_fact_id),
  source_message_id message_id_text NOT NULL,
  turn_id turn_id_text NOT NULL,
  task_id task_id_text NOT NULL UNIQUE REFERENCES tasks(task_id),
  result_json jsonb NOT NULL CHECK (jsonb_typeof(result_json) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (incoming_producer_fact_id, turn_id),
  FOREIGN KEY (source_message_id, incoming_producer_fact_id)
    REFERENCES messages(message_id, producer_fact_id),
  FOREIGN KEY (task_id, source_message_id, incoming_producer_fact_id)
    REFERENCES tasks(task_id, source_message_id, source_producer_fact_id)
);

CREATE TABLE observation_cursors (
  actor_kind text NOT NULL CHECK (actor_kind IN ('human', 'agent')),
  actor_id text NOT NULL,
  stream text NOT NULL CHECK (stream IN ('client_message', 'agent_attention')),
  target_kind text NOT NULL,
  target_id text NOT NULL,
  thread_root_message_id message_id_text,
  membership_epoch bigint NOT NULL CHECK (membership_epoch >= 1),
  server_seq bigint NOT NULL CHECK (server_seq >= 1),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (strict_actor_ok(actor_kind, actor_id)),
  CHECK (strict_target_ok(target_kind, target_id, thread_root_message_id)),
  UNIQUE NULLS NOT DISTINCT (
    actor_kind, actor_id, stream, target_kind, target_id, thread_root_message_id, membership_epoch
  )
);

ALTER TABLE reminders
  ADD COLUMN fire_producer_fact_id producer_fact_id_text,
  ADD COLUMN request_digest artifact_digest_text,
  ADD COLUMN status text NOT NULL DEFAULT 'scheduled'
    CHECK (status IN ('scheduled', 'canceled', 'fired')),
  ADD COLUMN row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0);

UPDATE reminders SET
  fire_producer_fact_id = (
    'fac_' || substr(encode(sha256(convert_to(reminder_id::text || ':' || generation::text, 'UTF8')), 'hex'), 1, 26)
  )::producer_fact_id_text,
  request_digest = (
    'sha256:' || encode(sha256(convert_to(reminder_id::text || ':' || generation::text || ':legacy', 'UTF8')), 'hex')
  )::artifact_digest_text;

ALTER TABLE reminders
  ALTER COLUMN fire_producer_fact_id SET NOT NULL,
  ADD CONSTRAINT reminders_fire_identity UNIQUE (reminder_id, generation, fire_producer_fact_id),
  ADD CONSTRAINT reminders_fire_producer_once UNIQUE (fire_producer_fact_id);

CREATE TABLE reminder_heads (
  reminder_id command_id_text PRIMARY KEY,
  current_generation bigint NOT NULL CHECK (current_generation >= 1),
  row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
  FOREIGN KEY (reminder_id, current_generation)
    REFERENCES reminders(reminder_id, generation) DEFERRABLE INITIALLY DEFERRED
);

INSERT INTO reminder_heads(reminder_id, current_generation, row_version)
SELECT DISTINCT ON (reminder_id) reminder_id, generation, row_version
FROM reminders
ORDER BY reminder_id, generation DESC;

CREATE TABLE reminder_fires (
  reminder_id command_id_text NOT NULL,
  generation bigint NOT NULL CHECK (generation >= 1),
  producer_fact_id producer_fact_id_text NOT NULL,
  request_digest artifact_digest_text NOT NULL,
  outbox_job_id bigint NOT NULL UNIQUE REFERENCES outbox_jobs(job_id),
  receipt_id receipt_id_text NOT NULL UNIQUE REFERENCES receipts(receipt_id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (reminder_id, generation),
  FOREIGN KEY (reminder_id, generation, producer_fact_id)
    REFERENCES reminders(reminder_id, generation, fire_producer_fact_id)
);

ALTER TABLE receipts
  ADD CONSTRAINT receipts_reminder_effect_fk
    FOREIGN KEY (effect_reminder_id, effect_reminder_generation)
    REFERENCES reminders(reminder_id, generation) DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX reminder_fires_producer_once ON reminder_fires(producer_fact_id);

-- Dedicated Wave 1 command ledgers own their replay semantics. The generic
-- Wave 0 ledger remains valid for existing repositories and receives only the
-- new top-level mutation namespaces.
ALTER TABLE command_requests DROP CONSTRAINT command_requests_scope_check;
ALTER TABLE command_requests ADD CONSTRAINT command_requests_scope_v1 CHECK (scope IN (
  'message.append.v1', 'message.reply.v1', 'claim.mutate.v1', 'task_graph.mutate.v1',
  'task.create.v1', 'reminder.mutate.v1', 'launch.mutate.v1', 'delivery.mutate.v1',
  'receipt.record.v1', 'artifact.mutate.v1', 'outbox.mutate.v1', 'registry.config.v1',
  'registry.liveness.v1', 'membership.mutate.v1', 'route.mutate.v1', 'cursor.ack.v1'
));
