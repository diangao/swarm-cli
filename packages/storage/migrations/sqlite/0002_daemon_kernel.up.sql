CREATE TABLE local_agent_slots (
  agent_id TEXT PRIMARY KEY
    CHECK (length(agent_id) = 30 AND substr(agent_id, 1, 4) = 'agt_' AND substr(agent_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  machine_id TEXT NOT NULL
    CHECK (length(machine_id) = 30 AND substr(machine_id, 1, 4) = 'mch_' AND substr(machine_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  state TEXT NOT NULL CHECK (state IN ('nonresident', 'queued', 'starting', 'spawned', 'ready', 'activated', 'stopping')),
  stop_epoch INTEGER NOT NULL CHECK (stop_epoch >= 0),
  current_launch_id TEXT CHECK (current_launch_id IS NULL OR (length(current_launch_id) = 30 AND substr(current_launch_id, 1, 4) = 'lnc_' AND substr(current_launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  current_state_instance_id TEXT CHECK (current_state_instance_id IS NULL OR (length(current_state_instance_id) = 30 AND substr(current_state_instance_id, 1, 4) = 'sti_' AND substr(current_state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  current_session_id TEXT CHECK (current_session_id IS NULL OR (length(current_session_id) = 30 AND substr(current_session_id, 1, 4) = 'ses_' AND substr(current_session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  queue_ordinal INTEGER CHECK (queue_ordinal IS NULL OR queue_ordinal >= 1),
  updated_at TEXT NOT NULL,
  CHECK (
    (state = 'nonresident' AND current_launch_id IS NULL AND current_state_instance_id IS NULL AND current_session_id IS NULL AND queue_ordinal IS NULL) OR
    (state IN ('queued', 'starting') AND current_launch_id IS NOT NULL AND current_state_instance_id IS NULL AND current_session_id IS NULL AND queue_ordinal IS NOT NULL) OR
    (state = 'spawned' AND current_launch_id IS NOT NULL AND current_state_instance_id IS NOT NULL AND current_session_id IS NULL AND queue_ordinal IS NOT NULL) OR
    (state IN ('ready', 'activated') AND current_launch_id IS NOT NULL AND current_state_instance_id IS NOT NULL AND current_session_id IS NOT NULL AND queue_ordinal IS NOT NULL) OR
    (state = 'stopping' AND current_launch_id IS NOT NULL)
  )
) STRICT;

CREATE TABLE local_launches (
  launch_id TEXT PRIMARY KEY
    CHECK (length(launch_id) = 30 AND substr(launch_id, 1, 4) = 'lnc_' AND substr(launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  agent_id TEXT NOT NULL REFERENCES local_agent_slots(agent_id),
  machine_id TEXT NOT NULL
    CHECK (length(machine_id) = 30 AND substr(machine_id, 1, 4) = 'mch_' AND substr(machine_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  runtime_kind TEXT NOT NULL CHECK (runtime_kind IN ('codex', 'claude', 'scripted_fake')),
  routing_generation INTEGER NOT NULL CHECK (routing_generation >= 0),
  workspace_generation INTEGER NOT NULL CHECK (workspace_generation >= 1),
  stop_epoch INTEGER NOT NULL CHECK (stop_epoch >= 0),
  queue_ordinal INTEGER NOT NULL CHECK (queue_ordinal >= 1),
  state TEXT NOT NULL CHECK (state IN ('queued', 'starting', 'spawned', 'ready', 'activated', 'stopping', 'terminal')),
  state_instance_id TEXT CHECK (state_instance_id IS NULL OR (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  session_id TEXT CHECK (session_id IS NULL OR (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  driver_identity_digest TEXT NOT NULL CHECK (length(driver_identity_digest) = 71 AND substr(driver_identity_digest, 1, 7) = 'sha256:' AND substr(driver_identity_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  process_handle_digest TEXT CHECK (process_handle_digest IS NULL OR (length(process_handle_digest) = 71 AND substr(process_handle_digest, 1, 7) = 'sha256:' AND substr(process_handle_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  runtime_session_ref_private TEXT,
  runtime_session_ref_digest TEXT CHECK (runtime_session_ref_digest IS NULL OR (length(runtime_session_ref_digest) = 71 AND substr(runtime_session_ref_digest, 1, 7) = 'sha256:' AND substr(runtime_session_ref_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  manifest_digest TEXT CHECK (manifest_digest IS NULL OR (length(manifest_digest) = 71 AND substr(manifest_digest, 1, 7) = 'sha256:' AND substr(manifest_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  transport_digest TEXT CHECK (transport_digest IS NULL OR (length(transport_digest) = 71 AND substr(transport_digest, 1, 7) = 'sha256:' AND substr(transport_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  queued_at TEXT NOT NULL,
  started_at TEXT,
  spawned_at TEXT,
  ready_at TEXT,
  activated_at TEXT,
  stop_requested_at TEXT,
  terminal_at TEXT,
  terminal_reason TEXT,
  CHECK (state NOT IN ('starting', 'spawned', 'ready', 'activated') OR (started_at IS NOT NULL AND transport_digest IS NOT NULL)),
  CHECK (state NOT IN ('spawned', 'ready', 'activated') OR (state_instance_id IS NOT NULL AND process_handle_digest IS NOT NULL AND spawned_at IS NOT NULL)),
  CHECK (state NOT IN ('ready', 'activated') OR (session_id IS NOT NULL AND runtime_session_ref_private IS NOT NULL AND runtime_session_ref_digest IS NOT NULL AND manifest_digest IS NOT NULL AND ready_at IS NOT NULL)),
  CHECK (state <> 'activated' OR activated_at IS NOT NULL),
  CHECK (state <> 'stopping' OR stop_requested_at IS NOT NULL),
  CHECK (state <> 'terminal' OR (terminal_at IS NOT NULL AND terminal_reason IS NOT NULL))
) STRICT;

CREATE UNIQUE INDEX local_launches_one_nonterminal_per_agent
  ON local_launches(agent_id)
  WHERE state <> 'terminal';

CREATE TABLE IF NOT EXISTS native_attempts (
  delivery_id TEXT NOT NULL REFERENCES pending_deliveries(delivery_id)
    CHECK (length(delivery_id) = 30 AND substr(delivery_id, 1, 4) = 'dlv_' AND substr(delivery_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  attempt INTEGER NOT NULL CHECK (attempt >= 1),
  fence_json TEXT NOT NULL CHECK (json_valid(fence_json)),
  state TEXT NOT NULL CHECK (state IN ('accepted', 'pre_permit_disconnect', 'permit_recorded', 'write_started', 'not_written', 'input_written', 'model_visible', 'ambiguous', 'suppressed', 'consumed')),
  permit_id TEXT CHECK (permit_id IS NULL OR (length(permit_id) = 30 AND substr(permit_id, 1, 4) = 'cmd_' AND substr(permit_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  invocation_generation INTEGER CHECK (invocation_generation IS NULL OR invocation_generation >= 1),
  invocation_id TEXT CHECK (invocation_id IS NULL OR (length(invocation_id) = 30 AND substr(invocation_id, 1, 4) = 'cmd_' AND substr(invocation_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  body_digest TEXT CHECK (body_digest IS NULL OR (length(body_digest) = 71 AND substr(body_digest, 1, 7) = 'sha256:' AND substr(body_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  previous_invocation_generation INTEGER,
  previous_proof_digest TEXT CHECK (previous_proof_digest IS NULL OR (length(previous_proof_digest) = 71 AND substr(previous_proof_digest, 1, 7) = 'sha256:' AND substr(previous_proof_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  proof_json TEXT,
  disconnect_id TEXT CHECK (disconnect_id IS NULL OR (length(disconnect_id) = 30 AND substr(disconnect_id, 1, 4) = 'cmd_' AND substr(disconnect_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  suppression_reason TEXT,
  launch_id TEXT REFERENCES local_launches(launch_id),
  state_instance_id TEXT CHECK (state_instance_id IS NULL OR (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  session_id TEXT CHECK (session_id IS NULL OR (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  PRIMARY KEY (delivery_id, attempt)
) STRICT;

CREATE TABLE IF NOT EXISTS native_invocation_entries (
  delivery_id TEXT NOT NULL
    CHECK (length(delivery_id) = 30 AND substr(delivery_id, 1, 4) = 'dlv_' AND substr(delivery_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  attempt INTEGER NOT NULL,
  invocation_generation INTEGER NOT NULL CHECK (invocation_generation >= 1),
  sequence INTEGER NOT NULL CHECK (sequence >= 1),
  kind TEXT NOT NULL CHECK (kind IN ('permit_recorded', 'write_started', 'input_written', 'model_visible')),
  entry_json TEXT NOT NULL CHECK (json_valid(entry_json)),
  PRIMARY KEY (delivery_id, attempt, invocation_generation, sequence),
  UNIQUE (delivery_id, attempt, sequence),
  UNIQUE (delivery_id, attempt, invocation_generation, kind),
  FOREIGN KEY (delivery_id, attempt) REFERENCES native_attempts(delivery_id, attempt)
) STRICT;

CREATE TABLE visible_message_ids (
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  target_key TEXT NOT NULL COLLATE BINARY,
  message_id TEXT NOT NULL
    CHECK (length(message_id) = 30 AND substr(message_id, 1, 4) = 'msg_' AND substr(message_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  delivery_id TEXT NOT NULL
    CHECK (length(delivery_id) = 30 AND substr(delivery_id, 1, 4) = 'dlv_' AND substr(delivery_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  attempt INTEGER NOT NULL CHECK (attempt >= 1),
  server_seq INTEGER NOT NULL CHECK (server_seq >= 1),
  model_visible_receipt_id TEXT NOT NULL
    CHECK (length(model_visible_receipt_id) = 30 AND substr(model_visible_receipt_id, 1, 4) = 'rcp_' AND substr(model_visible_receipt_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  visible_at TEXT NOT NULL,
  PRIMARY KEY (session_id, target_key, message_id),
  UNIQUE (delivery_id, attempt),
  FOREIGN KEY (delivery_id) REFERENCES pending_deliveries(delivery_id)
) STRICT;

CREATE TABLE notice_visibility (
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  target_key TEXT NOT NULL COLLATE BINARY,
  membership_epoch INTEGER NOT NULL CHECK (membership_epoch >= 1),
  first_message_id TEXT NOT NULL
    CHECK (length(first_message_id) = 30 AND substr(first_message_id, 1, 4) = 'msg_' AND substr(first_message_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  latest_message_id TEXT NOT NULL
    CHECK (length(latest_message_id) = 30 AND substr(latest_message_id, 1, 4) = 'msg_' AND substr(latest_message_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  first_server_seq INTEGER NOT NULL CHECK (first_server_seq >= 1),
  latest_server_seq INTEGER NOT NULL CHECK (latest_server_seq >= first_server_seq),
  input_delivery_id TEXT NOT NULL
    CHECK (length(input_delivery_id) = 30 AND substr(input_delivery_id, 1, 4) = 'dlv_' AND substr(input_delivery_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  input_attempt INTEGER NOT NULL CHECK (input_attempt >= 1),
  committed_at TEXT NOT NULL,
  PRIMARY KEY (session_id, target_key, membership_epoch),
  FOREIGN KEY (input_delivery_id, input_attempt) REFERENCES native_attempts(delivery_id, attempt)
) STRICT;

CREATE TABLE local_turns (
  protocol_turn_id TEXT PRIMARY KEY
    CHECK (length(protocol_turn_id) = 30 AND substr(protocol_turn_id, 1, 4) = 'trn_' AND substr(protocol_turn_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  launch_id TEXT NOT NULL REFERENCES local_launches(launch_id)
    CHECK (length(launch_id) = 30 AND substr(launch_id, 1, 4) = 'lnc_' AND substr(launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  state_instance_id TEXT NOT NULL
    CHECK (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  root_producer_fact_id TEXT NOT NULL
    CHECK (length(root_producer_fact_id) = 30 AND substr(root_producer_fact_id, 1, 4) = 'fac_' AND substr(root_producer_fact_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  input_ordinal INTEGER NOT NULL CHECK (input_ordinal >= 0),
  driver_turn_ref_digest TEXT NOT NULL
    CHECK (length(driver_turn_ref_digest) = 71 AND substr(driver_turn_ref_digest, 1, 7) = 'sha256:' AND substr(driver_turn_ref_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  mode TEXT NOT NULL CHECK (mode IN ('ordinary', 'steer')),
  expected_turn_id TEXT CHECK (expected_turn_id IS NULL OR (length(expected_turn_id) = 30 AND substr(expected_turn_id, 1, 4) = 'trn_' AND substr(expected_turn_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  state TEXT NOT NULL CHECK (state IN ('queued', 'write_started', 'input_written', 'model_visible', 'completed', 'ambiguous', 'interrupted', 'terminal_error')),
  queued_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK ((mode = 'ordinary' AND input_ordinal = 0 AND expected_turn_id IS NULL) OR (mode = 'steer' AND input_ordinal >= 1 AND expected_turn_id = protocol_turn_id))
) STRICT;

CREATE TABLE driver_event_cursor (
  state_instance_id TEXT NOT NULL
    CHECK (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  next_ordinal INTEGER NOT NULL CHECK (next_ordinal >= 0),
  last_event_digest TEXT CHECK (last_event_digest IS NULL OR (length(last_event_digest) = 71 AND substr(last_event_digest, 1, 7) = 'sha256:' AND substr(last_event_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  reader_owner_token TEXT CHECK (reader_owner_token IS NULL OR (length(reader_owner_token) = 71 AND substr(reader_owner_token, 1, 7) = 'sha256:' AND substr(reader_owner_token, 8) NOT GLOB '*[^0-9a-f]*')),
  reader_epoch INTEGER NOT NULL DEFAULT 0 CHECK (reader_epoch >= 0),
  updated_at TEXT NOT NULL,
  PRIMARY KEY (state_instance_id, session_id),
  UNIQUE (state_instance_id),
  CHECK ((next_ordinal = 0 AND last_event_digest IS NULL) OR (next_ordinal > 0 AND last_event_digest IS NOT NULL))
) STRICT;

CREATE TABLE driver_event_records (
  state_instance_id TEXT NOT NULL
    CHECK (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  event_digest TEXT NOT NULL
    CHECK (length(event_digest) = 71 AND substr(event_digest, 1, 7) = 'sha256:' AND substr(event_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  turn_id TEXT CHECK (turn_id IS NULL OR (length(turn_id) = 30 AND substr(turn_id, 1, 4) = 'trn_' AND substr(turn_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  binding_digest TEXT CHECK (binding_digest IS NULL OR (length(binding_digest) = 71 AND substr(binding_digest, 1, 7) = 'sha256:' AND substr(binding_digest, 8) NOT GLOB '*[^0-9a-f]*')),
  recorded_at TEXT NOT NULL,
  PRIMARY KEY (state_instance_id, session_id, ordinal),
  FOREIGN KEY (state_instance_id, session_id) REFERENCES driver_event_cursor(state_instance_id, session_id)
) STRICT;

CREATE INDEX notice_visibility_input ON notice_visibility(input_delivery_id, input_attempt);
CREATE UNIQUE INDEX local_turns_one_active_per_session ON local_turns(session_id)
  WHERE state IN ('queued', 'write_started', 'input_written', 'model_visible', 'ambiguous');
