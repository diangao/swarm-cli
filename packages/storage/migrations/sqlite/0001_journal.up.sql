CREATE TABLE local_operations (
  operation_id TEXT PRIMARY KEY
    CHECK (length(operation_id) = 30 AND substr(operation_id, 1, 4) = 'cmd_' AND substr(operation_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  operation_kind TEXT NOT NULL CHECK (trim(operation_kind) <> ''),
  launch_id TEXT NOT NULL
    CHECK (length(launch_id) = 30 AND substr(launch_id, 1, 4) = 'lnc_' AND substr(launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  state_instance_id TEXT NOT NULL
    CHECK (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  turn_id TEXT CHECK (turn_id IS NULL OR (length(turn_id) = 30 AND substr(turn_id, 1, 4) = 'trn_' AND substr(turn_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  session_id TEXT CHECK (session_id IS NULL OR (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  idempotency_key TEXT NOT NULL CHECK (trim(idempotency_key) <> ''),
  payload_digest TEXT NOT NULL CHECK (length(payload_digest) = 71 AND substr(payload_digest, 1, 7) = 'sha256:' AND substr(payload_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  state TEXT NOT NULL CHECK (state IN ('prepared', 'effect_started', 'effect_observed', 'terminal', 'canceled')),
  prepared_at TEXT NOT NULL,
  effect_started_at TEXT,
  effect_observed_at TEXT,
  terminal_at TEXT,
  canceled_at TEXT,
  detail_json TEXT NOT NULL CHECK (json_valid(detail_json) AND json_type(detail_json, '$.protocolVersion') = 'integer' AND json_extract(detail_json, '$.protocolVersion') BETWEEN 1 AND 999999),
  CHECK (effect_observed_at IS NULL OR effect_started_at IS NOT NULL),
  CHECK (terminal_at IS NULL OR effect_observed_at IS NOT NULL),
  CHECK (canceled_at IS NULL OR state = 'canceled')
) STRICT;

CREATE TABLE pending_deliveries (
  delivery_id TEXT PRIMARY KEY
    CHECK (length(delivery_id) = 30 AND substr(delivery_id, 1, 4) = 'dlv_' AND substr(delivery_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  attempt INTEGER NOT NULL CHECK (attempt >= 1),
  replay_of TEXT REFERENCES pending_deliveries(delivery_id),
  message_id TEXT NOT NULL
    CHECK (length(message_id) = 30 AND substr(message_id, 1, 4) = 'msg_' AND substr(message_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  producer_fact_id TEXT NOT NULL
    CHECK (length(producer_fact_id) = 30 AND substr(producer_fact_id, 1, 4) = 'fac_' AND substr(producer_fact_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  agent_id TEXT NOT NULL
    CHECK (length(agent_id) = 30 AND substr(agent_id, 1, 4) = 'agt_' AND substr(agent_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  machine_id TEXT NOT NULL
    CHECK (length(machine_id) = 30 AND substr(machine_id, 1, 4) = 'mch_' AND substr(machine_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  expected_launch_id TEXT CHECK (expected_launch_id IS NULL OR (length(expected_launch_id) = 30 AND substr(expected_launch_id, 1, 4) = 'lnc_' AND substr(expected_launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  envelope_digest TEXT NOT NULL CHECK (length(envelope_digest) = 71 AND substr(envelope_digest, 1, 7) = 'sha256:' AND substr(envelope_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  target_key TEXT NOT NULL COLLATE BINARY,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('channel', 'direct')),
  target_id TEXT NOT NULL CHECK (
    (target_kind = 'channel' AND length(target_id) = 30 AND substr(target_id, 1, 4) = 'chn_' AND substr(target_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*') OR
    (target_kind = 'direct' AND length(target_id) = 30 AND substr(target_id, 1, 4) = 'cvs_' AND substr(target_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')
  ),
  thread_root_message_id TEXT CHECK (thread_root_message_id IS NULL OR (length(thread_root_message_id) = 30 AND substr(thread_root_message_id, 1, 4) = 'msg_' AND substr(thread_root_message_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  launch_id TEXT NOT NULL
    CHECK (length(launch_id) = 30 AND substr(launch_id, 1, 4) = 'lnc_' AND substr(launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  state_instance_id TEXT NOT NULL
    CHECK (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  turn_id TEXT NOT NULL
    CHECK (length(turn_id) = 30 AND substr(turn_id, 1, 4) = 'trn_' AND substr(turn_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  server_seq INTEGER NOT NULL CHECK (server_seq >= 1),
  received_at TEXT NOT NULL,
  input_written_at TEXT,
  model_visible_at TEXT,
  ack_intent_at TEXT,
  consumed_at TEXT,
  canceled_at TEXT,
  CHECK ((attempt = 1 AND replay_of IS NULL) OR (attempt > 1 AND replay_of IS NOT NULL AND replay_of <> delivery_id)),
  CHECK (model_visible_at IS NULL OR input_written_at IS NOT NULL),
  CHECK (ack_intent_at IS NULL OR model_visible_at IS NOT NULL),
  CHECK (consumed_at IS NULL OR ack_intent_at IS NOT NULL),
  UNIQUE (agent_id, producer_fact_id, attempt),
  UNIQUE (delivery_id, agent_id, producer_fact_id),
  FOREIGN KEY (replay_of, agent_id, producer_fact_id)
    REFERENCES pending_deliveries(delivery_id, agent_id, producer_fact_id)
) STRICT;

CREATE TABLE visibility_checkpoints (
  checkpoint_id INTEGER PRIMARY KEY,
  session_id TEXT NOT NULL
    CHECK (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  target_key TEXT NOT NULL COLLATE BINARY,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('channel', 'direct')),
  target_id TEXT NOT NULL CHECK (
    (target_kind = 'channel' AND length(target_id) = 30 AND substr(target_id, 1, 4) = 'chn_' AND substr(target_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*') OR
    (target_kind = 'direct' AND length(target_id) = 30 AND substr(target_id, 1, 4) = 'cvs_' AND substr(target_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')
  ),
  thread_root_message_id TEXT CHECK (thread_root_message_id IS NULL OR (length(thread_root_message_id) = 30 AND substr(thread_root_message_id, 1, 4) = 'msg_' AND substr(thread_root_message_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  highest_model_visible_server_seq INTEGER NOT NULL CHECK (highest_model_visible_server_seq >= 1),
  last_message_id TEXT NOT NULL,
  last_delivery_id TEXT NOT NULL REFERENCES pending_deliveries(delivery_id),
  last_local_receipt_id INTEGER,
  UNIQUE (session_id, target_key)
) STRICT;

CREATE TABLE runtime_observations (
  launch_id TEXT NOT NULL
    CHECK (length(launch_id) = 30 AND substr(launch_id, 1, 4) = 'lnc_' AND substr(launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  state_instance_id TEXT NOT NULL
    CHECK (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  session_id TEXT CHECK (session_id IS NULL OR (length(session_id) = 30 AND substr(session_id, 1, 4) = 'ses_' AND substr(session_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  process_handle_digest TEXT NOT NULL CHECK (length(process_handle_digest) = 71 AND substr(process_handle_digest, 1, 7) = 'sha256:' AND substr(process_handle_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  observed_at TEXT NOT NULL,
  ready_at TEXT,
  terminal_at TEXT,
  PRIMARY KEY (launch_id, state_instance_id)
) STRICT;

CREATE TABLE outbound_intents (
  intent_id TEXT PRIMARY KEY
    CHECK (length(intent_id) = 30 AND substr(intent_id, 1, 4) = 'cmd_' AND substr(intent_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*'),
  command_kind TEXT NOT NULL CHECK (trim(command_kind) <> ''),
  request_id TEXT NOT NULL CHECK (
    (length(request_id) = 30 AND substr(request_id, 1, 4) = 'cmd_' AND substr(request_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*') OR
    (length(request_id) = 30 AND substr(request_id, 1, 4) = 'fac_' AND substr(request_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')
  ),
  payload_digest TEXT NOT NULL CHECK (length(payload_digest) = 71 AND substr(payload_digest, 1, 7) = 'sha256:' AND substr(payload_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  state TEXT NOT NULL CHECK (state IN ('prepared', 'sent', 'server_confirmed', 'canceled')),
  server_producer_fact_id TEXT CHECK (server_producer_fact_id IS NULL OR (length(server_producer_fact_id) = 30 AND substr(server_producer_fact_id, 1, 4) = 'fac_' AND substr(server_producer_fact_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  server_receipt_id TEXT CHECK (server_receipt_id IS NULL OR (length(server_receipt_id) = 30 AND substr(server_receipt_id, 1, 4) = 'rcp_' AND substr(server_receipt_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  prepared_at TEXT NOT NULL,
  sent_at TEXT,
  confirmed_at TEXT,
  canceled_at TEXT,
  UNIQUE (command_kind, request_id),
  CHECK (state <> 'server_confirmed' OR (server_producer_fact_id IS NOT NULL AND server_receipt_id IS NOT NULL AND confirmed_at IS NOT NULL))
) STRICT;

CREATE TABLE local_receipts (
  local_receipt_id INTEGER PRIMARY KEY,
  operation_id TEXT REFERENCES local_operations(operation_id),
  delivery_id TEXT REFERENCES pending_deliveries(delivery_id),
  boundary_kind TEXT NOT NULL CHECK (boundary_kind IN ('prepared', 'input_written', 'model_visible', 'ack_intent', 'consumed', 'canceled', 'reconciled')),
  lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch >= 1),
  fence_token TEXT CHECK (fence_token IS NULL OR (length(fence_token) = 30 AND substr(fence_token, 1, 4) = 'fnc_' AND substr(fence_token, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  launch_id TEXT CHECK (launch_id IS NULL OR (length(launch_id) = 30 AND substr(launch_id, 1, 4) = 'lnc_' AND substr(launch_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  state_instance_id TEXT CHECK (state_instance_id IS NULL OR (length(state_instance_id) = 30 AND substr(state_instance_id, 1, 4) = 'sti_' AND substr(state_instance_id, 5) NOT GLOB '*[^0-9a-hjkmnp-tv-z]*')),
  occurred_at TEXT NOT NULL,
  detail_digest TEXT NOT NULL CHECK (length(detail_digest) = 71 AND substr(detail_digest, 1, 7) = 'sha256:' AND substr(detail_digest, 8) NOT GLOB '*[^0-9a-f]*'),
  CHECK (operation_id IS NOT NULL OR delivery_id IS NOT NULL)
) STRICT;

CREATE INDEX pending_deliveries_recovery ON pending_deliveries(received_at)
  WHERE consumed_at IS NULL AND canceled_at IS NULL;
CREATE INDEX local_operations_recovery ON local_operations(prepared_at)
  WHERE state NOT IN ('terminal', 'canceled');
CREATE INDEX outbound_intents_recovery ON outbound_intents(prepared_at)
  WHERE state NOT IN ('server_confirmed', 'canceled');
