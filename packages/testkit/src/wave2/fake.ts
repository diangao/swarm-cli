// Canonical Wave 2 fake: the acceptance authority that PRODUCES the neutral
// ledger facts the Gate 2 oracle consumes. Every Gate 2 negative must first
// prove this fake can expose its defect (contract §9.1). The fake emits, for a
// scenario group, a healthy ledger; and for a defect, the same shape plus the
// runtime's fence/observe of that specific defect, with the named sibling facts
// held byte-unchanged. Fields are neutral only: kinds, ids, epochs, counts,
// flags, digests — never a body / prompt / raw vendor payload / secret.

export type LedgerEvent = {
  readonly kind: string;
  readonly [field: string]: string | number | boolean | undefined;
};

export type Wave2Ledger = {
  readonly events: readonly LedgerEvent[];
  readonly siblings: Readonly<Record<string, number | string>>;
};

/** The fence/observe the fake independently emits when it exposes a defect. */
export type DefectResponse =
  | { readonly fenceError: string }
  | { readonly fenceErrors: readonly string[] }
  | { readonly observe: string };

function ev(kind: string, fields: Record<string, string | number | boolean> = {}): LedgerEvent {
  return { kind, ...fields };
}

// The fake's OWN model of the runtime's response to each seeded defect, keyed by
// the defect's action string. This map is INDEPENDENT of the Gate 2 plan's
// expected outcomes (derived from the contract's fence semantics, not from the
// plan). The Gate 0 proof is meaningful precisely because this independent
// exposure must AGREE with the plan's independently-loaded expectation; a
// tautology (fake echoing the plan) is impossible because the plan's expected
// codes never flow into the fake.
const DEFECT_RESPONSES: Record<string, DefectResponse> = {
  // g2_1 machine lock & start dedupe
  allow_two_daemons_to_open_one_journal: { fenceError: "MACHINE_JOURNAL_LOCKED" },
  remove_per_agent_start_cas: { observe: "two_process_start_effects" },
  offer_second_launch_id_while_slot_occupied: { fenceError: "START_SLOT_CONFLICT" },
  // g2_2 bounded starts & fairness
  start_without_capacity_token: { fenceError: "START_RATE_LIMITED" },
  refill_from_real_time: { observe: "non_deterministic_start_order" },
  new_wake_replaces_agent_queue_ordinal: { observe: "older_agent_overtaken" },
  // g2_3 stop during spawn
  omit_post_spawn_epoch_recheck: { fenceError: "LATE_SPAWN_INVALIDATED" },
  mutate_launch_fence_to_new_stop_epoch: { fenceError: "STALE_STOP_EPOCH" },
  apply_ready_from_old_state_instance: { fenceError: "STALE_STATE_INSTANCE" },
  // g2_4 readiness/activation separation & pump/order
  activate_directly_from_spawned: { fenceError: "ACTIVATION_PREDECESSOR_REQUIRED" },
  activate_with_nonmatching_session: { fenceError: "STALE_SESSION_FENCE" },
  treat_process_spawn_as_protocol_readiness: { fenceError: "READINESS_PREDECESSOR_REQUIRED" },
  emit_immediate_initialize_response_before_waiter_registration: { fenceError: "DRIVER_WAITER_PREDECESSOR_REQUIRED" },
  emit_immediate_turn_started_before_write_waiter_registration: { fenceError: "DRIVER_WAITER_PREDECESSOR_REQUIRED" },
  attach_competing_raw_event_reader_to_same_state_instance: { fenceError: "DRIVER_EVENT_READER_CONFLICT" },
  resume_before_old_pump_releases_cursor_ownership: { fenceError: "DRIVER_RESUME_OVERLAP" },
  // g2_5 delivery visible once
  compile_before_activation: { fenceError: "ACTIVATION_PREDECESSOR_REQUIRED" },
  suppress_gap_without_exact_message_id: { observe: "missing_message_was_suppressed" },
  insert_visibility_after_ack_request_only: { fenceError: "MODEL_VISIBLE_PREDECESSOR_REQUIRED" },
  // g2_6 failed/ambiguous/lost-ack
  lease_new_attempt_after_ambiguous: { observe: "second_driver_call" },
  map_all_conflicts_to_one_code: { fenceErrors: ["STALE_INVOCATION_GENERATION", "INVOCATION_STATE_CONFLICT"] },
  call_driver_instead_of_reconcile: { observe: "second_driver_call" },
  // g2_7 notice dedupe & visibility ledger
  dedupe_notice_on_compile: { fenceError: "NOTICE_DEDUPE_PREDECESSOR_REQUIRED" },
  persist_notice_excerpt_or_author: { observe: "privacy_poison" },
  reuse_message_key_for_changed_delivery: { fenceError: "VISIBILITY_LEDGER_CONFLICT" },
  insert_changed_notice_range_without_epoch_key_compare: { observe: "two_notice_rows_for_one_session_target_membership_epoch" },
  // g2_8 turn state & steer fences
  steer_without_exact_turn_compare: { fenceError: "ACTIVE_TURN_CONFLICT" },
  treat_review_as_steerable: { fenceError: "ACTIVE_TURN_NOT_STEERABLE" },
  schedule_continuation_before_pending_external_input: { observe: "external_input_overtaken" },
  emit_steer_or_interrupt_with_unmapped_runtime_turn: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  write_second_distinct_uuid_before_first_replay: { fenceError: "TURN_INPUT_ALREADY_IN_FLIGHT" },
  close_active_window_at_replay_and_write_second_uuid_before_result: { fenceError: "TURN_INPUT_ALREADY_IN_FLIGHT" },
  emit_process_global_interrupt_for_old_or_completed_turn: { fenceError: "ACTIVE_TURN_CONFLICT" },
  accept_interrupt_control_response_with_wrong_request_id_or_nonempty_queued_uuid_set: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  // g2_9 adapter invariance & driver identity
  accept_unknown_variant: { fenceError: "DRIVER_PROTOCOL_UNSUPPORTED" },
  model_visible_before_input_written: { fenceError: "DRIVER_EVENT_ORDER_INVALID" },
  concrete_adapter_imports_server_repository: { observe: "package_boundary_violation" },
  accept_turn_started_with_wrong_missing_or_null_client_id: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  accept_turn_started_from_wrong_thread_or_runtime_turn: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  accept_turn_completed_before_model_visible: { fenceError: "DRIVER_EVENT_ORDER_INVALID" },
  reuse_event_ordinal_or_identity_with_changed_bytes: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  accept_replay_with_missing_or_wrong_uuid_as_model_visible: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  accept_wrong_session_or_changed_content_under_same_uuid: { fenceError: "DRIVER_EVENT_FENCE_MISMATCH" },
  accept_replay_before_successful_stdin_write: { fenceError: "DRIVER_EVENT_ORDER_INVALID" },
  treat_exact_duplicate_replay_as_second_model_input: { observe: "second_model_input_or_visibility_boundary" },
  accept_coordination_call_before_nonempty_root_reply: { observe: "task_effect_before_reply_or_without_reply" },
  accept_empty_reply_or_second_distinct_reply_for_root_turn: { observe: "empty_or_second_reply_fact" },
  accept_normalized_driver_event_after_terminal_completion: { fenceError: "DRIVER_EVENT_ORDER_INVALID" },
  // g2_10 manifest & driver protocol identity
  include_current_wake_in_standing_prompt_hash: { observe: "manifest_digest_changed_for_wake_only" },
  change_canonical_wire_bytes_or_omit_wire_protocol_digest: { fenceError: "DRIVER_PROTOCOL_UNSUPPORTED" },
  reprobe_and_replace_capabilities: { fenceError: "DRIVER_CAPABILITY_MISMATCH" },
  generate_schema_without_exact_experimental_flag_or_from_nonempty_config_home: { fenceError: "DRIVER_PROTOCOL_UNSUPPORTED" },
  hash_nondeterministic_raw_json_instead_of_canonical_json: { observe: "same_schema_semantics_produce_different_wire_digest" },
  // g2_11 launch environment
  copy_full_host_environment: { fenceError: "SECURITY_LAUNCH_GATE_FAILED" },
  reuse_private_home_across_launches: { fenceError: "SECURITY_LAUNCH_GATE_FAILED" },
  write_reusable_credential_to_transport: { fenceError: "SECURITY_LAUNCH_GATE_FAILED" },
  // g2_12 carry gates
  frozen_install_changes_lockfile: { observe: "lockfile_changed" },
  change_one_placeholder_to_asserted: { observe: "dishonest_wave2_disposition" },
  disable_one_required_negative: { observe: "mutation_survived" },
};

// Per-group canonical healthy baselines. Each carries a superset of the sibling
// facts any of its negatives names, so a defect can prove zero effect on them.
const BASELINES: Record<string, () => Wave2Ledger> = {
  g2_1_machine_lock_and_start_dedupe: () => ({
    events: [
      ev("machine_lock", { owner: "daemon_1" }),
      ev("wake", { wakeCount: 5 }),
      ev("start_queued", { launchId: "lnc_1" }),
      ev("start_begun", { launchId: "lnc_1" }),
      ev("process_spawned", { launchId: "lnc_1" }),
      ev("runtime_ready", { launchId: "lnc_1" }),
      ev("activated", { launchId: "lnc_1" }),
      ev("daemon_accepted", { deliveryId: "dlv_1", bodyless: true }),
      ev("daemon_accepted", { deliveryId: "dlv_2", bodyless: true }),
      ev("body_permit", { deliveryId: "dlv_1" }),
    ],
    siblings: {
      journal_schema: "v2",
      recovery_effects: 0,
      process_effects: 1,
      delivery_count: 2,
      delivery_body_columns: 0,
      current_launch_id: "lnc_1",
      process_start_count: 1,
      activation_count: 1,
    },
  }),
  g2_4_readiness_and_activation_separation: () => ({
    events: [
      ev("process_spawned", { launchId: "lnc_1" }),
      ev("adapter_pump", { stateInstance: "si_1", owner: "pump_1" }),
      ev("waiter_registered", { waiter: "initialize", stateInstance: "si_1" }),
      ev("waiter_registered", { waiter: "turn_started", stateInstance: "si_1" }),
      ev("driver_handshake", { launchId: "lnc_1" }),
      ev("runtime_ready", { launchId: "lnc_1", session: "ses_1" }),
      ev("activated", { launchId: "lnc_1", session: "ses_1", routeGeneration: 1 }),
    ],
    siblings: {
      server_launch_state: "active",
      local_slot_state: "resident",
      delivery_state: "pending",
      active_route: "rte_1",
      body_permit_count: 0,
      runtime_session_ref_digest: "sha256:aa",
      server_ready_receipt: "rcp_ready_1",
      driver_event_cursor: 7,
      runtime_ready_count: 1,
      input_written_count: 0,
      model_visible_count: 0,
      visibility_ledger: "vl_1",
    },
  }),
  g2_7_notice_dedupe_and_visibility_ledger: () => ({
    events: [
      ev("input_written", { runtimeWriteId: "rw_1" }),
      ev("notice_visibility", { session: "ses_1", targetKey: "#c", membershipEpoch: 4, range: "101-104" }),
      ev("model_visible", { messageId: "msg_105" }),
    ],
    siblings: {
      notice_visibility: "one_row",
      input_written_count: 1,
      model_visible_count: 1,
      permitted_current_body: "one",
      existing_visibility_row: "vr_1",
      high_water: 105,
      reply_count: 1,
    },
  }),
  g2_9_adapter_invariance: () => ({
    events: [
      ev("input_written", { runtimeWriteId: "rw_1" }),
      ev("model_visible", { messageId: "msg_1", clientId: "cli_1", threadTurn: "tt_1" }),
      ev("reply_committed", { replyId: "rep_1" }),
    ],
    siblings: {
      server_projection: "P1",
      driver_cursor: 3,
      reply_count: 1,
      server_receipts: 2,
      visibility_ledger: "vl_1",
      logical_message_count: 1,
      model_visible_count: 1,
      task_count: 0,
      allowed_dependency_graph: "G1",
      delivery_attempt: 1,
    },
  }),
  g2_2_bounded_starts_and_fairness: () => ({
    events: [
      ev("start_queued", { agentId: "agt_2", queueOrdinal: 1 }),
      ev("start_begun", { agentId: "agt_2" }),
      ev("start_queued", { agentId: "agt_1", queueOrdinal: 2 }),
      ev("start_begun", { agentId: "agt_1" }),
    ],
    siblings: {
      queued_rows: 2,
      started_at: "fake_t0",
      process_effect_count: 2,
      fake_clock_value: 2000,
      older_agent_original_queue_ordinal: 1,
    },
  }),
  g2_3_stop_during_spawn: () => ({
    events: [
      ev("start_begun", { launchId: "lnc_1", stopEpoch: 7 }),
      ev("stop_requested", { launchId: "lnc_1", stopEpoch: 8 }),
      ev("terminal", { launchId: "lnc_1", reason: "stopped" }),
    ],
    siblings: {
      ready_count: 0,
      activation_count: 0,
      body_permit_count: 0,
      original_launch_fence: "epoch_7",
      server_launch_identity: "lnc_1",
      slot_state: "nonresident",
      launch_terminal_reason: "stopped",
      route_state: "cleared",
    },
  }),
  g2_5_delivery_visible_once_after_launch: () => ({
    events: [
      ev("daemon_accepted", { deliveryId: "dlv_1", bodyless: true }),
      ev("input_written", { runtimeWriteId: "rw_1" }),
      ev("model_visible", { messageId: "msg_1" }),
      ev("reply_committed", { replyId: "rep_1" }),
    ],
    siblings: {
      body_read_count: 1,
      native_write_count: 1,
      visibility_count: 1,
      visible_message_ids: "msg_1",
      reply_count: 1,
      consumed_count: 1,
    },
  }),
  g2_6_failed_ambiguous_and_lost_ack_writes: () => ({
    events: [
      ev("write_started", { attempt: 1 }),
      ev("input_written", { runtimeWriteId: "rw_1" }),
      ev("model_visible", { messageId: "msg_1" }),
    ],
    siblings: {
      logical_message_count: 1,
      producer_fact_count: 1,
      active_invocation: "inv_1",
      job_state: "written",
      receipt_count: 1,
      delivery_attempt: 1,
      invocation_generation: 1,
      server_projection: "P1",
    },
  }),
  g2_8_turn_state_and_steer_fences: () => ({
    events: [
      ev("turn_started", { turnId: "trn_1" }),
      ev("input_written", { runtimeWriteId: "rw_1" }),
      ev("model_visible", { messageId: "msg_1" }),
      ev("turn_completed", { turnId: "trn_1" }),
    ],
    siblings: {
      input_ordinal: 1,
      driver_write_count: 1,
      visibility_count: 1,
      delivery_state: "consumed",
      notice_visibility: "one_row",
      external_delivery_receive_ordinal: 5,
      control_request_count: 0,
      server_projection: "P1",
      stdin_write_count: 1,
      model_input_count: 1,
      active_turn_state: "completed",
    },
  }),
  g2_10_manifest_and_driver_protocol_identity: () => ({
    events: [
      ev("manifest_frozen", { digest: "sha256:m1" }),
      ev("wire_digest_bound", { digest: "sha256:w1" }),
    ],
    siblings: {
      runtime_identity_digest: "sha256:id1",
      launch_state: "active",
      process_effect_count: 1,
      frozen_driver_identity: "drv_1",
      active_turn_state: "idle",
      canonical_wire_artifact: "sha256:w1",
      cli_version: "0.145.0",
    },
  }),
  g2_11_launch_environment: () => ({
    events: [
      ev("private_home", { launchId: "lnc_1", home: "H1" }),
      ev("private_home", { launchId: "lnc_2", home: "H2" }),
      ev("launch_capability", { launchId: "lnc_1", capabilityKind: "inert" }),
    ],
    siblings: {
      process_effect_count: 2,
      launch_capability_count: 2,
      transport_contents: "reference_only",
      transport_reference_rows: 2,
    },
  }),
  g2_12_carry_gates: () => ({
    events: [
      ev("carry_gate", { name: "wave0_1", status: "green" }),
      ev("deferred_condition", { count: 7, status: "fail_closed" }),
    ],
    siblings: {
      declared_toolchain: "node24.19.0/pnpm10.15.1/pg16",
      other_six_deferred_conditions: "unchanged",
      positive_scenario_projection: "P_all",
    },
  }),
};

export class Wave2Fake {
  /** The canonical healthy ledger for a scenario group. */
  healthy(groupId: string): Wave2Ledger {
    const make = BASELINES[groupId];
    if (make === undefined) throw new Error(`no Wave2Fake baseline for group "${groupId}"`);
    return make();
  }

  /**
   * Expose a defect BY ITS DEFECT KEY ONLY. The emitted fence/observation comes
   * from the fake's own independent `DEFECT_RESPONSES` model — never from the
   * plan's expected outcome — so a caller cannot pipe the expectation into the
   * fake. The named sibling facts are held byte-unchanged (zero effect). Throws
   * if the fake has no model for the defect (the fake must be able to expose it,
   * per contract §9.1).
   */
  withDefect(groupId: string, defectKey: string): Wave2Ledger {
    const base = this.healthy(groupId);
    const response = DEFECT_RESPONSES[defectKey];
    if (response === undefined) {
      throw new Error(`Wave2Fake has no independent exposure for defect "${defectKey}"`);
    }
    const marker = ev("defect_attempt", { defect: defectKey });
    const caught: LedgerEvent[] =
      "fenceError" in response
        ? [ev("fence", { defect: defectKey, error: response.fenceError })]
        : "fenceErrors" in response
          ? response.fenceErrors.map((error) => ev("fence", { defect: defectKey, error }))
          : [ev("observe", { defect: defectKey, observe: response.observe })];
    return { events: [...base.events, marker, ...caught], siblings: { ...base.siblings } };
  }
}
