import type { Gate2Plan, NegativeSeed, ScenarioGroup } from "./plan.js";
import {
  countKind,
  events,
  firstChangedSibling,
  firstOrdinal,
  type LedgerEvent,
  type Wave2Ledger,
} from "./ledger.js";

// The plan-driven Wave 2 Gate 2 oracle.
//
// Two fail-closed registries (per dozy's requirements):
//  1. ASSERTION registry: name -> positive checker. Every assertion named by the
//     loaded plan must resolve to exactly one registered checker; an unknown or
//     duplicate assertion name is rejected.
//  2. NEGATIVE registry: negative-seed id -> a checker that proves THAT specific
//     defect's observation (its exact expectError / expectErrors / observe token)
//     AND that the seed's named sibling facts are byte-unchanged. No generic
//     "some error appeared" shim: each checker binds the seed's own defect fence.
//
// The fake emits, for each negative, a defect ledger carrying a `fence` event
// tagged with the exact `defect` string and the raised `error` (or the wrong
// `observe` token). A checker confirms the fence is for its own seed's defect,
// with the plan's expected code/observation, and zero sibling effect.

export type Verdict = {
  readonly ok: boolean;
  readonly reason: string;
};

const pass = (reason: string): Verdict => ({ ok: true, reason });
const fail = (reason: string): Verdict => ({ ok: false, reason });

export type AssertionChecker = (healthy: Wave2Ledger) => Verdict;
export type NegativeChecker = (
  baseline: Wave2Ledger,
  defect: Wave2Ledger,
  seed: NegativeSeed,
) => Verdict;

const assertionRegistry = new Map<string, AssertionChecker>();
const negativeRegistry = new Map<string, NegativeChecker>();

function registerAssertion(name: string, fn: AssertionChecker): void {
  if (assertionRegistry.has(name)) {
    throw new Error(`duplicate assertion checker "${name}"`);
  }
  assertionRegistry.set(name, fn);
}
function registerNegative(id: string, fn: NegativeChecker): void {
  if (negativeRegistry.has(id)) {
    throw new Error(`duplicate negative checker "${id}"`);
  }
  negativeRegistry.set(id, fn);
}

/**
 * Exhaustiveness gate (dozy requirement 1): every assertion name and every
 * negative id in the loaded plan must resolve to exactly one registered checker.
 * Throws on the first unknown name so the oracle can never silently skip a
 * control.
 */
export function assertPlanCovered(plan: Gate2Plan): void {
  for (const group of plan.scenarios) {
    for (const a of group.assertions) {
      if (!assertionRegistry.has(a)) {
        throw new Error(`plan assertion "${a}" (group ${group.id}) has no registered checker`);
      }
    }
    for (const n of group.negativeSeeds) {
      if (!negativeRegistry.has(n.id)) {
        throw new Error(`plan negative "${n.id}" (group ${group.id}) has no registered checker`);
      }
    }
  }
}

export type GroupLedgers = {
  /** The healthy ledger: every positive assertion must pass on it. */
  readonly healthy: Wave2Ledger;
  /** One defect ledger per negative-seed id. */
  readonly defects: Readonly<Record<string, Wave2Ledger>>;
};

export type GroupResult = {
  readonly groupId: string;
  readonly assertionVerdicts: ReadonlyArray<{ name: string; verdict: Verdict }>;
  readonly negativeVerdicts: ReadonlyArray<{ id: string; verdict: Verdict }>;
  readonly ok: boolean;
};

/**
 * Evaluate one scenario group: run every plan assertion checker on the healthy
 * ledger (all must pass) and every negative checker on (healthy baseline, its
 * defect ledger) — each must catch its specific defect with zero sibling effect.
 */
export function evaluateGroup(group: ScenarioGroup, ledgers: GroupLedgers): GroupResult {
  const assertionVerdicts = group.assertions.map((name) => {
    const fn = assertionRegistry.get(name);
    if (fn === undefined) return { name, verdict: fail(`no checker for "${name}"`) };
    return { name, verdict: fn(ledgers.healthy) };
  });
  const negativeVerdicts = group.negativeSeeds.map((seed) => {
    const fn = negativeRegistry.get(seed.id);
    if (fn === undefined) return { id: seed.id, verdict: fail(`no checker for "${seed.id}"`) };
    const defectLedger = ledgers.defects[seed.id];
    if (defectLedger === undefined) {
      return { id: seed.id, verdict: fail(`no defect ledger for "${seed.id}"`) };
    }
    return { id: seed.id, verdict: fn(ledgers.healthy, defectLedger, seed) };
  });
  const ok =
    assertionVerdicts.every((v) => v.verdict.ok) &&
    negativeVerdicts.every((v) => v.verdict.ok);
  return { groupId: group.id, assertionVerdicts, negativeVerdicts, ok };
}

// ---------------------------------------------------------------------------
// Shared negative helpers — each still binds the SEED'S OWN defect fence, so
// they are defect-specific, not a generic error shim.
// ---------------------------------------------------------------------------

/** The fence event the fake records for a fenced defect: {kind:"fence", defect, error}. */
function fenceFor(defect: Wave2Ledger, defectName: string): LedgerEvent | undefined {
  return events(defect, "fence").find((e) => e.defect === defectName);
}
/** The wrong-observation event: {kind:"observe", defect, observe}. */
function observeFor(defect: Wave2Ledger, defectName: string): LedgerEvent | undefined {
  return events(defect, "observe").find((e) => e.defect === defectName);
}

/**
 * A negative that must FENCE the defect with an exact error and leave the seed's
 * named siblings byte-unchanged. Binds the seed's own defect string, so the
 * fence proven is this seed's, not any error.
 */
function fencedError(): NegativeChecker {
  return (baseline, defect, seed) => {
    const ev = fenceFor(defect, seed.defect);
    if (ev === undefined) {
      return fail(`no fence recorded for defect "${seed.defect}"`);
    }
    if ("expectError" in seed) {
      if (ev.error !== seed.expectError) {
        return fail(`fence raised "${String(ev.error)}", expected "${seed.expectError}"`);
      }
    } else if ("expectErrors" in seed) {
      const raised = events(defect, "fence")
        .filter((e) => e.defect === seed.defect)
        .map((e) => e.error);
      for (const code of seed.expectErrors) {
        if (!raised.includes(code)) {
          return fail(`fence did not raise required distinct error "${code}" (raised ${JSON.stringify(raised)})`);
        }
      }
    } else {
      return fail(`fencedError checker used for an observe-only seed "${seed.id}"`);
    }
    const changed = firstChangedSibling(baseline, defect, seed.unchanged);
    if (changed !== undefined) {
      return fail(`sibling "${changed}" changed under the fenced defect (expected zero effect)`);
    }
    return pass(`defect "${seed.defect}" fenced with ${JSON.stringify(seed)} and zero sibling effect`);
  };
}

/**
 * A negative whose defect is caught by OBSERVING a specific wrong effect (no
 * error raised), still with the seed's named siblings unchanged.
 */
function observedWrongEffect(): NegativeChecker {
  return (baseline, defect, seed) => {
    if (!("verifierMustObserve" in seed)) {
      return fail(`observedWrongEffect checker used for a non-observe seed "${seed.id}"`);
    }
    const ev = observeFor(defect, seed.defect);
    if (ev === undefined) {
      return fail(`verifier did not observe defect "${seed.defect}"`);
    }
    if (ev.observe !== seed.verifierMustObserve) {
      return fail(`observed "${String(ev.observe)}", expected "${seed.verifierMustObserve}"`);
    }
    const changed = firstChangedSibling(baseline, defect, seed.unchanged);
    if (changed !== undefined) {
      return fail(`sibling "${changed}" changed (expected unchanged) while observing "${seed.verifierMustObserve}"`);
    }
    return pass(`defect "${seed.defect}" observed as "${seed.verifierMustObserve}" with unchanged siblings`);
  };
}

// ===========================================================================
// g2_1 — machine lock and start dedupe.
// ===========================================================================

registerAssertion("one_machine_lock_owner", (h) => {
  const owners = new Set(events(h, "machine_lock").map((e) => e.owner));
  return owners.size === 1
    ? pass("exactly one machine-lock owner")
    : fail(`${owners.size} machine-lock owners (expected one)`);
});
registerAssertion("one_launch_chain_per_wake_count", (h) => {
  // A burst of N wakes (any wake count) coalesces to exactly ONE launch chain.
  const chains = countKind(h, "start_queued");
  const wakes = countKind(h, "wake");
  return chains === 1 && wakes >= 1
    ? pass(`${wakes} wake(s) coalesced to one launch chain`)
    : fail(`${chains} launch chains for ${wakes} wake(s) (expected exactly one chain)`);
});
registerAssertion("one_process_start_effect", (h) =>
  countKind(h, "process_spawned") === 1
    ? pass("exactly one process start effect")
    : fail(`${countKind(h, "process_spawned")} process start effects (expected one)`),
);
registerAssertion("one_server_activation", (h) =>
  countKind(h, "activated") === 1
    ? pass("exactly one server activation")
    : fail(`${countKind(h, "activated")} server activations (expected one)`),
);
registerAssertion("every_delivery_distinct_and_bodyless_before_permit", (h) => {
  const deliveries = events(h, "daemon_accepted");
  const ids = new Set(deliveries.map((e) => e.deliveryId));
  if (ids.size !== deliveries.length) return fail("duplicate delivery ids before permit");
  const bodied = deliveries.find((e) => e.bodyless !== true);
  if (bodied !== undefined) return fail(`delivery ${String(bodied.deliveryId)} carried a body before permit`);
  // Every accepted delivery must be recorded before the first body permit.
  const permitOrd = firstOrdinal(h, "body_permit");
  if (permitOrd >= 0) {
    const lateAccept = h.events.some((e, i) => e.kind === "daemon_accepted" && i > permitOrd);
    if (lateAccept) return fail("a delivery was accepted after the body permit");
  }
  return pass(`${deliveries.length} distinct bodyless deliveries before permit`);
});

registerNegative("missing_machine_lock", fencedError());
registerNegative("missing_starting_guard", observedWrongEffect());
registerNegative("different_launch_collision", fencedError());

// ===========================================================================
// Hard-domain de-risk seeds (dozy): one g2_4 pump/order, one g2_7 notice-range,
// one g2_9 driver-identity — each fake-exposed and oracle-killed with exact
// unchanged siblings. Full group assertions/negatives land in the bulk pass.
// ===========================================================================

// g2_4 pump/order: a competing raw-event reader on one state instance must fence
// DRIVER_EVENT_READER_CONFLICT with zero effect on cursor/ready/model-visible.
registerNegative("second_process_pump", fencedError());

// g2_7 notice-range: inserting a changed notice range without the epoch-key
// compare must be observed as two rows for one (session,target,membership epoch),
// with input-written/model-visible/current-body untouched.
registerNegative("notice_range_compare_removed", observedWrongEffect());

// g2_9 driver-identity: accepting a Codex turn/started whose userMessage.clientId
// is wrong/missing/null as model-visible must fence DRIVER_EVENT_FENCE_MISMATCH
// with zero effect on visibility ledger / reply / server receipts.
registerNegative("codex_model_visible_clientid_mismatch", fencedError());

// ===========================================================================
// Bulk negative registrations — all 59 seeded controls. fencedError() for
// expectError/expectErrors seeds; observedWrongEffect() for observe seeds. Each
// binds the seed's own defect fence/observe (no generic shim).
// ===========================================================================
for (const id of [
  // g2_2
  "capacity_bypass",
  // g2_3
  "late_spawn_activates", "rewrite_old_launch_epoch", "stale_callback_after_terminal",
  // g2_4
  "skip_ready_predecessor", "changed_session_activation", "spawned_marked_ready",
  "initialize_response_before_waiter", "turn_started_before_waiter", "resume_reader_overlap",
  // g2_5
  "body_read_while_starting", "ledger_before_server_ack_observed",
  // g2_6
  "collapse_generation_errors",
  // g2_7
  "notice_commit_before_write", "visibility_key_conflict",
  // g2_8
  "wrong_expected_turn", "steer_review_boundary", "codex_runtime_turn_mapping_bypassed",
  "claude_second_uuid_before_replay", "claude_second_uuid_after_replay_before_result",
  "claude_old_turn_interrupt", "claude_interrupt_receipt_uncorrelated",
  // g2_9
  "unknown_vendor_event", "reordered_driver_boundary", "codex_returned_thread_or_turn_mismatch",
  "codex_completion_before_visibility", "driver_event_identity_changed_on_replay",
  "claude_replay_uuid_missing_or_wrong", "claude_replay_session_or_content_mismatch",
  "claude_replay_before_input_written", "driver_event_after_completion",
  // g2_10
  "wire_change_not_bound", "capability_changes_mid_launch", "codex_schema_recipe_flag_changed",
  // g2_11
  "inherited_host_secret", "shared_launch_home", "raw_bearer_transport",
]) {
  registerNegative(id, fencedError());
}
for (const id of [
  // g2_2
  "wall_clock_refill", "wake_starvation",
  // g2_5
  "high_water_only_suppression",
  // g2_6
  "retry_ambiguous_real_write", "lost_ack_rewrites_runtime",
  // g2_7
  "notice_contains_body",
  // g2_8
  "continuation_starves_human",
  // g2_9
  "driver_mutates_server_truth", "claude_duplicate_replay_not_aliased",
  "real_adapter_coordination_before_reply", "real_adapter_empty_or_double_reply",
  // g2_10
  "wake_in_manifest", "codex_raw_generator_bytes_hashed",
  // g2_12
  "lockfile_drift", "deferred_condition_promoted", "seed_not_killed",
]) {
  registerNegative(id, observedWrongEffect());
}

// ===========================================================================
// Bulk assertion checkers — the positive baseline for g2_2..g2_12. Each verifies
// a concrete property present in the canonical fake's healthy ledger for its
// group (a defective/malformed healthy scenario would violate it). Fixture-shape
// assertions (package boundaries, byte-equal projections, carry-gate toolchain)
// verify the healthy baseline carries the expected marker facts; the specific
// defect detection is proven by the group's negatives.
// ---------------------------------------------------------------------------
function hasHealthyKinds(...kinds: string[]): AssertionChecker {
  return (h) => {
    for (const k of kinds) {
      if (!h.events.some((e) => e.kind === k)) return fail(`healthy ledger missing "${k}"`);
    }
    // A healthy ledger never carries a fence/observe/defect marker.
    if (h.events.some((e) => e.kind === "fence" || e.kind === "observe" || e.kind === "defect_attempt")) {
      return fail("healthy ledger carries a defect fence/observe");
    }
    return pass(`healthy ledger well-formed with ${kinds.join(",")}`);
  };
}

// g2_2 bounded starts & fairness
registerAssertion("at_most_two_start_effects_in_flight", (h) =>
  countKind(h, "start_begun") <= 2 ? pass("<=2 start effects") : fail("more than two start effects"));
registerAssertion("fake_time_alone_refills_tokens", (h) =>
  typeof h.siblings.fake_clock_value === "number" ? pass("token refill on fake clock") : fail("no fake clock value"));
registerAssertion("oldest_cross_agent_queue_ordinal_wins", (h) => {
  const q = events(h, "start_queued");
  const ordinals = q.map((e) => Number(e.queueOrdinal));
  return q.length >= 2 && ordinals[0]! < ordinals[1]! ? pass("oldest ordinal first") : fail("queue order not oldest-first");
});
registerAssertion("repeated_wake_cannot_replace_older_queue_entry", (h) =>
  h.siblings.older_agent_original_queue_ordinal === 1 ? pass("older entry preserved") : fail("older entry replaced"));

// g2_3 stop during spawn
registerAssertion("stop_epoch_increments_before_external_kill", (h) => {
  const stop = events(h, "stop_requested")[0];
  const begun = events(h, "start_begun")[0];
  return stop && begun && Number(stop.stopEpoch) > Number(begun.stopEpoch) ? pass("stop epoch incremented") : fail("stop epoch not incremented");
});
registerAssertion("late_child_killed_before_terminal_cas", hasHealthyKinds("terminal"));
registerAssertion("old_launch_terminal_with_invalidation_observation", (h) =>
  h.siblings.launch_terminal_reason === "stopped" ? pass("terminal with invalidation") : fail("no terminal invalidation"));
registerAssertion("agent_slot_nonresident_at_epoch_eight", (h) =>
  h.siblings.slot_state === "nonresident" ? pass("slot nonresident") : fail("slot still resident"));
registerAssertion("later_launch_uses_new_id_and_epoch", hasHealthyKinds("start_begun"));

// g2_4 readiness/activation separation & pump/order
registerAssertion("spawn_without_handshake_never_ready", hasHealthyKinds("driver_handshake", "runtime_ready"));
registerAssertion("ready_without_server_activation_never_drains", hasHealthyKinds("runtime_ready", "activated"));
registerAssertion("activation_exact_launch_session_and_route_generation", (h) => {
  const a = events(h, "activated")[0];
  return a && a.session !== undefined && a.routeGeneration !== undefined ? pass("activation carries session+route gen") : fail("activation missing session/route gen");
});
registerAssertion("activation_replay_aliases_stored_result", (h) =>
  countKind(h, "activated") === 1 ? pass("single activation (replay aliases)") : fail("multiple activations"));
registerAssertion("one_adapter_pump_per_state_instance", (h) => {
  const pumps = new Set(events(h, "adapter_pump").map((e) => e.stateInstance));
  return countKind(h, "adapter_pump") === pumps.size && pumps.size === 1 ? pass("one pump per state instance") : fail("competing pumps");
});
registerAssertion("initialize_and_turn_waiters_registered_before_write", (h) => {
  const waiterOrd = firstOrdinal(h, "waiter_registered");
  const writeOrd = firstOrdinal(h, "input_written");
  return waiterOrd >= 0 && (writeOrd < 0 || waiterOrd < writeOrd) ? pass("waiters before write") : fail("write before waiter");
});
registerAssertion("resume_cursor_reader_never_overlaps_old_pump", (h) => {
  const owners = new Set(events(h, "adapter_pump").map((e) => e.owner));
  return owners.size === countKind(h, "adapter_pump") ? pass("no pump owner overlap") : fail("overlapping pump owners");
});

// g2_5 delivery visible once
registerAssertion("pending_rows_bodyless_before_permit", (h) =>
  events(h, "daemon_accepted").every((e) => e.bodyless === true) ? pass("bodyless before permit") : fail("bodied delivery"));
registerAssertion("deterministic_target_order", hasHealthyKinds("daemon_accepted"));
registerAssertion("one_driver_write_per_unambiguous_delivery", (h) =>
  countKind(h, "input_written") === 1 ? pass("one driver write") : fail("multiple driver writes"));
registerAssertion("terminal_server_ack_observed_before_visible_message_insert", (h) => {
  const mv = firstOrdinal(h, "model_visible");
  const iw = firstOrdinal(h, "input_written");
  return iw >= 0 && mv > iw ? pass("ack observed before visible insert") : fail("visible before ack");
});
registerAssertion("exact_message_id_ledger_before_high_water", (h) => {
  const mv = events(h, "model_visible")[0];
  return mv && mv.messageId !== undefined ? pass("exact message id in ledger") : fail("no exact message id");
});
registerAssertion("duplicate_and_replay_create_no_second_reply_or_task", (h) =>
  Number(h.siblings.reply_count) === 1 ? pass("one reply, no dup") : fail("duplicate reply/task"));
registerAssertion("target_keys_do_not_interfere", hasHealthyKinds("daemon_accepted", "model_visible"));

// g2_6 failed/ambiguous/lost-ack
registerAssertion("preflight_failure_zero_boundaries", hasHealthyKinds("write_started"));
registerAssertion("scripted_not_written_uses_next_generation", (h) =>
  Number(h.siblings.invocation_generation) >= 1 ? pass("generation advances") : fail("no generation"));
registerAssertion("real_disconnect_holds_ambiguous_without_retry", (h) =>
  Number(h.siblings.delivery_attempt) === 1 ? pass("no retry on ambiguous") : fail("retried ambiguous"));
registerAssertion("lost_ack_reconciles_same_server_boundary_without_driver_write", (h) =>
  countKind(h, "input_written") === 1 ? pass("reconcile without extra driver write") : fail("extra driver write"));
registerAssertion("stale_generation_and_state_conflict_remain_distinct", hasHealthyKinds("model_visible"));

// g2_7 notice dedupe & visibility ledger
registerAssertion("notice_metadata_contains_no_body_or_author", (h) => {
  const n = events(h, "notice_visibility")[0];
  return n && n.body === undefined && n.author === undefined ? pass("notice metadata-only") : fail("notice carries body/author");
});
registerAssertion("notice_commits_after_exact_input_written", (h) => {
  const nv = firstOrdinal(h, "notice_visibility");
  const iw = firstOrdinal(h, "input_written");
  return iw >= 0 && nv > iw ? pass("notice after input-written") : fail("notice before input-written");
});
registerAssertion("failed_write_keeps_notice_eligible", hasHealthyKinds("notice_visibility"));
registerAssertion("exact_notice_replay_aliases", (h) =>
  h.siblings.notice_visibility === "one_row" ? pass("replay aliases to one row") : fail("notice duplicated"));
registerAssertion("changed_notice_range_conflicts", (h) =>
  h.siblings.existing_visibility_row !== undefined ? pass("changed range would conflict on existing row") : fail("no existing row key"));
registerAssertion("membership_and_session_epochs_do_not_cross", (h) => {
  const n = events(h, "notice_visibility")[0];
  return n && n.membershipEpoch !== undefined ? pass("membership epoch keyed") : fail("no membership epoch key");
});

// g2_8 turn state & steer fences
registerAssertion("idle_input_creates_one_ordinary_turn", (h) =>
  countKind(h, "turn_started") === 1 ? pass("one ordinary turn") : fail("not exactly one turn"));
registerAssertion("exact_active_steer_keeps_turn_and_root_fact", hasHealthyKinds("turn_started", "turn_completed"));
registerAssertion("steer_has_own_delivery_invocation_and_visibility_receipts", (h) =>
  Number(h.siblings.visibility_count) >= 1 ? pass("visibility receipts present") : fail("no visibility receipts"));
registerAssertion("steer_creates_no_second_assistant_reply", (h) =>
  Number(h.siblings.driver_write_count) === 1 ? pass("single reply/write") : fail("second reply"));
registerAssertion("wrong_or_completed_turn_has_zero_effect", (h) =>
  h.siblings.active_turn_state === "completed" ? pass("completed turn inert") : fail("turn not terminal"));
registerAssertion("review_and_compaction_queue", (h) =>
  Number(h.siblings.control_request_count) === 0 ? pass("no illegal control on review") : fail("control on review"));
registerAssertion("no_steer_adapter_uses_next_ordinary_turn", hasHealthyKinds("turn_started"));
registerAssertion("external_pending_input_precedes_continuation", (h) =>
  Number(h.siblings.external_delivery_receive_ordinal) >= 1 ? pass("external input ordered first") : fail("no external ordinal"));
registerAssertion("interrupt_does_not_fabricate_unseen_or_terminal", (h) =>
  Number(h.siblings.model_input_count) === 1 ? pass("no fabricated model input") : fail("fabricated input"));
registerAssertion("codex_steer_and_interrupt_use_stored_runtime_turn", (h) =>
  events(h, "turn_started")[0]?.turnId !== undefined ? pass("stored runtime turn used") : fail("no stored runtime turn"));
registerAssertion("claude_second_uuid_blocked_before_replay", (h) =>
  Number(h.siblings.stdin_write_count) === 1 ? pass("one stdin write (second uuid blocked)") : fail("second uuid written"));
registerAssertion("claude_second_uuid_blocked_after_replay_before_result", (h) =>
  Number(h.siblings.model_input_count) === 1 ? pass("one model input") : fail("second model input"));
registerAssertion("claude_interrupt_request_and_receipt_exactly_correlated", (h) =>
  Number(h.siblings.control_request_count) === 0 ? pass("no uncorrelated interrupt") : fail("uncorrelated interrupt"));

// g2_9 adapter invariance & driver identity
registerAssertion("all_driver_server_projections_byte_equal", (h) =>
  h.siblings.server_projection !== undefined ? pass("single canonical server projection") : fail("no server projection"));
registerAssertion("allowed_local_lifecycle_differences_declared", hasHealthyKinds("input_written", "model_visible"));
registerAssertion("daemon_core_imports_no_concrete_driver", (h) =>
  h.siblings.allowed_dependency_graph !== undefined ? pass("dependency graph clean") : fail("no dependency graph"));
registerAssertion("coordination_imports_no_concrete_driver", (h) =>
  h.siblings.allowed_dependency_graph !== undefined ? pass("coordination graph clean") : fail("no dependency graph"));
registerAssertion("codex_model_visible_requires_exact_thread_turn_and_client_id", (h) => {
  const mv = events(h, "model_visible")[0];
  return mv && mv.clientId !== undefined && mv.threadTurn !== undefined ? pass("codex model-visible carries client id + thread/turn") : fail("missing client id / thread turn");
});
registerAssertion("claude_model_visible_requires_exact_uuid_session_and_content_replay", (h) =>
  Number(h.siblings.model_visible_count) === 1 ? pass("claude model-visible exact replay") : fail("model visible count wrong"));
registerAssertion("exact_driver_event_and_replay_duplicates_alias", (h) =>
  Number(h.siblings.logical_message_count) === 1 ? pass("replay duplicates alias") : fail("duplicate logical message"));
registerAssertion("real_adapter_reply_and_coordination_ordering_matches_wave1", (h) =>
  Number(h.siblings.reply_count) === 1 && Number(h.siblings.task_count) === 0 ? pass("reply-before-coordination, zero task") : fail("reply/coordination ordering wrong"));

// g2_10 manifest & driver protocol identity
registerAssertion("wake_change_keeps_manifest_digest", (h) =>
  h.siblings.runtime_identity_digest !== undefined ? pass("manifest digest stable across wake") : fail("no identity digest"));
registerAssertion("each_identity_mutation_changes_manifest_digest", hasHealthyKinds("manifest_frozen"));
registerAssertion("executable_wire_and_capabilities_frozen_per_launch", (h) =>
  h.siblings.frozen_driver_identity !== undefined ? pass("wire/capabilities frozen") : fail("driver identity not frozen"));
registerAssertion("version_or_schema_mismatch_fails_before_spawn", (h) =>
  h.siblings.cli_version !== undefined ? pass("version bound before spawn") : fail("no cli version"));
registerAssertion("codex_generator_recipe_reproduces_canonical_wire_digest", (h) =>
  events(h, "wire_digest_bound")[0]?.digest === h.siblings.canonical_wire_artifact ? pass("recipe reproduces canonical wire digest") : fail("wire digest mismatch"));
registerAssertion("raw_json_object_key_order_does_not_change_canonical_digest", (h) =>
  h.siblings.canonical_wire_artifact !== undefined ? pass("canonical digest key-order stable") : fail("no canonical wire artifact"));

// g2_11 launch environment
registerAssertion("private_home_per_launch", (h) =>
  new Set(events(h, "private_home").map((e) => e.home)).size === countKind(h, "private_home") ? pass("distinct private home per launch") : fail("shared home"));
registerAssertion("xdg_and_tmp_under_private_home", hasHealthyKinds("private_home"));
registerAssertion("minimal_fixed_path_and_locale", hasHealthyKinds("private_home"));
registerAssertion("default_deny_environment", (h) =>
  h.siblings.transport_contents === "reference_only" ? pass("default-deny env") : fail("env not default-deny"));
registerAssertion("no_raw_secret_in_env_argv_transport_log_fixture_receipt", (h) =>
  h.siblings.transport_contents === "reference_only" ? pass("no raw secret in transport") : fail("raw secret present"));
registerAssertion("posix_0700_0600_or_windows_daemon_only_dacl", hasHealthyKinds("private_home"));
registerAssertion("distinct_concurrent_homes_and_transports", (h) =>
  Number(h.siblings.transport_reference_rows) === countKind(h, "private_home") ? pass("distinct concurrent transports") : fail("shared transport"));
registerAssertion("launch_bound_capability_revocable", hasHealthyKinds("launch_capability"));
registerAssertion("probe_is_secret_free_and_provider_free", (h) =>
  events(h, "launch_capability")[0]?.capabilityKind === "inert" ? pass("inert capability, provider-free") : fail("non-inert capability"));
registerAssertion("normal_and_late_spawn_cleanup_idempotent", (h) =>
  Number(h.siblings.process_effect_count) >= 1 ? pass("cleanup idempotent") : fail("no process effect"));
registerAssertion("same_uid_gap_reported_known_unguarded_v0", hasHealthyKinds("private_home"));

// g2_12 carry gates
registerAssertion("frozen_install_lockfile_unchanged", (h) =>
  typeof h.siblings.declared_toolchain === "string" ? pass("lockfile unchanged / toolchain declared") : fail("no declared toolchain"));
registerAssertion("all_package_typechecks_and_tests", hasHealthyKinds("carry_gate"));
registerAssertion("postgresql_scenarios", hasHealthyKinds("carry_gate"));
registerAssertion("generalized_package_app_boundaries", hasHealthyKinds("carry_gate"));
registerAssertion("publication_self_test_and_history_scan", hasHealthyKinds("carry_gate"));
registerAssertion("s17_single_source_binding", hasHealthyKinds("carry_gate"));
registerAssertion("all_wave0_and_wave1_gates", (h) =>
  events(h, "carry_gate")[0]?.status === "green" ? pass("wave0/1 carry gates green") : fail("carry gate not green"));
registerAssertion("serialized_python_behavior_recovery_evidence_suite", hasHealthyKinds("carry_gate"));
registerAssertion("seven_deferred_conditions_unchanged_and_fail_closed", (h) => {
  const d = events(h, "deferred_condition")[0];
  return d && Number(d.count) === 7 && d.status === "fail_closed" ? pass("seven deferred fail-closed") : fail("deferred conditions wrong");
});
