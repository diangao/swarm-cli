import type { AgentId, LaunchId, SessionId, TurnId } from "@swarm/protocol";
import {
  mintAgentId,
  mintLaunchId,
  mintSessionId,
  mintTurnId,
} from "./ids.js";
import type { FakeClock } from "./clock.js";

// Deterministic runtime double. It models what a real harness would drive across
// launch -> resume -> turn, but NEVER fabricates ledger rows. It only records the
// launch/turn structure a real driver would produce, measured against an injected
// FakeClock.

/** A logical seat that a session runs under. */
export type Seat = "owner" | "worker-1" | "worker-2" | "verifier";

/** Terminal verdict for a completed turn. */
export type TurnVerdict = "continue" | "complete" | "held" | "failed";

/** Named points inside a turn where an abrupt kill may be injected. */
export type KillPoint =
  | "before_owner_read"
  | "after_owner_read"
  | "before_typed_commit"
  | "after_typed_commit";

/** Where a steer may be injected relative to turn boundaries. */
export type SteerInjectionPoint = "between_turns" | "mid_turn";

/** A steer carrying the turn it expects to land in (wrong-turn detection). */
export type SteerVector = {
  readonly injectionPoint: SteerInjectionPoint;
  readonly expectedTurnId: TurnId;
  /** Neutral marker for the steer; carries no body content. */
  readonly marker: string;
};

/** Spec handed to `launch` describing the session to stand up. */
export type SessionSpec = {
  /** Default duration (measured on the injected clock) for turns in ms. */
  readonly turnDurationMs: number;
  /** Optional caller-provided launch id; one is minted otherwise. */
  readonly launchId?: LaunchId;
  /** Optional caller-provided session id; one is minted otherwise. */
  readonly sessionId?: SessionId;
};

/** A launched session. */
export type FakeSession = {
  readonly seat: Seat;
  readonly agentId: AgentId;
  readonly launchId: LaunchId;
  readonly sessionId: SessionId;
  readonly turnDurationMs: number;
  /** Monotonic count of resumes performed against this session. */
  readonly resumeCount: number;
};

/** One scripted step inside a turn. */
export type TurnStep = {
  /** Neutral label for the step (no lineage, no body). */
  readonly label: string;
  /** Duration this step consumes on the injected clock, in ms. */
  readonly durationMs: number;
};

/** A turn record. Carries the session id for continuity assertions. */
export type FakeTurn = {
  readonly turnId: TurnId;
  readonly sessionId: SessionId;
  readonly seat: Seat;
  /** Turn ordinal within the session, starting at 1. */
  readonly ordinal: number;
  readonly startedAtMs: number;
  readonly plannedDurationMs: number;
  readonly steps: readonly TurnStep[];
  readonly steers: readonly SteerVector[];
  readonly killPoint?: KillPoint;
  readonly killedAbruptly: boolean;
  readonly endedAtMs?: number;
  readonly verdict?: TurnVerdict;
};

type SessionState = {
  session: FakeSession;
  ordinal: number;
  killed: boolean;
};

export class FakeRuntime {
  readonly #clock: FakeClock;
  readonly #sessions = new Map<SessionId, SessionState>();
  readonly #turns = new Map<TurnId, FakeTurn>();
  /**
   * Stable seat -> AgentId map. A seat gets a grammar-valid `agt_` id minted
   * once and reused for every launch under that seat within this runtime, so
   * agent identity is stable per seat (the previous `agent-${seat}` value was
   * not a valid Protocol AgentId).
   */
  readonly #seatAgents = new Map<Seat, AgentId>();

  constructor(clock: FakeClock) {
    this.#clock = clock;
  }

  /** Stand up a session under a seat. */
  launch(seat: Seat, sessionSpec: SessionSpec): FakeSession {
    const launchId =
      sessionSpec.launchId ?? mintLaunchId();
    const sessionId =
      sessionSpec.sessionId ?? mintSessionId();
    const session: FakeSession = {
      seat,
      agentId: this.#seatAgentId(seat),
      launchId,
      sessionId,
      turnDurationMs: sessionSpec.turnDurationMs,
      resumeCount: 0,
    };
    this.#sessions.set(sessionId, { session, ordinal: 0, killed: false });
    return session;
  }

  /**
   * Resume a session into a fresh turn. Session-id CONTINUITY is guaranteed:
   * the returned turn carries the same sessionId as the session.
   */
  resume(session: FakeSession): FakeTurn {
    const state = this.#sessions.get(session.sessionId);
    if (state === undefined) {
      throw new Error("FakeRuntime.resume: unknown session");
    }
    if (state.killed) {
      throw new Error("FakeRuntime.resume: session was killed");
    }
    state.ordinal += 1;
    const resumed: FakeSession = {
      ...state.session,
      resumeCount: state.session.resumeCount + 1,
    };
    state.session = resumed;
    const turn: FakeTurn = {
      turnId: mintTurnId(),
      sessionId: session.sessionId,
      seat: session.seat,
      ordinal: state.ordinal,
      startedAtMs: this.#clock.now(),
      plannedDurationMs: session.turnDurationMs,
      steps: [],
      steers: [],
      killedAbruptly: false,
    };
    this.#turns.set(turn.turnId, turn);
    return turn;
  }

  /** Attach a scripted sequence of steps to a turn; advances the clock. */
  script(turn: FakeTurn, steps: readonly TurnStep[]): FakeTurn {
    const current = this.#requireLiveTurn(turn.turnId);
    for (const step of steps) {
      this.#clock.advance(step.durationMs);
    }
    const updated: FakeTurn = { ...current, steps: [...current.steps, ...steps] };
    this.#turns.set(turn.turnId, updated);
    return updated;
  }

  /** Inject a steer into a turn at the requested point. */
  steer(turn: FakeTurn, vector: SteerVector): FakeTurn {
    const current = this.#requireLiveTurn(turn.turnId);
    const updated: FakeTurn = {
      ...current,
      steers: [...current.steers, vector],
    };
    this.#turns.set(turn.turnId, updated);
    return updated;
  }

  /**
   * True iff the steer's expectedTurnId does not match the turn it landed in.
   * Lets a test detect a wrong-turn steer without inspecting internals.
   */
  isWrongTurnSteer(turn: FakeTurn, vector: SteerVector): boolean {
    return vector.expectedTurnId !== turn.turnId;
  }

  /** Abruptly kill a session's process group (SIGKILL semantics). */
  killGroup(session: FakeSession, killPoint?: KillPoint): void {
    const state = this.#sessions.get(session.sessionId);
    if (state === undefined) {
      throw new Error("FakeRuntime.killGroup: unknown session");
    }
    state.killed = true;
    for (const turn of this.#turns.values()) {
      if (turn.sessionId === session.sessionId && turn.verdict === undefined) {
        const killed: FakeTurn = {
          ...turn,
          killedAbruptly: true,
          endedAtMs: this.#clock.now(),
          ...(killPoint === undefined ? {} : { killPoint }),
        };
        this.#turns.set(turn.turnId, killed);
      }
    }
  }

  /** Complete a turn with a verdict; advances the clock to fill any remainder. */
  completeTurn(turn: FakeTurn, verdict: TurnVerdict): FakeTurn {
    const current = this.#requireLiveTurn(turn.turnId);
    const elapsed = this.#clock.now() - current.startedAtMs;
    const remainder = current.plannedDurationMs - elapsed;
    if (remainder > 0) {
      this.#clock.advance(remainder);
    }
    const completed: FakeTurn = {
      ...current,
      verdict,
      endedAtMs: this.#clock.now(),
    };
    this.#turns.set(turn.turnId, completed);
    return completed;
  }

  /** Read back the latest recorded state of a turn. */
  turn(turnId: TurnId): FakeTurn | undefined {
    return this.#turns.get(turnId);
  }

  #requireLiveTurn(turnId: TurnId): FakeTurn {
    const current = this.#turns.get(turnId);
    if (current === undefined) {
      throw new Error("FakeRuntime: unknown turn");
    }
    if (current.killedAbruptly) {
      throw new Error("FakeRuntime: turn was killed abruptly");
    }
    if (current.verdict !== undefined) {
      throw new Error("FakeRuntime: turn already completed");
    }
    return current;
  }

  #seatAgentId(seat: Seat): AgentId {
    const existing = this.#seatAgents.get(seat);
    if (existing !== undefined) {
      return existing;
    }
    const minted = mintAgentId();
    this.#seatAgents.set(seat, minted);
    return minted;
  }
}
