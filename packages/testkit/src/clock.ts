// Deterministic injected time source. Nothing here reads real `Date`; every
// consumer that needs "now" is handed a FakeClock so tests advance time by hand.

/** Tuning knobs that vary between accelerated test runs and production timing. */
export type ClockProfile = {
  /** Lease/notice time-to-live in milliseconds. */
  readonly ttlMs: number;
  /** Heartbeat interval in milliseconds. */
  readonly heartbeatMs: number;
};

/** Accelerated profile: 2000ms TTL / 500ms heartbeat, for fast tests. */
export const ACCELERATED_PROFILE: ClockProfile = {
  ttlMs: 2000,
  heartbeatMs: 500,
};

/** Production profile: 60000ms TTL / 15000ms heartbeat. */
export const PRODUCTION_PROFILE: ClockProfile = {
  ttlMs: 60000,
  heartbeatMs: 15000,
};

/** A wall-clock instant and a monotonic reading captured together. */
export type ClockReading = {
  /** Wall-clock milliseconds since the configured epoch. */
  readonly wallMs: number;
  /** Monotonic milliseconds since clock construction (never moves backward). */
  readonly monotonicMs: number;
};

export class FakeClock {
  readonly profile: ClockProfile;
  readonly #epochMs: number;
  #monotonicMs = 0;

  constructor(
    profile: ClockProfile = ACCELERATED_PROFILE,
    epochMs = 0,
  ) {
    this.profile = profile;
    this.#epochMs = epochMs;
  }

  /** Wall-clock milliseconds (epoch + elapsed). */
  now(): number {
    return this.#epochMs + this.#monotonicMs;
  }

  /** Monotonic milliseconds since construction. */
  monotonic(): number {
    return this.#monotonicMs;
  }

  /** Paired wall + monotonic snapshot. */
  reading(): ClockReading {
    return { wallMs: this.now(), monotonicMs: this.#monotonicMs };
  }

  /** Advance both views by a non-negative delta. */
  advance(ms: number): void {
    if (ms < 0) {
      throw new RangeError("FakeClock.advance requires a non-negative delta");
    }
    this.#monotonicMs += ms;
  }

  /** Advance the wall clock to an absolute instant; must not move backward. */
  advanceTo(wallMs: number): void {
    const delta = wallMs - this.now();
    if (delta < 0) {
      throw new RangeError("FakeClock.advanceTo cannot move time backward");
    }
    this.#monotonicMs += delta;
  }
}
