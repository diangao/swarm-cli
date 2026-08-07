import type { Seat } from "./runtime.js";

// Deterministic delivery-socket double. Delivers notice METADATA only (never a
// body). Supports holds, partitions, duplicate/concurrent wakes, and a real
// barrier so contention overlaps genuinely rather than running sequentially.

/** Notice metadata. Metadata-only by construction: there is no body field. */
export type NoticeMeta = {
  /** Neutral target label (e.g. a channel or direct marker). */
  readonly target: string;
  /** Count of pending items the notice summarizes. */
  readonly count: number;
  /** Opaque marker for correlation; carries no content. */
  readonly marker: string;
};

/** A single delivery event as observed by a seat. */
export type DeliveryEvent = {
  readonly seat: Seat;
  readonly meta: NoticeMeta;
  /** Monotonic sequence across all deliveries from this socket. */
  readonly seq: number;
  /** True for a duplicate of an already-delivered notice. */
  readonly duplicate: boolean;
};

/**
 * A barrier that N participants must all reach before any is released. The test
 * awaits `ready` to observe that every participant has genuinely arrived, then
 * the shared promise resolves and all proceed together.
 */
export type ContentionBarrier = {
  readonly size: number;
  /** Resolves once all `size` participants have arrived. */
  readonly ready: Promise<void>;
  /** Per-participant gates; each resolves only after the barrier is full. */
  readonly gates: readonly Promise<DeliveryEvent>[];
};

export class FakeSocket {
  #seq = 0;
  readonly #delivered: DeliveryEvent[] = [];
  readonly #held = new Set<Seat>();
  readonly #partitioned = new Set<Seat>();
  /** Notices buffered while a seat is held or partitioned. */
  readonly #pending: DeliveryEvent[] = [];

  /** Deliver a single notice to a seat unless held or partitioned. */
  deliverNotice(seat: Seat, meta: NoticeMeta): DeliveryEvent | undefined {
    const event: DeliveryEvent = {
      seat,
      meta,
      seq: ++this.#seq,
      duplicate: false,
    };
    if (this.#held.has(seat) || this.#partitioned.has(seat)) {
      this.#pending.push(event);
      return undefined;
    }
    this.#delivered.push(event);
    return event;
  }

  /** Hold future deliveries to a seat (they buffer until `release`). */
  holdDelivery(seat: Seat): void {
    this.#held.add(seat);
  }

  /** Deliver an exact duplicate of the most recent notice to a seat. */
  duplicateWake(seat: Seat): DeliveryEvent | undefined {
    const last = [...this.#delivered]
      .reverse()
      .find((event) => event.seat === seat);
    if (last === undefined) {
      return undefined;
    }
    const dup: DeliveryEvent = {
      seat,
      meta: last.meta,
      seq: ++this.#seq,
      duplicate: true,
    };
    if (this.#held.has(seat) || this.#partitioned.has(seat)) {
      this.#pending.push(dup);
      return undefined;
    }
    this.#delivered.push(dup);
    return dup;
  }

  /**
   * Fan a single notice out to N wakes that genuinely overlap at a shared
   * barrier. No gate resolves until all N have arrived, so a downstream claim
   * race is real rather than sequential.
   */
  concurrentWake(seat: Seat, n: number): ContentionBarrier {
    if (n < 1) {
      throw new RangeError("FakeSocket.concurrentWake requires n >= 1");
    }
    let arrived = 0;
    let openBarrier!: () => void;
    const barrierOpen = new Promise<void>((resolve) => {
      openBarrier = resolve;
    });
    let signalReady!: () => void;
    const ready = new Promise<void>((resolve) => {
      signalReady = resolve;
    });

    const gates: Array<Promise<DeliveryEvent>> = [];
    for (let index = 0; index < n; index += 1) {
      const meta: NoticeMeta = {
        target: `${seat}:contention`,
        count: 1,
        marker: `wake-${index + 1}`,
      };
      const event: DeliveryEvent = {
        seat,
        meta,
        seq: ++this.#seq,
        duplicate: index > 0,
      };
      const gate = (async (): Promise<DeliveryEvent> => {
        arrived += 1;
        if (arrived === n) {
          signalReady();
          openBarrier();
        }
        await barrierOpen;
        this.#delivered.push(event);
        return event;
      })();
      gates.push(gate);
    }

    return { size: n, ready, gates };
  }

  /** Partition a seat from the socket (deliveries buffer until `release`). */
  partition(seat: Seat): void {
    this.#partitioned.add(seat);
  }

  /** Lift all holds and partitions, flushing buffered notices in order. */
  release(): readonly DeliveryEvent[] {
    this.#held.clear();
    this.#partitioned.clear();
    const flushed = [...this.#pending];
    this.#pending.length = 0;
    for (const event of flushed) {
      this.#delivered.push(event);
    }
    return flushed;
  }

  /** All delivered events in delivery order. */
  delivered(): readonly DeliveryEvent[] {
    return this.#delivered;
  }
}
