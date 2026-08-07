import type { KillPoint } from "./runtime.js";

// A named, addressable registry of deterministic fault injections. Each injector
// is a typed factory that yields a plain descriptor of the fault a test wants to
// drive. No production business logic lives here; these describe fault shapes.

/** Shapes a plan can be malformed into. */
export type MalformedPlanShape =
  | "object-acceptance"
  | "over-budget"
  | "unknown-capability"
  | "duplicate-key";

/** Points at which a native turn can be intercepted before it runs. */
export type NativeInterceptPoint =
  | "before_context_compile"
  | "before_manifest_freeze";

/** Discriminated set of fault descriptors, addressable by `name`. */
export type Fault =
  | { readonly name: "leaseExpiryUnderRunningTurn" }
  | { readonly name: "processKillMidTurn"; readonly point: KillPoint }
  | { readonly name: "emptyBodySubmission" }
  | { readonly name: "malformedPlan"; readonly shape: MalformedPlanShape }
  | { readonly name: "lateOldAttemptWrite" }
  | { readonly name: "wholeRowRegistryWrite" }
  | { readonly name: "staleWakeOrphan" }
  | { readonly name: "writeRegistryDuringFreezeWindow" }
  | { readonly name: "interceptBeforeNativeTurn"; readonly point: NativeInterceptPoint }
  | { readonly name: "concurrentWakeDoubleSpawn" }
  | { readonly name: "failedNativeWriteThenAdvance" };

/** Every fault name (the registry's address space). */
export type FaultName = Fault["name"];

/**
 * Typed fault injectors. Each returns a descriptor whose `name` addresses it in
 * the registry. Parameterized faults take their parameter as an argument.
 */
export const DeterministicFaults = {
  leaseExpiryUnderRunningTurn(): Fault {
    return { name: "leaseExpiryUnderRunningTurn" };
  },
  processKillMidTurn(point: KillPoint): Fault {
    return { name: "processKillMidTurn", point };
  },
  emptyBodySubmission(): Fault {
    return { name: "emptyBodySubmission" };
  },
  malformedPlan(shape: MalformedPlanShape): Fault {
    return { name: "malformedPlan", shape };
  },
  lateOldAttemptWrite(): Fault {
    return { name: "lateOldAttemptWrite" };
  },
  wholeRowRegistryWrite(): Fault {
    return { name: "wholeRowRegistryWrite" };
  },
  staleWakeOrphan(): Fault {
    return { name: "staleWakeOrphan" };
  },
  writeRegistryDuringFreezeWindow(): Fault {
    return { name: "writeRegistryDuringFreezeWindow" };
  },
  interceptBeforeNativeTurn(point: NativeInterceptPoint): Fault {
    return { name: "interceptBeforeNativeTurn", point };
  },
  concurrentWakeDoubleSpawn(): Fault {
    return { name: "concurrentWakeDoubleSpawn" };
  },
  failedNativeWriteThenAdvance(): Fault {
    return { name: "failedNativeWriteThenAdvance" };
  },
} as const;

/** A named registry a test can populate and address faults by name. */
export class FaultRegistry {
  readonly #faults = new Map<FaultName, Fault>();

  /** Register (or replace) a fault by its name. */
  register(fault: Fault): void {
    this.#faults.set(fault.name, fault);
  }

  /** Look up a registered fault by name. */
  get(name: FaultName): Fault | undefined {
    return this.#faults.get(name);
  }

  /** True iff a fault with this name is registered. */
  has(name: FaultName): boolean {
    return this.#faults.has(name);
  }

  /** All registered fault names. */
  names(): readonly FaultName[] {
    return [...this.#faults.keys()];
  }
}
