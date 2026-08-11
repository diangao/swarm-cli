import assert from "node:assert/strict";
import { test } from "node:test";

// Public delivery-kernel surface: an app consumer must be able to import the
// DeliveryKernel value AND its five wiring ports from the package ROOT
// (`@swarm/daemon-core`), not only the internal `./delivery` barrel. This probe
// binds the exact root re-exports; if any is removed from src/index.ts the value
// and type imports fail closed during TypeScript compilation (TS2305), and a
// missing runtime named export would also fail ESM linking.
import {
  DeliveryKernel,
  type DeliveryClock,
  type DeliveryCommandIdDerivationPort,
  type DeliveryExecutionPort,
  type DeliveryJournalPort,
  type DeliveryServerCommitPort,
} from "@swarm/daemon-core";

test("public delivery exports: DeliveryKernel is a runtime root export and the five ports compile-bind from @swarm/daemon-core", () => {
  // Runtime export: the DeliveryKernel value is present at the package root.
  assert.equal(typeof DeliveryKernel, "function");
  assert.equal(DeliveryKernel.name, "DeliveryKernel");

  // Compile-bind all five consumer port types from the package root. A removed
  // root type export makes this declaration fail to resolve under tsc, so the
  // probe genuinely fails closed if the surface regresses.
  const ports: {
    journal: DeliveryJournalPort;
    execution: DeliveryExecutionPort;
    serverCommit: DeliveryServerCommitPort;
    clock: DeliveryClock;
    commandIdDerivation: DeliveryCommandIdDerivationPort;
  } | null = null;
  assert.equal(ports, null);
});
