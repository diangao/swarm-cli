import assert from "node:assert/strict";
import { test } from "node:test";

import * as storage from "../src/index.js";

test("public storage exports one authoritative server write surface", () => {
  assert.equal(Object.hasOwn(storage, "ServerMessageRepository"), true);
  assert.equal(Object.hasOwn(storage, "ServerDeliveryRepository"), true);
  assert.equal(Object.hasOwn(storage, "ServerReminderRepository"), true);

  assert.equal(Object.hasOwn(storage, "MessageRepository"), false);
  assert.equal(Object.hasOwn(storage, "ReminderRepository"), false);
  assert.equal(Object.hasOwn(storage, "TaskCommandRepository"), false);
  assert.equal(Object.hasOwn(storage, "ReminderHeadRepository"), false);
});
