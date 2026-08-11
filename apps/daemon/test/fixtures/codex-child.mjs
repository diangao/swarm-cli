#!/usr/bin/env node

if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.145.0\n");
  process.exit(0);
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  while (true) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.method === "initialized") continue;
    if (message.method === "initialize") respond(message.id, {});
    else if (message.method === "thread/start") {
      respond(message.id, { thread: { id: "codex-private-thread" } });
    } else if (message.method === "turn/start") {
      const runtimeTurnId = "codex-private-turn";
      respond(message.id, { turn: { id: runtimeTurnId } });
      notify("turn/started", {
        threadId: "codex-private-thread",
        turn: {
          id: runtimeTurnId,
          items: [{ type: "userMessage", clientId: message.params.clientUserMessageId }],
        },
      });
      notify("item/completed", {
        threadId: "codex-private-thread",
        turnId: runtimeTurnId,
        item: { type: "agentMessage", id: "reply-1", text: "Codex retained reply" },
      });
      notify("turn/completed", {
        threadId: "codex-private-thread",
        turn: { id: runtimeTurnId },
      });
    }
  }
});

function respond(id, result) {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id, result })}\n`);
}

function notify(method, params) {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
}
