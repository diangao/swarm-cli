#!/usr/bin/env node

if (process.argv.includes("--version")) {
  process.stdout.write("2.1.226 (Claude Code)\n");
  process.exit(0);
}

const sessionId = process.argv[process.argv.indexOf("--session-id") + 1];
emit({
  type: "system",
  subtype: "init",
  session_id: sessionId,
  capabilities: { control_requests: { interrupt: true, queue_receipt: true } },
});

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
    if (message.type === "control_request") {
      emit({
        type: "control_response",
        request_id: message.request_id,
        response: { subtype: "success", still_queued: [], cancelled: [] },
      });
      continue;
    }
    emit({ ...message, isReplay: true });
    emit({
      type: "assistant",
      session_id: sessionId,
      message: { content: [{ type: "text", text: "Claude retained reply" }] },
    });
    emit({ type: "result", session_id: sessionId, subtype: "success" });
  }
});

function emit(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}
