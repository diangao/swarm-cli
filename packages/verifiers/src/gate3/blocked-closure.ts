export type BlockedClosureOracleInput = {
  readonly rootCauseTaskId: string;
  readonly storedTopology: {
    readonly contains: readonly {
      readonly parentTaskId: string;
      readonly childTaskId: string;
    }[];
    readonly dependsOn: readonly {
      readonly prerequisiteTaskId: string;
      readonly dependentTaskId: string;
    }[];
  };
  readonly poisonedView: {
    readonly kind: string;
    readonly taskIds: readonly string[];
  };
};

export class BlockedClosureInputError extends Error {
  public readonly code = "oracle_input_projection_violation";

  public constructor(detail: string) {
    super(`oracle_input_projection_violation:${detail}`);
    this.name = "BlockedClosureInputError";
  }
}

function canonical(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const object = value as Readonly<Record<string, unknown>>;
  return `{${Object.keys(object)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonical(object[key])}`)
    .join(",")}}`;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new BlockedClosureInputError(`${label} must be object`);
  }
  return value as Record<string, unknown>;
}

function keys(value: Record<string, unknown>, expected: readonly string[], label: string): void {
  if (
    Object.keys(value).length !== expected.length ||
    expected.some((key) => !Object.hasOwn(value, key))
  ) {
    throw new BlockedClosureInputError(`${label} exact keys`);
  }
}

function stringArray(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new BlockedClosureInputError(`${label} must be string array`);
  }
  return value as readonly string[];
}

export function decodeBlockedClosureOracleInput(serialized: string): BlockedClosureOracleInput {
  let parsed: unknown;
  try {
    parsed = JSON.parse(serialized) as unknown;
  } catch {
    throw new BlockedClosureInputError("invalid JSON");
  }
  const root = record(parsed, "input");
  keys(root, ["rootCauseTaskId", "storedTopology", "poisonedView"], "input");
  const topology = record(root.storedTopology, "storedTopology");
  keys(topology, ["contains", "dependsOn"], "storedTopology");
  const poison = record(root.poisonedView, "poisonedView");
  keys(poison, ["kind", "taskIds"], "poisonedView");
  if (typeof root.rootCauseTaskId !== "string" || typeof poison.kind !== "string") {
    throw new BlockedClosureInputError("identity fields");
  }
  const edgeArray = (
    value: unknown,
    edgeKeys: readonly [string, string],
    label: string,
  ): readonly Record<string, string>[] => {
    if (!Array.isArray(value)) throw new BlockedClosureInputError(`${label} must be array`);
    return value.map((item, index) => {
      const edge = record(item, `${label}[${index}]`);
      keys(edge, edgeKeys, `${label}[${index}]`);
      if (typeof edge[edgeKeys[0]] !== "string" || typeof edge[edgeKeys[1]] !== "string") {
        throw new BlockedClosureInputError(`${label}[${index}] identity`);
      }
      return edge as Record<string, string>;
    });
  };
  const input: BlockedClosureOracleInput = {
    rootCauseTaskId: root.rootCauseTaskId,
    storedTopology: {
      contains: edgeArray(topology.contains, ["parentTaskId", "childTaskId"], "contains").map((edge) => ({
        parentTaskId: edge.parentTaskId!,
        childTaskId: edge.childTaskId!,
      })),
      dependsOn: edgeArray(topology.dependsOn, ["prerequisiteTaskId", "dependentTaskId"], "dependsOn").map((edge) => ({
        prerequisiteTaskId: edge.prerequisiteTaskId!,
        dependentTaskId: edge.dependentTaskId!,
      })),
    },
    poisonedView: {
      kind: poison.kind,
      taskIds: stringArray(poison.taskIds, "poisonedView.taskIds"),
    },
  };
  // Canonical bytes make duplicate keys, hidden aliases, and alternate JSON
  // spellings fail before the oracle executes.
  if (canonical(input) !== serialized) {
    throw new BlockedClosureInputError("input is not canonical serialized projection");
  }
  return input;
}

export function runBlockedClosureOracle(serialized: string): {
  readonly oracleExecuted: true;
  readonly affectedTaskIds: readonly string[];
} {
  const input = decodeBlockedClosureOracleInput(serialized);
  const affected = new Set<string>([input.rootCauseTaskId]);
  for (;;) {
    const sizeBefore = affected.size;
    for (const edge of input.storedTopology.dependsOn) {
      if (affected.has(edge.prerequisiteTaskId)) affected.add(edge.dependentTaskId);
    }
    for (const edge of input.storedTopology.contains) {
      if (affected.has(edge.childTaskId)) affected.add(edge.parentTaskId);
    }
    if (affected.size === sizeBefore) break;
  }
  return { oracleExecuted: true, affectedTaskIds: [...affected].sort() };
}

export function canonicalBlockedClosureOracleInput(input: BlockedClosureOracleInput): string {
  return canonical(input);
}
