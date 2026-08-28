import {
  type EventId,
  type ProviderRuntimeEvent,
  type ProviderDriverKind,
  RuntimeTaskId,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";

import type { AcpToolCallState } from "./acp/AcpRuntimeModel.ts";

type AgencyTaskEventResult = ProviderRuntimeEvent | null | undefined;

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function collectResultText(value: unknown, depth = 0): ReadonlyArray<string> {
  if (depth > 4) return [];
  if (typeof value === "string") return [value];
  if (typeof value === "number" || typeof value === "boolean") return [String(value)];
  if (Array.isArray(value)) {
    return value.flatMap((entry) => collectResultText(entry, depth + 1));
  }
  const record = asRecord(value);
  if (!record) return [];
  return Object.entries(record).flatMap(([key, entry]) => [
    `${key}:${typeof entry === "string" ? entry : ""}`,
    ...collectResultText(entry, depth + 1),
  ]);
}

function resultText(toolCall: AcpToolCallState): string {
  return [
    ...collectResultText(toolCall.data.rawOutput),
    ...collectResultText(toolCall.data.content),
  ].join("\n");
}

function resultAgentId(toolCall: AcpToolCallState): string | undefined {
  return /["']?agent[_-]?id["']?\s*[:=]\s*["']?([a-zA-Z0-9_-]+)/i.exec(resultText(toolCall))?.[1];
}

function resultStatus(toolCall: AcpToolCallState): string | undefined {
  return /["']?status["']?\s*[:=]\s*["']?([a-zA-Z_-]+)/i
    .exec(resultText(toolCall))?.[1]
    ?.toLowerCase();
}

function eventBase(input: {
  readonly stamp: { readonly eventId: EventId; readonly createdAt: string };
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly toolCall: AcpToolCallState;
  readonly rawPayload: unknown;
}) {
  return {
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    raw: {
      source: "acp.jsonrpc",
      method: "session/update",
      payload: input.rawPayload,
    },
  } as const;
}

export function makeGitHubCopilotAgencyTaskEvent(input: {
  readonly stamp: { readonly eventId: EventId; readonly createdAt: string };
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly toolCall: AcpToolCallState;
  readonly rawPayload: unknown;
}): AgencyTaskEventResult {
  const rawInput = asRecord(input.toolCall.data.rawInput);
  if (!rawInput) return undefined;

  const agentType = asString(rawInput.agent_type);
  const mode = asString(rawInput.mode);
  const name = asString(rawInput.name);
  const prompt = asString(rawInput.prompt);
  const description = asString(rawInput.description);
  const isBackgroundLaunch =
    agentType !== undefined && mode === "background" && name !== undefined && prompt !== undefined;

  if (isBackgroundLaunch) {
    if (input.toolCall.status !== "completed") {
      return input.toolCall.status === "failed" ? undefined : null;
    }
    const agentId = resultAgentId(input.toolCall);
    if (!agentId) return undefined;
    return {
      type: "task.started",
      ...eventBase(input),
      payload: {
        taskId: RuntimeTaskId.make(agentId),
        taskType: "subagent",
        title: name,
        role: agentType,
        toolUseId: input.toolCall.toolCallId,
        ...(description ? { description } : {}),
      },
    };
  }

  const agentId = asString(rawInput.agent_id);
  if (!agentId) return undefined;
  if (input.toolCall.status !== "completed") {
    return input.toolCall.status === "failed" ? undefined : null;
  }

  const status = resultStatus(input.toolCall);
  const taskId = RuntimeTaskId.make(agentId);
  switch (status) {
    case "running":
    case "waiting":
    case "idle":
      return {
        type: "task.updated",
        ...eventBase(input),
        payload: {
          taskId,
          taskType: "subagent",
          status,
        },
      };
    case "completed":
      return {
        type: "task.completed",
        ...eventBase(input),
        payload: {
          taskId,
          taskType: "subagent",
          status: "completed",
        },
      };
    case "failed":
      return {
        type: "task.completed",
        ...eventBase(input),
        payload: {
          taskId,
          taskType: "subagent",
          status: "failed",
        },
      };
    case "cancelled":
    case "interrupted":
    case "stopped":
      return {
        type: "task.completed",
        ...eventBase(input),
        payload: {
          taskId,
          taskType: "subagent",
          status: "stopped",
        },
      };
    default:
      return undefined;
  }
}
