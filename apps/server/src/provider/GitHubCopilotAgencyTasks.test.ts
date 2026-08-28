import { EventId, ProviderDriverKind, RuntimeTaskId, ThreadId, TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { AcpToolCallState } from "./acp/AcpRuntimeModel.ts";
import { makeGitHubCopilotAgencyTaskEvent } from "./GitHubCopilotAgencyTasks.ts";

const base = {
  stamp: {
    eventId: EventId.make("event-1"),
    createdAt: "2026-08-26T00:00:00.000Z",
  },
  provider: ProviderDriverKind.make("githubCopilot"),
  threadId: ThreadId.make("thread-1"),
  turnId: TurnId.make("turn-1"),
  rawPayload: {},
} as const;

function toolCall(input: {
  readonly status: NonNullable<AcpToolCallState["status"]>;
  readonly rawInput: Record<string, unknown>;
  readonly output: string;
}): AcpToolCallState {
  return {
    toolCallId: "tool-1",
    kind: "dynamic_tool_call",
    status: input.status,
    data: {
      toolCallId: "tool-1",
      rawInput: input.rawInput,
      rawOutput: {
        content: input.output,
      },
    },
  };
}

describe("GitHubCopilotAgencyTasks", () => {
  it("maps a completed background task launch to task.started", () => {
    const event = makeGitHubCopilotAgencyTaskEvent({
      ...base,
      toolCall: toolCall({
        status: "completed",
        rawInput: {
          agent_type: "explore",
          description: "Inspect the provider",
          mode: "background",
          name: "provider-investigator",
          prompt: "Inspect the provider and report back.",
        },
        output: 'agent_id: "agent-123"',
      }),
    });

    expect(event).toMatchObject({
      type: "task.started",
      payload: {
        taskId: RuntimeTaskId.make("agent-123"),
        taskType: "subagent",
        title: "provider-investigator",
        role: "explore",
        description: "Inspect the provider",
        toolUseId: "tool-1",
      },
    });
  });

  it("suppresses an in-flight background launch until it has an agent id", () => {
    const event = makeGitHubCopilotAgencyTaskEvent({
      ...base,
      toolCall: toolCall({
        status: "inProgress",
        rawInput: {
          agent_type: "explore",
          mode: "background",
          name: "provider-investigator",
          prompt: "Inspect the provider and report back.",
        },
        output: "",
      }),
    });

    expect(event).toBeNull();
  });

  it.each([
    ["running", "task.updated", "running"],
    ["idle", "task.updated", "idle"],
    ["completed", "task.completed", "completed"],
    ["failed", "task.completed", "failed"],
    ["cancelled", "task.completed", "stopped"],
  ] as const)("maps read_agent status %s to %s", (status, type, expectedStatus) => {
    const event = makeGitHubCopilotAgencyTaskEvent({
      ...base,
      toolCall: toolCall({
        status: "completed",
        rawInput: {
          agent_id: "agent-123",
          wait: true,
          timeout: 180,
        },
        output: `agent_id: "agent-123", status: "${status}"`,
      }),
    });

    expect(event).toMatchObject({
      type,
      payload: {
        taskId: RuntimeTaskId.make("agent-123"),
        taskType: "subagent",
        status: expectedStatus,
      },
    });
  });

  it("leaves unrelated dynamic tools on the generic item path", () => {
    const event = makeGitHubCopilotAgencyTaskEvent({
      ...base,
      toolCall: toolCall({
        status: "completed",
        rawInput: { skill: "unslop" },
        output: "loaded",
      }),
    });

    expect(event).toBeUndefined();
  });
});
