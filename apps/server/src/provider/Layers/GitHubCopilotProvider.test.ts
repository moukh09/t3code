import { describe, expect, it } from "vite-plus/test";

import { buildGitHubCopilotSlashCommands } from "./GitHubCopilotProvider.ts";

describe("buildGitHubCopilotSlashCommands", () => {
  it("normalizes ACP commands for the composer slash menu", () => {
    expect(
      buildGitHubCopilotSlashCommands([
        {
          name: "/review",
          description: " Review the current changes ",
          input: { hint: " Optional review focus " },
        },
        {
          name: "REVIEW",
          description: "Duplicate",
        },
        {
          name: "usage",
          description: "Show session usage",
        },
      ]),
    ).toEqual([
      {
        name: "review",
        description: "Review the current changes",
        input: { hint: "Optional review focus" },
      },
      {
        name: "usage",
        description: "Show session usage",
      },
    ]);
  });
});
