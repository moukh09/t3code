// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { createModelSelection } from "@t3tools/shared/model";
import { expect } from "vite-plus/test";
import { GitHubCopilotSettings, ProviderInstanceId } from "@t3tools/contracts";

import * as TextGeneration from "./TextGeneration.ts";
import { makeGitHubCopilotTextGeneration } from "./GitHubCopilotTextGeneration.ts";

const decodeGitHubCopilotSettings = Schema.decodeSync(GitHubCopilotSettings);

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
// Forward slashes plus single quotes keep the path a single `launchArgs`
// token on both POSIX and Windows.
const mockAgentArg = `'${NodePath.join(__dirname, "../../scripts/acp-mock-agent.ts").replaceAll("\\", "/")}'`;
const COPILOT_INSTANCE = ProviderInstanceId.make("githubCopilot");

/**
 * Runs against the shared ACP mock agent. `binaryPath` + `launchArgs` stand in
 * for the wrapper-binary shape Copilot is deployed with (e.g.
 * `{ binaryPath: "agency.exe", launchArgs: "copilot" }`) — the agent only
 * starts if both flow through ahead of the `--acp --stdio` flags.
 */
function withFakeAcpCopilot<A, E, R>(
  env: Record<string, string>,
  effectFn: (textGeneration: TextGeneration.TextGeneration["Service"]) => Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const settings = decodeGitHubCopilotSettings({
      enabled: true,
      binaryPath: process.execPath,
      launchArgs: mockAgentArg,
    });
    const textGeneration = yield* makeGitHubCopilotTextGeneration(settings, {
      ...process.env,
      ...env,
    });
    return yield* effectFn(textGeneration);
  }).pipe(Effect.scoped);
}

function makeRequestLogPath(): string {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-copilot-text-log-"));
  return NodePath.join(dir, "requests.ndjson");
}

function readJsonRpcRequests(
  filePath: string,
): ReadonlyArray<{ readonly method?: string; readonly params?: Record<string, unknown> }> {
  return NodeFS.readFileSync(filePath, "utf8")
    .trim()
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as { method?: string; params?: Record<string, unknown> });
}

it.layer(NodeServices.layer)("GitHubCopilotTextGeneration", (it) => {
  it.effect("runs over ACP with tool capabilities disabled and pins the requested model", () => {
    const requestLogPath = makeRequestLogPath();

    return withFakeAcpCopilot(
      {
        T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        T3_ACP_PROMPT_RESPONSE_TEXT: JSON.stringify({
          subject: "Add GitHub Copilot provider",
          body: "Wire up the ACP driver and headless text generation path.",
        }),
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateCommitMessage({
            cwd: process.cwd(),
            branch: "feature/github-copilot",
            stagedSummary: "M apps/server/src/provider/Drivers/GitHubCopilotDriver.ts",
            stagedPatch: "diff --git a/.../GitHubCopilotDriver.ts b/.../GitHubCopilotDriver.ts",
            modelSelection: createModelSelection(COPILOT_INSTANCE, "composer-2"),
          });

          expect(generated.subject).toBe("Add GitHub Copilot provider");
          expect(generated.body).toBe("Wire up the ACP driver and headless text generation path.");

          const requests = readJsonRpcRequests(requestLogPath);
          expect(
            requests.find((request) => request.method === "initialize")?.params?.clientCapabilities,
          ).toMatchObject({
            fs: { readTextFile: false, writeTextFile: false },
            terminal: false,
          });
          expect(
            requests.some(
              (request) =>
                request.method === "session/set_config_option" &&
                request.params?.configId === "model" &&
                request.params?.value === "composer-2",
            ),
          ).toBe(true);
        }),
    );
  });

  it.effect("extracts the JSON object when Copilot wraps it in prose", () =>
    withFakeAcpCopilot(
      {
        T3_ACP_PROMPT_RESPONSE_TEXT:
          "Sure! Here's a thread title:\n\n" +
          JSON.stringify({ title: "Investigate failing CI" }) +
          "\n\nLet me know if you need anything else.",
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "the lint job is red",
            modelSelection: createModelSelection(COPILOT_INSTANCE, "composer-2"),
          });
          expect(generated.title).toBe("Investigate failing CI");
        }),
    ),
  );

  it.effect("decodes a structured PR title + body", () =>
    withFakeAcpCopilot(
      {
        T3_ACP_PROMPT_RESPONSE_TEXT: JSON.stringify({
          title: "feat(githubCopilot): add provider driver",
          body: "## Summary\n- Register the Copilot driver.\n- Reuse the shared ACP session runtime.",
        }),
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generatePrContent({
            cwd: process.cwd(),
            baseBranch: "main",
            headBranch: "feat/github-copilot-provider",
            commitSummary: "feat: add github copilot provider",
            diffSummary: "M apps/server/src/provider/Drivers/GitHubCopilotDriver.ts",
            diffPatch: "diff --git a/.../GitHubCopilotDriver.ts b/.../GitHubCopilotDriver.ts",
            modelSelection: createModelSelection(COPILOT_INSTANCE, "default"),
          });

          expect(generated.title).toBe("feat(githubCopilot): add provider driver");
          expect(generated.body).toContain("Reuse the shared ACP session runtime.");
        }),
    ),
  );

  it.effect("surfaces config option failures as text generation errors", () =>
    withFakeAcpCopilot(
      {
        T3_ACP_FAIL_SET_CONFIG_OPTION: "1",
        T3_ACP_PROMPT_RESPONSE_TEXT: JSON.stringify({ branch: "unreachable" }),
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            textGeneration.generateBranchName({
              cwd: process.cwd(),
              message: "wire up copilot",
              modelSelection: createModelSelection(COPILOT_INSTANCE, "composer-2"),
            }),
          );
          expect(error._tag).toBe("TextGenerationError");
          expect(error.detail).toContain('config option "model"');
        }),
    ),
  );

  it.effect("fails with TextGenerationError when output is empty", () =>
    withFakeAcpCopilot(
      {
        T3_ACP_PROMPT_RESPONSE_TEXT: "   \n  ",
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            textGeneration.generateThreadTitle({
              cwd: process.cwd(),
              message: "anything",
              modelSelection: createModelSelection(COPILOT_INSTANCE, "default"),
            }),
          );
          expect(error._tag).toBe("TextGenerationError");
          expect(error.detail).toMatch(/empty/i);
        }),
    ),
  );

  it.effect("fails with TextGenerationError when output is unparseable JSON", () =>
    withFakeAcpCopilot(
      {
        T3_ACP_PROMPT_RESPONSE_TEXT: "totally not json output from a confused model",
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            textGeneration.generateThreadTitle({
              cwd: process.cwd(),
              message: "anything",
              modelSelection: createModelSelection(COPILOT_INSTANCE, "default"),
            }),
          );
          expect(error._tag).toBe("TextGenerationError");
          expect(error.detail).toMatch(/invalid structured output/i);
        }),
    ),
  );
});
