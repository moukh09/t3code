import {
  type GitHubCopilotSettings,
  type ProviderInteractionMode,
  type ProviderOptionSelection,
  type RuntimeMode,
} from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import { getProviderOptionStringSelectionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const AUTH_METHOD_ID = "copilot-login";
const MODEL_CONFIG_ID = "model";
const REASONING_CONFIG_ID = "reasoning_effort";
const MODE_CONFIG_ID = "mode";
const ALLOW_ALL_CONFIG_ID = "allow_all";

type CopilotRuntimeSettings = Pick<GitHubCopilotSettings, "binaryPath" | "launchArgs">;

export interface GitHubCopilotAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly settings: CopilotRuntimeSettings;
  readonly environment?: NodeJS.ProcessEnv;
}

export function buildGitHubCopilotAcpSpawnInput(
  settings: CopilotRuntimeSettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: settings.binaryPath || "copilot",
    args: [...tokenizeCliArgs(settings.launchArgs), "--acp", "--stdio"],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeGitHubCopilotAcpRuntime = (
  input: GitHubCopilotAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const context = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        authMethodId: AUTH_METHOD_ID,
        spawn: buildGitHubCopilotAcpSpawnInput(input.settings, input.cwd, input.environment),
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(Effect.provide(context));
  });

function normalizeToken(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

function findConfigOption(
  options: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
  id: string,
): EffectAcpSchema.SessionConfigOption | undefined {
  const expected = normalizeToken(id);
  return options.find((option) => normalizeToken(option.id) === expected);
}

function selectValues(
  option: EffectAcpSchema.SessionConfigOption,
): ReadonlyArray<{ readonly value: string; readonly name: string }> {
  if (option.type !== "select") return [];
  return option.options.flatMap((entry) =>
    "value" in entry
      ? [{ value: entry.value, name: entry.name }]
      : entry.options.map((nested) => ({ value: nested.value, name: nested.name })),
  );
}

function matchingSelectValue(
  option: EffectAcpSchema.SessionConfigOption | undefined,
  requested: string,
  options?: { readonly rejectAutopilot?: boolean },
): string | undefined {
  if (!option || option.type !== "select") return undefined;
  const expected = normalizeToken(requested);
  return selectValues(option).find((entry) => {
    const value = normalizeToken(entry.value);
    const name = normalizeToken(entry.name);
    if (options?.rejectAutopilot && (value.includes("autopilot") || name.includes("autopilot"))) {
      return false;
    }
    return value === expected || name === expected;
  })?.value;
}

function currentValue(option: EffectAcpSchema.SessionConfigOption): string | boolean | undefined {
  return option.currentValue;
}

export interface GitHubCopilotConfigErrorContext {
  readonly cause: EffectAcpErrors.AcpError;
  readonly configId: string;
}

export function currentGitHubCopilotModelId(
  setup:
    | EffectAcpSchema.LoadSessionResponse
    | EffectAcpSchema.NewSessionResponse
    | EffectAcpSchema.ResumeSessionResponse,
): string | undefined {
  const modelOption = findConfigOption(setup.configOptions ?? [], MODEL_CONFIG_ID);
  if (modelOption && typeof modelOption.currentValue === "string") {
    return modelOption.currentValue.trim() || undefined;
  }
  return setup.models?.currentModelId?.trim() || undefined;
}

export function resolveGitHubCopilotModelId(model: string | null | undefined): string {
  return model?.trim() || "default";
}

/**
 * Applies only the four Copilot ACP controls T3 owns. In particular, mode is
 * always Agent or Plan; Autopilot is never selected by inference.
 */
export function applyGitHubCopilotSessionConfiguration<E>(input: {
  readonly runtime: Pick<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    "getConfigOptions" | "setConfigOption"
  >;
  readonly model: string | null | undefined;
  readonly selections: ReadonlyArray<ProviderOptionSelection> | null | undefined;
  readonly interactionMode: ProviderInteractionMode | undefined;
  readonly runtimeMode: RuntimeMode;
  readonly mapError: (context: GitHubCopilotConfigErrorContext) => E;
}): Effect.Effect<string | undefined, E> {
  return Effect.gen(function* () {
    let configOptions = yield* input.runtime.getConfigOptions;
    let selectedModel: string | undefined;

    const setOption = (
      option: EffectAcpSchema.SessionConfigOption | undefined,
      value: string | boolean | undefined,
    ) =>
      Effect.gen(function* () {
        if (!option || value === undefined || currentValue(option) === value) return;
        yield* input.runtime
          .setConfigOption(option.id, value)
          .pipe(Effect.mapError((cause) => input.mapError({ cause, configId: option.id })));
        configOptions = yield* input.runtime.getConfigOptions;
      });

    const requestedModel = input.model?.trim();
    const modelOption = findConfigOption(configOptions, MODEL_CONFIG_ID);
    if (requestedModel && modelOption) {
      selectedModel =
        matchingSelectValue(modelOption, requestedModel) ??
        (modelOption.type === "select" ? undefined : requestedModel);
      yield* setOption(modelOption, selectedModel);
    } else if (modelOption && typeof modelOption.currentValue === "string") {
      selectedModel = modelOption.currentValue.trim() || undefined;
    }

    const requestedReasoning =
      getProviderOptionStringSelectionValue(input.selections, "reasoning") ??
      getProviderOptionStringSelectionValue(input.selections, "reasoningEffort");
    const reasoningOption = findConfigOption(configOptions, REASONING_CONFIG_ID);
    if (requestedReasoning && reasoningOption) {
      yield* setOption(reasoningOption, matchingSelectValue(reasoningOption, requestedReasoning));
    }

    const modeOption = findConfigOption(configOptions, MODE_CONFIG_ID);
    const requestedMode = input.interactionMode === "plan" ? "Plan" : "Agent";
    yield* setOption(
      modeOption,
      matchingSelectValue(modeOption, requestedMode, { rejectAutopilot: true }),
    );

    const allowAllOption = findConfigOption(configOptions, ALLOW_ALL_CONFIG_ID);
    const allowAll = input.runtimeMode === "full-access";
    if (allowAllOption?.type === "boolean") {
      yield* setOption(allowAllOption, allowAll);
    } else if (allowAllOption) {
      yield* setOption(
        allowAllOption,
        matchingSelectValue(allowAllOption, allowAll ? "on" : "off") ??
          matchingSelectValue(allowAllOption, String(allowAll)),
      );
    }

    return selectedModel;
  });
}
