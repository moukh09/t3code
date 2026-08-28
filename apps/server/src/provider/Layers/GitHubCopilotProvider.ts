import {
  type GitHubCopilotSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { HttpClient } from "effect/unstable/http";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  makeGitHubCopilotAcpRuntime,
  resolveGitHubCopilotModelId,
} from "../acp/GitHubCopilotAcpSupport.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  normalizeGitHubCopilotCliVersionBucket,
  recordGitHubCopilotTelemetry,
  type GitHubCopilotTelemetryErrorCode,
  type GitHubCopilotTelemetrySink,
  type GitHubCopilotTelemetryStage,
} from "../GitHubCopilotTelemetry.ts";

const PRESENTATION = {
  displayName: "GitHub Copilot",
  badgeLabel: "Early Access",
  showInteractionModeToggle: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});
const FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "default",
    name: "Default",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

const VERSION_PROBE_TIMEOUT_MS = 15_000;
const MODEL_DISCOVERY_TIMEOUT_MS = 60_000;

function flattenSelectOptions(
  option: EffectAcpSchema.SessionConfigOption | undefined,
): ReadonlyArray<{ readonly value: string; readonly name: string }> {
  if (!option || option.type !== "select") return [];
  return option.options.flatMap((entry) =>
    "value" in entry
      ? [{ value: entry.value, name: entry.name }]
      : entry.options.map((nested) => ({ value: nested.value, name: nested.name })),
  );
}

function configOption(
  options: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
  id: string,
): EffectAcpSchema.SessionConfigOption | undefined {
  const expected = id.trim().toLowerCase();
  return options.find((option) => option.id.trim().toLowerCase() === expected);
}

export function buildGitHubCopilotCapabilities(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> | null | undefined,
): ModelCapabilities {
  if (!configOptions) return EMPTY_CAPABILITIES;
  const reasoning = configOption(configOptions, "reasoning_effort");
  const choices = flattenSelectOptions(reasoning).flatMap((entry) => {
    const value = entry.value.trim();
    if (!value) return [];
    return [
      {
        value,
        label: entry.name.trim() || value,
        ...(reasoning?.currentValue === entry.value ? { isDefault: true } : {}),
      },
    ];
  });
  return createModelCapabilities({
    optionDescriptors:
      choices.length > 0
        ? [
            buildSelectOptionDescriptor({
              id: "reasoning",
              label: reasoning?.name.trim() || "Reasoning effort",
              options: choices,
            }),
          ]
        : [],
  });
}

export function buildGitHubCopilotModelsFromSessionSetup(
  setup:
    | EffectAcpSchema.LoadSessionResponse
    | EffectAcpSchema.NewSessionResponse
    | EffectAcpSchema.ResumeSessionResponse,
): ReadonlyArray<ServerProviderModel> {
  const capabilities = buildGitHubCopilotCapabilities(setup.configOptions);
  const modelConfig = configOption(setup.configOptions ?? [], "model");
  const configModels = flattenSelectOptions(modelConfig).map((model) => ({
    modelId: model.value,
    name: model.name,
  }));
  const candidates =
    setup.models?.availableModels && setup.models.availableModels.length > 0
      ? setup.models.availableModels
      : configModels;
  const seen = new Set<string>();
  return candidates.flatMap((model) => {
    const slug = resolveGitHubCopilotModelId(model.modelId);
    if (!slug || seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: model.name.trim() || slug,
        isCustom: false,
        capabilities,
      } satisfies ServerProviderModel,
    ];
  });
}

function fallbackModels(settings: GitHubCopilotSettings): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(FALLBACK_MODELS, settings.customModels, EMPTY_CAPABILITIES);
}

export function buildInitialGitHubCopilotProviderSnapshot(
  settings: GitHubCopilotSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: fallbackModels(settings),
      probe: settings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking GitHub Copilot CLI availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "GitHub Copilot is disabled in T3 Code settings.",
          },
    });
  });
}

const runVersionCommand = (settings: GitHubCopilotSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const command = settings.binaryPath || "copilot";
    const spawn = yield* resolveSpawnCommand(command, ["--version"], { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawn.command, spawn.args, {
        env: environment,
        shell: spawn.shell,
      }),
    );
  });

const discoverModels = (settings: GitHubCopilotSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    // Copilot scans cwd during session/new. An empty, scoped directory keeps
    // provider discovery independent from the server's own checkout.
    const cwd = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-github-copilot-probe-",
    });
    const runtime = yield* makeGitHubCopilotAcpRuntime({
      settings,
      environment,
      childProcessSpawner,
      cwd,
      clientInfo: { name: "t3-code-provider-probe", version: "0.0.0" },
    });
    const started = yield* runtime.start();
    return buildGitHubCopilotModelsFromSessionSetup(started.sessionSetupResult);
  }).pipe(Effect.scoped);

export const checkGitHubCopilotProviderStatus = Effect.fn("checkGitHubCopilotProviderStatus")(
  function* (
    settings: GitHubCopilotSettings,
    environment: NodeJS.ProcessEnv = process.env,
    telemetry: {
      readonly sink?: GitHubCopilotTelemetrySink;
      readonly onCliVersionBucket?: (bucket: string) => void;
    } = {},
  ): Effect.fn.Return<
    ServerProviderDraft,
    never,
    ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | FileSystem.FileSystem
  > {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallback = fallbackModels(settings);
    if (!settings.enabled) {
      return yield* buildInitialGitHubCopilotProviderSnapshot(settings);
    }
    const probeStartedAtMs = yield* Clock.currentTimeMillis;
    const telemetrySink = telemetry.sink ?? recordGitHubCopilotTelemetry;
    const emitProbeTelemetry = (
      outcome: "success" | "failure",
      stage: GitHubCopilotTelemetryStage | undefined,
      version: string | null,
      errorCode?: GitHubCopilotTelemetryErrorCode,
    ) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((completedAtMs) => {
          const cliVersionBucket = normalizeGitHubCopilotCliVersionBucket(version);
          telemetry.onCliVersionBucket?.(cliVersionBucket);
          return telemetrySink({
            operation: "probe",
            outcome,
            stage,
            durationMs: Math.max(0, completedAtMs - probeStartedAtMs),
            errorCode,
            retryable: outcome === "failure" && errorCode !== "command_missing",
            cliVersionBucket,
          });
        }),
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to record GitHub Copilot probe telemetry.", {
            errorTag: causeErrorTag(cause),
          }),
        ),
      );

    const versionResult = yield* runVersionCommand(settings, environment).pipe(
      Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
      Effect.result,
    );
    if (Result.isFailure(versionResult) && isCommandMissingCause(versionResult.failure)) {
      yield* emitProbeTelemetry("failure", "spawn", null, "command_missing");
      return buildServerProvider({
        presentation: PRESENTATION,
        enabled: true,
        checkedAt,
        models: fallback,
        probe: {
          installed: false,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "GitHub Copilot CLI (`copilot`) is not installed or not on PATH.",
        },
      });
    }

    let version: string | null = null;
    let probeWarning: string | undefined;
    let failureStage: GitHubCopilotTelemetryStage | undefined;
    let failureCode: GitHubCopilotTelemetryErrorCode | undefined;
    if (Result.isFailure(versionResult)) {
      failureStage = "spawn";
      failureCode = "version_probe_failed";
      probeWarning = "The Copilot version probe failed; ACP discovery will still be attempted.";
      yield* Effect.logWarning("GitHub Copilot version probe failed.", {
        errorTag: versionResult.failure._tag,
      });
    } else if (Option.isNone(versionResult.success)) {
      failureStage = "spawn";
      failureCode = "version_probe_timeout";
      probeWarning = "The Copilot version probe timed out; ACP discovery will still be attempted.";
    } else {
      const result = versionResult.success.value;
      version = parseGenericCliVersion(`${result.stdout}\n${result.stderr}`);
      if (result.code !== 0) {
        failureStage = "spawn";
        failureCode = "version_probe_exit";
        probeWarning =
          "The Copilot version probe exited unsuccessfully; ACP discovery will still be attempted.";
      }
    }

    const discovery = yield* discoverModels(settings, environment).pipe(
      Effect.timeoutOption(MODEL_DISCOVERY_TIMEOUT_MS),
      Effect.exit,
    );
    let discoveredModels: ReadonlyArray<ServerProviderModel> = [];
    let discoveryWarning: string | undefined;
    if (Exit.isFailure(discovery)) {
      failureStage = "session_new";
      failureCode = "model_discovery_failed";
      yield* Effect.logWarning("GitHub Copilot ACP discovery failed.", {
        errorTag: causeErrorTag(discovery.cause),
      });
      discoveryWarning = "Copilot ACP discovery failed; models will load when a session starts.";
    } else if (Option.isNone(discovery.value)) {
      failureStage = "session_new";
      failureCode = "model_discovery_timeout";
      discoveryWarning = `Copilot ACP discovery timed out after ${MODEL_DISCOVERY_TIMEOUT_MS}ms; models will load when a session starts.`;
    } else {
      discoveredModels = discovery.value.value;
      if (discoveredModels.length === 0) {
        failureStage = "session_new";
        failureCode = "model_discovery_empty";
        discoveryWarning = "Copilot ACP discovery returned no models.";
      }
    }

    const message = [probeWarning, discoveryWarning].filter(Boolean).join(" ") || undefined;
    yield* emitProbeTelemetry(message ? "failure" : "success", failureStage, version, failureCode);
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models:
        discoveredModels.length > 0
          ? providerModelsFromSettings(discoveredModels, settings.customModels, EMPTY_CAPABILITIES)
          : fallback,
      probe: {
        installed: true,
        version,
        status: message ? "warning" : "ready",
        auth: { status: "unknown" },
        ...(message ? { message } : {}),
      },
    });
  },
);

/**
 * Layers the "a newer CLI exists" advisory onto a published snapshot. Copilot
 * has no package-manager update path, so this only ever annotates — it never
 * offers an in-app update.
 */
export const enrichGitHubCopilotSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> =>
  enrichProviderSnapshotWithVersionAdvisory(input.snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => input.publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("GitHub Copilot version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
