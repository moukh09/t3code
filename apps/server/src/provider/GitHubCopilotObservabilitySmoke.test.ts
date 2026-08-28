/**
 * Optional end-to-end smoke test against the real GitHub Copilot CLI and the
 * local OTLP/Kusto stack.
 *
 * Run from `infra/local-observability` after `docker compose up -d`:
 * `T3_GITHUB_COPILOT_OBSERVABILITY_SMOKE=1 vp test run
 * apps/server/src/provider/GitHubCopilotObservabilitySmoke.test.ts`
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  GitHubCopilotSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import { ObservabilityLive } from "../observability/Layers/Observability.ts";
import * as ResourceAttribution from "../resourceTelemetry/ResourceAttribution.ts";
import type {
  GitHubCopilotTelemetryRecord,
  GitHubCopilotTelemetrySink,
} from "./GitHubCopilotTelemetry.ts";
import { recordGitHubCopilotTelemetry } from "./GitHubCopilotTelemetry.ts";
import { makeGitHubCopilotAdapter } from "./Layers/GitHubCopilotAdapter.ts";
import { checkGitHubCopilotProviderStatus } from "./Layers/GitHubCopilotProvider.ts";

const decodeGitHubCopilotSettings = Schema.decodeSync(GitHubCopilotSettings);
const PROVIDER = ProviderDriverKind.make("githubCopilot");
const INSTANCE_ID = ProviderInstanceId.make("githubCopilot");
const SMOKE_TIMEOUT_MS = 5 * 60_000;
const KUSTO_QUERY_ENDPOINT =
  process.env.T3CODE_KUSTO_QUERY_ENDPOINT ?? "http://localhost:8080/v1/rest/query";
const KustoResponse = Schema.Struct({
  Tables: Schema.Array(
    Schema.Struct({
      Rows: Schema.Array(Schema.Array(Schema.Unknown)),
    }),
  ),
});
const decodeKustoResponse = Schema.decodeUnknownEffect(KustoResponse);

const queryKusto = Effect.fn("GitHubCopilotObservabilitySmoke.queryKusto")(function* (
  query: string,
) {
  const httpClient = yield* HttpClient.HttpClient;
  const response = yield* httpClient.post(KUSTO_QUERY_ENDPOINT, {
    body: yield* HttpBody.json({ db: "T3CodeLocal", csl: query }),
  });
  if (response.status < 200 || response.status >= 300) {
    return yield* Effect.fail(`Kusto query failed with HTTP ${response.status}.`);
  }
  return yield* decodeKustoResponse(yield* response.json);
});

const waitForKustoRows = Effect.fn("GitHubCopilotObservabilitySmoke.waitForKustoRows")(function* (
  query: string,
  requiredNames: ReadonlyArray<string>,
) {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const response = yield* queryKusto(query);
    const names = new Set(
      (response.Tables[0]?.Rows ?? []).flatMap((row) =>
        typeof row[0] === "string" ? [row[0]] : [],
      ),
    );
    if (requiredNames.every((name) => names.has(name))) return;
    yield* Effect.sleep("1 second");
  }
  return yield* Effect.fail(`Kusto did not return required rows: ${requiredNames.join(", ")}.`);
});

const baseConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-github-copilot-observability-smoke-",
}).pipe(Layer.provide(NodeServices.layer));

const smokeConfigLayer = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.map(ServerConfig.ServerConfig, (config) =>
    ServerConfig.make({
      ...config,
      otlpTracesUrl: process.env.T3CODE_OTLP_TRACES_URL ?? "http://localhost:4318/v1/traces",
      otlpMetricsUrl: process.env.T3CODE_OTLP_METRICS_URL ?? "http://localhost:4318/v1/metrics",
      otlpExportIntervalMs: 500,
      otlpServiceName: "t3-copilot-observability-smoke",
    }),
  ),
).pipe(Layer.provide(baseConfigLayer));

const observabilityLayer = ObservabilityLive.pipe(
  Layer.provideMerge(ResourceAttribution.layer),
  Layer.provide(smokeConfigLayer),
  Layer.provideMerge(FetchHttpClient.layer),
);

const smokeLayer = Layer.mergeAll(NodeServices.layer, smokeConfigLayer, observabilityLayer);

describe.runIf(process.env.T3_GITHUB_COPILOT_OBSERVABILITY_SMOKE === "1")(
  "GitHub Copilot local observability smoke",
  () => {
    it.effect(
      "exports a real turn through OTLP",
      () =>
        Effect.gen(function* () {
          const startedAt = DateTime.formatIso(
            DateTime.subtract(yield* DateTime.now, { seconds: 1 }),
          );
          yield* Effect.gen(function* () {
            const fileSystem = yield* FileSystem.FileSystem;
            const cwd = yield* fileSystem.makeTempDirectoryScoped({
              prefix: "t3-copilot-real-smoke-",
            });
            const records: Array<GitHubCopilotTelemetryRecord> = [];
            const telemetrySink: GitHubCopilotTelemetrySink = (record) =>
              Effect.sync(() => void records.push(record)).pipe(
                Effect.andThen(recordGitHubCopilotTelemetry(record)),
              );
            const settings = decodeGitHubCopilotSettings({
              enabled: true,
              binaryPath: process.env.T3_GITHUB_COPILOT_BINARY ?? "copilot",
            });
            let cliVersionBucket = "unknown";
            yield* checkGitHubCopilotProviderStatus(settings, process.env, {
              sink: telemetrySink,
              onCliVersionBucket: (bucket) => {
                cliVersionBucket = bucket;
              },
            });
            const adapter = yield* makeGitHubCopilotAdapter(settings, {
              instanceId: INSTANCE_ID,
              telemetrySink,
              getCliVersionBucket: () => cliVersionBucket,
            });
            const threadId = ThreadId.make("local-observability-smoke");

            yield* adapter.startSession({
              threadId,
              provider: PROVIDER,
              cwd,
              runtimeMode: "full-access",
            });
            yield* adapter.sendTurn({
              threadId,
              input: "Reply with the single word OK. Do not use tools.",
            });
            yield* adapter.stopSession(threadId);

            assert.includeMembers(
              records.map((record) => record.operation),
              [
                "probe",
                "spawn",
                "session_new",
                "turn",
                "first_response",
                "usage",
                "post_settlement_tail",
              ],
            );
          }).pipe(Effect.scoped, Effect.provide(smokeLayer));

          yield* waitForKustoRows(
            `ProviderOperations | where Provider == "githubCopilot" and Timestamp >= datetime(${startedAt}) | summarize Rows=count() by Operation`,
            ["probe", "session_new", "turn", "first_response", "usage", "post_settlement_tail"],
          );
          yield* waitForKustoRows(
            `ProviderMetrics | where Provider == "githubCopilot" and Timestamp >= datetime(${startedAt}) | summarize Rows=count() by MetricName`,
            [
              "t3_provider_operations_total",
              "t3_provider_operation_duration",
              "t3_provider_first_response_duration",
              "t3_provider_post_settlement_tail_duration",
              "t3_provider_used_tokens",
              "t3_provider_context_size",
            ],
          );
        }).pipe(Effect.provide(FetchHttpClient.layer)),
      SMOKE_TIMEOUT_MS,
    );
  },
);
