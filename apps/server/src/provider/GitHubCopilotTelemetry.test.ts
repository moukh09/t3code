import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Metric from "effect/Metric";
import * as AcpErrors from "effect-acp/errors";

import {
  classifyGitHubCopilotOperationError,
  normalizeGitHubCopilotAcpErrorCode,
  normalizeGitHubCopilotCliVersionBucket,
  recordGitHubCopilotTelemetry,
  telemetryFailureRecord,
} from "./GitHubCopilotTelemetry.ts";

describe("GitHubCopilotTelemetry", () => {
  it("classifies ACP failures without copying sensitive data", () => {
    const sensitiveValues = [
      "prompt=do-not-export",
      "raw-error-text",
      "stack-trace-value",
      "C:\\private\\repo\\file.ts",
      "rm -rf build",
      '{"toolInput":"secret"}',
      "private-repository-name",
      "thread-secret",
      "turn-secret",
      "provider-instance-secret",
      "agency copilot --acp",
      "internal-mcp-name",
      "custom-model-secret",
    ];
    const secret = sensitiveValues.join(" ");
    const classified = classifyGitHubCopilotOperationError({
      operation: "session_new",
      fallbackStage: "session_new",
      cause: new AcpErrors.AcpRequestError({
        code: -32_000,
        errorMessage: secret,
        method: "authenticate",
        data: { secret },
      }),
    });
    const record = telemetryFailureRecord({
      operation: "session_new",
      fallbackStage: "session_new",
      cause: classified.cause,
      cliVersionBucket: "1.0",
    });

    assert.equal(classified.stage, "authenticate");
    assert.equal(classified.errorCode, "authentication_required");
    assert.equal(classified.acpErrorCode, "-32000");
    const exported = `${classified.message}\n${JSON.stringify(record)}`;
    for (const sensitive of sensitiveValues) {
      assert.notInclude(exported, sensitive);
    }
  });

  it("buckets only numeric CLI major and minor versions", () => {
    assert.equal(normalizeGitHubCopilotCliVersionBucket("1.0.81-5"), "1.0");
    assert.equal(normalizeGitHubCopilotCliVersionBucket("custom-model"), "unknown");
    assert.equal(normalizeGitHubCopilotCliVersionBucket(undefined), "unknown");
  });

  it("keeps ACP metric error-code labels bounded", () => {
    assert.equal(normalizeGitHubCopilotAcpErrorCode("-32000"), "-32000");
    assert.equal(normalizeGitHubCopilotAcpErrorCode("123456"), "other");
    assert.equal(normalizeGitHubCopilotAcpErrorCode(undefined), undefined);
  });

  it("keeps provider failure stages distinct", () => {
    const cases = [
      [new AcpErrors.AcpSpawnError({ command: "secret-command", cause: "secret" }), "spawn"],
      [
        new AcpErrors.AcpRequestError({
          code: -32_000,
          errorMessage: "secret",
          method: "authenticate",
        }),
        "authenticate",
      ],
      [
        new AcpErrors.AcpRequestError({
          code: -32_603,
          errorMessage: "secret",
          method: "session/set_config_option",
        }),
        "config",
      ],
      [
        new AcpErrors.AcpRequestError({
          code: -32_603,
          errorMessage: "secret",
          method: "session/prompt",
        }),
        "prompt",
      ],
      [
        new AcpErrors.AcpTransportError({
          method: "session/cancel",
          operation: "call-rpc",
          cause: "secret",
        }),
        "cancel",
      ],
    ] as const;

    assert.deepStrictEqual(
      cases.map(
        ([cause]) =>
          classifyGitHubCopilotOperationError({
            operation: "turn",
            fallbackStage: "prompt",
            cause,
          }).stage,
      ),
      cases.map(([, stage]) => stage),
    );
  });

  it.effect("records bounded Effect metrics", () =>
    Effect.gen(function* () {
      yield* recordGitHubCopilotTelemetry({
        operation: "turn",
        outcome: "success",
        stage: "prompt",
        durationMs: 125,
        stopReason: "end_turn",
        interactionMode: "plan",
        runtimeMode: "approval-required",
        cliVersionBucket: "1.0",
      });

      const snapshots = yield* Metric.snapshot;
      const counter = snapshots.find(
        (snapshot) =>
          snapshot.id === "t3_provider_operations_total" &&
          snapshot.attributes?.provider === "githubCopilot" &&
          snapshot.attributes?.operation === "turn" &&
          snapshot.attributes?.outcome === "success",
      );
      const timer = snapshots.find(
        (snapshot) =>
          snapshot.id === "t3_provider_operation_duration" &&
          snapshot.attributes?.provider === "githubCopilot" &&
          snapshot.attributes?.operation === "turn",
      );

      assert.exists(counter);
      assert.exists(timer);
      assert.notProperty(counter?.attributes ?? {}, "threadId");
      assert.notProperty(counter?.attributes ?? {}, "turnId");
      assert.notProperty(counter?.attributes ?? {}, "model");
    }),
  );
});
