import type { ProviderInteractionMode, RuntimeMode } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Metric from "effect/Metric";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as EffectAcpErrors from "effect-acp/errors";

import {
  increment,
  metricAttributes,
  providerFirstResponseDuration,
  providerOperationDuration,
  providerOperationsTotal,
  providerPostSettlementTailDuration,
  providerTokenUsage,
} from "../observability/Metrics.ts";

export const GITHUB_COPILOT_TELEMETRY_STAGES = [
  "spawn",
  "initialize",
  "authenticate",
  "session_new",
  "session_load",
  "config",
  "prompt",
  "permission",
  "cancel",
  "notification",
  "shutdown",
] as const;

export type GitHubCopilotTelemetryStage = (typeof GITHUB_COPILOT_TELEMETRY_STAGES)[number];

export const GITHUB_COPILOT_TELEMETRY_OPERATIONS = [
  "probe",
  "spawn",
  "session_new",
  "session_load",
  "turn",
  "first_response",
  "post_settlement_tail",
  "tool",
  "permission",
  "usage",
  "config",
  "cancel",
  "notification",
  "shutdown",
] as const;

export type GitHubCopilotTelemetryOperation = (typeof GITHUB_COPILOT_TELEMETRY_OPERATIONS)[number];

export type GitHubCopilotTelemetryOutcome = "success" | "failure" | "cancelled" | "interrupted";

export const GITHUB_COPILOT_TELEMETRY_ERROR_CODES = [
  "unknown",
  "acp_spawn",
  "acp_process_exited",
  "acp_protocol_parse",
  "acp_transport",
  "acp_input_stream_ended",
  "acp_request",
  "authentication_required",
  "command_missing",
  "version_probe_failed",
  "version_probe_timeout",
  "version_probe_exit",
  "model_discovery_failed",
  "model_discovery_timeout",
  "model_discovery_empty",
] as const;

export type GitHubCopilotTelemetryErrorCode = (typeof GITHUB_COPILOT_TELEMETRY_ERROR_CODES)[number];

export type GitHubCopilotPermissionDecision =
  | "accept"
  | "accept_for_session"
  | "accept_always"
  | "decline"
  | "cancel"
  | "auto_approved";

export type GitHubCopilotToolKind =
  | "read"
  | "edit"
  | "delete"
  | "move"
  | "search"
  | "execute"
  | "think"
  | "fetch"
  | "switch_mode"
  | "subagent"
  | "other";

export interface GitHubCopilotTelemetryRecord {
  readonly operation: GitHubCopilotTelemetryOperation;
  readonly outcome: GitHubCopilotTelemetryOutcome;
  readonly stage?: GitHubCopilotTelemetryStage | undefined;
  readonly durationMs?: number | undefined;
  readonly stopReason?:
    | "end_turn"
    | "max_tokens"
    | "max_turn_requests"
    | "refusal"
    | "cancelled"
    | undefined;
  readonly interactionMode?: ProviderInteractionMode | undefined;
  readonly runtimeMode?: RuntimeMode | undefined;
  readonly errorCode?: GitHubCopilotTelemetryErrorCode | undefined;
  readonly acpErrorCode?: string | undefined;
  readonly retryable?: boolean | undefined;
  readonly toolKind?: GitHubCopilotToolKind | undefined;
  readonly toolOutcome?: "completed" | "failed" | undefined;
  readonly permissionRequested?: boolean | undefined;
  readonly permissionDecision?: GitHubCopilotPermissionDecision | undefined;
  readonly usedTokens?: number | undefined;
  readonly contextSize?: number | undefined;
  readonly cliVersionBucket: string;
  readonly crossedTurnWindow?: boolean | undefined;
}

export function normalizeGitHubCopilotToolKind(
  kind: string | null | undefined,
): GitHubCopilotToolKind {
  switch (kind) {
    case "read":
    case "edit":
    case "delete":
    case "move":
    case "search":
    case "execute":
    case "think":
    case "fetch":
    case "switch_mode":
      return kind;
    default:
      return "other";
  }
}

export type GitHubCopilotTelemetrySink = (
  record: GitHubCopilotTelemetryRecord,
) => Effect.Effect<void>;

export class GitHubCopilotOperationError extends Schema.TaggedErrorClass<GitHubCopilotOperationError>()(
  "GitHubCopilotOperationError",
  {
    operation: Schema.Literals(GITHUB_COPILOT_TELEMETRY_OPERATIONS),
    stage: Schema.Literals(GITHUB_COPILOT_TELEMETRY_STAGES),
    errorCode: Schema.Literals(GITHUB_COPILOT_TELEMETRY_ERROR_CODES),
    acpErrorCode: Schema.optional(Schema.String),
    retryable: Schema.Boolean,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `GitHub Copilot operation '${this.operation}' failed at stage '${this.stage}' (${this.errorCode}).`;
  }
}

const isAcpError = Schema.is(EffectAcpErrors.AcpError);

function stageFromMethod(
  method: string | undefined,
  fallback: GitHubCopilotTelemetryStage,
): GitHubCopilotTelemetryStage {
  switch (method) {
    case "initialize":
      return "initialize";
    case "authenticate":
      return "authenticate";
    case "session/new":
      return "session_new";
    case "session/load":
      return "session_load";
    case "session/set_config_option":
    case "session/set_model":
      return "config";
    case "session/prompt":
      return "prompt";
    case "session/request_permission":
      return "permission";
    case "session/cancel":
      return "cancel";
    default:
      return fallback;
  }
}

export function classifyGitHubCopilotOperationError(input: {
  readonly operation: GitHubCopilotTelemetryOperation;
  readonly fallbackStage: GitHubCopilotTelemetryStage;
  readonly cause: unknown;
}): GitHubCopilotOperationError {
  const nestedCause =
    Predicate.isObject(input.cause) && "cause" in input.cause ? input.cause.cause : undefined;
  const candidate = isAcpError(input.cause)
    ? input.cause
    : isAcpError(nestedCause)
      ? nestedCause
      : undefined;
  if (!candidate) {
    return new GitHubCopilotOperationError({
      operation: input.operation,
      stage: input.fallbackStage,
      errorCode: "unknown",
      retryable: false,
      cause: input.cause,
    });
  }

  const cause = candidate;
  switch (cause._tag) {
    case "AcpSpawnError":
      return new GitHubCopilotOperationError({
        operation: input.operation,
        stage: "spawn",
        errorCode: "acp_spawn",
        retryable: true,
        cause,
      });
    case "AcpProcessExitedError":
      return new GitHubCopilotOperationError({
        operation: input.operation,
        stage: input.fallbackStage,
        errorCode: "acp_process_exited",
        retryable: true,
        cause,
      });
    case "AcpProtocolParseError":
      return new GitHubCopilotOperationError({
        operation: input.operation,
        stage: stageFromMethod(cause.method, input.fallbackStage),
        errorCode: "acp_protocol_parse",
        retryable: false,
        cause,
      });
    case "AcpTransportError":
      return new GitHubCopilotOperationError({
        operation: input.operation,
        stage: stageFromMethod(cause.method, input.fallbackStage),
        errorCode: "acp_transport",
        retryable: true,
        cause,
      });
    case "AcpInputStreamEndedError":
      return new GitHubCopilotOperationError({
        operation: input.operation,
        stage: input.fallbackStage,
        errorCode: "acp_input_stream_ended",
        retryable: true,
        cause,
      });
    case "AcpRequestError":
      return new GitHubCopilotOperationError({
        operation: input.operation,
        stage: stageFromMethod(cause.method, input.fallbackStage),
        errorCode: cause.code === -32_000 ? "authentication_required" : "acp_request",
        acpErrorCode: String(cause.code),
        retryable: cause.code <= -32_000 && cause.code > -32_100,
        cause,
      });
  }
}

export function normalizeGitHubCopilotCliVersionBucket(version: string | null | undefined): string {
  const match = version?.trim().match(/^(\d+)\.(\d+)(?:\.|$)/);
  return match ? `${match[1]}.${match[2]}` : "unknown";
}

const KNOWN_ACP_ERROR_CODES = new Set([
  "-32700",
  "-32600",
  "-32601",
  "-32602",
  "-32603",
  "-32800",
  "-32000",
  "-32002",
  "-32042",
]);

export function normalizeGitHubCopilotAcpErrorCode(code: string | undefined): string | undefined {
  if (code === undefined) return undefined;
  return KNOWN_ACP_ERROR_CODES.has(code) ? code : "other";
}

function telemetryAttributes(record: GitHubCopilotTelemetryRecord) {
  return {
    provider: "githubCopilot",
    operation: record.operation,
    outcome: record.outcome,
    stage: record.stage,
    stopReason: record.stopReason,
    interactionMode: record.interactionMode,
    runtimeMode: record.runtimeMode,
    errorCode: record.errorCode,
    acpErrorCode: normalizeGitHubCopilotAcpErrorCode(record.acpErrorCode),
    retryable: record.retryable,
    toolKind: record.toolKind,
    toolOutcome: record.toolOutcome,
    permissionRequested: record.permissionRequested,
    permissionDecision: record.permissionDecision,
    cliVersionBucket: record.cliVersionBucket,
    crossedTurnWindow: record.crossedTurnWindow,
  };
}

function traceAttributes(record: GitHubCopilotTelemetryRecord, timestampMs: number) {
  return {
    "provider.name": "githubCopilot",
    "provider.operation": record.operation,
    "provider.outcome": record.outcome,
    "provider.stage": record.stage ?? "",
    "provider.duration_ms": record.durationMs,
    "provider.error_code": record.errorCode ?? "",
    "provider.acp_error_code": record.acpErrorCode ?? "",
    "provider.retryable": record.retryable,
    "provider.stop_reason": record.stopReason ?? "",
    "provider.interaction_mode": record.interactionMode ?? "",
    "provider.runtime_mode": record.runtimeMode ?? "",
    "provider.tool_kind": record.toolKind ?? "",
    "provider.tool_outcome": record.toolOutcome ?? "",
    "provider.permission_requested": record.permissionRequested,
    "provider.permission_decision": record.permissionDecision ?? "",
    "provider.used_tokens": record.usedTokens,
    "provider.context_size": record.contextSize,
    "provider.cli_version_bucket": record.cliVersionBucket,
    "provider.crossed_turn_window": record.crossedTurnWindow,
    "provider.timestamp_unix_ms": timestampMs,
  };
}

export const recordGitHubCopilotTelemetry: GitHubCopilotTelemetrySink = Effect.fn(
  "t3.provider.operation",
)(function* (record: GitHubCopilotTelemetryRecord) {
  const timestampMs = yield* Effect.sync(Date.now);
  const attributes = telemetryAttributes(record);
  yield* increment(providerOperationsTotal, attributes);

  if (record.durationMs !== undefined) {
    const metric =
      record.operation === "first_response"
        ? providerFirstResponseDuration
        : record.operation === "post_settlement_tail"
          ? providerPostSettlementTailDuration
          : providerOperationDuration;
    yield* Metric.update(
      Metric.withAttributes(metric, metricAttributes(attributes)),
      Duration.millis(record.durationMs),
    );
  }

  if (record.usedTokens !== undefined) {
    yield* Metric.update(
      Metric.withAttributes(
        providerTokenUsage,
        metricAttributes({ ...attributes, measurement: "used_tokens" }),
      ),
      record.usedTokens,
    );
  }
  if (record.contextSize !== undefined) {
    yield* Metric.update(
      Metric.withAttributes(
        providerTokenUsage,
        metricAttributes({ ...attributes, measurement: "context_size" }),
      ),
      record.contextSize,
    );
  }

  yield* Effect.annotateCurrentSpan(traceAttributes(record, timestampMs));
});

export function telemetryFailureRecord(input: {
  readonly operation: GitHubCopilotTelemetryOperation;
  readonly fallbackStage: GitHubCopilotTelemetryStage;
  readonly cause: unknown;
  readonly durationMs?: number | undefined;
  readonly cliVersionBucket: string;
  readonly interactionMode?: ProviderInteractionMode | undefined;
  readonly runtimeMode?: RuntimeMode | undefined;
}): GitHubCopilotTelemetryRecord {
  const error = classifyGitHubCopilotOperationError(input);
  return {
    operation: input.operation,
    outcome: "failure",
    stage: error.stage,
    errorCode: error.errorCode,
    acpErrorCode: error.acpErrorCode,
    retryable: error.retryable,
    durationMs: input.durationMs,
    cliVersionBucket: input.cliVersionBucket,
    interactionMode: input.interactionMode,
    runtimeMode: input.runtimeMode,
  };
}
