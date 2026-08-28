import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";

import {
  completePrefix,
  deterministicBatchId,
  ingestionCommand,
  transformTraceRecord,
} from "./worker.mjs";

function attribute(key, value) {
  const field =
    typeof value === "boolean"
      ? "boolValue"
      : typeof value === "number"
        ? "doubleValue"
        : "stringValue";
  return { key, value: { [field]: value } };
}

NodeTest.test("only transforms the allowlisted provider operation span", () => {
  const sensitiveValues = [
    "private prompt",
    "raw error text",
    "stack trace value",
    "/private/repository/file.ts",
    "rm -rf build",
    "secret tool input",
    "private-repository-name",
    "thread-secret",
    "turn-secret",
    "provider-instance-secret",
    "agency copilot --acp",
    "internal-mcp-name",
    "custom-model-secret",
  ];
  const record = {
    resourceSpans: [
      {
        scopeSpans: [
          {
            spans: [
              {
                name: "some.other.span",
                endTimeUnixNano: "1700000000000000000",
                attributes: [attribute("provider.operation", "ignored")],
              },
              {
                name: "t3.provider.operation",
                endTimeUnixNano: "1700000000000000000",
                attributes: [
                  attribute("provider.name", "githubCopilot"),
                  attribute("provider.operation", "turn"),
                  attribute("provider.outcome", "success"),
                  attribute("provider.timestamp_unix_ms", 1_700_000_000_000),
                  attribute("provider.duration_ms", 125),
                  attribute("provider.used_tokens", 800),
                  attribute("provider.context_size", 4000),
                  attribute("provider.retryable", false),
                  attribute("provider.permission_decision", "accept_always"),
                  ...sensitiveValues.map((value, index) =>
                    attribute(`unapproved.secret_${index}`, value),
                  ),
                ],
              },
              {
                name: "t3.provider.operation",
                endTimeUnixNano: "1700000000000000000",
                attributes: [
                  attribute("provider.name", "githubCopilot"),
                  attribute("provider.operation", "turn"),
                  attribute("provider.outcome", "success"),
                  attribute("provider.error_code", sensitiveValues[1]),
                  attribute("provider.stop_reason", sensitiveValues[0]),
                  attribute("provider.interaction_mode", sensitiveValues[10]),
                  attribute("provider.runtime_mode", sensitiveValues[9]),
                  attribute("provider.tool_kind", sensitiveValues[4]),
                  attribute("provider.permission_decision", sensitiveValues[11]),
                  attribute("provider.cli_version_bucket", sensitiveValues[12]),
                ],
              },
            ],
          },
        ],
      },
    ],
  };

  const transformed = transformTraceRecord(record);
  NodeAssert.equal(transformed.operations.length, 2);
  NodeAssert.equal(transformed.operations[0].Provider, "githubCopilot");
  NodeAssert.equal(transformed.operations[0].DurationMs, 125);
  NodeAssert.equal(transformed.operations[0].Stage, "");
  NodeAssert.equal(transformed.operations[0].PermissionRequested, null);
  NodeAssert.equal(transformed.operations[0].PermissionDecision, "accept_always");
  NodeAssert.deepEqual(
    transformed.metrics.map((row) => row.MetricName),
    [
      "t3_provider_operations_total",
      "t3_provider_operation_duration",
      "t3_provider_used_tokens",
      "t3_provider_context_size",
      "t3_provider_operations_total",
    ],
  );
  const serialized = JSON.stringify(transformed);
  for (const sensitive of sensitiveValues) {
    NodeAssert.doesNotMatch(
      serialized,
      new RegExp(sensitive.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")),
    );
  }
  NodeAssert.equal(transformed.operations[1].ErrorCode, "");
  NodeAssert.equal(transformed.operations[1].CliVersionBucket, "unknown");
});

NodeTest.test("leaves an incomplete OTLP JSON line for the next read", () => {
  const bytes = Buffer.from('{"one":1}\n{"two":');
  NodeAssert.equal(completePrefix(bytes).toString(), '{"one":1}\n');
  NodeAssert.equal(completePrefix(Buffer.from('{"one":1}')).length, 0);
});

NodeTest.test("preserves the subagent tool kind", () => {
  const transformed = transformTraceRecord({
    resourceSpans: [
      {
        scopeSpans: [
          {
            spans: [
              {
                name: "t3.provider.operation",
                endTimeUnixNano: "1700000000000000000",
                attributes: [
                  attribute("provider.name", "githubCopilot"),
                  attribute("provider.operation", "tool"),
                  attribute("provider.outcome", "success"),
                  attribute("provider.tool_kind", "subagent"),
                  attribute("provider.tool_outcome", "completed"),
                ],
              },
            ],
          },
        ],
      },
    ],
  });

  NodeAssert.equal(transformed.operations[0].ToolKind, "subagent");
});

NodeTest.test("batch identity is deterministic and source-aware", () => {
  const bytes = Buffer.from('{"resourceSpans":[]}\n');
  NodeAssert.equal(
    deterministicBatchId("traces", 0, bytes),
    deterministicBatchId("traces", 0, bytes),
  );
  NodeAssert.notEqual(
    deterministicBatchId("traces", 0, bytes),
    deterministicBatchId("metrics", 0, bytes),
  );
});

NodeTest.test("ingestion uses matching ingest-by tags for idempotency", () => {
  const command = ingestionCommand(
    "ProviderOperations",
    "ProviderOperationsJsonMapping",
    "batch-1",
    "/staging/batch-1.json",
  );
  NodeAssert.match(command, /tags='\["ingest-by:t3code-batch:batch-1:ProviderOperations"\]'/);
  NodeAssert.match(command, /ingestIfNotExists='\["t3code-batch:batch-1:ProviderOperations"\]'/);
});
