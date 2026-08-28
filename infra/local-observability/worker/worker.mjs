import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";

const { createHash } = NodeCrypto;
const { mkdir, open, readdir, readFile, rename, stat, unlink, writeFile } = NodeFSP;
const { pathToFileURL } = NodeURL;

const endpoint = process.env.KUSTO_ENDPOINT ?? "http://kusto:8080";
const database = process.env.KUSTO_DATABASE ?? "T3CodeLocal";
const adminDatabase = process.env.KUSTO_ADMIN_DATABASE ?? "NetDefaultDB";
const pollIntervalMs = positiveInteger(process.env.INGEST_POLL_INTERVAL_MS, 2_000);
const retentionIntervalMs = positiveInteger(process.env.RETENTION_INTERVAL_MS, 3_600_000);
const maxReadBytes = positiveInteger(process.env.MAX_READ_BYTES, 64 * 1024 * 1024);
const statePath = "/state/checkpoints.json";

const sourceDefinitions = [
  { kind: "traces", baseName: "traces", transform: true },
  { kind: "metrics", baseName: "metrics", transform: false },
];

const allowedAttributes = new Set([
  "provider.name",
  "provider.operation",
  "provider.outcome",
  "provider.stage",
  "provider.duration_ms",
  "provider.error_code",
  "provider.acp_error_code",
  "provider.retryable",
  "provider.stop_reason",
  "provider.interaction_mode",
  "provider.runtime_mode",
  "provider.tool_kind",
  "provider.tool_outcome",
  "provider.permission_requested",
  "provider.permission_decision",
  "provider.used_tokens",
  "provider.context_size",
  "provider.cli_version_bucket",
  "provider.crossed_turn_window",
  "provider.timestamp_unix_ms",
]);
const allowedOperations = new Set([
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
]);
const allowedOutcomes = new Set(["success", "failure", "cancelled", "interrupted"]);
const allowedStages = new Set([
  "",
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
]);
const allowedStopReasons = new Set([
  "",
  "end_turn",
  "max_tokens",
  "max_turn_requests",
  "refusal",
  "cancelled",
]);
const allowedInteractionModes = new Set(["", "default", "plan"]);
const allowedRuntimeModes = new Set([
  "",
  "approval-required",
  "auto",
  "auto-accept-edits",
  "full-access",
]);
const allowedErrorCodes = new Set([
  "",
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
]);
const allowedToolKinds = new Set([
  "",
  "read",
  "edit",
  "delete",
  "move",
  "search",
  "execute",
  "think",
  "fetch",
  "switch_mode",
  "subagent",
  "other",
]);
const allowedToolOutcomes = new Set(["", "completed", "failed"]);
const allowedPermissionDecisions = new Set([
  "",
  "accept",
  "accept_for_session",
  "accept_always",
  "decline",
  "cancel",
  "auto_approved",
]);

function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

export function completePrefix(buffer) {
  const newline = buffer.lastIndexOf(0x0a);
  return newline < 0 ? Buffer.alloc(0) : buffer.subarray(0, newline + 1);
}

export function deterministicBatchId(source, start, completeBytes) {
  return sha256(Buffer.concat([Buffer.from(`${source}\0${start}\0`, "utf8"), completeBytes]));
}

function otlpValue(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== "object") return value;
  for (const key of [
    "stringValue",
    "string_value",
    "intValue",
    "int_value",
    "doubleValue",
    "double_value",
    "boolValue",
    "bool_value",
  ]) {
    if (Object.hasOwn(value, key)) return value[key];
  }
  return undefined;
}

function privacySafeAttributes(span) {
  const result = {};
  const attributes = Array.isArray(span.attributes) ? span.attributes : [];
  for (const attribute of attributes) {
    if (!attribute || !allowedAttributes.has(attribute.key)) continue;
    result[attribute.key] = otlpValue(attribute.value);
  }
  return result;
}

function stringValue(value) {
  return typeof value === "string" ? value : "";
}

function closedString(value, allowed, fallback = "") {
  const string = stringValue(value);
  return allowed.has(string) ? string : fallback;
}

function numberValue(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function nonNegativeNumber(value) {
  const parsed = numberValue(value);
  return parsed !== null && parsed >= 0 ? parsed : null;
}

function booleanValue(value) {
  return typeof value === "boolean" ? value : null;
}

function acpErrorCode(value) {
  const string = stringValue(value);
  return /^-?\d{1,10}$/.test(string) ? string : "";
}

function cliVersionBucket(value) {
  const string = stringValue(value);
  return string === "unknown" || /^\d+\.\d+$/.test(string) ? string : "unknown";
}

function spanTimestamp(span, attributes) {
  const explicit = nonNegativeNumber(attributes["provider.timestamp_unix_ms"]);
  if (explicit !== null && explicit <= 8_640_000_000_000_000) {
    return new Date(explicit).toISOString();
  }
  const raw =
    span.endTimeUnixNano ??
    span.end_time_unix_nano ??
    span.startTimeUnixNano ??
    span.start_time_unix_nano;
  if (raw === undefined || raw === null) return null;
  try {
    const milliseconds = Number(BigInt(raw) / 1_000_000n);
    if (!Number.isFinite(milliseconds)) return null;
    return new Date(milliseconds).toISOString();
  } catch {
    return null;
  }
}

function operationRow(span) {
  if (span.name !== "t3.provider.operation") return null;
  const attributes = privacySafeAttributes(span);
  const Timestamp = spanTimestamp(span, attributes);
  if (Timestamp === null) return null;
  const Provider = stringValue(attributes["provider.name"]);
  const Operation = closedString(attributes["provider.operation"], allowedOperations);
  if (Provider !== "githubCopilot" || Operation === "") return null;
  const Outcome = closedString(attributes["provider.outcome"], allowedOutcomes);
  const ErrorCode = closedString(
    attributes["provider.error_code"],
    allowedErrorCodes,
    Outcome === "failure" ? "unknown" : "",
  );
  return {
    Timestamp,
    Provider,
    Operation,
    Outcome,
    Stage: closedString(attributes["provider.stage"], allowedStages),
    DurationMs: nonNegativeNumber(attributes["provider.duration_ms"]),
    ErrorCode,
    AcpErrorCode: acpErrorCode(attributes["provider.acp_error_code"]),
    Retryable: booleanValue(attributes["provider.retryable"]),
    StopReason: closedString(attributes["provider.stop_reason"], allowedStopReasons),
    InteractionMode: closedString(attributes["provider.interaction_mode"], allowedInteractionModes),
    RuntimeMode: closedString(attributes["provider.runtime_mode"], allowedRuntimeModes),
    ToolKind: closedString(attributes["provider.tool_kind"], allowedToolKinds),
    ToolOutcome: closedString(attributes["provider.tool_outcome"], allowedToolOutcomes),
    PermissionRequested: booleanValue(attributes["provider.permission_requested"]),
    PermissionDecision: closedString(
      attributes["provider.permission_decision"],
      allowedPermissionDecisions,
    ),
    UsedTokens: nonNegativeNumber(attributes["provider.used_tokens"]),
    ContextSize: nonNegativeNumber(attributes["provider.context_size"]),
    CliVersionBucket: cliVersionBucket(attributes["provider.cli_version_bucket"]),
    CrossedTurnWindow: booleanValue(attributes["provider.crossed_turn_window"]),
  };
}

function metricDimensions(row) {
  return {
    Timestamp: row.Timestamp,
    Provider: row.Provider,
    Outcome: row.Outcome,
    Stage: row.Stage,
    Operation: row.Operation,
    StopReason: row.StopReason,
    InteractionMode: row.InteractionMode,
    RuntimeMode: row.RuntimeMode,
    CliVersionBucket: row.CliVersionBucket,
    CrossedTurnWindow: row.CrossedTurnWindow,
    ToolKind: row.ToolKind,
    ToolOutcome: row.ToolOutcome,
    PermissionRequested: row.PermissionRequested,
    PermissionDecision: row.PermissionDecision,
  };
}

function metricRows(row) {
  const dimensions = metricDimensions(row);
  const rows = [
    {
      ...dimensions,
      MetricName: "t3_provider_operations_total",
      Value: 1,
      Unit: "count",
    },
  ];
  if (row.DurationMs !== null) {
    const MetricName =
      row.Operation === "first_response"
        ? "t3_provider_first_response_duration"
        : row.Operation === "post_settlement_tail"
          ? "t3_provider_post_settlement_tail_duration"
          : "t3_provider_operation_duration";
    rows.push({ ...dimensions, MetricName, Value: row.DurationMs, Unit: "ms" });
  }
  if (row.UsedTokens !== null) {
    rows.push({
      ...dimensions,
      MetricName: "t3_provider_used_tokens",
      Value: row.UsedTokens,
      Unit: "tokens",
    });
  }
  if (row.ContextSize !== null) {
    rows.push({
      ...dimensions,
      MetricName: "t3_provider_context_size",
      Value: row.ContextSize,
      Unit: "tokens",
    });
  }
  return rows;
}

function spansFromRecord(record) {
  const resourceSpans = record.resourceSpans ?? record.resource_spans ?? [];
  const spans = [];
  for (const resourceSpan of resourceSpans) {
    const scopeSpans =
      resourceSpan.scopeSpans ??
      resourceSpan.scope_spans ??
      resourceSpan.instrumentationLibrarySpans ??
      resourceSpan.instrumentation_library_spans ??
      [];
    for (const scopeSpan of scopeSpans) {
      if (Array.isArray(scopeSpan.spans)) spans.push(...scopeSpan.spans);
    }
  }
  return spans;
}

export function transformTraceRecord(record) {
  const operations = [];
  const metrics = [];
  for (const span of spansFromRecord(record)) {
    const row = operationRow(span);
    if (row === null) continue;
    operations.push(row);
    metrics.push(...metricRows(row));
  }
  return { operations, metrics };
}

function parseCompleteRecords(bytes, transform) {
  const operations = [];
  const metrics = [];
  for (const line of bytes.toString("utf8").split("\n")) {
    if (line.trim() === "") continue;
    try {
      const record = JSON.parse(line);
      if (!transform) continue;
      const rows = transformTraceRecord(record);
      operations.push(...rows.operations);
      metrics.push(...rows.metrics);
    } catch (error) {
      throw new Error("A complete OTLP JSON record is invalid.", { cause: error });
    }
  }
  return { operations, metrics };
}

async function kustoRequest(path, db, csl) {
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ db, csl, properties: {} }),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Kusto ${response.status}: ${body.slice(0, 1_000)}`);
  }
  return body;
}

async function management(db, csl) {
  return kustoRequest("/v1/rest/mgmt", db, csl);
}

async function waitForKusto() {
  for (;;) {
    try {
      await management(adminDatabase, ".show databases");
      return;
    } catch (error) {
      console.log(`Waiting for Kusto at ${endpoint}: ${error.message}`);
      await sleep(2_000);
    }
  }
}

async function ensureDatabase() {
  try {
    await management(database, ".show tables");
    return;
  } catch {
    // Persisted databases must be attached again when the emulator forgets its catalog.
  }

  try {
    await management(
      adminDatabase,
      `.attach database ${database} from @"/kustodata/dbs/${database}/md"`,
    );
    console.log(`Attached persistent Kusto database ${database}.`);
    return;
  } catch {
    await management(
      adminDatabase,
      `.create database ${database} persist (` +
        `@"/kustodata/dbs/${database}/md", ` +
        `@"/kustodata/dbs/${database}/data")`,
    );
    console.log(`Created persistent Kusto database ${database}.`);
  }
}

function splitKqlCommands(text) {
  return text
    .split(/\r?\n\s*\r?\n/)
    .map((command) => command.trim())
    .filter((command) => command !== "" && !command.startsWith("//"));
}

async function runKqlFile(path) {
  const commands = splitKqlCommands(await readFile(path, "utf8"));
  for (const command of commands) await management(database, command);
}

async function loadState() {
  try {
    const parsed = JSON.parse(await readFile(statePath, "utf8"));
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (error) {
    if (error.code === "ENOENT") return {};
    throw error;
  }
}

async function saveState(state) {
  const pending = `${statePath}.next`;
  await writeFile(pending, `${JSON.stringify(state, null, 2)}\n`, "utf8");
  await rename(pending, statePath);
}

async function headHash(path, length) {
  if (length === 0) return sha256("");
  const handle = await open(path, "r");
  try {
    const bytes = Buffer.alloc(length);
    const { bytesRead } = await handle.read(bytes, 0, length, 0);
    return sha256(bytes.subarray(0, bytesRead));
  } finally {
    await handle.close();
  }
}

async function readUnreadCompleteBytes(source, checkpoint) {
  let details;
  try {
    details = await stat(source.path);
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }

  let offset = Number.isSafeInteger(checkpoint?.offset) ? checkpoint.offset : 0;
  const checkpointHeadLength = Number.isSafeInteger(checkpoint?.headLength)
    ? checkpoint.headLength
    : 0;
  const checkpointHeadHash =
    checkpointHeadLength <= details.size ? await headHash(source.path, checkpointHeadLength) : null;
  if (
    offset > details.size ||
    (checkpoint?.headHash && checkpoint.headHash !== checkpointHeadHash)
  ) {
    offset = 0;
  }
  const headLength = Math.min(details.size, 4_096);
  const currentHeadHash = await headHash(source.path, headLength);
  const length = Math.min(details.size - offset, maxReadBytes);
  if (length <= 0) return null;

  const handle = await open(source.path, "r");
  try {
    const unread = Buffer.alloc(length);
    const { bytesRead } = await handle.read(unread, 0, length, offset);
    const complete = completePrefix(unread.subarray(0, bytesRead));
    if (complete.length === 0) return null;
    return {
      bytes: complete,
      start: offset,
      end: offset + complete.length,
      headHash: currentHeadHash,
      headLength,
    };
  } finally {
    await handle.close();
  }
}

function ndjson(rows) {
  return `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`;
}

async function stageImmutable(path, content) {
  try {
    const existing = await readFile(path, "utf8");
    if (existing !== content) {
      throw new Error(`Immutable staged batch differs: ${path}`);
    }
    return;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }

  const pending = `${path}.pending-${process.pid}-${NodeCrypto.randomUUID()}`;
  await writeFile(pending, content, { encoding: "utf8", flag: "wx" });
  try {
    await rename(pending, path);
  } catch (error) {
    await unlink(pending).catch(() => {});
    if (error.code !== "EEXIST") throw error;
    const existing = await readFile(path, "utf8");
    if (existing !== content) {
      throw new Error(`Immutable staged batch differs: ${path}`, { cause: error });
    }
  }
}

export function ingestionCommand(table, mapping, batchId, containerPath) {
  const batchKey = `t3code-batch:${batchId}:${table}`;
  return (
    `.ingest into table ${table} ('${containerPath}') with (` +
    `format='multijson', ingestionMappingReference='${mapping}', ` +
    `tags='["ingest-by:${batchKey}"]', ingestIfNotExists='["${batchKey}"]')`
  );
}

async function ingestRows(table, mapping, batchId, rows) {
  if (rows.length === 0) return;
  const fileName = `${batchId}-${table}.json`;
  const containerPath = `/staging/${fileName}`;
  await stageImmutable(containerPath, ndjson(rows));
  await management(database, ingestionCommand(table, mapping, batchId, containerPath));
}

function completeLines(bytes) {
  const lines = [];
  let start = 0;
  for (let index = 0; index < bytes.length; index += 1) {
    if (bytes[index] !== 0x0a) continue;
    lines.push(bytes.subarray(start, index + 1));
    start = index + 1;
  }
  return lines;
}

async function listSources() {
  const entries = await readdir("/var/lib/otel", { withFileTypes: true }).catch((error) => {
    if (error.code === "ENOENT") return [];
    throw error;
  });
  const sources = [];
  for (const definition of sourceDefinitions) {
    const matching = entries
      .filter(
        (entry) =>
          entry.isFile() &&
          (entry.name === `${definition.baseName}.ndjson` ||
            (entry.name.startsWith(`${definition.baseName}-`) && entry.name.endsWith(".ndjson"))),
      )
      .map((entry) => entry.name)
      .sort((left, right) => {
        if (left === `${definition.baseName}.ndjson`) return 1;
        if (right === `${definition.baseName}.ndjson`) return -1;
        return left.localeCompare(right);
      });
    for (const name of matching) {
      sources.push({
        ...definition,
        key: `${definition.kind}:${name}`,
        path: `/var/lib/otel/${name}`,
      });
    }
  }
  return sources;
}

async function processSource(source, state) {
  const unread = await readUnreadCompleteBytes(source, state[source.key]);
  if (unread === null) return false;
  for (const line of completeLines(unread.bytes)) {
    const batchId = deterministicBatchId(source.kind, 0, line);
    const rows = parseCompleteRecords(line, source.transform);
    await ingestRows(
      "ProviderOperations",
      "ProviderOperationsJsonMapping",
      batchId,
      rows.operations,
    );
    await ingestRows("ProviderMetrics", "ProviderMetricsJsonMapping", batchId, rows.metrics);
  }

  state[source.key] = {
    offset: unread.end,
    headHash: unread.headHash,
    headLength: unread.headLength,
  };
  await saveState(state);
  return true;
}

async function cleanupStaging() {
  const cutoff = Date.now() - 30 * 24 * 60 * 60 * 1_000;
  const entries = await readdir("/staging", { withFileTypes: true });
  for (const entry of entries) {
    if (
      !entry.isFile() ||
      (!entry.name.endsWith(".json") && !entry.name.includes(".json.pending-"))
    ) {
      continue;
    }
    const path = `/staging/${entry.name}`;
    const details = await stat(path);
    if (details.mtimeMs < cutoff) await unlink(path);
  }
}

async function main() {
  await mkdir("/state", { recursive: true });
  await mkdir("/staging", { recursive: true });
  await waitForKusto();
  await ensureDatabase();
  await runKqlFile("/app/kusto/schema.kql");
  await runKqlFile("/app/kusto/retention.kql");
  await cleanupStaging();
  console.log(`Initialized ${database} schemas, mappings, and 30-day retention.`);

  const state = await loadState();
  let lastRetention = Date.now();
  for (;;) {
    let progressed = false;
    for (const source of await listSources()) {
      progressed = (await processSource(source, state)) || progressed;
    }
    if (Date.now() - lastRetention >= retentionIntervalMs) {
      await runKqlFile("/app/kusto/retention.kql");
      await cleanupStaging();
      lastRetention = Date.now();
    }
    if (!progressed) await sleep(pollIntervalMs);
  }
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
