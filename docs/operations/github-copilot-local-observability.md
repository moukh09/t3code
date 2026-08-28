# GitHub Copilot local observability

> For maintainers. This stack stores only the bounded GitHub Copilot provider telemetry emitted by
> the `t3.provider.operation` span. It is local development infrastructure, not a production
> deployment.

The Compose stack in `infra/local-observability` runs:

- the Kusto Emulator at `http://localhost:8080`
- an OpenTelemetry Collector OTLP/HTTP receiver at `http://localhost:4318`
- a dependency-free Node ingestion worker

The collector filters its trace pipeline to `t3.provider.operation`, then writes newline-delimited
OTLP JSON to a shared volume. The worker waits for complete lines and projects only allowlisted
`provider.*` attributes into the fixed `ProviderOperations` and `ProviderMetrics` tables in the
`T3CodeLocal` database. Raw OTLP metrics are checkpointed but never ingested. Neither the Collector
trace files nor the Kusto tables store prompts, responses, paths, IDs, raw errors, or arbitrary
attributes.

## Start

From the repository root:

```sh
docker compose -f infra/local-observability/compose.yaml up -d --build
```

Or run `docker compose up -d --build` from `infra/local-observability`.

Do not launch T3 Code until the ingestion worker reports that schema initialization completed:

```sh
docker compose -f infra/local-observability/compose.yaml logs -f ingestion-worker
```

Set the OTLP endpoints in the shell that launches T3 Code:

```sh
export T3CODE_OTLP_TRACES_URL=http://localhost:4318/v1/traces
export T3CODE_OTLP_METRICS_URL=http://localhost:4318/v1/metrics
export T3CODE_OTLP_SERVICE_NAME=t3-github-copilot-local
```

PowerShell:

```powershell
$env:T3CODE_OTLP_TRACES_URL = "http://localhost:4318/v1/traces"
$env:T3CODE_OTLP_METRICS_URL = "http://localhost:4318/v1/metrics"
$env:T3CODE_OTLP_SERVICE_NAME = "t3-github-copilot-local"
```

Fully restart T3 Code after changing these variables.

## Query

The Kusto query endpoint is `http://localhost:8080/v1/rest/query`; the container-to-container
endpoint used by the worker is `http://kusto:8080`. Submit a query with:

```sh
curl --fail-with-body --json \
  '{"db":"T3CodeLocal","csl":"ProviderOperations | order by Timestamp desc | take 20"}' \
  http://localhost:8080/v1/rest/query
```

Ready-to-run investigations are in `infra/local-observability/queries`:

- `errors-error-rate.kql`
- `latency-percentiles.kql` for p50/p95 probe, session, first-response, and turn latency
- `outcomes-stop-reasons.kql`
- `tails-cross-turn.kql`
- `tools-permissions.kql`
- `token-context-utilization.kql`

Paste a file into a Kusto client connected to the `T3CodeLocal` database, or send its query text to
the REST endpoint. The table schemas and JSON mappings are in
`infra/local-observability/kusto/schema.kql`.

## Smoke test

The real smoke test starts the direct provider, runs the provider probe and one Copilot turn, then
flushes its spans through the local Collector:

```powershell
$env:T3CODE_OTLP_TRACES_URL = "http://localhost:4318/v1/traces"
$env:T3CODE_OTLP_METRICS_URL = "http://localhost:4318/v1/metrics"
$env:T3_GITHUB_COPILOT_OBSERVABILITY_SMOKE = "1"
npx vp test run apps/server/src/provider/GitHubCopilotObservabilitySmoke.test.ts
```

Query both tables after the test exits:

```sh
curl --fail-with-body --json \
  '{"db":"T3CodeLocal","csl":"ProviderOperations | where Provider == \"githubCopilot\" | summarize Rows=count() by Operation"}' \
  http://localhost:8080/v1/rest/query
curl --fail-with-body --json \
  '{"db":"T3CodeLocal","csl":"ProviderMetrics | where Provider == \"githubCopilot\" | summarize Rows=count() by MetricName"}' \
  http://localhost:8080/v1/rest/query
```

For a stack-only check without launching Copilot, send one privacy-safe span directly to the
collector:

```sh
curl --fail-with-body --json '{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"t3.provider.operation","kind":1,"startTimeUnixNano":"1700000000000000000","endTimeUnixNano":"1700000000125000000","attributes":[{"key":"provider.name","value":{"stringValue":"githubCopilot"}},{"key":"provider.operation","value":{"stringValue":"turn"}},{"key":"provider.outcome","value":{"stringValue":"success"}},{"key":"provider.duration_ms","value":{"doubleValue":125}},{"key":"provider.cli_version_bucket","value":{"stringValue":"0.0"}}],"status":{}}]}]}]}' \
  http://localhost:4318/v1/traces
```

Wait a few seconds, then query the marker:

```sh
curl --fail-with-body --json \
  '{"db":"T3CodeLocal","csl":"ProviderOperations | where CliVersionBucket == \"0.0\" | project Timestamp, Provider, Operation, Outcome, DurationMs"}' \
  http://localhost:8080/v1/rest/query
```

Repeating ingestion of the same staged batch is safe: the worker derives its immutable batch name
from the source bytes and offset, supplies the same tag through both `tags` and
`ingestIfNotExists`, and advances its persisted checkpoint only after both table ingestions finish.

## Stop and restart

Stop containers while preserving all named volumes:

```sh
docker compose -f infra/local-observability/compose.yaml down
```

Restart with the normal start command. Kusto data remains in the exact named volume
`t3code-kusto-data` at `/kustodata`. The worker reattaches or creates `T3CodeLocal`, reapplies the
fixed schemas and mappings, and resumes from `t3code-ingestion-state`.

## Reset

This permanently deletes local observability data, raw OTLP files, staged batches, and checkpoints:

```sh
docker compose -f infra/local-observability/compose.yaml down -v
```

If a volume was left behind by an interrupted Compose operation, verify that no stack container is
using it before removing the named volumes individually:

```sh
docker volume rm t3code-kusto-data t3code-otel-files t3code-ingestion-state t3code-ingestion-staging
```

## Retention

On startup and hourly thereafter, the worker runs:

```kusto
.drop extents older 30 days from ProviderMetrics
.drop extents older 30 days from ProviderOperations
```

This removes Kusto extents older than 30 days. The Collector rotates raw OTLP files at 10 MB and
keeps at most 30 days or 100 backups. The worker deletes immutable staged batches after 30 days.
Checkpoints remain until reset so a retained batch cannot be ingested twice after restart.

## Troubleshooting

Show container state and worker logs:

```sh
docker compose -f infra/local-observability/compose.yaml ps
docker compose -f infra/local-observability/compose.yaml logs --tail=200 ingestion-worker
docker compose -f infra/local-observability/compose.yaml logs --tail=200 otel-collector kusto
```

- **Port 4318 or 8080 is already allocated:** stop the conflicting local service. The stack binds
  both endpoints only to `127.0.0.1`. If port 8080 must remain occupied, set
  `KUSTO_HOST_PORT=8082` for Compose and use `http://localhost:8082` for local queries. Container
  traffic still uses `http://kusto:8080`; the checked-in default remains port 8080.
- **Local trace file updates but Kusto does not:** confirm the app inherited both
  `T3CODE_OTLP_*_URL` variables, then fully restart it. Check collector logs before worker logs.
- **Collector receives data but tables stay empty:** only the exact span name
  `t3.provider.operation` is accepted. Metrics and other spans are intentionally checkpointed and
  ignored. Confirm the span has an OTLP timestamp and allowlisted `provider.*` attributes.
- **Worker repeatedly restarts during initial startup:** Kusto can take time to become responsive.
  The worker retries the endpoint. A persistent management error is visible in worker logs.
- **A partial final line is present in a collector file:** this is expected while the exporter is
  writing. The worker leaves it uncheckpointed until its terminating newline arrives.
- **Data reappears after `down`:** `down` preserves named volumes. Use the reset procedure when the
  data itself must be deleted.
