// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ApprovalRequestId,
  GitHubCopilotSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import {
  githubCopilotPromptSettlementBelongsToContext,
  makeGitHubCopilotAdapter,
} from "./GitHubCopilotAdapter.ts";
import type { GitHubCopilotTelemetryRecord } from "../GitHubCopilotTelemetry.ts";

const decodeGitHubCopilotSettings = Schema.decodeSync(GitHubCopilotSettings);
const encodeUnknownJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const PROVIDER = ProviderDriverKind.make("githubCopilot");
const INSTANCE_ID = ProviderInstanceId.make("githubCopilot");
const TEST_TIMEOUT_MS = 30_000;

/**
 * A newline-delimited JSON-RPC agent shaped like `copilot --acp --stdio`:
 * config options for model/reasoning_effort/mode/allow_all, reasoning and
 * usage notifications, and an optional model list that only appears through
 * `config_option_update` after the first prompt.
 */
const MOCK_AGENT_SOURCE = String.raw`
import * as NodeFS from "node:fs";

const flag = (name) => process.env[name] === "1";
const requestLogPath = process.env.COPILOT_MOCK_REQUEST_LOG;
const SESSION_ID = "copilot-mock-session-1";

const lateModels = flag("COPILOT_MOCK_LATE_MODELS");
const wantsPermission = flag("COPILOT_MOCK_REQUEST_PERMISSION");
const hangPrompt = flag("COPILOT_MOCK_HANG_PROMPT");
const releasePromptOnConfig = flag("COPILOT_MOCK_RELEASE_PROMPT_ON_CONFIG");
const lateChunkAfterCancel = flag("COPILOT_MOCK_LATE_CHUNK_AFTER_CANCEL");
const staleChunkOnConfig = flag("COPILOT_MOCK_STALE_CHUNK_ON_CONFIG");

let modelsPublished = !lateModels;
let currentModel = "gpt-5.1-codex";
let currentReasoning = "medium";
let currentMode = "agent";
let allowAll = false;
let promptCount = 0;
let heldPromptId;
let configWriteSeen = false;
let nextOutgoingId = 9000;
const pendingOutgoing = new Map();

const write = (message) => {
  process.stdout.write(JSON.stringify(message) + "\n");
};
const respond = (id, result) => write({ jsonrpc: "2.0", id, result });
const notify = (method, params) => write({ jsonrpc: "2.0", method, params });
const sessionUpdate = (update) => notify("session/update", { sessionId: SESSION_ID, update });
const logEntry = (entry) => {
  if (requestLogPath) NodeFS.appendFileSync(requestLogPath, JSON.stringify(entry) + "\n", "utf8");
};

const configOptions = () => [
  ...(modelsPublished
    ? [
        {
          id: "model",
          name: "Model",
          category: "model",
          type: "select",
          currentValue: currentModel,
          options: [
            { value: "gpt-5.1-codex", name: "GPT-5.1-Codex" },
            { value: "claude-sonnet-4.5", name: "Claude Sonnet 4.5" },
          ],
        },
      ]
    : []),
  {
    id: "reasoning_effort",
    name: "Reasoning effort",
    category: "thought_level",
    type: "select",
    currentValue: currentReasoning,
    options: [
      { value: "low", name: "Low" },
      { value: "medium", name: "Medium" },
      { value: "high", name: "High" },
    ],
  },
  {
    id: "mode",
    name: "Mode",
    category: "mode",
    type: "select",
    currentValue: currentMode,
    options: [
      { value: "agent", name: "Agent" },
      { value: "plan", name: "Plan" },
      { value: "autopilot", name: "Autopilot" },
    ],
  },
  { id: "allow_all", name: "Allow all", type: "boolean", currentValue: allowAll },
];

const requestFromClient = (method, params) => {
  const id = nextOutgoingId++;
  return new Promise((resolve) => {
    pendingOutgoing.set(id, resolve);
    write({ jsonrpc: "2.0", id, method, params });
  });
};

const handlePrompt = async (id) => {
  promptCount += 1;
  const answerIndex = promptCount;
  sessionUpdate({
    sessionUpdate: "agent_thought_chunk",
    content: { type: "text", text: "weighing the options" },
  });
  sessionUpdate({
    sessionUpdate: "agent_message_chunk",
    content: { type: "text", text: "answer " + answerIndex },
  });
  sessionUpdate({ sessionUpdate: "usage_update", used: 1200, size: 128000 });
  if (lateModels && !modelsPublished) {
    // Copilot publishes its model list late, sometimes only mid-prompt.
    modelsPublished = true;
    sessionUpdate({ sessionUpdate: "config_option_update", configOptions: configOptions() });
  }
  if (wantsPermission) {
    const outcome = await requestFromClient("session/request_permission", {
      sessionId: SESSION_ID,
      toolCall: {
        toolCallId: "tool-call-1",
        title: "\u0060rm -rf build\u0060",
        kind: "execute",
        status: "pending",
      },
      options: [
        { optionId: "allow-once", name: "Allow once", kind: "allow_once" },
        { optionId: "allow-always", name: "Allow always", kind: "allow_always" },
        { optionId: "reject-once", name: "Reject", kind: "reject_once" },
      ],
    });
    logEntry({ kind: "permission-outcome", outcome });
  }
  if (hangPrompt || (releasePromptOnConfig && promptCount === 1 && !configWriteSeen)) {
    heldPromptId = id;
    return;
  }
  respond(id, { stopReason: "end_turn" });
};

const handleMessage = (message) => {
  if (message.method === undefined && message.id !== undefined) {
    const resolve = pendingOutgoing.get(message.id);
    if (resolve) {
      pendingOutgoing.delete(message.id);
      resolve(message.result ?? message.error);
    }
    return;
  }

  const { id, method, params } = message;
  logEntry({ kind: "request", method, params });

  switch (method) {
    case "initialize":
      respond(id, {
        protocolVersion: 1,
        agentCapabilities: {
          loadSession: true,
          promptCapabilities: { image: true, embeddedContext: true },
        },
        authMethods: [{ id: "copilot-login", name: "GitHub", description: null }],
      });
      return;
    case "authenticate":
      respond(id, {});
      return;
    case "session/new":
      respond(id, { sessionId: SESSION_ID, configOptions: configOptions() });
      return;
    case "session/load":
      respond(id, { configOptions: configOptions() });
      return;
    case "session/set_config_option": {
      const configId = params?.configId;
      const value = params?.value;
      if (staleChunkOnConfig && promptCount > 0) {
        sessionUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "stale tail" },
        });
      }
      if (configId === "model" && typeof value === "string") currentModel = value;
      if (configId === "reasoning_effort" && typeof value === "string") currentReasoning = value;
      if (configId === "mode" && typeof value === "string") currentMode = value;
      if (configId === "allow_all") allowAll = value === true || value === "true";
      respond(id, { configOptions: configOptions() });
      configWriteSeen = true;
      if (releasePromptOnConfig && heldPromptId !== undefined) {
        const releasedId = heldPromptId;
        heldPromptId = undefined;
        respond(releasedId, { stopReason: "end_turn" });
      }
      return;
    }
    case "session/prompt":
      void handlePrompt(id);
      return;
    case "session/cancel":
      if (heldPromptId !== undefined) {
        const cancelledId = heldPromptId;
        heldPromptId = undefined;
        respond(cancelledId, { stopReason: "cancelled" });
      }
      if (lateChunkAfterCancel) {
        // The real CLI keeps streaming well past the point T3 settled the turn.
        sessionUpdate({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "late tail" },
        });
      }
      return;
    default:
      if (id !== undefined) {
        write({ jsonrpc: "2.0", id, error: { code: -32601, message: "Unknown method " + method } });
      }
  }
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newlineIndex = buffer.indexOf("\n");
  while (newlineIndex >= 0) {
    const line = buffer.slice(0, newlineIndex).trim();
    buffer = buffer.slice(newlineIndex + 1);
    if (line.length > 0) handleMessage(JSON.parse(line));
    newlineIndex = buffer.indexOf("\n");
  }
});
process.stdin.on("end", () => process.exit(0));
process.once("SIGTERM", () => process.exit(0));
`;

async function writeMockAgent() {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "copilot-acp-mock-"));
  const agentPath = NodePath.join(dir, "copilot-acp-mock.mjs");
  await NodeFSP.writeFile(agentPath, MOCK_AGENT_SOURCE, "utf8");
  return { dir, agentPath, requestLogPath: NodePath.join(dir, "requests.ndjson") };
}

async function readRequestLog(requestLogPath: string) {
  const raw = await NodeFSP.readFile(requestLogPath, "utf8").catch(() => "");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

const copilotAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-github-copilot-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (
  agentPath: string,
  environment: Record<string, string>,
  telemetryRecords?: Array<GitHubCopilotTelemetryRecord>,
) =>
  makeGitHubCopilotAdapter(
    decodeGitHubCopilotSettings({
      binaryPath: process.execPath,
      launchArgs: `"${agentPath}"`,
    }),
    {
      environment: { ...process.env, ...environment },
      instanceId: INSTANCE_ID,
      ...(telemetryRecords
        ? {
            telemetrySink: (record: GitHubCopilotTelemetryRecord) =>
              Effect.sync(() => void telemetryRecords.push(record)),
            getCliVersionBucket: () => "1.0",
          }
        : {}),
    },
  ).pipe(Effect.orDie);

interface EventRecorderWaiter {
  readonly predicate: (event: ProviderRuntimeEvent) => boolean;
  readonly deferred: Deferred.Deferred<ProviderRuntimeEvent>;
}

/** Records every runtime event and lets a test await the first match. */
const makeEventRecorder = (streamEvents: Stream.Stream<ProviderRuntimeEvent>) =>
  Effect.gen(function* () {
    const events: Array<ProviderRuntimeEvent> = [];
    const waiters: Array<EventRecorderWaiter> = [];
    const fiber = yield* Stream.runForEach(streamEvents, (event) =>
      Effect.gen(function* () {
        events.push(event);
        const matched = waiters.filter((waiter) => waiter.predicate(event));
        for (const waiter of matched) {
          waiters.splice(waiters.indexOf(waiter), 1);
          yield* Deferred.succeed(waiter.deferred, event);
        }
      }),
    ).pipe(Effect.forkChild);

    const waitFor = (predicate: (event: ProviderRuntimeEvent) => boolean) =>
      Effect.gen(function* () {
        const existing = events.find(predicate);
        if (existing) return existing;
        const deferred = yield* Deferred.make<ProviderRuntimeEvent>();
        waiters.push({ predicate, deferred });
        return yield* Deferred.await(deferred);
      });

    return { events, waitFor, fiber } as const;
  });

const contentDeltas = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.filter(
    (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
      event.type === "content.delta",
  );

it("requires a settlement to match the live GitHub Copilot turn", () => {
  const staleTurnId = TurnId.make("stale-turn");
  const replacementTurnId = TurnId.make("replacement-turn");

  assert.isFalse(
    githubCopilotPromptSettlementBelongsToContext({
      liveAcpSessionId: "session-1",
      expectedAcpSessionId: "session-1",
      liveActiveTurnId: replacementTurnId,
      liveSessionActiveTurnId: replacementTurnId,
      turnId: staleTurnId,
    }),
  );
  assert.isFalse(
    githubCopilotPromptSettlementBelongsToContext({
      liveAcpSessionId: "replacement-session",
      expectedAcpSessionId: "stale-session",
      liveActiveTurnId: staleTurnId,
      liveSessionActiveTurnId: staleTurnId,
      turnId: staleTurnId,
    }),
  );
  assert.isTrue(
    githubCopilotPromptSettlementBelongsToContext({
      liveAcpSessionId: "session-1",
      expectedAcpSessionId: "session-1",
      liveActiveTurnId: staleTurnId,
      liveSessionActiveTurnId: staleTurnId,
      turnId: staleTurnId,
    }),
  );
});

it.layer(copilotAdapterTestLayer)("GitHubCopilotAdapter", (it) => {
  it.effect(
    "maps a Copilot turn to assistant, reasoning and token-usage events",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-happy-path");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const telemetryRecords: Array<GitHubCopilotTelemetryRecord> = [];
        const adapter = yield* makeTestAdapter(
          mock.agentPath,
          {
            COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
          },
          telemetryRecords,
        );
        const recorder = yield* makeEventRecorder(adapter.streamEvents);

        const session = yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection: {
            instanceId: INSTANCE_ID,
            model: "claude-sonnet-4.5",
            options: [{ id: "reasoning", value: "high" }],
          },
        });

        assert.equal(session.provider, "githubCopilot");
        assert.equal(session.model, "claude-sonnet-4.5");
        assert.deepStrictEqual(session.resumeCursor, {
          schemaVersion: 1,
          sessionId: "copilot-mock-session-1",
        });

        const turn = yield* adapter.sendTurn({ threadId, input: "hello copilot" });
        yield* recorder.waitFor(
          (event) => event.type === "turn.completed" && event.turnId === turn.turnId,
        );

        const types = recorder.events.map((event) => event.type);
        assert.includeMembers(types, [
          "session.started",
          "session.state.changed",
          "thread.started",
          "turn.started",
          "content.delta",
          "thread.token-usage.updated",
          "turn.completed",
        ] as const);

        const deltas = contentDeltas(recorder.events);
        assert.deepStrictEqual(
          deltas.map((event) => [event.payload.streamKind, event.payload.delta]),
          [
            ["reasoning_text", "weighing the options"],
            ["assistant_text", "answer 1"],
          ],
        );
        assert.isTrue(deltas.every((event) => event.turnId === turn.turnId));

        const usage = recorder.events.find(
          (event): event is Extract<ProviderRuntimeEvent, { type: "thread.token-usage.updated" }> =>
            event.type === "thread.token-usage.updated",
        );
        assert.equal(usage?.payload.usage.usedTokens, 1200);
        assert.equal(usage?.payload.usage.maxTokens, 128000);

        const completed = recorder.events.find(
          (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
            event.type === "turn.completed",
        );
        assert.equal(completed?.payload.state, "completed");

        // Reasoning effort and full-access allow_all are pushed as Copilot
        // config options; the mode is Agent, never Autopilot.
        const requests = yield* Effect.promise(() => readRequestLog(mock.requestLogPath));
        const configWrites = requests.flatMap((entry) => {
          const params = entry.params as Record<string, unknown> | undefined;
          return entry.method === "session/set_config_option" && params
            ? [[params.configId, params.value] as const]
            : [];
        });
        assert.includeDeepMembers(configWrites, [
          ["model", "claude-sonnet-4.5"],
          ["reasoning_effort", "high"],
          ["allow_all", true],
        ]);
        assert.isFalse(configWrites.some(([, value]) => value === "autopilot"));

        yield* Fiber.interrupt(recorder.fiber);
        yield* adapter.stopSession(threadId);

        assert.includeMembers(
          telemetryRecords.map((record) => record.operation),
          [
            "spawn",
            "session_new",
            "config",
            "first_response",
            "turn",
            "usage",
            "post_settlement_tail",
            "shutdown",
          ],
        );
        assert.deepInclude(
          telemetryRecords.find((record) => record.operation === "turn"),
          {
            operation: "turn",
            outcome: "success",
            stage: "prompt",
            stopReason: "end_turn",
            interactionMode: "default",
            runtimeMode: "full-access",
            cliVersionBucket: "1.0",
          },
        );
        const serializedTelemetry = yield* encodeUnknownJson(telemetryRecords);
        assert.notInclude(serializedTelemetry, "hello copilot");
        assert.notInclude(serializedTelemetry, process.cwd());
        assert.notInclude(serializedTelemetry, "copilot-happy-path");
        assert.notInclude(serializedTelemetry, "copilot-mock-session-1");
        assert.notInclude(serializedTelemetry, "claude-sonnet-4.5");
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "applies the requested model once Copilot publishes its model list mid-turn",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-late-model-list");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {
          COPILOT_MOCK_LATE_MODELS: "1",
          COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
        });

        const session = yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          modelSelection: { instanceId: INSTANCE_ID, model: "claude-sonnet-4.5" },
        });
        // No model option exists yet, so the session still reports the choice.
        assert.equal(session.model, "claude-sonnet-4.5");

        const requestsBeforeModels = yield* Effect.promise(() =>
          readRequestLog(mock.requestLogPath),
        );
        assert.isFalse(
          requestsBeforeModels.some(
            (entry) =>
              entry.method === "session/set_config_option" &&
              (entry.params as Record<string, unknown> | undefined)?.configId === "model",
          ),
        );

        // The first prompt is what makes Copilot publish `config_option_update`.
        yield* adapter.sendTurn({ threadId, input: "first" });
        yield* adapter.sendTurn({ threadId, input: "second" });

        const requests = yield* Effect.promise(() => readRequestLog(mock.requestLogPath));
        const modelWrites = requests.filter(
          (entry) =>
            entry.method === "session/set_config_option" &&
            (entry.params as Record<string, unknown> | undefined)?.configId === "model",
        );
        assert.equal(modelWrites.length, 1);
        assert.equal(
          (modelWrites[0]?.params as Record<string, unknown> | undefined)?.value,
          "claude-sonnet-4.5",
        );

        const sessions = yield* adapter.listSessions();
        assert.equal(
          sessions.find((entry) => entry.threadId === threadId)?.model,
          "claude-sonnet-4.5",
        );

        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "ignores unsupported reasoning selections instead of failing the session",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-unsupported-reasoning");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {
          COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
        });

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          modelSelection: {
            instanceId: INSTANCE_ID,
            model: "gpt-5.1-codex",
            options: [{ id: "reasoning", value: "xhigh" }],
          },
        });
        yield* adapter.sendTurn({ threadId, input: "hello" });

        const requests = yield* Effect.promise(() => readRequestLog(mock.requestLogPath));
        assert.isFalse(
          requests.some(
            (entry) =>
              entry.method === "session/set_config_option" &&
              (entry.params as Record<string, unknown> | undefined)?.configId ===
                "reasoning_effort",
          ),
        );

        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "drops stale post-settlement notifications during the next turn's preparation",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-stale-tail-before-next-prompt");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const telemetryRecords: Array<GitHubCopilotTelemetryRecord> = [];
        const adapter = yield* makeTestAdapter(
          mock.agentPath,
          {
            COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
            COPILOT_MOCK_STALE_CHUNK_ON_CONFIG: "1",
          },
          telemetryRecords,
        );
        const recorder = yield* makeEventRecorder(adapter.streamEvents);

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          modelSelection: { instanceId: INSTANCE_ID, model: "gpt-5.1-codex" },
        });
        const first = yield* adapter.sendTurn({ threadId, input: "first" });
        yield* recorder.waitFor(
          (event) => event.type === "turn.completed" && event.turnId === first.turnId,
        );

        const second = yield* adapter.sendTurn({
          threadId,
          input: "second",
          modelSelection: {
            instanceId: INSTANCE_ID,
            model: "gpt-5.1-codex",
            options: [{ id: "reasoning", value: "high" }],
          },
        });
        yield* recorder.waitFor(
          (event) => event.type === "turn.completed" && event.turnId === second.turnId,
        );

        const secondTurnText = contentDeltas(recorder.events)
          .filter(
            (event) =>
              event.turnId === second.turnId && event.payload.streamKind === "assistant_text",
          )
          .map((event) => event.payload.delta);
        assert.deepStrictEqual(secondTurnText, ["answer 2"]);
        assert.deepInclude(
          telemetryRecords.find(
            (record) =>
              record.operation === "post_settlement_tail" && record.crossedTurnWindow === true,
          ),
          {
            operation: "post_settlement_tail",
            crossedTurnWindow: true,
          },
        );

        yield* Fiber.interrupt(recorder.fiber);
        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "folds a mid-turn sendTurn into the active turn and completes it once",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-steering");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {
          COPILOT_MOCK_RELEASE_PROMPT_ON_CONFIG: "1",
          COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
        });
        const recorder = yield* makeEventRecorder(adapter.streamEvents);

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          modelSelection: { instanceId: INSTANCE_ID, model: "gpt-5.1-codex" },
        });

        const firstTurnFiber = yield* adapter
          .sendTurn({ threadId, input: "start the work" })
          .pipe(Effect.forkChild);
        const started = yield* recorder.waitFor((event) => event.type === "turn.started");

        // Steering while the first prompt is still in flight: the mock only
        // answers that prompt once the steering turn's model switch lands, so
        // both prompts are provably in flight together.
        const secondTurn = yield* adapter.sendTurn({
          threadId,
          input: "actually do this instead",
          modelSelection: { instanceId: INSTANCE_ID, model: "claude-sonnet-4.5" },
        });
        const firstTurn = yield* Fiber.join(firstTurnFiber);

        assert.equal(firstTurn.turnId, started.turnId);
        assert.equal(secondTurn.turnId, started.turnId);
        assert.lengthOf(
          recorder.events.filter((event) => event.type === "turn.started"),
          1,
        );
        assert.lengthOf(
          recorder.events.filter((event) => event.type === "turn.completed"),
          1,
        );

        const readySessions = yield* adapter.listSessions();
        const readySession = readySessions.find((entry) => entry.threadId === threadId);
        assert.equal(readySession?.status, "ready");
        assert.isUndefined(readySession?.activeTurnId);

        yield* Fiber.interrupt(recorder.fiber);
        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "cancels a silent prompt and never re-attaches later notifications to it",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-cancel-late-notifications");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {
          COPILOT_MOCK_HANG_PROMPT: "1",
          COPILOT_MOCK_LATE_CHUNK_AFTER_CANCEL: "1",
        });
        const recorder = yield* makeEventRecorder(adapter.streamEvents);

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const turnFiber = yield* adapter
          .sendTurn({ threadId, input: "run forever" })
          .pipe(Effect.forkChild);
        const started = yield* recorder.waitFor((event) => event.type === "turn.started");

        yield* adapter.interruptTurn(threadId);
        yield* Fiber.join(turnFiber);

        const completed = recorder.events.find(
          (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
            event.type === "turn.completed",
        );
        assert.equal(completed?.payload.state, "cancelled");
        assert.equal(completed?.turnId, started.turnId);

        const completedIndex = recorder.events.findIndex((event) => event === completed);
        assert.isFalse(
          recorder.events
            .slice(completedIndex + 1)
            .some((event) => event.turnId === started.turnId),
        );
        assert.isFalse(
          contentDeltas(recorder.events).some((event) => event.payload.delta === "late tail"),
        );

        const readySessions = yield* adapter.listSessions();
        assert.equal(readySessions.find((entry) => entry.threadId === threadId)?.status, "ready");

        yield* Fiber.interrupt(recorder.fiber);
        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "answers an ACP permission request with the provider-supplied option id",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-permission");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {
          COPILOT_MOCK_REQUEST_PERMISSION: "1",
          COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
        });
        const recorder = yield* makeEventRecorder(adapter.streamEvents);

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });

        const turnFiber = yield* adapter
          .sendTurn({ threadId, input: "delete the build dir" })
          .pipe(Effect.forkChild);
        const opened = yield* recorder.waitFor((event) => event.type === "request.opened");
        assert.isDefined(opened.requestId);

        yield* adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(String(opened.requestId)),
          "accept",
        );
        yield* Fiber.join(turnFiber);

        const resolved = recorder.events.find(
          (event): event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> =>
            event.type === "request.resolved",
        );
        assert.equal(resolved?.payload.decision, "accept");
        assert.equal(resolved?.payload.requestType, "exec_command_approval");

        const requests = yield* Effect.promise(() => readRequestLog(mock.requestLogPath));
        const outcome = requests.find((entry) => entry.kind === "permission-outcome");
        assert.deepStrictEqual(outcome?.outcome, {
          outcome: { outcome: "selected", optionId: "allow-once" },
        });

        yield* Fiber.interrupt(recorder.fiber);
        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "maps persistent approval to the provider allow-always option",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-permission-always");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {
          COPILOT_MOCK_REQUEST_PERMISSION: "1",
          COPILOT_MOCK_REQUEST_LOG: mock.requestLogPath,
        });
        const recorder = yield* makeEventRecorder(adapter.streamEvents);

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });

        const turnFiber = yield* adapter
          .sendTurn({ threadId, input: "delete the build dir" })
          .pipe(Effect.forkChild);
        const opened = yield* recorder.waitFor((event) => event.type === "request.opened");
        assert.isDefined(opened.requestId);

        yield* adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(String(opened.requestId)),
          "acceptAlways",
        );
        yield* Fiber.join(turnFiber);

        const resolved = recorder.events.find(
          (event): event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> =>
            event.type === "request.resolved",
        );
        assert.equal(resolved?.payload.decision, "acceptAlways");

        const requests = yield* Effect.promise(() => readRequestLog(mock.requestLogPath));
        const outcome = requests.find((entry) => entry.kind === "permission-outcome");
        assert.deepStrictEqual(outcome?.outcome, {
          outcome: { outcome: "selected", optionId: "allow-always" },
        });

        yield* Fiber.interrupt(recorder.fiber);
        yield* adapter.stopSession(threadId);
      }),
    TEST_TIMEOUT_MS,
  );

  it.effect(
    "rejects turns without text or attachments and unknown approval responses",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("copilot-validation");
        const mock = yield* Effect.promise(() => writeMockAgent());
        const adapter = yield* makeTestAdapter(mock.agentPath, {});

        yield* adapter.startSession({
          threadId,
          provider: PROVIDER,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });

        const emptyTurnError = yield* Effect.flip(adapter.sendTurn({ threadId, attachments: [] }));
        assert.equal(emptyTurnError._tag, "ProviderAdapterValidationError");

        const unknownApprovalError = yield* Effect.flip(
          adapter.respondToRequest(threadId, ApprovalRequestId.make("missing"), "accept"),
        );
        assert.equal(unknownApprovalError._tag, "ProviderAdapterRequestError");

        const providerMismatchError = yield* Effect.flip(
          adapter.startSession({
            threadId,
            provider: ProviderDriverKind.make("grok"),
            cwd: process.cwd(),
            runtimeMode: "approval-required",
          }),
        );
        assert.equal(providerMismatchError._tag, "ProviderAdapterValidationError");

        // A turn is still accepted after the rejected ones.
        const turn = yield* adapter.sendTurn({ threadId, input: "still works" });
        assert.isTrue(yield* adapter.hasSession(threadId));
        const thread = yield* adapter.readThread(threadId);
        assert.deepStrictEqual(
          thread.turns.map((entry) => entry.id),
          [turn.turnId],
        );

        yield* adapter.stopSession(threadId);
        assert.isFalse(yield* adapter.hasSession(threadId));
      }),
    TEST_TIMEOUT_MS,
  );
});
