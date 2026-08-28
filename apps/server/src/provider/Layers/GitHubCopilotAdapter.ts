/**
 * GitHubCopilotAdapter — ACP adapter for the GitHub Copilot CLI (`copilot --acp --stdio`).
 *
 * Copilot speaks the same ACP dialect as Grok, so the turn/steering state
 * machine here follows {@link ./GrokAdapter.ts}: prompts in flight are counted
 * so a mid-turn `sendTurn` steers the active turn, and interrupted turn ids are
 * remembered so a late prompt RPC can neither resurrect a cancelled turn nor
 * emit a second terminal event.
 *
 * Copilot-specific behavior, measured against the real CLI:
 * - `session/new` is authoritative for models *and* modes, and the model list
 *   can arrive later through `config_option_update` — sometimes only after the
 *   first prompt. The requested model/selections are therefore remembered on
 *   the session and re-applied on every turn.
 * - Session startup is slow (6s warm, ~1min cold), so nothing here wraps
 *   startup in a short timeout.
 * - Permission requests are not observed today even for writes, but the ACP
 *   permission protocol is implemented so approvals work the moment Copilot
 *   starts asking.
 *
 * @module GitHubCopilotAdapter
 */
import {
  ApprovalRequestId,
  EventId,
  type GitHubCopilotSettings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInteractionMode,
  type ProviderOptionSelection,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type RuntimeMode,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as Cause from "effect/Cause";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  normalizeGitHubCopilotCliVersionBucket,
  normalizeGitHubCopilotToolKind,
  recordGitHubCopilotTelemetry,
  telemetryFailureRecord,
  type GitHubCopilotTelemetryOperation,
  type GitHubCopilotTelemetryRecord,
  type GitHubCopilotTelemetrySink,
  type GitHubCopilotTelemetryStage,
} from "../GitHubCopilotTelemetry.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpReasoningDeltaEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpThreadTokenUsageUpdatedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import { makeAcpNativeLoggerFactory } from "../acp/AcpNativeLogging.ts";
import {
  applyGitHubCopilotSessionConfiguration,
  currentGitHubCopilotModelId,
  makeGitHubCopilotAcpRuntime,
} from "../acp/GitHubCopilotAcpSupport.ts";
import { makeGitHubCopilotAgencyTaskEvent } from "../GitHubCopilotAgencyTasks.ts";
import { type GitHubCopilotAdapterShape } from "../Services/GitHubCopilotAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const encodeUnknownJsonStringExit = Schema.encodeUnknownExit(Schema.fromJsonString(Schema.Unknown));

const PROVIDER = ProviderDriverKind.make("githubCopilot");
const GITHUB_COPILOT_RESUME_VERSION = 1 as const;
/**
 * Copilot replays a resumed session before answering `session/load`, and a cold
 * start of the CLI has been measured near a minute. The shared runtime default
 * (90s) is too tight for that, so resume gets a deliberately generous budget.
 */
const SESSION_LOAD_TIMEOUT = Duration.minutes(5);

function encodeJsonStringForDiagnostics(input: unknown): string | undefined {
  const result = encodeUnknownJsonStringExit(input);
  return Exit.isSuccess(result) ? result.value : undefined;
}

export interface GitHubCopilotAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  /**
   * Selections are honored when `modelSelection.instanceId` matches this value.
   * Defaults to the built-in instance id (`githubCopilot`).
   */
  readonly instanceId?: ProviderInstanceId;
  readonly telemetrySink?: GitHubCopilotTelemetrySink;
  readonly getCliVersionBucket?: () => string;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface GitHubCopilotSessionContext {
  readonly threadId: ThreadId;
  readonly acpSessionId: string;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  lastPlanFingerprint: string | undefined;
  activeTurnId: TurnId | undefined;
  /**
   * False while a new turn is being prepared. Copilot can continue streaming
   * after a prompt settles, so preparation must not re-parent that stale tail
   * to the next turn.
   */
  acceptingNotifications: boolean;
  /** Turns already interrupted; late prompt RPCs must not resurrect them. */
  interruptedTurnIds: Set<TurnId>;
  /** Number of sendTurn prompts currently in flight or being prepared.
   * >0 means a turn is actively running, so a new sendTurn is a steer that
   * continues it, and only the last remaining prompt settles the turn. */
  promptsInFlight: number;
  /**
   * Last model/option selection the user asked for. Copilot may publish its
   * model list only after the first prompt, so the request is replayed on every
   * turn until the `model` config option actually exists.
   */
  requestedModel: string | undefined;
  requestedSelections: ReadonlyArray<ProviderOptionSelection> | undefined;
  currentModelId: string | undefined;
  turnTelemetry: GitHubCopilotTurnTelemetry | undefined;
  pendingTailTelemetry: GitHubCopilotTailTelemetry | undefined;
  readonly permissionRequestedToolCallIds: Set<string>;
  readonly terminalToolCallIds: Set<string>;
  stopped: boolean;
}

interface GitHubCopilotTurnTelemetry {
  readonly turnId: TurnId;
  readonly startedAtMs: number;
  readonly interactionMode: ProviderInteractionMode;
  readonly runtimeMode: RuntimeMode;
  firstResponseRecorded: boolean;
  promptSubmittedAtMs: number | undefined;
  failureRecorded: boolean;
  latestUsage:
    | {
        readonly usedTokens: number;
        readonly contextSize: number | undefined;
      }
    | undefined;
}

interface GitHubCopilotTailTelemetry {
  readonly settledAtMs: number;
  lastNotificationAtMs: number | undefined;
  crossedTurnWindow: boolean;
  nextTurnWindowStarted: boolean;
  readonly interactionMode: ProviderInteractionMode;
  readonly runtimeMode: RuntimeMode;
}

function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingApprovals.values()),
    (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
    { discard: true },
  );
}

function appendPromptResultToTurn(
  ctx: GitHubCopilotSessionContext,
  turnId: TurnId,
  promptParts: ReadonlyArray<EffectAcpSchema.ContentBlock>,
  result: EffectAcpSchema.PromptResponse,
): void {
  const existingTurnRecord = ctx.turns.find((turn) => turn.id === turnId);
  ctx.turns = existingTurnRecord
    ? ctx.turns.map((turn) =>
        turn.id === turnId
          ? { ...turn, items: [...turn.items, { prompt: promptParts, result }] }
          : turn,
      )
    : [...ctx.turns, { id: turnId, items: [{ prompt: promptParts, result }] }];
}

function parseGitHubCopilotResume(raw: unknown): { sessionId: string } | undefined {
  if (!Predicate.isObject(raw)) return undefined;
  if (raw.schemaVersion !== GITHUB_COPILOT_RESUME_VERSION) return undefined;
  if (!Predicate.isString(raw.sessionId) || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function selectPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession" || decision === "acceptAlways"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  const option = request.options.find((entry) => entry.kind === kind);
  return option?.optionId.trim() || undefined;
}

function selectAutoApprovedPermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    selectPermissionOptionId(request, "acceptForSession") ??
    selectPermissionOptionId(request, "accept")
  );
}

/**
 * A prompt settlement only owns the live session when it still targets the same
 * ACP session and the turn it opened. Anything else is a late RPC for a
 * superseded turn and must not touch session state.
 */
export function githubCopilotPromptSettlementBelongsToContext(input: {
  readonly liveAcpSessionId: string;
  readonly expectedAcpSessionId: string;
  readonly liveActiveTurnId: TurnId | undefined;
  readonly liveSessionActiveTurnId: TurnId | undefined;
  readonly turnId: TurnId;
}): boolean {
  return (
    input.liveAcpSessionId === input.expectedAcpSessionId &&
    (input.liveActiveTurnId === input.turnId || input.liveSessionActiveTurnId === input.turnId)
  );
}

export function makeGitHubCopilotAdapter(
  copilotSettings: GitHubCopilotSettings,
  options?: GitHubCopilotAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("githubCopilot");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
    const makeAcpNativeLoggers = yield* makeAcpNativeLoggerFactory();
    const telemetrySink = options?.telemetrySink ?? recordGitHubCopilotTelemetry;
    const getCliVersionBucket = () =>
      normalizeGitHubCopilotCliVersionBucket(options?.getCliVersionBucket?.());

    const sessions = new Map<ThreadId, GitHubCopilotSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate GitHub Copilot runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });
    const emitTelemetry = (
      record: Omit<GitHubCopilotTelemetryRecord, "cliVersionBucket">,
    ): Effect.Effect<void> =>
      telemetrySink({
        ...record,
        cliVersionBucket: getCliVersionBucket(),
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to record GitHub Copilot telemetry.", {
            errorTag: causeErrorTag(cause),
          }),
        ),
      );
    const observeOperation = <A, E, R>(input: {
      readonly effect: Effect.Effect<A, E, R>;
      readonly operation: GitHubCopilotTelemetryOperation;
      readonly stage: GitHubCopilotTelemetryStage;
      readonly interactionMode?: ProviderInteractionMode | undefined;
      readonly runtimeMode?: RuntimeMode | undefined;
    }): Effect.Effect<A, E, R> =>
      Effect.gen(function* () {
        const startedAtMs = yield* Clock.currentTimeMillis;
        const exit = yield* Effect.exit(input.effect);
        const endedAtMs = yield* Clock.currentTimeMillis;
        const durationMs = Math.max(0, endedAtMs - startedAtMs);
        if (Exit.isSuccess(exit)) {
          yield* emitTelemetry({
            operation: input.operation,
            outcome: "success",
            stage: input.stage,
            durationMs,
            interactionMode: input.interactionMode,
            runtimeMode: input.runtimeMode,
          });
          return exit.value;
        }
        const failure = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
        const { cliVersionBucket: _cliVersionBucket, ...failureRecord } = telemetryFailureRecord({
          operation: input.operation,
          fallbackStage: input.stage,
          cause: failure ?? exit.cause,
          durationMs,
          cliVersionBucket: getCliVersionBucket(),
          interactionMode: input.interactionMode,
          runtimeMode: input.runtimeMode,
        });
        yield* emitTelemetry(failureRecord);
        return yield* Effect.failCause(exit.cause);
      });
    const mapAcpCallbackFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new EffectAcpErrors.AcpTransportError({
              detail: "Failed to process GitHub Copilot ACP callback.",
              cause,
            }),
        ),
      );

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    /**
     * Applies the four Copilot ACP controls T3 owns (model, reasoning effort,
     * mode, allow_all) and reports the model that ended up selected.
     */
    const applySessionConfiguration = (input: {
      readonly threadId: ThreadId;
      readonly runtime: AcpSessionRuntime.AcpSessionRuntime["Service"];
      readonly model: string | undefined;
      readonly selections: ReadonlyArray<ProviderOptionSelection> | undefined;
      readonly interactionMode: ProviderInteractionMode | undefined;
      readonly runtimeMode: RuntimeMode;
    }) =>
      applyGitHubCopilotSessionConfiguration({
        runtime: input.runtime,
        model: input.model,
        selections: input.selections,
        interactionMode: input.interactionMode,
        runtimeMode: input.runtimeMode,
        mapError: ({ cause, configId }) =>
          mapAcpToAdapterError(
            PROVIDER,
            input.threadId,
            `session/set_config_option (${configId})`,
            cause,
          ),
      });

    const completeTurnTelemetry = (
      ctx: GitHubCopilotSessionContext,
      turnId: TurnId,
      outcome: "success" | "failure" | "cancelled",
      stopReason?: EffectAcpSchema.StopReason,
    ) =>
      Effect.gen(function* () {
        const turn = ctx.turnTelemetry;
        if (!turn || turn.turnId !== turnId) return;
        const completedAtMs = yield* Clock.currentTimeMillis;
        if (!(outcome === "failure" && turn.failureRecorded)) {
          yield* emitTelemetry({
            operation: "turn",
            outcome,
            stage: "prompt",
            durationMs: Math.max(0, completedAtMs - turn.startedAtMs),
            stopReason,
            interactionMode: turn.interactionMode,
            runtimeMode: turn.runtimeMode,
          });
        }
        if (turn.latestUsage) {
          yield* emitTelemetry({
            operation: "usage",
            outcome: "success",
            stage: "notification",
            interactionMode: turn.interactionMode,
            runtimeMode: turn.runtimeMode,
            usedTokens: turn.latestUsage.usedTokens,
            contextSize: turn.latestUsage.contextSize,
          });
        }
        ctx.turnTelemetry = undefined;
      });

    const completeTailTelemetry = (ctx: GitHubCopilotSessionContext) =>
      Effect.gen(function* () {
        const tail = ctx.pendingTailTelemetry;
        if (!tail) return;
        const finalNotificationAtMs = tail.lastNotificationAtMs ?? tail.settledAtMs;
        yield* emitTelemetry({
          operation: "post_settlement_tail",
          outcome: "success",
          stage: "notification",
          durationMs: Math.max(0, finalNotificationAtMs - tail.settledAtMs),
          interactionMode: tail.interactionMode,
          runtimeMode: tail.runtimeMode,
          crossedTurnWindow: tail.crossedTurnWindow,
        });
        ctx.pendingTailTelemetry = undefined;
      });

    const settlePromptInFlight = (
      threadId: ThreadId,
      turnId: TurnId,
      expectedAcpSessionId: string,
      settleOptions?: {
        readonly errorMessage?: string;
        readonly completedStopReason?: EffectAcpSchema.StopReason;
        readonly emitTurnCompletion?: boolean;
        /** Interrupt/cancel: drop every outstanding prompt slot and settle once. */
        readonly settleAllPrompts?: boolean;
      },
    ) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(threadId);
        if (!liveCtx) {
          return;
        }
        const settlementBelongsToLiveContext = githubCopilotPromptSettlementBelongsToContext({
          liveAcpSessionId: liveCtx.acpSessionId,
          expectedAcpSessionId,
          liveActiveTurnId: liveCtx.activeTurnId,
          liveSessionActiveTurnId: liveCtx.session.activeTurnId,
          turnId,
        });
        if (!settlementBelongsToLiveContext) {
          // interruptTurn already consumed every prompt slot for this turn. A
          // late prompt result must neither emit a second terminal event nor
          // consume a slot belonging to a newer turn on the same ACP session.
          if (
            liveCtx.acpSessionId !== expectedAcpSessionId ||
            liveCtx.interruptedTurnIds.has(turnId)
          ) {
            return;
          }
          if (settleOptions?.emitTurnCompletion !== false) {
            if (settleOptions?.errorMessage !== undefined) {
              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId,
                turnId,
                payload: {
                  state: "failed",
                  errorMessage: settleOptions.errorMessage,
                },
              });
            } else if (settleOptions?.completedStopReason !== undefined) {
              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId,
                turnId,
                payload: {
                  state:
                    settleOptions.completedStopReason === "cancelled" ? "cancelled" : "completed",
                  stopReason: settleOptions.completedStopReason,
                },
              });
            }
          }
          return;
        }
        let settleTurnId = turnId;
        if (settleOptions?.settleAllPrompts) {
          liveCtx.promptsInFlight = 0;
          if (liveCtx.activeTurnId !== turnId && liveCtx.session.activeTurnId !== turnId) {
            const fallbackTurnId = liveCtx.activeTurnId ?? liveCtx.session.activeTurnId;
            if (!fallbackTurnId) {
              if (liveCtx.session.status === "running" || liveCtx.session.status === "connecting") {
                const updatedAt = yield* nowIso;
                const { activeTurnId: _activeTurnId, ...readySession } = liveCtx.session;
                liveCtx.activeTurnId = undefined;
                liveCtx.acceptingNotifications = false;
                liveCtx.session = {
                  ...readySession,
                  status: "ready",
                  updatedAt,
                };
              }
              return;
            }
            settleTurnId = fallbackTurnId;
          }
        } else {
          const remainingPrompts = Math.max(0, liveCtx.promptsInFlight - 1);
          if (
            remainingPrompts > 0 ||
            liveCtx.activeTurnId !== settleTurnId ||
            liveCtx.session.activeTurnId !== settleTurnId
          ) {
            liveCtx.promptsInFlight = remainingPrompts;
            return;
          }
          liveCtx.promptsInFlight = remainingPrompts;
        }
        const updatedAt = yield* nowIso;
        const canEmitTurnCompletion =
          liveCtx.session.status === "running" || liveCtx.session.status === "connecting";
        const shouldEmitFailedTurn =
          settleOptions?.errorMessage !== undefined && canEmitTurnCompletion;
        const shouldEmitCompletedTurn =
          settleOptions?.completedStopReason !== undefined && canEmitTurnCompletion;
        const { activeTurnId: _activeTurnId, ...readySession } = liveCtx.session;
        liveCtx.activeTurnId = undefined;
        liveCtx.acceptingNotifications = false;
        liveCtx.session = {
          ...readySession,
          status: "ready",
          updatedAt,
        };
        if (settleOptions?.emitTurnCompletion === false) {
          if (settleOptions.errorMessage !== undefined) {
            yield* completeTurnTelemetry(liveCtx, settleTurnId, "failure");
          }
          return;
        }
        if (shouldEmitFailedTurn) {
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId: settleTurnId,
            payload: {
              state: "failed",
              errorMessage: settleOptions.errorMessage,
            },
          });
          yield* completeTurnTelemetry(liveCtx, settleTurnId, "failure");
        } else if (shouldEmitCompletedTurn) {
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId: settleTurnId,
            payload: {
              state: settleOptions.completedStopReason === "cancelled" ? "cancelled" : "completed",
              stopReason: settleOptions.completedStopReason,
            },
          });
          yield* completeTurnTelemetry(
            liveCtx,
            settleTurnId,
            settleOptions.completedStopReason === "cancelled" ? "cancelled" : "success",
            settleOptions.completedStopReason,
          );
        }
      });

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to write native GitHub Copilot notification log.", {
            cause,
            threadId,
            method,
          }),
        ),
      );

    const emitPlanUpdate = (
      ctx: GitHubCopilotSessionContext,
      turnId: TurnId | undefined,
      stamp: { readonly eventId: EventId; readonly createdAt: string },
      payload: {
        readonly explanation?: string | null;
        readonly plan: ReadonlyArray<{
          readonly step: string;
          readonly status: "pending" | "inProgress" | "completed";
        }>;
      },
      rawPayload: unknown,
      method: string,
    ) =>
      Effect.gen(function* () {
        const fingerprint = `${turnId ?? "no-turn"}:${encodeJsonStringForDiagnostics(payload) ?? "[unserializable payload]"}`;
        if (ctx.lastPlanFingerprint === fingerprint) {
          return;
        }
        ctx.lastPlanFingerprint = fingerprint;
        yield* offerRuntimeEvent(
          makeAcpPlanUpdatedEvent({
            stamp,
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId,
            payload,
            source: "acp.jsonrpc",
            method,
            rawPayload,
          }),
        );
      });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<GitHubCopilotSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: GitHubCopilotSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        const startedAtMs = yield* Clock.currentTimeMillis;
        ctx.stopped = true;
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* Effect.ignore(ctx.acp.drainEvents);
        yield* completeTailTelemetry(ctx);
        if (ctx.turnTelemetry) {
          yield* completeTurnTelemetry(ctx, ctx.turnTelemetry.turnId, "cancelled", "cancelled");
        }
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
        const completedAtMs = yield* Clock.currentTimeMillis;
        yield* emitTelemetry({
          operation: "shutdown",
          outcome: "success",
          stage: "shutdown",
          durationMs: Math.max(0, completedAtMs - startedAtMs),
        });
      });

    const startSession: GitHubCopilotAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = path.resolve(input.cwd.trim());
          const copilotModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          // Copilot advertises `loadSession: true`, so a persisted cursor lets a
          // thread survive a server restart.
          const resumeSessionId = parseGitHubCopilotResume(input.resumeCursor)?.sessionId;
          const acpNativeLoggers = makeAcpNativeLoggers({
            nativeEventLogger,
            provider: PROVIDER,
            threadId: input.threadId,
          });

          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          const acp = yield* observeOperation({
            operation: "spawn",
            stage: "spawn",
            runtimeMode: input.runtimeMode,
            effect: makeGitHubCopilotAcpRuntime({
              settings: copilotSettings,
              ...(options?.environment ? { environment: options.environment } : {}),
              childProcessSpawner,
              cwd,
              ...(resumeSessionId ? { resumeSessionId } : {}),
              sessionLoadTimeout: SESSION_LOAD_TIMEOUT,
              clientInfo: { name: "t3-code", version: "0.0.0" },
              ...(mcpSession
                ? {
                    mcpServers: [
                      {
                        type: "http" as const,
                        name: "t3-code",
                        url: mcpSession.endpoint,
                        headers: [
                          {
                            name: "Authorization",
                            value: mcpSession.authorizationHeader,
                          },
                        ],
                      },
                    ],
                  }
                : {}),
              ...acpNativeLoggers,
            }).pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.provideService(Scope.Scope, sessionScope),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: cause.message,
                    cause,
                  }),
              ),
            ),
          });

          const started = yield* Effect.gen(function* () {
            yield* acp.handleRequestPermission((params) =>
              mapAcpCallbackFailure(
                Effect.gen(function* () {
                  yield* logNative(input.threadId, "session/request_permission", params);
                  const permissionCtx = sessions.get(input.threadId);
                  permissionCtx?.permissionRequestedToolCallIds.add(params.toolCall.toolCallId);
                  if (input.runtimeMode === "full-access") {
                    const autoApprovedOptionId = selectAutoApprovedPermissionOption(params);
                    if (autoApprovedOptionId !== undefined) {
                      yield* emitTelemetry({
                        operation: "permission",
                        outcome: "success",
                        stage: "permission",
                        interactionMode: permissionCtx?.turnTelemetry?.interactionMode,
                        runtimeMode: input.runtimeMode,
                        permissionRequested: true,
                        permissionDecision: "auto_approved",
                      });
                      return {
                        outcome: {
                          outcome: "selected" as const,
                          optionId: autoApprovedOptionId,
                        },
                      };
                    }
                  }
                  const permissionRequest = parsePermissionRequest(params);
                  const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
                  const runtimeRequestId = RuntimeRequestId.make(requestId);
                  const decision = yield* Deferred.make<ProviderApprovalDecision>();
                  const turnId = sessions.get(input.threadId)?.activeTurnId;
                  pendingApprovals.set(requestId, { decision });
                  yield* offerRuntimeEvent(
                    makeAcpRequestOpenedEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      requestId: runtimeRequestId,
                      permissionRequest,
                      detail:
                        permissionRequest.detail ??
                        encodeJsonStringForDiagnostics(params)?.slice(0, 2000) ??
                        "[unserializable params]",
                      args: params,
                      source: "acp.jsonrpc",
                      method: "session/request_permission",
                      rawPayload: params,
                    }),
                  );
                  const resolved = yield* Deferred.await(decision);
                  pendingApprovals.delete(requestId);
                  yield* emitTelemetry({
                    operation: "permission",
                    outcome: resolved === "cancel" ? "cancelled" : "success",
                    stage: "permission",
                    interactionMode: sessions.get(input.threadId)?.turnTelemetry?.interactionMode,
                    runtimeMode: input.runtimeMode,
                    permissionRequested: true,
                    permissionDecision:
                      resolved === "acceptForSession"
                        ? "accept_for_session"
                        : resolved === "acceptAlways"
                          ? "accept_always"
                          : resolved,
                  });
                  yield* offerRuntimeEvent(
                    makeAcpRequestResolvedEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      requestId: runtimeRequestId,
                      permissionRequest,
                      decision: resolved,
                    }),
                  );
                  const selectedOptionId =
                    resolved === "cancel" ? undefined : selectPermissionOptionId(params, resolved);
                  return {
                    outcome: selectedOptionId
                      ? {
                          outcome: "selected" as const,
                          optionId: selectedOptionId,
                        }
                      : ({ outcome: "cancelled" } as const),
                  };
                }),
              ),
            );
            return yield* observeOperation({
              operation: resumeSessionId ? "session_load" : "session_new",
              stage: resumeSessionId ? "session_load" : "session_new",
              runtimeMode: input.runtimeMode,
              effect: acp.start(),
            });
          }).pipe(
            Effect.mapError((error) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
            ),
          );

          const requestedModel = copilotModelSelection?.model?.trim() || undefined;
          const requestedSelections = copilotModelSelection?.options ?? undefined;
          const appliedModel = yield* observeOperation({
            operation: "config",
            stage: "config",
            runtimeMode: input.runtimeMode,
            effect: applySessionConfiguration({
              threadId: input.threadId,
              runtime: acp,
              model: requestedModel,
              selections: requestedSelections,
              // startSession carries no interaction mode; Copilot starts in Agent
              // mode and a per-turn `plan` request switches it.
              interactionMode: undefined,
              runtimeMode: input.runtimeMode,
            }),
          });
          // Copilot sometimes publishes its model list only after the first
          // prompt, so fall back to what the user asked for instead of dropping
          // the selection from the session snapshot.
          const boundModelId =
            appliedModel ??
            requestedModel ??
            currentGitHubCopilotModelId(started.sessionSetupResult);

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(boundModelId ? { model: boundModelId } : {}),
            threadId: input.threadId,
            resumeCursor: {
              schemaVersion: GITHUB_COPILOT_RESUME_VERSION,
              sessionId: started.sessionId,
            },
            createdAt: now,
            updatedAt: now,
          };

          const ctx: GitHubCopilotSessionContext = {
            threadId: input.threadId,
            acpSessionId: started.sessionId,
            session,
            scope: sessionScope,
            acp,
            notificationFiber: undefined,
            pendingApprovals,
            turns: [],
            lastPlanFingerprint: undefined,
            activeTurnId: undefined,
            acceptingNotifications: false,
            interruptedTurnIds: new Set(),
            promptsInFlight: 0,
            requestedModel,
            requestedSelections,
            currentModelId: boundModelId,
            turnTelemetry: undefined,
            pendingTailTelemetry: undefined,
            permissionRequestedToolCallIds: new Set(),
            terminalToolCallIds: new Set(),
            stopped: false,
          };

          const nf = yield* Stream.runDrain(
            Stream.mapEffect(acp.getEvents(), (event) =>
              Effect.gen(function* () {
                if (event._tag === "EventStreamBarrier") {
                  yield* Deferred.succeed(event.acknowledge, undefined);
                  return;
                }
                const observedAtMs = yield* Clock.currentTimeMillis;
                if (ctx.pendingTailTelemetry) {
                  ctx.pendingTailTelemetry.lastNotificationAtMs = observedAtMs;
                  if (ctx.pendingTailTelemetry.nextTurnWindowStarted) {
                    ctx.pendingTailTelemetry.crossedTurnWindow = true;
                  }
                }
                if (
                  event._tag === "PlanUpdated" ||
                  event._tag === "ToolCallUpdated" ||
                  event._tag === "ContentDelta" ||
                  event._tag === "ReasoningDelta" ||
                  event._tag === "UsageUpdated"
                ) {
                  yield* logNative(ctx.threadId, "session/update", event.rawPayload);
                }

                if (event._tag === "ModeChanged") {
                  return;
                }

                // Copilot has been observed streaming for minutes after a
                // prompt settled. Turn lifecycle is owned by prompt
                // settlement, so notifications without a live turn — or for a
                // turn already interrupted — are dropped instead of
                // resurrecting it.
                const notificationTurnId = ctx.activeTurnId;
                if (
                  notificationTurnId === undefined ||
                  !ctx.acceptingNotifications ||
                  ctx.interruptedTurnIds.has(notificationTurnId)
                ) {
                  return;
                }
                const stamp = yield* makeEventStamp();

                switch (event._tag) {
                  case "AssistantItemStarted":
                    yield* offerRuntimeEvent(
                      makeAcpAssistantItemEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        itemId: event.itemId,
                        lifecycle: "item.started",
                      }),
                    );
                    return;
                  case "AssistantItemCompleted":
                    yield* offerRuntimeEvent(
                      makeAcpAssistantItemEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        itemId: event.itemId,
                        lifecycle: "item.completed",
                      }),
                    );
                    return;
                  case "PlanUpdated":
                    yield* emitPlanUpdate(
                      ctx,
                      notificationTurnId,
                      stamp,
                      event.payload,
                      event.rawPayload,
                      "session/update",
                    );
                    return;
                  case "ToolCallUpdated":
                    const agencyTaskEvent = makeGitHubCopilotAgencyTaskEvent({
                      stamp,
                      provider: PROVIDER,
                      threadId: ctx.threadId,
                      turnId: notificationTurnId,
                      toolCall: event.toolCall,
                      rawPayload: event.rawPayload,
                    });
                    if (
                      (event.toolCall.status === "completed" ||
                        event.toolCall.status === "failed") &&
                      !ctx.terminalToolCallIds.has(event.toolCall.toolCallId)
                    ) {
                      ctx.terminalToolCallIds.add(event.toolCall.toolCallId);
                      yield* emitTelemetry({
                        operation: "tool",
                        outcome: event.toolCall.status === "completed" ? "success" : "failure",
                        stage: "notification",
                        interactionMode: ctx.turnTelemetry?.interactionMode,
                        runtimeMode: ctx.session.runtimeMode,
                        toolKind:
                          agencyTaskEvent !== undefined
                            ? "subagent"
                            : normalizeGitHubCopilotToolKind(event.toolCall.kind),
                        toolOutcome: event.toolCall.status,
                        permissionRequested: ctx.permissionRequestedToolCallIds.delete(
                          event.toolCall.toolCallId,
                        ),
                      });
                    }
                    if (agencyTaskEvent !== null) {
                      yield* offerRuntimeEvent(
                        agencyTaskEvent ??
                          makeAcpToolCallEvent({
                            stamp,
                            provider: PROVIDER,
                            threadId: ctx.threadId,
                            turnId: notificationTurnId,
                            toolCall: event.toolCall,
                            rawPayload: event.rawPayload,
                          }),
                      );
                    }
                    return;
                  case "ContentDelta":
                    if (ctx.turnTelemetry && !ctx.turnTelemetry.firstResponseRecorded) {
                      ctx.turnTelemetry.firstResponseRecorded = true;
                      yield* emitTelemetry({
                        operation: "first_response",
                        outcome: "success",
                        stage: "notification",
                        durationMs: Math.max(
                          0,
                          observedAtMs -
                            (ctx.turnTelemetry.promptSubmittedAtMs ??
                              ctx.turnTelemetry.startedAtMs),
                        ),
                        interactionMode: ctx.turnTelemetry.interactionMode,
                        runtimeMode: ctx.turnTelemetry.runtimeMode,
                      });
                    }
                    yield* offerRuntimeEvent(
                      makeAcpContentDeltaEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        ...(event.itemId ? { itemId: event.itemId } : {}),
                        text: event.text,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                  case "ReasoningDelta":
                    if (ctx.turnTelemetry && !ctx.turnTelemetry.firstResponseRecorded) {
                      ctx.turnTelemetry.firstResponseRecorded = true;
                      yield* emitTelemetry({
                        operation: "first_response",
                        outcome: "success",
                        stage: "notification",
                        durationMs: Math.max(
                          0,
                          observedAtMs -
                            (ctx.turnTelemetry.promptSubmittedAtMs ??
                              ctx.turnTelemetry.startedAtMs),
                        ),
                        interactionMode: ctx.turnTelemetry.interactionMode,
                        runtimeMode: ctx.turnTelemetry.runtimeMode,
                      });
                    }
                    yield* offerRuntimeEvent(
                      makeAcpReasoningDeltaEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        text: event.text,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                  case "UsageUpdated":
                    if (ctx.turnTelemetry) {
                      ctx.turnTelemetry.latestUsage = {
                        usedTokens: event.usage.usedTokens,
                        contextSize: event.usage.maxTokens,
                      };
                    }
                    yield* offerRuntimeEvent(
                      makeAcpThreadTokenUsageUpdatedEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        usage: event.usage,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                }
              }),
            ),
          ).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                const { cliVersionBucket: _cliVersionBucket, ...failureRecord } =
                  telemetryFailureRecord({
                    operation: "notification",
                    fallbackStage: "notification",
                    cause,
                    cliVersionBucket: getCliVersionBucket(),
                  });
                yield* emitTelemetry(failureRecord);
                yield* Effect.logError("Failed to process GitHub Copilot runtime notification.", {
                  cause,
                });
              }),
            ),
            // Fork into the session scope, not the calling fiber. `forkChild`
            // makes this a child of `startSession`, and Effect interrupts a
            // fiber's children when it completes, so the consumer died as soon
            // as `startSession` returned and every later notification was
            // dropped. The scope is created, stored on the context and closed
            // on teardown already; only the fork target was wrong.
            Effect.forkIn(ctx.scope),
          );

          ctx.notificationFiber = nf;
          sessions.set(input.threadId, ctx);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: started.initializeResult },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "GitHub Copilot ACP session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: started.sessionId },
          });

          return session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: GitHubCopilotAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const prepared = yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(input.threadId);
            // A sendTurn while a prompt is in flight is a steer: the agent
            // folds the new prompt into the ongoing work, so the active turn
            // id is reused instead of opening a new turn.
            const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
            const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
            const turnStartedAtMs = yield* Clock.currentTimeMillis;
            // Count this prompt immediately so a superseded in-flight prompt
            // resolving from here on does not settle the turn; decremented on
            // preparation failure here, and after the prompt below otherwise.
            ctx.promptsInFlight += 1;
            // Bind the turn id before cooperative yields so interruptTurn can
            // settle this prompt even if stop arrives during preparation.
            ctx.activeTurnId = turnId;
            if (steeringTurnId === undefined) {
              ctx.acceptingNotifications = false;
              if (ctx.pendingTailTelemetry) {
                ctx.pendingTailTelemetry.nextTurnWindowStarted = true;
              }
              ctx.turnTelemetry = {
                turnId,
                startedAtMs: turnStartedAtMs,
                interactionMode: input.interactionMode ?? "default",
                runtimeMode: ctx.session.runtimeMode,
                firstResponseRecorded: false,
                promptSubmittedAtMs: undefined,
                failureRecorded: false,
                latestUsage: undefined,
              };
            }
            ctx.session = {
              ...ctx.session,
              status: steeringTurnId === undefined ? "connecting" : "running",
              activeTurnId: turnId,
              updatedAt: yield* nowIso,
            };

            return yield* Effect.gen(function* () {
              const turnModelSelection =
                input.modelSelection?.instanceId === boundInstanceId
                  ? input.modelSelection
                  : undefined;
              // Replay the standing selection when this turn carries none: the
              // `model` config option may only have appeared after the first
              // prompt, and the user's choice must still land.
              const requestedModel = turnModelSelection?.model?.trim() || ctx.requestedModel;
              const requestedSelections = turnModelSelection?.options ?? ctx.requestedSelections;
              const appliedModel = yield* observeOperation({
                operation: "config",
                stage: "config",
                interactionMode: input.interactionMode ?? "default",
                runtimeMode: ctx.session.runtimeMode,
                effect: applySessionConfiguration({
                  threadId: input.threadId,
                  runtime: ctx.acp,
                  model: requestedModel,
                  selections: requestedSelections,
                  interactionMode: input.interactionMode,
                  runtimeMode: ctx.session.runtimeMode,
                }),
              });

              const text = input.input?.trim();
              const imagePromptParts = yield* Effect.forEach(
                input.attachments ?? [],
                (attachment) =>
                  Effect.gen(function* () {
                    const attachmentPath = resolveAttachmentPath({
                      attachmentsDir: serverConfig.attachmentsDir,
                      attachment,
                    });
                    if (!attachmentPath) {
                      return yield* new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "session/prompt",
                        detail: `Invalid attachment id '${attachment.id}'.`,
                      });
                    }
                    const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                      Effect.mapError(
                        (cause) =>
                          new ProviderAdapterRequestError({
                            provider: PROVIDER,
                            method: "session/prompt",
                            detail: cause.message,
                            cause,
                          }),
                      ),
                    );
                    return {
                      type: "image",
                      data: Buffer.from(bytes).toString("base64"),
                      mimeType: attachment.mimeType,
                    } satisfies EffectAcpSchema.ContentBlock;
                  }),
              );
              const promptParts: Array<EffectAcpSchema.ContentBlock> = [
                ...(text ? [{ type: "text" as const, text }] : []),
                ...imagePromptParts,
              ];

              if (promptParts.length === 0) {
                return yield* new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "sendTurn",
                  issue: "Turn requires non-empty text or attachments.",
                });
              }

              ctx.requestedModel = requestedModel;
              ctx.requestedSelections = requestedSelections;
              ctx.currentModelId = appliedModel ?? requestedModel ?? ctx.currentModelId;
              const displayModel = ctx.currentModelId;
              for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
                yield* Effect.yieldNow;
              }
              if (ctx.interruptedTurnIds.has(turnId)) {
                yield* settlePromptInFlight(input.threadId, turnId, ctx.acpSessionId, {
                  completedStopReason: "cancelled",
                  emitTurnCompletion: false,
                  settleAllPrompts: true,
                });
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: "GitHub Copilot prompt was interrupted during preparation.",
                });
              }
              if (steeringTurnId === undefined) {
                ctx.lastPlanFingerprint = undefined;
              }
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: turnId,
                updatedAt: yield* nowIso,
                ...(displayModel ? { model: displayModel } : {}),
              };

              if (steeringTurnId === undefined) {
                // Flush everything Copilot queued after the previous prompt
                // while notifications are still gated. Once the new prompt is
                // live, subsequent notifications belong to this turn.
                yield* ctx.acp.drainEvents;
                yield* completeTailTelemetry(ctx);
                ctx.permissionRequestedToolCallIds.clear();
                ctx.terminalToolCallIds.clear();
                yield* offerRuntimeEvent({
                  type: "turn.started",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: displayModel ? { model: displayModel } : {},
                });
                ctx.acceptingNotifications = true;
              }

              return {
                acp: ctx.acp,
                acpSessionId: ctx.acpSessionId,
                displayModel,
                promptParts,
                turnId,
              };
            }).pipe(
              Effect.tapCause(() =>
                Effect.gen(function* () {
                  const liveCtx = sessions.get(input.threadId);
                  if (!liveCtx) {
                    return;
                  }
                  yield* settlePromptInFlight(input.threadId, turnId, liveCtx.acpSessionId, {
                    errorMessage: "GitHub Copilot prompt preparation failed.",
                    emitTurnCompletion: false,
                  });
                }),
              ),
            );
          }),
        );
        const promptSettled = yield* Ref.make(false);
        const promptRpcSucceeded = yield* Ref.make(false);
        const promptResultRef = yield* Ref.make<EffectAcpSchema.PromptResponse | undefined>(
          undefined,
        );
        const promptFailureMessageRef = yield* Ref.make<string | undefined>(undefined);

        return yield* Effect.gen(function* () {
          const promptSubmittedAtMs = yield* Clock.currentTimeMillis;
          const liveCtx = sessions.get(input.threadId);
          if (liveCtx?.turnTelemetry?.turnId === prepared.turnId) {
            liveCtx.turnTelemetry.promptSubmittedAtMs = promptSubmittedAtMs;
          }
          const result = yield* prepared.acp
            .prompt({
              prompt: prepared.promptParts,
            })
            .pipe(
              Effect.tap((promptResult) =>
                Effect.gen(function* () {
                  yield* Ref.set(promptRpcSucceeded, true);
                  yield* Ref.set(promptResultRef, promptResult);
                  const settledAtMs = yield* Clock.currentTimeMillis;
                  const liveCtx = sessions.get(input.threadId);
                  if (
                    liveCtx &&
                    liveCtx.acpSessionId === prepared.acpSessionId &&
                    liveCtx.turnTelemetry?.turnId === prepared.turnId
                  ) {
                    liveCtx.pendingTailTelemetry = {
                      settledAtMs,
                      lastNotificationAtMs: undefined,
                      crossedTurnWindow: false,
                      nextTurnWindowStarted: false,
                      interactionMode: liveCtx.turnTelemetry.interactionMode,
                      runtimeMode: liveCtx.turnTelemetry.runtimeMode,
                    };
                  }
                }),
              ),
              Effect.tapError((error) =>
                Effect.gen(function* () {
                  yield* Ref.set(
                    promptFailureMessageRef,
                    mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error).message,
                  );
                  const failedAtMs = yield* Clock.currentTimeMillis;
                  const failedCtx = sessions.get(input.threadId);
                  const turn = failedCtx?.turnTelemetry;
                  if (turn?.turnId === prepared.turnId) {
                    const { cliVersionBucket: _cliVersionBucket, ...failureRecord } =
                      telemetryFailureRecord({
                        operation: "turn",
                        fallbackStage: "prompt",
                        cause: error,
                        durationMs: Math.max(0, failedAtMs - turn.startedAtMs),
                        cliVersionBucket: getCliVersionBucket(),
                        interactionMode: turn.interactionMode,
                        runtimeMode: turn.runtimeMode,
                      });
                    yield* emitTelemetry(failureRecord);
                    turn.failureRecorded = true;
                  }
                  yield* prepared.acp.drainEvents;
                }),
              ),
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
              ),
            );

          return yield* withThreadLock(
            input.threadId,
            Effect.gen(function* () {
              const ctx = yield* requireSession(input.threadId);
              if (ctx.acpSessionId !== prepared.acpSessionId) {
                yield* settlePromptInFlight(
                  input.threadId,
                  prepared.turnId,
                  prepared.acpSessionId,
                  {
                    errorMessage: "GitHub Copilot session changed before the turn completed.",
                    settleAllPrompts: true,
                  },
                );
                yield* Ref.set(promptSettled, true);
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: "GitHub Copilot session changed before the turn completed.",
                });
              }
              // Keep prompt settlement atomic with respect to Stop and steering.
              // interruptTurn marks its target before waiting for this lock, so
              // cancellation can still win while queued ACP events are drained.
              for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
                yield* Effect.yieldNow;
              }
              // Every notification queued before the prompt returned is
              // published under the still-open turn before it completes.
              yield* prepared.acp.drainEvents;
              if (ctx.interruptedTurnIds.has(prepared.turnId)) {
                yield* Ref.set(promptSettled, true);
                return {
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  resumeCursor: ctx.session.resumeCursor,
                };
              }

              if (
                ctx.promptsInFlight <= 0 ||
                ctx.activeTurnId !== prepared.turnId ||
                ctx.session.activeTurnId !== prepared.turnId
              ) {
                yield* Ref.set(promptSettled, true);
                return {
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  resumeCursor: ctx.session.resumeCursor,
                };
              }

              appendPromptResultToTurn(ctx, prepared.turnId, prepared.promptParts, result);
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: prepared.turnId,
                updatedAt: yield* nowIso,
                ...(prepared.displayModel ? { model: prepared.displayModel } : {}),
              };
              const remainingPrompts = Math.max(0, ctx.promptsInFlight - 1);
              ctx.promptsInFlight = remainingPrompts;

              // Only the last remaining prompt settles the turn. A steer-
              // superseded prompt resolving while another is in flight or
              // pending must leave the merged turn running.
              if (
                remainingPrompts === 0 &&
                ctx.activeTurnId === prepared.turnId &&
                ctx.session.activeTurnId === prepared.turnId
              ) {
                if (ctx.interruptedTurnIds.has(prepared.turnId)) {
                  yield* Ref.set(promptSettled, true);
                  return {
                    threadId: input.threadId,
                    turnId: prepared.turnId,
                    resumeCursor: ctx.session.resumeCursor,
                  };
                }
                const completedAt = yield* nowIso;
                const { activeTurnId: _completedTurnId, ...readySession } = ctx.session;
                ctx.activeTurnId = undefined;
                ctx.acceptingNotifications = false;
                ctx.session = {
                  ...readySession,
                  status: "ready",
                  updatedAt: completedAt,
                  ...(prepared.displayModel ? { model: prepared.displayModel } : {}),
                };
                yield* offerRuntimeEvent({
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  payload: {
                    state: result.stopReason === "cancelled" ? "cancelled" : "completed",
                    stopReason: result.stopReason,
                  },
                });
                yield* completeTurnTelemetry(
                  ctx,
                  prepared.turnId,
                  result.stopReason === "cancelled" ? "cancelled" : "success",
                  result.stopReason,
                );
                ctx.interruptedTurnIds.delete(prepared.turnId);
                yield* Ref.set(promptSettled, true);
              } else if (remainingPrompts > 0) {
                yield* Ref.set(promptSettled, true);
              }

              return {
                threadId: input.threadId,
                turnId: prepared.turnId,
                resumeCursor: ctx.session.resumeCursor,
              };
            }),
          );
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              if (yield* Ref.get(promptSettled)) {
                return;
              }

              if (yield* Ref.get(promptRpcSucceeded)) {
                const promptResult = yield* Ref.get(promptResultRef);
                if (promptResult === undefined) {
                  return;
                }
                yield* withThreadLock(
                  input.threadId,
                  Effect.gen(function* () {
                    const ctx = yield* requireSession(input.threadId);
                    if (ctx.acpSessionId !== prepared.acpSessionId) {
                      yield* settlePromptInFlight(
                        input.threadId,
                        prepared.turnId,
                        prepared.acpSessionId,
                        {
                          errorMessage: "GitHub Copilot session changed before the turn completed.",
                          settleAllPrompts: true,
                        },
                      );
                      return;
                    }
                    if (ctx.interruptedTurnIds.has(prepared.turnId)) {
                      return;
                    }
                    if (
                      ctx.promptsInFlight <= 0 ||
                      ctx.activeTurnId !== prepared.turnId ||
                      ctx.session.activeTurnId !== prepared.turnId
                    ) {
                      return;
                    }
                    appendPromptResultToTurn(
                      ctx,
                      prepared.turnId,
                      prepared.promptParts,
                      promptResult,
                    );
                    yield* settlePromptInFlight(
                      input.threadId,
                      prepared.turnId,
                      prepared.acpSessionId,
                      { completedStopReason: promptResult.stopReason },
                    );
                  }),
                );
                return;
              }

              const errorMessage = yield* Ref.get(promptFailureMessageRef);
              yield* withThreadLock(
                input.threadId,
                settlePromptInFlight(input.threadId, prepared.turnId, prepared.acpSessionId, {
                  errorMessage: errorMessage ?? "GitHub Copilot prompt request failed.",
                }),
              );
            }).pipe(Effect.catch(() => Effect.void)),
          ),
        );
      });

    const interruptTurn: GitHubCopilotAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        const observed = yield* Effect.sync(() => {
          const ctx = sessions.get(threadId);
          if (!ctx || ctx.stopped) {
            return {
              _tag: "Proceed" as const,
              acpSessionId: undefined,
              interruptedTurnId: turnId,
            };
          }
          const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
          if (turnId !== undefined && activeTurnId !== undefined && activeTurnId !== turnId) {
            return { _tag: "Ignore" as const };
          }
          const interruptedTurnId = turnId ?? activeTurnId;
          // Marked before the lock is taken so a prompt settling in the
          // meantime observes the cancellation instead of completing the turn.
          if (interruptedTurnId !== undefined) {
            ctx.interruptedTurnIds.add(interruptedTurnId);
          }
          return {
            _tag: "Proceed" as const,
            acpSessionId: ctx.acpSessionId,
            interruptedTurnId,
          };
        });
        if (observed._tag === "Ignore") {
          return;
        }

        yield* withThreadLock(
          threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(threadId);
            if (observed.acpSessionId !== undefined && ctx.acpSessionId !== observed.acpSessionId) {
              return;
            }
            const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
            if (turnId !== undefined && activeTurnId !== undefined && activeTurnId !== turnId) {
              return;
            }
            if (
              observed.interruptedTurnId !== undefined &&
              activeTurnId !== undefined &&
              activeTurnId !== observed.interruptedTurnId
            ) {
              return;
            }
            const interruptedTurnId =
              observed.interruptedTurnId ?? turnId ?? activeTurnId ?? ctx.session.activeTurnId;
            yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
            yield* Effect.ignore(
              observeOperation({
                operation: "cancel",
                stage: "cancel",
                interactionMode: ctx.turnTelemetry?.interactionMode,
                runtimeMode: ctx.session.runtimeMode,
                effect: ctx.acp.cancel.pipe(
                  Effect.mapError((error) =>
                    mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
                  ),
                ),
              }),
            );
            if (interruptedTurnId) {
              ctx.interruptedTurnIds.add(interruptedTurnId);
              yield* settlePromptInFlight(threadId, interruptedTurnId, ctx.acpSessionId, {
                completedStopReason: "cancelled",
                settleAllPrompts: true,
              });
            } else if (
              ctx.promptsInFlight > 0 ||
              ctx.session.status === "running" ||
              ctx.session.status === "connecting"
            ) {
              const updatedAt = yield* nowIso;
              ctx.promptsInFlight = 0;
              ctx.activeTurnId = undefined;
              ctx.acceptingNotifications = false;
              const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
              ctx.session = {
                ...readySession,
                status: "ready",
                updatedAt,
              };
            }
          }),
        );
      });

    const respondToRequest: GitHubCopilotAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: GitHubCopilotAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
    ) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        // Copilot exposes no structured user-input request over ACP, so this
        // adapter never opens one and any answer is for an unknown request.
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/user_input",
          detail: `Unknown pending user-input request: ${requestId}`,
        });
      });

    const readThread: GitHubCopilotAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: GitHubCopilotAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "GitHub Copilot ACP sessions do not support provider-side rollback yet.",
        });
      });

    const stopSession: GitHubCopilotAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: GitHubCopilotAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: GitHubCopilotAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: GitHubCopilotAdapterShape["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.ignore(stopAll()).pipe(
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
        Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies GitHubCopilotAdapterShape;
  });
}
