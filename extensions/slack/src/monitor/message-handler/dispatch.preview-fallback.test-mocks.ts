// Mocks and per-test state shared by the dispatchPreparedSlackMessage test files.
// Every vi.mock lives here, so it is registered before anything from the dispatch
// graph loads; the helpers and tests take their plugin-sdk and Slack imports from
// this module as well, so a sorted import order cannot load a real module first.
import { projectAgentToolActivity } from "openclaw/plugin-sdk/agent-harness-runtime";
import { createMessageReceiptFromOutboundResults } from "openclaw/plugin-sdk/channel-outbound";
import {
  createReplyDispatcher,
  type GetReplyOptions,
  type ReplyPayload,
} from "openclaw/plugin-sdk/reply-runtime";
import { vi } from "vitest";
import type { SlackSendResult } from "../../send.js";
import { planSlackStreamUpdateFit, SlackStreamMessageLedger } from "../../stream-size.js";
import type { SlackReplyOptionEvent } from "./dispatch.compact-progress.test-support.js";

export { projectProgressCardChannelUpdate } from "openclaw/plugin-sdk/agent-harness-runtime";
export { createMessageReceiptFromOutboundResults } from "openclaw/plugin-sdk/channel-outbound";
export {
  createTestRegistry,
  resetPluginRuntimeStateForTest,
  setActivePluginRegistry,
} from "openclaw/plugin-sdk/channel-test-helpers";
export { finalizeInboundContext } from "openclaw/plugin-sdk/reply-runtime";
export { createRequireRecord } from "openclaw/plugin-sdk/test-fixtures";
export { buildSlackCompleteBlocksFallbackText } from "../../blocks-fallback.js";
export { slackSetupPlugin } from "../../channel.setup.js";
export { getSlackSessionRuns } from "../session-run-targets.js";
export { emitCompactProgressScenario } from "./dispatch.compact-progress.test-support.js";
export type { SlackReplyOptionEvent } from "./dispatch.compact-progress.test-support.js";
export type { PreparedSlackMessage } from "./types.js";

export const FINAL_REPLY_TEXT = "final answer";
export const THREAD_TS = "thread-1";
export const STREAM_MESSAGE_TS = "171234.567";
export const SAME_TEXT = "same reply";

const hoisted = vi.hoisted(() => ({
  getGlobalHookRunnerMock: vi.fn(),
  updateLastRouteMock: vi.fn(async () => {}),
}));
export const getGlobalHookRunnerMock = hoisted.getGlobalHookRunnerMock;
export const createSlackDraftStreamMock = vi.fn();
export type DeliveryParams = Omit<
  Parameters<typeof import("../replies.js").deliverReplies>[0],
  "replies"
> & {
  replies: ReplyPayload[];
};
export const normalDeliveryResult = {
  messageId: "normal-final",
  channelId: "C123",
  receipt: createMessageReceiptFromOutboundResults({
    results: [{ messageId: "normal-final", channelId: "C123" }],
  }),
};
export const deliverRepliesMock = vi.fn(
  async (_params: DeliveryParams): Promise<SlackSendResult | undefined> => normalDeliveryResult,
);
export const sendMessageSlackMock = vi.fn<typeof import("../../send.js").sendMessageSlack>();
export const finalizeSlackPreviewEditMock = vi.fn(async (_input: { blocks?: unknown }) => {});
export const normalizeSlackOutboundTextMock = vi.fn((value: string) => value.trim());
export const postMessageMock = vi.fn(async () => ({ ok: true, ts: "171234.999" }));
export const chatUpdateMock = vi.fn(async () => ({ ok: true, ts: "171234.999" }));
export const recordSlackThreadParticipationMock = vi.fn();
export const updateLastRouteMock = hoisted.updateLastRouteMock;
export const appendSlackStreamMock = vi.fn(async (_input?: unknown) => {});
export const startSlackStreamMock = vi.fn(async (_input?: unknown) => ({
  channel: "C123",
  threadTs: THREAD_TS,
  stopped: false,
  delivered: true,
  pendingText: "",
}));
export const stopSlackStreamMock = vi.fn(
  async (_params?: unknown) => ({}) as { messageId?: string },
);
// Every accepted start, append and stop is charged to its message with the
// production ledger; a message past the row cap or the byte budget fails the
// test in afterEach. On for every test: no scenario may exceed one message.
// The budget is read back from the planner (no production export): the row
// cap is how many tiny rows a fresh message admits, the byte budget the
// longest text a message that already holds text still takes.
export const SLACK_STREAM_MESSAGE_BUDGET = (() => {
  const rows = planSlackStreamUpdateFit(new SlackStreamMessageLedger(), {
    chunks: Array.from({ length: 200 }, (_, index) => ({
      type: "task_update" as const,
      id: `r${index}`,
      title: "x",
      status: "complete" as const,
    })),
  }).admittedTaskIds.size;
  const held = new SlackStreamMessageLedger();
  held.recordText("x");
  let low = 1;
  let high = 100_000;
  while (low < high) {
    const mid = Math.ceil((low + high) / 2);
    if (planSlackStreamUpdateFit(held, { text: "y".repeat(mid) }).textFits) {
      low = mid;
    } else {
      high = mid - 1;
    }
  }
  return { tasks: rows, chars: low + held.size };
})();
export const streamBudgetGuard = {
  enabled: true,
  ledgers: new WeakMap<object, SlackStreamMessageLedger>(),
  violations: [] as string[],
};
export function chargeStreamBudget(session: unknown, call: string, update: unknown) {
  if (!streamBudgetGuard.enabled || !session || typeof session !== "object") {
    return;
  }
  const params = update as { text?: unknown; chunks?: unknown };
  let ledger = streamBudgetGuard.ledgers.get(session);
  if (!ledger) {
    ledger = new SlackStreamMessageLedger();
    streamBudgetGuard.ledgers.set(session, ledger);
  }
  ledger.record({
    ...(typeof params.text === "string" ? { text: params.text } : {}),
    ...(Array.isArray(params.chunks)
      ? { chunks: params.chunks as Parameters<SlackStreamMessageLedger["record"]>[0]["chunks"] }
      : {}),
  });
  if (
    ledger.taskCount > SLACK_STREAM_MESSAGE_BUDGET.tasks ||
    ledger.size > SLACK_STREAM_MESSAGE_BUDGET.chars
  ) {
    streamBudgetGuard.violations.push(
      `${call}: message holds ${ledger.taskCount} rows and ${ledger.size} weighted bytes (budget ${SLACK_STREAM_MESSAGE_BUDGET.tasks} rows, ${SLACK_STREAM_MESSAGE_BUDGET.chars} bytes)`,
    );
  }
}
export const emitSlackMessageSentHooksMock = vi.fn(() => {});
export const reactSlackMessageMock = vi.fn(async () => {});
export const removeSlackReactionMock = vi.fn(async () => {});
export const logVerboseMock = vi.fn();
export class TestSlackStreamNotDeliveredError extends Error {
  readonly pendingText: string;
  readonly slackCode: string;
  constructor(pendingText: string, slackCode: string) {
    super(`slack-stream not delivered: ${slackCode}`);
    this.name = "SlackStreamNotDeliveredError";
    this.pendingText = pendingText;
    this.slackCode = slackCode;
  }
}
export class TestSlackStreamMessageTooLongError extends TestSlackStreamNotDeliveredError {
  constructor(pendingText: string) {
    super(pendingText, "msg_too_long");
    this.name = "SlackStreamMessageTooLongError";
  }
}
export const statusReactionControllerMock = {
  setQueued: vi.fn(async () => {}),
  setThinking: vi.fn(async () => {}),
  setTool: vi.fn(async () => {}),
  setError: vi.fn(async () => {}),
  setDone: vi.fn(async () => {}),
  clear: vi.fn(async () => {}),
  restoreInitial: vi.fn(async () => {}),
};
export type TestReplyDispatchKind = "tool" | "block" | "final";
export type TestReplyPayload = {
  text?: string;
  isError?: boolean;
  isReasoning?: boolean;
  mediaUrl?: string;
  mediaUrls?: string[];
  audioAsVoice?: boolean;
  spokenText?: string;
  ttsSupplement?: { spokenText: string; visibleTextAlreadyDelivered?: boolean };
  presentation?: { blocks: unknown[] };
};
export type TestDispatchCounts = Record<TestReplyDispatchKind, number>;
export type TestDispatchSequenceEntry =
  | {
      kind: TestReplyDispatchKind;
      payload: TestReplyPayload;
    }
  | { kind: "queued_followup" }
  | { kind: "item"; progressText: string };

type HarnessState = {
  mockedNativeStreaming: boolean;
  mockedBlockStreamingEnabled: boolean | undefined;
  mockedSlackStreamingMode: "off" | "partial" | "block" | "progress";
  mockedPinnedMainDmOwner: string | undefined;
  capturedReplyOptions: GetReplyOptions | undefined;
  capturedStatusReactionOptions: { enabled?: boolean; initialEmoji?: string } | undefined;
  mockedReplyThreadTs: string | undefined;
  mockedStatusThreadTs: string | undefined;
  mockedReplyThreadTsSequence: Array<string | undefined> | undefined;
  mockedSlackReplyBlocks: unknown[] | undefined;
  mockedSlackIsThreadReply: boolean;
  capturedTyping:
    | {
        start: () => Promise<void>;
        stop?: () => Promise<void>;
        onStartError: (err: unknown) => void;
        onStopError?: (err: unknown) => void;
      }
    | undefined;
  mockedDispatchSequence: TestDispatchSequenceEntry[];
  mockedQueuedDispatchCounts: TestDispatchCounts;
  mockedAgentRunTerminalOutcome: "completed" | "failed" | undefined;
  mockedSourceReplyDelivered: boolean;
  mockedDispatchError: Error | undefined;
  useRealChannelInboundTurn: boolean;
  mockedProgressEvents: string[];
  mockedReplyOptionEvents: SlackReplyOptionEvent[];
};

/** Per-test state the mocks read and tests arrange; reset by the harness hooks. */
export const harness: HarnessState = {
  mockedNativeStreaming: false,
  mockedBlockStreamingEnabled: false,
  mockedSlackStreamingMode: "partial",
  mockedPinnedMainDmOwner: undefined,
  capturedReplyOptions: undefined,
  capturedStatusReactionOptions: undefined,
  mockedReplyThreadTs: THREAD_TS,
  mockedStatusThreadTs: THREAD_TS,
  mockedReplyThreadTsSequence: undefined,
  mockedSlackReplyBlocks: undefined,
  mockedSlackIsThreadReply: true,
  capturedTyping: undefined,
  mockedDispatchSequence: [],
  mockedQueuedDispatchCounts: { tool: 0, block: 0, final: 0 },
  mockedAgentRunTerminalOutcome: undefined,
  mockedSourceReplyDelivered: false,
  mockedDispatchError: undefined,
  useRealChannelInboundTurn: false,
  mockedProgressEvents: [],
  mockedReplyOptionEvents: [],
};

vi.mock("openclaw/plugin-sdk/agent-runtime", () => ({
  resolveHumanDelayConfig: () => undefined,
}));

vi.mock("openclaw/plugin-sdk/channel-feedback", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/channel-feedback")>()),
  createStatusReactionController: (params: { enabled?: boolean; initialEmoji?: string }) => {
    harness.capturedStatusReactionOptions = params;
    return statusReactionControllerMock;
  },
  logAckFailure: () => {},
  logTypingFailure: () => {},
}));

vi.mock("openclaw/plugin-sdk/channel-outbound", async (importOriginal) => {
  const actual = await importOriginal<typeof import("openclaw/plugin-sdk/channel-outbound")>();
  return {
    ...actual,
    createChannelProgressDraftCompositor: (
      params: Parameters<typeof actual.createChannelProgressDraftCompositor>[0],
    ) =>
      actual.createChannelProgressDraftCompositor({
        ...params,
        // Gate timing lives in the compositor suite; dispatch tests exercise
        // Slack rendering and delivery after work admits the draft.
        setTimeoutFn: ((handler: () => void) => {
          handler();
          return 0 as never;
        }) as unknown as typeof setTimeout,
        clearTimeoutFn: (() => {}) as typeof clearTimeout,
      }),
    createChannelMessageReplyPipeline: (params: {
      transformReplyPayload?: (payload: TestReplyPayload) => TestReplyPayload | null;
      typing?: {
        start: () => Promise<void>;
        stop?: () => Promise<void>;
        onStartError: (err: unknown) => void;
        onStopError?: (err: unknown) => void;
      };
    }) => {
      harness.capturedTyping = params.typing;
      return {
        ...(params.typing
          ? {
              typingCallbacks: {
                onReplyStart: params.typing.start,
                onIdle: () => {
                  void params.typing?.stop?.();
                },
              },
            }
          : {}),
        ...(params.transformReplyPayload
          ? { transformReplyPayload: params.transformReplyPayload }
          : {}),
        onModelSelected: undefined,
      };
    },
    resolveChannelMessageSourceReplyDeliveryMode:
      actual.resolveChannelMessageSourceReplyDeliveryMode,
    resolveAgentOutboundIdentity: () => undefined,
    buildChannelProgressDraftLine: ({ explanation }: { explanation?: string }) =>
      explanation
        ? {
            kind: "plan",
            text: `🗺️ Update Plan: ${explanation}`,
            label: "Update Plan",
            detail: explanation,
            toolName: "update_plan",
          }
        : undefined,
    resolveChannelProgressDraftMaxLineChars: (entry?: {
      streaming?: { progress?: { maxLineChars?: number } };
    }) => entry?.streaming?.progress?.maxLineChars,
    resolveChannelStreamingBlockEnabled: () => harness.mockedBlockStreamingEnabled,
  };
});

vi.mock("openclaw/plugin-sdk/reply-payload", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/reply-payload")>()),
  resolveAskUserQuestionOptionIndices: () => undefined,
  isReplyPayloadNonTerminalToolErrorWarning: () => false,
}));

vi.mock("openclaw/plugin-sdk/runtime-env", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/runtime-env")>()),
  danger: (message: string) => message,
  logVerbose: logVerboseMock,
  shouldLogVerbose: () => false,
}));

vi.mock("openclaw/plugin-sdk/plugin-runtime", async (importOriginal) => {
  const actual = await importOriginal<typeof import("openclaw/plugin-sdk/plugin-runtime")>();
  return { ...actual, getGlobalHookRunner: hoisted.getGlobalHookRunnerMock };
});

vi.mock("openclaw/plugin-sdk/security-runtime", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/security-runtime")>()),
  resolvePinnedMainDmOwnerFromAllowlist: () => harness.mockedPinnedMainDmOwner,
}));

vi.mock("../../actions.js", () => ({
  reactSlackMessage: reactSlackMessageMock,
  removeSlackReaction: removeSlackReactionMock,
}));

vi.mock("../../draft-stream.js", () => ({
  createSlackDraftStream: createSlackDraftStreamMock,
}));

vi.mock("../../format.js", () => ({
  markdownToSlackMrkdwnChunks: (value: string) => [value],
  normalizeSlackOutboundText: normalizeSlackOutboundTextMock,
}));

vi.mock("../../limits.js", () => ({
  SLACK_TEXT_LIMIT: 4000,
  SLACK_EDIT_TEXT_MAX_BYTES: 4000,
}));

vi.mock("../../sent-thread-cache.js", () => ({
  clearSlackThreadFailureNotice: () => {},
  hasSlackThreadParticipation: () => false,
  recordSlackThreadFailureNotice: () => true,
  recordSlackThreadParticipation: recordSlackThreadParticipationMock,
}));

vi.mock("../../stream-mode.js", () => ({
  applyAppendOnlyStreamUpdate: ({ incoming }: { incoming: string }) => ({
    changed: true,
    rendered: incoming,
    source: incoming,
  }),
  resolveSlackStreamingConfig: () => ({
    mode: harness.mockedSlackStreamingMode,
    nativeStreaming: harness.mockedNativeStreaming,
  }),
}));

vi.mock("../../streaming.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../streaming.js")>()),
  discardSlackStreamPendingText: (session: { pendingText: string }) => {
    session.pendingText = "";
  },
  markSlackStreamFallbackDelivered: (session: {
    delivered: boolean;
    pendingText: string;
    stopped: boolean;
  }) => {
    session.pendingText = "";
    session.stopped = !session.delivered;
  },
  SlackStreamMessageTooLongError: TestSlackStreamMessageTooLongError,
  SlackStreamNotDeliveredError: TestSlackStreamNotDeliveredError,
  appendSlackStream: async (input: { session?: unknown }) => {
    const result = await appendSlackStreamMock(input);
    chargeStreamBudget(input.session, "append", input);
    return result;
  },
  startSlackStream: async (input: unknown) => {
    const session = Object.assign(await startSlackStreamMock(input), {
      streamer: { ts: STREAM_MESSAGE_TS },
    });
    chargeStreamBudget(session, "start", input);
    return session;
  },
  stopSlackStream: async (params: { session: { stopped: boolean } }) => {
    params.session.stopped = true;
    const result = await stopSlackStreamMock(params);
    chargeStreamBudget(params.session, "stop", params);
    return result;
  },
}));

vi.mock("../../message-sent-hook.js", () => ({
  emitSlackMessageSentHooks: emitSlackMessageSentHooksMock,
}));

vi.mock("../../threading.js", () => ({
  resolveSlackThreadContext: () => ({
    messageThreadId: harness.mockedStatusThreadTs,
    isThreadReply: harness.mockedSlackIsThreadReply,
  }),
}));

vi.mock("../allow-list.js", () => ({
  normalizeSlackAllowOwnerEntry: (value: string) => value,
}));

vi.mock("openclaw/plugin-sdk/session-store-runtime", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/session-store-runtime")>()),
  resolveStorePath: () => "/tmp/openclaw-store.json",
  updateLastRoute: hoisted.updateLastRouteMock,
}));

vi.mock("../../reply-blocks.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../reply-blocks.js")>()),
  resolveSlackReplyBlocks: () => harness.mockedSlackReplyBlocks,
}));

vi.mock("../replies.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../replies.js")>()),
  createSlackReplyDeliveryPlan: () => ({
    peekThreadTs: () =>
      harness.mockedReplyThreadTsSequence
        ? harness.mockedReplyThreadTsSequence[0]
        : harness.mockedReplyThreadTs,
    nextThreadTs: () =>
      harness.mockedReplyThreadTsSequence
        ? harness.mockedReplyThreadTsSequence.shift()
        : harness.mockedReplyThreadTs,
    markSent: () => {},
  }),
  deliverReplies: (params: Parameters<typeof import("../replies.js").deliverReplies>[0]) =>
    deliverRepliesMock({ ...params, replies: params.replies.map((prepared) => prepared.payload) }),
}));

vi.mock("../../send.js", () => ({ sendMessageSlack: sendMessageSlackMock }));

vi.mock("openclaw/plugin-sdk/channel-inbound", async (importOriginal) => {
  const actual = await importOriginal<typeof import("openclaw/plugin-sdk/channel-inbound")>();
  type DispatchParams = Parameters<typeof actual.dispatchChannelInboundTurn>[0];
  return {
    ...actual,
    readAgentRunTerminalOutcome: () => harness.mockedAgentRunTerminalOutcome,
    dispatchChannelInboundTurn: async (params: DispatchParams) => {
      if (harness.useRealChannelInboundTurn) {
        return actual.dispatchChannelInboundTurn(params);
      }
      harness.capturedReplyOptions = params.replyOptions as typeof harness.capturedReplyOptions;
      if (harness.mockedReplyOptionEvents.length > 0) {
        for (const [index, entry] of harness.mockedReplyOptionEvents.entries()) {
          if (entry.kind === "item") {
            const { kind: _kind, itemKind, ...payload } = entry;
            await params.replyOptions?.onItemEvent?.({ ...payload, kind: itemKind });
          } else if (entry.kind === "command_output") {
            const { kind: _kind, explanation: _explanation, ...payload } = entry;
            await params.replyOptions?.onCommandOutput?.(payload);
            if (entry.phase === "end") {
              const item = projectAgentToolActivity({
                toolCallId: entry.toolCallId ?? entry.itemId ?? `tool-${index}`,
                name: entry.name ?? "exec",
                phase: "result",
                isError: entry.exitCode == null ? undefined : entry.exitCode !== 0,
                meta: entry.title,
              });
              await params.replyOptions?.onItemEvent?.({
                ...item,
                itemId: entry.itemId ?? item.itemId,
              });
            }
          } else if (entry.kind === "tool_start") {
            const { kind: _kind, ...payload } = entry;
            await params.replyOptions?.onToolStart?.(payload);
            const item = projectAgentToolActivity({
              toolCallId: entry.toolCallId ?? entry.itemId ?? `tool-${index}`,
              name: entry.name,
              phase: entry.phase === "update" ? "update" : "start",
              args: entry.args,
            });
            await params.replyOptions?.onItemEvent?.({
              ...item,
              itemId: entry.itemId ?? item.itemId,
            });
          } else if (entry.kind === "patch") {
            const { kind: _kind, ...payload } = entry;
            await params.replyOptions?.onPatchSummary?.(payload);
            if (entry.phase === "end") {
              await params.replyOptions?.onItemEvent?.({
                itemId: entry.itemId,
                toolCallId: entry.toolCallId,
                kind: "patch",
                phase: "end",
                status: "completed",
                title: entry.title ?? "Apply Patch",
                name: entry.name,
                meta: entry.summary,
              });
            }
          } else if (entry.kind === "plan") {
            const { kind: _kind, ...payload } = entry;
            await params.replyOptions?.onPlanUpdate?.(payload);
          } else if (entry.kind === "concurrent_items") {
            await Promise.all(
              entry.progressTexts.map((progressText) =>
                Promise.resolve(params.replyOptions?.onItemEvent?.({ progressText })),
              ),
            );
          } else if (entry.kind === "assistant_start") {
            await params.replyOptions?.onAssistantMessageStart?.();
          } else if (entry.kind === "reasoning") {
            const { kind: _kind, ...payload } = entry;
            await params.replyOptions?.onReasoningStream?.(payload);
          } else if (entry.kind === "reasoning_end") {
            await params.replyOptions?.onReasoningEnd?.();
          } else if (entry.kind === "checkpoint") {
            await entry.run();
          } else if (entry.kind === "approval") {
            const { kind: _kind, ...payload } = entry;
            await params.replyOptions?.onApprovalEvent?.(payload);
          } else {
            await params.replyOptions?.onPartialReply?.({ text: entry.text });
          }
        }
      } else {
        for (const progressText of harness.mockedProgressEvents) {
          await params.replyOptions?.onItemEvent?.({ progressText });
        }
      }
      if (harness.mockedDispatchError) {
        throw harness.mockedDispatchError;
      }
      for (const entry of harness.mockedDispatchSequence) {
        if (entry.kind === "queued_followup") {
          await params.replyOptions?.onQueuedFollowupAdmitted?.();
          continue;
        }
        if (entry.kind === "item") {
          await params.replyOptions?.onItemEvent?.({ progressText: entry.progressText });
          continue;
        }
        const payload = entry.payload as ReplyPayload;
        const transformed = params.dispatcherOptions?.transformReplyPayload
          ? params.dispatcherOptions.transformReplyPayload(payload)
          : payload;
        if (!transformed) {
          continue;
        }
        const deliverPayload = params.dispatcherOptions?.beforeDeliver
          ? await params.dispatcherOptions.beforeDeliver(transformed, { kind: entry.kind })
          : transformed;
        if (!deliverPayload) {
          continue;
        }
        harness.mockedQueuedDispatchCounts[entry.kind] += 1;
        const dispatcher = createReplyDispatcher({
          deliver: params.delivery.deliver,
          onError: params.delivery.onError,
        });
        if (entry.kind === "tool") {
          dispatcher.sendToolResult(deliverPayload);
        } else if (entry.kind === "block") {
          dispatcher.sendBlockReply(deliverPayload);
        } else {
          dispatcher.sendFinalReply(deliverPayload);
        }
        dispatcher.markComplete();
        await dispatcher.waitForIdle();
      }
      return {
        admission: { kind: "dispatch" } as const,
        dispatched: true as const,
        ctxPayload: params.ctxPayload,
        routeSessionKey: params.route.sessionKey,
        dispatchResult: {
          queuedFinal: false,
          counts: { ...harness.mockedQueuedDispatchCounts },
          observedReplyDelivery: harness.mockedSourceReplyDelivered,
        },
      };
    },
  };
});

vi.mock("./preview-finalize.js", () => ({
  finalizeSlackPreviewEdit: finalizeSlackPreviewEditMock,
}));

/** Clears every mock and arranges the default per-test state. */
export function resetHarnessMocks() {
  createSlackDraftStreamMock.mockReset();
  deliverRepliesMock.mockReset();
  sendMessageSlackMock.mockReset();
  harness.useRealChannelInboundTurn = false;
  finalizeSlackPreviewEditMock.mockReset();
  normalizeSlackOutboundTextMock.mockClear();
  postMessageMock.mockClear();
  chatUpdateMock.mockClear();
  recordSlackThreadParticipationMock.mockReset();
  updateLastRouteMock.mockReset();
  appendSlackStreamMock.mockReset();
  startSlackStreamMock.mockReset();
  stopSlackStreamMock.mockReset();
  reactSlackMessageMock.mockReset();
  removeSlackReactionMock.mockReset();
  logVerboseMock.mockReset();
  getGlobalHookRunnerMock.mockReset().mockReturnValue(undefined);
  for (const value of Object.values(statusReactionControllerMock)) {
    value.mockClear();
  }
  harness.mockedNativeStreaming = false;
  harness.mockedBlockStreamingEnabled = false;
  harness.mockedSlackStreamingMode = "partial";
  harness.mockedPinnedMainDmOwner = undefined;
  harness.capturedReplyOptions = undefined;
  harness.capturedStatusReactionOptions = undefined;
  harness.capturedTyping = undefined;
  harness.mockedReplyThreadTs = THREAD_TS;
  harness.mockedStatusThreadTs = THREAD_TS;
  harness.mockedReplyThreadTsSequence = undefined;
  harness.mockedSlackReplyBlocks = undefined;
  harness.mockedSlackIsThreadReply = true;
  harness.mockedDispatchSequence = [{ kind: "final", payload: { text: FINAL_REPLY_TEXT } }];
  harness.mockedQueuedDispatchCounts = { tool: 0, block: 0, final: 0 };
  harness.mockedAgentRunTerminalOutcome = undefined;
  harness.mockedSourceReplyDelivered = false;
  harness.mockedDispatchError = undefined;
  harness.mockedProgressEvents = [];
  harness.mockedReplyOptionEvents = [];
  emitSlackMessageSentHooksMock.mockClear();
}
