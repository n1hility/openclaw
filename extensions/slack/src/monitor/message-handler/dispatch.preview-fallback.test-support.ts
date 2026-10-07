// Scenario drivers, assertions and lifecycle hooks for the dispatchPreparedSlackMessage
// test files; the mocks they drive live in dispatch.preview-fallback.test-mocks.ts.
import { afterEach, beforeAll, beforeEach, expect, vi } from "vitest";
import {
  THREAD_TS,
  appendSlackStreamMock,
  chatUpdateMock,
  createRequireRecord,
  createSlackDraftStreamMock,
  createTestRegistry,
  deliverRepliesMock,
  finalizeSlackPreviewEditMock,
  harness,
  postMessageMock,
  resetHarnessMocks,
  resetPluginRuntimeStateForTest,
  setActivePluginRegistry,
  slackSetupPlugin,
  startSlackStreamMock,
  stopSlackStreamMock,
  streamBudgetGuard,
  type PreparedSlackMessage,
  type SlackReplyOptionEvent,
  type TestDispatchSequenceEntry,
  type TestReplyPayload,
} from "./dispatch.preview-fallback.test-mocks.js";

export function requireCapturedTyping() {
  if (!harness.capturedTyping) {
    throw new Error("expected Slack typing callback");
  }
  return harness.capturedTyping;
}

export function createSlackPlatformError(
  error: string,
  details?: { needed?: string; provided?: string },
) {
  // Mirrors @slack/web-api 7.18.0 platformErrorFromResult: message plus structured result data.
  return Object.assign(new Error(`An API error occurred: ${error}`), {
    code: "slack_webapi_platform_error",
    data: { ok: false, error, ...details },
  });
}

export function requireCapturedItemEventHandler() {
  const handler = harness.capturedReplyOptions?.onItemEvent;
  if (!handler) {
    throw new Error("expected Slack reply item event handler");
  }
  return handler;
}

export const requireRecord = createRequireRecord("object", "label-not-object");

export function expectRecordFields(
  record: Record<string, unknown>,
  fields: Record<string, unknown>,
) {
  for (const [key, value] of Object.entries(fields)) {
    expect(record[key]).toEqual(value);
  }
}

export function requireMockCall(mock: unknown, index: number, label: string): unknown[] {
  const call = (mock as { mock?: { calls?: unknown[][] } }).mock?.calls?.[index];
  if (!call) {
    throw new Error(`missing ${label} call ${index + 1}`);
  }
  return call;
}

export function expectMockCallArgFields(
  mock: unknown,
  index: number,
  fields: Record<string, unknown>,
) {
  expectRecordFields(requireRecord(requireMockCall(mock, index, "call")[0], "params"), fields);
}

export function expectNativeProgressStart(chunks: unknown[]) {
  expect(postMessageMock).not.toHaveBeenCalled();
  expect(chatUpdateMock).not.toHaveBeenCalled();
  expectMockCallArgFields(startSlackStreamMock, 0, {
    channel: "C123",
    threadTs: THREAD_TS,
    taskDisplayMode: "plan",
    chunks,
  });
}

export function expectNativeProgressAppend(index: number, chunks: unknown[]) {
  expectMockCallArgFields(appendSlackStreamMock, index, {
    chunks,
  });
}

export function expectNativeStreamText(text: string, count = 1) {
  const matches = [...startSlackStreamMock.mock.calls, ...appendSlackStreamMock.mock.calls].filter(
    (call) => {
      const params = requireRecord(call[0], "native stream text append");
      return params.text === text;
    },
  );
  expect(matches).toHaveLength(count);
}

export function planUpdate(title: string) {
  return { type: "plan_update", title };
}

export function taskUpdate(
  id: unknown,
  title: string,
  status: "pending" | "in_progress" | "complete" | "error",
  extra?: Record<string, unknown>,
) {
  return { type: "task_update", id, title, status, ...extra };
}

export function contentTaskId(prefix: string) {
  return expect.stringMatching(new RegExp(`^${prefix}_[a-f0-9]{8}_1$`, "u"));
}

export function collectNativeTaskUpdates() {
  return [
    ...startSlackStreamMock.mock.calls,
    ...appendSlackStreamMock.mock.calls,
    ...stopSlackStreamMock.mock.calls,
  ]
    .flatMap(([value]) => {
      const arg = requireRecord(value, "native progress call");
      return Array.isArray(arg.chunks) ? arg.chunks : [];
    })
    .flatMap((chunk) => {
      const record = requireRecord(chunk, "native progress chunk");
      return record.type === "task_update" ? [record] : [];
    });
}

export function expectDeliverReplyCall(
  index: number,
  text: string,
  fields?: Record<string, unknown>,
) {
  const params = requireRecord(
    requireMockCall(deliverRepliesMock, index, "deliver replies")[0],
    "deliver replies params",
  );
  expectRecordFields(params, { replyThreadTs: THREAD_TS, ...fields });
  expect(params.replies).toEqual([{ text }]);
}

export function progressAccount(
  progress: Record<string, unknown> = { toolProgress: true, label: "Working" },
) {
  harness.mockedSlackStreamingMode = "progress";
  return { streaming: { mode: "progress", progress } };
}

export function preamble(
  progressText: string,
  itemId: string,
  phase?: string,
): SlackReplyOptionEvent {
  return { kind: "item", itemKind: "preamble", progressText, itemId, phase };
}

export function checkpoint(run: () => Promise<void>): SlackReplyOptionEvent {
  return { kind: "checkpoint", run };
}

export function delivered(index = 0) {
  return deliverRepliesMock.mock.calls[index]![0];
}

export function ttsPayload(spokenText = "Spoken answer", visibleTextAlreadyDelivered?: boolean) {
  return {
    mediaUrl: "https://example.com/tts.mp3",
    audioAsVoice: true,
    spokenText,
    ttsSupplement: {
      spokenText,
      ...(visibleTextAlreadyDelivered ? { visibleTextAlreadyDelivered: true } : {}),
    },
  };
}

export const noop = () => {};
export const noopAsync = async () => {};
export function createNativeStreamSession() {
  return {
    channel: "C123",
    threadTs: THREAD_TS,
    stopped: false,
    delivered: true,
    pendingText: "",
  };
}

export function createDraftStreamStub() {
  return {
    update: vi.fn(),
    flush: vi.fn(noopAsync),
    clear: vi.fn(noopAsync),
    discardPending: vi.fn(noopAsync),
    seal: vi.fn(noopAsync),
    stop: vi.fn(noop),
    forceNewMessage: vi.fn(),
    dropDetachedMessages: vi.fn(noopAsync),
    finalizeMessage: vi.fn(async (_messageId: string, editFinal: () => Promise<void>) => {
      await editFinal();
      return true;
    }),
    messageId: (): string | undefined => "171234.567",
    channelId: () => "C123",
  };
}

export function useDraftStream() {
  const draftStream = createDraftStreamStub();
  createSlackDraftStreamMock.mockReturnValueOnce(draftStream);
  return draftStream;
}

export function draftUpdateTexts(draftStream: ReturnType<typeof createDraftStreamStub>): string[] {
  return draftStream.update.mock.calls.map(([update]) => {
    if (typeof update === "string") {
      return update;
    }
    return requireRecord(update, "draft update").text as string;
  });
}

export function expectLastDraftUpdateText(
  draftStream: ReturnType<typeof createDraftStreamStub>,
  expected: string,
) {
  expect(draftUpdateTexts(draftStream).at(-1)).toBe(expected);
}

export function createPreparedSlackMessage(params?: {
  cfg?: Record<string, unknown>;
  accountConfig?: Record<string, unknown>;
  ctxPayload?: Record<string, unknown>;
  message?: Partial<PreparedSlackMessage["message"]>;
  replyToMode?: "off" | "first" | "all" | "batched";
  isDirectMessage?: boolean;
  route?: Partial<PreparedSlackMessage["route"]>;
  setSlackSessionStatus?: PreparedSlackMessage["ctx"]["setSlackSessionStatus"];
  typingReaction?: string;
  ackReactionMessageTs?: string;
  ackReactionPromise?: Promise<boolean> | null;
  relayIdentity?: { username?: string; iconUrl?: string; iconEmoji?: string };
  turnAdoptionLifecycle?: object;
  dispatchReplyFromConfig?: unknown;
  eventScope?: {
    teamId: string;
    client: Record<string, unknown>;
  };
}) {
  const routeSessionKey = params?.route?.sessionKey ?? "agent:agent-1:slack:C123";
  const mainSessionKey = params?.route?.mainSessionKey ?? "main";
  const lastRoutePolicy =
    params?.route?.lastRoutePolicy ?? (routeSessionKey === mainSessionKey ? "main" : "session");
  const message = {
    channel: "C123",
    ts: "171234.111",
    thread_ts: THREAD_TS,
    user: "U123",
    ...params?.message,
  };

  return {
    ctx: {
      cfg: params?.cfg ?? {},
      runtime: {},
      botToken: "xoxb-test",
      app: { client: { chat: { postMessage: postMessageMock, update: chatUpdateMock } } },
      teamId: "T1",
      botUserId: "U_OPENCLAW",
      botId: "B_OPENCLAW",
      textLimit: 4000,
      typingReaction: params?.typingReaction ?? "",
      historyLimit: 0,
      allowFrom: [],
      dispatchReplyFromConfig: params?.dispatchReplyFromConfig,
      setSlackSessionStatus: params?.setSlackSessionStatus ?? (async () => true),
    },
    account: {
      accountId: "default",
      config: params?.accountConfig ?? {},
    },
    relayIdentity: params?.relayIdentity,
    turnAdoptionLifecycle: params?.turnAdoptionLifecycle,
    eventScope: params?.eventScope,
    message,
    route: {
      agentId: "agent-1",
      accountId: "default",
      mainSessionKey,
      sessionKey: routeSessionKey,
      lastRoutePolicy,
      ...params?.route,
    },
    channelConfig: null,
    replyTarget: `channel:${message.channel}`,
    ctxPayload: {
      MessageThreadId: THREAD_TS,
      ...params?.ctxPayload,
    },
    turn: {
      record: {},
    },
    replyToMode: params?.replyToMode ?? "all",
    isDirectMessage: params?.isDirectMessage ?? false,
    isRoomish: false,
    ackReactionValue: "eyes",
    ackReactionMessageTs: params?.ackReactionMessageTs,
    ackReactionPromise: params?.ackReactionPromise ?? null,
  } as never;
}

export async function dispatch(params?: Parameters<typeof createPreparedSlackMessage>[0]) {
  await dispatchPreparedSlackMessage(createPreparedSlackMessage(params));
}

export async function dispatchNativeProgressScenario(params: {
  events: typeof harness.mockedReplyOptionEvents;
  finalPayload?: TestReplyPayload;
  /** Reply payloads dispatched after the events and before the final. */
  replies?: TestDispatchSequenceEntry[];
  progress?: {
    style?: "card" | "compact";
    label?: string | false;
    maxLineChars?: number;
    nativeTaskCards?: true;
    render?: "rich";
    toolProgress?: boolean;
    commandText?: "raw" | "status";
    reasoning?: "narration" | "cards";
    commentary?: boolean;
  };
  replyToMode?: "off" | "first" | "all" | "batched";
  eventScope?: {
    teamId: string;
    client: Record<string, unknown>;
  };
}) {
  harness.mockedNativeStreaming = true;
  harness.mockedSlackStreamingMode = "progress";
  harness.mockedDispatchSequence = [
    ...(params.replies ?? []),
    ...(params.finalPayload === undefined
      ? []
      : [{ kind: "final" as const, payload: params.finalPayload }]),
  ];
  harness.mockedReplyOptionEvents = params.events;

  await dispatch({
    replyToMode: params.replyToMode,
    eventScope: params.eventScope,
    accountConfig: progressAccount({
      toolProgress: true,
      ...(params.progress ?? { nativeTaskCards: true }),
    }),
  });
}

export type NativeStreamCall = {
  kind: "start" | "append" | "stop";
  params: Record<string, unknown>;
};

// Every native stream call in the order it was made, so a chain of stream
// messages can be split at each start.
export function collectNativeStreamTimeline(): NativeStreamCall[] {
  const entries: Array<NativeStreamCall & { order: number }> = [];
  const collect = (
    kind: NativeStreamCall["kind"],
    mock: { mock: { calls: unknown[][]; invocationCallOrder: number[] } },
  ) => {
    mock.mock.calls.forEach((call, index) => {
      entries.push({
        kind,
        params: requireRecord(call[0], `${kind} call`),
        order: mock.mock.invocationCallOrder[index] ?? 0,
      });
    });
  };
  collect("start", startSlackStreamMock);
  collect("append", appendSlackStreamMock);
  collect("stop", stopSlackStreamMock);
  return entries
    .toSorted((left, right) => left.order - right.order)
    .map(({ kind, params }) => ({ kind, params }));
}

export function chunksOf(params: Record<string, unknown>): Array<Record<string, unknown>> {
  return Array.isArray(params.chunks)
    ? params.chunks.map((chunk) => requireRecord(chunk, "chunk"))
    : [];
}

export function splitNativeStreamMessages(timeline: NativeStreamCall[]): NativeStreamCall[][] {
  const messages: NativeStreamCall[][] = [];
  for (const call of timeline) {
    if (call.kind === "start" || messages.length === 0) {
      messages.push([]);
    }
    messages.at(-1)?.push(call);
  }
  return messages;
}

export function reasoningIdsOf(message: NativeStreamCall[]): number[] {
  const ids = new Set<number>();
  for (const call of message) {
    for (const chunk of chunksOf(call.params)) {
      const match = /^reasoning_(\d+)_[a-f0-9]{8}$/u.exec(String(chunk.id));
      if (match) {
        ids.add(Number(match[1]));
      }
    }
  }
  return [...ids].toSorted((left, right) => left - right);
}

export function planTitlesOf(message: NativeStreamCall[]): string[] {
  return message.flatMap((call) =>
    chunksOf(call.params)
      .filter((chunk) => chunk.type === "plan_update")
      .map((chunk) => String(chunk.title)),
  );
}

export function sleepRealMs(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

// The shared session from beforeEach is stopped by the first rollover;
// real starts return a new session per message.
export function useFreshStreamSessions() {
  startSlackStreamMock.mockImplementation(async () => ({
    channel: "C123",
    threadTs: THREAD_TS,
    stopped: false,
    delivered: true,
    pendingText: "",
  }));
}

export let dispatchPreparedSlackMessage: typeof import("./dispatch.js").dispatchPreparedSlackMessage;

/** Registers the harness lifecycle in the calling describe block. */
export function installPreviewFallbackHarness() {
  beforeAll(async () => {
    ({ dispatchPreparedSlackMessage } = await import("./dispatch.js"));
  });

  beforeEach(() => {
    setActivePluginRegistry(
      createTestRegistry([{ pluginId: "slack", source: "test", plugin: slackSetupPlugin }]),
    );
    resetHarnessMocks();
    createSlackDraftStreamMock.mockReturnValue(createDraftStreamStub());
    finalizeSlackPreviewEditMock.mockRejectedValue(new Error("socket closed"));
    startSlackStreamMock.mockResolvedValue(createNativeStreamSession());
    appendSlackStreamMock.mockResolvedValue(undefined);
    stopSlackStreamMock.mockResolvedValue({});
  });

  afterEach(() => {
    const violations = streamBudgetGuard.violations;
    streamBudgetGuard.violations = [];
    streamBudgetGuard.enabled = true;
    resetPluginRuntimeStateForTest();
    expect(violations).toEqual([]);
  });
}
