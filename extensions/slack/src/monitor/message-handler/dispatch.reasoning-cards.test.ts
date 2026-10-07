// Native progress with progress.reasoning: "cards": reasoning rows, their order
// against tool rows, the finished title, and where the answer lands.
import { describe, expect, it, vi } from "vitest";
import {
  FINAL_REPLY_TEXT,
  THREAD_TS,
  TestSlackStreamMessageTooLongError,
  appendSlackStreamMock,
  deliverRepliesMock,
  harness,
  startSlackStreamMock,
  stopSlackStreamMock,
} from "./dispatch.preview-fallback.test-mocks.js";
import {
  chunksOf,
  collectNativeStreamTimeline,
  collectNativeTaskUpdates,
  dispatchNativeProgressScenario,
  expectDeliverReplyCall,
  expectNativeProgressAppend,
  expectNativeProgressStart,
  expectNativeStreamText,
  installPreviewFallbackHarness,
  planTitlesOf,
  planUpdate,
  reasoningIdsOf,
  requireMockCall,
  requireRecord,
  sleepRealMs,
  splitNativeStreamMessages,
  taskUpdate,
  useFreshStreamSessions,
} from "./dispatch.preview-fallback.test-support.js";

describe("dispatchPreparedSlackMessage reasoning cards", () => {
  installPreviewFallbackHarness();

  it("keeps the narration presentation unchanged when progress.reasoning is narration", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "narration" },
      events: [
        { kind: "reasoning", text: "Checking", isReasoningSnapshot: true },
        {
          kind: "reasoning",
          text: "Checking the Slack handler",
          isReasoningSnapshot: true,
        },
      ],
    });

    expect(collectNativeTaskUpdates()).toEqual([]);
    expectNativeStreamText("Checking");
    expectNativeProgressAppend(0, [{ type: "markdown_text", text: " the Slack handler" }]);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
    expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
    expect(appendSlackStreamMock).toHaveBeenCalledTimes(2);
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
  });

  it("renders streamed reasoning as segment task cards instead of narration", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "Checking", isReasoningSnapshot: true },
        { kind: "reasoning", text: "Checking the Slack handler", isReasoningSnapshot: true },
        { kind: "reasoning_end" },
      ],
    });

    const reasoningCardId = expect.stringMatching(/^reasoning_1_[a-f0-9]{8}$/u);
    expectNativeProgressStart([
      planUpdate("Thinking"),
      taskUpdate(reasoningCardId, "🧠 Checking", "in_progress"),
    ]);
    // The second snapshot arrives inside the throttle window and rides the
    // completion append, sealed by the end of the reasoning phase.
    expectNativeProgressAppend(0, [
      planUpdate(expect.stringMatching(/^Thought for \d+s$/u)),
      taskUpdate(reasoningCardId, "🧠 Checking the Slack handler", "complete"),
    ]);
    expectNativeStreamText("Checking", 0);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
    // A short think is one stream message: no rollover calls.
    expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
    expect(appendSlackStreamMock).toHaveBeenCalledTimes(2);
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
    const streamTexts = [...startSlackStreamMock.mock.calls, ...appendSlackStreamMock.mock.calls]
      .map((call) => requireRecord(call[0], "native stream call").text)
      .filter((text): text is string => typeof text === "string");
    expect(streamTexts.join("")).not.toContain("Checking");
  });

  it("merges reasoning deltas, prefix extensions, and flagged snapshots into one card", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "Reading" },
        { kind: "reasoning", text: " the handler" },
        { kind: "reasoning", text: "Reading the handler now" },
        { kind: "reasoning", text: "<think>Fresh snapshot</think>", isReasoningSnapshot: true },
      ],
    });

    const reasoningCardId = expect.stringMatching(/^reasoning_1_[a-f0-9]{8}$/u);
    expect(collectNativeTaskUpdates()).toEqual([
      taskUpdate(reasoningCardId, "🧠 Reading", "in_progress"),
      taskUpdate(reasoningCardId, "🧠 Fresh snapshot", "complete"),
    ]);
  });

  it.each([
    { split: "a trailing space", deltas: ["Reading ", "the handler"], text: "Reading the handler" },
    {
      split: "a wrapper tag closed by a later delta",
      deltas: ["<think>Fresh", " snapshot</think>"],
      text: "Fresh snapshot",
    },
  ])("keeps the raw reasoning across deltas split at $split", async ({ deltas, text }) => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        ...deltas.map((delta) => ({ kind: "reasoning" as const, text: delta })),
        { kind: "reasoning_end" },
      ],
    });

    const tasks = collectNativeTaskUpdates();
    expect(new Set(tasks.map((task) => task.id)).size).toBe(1);
    expect(tasks.at(-1)).toEqual(
      taskUpdate(expect.stringMatching(/^reasoning_1_[a-f0-9]{8}$/u), `🧠 ${text}`, "complete"),
    );
  });

  it("seals the open reasoning card at a tool boundary so later thinking starts a new card", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "Plan the fix." },
        {
          kind: "tool_start",
          itemId: "tool-1",
          name: "bash",
          phase: "start",
          args: { command: "pnpm test" },
        },
        { kind: "reasoning", text: "Tests pass, now summarize." },
        { kind: "reasoning_end" },
      ],
    });

    const tasks = collectNativeTaskUpdates();
    expect(tasks).toEqual([
      taskUpdate(
        expect.stringMatching(/^reasoning_1_[a-f0-9]{8}$/u),
        "🧠 Plan the fix.",
        "in_progress",
      ),
      taskUpdate(
        expect.stringMatching(/^reasoning_1_[a-f0-9]{8}$/u),
        "🧠 Plan the fix.",
        "complete",
      ),
      taskUpdate(expect.stringMatching(/^tool_1_[a-f0-9]{8}$/u), "Bash", "complete"),
      taskUpdate(
        expect.stringMatching(/^reasoning_2_[a-f0-9]{8}$/u),
        "🧠 Tests pass, now summarize.",
        "complete",
      ),
    ]);
    const completion = requireRecord(
      requireMockCall(appendSlackStreamMock, 0, "completion append")[0],
      "completion append",
    );
    expect((completion.chunks as unknown[])[0]).toEqual(
      planUpdate(expect.stringMatching(/^Thought for \d+s, 1 tool call$/u)),
    );
  });

  it("keeps an explicit progress title through reasoning card completion", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards", label: "Shelling" },
      events: [{ kind: "reasoning", text: "Checking the handler" }, { kind: "reasoning_end" }],
    });

    const plans = [...startSlackStreamMock.mock.calls, ...appendSlackStreamMock.mock.calls]
      .flatMap((call) => {
        const chunks = requireRecord(call[0], "native stream call").chunks;
        return Array.isArray(chunks) ? chunks : [];
      })
      .filter((chunk) => requireRecord(chunk, "chunk").type === "plan_update");
    expect(plans).toEqual([planUpdate("Shelling")]);
  });

  it("keeps reasoning in narration on the quiet card even when cards are configured", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { style: "card", toolProgress: false, nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "Checking", isReasoningSnapshot: true },
        { kind: "reasoning", text: "Checking the Slack handler", isReasoningSnapshot: true },
      ],
    });

    expect(
      collectNativeTaskUpdates().filter(
        (task) => typeof task.id === "string" && task.id.startsWith("reasoning_"),
      ),
    ).toEqual([]);
    expectNativeStreamText("Checking");
  });

  it("delivers every card of a burst wider than a rolling window, once and in order", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const segments = Array.from({ length: 70 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        // One cumulative snapshot admits 70 cards before the paced loop sends again.
        { kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const timeline = collectNativeStreamTimeline();
    const messages = splitNativeStreamMessages(timeline);
    const perMessageIds = messages.map((message) => reasoningIdsOf(message));
    // No segment is lost to the compositor before its first send, none is repeated.
    expect(perMessageIds.flat()).toEqual(Array.from({ length: 70 }, (_, index) => index + 1));
    expect(messages).toHaveLength(3);
    for (const ids of perMessageIds) {
      expect(ids.length).toBeLessThanOrEqual(24);
    }
    for (const call of timeline) {
      for (const chunk of chunksOf(call.params)) {
        const match = /^reasoning_(\d+)_[a-f0-9]{8}$/u.exec(String(chunk.id));
        if (match) {
          expect(chunk.title).toBe(`🧠 ${segments[Number(match[1]) - 1]}`);
        }
      }
    }
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("keeps a tool row behind the reasoning cards queued before it", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const segments = Array.from({ length: 60 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        // 60 cards queue beyond the current message; the tool starts before the next paced send.
        { kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true },
        {
          kind: "tool_start",
          itemId: "tool-1",
          name: "bash",
          phase: "start",
          args: { command: "pnpm test" },
        },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const timeline = collectNativeStreamTimeline();
    // First appearance of every row, in transport order: the reasoning that
    // preceded the tool call comes first, on every message it needs.
    const order: string[] = [];
    for (const call of timeline) {
      for (const chunk of chunksOf(call.params)) {
        if (chunk.type !== "task_update" || typeof chunk.id !== "string") {
          continue;
        }
        const id = chunk.id.replace(/_[a-f0-9]{8}$/u, "");
        if (!order.includes(id)) {
          order.push(id);
        }
      }
    }
    expect(order).toEqual([
      ...Array.from({ length: 60 }, (_, index) => `reasoning_${index + 1}`),
      "tool_1",
    ]);
    const messages = splitNativeStreamMessages(timeline);
    expect(messages.flatMap((message) => reasoningIdsOf(message))).toEqual(
      Array.from({ length: 60 }, (_, index) => index + 1),
    );
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("streams the answer as its own message when Slack rejects it with msg_too_long", async () => {
    useFreshStreamSessions();
    appendSlackStreamMock.mockImplementation(async (input?: unknown) => {
      const params = requireRecord(input, "append");
      if (params.text === `\n${FINAL_REPLY_TEXT}` && appendSlackStreamMock.mock.calls.length < 3) {
        throw new TestSlackStreamMessageTooLongError(params.text);
      }
    });
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [{ kind: "reasoning", text: "Checking the handler" }, { kind: "reasoning_end" }],
    });

    const timeline = collectNativeStreamTimeline();
    expect(timeline.map((call) => call.kind)).toEqual([
      "start",
      "append",
      "append",
      "stop",
      "start",
      "stop",
    ]);
    // The think closed normally on the first message before the answer was rejected.
    expect(chunksOf(timeline[1]?.params ?? {})[0]).toEqual(
      planUpdate(expect.stringMatching(/^Thought for \d+s$/u)),
    );
    expect(timeline[2]?.params.text).toBe(`\n${FINAL_REPLY_TEXT}`);
    expect(timeline[3]?.params.chunks).toBeUndefined();
    // The retry is a text-only stream in the same thread.
    expect(timeline[4]?.params).toMatchObject({
      threadTs: THREAD_TS,
      text: FINAL_REPLY_TEXT,
      chunks: [],
    });
    expect(timeline[4]?.params.taskDisplayMode).toBeUndefined();
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("delivers the answer normally when its retry as a new message is rejected too", async () => {
    useFreshStreamSessions();
    appendSlackStreamMock.mockImplementation(async (input?: unknown) => {
      const params = requireRecord(input, "append");
      if (params.text === `\n${FINAL_REPLY_TEXT}`) {
        throw new TestSlackStreamMessageTooLongError(params.text);
      }
    });
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "Checking the handler" },
        {
          kind: "checkpoint",
          run: async () => {
            startSlackStreamMock.mockRejectedValueOnce(
              new TestSlackStreamMessageTooLongError(FINAL_REPLY_TEXT),
            );
          },
        },
        { kind: "reasoning_end" },
      ],
    });

    const timeline = collectNativeStreamTimeline();
    expect(timeline.map((call) => call.kind)).toEqual([
      "start",
      "append",
      "append",
      "stop",
      "start",
    ]);
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
  });

  it("finishes the think and streams a long answer separately when both would not fit one message", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const segments = Array.from({ length: 24 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
    const answer = "a".repeat(3_300);
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    await dispatchNativeProgressScenario({
      finalPayload: { text: answer },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const messages = splitNativeStreamMessages(collectNativeStreamTimeline());
    expect(messages).toHaveLength(2);
    const [think, reply] = messages;
    if (!think || !reply) {
      throw new Error("expected two stream messages");
    }
    expect(reasoningIdsOf(think)).toEqual(Array.from({ length: 24 }, (_, index) => index + 1));
    // The think closes with its summary title; the answer is not squeezed under it.
    const stop = think.at(-1);
    expect(stop?.kind).toBe("stop");
    expect(chunksOf(stop?.params ?? {})).toContainEqual(
      planUpdate(expect.stringMatching(/^Thought for \d+s$/u)),
    );
    expect(think.some((call) => call.params.text === `\n${answer}`)).toBe(false);
    expect(reply.map((call) => call.kind)).toEqual(["start", "stop"]);
    expect(reply[0]?.params).toMatchObject({ threadTs: THREAD_TS, text: answer, chunks: [] });
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("keeps a fitting answer on the card message when the draft boundary lands during final delivery", async () => {
    // Real event order from a live model: thinking deltas, then the assistant text; core fires
    // onReasoningEnd and onAssistantMessageStart best effort (not awaited) while the seal's
    // append is still in flight, and the final reply is dispatched right behind them.
    const realSecond = {
      kind: "checkpoint" as const,
      run: () => sleepRealMs(1_050),
    };
    // Slack round trips take time; a stop returns a little faster than an append.
    appendSlackStreamMock.mockImplementation(async () => {
      await sleepRealMs(10);
    });
    stopSlackStreamMock.mockImplementation(async () => {
      await sleepRealMs(5);
      return {};
    });
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "The user wants a short answer.", isReasoningSnapshot: true },
        realSecond,
        {
          kind: "reasoning",
          text: "The user wants a short answer. Keep it short and Slack formatted.",
          isReasoningSnapshot: true,
        },
        realSecond,
        {
          kind: "checkpoint",
          run: async () => {
            // The seal's append is still in flight when the run ends.
            appendSlackStreamMock.mockImplementationOnce(async () => {
              await sleepRealMs(30);
            });
            void harness.capturedReplyOptions?.onReasoningEnd?.();
            void harness.capturedReplyOptions?.onAssistantMessageStart?.();
          },
        },
      ],
    });
    // The late boundary settles after the dispatch returns.
    await sleepRealMs(60);

    const timeline = collectNativeStreamTimeline();
    expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
    expect(timeline.map((call) => call.kind)).toEqual([
      "start",
      "append",
      "append",
      "append",
      "append",
      "stop",
    ]);
    expect(planTitlesOf(timeline).at(-1)).toMatch(/^Thought for \d+s$/u);
    expect(timeline[4]?.params.text).toBe(`\n${FINAL_REPLY_TEXT}`);
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("titles the finished think with its summary even when a preamble headline was showing", async () => {
    vi.useFakeTimers();
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    const preamble =
      "The full-home scan was too slow — let me grab random files from a shallower sweep instead.";
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "The find command got terminated.", isReasoningSnapshot: true },
        settle,
        { kind: "item", itemKind: "preamble", itemId: "preamble-1", progressText: preamble },
        settle,
        { kind: "reasoning", text: "Six random files it is.", isReasoningSnapshot: true },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const titles = planTitlesOf(collectNativeStreamTimeline());
    // The headline may show while the turn runs, but the finished card is the think's summary.
    expect(titles).toContain(preamble);
    expect(titles.at(-1)).toMatch(/^Thought for \d+s$/u);
    expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
  });
});
