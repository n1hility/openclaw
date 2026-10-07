// Native progress in cards mode: the per-message row and byte budget, plan
// rows across continuations, and stops or rejections during a rollover.
import { describe, expect, it, vi } from "vitest";
import {
  FINAL_REPLY_TEXT,
  TestSlackStreamMessageTooLongError,
  appendSlackStreamMock,
  deliverRepliesMock,
  postMessageMock,
  startSlackStreamMock,
  stopSlackStreamMock,
} from "./dispatch.preview-fallback.test-mocks.js";
import {
  chunksOf,
  collectNativeStreamTimeline,
  dispatchNativeProgressScenario,
  expectDeliverReplyCall,
  expectNativeStreamText,
  installPreviewFallbackHarness,
  planUpdate,
  reasoningIdsOf,
  requireRecord,
  splitNativeStreamMessages,
  taskUpdate,
  useFreshStreamSessions,
  type NativeStreamCall,
} from "./dispatch.preview-fallback.test-support.js";

describe("dispatchPreparedSlackMessage stream budget", () => {
  installPreviewFallbackHarness();

  it.each([
    { how: "by size", narration: "n".repeat(8_800), rejectOnce: false },
    { how: "after msg_too_long", narration: "n".repeat(1_000), rejectOnce: true },
  ])(
    "opens the narration continuation within budget and delivers the queued cards later ($how)",
    async ({ narration, rejectOnce }) => {
      vi.useFakeTimers();
      useFreshStreamSessions();
      const segments = Array.from({ length: 60 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
      const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
      if (rejectOnce) {
        let rejected = false;
        appendSlackStreamMock.mockImplementation(async (input?: unknown) => {
          const params = requireRecord(input, "append");
          if (!rejected && params.text === narration) {
            rejected = true;
            requireRecord(params.session, "session").pendingText = narration;
            throw new TestSlackStreamMessageTooLongError(narration);
          }
        });
      }
      await dispatchNativeProgressScenario({
        finalPayload: { text: FINAL_REPLY_TEXT },
        progress: { nativeTaskCards: true, reasoning: "cards" },
        // 60 cards inside one throttle window, then a text payload the current message cannot take.
        events: [{ kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true }],
        replies: [{ kind: "block", payload: { text: narration } }],
      });
      await settle.run();

      const timeline = collectNativeStreamTimeline();
      const messages = splitNativeStreamMessages(timeline);
      const perMessageIds = messages.map((message) => reasoningIdsOf(message));
      // Every card arrives once, in order, and no message takes more than the row budget allows.
      expect(perMessageIds.flat()).toEqual(Array.from({ length: 60 }, (_, index) => index + 1));
      for (const ids of perMessageIds) {
        expect(ids.length).toBeLessThanOrEqual(24);
      }
      // The narration lands once, on the message opened for it (after the
      // rejected append on the first one, when Slack rejected), never on a stop.
      const carrying = timeline.filter(
        (call) => call.kind !== "stop" && call.params.text === narration,
      );
      expect(carrying.every((call) => call.kind === "append")).toBe(true);
      expect(
        messages.flatMap((message, index) =>
          message.some((call) => call.kind === "append" && call.params.text === narration)
            ? [index]
            : [],
        ),
      ).toEqual(rejectOnce ? [0, 1] : [1]);
      expect(deliverRepliesMock).not.toHaveBeenCalled();
    },
  );

  it.each([
    { terminal: "error", finalPayload: { text: "it broke", isError: true }, status: "error" },
    {
      terminal: "media",
      finalPayload: { text: "see the chart", mediaUrl: "https://example.com/chart.png" },
      status: "complete",
    },
  ] as const)(
    "drains a 400-card queue through budgeted messages before a $terminal closeout",
    async ({ finalPayload, status }) => {
      vi.useFakeTimers();
      useFreshStreamSessions();
      const segments = Array.from({ length: 400 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
      await dispatchNativeProgressScenario({
        finalPayload,
        progress: { nativeTaskCards: true, reasoning: "cards" },
        events: [{ kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true }],
      });

      const messages = splitNativeStreamMessages(collectNativeStreamTimeline());
      const perMessageIds = messages.map((message) => reasoningIdsOf(message));
      expect(perMessageIds.flat()).toEqual(Array.from({ length: 400 }, (_, index) => index + 1));
      expect(messages.length).toBeGreaterThanOrEqual(17);
      for (const ids of perMessageIds) {
        expect(ids.length).toBeLessThanOrEqual(24);
      }
      const last = messages.at(-1);
      expect(last?.at(-1)?.kind).toBe("stop");
      expect(last?.flatMap((call) => chunksOf(call.params))).toContainEqual(
        taskUpdate(
          expect.stringMatching(/^reasoning_400_[a-f0-9]{8}$/u),
          `🧠 ${segments[399]}`,
          status,
        ),
      );
      expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
    },
  );

  it("drains tool rows the compositor admitted past the budget before an error closeout", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    // 120 distinct tool rows inside one throttle window: the first message
    // and each continuation take a budget's worth, so a fresh opening still
    // has to defer rows.
    const toolStarts = Array.from({ length: 120 }, (_, index) => ({
      kind: "tool_start" as const,
      itemId: `tool-${index + 1}`,
      name: "bash",
      phase: "start" as const,
      args: { command: `pnpm test --filter case-${index + 1} ${"x".repeat(60)}` },
    }));
    await dispatchNativeProgressScenario({
      finalPayload: { text: "it broke", isError: true },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: toolStarts,
    });

    const timeline = collectNativeStreamTimeline();
    const messages = splitNativeStreamMessages(timeline);
    const toolIdsOf = (calls: NativeStreamCall[]) => [
      ...new Set(
        calls.flatMap((call) =>
          chunksOf(call.params)
            .filter((chunk) => chunk.type === "task_update" && /^tool_\d+_/u.test(String(chunk.id)))
            .map((chunk) => Number(/^tool_(\d+)_/u.exec(String(chunk.id))?.[1])),
        ),
      ),
    ];
    // Every tool row reaches Slack once, on budgeted messages, with the error status at the end.
    expect(toolIdsOf(timeline).toSorted((a, b) => a - b)).toEqual(
      Array.from({ length: 120 }, (_, index) => index + 1),
    );
    expect(messages.length).toBeGreaterThanOrEqual(3);
    const perMessage = messages.map((message) => toolIdsOf(message));
    expect(perMessage.flat()).toHaveLength(120);
    const last = messages.at(-1);
    expect(last?.at(-1)?.kind).toBe("stop");
    expect(
      last?.flatMap((call) => chunksOf(call.params)).some((chunk) => chunk.status === "error"),
    ).toBe(true);
    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
  });

  it("leaves continuation capacity for queued cards behind a plan that fills the row budget", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const segments = Array.from({ length: 30 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        {
          kind: "plan",
          phase: "update",
          steps: Array.from({ length: 50 }, (_, index) => ({
            step: `Step ${index + 1}`,
            status: "pending" as const,
          })),
        },
        { kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const messages = splitNativeStreamMessages(collectNativeStreamTimeline());
    const perMessageIds = messages.map((message) => reasoningIdsOf(message));
    // The plan alone fills the first message; every continuation carries cards,
    // and the run converges with each card delivered once.
    expect(perMessageIds.flat()).toEqual(Array.from({ length: 30 }, (_, index) => index + 1));
    expect(messages.length).toBeGreaterThanOrEqual(2);
    expect(messages.length).toBeLessThanOrEqual(4);
    for (const ids of perMessageIds.slice(1)) {
      expect(ids.length).toBeGreaterThan(0);
    }
    const last = messages.at(-1);
    expect(last?.some((call) => call.params.text === `\n${FINAL_REPLY_TEXT}`)).toBe(true);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("keeps plan progress visible across budgeted continuations and settles it at completion", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const segments = Array.from({ length: 30 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    const plan = (inProgress: number) =>
      Array.from({ length: 50 }, (_, index) => ({
        step: `Step ${index + 1}`,
        status:
          index + 1 < inProgress
            ? ("completed" as const)
            : index + 1 === inProgress
              ? ("in_progress" as const)
              : ("pending" as const),
      }));
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "plan", phase: "update", steps: plan(1) },
        { kind: "reasoning", text: segments.join(" "), isReasoningSnapshot: true },
        settle,
        // Step 1 finishes and step 2 starts while the cards are still going out.
        { kind: "plan", phase: "update", steps: plan(2) },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const messages = splitNativeStreamMessages(collectNativeStreamTimeline());
    expect(messages.length).toBeGreaterThanOrEqual(2);
    const planStatuses = (calls: NativeStreamCall[], step: number) =>
      calls.flatMap((call) =>
        chunksOf(call.params)
          .filter((chunk) => chunk.type === "task_update" && chunk.id === `plan_step_${step}`)
          .map((chunk) => chunk.status),
      );
    const second = messages[1] as NativeStreamCall[];
    const last = messages.at(-1) as NativeStreamCall[];
    // The running step opens every continuation; the burst drains through the
    // continuations within the first paced send, so the plan update lands on
    // the message current then, the last one, and the completion settles the
    // step that was running at the end.
    expect(planStatuses(second, 1)[0]).toBe("in_progress");
    expect(planStatuses(last, 1)[0]).toBe("in_progress");
    expect(planStatuses(last, 1)).toContain("complete");
    expect(planStatuses(last, 2)).toContain("in_progress");
    expect(planStatuses(last, 2).at(-1)).toBe("complete");
    expect(messages.flatMap((message) => reasoningIdsOf(message))).toEqual(
      Array.from({ length: 30 }, (_, index) => index + 1),
    );
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("delivers a later result for a running row frozen by an overflow roll", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    const toolStarts = Array.from({ length: 120 }, (_, index) => ({
      kind: "tool_start" as const,
      itemId: `tool-${index + 1}`,
      name: "bash",
      phase: "start" as const,
      args: { command: `pnpm test --filter case-${index + 1}` },
    }));
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        ...toolStarts,
        // The burst rolls through several messages; the running rows of a
        // rolled message are frozen there.
        settle,
        // The first tool, on the first message, fails afterwards.
        { kind: "command_output", itemId: "tool-1", name: "bash", phase: "end", exitCode: 1 },
        settle,
      ],
    });

    const timeline = collectNativeStreamTimeline();
    const messages = splitNativeStreamMessages(timeline);
    expect(messages.length).toBeGreaterThanOrEqual(3);
    const tool1 = (calls: NativeStreamCall[]) =>
      calls.flatMap((call) =>
        chunksOf(call.params).filter(
          (chunk) => chunk.type === "task_update" && /^tool_1_[a-f0-9]{8}$/u.test(String(chunk.id)),
        ),
      );
    const first = messages[0] as NativeStreamCall[];
    const last = messages.at(-1) as NativeStreamCall[];
    // The first message closed with the row painted complete (the message overflowed).
    expect(tool1(first).at(-1)?.status).toBe("complete");
    // The failure reaches the current message and the completion settles it there.
    expect(tool1(last).map((chunk) => chunk.status)).toContain("error");
    expect(tool1(last).at(-1)).toEqual(
      taskUpdate(
        expect.stringMatching(/^tool_1_[a-f0-9]{8}$/u),
        expect.stringMatching(/^Recovered: /u),
        "complete",
      ),
    );
    expect(last.some((call) => call.params.text === `\n${FINAL_REPLY_TEXT}`)).toBe(true);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("sends commentary rejected together with a card update once, on the continuation", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const commentary = "Let me check the handler.";
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    let pendingTextAtRollStop: unknown = "unset";
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards", commentary: true },
      events: [
        { kind: "reasoning", text: "Plan the fix." },
        settle,
        { kind: "reasoning_end" },
        {
          kind: "checkpoint",
          run: async () => {
            // Slack rejects the batch; the SDK keeps its text in the buffer, as
            // the real streamer does after a failed flush.
            appendSlackStreamMock.mockImplementationOnce(async (input) => {
              const params = requireRecord(input, "append");
              const text = typeof params.text === "string" ? params.text : "";
              requireRecord(params.session, "session").pendingText = text;
              throw new TestSlackStreamMessageTooLongError(text);
            });
            stopSlackStreamMock.mockImplementationOnce(async (input) => {
              pendingTextAtRollStop = requireRecord(
                requireRecord(input, "stop").session,
                "session",
              ).pendingText;
              return {};
            });
          },
        },
        { kind: "item", itemKind: "preamble", itemId: "preamble-1", progressText: commentary },
        { kind: "reasoning", text: "Then test it." },
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const timeline = collectNativeStreamTimeline();
    const messages = splitNativeStreamMessages(timeline);
    expect(messages).toHaveLength(2);
    const [first, second] = messages;
    if (!first || !second) {
      throw new Error("expected two stream messages");
    }
    // The rolled message's stop flushes nothing the SDK retained from the rejected batch.
    expect(pendingTextAtRollStop).toBe("");
    expect(first.at(-1)?.kind).toBe("stop");
    expect(
      chunksOf(first.at(-1)?.params ?? {}).some((chunk) => chunk.type === "markdown_text"),
    ).toBe(false);
    // Apart from the rejected append, the commentary reaches Slack exactly
    // once, on the continuation's start, and never on the rolled message.
    const carries = (call: NativeStreamCall) =>
      typeof call.params.text === "string" && call.params.text.includes(commentary);
    const rejectedAppend = first.at(-2);
    expect(rejectedAppend?.kind).toBe("append");
    expect(rejectedAppend && carries(rejectedAppend)).toBe(true);
    expect(first.filter((call) => call !== rejectedAppend && carries(call))).toHaveLength(0);
    expect(second.filter(carries)).toEqual([second[0]]);
    expect(second[0]?.kind).toBe("start");
    expect(reasoningIdsOf(second)).toEqual([2]);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("does not open a continuation or post the answer after a Slack Stop lands during the rollover", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const segments = Array.from({ length: 30 }, (_, index) => `s${index + 1}`.padEnd(240, "x"));
    const snapshotUpTo = (count: number) => ({
      kind: "reasoning" as const,
      text: segments.slice(0, count).join(" "),
      isReasoningSnapshot: true,
    });
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        snapshotUpTo(20),
        settle,
        {
          kind: "checkpoint",
          run: async () => {
            // Slack's Stop for this message is admitted while the rollover stop is in flight.
            stopSlackStreamMock.mockImplementationOnce(async (input) => {
              const session = requireRecord(requireRecord(input, "stop").session, "session");
              session.stopped = true;
              session.stoppedBySlack = true;
              return {};
            });
          },
        },
        snapshotUpTo(30),
        settle,
        { kind: "reasoning_end" },
        settle,
      ],
    });

    const timeline = collectNativeStreamTimeline();
    expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
    // Nothing is written after the stopped message: no continuation, no completion, no answer.
    expect(timeline.at(-1)?.kind).toBe("stop");
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`, 0);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
    expect(postMessageMock).not.toHaveBeenCalled();
  });

  it("stops the stream and delivers the answer normally when the continuation is rejected too", async () => {
    vi.useFakeTimers();
    useFreshStreamSessions();
    const settle = { kind: "checkpoint" as const, run: () => vi.advanceTimersByTimeAsync(1_000) };
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { nativeTaskCards: true, reasoning: "cards" },
      events: [
        { kind: "reasoning", text: "Plan the fix." },
        settle,
        {
          kind: "checkpoint",
          run: async () => {
            appendSlackStreamMock.mockRejectedValueOnce(new TestSlackStreamMessageTooLongError(""));
            startSlackStreamMock.mockRejectedValueOnce(new TestSlackStreamMessageTooLongError(""));
          },
        },
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
    expect(timeline.map((call) => call.kind)).toEqual(["start", "append", "stop", "start"]);
    expect(chunksOf(timeline[2]?.params ?? {})[0]).toEqual(planUpdate("Thinking, continued below"));
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
  });
});
