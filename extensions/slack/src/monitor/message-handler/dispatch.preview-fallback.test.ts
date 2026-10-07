// Slack tests cover dispatch.preview fallback plugin behavior.
import { realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  FINAL_REPLY_TEXT,
  SAME_TEXT,
  STREAM_MESSAGE_TS,
  THREAD_TS,
  TestSlackStreamNotDeliveredError,
  appendSlackStreamMock,
  buildSlackCompleteBlocksFallbackText,
  chatUpdateMock,
  createMessageReceiptFromOutboundResults,
  createSlackDraftStreamMock,
  deliverRepliesMock,
  emitCompactProgressScenario,
  emitSlackMessageSentHooksMock,
  finalizeInboundContext,
  finalizeSlackPreviewEditMock,
  getGlobalHookRunnerMock,
  getSlackSessionRuns,
  harness,
  logVerboseMock,
  normalDeliveryResult,
  normalizeSlackOutboundTextMock,
  postMessageMock,
  projectProgressCardChannelUpdate,
  reactSlackMessageMock,
  removeSlackReactionMock,
  sendMessageSlackMock,
  startSlackStreamMock,
  statusReactionControllerMock,
  stopSlackStreamMock,
  updateLastRouteMock,
  type SlackReplyOptionEvent,
} from "./dispatch.preview-fallback.test-mocks.js";
import {
  checkpoint,
  collectNativeTaskUpdates,
  contentTaskId,
  createDraftStreamStub,
  createNativeStreamSession,
  createPreparedSlackMessage,
  createSlackPlatformError,
  delivered,
  dispatch,
  dispatchNativeProgressScenario,
  dispatchPreparedSlackMessage,
  draftUpdateTexts,
  expectDeliverReplyCall,
  expectLastDraftUpdateText,
  expectMockCallArgFields,
  expectNativeProgressAppend,
  expectNativeProgressStart,
  expectNativeStreamText,
  expectRecordFields,
  installPreviewFallbackHarness,
  noopAsync,
  planUpdate,
  preamble,
  progressAccount,
  requireCapturedItemEventHandler,
  requireCapturedTyping,
  requireMockCall,
  requireRecord,
  taskUpdate,
  ttsPayload,
  useDraftStream,
} from "./dispatch.preview-fallback.test-support.js";

describe("dispatchPreparedSlackMessage preview fallback", () => {
  installPreviewFallbackHarness();

  it.each([
    { agents: ["alice", "bob"], withMedia: true },
    { agents: ["alice", "bob"], withMedia: false },
  ])(
    "binds group-thread completion hooks and media to the participant: $agents (media: $withMedia)",
    async ({ agents, withMedia }) => {
      harness.useRealChannelInboundTurn = true;
      harness.mockedNativeStreaming = true;
      const { resolveGroupThreadMentionFacts } =
        await import("openclaw/plugin-sdk/channel-inbound");
      const workspaceRoot = realpathSync(tmpdir());
      const cfg = {
        agents: {
          entries: Object.fromEntries(
            ["root", "alice", "bob"].map((id) => [id, { workspace: path.join(workspaceRoot, id) }]),
          ),
        },
        broadcast: { "slack:C123": agents },
      };
      const rootSessionKey = `agent:root:slack:channel:c123:thread:${THREAD_TS}`;
      const actualReplies = await vi.importActual<typeof import("../replies.js")>("../replies.js");
      const { prepareSlackReply } = await import("../../reply-blocks.js");
      deliverRepliesMock.mockImplementation(async (params) =>
        actualReplies.deliverReplies({ ...params, replies: params.replies.map(prepareSlackReply) }),
      );
      sendMessageSlackMock.mockResolvedValue({
        messageId: "sent-1",
        channelId: "C123",
        receipt: createMessageReceiptFromOutboundResults({
          results: [{ channel: "slack", messageId: "sent-1", channelId: "C123" }],
          kind: withMedia ? "media" : "text",
        }),
      });
      const participantRuns: string[] = [];
      const dispatchReplyFromConfig: NonNullable<
        Parameters<typeof dispatchPreparedSlackMessage>[0]["ctx"]["dispatchReplyFromConfig"]
      > = async ({ ctx, dispatcher }) => {
        if (!ctx.AgentId) {
          throw new Error("Expected participant agent identity");
        }
        participantRuns.push(ctx.AgentId);
        return {
          queuedFinal: dispatcher.sendFinalReply({
            text: `Reply from ${ctx.AgentId}`,
            ...(withMedia
              ? { mediaUrl: path.join(workspaceRoot, ctx.AgentId, "attachment.txt") }
              : {}),
          }),
          counts: dispatcher.getQueuedCounts(),
        };
      };

      await dispatch({
        cfg,
        route: { agentId: "root", sessionKey: rootSessionKey },
        ctxPayload: finalizeInboundContext({
          AgentId: "root",
          SessionKey: rootSessionKey,
          ChatType: "channel",
          Provider: "slack",
          Surface: "slack",
          OriginatingChannel: "slack",
          OriginatingTo: "channel:C123",
          NativeChannelId: "C123",
          AccountId: "default",
          From: "slack:C123",
          To: "channel:C123",
          SenderId: "U123",
          MessageSid: "171234.111",
          MessageThreadId: THREAD_TS,
          Body: "Review the attachment.",
          GroupThread: resolveGroupThreadMentionFacts({
            cfg,
            channel: "slack",
            peerId: "C123",
            text: "Review the attachment.",
            sessionKey: rootSessionKey,
          }),
        }),
        dispatchReplyFromConfig,
      });

      expect(participantRuns.toSorted()).toEqual(agents.toSorted());
      expect(emitSlackMessageSentHooksMock).toHaveBeenCalledTimes(agents.length);
      expect(sendMessageSlackMock).toHaveBeenCalledTimes(agents.length);
      for (const agentId of agents) {
        expect(emitSlackMessageSentHooksMock).toHaveBeenCalledWith(
          expect.objectContaining({
            sessionKeyForInternalHooks: `agent:${agentId}:slack:channel:c123:thread:${THREAD_TS}`,
            success: true,
          }),
        );
        expect(sendMessageSlackMock).toHaveBeenCalledWith(
          "channel:C123",
          expect.stringContaining(`Reply from ${agentId}`),
          expect.objectContaining({
            ...(withMedia ? { mediaUrl: path.join(workspaceRoot, agentId, "attachment.txt") } : {}),
            mediaLocalRoots: expect.arrayContaining([path.join(workspaceRoot, agentId)]),
          }),
        );
      }
      expect(startSlackStreamMock).not.toHaveBeenCalled();
      expect(createSlackDraftStreamMock).not.toHaveBeenCalled();
    },
  );

  it.each(["literal", "native literal", "split table", "error"] as const)(
    "delivers %s finals through the normal sender instead of a preview edit",
    async (scenario) => {
      const draftStream = useDraftStream();
      const literal = scenario === "literal" || scenario === "native literal";
      harness.mockedNativeStreaming = scenario === "native literal";
      if (literal) {
        finalizeSlackPreviewEditMock.mockResolvedValue(undefined);
      }
      const payload = literal
        ? {
            text: "Run /inspect *literal* <!channel>, then check the report.",
            presentationTextMode: "fallback" as const,
            presentation: {
              blocks: [
                {
                  type: "buttons" as const,
                  buttons: [
                    { label: "Inspect", action: { type: "command" as const, command: "/inspect" } },
                  ],
                },
              ],
            },
          }
        : scenario === "split table"
          ? {
              text: "Accounts",
              presentation: {
                blocks: [
                  {
                    type: "table",
                    caption: "Account owners",
                    headers: ["Owner"],
                    rows: Array.from({ length: 100 }, (_entry, index) => [
                      `owner-${String(index)}-${"x".repeat(110)}`,
                    ]),
                  },
                  { type: "buttons", buttons: [{ label: "Refresh", value: "refresh" }] },
                ],
              },
            }
          : { text: "Something failed", isError: true };
      harness.mockedDispatchSequence = [{ kind: "final", payload }];
      await dispatch();
      expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
      expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
      expect(delivered().replies).toEqual([payload]);
      if (literal) {
        expect(startSlackStreamMock).not.toHaveBeenCalled();
        expect(appendSlackStreamMock).not.toHaveBeenCalled();
        expect(deliverRepliesMock).toHaveBeenCalledWith(
          expect.objectContaining({ replies: [payload] }),
        );
      } else if (scenario === "error") {
        expect(draftStream.flush).not.toHaveBeenCalled();
        expect(draftStream.discardPending).toHaveBeenCalled();
      }
    },
  );

  it("preserves rejected queue admission without retaining a publisher", async () => {
    const onSettled = vi.fn();
    const prepared: Parameters<typeof dispatchPreparedSlackMessage>[0] = createPreparedSlackMessage(
      {
        turnAdoptionLifecycle: {
          admission: "exclusive",
          onAdopted: async () => {},
          onDeferred: () => false,
          onSettled,
        },
      },
    );
    await dispatchPreparedSlackMessage(prepared);
    expect(harness.capturedReplyOptions?.turnAdoptionLifecycle?.onDeferred?.()).toBe(false);
    expect(getSlackSessionRuns(prepared.ctx, { channelId: "C123", threadTs: THREAD_TS })).toEqual(
      [],
    );
    harness.capturedReplyOptions?.turnAdoptionLifecycle?.onSettled?.();
    expect(onSettled).toHaveBeenCalledOnce();
  });

  it("tracks a first-mode root publisher without a status thread", async () => {
    const message = {
      type: "message" as const,
      channel: "C123",
      ts: "171234.111",
      thread_ts: undefined,
    };
    const threading =
      await vi.importActual<typeof import("../../threading.js")>("../../threading.js");
    harness.mockedStatusThreadTs = threading.resolveSlackThreadContext({
      message,
      replyToMode: "first",
    }).messageThreadId;
    expect(harness.mockedStatusThreadTs).toBeUndefined();
    harness.mockedReplyThreadTs = message.ts;
    harness.mockedSlackIsThreadReply = false;
    const prepared: Parameters<typeof dispatchPreparedSlackMessage>[0] = createPreparedSlackMessage(
      { message, replyToMode: "first" },
    );
    harness.mockedReplyOptionEvents = [
      checkpoint(async () => {
        expect(
          getSlackSessionRuns(prepared.ctx, {
            channelId: message.channel,
            threadTs: message.ts,
          }).map(({ route }) => route.sessionKey),
        ).toEqual([prepared.route.sessionKey]);
      }),
    ];
    await dispatchPreparedSlackMessage(prepared);
  });

  it("retains queued publisher ownership until settlement", async () => {
    const prepared: Parameters<typeof dispatchPreparedSlackMessage>[0] = createPreparedSlackMessage(
      {
        turnAdoptionLifecycle: {
          admission: "exclusive",
          onAdopted: async () => {},
          onDeferred: () => {},
          onAbandoned: () => {},
        },
      },
    );
    const address = { channelId: "C123", threadTs: THREAD_TS };
    harness.mockedReplyOptionEvents = [
      checkpoint(async () => {
        expect(
          getSlackSessionRuns(prepared.ctx, address).map(({ route }) => route.sessionKey),
        ).toEqual([prepared.route.sessionKey]);
        harness.capturedReplyOptions?.turnAdoptionLifecycle?.onDeferred?.();
      }),
    ];
    await dispatchPreparedSlackMessage(prepared);
    expect(getSlackSessionRuns(prepared.ctx, address).map(({ route }) => route.sessionKey)).toEqual(
      [prepared.route.sessionKey],
    );
    const endQueuedRun = harness.capturedReplyOptions?.queuedDeliveryCorrelations?.[0]?.begin();
    harness.capturedReplyOptions?.turnAdoptionLifecycle?.onSettled?.();
    expect(getSlackSessionRuns(prepared.ctx, address)).toHaveLength(1);
    expect(getSlackSessionRuns({ ...prepared.ctx }, address)).toHaveLength(1);
    const restarted: Parameters<typeof dispatchPreparedSlackMessage>[0] =
      createPreparedSlackMessage();
    expect(getSlackSessionRuns(restarted.ctx, address)).toEqual([]);
    endQueuedRun?.();
    expect(getSlackSessionRuns(prepared.ctx, address)).toEqual([]);
  });

  it.each([
    { hook: "reply_payload_sending", native: false, progress: false },
    { hook: "message_sending", native: true, progress: true },
  ])(
    "gates pre-hook previews ($hook, native=$native, progress=$progress)",
    async ({ hook, native, progress }) => {
      getGlobalHookRunnerMock.mockReturnValue({ hasHooks: vi.fn((name: string) => name === hook) });
      harness.mockedNativeStreaming = native;
      if (progress) {
        await dispatchNativeProgressScenario({
          finalPayload: { text: FINAL_REPLY_TEXT },
          events: [{ kind: "item", progressText: "private progress" }],
        });
      } else {
        await dispatch();
      }
      expect(createSlackDraftStreamMock).not.toHaveBeenCalled();
      if (native && !progress) {
        expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
        expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
        expect(deliverRepliesMock).not.toHaveBeenCalled();
      } else {
        expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
        expect(startSlackStreamMock).not.toHaveBeenCalled();
        expect(appendSlackStreamMock).not.toHaveBeenCalled();
        expect(deliverRepliesMock).toHaveBeenCalledOnce();
        expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
      }
    },
  );

  it("posts the final below a human message received while the preview was sealing", async () => {
    let messageId: string | undefined = "171234.567";
    const draftStream = {
      ...createDraftStreamStub(),
      seal: vi.fn(async () => {
        messageId = undefined;
      }),
      finalizeMessage: vi.fn(async () => false),
      messageId: () => messageId,
      channelId: () => (messageId ? "C123" : undefined),
    };
    createSlackDraftStreamMock.mockReturnValueOnce(draftStream);

    await dispatch();

    expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
    expect(deliverRepliesMock).toHaveBeenCalledOnce();
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
  });

  it.each([false, true])("preserves custom authorship with native streaming %s", async (native) => {
    harness.mockedNativeStreaming = native;
    const relayIdentity = {
      username: "Nik Team Claw",
      ...(native ? {} : { iconEmoji: ":robot_face:" }),
    };
    const draftStream = useDraftStream();
    await dispatch({ relayIdentity });
    if (native) {
      expectMockCallArgFields(startSlackStreamMock, 0, {
        text: FINAL_REPLY_TEXT,
        identity: relayIdentity,
      });
      expect(createSlackDraftStreamMock).not.toHaveBeenCalled();
      expect(deliverRepliesMock).not.toHaveBeenCalled();
    } else {
      expect(createSlackDraftStreamMock).toHaveBeenCalledWith(
        expect.not.objectContaining({ identity: expect.anything() }),
      );
      expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
      expect(draftStream.discardPending).toHaveBeenCalled();
      expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
      expectDeliverReplyCall(0, FINAL_REPLY_TEXT, { identity: relayIdentity });
    }
  });

  it("does not create a Slack thread for top-level messages when replyToMode is off", async () => {
    harness.mockedSlackStreamingMode = "off";
    harness.mockedSlackIsThreadReply = false;

    await dispatch({ replyToMode: "off" });

    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT, { replyThreadTs: undefined });
  });

  it.each([
    { name: "non-main direct session", transport: false },
    { name: "transport thread", transport: true },
  ])("records DM last-route metadata for $name", async ({ transport }) => {
    const sessionKey = transport ? "agent:main:main" : "agent:main:slack:direct:u1";
    const threadId = transport ? "701.000" : "500.000";
    const ctxPayload = transport
      ? {
          MessageThreadId: undefined,
          ReplyToId: threadId,
          TransportThreadId: threadId,
          SessionKey: sessionKey,
        }
      : { MessageThreadId: threadId, SessionKey: sessionKey };
    harness.mockedPinnedMainDmOwner = transport ? undefined : "U2";
    await dispatch({
      ...(!transport ? { cfg: { session: { dmScope: "per-channel-peer" } } } : {}),
      isDirectMessage: true,
      message: {
        channel: "D123",
        user: "U1",
        ts: transport ? threadId : "501.000",
        thread_ts: threadId,
      },
      route: {
        agentId: "main",
        mainSessionKey: "agent:main:main",
        sessionKey,
        lastRoutePolicy: transport ? "main" : "session",
      },
      ctxPayload,
    });
    expect(updateLastRouteMock).toHaveBeenCalledWith({
      storePath: "/tmp/openclaw-store.json",
      sessionKey,
      deliveryContext: { channel: "slack", to: "user:U1", accountId: "default", threadId },
      ctx: ctxPayload,
    });
  });

  it.each([
    ["code", "```\n| Name | Value |\n| ---- | ----- |\n| Beta | 2     |\n```"],
    ["bullets", "*Beta*\n• Value: 2"],
  ] as const)(
    "preserves %s table mode when finalizing authored preview text",
    async (tables, expected) => {
      const { normalizeSlackOutboundText } =
        await vi.importActual<typeof import("../../format.js")>("../../format.js");
      normalizeSlackOutboundTextMock.mockImplementation(normalizeSlackOutboundText);
      try {
        finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);
        harness.mockedDispatchSequence = [
          { kind: "final", payload: { text: "| Name | Value |\n| --- | --- |\n| Beta | 2 |" } },
        ];

        await dispatch({
          cfg: { channels: { slack: { markdown: { tables } } } },
        });

        expect(finalizeSlackPreviewEditMock).toHaveBeenCalledOnce();
        expectMockCallArgFields(finalizeSlackPreviewEditMock, 0, {
          channelId: "C123",
          messageId: "171234.567",
          text: expected,
        });
        expect(deliverRepliesMock).not.toHaveBeenCalled();
      } finally {
        normalizeSlackOutboundTextMock.mockImplementation((value: string) => value.trim());
      }
    },
  );

  it.each([
    {
      operation: "add",
      error: "missing_scope",
      details: { needed: "reactions:write", provided: "chat:write" },
      suffix: "; needed: reactions:write; provided: chat:write",
    },
    { operation: "remove", error: "invalid_auth", details: undefined, suffix: "" },
  ])(
    "logs the formatted Slack error when typing reaction $operation fails",
    async ({ operation, error, details, suffix }) => {
      (operation === "add" ? reactSlackMessageMock : removeSlackReactionMock).mockRejectedValueOnce(
        createSlackPlatformError(error, details),
      );
      await dispatch({ typingReaction: "hourglass_flowing_sand" });
      const typing = requireCapturedTyping();
      await expect(typing.start()).resolves.toBeUndefined();
      if (operation === "remove") {
        await expect(typing.stop?.()).resolves.toBeUndefined();
      }
      expect(logVerboseMock).toHaveBeenCalledWith(
        `slack send: typing reaction${operation === "remove" ? " removal" : ""} failed: An API error occurred: ${error}; code: slack_webapi_platform_error; slack error: ${error}${suffix}`,
      );
    },
  );

  it.each(["message-tool-only", "recovered-failure", "room-event", "reasoning-only"] as const)(
    "settles lifecycle reactions for %s delivery",
    async (scenario) => {
      const failed = scenario === "recovered-failure";
      const toolOnly = scenario === "message-tool-only";
      const ambient = scenario === "room-event";
      if (failed) {
        harness.mockedAgentRunTerminalOutcome = "failed";
        harness.mockedNativeStreaming = true;
        harness.mockedReplyOptionEvents = [{ kind: "item", progressText: "Recovering failed run" }];
        harness.mockedDispatchSequence = [
          { kind: "final", payload: { text: "Something failed", isError: true } },
        ];
      } else if (scenario === "reasoning-only") {
        harness.mockedDispatchSequence = [
          { kind: "final", payload: { text: "Reasoning:\n_hidden_", isReasoning: true } },
        ];
      }
      await dispatch({
        cfg: {
          messages: {
            ...(toolOnly ? { groupChat: { visibleReplies: "message_tool" } } : {}),
            statusReactions: { enabled: true },
          },
        },
        ...(failed
          ? { accountConfig: progressAccount({ toolProgress: true, nativeTaskCards: true }) }
          : {}),
        ...(toolOnly || ambient
          ? {
              ctxPayload: {
                ChatType: "channel",
                ...(ambient ? { InboundEventKind: "room_event" } : {}),
              },
            }
          : {}),
        ackReactionMessageTs: "171234.111",
        ackReactionPromise: Promise.resolve(true),
      });
      if (toolOnly || ambient) {
        expectRecordFields(
          requireRecord(harness.capturedStatusReactionOptions, "status reaction options"),
          { enabled: !ambient, initialEmoji: "eyes" },
        );
        expect(statusReactionControllerMock.setQueued).toHaveBeenCalledTimes(ambient ? 0 : 1);
      }
      if (toolOnly) {
        expect(harness.capturedReplyOptions?.disableBlockStreaming).toBe(true);
        expect(
          harness.capturedReplyOptions?.allowProgressCallbacksWhenSourceDeliverySuppressed,
        ).toBe(true);
        expect(harness.capturedReplyOptions?.allowToolLifecycleWhenProgressHidden).toBe(true);
      }
      expect(statusReactionControllerMock.setDone).toHaveBeenCalledTimes(toolOnly ? 1 : 0);
      if (!ambient) {
        expect(statusReactionControllerMock.restoreInitial).toHaveBeenCalledTimes(1);
      }
      if (toolOnly || failed) {
        const terminal = failed
          ? statusReactionControllerMock.setError
          : statusReactionControllerMock.setDone;
        expect(terminal).toHaveBeenCalledTimes(1);
        expect(terminal.mock.invocationCallOrder[0]).toBeLessThan(
          statusReactionControllerMock.restoreInitial.mock.invocationCallOrder[0] ?? 0,
        );
      }
      if (failed) {
        expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
        expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
        expect(stopSlackStreamMock).toHaveBeenCalledTimes(1);
        expect(collectNativeTaskUpdates().at(-1)).toEqual(
          expect.objectContaining({ status: "error" }),
        );
      } else if (scenario === "reasoning-only") {
        expect(deliverRepliesMock).not.toHaveBeenCalled();
      }
    },
  );

  it("finalizes a labeled session card separately from final text", async () => {
    const draftStream = useDraftStream();
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);
    harness.mockedSlackStreamingMode = "progress";
    harness.mockedReplyOptionEvents = [
      { kind: "item", progressText: "tool one" },
      { kind: "partial", text: "partial answer" },
      { kind: "item", progressText: "tool two" },
    ];
    await dispatch({
      cfg: {
        gateway: { publicOrigin: "https://team.openclaw.ai", controlUi: { basePath: "/openclaw" } },
      },
      accountConfig: { streaming: { progress: { toolProgress: true, label: "Shelling" } } },
    });
    const title = { type: "section", text: { type: "plain_text", text: "Shelling", emoji: false } };
    expect(draftStream.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        text: "Shelling\n\n_tool one_\n_tool two_\n\n1s",
        blocks: expect.arrayContaining([title]),
      }),
    );
    expect(finalizeSlackPreviewEditMock).toHaveBeenCalledTimes(1);
    expectMockCallArgFields(finalizeSlackPreviewEditMock, 0, {
      channelId: "C123",
      messageId: "171234.567",
    });
    const finalEdit = requireRecord(
      requireMockCall(finalizeSlackPreviewEditMock, 0, "session card final edit")[0],
      "session card final edit",
    );
    expect(finalEdit.blocks).toContainEqual(title);
    expect(JSON.stringify(finalEdit.blocks)).toContain("Open in OpenClaw");
    expect(JSON.stringify(finalEdit.blocks)).toContain(
      "https://team.openclaw.ai/openclaw/chat/agent-1/slack/C123",
    );
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
    expect(draftStream.clear).not.toHaveBeenCalled();
    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
  });

  it.each(["dispatch", "reply"])("settles a working card after failed %s", async (source) => {
    const draftStream = useDraftStream();
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);
    harness.mockedSlackStreamingMode = "progress";
    const payload = { text: "tool failed", isError: true };
    harness.mockedReplyOptionEvents = [{ kind: "item", progressText: "working" }];
    harness.mockedDispatchSequence = source === "dispatch" ? [] : [{ kind: "final", payload }];
    if (source === "dispatch") {
      harness.mockedDispatchError = new Error("agent dispatch failed");
    }
    const result = dispatch({
      accountConfig: { streaming: { mode: "progress", progress: { toolProgress: true } } },
    });
    if (source === "dispatch") {
      await expect(result).rejects.toThrow("agent dispatch failed");
    } else {
      await result;
      expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
      expect(deliverRepliesMock).toHaveBeenCalledWith(
        expect.objectContaining({ replies: [payload] }),
      );
    }
    expect(finalizeSlackPreviewEditMock).toHaveBeenCalledTimes(1);
    const finalEdit = requireRecord(
      requireMockCall(finalizeSlackPreviewEditMock, 0, "failed card edit")[0],
      "failed card edit",
    );
    expect(finalEdit.text).toBe("Failed\n\n_working_");
    expect(finalEdit.blocks).toEqual([
      { type: "section", text: { type: "plain_text", text: "Failed", emoji: false } },
      { type: "section", text: { type: "mrkdwn", text: "_working_" } },
    ]);
    expect(JSON.stringify(finalEdit.blocks)).not.toMatch(/\p{Extended_Pictographic}/u);
    expect(draftStream.clear).not.toHaveBeenCalled();
  });

  it("posts and seals a failure card for a quiet failed turn without a reply", async () => {
    const { createSlackDraftStream } =
      await vi.importActual<typeof import("../../draft-stream.js")>("../../draft-stream.js");
    let draftStream: ReturnType<typeof createSlackDraftStream> | undefined;
    const edit = vi.fn(noopAsync);
    const remove = vi.fn(noopAsync);
    createSlackDraftStreamMock.mockImplementationOnce(
      (params: Parameters<typeof createSlackDraftStream>[0]) => {
        draftStream = createSlackDraftStream({ ...params, edit, remove });
        return draftStream;
      },
    );
    sendMessageSlackMock.mockResolvedValue(normalDeliveryResult);
    finalizeSlackPreviewEditMock.mockResolvedValue(undefined);
    harness.mockedSlackStreamingMode = "progress";
    harness.mockedAgentRunTerminalOutcome = "failed";
    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [
      { kind: "tool_start", itemId: "tool-1", name: "bash", phase: "start" },
      checkpoint(async () => {
        await draftStream?.flush();
        expect(sendMessageSlackMock).not.toHaveBeenCalled();
      }),
    ];

    await dispatch({
      accountConfig: { streaming: { mode: "progress", progress: { style: "card" } } },
    });

    const blocks = [
      { type: "section", text: { type: "plain_text", text: "Failed", emoji: false } },
    ];
    expect(sendMessageSlackMock).toHaveBeenCalledExactlyOnceWith(
      expect.any(String),
      "Failed",
      expect.objectContaining({ blocks }),
    );
    expect(finalizeSlackPreviewEditMock).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ text: "Failed", blocks, messageId: "normal-final" }),
    );
    expect(draftStream?.messageId()).toBe("normal-final");
    // Sealed cards ignore late updates and survive dispatch cleanup.
    draftStream?.update("late tool activity");
    await draftStream?.flush();
    expect(sendMessageSlackMock).toHaveBeenCalledTimes(1);
    expect(edit).not.toHaveBeenCalled();
    expect(remove).not.toHaveBeenCalled();

    expect(deliverRepliesMock).not.toHaveBeenCalled();
  });

  it("keeps a failed no-reply card but deletes a silent successful card", async () => {
    const failedDraft = createDraftStreamStub();
    const silentDraft = createDraftStreamStub();
    createSlackDraftStreamMock.mockReturnValueOnce(failedDraft).mockReturnValueOnce(silentDraft);
    finalizeSlackPreviewEditMock.mockResolvedValue(undefined);

    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [{ kind: "item", progressText: "working" }];
    harness.mockedAgentRunTerminalOutcome = "failed";

    await dispatch({
      accountConfig: progressAccount({ toolProgress: true }),
    });
    expect(failedDraft.clear).not.toHaveBeenCalled();
    expect(finalizeSlackPreviewEditMock.mock.calls[0]?.[0]?.blocks).toEqual([
      { type: "section", text: { type: "plain_text", text: "Failed", emoji: false } },
      { type: "section", text: { type: "mrkdwn", text: "_working_" } },
    ]);

    finalizeSlackPreviewEditMock.mockClear();
    harness.mockedAgentRunTerminalOutcome = "completed";
    await dispatch({
      accountConfig: progressAccount({ toolProgress: true }),
    });
    expect(silentDraft.clear).toHaveBeenCalledTimes(1);
    expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
  });

  it("batches native milestone updates and prevents pending updates after final delivery", async () => {
    vi.useFakeTimers();
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      events: [
        {
          kind: "plan",
          phase: "update",
          explanation: "Checking the workspace",
          steps: [{ step: "Inspect", status: "in_progress" }],
        },
        {
          kind: "plan",
          phase: "update",
          explanation: "Checking the workspace",
          steps: [{ step: "Intermediate", status: "in_progress" }],
        },
        {
          kind: "plan",
          phase: "update",
          explanation: "Checking the workspace",
          steps: [{ step: "Run tests", status: "in_progress" }],
        },
        checkpoint(async () => {
          expect(startSlackStreamMock).toHaveBeenCalledOnce();
          expect(appendSlackStreamMock).not.toHaveBeenCalled();
          await vi.advanceTimersByTimeAsync(999);
          expect(appendSlackStreamMock).not.toHaveBeenCalled();
          await vi.advanceTimersByTimeAsync(1);
          expectNativeProgressAppend(0, [taskUpdate("plan_step_1", "Run tests", "in_progress")]);
        }),
        {
          kind: "plan",
          phase: "update",
          explanation: "Checking the workspace",
          steps: [{ step: "Final checks", status: "in_progress" }],
        },
      ],
    });
    expect(collectNativeTaskUpdates()).toEqual([
      taskUpdate("plan_step_1", "Inspect", "in_progress"),
      taskUpdate("plan_step_1", "Run tests", "in_progress"),
      taskUpdate("plan_step_1", "Final checks", "complete"),
    ]);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
    const appendCount = appendSlackStreamMock.mock.calls.length;
    await vi.advanceTimersByTimeAsync(2_000);
    expect(appendSlackStreamMock).toHaveBeenCalledTimes(appendCount);
    expect(stopSlackStreamMock).toHaveBeenCalledOnce();
  });

  it("flushes approval attention immediately while ordinary progress is batched", async () => {
    vi.useFakeTimers();
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { style: "card", toolProgress: false, nativeTaskCards: true },
      events: [
        { kind: "tool_start", phase: "start", name: "bash" },
        { kind: "approval", phase: "requested", approvalId: "approval-1", command: "run checks" },
        checkpoint(async () => {
          expectNativeProgressAppend(0, [
            taskUpdate(
              expect.stringMatching(/^openclaw-attention-/u),
              "Approval required: run checks; approval requested",
              "pending",
            ),
          ]);
        }),
        { kind: "approval", phase: "resolved", approvalId: "approval-1" },
      ],
    });
    expect(
      collectNativeTaskUpdates().filter(
        (task) => typeof task.id === "string" && task.id.startsWith("openclaw-attention-"),
      ),
    ).toEqual([
      taskUpdate(
        expect.stringMatching(/^openclaw-attention-/u),
        "Approval required: run checks; approval requested",
        "pending",
      ),
      taskUpdate(
        expect.stringMatching(/^openclaw-attention-/u),
        "Approval required: run checks; approval requested",
        "complete",
      ),
    ]);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
  });

  it("keeps intermediate command failures out of quiet native streams", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
      progress: { style: "card", toolProgress: false, nativeTaskCards: true },
      events: [
        {
          kind: "plan",
          phase: "update",
          explanation: "Checking the workspace",
          steps: [{ step: "Run checks", status: "in_progress" }],
        },
        { kind: "command_output", phase: "end", name: "Bash", title: "run checks", exitCode: 1 },
        {
          kind: "command_output",
          phase: "end",
          name: "Bash",
          title: "retry checks",
          exitCode: 8,
        },
      ],
    });

    const outgoing = JSON.stringify([
      ...startSlackStreamMock.mock.calls,
      ...appendSlackStreamMock.mock.calls,
      ...stopSlackStreamMock.mock.calls,
    ]);
    expect(outgoing).not.toMatch(/Bash|exit [18]|Recovered:/u);
    expect(collectNativeTaskUpdates()).toEqual([
      taskUpdate("plan_step_1", "Run checks", "in_progress"),
      taskUpdate("plan_step_1", "Run checks", "complete"),
    ]);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
  });

  it.each([false, true])(
    "continues a buffered native progress stream (final=%s)",
    async (withFinal) => {
      const session = { ...createNativeStreamSession(), delivered: false };
      if (withFinal) {
        startSlackStreamMock.mockResolvedValue(session);
      } else {
        startSlackStreamMock.mockResolvedValueOnce(session);
        appendSlackStreamMock.mockImplementationOnce(async () => {
          session.delivered = true;
        });
      }
      await dispatchNativeProgressScenario({
        ...(withFinal ? { finalPayload: { text: FINAL_REPLY_TEXT } } : {}),
        events: withFinal
          ? [{ kind: "item", progressText: "slow tool" }]
          : [
              { kind: "item", itemId: "item-1", progressText: "still working" },
              { kind: "item", itemId: "item-1", progressText: "still working" },
            ],
      });
      if (withFinal) {
        expect(deliverRepliesMock).not.toHaveBeenCalled();
        expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
      } else {
        expect(startSlackStreamMock).toHaveBeenCalledOnce();
        expect(appendSlackStreamMock).toHaveBeenCalledOnce();
        expect(session.delivered).toBe(true);
      }
    },
  );

  it.each(["stop-delivered", "fallback-failed", "oversized-fallback"] as const)(
    "settles definitely rejected native text through %s",
    async (outcome) => {
      harness.mockedNativeStreaming = true;
      const nativeStopSucceeds = outcome === "stop-delivered";
      const oversized = outcome === "oversized-fallback";
      const text = oversized ? "x".repeat(8500) : "rejected reply";
      harness.mockedDispatchSequence = [
        { kind: "final", payload: { text: "already visible" } },
        { kind: "final", payload: { text } },
      ];
      const session = createNativeStreamSession();
      const rejection = new TestSlackStreamNotDeliveredError(
        text,
        oversized ? "team_not_found" : "user_not_found",
      );
      const sendError = new Error("fallback send failed");
      startSlackStreamMock.mockResolvedValueOnce(session);
      appendSlackStreamMock.mockImplementationOnce(async () => {
        session.pendingText = text;
        throw rejection;
      });
      if (nativeStopSucceeds) {
        stopSlackStreamMock.mockImplementationOnce(async () => {
          session.pendingText = "";
          return { messageId: STREAM_MESSAGE_TS };
        });
      } else {
        stopSlackStreamMock.mockRejectedValueOnce(rejection);
        if (!oversized) {
          deliverRepliesMock.mockRejectedValueOnce(sendError);
        }
      }
      const result = await dispatch().catch((error: unknown) => error);
      expect(result).toBe(outcome === "fallback-failed" ? sendError : undefined);
      expect(deliverRepliesMock).toHaveBeenCalledTimes(nativeStopSucceeds ? 0 : 1);
      if (oversized) {
        expect(postMessageMock).not.toHaveBeenCalled();
        expectDeliverReplyCall(0, text, { textLimit: 4000 });
        expect(session.stopped).toBe(true);
      } else {
        expect(stopSlackStreamMock).toHaveBeenCalledOnce();
        expect(emitSlackMessageSentHooksMock).toHaveBeenCalledTimes(2);
        expectMockCallArgFields(emitSlackMessageSentHooksMock, 0, {
          content: "already visible",
          success: true,
          messageId: STREAM_MESSAGE_TS,
        });
        expectMockCallArgFields(emitSlackMessageSentHooksMock, 1, {
          content: text,
          success: nativeStopSucceeds,
          ...(nativeStopSucceeds ? { messageId: STREAM_MESSAGE_TS } : { error: sendError.message }),
        });
      }
    },
  );

  it.each<{
    name: string;
    events: SlackReplyOptionEvent[];
    updates: unknown[];
    progress?: Parameters<typeof dispatchNativeProgressScenario>[0]["progress"];
    tasks?: unknown[];
    completion?: unknown[];
    finalInStream?: boolean;
  }>([
    {
      name: "typed plan steps",
      events: [
        {
          kind: "plan",
          phase: "update",
          explanation: "Executing the checklist.",
          steps: [
            { step: "Inspect", status: "completed" },
            { step: "Patch", status: "in_progress" },
            { step: "Test", status: "pending" },
          ],
        },
      ],
      updates: [
        planUpdate("Executing the checklist."),
        taskUpdate("plan_step_1", "Inspect", "complete"),
        taskUpdate("plan_step_2", "Patch", "in_progress"),
        taskUpdate("plan_step_3", "Test", "pending"),
      ],
    },
    {
      name: "deduplicated preamble and prepared note",
      events: [
        preamble("Checking results", "preamble-1"),
        {
          kind: "plan",
          phase: "update",
          ...projectProgressCardChannelUpdate({ markdown: "**Checking** results" }),
          steps: [],
        },
      ],
      updates: [
        planUpdate("Checking results"),
        taskUpdate(expect.any(String), "Update Plan", "in_progress", {
          details: "Checking results",
        }),
      ],
    },
    {
      name: "configured command truncation",
      progress: { label: "Shelling", maxLineChars: 12, nativeTaskCards: true, commandText: "raw" },
      events: [
        {
          kind: "tool_start",
          itemId: "exec-call-1",
          toolCallId: "tool-call-1",
          name: "bash",
          phase: "start",
          args: { command: "1234567890abcdefghijklmnopqrstuvwxyz" },
        },
      ],
      updates: [
        planUpdate("Shelling"),
        taskUpdate(expect.stringMatching(/^exec_call_1_[a-f0-9]{8}$/), "Bash", "in_progress", {
          details: "12345…uvwxyz",
        }),
      ],
      completion: [
        taskUpdate(expect.stringMatching(/^exec_call_1_[a-f0-9]{8}$/), "Bash", "complete"),
      ],
    },
    {
      name: "patch item identity",
      events: [
        {
          kind: "patch",
          itemId: "patch:item-1",
          toolCallId: "patch-call-1",
          name: "apply_patch",
          phase: "end",
          summary: "updated Slack progress tests",
        },
      ],
      updates: [
        planUpdate("Apply Patch — updated Slack progress tests"),
        taskUpdate(expect.stringMatching(/^patch_item_1_[a-f0-9]{8}$/), "Apply Patch", "complete", {
          details: "updated Slack progress tests",
        }),
      ],
      tasks: [
        taskUpdate(expect.stringMatching(/^patch_item_1_[a-f0-9]{8}$/), "Apply Patch", "complete", {
          details: "updated Slack progress tests",
        }),
      ],
      finalInStream: true,
    },
  ])(
    "renders native progress from $name",
    async ({ events, updates, progress, tasks, completion, finalInStream }) => {
      await dispatchNativeProgressScenario({
        finalPayload: { text: FINAL_REPLY_TEXT },
        events,
        progress,
      });
      expectNativeProgressStart(updates);
      if (tasks) {
        expect(collectNativeTaskUpdates()).toEqual(tasks);
      }
      if (completion) {
        expectNativeProgressAppend(0, completion);
      }
      if (finalInStream) {
        expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
      }
    },
  );

  it("streams rolling reasoning snapshots as deduplicated narration", async () => {
    await dispatchNativeProgressScenario({
      finalPayload: { text: FINAL_REPLY_TEXT },
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
    // A snapshot arriving inside the throttle window rides the completion append.
    expectNativeProgressAppend(0, [{ type: "markdown_text", text: " the Slack handler" }]);
    expectNativeStreamText(`\n${FINAL_REPLY_TEXT}`);
  });

  it("keeps final fallback in the planned thread when native Slack progress start fails", async () => {
    startSlackStreamMock.mockRejectedValueOnce(new Error("start stream failed"));
    harness.mockedReplyThreadTsSequence = [THREAD_TS, undefined];

    await dispatchNativeProgressScenario({
      replyToMode: "first",
      finalPayload: { text: FINAL_REPLY_TEXT },
      events: [{ kind: "item", progressText: "slow tool" }],
    });

    expect(startSlackStreamMock).toHaveBeenCalledTimes(1);
    expect(appendSlackStreamMock).not.toHaveBeenCalled();
    expect(stopSlackStreamMock).not.toHaveBeenCalled();
    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
  });

  it("settles a failed native progress rotation before starting the queued turn", async () => {
    const firstSession = createNativeStreamSession();
    const secondSession = { ...firstSession };
    startSlackStreamMock.mockResolvedValueOnce(firstSession).mockResolvedValueOnce(secondSession);
    stopSlackStreamMock
      .mockRejectedValueOnce(new Error("socket reset"))
      .mockResolvedValueOnce({ messageId: "171234.702" });
    finalizeSlackPreviewEditMock.mockResolvedValue(undefined);
    harness.mockedNativeStreaming = true;

    harness.mockedReplyOptionEvents = [{ kind: "item", progressText: "first tool" }];
    harness.mockedDispatchSequence = [
      { kind: "final", payload: { text: "same answer" } },
      { kind: "queued_followup" },
      { kind: "item", progressText: "queued tool" },
      { kind: "final", payload: { text: "same answer" } },
    ];

    await dispatch({
      accountConfig: progressAccount({ toolProgress: true, nativeTaskCards: true }),
    });

    expect(startSlackStreamMock).toHaveBeenCalledTimes(2);
    expect(stopSlackStreamMock).toHaveBeenCalledTimes(2);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
    expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
    expectNativeStreamText("\nsame answer", 2);
    expectMockCallArgFields(stopSlackStreamMock, 0, {
      session: firstSession,
    });
    expectMockCallArgFields(stopSlackStreamMock, 1, {
      session: secondSession,
    });
    expect(stopSlackStreamMock.mock.invocationCallOrder[0]).toBeLessThan(
      startSlackStreamMock.mock.invocationCallOrder[1] ?? 0,
    );
  });

  it("completes a native Slack progress plan even when no final text is sent", async () => {
    await dispatchNativeProgressScenario({
      events: [{ kind: "concurrent_items", progressTexts: ["tool one", "tool two", "tool three"] }],
    });

    expectNativeProgressStart([
      planUpdate("tool three"),
      taskUpdate(contentTaskId("item"), "tool one", "in_progress"),
      taskUpdate(contentTaskId("item"), "tool two", "in_progress"),
      taskUpdate(contentTaskId("item"), "tool three", "in_progress"),
    ]);
    expect(appendSlackStreamMock).not.toHaveBeenCalled();
    expect(deliverRepliesMock).not.toHaveBeenCalled();
    expectMockCallArgFields(stopSlackStreamMock, 0, {
      chunks: [
        taskUpdate(contentTaskId("item"), "tool one", "complete"),
        taskUpdate(contentTaskId("item"), "tool two", "complete"),
        taskUpdate(contentTaskId("item"), "tool three", "complete"),
      ],
    });
  });

  it("re-arms an isolated progress draft on an assistant boundary after final delivery", async () => {
    const draftStream = useDraftStream();
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);

    harness.mockedReplyOptionEvents = [{ kind: "item", progressText: "first turn" }];

    await dispatch({
      accountConfig: progressAccount(),
    });
    await harness.capturedReplyOptions?.onAssistantMessageStart?.();
    await requireCapturedItemEventHandler()({ progressText: "second turn" });

    expect(draftStream.forceNewMessage).toHaveBeenCalledTimes(1);
    expect(finalizeSlackPreviewEditMock.mock.invocationCallOrder.at(-1)).toBeLessThan(
      draftStream.forceNewMessage.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
    );
    expectLastDraftUpdateText(draftStream, "Working\n\n_second turn_\n\n1s");
  });

  it.each([false, true])("clears settled queued progress (quiet=%s)", async (quiet) => {
    const draftStream = useDraftStream();
    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = quiet ? [] : [{ kind: "item", progressText: "silent turn" }];
    await dispatch({
      accountConfig: progressAccount(
        quiet ? { label: false, commentary: true, toolProgress: false, maxLines: 1 } : undefined,
      ),
    });
    await harness.capturedReplyOptions?.onQueuedFollowupAdmitted?.();
    await requireCapturedItemEventHandler()(
      quiet
        ? { kind: "preamble", itemId: "queued-preamble", progressText: "Checking the followup" }
        : { progressText: "queued turn" },
    );
    if (quiet) {
      expectLastDraftUpdateText(draftStream, "_Checking the followup_");
    } else {
      expect(draftStream.forceNewMessage).toHaveBeenCalledTimes(1);
    }
    const clearCalls = draftStream.clear.mock.calls.length;
    const dropCalls = draftStream.dropDetachedMessages.mock.calls.length;
    await harness.capturedReplyOptions?.onQueuedFollowupSettled?.();
    expect(draftStream.clear).toHaveBeenCalledTimes(clearCalls + 1);
    expect(draftStream.clear.mock.invocationCallOrder.at(-1)).toBeGreaterThan(
      draftStream.update.mock.invocationCallOrder.at(-1) ?? Number.POSITIVE_INFINITY,
    );
    if (quiet) {
      expect(deliverRepliesMock).not.toHaveBeenCalled();
    } else {
      expectLastDraftUpdateText(draftStream, "Working\n\n_queued turn_\n\n1s");
      expect(draftStream.dropDetachedMessages).toHaveBeenCalledTimes(dropCalls + 1);
      expect(draftStream.dropDetachedMessages.mock.invocationCallOrder.at(-1)).toBeGreaterThan(
        draftStream.clear.mock.invocationCallOrder.at(-1) ?? Number.POSITIVE_INFINITY,
      );
    }
  });

  it.each([
    { humanReply: false, messageToolReply: false, delayedReceipt: false, silent: false },
    { humanReply: true, messageToolReply: false, delayedReceipt: false, silent: false },
    { humanReply: true, messageToolReply: true, delayedReceipt: true, silent: false },
    { humanReply: true, messageToolReply: false, delayedReceipt: false, silent: true },
  ])(
    "settles interrupted previews without losing human context (human=$humanReply, message tool=$messageToolReply, delayed receipt=$delayedReceipt, silent=$silent)",
    async ({ humanReply, messageToolReply, delayedReceipt, silent }) => {
      harness.mockedSlackStreamingMode = "partial";
      harness.mockedDispatchSequence =
        messageToolReply || silent ? [] : [{ kind: "final", payload: { text: FINAL_REPLY_TEXT } }];
      harness.mockedSourceReplyDelivered = messageToolReply;
      const { createSlackDraftStream } =
        await vi.importActual<typeof import("../../draft-stream.js")>("../../draft-stream.js");
      const { noteSlackDraftConversationMessage } =
        await import("../../draft-message-boundaries.js");
      const visibleMessages = new Map<string, string>();
      let nextMessageId = 100;
      let draftStream: ReturnType<typeof createSlackDraftStream> | undefined;
      let noteHumanReply = () => {};
      let releaseReceipt!: () => void;
      const receipt = new Promise<void>((resolve) => {
        releaseReceipt = resolve;
      });
      let closeoutStarted = false;
      let pendingFlush: Promise<void> | undefined;
      createSlackDraftStreamMock.mockImplementationOnce(
        (params: Parameters<typeof createSlackDraftStream>[0]) => {
          draftStream = createSlackDraftStream({
            ...params,
            send: async (_target, text) => {
              const messageId = String(nextMessageId++);
              visibleMessages.set(messageId, text);
              if (delayedReceipt) {
                await receipt;
              }
              return {
                channelId: "C123",
                messageId,
                receipt: createMessageReceiptFromOutboundResults({
                  results: [{ channel: "slack", channelId: "C123", messageId }],
                  kind: "preview",
                }),
              };
            },
            edit: async (_channelId, messageId, text) => {
              visibleMessages.set(messageId, text);
            },
            remove: async (_channelId, messageId) => {
              visibleMessages.delete(messageId);
            },
          });
          const discardPending = draftStream.discardPending;
          draftStream.discardPending = () => {
            closeoutStarted = true;
            return discardPending();
          };
          noteHumanReply = () =>
            noteSlackDraftConversationMessage({
              accountId: params.accountId,
              teamId: params.eventScope?.teamId,
              channelId: "C123",
              threadTs: params.resolveThreadTs?.(),
              messageTs: String(nextMessageId++),
              userId: "U_HUMAN",
            });
          return draftStream;
        },
      );
      finalizeSlackPreviewEditMock.mockImplementationOnce(async (input) => {
        const edit = requireRecord(input, "final preview edit");
        visibleMessages.set(String(edit.messageId), String(edit.text));
      });
      harness.mockedReplyOptionEvents = [
        { kind: "partial", text: "I will inspect the files." },
        checkpoint(async () => {
          pendingFlush = draftStream?.flush();
          if (delayedReceipt) {
            await vi.waitFor(() => expect(visibleMessages.size).toBe(1));
          } else {
            await pendingFlush;
          }
          expect([...visibleMessages.values()]).toEqual(["I will inspect the files."]);
          if (humanReply && !delayedReceipt) {
            noteHumanReply();
          }
          if (messageToolReply) {
            visibleMessages.set("message-tool-reply", FINAL_REPLY_TEXT);
          }
        }),
        { kind: "assistant_start" },
        ...(messageToolReply || silent
          ? []
          : [{ kind: "partial" as const, text: FINAL_REPLY_TEXT }]),
      ];

      const dispatching = dispatch();
      if (delayedReceipt) {
        await vi.waitFor(() => expect(closeoutStarted).toBe(true));
        if (humanReply) {
          noteHumanReply();
        }
        releaseReceipt();
      }
      await dispatching;
      await pendingFlush;
      draftStream?.update("Late preview after final delivery");
      await draftStream?.flush();

      expect([...visibleMessages.values()]).toEqual(
        silent
          ? []
          : humanReply
            ? ["I will inspect the files.", FINAL_REPLY_TEXT]
            : [FINAL_REPLY_TEXT],
      );
    },
  );

  it("starts a new draft delivery target when a queued followup is admitted", async () => {
    const draftStream = useDraftStream();
    harness.mockedSlackStreamingMode = "partial";
    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [{ kind: "partial", text: "first reply" }];

    await dispatch({});
    await harness.capturedReplyOptions?.onQueuedFollowupAdmitted?.();

    expect(draftStream.flush).toHaveBeenCalledTimes(1);
    expect(draftStream.forceNewMessage).toHaveBeenCalledTimes(1);
  });

  it("keeps complete preambles visible between streamed updates", async () => {
    const checkpoints = vi.fn();
    let postedMessageId: string | undefined;
    const draftStream = {
      ...createDraftStreamStub(),
      messageId: () => postedMessageId,
    };
    draftStream.flush.mockImplementation(async () => {
      if (draftStream.update.mock.calls.length > 0) {
        postedMessageId = "171234.567";
      }
    });
    createSlackDraftStreamMock.mockReturnValueOnce(draftStream);
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);

    harness.mockedReplyOptionEvents = [
      preamble("I", "p1", "update"),
      checkpoint(async () => {
        // Even a timer/flush must not post the first token: Slack freezes
        // its push notification at creation, then edits do not re-notify.
        checkpoints();
        await draftStream.flush();
        expect(draftStream.update).not.toHaveBeenCalled();
        expect(postedMessageId).toBeUndefined();
      }),
      preamble("I will check the result.", "p1", "update"),
      checkpoint(async () => {
        checkpoints();
        expect(draftStream.update).not.toHaveBeenCalled();
      }),
      preamble("I will check the result.", "p1", "end"),
      checkpoint(async () => {
        checkpoints();
        expect(postedMessageId).toBe("171234.567");
        expect(draftUpdateTexts(draftStream)).toEqual(["_I will check the result._"]);
      }),
      preamble("The result", "p2", "update"),
      checkpoint(async () => {
        checkpoints();
        // A human reply can rotate the preview at this point. Keeping the
        // last complete preamble prevents an abandoned word fragment.
        expectLastDraftUpdateText(draftStream, "_I will check the result._");
        expect(draftUpdateTexts(draftStream)).toEqual(["_I will check the result._"]);
        expect(draftStream.update.mock.calls.at(-1)?.[0]).toMatchObject({
          allowNewMessage: true,
        });
      }),
      preamble("The result is ready.", "p2", "end"),
    ];

    await dispatch({
      accountConfig: progressAccount({
        label: false,
        commentary: true,
        toolProgress: false,
        maxLines: 1,
      }),
    });

    // Assert the intermediate observations ran; the final text alone cannot
    // prove that Slack never received a first-token notification.
    expect(checkpoints).toHaveBeenCalledTimes(4);
    expectLastDraftUpdateText(draftStream, "_The result is ready._");
    expect(draftStream.update.mock.calls.at(-1)?.[0]).toMatchObject({
      allowNewMessage: true,
    });
    expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
  });

  it.each([false, true])(
    "keeps the latest Slack preamble (tool progress=%s)",
    async (toolProgress) => {
      const draftStream = useDraftStream();
      harness.mockedDispatchSequence = [];
      const first = toolProgress
        ? "Checking the legacy Slack path"
        : "Checking the Slack event path";
      const latest = toolProgress ? "Keeping the released behavior" : "Preparing the smallest fix";
      harness.mockedReplyOptionEvents = [
        {
          kind: "tool_start",
          itemId: "tool-1",
          name: "bash",
          phase: "start",
          args: { command: "pnpm test" },
        },
        preamble(first, "preamble-1"),
        preamble(latest, "preamble-2"),
      ];
      await dispatch({
        accountConfig: progressAccount({
          label: false,
          ...(!toolProgress ? { commentary: true } : {}),
          toolProgress,
          maxLines: 1,
        }),
      });
      if (toolProgress) {
        expect(harness.capturedReplyOptions?.commentaryProgressEnabled).toBeUndefined();
        expect(harness.capturedReplyOptions?.commentaryPayloadsEnabled).toBeUndefined();
        expect(harness.capturedReplyOptions?.shouldDeliverCommentaryPayloads).toBeUndefined();
        expect(harness.capturedReplyOptions?.onVerboseProgressVisibilityAsync).toBeUndefined();
        expect(harness.capturedReplyOptions?.progressPreambleEnabled).toBe(true);
        expectLastDraftUpdateText(draftStream, `_${latest}_\n\nBash — running\n\n1 tool · 1s`);
      } else {
        expect(harness.capturedReplyOptions?.commentaryProgressEnabled).toBe(true);
        expect(harness.capturedReplyOptions?.commentaryPayloadsEnabled).toBe(true);
        expect(harness.capturedReplyOptions?.shouldDeliverCommentaryPayloads?.()).toBe(false);
        expect(harness.capturedReplyOptions?.suppressDefaultToolProgressMessages).toBe(true);
        expectLastDraftUpdateText(draftStream, `_${latest}_`);
        expect(draftUpdateTexts(draftStream).join("\n")).not.toContain("pnpm test");
        const updateCount = draftStream.update.mock.calls.length;
        await harness.capturedReplyOptions?.onVerboseProgressVisibilityAsync?.(async () => true);
        expect(harness.capturedReplyOptions?.shouldDeliverCommentaryPayloads?.()).toBe(true);
        await requireCapturedItemEventHandler()({
          kind: "preamble",
          itemId: "preamble-3",
          progressText: "Delivered by the verbose lane",
        });
        expect(draftStream.update).toHaveBeenCalledTimes(updateCount);
      }
    },
  );

  it("escapes Slack mentions and renders commentary without losing outer italics or inline code", async () => {
    const { normalizeSlackOutboundText } =
      await vi.importActual<typeof import("../../format.js")>("../../format.js");
    const draftStream = useDraftStream();

    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [
      preamble(
        "checking <@U123> in <#C123> and <!channel> with *urgent* _context_ `src/one.ts` & Linux x86_64",
        "preamble-1",
      ),
    ];

    await normalizeSlackOutboundTextMock.withImplementation(
      normalizeSlackOutboundText,
      async () => {
        await dispatch({
          accountConfig: progressAccount({ label: false, commentary: true, toolProgress: false }),
        });
      },
    );

    expectLastDraftUpdateText(
      draftStream,
      "_checking &lt;@U123&gt; in &lt;#C123&gt; and &lt;!channel&gt; with urgent context `src/one.ts` &amp; Linux x86_64_",
    );
  });

  it("keeps only preambles through reasoning and failed tools by default", async () => {
    const draftStream = useDraftStream();
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);

    harness.mockedReplyOptionEvents = [
      checkpoint(async () => {
        if (!harness.capturedReplyOptions) {
          throw new Error("expected Slack reply options");
        }
        await emitCompactProgressScenario(harness.capturedReplyOptions);
      }),
    ];

    await dispatch({
      accountConfig: progressAccount({
        nativeTaskCards: true,
        label: false,
        toolProgress: false,
        maxLines: 1,
      }),
    });

    expect(createSlackDraftStreamMock).toHaveBeenCalledTimes(1);
    expect(startSlackStreamMock).not.toHaveBeenCalled();
    expect(appendSlackStreamMock).not.toHaveBeenCalled();
    expect(stopSlackStreamMock).not.toHaveBeenCalled();
    expect(
      draftStream.update.mock.calls.every(
        ([update]) => typeof update === "string" || !("blocks" in update),
      ),
    ).toBe(true);
    expect(draftUpdateTexts(draftStream)).toEqual([
      "Checking the current Slack behavior.",
      "The fix is ready; I’m checking the result.",
    ]);
    expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
    expect(deliverRepliesMock).toHaveBeenCalledOnce();
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
    expect(draftStream.discardPending.mock.invocationCallOrder[0]).toBeLessThan(
      deliverRepliesMock.mock.invocationCallOrder[0]!,
    );
    expect(deliverRepliesMock.mock.invocationCallOrder[0]).toBeLessThan(
      draftStream.clear.mock.invocationCallOrder[0]!,
    );
  });

  it("publishes compact media finals before clearing the preview after a suppressed send", async () => {
    const draftStream = useDraftStream();

    const payload = { text: "The fix works.", mediaUrl: "https://example.com/demo.mp4" };
    harness.mockedDispatchSequence = [
      { kind: "final", payload },
      { kind: "final", payload },
    ];
    deliverRepliesMock.mockResolvedValueOnce(undefined);
    deliverRepliesMock.mockImplementationOnce(async () => {
      expect(draftStream.clear).not.toHaveBeenCalled();
      return normalDeliveryResult;
    });

    await dispatch({
      accountConfig: progressAccount({ style: "compact" }),
    });

    expect(deliverRepliesMock).toHaveBeenCalledTimes(2);
    expect(deliverRepliesMock).toHaveBeenCalledWith(
      expect.objectContaining({ replies: [payload], replyThreadTs: THREAD_TS }),
    );
    expect(draftStream.clear).toHaveBeenCalled();
    expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
  });

  it("retracts and resumes Slack plans while retaining other block progress", async () => {
    const mode = "block";

    const draftStream = useDraftStream();
    harness.mockedSlackStreamingMode = mode;
    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [
      { kind: "plan", phase: "update", steps: [{ step: "Inspect", status: "in_progress" }] },
      { kind: "assistant_start" },
      { kind: "plan", phase: "update", steps: [] },
      checkpoint(async () => {
        expect(draftStream.clear).toHaveBeenCalledTimes(1);
        expect(draftStream.forceNewMessage).toHaveBeenCalledTimes(1);
      }),
      { kind: "plan", phase: "update", steps: [{ step: "Resume", status: "in_progress" }] },
      { kind: "assistant_start" },
      {
        kind: "item",
        itemId: "card-rejected",
        itemKind: "tool",
        name: "progress_card",
        phase: "end",
        status: "blocked",
      },
      { kind: "assistant_start" },
      { kind: "item", itemId: "independent", progressText: "Independent work" },
      checkpoint(async () => {
        const text = draftUpdateTexts(draftStream).at(-1);
        expect(text).toContain("Independent work");
        expect(text).toContain("blocked");
        expect(text).toContain("▸ Resume");
      }),
      { kind: "assistant_start" },
      { kind: "plan", phase: "update", steps: [] },
      checkpoint(async () => {
        const text = draftUpdateTexts(draftStream).at(-1);
        expect(text).toContain("Independent work");
        expect(text).toContain("blocked");
        expect(text).not.toContain("Resume");
        expect(draftStream.clear).toHaveBeenCalledTimes(1);
      }),
    ];

    await dispatch({
      accountConfig: { streaming: { mode, progress: { label: false } } },
    });
  });

  it("keeps one partial preview across reasoning and tool boundaries", async () => {
    const draftStream = useDraftStream();
    harness.mockedSlackStreamingMode = "partial";
    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [
      { kind: "reasoning", text: "Checking the first path" },
      { kind: "reasoning_end" },
      { kind: "item", progressText: "ran <!here> <@U123> *bold* `code` & done" },
      { kind: "assistant_start" },
      { kind: "reasoning", text: "Checking the second path" },
      { kind: "reasoning_end" },
      { kind: "item", progressText: "tool two" },
      { kind: "assistant_start" },
      { kind: "partial", text: "final answer" },
    ];

    await dispatch({
      accountConfig: {
        streaming: { mode: "partial", progress: { label: false } },
      },
    });

    expect(draftUpdateTexts(draftStream).join("\n")).toContain(
      "ran &lt;!here&gt; &lt;@U123&gt; *bold* `code` &amp; done",
    );
    expect(draftStream.forceNewMessage).not.toHaveBeenCalled();
    expect(draftStream.update).toHaveBeenLastCalledWith("final answer");
  });

  it("resolves and caches the native stream recipient team per enterprise client", async () => {
    harness.mockedNativeStreaming = true;
    const usersInfo = vi.fn(async () => ({ user: { team_id: "T_RECIPIENT" } }));
    const eventClient = {
      chat: { postMessage: postMessageMock, update: chatUpdateMock },
      users: { info: usersInfo },
    };
    const eventScope = {
      teamId: "T_ENTERPRISE",
      client: eventClient,
    };

    await dispatch({ eventScope });
    await dispatch({ eventScope });

    expect(usersInfo).toHaveBeenCalledTimes(1);
    expect(usersInfo).toHaveBeenCalledWith({ token: "xoxb-test", user: "U123" });
    expectMockCallArgFields(startSlackStreamMock, 0, {
      client: eventClient,
      teamId: "T_RECIPIENT",
    });
    expectMockCallArgFields(startSlackStreamMock, 1, {
      client: eventClient,
      teamId: "T_RECIPIENT",
    });
  });

  it("suppresses reasoning payloads during a definite stream-rejection fallback", async () => {
    harness.mockedNativeStreaming = true;
    harness.mockedDispatchSequence = [
      { kind: "block", payload: { text: "Let me analyze...", isReasoning: true } },
      { kind: "final", payload: { text: FINAL_REPLY_TEXT } },
    ];
    startSlackStreamMock.mockRejectedValueOnce(
      new TestSlackStreamNotDeliveredError(FINAL_REPLY_TEXT, "missing_scope"),
    );

    await dispatch();

    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
    expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
  });

  it("keeps same-content tool and final payloads distinct after preview fallback", async () => {
    harness.mockedDispatchSequence = [
      { kind: "tool", payload: { text: SAME_TEXT } },
      { kind: "final", payload: { text: SAME_TEXT } },
    ];

    await dispatch();

    expect(finalizeSlackPreviewEditMock).toHaveBeenCalledTimes(1);
    expect(deliverRepliesMock).toHaveBeenCalledTimes(2);
    expectDeliverReplyCall(0, SAME_TEXT);
    expectDeliverReplyCall(1, SAME_TEXT);
  });

  it("keeps multi-part block replies in the first reply thread after the plan is consumed", async () => {
    harness.mockedReplyThreadTsSequence = [THREAD_TS, undefined];
    harness.mockedDispatchSequence = [
      { kind: "block", payload: { text: "first block" } },
      { kind: "block", payload: { text: "second block" } },
    ];

    await dispatch({
      replyToMode: "first",
    });

    expect(deliverRepliesMock).toHaveBeenCalledTimes(2);
    expectDeliverReplyCall(0, "first block");
    expectDeliverReplyCall(1, "second block");
  });

  it("preserves normal final delivery when stale-preview cleanup fails", async () => {
    const draftStream = createDraftStreamStub();
    draftStream.clear.mockRejectedValueOnce(new Error("preview cleanup failed"));
    createSlackDraftStreamMock.mockReturnValueOnce(draftStream);
    harness.mockedDispatchSequence = [
      {
        kind: "final",
        payload: { text: "Photo", mediaUrl: "https://example.com/a.png" },
      },
    ];

    await dispatch();

    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
  });

  it.each(["interrupted", "oversized", "already-visible", "chart"] as const)(
    "preserves TTS text and media through %s preview fallback",
    async (scenario) => {
      let messageId: string | undefined = "171234.567";
      const draftStream = {
        ...createDraftStreamStub(),
        messageId: () => messageId,
        channelId: () => (messageId ? "C123" : undefined),
      };
      createSlackDraftStreamMock.mockReturnValueOnce(draftStream);
      const spokenText = scenario === "oversized" ? "界".repeat(1_334) : "Spoken answer";
      const previouslyVisible = scenario === "already-visible";
      if (scenario === "interrupted") {
        draftStream.flush.mockImplementation(async () => {
          messageId = undefined;
        });
      } else if (scenario === "oversized") {
        finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);
      } else {
        harness.mockedReplyThreadTsSequence = [undefined];
      }
      if (scenario === "chart") {
        harness.mockedSlackReplyBlocks = [
          { type: "section", text: { type: "mrkdwn", text: "Spoken answer", verbatim: true } },
          {
            type: "data_visualization",
            title: "Revenue",
            chart: {
              type: "bar",
              series: [
                {
                  name: "<@U123>",
                  data: [
                    { label: "Q1", value: 12 },
                    { label: "Q2", value: 18 },
                  ],
                },
              ],
              axis_config: { categories: ["Q1", "Q2"] },
            },
          },
        ];
      }
      const payload = {
        ...ttsPayload(spokenText, previouslyVisible),
        ...(scenario === "chart"
          ? {
              presentation: {
                blocks: [
                  {
                    type: "chart",
                    chartType: "bar",
                    title: "Revenue",
                    categories: ["Q1", "Q2"],
                    series: [{ name: "<@U123>", values: [12, 18] }],
                  },
                ],
              },
            }
          : {}),
      };
      harness.mockedDispatchSequence = [{ kind: "final", payload }];
      await dispatch();
      expect(delivered().replies).toEqual([{ ...payload, text: spokenText }]);
      if (scenario === "chart") {
        expectMockCallArgFields(finalizeSlackPreviewEditMock, 0, {
          text: "Spoken answer\n\nRevenue (bar chart)\n- &lt;@U123&gt;: Q1: 12; Q2: 18",
          blocks: harness.mockedSlackReplyBlocks,
        });
        expect(normalizeSlackOutboundTextMock).not.toHaveBeenCalled();
      } else if (scenario === "already-visible") {
        expect(finalizeSlackPreviewEditMock).toHaveBeenCalledTimes(1);
        expect(draftStream.discardPending).toHaveBeenCalled();
        expectRecordFields(delivered(), { replyThreadTs: THREAD_TS });
      } else {
        expect(finalizeSlackPreviewEditMock).not.toHaveBeenCalled();
        expect(deliverRepliesMock).toHaveBeenCalledOnce();
      }
    },
  );

  it("defers hooks and suppresses duplicate TTS finals when flush creates the preview id", async () => {
    let flushed = false;
    const draftStream = {
      ...createDraftStreamStub(),
      flush: vi.fn(async () => {
        flushed = true;
      }),
      messageId: () => (flushed ? "171234.567" : undefined),
    };
    createSlackDraftStreamMock.mockReturnValueOnce(draftStream);
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);
    harness.mockedSlackIsThreadReply = false;
    harness.mockedReplyThreadTsSequence = [undefined, undefined];
    const payload = { ...ttsPayload(), text: "Spoken answer" };
    harness.mockedDispatchSequence = [
      { kind: "final", payload },
      { kind: "final", payload },
    ];

    await dispatch({
      message: { thread_ts: undefined },
      replyToMode: "first",
    });

    expect(finalizeSlackPreviewEditMock).toHaveBeenCalledTimes(1);
    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
    const delivery = delivered(0);
    expectRecordFields(delivery, { replyThreadTs: THREAD_TS });
    expect(emitSlackMessageSentHooksMock).not.toHaveBeenCalled();
  });

  it("awaits the Slack receipt for a short tool reply", async () => {
    const kind = "tool";

    harness.mockedNativeStreaming = true;
    harness.mockedDispatchSequence = [{ kind, payload: { text: "short reply" } }];
    let acknowledgedBeforeStop = false;
    startSlackStreamMock.mockImplementationOnce(async (input) => {
      const params = requireRecord(input, "stream start");
      return {
        channel: "C123",
        threadTs: THREAD_TS,
        stopped: false,
        delivered: Array.isArray(params.chunks),
        pendingText: Array.isArray(params.chunks) ? "" : "short reply",
      };
    });
    stopSlackStreamMock.mockImplementationOnce(async (input) => {
      const params = requireRecord(input, "stream stop");
      acknowledgedBeforeStop = requireRecord(params.session, "stream session").delivered === true;
      return {};
    });

    await dispatch();

    expectMockCallArgFields(startSlackStreamMock, 0, {
      text: "short reply",
      chunks: [],
    });
    expect(acknowledgedBeforeStop).toBe(true);
    expect(deliverRepliesMock).not.toHaveBeenCalled();
    expect(emitSlackMessageSentHooksMock).toHaveBeenCalledOnce();
  });

  it.each(["update", "completion"])(
    "keeps acknowledged narration successful when optional progress %s fails",
    async (phase) => {
      harness.mockedNativeStreaming = true;
      harness.mockedSlackStreamingMode = "progress";
      harness.mockedProgressEvents = phase === "completion" ? ["working"] : [];
      harness.mockedDispatchSequence = [
        { kind: "block", payload: { text: "acknowledged narration" } },
        ...(phase === "update" ? [{ kind: "item" as const, progressText: "working" }] : []),
        { kind: "final", payload: { text: FINAL_REPLY_TEXT } },
      ];
      const session = createNativeStreamSession();
      startSlackStreamMock.mockResolvedValueOnce(session);
      if (phase === "completion") {
        appendSlackStreamMock.mockResolvedValueOnce(undefined);
      }
      appendSlackStreamMock.mockImplementationOnce(async () => {
        session.stopped = true;
        throw new Error("network socket closed");
      });

      await dispatch();

      expect(stopSlackStreamMock).not.toHaveBeenCalled();
      expect(deliverRepliesMock).toHaveBeenCalledOnce();
      expectDeliverReplyCall(0, FINAL_REPLY_TEXT);
      expect(emitSlackMessageSentHooksMock).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          content: "acknowledged narration",
          success: true,
          messageId: STREAM_MESSAGE_TS,
        }),
      );
    },
  );

  it.each([true, false])(
    "preserves native Stop with an acknowledged append: %s",
    async (acknowledged) => {
      harness.mockedNativeStreaming = true;
      harness.mockedDispatchSequence = [
        { kind: "final", payload: { text: "already visible" } },
        { kind: "final", payload: { text: "stopped reply" } },
        { kind: "final", payload: { text: "after Stop" } },
      ];
      const session = {
        channel: "C123",
        threadTs: THREAD_TS,
        stopped: false,
        stoppedBySlack: false,
        delivered: true,
        pendingText: "",
      };
      startSlackStreamMock.mockResolvedValueOnce(session);
      appendSlackStreamMock.mockImplementationOnce(async () => {
        session.stopped = true;
        session.stoppedBySlack = true;
        session.pendingText = acknowledged ? "" : "stopped reply";
      });

      await dispatch();

      expect(appendSlackStreamMock).toHaveBeenCalledOnce();
      expect(stopSlackStreamMock).not.toHaveBeenCalled();
      expect(deliverRepliesMock).not.toHaveBeenCalled();
      expect(emitSlackMessageSentHooksMock).toHaveBeenCalledTimes(2);
      expectMockCallArgFields(emitSlackMessageSentHooksMock, 0, {
        content: "already visible",
        success: true,
      });
      expectMockCallArgFields(emitSlackMessageSentHooksMock, 1, {
        content: "stopped reply",
        success: acknowledged,
        ...(acknowledged ? { messageId: STREAM_MESSAGE_TS } : { error: "Stopped by Slack user" }),
      });
    },
  );

  it("preserves an acknowledged final answer when empty stream finalization loses its response", async () => {
    harness.mockedNativeStreaming = true;
    stopSlackStreamMock.mockRejectedValueOnce(new Error("network socket closed"));

    const result = await dispatch({
      cfg: { messages: { statusReactions: { enabled: true } } },
      ackReactionMessageTs: "171234.111",
      ackReactionPromise: Promise.resolve(true),
    }).catch((caught: unknown) => caught);

    expect({
      errorReactions: statusReactionControllerMock.setError.mock.calls.length,
      doneReactions: statusReactionControllerMock.setDone.mock.calls.length,
      outcome: result,
    }).toEqual({ errorReactions: 0, doneReactions: 1, outcome: undefined });
    expectMockCallArgFields(startSlackStreamMock, 0, {
      text: FINAL_REPLY_TEXT,
      chunks: [],
    });
    expect(stopSlackStreamMock).toHaveBeenCalledOnce();
    expect(deliverRepliesMock).not.toHaveBeenCalled();
    expect(emitSlackMessageSentHooksMock).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ content: FINAL_REPLY_TEXT, success: true }),
    );
  });

  it.each(["start", "append", "stop"])(
    "does not replay text after an ambiguous %s failure",
    async (operation) => {
      harness.mockedNativeStreaming = true;
      const prefix = "already visible";
      harness.mockedDispatchSequence =
        operation === "start"
          ? [{ kind: "final", payload: { text: FINAL_REPLY_TEXT } }]
          : [
              { kind: "final", payload: { text: prefix } },
              { kind: "block", payload: { text: "second acknowledged" } },
              { kind: "final", payload: { text: "failed reply" } },
            ];
      const session = createNativeStreamSession();
      const error = new Error("network socket closed");
      if (operation === "start") {
        startSlackStreamMock.mockRejectedValueOnce(error);
      } else {
        startSlackStreamMock.mockResolvedValueOnce(session);
      }
      appendSlackStreamMock.mockResolvedValueOnce(undefined).mockImplementationOnce(async () => {
        session.pendingText = "failed reply";
        if (operation === "append") {
          session.stopped = true;
          throw error;
        }
        if (operation === "stop") {
          throw new TestSlackStreamNotDeliveredError(session.pendingText, "user_not_found");
        }
      });
      stopSlackStreamMock.mockRejectedValueOnce(error);

      const result = await dispatch(
        operation === "start"
          ? undefined
          : {
              cfg: { messages: { statusReactions: { enabled: true } } },
              ackReactionMessageTs: "171234.111",
              ackReactionPromise: Promise.resolve(true),
            },
      ).catch((caught: unknown) => caught);
      if (operation === "start") {
        expect(result).toBe(error);
        expect(deliverRepliesMock).not.toHaveBeenCalled();
        expect(stopSlackStreamMock).not.toHaveBeenCalled();
        expect(postMessageMock).not.toHaveBeenCalled();
        expect(emitSlackMessageSentHooksMock).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            content: FINAL_REPLY_TEXT,
            success: false,
            error: error.message,
          }),
        );
        return;
      }
      expect({
        errorReactions: statusReactionControllerMock.setError.mock.calls.length,
        doneReactions: statusReactionControllerMock.setDone.mock.calls.length,
        outcome: result,
      }).toEqual({ errorReactions: 1, doneReactions: 0, outcome: error });

      expect(deliverRepliesMock).not.toHaveBeenCalled();
      expect(stopSlackStreamMock).toHaveBeenCalledTimes(operation === "append" ? 0 : 1);
      expect(postMessageMock).not.toHaveBeenCalled();
      expect(emitSlackMessageSentHooksMock).toHaveBeenCalledTimes(3);
      expectMockCallArgFields(emitSlackMessageSentHooksMock, 0, {
        content: prefix,
        success: true,
      });
      expectMockCallArgFields(emitSlackMessageSentHooksMock, 1, {
        content: "second acknowledged",
        success: true,
      });
      expectMockCallArgFields(emitSlackMessageSentHooksMock, 2, {
        content: "failed reply",
        success: false,
        error: error.message,
      });
    },
  );
  it("normalizes only authored preview text when blocks own their fallback", async () => {
    useDraftStream();
    finalizeSlackPreviewEditMock.mockResolvedValueOnce(undefined);
    harness.mockedDispatchSequence = [
      {
        kind: "final",
        payload: {
          text: "**Summary**",
          presentation: {
            blocks: [
              {
                type: "buttons",
                buttons: [
                  {
                    label: "Owner <@U123>",
                    action: { type: "callback", value: "owner" },
                  },
                ],
              },
            ],
          },
        },
      },
    ];

    await dispatch();

    expect(normalizeSlackOutboundTextMock).toHaveBeenCalledTimes(1);
    expect(normalizeSlackOutboundTextMock).toHaveBeenCalledWith("**Summary**", {
      tableMode: "code",
    });
    expectMockCallArgFields(finalizeSlackPreviewEditMock, 0, {
      text: "**Summary**",
    });
  });

  it("deduplicates a plain plan headline without promoting it to a title", async () => {
    const draftStream = useDraftStream();
    harness.mockedSlackStreamingMode = "progress";
    harness.mockedDispatchSequence = [];
    harness.mockedReplyOptionEvents = [
      {
        kind: "item",
        itemKind: "preamble",
        itemId: "plain-preamble",
        progressText: "Read *literal* <@U123>",
      },
      {
        kind: "plan",
        phase: "update",
        explanation: "Read *literal* <@U123>",
        explanationFormat: "plain",
        steps: [{ step: "Inspect", status: "in_progress" }],
      },
    ];

    await dispatchPreparedSlackMessage(
      createPreparedSlackMessage({
        accountConfig: {
          streaming: { mode: "progress", progress: { style: "card", toolProgress: false } },
        },
      }),
    );

    expect(draftStream.update).toHaveBeenLastCalledWith({
      text: "Read *literal* &lt;@U123&gt;",
      blocks: [
        {
          type: "section",
          text: { type: "plain_text", text: "Read *literal* <@U123>", emoji: false },
        },
      ],
    });
  });

  it("caps the card fallback text at the draft limit while keeping the long blocks", async () => {
    const draftStream = useDraftStream();
    harness.mockedSlackStreamingMode = "progress";
    const long = (word: string) => Array.from({ length: 400 }, () => word).join(" ");
    harness.mockedReplyOptionEvents = [
      {
        kind: "plan",
        phase: "update",
        explanation: long("explain"),
        steps: [{ step: "Inspect", status: "in_progress" }],
      },
      { kind: "item", itemKind: "preamble", itemId: "preamble-1", progressText: long("status") },
      ...Array.from({ length: 20 }, (_, index) => ({
        kind: "approval" as const,
        phase: "requested" as const,
        approvalId: `approval-${index}`,
        command: `${long("run")}-${index}`,
      })),
    ];
    await dispatchPreparedSlackMessage(
      createPreparedSlackMessage({
        accountConfig: { streaming: { mode: "progress", progress: { style: "card" } } },
      }),
    );
    const update = requireRecord(draftStream.update.mock.calls.at(-1)?.[0], "long card update");
    const blocks = update.blocks as unknown[];
    // Precondition: the full card text exceeds the 4,000-character draft limit.
    expect(buildSlackCompleteBlocksFallbackText(blocks).length).toBeGreaterThan(4000);
    expect((update.text as string).length).toBeLessThanOrEqual(4000);
    expect(JSON.stringify(blocks)).toContain("Approval required:");
  });

  it("deduplicates commentary overlapping a detailed plan headline", async () => {
    const draftStream = useDraftStream();
    harness.mockedSlackStreamingMode = "progress";
    harness.mockedReplyOptionEvents = [
      {
        kind: "plan",
        phase: "update",
        explanation: "Checking the workspace",
        steps: [{ step: "Inspect", status: "in_progress" }],
      },
      {
        kind: "item",
        itemKind: "preamble",
        itemId: "preamble-1",
        progressText: "Checking the workspace",
      },
    ];
    await dispatchPreparedSlackMessage(
      createPreparedSlackMessage({
        accountConfig: {
          streaming: {
            mode: "progress",
            progress: { style: "card", commentary: true, toolProgress: true },
          },
        },
      }),
    );
    const text = draftUpdateTexts(draftStream).at(-1)!;
    expect(text.match(/Checking the workspace/g)).toHaveLength(1);
    expect(text).toContain("_Checking the workspace_");
    expect(text).toContain("▸ Inspect");
  });

  it.each([
    { firstSend: "delivered", messageId: "171234.567" },
    // The first Slack post is still queued or in flight, so no message id exists yet.
    { firstSend: "pending", messageId: undefined },
  ])(
    "deletes a working card once a resolved approval was its last visible row (first send $firstSend)",
    async ({ messageId }) => {
      const draftStream = useDraftStream();
      draftStream.messageId = () => messageId;
      harness.mockedSlackStreamingMode = "progress";
      harness.mockedReplyOptionEvents = [
        // The default card hides the plan, but the compositor still holds it.
        { kind: "plan", phase: "update", steps: [{ step: "Inspect", status: "in_progress" }] },
        { kind: "approval", phase: "requested", approvalId: "approval-1", command: "run checks" },
        { kind: "approval", phase: "resolved", approvalId: "approval-1" },
      ];
      await dispatchPreparedSlackMessage(
        createPreparedSlackMessage({
          accountConfig: { streaming: { mode: "progress", progress: { style: "card" } } },
        }),
      );
      expect(draftUpdateTexts(draftStream)).toEqual([
        "Approval required: run checks; approval requested",
      ]);
      expect(draftStream.clear).toHaveBeenCalled();
      expect(draftStream.forceNewMessage).toHaveBeenCalled();
    },
  );
});
/* oxlint-disable max-lines -- TODO: split this grandfathered oversized file. */
