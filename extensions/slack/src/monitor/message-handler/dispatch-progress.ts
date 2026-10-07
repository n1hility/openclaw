import {
  type AgentPlanStep,
  createChannelProgressDraftCompositor,
  createChannelProgressWorkCounter,
  createDraftStreamLoop,
  createLivePreviewLifecycle,
  resolveChannelProgressDraftMaxLineChars,
  resolveChannelStreamingPreviewToolProgress,
  resolveChannelStreamingSuppressDefaultToolProgressMessages,
  type ChannelProgressDraftCompositorSnapshot,
  type LivePreviewDeliveryResult,
} from "openclaw/plugin-sdk/channel-outbound";
import type { ReplyDispatchKind, ReplyPayload } from "openclaw/plugin-sdk/reply-runtime";
import { danger, logVerbose } from "openclaw/plugin-sdk/runtime-env";
import { sanitizeAssistantVisibleText } from "openclaw/plugin-sdk/text-chunking";
import { createSlackDraftStream } from "../../draft-stream.js";
import { formatSlackError } from "../../errors.js";
import { SLACK_EDIT_TEXT_MAX_BYTES, SLACK_TEXT_LIMIT } from "../../limits.js";
import { buildSlackProgressTextBlocks } from "../../progress-blocks.js";
import { applyAppendOnlyStreamUpdate } from "../../stream-mode.js";
import { appendSlackStream } from "../../streaming.js";
import {
  resolveExplicitSlackProgressTitle,
  resolveSlackProgressReasoningMode,
} from "./dispatch-helpers.js";
import {
  createSlackDraftProgressCardRuntime,
  formatSlackProgressDraftLine,
} from "./dispatch-progress-card.js";
import { createSlackNativeStreamChain } from "./dispatch-progress-chain.js";
import { createSlackNativeProgressTransport } from "./dispatch-progress-native.js";
import {
  createSlackReasoningCardsRuntime,
  withSlackReasoningCardsWindow,
} from "./dispatch-progress-reasoning.js";
import {
  combineProgressHeadlineAndExplanation,
  resolveNativeProgressNarration,
} from "./dispatch-progress-render.js";
import { createSlackNativeProgressStream } from "./dispatch-progress-stream.js";
import type { SlackDispatchSetup } from "./dispatch-setup.js";
import type { SlackStreamingDeliveryRuntime } from "./dispatch-streaming.js";

export function createSlackProgressRuntime(runtimeParams: {
  setup: SlackDispatchSetup;
  delivery: SlackStreamingDeliveryRuntime;
}) {
  const { setup, delivery } = runtimeParams;
  const {
    account,
    cfg,
    ctx,
    hasSlackCustomIdentity,
    message,
    prepared,
    replyPlan,
    runtime,
    slackClient,
    slackIdentity,
    slackMessageMetadata,
    slackStreaming,
    slackProgressStyle,
    quietProgress,
    shouldUseDraftStream,
    useStreaming,
    previewStreamingEnabled,
  } = setup;
  const draftStream = shouldUseDraftStream
    ? createSlackDraftStream({
        target: prepared.replyTarget,
        cfg,
        token: ctx.botToken,
        accountId: account.accountId,
        conversationChannelId: message.channel,
        eventScope: prepared.eventScope,
        // Impersonated Slack messages cannot be deleted. Keep the temporary
        // preview app-authored and apply custom identity only to final delivery.
        ...(!hasSlackCustomIdentity && slackIdentity ? { identity: slackIdentity } : {}),
        ...(slackMessageMetadata ? { metadata: slackMessageMetadata } : {}),
        maxChars: Math.min(ctx.textLimit, SLACK_TEXT_LIMIT),
        resolveThreadTs: () => {
          const ts = replyPlan.peekThreadTs();
          if (ts) {
            delivery.usedReplyThreadTs ??= ts;
          }
          return ts;
        },
        log: logVerbose,
        warn: logVerbose,
      })
    : undefined;
  const isProgressMode = slackStreaming.mode === "progress";
  const useNativeProgressStreaming = useStreaming && slackStreaming.mode === "progress";
  const progressDraftActive = Boolean(draftStream) || useNativeProgressStreaming;
  const previewToolProgressEnabled =
    progressDraftActive &&
    resolveChannelStreamingPreviewToolProgress(
      account.config,
      slackStreaming.mode !== "progress",
      slackStreaming.mode,
    );
  const suppressDefaultToolProgressMessages =
    quietProgress ||
    resolveChannelStreamingSuppressDefaultToolProgressMessages(account.config, {
      draftStreamActive: Boolean(draftStream) || useNativeProgressStreaming,
      mode: slackStreaming.mode,
      previewToolProgressEnabled,
      previewStreamingEnabled,
    });
  let appendRenderedText = "";
  let appendSourceText = "";
  // A requested card post may still be queued or in flight without a message id.
  let cardPostRequested = false;
  // Terminal status of the turn's final payload; completion retries and
  // queued rotation must not repaint an errored turn as complete.
  let nativeProgressTerminalStatus: "complete" | "error" = "complete";
  // Native streaming appends; overlapping updates would re-append identical
  // narration/chunks because delta state commits only after network success.
  // One chain keeps each update's compute -> append -> commit atomic.
  const nativeChain = createSlackNativeStreamChain();
  const withNativeStreamOrder = nativeChain.run;
  const progressWorkCounter = createChannelProgressWorkCounter();
  const progressSeed = `${account.accountId}:${message.channel}`;
  // Compact quiet Slack is the latest model preamble, not the shared progress
  // card's summary. Keep reasoning and tool telemetry (including failures and
  // edit counters) out of this lane when refactoring channel presentation.
  const preambleOnlyProgress =
    isProgressMode && slackProgressStyle === "compact" && !previewToolProgressEnabled;
  // THIS BEHAVIOR IS INTENTIONAL AND MUST NOT BE CASUALLY ADJUSTED.
  // DO NOT CHANGE THIS WITHOUT APPROVAL FROM SJF OR PASHPASHPASH.
  const useDraftProgressCard =
    Boolean(draftStream) && isProgressMode && slackProgressStyle === "card";
  const explicitProgressTitle = resolveExplicitSlackProgressTitle(account.config);
  // Reasoning cards are task rows, so they need the detailed native card; the
  // quiet card keeps one summary row and leaves reasoning in the narration text.
  const useReasoningCards =
    useNativeProgressStreaming &&
    previewToolProgressEnabled &&
    resolveSlackProgressReasoningMode(account.config) === "cards";
  // A long think fills a message past Slack's size for one streamed message.
  // Cards mode chains messages: a full message finishes as "continued below"
  // and the turn goes on in a continuation in the same thread. Narration mode
  // keeps reasoning to one compacted line and is left as it was.
  const useStreamRollover = useReasoningCards;
  const progressCompositorEntry = useReasoningCards
    ? withSlackReasoningCardsWindow(account.config)
    : account.config;
  const reasoningCards = createSlackReasoningCardsRuntime({
    enabled: useReasoningCards,
    compositor: () => progressDraft,
    admits: (line) => nativeStream.admitsRow(line),
    flushQueued: () => withNativeStreamOrder(() => nativeStream.drain()),
    noteQueued: () => nativeUpdates.update(true),
  });
  const progressDraftMaxLineChars = resolveChannelProgressDraftMaxLineChars(account.config);
  const progressCard = createSlackDraftProgressCardRuntime({
    setup: { account, cfg, ctx, prepared, slackClient },
    draftStream,
    enabled: useDraftProgressCard,
    detailed: previewToolProgressEnabled,
    progressWorkCounter: previewToolProgressEnabled ? progressWorkCounter : undefined,
    explicitTitle: explicitProgressTitle,
    maxLineChars: progressDraftMaxLineChars,
    getSnapshot: () => progressDraft.getSnapshot(),
    getThreadTs: () => delivery.usedReplyThreadTs,
  });
  const nativeTransport = createSlackNativeProgressTransport({ setup, delivery });
  // Card-only cleanup. Other draft modes abandon a preview holding streamed
  // assistant text the human already replied to; that message stays visible.
  const dropDetachedProgressCards = async () => {
    if (!useDraftProgressCard) {
      return;
    }
    await draftStream?.dropDetachedMessages();
  };

  const resolveNativeProgressTitle = (snapshot: ChannelProgressDraftCompositorSnapshot) =>
    combineProgressHeadlineAndExplanation(
      explicitProgressTitle ?? snapshot.statusHeadline,
      snapshot.planExplanation,
    );

  // The finished card summarizes the think instead of keeping the running
  // headline; an explicit progress title still wins.
  const resolveNativeProgressCompletionTitle = (
    snapshot: ChannelProgressDraftCompositorSnapshot,
  ) =>
    explicitProgressTitle !== undefined
      ? resolveNativeProgressTitle(snapshot)
      : combineProgressHeadlineAndExplanation(
          reasoningCards.summaryTitle() ?? snapshot.statusHeadline,
          snapshot.planExplanation,
        );

  const nativeStream = createSlackNativeProgressStream({
    delivery,
    transport: nativeTransport,
    replyPlan,
    runtime,
    rollover: useStreamRollover,
    explicitTitle: explicitProgressTitle,
    maxLineChars: progressDraftMaxLineChars,
    summaryRow: !previewToolProgressEnabled,
    getSnapshot: () => progressDraft.getSnapshot(),
    resolveTitle: resolveNativeProgressTitle,
    resolveCompletionTitle: resolveNativeProgressCompletionTitle,
    resolveSessionLinks: () => progressCard.resolveSessionLinks(),
    onRolled: (throughCard) => reasoningCards.rollover(throughCard),
    pendingRows: () => reasoningCards.pendingRows(),
    releasePending: async () => {
      if (!(await nativeChain.awaitCompositorFlushes())) {
        return undefined;
      }
      return await reasoningCards.release();
    },
    peekPendingLines: () => reasoningCards.peekPendingLines(),
    markPendingPlaced: (lineIds) => reasoningCards.markPlaced(lineIds),
  });
  const buildNativeProgressCompletionChunks = nativeStream.buildCompletionChunks;

  const appendNativeProgressCompletion = async (isError: boolean) => {
    const session = delivery.streamSession;
    if (isError) {
      nativeProgressTerminalStatus = "error";
    }
    if (!session || nativeStream.completionSent || delivery.isStoppedBySlack()) {
      return;
    }
    const chunks = buildNativeProgressCompletionChunks(isError ? "error" : "complete");
    const narrationUpdate = nativeStream.resolveNarrationUpdate(
      resolveNativeProgressNarration(progressDraft.getSnapshot()),
    );
    if (!chunks?.length && !narrationUpdate.delta) {
      return;
    }
    try {
      delivery.streamLedger.record({ chunks });
      await appendSlackStream({ session, chunks });
      nativeStream.commitNarration(narrationUpdate.next);
      nativeStream.completionSent = true;
      delivery.observedReplyDelivery ||= session.delivered;
    } catch (err) {
      delivery.streamFailed = true;
      runtime.error?.(
        danger(`slack-stream: native progress completion failed: ${formatSlackError(err)}`),
      );
    }
  };

  const normalizeProgressText = (text: string | undefined) =>
    text?.replace(/\s+/gu, " ").trim() ?? "";

  const isRenderedAsProgressTitle = (text: string | undefined): boolean => {
    const candidate = normalizeProgressText(text);
    if (!candidate) {
      return false;
    }
    const snapshot = progressDraft.getSnapshot();
    const title = normalizeProgressText(
      combineProgressHeadlineAndExplanation(
        explicitProgressTitle ??
          (snapshot.statusHeadlineFormat === "plain" ? undefined : snapshot.statusHeadline),
        snapshot.planExplanationFormat === "plain" ? undefined : snapshot.planExplanation,
      ),
    );
    return title.length > 0 && title.includes(candidate);
  };

  // Rows still queued when the turn ends go out through the rollover owner,
  // budgeted and rolling as needed, before any closeout: a completion append
  // or stop carrying them could pass Slack's size or its 50-row plan block.
  // A send settles unless a start was not accepted or the compositor refused
  // a release; the few passes cover the former, and cards still queued after
  // them are placed straight from the queue on budgeted messages. `Now` runs
  // inside the transport chain, before a final that does not stream; the
  // other form joins the chain for the silent closeout and turn rotation,
  // where a final that streamed already drained the rows itself.
  let nativeFinalDelivered = false;
  const drainNativeProgressBeforeCloseNow = async (): Promise<void> => {
    if (
      !useStreamRollover ||
      !delivery.streamSession ||
      delivery.streamFailed ||
      delivery.isStoppedBySlack()
    ) {
      return;
    }
    await nativeStream.drain();
    nativeStream.admitPlanForCompletion();
  };
  const drainNativeProgressBeforeClose = async (): Promise<void> => {
    if (nativeFinalDelivered || nativeStream.completionSent) {
      return;
    }
    await withNativeStreamOrder(drainNativeProgressBeforeCloseNow);
  };

  const updateNativeProgressStreamNow = async (): Promise<boolean> => {
    if (!useNativeProgressStreaming || delivery.streamFailed || nativeUpdatesStopped) {
      return false;
    }
    const canContinue = await nativeTransport.waitForStart();
    if (!canContinue) {
      return false;
    }
    return (await nativeStream.send()).sent;
  };

  let nativeUpdatesStopped = false;
  // Read the latest compositor snapshot only when the batch sends. Terminal
  // delivery cancels pending batches before joining the same transport chain.
  const nativeUpdates = createDraftStreamLoop<boolean>({
    throttleMs: 1_000,
    coalesceInFlight: true,
    emptyValue: false,
    isEmpty: (pending) => !pending,
    isStopped: () => nativeUpdatesStopped,
    sendOrEditStreamMessage: () => nativeChain.runLoopSend(updateNativeProgressStreamNow),
    onBackgroundFlushError: (err) =>
      runtime.error?.(danger(`slack-stream: progress update failed: ${formatSlackError(err)}`)),
  });
  const cancelNativeUpdates = async () => {
    nativeUpdatesStopped = true;
    nativeUpdates.stop();
    await nativeUpdates.waitForInFlight();
  };

  const appendNativeNarration = (
    payload: ReplyPayload,
    kind: ReplyDispatchKind,
  ): Promise<LivePreviewDeliveryResult> =>
    withNativeStreamOrder(async () => {
      // The same preamble reaches us as a reply payload and as the compositor
      // headline behind the card title. The card updates it in place, so
      // streaming it as text too would print the line twice.
      if (isRenderedAsProgressTitle(payload.text)) {
        return { visibleReplySent: false };
      }
      const narrationUpdate = nativeStream.resolveNarrationUpdate(payload.text?.trimEnd());
      if (!narrationUpdate.delta) {
        return { visibleReplySent: false };
      }
      if (!(await nativeStream.ensureRoomForNarration(narrationUpdate.delta))) {
        return { visibleReplySent: false };
      }
      const result = await delivery.deliverWithStreaming({
        payload,
        kind,
        streamText: narrationUpdate.delta,
        appendSeparator: false,
        taskDisplayMode: "plan",
        ...(useStreamRollover ? { onMessageTooLong: nativeStream.retryNarrationOnNewMessage } : {}),
      });
      if (result.visibleReplySent && !delivery.streamFailed) {
        nativeStream.commitNarration(narrationUpdate.next);
      }
      return result;
    });

  const resetProgressTurnState = () => {
    progressWorkCounter.reset();
    cardPostRequested = false;
    reasoningCards.reset();
    nativeStream.reset();
    nativeFinalDelivered = false;
  };

  const progressDraft = createChannelProgressDraftCompositor({
    preparedItems: true,
    entry: progressCompositorEntry,
    mode: slackStreaming.mode,
    active: progressDraftActive,
    seed: progressSeed,
    formatLine: formatSlackProgressDraftLine,
    reasoningLinePrefix: "🧠 ",
    reasoningGate: !preambleOnlyProgress,
    // A completed preamble may have the same text as its final delta. Its
    // completion still has to reach the transport after a human boundary.
    updateOnLineChange: useNativeProgressStreaming || useDraftProgressCard || preambleOnlyProgress,
    update: async (previewText, options) => {
      if (useNativeProgressStreaming) {
        const priorSnapshot = nativeStream.snapshot;
        const priorNarration = nativeStream.narrationRenderedText;
        nativeUpdates.update(true);
        if (nativeChain.inside()) {
          // Inside a chain task: the send in progress reads the lines itself
          // or the loop follows up; waiting here would wait on ourselves.
          return false;
        }
        if (options?.flush) {
          await nativeChain.trackCompositorFlush(() => nativeUpdates.flush());
        } else {
          await nativeUpdates.waitForInFlight();
        }
        return (
          priorSnapshot !== nativeStream.snapshot ||
          priorNarration !== nativeStream.narrationRenderedText
        );
      }
      if (!draftStream) {
        return false;
      }
      const snapshot = options.snapshot;
      const latestLine = snapshot.lines.at(-1);
      if (preambleOnlyProgress && typeof latestLine === "object" && latestLine.complete === false) {
        // Keep the last complete preamble visible. A human reply can rotate this
        // draft between deltas, leaving a word fragment visible until cleanup.
        return false;
      }
      const cardBlocks = useDraftProgressCard
        ? progressCard.resolvePresentation(snapshot, "working")
        : undefined;
      if (cardBlocks?.length === 0) {
        // Hidden state (e.g. a plan in the default card) can outlive the last visible
        // row; delete the card rather than leave a resolved approval on screen.
        if (cardPostRequested || draftStream.messageId()) {
          cardPostRequested = false;
          await draftStream.clear();
          draftStream.forceNewMessage();
        }
        return false;
      }
      draftStream.update(
        preambleOnlyProgress
          ? {
              text: previewText,
              allowNewMessage: typeof latestLine !== "object" || latestLine.complete !== false,
              ...(snapshot.preparedBlocks
                ? { blocks: buildSlackProgressTextBlocks(snapshot.preparedBlocks) }
                : {}),
            }
          : cardBlocks
            ? {
                text: progressCard.resolveCardText(cardBlocks),
                blocks: cardBlocks,
              }
            : snapshot.preparedBlocks
              ? { text: previewText, blocks: buildSlackProgressTextBlocks(snapshot.preparedBlocks) }
              : previewText,
      );
      if (cardBlocks) {
        cardPostRequested = true;
      }
      if (options?.flush) {
        await draftStream.flush();
      }
      return Boolean(draftStream.messageId() && draftStream.channelId());
    },
    deleteCurrent: async () => {
      if (useNativeProgressStreaming) {
        // Native streams append task changes; clearing a plan retires its task rows.
        nativeUpdates.update(true);
        await nativeUpdates.flush();
      } else {
        cardPostRequested = false;
        await draftStream?.clear();
        draftStream?.forceNewMessage();
      }
    },
  });
  const previewLifecycle = createLivePreviewLifecycle<
    ReplyPayload,
    { channelId: string; messageId: string }
  >({
    // Native streams and persistent cards have their own terminal operations,
    // not temporary-preview deletion semantics.
    draft:
      draftStream && !useDraftProgressCard
        ? {
            flush: draftStream.flush,
            discardPending: draftStream.discardPending,
            seal: draftStream.seal,
            id: () => {
              const channelId = draftStream.channelId();
              const messageId = draftStream.messageId();
              return channelId && messageId ? { channelId, messageId } : undefined;
            },
            clear: async (): Promise<void> => {
              await draftStream.clear({
                preserveHumanReplies: !isProgressMode && previewLifecycle.finalDelivered,
              });
            },
          }
        : undefined,
    cleanupUndelivered: true,
    // A native stream marks the compositor's final itself, once the rows still
    // queued for the card have drained (deliverNativeFinalNow): the compositor
    // refuses new lines after the mark, and those rows must land before the answer.
    onFinalStarted: () => {
      if (!useNativeProgressStreaming) {
        progressDraft.markFinalReplyStarted();
      }
    },
    onFinalDelivered: () => progressDraft.markFinalReplyDelivered(),
    onCleanupFailure: (error) =>
      logVerbose(`slack: progress preview cleanup failed (${formatSlackError(error)})`),
  });
  const commentaryProgressEnabled = progressDraft.commentaryProgressEnabled;

  // Core fires onReasoningEnd and onAssistantMessageStart best effort, not
  // awaited, so with a live model a draft boundary can arrive while the final
  // reply is being delivered (the seal's append is still in flight when the run
  // ends). A boundary that lands then must wait for the answer to reach its
  // message; otherwise it would stop the stream and reset the turn under it.
  const awaitFinalDelivery = nativeChain.awaitFinalDelivery;

  const deliverNativeFinal = (
    payload: ReplyPayload,
    kind: ReplyDispatchKind,
  ): Promise<LivePreviewDeliveryResult> =>
    // The pacing loop stops now (synchronously); the compositor's final mark
    // waits until the queued rows have drained, since it refuses new lines.
    nativeChain.trackFinalDelivery(async () => {
      await cancelNativeUpdates();
      return await withNativeStreamOrder(() => deliverNativeFinalNow(payload, kind));
    });

  const deliverNativeFinalNow = async (payload: ReplyPayload, kind: ReplyDispatchKind) => {
    const streamReady = await nativeTransport.waitForStart();
    const finalThreadTs = delivery.streamSession?.threadTs ?? delivery.nativeProgressStreamThreadTs;
    // Optional progress may still be buffered locally. Join its stream so
    // final delivery cannot leave a second message to be flushed by stop.
    const canFinishInStream =
      payload.isError !== true &&
      streamReady &&
      Boolean(delivery.streamSession) &&
      delivery.isStreamingEligible(payload, { maxTextBytes: SLACK_EDIT_TEXT_MAX_BYTES });
    if (canFinishInStream) {
      await nativeStream.prepareForAnswer(payload);
    } else {
      await drainNativeProgressBeforeCloseNow();
    }
    progressDraft.markFinalReplyStarted();
    let result: LivePreviewDeliveryResult;
    if (canFinishInStream && !delivery.streamFailed) {
      // Flush the terminal task row before buffering the answer so Slack
      // preserves narration -> plan -> final answer ordering.
      await appendNativeProgressCompletion(false);
      result = await delivery.deliverWithStreaming({
        payload,
        kind,
        // The chain's thread, in case the think was finished and the answer streams alone.
        ...(useStreamRollover && finalThreadTs ? { forcedThreadTs: finalThreadTs } : {}),
        ...(useStreamRollover ? { onMessageTooLong: nativeStream.retryAnswerOnNewMessage } : {}),
      });
    } else {
      result = await delivery.deliverNormally({ payload, kind, forcedThreadTs: finalThreadTs });
      await appendNativeProgressCompletion(payload.isError === true);
    }
    nativeFinalDelivered = true;
    return result;
  };

  const finishNativeProgressTurn = async (
    completionChunks: ReturnType<typeof buildNativeProgressCompletionChunks>,
  ) => {
    if (delivery.nativeProgressStreamStartPromise) {
      await delivery.nativeProgressStreamStartPromise.catch(() => null);
    }
    if (completionChunks?.length) {
      nativeStream.completionSent = true;
    }
    await delivery.finishStream(completionChunks);
    delivery.streamSession = null;
    delivery.nativeProgressStreamStartPromise = null;
    delivery.nativeProgressStreamThreadTs = undefined;
    delivery.streamFailed = false;
    delivery.stoppedBySlack = false;
  };

  const pushPlanProgress = async (
    steps?: AgentPlanStep[],
    explanation?: string,
    explanationFormat?: "plain",
  ) => {
    if (isProgressMode && slackProgressStyle === "compact") {
      return false;
    }
    return await progressDraft.pushPlanProgress(steps, { explanation, explanationFormat });
  };

  const updateDraftFromPartial = (text?: string) => {
    const trimmed = text && sanitizeAssistantVisibleText(text).trimEnd();
    if (!trimmed) {
      return false;
    }

    if (slackStreaming.mode === "block") {
      progressDraft.resetActivity({ suppressed: true });
      const next = applyAppendOnlyStreamUpdate({
        incoming: trimmed,
        rendered: appendRenderedText,
        source: appendSourceText,
      });
      appendRenderedText = next.rendered;
      appendSourceText = next.source;
      if (!next.changed) {
        return false;
      }
      draftStream?.update(next.rendered);
      return false;
    }

    if (isProgressMode) {
      return false;
    }

    progressDraft.resetActivity({ suppressed: true });
    draftStream?.update(trimmed);
    return false;
  };
  const pushReasoningProgress = async (payload?: {
    text?: string;
    isReasoningSnapshot?: boolean;
  }) => {
    if (!payload?.text) {
      return false;
    }
    if (!isProgressMode) {
      const normalized = progressDraft
        .mergeReasoningProgress(payload.text, {
          snapshot: payload.isReasoningSnapshot === true,
        })
        .replace(/^_(.*)_$/su, "$1")
        .trim();
      if (!normalized) {
        return false;
      }
      const visible = await progressDraft.pushToolProgress({
        id: "reasoning",
        kind: "item",
        text: normalized,
        label: "Reasoning",
      });
      // Tool admission closes reasoning bursts; restore this still-open preview lane.
      progressDraft.mergeReasoningProgress(normalized, { snapshot: true });
      return visible;
    }
    if (reasoningCards.enabled) {
      return await reasoningCards.push({
        text: payload.text,
        isReasoningSnapshot: payload.isReasoningSnapshot,
      });
    }
    return await progressDraft.pushReasoningProgress(payload.text, {
      snapshot: payload.isReasoningSnapshot === true,
    });
  };
  const resetDraftDeliveryState = () => {
    appendRenderedText = "";
    appendSourceText = "";
  };
  const beginNewProgressTurn = async (options?: { force?: boolean }) => {
    if (useNativeProgressStreaming) {
      // deliverNativeFinal stops the pacing loop before its own appends go out,
      // so a boundary arriving now reads "previous turn over" and would stop the
      // stream under the completion and answer appends (Slack then answers
      // message_not_in_streaming_state and the answer degrades to a plain
      // message). Let the in-flight final land first; the boundary then acts on
      // the settled state. A forced rotation (queued follow-up) queues behind it too.
      await awaitFinalDelivery();
      if (!nativeUpdatesStopped && options?.force !== true) {
        return false;
      }
      await cancelNativeUpdates();
      await drainNativeProgressBeforeClose();
    }
    const priorSnapshot = progressDraft.getSnapshot();
    const completionChunks =
      useNativeProgressStreaming && !nativeStream.completionSent
        ? buildNativeProgressCompletionChunks(nativeProgressTerminalStatus)
        : undefined;
    if (!progressDraft.beginNewTurn(options)) {
      return false;
    }
    // Native messages are one-shot streams. Stop the prior turn before the
    // reset compositor can publish the queued turn's first snapshot.
    if (useNativeProgressStreaming) {
      await finishNativeProgressTurn(completionChunks);
    } else {
      await progressCard.finalize("success", { snapshot: priorSnapshot });
      await previewLifecycle.cleanup();
      draftStream?.forceNewMessage();
      await dropDetachedProgressCards();
    }
    resetProgressTurnState();
    nativeProgressTerminalStatus = "complete";
    nativeUpdatesStopped = false;
    nativeUpdates.resetThrottleWindow();
    progressCard.reset();
    // A re-armed turn is a new visible reply: it must not dedupe against or
    // inherit delivery state from the settled turn (mirrors queued admission).
    previewLifecycle.reset();
    delivery.resetDeliveryTracker();
    return true;
  };
  const onDraftBoundary =
    !shouldUseDraftStream && !useNativeProgressStreaming
      ? undefined
      : async () => {
          if (isProgressMode) {
            await beginNewProgressTurn();
            progressDraft.beginAssistantMessage();
            return;
          }
          // Model boundaries do not accept a provisional preview as a reply.
          // Keep editing it; the draft owner rotates separately when a human speaks.
          resetDraftDeliveryState();
          progressDraft.beginAssistantMessage();
        };

  const onQueuedFollowupAdmitted =
    !shouldUseDraftStream && !useNativeProgressStreaming
      ? undefined
      : async () => {
          // A queued input is a new visible reply even though it drains through
          // this turn's callbacks. Do not let it edit or dedupe against this run.
          await draftStream?.flush();
          if (isProgressMode) {
            await beginNewProgressTurn({ force: true });
          } else {
            await previewLifecycle.cleanup();
            previewLifecycle.reset();
            draftStream?.forceNewMessage();
          }
          delivery.resetDeliveryTracker();
          resetDraftDeliveryState();
          progressDraft.reset();
        };
  // A queued turn can drain after its dispatch returned, so dispatch closeout is
  // no longer available to settle its temporary presentation.
  const onQueuedFollowupSettled =
    !draftStream && !useNativeProgressStreaming
      ? undefined
      : async () => {
          if (useNativeProgressStreaming) {
            // Same window as a draft boundary: a settle that lands while the
            // final reply is still appending must not stop the stream under it.
            await awaitFinalDelivery();
            progressDraft.markFinalReplyStarted();
            await cancelNativeUpdates();
            await drainNativeProgressBeforeClose();
            await finishNativeProgressTurn(
              nativeStream.completionSent
                ? undefined
                : buildNativeProgressCompletionChunks(nativeProgressTerminalStatus),
            );
            return;
          }
          if (!useDraftProgressCard) {
            progressDraft.markFinalReplyStarted();
            await previewLifecycle.cleanup();
            return;
          }
          if (!progressCard.hasTerminalized) {
            await draftStream?.clear();
          }
          await dropDetachedProgressCards();
        };

  return {
    draftStream,
    previewLifecycle,
    isProgressMode,
    useDraftProgressCard,
    useNativeProgressStreaming,
    progressDraftActive,
    preambleOnlyProgress,
    suppressDefaultToolProgressMessages,
    progressDraft,
    progressWorkCounter,
    commentaryProgressEnabled,
    async cancel() {
      progressDraft.cancel();
      await cancelNativeUpdates();
    },
    get nativeProgressCompletionSent() {
      return nativeStream.completionSent;
    },
    set nativeProgressCompletionSent(value: boolean) {
      nativeStream.completionSent = value;
    },
    get nativeProgressTerminalStatus() {
      return nativeProgressTerminalStatus;
    },
    appendNativeNarration,
    buildNativeProgressCompletionChunks,
    deliverNativeFinal,
    drainNativeProgressBeforeClose,
    dropDetachedProgressCards,
    finalizeDraftProgressCard: progressCard.finalize,
    onVisibleWorkSessions: progressCard.onVisibleWorkSessions,
    onDraftBoundary,
    onQueuedFollowupAdmitted,
    onQueuedFollowupSettled,
    pushPlanProgress,
    pushReasoningProgress,
    noteReasoningToolCall: reasoningCards.noteToolCall,
    sealReasoningCards: reasoningCards.seal,
    updateDraftFromPartial,
  };
}
