import {
  type ChannelProgressDraftLine,
  resolveChannelProgressDraftMaxLines,
} from "openclaw/plugin-sdk/channel-outbound";
import type { SlackAccountConfig } from "openclaw/plugin-sdk/config-contracts";
import {
  createSlackReasoningCardState,
  formatReasoningSummaryTitle,
  planSlackReasoningCards,
  segmentReasoningText,
} from "../../progress-reasoning.js";

// Each card is one line in the compositor's rolling window. A burst of
// reasoning larger than the window would evict a card before the throttled
// stream sends it, so cards mode widens the window; rows that leave it later
// are already complete in Slack and stay there.
const SLACK_REASONING_CARDS_MIN_WINDOW_LINES = 64;

type ReasoningCardsCompositor = {
  mergeReasoningProgress: (text?: string, options?: { snapshot?: boolean }) => string;
  resetReasoningProgress: () => void;
  pushToolProgress: (
    line: ChannelProgressDraftLine,
    options?: { reasoningLine?: boolean },
  ) => Promise<boolean>;
};

export function withSlackReasoningCardsWindow(entry: SlackAccountConfig): SlackAccountConfig {
  if (resolveChannelProgressDraftMaxLines(entry) >= SLACK_REASONING_CARDS_MIN_WINDOW_LINES) {
    return entry;
  }
  return {
    ...entry,
    streaming: {
      ...entry.streaming,
      progress: { ...entry.streaming?.progress, maxLines: SLACK_REASONING_CARDS_MIN_WINDOW_LINES },
    },
  };
}

/**
 * Per-turn reasoning card state for the native progress card. Cards are
 * ordinary compositor lines, so they share the paced update loop, the start
 * gate and the reconciler with tool rows.
 */
export function createSlackReasoningCardsRuntime(params: {
  enabled: boolean;
  compositor: () => ReasoningCardsCompositor;
  now?: () => number;
}) {
  const now = params.now ?? Date.now;
  let state = createSlackReasoningCardState();
  // Last pushed status/text per card id, so unchanged cards are not re-admitted.
  let pushedKeys = new Map<string, string>();
  let startedAt: number | undefined;

  const render = async (): Promise<boolean> => {
    const plan = planSlackReasoningCards(state);
    state.cardsEmitted = plan.cardsEmitted;
    let visible = false;
    for (const line of plan.lines) {
      const key = `${line.status ?? ""} ${line.text}`;
      if (pushedKeys.get(line.id) === key) {
        continue;
      }
      pushedKeys.set(line.id, key);
      // The card is the reasoning's own row: admitting it must not close the
      // burst, or the compositor would lose the raw phase text (the space
      // that ends "Reading ", the tag a later delta closes) and every further
      // delta would merge against display text instead.
      visible =
        (await params.compositor().pushToolProgress(line, { reasoningLine: true })) || visible;
    }
    return visible;
  };

  // Closes the open segment so later thinking starts a new card instead of
  // extending one that already sits above a tool row.
  const seal = async (): Promise<boolean> => {
    if (!params.enabled || !state.open) {
      return false;
    }
    state.sealed.push(...segmentReasoningText(state.open));
    state.open = "";
    params.compositor().resetReasoningProgress();
    return await render();
  };

  return {
    enabled: params.enabled,
    reset() {
      state = createSlackReasoningCardState();
      pushedKeys = new Map();
      startedAt = undefined;
    },
    async push(payload: { text: string; isReasoningSnapshot?: boolean }): Promise<boolean> {
      // The compositor keeps the raw phase text; only the rendered segments are normalized.
      const normalized = params.compositor().mergeReasoningProgress(payload.text, {
        snapshot: payload.isReasoningSnapshot === true,
      });
      if (!normalized) {
        return false;
      }
      startedAt ??= now();
      state.open = normalized;
      return await render();
    },
    seal,
    noteToolCall: async (): Promise<boolean> => {
      if (!params.enabled) {
        return false;
      }
      state.toolCalls += 1;
      return await seal();
    },
    /** Completion headline for the think, once reasoning has streamed. */
    summaryTitle(): string | undefined {
      return params.enabled && startedAt !== undefined
        ? formatReasoningSummaryTitle({ elapsedMs: now() - startedAt, toolCalls: state.toolCalls })
        : undefined;
    },
  };
}
