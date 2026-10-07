/**
 * Ordering for one native progress stream: Slack calls run on a single chain so
 * each update's compute -> append -> commit stays atomic, and two in-flight
 * states that other work must observe are tracked beside it.
 */
export function createSlackNativeStreamChain() {
  let order: Promise<unknown> = Promise.resolve();
  // Chain tasks in progress. A compositor publish that lands while one runs
  // (a card released from inside a send, an event arriving during a Slack
  // call) must not wait for the send it is part of.
  let depth = 0;
  // Compositor publishes awaiting the pacing loop's flush. The gate's startup
  // render is one: it holds the gate's start promise until the first send
  // returns, and a card released into the compositor from inside that send
  // would wait on that promise. The loop's own send defers such a release to
  // the next paced send; any other chain task waits for the flush to settle.
  let compositorFlushesPending = 0;
  let compositorFlushesSettled: Promise<void> = Promise.resolve();
  let settleCompositorFlushes: (() => void) | undefined;
  let loopSendInFlight = false;
  // The final reply's delivery while it is in flight. Progress boundaries the
  // host queued earlier (reasoning end, assistant message start, a queued
  // follow-up settling) must not finish the turn under its appends.
  let finalDelivery: Promise<void> | null = null;

  const noteCompositorFlush = (delta: 1 | -1) => {
    compositorFlushesPending += delta;
    if (compositorFlushesPending === 1 && delta === 1) {
      compositorFlushesSettled = new Promise<void>((resolve) => {
        settleCompositorFlushes = resolve;
      });
    } else if (compositorFlushesPending === 0) {
      settleCompositorFlushes?.();
      settleCompositorFlushes = undefined;
    }
  };

  const run = <T>(task: () => Promise<T>): Promise<T> => {
    const ordered = async () => {
      depth += 1;
      try {
        return await task();
      } finally {
        depth -= 1;
      }
    };
    const next = order.then(ordered, ordered);
    order = next.catch(() => undefined);
    return next;
  };

  return {
    run,
    /** True inside a chain task; waiting on the chain there would wait on itself. */
    inside: () => depth > 0,
    /** The pacing loop's own send, so a release from inside it can defer. */
    runLoopSend: <T>(task: () => Promise<T>): Promise<T> =>
      run(async () => {
        loopSendInFlight = true;
        try {
          return await task();
        } finally {
          loopSendInFlight = false;
        }
      }),
    trackCompositorFlush: async <T>(flush: () => Promise<T>): Promise<T> => {
      noteCompositorFlush(1);
      try {
        return await flush();
      } finally {
        noteCompositorFlush(-1);
      }
    },
    /** False when the caller is the loop's send and a flush is pending: release on the next paced send. */
    awaitCompositorFlushes: async (): Promise<boolean> => {
      if (compositorFlushesPending === 0) {
        return true;
      }
      if (loopSendInFlight) {
        return false;
      }
      await compositorFlushesSettled;
      return true;
    },
    trackFinalDelivery: async <T>(deliver: () => Promise<T>): Promise<T> => {
      const pending = deliver();
      const inFlight = pending.then(
        () => undefined,
        () => undefined,
      );
      finalDelivery = inFlight;
      try {
        return await pending;
      } finally {
        if (finalDelivery === inFlight) {
          finalDelivery = null;
        }
      }
    },
    /** Resolves once no final delivery is in flight (a second one started meanwhile is waited for too). */
    awaitFinalDelivery: async (): Promise<void> => {
      for (let pending = finalDelivery; pending; pending = finalDelivery) {
        await pending;
      }
    },
  };
}
