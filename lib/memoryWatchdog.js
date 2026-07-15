// lib/memoryWatchdog.js  (ESM)
// Self-cleaning memory manager, with a hard-restart safety net.
//
// There are three independent triggers:
//   1) Threshold trigger  — memory crosses `cleanMB` -> cleanup runs right away.
//   2) Scheduled trigger  — every `cleanupEveryMs`, cleanup runs anyway,
//      even if memory looks fine right now, so nothing has a chance to
//      quietly build up between checks.
//   3) Restart trigger    — memory crosses `restartMB` (kept comfortably
//      below Heroku's real R14 quota) -> cleanup/GC obviously isn't
//      keeping up, so instead of waiting for Heroku to hard-kill the dyno
//      with R14, the process closes sessions cleanly (via onRestart) and
//      exits itself with process.exit(1). Heroku's supervisor restarts the
//      dyno automatically, and index.js's autoReconnectAll() on boot picks
//      sessions back up from MongoDB — so this is a short, controlled
//      restart instead of an uncontrolled R14 crash + timeout.
//
// Cleanup (triggers 1 & 2) means: clear the in-memory message store + stale
// tracking Maps (passed in via onCleanup, defined in index.js) and, if the
// process was started with --expose-gc (it is: `node --expose-gc index.js`),
// force a real garbage-collection pass to actually give the freed memory
// back to the OS instead of just sitting in V8's heap unused.
function startMemoryWatchdog({
  cleanMB,                        // memory threshold (MB) that triggers an immediate cleanup
  restartMB,                      // optional: memory threshold (MB) that triggers a full self-restart
  checkEveryMs = 8000,            // how often we check current RSS against cleanMB / restartMB
  cleanupEveryMs = 5 * 60 * 1000, // forced cleanup on a timer, regardless of memory level
  onCleanup,
  onRestart,                      // optional: called right before process.exit(1), for graceful shutdown
} = {}) {
  if (!cleanMB) {
    throw new Error('memoryWatchdog: cleanMB is required');
  }
  if (restartMB && restartMB <= cleanMB) {
    throw new Error('memoryWatchdog: restartMB must be greater than cleanMB');
  }
  console.log(
    `[memoryWatchdog] active — cleanup on threshold (${cleanMB}MB), ` +
      `every ${(cleanupEveryMs / 60000).toFixed(1)} min regardless of memory level` +
      (restartMB ? `, and hard self-restart at ${restartMB}MB` : '')
  );

  let cleaning = false;
  let firedThisCycle = false;
  let restarting = false;

  async function triggerRestart(usedMB) {
    if (restarting) return; // already tearing down, don't double-fire
    restarting = true;
    console.error(
      `[memoryWatchdog] RESTART threshold crossed (${usedMB.toFixed(0)}MB >= ${restartMB}MB) — ` +
        `cleanup/GC isn't keeping up, restarting process now before Heroku force-kills it with R14`
    );
    try {
      if (typeof onRestart === 'function') {
        await onRestart();
      }
    } catch (err) {
      console.error('[memoryWatchdog] onRestart callback failed:', err);
    } finally {
      process.exit(1);
    }
  }

  async function runCleanup(reason, usedMB) {
    if (cleaning) return; // don't overlap two cleanups
    cleaning = true;
    try {
      console.warn(
        `[memoryWatchdog] Cleanup triggered — ${reason}` +
          (usedMB ? ` (memory was ${usedMB.toFixed(0)}MB)` : '')
      );
      if (typeof onCleanup === 'function') {
        await onCleanup();
      }
      if (typeof global.gc === 'function') {
        global.gc();
        const afterMB = process.memoryUsage().rss / 1024 / 1024;
        console.warn(`[memoryWatchdog] GC ran — memory now ${afterMB.toFixed(0)}MB`);
      }
    } catch (err) {
      console.error('[memoryWatchdog] onCleanup callback failed:', err);
    } finally {
      cleaning = false;
    }
  }

  // Trigger 1 & 3: threshold check (restart takes priority over cleanup —
  // no point cleaning up if we're about to exit anyway)
  const checkInterval = setInterval(() => {
    const usedMB = process.memoryUsage().rss / 1024 / 1024;

    if (restartMB && usedMB >= restartMB) {
      triggerRestart(usedMB);
      return;
    }

    if (usedMB >= cleanMB) {
      if (!firedThisCycle) {
        firedThisCycle = true;
        runCleanup('memory threshold crossed', usedMB);
      }
    } else {
      // dropped back below the threshold — allow it to fire again next climb
      firedThisCycle = false;
    }
  }, checkEveryMs);

  // Trigger 2: scheduled cleanup, independent of trigger 1. This is the
  // part that stops memory from creeping up slowly between threshold
  // crossings — it never waits for a problem, it just keeps tidying up.
  const scheduledInterval = setInterval(() => {
    if (restarting) return;
    runCleanup('scheduled periodic cleanup');
  }, cleanupEveryMs);

  return () => {
    clearInterval(checkInterval);
    clearInterval(scheduledInterval);
  };
}

export { startMemoryWatchdog };
