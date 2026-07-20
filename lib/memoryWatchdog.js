// lib/memoryWatchdog.js  (ESM)
// Ab yeh sirf ek kaam karta hai: memory `restartMB` cross kare to process ko
// controlled tareeke se restart karna (before Heroku force-kills it with
// R14). Cleanup/scheduled-cleanup wala kaam yahan se hata diya gaya hai —
// wo ab index.js ke apne "har 15 minute full bot clean" cycle mein hota hai.
function startMemoryWatchdog({
  restartMB,                // memory threshold (MB) jispar hard self-restart hoga
  checkEveryMs = 8000,      // kitni der mein current RSS ko restartMB ke against check karein
  onRestart,                // optional: process.exit(1) se pehle call hota hai, graceful shutdown ke liye
} = {}) {
  if (!restartMB) {
    throw new Error('memoryWatchdog: restartMB is required');
  }
  console.log(`[memoryWatchdog] active — hard self-restart threshold set at ${restartMB}MB`);

  let restarting = false;

  async function triggerRestart(usedMB) {
    if (restarting) return; // already tearing down, don't double-fire
    restarting = true;
    console.error(
      `[memoryWatchdog] RESTART threshold crossed (${usedMB.toFixed(0)}MB >= ${restartMB}MB) — ` +
        `restarting process now before Heroku force-kills it with R14`
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

  const checkInterval = setInterval(() => {
    const usedMB = process.memoryUsage().rss / 1024 / 1024;
    if (usedMB >= restartMB) {
      triggerRestart(usedMB);
    }
  }, checkEveryMs);

  return () => {
    clearInterval(checkInterval);
  };
}

export { startMemoryWatchdog };
