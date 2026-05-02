// Global DLQ alert banner.
//
// Polls /api/admin/failed-tasks once on DOMContentLoaded. Surfaces an
// alert when at least one fetch (TW / foreign / FX) is unresolved so
// the user knows the daily snapshot may be missing prices — most often
// foreign equities, where yfinance is the flakiest dependency.
//
// Failure modes are silent: a network error here must NEVER block page
// rendering. We just leave the banner hidden and try again on the next
// page load.

(function () {
  function summarize(tasks) {
    const counts = { tw_prices: 0, foreign_prices: 0, fx_rates: 0, other: 0 };
    for (const t of tasks) {
      if (counts[t.task_type] !== undefined) counts[t.task_type] += 1;
      else counts.other += 1;
    }
    const parts = [];
    if (counts.tw_prices) parts.push(`${counts.tw_prices} TW price${counts.tw_prices === 1 ? "" : "s"}`);
    if (counts.foreign_prices) parts.push(`${counts.foreign_prices} foreign price${counts.foreign_prices === 1 ? "" : "s"}`);
    if (counts.fx_rates) parts.push(`${counts.fx_rates} FX rate${counts.fx_rates === 1 ? "" : "s"}`);
    if (counts.other) parts.push(`${counts.other} other`);
    return parts.join(", ");
  }

  function activate() {
    const root = document.getElementById("dlq-banner");
    const textEl = document.getElementById("dlq-banner-text");
    if (!root || !textEl) return;

    fetch("/api/admin/failed-tasks", { headers: { Accept: "application/json" } })
      .then((res) => (res.ok ? res.json() : null))
      .then((body) => {
        if (!body || body.ok !== true) return;
        const data = body.data || {};
        const tasks = data.tasks || [];
        if (!tasks.length) return;
        const breakdown = summarize(tasks);
        textEl.textContent =
          ` ${tasks.length} failed fetch${tasks.length === 1 ? "" : "es"} ` +
          `(${breakdown}). Some holdings may be missing today's price.`;
        root.hidden = false;
      })
      .catch(() => {
        /* network failure → silently leave banner hidden */
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", activate);
  } else {
    activate();
  }
})();
