// Phase 13 — /today page wiring.
//
// Pulls four endpoints in parallel:
//   /api/today/snapshot   → hero KPIs + weekday-named heading
//   /api/today/movers     → top gainers/decliners
//   /api/today/sparkline  → 30-day equity line
//   /api/today/freshness  → in-page freshness dot (also lives globally
//                           in the footer via static/js/freshness.js)
//
// Per spec §6.4 the Developer Tools accordion is included from the
// shared partial; the deep-link expand is handled in that template's
// inline script so we don't need to touch it here.

(function () {
  const fmt = window.format || {};
  function fmtTWD(n) {
    if (n == null) return "—";
    return (fmt.currencyTWD ? fmt.currencyTWD(n) : `NT$${Math.round(n).toLocaleString()}`);
  }
  function fmtPct(n) {
    if (n == null) return "—";
    const sign = n > 0 ? "+" : "";
    return `${sign}${n.toFixed(2)}%`;
  }

  function paintHero(data) {
    if (!data || data.empty) {
      document.getElementById("data-date-heading").textContent =
        "No daily data yet";
      return;
    }
    const heading = document.getElementById("data-date-heading");
    heading.textContent = `Performance for ${data.weekday}, ${data.data_date}`;

    // Wall-clock context line: visible only when the most recent priced
    // day is not the current TPE date (weekend, holiday, or before
    // close).
    const ctx = document.getElementById("wallclock-context");
    if (data.today_in_tpe && data.today_in_tpe !== data.data_date) {
      ctx.textContent =
        `Wall clock today (TPE): ${data.today_in_tpe}. ` +
        `Markets closed or pre-open — showing the last priced session.`;
      ctx.hidden = false;
    } else {
      ctx.hidden = true;
    }

    document.getElementById("equity-twd").textContent = fmtTWD(data.equity_twd);
    document.getElementById("delta-twd").textContent =
      (data.delta_twd > 0 ? "+" : "") + fmtTWD(data.delta_twd);
    document.getElementById("delta-pct").textContent = fmtPct(data.delta_pct);
    document.getElementById("n-positions").textContent =
      String(data.n_positions ?? "—");
    document.getElementById("fx-usd-twd").textContent =
      data.fx_usd_twd ? data.fx_usd_twd.toFixed(3) : "—";

    const deltaEl = document.getElementById("delta-twd");
    deltaEl.classList.toggle("kpi__value--up", data.delta_twd > 0);
    deltaEl.classList.toggle("kpi__value--down", data.delta_twd < 0);
  }

  function makeMoverRow(m) {
    const li = document.createElement("li");
    const link = document.createElement("a");
    link.href = `/ticker/${encodeURIComponent(m.symbol)}`;
    const sym = document.createElement("span");
    sym.className = "data-list__symbol";
    sym.textContent = m.symbol;
    const pct = document.createElement("span");
    pct.className = "data-list__pct";
    pct.textContent = fmtPct(m.delta_pct);
    link.appendChild(sym);
    link.appendChild(pct);
    li.appendChild(link);
    return li;
  }

  function makeMutedItem() {
    const li = document.createElement("li");
    li.className = "muted";
    li.textContent = "—";
    return li;
  }

  function paintMovers(data) {
    const up = document.getElementById("movers-up");
    const down = document.getElementById("movers-down");
    up.replaceChildren();
    down.replaceChildren();
    const movers = (data && data.movers) || [];
    const gainers = movers.filter((m) => m.delta_pct > 0).slice(0, 5);
    const decliners = movers.filter((m) => m.delta_pct < 0).slice(0, 5);
    if (gainers.length === 0) up.appendChild(makeMutedItem());
    else gainers.forEach((m) => up.appendChild(makeMoverRow(m)));
    if (decliners.length === 0) down.appendChild(makeMutedItem());
    else decliners.forEach((m) => down.appendChild(makeMoverRow(m)));
  }

  function paintSparkline(data) {
    const canvas = document.getElementById("equity-sparkline");
    if (!canvas || !window.Chart) return;
    const points = (data && data.points) || [];
    if (points.length === 0) return;
    new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: points.map((p) => p.date),
        datasets: [{
          data: points.map((p) => p.equity_twd),
          borderColor: "#60a5fa",
          borderWidth: 1.5,
          pointRadius: 0,
          fill: true,
          backgroundColor: "rgba(96,165,250,.1)",
          tension: 0.2,
        }],
      },
      options: {
        plugins: { legend: { display: false } },
        scales: {
          x: { display: false },
          y: { display: false },
        },
        animation: false,
        responsive: false,
      },
    });
  }

  function paintFreshness(data) {
    const dot = document.getElementById("freshness-dot");
    const txt = document.getElementById("freshness-text");
    if (!dot || !txt) return;
    if (!data || !data.data_date) {
      dot.dataset.band = "red";
      txt.textContent = "no data";
      return;
    }
    dot.dataset.band = data.band;
    const days = data.stale_days;
    txt.textContent =
      `Latest data: ${data.data_date} · ${days <= 0 ? "today" : days + "d ago"}`;
  }

  async function loadAll() {
    const [snap, movers, sparkline, fresh] = await Promise.all([
      fetch("/api/today/snapshot").then((r) => r.json()),
      fetch("/api/today/movers").then((r) => r.json()),
      fetch("/api/today/sparkline").then((r) => r.json()),
      fetch("/api/today/freshness").then((r) => r.json()),
    ]);
    paintHero(snap.data || {});
    paintMovers(movers.data || {});
    paintSparkline(sparkline.data || {});
    paintFreshness(fresh.data || {});
  }

  function wireRefresh() {
    const btn = document.getElementById("refresh-btn");
    const status = document.getElementById("refresh-status");
    if (!btn) return;
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      status.textContent = "Refreshing…";
      try {
        const res = await fetch("/api/admin/refresh", { method: "POST" });
        const body = await res.json();
        if (!res.ok || body.ok === false) {
          throw new Error(body.error || `HTTP ${res.status}`);
        }
        const summary = body.data || {};
        status.textContent =
          `${summary.new_dates ?? 0} new dates, ${summary.new_rows ?? 0} rows`;
        await loadAll();
      } catch (e) {
        status.textContent = "refresh failed: " + (e.message || e);
      } finally {
        btn.disabled = false;
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => {
      wireRefresh();
      loadAll().catch((e) => console.error("today: load failed", e));
    });
  } else {
    wireRefresh();
    loadAll().catch((e) => console.error("today: load failed", e));
  }
})();
