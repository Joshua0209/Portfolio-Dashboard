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
    else gainers.forEach((m) => {
      const li = makeMoverRow(m);
      li.dataset.sign = "pos";
      up.appendChild(li);
    });
    if (decliners.length === 0) down.appendChild(makeMutedItem());
    else decliners.forEach((m) => {
      const li = makeMoverRow(m);
      li.dataset.sign = "neg";
      down.appendChild(li);
    });
  }

  function paintPeriodStrip(data) {
    const root = document.getElementById("period-strip");
    if (!root || !data || data.empty) return;
    const cells = root.querySelectorAll(".period-strip__cell");
    (data.windows || []).forEach((w, i) => {
      const cell = cells[i];
      if (!cell) return;
      const valueEl = cell.querySelector(".period-strip__value");
      const subEl = cell.querySelector(".period-strip__sub");
      if (w.delta_pct == null) {
        valueEl.textContent = "—";
        subEl.textContent = "no data";
        return;
      }
      const sign = w.delta_pct > 0 ? "+" : "";
      valueEl.textContent = `${sign}${w.delta_pct.toFixed(2)}%`;
      valueEl.classList.toggle("pos", w.delta_pct > 0);
      valueEl.classList.toggle("neg", w.delta_pct < 0);
      const dt = w.delta_twd != null ? `${w.delta_twd > 0 ? "+" : ""}${fmtTWD(w.delta_twd)}` : "";
      subEl.textContent = `${dt}${w.anchor_date ? " · since " + w.anchor_date : ""}`;
    });
  }

  function paintDrawdown(data) {
    if (!data || data.empty) return;
    const canvas = document.getElementById("dd-chart");
    const cur = document.getElementById("dd-current");
    const detail = document.getElementById("dd-detail");
    if (!canvas || !window.Chart) return;
    const points = data.points || [];
    if (points.length === 0) return;

    if (cur) {
      const v = data.current_dd || 0;
      cur.textContent = `${v.toFixed(2)}% from peak`;
      cur.style.color = v < -0.05 ? "var(--neg)" : "var(--text-soft)";
    }
    if (detail) {
      detail.textContent =
        `Worst drawdown ${data.max_dd.toFixed(2)}% on ${data.max_dd_date} ` +
        `(peak ${data.max_dd_peak_date}). Current peak ${data.current_peak_date}.`;
    }

    new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: points.map((p) => p.date),
        datasets: [{
          data: points.map((p) => p.drawdown_pct),
          borderColor: charts.cssVar("--neg"),
          borderWidth: 1.5,
          pointRadius: 0,
          fill: true,
          backgroundColor: (c) =>
            c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--neg"), 0.28)
              : "transparent",
          tension: 0.25,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => items[0].label,
              label: (c) => `Drawdown ${c.parsed.y.toFixed(2)}%`,
            },
          },
        },
        scales: {
          x: { ticks: { maxTicksLimit: 6, color: charts.cssVar("--text-faint") }, grid: { display: false } },
          y: {
            max: 0,
            ticks: {
              callback: (v) => `${v.toFixed(0)}%`,
              color: charts.cssVar("--text-faint"),
            },
            grid: { color: charts.cssVar("--line") },
          },
        },
      },
    });
  }

  function paintRiskMetrics(data) {
    if (!data || data.empty) return;
    const setText = (id, txt) => { const el = document.getElementById(id); if (el) el.textContent = txt; };
    const signClass = (id, value) => {
      const el = document.getElementById(id);
      if (!el || value == null) return;
      el.classList.toggle("pos", value > 0);
      el.classList.toggle("neg", value < 0);
    };

    const annR = data.ann_return_pct;
    setText("risk-ann-return", annR == null ? "—" : `${annR > 0 ? "+" : ""}${annR.toFixed(2)}%`);
    signClass("risk-ann-return", annR);

    setText("risk-ann-vol", data.ann_vol_pct == null ? "—" : `${data.ann_vol_pct.toFixed(2)}%`);
    setText("risk-rolling-vol", data.rolling_30d_vol_pct == null ? "—" :
      `30d rolling: ${data.rolling_30d_vol_pct.toFixed(2)}%`);

    setText("risk-sharpe", data.sharpe == null ? "—" : data.sharpe.toFixed(2));
    setText("risk-sortino", data.sortino == null ? "—" : `Sortino ${data.sortino.toFixed(2)}`);

    setText("risk-max-dd", data.max_drawdown_pct == null ? "—" : `${data.max_drawdown_pct.toFixed(2)}%`);
    const maxDdEl = document.getElementById("risk-max-dd");
    if (maxDdEl && data.max_drawdown_pct != null && data.max_drawdown_pct < 0) {
      maxDdEl.classList.add("neg");
    }
    setText("risk-hit-rate", data.hit_rate_pct == null ? "—" :
      `Up days ${data.hit_rate_pct.toFixed(0)}%`);

    setText("risk-best-day", data.best_day_pct == null ? "—" :
      `Best ${data.best_day_pct > 0 ? "+" : ""}${data.best_day_pct.toFixed(2)}% · ` +
      `worst ${data.worst_day_pct.toFixed(2)}%`);

    setText("risk-window-meta", `${data.n_days} trading days`);
  }

  // Diverging color: -capPct → red, 0 → neutral, +capPct → green.
  function colorForReturn(pct, capPct) {
    if (pct == null) return "var(--bg-elev-2)";
    const t = Math.max(-1, Math.min(1, pct / capPct));
    if (t === 0) return "var(--bg-elev-2)";
    // Use HSL transitions; alpha = magnitude.
    const alpha = Math.min(1, Math.abs(t) * 0.95 + 0.15).toFixed(2);
    if (t > 0) return `rgba(78, 201, 160, ${alpha})`; // pos
    return `rgba(226, 109, 109, ${alpha})`; // neg
  }

  function paintCalendar(data) {
    const root = document.getElementById("cal-months");
    if (!root) return;
    root.replaceChildren();
    if (!data || data.empty || !data.cells.length) {
      const empty = document.createElement("p");
      empty.className = "text-mute";
      empty.textContent = "Need at least 2 daily equity rows to render the calendar.";
      root.appendChild(empty);
      return;
    }

    // Bucket cells by (year, month)
    const buckets = new Map();
    for (const c of data.cells) {
      const key = `${c.year}-${c.month}`;
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push(c);
    }

    // Cap the diverging scale at the 95th percentile of |returns| (or 2%, whichever is bigger)
    const mags = data.cells.map((c) => Math.abs(c.return_pct)).sort((a, b) => a - b);
    const p95 = mags[Math.floor(mags.length * 0.95)] || 2.0;
    const cap = Math.max(2.0, p95);

    const WD = ["M", "T", "W", "T", "F", "S", "S"];

    for (const m of data.months) {
      const card = document.createElement("div");
      card.className = "cal-month";

      const title = document.createElement("div");
      title.className = "cal-month__title";
      title.textContent = m.label;
      card.appendChild(title);

      const grid = document.createElement("div");
      grid.className = "cal-month__grid";
      // Weekday header
      for (const w of WD) {
        const h = document.createElement("span");
        h.className = "cal-month__weekday";
        h.textContent = w;
        grid.appendChild(h);
      }

      // Pad leading blanks to align day-of-week (Monday = column 0)
      const cells = buckets.get(`${m.year}-${m.month}`) || [];
      // Find first day-of-month — pad up to its weekday from grid start
      const firstCell = cells[0];
      const firstWeekday = firstCell ? firstCell.weekday : 0;
      // Pad: first day might not be the 1st; we still align by its actual weekday
      // To keep the calendar visually clean, pad with empty cells from Monday → firstWeekday
      for (let i = 0; i < firstWeekday; i++) {
        const e = document.createElement("span");
        e.className = "cal-cell cal-cell--empty";
        grid.appendChild(e);
      }

      let prevWeekday = firstWeekday - 1;
      for (const c of cells) {
        // Insert empty cells for skipped weekdays (gap-fill within month)
        let gap = c.weekday - prevWeekday - 1;
        if (gap < 0) gap += 7;
        for (let i = 0; i < gap; i++) {
          const e = document.createElement("span");
          e.className = "cal-cell cal-cell--empty";
          grid.appendChild(e);
        }
        const cell = document.createElement("span");
        cell.className = "cal-cell";
        cell.style.backgroundColor = colorForReturn(c.return_pct, cap);
        cell.textContent = String(c.dom);
        const sign = c.return_pct > 0 ? "+" : "";
        cell.title = `${c.date}: ${sign}${c.return_pct.toFixed(2)}%`;
        grid.appendChild(cell);
        prevWeekday = c.weekday;
      }

      card.appendChild(grid);
      root.appendChild(card);
    }
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
          borderColor: charts.cssVar("--accent"),
          borderWidth: 1.5,
          pointRadius: 0,
          fill: true,
          backgroundColor: (c) =>
            c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.18)
              : "transparent",
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
        responsive: true,
        maintainAspectRatio: false,
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
    const [snap, movers, sparkline, fresh, periods, dd, risk, cal] = await Promise.all([
      fetch("/api/today/snapshot").then((r) => r.json()),
      fetch("/api/today/movers").then((r) => r.json()),
      fetch("/api/today/sparkline").then((r) => r.json()),
      fetch("/api/today/freshness").then((r) => r.json()),
      fetch("/api/today/period-returns").then((r) => r.json()),
      fetch("/api/today/drawdown").then((r) => r.json()),
      fetch("/api/today/risk-metrics").then((r) => r.json()),
      fetch("/api/today/calendar").then((r) => r.json()),
    ]);
    paintHero(snap.data || {});
    paintMovers(movers.data || {});
    paintSparkline(sparkline.data || {});
    paintFreshness(fresh.data || {});
    paintPeriodStrip(periods.data || {});
    paintDrawdown(dd.data || {});
    paintRiskMetrics(risk.data || {});
    paintCalendar(cal.data || {});
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
