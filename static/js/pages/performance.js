/**
 * Performance page: TWR/XIRR, drawdown, monthly returns, rolling returns, attribution.
 */
(function () {
  // Persist user's chosen TWR method across page loads.
  const METHOD_STORAGE_KEY = "perf.twr.method.v1";
  let charts_registry = []; // for destroy on re-render

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();

    const sel = document.getElementById("twr-method");
    if (sel) {
      const stored = localStorage.getItem(METHOD_STORAGE_KEY) || "day_weighted";
      sel.value = stored;
      sel.addEventListener("change", async () => {
        localStorage.setItem(METHOD_STORAGE_KEY, sel.value);
        await refresh(sel.value);
      });
    }

    const initial = (sel && sel.value) || "day_weighted";
    await refresh(initial);
  }

  async function refresh(method) {
    // Destroy previous Chart.js instances so re-rendering doesn't leak canvases.
    for (const c of charts_registry) { try { c.destroy(); } catch (_) {} }
    charts_registry = [];

    const q = `?method=${encodeURIComponent(method)}`;
    const [ts, rolling, attr] = await Promise.all([
      window.api.get(`/api/performance/timeseries${q}`),
      window.api.get(`/api/performance/rolling${q}`),
      window.api.get(`/api/performance/attribution`),
    ]);

    renderKPIs(ts);
    renderCumChart(ts);
    renderMonthlyChart(ts);
    renderDrawdown(ts);
    renderRolling(rolling);
    renderAttribution(attr);
    renderAttributionTotals(attr);
    renderDDEpisodes(ts.drawdown_episodes || []);
    renderTable(ts);
  }

  function renderKPIs(ts) {
    setTextColor("kpi-twr", fmt.pct(ts.twr_total), ts.twr_total);
    setText("kpi-twr-sub", `${ts.monthly.length} months`);
    setTextColor("kpi-cagr", fmt.pct(ts.cagr || 0), ts.cagr || 0);
    if (ts.xirr === null || ts.xirr === undefined) {
      setText("kpi-xirr", "—");
    } else {
      setTextColor("kpi-xirr", fmt.pct(ts.xirr), ts.xirr);
    }
    setText("kpi-hit", `${(ts.hit_rate * 100).toFixed(0)}%`);
    setText("kpi-hit-sub", `${ts.positive_months} pos · ${ts.negative_months} neg`);
    setText("kpi-vol", fmt.pctAbs(ts.annualized_volatility, 1));
    const sharpe = ts.sharpe_annualized || 0;
    setTextColor("kpi-sharpe", sharpe.toFixed(2), sharpe);
    setText("kpi-sharpe-sub", bandLabel(sharpe, RATIO_BANDS.sharpe));
    const sortino = ts.sortino_annualized || 0;
    setTextColor("kpi-sortino", capRatio(sortino), sortino);
    setText("kpi-sortino-sub", bandLabel(sortino, RATIO_BANDS.sortino));
    const calmar = ts.calmar || 0;
    setTextColor("kpi-calmar", capRatio(calmar), calmar);
    setText("kpi-calmar-sub", bandLabel(calmar, RATIO_BANDS.calmar));
  }

  // Reference bands so the user can interpret raw ratio numbers at a glance.
  // Edges align with widely-cited industry conventions (CFA / hedge-fund desks).
  const RATIO_BANDS = {
    sharpe:  [{ at: 0.5, label: "poor" }, { at: 1.0, label: "sub-par" },
              { at: 2.0, label: "good" }, { at: 3.0, label: "great" },
              { at: Infinity, label: "elite / thin sample" }],
    sortino: [{ at: 1.0, label: "weak" }, { at: 2.0, label: "acceptable" },
              { at: 3.0, label: "good" }, { at: 5.0, label: "excellent" },
              { at: Infinity, label: "elite / thin sample" }],
    calmar:  [{ at: 0.5, label: "weak" }, { at: 1.0, label: "acceptable" },
              { at: 3.0, label: "strong" },
              { at: Infinity, label: "exceptional / thin sample" }],
  };

  function bandLabel(v, bands) {
    if (!isFinite(v)) return "—";
    if (v < 0) return "negative — losing money relative to risk";
    for (const b of bands) {
      if (v < b.at) return `band: ${b.label}`;
    }
    return `band: ${bands[bands.length - 1].label}`;
  }

  // Cap extreme ratios so a thin sample with no real drawdown doesn't print "361.0".
  function capRatio(v) {
    if (!isFinite(v) || Math.abs(v) > 100) return v > 0 ? "≫ 10" : "≪ −10";
    return v.toFixed(2);
  }

  function setText(id, v) {
    const el = document.getElementById(id);
    if (el) el.textContent = v;
  }

  function setTextColor(id, v, signal) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = v;
    el.className = "kpi-value " + fmt.tone(signal);
  }

  function renderCumChart(ts) {
    const ctx = document.getElementById("chart-cum").getContext("2d");
    const labels = ts.monthly.map((m) => fmt.month(m.month));
    const cum = ts.monthly.map((m) => (m.cum_twr || 0) * 100);
    const eq = ts.monthly.map((m) => m.equity_twd);

    charts_registry.push(new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Cumulative TWR (%)",
            data: cum,
            yAxisID: "y",
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.35,
            fill: true,
            backgroundColor: (c) => c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.2)
              : "transparent",
          },
          {
            label: "Equity (TWD)",
            data: eq,
            yAxisID: "y2",
            borderColor: charts.cssVar("--c2"),
            borderDash: [4, 4],
            borderWidth: 1.5,
            pointRadius: 0,
            tension: 0.35,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          tooltip: {
            callbacks: {
              label: (c) => c.datasetIndex === 0
                ? `TWR: ${c.parsed.y.toFixed(2)}%`
                : `Equity: ${fmt.twd(c.parsed.y)}`,
            },
          },
          legend: { position: "top", align: "end" },
        },
        scales: {
          y: { ticks: { callback: (v) => `${v.toFixed(0)}%` } },
          y2: { position: "right", grid: { drawOnChartArea: false }, ticks: { callback: (v) => fmt.twdCompact(v) } },
        },
      },
    }));
  }

  function renderMonthlyChart(ts) {
    const ctx = document.getElementById("chart-monthly").getContext("2d");
    const labels = ts.monthly.map((m) => fmt.month(m.month));
    const data = ts.monthly.map((m) => (m.period_return || 0) * 100);
    const colors = data.map((v) => v >= 0 ? charts.cssVar("--pos") : charts.cssVar("--neg"));

    document.getElementById("month-stats").textContent =
      `${ts.positive_months} positive · ${ts.negative_months} negative`;

    charts_registry.push(new Chart(ctx, {
      type: "bar",
      data: { labels, datasets: [{ label: "Period return", data, backgroundColor: colors, borderRadius: 3 }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => `${c.parsed.y.toFixed(2)}%` } },
        },
        scales: { y: { ticks: { callback: (v) => `${v.toFixed(0)}%` } } },
      },
    }));
  }

  function renderDrawdown(ts) {
    const ctx = document.getElementById("chart-dd").getContext("2d");
    const labels = ts.monthly.map((m) => fmt.month(m.month));
    const dd = ts.monthly.map((m) => (m.drawdown || 0) * 100);

    document.getElementById("dd-max").textContent = `Max: ${fmt.pct(ts.max_drawdown)}`;

    charts_registry.push(new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: "Drawdown",
          data: dd,
          borderColor: charts.cssVar("--neg"),
          backgroundColor: (c) => c.chart.chartArea
            ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--neg"), 0.3)
            : "transparent",
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.3,
          fill: true,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => `${c.parsed.y.toFixed(2)}%` } },
        },
        scales: { y: { ticks: { callback: (v) => `${v.toFixed(0)}%` } } },
      },
    }));
  }

  function renderRolling(rolling) {
    const ctx = document.getElementById("chart-rolling").getContext("2d");
    const labels = rolling.rolling_3m.map((p) => fmt.month(p.month));
    const series = (data, label, color) => ({
      label,
      data: data.map((p) => p.value === null ? null : p.value * 100),
      borderColor: color,
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.35,
      spanGaps: true,
    });

    charts_registry.push(new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          series(rolling.rolling_3m, "3M", charts.cssVar("--c1")),
          series(rolling.rolling_6m, "6M", charts.cssVar("--c2")),
          series(rolling.rolling_12m, "12M", charts.cssVar("--c4")),
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: {
            callbacks: {
              label: (c) => c.parsed.y === null ? `${c.dataset.label}: —` : `${c.dataset.label}: ${c.parsed.y.toFixed(2)}%`,
            },
          },
        },
        scales: { y: { ticks: { callback: (v) => `${v.toFixed(0)}%` } } },
      },
    }));
  }

  function renderAttribution(attr) {
    const monthly = attr.monthly || [];
    const ctx = document.getElementById("chart-attr").getContext("2d");
    const labels = monthly.map((m) => fmt.month(m.month));
    charts_registry.push(new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "TW",
            data: monthly.map((m) => m.tw_pnl),
            backgroundColor: charts.cssVar("--c1"),
            stack: "s",
            borderRadius: 3,
          },
          {
            label: "Foreign (price)",
            data: monthly.map((m) => m.foreign_pnl_price),
            backgroundColor: charts.cssVar("--c2"),
            stack: "s",
            borderRadius: 3,
          },
          {
            label: "FX",
            data: monthly.map((m) => m.foreign_pnl_fx),
            backgroundColor: charts.cssVar("--c4"),
            stack: "s",
            borderRadius: 3,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(c.parsed.y)}` } },
        },
        scales: { y: { ticks: { callback: (v) => fmt.twdCompact(v) }, stacked: true }, x: { stacked: true } },
      },
    }));
  }

  function renderAttributionTotals(attr) {
    const t = attr.totals || {};
    const el = document.getElementById("attr-totals");
    if (!el) return;
    while (el.firstChild) el.removeChild(el.firstChild);
    const items = [
      ["TW equities", t.tw_pnl_twd],
      ["Foreign equities (price)", t.foreign_price_pnl_twd],
      ["FX (USD/TWD)", t.fx_pnl_twd],
      ["Total P&L", t.total_pnl_twd],
    ];
    const max = Math.max(1, ...items.map(([_, v]) => Math.abs(v || 0)));
    for (const [label, v] of items) {
      const row = document.createElement("div");
      row.className = "bar-row";
      const lab = document.createElement("span");
      lab.className = "text-sm";
      lab.textContent = label;
      const bar = document.createElement("span");
      bar.className = "bar " + ((v || 0) >= 0 ? "pos" : "neg");
      const fill = document.createElement("span");
      fill.style.width = `${(Math.abs(v || 0) / max * 100).toFixed(2)}%`;
      bar.appendChild(fill);
      const val = document.createElement("span");
      val.className = "num text-sm " + ((v || 0) >= 0 ? "value-pos" : "value-neg");
      val.textContent = fmt.twd(v || 0);
      row.append(lab, bar, val);
      el.appendChild(row);
    }
  }

  function renderDDEpisodes(eps) {
    const tbody = document.querySelector("#dd-table tbody");
    if (!tbody) return;
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!eps.length) {
      const tr = document.createElement("tr");
      const td_ = document.createElement("td");
      td_.colSpan = 6; td_.className = "table-empty"; td_.textContent = "No drawdowns recorded";
      tr.appendChild(td_); tbody.appendChild(tr); return;
    }
    for (const ep of eps) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.month(ep.peak_month)));
      tr.appendChild(td(fmt.month(ep.trough_month)));
      tr.appendChild(td(fmt.pct(ep.depth_pct), "num value-neg"));
      tr.appendChild(td(String(ep.drawdown_months), "num"));
      tr.appendChild(td(ep.recovery_months != null ? `${ep.recovery_months}M` : "—", "num"));
      tr.appendChild(td(ep.recovered ? "Recovered" : "Open", ep.recovered ? "text-mute text-tiny" : "text-warn text-tiny"));
      tbody.appendChild(tr);
    }
  }

  function renderTable(ts) {
    const tbody = document.querySelector("#months-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    for (const m of ts.monthly) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.month(m.month)));
      tr.appendChild(td(fmt.twd(m.v_start), "num"));
      tr.appendChild(td(fmt.twd(m.external_flow), `num ${fmt.tone(m.external_flow)}`));
      tr.appendChild(td(fmt.twd(m.weighted_flow ?? 0), "num text-mute"));
      tr.appendChild(td(fmt.twd(m.equity_twd), "num"));
      tr.appendChild(td(fmt.pct(m.period_return), `num ${fmt.tone(m.period_return)}`));
      tr.appendChild(td(fmt.pct(m.cum_twr), `num ${fmt.tone(m.cum_twr)}`));
      tr.appendChild(td(fmt.pct(m.drawdown), `num ${fmt.tone(m.drawdown)}`));
      tbody.appendChild(tr);
    }
  }

  function td(text, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    el.textContent = text;
    return el;
  }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load performance: ${err.message}`;
    main.prepend(box);
  }
})();
