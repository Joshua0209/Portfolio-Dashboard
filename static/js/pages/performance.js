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
    const [ts, rolling, attr, tax] = await Promise.all([
      window.api.get(`/api/performance/timeseries${q}`),
      window.api.get(`/api/performance/rolling${q}`),
      window.api.get(`/api/performance/attribution`),
      window.api.get(`/api/tax`),
    ]);

    renderKPIs(ts);
    renderCumChart(ts);
    renderMonthlyChart(ts);
    renderDrawdown(ts);
    renderRolling(rolling);
    renderAttribution(attr);
    renderAttributionTotals(attr);
    renderDDEpisodes(ts.drawdown_episodes || []);
    renderTickerContribution(tax);
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
    const labels = ts.monthly.map((m) => fmt.label(m));
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
    const labels = ts.monthly.map((m) => fmt.label(m));
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
    const labels = ts.monthly.map((m) => fmt.label(m));
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
    // Daily branch returns rolling_30d/60d/90d instead of rolling_3m/6m/12m.
    const r1 = rolling.rolling_3m || rolling.rolling_30d || [];
    const r2 = rolling.rolling_6m || rolling.rolling_60d || [];
    const r3 = rolling.rolling_12m || rolling.rolling_90d || [];
    const isDaily = !rolling.rolling_3m;
    const labels = r1.map((p) => fmt.label(p));
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
          series(r1, isDaily ? "30D" : "3M", charts.cssVar("--c1")),
          series(r2, isDaily ? "60D" : "6M", charts.cssVar("--c2")),
          series(r3, isDaily ? "90D" : "12M", charts.cssVar("--c4")),
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
    const canvas = document.getElementById("chart-attr-totals");
    if (!canvas) return;
    const t = attr.totals || {};
    const tw = t.tw_pnl_twd || 0;
    const fp = t.foreign_price_pnl_twd || 0;
    const fx = t.fx_pnl_twd || 0;
    const total = t.total_pnl_twd ?? (tw + fp + fx);

    // One bar = one cumulative-attribution column. Each component stacks
    // (positive above zero, negative below) so the visible bar height adds
    // up to the signed contribution of that component.
    const colors = {
      tw: charts.cssVar("--c1"),
      foreign: charts.cssVar("--c2"),
      fx: charts.cssVar("--c4"),
    };
    const datasets = [
      { label: "TW equities",            data: [tw], backgroundColor: colors.tw,      stack: "s", borderRadius: 4, borderSkipped: false },
      { label: "Foreign equities (price)", data: [fp], backgroundColor: colors.foreign, stack: "s", borderRadius: 4, borderSkipped: false },
      { label: "FX (USD/TWD)",           data: [fx], backgroundColor: colors.fx,      stack: "s", borderRadius: 4, borderSkipped: false },
    ];

    // Per-segment value labels + total beside the bar. Inline Chart.js
    // plugin — runs after the bars are drawn, reads each meta.data element's
    // pixel box, and centers a label inside it (skipping segments that are
    // too thin to fit a number). Indexes are flipped vs. the vertical layout:
    // for horizontal bars, segment width = |x - base|.
    const valueLabelsPlugin = {
      id: "attrTotalsValueLabels",
      afterDatasetsDraw(chart) {
        const { ctx } = chart;
        ctx.save();
        ctx.font = "600 11px " + charts.cssVar("--font-mono");
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";

        // Per-segment labels (centered inside each horizontal segment).
        chart.data.datasets.forEach((ds, dsi) => {
          const meta = chart.getDatasetMeta(dsi);
          meta.data.forEach((bar, idx) => {
            const v = ds.data[idx];
            if (!v) return;
            const { x, y, base } = bar.getProps(["x", "y", "base"], true);
            const w = Math.abs(x - base);
            if (w < 44) return; // too narrow to fit a TWD label
            ctx.fillStyle = "#fff";
            ctx.fillText(fmt.twdCompact(v), (x + base) / 2, y);
          });
        });

        // Total label at the end of the bar.
        const meta0 = chart.getDatasetMeta(0);
        const bar0 = meta0.data[0];
        if (bar0) {
          const { y } = bar0.getProps(["y"], true);
          const xEnd = chart.scales.x.getPixelForValue(total);
          const xPos = total >= 0
            ? Math.min(xEnd + 8, chart.chartArea.right - 4)
            : Math.max(xEnd - 8, chart.chartArea.left + 4);
          ctx.font = "700 13px " + charts.cssVar("--font-display");
          ctx.fillStyle = total >= 0 ? charts.cssVar("--pos") : charts.cssVar("--neg");
          ctx.textAlign = total >= 0 ? "left" : "right";
          ctx.textBaseline = "middle";
          ctx.fillText(`Total: ${fmt.twd(total)}`, xPos, y);
        }
        ctx.restore();
      },
    };

    charts_registry.push(new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: { labels: ["Cumulative P&L"], datasets },
      options: {
        indexAxis: "y", // horizontal bars
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { right: 96 } }, // room for the total label past the bar end
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: ${fmt.twd(c.parsed.x)}`,
              afterBody: () => [`Total: ${fmt.twd(total)}`],
            },
          },
        },
        scales: {
          x: {
            stacked: true,
            ticks: { callback: (v) => fmt.twdCompact(v) },
          },
          y: { stacked: true, grid: { display: false } },
        },
      },
      plugins: [valueLabelsPlugin],
    }));
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

  let contribTreemap = null;
  let contribTable = null;

  function renderTickerContribution(tax) {
    const rows = (tax?.by_ticker || []).map((r) => ({
      ...r,
      total_pnl_twd: (r.realized_pnl_twd || 0) + (r.dividends_twd || 0) + (r.unrealized_pnl_twd || 0),
    }));
    // Total absolute contribution sets the 100% baseline; using sum of |total|
    // (rather than signed sum) means winners and losers are weighted by their
    // magnitude, which is what the user actually wants to see.
    const grossPnl = rows.reduce((s, r) => s + Math.abs(r.total_pnl_twd || 0), 0) || 1;
    rows.forEach((r) => {
      r.contribution_share = (r.total_pnl_twd || 0) / grossPnl;
    });

    renderContribTreemap(rows);
    renderContribStats(rows);
    renderContribTable(rows);

    setText("contrib-sub",
      `${rows.length} tickers · gross |P&L| ${fmt.twd(grossPnl)}`);
  }

  function renderContribTreemap(rows) {
    const canvas = document.getElementById("contrib-treemap");
    if (!canvas) return;

    const tree = rows
      .filter((r) => Math.abs(r.total_pnl_twd || 0) > 0)
      .map((r) => ({
        code: r.code || "",
        name: r.name || "",
        venue: r.venue || "",
        total: r.total_pnl_twd || 0,
        realized: r.realized_pnl_twd || 0,
        dividends: r.dividends_twd || 0,
        unrealized: r.unrealized_pnl_twd || 0,
        share: r.contribution_share,
        _value: Math.abs(r.total_pnl_twd || 0),
      }));

    if (!tree.length) {
      const ctx = canvas.getContext("2d");
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = charts.cssVar("--text-faint");
      ctx.font = "13px " + charts.cssVar("--font-sans");
      ctx.textAlign = "center";
      ctx.fillText("No P&L attribution yet", canvas.width / 2, 40);
      return;
    }

    // Color: green for net contributors, red for net detractors. Intensity
    // scales with share of gross P&L, capped to 25% so a single dominant
    // name doesn't wash everyone else into a single shade.
    const posBase = charts.cssVar("--pos");
    const negBase = charts.cssVar("--neg");
    function colorFor(ctx) {
      const item = ctx?.raw?._data;
      if (!item) return charts.cssVar("--bg-elev-2");
      const intensity = Math.min(1, Math.abs(item.share || 0) / 0.25);
      const alpha = 0.30 + 0.55 * intensity;
      const base = (item.total || 0) >= 0 ? posBase : negBase;
      return charts.hexWithAlpha(base, alpha);
    }

    const config = {
      type: "treemap",
      data: {
        datasets: [{
          tree,
          key: "_value",
          borderWidth: 1,
          borderColor: charts.cssVar("--bg"),
          spacing: 1,
          backgroundColor: colorFor,
          labels: {
            display: true,
            align: "left",
            position: "top",
            color: charts.cssVar("--text"),
            font: { size: 11, weight: "600", family: charts.cssVar("--font-mono") || "monospace" },
            padding: 4,
            formatter: (ctx) => {
              const d = ctx?.raw?._data;
              if (!d) return "";
              const sharePct = (d.share * 100).toFixed(1);
              return [d.code, `${sharePct}% · ${fmt.twdCompact(d.total)}`];
            },
          },
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => {
                const d = items?.[0]?.raw?._data;
                return d ? `${d.code} · ${d.name}` : "";
              },
              label: (ctx) => {
                const d = ctx?.raw?._data;
                if (!d) return "";
                return [
                  `Realized:   ${fmt.twd(d.realized)}`,
                  `Dividends:  ${fmt.twd(d.dividends)}`,
                  `Unrealized: ${fmt.twd(d.unrealized)}`,
                  `Total:      ${fmt.twd(d.total)}`,
                  `Share:      ${(d.share * 100).toFixed(2)}% of gross`,
                ];
              },
            },
          },
        },
      },
    };

    if (contribTreemap) contribTreemap.destroy();
    contribTreemap = new Chart(canvas.getContext("2d"), config);
    charts_registry.push(contribTreemap);

    canvas.onclick = (evt) => {
      const points = contribTreemap.getElementsAtEventForMode(evt, "nearest", { intersect: true }, false);
      if (!points.length) return;
      const data = points[0]?.element?.$context?.raw?._data;
      if (data?.code) window.location.href = `/ticker/${encodeURIComponent(data.code)}`;
    };
  }

  function renderContribStats(rows) {
    const el = document.getElementById("contrib-stats");
    if (!el) return;
    while (el.firstChild) el.removeChild(el.firstChild);

    // Net contributors only, sorted by absolute total so big losers count too.
    const sorted = [...rows].sort((a, b) =>
      Math.abs(b.total_pnl_twd || 0) - Math.abs(a.total_pnl_twd || 0));
    const grossPnl = sorted.reduce((s, r) => s + Math.abs(r.total_pnl_twd || 0), 0) || 1;
    const netPnl = sorted.reduce((s, r) => s + (r.total_pnl_twd || 0), 0);

    const winners = rows.filter((r) => (r.total_pnl_twd || 0) > 0);
    const losers  = rows.filter((r) => (r.total_pnl_twd || 0) < 0);
    const winnerSum = winners.reduce((s, r) => s + r.total_pnl_twd, 0);
    const loserSum  = losers.reduce((s, r) => s + r.total_pnl_twd, 0);

    function topNShare(n) {
      const top = sorted.slice(0, n).reduce((s, r) => s + Math.abs(r.total_pnl_twd || 0), 0);
      return top / grossPnl;
    }

    const items = [
      { label: "Net P&L",    value: fmt.twd(netPnl),    sub: `${rows.length} tickers contributed`, tone: fmt.tone(netPnl) },
      { label: "Winners",    value: fmt.twd(winnerSum), sub: `${winners.length} names`,            tone: "value-pos" },
      { label: "Losers",     value: fmt.twd(loserSum),  sub: `${losers.length} names`,             tone: "value-neg" },
      { label: "Top-3 share",  value: fmt.pctAbs(topNShare(3),  1), sub: "of gross |P&L|" },
      { label: "Top-5 share",  value: fmt.pctAbs(topNShare(5),  1), sub: "of gross |P&L|" },
      { label: "Top-10 share", value: fmt.pctAbs(topNShare(10), 1), sub: "of gross |P&L|" },
    ];

    for (const it of items) {
      const row = document.createElement("div");
      row.style.cssText =
        "display:flex; justify-content:space-between; align-items:baseline; padding:6px 0; border-bottom:1px solid var(--line);";
      const left = document.createElement("div");
      left.style.cssText = "display:flex; flex-direction:column; gap:2px;";
      const lab = document.createElement("span");
      lab.className = "text-tiny text-mute";
      lab.style.cssText = "letter-spacing:0.1em; text-transform:uppercase;";
      lab.textContent = it.label;
      const sub = document.createElement("span");
      sub.className = "text-tiny text-mute";
      sub.textContent = it.sub;
      left.append(lab, sub);
      const val = document.createElement("span");
      val.className = `num text-display ${it.tone || ""}`;
      val.style.cssText = "font-size:18px; font-weight:600;";
      val.textContent = it.value;
      row.append(left, val);
      el.appendChild(row);
    }
  }

  function renderContribTable(rows) {
    if (contribTable) {
      contribTable.setRows(rows);
      return;
    }
    contribTable = window.dataTable({
      tableId: "contrib-table",
      rows,
      searchKeys: ["code", "name"],
      searchPlaceholder: "Search code or name…",
      filters: [
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
      ],
      defaultSort: { key: "total_pnl_twd", dir: "desc" },
      colspan: 8,
      pageSize: 25,
      emptyText: "No tickers",
      row: (r) => [
        tdCodeLink(r.code),
        td(r.name || ""),
        td(r.venue || "", "text-mute text-tiny"),
        td(fmt.twd(r.realized_pnl_twd || 0), `num ${fmt.tone(r.realized_pnl_twd || 0)}`),
        td(fmt.twd(r.dividends_twd || 0), "num value-pos"),
        td(fmt.twd(r.unrealized_pnl_twd || 0), `num ${fmt.tone(r.unrealized_pnl_twd || 0)}`),
        td(fmt.twd(r.total_pnl_twd || 0), `num ${fmt.tone(r.total_pnl_twd || 0)}`),
        td(`${(r.contribution_share * 100).toFixed(1)}%`, `num ${fmt.tone(r.total_pnl_twd || 0)}`),
      ],
    });
  }

  function tdCodeLink(code) {
    const el = document.createElement("td");
    el.className = "code";
    const a = document.createElement("a");
    a.href = `/ticker/${encodeURIComponent(code || "")}`;
    a.textContent = code || "—";
    el.appendChild(a);
    return el;
  }

  let monthsTable = null;
  function renderTable(ts) {
    // Decorate each monthly row with a stable `month_label` field so the
    // unified table's free-text search ("2025-08", "Aug") matches both
    // ISO and human-formatted labels.
    const rows = (ts.monthly || []).map((m) => ({
      ...m,
      month_label: fmt.label(m),
      sort_key: m.month || m.date || "",
    }));
    if (monthsTable) {
      monthsTable.setRows(rows);
      return;
    }
    monthsTable = window.dataTable({
      tableId: "months-table",
      rows,
      searchKeys: ["month_label", "sort_key"],
      searchPlaceholder: "Search month (e.g. 2025-08, Aug)…",
      defaultSort: { key: "sort_key", dir: "desc" },
      colspan: 8,
      pageSize: 25,
      emptyText: "No matching months",
      row: (m) => [
        td(fmt.label(m)),
        td(fmt.twd(m.v_start ?? m.equity_twd), "num"),
        td(fmt.twd(m.external_flow ?? m.flow_twd ?? 0), `num ${fmt.tone(m.external_flow ?? m.flow_twd ?? 0)}`),
        td(fmt.twd(m.weighted_flow ?? 0), "num text-mute"),
        td(fmt.twd(m.equity_twd), "num"),
        td(fmt.pct(m.period_return), `num ${fmt.tone(m.period_return)}`),
        td(fmt.pct(m.cum_twr), `num ${fmt.tone(m.cum_twr)}`),
        td(fmt.pct(m.drawdown), `num ${fmt.tone(m.drawdown)}`),
      ],
    });
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
