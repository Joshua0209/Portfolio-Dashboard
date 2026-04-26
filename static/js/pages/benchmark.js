/**
 * Benchmark page: portfolio TWR vs market strategies.
 *
 * Lets the user toggle one or more strategies (TW + US sides) and see
 * how their actual portfolio performs against passive index, dividend,
 * single-name, and balanced 60/40 alternatives.
 */
(function () {
  const STORAGE_KEY = "benchmark.selected.v1";
  const DEFAULT_KEYS = ["tw_passive", "us_passive"];
  let strategies = [];
  let selected = new Set(loadSelected());
  let cumChart = null;
  let monthlyChart = null;
  let scatterChart = null;

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const stratResp = await window.api.get("/api/benchmarks/strategies");
    strategies = stratResp;
    renderStrategyList();
    await refresh();
  }

  function loadSelected() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const arr = JSON.parse(raw);
        if (Array.isArray(arr) && arr.length) return arr;
      }
    } catch (_) { /* fall through */ }
    return [...DEFAULT_KEYS];
  }

  function persistSelected() {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify([...selected])); }
    catch (_) { /* ignore quota errors */ }
  }

  function renderStrategyList() {
    const tw = strategies.filter((s) => s.market === "TW");
    const us = strategies.filter((s) => s.market === "US");
    fillList("strategy-list-tw", tw);
    fillList("strategy-list-us", us);
  }

  function fillList(id, items) {
    const el = document.getElementById(id);
    while (el.firstChild) el.removeChild(el.firstChild);
    for (const s of items) {
      const row = document.createElement("label");
      row.className = "strategy-row";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = s.key;
      cb.checked = selected.has(s.key);
      cb.addEventListener("change", () => {
        if (cb.checked) selected.add(s.key);
        else selected.delete(s.key);
        persistSelected();
        refresh();
      });
      const label = document.createElement("div");
      label.className = "strategy-text";
      const name = document.createElement("strong");
      name.textContent = s.name;
      const desc = document.createElement("span");
      desc.className = "text-mute text-tiny";
      desc.textContent = s.description;
      label.append(name, desc);
      row.append(cb, label);
      el.appendChild(row);
    }
  }

  async function refresh() {
    const keys = [...selected].join(",");
    const data = await window.api.get(`/api/benchmarks/compare?keys=${encodeURIComponent(keys)}`);
    drawAll(data);
  }

  function drawAll(data) {
    drawCum(data);
    drawMonthly(data);
    drawScatter(data);
    drawStatsTable(data);
  }

  function colorFor(i) {
    const palette = ["--c1", "--c2", "--c3", "--c4", "--c5", "--c6", "--c7", "--c8"];
    return charts.cssVar(palette[i % palette.length]);
  }

  function drawCum(data) {
    const ctx = document.getElementById("chart-cum").getContext("2d");
    const labels = data.months.map(fmt.month);
    const datasets = [
      {
        label: data.portfolio.name,
        data: data.portfolio.curve.map((p) => (p.cum_return || 0) * 100),
        borderColor: charts.cssVar("--accent"),
        borderWidth: 2.5,
        pointRadius: 0,
        tension: 0.3,
        fill: true,
        backgroundColor: (c) => c.chart.chartArea
          ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.18)
          : "transparent",
      },
      ...data.strategies.map((s, i) => ({
        label: s.name,
        data: s.curve.map((p) => p.cum_return === null ? null : (p.cum_return || 0) * 100),
        borderColor: colorFor(i),
        borderWidth: 1.5,
        borderDash: [4, 4],
        pointRadius: 0,
        tension: 0.3,
        spanGaps: true,
      })),
    ];

    if (cumChart) cumChart.destroy();
    cumChart = new Chart(ctx, {
      type: "line",
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y.toFixed(2)}%` } },
        },
        scales: { y: { ticks: { callback: (v) => `${v.toFixed(0)}%` } } },
      },
    });
  }

  function drawMonthly(data) {
    const ctx = document.getElementById("chart-monthly").getContext("2d");
    const labels = data.months.map(fmt.month);
    const datasets = [
      {
        label: data.portfolio.name,
        data: data.portfolio.curve.map((p) => (p.period_return || 0) * 100),
        backgroundColor: charts.cssVar("--accent"),
        borderRadius: 2,
      },
      ...data.strategies.map((s, i) => ({
        label: s.name,
        data: s.curve.map((p) => p.period_return === null ? null : (p.period_return || 0) * 100),
        backgroundColor: charts.hexWithAlpha(colorFor(i), 0.7),
        borderRadius: 2,
      })),
    ];

    if (monthlyChart) monthlyChart.destroy();
    monthlyChart = new Chart(ctx, {
      type: "bar",
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y === null ? "—" : c.parsed.y.toFixed(2) + "%"}` } },
        },
        scales: { y: { ticks: { callback: (v) => `${v.toFixed(0)}%` } } },
      },
    });
  }

  function drawScatter(data) {
    const ctx = document.getElementById("chart-scatter").getContext("2d");
    const points = [
      {
        label: data.portfolio.name,
        data: [{ x: data.portfolio.stats.annualized_volatility * 100, y: data.portfolio.stats.twr_total * 100 }],
        backgroundColor: charts.cssVar("--accent"),
        borderColor: charts.cssVar("--accent"),
        pointRadius: 8,
        pointHoverRadius: 10,
        pointStyle: "rectRot",
      },
      ...data.strategies.map((s, i) => ({
        label: s.name,
        data: [{ x: s.stats.annualized_volatility * 100, y: s.stats.twr_total * 100 }],
        backgroundColor: colorFor(i),
        borderColor: colorFor(i),
        pointRadius: 6,
        pointHoverRadius: 8,
      })),
    ];

    if (scatterChart) scatterChart.destroy();
    scatterChart = new Chart(ctx, {
      type: "scatter",
      data: { datasets: points },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: TWR ${c.parsed.y.toFixed(1)}% · Vol ${c.parsed.x.toFixed(1)}%`,
            },
          },
        },
        scales: {
          x: { title: { display: true, text: "Annualized volatility (%)" }, ticks: { callback: (v) => `${v.toFixed(0)}%` } },
          y: { title: { display: true, text: "Cumulative TWR (%)" }, ticks: { callback: (v) => `${v.toFixed(0)}%` } },
        },
      },
    });
  }

  function drawStatsTable(data) {
    const tbody = document.querySelector("#stats-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    const youTwr = data.portfolio.stats.twr_total || 0;

    const rows = [
      { name: data.portfolio.name, stats: data.portfolio.stats, isYou: true },
      ...data.strategies.map((s) => ({ name: s.name, stats: s.stats, isYou: false })),
    ];

    for (const r of rows) {
      const tr = document.createElement("tr");
      if (r.isYou) tr.style.fontWeight = "600";
      tr.appendChild(td(r.name));
      tr.appendChild(td(fmt.pct(r.stats.twr_total), `num ${fmt.tone(r.stats.twr_total)}`));
      tr.appendChild(td(fmt.pctAbs(r.stats.annualized_volatility, 1), "num text-mute"));
      tr.appendChild(td(fmt.pct(r.stats.max_drawdown), `num ${fmt.tone(r.stats.max_drawdown)}`));
      tr.appendChild(td(r.stats.sharpe.toFixed(2), `num ${fmt.tone(r.stats.sharpe)}`));
      tr.appendChild(td(r.stats.sortino.toFixed(2), `num ${fmt.tone(r.stats.sortino)}`));
      const excess = r.isYou ? null : (youTwr - (r.stats.twr_total || 0));
      tr.appendChild(td(
        excess === null ? "—" : fmt.pct(excess),
        excess === null ? "num text-mute" : `num ${fmt.tone(excess)}`
      ));
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
    box.textContent = `Failed to load benchmark: ${err.message}`;
    main.prepend(box);
  }
})();
