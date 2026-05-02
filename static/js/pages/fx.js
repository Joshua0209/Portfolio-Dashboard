/**
 * FX page: rate curve, FX P&L, currency exposure stack.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const fx = await window.api.get("/api/fx");
    if (fx.empty || !fx.rate_curve) {
      showEmpty();
      return;
    }
    renderKPIs(fx);
    renderRate(fx);
    renderFxPnl(fx);
    renderCcyStack(fx);
  }

  function showEmpty() {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "empty-state";
    const h = document.createElement("h3");
    h.textContent = "No FX data yet";
    const p = document.createElement("p");
    p.textContent = "Run parse_statements.py against the latest PDFs to populate data/portfolio.json.";
    box.append(h, p);
    main.appendChild(box);
  }

  function renderKPIs(fx) {
    setText("kpi-cur", fx.current_rate ? fx.current_rate.toFixed(3) : "—");
    setText("kpi-first", fx.first_rate ? `from ${fx.first_rate.toFixed(3)}` : "");
    setText("kpi-fx", fmt.pctAbs(fx.foreign_share, 1));
    setText("kpi-fx-twd", fmt.twd(fx.foreign_value_twd));
    const pnl = fx.fx_pnl?.contribution_twd || 0;
    const el = document.getElementById("kpi-fx-pnl");
    el.textContent = fmt.twd(pnl);
    el.className = "kpi-value " + fmt.tone(pnl);
    if (fx.first_rate && fx.current_rate) {
      const d = (fx.current_rate - fx.first_rate) / fx.first_rate;
      const dEl = document.getElementById("kpi-drate");
      dEl.textContent = fmt.pct(d);
      dEl.className = "kpi-value " + fmt.tone(d);
    }
  }

  function renderRate(fx) {
    const ctx = document.getElementById("chart-rate").getContext("2d");
    const labels = fx.rate_curve.map((p) => fmt.label(p));
    const data = fx.rate_curve.map((p) => p.fx_usd_twd);
    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: "USD/TWD",
          data,
          borderColor: charts.cssVar("--c2"),
          borderWidth: 2,
          pointRadius: 0,
          pointBackgroundColor: charts.cssVar("--c2"),
          tension: 0.3,
          fill: false,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => `${c.parsed.y.toFixed(4)}` } },
        },
        scales: { y: { ticks: { callback: (v) => v.toFixed(2) } } },
      },
    });
  }

  function renderFxPnl(fx) {
    const ctx = document.getElementById("chart-fx-pnl").getContext("2d");
    const monthly = fx.fx_pnl.monthly || [];
    const labels = monthly.map((m) => fmt.month(m.month));
    const cum = monthly.map((m) => m.cumulative_fx_pnl_twd);
    const per = monthly.map((m) => m.fx_pnl_twd);

    new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            type: "bar",
            label: "Monthly FX P&L",
            data: per,
            backgroundColor: per.map((v) => v >= 0 ? charts.cssVar("--pos") : charts.cssVar("--neg")),
            borderRadius: 3,
          },
          {
            type: "line",
            label: "Cumulative",
            data: cum,
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.3,
            fill: false,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(c.parsed.y)}` } },
        },
        scales: { y: { ticks: { callback: (v) => fmt.twdCompact(v) } } },
      },
    });
  }

  function renderCcyStack(fx) {
    const ctx = document.getElementById("chart-ccy").getContext("2d");
    const labels = fx.by_ccy_monthly.map((p) => fmt.month(p.month));
    const ccys = ["TWD", "USD", "HKD"];
    const colors = [charts.cssVar("--c1"), charts.cssVar("--c2"), charts.cssVar("--c4")];

    const datasets = ccys.map((c, i) => ({
      label: c,
      data: fx.by_ccy_monthly.map((p) => p[c] || 0),
      backgroundColor: colors[i],
      borderColor: colors[i],
      borderWidth: 0,
      stack: "ccy",
    })).filter((ds) => ds.data.some((v) => v > 0));

    new Chart(ctx, {
      type: "bar",
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(c.parsed.y)}` } },
        },
        scales: {
          y: { stacked: true, ticks: { callback: (v) => fmt.twdCompact(v) } },
          x: { stacked: true },
        },
      },
    });
  }

  function setText(id, t) { const e = document.getElementById(id); if (e) e.textContent = t; }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load FX: ${err.message}`;
    main.prepend(box);
  }
})();
