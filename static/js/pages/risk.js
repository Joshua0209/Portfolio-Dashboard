/**
 * Risk page: drawdown curve, concentration metrics, leverage exposure.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const r = await window.api.get("/api/risk");
    renderKPIs(r);
    renderDrawdown(r);
    renderWeightsChart(r.weight_distribution);
    renderWeightsList(r.weight_distribution);
  }

  function renderKPIs(r) {
    const mdd = document.getElementById("kpi-mdd");
    mdd.textContent = fmt.pct(r.max_drawdown);
    mdd.className = "kpi-value " + (r.max_drawdown < 0 ? "value-neg" : "value-mute");
    setText("kpi-vol", fmt.pctAbs(r.annualized_volatility, 1));
    setText("kpi-top5", fmt.pctAbs(r.top_5_share, 1));
    setText("kpi-positions", `${r.position_count} open positions`);
    setText("kpi-lev", fmt.pctAbs(r.leverage_pct, 1));
    setText("kpi-lev-sub", `${fmt.twd(r.leverage_value_twd)} on margin`);
  }

  function renderDrawdown(r) {
    const ctx = document.getElementById("chart-dd").getContext("2d");
    const labels = r.drawdown_curve.map((p) => fmt.month(p.month));
    const dd = r.drawdown_curve.map((p) => p.drawdown * 100);

    document.getElementById("dd-current").textContent = `Current: ${fmt.pct(r.current_drawdown)}`;

    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: "Drawdown",
          data: dd,
          borderColor: charts.cssVar("--neg"),
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.3,
          fill: true,
          backgroundColor: (c) => c.chart.chartArea
            ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--neg"), 0.3)
            : "transparent",
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
    });
  }

  function renderWeightsChart(weights) {
    const ctx = document.getElementById("chart-weights").getContext("2d");
    const top = weights.slice(0, 12);
    const data = top.map((w) => w.weight * 100);
    const others = weights.slice(12).reduce((s, w) => s + w.weight, 0) * 100;
    const labels = top.map((w) => w.code || "—");
    if (others > 0) {
      data.push(others);
      labels.push("Others");
    }
    const palette = charts.palette();
    const colors = labels.map((_, i) => palette[i % palette.length]);

    new Chart(ctx, {
      type: "doughnut",
      data: {
        labels,
        datasets: [{
          data,
          backgroundColor: colors,
          borderColor: charts.cssVar("--bg-elev"),
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        cutout: "55%",
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => `${c.label}: ${c.parsed.toFixed(1)}%` } },
        },
      },
    });
    setText("hhi-label", `HHI ${(weights.reduce((s, w) => s + w.weight ** 2, 0)).toFixed(3)}`);
  }

  function renderWeightsList(weights) {
    const el = document.getElementById("weights-list");
    while (el.firstChild) el.removeChild(el.firstChild);
    for (const w of weights) {
      const row = document.createElement("div");
      row.className = "bar-row";
      const lab = document.createElement("span");
      lab.className = "text-sm";
      lab.style.cssText = "display:flex;gap:8px;align-items:center;";
      const code = document.createElement("strong");
      code.textContent = w.code || "—";
      const name = document.createElement("span");
      name.className = "text-mute text-tiny";
      name.textContent = w.name || "";
      lab.append(code, name);
      const bar = document.createElement("span");
      bar.className = "bar accent";
      const fill = document.createElement("span");
      fill.style.width = `${(w.weight * 100).toFixed(2)}%`;
      bar.appendChild(fill);
      const val = document.createElement("span");
      val.className = "num text-sm";
      val.textContent = `${(w.weight * 100).toFixed(2)}%`;
      row.append(lab, bar, val);
      el.appendChild(row);
    }
  }

  function setText(id, t) { const e = document.getElementById(id); if (e) e.textContent = t; }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load risk: ${err.message}`;
    main.prepend(box);
  }
})();
