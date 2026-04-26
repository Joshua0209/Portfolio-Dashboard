/**
 * Cashflows page: real vs counterfactual, monthly flow waterfall, cumulative breakdown.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const [cf, monthly, bank] = await Promise.all([
      window.api.get("/api/cashflows/cumulative"),
      window.api.get("/api/cashflows/monthly"),
      window.api.get("/api/cashflows/bank"),
    ]);
    renderKPIs(cf);
    renderRealVsCounterfactual(cf);
    renderMonthlyFlows(monthly);
    renderBreakdown(cf.cumulative_flows);
    renderBank(bank);
  }

  function renderKPIs(cf) {
    setText("kpi-real", fmt.twd(cf.real_now_twd));
    setText("kpi-cf", fmt.twd(cf.counterfactual_twd));
    const profit = cf.profit_twd || 0;
    const el = document.getElementById("kpi-profit");
    el.textContent = fmt.twd(profit);
    el.className = "kpi-value " + fmt.tone(profit);
    const pct = cf.counterfactual_twd ? profit / cf.counterfactual_twd : 0;
    setText("kpi-profit-pct", `${fmt.pct(pct)} on capital`);
  }

  function renderRealVsCounterfactual(cf) {
    const ctx = document.getElementById("chart-cf").getContext("2d");
    const labels = cf.real_curve.map((p) => fmt.month(p.month));
    const real = cf.real_curve.map((p) => p.value);
    const counter = cf.counterfactual_curve.map((p) => p.value);

    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Real equity",
            data: real,
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            tension: 0.3,
            pointRadius: 0,
            fill: true,
            backgroundColor: (c) => c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.18)
              : "transparent",
          },
          {
            label: "Counterfactual (cumulative deposits)",
            data: counter,
            borderColor: charts.cssVar("--c2"),
            borderWidth: 1.5,
            borderDash: [4, 4],
            tension: 0.3,
            pointRadius: 0,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(c.parsed.y)}` } },
        },
        scales: { y: { ticks: { callback: (v) => fmt.twdCompact(v) } } },
      },
    });
  }

  function renderMonthlyFlows(monthly) {
    const ctx = document.getElementById("chart-monthly").getContext("2d");
    const labels = monthly.map((m) => fmt.month(m.month));
    // Buys negative (capital out), sells positive — venue-split.
    const twBuy = monthly.map((m) => -(m.tw_buy || 0));
    const twSell = monthly.map((m) => m.tw_sell || 0);
    const frBuy = monthly.map((m) => -(m.foreign_buy || 0));
    const frSell = monthly.map((m) => m.foreign_sell || 0);

    new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          { label: "TW buy",      data: twBuy,  backgroundColor: charts.cssVar("--neg"),  stack: "tw" },
          { label: "TW sell",     data: twSell, backgroundColor: charts.cssVar("--pos"),  stack: "tw" },
          { label: "Foreign buy", data: frBuy,  backgroundColor: charts.cssVar("--c2"),   stack: "fr" },
          { label: "Foreign sell",data: frSell, backgroundColor: charts.cssVar("--c4"),   stack: "fr" },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(Math.abs(c.parsed.y))}` } },
        },
        scales: {
          x: { stacked: true },
          y: { stacked: true, ticks: { callback: (v) => fmt.twdCompact(v) } },
        },
      },
    });
  }

  function renderBreakdown(flows) {
    const el = document.getElementById("breakdown-list");
    while (el.firstChild) el.removeChild(el.firstChild);
    const labelMap = {
      stock_buy_twd: "Stock purchases",
      stock_sell_twd: "Stock sales",
      rebate_in_twd: "Rebates received",
      tw_dividend_in_twd: "TW dividends",
      fx_to_usd_twd: "FX → USD",
      fx_to_twd_twd: "FX → TWD",
      salary_in_twd: "Salary deposits",
      transfer_net_twd: "Net transfers",
      interest_in_twd: "Interest received",
    };
    const rows = Object.entries(flows || {})
      .map(([k, v]) => ({ key: k, label: labelMap[k] || k, value: v || 0 }))
      .filter((r) => Math.abs(r.value) > 1)
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value));

    const max = Math.max(1, ...rows.map((r) => Math.abs(r.value)));
    for (const r of rows) {
      const row = document.createElement("div");
      row.className = "bar-row";
      const lab = document.createElement("span");
      lab.className = "text-sm";
      lab.textContent = r.label;
      const bar = document.createElement("span");
      bar.className = "bar " + (r.value >= 0 ? "pos" : "neg");
      const fill = document.createElement("span");
      fill.style.width = `${(Math.abs(r.value) / max * 100).toFixed(2)}%`;
      bar.appendChild(fill);
      const val = document.createElement("span");
      val.className = `num text-sm ${fmt.tone(r.value)}`;
      val.textContent = fmt.twd(r.value);
      row.append(lab, bar, val);
      el.appendChild(row);
    }
  }

  function renderBank(rows) {
    const tbody = document.querySelector("#bank-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 7;
      td.className = "table-empty";
      td.textContent = "No bank transactions";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    rows.sort((a, b) => (b.date || "").localeCompare(a.date || ""));
    for (const t of rows.slice(0, 200)) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.month(t.month)));
      tr.appendChild(td(fmt.date(t.date), "text-mute"));
      tr.appendChild(td(t.category || ""));
      tr.appendChild(td(t.memo || t.party || t.description || ""));
      tr.appendChild(td(t.ccy || "TWD", "text-mute"));
      const amt = t.amount_twd ?? t.amount ?? 0;
      tr.appendChild(td(fmt.twd(amt), `num ${fmt.tone(amt)}`));
      tr.appendChild(td(t.fx ? Number(t.fx).toFixed(3) : "—", "num text-mute"));
      tbody.appendChild(tr);
    }
  }

  function td(text, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    el.textContent = text;
    return el;
  }
  function setText(id, t) { const e = document.getElementById(id); if (e) e.textContent = t; }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load cashflows: ${err.message}`;
    main.prepend(box);
  }
})();
