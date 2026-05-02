/**
 * Cashflows page: real vs counterfactual, monthly flow waterfall, cumulative breakdown.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const [cf, monthlyRaw, bank] = await Promise.all([
      window.api.get("/api/cashflows/cumulative"),
      window.api.get("/api/cashflows/monthly"),
      window.api.get("/api/cashflows/bank"),
    ]);

    // Defensive unwrap: monthly is currently a bare list, but ?resolution=daily
    // may evolve to return a dict like {monthly: [...], daily: [...]}. Tolerate
    // both shapes so a backend change does not crash the page.
    const monthly = Array.isArray(monthlyRaw)
      ? monthlyRaw
      : (monthlyRaw && Array.isArray(monthlyRaw.monthly) ? monthlyRaw.monthly : []);

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
    // Daily branch: when the API returns *_daily curves, render the chart
    // at daily precision on a time x-axis. Falls back to monthly month-end
    // anchors when the daily store is empty.
    const realDaily = cf.real_curve_daily;
    const cfDaily = cf.counterfactual_curve_daily;
    const isDaily = Array.isArray(realDaily) && realDaily.length > 0;

    const realPoints = isDaily
      ? realDaily.map((p) => ({ x: p.date, y: p.value }))
      : cf.real_curve.map((p) => ({ x: p.month, y: p.value }));
    const counterPoints = isDaily
      ? cfDaily.map((p) => ({ x: p.date, y: p.value }))
      : cf.counterfactual_curve.map((p) => ({ x: p.month, y: p.value }));

    const xScale = isDaily ? charts.dailyTimeAxis() : {};

    new Chart(ctx, {
      type: "line",
      data: {
        datasets: [
          {
            label: "Real equity",
            data: realPoints,
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            tension: isDaily ? 0 : 0.3,
            pointRadius: 0,
            fill: true,
            backgroundColor: (c) => c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.18)
              : "transparent",
          },
          {
            label: "Counterfactual (cumulative deposits)",
            data: counterPoints,
            borderColor: charts.cssVar("--c2"),
            borderWidth: 1.5,
            borderDash: [4, 4],
            tension: isDaily ? 0 : 0.3,
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
        scales: {
          x: xScale,
          y: { ticks: { callback: (v) => fmt.twdCompact(v) } },
        },
      },
    });
  }

  function renderMonthlyFlows(monthly) {
    const ctx = document.getElementById("chart-monthly").getContext("2d");
    const select = document.getElementById("flow-view");
    const hint = document.getElementById("flow-hint");
    let chart = null;

    const HINTS = {
      venue: "Trades booked at the broker, split by venue. Negative bars = buys (capital deployed); positive = sells (capital returned).",
      gross: "Bank deposits / withdrawals tagged to broker activity (settlements, dividends, FX). Positive = money landed in your bank from the broker; negative = money left for the broker.",
      external: "Net broker↔bank flow per month. A negative bar can mean you sold a lot of foreign stock — the proceeds came back to bank.",
      deposits: "Money you actively moved INTO your bank from outside (peer transfers + salary). Closer to 'how much new capital did I add this month'.",
    };

    function build(view) {
      const labels = monthly.map((m) => fmt.month(m.month));
      let datasets;
      let stacked = false;

      if (view === "venue") {
        const twBuy = monthly.map((m) => -(m.tw_buy || 0));
        const twSell = monthly.map((m) => m.tw_sell || 0);
        const frBuy = monthly.map((m) => -(m.foreign_buy || 0));
        const frSell = monthly.map((m) => m.foreign_sell || 0);
        datasets = [
          { label: "TW buy",       data: twBuy,  backgroundColor: charts.cssVar("--neg"), stack: "tw" },
          { label: "TW sell",      data: twSell, backgroundColor: charts.cssVar("--pos"), stack: "tw" },
          { label: "Foreign buy",  data: frBuy,  backgroundColor: charts.cssVar("--c2"),  stack: "fr" },
          { label: "Foreign sell", data: frSell, backgroundColor: charts.cssVar("--c4"),  stack: "fr" },
        ];
        stacked = true;
      } else if (view === "gross") {
        datasets = [
          {
            label: "Inflows (bank ← broker)",
            data: monthly.map((m) => m.gross_in || 0),
            backgroundColor: charts.cssVar("--pos"),
            stack: "f", borderRadius: 3,
          },
          {
            label: "Outflows (bank → broker)",
            data: monthly.map((m) => -(m.gross_out || 0)),
            backgroundColor: charts.cssVar("--neg"),
            stack: "f", borderRadius: 3,
          },
        ];
        stacked = true;
      } else if (view === "deposits") {
        const data = monthly.map((m) => m.deposits_net || 0);
        const colors = data.map((v) => v >= 0 ? charts.cssVar("--pos") : charts.cssVar("--neg"));
        datasets = [{ label: "Net external deposits", data, backgroundColor: colors, borderRadius: 3 }];
      } else {
        const data = monthly.map((m) => m.external_flow);
        const colors = data.map((v) => v >= 0 ? charts.cssVar("--pos") : charts.cssVar("--neg"));
        datasets = [{ label: "External flow", data, backgroundColor: colors, borderRadius: 3 }];
      }

      if (chart) chart.destroy();
      chart = new Chart(ctx, {
        type: "bar",
        data: { labels, datasets },
        options: {
          responsive: true, maintainAspectRatio: false,
          plugins: {
            legend: { display: datasets.length > 1, position: "top", align: "end" },
            tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(Math.abs(c.parsed.y))}` } },
          },
          scales: {
            x: { stacked },
            y: { stacked, ticks: { callback: (v) => fmt.twdCompact(v) } },
          },
        },
      });
      hint.textContent = HINTS[view] || "";
    }

    build(select.value || "venue");
    select.addEventListener("change", () => build(select.value));
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

  function renderBank(allRows) {
    allRows.sort((a, b) => (b.date || "").localeCompare(a.date || ""));
    // Pre-compute a lowercased haystack (memo + summary + category) so the
    // unified table's search predicate can use a single key. The data is
    // already in memory; this is cheap and avoids a custom predicate.
    for (const r of allRows) {
      r._haystack = `${r.memo || ""} ${r.summary || ""} ${r.category || ""}`.toLowerCase();
    }
    const cats = [...new Set(allRows.map((r) => r.category).filter(Boolean))].sort();
    const months = [...new Set(allRows.map((r) => r.month).filter(Boolean))].sort().reverse();
    const monthOpts = months.map((m) => ({ value: m, label: fmt.month(m) }));

    window.dataTable({
      tableId: "bank-table",
      rows: allRows,
      searchKeys: ["_haystack"],
      searchPlaceholder: "Search memo or category…",
      filters: [
        { id: "account",  key: "account",  label: "All accounts",   options: ["TWD", "FOREIGN"] },
        { id: "category", key: "category", label: "All categories", options: cats },
        { id: "month",    key: "month",    label: "All months",     options: monthOpts },
      ],
      defaultSort: { key: "date", dir: "desc" },
      colspan: 9,
      pageSize: 50,
      emptyText: "No matching transactions",
      row: (t) => {
        const local = t.signed_amount ?? t.amount ?? 0;
        const twd = t.amount_twd ?? local;
        return [
          td(fmt.month(t.month)),
          td(fmt.date(t.date), "text-mute"),
          tdPill(t.account || "TWD"),
          td(t.category || ""),
          td(t.memo || t.summary || "", "text-mute"),
          td(t.ccy || "TWD", "text-mute"),
          td(fmt.num(local, 2), `num ${fmt.tone(local)}`),
          td(fmt.twd(twd), `num ${fmt.tone(twd)}`),
          td(fmt.num(t.balance, 2), "num text-mute"),
        ];
      },
    });
  }

  function tdPill(text) {
    const el = document.createElement("td");
    const p = document.createElement("span");
    p.className = "pill";
    p.textContent = text || "";
    el.appendChild(p);
    return el;
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
