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
    const PAGE_SIZE = 50;
    let page = 0;

    const tbody = document.querySelector("#bank-table tbody");
    const qEl = document.getElementById("bank-q");
    const accEl = document.getElementById("bank-account");
    const catEl = document.getElementById("bank-category");
    const monthEl = document.getElementById("bank-month");
    const countEl = document.getElementById("bank-count");
    const pageEl = document.getElementById("bank-page");

    const cats = [...new Set(allRows.map((r) => r.category).filter(Boolean))].sort();
    for (const c of cats) {
      const o = document.createElement("option");
      o.value = c; o.textContent = c;
      catEl.appendChild(o);
    }
    const months = [...new Set(allRows.map((r) => r.month))].sort().reverse();
    for (const m of months) {
      const o = document.createElement("option");
      o.value = m; o.textContent = fmt.month(m);
      monthEl.appendChild(o);
    }

    function filtered() {
      const q = (qEl.value || "").toLowerCase();
      const acc = accEl.value;
      const cat = catEl.value;
      const mo = monthEl.value;
      return allRows.filter((r) => {
        if (acc && r.account !== acc) return false;
        if (cat && r.category !== cat) return false;
        if (mo && r.month !== mo) return false;
        if (q) {
          const memo = String(r.memo || r.summary || "").toLowerCase();
          const c = String(r.category || "").toLowerCase();
          if (!memo.includes(q) && !c.includes(q)) return false;
        }
        return true;
      });
    }

    function rerender() {
      while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
      const rows = filtered();
      const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
      if (page >= totalPages) page = totalPages - 1;
      if (page < 0) page = 0;
      countEl.textContent = `${rows.length} of ${allRows.length} transactions`;
      pageEl.textContent = `${page + 1} / ${totalPages}`;

      if (!rows.length) {
        const tr = document.createElement("tr");
        const tdEl = document.createElement("td");
        tdEl.colSpan = 9;
        tdEl.className = "table-empty";
        tdEl.textContent = "No matching transactions";
        tr.appendChild(tdEl);
        tbody.appendChild(tr);
        return;
      }

      const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
      for (const t of slice) {
        const tr = document.createElement("tr");
        tr.appendChild(td(fmt.month(t.month)));
        tr.appendChild(td(fmt.date(t.date), "text-mute"));
        tr.appendChild(tdPill(t.account || "TWD"));
        tr.appendChild(td(t.category || ""));
        tr.appendChild(td(t.memo || t.summary || "", "text-mute"));
        tr.appendChild(td(t.ccy || "TWD", "text-mute"));
        const local = t.signed_amount ?? t.amount ?? 0;
        tr.appendChild(td(fmt.num(local, 2), `num ${fmt.tone(local)}`));
        const twd = t.amount_twd ?? local;
        tr.appendChild(td(fmt.twd(twd), `num ${fmt.tone(twd)}`));
        tr.appendChild(td(fmt.num(t.balance, 2), "num text-mute"));
        tbody.appendChild(tr);
      }
    }

    [qEl, accEl, catEl, monthEl].forEach((el) => {
      el.addEventListener("input", () => { page = 0; rerender(); });
      el.addEventListener("change", () => { page = 0; rerender(); });
    });
    document.getElementById("bank-prev").addEventListener("click", () => { page--; rerender(); });
    document.getElementById("bank-next").addEventListener("click", () => { page++; rerender(); });

    rerender();
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
