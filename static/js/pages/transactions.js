/**
 * Transactions page: filterable trade log with monthly volume and fee charts.
 */
(function () {
  let allTx = [];

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();

    const [tx, agg] = await Promise.all([
      window.api.get("/api/transactions"),
      window.api.get("/api/transactions/aggregates"),
    ]);
    allTx = tx;
    renderKPIs(agg.totals);
    renderVolumeChart(agg);
    renderFeeChart(agg);
    populateMonthFilter(allTx);
    bindFilters();
    rerender();
    document.getElementById("export-tx").addEventListener("click", exportCsv);
  }

  function renderKPIs(t) {
    setText("kpi-n", String(t.trades));
    setText("kpi-buy", fmt.twd(t.buy_twd));
    setText("kpi-sell", fmt.twd(t.sell_twd));
    const gross = (t.fees_twd || 0) + (t.tax_twd || 0);
    const rebate = t.rebate_twd || 0;
    const net = t.net_cost_twd ?? (gross - rebate);
    setText("kpi-cost", fmt.twd(net));
    const subParts = [`drag ${fmt.pctAbs(t.fee_drag_pct, 3)} of volume`];
    if (rebate > 0) subParts.unshift(`gross ${fmt.twd(gross)} − rebates ${fmt.twd(rebate)}`);
    setText("kpi-drag", subParts.join(" · "));
  }

  function renderVolumeChart(agg) {
    const ctx = document.getElementById("chart-volume").getContext("2d");
    const labels = agg.monthly.map((m) => fmt.month(m.month));
    const venues = agg.venues || [];
    const colors = [charts.cssVar("--c1"), charts.cssVar("--c2"), charts.cssVar("--c3")];

    // One bar per month: buys go up (positive), sells go down (negative).
    // Same stack key so all venues stack into a single positive segment and
    // a single negative segment within one column — instead of side-by-side
    // buy/sell bars.
    const datasets = [];
    venues.forEach((v, i) => {
      const base = colors[i] || charts.cssVar("--c4");
      datasets.push({
        label: `${v} buy`,
        data: agg.monthly.map((m) => m[`${v}_buy`] || 0),
        backgroundColor: base,
        stack: "vol",
      });
      datasets.push({
        label: `${v} sell`,
        data: agg.monthly.map((m) => -(m[`${v}_sell`] || 0)),
        backgroundColor: charts.hexWithAlpha(base, 0.45),
        stack: "vol",
      });
    });

    new Chart(ctx, {
      type: "bar",
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(Math.abs(c.parsed.y))}` } },
        },
        scales: { y: { ticks: { callback: (v) => fmt.twdCompact(v) }, stacked: true }, x: { stacked: true } },
      },
    });
  }

  function renderFeeChart(agg) {
    const ctx = document.getElementById("chart-fees").getContext("2d");
    const labels = agg.monthly.map((m) => fmt.month(m.month));
    const venues = agg.venues || [];
    const datasets = [];
    venues.forEach((v, i) => {
      datasets.push({
        label: `${v} fees`,
        data: agg.monthly.map((m) => m[`${v}_fees`] || 0),
        backgroundColor: charts.cssVar(`--c${(i % 8) + 1}`),
        stack: "s",
      });
    });
    datasets.push({
      label: "tax",
      data: agg.monthly.map((m) => venues.reduce((s, v) => s + (m[`${v}_tax`] || 0), 0)),
      backgroundColor: charts.cssVar("--c5"),
      stack: "s",
    });
    datasets.push({
      label: "rebate",
      data: agg.monthly.map((m) => -(m.rebate || 0)),
      backgroundColor: charts.cssVar("--pos"),
      stack: "s",
    });

    new Chart(ctx, {
      type: "bar",
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(Math.abs(c.parsed.y))}` } },
        },
        scales: { y: { ticks: { callback: (v) => fmt.twdCompact(v) }, stacked: true }, x: { stacked: true } },
      },
    });
  }

  function populateMonthFilter(rows) {
    const months = [...new Set(rows.map((t) => t.month))].sort();
    const sel = document.getElementById("month");
    for (const m of months) {
      const opt = document.createElement("option");
      opt.value = m;
      opt.textContent = fmt.month(m);
      sel.appendChild(opt);
    }
  }

  function bindFilters() {
    ["q", "venue", "month"].forEach((id) => {
      document.getElementById(id).addEventListener("input", rerender);
      document.getElementById(id).addEventListener("change", rerender);
    });
  }

  function filtered() {
    const q = (document.getElementById("q").value || "").toLowerCase();
    const v = document.getElementById("venue").value;
    const m = document.getElementById("month").value;
    return allTx.filter((t) => {
      if (v && t.venue !== v) return false;
      if (m && t.month !== m) return false;
      if (!q) return true;
      const code = String(t.code || "").toLowerCase();
      const name = String(t.name || "").toLowerCase();
      return code.includes(q) || name.includes(q);
    });
  }

  function rerender() {
    const rows = filtered();
    document.getElementById("tx-count").textContent = `${rows.length} of ${allTx.length} trades`;
    const tbody = document.querySelector("#tx-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 12;
      td.className = "table-empty";
      td.textContent = "No matching trades";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const t of rows.slice(0, 500)) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.date(t.date), "text-mute"));
      tr.appendChild(tdPill(t.venue));
      tr.appendChild(td(t.side || ""));
      tr.appendChild(tdLink(t.code || "", `/ticker/${encodeURIComponent(t.code || "")}`, "code"));
      tr.appendChild(td(t.name || ""));
      tr.appendChild(td(fmt.int(t.qty), "num"));
      tr.appendChild(td(fmt.num(t.price, 2), "num"));
      tr.appendChild(td(t.ccy || "", "text-mute"));
      tr.appendChild(td(fmt.twd(t.gross_twd), "num"));
      tr.appendChild(td(fmt.twd(t.fee_twd), "num text-mute"));
      tr.appendChild(td(fmt.twd(t.tax_twd), "num text-mute"));
      tr.appendChild(td(fmt.twd(t.net_twd), `num ${fmt.tone(t.net_twd)}`));
      tbody.appendChild(tr);
    }
  }

  function td(text, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    el.textContent = text;
    return el;
  }
  function tdPill(text) {
    const el = document.createElement("td");
    const p = document.createElement("span");
    p.className = "pill";
    p.textContent = text || "";
    el.appendChild(p);
    return el;
  }
  function tdLink(text, href, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    const a = document.createElement("a");
    a.href = href;
    a.textContent = text;
    el.appendChild(a);
    return el;
  }
  function setText(id, t) { const e = document.getElementById(id); if (e) e.textContent = t; }

  function exportCsv() {
    const rows = filtered();
    const headers = ["month", "date", "venue", "side", "code", "name", "qty", "price", "ccy", "gross_twd", "fee_twd", "tax_twd", "net_twd"];
    const lines = [headers.join(",")];
    for (const r of rows) {
      lines.push(headers.map((k) => csvCell(r[k])).join(","));
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `transactions-${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }
  function csvCell(v) {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load transactions: ${err.message}`;
    main.prepend(box);
  }
})();
