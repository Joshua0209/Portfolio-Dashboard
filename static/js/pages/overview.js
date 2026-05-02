/**
 * Overview page: KPIs, equity curve, allocation donut, winners/losers, recent activity.
 * All data values are escaped before insertion to prevent XSS from
 * unexpected characters in PDF-parsed strings.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  function esc(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  async function init() {
    window.charts.applyDefaults();

    const [summary, holdings, txs] = await Promise.all([
      window.api.get("/api/summary"),
      window.api.get("/api/holdings/current"),
      window.api.get("/api/transactions"),
    ]);

    renderKPIs(summary);
    renderEquityCurve(summary);
    renderAllocation(summary);
    renderTopMovers(holdings.holdings);
    renderActivity(txs);
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function renderKPIs(s) {
    const fx = s.kpis?.fx_usd_twd;
    const equity = s.kpis?.real_now_twd ?? 0;
    setText("kpi-equity", fmt.twd(equity));
    setText("kpi-equity-sub", `${fmt.month(s.kpis?.as_of)} · USD/TWD ${fx ? fx.toFixed(3) : "—"}`);

    const profit = s.profit_twd ?? 0;
    const profitEl = document.getElementById("kpi-profit");
    profitEl.textContent = fmt.twd(profit);
    profitEl.className = "kpi-value " + fmt.tone(profit);
    setText("kpi-profit-sub", `vs. ${fmt.twd(s.invested_twd ?? 0)} capital`);

    const twr = document.getElementById("kpi-twr");
    twr.textContent = fmt.pct(s.twr ?? 0);
    twr.className = "kpi-value " + fmt.tone(s.twr);
    setText("kpi-twr-since", fmt.month(s.first_month));

    const xirr = document.getElementById("kpi-xirr");
    if (s.xirr === null || s.xirr === undefined) {
      xirr.textContent = "—";
    } else {
      xirr.textContent = fmt.pct(s.xirr);
      xirr.className = "kpi-value " + fmt.tone(s.xirr);
    }
  }

  function renderEquityCurve(s) {
    const ctx = document.getElementById("chart-equity").getContext("2d");
    const labels = s.equity_curve.map((p) => fmt.label(p));
    const equity = s.equity_curve.map((p) => p.equity_twd);
    const cumTwr = s.equity_curve.map((p) => (p.cum_twr || 0) * 100);

    setText("equity-range",
      `${fmt.month(s.first_month)} → ${fmt.month(s.last_month)} · ${s.months_covered} months`);

    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Equity (TWD)",
            data: equity,
            yAxisID: "y",
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            tension: 0.35,
            pointRadius: 0,
            pointHoverRadius: 4,
            fill: true,
            backgroundColor: (c) => c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.22)
              : "transparent",
          },
          {
            label: "Cumulative TWR (%)",
            data: cumTwr,
            yAxisID: "y2",
            borderColor: charts.cssVar("--c2"),
            borderWidth: 1.5,
            tension: 0.35,
            pointRadius: 0,
            pointHoverRadius: 3,
            borderDash: [4, 4],
            fill: false,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { display: true, position: "top", align: "end" },
          tooltip: {
            callbacks: {
              label: (c) => {
                if (c.datasetIndex === 0) return `Equity: ${fmt.twd(c.parsed.y)}`;
                return `Cum TWR: ${(c.parsed.y).toFixed(2)}%`;
              },
            },
          },
        },
        scales: {
          y: { position: "left", ticks: { callback: (v) => fmt.twdCompact(v) } },
          y2: {
            position: "right",
            grid: { drawOnChartArea: false },
            ticks: { callback: (v) => `${v.toFixed(0)}%` },
          },
        },
      },
    });
  }

  function renderAllocation(s) {
    const a = s.allocation;
    const segments = [
      { label: "TW securities", value: a.tw, color: charts.cssVar("--c1") },
      { label: "Foreign securities", value: a.foreign, color: charts.cssVar("--c2") },
      { label: "Cash (TWD)", value: a.bank_twd, color: charts.cssVar("--c4") },
      { label: "Cash (USD)", value: a.bank_usd, color: charts.cssVar("--c6") },
    ].filter((seg) => seg.value > 0);

    const total = segments.reduce((acc, x) => acc + x.value, 0);
    setText("alloc-total", fmt.twd(total));

    const ctx = document.getElementById("chart-alloc").getContext("2d");
    new Chart(ctx, {
      type: "doughnut",
      data: {
        labels: segments.map((seg) => seg.label),
        datasets: [{
          data: segments.map((seg) => seg.value),
          backgroundColor: segments.map((seg) => seg.color),
          borderColor: charts.cssVar("--bg-elev"),
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "62%",
        layout: { padding: 4 },
        plugins: {
          legend: {
            display: true,
            position: "right",
            align: "center",
            labels: charts.pieLegendLabels((label, v) => {
              const pct = total > 0 ? (v / total) * 100 : 0;
              return `${label} — ${pct.toFixed(1)}%`;
            }),
          },
          tooltip: {
            callbacks: {
              label: (c) => {
                const pct = ((c.parsed / total) * 100).toFixed(1);
                return `${c.label}: ${fmt.twd(c.parsed)} (${pct}%)`;
              },
            },
          },
        },
      },
    });
  }

  function renderTopMovers(holdings) {
    const sorted = [...holdings].sort((a, b) => b.unrealized_pnl_twd - a.unrealized_pnl_twd);
    populateMoverList("winners-list", sorted.slice(0, 5), "pos");
    populateMoverList("losers-list", sorted.slice(-5).reverse(), "neg");
  }

  function populateMoverList(elId, rows, tone) {
    const el = document.getElementById(elId);
    while (el.firstChild) el.removeChild(el.firstChild);
    if (!rows.length) {
      const empty = document.createElement("div");
      empty.className = "empty-state";
      empty.textContent = "No positions";
      el.appendChild(empty);
      return;
    }
    for (const r of rows) {
      const row = document.createElement("a");
      row.href = `/ticker/${encodeURIComponent(r.code || "")}`;
      row.className = "bar-row";
      row.style.gridTemplateColumns = "1.6fr 1fr 1fr";
      row.style.padding = "8px 4px";
      row.style.borderBottom = "1px solid var(--line)";
      row.style.color = "inherit";

      const left = document.createElement("span");
      const codeEl = document.createElement("strong");
      codeEl.style.fontSize = "13px";
      codeEl.textContent = r.code || "";
      const nameEl = document.createElement("span");
      nameEl.className = "text-mute text-sm";
      nameEl.style.marginLeft = "8px";
      nameEl.textContent = r.name || "";
      left.append(codeEl, nameEl);

      const pnlEl = document.createElement("span");
      pnlEl.className = `num text-sm value-${tone === "pos" ? "pos" : "neg"}`;
      pnlEl.textContent = fmt.twd(r.unrealized_pnl_twd);

      const pctEl = document.createElement("span");
      pctEl.className = `num text-sm value-${tone === "pos" ? "pos" : "neg"}`;
      pctEl.textContent = fmt.pct(r.unrealized_pct);

      row.append(left, pnlEl, pctEl);
      el.appendChild(row);
    }
  }

  function renderActivity(rows) {
    window.dataTable({
      tableId: "activity-table",
      rows: rows || [],
      searchKeys: ["code", "name", "venue", "side"],
      searchPlaceholder: "Search code, name, side…",
      filters: [
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
      ],
      defaultSort: { key: "date", dir: "desc" },
      colspan: 8,
      pageSize: 15,
      emptyText: "No transactions",
      row: (t) => [
        td(fmt.date(t.date), "text-mute"),
        tdPill(t.venue),
        td(t.side || ""),
        tdLink(t.code || "", `/ticker/${encodeURIComponent(t.code || "")}`, "code"),
        td(t.name || ""),
        td(fmt.int(t.qty), "num"),
        td(fmt.num(t.price, 2), "num"),
        td(fmt.twd(t.net_twd), `num ${fmt.tone(t.net_twd)}`),
      ],
    });
  }

  function td(text, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    el.textContent = text;
    return el;
  }

  function tdPill(text) {
    const el = document.createElement("td");
    const pill = document.createElement("span");
    pill.className = "pill";
    pill.textContent = text || "";
    el.appendChild(pill);
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

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load overview: ${err.message}`;
    main.prepend(box);
  }
})();
