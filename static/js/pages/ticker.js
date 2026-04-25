/**
 * Per-ticker drill-down: position history, trades, dividends, P&L curve.
 */
(function () {
  const code = window.TICKER_CODE;

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const data = await window.api.get(`/api/tickers/${encodeURIComponent(code)}`);
    if (!data || data.error) {
      showEmpty();
      return;
    }
    renderHeader(data);
    renderKPIs(data);
    renderPositionChart(data.position_history);
    renderPnlChart(data.position_history);
    renderTrades(data.trades);
    renderDividends(data.dividends);
  }

  function renderHeader(data) {
    document.getElementById("ticker-name").textContent = data.name || "";
  }

  function renderKPIs(data) {
    const cur = data.current;
    setText("kpi-qty", cur ? fmt.int(cur.qty) : "0");
    setText("kpi-status", cur ? "Open position" : "Closed");
    setText("kpi-avg", cur ? fmt.num(cur.avg_cost, 2) : "—");
    setText("kpi-cost", cur ? `${fmt.twd(cur.cost_twd)} basis` : "—");

    const s = data.summary || {};
    const realEl = document.getElementById("kpi-real");
    realEl.textContent = fmt.twd(s.realized_pnl_twd);
    realEl.className = "kpi-value " + fmt.tone(s.realized_pnl_twd);
    setText("kpi-real-pct", s.realized_pnl_pct === null || s.realized_pnl_pct === undefined ? "—" : fmt.pct(s.realized_pnl_pct));

    if (cur) {
      const pnl = cur.unrealized_pnl_twd ?? 0;
      const pct = cur.cost_twd ? pnl / cur.cost_twd : 0;
      const el = document.getElementById("kpi-unreal");
      el.textContent = fmt.twd(pnl);
      el.className = "kpi-value " + fmt.tone(pnl);
      setText("kpi-unreal-pct", fmt.pct(pct));
    } else {
      setText("kpi-unreal", "—");
      setText("kpi-unreal-pct", "fully closed");
    }
  }

  function renderPositionChart(history) {
    const ctx = document.getElementById("chart-pos").getContext("2d");
    const labels = history.map((h) => fmt.month(h.month));
    const qty = history.map((h) => h.qty);
    const price = history.map((h) => h.ref_price);
    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Quantity",
            data: qty,
            yAxisID: "y",
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            pointRadius: 3,
            tension: 0.3,
          },
          {
            label: "Ref price",
            data: price,
            yAxisID: "y2",
            borderColor: charts.cssVar("--c2"),
            borderWidth: 1.5,
            borderDash: [4, 4],
            pointRadius: 0,
            tension: 0.3,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top", align: "end" },
        },
        scales: {
          y: { ticks: { callback: (v) => fmt.int(v) } },
          y2: { position: "right", grid: { drawOnChartArea: false } },
        },
      },
    });
  }

  function renderPnlChart(history) {
    const ctx = document.getElementById("chart-pnl").getContext("2d");
    const labels = history.map((h) => fmt.month(h.month));
    const cost = history.map((h) => h.cost_twd);
    const mv = history.map((h) => h.mkt_value_twd);
    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Cost basis",
            data: cost,
            borderColor: charts.cssVar("--text-mute"),
            backgroundColor: "transparent",
            borderWidth: 1.5,
            borderDash: [3, 3],
            pointRadius: 0,
            tension: 0.3,
          },
          {
            label: "Market value",
            data: mv,
            borderColor: charts.cssVar("--accent"),
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.3,
            fill: true,
            backgroundColor: (c) => c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.18)
              : "transparent",
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

  function renderTrades(trades) {
    const tbody = document.querySelector("#trade-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!trades.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 7;
      td.className = "table-empty";
      td.textContent = "No trades";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const t of trades) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.date(t.date), "text-mute"));
      tr.appendChild(td(t.side || ""));
      tr.appendChild(td(fmt.int(t.qty), "num"));
      tr.appendChild(td(fmt.num(t.price, 2), "num"));
      tr.appendChild(td(fmt.twd(t.gross_twd), "num"));
      tr.appendChild(td(fmt.twd(t.fee_twd), "num text-mute"));
      tr.appendChild(td(fmt.twd(t.net_twd), `num ${fmt.tone(t.net_twd)}`));
      tbody.appendChild(tr);
    }
  }

  function renderDividends(divs) {
    const tbody = document.querySelector("#div-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!divs.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 5;
      td.className = "table-empty";
      td.textContent = "No dividends";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const d of divs) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.month(d.month)));
      tr.appendChild(td(fmt.date(d.date), "text-mute"));
      tr.appendChild(td(d.ccy || "", "text-mute"));
      tr.appendChild(td(fmt.num(d.amount_local, 2), "num"));
      tr.appendChild(td(fmt.twd(d.amount_twd), "num value-pos"));
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

  function showEmpty() {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "empty-state";
    const h = document.createElement("h3");
    h.textContent = `No data for ${code}`;
    const p = document.createElement("p");
    p.textContent = "This ticker isn't in the parsed by_ticker summary. Check tw_ticker_map.json.";
    box.append(h, p);
    main.appendChild(box);
  }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load ticker: ${err.message}`;
    main.prepend(box);
  }
})();
