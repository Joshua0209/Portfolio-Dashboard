/**
 * Per-ticker drill-down: position history, trades, dividends, P&L curve.
 */
(function () {
  // CSP `script-src 'self'` blocks inline <script> setters, so we read
  // the ticker code from a data attribute on the existing #ticker-code
  // span (rendered by the ticker.html heading block). Falls back to the
  // URL pathname if the element is missing for any reason.
  const codeEl = document.getElementById("ticker-code");
  const code = (codeEl && codeEl.dataset.code)
    || decodeURIComponent(window.location.pathname.split("/").pop() || "");

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    let data;
    try {
      data = await window.api.get(`/api/tickers/${encodeURIComponent(code)}`);
    } catch (e) {
      showEmpty();
      return;
    }
    if (!data || data.error) {
      showEmpty();
      return;
    }
    renderHeader(data);
    renderKPIs(data);
    if (data.daily_prices && data.daily_prices.points && data.daily_prices.points.length) {
      renderDailyPriceChart(data.daily_prices);
    }
    // Prefer daily-resolution position history when the backend supplies
    // it (only happens when the daily store has rows for this ticker).
    // Falls back to monthly snapshots so closed positions and tickers
    // outside the daily backfill window still render correctly.
    const usesDaily = Array.isArray(data.position_history_daily)
      && data.position_history_daily.length > 0;
    const history = usesDaily ? data.position_history_daily : data.position_history;
    renderPositionChart(history, usesDaily);
    renderPnlChart(history, usesDaily);
    renderTrades(data.trades);
    renderDividends(data.dividends);
  }

  function labelOf(row, useDaily) {
    if (useDaily) return row.date;
    return fmt.month(row.month);
  }

  /**
   * Daily price line + buy/sell trade markers (Phase 8).
   *
   * Aligns marker x-positions to the price-line dates so they sit exactly
   * on the close of the trading day. Trades on dates with no cached price
   * (e.g. a buy on a holiday before the daily series starts) get appended
   * to the labels axis with `null` price values so the line still renders
   * the marker at the correct horizontal slot.
   */
  function renderDailyPriceChart(daily) {
    const card = document.getElementById("chart-daily-card");
    if (!card) return;
    card.removeAttribute("hidden");

    const ctx = document.getElementById("chart-daily").getContext("2d");
    const points = daily.points;
    const trades = daily.trades || [];

    const labels = points.map((p) => p.date);
    const closes = points.map((p) => p.close);

    const buyData = labels.map(() => null);
    const sellData = labels.map(() => null);
    for (const t of trades) {
      const idx = labels.indexOf(t.date);
      if (idx === -1) continue;
      const isBuy = (t.side || "").includes("買");
      if (isBuy) buyData[idx] = closes[idx];
      else sellData[idx] = closes[idx];
    }

    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Close",
            data: closes,
            borderColor: charts.cssVar("--accent"),
            backgroundColor: (c) => c.chart.chartArea
              ? charts.gradientFill(c.chart.ctx, c.chart.chartArea, charts.cssVar("--accent"), 0.15)
              : "transparent",
            borderWidth: 1.5,
            pointRadius: 0,
            spanGaps: true, // preserves the gap-fill behavior from commit 041bf7f
            tension: 0.2,
            fill: true,
          },
          {
            label: "Buy",
            data: buyData,
            type: "scatter",
            backgroundColor: charts.cssVar("--pos"),
            borderColor: charts.cssVar("--pos"),
            pointStyle: "triangle",
            pointRadius: 8,
            pointHoverRadius: 11,
            showLine: false,
          },
          {
            label: "Sell",
            data: sellData,
            type: "scatter",
            backgroundColor: charts.cssVar("--neg"),
            borderColor: charts.cssVar("--neg"),
            pointStyle: "rectRot",
            pointRadius: 7,
            pointHoverRadius: 10,
            showLine: false,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top", align: "end" },
          tooltip: {
            callbacks: {
              label: (c) => {
                if (c.dataset.label === "Close") {
                  return `Close: ${fmt.num(c.parsed.y, 2)}`;
                }
                const trade = trades.find((t) => t.date === c.label
                  && ((t.side || "").includes("買") ? c.dataset.label === "Buy" : c.dataset.label === "Sell"));
                if (!trade) return c.dataset.label;
                return `${trade.side} · ${fmt.int(trade.qty)} @ ${fmt.num(trade.price, 2)}`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: {
              maxTicksLimit: 12,
              autoSkip: true,
              callback: function (val) {
                const label = this.getLabelForValue(val);
                return label ? label.slice(0, 7) : "";
              },
            },
          },
          y: { ticks: { callback: (v) => fmt.num(v, 0) } },
        },
      },
    });
  }

  function renderHeader(data) {
    document.getElementById("ticker-name").textContent = data.name || "";
  }

  function renderKPIs(data) {
    const cur = data.current;
    const lastSeen = data.last_seen_month;
    setText("kpi-qty", cur ? fmt.int(cur.qty) : "0");
    setText("kpi-status", data.is_open
      ? "Open position"
      : lastSeen ? `Closed · last seen ${fmt.month(lastSeen)}` : "Never held");
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

  function renderPositionChart(history, useDaily) {
    const ctx = document.getElementById("chart-pos").getContext("2d");
    const labels = history.map((h) => labelOf(h, useDaily));
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

  function renderPnlChart(history, useDaily) {
    const ctx = document.getElementById("chart-pnl").getContext("2d");
    const labels = history.map((h) => labelOf(h, useDaily));
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
    window.dataTable({
      tableId: "trade-table",
      rows: trades || [],
      searchKeys: ["side", "date"],
      searchPlaceholder: "Search side or date…",
      defaultSort: { key: "date", dir: "desc" },
      colspan: 7,
      pageSize: 25,
      emptyText: "No trades",
      row: (t) => [
        td(fmt.date(t.date), "text-mute"),
        td(t.side || ""),
        td(fmt.int(t.qty), "num"),
        td(fmt.num(t.price, 2), "num"),
        td(fmt.twd(t.gross_twd), "num"),
        td(fmt.twd(t.fee_twd), "num text-mute"),
        td(fmt.twd(t.net_twd), `num ${fmt.tone(t.net_twd)}`),
      ],
    });
  }

  function renderDividends(divs) {
    window.dataTable({
      tableId: "div-table",
      rows: divs || [],
      searchKeys: ["ccy", "month", "date"],
      searchPlaceholder: "Search month or ccy…",
      defaultSort: { key: "date", dir: "desc" },
      colspan: 5,
      pageSize: 15,
      emptyText: "No dividends",
      row: (d) => [
        td(fmt.month(d.month)),
        td(fmt.date(d.date), "text-mute"),
        td(d.ccy || "", "text-mute"),
        td(fmt.num(d.amount_local, 2), "num"),
        td(fmt.twd(d.amount_twd), "num value-pos"),
      ],
    });
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
