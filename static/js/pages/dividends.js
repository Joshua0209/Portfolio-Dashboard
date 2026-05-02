/**
 * Dividends page: stacked monthly income (TW + Foreign), top payers,
 * total-return-on-cost table, rebate ledger, full distribution log.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const data = await window.api.get("/api/dividends");
    renderKPIs(data);
    renderMonthlyChart(data.monthly);
    renderTopPayers(data.by_ticker);
    renderTotalReturn(data.holdings_total_return);
    renderRebates(data.rebates);
    renderTable(data.rows);
  }

  function renderKPIs(d) {
    setText("kpi-total", fmt.twd(d.total_twd));
    const ccyParts = Object.entries(d.totals_by_ccy || {}).map(([c, v]) =>
      c === "TWD" ? `NT$${Number(v).toFixed(0)}` : `${c} ${Number(v).toFixed(2)}`,
    );
    setText("kpi-total-sub", ccyParts.join(" · ") || "—");
    const y = d.yields || {};
    setText("kpi-ttm", fmt.twd(y.ttm_dividend_twd || 0));
    setText("kpi-ttm-yield", y.ttm_yield_on_cost != null
      ? `${(y.ttm_yield_on_cost * 100).toFixed(2)}% on cost`
      : "trailing 12-month");
    setText("kpi-yield", y.annualized_yield_on_cost != null
      ? `${(y.annualized_yield_on_cost * 100).toFixed(2)}%`
      : "—");
    setText("kpi-payers", String((d.by_ticker || []).length));
    setText("kpi-events", `${d.count} events`);
  }

  function renderMonthlyChart(monthly) {
    const labels = monthly.map((m) => fmt.month(m.month));
    const tw = monthly.map((m) => m.tw_twd || 0);
    const fr = monthly.map((m) => m.foreign_twd || 0);
    const ctx = document.getElementById("chart-monthly").getContext("2d");
    new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          { label: "TW", data: tw, backgroundColor: charts.cssVar("--c1"), stack: "div", borderRadius: 4 },
          { label: "Foreign", data: fr, backgroundColor: charts.cssVar("--c4"), stack: "div", borderRadius: 4 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: true, position: "bottom" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.twd(c.parsed.y)}` } },
        },
        scales: {
          x: { stacked: true },
          y: { stacked: true, ticks: { callback: (v) => fmt.twdCompact(v) } },
        },
      },
    });
  }

  function renderTopPayers(payers) {
    const el = document.getElementById("top-payers");
    while (el.firstChild) el.removeChild(el.firstChild);
    if (!payers || !payers.length) {
      el.textContent = "No dividend payers yet";
      el.className = "empty-state text-mute";
      return;
    }
    const max = Math.max(1, ...payers.map((p) => p.total_twd));
    for (const p of payers.slice(0, 10)) {
      const row = document.createElement("div");
      row.className = "bar-row";
      const lab = document.createElement("span");
      lab.className = "text-sm";
      lab.style.cssText = "display:flex; gap:8px; align-items:center;";
      const code = document.createElement("strong");
      code.textContent = p.code || "—";
      const name = document.createElement("span");
      name.className = "text-mute text-tiny";
      name.textContent = `${p.name || ""} · ${p.count}×`;
      lab.append(code, name);
      const bar = document.createElement("span");
      bar.className = "bar pos";
      const fill = document.createElement("span");
      fill.style.width = `${(p.total_twd / max * 100).toFixed(2)}%`;
      bar.appendChild(fill);
      const val = document.createElement("span");
      val.className = "num text-sm";
      val.textContent = fmt.twd(p.total_twd);
      row.append(lab, bar, val);
      el.appendChild(row);
    }
  }

  function renderTotalReturn(rows) {
    window.dataTable({
      tableId: "tr-table",
      rows: rows || [],
      searchKeys: ["code", "name", "venue"],
      searchPlaceholder: "Search code, name, venue…",
      filters: [
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
      ],
      defaultSort: { key: "unrealized_pnl_with_div_twd", dir: "desc" },
      colspan: 8,
      pageSize: 15,
      emptyText: "No holdings",
      row: (r) => [
        td(r.venue, "text-mute text-tiny"),
        tdLink(r.code || "", r.code ? `/ticker/${encodeURIComponent(r.code)}` : null, "code"),
        td(r.name || ""),
        td(fmt.twd(r.cost_twd), "num"),
        td(fmt.twd(r.mkt_value_twd), "num"),
        td(fmt.twd(r.cum_dividend_twd), "num"),
        td(fmt.twd(r.unrealized_pnl_with_div_twd),
          "num " + (r.unrealized_pnl_with_div_twd >= 0 ? "value-pos" : "value-neg")),
        td(r.total_return_pct != null ? `${(r.total_return_pct * 100).toFixed(2)}%` : "—",
          "num " + ((r.total_return_pct || 0) >= 0 ? "value-pos" : "value-neg")),
      ],
    });
  }

  function renderRebates(rebates) {
    const tbody = document.querySelector("#rebate-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!rebates || !rebates.length) {
      const tr = document.createElement("tr");
      const td_ = document.createElement("td");
      td_.colSpan = 3; td_.className = "table-empty"; td_.textContent = "No rebates";
      tr.appendChild(td_); tbody.appendChild(tr);
      return;
    }
    for (const r of rebates) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.month(r.month)));
      tr.appendChild(td(r.type));
      tr.appendChild(td(fmt.twd(r.amount_twd), "num value-pos"));
      tbody.appendChild(tr);
    }
  }

  function renderTable(rows) {
    window.dataTable({
      tableId: "div-table",
      rows: rows || [],
      searchKeys: ["code", "name", "venue", "ccy"],
      searchPlaceholder: "Search code, name, venue…",
      filters: [
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
        { id: "ccy",   key: "ccy",   label: "All ccy",    options: distinct(rows, "ccy") },
      ],
      defaultSort: { key: "date", dir: "desc" },
      colspan: 7,
      pageSize: 25,
      emptyText: "No distributions",
      row: (r) => [
        td(fmt.date(r.date), "text-mute"),
        td(r.venue, "text-mute text-tiny"),
        tdLink(r.code || "", r.code ? `/ticker/${encodeURIComponent(r.code)}` : null, "code"),
        td(r.name || ""),
        td(r.ccy || "TWD", "text-mute"),
        td(fmt.num(r.amount_local, 2), "num"),
        td(fmt.twd(r.amount_twd), "num value-pos"),
      ],
    });
  }

  function distinct(rows, key) {
    return [...new Set((rows || []).map((r) => r[key]).filter(Boolean))].sort();
  }

  function td(text, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    el.textContent = text;
    return el;
  }
  function tdLink(text, href, cls) {
    const el = document.createElement("td");
    if (cls) el.className = cls;
    if (href) {
      const a = document.createElement("a");
      a.href = href;
      a.textContent = text;
      el.appendChild(a);
    } else {
      el.textContent = text;
    }
    return el;
  }
  function setText(id, t) { const e = document.getElementById(id); if (e) e.textContent = t; }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load dividends: ${err.message}`;
    main.prepend(box);
  }
})();
