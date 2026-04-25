/**
 * Dividends page: monthly income chart, top payers, full distribution log.
 */
(function () {
  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const data = await window.api.get("/api/dividends");
    renderKPIs(data);
    renderMonthlyChart(data.rows);
    renderTopPayers(data.by_ticker);
    renderTable(data.rows);
  }

  function renderKPIs(d) {
    setText("kpi-total", fmt.twd(d.total_twd));
    const ccyParts = Object.entries(d.totals_by_ccy || {}).map(([c, v]) =>
      c === "TWD" ? `NT$${Number(v).toLocaleString()}` : `${c} ${Number(v).toLocaleString()}`
    );
    setText("kpi-total-sub", ccyParts.join(" · ") || "—");
    setText("kpi-count", String(d.count));
    setText("kpi-payers", String((d.by_ticker || []).length));
  }

  function renderMonthlyChart(rows) {
    const byMonth = {};
    for (const r of rows) {
      byMonth[r.month] = (byMonth[r.month] || 0) + (r.amount_twd || 0);
    }
    const labels = Object.keys(byMonth).sort().map((m) => fmt.month(m));
    const data = Object.keys(byMonth).sort().map((m) => byMonth[m]);
    const ctx = document.getElementById("chart-monthly").getContext("2d");
    new Chart(ctx, {
      type: "bar",
      data: { labels, datasets: [{ label: "Income", data, backgroundColor: charts.cssVar("--c4"), borderRadius: 4 }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => fmt.twd(c.parsed.y) } } },
        scales: { y: { ticks: { callback: (v) => fmt.twdCompact(v) } } },
      },
    });
  }

  function renderTopPayers(payers) {
    const el = document.getElementById("top-payers");
    while (el.firstChild) el.removeChild(el.firstChild);
    if (!payers.length) {
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
      name.textContent = p.name || "";
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

  function renderTable(rows) {
    const tbody = document.querySelector("#div-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 7;
      td.className = "table-empty";
      td.textContent = "No distributions";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.appendChild(td(fmt.month(r.month)));
      tr.appendChild(td(fmt.date(r.date), "text-mute"));
      tr.appendChild(tdLink(r.code || "", r.code ? `/ticker/${encodeURIComponent(r.code)}` : null, "code"));
      tr.appendChild(td(r.name || (r.is_rebate ? "Rebate" : "")));
      tr.appendChild(td(r.ccy || "TWD", "text-mute"));
      tr.appendChild(td(fmt.num(r.amount_local, 2), "num"));
      tr.appendChild(td(fmt.twd(r.amount_twd), "num value-pos"));
      tbody.appendChild(tr);
    }
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
