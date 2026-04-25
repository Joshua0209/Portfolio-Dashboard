/**
 * Tax / P&L page: realized + unrealized gains by ticker.
 */
(function () {
  let allRows = [];

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();
    const d = await window.api.get("/api/tax");
    renderKPIs(d.totals);
    allRows = d.by_ticker;
    renderMovers();
    bindFilters();
    rerender();
    document.getElementById("export-tax").addEventListener("click", exportCsv);
  }

  function renderKPIs(t) {
    setColored("kpi-real", fmt.twd(t.realized_pnl_twd), t.realized_pnl_twd);
    setColored("kpi-unreal", fmt.twd(t.unrealized_pnl_twd), t.unrealized_pnl_twd);
    setColored("kpi-total", fmt.twd(t.total_pnl_twd), t.total_pnl_twd);
    setText("kpi-win", fmt.pctAbs(t.win_rate, 1));
    setText("kpi-win-sub", `${t.winners_count} winners · ${t.losers_count} losers`);
  }
  function setColored(id, txt, val) {
    const el = document.getElementById(id);
    el.textContent = txt;
    el.className = "kpi-value " + fmt.tone(val);
  }

  function renderMovers() {
    const sorted = [...allRows].sort((a, b) => b.realized_pnl_twd - a.realized_pnl_twd);
    populate("winners-list", sorted.slice(0, 5).filter((r) => r.realized_pnl_twd > 0), "pos");
    populate("losers-list", sorted.slice(-5).reverse().filter((r) => r.realized_pnl_twd < 0), "neg");
  }

  function populate(elId, rows, tone) {
    const el = document.getElementById(elId);
    while (el.firstChild) el.removeChild(el.firstChild);
    if (!rows.length) {
      el.textContent = "Nothing here";
      el.className = "empty-state text-mute";
      return;
    }
    const max = Math.max(1, ...rows.map((r) => Math.abs(r.realized_pnl_twd)));
    for (const r of rows) {
      const row = document.createElement("a");
      row.href = `/ticker/${encodeURIComponent(r.code || "")}`;
      row.className = "bar-row";
      row.style.color = "inherit";
      const lab = document.createElement("span");
      lab.className = "text-sm";
      lab.style.cssText = "display:flex; gap:8px; align-items:center;";
      const code = document.createElement("strong");
      code.textContent = r.code;
      const name = document.createElement("span");
      name.className = "text-mute text-tiny";
      name.textContent = r.name || "";
      lab.append(code, name);
      const bar = document.createElement("span");
      bar.className = "bar " + tone;
      const fill = document.createElement("span");
      fill.style.width = `${Math.abs(r.realized_pnl_twd) / max * 100}%`;
      bar.appendChild(fill);
      const val = document.createElement("span");
      val.className = `num text-sm value-${tone}`;
      val.textContent = fmt.twd(r.realized_pnl_twd);
      row.append(lab, bar, val);
      el.appendChild(row);
    }
  }

  function bindFilters() {
    document.getElementById("q").addEventListener("input", rerender);
    document.getElementById("filter").addEventListener("change", rerender);
  }

  function filtered() {
    const q = (document.getElementById("q").value || "").toLowerCase();
    const f = document.getElementById("filter").value;
    return allRows.filter((r) => {
      if (q) {
        const code = String(r.code || "").toLowerCase();
        const name = String(r.name || "").toLowerCase();
        if (!code.includes(q) && !name.includes(q)) return false;
      }
      if (f === "closed" && !r.fully_closed) return false;
      if (f === "open" && r.fully_closed) return false;
      if (f === "winners" && r.realized_pnl_twd <= 0) return false;
      if (f === "losers" && r.realized_pnl_twd >= 0) return false;
      return true;
    });
  }

  function rerender() {
    const rows = filtered();
    rows.sort((a, b) => (b.total_pnl_twd || 0) - (a.total_pnl_twd || 0));
    const tbody = document.querySelector("#tax-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 12;
      td.className = "table-empty";
      td.textContent = "No matching tickers";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const r of rows) {
      const tr = document.createElement("tr");
      const codeTd = document.createElement("td");
      codeTd.className = "code";
      const a = document.createElement("a");
      a.href = `/ticker/${encodeURIComponent(r.code || "")}`;
      a.textContent = r.code;
      codeTd.appendChild(a);
      tr.appendChild(codeTd);
      tr.appendChild(td(r.name || ""));
      tr.appendChild(td(fmt.int(r.buy_qty), "num"));
      tr.appendChild(td(fmt.int(r.sell_qty), "num"));
      tr.appendChild(td(fmt.num(r.avg_buy_price_twd, 2), "num"));
      tr.appendChild(td(fmt.twd(r.cost_of_sold_twd), "num text-mute"));
      tr.appendChild(td(fmt.twd(r.sell_proceeds_twd), "num"));
      tr.appendChild(td(fmt.twd(r.fees_twd), "num text-mute"));
      tr.appendChild(td(fmt.twd(r.tax_twd), "num text-mute"));
      tr.appendChild(td(fmt.twd(r.realized_pnl_twd), `num ${fmt.tone(r.realized_pnl_twd)}`));
      tr.appendChild(td(fmt.twd(r.unrealized_pnl_twd), `num ${fmt.tone(r.unrealized_pnl_twd)}`));
      tr.appendChild(td(fmt.twd(r.total_pnl_twd), `num ${fmt.tone(r.total_pnl_twd)}`));
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

  function exportCsv() {
    const rows = filtered();
    const headers = [
      "code", "name", "buy_qty", "sell_qty", "avg_buy_price_twd",
      "cost_of_sold_twd", "sell_proceeds_twd", "fees_twd", "tax_twd",
      "realized_pnl_twd", "unrealized_pnl_twd", "total_pnl_twd", "fully_closed",
    ];
    const lines = [headers.join(",")];
    for (const r of rows) lines.push(headers.map((k) => csvCell(r[k])).join(","));
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `tax-pnl-${new Date().toISOString().slice(0, 10)}.csv`;
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
    box.textContent = `Failed to load tax: ${err.message}`;
    main.prepend(box);
  }
})();
