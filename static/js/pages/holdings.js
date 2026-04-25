/**
 * Holdings page: KPIs, treemap, sector breakdown, sortable/filterable table.
 */
(function () {
  let allRows = [];
  let sortKey = "mkt_value_twd";
  let sortDir = "desc";

  document.addEventListener("DOMContentLoaded", () => init().catch(showError));

  async function init() {
    window.charts.applyDefaults();

    const [hold, sectors] = await Promise.all([
      window.api.get("/api/holdings/current"),
      window.api.get("/api/holdings/sectors"),
    ]);

    renderKPIs(hold);
    renderTreemap(hold.holdings);
    renderSectors(sectors);
    allRows = hold.holdings;
    bindFilters();
    bindSorting();
    rerenderTable();
    document.getElementById("export-holdings").addEventListener("click", exportCsv);
  }

  function renderKPIs(d) {
    setText("kpi-mv", fmt.twd(d.total_mv_twd));
    setText("kpi-count", String(d.holdings.length));
    setText("kpi-cost", fmt.twd(d.total_cost_twd));
    const upnl = document.getElementById("kpi-upnl");
    upnl.textContent = fmt.twd(d.total_upnl_twd);
    upnl.className = "kpi-value " + fmt.tone(d.total_upnl_twd);
    const pctEl = document.getElementById("kpi-upnl-pct");
    pctEl.textContent = fmt.pct(d.total_upnl_pct);
    pctEl.className = "kpi-sub " + fmt.tone(d.total_upnl_pct);
    setText("kpi-fx", d.fx_usd_twd ? d.fx_usd_twd.toFixed(3) : "—");
    setText("kpi-fx-sub", `as of ${fmt.month(d.as_of)}`);
  }

  function renderTreemap(rows) {
    const grid = document.getElementById("treemap");
    while (grid.firstChild) grid.removeChild(grid.firstChild);
    const total = rows.reduce((s, r) => s + r.mkt_value_twd, 0) || 1;
    const sorted = [...rows].sort((a, b) => b.mkt_value_twd - a.mkt_value_twd);

    const cols = 6;
    grid.style.gridTemplateColumns = `repeat(${cols}, 1fr)`;
    grid.style.gridAutoRows = "minmax(64px, auto)";

    const colors = [
      ["--c1", 0.85], ["--c1", 0.55], ["--c2", 0.85], ["--c2", 0.55],
      ["--c3", 0.85], ["--c3", 0.55], ["--c4", 0.85], ["--c4", 0.55],
      ["--c6", 0.85], ["--c6", 0.55], ["--c7", 0.85], ["--c7", 0.55],
    ];

    sorted.forEach((r, i) => {
      const weight = r.mkt_value_twd / total;
      const span = Math.max(1, Math.min(cols, Math.round(weight * cols * 6)));
      const tile = document.createElement("a");
      tile.href = `/ticker/${encodeURIComponent(r.code || "")}`;
      tile.className = "tile";
      tile.style.gridColumn = `span ${Math.min(span, 4)}`;
      const isPos = (r.unrealized_pnl_twd || 0) >= 0;
      const baseColor = isPos ? "var(--pos)" : "var(--neg)";
      tile.style.background = `linear-gradient(135deg, ${baseColor} 0%, var(--bg-elev-2) 80%)`;
      tile.style.color = "var(--text)";
      tile.style.minHeight = `${Math.max(60, weight * 600)}px`;

      const top = document.createElement("div");
      const code = document.createElement("strong");
      code.style.fontSize = "13px";
      code.textContent = r.code || "";
      const name = document.createElement("div");
      name.className = "text-tiny text-mute";
      name.textContent = r.name || "";
      top.append(code, name);

      const bot = document.createElement("div");
      bot.className = "text-tiny num";
      const w = document.createElement("div");
      w.textContent = `${(weight * 100).toFixed(1)}%`;
      const p = document.createElement("div");
      p.style.color = isPos ? "var(--pos-soft)" : "var(--neg-soft)";
      p.textContent = fmt.pct(r.unrealized_pct);
      bot.append(w, p);

      tile.append(top, bot);
      grid.appendChild(tile);
    });
  }

  function renderSectors(sectors) {
    const el = document.getElementById("sector-list");
    while (el.firstChild) el.removeChild(el.firstChild);
    const total = sectors.reduce((s, x) => s + x.value_twd, 0) || 1;
    sectors.forEach((sec, i) => {
      const row = document.createElement("div");
      row.className = "bar-row";
      const label = document.createElement("span");
      label.className = "text-sm";
      label.style.cssText = "display:flex; gap:8px; align-items:center;";
      const sw = document.createElement("i");
      sw.style.cssText = `width:8px;height:8px;border-radius:2px;background:var(--c${(i % 8) + 1});display:inline-block;`;
      label.append(sw, document.createTextNode(sec.sector));
      const sub = document.createElement("span");
      sub.className = "text-mute text-tiny";
      sub.style.marginLeft = "8px";
      sub.textContent = `${sec.count}`;
      label.append(sub);

      const bar = document.createElement("span");
      bar.className = "bar";
      const fill = document.createElement("span");
      fill.style.width = `${(sec.value_twd / total * 100).toFixed(2)}%`;
      bar.appendChild(fill);

      const pct = document.createElement("span");
      pct.className = "num text-sm";
      pct.textContent = `${(sec.value_twd / total * 100).toFixed(1)}%`;

      row.append(label, bar, pct);
      el.appendChild(row);
    });
  }

  function bindFilters() {
    document.getElementById("filter-q").addEventListener("input", rerenderTable);
    document.getElementById("filter-venue").addEventListener("change", rerenderTable);
  }

  function bindSorting() {
    document.querySelectorAll("#holdings-table thead th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const k = th.dataset.key;
        if (sortKey === k) {
          sortDir = sortDir === "asc" ? "desc" : "asc";
        } else {
          sortKey = k;
          sortDir = "desc";
        }
        rerenderTable();
      });
    });
  }

  function filteredRows() {
    const q = (document.getElementById("filter-q").value || "").toLowerCase();
    const v = document.getElementById("filter-venue").value;
    return allRows.filter((r) => {
      if (v && r.venue !== v) return false;
      if (!q) return true;
      const code = String(r.code || "").toLowerCase();
      const name = String(r.name || "").toLowerCase();
      return code.includes(q) || name.includes(q);
    });
  }

  function rerenderTable() {
    const rows = filteredRows();
    rows.sort((a, b) => {
      const av = a[sortKey], bv = b[sortKey];
      const an = (av === null || av === undefined) ? -Infinity : av;
      const bn = (bv === null || bv === undefined) ? -Infinity : bv;
      if (typeof an === "number" && typeof bn === "number") {
        return sortDir === "asc" ? an - bn : bn - an;
      }
      const as = String(av ?? ""), bs = String(bv ?? "");
      return sortDir === "asc" ? as.localeCompare(bs) : bs.localeCompare(as);
    });

    document.querySelectorAll("#holdings-table thead th.sortable").forEach((th) => {
      th.classList.remove("sorted-asc", "sorted-desc");
      if (th.dataset.key === sortKey) {
        th.classList.add(sortDir === "asc" ? "sorted-asc" : "sorted-desc");
      }
    });

    const tbody = document.querySelector("#holdings-table tbody");
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 13;
      td.className = "table-empty";
      td.textContent = "No matching positions";
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
      a.textContent = r.code || "";
      codeTd.appendChild(a);
      tr.appendChild(codeTd);
      tr.appendChild(td(r.name || ""));
      tr.appendChild(tdPill(r.venue));
      tr.appendChild(td(r.type || ""));
      tr.appendChild(td(r.ccy || ""));
      tr.appendChild(td(fmt.int(r.qty), "num"));
      tr.appendChild(td(fmt.num(r.avg_cost, 2), "num"));
      tr.appendChild(td(fmt.num(r.ref_price, 2), "num"));
      tr.appendChild(td(fmt.twd(r.cost_twd), "num"));
      tr.appendChild(td(fmt.twd(r.mkt_value_twd), "num"));
      tr.appendChild(td(fmt.twd(r.unrealized_pnl_twd), `num ${fmt.tone(r.unrealized_pnl_twd)}`));
      tr.appendChild(td(fmt.pct(r.unrealized_pct), `num ${fmt.tone(r.unrealized_pct)}`));
      tr.appendChild(td(fmt.pctAbs(r.weight, 1), "num"));
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
    const pill = document.createElement("span");
    pill.className = "pill";
    pill.textContent = text || "";
    el.appendChild(pill);
    return el;
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function exportCsv() {
    const rows = filteredRows();
    const headers = [
      "code", "name", "venue", "type", "ccy", "qty", "avg_cost", "ref_price",
      "cost_twd", "mkt_value_twd", "unrealized_pnl_twd", "unrealized_pct", "weight",
    ];
    const lines = [headers.join(",")];
    for (const r of rows) {
      lines.push(headers.map((k) => csvCell(r[k])).join(","));
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `holdings-${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function csvCell(v) {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (s.includes(",") || s.includes("\"") || s.includes("\n")) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  }

  function showError(err) {
    const main = document.querySelector(".content");
    const box = document.createElement("div");
    box.className = "error-box";
    box.textContent = `Failed to load holdings: ${err.message}`;
    main.prepend(box);
  }
})();
