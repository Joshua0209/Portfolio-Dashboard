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
    const canvas = document.getElementById("treemap");
    if (!canvas || !rows.length) return;

    // Squarified treemap via chartjs-chart-treemap plugin.
    // Tree array: each node carries the row payload plus a `_value` we want
    // to size by. The plugin reads `key: '_value'` to lay out rectangles.
    const total = rows.reduce((s, r) => s + (r.mkt_value_twd || 0), 0) || 1;
    const tree = rows
      .filter((r) => (r.mkt_value_twd || 0) > 0)
      .map((r) => ({
        code: r.code || "",
        name: r.name || "",
        venue: r.venue || "",
        mkt: r.mkt_value_twd || 0,
        upnl: r.unrealized_pnl_twd || 0,
        upct: r.unrealized_pct || 0,
        weight: (r.mkt_value_twd || 0) / total,
        _value: r.mkt_value_twd || 0,
      }));

    // Color: green for gainers, red for losers. Intensity scales with |upct|
    // capped at 30% so a single 200% outlier doesn't wash out everyone else.
    const posBase = charts.cssVar("--pos");
    const negBase = charts.cssVar("--neg");
    function colorFor(ctx) {
      const item = ctx?.raw?._data;
      if (!item) return charts.cssVar("--bg-elev-2");
      const intensity = Math.min(1, Math.abs(item.upct || 0) / 0.30);
      const alpha = 0.30 + 0.55 * intensity;
      const base = (item.upnl || 0) >= 0 ? posBase : negBase;
      return charts.hexWithAlpha(base, alpha);
    }

    const config = {
      type: "treemap",
      data: {
        datasets: [{
          tree,
          key: "_value",
          borderWidth: 1,
          borderColor: charts.cssVar("--bg"),
          spacing: 1,
          backgroundColor: colorFor,
          labels: {
            display: true,
            align: "left",
            position: "top",
            color: charts.cssVar("--text"),
            font: { size: 11, weight: "600", family: charts.cssVar("--font-mono") || "monospace" },
            padding: 4,
            formatter: (ctx) => {
              const d = ctx?.raw?._data;
              if (!d) return "";
              const w = (d.weight * 100).toFixed(1);
              const pct = fmt.pct(d.upct);
              return [d.code, `${w}% · ${pct}`];
            },
          },
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => {
                const d = items?.[0]?.raw?._data;
                return d ? `${d.code} · ${d.name}` : "";
              },
              label: (ctx) => {
                const d = ctx?.raw?._data;
                if (!d) return "";
                return [
                  `Market value: ${fmt.twd(d.mkt)}`,
                  `Weight: ${(d.weight * 100).toFixed(2)}%`,
                  `Unrealized: ${fmt.twd(d.upnl)} (${fmt.pct(d.upct)})`,
                  `Venue: ${d.venue}`,
                ];
              },
            },
          },
        },
      },
    };

    if (window._treemapChart) window._treemapChart.destroy();
    window._treemapChart = new Chart(canvas.getContext("2d"), config);

    // Click-through: navigate to per-ticker page on rectangle click.
    canvas.onclick = (evt) => {
      const points = window._treemapChart.getElementsAtEventForMode(evt, "nearest", { intersect: true }, false);
      if (!points.length) return;
      const data = points[0]?.element?.$context?.raw?._data;
      if (data?.code) window.location.href = `/ticker/${encodeURIComponent(data.code)}`;
    };
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
