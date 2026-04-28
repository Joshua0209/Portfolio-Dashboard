/**
 * Unified data-table helper.
 *
 * Wraps an existing `<table class="data" id="...">` element with a consistent
 * toolbar (search + optional select filters), client-side sorting (when
 * <th class="sortable" data-key="..."> is present), and pagination via
 * createPager().
 *
 * Usage:
 *   const t = window.dataTable({
 *     tableId: "tx-table",
 *     rows: allRows,
 *     searchKeys: ["code", "name"],   // free-text fields (case-insensitive)
 *     filters: [                      // optional <select> filters
 *       { id: "venue", label: "All venues", options: ["TW", "Foreign"], key: "venue" },
 *     ],
 *     defaultSort: { key: "mkt_value_twd", dir: "desc" },
 *     row: (r) => [                   // returns array of <td> elements
 *       td(r.code, "code"), td(r.name), td(fmt.twd(r.mkt), "num"), ...
 *     ],
 *     emptyText: "No matching rows",
 *     colspan: 13,
 *     pageSize: 50,
 *   });
 *
 *   // Later — swap data without rebuilding:
 *   t.setRows(newRows);
 *
 * Contract: a single <div class="dt-toolbar"></div> is inserted as the
 * preceding sibling of the table's .table-wrap (or the table itself).
 * A <div class="dt-pager"></div> is inserted as the following sibling.
 * Both are removed and re-inserted on every call so it is safe to invoke
 * idempotently.
 */
(function () {
  let _seq = 0;

  function dataTable(cfg) {
    const table = document.getElementById(cfg.tableId);
    if (!table) throw new Error(`dataTable: #${cfg.tableId} not found`);
    const wrap = table.closest(".table-wrap") || table;
    const card = wrap.parentElement;
    const id = `dt-${++_seq}`;

    let rows = cfg.rows || [];
    let searchValue = "";
    const filterValues = Object.create(null);
    let sortKey = cfg.defaultSort?.key || null;
    let sortDir = cfg.defaultSort?.dir || "desc";

    // ── Toolbar ────────────────────────────────────────────────────────
    const toolbarId = `${id}-toolbar`;
    const pagerId   = `${id}-pager`;
    let toolbar = document.getElementById(toolbarId);
    if (!toolbar) {
      toolbar = document.createElement("div");
      toolbar.id = toolbarId;
      toolbar.className = "dt-toolbar";
      card.insertBefore(toolbar, wrap);
    } else {
      while (toolbar.firstChild) toolbar.removeChild(toolbar.firstChild);
    }

    const searchInput = document.createElement("input");
    searchInput.className = "input dt-search";
    searchInput.placeholder = cfg.searchPlaceholder || "Search…";
    searchInput.setAttribute("aria-label", "Search rows");
    searchInput.addEventListener("input", () => {
      searchValue = (searchInput.value || "").toLowerCase();
      pager.reset();
      rerender();
    });
    toolbar.appendChild(searchInput);

    for (const f of cfg.filters || []) {
      const sel = document.createElement("select");
      sel.className = "select dt-filter";
      sel.setAttribute("aria-label", f.label || f.id);
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = f.label || `All ${f.id}`;
      sel.appendChild(blank);
      for (const o of (f.options || [])) {
        const opt = document.createElement("option");
        if (typeof o === "object") {
          opt.value = o.value;
          opt.textContent = o.label;
        } else {
          opt.value = String(o);
          opt.textContent = String(o);
        }
        sel.appendChild(opt);
      }
      sel.addEventListener("change", () => {
        filterValues[f.key || f.id] = sel.value;
        pager.reset();
        rerender();
      });
      toolbar.appendChild(sel);
    }

    if (cfg.toolbarExtras) {
      const extras = cfg.toolbarExtras();
      if (extras instanceof Node) toolbar.appendChild(extras);
    }

    // Counter on the right side of the toolbar.
    const spacer = document.createElement("span");
    spacer.style.cssText = "flex:1";
    toolbar.appendChild(spacer);
    const counter = document.createElement("span");
    counter.className = "dt-counter text-mute text-tiny";
    toolbar.appendChild(counter);

    // ── Pager ──────────────────────────────────────────────────────────
    let pagerHost = document.getElementById(pagerId);
    if (!pagerHost) {
      pagerHost = document.createElement("div");
      pagerHost.id = pagerId;
      pagerHost.className = "dt-pager";
      if (wrap.nextSibling) card.insertBefore(pagerHost, wrap.nextSibling);
      else card.appendChild(pagerHost);
    } else {
      while (pagerHost.firstChild) pagerHost.removeChild(pagerHost.firstChild);
    }

    const pager = window.createPager({
      containerId: pagerId,
      pageSize: cfg.pageSize || 25,
      onChange: renderRows,
    });

    // ── Sortable headers (opt-in via th.sortable[data-key]) ────────────
    const headers = table.querySelectorAll("thead th.sortable[data-key]");
    headers.forEach((th) => {
      th.addEventListener("click", () => {
        const k = th.dataset.key;
        if (sortKey === k) sortDir = sortDir === "asc" ? "desc" : "asc";
        else { sortKey = k; sortDir = "desc"; }
        rerender();
      });
    });

    function applySortIndicators() {
      headers.forEach((th) => {
        th.classList.remove("sorted-asc", "sorted-desc");
        if (th.dataset.key === sortKey) {
          th.classList.add(sortDir === "asc" ? "sorted-asc" : "sorted-desc");
        }
      });
    }

    function predicate(r) {
      // Free-text search across configured keys.
      if (searchValue) {
        const keys = cfg.searchKeys || [];
        const hit = keys.some((k) => String(r[k] ?? "").toLowerCase().includes(searchValue));
        if (!hit) return false;
      }
      for (const [k, v] of Object.entries(filterValues)) {
        if (!v) continue;
        // Custom predicate via filter spec? Look it up by key.
        const f = (cfg.filters || []).find((x) => (x.key || x.id) === k);
        if (f && typeof f.predicate === "function") {
          if (!f.predicate(r, v)) return false;
        } else if (String(r[k] ?? "") !== v) {
          return false;
        }
      }
      return cfg.extraFilter ? cfg.extraFilter(r) : true;
    }

    function compare(a, b) {
      if (!sortKey) return 0;
      const av = a[sortKey], bv = b[sortKey];
      const an = (av === null || av === undefined) ? -Infinity : av;
      const bn = (bv === null || bv === undefined) ? -Infinity : bv;
      if (typeof an === "number" && typeof bn === "number") {
        return sortDir === "asc" ? an - bn : bn - an;
      }
      const as = String(av ?? ""), bs = String(bv ?? "");
      return sortDir === "asc" ? as.localeCompare(bs) : bs.localeCompare(as);
    }

    function filteredSorted() {
      const f = rows.filter(predicate);
      if (sortKey) f.sort(compare);
      return f;
    }

    function rerender() {
      // Compute filteredSorted() once and pass through to renderRows so
      // large tables (transactions, holdings) don't redo filter+sort on
      // every keystroke. Both calls produce the same result because
      // `rows` is not mutated between them.
      const f = filteredSorted();
      counter.textContent = `${f.length.toLocaleString()} of ${rows.length.toLocaleString()} rows`;
      pager.update(f.length);
      applySortIndicators();
      renderRows(f);
    }

    function renderRows(precomputed) {
      const f = precomputed || filteredSorted();
      const slice = pager.slice(f);
      const tbody = table.querySelector("tbody");
      while (tbody.firstChild) tbody.removeChild(tbody.firstChild);

      if (!slice.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = cfg.colspan || 12;
        td.className = "table-empty";
        td.textContent = cfg.emptyText || "No rows";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }

      for (const r of slice) {
        const tr = document.createElement("tr");
        const cells = cfg.row(r);
        for (const c of cells) tr.appendChild(c);
        tbody.appendChild(tr);
      }
    }

    rerender();

    return {
      setRows(newRows) { rows = newRows || []; pager.reset(); rerender(); },
      rerender,
      filtered: filteredSorted,
    };
  }

  window.dataTable = dataTable;
})();
