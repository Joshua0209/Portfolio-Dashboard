// DataTable — typed port of static/js/data-table.js + pagination.js.
// Phase 8 Cycle 58.
//
// Wraps an existing <table class="data" id="..."> inside its enclosing
// .card with a toolbar (search + optional filters + counter) and a
// pager. Sortable headers (th.sortable[data-key]) get click handlers
// that toggle asc/desc + apply .sorted-asc / .sorted-desc for the
// CSS arrow indicator app.css already styles.

export type SortDir = "asc" | "desc";

export interface SortSpec<T> {
  key: keyof T;
  dir: SortDir;
}

export type FilterOption =
  | string
  | number
  | { value: string; label: string };

export interface FilterSpec<T> {
  id: string;
  label?: string;
  options?: ReadonlyArray<FilterOption>;
  key?: keyof T;
  predicate?: (row: T, value: string) => boolean;
}

export interface DataTableConfig<T> {
  tableId: string;
  rows: ReadonlyArray<T>;
  searchKeys: ReadonlyArray<keyof T>;
  row: (row: T) => HTMLTableCellElement[];
  colspan: number;
  filters?: ReadonlyArray<FilterSpec<T>>;
  defaultSort?: SortSpec<T>;
  pageSize?: number;
  emptyText?: string;
  searchPlaceholder?: string;
  toolbarExtras?: () => Node | null;
  extraFilter?: (row: T) => boolean;
}

export interface DataTableHandle<T> {
  setRows(rows: ReadonlyArray<T>): void;
  rerender(): void;
  filtered(): T[];
}

type PageSize = number | "all";

interface Pager {
  slice<T>(rows: ReadonlyArray<T>): T[];
  update(total: number): void;
  reset(): void;
}

const PAGE_SIZES: ReadonlyArray<PageSize> = [25, 50, 100, 200, "all"];

const createPager = (
  containerId: string,
  pageSize: PageSize,
  onChange: () => void,
): Pager => {
  const container = document.getElementById(containerId);
  let page = 1;
  let size: PageSize = pageSize;
  let total = 0;

  const pageCount = (): number => {
    if (size === "all") return 1;
    return Math.max(1, Math.ceil(total / size));
  };

  const render = (): void => {
    if (!container) return;
    while (container.firstChild) container.removeChild(container.firstChild);

    const wrap = document.createElement("div");
    wrap.className = "pager";

    const sizeLabel = document.createElement("label");
    sizeLabel.className = "pager-size";
    const sizeText = document.createElement("span");
    sizeText.className = "text-mute text-tiny";
    sizeText.textContent = "Rows per page:";
    const select = document.createElement("select");
    select.className = "select";
    for (const s of PAGE_SIZES) {
      const opt = document.createElement("option");
      opt.value = String(s);
      opt.textContent = String(s);
      if (String(size) === String(s)) opt.selected = true;
      select.appendChild(opt);
    }
    select.addEventListener("change", () => {
      size = select.value === "all" ? "all" : Number(select.value);
      page = 1;
      render();
      onChange();
    });
    sizeLabel.append(sizeText, select);

    const nav = document.createElement("div");
    nav.className = "pager-nav";

    const prev = document.createElement("button");
    prev.className = "btn btn-sm";
    prev.textContent = "‹ Prev";
    prev.disabled = page <= 1;
    prev.addEventListener("click", () => {
      page = Math.max(1, page - 1);
      render();
      onChange();
    });

    const counter = document.createElement("span");
    counter.className = "pager-counter num text-mute text-tiny";
    const start = total === 0 ? 0 : size === "all" ? 1 : (page - 1) * size + 1;
    const end =
      total === 0 ? 0 : size === "all" ? total : Math.min(total, page * size);
    counter.textContent = `${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}`;

    const next = document.createElement("button");
    next.className = "btn btn-sm";
    next.textContent = "Next ›";
    next.disabled = page >= pageCount();
    next.addEventListener("click", () => {
      page = Math.min(pageCount(), page + 1);
      render();
      onChange();
    });

    nav.append(prev, counter, next);
    wrap.append(sizeLabel, nav);
    container.appendChild(wrap);
  };

  const slice = <T,>(rows: ReadonlyArray<T>): T[] => {
    total = rows.length;
    if (page > pageCount()) page = pageCount();
    if (size === "all") return rows.slice();
    const start = (page - 1) * size;
    return rows.slice(start, start + size);
  };

  const update = (newTotal: number): void => {
    total = newTotal;
    if (page > pageCount()) page = pageCount();
    render();
  };

  const reset = (): void => {
    page = 1;
    render();
  };

  render();
  return { slice, update, reset };
};

let _seq = 0;

const cmp = <T,>(a: T, b: T, key: keyof T, dir: SortDir): number => {
  const av = a[key];
  const bv = b[key];
  const an =
    av === null || av === undefined ? Number.NEGATIVE_INFINITY : (av as unknown);
  const bn =
    bv === null || bv === undefined ? Number.NEGATIVE_INFINITY : (bv as unknown);
  if (typeof an === "number" && typeof bn === "number") {
    return dir === "asc" ? an - bn : bn - an;
  }
  const as = String(av ?? "");
  const bs = String(bv ?? "");
  return dir === "asc" ? as.localeCompare(bs) : bs.localeCompare(as);
};

export const mountDataTable = <T,>(
  cfg: DataTableConfig<T>,
): DataTableHandle<T> => {
  const table = document.getElementById(cfg.tableId) as HTMLTableElement | null;
  if (!table) {
    throw new Error(`mountDataTable: #${cfg.tableId} not found`);
  }
  const wrap =
    (table.closest(".table-wrap") as HTMLElement | null) ?? (table as HTMLElement);
  const card = wrap.parentElement;
  if (!card) {
    throw new Error(`mountDataTable: #${cfg.tableId} has no parent card`);
  }

  // Idempotent: tear down any prior toolbar/pager for the same tableId.
  const priorToolbar = card.querySelector(`[data-dt-toolbar="${cfg.tableId}"]`);
  if (priorToolbar) priorToolbar.remove();
  const priorPager = card.querySelector(`[data-dt-pager="${cfg.tableId}"]`);
  if (priorPager) priorPager.remove();

  const id = `dt-${++_seq}`;
  const pagerId = `${id}-pager`;

  let rows: ReadonlyArray<T> = cfg.rows;
  let searchValue = "";
  const filterValues: Record<string, string> = Object.create(null);
  let sortKey: keyof T | null = cfg.defaultSort?.key ?? null;
  let sortDir: SortDir = cfg.defaultSort?.dir ?? "desc";

  // Toolbar
  const toolbar = document.createElement("div");
  toolbar.className = "dt-toolbar";
  toolbar.setAttribute("data-dt-toolbar", cfg.tableId);
  card.insertBefore(toolbar, wrap);

  const searchInput = document.createElement("input");
  searchInput.className = "input dt-search";
  searchInput.placeholder = cfg.searchPlaceholder ?? "Search…";
  searchInput.setAttribute("aria-label", "Search rows");
  searchInput.addEventListener("input", () => {
    searchValue = (searchInput.value ?? "").toLowerCase();
    pager.reset();
    rerender();
  });
  toolbar.appendChild(searchInput);

  for (const f of cfg.filters ?? []) {
    const sel = document.createElement("select");
    sel.className = "select dt-filter";
    sel.setAttribute("aria-label", f.label ?? f.id);
    const blank = document.createElement("option");
    blank.value = "";
    blank.textContent = f.label ?? `All ${f.id}`;
    sel.appendChild(blank);
    for (const o of f.options ?? []) {
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
      filterValues[f.id] = sel.value;
      pager.reset();
      rerender();
    });
    toolbar.appendChild(sel);
  }

  if (cfg.toolbarExtras) {
    const extras = cfg.toolbarExtras();
    if (extras instanceof Node) toolbar.appendChild(extras);
  }

  const spacer = document.createElement("span");
  spacer.style.flex = "1";
  toolbar.appendChild(spacer);
  const counter = document.createElement("span");
  counter.className = "dt-counter text-mute text-tiny";
  toolbar.appendChild(counter);

  // Pager host
  const pagerHost = document.createElement("div");
  pagerHost.id = pagerId;
  pagerHost.className = "dt-pager";
  pagerHost.setAttribute("data-dt-pager", cfg.tableId);
  if (wrap.nextSibling) {
    card.insertBefore(pagerHost, wrap.nextSibling);
  } else {
    card.appendChild(pagerHost);
  }

  const pager = createPager(pagerId, cfg.pageSize ?? 25, () => renderRows());

  // Sortable headers
  const headers = Array.from(
    table.querySelectorAll<HTMLTableCellElement>("thead th.sortable[data-key]"),
  );
  for (const th of headers) {
    th.addEventListener("click", () => {
      const k = th.dataset.key as keyof T | undefined;
      if (!k) return;
      if (sortKey === k) {
        sortDir = sortDir === "asc" ? "desc" : "asc";
      } else {
        sortKey = k;
        sortDir = "desc";
      }
      rerender();
    });
  }

  const applySortIndicators = (): void => {
    for (const th of headers) {
      th.classList.remove("sorted-asc", "sorted-desc");
      if ((th.dataset.key as keyof T | undefined) === sortKey) {
        th.classList.add(sortDir === "asc" ? "sorted-asc" : "sorted-desc");
      }
    }
  };

  const predicate = (r: T): boolean => {
    if (searchValue) {
      const hit = cfg.searchKeys.some((k) =>
        String((r as Record<keyof T, unknown>)[k] ?? "")
          .toLowerCase()
          .includes(searchValue),
      );
      if (!hit) return false;
    }
    for (const [id, v] of Object.entries(filterValues)) {
      if (!v) continue;
      const f = (cfg.filters ?? []).find((x) => x.id === id);
      if (f?.predicate) {
        if (!f.predicate(r, v)) return false;
      } else if (f) {
        const k = (f.key ?? f.id) as keyof T;
        if (String((r as Record<keyof T, unknown>)[k] ?? "") !== v) return false;
      }
    }
    return cfg.extraFilter ? cfg.extraFilter(r) : true;
  };

  const filteredSorted = (): T[] => {
    const f = rows.filter(predicate);
    if (sortKey) {
      const key = sortKey;
      const dir = sortDir;
      f.sort((a, b) => cmp(a, b, key, dir));
    }
    return f;
  };

  const renderRows = (precomputed?: T[]): void => {
    const f = precomputed ?? filteredSorted();
    const slice = pager.slice(f);
    const tbody = table.querySelector("tbody");
    if (!tbody) return;
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);

    if (!slice.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = cfg.colspan;
      td.className = "table-empty";
      td.textContent = cfg.emptyText ?? "No rows";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    for (const r of slice) {
      const tr = document.createElement("tr");
      for (const c of cfg.row(r)) tr.appendChild(c);
      tbody.appendChild(tr);
    }
  };

  const rerender = (): void => {
    const f = filteredSorted();
    counter.textContent = `${f.length.toLocaleString()} of ${rows.length.toLocaleString()} rows`;
    pager.update(f.length);
    applySortIndicators();
    renderRows(f);
  };

  rerender();

  return {
    setRows(newRows) {
      rows = newRows;
      pager.reset();
      rerender();
    },
    rerender,
    filtered: filteredSorted,
  };
};
