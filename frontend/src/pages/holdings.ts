// /holdings — KPIs, treemap canvas slot, sector breakdown, sortable
// holdings table. Phase 8 Cycle 60.

import { mountDataTable } from "../components/DataTable";
import type { DataTableHandle } from "../components/DataTable";
import { EM_DASH, int, month, num, pct, pctAbs, tone, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { palette } from "../lib/charts";
import { paintTreemap } from "../lib/paint";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  downloadBlob?: (content: string, filename: string, mime?: string) => void;
  Chart?: ChartCtor;
}

interface Holding {
  code?: string;
  name?: string;
  venue?: "TW" | "Foreign";
  type?: string;
  ccy?: string;
  qty?: number;
  avg_cost?: number;
  ref_price?: number;
  cost_twd?: number;
  mkt_value_twd?: number;
  unrealized_pnl_twd?: number;
  unrealized_pct?: number;
  weight?: number;
}

interface HoldingsResponse {
  total_mv_twd?: number;
  total_cost_twd?: number;
  total_upnl_twd?: number;
  total_upnl_pct?: number;
  fx_usd_twd?: number;
  as_of?: string;
  holdings: Holding[];
}

interface Sector {
  sector: string;
  value_twd: number;
  count: number;
}

const el = (
  tag: string,
  attrs: Record<string, string> = {},
  text?: string,
): HTMLElement => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (text !== undefined) n.textContent = text;
  return n;
};

const setText = (id: string, text: string): void => {
  const node = document.getElementById(id);
  if (node) node.textContent = text;
};

const KPIS: ReadonlyArray<readonly [string, string, string]> = [
  ["Market value", "kpi-mv", EM_DASH],
  ["Positions", "kpi-count", EM_DASH],
  ["Cost basis", "kpi-cost", EM_DASH],
  ["Unrealized P&L", "kpi-upnl", EM_DASH],
  ["USD/TWD", "kpi-fx", EM_DASH],
];

const TABLE_HEADERS: ReadonlyArray<readonly [string, string, boolean]> = [
  ["Code", "code", false],
  ["Name", "name", false],
  ["Venue", "venue", false],
  ["Type", "type", false],
  ["Ccy", "ccy", false],
  ["Qty", "qty", true],
  ["Avg cost", "avg_cost", true],
  ["Ref price", "ref_price", true],
  ["Cost (TWD)", "cost_twd", true],
  ["MV (TWD)", "mkt_value_twd", true],
  ["Unrealized", "unrealized_pnl_twd", true],
  ["Unrealized %", "unrealized_pct", true],
  ["Weight", "weight", true],
];

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  // KPI grid
  const kpis = el("div", { class: "card-grid cards-5 section" });
  for (const [label, valueId, _initial] of KPIS) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (valueId === "kpi-upnl") {
      card.appendChild(el("span", { class: "kpi-sub", id: "kpi-upnl-pct" }, EM_DASH));
    }
    if (valueId === "kpi-fx") {
      card.appendChild(el("span", { class: "kpi-sub", id: "kpi-fx-sub" }, EM_DASH));
    }
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  // Treemap + sectors row
  const row = el("div", {
    class: "section card-grid",
    style: "grid-template-columns: 2fr 1fr;",
  });

  const tmCard = el("div", { class: "card" });
  const tmHeader = el("div", { class: "card-header" });
  tmHeader.appendChild(el("h3", { class: "card-title" }, "Position map"));
  tmCard.appendChild(tmHeader);
  const tmBox = el("div", { class: "chart-box h-420" });
  tmBox.appendChild(el("canvas", { id: "treemap" }));
  tmCard.appendChild(tmBox);
  row.appendChild(tmCard);

  const sectorCard = el("div", { class: "card" });
  const sectorHeader = el("div", { class: "card-header" });
  sectorHeader.appendChild(el("h3", { class: "card-title" }, "Sector breakdown"));
  sectorCard.appendChild(sectorHeader);
  sectorCard.appendChild(el("div", { id: "sector-list", class: "flex-col gap-2" }));
  row.appendChild(sectorCard);
  outlet.appendChild(row);

  // Holdings table
  const tableSection = el("div", { class: "section" });
  const tableCard = el("div", { class: "card" });
  const tableHeader = el("div", { class: "card-header" });
  tableHeader.appendChild(el("h3", { class: "card-title" }, "All holdings"));
  const exportBtn = el(
    "button",
    {
      id: "export-holdings",
      class: "btn btn-sm",
      type: "button",
    },
    "Export CSV",
  );
  tableHeader.appendChild(exportBtn);
  tableCard.appendChild(tableHeader);

  const tableWrap = el("div", { class: "table-wrap" });
  const table = el("table", { class: "data", id: "holdings-table" });
  const thead = el("thead");
  const headerRow = el("tr");
  for (const [text, key, isNum] of TABLE_HEADERS) {
    headerRow.appendChild(
      el(
        "th",
        { class: isNum ? "num sortable" : "sortable", "data-key": key },
        text,
      ),
    );
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);
  table.appendChild(el("tbody"));
  tableWrap.appendChild(table);
  tableCard.appendChild(tableWrap);
  tableSection.appendChild(tableCard);
  outlet.appendChild(tableSection);
};

const renderKpis = (d: HoldingsResponse): void => {
  setText("kpi-mv", twd(d.total_mv_twd));
  setText("kpi-count", String(d.holdings.length));
  setText("kpi-cost", twd(d.total_cost_twd));

  const upnl = document.getElementById("kpi-upnl");
  if (upnl) {
    upnl.textContent = twd(d.total_upnl_twd);
    upnl.className = `kpi-value ${tone(d.total_upnl_twd)}`;
  }
  const upnlPct = document.getElementById("kpi-upnl-pct");
  if (upnlPct) {
    upnlPct.textContent = pct(d.total_upnl_pct);
    upnlPct.className = `kpi-sub ${tone(d.total_upnl_pct)}`;
  }

  setText("kpi-fx", d.fx_usd_twd ? d.fx_usd_twd.toFixed(3) : EM_DASH);
  setText("kpi-fx-sub", `as of ${month(d.as_of)}`);
};

const renderSectors = (sectors: ReadonlyArray<Sector>): void => {
  const root = document.getElementById("sector-list");
  if (!root) return;
  while (root.firstChild) root.removeChild(root.firstChild);
  const total = sectors.reduce((s, x) => s + x.value_twd, 0) || 1;
  sectors.forEach((sec, i) => {
    const row = el("div", { class: "bar-row" });
    const label = el("span", { class: "text-sm" });
    label.style.cssText = "display:flex; gap:8px; align-items:center;";
    const sw = el("i");
    sw.style.cssText = `width:8px;height:8px;border-radius:2px;background:var(--c${(i % 8) + 1});display:inline-block;`;
    label.append(sw, document.createTextNode(sec.sector));
    const sub = el("span", { class: "text-mute text-tiny" }, String(sec.count));
    sub.style.marginLeft = "8px";
    label.appendChild(sub);

    const bar = el("span", { class: "bar" });
    const fill = el("span");
    fill.style.width = `${((sec.value_twd / total) * 100).toFixed(2)}%`;
    bar.appendChild(fill);

    const pctEl = el(
      "span",
      { class: "num text-sm" },
      `${((sec.value_twd / total) * 100).toFixed(1)}%`,
    );

    row.append(label, bar, pctEl);
    root.appendChild(row);
  });
};

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const tdCodeLink = (code: string | undefined): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.className = "code";
  const a = el("a", {
    href: `/ticker/${encodeURIComponent(code ?? "")}`,
  }, code ?? "");
  c.appendChild(a);
  return c;
};

const tdPill = (text: string | undefined): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.appendChild(el("span", { class: "pill" }, text ?? ""));
  return c;
};

const renderRow = (r: Holding): HTMLTableCellElement[] => [
  tdCodeLink(r.code),
  td(r.name ?? ""),
  tdPill(r.venue),
  td(r.type ?? ""),
  td(r.ccy ?? ""),
  td(int(r.qty), "num"),
  td(num(r.avg_cost, 2), "num"),
  td(num(r.ref_price, 2), "num"),
  td(twd(r.cost_twd), "num"),
  td(twd(r.mkt_value_twd), "num"),
  td(twd(r.unrealized_pnl_twd), `num ${tone(r.unrealized_pnl_twd)}`),
  td(pct(r.unrealized_pct), `num ${tone(r.unrealized_pct)}`),
  td(pctAbs(r.weight, 1), "num"),
];

const csvCell = (v: unknown): string => {
  if (v === null || v === undefined) return "";
  const s = String(v);
  // Prefix formula-injection chars (=, +, -, @, tab, CR) so spreadsheets
  // do not interpret the cell as a formula.
  const safe = /^[=+\-@\t\r]/.test(s) ? `'${s}` : s;
  return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
};

const CSV_KEYS: ReadonlyArray<keyof Holding> = [
  "code", "name", "venue", "type", "ccy", "qty", "avg_cost", "ref_price",
  "cost_twd", "mkt_value_twd", "unrealized_pnl_twd", "unrealized_pct", "weight",
];

const buildCsv = (rows: ReadonlyArray<Holding>): string => {
  const lines = [CSV_KEYS.join(",")];
  for (const r of rows) {
    lines.push(CSV_KEYS.map((k) => csvCell(r[k])).join(","));
  }
  return lines.join("\n");
};

const defaultDownloadBlob = (
  content: string,
  filename: string,
  mime = "text/csv",
): void => {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = el("a", { href: url, download: filename });
  document.body.appendChild(a);
  (a as HTMLAnchorElement).click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load holdings: ${err.message}`),
  );
};

export const mountHoldings = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const [hold, sectors] = await Promise.all([
      deps.api.get<HoldingsResponse>("/api/holdings/current"),
      deps.api.get<ReadonlyArray<Sector>>("/api/holdings/sectors"),
    ]);
    renderKpis(hold);
    renderSectors(sectors);
    if (deps.Chart && hold.holdings?.length) {
      const pal = palette();
      paintTreemap(
        deps.Chart,
        "treemap",
        hold.holdings.map((h, i) => ({
          label: h.code ?? h.name ?? "",
          value: h.mkt_value_twd ?? 0,
          color: pal[i % pal.length] || "#888",
        })),
      );
    }

    const handle: DataTableHandle<Holding> = mountDataTable<Holding>({
      tableId: "holdings-table",
      rows: hold.holdings,
      searchKeys: ["code", "name"],
      searchPlaceholder: "Search code or name…",
      filters: [
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
      ],
      defaultSort: { key: "mkt_value_twd", dir: "desc" },
      colspan: 13,
      pageSize: 25,
      emptyText: "No matching positions",
      row: renderRow,
    });

    const dl = deps.downloadBlob ?? defaultDownloadBlob;
    const exportBtn = document.getElementById("export-holdings");
    exportBtn?.addEventListener("click", () => {
      const rows = handle.filtered();
      const date = new Date().toISOString().slice(0, 10);
      dl(buildCsv(rows), `holdings-${date}.csv`, "text/csv");
    });
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
