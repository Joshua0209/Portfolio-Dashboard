// /transactions — filterable trade log + monthly volume/fees charts.
// Phase 8 Cycle 64. Charts deferred to Cycle 66.

import { mountDataTable } from "../components/DataTable";
import type { DataTableHandle } from "../components/DataTable";
import { EM_DASH, date as fmtDate, int, month as fmtMonth, num, pctAbs, tone, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintBar } from "../lib/paint";
import { el, setText } from "../lib/dom";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  downloadBlob?: (content: string, filename: string, mime?: string) => void;
  Chart?: ChartCtor;
}

interface Tx {
  date?: string;
  month?: string;
  venue?: "TW" | "Foreign";
  side?: string;
  code?: string;
  name?: string;
  qty?: number;
  price?: number;
  ccy?: string;
  gross_twd?: number;
  fee_twd?: number;
  tax_twd?: number;
  net_twd?: number;
}

interface AggTotals {
  trades?: number;
  buy_twd?: number;
  sell_twd?: number;
  fees_twd?: number;
  tax_twd?: number;
  rebate_twd?: number;
  net_cost_twd?: number;
  fee_drag_pct?: number;
}

interface AggResponse {
  totals: AggTotals;
  monthly: ReadonlyArray<Record<string, number | string>>;
  venues: ReadonlyArray<string>;
}

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const tdPill = (text?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.appendChild(el("span", { class: "pill" }, text ?? ""));
  return c;
};

const tdLink = (text: string, href: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.appendChild(el("a", { href }, text));
  return c;
};

const HEADERS: ReadonlyArray<readonly [string, string, boolean]> = [
  ["Date", "date", false],
  ["Venue", "venue", false],
  ["Side", "side", false],
  ["Code", "code", false],
  ["Name", "name", false],
  ["Qty", "qty", true],
  ["Price", "price", true],
  ["Ccy", "ccy", false],
  ["Gross (TWD)", "gross_twd", true],
  ["Fee (TWD)", "fee_twd", true],
  ["Tax (TWD)", "tax_twd", true],
  ["Net (TWD)", "net_twd", true],
];

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  // KPIs
  const kpis = el("div", { class: "card-grid cards-4 section" });
  for (const [label, valueId, subId] of [
    ["Trades", "kpi-n", ""],
    ["Bought (TWD)", "kpi-buy", ""],
    ["Sold (TWD)", "kpi-sell", ""],
    ["Fees + tax", "kpi-cost", "kpi-drag"],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    card.appendChild(el("span", { class: "kpi-sub", id: subId || "" }, EM_DASH));
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  // Charts deferred — slots scaffolded
  const charts = el("div", { class: "section card-grid cards-2" });
  for (const [title, canvasId] of [
    ["Monthly volume", "chart-volume"],
    ["Monthly fees", "chart-fees"],
  ] as const) {
    const card = el("div", { class: "card" });
    card.appendChild(el("h3", { class: "card-title" }, title));
    const box = el("div", { class: "chart-box h-260" });
    box.appendChild(el("canvas", { id: canvasId }));
    card.appendChild(box);
    charts.appendChild(card);
  }
  outlet.appendChild(charts);

  // Table
  const tableSection = el("div", { class: "section" });
  const card = el("div", { class: "card" });
  const header = el("div", { class: "card-header" });
  header.appendChild(el("h3", { class: "card-title" }, "Trade log"));
  header.appendChild(
    el("button", { id: "export-tx", class: "btn btn-sm", type: "button" }, "Export CSV"),
  );
  card.appendChild(header);
  const wrap = el("div", { class: "table-wrap" });
  const table = el("table", { class: "data", id: "tx-table" });
  const thead = el("thead");
  const headRow = el("tr");
  for (const [text, key, isNum] of HEADERS) {
    headRow.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  thead.appendChild(headRow);
  table.appendChild(thead);
  table.appendChild(el("tbody"));
  wrap.appendChild(table);
  card.appendChild(wrap);
  tableSection.appendChild(card);
  outlet.appendChild(tableSection);
};

const renderKpis = (t: AggTotals): void => {
  setText("kpi-n", String(t.trades ?? 0));
  setText("kpi-buy", twd(t.buy_twd));
  setText("kpi-sell", twd(t.sell_twd));
  const gross = (t.fees_twd ?? 0) + (t.tax_twd ?? 0);
  const rebate = t.rebate_twd ?? 0;
  const net = t.net_cost_twd ?? gross - rebate;
  setText("kpi-cost", twd(net));
  const subParts = [`drag ${pctAbs(t.fee_drag_pct, 3)} of volume`];
  if (rebate > 0) subParts.unshift(`gross ${twd(gross)} − rebates ${twd(rebate)}`);
  setText("kpi-drag", subParts.join(" · "));
};

const renderRow = (t: Tx): HTMLTableCellElement[] => [
  td(fmtDate(t.date), "text-mute"),
  tdPill(t.venue),
  td(t.side ?? ""),
  tdLink(t.code ?? "", `/ticker/${encodeURIComponent(t.code ?? "")}`, "code"),
  td(t.name ?? ""),
  td(int(t.qty), "num"),
  td(num(t.price, 2), "num"),
  td(t.ccy ?? "", "text-mute"),
  td(twd(t.gross_twd), "num"),
  td(twd(t.fee_twd), "num text-mute"),
  td(twd(t.tax_twd), "num text-mute"),
  td(twd(t.net_twd), `num ${tone(t.net_twd)}`),
];

const csvCell = (v: unknown): string => {
  if (v === null || v === undefined) return "";
  const s = String(v);
  // Prefix formula-injection chars (=, +, -, @, tab, CR) so spreadsheets
  // do not interpret the cell as a formula.
  const safe = /^[=+\-@\t\r]/.test(s) ? `'${s}` : s;
  return /[",\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
};

const CSV_KEYS: ReadonlyArray<keyof Tx> = [
  "month", "date", "venue", "side", "code", "name", "qty", "price", "ccy",
  "gross_twd", "fee_twd", "tax_twd", "net_twd",
];

const buildCsv = (rows: ReadonlyArray<Tx>): string => {
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

const sumByVenueSuffix = (
  row: Record<string, number | string>,
  suffix: string,
  fallback: string,
): number => {
  const direct = row[fallback];
  if (typeof direct === "number") return direct;
  // Backend returns per-venue prefixed keys (e.g. TW_buy, Foreign_buy).
  // Sum across every key that ends with `_<suffix>` so the chart reflects
  // the total trading volume regardless of the venue split.
  let total = 0;
  for (const [k, v] of Object.entries(row)) {
    if (typeof v === "number" && k.endsWith(`_${suffix}`)) total += v;
  }
  return total;
};

const paintTxCharts = (Chart: ChartCtor, agg: AggResponse): void => {
  const monthly = agg.monthly ?? [];
  const labels = monthly.map((r) => String(r.month ?? ""));
  paintBar(Chart, "chart-volume", {
    labels,
    datasets: [
      {
        label: "Buys (TWD)",
        data: monthly.map((r) => sumByVenueSuffix(r, "buy", "buy_twd")),
        color: cssVar("--c1") || palette()[0],
        stack: "vol",
      },
      {
        label: "Sells (TWD)",
        data: monthly.map((r) => sumByVenueSuffix(r, "sell", "sell_twd")),
        color: cssVar("--c3") || palette()[2],
        stack: "vol",
      },
    ],
  });
  paintBar(Chart, "chart-fees", {
    labels,
    datasets: [
      {
        label: "Fees (TWD)",
        data: monthly.map((r) => sumByVenueSuffix(r, "fees", "fees_twd")),
        color: cssVar("--c5") || palette()[4],
        stack: "cost",
      },
      {
        label: "Tax (TWD)",
        data: monthly.map((r) => sumByVenueSuffix(r, "tax", "tax_twd")),
        color: cssVar("--c7") || palette()[6],
        stack: "cost",
      },
    ],
  });
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load transactions: ${err.message}`),
  );
};

export const mountTransactions = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const [tx, agg] = await Promise.all([
      deps.api.get<Tx[]>("/api/transactions"),
      deps.api.get<AggResponse>("/api/transactions/aggregates"),
    ]);
    renderKpis(agg.totals ?? {});
    if (deps.Chart) paintTxCharts(deps.Chart, agg);
    const months = [
      ...new Set(tx.map((t) => t.month).filter((m): m is string => Boolean(m))),
    ].sort();
    const handle: DataTableHandle<Tx> = mountDataTable<Tx>({
      tableId: "tx-table",
      rows: tx,
      searchKeys: ["code", "name"],
      searchPlaceholder: "Search code or name…",
      filters: [
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
        {
          id: "side",
          key: "side",
          label: "All sides",
          options: [
            { value: "buy", label: "Buys only" },
            { value: "sell", label: "Sells only" },
          ],
          predicate: (r, v) =>
            v === "buy" ? /買/.test(r.side ?? "") : /賣/.test(r.side ?? ""),
        },
        {
          id: "month",
          key: "month",
          label: "All months",
          options: months.map((m) => ({ value: m, label: fmtMonth(m) })),
        },
      ],
      defaultSort: { key: "date", dir: "desc" },
      colspan: 12,
      pageSize: 50,
      emptyText: "No matching trades",
      row: renderRow,
    });

    const dl = deps.downloadBlob ?? defaultDownloadBlob;
    document.getElementById("export-tx")?.addEventListener("click", () => {
      const rows = handle.filtered();
      const date = new Date().toISOString().slice(0, 10);
      dl(buildCsv(rows), `transactions-${date}.csv`, "text/csv");
    });
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
