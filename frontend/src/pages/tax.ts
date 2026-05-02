// /tax — Realized + unrealized P&L by ticker. Phase 8 Cycle 65.

import { mountDataTable } from "../components/DataTable";
import type { DataTableHandle } from "../components/DataTable";
import { EM_DASH, int, pctAbs, tone, twd } from "../lib/format";
import { el, setText } from "../lib/dom";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  downloadBlob?: (content: string, filename: string, mime?: string) => void;
}

interface TaxTotals {
  realized_pnl_twd?: number;
  rebate_twd?: number;
  closed_positions?: number;
  dividends_twd?: number;
  unrealized_pnl_twd?: number;
  total_pnl_twd?: number;
  win_rate?: number;
  winners_count?: number;
  losers_count?: number;
  avg_holding_days?: number;
  fees_twd?: number;
  tax_twd?: number;
  net_cost_twd?: number;
}

interface TaxRow {
  code?: string;
  name?: string;
  venue?: "TW" | "Foreign";
  sell_qty?: number;
  open_qty?: number;
  cost_of_sold_twd?: number;
  sell_proceeds_twd?: number;
  realized_pnl_twd?: number;
  realized_pnl_avg_twd?: number;
  dividends_twd?: number;
  unrealized_pnl_twd?: number;
  total_pnl_twd?: number;
  win_rate?: number | null;
  avg_holding_days?: number | null;
  fully_closed?: boolean;
}

interface TaxResponse {
  totals: TaxTotals;
  by_ticker: TaxRow[];
}

const setColored = (id: string, txt: string, val: number | null | undefined): void => {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = txt;
  node.className = `kpi-value ${tone(val)}`;
};

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const tdCodeLink = (code?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.className = "code";
  c.appendChild(el("a", { href: `/ticker/${encodeURIComponent(code ?? "")}` }, code ?? ""));
  return c;
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const kpis = el("div", { class: "card-grid cards-4 section" });
  for (const [label, valueId, subId] of [
    ["Realized P&L", "kpi-real", "kpi-real-sub"],
    ["Dividends", "kpi-div", ""],
    ["Unrealized P&L", "kpi-unreal", ""],
    ["Total P&L", "kpi-total", ""],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    else card.appendChild(el("span", { class: "kpi-sub" }));
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  const kpis2 = el("div", { class: "card-grid cards-3 section" });
  for (const [label, valueId, subId] of [
    ["Win rate", "kpi-win", "kpi-win-sub"],
    ["Avg holding", "kpi-hold", ""],
    ["Net cost (fees + tax)", "kpi-cost", "kpi-cost-sub"],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    else card.appendChild(el("span", { class: "kpi-sub" }));
    kpis2.appendChild(card);
  }
  outlet.appendChild(kpis2);

  // Movers
  const movers = el("div", { class: "section card-grid cards-2" });
  for (const [title, listId] of [
    ["Top winners", "winners-list"],
    ["Top losers", "losers-list"],
  ] as const) {
    const card = el("div", { class: "card" });
    card.appendChild(el("h3", { class: "card-title" }, title));
    card.appendChild(el("div", { id: listId, class: "flex-col gap-2" }));
    movers.appendChild(card);
  }
  outlet.appendChild(movers);

  // Table
  const tableSec = el("div", { class: "section" });
  const card = el("div", { class: "card" });
  const header = el("div", { class: "card-header" });
  header.appendChild(el("h3", { class: "card-title" }, "Per-ticker P&L"));
  header.appendChild(
    el("button", { id: "export-tax", class: "btn btn-sm", type: "button" }, "Export CSV"),
  );
  card.appendChild(header);
  const wrap = el("div", { class: "table-wrap" });
  const table = el("table", { class: "data", id: "tax-table" });
  const thead = el("thead");
  const headRow = el("tr");
  for (const [text, key, isNum] of [
    ["Code", "code", false],
    ["Name", "name", false],
    ["Venue", "venue", false],
    ["Sold qty", "sell_qty", true],
    ["Open qty", "open_qty", true],
    ["Cost of sold", "cost_of_sold_twd", true],
    ["Proceeds", "sell_proceeds_twd", true],
    ["Realized", "realized_pnl_twd", true],
    ["Dividends", "dividends_twd", true],
    ["Unrealized", "unrealized_pnl_twd", true],
    ["Total P&L", "total_pnl_twd", true],
    ["Win rate", "win_rate", true],
    ["Avg days", "avg_holding_days", true],
  ] as const) {
    headRow.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  thead.appendChild(headRow);
  table.appendChild(thead);
  table.appendChild(el("tbody"));
  wrap.appendChild(table);
  card.appendChild(wrap);
  tableSec.appendChild(card);
  outlet.appendChild(tableSec);
};

const renderKpis = (t: TaxTotals): void => {
  const rebate = t.rebate_twd ?? 0;
  const realizedNet = (t.realized_pnl_twd ?? 0) + rebate;
  const totalNet = (t.total_pnl_twd ?? 0) + rebate;
  setColored("kpi-real", twd(realizedNet), realizedNet);
  if (rebate > 0) {
    setText(
      "kpi-real-sub",
      `${t.closed_positions ?? 0} closed · +${twd(rebate)} rebates`,
    );
  } else {
    setText("kpi-real-sub", `${t.closed_positions ?? 0} closed positions`);
  }
  setColored("kpi-div", twd(t.dividends_twd ?? 0), t.dividends_twd ?? 0);
  setColored("kpi-unreal", twd(t.unrealized_pnl_twd), t.unrealized_pnl_twd);
  setColored("kpi-total", twd(totalNet), totalNet);
  setText("kpi-win", pctAbs(t.win_rate ?? 0, 1));
  setText(
    "kpi-win-sub",
    `${t.winners_count ?? 0} winners · ${t.losers_count ?? 0} losers`,
  );
  setText(
    "kpi-hold",
    t.avg_holding_days != null ? `${Math.round(t.avg_holding_days)}d` : EM_DASH,
  );
  const grossCost = (t.fees_twd ?? 0) + (t.tax_twd ?? 0);
  const netCost = t.net_cost_twd ?? grossCost - rebate;
  setText("kpi-cost", twd(netCost));
  if (rebate > 0) {
    setText(
      "kpi-cost-sub",
      `fees ${twd(t.fees_twd ?? 0)} · tax ${twd(t.tax_twd ?? 0)} · rebates -${twd(rebate)}`,
    );
  } else {
    setText(
      "kpi-cost-sub",
      `fees ${twd(t.fees_twd ?? 0)} · tax ${twd(t.tax_twd ?? 0)}`,
    );
  }
};

const populate = (
  listId: string,
  rows: ReadonlyArray<TaxRow>,
  toneCls: "pos" | "neg",
): void => {
  const list = document.getElementById(listId);
  if (!list) return;
  while (list.firstChild) list.removeChild(list.firstChild);
  if (!rows.length) {
    list.textContent = "Nothing here";
    list.className = "empty-state text-mute";
    return;
  }
  const max = Math.max(1, ...rows.map((r) => Math.abs(r.realized_pnl_twd ?? 0)));
  for (const r of rows) {
    const row = el("a", {
      href: `/ticker/${encodeURIComponent(r.code ?? "")}`,
      class: "bar-row",
    });
    row.style.color = "inherit";
    const lab = el("span", { class: "text-sm" });
    lab.style.cssText = "display:flex; gap:8px; align-items:center;";
    lab.appendChild(el("strong", {}, r.code ?? ""));
    lab.appendChild(el("span", { class: "text-mute text-tiny" }, r.name ?? ""));
    const bar = el("span", { class: `bar ${toneCls}` });
    const fill = el("span");
    fill.style.width = `${(Math.abs(r.realized_pnl_twd ?? 0) / max) * 100}%`;
    bar.appendChild(fill);
    row.append(
      lab,
      bar,
      el("span", { class: `num text-sm value-${toneCls}` }, twd(r.realized_pnl_twd ?? 0)),
    );
    list.appendChild(row);
  }
};

const renderMovers = (rows: ReadonlyArray<TaxRow>): void => {
  const sorted = [...rows].sort(
    (a, b) => (b.realized_pnl_twd ?? 0) - (a.realized_pnl_twd ?? 0),
  );
  populate(
    "winners-list",
    sorted.slice(0, 5).filter((r) => (r.realized_pnl_twd ?? 0) > 0),
    "pos",
  );
  populate(
    "losers-list",
    sorted.slice(-5).reverse().filter((r) => (r.realized_pnl_twd ?? 0) < 0),
    "neg",
  );
};

const csvCell = (v: unknown): string => {
  if (v === null || v === undefined) return "";
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

const CSV_KEYS: ReadonlyArray<keyof TaxRow> = [
  "code", "name", "venue", "sell_qty", "open_qty",
  "cost_of_sold_twd", "sell_proceeds_twd",
  "realized_pnl_twd", "realized_pnl_avg_twd",
  "dividends_twd", "unrealized_pnl_twd", "total_pnl_twd",
  "win_rate", "avg_holding_days", "fully_closed",
];

const buildCsv = (rows: ReadonlyArray<TaxRow>): string => {
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
    el("div", { class: "error-box" }, `Failed to load tax: ${err.message}`),
  );
};

export const mountTax = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const d = await deps.api.get<TaxResponse>("/api/tax");
    renderKpis(d.totals ?? {});
    renderMovers(d.by_ticker ?? []);

    const handle: DataTableHandle<TaxRow> = mountDataTable<TaxRow>({
      tableId: "tax-table",
      rows: d.by_ticker ?? [],
      searchKeys: ["code", "name"],
      searchPlaceholder: "Search code or name…",
      filters: [
        {
          id: "status",
          key: "fully_closed",
          label: "All",
          options: [
            { value: "closed", label: "Fully closed" },
            { value: "open", label: "Open positions" },
            { value: "winners", label: "Winners only" },
            { value: "losers", label: "Losers only" },
          ],
          predicate: (r, v) =>
            v === "closed"
              ? !!r.fully_closed
              : v === "open"
                ? !r.fully_closed
                : v === "winners"
                  ? (r.realized_pnl_twd ?? 0) > 0
                  : v === "losers"
                    ? (r.realized_pnl_twd ?? 0) < 0
                    : true,
        },
        { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
      ],
      defaultSort: { key: "total_pnl_twd", dir: "desc" },
      colspan: 13,
      pageSize: 50,
      emptyText: "No matching tickers",
      row: (r) => [
        tdCodeLink(r.code),
        td(r.name ?? ""),
        td(r.venue ?? "", "text-mute text-tiny"),
        td(int(r.sell_qty ?? 0), "num"),
        td(int(r.open_qty ?? 0), "num"),
        td(twd(r.cost_of_sold_twd ?? 0), "num text-mute"),
        td(twd(r.sell_proceeds_twd ?? 0), "num"),
        td(twd(r.realized_pnl_twd ?? 0), `num ${tone(r.realized_pnl_twd ?? 0)}`),
        td(twd(r.dividends_twd ?? 0), "num value-pos"),
        td(twd(r.unrealized_pnl_twd ?? 0), `num ${tone(r.unrealized_pnl_twd ?? 0)}`),
        td(twd(r.total_pnl_twd ?? 0), `num ${tone(r.total_pnl_twd ?? 0)}`),
        td(
          r.win_rate != null ? `${(r.win_rate * 100).toFixed(0)}%` : EM_DASH,
          "num text-mute",
        ),
        td(
          r.avg_holding_days != null ? `${Math.round(r.avg_holding_days)}` : EM_DASH,
          "num text-mute",
        ),
      ],
    });

    const dl = deps.downloadBlob ?? defaultDownloadBlob;
    document.getElementById("export-tax")?.addEventListener("click", () => {
      const rows = handle.filtered();
      const date = new Date().toISOString().slice(0, 10);
      dl(buildCsv(rows), `tax-pnl-${date}.csv`, "text/csv");
    });
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
