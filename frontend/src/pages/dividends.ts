// /dividends — monthly stacked income, top payers, total-return-on-cost,
// rebates, full distribution log. Phase 8 Cycle 64.
// Charts deferred to Cycle 66.

import { mountDataTable } from "../components/DataTable";
import { EM_DASH, date as fmtDate, month as fmtMonth, num, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintBar } from "../lib/paint";
import { el, setText } from "../lib/dom";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  Chart?: ChartCtor;
}

interface DividendsResponse {
  total_twd?: number;
  totals_by_ccy?: Record<string, number>;
  count?: number;
  yields?: {
    ttm_dividend_twd?: number;
    ttm_yield_on_cost?: number | null;
    annualized_yield_on_cost?: number | null;
  };
  monthly?: ReadonlyArray<{ month: string; tw_twd?: number; foreign_twd?: number; twd_amount?: number }>;
  by_ticker?: ReadonlyArray<{ code?: string; name?: string; total_twd: number; count: number }>;
  holdings_total_return?: ReadonlyArray<{
    code?: string; name?: string; venue?: string;
    cost_twd?: number; mkt_value_twd?: number;
    cum_dividend_twd?: number;
    unrealized_pnl_with_div_twd?: number;
    total_return_pct?: number | null;
  }>;
  rebates?: ReadonlyArray<{ month: string; type: string; amount_twd: number }>;
  rows?: ReadonlyArray<{
    date?: string; venue?: string; code?: string; name?: string; ccy?: string;
    amount_local?: number; amount_twd?: number;
  }>;
}

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const tdLink = (text: string, href: string | null, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  if (href) c.appendChild(el("a", { href }, text));
  else c.textContent = text;
  return c;
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const kpis = el("div", { class: "card-grid cards-4 section" });
  for (const [label, valueId, subId] of [
    ["Total received", "kpi-total", "kpi-total-sub"],
    ["TTM dividends", "kpi-ttm", "kpi-ttm-yield"],
    ["Annualized yield", "kpi-yield", ""],
    ["Distinct payers", "kpi-payers", "kpi-events"],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    else card.appendChild(el("span", { class: "kpi-sub" }));
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  // Charts + top payers row
  const row1 = el("div", { class: "section card-grid cards-2" });
  const chartCard = el("div", { class: "card" });
  chartCard.appendChild(el("h3", { class: "card-title" }, "Monthly income"));
  const chartBox = el("div", { class: "chart-box h-260" });
  chartBox.appendChild(el("canvas", { id: "chart-monthly" }));
  chartCard.appendChild(chartBox);
  row1.appendChild(chartCard);

  const topCard = el("div", { class: "card" });
  topCard.appendChild(el("h3", { class: "card-title" }, "Top payers"));
  topCard.appendChild(el("div", { id: "top-payers", class: "flex-col gap-2" }));
  row1.appendChild(topCard);
  outlet.appendChild(row1);

  // Total-return table
  const trSec = el("div", { class: "section" });
  const trCard = el("div", { class: "card" });
  trCard.appendChild(el("h3", { class: "card-title" }, "Total return per holding"));
  const trWrap = el("div", { class: "table-wrap" });
  const trTable = el("table", { class: "data", id: "tr-table" });
  const trThead = el("thead");
  const trHead = el("tr");
  for (const [text, key, isNum] of [
    ["Venue", "venue", false],
    ["Code", "code", false],
    ["Name", "name", false],
    ["Cost (TWD)", "cost_twd", true],
    ["MV (TWD)", "mkt_value_twd", true],
    ["Cum div (TWD)", "cum_dividend_twd", true],
    ["Unrealized + div", "unrealized_pnl_with_div_twd", true],
    ["Total return", "total_return_pct", true],
  ] as const) {
    trHead.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  trThead.appendChild(trHead);
  trTable.appendChild(trThead);
  trTable.appendChild(el("tbody"));
  trWrap.appendChild(trTable);
  trCard.appendChild(trWrap);
  trSec.appendChild(trCard);
  outlet.appendChild(trSec);

  // Rebates table
  const rebSec = el("div", { class: "section" });
  const rebCard = el("div", { class: "card" });
  rebCard.appendChild(el("h3", { class: "card-title" }, "Rebates (折讓金)"));
  const rebWrap = el("div", { class: "table-wrap" });
  const rebTable = el("table", { class: "data", id: "rebate-table" });
  const rebThead = el("thead");
  const rebHead = el("tr");
  for (const [text, isNum] of [
    ["Month", false],
    ["Type", false],
    ["Amount (TWD)", true],
  ] as const) {
    rebHead.appendChild(el("th", { class: isNum ? "num" : "" }, text));
  }
  rebThead.appendChild(rebHead);
  rebTable.appendChild(rebThead);
  rebTable.appendChild(el("tbody"));
  rebWrap.appendChild(rebTable);
  rebCard.appendChild(rebWrap);
  rebSec.appendChild(rebCard);
  outlet.appendChild(rebSec);

  // Distribution log
  const divSec = el("div", { class: "section" });
  const divCard = el("div", { class: "card" });
  divCard.appendChild(el("h3", { class: "card-title" }, "Distribution log"));
  const divWrap = el("div", { class: "table-wrap" });
  const divTable = el("table", { class: "data", id: "div-table" });
  const divThead = el("thead");
  const divHead = el("tr");
  for (const [text, key, isNum] of [
    ["Date", "date", false],
    ["Venue", "venue", false],
    ["Code", "code", false],
    ["Name", "name", false],
    ["Ccy", "ccy", false],
    ["Amount", "amount_local", true],
    ["TWD", "amount_twd", true],
  ] as const) {
    divHead.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  divThead.appendChild(divHead);
  divTable.appendChild(divThead);
  divTable.appendChild(el("tbody"));
  divWrap.appendChild(divTable);
  divCard.appendChild(divWrap);
  divSec.appendChild(divCard);
  outlet.appendChild(divSec);
};

const renderKpis = (d: DividendsResponse): void => {
  setText("kpi-total", twd(d.total_twd));
  const ccyParts = Object.entries(d.totals_by_ccy ?? {}).map(([c, v]) =>
    c === "TWD" ? `NT$${Number(v).toFixed(0)}` : `${c} ${Number(v).toFixed(2)}`,
  );
  setText("kpi-total-sub", ccyParts.join(" · ") || EM_DASH);

  const y = d.yields ?? {};
  setText("kpi-ttm", twd(y.ttm_dividend_twd ?? 0));
  setText(
    "kpi-ttm-yield",
    y.ttm_yield_on_cost != null
      ? `${(y.ttm_yield_on_cost * 100).toFixed(2)}% on cost`
      : "trailing 12-month",
  );
  setText(
    "kpi-yield",
    y.annualized_yield_on_cost != null
      ? `${(y.annualized_yield_on_cost * 100).toFixed(2)}%`
      : EM_DASH,
  );
  setText("kpi-payers", String((d.by_ticker ?? []).length));
  setText("kpi-events", `${d.count ?? 0} events`);
};

const renderTopPayers = (
  payers: ReadonlyArray<{ code?: string; name?: string; total_twd: number; count: number }>,
): void => {
  const root = document.getElementById("top-payers");
  if (!root) return;
  while (root.firstChild) root.removeChild(root.firstChild);
  if (!payers.length) {
    root.textContent = "No dividend payers yet";
    root.className = "empty-state text-mute";
    return;
  }
  const max = Math.max(1, ...payers.map((p) => p.total_twd));
  for (const p of payers.slice(0, 10)) {
    const row = el("div", { class: "bar-row" });
    const lab = el("span", { class: "text-sm" });
    lab.style.cssText = "display:flex; gap:8px; align-items:center;";
    lab.appendChild(el("strong", {}, p.code ?? EM_DASH));
    lab.appendChild(
      el("span", { class: "text-mute text-tiny" }, `${p.name ?? ""} · ${p.count}×`),
    );
    const bar = el("span", { class: "bar pos" });
    const fill = el("span");
    fill.style.width = `${((p.total_twd / max) * 100).toFixed(2)}%`;
    bar.appendChild(fill);
    row.append(lab, bar, el("span", { class: "num text-sm" }, twd(p.total_twd)));
    root.appendChild(row);
  }
};

const renderTotalReturn = (
  rows: NonNullable<DividendsResponse["holdings_total_return"]>,
): void => {
  mountDataTable<typeof rows[number]>({
    tableId: "tr-table",
    rows,
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
      td(r.venue ?? "", "text-mute text-tiny"),
      tdLink(r.code ?? "", r.code ? `/ticker/${encodeURIComponent(r.code)}` : null, "code"),
      td(r.name ?? ""),
      td(twd(r.cost_twd), "num"),
      td(twd(r.mkt_value_twd), "num"),
      td(twd(r.cum_dividend_twd), "num"),
      td(
        twd(r.unrealized_pnl_with_div_twd),
        `num ${(r.unrealized_pnl_with_div_twd ?? 0) >= 0 ? "value-pos" : "value-neg"}`,
      ),
      td(
        r.total_return_pct != null ? `${(r.total_return_pct * 100).toFixed(2)}%` : EM_DASH,
        `num ${(r.total_return_pct ?? 0) >= 0 ? "value-pos" : "value-neg"}`,
      ),
    ],
  });
};

const renderRebates = (rows: ReadonlyArray<{ month: string; type: string; amount_twd: number }>): void => {
  const tbody = document.querySelector("#rebate-table tbody");
  if (!tbody) return;
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
  if (!rows.length) {
    const tr = el("tr");
    const cell = td("No rebates", "table-empty");
    cell.colSpan = 3;
    tr.appendChild(cell);
    tbody.appendChild(tr);
    return;
  }
  for (const r of rows) {
    const tr = el("tr");
    tr.appendChild(td(fmtMonth(r.month)));
    tr.appendChild(td(r.type));
    tr.appendChild(td(twd(r.amount_twd), "num value-pos"));
    tbody.appendChild(tr);
  }
};

const distinct = <T extends Record<string, unknown>>(
  rows: ReadonlyArray<T>,
  key: keyof T,
): string[] => {
  const seen = new Set<string>();
  for (const r of rows) {
    const v = r[key];
    if (typeof v === "string" && v) seen.add(v);
  }
  return [...seen].sort();
};

const renderTable = (rows: NonNullable<DividendsResponse["rows"]>): void => {
  mountDataTable<typeof rows[number]>({
    tableId: "div-table",
    rows,
    searchKeys: ["code", "name", "venue", "ccy"],
    searchPlaceholder: "Search code, name, venue…",
    filters: [
      { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
      { id: "ccy", key: "ccy", label: "All ccy", options: distinct(rows, "ccy") },
    ],
    defaultSort: { key: "date", dir: "desc" },
    colspan: 7,
    pageSize: 25,
    emptyText: "No distributions",
    row: (r) => [
      td(fmtDate(r.date), "text-mute"),
      td(r.venue ?? "", "text-mute text-tiny"),
      tdLink(r.code ?? "", r.code ? `/ticker/${encodeURIComponent(r.code)}` : null, "code"),
      td(r.name ?? ""),
      td(r.ccy ?? "TWD", "text-mute"),
      td(num(r.amount_local, 2), "num"),
      td(twd(r.amount_twd), "num value-pos"),
    ],
  });
};

const paintDivCharts = (Chart: ChartCtor, data: DividendsResponse): void => {
  const monthly = data.monthly ?? [];
  const labels = monthly.map((r) => r.month);
  // Stack tw + foreign when both present; else use legacy `twd_amount`.
  const hasSplit = monthly.some((r) => r.tw_twd !== undefined || r.foreign_twd !== undefined);
  if (hasSplit) {
    paintBar(Chart, "chart-monthly", {
      labels,
      datasets: [
        {
          label: "TW (TWD)",
          data: monthly.map((r) => r.tw_twd ?? 0),
          color: cssVar("--c1") || palette()[0],
          stack: "div",
        },
        {
          label: "Foreign (TWD)",
          data: monthly.map((r) => r.foreign_twd ?? 0),
          color: cssVar("--c2") || palette()[1],
          stack: "div",
        },
      ],
    });
  } else {
    paintBar(Chart, "chart-monthly", {
      labels,
      datasets: [
        {
          label: "Dividends (TWD)",
          data: monthly.map((r) => r.twd_amount ?? 0),
          color: cssVar("--c1") || palette()[0],
        },
      ],
    });
  }
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load dividends: ${err.message}`),
  );
};

export const mountDividends = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const data = await deps.api.get<DividendsResponse>("/api/dividends");
    renderKpis(data);
    renderTopPayers(data.by_ticker ?? []);
    renderTotalReturn(data.holdings_total_return ?? []);
    renderRebates(data.rebates ?? []);
    renderTable(data.rows ?? []);
    if (deps.Chart) paintDivCharts(deps.Chart, data);
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
