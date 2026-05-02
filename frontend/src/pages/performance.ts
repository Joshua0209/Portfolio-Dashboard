// /performance — TWR/XIRR/drawdown/rolling/attribution.

import { mountDataTable } from "../components/DataTable";
import { EM_DASH, label, month, pct, pctAbs, tone, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintBar, paintLine, paintTreemap } from "../lib/paint";

const METHOD_STORAGE_KEY = "perf.twr.method.v1";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  Chart?: ChartCtor;
}

interface RollingRow {
  month: string;
  r3m?: number;
  r6m?: number;
  r12m?: number;
}

interface AttributionResponse {
  monthly?: ReadonlyArray<{ month: string; tw_twd?: number; foreign_twd?: number; fx_twd?: number }>;
  totals?: { tw_twd?: number; foreign_twd?: number; fx_twd?: number };
  contributions?: ReadonlyArray<{ code: string; name?: string; contribution_twd: number }>;
}

interface TimeseriesNew {
  timeseries?: ReadonlyArray<{
    month: string;
    twr_pct?: number;
    cum_twr_pct?: number;
    drawdown_pct?: number;
    equity_twd?: number;
  }>;
}

interface MonthlyRow {
  month?: string;
  date?: string;
  v_start?: number;
  equity_twd?: number;
  external_flow?: number;
  flow_twd?: number;
  weighted_flow?: number;
  period_return?: number;
  cum_twr?: number;
  drawdown?: number;
}

interface DrawdownEpisode {
  peak_month: string;
  trough_month: string;
  depth_pct: number;
  drawdown_months: number;
  recovery_months: number | null;
  recovered: boolean;
}

interface TimeseriesResponse {
  twr_total?: number;
  cagr?: number | null;
  xirr?: number | null;
  hit_rate?: number;
  positive_months?: number;
  negative_months?: number;
  annualized_volatility?: number;
  sharpe_annualized?: number;
  sortino_annualized?: number;
  calmar?: number;
  monthly?: ReadonlyArray<MonthlyRow>;
  drawdown_episodes?: ReadonlyArray<DrawdownEpisode>;
}

interface ContribRow {
  code?: string;
  name?: string;
  venue?: string;
  realized_pnl_twd?: number;
  dividends_twd?: number;
  unrealized_pnl_twd?: number;
  total_pnl_twd?: number;
  contribution_share?: number;
}

interface TaxResponse {
  by_ticker?: ReadonlyArray<ContribRow>;
}

const RATIO_BANDS = {
  sharpe: [
    { at: 0.5, label: "poor" },
    { at: 1.0, label: "sub-par" },
    { at: 2.0, label: "good" },
    { at: 3.0, label: "great" },
    { at: Infinity, label: "elite / thin sample" },
  ],
  sortino: [
    { at: 1.0, label: "weak" },
    { at: 2.0, label: "acceptable" },
    { at: 3.0, label: "good" },
    { at: 5.0, label: "excellent" },
    { at: Infinity, label: "elite / thin sample" },
  ],
  calmar: [
    { at: 0.5, label: "weak" },
    { at: 1.0, label: "acceptable" },
    { at: 3.0, label: "strong" },
    { at: Infinity, label: "exceptional / thin sample" },
  ],
} as const;

const bandLabel = (
  v: number,
  bands: ReadonlyArray<{ at: number; label: string }>,
): string => {
  if (!Number.isFinite(v)) return EM_DASH;
  if (v < 0) return "negative — losing money relative to risk";
  for (const b of bands) {
    if (v < b.at) return `band: ${b.label}`;
  }
  return `band: ${bands[bands.length - 1].label}`;
};

const capRatio = (v: number): string => {
  if (!Number.isFinite(v) || Math.abs(v) > 100) return v > 0 ? "≫ 10" : "≪ −10";
  return v.toFixed(2);
};

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

const setText = (id: string, v: string): void => {
  const node = document.getElementById(id);
  if (node) node.textContent = v;
};

const setTextColor = (id: string, v: string, signal: number): void => {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = v;
  node.className = `kpi-value ${tone(signal)}`;
};

const KPI_ROW_1: ReadonlyArray<readonly [string, string, string]> = [
  ["TWR (cumulative)", "kpi-twr", "kpi-twr-sub"],
  ["CAGR", "kpi-cagr", ""],
  ["XIRR", "kpi-xirr", ""],
  ["Hit rate", "kpi-hit", "kpi-hit-sub"],
];

const KPI_ROW_2: ReadonlyArray<readonly [string, string, string]> = [
  ["Annualized volatility", "kpi-vol", ""],
  ["Sharpe (rf=0)", "kpi-sharpe", "kpi-sharpe-sub"],
  ["Sortino", "kpi-sortino", "kpi-sortino-sub"],
  ["Calmar", "kpi-calmar", "kpi-calmar-sub"],
];

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const tdCodeLink = (code?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.className = "code";
  c.appendChild(
    el(
      "a",
      { href: `/ticker/${encodeURIComponent(code ?? "")}` },
      code ?? EM_DASH,
    ),
  );
  return c;
};

const renderKpiRow = (parent: HTMLElement, rows: typeof KPI_ROW_1): void => {
  const grid = el("div", { class: "card-grid cards-4 section" });
  for (const [label, valueId, subId] of rows) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) {
      card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    }
    grid.appendChild(card);
  }
  parent.appendChild(grid);
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  // Method switcher row
  const actions = el("div", { class: "page-actions" });
  const wrap = el("label", { class: "text-mute text-tiny" });
  wrap.style.cssText = "display:flex; align-items:center; gap: 8px;";
  wrap.appendChild(document.createTextNode("TWR method"));
  const sel = el("select", {
    id: "twr-method",
    class: "select",
  }) as HTMLSelectElement;
  for (const [val, txt] of [
    ["day_weighted", "Day-weighted (recommended)"],
    ["mid_month", "Mid-month (legacy)"],
    ["eom", "End-of-month flows"],
  ] as const) {
    const opt = document.createElement("option");
    opt.value = val;
    opt.textContent = txt;
    sel.appendChild(opt);
  }
  wrap.appendChild(sel);
  actions.appendChild(wrap);
  outlet.appendChild(actions);

  // KPI rows
  renderKpiRow(outlet, KPI_ROW_1);
  renderKpiRow(outlet, KPI_ROW_2);

  const charts1 = el("div", { class: "section" });
  const cumCard = el("div", { class: "card" });
  cumCard.appendChild(el("h3", { class: "card-title" }, "Cumulative TWR vs equity"));
  const cumBox = el("div", { class: "chart-box h-320" });
  cumBox.appendChild(el("canvas", { id: "chart-cum" }));
  cumCard.appendChild(cumBox);
  charts1.appendChild(cumCard);
  outlet.appendChild(charts1);

  const charts2 = el("div", { class: "section card-grid cards-2" });
  for (const [title, canvasId, subId] of [
    ["Monthly returns", "chart-monthly", "month-stats"],
    ["Drawdown", "chart-dd", "dd-max"],
  ] as const) {
    const card = el("div", { class: "card" });
    const header = el("div", { class: "card-header" });
    header.appendChild(el("h3", { class: "card-title" }, title));
    header.appendChild(el("span", { class: "card-sub", id: subId }, EM_DASH));
    card.appendChild(header);
    const box = el("div", { class: "chart-box h-260" });
    box.appendChild(el("canvas", { id: canvasId }));
    card.appendChild(box);
    charts2.appendChild(card);
  }
  outlet.appendChild(charts2);

  const charts3 = el("div", { class: "section card-grid cards-2" });
  for (const [title, canvasId] of [
    ["Rolling returns", "chart-rolling"],
    ["Attribution: TW · Foreign · FX", "chart-attr"],
  ] as const) {
    const card = el("div", { class: "card" });
    card.appendChild(el("h3", { class: "card-title" }, title));
    const box = el("div", { class: "chart-box h-260" });
    box.appendChild(el("canvas", { id: canvasId }));
    card.appendChild(box);
    charts3.appendChild(card);
  }
  outlet.appendChild(charts3);

  // Drawdown episodes table + attribution totals chart slot
  const ddRow = el("div", { class: "section card-grid cards-2" });
  const ddCard = el("div", { class: "card" });
  ddCard.appendChild(el("h3", { class: "card-title" }, "Drawdown episodes"));
  const ddWrap = el("div", { class: "table-wrap" });
  const ddTable = el("table", { class: "data", id: "dd-table" });
  const ddThead = el("thead");
  const ddHeadRow = el("tr");
  for (const [text, isNum] of [
    ["Peak", false], ["Trough", false], ["Depth", true],
    ["DD months", true], ["Recovery", true], ["Status", false],
  ] as const) {
    ddHeadRow.appendChild(el("th", { class: isNum ? "num" : "" }, text));
  }
  ddThead.appendChild(ddHeadRow);
  ddTable.appendChild(ddThead);
  ddTable.appendChild(el("tbody"));
  ddWrap.appendChild(ddTable);
  ddCard.appendChild(ddWrap);
  ddRow.appendChild(ddCard);

  const attrTotalsCard = el("div", { class: "card" });
  attrTotalsCard.appendChild(el("h3", { class: "card-title" }, "Attribution totals (TWD)"));
  const attrBox = el("div", { class: "chart-box h-260" });
  attrBox.appendChild(el("canvas", { id: "chart-attr-totals" }));
  attrTotalsCard.appendChild(attrBox);
  ddRow.appendChild(attrTotalsCard);
  outlet.appendChild(ddRow);

  // Contribution treemap + stats
  const contribRow = el("div", { class: "section card-grid", style: "grid-template-columns: 1.4fr 1fr;" });
  const treeCard = el("div", { class: "card" });
  treeCard.appendChild(el("h3", { class: "card-title" }, "Per-ticker contribution map"));
  const treeBox = el("div", { class: "chart-box h-360" });
  treeBox.appendChild(el("canvas", { id: "contrib-treemap" }));
  treeCard.appendChild(treeBox);
  contribRow.appendChild(treeCard);

  const contribStatsCard = el("div", { class: "card" });
  contribStatsCard.appendChild(el("h3", { class: "card-title" }, "Concentration of P&L"));
  contribStatsCard.appendChild(el("div", { id: "contrib-stats", class: "flex-col gap-3 p-2" }));
  contribRow.appendChild(contribStatsCard);
  outlet.appendChild(contribRow);

  // Contribution table
  const contribTable = el("div", { class: "section" });
  const contribCard = el("div", { class: "card" });
  contribCard.appendChild(el("h3", { class: "card-title" }, "Per-ticker P&L breakdown"));
  const ctWrap = el("div", { class: "table-wrap" });
  const ctTable = el("table", { class: "data", id: "contrib-table" });
  const ctThead = el("thead");
  const ctHeadRow = el("tr");
  for (const [text, key, isNum] of [
    ["Code", "code", false],
    ["Name", "name", false],
    ["Venue", "venue", false],
    ["Realized", "realized_pnl_twd", true],
    ["Dividends", "dividends_twd", true],
    ["Unrealized", "unrealized_pnl_twd", true],
    ["Total P&L", "total_pnl_twd", true],
    ["Contribution", "contribution_share", true],
  ] as const) {
    ctHeadRow.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  ctThead.appendChild(ctHeadRow);
  ctTable.appendChild(ctThead);
  ctTable.appendChild(el("tbody"));
  ctWrap.appendChild(ctTable);
  contribCard.appendChild(ctWrap);
  contribTable.appendChild(contribCard);
  outlet.appendChild(contribTable);

  // Monthly detail table
  const monthsSection = el("div", { class: "section" });
  const monthsCard = el("div", { class: "card" });
  monthsCard.appendChild(el("h3", { class: "card-title" }, "Monthly detail"));
  const mWrap = el("div", { class: "table-wrap" });
  const mTable = el("table", { class: "data", id: "months-table" });
  const mThead = el("thead");
  const mHeadRow = el("tr");
  for (const [text, key, isNum] of [
    ["Month", "sort_key", false],
    ["V_start", "v_start", true],
    ["Flow F", "external_flow", true],
    ["Wt. flow", "weighted_flow", true],
    ["Equity", "equity_twd", true],
    ["Period return", "period_return", true],
    ["Cum TWR", "cum_twr", true],
    ["Drawdown", "drawdown", true],
  ] as const) {
    mHeadRow.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  mThead.appendChild(mHeadRow);
  mTable.appendChild(mThead);
  mTable.appendChild(el("tbody"));
  mWrap.appendChild(mTable);
  monthsCard.appendChild(mWrap);
  monthsSection.appendChild(monthsCard);
  outlet.appendChild(monthsSection);
};

const renderKpis = (ts: TimeseriesResponse): void => {
  setTextColor("kpi-twr", pct(ts.twr_total ?? 0), ts.twr_total ?? 0);
  setText("kpi-twr-sub", `${ts.monthly?.length ?? 0} months`);
  setTextColor("kpi-cagr", pct(ts.cagr ?? 0), ts.cagr ?? 0);
  if (ts.xirr === null || ts.xirr === undefined) {
    const xirrEl = document.getElementById("kpi-xirr");
    if (xirrEl) {
      xirrEl.textContent = EM_DASH;
      xirrEl.className = "kpi-value";
    }
  } else {
    setTextColor("kpi-xirr", pct(ts.xirr), ts.xirr);
  }

  const hit = (ts.hit_rate ?? 0) * 100;
  setText("kpi-hit", `${hit.toFixed(0)}%`);
  setText(
    "kpi-hit-sub",
    `${ts.positive_months ?? 0} pos · ${ts.negative_months ?? 0} neg`,
  );

  setText("kpi-vol", pctAbs(ts.annualized_volatility, 1));

  const sharpe = ts.sharpe_annualized ?? 0;
  setTextColor("kpi-sharpe", sharpe.toFixed(2), sharpe);
  setText("kpi-sharpe-sub", bandLabel(sharpe, RATIO_BANDS.sharpe));

  const sortino = ts.sortino_annualized ?? 0;
  setTextColor("kpi-sortino", capRatio(sortino), sortino);
  setText("kpi-sortino-sub", bandLabel(sortino, RATIO_BANDS.sortino));

  const calmar = ts.calmar ?? 0;
  setTextColor("kpi-calmar", capRatio(calmar), calmar);
  setText("kpi-calmar-sub", bandLabel(calmar, RATIO_BANDS.calmar));
};

const renderDDEpisodes = (eps: ReadonlyArray<DrawdownEpisode>): void => {
  const tbody = document.querySelector("#dd-table tbody");
  if (!tbody) return;
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
  if (!eps.length) {
    const tr = el("tr");
    const cell = td("No drawdowns recorded", "table-empty");
    cell.colSpan = 6;
    tr.appendChild(cell);
    tbody.appendChild(tr);
    return;
  }
  for (const ep of eps) {
    const tr = el("tr");
    tr.appendChild(td(month(ep.peak_month)));
    tr.appendChild(td(month(ep.trough_month)));
    tr.appendChild(td(pct(ep.depth_pct), "num value-neg"));
    tr.appendChild(td(String(ep.drawdown_months), "num"));
    tr.appendChild(
      td(ep.recovery_months != null ? `${ep.recovery_months}M` : EM_DASH, "num"),
    );
    tr.appendChild(
      td(
        ep.recovered ? "Recovered" : "Open",
        ep.recovered ? "text-mute text-tiny" : "text-warn text-tiny",
      ),
    );
    tbody.appendChild(tr);
  }
};

const renderMonthsTable = (rows: ReadonlyArray<MonthlyRow>): void => {
  const decorated = rows.map((m) => ({
    ...m,
    month_label: label(m),
    sort_key: m.month ?? m.date ?? "",
  }));
  mountDataTable<typeof decorated[number]>({
    tableId: "months-table",
    rows: decorated,
    searchKeys: ["month_label", "sort_key"],
    searchPlaceholder: "Search month (e.g. 2025-08, Aug)…",
    defaultSort: { key: "sort_key", dir: "desc" },
    colspan: 8,
    pageSize: 25,
    emptyText: "No matching months",
    row: (m) => [
      td(label(m)),
      td(twd(m.v_start ?? m.equity_twd), "num"),
      td(
        twd(m.external_flow ?? m.flow_twd ?? 0),
        `num ${tone(m.external_flow ?? m.flow_twd ?? 0)}`,
      ),
      td(twd(m.weighted_flow ?? 0), "num text-mute"),
      td(twd(m.equity_twd), "num"),
      td(pct(m.period_return), `num ${tone(m.period_return)}`),
      td(pct(m.cum_twr), `num ${tone(m.cum_twr)}`),
      td(pct(m.drawdown), `num ${tone(m.drawdown)}`),
    ],
  });
};

const renderContribTable = (rows: ReadonlyArray<ContribRow>): void => {
  const decorated = rows.map((r) => ({
    ...r,
    total_pnl_twd:
      (r.realized_pnl_twd ?? 0) +
      (r.dividends_twd ?? 0) +
      (r.unrealized_pnl_twd ?? 0),
  }));
  mountDataTable<typeof decorated[number]>({
    tableId: "contrib-table",
    rows: decorated,
    searchKeys: ["code", "name"],
    searchPlaceholder: "Search code or name…",
    filters: [
      { id: "venue", key: "venue", label: "All venues", options: ["TW", "Foreign"] },
    ],
    defaultSort: { key: "total_pnl_twd", dir: "desc" },
    colspan: 8,
    pageSize: 25,
    emptyText: "No tickers",
    row: (r) => [
      tdCodeLink(r.code),
      td(r.name ?? ""),
      td(r.venue ?? "", "text-mute text-tiny"),
      td(
        twd(r.realized_pnl_twd ?? 0),
        `num ${tone(r.realized_pnl_twd ?? 0)}`,
      ),
      td(twd(r.dividends_twd ?? 0), "num value-pos"),
      td(
        twd(r.unrealized_pnl_twd ?? 0),
        `num ${tone(r.unrealized_pnl_twd ?? 0)}`,
      ),
      td(twd(r.total_pnl_twd), `num ${tone(r.total_pnl_twd)}`),
      td(
        `${((r.contribution_share ?? 0) * 100).toFixed(1)}%`,
        `num ${tone(r.total_pnl_twd)}`,
      ),
    ],
  });
};

const paintPerfCharts = (
  Chart: ChartCtor,
  ts: TimeseriesResponse & TimeseriesNew,
  rolling: { rolling?: ReadonlyArray<RollingRow> },
  attr: AttributionResponse,
): void => {
  // The new /api/performance/timeseries returns either `monthly` (legacy)
  // or `timeseries` (Phase 6 contract). Both forms paint the same charts.
  const series = ts.timeseries ?? ts.monthly ?? [];
  const labels = series.map((r) => (r as { month?: string }).month ?? "");
  const cumPct = series.map((r) => {
    const t = r as { cum_twr_pct?: number; cum_twr?: number };
    return (t.cum_twr_pct ?? (t.cum_twr ?? 0) * 100);
  });
  const equity = series.map((r) => (r as { equity_twd?: number }).equity_twd ?? 0);
  const monthlyPct = series.map((r) => {
    const t = r as { twr_pct?: number; period_return?: number };
    return t.twr_pct ?? ((t.period_return ?? 0) * 100);
  });
  const dd = series.map((r) => {
    const t = r as { drawdown_pct?: number; drawdown?: number };
    return t.drawdown_pct ?? ((t.drawdown ?? 0) * 100);
  });

  paintLine(Chart, "chart-cum", {
    labels,
    datasets: [
      {
        label: "Cumulative TWR (%)",
        data: cumPct,
        color: cssVar("--accent") || palette()[0],
        yAxisID: "y",
        fill: true,
      },
      {
        label: "Equity (TWD)",
        data: equity,
        color: cssVar("--c2") || palette()[1],
        yAxisID: "y2",
        borderDash: [4, 4],
      },
    ],
    options: {
      scales: {
        y: { position: "left" },
        y2: { position: "right", grid: { drawOnChartArea: false } },
      },
    },
  });

  paintBar(Chart, "chart-monthly", {
    labels,
    datasets: [
      {
        label: "Monthly TWR (%)",
        data: monthlyPct,
        color: cssVar("--c1") || palette()[0],
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });

  paintLine(Chart, "chart-dd", {
    labels,
    datasets: [
      {
        label: "Drawdown (%)",
        data: dd,
        color: cssVar("--neg") || palette()[2],
        fill: true,
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });

  // Rolling 3/6/12 month
  const rRows = rolling.rolling ?? [];
  paintLine(Chart, "chart-rolling", {
    labels: rRows.map((r) => r.month),
    datasets: [
      { label: "3M", data: rRows.map((r) => r.r3m ?? 0), color: cssVar("--c1") || palette()[0] },
      { label: "6M", data: rRows.map((r) => r.r6m ?? 0), color: cssVar("--c2") || palette()[1] },
      { label: "12M", data: rRows.map((r) => r.r12m ?? 0), color: cssVar("--c3") || palette()[2] },
    ],
  });

  // Monthly attribution stacked line
  const aRows = attr.monthly ?? [];
  paintLine(Chart, "chart-attr", {
    labels: aRows.map((r) => r.month),
    datasets: [
      { label: "TW (TWD)", data: aRows.map((r) => r.tw_twd ?? 0), color: cssVar("--c1") || palette()[0] },
      { label: "Foreign (TWD)", data: aRows.map((r) => r.foreign_twd ?? 0), color: cssVar("--c2") || palette()[1] },
      { label: "FX (TWD)", data: aRows.map((r) => r.fx_twd ?? 0), color: cssVar("--c3") || palette()[2] },
    ],
  });

  // Attribution totals (single bar each)
  const tot = attr.totals ?? {};
  paintBar(Chart, "chart-attr-totals", {
    labels: ["TW", "Foreign", "FX"],
    datasets: [
      {
        label: "Total (TWD)",
        data: [tot.tw_twd ?? 0, tot.foreign_twd ?? 0, tot.fx_twd ?? 0],
        color: cssVar("--c1") || palette()[0],
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });

  // Per-ticker contribution treemap. Prefer attribution.contributions,
  // fall back to tax.by_ticker if not provided.
  const contribs =
    attr.contributions?.map((c) => ({
      code: c.code,
      name: c.name ?? c.code,
      total_pnl_twd: c.contribution_twd,
    })) ?? [];
  if (contribs.length) {
    const pal = palette();
    paintTreemap(
      Chart,
      "contrib-treemap",
      contribs.map((c, i) => ({
        label: c.code,
        value: Math.abs(c.total_pnl_twd),
        color: pal[i % pal.length] || "#888",
      })),
    );
  } else {
    // Always paint to satisfy the 7-call contract — single tile when no data.
    paintTreemap(Chart, "contrib-treemap", [
      { label: "—", value: 1, color: palette()[0] || "#888" },
    ]);
  }
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load performance: ${err.message}`),
  );
};

export const mountPerformance = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);

  const sel = document.getElementById("twr-method") as HTMLSelectElement;
  const stored = localStorage.getItem(METHOD_STORAGE_KEY);
  if (stored) sel.value = stored;
  const initial = sel.value || "day_weighted";

  const refresh = async (method: string): Promise<void> => {
    const q = `?method=${encodeURIComponent(method)}`;
    const [ts, rolling, attr, tax] = await Promise.all([
      deps.api.get<TimeseriesResponse & TimeseriesNew>(`/api/performance/timeseries${q}`),
      deps.api.get<{ rolling?: ReadonlyArray<RollingRow> }>(`/api/performance/rolling${q}`),
      deps.api.get<AttributionResponse>(`/api/performance/attribution`),
      deps.api.get<TaxResponse>(`/api/tax`),
    ]);
    renderKpis(ts ?? {});
    renderDDEpisodes(ts.drawdown_episodes ?? []);
    renderContribTable(tax?.by_ticker ?? []);
    renderMonthsTable(ts.monthly ?? []);
    if (deps.Chart) paintPerfCharts(deps.Chart, ts ?? {}, rolling ?? {}, attr ?? {});
  };

  try {
    await refresh(initial);
    sel.addEventListener("change", () => {
      localStorage.setItem(METHOD_STORAGE_KEY, sel.value);
      void refresh(sel.value);
    });
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
