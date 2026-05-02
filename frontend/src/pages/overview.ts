// Overview page — KPIs, equity-curve canvas, allocation canvas, top
// winners/losers, recent activity. Phase 8 Cycle 57 port of
// templates/overview.html + static/js/pages/overview.js.
//
// Charts (equity, allocation) and the activity DataTable are scaffolded
// here as canvas slots and a table skeleton — the actual Chart.js wiring
// + DataTable mount land in Cycles 58 (DataTable component) and 67
// (chart wiring sweep). app.css already styles the canvas containers
// and table-wrap so the layout is correct from day one.

import { EM_DASH, date, int, month, num, pct, tone, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintDoughnut, paintLine } from "../lib/paint";
import { el, setText } from "../lib/dom";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  Chart?: ChartCtor;
}

interface SummaryResponse {
  kpis?: {
    real_now_twd?: number;
    fx_usd_twd?: number;
    as_of?: string;
  };
  profit_twd?: number;
  invested_twd?: number;
  twr?: number;
  xirr?: number | null;
  first_month?: string;
  last_month?: string;
  months_covered?: number;
  equity_curve?: ReadonlyArray<{
    date?: string;
    month?: string;
    equity_twd?: number;
    cum_twr?: number;
  }>;
  allocation?: {
    tw?: number;
    foreign?: number;
    bank_twd?: number;
    bank_usd?: number;
  };
}

interface Holding {
  code?: string;
  name?: string;
  unrealized_pnl_twd?: number;
  unrealized_pct?: number;
}

interface HoldingsResponse {
  holdings?: ReadonlyArray<Holding>;
}

interface Transaction {
  date?: string;
  venue?: string;
  side?: string;
  code?: string;
  name?: string;
  qty?: number;
  price?: number;
  net_twd?: number;
}

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const kpiGrid = el("div", { class: "card-grid cards-4 section" });
  const kpis: ReadonlyArray<{
    label: string;
    valueId: string;
    subId: string;
    subInitial: string;
    isHero?: boolean;
  }> = [
    {
      label: "Portfolio equity",
      valueId: "kpi-equity",
      subId: "kpi-equity-sub",
      subInitial: EM_DASH,
      isHero: true,
    },
    {
      label: "Net profit",
      valueId: "kpi-profit",
      subId: "kpi-profit-sub",
      subInitial: "vs. counterfactual capital",
    },
    {
      label: "Time-weighted return",
      valueId: "kpi-twr",
      subId: "kpi-twr-since",
      subInitial: EM_DASH,
    },
    {
      label: "Money-weighted (XIRR)",
      valueId: "kpi-xirr",
      subId: "kpi-xirr-sub",
      subInitial: "annualized, deposit-aware",
    },
  ];
  for (const k of kpis) {
    const card = el("div", { class: k.isHero ? "kpi kpi-hero" : "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, k.label));
    card.appendChild(
      el("span", { class: "kpi-value", id: k.valueId }, EM_DASH),
    );
    const sub = el("span", { class: "kpi-sub" });
    if (k.valueId === "kpi-twr") {
      sub.appendChild(document.createTextNode("since "));
      sub.appendChild(el("span", { id: k.subId }, EM_DASH));
    } else {
      sub.appendChild(el("span", { id: k.subId, class: "text-mute" }, k.subInitial));
    }
    card.appendChild(sub);
    kpiGrid.appendChild(card);
  }
  outlet.appendChild(kpiGrid);

  const chartsRow = el("div", {
    class: "section card-grid",
    style: "grid-template-columns: 1.6fr 1fr;",
  });
  const equityCard = el("div", { class: "card" });
  const equityHeader = el("div", { class: "card-header" });
  equityHeader.appendChild(el("h3", { class: "card-title" }, "Equity curve"));
  equityHeader.appendChild(el("span", { class: "card-sub", id: "equity-range" }, EM_DASH));
  equityCard.appendChild(equityHeader);
  const equityBox = el("div", { class: "chart-box h-320" });
  equityBox.appendChild(el("canvas", { id: "chart-equity" }));
  equityCard.appendChild(equityBox);
  chartsRow.appendChild(equityCard);

  const allocCard = el("div", { class: "card" });
  const allocHeader = el("div", { class: "card-header" });
  allocHeader.appendChild(el("h3", { class: "card-title" }, "Allocation"));
  allocHeader.appendChild(el("span", { class: "card-sub", id: "alloc-total" }, EM_DASH));
  allocCard.appendChild(allocHeader);
  const allocBox = el("div", { class: "chart-box h-320" });
  allocBox.appendChild(el("canvas", { id: "chart-alloc" }));
  allocCard.appendChild(allocBox);
  chartsRow.appendChild(allocCard);
  outlet.appendChild(chartsRow);

  const moversRow = el("div", { class: "section card-grid cards-2" });
  for (const [title, listId, link] of [
    ["Top winners", "winners-list", "/holdings"],
    ["Top losers", "losers-list", "/holdings"],
  ] as const) {
    const card = el("div", { class: "card" });
    const header = el("div", { class: "card-header" });
    header.appendChild(el("h3", { class: "card-title" }, title));
    header.appendChild(el("a", { href: link, class: "card-sub" }, "All holdings →"));
    card.appendChild(header);
    card.appendChild(el("div", { id: listId, class: "flex-col gap-2" }));
    moversRow.appendChild(card);
  }
  outlet.appendChild(moversRow);

  const activitySection = el("div", { class: "section" });
  const activityCard = el("div", { class: "card" });
  const activityHeader = el("div", { class: "card-header" });
  activityHeader.appendChild(el("h3", { class: "card-title" }, "Recent activity"));
  activityHeader.appendChild(
    el("a", { href: "/transactions", class: "card-sub" }, "Transaction log →"),
  );
  activityCard.appendChild(activityHeader);
  const tableWrap = el("div", { class: "table-wrap" });
  const table = el("table", { class: "data", id: "activity-table" });
  const thead = el("thead");
  const headerRow = el("tr");
  for (const [text, key, isNum] of [
    ["Date", "date", false],
    ["Venue", "venue", false],
    ["Side", "side", false],
    ["Code", "code", false],
    ["Name", "name", false],
    ["Qty", "qty", true],
    ["Price", "price", true],
    ["Net (TWD)", "net_twd", true],
  ] as const) {
    const th = el(
      "th",
      { class: isNum ? "num sortable" : "sortable", "data-key": key },
      text,
    );
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);
  table.appendChild(el("tbody"));
  tableWrap.appendChild(table);
  activityCard.appendChild(tableWrap);
  activitySection.appendChild(activityCard);
  outlet.appendChild(activitySection);
};

const setKpiWithTone = (
  id: string,
  v: number | null | undefined,
  formatted: string,
): void => {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = formatted;
  node.className = `kpi-value ${tone(v)}`;
};

const renderKpis = (s: SummaryResponse): void => {
  const equity = s.kpis?.real_now_twd ?? 0;
  setText("kpi-equity", twd(equity));
  const fx = s.kpis?.fx_usd_twd;
  setText(
    "kpi-equity-sub",
    `${month(s.kpis?.as_of)} · USD/TWD ${
      fx !== undefined && fx !== null ? fx.toFixed(3) : EM_DASH
    }`,
  );

  const profit = s.profit_twd ?? 0;
  setKpiWithTone("kpi-profit", profit, twd(profit));
  setText("kpi-profit-sub", `vs. ${twd(s.invested_twd ?? 0)} capital`);

  setKpiWithTone("kpi-twr", s.twr ?? 0, pct(s.twr ?? 0));
  setText("kpi-twr-since", month(s.first_month));

  const xirrEl = document.getElementById("kpi-xirr");
  if (!xirrEl) return;
  if (s.xirr === null || s.xirr === undefined) {
    xirrEl.textContent = EM_DASH;
    xirrEl.className = "kpi-value";
  } else {
    xirrEl.textContent = pct(s.xirr);
    xirrEl.className = `kpi-value ${tone(s.xirr)}`;
  }
};

const renderAllocationTotal = (s: SummaryResponse): void => {
  const a = s.allocation ?? {};
  const total =
    (a.tw ?? 0) + (a.foreign ?? 0) + (a.bank_twd ?? 0) + (a.bank_usd ?? 0);
  setText("alloc-total", twd(total));
  // Chart.js wiring lands in a follow-up cycle that sweeps all charts.
};

const renderEquityRange = (s: SummaryResponse): void => {
  const span =
    s.first_month && s.last_month
      ? `${month(s.first_month)} → ${month(s.last_month)} · ${
          s.months_covered ?? 0
        } months`
      : EM_DASH;
  setText("equity-range", span);
};

const renderMover = (
  listId: string,
  rows: ReadonlyArray<Holding>,
  toneCls: "value-pos" | "value-neg",
): void => {
  const list = document.getElementById(listId);
  if (!list) return;
  while (list.firstChild) list.removeChild(list.firstChild);
  if (rows.length === 0) {
    list.appendChild(el("div", { class: "empty-state" }, "No positions"));
    return;
  }
  for (const r of rows) {
    const row = el("a", {
      href: `/ticker/${encodeURIComponent(r.code ?? "")}`,
      class: "bar-row",
      style:
        "grid-template-columns: 1.6fr 1fr 1fr; padding: 8px 4px; border-bottom: 1px solid var(--line); color: inherit;",
    });
    const left = el("span");
    const code = el("strong", { style: "font-size: 13px;" }, r.code ?? "");
    const name = el(
      "span",
      { class: "text-mute text-sm", style: "margin-left: 8px;" },
      r.name ?? "",
    );
    left.appendChild(code);
    left.appendChild(name);
    row.appendChild(left);
    row.appendChild(
      el("span", { class: `num text-sm ${toneCls}` }, twd(r.unrealized_pnl_twd)),
    );
    row.appendChild(
      el("span", { class: `num text-sm ${toneCls}` }, pct(r.unrealized_pct)),
    );
    list.appendChild(row);
  }
};

const renderTopMovers = (resp: HoldingsResponse): void => {
  const rows = [...(resp.holdings ?? [])].sort(
    (a, b) => (b.unrealized_pnl_twd ?? 0) - (a.unrealized_pnl_twd ?? 0),
  );
  renderMover("winners-list", rows.slice(0, 5), "value-pos");
  renderMover("losers-list", rows.slice(-5).reverse(), "value-neg");
};

const renderActivityRows = (rows: ReadonlyArray<Transaction>): void => {
  // Bare-bones rendering until Cycle 58 ships the typed DataTable.
  const tbody = document.querySelector<HTMLElement>(
    "#activity-table tbody",
  );
  if (!tbody) return;
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
  for (const t of rows.slice(0, 15)) {
    const tr = el("tr");
    tr.appendChild(el("td", { class: "text-mute" }, date(t.date)));
    const venuePill = el("td");
    venuePill.appendChild(el("span", { class: "pill" }, t.venue ?? ""));
    tr.appendChild(venuePill);
    tr.appendChild(el("td", {}, t.side ?? ""));
    const codeCell = el("td", { class: "code" });
    codeCell.appendChild(
      el(
        "a",
        { href: `/ticker/${encodeURIComponent(t.code ?? "")}` },
        t.code ?? "",
      ),
    );
    tr.appendChild(codeCell);
    tr.appendChild(el("td", {}, t.name ?? ""));
    tr.appendChild(el("td", { class: "num" }, int(t.qty)));
    tr.appendChild(el("td", { class: "num" }, num(t.price, 2)));
    tr.appendChild(
      el(
        "td",
        { class: `num ${tone(t.net_twd)}` },
        twd(t.net_twd),
      ),
    );
    tbody.appendChild(tr);
  }
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  const box = el(
    "div",
    { class: "error-box" },
    `Failed to load overview: ${err.message}`,
  );
  outlet.appendChild(box);
};

const paintCharts = (Chart: ChartCtor, summary: SummaryResponse): void => {
  const curve = summary.equity_curve ?? [];
  if (curve.length) {
    const labels = curve.map((p) => p.month ?? p.date ?? "");
    const equity = curve.map((p) => p.equity_twd ?? 0);
    const cumTwr = curve.map((p) => (p.cum_twr ?? 0) * 100);
    paintLine(Chart, "chart-equity", {
      labels,
      datasets: [
        {
          label: "Equity (TWD)",
          data: equity,
          color: cssVar("--accent") || palette()[0],
          yAxisID: "y",
          fill: true,
        },
        {
          label: "Cumulative TWR (%)",
          data: cumTwr,
          color: cssVar("--c2") || palette()[1],
          yAxisID: "y2",
          borderDash: [4, 4],
          borderWidth: 1.5,
        },
      ],
      options: {
        scales: {
          y: { position: "left" },
          y2: { position: "right", grid: { drawOnChartArea: false } },
        },
      },
    });
  }

  const a = summary.allocation ?? {};
  const segments = [
    { label: "TW securities", value: a.tw ?? 0, color: cssVar("--c1") || palette()[0] },
    { label: "Foreign securities", value: a.foreign ?? 0, color: cssVar("--c2") || palette()[1] },
    { label: "Cash (TWD)", value: a.bank_twd ?? 0, color: cssVar("--c4") || palette()[3] },
    { label: "Cash (USD)", value: a.bank_usd ?? 0, color: cssVar("--c6") || palette()[5] },
  ];
  paintDoughnut(Chart, "chart-alloc", {
    labels: segments.map((s) => s.label),
    values: segments.map((s) => s.value),
    colors: segments.map((s) => s.color),
  });
};

export const mountOverview = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const [summary, holdings, txs] = await Promise.all([
      deps.api.get<SummaryResponse>("/api/summary"),
      deps.api.get<HoldingsResponse>("/api/holdings/current"),
      deps.api.get<ReadonlyArray<Transaction>>("/api/transactions"),
    ]);
    renderKpis(summary);
    renderAllocationTotal(summary);
    renderEquityRange(summary);
    renderTopMovers(holdings);
    renderActivityRows(txs ?? []);
    if (deps.Chart) paintCharts(deps.Chart, summary);
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
