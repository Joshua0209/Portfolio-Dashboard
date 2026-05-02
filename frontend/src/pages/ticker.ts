// /ticker/:code drill-down — header, KPIs, position+pnl chart slots,
// trades + dividends DataTables. Phase 8 Cycle 65.
// Charts deferred to Cycle 66.

import { mountDataTable } from "../components/DataTable";
import { EM_DASH, date as fmtDate, int, month as fmtMonth, num, pct, tone, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintLine } from "../lib/paint";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  code: string;
  Chart?: ChartCtor;
}

interface PositionHistoryPoint {
  month?: string;
  date?: string;
  qty?: number;
  ref_price?: number;
  cost_twd?: number;
  mkt_value_twd?: number;
  market_value_twd?: number;
}

interface TickerData {
  error?: string;
  name?: string;
  is_open?: boolean;
  last_seen_month?: string | null;
  current?: {
    qty?: number;
    avg_cost?: number;
    cost_twd?: number;
    unrealized_pnl_twd?: number;
  };
  summary?: {
    realized_pnl_twd?: number;
    realized_pnl_pct?: number | null;
  };
  position_history?: ReadonlyArray<PositionHistoryPoint>;
  position_history_daily?: ReadonlyArray<PositionHistoryPoint>;
  history?: ReadonlyArray<PositionHistoryPoint>;
  daily_prices?:
    | { points: ReadonlyArray<{ date: string; close: number }>; trades?: ReadonlyArray<unknown> }
    | ReadonlyArray<{ date: string; close: number }>;
  trades?: ReadonlyArray<{
    date?: string;
    side?: string;
    qty?: number;
    price?: number;
    gross_twd?: number;
    fee_twd?: number;
    net_twd?: number;
  }>;
  dividends?: ReadonlyArray<{
    month?: string;
    date?: string;
    ccy?: string;
    amount_local?: number;
    amount_twd?: number;
  }>;
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

const setText = (id: string, t: string): void => {
  const node = document.getElementById(id);
  if (node) node.textContent = t;
};

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const renderScaffold = (outlet: HTMLElement, code: string): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const heading = el("div", { class: "page-actions" });
  heading.appendChild(el("h2", { id: "ticker-code" }, code));
  heading.appendChild(el("span", { id: "ticker-name", class: "text-mute" }));
  outlet.appendChild(heading);

  const kpis = el("div", { class: "card-grid cards-4 section" });
  for (const [label, valueId, subId] of [
    ["Quantity", "kpi-qty", "kpi-status"],
    ["Avg cost", "kpi-avg", "kpi-cost"],
    ["Realized P&L", "kpi-real", "kpi-real-pct"],
    ["Unrealized P&L", "kpi-unreal", "kpi-unreal-pct"],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  // Chart slots (paint deferred to Cycle 66)
  const dailyCard = el("div", { class: "card section", id: "chart-daily-card", hidden: "" });
  dailyCard.appendChild(el("h3", { class: "card-title" }, "Daily price + trade markers"));
  const dailyBox = el("div", { class: "chart-box h-320" });
  dailyBox.appendChild(el("canvas", { id: "chart-daily" }));
  dailyCard.appendChild(dailyBox);
  outlet.appendChild(dailyCard);

  const charts = el("div", { class: "section card-grid cards-2" });
  for (const [title, canvasId] of [
    ["Position over time", "chart-pos"],
    ["Cost basis vs market value", "chart-pnl"],
  ] as const) {
    const card = el("div", { class: "card" });
    card.appendChild(el("h3", { class: "card-title" }, title));
    const box = el("div", { class: "chart-box h-260" });
    box.appendChild(el("canvas", { id: canvasId }));
    card.appendChild(box);
    charts.appendChild(card);
  }
  outlet.appendChild(charts);

  // Trades table
  const tradeSec = el("div", { class: "section" });
  const tradeCard = el("div", { class: "card" });
  tradeCard.appendChild(el("h3", { class: "card-title" }, "Trades"));
  const tradeWrap = el("div", { class: "table-wrap" });
  const tradeTable = el("table", { class: "data", id: "trade-table" });
  const tradeThead = el("thead");
  const tradeHead = el("tr");
  for (const [text, key, isNum] of [
    ["Date", "date", false],
    ["Side", "side", false],
    ["Qty", "qty", true],
    ["Price", "price", true],
    ["Gross (TWD)", "gross_twd", true],
    ["Fee", "fee_twd", true],
    ["Net (TWD)", "net_twd", true],
  ] as const) {
    tradeHead.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  tradeThead.appendChild(tradeHead);
  tradeTable.appendChild(tradeThead);
  tradeTable.appendChild(el("tbody"));
  tradeWrap.appendChild(tradeTable);
  tradeCard.appendChild(tradeWrap);
  tradeSec.appendChild(tradeCard);
  outlet.appendChild(tradeSec);

  // Dividends table
  const divSec = el("div", { class: "section" });
  const divCard = el("div", { class: "card" });
  divCard.appendChild(el("h3", { class: "card-title" }, "Dividends"));
  const divWrap = el("div", { class: "table-wrap" });
  const divTable = el("table", { class: "data", id: "div-table" });
  const divThead = el("thead");
  const divHead = el("tr");
  for (const [text, key, isNum] of [
    ["Month", "month", false],
    ["Date", "date", false],
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

const renderKpis = (data: TickerData): void => {
  const cur = data.current;
  const lastSeen = data.last_seen_month;
  setText("kpi-qty", cur?.qty != null ? int(cur.qty) : "0");
  setText(
    "kpi-status",
    data.is_open
      ? "Open position"
      : lastSeen
        ? `Closed · last seen ${fmtMonth(lastSeen)}`
        : "Never held",
  );
  setText("kpi-avg", cur?.avg_cost != null ? num(cur.avg_cost, 2) : EM_DASH);
  setText("kpi-cost", cur?.cost_twd != null ? `${twd(cur.cost_twd)} basis` : EM_DASH);

  const s = data.summary ?? {};
  const realEl = document.getElementById("kpi-real");
  if (realEl) {
    realEl.textContent = twd(s.realized_pnl_twd);
    realEl.className = `kpi-value ${tone(s.realized_pnl_twd)}`;
  }
  setText(
    "kpi-real-pct",
    s.realized_pnl_pct === null || s.realized_pnl_pct === undefined
      ? EM_DASH
      : pct(s.realized_pnl_pct),
  );

  if (cur) {
    const pnl = cur.unrealized_pnl_twd ?? 0;
    const pctVal = cur.cost_twd ? pnl / cur.cost_twd : 0;
    const unrealEl = document.getElementById("kpi-unreal");
    if (unrealEl) {
      unrealEl.textContent = twd(pnl);
      unrealEl.className = `kpi-value ${tone(pnl)}`;
    }
    setText("kpi-unreal-pct", pct(pctVal));
  } else {
    setText("kpi-unreal", EM_DASH);
    setText("kpi-unreal-pct", "fully closed");
  }
};

const paintTickerCharts = (Chart: ChartCtor, data: TickerData): void => {
  // Daily price line
  const dp = data.daily_prices;
  const dailyPoints: ReadonlyArray<{ date: string; close: number }> = Array.isArray(dp)
    ? (dp as ReadonlyArray<{ date: string; close: number }>)
    : (dp as { points?: ReadonlyArray<{ date: string; close: number }> } | undefined)?.points ?? [];
  if (dailyPoints.length) {
    paintLine(Chart, "chart-daily", {
      labels: dailyPoints.map((p) => p.date),
      datasets: [
        {
          label: "Close",
          data: dailyPoints.map((p) => p.close),
          color: cssVar("--accent") || palette()[0],
          fill: true,
        },
      ],
      options: { plugins: { legend: { display: false } } },
    });
  }

  // Position history (qty + cost vs market value)
  const hist =
    data.position_history_daily ??
    data.position_history ??
    data.history ??
    [];
  if (hist.length) {
    const labels = hist.map((p) => p.date ?? p.month ?? "");
    paintLine(Chart, "chart-pos", {
      labels,
      datasets: [
        {
          label: "Quantity",
          data: hist.map((p) => p.qty ?? 0),
          color: cssVar("--c2") || palette()[1],
          fill: true,
        },
      ],
      options: { plugins: { legend: { display: false } } },
    });
    paintLine(Chart, "chart-pnl", {
      labels,
      datasets: [
        {
          label: "Cost (TWD)",
          data: hist.map((p) => p.cost_twd ?? 0),
          color: cssVar("--c5") || palette()[4],
        },
        {
          label: "Market value (TWD)",
          data: hist.map((p) => p.mkt_value_twd ?? p.market_value_twd ?? 0),
          color: cssVar("--accent") || palette()[0],
          fill: true,
        },
      ],
    });
  }
};

const showEmpty = (outlet: HTMLElement, code: string): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  const box = el("div", { class: "empty-state" });
  box.appendChild(el("h3", {}, `No data for ${code}`));
  box.appendChild(
    el(
      "p",
      {},
      "This ticker isn't in the parsed by_ticker summary. Check tw_ticker_map.json.",
    ),
  );
  outlet.appendChild(box);
};

export const mountTicker = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet, deps.code);
  let data: TickerData;
  try {
    data = await deps.api.get<TickerData>(
      `/api/tickers/${encodeURIComponent(deps.code)}`,
    );
  } catch {
    showEmpty(outlet, deps.code);
    return;
  }
  if (!data || data.error) {
    showEmpty(outlet, deps.code);
    return;
  }
  setText("ticker-name", data.name ?? "");
  renderKpis(data);
  if (deps.Chart) paintTickerCharts(deps.Chart, data);

  const trades = data.trades ?? [];
  mountDataTable<typeof trades[number]>({
    tableId: "trade-table",
    rows: trades,
    searchKeys: ["side", "date"],
    searchPlaceholder: "Search side or date…",
    defaultSort: { key: "date", dir: "desc" },
    colspan: 7,
    pageSize: 25,
    emptyText: "No trades",
    row: (t) => [
      td(fmtDate(t.date), "text-mute"),
      td(t.side ?? ""),
      td(int(t.qty), "num"),
      td(num(t.price, 2), "num"),
      td(twd(t.gross_twd), "num"),
      td(twd(t.fee_twd), "num text-mute"),
      td(twd(t.net_twd), `num ${tone(t.net_twd)}`),
    ],
  });

  const divs = data.dividends ?? [];
  mountDataTable<typeof divs[number]>({
    tableId: "div-table",
    rows: divs,
    searchKeys: ["ccy", "month", "date"],
    searchPlaceholder: "Search month or ccy…",
    defaultSort: { key: "date", dir: "desc" },
    colspan: 5,
    pageSize: 15,
    emptyText: "No dividends",
    row: (d) => [
      td(fmtMonth(d.month)),
      td(fmtDate(d.date), "text-mute"),
      td(d.ccy ?? "", "text-mute"),
      td(num(d.amount_local, 2), "num"),
      td(twd(d.amount_twd), "num value-pos"),
    ],
  });
};
