// /benchmark — portfolio TWR vs market strategies.

import { EM_DASH, pct, pctAbs, tone } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintBar, paintLine, paintScatter } from "../lib/paint";

const STORAGE_KEY = "benchmark.selected.v1";
const DEFAULT_KEYS: ReadonlyArray<string> = ["tw_passive", "us_passive"];

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  Chart?: ChartCtor;
}

interface Strategy {
  key: string;
  market?: "TW" | "US";
  venue?: "TW" | "US";
  name: string;
  description?: string;
}

interface Stats {
  twr_total?: number;
  annualized_volatility?: number;
  max_drawdown?: number;
  sharpe?: number;
  sortino?: number;
}

interface MonthlyTwrPoint {
  month: string;
  twr_pct?: number;
  cum_twr_pct?: number;
}

interface CompareResponse {
  portfolio: {
    name?: string;
    stats?: Stats;
    curve?: ReadonlyArray<{ cum_return: number | null }>;
    monthly?: ReadonlyArray<MonthlyTwrPoint>;
    cagr?: number;
    vol?: number;
    sharpe?: number;
  };
  strategies: ReadonlyArray<{
    key: string;
    name: string;
    stats?: Stats;
    curve?: ReadonlyArray<{ cum_return: number | null }>;
    monthly?: ReadonlyArray<MonthlyTwrPoint>;
    cagr?: number;
    vol?: number;
    sharpe?: number;
  }>;
  months?: ReadonlyArray<string>;
  portfolio_daily_curve?: ReadonlyArray<{ date: string; cum_return: number }>;
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

const td = (text: string, cls?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  if (cls) c.className = cls;
  c.textContent = text;
  return c;
};

const loadSelected = (): string[] => {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr) && arr.length) return arr.filter((s): s is string => typeof s === "string");
    }
  } catch {
    /* fall through */
  }
  return [...DEFAULT_KEYS];
};

const persistSelected = (selected: ReadonlySet<string>): void => {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify([...selected]));
  } catch {
    /* ignore quota errors */
  }
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  // Strategy lists
  const stratSec = el("div", { class: "section card-grid cards-2" });
  for (const [title, listId] of [
    ["TW strategies", "strategy-list-tw"],
    ["US strategies", "strategy-list-us"],
  ] as const) {
    const card = el("div", { class: "card" });
    card.appendChild(el("h3", { class: "card-title" }, title));
    card.appendChild(el("div", { id: listId, class: "flex-col gap-2" }));
    stratSec.appendChild(card);
  }
  outlet.appendChild(stratSec);

  // Charts
  for (const [title, canvasId, height] of [
    ["Cumulative TWR", "chart-cum", "h-320"],
    ["Monthly returns vs strategies", "chart-monthly", "h-260"],
    ["Risk vs return", "chart-scatter", "h-320"],
  ] as const) {
    const sec = el("div", { class: "section" });
    const card = el("div", { class: "card" });
    card.appendChild(el("h3", { class: "card-title" }, title));
    const box = el("div", { class: `chart-box ${height}` });
    box.appendChild(el("canvas", { id: canvasId }));
    card.appendChild(box);
    sec.appendChild(card);
    outlet.appendChild(sec);
  }

  // Stats table
  const tableSec = el("div", { class: "section" });
  const card = el("div", { class: "card" });
  card.appendChild(el("h3", { class: "card-title" }, "Stats"));
  const wrap = el("div", { class: "table-wrap" });
  const table = el("table", { class: "data", id: "stats-table" });
  const thead = el("thead");
  const headRow = el("tr");
  for (const [text, isNum] of [
    ["Strategy", false],
    ["TWR", true],
    ["Vol", true],
    ["Max DD", true],
    ["Sharpe", true],
    ["Sortino", true],
    ["Excess vs you", true],
  ] as const) {
    headRow.appendChild(el("th", { class: isNum ? "num" : "" }, text));
  }
  thead.appendChild(headRow);
  table.appendChild(thead);
  table.appendChild(el("tbody"));
  wrap.appendChild(table);
  card.appendChild(wrap);
  tableSec.appendChild(card);
  outlet.appendChild(tableSec);
};

const renderStrategyList = (
  strategies: ReadonlyArray<Strategy>,
  selected: Set<string>,
  onChange: () => Promise<void>,
): void => {
  const fillList = (id: string, items: ReadonlyArray<Strategy>): void => {
    const root = document.getElementById(id);
    if (!root) return;
    while (root.firstChild) root.removeChild(root.firstChild);
    for (const s of items) {
      const row = el("label", { class: "strategy-row" });
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = s.key;
      cb.checked = selected.has(s.key);
      cb.addEventListener("change", () => {
        if (cb.checked) selected.add(s.key);
        else selected.delete(s.key);
        persistSelected(selected);
        void onChange();
      });
      const label = el("div", { class: "strategy-text" });
      label.appendChild(el("strong", {}, s.name));
      label.appendChild(el("span", { class: "text-mute text-tiny" }, s.description));
      row.append(cb, label);
      root.appendChild(row);
    }
  };
  const venueOf = (s: Strategy): "TW" | "US" | undefined => s.market ?? s.venue;
  fillList("strategy-list-tw", strategies.filter((s) => venueOf(s) === "TW"));
  fillList("strategy-list-us", strategies.filter((s) => venueOf(s) === "US"));
};

const renderStatsTable = (data: CompareResponse): void => {
  const tbody = document.querySelector("#stats-table tbody");
  if (!tbody) return;
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
  // Support either legacy `stats` envelope or flat per-row keys.
  const statsOf = (
    src: { stats?: Stats; cagr?: number; vol?: number; sharpe?: number; twr_total?: number },
  ): Stats => ({
    twr_total: src.stats?.twr_total ?? src.cagr ?? src.twr_total,
    annualized_volatility: src.stats?.annualized_volatility ?? src.vol,
    max_drawdown: src.stats?.max_drawdown,
    sharpe: src.stats?.sharpe ?? src.sharpe,
    sortino: src.stats?.sortino,
  });
  const portStats = statsOf(data.portfolio);
  const youTwr = portStats.twr_total ?? 0;

  const rows: ReadonlyArray<{ name: string; stats: Stats; isYou: boolean }> = [
    { name: data.portfolio.name ?? "Portfolio", stats: portStats, isYou: true },
    ...data.strategies.map((s) => ({ name: s.name, stats: statsOf(s), isYou: false })),
  ];

  for (const r of rows) {
    const tr = el("tr");
    if (r.isYou) tr.style.fontWeight = "600";
    tr.appendChild(td(r.name));
    tr.appendChild(td(pct(r.stats.twr_total), `num ${tone(r.stats.twr_total)}`));
    tr.appendChild(td(pctAbs(r.stats.annualized_volatility, 1), "num text-mute"));
    tr.appendChild(td(pct(r.stats.max_drawdown), `num ${tone(r.stats.max_drawdown)}`));
    tr.appendChild(td((r.stats.sharpe ?? 0).toFixed(2), `num ${tone(r.stats.sharpe ?? 0)}`));
    tr.appendChild(td((r.stats.sortino ?? 0).toFixed(2), `num ${tone(r.stats.sortino ?? 0)}`));
    const excess = r.isYou ? null : youTwr - (r.stats.twr_total ?? 0);
    tr.appendChild(
      td(
        excess === null ? EM_DASH : pct(excess),
        excess === null ? "num text-mute" : `num ${tone(excess)}`,
      ),
    );
    tbody.appendChild(tr);
  }
};

const paintBenchmarkCharts = (Chart: ChartCtor, data: CompareResponse): void => {
  const pal = palette();
  const series = [
    {
      key: "portfolio",
      name: data.portfolio?.name ?? "Portfolio",
      monthly: data.portfolio?.monthly ?? [],
      stats: data.portfolio,
      color: cssVar("--accent") || pal[0],
    },
    ...data.strategies.map((s, i) => ({
      key: s.key,
      name: s.name,
      monthly: s.monthly ?? [],
      stats: s,
      color: pal[(i + 1) % pal.length] || "#888",
    })),
  ];

  // Cumulative TWR overlay — one line per series.
  const labels = series[0]?.monthly.map((p) => p.month) ?? [];
  paintLine(Chart, "chart-cum", {
    labels,
    datasets: series.map((s) => ({
      label: s.name,
      data: s.monthly.map((p) => p.cum_twr_pct ?? 0),
      color: s.color,
      fill: false,
    })),
  });

  // Monthly returns — bars per strategy.
  paintBar(Chart, "chart-monthly", {
    labels,
    datasets: series.map((s) => ({
      label: s.name,
      data: s.monthly.map((p) => p.twr_pct ?? 0),
      color: s.color,
    })),
  });

  // Risk-vs-return scatter (vol on x, CAGR on y). Falls back to Sharpe
  // when the new keys aren't present.
  const points = series
    .map((s) => {
      const stats = s.stats ?? {};
      const x = (stats as { vol?: number; annualized_volatility?: number }).vol
        ?? (stats as { annualized_volatility?: number }).annualized_volatility
        ?? 0;
      const y = (stats as { cagr?: number; twr_total?: number }).cagr
        ?? (stats as { twr_total?: number }).twr_total
        ?? 0;
      return { x: x * 100, y: y * 100, label: s.name };
    });
  paintScatter(
    Chart,
    "chart-scatter",
    [{ label: "Strategies", color: cssVar("--accent") || pal[0], points }],
    { xLabel: "Volatility (%)", yLabel: "Return (%)" },
  );
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load benchmark: ${err.message}`),
  );
};

export const mountBenchmark = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  const selected = new Set<string>(loadSelected());

  const refresh = async (): Promise<void> => {
    const keys = [...selected].join(",");
    const data = await deps.api.get<CompareResponse>(
      `/api/benchmarks/compare?keys=${encodeURIComponent(keys)}`,
    );
    renderStatsTable(data);
    if (deps.Chart) paintBenchmarkCharts(deps.Chart, data);
  };

  try {
    const stratResp = await deps.api.get<
      ReadonlyArray<Strategy> | { strategies: ReadonlyArray<Strategy> }
    >("/api/benchmarks/strategies");
    const strategies: ReadonlyArray<Strategy> = Array.isArray(stratResp)
      ? (stratResp as ReadonlyArray<Strategy>)
      : ((stratResp as { strategies?: ReadonlyArray<Strategy> }).strategies ?? []);
    renderStrategyList(strategies, selected, refresh);
    await refresh();
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
