// /today — tactical view: hero, period strip, drawdown, risk, calendar, movers.

import { EM_DASH, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintLine } from "../lib/paint";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

interface FetchEnvelope<T> {
  ok?: boolean;
  data?: T;
  error?: string;
}

export interface MountDeps {
  api: ApiLike;
  fetchJson: (path: string, init?: RequestInit) => Promise<FetchEnvelope<unknown>>;
  Chart?: ChartCtor;
}

interface SparklineResponse {
  points?: ReadonlyArray<{ date: string; equity_twd: number }>;
}

interface DrawdownResponse {
  empty?: boolean;
  points?: ReadonlyArray<{ date: string; drawdown_pct: number }>;
}

interface Snapshot {
  empty?: boolean;
  data_date?: string;
  weekday?: string;
  today_in_tpe?: string;
  equity_twd?: number;
  delta_twd?: number;
  delta_pct?: number;
  n_positions?: number;
  fx_usd_twd?: number;
}

interface Mover {
  symbol: string;
  delta_pct: number;
}

interface MoversResponse {
  movers?: ReadonlyArray<Mover>;
}

interface PeriodWindow {
  label?: string;
  delta_pct?: number | null;
  delta_twd?: number | null;
  anchor_date?: string | null;
}

interface PeriodsResponse {
  empty?: boolean;
  windows?: ReadonlyArray<PeriodWindow>;
}

interface RiskMetrics {
  empty?: boolean;
  ann_return_pct?: number | null;
  ann_vol_pct?: number | null;
  rolling_30d_vol_pct?: number | null;
  sharpe?: number | null;
  sortino?: number | null;
  max_drawdown_pct?: number | null;
  hit_rate_pct?: number | null;
  best_day_pct?: number | null;
  worst_day_pct?: number | null;
  n_days?: number;
}

interface CalendarResponse {
  empty?: boolean;
  cells?: ReadonlyArray<{ date: string; return_pct: number }>;
  months?: ReadonlyArray<{ year: number; month: number; label: string }>;
}

interface FreshnessResponse {
  data_date?: string | null;
  band?: "green" | "amber" | "red";
  stale_days?: number;
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

const fmtPct = (n: number | null | undefined, digits = 2): string => {
  if (n === null || n === undefined) return EM_DASH;
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(digits)}%`;
};

const fmtSignedTwd = (n: number | null | undefined): string => {
  if (n === null || n === undefined) return EM_DASH;
  const sign = n > 0 ? "+" : "";
  return `${sign}${twd(n)}`;
};

const setText = (id: string, text: string): void => {
  const node = document.getElementById(id);
  if (node) node.textContent = text;
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  // Refresh row
  const actions = el("div", { class: "page-actions" });
  const refreshBtn = el("button", {
    id: "refresh-btn",
    class: "btn btn--primary",
    type: "button",
  }, "Refresh now");
  actions.appendChild(refreshBtn);
  actions.appendChild(el("span", { id: "refresh-status", class: "meta" }));
  outlet.appendChild(actions);

  // Hero
  const hero = el("section", { class: "hero" });
  const heroTitle = el("div", { class: "hero__title" });
  heroTitle.appendChild(el("h2", { id: "data-date-heading" }, EM_DASH));
  heroTitle.appendChild(
    el("p", { id: "wallclock-context", class: "hero__subtitle", hidden: "" }),
  );
  hero.appendChild(heroTitle);

  const kpiRow = el("div", { class: "hero__kpis" });
  for (const [label, valueId, subId] of [
    ["Equity (TWD)", "equity-twd", null],
    ["Δ vs prior session", "delta-twd", "delta-pct"],
    ["Positions", "n-positions", null],
    ["USD/TWD", "fx-usd-twd", null],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi__label" }, label));
    card.appendChild(el("span", { class: "kpi__value", id: valueId }, EM_DASH));
    if (subId) {
      card.appendChild(el("span", { class: "kpi__sub", id: subId }, EM_DASH));
    }
    kpiRow.appendChild(card);
  }
  hero.appendChild(kpiRow);

  const heroChart = el("div", { class: "hero__chart" });
  heroChart.appendChild(el("canvas", { id: "equity-sparkline" }));
  hero.appendChild(heroChart);

  const periodStrip = el("div", { class: "period-strip", id: "period-strip" });
  for (const label of ["MTD", "QTD", "YTD", "Inception"]) {
    const cell = el("div", { class: "period-strip__cell" });
    cell.appendChild(el("span", { class: "period-strip__label" }, label));
    cell.appendChild(el("span", { class: "period-strip__value" }, EM_DASH));
    cell.appendChild(el("span", { class: "period-strip__sub" }, EM_DASH));
    periodStrip.appendChild(cell);
  }
  hero.appendChild(periodStrip);

  const fresh = el("div", { class: "hero__freshness" });
  fresh.appendChild(el("span", { class: "freshness-dot", id: "freshness-dot" }));
  fresh.appendChild(el("span", { id: "freshness-text" }, EM_DASH));
  hero.appendChild(fresh);

  outlet.appendChild(hero);

  // Drawdown + risk
  const grid = el("section", { class: "analyst-grid" });

  const ddCard = el("div", { class: "drawdown-card" });
  const ddHeader = el("div", { class: "drawdown-card__header" });
  ddHeader.appendChild(el("h3", { class: "drawdown-card__title" }, "Underwater equity"));
  ddHeader.appendChild(el("span", { class: "drawdown-card__current", id: "dd-current" }, EM_DASH));
  ddCard.appendChild(ddHeader);
  const ddChartBox = el("div", { class: "drawdown-card__chart" });
  ddChartBox.appendChild(el("canvas", { id: "dd-chart" }));
  ddCard.appendChild(ddChartBox);
  ddCard.appendChild(el("p", { class: "text-sm text-mute", id: "dd-detail" }, EM_DASH));
  grid.appendChild(ddCard);

  const riskTile = el("div", { class: "risk-tile" });
  const riskHeader = el("div", { class: "risk-tile__header" });
  riskHeader.appendChild(el("h3", { class: "risk-tile__title" }, "Risk & return"));
  riskHeader.appendChild(el("span", { class: "risk-tile__sub", id: "risk-window-meta" }, EM_DASH));
  riskTile.appendChild(riskHeader);
  const riskGrid = el("div", { class: "risk-tile__grid" });
  for (const [label, valueId, detailId] of [
    ["Annualized return", "risk-ann-return", "risk-best-day"],
    ["Annualized vol", "risk-ann-vol", "risk-rolling-vol"],
    ["Sharpe", "risk-sharpe", "risk-sortino"],
    ["Max drawdown", "risk-max-dd", "risk-hit-rate"],
  ] as const) {
    const stat = el("div", { class: "risk-stat" });
    stat.appendChild(el("span", { class: "risk-stat__label" }, label));
    stat.appendChild(el("span", { class: "risk-stat__value", id: valueId }, EM_DASH));
    stat.appendChild(el("span", { class: "risk-stat__detail", id: detailId }, EM_DASH));
    riskGrid.appendChild(stat);
  }
  riskTile.appendChild(riskGrid);
  grid.appendChild(riskTile);

  outlet.appendChild(grid);

  // Calendar
  const cal = el("section", { class: "cal-heatmap" });
  cal.appendChild(el("h3", { class: "cal-heatmap__title" }, "Daily return calendar"));
  cal.appendChild(el("div", { class: "cal-heatmap__months", id: "cal-months" }));
  outlet.appendChild(cal);

  // Movers
  const movers = el("section", { class: "movers" });
  movers.appendChild(el("h3", {}, "Top movers"));
  const split = el("div", { class: "movers__split" });
  for (const [title, listId] of [
    ["Gainers", "movers-up"],
    ["Decliners", "movers-down"],
  ] as const) {
    const col = el("div");
    col.appendChild(el("h4", {}, title));
    col.appendChild(el("ul", { id: listId, class: "data-list" }));
    split.appendChild(col);
  }
  movers.appendChild(split);
  outlet.appendChild(movers);
};

const paintHero = (s: Snapshot): void => {
  const heading = document.getElementById("data-date-heading");
  if (!s || s.empty) {
    if (heading) heading.textContent = "No daily data yet";
    return;
  }
  if (heading && s.weekday && s.data_date) {
    heading.textContent = `Performance for ${s.weekday}, ${s.data_date}`;
  }

  const ctx = document.getElementById("wallclock-context") as HTMLElement | null;
  if (ctx) {
    if (s.today_in_tpe && s.today_in_tpe !== s.data_date) {
      ctx.textContent =
        `Wall clock today (TPE): ${s.today_in_tpe}. ` +
        `Markets closed or pre-open — showing the last priced session.`;
      ctx.hidden = false;
    } else {
      ctx.hidden = true;
    }
  }

  setText("equity-twd", twd(s.equity_twd));
  const deltaEl = document.getElementById("delta-twd");
  if (deltaEl) {
    deltaEl.textContent = fmtSignedTwd(s.delta_twd);
    deltaEl.classList.toggle("kpi__value--up", (s.delta_twd ?? 0) > 0);
    deltaEl.classList.toggle("kpi__value--down", (s.delta_twd ?? 0) < 0);
  }
  setText("delta-pct", fmtPct(s.delta_pct));
  setText("n-positions", s.n_positions != null ? String(s.n_positions) : EM_DASH);
  setText(
    "fx-usd-twd",
    s.fx_usd_twd != null ? s.fx_usd_twd.toFixed(3) : EM_DASH,
  );
};

const renderMoversList = (
  listId: string,
  rows: ReadonlyArray<Mover>,
  sign: "pos" | "neg",
): void => {
  const list = document.getElementById(listId);
  if (!list) return;
  while (list.firstChild) list.removeChild(list.firstChild);
  if (rows.length === 0) {
    list.appendChild(el("li", { class: "muted" }, EM_DASH));
    return;
  }
  for (const m of rows) {
    const li = el("li");
    li.dataset.sign = sign;
    const link = el("a", {
      href: `/ticker/${encodeURIComponent(m.symbol)}`,
    });
    link.appendChild(el("span", { class: "data-list__symbol" }, m.symbol));
    link.appendChild(el("span", { class: "data-list__pct" }, fmtPct(m.delta_pct)));
    li.appendChild(link);
    list.appendChild(li);
  }
};

const paintMovers = (resp: MoversResponse): void => {
  const movers = resp.movers ?? [];
  renderMoversList("movers-up", movers.filter((m) => m.delta_pct > 0).slice(0, 5), "pos");
  renderMoversList(
    "movers-down",
    movers.filter((m) => m.delta_pct < 0).slice(0, 5),
    "neg",
  );
};

const paintPeriodStrip = (data: PeriodsResponse): void => {
  if (!data || data.empty) return;
  const cells = document.querySelectorAll<HTMLElement>(".period-strip__cell");
  (data.windows ?? []).forEach((w, i) => {
    const cell = cells[i];
    if (!cell) return;
    const valueEl = cell.querySelector<HTMLElement>(".period-strip__value");
    const subEl = cell.querySelector<HTMLElement>(".period-strip__sub");
    if (!valueEl || !subEl) return;
    if (w.delta_pct == null) {
      valueEl.textContent = EM_DASH;
      subEl.textContent = "no data";
      return;
    }
    valueEl.textContent = fmtPct(w.delta_pct);
    valueEl.classList.toggle("pos", w.delta_pct > 0);
    valueEl.classList.toggle("neg", w.delta_pct < 0);
    const dt = w.delta_twd != null ? fmtSignedTwd(w.delta_twd) : "";
    subEl.textContent = `${dt}${w.anchor_date ? " · since " + w.anchor_date : ""}`;
  });
};

const paintRisk = (data: RiskMetrics): void => {
  if (!data || data.empty) return;
  const annR = data.ann_return_pct;
  const annRel = document.getElementById("risk-ann-return");
  if (annRel) {
    annRel.textContent = annR == null ? EM_DASH : fmtPct(annR);
    annRel.classList.toggle("pos", (annR ?? 0) > 0);
    annRel.classList.toggle("neg", (annR ?? 0) < 0);
  }

  setText(
    "risk-ann-vol",
    data.ann_vol_pct == null ? EM_DASH : `${data.ann_vol_pct.toFixed(2)}%`,
  );
  setText(
    "risk-rolling-vol",
    data.rolling_30d_vol_pct == null
      ? EM_DASH
      : `30d rolling: ${data.rolling_30d_vol_pct.toFixed(2)}%`,
  );

  setText("risk-sharpe", data.sharpe == null ? EM_DASH : data.sharpe.toFixed(2));
  setText(
    "risk-sortino",
    data.sortino == null ? EM_DASH : `Sortino ${data.sortino.toFixed(2)}`,
  );

  const maxDdEl = document.getElementById("risk-max-dd");
  if (maxDdEl) {
    maxDdEl.textContent =
      data.max_drawdown_pct == null
        ? EM_DASH
        : `${data.max_drawdown_pct.toFixed(2)}%`;
    if (data.max_drawdown_pct != null && data.max_drawdown_pct < 0) {
      maxDdEl.classList.add("neg");
    }
  }
  setText(
    "risk-hit-rate",
    data.hit_rate_pct == null
      ? EM_DASH
      : `Up days ${data.hit_rate_pct.toFixed(0)}%`,
  );

  if (data.best_day_pct != null && data.worst_day_pct != null) {
    setText(
      "risk-best-day",
      `Best ${data.best_day_pct > 0 ? "+" : ""}${data.best_day_pct.toFixed(2)}% · ` +
        `worst ${data.worst_day_pct.toFixed(2)}%`,
    );
  } else {
    setText("risk-best-day", EM_DASH);
  }

  setText("risk-window-meta", `${data.n_days ?? 0} trading days`);
};

const paintCalendar = (data: CalendarResponse): void => {
  const root = document.getElementById("cal-months");
  if (!root) return;
  while (root.firstChild) root.removeChild(root.firstChild);
  if (!data || data.empty || !(data.cells?.length)) {
    const empty = el(
      "p",
      { class: "text-mute" },
      "Need at least 2 daily equity rows to render the calendar.",
    );
    root.appendChild(empty);
    return;
  }
  // Cell paint deferred to Cycle 66.
  for (const m of data.months ?? []) {
    const card = el("div", { class: "cal-month" });
    card.appendChild(el("div", { class: "cal-month__title" }, m.label));
    root.appendChild(card);
  }
};

const paintFreshness = (data: FreshnessResponse): void => {
  const dot = document.getElementById("freshness-dot") as HTMLElement | null;
  const txt = document.getElementById("freshness-text");
  if (!dot || !txt) return;
  if (!data || !data.data_date) {
    dot.dataset.band = "red";
    txt.textContent = "no data";
    return;
  }
  dot.dataset.band = data.band ?? "red";
  const days = data.stale_days ?? 0;
  txt.textContent =
    `Latest data: ${data.data_date} · ${days <= 0 ? "today" : days + "d ago"}`;
};

const wireRefresh = (deps: MountDeps, reload: () => Promise<void>): void => {
  const btn = document.getElementById("refresh-btn") as HTMLButtonElement | null;
  const status = document.getElementById("refresh-status");
  if (!btn || !status) return;
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    status.textContent = "Refreshing…";
    try {
      const body = await deps.fetchJson("/api/admin/refresh", { method: "POST" });
      if (body.ok === false) {
        throw new Error(body.error ?? "refresh failed");
      }
      const summary = (body.data ?? {}) as { new_dates?: number; new_rows?: number };
      status.textContent = `${summary.new_dates ?? 0} new dates, ${summary.new_rows ?? 0} rows`;
      await reload();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      status.textContent = `refresh failed: ${msg}`;
    } finally {
      btn.disabled = false;
    }
  });
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  const box = el("div", { class: "error-box" }, `Failed to load /today: ${err.message}`);
  outlet.appendChild(box);
};

const paintSparkline = (Chart: ChartCtor, data: SparklineResponse): void => {
  const points = data.points ?? [];
  if (!points.length) return;
  paintLine(Chart, "equity-sparkline", {
    labels: points.map((p) => p.date),
    datasets: [
      {
        label: "Equity (TWD)",
        data: points.map((p) => p.equity_twd),
        color: cssVar("--accent") || palette()[0],
        fill: true,
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });
};

const paintDrawdownChart = (Chart: ChartCtor, data: DrawdownResponse): void => {
  if (data.empty || !(data.points?.length)) return;
  const ddCurrent = document.getElementById("dd-current");
  if (ddCurrent && data.points.length) {
    const last = data.points[data.points.length - 1];
    ddCurrent.textContent = `${last.drawdown_pct.toFixed(2)}%`;
  }
  paintLine(Chart, "dd-chart", {
    labels: data.points.map((p) => p.date),
    datasets: [
      {
        label: "Drawdown (%)",
        data: data.points.map((p) => p.drawdown_pct),
        color: cssVar("--neg") || palette()[2],
        fill: true,
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });
};

export const mountToday = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);

  const loadAll = async (): Promise<void> => {
    const [snap, mv, spark, fresh, periods, dd, risk, cal] = await Promise.all([
      deps.api.get<Snapshot>("/api/today/snapshot"),
      deps.api.get<MoversResponse>("/api/today/movers"),
      deps.api.get<SparklineResponse>("/api/today/sparkline"),
      deps.api.get<FreshnessResponse>("/api/today/freshness"),
      deps.api.get<PeriodsResponse>("/api/today/period-returns"),
      deps.api.get<DrawdownResponse>("/api/today/drawdown"),
      deps.api.get<RiskMetrics>("/api/today/risk-metrics"),
      deps.api.get<CalendarResponse>("/api/today/calendar"),
    ]);
    paintHero(snap ?? {});
    paintMovers(mv ?? {});
    paintFreshness(fresh ?? {});
    paintPeriodStrip(periods ?? {});
    paintRisk(risk ?? {});
    paintCalendar(cal ?? {});
    if (deps.Chart) {
      paintSparkline(deps.Chart, spark ?? {});
      paintDrawdownChart(deps.Chart, dd ?? {});
    }
  };

  try {
    await loadAll();
    wireRefresh(deps, loadAll);
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
