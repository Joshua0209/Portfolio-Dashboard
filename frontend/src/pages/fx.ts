// /fx — USD/TWD curve, FX P&L, currency exposure.

import { EM_DASH, pct, pctAbs, tone, twd } from "../lib/format";
import type { ChartCtor } from "../lib/charts";
import { cssVar, palette } from "../lib/charts";
import { paintBar, paintLine } from "../lib/paint";

interface ApiLike {
  get<T = unknown>(path: string): Promise<T>;
}

export interface MountDeps {
  api: ApiLike;
  Chart?: ChartCtor;
}

interface FxResponse {
  empty?: boolean;
  current_rate?: number;
  first_rate?: number;
  foreign_share?: number;
  foreign_value_twd?: number;
  fx_pnl?: {
    contribution_twd?: number;
    monthly?: ReadonlyArray<{ month: string; fx_pnl_twd: number; cumulative_fx_pnl_twd: number }>;
  };
  rate_curve?: ReadonlyArray<{
    date?: string;
    month?: string;
    fx_usd_twd?: number;
    usd_twd?: number;
  }>;
  fx_pnl_monthly?: ReadonlyArray<{ month: string; fx_pnl_twd: number }>;
  fx_pnl_total_twd?: number;
  currency_exposure?: ReadonlyArray<{ currency: string; value_twd: number }>;
  by_ccy_monthly?: ReadonlyArray<Record<string, number | string>>;
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

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const kpis = el("div", { class: "card-grid cards-4 section" });
  const TILES: ReadonlyArray<readonly [string, string, string]> = [
    ["Current USD/TWD", "kpi-cur", "kpi-first"],
    ["Foreign exposure", "kpi-fx", "kpi-fx-twd"],
    ["FX-attributed P&L", "kpi-fx-pnl", ""],
    ["Δ rate", "kpi-drate", ""],
  ];
  for (const [label, valueId, subId] of TILES) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) {
      card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    } else {
      card.appendChild(el("span", { class: "kpi-sub" }));
    }
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  // Chart slots
  for (const [title, canvasId, height] of [
    ["USD/TWD rate", "chart-rate", "h-260"],
    ["FX P&L", "chart-fx-pnl", "h-260"],
    ["Currency exposure", "chart-ccy", "h-320"],
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
};

const renderEmpty = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  const box = el("div", { class: "empty-state" });
  box.appendChild(el("h3", {}, "No FX data yet"));
  box.appendChild(
    el(
      "p",
      {},
      "Run parse_statements.py against the latest PDFs to populate data/portfolio.json.",
    ),
  );
  outlet.appendChild(box);
};

const renderKpis = (fx: FxResponse): void => {
  setText(
    "kpi-cur",
    fx.current_rate ? fx.current_rate.toFixed(3) : EM_DASH,
  );
  setText(
    "kpi-first",
    fx.first_rate ? `from ${fx.first_rate.toFixed(3)}` : "",
  );
  setText("kpi-fx", pctAbs(fx.foreign_share, 1));
  setText("kpi-fx-twd", twd(fx.foreign_value_twd));

  const contrib = fx.fx_pnl?.contribution_twd ?? 0;
  const pnlEl = document.getElementById("kpi-fx-pnl");
  if (pnlEl) {
    pnlEl.textContent = twd(contrib);
    pnlEl.className = `kpi-value ${tone(contrib)}`;
  }

  if (fx.first_rate && fx.current_rate) {
    const d = (fx.current_rate - fx.first_rate) / fx.first_rate;
    const dEl = document.getElementById("kpi-drate");
    if (dEl) {
      dEl.textContent = pct(d);
      dEl.className = `kpi-value ${tone(d)}`;
    }
  }
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load FX: ${err.message}`),
  );
};

const paintCharts = (Chart: ChartCtor, fx: FxResponse): void => {
  const rate = fx.rate_curve ?? [];
  if (rate.length) {
    paintLine(Chart, "chart-rate", {
      labels: rate.map((p) => p.month ?? p.date ?? ""),
      datasets: [
        {
          label: "USD/TWD",
          data: rate.map((p) => p.fx_usd_twd ?? p.usd_twd ?? 0),
          color: cssVar("--accent") || palette()[0],
          fill: true,
        },
      ],
      options: { plugins: { legend: { display: false } } },
    });
  }

  const fxMonthly = fx.fx_pnl_monthly ?? fx.fx_pnl?.monthly ?? [];
  paintBar(Chart, "chart-fx-pnl", {
    labels: fxMonthly.map((p) => p.month),
    datasets: [
      {
        label: "FX P&L (TWD)",
        data: fxMonthly.map((p) => p.fx_pnl_twd),
        color: cssVar("--c2") || palette()[1],
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });

  const exposure = fx.currency_exposure ?? [];
  const pal = palette();
  paintBar(Chart, "chart-ccy", {
    labels: exposure.map((e) => e.currency),
    datasets: [
      {
        label: "Value (TWD)",
        data: exposure.map((e) => e.value_twd),
        color: pal[0] || "#888",
      },
    ],
    options: { plugins: { legend: { display: false } } },
  });
};

export const mountFx = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const fx = await deps.api.get<FxResponse>("/api/fx");
    if (fx.empty) {
      renderEmpty(outlet);
      return;
    }
    renderKpis(fx);
    if (deps.Chart) paintCharts(deps.Chart, fx);
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
