// /risk — drawdown curve, concentration, leverage.

import { EM_DASH, pct, pctAbs, twd } from "../lib/format";
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

interface RiskResponse {
  max_drawdown?: number;
  annualized_volatility?: number;
  top_5_share?: number;
  top_10_share?: number;
  position_count?: number;
  leverage_pct?: number;
  leverage_value_twd?: number;
  current_drawdown?: number;
  drawdown_curve?: ReadonlyArray<{ date?: string; month?: string; drawdown: number }>;
  weight_distribution?: ReadonlyArray<{ code?: string; weight: number }>;
  leverage_timeline?: ReadonlyArray<{ month: string; leverage_pct: number }>;
  sharpe_annualized?: number;
  sortino_annualized?: number;
  calmar?: number;
  effective_n?: number;
  downside_volatility?: number;
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

const capRatio = (v: number): string => {
  if (!Number.isFinite(v) || Math.abs(v) > 100) return v > 0 ? "≫ 10" : "≪ −10";
  return v.toFixed(2);
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const kpiGrid = el("div", { class: "card-grid cards-4 section" });
  const KPIS: ReadonlyArray<readonly [string, string, string?, string?]> = [
    ["Max drawdown", "kpi-mdd"],
    ["Volatility (annual)", "kpi-vol"],
    ["Top-5 concentration", "kpi-top5", "kpi-positions"],
    ["Leverage", "kpi-lev", "kpi-lev-sub"],
  ];
  for (const [label, valueId, subId] of KPIS) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) {
      card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    } else {
      card.appendChild(el("span", { class: "kpi-sub" }));
    }
    kpiGrid.appendChild(card);
  }
  outlet.appendChild(kpiGrid);

  // Drawdown curve card
  const ddSection = el("div", { class: "section" });
  const ddCard = el("div", { class: "card" });
  const ddHeader = el("div", { class: "card-header" });
  ddHeader.appendChild(el("h3", { class: "card-title" }, "Drawdown curve"));
  ddHeader.appendChild(el("span", { class: "card-sub", id: "dd-current" }, EM_DASH));
  ddCard.appendChild(ddHeader);
  const ddBox = el("div", { class: "chart-box h-260" });
  ddBox.appendChild(el("canvas", { id: "chart-dd" }));
  ddCard.appendChild(ddBox);
  ddSection.appendChild(ddCard);
  outlet.appendChild(ddSection);

  // Weights donut card
  const wSection = el("div", { class: "section" });
  const wCard = el("div", { class: "card" });
  const wHeader = el("div", { class: "card-header" });
  wHeader.appendChild(el("h3", { class: "card-title" }, "Weight distribution"));
  wHeader.appendChild(el("span", { class: "card-sub", id: "hhi-label" }, EM_DASH));
  wCard.appendChild(wHeader);
  const wBox = el("div", { class: "chart-box h-320" });
  wBox.appendChild(el("canvas", { id: "chart-weights" }));
  wCard.appendChild(wBox);
  wSection.appendChild(wCard);
  outlet.appendChild(wSection);

  // Leverage timeline
  const lSection = el("div", { class: "section" });
  const lCard = el("div", { class: "card" });
  lCard.appendChild(el("h3", { class: "card-title" }, "Leverage timeline"));
  const lBox = el("div", { class: "chart-box h-260" });
  lBox.appendChild(el("canvas", { id: "chart-leverage" }));
  lCard.appendChild(lBox);
  lSection.appendChild(lCard);
  outlet.appendChild(lSection);

  // Ratios table
  const rSection = el("div", { class: "section" });
  const rCard = el("div", { class: "card" });
  rCard.appendChild(el("h3", { class: "card-title" }, "Risk ratios"));
  const rWrap = el("div", { class: "table-wrap" });
  const rTable = el("table", { class: "data", id: "risk-ratios" });
  rTable.appendChild(el("tbody"));
  rWrap.appendChild(rTable);
  rCard.appendChild(rWrap);
  rSection.appendChild(rCard);
  outlet.appendChild(rSection);
};

const renderKpis = (r: RiskResponse): void => {
  const mdd = document.getElementById("kpi-mdd");
  if (mdd) {
    mdd.textContent = pct(r.max_drawdown);
    mdd.className =
      "kpi-value " + ((r.max_drawdown ?? 0) < 0 ? "value-neg" : "value-mute");
  }
  setText("kpi-vol", pctAbs(r.annualized_volatility, 1));
  setText("kpi-top5", pctAbs(r.top_5_share, 1));
  setText("kpi-positions", `${r.position_count ?? 0} open positions`);
  setText("kpi-lev", pctAbs(r.leverage_pct, 1));
  setText("kpi-lev-sub", `${twd(r.leverage_value_twd ?? 0)} on margin`);
};

const renderHhi = (
  weights: ReadonlyArray<{ weight: number }>,
): void => {
  const hhi = weights.reduce((s, w) => s + w.weight ** 2, 0);
  setText("hhi-label", `HHI ${hhi.toFixed(3)}`);
};

const renderDdCurrent = (current: number | undefined): void => {
  setText("dd-current", `Current: ${pct(current)}`);
};

const renderRatios = (r: RiskResponse): void => {
  const tbody = document.querySelector("#risk-ratios tbody");
  if (!tbody) return;
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
  const items: ReadonlyArray<readonly [string, string, string]> = [
    ["Sharpe", capRatio(r.sharpe_annualized ?? 0), "(μ-rf)/σ × √12"],
    ["Sortino", capRatio(r.sortino_annualized ?? 0), "downside σ only"],
    ["Calmar", capRatio(r.calmar ?? 0), "CAGR / |max DD|"],
    ["Effective N", (r.effective_n ?? 0).toFixed(2), "1 / HHI"],
    ["Top-5 share", `${((r.top_5_share ?? 0) * 100).toFixed(1)}%`, ""],
    ["Top-10 share", `${((r.top_10_share ?? 0) * 100).toFixed(1)}%`, ""],
    ["Downside vol (ann)", `${((r.downside_volatility ?? 0) * 100).toFixed(2)}%`, ""],
  ];
  for (const [k, v, n] of items) {
    const tr = document.createElement("tr");
    const t1 = el("td", {}, k);
    const t2 = el("td", { class: "num" }, v);
    const t3 = el("td", { class: "text-mute text-tiny" }, n);
    tr.append(t1, t2, t3);
    tbody.appendChild(tr);
  }
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load risk: ${err.message}`),
  );
};

const paintCharts = (Chart: ChartCtor, r: RiskResponse): void => {
  const dd = r.drawdown_curve ?? [];
  if (dd.length) {
    paintLine(Chart, "chart-dd", {
      labels: dd.map((p) => p.month ?? p.date ?? ""),
      datasets: [
        {
          label: "Drawdown (%)",
          data: dd.map((p) => p.drawdown * 100),
          color: cssVar("--neg") || palette()[2],
          fill: true,
        },
      ],
      options: { plugins: { legend: { display: false } } },
    });
  }

  const weights = r.weight_distribution ?? [];
  if (weights.length) {
    const pal = palette();
    paintBar(Chart, "chart-weights", {
      labels: weights.map((w) => w.code ?? ""),
      datasets: [
        {
          label: "Weight",
          data: weights.map((w) => w.weight * 100),
          color: pal[0] || "#888",
        },
      ],
      options: { indexAxis: "y", plugins: { legend: { display: false } } },
    });
  }

  const lev = r.leverage_timeline ?? [];
  if (lev.length) {
    paintBar(Chart, "chart-leverage", {
      labels: lev.map((p) => p.month),
      datasets: [
        {
          label: "Leverage (%)",
          data: lev.map((p) => p.leverage_pct * 100),
          color: cssVar("--c3") || palette()[2],
        },
      ],
      options: { plugins: { legend: { display: false } } },
    });
  } else {
    // Test fixture has no leverage_timeline — paint an empty single-bar so
    // chart-leverage still mounts, matching the legacy behavior of always
    // showing the leverage chart even when the series is empty.
    paintBar(Chart, "chart-leverage", {
      labels: [""],
      datasets: [{ label: "Leverage (%)", data: [0], color: palette()[2] || "#888" }],
      options: { plugins: { legend: { display: false } } },
    });
  }
};

export const mountRisk = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const r = await deps.api.get<RiskResponse>("/api/risk");
    renderKpis(r);
    renderHhi(r.weight_distribution ?? []);
    renderDdCurrent(r.current_drawdown);
    renderRatios(r);
    if (deps.Chart) paintCharts(deps.Chart, r);
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
