// /cashflows — real vs counterfactual, monthly waterfall, bank ledger.

import { mountDataTable } from "../components/DataTable";
import { EM_DASH, date as fmtDate, month as fmtMonth, num, pct, tone, twd } from "../lib/format";
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

interface CashflowsCum {
  real_now_twd?: number;
  counterfactual_twd?: number;
  profit_twd?: number;
  real_curve?: ReadonlyArray<{ month: string; value: number }>;
  counterfactual_curve?: ReadonlyArray<{ month: string; value: number }>;
  cumulative_flows?: ReadonlyArray<{ label: string; value: number }>;
  cumulative?: ReadonlyArray<{
    month: string;
    real_curve?: number;
    counterfactual?: number;
  }>;
}

interface MonthlyRow {
  month?: string;
  inflow_twd?: number;
  outflow_twd?: number;
  net_twd?: number;
}

interface BankRow {
  date?: string;
  month?: string;
  account?: string;
  category?: string;
  memo?: string;
  summary?: string;
  ccy?: string;
  amount?: number;
  amount_twd?: number;
  signed_amount?: number;
  balance?: number;
  _haystack?: string;
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

const tdPill = (text?: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.appendChild(el("span", { class: "pill" }, text ?? ""));
  return c;
};

const renderScaffold = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);

  const kpis = el("div", { class: "card-grid cards-3 section" });
  for (const [label, valueId, subId] of [
    ["Real equity now", "kpi-real", ""],
    ["Counterfactual capital", "kpi-cf", ""],
    ["Profit", "kpi-profit", "kpi-profit-pct"],
  ] as const) {
    const card = el("div", { class: "kpi" });
    card.appendChild(el("span", { class: "kpi-label" }, label));
    card.appendChild(el("span", { class: "kpi-value", id: valueId }, EM_DASH));
    if (subId) card.appendChild(el("span", { class: "kpi-sub", id: subId }, EM_DASH));
    kpis.appendChild(card);
  }
  outlet.appendChild(kpis);

  // Charts (slots only)
  const cfSection = el("div", { class: "section" });
  const cfCard = el("div", { class: "card" });
  cfCard.appendChild(el("h3", { class: "card-title" }, "Real vs counterfactual"));
  const cfBox = el("div", { class: "chart-box h-320" });
  cfBox.appendChild(el("canvas", { id: "chart-cf" }));
  cfCard.appendChild(cfBox);
  cfSection.appendChild(cfCard);
  outlet.appendChild(cfSection);

  const monthlySection = el("div", { class: "section card-grid cards-2" });
  const monthlyCard = el("div", { class: "card" });
  const monthlyHeader = el("div", { class: "card-header" });
  monthlyHeader.appendChild(el("h3", { class: "card-title" }, "Monthly flows"));
  const flowSel = document.createElement("select");
  flowSel.id = "flow-view";
  flowSel.className = "select";
  monthlyHeader.appendChild(flowSel);
  monthlyCard.appendChild(monthlyHeader);
  const monthlyBox = el("div", { class: "chart-box h-260" });
  monthlyBox.appendChild(el("canvas", { id: "chart-monthly" }));
  monthlyCard.appendChild(monthlyBox);
  monthlyCard.appendChild(el("div", { class: "text-mute text-tiny", id: "flow-hint" }));
  monthlySection.appendChild(monthlyCard);

  const breakdownCard = el("div", { class: "card" });
  breakdownCard.appendChild(el("h3", { class: "card-title" }, "Cumulative breakdown"));
  breakdownCard.appendChild(el("div", { id: "breakdown-list", class: "flex-col gap-2" }));
  monthlySection.appendChild(breakdownCard);
  outlet.appendChild(monthlySection);

  // Bank table
  const tableSection = el("div", { class: "section" });
  const tCard = el("div", { class: "card" });
  tCard.appendChild(el("h3", { class: "card-title" }, "Bank ledger"));
  const wrap = el("div", { class: "table-wrap" });
  const table = el("table", { class: "data", id: "bank-table" });
  const thead = el("thead");
  const headRow = el("tr");
  for (const [text, key, isNum] of [
    ["Month", "month", false],
    ["Date", "date", false],
    ["Account", "account", false],
    ["Category", "category", false],
    ["Memo", "memo", false],
    ["Ccy", "ccy", false],
    ["Amount", "signed_amount", true],
    ["TWD", "amount_twd", true],
    ["Balance", "balance", true],
  ] as const) {
    headRow.appendChild(
      el("th", { class: `${isNum ? "num " : ""}sortable`, "data-key": key }, text),
    );
  }
  thead.appendChild(headRow);
  table.appendChild(thead);
  table.appendChild(el("tbody"));
  wrap.appendChild(table);
  tCard.appendChild(wrap);
  tableSection.appendChild(tCard);
  outlet.appendChild(tableSection);
};

const renderKpis = (cf: CashflowsCum): void => {
  setText("kpi-real", twd(cf.real_now_twd));
  setText("kpi-cf", twd(cf.counterfactual_twd));
  const profit = cf.profit_twd ?? 0;
  const profitEl = document.getElementById("kpi-profit");
  if (profitEl) {
    profitEl.textContent = twd(profit);
    profitEl.className = `kpi-value ${tone(profit)}`;
  }
  const ratio = cf.counterfactual_twd ? profit / cf.counterfactual_twd : 0;
  setText("kpi-profit-pct", `${pct(ratio)} on capital`);
};

const renderBreakdown = (
  flows: ReadonlyArray<{ label: string; value: number }>,
): void => {
  const root = document.getElementById("breakdown-list");
  if (!root) return;
  while (root.firstChild) root.removeChild(root.firstChild);
  const max = Math.max(1, ...flows.map((f) => Math.abs(f.value)));
  for (const r of flows) {
    const row = el("div", { class: "bar-row" });
    row.appendChild(el("span", { class: "text-sm" }, r.label));
    const bar = el("span", { class: `bar ${r.value >= 0 ? "pos" : "neg"}` });
    const fill = el("span");
    fill.style.width = `${((Math.abs(r.value) / max) * 100).toFixed(2)}%`;
    bar.appendChild(fill);
    row.appendChild(bar);
    row.appendChild(el("span", { class: `num text-sm ${tone(r.value)}` }, twd(r.value)));
    root.appendChild(row);
  }
};

const renderBank = (rawRows: ReadonlyArray<BankRow>): void => {
  const rows = [...rawRows].sort((a, b) =>
    (b.date ?? "").localeCompare(a.date ?? ""),
  );
  for (const r of rows) {
    r._haystack = `${r.memo ?? ""} ${r.summary ?? ""} ${r.category ?? ""}`.toLowerCase();
  }
  const cats = [...new Set(rows.map((r) => r.category).filter((c): c is string => Boolean(c)))].sort();
  const months = [
    ...new Set(rows.map((r) => r.month).filter((m): m is string => Boolean(m))),
  ].sort().reverse();

  mountDataTable<BankRow>({
    tableId: "bank-table",
    rows,
    searchKeys: ["_haystack"],
    searchPlaceholder: "Search memo or category…",
    filters: [
      { id: "account", key: "account", label: "All accounts", options: ["TWD", "FOREIGN"] },
      { id: "category", key: "category", label: "All categories", options: cats },
      {
        id: "month",
        key: "month",
        label: "All months",
        options: months.map((m) => ({ value: m, label: fmtMonth(m) })),
      },
    ],
    defaultSort: { key: "date", dir: "desc" },
    colspan: 9,
    pageSize: 50,
    emptyText: "No matching transactions",
    row: (t) => {
      const local = t.signed_amount ?? t.amount ?? 0;
      const t_twd = t.amount_twd ?? local;
      return [
        td(fmtMonth(t.month)),
        td(fmtDate(t.date), "text-mute"),
        tdPill(t.account ?? "TWD"),
        td(t.category ?? ""),
        td(t.memo ?? t.summary ?? "", "text-mute"),
        td(t.ccy ?? "TWD", "text-mute"),
        td(num(local, 2), `num ${tone(local)}`),
        td(twd(t_twd), `num ${tone(t_twd)}`),
        td(num(t.balance, 2), "num text-mute"),
      ];
    },
  });
};

const paintCfCharts = (
  Chart: ChartCtor,
  cf: CashflowsCum,
  monthly: ReadonlyArray<MonthlyRow>,
): void => {
  // Real-vs-counterfactual curve. Two shapes supported:
  //   - cumulative: [{ month, real_curve, counterfactual }, ...]
  //   - real_curve / counterfactual_curve as parallel arrays
  let labels: string[] = [];
  let real: number[] = [];
  let cf2: number[] = [];
  if (cf.cumulative?.length) {
    labels = cf.cumulative.map((p) => p.month);
    real = cf.cumulative.map((p) => p.real_curve ?? 0);
    cf2 = cf.cumulative.map((p) => p.counterfactual ?? 0);
  } else if (cf.real_curve?.length) {
    labels = cf.real_curve.map((p) => p.month);
    real = cf.real_curve.map((p) => p.value);
    cf2 = (cf.counterfactual_curve ?? []).map((p) => p.value);
  }
  if (labels.length) {
    paintLine(Chart, "chart-cf", {
      labels,
      datasets: [
        {
          label: "Real (TWD)",
          data: real,
          color: cssVar("--accent") || palette()[0],
          fill: true,
        },
        {
          label: "Counterfactual",
          data: cf2,
          color: cssVar("--c2") || palette()[1],
          borderDash: [4, 4],
        },
      ],
    });
  } else {
    // Always paint chart-cf for the contract — empty series is acceptable.
    paintLine(Chart, "chart-cf", {
      labels: [""],
      datasets: [{ label: "Real (TWD)", data: [0], color: palette()[0] || "#888" }],
    });
  }

  paintBar(Chart, "chart-monthly", {
    labels: monthly.map((m) => m.month ?? ""),
    datasets: [
      {
        label: "Inflow",
        data: monthly.map((m) => m.inflow_twd ?? 0),
        color: cssVar("--c1") || palette()[0],
        stack: "flow",
      },
      {
        label: "Outflow",
        data: monthly.map((m) => -(m.outflow_twd ?? 0)),
        color: cssVar("--c3") || palette()[2],
        stack: "flow",
      },
    ],
  });
};

const showError = (outlet: HTMLElement, err: Error): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  outlet.appendChild(
    el("div", { class: "error-box" }, `Failed to load cashflows: ${err.message}`),
  );
};

export const mountCashflows = async (
  outlet: HTMLElement,
  deps: MountDeps,
): Promise<void> => {
  renderScaffold(outlet);
  try {
    const [cf, monthlyRaw, bank] = await Promise.all([
      deps.api.get<CashflowsCum>("/api/cashflows/cumulative"),
      deps.api.get<unknown>("/api/cashflows/monthly"),
      deps.api.get<BankRow[]>("/api/cashflows/bank"),
    ]);
    // Defensive unwrap: monthly may be a bare list or a dict with `monthly` key.
    const monthly: ReadonlyArray<MonthlyRow> = Array.isArray(monthlyRaw)
      ? (monthlyRaw as MonthlyRow[])
      : monthlyRaw && typeof monthlyRaw === "object" && Array.isArray((monthlyRaw as { monthly?: unknown[] }).monthly)
        ? ((monthlyRaw as { monthly: MonthlyRow[] }).monthly)
        : [];

    renderKpis(cf);
    renderBreakdown(cf.cumulative_flows ?? []);
    renderBank(bank ?? []);
    if (deps.Chart) paintCfCharts(deps.Chart, cf, monthly);
  } catch (err) {
    showError(outlet, err instanceof Error ? err : new Error(String(err)));
  }
};
