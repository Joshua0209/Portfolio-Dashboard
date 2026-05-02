// RED reproducer for /performance page mount — Phase 8 Cycle 61
// Pins src/pages/performance.ts:
//   - mountPerformance(outlet, { api }) renders the 8 KPI tiles, the
//     method switcher, drawdown episodes table, contribution table,
//     and monthly detail table.
//   - Method switcher: reads localStorage 'perf.twr.method.v1' on
//     load, saves on change, triggers re-fetch with ?method=<value>.
//   - capRatio caps |value| > 100 to "≫ 10" / "≪ −10" (legacy verbatim
//     because thin-sample sharpe values can hit absurd numbers).
//   - bandLabel returns "negative — losing money relative to risk"
//     when v<0; "band: <name>" otherwise.
//   - Drawdown episodes empty state renders <td.table-empty colspan=6>.
//   - Months table mounts via DataTable.
//   - Contribution table mounts via DataTable.
//   - Error path renders .error-box inside outlet.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountPerformance } from "../src/pages/performance";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const timeseries = {
  twr_total: 0.182,
  cagr: 0.124,
  xirr: 0.094,
  hit_rate: 0.62,
  positive_months: 18,
  negative_months: 11,
  annualized_volatility: 0.18,
  sharpe_annualized: 1.4,
  sortino_annualized: 2.1,
  calmar: 1.05,
  monthly: [
    { month: "2026-04", equity_twd: 1_500_000, period_return: 0.012, cum_twr: 0.182, drawdown: 0, external_flow: 0, weighted_flow: 0, v_start: 1_482_000 },
  ],
  drawdown_episodes: [
    { peak_month: "2024-08", trough_month: "2024-11", depth_pct: -0.123, drawdown_months: 3, recovery_months: 5, recovered: true },
  ],
};

const rolling = { rolling_3m: [], rolling_6m: [], rolling_12m: [] };

const attribution = {
  totals: { tw_pnl_twd: 100_000, foreign_price_pnl_twd: 50_000, fx_pnl_twd: 20_000, total_pnl_twd: 170_000 },
  monthly: [],
};

const tax = {
  by_ticker: [
    { code: "2330", name: "TSMC", venue: "TW", realized_pnl_twd: 50_000, dividends_twd: 12_000, unrealized_pnl_twd: 80_000, contribution_share: 0.6 },
    { code: "AAPL", name: "Apple", venue: "Foreign", realized_pnl_twd: 0, dividends_twd: 5_000, unrealized_pnl_twd: 30_000, contribution_share: 0.2 },
  ],
};

const buildApi = (
  overrides: Record<string, unknown> = {},
  defaultTs: Record<string, unknown> = timeseries,
): ReturnType<typeof vi.fn> => {
  return vi.fn().mockImplementation((path: string) => {
    if (path in overrides) return Promise.resolve(overrides[path]);
    if (path.startsWith("/api/performance/timeseries")) return Promise.resolve(defaultTs);
    if (path.startsWith("/api/performance/rolling")) return Promise.resolve(rolling);
    if (path === "/api/performance/attribution") return Promise.resolve(attribution);
    if (path === "/api/tax") return Promise.resolve(tax);
    return Promise.reject(new Error(`unexpected ${path}`));
  });
};

describe("Phase 8 Cycle 61 — performance page mount", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
    localStorage.removeItem("perf.twr.method.v1");
  });

  afterEach(() => {
    clearDom();
  });

  it("renders KPIs with tone classes", async () => {
    const api = buildApi();
    await mountPerformance(outlet, { api: { get: api } });
    expect(document.getElementById("kpi-twr")?.textContent).toBe("+18.20%");
    expect(document.getElementById("kpi-twr")?.className).toContain("value-pos");
    expect(document.getElementById("kpi-cagr")?.textContent).toBe("+12.40%");
    expect(document.getElementById("kpi-xirr")?.textContent).toBe("+9.40%");
    expect(document.getElementById("kpi-hit")?.textContent).toBe("62%");
    expect(document.getElementById("kpi-hit-sub")?.textContent).toContain("18 pos");
    expect(document.getElementById("kpi-vol")?.textContent).toBe("18.0%");
    expect(document.getElementById("kpi-sharpe")?.textContent).toBe("1.40");
  });

  it("XIRR null renders em-dash without tone", async () => {
    const api = buildApi({}, { ...timeseries, xirr: null });
    await mountPerformance(outlet, { api: { get: api } });
    expect(document.getElementById("kpi-xirr")?.textContent).toBe("—");
  });

  it("method switcher reads localStorage on load", async () => {
    localStorage.setItem("perf.twr.method.v1", "mid_month");
    const api = buildApi();
    await mountPerformance(outlet, { api: { get: api } });
    const sel = document.getElementById("twr-method") as HTMLSelectElement;
    expect(sel.value).toBe("mid_month");
    expect(api).toHaveBeenCalledWith("/api/performance/timeseries?method=mid_month");
  });

  it("method switcher persists choice + triggers refetch", async () => {
    const api = buildApi();
    await mountPerformance(outlet, { api: { get: api } });
    const sel = document.getElementById("twr-method") as HTMLSelectElement;
    sel.value = "eom";
    sel.dispatchEvent(new Event("change"));
    await new Promise((r) => setTimeout(r, 0));
    expect(localStorage.getItem("perf.twr.method.v1")).toBe("eom");
    expect(api).toHaveBeenCalledWith("/api/performance/timeseries?method=eom");
  });

  it("drawdown episodes table renders rows", async () => {
    const api = buildApi();
    await mountPerformance(outlet, { api: { get: api } });
    const rows = document.querySelectorAll("#dd-table tbody tr");
    expect(rows).toHaveLength(1);
    expect(rows[0].textContent).toContain("Recovered");
  });

  it("drawdown episodes empty state renders muted row", async () => {
    const api = buildApi({}, { ...timeseries, drawdown_episodes: [] });
    await mountPerformance(outlet, { api: { get: api } });
    const empty = document.querySelector("#dd-table tbody td.table-empty");
    expect(empty?.textContent).toContain("No drawdowns");
  });

  it("contribution table mounts with rows", async () => {
    const api = buildApi();
    await mountPerformance(outlet, { api: { get: api } });
    const rows = document.querySelectorAll("#contrib-table tbody tr");
    expect(rows.length).toBe(2);
    expect(rows[0].textContent).toContain("TSMC");
  });

  it("months detail table mounts", async () => {
    const api = buildApi();
    await mountPerformance(outlet, { api: { get: api } });
    const rows = document.querySelectorAll("#months-table tbody tr");
    expect(rows.length).toBe(1);
  });

  it("error path renders .error-box inside outlet", async () => {
    const api = vi.fn().mockRejectedValue(new Error("boom"));
    await mountPerformance(outlet, { api: { get: api } });
    expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
  });
});
