// RED reproducer for Phase 8 Cycle 66 — Chart.js wiring sweep.
//
// Pins the paint contract for every canvas slot across the 12-page
// dashboard. Each page mount, when handed a `Chart` constructor via
// deps.Chart, must call `new Chart(canvas, config)` exactly once per
// canvas slot with the right `config.type` and the right dataset shape.
//
// When deps.Chart is undefined, paint is a silent no-op — preserves the
// legacy `if (!global.Chart) return;` guard in static/js/charts.js so
// existing scaffold tests keep asserting non-chart DOM behavior without
// pulling in the chart.js bundle.
//
// We pass a fake Chart constructor that records its (canvas, config)
// arguments. That's the test surface — happy-dom can't render to a
// real CanvasRenderingContext2D anyway, so the value of this test is
// the configuration that arrives at `new Chart(...)`, not the pixels
// it would produce.

import { afterEach, describe, expect, it, vi } from "vitest";
import { mountOverview } from "../src/pages/overview";
import { mountToday } from "../src/pages/today";
import { mountHoldings } from "../src/pages/holdings";
import { mountPerformance } from "../src/pages/performance";
import { mountRisk } from "../src/pages/risk";
import { mountFx } from "../src/pages/fx";
import { mountTransactions } from "../src/pages/transactions";
import { mountCashflows } from "../src/pages/cashflows";
import { mountDividends } from "../src/pages/dividends";
import { mountTicker } from "../src/pages/ticker";
import { mountBenchmark } from "../src/pages/benchmark";

interface ChartCall {
  canvas: HTMLCanvasElement;
  config: {
    type: string;
    data?: { labels?: unknown[]; datasets?: Array<Record<string, unknown>> };
    options?: Record<string, unknown>;
  };
}

const makeFakeChart = (): {
  Chart: ReturnType<typeof vi.fn>;
  calls: ChartCall[];
} => {
  const calls: ChartCall[] = [];
  const Chart = vi.fn().mockImplementation((target: unknown, config: unknown) => {
    // Pages may pass canvas DOM node OR canvas.getContext('2d') as the
    // target. Normalize so the test can compare against the actual <canvas>.
    let canvas: HTMLCanvasElement;
    if (target instanceof HTMLCanvasElement) {
      canvas = target;
    } else if (
      target &&
      typeof target === "object" &&
      "canvas" in target &&
      target.canvas instanceof HTMLCanvasElement
    ) {
      canvas = target.canvas as HTMLCanvasElement;
    } else {
      throw new Error("fake Chart got unexpected target");
    }
    calls.push({ canvas, config: config as ChartCall["config"] });
    return { destroy: vi.fn() };
  });
  return { Chart, calls };
};

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const setupOutlet = (): HTMLElement => {
  clearDom();
  const outlet = document.createElement("div");
  outlet.id = "page";
  document.body.appendChild(outlet);
  return outlet;
};

const findChart = (calls: ChartCall[], canvasId: string): ChartCall | undefined =>
  calls.find((c) => c.canvas.id === canvasId);

describe("Phase 8 Cycle 66 — Chart.js wiring across all pages", () => {
  afterEach(() => clearDom());

  describe("paint is a silent no-op when deps.Chart is undefined", () => {
    it("mountOverview without Chart still renders DOM and does not throw", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/summary") {
          return Promise.resolve({
            kpis: { real_now_twd: 0, fx_usd_twd: 0, as_of: "2026-04" },
            profit_twd: 0, invested_twd: 0, twr: 0, xirr: 0,
            first_month: "2024-01", last_month: "2026-04", months_covered: 28,
            equity_curve: [{ month: "2024-01", equity_twd: 100, cum_twr: 0 }],
            allocation: { tw: 1, foreign: 0, bank_twd: 0, bank_usd: 0 },
          });
        }
        if (path === "/api/holdings/current") return Promise.resolve({ holdings: [] });
        if (path === "/api/transactions") return Promise.resolve([]);
        return Promise.reject(new Error("unexpected"));
      });
      await mountOverview(outlet, { api: { get: apiGet } });
      expect(document.getElementById("chart-equity")).toBeTruthy();
      expect(document.getElementById("chart-alloc")).toBeTruthy();
    });
  });

  describe("overview", () => {
    it("paints chart-equity (line, dual-axis) and chart-alloc (doughnut)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/summary") {
          return Promise.resolve({
            kpis: { real_now_twd: 1_000_000, fx_usd_twd: 31.5, as_of: "2026-04" },
            profit_twd: 50_000, invested_twd: 1_000_000, twr: 0.05, xirr: 0.07,
            first_month: "2024-01", last_month: "2026-04", months_covered: 28,
            equity_curve: [
              { month: "2024-01", equity_twd: 100, cum_twr: 0 },
              { month: "2024-02", equity_twd: 110, cum_twr: 0.1 },
            ],
            allocation: { tw: 600, foreign: 400, bank_twd: 200, bank_usd: 50 },
          });
        }
        if (path === "/api/holdings/current") return Promise.resolve({ holdings: [] });
        if (path === "/api/transactions") return Promise.resolve([]);
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountOverview(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(2);
      const equity = findChart(calls, "chart-equity");
      expect(equity?.config.type).toBe("line");
      expect(equity?.config.data?.datasets?.length).toBe(2);
      const alloc = findChart(calls, "chart-alloc");
      expect(alloc?.config.type).toBe("doughnut");
      // 4 segments — tw, foreign, bank_twd, bank_usd
      expect((alloc?.config.data?.datasets?.[0]?.data as number[])?.length).toBe(4);
    });
  });

  describe("today", () => {
    it("paints equity-sparkline (line) and dd-chart (line)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/today/snapshot") {
          return Promise.resolve({
            data_date: "2026-05-01", weekday: "Friday", today_in_tpe: "2026-05-01",
            equity_twd: 1500000, delta_twd: 100, delta_pct: 0.5,
            n_positions: 10, fx_usd_twd: 32.0,
          });
        }
        if (path === "/api/today/movers") return Promise.resolve({ movers: [] });
        if (path === "/api/today/sparkline") {
          return Promise.resolve({
            points: [
              { date: "2026-04-25", equity_twd: 1480000 },
              { date: "2026-05-01", equity_twd: 1500000 },
            ],
          });
        }
        if (path === "/api/today/freshness") {
          return Promise.resolve({ data_date: "2026-05-01", band: "green", stale_days: 0 });
        }
        if (path === "/api/today/period-returns") return Promise.resolve({ windows: [] });
        if (path === "/api/today/drawdown") {
          return Promise.resolve({
            empty: false, current_dd: -3, max_dd: -10, max_dd_date: "2024-08-05",
            points: [
              { date: "2026-04-25", drawdown_pct: -2 },
              { date: "2026-05-01", drawdown_pct: -3 },
            ],
          });
        }
        if (path === "/api/today/risk-metrics") return Promise.resolve({});
        if (path === "/api/today/calendar") return Promise.resolve({ empty: true });
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountToday(outlet, {
        api: { get: apiGet },
        fetchJson: vi.fn(),
        Chart,
      });
      expect(Chart).toHaveBeenCalledTimes(2);
      expect(findChart(calls, "equity-sparkline")?.config.type).toBe("line");
      expect(findChart(calls, "dd-chart")?.config.type).toBe("line");
    });
  });

  describe("holdings", () => {
    it("paints treemap when holdings exist", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/holdings/current") {
          return Promise.resolve({
            holdings: [
              { code: "2330", name: "TSMC", mkt_value_twd: 600000, unrealized_pct: 0.1, weight: 0.6 },
              { code: "0050", name: "ETF50", mkt_value_twd: 400000, unrealized_pct: 0.05, weight: 0.4 },
            ],
          });
        }
        if (path === "/api/holdings/sectors") {
          return Promise.resolve([{ sector: "Tech", weight_pct: 1.0, market_value_twd: 1000000 }]);
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountHoldings(outlet, { api: { get: apiGet }, Chart });
      const tm = findChart(calls, "treemap");
      expect(tm?.config.type).toBe("treemap");
    });
  });

  describe("performance", () => {
    it("paints 7 canvases (cum + monthly + dd + rolling + attr + attr-totals + treemap)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path.startsWith("/api/performance/timeseries")) {
          return Promise.resolve({
            timeseries: [
              { month: "2024-01", twr_pct: 1, cum_twr_pct: 1, drawdown_pct: 0, equity_twd: 100 },
              { month: "2024-02", twr_pct: 2, cum_twr_pct: 3, drawdown_pct: -1, equity_twd: 110 },
            ],
          });
        }
        if (path.startsWith("/api/performance/rolling")) {
          return Promise.resolve({
            rolling: [
              { month: "2024-12", r3m: 1, r6m: 2, r12m: 5 },
              { month: "2025-01", r3m: 1.5, r6m: 2.5, r12m: 6 },
            ],
          });
        }
        if (path === "/api/performance/attribution") {
          return Promise.resolve({
            monthly: [{ month: "2024-01", tw_twd: 10, foreign_twd: 5, fx_twd: 1 }],
            totals: { tw_twd: 100, foreign_twd: 50, fx_twd: 10 },
            episodes: [],
            contributions: [
              { code: "2330", name: "TSMC", contribution_twd: 80 },
              { code: "0050", name: "ETF50", contribution_twd: 20 },
            ],
          });
        }
        if (path === "/api/tax") {
          return Promise.resolve({
            by_ticker: [
              { code: "2330", name: "TSMC", total_pnl_twd: 80, contribution_share: 0.8 },
              { code: "0050", name: "ETF50", total_pnl_twd: 20, contribution_share: 0.2 },
            ],
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountPerformance(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(7);
      expect(findChart(calls, "chart-cum")?.config.type).toBe("line");
      expect(findChart(calls, "chart-monthly")?.config.type).toBe("bar");
      expect(findChart(calls, "chart-dd")?.config.type).toBe("line");
      expect(findChart(calls, "chart-rolling")?.config.type).toBe("line");
      expect(findChart(calls, "chart-attr")?.config.type).toBe("line");
      expect(findChart(calls, "chart-attr-totals")?.config.type).toBe("bar");
      expect(findChart(calls, "contrib-treemap")?.config.type).toBe("treemap");
    });
  });

  describe("risk", () => {
    it("paints chart-dd (line), chart-weights (bar), chart-leverage (bar)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/risk") {
          return Promise.resolve({
            drawdown_curve: [
              { month: "2024-01", drawdown_pct: 0, equity_twd: 100 },
              { month: "2024-02", drawdown_pct: -1, equity_twd: 99 },
            ],
            weight_distribution: [
              { code: "2330", name: "TSMC", weight_pct: 0.5 },
              { code: "0050", name: "ETF50", weight_pct: 0.3 },
            ],
            leverage: { gross_exposure_twd: 100, equity_twd: 80, leverage_ratio: 1.25 },
            concentration: { hhi: 0.4, top5_pct: 0.85, top10_pct: 0.95 },
            ratios: { sharpe: 1.0, sortino: 1.2, calmar: 0.5 },
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountRisk(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(3);
      expect(findChart(calls, "chart-dd")?.config.type).toBe("line");
      expect(findChart(calls, "chart-weights")?.config.type).toBe("bar");
      expect(findChart(calls, "chart-leverage")?.config.type).toBe("bar");
    });
  });

  describe("fx", () => {
    it("paints chart-rate (line), chart-fx-pnl (bar), chart-ccy (bar)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/fx") {
          return Promise.resolve({
            rate_curve: [
              { month: "2024-01", usd_twd: 31.0 },
              { month: "2024-02", usd_twd: 31.2 },
            ],
            fx_pnl_monthly: [{ month: "2024-01", fx_pnl_twd: 100 }],
            currency_exposure: [
              { currency: "TWD", value_twd: 600 },
              { currency: "USD", value_twd: 400 },
            ],
            fx_pnl_total_twd: 100,
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountFx(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(3);
      expect(findChart(calls, "chart-rate")?.config.type).toBe("line");
      expect(findChart(calls, "chart-fx-pnl")?.config.type).toBe("bar");
      expect(findChart(calls, "chart-ccy")?.config.type).toBe("bar");
    });
  });

  describe("transactions", () => {
    it("paints chart-volume (bar) and chart-fees (bar)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/transactions") return Promise.resolve([]);
        if (path === "/api/transactions/aggregates") {
          return Promise.resolve({
            monthly: [
              { month: "2024-01", buy_twd: 100, sell_twd: 50, net_twd: -50, fees_twd: 5, tax_twd: 1, rebate_twd: 0 },
            ],
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountTransactions(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(2);
      expect(findChart(calls, "chart-volume")?.config.type).toBe("bar");
      expect(findChart(calls, "chart-fees")?.config.type).toBe("bar");
    });
  });

  describe("cashflows", () => {
    it("paints chart-cf (line) and chart-monthly (bar)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/cashflows/cumulative") {
          return Promise.resolve({
            cumulative: [
              { month: "2024-01", real_curve: 100, counterfactual: 110 },
              { month: "2024-02", real_curve: 110, counterfactual: 121 },
            ],
          });
        }
        if (path === "/api/cashflows/monthly") {
          return Promise.resolve({
            monthly: [
              { month: "2024-01", inflow_twd: 100, outflow_twd: 0, net_twd: 100 },
            ],
          });
        }
        if (path === "/api/cashflows/bank") return Promise.resolve([]);
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountCashflows(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(2);
      expect(findChart(calls, "chart-cf")?.config.type).toBe("line");
      expect(findChart(calls, "chart-monthly")?.config.type).toBe("bar");
    });
  });

  describe("dividends", () => {
    it("paints chart-monthly (bar)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/dividends") {
          return Promise.resolve({
            monthly: [
              { month: "2024-01", twd_amount: 1000 },
              { month: "2024-02", twd_amount: 2000 },
            ],
            top_payers: [],
            events: [],
            kpis: {
              total_twd: 3000, ttm_twd: 3000,
              annualized_yield_pct: 0.04, distinct_payers: 1, n_events: 2,
            },
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountDividends(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(1);
      expect(findChart(calls, "chart-monthly")?.config.type).toBe("bar");
    });
  });

  describe("ticker", () => {
    it("paints chart-daily, chart-pos, chart-pnl (all line)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path.startsWith("/api/tickers/2330")) {
          return Promise.resolve({
            code: "2330", name: "TSMC",
            history: [
              { month: "2024-01", qty: 100, cost_twd: 50000, market_value_twd: 60000 },
              { month: "2024-02", qty: 100, cost_twd: 50000, market_value_twd: 65000 },
            ],
            daily_prices: [
              { date: "2026-04-25", close: 580 },
              { date: "2026-05-01", close: 585 },
            ],
            trades: [], dividends: [], kpis: {},
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountTicker(outlet, { api: { get: apiGet }, code: "2330", Chart });
      expect(Chart).toHaveBeenCalledTimes(3);
      expect(findChart(calls, "chart-daily")?.config.type).toBe("line");
      expect(findChart(calls, "chart-pos")?.config.type).toBe("line");
      expect(findChart(calls, "chart-pnl")?.config.type).toBe("line");
    });
  });

  describe("benchmark", () => {
    it("paints chart-cum (line), chart-monthly (bar), chart-scatter (scatter)", async () => {
      const outlet = setupOutlet();
      const apiGet = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/benchmarks/strategies") {
          return Promise.resolve({
            strategies: [
              { key: "tw_passive", name: "TW Passive", venue: "TW" },
              { key: "us_passive", name: "US Passive", venue: "US" },
            ],
          });
        }
        if (path.startsWith("/api/benchmarks/compare")) {
          return Promise.resolve({
            portfolio: {
              monthly: [
                { month: "2024-01", twr_pct: 1, cum_twr_pct: 1 },
                { month: "2024-02", twr_pct: 2, cum_twr_pct: 3 },
              ],
              cagr: 0.05, vol: 0.12, sharpe: 0.4,
            },
            strategies: [
              {
                key: "tw_passive", name: "TW Passive",
                monthly: [
                  { month: "2024-01", twr_pct: 0.5, cum_twr_pct: 0.5 },
                  { month: "2024-02", twr_pct: 1, cum_twr_pct: 1.5 },
                ],
                cagr: 0.04, vol: 0.10, sharpe: 0.3,
              },
            ],
          });
        }
        return Promise.reject(new Error("unexpected"));
      });
      const { Chart, calls } = makeFakeChart();
      await mountBenchmark(outlet, { api: { get: apiGet }, Chart });
      expect(Chart).toHaveBeenCalledTimes(3);
      expect(findChart(calls, "chart-cum")?.config.type).toBe("line");
      expect(findChart(calls, "chart-monthly")?.config.type).toBe("bar");
      expect(findChart(calls, "chart-scatter")?.config.type).toBe("scatter");
    });
  });
});
