// RED reproducer for /tax /ticker/<code> /benchmark — Phase 8 Cycle 65
// Closes out the page-port phase with the last three pages.
//
// Pins:
//   tax: 7-tile KPI grid (with rebate-aware math), winners/losers
//        bar lists, tax-table via DataTable + 4-option status filter
//        with predicate, export-tax CSV.
//   ticker: per-code drill-down — pulls /api/tickers/<code>, renders
//        header + KPIs, position+pnl chart canvas slots, trades and
//        dividends DataTables. Empty state when API errors or returns
//        {error: ...}.
//   benchmark: strategy checkboxes (TW + US), persisted in localStorage,
//        triggers /api/benchmarks/compare with comma-joined keys.
//        Stats table renders one row per portfolio + strategy with
//        an excess-return column (em-dash for the portfolio row).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountTax } from "../src/pages/tax";
import { mountTicker } from "../src/pages/ticker";
import { mountBenchmark } from "../src/pages/benchmark";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const taxResp = {
  totals: {
    realized_pnl_twd: 200_000,
    rebate_twd: 5_000,
    closed_positions: 12,
    dividends_twd: 35_000,
    unrealized_pnl_twd: 150_000,
    total_pnl_twd: 380_000,
    win_rate: 0.65,
    winners_count: 8,
    losers_count: 4,
    avg_holding_days: 180,
    fees_twd: -8000,
    tax_twd: -2000,
    net_cost_twd: -5000,
  },
  by_ticker: [
    {
      code: "2330", name: "TSMC", venue: "TW",
      sell_qty: 100, open_qty: 200,
      cost_of_sold_twd: 50000, sell_proceeds_twd: 70000,
      realized_pnl_twd: 20000, dividends_twd: 5000,
      unrealized_pnl_twd: 80000, total_pnl_twd: 105000,
      win_rate: 0.7, avg_holding_days: 90, fully_closed: false,
    },
    {
      code: "AAPL", name: "Apple", venue: "Foreign",
      sell_qty: 50, open_qty: 0,
      cost_of_sold_twd: 100000, sell_proceeds_twd: 150000,
      realized_pnl_twd: 50000, dividends_twd: 1000,
      unrealized_pnl_twd: 0, total_pnl_twd: 51000,
      win_rate: 1.0, avg_holding_days: 365, fully_closed: true,
    },
  ],
};

const tickerResp = {
  name: "TSMC",
  is_open: true,
  last_seen_month: null,
  current: { qty: 1000, avg_cost: 800, cost_twd: 800_000, unrealized_pnl_twd: 200_000 },
  summary: { realized_pnl_twd: 20_000, realized_pnl_pct: 0.05 },
  position_history: [
    { month: "2026-04", qty: 1000, ref_price: 1000, cost_twd: 800_000, mkt_value_twd: 1_000_000 },
  ],
  trades: [
    { date: "2026/04/15", side: "現買", qty: 100, price: 1000, gross_twd: 100_000, fee_twd: -150, net_twd: -100_150 },
  ],
  dividends: [
    { month: "2026-04", date: "2026/04/05", ccy: "TWD", amount_local: 5_000, amount_twd: 5_000 },
  ],
};

const benchStrategies = [
  { key: "tw_passive", market: "TW", name: "TW Passive", description: "0050+0056" },
  { key: "us_passive", market: "US", name: "US Passive", description: "VOO" },
  { key: "tw_div", market: "TW", name: "TW Dividend", description: "0056" },
];

const benchCompare = {
  portfolio: {
    name: "Portfolio",
    stats: { twr_total: 0.18, annualized_volatility: 0.16, max_drawdown: -0.08, sharpe: 1.2, sortino: 1.8 },
    curve: [{ cum_return: 0.18 }],
  },
  strategies: [
    {
      key: "tw_passive", name: "TW Passive",
      stats: { twr_total: 0.10, annualized_volatility: 0.14, max_drawdown: -0.10, sharpe: 0.8, sortino: 1.0 },
      curve: [{ cum_return: 0.10 }],
    },
  ],
  months: ["2026-04"],
};

describe("Phase 8 Cycle 65 — tax/ticker/benchmark", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
    localStorage.removeItem("benchmark.selected.v1");
  });

  afterEach(() => clearDom());

  describe("tax", () => {
    const buildApi = (): ReturnType<typeof vi.fn> =>
      vi.fn().mockResolvedValue(taxResp);

    it("renders rebate-aware KPIs", async () => {
      await mountTax(outlet, { api: { get: buildApi() } });
      // realized + rebate = 200000 + 5000
      expect(document.getElementById("kpi-real")?.textContent).toBe("NT$205,000");
      // total + rebate = 380000 + 5000
      expect(document.getElementById("kpi-total")?.textContent).toBe("NT$385,000");
      expect(document.getElementById("kpi-real-sub")?.textContent).toContain("12 closed");
      expect(document.getElementById("kpi-real-sub")?.textContent).toContain("rebates");
      expect(document.getElementById("kpi-win")?.textContent).toBe("65.0%");
      expect(document.getElementById("kpi-hold")?.textContent).toBe("180d");
    });

    it("status=closed filter hides open positions", async () => {
      await mountTax(outlet, { api: { get: buildApi() } });
      const sel = document.querySelectorAll<HTMLSelectElement>(".dt-filter")[0];
      sel.value = "closed";
      sel.dispatchEvent(new Event("change"));
      const rows = document.querySelectorAll("#tax-table tbody tr");
      expect(rows.length).toBe(1);
      expect(rows[0].textContent).toContain("AAPL");
    });

    it("CSV export wires through downloadBlob", async () => {
      const downloadBlob = vi.fn();
      await mountTax(outlet, { api: { get: buildApi() }, downloadBlob });
      (document.getElementById("export-tax") as HTMLButtonElement).click();
      expect(downloadBlob).toHaveBeenCalled();
      const [content, filename] = downloadBlob.mock.calls[0];
      expect(filename).toMatch(/^tax-pnl-\d{4}-\d{2}-\d{2}\.csv$/);
      expect(content).toContain("code,name,venue");
    });

    it("error renders .error-box", async () => {
      const api = vi.fn().mockRejectedValue(new Error("boom"));
      await mountTax(outlet, { api: { get: api } });
      expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
    });
  });

  describe("ticker", () => {
    it("renders header + KPIs + tables", async () => {
      const api = vi.fn().mockResolvedValue(tickerResp);
      await mountTicker(outlet, { api: { get: api }, code: "2330" });
      expect(document.getElementById("ticker-name")?.textContent).toBe("TSMC");
      expect(document.getElementById("kpi-qty")?.textContent).toBe("1,000");
      expect(document.getElementById("kpi-status")?.textContent).toBe("Open position");
      expect(document.getElementById("kpi-avg")?.textContent).toBe("800.00");
      expect(document.querySelectorAll("#trade-table tbody tr").length).toBe(1);
      expect(document.querySelectorAll("#div-table tbody tr").length).toBe(1);
    });

    it("api {error: ...} renders empty state", async () => {
      const api = vi.fn().mockResolvedValue({ error: "Not found" });
      await mountTicker(outlet, { api: { get: api }, code: "ZZZ" });
      const empty = outlet.querySelector(".empty-state");
      expect(empty?.textContent).toContain("ZZZ");
    });

    it("api rejection renders empty state", async () => {
      const api = vi.fn().mockRejectedValue(new Error("404"));
      await mountTicker(outlet, { api: { get: api }, code: "FAIL" });
      expect(outlet.querySelector(".empty-state")).not.toBeNull();
    });
  });

  describe("benchmark", () => {
    const buildApi = (): ReturnType<typeof vi.fn> =>
      vi.fn().mockImplementation((path: string) => {
        if (path === "/api/benchmarks/strategies") return Promise.resolve(benchStrategies);
        if (path.startsWith("/api/benchmarks/compare")) return Promise.resolve(benchCompare);
        return Promise.reject(new Error(`unexpected ${path}`));
      });

    it("renders strategy checkboxes with default selection", async () => {
      const api = buildApi();
      await mountBenchmark(outlet, { api: { get: api } });
      const tw = document.querySelectorAll<HTMLInputElement>("#strategy-list-tw input");
      const us = document.querySelectorAll<HTMLInputElement>("#strategy-list-us input");
      expect(tw.length).toBe(2);
      expect(us.length).toBe(1);
      // Default selection: tw_passive + us_passive
      const checked = Array.from(document.querySelectorAll<HTMLInputElement>("input[type=checkbox]:checked"))
        .map((cb) => cb.value);
      expect(checked).toEqual(expect.arrayContaining(["tw_passive", "us_passive"]));
    });

    it("toggling checkbox persists to localStorage and triggers refetch", async () => {
      const api = buildApi();
      await mountBenchmark(outlet, { api: { get: api } });
      const cbTwDiv = document.querySelector<HTMLInputElement>(
        "#strategy-list-tw input[value=tw_div]",
      )!;
      cbTwDiv.checked = true;
      cbTwDiv.dispatchEvent(new Event("change"));
      await new Promise((r) => setTimeout(r, 0));
      const stored = JSON.parse(localStorage.getItem("benchmark.selected.v1") ?? "[]");
      expect(stored).toContain("tw_div");
      // compare endpoint called at least twice (initial + post-toggle)
      const calls = (api as ReturnType<typeof vi.fn>).mock.calls.filter(
        ([p]) => typeof p === "string" && p.startsWith("/api/benchmarks/compare"),
      );
      expect(calls.length).toBeGreaterThanOrEqual(2);
    });

    it("stats-table renders portfolio + strategy with em-dash excess on portfolio row", async () => {
      const api = buildApi();
      await mountBenchmark(outlet, { api: { get: api } });
      const rows = document.querySelectorAll("#stats-table tbody tr");
      expect(rows.length).toBe(2);
      const portfolioRow = rows[0];
      const lastCell = portfolioRow.querySelectorAll("td")[6];
      expect(lastCell?.textContent).toBe("—");
    });

    it("error renders .error-box", async () => {
      const api = vi.fn().mockRejectedValue(new Error("boom"));
      await mountBenchmark(outlet, { api: { get: api } });
      expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
    });
  });
});
