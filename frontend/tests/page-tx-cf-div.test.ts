// RED reproducer for /transactions /cashflows /dividends — Cycle 64
// Three table-heavy pages share the KPI grid + DataTable + error-box
// pattern from holdings. This bundles all three since the contracts
// are tightly analogous.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountTransactions } from "../src/pages/transactions";
import { mountCashflows } from "../src/pages/cashflows";
import { mountDividends } from "../src/pages/dividends";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const txs = [
  {
    date: "2026/04/15", month: "2026-04", venue: "TW", side: "現買",
    code: "2330", name: "TSMC", qty: 100, price: 1000, ccy: "TWD",
    gross_twd: 100_000, fee_twd: -150, tax_twd: -300, net_twd: -100_450,
  },
  {
    date: "2026/04/20", month: "2026-04", venue: "Foreign", side: "現賣",
    code: "AAPL", name: "Apple", qty: 50, price: 200, ccy: "USD",
    gross_twd: 315_000, fee_twd: -100, tax_twd: 0, net_twd: 314_900,
  },
];

const txAggs = {
  totals: { trades: 2, buy_twd: 100_000, sell_twd: 315_000, fees_twd: -250, tax_twd: -300, rebate_twd: 0, fee_drag_pct: 0.0006 },
  monthly: [{ month: "2026-04", TW_buy: 100_000, TW_sell: 0 }],
  venues: ["TW", "Foreign"],
};

const cfData = {
  real_now_twd: 2_000_000,
  counterfactual_twd: 1_500_000,
  profit_twd: 500_000,
  real_curve: [{ month: "2026-04", value: 2_000_000 }],
  counterfactual_curve: [{ month: "2026-04", value: 1_500_000 }],
  cumulative_flows: [{ label: "Deposits", value: 1_500_000 }],
};

const cfMonthly = [
  { month: "2026-04", external_flow: 50_000, dividends_twd: 12_000, fees_twd: -250 },
];

const cfBank = [
  { date: "2026-04-15", month: "2026-04", account: "TWD", category: "deposit", memo: "salary", ccy: "TWD", amount: 50_000, amount_twd: 50_000, balance: 200_000, signed_amount: 50_000 },
];

const dividends = {
  total_twd: 250_000,
  totals_by_ccy: { TWD: 230_000, USD: 645 },
  count: 18,
  yields: { ttm_dividend_twd: 80_000, ttm_yield_on_cost: 0.045, annualized_yield_on_cost: 0.04 },
  monthly: [{ month: "2026-04", tw_twd: 12_000, foreign_twd: 3_000 }],
  by_ticker: [
    { code: "2330", name: "TSMC", total_twd: 30_000, count: 4 },
  ],
  holdings_total_return: [
    { code: "2330", name: "TSMC", venue: "TW", cost_twd: 800_000, mkt_value_twd: 1_000_000, cum_dividend_twd: 30_000, unrealized_pnl_with_div_twd: 230_000 },
  ],
  rebates: [],
  rows: [
    { date: "2026-04-12", code: "2330", name: "TSMC", amount_twd: 8_000, ccy: "TWD" },
  ],
};

describe("Phase 8 Cycle 64 — transactions/cashflows/dividends", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
  });

  afterEach(() => clearDom());

  describe("transactions", () => {
    const buildApi = (): ReturnType<typeof vi.fn> =>
      vi.fn().mockImplementation((path: string) => {
        if (path === "/api/transactions") return Promise.resolve(txs);
        if (path === "/api/transactions/aggregates") return Promise.resolve(txAggs);
        return Promise.reject(new Error(`unexpected ${path}`));
      });

    it("renders KPIs + transactions table", async () => {
      const api = buildApi();
      await mountTransactions(outlet, { api: { get: api } });
      expect(document.getElementById("kpi-n")?.textContent).toBe("2");
      expect(document.getElementById("kpi-buy")?.textContent).toBe("NT$100,000");
      expect(document.getElementById("kpi-sell")?.textContent).toBe("NT$315,000");
      const rows = document.querySelectorAll("#tx-table tbody tr");
      expect(rows.length).toBe(2);
    });

    it("CSV export wires through downloadBlob with tx- prefix", async () => {
      const api = buildApi();
      const downloadBlob = vi.fn();
      await mountTransactions(outlet, { api: { get: api }, downloadBlob });
      (document.getElementById("export-tx") as HTMLButtonElement).click();
      expect(downloadBlob).toHaveBeenCalledTimes(1);
      const [content, filename] = downloadBlob.mock.calls[0];
      expect(filename).toMatch(/^transactions-\d{4}-\d{2}-\d{2}\.csv$/);
      expect(content).toContain("month,date,venue,side");
    });

    it("error path renders .error-box", async () => {
      const api = vi.fn().mockRejectedValue(new Error("boom"));
      await mountTransactions(outlet, { api: { get: api } });
      expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
    });
  });

  describe("cashflows", () => {
    const buildApi = (): ReturnType<typeof vi.fn> =>
      vi.fn().mockImplementation((path: string) => {
        if (path === "/api/cashflows/cumulative") return Promise.resolve(cfData);
        if (path === "/api/cashflows/monthly") return Promise.resolve(cfMonthly);
        if (path === "/api/cashflows/bank") return Promise.resolve(cfBank);
        return Promise.reject(new Error(`unexpected ${path}`));
      });

    it("renders KPIs from /api/cashflows/cumulative", async () => {
      const api = buildApi();
      await mountCashflows(outlet, { api: { get: api } });
      expect(document.getElementById("kpi-real")?.textContent).toBe("NT$2,000,000");
      expect(document.getElementById("kpi-cf")?.textContent).toBe("NT$1,500,000");
      const profit = document.getElementById("kpi-profit");
      expect(profit?.textContent).toBe("NT$500,000");
      expect(profit?.className).toContain("value-pos");
    });

    it("bank table renders rows via DataTable", async () => {
      const api = buildApi();
      await mountCashflows(outlet, { api: { get: api } });
      const rows = document.querySelectorAll("#bank-table tbody tr");
      expect(rows.length).toBe(1);
      expect(rows[0].textContent).toContain("salary");
    });

    it("monthly raw can be {monthly: [...]} shape (defensive unwrap)", async () => {
      const api = vi.fn().mockImplementation((path: string) => {
        if (path === "/api/cashflows/cumulative") return Promise.resolve(cfData);
        if (path === "/api/cashflows/monthly") return Promise.resolve({ monthly: cfMonthly });
        if (path === "/api/cashflows/bank") return Promise.resolve(cfBank);
        return Promise.reject(new Error(`unexpected ${path}`));
      });
      await mountCashflows(outlet, { api: { get: api } });
      // Must not throw — defensive unwrap covers both bare-list and dict shapes
      expect(outlet.querySelector(".error-box")).toBeNull();
    });

    it("error path renders .error-box", async () => {
      const api = vi.fn().mockRejectedValue(new Error("boom"));
      await mountCashflows(outlet, { api: { get: api } });
      expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
    });
  });

  describe("dividends", () => {
    const buildApi = (overrides: Record<string, unknown> = {}): ReturnType<typeof vi.fn> =>
      vi.fn().mockImplementation((path: string) => {
        if (path === "/api/dividends") return Promise.resolve(overrides[path] ?? dividends);
        return Promise.reject(new Error(`unexpected ${path}`));
      });

    it("renders KPIs + tables + top payers", async () => {
      const api = buildApi();
      await mountDividends(outlet, { api: { get: api } });
      expect(document.getElementById("kpi-total")?.textContent).toBe("NT$250,000");
      expect(document.getElementById("kpi-events")?.textContent).toContain("18");
      expect(document.getElementById("kpi-payers")?.textContent).toBe("1");
      // top-payers list
      const topPayers = document.getElementById("top-payers");
      expect(topPayers?.textContent).toContain("TSMC");
      // total-return table
      expect(document.querySelectorAll("#tr-table tbody tr").length).toBe(1);
      // dividend log table
      expect(document.querySelectorAll("#div-table tbody tr").length).toBe(1);
    });

    it("top-payers empty state is muted", async () => {
      const api = buildApi({
        "/api/dividends": { ...dividends, by_ticker: [] },
      });
      await mountDividends(outlet, { api: { get: api } });
      const tp = document.getElementById("top-payers");
      expect(tp?.textContent).toContain("No dividend payers");
    });

    it("error path renders .error-box", async () => {
      const api = vi.fn().mockRejectedValue(new Error("boom"));
      await mountDividends(outlet, { api: { get: api } });
      expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
    });
  });
});
