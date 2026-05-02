import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountOverview } from "../src/pages/overview";

const clearDom = (): void => {
  while (document.body.firstChild) {
    document.body.removeChild(document.body.firstChild);
  }
};

const summary = {
  kpis: { real_now_twd: 1_234_567, fx_usd_twd: 31.5, as_of: "2026-04" },
  profit_twd: 50_000,
  invested_twd: 1_000_000,
  twr: 0.0512,
  xirr: 0.0734,
  first_month: "2024-01",
  last_month: "2026-04",
  months_covered: 28,
  equity_curve: [],
  allocation: { tw: 600_000, foreign: 400_000, bank_twd: 200_000, bank_usd: 0 },
};

const holdings = {
  holdings: [
    { code: "2330", name: "TSMC", unrealized_pnl_twd: 90_000, unrealized_pct: 0.18 },
    { code: "0050", name: "ETF50", unrealized_pnl_twd: 30_000, unrealized_pct: 0.05 },
    { code: "2317", name: "Hon Hai", unrealized_pnl_twd: -20_000, unrealized_pct: -0.04 },
  ],
};

const txs = [
  { date: "2026/04/15", venue: "TW", side: "buy", code: "2330", name: "TSMC", qty: 100, price: 580, net_twd: -58_000 },
];

describe("Phase 8 Cycle 57 — overview page mount", () => {
  let outlet: HTMLElement;
  let apiGet: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
    apiGet = vi.fn().mockImplementation((path: string) => {
      if (path === "/api/summary") return Promise.resolve(summary);
      if (path === "/api/holdings/current") return Promise.resolve(holdings);
      if (path === "/api/transactions") return Promise.resolve(txs);
      return Promise.reject(new Error(`unexpected ${path}`));
    });
  });

  afterEach(() => {
    clearDom();
  });

  it("renders KPI hero (#kpi-equity) with twd-formatted value", async () => {
    await mountOverview(outlet, { api: { get: apiGet } });
    expect(document.getElementById("kpi-equity")?.textContent).toBe(
      "NT$1,234,567",
    );
  });

  it("renders KPI sub line with as_of month + USD/TWD rate", async () => {
    await mountOverview(outlet, { api: { get: apiGet } });
    expect(document.getElementById("kpi-equity-sub")?.textContent).toContain(
      "Apr 2026",
    );
    expect(document.getElementById("kpi-equity-sub")?.textContent).toContain(
      "31.500",
    );
  });

  it("renders profit KPI with tone class derived from sign", async () => {
    await mountOverview(outlet, { api: { get: apiGet } });
    const profitEl = document.getElementById("kpi-profit");
    expect(profitEl?.textContent).toBe("NT$50,000");
    expect(profitEl?.className).toContain("value-pos");
  });

  it("renders TWR + XIRR pcts with sign and tone", async () => {
    await mountOverview(outlet, { api: { get: apiGet } });
    expect(document.getElementById("kpi-twr")?.textContent).toBe("+5.12%");
    expect(document.getElementById("kpi-xirr")?.textContent).toBe("+7.34%");
  });

  it("renders xirr as em-dash when null without tone class", async () => {
    apiGet.mockImplementationOnce(() =>
      Promise.resolve({ ...summary, xirr: null }),
    );
    await mountOverview(outlet, { api: { get: apiGet } });
    expect(document.getElementById("kpi-xirr")?.textContent).toBe("—");
  });

  it("renders top winners + losers with ticker links", async () => {
    await mountOverview(outlet, { api: { get: apiGet } });
    const winners = document.getElementById("winners-list");
    const losers = document.getElementById("losers-list");
    expect(winners?.textContent).toContain("TSMC");
    expect(losers?.textContent).toContain("Hon Hai");
    const firstWinnerLink = winners?.querySelector("a");
    expect(firstWinnerLink?.getAttribute("href")).toBe("/ticker/2330");
  });

  it("renders empty state when no holdings", async () => {
    apiGet.mockImplementation((path: string) => {
      if (path === "/api/summary") return Promise.resolve(summary);
      if (path === "/api/holdings/current")
        return Promise.resolve({ holdings: [] });
      return Promise.resolve(txs);
    });
    await mountOverview(outlet, { api: { get: apiGet } });
    expect(document.getElementById("winners-list")?.textContent).toContain(
      "No positions",
    );
  });

  it("renders error box when api throws", async () => {
    apiGet.mockRejectedValue(new Error("boom"));
    await mountOverview(outlet, { api: { get: apiGet } });
    const err = outlet.querySelector(".error-box");
    expect(err?.textContent).toContain("boom");
  });
});
