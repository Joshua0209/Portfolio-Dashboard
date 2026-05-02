// RED reproducer for /holdings page mount — Phase 8 Cycle 60
// Pins src/pages/holdings.ts:
//   - mountHoldings(outlet, { api, downloadBlob? }) renders KPIs,
//     treemap canvas slot, sector list, and a DataTable-backed table.
//   - KPI tile pulls from /api/holdings/current top-level fields:
//     total_mv_twd → #kpi-mv, holdings.length → #kpi-count, etc.
//   - Sector list iterates /api/holdings/sectors and emits a .bar-row
//     per sector with a .bar fill width = pct of total.
//   - Holdings table mounts via mountDataTable<Holding> with venue
//     filter and code/name search.
//   - Export-CSV button downloads a CSV via the injected
//     downloadBlob hook (avoids window.URL/createObjectURL in tests).
//   - Error path renders .error-box inside outlet (Cycle 57 contract).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountHoldings } from "../src/pages/holdings";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const holdingsResponse = {
  total_mv_twd: 2_000_000,
  total_cost_twd: 1_500_000,
  total_upnl_twd: 500_000,
  total_upnl_pct: 0.333,
  fx_usd_twd: 31.5,
  as_of: "2026-04",
  holdings: [
    {
      code: "2330",
      name: "TSMC",
      venue: "TW",
      type: "現股",
      ccy: "TWD",
      qty: 1000,
      avg_cost: 800,
      ref_price: 1000,
      cost_twd: 800_000,
      mkt_value_twd: 1_000_000,
      unrealized_pnl_twd: 200_000,
      unrealized_pct: 0.25,
      weight: 0.5,
    },
    {
      code: "AAPL",
      name: "Apple",
      venue: "Foreign",
      type: "現股",
      ccy: "USD",
      qty: 100,
      avg_cost: 150,
      ref_price: 195,
      cost_twd: 472_500,
      mkt_value_twd: 614_250,
      unrealized_pnl_twd: 141_750,
      unrealized_pct: 0.30,
      weight: 0.31,
    },
  ],
};

const sectorsResponse = [
  { sector: "Semiconductors", value_twd: 1_000_000, count: 1 },
  { sector: "Tech", value_twd: 614_250, count: 1 },
];

describe("Phase 8 Cycle 60 — holdings page mount", () => {
  let outlet: HTMLElement;
  let apiGet: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
    apiGet = vi.fn().mockImplementation((path: string) => {
      if (path === "/api/holdings/current") return Promise.resolve(holdingsResponse);
      if (path === "/api/holdings/sectors") return Promise.resolve(sectorsResponse);
      return Promise.reject(new Error(`unexpected ${path}`));
    });
  });

  afterEach(() => clearDom());

  it("renders KPI tiles from /api/holdings/current", async () => {
    await mountHoldings(outlet, { api: { get: apiGet } });
    expect(document.getElementById("kpi-mv")?.textContent).toBe("NT$2,000,000");
    expect(document.getElementById("kpi-count")?.textContent).toBe("2");
    expect(document.getElementById("kpi-cost")?.textContent).toBe("NT$1,500,000");
    expect(document.getElementById("kpi-upnl")?.textContent).toBe("NT$500,000");
    const upnl = document.getElementById("kpi-upnl");
    expect(upnl?.className).toContain("value-pos");
    expect(document.getElementById("kpi-fx")?.textContent).toBe("31.500");
  });

  it("renders sector breakdown bars", async () => {
    await mountHoldings(outlet, { api: { get: apiGet } });
    const rows = document.querySelectorAll("#sector-list .bar-row");
    expect(rows).toHaveLength(2);
    expect(rows[0].textContent).toContain("Semiconductors");
  });

  it("renders holdings rows via DataTable", async () => {
    await mountHoldings(outlet, { api: { get: apiGet } });
    const tbody = document.querySelector("#holdings-table tbody");
    const rows = tbody?.querySelectorAll("tr") ?? [];
    expect(rows.length).toBe(2);
    // Code link
    const firstLink = rows[0].querySelector("a");
    expect(firstLink?.getAttribute("href")).toBe("/ticker/2330");
  });

  it("venue filter narrows the table", async () => {
    await mountHoldings(outlet, { api: { get: apiGet } });
    const sel = document.querySelector<HTMLSelectElement>(".dt-filter")!;
    sel.value = "TW";
    sel.dispatchEvent(new Event("change"));
    const rows = document.querySelectorAll("#holdings-table tbody tr");
    expect(rows.length).toBe(1);
  });

  it("export CSV emits header + rows via downloadBlob", async () => {
    const downloadBlob = vi.fn();
    await mountHoldings(outlet, { api: { get: apiGet }, downloadBlob });
    const btn = document.getElementById("export-holdings") as HTMLButtonElement;
    btn.click();
    expect(downloadBlob).toHaveBeenCalledTimes(1);
    const [content, filename] = downloadBlob.mock.calls[0];
    expect(filename).toMatch(/^holdings-\d{4}-\d{2}-\d{2}\.csv$/);
    expect(content).toContain("code,name,venue");
    expect(content).toContain("2330,TSMC,TW");
  });

  it("error during load renders .error-box inside outlet", async () => {
    apiGet.mockRejectedValue(new Error("boom"));
    await mountHoldings(outlet, { api: { get: apiGet } });
    expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
  });
});
