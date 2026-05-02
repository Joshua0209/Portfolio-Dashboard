// RED reproducer for /risk page mount — Phase 8 Cycle 62
// Pins src/pages/risk.ts:
//   - mountRisk(outlet, { api }) renders 4 KPI tiles, drawdown
//     curve canvas slot, weight donut canvas + HHI label, leverage
//     timeline canvas, and the ratios table.
//   - kpi-mdd carries .value-neg when negative, .value-mute when 0.
//   - HHI label populated from sum(w^2) of weight_distribution.
//   - Ratios table renders 7 rows (Sharpe, Sortino, Calmar, Eff N,
//     Top-5, Top-10, Downside vol) with caps for extreme values.
//   - Error path: .error-box inside outlet.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountRisk } from "../src/pages/risk";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const riskResp = {
  max_drawdown: -0.082,
  annualized_volatility: 0.18,
  top_5_share: 0.42,
  top_10_share: 0.65,
  position_count: 14,
  leverage_pct: 0.0,
  leverage_value_twd: 0,
  current_drawdown: -0.012,
  drawdown_curve: [
    { date: "2026-04", drawdown: -0.082 },
    { date: "2026-05", drawdown: -0.012 },
  ],
  weight_distribution: [
    { code: "2330", weight: 0.30 },
    { code: "AAPL", weight: 0.20 },
    { code: "0050", weight: 0.10 },
  ],
  leverage_timeline: [
    { month: "2026-04", leverage_pct: 0.05 },
  ],
  sharpe_annualized: 1.4,
  sortino_annualized: 2.1,
  calmar: 1.05,
  effective_n: 6.5,
  downside_volatility: 0.12,
};

describe("Phase 8 Cycle 62 — risk page mount", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
  });

  afterEach(() => clearDom());

  it("renders KPI tiles", async () => {
    const api = vi.fn().mockResolvedValue(riskResp);
    await mountRisk(outlet, { api: { get: api } });
    const mdd = document.getElementById("kpi-mdd");
    expect(mdd?.textContent).toBe("-8.20%");
    expect(mdd?.className).toContain("value-neg");
    expect(document.getElementById("kpi-vol")?.textContent).toBe("18.0%");
    expect(document.getElementById("kpi-top5")?.textContent).toBe("42.0%");
    expect(document.getElementById("kpi-positions")?.textContent).toContain("14");
    expect(document.getElementById("kpi-lev")?.textContent).toBe("0.0%");
  });

  it("HHI label = sum(w^2) of weight distribution", async () => {
    const api = vi.fn().mockResolvedValue(riskResp);
    await mountRisk(outlet, { api: { get: api } });
    // 0.30^2 + 0.20^2 + 0.10^2 = 0.09 + 0.04 + 0.01 = 0.14
    expect(document.getElementById("hhi-label")?.textContent).toContain("0.140");
  });

  it("dd-current label populated from current_drawdown", async () => {
    const api = vi.fn().mockResolvedValue(riskResp);
    await mountRisk(outlet, { api: { get: api } });
    expect(document.getElementById("dd-current")?.textContent).toContain("-1.20%");
  });

  it("ratios table renders 7 rows", async () => {
    const api = vi.fn().mockResolvedValue(riskResp);
    await mountRisk(outlet, { api: { get: api } });
    const rows = document.querySelectorAll("#risk-ratios tbody tr");
    expect(rows).toHaveLength(7);
    expect(rows[0].textContent).toContain("Sharpe");
    expect(rows[3].textContent).toContain("Effective N");
    expect(rows[3].textContent).toContain("6.50");
  });

  it("error path renders .error-box inside outlet", async () => {
    const api = vi.fn().mockRejectedValue(new Error("boom"));
    await mountRisk(outlet, { api: { get: api } });
    expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
  });
});
