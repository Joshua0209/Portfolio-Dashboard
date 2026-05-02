// RED reproducer for /fx page mount — Phase 8 Cycle 63
// Pins src/pages/fx.ts:
//   - mountFx(outlet, { api }) renders KPIs (kpi-cur, kpi-first sub,
//     kpi-fx + kpi-fx-twd sub, kpi-fx-pnl with tone, kpi-drate
//     with tone) and chart canvas slots (chart-rate, chart-fx-pnl,
//     chart-ccy).
//   - empty:true response shows a .empty-state with the legacy
//     "No FX data yet" prompt.
//   - Error path: .error-box inside outlet.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountFx } from "../src/pages/fx";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const fxResp = {
  current_rate: 32.123,
  first_rate: 30.500,
  foreign_share: 0.42,
  foreign_value_twd: 850_000,
  fx_pnl: { contribution_twd: 75_000, monthly: [] },
  rate_curve: [
    { date: "2024-01", fx_usd_twd: 30.5 },
    { date: "2026-04", fx_usd_twd: 32.123 },
  ],
  by_ccy_monthly: [],
};

describe("Phase 8 Cycle 63 — fx page mount", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
  });

  afterEach(() => clearDom());

  it("renders KPIs from /api/fx", async () => {
    const api = vi.fn().mockResolvedValue(fxResp);
    await mountFx(outlet, { api: { get: api } });
    expect(document.getElementById("kpi-cur")?.textContent).toBe("32.123");
    expect(document.getElementById("kpi-first")?.textContent).toContain("30.500");
    expect(document.getElementById("kpi-fx")?.textContent).toBe("42.0%");
    expect(document.getElementById("kpi-fx-twd")?.textContent).toBe("NT$850,000");
    const pnl = document.getElementById("kpi-fx-pnl");
    expect(pnl?.textContent).toBe("NT$75,000");
    expect(pnl?.className).toContain("value-pos");
    const drate = document.getElementById("kpi-drate");
    // (32.123 - 30.5) / 30.5 = 0.05321...
    expect(drate?.textContent).toBe("+5.32%");
    expect(drate?.className).toContain("value-pos");
  });

  it("empty:true renders .empty-state instead of KPIs", async () => {
    const api = vi.fn().mockResolvedValue({ empty: true });
    await mountFx(outlet, { api: { get: api } });
    const empty = outlet.querySelector(".empty-state");
    expect(empty?.textContent).toContain("No FX data yet");
  });

  it("error path renders .error-box inside outlet", async () => {
    const api = vi.fn().mockRejectedValue(new Error("boom"));
    await mountFx(outlet, { api: { get: api } });
    expect(outlet.querySelector(".error-box")?.textContent).toContain("boom");
  });
});
