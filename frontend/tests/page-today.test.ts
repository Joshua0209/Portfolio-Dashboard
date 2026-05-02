// RED reproducer for /today page mount — Phase 8 Cycle 59
// Pins src/pages/today.ts:
//   - mountToday(outlet, { api, refresh? }) renders DOM verbatim from
//     templates/today.html (#equity-twd, #data-date-heading, #wallclock-context,
//     #movers-up, #movers-down, #period-strip, #cal-months, #freshness-dot,
//     #risk-* IDs).
//   - Hero KPIs: equity-twd, delta-twd (signed twd + tone class), delta-pct
//     (signed pct), n-positions, fx-usd-twd (3-decimal raw number).
//   - Wallclock-context line is hidden when today_in_tpe === data_date,
//     visible otherwise. The today.js legacy used hidden=true; same here.
//   - Movers: gainers go to #movers-up, decliners to #movers-down. Each
//     row links to /ticker/<symbol>. Empty state renders <li class="muted">—</li>.
//   - Period strip: 4 .period-strip__cell, each gets value (signed pct)
//     and sub line (signed twd · since <anchor_date>). Tone class on value.
//   - Risk metrics tile populates the 8 risk-* IDs from /api/today/risk-metrics.
//   - Calendar empty state when data.empty true: prints "Need at least 2..."
//     muted note.
//   - Refresh button POSTs /api/admin/refresh and reloads; failures show
//     "refresh failed:" prefix in #refresh-status.
//   - Error during initial load renders .error-box inside the outlet
//     (consistent with Cycle 57 contract).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountToday } from "../src/pages/today";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

const snapshot = {
  data_date: "2026-05-01",
  weekday: "Friday",
  today_in_tpe: "2026-05-02",
  equity_twd: 1_500_000,
  delta_twd: 12_345,
  delta_pct: 0.83,
  n_positions: 14,
  fx_usd_twd: 32.123,
};

const movers = {
  movers: [
    { symbol: "2330", delta_pct: 1.5 },
    { symbol: "AAPL", delta_pct: 0.7 },
    { symbol: "TSLA", delta_pct: -2.1 },
  ],
};

const periods = {
  windows: [
    { label: "MTD", delta_pct: 1.2, delta_twd: 18000, anchor_date: "2026-04-30" },
    { label: "QTD", delta_pct: 3.4, delta_twd: 50000, anchor_date: "2026-03-31" },
    { label: "YTD", delta_pct: -2.5, delta_twd: -42000, anchor_date: "2025-12-31" },
    { label: "Inception", delta_pct: 18.2, delta_twd: 240000, anchor_date: "2024-01-01" },
  ],
};

const risk = {
  ann_return_pct: 12.5,
  ann_vol_pct: 18.4,
  rolling_30d_vol_pct: 14.2,
  sharpe: 0.68,
  sortino: 1.02,
  max_drawdown_pct: -12.3,
  hit_rate_pct: 56,
  best_day_pct: 3.2,
  worst_day_pct: -4.1,
  n_days: 504,
};

const sparkline = {
  points: [
    { date: "2026-04-25", equity_twd: 1_480_000 },
    { date: "2026-05-01", equity_twd: 1_500_000 },
  ],
};

const drawdown = {
  empty: false,
  current_dd: -3.5,
  max_dd: -12.3,
  max_dd_date: "2024-08-05",
  max_dd_peak_date: "2024-07-20",
  current_peak_date: "2026-04-15",
  points: [
    { date: "2026-04-25", drawdown_pct: -3.0 },
    { date: "2026-05-01", drawdown_pct: -3.5 },
  ],
};

const calendarEmpty = { empty: true, cells: [], months: [] };

const freshness = { data_date: "2026-05-01", band: "green", stale_days: 0 };

const wireMockApi = (
  overrides: Record<string, unknown> = {},
): ReturnType<typeof vi.fn> => {
  const responses: Record<string, unknown> = {
    "/api/today/snapshot": snapshot,
    "/api/today/movers": movers,
    "/api/today/sparkline": sparkline,
    "/api/today/freshness": freshness,
    "/api/today/period-returns": periods,
    "/api/today/drawdown": drawdown,
    "/api/today/risk-metrics": risk,
    "/api/today/calendar": calendarEmpty,
    ...overrides,
  };
  return vi.fn().mockImplementation((path: string) => {
    if (path in responses) return Promise.resolve(responses[path]);
    return Promise.reject(new Error(`unexpected ${path}`));
  });
};

describe("Phase 8 Cycle 59 — today page mount", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    clearDom();
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
  });

  afterEach(() => clearDom());

  it("renders hero KPIs from /api/today/snapshot", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    expect(document.getElementById("equity-twd")?.textContent).toBe("NT$1,500,000");
    expect(document.getElementById("delta-twd")?.textContent).toContain("+");
    expect(document.getElementById("delta-pct")?.textContent).toBe("+0.83%");
    expect(document.getElementById("n-positions")?.textContent).toBe("14");
    expect(document.getElementById("fx-usd-twd")?.textContent).toBe("32.123");
  });

  it("data-date heading uses weekday + data_date", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const heading = document.getElementById("data-date-heading")?.textContent ?? "";
    expect(heading).toContain("Friday");
    expect(heading).toContain("2026-05-01");
  });

  it("wallclock-context visible when today_in_tpe != data_date", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const ctx = document.getElementById("wallclock-context") as HTMLElement;
    expect(ctx.hidden).toBe(false);
    expect(ctx.textContent).toContain("2026-05-02");
  });

  it("wallclock-context hidden when today_in_tpe == data_date", async () => {
    const api = wireMockApi({
      "/api/today/snapshot": { ...snapshot, today_in_tpe: snapshot.data_date },
    });
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    expect((document.getElementById("wallclock-context") as HTMLElement).hidden).toBe(true);
  });

  it("renders gainers + decliners with /ticker links", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const up = document.getElementById("movers-up")!;
    const down = document.getElementById("movers-down")!;
    const upLinks = up.querySelectorAll("a");
    const downLinks = down.querySelectorAll("a");
    expect(upLinks.length).toBe(2);
    expect(downLinks.length).toBe(1);
    expect(upLinks[0].getAttribute("href")).toBe("/ticker/2330");
    expect(downLinks[0].getAttribute("href")).toBe("/ticker/TSLA");
  });

  it("movers empty state renders muted '—' when none", async () => {
    const api = wireMockApi({ "/api/today/movers": { movers: [] } });
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const upMuted = document.querySelector("#movers-up li.muted");
    const downMuted = document.querySelector("#movers-down li.muted");
    expect(upMuted?.textContent).toBe("—");
    expect(downMuted?.textContent).toBe("—");
  });

  it("period strip fills 4 cells with signed pct + tone", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const cells = document.querySelectorAll(".period-strip__cell");
    expect(cells).toHaveLength(4);
    const mtdValue = cells[0].querySelector(".period-strip__value");
    expect(mtdValue?.textContent).toBe("+1.20%");
    expect(mtdValue?.classList.contains("pos")).toBe(true);
    const ytdValue = cells[2].querySelector(".period-strip__value");
    expect(ytdValue?.textContent).toBe("-2.50%");
    expect(ytdValue?.classList.contains("neg")).toBe(true);
  });

  it("risk metrics tile populates risk-* IDs", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    expect(document.getElementById("risk-ann-return")?.textContent).toBe("+12.50%");
    expect(document.getElementById("risk-ann-vol")?.textContent).toBe("18.40%");
    expect(document.getElementById("risk-sharpe")?.textContent).toBe("0.68");
    expect(document.getElementById("risk-sortino")?.textContent).toContain("1.02");
    expect(document.getElementById("risk-max-dd")?.textContent).toBe("-12.30%");
    expect(document.getElementById("risk-window-meta")?.textContent).toContain("504");
  });

  it("calendar shows muted prompt when data.empty", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const cal = document.getElementById("cal-months");
    expect(cal?.textContent).toContain("at least 2");
  });

  it("freshness dot data-band + text from /api/today/freshness", async () => {
    const api = wireMockApi();
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const dot = document.getElementById("freshness-dot") as HTMLElement;
    expect(dot.dataset.band).toBe("green");
    expect(document.getElementById("freshness-text")?.textContent).toContain("today");
  });

  it("refresh button POSTs /api/admin/refresh and updates status", async () => {
    const api = wireMockApi();
    const fetchJson = vi.fn().mockResolvedValue({
      ok: true,
      data: { new_dates: 1, new_rows: 14 },
    });
    await mountToday(outlet, { api: { get: api }, fetchJson });
    const btn = document.getElementById("refresh-btn") as HTMLButtonElement;
    btn.click();
    await new Promise((r) => setTimeout(r, 0));
    expect(fetchJson).toHaveBeenCalledWith("/api/admin/refresh", { method: "POST" });
    expect(document.getElementById("refresh-status")?.textContent).toContain("1 new");
  });

  it("error during load renders .error-box inside outlet", async () => {
    const api = vi.fn().mockRejectedValue(new Error("boom"));
    await mountToday(outlet, { api: { get: api }, fetchJson: vi.fn() });
    const err = outlet.querySelector(".error-box");
    expect(err?.textContent).toContain("boom");
  });
});
