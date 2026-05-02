// Shared Chart.js configuration. Reads CSS custom properties so charts
// follow the dark/light theme. Phase 8 Cycle 54 port of static/js/charts.js
// — pure helpers + an applyDefaults() side-effect that takes a Chart class
// dependency-injected (so tests don't need to import the heavy chart.js
// bundle just to assert hex math).

export const cssVar = (name: string): string => {
  if (typeof document === "undefined") return "";
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
};

export const palette = (): readonly string[] =>
  Array.from({ length: 8 }, (_, i) => cssVar(`--c${i + 1}`));

export const hexWithAlpha = (hexOrRgb: string, alpha: number): string => {
  const c = hexOrRgb.trim();
  if (c.startsWith("rgba")) return c.replace(/[\d.]+\)$/, `${alpha})`);
  if (c.startsWith("rgb"))
    return c.replace("rgb(", "rgba(").replace(")", `, ${alpha})`);
  if (c.startsWith("#")) {
    const s =
      c.length === 4
        ? c
            .slice(1)
            .split("")
            .map((x) => x + x)
            .join("")
        : c.slice(1);
    if (s.length !== 6) return c;
    const r = parseInt(s.slice(0, 2), 16);
    const g = parseInt(s.slice(2, 4), 16);
    const b = parseInt(s.slice(4, 6), 16);
    if ([r, g, b].some(Number.isNaN)) return c;
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }
  return c;
};

export interface ChartArea {
  top: number;
  bottom: number;
}

export const gradientFill = (
  ctx: CanvasRenderingContext2D,
  area: ChartArea,
  color: string,
  opacity = 0.25,
): CanvasGradient => {
  const g = ctx.createLinearGradient(0, area.top, 0, area.bottom);
  g.addColorStop(0, hexWithAlpha(color, opacity));
  g.addColorStop(1, hexWithAlpha(color, 0));
  return g;
};

// Chart.js types are loose here — we treat the class as opaque to keep
// this module test-friendly and decouple from chart.js's massive type tree.
// applyDefaults runs once at app boot via main.ts.
type ChartLike = {
  defaults: {
    color: string;
    borderColor: string;
    font: { family: string; size: number };
    set: (path: string, value: unknown) => void;
    plugins: {
      legend: { labels: Record<string, unknown> };
      tooltip: Record<string, unknown>;
      decimation: Record<string, unknown>;
    };
  };
};

// Public alias used by every page mount that needs to instantiate charts.
// We type the constructor with `unknown` configs because chart.js v4's
// public types are huge and parameterized over the chart kind — the page
// code already knows what shape it's passing, and the runtime just hands
// the config straight to chart.js. Tests inject a vi.fn() that records
// calls; the dev server passes the real Chart class from chart.js.
export type ChartCtor = new (
  target: HTMLCanvasElement | CanvasRenderingContext2D,
  config: unknown,
) => unknown;

export const applyDefaults = (Chart: ChartLike): void => {
  const tickColor = cssVar("--text-soft");
  const gridColor = cssVar("--line");
  const borderColor = cssVar("--line-strong");

  Chart.defaults.color = tickColor;
  Chart.defaults.font.family = cssVar("--font-sans");
  Chart.defaults.font.size = 11;
  Chart.defaults.borderColor = gridColor;

  const baseTicks = {
    color: tickColor,
    padding: 8,
    font: { size: 11, family: cssVar("--font-mono") },
    autoSkip: true,
    autoSkipPadding: 16,
    maxRotation: 0,
    minRotation: 0,
  };
  Chart.defaults.set("scale", {
    grid: { color: gridColor, drawTicks: false, tickLength: 0 },
    border: { display: false, color: borderColor },
    ticks: baseTicks,
  });
  const TICK_LIMITS: Record<string, number> = {
    linear: 6,
    logarithmic: 6,
    category: 10,
    time: 8,
    timeseries: 8,
    radialLinear: 6,
  };
  for (const [scaleType, limit] of Object.entries(TICK_LIMITS)) {
    Chart.defaults.set(`scales.${scaleType}`, {
      grid: { color: gridColor, drawTicks: false, tickLength: 0 },
      border: { display: false, color: borderColor },
      ticks: { ...baseTicks, maxTicksLimit: limit },
    });
  }

  const legendLabels = Chart.defaults.plugins.legend.labels;
  legendLabels.usePointStyle = true;
  legendLabels.boxWidth = 8;
  legendLabels.boxHeight = 8;
  legendLabels.padding = 14;
  legendLabels.color = cssVar("--text-soft");

  const tooltip = Chart.defaults.plugins.tooltip;
  tooltip.backgroundColor = cssVar("--bg-elev-2");
  tooltip.titleColor = cssVar("--text");
  tooltip.bodyColor = cssVar("--text-soft");
  tooltip.borderColor = cssVar("--line-strong");
  tooltip.borderWidth = 1;
  tooltip.padding = 10;
  tooltip.titleFont = { weight: "600", size: 12 };
  tooltip.bodyFont = { size: 12 };
  tooltip.cornerRadius = 8;
  tooltip.displayColors = true;
  tooltip.boxPadding = 4;

  Chart.defaults.plugins.decimation = {
    enabled: true,
    algorithm: "lttb",
    samples: 200,
    threshold: 500,
  };
};

export interface DailyTimeAxis {
  type: "time";
  time: { unit: string; tooltipFormat: string };
  ticks: { maxTicksLimit: number; color: string };
  grid: { display: boolean };
}

export const dailyTimeAxis = (): DailyTimeAxis => ({
  type: "time",
  time: { unit: "month", tooltipFormat: "PP" },
  ticks: { maxTicksLimit: 8, color: cssVar("--text-faint") },
  grid: { display: false },
});
