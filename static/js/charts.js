/**
 * Shared Chart.js configuration. Reads CSS custom properties so charts
 * follow the dark/light theme.
 */
(function (global) {
  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function palette() {
    return [
      cssVar("--c1"), cssVar("--c2"), cssVar("--c3"), cssVar("--c4"),
      cssVar("--c5"), cssVar("--c6"), cssVar("--c7"), cssVar("--c8"),
    ];
  }

  function applyDefaults() {
    if (!global.Chart) return;
    const C = global.Chart;
    const tickColor = cssVar("--text-soft");
    const gridColor = cssVar("--line");
    const borderColor = cssVar("--line-strong");

    C.defaults.color = tickColor;
    C.defaults.font.family = cssVar("--font-sans");
    C.defaults.font.size = 11;
    C.defaults.borderColor = gridColor;

    // Chart.js v4 stores scale defaults via `defaults.set` with a routes
    // registry. Direct property assignment silently drops internal v4 keys
    // (tickLength, lineWidth, display) that bar baseline math depends on.
    C.defaults.set("scale", {
      grid: { color: gridColor, drawTicks: false, tickLength: 0 },
      border: { display: false, color: borderColor },
      ticks: {
        color: tickColor,
        padding: 8,
        font: { size: 11, family: cssVar("--font-mono") },
      },
    });
    for (const t of ["linear", "logarithmic", "category", "time", "timeseries", "radialLinear"]) {
      C.defaults.set(`scales.${t}`, {
        grid: { color: gridColor, drawTicks: false, tickLength: 0 },
        border: { display: false, color: borderColor },
        ticks: {
          color: tickColor,
          padding: 8,
          font: { size: 11, family: cssVar("--font-mono") },
        },
      });
    }

    C.defaults.plugins.legend.labels.usePointStyle = true;
    C.defaults.plugins.legend.labels.boxWidth = 8;
    C.defaults.plugins.legend.labels.boxHeight = 8;
    C.defaults.plugins.legend.labels.padding = 14;
    C.defaults.plugins.legend.labels.color = cssVar("--text-soft");

    C.defaults.plugins.tooltip.backgroundColor = cssVar("--bg-elev-2");
    C.defaults.plugins.tooltip.titleColor = cssVar("--text");
    C.defaults.plugins.tooltip.bodyColor = cssVar("--text-soft");
    C.defaults.plugins.tooltip.borderColor = cssVar("--line-strong");
    C.defaults.plugins.tooltip.borderWidth = 1;
    C.defaults.plugins.tooltip.padding = 10;
    C.defaults.plugins.tooltip.titleFont = { weight: "600", size: 12 };
    C.defaults.plugins.tooltip.bodyFont = { size: 12 };
    C.defaults.plugins.tooltip.cornerRadius = 8;
    C.defaults.plugins.tooltip.displayColors = true;
    C.defaults.plugins.tooltip.boxPadding = 4;

    // Decimation: when daily-resolution series push point counts past
    // ~500, downsample to ~200 visually equivalent points using LTTB.
    // Default-on so all line charts inherit it; bar charts ignore it.
    C.defaults.plugins.decimation = {
      enabled: true,
      algorithm: "lttb",
      samples: 200,
      threshold: 500,
    };
  }

  /** Build a gradient fill from accent down to transparent. */
  function gradientFill(ctx, area, color, opacity = 0.25) {
    const g = ctx.createLinearGradient(0, area.top, 0, area.bottom);
    g.addColorStop(0, hexWithAlpha(color, opacity));
    g.addColorStop(1, hexWithAlpha(color, 0));
    return g;
  }

  function hexWithAlpha(hexOrRgb, alpha) {
    const c = hexOrRgb.trim();
    if (c.startsWith("rgba")) return c.replace(/[\d.]+\)$/, `${alpha})`);
    if (c.startsWith("rgb"))  return c.replace("rgb(", "rgba(").replace(")", `, ${alpha})`);
    if (c.startsWith("#")) {
      const s = c.length === 4
        ? c.slice(1).split("").map((x) => x + x).join("")
        : c.slice(1);
      const r = parseInt(s.slice(0, 2), 16);
      const g = parseInt(s.slice(2, 4), 16);
      const b = parseInt(s.slice(4, 6), 16);
      return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }
    return c;
  }

  /** Money tooltip callback. */
  function moneyTooltip(values, currency = "TWD") {
    return function (ctx) {
      const v = ctx.parsed?.y ?? ctx.parsed;
      if (typeof v !== "number") return ctx.formattedValue;
      const fmt = (currency === "TWD") ? global.fmt.twd : global.fmt.usd;
      return `${ctx.dataset.label}: ${fmt(v)}`;
    };
  }

  global.charts = {
    cssVar, palette, applyDefaults, gradientFill, hexWithAlpha, moneyTooltip,
  };
})(window);
