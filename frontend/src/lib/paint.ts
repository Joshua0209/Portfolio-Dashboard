// Chart-paint helpers shared across page mounts. Each helper builds a
// chart.js v4 config from a small, typed input and instantiates a Chart
// against the canvas. When the canvas isn't found in the DOM (page got
// unmounted before paint, or scaffold skipped a slot), the helper bails
// silently — same legacy behavior as `if (!ctx) return;` in static/js/*.
//
// Phase 8 Cycle 66 — Chart.js wiring sweep.

import type { ChartCtor } from "./charts";
import { cssVar, gradientFill, hexWithAlpha, palette } from "./charts";
import { twd } from "./format";

type Cfg = Record<string, unknown>;

// chart.js v4 accepts a <canvas> element directly. We pass the canvas
// (not the 2d context) so that happy-dom — which returns null from
// getContext('2d') — can still exercise the paint contract in tests.
// In production chart.js handles either form.
const canvasFor = (id: string): HTMLCanvasElement | null => {
  const node = document.getElementById(id);
  return node instanceof HTMLCanvasElement ? node : null;
};

// ─────────────── Line ───────────────

interface LineDataset {
  label: string;
  data: ReadonlyArray<number | null>;
  color: string;
  yAxisID?: string;
  borderWidth?: number;
  borderDash?: number[];
  fill?: boolean;
  fillOpacity?: number;
  tension?: number;
}

export interface LineSpec {
  labels: ReadonlyArray<string>;
  datasets: ReadonlyArray<LineDataset>;
  options?: Cfg;
}

export const paintLine = (
  Chart: ChartCtor,
  canvasId: string,
  spec: LineSpec,
): void => {
  const canvas = canvasFor(canvasId);
  if (!canvas) return;
  const datasets = spec.datasets.map((d) => ({
    label: d.label,
    data: d.data as number[],
    yAxisID: d.yAxisID,
    borderColor: d.color,
    borderWidth: d.borderWidth ?? 2,
    borderDash: d.borderDash,
    tension: d.tension ?? 0.3,
    pointRadius: 0,
    pointHoverRadius: 4,
    fill: d.fill ?? false,
    backgroundColor: d.fill
      ? (c: { chart: { ctx: CanvasRenderingContext2D; chartArea?: { top: number; bottom: number } } }) =>
          c.chart.chartArea
            ? gradientFill(c.chart.ctx, c.chart.chartArea, d.color, d.fillOpacity ?? 0.22)
            : "transparent"
      : undefined,
  }));
  new Chart(canvas, {
    type: "line",
    data: { labels: spec.labels as string[], datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { display: spec.datasets.length > 1, position: "top", align: "end" } },
      ...(spec.options ?? {}),
    },
  });
};

// ─────────────── Bar ───────────────

interface BarDataset {
  label: string;
  data: ReadonlyArray<number>;
  color: string;
  stack?: string;
}

export interface BarSpec {
  labels: ReadonlyArray<string>;
  datasets: ReadonlyArray<BarDataset>;
  options?: Cfg;
}

export const paintBar = (
  Chart: ChartCtor,
  canvasId: string,
  spec: BarSpec,
): void => {
  const canvas = canvasFor(canvasId);
  if (!canvas) return;
  const datasets = spec.datasets.map((d) => ({
    label: d.label,
    data: d.data as number[],
    backgroundColor: hexWithAlpha(d.color, 0.7),
    borderColor: d.color,
    borderWidth: 1,
    stack: d.stack,
  }));
  new Chart(canvas, {
    type: "bar",
    data: { labels: spec.labels as string[], datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: spec.datasets.length > 1, position: "top", align: "end" } },
      scales: { x: { stacked: !!spec.datasets.find((d) => d.stack) }, y: { stacked: !!spec.datasets.find((d) => d.stack) } },
      ...(spec.options ?? {}),
    },
  });
};

// ─────────────── Doughnut ───────────────

export interface DoughnutSpec {
  labels: ReadonlyArray<string>;
  values: ReadonlyArray<number>;
  colors: ReadonlyArray<string>;
}

export const paintDoughnut = (
  Chart: ChartCtor,
  canvasId: string,
  spec: DoughnutSpec,
): void => {
  const canvas = canvasFor(canvasId);
  if (!canvas) return;
  const total = spec.values.reduce((a, b) => a + b, 0);
  new Chart(canvas, {
    type: "doughnut",
    data: {
      labels: spec.labels as string[],
      datasets: [
        {
          data: spec.values as number[],
          backgroundColor: spec.colors as string[],
          borderColor: cssVar("--bg-elev"),
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "62%",
      layout: { padding: 4 },
      plugins: {
        legend: { display: true, position: "right" },
        tooltip: {
          callbacks: {
            label: (c: { label: string; parsed: number }) => {
              const pct = total > 0 ? (c.parsed / total) * 100 : 0;
              return `${c.label}: ${twd(c.parsed)} (${pct.toFixed(1)}%)`;
            },
          },
        },
      },
    },
  });
};

// ─────────────── Treemap ───────────────

export interface TreemapNode {
  label: string;
  value: number;
  color: string;
}

export const paintTreemap = (
  Chart: ChartCtor,
  canvasId: string,
  nodes: ReadonlyArray<TreemapNode>,
): void => {
  const canvas = canvasFor(canvasId);
  if (!canvas) return;
  if (!nodes.length) return;
  const tree = nodes.map((n) => ({ label: n.label, _value: n.value, _color: n.color }));
  new Chart(canvas, {
    type: "treemap",
    data: {
      datasets: [
        {
          tree,
          key: "_value",
          labels: {
            display: true,
            color: cssVar("--text"),
            font: { family: cssVar("--font-sans"), size: 11, weight: "600" },
            formatter: (c: { raw: { _data: TreemapNode } }) => c.raw._data.label,
          },
          backgroundColor: (c: { raw: { _data: { _color: string } } }) =>
            c.raw?._data?._color ?? palette()[0],
          borderColor: cssVar("--bg-elev"),
          borderWidth: 1,
          spacing: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
    },
  });
};

// ─────────────── Scatter ───────────────

export interface ScatterPoint {
  x: number;
  y: number;
  label: string;
}

export interface ScatterSeries {
  label: string;
  color: string;
  points: ReadonlyArray<ScatterPoint>;
}

export const paintScatter = (
  Chart: ChartCtor,
  canvasId: string,
  series: ReadonlyArray<ScatterSeries>,
  axes?: { xLabel?: string; yLabel?: string },
): void => {
  const canvas = canvasFor(canvasId);
  if (!canvas) return;
  new Chart(canvas, {
    type: "scatter",
    data: {
      datasets: series.map((s) => ({
        label: s.label,
        data: s.points.map((p) => ({ x: p.x, y: p.y, _label: p.label })),
        backgroundColor: hexWithAlpha(s.color, 0.65),
        borderColor: s.color,
        pointRadius: 6,
        pointHoverRadius: 8,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: "top" },
        tooltip: {
          callbacks: {
            label: (c: { raw: { _label: string; x: number; y: number } }) =>
              `${c.raw._label}: ${c.raw.x.toFixed(2)} vol, ${c.raw.y.toFixed(2)} return`,
          },
        },
      },
      scales: {
        x: { title: { display: !!axes?.xLabel, text: axes?.xLabel ?? "" } },
        y: { title: { display: !!axes?.yLabel, text: axes?.yLabel ?? "" } },
      },
    },
  });
};
