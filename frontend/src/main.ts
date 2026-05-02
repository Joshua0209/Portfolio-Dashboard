// Entry point — boots theme, mounts shell, starts router.
// Phase 8 Cycle 55. Pages are registered as stubs; later cycles fill in
// the actual mount functions.

import { Chart, registerables } from "chart.js";
import { TreemapController, TreemapElement } from "chartjs-chart-treemap";
import "chartjs-adapter-date-fns";

import { renderShell } from "./components/shell";
import { mountFreshnessDot } from "./components/FreshnessDot";
import { mountDlqBanner, mountReconcileBanner } from "./components/banners";
import { api } from "./lib/api";
import { initTheme } from "./lib/theme";
import { applyDefaults } from "./lib/charts";
import type { ChartCtor } from "./lib/charts";
import { createRouter } from "./lib/router";
import type { Route } from "./lib/router";

Chart.register(...registerables, TreemapController, TreemapElement);
applyDefaults(Chart as unknown as Parameters<typeof applyDefaults>[0]);
const C = Chart as unknown as ChartCtor;
import { mountOverview } from "./pages/overview";
import { mountToday } from "./pages/today";
import { mountHoldings } from "./pages/holdings";
import { mountPerformance } from "./pages/performance";
import { mountRisk } from "./pages/risk";
import { mountFx } from "./pages/fx";
import { mountTransactions } from "./pages/transactions";
import { mountCashflows } from "./pages/cashflows";
import { mountDividends } from "./pages/dividends";
import { mountTax } from "./pages/tax";
import { mountTicker } from "./pages/ticker";
import { mountBenchmark } from "./pages/benchmark";

const fetchJson = async (
  path: string,
  init?: RequestInit,
): Promise<{ ok?: boolean; data?: unknown; error?: string }> => {
  const r = await fetch(path, init);
  return r.json();
};

const ROUTES: readonly Route[] = [
  {
    path: "/",
    page: "overview",
    mount: (outlet) => mountOverview(outlet, { api, Chart: C }),
  },
  {
    path: "/today",
    page: "today",
    mount: (outlet) => mountToday(outlet, { api, fetchJson, Chart: C }),
  },
  {
    path: "/holdings",
    page: "holdings",
    mount: (outlet) => mountHoldings(outlet, { api, Chart: C }),
  },
  {
    path: "/performance",
    page: "performance",
    mount: (outlet) => mountPerformance(outlet, { api, Chart: C }),
  },
  {
    path: "/risk",
    page: "risk",
    mount: (outlet) => mountRisk(outlet, { api, Chart: C }),
  },
  {
    path: "/benchmark",
    page: "benchmark",
    mount: (outlet) => mountBenchmark(outlet, { api, Chart: C }),
  },
  {
    path: "/fx",
    page: "fx",
    mount: (outlet) => mountFx(outlet, { api, Chart: C }),
  },
  {
    path: "/transactions",
    page: "transactions",
    mount: (outlet) => mountTransactions(outlet, { api, Chart: C }),
  },
  {
    path: "/cashflows",
    page: "cashflows",
    mount: (outlet) => mountCashflows(outlet, { api, Chart: C }),
  },
  {
    path: "/dividends",
    page: "dividends",
    mount: (outlet) => mountDividends(outlet, { api, Chart: C }),
  },
  {
    path: "/tax",
    page: "tax",
    mount: (outlet) => mountTax(outlet, { api }),
  },
  {
    path: "/ticker/:code",
    page: "ticker",
    mount: (outlet, ctx) =>
      mountTicker(outlet, { api, code: ctx.params.code ?? "", Chart: C }),
  },
];

export function init(): void {
  const root = document.getElementById("app");
  if (!root) return;
  initTheme();
  const shell = renderShell(root);
  const router = createRouter({
    outlet: shell.outlet,
    routes: ROUTES,
    onRouteChange: (page) => shell.setActivePage(page),
  });
  router.start();
  void mountFreshnessDot();

  const reconcileSlot = document.getElementById("reconcile-banner-mount");
  if (reconcileSlot) void mountReconcileBanner({ root: reconcileSlot, fetchJson });
  const dlqSlot = document.getElementById("dlq-banner-mount");
  if (dlqSlot) void mountDlqBanner({ root: dlqSlot, fetchJson });

  // Intercept in-app link clicks so navigation stays SPA-style.
  document.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof Element)) return;
    const anchor = target.closest("a[href]");
    if (!(anchor instanceof HTMLAnchorElement)) return;
    const href = anchor.getAttribute("href");
    if (!href || !href.startsWith("/")) return;
    if (anchor.target === "_blank") return;
    if (ev.metaKey || ev.ctrlKey || ev.shiftKey) return;
    ev.preventDefault();
    router.navigate(href);
  });

  root.dataset.bootstrapped = "true";
}

if (typeof document !== "undefined" && document.readyState !== "loading") {
  init();
} else if (typeof document !== "undefined") {
  document.addEventListener("DOMContentLoaded", init);
}
