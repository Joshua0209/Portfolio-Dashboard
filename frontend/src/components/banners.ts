// Global DLQ + reconcile banners. Phase 8 Cycle 67. Ports of
// static/js/{dlq-banner,reconcile-banner}.js into typed mounts that
// paint via the existing renderBanner component (Cycle 56).
//
// Both mounts are silent on network failure — staleness, not
// security: a flaky polling endpoint must never block navigation
// or render an alarming error.

import { renderBanner } from "./Banner";

interface FetchEnvelope<T> {
  ok?: boolean;
  data?: T;
  error?: string;
}

type FetchJson = (
  path: string,
  init?: RequestInit,
) => Promise<FetchEnvelope<unknown>>;

interface FailedTask {
  task_type?: string;
}

interface DlqResponse {
  tasks?: ReadonlyArray<FailedTask>;
}

interface ReconcileEvent {
  id?: number;
  event_type?: string;
  pdf_month?: string;
  code?: string;
  sdk_leg_count?: number | string;
  pdf_trade_count?: number | string;
  only_in_pdf_count?: number | string;
  only_in_overlay_count?: number | string;
}

interface ReconcileResponse {
  events?: ReadonlyArray<ReconcileEvent>;
}

const summarizeDlq = (tasks: ReadonlyArray<FailedTask>): string => {
  const counts: Record<string, number> = {
    tw_prices: 0,
    foreign_prices: 0,
    fx_rates: 0,
    other: 0,
  };
  for (const t of tasks) {
    const k = t.task_type ?? "other";
    if (counts[k] !== undefined) counts[k] += 1;
    else counts.other += 1;
  }
  const parts: string[] = [];
  if (counts.tw_prices)
    parts.push(`${counts.tw_prices} TW price${counts.tw_prices === 1 ? "" : "s"}`);
  if (counts.foreign_prices)
    parts.push(
      `${counts.foreign_prices} foreign price${counts.foreign_prices === 1 ? "" : "s"}`,
    );
  if (counts.fx_rates)
    parts.push(`${counts.fx_rates} FX rate${counts.fx_rates === 1 ? "" : "s"}`);
  if (counts.other) parts.push(`${counts.other} other`);
  return parts.join(", ");
};

interface MountOptions {
  root: HTMLElement;
  fetchJson: FetchJson;
}

export const mountDlqBanner = async (opts: MountOptions): Promise<void> => {
  let body: FetchEnvelope<DlqResponse>;
  try {
    body = (await opts.fetchJson("/api/admin/failed-tasks")) as FetchEnvelope<DlqResponse>;
  } catch {
    return;
  }
  if (!body || body.ok !== true) return;
  const tasks = body.data?.tasks ?? [];
  if (!tasks.length) return;

  const breakdown = summarizeDlq(tasks);
  const message =
    `${tasks.length} failed fetch${tasks.length === 1 ? "" : "es"} ` +
    `(${breakdown}). Some holdings may be missing today's price.`;

  renderBanner(opts.root, {
    tone: "warn",
    message,
    action: { label: "View details", href: "/today#developer-tools" },
  });
};

const formatReconcile = (
  events: ReadonlyArray<ReconcileEvent>,
  top: ReconcileEvent,
): string => {
  const count = events.length;
  const plural = count === 1 ? "" : "s";
  const prefix = `${count} unresolved alert${plural} `;

  if (top.event_type === "broker_pdf_buy_leg_mismatch") {
    const sdk = Number(top.sdk_leg_count) || 0;
    const pdf = Number(top.pdf_trade_count) || 0;
    const code = top.code ?? "?";
    return (
      prefix +
      `(latest: ${top.pdf_month}, ${code} broker shows ${sdk} buy leg${
        sdk === 1 ? "" : "s"
      } vs PDF ${pdf}).`
    );
  }

  const total =
    (Number(top.only_in_pdf_count) || 0) +
    (Number(top.only_in_overlay_count) || 0);
  return (
    prefix +
    `(latest: ${top.pdf_month}, ${total} differing trade${
      total === 1 ? "" : "s"
    }).`
  );
};

export const mountReconcileBanner = async (opts: MountOptions): Promise<void> => {
  let body: FetchEnvelope<ReconcileResponse>;
  try {
    body = (await opts.fetchJson("/api/today/reconcile")) as FetchEnvelope<ReconcileResponse>;
  } catch {
    return;
  }
  if (!body || body.ok !== true) return;
  const events = body.data?.events ?? [];
  if (!events.length) return;

  const top = events[0];
  const message = formatReconcile(events, top);
  renderBanner(opts.root, {
    tone: "neg",
    message,
    action: { label: "View details", href: "/today#developer-tools" },
  });
};
