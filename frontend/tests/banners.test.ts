// RED reproducer for global DLQ + reconcile banner mounts — Cycle 67
// Pins src/components/banners.ts:
//   mountDlqBanner({ root, fetchJson }): Promise<void>
//     - Polls /api/admin/failed-tasks once.
//     - On {ok: true, data: {tasks: [...]}}, renders a banner inside
//       the host element with a summary "N failed fetches (...)".
//     - Empty tasks → no DOM (Banner empty-message contract).
//     - Network failure → silent, no DOM, no throw.
//
//   mountReconcileBanner({ root, fetchJson }): Promise<void>
//     - Polls /api/today/reconcile once.
//     - Renders top event with deep-link to /today#developer-tools.
//     - broker_pdf_buy_leg_mismatch flavor formats as
//       "(latest: YYYY-MM, 2330 broker shows N buy legs vs PDF M)".
//     - Legacy diff flavor formats as
//       "(latest: YYYY-MM, N differing trades)".
//     - Empty events → no DOM.
//     - Dismiss button POSTs /api/admin/reconcile/<id>/dismiss; only
//       hides banner on res.ok=true (fail-closed semantics).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mountDlqBanner, mountReconcileBanner } from "../src/components/banners";

const clearDom = (): void => {
  while (document.body.firstChild) document.body.removeChild(document.body.firstChild);
};

describe("Phase 8 Cycle 67 — global banners", () => {
  let root: HTMLElement;

  beforeEach(() => {
    clearDom();
    root = document.createElement("div");
    document.body.appendChild(root);
  });

  afterEach(() => clearDom());

  describe("DLQ banner", () => {
    it("renders summary when tasks present", async () => {
      const fetchJson = vi.fn().mockResolvedValue({
        ok: true,
        data: {
          tasks: [
            { task_type: "tw_prices" },
            { task_type: "foreign_prices" },
            { task_type: "foreign_prices" },
          ],
        },
      });
      await mountDlqBanner({ root, fetchJson });
      const text = root.querySelector(".banner-text")?.textContent ?? "";
      expect(text).toContain("3 failed");
      expect(text).toContain("1 TW price");
      expect(text).toContain("2 foreign prices");
    });

    it("empty tasks → no DOM", async () => {
      const fetchJson = vi.fn().mockResolvedValue({ ok: true, data: { tasks: [] } });
      await mountDlqBanner({ root, fetchJson });
      expect(root.querySelector(".banner")).toBeNull();
    });

    it("network failure leaves banner hidden, never throws", async () => {
      const fetchJson = vi.fn().mockRejectedValue(new Error("boom"));
      await mountDlqBanner({ root, fetchJson });
      expect(root.querySelector(".banner")).toBeNull();
    });
  });

  describe("Reconcile banner", () => {
    it("renders broker_pdf_buy_leg_mismatch event", async () => {
      const fetchJson = vi.fn().mockResolvedValue({
        ok: true,
        data: {
          events: [
            {
              id: 7,
              event_type: "broker_pdf_buy_leg_mismatch",
              pdf_month: "2026-04",
              code: "2330",
              sdk_leg_count: 2,
              pdf_trade_count: 3,
            },
          ],
        },
      });
      await mountReconcileBanner({ root, fetchJson });
      const text = root.querySelector(".banner-text")?.textContent ?? "";
      expect(text).toContain("1 unresolved alert");
      expect(text).toContain("2026-04");
      expect(text).toContain("2330");
      expect(text).toContain("2 buy legs vs PDF 3");
      const link = root.querySelector(".banner-action") as HTMLAnchorElement;
      expect(link.getAttribute("href")).toBe("/today#developer-tools");
    });

    it("renders legacy diff event", async () => {
      const fetchJson = vi.fn().mockResolvedValue({
        ok: true,
        data: {
          events: [
            { id: 9, pdf_month: "2026-03", only_in_pdf_count: 2, only_in_overlay_count: 3 },
          ],
        },
      });
      await mountReconcileBanner({ root, fetchJson });
      const text = root.querySelector(".banner-text")?.textContent ?? "";
      expect(text).toContain("5 differing trades");
    });

    it("empty events → no DOM", async () => {
      const fetchJson = vi.fn().mockResolvedValue({ ok: true, data: { events: [] } });
      await mountReconcileBanner({ root, fetchJson });
      expect(root.querySelector(".banner")).toBeNull();
    });

    it("network failure leaves banner hidden", async () => {
      const fetchJson = vi.fn().mockRejectedValue(new Error("net"));
      await mountReconcileBanner({ root, fetchJson });
      expect(root.querySelector(".banner")).toBeNull();
    });
  });
});
