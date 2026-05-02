import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderKpiCard } from "../src/components/KpiCard";
import { renderBanner } from "../src/components/Banner";
import { mountFreshnessDot } from "../src/components/FreshnessDot";

const clearDom = (): void => {
  while (document.body.firstChild) {
    document.body.removeChild(document.body.firstChild);
  }
};

describe("Phase 8 Cycle 56 — components", () => {
  beforeEach(() => {
    clearDom();
  });
  afterEach(() => {
    clearDom();
  });

  describe("KpiCard", () => {
    it("renders label + formatted value", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderKpiCard(root, { label: "NAV", value: 1234567, kind: "twd" });
      expect(root.querySelector(".kpi-label")?.textContent).toBe("NAV");
      expect(root.querySelector(".kpi-value")?.textContent).toBe(
        "NT$1,234,567",
      );
    });

    it("renders pct delta with tone class", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderKpiCard(root, {
        label: "Day",
        value: 0.0123,
        kind: "pct",
        delta: 0.0123,
      });
      const delta = root.querySelector(".kpi-delta");
      expect(delta?.textContent).toBe("+1.23%");
      expect(delta?.classList.contains("value-pos")).toBe(true);
    });

    it("renders em-dash for null value", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderKpiCard(root, { label: "x", value: null, kind: "twd" });
      expect(root.querySelector(".kpi-value")?.textContent).toBe("—");
    });

    it("does not render delta element when delta is undefined", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderKpiCard(root, { label: "x", value: 1, kind: "int" });
      expect(root.querySelector(".kpi-delta")).toBeNull();
    });
  });

  describe("Banner", () => {
    it("renders nothing when message is empty", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderBanner(root, { tone: "warn", message: "" });
      expect(root.children.length).toBe(0);
    });

    it("renders message with tone class", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderBanner(root, { tone: "neg", message: "DLQ has 3 failed tasks" });
      const banner = root.querySelector(".banner");
      expect(banner?.classList.contains("banner--neg")).toBe(true);
      expect(banner?.textContent).toContain("DLQ has 3 failed tasks");
    });

    it("renders an action link when href provided", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderBanner(root, {
        tone: "warn",
        message: "Reconcile pending",
        action: { label: "Review", href: "/today#reconcile" },
      });
      const link = root.querySelector("a.banner-action");
      expect(link?.getAttribute("href")).toBe("/today#reconcile");
      expect(link?.textContent).toBe("Review");
    });
  });

  describe("FreshnessDot", () => {
    it("paints data-date and stale_days from /api/today/freshness", async () => {
      const dot = document.createElement("span");
      dot.id = "freshness-footer-dot";
      const txt = document.createElement("span");
      txt.id = "freshness-footer-text";
      document.body.appendChild(dot);
      document.body.appendChild(txt);

      const fakeFetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            ok: true,
            data: {
              data_date: "2026-05-01",
              stale_days: 1,
              band: "amber",
            },
          }),
          { status: 200 },
        ),
      );
      await mountFreshnessDot({ fetch: fakeFetch as unknown as typeof fetch });
      expect(dot.dataset.band).toBe("amber");
      expect(txt.textContent).toBe("2026-05-01 · 1d ago");
    });

    it("paints 'today' when stale_days <= 0", async () => {
      const dot = document.createElement("span");
      dot.id = "freshness-footer-dot";
      const txt = document.createElement("span");
      txt.id = "freshness-footer-text";
      document.body.appendChild(dot);
      document.body.appendChild(txt);

      const fakeFetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            ok: true,
            data: { data_date: "2026-05-02", stale_days: 0, band: "green" },
          }),
          { status: 200 },
        ),
      );
      await mountFreshnessDot({ fetch: fakeFetch as unknown as typeof fetch });
      expect(txt.textContent).toBe("2026-05-02 · today");
    });

    it("paints 'no data' when data_date is missing", async () => {
      const dot = document.createElement("span");
      dot.id = "freshness-footer-dot";
      const txt = document.createElement("span");
      txt.id = "freshness-footer-text";
      document.body.appendChild(dot);
      document.body.appendChild(txt);

      const fakeFetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ ok: true, data: { data_date: null } }),
          { status: 200 },
        ),
      );
      await mountFreshnessDot({ fetch: fakeFetch as unknown as typeof fetch });
      expect(dot.dataset.band).toBe("red");
      expect(txt.textContent).toBe("no data");
    });

    it("leaves '—' sentinel on network failure (never throws)", async () => {
      const dot = document.createElement("span");
      dot.id = "freshness-footer-dot";
      dot.dataset.band = "—";
      const txt = document.createElement("span");
      txt.id = "freshness-footer-text";
      txt.textContent = "—";
      document.body.appendChild(dot);
      document.body.appendChild(txt);

      const fakeFetch = vi.fn().mockRejectedValue(new Error("net down"));
      await expect(
        mountFreshnessDot({ fetch: fakeFetch as unknown as typeof fetch }),
      ).resolves.toBeUndefined();
      expect(dot.dataset.band).toBe("—");
      expect(txt.textContent).toBe("—");
    });

    it("no-ops when widget not on page (no #freshness-footer-dot)", async () => {
      const fakeFetch = vi.fn();
      await mountFreshnessDot({ fetch: fakeFetch as unknown as typeof fetch });
      expect(fakeFetch).not.toHaveBeenCalled();
    });
  });
});
