import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { renderShell } from "../src/components/shell";
import { initTheme, applyTheme } from "../src/lib/theme";

const clearDom = (): void => {
  while (document.body.firstChild) {
    document.body.removeChild(document.body.firstChild);
  }
};

describe("Phase 8 Cycle 55 — app shell", () => {
  beforeEach(() => {
    clearDom();
  });
  afterEach(() => {
    clearDom();
    localStorage.clear();
    document.documentElement.removeAttribute("data-theme");
  });

  describe("renderShell", () => {
    it("mounts sidebar + main + content outlet with same DOM IDs as legacy", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderShell(root);
      expect(document.querySelector(".sidebar")).toBeTruthy();
      expect(document.querySelector(".content")).toBeTruthy();
      expect(document.getElementById("page")).toBeTruthy();
      expect(document.getElementById("theme-toggle")).toBeTruthy();
      expect(document.getElementById("as-of")).toBeTruthy();
      expect(document.getElementById("freshness-footer")).toBeTruthy();
    });

    it("renders all 12 nav links with /href shapes the legacy sidebar exposes", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      renderShell(root);
      const expected = [
        "/",
        "/today",
        "/holdings",
        "/performance",
        "/risk",
        "/benchmark",
        "/fx",
        "/transactions",
        "/cashflows",
        "/dividends",
        "/tax",
      ];
      for (const href of expected) {
        const link = document.querySelector(`a.nav-link[href="${href}"]`);
        expect(link, `missing nav link ${href}`).toBeTruthy();
      }
    });

    it("setActivePage marks the matching link with .active", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      const shell = renderShell(root);
      shell.setActivePage("holdings");
      const active = document.querySelector("a.nav-link.active");
      expect(active?.getAttribute("href")).toBe("/holdings");
    });

    it("setActivePage(null) clears all active states", () => {
      const root = document.createElement("div");
      document.body.appendChild(root);
      const shell = renderShell(root);
      shell.setActivePage("holdings");
      shell.setActivePage(null);
      expect(document.querySelector("a.nav-link.active")).toBeNull();
    });
  });

  describe("theme", () => {
    it("applyTheme sets data-theme on <html> and persists to localStorage", () => {
      applyTheme("light");
      expect(document.documentElement.getAttribute("data-theme")).toBe("light");
      expect(localStorage.getItem("sinopac-theme")).toBe("light");
    });

    it("initTheme defaults to dark when nothing stored", () => {
      initTheme();
      expect(document.documentElement.getAttribute("data-theme")).toBe("dark");
    });

    it("initTheme restores stored theme", () => {
      localStorage.setItem("sinopac-theme", "light");
      initTheme();
      expect(document.documentElement.getAttribute("data-theme")).toBe("light");
    });
  });
});
