import { describe, expect, it } from "vitest";
import { hexWithAlpha, palette, cssVar } from "../src/lib/charts";

describe("Phase 8 Cycle 54 — chart helpers (hex/palette only)", () => {
  describe("hexWithAlpha", () => {
    it("expands 3-digit hex to rgba", () => {
      expect(hexWithAlpha("#abc", 0.5)).toBe("rgba(170, 187, 204, 0.5)");
    });
    it("converts 6-digit hex to rgba", () => {
      expect(hexWithAlpha("#d4a45c", 0.25)).toBe("rgba(212, 164, 92, 0.25)");
    });
    it("rewrites alpha on existing rgba()", () => {
      expect(hexWithAlpha("rgba(1, 2, 3, 0.99)", 0.1)).toBe(
        "rgba(1, 2, 3, 0.1)",
      );
    });
    it("upgrades rgb() to rgba()", () => {
      expect(hexWithAlpha("rgb(10, 20, 30)", 0.4)).toBe(
        "rgba(10, 20, 30, 0.4)",
      );
    });
    it("trims whitespace before parsing", () => {
      expect(hexWithAlpha("  #fff  ", 1)).toBe("rgba(255, 255, 255, 1)");
    });
    it("echoes unknown formats unchanged", () => {
      expect(hexWithAlpha("oklch(50% 0 0)", 0.5)).toBe("oklch(50% 0 0)");
    });
  });

  describe("cssVar", () => {
    it("returns whatever getComputedStyle reports, trimmed", () => {
      // happy-dom returns '' for undefined custom props; trim() is the test.
      document.documentElement.style.setProperty("--test-color", "  #abc  ");
      expect(cssVar("--test-color")).toBe("#abc");
      document.documentElement.style.removeProperty("--test-color");
    });
    it("returns empty string when var is unset", () => {
      expect(cssVar("--definitely-not-set")).toBe("");
    });
  });

  describe("palette", () => {
    it("reads --c1..--c8 in order", () => {
      for (let i = 1; i <= 8; i++) {
        document.documentElement.style.setProperty(`--c${i}`, `#00000${i}`);
      }
      const p = palette();
      expect(p).toEqual([
        "#000001",
        "#000002",
        "#000003",
        "#000004",
        "#000005",
        "#000006",
        "#000007",
        "#000008",
      ]);
      for (let i = 1; i <= 8; i++) {
        document.documentElement.style.removeProperty(`--c${i}`);
      }
    });
  });
});
