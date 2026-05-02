import { describe, expect, it } from "vitest";
import {
  date,
  EM_DASH,
  int,
  label,
  month,
  num,
  pct,
  pctAbs,
  tone,
  twd,
  twdCompact,
  usd,
} from "../src/lib/format";

describe("Phase 8 Cycle 54 — format library", () => {
  describe("twd", () => {
    it("formats positive integer with NT$ prefix and thousand separators", () => {
      expect(twd(1234567)).toBe("NT$1,234,567");
    });
    it("formats negative with -NT$ prefix", () => {
      expect(twd(-1500)).toBe("-NT$1,500");
    });
    it("returns em-dash on null/undefined/empty", () => {
      expect(twd(null)).toBe(EM_DASH);
      expect(twd(undefined)).toBe(EM_DASH);
      expect(twd("")).toBe(EM_DASH);
    });
    it("returns em-dash on non-numeric string", () => {
      expect(twd("abc")).toBe(EM_DASH);
    });
    it("respects decimals", () => {
      expect(twd(1234.5, 2)).toBe("NT$1,234.50");
    });
  });

  describe("twdCompact", () => {
    it("billions → B with 2 decimals", () => {
      expect(twdCompact(2_500_000_000)).toBe("NT$2.50B");
    });
    it("millions → M with 2 decimals", () => {
      expect(twdCompact(1_234_567)).toBe("NT$1.23M");
    });
    it("thousands → k with 1 decimal", () => {
      expect(twdCompact(12_345)).toBe("NT$12.3k");
    });
    it("sub-thousand → integer", () => {
      expect(twdCompact(800)).toBe("NT$800");
    });
    it("negative billions get sign in front of NT$", () => {
      expect(twdCompact(-2_500_000_000)).toBe("-NT$2.50B");
    });
  });

  describe("usd / int / num", () => {
    it("usd default 2 decimals", () => {
      expect(usd(1234.5)).toBe("$1,234.50");
    });
    it("usd negative", () => {
      expect(usd(-99)).toBe("-$99.00");
    });
    it("int rounds to 0 decimals", () => {
      expect(int(1500.99)).toBe("1,501");
    });
    it("num custom decimals", () => {
      expect(num(0.123456, 4)).toBe("0.1235");
    });
    it("num em-dash on null", () => {
      expect(num(null)).toBe(EM_DASH);
    });
  });

  describe("pct / pctAbs", () => {
    it("pct adds + sign for positive", () => {
      expect(pct(0.0123)).toBe("+1.23%");
    });
    it("pct no sign for negative (negative number provides its own minus)", () => {
      expect(pct(-0.0123)).toBe("-1.23%");
    });
    it("pct no + on zero", () => {
      expect(pct(0)).toBe("0.00%");
    });
    it("pctAbs never adds sign", () => {
      expect(pctAbs(0.5)).toBe("50.00%");
    });
  });

  describe("month / date / label", () => {
    it("month '2026-04' → 'Apr 2026'", () => {
      expect(month("2026-04")).toBe("Apr 2026");
    });
    it("month em-dash on null", () => {
      expect(month(null)).toBe(EM_DASH);
    });
    it("month echoes input on malformed string", () => {
      expect(month("garbage")).toBe("garbage");
    });
    it("date '2026/03/12' → 'Mar 12, 2026'", () => {
      expect(date("2026/03/12")).toBe("Mar 12, 2026");
    });
    it("label prefers .date (ISO) → 'Mar 12, 2026'", () => {
      expect(label({ date: "2026-03-12" })).toBe("Mar 12, 2026");
    });
    it("label falls back to .month → 'Mar 2026'", () => {
      expect(label({ month: "2026-03" })).toBe("Mar 2026");
    });
    it("label em-dash on empty", () => {
      expect(label(null)).toBe(EM_DASH);
    });
  });

  describe("tone", () => {
    it("zero / null → value-mute", () => {
      expect(tone(0)).toBe("value-mute");
      expect(tone(null)).toBe("value-mute");
    });
    it("positive → value-pos", () => {
      expect(tone(1)).toBe("value-pos");
    });
    it("negative → value-neg", () => {
      expect(tone(-1)).toBe("value-neg");
    });
  });
});
