/**
 * Number / currency / date formatters used across the dashboard.
 * All functions return strings; never null. Missing values render as "—".
 */
(function (global) {
  const EM_DASH = "—";

  /** @param {unknown} v */
  function toFinite(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = typeof v === "number" ? v : Number(v);
    return Number.isFinite(n) ? n : null;
  }

  /** @param {number} n @param {number} [decimals] */
  function nf(n, decimals = 0) {
    return n.toLocaleString("en-US", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }

  /** Format TWD with thousand separators. */
  function twd(v, decimals = 0) {
    const n = toFinite(v);
    if (n === null) return EM_DASH;
    const sign = n < 0 ? "-NT$" : "NT$";
    return sign + nf(Math.abs(n), decimals);
  }

  /** Compact TWD: 1,234,567 -> NT$1.23M */
  function twdCompact(v) {
    const n = toFinite(v);
    if (n === null) return EM_DASH;
    const abs = Math.abs(n);
    const sign = n < 0 ? "-" : "";
    if (abs >= 1e9) return `${sign}NT$${(abs / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${sign}NT$${(abs / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) return `${sign}NT$${(abs / 1e3).toFixed(1)}k`;
    return `${sign}NT$${abs.toFixed(0)}`;
  }

  function usd(v, decimals = 2) {
    const n = toFinite(v);
    if (n === null) return EM_DASH;
    const sign = n < 0 ? "-$" : "$";
    return sign + nf(Math.abs(n), decimals);
  }

  function int(v) {
    const n = toFinite(v);
    return n === null ? EM_DASH : nf(n, 0);
  }

  function num(v, decimals = 2) {
    const n = toFinite(v);
    return n === null ? EM_DASH : nf(n, decimals);
  }

  /** Percent with sign. 0.0123 -> +1.23% */
  function pct(v, decimals = 2) {
    const n = toFinite(v);
    if (n === null) return EM_DASH;
    const sign = n > 0 ? "+" : "";
    return `${sign}${(n * 100).toFixed(decimals)}%`;
  }

  /** Unsigned percent. 0.0123 -> 1.23% */
  function pctAbs(v, decimals = 2) {
    const n = toFinite(v);
    return n === null ? EM_DASH : `${(n * 100).toFixed(decimals)}%`;
  }

  /** Pretty month "2025-08" -> "Aug 2025". */
  function month(s) {
    if (!s) return EM_DASH;
    const m = String(s).match(/^(\d{4})-(\d{2})/);
    if (!m) return s;
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const mi = parseInt(m[2], 10) - 1;
    return `${months[mi] || m[2]} ${m[1]}`;
  }

  /** Resolution-aware label. Reads `row.date` first (daily branch),
   * falls back to `row.month` (monthly), then formats appropriately:
   * ISO daily date → "Aug 22, 2026"; YYYY-MM → "Aug 2025". */
  function label(row) {
    if (!row) return EM_DASH;
    if (row.date) {
      // ISO YYYY-MM-DD → short date label
      const m = String(row.date).match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (m) {
        const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        return `${months[parseInt(m[2], 10) - 1] || m[2]} ${parseInt(m[3], 10)}, ${m[1]}`;
      }
      return row.date;
    }
    return month(row.month);
  }

  /** Class name for positive/negative/neutral coloring. */
  function tone(v) {
    const n = toFinite(v);
    if (n === null || n === 0) return "value-mute";
    return n > 0 ? "value-pos" : "value-neg";
  }

  /** "2026/03/12" -> "Mar 12, 2026" */
  function date(s) {
    if (!s) return EM_DASH;
    const m = String(s).match(/^(\d{4})\/(\d{2})\/(\d{2})/);
    if (!m) return s;
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${months[parseInt(m[2], 10) - 1] || m[2]} ${parseInt(m[3], 10)}, ${m[1]}`;
  }

  global.fmt = { twd, twdCompact, usd, int, num, pct, pctAbs, month, label, tone, date };
})(window);
