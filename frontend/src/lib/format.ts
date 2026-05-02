// Number / currency / date formatters used across the dashboard.
// All functions return strings; never null. Missing values render as EM_DASH.
// Phase 8 Cycle 54 port of static/js/format.js — behavior preserved verbatim.

export const EM_DASH = "—";

const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

const toFinite = (v: unknown): number | null => {
  if (v === null || v === undefined || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
};

const nf = (n: number, decimals = 0): string =>
  n.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });

export const twd = (v: unknown, decimals = 0): string => {
  const n = toFinite(v);
  if (n === null) return EM_DASH;
  const sign = n < 0 ? "-NT$" : "NT$";
  return sign + nf(Math.abs(n), decimals);
};

export const twdCompact = (v: unknown): string => {
  const n = toFinite(v);
  if (n === null) return EM_DASH;
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}NT$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}NT$${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}NT$${(abs / 1e3).toFixed(1)}k`;
  return `${sign}NT$${abs.toFixed(0)}`;
};

export const usd = (v: unknown, decimals = 2): string => {
  const n = toFinite(v);
  if (n === null) return EM_DASH;
  const sign = n < 0 ? "-$" : "$";
  return sign + nf(Math.abs(n), decimals);
};

export const int = (v: unknown): string => {
  const n = toFinite(v);
  return n === null ? EM_DASH : nf(n, 0);
};

export const num = (v: unknown, decimals = 2): string => {
  const n = toFinite(v);
  return n === null ? EM_DASH : nf(n, decimals);
};

export const pct = (v: unknown, decimals = 2): string => {
  const n = toFinite(v);
  if (n === null) return EM_DASH;
  const sign = n > 0 ? "+" : "";
  return `${sign}${(n * 100).toFixed(decimals)}%`;
};

export const pctAbs = (v: unknown, decimals = 2): string => {
  const n = toFinite(v);
  return n === null ? EM_DASH : `${(n * 100).toFixed(decimals)}%`;
};

export const month = (s: unknown): string => {
  if (s === null || s === undefined || s === "") return EM_DASH;
  const str = String(s);
  const m = str.match(/^(\d{4})-(\d{2})/);
  if (!m) return str;
  const mi = parseInt(m[2]!, 10) - 1;
  return `${MONTHS[mi] ?? m[2]} ${m[1]}`;
};

export const date = (s: unknown): string => {
  if (s === null || s === undefined || s === "") return EM_DASH;
  const str = String(s);
  const m = str.match(/^(\d{4})\/(\d{2})\/(\d{2})/);
  if (!m) return str;
  return `${MONTHS[parseInt(m[2]!, 10) - 1] ?? m[2]} ${parseInt(m[3]!, 10)}, ${m[1]}`;
};

export interface LabelRow {
  date?: string | null;
  month?: string | null;
}

export const label = (row: LabelRow | null | undefined): string => {
  if (!row) return EM_DASH;
  if (row.date) {
    const m = String(row.date).match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) {
      return `${MONTHS[parseInt(m[2]!, 10) - 1] ?? m[2]} ${parseInt(m[3]!, 10)}, ${m[1]}`;
    }
    return row.date;
  }
  return month(row.month);
};

export const tone = (v: unknown): "value-pos" | "value-neg" | "value-mute" => {
  const n = toFinite(v);
  if (n === null || n === 0) return "value-mute";
  return n > 0 ? "value-pos" : "value-neg";
};
