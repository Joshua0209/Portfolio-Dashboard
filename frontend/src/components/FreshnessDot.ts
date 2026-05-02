// FreshnessDot — paints the global freshness widget mounted in the
// sidebar footer (#freshness-footer-dot + #freshness-footer-text).
// Phase 8 Cycle 56 port of static/js/freshness.js.
//
// Contract: never throws. Network failure leaves the '—' sentinel
// untouched so navigation never breaks.

export interface MountOptions {
  fetch?: typeof fetch;
  endpoint?: string;
}

interface FreshnessEnvelope {
  ok?: boolean;
  data?: {
    data_date?: string | null;
    stale_days?: number;
    band?: string;
  };
}

export const mountFreshnessDot = async (
  opts: MountOptions = {},
): Promise<void> => {
  const dot = document.getElementById("freshness-footer-dot");
  const txt = document.getElementById("freshness-footer-text");
  if (!dot || !txt) return;

  const fetchImpl = opts.fetch ?? globalThis.fetch.bind(globalThis);
  const endpoint = opts.endpoint ?? "/api/today/freshness";

  try {
    const res = await fetchImpl(endpoint, {
      headers: { Accept: "application/json" },
    });
    if (!res.ok) return;
    const body = (await res.json()) as FreshnessEnvelope;
    if (body?.ok !== true) return;
    const data = body.data ?? {};
    if (!data.data_date) {
      dot.dataset.band = "red";
      txt.textContent = "no data";
      return;
    }
    dot.dataset.band = data.band ?? "—";
    const days = data.stale_days ?? 0;
    const ageLabel = days <= 0 ? "today" : `${days}d ago`;
    txt.textContent = `${data.data_date} · ${ageLabel}`;
  } catch {
    // Keep the '—' sentinel; do not throw.
    dot.dataset.band = "—";
    txt.textContent = "—";
  }
};
