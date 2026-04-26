// Phase 14 — global freshness widget.
//
// Fetches /api/today/freshness once on DOMContentLoaded and paints the
// footer dot + text. Network failure leaves the widget at its initial
// "—" state — never blocks page rendering, never throws.
//
// Color band → CSS-driven via the data-band attribute.

(function () {
  function paint() {
    const dot = document.getElementById("freshness-footer-dot");
    const txt = document.getElementById("freshness-footer-text");
    if (!dot || !txt) return; // widget not on this page (shouldn't happen)

    fetch("/api/today/freshness", { headers: { Accept: "application/json" } })
      .then((res) => (res.ok ? res.json() : null))
      .then((body) => {
        if (!body || body.ok !== true) return;
        const data = body.data || {};
        if (!data.data_date) {
          dot.dataset.band = "red";
          txt.textContent = "no data";
          return;
        }
        dot.dataset.band = data.band;
        const days = data.stale_days;
        const ageLabel = days <= 0 ? "today" : `${days}d ago`;
        txt.textContent = `${data.data_date} · ${ageLabel}`;
      })
      .catch(() => {
        // Network failure: keep the "—" sentinel; do not throw.
        dot.dataset.band = "—";
        txt.textContent = "—";
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", paint);
  } else {
    paint();
  }
})();
