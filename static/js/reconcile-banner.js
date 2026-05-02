// Phase 12 — global reconciliation banner.
//
// Polls /api/today/reconcile once on DOMContentLoaded and renders if
// there's at least one open event. Per spec §6.4 the "View details"
// link deep-links to /today#developer-tools so the accordion auto-
// expands; clicking dismiss POSTs to the admin endpoint and hides
// the banner without a page reload.

(function () {
  // Two event flavors share the reconcile_events table:
  //  - Legacy PDF-vs-overlay diff (run_for_month):
  //      payload has only_in_pdf_count / only_in_overlay_count.
  //  - Phase 11 audit hook (broker_pdf_buy_leg_mismatch):
  //      payload has event_type + sdk_leg_count/pdf_trade_count + code.
  // The banner shows a single line summarizing the top event; deep-link
  // to /today#developer-tools surfaces the full payload for triage.
  function formatBannerText(events, top) {
    const count = events.length;
    const plural = count === 1 ? "" : "s";
    const prefix = ` ${count} unresolved alert${plural} `;

    if (top.event_type === "broker_pdf_buy_leg_mismatch") {
      const sdk = Number(top.sdk_leg_count) || 0;
      const pdf = Number(top.pdf_trade_count) || 0;
      const code = top.code || "?";
      return (
        prefix +
        `(latest: ${top.pdf_month}, ${code} broker shows ${sdk} buy leg${
          sdk === 1 ? "" : "s"
        } vs PDF ${pdf}).`
      );
    }

    // Legacy diff event
    const total =
      (Number(top.only_in_pdf_count) || 0) +
      (Number(top.only_in_overlay_count) || 0);
    return (
      prefix +
      `(latest: ${top.pdf_month}, ${total} differing trade${
        total === 1 ? "" : "s"
      }).`
    );
  }

  function activate() {
    const root = document.getElementById("reconcile-banner");
    const textEl = document.getElementById("reconcile-banner-text");
    const dismissBtn = document.getElementById("reconcile-banner-dismiss");
    if (!root || !textEl || !dismissBtn) return;

    fetch("/api/today/reconcile", { headers: { Accept: "application/json" } })
      .then((res) => (res.ok ? res.json() : null))
      .then((body) => {
        if (!body || body.ok !== true) return;
        const events = (body.data && body.data.events) || [];
        if (events.length === 0) return;

        const top = events[0];
        textEl.textContent = formatBannerText(events, top);
        root.dataset.eventId = String(top.id);
        root.hidden = false;
      })
      .catch(() => {
        /* network failure → silently leave banner hidden */
      });

    dismissBtn.addEventListener("click", () => {
      const id = root.dataset.eventId;
      if (!id) return;
      fetch(`/api/admin/reconcile/${id}/dismiss`, { method: "POST" })
        .then((res) => {
          // Fail-closed: only hide on a confirmed server-side dismiss.
          // A network or 5xx error must leave the banner visible because
          // it signals data-integrity divergence (PDF vs overlay), not
          // staleness. Hiding on transient errors would mask a real
          // unresolved reconciliation event.
          if (res.ok) {
            root.hidden = true;
          }
        })
        .catch(() => {
          /* network failure → leave banner visible (fail-closed) */
        });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", activate);
  } else {
    activate();
  }
})();
