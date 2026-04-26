/**
 * Floating tooltip for `[data-info]` elements.
 *
 * Why a JS-positioned tooltip instead of pure CSS `::after`:
 * KPI cards and chart cards both clip overflow, which would cut off any
 * absolutely-positioned tooltip. We render a single floating element on
 * `body` and reposition it on hover so it can never be clipped.
 *
 * Also re-runs on dynamic content (charts/tables that load after JS init)
 * via a MutationObserver so per-page scripts don't have to call us back.
 */
(function () {
  let tip;

  function ensure() {
    if (tip) return tip;
    tip = document.createElement("div");
    tip.className = "info-tooltip";
    document.body.appendChild(tip);
    return tip;
  }

  function position(target) {
    const t = ensure();
    const text = target.getAttribute("data-info");
    if (!text) return;
    t.textContent = text;
    t.style.display = "block";
    // Force layout so we can measure the tooltip
    const tipRect = t.getBoundingClientRect();
    const r = target.getBoundingClientRect();
    let top = r.top - tipRect.height - 10;
    let left = r.left + r.width / 2 - tipRect.width / 2;
    if (top < 8) top = r.bottom + 10;          // flip below if no room above
    if (left < 8) left = 8;                     // clamp to viewport
    if (left + tipRect.width > window.innerWidth - 8) {
      left = window.innerWidth - tipRect.width - 8;
    }
    t.style.top = `${top}px`;
    t.style.left = `${left}px`;
  }

  function hide() {
    if (tip) tip.style.display = "none";
  }

  function bind(el) {
    if (el.__infoBound) return;
    el.__infoBound = true;
    el.addEventListener("mouseenter", () => position(el));
    el.addEventListener("mouseleave", hide);
    el.addEventListener("focus", () => position(el));
    el.addEventListener("blur", hide);
  }

  function scan() {
    document.querySelectorAll("[data-info]").forEach(bind);
  }

  function init() {
    scan();
    // Watch for dynamically-injected content (table rows etc.).
    const obs = new MutationObserver(() => scan());
    obs.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
