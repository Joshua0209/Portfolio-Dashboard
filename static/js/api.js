/**
 * Tiny fetch helper for the dashboard API.
 *
 * Phase 4: at page load, probe /api/health once. If the daily layer
 * reports state == "READY", set RESOLUTION = "daily" so every subsequent
 * fetch helper appends `?resolution=daily` automatically — no per-page
 * opt-in needed. If state != "READY", we leave the param off and the
 * backend serves the existing monthly response (backwards compatible).
 *
 * The probe is one-shot, not polling. A user with a long-lived tab won't
 * auto-flip to daily after backfill completes mid-session — that's
 * acceptable for v1; the next hard refresh handles it.
 */
(function (global) {
  let RESOLUTION = "monthly";
  let resolutionReady = null;

  async function readyResolution() {
    if (resolutionReady) return resolutionReady;
    resolutionReady = (async () => {
      try {
        const res = await fetch("/api/health", {
          headers: { Accept: "application/json" },
        });
        if (!res.ok) return;
        const body = await res.json();
        if (body && body.data && body.data.daily_state === "READY") {
          RESOLUTION = "daily";
        }
      } catch (e) {
        // Network failure → leave RESOLUTION at "monthly". Pages still work.
      }
    })();
    return resolutionReady;
  }

  async function get(path, params) {
    await readyResolution();
    const url = new URL(path, global.location.origin);
    if (RESOLUTION !== "monthly" && !path.startsWith("/api/daily/")) {
      url.searchParams.set("resolution", RESOLUTION);
    }
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v === undefined || v === null || v === "") continue;
        url.searchParams.set(k, v);
      }
    }
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} from ${path}`);
    }
    const body = await res.json();
    if (body && body.ok === false) {
      throw new Error(body.error || "API returned ok:false");
    }
    return body.data;
  }

  global.api = {
    get,
    /** Lets pages introspect the resolution after a fetch (e.g. to render
     *  a "daily prices" badge). Read-only; mutate via the readyResolution
     *  one-shot only. */
    get resolution() {
      return RESOLUTION;
    },
  };
})(window);
