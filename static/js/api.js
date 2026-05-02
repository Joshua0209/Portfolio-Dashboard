/**
 * Tiny fetch helper for the dashboard API.
 *
 * Phase 4: at page load, probe /api/health once. If the daily layer
 * reports state == "READY", set RESOLUTION = "daily" so every subsequent
 * fetch helper appends `?resolution=daily` automatically — no per-page
 * opt-in needed. If state != "READY", we leave the param off and the
 * backend serves the existing monthly response (backwards compatible).
 *
 * Phase 9: /api/daily/* endpoints can return 202 with a progress envelope
 * while the backfill thread is running. `get()` recognizes 202 and
 * retries with exponential backoff (capped at 10s, max ~2min total —
 * matches the spec §6.4 timeout banner trigger). Callers can pass
 * `{ on_warming }` to surface progress in the UI between polls.
 *
 * 503 (FAILED state) propagates as an error so pages can deep-link to
 * the Developer Tools accordion (Phase 10).
 *
 * The probe is one-shot, not polling. A user with a long-lived tab won't
 * auto-flip to daily after backfill completes mid-session — that's
 * acceptable for v1; the next hard refresh handles it.
 */
(function (global) {
  let RESOLUTION = "monthly";
  let resolutionReady = null;

  const WARMING_TIMEOUT_MS = 120_000; // 2 min per spec §6.4
  const WARMING_INITIAL_DELAY_MS = 500;
  const WARMING_MAX_DELAY_MS = 10_000;

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

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function fetchWithWarmupRetry(url, opts, onWarming) {
    const start = Date.now();
    let delay = WARMING_INITIAL_DELAY_MS;
    while (true) {
      const res = await fetch(url, opts);
      if (res.status !== 202) return res;
      // 202 → backfill in flight; pull progress, surface to caller, back off.
      try {
        const body = await res.json();
        if (typeof onWarming === "function") {
          onWarming(body && body.data ? body.data : {});
        }
      } catch (_) {
        // Ignore — proceed with the backoff regardless.
      }
      if (Date.now() - start > WARMING_TIMEOUT_MS) {
        const err = new Error("warming timeout");
        err.warming = true;
        err.timeout = true;
        throw err;
      }
      await sleep(delay);
      delay = Math.min(delay * 2, WARMING_MAX_DELAY_MS);
    }
  }

  async function get(path, params, opts) {
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
    const res = await fetchWithWarmupRetry(
      url,
      { headers: { Accept: "application/json" } },
      opts && opts.on_warming
    );
    if (!res.ok) {
      let bodyText = "";
      try { bodyText = await res.text(); } catch (_) { /* ignore */ }
      const err = new Error(`HTTP ${res.status} from ${path}`);
      err.status = res.status;
      err.body = bodyText;
      throw err;
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
