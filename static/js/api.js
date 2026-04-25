/**
 * Tiny fetch helper for the dashboard API.
 * Always returns the unwrapped `data` payload, or throws.
 */
(function (global) {
  async function get(path, params) {
    const url = new URL(path, global.location.origin);
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

  global.api = { get };
})(window);
