// Typed API client. Phase 8 Cycle 53 port of static/js/api.js with
// dependency injection so the tests run synchronously.
//
// Behaviour ported verbatim from the legacy module:
//   1. One-shot /api/health probe — if daily_state == "READY", every
//      subsequent /api/* request (except /api/daily/*) gets
//      ?resolution=daily appended. Probe network failure leaves the
//      client on monthly.
//   2. 202 backoff retry — backfill-warming responses are retried with
//      exponential backoff (500ms → 10s cap) up to 2 minutes total.
//      onWarming(payload) fires between retries so callers can render
//      progress without polling separately.
//   3. 503 propagates as Error with .status === 503 so banner code can
//      deep-link to /today#developer-tools (Phase 10 contract).

const WARMING_TIMEOUT_MS = 120_000;
const WARMING_INITIAL_DELAY_MS = 500;
const WARMING_MAX_DELAY_MS = 10_000;

export type Resolution = "monthly" | "daily";

export interface WarmingPayload {
  progress?: number;
  [k: string]: unknown;
}

export interface GetOptions {
  onWarming?: (payload: WarmingPayload) => void;
}

export type QueryParams = Record<
  string,
  string | number | boolean | null | undefined
>;

export interface ApiClient {
  get<T = unknown>(
    path: string,
    params?: QueryParams,
    opts?: GetOptions,
  ): Promise<T>;
  readonly resolution: Resolution;
}

export interface CreateApiOptions {
  fetch?: typeof fetch;
  baseUrl?: string;
  sleep?: (ms: number) => Promise<void>;
}

interface HealthEnvelope {
  ok?: boolean;
  data?: { daily_state?: string };
}

interface DataEnvelope<T> {
  ok?: boolean;
  data?: T;
  error?: string;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export function createApi(opts: CreateApiOptions = {}): ApiClient {
  const fetchImpl = opts.fetch ?? globalThis.fetch.bind(globalThis);
  const baseUrl = opts.baseUrl ?? "";
  const sleep = opts.sleep ?? defaultSleep;

  let resolution: Resolution = "monthly";
  let probePromise: Promise<void> | null = null;

  const buildUrl = (path: string, params?: QueryParams): string => {
    const url = new URL(path, baseUrl || "http://localhost");
    if (resolution !== "monthly" && !path.startsWith("/api/daily/")) {
      url.searchParams.set("resolution", resolution);
    }
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v === undefined || v === null || v === "") continue;
        url.searchParams.set(k, String(v));
      }
    }
    return baseUrl ? url.toString() : `${url.pathname}${url.search}`;
  };

  const probe = async (): Promise<void> => {
    if (probePromise) return probePromise;
    probePromise = (async () => {
      try {
        const res = await fetchImpl(`${baseUrl}/api/health`, {
          headers: { Accept: "application/json" },
        });
        if (!res.ok) return;
        const body = (await res.json()) as HealthEnvelope;
        if (body?.data?.daily_state === "READY") {
          resolution = "daily";
        }
      } catch {
        // Network failure → stay on monthly. Pages still work.
      }
    })();
    return probePromise;
  };

  const requestWithWarmupRetry = async (
    url: string,
    onWarming?: (payload: WarmingPayload) => void,
  ): Promise<Response> => {
    const start = Date.now();
    let delay = WARMING_INITIAL_DELAY_MS;
    while (true) {
      const res = await fetchImpl(url, {
        headers: { Accept: "application/json" },
      });
      if (res.status !== 202) return res;
      try {
        const body = (await res.json()) as DataEnvelope<WarmingPayload>;
        if (onWarming) onWarming(body?.data ?? {});
      } catch {
        // proceed to backoff regardless
      }
      if (Date.now() - start > WARMING_TIMEOUT_MS) {
        const err = new Error("warming timeout") as Error & {
          warming: boolean;
          timeout: boolean;
        };
        err.warming = true;
        err.timeout = true;
        throw err;
      }
      await sleep(delay);
      delay = Math.min(delay * 2, WARMING_MAX_DELAY_MS);
    }
  };

  const get = async <T>(
    path: string,
    params?: QueryParams,
    callOpts?: GetOptions,
  ): Promise<T> => {
    await probe();
    const url = buildUrl(path, params);
    const res = await requestWithWarmupRetry(url, callOpts?.onWarming);
    if (!res.ok) {
      let bodyText = "";
      try {
        bodyText = await res.text();
      } catch {
        // ignore
      }
      const err = new Error(`HTTP ${res.status} from ${path}`) as Error & {
        status: number;
        body: string;
      };
      err.status = res.status;
      err.body = bodyText;
      throw err;
    }
    const body = (await res.json()) as DataEnvelope<T>;
    if (body?.ok === false) {
      throw new Error(body.error ?? "API returned ok:false");
    }
    return body.data as T;
  };

  return {
    get,
    get resolution(): Resolution {
      return resolution;
    },
  };
}

// Default singleton for app code (real fetch, real timers).
export const api: ApiClient = createApi();
