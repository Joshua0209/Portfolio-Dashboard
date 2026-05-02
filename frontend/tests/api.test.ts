import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createApi } from "../src/lib/api";
import type { ApiClient } from "../src/lib/api";

type FetchStub = ReturnType<typeof vi.fn>;

const okJson = (data: unknown, init: ResponseInit = {}) =>
  new Response(JSON.stringify({ ok: true, data }), {
    status: 200,
    headers: { "content-type": "application/json" },
    ...init,
  });

const status = (
  code: number,
  body: unknown,
  init: ResponseInit = {},
): Response =>
  new Response(JSON.stringify(body), {
    status: code,
    headers: { "content-type": "application/json" },
    ...init,
  });

describe("Phase 8 Cycle 53 — typed API client", () => {
  let fetchStub: FetchStub;
  let api: ApiClient;

  beforeEach(() => {
    fetchStub = vi.fn();
    vi.useFakeTimers();
    api = createApi({
      fetch: fetchStub as unknown as typeof fetch,
      baseUrl: "http://api.test",
      sleep: () => Promise.resolve(),
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("envelope unwrap", () => {
    it("unwraps {ok:true, data} envelope on 200", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ months_loaded: 12 }));
      fetchStub.mockResolvedValueOnce(okJson({ value: 42 }));
      const out = await api.get<{ value: number }>("/api/x");
      expect(out).toEqual({ value: 42 });
    });

    it("throws ApiError on {ok:false} body", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(
        new Response(JSON.stringify({ ok: false, error: "boom" }), {
          status: 200,
        }),
      );
      await expect(api.get("/api/x")).rejects.toThrow("boom");
    });

    it("throws on HTTP non-2xx with status preserved", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(status(500, { detail: "kaboom" }));
      const err = await api.get("/api/x").catch((e: Error) => e);
      expect(err).toBeInstanceOf(Error);
      expect((err as Error & { status?: number }).status).toBe(500);
    });
  });

  describe("resolution probe (one-shot)", () => {
    it("appends ?resolution=daily after health reports READY", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(okJson({ value: 1 }));
      await api.get("/api/summary");
      const url = (fetchStub.mock.calls[1]?.[0] as string) ?? "";
      expect(url).toContain("resolution=daily");
    });

    it("does NOT append resolution when health is INITIALIZING", async () => {
      fetchStub.mockResolvedValueOnce(
        okJson({ daily_state: "INITIALIZING" }),
      );
      fetchStub.mockResolvedValueOnce(okJson({ value: 1 }));
      await api.get("/api/summary");
      const url = (fetchStub.mock.calls[1]?.[0] as string) ?? "";
      expect(url).not.toContain("resolution=");
    });

    it("never appends resolution to /api/daily/* paths", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(okJson({ rows: [] }));
      await api.get("/api/daily/equity");
      const url = (fetchStub.mock.calls[1]?.[0] as string) ?? "";
      expect(url).not.toContain("resolution=");
    });

    it("survives health probe network error and stays on monthly", async () => {
      fetchStub.mockRejectedValueOnce(new Error("net down"));
      fetchStub.mockResolvedValueOnce(okJson({ value: 1 }));
      const out = await api.get("/api/summary");
      expect(out).toEqual({ value: 1 });
      const url = (fetchStub.mock.calls[1]?.[0] as string) ?? "";
      expect(url).not.toContain("resolution=");
    });
  });

  describe("202 warmup retry", () => {
    it("retries 202 then resolves with data on next 200", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(
        status(202, { ok: true, data: { progress: 42 } }),
      );
      fetchStub.mockResolvedValueOnce(okJson({ rows: [1, 2] }));
      const out = await api.get<{ rows: number[] }>("/api/daily/equity");
      expect(out).toEqual({ rows: [1, 2] });
      // health + first 202 + second success
      expect(fetchStub).toHaveBeenCalledTimes(3);
    });

    it("invokes onWarming with progress payload between retries", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(
        status(202, { ok: true, data: { progress: 42 } }),
      );
      fetchStub.mockResolvedValueOnce(okJson({ rows: [] }));
      const onWarming = vi.fn();
      await api.get("/api/daily/equity", undefined, { onWarming });
      expect(onWarming).toHaveBeenCalledWith({ progress: 42 });
    });
  });

  describe("503 (FAILED) propagation", () => {
    it("throws and tags error with status=503", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "READY" }));
      fetchStub.mockResolvedValueOnce(
        status(503, { ok: false, error: "backfill_failed" }),
      );
      const err = await api
        .get("/api/daily/equity")
        .catch((e: Error) => e);
      expect((err as Error & { status?: number }).status).toBe(503);
    });
  });

  describe("query params", () => {
    it("appends params with stringification, skipping null/undefined/''", async () => {
      fetchStub.mockResolvedValueOnce(okJson({ daily_state: "INITIALIZING" }));
      fetchStub.mockResolvedValueOnce(okJson({ ok: true }));
      await api.get("/api/transactions", {
        venue: "TW",
        side: "buy",
        empty: "",
        nullish: null,
        und: undefined,
        page: 3,
      });
      const url = (fetchStub.mock.calls[1]?.[0] as string) ?? "";
      expect(url).toContain("venue=TW");
      expect(url).toContain("side=buy");
      expect(url).toContain("page=3");
      expect(url).not.toContain("empty=");
      expect(url).not.toContain("nullish=");
      expect(url).not.toContain("und=");
    });
  });
});
