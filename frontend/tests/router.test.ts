import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createRouter } from "../src/lib/router";
import type { Route } from "../src/lib/router";

const clearDom = (): void => {
  while (document.body.firstChild) {
    document.body.removeChild(document.body.firstChild);
  }
};

describe("Phase 8 Cycle 55 — pushState router", () => {
  let outlet: HTMLElement;

  beforeEach(() => {
    outlet = document.createElement("div");
    outlet.id = "page";
    document.body.appendChild(outlet);
    history.replaceState(null, "", "/");
  });

  afterEach(() => {
    clearDom();
  });

  it("renders the matching route on start()", async () => {
    const overview = vi.fn();
    const routes: Route[] = [{ path: "/", page: "overview", mount: overview }];
    const router = createRouter({ outlet, routes });
    router.start();
    await Promise.resolve();
    expect(overview).toHaveBeenCalledTimes(1);
    expect(overview).toHaveBeenCalledWith(outlet, expect.objectContaining({}));
  });

  it("renders 'not found' fallback when no route matches", async () => {
    const router = createRouter({ outlet, routes: [] });
    history.replaceState(null, "", "/nope");
    router.start();
    await Promise.resolve();
    expect(outlet.textContent).toContain("not found");
  });

  it("navigate() pushes state and re-renders", async () => {
    const overview = vi.fn();
    const holdings = vi.fn();
    const routes: Route[] = [
      { path: "/", page: "overview", mount: overview },
      { path: "/holdings", page: "holdings", mount: holdings },
    ];
    const router = createRouter({ outlet, routes });
    router.start();
    await Promise.resolve();
    router.navigate("/holdings");
    await Promise.resolve();
    expect(holdings).toHaveBeenCalledTimes(1);
    expect(window.location.pathname).toBe("/holdings");
  });

  it("matches dynamic routes — /ticker/:code passes the param", async () => {
    const ticker = vi.fn();
    const routes: Route[] = [
      { path: "/ticker/:code", page: "ticker", mount: ticker },
    ];
    history.replaceState(null, "", "/ticker/2330");
    const router = createRouter({ outlet, routes });
    router.start();
    await Promise.resolve();
    expect(ticker).toHaveBeenCalledWith(
      outlet,
      expect.objectContaining({ params: { code: "2330" } }),
    );
  });

  it("popstate triggers re-render", async () => {
    const overview = vi.fn();
    const holdings = vi.fn();
    const routes: Route[] = [
      { path: "/", page: "overview", mount: overview },
      { path: "/holdings", page: "holdings", mount: holdings },
    ];
    const router = createRouter({ outlet, routes });
    router.start();
    await Promise.resolve();
    router.navigate("/holdings");
    await Promise.resolve();
    history.back();
    window.dispatchEvent(new PopStateEvent("popstate"));
    await Promise.resolve();
    expect(overview).toHaveBeenCalledTimes(2);
  });
});
