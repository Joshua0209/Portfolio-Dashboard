// Tiny pushState router. Phase 8 Cycle 55 — keeps the page-per-file
// shape from the legacy Flask app but lifts page transitions into a
// single SPA so chart.js instances and the resolution probe survive
// across navigations.

export interface RouteContext {
  params: Record<string, string>;
  query: URLSearchParams;
}

export type MountFn = (
  outlet: HTMLElement,
  ctx: RouteContext,
) => void | Promise<void>;

export interface Route {
  path: string;
  page: string;
  mount: MountFn;
}

export interface Router {
  start: () => void;
  navigate: (path: string, opts?: { replace?: boolean }) => void;
  current: () => string;
}

export interface CreateRouterOptions {
  outlet: HTMLElement;
  routes: readonly Route[];
  onRouteChange?: (page: string | null) => void;
}

interface CompiledRoute extends Route {
  matcher: RegExp;
  paramNames: readonly string[];
}

const compileRoute = (
  route: Route,
): { matcher: RegExp; paramNames: readonly string[] } => {
  const names: string[] = [];
  const escaped = route.path
    .split("/")
    .map((seg) => {
      if (seg.startsWith(":")) {
        names.push(seg.slice(1));
        return "([^/]+)";
      }
      return seg.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    })
    .join("/");
  return { matcher: new RegExp(`^${escaped}/?$`), paramNames: names };
};

const renderNotFound = (outlet: HTMLElement): void => {
  while (outlet.firstChild) outlet.removeChild(outlet.firstChild);
  const wrap = document.createElement("div");
  wrap.className = "page-empty";
  wrap.textContent = "Page not found";
  outlet.appendChild(wrap);
};

export const createRouter = (opts: CreateRouterOptions): Router => {
  const compiled: CompiledRoute[] = opts.routes.map((r) => ({
    ...r,
    ...compileRoute(r),
  }));

  const match = (
    path: string,
  ): { route: CompiledRoute; params: Record<string, string> } | null => {
    for (const r of compiled) {
      const m = r.matcher.exec(path);
      if (!m) continue;
      const params: Record<string, string> = {};
      r.paramNames.forEach((n, i) => {
        params[n] = decodeURIComponent(m[i + 1] ?? "");
      });
      return { route: r, params };
    }
    return null;
  };

  const render = async (): Promise<void> => {
    const { pathname, search } = window.location;
    const found = match(pathname);
    if (!found) {
      renderNotFound(opts.outlet);
      opts.onRouteChange?.(null);
      return;
    }
    while (opts.outlet.firstChild) {
      opts.outlet.removeChild(opts.outlet.firstChild);
    }
    opts.onRouteChange?.(found.route.page);
    await found.route.mount(opts.outlet, {
      params: found.params,
      query: new URLSearchParams(search),
    });
  };

  const navigate = (path: string, navOpts?: { replace?: boolean }): void => {
    if (navOpts?.replace) {
      history.replaceState(null, "", path);
    } else {
      history.pushState(null, "", path);
    }
    void render();
  };

  const start = (): void => {
    window.addEventListener("popstate", () => {
      void render();
    });
    void render();
  };

  return {
    start,
    navigate,
    current: () => window.location.pathname,
  };
};
