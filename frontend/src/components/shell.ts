// App shell — sidebar + content frame + page outlet.
// Phase 8 Cycle 55 port of templates/base.html.
//
// Same DOM hooks as the legacy template:
//   .sidebar, .nav, a.nav-link, #theme-toggle, #as-of, #freshness-footer
//   .content, .page-header, .subtitle, .page-actions, #page (outlet)
// app.css already targets these — keeping the IDs/classes identical means
// the visual parity check in Phase 9 passes by construction.

import { toggleTheme } from "../lib/theme";

interface NavEntry {
  href: string;
  page: string;
  glyph: string;
  label: string;
}

interface NavSection {
  title?: string;
  entries: readonly NavEntry[];
}

const NAV: readonly NavSection[] = [
  {
    entries: [
      { href: "/", page: "overview", glyph: "◐", label: "Overview" },
      { href: "/today", page: "today", glyph: "⊙", label: "Today" },
      { href: "/holdings", page: "holdings", glyph: "▤", label: "Holdings" },
      {
        href: "/performance",
        page: "performance",
        glyph: "↗",
        label: "Performance",
      },
      { href: "/risk", page: "risk", glyph: "▽", label: "Risk" },
      {
        href: "/benchmark",
        page: "benchmark",
        glyph: "⊥",
        label: "Benchmark",
      },
      { href: "/fx", page: "fx", glyph: "₱", label: "Currency" },
    ],
  },
  {
    title: "Activity",
    entries: [
      {
        href: "/transactions",
        page: "transactions",
        glyph: "⇄",
        label: "Transactions",
      },
      {
        href: "/cashflows",
        page: "cashflows",
        glyph: "⤳",
        label: "Cashflows",
      },
      {
        href: "/dividends",
        page: "dividends",
        glyph: "◇",
        label: "Dividends",
      },
      { href: "/tax", page: "tax", glyph: "∑", label: "Tax / P&L" },
    ],
  },
];

export interface Shell {
  setActivePage: (page: string | null) => void;
  outlet: HTMLElement;
}

const el = (
  tag: string,
  attrs: Record<string, string> = {},
  text?: string,
): HTMLElement => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (text !== undefined) node.textContent = text;
  return node;
};

const renderNav = (): HTMLElement => {
  const nav = el("nav", { class: "nav" });
  for (const section of NAV) {
    if (section.title) {
      nav.appendChild(el("div", { class: "nav-section" }, section.title));
    }
    for (const entry of section.entries) {
      const a = el("a", {
        href: entry.href,
        class: "nav-link",
        "data-page": entry.page,
      });
      a.appendChild(el("span", { class: "nav-ico" }, entry.glyph));
      a.appendChild(document.createTextNode(` ${entry.label}`));
      nav.appendChild(a);
    }
  }
  return nav;
};

const renderSidebar = (): HTMLElement => {
  const aside = el("aside", { class: "sidebar" });

  const brand = el("div", { class: "brand" });
  brand.appendChild(el("span", { class: "brand-mark" }, "永"));
  const brandText = el("div", { class: "brand-text" });
  brandText.appendChild(el("strong", {}, "Sinopac"));
  brandText.appendChild(el("small", {}, "Portfolio Lab"));
  brand.appendChild(brandText);
  aside.appendChild(brand);

  aside.appendChild(renderNav());

  const footer = el("div", { class: "sidebar-footer" });
  const themeBtn = el(
    "button",
    {
      id: "theme-toggle",
      class: "theme-btn",
      title: "Toggle theme",
      type: "button",
    },
    "◑",
  );
  themeBtn.addEventListener("click", () => {
    toggleTheme();
  });
  footer.appendChild(themeBtn);
  footer.appendChild(el("span", { class: "as-of", id: "as-of" }, "…"));
  const freshness = el("span", {
    id: "freshness-footer",
    class: "freshness-footer",
    title: "Daily-data freshness",
  });
  freshness.appendChild(
    el("span", {
      id: "freshness-footer-dot",
      class: "freshness-footer__dot",
      "data-band": "—",
    }),
  );
  freshness.appendChild(
    el(
      "span",
      { id: "freshness-footer-text", class: "freshness-footer__text" },
      "—",
    ),
  );
  footer.appendChild(freshness);
  aside.appendChild(footer);

  return aside;
};

const renderContent = (): { content: HTMLElement; outlet: HTMLElement } => {
  const main = el("main", { class: "content" });

  // Banner mounts (filled by reconcile-banner / dlq-banner in later cycles).
  const reconcile = el("div", { id: "reconcile-banner-mount" });
  const dlq = el("div", { id: "dlq-banner-mount" });
  main.appendChild(reconcile);
  main.appendChild(dlq);

  const header = el("header", { class: "page-header" });
  const headerLeft = el("div");
  headerLeft.appendChild(el("h1", { id: "page-title" }, "Dashboard"));
  headerLeft.appendChild(el("p", { class: "subtitle", id: "page-subtitle" }));
  header.appendChild(headerLeft);
  header.appendChild(el("div", { class: "page-actions", id: "page-actions" }));
  main.appendChild(header);

  const outlet = el("div", { id: "page" });
  main.appendChild(outlet);

  return { content: main, outlet };
};

export const renderShell = (root: HTMLElement): Shell => {
  while (root.firstChild) root.removeChild(root.firstChild);
  root.appendChild(renderSidebar());
  const { content, outlet } = renderContent();
  root.appendChild(content);

  const setActivePage = (page: string | null): void => {
    const links = document.querySelectorAll<HTMLAnchorElement>("a.nav-link");
    links.forEach((link) => {
      const matches = page !== null && link.dataset.page === page;
      link.classList.toggle("active", matches);
    });
  };

  return { setActivePage, outlet };
};
