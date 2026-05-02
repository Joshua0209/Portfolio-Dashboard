import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const FRONTEND = resolve(HERE, "..");
const REPO = resolve(FRONTEND, "..");

describe("Phase 8 Cycle 52 — frontend scaffolding", () => {
  it("tokens.css is copied verbatim from static/", () => {
    const legacy = readFileSync(resolve(REPO, "static/css/tokens.css"), "utf8");
    const current = readFileSync(
      resolve(FRONTEND, "src/styles/tokens.css"),
      "utf8",
    );
    expect(current).toBe(legacy);
  });

  it("app.css is copied verbatim from static/", () => {
    const legacy = readFileSync(resolve(REPO, "static/css/app.css"), "utf8");
    const current = readFileSync(
      resolve(FRONTEND, "src/styles/app.css"),
      "utf8",
    );
    expect(current).toBe(legacy);
  });

  it("index.html links the design-system stylesheets", () => {
    const html = readFileSync(resolve(FRONTEND, "index.html"), "utf8");
    expect(html).toContain("/src/styles/tokens.css");
    expect(html).toContain("/src/styles/app.css");
    expect(html).toContain('data-theme="dark"');
  });

  it("entry module exports an init() that returns void", async () => {
    const mod = (await import(resolve(FRONTEND, "src/main.ts"))) as {
      init: () => void;
    };
    expect(typeof mod.init).toBe("function");
  });
});
