import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const FRONTEND = resolve(HERE, "..");

describe("Phase 8 Cycle 52 — frontend scaffolding", () => {
  // Note: the legacy static/ sync tests (tokens.css, app.css) were removed
  // in Phase 12 cutover — static/ was deleted; styles now live only in
  // frontend/src/styles/ and are no longer mirrored from a Flask static dir.

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
