// RED reproducer for DataTable component — Phase 8 Cycle 58
// Pins the contract for src/components/DataTable.ts:
//   - Wraps an existing <table class="data" id="..."> in a card
//   - Inserts .dt-toolbar before .table-wrap, .dt-pager after
//   - Search input filters via cfg.searchKeys (case-insensitive)
//   - Filter <select>s key on cfg.filters[].key (or .id), with optional
//     custom predicate(row, value)
//   - th.sortable[data-key] click toggles sort dir, applies sorted-asc /
//     sorted-desc class for CSS arrow
//   - Numeric sort with null/undefined → -Infinity (sinks on desc)
//   - Pager slices to pageSize; "all" returns full set
//   - setRows(newRows) swaps data and resets pager to page 1
//   - Empty state renders <td.table-empty colspan=...> with cfg.emptyText
//   - Idempotent: re-mounting the same tableId removes prior toolbar/pager
//
// The DataTable runs entirely client-side over typed row objects T.
// Imports ../src/components/DataTable which does not yet exist.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mountDataTable } from "../src/components/DataTable";

interface Row {
  code: string;
  name: string;
  mv_twd: number;
  venue: "TW" | "Foreign";
  cost?: number | null;
}

const sampleRows: Row[] = [
  { code: "2330", name: "TSMC", mv_twd: 1_000_000, venue: "TW", cost: 800_000 },
  { code: "AAPL", name: "Apple", mv_twd: 500_000, venue: "Foreign", cost: 400_000 },
  { code: "0050", name: "ETF50", mv_twd: 200_000, venue: "TW", cost: null },
  { code: "TSLA", name: "Tesla", mv_twd: 300_000, venue: "Foreign", cost: 250_000 },
];

const makeTh = (
  key: string | null,
  text: string,
  extra?: string,
): HTMLTableCellElement => {
  const th = document.createElement("th");
  if (key) {
    th.classList.add("sortable");
    th.dataset.key = key;
  }
  if (extra) th.classList.add(...extra.split(" "));
  th.textContent = text;
  return th;
};

const setupDom = (): HTMLTableElement => {
  while (document.body.firstChild) {
    document.body.removeChild(document.body.firstChild);
  }
  const card = document.createElement("div");
  card.className = "card";
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  const table = document.createElement("table");
  table.className = "data";
  table.id = "test-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  headRow.append(
    makeTh("code", "Code"),
    makeTh(null, "Name"),
    makeTh("mv_twd", "MV (TWD)", "num"),
    makeTh(null, "Venue"),
  );
  thead.appendChild(headRow);
  const tbody = document.createElement("tbody");
  table.append(thead, tbody);

  wrap.appendChild(table);
  card.appendChild(wrap);
  document.body.appendChild(card);
  return table;
};

const td = (text: string): HTMLTableCellElement => {
  const c = document.createElement("td");
  c.textContent = text;
  return c;
};

const renderRow = (r: Row): HTMLTableCellElement[] => [
  td(r.code),
  td(r.name),
  td(String(r.mv_twd)),
  td(r.venue),
];

const tbodyText = (table: HTMLTableElement): string[] =>
  Array.from(table.querySelectorAll("tbody tr")).map((tr) =>
    Array.from(tr.querySelectorAll("td"))
      .map((c) => c.textContent ?? "")
      .join("|"),
  );

describe("Phase 8 Cycle 58 — DataTable", () => {
  beforeEach(() => {
    setupDom();
  });
  afterEach(() => {
    while (document.body.firstChild) {
      document.body.removeChild(document.body.firstChild);
    }
  });

  it("renders rows + toolbar + pager skeleton", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code", "name"],
      colspan: 4,
      row: renderRow,
    });
    const card = document.querySelector(".card")!;
    expect(card.querySelector(".dt-toolbar")).not.toBeNull();
    expect(card.querySelector(".dt-pager")).not.toBeNull();
    expect(card.querySelector(".dt-search")).not.toBeNull();
    expect(tbodyText(document.getElementById("test-table") as HTMLTableElement))
      .toHaveLength(4);
  });

  it("filters via search across searchKeys (case-insensitive)", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code", "name"],
      colspan: 4,
      row: renderRow,
    });
    const search = document.querySelector<HTMLInputElement>(".dt-search")!;
    search.value = "tsmc";
    search.dispatchEvent(new Event("input"));
    const lines = tbodyText(
      document.getElementById("test-table") as HTMLTableElement,
    );
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("2330");
  });

  it("sorts numeric column with null sinking on desc", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
      defaultSort: { key: "mv_twd", dir: "desc" },
    });
    const lines = tbodyText(
      document.getElementById("test-table") as HTMLTableElement,
    );
    expect(lines.map((l) => l.split("|")[0])).toEqual([
      "2330",
      "AAPL",
      "TSLA",
      "0050",
    ]);
  });

  it("sortable header click toggles direction and applies indicator class", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
      defaultSort: { key: "mv_twd", dir: "desc" },
    });
    const th = document.querySelector<HTMLTableCellElement>(
      'th[data-key="mv_twd"]',
    )!;
    expect(th.classList.contains("sorted-desc")).toBe(true);
    th.click();
    expect(th.classList.contains("sorted-asc")).toBe(true);
    expect(th.classList.contains("sorted-desc")).toBe(false);
  });

  it("filter select narrows by exact match on filter.key", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
      filters: [
        { id: "venue", label: "All venues", options: ["TW", "Foreign"], key: "venue" },
      ],
    });
    const sel = document.querySelector<HTMLSelectElement>(".dt-filter")!;
    sel.value = "TW";
    sel.dispatchEvent(new Event("change"));
    const lines = tbodyText(
      document.getElementById("test-table") as HTMLTableElement,
    );
    expect(lines).toHaveLength(2);
    expect(lines.every((l) => l.includes("TW"))).toBe(true);
  });

  it("filter custom predicate runs when provided", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
      filters: [
        {
          id: "profitable",
          label: "All",
          options: [{ value: "yes", label: "Profitable" }],
          predicate: (r, v) =>
            v !== "yes" ||
            (r.cost != null && r.mv_twd > r.cost),
        },
      ],
    });
    const sel = document.querySelector<HTMLSelectElement>(".dt-filter")!;
    sel.value = "yes";
    sel.dispatchEvent(new Event("change"));
    const lines = tbodyText(
      document.getElementById("test-table") as HTMLTableElement,
    );
    // 0050 (cost null) excluded; the other 3 all profit
    expect(lines).toHaveLength(3);
    expect(lines.some((l) => l.includes("0050"))).toBe(false);
  });

  it("pager limits to pageSize, 'all' shows full", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
      pageSize: 2,
    });
    const lines = tbodyText(
      document.getElementById("test-table") as HTMLTableElement,
    );
    expect(lines).toHaveLength(2);
    const sizeSel = document.querySelector<HTMLSelectElement>(
      ".pager .select",
    )!;
    sizeSel.value = "all";
    sizeSel.dispatchEvent(new Event("change"));
    expect(
      tbodyText(document.getElementById("test-table") as HTMLTableElement),
    ).toHaveLength(4);
  });

  it("renders empty state with colspan when no rows match", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code", "name"],
      colspan: 4,
      row: renderRow,
      emptyText: "No matching rows",
    });
    const search = document.querySelector<HTMLInputElement>(".dt-search")!;
    search.value = "zzz_no_match";
    search.dispatchEvent(new Event("input"));
    const cell = document
      .getElementById("test-table")
      ?.querySelector("tbody td.table-empty");
    expect(cell?.textContent).toBe("No matching rows");
    expect(cell?.getAttribute("colspan")).toBe("4");
  });

  it("setRows swaps data and resets pager", () => {
    const handle = mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
      pageSize: 2,
    });
    handle.setRows([
      { code: "X", name: "X", mv_twd: 1, venue: "TW" },
      { code: "Y", name: "Y", mv_twd: 2, venue: "TW" },
      { code: "Z", name: "Z", mv_twd: 3, venue: "TW" },
    ]);
    const lines = tbodyText(
      document.getElementById("test-table") as HTMLTableElement,
    );
    expect(lines).toHaveLength(2);
    expect(lines.map((l) => l.split("|")[0])).toEqual(["X", "Y"]);
  });

  it("re-mounting the same tableId is idempotent (no duplicate toolbars)", () => {
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows,
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
    });
    mountDataTable<Row>({
      tableId: "test-table",
      rows: sampleRows.slice(0, 2),
      searchKeys: ["code"],
      colspan: 4,
      row: renderRow,
    });
    expect(document.querySelectorAll(".dt-toolbar")).toHaveLength(1);
    expect(document.querySelectorAll(".dt-pager")).toHaveLength(1);
  });
});
