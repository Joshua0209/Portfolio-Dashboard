/**
 * Tiny pagination helper for client-side row tables.
 *
 * Usage:
 *   const pager = createPager({
 *     containerId: "tx-pager",
 *     pageSize: 50,
 *     onChange: () => rerender(),
 *   });
 *   const view = pager.slice(filteredRows);   // returns the page's rows
 *   pager.update(filteredRows.length);        // call after filters change
 *
 * Renders into containerId: [page size select] [prev] [page X / Y] [next]
 * Resets to page 1 whenever the underlying total shrinks below current page.
 */
(function () {
  function createPager({ containerId, pageSize = 50, onChange = () => {}, sizes = [25, 50, 100, 200, "all"] }) {
    const container = document.getElementById(containerId);
    let page = 1;
    let size = pageSize;
    let total = 0;

    function pageCount() {
      if (size === "all") return 1;
      return Math.max(1, Math.ceil(total / size));
    }

    function render() {
      if (!container) return;
      while (container.firstChild) container.removeChild(container.firstChild);

      const wrap = document.createElement("div");
      wrap.className = "pager";

      const sizeLabel = document.createElement("label");
      sizeLabel.className = "pager-size";
      const sizeText = document.createElement("span");
      sizeText.className = "text-mute text-tiny";
      sizeText.textContent = "Rows per page:";
      const select = document.createElement("select");
      select.className = "select";
      for (const s of sizes) {
        const opt = document.createElement("option");
        opt.value = String(s);
        opt.textContent = String(s);
        if (String(size) === String(s)) opt.selected = true;
        select.appendChild(opt);
      }
      select.addEventListener("change", () => {
        size = select.value === "all" ? "all" : Number(select.value);
        page = 1;
        render();
        onChange();
      });
      sizeLabel.append(sizeText, select);

      const nav = document.createElement("div");
      nav.className = "pager-nav";
      const prev = document.createElement("button");
      prev.className = "btn btn-sm";
      prev.textContent = "‹ Prev";
      prev.disabled = page <= 1;
      prev.addEventListener("click", () => { page = Math.max(1, page - 1); render(); onChange(); });

      const counter = document.createElement("span");
      counter.className = "pager-counter num text-mute text-tiny";
      const start = total === 0 ? 0 : (size === "all" ? 1 : (page - 1) * size + 1);
      const end = total === 0 ? 0 : (size === "all" ? total : Math.min(total, page * size));
      counter.textContent = `${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}`;

      const next = document.createElement("button");
      next.className = "btn btn-sm";
      next.textContent = "Next ›";
      next.disabled = page >= pageCount();
      next.addEventListener("click", () => { page = Math.min(pageCount(), page + 1); render(); onChange(); });

      nav.append(prev, counter, next);
      wrap.append(sizeLabel, nav);
      container.appendChild(wrap);
    }

    function slice(rows) {
      total = rows.length;
      if (page > pageCount()) page = pageCount();
      if (size === "all") return rows;
      const start = (page - 1) * size;
      return rows.slice(start, start + size);
    }

    function update(newTotal) {
      total = newTotal;
      if (page > pageCount()) page = pageCount();
      render();
    }

    function reset() { page = 1; render(); }

    render();
    return { slice, update, reset };
  }

  window.createPager = createPager;
})();
