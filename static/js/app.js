/**
 * App-wide bootstrap: theme toggle, "as of" footer, page-level dispatcher.
 */
(function () {
  const STORAGE_KEY = "sinopac-theme";

  function setTheme(name) {
    document.documentElement.setAttribute("data-theme", name);
    try { localStorage.setItem(STORAGE_KEY, name); } catch (_) {}
    if (window.charts) window.charts.applyDefaults();
    window.dispatchEvent(new CustomEvent("theme-change", { detail: name }));
  }

  function initTheme() {
    let saved = "dark";
    try { saved = localStorage.getItem(STORAGE_KEY) || "dark"; } catch (_) {}
    setTheme(saved);
    const btn = document.getElementById("theme-toggle");
    if (btn) {
      btn.addEventListener("click", () => {
        const cur = document.documentElement.getAttribute("data-theme");
        setTheme(cur === "dark" ? "light" : "dark");
      });
    }
  }

  async function initFooter() {
    const el = document.getElementById("as-of");
    if (!el) return;
    try {
      const data = await window.api.get("/api/health");
      if (data && data.as_of) {
        el.textContent = `as of ${window.fmt.month(data.as_of)}`;
      } else {
        el.textContent = "no data";
      }
    } catch (e) {
      el.textContent = "offline";
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    initTheme();
    initFooter();
  });
})();
