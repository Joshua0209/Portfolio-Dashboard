// Theme toggle — port of static/js/app.js theme handling.
// localStorage key matches the legacy app so users keep their preference
// across the cutover.

const STORAGE_KEY = "sinopac-theme";

export type Theme = "dark" | "light";

const isTheme = (v: unknown): v is Theme => v === "dark" || v === "light";

const readStored = (): Theme => {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return isTheme(raw) ? raw : "dark";
  } catch {
    return "dark";
  }
};

export const applyTheme = (name: Theme): void => {
  document.documentElement.setAttribute("data-theme", name);
  try {
    localStorage.setItem(STORAGE_KEY, name);
  } catch {
    // Storage may be disabled in private mode — ignore.
  }
  window.dispatchEvent(
    new CustomEvent<Theme>("theme-change", { detail: name }),
  );
};

export const initTheme = (): Theme => {
  const t = readStored();
  applyTheme(t);
  return t;
};

export const currentTheme = (): Theme => {
  const v = document.documentElement.getAttribute("data-theme");
  return isTheme(v) ? v : "dark";
};

export const toggleTheme = (): Theme => {
  const next: Theme = currentTheme() === "dark" ? "light" : "dark";
  applyTheme(next);
  return next;
};
