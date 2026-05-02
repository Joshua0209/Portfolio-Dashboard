// Tiny DOM primitives shared by every page mount. Centralised here so the
// 12 page modules don't each re-declare an identical `el()` / `setText()`.
//
// The contract is intentionally minimal — pages compose these with
// `appendChild` chains rather than passing children, because that's what
// the existing call sites already do.

export const el = (
  tag: string,
  attrs: Record<string, string> = {},
  text?: string,
): HTMLElement => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (text !== undefined) node.textContent = text;
  return node;
};

export const setText = (id: string, text: string): void => {
  const node = document.getElementById(id);
  if (node) node.textContent = text;
};
