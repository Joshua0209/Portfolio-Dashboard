// Banner — typed mount for DLQ + reconcile messages.
// Phase 8 Cycle 56. Shape matches templates/_dlq_banner.html and
// templates/_reconcile_banner.html so app.css continues to apply.

export type BannerTone = "neg" | "warn" | "info" | "pos";

export interface BannerAction {
  label: string;
  href: string;
}

export interface BannerProps {
  tone: BannerTone;
  message: string;
  action?: BannerAction;
}

export const renderBanner = (root: HTMLElement, props: BannerProps): void => {
  while (root.firstChild) root.removeChild(root.firstChild);
  if (!props.message) return;

  const banner = document.createElement("div");
  banner.className = `banner banner--${props.tone}`;

  const text = document.createElement("span");
  text.className = "banner-text";
  text.textContent = props.message;
  banner.appendChild(text);

  if (props.action) {
    const link = document.createElement("a");
    link.className = "banner-action";
    link.href = props.action.href;
    link.textContent = props.action.label;
    banner.appendChild(link);
  }

  root.appendChild(banner);
};
